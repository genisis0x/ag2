# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``Hub`` — registry, dispatcher, persistence root.

M2 surface: registry + envelope dispatch (M1) + session machinery and
adapter state cache + TTL sweeper. The expectation sweeper, audit log,
and capability index are M3.

Session machinery:
* Adapter registry by ``(manifest.type, manifest.version)``.
* Per-session ``AdapterState`` cache, folded under the per-session
  WAL lock so ``validate_send`` and ``on_accepted`` are O(1).
* Single-recipient consulting handshake: ``create_session`` posts
  ``EV_SESSION_INVITE``, awaits ``EV_SESSION_INVITE_ACK`` (timeout
  ``invite_ack_timeout``), broadcasts ``EV_SESSION_OPENED`` on quorum.
* TTL: parsed from ``Rule.limits.session_ttl_default`` /
  ``task_ttl_default`` (or per-session override). The ``_TtlSweeper``
  walks active sessions and tasks every ``ttl_sweep_interval``;
  cascades non-terminal tasks under closing sessions to ``EXPIRED``.

The hub never calls ``Agent.ask``, executes tenant transforms, or
imports tenant modules — the trust boundary runs through ``HubClient``
/ ``AgentClient``.
"""

import asyncio
import fnmatch
import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

from autogen.beta.knowledge import KnowledgeStore
from autogen.beta.task import TERMINAL_TASK_STATES, TaskMetadata, TaskSpec, TaskState

from ..adapters.base import SessionAdapter
from ..adapters.consulting import ConsultingAdapter
from ..auth import AuthRegistry, default_registry
from ..envelope import (
    EV_SESSION_CLOSED,
    EV_SESSION_EXPIRED,
    EV_SESSION_INVITE,
    EV_SESSION_INVITE_ACK,
    EV_SESSION_INVITE_REJECT,
    EV_SESSION_OPENED,
    Envelope,
)
from ..errors import AccessDeniedError, NetworkError, NotFoundError, ProtocolError
from ..identity import Passport, Resume
from ..ids import make_id
from ..rule import Rule, parse_duration
from ..session import (
    Participant,
    ParticipantRole,
    SessionMetadata,
    SessionState,
    is_terminal_session_state,
)
from ..transport.frames import (
    AcceptFrame,
    ErrorFrame,
    Frame,
    HelloFrame,
    NotifyFrame,
    PingFrame,
    PongFrame,
    SendFrame,
    WelcomeFrame,
)
from ..transport.link import LinkEndpoint
from .layout import (
    agents_root,
    passport_path,
    resume_path,
    rule_path,
    session_metadata_path,
    sessions_root,
    skill_path,
    task_metadata_path,
    tasks_root,
    wal_path,
)
from .sweepers import _IntervalSweeper

__all__ = ("Hub",)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_ERROR_CODE_MAP: dict[type, str] = {
    NotFoundError: "not_found",
    AccessDeniedError: "access_denied",
    ProtocolError: "protocol_error",
}


def _error_code(exc: BaseException) -> str:
    for cls, code in _ERROR_CODE_MAP.items():
        if isinstance(exc, cls):
            return code
    return "error"


def _match_any(name: str, patterns: list[str]) -> bool:
    """True if ``name`` matches any of the glob patterns (``["*"]`` allows all)."""
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def _is_session_protocol_event(event_type: str) -> bool:
    return event_type.startswith("ag2.session.")


def _is_task_event(event_type: str) -> bool:
    return event_type.startswith("ag2.task.")


def _is_protocol_event(event_type: str) -> bool:
    return _is_session_protocol_event(event_type) or _is_task_event(event_type)


def _expires_at(now_iso: str, ttl_seconds: int) -> str:
    """Compute ``expires_at`` ISO timestamp from a base + duration."""
    if ttl_seconds <= 0:
        return ""
    base = datetime.fromisoformat(now_iso)
    return (base + timedelta(seconds=ttl_seconds)).isoformat()


class Hub:
    """In-process registry, dispatcher, session state-machine, persistence root.

    Construct with :meth:`open` for production (hydrates from disk and
    spawns sweepers); the sync ``__init__`` is for tests that need
    fine-grained control.
    """

    def __init__(
        self,
        store: KnowledgeStore,
        *,
        auth: AuthRegistry | None = None,
        clock: Callable[[], str] | None = None,
        ttl_sweep_interval: float = 30.0,
        invite_ack_timeout: float = 30.0,
    ) -> None:
        # __init__ stores params; side effects deferred to start()/hydrate().
        self._store = store
        self._auth = auth if auth is not None else default_registry
        self._clock = clock if clock is not None else _utc_now_iso
        self._ttl_sweep_interval = ttl_sweep_interval
        self._invite_ack_timeout = invite_ack_timeout

        # Identity caches.
        self._passports: dict[str, Passport] = {}
        self._resumes: dict[str, Resume] = {}
        self._rules: dict[str, Rule] = {}
        self._skills: dict[str, str] = {}
        self._name_to_id: dict[str, str] = {}

        # Adapter registry.
        self._adapters: dict[tuple[str, int], SessionAdapter] = {}

        # Session caches.
        self._sessions: dict[str, SessionMetadata] = {}
        self._active_sessions: dict[str, SessionMetadata] = {}
        self._adapter_states: dict[str, object] = {}
        self._session_open_waiters: dict[str, asyncio.Future[SessionMetadata]] = {}

        # Task caches (observed; not owned).
        self._tasks: dict[str, TaskMetadata] = {}
        self._session_tasks: dict[str, set[str]] = {}

        # Transport-side state.
        self._endpoints_by_id: dict[str, LinkEndpoint] = {}
        self._agent_to_endpoint: dict[str, str] = {}
        self._endpoint_to_agents: dict[str, set[str]] = {}
        self._endpoint_tasks: set[asyncio.Task[None]] = set()

        # Per-session locks for WAL append + dispatch ordering.
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._registration_lock = asyncio.Lock()

        self._ttl_sweeper: _IntervalSweeper | None = None
        self._closed = False

    # ── Lifecycle ────────────────────────────────────────────────────────────

    @classmethod
    async def open(
        cls,
        store: KnowledgeStore,
        *,
        auth: AuthRegistry | None = None,
        clock: Callable[[], str] | None = None,
        ttl_sweep_interval: float = 30.0,
        invite_ack_timeout: float = 30.0,
        register_default_adapters: bool = True,
    ) -> "Hub":
        """Construct + hydrate from disk + start sweepers. Production entry point.

        ``register_default_adapters=True`` (default) registers
        ``ConsultingAdapter()`` for ``consulting@v1`` so simple test
        setups don't need an explicit registration call.
        """
        hub = cls(
            store,
            auth=auth,
            clock=clock,
            ttl_sweep_interval=ttl_sweep_interval,
            invite_ack_timeout=invite_ack_timeout,
        )
        if register_default_adapters:
            hub.register_adapter(ConsultingAdapter())
        await hub.hydrate()
        await hub.start()
        return hub

    async def hydrate(self) -> None:
        """Walk the store; rebuild caches. Idempotent.

        M2 hydrates identities (M1) plus sessions and tasks. Active
        session WALs are re-folded through their adapter so the
        ``_adapter_states`` cache is rebuilt deterministically.
        """
        self._passports.clear()
        self._resumes.clear()
        self._rules.clear()
        self._skills.clear()
        self._name_to_id.clear()
        self._sessions.clear()
        self._active_sessions.clear()
        self._adapter_states.clear()
        self._tasks.clear()
        self._session_tasks.clear()

        # Identities.
        agent_children = await self._store.list(agents_root())
        for child in agent_children:
            if not child.endswith("/"):
                continue
            agent_id = child.rstrip("/")
            await self._load_agent(agent_id)

        # Sessions — load metadata first, then re-fold WALs.
        session_children = await self._store.list(sessions_root())
        for child in session_children:
            if not child.endswith("/"):
                continue
            session_id = child.rstrip("/")
            await self._load_session(session_id)

        # Tasks.
        task_children = await self._store.list(tasks_root())
        for child in task_children:
            if not child.endswith("/"):
                continue
            task_id = child.rstrip("/")
            await self._load_task(task_id)

    async def start(self) -> None:
        """Spawn the TTL sweeper. Idempotent. ``ttl_sweep_interval=0`` disables."""
        if self._ttl_sweep_interval > 0 and self._ttl_sweeper is None:
            self._ttl_sweeper = _IntervalSweeper(
                name="ttl",
                interval=self._ttl_sweep_interval,
                fn=self.expire_due,
            )
            self._ttl_sweeper.start()

    async def close(self) -> None:
        """Cancel sweepers + endpoint tasks; drain queues. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._ttl_sweeper is not None:
            await self._ttl_sweeper.stop()
            self._ttl_sweeper = None
        for task in list(self._endpoint_tasks):
            task.cancel()
        if self._endpoint_tasks:
            await asyncio.gather(*self._endpoint_tasks, return_exceptions=True)
        self._endpoint_tasks.clear()

    async def __aenter__(self) -> "Hub":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ── Adapter registry ────────────────────────────────────────────────────

    def register_adapter(self, adapter: SessionAdapter) -> None:
        """Register a ``SessionAdapter`` keyed by ``(type, version)``.

        Re-registering at the same key replaces the prior adapter; the
        old key's existing in-flight sessions keep their snapshotted
        manifest for life.
        """
        key = (adapter.manifest.type, adapter.manifest.version)
        self._adapters[key] = adapter

    def _adapter_for(self, manifest_type: str, manifest_version: int) -> SessionAdapter:
        adapter = self._adapters.get((manifest_type, manifest_version))
        if adapter is None:
            raise NotFoundError(
                f"no adapter registered for {manifest_type!r}@v{manifest_version}"
            )
        return adapter

    # ── Registration (M1) ───────────────────────────────────────────────────

    async def register(
        self,
        passport: Passport,
        resume: Resume,
        *,
        skill_md: str | None = None,
        rule: Rule | None = None,
    ) -> Passport:
        adapter = self._auth.get(passport.auth.scheme)
        await adapter.validate(passport, passport.auth.claim)

        async with self._registration_lock:
            agent_id = make_id()
            passport.agent_id = agent_id
            passport.created_at = self._clock()

            effective_rule = rule if rule is not None else Rule()

            await self._persist_passport(passport)
            await self._persist_resume(agent_id, resume)
            await self._persist_rule(agent_id, effective_rule)
            if skill_md is not None:
                await self._persist_skill(agent_id, skill_md)

            self._passports[agent_id] = passport
            self._resumes[agent_id] = resume
            self._rules[agent_id] = effective_rule
            if skill_md is not None:
                self._skills[agent_id] = skill_md
            self._name_to_id[passport.name] = agent_id

        return passport

    async def unregister(self, agent_id: str) -> None:
        if agent_id not in self._passports:
            raise NotFoundError(f"agent not registered: {agent_id}")

        async with self._registration_lock:
            passport = self._passports.pop(agent_id, None)
            self._resumes.pop(agent_id, None)
            self._rules.pop(agent_id, None)
            self._skills.pop(agent_id, None)
            if passport is not None and self._name_to_id.get(passport.name) == agent_id:
                self._name_to_id.pop(passport.name, None)

            endpoint_id = self._agent_to_endpoint.pop(agent_id, None)
            if endpoint_id is not None:
                bound = self._endpoint_to_agents.get(endpoint_id)
                if bound is not None:
                    bound.discard(agent_id)
                    if not bound:
                        self._endpoint_to_agents.pop(endpoint_id, None)

    # ── Discovery (read-side) ────────────────────────────────────────────────

    async def get_agent(self, name_or_id: str) -> Passport:
        agent_id = self._name_to_id.get(name_or_id, name_or_id)
        passport = self._passports.get(agent_id)
        if passport is None:
            raise NotFoundError(f"agent not found: {name_or_id}")
        return passport

    async def get_resume(self, agent_id: str) -> Resume:
        resume = self._resumes.get(agent_id)
        if resume is None:
            raise NotFoundError(f"resume not found: {agent_id}")
        return resume

    async def get_skill(self, agent_id: str) -> str | None:
        if agent_id in self._skills:
            return self._skills[agent_id]
        if agent_id not in self._passports:
            return None
        body = await self._store.read(skill_path(agent_id))
        if body is not None:
            self._skills[agent_id] = body
        return body

    async def list_agents(
        self,
        *,
        capability: str | None = None,
        query: str | None = None,
        sort_by: str | None = None,
        limit: int = 50,
    ) -> list[Passport]:
        results: list[Passport] = []
        query_lower = query.lower() if query else None
        for agent_id, passport in self._passports.items():
            if capability is not None:
                resume = self._resumes.get(agent_id)
                if resume is None:
                    continue
                claimed = set(resume.claimed_capabilities)
                observed = set(resume.observed.keys())
                if capability not in claimed and capability not in observed:
                    continue
            if query_lower is not None:
                resume = self._resumes.get(agent_id)
                summary = resume.summary.lower() if resume else ""
                if query_lower not in summary:
                    continue
            results.append(passport)

        if sort_by == "name":
            results.sort(key=lambda p: p.name)

        return results[:limit]

    # ── Mutation (M1) ────────────────────────────────────────────────────────

    async def set_resume(self, agent_id: str, resume: Resume) -> None:
        if agent_id not in self._passports:
            raise NotFoundError(f"agent not registered: {agent_id}")
        resume.last_updated = self._clock()
        resume.version = (
            (self._resumes[agent_id].version + 1) if agent_id in self._resumes else resume.version
        )
        await self._persist_resume(agent_id, resume)
        self._resumes[agent_id] = resume

    async def set_skill(self, agent_id: str, skill_md: str | None) -> None:
        if agent_id not in self._passports:
            raise NotFoundError(f"agent not registered: {agent_id}")
        if skill_md is None:
            await self._store.delete(skill_path(agent_id))
            self._skills.pop(agent_id, None)
        else:
            await self._persist_skill(agent_id, skill_md)
            self._skills[agent_id] = skill_md

    async def set_rule(self, agent_id: str, rule: Rule) -> None:
        if agent_id not in self._passports:
            raise NotFoundError(f"agent not registered: {agent_id}")
        rule.version = (
            (self._rules[agent_id].version + 1) if agent_id in self._rules else rule.version
        )
        await self._persist_rule(agent_id, rule)
        self._rules[agent_id] = rule

    # ── Sessions ────────────────────────────────────────────────────────────

    async def create_session(
        self,
        *,
        creator_id: str,
        manifest_type: str,
        manifest_version: int = 1,
        participants: list[str],
        required_acks: int | None = None,
        ttl: str | int | None = None,
        knobs: dict[str, object] | None = None,
        intent: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> SessionMetadata:
        """Allocate ``session_id``, post invites, await acks, return metadata.

        For consulting (M2 only) this is a single-recipient handshake:
        one ``EV_SESSION_INVITE`` to the respondent, one
        ``EV_SESSION_INVITE_ACK`` back, transition to ``ACTIVE``,
        broadcast ``EV_SESSION_OPENED``. Times out after
        ``invite_ack_timeout`` if the ack does not arrive.
        """
        if creator_id not in self._passports:
            raise NotFoundError(f"creator not registered: {creator_id}")
        if not participants:
            raise ProtocolError("session requires at least one participant")
        seen: set[str] = set()
        for p_id in participants:
            if p_id in seen:
                raise ProtocolError(f"participant listed twice: {p_id!r}")
            seen.add(p_id)
            if p_id != creator_id and p_id not in self._passports:
                raise NotFoundError(f"participant not registered: {p_id}")
        if creator_id not in participants:
            participants = [creator_id, *participants]

        adapter = self._adapter_for(manifest_type, manifest_version)

        session_id = make_id()
        now = self._clock()

        creator_rule = self._rules.get(creator_id, Rule())
        ttl_value: str | int = ttl if ttl is not None else creator_rule.limits.session_ttl_default
        ttl_seconds = parse_duration(ttl_value)
        expires_at = _expires_at(now, ttl_seconds) or None

        metadata_participants: list[Participant] = []
        for index, p_id in enumerate(participants):
            if p_id == creator_id:
                role = ParticipantRole.INITIATOR
            elif len(participants) == 2:
                role = ParticipantRole.RESPONDENT
            else:
                role = ParticipantRole.PARTICIPANT
            metadata_participants.append(
                Participant(agent_id=p_id, role=role, order=index, joined_at=now)
            )

        final_labels: dict[str, str] = dict(labels) if labels else {}
        if intent:
            final_labels["intent"] = intent

        invitees = [p_id for p_id in participants if p_id != creator_id]

        metadata = SessionMetadata(
            session_id=session_id,
            manifest=adapter.manifest,
            creator_id=creator_id,
            participants=metadata_participants,
            state=SessionState.PENDING,
            created_at=now,
            expires_at=expires_at,
            knobs=dict(knobs) if knobs else {},
            labels=final_labels,
            required_acks=required_acks,
            pending_acks=list(invitees),
        )

        adapter.validate_create(metadata)

        # Activate caches before persistence so the post_envelope path
        # finds the metadata when the invite is dispatched.
        self._sessions[session_id] = metadata
        self._active_sessions[session_id] = metadata
        self._adapter_states[session_id] = adapter.initial_state(metadata)

        await self._persist_session_metadata(metadata)

        if not invitees:
            # Self-only session — already complete; transition to ACTIVE.
            await self._activate_session(session_id)
            return metadata

        waiter: asyncio.Future[SessionMetadata] = asyncio.get_event_loop().create_future()
        self._session_open_waiters[session_id] = waiter

        # Post invites — each goes to one invitee via post_envelope.
        invite_data: dict[str, object] = {
            "session_id": session_id,
            "manifest_type": manifest_type,
            "manifest_version": manifest_version,
            "creator_id": creator_id,
            "knobs": metadata.knobs,
            "labels": metadata.labels,
        }
        try:
            for invitee_id in invitees:
                envelope = Envelope(
                    session_id=session_id,
                    sender_id=creator_id,
                    audience=[invitee_id],
                    event_type=EV_SESSION_INVITE,
                    event_data=invite_data,
                )
                await self.post_envelope(envelope)
        except Exception:
            self._session_open_waiters.pop(session_id, None)
            await self._transition_session(session_id, SessionState.CLOSED, "invite_failed")
            raise

        try:
            return await asyncio.wait_for(waiter, timeout=self._invite_ack_timeout)
        except asyncio.TimeoutError as exc:
            await self._transition_session(session_id, SessionState.CLOSED, "invite_timeout")
            raise ProtocolError(f"session {session_id!r} ack timeout") from exc
        finally:
            self._session_open_waiters.pop(session_id, None)

    async def close_session(self, session_id: str, *, reason: str = "") -> SessionMetadata:
        await self._transition_session(session_id, SessionState.CLOSED, reason or "explicit_close")
        return self._sessions[session_id]

    async def get_session(self, session_id: str) -> SessionMetadata:
        metadata = self._sessions.get(session_id)
        if metadata is None:
            raise NotFoundError(f"session not found: {session_id}")
        return metadata

    async def list_sessions(
        self,
        *,
        agent_id: str | None = None,
        state: SessionState | None = None,
        limit: int = 50,
    ) -> list[SessionMetadata]:
        results: list[SessionMetadata] = []
        for metadata in self._sessions.values():
            if state is not None and metadata.state != state:
                continue
            if agent_id is not None and agent_id not in metadata.participant_ids():
                continue
            results.append(metadata)
        return results[:limit]

    async def read_wal(
        self,
        session_id: str,
        *,
        since: int = 0,
        until: int | None = None,
    ) -> list[Envelope]:
        body = await self._store.read(wal_path(session_id))
        if not body:
            return []
        envelopes: list[Envelope] = []
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            envelopes.append(Envelope.from_json(line))
        end = len(envelopes) if until is None else until
        return envelopes[since:end]

    # ── Tasks (observe-only) ────────────────────────────────────────────────

    async def observe_task(self, metadata: TaskMetadata) -> None:
        """Register a task observed via the agent's stream.

        Hub does not create, assign, or cancel — it stores
        ``TaskMetadata``, persists it, and starts TTL accounting.
        """
        if metadata.task_id in self._tasks:
            # Update in place — owner re-emitting TaskStarted on retry, etc.
            existing = self._tasks[metadata.task_id]
            existing.state = metadata.state
            existing.started_at = metadata.started_at or existing.started_at
            existing.expires_at = metadata.expires_at or existing.expires_at
            existing.session_id = metadata.session_id or existing.session_id
            existing.progress.update(metadata.progress)
            await self._persist_task_metadata(existing)
            return
        self._tasks[metadata.task_id] = metadata
        if metadata.session_id:
            self._session_tasks.setdefault(metadata.session_id, set()).add(metadata.task_id)
        await self._persist_task_metadata(metadata)

    async def get_task(self, task_id: str) -> TaskMetadata:
        metadata = self._tasks.get(task_id)
        if metadata is None:
            raise NotFoundError(f"task not found: {task_id}")
        return metadata

    async def update_task(
        self,
        task_id: str,
        *,
        state: TaskState | None = None,
        progress: dict[str, object] | None = None,
        result: object | None = None,
        error: str | None = None,
    ) -> None:
        """Update an observed task's lifecycle. Used by ``task_mirror``.

        Terminal-state transitions stamp ``completed_at``. Idempotent —
        terminal-on-terminal is a no-op (further events ignored).
        """
        metadata = self._tasks.get(task_id)
        if metadata is None:
            raise NotFoundError(f"task not found: {task_id}")
        if metadata.state in TERMINAL_TASK_STATES:
            return
        if progress:
            metadata.progress.update(progress)
            metadata.last_progress_at = self._clock()
        if result is not None:
            metadata.result = result
        if error:
            metadata.error = error
        if state is not None:
            metadata.state = state
            if state in TERMINAL_TASK_STATES:
                metadata.completed_at = self._clock()
        await self._persist_task_metadata(metadata)

    async def list_tasks(
        self,
        *,
        agent_id: str | None = None,
        session_id: str | None = None,
        state: TaskState | None = None,
        limit: int = 50,
    ) -> list[TaskMetadata]:
        results: list[TaskMetadata] = []
        for metadata in self._tasks.values():
            if agent_id is not None and metadata.owner_id != agent_id:
                continue
            if session_id is not None and metadata.session_id != session_id:
                continue
            if state is not None and metadata.state != state:
                continue
            results.append(metadata)
        return results[:limit]

    # ── Sweeper hook ────────────────────────────────────────────────────────

    async def expire_due(self) -> None:
        """Walk active sessions and tasks; expire ones past their TTL.

        Cascades non-terminal tasks under closing sessions (via
        :meth:`_transition_session`).
        """
        now = self._clock()

        expired_sessions: list[str] = []
        for session_id, metadata in list(self._active_sessions.items()):
            if metadata.expires_at and metadata.expires_at <= now:
                expired_sessions.append(session_id)
        for session_id in expired_sessions:
            await self._transition_session(session_id, SessionState.EXPIRED, "ttl_expired")

        # Expire standalone tasks (those not under an expiring session).
        expired_tasks: list[str] = []
        for task_id, metadata in list(self._tasks.items()):
            if metadata.state in TERMINAL_TASK_STATES:
                continue
            if metadata.expires_at and metadata.expires_at <= now:
                expired_tasks.append(task_id)
        for task_id in expired_tasks:
            await self._transition_task(task_id, TaskState.EXPIRED, "ttl_expired")

    # ── Envelope dispatch ───────────────────────────────────────────────────

    async def post_envelope(self, envelope: Envelope) -> str:
        """Validate sender + adapter + WAL append + dispatch.

        Per-session lock makes ``validate_send`` / ``fold`` /
        ``on_accepted`` see a consistent state. Dispatch and post-accept
        transitions happen outside the lock so the broadcast of
        ``EV_SESSION_CLOSED`` does not deadlock on the same lock.
        """
        sender = self._passports.get(envelope.sender_id)
        if sender is None:
            raise NotFoundError(f"sender not registered: {envelope.sender_id}")

        sender_rule = self._rules.get(envelope.sender_id, Rule())

        # Outbound access check.
        if envelope.audience is not None:
            for recipient_id in envelope.audience:
                recipient = self._passports.get(recipient_id)
                if recipient is None:
                    continue
                if not _match_any(recipient.name, sender_rule.access.outbound_to):
                    raise AccessDeniedError(
                        f"sender {sender.name!r} not permitted to send to {recipient.name!r}"
                    )

        metadata = self._sessions.get(envelope.session_id)
        if metadata is None:
            raise NotFoundError(f"session not found: {envelope.session_id}")
        if metadata.is_terminal():
            raise ProtocolError(
                f"session {envelope.session_id!r} is {metadata.state.value}"
            )
        if (
            not _is_protocol_event(envelope.event_type)
            and metadata.state != SessionState.ACTIVE
        ):
            raise ProtocolError(
                f"session {envelope.session_id!r} not active (state={metadata.state.value})"
            )

        adapter = self._adapter_for(metadata.manifest.type, metadata.manifest.version)

        # Critical section: validate, append, fold, on_accepted under lock.
        async with self._wal_lock(envelope.session_id):
            state = self._adapter_states[envelope.session_id]
            adapter.validate_send(metadata, envelope, state)

            envelope.envelope_id = make_id()
            envelope.created_at = self._clock()

            await self._wal_append(envelope)
            new_state = adapter.fold(envelope, state)
            self._adapter_states[envelope.session_id] = new_state
            result = adapter.on_accepted(metadata, envelope, new_state)

        # Outside lock: dispatch + post-accept handling.
        # Acks/rejects are absorbed by the hub — they aren't dispatched.
        if envelope.event_type == EV_SESSION_INVITE_ACK:
            await self._handle_invite_ack(envelope, metadata)
            return envelope.envelope_id
        if envelope.event_type == EV_SESSION_INVITE_REJECT:
            await self._handle_invite_reject(envelope, metadata)
            return envelope.envelope_id

        await self._dispatch(envelope, metadata)

        if result.next_state is not None:
            await self._transition_session(
                envelope.session_id,
                result.next_state,
                result.auto_close_reason,
            )

        return envelope.envelope_id

    # ── Endpoint management ─────────────────────────────────────────────────

    def attach_endpoint(self, endpoint: LinkEndpoint) -> None:
        if self._closed:
            return
        self._endpoints_by_id[endpoint.endpoint_id] = endpoint
        task = asyncio.create_task(self._handle_endpoint(endpoint))
        self._endpoint_tasks.add(task)
        task.add_done_callback(self._endpoint_tasks.discard)

    def bind_endpoint(self, endpoint_id: str, agent_id: str) -> None:
        if endpoint_id not in self._endpoints_by_id:
            raise NotFoundError(f"endpoint not attached: {endpoint_id}")
        if agent_id not in self._passports:
            raise NotFoundError(f"agent not registered: {agent_id}")
        self._endpoints_by_id[endpoint_id].agent_id = agent_id
        self._agent_to_endpoint[agent_id] = endpoint_id
        self._endpoint_to_agents.setdefault(endpoint_id, set()).add(agent_id)

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _wal_lock(self, session_id: str) -> asyncio.Lock:
        lock = self._session_locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._session_locks[session_id] = lock
        return lock

    async def _wal_append(self, envelope: Envelope) -> None:
        await self._store.append(wal_path(envelope.session_id), envelope.to_json() + "\n")

    async def _dispatch(self, envelope: Envelope, metadata: SessionMetadata) -> None:
        """Send NotifyFrames to the audience (or all participants if broadcast)."""
        if envelope.audience is None:
            recipients = [
                p.agent_id for p in metadata.participants if p.agent_id != envelope.sender_id
            ]
        else:
            recipients = list(envelope.audience)

        sender_passport = self._passports.get(envelope.sender_id)
        sender_name = sender_passport.name if sender_passport is not None else envelope.sender_id

        for recipient_id in recipients:
            recipient_rule = self._rules.get(recipient_id)
            if recipient_rule is not None and not _match_any(
                sender_name, recipient_rule.access.inbound_from
            ):
                continue
            endpoint = self._endpoint_for(recipient_id)
            if endpoint is None:
                continue
            await endpoint.send_frame(
                NotifyFrame(envelope=envelope, recipient_id=recipient_id)
            )

    def _endpoint_for(self, agent_id: str) -> LinkEndpoint | None:
        endpoint_id = self._agent_to_endpoint.get(agent_id)
        if endpoint_id is None:
            return None
        return self._endpoints_by_id.get(endpoint_id)

    async def _handle_endpoint(self, endpoint: LinkEndpoint) -> None:
        try:
            async for frame in endpoint.frames():
                await self._dispatch_frame(endpoint, frame)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def _dispatch_frame(self, endpoint: LinkEndpoint, frame: Frame) -> None:
        if isinstance(frame, SendFrame):
            try:
                envelope_id = await self.post_envelope(frame.envelope)
                await endpoint.send_frame(AcceptFrame(envelope_id=envelope_id))
            except NetworkError as exc:
                await endpoint.send_frame(
                    ErrorFrame(code=_error_code(exc), message=str(exc))
                )
        elif isinstance(frame, HelloFrame):
            agent_id = self._name_to_id.get(frame.name)
            if agent_id is None:
                await endpoint.send_frame(
                    ErrorFrame(code="not_found", message=f"unknown name: {frame.name}")
                )
                return
            try:
                self.bind_endpoint(endpoint.endpoint_id, agent_id)
            except NetworkError as exc:
                await endpoint.send_frame(
                    ErrorFrame(code=_error_code(exc), message=str(exc))
                )
                return
            await endpoint.send_frame(
                WelcomeFrame(endpoint_id=endpoint.endpoint_id, hub_time=self._clock())
            )
        elif isinstance(frame, PingFrame):
            await endpoint.send_frame(PongFrame())

    # ── Session transition helpers ──────────────────────────────────────────

    async def _handle_invite_ack(
        self, envelope: Envelope, metadata: SessionMetadata
    ) -> None:
        if metadata.state != SessionState.PENDING:
            return
        if envelope.sender_id in metadata.pending_acks:
            metadata.pending_acks.remove(envelope.sender_id)
            await self._persist_session_metadata(metadata)
        if not metadata.pending_acks and not metadata.rejected_by:
            await self._activate_session(metadata.session_id)

    async def _handle_invite_reject(
        self, envelope: Envelope, metadata: SessionMetadata
    ) -> None:
        if metadata.state != SessionState.PENDING:
            return
        if envelope.sender_id in metadata.pending_acks:
            metadata.pending_acks.remove(envelope.sender_id)
        if envelope.sender_id not in metadata.rejected_by:
            metadata.rejected_by.append(envelope.sender_id)
        await self._persist_session_metadata(metadata)
        # M2: any reject fails the session (consulting all-or-nothing).
        await self._transition_session(
            metadata.session_id, SessionState.CLOSED, "invite_rejected"
        )
        waiter = self._session_open_waiters.get(metadata.session_id)
        if waiter is not None and not waiter.done():
            waiter.set_exception(
                ProtocolError(f"session rejected by {envelope.sender_id}")
            )

    async def _activate_session(self, session_id: str) -> None:
        metadata = self._sessions.get(session_id)
        if metadata is None or metadata.state != SessionState.PENDING:
            return
        metadata.state = SessionState.ACTIVE
        await self._persist_session_metadata(metadata)
        opened_envelope = Envelope(
            session_id=session_id,
            sender_id=metadata.creator_id,
            audience=[p.agent_id for p in metadata.participants],
            event_type=EV_SESSION_OPENED,
            event_data={"session_id": session_id},
        )
        await self.post_envelope(opened_envelope)
        waiter = self._session_open_waiters.get(session_id)
        if waiter is not None and not waiter.done():
            waiter.set_result(metadata)

    async def _transition_session(
        self,
        session_id: str,
        new_state: SessionState,
        reason: str,
    ) -> None:
        metadata = self._sessions.get(session_id)
        if metadata is None or metadata.is_terminal():
            return

        # Cascade non-terminal tasks before flipping session state so
        # observers see ``ag2.task.expired`` before ``ag2.session.closed``.
        if is_terminal_session_state(new_state):
            for task_id in list(self._session_tasks.get(session_id, set())):
                task_meta = self._tasks.get(task_id)
                if task_meta is not None and task_meta.state not in TERMINAL_TASK_STATES:
                    await self._transition_task(task_id, TaskState.EXPIRED, "session_closed")

        metadata.state = new_state
        metadata.close_reason = reason
        if is_terminal_session_state(new_state):
            metadata.closed_at = self._clock()
            self._active_sessions.pop(session_id, None)

        await self._persist_session_metadata(metadata)

        if is_terminal_session_state(new_state):
            event_type = (
                EV_SESSION_EXPIRED if new_state == SessionState.EXPIRED else EV_SESSION_CLOSED
            )
            close_envelope = Envelope(
                session_id=session_id,
                sender_id=metadata.creator_id,
                audience=[p.agent_id for p in metadata.participants],
                event_type=event_type,
                event_data={"reason": reason, "session_id": session_id},
            )
            close_envelope.envelope_id = make_id()
            close_envelope.created_at = self._clock()
            async with self._wal_lock(session_id):
                await self._wal_append(close_envelope)
            await self._dispatch(close_envelope, metadata)

    async def _transition_task(
        self,
        task_id: str,
        new_state: TaskState,
        reason: str,
    ) -> None:
        metadata = self._tasks.get(task_id)
        if metadata is None or metadata.state in TERMINAL_TASK_STATES:
            return
        metadata.state = new_state
        if new_state in TERMINAL_TASK_STATES:
            metadata.completed_at = self._clock()
            if new_state == TaskState.EXPIRED:
                metadata.error = reason or metadata.error or "expired"
        await self._persist_task_metadata(metadata)

    # ── Persistence helpers ──────────────────────────────────────────────────

    async def _persist_passport(self, passport: Passport) -> None:
        assert passport.agent_id is not None
        await self._store.write(passport_path(passport.agent_id), json.dumps(passport.to_dict()))

    async def _persist_resume(self, agent_id: str, resume: Resume) -> None:
        await self._store.write(resume_path(agent_id), json.dumps(resume.to_dict()))

    async def _persist_rule(self, agent_id: str, rule: Rule) -> None:
        await self._store.write(rule_path(agent_id), json.dumps(rule.to_dict()))

    async def _persist_skill(self, agent_id: str, skill_md: str) -> None:
        await self._store.write(skill_path(agent_id), skill_md)

    async def _persist_session_metadata(self, metadata: SessionMetadata) -> None:
        await self._store.write(
            session_metadata_path(metadata.session_id),
            json.dumps(metadata.to_dict()),
        )

    async def _persist_task_metadata(self, metadata: TaskMetadata) -> None:
        await self._store.write(
            task_metadata_path(metadata.task_id),
            json.dumps(_task_metadata_to_dict(metadata)),
        )

    async def _load_agent(self, agent_id: str) -> None:
        passport_data = await self._store.read(passport_path(agent_id))
        if passport_data is None:
            return
        passport = Passport.from_dict(json.loads(passport_data))
        self._passports[agent_id] = passport
        self._name_to_id[passport.name] = agent_id

        resume_data = await self._store.read(resume_path(agent_id))
        if resume_data is not None:
            self._resumes[agent_id] = Resume.from_dict(json.loads(resume_data))

        rule_data = await self._store.read(rule_path(agent_id))
        if rule_data is not None:
            self._rules[agent_id] = Rule.from_dict(json.loads(rule_data))
        else:
            self._rules[agent_id] = Rule()

    async def _load_session(self, session_id: str) -> None:
        metadata_data = await self._store.read(session_metadata_path(session_id))
        if metadata_data is None:
            return
        metadata = SessionMetadata.from_dict(json.loads(metadata_data))
        self._sessions[session_id] = metadata
        if not metadata.is_terminal():
            self._active_sessions[session_id] = metadata

        adapter = self._adapters.get((metadata.manifest.type, metadata.manifest.version))
        if adapter is None:
            return  # adapter not registered; cannot fold

        state = adapter.initial_state(metadata)
        wal = await self.read_wal(session_id)
        for envelope in wal:
            state = adapter.fold(envelope, state)
        self._adapter_states[session_id] = state

    async def _load_task(self, task_id: str) -> None:
        metadata_data = await self._store.read(task_metadata_path(task_id))
        if metadata_data is None:
            return
        metadata = _task_metadata_from_dict(json.loads(metadata_data))
        self._tasks[task_id] = metadata
        if metadata.session_id:
            self._session_tasks.setdefault(metadata.session_id, set()).add(task_id)


def _task_metadata_to_dict(metadata: TaskMetadata) -> dict[str, object]:
    """Serialise framework-core ``TaskMetadata`` for hub persistence.

    Mirrors the dataclass shape but coerces ``state`` (an Enum) to a
    string and ``spec`` (a ``TaskSpec`` dataclass) to a dict.
    """
    return {
        "task_id": metadata.task_id,
        "owner_id": metadata.owner_id,
        "spec": {
            "title": metadata.spec.title,
            "description": metadata.spec.description,
            "payload": dict(metadata.spec.payload),
        },
        "state": metadata.state.value,
        "created_at": metadata.created_at,
        "started_at": metadata.started_at,
        "completed_at": metadata.completed_at,
        "expires_at": metadata.expires_at,
        "last_progress_at": metadata.last_progress_at,
        "progress": dict(metadata.progress),
        "result": metadata.result,
        "error": metadata.error,
        "session_id": metadata.session_id,
    }


def _task_metadata_from_dict(data: dict[str, object]) -> TaskMetadata:
    spec_data = data.get("spec") or {}
    if isinstance(spec_data, dict):
        spec = TaskSpec(
            title=str(spec_data.get("title", "")),
            description=str(spec_data.get("description", "")),
            payload=dict(spec_data.get("payload") or {}),  # type: ignore[arg-type]
        )
    else:
        spec = TaskSpec(title="")
    state_raw = data.get("state", TaskState.CREATED.value)
    state = TaskState(state_raw) if isinstance(state_raw, str) else state_raw
    return TaskMetadata(
        task_id=str(data["task_id"]),
        owner_id=str(data["owner_id"]),
        spec=spec,
        state=state,  # type: ignore[arg-type]
        created_at=str(data.get("created_at", "")),
        started_at=data.get("started_at"),  # type: ignore[arg-type]
        completed_at=data.get("completed_at"),  # type: ignore[arg-type]
        expires_at=data.get("expires_at"),  # type: ignore[arg-type]
        last_progress_at=data.get("last_progress_at"),  # type: ignore[arg-type]
        progress=dict(data.get("progress") or {}),  # type: ignore[arg-type]
        result=data.get("result"),
        error=str(data.get("error", "")),
        session_id=data.get("session_id"),  # type: ignore[arg-type]
    )
