# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``AgentClient`` — per-registration tenant handle.

M2 surface:

* Properties (agent, passport, resume, agent_id).
* ``receive`` (NetworkClient impl) — routes envelopes to the optional
  per-session inbox queue (used by ``delegate``) AND to the registered
  notify-handler callback (default = ``handlers.default_handler``,
  which auto-acks invites and runs ``Agent.ask`` on text envelopes).
* ``send_envelope`` — direct ``Hub.post_envelope`` call.
* ``open(type=..., target=..., ...)`` — create a session via the hub;
  returns a :class:`Session` handle.
* ``wait_for_session_event`` — block until an inbound envelope on a
  session matches a predicate; used by ``delegate`` to await replies.
* Tenant-driven mutation (``set_resume`` / ``set_skill`` / ``set_rule``).
* ``on_envelope(callback)`` — override the default notify handler
  (testing seam; M3 replaces with the per-session-type registry).

The ``NetworkPlugin`` is attached at registration by ``HubClient`` so
``agent.tools`` includes ``say`` / ``delegate`` and the assembly chain
includes ``NetworkContextPolicy``.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from autogen.beta.agent import Agent

from ..envelope import Envelope
from ..identity import Passport, Resume, ResumeExample
from ..rule import Rule
from .handlers import default_handler
from .session import Session

if TYPE_CHECKING:
    from ..hub import Hub
    from .hub_client import HubClient

__all__ = ("AgentClient",)


EnvelopeHandler = Callable[[Envelope], Awaitable[None]]
EnvelopePredicate = Callable[[Envelope], bool]


class AgentClient:
    """Tenant-side handle for one ``(Agent, identity, hub)`` registration."""

    def __init__(
        self,
        *,
        agent: Agent,
        passport: Passport,
        resume: Resume,
        rule: Rule,
        hub: "Hub",
        hub_client: "HubClient",
        attach_default_handler: bool = True,
    ) -> None:
        # __init__ stores params; no side effects.
        self._agent = agent
        self._passport = passport
        self._resume = resume
        self._rule = rule
        self._hub = hub
        self._hub_client = hub_client
        self._on_envelope: EnvelopeHandler | None = (
            self._run_default_handler if attach_default_handler else None
        )
        self._disconnected = False

        # Per-session inbox queues for ``wait_for_session_event``
        # (used by the ``delegate`` tool to await consulting replies).
        self._session_inboxes: dict[str, "asyncio.Queue[Envelope]"] = {}

        # Sessions where the default notify handler should NOT run —
        # used by ``delegate`` while it owns the session lifecycle.
        self._handler_suppressed_sessions: set[str] = set()

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def agent(self) -> Agent:
        return self._agent

    @property
    def passport(self) -> Passport:
        return self._passport

    @property
    def resume(self) -> Resume:
        return self._resume

    @property
    def rule(self) -> Rule:
        return self._rule

    @property
    def agent_id(self) -> str:
        if self._passport.agent_id is None:
            raise RuntimeError("AgentClient has unstamped passport (not registered)")
        return self._passport.agent_id

    # ── NetworkClient impl ───────────────────────────────────────────────────

    async def receive(self, envelope: Envelope) -> None:
        """Hub delivery → fan out to inbox + (suppressible) handler."""
        inbox = self._session_inboxes.get(envelope.session_id)
        if inbox is not None:
            await inbox.put(envelope)
        if envelope.session_id in self._handler_suppressed_sessions:
            return
        if self._on_envelope is not None:
            await self._on_envelope(envelope)

    def on_envelope(self, callback: EnvelopeHandler) -> None:
        """Override the default notify handler with a custom callback.

        M3 replaces this with the ``@client.on(session_type)`` registry.
        Calling with the default handler restores it: pass
        ``self._run_default_handler`` (or simply construct without
        ``attach_default_handler=False``).
        """
        self._on_envelope = callback

    async def disconnect(self) -> None:
        self._disconnected = True
        self._on_envelope = None

    async def _run_default_handler(self, envelope: Envelope) -> None:
        """Bound-method wrapper around :func:`handlers.default_handler`."""
        await default_handler(envelope, self)

    # ── Session lifecycle ────────────────────────────────────────────────────

    async def open(
        self,
        *,
        type: str,
        target: str | list[str],
        ttl: str | int | None = None,
        knobs: dict[str, object] | None = None,
        intent: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> Session:
        """Open a session via the hub and return its :class:`Session` handle.

        ``target`` accepts peer **names** or agent_ids; this method
        resolves names via ``hub.get_agent``. Awaits the hub's
        invite/ack handshake before returning.
        """
        if self._disconnected:
            raise RuntimeError("AgentClient is disconnected")

        targets = [target] if isinstance(target, str) else list(target)
        target_ids: list[str] = []
        for t in targets:
            passport = await self._hub.get_agent(t)
            if passport.agent_id is None:
                raise RuntimeError(f"target {t!r} has no agent_id")
            target_ids.append(passport.agent_id)

        metadata = await self._hub.create_session(
            creator_id=self.agent_id,
            manifest_type=type,
            participants=target_ids,
            ttl=ttl,
            knobs=knobs,
            intent=intent,
            labels=labels,
        )
        return Session(metadata=metadata, client=self)

    async def wait_for_session_event(
        self,
        *,
        session_id: str,
        predicate: EnvelopePredicate,
        timeout: float = 300.0,
    ) -> Envelope:
        """Block until an inbound envelope on ``session_id`` matches.

        Used by ``delegate`` to await the consulting respondent's
        reply. The inbox is created on demand and shared across waits;
        callers should not hold multiple concurrent waits on the same
        session in M2.

        Raises ``asyncio.TimeoutError`` on timeout.
        """
        inbox = self._session_inboxes.get(session_id)
        if inbox is None:
            inbox = asyncio.Queue()
            self._session_inboxes[session_id] = inbox

        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            envelope = await asyncio.wait_for(inbox.get(), timeout=remaining)
            if predicate(envelope):
                return envelope

    def _suppress_handler(self, session_id: str) -> None:
        """Internal: stop running the default notify handler for ``session_id``.

        Used by ``delegate`` to own the session lifecycle while waiting
        for the respondent's reply — the default handler would
        otherwise try to ``Agent.ask`` on every inbound EV_TEXT.
        """
        self._handler_suppressed_sessions.add(session_id)

    def _unsuppress_handler(self, session_id: str) -> None:
        self._handler_suppressed_sessions.discard(session_id)

    # ── Envelope send ────────────────────────────────────────────────────────

    async def send_envelope(self, envelope: Envelope) -> str:
        """Post an envelope through the hub. Returns the stamped envelope_id."""
        if self._disconnected:
            raise RuntimeError("AgentClient is disconnected")
        if envelope.sender_id == "":
            envelope.sender_id = self.agent_id
        return await self._hub.post_envelope(envelope)

    # ── Tenant-driven mutation ───────────────────────────────────────────────

    async def set_resume(self, resume: Resume) -> None:
        await self._hub.set_resume(self.agent_id, resume)
        # Refresh local cache so subsequent reads see the bumped version.
        self._resume = await self._hub.get_resume(self.agent_id)

    async def add_example(self, example: ResumeExample) -> None:
        """Append a ``ResumeExample`` to this agent's resume.

        Fetches the latest resume from the hub first so concurrent
        ``set_resume`` / ``record_observation`` updates don't get
        clobbered.
        """
        current = await self._hub.get_resume(self.agent_id)
        current.examples.append(example)
        await self.set_resume(current)

    async def set_skill(self, skill_md: str | None) -> None:
        await self._hub.set_skill(self.agent_id, skill_md)

    async def set_rule(self, rule: Rule) -> None:
        await self._hub.set_rule(self.agent_id, rule)
        self._rule = rule

    async def unregister(self) -> None:
        if not self._disconnected:
            await self._hub.unregister(self.agent_id)
            self._disconnected = True
