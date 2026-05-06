# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Built-in expectation evaluators + violation handlers.

Expectations are protocol-shape contracts declared on
``SessionManifest.expectations``. The hub evaluates them on a periodic
sweeper tick — independently of the per-envelope ``validate_send`` /
``on_accepted`` path. ``validate_send`` rejects bad **sends**;
expectations react to bad **silence** (or bad pacing).

Evaluators
----------
* ``acks_within(seconds)`` — invitee hasn't ack'd or rejected within T
  after ``EV_SESSION_INVITE``. Fires only while the session is PENDING.
* ``reply_within(seconds)`` — a participant with envelopes addressed
  to them hasn't sent a response within T.
* ``max_silence(seconds)`` — session has had no content envelopes from
  anyone for T.

Handlers
--------
* ``audit`` — record an entry in ``audit.jsonl``; no envelope sent.
* ``notify_session`` — audit + post ``EV_EXPECTATION_VIOLATED`` to
  every participant.
* ``auto_close`` — audit + transition the session to ``CLOSED`` with
  ``close_reason="expectation_violated:{name}"``.

All handlers are passive: the hub records, signals, or closes; it
never re-tries, substitutes content, or makes outcome decisions for
the agent.
"""

import contextlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from ..envelope import EV_EXPECTATION_VIOLATED, EV_TEXT, Envelope
from ..session import Expectation, SessionMetadata, SessionState
from .audit import AUDIT_KIND_EXPECTATION_VIOLATED

EV_PARTICIPANT_REMOVED = "ag2.participant.removed"

if TYPE_CHECKING:
    from .core import Hub

__all__ = (
    "AcksWithinEvaluator",
    "AuditHandler",
    "AutoCloseHandler",
    "ExpectationContext",
    "ExpectationEvaluator",
    "HideHandler",
    "MaxSilenceEvaluator",
    "MinParticipationEvaluator",
    "NotifySessionHandler",
    "ProgressWithinEvaluator",
    "RemoveHandler",
    "ReplyWithinEvaluator",
    "TurnWithinEvaluator",
    "Violation",
    "ViolationHandler",
    "WarnHandler",
    "default_evaluators",
    "default_handlers",
)


@dataclass(slots=True)
class Violation:
    """Result of an evaluator firing.

    ``violator_ids`` is the list of participants the violation applies
    to; an empty list represents a session-wide violation (e.g.
    ``max_silence`` — nobody specifically is silent, the session is).
    """

    expectation: Expectation
    violator_ids: list[str] = field(default_factory=list)
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExpectationContext:
    """Inputs handed to every evaluator on each sweeper tick."""

    metadata: SessionMetadata
    state: object  # opaque AdapterState
    wal: list[Envelope]
    now_iso: str
    now_seconds: float


class ExpectationEvaluator(Protocol):
    """Pure predicate over ``(metadata, state, wal, clock)``.

    Returns ``None`` for "no violation right now." Evaluators must be
    deterministic functions of their inputs — the sweeper is called on
    a periodic tick, so non-deterministic evaluators would flap.
    """

    name: str

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None: ...


class ViolationHandler(Protocol):
    """What the hub does when an evaluator fires.

    Handlers are async because they may post envelopes or transition
    sessions. They must be tolerant of duplicate calls — the sweeper
    deduplicates per (session, expectation, violator) before invoking,
    but transient re-fires across hub restarts are possible since the
    fired-violation cache is in-memory only.
    """

    name: str

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None: ...


# ── Evaluators ──────────────────────────────────────────────────────────────


def _parse_iso_seconds(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


def _is_content_event(event_type: str) -> bool:
    """Substantive (non-protocol, non-task) envelope."""
    if event_type.startswith("ag2.session."):
        return False
    if event_type.startswith("ag2.task."):
        return False
    return event_type != EV_EXPECTATION_VIOLATED


class AcksWithinEvaluator:
    """Fire when invitees haven't acked within ``params.seconds``.

    Only meaningful while the session is in ``PENDING`` state. Returns
    one violation listing every still-pending invitee.
    """

    name = "acks_within"

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None:
        if context.metadata.state != SessionState.PENDING:
            return None
        seconds = float(expectation.params.get("seconds", 30))
        elapsed = context.now_seconds - _parse_iso_seconds(context.metadata.created_at)
        if elapsed < seconds:
            return None
        if not context.metadata.pending_acks:
            return None
        return Violation(
            expectation=expectation,
            violator_ids=list(context.metadata.pending_acks),
            detail={
                "elapsed_seconds": elapsed,
                "threshold_seconds": seconds,
            },
        )


class ReplyWithinEvaluator:
    """Fire when a participant addressed by a send hasn't replied in T.

    For each participant, finds the most recent ``EV_TEXT`` envelope
    addressed to them (broadcast or via ``audience``). If they haven't
    sent any ``EV_TEXT`` after that timestamp, and that timestamp is
    older than ``params.seconds``, they're a violator.
    """

    name = "reply_within"

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None:
        if context.metadata.state != SessionState.ACTIVE:
            return None
        seconds = float(expectation.params.get("seconds", 600))

        # Index: latest EV_TEXT addressed to each participant + each
        # participant's latest sent EV_TEXT.
        latest_in: dict[str, Envelope] = {}
        latest_out: dict[str, Envelope] = {}
        for env in context.wal:
            if env.event_type != EV_TEXT:
                continue
            sender = env.sender_id
            latest_out[sender] = env
            for p in context.metadata.participants:
                pid = p.agent_id
                if pid == sender:
                    continue
                if env.audience is not None and pid not in env.audience:
                    continue
                latest_in[pid] = env

        violators: list[str] = []
        for p in context.metadata.participants:
            pid = p.agent_id
            inbound = latest_in.get(pid)
            if inbound is None:
                continue
            outbound = latest_out.get(pid)
            inbound_at = _parse_iso_seconds(inbound.created_at)
            if outbound is not None:
                outbound_at = _parse_iso_seconds(outbound.created_at)
                if outbound_at >= inbound_at:
                    continue
            if context.now_seconds - inbound_at >= seconds:
                violators.append(pid)

        if not violators:
            return None
        return Violation(
            expectation=expectation,
            violator_ids=violators,
            detail={"threshold_seconds": seconds},
        )


class MaxSilenceEvaluator:
    """Fire when the session has had no content envelope for T.

    Session-wide — ``violator_ids`` is empty. Anchors on the latest
    content envelope's timestamp, falling back to session creation
    when no content has been posted.
    """

    name = "max_silence"

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None:
        if context.metadata.state != SessionState.ACTIVE:
            return None
        seconds = float(expectation.params.get("seconds", 3600))
        anchor_iso = context.metadata.created_at
        for env in reversed(context.wal):
            if _is_content_event(env.event_type):
                anchor_iso = env.created_at
                break
        elapsed = context.now_seconds - _parse_iso_seconds(anchor_iso)
        if elapsed < seconds:
            return None
        return Violation(
            expectation=expectation,
            violator_ids=[],
            detail={
                "elapsed_seconds": elapsed,
                "threshold_seconds": seconds,
            },
        )


class TurnWithinEvaluator:
    """Fire when an adapter's expected next speaker hasn't posted
    within T seconds of the trigger envelope.

    Reads ``state.expected_next_speaker`` opportunistically — works for
    workflow and round-robin discussion adapters that track this
    explicitly. Adapters without that attribute (consulting,
    conversation) fall through with no violation, since their turn
    semantics are already covered by ``reply_within``.

    Anchors on the latest content envelope sent by someone other than
    the expected speaker — the trigger that put them on the hook.
    """

    name = "turn_within"

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None:
        if context.metadata.state != SessionState.ACTIVE:
            return None
        expected = getattr(context.state, "expected_next_speaker", None)
        if not isinstance(expected, str) or not expected:
            return None
        seconds = float(expectation.params.get("seconds", 120))

        # Find the most recent inbound trigger (content envelope from
        # someone other than the expected speaker). If the most recent
        # content envelope is from the expected speaker themselves,
        # they're caught up — no violation.
        trigger_iso: str | None = None
        for env in reversed(context.wal):
            if not _is_content_event(env.event_type):
                continue
            if env.sender_id == expected:
                return None
            trigger_iso = env.created_at
            break
        if trigger_iso is None:
            # No inbound trigger yet (e.g. session just opened) — give
            # the expected speaker the same threshold from session
            # creation so initiator turns are also covered.
            trigger_iso = context.metadata.created_at
        elapsed = context.now_seconds - _parse_iso_seconds(trigger_iso)
        if elapsed < seconds:
            return None
        return Violation(
            expectation=expectation,
            violator_ids=[expected],
            detail={
                "elapsed_seconds": elapsed,
                "threshold_seconds": seconds,
            },
        )


class ProgressWithinEvaluator:
    """Fire when an active task has had no progress for T.

    Pure-WAL evaluator: walks task envelopes (``ag2.task.started`` /
    ``ag2.task.progress``), groups by ``task_id``, and fires per-task
    violations on tasks that have started, haven't terminated, and
    whose most recent progress (or start) is older than T.
    """

    name = "progress_within"

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None:
        if context.metadata.state != SessionState.ACTIVE:
            return None
        seconds = float(expectation.params.get("seconds", 60))

        # task_id → (latest_progress_iso, owner_id, terminal?)
        tasks: dict[str, tuple[str, str, bool]] = {}
        for env in context.wal:
            tid = env.task_id
            if not tid:
                continue
            etype = env.event_type
            if etype == "ag2.task.started":
                tasks[tid] = (env.created_at, env.sender_id, False)
            elif etype == "ag2.task.progress":
                prev = tasks.get(tid)
                owner = prev[1] if prev else env.sender_id
                tasks[tid] = (env.created_at, owner, False)
            elif etype in ("ag2.task.completed", "ag2.task.failed", "ag2.task.expired"):
                prev = tasks.get(tid)
                if prev is not None:
                    tasks[tid] = (prev[0], prev[1], True)

        violators: list[str] = []
        latest_elapsed = 0.0
        for tid, (last_iso, owner, terminal) in tasks.items():
            if terminal:
                continue
            elapsed = context.now_seconds - _parse_iso_seconds(last_iso)
            if elapsed >= seconds:
                violators.append(owner)
                latest_elapsed = max(latest_elapsed, elapsed)

        if not violators:
            return None
        return Violation(
            expectation=expectation,
            violator_ids=violators,
            detail={
                "elapsed_seconds": latest_elapsed,
                "threshold_seconds": seconds,
                "stalled_tasks": sorted(
                    tid for tid, (_, _, term) in tasks.items() if not term
                ),
            },
        )


class MinParticipationEvaluator:
    """Fire when a participant posted fewer than ``count`` content
    envelopes in the last ``window_seconds``.

    Defaults: ``count=1``, ``window_seconds=600``. Useful for
    discussion-style sessions where every voice should be heard.
    Initiator-only or single-recipient adapters typically skip this
    expectation in their manifest — the violation is uninteresting
    when the protocol structurally expects asymmetric participation.
    """

    name = "min_participation"

    def evaluate(
        self,
        expectation: Expectation,
        context: ExpectationContext,
    ) -> Violation | None:
        if context.metadata.state != SessionState.ACTIVE:
            return None
        count = int(expectation.params.get("count", 1))
        window = float(expectation.params.get("window_seconds", 600))
        cutoff = context.now_seconds - window

        # Count substantive sends per participant within the window.
        sends: dict[str, int] = {p.agent_id: 0 for p in context.metadata.participants}
        for env in context.wal:
            if not _is_content_event(env.event_type):
                continue
            sender = env.sender_id
            if sender not in sends:
                continue
            if _parse_iso_seconds(env.created_at) >= cutoff:
                sends[sender] += 1

        violators = sorted(pid for pid, n in sends.items() if n < count)
        if not violators:
            return None
        return Violation(
            expectation=expectation,
            violator_ids=violators,
            detail={
                "count_threshold": count,
                "window_seconds": window,
                "actual": {pid: sends[pid] for pid in violators},
            },
        )


# ── Handlers ────────────────────────────────────────────────────────────────


async def _audit_violation(
    hub: "Hub",
    session_id: str,
    violation: Violation,
) -> None:
    await hub._audit_log.append({
        "at": hub._clock(),
        "kind": AUDIT_KIND_EXPECTATION_VIOLATED,
        "session_id": session_id,
        "expectation": violation.expectation.name,
        "on_violation": violation.expectation.on_violation,
        "params": dict(violation.expectation.params),
        "violators": list(violation.violator_ids),
        "detail": dict(violation.detail),
    })


class AuditHandler:
    """Record the violation; no envelope, no state change."""

    name = "audit"

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None:
        await _audit_violation(hub, session_id, violation)


class NotifySessionHandler:
    """Audit + broadcast ``EV_EXPECTATION_VIOLATED`` to every participant."""

    name = "notify_session"

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None:
        await _audit_violation(hub, session_id, violation)
        metadata = hub._sessions.get(session_id)
        if metadata is None or metadata.is_terminal():
            return
        envelope = Envelope(
            session_id=session_id,
            sender_id=metadata.creator_id,
            audience=None,
            event_type=EV_EXPECTATION_VIOLATED,
            event_data={
                "expectation": violation.expectation.name,
                "violators": list(violation.violator_ids),
                "detail": dict(violation.detail),
            },
        )
        # Posting violations is best-effort — a closed/closing
        # session shouldn't crash the sweeper.
        with contextlib.suppress(Exception):
            await hub.post_envelope(envelope)


class AutoCloseHandler:
    """Audit + transition the session to ``CLOSED``."""

    name = "auto_close"

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None:
        await _audit_violation(hub, session_id, violation)
        metadata = hub._sessions.get(session_id)
        if metadata is None or metadata.is_terminal():
            return
        with contextlib.suppress(Exception):
            await hub.close_session(
                session_id,
                reason=f"expectation_violated:{violation.expectation.name}",
            )


class WarnHandler:
    """Audit + emit ``EV_EXPECTATION_VIOLATED`` to the violator(s)
    only.

    Differs from ``notify_session`` (broadcast): ``warn`` is targeted,
    so the offending participant gets the signal without spamming the
    whole session. Falls back to a broadcast for session-wide
    violations (``violator_ids=[]``) since there's no specific target.
    """

    name = "warn"

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None:
        await _audit_violation(hub, session_id, violation)
        metadata = hub._sessions.get(session_id)
        if metadata is None or metadata.is_terminal():
            return
        audience = list(violation.violator_ids) or None
        envelope = Envelope(
            session_id=session_id,
            sender_id=metadata.creator_id,
            audience=audience,
            event_type=EV_EXPECTATION_VIOLATED,
            event_data={
                "expectation": violation.expectation.name,
                "violators": list(violation.violator_ids),
                "detail": dict(violation.detail),
            },
        )
        with contextlib.suppress(Exception):
            await hub.post_envelope(envelope)


class HideHandler:
    """Audit + suppress live notifies to the violator.

    The violator's WAL view is unaffected (audit truth is unchanged);
    only their live ``notify`` deliveries are dropped. Useful for
    silently sidelining a slow / disruptive participant without
    closing the session. In-memory only — a hub restart drops the
    flag.
    """

    name = "hide"

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None:
        await _audit_violation(hub, session_id, violation)
        for vid in violation.violator_ids:
            hub.mark_hidden(session_id, vid)


class RemoveHandler:
    """Audit + bar the violator from sending into the session.

    Posts ``ag2.participant.removed`` with the offending agent_id and
    reason so peers can react. The bar is persisted to
    ``sessions/{id}/removed.json`` and reapplied on hub restart.

    Does NOT mutate ``metadata.participants`` — that would desync the
    adapter's folded state (which still references the removed
    participant in ``participant_order``). Instead, ``post_envelope``
    rejects substantive sends from removed agents at the access layer.
    """

    name = "remove"

    async def handle(
        self,
        hub: "Hub",
        session_id: str,
        violation: Violation,
    ) -> None:
        await _audit_violation(hub, session_id, violation)
        metadata = hub._sessions.get(session_id)
        if metadata is None or metadata.is_terminal():
            return
        for vid in violation.violator_ids:
            await hub.mark_removed(session_id, vid)
            envelope = Envelope(
                session_id=session_id,
                sender_id=metadata.creator_id,
                audience=None,
                event_type=EV_PARTICIPANT_REMOVED,
                event_data={
                    "agent_id": vid,
                    "reason": f"expectation_violated:{violation.expectation.name}",
                },
            )
            with contextlib.suppress(Exception):
                await hub.post_envelope(envelope)


# ── Factories ───────────────────────────────────────────────────────────────


def default_evaluators() -> list[ExpectationEvaluator]:
    return [
        AcksWithinEvaluator(),
        ReplyWithinEvaluator(),
        MaxSilenceEvaluator(),
        TurnWithinEvaluator(),
        ProgressWithinEvaluator(),
        MinParticipationEvaluator(),
    ]


def default_handlers() -> list[ViolationHandler]:
    return [
        AuditHandler(),
        NotifySessionHandler(),
        AutoCloseHandler(),
        WarnHandler(),
        HideHandler(),
        RemoveHandler(),
    ]
