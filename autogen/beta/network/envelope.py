# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Envelope — the wire shape for every message between agents.

Every Agent-to-Agent exchange (post-M2) happens inside a session and
the carrier is an ``Envelope``. Envelopes are JSON-serialisable, hub-
stamped at ``post_envelope``, and persisted to the per-session WAL.
``audience`` is the addressing primitive: ``None`` broadcasts within
the session, a list targets a subset.

Streaming chunks use a separate transport-level ``chunk`` frame (Phase
2 surface) and are not envelopes — they bypass the WAL entirely.
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

__all__ = (
    "EV_EXPECTATION_VIOLATED",
    "EV_HANDOFF",
    "EV_SESSION_CLOSED",
    "EV_SESSION_EXPIRED",
    "EV_SESSION_INVITE",
    "EV_SESSION_INVITE_ACK",
    "EV_SESSION_INVITE_REJECT",
    "EV_SESSION_OPENED",
    "EV_TEXT",
    "Envelope",
    "Priority",
    "visible_to",
)


Priority = Literal["background", "normal", "urgent"]


# ── Stable event-type names ──────────────────────────────────────────────────
# V1 ships a fixed set; new names are added in code, not at runtime. User-
# defined event types may be posted with arbitrary strings (no namespace
# check in V1) — the framework only special-cases the names below.

EV_TEXT = "ag2.msg.text"

# Tool-driven workflow transition signal — see workflow.md. ``event_data``
# carries ``{"tool": <tool_name>, "reason": <free-form>}``. Read by
# ``WorkflowAdapter``'s ``ToolCalled`` condition. Adapter-agnostic — any
# future adapter that wants tool-driven transitions reads it the same way.
EV_HANDOFF = "ag2.handoff"

EV_SESSION_INVITE = "ag2.session.invite"
EV_SESSION_INVITE_ACK = "ag2.session.invite.ack"
EV_SESSION_INVITE_REJECT = "ag2.session.invite.reject"
EV_SESSION_OPENED = "ag2.session.opened"
EV_SESSION_CLOSED = "ag2.session.closed"
EV_SESSION_EXPIRED = "ag2.session.expired"

EV_EXPECTATION_VIOLATED = "ag2.expectation.violated"

# Phase 2/3 event types removed from V1: ``EV_SESSION_IDLE``,
# ``EV_SESSION_QUORUM_CHANGED``, ``EV_TASK_*``, ``EV_PEER_*``,
# ``EV_PARTICIPANT_REMOVED``, ``EV_ERROR``. None of them were emitted
# by the hub. ``max_silence`` expectations cover idle-detection;
# task lifecycle is mirrored as Python events on the agent's own
# stream (see :mod:`autogen.beta.network.task_mirror`); peer
# reachability needs the WebSocket transport (Phase 3); participant
# removal needs the ``remove`` violation handler (Phase 2). Re-add
# the constant in the same milestone the producer ships.


@dataclass(slots=True)
class Envelope:
    """Wire shape for every Agent-to-Agent message.

    Field semantics:

    * ``envelope_id`` — hub-stamped on accept (UUID7-like). Sender-side
      construction leaves this empty; ``Hub.post_envelope`` populates.
    * ``audience`` — ``None`` broadcasts within the session; a list
      targets a subset. Hub WAL stores the full envelope regardless of
      addressing (audit + debug); ``notify`` lands only on listed peers.
    * ``causation_id`` — envelope this is responding to. Used by view
      policies to thread replies to their prompts.
    * ``depth`` — delegation hop count. Hub auto-increments on the
      reply path; ``Rule.limits.delegation_depth`` caps it.
    * ``ttl_seconds`` — per-envelope TTL. ``None`` defers to the
      session's ``expires_at``.
    * ``idempotency_key`` — Phase 3 dedup key; ignored in V1.
    """

    session_id: str
    sender_id: str
    audience: list[str] | None
    event_type: str
    event_data: dict[str, Any]

    envelope_id: str = ""  # hub-stamped on accept
    task_id: str | None = None
    causation_id: str | None = None
    trace_id: str | None = None
    priority: Priority = "normal"
    depth: int = 0
    idempotency_key: str | None = None  # Phase 3

    created_at: str = ""  # ISO-Z, hub-stamped on accept
    ttl_seconds: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-compatible dict (every field round-trips byte-stable)."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialise to JSON. Sort keys so cross-process hashes match."""
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Envelope":
        return cls(**data)

    @classmethod
    def from_json(cls, text: str) -> "Envelope":
        return cls.from_dict(json.loads(text))


def visible_to(envelope: Envelope, participant_id: str) -> bool:
    """Pure delivery / view-filtering predicate.

    Sender always sees their own envelope; broadcasts (``audience=None``)
    are visible to all session participants; subset addressing is
    visible only to listed peers.
    """
    if envelope.sender_id == participant_id:
        return True
    if envelope.audience is None:
        return True
    return participant_id in envelope.audience
