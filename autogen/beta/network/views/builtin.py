# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Built-in ``ViewPolicy`` implementations.

V1 ships ``FullTranscript`` (M2) and ``WindowedSummary`` (M3).
``Composite`` is Phase 2.

``WindowedSummary`` keeps a bounded tail of recent ``EV_TEXT`` envelopes
and replaces older ones with a single :class:`CompactionSummary` event.
M3 ships a static no-LLM summary (sender ids + count); composing with
framework-core ``compact.SummarizeCompact`` for semantic summaries is
Phase 2.
"""

from autogen.beta.compact import CompactionSummary
from autogen.beta.events import BaseEvent, ModelMessage, ModelRequest, TextInput

from ..envelope import EV_TEXT, Envelope, visible_to
from ..session import SessionMetadata

__all__ = ("FullTranscript", "WindowedSummary")


class FullTranscript:
    """Translate every envelope visible to ``participant_id``.

    M2 projects only ``EV_TEXT`` envelopes — protocol-level events
    (``EV_SESSION_*``, ``EV_TASK_*``, expectation violations) are hub
    bookkeeping that the LLM doesn't need to reason about. The
    ``NetworkContextPolicy`` renders session expectations / active task
    metadata into the prompt prefix instead.

    Inbound envelopes (sender != participant) become ``ModelRequest``
    (a "user turn"); own past envelopes become ``ModelMessage``.
    """

    name = "full_transcript"

    async def project(
        self,
        wal: list[Envelope],
        *,
        participant_id: str,
        session: SessionMetadata,
    ) -> list[BaseEvent]:
        events: list[BaseEvent] = []
        for envelope in wal:
            if not visible_to(envelope, participant_id):
                continue
            if envelope.event_type != EV_TEXT:
                continue
            text = envelope.event_data.get("text", "")
            if not isinstance(text, str):
                continue
            if envelope.sender_id == participant_id:
                events.append(ModelMessage(text))
            else:
                events.append(ModelRequest([TextInput(text)]))
        return events


class WindowedSummary:
    """Keep the last ``recent_n`` visible ``EV_TEXT`` envelopes verbatim;
    fold everything older into a single :class:`CompactionSummary` at
    the head of the projection.

    Bounds prompt size at any turn count — the projection is at most
    ``recent_n + 1`` events regardless of WAL length. ``CompactionSummary``
    is recognised by ``autogen/beta/policies/conversation.py`` so it
    renders correctly in the LLM-facing message stream.

    M3 generates a static stat-style summary
    (``"Earlier in this session: N messages from a, b."``) without an
    LLM call. Phase 2 makes the compactor pluggable so callers can pass
    a ``CompactStrategy`` (e.g. ``SummarizeCompact``) to produce
    semantic summaries.
    """

    name = "windowed_summary"

    def __init__(self, recent_n: int) -> None:
        if recent_n < 1:
            raise ValueError(f"recent_n must be >= 1, got {recent_n}")
        self._recent_n = recent_n

    @property
    def recent_n(self) -> int:
        return self._recent_n

    async def project(
        self,
        wal: list[Envelope],
        *,
        participant_id: str,
        session: SessionMetadata,
    ) -> list[BaseEvent]:
        visible: list[Envelope] = []
        for envelope in wal:
            if not visible_to(envelope, participant_id):
                continue
            if envelope.event_type != EV_TEXT:
                continue
            text = envelope.event_data.get("text", "")
            if not isinstance(text, str):
                continue
            visible.append(envelope)

        if len(visible) <= self._recent_n:
            return [_to_event(e, participant_id) for e in visible]

        cutoff = len(visible) - self._recent_n
        older = visible[:cutoff]
        recent = visible[cutoff:]
        summary = _summarize_older(older)
        compaction = CompactionSummary(summary=summary, event_count=len(older))
        return [compaction, *(_to_event(e, participant_id) for e in recent)]


def _to_event(envelope: Envelope, participant_id: str) -> BaseEvent:
    text = envelope.event_data.get("text", "")
    if envelope.sender_id == participant_id:
        return ModelMessage(text)
    return ModelRequest([TextInput(text)])


def _summarize_older(older: list[Envelope]) -> str:
    speakers = sorted({e.sender_id for e in older})
    plural = "s" if len(older) != 1 else ""
    return (
        f"Earlier in this session: {len(older)} message{plural} "
        f"from {', '.join(speakers)}."
    )
