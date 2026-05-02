# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Built-in ``ViewPolicy`` implementations.

V1 (M2) ships ``FullTranscript`` only. M3 adds ``WindowedSummary``
(composes with framework-core ``compact.py``). ``Composite`` is
Phase 2.
"""

from autogen.beta.events import BaseEvent, ModelMessage, ModelRequest, TextInput

from ..envelope import EV_TEXT, Envelope, visible_to
from ..session import SessionMetadata

__all__ = ("FullTranscript",)


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
