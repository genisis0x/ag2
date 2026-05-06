# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Cut 3.3 — optional ``dispatch_audience`` adapter hook.

Adapters may implement::

    def dispatch_audience(envelope, metadata, state) -> list[str] | None

to narrow per-recipient delivery. ``WorkflowAdapter`` uses it to skip
wire round-trips for participants who aren't the
``expected_next_speaker``. Other adapters (consulting / conversation /
discussion) keep the broadcast default by not implementing the hook.

Coverage:

* ``WorkflowAdapter.dispatch_audience`` returns the next speaker for
  substantive envelopes
* returns ``None`` for protocol envelopes / when sender == next speaker
* returns ``None`` when the envelope already specifies an audience
* hub honours the override and only notifies the narrowed set
* discussion / conversation adapters don't implement the hook —
  broadcast continues for them
"""

from typing import Any

import pytest

from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_TEXT,
    Envelope,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.adapters.workflow import (
    WORKFLOW_TYPE,
    WorkflowAdapter,
    WorkflowState,
)
from autogen.beta.network.envelope import EV_HANDOFF, EV_SESSION_OPENED
from autogen.beta.network.session import (
    Participant,
    ParticipantRole,
    SessionManifest,
    SessionMetadata,
    SessionState,
)
from autogen.beta.network.transitions import (
    Always,
    AgentTarget,
    RoundRobinTarget,
    Transition,
    TransitionGraph,
)


def _participants(*names: str) -> list[Participant]:
    return [
        Participant(agent_id=name, role=ParticipantRole.PARTICIPANT, order=i)
        for i, name in enumerate(names)
    ]


def _metadata(graph: TransitionGraph, participants: list[Participant]) -> SessionMetadata:
    return SessionMetadata(
        session_id="sess-1",
        manifest=SessionManifest(type=WORKFLOW_TYPE, version=1),
        creator_id=participants[0].agent_id,
        participants=participants,
        state=SessionState.ACTIVE,
        knobs={"graph": graph.to_dict()},
        created_at="2026-05-06T00:00:00+00:00",
    )


def test_workflow_audience_narrows_to_next_speaker() -> None:
    graph = TransitionGraph(
        initial_speaker="alice",
        transitions=[Transition(when=Always(), then=AgentTarget("bob"))],
        default_target=AgentTarget("bob"),
    )
    adapter = WorkflowAdapter()
    metadata = _metadata(graph, _participants("alice", "bob", "carol"))
    state = adapter.initial_state(metadata)
    env = Envelope(
        session_id="sess-1",
        sender_id="alice",
        audience=None,
        event_type=EV_TEXT,
        event_data={"text": "hi"},
        envelope_id="env-1",
    )
    new_state = adapter.fold(env, state)

    audience = adapter.dispatch_audience(env, metadata, new_state)
    assert audience == ["bob"]


def test_workflow_audience_passthrough_for_protocol_envelope() -> None:
    graph = TransitionGraph(
        initial_speaker="alice",
        transitions=[Transition(when=Always(), then=AgentTarget("bob"))],
        default_target=AgentTarget("bob"),
    )
    adapter = WorkflowAdapter()
    metadata = _metadata(graph, _participants("alice", "bob"))
    state = adapter.initial_state(metadata)

    opened = Envelope(
        session_id="sess-1",
        sender_id="alice",
        audience=None,
        event_type=EV_SESSION_OPENED,
        event_data={},
        envelope_id="env-open",
    )
    assert adapter.dispatch_audience(opened, metadata, state) is None


def test_workflow_audience_passthrough_when_envelope_specifies_audience() -> None:
    graph = TransitionGraph(
        initial_speaker="alice",
        transitions=[Transition(when=Always(), then=AgentTarget("bob"))],
        default_target=AgentTarget("bob"),
    )
    adapter = WorkflowAdapter()
    metadata = _metadata(graph, _participants("alice", "bob", "carol"))
    state = adapter.initial_state(metadata)

    env = Envelope(
        session_id="sess-1",
        sender_id="alice",
        audience=["carol"],  # explicit audience
        event_type=EV_TEXT,
        event_data={"text": "for carol only"},
        envelope_id="env-1",
    )
    new_state = adapter.fold(env, state)

    # Explicit audience wins over the workflow's narrowing.
    assert adapter.dispatch_audience(env, metadata, new_state) is None


def test_workflow_audience_passthrough_when_no_next_speaker() -> None:
    """Terminated workflow has ``expected_next_speaker == None``."""
    adapter = WorkflowAdapter()
    state = WorkflowState(
        participant_order=["alice", "bob"],
        expected_next_speaker=None,
        creator_id="alice",
        graph_data={},
    )
    metadata = _metadata(
        TransitionGraph(
            initial_speaker="alice",
            transitions=[],
            default_target=AgentTarget("alice"),
        ),
        _participants("alice", "bob"),
    )
    env = Envelope(
        session_id="sess-1",
        sender_id="alice",
        audience=None,
        event_type=EV_TEXT,
        event_data={"text": "x"},
        envelope_id="env-1",
    )
    assert adapter.dispatch_audience(env, metadata, state) is None


def test_workflow_audience_handoff_narrows_too() -> None:
    """``EV_HANDOFF`` is substantive and gets narrowed too."""
    graph = TransitionGraph(
        initial_speaker="alice",
        transitions=[Transition(when=Always(), then=AgentTarget("bob"))],
        default_target=AgentTarget("bob"),
    )
    adapter = WorkflowAdapter()
    metadata = _metadata(graph, _participants("alice", "bob", "carol"))
    state = adapter.initial_state(metadata)

    handoff = Envelope(
        session_id="sess-1",
        sender_id="alice",
        audience=None,
        event_type=EV_HANDOFF,
        event_data={"reason": "go"},
        envelope_id="env-h",
    )
    new_state = adapter.fold(handoff, state)
    assert adapter.dispatch_audience(handoff, metadata, new_state) == ["bob"]


@pytest.mark.asyncio
async def test_hub_dispatch_honours_workflow_audience_override() -> None:
    """End-to-end: in a 3-way workflow, only the next speaker gets the notify."""
    import asyncio

    from autogen.beta.network.envelope import (
        EV_SESSION_INVITE,
        EV_SESSION_INVITE_ACK,
    )
    from autogen.beta.network.transport.frames import NotifyFrame

    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    received: dict[str, list[Envelope]] = {}

    # Register identities directly (no AgentClient, so we observe raw
    # dispatch). One auto-acking endpoint per agent so the invite
    # handshake completes.
    alice_p = await hub.register(Passport(name="alice"), Resume())
    bob_p = await hub.register(Passport(name="bob"), Resume())
    carol_p = await hub.register(Passport(name="carol"), Resume())

    for p in (alice_p, bob_p, carol_p):
        received[p.agent_id] = []

    name_by_id = {alice_p.agent_id: "alice", bob_p.agent_id: "bob", carol_p.agent_id: "carol"}

    class _AutoAckEndpoint:
        def __init__(self, agent_id: str) -> None:
            self.endpoint_id = f"ep-{agent_id}"
            self.agent_id = agent_id

        async def send_frame(self, frame: Any) -> None:
            if not isinstance(frame, NotifyFrame):
                return
            ev = frame.envelope
            if ev.event_type == EV_SESSION_INVITE and ev.audience and self.agent_id in ev.audience:
                ack = Envelope(
                    session_id=ev.session_id,
                    sender_id=self.agent_id,
                    audience=[ev.sender_id],
                    event_type=EV_SESSION_INVITE_ACK,
                    event_data={},
                    causation_id=ev.envelope_id,
                )
                # Post in a task so we don't block the dispatcher.
                asyncio.create_task(hub.post_envelope(ack))
            elif ev.event_type == EV_TEXT:
                received[self.agent_id].append(ev)

        async def close(self) -> None:
            return None

    for p in (alice_p, bob_p, carol_p):
        ep = _AutoAckEndpoint(p.agent_id)
        hub._endpoints_by_id[ep.endpoint_id] = ep  # type: ignore[assignment]
        hub._agent_to_endpoint[p.agent_id] = ep.endpoint_id

    graph = TransitionGraph(
        initial_speaker=alice_p.agent_id,
        transitions=[
            Transition(when=Always(), then=RoundRobinTarget()),
        ],
        default_target=RoundRobinTarget(),
    )

    session = await hub.create_session(
        creator_id=alice_p.agent_id,
        manifest_type=WORKFLOW_TYPE,
        manifest_version=1,
        participants=[alice_p.agent_id, bob_p.agent_id, carol_p.agent_id],
        knobs={"graph": graph.to_dict()},
    )

    # Alice posts EV_TEXT (broadcast). With dispatch_audience narrowing
    # to ``expected_next_speaker`` (RoundRobinTarget rotates to bob),
    # only bob should see the notify.
    text = Envelope(
        session_id=session.session_id,
        sender_id=alice_p.agent_id,
        audience=None,
        event_type=EV_TEXT,
        event_data={"text": "hello"},
    )
    await hub.post_envelope(text)

    # Translate captures by name for readable assertions.
    by_name = {name_by_id[aid]: envs for aid, envs in received.items()}

    assert by_name["alice"] == []
    assert len(by_name["bob"]) == 1
    assert by_name["bob"][0].event_data == {"text": "hello"}
    assert by_name["carol"] == [], "carol should be skipped by dispatch_audience override"

    await hub.close()


@pytest.mark.asyncio
async def test_other_adapters_keep_broadcast_default() -> None:
    """``conversation`` and ``discussion`` don't implement the hook."""
    from autogen.beta.network.adapters.conversation import ConversationAdapter
    from autogen.beta.network.adapters.discussion import DiscussionAdapter

    assert not hasattr(ConversationAdapter(), "dispatch_audience")
    assert not hasattr(DiscussionAdapter(), "dispatch_audience")
