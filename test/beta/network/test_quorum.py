# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""N-of-M quorum tracking.

Covers:

* ``required_acks=N`` activates the session as soon as N acks land,
  even if some invitees are still pending.
* A reject only fails the handshake when it makes the threshold
  unreachable; otherwise the session continues toward quorum.
* ``ag2.session.quorum_changed`` fires when an active-session
  participant is removed via ``mark_removed``.
"""

import asyncio

import pytest

from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_QUORUM_CHANGED,
    EV_SESSION_INVITE,
    EV_SESSION_INVITE_REJECT,
    Envelope,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.errors import ProtocolError
from autogen.beta.network.session import SessionState
from autogen.beta.testing import TestConfig


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


def _make_rejecter(client):
    """Replace a peer's notify handler with one that rejects every invite."""

    async def _reject_invite(envelope: Envelope) -> None:
        if envelope.event_type != EV_SESSION_INVITE:
            return
        rej = Envelope(
            session_id=envelope.session_id,
            sender_id=client.agent_id,
            audience=None,
            event_type=EV_SESSION_INVITE_REJECT,
            event_data={"session_id": envelope.session_id},
            causation_id=envelope.envelope_id,
        )
        await client.send_envelope(rej)

    return _reject_invite


@pytest.mark.asyncio
async def test_required_acks_activates_with_partial_quorum() -> None:
    """``required_acks=1`` activates after the first ack — others still pending."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    c_hc = HubClient(link, hub=hub)

    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    carol = await c_hc.register(_agent("carol"), Passport(name="carol"), Resume(), attach_plugin=False)

    # Carol never replies — her invite stays pending.
    async def _silent(_env: Envelope) -> None:
        return None

    carol.on_envelope(_silent)

    # Drive create_session directly so we can pin required_acks.
    metadata = await hub.create_session(
        creator_id=alice.agent_id,
        manifest_type="discussion",
        participants=[bob.agent_id, carol.agent_id],
        knobs={"ordering": "round_robin"},
        required_acks=1,
    )

    # Bob ack'd; the session is active before carol responds.
    assert metadata.state == SessionState.ACTIVE
    # Carol is still in pending_acks — her ack just isn't required.
    assert carol.agent_id in metadata.pending_acks

    await a_hc.close()
    await b_hc.close()
    await c_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_quorum_unreachable_fails_session() -> None:
    """A reject that drops the achievable count below threshold fails creation."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    c_hc = HubClient(link, hub=hub)

    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume(), attach_plugin=False)
    carol = await c_hc.register(_agent("carol"), Passport(name="carol"), Resume(), attach_plugin=False)

    # Both peers reject — required_acks=2 means quorum is unreachable
    # after the second reject.
    bob.on_envelope(_make_rejecter(bob))
    carol.on_envelope(_make_rejecter(carol))

    with pytest.raises(ProtocolError, match="quorum_unreachable"):
        await hub.create_session(
            creator_id=alice.agent_id,
            manifest_type="discussion",
            participants=[bob.agent_id, carol.agent_id],
            knobs={"ordering": "round_robin"},
            required_acks=2,
        )

    await a_hc.close()
    await b_hc.close()
    await c_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_partial_reject_with_reachable_quorum_still_activates() -> None:
    """One reject is fine when remaining acks can still meet threshold."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    c_hc = HubClient(link, hub=hub)

    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    carol = await c_hc.register(_agent("carol"), Passport(name="carol"), Resume(), attach_plugin=False)

    # Carol rejects, bob acks. With required_acks=1 quorum is met.
    carol.on_envelope(_make_rejecter(carol))

    metadata = await hub.create_session(
        creator_id=alice.agent_id,
        manifest_type="discussion",
        participants=[bob.agent_id, carol.agent_id],
        knobs={"ordering": "round_robin"},
        required_acks=1,
    )

    # Whether bob's ack lands first (session goes ACTIVE before carol
    # rejects) or carol's reject lands first (still activates because
    # quorum stays reachable), the end state must be ACTIVE.
    assert metadata.state == SessionState.ACTIVE

    await a_hc.close()
    await b_hc.close()
    await c_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_quorum_changed_fires_on_remove() -> None:
    """Removing an active participant emits ``ag2.session.quorum_changed``."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    c_hc = HubClient(link, hub=hub)

    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    carol = await c_hc.register(_agent("carol"), Passport(name="carol"), Resume())

    metadata = await hub.create_session(
        creator_id=alice.agent_id,
        manifest_type="discussion",
        participants=[bob.agent_id, carol.agent_id],
        knobs={"ordering": "round_robin"},
        required_acks=2,
    )
    assert metadata.state == SessionState.ACTIVE

    # Remove carol — should emit quorum_changed with remaining=1, required=2.
    await hub.mark_removed(metadata.session_id, carol.agent_id)
    await asyncio.sleep(0.01)  # let the post_envelope finish

    wal = await hub.read_wal(metadata.session_id)
    quorum_events = [e for e in wal if e.event_type == EV_QUORUM_CHANGED]
    assert len(quorum_events) == 1
    assert quorum_events[0].event_data == {"remaining": 1, "required": 2}

    await a_hc.close()
    await b_hc.close()
    await c_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_quorum_changed_only_for_active_sessions() -> None:
    """Removing from a non-active session does not emit a spurious envelope."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)

    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conversation", target=bob.agent_id)
    await hub.close_session(session.session_id, reason="manual")

    # Hub is closed; mark_removed shouldn't crash or emit on a
    # terminal session.
    await hub.mark_removed(session.session_id, bob.agent_id)
    wal = await hub.read_wal(session.session_id)
    quorum_events = [e for e in wal if e.event_type == EV_QUORUM_CHANGED]
    assert quorum_events == []

    await a_hc.close()
    await b_hc.close()
    await hub.close()
