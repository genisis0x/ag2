# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Phase 2.0 durability primitives.

Covers:

* ``Hub.find_envelope_by_causation`` — index populated on post, rebuilt
  on hydrate.
* Default notify handler dedup — re-firing against an already-replied
  envelope is a no-op (no duplicate reply in WAL).
* ``Hub.pending_turns_for`` — surfaces sessions where the agent is
  expected to act but no reply has landed.
* ``HubClient.attach()`` reconnect — restored ``AgentClient`` resumes
  the pending turn from a prior incarnation.
* ``Task.checkpoint`` + ``resume_from`` — owner-supplied state survives
  task restart through a hub-backed ``CheckpointStore``.
"""

import json

import pytest

from autogen.beta import Agent
from autogen.beta.knowledge import DiskKnowledgeStore, MemoryKnowledgeStore
from autogen.beta.network import (
    EV_TEXT,
    Envelope,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.client.checkpoint import HubBackedCheckpointStore
from autogen.beta.network.hub.layout import task_checkpoint_path
from autogen.beta.testing import TestConfig


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


@pytest.mark.asyncio
async def test_find_envelope_by_causation_returns_reply_after_post() -> None:
    """post_envelope populates the causation index in-line with WAL append."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)

    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume()
    )
    await bob_hc.register(
        _agent("bob", "reply from bob"),
        Passport(name="bob"),
        Resume(),
    )

    session = await alice.open(type="consulting", target="bob")
    prompt_id = await session.send("question", audience=[s.agent_id for s in session.metadata.participants if s.agent_id != alice.agent_id])

    # Wait for bob's reply to land.
    reply_envelope = await alice.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT and e.sender_id != alice.agent_id,
        timeout=5.0,
    )

    # Bob's reply is indexed under (bob.agent_id, prompt_id).
    found = hub.find_envelope_by_causation(
        session.session_id,
        sender_id=reply_envelope.sender_id,
        causation_id=prompt_id,
    )
    assert found is not None
    assert found.envelope_id == reply_envelope.envelope_id
    assert found.event_data["text"] == "reply from bob"

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_find_envelope_by_causation_returns_none_for_unknown() -> None:
    """Empty causation_id and unknown keys both return None."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="consulting", target="bob")

    # Empty causation — never indexed.
    assert hub.find_envelope_by_causation(
        session.session_id, sender_id=bob.agent_id, causation_id=""
    ) is None

    # Bogus causation — index has no such key.
    assert hub.find_envelope_by_causation(
        session.session_id, sender_id=bob.agent_id, causation_id="never-existed"
    ) is None

    # Unknown session — also None.
    assert hub.find_envelope_by_causation(
        "no-such-session", sender_id=bob.agent_id, causation_id="anything"
    ) is None

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_causation_index_rebuilt_on_hydrate(tmp_path) -> None:
    """Hub.hydrate walks each session's WAL and refills the causation index."""
    store = DiskKnowledgeStore(str(tmp_path))
    hub1 = await Hub.open(store, ttl_sweep_interval=0)
    link1 = LocalLink(hub1)

    alice_hc = HubClient(link1, hub=hub1)
    bob_hc = HubClient(link1, hub=hub1)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(
        _agent("bob", "the answer"),
        Passport(name="bob"),
        Resume(),
    )

    session = await alice.open(type="consulting", target="bob")
    prompt_id = await session.send("Q", audience=[bob.agent_id])
    await alice.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT and e.sender_id == bob.agent_id,
        timeout=5.0,
    )

    # Capture bob's id + the reply env id while hub1 is alive.
    reply_in_hub1 = hub1.find_envelope_by_causation(
        session.session_id, sender_id=bob.agent_id, causation_id=prompt_id
    )
    assert reply_in_hub1 is not None
    expected_reply_id = reply_in_hub1.envelope_id

    await alice_hc.close()
    await bob_hc.close()
    await hub1.close()

    # Reopen against the same store; index rebuilds from WAL.
    store2 = DiskKnowledgeStore(str(tmp_path))
    hub2 = await Hub.open(store2, ttl_sweep_interval=0)
    rebuilt = hub2.find_envelope_by_causation(
        session.session_id, sender_id=bob.agent_id, causation_id=prompt_id
    )
    assert rebuilt is not None
    assert rebuilt.envelope_id == expected_reply_id

    await hub2.close()


@pytest.mark.asyncio
async def test_default_handler_dedups_duplicate_invocation() -> None:
    """Re-firing the handler against an already-replied envelope is a no-op.

    Phase 2.0 idempotency: ``find_envelope_by_causation`` short-circuits
    before ``agent.ask`` so no duplicate reply lands in the WAL.
    """
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)

    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    # Conversation adapter: bidirectional, doesn't auto-close so we can
    # re-fire bob's handler without stepping on consulting's strict 1Q1R.
    bob = await bob_hc.register(
        _agent("bob", "first reply", "second reply"),
        Passport(name="bob"),
        Resume(),
    )

    session = await alice.open(type="conversation", target="bob")
    prompt_id = await session.send("hello", audience=[bob.agent_id])

    # Wait for the first reply to land.
    first_reply = await alice.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT and e.sender_id == bob.agent_id,
        timeout=5.0,
    )

    wal_before = await hub.read_wal(session.session_id)
    bob_text_count_before = sum(
        1 for e in wal_before
        if e.event_type == EV_TEXT and e.sender_id == bob.agent_id
    )
    assert bob_text_count_before == 1

    # Re-fetch alice's prompt envelope and re-fire bob's handler against it.
    alice_prompt = next(
        e for e in wal_before
        if e.envelope_id == prompt_id
    )
    await bob.receive(alice_prompt)

    # No new reply — dedup short-circuited.
    wal_after = await hub.read_wal(session.session_id)
    bob_text_count_after = sum(
        1 for e in wal_after
        if e.event_type == EV_TEXT and e.sender_id == bob.agent_id
    )
    assert bob_text_count_after == bob_text_count_before
    # The original reply is still the latest from bob.
    assert wal_after[-1].envelope_id == first_reply.envelope_id or any(
        e.envelope_id == first_reply.envelope_id for e in wal_after
    )

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


async def _noop_handler(_env: object) -> None:
    return None


@pytest.mark.asyncio
async def test_pending_turns_surfaces_unfinished_responder() -> None:
    """Bob is the consulting respondent, hasn't replied → pending turn for bob."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    # Open consulting first so bob's default handler auto-acks the
    # invite. Once active, replace bob's handler with a no-op to
    # simulate "agent crashed mid-handler" — the inbound prompt sits
    # unhandled and the WAL records no reply.
    session = await alice.open(type="consulting", target="bob")
    bob.on_envelope(_noop_handler)
    prompt_id = await session.send("Q", audience=[bob.agent_id])

    pending = await hub.pending_turns_for(bob.agent_id)
    assert len(pending) == 1
    assert pending[0].session_id == session.session_id
    assert pending[0].last_envelope_id == prompt_id

    # Alice's turn is not pending — she's the initiator who already sent.
    assert await hub.pending_turns_for(alice.agent_id) == []

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_pending_turns_empty_after_reply_lands() -> None:
    """Once bob replies, the consulting session auto-closes — no pending."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(
        _agent("bob", "ok"),
        Passport(name="bob"),
        Resume(),
    )

    session = await alice.open(type="consulting", target="bob")
    await session.send("Q", audience=[bob.agent_id])

    # Wait for bob's reply.
    await alice.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT and e.sender_id == bob.agent_id,
        timeout=5.0,
    )

    # Both agents' turn lists are empty: session is closed.
    assert await hub.pending_turns_for(bob.agent_id) == []
    assert await hub.pending_turns_for(alice.agent_id) == []

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_attach_resumes_pending_turn(tmp_path) -> None:
    """Bob's process dies mid-handler; reconnect via attach() drives the reply.

    Simulates the hub-survives, agent-crashes scenario:
    1. alice sends a consulting prompt.
    2. bob's HubClient is closed before bob's handler runs.
    3. A fresh HubClient attaches to the existing identity.
    4. Bob's pending turn fires automatically; reply lands.
    """
    store = DiskKnowledgeStore(str(tmp_path))
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc_v1 = HubClient(link, hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob_v1 = await bob_hc_v1.register(_agent("bob"), Passport(name="bob"), Resume())

    # Let bob auto-ack the invite, then swap to a no-op so the prompt
    # arrives but never gets a reply — the "crashed mid-handler" shape.
    session = await alice.open(type="consulting", target="bob")
    bob_v1.on_envelope(_noop_handler)
    prompt_id = await session.send("Q", audience=[bob_v1.agent_id])

    # Confirm bob has a pending turn.
    assert len(await hub.pending_turns_for(bob_v1.agent_id)) == 1

    # Simulate bob's process dying.
    await bob_hc_v1.close()

    # New process: attach to the existing identity. Default handler is
    # back, and ``resume_pending_turns`` fires the inbound prompt.
    bob_hc_v2 = HubClient(link, hub=hub)
    bob_v2 = await bob_hc_v2.attach(
        _agent("bob", "delayed reply"),
        name="bob",
    )

    # Bob's reply now lands. Wait on alice's inbox.
    reply = await alice.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT and e.sender_id == bob_v2.agent_id,
        timeout=5.0,
    )
    assert reply.event_data["text"] == "delayed reply"
    assert reply.causation_id == prompt_id

    # Pending list is now empty.
    assert await hub.pending_turns_for(bob_v2.agent_id) == []

    await alice_hc.close()
    await bob_hc_v2.close()
    await hub.close()


@pytest.mark.asyncio
async def test_attach_dedups_when_prior_reply_already_landed(tmp_path) -> None:
    """If the prior incarnation actually finished its reply, attach() is harmless.

    The dedup query in the default handler short-circuits before
    agent.ask, so the resumed handler doesn't post a duplicate.
    """
    store = DiskKnowledgeStore(str(tmp_path))
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc_v1 = HubClient(link, hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob_v1 = await bob_hc_v1.register(
        _agent("bob", "the reply"),
        Passport(name="bob"),
        Resume(),
    )

    session = await alice.open(type="consulting", target="bob")
    await session.send("Q", audience=[bob_v1.agent_id])
    await alice.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT and e.sender_id == bob_v1.agent_id,
        timeout=5.0,
    )

    wal_before = await hub.read_wal(session.session_id)
    bob_replies_before = [
        e for e in wal_before if e.event_type == EV_TEXT and e.sender_id == bob_v1.agent_id
    ]
    assert len(bob_replies_before) == 1

    await bob_hc_v1.close()

    # Reattach — the session is now closed, so resume_pending_turns
    # finds nothing. Even if it did, the dedup query would catch it.
    bob_hc_v2 = HubClient(link, hub=hub)
    bob_v2 = await bob_hc_v2.attach(
        _agent("bob", "duplicate would be bad"),
        name="bob",
    )

    wal_after = await hub.read_wal(session.session_id)
    bob_replies_after = [
        e for e in wal_after if e.event_type == EV_TEXT and e.sender_id == bob_v2.agent_id
    ]
    assert len(bob_replies_after) == 1  # no dup

    await alice_hc.close()
    await bob_hc_v2.close()
    await hub.close()


@pytest.mark.asyncio
async def test_task_checkpoint_writes_to_store(tmp_path) -> None:
    """Task.checkpoint persists JSON via the supplied CheckpointStore."""
    store = DiskKnowledgeStore(str(tmp_path))
    checkpoint_store = HubBackedCheckpointStore(store)

    agent = _agent("worker")

    async with agent.task(
        "long-running",
        checkpoint_store=checkpoint_store,
    ) as task:
        await task.checkpoint({"step": 1, "result_so_far": [1, 2, 3]})
        # Update — last-write-wins.
        await task.checkpoint({"step": 2, "result_so_far": [1, 2, 3, 4]})

        # Verify on-disk via the store directly.
        body = await store.read(task_checkpoint_path(task.task_id))
        assert body is not None
        assert json.loads(body) == {"step": 2, "result_so_far": [1, 2, 3, 4]}


@pytest.mark.asyncio
async def test_task_checkpoint_no_store_is_noop() -> None:
    """Without a CheckpointStore, checkpoint() silently does nothing.

    Checkpointing is opt-in; standalone tasks shouldn't need to set up
    storage just to use the primitive's other features.
    """
    agent = _agent("worker")
    async with agent.task("standalone") as task:
        # Should not raise.
        await task.checkpoint({"anything": "goes"})
        # Resumed state is None when not resuming.
        assert task.resumed_state is None


@pytest.mark.asyncio
async def test_task_resume_from_reads_prior_checkpoint(tmp_path) -> None:
    """A new task constructed with resume_from sees prior checkpoint state."""
    store = DiskKnowledgeStore(str(tmp_path))
    checkpoint_store = HubBackedCheckpointStore(store)

    agent = _agent("worker")

    # First incarnation: write a checkpoint, then crash before completing.
    first_task_id: str = ""
    async with agent.task(
        "stage one",
        checkpoint_store=checkpoint_store,
    ) as task:
        first_task_id = task.task_id
        await task.checkpoint({"phase": "midway", "items_done": 7})
        # Simulate crash by failing the task — terminal state.
        await task.fail("simulated crash")

    assert first_task_id  # captured

    # Second incarnation: resume from the prior task id.
    async with agent.task(
        "stage one",
        checkpoint_store=checkpoint_store,
        resume_from=first_task_id,
    ) as resumed:
        assert resumed.task_id == first_task_id  # id pinned to prior
        assert resumed.resumed_state == {"phase": "midway", "items_done": 7}


@pytest.mark.asyncio
async def test_task_resume_from_with_no_prior_checkpoint_is_safe(tmp_path) -> None:
    """resume_from against a task that never checkpointed → resumed_state is None."""
    store = DiskKnowledgeStore(str(tmp_path))
    checkpoint_store = HubBackedCheckpointStore(store)

    agent = _agent("worker")
    async with agent.task(
        "fresh",
        checkpoint_store=checkpoint_store,
        resume_from="never-existed",
    ) as task:
        assert task.task_id == "never-existed"  # id still pinned
        assert task.resumed_state is None  # but no prior state


@pytest.mark.asyncio
async def test_agent_client_checkpoint_store_is_hub_backed() -> None:
    """AgentClient.checkpoint_store is a HubBackedCheckpointStore wired to the hub's store."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)
    link = LocalLink(hub)

    hc = HubClient(link, hub=hub)
    client = await hc.register(_agent("worker"), Passport(name="worker"), Resume())

    assert isinstance(client.checkpoint_store, HubBackedCheckpointStore)
    # Two accesses return the same instance (lazy-cached).
    assert client.checkpoint_store is client.checkpoint_store

    # Round-trip through the property.
    await client.checkpoint_store.write("task-xyz", {"k": "v"})
    body = await store.read(task_checkpoint_path("task-xyz"))
    assert body is not None
    assert json.loads(body) == {"k": "v"}

    await hc.close()
    await hub.close()
