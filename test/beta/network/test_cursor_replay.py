# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Cut 3.1 — receipt + per-(agent, session) cursor + Hello replay.

Receipts advance the hub's per-(agent, session) ``inbox.cursor``; on
``HelloFrame`` reconnect over a wire transport, the hub replays any
envelope that landed past the cursor as a fresh ``NotifyFrame``. The
default handler's ``find_envelope_by_causation`` dedup absorbs duplicate
deliveries, so replay is safe even when the handler completed its work
before the disconnect.

Coverage:

* ack advances the cursor and is write-through-persisted
* hydrate rebuilds the cursor from disk
* nack appends to ``inbox_nacks.jsonl``
* on Hello reconnect, hub replays envelopes past the cursor
* envelopes already past the cursor are NOT re-sent
* unregister deletes the cursor
"""

import asyncio
import json

import pytest

from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_TEXT,
    Envelope,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
    WsLink,
    serve_ws,
)
from autogen.beta.network.envelope import (
    EV_SESSION_INVITE,
    EV_SESSION_INVITE_ACK,
)
from autogen.beta.network.hub.layout import inbox_cursor_path, inbox_nacks_path
from autogen.beta.network.transport.frames import (
    HelloFrame,
    NotifyFrame,
    ReceiptFrame,
    SendFrame,
    WelcomeFrame,
)
from autogen.beta.testing import TestConfig


def _agent(name: str) -> Agent:
    return Agent(name=name, config=TestConfig())


def _invite_only_handler(client):
    """Build a handler that auto-acks invites and ignores everything else.

    Lets us drive a real session handshake without engaging the empty
    ``TestConfig`` LLM on EV_TEXT — receipts still fire from the
    ``AgentClient.receive`` ack path because the handler returns
    cleanly on every invocation.
    """
    async def handler(envelope: Envelope) -> None:
        if envelope.event_type != EV_SESSION_INVITE:
            return
        ack = Envelope(
            session_id=envelope.session_id,
            sender_id=client.agent_id,
            audience=[envelope.sender_id],
            event_type=EV_SESSION_INVITE_ACK,
            event_data={},
            causation_id=envelope.envelope_id,
        )
        await client.send_envelope(ack)

    return handler


@pytest.mark.asyncio
async def test_ack_advances_cursor_and_persists() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    bob.on_envelope(_invite_only_handler(bob))

    session = await alice.open(type="conversation", target="bob")
    await asyncio.sleep(0.05)  # let invite/ack handshake settle
    await session.send("hi bob", audience=[bob.agent_id])
    await asyncio.sleep(0.05)

    wal = await hub.read_wal(session.metadata.session_id)
    text_env = next(e for e in wal if e.event_type == EV_TEXT)

    # Cursor should now reflect bob's most recent successful ack — the
    # EV_TEXT envelope.
    cursor = hub._inbox_cursors.get(bob.agent_id, {}).get(session.metadata.session_id)
    assert cursor == text_env.envelope_id

    # Write-through persistence: the on-disk file mirrors the live map.
    raw = await store.read(inbox_cursor_path(bob.agent_id))
    assert raw is not None
    persisted = json.loads(raw)
    assert persisted[session.metadata.session_id] == text_env.envelope_id

    await alice_hc.shutdown()
    await bob_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_hydrate_restores_cursor() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    bob.on_envelope(_invite_only_handler(bob))

    session = await alice.open(type="conversation", target="bob")
    await asyncio.sleep(0.05)
    await session.send("survive the restart", audience=[bob.agent_id])
    await asyncio.sleep(0.05)

    wal = await hub.read_wal(session.metadata.session_id)
    text_env = next(e for e in wal if e.event_type == EV_TEXT)

    # Close the connections without unregistering — we want the
    # persisted cursor file intact across the restart.
    await alice_hc.close()
    await bob_hc.close()
    await hub.close()

    # Fresh hub against the same store.
    hub2 = await Hub.open(store, ttl_sweep_interval=0)
    assert hub2._inbox_cursors[bob.agent_id][session.metadata.session_id] == text_env.envelope_id
    await hub2.close()


@pytest.mark.asyncio
async def test_nack_appends_to_inbox_nacks() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())

    # Send a synthetic nack via the link.
    await alice_hc._send_receipt(
        envelope_id="env-xyz",
        session_id="sess-abc",
        status="nack",
        reason="handler_failed",
    )
    await asyncio.sleep(0.05)

    raw = await store.read(inbox_nacks_path(alice.agent_id))
    assert raw is not None
    lines = [ln for ln in raw.splitlines() if ln]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["envelope_id"] == "env-xyz"
    assert record["session_id"] == "sess-abc"
    assert record["reason"] == "handler_failed"

    await alice_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_unregister_clears_cursor() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conversation", target="bob")
    await asyncio.sleep(0.05)
    await session.send("hi", audience=[bob.agent_id])
    await asyncio.sleep(0.05)

    assert bob.agent_id in hub._inbox_cursors

    await hub.unregister(bob.agent_id)
    assert bob.agent_id not in hub._inbox_cursors
    assert await store.read(inbox_cursor_path(bob.agent_id)) is None

    await alice_hc.shutdown()
    await bob_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_hello_replay_redelivers_unacked_envelopes() -> None:
    """Wire reconnect: hub replays every envelope past bob's cursor.

    Bob registers via ``LocalLink`` to bootstrap his identity, then
    connects over WS so dispatch follows the wire endpoint after the
    Hello rebind. Bob acks the invite, but disconnects without
    ack-ing the EV_TEXT alice sends. On reconnect (a fresh
    ``HelloFrame`` against a new WS connection), the hub re-delivers
    the EV_TEXT.
    """
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    async with serve_ws(hub) as port:
        bob_ws1 = WsLink(f"ws://127.0.0.1:{port}").client()
        await bob_ws1.open()
        await bob_ws1.send_frame(HelloFrame(name="bob"))

        # Pump frames on a single consumer task — the WS frames()
        # iterator is single-consumer, so we collect the welcome and
        # the invite/text deliveries in one place.
        text_seen = asyncio.Event()
        text_envelope_id: list[str] = []

        async def consume_until_text() -> None:
            async for frame in bob_ws1.frames():
                if isinstance(frame, WelcomeFrame):
                    continue
                if isinstance(frame, NotifyFrame):
                    ev = frame.envelope
                    if ev.event_type == EV_SESSION_INVITE:
                        ack = Envelope(
                            session_id=ev.session_id,
                            sender_id=bob.agent_id,
                            audience=[ev.sender_id],
                            event_type=EV_SESSION_INVITE_ACK,
                            event_data={},
                            causation_id=ev.envelope_id,
                        )
                        await bob_ws1.send_frame(SendFrame(envelope=ack))
                    elif ev.event_type == EV_TEXT:
                        text_envelope_id.append(ev.envelope_id)
                        text_seen.set()
                        # Don't ack — that's the whole point of the test.
                        return

        consumer = asyncio.create_task(consume_until_text())

        # Give the hub a moment to bind the WS endpoint before we
        # dispatch the invite.
        await asyncio.sleep(0.1)

        session = await alice.open(type="conversation", target="bob")
        await session.send("the message", audience=[bob.agent_id])
        await asyncio.wait_for(text_seen.wait(), timeout=3.0)
        await consumer

        # Drop the connection without sending ReceiptFrame for EV_TEXT.
        await bob_ws1.close()
        await asyncio.sleep(0.05)

        # The EV_TEXT was never acked — cursor still trails behind.
        text_env_id = text_envelope_id[0]
        cursor_for_bob = hub._inbox_cursors.get(bob.agent_id, {}).get(session.metadata.session_id)
        assert cursor_for_bob != text_env_id

        # Reconnect: a fresh WS client + Hello should trigger replay.
        bob_ws2 = WsLink(f"ws://127.0.0.1:{port}").client()
        await bob_ws2.open()
        try:
            await bob_ws2.send_frame(HelloFrame(name="bob"))

            replayed: list[NotifyFrame] = []
            saw_welcome = False
            try:
                async with asyncio.timeout(3.0):
                    async for frame in bob_ws2.frames():
                        if isinstance(frame, WelcomeFrame):
                            saw_welcome = True
                        elif isinstance(frame, NotifyFrame):
                            if frame.envelope.event_type == EV_TEXT:
                                replayed.append(frame)
                                break
            except asyncio.TimeoutError:
                pass

            assert saw_welcome
            assert len(replayed) == 1, "expected EV_TEXT to be replayed"
            assert replayed[0].envelope.envelope_id == text_env_id
            assert replayed[0].recipient_id == bob.agent_id
        finally:
            await bob_ws2.close()

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_replay_skips_envelopes_past_cursor() -> None:
    """Bob acks the EV_TEXT before disconnecting; reconnect replays nothing."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    async with serve_ws(hub) as port:
        bob_ws1 = WsLink(f"ws://127.0.0.1:{port}").client()
        await bob_ws1.open()
        await bob_ws1.send_frame(HelloFrame(name="bob"))

        ack_event = asyncio.Event()

        async def consumer() -> None:
            async for frame in bob_ws1.frames():
                if isinstance(frame, WelcomeFrame):
                    continue
                if isinstance(frame, NotifyFrame):
                    ev = frame.envelope
                    if ev.event_type == EV_SESSION_INVITE:
                        ack = Envelope(
                            session_id=ev.session_id,
                            sender_id=bob.agent_id,
                            audience=[ev.sender_id],
                            event_type=EV_SESSION_INVITE_ACK,
                            event_data={},
                            causation_id=ev.envelope_id,
                        )
                        await bob_ws1.send_frame(SendFrame(envelope=ack))
                    elif ev.event_type == EV_TEXT:
                        # Ack this one — that's the whole point of the test.
                        await bob_ws1.send_frame(
                            ReceiptFrame(
                                envelope_id=ev.envelope_id,
                                session_id=ev.session_id,
                                status="ack",
                            )
                        )
                        ack_event.set()
                        return

        consumer_task = asyncio.create_task(consumer())
        await asyncio.sleep(0.1)

        session = await alice.open(type="conversation", target="bob")
        await session.send("acked message", audience=[bob.agent_id])
        await asyncio.wait_for(ack_event.wait(), timeout=3.0)
        await consumer_task
        await asyncio.sleep(0.05)  # let the ReceiptFrame land

        await bob_ws1.close()
        await asyncio.sleep(0.05)

        # Reconnect — replay should find nothing past the cursor.
        bob_ws2 = WsLink(f"ws://127.0.0.1:{port}").client()
        await bob_ws2.open()
        try:
            await bob_ws2.send_frame(HelloFrame(name="bob"))

            extra_notifies: list[NotifyFrame] = []
            try:
                async with asyncio.timeout(0.5):
                    async for frame in bob_ws2.frames():
                        if isinstance(frame, NotifyFrame):
                            extra_notifies.append(frame)
            except asyncio.TimeoutError:
                pass

            assert extra_notifies == [], "no replay expected when cursor is current"
        finally:
            await bob_ws2.close()

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()
