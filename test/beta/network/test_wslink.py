# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""WebSocket transport (``WsLink`` + ``serve_ws``).

These tests spin up a real ``websockets`` server bound to a free
local port and verify the wire shape end-to-end. Register-over-the-wire
is not yet covered; here we register the agent in-process and use
``HelloFrame`` to bind the wire connection to the existing identity —
same path a re-attaching deployment would use.

Covers:

* ``HelloFrame`` → ``WelcomeFrame`` handshake binds an existing
  identity to a wire endpoint.
* ``HelloFrame`` for an unknown name surfaces an ``ErrorFrame``.
* ``PingFrame`` round-trips through ``PongFrame``.
* Envelope dispatch (hub → WsLink client) reaches a wire-connected
  agent: send through LocalLink, deliver via WS as ``NotifyFrame``.
"""

import asyncio

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
from autogen.beta.network.transport.frames import (
    AcceptFrame,
    ErrorFrame,
    HelloFrame,
    NotifyFrame,
    PingFrame,
    PongFrame,
    SendFrame,
    WelcomeFrame,
)
from autogen.beta.testing import TestConfig


def _agent(name: str) -> Agent:
    return Agent(name=name, config=TestConfig())


@pytest.mark.asyncio
async def test_hello_welcome_handshake_binds_existing_identity() -> None:
    """A registered name + ``HelloFrame`` → ``WelcomeFrame`` binds the wire endpoint."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    # Register alice in-process so the name exists.
    alice_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())

    async with serve_ws(hub) as port:
        client = WsLink(f"ws://127.0.0.1:{port}").client()
        await client.open()
        try:
            await client.send_frame(HelloFrame(name="alice"))
            received: list[object] = []
            async for frame in client.frames():
                received.append(frame)
                if isinstance(frame, WelcomeFrame):
                    break
            assert isinstance(received[-1], WelcomeFrame)
            # Hub re-bound alice to the new endpoint.
            assert hub._agent_to_endpoint[alice.agent_id] != alice_hc._client_link.endpoint_id
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_hello_for_unknown_name_returns_error_frame() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    async with serve_ws(hub) as port:
        client = WsLink(f"ws://127.0.0.1:{port}").client()
        await client.open()
        try:
            await client.send_frame(HelloFrame(name="ghost"))
            async for frame in client.frames():
                assert isinstance(frame, ErrorFrame)
                assert frame.code == "not_found"
                break
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_ping_round_trips_through_pong() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    async with serve_ws(hub) as port:
        client = WsLink(f"ws://127.0.0.1:{port}").client()
        await client.open()
        try:
            await client.send_frame(PingFrame())
            async for frame in client.frames():
                assert isinstance(frame, PongFrame)
                break
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_envelope_dispatched_to_ws_connected_agent() -> None:
    """Bob attaches over WS, manually acks the invite, then receives EV_TEXT
    over the wire when alice sends.

    Bob's WS connection is a "dumb" client — no notify-handler glue.
    A real deployment would either run an ``AgentClient`` over the
    wire or an HTTP-driven client that posts the ack via the CRUD
    surface; here we simulate the latter by sending a ``SendFrame``
    carrying the ack envelope directly.
    """
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    async with serve_ws(hub) as port:
        bob_ws = WsLink(f"ws://127.0.0.1:{port}").client()
        await bob_ws.open()
        try:
            await bob_ws.send_frame(HelloFrame(name="bob"))

            received_text: list[NotifyFrame] = []

            async def driver() -> None:
                async for frame in bob_ws.frames():
                    if isinstance(frame, NotifyFrame):
                        ev = frame.envelope
                        if ev.event_type == EV_SESSION_INVITE:
                            # Manually ack the invite so the session activates.
                            ack = Envelope(
                                session_id=ev.session_id,
                                sender_id=bob.agent_id,
                                audience=[ev.sender_id],
                                event_type=EV_SESSION_INVITE_ACK,
                                event_data={},
                                causation_id=ev.envelope_id,
                            )
                            await bob_ws.send_frame(SendFrame(envelope=ack))
                        elif ev.event_type == EV_TEXT:
                            received_text.append(frame)
                            return

            driver_task = asyncio.create_task(driver())
            await asyncio.sleep(0.05)

            session = await alice.open(type="conversation", target="bob")
            audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
            await session.send("hello over the wire", audience=audience)

            await asyncio.wait_for(driver_task, timeout=3.0)
            assert len(received_text) == 1
            assert received_text[0].envelope.event_data == {"text": "hello over the wire"}
            assert received_text[0].recipient_id == bob.agent_id
        finally:
            await bob_ws.close()
