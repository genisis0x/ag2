# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Tenant-side per-envelope send/receive hooks.

Covers:

* ``AgentClient.add_send_hook`` — outbound chain runs in registration
  order, can transform or drop.
* ``AgentClient.add_receive_hook`` — inbound chain runs before inbox
  fan-out and notify handler.
* Drop semantics: a hook returning ``None`` short-circuits the chain
  and skips dispatch (send) or delivery (receive).
* Hub-side WAL is unaffected by client-side receive hooks — the same
  envelope is still durable in the session's WAL.
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
)
from autogen.beta.testing import TestConfig


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


async def _two_party(hub: Hub) -> tuple[object, object]:
    link = LocalLink(hub)
    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    return alice, bob


@pytest.mark.asyncio
async def test_send_hook_transforms_envelope_before_post() -> None:
    """A send hook can rewrite event_data; the WAL records the transformed body."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, bob = await _two_party(hub)

    async def redact(envelope: Envelope) -> Envelope:
        if envelope.event_type == EV_TEXT:
            envelope.event_data = {"text": "[REDACTED]"}
        return envelope

    alice.add_send_hook(redact)

    session = await alice.open(type="consulting", target="bob")
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    envelope_id = await session.send("secret payload", audience=audience)

    wal = await hub.read_wal(session.session_id)
    posted = next(e for e in wal if e.envelope_id == envelope_id)
    assert posted.event_data == {"text": "[REDACTED]"}


@pytest.mark.asyncio
async def test_send_hook_returning_none_drops_envelope() -> None:
    """A hook returning None short-circuits; nothing reaches the hub."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, _bob = await _two_party(hub)

    async def block_all(_envelope: Envelope) -> Envelope | None:
        return None

    alice.add_send_hook(block_all)

    session = await alice.open(type="consulting", target="bob")
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    returned_id = await session.send("never reaches hub", audience=audience)

    assert returned_id == ""
    wal = await hub.read_wal(session.session_id)
    text_envelopes = [e for e in wal if e.event_type == EV_TEXT]
    assert text_envelopes == []


@pytest.mark.asyncio
async def test_send_hooks_run_in_registration_order() -> None:
    """Two hooks compose left-to-right; the second sees the first's output."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, _bob = await _two_party(hub)

    async def first(envelope: Envelope) -> Envelope:
        envelope.event_data = {"text": envelope.event_data["text"] + "-A"}
        return envelope

    async def second(envelope: Envelope) -> Envelope:
        envelope.event_data = {"text": envelope.event_data["text"] + "-B"}
        return envelope

    alice.add_send_hook(first)
    alice.add_send_hook(second)

    session = await alice.open(type="consulting", target="bob")
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    envelope_id = await session.send("base", audience=audience)

    wal = await hub.read_wal(session.session_id)
    posted = next(e for e in wal if e.envelope_id == envelope_id)
    assert posted.event_data == {"text": "base-A-B"}


@pytest.mark.asyncio
async def test_receive_hook_observes_inbound_envelope() -> None:
    """A receive hook fires on hub-delivered envelopes for this client."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, bob = await _two_party(hub)

    seen: list[Envelope] = []

    async def observe(envelope: Envelope) -> Envelope:
        seen.append(envelope)
        return envelope

    bob.add_receive_hook(observe)

    session = await alice.open(type="consulting", target="bob")
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    await session.send("hello bob", audience=audience)

    await bob.wait_for_session_event(
        session_id=session.session_id,
        predicate=lambda e: e.event_type == EV_TEXT,
        timeout=5.0,
    )

    text_seen = [e for e in seen if e.event_type == EV_TEXT]
    assert len(text_seen) == 1
    assert text_seen[0].event_data == {"text": "hello bob"}


@pytest.mark.asyncio
async def test_receive_hook_returning_none_blocks_delivery_but_preserves_wal() -> None:
    """Drop on receive: handler does not run, but the envelope is still in the WAL."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, bob = await _two_party(hub)

    # Open session first so bob's default handler auto-acks the invite.
    session = await alice.open(type="consulting", target="bob")

    handler_calls: list[Envelope] = []

    async def custom_handler(envelope: Envelope) -> None:
        handler_calls.append(envelope)

    bob.on_envelope(custom_handler)

    async def drop_text(envelope: Envelope) -> Envelope | None:
        if envelope.event_type == EV_TEXT:
            return None
        return envelope

    bob.add_receive_hook(drop_text)

    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    envelope_id = await session.send("blocked client-side", audience=audience)

    # WAL still has it (hub never sees the client-side hook).
    wal = await hub.read_wal(session.session_id)
    posted = next(e for e in wal if e.envelope_id == envelope_id)
    assert posted.event_data == {"text": "blocked client-side"}

    # But bob's custom handler never saw the EV_TEXT.
    text_calls = [e for e in handler_calls if e.event_type == EV_TEXT]
    assert text_calls == []


@pytest.mark.asyncio
async def test_receive_hook_can_rewrite_envelope_seen_by_handler() -> None:
    """A receive hook's transformed envelope is what the handler observes."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, bob = await _two_party(hub)

    # Open session first so bob's default handler auto-acks the invite.
    session = await alice.open(type="consulting", target="bob")

    handler_calls: list[Envelope] = []

    async def custom_handler(envelope: Envelope) -> None:
        handler_calls.append(envelope)

    bob.on_envelope(custom_handler)

    async def rewrite(envelope: Envelope) -> Envelope:
        if envelope.event_type == EV_TEXT:
            envelope.event_data = {"text": "REWRITTEN"}
        return envelope

    bob.add_receive_hook(rewrite)

    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    await session.send("original", audience=audience)

    text_calls: list[Envelope] = []
    for _ in range(50):
        text_calls = [e for e in handler_calls if e.event_type == EV_TEXT]
        if text_calls:
            break
        await asyncio.sleep(0.02)

    assert len(text_calls) == 1
    assert text_calls[0].event_data == {"text": "REWRITTEN"}
