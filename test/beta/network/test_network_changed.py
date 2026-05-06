# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Cut 3.2 — ``NetworkChangedFrame`` + ``HubClient`` discovery cache.

The hub broadcasts a ``NetworkChangedFrame`` to every bound endpoint
on identity mutation (register / unregister / set_resume / set_skill /
record_observation). ``HubClient`` keeps a small dict cache around the
discovery passthroughs and clears it on inbound ``NetworkChangedFrame``.

Coverage:

* ``list_agents`` second call hits the cache (no extra hub work)
* register on a peer broadcasts ``NetworkChangedFrame`` and invalidates
* unregister broadcasts and invalidates
* ``set_resume`` and ``set_skill`` broadcast and invalidate
* the frame's ``change`` and ``agent_id`` fields are populated
"""

import asyncio

import pytest

from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.transport.frames import NetworkChangedFrame
from autogen.beta.testing import TestConfig


def _agent(name: str) -> Agent:
    return Agent(name=name, config=TestConfig())


@pytest.mark.asyncio
async def test_list_agents_cached_until_invalidated() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    # Prime the cache from alice's hub client.
    first = await alice_hc.list_agents()
    second = await alice_hc.list_agents()
    assert first == second
    assert len(first) == 2

    # Same args land on the cache — verify by checking the dict directly.
    assert ("list_agents", None, None, None, 50) in alice_hc._discovery_cache

    await alice_hc.shutdown()
    await bob_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_register_broadcasts_and_invalidates_cache() -> None:
    """A peer registering invalidates alice's discovery cache."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())

    initial = await alice_hc.list_agents()
    assert len(initial) == 1
    assert ("list_agents", None, None, None, 50) in alice_hc._discovery_cache

    # Bob joins. Hub broadcasts NetworkChangedFrame to alice's endpoint.
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    # Yield so alice's receive loop processes the frame.
    await asyncio.sleep(0.05)

    # Cache cleared by the inbound NetworkChangedFrame.
    assert alice_hc._discovery_cache == {}

    # Next call sees both peers.
    refreshed = await alice_hc.list_agents()
    assert len(refreshed) == 2

    await alice_hc.shutdown()
    await bob_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_unregister_invalidates_cache() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    await alice_hc.list_agents()
    assert alice_hc._discovery_cache  # populated

    await bob_hc.unregister_agent(bob.agent_id)
    await asyncio.sleep(0.05)

    assert alice_hc._discovery_cache == {}

    # Final view shows only alice.
    after = await alice_hc.list_agents()
    assert len(after) == 1
    assert after[0].name == "alice"

    await alice_hc.shutdown()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_set_resume_invalidates_cache() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    # Prime alice's cache with bob's resume.
    await alice_hc.get_resume(bob.agent_id)
    assert ("get_resume", bob.agent_id) in alice_hc._discovery_cache

    new_resume = Resume(claimed_capabilities=["math"], summary="updated")
    await bob_hc.set_resume(bob.agent_id, new_resume)
    await asyncio.sleep(0.05)

    assert alice_hc._discovery_cache == {}

    refreshed = await alice_hc.get_resume(bob.agent_id)
    assert "math" in refreshed.claimed_capabilities

    await alice_hc.shutdown()
    await bob_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_set_skill_invalidates_cache() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    await alice_hc.get_skill(bob.agent_id)
    assert ("get_skill", bob.agent_id) in alice_hc._discovery_cache

    await bob_hc.set_skill(bob.agent_id, "# bob skill\nDoes math.")
    await asyncio.sleep(0.05)

    assert alice_hc._discovery_cache == {}

    refreshed = await alice_hc.get_skill(bob.agent_id)
    assert refreshed is not None
    assert "math" in refreshed

    await alice_hc.shutdown()
    await bob_hc.shutdown()
    await hub.close()


@pytest.mark.asyncio
async def test_broadcast_payload_shape() -> None:
    """Hub broadcast carries the right ``change`` and ``agent_id``."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    captured: list[object] = []

    class _CaptureEndpoint:
        endpoint_id = "capture"
        agent_id = "bound-fake"

        async def send_frame(self, frame: object) -> None:
            captured.append(frame)

        async def close(self) -> None:
            return None

    hub._endpoints_by_id["capture"] = _CaptureEndpoint()  # type: ignore[assignment]

    await hub._broadcast_network_changed("agent_registered", "abc")
    await hub._broadcast_network_changed("resume_set", "def")

    assert len(captured) == 2
    assert isinstance(captured[0], NetworkChangedFrame)
    assert captured[0].change == "agent_registered"
    assert captured[0].agent_id == "abc"
    assert isinstance(captured[1], NetworkChangedFrame)
    assert captured[1].change == "resume_set"
    assert captured[1].agent_id == "def"

    await hub.close()


@pytest.mark.asyncio
async def test_broadcast_skips_unbound_endpoints() -> None:
    """Endpoints that haven't claimed an identity yet aren't recipients."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)

    captured: list[object] = []

    class _UnboundEndpoint:
        endpoint_id = "unbound"
        agent_id = None  # never bound

        async def send_frame(self, frame: object) -> None:
            captured.append(frame)

        async def close(self) -> None:
            return None

    hub._endpoints_by_id["unbound"] = _UnboundEndpoint()  # type: ignore[assignment]

    await hub._broadcast_network_changed("agent_registered", "abc")
    assert captured == []

    await hub.close()
