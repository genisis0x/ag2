# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""HTTP CRUD surface (10 routes) via Starlette.

Tests drive the ASGI app directly through ``httpx.AsyncClient`` +
``httpx.ASGITransport``. No real network socket is opened.

Covers:

* register / get_agent / list_agents / unregister round-trips
* create_session / get_session / close_session lifecycle
* post_envelope (substantive event after session is active)
* read_wal returns the persisted envelopes
* list_sessions filters by ``include_terminal``
* Auth middleware: NoAuth-only registry skips, ApiKeyAuth registry
  rejects missing creds and accepts valid ones
* Errors map to standard HTTP status codes
"""

import httpx
import pytest

from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    ApiKeyAuth,
    AuthBlock,
    AuthRegistry,
    Hub,
    HubClient,
    LocalLink,
    NoAuth,
    Passport,
    Resume,
    SessionState,
    make_http_app,
)
from autogen.beta.testing import TestConfig
from autogen.beta import Agent


def _agent(name: str) -> Agent:
    return Agent(name=name, config=TestConfig())


def _http_client(hub: Hub) -> httpx.AsyncClient:
    """ASGI-driven client — no socket, identical wire shape."""
    transport = httpx.ASGITransport(app=make_http_app(hub))
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
async def test_register_then_get_then_list_then_unregister() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    async with _http_client(hub) as client:
        body = {
            "passport": Passport(name="alice").to_dict(),
            "resume": Resume(claimed_capabilities=["math"]).to_dict(),
        }
        resp = await client.post("/agents", json=body)
        assert resp.status_code == 201
        passport = resp.json()["passport"]
        assert passport["name"] == "alice"
        assert passport["agent_id"]

        resp = await client.get(f"/agents/{passport['agent_id']}")
        assert resp.status_code == 200
        assert resp.json()["passport"]["name"] == "alice"

        resp = await client.get("/agents", params={"capability": "math"})
        assert resp.status_code == 200
        names = [p["name"] for p in resp.json()["agents"]]
        assert "alice" in names

        resp = await client.delete(f"/agents/{passport['agent_id']}")
        assert resp.status_code == 204

        resp = await client.get(f"/agents/{passport['agent_id']}")
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_session_lifecycle_via_http() -> None:
    """Create + get + close round-trip — uses an in-process registration
    for the participants because the invite-ack ceremony still goes
    through the dispatch path (LocalLink). The HTTP surface here covers
    correctness for the control plane."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    session = await alice.open(type="conversation", target="bob")

    async with _http_client(hub) as client:
        resp = await client.get(f"/sessions/{session.session_id}")
        assert resp.status_code == 200
        assert resp.json()["session"]["state"] == SessionState.ACTIVE.value

        resp = await client.get("/sessions", params={"agent_id": alice.agent_id})
        assert resp.status_code == 200
        ids = [s["session_id"] for s in resp.json()["sessions"]]
        assert session.session_id in ids

        resp = await client.post(f"/sessions/{session.session_id}/close", json={"reason": "done"})
        assert resp.status_code == 200
        assert resp.json()["session"]["state"] == SessionState.CLOSED.value


@pytest.mark.asyncio
async def test_post_envelope_and_read_wal() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    session = await alice.open(type="conversation", target="bob")

    async with _http_client(hub) as client:
        envelope_body = {
            "envelope": {
                "session_id": session.session_id,
                "sender_id": alice.agent_id,
                "audience": [bob.agent_id],
                "event_type": "ag2.msg.text",
                "event_data": {"text": "hi over http"},
            },
        }
        resp = await client.post(
            f"/sessions/{session.session_id}/envelopes",
            json=envelope_body,
        )
        assert resp.status_code == 201
        envelope_id = resp.json()["envelope_id"]
        assert envelope_id

        resp = await client.get(f"/sessions/{session.session_id}/wal")
        assert resp.status_code == 200
        envelopes = resp.json()["envelopes"]
        text_envelopes = [e for e in envelopes if e["event_type"] == "ag2.msg.text"]
        assert len(text_envelopes) == 1
        assert text_envelopes[0]["envelope_id"] == envelope_id


@pytest.mark.asyncio
async def test_list_sessions_filter_terminal() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice_hc = HubClient(LocalLink(hub), hub=hub)
    bob_hc = HubClient(LocalLink(hub), hub=hub)
    alice = await alice_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    session = await alice.open(type="conversation", target="bob")
    await session.close()

    async with _http_client(hub) as client:
        resp = await client.get("/sessions", params={"agent_id": alice.agent_id})
        assert resp.status_code == 200
        assert resp.json()["sessions"] == []  # CLOSED filtered out by default

        resp = await client.get(
            "/sessions",
            params={"agent_id": alice.agent_id, "include_terminal": "true"},
        )
        assert resp.status_code == 200
        states = [s["state"] for s in resp.json()["sessions"]]
        assert SessionState.CLOSED.value in states


@pytest.mark.asyncio
async def test_apikey_auth_rejects_missing_credentials() -> None:
    auth = AuthRegistry([NoAuth(), ApiKeyAuth(keys={"alice": "k"})])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    alice_hc = HubClient(LocalLink(hub), hub=hub)
    await alice_hc.register(
        _agent("alice"),
        Passport(name="alice", auth=AuthBlock(scheme="api_key", claim={"key": "k"})),
        Resume(),
    )

    async with _http_client(hub) as client:
        resp = await client.get("/agents/alice")
        assert resp.status_code == 401

        resp = await client.get("/agents/alice", headers={"X-Agent-Name": "alice", "X-Api-Key": "wrong"})
        assert resp.status_code == 401

        resp = await client.get("/agents/alice", headers={"X-Agent-Name": "alice", "X-Api-Key": "k"})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_unknown_session_returns_404() -> None:
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    async with _http_client(hub) as client:
        resp = await client.get("/sessions/missing")
        assert resp.status_code == 404
        assert resp.json()["code"] == "not_found"
