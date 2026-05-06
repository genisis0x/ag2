# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``ApiKeyAuth`` adapter.

Covers:

* Valid key in static mapping → registration succeeds.
* Wrong key → ``AuthError`` raised, registration aborts.
* Unknown name (not in mapping, no resolver) → ``AuthError``.
* Resolver callable used when name is missing from the static mapping.
* Empty / missing ``key`` claim → ``AuthError``.
* Constant-time compare doesn't accept a prefix that matches the
  expected key.
"""

import pytest

from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    ApiKeyAuth,
    AuthBlock,
    AuthError,
    AuthRegistry,
    Hub,
    HubClient,
    LocalLink,
    NoAuth,
    Passport,
    Resume,
)
from autogen.beta.testing import TestConfig


def _agent(name: str) -> Agent:
    return Agent(name=name, config=TestConfig())


@pytest.mark.asyncio
async def test_valid_static_key_passes() -> None:
    auth = AuthRegistry([NoAuth(), ApiKeyAuth(keys={"alice": "s3cret"})])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    link = LocalLink(hub)
    hc = HubClient(link, hub=hub)

    passport = Passport(name="alice", auth=AuthBlock(scheme="api_key", claim={"key": "s3cret"}))
    client = await hc.register(_agent("alice"), passport, Resume())
    assert client.agent_id


@pytest.mark.asyncio
async def test_wrong_key_raises_auth_error() -> None:
    auth = AuthRegistry([NoAuth(), ApiKeyAuth(keys={"alice": "right"})])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    link = LocalLink(hub)
    hc = HubClient(link, hub=hub)

    passport = Passport(name="alice", auth=AuthBlock(scheme="api_key", claim={"key": "wrong"}))
    with pytest.raises(AuthError, match="api_key mismatch"):
        await hc.register(_agent("alice"), passport, Resume())


@pytest.mark.asyncio
async def test_unknown_identity_fails_closed() -> None:
    auth = AuthRegistry([NoAuth(), ApiKeyAuth(keys={"alice": "k"})])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    link = LocalLink(hub)
    hc = HubClient(link, hub=hub)

    passport = Passport(name="bob", auth=AuthBlock(scheme="api_key", claim={"key": "k"}))
    with pytest.raises(AuthError, match="unknown identity"):
        await hc.register(_agent("bob"), passport, Resume())


@pytest.mark.asyncio
async def test_resolver_callable_used_when_static_misses() -> None:
    lookups: list[str] = []

    def resolve(name: str) -> str | None:
        lookups.append(name)
        return "dynamic-key" if name == "carol" else None

    auth = AuthRegistry([NoAuth(), ApiKeyAuth(resolver=resolve)])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    link = LocalLink(hub)
    hc = HubClient(link, hub=hub)

    passport = Passport(name="carol", auth=AuthBlock(scheme="api_key", claim={"key": "dynamic-key"}))
    await hc.register(_agent("carol"), passport, Resume())
    assert lookups == ["carol"]


@pytest.mark.asyncio
async def test_missing_key_claim_raises() -> None:
    auth = AuthRegistry([NoAuth(), ApiKeyAuth(keys={"alice": "k"})])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    link = LocalLink(hub)
    hc = HubClient(link, hub=hub)

    passport = Passport(name="alice", auth=AuthBlock(scheme="api_key", claim={}))
    with pytest.raises(AuthError, match="missing 'key'"):
        await hc.register(_agent("alice"), passport, Resume())


@pytest.mark.asyncio
async def test_prefix_match_does_not_pass() -> None:
    """``hmac.compare_digest`` must reject a prefix of the expected key."""
    auth = AuthRegistry([NoAuth(), ApiKeyAuth(keys={"alice": "long-secret-key"})])
    hub = await Hub.open(MemoryKnowledgeStore(), auth=auth, ttl_sweep_interval=0)
    link = LocalLink(hub)
    hc = HubClient(link, hub=hub)

    passport = Passport(name="alice", auth=AuthBlock(scheme="api_key", claim={"key": "long-secret"}))
    with pytest.raises(AuthError, match="api_key mismatch"):
        await hc.register(_agent("alice"), passport, Resume())
