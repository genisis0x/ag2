# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""M3 cut 3.4 — resume observation, capability index, skill render, mutation.

Three layers covered:

* **Skill render** (unit) — ``parse_skill_frontmatter`` + ``render_fallback_skill``.
* **Capability index + record_observation** (integration) — register
  populates the index; ``record_observation`` updates ``Resume.observed``
  and adds the agent to the capability bucket; ``unregister`` removes
  the agent and prunes empty buckets; the index round-trips through
  ``Hub.hydrate()``; the on-disk JSON cache reflects the in-memory state.
* **AgentClient mutation** — ``set_resume`` / ``add_example`` go through
  the hub and refresh the local cache.
* **TaskMirror end-to-end** — ``Agent.task(..., capability="X")`` inside
  a notify-handler turn drives ``Hub.record_observation`` automatically.
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
    ResumeExample,
)
from autogen.beta.network.client.skill_render import (
    parse_skill_frontmatter,
    render_fallback_skill,
)
from autogen.beta.network.hub.layout import by_capability_path
from autogen.beta.network.identity import (
    ObservedStat,
    Resume,
)
from autogen.beta.task import TaskState
from autogen.beta.testing import TestConfig

from ._helpers import ScriptedConfig, wait_for_text_count


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


# ── Skill render unit tests ─────────────────────────────────────────────────


class TestParseSkillFrontmatter:
    def test_parses_basic_frontmatter_and_body(self) -> None:
        md = (
            "---\n"
            "name: alice\n"
            "description: Senior policy analyst.\n"
            "---\n"
            "\n"
            "## What I do\n"
            "\n"
            "Cost-benefit framing.\n"
        )
        parsed = parse_skill_frontmatter(md)
        assert parsed.frontmatter == {
            "name": "alice",
            "description": "Senior policy analyst.",
        }
        assert "## What I do" in parsed.body
        assert parsed.body.startswith("\n## What I do") or parsed.body.startswith("## What I do")

    def test_no_frontmatter_returns_full_body(self) -> None:
        md = "# alice\n\njust a body\n"
        parsed = parse_skill_frontmatter(md)
        assert parsed.frontmatter == {}
        assert parsed.body == md

    def test_unterminated_frontmatter_returns_full_body(self) -> None:
        md = "---\nname: alice\nbut no closing fence"
        parsed = parse_skill_frontmatter(md)
        assert parsed.frontmatter == {}
        assert parsed.body == md

    def test_skips_empty_and_comment_lines(self) -> None:
        md = "---\nname: alice\n\n# a comment\nrole: analyst\n---\nbody"
        parsed = parse_skill_frontmatter(md)
        assert parsed.frontmatter == {"name": "alice", "role": "analyst"}


class TestRenderFallbackSkill:
    def test_includes_capabilities_domains_and_summary(self) -> None:
        passport = Passport(name="alice")
        resume = Resume(
            claimed_capabilities=["debate", "analysis"],
            domains=["policy", "economics"],
            summary="Senior policy analyst.",
        )
        rendered = render_fallback_skill(passport, resume)
        assert rendered.startswith("---\n")
        assert "name: alice" in rendered
        assert "description: Senior policy analyst." in rendered
        assert "## Capabilities" in rendered
        assert "- debate" in rendered
        assert "## Domains" in rendered

    def test_includes_observed_track_record(self) -> None:
        passport = Passport(name="bob")
        resume = Resume(
            claimed_capabilities=["analysis"],
            observed={
                "analysis": ObservedStat(n=3, completed=2, failed=1),
            },
        )
        rendered = render_fallback_skill(passport, resume)
        assert "## Track record" in rendered
        assert "analysis" in rendered
        assert "n=3" in rendered

    def test_minimal_resume_renders_default_description(self) -> None:
        passport = Passport(name="solo")
        resume = Resume()
        rendered = render_fallback_skill(passport, resume)
        assert "name: solo" in rendered
        assert "Network-registered agent." in rendered


# ── Capability index integration tests ──────────────────────────────────────


@pytest.mark.asyncio
async def test_register_populates_capability_index() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"),
        Passport(name="alice"),
        Resume(claimed_capabilities=["debate", "analysis"]),
    )

    assert hub.agents_with_capability("debate") == [alice.agent_id]
    assert hub.agents_with_capability("analysis") == [alice.agent_id]
    assert hub.agents_with_capability("missing") == []

    await alice_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_unregister_removes_from_capability_index() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume(claimed_capabilities=["debate"])
    )
    bob = await bob_hc.register(
        _agent("bob"), Passport(name="bob"), Resume(claimed_capabilities=["debate"])
    )

    assert set(hub.agents_with_capability("debate")) == {alice.agent_id, bob.agent_id}

    await hub.unregister(alice.agent_id)
    assert hub.agents_with_capability("debate") == [bob.agent_id]

    await hub.unregister(bob.agent_id)
    assert hub.agents_with_capability("debate") == []
    # Empty bucket pruned from the index entirely.
    assert "debate" not in hub._capability_index

    await alice_hc.close()
    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_capability_index_persisted_to_disk(tmp_path) -> None:
    store = DiskKnowledgeStore(str(tmp_path))
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"),
        Passport(name="alice"),
        Resume(claimed_capabilities=["debate"]),
    )

    raw = await store.read(by_capability_path())
    assert raw is not None
    snapshot = json.loads(raw)
    assert snapshot == {"debate": [alice.agent_id]}

    await alice_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_capability_index_rebuilt_on_hydrate(tmp_path) -> None:
    store = DiskKnowledgeStore(str(tmp_path))
    hub1 = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link1 = LocalLink(hub1)

    alice_hc = HubClient(link1, hub=hub1)
    alice = await alice_hc.register(
        _agent("alice"),
        Passport(name="alice"),
        Resume(
            claimed_capabilities=["debate"],
            observed={"reviews": ObservedStat(n=5, completed=4, failed=1)},
        ),
    )

    await alice_hc.close()
    await hub1.close()

    store2 = DiskKnowledgeStore(str(tmp_path))
    hub2 = await Hub.open(store2, ttl_sweep_interval=0, expectation_sweep_interval=0)

    # Both claimed and observed capabilities show up in the rebuilt index.
    assert hub2.agents_with_capability("debate") == [alice.agent_id]
    assert hub2.agents_with_capability("reviews") == [alice.agent_id]

    await hub2.close()


# ── record_observation tests ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_record_observation_updates_resume_observed_counters() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"),
        Passport(name="alice"),
        Resume(claimed_capabilities=["analysis"]),
    )

    await hub.record_observation(
        owner_id=alice.agent_id,
        capability="analysis",
        outcome=TaskState.COMPLETED,
        latency_ms=420,
    )
    await hub.record_observation(
        owner_id=alice.agent_id,
        capability="analysis",
        outcome=TaskState.FAILED,
    )
    await hub.record_observation(
        owner_id=alice.agent_id,
        capability="analysis",
        outcome=TaskState.EXPIRED,
    )

    resume = await hub.get_resume(alice.agent_id)
    stat = resume.observed["analysis"]
    assert stat.n == 3
    assert stat.completed == 1
    assert stat.failed == 1
    assert stat.expired == 1
    assert stat.p50_latency_ms == 420  # last observed value (V1 placeholder)

    await alice_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_record_observation_adds_unclaimed_capability_to_index() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume()  # no claims
    )

    assert hub.agents_with_capability("emergent") == []

    await hub.record_observation(
        owner_id=alice.agent_id,
        capability="emergent",
        outcome=TaskState.COMPLETED,
    )

    assert hub.agents_with_capability("emergent") == [alice.agent_id]

    await alice_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_record_observation_ignores_non_terminal_state() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume()
    )

    await hub.record_observation(
        owner_id=alice.agent_id,
        capability="x",
        outcome=TaskState.RUNNING,
    )

    resume = await hub.get_resume(alice.agent_id)
    assert "x" not in resume.observed

    await alice_hc.close()
    await hub.close()


# ── AgentClient mutation tests ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_agent_client_set_resume_refreshes_local_cache() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume(summary="initial")
    )

    await alice.set_resume(Resume(summary="updated", claimed_capabilities=["x"]))

    assert alice.resume.summary == "updated"
    assert "x" in alice.resume.claimed_capabilities
    assert hub.agents_with_capability("x") != [alice.agent_id]
    # set_resume doesn't currently re-index claims (that's covered by
    # register / record_observation). Just verify cache refresh works.

    await alice_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_agent_client_add_example_appends() -> None:
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    alice_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume()
    )
    assert alice.resume.examples == []

    await alice.add_example(
        ResumeExample(title="reviewed PR #42", outcome="completed")
    )
    await alice.add_example(
        ResumeExample(title="triaged incident #7", outcome="completed")
    )

    fresh = await hub.get_resume(alice.agent_id)
    titles = [e.title for e in fresh.examples]
    assert titles == ["reviewed PR #42", "triaged incident #7"]

    await alice_hc.close()
    await hub.close()


# ── TaskMirror end-to-end via Task lifecycle ────────────────────────────────


@pytest.mark.asyncio
async def test_task_mirror_records_observation_on_capability_tagged_task() -> None:
    """Running ``agent.task(capability=X)`` to completion through a mirror
    auto-calls ``Hub.record_observation`` and updates ``Resume.observed[X]``.

    The full notify-handler integration (mirror attached automatically by
    the default handler when the LLM calls a ``tasks(action="start")``
    tool) lands in cut 3.5; this test exercises the mirror plumbing
    directly so the cut 3.4 contract is verified without depending on
    the tool surface.
    """
    from autogen.beta import Context
    from autogen.beta.network.task_mirror import TaskMirror
    from autogen.beta.stream import MemoryStream

    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    bob_hc = HubClient(link, hub=hub)
    bob_agent = Agent(name="bob", config=ScriptedConfig("ack"))
    bob = await bob_hc.register(
        bob_agent,
        Passport(name="bob"),
        Resume(claimed_capabilities=["analysis"]),
    )

    stream = MemoryStream()
    mirror = TaskMirror(hub=hub, owner_id=bob.agent_id)
    sub_ids = mirror.attach(stream)
    try:
        async with bob_agent.task(
            "analysing alice's question",
            capability="analysis",
            context=Context(stream=stream),
        ) as task:
            await task.complete(result="done")
    finally:
        mirror.detach(stream, sub_ids)

    fresh = await hub.get_resume(bob.agent_id)
    assert "analysis" in fresh.observed
    stat = fresh.observed["analysis"]
    assert stat.n == 1
    assert stat.completed == 1
    assert bob.agent_id in hub.agents_with_capability("analysis")

    await bob_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_task_mirror_no_observation_when_capability_absent() -> None:
    """Untagged tasks emit lifecycle events but don't touch ``observed``."""
    from autogen.beta import Context
    from autogen.beta.network.task_mirror import TaskMirror
    from autogen.beta.stream import MemoryStream

    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    bob_hc = HubClient(link, hub=hub)
    bob_agent = Agent(name="bob", config=ScriptedConfig("ack"))
    bob = await bob_hc.register(
        bob_agent,
        Passport(name="bob"),
        Resume(),
    )

    stream = MemoryStream()
    mirror = TaskMirror(hub=hub, owner_id=bob.agent_id)
    sub_ids = mirror.attach(stream)
    try:
        async with bob_agent.task(
            "untagged work",
            context=Context(stream=stream),
        ) as task:
            await task.complete(result="ok")
    finally:
        mirror.detach(stream, sub_ids)

    fresh = await hub.get_resume(bob.agent_id)
    assert fresh.observed == {}

    await bob_hc.close()
    await hub.close()
