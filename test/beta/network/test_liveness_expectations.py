# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Expectation evaluators + violation handlers.

Each test drives the hub's :meth:`evaluate_expectations` directly with
a controllable clock so the sweeper logic is exercised deterministically.
"""

import asyncio
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
from autogen.beta.network.adapters.base import SessionAdapter
from autogen.beta.network.adapters.conversation import (
    CONVERSATION_TYPE,
    ConversationAdapter,
    ConversationState,
)
from autogen.beta.network.adapters.discussion import DiscussionAdapter
from autogen.beta.network.errors import ProtocolError
from autogen.beta.network.hub.layout import session_removed_path
from autogen.beta.network.session import (
    Expectation,
    ParticipantSchema,
    SessionManifest,
    SessionMetadata,
    SessionState,
)
from autogen.beta.testing import TestConfig

from ._helpers import _MockClock


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


class _CustomConversationAdapter(ConversationAdapter):
    """Conversation adapter with custom expectations baked in.

    Re-uses ``ConversationAdapter`` semantics — bidirectional, no
    auto-close — but lets each test declare its own expectation set
    via the constructor without hand-rolling the validate/fold/accept
    code.
    """

    def __init__(self, type_name: str, expectations: list[Expectation]) -> None:
        super().__init__()
        self.manifest = SessionManifest(
            type=type_name,
            version=1,
            participants=ParticipantSchema(min=2, max=2),
            knobs_schema=dict(self.manifest.knobs_schema),
            default_view_policy=self.manifest.default_view_policy,
            expectations=expectations,
        )


@pytest.mark.asyncio
async def test_min_participation_fires_when_silent_member_lags() -> None:
    """Bob hasn't sent any content within the window → violator."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_min_participation",
            expectations=[
                Expectation(
                    name="min_participation",
                    on_violation="audit",
                    params={"count": 1, "window_seconds": 60},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conv_min_participation", target=bob.agent_id)

    # Alice posts; bob stays silent. Advance past the window.
    clock.advance(5)
    await session.send("just me", audience=[bob.agent_id])
    clock.advance(120)

    await hub.evaluate_expectations()

    audit_lines = (await store.read("/audit/audit.jsonl") or "").strip().splitlines()
    audit_records = [json.loads(line) for line in audit_lines]
    violations = [
        r for r in audit_records
        if r.get("kind") == "expectation_violated"
        and r.get("expectation") == "min_participation"
    ]
    assert len(violations) >= 1
    # Bob is silent within the window. Alice posted at t=5 but the
    # window is t=65..125 by the time we evaluate, so alice is
    # *also* below threshold — both end up as violators here.
    assert bob.agent_id in violations[0]["violators"]

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_progress_within_fires_for_stalled_task() -> None:
    """A task with TaskStarted but no progress past T → fires."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_progress_within",
            expectations=[
                Expectation(
                    name="progress_within",
                    on_violation="audit",
                    params={"seconds": 30},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conv_progress_within", target=bob.agent_id)

    # Bob starts a task into the session WAL but never reports progress.
    started = Envelope(
        session_id=session.session_id,
        sender_id=bob.agent_id,
        audience=None,
        event_type="ag2.task.started",
        event_data={"title": "long work"},
        task_id="task-stall-1",
    )
    await hub.post_envelope(started)

    # No violation yet — within threshold.
    clock.advance(10)
    await hub.evaluate_expectations()
    audit_after_first = (await store.read("/audit/audit.jsonl") or "").strip().splitlines()
    assert not any(
        json.loads(ln).get("expectation") == "progress_within"
        for ln in audit_after_first
    )

    # Past threshold — should fire.
    clock.advance(60)
    await hub.evaluate_expectations()
    audit_records = [
        json.loads(ln)
        for ln in (await store.read("/audit/audit.jsonl") or "").strip().splitlines()
    ]
    violations = [
        r for r in audit_records
        if r.get("expectation") == "progress_within"
    ]
    assert len(violations) == 1
    assert bob.agent_id in violations[0]["violators"]
    assert "task-stall-1" in violations[0]["detail"]["stalled_tasks"]

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_progress_within_does_not_fire_after_task_completes() -> None:
    """A terminal task is no longer eligible for stall detection."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_progress_within_completes",
            expectations=[
                Expectation(
                    name="progress_within",
                    on_violation="audit",
                    params={"seconds": 30},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conv_progress_within_completes", target=bob.agent_id)
    await hub.post_envelope(
        Envelope(
            session_id=session.session_id,
            sender_id=bob.agent_id,
            audience=None,
            event_type="ag2.task.started",
            event_data={"title": "quick work"},
            task_id="task-quick-1",
        )
    )
    await hub.post_envelope(
        Envelope(
            session_id=session.session_id,
            sender_id=bob.agent_id,
            audience=None,
            event_type="ag2.task.completed",
            event_data={"result": "done"},
            task_id="task-quick-1",
        )
    )

    clock.advance(120)
    await hub.evaluate_expectations()

    audit_records = [
        json.loads(ln)
        for ln in (await store.read("/audit/audit.jsonl") or "").strip().splitlines()
    ]
    assert not any(
        r.get("expectation") == "progress_within" for r in audit_records
    )

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_warn_handler_targets_violator_only() -> None:
    """``warn`` posts EV_EXPECTATION_VIOLATED with audience scoped to violators."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_warn",
            expectations=[
                Expectation(
                    name="min_participation",
                    on_violation="warn",
                    params={"count": 1, "window_seconds": 60},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conv_warn", target=bob.agent_id)
    clock.advance(5)
    await session.send("hi", audience=[bob.agent_id])
    clock.advance(120)
    await hub.evaluate_expectations()

    wal = await hub.read_wal(session.session_id)
    violations = [e for e in wal if e.event_type == "ag2.expectation.violated"]
    # At least one warn envelope landed; audience is scoped to the
    # listed violators (not a broadcast).
    assert len(violations) >= 1
    audience = violations[0].audience
    assert audience is not None and bob.agent_id in audience

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_hide_handler_suppresses_notify_to_violator() -> None:
    """Hidden agent's WAL view stays current, but live notifies stop."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_hide",
            expectations=[
                # Window 200s so alice's t=5 send counts at evaluate time
                # but bob (who never speaks) still violates.
                Expectation(
                    name="min_participation",
                    on_violation="hide",
                    params={"count": 1, "window_seconds": 200},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    # Open first so bob's default handler auto-acks the invite. Then
    # swap to a capture-only handler so we can count what arrives.
    session = await alice.open(type="conv_hide", target=bob.agent_id)

    bob_received: list[Envelope] = []

    async def _capture(env: Envelope) -> None:
        bob_received.append(env)

    bob.on_envelope(_capture)

    clock.advance(5)
    await session.send("first", audience=[bob.agent_id])
    # NotifyFrame travels via an asyncio.Queue; yield once to let it drain.
    await asyncio.sleep(0.01)
    received_before_hide = len(bob_received)
    assert received_before_hide >= 1  # bob saw the first message

    clock.advance(120)
    await hub.evaluate_expectations()
    assert hub.is_hidden(session.session_id, bob.agent_id)

    # Alice posts again — bob shouldn't receive a notify.
    await session.send("second", audience=[bob.agent_id])
    await asyncio.sleep(0.01)
    received_after_hide = len(bob_received)
    assert received_after_hide == received_before_hide

    # WAL still records both envelopes — audit truth is unchanged.
    wal = await hub.read_wal(session.session_id)
    text_envelopes = [e for e in wal if e.event_type == EV_TEXT]
    assert len(text_envelopes) == 2

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_remove_handler_bars_sender_and_persists(tmp_path) -> None:
    """``remove`` blocks future sends from the violator and survives hub restart."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = DiskKnowledgeStore(str(tmp_path))
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_remove",
            expectations=[
                # Window 200s so alice's send at t=5 counts at evaluate
                # time (t=125), pinning the violation on bob alone.
                Expectation(
                    name="min_participation",
                    on_violation="remove",
                    params={"count": 1, "window_seconds": 200},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conv_remove", target=bob.agent_id)
    clock.advance(5)
    await session.send("alone", audience=[bob.agent_id])
    clock.advance(120)
    await hub.evaluate_expectations()

    assert hub.is_removed(session.session_id, bob.agent_id)

    # Subsequent substantive send from bob is rejected.
    rejected = Envelope(
        session_id=session.session_id,
        sender_id=bob.agent_id,
        audience=[alice.agent_id],
        event_type=EV_TEXT,
        event_data={"text": "let me back in"},
    )
    with pytest.raises(ProtocolError):
        await hub.post_envelope(rejected)

    # Persistence: removed.json on disk.
    body = await store.read(session_removed_path(session.session_id))
    assert body is not None
    assert json.loads(body) == [bob.agent_id]

    await a_hc.close()
    await b_hc.close()
    await hub.close()

    # Restore: a fresh hub against the same store re-applies the bar.
    store2 = DiskKnowledgeStore(str(tmp_path))
    hub2 = await Hub.open(
        store2,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    assert hub2.is_removed(session.session_id, bob.agent_id)

    # And still rejects bob's send after restore.
    rejected2 = Envelope(
        session_id=session.session_id,
        sender_id=bob.agent_id,
        audience=[alice.agent_id],
        event_type=EV_TEXT,
        event_data={"text": "hello again"},
    )
    with pytest.raises(ProtocolError):
        await hub2.post_envelope(rejected2)

    await hub2.close()


@pytest.mark.asyncio
async def test_turn_within_fires_for_idle_workflow_speaker() -> None:
    """Discussion(round_robin) — bob is expected next, doesn't post → fires."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )

    # Custom discussion adapter with turn_within(audit, 30s) baked in
    # so we can drive evaluator semantics without engaging an LLM.
    base = DiscussionAdapter()
    custom = DiscussionAdapter()
    custom.manifest = SessionManifest(
        type="discussion_turn_within",
        version=1,
        participants=ParticipantSchema(min=3, max=10),
        knobs_schema=dict(base.manifest.knobs_schema),
        default_view_policy=base.manifest.default_view_policy,
        expectations=[
            Expectation(
                name="turn_within",
                on_violation="audit",
                params={"seconds": 30},
            ),
        ],
    )
    hub.register_adapter(custom)

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    c_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    carol = await c_hc.register(_agent("carol"), Passport(name="carol"), Resume())

    session = await alice.open(
        type="discussion_turn_within",
        target=[bob.agent_id, carol.agent_id],
        knobs={"ordering": "round_robin"},
    )

    # Open first so all three default handlers auto-ack the invite.
    # Then swap bob (and carol) to no-ops so the trigger from alice
    # stays unanswered through the test window.
    async def _noop(_env: object) -> None:
        return None

    bob.on_envelope(_noop)
    carol.on_envelope(_noop)

    # Alice posts first (initiator is always position 0 → first speaker).
    clock.advance(2)
    await session.send("kicking off")
    # Bob is now expected_next_speaker; he stays silent.
    clock.advance(120)
    await hub.evaluate_expectations()

    audit_records = [
        json.loads(ln)
        for ln in (await store.read("/audit/audit.jsonl") or "").strip().splitlines()
    ]
    violations = [
        r for r in audit_records
        if r.get("expectation") == "turn_within"
    ]
    assert len(violations) == 1
    assert violations[0]["violators"] == [bob.agent_id]

    await a_hc.close()
    await b_hc.close()
    await c_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_turn_within_skipped_for_adapters_without_expected_speaker() -> None:
    """Consulting/conversation have no ``expected_next_speaker`` — evaluator returns None."""
    clock = _MockClock("2026-01-01T00:00:00+00:00")
    store = MemoryKnowledgeStore()
    hub = await Hub.open(
        store,
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
        clock=clock,
    )
    hub.register_adapter(
        _CustomConversationAdapter(
            type_name="conv_turn_within_noop",
            expectations=[
                Expectation(
                    name="turn_within",
                    on_violation="audit",
                    params={"seconds": 30},
                ),
            ],
        )
    )

    link = LocalLink(hub)
    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conv_turn_within_noop", target=bob.agent_id)
    clock.advance(5)
    await session.send("hi", audience=[bob.agent_id])
    clock.advance(120)
    await hub.evaluate_expectations()

    audit_lines = (await store.read("/audit/audit.jsonl") or "").strip().splitlines()
    audit_records = [json.loads(line) for line in audit_lines]
    # No turn_within violations — adapter state has no expected_next_speaker.
    assert not any(
        r.get("expectation") == "turn_within" for r in audit_records
    )

    await a_hc.close()
    await b_hc.close()
    await hub.close()
