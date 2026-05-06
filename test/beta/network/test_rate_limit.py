# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Token-bucket rate limiter on ``Hub.post_envelope``.

Covers:

* ``per_minute = 0`` (default) — no throttle.
* Burst within budget — sequential sends succeed.
* Burst exceeded — ``N+1``-th substantive send raises ``RateLimited``.
* Time-based refill — advancing the monotonic clock restores tokens.
* Independent per-sender buckets — alice's throttle does not stop bob.
* ``set_rule`` invalidates the cached bucket — a tightened rule takes
  effect on the next post.
* Protocol envelopes bypass — invites/acks flow even when the sender
  is rate-limited (otherwise the session machine would deadlock).
"""

import pytest

from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_TEXT,
    Envelope,
    Hub,
    HubClient,
    LimitsBlock,
    LocalLink,
    Passport,
    RateBlock,
    RateLimited,
    Resume,
    Rule,
    SessionState,
)
from autogen.beta.testing import TestConfig


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


class _MonotonicClock:
    """Controllable monotonic clock for rate-limiter tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def _open_consulting(hub: Hub, *, alice_rule: Rule | None = None) -> tuple[object, object, object]:
    link = LocalLink(hub)
    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)
    alice = await alice_hc.register(
        _agent("alice"),
        Passport(name="alice"),
        Resume(),
        rule=alice_rule if alice_rule is not None else Rule(),
    )
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())
    session = await alice.open(type="conversation", target="bob")
    return alice, bob, session


@pytest.mark.asyncio
async def test_default_rule_has_no_rate_limit() -> None:
    """``RateBlock(per_minute=0)`` — sender posts unlimited substantive events."""
    hub = await Hub.open(MemoryKnowledgeStore(), ttl_sweep_interval=0)
    alice, _bob, session = await _open_consulting(hub)
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    for i in range(20):
        await session.send(f"msg {i}", audience=audience)


@pytest.mark.asyncio
async def test_burst_exceeded_raises_rate_limited() -> None:
    """With ``burst=3``, the 4th rapid send raises ``RateLimited``."""
    clock = _MonotonicClock()
    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        monotonic_clock=clock,
    )
    rule = Rule(limits=LimitsBlock(rate=RateBlock(per_minute=60, burst=3)))
    alice, _bob, session = await _open_consulting(hub, alice_rule=rule)
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]

    for _ in range(3):
        await session.send("ok", audience=audience)

    with pytest.raises(RateLimited):
        await session.send("over", audience=audience)


@pytest.mark.asyncio
async def test_clock_advance_refills_bucket() -> None:
    """Advancing the monotonic clock by one refill period restores a token."""
    clock = _MonotonicClock()
    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        monotonic_clock=clock,
    )
    # 60/min = 1/sec. burst=1 → after 1 send we're empty; advance 1s for refill.
    rule = Rule(limits=LimitsBlock(rate=RateBlock(per_minute=60, burst=1)))
    alice, _bob, session = await _open_consulting(hub, alice_rule=rule)
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]

    await session.send("first", audience=audience)
    with pytest.raises(RateLimited):
        await session.send("second-too-soon", audience=audience)

    clock.advance(1.5)
    await session.send("after-refill", audience=audience)


@pytest.mark.asyncio
async def test_buckets_are_per_sender() -> None:
    """Alice's throttle does not affect bob's independent budget."""
    clock = _MonotonicClock()
    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        monotonic_clock=clock,
    )
    link = LocalLink(hub)
    alice_hc = HubClient(link, hub=hub)
    bob_hc = HubClient(link, hub=hub)

    tight = Rule(limits=LimitsBlock(rate=RateBlock(per_minute=60, burst=1)))
    alice = await alice_hc.register(
        _agent("alice"), Passport(name="alice"), Resume(), rule=tight
    )
    bob = await bob_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conversation", target="bob")
    audience_to_bob = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]
    audience_to_alice = [p.agent_id for p in session.metadata.participants if p.agent_id != bob.agent_id]

    # Alice exhausts her single-token burst.
    await session.send("alice-1", audience=audience_to_bob)
    with pytest.raises(RateLimited):
        await session.send("alice-2", audience=audience_to_bob)

    # Bob (no rate limit) keeps posting freely on the same session.
    for i in range(5):
        envelope = Envelope(
            session_id=session.session_id,
            sender_id=bob.agent_id,
            audience=audience_to_alice,
            event_type=EV_TEXT,
            event_data={"text": f"bob-{i}"},
        )
        await bob.send_envelope(envelope)


@pytest.mark.asyncio
async def test_set_rule_invalidates_cached_bucket() -> None:
    """Tightening rate via ``set_rule`` takes effect on the next post."""
    clock = _MonotonicClock()
    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        monotonic_clock=clock,
    )
    alice, _bob, session = await _open_consulting(hub)
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]

    # Initially unlimited — three sends succeed.
    for i in range(3):
        await session.send(f"msg-{i}", audience=audience)

    # Tighten to single-token burst.
    await alice.set_rule(Rule(limits=LimitsBlock(rate=RateBlock(per_minute=60, burst=1))))

    # First post under new rule fills + drains the bucket; second is throttled.
    await session.send("after-rule-1", audience=audience)
    with pytest.raises(RateLimited):
        await session.send("after-rule-2", audience=audience)


@pytest.mark.asyncio
async def test_protocol_envelopes_bypass_rate_limit() -> None:
    """Drained-bucket sender can still close their session.

    ``EV_SESSION_CLOSED`` is a protocol envelope. Without the bypass,
    a sender that exhausted their burst on ``EV_TEXT`` could not close
    the session they themselves opened.
    """
    clock = _MonotonicClock()
    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        monotonic_clock=clock,
    )
    rule = Rule(limits=LimitsBlock(rate=RateBlock(per_minute=60, burst=2)))
    alice, _bob, session = await _open_consulting(hub, alice_rule=rule)
    audience = [p.agent_id for p in session.metadata.participants if p.agent_id != alice.agent_id]

    # Drain alice's burst with substantive sends.
    await session.send("one", audience=audience)
    await session.send("two", audience=audience)
    with pytest.raises(RateLimited):
        await session.send("three-throttled", audience=audience)

    # Closing emits EV_SESSION_CLOSED — protocol event — and succeeds
    # despite alice's bucket being empty.
    closed = await session.close()
    assert closed.state == SessionState.CLOSED
