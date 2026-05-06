# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Token-bucket rate limiter.

A pure data + logic module — :class:`Hub` owns the per-agent bucket
cache and decides when to consume. The bucket itself has no clock
dependency: the caller passes ``now`` (a monotonic timestamp). This
keeps the limiter testable without a global mock and lets the hub
share a single monotonic source for all enforcement.

A bucket refills at ``per_minute / 60`` tokens per second up to
``capacity``. ``capacity = max(burst, per_minute)`` so callers can pin
``per_minute`` without ever bursting if they wish (``burst = 0``
defaults capacity to ``per_minute`` — one minute's worth of headroom).
"""

from dataclasses import dataclass

__all__ = ("TokenBucket", "make_bucket")


@dataclass(slots=True)
class TokenBucket:
    """Refilling token bucket. Caller supplies the clock.

    ``consume(now)`` first credits accumulated tokens for the elapsed
    interval, then atomically debits ``n`` if available. Returns
    ``True`` when the consume succeeded; ``False`` means the budget is
    spent and the caller should reject.
    """

    capacity: float
    refill_per_second: float
    tokens: float
    last_refill: float

    def consume(self, now: float, n: float = 1.0) -> bool:
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_second)
            self.last_refill = now
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False


def make_bucket(per_minute: int, burst: int, now: float) -> TokenBucket | None:
    """Build a bucket from a ``RateBlock``; returns ``None`` if disabled.

    ``per_minute <= 0`` is the disabled sentinel — callers cache the
    ``None`` and skip the limiter entirely. ``burst <= 0`` defaults to
    ``per_minute`` (one minute's worth of headroom).
    """
    if per_minute <= 0:
        return None
    capacity = float(burst) if burst > 0 else float(per_minute)
    return TokenBucket(
        capacity=capacity,
        refill_per_second=per_minute / 60.0,
        tokens=capacity,
        last_refill=now,
    )
