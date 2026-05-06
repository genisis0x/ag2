"""Helpers shared between the parity tests."""

import asyncio
from typing import Any

from autogen.beta.network import (
    EV_HANDOFF,
    EV_TEXT,
    Hub,
)


SUBSTANTIVE_EVENTS = (EV_TEXT, EV_HANDOFF)


async def wait_for_substantive_count(
    hub: Hub,
    session_id: str,
    *,
    expected: int,
    timeout: float = 120.0,
) -> int:
    """Poll the hub WAL until ``expected`` substantive envelopes land,
    or the timeout elapses. Returns the final count."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        wal = await hub.read_wal(session_id)
        count = sum(1 for e in wal if e.event_type in SUBSTANTIVE_EVENTS)
        if count >= expected:
            return count
        await asyncio.sleep(0.3)
    wal = await hub.read_wal(session_id)
    return sum(1 for e in wal if e.event_type in SUBSTANTIVE_EVENTS)


async def wait_for_predicate(
    pred,
    *,
    timeout: float = 120.0,
    interval: float = 0.3,
) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if await _maybe_await(pred):
            return True
        await asyncio.sleep(interval)
    return False


async def _maybe_await(value_or_coro: Any) -> Any:
    if asyncio.iscoroutine(value_or_coro):
        return await value_or_coro
    if callable(value_or_coro):
        result = value_or_coro()
        if asyncio.iscoroutine(result):
            return await result
        return result
    return value_or_coro


def transcript_lines(wal: list, name_by_id: dict[str, str]) -> list[str]:
    """Render a session WAL into ``"name: text"`` transcript lines for
    the substantive envelopes only."""
    out: list[str] = []
    for env in wal:
        if env.event_type == EV_TEXT:
            text = (env.event_data or {}).get("text", "")
            who = name_by_id.get(env.sender_id, env.sender_id)
            out.append(f"{who}: {text}")
        elif env.event_type == EV_HANDOFF:
            data = env.event_data or {}
            tool = data.get("tool", "?")
            reason = data.get("reason", "")
            who = name_by_id.get(env.sender_id, env.sender_id)
            out.append(f"{who} → handoff[{tool}]: {reason}")
    return out


def print_section(title: str) -> None:
    bar = "=" * (len(title) + 4)
    print(f"\n{bar}\n  {title}\n{bar}")
