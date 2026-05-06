# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Streaming chunk subscriptions on the client side.

A ``ChunkDelta`` is the per-yield value of :meth:`Session.iter_chunks`.
``ChunkSubscription`` owns the per-(session, parent_envelope_id) queue
and is fanned out to from :meth:`AgentClient.receive_chunk`.

The client side is intentionally tiny: the hub already routes
``ChunkFrame``s to the right ``recipient_id``; the subscription only
demuxes by ``parent_envelope_id`` so multiple in-flight streams to the
same agent on the same session don't cross-talk.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

__all__ = ("ChunkDelta", "ChunkSubscription")


@dataclass(slots=True)
class ChunkDelta:
    """One streamed chunk delivered to a subscriber.

    ``text`` is the partial body; ``sequence`` is sender-monotonic per
    parent so receivers can detect drops; ``is_final`` marks the
    terminal chunk so callers can ``break`` cleanly.
    """

    sender_id: str
    sequence: int
    text: str
    is_final: bool


class ChunkSubscription:
    """Per-(session, parent_envelope_id) queue.

    ``put`` is non-blocking (unbounded queue); ``aiter`` yields deltas
    until ``is_final`` lands or :meth:`close` is called explicitly.
    """

    def __init__(self) -> None:
        # __init__ stores params; no side effects.
        self._queue: asyncio.Queue[ChunkDelta | None] = asyncio.Queue()
        self._closed = False

    async def put(self, delta: ChunkDelta) -> None:
        if self._closed:
            return
        await self._queue.put(delta)
        if delta.is_final:
            self._closed = True
            await self._queue.put(None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._queue.put(None)

    def __aiter__(self) -> AsyncIterator[ChunkDelta]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[ChunkDelta]:
        while True:
            delta = await self._queue.get()
            if delta is None:
                return
            yield delta
