# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``HubBackedCheckpointStore`` — Phase 2.0 task checkpoint adapter.

Wraps a hub's :class:`KnowledgeStore` so ``Task.checkpoint(state)``
writes JSON to ``/tasks/{task_id}/checkpoint.json`` and resume reads
from the same path.

The framework-core ``CheckpointStore`` Protocol is intentionally tiny
(``read`` + ``write``); this adapter is the only network-side piece
needed to make Tasks restart-recoverable through the hub's persistence
root. Standalone agents can supply any other backend (e.g. a
``MemoryKnowledgeStore`` for tests, a plain dict for ad-hoc work) by
satisfying the same Protocol.
"""

import json
from typing import Any

from autogen.beta.knowledge import KnowledgeStore
from autogen.beta.task import CheckpointStore

from ..hub.layout import task_checkpoint_path

__all__ = ("HubBackedCheckpointStore",)


class HubBackedCheckpointStore(CheckpointStore):
    """``CheckpointStore`` backed by a hub's ``KnowledgeStore``.

    The store path mirrors the on-disk layout the hub already uses for
    task metadata (``tasks/{task_id}/...``), so a checkpoint sits next
    to its owning task on disk.
    """

    def __init__(self, store: KnowledgeStore) -> None:
        # __init__ stores params; no side effects.
        self._store = store

    async def read(self, task_id: str) -> dict[str, Any] | None:
        body = await self._store.read(task_checkpoint_path(task_id))
        if not body:
            return None
        loaded = json.loads(body)
        # Sanity-check the payload shape so a corrupt write doesn't
        # silently feed non-dict state into resume code.
        if not isinstance(loaded, dict):
            return None
        return loaded

    async def write(self, task_id: str, state: dict[str, Any]) -> None:
        await self._store.write(task_checkpoint_path(task_id), json.dumps(state))
