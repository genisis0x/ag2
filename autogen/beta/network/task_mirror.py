# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``TaskMirror`` — bridges agent ``Task*`` stream events to ``Hub.observe_task``.

Per ``design/tasks.md``: the network is *one observer* of agent-owned
tasks. The mirror subscribes to ``TaskStarted`` / ``TaskProgress`` /
``TaskCompleted`` / ``TaskFailed`` / ``TaskExpired`` on the agent's
stream and forwards corresponding ``TaskMetadata`` updates to the hub.

M2 ships the mirror but exercise of it is sparse — the basic
consulting flow (``delegate`` round-trip) does not directly use
``Agent.task(...)``. M3 wires LLM-facing ``tasks(action="start")``
which lights it up.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from autogen.beta.events import (
    TaskCompleted,
    TaskExpired,
    TaskFailed,
    TaskProgress,
    TaskStarted,
)
from autogen.beta.task import TaskMetadata, TaskSpec, TaskState

from .errors import NotFoundError

if TYPE_CHECKING:
    from autogen.beta.context import Stream

    from .hub import Hub

__all__ = ("TaskMirror",)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskMirror:
    """Forwards an Agent's ``Task*`` events to the hub.

    Construct one per ``AgentClient`` (the owner is the Agent's
    ``agent_id``). Attach to a stream for the duration of a notify
    handler / Agent.ask call, then detach.

    Failures forwarding to the hub are swallowed — the mirror must
    never crash the agent's turn. Production builds should log; M2
    keeps quiet.
    """

    def __init__(
        self,
        *,
        hub: "Hub",
        owner_id: str,
        session_id: str | None = None,
    ) -> None:
        # __init__ stores params; subscription happens in attach().
        self._hub = hub
        self._owner_id = owner_id
        self._session_id = session_id

    def attach(self, stream: "Stream") -> list[object]:
        """Subscribe to ``Task*`` events; returns sub ids for ``detach``."""
        return [
            stream.where(TaskStarted).subscribe(self._on_started, sync_to_thread=False),
            stream.where(TaskProgress).subscribe(self._on_progress, sync_to_thread=False),
            stream.where(TaskCompleted).subscribe(self._on_completed, sync_to_thread=False),
            stream.where(TaskFailed).subscribe(self._on_failed, sync_to_thread=False),
            stream.where(TaskExpired).subscribe(self._on_expired, sync_to_thread=False),
        ]

    def detach(self, stream: "Stream", sub_ids: list[object]) -> None:
        """Unsubscribe the previously-attached subscriptions."""
        for sid in sub_ids:
            try:
                stream.unsubscribe(sid)  # type: ignore[arg-type]
            except Exception:
                pass

    async def _on_started(self, event: TaskStarted) -> None:
        spec = event.spec if event.spec is not None else TaskSpec(title=event.objective or "")
        now = _now_iso()
        metadata = TaskMetadata(
            task_id=event.task_id,
            owner_id=self._owner_id,
            spec=spec,
            state=TaskState.RUNNING,
            created_at=now,
            started_at=now,
            session_id=self._session_id,
        )
        try:
            await self._hub.observe_task(metadata)
        except Exception:
            pass

    async def _on_progress(self, event: TaskProgress) -> None:
        try:
            await self._hub.update_task(
                event.task_id,
                progress=dict(event.payload) if event.payload else None,
            )
        except NotFoundError:
            pass
        except Exception:
            pass

    async def _on_completed(self, event: TaskCompleted) -> None:
        try:
            await self._hub.update_task(
                event.task_id,
                state=TaskState.COMPLETED,
                result=event.result,
            )
        except NotFoundError:
            pass
        except Exception:
            pass

    async def _on_failed(self, event: TaskFailed) -> None:
        try:
            await self._hub.update_task(
                event.task_id,
                state=TaskState.FAILED,
                error=str(event.error),
            )
        except NotFoundError:
            pass
        except Exception:
            pass

    async def _on_expired(self, event: TaskExpired) -> None:
        try:
            await self._hub.update_task(event.task_id, state=TaskState.EXPIRED)
        except NotFoundError:
            pass
        except Exception:
            pass
