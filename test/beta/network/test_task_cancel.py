# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Task cancellation.

Covers:

* ``TaskState.CANCELLED`` is recognised as terminal.
* ``Task.cancel(reason)`` emits ``TaskCancelled`` and transitions state.
* Network-mirrored cancellation: hub's ``TaskMetadata.state`` updates
  to ``CANCELLED``.
* ``tasks(action="cancel", task_id, reason)`` LLM verb posts an
  ``ag2.task.cancel_request`` envelope addressed to the owner.
"""

import asyncio
import json
from typing import Any

import pytest

from autogen.beta import Agent, Context
from autogen.beta.events import TaskCancelled, ToolCallEvent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_TASK_CANCEL_REQUEST,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.client.tools.tasks import make_tasks_tool
from autogen.beta.network.policies import AGENT_CLIENT_DEP
from autogen.beta.network.task_mirror import TaskMirror
from autogen.beta.stream import MemoryStream
from autogen.beta.task import TERMINAL_TASK_STATES, TaskMetadata, TaskSpec, TaskState
from autogen.beta.testing import TestConfig


async def _invoke_tool(tool: Any, args: dict, *, dependencies: dict | None = None) -> Any:
    """Drive a ``FunctionTool`` end-to-end and return the unwrapped value."""
    event = ToolCallEvent(name=tool.name, arguments=json.dumps(args))
    context = Context(stream=MemoryStream(), dependencies=dependencies or {})
    result_event = await tool(event, context)
    parts = getattr(result_event, "result", None)
    if parts is None or not parts.parts:
        return result_event
    part = parts.parts[0]
    if hasattr(part, "data"):
        return part.data
    return getattr(part, "content", part)


def _agent(name: str, *events: object) -> Agent:
    return Agent(name=name, config=TestConfig(*events))


def test_cancelled_is_terminal() -> None:
    """``CANCELLED`` joins the frozen terminal set."""
    assert TaskState.CANCELLED in TERMINAL_TASK_STATES


@pytest.mark.asyncio
async def test_task_cancel_emits_event_and_state() -> None:
    """``task.cancel(reason)`` transitions to CANCELLED and emits TaskCancelled."""
    agent = _agent("worker")

    cancel_events: list[TaskCancelled] = []

    async def _capture(event: TaskCancelled) -> None:
        cancel_events.append(event)

    async with agent.task("long-running") as task:
        task.context.stream.where(TaskCancelled).subscribe(
            _capture, sync_to_thread=False
        )
        await task.cancel("user requested abort")
        # Yield so the subscription delivery completes before we assert.
        await asyncio.sleep(0)

    assert task.state == TaskState.CANCELLED
    assert task.metadata.error == "user requested abort"
    assert len(cancel_events) == 1
    assert cancel_events[0].reason == "user requested abort"


@pytest.mark.asyncio
async def test_task_cancel_after_terminal_is_noop() -> None:
    """Once a task has completed, a subsequent ``cancel`` does nothing."""
    agent = _agent("worker")

    async with agent.task("done already") as task:
        await task.complete(result="ok")
        await task.cancel("too late")

    assert task.state == TaskState.COMPLETED


@pytest.mark.asyncio
async def test_cancel_mirrors_to_hub_observation() -> None:
    """When the owner cancels, the hub's TaskMetadata reflects CANCELLED."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    # Open a session so the task has somewhere to mirror into.
    session = await alice.open(type="conversation", target=bob.agent_id)

    # Construct the stream up front so the mirror catches TaskStarted —
    # the mirror has to be attached *before* __aenter__ emits the event.
    from autogen.beta.context import ConversationContext

    ctx = ConversationContext(stream=MemoryStream())
    mirror = TaskMirror(
        hub_client=alice._hub_client,
        owner_id=alice.agent_id,
        session_id=session.session_id,
    )
    sub_ids = mirror.attach(ctx.stream)
    try:
        async with alice.agent.task("long-running", context=ctx) as task:
            # Yield so TaskStarted reaches the hub before cancel.
            await asyncio.sleep(0.02)
            await task.cancel("aborting")
            # Yield so TaskCancelled reaches the hub.
            await asyncio.sleep(0.02)
    finally:
        mirror.detach(ctx.stream, sub_ids)

    meta = await hub.get_task(task.task_id)
    assert meta.state == TaskState.CANCELLED
    assert meta.error == "aborting"

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_cancel_request_verb_posts_envelope() -> None:
    """``tasks(action="cancel")`` posts ``ag2.task.cancel_request`` to the owner."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    b_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())
    bob = await b_hc.register(_agent("bob"), Passport(name="bob"), Resume())

    session = await alice.open(type="conversation", target=bob.agent_id)

    # Bob has a known task in this session.
    task_id = "task-bob-123"
    await hub.observe_task(
        TaskMetadata(
            task_id=task_id,
            owner_id=bob.agent_id,
            spec=TaskSpec(title="bob's work"),
            state=TaskState.RUNNING,
            session_id=session.session_id,
        )
    )

    # Alice's `tasks(action="cancel")` posts a cancel_request envelope.
    tasks_tool = make_tasks_tool(alice)
    result = await _invoke_tool(
        tasks_tool,
        {"action": "cancel", "task_id": task_id, "reason": "running too long"},
        dependencies={AGENT_CLIENT_DEP: alice},
    )
    assert "posted" in result.lower() or task_id in result

    wal = await hub.read_wal(session.session_id)
    cancel_requests = [
        e for e in wal
        if e.event_type == EV_TASK_CANCEL_REQUEST and e.task_id == task_id
    ]
    assert len(cancel_requests) == 1
    req = cancel_requests[0]
    assert req.sender_id == alice.agent_id
    assert req.audience == [bob.agent_id]
    assert req.event_data == {"task_id": task_id, "reason": "running too long"}

    await a_hc.close()
    await b_hc.close()
    await hub.close()


@pytest.mark.asyncio
async def test_cancel_request_verb_errors_without_session() -> None:
    """Cancelling a task that has no session_id is a clear error."""
    store = MemoryKnowledgeStore()
    hub = await Hub.open(store, ttl_sweep_interval=0, expectation_sweep_interval=0)
    link = LocalLink(hub)

    a_hc = HubClient(link, hub=hub)
    alice = await a_hc.register(_agent("alice"), Passport(name="alice"), Resume())

    task_id = "task-orphan"
    await hub.observe_task(
        TaskMetadata(
            task_id=task_id,
            owner_id=alice.agent_id,
            spec=TaskSpec(title="orphan"),
            state=TaskState.RUNNING,
        )
    )

    tasks_tool = make_tasks_tool(alice)
    result = await _invoke_tool(
        tasks_tool,
        {"action": "cancel", "task_id": task_id},
        dependencies={AGENT_CLIENT_DEP: alice},
    )
    assert "Error" in result
    assert "session" in result

    await a_hc.close()
    await hub.close()
