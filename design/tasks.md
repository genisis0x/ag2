# Tasks

Task is a **framework-core primitive**, not a network-only concept. Any `Agent` can wrap a unit of work in a `Task` to give it a trackable lifecycle — even standalone, with no hub. The agent decides whether the work warrants a Task; if it does, it gets a `task_id` and a stream of `Task*` events that any observer can subscribe to.

The network layer is **one observer**. When an `AgentClient` is attached, it mirrors the agent's task events to the hub as `ag2.task.*` envelopes so peers in the same session — or any peer subscribing by `task_id` — can track without participating in execution. The hub does not assign tasks. The hub does not own task state. The hub observes.

This factoring collapses the user's mental model down to: **tasks are agent-owned; networks are observers.**

## Framework-core API

```python
# autogen/beta/task.py

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


TERMINAL_TASK_STATES = frozenset({
    TaskState.COMPLETED, TaskState.FAILED, TaskState.EXPIRED,
})


@dataclass(slots=True)
class TaskSpec:
    title: str
    description: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TaskMetadata:
    task_id: str                                # UUID7
    owner_id: str                               # the Agent (or other actor) doing the work
    spec: TaskSpec
    state: TaskState
    created_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    expires_at: str | None = None
    last_progress_at: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: str = ""
    # Optional network association — set when an AgentClient mirrors this task.
    session_id: str | None = None


class Task:
    """Lifecycle handle for a unit of work an Agent is doing.

    Created via `agent.task(...)`. Emits Task* events on the agent's stream
    so any observer (network mirror, watcher, UI, test harness) can track.
    """

    @property
    def task_id(self) -> str: ...
    @property
    def state(self) -> TaskState: ...
    @property
    def metadata(self) -> TaskMetadata: ...

    async def progress(self, payload: dict[str, Any]) -> None:
        """Emit TaskProgress; merges payload into metadata.progress."""

    async def complete(self, result: Any = None) -> None:
        """Terminal: emit TaskCompleted; state ← COMPLETED."""

    async def fail(self, error: str) -> None:
        """Terminal: emit TaskFailed; state ← FAILED."""

    async def __aenter__(self) -> "Task":
        """Emit TaskStarted; state ← RUNNING."""

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Auto-fail on exception; auto-complete with None on clean exit if
        not already terminal."""
```

`Agent.task()` is the construction point:

```python
# autogen/beta/agent.py — added at framework-core

class Agent:
    def task(
        self,
        title: str,
        *,
        description: str = "",
        payload: dict[str, Any] | None = None,
        ttl_seconds: int | None = None,
    ) -> Task: ...
```

## Task* events

Emitted on the agent's stream (framework-core `Stream`):

```python
# autogen/beta/events/task.py

@dataclass
class TaskStarted(BaseEvent):
    task_id: str
    spec: TaskSpec

@dataclass
class TaskProgress(BaseEvent):
    task_id: str
    payload: dict[str, Any]

@dataclass
class TaskCompleted(BaseEvent):
    task_id: str
    result: Any

@dataclass
class TaskFailed(BaseEvent):
    task_id: str
    error: str

@dataclass
class TaskExpired(BaseEvent):
    task_id: str
```

`TaskExpired` is emitted by whichever observer holds the TTL clock. Standalone agent: no expiry unless app code arranges it. Networked agent: hub emits via the TTL sweeper, AgentClient mirrors back to the agent's stream.

## Usage — standalone

```python
agent = Agent(name="researcher", config=cfg)

async with agent.task("research framework X") as task:
    await task.progress({"step": "search"})
    findings = await search(...)
    await task.progress({"step": "synthesize"})
    summary = await summarize(findings)
    await task.complete(summary)
# Task* events are on agent's stream; observers see lifecycle.
```

If no observer is registered, the events fly past harmlessly. The Task primitive doesn't require anyone to be listening.

## Agent awareness from inside a Task

When an Agent is running inside a Task — either via `async with agent.task(...)` or as the inner Agent of `run_subtask` — it should know it's in a Task. The framework wires this in two places:

**Dependency injection.** The active `Task` is stamped into `context.dependencies["ag2.task"]` for the duration of the `async with` block. `TaskInject` (framework-core, see [network_plugin.md](network_plugin.md)) resolves to it. Any tool that takes `task: TaskInject` or `task: TaskInject | None` sees the active Task and can call `task.progress(...)` / `task.complete(...)` directly.

**Prompt visibility.** A small framework-core assembly policy injects task metadata into the prompt prefix when active:

```
Active task:
- task <id>: <title>
  started <duration> ago; last progress <duration> ago
  description: <text or first 200 chars>
```

This policy is added automatically by `Agent._spawn_subtask` so subtask Agents see the task they are. Standalone agents that opt into `async with agent.task(...)` from their own tool implementations get the same prompt block.

The LLM-facing `tasks(...)` tool (see [network_plugin.md](network_plugin.md)) exposes `start`, `progress`, `complete` actions that operate on the **current** Task via `TaskInject` — so a subtask Agent can emit progress without needing to know its own `task_id`.

## Usage — under run_subtask

`run_subtask` becomes a Task wrapper around sub-Agent invocation. Same semantics as before — fresh child Agent, isolated stream, parent receives result string — but now wrapped in observable Task lifecycle:

```python
# autogen/beta/agent.py — _spawn_subtask repurposed

async def _spawn_subtask(self, prompt: str, ctx: Context) -> str:
    async with self.task(f"subtask: {prompt[:80]}", description=prompt) as task:
        # ... existing run_subtask body — spawn child, run, collect result ...
        result = await _run_task(child_agent, prompt, parent_context=ctx)
        await task.complete(result.result or "")
        return result.result or ""
```

User code calling `run_subtask` doesn't change. What changes: the parent Agent's stream now emits `TaskStarted` / `TaskCompleted` envelopes, so observers (network mirror, UI, audit log) get a structured lifecycle for free.

## Network mirroring

When an Agent has an `AgentClient` attached (i.e., is registered with a hub via `NetworkPlugin`), a thin mirror subscribes to `Task*` events on the agent's stream and forwards them as `ag2.task.*` envelopes:

```
agent.stream            AgentClient                       Hub
   │                        │                              │
   │── TaskStarted ────────▶│── ag2.task.started ─────────▶│  observe_task
   │                        │                              │  (register TaskMetadata)
   │── TaskProgress ───────▶│── ag2.task.progress ────────▶│  update progress
   │── TaskCompleted ──────▶│── ag2.task.result ──────────▶│  state ← completed
```

The `session_id` on the envelope is the mirror's choice: by default, the session the agent is currently handling (if any) so peers in that session see the task as part of the session's WAL. With no current session, the mirror creates a per-agent task channel.

## Hub responsibilities for observed tasks

The hub does **not** create, assign, cancel, or retry tasks. It observes. Specifically:

- **Stores `TaskMetadata`** indexed by `task_id` (so peers can `subscribe(task_id)` even after the producing session closes).
- **Applies task TTL.** When `expires_at` passes without a terminal event, hub emits `ag2.task.expired` and updates state to `EXPIRED`. Owner is informed via the same envelope on its inbox.
- **Forwards task envelopes** to subscribers per the session's audience addressing.
- **Emits `ag2.task.stalled`** when a non-terminal task has no progress event for `Rule.limits.task_stall_threshold` (default 60s). Passive signal — owner can resume by emitting another progress event; peers can react however their choreography prescribes.
- **Cascades on session close** — non-terminal tasks tied to a closed session transition to `EXPIRED` with `reason="session_closed"` before `EV_SESSION_CLOSED` lands. The owner sees this on its stream via the mirror.
- **Records observations on terminal events.** When a terminal task envelope (`EV_TASK_RESULT` / `EV_TASK_ERROR` / `EV_TASK_EXPIRED`) lands and `TaskSpec.payload` carries a `capability` tag, the hub calls `Hub.record_observation(owner_id, capability=, outcome=, duration_ms=)` to update `Resume.observed[capability]`. Tasks without a capability tag don't update observed stats. See [identity.md](identity.md) for the resume mutation contract.

There is no `Hub.create_task`. There is no `Hub.cancel_task` in V1. Cancellation, if needed, is the owner's responsibility — they call `task.fail("cancelled by request")` or `task.complete(...)` early. Phase 2 may add a hub-mediated cancellation request envelope (`ag2.task.cancel_request`) the owner is free to honour or ignore.

```python
# Hub public API for tasks (V1):
async def observe_task(self, metadata: TaskMetadata) -> None:
    """Register an existing local task. Called by AgentClient when it
    sees TaskStarted on its agent's stream."""

async def expire_due_tasks(self) -> None:
    """Sweeper hook: walk _tasks, transition expired ones."""
```

## Task envelopes bypass adapter delivery rules

A `consulting` session can host any number of `progress` / `result` envelopes from the owner without the consulting adapter's 1Q1R rule auto-closing it. Implemented as a `if envelope.event_type in TASK_EVENT_TYPES` branch in `Hub.post_envelope` that runs access / rate / depth checks but skips `adapter.validate_send` / `adapter.on_accepted`.

## Blocking vs non-blocking — observer side

Subscribers (peers waiting on a delegated task) use the same primitive — subscribe to the WAL with a `task_id` predicate, resolve on the first envelope whose event type is in `TERMINAL_TASK_EVENT_TYPES`:

```python
# tasks(action="wait", task_id, timeout=300) is the LLM-facing version.
result = await client.tasks_wait(task_id, timeout=300)
```

The owner's agent loop is unaffected — they're running their work synchronously inside the `async with agent.task(...)` block.

## delegate — the LLM verb

`delegate(target, prompt)` (see [network_plugin.md](network_plugin.md)) is the convenience: "open a consulting session to `target`, ask them to do this thing, return their result." Implementation:

- **No hub**: falls through to `Agent.run_subtask` directly. `run_subtask` already wraps in a Task, so the lifecycle is observable on the local stream.
- **Hub attached**: opens a `consulting` session with `target`, posts the prompt, waits for the consulting reply (which `target` produces by running its own work — possibly itself wrapped in a Task on the target's side, mirrored through the same hub).

One LLM verb on the surface; both paths produce identical observable Task events.

## Why this factoring is better

| Before | After |
|---|---|
| Task lives in `network/task.py`; standalone agents have no Task primitive | Task lives in `autogen/beta/task.py`; works with or without a hub |
| `Hub.create_task` — hub assigns work | No `create_task`; agent owns; `Hub.observe_task` registers |
| `EV_TASK_ASSIGNED` — hub → owner | Removed; owner emits `TaskStarted` on its own stream |
| run_subtask and Task are unrelated | run_subtask wraps in Task; same lifecycle, same observer surface |
| Network has parallel notion of "delegated task" | Network is one observer; same primitive, more witnesses |

## Phase 2 additions

- `TaskState.CANCELLED` + `task.cancel(reason)` (owner-driven) + `EV_TASK_CANCELLED`
- `ag2.task.cancel_request` envelope: peer asks owner to stop; owner free to honour or ignore
- `TaskPhase` + `current_phase` + `TaskPhaseEntered` / `TaskPhaseCompleted` events for saga-style multi-step tasks with restart/resume semantics. `current_phase` advances on every phase event and persists on disk via the network mirror; a restarting hub `hydrate()`s observed task state and the owner can resume from the last committed phase if it persisted phase metadata locally.
- `tasks(action="cancel", task_id, reason?)` LLM verb wires up the request envelope.
