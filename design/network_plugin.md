# NetworkPlugin & LLM tools

The `NetworkPlugin` is what attaches an `Agent` to a `Network`. Its responsibilities:

1. Add the 6 LLM tools to `agent.tools` as real `FunctionTool`s — visible at every tool-listing path, callable in any `Agent.ask`, not just inside notify handlers. This is the fix that lets agents **initiate** sessions, not only respond inside them.
2. Register an assembly policy (`NetworkContextPolicy`) that injects network metadata into the prompt, refreshed lazily.
3. Wire context dependencies (`SESSION_DEP`, `AGENT_CLIENT_DEP`, `HUB_DEP`, `TASK_DEP`) on every notify handler entry so the tools can resolve their bindings.

Plugins are first-class in beta (`autogen/beta/agent.py:1234`); the network plugin uses the existing slot.

## Attachment

The plugin attaches at registration:

```python
hub_client = HubClient(link=LocalLink(hub))
agent_client = await hub_client.register(agent, identity)
# By this point, agent.tools includes the 6 verbs and agent's assembly chain
# includes NetworkContextPolicy.
```

`hub_client.register(...)` internally:

```python
async def register(self, agent, identity, rule=None):
    stamped = await self._hub.register(identity, rule or Rule())
    agent_client = AgentClient(self, agent, stamped, ...)
    plugin = NetworkPlugin(agent_client)
    plugin.register(agent)                     # adds tools, prompt, policies
    return agent_client
```

## Plugin shape

```python
# autogen/beta/network/client/plugin.py

from autogen.beta.agent import Plugin


class NetworkPlugin(Plugin):
    """Attaches an Agent to a network: adds 6 LLM verbs as Agent tools and
    registers an assembly policy that injects network metadata into prompts."""

    def __init__(self, client: AgentClient) -> None:
        super().__init__(
            tools=[
                make_say_tool(client),
                make_delegate_tool(client),
                make_peers_tool(client),
                make_sessions_tool(client),
                make_tasks_tool(client),
                make_context_tool(client),
            ],
        )
        self._client = client

    def register(self, agent: Agent) -> None:
        super().register(agent)
        agent._policies.append(NetworkContextPolicy(self._client))
```

## NetworkContextPolicy

A framework-core `AssemblyPolicy` (`autogen/beta/assembly.py:21`) that injects network metadata into the prompt before each LLM call. The current session's expectations and any active task are also included so the agent always sees the contract it's working under:

```
You are <name> (agent_id: <id>) in network <network_id>.

Active peers (M of N shown):
- alice [analyst, debate]: senior policy researcher, focuses on cost/benefit
- bob [coder]: backend systems, performance
- ...

Session types you can initiate: consulting, conversation, discussion

Currently in:
- session <id> (discussion, 5 participants), turn 12
  expectations:
  - turn_within(120s, warn) — speak within 2m of being expected next
  - turn_within(600s, hide) — silenced from the session if quiet 10m
  - max_silence(10m, audit)

Active task:
- task <id>: research framework X
  started 12s ago; last progress 4s ago

Your tools: say, delegate, peers, sessions, tasks, context.
```

The session block is rendered when the current notify handler has a current `Session`; the task block is rendered when the agent is inside an `agent.task(...)` context (resolved via `TaskInject`). Both blocks are omitted when not applicable, so a bare-network agent just sees the peer list and tool surface.

Implementation outline:

```python
class NetworkContextPolicy:
    name = "network_context"

    def __init__(self, client: AgentClient, *, peer_cache_ttl: float = 10.0) -> None:
        self._client = client
        self._cache: NetworkMetadata | None = None
        self._cached_at: float = 0.0
        self._peer_cache_ttl = peer_cache_ttl

    async def apply(self, prompts, events, context):
        if self._cache is None or (time.time() - self._cached_at) > self._peer_cache_ttl:
            self._cache = await self._client.describe_network()
            self._cached_at = time.time()

        prefix = self._render(self._cache, context)
        return [prefix, *prompts], events
```

Phase 2 wires `network_changed` push frames so the cache is invalidated proactively rather than just via TTL.

## DI surface for user tools

Pre-canned `Annotated` aliases for users writing their own tools that depend on session, network, or task state.

`TaskInject` is **framework-core** because Task itself is framework-core (see [tasks.md](tasks.md)). It resolves whether or not a hub is attached — any tool running inside `agent.task(...)` or `Agent.run_subtask` sees the active task. The other three are network-only and resolve to absent outside a network turn.

```python
# autogen/beta/annotations.py — framework-core
from typing import Annotated
from autogen.beta.annotations import Inject

TaskInject = Annotated["Task", Inject("ag2.task")]                  # framework-core


# autogen/beta/network/client/inject.py — network-only
SessionInject = Annotated["Session", Inject("ag2.network.session")]
AgentClientInject = Annotated["AgentClient", Inject("ag2.network.agent_client")]
HubInject = Annotated["Hub", Inject("ag2.network.hub")]
```

`Agent._spawn_subtask` and the `agent.task(...)` context manager stamp the active `Task` into `context.dependencies["ag2.task"]`. The default network notify handler additionally stamps the `Session`, `AgentClient`, and `Hub` keys.

Tools that want to be both on- and off-network use the optional form:

```python
Annotated[Session | None, Inject("ag2.network.session", default=None)]
Annotated[Task | None,    Inject("ag2.task",            default=None)]
```

## The 6 LLM tools

The surface is **2 flat + 4 grouped**, total 6 registered tools and ~14 distinct actions. Grouping follows the framework-core `_make_knowledge_tool` precedent (`autogen/beta/agent.py:1186`): single tool, action-dispatch body. The pattern keeps the LLM tool list short while exposing more capability per tool.

### Flat (high-frequency hot path)

#### `say`

```python
@tool
async def say(
    content: str,
    *,
    audience: list[str] | None = None,
    session_id: str | None = None,
    session: SessionInject | None = None,
    client: AgentClientInject,
) -> str:
    """Post into a session. Defaults to the current session in handler context;
    `session_id` overrides. Returns envelope_id.

    audience: list of agent names; None = broadcast within session.
    Errors if no current session and no session_id is given.
    """
```

#### `delegate`

```python
@tool
async def delegate(
    target: str,
    prompt: str,
    *,
    payload: dict | None = None,
    capability: str | None = None,
    blocking: bool = True,
    timeout: float | None = 300,
    session: SessionInject | None = None,
    client: AgentClientInject | None = None,
) -> str | dict:
    """One-shot consult: open a consulting session to `target`, run a Task,
    return the result string (or task handle if blocking=False).

    payload: optional structured handoff (file refs, prior context, expected
             schema). Passed through as TaskSpec.payload on the resulting task.
    capability: optional tag indicating which of `target`'s capabilities is
                being exercised. Hub uses it to update Resume.observed on
                terminal completion.

    Falls back to Agent.run_subtask() if no hub is attached.
    """
```

### Grouped (action-dispatch)

#### `peers(action, ...)`

```python
@tool
async def peers(
    action: Literal["find", "describe"],
    *,
    query: str | None = None,
    capability: str | None = None,
    sort_by: Literal["name", "cost", "track_record"] | None = None,
    name: str | None = None,
    limit: int = 20,
    client: AgentClientInject,
) -> list[dict] | dict:
    """Discover and describe peers in the network.

    action="find":     args query?, capability?, sort_by?, limit
                       returns [{name, agent_id, summary, capabilities, observed_success_rate, cost}, ...]
                       `query` matches Resume.summary substring; `capability`
                       matches claimed_capabilities ∪ observed.keys();
                       `sort_by="track_record"` ranks by observed success rate;
                       `sort_by="cost"` ranks by input_per_mtok ascending.
    action="describe": args name (or agent_id)
                       returns {passport, resume, skill_md} — full peer profile.
                       skill_md is the SKILL.md body verbatim (Anthropic-format
                       Markdown with frontmatter), or a fallback rendered from
                       resume.summary + capabilities when SKILL.md is absent.
                       The LLM reads skill_md the same way it reads harness skills.
    """
```

#### `sessions(action, ...)`

```python
@tool
async def sessions(
    action: Literal["list", "open", "info", "close"],
    *,
    type: str | None = None,
    target: str | list[str] | None = None,
    knobs: dict | None = None,
    intent: str | None = None,
    ttl: str | int | None = None,
    session_id: str | None = None,
    state: str = "active",
    client: AgentClientInject,
    current: SessionInject | None = None,
) -> list[dict] | dict:
    """Session lifecycle.

    action="list":  args state="active"|"all"
                    returns [{session_id, type, participants, state}, ...]
    action="open":  args type, target, knobs?, intent?, ttl?
                    returns {session_id, type, participants}
    action="info":  args session_id
                    returns full SessionMetadata as dict, including
                    `manifest.expectations` so the agent can see the
                    protocol-shape contract (reply windows, turn timeouts,
                    silence thresholds, etc.) for that session
    action="close": args session_id? (default current)
                    returns {session_id, state, close_reason}
    """
```

#### `tasks(action, ...)`

```python
@tool
async def tasks(
    action: Literal["start", "progress", "complete", "list", "status", "wait", "cancel"],
    *,
    title: str | None = None,
    description: str | None = None,
    payload: dict | None = None,
    result: Any | None = None,
    error: str | None = None,
    task_id: str | None = None,
    scope: Literal["own", "observed", "both"] = "both",
    state: Literal["active", "all"] = "active",
    timeout: float = 300,
    reason: str | None = None,
    limit: int = 20,
    task: TaskInject | None = None,
    client: AgentClientInject | None = None,
) -> dict | list[dict]:
    """Task lifecycle. Tasks are owned by the agent that started them; the
    network observes (see tasks.md). For one-shot delegated work, use
    `delegate` instead of composing start + wait.

    Self (operate on the agent's own currently-active Task — resolved via
    TaskInject; errors if no active task and the action requires one):

    action="start":    args title, description?, payload?
                       Wraps the agent's next unit of work in a Task.
                       Returns {task_id, state}. Auto-mirrored to network
                       if a hub is attached.
    action="progress": args payload (dict)
                       Emits TaskProgress on the active task. Use this to
                       declare work plan and signal liveness so observers
                       (and the hub's stall expectation) see progress.
    action="complete": args result?
                       Terminal: emits TaskCompleted. Usually unnecessary —
                       the `async with agent.task(...)` exits auto-complete.
                       Use only for explicit early-finish.

    List (introspect what tasks I'm currently involved in):

    action="list":     args scope="own"|"observed"|"both", state="active"|"all", limit
                       returns [{task_id, title, state, owner_id, last_progress_at}, ...]
                       scope="own" returns tasks where this agent is the owner;
                       scope="observed" returns tasks this agent is waiting on
                       or subscribed to; "both" merges them.

    Observe (operate on a peer's task by id):

    action="status":   args task_id  → {state, progress, result, error}
    action="wait":     args task_id, timeout=300
                       Subscribes to a task by id and resolves when terminal.
    action="cancel":   args task_id, reason?  (Phase 2)
                       Sends `ag2.task.cancel_request` to the owner; the
                       owner decides whether to honour it.
    """
```

The `start` / `progress` / `complete` actions are the LLM-facing wrappers around the framework-core `Agent.task(...)` context manager. The agent doesn't have to use these — `delegate` and `run_subtask` create tasks implicitly. The grouped tool exists for the case where the agent wants explicit control over its own task lifecycle, especially to emit `progress` mid-loop so observers see the work plan and the hub's `progress_within` expectation doesn't trip on a long-running step.

#### `context(action, ...)`

```python
@tool
async def context(
    action: Literal["search", "quote"],
    *,
    query: str | None = None,
    scope: Literal["session", "knowledge"] = "session",
    speaker: str | None = None,
    recent_n: int = 1,
    limit: int = 10,
    session_id: str | None = None,
    session: SessionInject | None = None,
    client: AgentClientInject,
) -> list[dict]:
    """Read from past content.

    action="search": args query, scope="session"|"knowledge", limit=10
                     returns [{sender_name, when, excerpt, envelope_id|knowledge_path}, ...]
    action="quote":  args speaker, recent_n=1, session_id?
                     returns the last N envelopes from `speaker` in the session
    """
```

## Tool implementation pattern

Each grouped tool dispatches on `action` (matches `_make_knowledge_tool`):

```python
def make_peers_tool(client: AgentClient) -> Tool:
    @tool
    async def peers(action: str, **kwargs) -> Any:
        if action == "find":
            return await client.list_peers(
                query=kwargs.get("query"),
                capability=kwargs.get("capability"),
                sort_by=kwargs.get("sort_by"),
                limit=kwargs.get("limit", 20),
            )
        if action == "describe":
            return await client.describe_peer(kwargs["name"])
        return f"Unknown action: {action!r}. Available: find, describe."
    return peers
```

Closures capture `client` once at registration time, so the FunctionTool definition is stable across turns — matches the framework-core "no nested functions in runtime execution paths" rule (decorators are exempt).

## What's not on the LLM surface

| Not exposed | Why |
|---|---|
| `subscribe` to a session I'm not a participant of | Power-user feature; defer |
| Streaming chunk send / iter | Wire-level, not LLM-level |
| `set_rule`, `unregister`, hub admin | Not the LLM's job — tenant Python code |
| `leave` (drop out of a session without closing it) | V1 has fixed quorum; leaving would break adapter participant counts. Phase 2 may add `sessions(action="leave")` once we have an `auto_repartition` knob on adapters. |
| `set_resume`, `set_skill`, `add_example` (self-attest more) | Sycophancy hazard: an LLM editing its own resume mid-conversation is a self-promotion vector. Resume mutation is tenant Python code only. |
| Cross-peer knowledge read | The `context(action="search", scope="knowledge")` reads only the calling agent's `KnowledgeStore`. Cross-peer knowledge bridge is deferred to AG2 Cloud; an agent that wants someone else's knowledge opens a session and asks. |

## Why these tools and not others

Two design constraints drove the surface:

- **Hot path stays flat.** `say` and `delegate` are the two most-called actions. They get their own tools so the LLM doesn't have to think "this is a session action" before composing the call.
- **Lifecycle / inspection groups by domain object.** All `peers.*`, `sessions.*`, `tasks.*`, `context.*` actions share a single mental model that fits in one description. Six tool names total instead of fourteen reduces prompt overhead by ~60%.

`delegate` overlaps with `sessions(action="open", type="consulting") + tasks(action="create", ...) + tasks(action="wait", ...)`. We keep it flat because it's the single most common multi-agent pattern; the precedent is framework-core keeping `run_subtask` flat alongside the explicit `Task` machinery.
