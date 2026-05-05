# Hub

The hub is the router. It owns the registry, the session and task state machines, the WAL, the dispatch path, the adapter state cache, and the internal sweepers.

## What the hub is NOT

- Does not call `Agent.ask`.
- Does not execute tenant transforms (Phase 3).
- Does not participate in turn-by-turn LLM calls.
- Does not import tenant Python modules.
- **Does not create, assign, cancel, or retry tasks.** Tasks are agent-owned; hub observes via `observe_task` (see [tasks.md](tasks.md)).
- **Does not enforce response guarantees.** The hub flags protocol-shape violations (`Expectation`s) and surfaces liveness signals; reaction is the agent's choreography (see [failure_modes.md](failure_modes.md)).
- **Does not orchestrate across sessions.** One session at a time; multi-session flows are app code.
- **Does not judge content.** Whether a reply is "useful" is an LLM-quality call, not a wire concern.

The trust boundary runs through `HubClient` / `AgentClient` (see [clients.md](clients.md)).

## Hub vs Network

A **Hub** is a process. A **Network** is a logical addressable namespace. V1 has 1:1 mapping; Phase 2+ may have one Hub host multiple Networks (separate registries, separate WAL roots) by routing on a `network_id` parameter that defaults to `"default"`. The names are kept distinct in code from day one so the V2 split is non-breaking.

## Construction

```python
# autogen/beta/network/hub/core.py

class Hub:
    def __init__(
        self,
        store: KnowledgeStore,
        *,
        ttl_sweep_interval: float = 30.0,
        expectation_sweep_interval: float = 10.0,
        invite_ack_timeout_s: float = 30.0,
        audit_retention_days: int = 30,
        clock: Callable[[], str] = _utc_now_iso,
        auth: AuthRegistry | None = None,
        adapters: list[SessionAdapter] | None = None,
    ) -> None: ...

    @classmethod
    async def open(cls, store: KnowledgeStore, **kwargs) -> "Hub":
        """Construct + hydrate from disk + start sweepers. Use this in
        production; the sync constructor is for tests with no disk state."""
        hub = cls(store, **kwargs)
        await hub.hydrate()
        await hub.start()
        return hub

    async def __aenter__(self) -> "Hub": ...
    async def __aexit__(self, *exc) -> None: ...
```

## Public API

```python
# ── Registration ────────────────────────────────────────────────────────────

async def register(
    self,
    passport: Passport,
    resume: Resume,
    *,
    skill_md: str | None = None,
    rule: Rule | None = None,
) -> Passport:
    """Stamp agent_id (UUID7), persist passport + resume + optional SKILL.md +
    rule, write an `agent.registered` audit line, return passport with id set."""

async def unregister(self, agent_id: str) -> None:
    """Remove registry entries; write an `agent.unregistered` audit line.
    Closed sessions and observed tasks remain on disk."""

async def set_rule(self, agent_id: str, rule: Rule) -> None:
    """Replace rule; bump version; write `rule.changed` audit line."""

# ── Discovery (read-side enumeration) ───────────────────────────────────────

async def get_agent(self, name_or_id: str) -> Passport:
    """Return passport. Raises NotFoundError if absent."""

async def get_resume(self, agent_id: str) -> Resume: ...

async def get_skill(self, agent_id: str) -> str | None:
    """Return SKILL.md body (without frontmatter parsed) or None if absent."""

async def list_agents(
    self, *,
    capability: str | None = None,
    query: str | None = None,
    sort_by: str | None = None,        # "name" | "cost" | "track_record" | None
    limit: int = 50,
) -> list[Passport]:
    """Filter + rank registered agents. `query` matches Resume.summary substring;
    `capability` matches `claimed_capabilities ∪ observed.keys()`. `sort_by`
    drives ranking — V1 supports `"name"` (lex), `"cost"` (input_per_mtok asc,
    None last), `"track_record"` (success_rate desc, requires non-zero observed.n),
    or None (registration order)."""

async def set_resume(self, agent_id: str, resume: Resume) -> None:
    """Tenant-driven resume replace; bump version + last_updated."""

async def set_skill(self, agent_id: str, skill_md: str | None) -> None:
    """Tenant-driven SKILL.md replace. None deletes the file."""

async def record_observation(
    self,
    agent_id: str,
    *,
    capability: str,
    outcome: str,                      # "completed" | "failed" | "expired"
    duration_ms: int | None = None,
    task_id: str | None = None,
) -> None:
    """Hub-driven resume mutation. Called by the task-mirror on terminal task
    events whose payload carries a `capability` tag. Updates
    Resume.observed[capability] in-place; bumps last_updated."""

async def describe_network(self) -> NetworkMetadata:
    """Adapters available, peer count, my own state. Used by NetworkPlugin's
    NetworkContextPolicy to build the per-turn prompt prefix."""

# ── Sessions ────────────────────────────────────────────────────────────────

async def create_session(
    self, *,
    creator_id: str,
    manifest_type: str,
    manifest_version: int = 1,
    participants: list[str],           # agent ids
    required_acks: int | None = None,
    ttl: str | int | None = None,
    knobs: dict | None = None,
    intent: str | None = None,
    labels: dict[str, str] | None = None,
) -> SessionMetadata:
    """Allocate session_id, write metadata, broadcast EV_SESSION_INVITE.
    `intent` is stored on `metadata.labels["intent"]` if set — keeps
    SessionMetadata stable as new fields appear."""

async def close_session(self, session_id: str, *, reason: str = "") -> SessionMetadata: ...

async def get_session(self, session_id: str) -> SessionMetadata:
    """Return metadata. Raises NotFoundError if absent."""

async def list_sessions(
    self, *,
    agent_id: str | None = None,        # filter to sessions this agent participates in
    state: SessionState | None = None,
    limit: int = 50,
) -> list[SessionMetadata]: ...

async def post_envelope(self, envelope: Envelope) -> str:
    """Validate + WAL-append + fold + dispatch. Returns hub-stamped envelope_id."""

async def read_wal(
    self, session_id: str, *, since: int = 0, until: int | None = None,
) -> list[Envelope]: ...

def find_envelope_by_causation(
    self,
    session_id: str,
    *,
    sender_id: str,
    causation_id: str,
) -> Envelope | None:
    """Look up an envelope by ``(sender_id, causation_id)`` within a
    session's WAL. Used by the default notify handler to short-circuit
    duplicate replies after redelivery (see failure_modes.md mode 11).
    Returns the first match; ``None`` if absent.

    Synchronous because the index is in-memory; matches the
    ``can_send`` precedent for pure cache lookups. The index is rebuilt
    from the WAL on ``hydrate()`` — no separate persisted file. Phase 2.0."""

async def pending_turns_for(self, agent_id: str) -> list[PendingTurn]:
    """Return non-terminal sessions where adapter state expects this
    agent to act but no reply has landed since the triggering envelope.

    Each ``PendingTurn`` carries ``session_id``, ``last_envelope_id``,
    and ``reason`` (e.g. ``"workflow_next_speaker"``,
    ``"consulting_respondent"``). Used by the default notify handler
    on reconnect to wake up unfinished turns. Phase 2.0."""

# ── Tasks (observe-only; tasks are owned by the agent — see tasks.md) ───────

async def observe_task(self, metadata: TaskMetadata) -> None:
    """Register an existing local task. Called by AgentClient when it sees
    TaskStarted on the agent's stream. Hub stores TaskMetadata, starts TTL
    accounting, and forwards subsequent task envelopes per audience addressing.
    The hub never creates, assigns, or cancels."""

async def get_task(self, task_id: str) -> TaskMetadata:
    """Return metadata. Raises NotFoundError if absent."""

async def list_tasks(
    self, *,
    agent_id: str | None = None,        # filter to tasks owned by this agent
    session_id: str | None = None,
    state: TaskState | None = None,
    limit: int = 50,
) -> list[TaskMetadata]: ...

async def expire_due(self) -> None:
    """Sweeper hook: walk active sessions and tasks, transition expired
    ones to ``EXPIRED``, emit ``ag2.session.expired`` /
    ``ag2.task.expired``. Public so users running their own scheduler
    can drive it directly."""

async def evaluate_expectations(self) -> None:
    """Sweeper hook: evaluate every expectation on every active session
    and apply registered violation handlers. Public sibling of
    ``expire_due()``; Phase 2.0 promotion of the internal
    ``_expectation_tick``."""

# ── Subscriptions ───────────────────────────────────────────────────────────

async def subscribe(
    self,
    subscriber_id: str,
    *,
    session_id: str | None = None,
    task_id: str | None = None,
    event_types: list[str] | None = None,
    since_envelope_id: str | None = None,
) -> Subscription:
    """Open a live subscription. At least one of session_id / task_id must be
    set. Returns a handle whose `.events()` is an async iterator. Used by
    `tasks(action="wait")` and Phase 3 WS clients reconnecting with a cursor."""

async def unsubscribe(self, subscription_id: str) -> None: ...

# ── Audit ───────────────────────────────────────────────────────────────────

async def read_audit(
    self, *,
    since: str | None = None,           # ISO-Z lower bound
    until: str | None = None,           # ISO-Z upper bound
    kind: str | None = None,            # filter by event kind
    limit: int = 100,
) -> list[dict]:
    """Read hub-cross-cutting audit events. Backed by `audit/{YYYY-MM-DD}.jsonl`."""

# ── Adapter registry ────────────────────────────────────────────────────────

def register_adapter(self, adapter: SessionAdapter) -> None: ...

# ── Lifecycle ───────────────────────────────────────────────────────────────

async def hydrate(self) -> None:
    """Walk the store; rebuild caches; re-fold AdapterState for every active
    session by replaying its WAL. Idempotent."""

async def start(self) -> None:
    """Spawn internal sweepers; write `hub.started` audit line."""

async def close(self) -> None:
    """Cancel sweepers, drain queues, close subscriptions; write `hub.stopped`
    audit line."""
```

## Internal in-memory caches

The hub rebuilds these from disk on `hydrate()`:

- `_passports: dict[str, Passport]` — by agent_id
- `_resumes: dict[str, Resume]` — by agent_id
- `_skills: dict[str, str]` — small LRU; loaded on-demand by `peers(action="describe")`
- `_rules: dict[str, Rule]`
- `_name_to_id: dict[str, str]` — name index
- `_capability_index: dict[str, set[str]]` — capability → set of agent_ids
- `_sessions: dict[str, SessionMetadata]` — by session_id
- `_active_sessions: dict[str, SessionMetadata]` — non-terminal subset
- `_pending_acks: dict[str, set[str]]` — session_id → set of agent_ids whose ack is still outstanding
- `_adapter_states: dict[str, AdapterState]` — folded state per session, invalidated on every WAL append
- `_tasks: dict[str, TaskMetadata]` — observed (not owned) tasks
- `_session_tasks: dict[str, set[str]]` — non-terminal task ids per session
- `_subscriptions: dict[str, list[Subscription]]` — live in-memory fanout map (not persisted)
- `_endpoints: dict[str, LinkEndpoint]` — current connections

Every disk write is paired with a cache update; cache is never authoritative.

## Adapter state cache

The hub holds one `AdapterState` per active session, computed by folding WAL envelopes through `adapter.fold(envelope, state)`. This is what makes `validate_send` and `on_accepted` O(1) instead of O(WAL):

```python
async def post_envelope(self, envelope: Envelope) -> str:
    async with self._session_lock(envelope.session_id):
        meta = self._sessions[envelope.session_id]
        state = self._adapter_states[envelope.session_id]
        adapter = self._adapter_for(meta.manifest)

        adapter.validate_send(meta, envelope, state)              # O(1)

        envelope_id = await self._wal_append(envelope)
        new_state = adapter.fold(envelope, state)                 # O(1)
        self._adapter_states[envelope.session_id] = new_state

        result = adapter.on_accepted(meta, envelope, new_state)   # O(1)
        if result.next_state:
            await self._transition_session(envelope.session_id, result.next_state, result.auto_close_reason)

        await self._dispatch(envelope)
        return envelope_id
```

On `hydrate()`, the hub re-folds every active session's WAL once to rebuild the cache. The fold is pure, so this is deterministic and idempotent. Closed sessions are not folded — their state is irrelevant.

## Sweepers

V1 ships **two** sweepers, both internal. `Hub.start()` spawns an `asyncio.Task` per sweeper that loops on a fixed interval:

```python
# autogen/beta/network/hub/sweepers.py

class _IntervalSweeper:
    """Run a coroutine on a fixed interval until cancelled."""

    def __init__(self, name: str, interval: float, fn: Callable[[], Awaitable[None]]) -> None: ...
    def start(self) -> None: ...
    async def stop(self) -> None: ...
```

| Sweeper | Default interval | What it does |
|---|---|---|
| `_TtlSweeper` | `ttl_sweep_interval` (30s) | Walks `_active_sessions` and `_tasks`; transitions anything past `expires_at` to `EXPIRED`; emits `ag2.session.expired` / `ag2.task.expired`; cascades open tasks under closing sessions. |
| `_ExpectationSweeper` | `expectation_sweep_interval` (10s) | Walks `_active_sessions`; for each `SessionManifest.expectations` entry, evaluates the built-in predicate against `(metadata, AdapterState, WAL tail, now)`; on violation, applies the declared `on_violation` handler (`audit` / `warn` / `notify_session` / `hide` / `remove` / `auto_close`). Also emits `ag2.task.stalled`, `ag2.session.idle`, `ag2.peer.unreachable` / `ag2.peer.reconnected` derived from heartbeat state. |

The framework-core `Watch` primitive is NOT used here — it is the trigger primitive for assembly/compact/aggregate inside the Agent harness, not a fleet manager. `asyncio.Task` + `asyncio.sleep` is enough; no public `Scheduler` surface.

All emitted envelopes go through `post_envelope` so they participate in the same WAL-append + dispatch path as agent-emitted envelopes. There is no parallel "hub-internal events" channel.

Phase 2.0's idempotency dedup is a **query** (`Hub.find_envelope_by_causation`), not a separate sweeper or stored table — the WAL is the source of truth and the in-memory index is rebuilt on `hydrate()` by walking it once. No sweeper needed.

## Audit log

Hub-cross-cutting events that don't belong on any single session's WAL — registrations, rule changes, expectation fires, hub start/stop — are appended to `audit/{YYYY-MM-DD}.jsonl` (one line per event). Each line is `{when, kind, ...kind-specific fields}`:

| `kind` | Fields | Emitted by |
|---|---|---|
| `agent.registered` | `agent_id`, `name`, `owner` | `register` |
| `agent.unregistered` | `agent_id`, `name` | `unregister` |
| `rule.changed` | `agent_id`, `version` | `set_rule` |
| `resume.updated` | `agent_id`, `version`, `source` ("tenant" \| "observed") | `set_resume`, `record_observation` |
| `expectation.violated` | `session_id`, `name`, `on_violation`, `applies_to` | `_ExpectationSweeper` |
| `participant.removed` | `session_id`, `agent_id`, `reason` | `remove` violation handler |
| `hub.started` | `version`, `pid` | `start` |
| `hub.stopped` | (empty) | `close` |

V1 ships read-only access via `Hub.read_audit(...)`. V1 has no rotation policy beyond date-stamped files; an admin task can prune by date. Phase 3 adds an audit retention sweeper.

## Quorum tracking

Multi-party `create_session(required_acks=N)` uses `_pending_acks[session_id]` to track outstanding acks. The set is initialized to all invitees and shrinks on every `EV_SESSION_INVITE_ACK`; on `EV_SESSION_INVITE_REJECT`, the hub recomputes whether `len(invitees) - rejects >= required_acks` is still satisfiable and either continues waiting, transitions to `ACTIVE` if the threshold is now met, or fails the handshake with `quorum_unreachable` if it isn't.

`_pending_acks` is rebuilt on `hydrate()` by replaying invite/ack/reject envelopes from the WAL of any `PENDING` session. It is not persisted as a separate file.

## Adapter version migration

Manifests are snapshotted into `SessionMetadata.manifest` at create time. If `consulting@v1` ships and `consulting@v2` lands later, existing V1 sessions keep their V1 manifest for life — the hub looks up the adapter by `(manifest.type, manifest.version)`, and re-registering an adapter at a new `version` does not mutate any in-flight session. There is no migration tooling in V1; closed-session compaction (Phase 3) drops manifest detail along with the WAL.

## Dispatch invariants

- The hub never calls `Agent.ask` directly. Every delivery is via the `Link.notify(envelope)` frame; the receiving `AgentClient` runs the handler.
- Every WAL append is paired with the fold, the `on_accepted` decision, and subscription fan-out under a single per-session lock so subscribers see exactly-once delivery within one process and adapter state never desyncs from the WAL.
- Every state transition is paired with the envelope that drove it under the same lock.
- All hub mutations route through `_apply_session_event` / `_apply_task_event` so external audit log writers can hook one place if needed.

## At-least-once delivery

Across reconnects, every WAL envelope is delivered ≥1 time per recipient before its session closes. The receiving `AgentClient` checkpoints `inbox.cursor` on every successful `receipt(status="ack")`. On reconnect, hub replays from the cursor up to the WAL head.

| Phase | What ships |
|---|---|
| V1 | Exactly-once by lock construction (single-process, no cross-process replay needed). |
| Phase 2.0 | In-process redelivery via `inbox.cursor` over `LocalLink`. Default handler issues Receipt only after handler completes; on Hello, hub replays unacked. |
| Phase 3 | Cross-process variant over `WsLink`. Same semantics on the wire. |

This is an explicit framework invariant — not an implementation detail. New transports must preserve it.
