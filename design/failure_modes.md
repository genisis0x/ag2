# Failure modes

Pure event-driven choreography has no response guarantee. An agent can open a session and never see a reply; a peer can start a task and never report back; an LLM can hang, crash, or simply choose silence. The framework does **not** promise that peers behave — it can only promise that nothing waits forever, and surface enough signals that an agent can choreograph reactions.

This doc is the contract: what failure modes exist, what the framework does about each, and what the agent is responsible for.

## What the framework guarantees

| Guarantee | Mechanism | V1 status |
|---|---|---|
| **Bounded waits** | Every session has `expires_at`; every task has `expires_at`. Hub TTL sweeper transitions to `EXPIRED` and emits terminal envelopes. | ✅ V1 |
| **At-least-once delivery** | WAL is durable; receipts checkpoint `inbox.cursor`; on reconnect, hub replays from cursor. | V1: in-process exactly-once by lock. Phase 3: cross-process at-least-once. |
| **Idle / ack-stall signals** | `acks_within`, `reply_within`, `max_silence` expectations declared on a manifest fire `ag2.expectation.violated` envelopes (or run `audit` / `auto_close` handlers). | ✅ V1 — see `expectations.py` |
| **Peer reachability signals** | Heartbeat-derived `peer.unreachable` / `peer.reconnected` envelopes propagated to active sessions. | Phase 3 — needs WebSocket transport. |
| **Per-task stall signal** | `task.stalled` when no progress within a per-task threshold. | Phase 2 — per-task `last_progress_at` sweeping. |
| **Quorum signals** | `session.quorum_changed(remaining, required)` when participant counts change in active multi-party sessions. | Phase 2 — N-of-M quorum tracking. |
| **Protocol-shape enforcement** | `SessionManifest.expectations` declared by the adapter author; hub evaluates and applies declared `on_violation` handlers. | ✅ V1 |
| **Adapter contracts** | `validate_send` rejects malformed sends pre-WAL; `on_accepted` advances state per protocol. | ✅ V1 |

## What the agent is responsible for

| Responsibility | Why agent and not framework |
|---|---|
| **Reacting to violations / stalls** | Retry? Pick a different peer? Escalate to human? Give up? Choreography decision; only the agent has the goal context. |
| **Content-quality judgement** | "Did B answer my actual question?" is an LLM evaluation, not a wire concern. |
| **Idempotency of retried work** | Agents aren't idempotent by default; re-asking is a different operation. App code decides whether retry is meaningful. |
| **Saga / compensation** | Build from existing tasks + sessions in app code. Framework-core does not provide a saga engine. |
| **Multi-session orchestration** | Hub knows one session at a time; coordinating across sessions is the orchestrator pattern we're escaping. |

## Failure modes and their handling

### 1. Recipient never acks the invite

**Symptom**: `EV_SESSION_INVITE` sent; no `EV_SESSION_INVITE_ACK` arrives.

**Framework**: `acks_within` expectation on the manifest (default 30s) → `on_violation="auto_close"` for strict adapters like `consulting`. Hub transitions session to `EXPIRED`; initiator's `await client.open(...)` raises or returns a failure handle.

**Agent**: choose another target, retry with a different peer, or give up.

### 2. Peer ack'd but never replies

**Symptom**: Recipient ack'd `EV_SESSION_INVITE`; no content envelope follows within reasonable time.

**Framework**: `reply_within` expectation on the manifest. For `consulting` (default 600s) → `auto_close`. For `conversation` (no default) → caller's `delegate(timeout=)` or `tasks(action="wait", timeout=)` wins eventually via task TTL.

**Agent**: same set of reactions as #1.

### 3. Peer process died / connection dropped

**Symptom**: WS heartbeat misses; transport-level disconnect.

**V1**: not detected — `LocalLink` shares the process, so a process death takes the hub down with it. **Phase 3**: hub marks `runtime.json.reachable=false` after `peer_heartbeat_timeout`; emits `ag2.peer.unreachable(peer_id, since)`. Hub holds queued envelopes up to `inbox.max_pending`; reconnect within session TTL replays from cursor; otherwise inbox-overflow policy applies.

**Agent**: V1 — close the session manually if the peer fails to reply within an expected window; the manifest's `reply_within` / `max_silence` expectations help. Phase 3 — react to `ag2.peer.unreachable` in the WAL projection.

### 4. Task starts but stalls

**Symptom**: `TaskStarted` was emitted but no `TaskProgress` for an extended period.

**V1**: surfaced session-side via `max_silence` expectation if the stalled task is the only thing keeping the session alive. Per-task `ag2.task.stalled` envelopes are **Phase 2** — they require a per-task `last_progress_at` sweeper.

**Agent**: V1 — design tasks with conservative TTLs; `EXPIRED` is the deterministic signal. Phase 2 — react to `ag2.task.stalled` from peers waiting via `tasks(action="wait")`.

### 5. Task expires (TTL)

**Symptom**: `expires_at` passes without a terminal event.

**Framework**: hub TTL sweeper emits `ag2.task.expired`; state ← `EXPIRED`. Both owner and any waiting peers are notified; their `wait()` resolves with the terminal state.

**Agent**: same reactions as #1; for owners, the agent stream sees `TaskExpired` and can record what was lost.

### 6. Session expires (TTL)

**Symptom**: `expires_at` passes without explicit close.

**Framework**: hub emits `ag2.session.expired`; state ← `EXPIRED`. All non-terminal tasks under the session cascade to `EXPIRED` first with `reason="session_closed"`. Session is closed in the WAL.

**Agent**: app code decides whether to open a fresh session, reduce scope, or surface to a human.

### 7. Inbox overflow

**Symptom**: Peer's `inbox.max_pending` is reached.

**Framework**: hub applies `inbox.overflow` policy (`reject` | `drop_oldest` | `drop_newest`). On `reject`, sender's `send` raises `InboxFull`. WAL still records the rejected attempt as an audit envelope.

**Agent**: senders can throttle; receivers can widen `max_pending` or post-process faster.

### 8. Adapter rejects send

**Symptom**: `adapter.validate_send` raises (wrong turn, wrong sender, protocol violation).

**Framework**: hub returns `ProtocolError` to sender; envelope is **not** appended to WAL; adapter state unchanged.

**Agent**: caller's tool returns an error string; LLM can read the error and reformulate or call `sessions(action="info")` to inspect state.

### 9. Expectation violated mid-session

**Symptom**: Some declared expectation (`min_participation`, `max_silence`, etc.) fires.

**Framework**: hub applies the declared `on_violation` handler (`audit` | `warn` | `notify_session` | `hide` | `remove` | `auto_close`). All handlers are passive — hub records, signals, or removes; never substitutes content.

**Agent**: react to the `ag2.expectation.violated` envelope according to choreography.

### 10. Hub itself crashes

**Symptom**: Hub process dies.

**Framework**: V1 — single-process; agents lose their AgentClients; `HubClient.frames()` raises on next read. On hub restart with `Hub.open(store)`, `hydrate()` rebuilds from disk: identities, rules, sessions (state and adapter state by re-folding WAL), tasks, runtime records. Phase 3: WS clients reconnect with `since` cursor; replay covers in-flight envelopes.

**Agent**: app-level supervisor restarts the hub; reconnect logic is in `HubClient`. No data loss for committed envelopes (WAL is durable). In-flight `notify`s that hadn't been ack'd are re-delivered via cursor replay.

## Configuration knobs

V1 keeps the per-tenant `LimitsBlock` deliberately small — only the
fields the hub actually enforces:

```python
@dataclass(slots=True)
class LimitsBlock:
    max_concurrent_sessions: int = 0
    max_concurrent_tasks: int = 0
    session_ttl_default: str = "2h"
    task_ttl_default: str = "15m"
    rate: RateBlock = ...
    delegation_depth: int = 5
    inbox: InboxBlock = ...
```

Failure-mode thresholds intentionally live on the **manifest** in V1
via the adapter's declared `expectations`, not on `LimitsBlock` —
that keeps the V1 mechanism unified (one knob per behaviour) and
prevents callers from setting per-tenant rules that look enforced but
aren't:

* `acks_within(seconds)` — invitee must ack within T after `EV_SESSION_INVITE`.
* `reply_within(seconds)` — addressed participant must respond within T.
* `max_silence(seconds)` — session must see content within T.

Phase 2 adds `turn_within`, `progress_within`, `min_participation`;
Phase 3 adds peer reachability (which needs the WebSocket transport).
See [sessions.md](sessions.md) for the full expectation table.

## What this is NOT

- **Not a saga engine.** Compensating actions are app code.
- **Not a transaction system.** No 2PC, no atomic multi-session commits.
- **Not response-guarantee infrastructure.** Pub/sub guarantees delivery; nothing guarantees reply.
- **Not a circuit breaker.** Agent-side middleware (or future tenant transforms) implement that pattern.
- **Not an SLA enforcer.** Agents can declare SLAs (via expectations); the hub will surface violations; the *reaction* is the agent's call.

## Quick reference — events by mode

V1 ships only the events listed under "V1" below. Phase 2/3 events
land with their producers; the constants are not exposed until then.

| Mode | Event(s) emitted | Origin | Phase |
|---|---|---|---|
| Invite never ack'd | `ag2.expectation.violated(name="acks_within")`, then `ag2.session.expired` (if `auto_close`) | Hub | V1 |
| Reply never sent | `ag2.expectation.violated(name="reply_within")` | Hub | V1 |
| Session silent | `ag2.expectation.violated(name="max_silence")` | Hub | V1 |
| Session expired | `ag2.session.expired` | Hub | V1 |
| Session closed | `ag2.session.closed` | Hub | V1 |
| Adapter rejected send | `ProtocolError` raised back to sender (no envelope; offending send is **not** WAL'd) | Hub | V1 |
| Inbox overflow | `InboxFull` raised back to sender | Hub | V1 |
| Peer disconnected | `ag2.peer.unreachable(peer_id, since)` | Hub | Phase 3 |
| Peer reconnected | `ag2.peer.reconnected(peer_id)` | Hub | Phase 3 |
| Task stalled | `ag2.task.stalled(task_id, last_progress_at)` | Hub | Phase 2 |
| Session quorum changed | `ag2.session.quorum_changed(remaining, required)` | Hub | Phase 2 |
| Participant removed | `ag2.participant.removed(agent_id, reason)` | Hub | Phase 2 (`remove` violation handler) |
