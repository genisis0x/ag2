# AG2 Beta Network — Plan

`autogen/beta/network/` lets multiple `Agent`s discover each other, exchange durable messages, follow protocol-driven multi-participant interactions, and coordinate long-running work across processes. It is purely additive on top of framework-core: every existing `Agent` continues to work standalone with no behavioural change when the network package is not imported.

## Goals (V1)

- Agent registry with three-part identity: **`Passport`** (immutable id + billing) + **`Resume`** (mutable claims + observed track record) + optional **`SKILL.md`** (Anthropic-format LLM-facing usage doc). Discovery returns different slices for `find` vs `describe`.
- Four built-in session types (`consulting`, `conversation`, `discussion`, `workflow`) plus an extensible `SessionAdapter` Protocol — `workflow` carries declarative `Transition` graphs for orchestrated flows (see [workflow.md](workflow.md))
- `SessionManifest.expectations` — declarative protocol-shape contracts the hub enforces with passive `on_violation` handlers
- Per-tenant rules: `access` + `limits` (transforms deferred to Phase 3). Per-tenant failure-mode thresholds dropped from V1: peer reachability needs the WebSocket transport (Phase 3); session-idle is covered by the manifest-level `max_silence` expectation; per-task stall surfacing is Phase 2.
- **Task as a framework-core primitive** (`autogen/beta/task.py`) — any Agent can wrap work in a trackable lifecycle, with or without a hub. The network is one observer.
- Two view policies (`FullTranscript`, `WindowedSummary`) — `Composite` deferred to Phase 2
- A `NetworkClient` Protocol — `AgentClient` is the V1 implementation; future `HumanClient` / `AdminClient` slot in
- A `NetworkPlugin` that attaches to an `Agent`, adds 6 LLM tools (2 flat + 4 grouped), and injects network metadata into prompts
- Idle and ack-stall signals via a focused expectation set: 3 evaluators (`acks_within`, `reply_within`, `max_silence`) × 3 handlers (`audit`, `notify_session`, `auto_close`). Peer-reachability and per-task-stall envelopes (`ag2.peer.unreachable`, `ag2.task.stalled`) are Phase 2/3 — they need transport-level disconnect events and per-task last-progress sweeping respectively.
- Append-only audit log for hub-cross-cutting events (register/unregister, rule changes, expectation fires) — single `audit.jsonl`, rotation deferred to Phase 2
- In-process `LocalLink` transport
- `MemoryKnowledgeStore` + `DiskKnowledgeStore` persistence (already in framework-core)

## Non-goals (V1)

- WebSocket / HTTP transport (Phase 3)
- Rule transforms (Phase 3)
- Cross-process / multi-host hub deployment
- JWT / mTLS / SignedChallenge auth — V1 ships `NoAuth` only
- Multi-hub federation; multi-identity per Agent; signed envelope chains
- Token / cost budget enforcement; archival sweeper; audit log rotation (V1 writes a single `audit.jsonl` indefinitely)
- Task phases; task cancellation; custom expectation evaluators (Phase 2)
- Saga / compensation engine; circuit breakers (app-level concerns)
- `notification`, `broadcast`, `auction` adapters; `BySpeaker`, `PreviousOnly` views (Phase 2 on demand)
- `Composite` view policy (Phase 2)
- 3 expectation kinds — `turn_within`, `progress_within`, `min_participation` (Phase 2)
- 3 violation handlers — `warn`, `hide`, `remove` (Phase 2)
- N-of-M quorum tracking — V1 ships all-or-nothing accept; partial-quorum recomputation, `required_acks` integer, and `quorum_changed` events are Phase 2
- `drop_oldest` / `drop_newest` inbox overflow policies — V1 reject-only
- Discussion `dynamic` and `static` ordering modes — V1 ships round_robin only
- Streaming `chunk` frames at the wire layer — V1 is text-only envelopes
- Rate limiter token bucket — V1 honors `delegation_depth` and concurrency caps but skips per-minute throttle
- `allowed_events` field on `SessionManifest` — removed entirely (was never validated)

Everything in this list is an AG2 Cloud or later-phase concern. Framework-core V1 always works without it.

## Core principles

1. **Sessions are protocols, not flat channels.** Every Agent-to-Agent exchange happens inside a `Session` with a defined type and adapter. Adapters define the choreography (or orchestration, in `workflow`'s case); participants follow it. The hub is never the orchestrator — orchestration logic lives in the adapter's pure `on_accepted` method, derived from folded state.

2. **Adapters are stateless.** Every decision derives from session metadata plus a per-session `AdapterState` folded from the WAL. The hub `hydrate()`s state from disk on restart by re-folding. `validate_send` and `on_accepted` are O(1), not O(WAL).

3. **Context is a per-participant projection.** The WAL is durable truth; what each participant's LLM sees is a `ViewPolicy` projection. View policies compose with framework-core `compact.py` so the same `SummarizeCompact` strategy compresses both an Agent's local history and the session view tail.

4. **Hub is infrastructure, not a trust authority.** The trust boundary runs through `HubClient` / `AgentClient`. The hub never imports tenant Python modules and never executes tenant callables. Future rule transforms run on the tenant side.

5. **Tasks are agent-owned; the network observes.** Task is a framework-core primitive. Any Agent can wrap work in `agent.task(...)`. Hub does not create, assign, or cancel — it mirrors `Task*` events from the agent's stream and applies TTL.

6. **No response guarantee, only liveness.** Pure choreography means peers can stay silent. The framework promises bounded waits (TTL), liveness signals, and protocol-shape enforcement via `Expectation`s — never that a reply will arrive. Reaction is the agent's job.

7. **Identity is registration input.** A bare `Agent` has no `agent_id`. Registering with a hub stamps a UUID7. Re-registering produces a new id.

8. **One transport surface, two encodings.** WebSocket for stateful, HTTP for stateless CRUD (Phase 3). `LocalLink` runs the same `Link` Protocol in-memory for V1 and tests.

9. **Verbs are real tools.** The `NetworkPlugin` attaches verbs to `agent.tools` at registration so the LLM sees them in every turn — not stamped per-handler. Outside-handler initiation works.

## Architecture

```
┌─────────────────────────── tenant process A ────────────────────────────┐
│                                                                          │
│   Agent  ──[ AgentClient ]──┐                                            │
│   Agent  ──[ AgentClient ]──┼──[ HubClient ]──┐                          │
│                              │                  │                          │
└──────────────────────────────┼──────────────────┼──────────────────────────┘
                               │                  │
                               │  ┌───────────────▼─────────────────┐
                               │  │            Hub                  │
                               │  │  • registry (identity + rule)   │
                               │  │  • session state machines       │
                               │  │  • task state machines          │
                               │  │  • dispatch (notify / send)     │
                               │  │  • access + limits enforcement  │
                               │  │  • TTL sweeper                  │
                               │  │  • adapter state cache          │
                               │  │  KnowledgeStore  (persistence)  │
                               │  └─────────────────▲───────────────┘
                               │                    │
┌──────────────────────────────┼────────────────────┘
│                              │
│   Agent  ──[ AgentClient ]──[ HubClient ]
│
│       tenant process B (Phase 3 — separate host)
└──────────────────────────────────────────────────
```

## Layering

Each layer is replaceable without disturbing the others.

```
L1  Transport         Link + frames
L2  Routing           Hub + WAL + dispatch + adapter state cache
L3  Choreography      SessionManifest (data) + SessionAdapter (code)
L4  Context           ViewPolicy + recall/quote
L5  Identity & Trust  Passport + Resume + SKILL.md + Rule + Auth
L6  Bridge            NetworkClient impls (AgentClient = V1; HumanClient, AdminClient later)
L7  Verb surface      NetworkPlugin + 6 LLM tools
```

## Module layout

```
autogen/beta/
├── task.py                           # framework-core (NEW): Task, TaskSpec, TaskState,
│                                     # TaskMetadata, agent.task() entry point
└── events/task_events.py             # framework-core (EXISTS): already has
                                      # TaskStarted, TaskProgress, TaskCompleted, TaskFailed
                                      # — V1 only adds TaskExpired

autogen/beta/network/
├── __init__.py                       public surface (re-exports)
├── ids.py                            UUID7 helper
├── errors.py                         NetworkError hierarchy
├── identity.py                       Passport, Resume, ResumeExample, ObservedStat,
│                                     CostProfile, AuthBlock, AgentRuntime
├── envelope.py                       Envelope, EV_* constants, Priority
├── session.py                        SessionMetadata, SessionManifest,
│                                     Expectation, ParticipantSchema,
│                                     Participant, ParticipantRole, SessionState
├── rule.py                           Rule, AccessBlock, LimitsBlock,
│                                     SessionTypeAccess, RateBlock, InboxBlock
├── transitions.py                    Transition, TransitionTarget Protocol + 5 V1
│                                     concretes, TransitionCondition Protocol + 3
│                                     V1 concretes, TransitionGraph (+ dumps/loads
│                                     and named registries)
├── task_mirror.py                    Bridges agent's Task* events to hub
├── auth.py                           AuthAdapter, NoAuth, AuthRegistry
├── adapters/
│   ├── __init__.py
│   ├── base.py                       SessionAdapter Protocol, AdapterState, AdapterResult
│   ├── consulting.py
│   ├── conversation.py
│   ├── discussion.py
│   └── workflow.py                   WorkflowAdapter, WorkflowState (M4)
├── views/
│   ├── __init__.py
│   ├── base.py                       ViewPolicy Protocol
│   └── builtin.py                    FullTranscript, WindowedSummary  (Composite → Phase 2)
├── transport/
│   ├── __init__.py
│   ├── frames.py                     all frame dataclasses + encode/decode
│   ├── link.py                       LinkClient, LinkEndpoint Protocols
│   └── local.py                      LocalLink + LocalLinkClient + LocalLinkEndpoint
├── hub/
│   ├── __init__.py
│   ├── core.py                       Hub class
│   ├── layout.py                     FS path helpers
│   ├── audit.py                      Audit log writer/reader
│   ├── sweepers.py                   _IntervalSweeper, _TtlSweeper, _ExpectationSweeper
│   └── expectations.py               Built-in expectation evaluators
├── client/
│   ├── __init__.py
│   ├── network_client.py             NetworkClient Protocol
│   ├── hub_client.py                 HubClient
│   ├── agent_client.py               AgentClient
│   ├── session.py                    Session client handle
│   ├── task.py                       Task client handle
│   ├── handlers.py                   default notify handlers (decomposed hooks)
│   ├── plugin.py                     NetworkPlugin, NetworkContextPolicy
│   ├── inject.py                     SessionInject, AgentClientInject, HubInject, TaskInject
│   ├── skill_render.py               SKILL.md frontmatter parser + fallback renderer
│   └── tools/
│       ├── __init__.py
│       ├── say.py
│       ├── delegate.py
│       ├── peers.py
│       ├── sessions.py
│       ├── tasks.py
│       ├── context.py
│       └── handoff.py                Materializes one tool per ToolCalled
│                                     transition in a workflow's graph (M4)
└── policies.py                       qualified-key constants (SESSION_DEP, AGENT_CLIENT_DEP, HUB_DEP, TASK_DEP)
```

## Phases

### Phase 1 — In-process foundation

Goal: minimum end-to-end with every load-bearing contract in place, tested against `LocalLink` only. Phase 1 lands as **four sequential milestones** (M1 → M2 → M3 → M4), each independently mergeable to `main` because `autogen.beta.network` is opt-in by import path. No milestone rewrites earlier work — every milestone is strictly additive. M1–M3 deliver the choreography surface; M4 layers orchestrated workflows on top as the migration path for AG2-classic's `GroupChat` / `Handoffs` / `AfterWork`.

| Milestone | Status | Tests |
|---|---|---|
| Framework-core precondition | ✅ shipped (`77622fac9e`) | 22 tests |
| M1 — Foundation | ✅ shipped (`99d9e6da82`) | 5 integration tests |
| M2 — Consulting loop | ✅ shipped (`ee67cb258d`) | 8 integration tests |
| M3 — Multi-party + observability | ✅ shipped (`5f667265f6`) | 62 integration + 2 anthropic smoke |
| M4 — Workflow orchestration | ✅ shipped (this PR) | 26 integration + 1 anthropic smoke |

Beta suite total: **1596 passing**, zero regressions across milestones.

**Framework-core precondition (separate PR; lands before M1):**
- New `autogen/beta/task.py` — `Task`, `TaskSpec`, `TaskState`, `TaskMetadata`, `Agent.task(...)` entry point, `TaskInject` annotation
- Extend existing `autogen/beta/events/task_events.py`: add `TaskExpired`; widen `TaskCompleted.result` to `Any`; add optional `spec` to `TaskStarted` and `payload` to `TaskProgress` (additive, backward-compatible)
- `_spawn_subtask` is **not** wrapped in a Task in V1 — `_run_task` already emits `TaskStarted/Progress/Completed/Failed` on the parent stream, which is the contract the network mirror observes. Wrapping (so `TaskInject` resolves inside subagent context) is a Phase 2 nice-to-have

#### M1 — Foundation ✅ (shipped, ~1100 LOC)

Pure plumbing. No LLM call yet; tested with raw envelopes only.

- `ids.py`, `errors.py`, `policies.py` (qualified-key DI constants)
- `identity.py` — `Passport`, `Resume` (skill_md stored as plain text; frontmatter parser deferred to M3)
- `envelope.py` — `Envelope`, `EV_*` constants, `Priority`, `visible_to()`
- `rule.py` — `Rule`, `AccessBlock`, `LimitsBlock` (concurrency caps + TTL parsing only; no rate token bucket)
- `auth.py` — `AuthAdapter` Protocol, `NoAuth`
- `transport/` — `Link` Protocol, `LocalLink`, frame vocabulary subset (no chunk frames)
- `hub/layout.py` — FS path helpers
- `hub/core.py` — registry + `register` / `unregister` / `post_envelope` only; **no sweepers, no expectations, no audit log**
- `client/network_client.py`, `client/hub_client.py`, `client/agent_client.py` — bare bones; `register` returns an `AgentClient` capable of raw `send`
- Persistence layout under `KnowledgeStore` (no `inbox.cursor`, no per-task `events.jsonl`)

Exit: integration test where two `AgentClient`s register through `LocalLink` and exchange raw envelopes; `Hub.hydrate()` rebuilds passport/resume/rule caches from disk.

#### M2 — Consulting loop ✅ (shipped, ~1900 LOC)

First end-to-end LLM-driven session.

- `adapters/base.py`, `adapters/consulting.py` (default expectations declared but only `auto_close` handler wired; full expectation sweeper arrives in M3)
- `views/base.py`, `views/builtin.py` — `FullTranscript` only
- `client/session.py` (Session client handle), `client/task.py` (Task client handle)
- `task_mirror.py` — bridges agent's `Task*` events to `Hub.observe_task`; **`record_observation` deferred to M3**
- `client/handlers.py` — default notify handler with DI stamping (`SESSION_DEP`, `AGENT_CLIENT_DEP`, `HUB_DEP`, `TASK_DEP`)
- `client/inject.py` — `SessionInject`, `AgentClientInject`, `HubInject`, `TaskInject`
- `client/plugin.py` — `NetworkPlugin` + `NetworkContextPolicy` rendering peer list + active session into the prompt prefix
- `client/tools/say.py`, `client/tools/delegate.py` — only the 2 flat tools
- `hub/sweepers.py` — `_TtlSweeper` only (cascades task expiry on session close)
- Hub access + limits enforcement (delegation depth + concurrency caps)
- Adapter state cache + `Hub.hydrate()` re-folds active session WALs

**Design refinements during M2 (deviations from the original plan):**
- `NotifyFrame` stamps `recipient_id` per-delivery — the hub iterates per-recipient anyway, and stamping the target lets `HubClient` demux directly without re-walking session participants. Required so broadcasts (`audience=None`) route correctly when one connection hosts multiple identities.
- DI inject annotations use `Annotated[Any, Inject(...)]` rather than the concrete classes — Pydantic (used by the `tool` decorator's signature schema) cannot generate a JSON schema for `Session` / `AgentClient` / `Hub` (non-Pydantic types). Type precision is lost, but these injects never appear in the LLM-facing parameter surface (they resolve from `context.dependencies`).
- Default notify handler does an adapter-driven "can I respond?" probe (`adapter.validate_send` with self as sender) before engaging the LLM. Prevents the consulting initiator from auto-firing a second LLM turn when the respondent's reply lands on its inbox.
- Consulting adapter returns `next_state=CLOSED` directly (skipping the transitional `CLOSING` state). M2 has no async cleanup phase between CLOSING → CLOSED; the transitional state is reserved for future adapters that need a quiescence window.
- Adapter state cache **O(1) benchmark deferred to M3** — consulting is 1Q1R (max 2 turns), so a "1000-turn" benchmark is meaningful only with the `discussion` adapter.

Exit: LLM-driven 1:1 consulting end-to-end. Alice registers, Bob registers, Alice's LLM calls `delegate(target="bob", prompt=..., blocking=True)`, Bob's notify handler runs Bob's LLM, Bob replies, consulting auto-closes via the adapter's `on_accepted`. Validated by `test/beta/network/test_m2_consulting.py` (8 tests).

#### M3 — Multi-party + observability ✅ (shipped `5f667265f6`, ~5300 LOC)

Full V1 surface. Landed as 5 internal cuts under one milestone commit.

- **Cut 3.1** — `adapters/conversation.py` (1+1 bidirectional, no auto-close); `views/builtin.py` adds `WindowedSummary` with `CompactionSummary` head for bounded prompt size at any turn count.
- **Cut 3.2** — `adapters/discussion.py` with `round_robin` ordering only (`dynamic`/`static` deferred); multi-party N-of-N handshake reuses M1's `pending_acks` machinery (any reject fails the session — N-of-M quorum is Phase 2).
- **Cut 3.3** — `hub/expectations.py` ships 3 evaluators (`acks_within`, `reply_within`, `max_silence`) and 3 handlers (`audit`, `notify_session`, `auto_close`) on a `_ExpectationSweeper` (10s tick, configurable, 0 disables). Per-(session, expectation, violator) dedup in `_fired_violations`, cleared on terminal session transition. `hub/audit.py` ships a single append-only `audit.jsonl` recording register/unregister, set_resume/skill/rule, and every violation fire.
- **Cut 3.4** — `Hub.record_observation` updates `Resume.observed[capability]` from terminal task events; `task_mirror.py` invokes it automatically when `TaskSpec.capability` is set; default notify handler attaches the mirror to every LLM turn so `agent.task(capability=)` is observed end-to-end. `registry/by_capability.json` maintained on register/unregister/observe and rebuilt from resumes on hydrate. `client/skill_render.py` parses SKILL.md frontmatter and renders a passport+resume fallback for `peers(action="describe")`. `AgentClient.set_resume` / `set_skill` / `set_rule` (M2) joined by `add_example`.
- **Cut 3.5** — 4 grouped LLM tools: `client/tools/peers.py`, `client/tools/sessions.py`, `client/tools/tasks.py`, `client/tools/context.py`. Wired into `NetworkPlugin` so every registered Agent gets the full surface (2 flat + 4 grouped = 6 tools).

**Design refinements during M3 (deviations and bug fixes):**
- `_transition_session` now releases dangling `_session_open_waiters` futures when the sweeper auto-closes a `PENDING` session. Previously `create_session` would block on the waiter until `invite_ack_timeout` even after out-of-band closure (e.g. via the `acks_within(auto_close)` expectation).
- `SessionInject | None = None` silently bypassed `fast_depends` injection — wrapping the `Annotated` in a `Union` hides the `Inject` metadata so the param was never resolved. Latent in M2's `say`/`delegate` (M2 tests went through `Agent.ask`, never through direct `tool(event, context)` dispatch). Pattern is now `session: SessionInject = None` (no Union); fixed in all 6 tools and documented in `network_plugin.md`.
- `_ScriptedConfig` test helper added in `test/beta/network/_helpers.py` because `autogen.beta.testing.TestConfig` resets its iterator on every `create()`, so a single agent keeps replaying its first scripted reply across multiple turns. Multi-turn LLM-driven adapter tests need persistent scripts; this wrapper feeds one shared cursor across all clients it produces.
- Hydrate scale test ships at 100 sessions × 100 envelopes (10k envelopes total, ~1.6s including the discussion variant) rather than the 100 × 1000 (100k envelopes, ~10s populate) called out in the original plan. Hydrate itself stays sub-second even at the larger scale; the constraint is populate write throughput, which is irrelevant to the correctness contract being tested. Bumping `ENVELOPES_PER_SESSION` in `test_m3_hydrate_scale.py` runs the full sweep locally.
- Adapter state cache O(1) benchmark (deferred from M2) is **deferred again to Phase 2**'s perf-regression suite. The hydrate scale test exercises folding correctness; perf is a separate concern.
- LLM-driven 5-way discussion exit criterion lives as an `@pytest.mark.anthropic` smoke test (`test/beta/providers/anthropic/test_network_smoke.py`) rather than a `TestConfig`-mocked integration test. Mocking 5 LLMs taking turns through tool calls is brittle; verifying against the real model is cheap (~$0.005 at haiku rates) and catches more.

Exit: ✅ Validated by 62 in-tree integration tests covering each cut + 2 anthropic smoke tests against real `claude-haiku-4-5`. The smoke suite proves the M3 exit criterion: alice's LLM autonomously calls `peers(action="find", capability="math")` → `delegate(target="bob", prompt=...)` → returns "204" for "12 × 17"; 5 LLM agents take round-robin turns via the `say` tool with all contributions landing in WAL in `[alice, bob, carol, dave, erin]` order.

#### M4 — Workflow orchestration ✅ (shipped this PR, ~1400 LOC)

The orchestrator surface — successor for AG2-classic's `GroupChat` + `Handoffs` + `AfterWork`. Strictly additive on top of M3; no rewrites.

- `transitions.py` — `Transition`, `TransitionTarget` Protocol + 5 V1 concretes (`AgentTarget`, `RoundRobinTarget`, `StayTarget`, `RevertToInitiatorTarget`, `TerminateTarget`), `TransitionCondition` Protocol + 3 V1 concretes (`Always`, `FromSpeaker`, `ToolCalled`), `TransitionGraph` with `to_dict()` / `dumps()` / `loads()` and named registries (`register_target`, `register_condition`). Convenience factories `TransitionGraph.round_robin(...)` and `.sequence(...)` for the common patterns.
- `adapters/workflow.py` — `WorkflowAdapter`, `WorkflowState`. Stateless and pure; reuses the existing dispatch path with no hub changes. `WorkflowState` snapshots `participant_order` + `creator_id` + `graph_data` at `initial_state` so `fold` (which has no metadata) can compute the next speaker on each accepted envelope.
- `client/tools/handoff.py` — `make_handoff_tool(client, tool_name)` builds one LLM tool that posts `ag2.handoff`; `make_handoff_tools_for_graph(client, graph)` materializes one tool per unique `ToolCalled` transition. `NetworkPlugin.register_workflow(graph)` is the convenience wrapper that appends them to `agent.tools` (idempotent — repeat calls don't duplicate).
- `EV_HANDOFF` (`ag2.handoff`) added to envelope's stable event-type set.
- `client/handlers.py` — `default_handler` now treats `EV_HANDOFF` as a substantive turn-advancing envelope (alongside `EV_TEXT`). The next speaker's notify handler engages their LLM with the handoff's `reason` synthesised into `"[Handed off via <tool>] <reason>"`. Without this the workflow stalls after the handoff because the receiving agent's handler ignored the envelope.

**Design refinements during M4:**
- `TransitionTarget.resolve` and `TransitionCondition.evaluate` deliberately take only `(state, envelope)` — no metadata. `WorkflowState` carries `participant_order` (for `RoundRobinTarget`) and `creator_id` (for `RevertToInitiatorTarget`) so transitions can be evaluated inside `WorkflowAdapter.fold`, which has no metadata access. Doc previously hinted at `(metadata, state, envelope)` — workflow.md will follow up.
- Handoff tools are scoped per-agent (registered onto `agent.tools`) rather than per-session. If an agent joins multiple workflows, the union of tools is fine: each tool emits on the *current* session, and non-matching adapters fall through to `default_target`. Per-session scoping is Phase 2 once we have a clean reason to need it.
- `WorkflowState.graph_data` stores the JSON-friendly `to_dict()` form. `fold` deserialises on each call (cheap; the graph is small and bounded). Caching the deserialised graph on the adapter would tie state to the adapter instance, which violates the stateless-adapter principle.
- `WorkflowGraph.sequence(steps)` sets `max_turns=len(steps)` so the pipeline terminates cleanly after the last step posts. The exit criterion's "triage closes via TerminateTarget" is exercised in tests via `hub.close_session(...)` (deterministic) rather than waiting on the LLM to call `sessions(action="close")`.

Exit: ✅ Validated by 26 in-tree integration tests in `test/beta/network/test_m4_workflow.py` (4 patterns + Hub.hydrate recovery + serialization round-trip + registry extension + handoff tool dispatch) and 1 anthropic smoke test in `test/beta/providers/anthropic/test_workflow_smoke.py` proving the full exit criterion against real `claude-haiku-4-5`: triage's LLM autonomously calls `transfer_to_eng` → eng's notify handler engages eng's LLM with the synthesised handoff prompt → eng's reply rotates control back to triage via `FromSpeaker(eng) → RevertToInitiatorTarget` → workflow state survives a mid-flow `Hub.hydrate()` → triage closes the session.

See [workflow.md](workflow.md) for the full design.

### Phase 2 — Multi-participant power features

- `TaskState.CANCELLED` + `task.cancel(reason)` + `EV_TASK_CANCELLED` + `ag2.task.cancel_request` envelope
- `TaskPhase` + `current_phase` + phase events for saga-style tasks
- Custom expectation kinds (user-registered evaluators)
- 3 additional expectation evaluators: `turn_within`, `progress_within`, `min_participation`
- 3 additional violation handlers: `warn`, `hide`, `remove`
- N-of-M quorum tracking — `required_acks` integer, partial-quorum recomputation, `quorum_changed` events
- `Composite` view policy
- `BySpeaker`, `PreviousOnly` view policies (only on demand)
- `notification`, `broadcast`, `auction` adapters as proofs of extensibility (live in `examples/` if not framework-core)
- Discussion `dynamic` and `static` ordering modes
- Streaming `chunk` frames + `Session.send_chunk` / `Session.iter_chunks`
- Audit log daily rotation
- `drop_oldest` / `drop_newest` inbox overflow policies
- Rate limiter token bucket (per-minute, burst)
- `network_changed` push + cache invalidation in `NetworkContextPolicy`
- `inbox_pressure` backpressure events
- Adapter state cache benchmark regression suite
- Workflow extensions: `RandomTarget`, `LLMSelectorTarget` (async sub-session resolution), `NestedSessionTarget` (SocietyOfMind), `ContextExpr` and `TurnCountReached` conditions, `SubGraph` composition target, saga / `OnFailure` transitions, `dispatch_audience` adapter hook (per-recipient routing optimization), classic `Pattern` → `WorkflowGraph` migration helper

### Phase 3 — Cross-process

- `WsLink` (WebSocket transport) — same `Link` Protocol surface
- HTTP CRUD surface (10 endpoints) via Starlette
- `ApiKeyAuth` adapter
- Reconnect with subscription cursor + at-least-once redelivery via `inbox.cursor`
- `RuleChangedFrame` push + `set_rule` API hot-reload
- Rule transforms (4 stages, named + Python forms) + standard library transforms (`redact_pii`, `truncate_long_content`, `stamp_audit_header`)
- Idempotency dedup table + sweeper

### Phase 4 — Polish

- Smoke tests against real LLM providers
- Examples / playground
- User-facing docs

## Documents

Read in this order on first pass; the docs are otherwise standalone.

- [identity.md](identity.md) — `Passport`, `Resume`, `SKILL.md`, `AuthBlock`, registration, `NoAuth` / `ApiKeyAuth`
- [envelope.md](envelope.md) — `Envelope`, event types, `audience` addressing
- [sessions.md](sessions.md) — `SessionManifest`, `SessionAdapter`, V1 adapters
- [workflow.md](workflow.md) — `WorkflowAdapter`, `Transition` vocabulary, orchestrated flows
- [views.md](views.md) — `ViewPolicy`, V1 built-ins
- [tasks.md](tasks.md) — Task as framework-core primitive; network as observer
- [rules.md](rules.md) — Access + limits (V1; transforms Phase 3)
- [hub.md](hub.md) — Hub internals, sweepers, invariants
- [clients.md](clients.md) — `NetworkClient`, `HubClient`, `AgentClient`
- [transport.md](transport.md) — `Link` Protocol, `LocalLink`, frames
- [network_plugin.md](network_plugin.md) — `NetworkPlugin` and 6 LLM tools
- [persistence.md](persistence.md) — `KnowledgeStore` layout
- [failure_modes.md](failure_modes.md) — what the framework guarantees vs the agent

## Naming and style rules

These follow `CLAUDE.md` and apply to every module under `autogen/beta/network/`:

- No `from __future__ import annotations`.
- No global variables; no top-level side-effect calls.
- For filesystem paths: `pathlib.Path` internally; public signatures accept `str | os.PathLike[str]`.
- Top-level imports only; no function-level imports (lazy imports for optional deps go in `try / except ImportError` at module top).
- No nested functions in runtime execution paths (decorators are exempt).
- No side effects in `__init__` methods — `__init__` stores params; `start()` / `open()` / first method call performs side effects.
- Dataclasses use `@dataclass(slots=True)`.
- All `Protocol` types are `runtime_checkable` only when introspection is needed.
- Async throughout; sync-only helpers are `_private`.
- Re-export rules: every public class is exported from its module's `__init__.py` and listed in `__all__`. Optional-dep imports use the `missing_optional_dependency` fallback per `CLAUDE.md`.

Test file layout mirrors source: `test/beta/network/test_<module>.py`. Smoke tests against real providers under `test/beta/providers/{anthropic,openai,gemini}/`, marked with the matching `@pytest.mark.{provider}` so they're excluded from default unit runs (which use `--ignore=test/beta/providers`).

## Appendix — End-to-end example

A 5-way debate. Each Agent runs in its own `HubClient` (could be one process or five, in V1 it's one).

```python
hub = await Hub.open(store=DiskKnowledgeStore("/var/hub"))

clients = []
for name in ("alice", "bob", "carol", "dave", "erin"):
    agent = Agent(
        name=name,
        config=AnthropicConfig(model="claude-sonnet-4-6"),
        knowledge=KnowledgeConfig(
            store=DiskKnowledgeStore(f"/var/agents/{name}"),
            aggregate=WorkingMemoryAggregate(every_n_turns=5),
        ),
    )
    passport = Passport(name=name, owner="acme", provider="anthropic", model="claude-sonnet-4-6")
    resume = Resume(
        claimed_capabilities=["debate", "analysis"],
        domains=["policy", "economics"],
        summary=f"{name}: senior policy analyst focusing on cost/benefit framing.",
    )
    skill_md = (Path(__file__).parent / f"skills/{name}.md").read_text()  # optional
    rule = Rule(limits=LimitsBlock(session_ttl_default="4h"))

    hub_client = HubClient(link=LocalLink(hub))
    agent_client = await hub_client.register(agent, passport, resume, skill_md=skill_md, rule=rule)
    # NetworkPlugin attached: agent.tools now includes say, delegate, peers,
    # sessions, tasks, context. agent's assembly chain has NetworkContextPolicy.
    clients.append(agent_client)

# Alice opens a 5-way discussion.
session = await clients[0].open(
    type="discussion",
    target=["bob", "carol", "dave", "erin"],
    knobs={"ordering": "round_robin"},
    intent="debate framework X adoption",
)

# Each turn, the round-robin discussion adapter advances `expected_next_speaker`
# in AdapterState. The next speaker's notify handler:
#   1. Reads WAL up to the current envelope.
#   2. Calls WindowedSummary(recent_n=10).project(...). Bounded ~8K tokens.
#   3. Stamps SESSION_DEP / AGENT_CLIENT_DEP / HUB_DEP / TASK_DEP.
#   4. Calls agent.ask(*projection, current_envelope) — verbs already on
#      agent.tools from registration.
#   5. Posts the reply via `say(...)` from inside the LLM, which the tool
#      converts to session.send(reply, causation_id=...).
#
# Private side-channel from alice to bob:
#   await session.send("between you and me, ...", audience=[bob_id])
# Hub records in WAL but only delivers notify() to bob. Carol/dave/erin's
# WindowedSummary skips it (visible_to() returns False).
#
# carol calls a verb to look back:
#   context(action="search", query="alice cost argument", scope="session")
# Returns up to 10 excerpt dicts.
#
# Each Agent's KnowledgeStore accumulates working memory across this session
# and into future sessions.

await session.close()
await hub.close()
```

The framework provides the protocol and the projection primitives; the application chooses participants, session type, and steering. Bounded prompt size at any turn count. Subset addressing for private side-channels. Working memory accumulating outside the session.
