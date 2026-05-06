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
- Per-envelope tenant logic via `AgentClient` send/receive hooks (Phase 3)
- Cross-process / multi-host hub deployment
- JWT / mTLS / SignedChallenge auth — V1 ships `NoAuth` only (post Phase 4)
- Multi-hub federation; multi-identity per Agent; signed envelope chains (post Phase 4)
- Token / cost budget enforcement; archival sweeper; audit log rotation — V1 writes a single `audit.jsonl` indefinitely (Phase 4)
- Task cancellation, `Task.checkpoint` — saga primitives (Phase 2.0); custom expectation evaluators (Phase 2.1)
- Saga / compensation engine — composes from `Task.checkpoint` + `OnFailure` transitions; not a framework module. Circuit breakers (app-level concerns)
- `notification`, `broadcast`, `auction` adapters live in `examples/` as proofs of extensibility; `BySpeaker`, `PreviousOnly` views (Phase 4 on demand)
- `Composite` view policy (Phase 2.1)
- 3 expectation kinds — `turn_within`, `progress_within`, `min_participation` (Phase 2.0)
- 3 violation handlers — `warn`, `hide`, `remove` (Phase 2.0)
- N-of-M quorum tracking — V1 ships all-or-nothing accept; partial-quorum recomputation, `required_acks` integer, and `quorum_changed` events are Phase 2.0
- `drop_oldest` / `drop_newest` inbox overflow policies — V1 reject-only (Phase 2.1)
- Discussion `dynamic` and `static` ordering modes — V1 ships round_robin only (Phase 2.1)
- Streaming `chunk` frames at the wire layer — V1 is text-only envelopes (Phase 2.1)
- Rate limiter token bucket — V1 honors `delegation_depth` and concurrency caps but skips per-minute throttle (Phase 2.1)
- `allowed_events` field on `SessionManifest` — removed entirely (was never validated)

Everything in this list is a later-phase or post Phase 4 concern. Framework-core V1 always works without it.

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
- `_spawn_subtask` is **not** wrapped in a Task in V1 — `_run_task` already emits `TaskStarted/Progress/Completed/Failed` on the parent stream, which is the contract the network mirror observes. Wrapping (so `TaskInject` resolves inside subagent context) is a Phase 4 nice-to-have

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
- Handoff tools are scoped per-agent (registered onto `agent.tools`) rather than per-session. If an agent joins multiple workflows, the union of tools is fine: each tool emits on the *current* session, and non-matching adapters fall through to `default_target`. Per-session scoping is Phase 4 (on-demand) once we have a clean reason to need it.
- `WorkflowState.graph_data` stores the JSON-friendly `to_dict()` form. `fold` deserialises on each call (cheap; the graph is small and bounded). Caching the deserialised graph on the adapter would tie state to the adapter instance, which violates the stateless-adapter principle.
- `WorkflowGraph.sequence(steps)` sets `max_turns=len(steps)` so the pipeline terminates cleanly after the last step posts. The exit criterion's "triage closes via TerminateTarget" is exercised in tests via `hub.close_session(...)` (deterministic) rather than waiting on the LLM to call `sessions(action="close")`.

Exit: ✅ Validated by 26 in-tree integration tests in `test/beta/network/test_m4_workflow.py` (4 patterns + Hub.hydrate recovery + serialization round-trip + registry extension + handoff tool dispatch) and 1 anthropic smoke test in `test/beta/providers/anthropic/test_workflow_smoke.py` proving the full exit criterion against real `claude-haiku-4-5`: triage's LLM autonomously calls `transfer_to_eng` → eng's notify handler engages eng's LLM with the synthesised handoff prompt → eng's reply rotates control back to triage via `FromSpeaker(eng) → RevertToInitiatorTarget` → workflow state survives a mid-flow `Hub.hydrate()` → triage closes the session.

See [workflow.md](workflow.md) for the full design.

### Phase 2.0 — Durability and adoption ✅ shipped

The unmet need: long-running sessions and workflows that survive interruption and resume without starting over. AG2-classic's `GroupChat` can't do this natively; users have asked for it repeatedly. V1 ships durable WAL + deterministic fold, so the hub-side state already survives restart — the gap is the agent-side activation mechanism.

Phase 2.0 closes that gap with **primitives, not a system**. Each item is a method or a vocabulary entry; default handlers compose them into useful behavior; users override the handlers if they want different semantics. **No new protocol shape**: the WAL stays append-only, adapters stay stateless, fold stays pure.

| Commit | Theme | Tests added |
|---|---|---|
| `eb7251890cb` | Durability foundation | 13 |
| `d4fffb63070` | Expectations + violation handlers | 8 |
| `d66289ffcb8` | Task cancellation | 6 |
| `a8156de6373` | N-of-M quorum tracking | 5 |
| `e835e8e259f` | LLMSelectorTarget + classic Pattern migration | 12 |

Beta suite total: **1637 passing**, +44 from V1 baseline, zero regressions across the 5 implementation commits.

**Durability primitives:**
- `Hub.find_envelope_by_causation(session_id, *, sender_id, causation_id) -> Envelope | None` — idempotency query. Default handler checks before sending replies; redelivery doesn't produce duplicates. Index rebuilt by walking WAL on `hydrate()`.
- `Hub.pending_turns_for(agent_id) -> list[PendingTurn]` — wake-up query. Returns sessions where adapter state expects this agent to act but no reply has landed. Default handler calls on reconnect and re-runs the existing `_process_text` path against the triggering envelope. **Same code path as live notifies** — no resume-specific branch in user-visible code.
- `HubClient.attach(agent, name=...)` + `AgentClient.resume_pending_turns()` — reconnect to an existing identity by name and re-fire the registered handler against any unfinished triggers.
- `Task.checkpoint(state: dict)` — opt-in framework-core primitive. Persists JSON to `tasks/{id}/checkpoint.json` via the supplied `CheckpointStore`. `agent.task(resume_from=task_id)` reads it on construction. The owner chooses what to checkpoint and when; the framework provides storage.
- `HubBackedCheckpointStore` + `AgentClient.checkpoint_store` — hub-backed `CheckpointStore` adapter so network agents get durable task state for free.

`inbox.cursor` + Receipt wiring was scoped out of 2.0 — `pending_turns_for` is the in-process semantic primitive that solves the agent-restart case, and cursor-driven replay only earns its keep when transports can drop and replay (Phase 3 cross-process).

**Liveness expectations** (registered through the existing `register_expectation_evaluator` registry — no new infrastructure):
- 3 evaluators: `turn_within`, `progress_within`, `min_participation`
- 3 violation handlers: `warn`, `hide`, `remove`. `hide` is in-memory; `remove` persists to `sessions/{id}/removed.json` so the bar survives hub restart.

**Other primitives:**
- `TaskState.CANCELLED` + `Task.cancel(reason)` — owner-driven; emits `TaskCancelled`. `EV_TASK_CANCELLED` mirrors terminal state; `ag2.task.cancel_request` is the peer-side ask (owner free to honour or ignore). `tasks(action="cancel", task_id, reason)` LLM verb posts the request envelope.
- N-of-M quorum tracking — `required_acks: int | None`. `None` keeps V1 all-or-nothing semantics; positive integer activates as soon as N acks land. Rejects only fail with `quorum_unreachable` when the threshold becomes unreachable. `mark_removed` on an active session emits `ag2.session.quorum_changed(remaining, required)`.
- `LLMSelectorTarget(selector_id, candidates=[])` — workflow transition target that routes to a selector agent who then picks via tool-call handoff. Pure synchronous resolver — the selector's LLM deliberation happens during their normal turn; the framework only routes. `TransitionGraph.auto_pattern(selector_id, candidates, handoff_tools=...)` factory wires the full selector + candidate routing in one call.
- `from_classic_pattern(pattern, *, selector_id?, ...)` — translates AG2-classic `RoundRobinPattern` and `AutoPattern` into the equivalent `TransitionGraph`. Other classic patterns raise `UnsupportedPatternError` with a phase pointer (RandomPattern → Phase 4, ManualPattern → post Phase 4, DefaultPattern → Phase 2.1).

**Hygiene** (tools-not-systems): `_expectation_tick` promoted to public `Hub.evaluate_expectations()` so users running their own scheduler don't reach into privates. `Hub.get_rule(agent_id)` / `Hub.mark_hidden` / `Hub.mark_removed` exposed for the same reason.

**Cross-paradigm parity suite (`multiagent_orchestration/`, ~1530 LOC across 5 test files):** off-by-default Gemini-driven test harness that pairs each AG2-classic pattern with its `TransitionGraph` recipe and asserts behavioural equivalence. Covers `RoundRobinPattern`, `AutoPattern` (auto-manager), sequential pipelines, swarm handoffs, and custom handoff routing. Lives outside `test/beta/network/` because it hits real models; serves as the load-bearing acceptance evidence that `from_classic_pattern` migration is real, not just a translator unit-test.

### Phase 2.1 — Sugar

Useful but not load-bearing. Each ships when there's user demand.
- `Composite` view policy
- Discussion `dynamic` and `static` ordering modes
- `ContextExpr`, `TurnCountReached` workflow conditions
- Custom expectation evaluators (user-registered Python callables)
- `drop_oldest` / `drop_newest` inbox overflow policies
- `OnFailure` transitions — saga choreography composed from existing `Transition` vocabulary
- Causation-index pruning on session close — `Hub._index_causation` grows for the lifetime of a session; bound it once long-lived sessions become real (audit surfaced this; not blocking 2.0 adoption since current sessions are bounded)
- `DefaultPattern` migration target — `from_classic_pattern` already raises `UnsupportedPatternError("Phase 2.1")` with guidance; concrete translation lands here

(Streaming chunk frames and the rate-limiter token bucket originally lived here; pulled forward to Phase 3 because they pair structurally with going on the wire — see below.)

### Phase 3 — Cross-process + production hardening

Tightened: durability primitives ship in 2.0 over `LocalLink`, so Phase 3 is the wire-level work to make them work across hosts. **Two items pulled forward from 2.1** because they don't earn their keep until you cross a network boundary — streaming chunks (LLM UX is broken without progressive output over a wire) and the rate-limiter token bucket (you want a throttle on `ApiKeyAuth` before the first abuse report, not after). The remainder of 2.1 stays demand-driven.

Phase 3 lands as **three sequential milestones** under the same additive-merge rule as Phase 1: every milestone is independently mergeable; nothing rewrites earlier work.

| Milestone | Status | Theme | Tests |
|---|---|---|---|
| M1 — Streaming + safety + hooks | ✅ shipped (uncommitted) | additive surface, no transport changes | 18 (6 hooks + 6 rate-limit + 6 streaming) |
| M2 — Wire transport + auth | ✅ shipped (uncommitted) | new transport plane — `WsLink`, HTTP CRUD, `ApiKeyAuth` | 16 (6 ApiKeyAuth + 4 WsLink + 6 HTTP) |
| M3 — Cross-process semantics | ✅ shipped (uncommitted) | exercises 2.0 durability on the wire | 20 (6 cursor-replay + 7 network-changed + 7 dispatch-audience) |

After M1+M2+M3: beta suite **1691 passing, zero regressions** (231 in `test/beta/network/`), +54 from Phase 2.0 baseline.

Item-level detail:
- ✅ **Streaming `chunk` frames + `Session.send_chunk` / `Session.iter_chunks`** (M1) — `ChunkFrame` is a new wire frame; chunks are ephemeral (no WAL append) and reference a parent envelope id. Hub fans out per-recipient using the same audience/access path as `NotifyFrame`. Sender-monotonic sequence numbers per parent envelope. `ChunkSubscription` on the client-side demuxes by `(session_id, parent_envelope_id)` so concurrent streams to the same agent stay isolated.
- ✅ **Rate limiter token bucket** (M1) — per-sender token bucket in `hub/rate_limiter.py`. Wired into `Hub.post_envelope` between the delegation-depth check and the WAL append; substantive events only (protocol envelopes bypass so the session machine never deadlocks under throttle). `LimitsBlock.rate` activates it; `per_minute=0` (V1 default) skips entirely. `set_rule` invalidates the cached bucket; `unregister` drops it. Hub takes an optional `monotonic_clock` constructor arg for deterministic testing.
- ✅ `AgentClient.add_send_hook(callable)` / `add_receive_hook(callable)` (M1) — two hook points for tenant-side per-envelope logic. Replaces the prior 4-stage `TransformPipeline` design; the stdlib of named transforms (`redact_pii`, `truncate_long_content`, `stamp_audit_header`) lives in `examples/`, not framework-core. Hooks return `Envelope` to continue or `None` to drop; first `None` short-circuits.
- ✅ `WsLink` (WebSocket transport) (M2) — same `Link` Protocol surface as `LocalLink`; JSON-encoded frames over `websockets.asyncio`. New module `transport/ws.py` with `WsLink`/`WsLinkClient`/`WsLinkEndpoint` + `serve_ws(hub)` async-context-manager server. `HubClient._ensure_connected` is now async so wire transports can await connect; `LocalLinkClient.open()` stays a no-op so in-process callers see no behaviour change.
- ✅ HTTP CRUD surface (10 endpoints) via Starlette (M2) — `make_http_app(hub)` returns an ASGI app. Routes: register / list_agents / get_agent / unregister / create_session / list_sessions / get_session / close_session / post_envelope / read_wal. Pure-ASGI auth middleware (not `BaseHTTPMiddleware` — that breaks under `httpx.ASGITransport`). Auth uses the passport's declared `AuthBlock.scheme` so a mixed `NoAuth + ApiKeyAuth` registry isn't a backdoor for ApiKeyAuth-tagged identities.
- ✅ `ApiKeyAuth` adapter (M2) — `AuthAdapter` impl in `auth.py`. Static `keys: Mapping[str, str]` or dynamic `resolver: Callable`. Constant-time compare via `hmac.compare_digest`. Fails closed on unknown identity.
- **Cut 3.1 — Receipt + cursor + Hello replay** (M3) — `ReceiptFrame` already exists in the vocabulary; this cut wires it end-to-end. Hub handles `ack`/`nack` in `_dispatch_frame`, persists a per-agent `inbox.cursor` write-through (one tiny JSON file per receipt — fsync pressure is negligible at chat-style envelope rates; revisit only if a real workload shows otherwise). On `HelloFrame` reconnect over a wire transport, hub replays unacked notifies past the cursor as fresh `NotifyFrame`s; `find_envelope_by_causation` makes redelivery idempotent. Default handler emits a `ReceiptFrame(status="ack")` after `_process_text` returns (or after the dedup short-circuit). Receipts are wired everywhere for code-path uniformity, but `LocalLink` has no reconnect event so replay is exercised on `WsLink` only. **Out of scope:** `SubscribeFrame.since_envelope_id` replay — the subscribe/event surface isn't a load-bearing client concern yet, and Hello-driven replay covers the resume story. Re-open when subscribe semantics actually grow.
- **Cut 3.2 — `NetworkChangedFrame` + `HubClient` peer cache** (M3) — peer/capability lookups become real round-trips over the wire. New `NetworkChangedFrame(kind="agent_registered" | "agent_unregistered" | "resume_set" | "skill_set")` carrying just the affected `agent_id`. Hub broadcasts to all attached endpoints on identity mutation. `HubClient` gains a small TTL'd cache around `list_agents` / `get_resume` / `get_skill` and invalidates on inbound `NetworkChangedFrame`. `NetworkContextPolicy` is **not** touched — today it renders a static `"You are <name>"` prefix and does not depend on peer state. If we later grow the policy to inject a peer list into the prefix, the cache it consumes is the one already on `HubClient`.
- **Cut 3.3 — `dispatch_audience` adapter hook** (M3) — optional `SessionAdapter.dispatch_audience(envelope, metadata, state) -> list[str] | None` returning a narrowed audience (`None` keeps the current default — `envelope.audience` or all non-sender). `Hub._dispatch` consults it before the access-rule loop. `WorkflowAdapter` overrides to return `[expected_next_speaker]` for `EV_TEXT` / `EV_HANDOFF` so workflows don't broadcast a turn that only one peer needs to see. Other adapters (`consulting`, `conversation`, `discussion`) keep the default — broadcast semantics are correct for them.

### Phase 4 — On-demand

Ships when a real user asks. The Protocol design accommodates each without architectural change.

- `SubGraph` workflow target (workflow composition)
- `BySpeaker`, `PreviousOnly` view policies
- `inbox_pressure` backpressure events
- Audit log daily rotation + retention policy
- Workflow extras: `RandomTarget`, `NestedSessionTarget` (SocietyOfMind)
- Adapter state cache perf-regression benchmark suite (CI hygiene)
- Smoke tests against real LLM providers
- User-facing docs site, examples / playground

### Lives in `examples/`, never framework-core

These exist to prove the Protocol is genuinely extensible. The maintenance cost belongs with the example, not the framework.
- `notification`, `broadcast`, `auction` adapters
- Saga skeleton (composes `Task.checkpoint` + `OnFailure` transitions)
- Stdlib transform examples (`redact_pii`, `truncate_long_content`, `stamp_audit_header`) — referenced by the `add_send_hook` / `add_receive_hook` cookbook

### Cut

- Auto-shipping pickled transition classes — security smell. Cross-process answer is "both ends deploy the same Python."

### Post Phase 4

Anything beyond the framework-core surface is out of scope for this OSS framework. That includes multi-hub federation, multi-tenancy, managed transform pipelines, archival to external object storage, alternative `KnowledgeStore` backends (Sqlite / Redis / S3 / FoundationDB) for cross-process coordination, JWT / mTLS / signed-challenge auth schemes, HTTP endpoints beyond the basic 10, cross-agent knowledge bridge, and hot-reload rule push beyond the basic `set_rule`.

These belong to a managed deployment layer that builds on framework-core; framework-core stays a tool, not a system.

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
