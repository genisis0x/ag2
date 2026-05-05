# AG2 Network — Phase 1 PR Split

This document describes how the `network-design` branch will be published to `main` as three stacked pull requests. It is the reference for branch creation, PR descriptions, and reviewer guidance.

Source of truth for the design itself is [PLAN.md](PLAN.md). This file only covers **how Phase 1 ships**.

## Strategy

- **Three sequential PRs**, each stacked on the previous (`PR1 ← PR2 ← PR3`). Independence between PRs is **not** maintained — PR2 branches off PR1, PR3 off PR2.
- The split follows architectural layers, not development milestones:
  - PR1 — the framework-core `Task` primitive (no network)
  - PR2 — the network protocol + state + tenant-side control plane (no LLM-facing tool surface)
  - PR3 — the LLM-facing tool surface + workflow orchestration + integration tests that drive agents through tools
- **Cross-PR file modifications are minimised but unavoidable**: three files are slim in PR2 and modified in PR3 (`network/__init__.py`, `client/__init__.py`, `client/hub_client.py`). All other source files ship at branch-HEAD state in their introducing PR.
- **`design/`** is excluded from every PR. PLAN.md and per-area design docs are internal references, not part of the V1 contract under review.

## PR table

| PR | Title topic | Branches off | Source LOC* | Test LOC* |
|----|-------------|--------------|------------:|----------:|
| PR1 | Task primitive (framework-core) | `origin/main` | ~700 | ~320 |
| PR2 | Network protocol + state + control plane | PR1 | ~7K | ~3.5K |
| PR3 | Session participation tools + workflow | PR2 | ~1.1K | ~2.5K |

\* Approximate. PR2 ships the bulk of the source and adapter/expectation/observation tests. PR3 is mostly the LLM tool surface + tests that drive agents through tools.

## PR1 — `Task` primitive (framework-core)

**Goal:** ship the framework-core lifecycle primitive any `Agent` can wrap work in. No network — bare `Agent` continues to work standalone with no behavioural change when `autogen.beta.network` is not imported.

**Branch:** `feat/network-pr1-task` off `origin/main`.

**Files:**

- `autogen/beta/__init__.py` (Task surface additions)
- `autogen/beta/agent.py` (`agent.task(...)` + public `add_policy()`)
- `autogen/beta/events/__init__.py` (export `TaskExpired`)
- `autogen/beta/events/task_events.py` (`TaskExpired`, widened `TaskCompleted.result` to `Any`, optional `spec`/`payload`)
- `autogen/beta/task.py` (NEW — `Task`, `TaskSpec`, `TaskState`, `TaskMetadata`, `TaskInject`)
- `test/beta/test_task.py`

**Exit criteria:** `agent.task(...)` lifecycle emits `TaskStarted` / `TaskProgress` / `TaskCompleted` / `TaskFailed` / `TaskExpired` events on a bound stream; `TaskInject` resolves to the active task inside the `async with` block. Validated by `test_task.py` (22 tests).

**Validation command:**
```
.venv-beta/bin/pytest test/beta/test_task.py -v
```

## PR2 — Network protocol + state + control plane

**Goal:** ship the entire network module **except** the LLM-facing tool surface. After this PR, agents can register through a hub, exchange envelopes inside protocol-defined sessions (consulting / conversation / discussion / workflow), participate in turn-taking via the default notify handler, and observe each other's tasks — all by writing tenant code that calls `Session.send()` and similar methods directly. The 6 LLM-facing tools (`say`, `delegate`, `peers`, `sessions`, `tasks`, `context`) and `NetworkPlugin` arrive in PR3.

**Branch:** `feat/network-pr2-protocol` off PR1.

**Files (all new in this PR):**

Network primitives and protocol:
- `autogen/beta/network/__init__.py` (slim — re-exports the PR2 surface only)
- `autogen/beta/network/{ids, errors, policies, identity, auth, envelope, rule, session, transitions, task_mirror}.py`
- `autogen/beta/network/transport/{__init__, frames, link, local}.py`
- `autogen/beta/network/views/{__init__, base, builtin}.py`
- `autogen/beta/network/adapters/{__init__, base, consulting, conversation, discussion, workflow}.py`

Hub:
- `autogen/beta/network/hub/{__init__, audit, core, expectations, layout, sweepers}.py`

Client (control plane + session participation, no LLM tools):
- `autogen/beta/network/client/__init__.py` (slim — no plugin/tools re-exports)
- `autogen/beta/network/client/{network_client, agent_client, session, task, inject, handlers, skill_render}.py`
- `autogen/beta/network/client/hub_client.py` (slim — no `.plugin` import; no plugin attachment in `register()`)

Tests:
- `test/beta/network/__init__.py`
- `test/beta/network/_helpers.py`
- `test/beta/network/test_foundation.py` (raw envelope round-trip + identity hydrate)
- `test/beta/network/test_audit_and_lifecycle.py` (audit log + lifecycle invariants)
- `test/beta/network/test_consulting.py` (consulting adapter end-to-end via `TestConfig`-mocked `Agent.ask`)
- `test/beta/network/test_conversation.py`
- `test/beta/network/test_discussion.py`
- `test/beta/network/test_expectations.py` (evaluator unit + handler integration via manual sweeper tick)
- `test/beta/network/test_observation.py` (skill render + capability index + `TaskMirror` end-to-end)
- `test/beta/network/test_hydrate_scale.py`

**Exit criteria:** two `AgentClient`s register through `LocalLink` and exchange raw envelopes; multi-party adapters run end-to-end driven by `TestConfig`-mocked LLM responses; expectation evaluators fire correctly; the audit log records the documented kinds; `Hub.hydrate()` correctness at 100×100 scale.

**Reviewer notes for PR2:**
- This is the largest PR. Reviewing by sub-area is encouraged: data types → adapters/views → hub → client.
- `client/hub_client.py` is shipped as a slim version: it does not import `.plugin` and does not attach a `NetworkPlugin` in `register()`. PR3 adds five lines to enable plugin attachment.
- `network/__init__.py` and `client/__init__.py` ship as slim versions: they re-export only what PR2 contributes. PR3 grows both with the LLM-tool surface re-exports.
- Consulting / conversation / discussion tests use `TestConfig` and `_ScriptedConfig` to mock LLM responses — they exercise the default notify handler's LLM-driven response path **without** the LLM-facing tool surface.

**Validation command:**
```
.venv-beta/bin/pytest test/beta/network/test_foundation.py test/beta/network/test_audit_and_lifecycle.py test/beta/network/test_consulting.py test/beta/network/test_conversation.py test/beta/network/test_discussion.py test/beta/network/test_expectations.py test/beta/network/test_observation.py test/beta/network/test_hydrate_scale.py -v
```

## PR3 — LLM tool surface + workflow

**Goal:** ship the LLM-facing presentation layer of the network. `NetworkPlugin` attaches six grouped tools (`say` / `delegate` / `peers` / `sessions` / `tasks` / `context`) to an `Agent` so the LLM can drive its own session participation. Also ships workflow handoff tools so `WorkflowAdapter`'s `ToolCalled` transitions can be triggered by the LLM. After this PR, agents can autonomously discover, delegate to, and orchestrate each other.

**Branch:** `feat/network-pr3-tools` off PR2.

**Files:**

New source files:
- `autogen/beta/network/client/plugin.py` (`NetworkPlugin`, `NetworkContextPolicy`, `register_workflow`)
- `autogen/beta/network/client/tools/__init__.py`
- `autogen/beta/network/client/tools/{say, delegate, peers, sessions, tasks, context, handoff}.py`

Modified files (re-exports + plugin attachment):
- `autogen/beta/network/__init__.py` (add `NetworkContextPolicy`, `NetworkPlugin` to re-exports)
- `autogen/beta/network/client/__init__.py` (add plugin + tool factories to re-exports)
- `autogen/beta/network/client/hub_client.py` (add `from .plugin import NetworkPlugin`; attach `NetworkPlugin` in `register()` when `attach_plugin=True`)

Tests:
- `test/beta/network/test_hub_invariants.py` (registration / concurrency / dispatch / projection invariants — uses `make_delegate_tool` for race + fast-fail tests)
- `test/beta/network/test_tools.py` (per-action coverage of all 6 grouped tools)
- `test/beta/network/test_sweeper_and_registry.py` (background sweeper + registry isolation + cross-tool flow)
- `test/beta/network/test_workflow.py` (transition vocabulary unit + `WorkflowAdapter` integration + `register_workflow` handoff tools)
- `test/beta/providers/anthropic/test_network_smoke.py`
- `test/beta/providers/anthropic/test_workflow_smoke.py`

**Exit criteria:** Alice's LLM autonomously calls `peers(action="find", capability="math")` → `delegate(target="bob", ...)` → returns `"204"` for `"12 × 17"` (Anthropic smoke). Triage's LLM calls `transfer_to_eng` → eng's notify handler engages eng's LLM with the synthesised handoff prompt → eng's reply rotates control back to triage via `FromSpeaker(eng) → RevertToInitiatorTarget` → workflow state survives a mid-flow `Hub.hydrate()` → triage closes the session (Anthropic workflow smoke).

**Reviewer notes for PR3:**
- The three modified files (`network/__init__.py`, `client/__init__.py`, `client/hub_client.py`) get small additive diffs that activate the surface PR2 already left a slot for.
- `test_hub_invariants.py` ships here (not in PR2) because three of its tests use `make_delegate_tool` directly to exercise inbox-race and fast-fail scenarios — those depend on the tool's behaviour, not just on hub mechanics.
- `_helpers.py` ships in PR2; this PR's tests reuse the same `_ScriptedConfig` helper.

**Validation command:**
```
.venv-beta/bin/pytest test/beta/network/test_hub_invariants.py test/beta/network/test_tools.py test/beta/network/test_sweeper_and_registry.py test/beta/network/test_workflow.py -v
.venv-beta/bin/pytest -m anthropic test/beta/providers/anthropic/test_network_smoke.py test/beta/providers/anthropic/test_workflow_smoke.py -v   # ~$0.01 against haiku
```

## Operational steps

After the cleanup commit (`fdf1179093`) is in place on `network-design`:

```bash
# PR1
git checkout --no-track -b feat/network-pr1-task origin/main
git checkout network-design -- <PR1 file list>
git commit -m "feat(beta): add Task lifecycle primitive"
git push -u origin feat/network-pr1-task
gh pr create --base main --title "feat(beta): add Task lifecycle primitive" --body-file design/pr_bodies/pr1.md

# PR2 (after PR1 lands or stacked on the branch)
git checkout --no-track -b feat/network-pr2-protocol feat/network-pr1-task
git checkout network-design -- <PR2 file list>
# Apply slim versions of network/__init__.py, client/__init__.py, client/hub_client.py
git commit -m "feat(beta/network): protocol, state, and control plane"
git push -u origin feat/network-pr2-protocol
gh pr create --base feat/network-pr1-task --title "feat(beta/network): protocol, state, and control plane" --body-file design/pr_bodies/pr2.md

# PR3
git checkout --no-track -b feat/network-pr3-tools feat/network-pr2-protocol
git checkout network-design -- <PR3 file list>
# Apply HEAD versions of the three slim files
git commit -m "feat(beta/network): LLM tool surface and workflow"
git push -u origin feat/network-pr3-tools
gh pr create --base feat/network-pr2-protocol --title "feat(beta/network): LLM tool surface and workflow" --body-file design/pr_bodies/pr3.md
```

PR descriptions (`design/pr_bodies/pr{1,2,3}.md`) will be authored once each PR is ready. They follow this template:

```
## Summary
<2-3 bullet points: what concern, what surface area, what tests>

## Test plan
- [ ] `.venv-beta/bin/pytest <PR-specific test paths>`
- [ ] Anthropic smoke (if applicable): cost <$0.01

## Reviewer notes
<copy "Reviewer notes" from this doc's PR section>

## Stacks on
<link to prior PR>
```

## Dependency graph reference

```
origin/main
    └── PR1 (Task primitive)
        └── PR2 (network protocol + state + control plane)
            └── PR3 (LLM tool surface + workflow)
```

When merging:
1. PR1 lands → rebase PR2 onto `main`, fast-forward.
2. PR2 lands → rebase PR3 onto `main`, fast-forward.

GitHub UI handles the rebase if each PR is mergeable into the next. Squash-on-merge keeps `main` history at 3 commits.

## Slim files in PR2

PR2 ships three files in slim form because PR3 adds the LLM tool surface that completes them. The diffs PR3 applies to these files are documented here so reviewers can see the upgrade path in advance.

### `autogen/beta/network/__init__.py`

PR2 ships this file with the re-export block restricted to the symbols defined in PR2. PR3 adds re-exports for `NetworkContextPolicy`, `NetworkPlugin`, and the seven tool factory functions (`make_say_tool` / `make_delegate_tool` / `make_peers_tool` / `make_sessions_tool` / `make_tasks_tool` / `make_context_tool` / `make_handoff_tool` / `make_handoff_tools_for_graph`).

### `autogen/beta/network/client/__init__.py`

PR2 ships this file with re-exports for `AgentClient`, `HubClient`, `NetworkClient`, `Session`, `ClientTask`, `default_handler`, dependency-injection annotations, and skill-render helpers. PR3 adds `NetworkPlugin`, `NetworkContextPolicy`, and the tool factories.

### `autogen/beta/network/client/hub_client.py`

PR2 ships this file without the `from .plugin import NetworkPlugin` import and without the `attach_plugin` parameter on `register()`. PR3 adds the import and the five-line block in `register()` that constructs a `NetworkPlugin` and attaches it to the agent.
