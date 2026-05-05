# AG2 Network — Phase 1 PR Split

This document describes how the `network-design` branch will be published to `main` as four stacked pull requests. It is the reference for branch creation, PR descriptions, and reviewer guidance.

Source of truth for the design itself is [PLAN.md](PLAN.md). This file only covers **how Phase 1 ships**.

## Strategy

- **Four sequential PRs**, each stacked on the previous (`PR1 ← PR2 ← PR3 ← PR4`). Independence between PRs is **not** maintained — PR2 branches off PR1, PR3 off PR2, etc.
- One PR per PLAN milestone: PR1 carries the framework-core `Task` precondition + M1 foundation. PR2/PR3/PR4 map 1:1 to M2/M3/M4.
- **Each file ships once**, in the PR for the milestone that **first introduced** it, at branch-HEAD state (with all audit fixes folded in). Files that grew across milestones (notably `hub/core.py`, `client/agent_client.py`, `client/plugin.py`, `views/builtin.py`, `task_mirror.py`) ship in their introducing PR with M2/M3/M4 hooks already present as inert code paths until later PRs land their callers/tests. This trades a small amount of "why is this here?" reviewer overhead for zero file churn between PRs.
- **`design/`** is excluded from every PR. PLAN.md and the per-area design docs land separately (or stay branch-local) — they are internal references, not part of the V1 contract under review.

## PR table

| PR | Title | Branches off | Source LOC* | Test LOC* | Total |
|----|-------|--------------|------------:|----------:|------:|
| PR1 | `feat(beta): Task primitive + network foundation (M1)` | `origin/main` | ~3,400 | ~510 | ~3.9K |
| PR2 | `feat(beta/network): consulting loop (M2)` | PR1 | ~1,800 | ~325 | ~2.1K |
| PR3 | `feat(beta/network): multi-party + observability (M3)` | PR2 | ~2,800 | ~2,815 | ~5.6K |
| PR4 | `feat(beta/network): workflow orchestration (M4)` | PR3 | ~900 | ~2,650 | ~3.5K |

\* Approximate. Source LOC is the file-state ships in that PR; test LOC includes that PR's added test files and audit-regression tests where applicable.

## PR1 — Task primitive + network foundation (M1)

**Goal:** ship the framework-core `Task` lifecycle primitive plus the network foundation (identity, transport, hub bare bones). No LLM yet — tested with raw envelopes through `LocalLink`.

**Branch:** `feat/network-pr1-foundation` off `origin/main`.

**Files (all at branch-HEAD state):**

Framework-core precondition:
- `autogen/beta/__init__.py` (Task surface additions)
- `autogen/beta/agent.py` (`agent.task(...)` + `add_policy()` public surface)
- `autogen/beta/events/__init__.py` (TaskExpired export)
- `autogen/beta/events/task_events.py` (TaskExpired, widened `result`, optional `spec`/`payload`)
- `autogen/beta/task.py` (NEW)

Network foundation:
- `autogen/beta/network/__init__.py`
- `autogen/beta/network/{ids,errors,policies,identity,auth,envelope,rule}.py`
- `autogen/beta/network/transport/{__init__,frames,link,local}.py`
- `autogen/beta/network/hub/{__init__,core,layout}.py`
- `autogen/beta/network/client/{__init__,network_client,hub_client,agent_client}.py`

Tests:
- `test/beta/test_task.py`
- `test/beta/network/__init__.py`
- `test/beta/network/test_foundation.py`

**Exit criteria:** two `AgentClient`s register through `LocalLink` and exchange raw envelopes. `Hub.hydrate()` rebuilds passport/resume/rule caches from disk. Validated by `test_foundation.py` (5 tests) and `test_task.py` (22 tests).

**Reviewer notes for PR1:**
- `hub/core.py` is a large file (~1700 LOC) that includes routing hooks for adapters and observability features that are inert until PR2/PR3 ship the calling code. M1 tests exercise registration, raw envelope dispatch, and hydrate; the rest is structural scaffolding.
- `client/{hub_client,agent_client}.py` likewise carry session/task client surfaces unused by M1 tests.

**Validation command:**
```
.venv-beta/bin/pytest test/beta/test_task.py test/beta/network/test_foundation.py -v
```

## PR2 — Consulting loop (M2)

**Goal:** first end-to-end LLM-driven session. One agent delegates a task to another via `delegate(target, prompt, blocking=True)`; the recipient's notify handler engages its LLM; the consulting auto-closes when the reply lands.

**Branch:** `feat/network-pr2-consulting` off PR1.

**Files (all new in this PR, at branch-HEAD state):**

Network adapters & views:
- `autogen/beta/network/adapters/{__init__,base,consulting}.py`
- `autogen/beta/network/views/{__init__,base,builtin}.py` (`FullTranscript` + `WindowedSummary` head)
- `autogen/beta/network/session.py`
- `autogen/beta/network/task_mirror.py`
- `autogen/beta/network/hub/sweepers.py`

Client surface:
- `autogen/beta/network/client/{handlers,plugin,session,task,inject}.py`
- `autogen/beta/network/client/tools/{__init__,say,delegate}.py`

Tests:
- `test/beta/network/test_consulting.py`

**Exit criteria:** Alice's LLM calls `delegate(target="bob", prompt="...", blocking=True)`. Bob's notify handler runs Bob's LLM. Bob replies. Consulting auto-closes via the adapter's `on_accepted`. Validated by `test_consulting.py` (8 tests).

**Reviewer notes for PR2:**
- `views/builtin.py` ships `WindowedSummary` (added in M3) at HEAD state alongside `FullTranscript`. M2 tests only exercise `FullTranscript`; M3 tests will exercise `WindowedSummary`.
- `client/handlers.py` includes M3-era observation wiring (record_observation hook) and M4-era handoff handling that won't fire under M2 tests.
- `client/plugin.py` carries the workflow registration surface (`register_workflow`) inert until M4.

**Validation command:**
```
.venv-beta/bin/pytest test/beta/network/test_consulting.py -v
```

## PR3 — Multi-party + observability (M3)

**Goal:** full V1 surface for multi-party sessions, expectations, audit log, capability observation, and the 4 grouped LLM tools. Largest PR by LOC; the test count reflects six independent feature areas (3.1–3.5 cuts in PLAN.md).

**Branch:** `feat/network-pr3-multiparty` off PR2.

**Files (all new in this PR, at branch-HEAD state):**

Adapters:
- `autogen/beta/network/adapters/{conversation,discussion}.py`

Hub:
- `autogen/beta/network/hub/{audit,expectations}.py`

Client:
- `autogen/beta/network/client/skill_render.py`
- `autogen/beta/network/client/tools/{peers,sessions,tasks,context}.py`

Tests:
- `test/beta/network/_helpers.py` (`_ScriptedConfig` for multi-turn LLM tests)
- `test/beta/network/test_conversation.py`
- `test/beta/network/test_discussion.py`
- `test/beta/network/test_expectations.py`
- `test/beta/network/test_hydrate_scale.py`
- `test/beta/network/test_observation.py`
- `test/beta/network/test_tools.py`
- `test/beta/providers/anthropic/test_network_smoke.py`

**Exit criteria:** five LLMs round-robin through a discussion via `say`; alice's LLM autonomously calls `peers(action="find", capability="math")` → `delegate(target="bob", ...)` → returns `"204"` for `"12 × 17"` (anthropic smoke). Per-(session, expectation, violator) dedup, audit log, capability observation, and skill rendering all exercised by the listed test files. Validated by 62 in-tree tests + 2 anthropic smoke tests.

**Reviewer notes for PR3:**
- This is the heaviest PR. Reviewing by sub-area is encouraged: adapters → hub/expectations → hub/audit → client tools → tests.
- `_helpers.py::_ScriptedConfig` exists because `autogen.beta.testing.TestConfig` resets its iterator on every `create()` call; multi-turn LLM-driven adapter tests need a persistent script cursor.
- Hydrate scale test runs at 100 sessions × 100 envelopes (10K total). Bumping `ENVELOPES_PER_SESSION` runs the larger 100K sweep locally.

**Validation command:**
```
.venv-beta/bin/pytest test/beta/network/test_conversation.py test/beta/network/test_discussion.py test/beta/network/test_expectations.py test/beta/network/test_hydrate_scale.py test/beta/network/test_observation.py test/beta/network/test_tools.py -v
.venv-beta/bin/pytest -m anthropic test/beta/providers/anthropic/test_network_smoke.py -v   # ~$0.005 against haiku
```

## PR4 — Workflow orchestration (M4)

**Goal:** the orchestrator surface — successor for AG2-classic's `GroupChat` + `Handoffs` + `AfterWork`. Strictly additive on top of M3.

**Branch:** `feat/network-pr4-workflow` off PR3.

**Files (all new in this PR, at branch-HEAD state):**

Network:
- `autogen/beta/network/transitions.py`
- `autogen/beta/network/adapters/workflow.py`
- `autogen/beta/network/client/tools/handoff.py`

Tests:
- `test/beta/network/test_workflow.py`
- `test/beta/network/test_audit_and_lifecycle.py` (audit log + lifecycle invariants)
- `test/beta/network/test_sweeper_and_registry.py` (background sweeper + registry isolation + cross-tool flow)
- `test/beta/network/test_hub_invariants.py` (registration / concurrency / dispatch / projection invariants)
- `test/beta/providers/anthropic/test_workflow_smoke.py`

**Exit criteria:** triage's LLM calls `transfer_to_eng` → eng's notify handler engages eng's LLM with the synthesised handoff prompt → eng's reply rotates control back to triage via `FromSpeaker(eng) → RevertToInitiatorTarget` → workflow state survives a mid-flow `Hub.hydrate()` → triage closes the session. Validated by `test_workflow.py` (26 tests) + `test_workflow_smoke.py` (1 anthropic test). Plus the audit / sweeper / hub-invariant regression files (~1.7K LOC) lock in invariants for the entire V1 surface.

**Reviewer notes for PR4:**
- `EV_HANDOFF` (`ag2.handoff`) was added to `envelope.py` and `client/handlers.py` in PR1/PR2's at-HEAD versions; PR4 adds the *callers* (transitions + workflow adapter + handoff tool).
- `client/plugin.py::register_workflow(graph)` is the user-facing convenience that materialises one tool per `ToolCalled` transition. The plugin code shipped in PR2; PR4 activates it.
- `test_audit_and_lifecycle.py`, `test_sweeper_and_registry.py`, and `test_hub_invariants.py` are post-hoc regression suites covering the whole V1 surface, not the workflow code specifically. They land here because PR4 is the last in the stack.

**Validation command:**
```
.venv-beta/bin/pytest test/beta/network/test_workflow.py test/beta/network/test_audit_and_lifecycle.py test/beta/network/test_sweeper_and_registry.py test/beta/network/test_hub_invariants.py -v
.venv-beta/bin/pytest -m anthropic test/beta/providers/anthropic/test_workflow_smoke.py -v   # ~$0.005 against haiku
```

## Operational steps

After the AGENTS.md compliance pass commit lands on `network-design`:

```bash
# PR1
git checkout -b feat/network-pr1-foundation origin/main
git checkout network-design -- <PR1 file list>
git commit -m "feat(beta): Task primitive + network foundation (M1)"
git push -u origin feat/network-pr1-foundation
gh pr create --base main --title "feat(beta): Task primitive + network foundation (M1)" --body-file design/pr_bodies/pr1.md

# PR2
git checkout -b feat/network-pr2-consulting feat/network-pr1-foundation
git checkout network-design -- <PR2 file list>
git commit -m "feat(beta/network): consulting loop (M2)"
git push -u origin feat/network-pr2-consulting
gh pr create --base feat/network-pr1-foundation --title "feat(beta/network): consulting loop (M2)" --body-file design/pr_bodies/pr2.md

# Repeat for PR3, PR4. Each subsequent PR's --base is the previous PR's branch.
```

PR descriptions (`design/pr_bodies/pr{1,2,3,4}.md`) will be authored once each PR is ready. They follow this template:

```
## Summary
<2-3 bullet points: what milestone, what features, what tests>

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
    └── PR1 (foundation)
        └── PR2 (consulting)
            └── PR3 (multi-party + observability)
                └── PR4 (workflow + audit-regression tests)
```

When merging:
1. PR1 lands → rebase PR2 onto `main`, fast-forward.
2. PR2 lands → rebase PR3 onto `main`, fast-forward.
3. PR3 lands → rebase PR4 onto `main`, fast-forward.

GitHub UI handles the rebase if each PR is mergeable into the next. Squash-on-merge keeps `main` history at 4 commits.

## Pre-publish: AGENTS.md compliance pass

Before publishing PR1, a single commit on `network-design` brings the branch into AGENTS.md/CLAUDE.md compliance. Findings (full audit summary in conversation history):

| Fix | Files |
|-----|-------|
| Replace global mutable `_default_registry` + `global` keyword | `network/transitions.py` |
| Replace top-level `default_registry = AuthRegistry([NoAuth()])` constructor with classmethod or lazy init | `network/auth.py` |
| Move function-level `from ..envelope import EV_TEXT` to module top | `network/hub/core.py` |
| Add public `Agent.add_policy()` to remove private-attr access | `autogen/beta/agent.py`, `network/client/plugin.py` |
| Complete `__all__` re-exports per submodule contracts | `network/__init__.py`, `network/hub/__init__.py` |
| Remove unused `@runtime_checkable` from Protocols (none use isinstance) | `network/auth.py`, `network/transport/link.py`, `network/views/base.py`, `network/adapters/base.py`, `network/transitions.py`, `network/client/network_client.py`, `network/hub/expectations.py` |

Commit title: `fix(beta/network): pre-publish AGENTS.md compliance pass`.

These fixes ship at HEAD state of each affected file, so they land naturally in whichever PR ships that file.
