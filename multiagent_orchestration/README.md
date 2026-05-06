# multiagent_orchestration/

Side-by-side parity tests comparing **AG2-classic** orchestration patterns
(`autogen.agentchat.group.patterns`) with **AG2 V2 / beta network**
`WorkflowAdapter` + `TransitionGraph`.

These tests exercise real Gemini calls (not mocked) to make sure every
pattern that classic supports today can be expressed cleanly in the new
workflow surface. They are **not part of the release test suite** —
they live outside `test/` (which is `pyproject.toml`'s `testpaths`) and
must be invoked explicitly:

```bash
pytest multiagent_orchestration/ -m gemini -v -s
```

`-s` is recommended so the per-test transcripts print live.

## Models

The Gemini key in `.env` only has access to Gemini 3.x previews; tests
default to `gemini-3-flash-preview` (fast, cheap). Override with
`AG2_GEMINI_MODEL=gemini-3.1-pro-preview` for the stronger model.

## Patterns covered

| # | Classic                          | Workflow                                        | Test file              |
|---|----------------------------------|-------------------------------------------------|------------------------|
| 1 | `RoundRobinPattern`              | `TransitionGraph.round_robin([...])`            | `test_round_robin.py`  |
| 2 | sequential handoffs (`set_after_work` chain) | `TransitionGraph.sequence([...])`     | `test_sequential.py`   |
| 3 | swarm via `OnCondition` handoffs | `ToolCalled` + `RevertToInitiatorTarget`        | `test_swarm_handoff.py`|
| 4 | `AutoPattern` (manager picks)    | manager-as-initiator + `RevertToInitiatorTarget`| `test_auto_manager.py` |
| 5 | raw `GroupChat(speaker_selection_method=callable)` | user-registered `TransitionCondition` (`TextContains`) via `register_condition` | `test_custom_handoff.py` |

Each test file is also runnable as a script for ad-hoc inspection:

```bash
python multiagent_orchestration/test_round_robin.py
```

## Notes

* Legacy uses `ConversableAgent` + `LLMConfig({"api_type": "google", ...})`.
* Workflow uses `autogen.beta.Agent` + `GeminiConfig(...)` and registers
  through `HubClient`.
* Pattern parity here is **functional** (same goal accomplished, same
  agent-selection semantics) — we don't expect byte-identical
  transcripts because the underlying agent runtimes differ.

## Findings (initial run, 2026-05-05, gemini-3-flash-preview)

All 8 tests pass against real Gemini in ~6 minutes total. Every classic
pattern has a clean workflow equivalent at the API level:

| Pattern | Classic API | Workflow API |
|---|---|---|
| Round-robin | `RoundRobinPattern(initial, agents)` | `TransitionGraph.round_robin(participants, max_turns=N)` |
| Sequential pipeline | per-agent `handoffs.set_after_work(AgentTarget(next))` chain | `TransitionGraph.sequence([a, b, c])` |
| Swarm | `agent.handoffs.add_llm_condition(OnCondition(...))` + per-agent `set_after_work` | `Transition(when=ToolCalled(name), then=AgentTarget(...))` + `RevertToInitiatorTarget` default |
| Auto / manager-picks | `AutoPattern` with LLM-backed group manager | manager-as-initiator + `ask_*` handoff tools + `RevertToInitiatorTarget` default |

Notable observations:

1. **Workflow's "manager-as-initiator" is more concise than classic's
   `AutoPattern`.** AutoPattern needs a separate group-manager LLM that
   reads agent descriptions on every selection turn — extra tokens per
   round. The workflow recipe lets the initial speaker do its own
   routing via tool calls; one LLM, one set of tokens.

2. **Workflow agents need to use `say` or a handoff tool to put text
   into a session.** A bare LLM body (no tool call) goes through
   `default_handler`'s `session.send(reply.body)` path, which under
   Gemini sometimes hangs (no failure, just no further notifies fire).
   When the LLM uses `say(content=...)` directly the message lands
   reliably. *Likely a yield-point gap in the notify dispatch path
   when `session.send` is invoked from `_process_text` after
   `agent.ask`* — worth surfacing to whoever is on Phase 2/3. All four
   workflow tests instruct agents to call `say` explicitly to sidestep
   it.

3. **`RevertToInitiatorTarget` is the right primitive** for the
   "specialists answer back to the manager" pattern. Classic has no
   direct equivalent — `RevertToUserTarget` reverts to the user proxy,
   not to the initial agent. We use `AgentTarget(initial_agent)`
   explicitly in the classic test as the closest match.

4. **`max_turns` counts substantive envelopes only.** A `transfer_to_eng`
   handoff and the eng reply each count as one. In the swarm test,
   `max_turns=6` gives triage budget for the initial handoff + eng
   reply + triage's summary + headroom. Setting it too low causes the
   workflow to terminate before the manager can summarise.

5. **`max_turns=len(steps)` in `TransitionGraph.sequence` is exactly
   right for one-pass pipelines.** Each step posts once; the workflow
   auto-closes with `auto_close_reason="max_turns"` after the last
   step.
