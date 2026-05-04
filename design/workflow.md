# Workflow

`WorkflowAdapter` is the Network's answer to AG2-classic's `GroupChat` + `Handoffs` + `AfterWork` triad. It is one more `SessionAdapter`: a flow whose next-speaker is determined by a declarative `TransitionGraph` over folded state.

## Why a separate adapter

`consulting` is strict 1:1. `discussion` is symmetric multi-party with `round_robin` / `dynamic` / `static` ordering. Neither expresses "Alice → conditionally → Bob | Carol with the choice driven by Alice's tool call." Rather than overload `discussion`, `WorkflowAdapter` is purpose-built for orchestrated flows where speaker selection is the central concern.

The mechanic itself is what `discussion(round_robin)` already uses — `AdapterState.expected_next_speaker` is folded from the WAL, `validate_send` rejects sends from anyone else. `WorkflowAdapter` only adds a richer rule for *how* `expected_next_speaker` advances. **No hub changes are required.**

## Transition vocabulary

```python
# autogen/beta/network/transitions.py

class TransitionTarget(Protocol):
    """Where the next turn goes. Pure resolver — no I/O."""

    name: ClassVar[str]                            # registry key

    def resolve(
        self,
        metadata: SessionMetadata,
        state: WorkflowState,
        envelope: Envelope,
    ) -> TransitionDecision: ...


@dataclass(slots=True)
class TransitionDecision:
    next_speaker: str | None                       # None = terminate
    close_reason: str = ""                         # populated only when terminating


class TransitionCondition(Protocol):
    """When a transition fires. Pure predicate — no I/O."""

    name: ClassVar[str]                            # registry key

    def evaluate(
        self,
        metadata: SessionMetadata,
        state: WorkflowState,
        envelope: Envelope,
    ) -> bool: ...


@dataclass(slots=True)
class Transition:
    when: TransitionCondition
    then: TransitionTarget
    priority: int = 0                              # lower = earlier; ties → list order
```

### Built-in TransitionTargets (V1)

| Target | Args | Resolves to |
|---|---|---|
| `AgentTarget` | `agent_id: str` | the named agent |
| `RoundRobinTarget` | — | next participant in `metadata.participants` `order` after `state.last_speaker_id` |
| `StayTarget` | — | `state.last_speaker_id` |
| `RevertToInitiatorTarget` | — | `metadata.creator_id` |
| `TerminateTarget` | `reason: str = "after_work"` | `next_speaker=None`, populates `close_reason` |

`RandomTarget`, `LLMSelectorTarget`, and `NestedSessionTarget` are Phase 2 (see [Deferred](#deferred-to-phase-2)).

### Built-in TransitionConditions (V1)

| Condition | Args | Fires when |
|---|---|---|
| `Always` | — | every accepted turn |
| `FromSpeaker` | `agent_id: str` | the just-accepted envelope was sent by `agent_id` |
| `ToolCalled` | `tool_name: str` | the just-accepted envelope is `event_type="ag2.handoff"` with `event_data["tool"]==tool_name` |

`ContextExpr`, `TurnCountReached`, and richer condition kinds are Phase 2.

These five targets and three conditions are deliberately the minimum that covers the common patterns without forcing architectural decisions (async resolution, expression evaluators, child-session lifecycle) before they're needed. The `Protocol` is open and the named registry (see [Registries](#registries)) accepts new types in two lines.

## TransitionGraph

```python
@dataclass(slots=True)
class TransitionGraph:
    initial_speaker: str
    transitions: list[Transition]
    default_target: TransitionTarget = field(default_factory=TerminateTarget)
    max_turns: int | None = None
```

**Evaluation order on each accepted envelope:**

1. If `max_turns` set and reached → terminate (`auto_close_reason="max_turns"`).
2. Walk `transitions` in `priority` order (ties = list order); first whose `when.evaluate(...)` returns `True` wins.
3. If no transition matches → `default_target.resolve(...)`.
4. Apply the `TransitionDecision`: update `state.expected_next_speaker` or transition the session to `CLOSED`.

The graph is **not Turing-complete by design**. No loops over the graph itself (only over WAL turns); no recursion of resolution. This is what makes it persistable and replayable.

## WorkflowAdapter

```python
# autogen/beta/network/adapters/workflow.py

WORKFLOW_TYPE = "workflow"


@dataclass(slots=True)
class WorkflowState:
    expected_next_speaker: str | None = None
    last_speaker_id: str | None = None
    last_envelope_id: str | None = None
    turn_count: int = 0


class WorkflowAdapter:
    """Generic orchestrated multi-party session.

    knobs:
        graph: TransitionGraph (serialized)        # required
    """

    def __init__(self) -> None:
        self.manifest = SessionManifest(
            type=WORKFLOW_TYPE,
            version=1,
            participants=ParticipantSchema(min=2),
            knobs_schema={"graph": "TransitionGraph"},
            default_view_policy=WindowedSummary.name,
            expectations=[
                Expectation(name="turn_within", on_violation="warn",
                            params={"seconds": 120}),
                Expectation(name="turn_within", on_violation="auto_close",
                            params={"seconds": 600}),
            ],
        )

    def initial_state(self, metadata):
        graph = TransitionGraph.loads(metadata.knobs["graph"])
        return WorkflowState(expected_next_speaker=graph.initial_speaker)

    def fold(self, envelope, state):
        # Session-protocol and task envelopes don't advance turns.
        # ag2.handoff and ag2.msg.text envelopes update last_speaker_id
        # and turn_count; on_accepted then advances expected_next_speaker.
        ...

    def validate_send(self, metadata, envelope, state):
        if state.expected_next_speaker and envelope.sender_id != state.expected_next_speaker:
            raise ProtocolError(
                f"workflow {metadata.session_id!r} expects "
                f"{state.expected_next_speaker!r} to speak, got {envelope.sender_id!r}"
            )

    def on_accepted(self, metadata, envelope, state):
        graph = TransitionGraph.loads(metadata.knobs["graph"])
        if graph.max_turns is not None and state.turn_count >= graph.max_turns:
            return AdapterResult(next_state=SessionState.CLOSED,
                                 auto_close_reason="max_turns")
        decision = self._select(graph, metadata, state, envelope)
        if decision.next_speaker is None:
            return AdapterResult(next_state=SessionState.CLOSED,
                                 auto_close_reason=decision.close_reason)
        state.expected_next_speaker = decision.next_speaker
        return AdapterResult()

    @staticmethod
    def _select(graph, metadata, state, envelope) -> TransitionDecision:
        for tr in sorted(graph.transitions, key=lambda t: t.priority):
            if tr.when.evaluate(metadata, state, envelope):
                return tr.then.resolve(metadata, state, envelope)
        return graph.default_target.resolve(metadata, state, envelope)
```

The adapter is stateless and pure. All state lives in `WorkflowState`, folded from the WAL. `Hub.hydrate()` rebuilds it on restart by replaying the WAL through `fold` — same mechanism every other adapter uses.

## Dispatch

`WorkflowAdapter` reuses the existing dispatch path with no hub changes:

1. Sender posts envelope; hub `_dispatch` broadcasts `NotifyFrame` to all participants (hub/core.py:844).
2. Each participant's notify handler calls `adapter.validate_send` for itself before engaging the LLM.
3. Only the agent matching `state.expected_next_speaker` survives the gate; everyone else's handler is a no-op.

Per-recipient routing (stamping `audience=[expected_next_speaker]` on outbound dispatch) is a Phase 2 optimization that adds a `dispatch_audience` hook to the `SessionAdapter` Protocol. Not on the M4 critical path — broadcast cost is negligible at <20 participants.

## LLM-driven handoffs

The `OnCondition`-style "LLM picks the transition" pattern collapses to **one tool per `ToolCalled` transition**. `NetworkPlugin.register_workflow(graph)` materializes those tools and attaches them to `agent.tools` on registration:

```python
@tool(description="Transfer the conversation to the engineering team.")
async def transfer_to_engineering(
    reason: str,
    session: SessionInject,
) -> str:
    await session.send(
        content=f"[handoff] {reason}",
        event_type="ag2.handoff",
        event_data={"tool": "transfer_to_engineering"},
    )
    return "handoff posted"
```

The adapter's `fold` reads `event_type=="ag2.handoff"`, the `ToolCalled("transfer_to_engineering")` condition fires in `on_accepted`, and `state.expected_next_speaker` advances. The LLM never sees `expected_next_speaker` directly — it sees a button labeled "transfer," and the protocol does the rest. **Handoffs are a UX over the choreography.**

`OnContextCondition`-style handoffs (no LLM) become `Transition(when=ContextExpr(...), then=...)` once Phase 2 ships `ContextExpr`. The vocabulary is identical; only the evaluation strategy differs.

`ag2.handoff` is added to the framework's stable event-type set in [envelope.md](envelope.md). Like `ag2.msg.text`, it's adapter-agnostic — any future adapter that wants tool-driven transitions reads it the same way.

## Persistence

`TransitionGraph` is data, not code. It serializes to JSON via `TransitionGraph.dumps()` and restores via `TransitionGraph.loads(data)`. Targets and conditions resolve through named registries:

```python
# autogen/beta/network/transitions.py

_TARGET_REGISTRY: dict[str, type[TransitionTarget]] = {
    "agent": AgentTarget,
    "round_robin": RoundRobinTarget,
    "stay": StayTarget,
    "revert_to_initiator": RevertToInitiatorTarget,
    "terminate": TerminateTarget,
}

_CONDITION_REGISTRY: dict[str, type[TransitionCondition]] = {
    "always": Always,
    "from_speaker": FromSpeaker,
    "tool_called": ToolCalled,
}

def register_target(target_cls: type[TransitionTarget]) -> None: ...
def register_condition(condition_cls: type[TransitionCondition]) -> None: ...
```

`metadata.knobs["graph"]` stores the serialized form (a JSON dict tagged by registry name). `Hub.hydrate()` re-folds the WAL through `WorkflowAdapter.fold` exactly like any other adapter; `loads` looks up registered classes by name.

Re-registering a name replaces the prior class and logs a warning, mirroring `Hub.register_adapter`. Unknown names raise `WorkflowGraphError("no target/condition registered for X")`.

## Pattern recipes

Each classic AG2 pattern collapses to a `TransitionGraph` literal. `transitions.py` ships factory helpers:

```python
# Round-robin (matches discussion(round_robin) + AfterWork.TERMINATE)
WorkflowGraph.round_robin(
    participants=["alice", "bob", "carol"],
    max_turns=12,
)

# Sequential pipeline
WorkflowGraph.sequence(
    steps=["researcher", "writer", "reviewer"],
)

# Swarm with tool-driven handoffs (initiator routes; respondents revert)
WorkflowGraph(
    initial_speaker="triage",
    transitions=[
        Transition(when=ToolCalled("transfer_to_eng"),   then=AgentTarget("eng")),
        Transition(when=ToolCalled("transfer_to_legal"), then=AgentTarget("legal")),
        Transition(when=FromSpeaker("eng"),              then=RevertToInitiatorTarget()),
        Transition(when=FromSpeaker("legal"),            then=RevertToInitiatorTarget()),
    ],
    default_target=TerminateTarget(reason="triage_done"),
    max_turns=20,
)

# Manager-as-initiator (auto-pattern equivalent — no LLMSelectorTarget needed in V1)
WorkflowGraph(
    initial_speaker="manager",
    transitions=[
        Transition(when=ToolCalled("ask_alice"), then=AgentTarget("alice")),
        Transition(when=ToolCalled("ask_bob"),   then=AgentTarget("bob")),
    ],
    default_target=RevertToInitiatorTarget(),
    max_turns=20,
)
```

The manager-as-initiator recipe is how V1 expresses AG2-classic's `AutoPattern` without introducing `LLMSelectorTarget`'s async-resolution edge case. The manager agent is itself in the participant list, gets every off-turn back via `RevertToInitiatorTarget`, and uses `ToolCalled` handoffs to direct. This matches AutoPattern semantics one-for-one — the manager just happens to also be the initiator.

A migration helper `WorkflowGraph.from_pattern(...)` that consumes a classic `Pattern` instance is Phase 2.

## Registries — extending the vocabulary

Custom targets and conditions plug in via two-line registration:

```python
@dataclass(slots=True)
class WhenTurnCount:
    n: int
    name: ClassVar[str] = "turn_count"

    def evaluate(self, metadata, state, envelope) -> bool:
        return state.turn_count >= self.n

register_condition(WhenTurnCount)
```

Custom classes serialize the same way V1 ones do, as long as they're `@dataclass(slots=True)` with JSON-friendly fields. The registry is process-local; cross-process usage (Phase 3) requires both ends to register the same name.

## Deferred to Phase 2

Kept out of M4 to keep the core surface tight. The Protocol design accommodates each without architectural disruption.

- `RandomTarget` — random speaker pick.
- `LLMSelectorTarget` — selector agent picks the next speaker. Requires the hub to resolve a target asynchronously (open a sub-consulting session, await reply, parse the pick). V1 expresses the same use case via the manager-as-initiator recipe above.
- `NestedSessionTarget` — opens a child session under `parent_session_id`. The Network's `SocietyOfMind` story. Needs close-cascade tweaks.
- `ContextExpr` and `TurnCountReached` — pure-Python no-LLM conditions. Need a shared expression evaluator (not duplicated from `rules.py`).
- `SubGraph` target — composing one workflow into another.
- Saga / compensation — `OnFailure` transitions + reversal targets.
- `dispatch_audience` hook on `SessionAdapter` — per-recipient routing optimization.
- Migration helper: classic `Pattern` → `WorkflowGraph`.
- Cross-process auto-shipping of registered classes to a remote hub.

## Invariants

- A workflow session is referenced by exactly one `TransitionGraph` for its lifetime; the graph is snapshotted into `metadata.knobs["graph"]` at create time and never mutates.
- `WorkflowState.expected_next_speaker` is always a current participant or `None`. Removing a participant (via `Expectation`'s `remove` handler) falls through to `default_target` on the next turn.
- `TransitionTarget.resolve` and `TransitionCondition.evaluate` are pure functions of `(metadata, state, envelope)`. Side-effecting implementations are forbidden — they break `Hub.hydrate()`.
- `AdapterState.expected_next_speaker` advances exactly once per accepted non-protocol envelope.
- `max_turns` counts substantive envelopes (`ag2.msg.text` and `ag2.handoff`); session-protocol and task envelopes don't increment.
