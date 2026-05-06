# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Phase 2.0 workflow extras: LLMSelectorTarget + classic Pattern migration.

LLMSelectorTarget routes the next turn to a selector agent who then
hands off to a candidate via tool call. ``TransitionGraph.auto_pattern``
wires the full selector + handoff routing in one factory.

The migration helper translates AG2-classic ``Pattern`` instances into
equivalent ``TransitionGraph``s so users can drop in V2 without
hand-rewriting their orchestration.
"""

import pytest

from autogen.beta.network import (
    UnsupportedPatternError,
    from_classic_pattern,
)
from autogen.beta.network.adapters.workflow import WorkflowState
from autogen.beta.network.transitions import (
    AgentTarget,
    LLMSelectorTarget,
    TerminateTarget,
    ToolCalled,
    Transition,
    TransitionGraph,
    TransitionRegistry,
)
from autogen.beta.network.envelope import EV_HANDOFF, Envelope


def _state(**kwargs) -> WorkflowState:
    """Build a minimal WorkflowState for resolver testing."""
    defaults = {
        "expected_next_speaker": None,
        "last_speaker_id": None,
        "last_envelope_id": None,
        "turn_count": 0,
        "participant_order": [],
        "creator_id": "alice",
        "graph_data": {},
        "pending_close_reason": "",
    }
    defaults.update(kwargs)
    return WorkflowState(**defaults)


def _envelope(sender: str = "alice") -> Envelope:
    return Envelope(
        session_id="s1",
        sender_id=sender,
        audience=None,
        event_type="ag2.msg.text",
        event_data={"text": "hi"},
    )


def test_llm_selector_target_resolves_to_selector() -> None:
    """``LLMSelectorTarget.resolve`` always picks the selector agent."""
    target = LLMSelectorTarget(
        selector_id="manager",
        candidates=["alice", "bob", "carol"],
    )
    decision = target.resolve(_state(), _envelope())
    assert decision.next_speaker == "manager"
    assert decision.close_reason == ""


def test_llm_selector_target_round_trips_via_registry() -> None:
    """Serialise + restore via the default registry preserves args."""
    graph = TransitionGraph(
        initial_speaker="manager",
        transitions=[
            Transition(
                when=ToolCalled("transfer_to_bob"),
                then=AgentTarget("bob"),
            ),
        ],
        default_target=LLMSelectorTarget(
            selector_id="manager",
            candidates=["bob", "carol"],
        ),
    )
    restored = TransitionGraph.loads(graph.dumps())
    assert isinstance(restored.default_target, LLMSelectorTarget)
    assert restored.default_target.selector_id == "manager"
    assert restored.default_target.candidates == ["bob", "carol"]


def test_auto_pattern_factory_wires_selector_and_handoffs() -> None:
    """``TransitionGraph.auto_pattern`` produces selector → candidate routing."""
    graph = TransitionGraph.auto_pattern(
        selector_id="manager",
        candidates=["alice", "bob"],
    )
    assert graph.initial_speaker == "manager"

    # Two ToolCalled transitions (one per candidate) + two FromSpeaker
    # routes back to the selector.
    tool_called_transitions = [
        t for t in graph.transitions if t.when.name == "tool_called"
    ]
    from_speaker_transitions = [
        t for t in graph.transitions if t.when.name == "from_speaker"
    ]
    assert len(tool_called_transitions) == 2
    assert len(from_speaker_transitions) == 2

    # Default tool names follow the documented convention.
    tool_names = sorted(t.when.tool_name for t in tool_called_transitions)
    assert tool_names == ["transfer_to_alice", "transfer_to_bob"]


def test_auto_pattern_factory_accepts_custom_tool_mapping() -> None:
    """Caller-supplied tool names override the default ``transfer_to_X``."""
    graph = TransitionGraph.auto_pattern(
        selector_id="boss",
        candidates=["eng", "legal"],
        handoff_tools={"eng": "ask_engineering", "legal": "ask_legal"},
    )
    tool_names = sorted(
        t.when.tool_name
        for t in graph.transitions
        if t.when.name == "tool_called"
    )
    assert tool_names == ["ask_engineering", "ask_legal"]


def test_auto_pattern_factory_rejects_empty_candidates() -> None:
    from autogen.beta.network.transitions import WorkflowGraphError

    with pytest.raises(WorkflowGraphError):
        TransitionGraph.auto_pattern(selector_id="m", candidates=[])


# ── Migration helper ────────────────────────────────────────────────────────


class _FakeAgent:
    """Stand-in for ``ConversableAgent``: the migration helper only
    reads ``.name``, so we don't need the real classic dependency."""

    def __init__(self, name: str) -> None:
        self.name = name


class _RoundRobinPattern:
    """Mimic enough of classic ``RoundRobinPattern`` for migration."""

    def __init__(self, initial_agent, agents, user_agent=None) -> None:
        self.initial_agent = initial_agent
        self.agents = agents
        self.user_agent = user_agent

    def __class__(self):  # pragma: no cover — only the name matters
        return type(self)


# Force the type name without importing classic.
_RoundRobinPattern.__name__ = "RoundRobinPattern"


class _AutoPattern:
    def __init__(self, initial_agent, agents, user_agent=None) -> None:
        self.initial_agent = initial_agent
        self.agents = agents
        self.user_agent = user_agent


_AutoPattern.__name__ = "AutoPattern"


class _RandomPattern:
    def __init__(self, initial_agent, agents) -> None:
        self.initial_agent = initial_agent
        self.agents = agents


_RandomPattern.__name__ = "RandomPattern"


def test_migrates_round_robin_pattern_preserves_order() -> None:
    """``RoundRobinPattern`` → round-robin graph with classic ordering."""
    initial = _FakeAgent("alice")
    bob = _FakeAgent("bob")
    carol = _FakeAgent("carol")
    user = _FakeAgent("user")
    pattern = _RoundRobinPattern(
        initial_agent=initial,
        agents=[initial, bob, carol],
        user_agent=user,
    )

    graph = from_classic_pattern(pattern)

    # Initial speaker is the classic initial_agent.
    assert graph.initial_speaker == "alice"
    # max_turns defaults to participant count.
    assert graph.max_turns == 4
    # Transitions are the round-robin shape (single Always → RoundRobinTarget).
    assert len(graph.transitions) == 1
    assert graph.transitions[0].when.name == "always"
    assert graph.transitions[0].then.name == "round_robin"


def test_migrates_auto_pattern_with_explicit_selector() -> None:
    """``AutoPattern`` requires explicit selector_id; classic has no manager id."""
    selector = _FakeAgent("manager")
    bob = _FakeAgent("bob")
    carol = _FakeAgent("carol")
    pattern = _AutoPattern(
        initial_agent=selector,
        agents=[selector, bob, carol],
    )

    graph = from_classic_pattern(pattern, selector_id="manager")

    assert graph.initial_speaker == "manager"
    candidates_in_handoff = sorted(
        t.then.agent_id
        for t in graph.transitions
        if t.when.name == "tool_called"
    )
    assert candidates_in_handoff == ["bob", "carol"]


def test_migrates_auto_pattern_without_selector_errors() -> None:
    """Auto-pattern migration without selector_id raises a helpful error."""
    selector = _FakeAgent("manager")
    pattern = _AutoPattern(
        initial_agent=selector,
        agents=[selector, _FakeAgent("bob")],
    )
    with pytest.raises(UnsupportedPatternError, match="selector_id"):
        from_classic_pattern(pattern)


def test_migrates_random_pattern_errors() -> None:
    """RandomPattern is Phase 4 — error names the missing primitive."""
    initial = _FakeAgent("alice")
    pattern = _RandomPattern(
        initial_agent=initial,
        agents=[initial, _FakeAgent("bob")],
    )
    with pytest.raises(UnsupportedPatternError, match="RandomTarget"):
        from_classic_pattern(pattern)


def test_migrates_unknown_pattern_errors() -> None:
    """Unknown pattern types raise a clear error with the supported list."""

    class _CustomPattern:
        initial_agent = _FakeAgent("alice")
        agents = []
        user_agent = None

    _CustomPattern.__name__ = "CustomPattern"

    with pytest.raises(UnsupportedPatternError, match="Supported"):
        from_classic_pattern(_CustomPattern())


def test_migrate_round_robin_uses_custom_registry_via_loads() -> None:
    """The migrated graph round-trips through ``loads`` with a fresh registry."""
    pattern = _RoundRobinPattern(
        initial_agent=_FakeAgent("a"),
        agents=[_FakeAgent("a"), _FakeAgent("b"), _FakeAgent("c")],
    )
    graph = from_classic_pattern(pattern)
    blob = graph.dumps()

    restored = TransitionGraph.loads(blob, registry=TransitionRegistry())
    assert restored.initial_speaker == graph.initial_speaker
    assert restored.max_turns == graph.max_turns


# ── Smoke: LLMSelectorTarget actually drives a workflow turn ────────────────


def test_workflow_state_advances_via_llm_selector_target() -> None:
    """Resolve from a non-selector to the selector via LLMSelectorTarget."""
    target = LLMSelectorTarget(
        selector_id="manager",
        candidates=["alice", "bob"],
    )
    state = _state(
        expected_next_speaker=None,
        last_speaker_id="alice",
        creator_id="alice",
        participant_order=["alice", "bob", "manager"],
    )
    env = Envelope(
        session_id="s1",
        sender_id="alice",
        audience=None,
        event_type=EV_HANDOFF,
        event_data={"tool": "transfer_to_manager"},
    )
    decision = target.resolve(state, env)
    assert decision.next_speaker == "manager"
