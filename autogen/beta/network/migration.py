# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Phase 2.0 — AG2-classic ``Pattern`` → ``WorkflowGraph`` migration.

The framework's V2 ``WorkflowAdapter`` is a superset of classic
``GroupChat`` + ``Handoffs`` + ``AfterWork``: every classic
orchestration pattern collapses to a declarative ``TransitionGraph``.
This helper performs the translation so users moving off
``autogen.agentchat.group.patterns`` get a drop-in replacement
without hand-rewriting their flow.

Supported in V1 of the helper:

* ``RoundRobinPattern`` → :meth:`TransitionGraph.round_robin`.
* ``AutoPattern`` → :meth:`TransitionGraph.auto_pattern` (requires a
  ``selector_id`` since classic's group manager has no agent_id —
  pick one of the participants or pass an explicit selector).

Unsupported (raise ``NotImplementedError``) so callers get a clear
signal instead of a silently-wrong graph:

* ``RandomPattern`` — needs ``RandomTarget`` (Phase 4).
* ``ManualPattern`` — needs a ``HumanClient`` (post Phase 4).
* ``DefaultPattern`` with arbitrary handoffs — handoff extraction is
  Phase 2.1 once the classic ``AfterWork`` / ``OnContextCondition``
  vocabulary settles in V2 conditions.
"""

from typing import TYPE_CHECKING, Any

from .errors import NetworkError
from .transitions import TransitionGraph

if TYPE_CHECKING:
    from autogen.agentchat.group.patterns.pattern import Pattern

__all__ = ("UnsupportedPatternError", "from_classic_pattern")


class UnsupportedPatternError(NetworkError):
    """Raised when a classic ``Pattern`` has no V2 equivalent yet."""


def _agent_name(agent: Any) -> str:
    """Best-effort extraction of the agent's name."""
    name = getattr(agent, "name", None)
    if name:
        return str(name)
    return str(agent)


def _participants_in_classic_order(pattern: "Pattern") -> list[str]:
    """Reproduce ``RoundRobinPattern._generate_handoffs`` ordering.

    [initial_agent, ...other agents excluding initial + user, user_agent_last_if_present].
    """
    initial = _agent_name(pattern.initial_agent)
    user = (
        _agent_name(pattern.user_agent) if pattern.user_agent is not None else None
    )
    seen = {initial}
    if user is not None:
        seen.add(user)
    ordered: list[str] = [initial]
    for agent in pattern.agents:
        n = _agent_name(agent)
        if n not in seen:
            ordered.append(n)
            seen.add(n)
    if user is not None:
        ordered.append(user)
    return ordered


def from_classic_pattern(
    pattern: "Pattern",
    *,
    selector_id: str | None = None,
    handoff_tools: dict[str, str] | None = None,
    max_turns: int | None = None,
) -> TransitionGraph:
    """Translate a classic ``Pattern`` instance into a ``TransitionGraph``.

    Args:
        pattern: an instance of one of the classic ``Pattern``
            subclasses (``RoundRobinPattern``, ``AutoPattern``, etc.).
        selector_id: required for ``AutoPattern`` — classic's group
            manager has no agent_id, so the caller picks which
            participant (or external manager) plays the selector role.
        handoff_tools: passed through to
            :meth:`TransitionGraph.auto_pattern` for ``AutoPattern``.
        max_turns: caps the resulting graph's turn count. Defaults to
            ``len(participants)`` for round-robin, ``None`` for auto.

    Returns:
        A :class:`TransitionGraph` that can be passed via
        ``client.open(type="workflow", knobs={"graph": graph.dumps()})``.

    Raises:
        UnsupportedPatternError: when the pattern has no V2 equivalent
            in the current phase. The error message names the missing
            primitive (e.g. ``RandomTarget``) so the user can tell when
            the gap will close.
    """
    cls_name = type(pattern).__name__

    if cls_name == "RoundRobinPattern":
        participants = _participants_in_classic_order(pattern)
        return TransitionGraph.round_robin(
            participants=participants,
            max_turns=max_turns if max_turns is not None else len(participants),
        )

    if cls_name == "AutoPattern":
        if selector_id is None:
            raise UnsupportedPatternError(
                "from_classic_pattern(AutoPattern, ...) requires "
                "selector_id= — classic's group manager has no agent_id, "
                "so pick which agent plays the selector role."
            )
        candidates = [
            _agent_name(a)
            for a in pattern.agents
            if _agent_name(a) != selector_id
        ]
        if pattern.user_agent is not None:
            user_name = _agent_name(pattern.user_agent)
            if user_name != selector_id and user_name not in candidates:
                candidates.append(user_name)
        return TransitionGraph.auto_pattern(
            selector_id=selector_id,
            candidates=candidates,
            handoff_tools=handoff_tools,
            max_turns=max_turns,
        )

    if cls_name == "RandomPattern":
        raise UnsupportedPatternError(
            "RandomPattern → WorkflowGraph requires RandomTarget (Phase 4)."
        )

    if cls_name == "ManualPattern":
        raise UnsupportedPatternError(
            "ManualPattern → WorkflowGraph requires HumanClient (post Phase 4)."
        )

    if cls_name == "DefaultPattern":
        # DefaultPattern carries arbitrary per-agent handoffs which
        # need ContextExpr / OnContextCondition translation. Phase 2.1.
        raise UnsupportedPatternError(
            f"DefaultPattern → WorkflowGraph translation is Phase 2.1 "
            f"(needs ContextExpr / OnContextCondition); for now express "
            f"the handoffs directly as Transition(when=..., then=...)."
        )

    raise UnsupportedPatternError(
        f"Unknown classic pattern: {cls_name}. Supported: "
        f"RoundRobinPattern, AutoPattern."
    )
