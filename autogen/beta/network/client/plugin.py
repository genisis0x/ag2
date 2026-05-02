# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``NetworkPlugin`` — attaches the network tool surface to an ``Agent``.

M2 surface:
* Adds the ``say`` and ``delegate`` tools to ``agent.tools``.
* Appends ``NetworkContextPolicy`` to the agent's assembly chain so
  every LLM call sees a "you are <name>" prefix plus the available
  tool names. M3 enriches the prefix with peer list, active session
  expectations, and active task metadata.

Plugins are first-class in beta (``autogen/beta/agent.py`` ``Plugin``
class). The network plugin uses the existing slot.
"""

from typing import TYPE_CHECKING

from autogen.beta.agent import Plugin
from autogen.beta.assembly import AssemblyPolicy
from autogen.beta.events import BaseEvent

from .tools import make_delegate_tool, make_say_tool

if TYPE_CHECKING:
    from autogen.beta.agent import Agent
    from autogen.beta.context import ConversationContext as Context

    from .agent_client import AgentClient

__all__ = ("NetworkContextPolicy", "NetworkPlugin")


class NetworkContextPolicy:
    """Assembly policy: prepends a network-aware prefix to every LLM call.

    M2 minimal version — names the agent and lists its network tools.
    M3 expands to peer list (with TTL-cached ``describe_network``),
    active session expectations, and active task metadata.
    """

    name = "network_context"

    def __init__(self, client: "AgentClient") -> None:
        # __init__ stores params; no side effects.
        self._client = client

    async def apply(
        self,
        prompts: list[str],
        events: list[BaseEvent],
        context: "Context",
    ) -> tuple[list[str], list[BaseEvent]]:
        prefix = (
            f"You are {self._client.passport.name} "
            f"(agent_id: {self._client.agent_id}).\n"
            "Network tools available: say, delegate."
        )
        return [prefix, *prompts], events


class NetworkPlugin(Plugin):
    """Attaches an Agent to a network.

    Adds ``say`` and ``delegate`` to ``agent.tools`` so the LLM sees
    them on every turn — the verbs are stable for the life of the
    registration. Also appends ``NetworkContextPolicy`` to the agent's
    assembly chain.
    """

    def __init__(self, client: "AgentClient") -> None:
        super().__init__(
            tools=[
                make_say_tool(client),
                make_delegate_tool(client),
            ],
        )
        self._client = client

    def register(self, agent: "Agent") -> None:
        """Wire tools + assembly policy onto the agent. Idempotent-ish.

        Calling ``register`` more than once on the same agent will add
        the tools / policies again. ``HubClient.register`` only attaches
        once per ``(Agent, identity)``, so this is rare in practice.
        """
        super().register(agent)
        agent._policies.append(NetworkContextPolicy(self._client))


# Make ``NetworkPlugin`` satisfy ``AssemblyPolicy`` indirectly via its
# context-policy member. The Protocol is structural; ``NetworkContextPolicy``
# implements ``apply`` correctly so the implicit assertion holds.
_: AssemblyPolicy
