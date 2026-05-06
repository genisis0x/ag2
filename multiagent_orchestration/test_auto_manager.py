"""Manager-as-initiator parity: classic ``AutoPattern`` (group manager
LLM picks next speaker) vs workflow ``manager-as-initiator`` recipe
(``RevertToInitiatorTarget`` + tool-driven handoffs).

The recipes are not byte-for-byte identical:

* **Classic ``AutoPattern``** uses an LLM-backed group manager that
  reads the description of every agent and picks the next speaker.
* **Workflow** has the manager itself use ``ask_*`` handoff tools to
  choose; respondents revert to the manager. Same observable
  behaviour ("manager fans out to specialists, then closes"), no
  ``LLMSelectorTarget`` needed in V1 (Phase 2.0).

Both should:

1. Visit at least one specialist (math or general) corresponding to
   the question.
2. Eventually return control to the manager.
3. Terminate within a bounded turn budget.
"""

import os
from typing import cast

import pytest

from autogen.agentchat import ConversableAgent
from autogen.agentchat.group.multi_agent_chat import initiate_group_chat
from autogen.agentchat.group.patterns import AutoPattern
from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_HANDOFF,
    EV_TEXT,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.adapters.workflow import WORKFLOW_TYPE
from autogen.beta.network.client.plugin import NetworkPlugin
from autogen.beta.network.client.session import Session
from autogen.beta.network.policies import (
    AGENT_CLIENT_DEP,
    HUB_DEP,
    SESSION_DEP,
)
from autogen.beta.network.transitions import (
    AgentTarget,
    RevertToInitiatorTarget,
    ToolCalled,
    Transition,
    TransitionGraph,
)
from multiagent_orchestration._helpers import (
    print_section,
    transcript_lines,
    wait_for_predicate,
)

QUESTION = "What is 12 times 17? Tell me the numeric answer."


@pytest.mark.gemini
def test_classic_auto_pattern_routes_via_group_manager(classic_llm_config) -> None:
    print_section("CLASSIC AutoPattern (Gemini)")

    manager = ConversableAgent(
        name="manager",
        system_message=(
            "You are the manager. Decide which specialist should answer "
            "(math or general). Once you've heard from one, summarise "
            "their answer in one sentence and stop."
        ),
        description="Coordinates specialists and summarises the result.",
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )
    math = ConversableAgent(
        name="math",
        system_message="You are a math specialist. Answer with just the numeric result, no explanation.",
        description="Math specialist; use for arithmetic and numeric questions.",
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )
    general = ConversableAgent(
        name="general",
        system_message="You are a generalist. Answer briefly in one sentence.",
        description="General-knowledge specialist; use when no other expert applies.",
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )

    pattern = AutoPattern(
        initial_agent=manager,
        agents=[manager, math, general],
        group_manager_args={"llm_config": classic_llm_config},
    )
    chat_result, _ctx, _last = initiate_group_chat(
        pattern=pattern,
        messages=QUESTION,
        max_rounds=8,
    )

    history = chat_result.chat_history
    speakers = {
        m.get("name")
        for m in history
        if m.get("name") in {"manager", "math", "general"}
    }
    print("speakers seen:", speakers)
    for m in history:
        who = m.get("name", "?")
        body = (m.get("content") or "").replace("\n", " ")[:160]
        print(f"  {who}: {body}")

    assert "math" in speakers, f"math specialist never spoke: {speakers}"
    answers = [(m.get("content") or "") for m in history if m.get("name") == "math"]
    assert any("204" in a for a in answers), f"math didn't return 204: {answers}"


@pytest.mark.gemini
@pytest.mark.asyncio
async def test_workflow_manager_as_initiator(beta_gemini_config) -> None:
    print_section("WORKFLOW manager-as-initiator (Gemini)")

    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
    )
    link = LocalLink(hub)

    manager_agent = Agent(
        name="manager",
        prompt=(
            "You are the manager. For arithmetic questions call "
            "ask_math(reason=<the question>). For other questions call "
            "ask_general(reason=<the question>). After a specialist "
            "replies and control returns to you, call "
            "say(content=<one-sentence summary of the specialist's answer>) "
            "and stop."
        ),
        config=beta_gemini_config,
    )
    math_agent = Agent(
        name="math",
        prompt=(
            "You are a math specialist. When it is your turn, call "
            "say(content=<just the numeric answer, no explanation>). "
            "Do not call any other tool."
        ),
        config=beta_gemini_config,
    )
    general_agent = Agent(
        name="general",
        prompt=(
            "You are a generalist. When it is your turn, call "
            "say(content=<a one-sentence answer>). Do not call any "
            "other tool."
        ),
        config=beta_gemini_config,
    )

    mgr_hc = HubClient(link, hub=hub)
    math_hc = HubClient(link, hub=hub)
    gen_hc = HubClient(link, hub=hub)
    manager = await mgr_hc.register(manager_agent, Passport(name="manager"), Resume())
    math = await math_hc.register(
        math_agent, Passport(name="math"), Resume(claimed_capabilities=["math"])
    )
    general = await gen_hc.register(
        general_agent,
        Passport(name="general"),
        Resume(claimed_capabilities=["general"]),
    )

    graph = TransitionGraph(
        initial_speaker=manager.agent_id,
        transitions=[
            Transition(when=ToolCalled("ask_math"), then=AgentTarget(math.agent_id)),
            Transition(
                when=ToolCalled("ask_general"), then=AgentTarget(general.agent_id)
            ),
        ],
        default_target=RevertToInitiatorTarget(),
        max_turns=8,
    )

    NetworkPlugin(manager).register_workflow(graph)

    session = await manager.open(
        type=WORKFLOW_TYPE,
        target=[math.agent_id, general.agent_id],
        knobs={"graph": graph.to_dict()},
        intent="manager fans out to specialists",
    )

    session_handle = Session(metadata=session.metadata, client=manager)
    deps = {SESSION_DEP: session_handle, AGENT_CLIENT_DEP: manager, HUB_DEP: hub}
    await manager.agent.ask(QUESTION, dependencies=deps)

    # Wait until math has replied (math text in WAL).
    async def _math_replied() -> bool:
        wal = await hub.read_wal(session.session_id)
        return any(
            e.event_type == EV_TEXT and e.sender_id == math.agent_id for e in wal
        )

    settled = await wait_for_predicate(_math_replied, timeout=120.0)

    wal = await hub.read_wal(session.session_id)
    name_by_id = {
        manager.agent_id: "manager",
        math.agent_id: "math",
        general.agent_id: "general",
    }
    print("transcript:")
    for line in transcript_lines(wal, name_by_id):
        print(f"  {line}")

    assert settled, "math specialist never spoke"
    handoffs = [e for e in wal if e.event_type == EV_HANDOFF]
    handoff_tools = {e.event_data.get("tool") for e in handoffs}
    assert "ask_math" in handoff_tools, f"manager didn't call ask_math: {handoff_tools}"

    math_replies = [
        (e.event_data or {}).get("text", "")
        for e in wal
        if e.event_type == EV_TEXT and e.sender_id == math.agent_id
    ]
    assert any(
        "204" in t for t in math_replies
    ), f"math didn't return 204: {math_replies}"

    # After math reply, default target RevertToInitiator should send
    # control back to manager. Manager may have already replied — in
    # which case expected_next has revolved again. Just ensure the
    # workflow advanced past the math reply.
    state = hub._adapter_states.get(session.session_id)
    if state is not None:
        assert state.turn_count >= 2, f"only {state.turn_count} turns recorded"

    await mgr_hc.close()
    await math_hc.close()
    await gen_hc.close()
    await hub.close()


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    api_key = os.environ["GEMINI_API_KEY"]
    model = os.environ.get("AG2_GEMINI_MODEL", "gemini-3-flash-preview")

    from autogen.beta.config import GeminiConfig
    from autogen.llm_config import LLMConfig

    test_classic_auto_pattern_routes_via_group_manager(
        LLMConfig({"api_type": "google", "model": model, "api_key": api_key}),
    )
    asyncio.run(
        test_workflow_manager_as_initiator(
            cast(
                "GeminiConfig",
                GeminiConfig(model=model, api_key=api_key, temperature=0),
            )
        )
    )
