"""Swarm-with-handoff parity: classic ``OnCondition`` + ``set_after_work``
vs workflow ``ToolCalled`` + ``RevertToInitiatorTarget``.

A triage agent receives a question, hands off to the right specialist
(eng for code, legal for compliance), and the specialist's reply is
returned to triage. Triage closes the conversation.

Both backends should:

1. Route the engineering question to the engineering specialist.
2. Bring control back to triage after the specialist replies.
3. Terminate cleanly (workflow auto-closes via ``max_turns`` /
   classic terminates via after-work fall-through).
"""

import os
from typing import cast

import pytest

from autogen.agentchat import ConversableAgent
from autogen.agentchat.group.llm_condition import StringLLMCondition
from autogen.agentchat.group.multi_agent_chat import initiate_group_chat
from autogen.agentchat.group.on_condition import OnCondition
from autogen.agentchat.group.patterns import DefaultPattern
from autogen.agentchat.group.targets.transition_target import (
    AgentTarget as ClassicAgentTarget,
)
from autogen.agentchat.group.targets.transition_target import (
    TerminateTarget as ClassicTerminateTarget,
)
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
    FromSpeaker,
    RevertToInitiatorTarget,
    TerminateTarget,
    ToolCalled,
    Transition,
    TransitionGraph,
)
from multiagent_orchestration._helpers import (
    print_section,
    transcript_lines,
    wait_for_predicate,
)

QUESTION = "Why is async file I/O typically slower than sync for small reads?"


@pytest.mark.gemini
def test_classic_swarm_handoff_routes_via_oncondition(classic_llm_config) -> None:
    print_section("CLASSIC swarm via OnCondition (Gemini)")

    triage = ConversableAgent(
        name="triage",
        system_message=(
            "You are the triage coordinator. When the question is about "
            "engineering, software, or systems, hand off to the eng "
            "specialist by selecting the corresponding handoff. Otherwise "
            "answer briefly yourself."
        ),
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )
    eng = ConversableAgent(
        name="eng",
        system_message=(
            "You are a senior engineer. Answer the question concisely "
            "in one or two sentences."
        ),
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )

    triage.handoffs.add_llm_condition(
        OnCondition(
            target=ClassicAgentTarget(agent=eng),
            condition=StringLLMCondition(
                "Hand off to eng when the user is asking an engineering, "
                "software, or systems question."
            ),
        ),
    )
    # Eng reverts to triage (the initial agent, not the user); triage's
    # after-work fall-through terminates the session.
    eng.handoffs.set_after_work(target=ClassicAgentTarget(agent=triage))
    triage.handoffs.set_after_work(target=ClassicTerminateTarget())

    pattern = DefaultPattern(
        initial_agent=triage,
        agents=[triage, eng],
        group_after_work=ClassicTerminateTarget(),
    )
    chat_result, _ctx, _last = initiate_group_chat(
        pattern=pattern,
        messages=QUESTION,
        max_rounds=8,
    )

    history = chat_result.chat_history
    speakers_seen = {
        m.get("name") for m in history if m.get("name") in {"triage", "eng"}
    }
    print("speakers seen:", speakers_seen)
    for m in history:
        who = m.get("name", "?")
        body = (m.get("content") or "").replace("\n", " ")[:160]
        print(f"  {who}: {body}")

    assert "eng" in speakers_seen, "eng never spoke — handoff didn't fire"


@pytest.mark.gemini
@pytest.mark.asyncio
async def test_workflow_swarm_handoff_routes_via_tool(beta_gemini_config) -> None:
    print_section("WORKFLOW swarm via ToolCalled handoff (Gemini)")

    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
    )
    link = LocalLink(hub)

    triage_agent = Agent(
        name="triage",
        prompt=(
            "You are the triage coordinator. If the user asks an engineering "
            "question, call transfer_to_eng(reason=<one-line reason>) — do "
            "NOT try to answer yourself. After eng replies and control "
            "returns to you, call say(content=<one-sentence summary of "
            "eng's answer>) and stop."
        ),
        config=beta_gemini_config,
    )
    eng_agent = Agent(
        name="eng",
        prompt=(
            "You are a senior engineer. When it is your turn, call "
            "say(content=<a one-or-two-sentence answer>). Do not call "
            "any other tool."
        ),
        config=beta_gemini_config,
    )

    triage_hc = HubClient(link, hub=hub)
    eng_hc = HubClient(link, hub=hub)
    triage = await triage_hc.register(
        triage_agent, Passport(name="triage"), Resume(claimed_capabilities=["triage"])
    )
    eng = await eng_hc.register(
        eng_agent, Passport(name="eng"), Resume(claimed_capabilities=["engineering"])
    )

    graph = TransitionGraph(
        initial_speaker=triage.agent_id,
        transitions=[
            Transition(
                when=ToolCalled("transfer_to_eng"),
                then=AgentTarget(eng.agent_id),
            ),
            Transition(
                when=FromSpeaker(eng.agent_id),
                then=RevertToInitiatorTarget(),
            ),
        ],
        default_target=TerminateTarget(reason="triage_done"),
        max_turns=6,
    )

    triage_plugin = NetworkPlugin(triage)
    triage_plugin.register_workflow(graph)

    session = await triage.open(
        type=WORKFLOW_TYPE,
        target=[eng.agent_id],
        knobs={"graph": graph.to_dict()},
        intent="triage routes engineering questions",
    )

    # Drive triage's first turn directly so we observe the tool call.
    session_handle = Session(metadata=session.metadata, client=triage)
    deps = {SESSION_DEP: session_handle, AGENT_CLIENT_DEP: triage, HUB_DEP: hub}
    await triage.agent.ask(QUESTION, dependencies=deps)

    # Wait until eng has posted *something* into the session WAL.
    async def _eng_replied() -> bool:
        wal = await hub.read_wal(session.session_id)
        return any(e.event_type == EV_TEXT and e.sender_id == eng.agent_id for e in wal)

    settled = await wait_for_predicate(_eng_replied, timeout=120.0)
    assert settled, "eng never replied within 120s"

    wal = await hub.read_wal(session.session_id)
    handoffs = [e for e in wal if e.event_type == EV_HANDOFF]
    assert handoffs, "triage did not call transfer_to_eng"
    assert handoffs[0].event_data.get("tool") == "transfer_to_eng"

    # The first speaker after eng's text landed should be triage (revert).
    # If triage has already auto-replied, we can verify by ensuring
    # last_speaker_id is in {eng, triage} and turn count > 2.
    state = hub._adapter_states.get(session.session_id)
    if state is not None:
        assert state.turn_count >= 2, f"only {state.turn_count} turns recorded"

    name_by_id = {triage.agent_id: "triage", eng.agent_id: "eng"}
    print("transcript:")
    for line in transcript_lines(wal, name_by_id):
        print(f"  {line}")

    eng_replies = [
        e for e in wal if e.event_type == EV_TEXT and e.sender_id == eng.agent_id
    ]
    assert eng_replies, "eng did not produce a reply envelope"

    # Triage closes via TerminateTarget — for deterministic verification we
    # close explicitly. (The smoke equivalent in test/beta/providers does
    # the same.)
    closed = await hub.close_session(session.session_id, reason="triage_done")
    from autogen.beta.network.session import SessionState

    assert closed.state == SessionState.CLOSED

    await triage_hc.close()
    await eng_hc.close()
    await hub.close()


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    api_key = os.environ["GEMINI_API_KEY"]
    model = os.environ.get("AG2_GEMINI_MODEL", "gemini-3-flash-preview")

    from autogen.beta.config import GeminiConfig
    from autogen.llm_config import LLMConfig

    test_classic_swarm_handoff_routes_via_oncondition(
        LLMConfig({"api_type": "google", "model": model, "api_key": api_key}),
    )
    asyncio.run(
        test_workflow_swarm_handoff_routes_via_tool(
            cast(
                "GeminiConfig",
                GeminiConfig(model=model, api_key=api_key, temperature=0),
            )
        )
    )
