"""Sequential pipeline parity: classic ``set_after_work`` chain vs
``TransitionGraph.sequence``.

Three agents form a research → write → review pipeline:
* researcher gathers facts;
* writer turns them into a paragraph;
* reviewer signs off in one sentence.

Each agent runs **once**, in the order they were declared, then the
session terminates.
"""

import os
from typing import cast

import pytest

from autogen.agentchat import ConversableAgent
from autogen.agentchat.group.multi_agent_chat import initiate_group_chat
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
    EV_TEXT,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.adapters.workflow import WORKFLOW_TYPE
from autogen.beta.network.transitions import TransitionGraph
from multiagent_orchestration._helpers import (
    print_section,
    transcript_lines,
    wait_for_substantive_count,
)

PROMPT = "Topic: one key benefit of test-driven development."


@pytest.mark.gemini
def test_classic_sequential_pipeline(classic_llm_config) -> None:
    print_section("CLASSIC sequential pipeline (Gemini)")

    researcher = ConversableAgent(
        name="researcher",
        system_message=(
            "You are the researcher. State two crisp facts (one sentence each) "
            "about the requested topic. Do not write a paragraph yourself."
        ),
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )
    writer = ConversableAgent(
        name="writer",
        system_message=(
            "You are the writer. Use the researcher's facts to compose a single "
            "short paragraph (<=3 sentences) that summarises the benefit."
        ),
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )
    reviewer = ConversableAgent(
        name="reviewer",
        system_message=(
            "You are the reviewer. Reply with exactly one sentence beginning "
            "'Approved:' that endorses the writer's paragraph. Do not rewrite it."
        ),
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )

    researcher.handoffs.set_after_work(target=ClassicAgentTarget(agent=writer))
    writer.handoffs.set_after_work(target=ClassicAgentTarget(agent=reviewer))
    reviewer.handoffs.set_after_work(target=ClassicTerminateTarget())

    pattern = DefaultPattern(
        initial_agent=researcher,
        agents=[researcher, writer, reviewer],
        group_after_work=ClassicTerminateTarget(),
    )
    chat_result, _ctx, _last = initiate_group_chat(
        pattern=pattern,
        messages=PROMPT,
        max_rounds=4,  # 1 user seed + 3 agent turns
    )

    history = chat_result.chat_history
    speakers = [
        m.get("name")
        for m in history
        if m.get("name") in {"researcher", "writer", "reviewer"}
    ]
    print("speakers:", speakers)
    for m in history:
        who = m.get("name", "?")
        body = (m.get("content") or "").replace("\n", " ")[:160]
        print(f"  {who}: {body}")

    assert speakers == [
        "researcher",
        "writer",
        "reviewer",
    ], f"unexpected order: {speakers}"


@pytest.mark.gemini
@pytest.mark.asyncio
async def test_workflow_sequential_pipeline(beta_gemini_config) -> None:
    print_section("WORKFLOW sequence pipeline (Gemini)")

    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
    )
    link = LocalLink(hub)

    role_prompts = {
        "researcher": (
            "You are the researcher. When it is your turn, call "
            "say(content=<two crisp facts about the topic, one per "
            "sentence>). Do not call any other tool."
        ),
        "writer": (
            "You are the writer. When it is your turn, call "
            "say(content=<one short paragraph summarising the topic in "
            "<=3 sentences>). Do not call any other tool."
        ),
        "reviewer": (
            "You are the reviewer. When it is your turn, call "
            "say(content=<exactly one sentence beginning 'Approved:'>)."
            " Do not call any other tool."
        ),
    }

    clients = []
    for name, prompt in role_prompts.items():
        agent = Agent(name=name, prompt=prompt, config=beta_gemini_config)
        hc = HubClient(link, hub=hub)
        client = await hc.register(agent, Passport(name=name), Resume())
        clients.append(client)

    researcher, writer, reviewer = clients
    name_by_id = {c.agent_id: c.passport.name for c in clients}

    graph = TransitionGraph.sequence(
        [researcher.agent_id, writer.agent_id, reviewer.agent_id],
    )
    session = await researcher.open(
        type=WORKFLOW_TYPE,
        target=[writer.agent_id, reviewer.agent_id],
        knobs={"graph": graph.to_dict()},
        intent="research → write → review",
    )

    # researcher is the initial speaker; their seed text is turn 1.
    await session.send(PROMPT)

    count = await wait_for_substantive_count(
        hub, session.session_id, expected=3, timeout=180.0
    )
    wal = await hub.read_wal(session.session_id)
    lines = transcript_lines(wal, name_by_id)
    print(f"substantive count: {count}")
    for line in lines:
        print(f"  {line}")

    speakers = [name_by_id[e.sender_id] for e in wal if e.event_type == EV_TEXT][:3]
    assert speakers == ["researcher", "writer", "reviewer"], f"order broken: {speakers}"

    contributions = [
        e.event_data.get("text", "") for e in wal if e.event_type == EV_TEXT
    ][:3]
    assert (
        "Approved:" in contributions[2]
    ), f"reviewer didn't follow protocol; got {contributions[2]!r}"

    for c in clients:
        await c._hub_client.close()
    await hub.close()


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    api_key = os.environ["GEMINI_API_KEY"]
    model = os.environ.get("AG2_GEMINI_MODEL", "gemini-3-flash-preview")

    from autogen.beta.config import GeminiConfig
    from autogen.llm_config import LLMConfig

    test_classic_sequential_pipeline(
        LLMConfig({"api_type": "google", "model": model, "api_key": api_key}),
    )
    asyncio.run(
        test_workflow_sequential_pipeline(
            cast(
                "GeminiConfig",
                GeminiConfig(model=model, api_key=api_key, temperature=0),
            )
        )
    )
