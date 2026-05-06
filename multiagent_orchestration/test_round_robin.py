"""Round-robin parity: ``RoundRobinPattern`` vs ``TransitionGraph.round_robin``.

Three agents take strict round-robin turns on the same prompt. Both
implementations should:

1. Visit each agent in deterministic order (alice, bob, carol).
2. Terminate after one full cycle (max_turns / max_rounds == 3).
3. Produce a non-empty contribution from every agent.

We assert the **selection order** in both backends; we don't compare
text content because two different runtimes won't produce the same
phrasing even at temperature 0.
"""

import os
from typing import cast

import pytest

from autogen.agentchat import ConversableAgent
from autogen.agentchat.group.multi_agent_chat import initiate_group_chat
from autogen.agentchat.group.patterns import RoundRobinPattern
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

PROMPT = (
    "Quick debate: should type hints be mandatory in new Python projects? "
    "Reply in exactly one short sentence."
)


@pytest.mark.gemini
def test_classic_round_robin_visits_each_agent_once(classic_llm_config) -> None:
    print_section("CLASSIC RoundRobinPattern (Gemini)")

    def _make(name: str, system: str) -> ConversableAgent:
        return ConversableAgent(
            name=name,
            system_message=system,
            llm_config=classic_llm_config,
            human_input_mode="NEVER",
        )

    alice = _make(
        "alice",
        "You are alice. When it is your turn, give your opinion in one short sentence.",
    )
    bob = _make(
        "bob",
        "You are bob. When it is your turn, give your opinion in one short sentence.",
    )
    carol = _make(
        "carol",
        "You are carol. When it is your turn, give your opinion in one short sentence.",
    )

    pattern = RoundRobinPattern(initial_agent=alice, agents=[alice, bob, carol])
    chat_result, _ctx, _last = initiate_group_chat(
        pattern=pattern,
        messages=PROMPT,
        max_rounds=4,  # 1 user seed + 3 agent turns
    )

    history = chat_result.chat_history
    speakers = [
        m.get("name") for m in history if m.get("name") in {"alice", "bob", "carol"}
    ]
    print("speakers:", speakers)
    for m in history:
        who = m.get("name", "?")
        body = (m.get("content") or "").replace("\n", " ")[:120]
        print(f"  {who}: {body}")

    assert speakers[:3] == ["alice", "bob", "carol"], f"unexpected order: {speakers}"


@pytest.mark.gemini
@pytest.mark.asyncio
async def test_workflow_round_robin_visits_each_agent_once(beta_gemini_config) -> None:
    print_section("WORKFLOW round_robin (Gemini)")

    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
    )
    link = LocalLink(hub)

    names = ["alice", "bob", "carol"]
    clients = []
    for name in names:
        agent = Agent(
            name=name,
            prompt=(
                f"You are {name}, a participant in a 3-way discussion. "
                "When it is your turn call say(content=<your one-sentence "
                "opinion>). Do not call any other tool. Do not ask questions."
            ),
            config=beta_gemini_config,
        )
        hc = HubClient(link, hub=hub)
        client = await hc.register(agent, Passport(name=name), Resume())
        clients.append(client)

    alice, bob, carol = clients
    name_by_id = {c.agent_id: c.passport.name for c in clients}

    graph = TransitionGraph.round_robin(
        [alice.agent_id, bob.agent_id, carol.agent_id],
        max_turns=3,
    )
    session = await alice.open(
        type=WORKFLOW_TYPE,
        target=[bob.agent_id, carol.agent_id],
        knobs={"graph": graph.to_dict()},
        intent="round-robin debate",
    )

    # Alice's seed turn — counts as turn 1 in the workflow.
    await session.send(PROMPT)

    count = await wait_for_substantive_count(
        hub, session.session_id, expected=3, timeout=120.0
    )
    wal = await hub.read_wal(session.session_id)
    lines = transcript_lines(wal, name_by_id)
    print(f"substantive count: {count}")
    for line in lines:
        print(f"  {line}")

    speakers = [name_by_id[e.sender_id] for e in wal if e.event_type == EV_TEXT][:3]
    assert speakers == ["alice", "bob", "carol"], f"unexpected order: {speakers}"

    contributions = [
        e.event_data.get("text", "") for e in wal if e.event_type == EV_TEXT
    ][:3]
    for who, text in zip(speakers, contributions):
        assert len(text) > 5, f"{who}'s turn was empty/trivial: {text!r}"

    for c in clients:
        await c._hub_client.close()
    await hub.close()


if __name__ == "__main__":
    # Allow ad-hoc invocation outside pytest (still requires Gemini key).
    import asyncio

    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    api_key = os.environ["GEMINI_API_KEY"]
    model = os.environ.get("AG2_GEMINI_MODEL", "gemini-3-flash-preview")

    from autogen.beta.config import GeminiConfig
    from autogen.llm_config import LLMConfig

    test_classic_round_robin_visits_each_agent_once(
        LLMConfig({"api_type": "google", "model": model, "api_key": api_key}),
    )
    asyncio.run(
        test_workflow_round_robin_visits_each_agent_once(
            cast(
                "GeminiConfig",
                GeminiConfig(model=model, api_key=api_key, temperature=0),
            )
        )
    )
