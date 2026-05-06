"""Custom-handoff parity: classic ``GroupChat(speaker_selection_method=callable)``
vs workflow with a **user-registered** ``TransitionCondition``.

Demonstrates that "the user controls how to proceed between agents":

* **Classic** uses raw ``GroupChat`` with a custom
  ``speaker_selection_method`` Python callable that inspects
  ``groupchat.messages`` and returns the next ``Agent``. Returning
  ``None`` terminates.
* **Workflow** uses ``register_condition`` to plug a ``TextContains``
  predicate into the graph. The graph composes built-in vocabulary
  (``FromSpeaker``, ``AgentTarget``, ``TerminateTarget``) with the
  custom condition — no adapter / hub changes required.

Scenario: a writer drafts a haiku; a reviewer either rejects with
feedback or approves. The reviewer is instructed to reject the first
draft and approve the revision. Routing must:

1. Send writer → reviewer after every writer turn.
2. Loop reviewer → writer when reviewer's text contains ``REJECT``.
3. Terminate when reviewer's text contains ``APPROVE``.

This is the smallest example of "agent-routing logic the framework
doesn't ship." Every other pattern reuses it through different
combinators.
"""

import os
from dataclasses import dataclass
from typing import Any, ClassVar, cast

import pytest

from autogen.agentchat import ConversableAgent
from autogen.agentchat.groupchat import GroupChat, GroupChatManager
from autogen.beta import Agent
from autogen.beta.knowledge import MemoryKnowledgeStore
from autogen.beta.network import (
    EV_TEXT,
    Envelope,
    Hub,
    HubClient,
    LocalLink,
    Passport,
    Resume,
)
from autogen.beta.network.adapters.workflow import WORKFLOW_TYPE
from autogen.beta.network.client.session import Session
from autogen.beta.network.policies import (
    AGENT_CLIENT_DEP,
    HUB_DEP,
    SESSION_DEP,
)
from autogen.beta.network.transitions import (
    AgentTarget,
    FromSpeaker,
    TerminateTarget,
    Transition,
    TransitionGraph,
    register_condition,
)
from multiagent_orchestration._helpers import (
    print_section,
    transcript_lines,
    wait_for_predicate,
)

# ── Custom workflow condition (user-supplied) ───────────────────────────────


@dataclass(slots=True)
class TextContains:
    """User-defined ``TransitionCondition``: fires when the just-accepted
    text envelope's body contains ``keyword`` (case-insensitive).

    Lives in user code, registered via ``register_condition`` once at
    import time. Survives ``TransitionGraph.dumps`` / ``loads`` round
    trips because the registry resolves it by ``name``.
    """

    keyword: str
    name: ClassVar[str] = "text_contains"

    def evaluate(self, _state: Any, envelope: Envelope) -> bool:
        if envelope.event_type != EV_TEXT:
            return False
        text = (envelope.event_data or {}).get("text", "")
        if not isinstance(text, str):
            return False
        return self.keyword.upper() in text.upper()


# Idempotent — calling ``register_condition`` again replaces the prior
# entry with the same name.
register_condition(TextContains)


# ── Shared prompts ──────────────────────────────────────────────────────────


WRITER_ROLE = (
    "You are a haiku writer. Produce a 3-line haiku about the "
    "requested topic. If the previous message starts with 'REJECT', "
    "write a different haiku that addresses the feedback."
)

REVIEWER_ROLE = (
    "You are the haiku reviewer. Look at the conversation so far. "
    "If this is the FIRST haiku you are reviewing in this thread "
    "(i.e. you have not yet replied 'REJECT' in this conversation), "
    "respond exactly: REJECT: please use stronger imagery. "
    "If you have already replied 'REJECT' once and the writer has "
    "revised, respond exactly: APPROVE. Use those exact strings."
)

# Workflow agents must invoke the ``say`` tool so the message lands in
# the session WAL — see the framework note in the README.
WORKFLOW_TOOL_DIRECTIVE = (
    " When it is your turn, call ONLY the say tool with the response "
    "above as ``content``. Output nothing else — no explanation, no "
    "additional text before or after the tool call."
)


WRITER_PROMPT = WRITER_ROLE + WORKFLOW_TOOL_DIRECTIVE
REVIEWER_PROMPT = REVIEWER_ROLE + WORKFLOW_TOOL_DIRECTIVE

INITIAL_TASK = "Topic: an autumn morning."


# ── Classic via raw GroupChat + custom speaker_selection_method ─────────────


@pytest.mark.gemini
def test_classic_custom_speaker_selection(classic_llm_config) -> None:
    print_section("CLASSIC GroupChat custom speaker_selection_method (Gemini)")

    writer = ConversableAgent(
        name="writer",
        system_message=WRITER_ROLE,
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )
    reviewer = ConversableAgent(
        name="reviewer",
        system_message=REVIEWER_ROLE,
        llm_config=classic_llm_config,
        human_input_mode="NEVER",
    )

    def pick_next(
        last_speaker: ConversableAgent, gc: GroupChat
    ) -> ConversableAgent | None:
        # Initiator is the writer; first hop is always to reviewer.
        if last_speaker is writer:
            return reviewer
        if last_speaker is reviewer:
            last_text = ""
            for m in reversed(gc.messages):
                if m.get("name") == "reviewer":
                    last_text = (m.get("content") or "").upper()
                    break
            if "APPROVE" in last_text:
                return None  # terminate
            if "REJECT" in last_text:
                return writer
        return None

    chat = GroupChat(
        agents=[writer, reviewer],
        messages=[],
        max_round=8,
        speaker_selection_method=pick_next,
    )
    manager = GroupChatManager(groupchat=chat, llm_config=classic_llm_config)

    chat_result = writer.initiate_chat(manager, message=INITIAL_TASK)

    history = chat_result.chat_history
    speakers = [
        m.get("name") for m in history if m.get("name") in {"writer", "reviewer"}
    ]
    print("speakers:", speakers)
    for m in history:
        who = m.get("name", "?")
        body = (m.get("content") or "").replace("\n", " ")[:140]
        print(f"  {who}: {body}")

    # Reviewer must have spoken at least once.
    assert "reviewer" in speakers, "reviewer never engaged"
    # The reject loop should produce at least 2 writer turns (initial +
    # one revision) before approve.
    writer_turns = sum(1 for s in speakers if s == "writer")
    reviewer_msgs = [
        (m.get("content") or "") for m in history if m.get("name") == "reviewer"
    ]
    saw_reject = any("REJECT" in t.upper() for t in reviewer_msgs)
    saw_approve = any("APPROVE" in t.upper() for t in reviewer_msgs)
    assert (
        saw_reject or saw_approve
    ), f"reviewer didn't follow protocol; replies={reviewer_msgs}"
    if saw_reject and saw_approve:
        assert (
            writer_turns >= 2
        ), f"saw reject+approve but only {writer_turns} writer turns"


# ── Workflow via custom TransitionCondition ─────────────────────────────────


@pytest.mark.gemini
@pytest.mark.asyncio
async def test_workflow_custom_text_contains_condition(beta_gemini_config) -> None:
    print_section("WORKFLOW custom TransitionCondition (Gemini)")

    hub = await Hub.open(
        MemoryKnowledgeStore(),
        ttl_sweep_interval=0,
        expectation_sweep_interval=0,
    )
    link = LocalLink(hub)

    writer_hc = HubClient(link, hub=hub)
    reviewer_hc = HubClient(link, hub=hub)

    writer_agent = Agent(
        name="writer",
        prompt=WRITER_PROMPT,
        config=beta_gemini_config,
    )
    reviewer_agent = Agent(
        name="reviewer",
        prompt=REVIEWER_PROMPT,
        config=beta_gemini_config,
    )
    writer = await writer_hc.register(writer_agent, Passport(name="writer"), Resume())
    reviewer = await reviewer_hc.register(
        reviewer_agent, Passport(name="reviewer"), Resume()
    )

    graph = TransitionGraph(
        initial_speaker=writer.agent_id,
        transitions=[
            # priority 0: after writer speaks, always go to reviewer
            Transition(
                when=FromSpeaker(writer.agent_id),
                then=AgentTarget(reviewer.agent_id),
                priority=0,
            ),
            # priority 1: reviewer says APPROVE → terminate
            Transition(
                when=TextContains("APPROVE"),
                then=TerminateTarget(reason="approved"),
                priority=1,
            ),
            # priority 2: reviewer says REJECT → loop back to writer
            Transition(
                when=TextContains("REJECT"),
                then=AgentTarget(writer.agent_id),
                priority=2,
            ),
        ],
        default_target=TerminateTarget(reason="exhausted"),
        max_turns=6,
    )

    # Round-trip the graph through dumps/loads to confirm the custom
    # condition survives serialisation via the named registry.
    restored = TransitionGraph.loads(graph.dumps())
    assert any(
        isinstance(t.when, TextContains) for t in restored.transitions
    ), "TextContains didn't survive registry round-trip"

    session = await writer.open(
        type=WORKFLOW_TYPE,
        target=[reviewer.agent_id],
        knobs={"graph": graph.to_dict()},
        intent="haiku draft + review loop with reject-retry",
    )

    # Drive writer's first turn (initial speaker) directly.
    session_handle = Session(metadata=session.metadata, client=writer)
    deps = {SESSION_DEP: session_handle, AGENT_CLIENT_DEP: writer, HUB_DEP: hub}
    await writer.agent.ask(INITIAL_TASK, dependencies=deps)

    # Wait until either approval lands OR max_turns is reached. Both
    # leave the session in CLOSED state.
    async def _terminal() -> bool:
        state = hub._adapter_states.get(session.session_id)
        if state is not None and state.expected_next_speaker is None:
            return True
        meta = await hub.get_session(session.session_id)
        return meta.is_terminal()

    settled = await wait_for_predicate(_terminal, timeout=180.0)

    wal = await hub.read_wal(session.session_id)
    name_by_id = {writer.agent_id: "writer", reviewer.agent_id: "reviewer"}
    state = hub._adapter_states.get(session.session_id)
    print(
        f"settled={settled} expected_next_speaker={state and state.expected_next_speaker} "
        f"turn_count={state and state.turn_count}"
    )
    print("transcript:")
    for line in transcript_lines(wal, name_by_id):
        print(f"  {line}")
    assert settled, "workflow never terminated (see transcript above)"

    text_envelopes = [e for e in wal if e.event_type == EV_TEXT]
    speakers = [name_by_id[e.sender_id] for e in text_envelopes]
    reviewer_texts = [
        (e.event_data or {}).get("text", "")
        for e in text_envelopes
        if e.sender_id == reviewer.agent_id
    ]

    assert "reviewer" in speakers, "reviewer never engaged"
    saw_reject = any("REJECT" in t.upper() for t in reviewer_texts)
    saw_approve = any("APPROVE" in t.upper() for t in reviewer_texts)
    assert (
        saw_reject or saw_approve
    ), f"reviewer didn't follow protocol; replies={reviewer_texts}"

    if saw_reject and saw_approve:
        # Reject-loop fired: writer must have produced 2+ drafts.
        writer_count = sum(1 for s in speakers if s == "writer")
        assert (
            writer_count >= 2
        ), f"reject-loop didn't route back to writer; writer turns={writer_count}"

    final = await hub.get_session(session.session_id)
    if saw_approve:
        # APPROVE must have come from our custom TextContains transition.
        assert final.close_reason in (
            "approved",
            "max_turns",
        ), f"unexpected close_reason: {final.close_reason!r}"

    await writer_hc.close()
    await reviewer_hc.close()
    await hub.close()


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    api_key = os.environ["GEMINI_API_KEY"]
    model = os.environ.get("AG2_GEMINI_MODEL", "gemini-3-flash-preview")

    from autogen.beta.config import GeminiConfig
    from autogen.llm_config import LLMConfig

    test_classic_custom_speaker_selection(
        LLMConfig({"api_type": "google", "model": model, "api_key": api_key}),
    )
    asyncio.run(
        test_workflow_custom_text_contains_condition(
            cast(
                "GeminiConfig",
                GeminiConfig(model=model, api_key=api_key, temperature=0),
            )
        )
    )
