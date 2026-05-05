# Views

Naive multi-participant sessions break for three reasons all at once: token cost grows quadratically with turns, addressing is all-or-nothing, and the LLM degrades on long transcripts. V1 ships three primitives that, composed, let multi-agent sessions scale beyond the single-orchestrator pattern.

## The three layers

```
┌────────────────────────────────────────────┐
│  Layer A — WAL (durable truth, hub-owned)  │
│  Append-only envelope log. Records every-  │
│  thing including subset-addressed sends.   │
└──────────────────┬─────────────────────────┘
                   │
                   │  ViewPolicy.project(wal, participant_id, metadata)
                   ▼
┌────────────────────────────────────────────┐
│  Layer B — Per-participant view (turn)     │
│  Curated slice the LLM sees on this turn.  │
│  Pure function of WAL + participant id.    │
└──────────────────┬─────────────────────────┘
                   │
                   │  prepended to current ModelRequest
                   ▼
┌────────────────────────────────────────────┐
│  Layer C — Per-participant working memory  │
│  Carried across turns AND across sessions. │
│  Lives in Agent's KnowledgeStore.          │
│  Already framework-core.                   │
└────────────────────────────────────────────┘
```

A participant's effective LLM context for a turn is `[layer_C_summary, ...layer_B_projection, current_envelope]`.

## ViewPolicy Protocol

```python
# autogen/beta/network/views/base.py

from typing import Protocol, runtime_checkable
from autogen.beta.events import BaseEvent


@runtime_checkable
class ViewPolicy(Protocol):
    name: str

    async def project(
        self,
        wal: list[Envelope],
        *,
        participant_id: str,
        session: SessionMetadata,
    ) -> list[BaseEvent]:
        """Convert the WAL slice this participant should see into model events.

        Pure function. Called once per turn before the participant's LLM call.
        Translates Envelopes into BaseEvents (ModelRequest for inbound,
        ModelMessage for own-past-turns) and returns them in chronological
        order. The current turn's ModelRequest is appended by the caller.
        """
```

## Built-in view policies (V1)

`autogen/beta/network/views/builtin.py`

| Policy | Behavior | Use case |
|---|---|---|
| `FullTranscript()` | Translate every envelope visible to `participant_id` (per `visible_to`). | Small sessions: consulting, ≤3-party discussion. |
| `WindowedSummary(recent_n=10, summary=SummarizeCompact())` | Last N visible envelopes verbatim + LLM-summarized tail of older history. Calls framework-core `CompactStrategy.compact()` for the summary. | Long conversations and discussions. |
| `Composite([policies], merge="concat" \| "first_nonempty")` | Compose multiple policies. | Custom flows. |

`WindowedSummary` is the workhorse for long-running sessions. It composes with framework-core `compact.py` so the same `SummarizeCompact` strategy that compresses an Agent's local history also compresses the session view tail — no duplicate summarisation logic.

`PreviousOnly` (static-pipeline discussions) and `BySpeaker` (filtered-by-speaker view) ship in Phase 4 only on demand. They are short enough that user code can express them via `Composite` (Phase 2.1) until then.

`OwnAndDirected` from the original design is just `FullTranscript` over WAL pre-filtered by `visible_to` — no separate policy needed.

## Visibility filter

```python
# autogen/beta/network/envelope.py

def visible_to(envelope: Envelope, participant_id: str) -> bool:
    if envelope.sender_id == participant_id:
        return True
    if envelope.audience is None:
        return True                            # broadcast
    return participant_id in envelope.audience
```

Eligibility is computed before `ViewPolicy.project()`. The hub honors this at delivery time — non-eligible peers do not receive a `notify` for envelopes addressed to a subset. The WAL still records the full envelope (audit). View policies only project envelopes that pass `visible_to`.

## Lookup verbs

When the projection is too narrow but the LLM needs older or other-speaker context, it calls a lookup tool. Two are exposed via the `context(...)` grouped tool (see [network_plugin.md](network_plugin.md)):

- `context(action="search", query, scope="session"|"knowledge")` — substring + token match in V1; vector search in a follow-up
- `context(action="quote", speaker, recent_n)` — pull the last N envelopes from a specific peer in the session

These let the LLM keep its turn projection bounded while still being able to reach into older history on demand.

## Working memory (Layer C)

Layer C lives entirely in framework-core. An Agent constructed with `knowledge=KnowledgeConfig(store=..., aggregate=WorkingMemoryAggregate(...))` accumulates working memory turn over turn via the existing assembly chain. The network does not introduce a parallel mechanism; `context(action="search", scope="knowledge")` reads from the same `KnowledgeStore`.

Working memory persists across sessions: what an Agent learned in session A is available in session B because both share the Agent's `KnowledgeStore`.

`scope="knowledge"` reads the **calling agent's own** knowledge — not a shared team store, not peers' stores. Cross-agent knowledge sharing is out of scope for framework-core (post Phase 4); an agent that needs another's knowledge opens a session and asks. The tool description makes this explicit so the LLM doesn't assume "knowledge" is shared.

## Adapter-default view policies

Each adapter declares `default_view_policy(metadata, participant_id) -> ViewPolicy`:

| Adapter | Default |
|---|---|
| `consulting` | `FullTranscript()` |
| `conversation` | `WindowedSummary(recent_n=10, summary=SummarizeCompact())` |
| `discussion` (any ordering) | `WindowedSummary(recent_n=N*2, summary=SummarizeCompact())` where N = participant count |

Tenants override per-participant by passing `view_policy=...` to `agent_client.open(...)` or `client.sessions(action="open", view_policy=...)`. They can also register policies on the Agent's framework-core assembly chain that compose with the network's per-turn projection.
