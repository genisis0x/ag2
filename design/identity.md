# Identity

A bare `Agent` is anonymous. Joining a hub requires three records:

- **`Passport`** — immutable identity + billing facts. Hub-stamped `agent_id` lives here. JSON.
- **`Resume`** — capability claims and observed track record. Mutable. JSON.
- **`SKILL.md`** — Anthropic-format usage doc. Optional but strongly recommended. Markdown.

The split mirrors three different readers, mutation rates, and trust models:

| Record | Reader | Mutation | Source | Carries |
|---|---|---|---|---|
| Passport | hub, billing, auth, routing | immutable for life of registration | self-attested at register | id, name, owner, provider/model, cost, region, auth |
| Resume | discovery, planning LLMs | mutates over time | self-claimed + hub-observed | claimed capabilities, domains, summary, examples, observed stats |
| SKILL.md | discovering LLMs (read into context) | rewritten by author | self-authored | LLM-facing usage doc with frontmatter |

Re-registering with the same `name` produces a new `agent_id` and a fresh passport. Resume and `SKILL.md` are passed at registration; resume mutates afterwards via hub-observed task outcomes and (optionally) explicit updates.

V1 ships only `agent`-kind participants. `HumanClient` / `AdminClient` are anticipated by the `NetworkClient` Protocol (see [clients.md](clients.md)) but live in separate FS namespaces (`humans/`, `admins/`) when they ship — V1 does not introduce a `kind` discriminator on `Passport`.

## Passport

```python
# autogen/beta/network/identity.py

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CostProfile:
    """Optional billing/routing hints. None of these fields are validated by V1."""

    input_per_mtok: float | None = None        # USD per million input tokens
    output_per_mtok: float | None = None       # USD per million output tokens
    latency_tier: str | None = None            # "fast" | "balanced" | "deep"


@dataclass(slots=True)
class AuthBlock:
    """How the hub validates this identity at the connection handshake."""

    scheme: str = "none"                       # "none" | "api_key" | future
    issuer: str | None = None
    audience: str | None = None
    key_fingerprint: str | None = None
    claim: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Passport:
    """Immutable identity + billing record for one registration.

    `agent_id` is hub-stamped at registration. Mutating any field requires
    unregister + re-register, which yields a fresh `agent_id`.
    """

    name: str                                  # human/LLM-facing address; unique per hub
    owner: str = ""                            # tenant / org
    provider: str | None = None                # "anthropic" | "openai" | None
    model: str | None = None
    cost: CostProfile | None = None
    region: str | None = None
    auth: AuthBlock = field(default_factory=AuthBlock)
    version: int = 1

    # Hub-stamped at registration. None on construction.
    agent_id: str | None = None
    created_at: str = ""                       # ISO-Z, hub-stamped
```

`name` is unique within a hub. `agent_id` is unique across time (UUID7 — timestamps recoverable from the id).

## Resume

```python
@dataclass(slots=True)
class ResumeExample:
    title: str
    outcome: str = ""                          # "completed" | "failed" | free-form
    task_id: str | None = None
    session_id: str | None = None
    when: str | None = None
    note: str = ""


@dataclass(slots=True)
class ObservedStat:
    """Hub-derived per-capability track record. Updated on terminal task events."""

    n: int = 0                                 # total observations
    completed: int = 0
    failed: int = 0
    expired: int = 0
    p50_latency_ms: int | None = None


@dataclass(slots=True)
class Resume:
    """Mutable capability claim + observed track record.

    Tenant code provides `claimed_capabilities`, `domains`, `summary`, and
    `examples` at registration. The hub mutates `observed` on terminal task
    events; tenant code may also replace the resume via `Hub.set_resume(...)`.
    """

    claimed_capabilities: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    summary: str = ""                          # one-line, indexed for `peers(action="find")`
    examples: list[ResumeExample] = field(default_factory=list)
    observed: dict[str, ObservedStat] = field(default_factory=dict)
    version: int = 1
    last_updated: str = ""                     # ISO-Z, hub-stamped on every mutation
```

Discovery (see `Hub.list_agents` in [hub.md](hub.md)) ranks against `claimed_capabilities ∪ observed.keys()`; ranking factors include `summary` text match, observed `success_rate = completed/n`, latency, and any cost weighting the caller passes via `sort_by`.

V1 ships with `observed = {}` and the hub recording terminal task outcomes from Phase 1 onwards. Discovery ranking by track record is best-effort in early V1 (zero data) and improves as the network runs.

## SKILL.md

`SKILL.md` is a Markdown file with [Anthropic Skill](https://docs.claude.com/en/api/skills) frontmatter. Storing the description in this format means a discovering LLM applies the same heuristic it already uses for harness skills.

```markdown
---
name: alice
description: Senior policy researcher specializing in cost/benefit analysis. Use when you need rigorous economic framing of policy tradeoffs.
---

## When to use me
- Multi-stakeholder cost analyses
- Distinguishing first-order vs externalized costs

## Inputs I expect
- Policy proposal as a single document or summary
- Stakeholder list with positions

## Examples
...
```

The frontmatter `description` is what `peers(action="find", query=...)` matches against. The body is what `peers(action="describe", name=...)` returns to the calling LLM verbatim.

If `SKILL.md` is omitted, `peers(action="describe")` falls back to a generated string from `Resume.summary` plus capability lists. Tenants can swap the fallback renderer via `AgentClient.skill_renderer = ...` (Phase 2).

## Runtime record

`runtime.json` lives next to the identity files but is owned and rewritten by the hub on every connection / heartbeat. Identity readers stay cache-friendly.

```python
# autogen/beta/network/hub/runtime.py

@dataclass(slots=True)
class AgentRuntime:
    agent_id: str
    binding: str                # "local" | "ws"
    target: str                 # opaque endpoint id (in-memory queue / ws connection id)
    reachable: bool
    last_heartbeat: str         # ISO-Z
```

The hub picks a transport from `runtime.binding` when dispatching envelopes. Local and WebSocket peers look identical to the caller.

Heartbeat cadence is transport-driven: `LocalLink` heartbeats are synchronous (every link operation refreshes `last_heartbeat`); `WsLink` (Phase 3) sends `ping` every `peer_heartbeat_timeout / 3` and the hub flips `reachable=false` after `peer_heartbeat_timeout` without a `pong`.

## Registration flow

1. Tenant constructs `Agent(name=..., config=..., ...)` — pure framework-core, no hub awareness.
2. Tenant opens a `HubClient` against a `Link` (`LocalLink(hub)` in V1; `WsLink("wss://hub/...")` in Phase 3).
3. Tenant builds `Passport(...)` and `Resume(...)`. `SKILL.md` is read from disk or supplied as a string.
4. Tenant calls `agent_client = await hub_client.register(agent, passport, resume, skill_md=None, rule=None)`.
5. Hub stamps `agent_id` (UUID7), persists `passport.json` + `resume.json` + optional `SKILL.md` + `rule.json`, opens the inbox cursor, attaches a `NetworkPlugin` to the Agent (so verbs become `agent.tools`), and returns the bound `AgentClient`.

The `AgentClient` holds the `Agent`, the passport, the resume, the rule, and the per-session-type notify handler registry. It runs the inbox loop on a background task. See [clients.md](clients.md).

## Resume mutation

Tenant-driven:

```python
await agent_client.set_resume(resume)        # full replace
await agent_client.add_example(example)      # append a ResumeExample
```

Hub-driven (automatic):

- On `EV_TASK_RESULT` / `EV_TASK_ERROR` / `EV_TASK_EXPIRED` for tasks owned by this agent, the hub increments `observed[capability].{n, completed/failed/expired}` and updates `p50_latency_ms`. The capability key is taken from `TaskSpec.payload.get("capability")` if present; tasks without a capability tag don't update observed stats.
- `last_updated` is bumped on every observation.

The LLM does **not** mutate its own resume. Resume mutation is a tenant decision, not an LLM tool surface, to avoid sycophancy-driven self-promotion.

## Unregistration

`await agent_client.unregister()` — closes the link, removes registry entries, deletes the inbox cursor, fans out an `agent.unregistered` event to active subscribers. Does not delete WAL of past sessions; closed sessions remain on disk. The unregistration is recorded in the hub's audit log (see [persistence.md](persistence.md)).

## Auth (V1 surface)

V1 ships `NoAuth` only. `ApiKeyAuth` lands in Phase 3 alongside the WebSocket transport. The Protocol stays open so further schemes ship additively.

```python
# autogen/beta/network/auth.py

class AuthAdapter(Protocol):
    scheme: str
    async def validate(self, passport: Passport, claim: dict) -> None: ...


class NoAuth:
    scheme = "none"
    async def validate(self, passport, claim): pass


class AuthRegistry:
    def __init__(self, adapters: list[AuthAdapter]) -> None: ...
    def get(self, scheme: str) -> AuthAdapter: ...

default_registry = AuthRegistry([NoAuth()])
```

## Phase 3 additions

- `ApiKeyAuth` — validates `claim['api_key']` against `passport.auth.key_fingerprint` with a constant-time SHA-256 compare.
- `dev_registry = AuthRegistry([NoAuth(), ApiKeyAuth()])` — dev convenience that accepts both.
- Auth runs at the WS `hello` frame and at the HTTP front door.

JWT, mTLS, and signed-challenge schemes are post Phase 4 — they're managed-deployment concerns beyond the framework-core surface.
