# Clients

V1 introduces a small protocol hierarchy:

- `NetworkClient` — abstract participant in a network. Any non-Agent participant kind plugs in here.
- `HubClient` — connection lifecycle, one per process per hub. Wraps a `Link`.
- `AgentClient` — per-registration handle, one per `(Agent, identity, hub)`. The V1 `NetworkClient` implementation backed by an `Agent`.

The split is the trust boundary. Tenant code (`Agent` user tools, future transforms, notify handlers) only runs inside the tenant process. The hub never imports tenant modules and never executes tenant callables.

```
┌─── tenant process ──────────────────────────────────┐
│                                                      │
│   Agent  ←──── notify handler ────  AgentClient ──┐  │
│                                                    │  │
│                                     AgentClient ──┤  │
│                                                    │  │
│                                       HubClient ──┘  │
│                                            │          │
└────────────────────────────────────────────┼──────────┘
                                             │   Link
                                             │
                                       ┌─────▼──────┐
                                       │    Hub     │
                                       └────────────┘
```

A single tenant process holds **one** `HubClient` per hub it connects to, and **one** `AgentClient` per identity it has registered through that connection.

## NetworkClient Protocol

```python
# autogen/beta/network/client/network_client.py

from typing import Protocol


class NetworkClient(Protocol):
    """A participant in a network. AgentClient is the V1 implementation
    backed by an Agent. Future HumanClient / AdminClient implement the
    same Protocol."""

    @property
    def agent_id(self) -> str: ...

    @property
    def passport(self) -> Passport: ...

    @property
    def resume(self) -> Resume: ...

    async def receive(self, envelope: Envelope) -> None:
        """Hub delivers an envelope to this participant. Implementations
        translate it into the local execution model (Agent.ask for
        AgentClient, queue push for HumanClient, etc.)."""

    async def open(
        self,
        type: str,
        target: str | list[str],
        *,
        ttl: str | int | None = None,
        knobs: dict | None = None,
        labels: dict[str, str] | None = None,
    ) -> Session:
        """Open a session of `type` with `target`."""

    async def disconnect(self) -> None: ...
```

This is the seam future participant types plug into:

| Impl | What it wraps | Lands |
|---|---|---|
| `AgentClient` | An `Agent` running an LLM loop | V1 |
| `HumanClient` | A queue + UI bridge | TBD |
| `AdminClient` | Operational tools, no LLM | TBD |

A custom client implementation does not need to inherit from `AgentClient` or vendor any of its internals — implementing the four members of `NetworkClient` is enough.

## HubClient

```python
# autogen/beta/network/client/hub_client.py

class HubClient:
    def __init__(self, link: Link, *, hub: "Hub | None" = None) -> None:
        """`hub` is the in-process Hub when using LocalLink; None for WsLink (Phase 3).
        The client transparently uses direct method calls vs frames as appropriate."""

    async def register(
        self,
        agent: Agent,
        passport: Passport,
        resume: Resume,
        *,
        skill_md: str | None = None,
        rule: Rule | None = None,
    ) -> AgentClient:
        """Stamp agent_id, persist passport + resume + optional SKILL.md + rule,
        attach NetworkPlugin to the Agent (so verbs become agent.tools), return
        the bound AgentClient."""

    async def list_agents(
        self, *,
        capability: str | None = None,
        query: str | None = None,
        sort_by: str | None = None,
        limit: int = 50,
    ) -> list[Passport]: ...

    async def get_agent(self, name_or_id: str) -> Passport: ...
    async def get_resume(self, agent_id: str) -> Resume: ...
    async def get_skill(self, agent_id: str) -> str | None: ...

    async def describe_network(self) -> NetworkMetadata:
        """Adapters available, peer count, my own state."""

    async def close(self) -> None:
        """Disconnect; existing AgentClients become no-ops on send."""

    async def shutdown(self) -> None:
        """Unregister every AgentClient, then close()."""
```

## AgentClient

```python
# autogen/beta/network/client/agent_client.py

class AgentClient:
    @property
    def agent(self) -> Agent: ...
    @property
    def passport(self) -> Passport: ...
    @property
    def resume(self) -> Resume: ...
    @property
    def agent_id(self) -> str: ...

    # NetworkClient impl
    async def receive(self, envelope: Envelope) -> None:
        """Hub delivers; AgentClient routes to the registered handler."""

    async def open(
        self,
        type: str,
        target: str | list[str],
        *,
        ttl: str | int | None = None,
        knobs: dict | None = None,
        labels: dict[str, str] | None = None,
        view_policy: ViewPolicy | None = None,
        intent: str | None = None,
    ) -> Session: ...

    # Handler registry — override the default per session type
    def on(self, session_type: str) -> Callable: ...

    def on_task(self, spec_type: str = "*") -> Callable: ...

    # Building blocks for custom handlers (used by default handler too)
    async def read_wal_until(self, envelope: Envelope) -> list[Envelope]: ...
    def resolve_view_policy(self, session: Session, envelope: Envelope) -> ViewPolicy: ...
    def stamp_dependencies(self, session: Session, envelope: Envelope) -> dict: ...

    # Discovery passthrough (used by `peers` tool)
    async def list_peers(self, **kwargs) -> list[Passport]: ...
    async def describe_peer(self, name_or_id: str) -> PeerDescription:
        """Returns {passport, resume, skill_md} — the LLM-facing peer profile.
        SKILL.md content is included when present; otherwise a generated
        fallback is rendered from resume.summary + capabilities."""

    # Identity mutation — tenant-driven; not exposed on the LLM tool surface
    async def set_resume(self, resume: Resume) -> None: ...
    async def add_example(self, example: ResumeExample) -> None: ...
    async def set_skill(self, skill_md: str | None) -> None: ...
    async def set_rule(self, rule: Rule) -> None: ...

    # Low-level
    async def inbox_iter(self) -> AsyncIterator[Envelope]:
        """For custom handlers that bypass the per-session-type registry."""

    async def disconnect(self) -> None: ...
    async def unregister(self) -> None: ...
```

## Default notify handlers

Per session type, the framework ships a default handler. The handler is decomposed into small public hooks so `@client.on("...")` overrides only need to replace what they care about — not re-implement the whole flow:

```python
# autogen/beta/network/client/handlers.py

async def default_handler(envelope: Envelope, client: AgentClient) -> None:
    session = await client._session(envelope.session_id)
    view_policy = client.resolve_view_policy(session, envelope)
    wal = await client.read_wal_until(envelope)
    projection = await view_policy.project(
        wal, participant_id=client.agent_id, session=session.metadata,
    )

    deps = client.stamp_dependencies(session, envelope)
    reply = await client.agent.ask(
        *projection,
        envelope_to_input(envelope),
        dependencies=deps,
    )
    if reply.body:
        await session.send(reply.body, causation_id=envelope.envelope_id)
```

The `NetworkPlugin` — attached at registration — already added the LLM verbs to `agent.tools`, so the handler doesn't need to inject them per turn. Tools resolve their bindings from the dependencies stamped by `stamp_dependencies`.

## Trust boundary recap

| What | Runs where | Why |
|---|---|---|
| `access`, `limits` | Hub | Cross-tenant, cross-call state |
| Notify handlers | Tenant process | Tenant code |
| Future transforms | Tenant process | Tenant business logic |
| LLM tool execution | Tenant process | Tenant code |

Even when running everything in one process for tests, the split is preserved: it is the trust model, not a deployment optimisation. A compromised or hostile hub cannot bypass tenant-side enforcement (when transforms ship in Phase 3).
