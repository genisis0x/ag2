# Transport

`Link` is the wire abstraction between `HubClient` and `Hub`. V1 ships `LocalLink` (in-memory duplex queue, single process). Phase 3 ships `WsLink` (WebSocket) with the same `Link` Protocol — the rest of the stack does not change.

## Link Protocol

```python
# autogen/beta/network/transport/link.py

from typing import Protocol, AsyncIterator


class LinkClient(Protocol):
    """Tenant-side handle to the hub."""

    async def open(self) -> None:
        """Connect, perform `hello`, await `welcome`."""

    async def send_frame(self, frame: Frame) -> None: ...

    def frames(self) -> AsyncIterator[Frame]:
        """Stream of inbound frames from the hub."""

    async def close(self) -> None: ...


class LinkEndpoint(Protocol):
    """Hub-side handle to one connected client."""

    endpoint_id: str
    agent_id: str | None      # set after `hello`

    async def send_frame(self, frame: Frame) -> None: ...
    def frames(self) -> AsyncIterator[Frame]: ...
    async def close(self) -> None: ...
```

V1 ships one `LinkClient` implementation: `LocalLinkClient`. Phase 3 adds `WsLinkClient`. Both produce frames that `decode_frame()` parses identically.

## Frame vocabulary

| Frame | Direction | Purpose | V1? |
|---|---|---|---|
| `hello` | client → hub | Authenticate (`identity.auth.claim`) | yes |
| `welcome` | hub → client | Auth ok; carries hub clock | yes |
| `ping` / `pong` | both | Heartbeat. Cadence: `Rule.limits.peer_heartbeat_timeout / 3` (default 10s). Hub flips `runtime.reachable=false` after `peer_heartbeat_timeout` (default 30s) without a pong. `LocalLink` skips the wire ping — `last_heartbeat` is refreshed synchronously on every link operation. | yes |
| `send` | client → hub | Post envelope into a session | yes |
| `accept` | hub → client | Accept a `send`; carries `envelope_id` | yes |
| `error` | hub → client | Reject with structured `code` + `message` | yes |
| `notify` | hub → client | Deliver an envelope | yes |
| `receipt` | client → hub | Ack or nack a notify | yes |
| `subscribe` | client → hub | Open a push subscription, optional `since` cursor | yes |
| `unsubscribe` | client → hub | Close subscription | yes |
| `event` | hub → client | Subscription delivery | yes |
| `chunk` | both | Streaming token (transient, not persisted to WAL) | yes |
| `network_changed` | hub → client | Cache-invalidation push for peer/capability lookups | Phase 3 |
| `rule_changed` | hub → client | Push updated rule (transforms portion) | post-V1 |

Frames are dataclasses in `autogen/beta/network/transport/frames.py` with `to_dict` / `from_dict` round-trip. JSON-line wire encoding when serialised.

## LocalLink (V1)

`LocalLink` is an in-memory duplex that runs the hub's connection handler as a background task per connection. Same frame vocabulary, same `LinkClient` / `LinkEndpoint` Protocols. Tests run real protocol traffic without a network socket — vital for a fast V1 test loop.

```python
# autogen/beta/network/transport/local.py

class LocalLink:
    """Bridges a HubClient to an in-process Hub via in-memory queues."""

    def __init__(self, hub: "Hub") -> None: ...

    def client(self) -> LocalLinkClient:
        """One LinkClient per HubClient (i.e., per tenant connection to this Hub)."""
```

## Streaming chunks

Streaming uses `chunk` frames carrying `(envelope_id, sender_id, audience, content_delta)`. The hub validates the sender, fans out to listed recipients (or all participants for broadcast), and does NOT persist chunks to the WAL — they are transient. Each `AgentClient` buffers chunks per envelope until the receiver opens the iterator.

`Session.send_chunk(envelope_id, content_delta)` and `Session.iter_chunks(envelope_id) -> AsyncIterator[str]` are the public API. View policies do not see in-progress chunks; they project finalized envelopes only. The chunk frames are wire-level and not exposed as LLM tools.

## Phase 2.0 — In-process cursor + replay

The `receipt` frame is wired to advance the per-agent `inbox.cursor`. The default notify handler issues a Receipt only after the handler completes (or after the dedup query finds a prior reply). On `hello`, hub replays unacked envelopes from the cursor up to the WAL head; the `AgentClient` additionally calls `Hub.pending_turns_for(self.agent_id)` to wake up unfinished turns whose triggering envelope had already been ack'd in a prior session (see [failure_modes.md](failure_modes.md) mode 11).

These run over `LocalLink` for V1 in-process; Phase 3 exercises the same semantics on `WsLink`.

## Phase 3 — WsLink

Same `Link` Protocol, WebSocket-backed. Adds:

- `hello` / `welcome` includes auth claim validation
- `chunk` frames ride the existing connection as JSON frames (no separate subprotocol — the implementation found no reason for one)

### Reconnect with cursor replay

`HelloFrame` is the reconnect anchor. The hub keeps a per-agent `inbox.cursor` (the highest-acked envelope position) and replays everything past it on each successful `hello`/`welcome` exchange.

1. Client (re-)opens the WebSocket and sends `HelloFrame(name=...)`.
2. Hub binds the endpoint to the existing `agent_id`, sends `WelcomeFrame`, then replays unacked notifies as fresh `NotifyFrame`s in WAL order.
3. `find_envelope_by_causation` makes redelivery idempotent on the handler side — duplicate replies are the dedup short-circuit, not the application's problem.

Receipts are wired on every transport (including `LocalLink`) for code-path uniformity, but `LocalLink` has no reconnect event so the replay path is exercised on `WsLink` only.

`SubscribeFrame.since_envelope_id`-based replay is **not** wired in M3 — the subscribe/event surface isn't a load-bearing client concern yet, and Hello-driven replay covers the resume story. Re-open if subscribe semantics grow.

### NetworkChangedFrame

Peer/capability lookups (`list_agents`, `get_resume`, `get_skill`) become real round-trips when the link crosses processes. `NetworkChangedFrame(kind, agent_id)` is a hub-pushed cache-invalidation signal:

* `kind`: `agent_registered` | `agent_unregistered` | `resume_set` | `skill_set`
* `agent_id`: the affected identity

`HubClient` keeps a small TTL'd cache around the discovery passthroughs and invalidates on inbound `NetworkChangedFrame`. `NetworkContextPolicy` is unchanged — it doesn't depend on peer state today.

## Phase 3 — HTTP surface

The HTTP CRUD surface (10 endpoints, mounted at `/v1/*`) lives in `autogen/beta/network/http/` and ships in Phase 3. V1 has no HTTP — all interaction goes through `LocalLink` frames.

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/agents` | Register (body: `{identity, rule, skill_md?}`) |
| DELETE | `/v1/agents/{id}` | Unregister |
| GET | `/v1/agents` | List with `?capability=&query=&limit=` |
| GET | `/v1/agents/{id}` | Describe (returns identity + skill_md) |
| PUT | `/v1/agents/{id}/rule` | Replace rule |
| POST | `/v1/sessions` | Create session |
| GET | `/v1/sessions/{id}` | Get metadata |
| GET | `/v1/sessions/{id}/wal?since=&until=` | Read WAL slice |
| POST | `/v1/sessions/{id}/close` | Close |
| GET | `/v1/admin/health` | Liveness |

`build_app(hub: Hub) -> Starlette` returns a Starlette app mountable in any larger ASGI project. `HttpServer(hub, host, port)` wraps it with uvicorn for standalone hosting. `starlette` and `uvicorn` are lazy-imported so the network package stays install-optional.

Anything beyond these ten — metrics, archival, force-close, list-sessions, knowledge bridge, task endpoints — is out of scope for framework-core (post Phase 4).
