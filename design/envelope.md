# Envelope

Every message between Agents is an `Envelope`.

## Shape

```python
# autogen/beta/network/envelope.py

from dataclasses import dataclass, field
from typing import Any, Literal


Priority = Literal["background", "normal", "urgent"]


@dataclass(slots=True)
class Envelope:
    envelope_id: str                           # UUID7, hub-stamped on accept
    session_id: str
    sender_id: str
    audience: list[str] | None                 # None = broadcast within session
    event_type: str                            # stable name, e.g. "ag2.msg.text"
    event_data: dict[str, Any]                 # event-specific payload

    task_id: str | None = None                 # set on ag2.task.* events
    causation_id: str | None = None            # envelope this is responding to
    trace_id: str | None = None
    priority: Priority = "normal"
    depth: int = 0                             # delegation hop count
    idempotency_key: str | None = None         # client-provided dedup key (Phase 3)

    created_at: str = ""                       # ISO-Z, hub-stamped on accept
    ttl_seconds: int | None = None
```

`audience` replaces the older `recipient_ids` for clarity (`audience=None` reads as "everyone in session" naturally).

## Audience semantics

`audience` is the addressing primitive that lets context flow scale (see [views.md](views.md)).

| Value | Meaning | Hub delivery |
|---|---|---|
| `None` | Broadcast within session | Notify every participant except sender |
| `[bob]` | Single recipient | Notify only bob |
| `[bob, carol]` | Subset addressing | Notify only bob and carol |

The hub stores the full envelope in the WAL regardless of `audience` (audit + debug), but `notify` only lands on listed peers. View policies further filter what each recipient's LLM actually sees:

```python
# autogen/beta/network/envelope.py

def visible_to(envelope: Envelope, participant_id: str) -> bool:
    if envelope.sender_id == participant_id:
        return True
    if envelope.audience is None:
        return True                            # broadcast
    return participant_id in envelope.audience
```

## Event types

V1 ships a fixed set of stable event-type names. New names are added in code, not at runtime.

| Constant | Value | Purpose |
|---|---|---|
| `EV_TEXT` | `ag2.msg.text` | User-content text envelope |
| `EV_SESSION_INVITE` | `ag2.session.invite` | Hub → recipient on session create |
| `EV_SESSION_INVITE_ACK` | `ag2.session.invite.ack` | Recipient → hub |
| `EV_SESSION_INVITE_REJECT` | `ag2.session.invite.reject` | Recipient → hub |
| `EV_SESSION_OPENED` | `ag2.session.opened` | Hub broadcast on quorum reached |
| `EV_SESSION_CLOSED` | `ag2.session.closed` | Hub broadcast on close |
| `EV_SESSION_EXPIRED` | `ag2.session.expired` | Hub broadcast on TTL sweep |
| `EV_SESSION_IDLE` | `ag2.session.idle` | Hub: no envelopes for `session_idle_threshold` |
| `EV_SESSION_QUORUM_CHANGED` | `ag2.session.quorum_changed` | Hub: participant count changed in active session |
| `EV_TASK_STARTED` | `ag2.task.started` | Owner → observers (mirrored from agent's `TaskStarted` event) |
| `EV_TASK_PROGRESS` | `ag2.task.progress` | Owner → observers |
| `EV_TASK_RESULT` | `ag2.task.result` | Owner → observers, terminal |
| `EV_TASK_ERROR` | `ag2.task.error` | Owner → observers, terminal |
| `EV_TASK_EXPIRED` | `ag2.task.expired` | Hub → observers, terminal (TTL) |
| `EV_TASK_STALLED` | `ag2.task.stalled` | Hub: no progress for `task_stall_threshold` |
| `EV_PEER_UNREACHABLE` | `ag2.peer.unreachable` | Hub: heartbeat missed past `peer_heartbeat_timeout` |
| `EV_PEER_RECONNECTED` | `ag2.peer.reconnected` | Hub: heartbeat resumed |
| `EV_EXPECTATION_VIOLATED` | `ag2.expectation.violated` | Hub: declared `Expectation` failed |
| `EV_PARTICIPANT_REMOVED` | `ag2.participant.removed` | Hub: participant removed from session |
| `EV_ERROR` | `ag2.error` | Generic error envelope |

User-defined event types may be posted with arbitrary `event_type` strings (no namespace check in V1). The framework only special-cases the names in the table.

There is no `EV_TASK_ASSIGNED` — the hub does not assign tasks. Owners emit `TaskStarted` on their own stream; the network mirror forwards it as `EV_TASK_STARTED` to subscribers (see [tasks.md](tasks.md)).

`EV_TASK_PHASE_*`, `EV_TASK_CANCELLED`, and `ag2.task.cancel_request` ship in Phase 2.

## Wire format

Envelopes are JSON dicts with `to_json()` / `from_json()` round-trip; every field round-trips byte-stable so cross-process transports can hash for idempotency / dedup.

## Streaming chunks

Streaming uses a separate `chunk` frame at the transport level (see [transport.md](transport.md)), not envelopes. Chunks are transient — they are not persisted to the WAL and are not visible to view policies. The LLM only sees finalized envelopes.

## Why hub-stamped envelope_id

Order across producers is well-defined only at the hub. Local UUID7 generation by the sender would lose monotonicity under concurrent senders to the same session. Stamping at hub-accept is also where idempotency dedup hooks (Phase 3).
