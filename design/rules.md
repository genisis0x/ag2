# Rules

V1 ships **access** + **limits** only. Transforms (per-envelope local enforcement) ship in Phase 3 alongside the WebSocket transport, when the cross-process trust boundary actually matters. V1 is in-process — transforms there reduce to middleware on the AgentClient and do not need their own pipeline machinery.

## Shape

```python
# autogen/beta/network/rule.py

from dataclasses import dataclass, field


@dataclass(slots=True)
class SessionTypeAccess:
    initiate: list[str] = field(default_factory=lambda: ["*"])
    accept: list[str] = field(default_factory=lambda: ["*"])


@dataclass(slots=True)
class AccessBlock:
    inbound_from: list[str] = field(default_factory=lambda: ["*"])    # globs over `name`
    outbound_to: list[str] = field(default_factory=lambda: ["*"])
    session_types: SessionTypeAccess = field(default_factory=SessionTypeAccess)


@dataclass(slots=True)
class RateBlock:
    per_minute: int = 0             # 0 disables
    burst: int = 0


@dataclass(slots=True)
class InboxBlock:
    max_pending: int = 1000
    overflow: str = "reject"        # "reject" | "drop_oldest" | "drop_newest"


@dataclass(slots=True)
class LimitsBlock:
    max_concurrent_sessions: int = 0     # 0 disables
    max_concurrent_tasks: int = 0
    session_ttl_default: str = "2h"      # parse_duration accepts "30s" | "15m" | "2h" | "1d"
    task_ttl_default: str = "15m"
    rate: RateBlock = field(default_factory=RateBlock)
    delegation_depth: int = 5            # 0 disables
    inbox: InboxBlock = field(default_factory=InboxBlock)

    # Failure-mode thresholds (see failure_modes.md)
    peer_heartbeat_timeout: str = "30s"      # emit ag2.peer.unreachable past this
    task_stall_threshold: str = "60s"        # emit ag2.task.stalled past this without progress
    session_idle_threshold: str = "5m"       # emit ag2.session.idle past this without envelopes


@dataclass(slots=True)
class Rule:
    version: int = 1
    access: AccessBlock = field(default_factory=AccessBlock)
    limits: LimitsBlock = field(default_factory=LimitsBlock)
```

Defaults are permissive — a freshly registered Agent with no rule changes can talk to anyone, accept any session type, and has no rate limit. Apps tighten by passing a non-default `Rule` to `hub_client.register(...)`.

Rules are per-(hub, agent) in V1. When Phase 2+ supports multi-network-per-hub, rules become per-(network_id, agent_id) and `Hub.set_rule` gains a `network_id` parameter. The V1 `Rule` shape is forward-compatible — there is no hub-wide rule concept that would conflict.

## Enforcement

Both blocks are enforced at the **hub**, never the client. This matches the trust model: the hub is the only place that has cross-tenant visibility.

| Block | Why hub is the right place |
|---|---|
| `access` | Cross-tenant — the hub is the authority on "is X allowed to message Y" |
| `limits` | Cross-call aggregation (rate windows, concurrent counts, depth) only the hub can see |

Per-envelope tenant logic (PII redaction, content truncation, audit annotation) belongs in transforms on the AgentClient — Phase 3.

## Limits invariants

- `max_concurrent_sessions = 0` disables the cap (default).
- `session_ttl_default` is parsed by `LimitsBlock.parse_duration(s) -> int seconds`, accepting `"30s"`, `"15m"`, `"2h"`, `"1d"`. `expires_at = created_at + ttl_seconds` is stamped at session create.
- `delegation_depth` counts hops via `Envelope.depth`. The reply path auto-increments. `0` disables the ceiling.
- `rate.per_minute = 0` disables the limiter. Otherwise a per-Agent token bucket runs in the hub.
- Inbox `max_pending` is enforced pre-flight (before WAL append) to avoid half-state on overflow.

## Rule changes

V1: `Hub.set_rule(agent_id, rule)` writes the rule, bumps `version`, and updates the in-memory cache. No live link to push over yet — the change takes effect on the hub's next access/limits decision.

Phase 3: HTTP `PUT /v1/agents/{id}/rule` plus `RuleChangedFrame` push over the WS link, so the AgentClient can rebuild its (Phase-3) transform pipeline atomically.

## Phase 3 additions

Per-envelope tenant logic lands as **two hook points on `AgentClient`**, not a managed pipeline. The hub stays out of it — the trust boundary already says transforms run tenant-side, so they're naturally Python code, not data.

```python
class AgentClient:
    def add_send_hook(
        self,
        hook: Callable[[Envelope], Envelope | None],
    ) -> None:
        """Register a callable run on every outbound envelope before
        the hub sees it. Returning ``None`` drops the envelope;
        returning an ``Envelope`` (possibly the same one) replaces it.
        Hooks run in registration order."""

    def add_receive_hook(
        self,
        hook: Callable[[Envelope], Envelope | None],
    ) -> None:
        """Register a callable run on every inbound envelope before
        the notify handler. Same semantics as ``add_send_hook``."""
```

Two hook points, no stages, no DSL, no named-transform registry. Common transforms (`redact_pii`, `truncate_long_content`, `stamp_audit_header`) live in `examples/transforms/` as plain functions users copy or import.

`Rule` itself stays purely about access + limits — it's data the hub enforces. Hooks are tenant Python; persisting them as data would force a registry and a stages framework, which is the opposite of tools-not-systems.

Also in Phase 3:

- `RuleChangedFrame` push for hot-reload of access / limits over a live connection. Hooks are not part of the push — tenant code reconfigures them directly.

Anything more elaborate — staged pipelines, named-transform registries, exec / HTTP / WebSocket sidecar forms, hot-shipping transform code across hosts — is out of scope (post Phase 4).
