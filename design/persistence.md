# Persistence

The hub uses one `KnowledgeStore` instance for everything (`autogen/beta/knowledge/base.py:37`). V1 supports `MemoryKnowledgeStore` and `DiskKnowledgeStore`; both ship in framework-core.

## Path layout

```
hub/
  hub.json                              # hub config: registered adapters, clock metadata, version

  audit/
    {YYYY-MM-DD}.jsonl                  # cross-cutting events: register/unregister, rule_changed,
                                        # expectation_violated, participant_removed, hub start/stop

  agents/                               # one directory per registered agent
    {agent_id}/                         # UUID7
      passport.json                     # immutable for life of registration
      resume.json                       # mutable: claims + observed track record
      SKILL.md                          # optional; Anthropic-format usage doc
      runtime.json                      # hub-owned: binding, target, reachable, last_heartbeat
      rule.json                         # mutable: access + limits (per-agent)
      inbox.cursor                      # single integer; advanced on receipt(status="ack")
      inbox_nacks.jsonl                 # failed deliveries: {envelope_id, reason, when}
      inbox_overflow.jsonl              # envelopes dropped by overflow policy: {envelope_id, when}

  registry/                             # rebuildable caches; not authoritative
    by_name.json                        # name → agent_id
    by_capability.json                  # capability → [agent_id]

  sessions/
    {session_id}/
      metadata.json                     # SessionMetadata; includes the manifest snapshot
      wal.jsonl                         # append-only Envelope log
      tasks.json                        # flat index: {task_id: state}; rebuildable from tasks/

  tasks/                                # tasks outlive sessions; live at top level
    {task_id}/
      metadata.json                     # TaskMetadata
      events.jsonl                      # full Task* event stream (optional; for replay/audit)
      checkpoint.json                   # Phase 2.0: owner-supplied resume state, written by Task.checkpoint()
```

A single `wal.jsonl` per session. If a session grows past a configured size, future versions can chunk into `wal/00001.jsonl` / `wal/00002.jsonl`; V1 does not need this.

V2+ may add sibling top-level namespaces (`humans/`, `admins/`) when those `NetworkClient` impls ship; the V1 layout is forward-compatible because the registry caches are keyed by id rather than namespace.

## Why this layout

**Three identity files, not one.** `passport.json` is immutable; `resume.json` is mutable; `SKILL.md` is author-rewritten. Splitting them lets readers cache the immutable record forever and revalidate the mutable one cheaply (mtime check). It also lets discovery return different slices: `peers(action="find")` returns a passport+resume snippet, `peers(action="describe")` returns the full `SKILL.md`. See [identity.md](identity.md).

**Cursor + nacks/overflow JSONL, no inbox subdirectories.** The WAL is the durable queue; `inbox.cursor` is the per-agent read position. Multi-bucket inbox directories (`pending/`, `received/`, `overflow/`) cost atomic-rename complexity and don't compose with at-least-once cursor replay. Failed deliveries (`nacks`) and dropped envelopes (`overflow`) get flat JSONL logs for operational visibility (`wc -l` shows depth without a directory walk) and audit replay.

**Tasks at the top level.** Tasks outlive their originating session. A peer can `subscribe(task_id=...)` after the session closes. Per-session task **indexes** (`sessions/{id}/tasks.json`) preserve the "what tasks ran here" question without making the session own the task lifetime. Archiving a closed session does not orphan its tasks.

**Audit log.** Hub-cross-cutting events (register, unregister, rule changes, expectation fires, participant removed, hub start/stop) need a durable record outside any single session's WAL. Daily-rotated JSONL is cheap and keeps a multi-day diagnostic window. The hub writes one line per event; readers consume via `Hub.read_audit(since=, until=)` (see [hub.md](hub.md)).

## KnowledgeStore methods used (V1)

V1 uses only methods already on `main`:

- `read(path) -> str | None`
- `write(path, content) -> None`
- `list(path) -> list[str]`
- `delete(path) -> None`
- `exists(path) -> bool`
- `append(path, content) -> int` (returns offset)
- `read_range(path, start, end | None) -> str`

`on_change` is NOT used by V1. V1 is single-process; the in-memory cache is updated synchronously on every write. Cross-process invalidation is out of scope for framework-core (post Phase 4).

## Hub I/O patterns

| Path | Read pattern | Write pattern |
|---|---|---|
| `agents/{id}/passport.json` | Read once on `hydrate()`; cached forever | Written once at register |
| `agents/{id}/resume.json` | Read on `hydrate()`; cached | Replaced on `set_resume` or hub-observed update |
| `agents/{id}/SKILL.md` | Read on demand for `peers(action="describe")`; small LRU cache | Replaced on `set_skill` |
| `agents/{id}/rule.json` | Read on `hydrate()`; cached | Replaced on `set_rule` |
| `agents/{id}/runtime.json` | Re-read on hub failover only | Rewritten on every heartbeat |
| `agents/{id}/inbox.cursor` | Read at subscription open | Advanced on `receipt(status="ack")` |
| `agents/{id}/inbox_nacks.jsonl` | Read on retry / audit | `append` only |
| `agents/{id}/inbox_overflow.jsonl` | Read on audit | `append` only |
| `audit/{date}.jsonl` | Read on `read_audit(...)` | `append` only |
| `registry/by_name.json` | Read on lookup miss | Replaced on register/unregister |
| `registry/by_capability.json` | Read on `list_agents(capability=)` | Replaced on resume mutation |
| `sessions/{id}/metadata.json` | Read on `hydrate()` and on demand | Replaced on every state transition |
| `sessions/{id}/wal.jsonl` | Re-folded on `hydrate()`; `read_range` afterwards | `append` only |
| `sessions/{id}/tasks.json` | Read on demand | Replaced when a task starts/terminates in this session |
| `tasks/{id}/metadata.json` | Read on `hydrate()`; cached | Replaced on every state transition |
| `tasks/{id}/events.jsonl` | Read on full-history replay only | `append` only |

Every disk write is paired synchronously with the in-memory cache update under the relevant lock. Cache is never authoritative — `hydrate()` rebuilds it from disk on hub start.

## Backends

| Backend | V1 status |
|---|---|
| `MemoryKnowledgeStore` | Ships in framework-core; used in tests |
| `DiskKnowledgeStore` | Ships in framework-core; used in production V1 |
| `SqliteKnowledgeStore` | Available in framework-core; not exercised by V1 hub |
| `RedisKnowledgeStore` | Available in framework-core; not exercised by V1 hub |

The network layer depends only on Memory + Disk in V1. Sqlite / Redis / S3 / FoundationDB backends are post Phase 4 — they're cross-process coordination paths beyond the framework-core surface.

## Phase 2.0 additions

- `inbox.cursor` becomes load-bearing in-process — the default notify handler advances it on Receipt only after a successful turn; on reconnect (Hello), hub replays unacked envelopes from the cursor up to the WAL head.
- `tasks/{task_id}/checkpoint.json` — owner-supplied JSON blob written by `Task.checkpoint(state)`. Read on construction by `agent.task(resume_from=task_id)`. Single-writer (the owner); the framework provides storage but never inspects the contents.

## Phase 3 additions

- Cross-process cursor replay over `WsLink` — the in-process semantics from Phase 2.0 already work over `LocalLink`; Phase 3 exercises them on the wire.
- `RuleChangedFrame` push for hot-reload of `Rule` over a live connection.

## Phase 4 additions

- Audit retention policy: daily rotation of `audit/{date}.jsonl` and a configurable retention window.
- Closed-session archival sweeper: compact closed sessions to `summary.json` + final `result` and discard the WAL.
