# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Append-only audit log writer (M3).

V1 writes to a single ``audit.jsonl`` indefinitely under the hub's
``KnowledgeStore`` root. Daily rotation is Phase 2.

The audit log records hub-cross-cutting events that are not visible
on per-session WALs:

* Identity changes — register, unregister, set_resume, set_skill, set_rule
* Expectation violations — one record per (session, expectation, violator)
  fire (the sweeper deduplicates so handlers don't re-record)

Each record is a JSON object on its own line with at least
``{"at": ISO-Z, "kind": "<event>"}`` plus event-specific fields.
"""

import json

from autogen.beta.knowledge import KnowledgeStore

from .layout import audit_path

__all__ = (
    "AUDIT_KIND_AGENT_REGISTERED",
    "AUDIT_KIND_AGENT_UNREGISTERED",
    "AUDIT_KIND_EXPECTATION_VIOLATED",
    "AUDIT_KIND_RESUME_SET",
    "AUDIT_KIND_RULE_SET",
    "AUDIT_KIND_SKILL_SET",
    "AuditLog",
)


AUDIT_KIND_AGENT_REGISTERED = "agent_registered"
AUDIT_KIND_AGENT_UNREGISTERED = "agent_unregistered"
AUDIT_KIND_RESUME_SET = "resume_set"
AUDIT_KIND_RULE_SET = "rule_set"
AUDIT_KIND_SKILL_SET = "skill_set"
AUDIT_KIND_EXPECTATION_VIOLATED = "expectation_violated"


class AuditLog:
    """Append-only writer over the hub's ``KnowledgeStore``.

    Stateless — every ``append`` is one JSON line. Reads are O(file
    size) and intended for tests / admin tooling, not hot paths.
    """

    def __init__(self, store: KnowledgeStore) -> None:
        # __init__ stores params; no side effects.
        self._store = store

    async def append(self, record: dict) -> None:
        """Serialise and append one record."""
        line = json.dumps(record, default=str, sort_keys=True) + "\n"
        await self._store.append(audit_path(), line)

    async def read_all(self) -> list[dict]:
        """Read and parse the entire audit log. Returns ``[]`` if absent."""
        data = await self._store.read(audit_path())
        if not data:
            return []
        records: list[dict] = []
        for line in data.splitlines():
            if not line.strip():
                continue
            records.append(json.loads(line))
        return records
