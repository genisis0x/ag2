# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""``delegate`` — one-shot consult: open consulting → ask → return reply.

This is the most common multi-agent pattern in beta and gets its own
flat tool (vs the grouped ``sessions(action="open") + tasks(...)``
pattern). The flat surface keeps the LLM's tool list short — ``say``
and ``delegate`` cover the hot path.

M2 ships ``blocking=True`` only — the ``blocking=False`` form returning
a task handle arrives in M3. ``capability`` is recorded as a knob for
the session and (in M3) feeds ``Resume.observed`` on terminal completion.
"""

import asyncio
from typing import TYPE_CHECKING

from autogen.beta.tools import tool

from ...envelope import EV_TEXT, Envelope
from ..inject import AgentClientInject

if TYPE_CHECKING:
    from ..agent_client import AgentClient

__all__ = ("make_delegate_tool",)


def make_delegate_tool(client: "AgentClient") -> object:
    """Return a closure-bound ``delegate`` tool."""

    @tool
    async def delegate(
        target: str,
        prompt: str,
        *,
        capability: str | None = None,
        timeout: float = 300.0,
        ag_client: AgentClientInject | None = None,
    ) -> str:
        """Open a one-shot consulting session with ``target`` and return its reply.

        target: peer **name** (or agent_id) to consult.
        prompt: the question or request to send.
        capability: optional capability tag. Recorded as a session
                    knob; M3 uses it for ``Resume.observed`` updates.
        timeout: max seconds to wait for the reply (default 300s).

        Returns the reply text on success, or an ``Error: ...`` string
        on failure (target unknown, timeout, session rejected, etc.).
        """
        actual_client = ag_client if ag_client is not None else client

        # Resolve target.
        try:
            target_passport = await actual_client._hub.get_agent(target)
        except Exception:
            return f"Error: target {target!r} not found"
        target_id = target_passport.agent_id
        if target_id is None:
            return f"Error: target {target!r} has no agent_id"

        # Open consulting session — handshake awaited inside.
        knobs = {"capability": capability} if capability else None
        try:
            session = await actual_client.open(
                type="consulting",
                target=target,
                knobs=knobs,
            )
        except Exception as exc:
            return f"Error: failed to open consulting session: {exc}"

        # Suppress the default handler for this session — we own its
        # lifecycle here; we don't want the handler to ALSO run a turn
        # on the reply envelope when it lands.
        actual_client._suppress_handler(session.session_id)
        try:
            # Send the prompt as the initiator's turn.
            try:
                await session.send(prompt, audience=[target_id])
            except Exception as exc:
                return f"Error: prompt send failed: {exc}"

            # Wait for the respondent's reply.
            try:
                reply = await actual_client.wait_for_session_event(
                    session_id=session.session_id,
                    predicate=_reply_predicate(target_id),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                return f"Error: delegate to {target!r} timed out after {timeout}s"
            except Exception as exc:
                return f"Error: delegate to {target!r} failed: {exc}"
        finally:
            actual_client._unsuppress_handler(session.session_id)

        body = reply.event_data.get("text", "")
        return body if isinstance(body, str) else str(body)

    return delegate


def _reply_predicate(target_id: str):
    """Match the consulting respondent's substantive reply."""

    def matches(envelope: Envelope) -> bool:
        return envelope.event_type == EV_TEXT and envelope.sender_id == target_id

    return matches
