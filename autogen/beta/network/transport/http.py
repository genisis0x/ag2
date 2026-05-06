# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""HTTP CRUD surface (10 routes) over Starlette.

The ``Link`` Protocol via ``WsLink`` carries the stateful, push-driven
side (notify dispatch, chunks, hello/welcome). This module ships the
stateless control plane: register / unregister / discovery / session
lifecycle / envelope post / WAL read.

Routes (10):

* ``POST   /agents``                       — register
* ``GET    /agents``                       — list agents
* ``GET    /agents/{name_or_id}``          — get passport
* ``DELETE /agents/{agent_id}``            — unregister
* ``POST   /sessions``                     — create session
* ``GET    /sessions``                     — list sessions
* ``GET    /sessions/{session_id}``        — get session metadata
* ``POST   /sessions/{session_id}/close``  — close session
* ``POST   /sessions/{session_id}/envelopes`` — post envelope
* ``GET    /sessions/{session_id}/wal``    — read WAL slice

Auth: when the configured :class:`AuthRegistry` includes a non-default
scheme, the middleware reads ``X-Agent-Name`` + ``X-Api-Key`` headers
and validates via the matching adapter. ``NoAuth`` skips the check.

Lazy import: this module imports ``starlette``. The package
``transport/__init__.py`` falls back to a ``missing_optional_dependency``
shim when starlette isn't installed.
"""

import json
from typing import TYPE_CHECKING, Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from ..auth import AuthRegistry
from ..envelope import Envelope
from ..errors import (
    AccessDeniedError,
    AuthError,
    InboxFull,
    NetworkError,
    NotFoundError,
    ProtocolError,
    RateLimited,
)
from ..identity import Passport, Resume
from ..rule import Rule
from ..session import SessionState

if TYPE_CHECKING:
    from ..hub import Hub

__all__ = ("make_http_app",)


_ERROR_CODES: dict[type[NetworkError], tuple[int, str]] = {
    NotFoundError: (404, "not_found"),
    AccessDeniedError: (403, "access_denied"),
    AuthError: (401, "auth_error"),
    ProtocolError: (409, "protocol_error"),
    InboxFull: (429, "inbox_full"),
    RateLimited: (429, "rate_limited"),
}


def _error_response(exc: NetworkError) -> JSONResponse:
    for cls, (status, code) in _ERROR_CODES.items():
        if isinstance(exc, cls):
            return JSONResponse({"code": code, "message": str(exc)}, status_code=status)
    return JSONResponse({"code": "network_error", "message": str(exc)}, status_code=500)


class _AuthMiddleware:
    """Pure-ASGI auth middleware.

    Validates ``X-Agent-Name`` + ``X-Api-Key`` against the hub's
    :class:`AuthRegistry`. Skipped entirely when only ``NoAuth`` is
    registered (the default).

    Implemented as a pure ASGI middleware (not Starlette's
    ``BaseHTTPMiddleware``) because the latter has known interaction
    issues with ``httpx.ASGITransport`` — short-circuit responses
    returned from ``dispatch`` don't reliably reach the client.
    """

    def __init__(self, app: ASGIApp, *, hub: "Hub") -> None:
        self._app = app
        self._hub = hub

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        registry: AuthRegistry = self._hub._auth
        schemes = registry.schemes()
        if schemes == ["none"]:
            await self._app(scope, receive, send)
            return

        # Register carries auth on the passport's AuthBlock — Hub
        # validates inside ``Hub.register``. Skip the middleware check
        # there to avoid a chicken-and-egg.
        method = scope.get("method", "")
        path = scope.get("path", "")
        if method == "POST" and path == "/agents":
            await self._app(scope, receive, send)
            return

        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        name = headers.get("x-agent-name")
        key = headers.get("x-api-key")
        if not name or not key:
            await _send_json(send, 401, {"code": "auth_error", "message": "X-Agent-Name + X-Api-Key required"})
            return

        agent_id = self._hub._name_to_id.get(name)
        passport = self._hub._passports.get(agent_id) if agent_id else None
        if passport is None:
            await _send_json(send, 401, {"code": "auth_error", "message": f"unknown identity: {name!r}"})
            return

        # Use the scheme declared on the passport — NoAuth in the
        # registry must not be a backdoor for ApiKeyAuth-tagged
        # identities. Iterating schemes and accepting the first
        # passing one would let any ``X-Api-Key`` value through
        # because NoAuth accepts every claim.
        try:
            adapter = registry.get(passport.auth.scheme)
        except AuthError as exc:
            await _send_json(send, 401, {"code": "auth_error", "message": str(exc)})
            return
        try:
            await adapter.validate(passport, {"key": key})
        except AuthError as exc:
            await _send_json(send, 401, {"code": "auth_error", "message": str(exc)})
            return
        await self._app(scope, receive, send)


async def _send_json(send: Send, status: int, body: dict[str, Any]) -> None:
    payload = json.dumps(body).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": payload})


def make_http_app(hub: "Hub") -> Starlette:
    """Construct a Starlette ASGI app exposing the 10-route CRUD
    surface backed by ``hub``.

    Apps deploy this with their preferred ASGI server (uvicorn,
    hypercorn, daphne). For tests, drive it with
    ``httpx.AsyncClient(transport=httpx.ASGITransport(app=...))``.
    """

    async def register(request: Request) -> Response:
        body = await request.json()
        passport = Passport.from_dict(body["passport"])
        resume = Resume.from_dict(body.get("resume", {}))
        skill_md = body.get("skill_md")
        rule_body = body.get("rule")
        rule = Rule.from_dict(rule_body) if rule_body is not None else None
        try:
            stamped = await hub.register(passport, resume, skill_md=skill_md, rule=rule)
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"passport": stamped.to_dict()}, status_code=201)

    async def get_agent(request: Request) -> Response:
        try:
            passport = await hub.get_agent(request.path_params["name_or_id"])
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"passport": passport.to_dict()})

    async def list_agents(request: Request) -> Response:
        params = request.query_params
        capability = params.get("capability")
        query = params.get("q")
        sort_by = params.get("sort_by")
        limit = int(params.get("limit", 50))
        try:
            passports = await hub.list_agents(
                capability=capability, query=query, sort_by=sort_by, limit=limit
            )
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"agents": [p.to_dict() for p in passports]})

    async def unregister(request: Request) -> Response:
        try:
            await hub.unregister(request.path_params["agent_id"])
        except NetworkError as exc:
            return _error_response(exc)
        return Response(status_code=204)

    async def create_session(request: Request) -> Response:
        body = await request.json()
        try:
            metadata = await hub.create_session(
                creator_id=body["creator_id"],
                manifest_type=body["manifest_type"],
                manifest_version=body.get("manifest_version", 1),
                participants=body["participants"],
                required_acks=body.get("required_acks"),
                ttl=body.get("ttl"),
                knobs=body.get("knobs"),
                intent=body.get("intent"),
                labels=body.get("labels"),
            )
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"session": metadata.to_dict()}, status_code=201)

    async def list_sessions(request: Request) -> Response:
        params = request.query_params
        agent_id = params.get("agent_id")
        limit = int(params.get("limit", 50))
        include_terminal = params.get("include_terminal", "false").lower() == "true"
        try:
            sessions = await hub.list_sessions(agent_id=agent_id, limit=limit * 4)
        except NetworkError as exc:
            return _error_response(exc)
        if not include_terminal:
            sessions = [
                m for m in sessions
                if m.state not in (SessionState.CLOSED, SessionState.EXPIRED)
            ]
        return JSONResponse({"sessions": [m.to_dict() for m in sessions[:limit]]})

    async def get_session(request: Request) -> Response:
        try:
            metadata = await hub.get_session(request.path_params["session_id"])
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"session": metadata.to_dict()})

    async def close_session(request: Request) -> Response:
        body: dict[str, Any] = {}
        if int(request.headers.get("content-length", "0") or 0) > 0:
            body = await request.json()
        try:
            metadata = await hub.close_session(
                request.path_params["session_id"],
                reason=body.get("reason", ""),
            )
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"session": metadata.to_dict()})

    async def post_envelope(request: Request) -> Response:
        body = await request.json()
        envelope = Envelope.from_dict(body["envelope"])
        envelope.session_id = request.path_params["session_id"]
        try:
            envelope_id = await hub.post_envelope(envelope)
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"envelope_id": envelope_id}, status_code=201)

    async def read_wal(request: Request) -> Response:
        params = request.query_params
        since = int(params.get("since", 0))
        until_param = params.get("until")
        until = int(until_param) if until_param is not None else None
        try:
            envelopes = await hub.read_wal(
                request.path_params["session_id"], since=since, until=until
            )
        except NetworkError as exc:
            return _error_response(exc)
        return JSONResponse({"envelopes": [e.to_dict() for e in envelopes]})

    routes = [
        Route("/agents", register, methods=["POST"]),
        Route("/agents", list_agents, methods=["GET"]),
        Route("/agents/{name_or_id:str}", get_agent, methods=["GET"]),
        Route("/agents/{agent_id:str}", unregister, methods=["DELETE"]),
        Route("/sessions", create_session, methods=["POST"]),
        Route("/sessions", list_sessions, methods=["GET"]),
        Route("/sessions/{session_id:str}", get_session, methods=["GET"]),
        Route("/sessions/{session_id:str}/close", close_session, methods=["POST"]),
        Route("/sessions/{session_id:str}/envelopes", post_envelope, methods=["POST"]),
        Route("/sessions/{session_id:str}/wal", read_wal, methods=["GET"]),
    ]

    middleware = [Middleware(_AuthMiddleware, hub=hub)]
    return Starlette(routes=routes, middleware=middleware)
