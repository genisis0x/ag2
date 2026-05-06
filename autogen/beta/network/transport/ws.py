# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""WebSocket transport.

``WsLinkClient`` and ``WsLinkEndpoint`` implement the same ``Link``
Protocol surface as ``LocalLink``'s in-process pair, but over a real
WebSocket connection. Frames are JSON-encoded via the existing
:func:`encode_frame` / :func:`decode_frame` helpers — the same
vocabulary that already round-trips correctly in tests.

The server entrypoint is :func:`serve_ws`, an async context manager
that opens a ``websockets`` server bound to ``host:port`` and creates
one ``WsLinkEndpoint`` per accepted connection. Endpoints attach to
the hub via the existing ``Hub.attach_endpoint`` path so the rest of
the dispatch machinery is oblivious to wire vs in-process.

This module imports ``websockets`` lazily — the network package's
``transport/__init__.py`` falls back to a ``missing_optional_dependency``
shim when ``websockets`` isn't installed, so importing
``autogen.beta.network`` doesn't fail in the slim install.
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

import websockets
from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import ServerConnection, serve as ws_serve

from ..ids import make_id
from .frames import Frame, decode_frame, encode_frame

if TYPE_CHECKING:
    from ..hub import Hub

__all__ = ("WsLink", "WsLinkClient", "WsLinkEndpoint", "serve_ws")


def _encode(frame: Frame) -> str:
    return json.dumps(encode_frame(frame))


def _decode(payload: str | bytes) -> Frame:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    return decode_frame(json.loads(payload))


class WsLinkClient:
    """Tenant-side WebSocket connection to a remote hub.

    Constructed via :meth:`WsLink.client`; ``open()`` performs the
    actual ``websockets.connect`` (the constructor only stores params,
    per CLAUDE.md no-side-effects-in-init rule).
    """

    def __init__(self, uri: str, *, endpoint_id: str | None = None) -> None:
        # __init__ stores params; no side effects.
        self.endpoint_id = endpoint_id if endpoint_id is not None else make_id()
        self._uri = uri
        self._ws: Any = None
        self._closed = False

    async def open(self) -> None:
        """Connect; idempotent."""
        if self._ws is not None or self._closed:
            return
        self._ws = await ws_connect(self._uri)

    async def send_frame(self, frame: Frame) -> None:
        if self._closed or self._ws is None:
            return
        await self._ws.send(_encode(frame))

    def frames(self) -> AsyncIterator[Frame]:
        return self._frames_impl()

    async def _frames_impl(self) -> AsyncIterator[Frame]:
        if self._ws is None:
            return
        try:
            async for raw in self._ws:
                yield _decode(raw)
        except websockets.exceptions.ConnectionClosed:
            return

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()


class WsLinkEndpoint:
    """Hub-side handle wrapping one accepted ``ServerConnection``."""

    def __init__(self, ws: ServerConnection, *, endpoint_id: str | None = None) -> None:
        # __init__ stores params; no side effects.
        self.endpoint_id = endpoint_id if endpoint_id is not None else make_id()
        self.agent_id: str | None = None
        self._ws = ws
        self._closed = False

    async def send_frame(self, frame: Frame) -> None:
        if self._closed:
            return
        try:
            await self._ws.send(_encode(frame))
        except websockets.exceptions.ConnectionClosed:
            self._closed = True

    def frames(self) -> AsyncIterator[Frame]:
        return self._frames_impl()

    async def _frames_impl(self) -> AsyncIterator[Frame]:
        try:
            async for raw in self._ws:
                yield _decode(raw)
        except websockets.exceptions.ConnectionClosed:
            return

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            await self._ws.close()


class WsLink:
    """Client-side factory mirroring the ``LocalLink`` shape.

    ``HubClient(link=WsLink("ws://host:port"))`` constructs a
    :class:`WsLinkClient` on first ``register``. Each ``WsLink`` is
    paired with a separate ``WsLinkClient`` per ``HubClient`` — one
    process can host multiple connections.
    """

    def __init__(self, uri: str) -> None:
        # __init__ stores params; no side effects.
        self._uri = uri

    @property
    def uri(self) -> str:
        return self._uri

    def client(self) -> WsLinkClient:
        """Construct a fresh client; caller awaits ``open()`` separately.

        The Hub-paired ``HubClient._ensure_connected`` calls ``open()``
        immediately after this returns, so tenant code never sees the
        unconnected handle.
        """
        return WsLinkClient(self._uri)


@contextlib.asynccontextmanager
async def serve_ws(
    hub: "Hub",
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    on_listen: Callable[[int], Awaitable[None]] | None = None,
) -> AsyncIterator[int]:
    """Serve the hub over WebSocket. Yields the bound port.

    Each accepted connection becomes a :class:`WsLinkEndpoint` and
    attaches to the hub via :meth:`Hub.attach_endpoint`. The hub's
    existing frame-processor task handles ``HelloFrame``-driven
    binding, ``SendFrame`` dispatch, ``ChunkFrame`` fan-out, etc.

    ``port=0`` lets the OS pick a free port — useful for tests. Use
    ``on_listen`` if you need the port number before yielding (e.g. to
    publish it elsewhere); the bound port is also yielded directly.

    Pass to an ``async with`` block; the server shuts down on exit.
    """

    async def _handler(ws: ServerConnection) -> None:
        endpoint = WsLinkEndpoint(ws)
        hub.attach_endpoint(endpoint)
        # Block until the connection closes — ``websockets`` keeps the
        # socket open while the handler is running. The hub's own
        # frame-processor consumes ``endpoint.frames()`` independently.
        try:
            await ws.wait_closed()
        finally:
            await endpoint.close()

    async with ws_serve(_handler, host, port) as server:
        # ``server.sockets`` is populated synchronously after ``serve``.
        sockets = list(server.sockets) if server.sockets else []
        bound = sockets[0].getsockname()[1] if sockets else port
        if on_listen is not None:
            await on_listen(bound)
        yield bound
