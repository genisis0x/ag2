# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Transport layer — frames + Link Protocol + ``LocalLink``.

Ships ``LocalLink`` (in-memory duplex). The ``Link`` Protocol surface
lets cross-process transports plug in without affecting layers above.
"""

from autogen.beta.exceptions import missing_optional_dependency

from .frames import (
    AcceptFrame,
    ChunkFrame,
    ErrorFrame,
    EventFrame,
    Frame,
    HelloFrame,
    NotifyFrame,
    PingFrame,
    PongFrame,
    ReceiptFrame,
    SendFrame,
    SubscribeFrame,
    UnsubscribeFrame,
    WelcomeFrame,
    decode_frame,
    encode_frame,
)
from .link import LinkClient, LinkEndpoint
from .local import LocalLink, LocalLinkClient, LocalLinkEndpoint

# WebSocket transport. Optional — pulls ``websockets``, which is not
# in the base install. Falls back to a Mock that raises ImportError
# with a clear install hint when the dep is missing.
try:
    from .ws import WsLink, WsLinkClient, WsLinkEndpoint, serve_ws
except ImportError as e:  # pragma: no cover — exercised only in slim installs
    WsLink = missing_optional_dependency("WsLink", "websockets", e)  # type: ignore[misc]
    WsLinkClient = missing_optional_dependency("WsLinkClient", "websockets", e)  # type: ignore[misc]
    WsLinkEndpoint = missing_optional_dependency("WsLinkEndpoint", "websockets", e)  # type: ignore[misc]
    serve_ws = missing_optional_dependency("serve_ws", "websockets", e)  # type: ignore[misc]

# HTTP CRUD surface. Optional — pulls ``starlette``.
try:
    from .http import make_http_app
except ImportError as e:  # pragma: no cover
    make_http_app = missing_optional_dependency("make_http_app", "starlette", e)  # type: ignore[misc]

__all__ = (
    "AcceptFrame",
    "ChunkFrame",
    "ErrorFrame",
    "EventFrame",
    "Frame",
    "HelloFrame",
    "LinkClient",
    "LinkEndpoint",
    "LocalLink",
    "LocalLinkClient",
    "LocalLinkEndpoint",
    "NotifyFrame",
    "PingFrame",
    "PongFrame",
    "ReceiptFrame",
    "SendFrame",
    "SubscribeFrame",
    "UnsubscribeFrame",
    "WelcomeFrame",
    "WsLink",
    "WsLinkClient",
    "WsLinkEndpoint",
    "decode_frame",
    "encode_frame",
    "make_http_app",
    "serve_ws",
)
