# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Authentication adapters.

Ships ``NoAuth`` (default) and ``ApiKeyAuth``. The ``AuthAdapter``
Protocol stays open so callers can plug in alternate schemes (JWT,
mTLS, signed-challenge) by passing a custom ``AuthRegistry``.
"""

import hmac
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Protocol

from .errors import AuthError
from .identity import Passport

__all__ = (
    "ApiKeyAuth",
    "AuthAdapter",
    "AuthRegistry",
    "NoAuth",
)


class AuthAdapter(Protocol):
    """Validates a passport's auth claim at the connection handshake."""

    scheme: str

    async def validate(self, passport: Passport, claim: dict[str, Any]) -> None:
        """Raise ``AuthError`` on failure; return ``None`` on success."""
        ...


class NoAuth:
    """No-op adapter — accepts every claim. Default registry entry."""

    scheme = "none"

    async def validate(self, passport: Passport, claim: dict[str, Any]) -> None:
        return None


class ApiKeyAuth:
    """Shared-secret API key validation.

    Constructed with either a static ``keys`` mapping (``name → key``)
    or a ``resolver`` callable that looks up the expected key per
    name. Claims look like ``{"key": "<secret>"}``. Compare uses
    :func:`hmac.compare_digest` for constant-time equality.

    A passport whose name has no configured key fails closed —
    ``AuthError("unknown identity")``. Pass an empty mapping plus a
    permissive resolver if you want a different default.
    """

    scheme = "api_key"

    def __init__(
        self,
        keys: Mapping[str, str] | None = None,
        *,
        resolver: Callable[[str], str | None] | None = None,
    ) -> None:
        # __init__ stores params; no side effects.
        self._keys: dict[str, str] = dict(keys) if keys is not None else {}
        self._resolver = resolver

    def _expected(self, name: str) -> str | None:
        if name in self._keys:
            return self._keys[name]
        if self._resolver is not None:
            return self._resolver(name)
        return None

    async def validate(self, passport: Passport, claim: dict[str, Any]) -> None:
        expected = self._expected(passport.name)
        if expected is None:
            raise AuthError(f"unknown identity: {passport.name!r}")
        provided = claim.get("key")
        if not isinstance(provided, str) or not provided:
            raise AuthError("api_key claim missing 'key'")
        if not hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8")):
            raise AuthError(f"api_key mismatch for {passport.name!r}")


class AuthRegistry:
    """Registry mapping ``scheme`` strings to ``AuthAdapter`` impls.

    Apps wanting a custom adapter construct their own
    ``AuthRegistry([NoAuth(), MyAuth()])`` and pass it to
    ``Hub(... auth=...)``. Use :meth:`default` for the ``NoAuth``-only
    default.
    """

    _DEFAULT: ClassVar["AuthRegistry | None"] = None

    def __init__(self, adapters: list[AuthAdapter]) -> None:
        # __init__ stores params; no side effects.
        self._adapters: dict[str, AuthAdapter] = {a.scheme: a for a in adapters}

    @classmethod
    def default(cls) -> "AuthRegistry":
        """Return the lazily-initialised default registry — ``NoAuth`` only."""
        if cls._DEFAULT is None:
            cls._DEFAULT = cls([NoAuth()])
        return cls._DEFAULT

    def get(self, scheme: str) -> AuthAdapter:
        try:
            return self._adapters[scheme]
        except KeyError as exc:
            raise AuthError(f"unknown auth scheme: {scheme!r}") from exc

    def schemes(self) -> list[str]:
        return list(self._adapters.keys())
