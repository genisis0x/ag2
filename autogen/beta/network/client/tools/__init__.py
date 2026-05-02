# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""LLM-facing tools attached by ``NetworkPlugin``.

V1 (M2) ships ``say`` and ``delegate``. The 4 grouped tools (``peers``,
``sessions``, ``tasks``, ``context``) arrive in M3.
"""

from .delegate import make_delegate_tool
from .say import make_say_tool

__all__ = ("make_delegate_tool", "make_say_tool")
