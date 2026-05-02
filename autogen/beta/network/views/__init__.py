# Copyright (c) 2026, AG2ai, Inc., AG2ai open-source projects maintainers and core contributors
#
# SPDX-License-Identifier: Apache-2.0

"""View policies — per-participant projection from WAL to ModelEvents.

A participant's effective LLM context for a turn is
``[layer_C_summary, *layer_B_projection, current_envelope]`` (see
``design/views.md``). Layer B is what view policies produce.

V1 ships ``FullTranscript`` only in M2; ``WindowedSummary`` lands in
M3 (composes with framework-core ``compact.py``); ``Composite`` is
Phase 2.
"""

from .base import ViewPolicy
from .builtin import FullTranscript

__all__ = ("FullTranscript", "ViewPolicy")
