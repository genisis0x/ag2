"""Shared fixtures for the orchestration parity tests.

Loads the repo-root ``.env`` once. Provides:
* ``gemini_model`` — model id (env override ``AG2_GEMINI_MODEL``).
* ``gemini_api_key`` — skips when missing so suite is safe to run blind.
* ``beta_gemini_config`` — ``GeminiConfig`` for ``autogen.beta``.
* ``classic_llm_config`` — ``LLMConfig`` for ``autogen.agentchat``.
"""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from autogen.beta.config import GeminiConfig
from autogen.llm_config import LLMConfig

_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env")


_ALLOWED_MODELS = {"gemini-3-flash-preview", "gemini-3.1-pro-preview"}


@pytest.fixture()
def gemini_model() -> str:
    model = os.getenv("AG2_GEMINI_MODEL", "gemini-3-flash-preview")
    if model not in _ALLOWED_MODELS:
        pytest.fail(
            f"AG2_GEMINI_MODEL={model!r} not allowed; pick one of {sorted(_ALLOWED_MODELS)}"
        )
    return model


@pytest.fixture()
def gemini_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        pytest.skip("GEMINI_API_KEY not set in .env")
    return key


@pytest.fixture()
def beta_gemini_config(gemini_api_key: str, gemini_model: str) -> GeminiConfig:
    return GeminiConfig(model=gemini_model, api_key=gemini_api_key, temperature=0)


@pytest.fixture()
def classic_llm_config(gemini_api_key: str, gemini_model: str) -> LLMConfig:
    return LLMConfig(
        {
            "api_type": "google",
            "model": gemini_model,
            "api_key": gemini_api_key,
        },
    )
