"""
LangSmith tracing configuration for Cora agents.

Configures LangSmith SDK to trace Anthropic calls.
Requires: LANGSMITH_API_KEY, LANGSMITH_PROJECT, LANGSMITH_TRACING=true

All config is read via config.agents.get_agents_settings() (Pydantic) —
never os.environ directly (CLAUDE.md tooling rule).
"""

import os

from config.agents import get_agents_settings


def configure_tracing() -> bool:
    """
    Bridge pydantic settings → os.environ so the LangSmith SDK picks them up.

    The LangSmith tracer (wrap_anthropic / LangGraph auto-trace) reads
    LANGSMITH_* from os.environ at emit time. Values loaded from .env via
    pydantic are NOT in os.environ, so without this bridge a .env-only config
    silently logs to the wrong project (or nowhere). Call once at process
    startup (agents runtime). Returns True if tracing was enabled.

    Does not overwrite a variable already set explicitly in the environment.
    """
    s = get_agents_settings()
    if not s.langsmith_api_key or not s.langsmith_tracing:
        return False

    os.environ.setdefault("LANGSMITH_TRACING", "true")
    os.environ.setdefault("LANGSMITH_API_KEY", s.langsmith_api_key.get_secret_value())
    os.environ.setdefault("LANGSMITH_PROJECT", s.langsmith_project)
    os.environ.setdefault("LANGSMITH_ENDPOINT", s.langsmith_endpoint)
    return True


def get_langsmith_client():
    """Return configured LangSmith client if tracing is enabled, else None."""
    s = get_agents_settings()
    if not s.langsmith_api_key or not s.langsmith_tracing:
        return None

    from langsmith import Client
    return Client(
        api_key=s.langsmith_api_key.get_secret_value(),
        api_url=s.langsmith_endpoint,
    )


def is_tracing_enabled() -> bool:
    """Check if LangSmith tracing is enabled (key present and tracing on)."""
    s = get_agents_settings()
    return s.langsmith_api_key is not None and s.langsmith_tracing


def get_project_name() -> str:
    """Get the LangSmith project name."""
    return get_agents_settings().langsmith_project


def get_endpoint() -> str:
    """Get the LangSmith API endpoint."""
    return get_agents_settings().langsmith_endpoint
