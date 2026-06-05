"""
Thin shim — delegates entirely to src.agents.__main__.

Production: agents run via Docker (docker/agents.Dockerfile).
    docker compose up agents

Local dev (no Docker):
    python -m src.agents --serve

This file exists only for backwards compatibility with any tooling that
invokes scripts/run_agents.py directly. Prefer python -m src.agents.
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.agents.__main__ import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
