"""Fleet agent identities.

Lifecycle's LangGraph-based autonomous decision runtime lives directly in this
package (graphs/, subgraphs/, supervisor.py, router.py — the process started
by `python -m src.agents --serve`). Other agent identities nested here
(e.g. hunter/) are NOT part of that runtime and are never imported by it —
they're plain, cron-triggered modules that happen to share this parent
package for organization by agent identity, not by execution technology.
"""

__all__ = ["get_agents_settings"]

from config.agents import get_agents_settings
