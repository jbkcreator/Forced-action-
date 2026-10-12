"""Portable agent core: Slack Socket Mode session, human send gate, egress relay, kill switch.

Extracted from the Banks orchestrator and generalised for any agent that must never contact
the outside world without a human click. The package is self-contained: it imports nothing
from ``src`` or ``config``, takes its settings as an :class:`AgentCoreConfig`, and reaches the
database only through the SQLAlchemy ``Engine`` it is given.

Module map:

* ``slack_session`` and ``chatport``: Socket Mode session manager and the one Slack surface.
* ``governance``: system prompt compiler, safety invariants, tool safety levels.
* ``approval``: Approve / Revise / Reject cards and the approver lock.
* ``pending_actions`` and ``relay``: frozen egress drafts and the executor that sends approved ones.
* ``halt``: the persisted kill switch every process checks.
* ``scheduler``, ``calendarport`` and ``briefing``: standing jobs, read-only calendar availability,
  brief rendering.
"""
from .config import AgentCoreConfig

__all__ = ["AgentCoreConfig"]
