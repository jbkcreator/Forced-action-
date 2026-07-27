"""
Vera — the Agent Lane's truth & verification agent.

Runs as its own scheduled process (python -m src.agents.vera), entirely
separate from the LangGraph Lifecycle/Lifecycle supervisor
(python -m src.agents --serve). Permanently read-only on every business
table; her only write target is vera_facts (src.agents.vera.facts).

Kept import-light: submodules are imported lazily by __main__.py so cron
invocations stay fast.
"""
