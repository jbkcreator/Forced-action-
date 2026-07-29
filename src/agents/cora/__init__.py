"""
Cora — drafts-only cold-revenue-generation agent.

Separate from the existing autonomous Lifecycle runtime under
src/agents/graphs/ and src/agents/subgraphs/ — this package never imports
from, and is never imported by, that runtime, src/agents/router.py,
src/agents/supervisor.py, or src/agents/state.py. Runs as its own process
(python -m src.agents.cora --serve), not via `python -m src.agents --serve`.

Permanently drafts-only: no send capability anywhere in this package. No
module here imports src.agents.tools.write_tools.send_email/send_sms, any
Stripe-write function, or any Relay-execution symbol.
"""
