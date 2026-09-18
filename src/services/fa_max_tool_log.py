"""Per-tool-call audit logging for the FA Max agent runtime (WP-T2-2).

Writes to `fa_max_tool_call_log` — distinct from `agent_decisions`
(src/agents/tools/write_tools.py::log_decision), which records a graph's
overall DECISION. This module records every individual TOOL INVOCATION
inside a work item's bounded tool-call loop, whether or not that call
produced a decision.

Two write paths:
  - start_tool_call() / finish_tool_call() — the two-phase form used by
    src.agents.fa_max.agent_graph._node_tool_step: a durable 'in_progress'
    row is written BEFORE a tool executes, then updated to its true final
    outcome after. This is what gives "every tool call logged" its actual
    meaning for a `send` tool with a real side effect — the audit row
    exists before the side effect can happen, not only after.
  - log_tool_call() — a single-shot INSERT for a call that has already
    finished; use only where the pre-execution guarantee isn't needed.

Redaction mirrors src.agents.tools.write_tools._safe_snapshot's intent
(strip PII/financial-shaped values before persisting) but is stricter:
FA Max compliance boundaries (CLAUDE.md, src.services.fa_max_send_
governance) forbid ANY borrower financial data from landing in new schema,
so this redacts by key-name pattern match, recursively, over both input
and output before either is ever written.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_REDACTED = "[redacted]"

# Mirrors the prohibited-key pattern in fa_max_send_governance.py, plus
# generic PII-shaped keys (phone/email/ssn/address) that governance doesn't
# need to police (it only guards outbound payload content) but a tool-call
# audit log must never retain regardless.
_SENSITIVE_KEY = re.compile(
    r"(^|_)(ssn|social_security|credit_score|fico|income|bank_statement|"
    r"tax_return|dti|debt_to_income|rate|interest_rate|term|commitment|"
    r"phone|email|address|dob|date_of_birth|password|token|secret|api_key)(_|$)",
    re.IGNORECASE,
)


def _redact(value: Any) -> Any:
    """Recursively strip sensitive-shaped keys/values from a JSON-like value."""
    if isinstance(value, dict):
        return {
            k: (_REDACTED if _SENSITIVE_KEY.search(str(k)) else _redact(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


def redact_for_tool_log(value: Optional[dict]) -> Optional[dict]:
    """Public redaction entry point, reused by the tool registry's wrapper
    so a tool implementation never has to hand-roll its own redaction."""
    if value is None:
        return None
    return _redact(value)


def log_tool_call(
    *,
    session: Session,
    agent_name: str,
    tool_name: str,
    input: Optional[dict],
    output: Optional[dict],
    duration_ms: int,
    status: str,
    work_item_id: Optional[str] = None,
) -> bool:
    """Write one COMPLETE row to fa_max_tool_call_log in a single INSERT.

    This is the single-shot form: it logs a call that has ALREADY finished,
    in one write. It does NOT give the "audit row exists before any side
    effect can occur" guarantee — for that, src.agents.fa_max.agent_graph
    uses start_tool_call() before the tool executes and finish_tool_call()
    after, so a durable row exists first and is only ever updated, never
    created after the fact. Kept for callers that log a call's outcome
    after the fact and don't need the pre-execution guarantee (e.g.
    timed_tool_call() below, or a future simple read-only tool wrapper).

    Never RAISES — a logging failure must not abort the agent's work item
    mid-call — but DOES report success/failure via its return value, so a
    caller for whom "every tool call logged" is a hard requirement can fail
    closed rather than silently continuing unaudited. Callers that
    genuinely don't need that guarantee may ignore the return value.

    status must be one of 'success', 'error', 'blocked' (blocked = a
    requires_send_gate tool that the tier gate or suppression check
    refused to execute).
    """
    if status not in ("success", "error", "blocked"):
        raise ValueError(f"status must be 'success', 'error', or 'blocked', got {status!r}")
    import json as _json

    try:
        session.execute(
            text(
                "INSERT INTO fa_max_tool_call_log "
                "(work_item_id, agent_name, tool_name, input, output, duration_ms, status, created_at) "
                "VALUES (CAST(:work_item_id AS uuid), :agent_name, :tool_name, "
                ":input ::jsonb, :output ::jsonb, :duration_ms, :status, now())"
            ),
            {
                "work_item_id": work_item_id,
                "agent_name": agent_name,
                "tool_name": tool_name,
                "input": _json.dumps(redact_for_tool_log(input)),
                "output": _json.dumps(redact_for_tool_log(output)),
                "duration_ms": duration_ms,
                "status": status,
            },
        )
        return True
    except Exception:
        logger.warning(
            "fa_max_tool_call_log write failed for agent=%s tool=%s work_item=%s",
            agent_name, tool_name, work_item_id, exc_info=True,
        )
        return False


def start_tool_call(
    *,
    session: Session,
    agent_name: str,
    tool_name: str,
    input: Optional[dict],
    work_item_id: Optional[str] = None,
) -> Optional[int]:
    """Write a durable 'in_progress' row BEFORE the tool call executes.

    This is the fix for the audit-ordering gap a WP-T2-2 review found: a
    single post-execution log write meant a `send` tool's real side effect
    (an outbound message queued via relay.queue.enqueue) could land before
    any audit row existed, or with none at all if the post-hoc write
    failed. Writing this row first means a durable record of "this call is
    happening" exists before the tool can do anything — not only after.

    Returns the new row's id, or None if the write itself failed (never
    raises). A caller for whom "every tool call logged" is a hard
    requirement (src.agents.fa_max.agent_graph._node_tool_step) must refuse
    to execute the tool at all when this returns None — an unauditable call
    is worse than a call that never happened.
    """
    import json as _json

    try:
        row = session.execute(
            text(
                "INSERT INTO fa_max_tool_call_log "
                "(work_item_id, agent_name, tool_name, input, output, duration_ms, status, created_at) "
                "VALUES (CAST(:work_item_id AS uuid), :agent_name, :tool_name, "
                ":input ::jsonb, NULL, NULL, 'in_progress', now()) "
                "RETURNING id"
            ),
            {
                "work_item_id": work_item_id,
                "agent_name": agent_name,
                "tool_name": tool_name,
                "input": _json.dumps(redact_for_tool_log(input)),
            },
        )
        log_id = row.scalar()
        return int(log_id) if log_id is not None else None
    except Exception:
        logger.warning(
            "fa_max_tool_call_log start-write failed for agent=%s tool=%s work_item=%s",
            agent_name, tool_name, work_item_id, exc_info=True,
        )
        return None


def finish_tool_call(
    *,
    session: Session,
    log_id: int,
    output: Optional[dict],
    duration_ms: Optional[int],
    status: str,
) -> bool:
    """Update the row start_tool_call() wrote with the call's true final
    outcome. Never raises; returns False on failure so a caller with a hard
    "every call logged" requirement can fail closed.

    Also used to RECONCILE a call that completed AFTER its timeout was
    already logged: src.agents.fa_max.agent_graph registers a callback on
    a timed-out call's underlying thread so that whenever it actually
    finishes — even long after the agent loop gave up waiting and moved on
    — this is called again for the same log_id with the real outcome.
    Calling this more than once for the same log_id is safe; the row
    reflects whatever finish_tool_call() call landed last.
    """
    if status not in ("success", "error", "blocked"):
        raise ValueError(f"status must be 'success', 'error', or 'blocked', got {status!r}")
    import json as _json

    try:
        session.execute(
            text(
                "UPDATE fa_max_tool_call_log SET "
                "output = :output ::jsonb, duration_ms = :duration_ms, status = :status "
                "WHERE id = :log_id"
            ),
            {
                "log_id": log_id,
                "output": _json.dumps(redact_for_tool_log(output)),
                "duration_ms": duration_ms,
                "status": status,
            },
        )
        return True
    except Exception:
        logger.warning(
            "fa_max_tool_call_log finish-write failed for log_id=%s", log_id, exc_info=True,
        )
        return False
