"""Per-tool-call audit logging for the FA Max agent runtime (WP-T2-2).

Writes to `fa_max_tool_call_log` — distinct from `agent_decisions`
(src/agents/tools/write_tools.py::log_decision), which records a graph's
overall DECISION. This module records every individual TOOL INVOCATION
inside a work item's bounded tool-call loop, whether or not that call
produced a decision. Every tool registered in the FA Max tool registry
(src/agents/fa_max/tool_registry.py) must route through log_tool_call() —
see that module for the wrapper that calls this on every invocation.

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
import time
from contextlib import contextmanager
from typing import Any, Generator, Optional

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
    """Write one row to fa_max_tool_call_log. Never RAISES — a logging
    failure must not abort the agent's work item mid-call — but DOES report
    success/failure via its return value, so a caller for whom "every tool
    call logged" is a hard requirement (src.agents.fa_max.agent_graph) can
    fail closed rather than silently continuing unaudited. Callers that
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


@contextmanager
def timed_tool_call(
    *,
    session: Session,
    agent_name: str,
    tool_name: str,
    input: Optional[dict],
    work_item_id: Optional[str] = None,
) -> Generator[dict, None, None]:
    """Context manager that times a tool call and logs it on exit.

    Usage:
        with timed_tool_call(session=s, agent_name=..., tool_name=..., input=args) as result:
            result["output"] = the_tool_fn(**args)

    On an uncaught exception inside the block, logs status='error' with the
    exception message as output, then re-raises.
    """
    start = time.monotonic()
    result: dict = {"output": None}
    try:
        yield result
    except Exception as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        log_tool_call(
            session=session, agent_name=agent_name, tool_name=tool_name,
            input=input, output={"error": str(exc)}, duration_ms=duration_ms,
            status="error", work_item_id=work_item_id,
        )
        raise
    else:
        duration_ms = int((time.monotonic() - start) * 1000)
        log_tool_call(
            session=session, agent_name=agent_name, tool_name=tool_name,
            input=input, output=result.get("output"), duration_ms=duration_ms,
            status="success", work_item_id=work_item_id,
        )
