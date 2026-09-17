"""
Command Center tool schemas and dispatch.

All tools are read-only. The dispatch function always returns a JSON string
(never raises) so every tool_call gets a well-formed tool_result, even on
error — the Anthropic API requires a tool_result for every tool_use_id in
the prior assistant turn.

Tool results are prefixed "DATA:" so Claude's system prompt can instruct it
to treat them as data, not as instructions.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.cora.command_center.db_tool import execute_query

logger = logging.getLogger(__name__)

FA_MAX_VENTURE_KEY = "fa_max_lending"

_DEFAULT_REPLY_RATE = 0.05
_DEFAULT_BOOKING_RATE = 0.40
_DEFAULT_COMPLETION_RATE = 0.80
_DEFAULT_FUNDED_RATE = 0.25
_DEFAULT_AVG_LOAN_USD = 300_000.0


# ──────────────────────────────────────────────────────────────────────────────
# Tool schemas — passed to Claude via the `tools` parameter
# ──────────────────────────────────────────────────────────────────────────────

QUERY_DB_SCHEMA: Dict[str, Any] = {
    "name": "query_db",
    "description": (
        "Run a SELECT query against the pipeline database. "
        "Allowed tables: buyer_entities, outbound_drafts, lender_box_programs, "
        "lender_box_geographies. Results capped at 100 rows. "
        "Always filter outbound_drafts with WHERE venture_key = 'fa_max_lending'."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "A single SELECT SQL statement.",
            }
        },
        "required": ["sql"],
    },
}

EVALUATE_DEAL_SCHEMA: Dict[str, Any] = {
    "name": "evaluate_deal",
    "description": (
        "Check whether a deal is within Backflip's lending box. "
        "Returns in_box, out_of_box, or uncertain with reasons. "
        "Use when Josh asks 'will Backflip do this deal?' or similar."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "property_type": {
                "type": "string",
                "description": "e.g. single_family, condo, duplex, triplex, fourplex",
            },
            "state": {"type": "string", "description": "2-letter state code e.g. FL"},
            "proposed_loan_amount": {"type": "number", "description": "USD"},
            "purchase_price": {"type": "number", "description": "USD (optional)"},
            "rehab_estimate": {"type": "number", "description": "USD (optional)"},
            "arv": {"type": "number", "description": "After-repair value USD (optional)"},
            "borrower_prior_loans": {
                "type": "integer",
                "description": "Number of prior closed loans (optional)",
            },
            "county": {"type": "string", "description": "County name (optional)"},
        },
        "required": ["property_type", "state", "proposed_loan_amount"],
    },
}

GET_BACKWARD_MATH_SCHEMA: Dict[str, Any] = {
    "name": "get_backward_math",
    "description": (
        "Compute how many outreaches, replies, and calls are needed each month "
        "to hit a deal-funded target. Identifies the current starving stage — "
        "the pipeline stage most short of its required throughput."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "target_deals_per_month": {
                "type": "number",
                "description": "Target number of funded deals per month.",
            },
            "avg_loan_usd": {
                "type": "number",
                "description": f"Average loan size USD. Default {_DEFAULT_AVG_LOAN_USD:,.0f}.",
            },
        },
        "required": ["target_deals_per_month"],
    },
}

GET_SCOREBOARD_SCHEMA: Dict[str, Any] = {
    "name": "get_scoreboard",
    "description": (
        "Get pre-aggregated weekly pipeline numbers: outreaches sent, replies "
        "received, reply rate, and active whale target count."
    ),
    "input_schema": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

SEARCH_OPPORTUNITY_SCHEMA: Dict[str, Any] = {
    "name": "search_opportunity",
    "description": (
        "Search for a whale or opportunity by company or individual name. "
        "Returns matching buyer entities and their outreach status."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Full or partial name / company to search.",
            }
        },
        "required": ["name"],
    },
}

TOOLS: list[Dict[str, Any]] = [
    QUERY_DB_SCHEMA,
    EVALUATE_DEAL_SCHEMA,
    GET_BACKWARD_MATH_SCHEMA,
    GET_SCOREBOARD_SCHEMA,
    SEARCH_OPPORTUNITY_SCHEMA,
]


# ──────────────────────────────────────────────────────────────────────────────
# Tool handlers
# ──────────────────────────────────────────────────────────────────────────────

def _handle_query_db(tool_input: Dict[str, Any], db: Session) -> Any:
    sql = str(tool_input.get("sql", "")).strip()
    if not sql:
        return {"error": "sql is required"}
    return execute_query(db, sql)


def _handle_evaluate_deal(tool_input: Dict[str, Any], db: Session) -> Any:
    from decimal import Decimal as D
    from src.services.lender_box import DealInput, evaluate

    def _dec(val: Any) -> Optional[Decimal]:
        return D(str(val)) if val is not None else None

    try:
        deal = DealInput(
            property_type=str(tool_input.get("property_type", "")),
            state=str(tool_input.get("state", "")).upper(),
            proposed_loan_amount=D(str(tool_input["proposed_loan_amount"])),
            purchase_price=_dec(tool_input.get("purchase_price")),
            rehab_estimate=_dec(tool_input.get("rehab_estimate")),
            arv=_dec(tool_input.get("arv")),
            borrower_prior_loans=tool_input.get("borrower_prior_loans"),
            county=tool_input.get("county"),
        )
        result = evaluate(deal, db)
        return {
            "status": result.status,
            "matched_program": result.matched_program,
            "matched_program_name": result.matched_program_name,
            "fail_reasons": result.fail_reasons,
            "uncertain_flags": result.uncertain_flags,
            "summary": result.summary(),
        }
    except Exception as exc:
        logger.warning("tools.evaluate_deal failed: %s", exc)
        return {"error": str(exc)}


def _handle_get_backward_math(tool_input: Dict[str, Any], db: Session) -> Any:
    target = float(tool_input.get("target_deals_per_month", 3))
    avg_loan = float(tool_input.get("avg_loan_usd", _DEFAULT_AVG_LOAN_USD))
    window = 90
    cutoff = date.today() - timedelta(days=window)

    try:
        outreach_row = db.execute(text("""
            SELECT COUNT(*) AS cnt
            FROM outbound_drafts
            WHERE venture_key = :vk
              AND created_at >= :cutoff
              AND status NOT IN ('draft', 'rejected', 'expired')
        """), {"vk": FA_MAX_VENTURE_KEY, "cutoff": cutoff}).mappings().first()
        actual_outreaches = int(outreach_row["cnt"]) if outreach_row else 0

        replies_row = db.execute(text("""
            SELECT COUNT(DISTINCT opportunity_thread_id) AS cnt
            FROM outbound_drafts
            WHERE venture_key = :vk AND replied_at >= :cutoff
        """), {"vk": FA_MAX_VENTURE_KEY, "cutoff": cutoff}).mappings().first()
        actual_replies = int(replies_row["cnt"]) if replies_row else 0
    except Exception as exc:
        logger.warning("tools.backward_math DB query failed: %s", exc)
        actual_outreaches, actual_replies = 0, 0

    reply_rate = (
        actual_replies / actual_outreaches
        if actual_outreaches >= 20 else _DEFAULT_REPLY_RATE
    )
    rates_source = "observed" if actual_outreaches >= 20 else "default_assumptions"

    scale = window / 30.0
    needed_outreaches = round((target / _DEFAULT_FUNDED_RATE / _DEFAULT_COMPLETION_RATE /
                               _DEFAULT_BOOKING_RATE / reply_rate) / scale)
    needed_replies = round((target / _DEFAULT_FUNDED_RATE / _DEFAULT_COMPLETION_RATE /
                            _DEFAULT_BOOKING_RATE) / scale)
    needed_booked = round((target / _DEFAULT_FUNDED_RATE / _DEFAULT_COMPLETION_RATE) / scale)
    needed_completed = round((target / _DEFAULT_FUNDED_RATE) / scale)

    actual_o_monthly = round(actual_outreaches / scale)
    actual_r_monthly = round(actual_replies / scale)

    stages = [
        ("outreach", needed_outreaches - actual_o_monthly),
        ("reply", needed_replies - actual_r_monthly),
    ]
    starving_stage, starving_deficit = max(stages, key=lambda s: s[1])

    return {
        "target_deals_per_month": target,
        "avg_loan_usd": avg_loan,
        "target_funded_volume_usd": target * avg_loan,
        "conversion_rates": {
            "reply_rate": round(reply_rate, 4),
            "booking_rate": _DEFAULT_BOOKING_RATE,
            "completion_rate": _DEFAULT_COMPLETION_RATE,
            "funded_rate": _DEFAULT_FUNDED_RATE,
            "source": rates_source,
        },
        "required_per_month": {
            "outreaches": needed_outreaches,
            "replies": needed_replies,
            "calls_booked": needed_booked,
            "calls_completed": needed_completed,
        },
        "actual_per_month": {
            "outreaches": actual_o_monthly,
            "replies": actual_r_monthly,
            "calls_booked": "tracked_in_opportunity_state_file",
            "calls_completed": "tracked_in_opportunity_state_file",
        },
        "starving_stage": starving_stage,
        "starving_deficit_per_month": starving_deficit,
    }


def _handle_get_scoreboard(tool_input: Dict[str, Any], db: Session) -> Any:
    cutoff_7d = date.today() - timedelta(days=7)
    try:
        out_row = db.execute(text("""
            SELECT COUNT(*) AS cnt FROM outbound_drafts
            WHERE venture_key = :vk AND created_at >= :cutoff
              AND status NOT IN ('draft', 'rejected', 'expired')
        """), {"vk": FA_MAX_VENTURE_KEY, "cutoff": cutoff_7d}).mappings().first()
        outreaches = int(out_row["cnt"]) if out_row else 0

        rep_row = db.execute(text("""
            SELECT COUNT(DISTINCT opportunity_thread_id) AS cnt FROM outbound_drafts
            WHERE venture_key = :vk AND replied_at >= :cutoff
        """), {"vk": FA_MAX_VENTURE_KEY, "cutoff": cutoff_7d}).mappings().first()
        replies = int(rep_row["cnt"]) if rep_row else 0

        whale_row = db.execute(text("""
            SELECT COUNT(*) AS cnt FROM buyer_entities
            WHERE is_whale = true AND opportunity_thread_id IS NOT NULL
        """)).mappings().first()
        whales = int(whale_row["cnt"]) if whale_row else 0

    except Exception as exc:
        logger.warning("tools.scoreboard DB query failed: %s", exc)
        return {"error": str(exc)}

    reply_rate = round(replies / outreaches, 4) if outreaches > 0 else None
    return {
        "as_of": str(date.today()),
        "period": "last_7_days",
        "outreaches_sent": outreaches,
        "replies_received": replies,
        "reply_rate": reply_rate,
        "active_whale_targets": whales,
    }


def _handle_search_opportunity(tool_input: Dict[str, Any], db: Session) -> Any:
    name = str(tool_input.get("name", "")).strip()
    if not name:
        return {"error": "name is required"}
    try:
        rows = db.execute(text("""
            SELECT be.id, be.canonical_name, be.entity_type,
                   be.is_whale, be.total_purchase_count, be.total_cash_volume,
                   be.opportunity_thread_id, be.whale_flagged_at,
                   od.status AS latest_draft_status, od.created_at AS last_outreach_at
            FROM buyer_entities be
            LEFT JOIN LATERAL (
                SELECT status, created_at FROM outbound_drafts
                WHERE opportunity_thread_id = be.opportunity_thread_id
                  AND venture_key = :vk
                ORDER BY created_at DESC LIMIT 1
            ) od ON true
            WHERE be.canonical_name ILIKE :pattern
            ORDER BY be.is_whale DESC, be.total_cash_volume DESC
            LIMIT 10
        """), {"vk": FA_MAX_VENTURE_KEY, "pattern": f"%{name}%"}).mappings().all()
        return {"results": [dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        logger.warning("tools.search_opportunity failed: %s", exc)
        return {"error": str(exc)}


# ──────────────────────────────────────────────────────────────────────────────
# Dispatcher — always returns (json_string, duration_ms), never raises
# ──────────────────────────────────────────────────────────────────────────────

_HANDLERS = {
    "query_db": _handle_query_db,
    "evaluate_deal": _handle_evaluate_deal,
    "get_backward_math": _handle_get_backward_math,
    "get_scoreboard": _handle_get_scoreboard,
    "search_opportunity": _handle_search_opportunity,
}


def dispatch_tool(
    tool_name: str,
    tool_input: Dict[str, Any],
    db: Session,
) -> tuple[str, int]:
    """
    Dispatch one tool call.  Returns (tool_result_json, duration_ms).

    The result is always prefixed "DATA: " so Claude's system prompt can
    instruct it to treat the content as data, not as instructions.
    Never raises — a failed handler produces an error dict instead.
    """
    handler = _HANDLERS.get(tool_name)
    if handler is None:
        result: Any = {"error": f"Unknown tool: {tool_name!r}"}
        return f"DATA: {json.dumps(result, default=str)}", 0

    start = time.monotonic()
    try:
        result = handler(tool_input, db)
    except Exception as exc:
        logger.warning("dispatch_tool %r raised unexpectedly: %s", tool_name, exc)
        result = {"error": str(exc)}
    duration_ms = int((time.monotonic() - start) * 1000)
    return f"DATA: {json.dumps(result, default=str)}", duration_ms
