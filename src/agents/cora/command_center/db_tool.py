"""
query_db tool — validated SELECT-only access to an explicit table allowlist.

Defense layers:
  1. Strip SQL comments before any analysis.
  2. Reject anything that isn't a single bare SELECT statement.
  3. Block write/DDL keywords and dangerous PG functions via token scan.
  4. Enforce ALLOWED_TABLES — table names extracted from FROM/JOIN clauses
     must all be in the allowlist.
  5. Append LIMIT if absent so runaway queries can't fetch unbounded rows.

The application-layer validator is a second line of defense. The primary
safeguard should be a dedicated read-only PG role with SELECT-only GRANTs
on the allowed tables only.
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

ROW_CAP = 100

ALLOWED_TABLES: frozenset[str] = frozenset([
    "buyer_entities",
    "outbound_drafts",
    "lender_box_programs",
    "lender_box_geographies",
])

# Human-readable schema shown in the system prompt so Claude knows what to query.
TABLE_SCHEMAS: Dict[str, str] = {
    "buyer_entities": (
        "id, canonical_name, entity_type, primary_mailing_address, "
        "confidence_score, verification_status, total_purchase_count, "
        "total_cash_volume, is_whale, whale_flagged_at, "
        "opportunity_thread_id, county_id"
    ),
    "outbound_drafts": (
        "draft_id, opportunity_thread_id, buyer_entity_id, venture_key, cell_id, "
        "status, replied_at, created_at. "
        "IMPORTANT: always filter WHERE venture_key = 'fa_max_lending'."
    ),
    "lender_box_programs": (
        "program_key, name, is_active, min_loan_amount, max_loan_amount, "
        "max_ltc, max_ltv, allowed_property_types, excluded_property_types, "
        "min_borrower_prior_loans, effective_date, expiry_date"
    ),
    "lender_box_geographies": (
        "id, program_key, state, county (NULL = state-wide), is_excluded"
    ),
}

# Keywords whose presence at the top statement level signals a write or DDL.
_WRITE_KEYWORDS: frozenset[str] = frozenset([
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "COPY", "VACUUM", "EXECUTE", "CALL", "REPLACE",
    "MERGE", "UPSERT", "EXPLAIN", "ANALYZE",
])

# Dangerous PG function patterns — even inside a SELECT these can read files
# or interact with the server process.
_DANGEROUS_FUNCS: re.Pattern = re.compile(
    r'\b(pg_read_file|pg_exec|pg_ls_dir|pg_stat_file|lo_import|lo_export|'
    r'dblink|dblink_exec|pg_sleep|pg_cancel_backend|pg_terminate_backend|'
    r'pg_reload_conf|pg_rotate_logfile|pg_start_backup|pg_stop_backup)\s*\(',
    re.IGNORECASE,
)

_FROM_JOIN_RE: re.Pattern = re.compile(
    r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

_LIMIT_RE: re.Pattern = re.compile(r'\bLIMIT\s+\d+', re.IGNORECASE)


def _strip_comments(sql: str) -> str:
    sql = re.sub(r'--[^\n]*', ' ', sql)
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    return sql


def validate_sql(sql: str) -> str:
    """
    Validate and normalise a SQL string for safe read-only execution.

    Returns the cleaned SQL (comments stripped, trailing semicolon removed).
    Raises ValueError with a user-safe message on any violation.
    """
    cleaned = _strip_comments(sql).strip().rstrip(";").strip()

    if not cleaned:
        raise ValueError("Empty SQL statement.")

    # Reject multiple statements — semicolons remaining after stripping one
    # trailing semicolon mean at least two statements.
    if ";" in cleaned:
        raise ValueError("Multiple statements are not allowed.")

    # First keyword must be SELECT.
    first_token = cleaned.split()[0].upper()
    if first_token != "SELECT":
        raise ValueError(f"Only SELECT statements are allowed (got {first_token!r}).")

    upper = cleaned.upper()

    # Block write/DDL keywords appearing anywhere as standalone tokens.
    for kw in _WRITE_KEYWORDS:
        if re.search(r'\b' + kw + r'\b', upper):
            raise ValueError(f"Disallowed keyword in query: {kw}.")

    # Block dangerous server-side functions.
    if _DANGEROUS_FUNCS.search(cleaned):
        raise ValueError("Disallowed server-side function detected.")

    # Table allowlist — all tables referenced in FROM/JOIN must be permitted.
    referenced = {m.group(1).lower() for m in _FROM_JOIN_RE.finditer(cleaned)}
    disallowed = referenced - ALLOWED_TABLES
    if disallowed:
        raise ValueError(
            f"Table(s) not in allowlist: {', '.join(sorted(disallowed))}. "
            f"Allowed: {', '.join(sorted(ALLOWED_TABLES))}."
        )

    return cleaned


def _inject_limit(sql: str) -> str:
    """Append LIMIT {ROW_CAP} if the query doesn't already have one."""
    if _LIMIT_RE.search(sql):
        return sql
    return f"{sql} LIMIT {ROW_CAP}"


def execute_query(db: Session, sql: str) -> Dict[str, Any]:
    """
    Validate and execute a SELECT query.  Returns a dict with "rows" (list of
    dicts) and "count".  On validation or execution error, returns an "error"
    key instead — never raises, so the tool_result is always well-formed.
    """
    try:
        clean = validate_sql(sql)
    except ValueError as exc:
        logger.warning("db_tool.validate_sql rejected: %s | sql=%r", exc, sql[:200])
        return {"error": str(exc)}

    limited = _inject_limit(clean)

    try:
        start = time.monotonic()
        rows = db.execute(text(limited)).mappings().all()
        duration_ms = int((time.monotonic() - start) * 1000)
        result_rows = [dict(r) for r in rows]
        logger.info(
            "db_tool.execute_query: rows=%d duration_ms=%d sql=%r",
            len(result_rows), duration_ms, limited[:120],
        )
        return {"rows": result_rows, "count": len(result_rows)}
    except Exception as exc:
        logger.warning("db_tool.execute_query failed: %s | sql=%r", exc, limited[:200])
        return {"error": f"Query execution failed: {exc}"}


def schema_description() -> str:
    """Return a formatted schema block for the system prompt."""
    lines = ["Available tables (SELECT only):"]
    for table, cols in TABLE_SCHEMAS.items():
        lines.append(f"  {table}: {cols}")
    return "\n".join(lines)
