"""WP-T2-12 — Card-Thread Fallback Responder.

Handles non-approve/reject replies from the authorized approver inside FA-Max
Relay card threads. Classifies intent via a single Haiku call, then either:

  - Answers with a parameterized read-only catalog query (simple_lookup), or
  - Posts a pointer to the Command Center channel (cc_query), or
  - Acknowledges and logs (other).

All DB reads use hardcoded parameterized sqlalchemy.text() queries — no LLM-authored SQL,
no free SELECT. Execution is wrapped in _execute_catalog_query() following the
command_center/db_tool.py never-raises, returns-dict pattern.

Writes one audit row to fa_max_thread_fallback_log per invocation.
In-thread posting goes through slack_post.post_thread_note.

Called from admin_router._handle_relay_thread_action (strict else-branch after the
existing approve/reject + pending-revision precedence).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Catalog — the ONLY lookup_ids the responder will answer directly.
# A data question with no match here is redirected to Command Center.
# ---------------------------------------------------------------------------
_CATALOG_IDS: frozenset[str] = frozenset(
    {"count_by_color", "top_uncalled_deal", "source_staleness", "deal_status"}
)

# Required params per lookup_id (any of these absent → downgrade to cc_query).
_REQUIRED_PARAMS: dict[str, list[str]] = {
    "deal_status": ["address"],
}

# Staleness threshold: sources with no new opportunity in this many days are "stale".
_SOURCE_STALE_DAYS = 7


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class Bucket(str, Enum):
    SIMPLE_LOOKUP = "simple_lookup"
    CC_QUERY = "cc_query"
    OTHER = "other"


@dataclass
class ClassifyResult:
    bucket: Bucket
    lookup_id: Optional[str] = None
    params: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Classify prompt
# ---------------------------------------------------------------------------

_CLASSIFY_SYSTEM = """\
You classify a Slack message from an authorized pipeline operator into one of three buckets.

Known lookup catalog (return "simple_lookup" for any of these — match on intent, not exact wording):

1. count_by_color
   Examples: "how many green deals?", "count yellows", "how many reds today?", "total open"
   params: { "color": "green"|"yellow"|"red"|null (null = all colors), "today": true|false }

2. top_uncalled_deal
   Examples: "biggest deal I haven't called", "top uncalled", "best green deal", "highest revenue open deal"
   params: {}

3. source_staleness
   Examples: "is Tracerfy stale?", "any stale sources?", "source status", "data feeds fresh?"
   params: { "source": "<named source>"|null (null = all sources) }

4. deal_status
   Examples: "what's the status of 4021 Bayshore?", "deal at 123 Main", "where is that Tampa deal?"
   params: { "address": "<partial or full address string>" }  ← REQUIRED

Return "cc_query" when the message asks for: deal evaluation, backward math, scoreboard,
analytics, pipeline forecasts, or any multi-step intelligence question not in the catalog.

Return "other" for: greetings, thanks, off-topic text, or anything with no data intent.

Respond with ONLY valid JSON. No explanation, no markdown fences.
Schema: {"bucket": "simple_lookup|cc_query|other", "lookup_id": string|null, "params": {...}}
"""


# ---------------------------------------------------------------------------
# Parse + validate Haiku output
# ---------------------------------------------------------------------------

def _parse_classify_response(raw: str) -> ClassifyResult:
    """Parse the Haiku JSON response and enforce catalog safety.

    Unknown lookup_ids and missing required params downgrade to cc_query so the
    caller gets a useful redirect rather than a silent error.
    """
    try:
        data = json.loads(raw.strip())
        bucket_str = data.get("bucket", "other")
        bucket = Bucket(bucket_str) if bucket_str in Bucket._value2member_map_ else Bucket.OTHER
        lookup_id: Optional[str] = data.get("lookup_id") or None
        params: dict[str, Any] = data.get("params") or {}

        if bucket == Bucket.SIMPLE_LOOKUP:
            # Guard 1: lookup_id must be in the catalog.
            if lookup_id not in _CATALOG_IDS:
                logger.warning(
                    "[ThreadFallback] unknown lookup_id %r — redirecting to CC", lookup_id
                )
                return ClassifyResult(bucket=Bucket.CC_QUERY)

            # Guard 2: required params must be present and non-empty.
            for required in _REQUIRED_PARAMS.get(lookup_id, []):
                if not params.get(required):
                    logger.warning(
                        "[ThreadFallback] lookup_id=%r missing required param %r — redirecting",
                        lookup_id,
                        required,
                    )
                    return ClassifyResult(bucket=Bucket.CC_QUERY)

        return ClassifyResult(bucket=bucket, lookup_id=lookup_id, params=params)

    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.warning("[ThreadFallback] classify parse failed: %s", exc)
        return ClassifyResult(bucket=Bucket.OTHER)


# ---------------------------------------------------------------------------
# Catalog query executor — db_tool.py pattern (never raises, returns dict)
# ---------------------------------------------------------------------------

def _execute_catalog_query(
    db: Session,
    sql: str,
    params: Optional[dict[str, Any]] = None,
    *,
    multi_row: bool = True,
) -> dict[str, Any]:
    """Execute a hardcoded parameterized catalog query.

    Follows the command_center/db_tool.execute_query never-raises, returns-dict
    convention. All SQL here is a compile-time constant — no LLM-authored SQL,
    no user input interpolated into the statement (only into bind params).

    Returns {"rows": [...], "count": N} for multi_row queries, or
            {"row": {...}|None} for single-row queries.
    On error returns {"error": "<message>"}.
    """
    try:
        result = db.execute(text(sql), params or {})
        if multi_row:
            rows = [dict(r) for r in result.mappings().all()]
            return {"rows": rows, "count": len(rows)}
        row = result.mappings().first()
        return {"row": dict(row) if row else None}
    except Exception as exc:
        logger.warning("[ThreadFallback] catalog query failed: %s | sql=%r", exc, sql[:120])
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Catalog queries — parameterized, read-only; all SQL is a compile-time constant
# ---------------------------------------------------------------------------

def _query_count_by_color(
    db: Session, color: Optional[str], today: bool
) -> list[dict[str, Any]]:
    sql = """
        SELECT gyr_color, COUNT(*) AS cnt
        FROM fa_max_opportunities
        WHERE outcome = 'open'
          AND (:color IS NULL OR gyr_color = :color)
          AND (:today = FALSE OR DATE(gyr_ranked_at AT TIME ZONE 'America/New_York') = CURRENT_DATE)
        GROUP BY gyr_color
        ORDER BY gyr_color
    """
    result = _execute_catalog_query(db, sql, {"color": color, "today": today})
    return result.get("rows", [])


def _query_top_uncalled_deal(db: Session) -> Optional[dict[str, Any]]:
    sql = """
        SELECT
            o.opportunity_id,
            o.gyr_color,
            o.expected_revenue_cents,
            o.current_stage,
            p.address
        FROM fa_max_opportunities o
        LEFT JOIN fa_max_opportunity_properties op
            ON op.opportunity_id = o.opportunity_id AND op.role = 'subject'
        LEFT JOIN properties p ON p.id = op.property_id
        WHERE o.outcome = 'open'
          AND o.gyr_color = 'green'
          AND NOT EXISTS (
              SELECT 1
              FROM dial_list_touch t
              WHERE t.opportunity_thread_id = o.opportunity_id::text
                AND t.action IN ('called', 'approved', 'marked_done')
          )
        ORDER BY o.expected_revenue_cents DESC NULLS LAST
        LIMIT 1
    """
    result = _execute_catalog_query(db, sql, multi_row=False)
    return result.get("row")


def _query_source_staleness(
    db: Session, source: Optional[str]
) -> list[dict[str, Any]]:
    """Return sources with no new opportunity in the last _SOURCE_STALE_DAYS days."""
    # _SOURCE_STALE_DAYS is an integer constant — safe to embed in the literal
    # INTERVAL string (not user input; cannot expand the query surface).
    sql = f"""
        SELECT source,
               MAX(created_at) AS last_seen,
               EXTRACT(DAY FROM now() - MAX(created_at))::int AS last_seen_days_ago
        FROM fa_max_opportunities
        WHERE (:source IS NULL OR source ILIKE :source_pattern)
        GROUP BY source
        HAVING MAX(created_at) < now() - INTERVAL '{_SOURCE_STALE_DAYS} days'
        ORDER BY last_seen ASC
    """
    result = _execute_catalog_query(
        db, sql,
        {"source": source, "source_pattern": f"%{source}%" if source else None},
    )
    return result.get("rows", [])


def _query_deal_status(db: Session, address: str) -> Optional[dict[str, Any]]:
    sql = """
        SELECT
            o.opportunity_id,
            o.gyr_color,
            o.current_stage,
            o.outcome,
            o.updated_at,
            p.address
        FROM fa_max_opportunities o
        LEFT JOIN fa_max_opportunity_properties op
            ON op.opportunity_id = o.opportunity_id AND op.role = 'subject'
        LEFT JOIN properties p ON p.id = op.property_id
        WHERE p.address ILIKE :address_pattern
           OR p.normalized_address ILIKE :address_pattern
        ORDER BY o.updated_at DESC
        LIMIT 1
    """
    result = _execute_catalog_query(db, sql, {"address_pattern": f"%{address}%"}, multi_row=False)
    return result.get("row")


# ---------------------------------------------------------------------------
# Result formatters — pure functions
# ---------------------------------------------------------------------------

def format_count_by_color_result(
    rows: list[dict[str, Any]], color_filter: Optional[str]
) -> str:
    if not rows:
        color_label = color_filter or "any color"
        return f"0 open opportunities matching {color_label}."

    parts = []
    for row in rows:
        color = row.get("gyr_color") or "uncolored"
        cnt = row.get("cnt", 0)
        parts.append(f"{cnt} {color}")

    return "Open opportunities: " + ", ".join(parts) + "."


def format_top_uncalled_deal_result(row: Optional[dict[str, Any]]) -> str:
    if not row:
        return "No uncalled green deals found right now."

    address = row.get("address") or "(no address)"
    revenue_cents = row.get("expected_revenue_cents") or 0
    revenue = f"${revenue_cents / 100:,.0f}"
    stage = row.get("current_stage") or "unknown stage"
    opp_id = str(row.get("opportunity_id") or "")[:8]

    return (
        f"Top uncalled green deal: {address} — {revenue} expected revenue, "
        f"stage: {stage} (#{opp_id})."
    )


def format_source_staleness_result(
    rows: list[dict[str, Any]], source_filter: Optional[str]
) -> str:
    if not rows:
        if source_filter:
            return f"{source_filter}: not stale (data received within last {_SOURCE_STALE_DAYS} days)."
        return f"All sources fresh — no stale feeds in the last {_SOURCE_STALE_DAYS} days."

    lines = []
    for row in rows:
        src = row.get("source") or "unknown"
        days = row.get("last_seen_days_ago") or "?"
        lines.append(f"• {src}: last seen {days} day(s) ago")

    return "Stale sources:\n" + "\n".join(lines)


def format_deal_status_result(
    row: Optional[dict[str, Any]], address_query: str
) -> str:
    if not row:
        return f'No deal found matching "{address_query}".'

    address = row.get("address") or address_query
    color = (row.get("gyr_color") or "uncolored").upper()
    stage = row.get("current_stage") or "unknown"
    outcome = row.get("outcome") or "unknown"
    updated = str(row.get("updated_at") or "")[:10]

    return (
        f"{address}: {color} | stage: {stage} | outcome: {outcome}"
        + (f" | updated: {updated}" if updated else "")
        + "."
    )


def build_redirect_text(cc_channel_id: str) -> str:
    channel_ref = f"<#{cc_channel_id}>" if cc_channel_id else "the Command Center channel"
    return (
        f"That's a pipeline-intelligence question — ask it in {channel_ref} "
        f"where the full analysis engine is available."
    )


def build_other_ack_text() -> str:
    return "Noted — flagged for follow-up."


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def _write_audit_log(
    db: Session,
    relay_item_id: int,
    slack_user_id: str,
    thread_ts: str,
    lane: str,
    raw_text: str,
    bucket: Bucket,
    lookup_id: Optional[str],
    reply_sent: str,
    tokens_in: int,
    tokens_out: int,
    cost_usd: float,
) -> None:
    """Write one row to fa_max_thread_fallback_log. Best-effort — never raises."""
    try:
        db.execute(
            text("""
                INSERT INTO fa_max_thread_fallback_log
                    (relay_item_id, slack_user_id, thread_ts, lane, raw_text, bucket,
                     lookup_id, reply_sent, tokens_in, tokens_out, cost_usd)
                VALUES
                    (:relay_item_id, :slack_user_id, :thread_ts, :lane, :raw_text, :bucket,
                     :lookup_id, :reply_sent, :tokens_in, :tokens_out, :cost_usd)
            """),
            {
                "relay_item_id": relay_item_id,
                "slack_user_id": slack_user_id,
                "thread_ts": thread_ts,
                "lane": lane or None,
                "raw_text": raw_text[:2000],
                "bucket": bucket.value,
                "lookup_id": lookup_id,
                "reply_sent": reply_sent[:4000],
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "cost_usd": cost_usd,
            },
        )
        db.commit()
    except Exception as exc:
        logger.error("[ThreadFallback] audit log write failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Catalog dispatch
# ---------------------------------------------------------------------------

def _run_catalog_lookup(result: ClassifyResult, db: Session) -> str:
    """Execute the matched catalog query and format the reply. Never raises."""
    try:
        lookup_id = result.lookup_id
        params = result.params

        if lookup_id == "count_by_color":
            color = params.get("color")
            today = bool(params.get("today", False))
            rows = _query_count_by_color(db, color=color, today=today)
            return format_count_by_color_result(rows, color_filter=color)

        if lookup_id == "top_uncalled_deal":
            row = _query_top_uncalled_deal(db)
            return format_top_uncalled_deal_result(row)

        if lookup_id == "source_staleness":
            source = params.get("source")
            rows = _query_source_staleness(db, source=source)
            return format_source_staleness_result(rows, source_filter=source)

        if lookup_id == "deal_status":
            address = params.get("address", "")
            row = _query_deal_status(db, address=address)
            return format_deal_status_result(row, address_query=address)

        # Should not reach here — _parse_classify_response guards catalog membership.
        logger.error("[ThreadFallback] unreachable: unknown lookup_id %r", lookup_id)
        return build_other_ack_text()

    except Exception as exc:
        logger.error("[ThreadFallback] catalog query failed for %r: %s", result.lookup_id, exc)
        return "Couldn't retrieve that data right now — try again or ask in Command Center."


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def handle_thread_fallback_reply(item: Any, event: dict) -> None:
    """Orchestrate classify → answer/redirect/ack for a non-command thread reply.

    Called from admin_router._handle_relay_thread_action (strict else-branch,
    authorized approver only). Never raises — all errors are logged.

    Args:
        item: relay_approval_queue ORM row (has .id, .slack_message_ts, .venture_key).
        event: Slack event dict from the socket payload.
    """
    from src.core.database import get_db_context
    from config.settings import get_settings

    raw_text: str = str(event.get("text") or "").strip()
    user_id: str = str(event.get("user") or "")
    thread_ts: str = str(item.slack_message_ts or "")

    tokens_in = tokens_out = 0
    cost_usd = 0.0
    bucket = Bucket.OTHER
    lookup_id: Optional[str] = None
    reply_text = build_other_ack_text()

    try:
        settings = get_settings()

        # ── 1. Classify ──────────────────────────────────────────────────────
        classify_resp = call_claude_with_usage(
            task_type="fa_max_thread_fallback",
            messages=[
                {
                    "role": "user",
                    "content": f"DATA: {raw_text}",
                }
            ],
            system=_CLASSIFY_SYSTEM,
            max_tokens=256,
        )
        tokens_in = classify_resp.get("input_tokens", 0)
        tokens_out = classify_resp.get("output_tokens", 0)
        cost_usd = classify_resp.get("cost_usd", 0.0)
        raw_classify = classify_resp.get("text") or ""

        classify_result = _parse_classify_response(raw_classify)
        bucket = classify_result.bucket
        lookup_id = classify_result.lookup_id

        # ── 2. Build reply ───────────────────────────────────────────────────
        # CC channel: CC_SLACK_CHANNEL env var takes precedence over RELATIONSHIPS.
        cc_channel = (
            settings.cc_slack_channel
            or settings.fa_max_slack_channel_relationships
            or ""
        )
        lane: str = getattr(item, "lane", "") or ""

        with get_db_context() as db:
            if bucket == Bucket.SIMPLE_LOOKUP:
                reply_text = _run_catalog_lookup(classify_result, db)
            elif bucket == Bucket.CC_QUERY:
                reply_text = build_redirect_text(cc_channel_id=cc_channel)
            else:
                reply_text = build_other_ack_text()

            _write_audit_log(
                db=db,
                relay_item_id=item.id,
                slack_user_id=user_id,
                thread_ts=thread_ts,
                lane=lane,
                raw_text=raw_text,
                bucket=bucket,
                lookup_id=lookup_id,
                reply_sent=reply_text,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                cost_usd=cost_usd,
            )

        # ── 3. Post reply in-thread via slack_post abstraction ───────────────
        from src.services.relay.slack_post import post_thread_note
        post_thread_note(item, reply_text)

    except Exception as exc:
        logger.error(
            "[ThreadFallback] unhandled error for item %s user %s: %s",
            getattr(item, "id", "?"),
            user_id,
            exc,
        )
