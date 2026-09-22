"""WP-T2-12 — FA Max Slack LLM Responder.

Answers plain-English operator questions in FA Max Slack lanes. Two entry points,
one shared classify/respond core:

  - handle_thread_fallback_reply: a non-approve/reject reply inside a relay card
    thread (acks 'other' so the typed reply is acknowledged).
  - handle_channel_message: any top-level message in a mapped lane channel
    (MONEY/EXCEPTIONS/RELATIONSHIPS/CC); stays silent on 'other' so ordinary
    channel chatter isn't answered.

Both classify intent via a single Haiku call (forced tool_use for a guaranteed
structured result — no prose/markdown parsing), then either:

  - Answer with a parameterized read-only catalog query (simple_lookup), or
  - Post a pointer to the Command Center channel (cc_query), or
  - Acknowledge / stay silent and log (other).

The channel path passes prior thread turns as conversation history so operator
follow-ups ("what about reds?") resolve against the earlier question.

All DB reads use hardcoded parameterized sqlalchemy.text() queries — no LLM-authored SQL,
no free SELECT. Execution is wrapped in _execute_catalog_query() following the
command_center/db_tool.py never-raises, returns-dict pattern.

Writes one audit row to fa_max_thread_fallback_log per invocation (relay_item_id
is NULL for channel messages). Posting goes through slack_post.post_note.

Authorized-approver gating is enforced by the caller
(admin_router._handle_relay_thread_action).
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
    SOCIAL = "social"
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
You classify the operator's LATEST Slack message into one of three buckets, then call the
classify_operator_message tool with the result. Earlier turns in the conversation are prior
context — use them to resolve follow-ups (e.g. after "how many green deals?", a later
"what about reds?" is count_by_color with color=red). Always classify the latest message.

Known lookup catalog (use bucket "simple_lookup" for any of these — match on intent, not exact wording):

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

Use bucket "cc_query" when the message asks for: deal evaluation, backward math, scoreboard,
analytics, pipeline forecasts, or any multi-step intelligence question not in the catalog.
ALSO use "cc_query" for:
- A vague follow-up you cannot map to a specific catalog lookup with concrete params
  (e.g. "what is it?", "tell me more", "why?", "which one?"). Do NOT guess — redirect instead.
- A message that asks for TWO OR MORE distinct things at once (compound / multi-intent questions,
  e.g. "how many greens AND is tracerfy stale?", "count yellows and show top deal"). Even if each
  part is individually in the catalog, the combination is a cc_query — pick one catalog item only
  when the message has exactly one clear intent.

Use bucket "social" for greetings, thanks, and encouragement directed at the team/assistant
("good morning team", "nice work", "thanks!", "great job"). These get a brief friendly reply.

Use bucket "other" ONLY for text with no data intent and no social intent: gibberish, random
numbers, off-topic chatter between people, status noise ("brb", "on a call"). These stay silent.
"""

# Tool schema — forcing tool_use guarantees a structured dict back (no prose, no
# markdown fences), so classification can never fail on output formatting.
CLASSIFY_TOOL: dict[str, Any] = {
    "name": "classify_operator_message",
    "description": "Route an operator's Slack message to a bucket with optional lookup + params.",
    "input_schema": {
        "type": "object",
        "properties": {
            "bucket": {
                "type": "string",
                "enum": ["simple_lookup", "cc_query", "social", "other"],
            },
            "lookup_id": {
                "type": ["string", "null"],
                "enum": [
                    "count_by_color",
                    "top_uncalled_deal",
                    "source_staleness",
                    "deal_status",
                    None,
                ],
                "description": "Required when bucket is simple_lookup; null otherwise.",
            },
            "params": {
                "type": "object",
                "description": "Lookup parameters (color/today, source, address). Empty when not needed.",
            },
        },
        "required": ["bucket"],
    },
}


# ---------------------------------------------------------------------------
# Parse + validate Haiku output
# ---------------------------------------------------------------------------

def _validate_classify(data: dict) -> ClassifyResult:
    """Enforce catalog safety on a classifier result dict.

    Unknown lookup_ids and missing required params downgrade to cc_query so the
    caller gets a useful redirect rather than a silent error. Shared by the
    tool_use path (dict straight from Haiku) and the legacy text-JSON fallback.
    """
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


def _parse_classify_response(raw: str) -> ClassifyResult:
    """Legacy text fallback: parse a JSON string, then validate.

    Only used when Haiku returns text instead of a tool_use block. Strips
    markdown fences defensively. The primary path is now forced tool_use, which
    returns a dict straight to _validate_classify.
    """
    try:
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = "\n".join(
                line for line in cleaned.splitlines()
                if not line.startswith("```")
            ).strip()
        return _validate_classify(json.loads(cleaned))
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


def build_social_reply(raw_text: str) -> str:
    """A brief, friendly reply to greetings/thanks/encouragement. Doubles as a
    gentle capability hint so operators know what they can ask. Deterministic —
    no LLM-generated prose, so it can't drift or hallucinate."""
    lower = raw_text.lower()
    if "thank" in lower or "nice" in lower or "great" in lower or "good job" in lower:
        return "Anytime! Ask me for deal counts, the top uncalled deal, source freshness, or a deal's status."
    return "👋 Hey! I can pull deal counts, the top uncalled deal, source freshness, or a deal's status — just ask."


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

def _coalesce_roles(messages: list[dict]) -> list[dict]:
    """Enforce the Anthropic messages contract: first turn is user, roles
    alternate. Drops leading assistant turns and merges consecutive same-role
    turns (join with newline) so a thread with two human messages in a row
    can't produce an invalid request.
    """
    out: list[dict] = []
    for m in messages:
        role = m["role"]
        content = m["content"]
        if not out and role != "user":
            continue
        if out and out[-1]["role"] == role:
            out[-1]["content"] += "\n" + content
        else:
            out.append({"role": role, "content": content})
    return out


def _classify_and_respond(
    *,
    raw_text: str,
    user_id: str,
    lane: str,
    audit_thread_ts: str,
    relay_item_id: Optional[int],
    silent_other: bool,
    history: Optional[list[dict]] = None,
) -> Optional[str]:
    """Classify one message, build the reply, and write the audit row.

    Returns the reply text to post, or None when nothing should be posted
    (silent_other and the message classified as 'other'). Never raises.

    `history` is prior thread turns ({"role": "user"|"assistant", "content": str})
    passed ahead of the current message so follow-ups like "what about reds?"
    resolve against the earlier question. Empty/None = single-shot classify.

    Shared by the card-thread path (handle_thread_fallback_reply) and the
    channel path (handle_channel_message); the caller owns where to post.
    """
    from src.core.database import get_db_context
    from config.settings import get_settings

    tokens_in = tokens_out = 0
    cost_usd = 0.0
    bucket = Bucket.OTHER
    lookup_id: Optional[str] = None
    reply_text = build_other_ack_text()

    try:
        settings = get_settings()

        messages = list(history or [])
        messages.append({"role": "user", "content": f"DATA: {raw_text}"})
        messages = _coalesce_roles(messages)

        classify_resp = call_claude_with_usage(
            task_type="fa_max_thread_fallback",
            messages=messages,
            system=_CLASSIFY_SYSTEM,
            cache_system=True,
            tools=[CLASSIFY_TOOL],
            tool_choice={"type": "tool", "name": CLASSIFY_TOOL["name"]},
            max_tokens=256,
        )
        tokens_in = classify_resp.get("input_tokens", 0)
        tokens_out = classify_resp.get("output_tokens", 0)
        cost_usd = classify_resp.get("cost_usd", 0.0)

        tool_input = classify_resp.get("tool_input")
        if isinstance(tool_input, dict):
            classify_result = _validate_classify(tool_input)
        else:
            # Fallback: model returned text instead of a tool_use block.
            classify_result = _parse_classify_response(classify_resp.get("text") or "")
        bucket = classify_result.bucket
        lookup_id = classify_result.lookup_id

        cc_channel = (
            settings.cc_slack_channel
            or settings.fa_max_slack_channel_relationships
            or ""
        )

        with get_db_context() as db:
            if bucket == Bucket.SIMPLE_LOOKUP:
                reply_text = _run_catalog_lookup(classify_result, db)
            elif bucket == Bucket.CC_QUERY:
                reply_text = build_redirect_text(cc_channel_id=cc_channel)
            elif bucket == Bucket.SOCIAL:
                reply_text = build_social_reply(raw_text)
            else:
                reply_text = build_other_ack_text()

            _write_audit_log(
                db=db,
                relay_item_id=relay_item_id,
                slack_user_id=user_id,
                thread_ts=audit_thread_ts,
                lane=lane,
                raw_text=raw_text,
                bucket=bucket,
                lookup_id=lookup_id,
                reply_sent=reply_text,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                cost_usd=cost_usd,
            )

        if silent_other and bucket == Bucket.OTHER:
            return None
        return reply_text

    except Exception as exc:
        logger.error(
            "[ThreadFallback] classify/respond error (item=%s user=%s): %s",
            relay_item_id, user_id, exc,
        )
        return None


def handle_thread_fallback_reply(item: Any, event: dict) -> None:
    """Card-thread path: a non-approve/reject reply inside a relay card thread.

    Authorized-approver only (enforced by the caller). Acks 'other' replies so
    the approver knows the typed reply was seen. Never raises.
    """
    reply = _classify_and_respond(
        raw_text=str(event.get("text") or "").strip(),
        user_id=str(event.get("user") or ""),
        lane=getattr(item, "lane", "") or "",
        audit_thread_ts=str(item.slack_message_ts or ""),
        relay_item_id=item.id,
        silent_other=False,
    )
    if reply is None:
        return
    from src.services.relay.slack_post import post_thread_note
    post_thread_note(item, reply)


def handle_channel_message(
    *, event: dict, venture_key: str, lane: str, channel: str
) -> None:
    """Channel path: any top-level message in a mapped FA Max lane channel.

    Authorized-approver only (enforced by the caller). Replies threaded under
    the operator's own message. Stays silent on 'other' (greetings/thanks) so
    ordinary channel chatter isn't answered. Never raises.
    """
    # Thread the reply under the message itself; if it's already a reply in a
    # non-card thread, stay in that thread.
    reply_thread_ts = str(event.get("thread_ts") or event.get("ts") or "")

    # Follow-up context: when the operator replies inside an existing thread,
    # pull the prior turns so questions like "what about reds?" resolve against
    # the earlier question. A top-level message (no thread_ts) has no history.
    history: list[dict] = []
    incoming_thread_ts = str(event.get("thread_ts") or "")
    if incoming_thread_ts:
        from src.services.relay.slack_post import fetch_thread_history
        history = fetch_thread_history(
            venture_key=venture_key,
            channel=channel,
            thread_ts=incoming_thread_ts,
            exclude_ts=str(event.get("ts") or ""),
        )

    reply = _classify_and_respond(
        raw_text=str(event.get("text") or "").strip(),
        user_id=str(event.get("user") or ""),
        lane=lane,
        audit_thread_ts=reply_thread_ts,
        relay_item_id=None,
        silent_other=True,
        history=history,
    )
    if reply is None:
        return
    from src.services.relay.slack_post import post_note
    post_note(
        venture_key=venture_key,
        channel=channel,
        thread_ts=reply_thread_ts,
        text=reply,
    )
