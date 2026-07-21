"""
T-B8-03 Operator Dashboard — Action Queue.

Read-time union over three existing tables (cora_incident, human_close_escalations,
scraper_alert_log). No persistence, no new ledger. Maps each pending row to a
common ActionItem and splits into two lanes:

  - approvals  — things awaiting a human decision (legal | deal)
  - failures   — FYI, no in-queue action (ops | source)

Also exports canonical count helpers (cora_approvals_waiting, source_failures)
that B8-01's /summary calls, so the KPI cards and this queue never diverge.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

# Scraper alert_type → severity (scraper_alert_log has no severity column).
_SCRAPER_SEVERITY = {
    "scraper_error": "red",
    "zero_records": "red",
    "low_count": "yellow",
    "health_check": "info",
}

# Cora action_taken buckets (breach_resolved IS NULL is the open predicate).
_CORA_LEGAL = ("human_escalated", "feature_killed")   # approvals lane
_CORA_OPS = ("auto_paused", "fallback_enabled", "no_op")  # failures lane


def _cora_item(row: dict) -> dict[str, Any]:
    legal = row["action_taken"] in _CORA_LEGAL
    return {
        "type": "approval" if legal else "source_failure",
        "source": "cora",
        "category": "legal" if legal else "ops",
        "lane": "approvals" if legal else "failures",
        "id": row["id"],
        "title": f'{row["metric_name"]} breach on {row.get("feature_name") or "—"}',
        "subtitle": f'{row["action_taken"]} · {row.get("root_cause") or ""}'.strip(" ·"),
        "severity": row["severity"],
        "created_at": row["breach_started"],
        "amount_cents": None,
        "county_id": row.get("county_id"),
        "action_url": "/admin/cora?tab=incidents",
    }


def _human_close_item(row: dict) -> dict[str, Any]:
    return {
        "type": "approval",
        "source": "human_close",
        "category": "deal",
        "lane": "approvals",
        "id": row["id"],
        "title": f'{row["target_tier"]} escalation · {row.get("vertical") or "—"}',
        "subtitle": f'signal {row["revenue_signal_score"]}',
        "severity": None,
        "created_at": row["routed_at"],
        "amount_cents": row.get("target_tier_price_cents"),
        "county_id": None,
        "action_url": "/admin/closer",
    }


def _query_cora(session: Session) -> list[dict]:
    rows = session.execute(sa_text(
        """
        SELECT id, metric_name, feature_name, county_id, severity,
               action_taken, root_cause, breach_started
        FROM cora_incident
        WHERE breach_resolved IS NULL
          AND action_taken <> 'resolved'
        """
    )).mappings().all()
    return [_cora_item(r) for r in rows]


def _query_human_close(session: Session) -> list[dict]:
    rows = session.execute(sa_text(
        """
        SELECT id, target_tier, vertical, revenue_signal_score,
               target_tier_price_cents, routed_at
        FROM human_close_escalations
        WHERE outcome IS NULL
        """
    )).mappings().all()
    return [_human_close_item(r) for r in rows]


def _scraper_item(row: dict) -> dict[str, Any]:
    return {
        "type": "source_failure",
        "source": "scraper",
        "category": "source",
        "lane": "failures",
        "id": row["id"],
        "title": f'{row["alert_type"]}: {row["source_type"]}',
        "subtitle": row.get("county_id") or "",
        "severity": _SCRAPER_SEVERITY.get(row["alert_type"], "info"),
        "created_at": row["alerted_at"],
        "amount_cents": None,
        "county_id": row.get("county_id"),
        "action_url": "/admin/scrapers",
    }


def _scraper_cutoff() -> datetime:
    """Open scraper alerts = alerted within the same cooldown window
    load_validator uses to suppress duplicates (settings.alert_cooldown_hours)."""
    from config.settings import get_settings
    hours = get_settings().alert_cooldown_hours
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def _query_scraper(session: Session) -> list[dict]:
    rows = session.execute(
        sa_text(
            """
            SELECT id, source_type, county_id, alert_type, alerted_at
            FROM scraper_alert_log
            WHERE alerted_at >= :cutoff
            """
        ),
        {"cutoff": _scraper_cutoff()},
    ).mappings().all()
    return [_scraper_item(r) for r in rows]


def build_action_queue(session: Session) -> dict[str, Any]:
    items = _query_cora(session)
    items += _query_human_close(session)
    items += _query_scraper(session)

    approvals = sorted(
        (i for i in items if i["lane"] == "approvals"),
        key=lambda i: i["created_at"],
    )  # oldest-first: stalest approval is most urgent
    failures = sorted(
        (i for i in items if i["lane"] == "failures"),
        key=lambda i: i["created_at"],
        reverse=True,
    )  # newest-first: freshest failure most actionable

    # Counts derived from the built lanes so the KPI cards and the queue can
    # never drift. The standalone helpers below use the identical predicates.
    counts = {
        "approvals": len(approvals),
        "failures": len(failures),
        "cora_approvals_waiting": sum(
            1 for i in approvals if i["source"] == "cora" and i["category"] == "legal"
        ),
        "source_failures": sum(1 for i in failures if i["source"] == "scraper"),
    }
    return {"approvals": approvals, "failures": failures, "counts": counts}


# ── Canonical count helpers (B8-01's /summary calls these) ────────────────────

def cora_approvals_waiting(session: Session) -> int:
    """Open cora incidents awaiting a human decision (legal lane only) —
    excludes auto-handled incidents and human-close escalations."""
    return int(session.execute(sa_text(
        """
        SELECT COUNT(*) FROM cora_incident
        WHERE breach_resolved IS NULL
          AND action_taken IN ('human_escalated', 'feature_killed')
        """
    )).scalar_one())


def source_failures(session: Session) -> int:
    """Scraper alerts inside the active cooldown window."""
    return int(session.execute(
        sa_text(
            "SELECT COUNT(*) FROM scraper_alert_log WHERE alerted_at >= :cutoff"
        ),
        {"cutoff": _scraper_cutoff()},
    ).scalar_one())
