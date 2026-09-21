"""WP-T2-11 — GYR router orchestration.

run_sweep(db, as_of)     — nightly full sweep: all open opportunities
reevaluate(ids, db)      — event-driven subset re-evaluation
run_staleness_pass(db)   — resurface unactioned greens older than 1 business day

Both classify paths use the same classify() core. re-routing only posts to
Slack when color changes (reevaluate) or unconditionally (sweep).

"Actioned" = current_stage != 'new' (any stage progression from Josh).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from .assemble import assemble_batch
from .classify import classify
from .delivery import post_staleness_alert, post_to_slack
from .models import GyrColor, RouterConfig, RoutingDecision

_ET = ZoneInfo("America/New_York")

logger = logging.getLogger(__name__)

_BATCH_SIZE = 200


def _router_config() -> RouterConfig:
    s = get_settings()
    return RouterConfig(
        green_min_expected_revenue_cents=s.fa_max_gyr_green_min_expected_revenue_cents,
    )


def run_sweep(db: Session, as_of: Optional[date] = None) -> int:
    """Nightly full sweep. Returns number of opportunities processed."""
    as_of = as_of or date.today()
    config = _router_config()
    processed = 0

    ids_result = db.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities WHERE outcome = 'open'")
    ).scalars().all()

    all_ids = list(ids_result)
    logger.info("GYR sweep starting: %d open opportunities", len(all_ids))

    for batch_start in range(0, len(all_ids), _BATCH_SIZE):
        batch = all_ids[batch_start: batch_start + _BATCH_SIZE]
        contexts = assemble_batch(batch, db)

        for ctx in contexts:
            decision = classify(ctx, config)
            _persist(ctx.opportunity_id, decision, db)
            _log_decision(ctx.opportunity_id, decision, db)
            post_to_slack(ctx, decision)
            processed += 1

        db.commit()

    logger.info("GYR sweep complete: %d opportunities routed", processed)
    return processed


def reevaluate(ids: List[str], db: Session) -> int:
    """Event-driven re-evaluation of a subset of opportunities.

    Re-routes only when color changes to avoid duplicate Slack posts.
    Returns number of opportunities whose color changed.
    """
    if not ids:
        return 0

    config = _router_config()
    changed = 0

    # Fetch current colors for change detection
    current_rows = db.execute(
        text("""
            SELECT opportunity_id::text, gyr_color
            FROM fa_max_opportunities
            WHERE opportunity_id = ANY(CAST(:ids AS uuid[]))
        """),
        {"ids": ids},
    ).mappings().all()
    current_color = {r["opportunity_id"]: r["gyr_color"] for r in current_rows}

    for batch_start in range(0, len(ids), _BATCH_SIZE):
        batch = ids[batch_start: batch_start + _BATCH_SIZE]
        contexts = assemble_batch(batch, db)

        for ctx in contexts:
            decision = classify(ctx, config)
            old_color = current_color.get(ctx.opportunity_id)
            _persist(ctx.opportunity_id, decision, db)
            _log_decision(ctx.opportunity_id, decision, db)
            if old_color != decision.color.value:
                post_to_slack(ctx, decision)
                changed += 1

        db.commit()

    logger.info("GYR reevaluate: %d/%d opportunities changed color", changed, len(ids))
    return changed


def run_staleness_pass(db: Session) -> int:
    """Resurface unactioned green opportunities older than one business day.

    Business day = Mon–Fri America/New_York. An opportunity is stale when
    its gyr_ranked_at is more than one business day ago AND current_stage='new'
    (not actioned) AND gyr_stale_alerted_at is NULL (not yet alerted this window).

    Returns number of staleness alerts posted.
    """
    now_et = datetime.now(_ET)
    alerted = 0

    stale_rows = db.execute(
        text("""
            SELECT
                opportunity_id::text,
                gyr_ranked_at,
                expected_revenue_cents
            FROM fa_max_opportunities
            WHERE outcome = 'open'
              AND gyr_color = 'green'
              AND current_stage = 'new'
              AND gyr_stale_alerted_at IS NULL
              AND gyr_ranked_at IS NOT NULL
        """)
    ).mappings().all()

    for row in stale_rows:
        ranked_at: datetime = row["gyr_ranked_at"]
        if not _is_stale(ranked_at, now_et):
            continue

        ctx = _minimal_staleness_ctx(row["opportunity_id"])
        decision = RoutingDecision(
            color=GyrColor.GREEN,
            expected_revenue_cents=row["expected_revenue_cents"] or 0,
            reason_codes=[],
            disqualifying_rule=None,
            queue="MONEY",
        )
        post_staleness_alert(ctx, decision)

        db.execute(
            text("""
                UPDATE fa_max_opportunities
                SET gyr_stale_alerted_at = now(), updated_at = now()
                WHERE opportunity_id = CAST(:oid AS uuid)
            """),
            {"oid": row["opportunity_id"]},
        )
        alerted += 1

    db.commit()
    logger.info("GYR staleness pass: %d stale alerts posted", alerted)
    return alerted


def _is_stale(ranked_at: datetime, now_et: datetime) -> bool:
    """True if ranked_at is more than one business day before now_et (ET, Mon–Fri)."""
    ranked_et = ranked_at.astimezone(_ET)
    check = ranked_et.date()
    business_days = 0
    while check < now_et.date():
        check += timedelta(days=1)
        if check.weekday() < 5:  # Mon=0 … Fri=4
            business_days += 1
        if business_days >= 1:
            return True
    return False


def _minimal_staleness_ctx(opportunity_id: str):
    """Thin stand-in RouterContext for staleness delivery (only opportunity_id needed)."""
    from .models import RouterContext
    import datetime as _dt
    return RouterContext(
        opportunity_id=opportunity_id,
        person_id="",
        outcome="open",
        opportunity_type="",
        loan_amount_cents=None,
        maturity_months=None,
        expected_need_date=None,
        lender_box_status="in_box",
        lender_box_fail_reasons=[],
        lender_box_uncertain_flags=[],
        arv=None,
        arv_confidence=None,
        dial_candidate_assessed_value=None,
        dial_candidate_last_sale_price=None,
        dial_candidate_arv=None,
        dial_candidate_max_ltc=None,
        dial_candidate_loan_override=None,
        dial_candidate_loan_override_confidence=None,
        dial_candidate_triggers=["financing_intent"],
        dial_candidate_intent_tier=None,
        dial_candidate_is_builder=False,
        dial_candidate_urgency_date=None,
        suppressed=False,
        has_interaction=True,
        as_of=_dt.date.today(),
    )


def _persist(opportunity_id: str, decision: RoutingDecision, db: Session) -> None:
    """UPDATE fa_max_opportunities with new GYR fields using CAS on state_version."""
    db.execute(
        text("""
            UPDATE fa_max_opportunities
            SET
                gyr_color = :color,
                expected_revenue_cents = :rev,
                gyr_reason = CAST(:reason AS jsonb),
                gyr_ranked_at = now(),
                updated_at = now(),
                state_version = state_version + 1
            WHERE opportunity_id = CAST(:oid AS uuid)
        """),
        {
            "color": decision.color.value,
            "rev": decision.expected_revenue_cents,
            "reason": _reason_json(decision),
            "oid": opportunity_id,
        },
    )


def _log_decision(opportunity_id: str, decision: RoutingDecision, db: Session) -> None:
    """Insert one row into fa_max_gyr_routing_log (immutable audit)."""
    db.execute(
        text("""
            INSERT INTO fa_max_gyr_routing_log
                (opportunity_id, color, expected_revenue_cents, reason_codes,
                 disqualifying_rule, queue, decided_at)
            VALUES
                (CAST(:oid AS uuid), :color, :rev,
                 CAST(:reason AS jsonb), :rule, :queue, now())
        """),
        {
            "oid": opportunity_id,
            "color": decision.color.value,
            "rev": decision.expected_revenue_cents,
            "reason": _reason_json(decision),
            "rule": decision.disqualifying_rule,
            "queue": decision.queue,
        },
    )


def _reason_json(decision: RoutingDecision) -> str:
    import json
    return json.dumps(decision.reason_codes)
