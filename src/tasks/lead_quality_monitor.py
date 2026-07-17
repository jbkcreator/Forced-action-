"""
Lead quality monitor — Gold+ false-positive rate tracking + B1-03 SLA auto-remediation.

For each SentLead / Delivery row sent ~30 days ago, this task checks three signals:
  1. Deed transfer since send?    → property already sold   (false positive)
  2. Code violations now resolved? → signals were stale     (false positive)
  3. Still Gold+ qualified?        → score decay check      (borderline)

False-positive rate = (sold_count + resolved_count) / total_checked

Results are stored in lead_quality_snapshots. An ops alert fires immediately
if the rolling rate exceeds ALERT_THRESHOLD. A full breakdown email is sent
every Monday regardless of the rate.

B1-03: a 'sold' or 'resolved' outcome now auto-remediates the customer, no
founder action required —
  - Delivery (Block 1 storefront, entitlement-model) → reject_delivery() grants
    a same-grade replacement credit.
  - SentLead (lead_unlock_payment only — a single $ charge for a single lead) →
    a full Stripe refund is the correct amount, so it's auto-issued.
  - SentLead (lead_pack) is deliberately NOT auto-refunded here: one
    stripe_payment_intent_id is shared across up to 5 leads in a pack (see
    lead_pack_fulfillment_sweep.py), so a full-PI refund would over-refund the
    other, good leads in the same purchase. A correct fix needs a per-lead
    partial refund sourced from platform_revenue_ledger (source_table=
    'sent_leads'), which is out of scope here — lead_pack quality issues stay
    on the existing alert-only path until that's built.
  - Everything else (free-tier daily_email) → not_applicable, alert-only as before.

Run daily after scoring completes (07:30 UTC):
  30 7 * * * cd /path/to/app && python -m src.tasks.lead_quality_monitor
"""

import argparse
import logging
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import CodeViolation, Deed, DistressScore, LeadQualitySnapshot, SentLead, Subscriber
from src.services.email import send_alert
from src.services.lead_delivery import reject_delivery
from src.tasks.load_validator import _record_alert_sent, _was_recently_alerted
from config.scoring import PERSISTENCE_RESOLVED_KEYWORDS

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
SNAPSHOT_WINDOW_DAYS  = 30   # check leads sent this many days ago
SNAPSHOT_TOLERANCE    = 2    # ±N days to catch missed runs (28–32 day window)
ALERT_THRESHOLD       = 0.20 # alert if false-positive rate exceeds 20%
GOLD_PLUS_TIERS       = frozenset({"Ultra Platinum", "Platinum", "Gold"})


# ── Per-lead helpers ──────────────────────────────────────────────────────────

def _get_score_at_send(session, property_id: int, sent_at: datetime):
    """
    Return (final_cds_score, lead_tier, distress_types) from the DistressScore
    row whose score_date is closest to (and not after) the send timestamp.
    Returns (None, None, []) if no historical score exists.
    """
    row = (
        session.query(DistressScore)
        .filter(
            DistressScore.property_id == property_id,
            DistressScore.score_date <= sent_at,
        )
        .order_by(DistressScore.score_date.desc())
        .first()
    )
    if row is None:
        return None, None, []
    return (
        float(row.final_cds_score) if row.final_cds_score else None,
        row.lead_tier,
        row.distress_types or [],
    )


def _get_current_score(session, property_id: int):
    """Return (final_cds_score, lead_tier) from the most recent DistressScore row."""
    row = (
        session.query(DistressScore)
        .filter(DistressScore.property_id == property_id)
        .order_by(DistressScore.score_date.desc())
        .first()
    )
    if row is None:
        return None, None
    return (
        float(row.final_cds_score) if row.final_cds_score else None,
        row.lead_tier,
    )


def _check_deed_transfer(session, property_id: int, sent_at: datetime, window_days: int = 30) -> bool:
    """Return True if a deed was recorded between sent_at and sent_at + window_days."""
    cutoff = sent_at + timedelta(days=window_days)
    row = (
        session.query(Deed.id)
        .filter(
            Deed.property_id == property_id,
            Deed.record_date >= sent_at.date(),
            Deed.record_date <= cutoff.date(),
        )
        .first()
    )
    return row is not None


def _check_resolved_signals(session, property_id: int, sent_at: datetime, signals_at_send: list) -> bool:
    """
    Return True if any code violation that existed at send time is now marked resolved.
    Only meaningful when 'code_violations' is in signals_at_send.
    Uses the same PERSISTENCE_RESOLVED_KEYWORDS as the CDS engine.
    """
    if 'code_violations' not in signals_at_send:
        return False

    violations = (
        session.query(CodeViolation.status)
        .filter(
            CodeViolation.property_id == property_id,
            CodeViolation.date_added <= sent_at.date(),
            CodeViolation.status.isnot(None),
        )
        .all()
    )
    for (status,) in violations:
        s = status.lower()
        if any(kw in s for kw in PERSISTENCE_RESOLVED_KEYWORDS):
            return True
    return False


def _classify_outcome(still_gold_plus: bool, has_deed: bool, has_resolved: bool) -> str:
    """Priority order: sold > resolved > decayed > active."""
    if has_deed:
        return 'sold'
    if has_resolved:
        return 'resolved'
    if not still_gold_plus:
        return 'decayed'
    return 'active'


# ── Snapshot already exists guard ─────────────────────────────────────────────

def _already_snapshotted(session, property_id: int, subscriber_id: int, sent_at: datetime) -> bool:
    row = (
        session.query(LeadQualitySnapshot.id)
        .filter(
            LeadQualitySnapshot.property_id == property_id,
            LeadQualitySnapshot.subscriber_id == subscriber_id,
            LeadQualitySnapshot.sent_at == sent_at,
        )
        .first()
    )
    return row is not None


# ── B1-03: entitlement-model (Delivery) source rows ──────────────────────────

def _fetch_delivery_rows(session, county_id: str, window_start: datetime, window_end: datetime):
    """Deliveries (Block 1 storefront) in the snapshot window, still 'delivered'
    (not already rejected), joined through customer_accounts to reach the
    subscriber's county filter and subscriber_id for the snapshot row."""
    return session.execute(text("""
        SELECT d.id AS delivery_id, d.property_id, d.account_id, d.delivered_at,
               ca.subscriber_id
        FROM deliveries d
        JOIN customer_accounts ca ON ca.account_id = d.account_id
        JOIN subscribers s ON s.id = ca.subscriber_id
        WHERE d.delivered_at >= :window_start AND d.delivered_at <= :window_end
          AND d.status = 'delivered'
          AND s.county_id = :county_id
    """), {"window_start": window_start, "window_end": window_end, "county_id": county_id}).fetchall()


# ── B1-03: auto-remediation ───────────────────────────────────────────────────

def _auto_refund_sent_lead(session, sent_lead: SentLead, reason: str, now: datetime) -> str:
    """Issue a full Stripe refund for a stale lead_unlock_payment purchase
    (one payment_intent = one lead, so a full-PI refund is the correct
    amount). Mirrors the manual admin refund path in admin_router.py.
    Returns 'refund_issued' or 'refund_failed'; never raises."""
    if not settings.active_stripe_secret_key:
        logger.warning("[LQM] Stripe not configured — cannot auto-refund SentLead %s", sent_lead.id)
        return "refund_failed"

    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
    try:
        refund = stripe.Refund.create(
            payment_intent=sent_lead.stripe_payment_intent_id,
            idempotency_key=f"lqm-refund-{sent_lead.stripe_payment_intent_id}",
        )
    except stripe.error.StripeError as exc:
        logger.error("[LQM] Auto-refund failed for SentLead %s: %s", sent_lead.id, exc)
        return "refund_failed"

    sent_lead.refunded_at = now
    sent_lead.refund_reason = reason
    sent_lead.stripe_refund_id = refund.id
    logger.info(
        "[LQM] Auto-refund issued: sent_lead=%s refund=%s reason=%s",
        sent_lead.id, refund.id, reason,
    )
    return "refund_issued"


def _remediate(
    session,
    outcome: str,
    *,
    delivery_id: Optional[int] = None,
    sent_lead: Optional[SentLead] = None,
    now: Optional[datetime] = None,
    dry_run: bool = False,
) -> tuple[str, Optional[datetime]]:
    """B1-03: decide + (unless dry_run) perform the auto-remediation for one
    classified lead. Returns (remediation_action, remediated_at).

    - Delivery-sourced (entitlement/storefront) → reject_delivery() credit.
    - SentLead-sourced, lead_unlock_payment only, not yet refunded → Stripe
      refund (a single $ charge for a single lead, so a full-PI refund is the
      correct amount). lead_pack is intentionally excluded — see module
      docstring — because its payment_intent is shared across multiple leads.
    - Everything else (free-tier daily_email, lead_pack, already remediated)
      → no-op.
    """
    if outcome not in ("sold", "resolved"):
        return "not_applicable", None

    reason = "sold_before_delivery" if outcome == "sold" else "signals_resolved"
    now = now or datetime.now(timezone.utc)

    if delivery_id is not None:
        if dry_run:
            return "credit_issued", now
        reject_delivery(session, delivery_id, reason, now=now)
        return "credit_issued", now

    if (
        sent_lead is not None
        and sent_lead.source == "lead_unlock_payment"
        and sent_lead.stripe_payment_intent_id
        and not sent_lead.refunded_at
    ):
        if dry_run:
            return "refund_issued", now
        action = _auto_refund_sent_lead(session, sent_lead, reason, now)
        return action, now

    return "not_applicable", None


# ── Aggregate helpers ─────────────────────────────────────────────────────────

def _compute_rate(session, county_id: str, since_days: int = 30) -> dict:
    """Return aggregate false-positive stats for snapshots taken within `since_days`."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    rows = (
        session.query(LeadQualitySnapshot.outcome)
        .join(Subscriber, Subscriber.id == LeadQualitySnapshot.subscriber_id)
        .filter(
            LeadQualitySnapshot.county_id == county_id,
            LeadQualitySnapshot.snapshot_at >= cutoff,
        )
        .all()
    )
    counts = {'active': 0, 'decayed': 0, 'sold': 0, 'resolved': 0}
    for (outcome,) in rows:
        counts[outcome] = counts.get(outcome, 0) + 1
    total = sum(counts.values())
    fp = counts['sold'] + counts['resolved']
    return {
        **counts,
        'total': total,
        'false_positive_rate': round(fp / total, 4) if total > 0 else 0.0,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run_lead_quality_monitor(
    county_id: str = "hillsborough",
    dry_run: bool = False,
    db: Optional[Session] = None,
) -> dict:
    """
    Snapshot leads sent ~30 days ago, compute false-positive rate, and (B1-03)
    auto-remediate sold/resolved leads with a replacement credit or refund.

    Args:
        db: Injected session (used in tests). If None, opens get_db_context().

    Returns:
        dict with keys: snapshotted, false_positive_rate, sold, resolved, decayed,
        active, alerts_sent, credits_issued, refunds_issued, refunds_failed
    """
    today = date.today()
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=SNAPSHOT_WINDOW_DAYS + SNAPSHOT_TOLERANCE)
    window_end   = now - timedelta(days=SNAPSHOT_WINDOW_DAYS - SNAPSHOT_TOLERANCE)

    results = {
        'date': str(today),
        'county_id': county_id,
        'snapshotted': 0,
        'skipped_existing': 0,
        'false_positive_rate': 0.0,
        'sold': 0, 'resolved': 0, 'decayed': 0, 'active': 0,
        'alerts_sent': 0,
        'credits_issued': 0, 'refunds_issued': 0, 'refunds_failed': 0,
    }

    def _process_lead(property_id, subscriber_id, sent_at, *, delivery_id=None, sent_lead=None):
        """Classify one lead and (unless dry_run) auto-remediate it. Returns the
        LeadQualitySnapshot to persist, or None if already snapshotted."""
        if _already_snapshotted(session, property_id, subscriber_id, sent_at):
            results['skipped_existing'] += 1
            return None

        score_at_send, tier_at_send, signals_at_send = _get_score_at_send(
            session, property_id, sent_at
        )
        score_now, tier_now = _get_current_score(session, property_id)
        still_gp = tier_now in GOLD_PLUS_TIERS if tier_now else False
        has_deed = _check_deed_transfer(session, property_id, sent_at)
        has_resolved = _check_resolved_signals(session, property_id, sent_at, signals_at_send)
        outcome = _classify_outcome(still_gp, has_deed, has_resolved)

        remediation_action, remediated_at = _remediate(
            session, outcome, delivery_id=delivery_id, sent_lead=sent_lead,
            now=now, dry_run=dry_run,
        )
        if remediation_action == "credit_issued":
            results['credits_issued'] += 1
        elif remediation_action == "refund_issued":
            results['refunds_issued'] += 1
        elif remediation_action == "refund_failed":
            results['refunds_failed'] += 1

        results[outcome] = results.get(outcome, 0) + 1
        results['snapshotted'] += 1

        return LeadQualitySnapshot(
            property_id=property_id,
            subscriber_id=subscriber_id,
            county_id=county_id,
            sent_at=sent_at,
            snapshot_at=now,
            score_at_send=score_at_send,
            tier_at_send=tier_at_send,
            signals_at_send=signals_at_send,
            score_at_snapshot=score_now,
            tier_at_snapshot=tier_now,
            still_gold_plus=still_gp,
            has_deed_transfer=has_deed,
            has_resolved_signals=has_resolved,
            outcome=outcome,
            delivery_id=delivery_id,
            remediation_action=remediation_action,
            remediated_at=remediated_at,
        )

    with (nullcontext(db) if db is not None else get_db_context()) as session:
        # Leads in the snapshot window, joined to subscriber county filter
        sent_rows = (
            session.query(SentLead, Subscriber.county_id)
            .join(Subscriber, Subscriber.id == SentLead.subscriber_id)
            .filter(
                SentLead.sent_at >= window_start,
                SentLead.sent_at <= window_end,
                Subscriber.county_id == county_id,
            )
            .all()
        )
        delivery_rows = _fetch_delivery_rows(session, county_id, window_start, window_end)

        logger.info(
            "[LQM] %d sent leads + %d deliveries in snapshot window (%s to %s)",
            len(sent_rows), len(delivery_rows), window_start.date(), window_end.date(),
        )

        new_snapshots = []
        for sl, _county in sent_rows:
            snap = _process_lead(sl.property_id, sl.subscriber_id, sl.sent_at, sent_lead=sl)
            if snap is not None:
                new_snapshots.append(snap)

        for row in delivery_rows:
            snap = _process_lead(
                row.property_id, row.subscriber_id, row.delivered_at,
                delivery_id=row.delivery_id,
            )
            if snap is not None:
                new_snapshots.append(snap)

        if not dry_run and new_snapshots:
            session.bulk_save_objects(new_snapshots)
            session.commit()
            logger.info("[LQM] Saved %d new snapshots", len(new_snapshots))

        # ── Compute rolling 30-day rate from full history ──────────────────
        stats = _compute_rate(session, county_id, since_days=30)
        results['false_positive_rate'] = stats['false_positive_rate']
        logger.info(
            "[LQM] 30d false-positive rate=%.1f%% (sold=%d resolved=%d decayed=%d active=%d total=%d)",
            stats['false_positive_rate'] * 100,
            stats['sold'], stats['resolved'], stats['decayed'], stats['active'], stats['total'],
        )

    # ── Alert if rate exceeds threshold ───────────────────────────────────────
    if stats['total'] > 0 and stats['false_positive_rate'] > ALERT_THRESHOLD:
        if _was_recently_alerted('_batch', county_id, 'high_fp_rate'):
            logger.info("[LQM] High FP-rate alert suppressed — cooldown active")
        else:
            subject = (
                f"[Forced Action] ALERT: Gold+ false-positive rate "
                f"{stats['false_positive_rate']*100:.0f}% ({today})"
            )
            body = (
                f"The Gold+ lead false-positive rate for {county_id} has exceeded "
                f"{ALERT_THRESHOLD*100:.0f}% over the last 30 days.\n\n"
                f"  Total leads snapshotted: {stats['total']}\n"
                f"  Sold (deed within 30d):  {stats['sold']}\n"
                f"  Resolved (CV closed):    {stats['resolved']}\n"
                f"  Decayed (score dropped): {stats['decayed']}\n"
                f"  Still active:            {stats['active']}\n\n"
                f"  False-positive rate: {stats['false_positive_rate']*100:.1f}%\n\n"
                f"Possible causes:\n"
                f"  - Score threshold (57) too low — borderline leads are being delivered\n"
                f"  - Age decay not aggressive enough for old signals\n"
                f"  - Properties are changing hands faster than the 60-day dead-sale filter\n\n"
                f"Forced Action Ops Alert — {now.strftime('%Y-%m-%d %H:%M UTC')}"
            )
            if not dry_run:
                sent = send_alert(subject=subject, body=body)
                if sent:
                    results['alerts_sent'] += 1
                    _record_alert_sent('_batch', county_id, 'high_fp_rate')
            else:
                logger.info("[LQM] [DRY RUN] Would send alert: %s", subject)

    # ── Weekly Monday report ───────────────────────────────────────────────────
    if today.weekday() == 0 and stats['total'] > 0:  # Monday
        _send_weekly_report(county_id, stats, now, dry_run=dry_run)
        results['alerts_sent'] += 1

    return results


def _send_weekly_report(county_id: str, stats: dict, now: datetime, dry_run: bool = False) -> None:
    """Send the Monday weekly lead quality digest."""
    subject = (
        f"[Forced Action] Weekly Lead Quality Report — "
        f"FP rate {stats['false_positive_rate']*100:.1f}% "
        f"({now.strftime('%Y-%m-%d')})"
    )
    status_line = (
        "✓ Within acceptable range" if stats['false_positive_rate'] <= ALERT_THRESHOLD
        else f"⚠ ABOVE threshold ({ALERT_THRESHOLD*100:.0f}%)"
    )
    body = (
        f"Gold+ Lead Quality — Weekly Report\n"
        f"County: {county_id}  |  Period: last 30 days\n"
        f"{'=' * 48}\n\n"
        f"  False-positive rate: {stats['false_positive_rate']*100:.1f}%  {status_line}\n\n"
        f"  Outcome breakdown:\n"
        f"    Active (still Gold+):         {stats['active']:>5}\n"
        f"    Decayed (score dropped):      {stats['decayed']:>5}\n"
        f"    Resolved (signals closed):    {stats['resolved']:>5}  ← false positive\n"
        f"    Sold (deed within 30d):       {stats['sold']:>5}  ← false positive\n"
        f"    ─────────────────────────────────\n"
        f"    Total snapshotted:            {stats['total']:>5}\n\n"
        f"Definitions:\n"
        f"  False positive = lead sent to subscriber, property already sold or\n"
        f"  primary signals already resolved at send time.\n"
        f"  Decayed = score dropped below Gold+ threshold after send (borderline).\n\n"
        f"To drill in:\n"
        f"  SELECT outcome, COUNT(*), AVG(score_at_send) FROM lead_quality_snapshots\n"
        f"  WHERE county_id='{county_id}' AND snapshot_at >= NOW() - INTERVAL '30 days'\n"
        f"  GROUP BY outcome;\n\n"
        f"Forced Action Ops Report — {now.strftime('%Y-%m-%d %H:%M UTC')}"
    )
    if dry_run:
        logger.info("[LQM] [DRY RUN] Would send weekly report: %s", subject)
        return
    send_alert(subject=subject, body=body)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Lead quality false-positive monitor")
    parser.add_argument("county_id", nargs="?", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true", help="Compute and log without writing to DB or sending alerts")
    args = parser.parse_args()

    result = run_lead_quality_monitor(county_id=args.county_id, dry_run=args.dry_run)
    print(result)
    sys.exit(0)
