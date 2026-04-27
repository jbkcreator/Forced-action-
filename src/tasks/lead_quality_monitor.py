"""
Lead quality monitor — Gold+ false-positive rate tracking.

For each SentLead row sent ~30 days ago, this task checks three signals:
  1. Deed transfer since send?    → property already sold   (false positive)
  2. Code violations now resolved? → signals were stale     (false positive)
  3. Still Gold+ qualified?        → score decay check      (borderline)

False-positive rate = (sold_count + resolved_count) / total_checked

Results are stored in lead_quality_snapshots. An ops alert fires immediately
if the rolling rate exceeds ALERT_THRESHOLD. A full breakdown email is sent
every Monday regardless of the rate.

Run daily after scoring completes (07:30 UTC):
  30 7 * * * cd /path/to/app && python -m src.tasks.lead_quality_monitor
"""

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from src.core.database import get_db_context
from src.core.models import CodeViolation, Deed, DistressScore, LeadQualitySnapshot, SentLead, Subscriber
from src.services.email import send_alert
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

def run_lead_quality_monitor(county_id: str = "hillsborough", dry_run: bool = False) -> dict:
    """
    Snapshot leads sent ~30 days ago and compute false-positive rate.

    Returns:
        dict with keys: snapshotted, false_positive_rate, sold, resolved, decayed, active, alerts_sent
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
    }

    with get_db_context() as session:
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

        logger.info("[LQM] %d sent leads in snapshot window (%s to %s)",
                    len(sent_rows), window_start.date(), window_end.date())

        new_snapshots = []
        for sl, _county in sent_rows:
            if _already_snapshotted(session, sl.property_id, sl.subscriber_id, sl.sent_at):
                results['skipped_existing'] += 1
                continue

            score_at_send, tier_at_send, signals_at_send = _get_score_at_send(
                session, sl.property_id, sl.sent_at
            )
            score_now, tier_now = _get_current_score(session, sl.property_id)
            still_gp = tier_now in GOLD_PLUS_TIERS if tier_now else False
            has_deed = _check_deed_transfer(session, sl.property_id, sl.sent_at)
            has_resolved = _check_resolved_signals(
                session, sl.property_id, sl.sent_at, signals_at_send
            )
            outcome = _classify_outcome(still_gp, has_deed, has_resolved)

            new_snapshots.append(LeadQualitySnapshot(
                property_id=sl.property_id,
                subscriber_id=sl.subscriber_id,
                county_id=county_id,
                sent_at=sl.sent_at,
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
            ))
            results[outcome] = results.get(outcome, 0) + 1
            results['snapshotted'] += 1

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
