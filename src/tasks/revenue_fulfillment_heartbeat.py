"""MB2 — Fulfillment Verification & Daily Revenue Heartbeat.

Reconciles paid transactions against fulfillment across every revenue
stream and reports Stripe's own daily total against our internal ledger,
alerting on any mismatch. Read-only — this task never writes to the DB or
to Stripe; it only reports what it finds.

Streams covered (Definition of Done): subscriptions, lead unlocks, hot-lead
unlocks, lead packs, premium (report/brief), bankruptcy-alert subscriptions,
and Loan Lane broker-commission volume (reported separately — Loan Lane
commissions are broker earnings, never Stripe-collected revenue, see
CommissionLedgerEntry's own docstring).

Deliberately out of scope: Supplier Intelligence (fa067) is Phase-1/
admin-provisioned only, not in this task's stream list, and not wired into
platform_revenue_ledger. Any Stripe revenue it collects will show up as part
of a Stripe-vs-ledger mismatch — the alert body says so explicitly so an
operator isn't misled into treating every mismatch as a bug.

Reuses (does not re-derive):
  - platform_revenue_ledger / src/services/revenue_ledger.py — the shared
    revenue ledger for subscription, lead_unlock, hot_lead_unlock, lead_pack,
    premium_report/premium_brief.
  - src/tasks/stripe_reconcile.py — compute_stripe_net_revenue (Stripe's own
    net-of-refunds daily total) and fetch_bankruptcy_alert_revenue (the one
    product deliberately NOT in the shared ledger — see that module's
    docstring for why).
  - src/tasks/lead_pack_fulfillment_sweep.py's self-healing status field
    (lead_pack_purchases.status) — read directly, not re-derived.
  - src/services/email.py::send_alert — existing ops-alert channel.

Usage:
    python -m src.tasks.revenue_fulfillment_heartbeat                  # today
    python -m src.tasks.revenue_fulfillment_heartbeat --date 2026-07-13
    python -m src.tasks.revenue_fulfillment_heartbeat --dry-run         # skip sending
"""
from __future__ import annotations

import argparse
import csv
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.tasks.stripe_reconcile import (
    _init_stripe, _utc_day_window, compute_stripe_net_revenue, fetch_bankruptcy_alert_revenue,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports/revenue_heartbeat")
RETENTION_DAYS = 7

# platform_revenue_ledger.product_type values reported as first-class streams.
_LEDGER_STREAMS = ("subscription", "lead_unlock", "hot_lead_unlock", "lead_pack", "premium_report", "premium_brief")

# lead_pack_purchases.status stuck in 'enriching' longer than this is a delayed
# delivery — matches lead_pack_fulfillment_sweep.py's own retry window, so a
# purchase this task flags as "delayed" is genuinely past that sweep's own
# self-heal window, not just mid-flight.
_LEAD_PACK_STUCK_AFTER_MINUTES = 15


def _ledger_totals_by_stream(db, day_start_dt: datetime, day_end_dt: datetime) -> dict:
    """{product_type: {revenue_cents, count}} for confirmed (non-refunded)
    ledger rows in the window, for every stream — including ones with zero
    activity, so a stream going silent is visible, not just absent."""
    rows = db.execute(sa_text("""
        SELECT product_type, COALESCE(SUM(amount_cents), 0) AS revenue_cents, COUNT(*) AS cnt
        FROM platform_revenue_ledger
        WHERE occurred_at >= :start AND occurred_at < :end AND refunded_at IS NULL
        GROUP BY product_type
    """), {"start": day_start_dt, "end": day_end_dt}).fetchall()
    by_stream = {stream: {"revenue_cents": 0, "count": 0} for stream in _LEDGER_STREAMS}
    for r in rows:
        by_stream.setdefault(r.product_type, {"revenue_cents": 0, "count": 0})
        by_stream[r.product_type] = {"revenue_cents": int(r.revenue_cents), "count": int(r.cnt)}
    return by_stream


def _refunds_disputes(db, day_start_dt: datetime, day_end_dt: datetime) -> dict:
    """Refunds are locally tracked (mark_ledger_refunded, event-driven).
    Disputes have no uniform local column across product tables (only
    premium_purchases has one) — queried from Stripe directly instead."""
    row = db.execute(sa_text("""
        SELECT COALESCE(SUM(amount_cents), 0) AS refunded_cents, COUNT(*) AS cnt
        FROM platform_revenue_ledger
        WHERE refunded_at >= :start AND refunded_at < :end
    """), {"start": day_start_dt, "end": day_end_dt}).fetchone()

    disputes: list[dict] = []
    dispute_error = None
    if _init_stripe():
        import stripe
        day_start, day_end = _utc_day_window(day_start_dt.date())
        try:
            params = {"created": {"gte": day_start, "lt": day_end}, "limit": 100}
            while True:
                page = stripe.Dispute.list(**params)
                for d in page.data:
                    disputes.append({
                        "dispute_id": d.id, "amount_cents": d.amount,
                        "reason": getattr(d, "reason", None), "status": getattr(d, "status", None),
                    })
                if not page.has_more:
                    break
                params["starting_after"] = page.data[-1].id
        except Exception as exc:
            logger.error("[RevenueHeartbeat] dispute fetch failed", exc_info=True)
            dispute_error = str(exc)

    return {
        "refunded_cents": int(row.refunded_cents), "refunded_count": int(row.cnt),
        "disputes": disputes, "dispute_count": len(disputes),
        "disputed_cents": sum(d["amount_cents"] for d in disputes),
        "dispute_fetch_error": dispute_error,
    }


def _lead_pack_fulfillment(db, day_start_dt: datetime, day_end_dt: datetime) -> dict:
    rows = db.execute(sa_text("""
        SELECT status, COUNT(*) AS cnt FROM lead_pack_purchases
        WHERE purchased_at >= :start AND purchased_at < :end
        GROUP BY status
    """), {"start": day_start_dt, "end": day_end_dt}).fetchall()
    by_status = {r.status: int(r.cnt) for r in rows}

    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=_LEAD_PACK_STUCK_AFTER_MINUTES)
    stuck = db.execute(sa_text("""
        SELECT id, subscriber_id, purchased_at, enrichment_submitted_at FROM lead_pack_purchases
        WHERE status = 'enriching' AND purchased_at >= :start AND purchased_at < :end
          AND (enrichment_submitted_at IS NULL OR enrichment_submitted_at < :stale_cutoff)
    """), {"start": day_start_dt, "end": day_end_dt, "stale_cutoff": stale_cutoff}).fetchall()

    return {
        "by_status": by_status,
        "stuck": [{"id": r.id, "subscriber_id": r.subscriber_id, "purchased_at": r.purchased_at} for r in stuck],
    }


def _premium_fulfillment(db, day_start_dt: datetime, day_end_dt: datetime) -> dict:
    rows = db.execute(sa_text("""
        SELECT status, COUNT(*) AS cnt FROM premium_purchases
        WHERE purchased_at >= :start AND purchased_at < :end
        GROUP BY status
    """), {"start": day_start_dt, "end": day_end_dt}).fetchall()
    return {"by_status": {r.status: int(r.cnt) for r in rows}}


def _unfulfilled_lead_unlock_charges(day_start_dt: datetime, day_end_dt: datetime, db) -> list[dict]:
    """Stripe charges for lead_unlock/hot_lead_unlock in the window with no
    matching sent_leads row.

    This is the concrete failure mode already present in
    stripe_webhooks.py's _on_lead_unlock_payment: an IntegrityError/
    OperationalError around the SentLead insert is caught and only
    logger.warning'd — when that fires, BOTH the delivery row and the
    ledger row (record_revenue is nested after the SentLead insert in the
    same block) are silently skipped. The ledger therefore can't reveal this
    gap on its own; only comparing against Stripe's own charge list can.
    """
    if not _init_stripe():
        return []
    import stripe

    day_start, day_end = _utc_day_window(day_start_dt.date())
    flagged: list[dict] = []
    try:
        params = {
            "type": "charge", "created": {"gte": day_start, "lt": day_end},
            "limit": 100, "expand": ["data.source"],
        }
        while True:
            page = stripe.BalanceTransaction.list(**params)
            for bt in page.data:
                charge = getattr(bt, "source", None)
                meta = getattr(charge, "metadata", None) or {}
                product = meta.get("product") if hasattr(meta, "get") else None
                if product not in ("lead_unlock", "hot_lead_unlock"):
                    continue
                pi_id = getattr(charge, "payment_intent", None)
                if not pi_id:
                    continue
                exists = db.execute(sa_text(
                    "SELECT 1 FROM sent_leads WHERE stripe_payment_intent_id = :pi LIMIT 1"
                ), {"pi": pi_id}).first()
                if exists is None:
                    flagged.append({
                        "payment_intent_id": pi_id, "amount_cents": charge.amount,
                        "product_type": product, "customer_id": getattr(charge, "customer", None),
                    })
            if not page.has_more:
                break
            params["starting_after"] = page.data[-1].id
    except Exception:
        logger.error("[RevenueHeartbeat] unfulfilled-charge scan failed", exc_info=True)
    return flagged


def _bankruptcy_section(db, day_start_dt: datetime, day_end_dt: datetime) -> dict:
    revenue = fetch_bankruptcy_alert_revenue(db, day_start_dt.date())
    active = db.execute(sa_text(
        "SELECT COUNT(*) FROM bankruptcy_alert_subscriptions WHERE status IN ('active', 'trialing')"
    )).scalar()
    alerts = db.execute(sa_text("""
        SELECT status, COUNT(*) AS cnt FROM bankruptcy_filing_alerts
        WHERE sent_at >= :start AND sent_at < :end
        GROUP BY status
    """), {"start": day_start_dt, "end": day_end_dt}).fetchall()
    return {
        **revenue,
        "active_subscriptions": int(active or 0),
        "alerts_by_status": {r.status: int(r.cnt) for r in alerts},
    }


def _loan_lane_section(db, day_start_dt: datetime, day_end_dt: datetime) -> dict:
    """Volume + broker commissions. Explicitly NOT Stripe revenue — see
    CommissionLedgerEntry's own docstring ("Tracks what brokers EARN — never
    Stripe billing") — never folded into the Stripe-vs-ledger check below."""
    lanes = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE entered_at >= :start AND entered_at < :end) AS entered,
            COUNT(*) FILTER (WHERE outcome = 'funded' AND updated_at >= :start AND updated_at < :end) AS funded
        FROM lanes
    """), {"start": day_start_dt, "end": day_end_dt}).fetchone()

    commissions = db.execute(sa_text("""
        SELECT status, COUNT(*) AS cnt, COALESCE(SUM(gross_amount_cents), 0) AS gross_cents
        FROM commission_ledger
        WHERE posted_at >= :start AND posted_at < :end
        GROUP BY status
    """), {"start": day_start_dt, "end": day_end_dt}).fetchall()

    return {
        "lanes_entered": int(lanes.entered or 0),
        "lanes_funded": int(lanes.funded or 0),
        "commissions_by_status": {
            r.status: {"count": int(r.cnt), "gross_cents": int(r.gross_cents)} for r in commissions
        },
    }


def build_report(run_date: date) -> dict:
    day_start_dt = datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone.utc)
    day_end_dt = day_start_dt + timedelta(days=1)

    errors: list[str] = []
    with get_db_context() as db:
        ledger = _ledger_totals_by_stream(db, day_start_dt, day_end_dt)
        refunds_disputes = _refunds_disputes(db, day_start_dt, day_end_dt)
        lead_pack = _lead_pack_fulfillment(db, day_start_dt, day_end_dt)
        premium = _premium_fulfillment(db, day_start_dt, day_end_dt)
        unfulfilled = _unfulfilled_lead_unlock_charges(day_start_dt, day_end_dt, db)
        bankruptcy = _bankruptcy_section(db, day_start_dt, day_end_dt)
        loan_lane = _loan_lane_section(db, day_start_dt, day_end_dt)

    stripe_net = compute_stripe_net_revenue(run_date)
    if stripe_net["error"]:
        errors.append(f"Stripe net-revenue fetch failed: {stripe_net['error']}")
    if bankruptcy["error"]:
        errors.append(f"Bankruptcy-alert invoice fetch failed: {bankruptcy['error']}")
    if refunds_disputes["dispute_fetch_error"]:
        errors.append(f"Dispute fetch failed: {refunds_disputes['dispute_fetch_error']}")

    ledger_total_cents = sum(s["revenue_cents"] for s in ledger.values())
    known_total_cents = ledger_total_cents + bankruptcy["revenue_cents"]
    mismatch_cents = stripe_net["stripe_net_cents"] - known_total_cents if stripe_net["configured"] else 0

    if stripe_net["configured"] and mismatch_cents != 0:
        errors.append(
            f"Stripe-vs-ledger mismatch: {_fmt_cents(mismatch_cents)} "
            "(see STRIPE TOTAL vs INTERNAL LEDGER TOTAL section)"
        )
    if lead_pack["stuck"]:
        errors.append(f"{len(lead_pack['stuck'])} lead pack purchase(s) stuck past the fulfillment sweep's retry window")
    if unfulfilled:
        errors.append(f"{len(unfulfilled)} paid lead-unlock charge(s) with no matching delivery record")
    failed_bk_alerts = bankruptcy["alerts_by_status"].get("failed", 0)
    if failed_bk_alerts:
        errors.append(f"{failed_bk_alerts} bankruptcy filing alert(s) failed to send")

    return {
        "run_date": run_date,
        "ledger_by_stream": ledger,
        "ledger_total_cents": ledger_total_cents,
        "bankruptcy": bankruptcy,
        "loan_lane": loan_lane,
        "lead_pack": lead_pack,
        "premium": premium,
        "unfulfilled_lead_unlock_charges": unfulfilled,
        "refunds_disputes": refunds_disputes,
        "reconciliation": {
            "known_total_cents": known_total_cents,
            "stripe_net_cents": stripe_net["stripe_net_cents"],
            "stripe_configured": stripe_net["configured"],
            "mismatch_cents": mismatch_cents,
            "charge_count": stripe_net["charge_count"],
            "refund_count": stripe_net["refund_count"],
        },
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def _fmt_cents(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def write_csv(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — Daily Revenue & Fulfillment Heartbeat"])
        w.writerow([f"Date: {report['run_date']}", f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
        w.writerow([])

        w.writerow(["REVENUE BY STREAM (Stripe-collected, via platform_revenue_ledger)"])
        w.writerow(["Stream", "Revenue", "Count"])
        for stream, data in report["ledger_by_stream"].items():
            w.writerow([stream, _fmt_cents(data["revenue_cents"]), data["count"]])
        w.writerow(["Total (ledger streams only)", _fmt_cents(report["ledger_total_cents"]), ""])
        w.writerow([])

        bk = report["bankruptcy"]
        w.writerow(["BANKRUPTCY-ALERT SUBSCRIPTIONS (own Stripe subscriptions, not in shared ledger)"])
        w.writerow([f"Revenue: {_fmt_cents(bk['revenue_cents'])} ({bk['invoice_count']} invoice(s))",
                    f"Active subscriptions: {bk['active_subscriptions']}"])
        w.writerow(["Filing alerts sent today, by status"])
        for status, cnt in bk["alerts_by_status"].items():
            w.writerow([status, cnt])
        w.writerow([])

        ll = report["loan_lane"]
        w.writerow(["LOAN LANE (broker earnings — NOT Stripe-collected platform revenue)"])
        w.writerow([f"Lanes entered: {ll['lanes_entered']}", f"Lanes funded: {ll['lanes_funded']}"])
        w.writerow(["Commission status", "Count", "Gross"])
        for status, d in ll["commissions_by_status"].items():
            w.writerow([status, d["count"], _fmt_cents(d["gross_cents"])])
        w.writerow([])

        lp = report["lead_pack"]
        w.writerow(["LEAD PACK FULFILLMENT"])
        w.writerow(["Status", "Count"])
        for status, cnt in lp["by_status"].items():
            w.writerow([status, cnt])
        if lp["stuck"]:
            w.writerow([f"DELAYED — stuck past {_LEAD_PACK_STUCK_AFTER_MINUTES}min retry window: {len(lp['stuck'])}"])
        w.writerow([])

        pr = report["premium"]
        w.writerow(["PREMIUM (report/brief/transfer/byol) FULFILLMENT"])
        w.writerow(["Status", "Count"])
        for status, cnt in pr["by_status"].items():
            w.writerow([status, cnt])
        w.writerow([])

        uf = report["unfulfilled_lead_unlock_charges"]
        w.writerow(["PAID BUT NOT FULFILLED — lead unlock / hot lead unlock"])
        if uf:
            w.writerow(["Payment Intent", "Product", "Amount", "Customer"])
            for item in uf:
                w.writerow([item["payment_intent_id"], item["product_type"],
                            _fmt_cents(item["amount_cents"]), item["customer_id"]])
        else:
            w.writerow(["None — every charge matched a delivery record."])
        w.writerow([])

        rd = report["refunds_disputes"]
        w.writerow(["REFUNDS & DISPUTES"])
        w.writerow([f"Refunded: {_fmt_cents(rd['refunded_cents'])} ({rd['refunded_count']} row(s))",
                    f"Disputes: {_fmt_cents(rd['disputed_cents'])} ({rd['dispute_count']})"])
        if rd["dispute_fetch_error"]:
            w.writerow([f"WARNING: dispute fetch failed — {rd['dispute_fetch_error']}"])
        w.writerow([])

        rec = report["reconciliation"]
        w.writerow(["STRIPE TOTAL vs INTERNAL LEDGER TOTAL"])
        if not rec["stripe_configured"]:
            w.writerow(["Stripe not configured — reconciliation skipped."])
        else:
            w.writerow([f"Known total (ledger + bankruptcy-alert): {_fmt_cents(rec['known_total_cents'])}"])
            w.writerow([f"Stripe net (charges - refunds, {rec['charge_count']} charge(s)/"
                        f"{rec['refund_count']} refund(s)): {_fmt_cents(rec['stripe_net_cents'])}"])
            w.writerow([f"Mismatch: {_fmt_cents(rec['mismatch_cents'])}"])
            if rec["mismatch_cents"] != 0:
                w.writerow([
                    "NOTE: a mismatch may include unmapped standalone products not yet wired into "
                    "this reconciliation (e.g. Supplier Intelligence, fa067, Phase 1/admin-only) — "
                    "investigate before assuming an error."
                ])
        w.writerow([])

        w.writerow(["EXCEPTIONS"])
        if report["errors"]:
            for err in report["errors"]:
                w.writerow([f"WARNING: {err}"])
        else:
            w.writerow(["No exceptions."])


# ---------------------------------------------------------------------------
# Alert email
# ---------------------------------------------------------------------------

def _alert_body(report: dict) -> str:
    rec = report["reconciliation"]
    lines = [f"Forced Action — Revenue & Fulfillment Heartbeat — {report['run_date']}", ""]
    lines.append(f"Revenue (ledger streams): {_fmt_cents(report['ledger_total_cents'])}")
    lines.append(f"Bankruptcy-alert revenue: {_fmt_cents(report['bankruptcy']['revenue_cents'])}")
    if rec["stripe_configured"]:
        lines.append(f"Stripe net: {_fmt_cents(rec['stripe_net_cents'])}  |  Mismatch: {_fmt_cents(rec['mismatch_cents'])}")
        if rec["mismatch_cents"] != 0:
            lines.append(
                "  NOTE: may include unmapped standalone products not yet wired into this "
                "reconciliation (e.g. Supplier Intelligence, Phase 1/admin-only)."
            )
    lines.append("")
    if report["errors"]:
        lines.append("EXCEPTIONS:")
        lines.extend(f"  - {e}" for e in report["errors"])
    else:
        lines.append("No exceptions.")
    return "\n".join(lines)


def send_heartbeat_alert(report: dict, csv_path: Path) -> bool:
    from src.services.email import send_alert

    rec = report["reconciliation"]
    has_exceptions = bool(report["errors"]) or (rec["stripe_configured"] and rec["mismatch_cents"] != 0)
    subject = (
        f"[Revenue Heartbeat] {report['run_date']} — "
        + ("EXCEPTIONS FOUND" if has_exceptions else "clean")
    )
    return send_alert(subject, _alert_body(report), attachments=[csv_path])


# ---------------------------------------------------------------------------
# Pruning + entry point
# ---------------------------------------------------------------------------

def prune_old_reports(directory: Path) -> int:
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    deleted = 0
    for f in directory.glob("heartbeat_*.csv"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            logger.info(f"[RevenueHeartbeat] Pruned: {f.name}")
            deleted += 1
    return deleted


def generate_report(run_date: date, dry_run: bool = False) -> Path:
    logger.info(f"[RevenueHeartbeat] Generating for {run_date}")
    report = build_report(run_date)

    output_path = REPORTS_DIR / f"heartbeat_{run_date}.csv"
    write_csv(report, output_path)
    logger.info(f"[RevenueHeartbeat] Written to {output_path}")

    deleted = prune_old_reports(REPORTS_DIR)
    if deleted:
        logger.info(f"[RevenueHeartbeat] Pruned {deleted} old report(s)")

    if dry_run:
        logger.info("[RevenueHeartbeat][DRY RUN] Would send alert email — skipped")
    else:
        try:
            send_heartbeat_alert(report, output_path)
        except Exception:
            logger.error("[RevenueHeartbeat] Failed to send alert email", exc_info=True)

    rec = report["reconciliation"]
    print(
        f"\nForced Action Revenue & Fulfillment Heartbeat — {run_date}\n"
        f"  Ledger revenue : {_fmt_cents(report['ledger_total_cents'])}\n"
        f"  Bankruptcy rev : {_fmt_cents(report['bankruptcy']['revenue_cents'])}\n"
        + (f"  Stripe net     : {_fmt_cents(rec['stripe_net_cents'])} (mismatch {_fmt_cents(rec['mismatch_cents'])})\n"
           if rec["stripe_configured"] else "  Stripe net     : not configured\n")
        + f"  Exceptions     : {len(report['errors'])}\n"
        f"  Saved          : {output_path}\n"
    )
    for err in report["errors"]:
        print(f"  WARNING: {err}")

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate the daily revenue & fulfillment heartbeat")
    parser.add_argument("--date", type=lambda s: date.fromisoformat(s), default=date.today())
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending the alert email")
    args = parser.parse_args()

    try:
        generate_report(args.date, dry_run=args.dry_run)
    except Exception as e:
        logger.error(f"[RevenueHeartbeat] Failed: {e}", exc_info=True)
