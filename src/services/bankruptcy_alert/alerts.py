"""
Stage 12 — Bankruptcy filing alert delivery.

Matches newly-ingested filings to eligible subscriptions, deduplicates against
`bankruptcy_filing_alerts` (UNIQUE subscription_id+filing_id+channel), sends via
email and/or SMS, and logs every attempt.

Matching rule (per subscription, per filing):
  - subscription.status in ALERT_ELIGIBLE_STATUSES
  - filing.jurisdiction in subscription.jurisdictions (NULL = all)
  - filing.chapter in subscription.chapters (NULL = all; NULL filing chapter always passes)

Dedup is enforced at the DB level: we INSERT the alert row with
ON CONFLICT DO NOTHING *before* sending; if the insert returns no id, the
subscriber was already alerted for this (filing, channel) and we skip. This
makes the whole dispatch idempotent and crash-safe.

All DB I/O via sa_text.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.bankruptcy_alert_config import (
    ALERT_ELIGIBLE_STATUSES,
    MAX_FILINGS_PER_DIGEST,
)

logger = logging.getLogger(__name__)


@dataclass
class DispatchResult:
    subscriptions_considered: int = 0
    filings_considered: int = 0
    emails_sent: int = 0
    sms_sent: int = 0
    failed: int = 0
    deduped: int = 0
    errors: list = field(default_factory=list)

    @property
    def total_attempts(self) -> int:
        return self.emails_sent + self.sms_sent + self.failed

    @property
    def failure_rate(self) -> float:
        return self.failed / self.total_attempts if self.total_attempts else 0.0


# ── Matching query ──────────────────────────────────────────────────────────────

def _eligible_subscriptions(db: Session) -> list:
    statuses = list(ALERT_ELIGIBLE_STATUSES)
    return db.execute(sa_text("""
        SELECT id, email, phone, jurisdictions, chapters, channel_email, channel_sms
        FROM bankruptcy_alert_subscriptions
        WHERE status = ANY(:statuses)
    """), {"statuses": statuses}).fetchall()


def _unalerted_filings_for(db: Session, sub, *, limit: int) -> list:
    """Return filings matching this subscription's filters that have NOT yet been
    alerted on ANY channel. JSONB NULL means 'all'.

    Uses NOT EXISTS against bankruptcy_filing_alerts so already-sent filings are
    excluded even before the per-channel dedup insert.
    """
    return db.execute(sa_text("""
        SELECT f.id, f.case_number, f.chapter, f.court, f.jurisdiction,
               f.filer, f.trustee, f.date_filed
        FROM bankruptcy_filings f
        WHERE
            (:jurisdictions IS NULL
             OR f.jurisdiction = ANY(SELECT jsonb_array_elements_text(:jurisdictions)))
          AND
            (:chapters IS NULL
             OR f.chapter IS NULL
             OR f.chapter = ANY(SELECT jsonb_array_elements_text(:chapters)))
          AND NOT EXISTS (
            SELECT 1 FROM bankruptcy_filing_alerts a
            WHERE a.subscription_id = :sub_id AND a.filing_id = f.id
          )
        ORDER BY f.date_filed DESC NULLS LAST, f.id DESC
        LIMIT :limit
    """), {
        "jurisdictions": json.dumps(sub.jurisdictions) if sub.jurisdictions is not None else None,
        "chapters": json.dumps(sub.chapters) if sub.chapters is not None else None,
        "sub_id": sub.id,
        "limit": limit,
    }).fetchall()


# ── Dedup insert (claim before send) ────────────────────────────────────────────

def _claim_alert(db: Session, sub_id: int, filing_id: int, channel: str) -> bool:
    """Insert a placeholder alert row. Returns True if WE claimed it (safe to send),
    False if a row already existed (already alerted → skip).
    Status starts 'sent'; flipped to 'failed' if delivery throws."""
    row = db.execute(sa_text("""
        INSERT INTO bankruptcy_filing_alerts
            (subscription_id, filing_id, channel, status, sent_at)
        VALUES (:sub_id, :filing_id, :channel, 'sent', NOW())
        ON CONFLICT (subscription_id, filing_id, channel) DO NOTHING
        RETURNING id
    """), {"sub_id": sub_id, "filing_id": filing_id, "channel": channel}).first()
    return row is not None


def _mark_failed(db: Session, sub_id: int, filing_id: int, channel: str, error: str) -> None:
    db.execute(sa_text("""
        UPDATE bankruptcy_filing_alerts
        SET status = 'failed', error = :error
        WHERE subscription_id = :sub_id AND filing_id = :filing_id AND channel = :channel
    """), {"sub_id": sub_id, "filing_id": filing_id, "channel": channel, "error": error[:500]})


# ── Message formatting ──────────────────────────────────────────────────────────

def _format_email(filings: list) -> tuple[str, str, str]:
    """Return (subject, text_body, html_body) for a digest of filings."""
    n = len(filings)
    subject = f"{n} new bankruptcy filing{'s' if n != 1 else ''} matching your alert"
    lines = []
    html_rows = []
    for f in filings:
        ch = f"Ch. {f.chapter}" if f.chapter else "Ch. —"
        date_str = f.date_filed.isoformat() if f.date_filed else "—"
        lines.append(f"• {f.filer or '(name unavailable)'} — {ch} — {f.case_number} — filed {date_str}")
        html_rows.append(
            f"<tr><td style='padding:6px 10px;'>{f.filer or '(name unavailable)'}</td>"
            f"<td style='padding:6px 10px;'>{ch}</td>"
            f"<td style='padding:6px 10px;'>{f.case_number}</td>"
            f"<td style='padding:6px 10px;'>{date_str}</td></tr>"
        )
    text_body = (
        f"{n} new bankruptcy filing(s) matched your alert:\n\n"
        + "\n".join(lines)
        + "\n\n— Forced Action Bankruptcy Alerts"
    )
    html_body = (
        f"<h2>{n} new bankruptcy filing(s)</h2>"
        "<table style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;'>"
        "<tr style='background:#f1f5f9;text-align:left;'>"
        "<th style='padding:6px 10px;'>Filer</th><th style='padding:6px 10px;'>Chapter</th>"
        "<th style='padding:6px 10px;'>Case #</th><th style='padding:6px 10px;'>Filed</th></tr>"
        + "".join(html_rows)
        + "</table><p style='color:#888;font-size:11px;'>Forced Action Bankruptcy Alerts</p>"
    )
    return subject, text_body, html_body


def _format_sms(filings: list) -> str:
    n = len(filings)
    if n == 1:
        f = filings[0]
        ch = f"Ch.{f.chapter}" if f.chapter else "Ch.?"
        return f"New bankruptcy filing: {f.filer or 'name N/A'} ({ch}, {f.case_number}). — Forced Action"
    return (
        f"{n} new bankruptcy filings match your alert. "
        f"Check your email for details. — Forced Action"
    )


# ── Dispatch ────────────────────────────────────────────────────────────────────

def dispatch_alerts(db: Session, *, max_per_digest: int = MAX_FILINGS_PER_DIGEST) -> DispatchResult:
    """Match unalerted filings to eligible subscriptions and send.

    Idempotent: re-running sends nothing new (dedup claim rows already exist).
    """
    from src.services.email import send_email
    from src.services.sms_compliance import send_sms

    result = DispatchResult()
    subs = _eligible_subscriptions(db)
    result.subscriptions_considered = len(subs)

    for sub in subs:
        filings = _unalerted_filings_for(db, sub, limit=max_per_digest)
        if not filings:
            continue
        result.filings_considered += len(filings)

        # Claim every (filing, channel) pair up-front so a crash mid-send doesn't
        # double-alert on retry. Channels are per-subscription toggles.
        channels = []
        if sub.channel_email and sub.email:
            channels.append("email")
        if sub.channel_sms and sub.phone:
            channels.append("sms")
        if not channels:
            continue

        for channel in channels:
            claimed = [f for f in filings if _claim_alert(db, sub.id, f.id, channel)]
            already = len(filings) - len(claimed)
            result.deduped += already
            if not claimed:
                continue
            db.flush()

            try:
                if channel == "email":
                    subject, text_body, html_body = _format_email(claimed)
                    ok = send_email(sub.email, subject, text_body, body_html=html_body)
                    if ok:
                        result.emails_sent += len(claimed)
                    else:
                        raise RuntimeError("send_email returned False (SMTP not configured or failed)")
                else:  # sms
                    body = _format_sms(claimed)
                    ok = send_sms(
                        sub.phone, body, db,
                        message_type="transactional",  # paid alert product, not marketing
                        task_type="bankruptcy_alert",
                        campaign="bankruptcy_alerts",
                    )
                    if ok:
                        result.sms_sent += len(claimed)
                    else:
                        raise RuntimeError("send_sms returned False (suppressed/failed)")
            except Exception as exc:  # noqa: BLE001
                # Mark every claimed row for this channel as failed for audit + retry.
                for f in claimed:
                    _mark_failed(db, sub.id, f.id, channel, str(exc))
                result.failed += len(claimed)
                result.errors.append(f"sub={sub.id} channel={channel}: {exc}")
                logger.warning("[bk-alert] delivery failed sub=%s channel=%s: %s", sub.id, channel, exc)

            db.flush()

    logger.info(
        "[bk-alert] dispatch: subs=%d filings=%d email=%d sms=%d failed=%d deduped=%d",
        result.subscriptions_considered, result.filings_considered,
        result.emails_sent, result.sms_sent, result.failed, result.deduped,
    )
    return result


# ── Status / reporting ───────────────────────────────────────────────────────────

def recent_alerts(db: Session, *, limit: int = 50) -> list[dict]:
    rows = db.execute(sa_text("""
        SELECT a.id, a.subscription_id, a.filing_id, a.channel, a.status,
               a.sent_at, f.case_number, f.filer, f.chapter, s.email
        FROM bankruptcy_filing_alerts a
        JOIN bankruptcy_filings f ON f.id = a.filing_id
        JOIN bankruptcy_alert_subscriptions s ON s.id = a.subscription_id
        ORDER BY a.sent_at DESC
        LIMIT :limit
    """), {"limit": limit}).mappings().fetchall()
    return [dict(r) for r in rows]


def _projected_mrr_cents(counts_by_status: dict) -> int:
    """Projected monthly recurring revenue from active + trialing subscribers.

    Flat-rate product (no per-subscriber discounts today), so this is a
    straight count × PRICE_MONTHLY_CENTS — not a ledger sum of actually
    collected payments (past_due/canceled contribute nothing).
    """
    from config.bankruptcy_alert_config import PRICE_MONTHLY_CENTS

    billable = counts_by_status.get("active", 0) + counts_by_status.get("trialing", 0)
    return billable * PRICE_MONTHLY_CENTS


def _paid_mrr_cents(counts_by_status: dict) -> int:
    """One month's recurring revenue from subscribers who have actually paid.

    'active' status is only ever set by a successful Stripe
    invoice.payment_succeeded webhook (see subscription.py:_on_payment_succeeded),
    so these are Stripe-verified paying subscribers, not a guess. 'trialing'
    is excluded: no card has been charged yet.

    This is a monthly run-rate (active_count × one month's price), NOT
    lifetime revenue collected — a subscriber active for six months counts
    once here, at one month's price. For true collected-to-date revenue,
    Stripe/ledger data would be needed (out of scope: this product doesn't
    write to platform_revenue_ledger, see the plan's decoupling note).
    """
    from config.bankruptcy_alert_config import PRICE_MONTHLY_CENTS

    return counts_by_status.get("active", 0) * PRICE_MONTHLY_CENTS


def _invite_conversion_stats(db: Session, window_days: int = 7) -> dict:
    """Invite → paid-conversion rate.

    Counts DISTINCT invited subscribers, not invite rows — a resend to the
    same person must not inflate either side (one person, one paid signup =
    one conversion, however many invites they got).

    Denominator: distinct subscribers actually sent a bankruptcy_alert_invite.
    Numerator: those whose email later appears as a PAID (status='active')
    bankruptcy_alert_subscriptions row within window_days of any of their
    sends — mirrors INVITE_GIVE_UP_HOURS' short attribution window rather
    than crediting an invite for an unrelated signup months later.

    Only 'active' (Stripe-charged) counts — a trialing/canceled signup in the
    window is not a paid conversion, matching _paid_mrr_cents' definition of
    real revenue. Attribution is correlational (email match + time proximity),
    so treat this as a floor estimate, not proven causation.
    """
    from config.bankruptcy_alert_config import INVITE_TEMPLATE_ID

    row = db.execute(sa_text("""
        SELECT
            COUNT(DISTINCT m.subscriber_id) AS invites_sent,
            COUNT(DISTINCT m.subscriber_id) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM bankruptcy_alert_subscriptions b
                    WHERE LOWER(b.email) = LOWER(s.email)
                      AND b.status = 'active'
                      AND b.created_at >= m.sent_at
                      AND b.created_at < m.sent_at + make_interval(days => :window_days)
                )
            ) AS converted
        FROM message_outcomes m
        JOIN subscribers s ON s.id = m.subscriber_id
        WHERE m.template_id = :tpl AND m.send_status = 'sent'
    """), {"tpl": INVITE_TEMPLATE_ID, "window_days": window_days}).first()

    invites_sent = int(row.invites_sent or 0) if row else 0
    converted = int(row.converted or 0) if row else 0

    return {
        "invites_sent": invites_sent,
        "converted": converted,
        "conversion_rate_pct": round(converted / invites_sent * 100, 1) if invites_sent else None,
        "window_days": window_days,
    }


def status_summary(db: Session) -> dict:
    """Counts for the /alerts/status endpoint."""
    sub_counts = db.execute(sa_text("""
        SELECT status, COUNT(*) AS c
        FROM bankruptcy_alert_subscriptions
        GROUP BY status
    """)).fetchall()
    alert_counts = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'sent')      AS sent,
            COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
            COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') AS last_24h
        FROM bankruptcy_filing_alerts
    """)).first()
    filings_total = db.execute(sa_text("SELECT COUNT(*) AS c FROM bankruptcy_filings")).first()

    subscribers_by_status = {r.status: int(r.c) for r in sub_counts}

    return {
        "subscribers_by_status": subscribers_by_status,
        "subscribers_total": sum(subscribers_by_status.values()),
        "alerts_sent": int(alert_counts.sent or 0) if alert_counts else 0,
        "alerts_failed": int(alert_counts.failed or 0) if alert_counts else 0,
        "alerts_last_24h": int(alert_counts.last_24h or 0) if alert_counts else 0,
        "filings_total": int(filings_total.c or 0) if filings_total else 0,
        "projected_mrr_cents": _projected_mrr_cents(subscribers_by_status),
        "paid_mrr_cents": _paid_mrr_cents(subscribers_by_status),
        "invite_conversion": _invite_conversion_stats(db),
    }
