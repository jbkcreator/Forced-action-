"""
Slack output for Vera's daily standing jobs.

Posts to the channel configured by VERA_SLACK_CHANNEL (client-supplied:
#vera-verification, ID C0BMLTUTQTA) using the same WebClient pattern as
lifecycle_slack.py.

Callers are responsible for emailing REPORT_RECIPIENTS before calling this.
No email fallback here — the report email is always sent by the caller
regardless of Slack availability.

Usage:
    from src.services.vera_slack import post_vera_report
    post_vera_report(subject="Vera — Live State", body="...", blocks=[...])
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Mapping, Optional

from config.settings import get_settings
from src.agents.vera.checks.live_state import format_outcome_label

logger = logging.getLogger(__name__)

# ── Block Kit helpers ─────────────────────────────────────────────────────────

def _header(text: str) -> dict:
    return {"type": "header", "text": {"type": "plain_text", "text": text[:150], "emoji": True}}

def _divider() -> dict:
    return {"type": "divider"}

def _section(text: str) -> dict:
    # Slack caps mrkdwn section text at 3000 chars
    text = text[:2950] + "…" if len(text) > 2950 else text
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}

def _capped_lines(lines: list, budget: int) -> str:
    """Join lines, dropping trailing ones (with a count note) rather than
    letting _section's char cap slice an item in half with no indication."""
    out = []
    used = 0
    for i, line in enumerate(lines):
        used += len(line) + 1
        if used > budget:
            out.append(f"_...{len(lines) - i} more (see email for full list)_")
            break
        out.append(line)
    return "\n".join(out)


# ── Per-report Block Kit builders ─────────────────────────────────────────────

def build_live_state_blocks(subject: str, deploy: dict, cron_beats: list, silent: dict,
                             one_number_line: str, report_date: str,
                             crashed: Optional[list] = None) -> list:
    """Block Kit layout for the Live-State report. `crashed` (from
    live_state.check_crashed_before_completion()) defaults to None -> treated
    as empty, so existing callers/tests predating it are unaffected."""
    crashed = crashed or []
    stale = [b for b in cron_beats if b.is_stale]
    fresh_count = len(cron_beats) - len(stale)
    drift = deploy.get("drift", "unknown")
    drift_icon = "✅" if drift == "in_sync" else ("⚠️" if drift == "unknown" else "🚨")
    pending = deploy.get("pending_migrations", [])

    # One number
    one_number_value = one_number_line.split(": ", 1)[-1] if ": " in one_number_line else one_number_line

    blocks = [
        _header(f"Vera — Live-State Report {report_date}"),
        _section(f"*💰 New MRR added yesterday:* `{one_number_value}`"),
        _divider(),
        _section(
            f"*🚀 DEPLOY*\n"
            f"{drift_icon} Drift: `{drift}`\n"
            f"Prod HEAD: `{deploy.get('head_sha', 'unknown')[:12] if deploy.get('head_sha') else 'unknown'}`  "
            f"Dev HEAD: `{deploy.get('dev_head_sha', 'unknown')[:12] if deploy.get('dev_head_sha') else 'unknown'}`\n"
            + (f"⚠️ Pending migrations: {len(pending)}\n```{chr(10).join(pending[:5])}{'...' if len(pending) > 5 else ''}```"
               if pending else "Pending migrations: none")
        ),
        _divider(),
    ]

    # Cron freshness — "last recorded outcome" says *why*, not just *that*,
    # a source is stale (see CronBeat.last_attempt_label / format_outcome_label).
    cron_text = f"*📡 CRON FRESHNESS*  {fresh_count}/{len(cron_beats)} sources fresh\n"
    if stale:
        for b in sorted(stale, key=lambda x: x.label()):
            last_attempt = b.last_attempt_label() or "none — genuinely never run"
            cron_text += (
                f"❌ `{b.label()}` — age {b.age_label()} (SLA {b.sla_minutes // 60}h)\n"
                f"     last recorded outcome: {last_attempt}\n"
            )
    else:
        cron_text += "✅ All sources fresh"
    blocks.append(_section(cron_text))
    blocks.append(_divider())

    # Silent failures
    def _silent_line(r: Mapping) -> str:
        label = format_outcome_label(r.get("outcome_category"), r.get("error_type"))
        msg = r.get("error_message")
        base = f"• `{r['source_type']}/{r['county_id']}`  {label}"
        return f"{base} — {msg[:160]}" if msg else base

    confirmed_no_data = silent.get("zero_ingest_confirmed_no_data", [])
    unexplained = silent.get("zero_ingest_unexplained", [])
    unscheduled = silent.get("unscheduled", [])
    sf_text = f"*🔇 ZERO-INGEST SOURCES*\n"
    if confirmed_no_data:
        sf_text += f"ℹ️ No new data today (confirmed — nothing to report) ({len(confirmed_no_data)}):\n"
        sf_text += _capped_lines([_silent_line(r) for r in confirmed_no_data], 800)
        sf_text += "\n"
    else:
        sf_text += "✅ No new data today (confirmed): none\n"
    if unexplained:
        sf_text += f"🔧 Scheduled-but-writing-nothing — needs investigation ({len(unexplained)}):\n"
        sf_text += _capped_lines([_silent_line(r) for r in unexplained], 800)
    else:
        sf_text += "✅ Scheduled-but-writing-nothing: none"
    if unscheduled:
        sf_text += f"\nEnabled-but-unscheduled ({len(unscheduled)}):\n"
        sf_text += "\n".join(f"• `{s}`" for s in unscheduled)
    else:
        sf_text += "\n✅ Enabled-but-unscheduled: none"
    blocks.append(_section(sf_text))
    blocks.append(_divider())

    # Crashed mid-run — only meaningful for scraper_run()-wrapped sources.
    # No category shown — a crashed row has no completion write by
    # definition, so there's nothing classified; how long it's been stuck
    # is the useful signal instead.
    def _crashed_line(r: Mapping) -> str:
        started = r.get("attempt_started_at")
        if started is None:
            return f"• `{r['source_type']}/{r['county_id']}`"
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        hours = (datetime.now(timezone.utc) - started).total_seconds() / 3600
        duration = f"{hours:.1f}h" if hours < 48 else f"{hours / 24:.1f}d"
        return f"• `{r['source_type']}/{r['county_id']}` — started {duration} ago, still no completion"

    cm_text = "*💥 CRASHED MID-RUN*\n"
    if crashed:
        cm_text += f"Started but never completed ({len(crashed)}):\n"
        cm_text += _capped_lines([_crashed_line(r) for r in crashed], 800)
    else:
        cm_text += "✅ Started but never completed: none"
    blocks.append(_section(cm_text))
    blocks.append(_context("— Vera."))
    return blocks


def build_revenue_truth_blocks(subject: str, reconciliation, mrr, new_yesterday_cents,
                                payments, refunds_disputes, report_date: str) -> list:
    """Block Kit layout for the Revenue Truth report."""
    if new_yesterday_cents is not None:
        one_number = f"${new_yesterday_cents / 100:,.2f}"
    else:
        one_number = "no prior day to compare (first run)"

    blocks = [
        _header(f"Vera — Revenue Truth Report {report_date}"),
        _section(f"*💰 New MRR added yesterday:* `{one_number}`"),
        _divider(),
    ]

    # Reconciliation
    if not reconciliation.stripe_ok:
        blocks.append(_section("*🔁 RECONCILIATION*\n🚨 Stripe unreachable — could not verify today."))
    else:
        pna = reconciliation.paying_no_access_count
        anp = reconciliation.access_not_paying_count
        recon_text = (
            f"*🔁 RECONCILIATION*\n"
            f"{'✅' if pna == 0 else '🚨'} Paying but no access: `{pna}`\n"
            f"{'✅' if anp == 0 else '⚠️'} Access but not paying: `{anp}`"
        )
        if pna > 0:
            pna_ids = reconciliation.paying_no_access_sample_ids
            pna_lines = [f"• `{cid}`" for cid in pna_ids]
            if pna > len(pna_ids):
                pna_lines.append(f"_(sample capped at {len(pna_ids)} of {pna} total — see email for full list)_")
            recon_text += "\n" + _capped_lines(pna_lines, budget=1200)
        if anp > 0:
            anp_details = reconciliation.access_not_paying_details
            anp_lines = [
                f"• `{d['customer_id']}` — {d['reason']} ({d.get('stripe_status', '?')})"
                for d in anp_details
            ]
            if anp > len(anp_details):
                anp_lines.append(f"_(sample capped at {len(anp_details)} of {anp} total — see email for full list)_")
            recon_text += "\n" + _capped_lines(anp_lines, budget=1200)
        blocks.append(_section(recon_text))
    blocks.append(_divider())

    # MRR
    mrr_text = f"*📊 MRR*\nDB: `${mrr.db_total_cents / 100:,.2f}`\n"
    if not mrr.stripe_ok:
        mrr_text += "🚨 Stripe unreachable — drift unknown"
    else:
        drift_icon = "✅" if mrr.drift_cents == 0 else "⚠️"
        mrr_text += (
            f"Stripe: `${mrr.stripe_total_cents / 100:,.2f}`\n"
            f"{drift_icon} Drift: `${mrr.drift_cents / 100:,.2f}`"
        )
    if mrr.active_null_plan_price_count:
        mrr_text += f"\n_⚠️ {mrr.active_null_plan_price_count} active subscriber(s) have NULL plan_price_"
    blocks.append(_section(mrr_text))
    blocks.append(_divider())

    # Payments
    if not payments.stripe_ok:
        blocks.append(_section("*💳 PAYMENTS TODAY*\n🚨 Stripe unreachable"))
    else:
        blocks.append(_section(
            f"*💳 PAYMENTS TODAY*\n"
            f"New: `{payments.new_count}` (`${payments.new_amount_cents / 100:,.2f}`) "
            f"— subscription `{payments.subscription_count}`, one-time `{payments.one_time_count}`\n"
            f"Failed: `{payments.failed_count}` (`${payments.failed_amount_cents / 100:,.2f}`)"
        ))
    blocks.append(_divider())

    # Refunds & disputes
    if not refunds_disputes.stripe_ok:
        blocks.append(_section("*↩️ REFUNDS & DISPUTES*\n🚨 Stripe unreachable"))
    else:
        rd_icon = "✅" if refunds_disputes.disputes_count == 0 else "🚨"
        blocks.append(_section(
            f"*↩️ REFUNDS & DISPUTES*\n"
            f"Refunds: `{refunds_disputes.refunds_count}` (`${refunds_disputes.refunds_amount_cents / 100:,.2f}`)\n"
            f"{rd_icon} Disputes: `{refunds_disputes.disputes_count}` (`${refunds_disputes.disputes_amount_cents / 100:,.2f}`)"
        ))

    blocks.append(_context("— Vera."))
    return blocks


def build_reconciliation_blocks(report_date: str, drift_cents: int, stripe_ok: bool) -> list:
    """Slim daily reconciliation companion post."""
    icon = "✅" if drift_cents == 0 and stripe_ok else ("🚨" if not stripe_ok else "⚠️")
    return [
        _section(
            f"{icon} *[Vera] Daily Reconciliation — {report_date}*\n"
            f"Stripe vs. subscribers drift: `${drift_cents / 100:,.2f}` "
            f"({'stripe reachable' if stripe_ok else 'stripe unreachable'})"
        ),
    ]


def build_digest_blocks(subject: str, discrepancies: list, digest, report_date: str,
                         unchecked: Optional[list] = None) -> list:
    """Block Kit layout for the Promise & Discrepancy Digest."""
    blocks = [
        _header(f"Vera — Promise & Discrepancy Digest {report_date}"),
        _section(
            f"*Open discrepancies:* `{len(discrepancies)}`   "
            f"*Open promises:* `{digest.open_count}` ({len(digest.overdue)} overdue)"
        ),
    ]
    if unchecked:
        blocks.append(_section(
            f"⚠️ *NOT CHECKED TODAY* — no fresh fact for: {', '.join(unchecked)}"
        ))
    blocks.append(_divider())

    # Discrepancies
    disc_text = "*🔍 DISCREPANCIES* (Doc claims X; live shows Y)\n"
    if discrepancies:
        for d in discrepancies:
            disc_text += f"• *Claim:* {d.claim}\n  *Live:* {d.live}\n"
    else:
        disc_text += "✅ none — every checked claim matches live state."
    blocks.append(_section(disc_text))
    blocks.append(_divider())

    # Overdue promises
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    overdue_text = "*🔴 OVERDUE PROMISES*\n"
    if digest.overdue:
        for p in digest.overdue:
            mrr = f"${p.mrr_at_risk_cents / 100:,.2f}" if p.mrr_at_risk_cents else "—"
            overdue_text += f"• [{p.owner}] {p.description} — {p.age_days(now)}d old, MRR-at-risk {mrr}\n"
    else:
        overdue_text += "✅ none"
    blocks.append(_section(overdue_text))

    # Pending promises
    if digest.pending:
        pending_text = "*🟡 OPEN PROMISES (not yet due)*\n"
        for p in digest.pending:
            mrr = f"${p.mrr_at_risk_cents / 100:,.2f}" if p.mrr_at_risk_cents else "—"
            pending_text += f"• [{p.owner}] {p.description} — due {p.due_at.date().isoformat() if p.due_at else '—'}, MRR {mrr}\n"
        blocks.append(_section(pending_text))

    blocks.append(_context("— Vera."))
    return blocks


def _default_blocks(subject: str, body: str) -> list:
    """Fallback block-kit layout used when no richer blocks are supplied."""
    body_truncated = body[:2900] + "\n…(truncated)" if len(body) > 2900 else body
    return [
        _header(subject),
        _section(body_truncated),
        _context("— Vera."),
    ]


def post_vera_report(
    subject: str,
    body: str,
    blocks: Optional[list] = None,
) -> Optional[str]:
    """Post a Vera report to #vera-verification.

    Returns the Slack message timestamp on success, or None when Slack is
    unconfigured or the post fails. Never raises. No email fallback — callers
    send to REPORT_RECIPIENTS before calling this.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.vera_slack_channel

    if blocks is None:
        blocks = _default_blocks(subject, body)

    if not token or not channel:
        logger.debug("[vera_slack] Slack not configured (no token/channel) — skipping")
        return None

    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError:
        logger.warning("[vera_slack] slack_sdk not installed")
        return None

    try:
        client = WebClient(token=token.get_secret_value())
        resp = client.chat_postMessage(
            channel=channel,
            text=subject,  # fallback plain text for notifications
            blocks=blocks,
        )
        return resp.get("ts")
    except SlackApiError as exc:
        logger.warning(
            "[vera_slack] Slack post failed: %s",
            exc.response.get("error") if exc.response else exc,
        )
    except Exception:
        logger.warning("[vera_slack] Slack post raised", exc_info=True)

    return None
