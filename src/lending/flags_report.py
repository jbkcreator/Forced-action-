"""Production on/off table of every lending feature flag (P0.5).

``python -m src.lending.flags_report`` prints a Markdown table of each flag's live value (read through
``config.settings``, the same values the services see), its code default and a one-line description.
``--post`` also posts it to Slack (Cora's channel unless ``--channel`` is given). Read-only: it never changes a flag.

Every ``LENDING_*_ENABLED`` / ``*_APPROVED`` setting must be listed in ``FLAGS``; a test fails otherwise, so a new
flag cannot be missing from the table.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from config.settings import AppSettings, get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Flag:
    env: str
    description: str


FLAGS: tuple[Flag, ...] = (
    # Lead intake and alerts
    Flag("LENDING_LENDINGFLOW_ENABLED", "LendingFlow lead intake webhook (T-11); closed (503) while off"),
    Flag("LENDING_LEAD_ARRIVAL_ALERT_ENABLED", "@-mention Josh in #lead-alerts when a LendingFlow lead arrives (T-05)"),
    Flag("LENDING_UNCALLED_ALARMS_ENABLED", "2-minute / 5-minute uncalled-lead alarms (T-10)"),
    Flag("LENDING_AFTER_HOURS_INTAKE_ENABLED", "After-hours voicemail / answering-service intake webhook; closed (503) while off"),
    Flag("LENDING_ENRICHMENT_ENABLED", "Background enrichment card + routing for LendingFlow leads (T-12)"),
    Flag("LENDING_TRANSCRIPT_INGEST_ENABLED", "Call transcript extraction (each extraction is a Claude call)"),
    # Texts and emails to borrowers
    Flag("LENDING_SMS_CARRIER_APPROVED", "10DLC carrier approval: until true, texts Cora approves wait (then block at expiry)"),
    Flag("LENDING_INTRO_SMS_ENABLED", "LendingFlow intro SMS: adds the tag that starts the GHL intro workflow (T-09)"),
    Flag("MISSED_CALL_TEXT_ENABLED", "Missed-call text-back through GHL (WP-GL-9)"),
    Flag("LENDING_TEXT_BACK_BOOKING_LINK_ENABLED", "Josh's booking link in the missed-call text"),
    Flag("BOOKING_REMINDER_TEXT_ENABLED", "Booking confirmation / night-before / 90-min / 10-min texts (WP-GL-10)"),
    Flag("BOOKING_REMINDER_EMAIL_ENABLED", "Email fallback for booking reminders (no text consent)"),
    # Documents
    Flag("LENDING_SOFT_APPROVAL_ENABLED", "Call-One soft approval card + PDF in #dial-tasks (T-08)"),
    Flag("LENDING_PREQUAL_PDF_ENABLED", "Minute-5 pre-qualification PDF letter (T-07); also partner pre-qual letters"),
    # Dialer and GHL sync
    Flag("LENDING_LENDINGFLOW_DIALER_ENABLED", "Load LendingFlow leads into the BatchDialer 'LendingFlow' campaign"),
    Flag("LENDING_WEB_LEAD_DIALER_ENABLED", "Load website leads into the BatchDialer 'Website' campaign"),
    Flag("LENDING_GHL_LOAD_SYNC_ENABLED", "Push every active BatchDialer load to GHL as a cold contact"),
    Flag("LENDING_BD_RESULT_TO_GHL_ENABLED", "BatchDialer call results as GHL tags + 'last BatchDialer call' field"),
    Flag("LENDING_GHL_RESULT_TO_BD_ENABLED", "GHL call results on the BatchDialer contact card"),
    Flag("LENDING_GHL_CALL_IMPORT_ENABLED", "Record calls made in GHL in lending.call_dispositions (14-day follow-up)"),
    Flag("LENDING_GHL_CALL_GATE_ENABLED", "Tag ndl-do-not-call-now on GHL list contacts that cannot be called now (G5)"),
    Flag("LENDING_GHL_LIST_SCRUB_ENABLED", "DNC scrub of numbers on Josh's GHL call lists (Tracerfy credits)"),
    Flag("LENDING_GHL_RELEASE_SLOT_ENABLED", "A failed caller check cancels the AI-booked GHL appointment"),
    Flag("LENDING_BACKFLIP_CHECK_ENABLED", "Backflip borrower conflict check on the dialer load (blocks all if the feed is stale)"),
    # Rules and reports
    Flag("LENDING_RULES_FROM_DB", "Lender rules read from lending.lender_rule_values instead of config (T-05)"),
    Flag("LENDING_DAVID_SUMMARY_ENABLED", "David's end-of-day activity post in #isa-daily"),
    Flag("LENDING_AUDIENCE_EXPORT_ENABLED", "Nightly hashed retargeting audience export"),
    # Testing and diagnostics
    Flag("LENDING_SLACK_TEST_ENABLED", "Post Slack messages about test records too (testing only)"),
    Flag("LENDING_GHL_LOG_PAYLOAD_SHAPE", "Log the key paths (never values) of lending GHL webhooks (diagnostics)"),
)


def _is_flag_setting(name: str) -> bool:
    upper = name.upper()
    return upper.startswith("LENDING_") and (upper.endswith("_ENABLED") or upper.endswith("_APPROVED"))


def unlisted_flags(settings_cls: type[AppSettings] = AppSettings) -> list[str]:
    """``LENDING_*_ENABLED`` / ``*_APPROVED`` settings missing from ``FLAGS`` (the test keeps this empty)."""
    listed = {flag.env for flag in FLAGS}
    return sorted(name.upper() for name in settings_cls.model_fields
                  if _is_flag_setting(name) and name.upper() not in listed)


def flag_rows(settings: Any) -> list[tuple[str, bool, bool, str]]:
    """(flag, live value, code default, description) for every flag, in ``FLAGS`` order."""
    rows = []
    for flag in FLAGS:
        field = flag.env.lower()
        rows.append((flag.env, bool(getattr(settings, field)), bool(AppSettings.model_fields[field].default),
                     flag.description))
    return rows


def markdown_table(settings: Any) -> str:
    rows = flag_rows(settings)
    on = sum(1 for _, live, _, _ in rows if live)
    lines = [f"Lending feature flags in production: {on} on, {len(rows) - on} off", "",
             "| Flag | Status | Default | What it does |", "|---|---|---|---|"]
    for env, live, default, description in rows:
        changed = " (changed)" if live != default else ""
        lines.append(f"| `{env}` | {'ON' if live else 'off'}{changed} | {'on' if default else 'off'} | {description} |")
    return "\n".join(lines)


def _post(text: str, channel: str, token: str) -> None:
    from slack_sdk import WebClient

    WebClient(token=token).chat_postMessage(channel=channel, text=f"```\n{text}\n```", unfurl_links=False)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Print (and optionally post) the lending feature flag table")
    parser.add_argument("--post", action="store_true", help="also post the table to Slack")
    parser.add_argument("--channel", help="Slack channel id (default: LENDING_CORA_CHANNEL)")
    args = parser.parse_args(argv)
    settings = get_settings()
    table = markdown_table(settings)
    print(table)
    if not args.post:
        return 0
    channel = args.channel or settings.lending_cora_channel
    token = settings.lending_cora_slack_bot_token
    if not (channel and token):
        logger.error("[flags-report] not posted: LENDING_CORA_CHANNEL (or --channel) or LENDING_CORA_SLACK_BOT_TOKEN not set")
        return 1
    try:
        _post(table, channel, token.get_secret_value())
    except Exception as exc:  # class only: Slack errors can echo the message
        logger.error("[flags-report] Slack post failed: %s", type(exc).__name__)
        return 1
    print(f"posted to {channel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
