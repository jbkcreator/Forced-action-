"""Manual smoke test — post a sample builder EXCEPTIONS alert to Slack.

Use a TEST workspace: set SLACK_BOT_TOKEN + FA_MAX_SLACK_CHANNEL_EXCEPTIONS in
the environment to a test bot token and a test channel (invite the bot first),
then run:

    PYTHONPATH=. python scripts/smoke_builder_exceptions_slack.py

If Slack is unconfigured it no-ops and says so — no error. This posts a single
fake low-confidence-resolution alert; it does NOT touch the database.
"""
from __future__ import annotations

import sys

from src.services.buyer_entity_resolution import _emit_exceptions_alerts, _build_exceptions_message


def _safe_print(text: str) -> None:
    """Print without crashing on a non-UTF-8 console (Windows cp1252)."""
    enc = sys.stdout.encoding or "utf-8"
    sys.stdout.write(text.encode(enc, errors="replace").decode(enc) + "\n")


def main() -> None:
    sample = [
        ("permit_staging", 101, "ACME HOMES LLC", 62, 5001),
        ("building_permits", 202, "BUILDPRO INC", 58, 5002),
    ]
    _safe_print("=== message that will be sent ===")
    _safe_print(_build_exceptions_message(sample))
    _safe_print("=== attempting Slack post ===")
    _emit_exceptions_alerts(sample)
    _safe_print("done - check the EXCEPTIONS channel (or the log line if unconfigured).")


if __name__ == "__main__":
    main()
