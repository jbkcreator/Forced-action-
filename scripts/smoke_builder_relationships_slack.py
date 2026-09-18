"""Manual smoke test — post a sample builder RELATIONSHIPS alert to Slack.

Use a TEST workspace: set SLACK_BOT_TOKEN + FA_MAX_SLACK_CHANNEL_RELATIONSHIPS in
the environment to a test bot token and a test channel (invite the bot first),
then run:

    PYTHONPATH=. python scripts/smoke_builder_relationships_slack.py

If Slack is unconfigured it no-ops and says so — no error. This posts a single
fake builder opportunity card; it does NOT touch the database.
"""
from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal

from src.services.builder_patterns import BuilderHit
from src.services.builder_relationships import (
    build_relationships_blocks,
    build_relationships_text,
    emit_relationships_alert,
)


def _safe_print(text: str) -> None:
    enc = sys.stdout.encoding or "utf-8"
    sys.stdout.write(text.encode(enc, errors="replace").decode(enc) + "\n")


def main() -> None:
    hit = BuilderHit(
        pattern="repeat_builder",
        buyer_entity_id=805056,
        principal_name="172 MANGROVE LLC",
        evidence_permit_ids=[1001, 1002, 1003],
        staging_permit_ids=[],
        county_id="hillsborough",
        latest_permit_date=date.today(),
        total_job_value=Decimal("1250000"),
        property_id=None,
    )

    _safe_print("=== plain-text fallback ===")
    _safe_print(build_relationships_text(hit))
    _safe_print("=== attempting Slack post ===")
    posted = emit_relationships_alert(hit)
    if posted:
        _safe_print("done - check the RELATIONSHIPS channel.")
    else:
        _safe_print("no-op — Slack unconfigured or post failed (check logs).")


if __name__ == "__main__":
    main()
