"""
WP-T2-1 EXCEPTIONS Alert Real E2E Test
========================================
Run: PYTHONPATH=. python scripts/e2e_wp_t2_1_exceptions_alert_test.py

Proves the "Alert in EXCEPTIONS queue when reputation falls below threshold"
scope item end to end, with real evidence:

  1. Seeds real synthetic rows into relay_approval_queue (venture_key=
     fa_max_lending) so the trailing-24h failure rate genuinely crosses
     FAILURE_RATE_CONCERN (10%) with more than MIN_SENT_FLOOR (10) total
     sends — the exact real signal fa_max_send_health_monitor.py watches.
  2. Calls the REAL production _relay_failure_trip() to compute a REAL
     Trip object from that data (no hand-authored alert text).
  3. Calls the REAL exceptions_alert_queue.enqueue_and_attempt() — the same
     function run_and_page() calls in production — with a full, correct
     sandbox env override (bot token + EXCEPTIONS channel) so delivery
     actually lands.
  4. Independently verifies delivery via conversations.history and prints
     the real message ts for the user to check in Slack themselves.

Posts to the sandbox EXCEPTIONS channel only — never production.
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone

if not os.environ.get("FA_MAX_SLACK_BOT_TOKEN"):
    raise SystemExit(
        "FA_MAX_SLACK_BOT_TOKEN is not set. Source the sandbox credentials first, e.g.:\n"
        "  set -a; source secrets/sandbox-slack-listener.env; set +a"
    )
os.environ.setdefault("FA_MAX_SLACK_CHANNEL_EXCEPTIONS", "C0C283PNZ5Y")
# post_exceptions_alert() previously posted with settings.slack_bot_token
# (the GENERAL SLACK_BOT_TOKEN) regardless of venture -- fixed to use the
# same venture-aware _resolve_bot_token convention as every other FA Max
# Slack surface (post_receipt, _resolve_bot_token). Deliberately NOT setting
# the general SLACK_BOT_TOKEN env var here: this run proves the fix by
# posting through FA_MAX_SLACK_BOT_TOKEN alone.

from config.settings import get_settings  # noqa: E402
get_settings.cache_clear()

from sqlalchemy import text  # noqa: E402

from src.core.database import get_db_context  # noqa: E402
from src.tasks.fa_max_send_health_monitor import _relay_failure_trip, FA_MAX_VENTURE  # noqa: E402
from src.services.relay import exceptions_alert_queue  # noqa: E402

BOLD, GREEN, RED, CYAN, RESET = "\033[1m", "\033[92m", "\033[91m", "\033[96m", "\033[0m"

BATCH_TAG = f"zztest_repalert_{uuid.uuid4().hex[:8]}"


def seed_rows() -> tuple[list[int], list[str]]:
    """Seed 8 failed + 2 sent rows -> 8/10 = 80% failure rate, well over the 10% concern threshold."""
    ids: list[int] = []
    person_ids: list[str] = []
    with get_db_context() as db:
        for i in range(10):
            pid = str(uuid.uuid4())
            db.execute(
                text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                     "VALUES (:pid ::uuid, 'identified', 'zztest_repalert')"),
                {"pid": pid},
            )
            person_ids.append(pid)
        db.commit()

        for i in range(8):
            row = db.execute(
                text(
                    "INSERT INTO relay_approval_queue "
                    "(idempotency_key, channel, recipient, payload, status, venture_key, "
                    " lane, agent_name, autonomy_tier_at_send, person_id, updated_at) "
                    "VALUES (:idem, 'sms', 'zztest-recipient', '{}'::jsonb, 'failed', :venture, "
                    " 'MONEY', 'zztest_agent', 'A', :pid ::uuid, now()) RETURNING id"
                ),
                {"idem": f"{BATCH_TAG}-fail-{i}", "venture": FA_MAX_VENTURE, "pid": person_ids[i]},
            ).scalar_one()
            ids.append(row)
        for i in range(2):
            row = db.execute(
                text(
                    "INSERT INTO relay_approval_queue "
                    "(idempotency_key, channel, recipient, payload, status, venture_key, "
                    " lane, agent_name, autonomy_tier_at_send, person_id, updated_at) "
                    "VALUES (:idem, 'sms', 'zztest-recipient', '{}'::jsonb, 'sent', :venture, "
                    " 'MONEY', 'zztest_agent', 'A', :pid ::uuid, now()) RETURNING id"
                ),
                {"idem": f"{BATCH_TAG}-sent-{i}", "venture": FA_MAX_VENTURE, "pid": person_ids[8 + i]},
            ).scalar_one()
            ids.append(row)
        db.commit()
    return ids, person_ids


def cleanup(ids: list[int], person_ids: list[str]) -> None:
    with get_db_context() as db:
        db.execute(text("DELETE FROM relay_approval_queue WHERE id = ANY(:ids)"), {"ids": ids})
        db.execute(
            text("DELETE FROM fa_max_exceptions_alert_queue WHERE rule = 'fa_max_relay_failure_rate_high' "
                 "AND message LIKE :pat"),
            {"pat": "%trailing 24h%"},
        )
        db.execute(text("DELETE FROM fa_max_persons WHERE person_id = ANY(:pids ::uuid[])"), {"pids": person_ids})
        db.commit()


def main() -> None:
    print(f"\n{BOLD}WP-T2-1 EXCEPTIONS Alert Test — real reputation-threshold breach -> real Slack post{RESET}\n")

    ids, person_ids = seed_rows()
    print(f"  seeded {len(ids)} real relay_approval_queue rows (8 failed / 2 sent, venture={FA_MAX_VENTURE})")

    with get_db_context() as db:
        trip = _relay_failure_trip(db, datetime.now(timezone.utc))

    if trip is None:
        print(f"  {RED}FAIL{RESET} — _relay_failure_trip() returned None; threshold not crossed.")
        cleanup(ids, person_ids)
        sys.exit(1)

    print(f"  {GREEN}real Trip computed{RESET}: rule={trip.rule!r}")
    print(f"    detail={trip.detail!r}")

    delivered = exceptions_alert_queue.enqueue_and_attempt(
        venture_key=FA_MAX_VENTURE, rule=trip.rule, message=trip.detail,
    )
    print(f"  enqueue_and_attempt() -> delivered={delivered}")

    with get_db_context() as db:
        row = db.execute(
            text(
                "SELECT id, status, attempts, error FROM fa_max_exceptions_alert_queue "
                "WHERE rule = :rule ORDER BY created_at DESC LIMIT 1"
            ),
            {"rule": trip.rule},
        ).mappings().first()

    if row is None:
        print(f"  {RED}FAIL{RESET} — no fa_max_exceptions_alert_queue row found.")
        cleanup(ids, person_ids)
        sys.exit(1)

    print(f"  DB row: id={row['id']} status={row['status']} attempts={row['attempts']} error={row['error']}")

    if row["status"] != "sent":
        print(f"  {RED}FAIL{RESET} — alert row did not reach status='sent'.")
        cleanup(ids, person_ids)
        sys.exit(1)

    # Independently verify via Slack Web API that the message really landed
    # (no slack_message_ts column on this table, so search recent history for it).
    from slack_sdk import WebClient
    web = WebClient(token=os.environ["FA_MAX_SLACK_BOT_TOKEN"])
    hist = web.conversations_history(channel=os.environ["FA_MAX_SLACK_CHANNEL_EXCEPTIONS"], limit=5)
    msgs = hist.get("messages", [])
    match = next((m for m in msgs if trip.rule in m.get("text", "")), None)
    if match is None:
        print(f"  {RED}FAIL{RESET} — conversations.history did not show the alert message in-channel.")
        print(f"  recent messages seen: {[m.get('text', '')[:80] for m in msgs]}")
        cleanup(ids, person_ids)
        sys.exit(1)

    print(f"\n  {GREEN}PASS{RESET} — real EXCEPTIONS alert delivered and independently confirmed via Slack API.")
    print(f"  {BOLD}channel=C0C283PNZ5Y  ts={match['ts']}{RESET}")
    print(f"  message text as posted: {match.get('text', '')[:300]}")
    print(f"\n  {CYAN}GO CHECK SLACK — sandbox EXCEPTIONS channel now has a real alert card.{RESET}")

    print("\n--- Cleanup ---")
    cleanup(ids, person_ids)
    print("cleaned up synthetic relay_approval_queue rows and the alert-queue row (Slack message itself is left "
          "in place as your evidence).")


if __name__ == "__main__":
    main()
