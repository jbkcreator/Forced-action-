"""
Relay — process entry point (RELAY-v2.2 sub-tasks R1 + R2).

Usage:
    python -m src.services.relay --health
    python -m src.services.relay --sweep
    python -m src.services.relay --seed --channel noop --recipient test@example.com --payload-json '{"subject": "Hi"}'
    python -m src.services.relay --setup-email-channel

--health is R1's scaffolding check. --sweep runs one approval-queue sweep
(what cron calls every 30 minutes — see scripts/cron/crontab.txt).
--seed is R1's stand-in for Cora (Phase 2, not yet built): it writes a
'pending' row via src.services.relay.queue.enqueue() and posts it to Slack
for approval — the exact same call Cora will make later, so nothing here
changes when she lands. --setup-email-channel is R2's one-time, idempotent
setup command: finds or creates the "Relay passthrough" Instantly campaign
and prints the id to set as RELAY_INSTANTLY_CAMPAIGN_ID in .env — see
src.services.relay.channels_email and RELAY-R2-Implementation-Plan.md.

Modeled on src/agents/vera/__main__.py's CLI shape, but Relay is a
deterministic non-agent service (see Locked decision #1 in
RELAY-R1-Implementation-Plan.md) — this is src/services/relay, not
src/agents/relay.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid

from src.utils.logger import setup_logging

# Import for its registration side effect only — makes the real 'email'
# channel available in DISPATCHERS before any --sweep/--seed runs (R1's
# channels.py ships only the 'noop' test channel).
import src.services.relay.channels_email  # noqa: F401

if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

_IS_TTY = sys.stderr.isatty()
_OK = "\033[32m✓\033[0m" if _IS_TTY else "[OK]"
_WARN = "\033[33m⚠\033[0m" if _IS_TTY else "[WARN]"
_FAIL = "\033[31m✗\033[0m" if _IS_TTY else "[FAIL]"


def _line(msg: str) -> None:
    print(msg, file=sys.stderr)


def cmd_health() -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.services.kill_switch_service import get_kill_switch_status
    from src.services.relay.config import KILL_SWITCH_FEATURE
    from sqlalchemy import text

    settings = get_settings()
    all_ok = True

    _line("\nRelay — health check")
    _line("-" * 40)

    dsn_set = bool(settings.database_url)
    _line(f"  {_OK if dsn_set else _FAIL} DATABASE_URL set")
    all_ok &= dsn_set

    table_ok = False
    if dsn_set:
        try:
            with get_db_context() as session:
                session.execute(text("SELECT 1 FROM relay_approval_queue LIMIT 1"))
            table_ok = True
        except Exception as exc:
            _line(f"  {_FAIL} relay_approval_queue not reachable: {exc}")
        else:
            _line(f"  {_OK} relay_approval_queue table reachable")
    all_ok &= table_ok

    status = get_kill_switch_status(KILL_SWITCH_FEATURE)
    color = status.get("color", "unknown")
    icon = _OK if color in ("green", "unknown") else (_WARN if color == "yellow" else _FAIL)
    _line(f"  {icon} kill switch [{KILL_SWITCH_FEATURE}] = {color}")

    slack_configured = bool(settings.slack_bot_token and settings.relay_slack_channel)
    icon = _OK if slack_configured else _WARN
    _line(f"  {icon} Slack configured (relay_slack_channel + slack_bot_token)")

    _line("-" * 40)
    if all_ok:
        _line(f"{_OK} Relay scaffolding healthy\n")
    else:
        _line(f"{_FAIL} Relay scaffolding has failing checks\n")
    return 0 if all_ok else 1


def cmd_sweep() -> int:
    from src.services.relay.sweep import run_sweep

    result = run_sweep()
    _line(
        f"sweep: sent={result.sent} skipped={result.skipped} "
        f"failed={result.failed} halted={result.halted} "
        f"processed={len(result.processed_ids)}"
    )
    return 0 if not result.halted else 1


def cmd_setup_email_channel() -> int:
    from config.settings import get_settings
    from src.services import instantly_service as instantly
    from src.services.relay.channels_email import PASSTHROUGH_CAMPAIGN_NAME

    settings = get_settings()

    existing = [
        c for c in instantly.list_campaigns()
        if c.get("name") == PASSTHROUGH_CAMPAIGN_NAME
    ]
    if existing:
        campaign_id = existing[0].get("id")
        _line(f"{_OK} found existing passthrough campaign id={campaign_id}")
    else:
        schedule = {
            "schedules": [{
                "name": "always-on",
                "timing": {"from": "00:00", "to": "23:59"},
                "days": {str(i): True for i in range(7)},
                "timezone": "America/Detroit",
            }],
        }
        sequence_steps = [{
            "type": "email",
            "delay": 0,
            "variants": [{"subject": "{{ra_subject}}", "body": "{{ra_body}}"}],
        }]
        result = instantly.create_campaign(
            name=PASSTHROUGH_CAMPAIGN_NAME,
            schedule=schedule,
            sequence_steps=sequence_steps,
            email_list=[settings.relay_instantly_sender_email],
        )
        if not result or not result.get("id"):
            _line(f"{_FAIL} campaign creation failed — check INSTANTLY_API_KEY/INSTANTLY_ENABLED and logs")
            return 1
        campaign_id = result["id"]
        instantly.activate_campaign(campaign_id)
        _line(f"{_OK} created + activated passthrough campaign id={campaign_id}")

    _line(f"\nSet this in .env, then restart Relay:\n  RELAY_INSTANTLY_CAMPAIGN_ID={campaign_id}\n")
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    from src.services.relay import queue as relay_queue
    from src.services.relay.slack_post import post_for_approval

    try:
        payload = json.loads(args.payload_json) if args.payload_json else {}
    except Exception as exc:
        _line(f"{_FAIL} --payload-json is not valid JSON: {exc}")
        return 2

    idempotency_key = args.idempotency_key or f"seed-{uuid.uuid4().hex[:16]}"
    item = relay_queue.enqueue(
        idempotency_key=idempotency_key,
        channel=args.channel,
        recipient=args.recipient,
        payload=payload,
        thread_id=args.thread_id,
        # --seed is a founder/dev manual-testing tool, not real Cora/THROUGH
        # traffic -- it has always allowed --thread-id to be omitted
        # (QUALITY-v2.2 Q3's contract guard would otherwise reject that).
        skip_contract_validation=True,
    )
    post_for_approval(item)
    _line(f"{_OK} seeded item id={item.id} idempotency_key={item.idempotency_key!r} status={item.status}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.services.relay",
        description="Relay — deterministic execution service (non-agent)",
    )
    parser.add_argument("--health", action="store_true", help="Run scaffolding health check and exit")
    parser.add_argument(
        "--sweep", action="store_true",
        help="Run one approval-queue sweep (execute all 'approved' rows as a batch) and exit",
    )
    parser.add_argument(
        "--seed", action="store_true",
        help="Seed one 'pending' row and post it to Slack for approval (Cora's Phase 2 stand-in) and exit",
    )
    parser.add_argument(
        "--setup-email-channel", action="store_true",
        help="Find or create the Relay passthrough Instantly campaign and print its id (one-time setup) and exit",
    )
    parser.add_argument("--channel", default="noop", help="Channel for --seed (default: noop)")
    parser.add_argument("--recipient", help="Recipient for --seed (email/phone)")
    parser.add_argument("--payload-json", help="JSON payload for --seed, e.g. '{\"subject\": \"Hi\"}'")
    parser.add_argument("--thread-id", help="Opportunity Thread ID for --seed (optional)")
    parser.add_argument("--idempotency-key", help="Idempotency key for --seed (default: random)")
    args = parser.parse_args(argv)

    setup_logging()

    if args.health:
        return cmd_health()

    if args.sweep:
        return cmd_sweep()

    if args.seed:
        if not args.recipient:
            parser.error("--seed requires --recipient")
        return cmd_seed(args)

    if args.setup_email_channel:
        return cmd_setup_email_channel()

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
