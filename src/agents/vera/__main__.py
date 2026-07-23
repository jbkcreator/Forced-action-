"""
Vera — process entry point.

Usage:
    python -m src.agents.vera --health
    python -m src.agents.vera --live-state
    python -m src.agents.vera --revenue-truth

--health is V1's scaffolding check. --live-state is V2's daily pre-8am
report (prod-hash vs dev HEAD, cron freshness, deploy/migration drift,
silent-failure detection — see src.agents.vera.checks.live_state).
--revenue-truth is V3's daily revenue truth report (two-way Stripe-vs-DB
subscriber reconciliation, real MRR, new/failed payments, refunds &
disputes — see src.agents.vera.checks.revenue_truth). A later sub-task (V4)
adds the promise digest / seeded-discrepancy subcommand, following the same
pattern: check the vera_global kill switch first, then run read-only checks
through src.agents.vera.db, then write results via src.agents.vera.facts.

Modeled on src/agents/__main__.py's health-check report shape, but this is a
fully separate process from that supervisor — no shared runtime, no shared
event loop.
"""
from __future__ import annotations

import argparse
import logging
import sys

from src.utils.logger import setup_logging

logger = logging.getLogger(__name__)

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
    from src.agents.vera.config import KILL_SWITCH_FEATURE
    from src.agents.vera.db import check_connection
    from src.services.kill_switch_service import get_kill_switch_status

    settings = get_settings()
    all_ok = True

    _line("\nVera — health check")
    _line("-" * 40)

    dsn_set = bool(settings.vera_database_url)
    _line(f"  {_OK if dsn_set else _FAIL} VERA_DATABASE_URL set")
    all_ok &= dsn_set

    if dsn_set:
        ro_ok = check_connection()
        _line(f"  {_OK if ro_ok else _FAIL} vera_readonly connection reachable")
        all_ok &= ro_ok
    else:
        _line(f"  {_WARN} skipping connection check — no DSN configured")
        ro_ok = False

    if ro_ok:
        from sqlalchemy import text
        from src.agents.vera.db import vera_db
        try:
            with vera_db.session_scope() as session:
                session.execute(text("SELECT 1 FROM vera_facts LIMIT 1"))
            _line(f"  {_OK} vera_facts table reachable")
        except Exception as exc:
            _line(f"  {_FAIL} vera_facts table not reachable: {exc}")
            all_ok = False

    status = get_kill_switch_status(KILL_SWITCH_FEATURE)
    color = status.get("color", "unknown")
    icon = _OK if color in ("green", "unknown") else (_WARN if color == "yellow" else _FAIL)
    _line(f"  {icon} kill switch [{KILL_SWITCH_FEATURE}] = {color}")

    _line("-" * 40)
    if all_ok:
        _line(f"{_OK} Vera scaffolding healthy\n")
    else:
        _line(f"{_FAIL} Vera scaffolding has failing checks\n")
    return 0 if all_ok else 1


def cmd_live_state() -> int:
    from src.agents.vera.checks.live_state import run_live_state
    return run_live_state()


def cmd_revenue_truth() -> int:
    from src.agents.vera.checks.revenue_truth import run_revenue_truth
    return run_revenue_truth()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.agents.vera",
        description="Vera — truth & verification agent (read-only)",
    )
    parser.add_argument("--health", action="store_true", help="Run scaffolding health check and exit")
    parser.add_argument(
        "--live-state", action="store_true",
        help="Run the daily live-state report (deploy drift, cron freshness, "
             "silent failures) and exit",
    )
    parser.add_argument(
        "--revenue-truth", action="store_true",
        help="Run the daily revenue truth report (two-way Stripe-vs-DB "
             "reconciliation, real MRR, new/failed payments, refunds & disputes) "
             "and exit",
    )
    args = parser.parse_args(argv)

    setup_logging()

    if args.health:
        return cmd_health()

    if args.live_state:
        return cmd_live_state()

    if args.revenue_truth:
        return cmd_revenue_truth()

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
