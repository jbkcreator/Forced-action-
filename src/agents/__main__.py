"""
Lifecycle Agents — process entry point.

Usage:
    python -m src.agents                # show help
    python -m src.agents --serve        # start supervisor with all listeners (production)
    python -m src.agents --migrate      # run LangGraph checkpoint migration and exit
    python -m src.agents --health       # run health checks and exit (0 = healthy)

The supervisor blocks until SIGINT / SIGTERM. It starts two daemon threads:
    - Redis Pub/Sub listener on channel "lifecycle:events"
    - Postgres LISTEN listener on channel "lifecycle_events"

Both are controlled by AGENTS_EVENT_SOURCE_REDIS / AGENTS_EVENT_SOURCE_POSTGRES.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logger = logging.getLogger(__name__)

# ── ANSI colours (stripped when output is not a TTY) ─────────────────────────
_IS_TTY = sys.stderr.isatty()
_G  = "\033[32m" if _IS_TTY else ""   # green
_Y  = "\033[33m" if _IS_TTY else ""   # yellow
_R  = "\033[31m" if _IS_TTY else ""   # red
_B  = "\033[34m" if _IS_TTY else ""   # blue
_DIM = "\033[2m" if _IS_TTY else ""   # dim
_RST = "\033[0m" if _IS_TTY else ""   # reset
_BOLD = "\033[1m" if _IS_TTY else ""  # bold

_OK   = f"{_G}✓{_RST}"
_WARN = f"{_Y}⚠{_RST}"
_FAIL = f"{_R}✗{_RST}"
_INFO = f"{_B}→{_RST}"


# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

def _configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-7s  %(name)s: %(message)s",
        stream=sys.stderr,
    )
    # Quiet noisy third-party loggers
    for noisy in ("httpx", "httpcore", "anthropic", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# Health checks
# ─────────────────────────────────────────────────────────────────────────────

def _run_health_checks(*, fatal_on_error: bool = True) -> bool:
    """
    Run all pre-flight checks and print a structured startup report.
    Returns True if all critical checks pass. Exits the process when
    fatal_on_error=True and a critical check fails.
    """
    from config.agents import get_agents_settings

    settings = get_agents_settings()
    checks_passed = 0
    checks_failed = 0

    _banner("Lifecycle Agents — startup health check")

    # ── 1. Config ─────────────────────────────────────────────────────────────
    _section("Configuration")
    _check("DATABASE_URL set",     bool(settings.database_url),        critical=True)
    _check("ANTHROPIC_API_KEY set", bool(settings.anthropic_api_key),  critical=True)
    _check("REDIS_URL set",         bool(settings.redis_url),          critical=False)
    _check(
        f"Global kill switch",
        not settings.agents_global_kill_switch,
        note="AGENTS_GLOBAL_KILL_SWITCH=true — supervisor will idle" if settings.agents_global_kill_switch else None,
        critical=False,
        invert_display=True,
    )
    _print_kv("Log level",           settings.agents_log_level)
    _print_kv("Worker concurrency",  str(settings.agents_worker_concurrency))
    _print_kv("Token budget/decision", str(settings.agents_max_tokens_per_decision))
    _print_kv("Cost budget/decision",  f"${settings.agents_max_cost_usd_per_decision:.2f}")
    _print_kv("Checkpoint schema",   settings.agents_checkpoint_schema)

    # ── 2. Enabled graphs ─────────────────────────────────────────────────────
    _section("Enabled Graphs")
    graphs = settings.enabled_graphs
    if graphs:
        for g in graphs:
            _line(f"  {_OK} {g}")
    else:
        _line(f"  {_WARN} No graphs enabled (AGENTS_GRAPHS_ENABLED is empty)")

    # ── 3. Postgres ───────────────────────────────────────────────────────────
    _section("Postgres")
    pg_ok = _check_postgres(settings.database_url)
    _check("Postgres reachable", pg_ok, critical=True)

    # ── 4. Checkpoint schema ──────────────────────────────────────────────────
    if pg_ok:
        ckpt_ok = _check_checkpoint_schema(settings)
        _check(
            f"Checkpoint schema '{settings.agents_checkpoint_schema}' reachable",
            ckpt_ok,
            critical=False,
        )

    # ── 5. Redis ──────────────────────────────────────────────────────────────
    _section("Redis")
    if settings.agents_event_source_redis and settings.redis_url:
        redis_ok = _check_redis(settings.redis_url)
        _check("Redis reachable",       redis_ok, critical=False)
        _check("Redis listener enabled", True,    critical=False)
    elif not settings.agents_event_source_redis:
        _line(f"  {_WARN} Redis listener disabled (AGENTS_EVENT_SOURCE_REDIS=false)")
    else:
        _line(f"  {_WARN} REDIS_URL not set — Redis listener will not start")

    # ── 6. Postgres LISTEN ────────────────────────────────────────────────────
    _section("Postgres LISTEN/NOTIFY")
    if settings.agents_event_source_postgres:
        _check("Postgres listener enabled", True, critical=False)
        _line(f"  {_INFO} Channel: lifecycle_events  |  queue sweep: 60s interval")
    else:
        _line(f"  {_WARN} Postgres listener disabled (AGENTS_EVENT_SOURCE_POSTGRES=false)")

    # ── 7. LangSmith ─────────────────────────────────────────────────────────
    _section("LangSmith Tracing")
    if settings.langsmith_tracing and settings.langsmith_api_key:
        _check("LangSmith tracing",  True, critical=False)
        _print_kv("Project", settings.langsmith_project)
        _print_kv("Endpoint", settings.langsmith_endpoint)
    elif settings.langsmith_tracing:
        _line(f"  {_WARN} LANGSMITH_TRACING=true but LANGSMITH_API_KEY not set — tracing disabled")
    else:
        _line(f"  {_DIM}Tracing disabled (set LANGSMITH_TRACING=true to enable){_RST}")

    # ── 8. Anthropic API ──────────────────────────────────────────────────────
    _section("Anthropic API")
    _check("API key configured", bool(settings.anthropic_api_key), critical=True)

    # ── Summary ───────────────────────────────────────────────────────────────
    all_ok = _summary()

    if not all_ok and fatal_on_error:
        _line(f"\n{_R}{_BOLD}Startup aborted — fix the errors above before running --serve.{_RST}\n")
        sys.exit(1)

    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
# Health check helpers
# ─────────────────────────────────────────────────────────────────────────────

_check_results: list[tuple[str, bool, bool]] = []  # (label, passed, critical)


def _banner(title: str) -> None:
    width = 60
    _line(f"\n{_BOLD}{'─' * width}{_RST}")
    _line(f"{_BOLD}  {title}{_RST}")
    _line(f"{_BOLD}{'─' * width}{_RST}")


def _section(name: str) -> None:
    _line(f"\n{_DIM}{name}{_RST}")


def _line(msg: str) -> None:
    print(msg, file=sys.stderr)


def _print_kv(key: str, val: str) -> None:
    _line(f"  {_DIM}{key}:{_RST} {val}")


def _check(
    label: str,
    passed: bool,
    *,
    critical: bool = True,
    note: str | None = None,
    invert_display: bool = False,
) -> bool:
    icon = (_OK if passed else _WARN) if not critical else (_OK if passed else _FAIL)
    if invert_display:
        icon = _OK if passed else _WARN
    suffix = f"  {_DIM}({note}){_RST}" if note else ""
    _line(f"  {icon} {label}{suffix}")
    _check_results.append((label, passed, critical))
    return passed


def _summary() -> bool:
    failed_critical = [(l, p, c) for l, p, c in _check_results if c and not p]
    _line(f"\n{'─' * 60}")
    if failed_critical:
        _line(f"  {_FAIL} {len(failed_critical)} critical check(s) failed:")
        for label, _, _ in failed_critical:
            _line(f"      • {label}")
        return False
    _line(f"  {_OK} {_BOLD}All critical checks passed — ready to serve{_RST}")
    return True


def _check_postgres(database_url: str) -> bool:
    try:
        import psycopg2
        url = database_url.replace("+psycopg2", "").replace("+psycopg", "")
        conn = psycopg2.connect(url, connect_timeout=5)
        conn.close()
        return True
    except Exception as exc:
        logger.debug("Postgres check failed: %s", exc)
        return False


def _check_checkpoint_schema(settings) -> bool:
    try:
        from psycopg import Connection
        url = settings.database_url
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if url.startswith(prefix):
                url = "postgresql://" + url[len(prefix):]
        conn = Connection.connect(url, autocommit=True, connect_timeout=5)
        schema = settings.agents_checkpoint_schema
        with conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                (schema,),
            )
            exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception as exc:
        logger.debug("Checkpoint schema check failed: %s", exc)
        return False


def _check_redis(redis_url: str) -> bool:
    try:
        import redis as _redis
        client = _redis.from_url(redis_url, socket_connect_timeout=3)
        client.ping()
        client.close()
        return True
    except Exception as exc:
        logger.debug("Redis check failed: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────

def cmd_migrate() -> int:
    from src.agents.checkpoint import run_checkpoint_migration
    logger.info("Running LangGraph checkpoint migration…")
    run_checkpoint_migration()
    logger.info("Checkpoint migration complete.")
    return 0


def cmd_serve() -> int:
    _run_health_checks(fatal_on_error=True)

    from config.agents import get_agents_settings
    settings = get_agents_settings()

    logger.info(
        "Starting Lifecycle Agents supervisor | graphs=%s | redis=%s | postgres=%s",
        settings.agents_graphs_enabled,
        settings.agents_event_source_redis,
        settings.agents_event_source_postgres,
    )

    from src.agents.events.ingestion import run_forever
    run_forever()
    return 0


def cmd_health() -> int:
    ok = _run_health_checks(fatal_on_error=False)
    return 0 if ok else 1


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    from config.agents import get_agents_settings

    parser = argparse.ArgumentParser(
        prog="python -m src.agents",
        description="Lifecycle Agents — autonomous decision supervisor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m src.agents --serve      # production: start supervisor\n"
            "  python -m src.agents --migrate    # run checkpoint migration\n"
            "  python -m src.agents --health     # pre-flight health check\n"
        ),
    )
    parser.add_argument("--serve",   action="store_true", help="Start supervisor with all listeners")
    parser.add_argument("--migrate", action="store_true", help="Run LangGraph checkpoint migration and exit")
    parser.add_argument("--health",  action="store_true", help="Run health checks and exit (0 = healthy)")

    args = parser.parse_args(argv)

    if not any([args.serve, args.migrate, args.health]):
        parser.print_help()
        return 2

    settings = get_agents_settings()
    _configure_logging(settings.agents_log_level)

    if args.migrate:
        return cmd_migrate()

    if args.health:
        return cmd_health()

    if args.serve:
        return cmd_serve()

    return 0


if __name__ == "__main__":
    sys.exit(main())
