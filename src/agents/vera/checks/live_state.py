"""
Vera — live-state report (VERA-v2.2 sub-task V2).

Runs once daily, before 8am UTC, via `python -m src.agents.vera --live-state`.
Answers three questions by checking the live system directly, never by
trusting a claim:

    1. Is prod actually running the latest code?      -> check_deploy_drift()
    2. Are the scrapers actually fresh?                -> check_cron_freshness()
    3. Any silent-failure sources (zero-ingest /
       enabled-but-unscheduled)?                       -> check_silent_failures()

Every result is written as a dated row into vera_facts via
src.agents.vera.facts.write_fact(), then rendered into one plain-text report
and emailed to REPORT_RECIPIENTS. See tasks/New-agent-lane/VERA-V2-Implementation-Plan.md
for the full design.

Scope note: THE ONE NUMBER (new MRR added yesterday) reads V3's
`revenue.mrr.new_yesterday` fact when fresh (src.agents.vera.checks.revenue_truth),
falling back to a placeholder if V3 hasn't run yet or the fact has gone stale.
"""
from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Optional

from sqlalchemy import text

from src.agents.vera.checks._shared import report_recipients
from src.agents.vera.config import FRESHNESS_STATIC, KILL_SWITCH_FEATURE
from src.agents.vera.db import vera_db
from src.agents.vera.facts import read_facts, write_fact
from src.tasks.heartbeat_monitor import HEARTBEAT_SLAS, MULTI_COUNTY_SOURCES, SOURCE_OFF_DAYS

logger = logging.getLogger(__name__)

# Same hardcoded prod path convention as scripts/cron/run.sh / deploy.sh
# (neither reads this from settings/env either — Vera runs from the same
# checkout). Overridable per-call for testing.
PROD_REPO_DIR = "/root/Forced-action-"

_GIT_TIMEOUT_SECONDS = 15

# migrations/apply_*.py convention (CLAUDE.md): idempotent
# CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS DDL. These two regexes
# are the only DDL shapes recognized — anything else (role/grant DO $$ blocks,
# ALTER TYPE, index-only migrations) reports as "unrecognized", never silently
# assumed applied or not.
_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)", re.IGNORECASE)
_COLUMN_RE = re.compile(r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(\w+)", re.IGNORECASE)

# module (as invoked by scripts/cron/run.sh) -> source_type(s) it feeds in
# scraper_run_stats. Hand-verified against scripts/cron/crontab.txt and
# HEARTBEAT_SLAS (2026-07-23) — extend when a new source is added to either.
_MODULE_TO_SOURCE_TYPES: dict[str, set[str]] = {
    "src.scrappers.foreclosures.foreclosure_engine": {"foreclosures"},
    "src.scrappers.permit.permit_engine": {"permits"},
    "src.scrappers.roofing_permits.roofing_permit_engine": {"roofing_permits"},
    "src.scrappers.violation.violation_engine": {"violations"},
    "src.scrappers.violation.pinellas_violations_engine": {"violations"},
    "src.scrappers.probate.probate_engine": {"probate"},
    "src.scrappers.evictions.evictions_engine": {"evictions"},
    "src.scrappers.divorce.divorce_engine": {"divorce_filings"},
    "src.scrappers.bankruptcy.bankruptcy_engine": {"bankruptcy"},
    "src.scrappers.liens.lien_engine": {
        "lien_ml", "lien_tcl", "lien_ccl", "lien_hoa", "lien_tl", "judgments",
    },
    "src.tasks.sunbiz_enrichment": {"sunbiz"},
    "src.scrappers.storm.storm_engine": {"storm_damage"},
    "src.scrappers.fire.fire_engine": {"fire_incidents"},
    "src.scrappers.fire.pinellas_fire_engine": {"fire_incidents"},
    "src.scrappers.flood.flood_engine": {"flood_damage"},
    "src.scrappers.insurance.insurance_engine": {"insurance_claims"},
    "src.connectors.foreclosure_outcomes": {"foreclosure_outcomes"},
    "src.connectors.tax_deed_outcomes": {"tax_deed_outcomes"},
    "src.connectors.appraiser_sale_outcomes": {"appraiser_sale_outcomes"},
    "src.connectors.label_layer": {"outcome_label_layer"},
    "src.connectors.dor_sale_outcomes": {"dor_sale_outcomes"},
}


# ─────────────────────────────────────────────────────────────────────────────
# 3A — Deploy drift (prod hash vs dev HEAD, migration drift)
# ─────────────────────────────────────────────────────────────────────────────

def _run_git(args: list[str], cwd: str) -> Optional[str]:
    """Run one git command, read-only. Never raises; logs and returns None on failure."""
    try:
        result = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True,
            timeout=_GIT_TIMEOUT_SECONDS, check=False,
        )
        if result.returncode != 0:
            logger.warning(
                "[Vera] git %s failed (exit %d): %s",
                args, result.returncode, result.stderr.strip()[:300],
            )
            return None
        return result.stdout.strip()
    except Exception as exc:
        logger.warning("[Vera] git %s raised: %s", args, exc)
        return None


def _dev_head_sha(repo_dir: str) -> Optional[str]:
    """Tip of origin/dev via ls-remote — a network read, writes nothing to .git
    (a `git fetch` would mutate local objects; deliberately avoided)."""
    out = _run_git(["ls-remote", "origin", "dev"], cwd=repo_dir)
    if not out:
        return None
    first_line = out.splitlines()[0]
    parts = first_line.split()
    return parts[0] if parts else None


def _classify_drift(head_sha: Optional[str], last_good_sha: Optional[str],
                     dev_head_sha: Optional[str]) -> str:
    """in_sync / behind / head_mismatch / unknown. Pure function — no I/O."""
    if head_sha is None or dev_head_sha is None:
        return "unknown"
    if last_good_sha is not None and head_sha != last_good_sha:
        return "head_mismatch"
    if head_sha == dev_head_sha:
        return "in_sync"
    return "behind"


def _extract_migration_targets(file_text: str) -> tuple[list[str], list[tuple[str, str]]]:
    """Regex-extract (tables, [(table, column), ...]) declared by a migration file's
    own text. Pure function — no I/O."""
    tables = sorted(set(_TABLE_RE.findall(file_text)))
    columns = sorted(set(_COLUMN_RE.findall(file_text)))
    return tables, columns


def _classify_migration(session, file_text: str) -> str:
    """applied / not_applied / unrecognized, verified against the live schema
    via information_schema — no migration ledger table needed (ADR 0024 keeps
    none)."""
    tables, columns = _extract_migration_targets(file_text)
    if not tables and not columns:
        return "unrecognized"

    for table in tables:
        exists = session.execute(
            text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
            {"t": table},
        ).scalar()
        if not exists:
            return "not_applied"

    for table, column in columns:
        exists = session.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c"
            ),
            {"t": table, "c": column},
        ).scalar()
        if not exists:
            return "not_applied"

    return "applied"


def check_deploy_drift(repo_dir: str = PROD_REPO_DIR) -> dict:
    """prod SHA vs dev HEAD vs .last-good-deploy, plus per-migration-file
    applied/not_applied/unrecognized status.

    Migration enumeration reads every migrations/apply_*.py file present in
    the LOCAL working tree (what's actually on disk on this host) rather than
    diffing prod-SHA against dev-HEAD in git history — that diff would need
    the dev-HEAD commit object locally, which a `git fetch` would provide but
    which we deliberately never run (read-only guarantee). Any migration
    added to dev but not yet pulled to this host is invisible until the next
    deploy — covered by the `behind` drift verdict, not by this list.
    """
    head_sha = _run_git(["rev-parse", "HEAD"], cwd=repo_dir)

    last_good_sha = None
    last_good_path = Path(repo_dir) / ".last-good-deploy"
    if last_good_path.exists():
        try:
            last_good_sha = last_good_path.read_text(encoding="utf-8").strip() or None
        except Exception as exc:
            logger.warning("[Vera] could not read .last-good-deploy: %s", exc)

    dev_head_sha = _dev_head_sha(repo_dir)
    drift = _classify_drift(head_sha, last_good_sha, dev_head_sha)

    migration_statuses: dict[str, str] = {}
    migrations_dir = Path(repo_dir) / "migrations"
    if migrations_dir.is_dir():
        with vera_db.session_scope() as session:
            for path in sorted(migrations_dir.glob("apply_*.py")):
                try:
                    file_text = path.read_text(encoding="utf-8")
                except Exception as exc:
                    logger.warning("[Vera] could not read migration file %s: %s", path, exc)
                    migration_statuses[path.name] = "unrecognized"
                    continue
                migration_statuses[path.name] = _classify_migration(session, file_text)

    pending_migrations = sorted(
        name for name, status in migration_statuses.items() if status == "not_applied"
    )

    return {
        "head_sha": head_sha,
        "last_good_sha": last_good_sha,
        "dev_head_sha": dev_head_sha,
        "drift": drift,
        "migration_statuses": migration_statuses,
        "pending_migrations": pending_migrations,
    }


def _write_deploy_facts(deploy: dict) -> None:
    write_fact(
        "deploy.prod_sha", deploy["head_sha"] or "unknown",
        source="git", method="git rev-parse HEAD", freshness_class=FRESHNESS_STATIC,
    )
    write_fact(
        "deploy.dev_head_sha", deploy["dev_head_sha"] or "unknown",
        source="git", method="git ls-remote origin dev", freshness_class=FRESHNESS_STATIC,
    )
    write_fact(
        "deploy.drift", deploy["drift"],
        source="git", method="prod HEAD vs .last-good-deploy vs dev HEAD",
        freshness_class=FRESHNESS_STATIC,
    )
    write_fact(
        "deploy.pending_migrations", str(len(deploy["pending_migrations"])),
        value_numeric=Decimal(len(deploy["pending_migrations"])),
        source="git", method="scan migrations/apply_*.py on disk",
        freshness_class=FRESHNESS_STATIC,
    )
    for name, status in deploy["migration_statuses"].items():
        write_fact(
            f"deploy.migration.{name}.status", status,
            source="information_schema",
            method="regex-extract DDL targets + information_schema introspection",
            freshness_class=FRESHNESS_STATIC,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3B — Cron freshness (reuses heartbeat_monitor's registries, not its session)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CronBeat:
    source_type: str
    county_id: Optional[str]
    sla_minutes: int
    last_success_at: Optional[datetime]
    age_minutes: Optional[int]
    is_stale: bool

    def label(self) -> str:
        return f"{self.source_type}/{self.county_id}" if self.county_id else self.source_type


def _last_success(session, source_type: str, county_id: Optional[str]) -> Optional[datetime]:
    query = (
        "SELECT MAX(created_at) FROM scraper_run_stats "
        "WHERE source_type = :source_type AND run_success = true"
    )
    params = {"source_type": source_type}
    if county_id is not None:
        query += " AND county_id = :county_id"
        params["county_id"] = county_id
    return session.execute(text(query), params).scalar()


def check_cron_freshness(now: Optional[datetime] = None) -> list[CronBeat]:
    """Reuses heartbeat_monitor's HEARTBEAT_SLAS / SOURCE_OFF_DAYS /
    MULTI_COUNTY_SOURCES registries (single source of truth for SLA numbers)
    but re-runs the freshness query through Vera's own read-only session
    (vera_db) instead of heartbeat_monitor.compute_heartbeats(), which reads
    via the app role. Reports only — heartbeat_monitor already pages ops;
    Vera never double-alerts."""
    now = now or datetime.now(timezone.utc)
    today_wd = now.weekday()
    beats: list[CronBeat] = []

    with vera_db.session_scope() as session:
        for source_type, sla_minutes in HEARTBEAT_SLAS.items():
            if today_wd in SOURCE_OFF_DAYS.get(source_type, set()):
                continue

            counties = MULTI_COUNTY_SOURCES.get(source_type)
            county_list = sorted(counties) if counties else [None]

            for county_id in county_list:
                last_success = _last_success(session, source_type, county_id)
                if last_success is not None and last_success.tzinfo is None:
                    last_success = last_success.replace(tzinfo=timezone.utc)

                if last_success is None:
                    age_minutes = None
                    is_stale = True
                else:
                    age_minutes = int((now - last_success).total_seconds() // 60)
                    is_stale = age_minutes > sla_minutes

                beats.append(CronBeat(
                    source_type=source_type, county_id=county_id, sla_minutes=sla_minutes,
                    last_success_at=last_success, age_minutes=age_minutes, is_stale=is_stale,
                ))

    return beats


def _write_cron_facts(beats: list[CronBeat]) -> None:
    for beat in beats:
        write_fact(
            f"cron.{beat.source_type}.last_success",
            beat.last_success_at.isoformat() if beat.last_success_at else "never",
            county_id=beat.county_id, source="scraper_run_stats",
            method="MAX(created_at) WHERE run_success = true",
            freshness_class=FRESHNESS_STATIC,
        )
        write_fact(
            f"cron.{beat.source_type}.age_min",
            str(beat.age_minutes) if beat.age_minutes is not None else "unknown",
            value_numeric=Decimal(beat.age_minutes) if beat.age_minutes is not None else None,
            county_id=beat.county_id, source="scraper_run_stats",
            method="now - last_success", freshness_class=FRESHNESS_STATIC,
        )
        write_fact(
            f"cron.{beat.source_type}.freshness", "stale" if beat.is_stale else "fresh",
            county_id=beat.county_id, source="scraper_run_stats",
            method="age_minutes > sla_minutes", freshness_class=FRESHNESS_STATIC,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3C — Silent failures (scheduled-but-writing-nothing, enabled-but-unscheduled)
# ─────────────────────────────────────────────────────────────────────────────

def _scheduled_source_types(crontab_text: str) -> set[str]:
    """Source types with at least one active (uncommented) run.sh line in
    crontab.txt. Pure function — no I/O."""
    scheduled: set[str] = set()
    for line in crontab_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "run.sh" not in stripped:
            continue
        tokens = stripped.split()
        for i, tok in enumerate(tokens):
            if tok.endswith("run.sh") and i + 1 < len(tokens):
                module = tokens[i + 1]
                scheduled |= _MODULE_TO_SOURCE_TYPES.get(module, set())
                break
    return scheduled


def check_silent_failures(
    run_date: Optional[date] = None,
    crontab_path: Optional[Path] = None,
) -> dict:
    run_date = run_date or datetime.now(timezone.utc).date()
    crontab_path = crontab_path or (Path(PROD_REPO_DIR) / "scripts" / "cron" / "crontab.txt")

    with vera_db.session_scope() as session:
        rows = session.execute(
            text(
                "SELECT source_type, county_id, total_scraped FROM scraper_run_stats "
                "WHERE run_date = :run_date AND run_success = true AND total_scraped = 0"
            ),
            {"run_date": run_date},
        ).mappings().all()
    zero_ingest = [dict(row) for row in rows]

    unscheduled: list[str] = []
    if crontab_path.exists():
        try:
            crontab_text = crontab_path.read_text(encoding="utf-8")
            scheduled = _scheduled_source_types(crontab_text)
            unscheduled = sorted(set(HEARTBEAT_SLAS.keys()) - scheduled)
        except Exception as exc:
            logger.warning("[Vera] could not read crontab.txt: %s", exc)
    else:
        logger.warning(
            "[Vera] crontab.txt not found at %s — skipping enabled-but-unscheduled check",
            crontab_path,
        )

    return {"zero_ingest": zero_ingest, "unscheduled": unscheduled}


def _write_silent_failure_facts(silent: dict) -> None:
    for row in silent["zero_ingest"]:
        write_fact(
            f"silent.zero_ingest.{row['source_type']}", "true",
            county_id=row["county_id"], source="scraper_run_stats",
            method="run_success=true AND total_scraped=0 for today",
            freshness_class=FRESHNESS_STATIC,
        )
    for source_type in silent["unscheduled"]:
        write_fact(
            f"silent.unscheduled.{source_type}", "true",
            source="crontab", method="no active run.sh line maps to this source_type",
            freshness_class=FRESHNESS_STATIC,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3D — Report renderer + delivery + orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def _short(sha: Optional[str]) -> str:
    return sha[:12] if sha else "unknown"


def _the_one_number_line(one_number_fact_row: Optional[Mapping] = None) -> str:
    """Pure function — no DB access. Given the (already-fetched, by the
    caller) revenue.mrr.new_yesterday fact row — or None if V3 hasn't run
    yet or the fact has gone stale (<24h, FRESHNESS_REVENUE_24H) — returns
    THE ONE NUMBER line. Falls back to the original placeholder unchanged
    when no row is passed, so V2 stays fully correct with or without V3.
    Deliberately does not call read_facts() itself: this function is
    exercised directly by render_live_state_report()'s own tests, which
    must not require a live DB connection — see run_live_state() for the
    read_facts() call that supplies this parameter in production."""
    placeholder = "THE ONE NUMBER — new MRR added yesterday: — (pending V3 revenue reconciliation)"
    if one_number_fact_row is None:
        return placeholder
    value = one_number_fact_row.get("value_numeric")
    if value is not None:
        try:
            return f"THE ONE NUMBER — new MRR added yesterday: ${float(value) / 100:,.2f}"
        except (TypeError, ValueError):
            pass
    # Fresh fact exists but carries no numeric delta (e.g. V3's first-ever run,
    # with no prior day to diff against) — show its own text, not the
    # V3-hasn't-run placeholder, since that would misstate the situation.
    fact_value = one_number_fact_row.get("fact_value")
    if fact_value:
        return f"THE ONE NUMBER — new MRR added yesterday: {fact_value}"
    return placeholder


def render_live_state_report(
    deploy: dict,
    cron_beats: list[CronBeat],
    silent: dict,
    report_date: Optional[date] = None,
    one_number_fact_row: Optional[Mapping] = None,
) -> tuple[str, str]:
    """Returns (subject, body). Plain text, numbers first, Vera's voice.
    Pure — no DB access; `one_number_fact_row` is pre-fetched by the caller
    (run_live_state()) so this function stays testable without a live
    connection. Defaults to None (renders the V3-pending placeholder), which
    is exactly what every existing caller/test that doesn't pass it gets."""
    report_date = report_date or datetime.now(timezone.utc).date()
    stale = [b for b in cron_beats if b.is_stale]
    fresh_count = len(cron_beats) - len(stale)

    lines = [
        f"Vera — Live-State Report — {report_date.isoformat()}",
        "=" * 60,
        "",
        _the_one_number_line(one_number_fact_row),
        "",
        "DEPLOY",
        f"  Prod HEAD:        {_short(deploy['head_sha'])}",
        f"  Last good deploy: {_short(deploy['last_good_sha'])}",
        f"  Dev HEAD:         {_short(deploy['dev_head_sha'])}",
        f"  Drift:            {deploy['drift']}",
    ]

    if deploy["pending_migrations"]:
        lines.append(f"  Pending migrations ({len(deploy['pending_migrations'])}):")
        for name in deploy["pending_migrations"]:
            lines.append(f"    - {name}: not_applied")
    else:
        lines.append("  Pending migrations: none")

    unrecognized = sorted(
        name for name, status in deploy["migration_statuses"].items() if status == "unrecognized"
    )
    if unrecognized:
        lines.append(
            "  Unrecognized DDL shape (cannot verify applied/not-applied): "
            + ", ".join(unrecognized)
        )
    lines.append(
        "  Note: migration status is DB-verified via information_schema introspection "
        "of CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS targets — not a ledger "
        "lookup (ADR 0024 keeps none). Files using other DDL shapes report 'unrecognized', "
        "never silently assumed applied."
    )

    lines += ["", "CRON FRESHNESS"]
    if stale:
        for beat in sorted(stale, key=lambda b: b.label()):
            age = f"{beat.age_minutes} min" if beat.age_minutes is not None else "never run"
            lines.append(
                f"  STALE  {beat.label():<30} age={age:<12} sla={beat.sla_minutes / 60:.0f}h"
            )
    lines.append(f"  {fresh_count}/{len(cron_beats)} sources fresh")

    lines += ["", "SILENT FAILURES"]
    if silent["zero_ingest"]:
        lines.append("  Scheduled-but-writing-nothing today:")
        for row in silent["zero_ingest"]:
            lines.append(f"    - {row['source_type']}/{row['county_id']}")
    else:
        lines.append("  Scheduled-but-writing-nothing: none")

    if silent["unscheduled"]:
        lines.append("  Enabled-but-unscheduled:")
        for source_type in silent["unscheduled"]:
            lines.append(f"    - {source_type}")
    else:
        lines.append("  Enabled-but-unscheduled: none")

    lines += ["", "— Vera."]
    body = "\n".join(lines)

    subject = (
        f"[Vera] Live-State Report {report_date.isoformat()} — "
        f"drift={deploy['drift']}, {len(stale)} stale, {len(silent['zero_ingest'])} zero-ingest"
    )
    return subject, body


def run_live_state() -> int:
    """Entry point for `python -m src.agents.vera --live-state`."""
    from src.services.kill_switch_service import get_kill_switch_status

    status = get_kill_switch_status(KILL_SWITCH_FEATURE)
    if status.get("color") == "red":
        logger.warning(
            "[Vera] kill switch [%s] = red — skipping live-state report", KILL_SWITCH_FEATURE
        )
        return 1

    deploy = check_deploy_drift()
    cron_beats = check_cron_freshness()
    silent = check_silent_failures()

    _write_deploy_facts(deploy)
    _write_cron_facts(cron_beats)
    _write_silent_failure_facts(silent)

    one_number_rows = read_facts("revenue.mrr.new_yesterday", fresh_only=True, limit=1)
    one_number_fact_row = one_number_rows[0] if one_number_rows else None

    subject, body = render_live_state_report(
        deploy, cron_beats, silent, one_number_fact_row=one_number_fact_row,
    )

    from src.services.email import send_alert

    recipients = report_recipients()
    if not recipients:
        logger.info("[Vera] no REPORT_RECIPIENTS configured — report generated but not emailed")
    for addr in recipients:
        try:
            send_alert(subject, body, to=addr)
        except Exception as exc:
            logger.warning("[Vera] failed to send live-state report to %s: %s", addr, exc)

    stale_count = sum(1 for b in cron_beats if b.is_stale)
    logger.info(
        "[Vera] live-state report complete: drift=%s stale=%d/%d zero_ingest=%d unscheduled=%d",
        deploy["drift"], stale_count, len(cron_beats),
        len(silent["zero_ingest"]), len(silent["unscheduled"]),
    )
    return 0
