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
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Optional

from sqlalchemy import text

from src.agents.vera.checks._shared import (
    html_kv_rows,
    html_headline,
    html_list,
    html_note,
    html_section,
    html_shell,
    html_table,
    html_warning,
    report_recipients,
    validate_finding,
)
from src.agents.vera.config import FRESHNESS_STATIC, KILL_SWITCH_FEATURE
from src.agents.vera.db import vera_db
from src.agents.vera.facts import read_facts, write_fact
from src.tasks.heartbeat_monitor import HEARTBEAT_SLAS, MULTI_COUNTY_SOURCES, SOURCE_OFF_DAYS

logger = logging.getLogger(__name__)


def format_outcome_label(outcome_category: Optional[str], error_type: Optional[str]) -> str:
    """One consistent, self-explanatory tag for a scraper_run_stats row's
    classification, used everywhere Vera's reports show a source's status
    (ZERO-INGEST SOURCES, CRON FRESHNESS's "last recorded outcome"). Visually
    distinguishes three states so a reader never has to guess which one
    they're looking at:
      - A migrated source's real, enforced outcome_category -> "[TIMEOUT]"
        etc. — one of the 5 real categories, confident and specific.
      - An un-migrated source's legacy error_type string -> "[legacy:
        scraper_error]" — explicitly labeled as the OLD, coarse,
        un-enforced convention (a catch-all for any failure, not a real
        category), so it's never mistaken for one of the 5 enforced values.
      - Neither present -> "[UNCLASSIFIED]" — stated outright rather than
        left blank, so absence of data reads as absence of data, not as an
        omission.
    Pure function — no I/O — importable from both live_state.py's own
    plain-text renderer and src.services.vera_slack's Block Kit renderer so
    the two can never drift into showing different labels for the same row."""
    if outcome_category:
        return f"[{outcome_category}]"
    if error_type:
        return f"[legacy: {error_type}]"
    return "[UNCLASSIFIED]"

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

    If `repo_dir` doesn't exist on this host (e.g. run outside the real prod
    server, where PROD_REPO_DIR is hardcoded to /root/Forced-action-), every
    git command would fail anyway — skipped up front so the report can say
    *why* the deploy fields are unknown instead of leaving that a mystery.
    """
    if not Path(repo_dir).is_dir():
        logger.warning(
            "[Vera] repo_dir %s not found on this host — skipping deploy-drift check "
            "(expected when Vera isn't running on the real prod server)",
            repo_dir,
        )
        return {
            "head_sha": None, "last_good_sha": None, "dev_head_sha": None,
            "drift": "unknown", "migration_statuses": {}, "pending_migrations": [],
            "repo_dir": repo_dir, "repo_dir_missing": True,
        }

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
        "repo_dir": repo_dir,
        "repo_dir_missing": False,
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
    # Populated only for stale beats (see check_cron_freshness) — the most
    # recent row for this source regardless of run_success, so a STALE line
    # can say *why* instead of just *that*. Added at the end with defaults
    # so every existing positional CronBeat(...) call site (tests included)
    # keeps working unchanged.
    last_attempt_run_date: Optional[date] = None
    last_attempt_outcome_category: Optional[str] = None
    last_attempt_error_type: Optional[str] = None
    last_attempt_error_message: Optional[str] = None

    def label(self) -> str:
        return f"{self.source_type}/{self.county_id}" if self.county_id else self.source_type

    def age_label(self) -> str:
        """Human-readable age — same min/h/d thresholds as
        heartbeat_monitor.Heartbeat.age_label(), reused rather than
        reinvented, so raw minutes (e.g. 'age=1833 min') never leak into a
        report a human has to read."""
        if self.age_minutes is None:
            return "never run"
        if self.age_minutes < 120:
            return f"{self.age_minutes} min"
        hours = self.age_minutes / 60.0
        if hours < 48:
            return f"{hours:.1f}h"
        return f"{hours / 24:.1f}d"

    def last_attempt_label(self, now: Optional[datetime] = None) -> Optional[str]:
        """"[CATEGORY] on YYYY-MM-DD (Nd ago — no runs recorded since)" for a
        stale beat, or None if no attempt row exists at all (a source that
        has genuinely never run). Distinguishes "this failed recently" from
        "this hasn't run in days and the last thing it ever said was fine"
        — collapsing those into one bare STALE line is exactly the
        confusion this method exists to remove."""
        if self.last_attempt_run_date is None:
            return None
        label = format_outcome_label(self.last_attempt_outcome_category, self.last_attempt_error_type)
        now = now or datetime.now(timezone.utc)
        days_since = (now.date() - self.last_attempt_run_date).days
        when = (
            "today" if days_since == 0
            else "yesterday" if days_since == 1
            else f"on {self.last_attempt_run_date.isoformat()} ({days_since}d ago — no runs recorded since)"
        )
        suffix = f" — {self.last_attempt_error_message[:160]}" if self.last_attempt_error_message else ""
        return f"{label} {when}{suffix}"


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


def _last_attempt_outcome(session, source_type: str, county_id: Optional[str]) -> Optional[Mapping]:
    """The single most recent row for this source regardless of
    run_success — only called for beats already determined stale (a small,
    bounded set), so this is one extra targeted query per stale source, not
    a change to the main freshness scan's cost."""
    query = (
        "SELECT run_date, outcome_category, error_type, error_message "
        "FROM scraper_run_stats WHERE source_type = :source_type"
    )
    params = {"source_type": source_type}
    if county_id is not None:
        query += " AND county_id = :county_id"
        params["county_id"] = county_id
    query += " ORDER BY run_date DESC, created_at DESC LIMIT 1"
    return session.execute(text(query), params).mappings().first()


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

                last_attempt = _last_attempt_outcome(session, source_type, county_id) if is_stale else None

                beats.append(CronBeat(
                    source_type=source_type, county_id=county_id, sla_minutes=sla_minutes,
                    last_success_at=last_success, age_minutes=age_minutes, is_stale=is_stale,
                    last_attempt_run_date=last_attempt["run_date"] if last_attempt else None,
                    last_attempt_outcome_category=last_attempt["outcome_category"] if last_attempt else None,
                    last_attempt_error_type=last_attempt["error_type"] if last_attempt else None,
                    last_attempt_error_message=last_attempt["error_message"] if last_attempt else None,
                ))

    return beats


def _off_day_pairs(now: Optional[datetime] = None) -> list[tuple[str, Optional[str]]]:
    """(source_type, county_id) pairs skipped today by SOURCE_OFF_DAYS — the
    same sources check_cron_freshness() skips over. Pure function, no I/O."""
    now = now or datetime.now(timezone.utc)
    today_wd = now.weekday()
    pairs: list[tuple[str, Optional[str]]] = []
    for source_type in HEARTBEAT_SLAS:
        if today_wd not in SOURCE_OFF_DAYS.get(source_type, set()):
            continue
        counties = MULTI_COUNTY_SOURCES.get(source_type)
        county_list = sorted(counties) if counties else [None]
        for county_id in county_list:
            pairs.append((source_type, county_id))
    return pairs


def _write_off_day_facts(pairs: list[tuple[str, Optional[str]]]) -> None:
    """Writes an explicit not_scheduled_today verdict for sources
    intentionally skipped today. Without this, a source's off-day leaves
    yesterday's 'stale' fact as the newest row forever — _read_stale_cron()
    (discrepancy_digest.py) reads the newest row per source regardless of
    age, so an unscheduled off-day would otherwise keep reporting a stale
    discrepancy that isn't actionable."""
    for source_type, county_id in pairs:
        write_fact(
            f"cron.{source_type}.freshness", "not_scheduled_today",
            county_id=county_id, source="scraper_run_stats",
            method="source on scheduled off-day (SOURCE_OFF_DAYS)",
            freshness_class=FRESHNESS_STATIC,
        )


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


def _is_confirmed_no_data(row: Mapping) -> bool:
    """Dual-read: outcome_category (enforced vocabulary) first, falling back
    to the unconstrained legacy error_type string only when outcome_category
    is NULL (source not yet migrated to the outcome-classification system).
    Pure function — no I/O."""
    outcome = row.get("outcome_category")
    if outcome is not None:
        return outcome == "NO_DATA"
    return row.get("error_type") in ("no_data", "rate_limited")


def check_silent_failures(
    run_date: Optional[date] = None,
    crontab_path: Optional[Path] = None,
) -> dict:
    run_date = run_date or datetime.now(timezone.utc).date()
    crontab_path = crontab_path or (Path(PROD_REPO_DIR) / "scripts" / "cron" / "crontab.txt")

    with vera_db.session_scope() as session:
        rows = session.execute(
            text(
                "SELECT source_type, county_id, total_scraped, error_type, outcome_category, error_message "
                "FROM scraper_run_stats "
                "WHERE run_date = :run_date AND run_success = true AND total_scraped = 0"
            ),
            {"run_date": run_date},
        ).mappings().all()
    zero_ingest = [dict(row) for row in rows]
    # Mirrors src/api/main.py:_classify_scraper_issues — a scraper that ran
    # fine and legitimately found nothing (error_type='no_data'/'rate_limited')
    # is not the same signal as one that reported success with zero rows and
    # no explanation at all.
    #
    # Dual-read: outcome_category is the enforced (CheckConstraint), single
    # source of truth for migrated sources — checked first. error_type is
    # unconstrained free text (two undocumented values already leaked into
    # prod before this system existed — see the plan's Context #3), so it's
    # only trusted as a fallback for sources that haven't migrated yet
    # (outcome_category IS NULL). A row with outcome_category=NO_DATA always
    # gets error_type='no_data' too (record_scraper_stats' derivation), so
    # this dual-read doesn't change today's behavior for migrated sources —
    # it's forward cover against error_type drifting out of sync.
    zero_ingest_confirmed_no_data = [row for row in zero_ingest if _is_confirmed_no_data(row)]
    zero_ingest_unexplained = [row for row in zero_ingest if not _is_confirmed_no_data(row)]

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

    return {
        "zero_ingest": zero_ingest,
        "zero_ingest_confirmed_no_data": zero_ingest_confirmed_no_data,
        "zero_ingest_unexplained": zero_ingest_unexplained,
        "unscheduled": unscheduled,
    }


_CRASHED_MIN_AGE_MINUTES = 120


def check_crashed_before_completion(
    run_date: Optional[date] = None,
    min_age_minutes: int = _CRASHED_MIN_AGE_MINUTES,
    now: Optional[datetime] = None,
) -> list[dict]:
    """Sources with a heartbeat (attempt_started_at) stamped today but no
    completion write (completed_at still NULL) — the process started and
    never reached record_scraper_stats() (hard crash, OOM kill, killed
    process, etc.), as distinct from "genuinely never ran" (no row at all,
    attempt_started_at never set) and from a normal completion of any kind
    (success/no-data/failure — completed_at is always set on that path).

    Requires attempt_started_at to be at least min_age_minutes old (default
    120) before flagging — without this, a source still legitimately mid-run
    when this check happens to fire (a slow Playwright/nodriver source, a
    manual trigger, a run caught mid-stagger-window) would be reported as
    "crashed" just for still being in progress. That's the exact false-alarm
    failure mode this whole classification system exists to close, so a
    genuinely-running source must never trip it.

    Only meaningful for sources migrated onto
    src.utils.scraper_run_tracking.scraper_run(), which is the only thing
    that stamps attempt_started_at via mark_scraper_attempt_started(). An
    un-migrated source can never appear here — that's simply not yet
    observable for it, not a false negative."""
    run_date = run_date or datetime.now(timezone.utc).date()
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=min_age_minutes)
    with vera_db.session_scope() as session:
        rows = session.execute(
            text(
                "SELECT source_type, county_id, attempt_started_at FROM scraper_run_stats "
                "WHERE run_date = :run_date "
                "AND attempt_started_at IS NOT NULL AND completed_at IS NULL"
            ),
            {"run_date": run_date},
        ).mappings().all()
    # Age-filter in Python, not SQL: attempt_started_at is a naive DateTime
    # column (stores UTC via func.now(), same convention as last_success in
    # check_cron_freshness() above) — comparing it directly against a
    # timezone-aware bind parameter risks a driver-level mismatch, so this
    # mirrors check_cron_freshness()'s existing naive->aware conversion
    # instead of pushing the comparison into SQL.
    crashed = []
    for row in rows:
        started_at = row["attempt_started_at"]
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        if started_at < cutoff:
            crashed.append(dict(row))
    return crashed


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


def _write_crashed_facts(crashed: list[dict]) -> None:
    for row in crashed:
        write_fact(
            f"crashed.{row['source_type']}", "true",
            county_id=row["county_id"], source="scraper_run_stats",
            method="attempt_started_at IS NOT NULL AND completed_at IS NULL for today",
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
    crashed: Optional[list[dict]] = None,
) -> tuple[str, str, str]:
    """Returns (subject, body, html_body). Numbers first, Vera's voice.
    Pure — no DB access; `one_number_fact_row` is pre-fetched by the caller
    (run_live_state()) so this function stays testable without a live
    connection. Defaults to None (renders the V3-pending placeholder), which
    is exactly what every existing caller/test that doesn't pass it gets.
    `crashed` (check_crashed_before_completion()'s output) also defaults to
    None -> treated as empty, so every existing caller/test that predates it
    is unaffected.
    Plain text and HTML are built together from the same data so they can't
    silently drift apart from each other over time."""
    report_date = report_date or datetime.now(timezone.utc).date()
    crashed = crashed or []
    stale = [b for b in cron_beats if b.is_stale]
    fresh_count = len(cron_beats) - len(stale)
    one_number_line = _the_one_number_line(one_number_fact_row)

    # ── DEPLOY ────────────────────────────────────────────────────────────
    lines = [
        f"Vera — Live-State Report — {report_date.isoformat()}",
        "=" * 60,
        "",
        one_number_line,
        "",
        "DEPLOY",
    ]
    if deploy.get("repo_dir_missing"):
        deploy_note = (
            f"Repo path {deploy.get('repo_dir', '(unknown)')} not found on this host — "
            "deploy drift cannot be checked from here (expected when Vera isn't running "
            "on the real prod server; will show real values once cron runs there)."
        )
        lines.append(f"  {deploy_note}")
        deploy_html = html_warning(deploy_note)
    else:
        lines += [
            f"  Prod HEAD:        {_short(deploy['head_sha'])}",
            f"  Last good deploy: {_short(deploy['last_good_sha'])}",
            f"  Dev HEAD:         {_short(deploy['dev_head_sha'])}",
            f"  Drift:            {deploy['drift']}",
        ]
        deploy_kv = [
            ("Prod HEAD", _short(deploy["head_sha"])),
            ("Last good deploy", _short(deploy["last_good_sha"])),
            ("Dev HEAD", _short(deploy["dev_head_sha"])),
            ("Drift", deploy["drift"]),
        ]

        if deploy["pending_migrations"]:
            lines.append(f"  Pending migrations ({len(deploy['pending_migrations'])}):")
            for name in deploy["pending_migrations"]:
                lines.append(f"    - {name}: not_applied")
            pending_html = html_list(f"{n}: not_applied" for n in deploy["pending_migrations"])
        else:
            lines.append("  Pending migrations: none")
            pending_html = html_note("Pending migrations: none")

        unrecognized = sorted(
            name for name, status in deploy["migration_statuses"].items()
            if status == "unrecognized"
        )
        unrecognized_html = ""
        if unrecognized:
            lines.append(
                "  Unrecognized DDL shape (cannot verify applied/not-applied): "
                + ", ".join(unrecognized)
            )
            unrecognized_html = html_note(
                "Unrecognized DDL shape (cannot verify applied/not-applied): "
                + ", ".join(unrecognized)
            )

        migration_note = (
            "Migration status is DB-verified via information_schema introspection of "
            "CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS targets — not a ledger "
            "lookup (ADR 0024 keeps none). Files using other DDL shapes report "
            "'unrecognized', never silently assumed applied."
        )
        lines.append(f"  Note: {migration_note}")
        deploy_html = (
            html_kv_rows(deploy_kv) + pending_html + unrecognized_html + html_note(migration_note)
        )

    # ── CRON FRESHNESS ────────────────────────────────────────────────────
    lines += ["", "CRON FRESHNESS"]
    cron_rows_html = []
    if stale:
        for beat in sorted(stale, key=lambda b: b.label()):
            age = beat.age_label()
            last_attempt = beat.last_attempt_label()
            lines.append(
                f"  STALE  {beat.label():<30} age={age:<10} sla={beat.sla_minutes / 60:.0f}h"
                + (f"   last recorded outcome: {last_attempt}" if last_attempt else "   last recorded outcome: none — genuinely never run")
            )
            cron_rows_html.append((
                beat.label(), age, f"{beat.sla_minutes / 60:.0f}h",
                last_attempt or "none — genuinely never run",
            ))
    summary_line = f"{fresh_count}/{len(cron_beats)} sources fresh"
    lines.append(f"  {summary_line}")
    sla_legend = "SLA = max time allowed since last successful run before a source is flagged stale."
    lines.append(f"  ({sla_legend})")

    cron_html = ""
    if cron_rows_html:
        cron_html += html_table(["Source", "Age", "SLA", "Last recorded outcome"], cron_rows_html)
    cron_html += html_note(summary_line) + html_note(sla_legend)

    # ── ZERO-INGEST SOURCES ────────────────────────────────────────────────
    lines += ["", "ZERO-INGEST SOURCES"]
    confirmed_no_data = silent["zero_ingest_confirmed_no_data"]
    unexplained = silent["zero_ingest_unexplained"]

    def _silent_entry(row: Mapping) -> str:
        label = format_outcome_label(row.get("outcome_category"), row.get("error_type"))
        msg = row.get("error_message")
        base = f"{row['source_type']}/{row['county_id']}  {label}"
        return f"{base} — {msg[:160]}" if msg else base

    if confirmed_no_data:
        lines.append("  ℹ️ No new data today (confirmed — nothing to report):")
        for row in confirmed_no_data:
            lines.append(f"    - {_silent_entry(row)}")
        confirmed_no_data_html = html_list(_silent_entry(row) for row in confirmed_no_data)
    else:
        lines.append("  ℹ️ No new data today (confirmed): none")
        confirmed_no_data_html = html_note("No new data today (confirmed): none")

    if unexplained:
        lines.append("  \U0001f527 Scheduled-but-writing-nothing — needs investigation:")
        for row in unexplained:
            lines.append(f"    - {_silent_entry(row)}")
        zero_ingest_html = html_list(_silent_entry(row) for row in unexplained)
    else:
        lines.append("  \U0001f527 Scheduled-but-writing-nothing: none")
        zero_ingest_html = html_note("Scheduled-but-writing-nothing: none")

    if silent["unscheduled"]:
        lines.append("  Enabled-but-unscheduled:")
        for source_type in silent["unscheduled"]:
            lines.append(f"    - {source_type}")
        unscheduled_html = html_list(silent["unscheduled"])
    else:
        lines.append("  Enabled-but-unscheduled: none")
        unscheduled_html = html_note("Enabled-but-unscheduled: none")

    # ── CRASHED MID-RUN ────────────────────────────────────────────────────
    # Only meaningful for scraper_run()-wrapped sources — un-migrated sources
    # never stamp attempt_started_at, so they can't appear here (not a false
    # negative, just not yet observable for them). No category shown here —
    # a crashed row has no completion write by definition, so there is
    # nothing classified to show; "started N ago, still nothing" already
    # says exactly what's known.
    def _crashed_entry(row: Mapping) -> str:
        started = row.get("attempt_started_at")
        if started is None:
            return f"{row['source_type']}/{row['county_id']}"
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        stuck_for = datetime.now(timezone.utc) - started
        hours = stuck_for.total_seconds() / 3600
        duration = f"{hours:.1f}h" if hours < 48 else f"{hours / 24:.1f}d"
        return f"{row['source_type']}/{row['county_id']} (started {duration} ago, still no completion)"

    lines += ["", "CRASHED MID-RUN"]
    if crashed:
        lines.append("  Started but never completed (heartbeat set, no completion write):")
        for row in crashed:
            lines.append(f"    - {_crashed_entry(row)}")
        crashed_html = html_list(_crashed_entry(row) for row in crashed)
    else:
        lines.append("  Started but never completed: none")
        crashed_html = html_note("Started but never completed: none")

    lines += ["", "— Vera."]
    body = "\n".join(lines)

    subject = (
        f"[Vera] Live-State Report {report_date.isoformat()} — "
        f"drift={deploy['drift']}, {len(stale)} stale, {len(unexplained)} zero-ingest"
        + (f", {len(crashed)} crashed" if crashed else "")
    )

    html_body = html_shell(
        title="Vera — Live-State Report",
        subtitle=report_date.isoformat(),
        body_html=(
            html_headline("THE ONE NUMBER", one_number_line.split(": ", 1)[-1])
            + html_section("Deploy", deploy_html)
            + html_section("Cron Freshness", cron_html)
            + html_section(
                "Zero-Ingest Sources",
                "<p style=\"color:#94a3b8;font-size:12px;margin:4px 0;\">No new data today (confirmed — nothing to report):</p>"
                + confirmed_no_data_html
                + "<p style=\"color:#94a3b8;font-size:12px;margin:8px 0 4px;\">Scheduled-but-writing-nothing — needs investigation:</p>"
                + zero_ingest_html
                + "<p style=\"color:#94a3b8;font-size:12px;margin:8px 0 4px;\">Enabled-but-unscheduled:</p>"
                + unscheduled_html
            )
            + html_section("Crashed Mid-Run", crashed_html)
        ),
    )
    return subject, body, html_body


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
    crashed = check_crashed_before_completion()

    _write_deploy_facts(deploy)
    _write_cron_facts(cron_beats)
    _write_off_day_facts(_off_day_pairs())
    _write_silent_failure_facts(silent)
    _write_crashed_facts(crashed)

    one_number_rows = read_facts("revenue.mrr.new_yesterday", fresh_only=True, limit=1)
    one_number_fact_row = one_number_rows[0] if one_number_rows else None

    subject, body, html_body = render_live_state_report(
        deploy, cron_beats, silent, one_number_fact_row=one_number_fact_row, crashed=crashed,
    )

    from src.services.email import send_alert
    from src.services.vera_slack import post_vera_report

    recipients = report_recipients()
    if not recipients:
        logger.info("[Vera] no REPORT_RECIPIENTS configured — report generated but not emailed")
    for addr in recipients:
        try:
            send_alert(subject, body, html_body=html_body, to=addr)
        except Exception as exc:
            logger.warning("[Vera] failed to send live-state report to %s: %s", addr, exc)

    from src.services.vera_slack import build_live_state_blocks
    _report_date = datetime.now(timezone.utc).date()
    slack_blocks = build_live_state_blocks(
        subject, deploy, cron_beats, silent,
        one_number_line=_the_one_number_line(one_number_fact_row),
        report_date=str(_report_date),
        crashed=crashed,
    )
    post_vera_report(subject, body, blocks=slack_blocks)

    stale_count = sum(1 for b in cron_beats if b.is_stale)

    # Validate actionable findings via Vera→Dev contract (spec §1.1.10).
    # Only assembles a finding when something requires dev attention — clean
    # reports (in_sync, no stale, no zero-ingest, no crashed) skip this
    # entirely. A crashed-mid-run source is unambiguously actionable — it's
    # not "no data," it's a process that started and never finished.
    is_actionable = (
        deploy["drift"] not in ("in_sync", "unknown")
        or bool(deploy["pending_migrations"])
        or stale_count > 0
        or bool(silent["zero_ingest_unexplained"])
        or bool(crashed)
    )
    if is_actionable:
        validate_finding(
            {
                "issue": subject,
                "evidence": body[:600],
                "repro": "python -m src.agents.vera --live-state",
                "suspected_cause": (
                    "Cron job failure, scraper crash, or pending deployment"
                ),
                "proposed_fix": (
                    f"Apply pending migrations: {deploy['pending_migrations'] or 'none'}; "
                    f"investigate {stale_count} stale source(s); "
                    f"investigate {len(crashed)} crashed-mid-run source(s); verify deploy status"
                ),
                "effort": "low",
                "risk": "high" if (deploy["pending_migrations"] or crashed) else "medium",
            },
            source="live_state",
        )

    logger.info(
        "[Vera] live-state report complete: drift=%s stale=%d/%d zero_ingest=%d unscheduled=%d crashed=%d",
        deploy["drift"], stale_count, len(cron_beats),
        len(silent["zero_ingest"]), len(silent["unscheduled"]), len(crashed),
    )
    return 0
