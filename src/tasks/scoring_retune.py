"""
CDS scoring retune orchestrator (client Q9 — the closed loop, end to end).

Runs the five-stage pipeline in order under one shared run id:

    B  build training CSV        src.services.scoring_training_data
    C  fit per-vertical weights  src.services.scoring_fit
    D  shadow-rescore w/ fit     src.services.cds_engine --shadow --fit-artifact
    E  validate shadow vs live   src.tasks.scoring_validation_report
    F  cutover (gated)           src.tasks.scoring_cutover

B, C and D are infrastructure stages — if any exits non-zero the run aborts
(no point fitting on a missing CSV, or validating an empty shadow table).
Stage E's exit code is a *verdict*, not a failure: PASS/WARN/FAIL all proceed
to Stage F, which independently enforces the gate and records the decision.
This keeps a WARN/FAIL from silently skipping the audit-log row + alert.

Each stage runs as its own subprocess (matches how these run standalone and
isolates the sklearn import to Stage C). This is the single cron entry that
makes the loop automatic.

Usage:
    PYTHONPATH=. python -m src.tasks.scoring_retune
    PYTHONPATH=. python -m src.tasks.scoring_retune --window-days 90 --since 2025-01-01

Exit codes:
    0 — pipeline ran; Stage F promoted the fit
    1 — pipeline ran; Stage F blocked promotion (not PASS / thin data)
    2 — an infrastructure stage (B/C/D) failed, Stage F hit bad input
        (missing/malformed artifact or report), or another run already
        holds the pipeline lock — nothing promoted
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

TRAINING_DIR = Path("data/scoring_training")
FIT_DIR = Path("data/scoring_fit")
VALIDATION_DIR = Path("data/validation")

ARTIFACT_RETENTION_DAYS = 180  # ~26 weekly runs of audit history on disk
_LOCK_KEY = "scoring_retune"


def _run(module: str, args: list[str]) -> int:
    """Run `python -m <module> <args>` as a subprocess; return its exit code."""
    cmd = [sys.executable, "-m", module, *args]
    logger.info("[retune] -> %s", " ".join(cmd))
    return subprocess.run(cmd, check=False).returncode


def _clear_shadow_table() -> None:
    """Empty distress_scores_shadow before the shadow rescore.

    Stage E reads the entire shadow table with no date filter, and the engine
    writes one row per property per calendar day — so without this, a weekly
    cadence accumulates multiple snapshots and the validation gate mixes runs
    (tier counts double, the event-rate CTE takes the oldest score_date). This
    table is disposable scratch (fa032), read only by Stage E, so truncating it
    to a single clean snapshot per cycle is safe.
    """
    from sqlalchemy import text

    from src.core.database import get_db_context

    with get_db_context() as session:
        session.execute(text("TRUNCATE TABLE distress_scores_shadow"))
        session.commit()
    logger.info("[retune] cleared distress_scores_shadow")


def _prune_old_artifacts() -> None:
    """Best-effort delete of scoring artifacts older than ARTIFACT_RETENTION_DAYS.

    Never raises — a pruning failure must not block the retune run itself.
    """
    cutoff = time.time() - ARTIFACT_RETENTION_DAYS * 86400
    for d in (TRAINING_DIR, FIT_DIR, VALIDATION_DIR):
        if not d.is_dir():
            continue
        for f in d.iterdir():
            try:
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
            except OSError as exc:
                logger.warning("[retune] could not prune %s: %s", f, exc)


def run_pipeline(*, window_days: int, since: Optional[str], county: Optional[str]) -> int:
    from sqlalchemy import text

    from src.core.database import get_db_context

    with get_db_context() as lock_session:
        got_lock = lock_session.execute(
            text("SELECT pg_try_advisory_lock(hashtext(:k))"), {"k": _LOCK_KEY}
        ).scalar()
        if not got_lock:
            logger.error("[retune] another scoring_retune run already holds the lock — aborting")
            return 2
        try:
            return _run_pipeline_locked(window_days=window_days, since=since, county=county)
        finally:
            lock_session.execute(
                text("SELECT pg_advisory_unlock(hashtext(:k))"), {"k": _LOCK_KEY}
            )


def _run_pipeline_locked(*, window_days: int, since: Optional[str], county: Optional[str]) -> int:
    try:
        _prune_old_artifacts()
    except Exception as exc:  # noqa: BLE001 — pruning must never block the retune run
        logger.warning("[retune] artifact pruning failed: %s", exc)

    run_id = uuid.uuid4().hex[:12]
    training_csv = TRAINING_DIR / f"{run_id}.csv"
    fit_artifact = FIT_DIR / f"{run_id}.json"
    report_json = VALIDATION_DIR / f"{run_id}.json"
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("[retune] run_id=%s window_days=%d", run_id, window_days)

    # ── Stage B — training data ───────────────────────────────────────────
    b_args = ["--run-id", run_id]
    if since:
        b_args += ["--since", since]
    if county:
        b_args += ["--county", county]
    b_args += ["--outcome-window-days", str(window_days)]
    if _run("src.services.scoring_training_data", b_args) != 0:
        logger.error("[retune] Stage B failed — aborting")
        return 2
    if not training_csv.is_file():
        logger.error("[retune] Stage B produced no CSV at %s — aborting", training_csv)
        return 2

    # ── Stage C — fit ─────────────────────────────────────────────────────
    if _run("src.services.scoring_fit",
            ["--training-csv", str(training_csv), "--run-id", run_id]) != 0:
        logger.error("[retune] Stage C failed — aborting")
        return 2
    if not fit_artifact.is_file():
        logger.error("[retune] Stage C produced no artifact at %s — aborting", fit_artifact)
        return 2

    # ── Stage D — shadow rescore with the proposed weights ────────────────
    try:
        _clear_shadow_table()
    except Exception as exc:  # noqa: BLE001 — stale shadow data would skew Stage E
        logger.error("[retune] could not clear shadow table (%s) — aborting", exc)
        return 2
    if _run("src.services.cds_engine",
            ["--shadow", "--fit-artifact", str(fit_artifact), "--no-ghl"]) != 0:
        logger.error("[retune] Stage D shadow rescore failed — aborting")
        return 2

    # ── Stage E — validation (verdict, not a gate; always proceed) ────────
    e_code = _run("src.tasks.scoring_validation_report",
                  ["--window-days", str(window_days), "--json", str(report_json)])
    logger.info("[retune] Stage E exit=%d (0=PASS 1=WARN 2=FAIL)", e_code)

    # ── Stage F — gated cutover (records + alerts on block) ───────────────
    f_code = _run("src.tasks.scoring_cutover",
                  ["--fit-artifact", str(fit_artifact), "--report", str(report_json),
                   "--run-id", run_id])
    if f_code == 0:
        logger.info("[retune] Stage F promoted the fit — live scoring will pick it up next run")
        return 0
    if f_code == 2:
        logger.error("[retune] Stage F failed on bad input (exit=2) — check report/artifact")
        return 2
    logger.warning("[retune] Stage F blocked promotion (exit=%d)", f_code)
    return 1


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the full CDS scoring retune loop (B->C->D->E->F).")
    p.add_argument("--window-days", type=int, default=90,
                   help="Outcome window in days for training + validation (default 90).")
    p.add_argument("--since", default=None,
                   help="ISO date floor for Stage B score_date (default: builder default).")
    p.add_argument("--county", default=None,
                   help="Restrict Stage B to a single county. Omit for all counties.")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = _parse_args(argv)
    return run_pipeline(window_days=args.window_days, since=args.since, county=args.county)


if __name__ == "__main__":
    raise SystemExit(main())
