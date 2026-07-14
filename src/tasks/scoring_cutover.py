"""
CDS scoring cutover (Stage F of the cross-county retune — closes client Q9).

Promotes a Stage C fit artifact to *live* scoring, but only when the Stage E
validation report PASSed and the fit has enough data per vertical to trust.
This is the one link that makes the loop automatic: once promoted, the daily
(non-shadow) scoring engine overlays the approved weights onto
config/scoring.py on its next run — no redeploy, no manual config edit.

Safety model (defense in depth — safe even if invoked by hand):
  1. Stage E overall_status MUST be PASS. WARN/FAIL → record a blocked attempt,
     alert, exit non-zero. Never promote on thin/insufficient shadow data.
  2. Per-vertical data bar. A vertical whose Stage C proposal carries a
     coverage_warning (too few events) or fit to all-zero weights is DROPPED
     from the approved artifact — the live engine keeps its config/scoring.py
     baseline for that vertical rather than zeroing it out. Only verticals that
     actually cleared the data bar override the live weights.
  3. If no vertical clears the bar, nothing is promoted (blocked attempt logged).

Every attempt — promoted or blocked — writes one scoring_cutover_log row so the
decision trail is auditable and the active artifact is reversible.

Usage:
    # promote a fit artifact, reading a Stage E report JSON for the verdict
    python -m src.tasks.scoring_cutover --fit-artifact data/scoring_fit/<id>.json \
        --report data/validation/<id>.json

    # no report given → re-run Stage E inline before deciding
    python -m src.tasks.scoring_cutover --fit-artifact data/scoring_fit/<id>.json

Exit codes:
    0 — promoted (or nothing to promote but no error)
    1 — blocked (validation not PASS, or no vertical cleared the data bar)
    2 — bad input (missing/malformed artifact or report)
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import ScoringCutoverLog

logger = logging.getLogger(__name__)

STATUS_PASS = "PASS"


# ── Active-weights pointer (read by cds_engine on live runs) ──────────────────

def active_fit_artifact_path(session) -> Optional[str]:
    """Path of the most recently approved (applied=true) fit artifact, or None.

    The live scoring engine calls this at startup; None means "use the
    config/scoring.py baseline unchanged".
    """
    row = session.execute(
        text(
            "SELECT fit_artifact_path FROM scoring_cutover_log "
            "WHERE applied = true ORDER BY created_at DESC, id DESC LIMIT 1"
        )
    ).first()
    return row.fit_artifact_path if row else None


# ── Gate helpers ──────────────────────────────────────────────────────────────

def _load_json(path: Path, what: str) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"{what} not found: {path}")
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"{what} is not valid JSON ({path}): {exc}") from exc


def _usable_proposals(artifact: dict) -> tuple[list[dict], list[str]]:
    """Split artifact proposals into (usable, dropped_verticals).

    A proposal is usable when it has no coverage_warning AND at least one
    non-zero signal weight. Everything else is dropped so the live engine keeps
    its baseline for that vertical.
    """
    usable: list[dict] = []
    dropped: list[str] = []
    for p in artifact.get("proposals", []):
        vertical = p.get("vertical", "<unknown>")
        weights = p.get("vertical_weights") or {}
        has_signal = any(float(w) > 0 for w in weights.values())
        if p.get("coverage_warning") or not has_signal:
            dropped.append(vertical)
        else:
            usable.append(p)
    return usable, dropped


def _alert(subject: str, body: str) -> None:
    """Best-effort ops alert. Never raises — cutover must not fail on alerting."""
    try:
        from src.services.email import send_alert
        send_alert(subject=subject, body=body)
    except Exception as exc:  # noqa: BLE001 — alerting is best-effort
        logger.error("[cutover] alert send failed: %s", exc)


# ── Promotion ─────────────────────────────────────────────────────────────────

def promote(artifact_path: Path, report: dict, *, run_id: Optional[str] = None) -> int:
    """Decide + record a cutover. Returns a process exit code (0/1)."""
    artifact = _load_json(artifact_path, "fit artifact")
    overall = report.get("overall_status")

    if overall != STATUS_PASS:
        detail = f"validation status {overall!r} != PASS — not promoting"
        logger.warning("[cutover] %s", detail)
        _record(artifact_path, overall or "UNKNOWN", applied=False,
                 snapshot=None, detail=detail, run_id=run_id)
        _alert("CDS cutover blocked", f"{detail}\nartifact={artifact_path}")
        return 1

    usable, dropped = _usable_proposals(artifact)
    if not usable:
        detail = ("no vertical cleared the data bar (all proposals had "
                  "coverage warnings or zero weights) — not promoting")
        logger.warning("[cutover] %s", detail)
        _record(artifact_path, overall, applied=False, snapshot=None, detail=detail,
                 run_id=run_id)
        _alert("CDS cutover blocked", f"{detail}\nartifact={artifact_path}")
        return 1

    # Write a sanitized artifact containing only the usable proposals. This is
    # what the live engine loads — dropped verticals keep their config baseline.
    approved = dict(artifact)
    approved["proposals"] = usable
    approved["approved_from"] = str(artifact_path)
    approved["dropped_verticals"] = dropped
    approved_path = artifact_path.with_suffix(".approved.json")
    approved_path.write_text(json.dumps(approved, indent=2, default=str))

    detail = (
        f"promoted {len(usable)} vertical(s): "
        f"{[p['vertical'] for p in usable]}"
        + (f"; kept baseline for {dropped}" if dropped else "")
    )
    logger.info("[cutover] %s", detail)
    _record(approved_path, overall, applied=True,
            snapshot={"proposals": usable}, detail=detail, run_id=run_id)
    return 0


def _record(artifact_path: Path, status: str, *, applied: bool,
            snapshot: Optional[dict], detail: str, run_id: Optional[str] = None) -> None:
    with get_db_context() as session:
        session.add(ScoringCutoverLog(
            fit_artifact_path=str(artifact_path),
            validation_status=status,
            applied=applied,
            weights_snapshot=snapshot,
            detail=detail,
            run_id=run_id,
        ))
        session.commit()


# ── CLI ──────────────────────────────────────────────────────────────────────

def _resolve_report(args) -> dict:
    if args.report:
        return _load_json(Path(args.report), "validation report")
    # No report supplied — run Stage E inline so the gate still has a verdict.
    from src.tasks.scoring_validation_report import run_report
    logger.info("[cutover] no --report given; running Stage E inline")
    return run_report(args.window_days).to_dict()


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage F cutover — promote a validated Stage C fit to live scoring."
    )
    p.add_argument("--fit-artifact", type=Path, required=True,
                   help="Stage C artifact to promote (data/scoring_fit/<id>.json).")
    p.add_argument("--report", type=Path, default=None,
                   help="Stage E validation report JSON. Omit to re-run Stage E inline.")
    p.add_argument("--window-days", type=int, default=90,
                   help="Outcome window for the inline Stage E run (default 90).")
    p.add_argument("--run-id", default=None,
                   help="Orchestrator run id (scoring_retune.py) to stamp on the audit row.")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = _parse_args(argv)
    try:
        report = _resolve_report(args)
        return promote(args.fit_artifact, report, run_id=args.run_id)
    except FileNotFoundError as exc:
        logger.error("[cutover] %s", exc)
        return 2
    except ValueError as exc:
        logger.error("[cutover] %s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
