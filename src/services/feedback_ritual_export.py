"""Feedback Ritual fine-tuning export (closes the 4.3 loop).

After an admin reviews a flagged Cora interaction (services/feedback_ritual.py),
the row sits in cora_training_overrides with source='feedback_ritual',
queue_status='pending', and a review_outcome. This module turns reviewed,
useful rows into a fine-tuning dataset and marks them exported — the step the
spec calls "directly updates the agent's fine-tuning training dataset".

Exportable rows (review_outcome):
  - approved          → the label IS Cora's own output (reinforce good behaviour)
  - needs_correction  → the label is the admin's corrected_output
  - discarded         → never exported
A row missing the text needed to build a (input, output) pair is skipped and
left 'pending' so a human can complete it; it is never silently dropped.

Output: data/feedback_ritual_export/<run_id>.jsonl, one example per line.

Usage:
    python -m src.services.feedback_ritual_export
    python -m src.services.feedback_ritual_export --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

EXPORTABLE_OUTCOMES = ("approved", "needs_correction")
DEFAULT_OUTPUT_DIR = Path("data/feedback_ritual_export")

_SELECT_REVIEWED_SQL = text(
    """
    SELECT id, subject_ref, review_outcome, correction_reason,
           corrected_output, snapshot_payload
    FROM cora_training_overrides
    WHERE source = 'feedback_ritual'
      AND queue_status = 'pending'
      AND review_outcome IN ('approved', 'needs_correction')
    ORDER BY id
    """
)

_MARK_EXPORTED_SQL = text(
    "UPDATE cora_training_overrides SET queue_status = 'exported' "
    "WHERE id = ANY(:ids)"
)


def build_export_example(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Turn one reviewed queue row into a fine-tuning example, or None if the
    text needed for a (input, output) pair is missing."""
    snapshot = row.get("snapshot_payload") or {}
    input_text = snapshot.get("raw_input_text")
    if not input_text:
        return None

    outcome = row.get("review_outcome")
    if outcome == "approved":
        target = snapshot.get("generated_output_text")
        label_source = "approved_output"
    elif outcome == "needs_correction":
        target = row.get("corrected_output")
        label_source = "corrected_output"
    else:
        return None

    if not target:
        return None

    return {
        "decision_id": row.get("subject_ref"),
        "input": input_text,
        "output": target,
        "label_source": label_source,
        "review_outcome": outcome,
        "correction_reason": row.get("correction_reason"),
        "graph_name": snapshot.get("graph_name"),
        "event_type": snapshot.get("event_type"),
        "confidence_score": snapshot.get("confidence_score"),
    }


def write_jsonl(examples: list[dict[str, Any]], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for ex in examples:
            fh.write(json.dumps(ex, ensure_ascii=False) + "\n")
    return len(examples)


def run_feedback_ritual_export(
    session,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    run_id: Optional[str] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Build the dataset from reviewed rows and (unless dry_run) write it and
    flip those rows to 'exported'. Does NOT commit — the caller owns the commit
    so the file write and the status flip stay in one transaction."""
    rows = session.execute(_SELECT_REVIEWED_SQL).mappings().all()

    examples: list[dict[str, Any]] = []
    exported_ids: list[int] = []
    skipped = 0
    for r in rows:
        example = build_export_example(dict(r))
        if example is None:
            skipped += 1
            logger.warning(
                "feedback_ritual row id=%s reviewed but not exportable (missing text) — left pending",
                r["id"],
            )
            continue
        examples.append(example)
        exported_ids.append(r["id"])

    output_path = output_dir / f"{run_id or uuid.uuid4().hex[:12]}.jsonl"
    if examples and not dry_run:
        write_jsonl(examples, output_path)
        session.execute(_MARK_EXPORTED_SQL, {"ids": exported_ids})

    return {
        "reviewed_rows": len(rows),
        "exported": len(examples),
        "skipped": skipped,
        "output_path": str(output_path) if (examples and not dry_run) else None,
    }


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )
    parser = argparse.ArgumentParser(description="Export reviewed Feedback Ritual rows to a fine-tuning dataset.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Build counts only; write nothing, flip nothing.")
    args = parser.parse_args(argv)

    with get_db_context() as session:
        stats = run_feedback_ritual_export(
            session, output_dir=args.output_dir, run_id=args.run_id, dry_run=args.dry_run
        )
        if not args.dry_run:
            session.commit()

    logger.info(
        "Feedback Ritual export: reviewed=%d exported=%d skipped=%d path=%s",
        stats["reviewed_rows"], stats["exported"], stats["skipped"], stats["output_path"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
