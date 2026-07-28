"""Tests for the Feedback Ritual fine-tuning export (closes the 4.3 loop)."""
import json
from datetime import datetime, timezone

from src.core.models import LifecycleTrainingOverride
from src.services.feedback_ritual_export import (
    build_export_example,
    run_feedback_ritual_export,
)


def _snapshot(**over):
    base = {
        "decision_id": "dec-1",
        "graph_name": "retention",
        "event_type": "retention_summary_due",
        "raw_input_text": "retention_summary_due tier=wallet",
        "generated_output_text": "Hi! Your wallet has 3 credits left.",
        "confidence_score": 0.42,
    }
    base.update(over)
    return base


class TestBuildExportExample:
    def test_approved_uses_lifecycle_output_as_label(self):
        row = {
            "subject_ref": "dec-1",
            "review_outcome": "approved",
            "correction_reason": None,
            "corrected_output": None,
            "snapshot_payload": _snapshot(),
        }
        ex = build_export_example(row)
        assert ex["input"] == "retention_summary_due tier=wallet"
        assert ex["output"] == "Hi! Your wallet has 3 credits left."
        assert ex["label_source"] == "approved_output"

    def test_needs_correction_uses_corrected_output_as_label(self):
        row = {
            "subject_ref": "dec-2",
            "review_outcome": "needs_correction",
            "correction_reason": "wrong_intent",
            "corrected_output": "The corrected, on-policy reply.",
            "snapshot_payload": _snapshot(),
        }
        ex = build_export_example(row)
        assert ex["output"] == "The corrected, on-policy reply."
        assert ex["label_source"] == "corrected_output"
        assert ex["correction_reason"] == "wrong_intent"

    def test_needs_correction_without_corrected_output_is_unbuildable(self):
        row = {
            "subject_ref": "dec-3",
            "review_outcome": "needs_correction",
            "correction_reason": "wrong_intent",
            "corrected_output": None,
            "snapshot_payload": _snapshot(),
        }
        assert build_export_example(row) is None

    def test_missing_input_text_is_unbuildable(self):
        row = {
            "subject_ref": "dec-4",
            "review_outcome": "approved",
            "corrected_output": None,
            "snapshot_payload": _snapshot(raw_input_text=None),
        }
        assert build_export_example(row) is None


def _mk_row(db, ref, *, outcome, queue_status="pending", corrected=None, snap=None):
    row = LifecycleTrainingOverride(
        source="feedback_ritual",
        subject_type="agent_decision",
        subject_ref=ref,
        correction_reason="wrong_intent" if outcome == "needs_correction" else None,
        corrected_output=corrected,
        review_outcome=outcome,
        queue_status=queue_status,
        dampener_active=False,
        created_by="tester",
        reviewed_by="admin@x" if outcome else None,
        reviewed_at=datetime.now(timezone.utc) if outcome else None,
        snapshot_payload=snap if snap is not None else _snapshot(decision_id=ref),
    )
    db.add(row)
    db.flush()
    return row


class TestRunExport:
    def test_reviewed_rows_exported_and_flipped(self, fresh_db, tmp_path):
        a = _mk_row(fresh_db, "exp-approved", outcome="approved")
        c = _mk_row(fresh_db, "exp-corrected", outcome="needs_correction",
                    corrected="fixed reply")
        fresh_db.flush()

        stats = run_feedback_ritual_export(fresh_db, output_dir=tmp_path, run_id="t1")

        assert stats["exported"] == 2
        # file written, two JSONL lines
        lines = (tmp_path / "t1.jsonl").read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
        outputs = {json.loads(l)["output"] for l in lines}
        assert "fixed reply" in outputs
        # both rows flipped to exported
        fresh_db.refresh(a)
        fresh_db.refresh(c)
        assert a.queue_status == "exported"
        assert c.queue_status == "exported"

    def test_discarded_and_unreviewed_are_not_exported(self, fresh_db, tmp_path):
        _mk_row(fresh_db, "exp-discarded", outcome="discarded", queue_status="discarded")
        _mk_row(fresh_db, "exp-unreviewed", outcome=None)  # flagged, not yet reviewed
        fresh_db.flush()

        stats = run_feedback_ritual_export(fresh_db, output_dir=tmp_path, run_id="t2")
        assert stats["exported"] == 0
        assert not (tmp_path / "t2.jsonl").exists()

    def test_unbuildable_row_left_pending(self, fresh_db, tmp_path):
        r = _mk_row(fresh_db, "exp-nofix", outcome="needs_correction", corrected=None)
        fresh_db.flush()

        stats = run_feedback_ritual_export(fresh_db, output_dir=tmp_path, run_id="t3")
        assert stats["exported"] == 0
        assert stats["skipped"] == 1
        fresh_db.refresh(r)
        assert r.queue_status == "pending"  # left for a human to complete

    def test_already_exported_not_repicked(self, fresh_db, tmp_path):
        _mk_row(fresh_db, "exp-done", outcome="approved", queue_status="exported")
        fresh_db.flush()
        stats = run_feedback_ritual_export(fresh_db, output_dir=tmp_path, run_id="t4")
        assert stats["exported"] == 0

    def test_dry_run_writes_nothing_and_flips_nothing(self, fresh_db, tmp_path):
        a = _mk_row(fresh_db, "exp-dry", outcome="approved")
        fresh_db.flush()
        stats = run_feedback_ritual_export(fresh_db, output_dir=tmp_path, run_id="t5", dry_run=True)
        assert stats["exported"] == 1
        assert stats["output_path"] is None
        assert not (tmp_path / "t5.jsonl").exists()
        fresh_db.refresh(a)
        assert a.queue_status == "pending"  # not flipped on dry run
