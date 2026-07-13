"""Stage F cutover gate — the decision logic that makes the retune loop safe.

Covers: PASS+usable promotes and sanitizes; WARN/FAIL blocks; all-thin blocks;
the sanitizer drops coverage-warned / zero-weight verticals so a live cutover
can never zero out a vertical's weights.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.tasks import scoring_cutover as sc


def _artifact(proposals: list[dict]) -> dict:
    return {"schema_version": 1, "proposals": proposals}


def _write(tmp_path: Path, artifact: dict) -> Path:
    p = tmp_path / "fit.json"
    p.write_text(json.dumps(artifact))
    return p


@pytest.fixture(autouse=True)
def _stub_side_effects(monkeypatch):
    """Capture DB writes + alerts instead of performing them."""
    records: list[dict] = []
    alerts: list[tuple[str, str]] = []

    def fake_record(path, status, *, applied, snapshot, detail):
        records.append({"path": str(path), "status": status,
                        "applied": applied, "snapshot": snapshot, "detail": detail})

    monkeypatch.setattr(sc, "_record", fake_record)
    monkeypatch.setattr(sc, "_alert", lambda subject, body: alerts.append((subject, body)))
    return {"records": records, "alerts": alerts}


def test_usable_proposals_drops_thin_and_zero():
    art = _artifact([
        {"vertical": "good", "vertical_weights": {"foreclosures": 40, "probate": 0}},
        {"vertical": "thin", "vertical_weights": {"foreclosures": 30},
         "coverage_warning": "only 5 events"},
        {"vertical": "zero", "vertical_weights": {"foreclosures": 0, "probate": 0}},
    ])
    usable, dropped = sc._usable_proposals(art)
    assert [p["vertical"] for p in usable] == ["good"]
    assert set(dropped) == {"thin", "zero"}


def test_promote_blocks_when_not_pass(tmp_path, _stub_side_effects):
    art = _write(tmp_path, _artifact(
        [{"vertical": "good", "vertical_weights": {"foreclosures": 40}}]))
    code = sc.promote(art, {"overall_status": "FAIL"})
    assert code == 1
    rec = _stub_side_effects["records"][-1]
    assert rec["applied"] is False and rec["status"] == "FAIL"
    assert _stub_side_effects["alerts"]  # alerted on block


def test_promote_blocks_when_all_thin(tmp_path, _stub_side_effects):
    art = _write(tmp_path, _artifact([
        {"vertical": "thin", "vertical_weights": {"foreclosures": 30},
         "coverage_warning": "too few"},
        {"vertical": "zero", "vertical_weights": {"foreclosures": 0}},
    ]))
    code = sc.promote(art, {"overall_status": "PASS"})
    assert code == 1
    assert _stub_side_effects["records"][-1]["applied"] is False


def test_promote_pass_writes_sanitized_artifact(tmp_path, _stub_side_effects):
    art = _write(tmp_path, _artifact([
        {"vertical": "good", "vertical_weights": {"foreclosures": 40, "probate": 20}},
        {"vertical": "thin", "vertical_weights": {"foreclosures": 30},
         "coverage_warning": "too few"},
    ]))
    code = sc.promote(art, {"overall_status": "PASS"})
    assert code == 0

    rec = _stub_side_effects["records"][-1]
    assert rec["applied"] is True and rec["status"] == "PASS"

    approved_path = Path(rec["path"])
    assert approved_path.name.endswith(".approved.json")
    approved = json.loads(approved_path.read_text())
    # only the well-fit vertical survives — 'thin' must not zero live weights
    assert [p["vertical"] for p in approved["proposals"]] == ["good"]
    assert approved["dropped_verticals"] == ["thin"]


def test_engine_active_apply_survives_corrupt_artifact(tmp_path, monkeypatch):
    """A corrupt approved artifact must fall back to baseline, not abort scoring
    (_apply_fit_artifact sys.exit(2)s on bad JSON — the live path must catch it)."""
    import contextlib
    import logging

    import src.core.database as db
    import src.tasks.scoring_cutover as sc2
    from src.services import cds_engine

    bad = tmp_path / "bad.approved.json"
    bad.write_text("{ not valid json")

    @contextlib.contextmanager
    def fake_ctx():
        yield None

    monkeypatch.setattr(db, "get_db_context", fake_ctx)
    monkeypatch.setattr(sc2, "active_fit_artifact_path", lambda s: str(bad))

    # Must return cleanly (baseline weights), NOT raise SystemExit.
    cds_engine._apply_active_fit_artifact(logging.getLogger("test"))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
