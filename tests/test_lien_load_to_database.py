"""
src.scrappers.liens.lien_engine._load_to_database() — the per-target
(liens/deeds/judgments) DB-load loop called from run_lien_pipeline()'s
--load-to-db path.

Regression for a Critical finding in PR review: this function used to
swallow every target's exception (`except Exception as e: logger.error(...)`,
no re-raise, no return value) and the caller unconditionally called
run.suppress_completion_write() right after it — so a liens DB-load crash
(bad CSV, DB constraint violation, etc.) produced ZERO rows in
scraper_run_stats (DATA_TYPE_TO_SOURCE deliberately has no 'liens' entry —
its success path is split per-document-type instead, so
load_scraped_data_to_db's own except block writes nothing for it either)
while the wrapper still stamped a clean completed_at, as if the run
finished successfully. The FEMA-timeout bug class, reproduced for the one
data_type both the success and failure paths treat specially.

Fixed by having _load_to_database() report (all_ok, first_error) back to
the caller instead of swallowing, so run_lien_pipeline can call run.fail()
instead of run.suppress_completion_write() when something actually failed.

Uses tmp_path + monkeypatched directory constants — no live DB access
needed here since load_scraped_data_to_db is itself mocked out; this test
is purely about _load_to_database's own control flow and return contract.
"""
from pathlib import Path
from unittest.mock import patch

from src.scrappers.liens import lien_engine


def _empty_dirs(tmp_path):
    return tmp_path / "liens", tmp_path / "deeds", tmp_path / "judgments"


def test_all_targets_empty_returns_ok_with_no_error(tmp_path, monkeypatch):
    liens_dir, deeds_dir, judgments_dir = _empty_dirs(tmp_path)
    monkeypatch.setattr(lien_engine, "PROCESSED_LIENS_DIR", liens_dir)
    monkeypatch.setattr(lien_engine, "PROCESSED_DEEDS_DIR", deeds_dir)
    monkeypatch.setattr(lien_engine, "PROCESSED_JUDGMENTS_DIR", judgments_dir)

    all_ok, first_error = lien_engine._load_to_database("hillsborough", 0.0)

    assert all_ok is True
    assert first_error is None


def test_liens_load_failure_is_reported_not_swallowed(tmp_path, monkeypatch):
    """The core regression: previously this returned None (nothing), and
    the caller had no way to know liens failed at all."""
    liens_dir, deeds_dir, judgments_dir = _empty_dirs(tmp_path)
    (liens_dir / "new").mkdir(parents=True)
    (liens_dir / "new" / "all_liens_20260101.csv").write_text("a,b\n1,2\n")
    monkeypatch.setattr(lien_engine, "PROCESSED_LIENS_DIR", liens_dir)
    monkeypatch.setattr(lien_engine, "PROCESSED_DEEDS_DIR", deeds_dir)
    monkeypatch.setattr(lien_engine, "PROCESSED_JUDGMENTS_DIR", judgments_dir)

    synthetic_error = ValueError("synthetic liens load failure")
    with patch(
        "src.utils.scraper_db_helper.load_scraped_data_to_db",
        side_effect=synthetic_error,
    ):
        all_ok, first_error = lien_engine._load_to_database("hillsborough", 0.0)

    assert all_ok is False
    assert first_error is synthetic_error


def test_one_target_failing_does_not_block_the_others(tmp_path, monkeypatch):
    """Per-target independence must be preserved — liens failing must not
    stop deeds/judgments from being attempted."""
    liens_dir, deeds_dir, judgments_dir = _empty_dirs(tmp_path)
    for d in (liens_dir, deeds_dir, judgments_dir):
        (d / "new").mkdir(parents=True)
    (liens_dir / "new" / "all_liens_20260101.csv").write_text("a,b\n1,2\n")
    (deeds_dir / "new" / "all_deeds_20260101.csv").write_text("a,b\n1,2\n")
    monkeypatch.setattr(lien_engine, "PROCESSED_LIENS_DIR", liens_dir)
    monkeypatch.setattr(lien_engine, "PROCESSED_DEEDS_DIR", deeds_dir)
    monkeypatch.setattr(lien_engine, "PROCESSED_JUDGMENTS_DIR", judgments_dir)

    calls = []

    def _fake_load(data_type, csv_path, **kwargs):
        calls.append(data_type)
        if data_type == "liens":
            raise ValueError("synthetic liens load failure")

    with patch("src.utils.scraper_db_helper.load_scraped_data_to_db", side_effect=_fake_load):
        all_ok, first_error = lien_engine._load_to_database("hillsborough", 0.0)

    assert all_ok is False
    assert isinstance(first_error, ValueError)
    assert "liens" in calls
    assert "deeds" in calls  # attempted despite liens failing first
