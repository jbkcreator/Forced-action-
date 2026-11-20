"""Sunbiz lookups for company sellers: who is looked up, and that every outcome is on disk and never repeated."""
from __future__ import annotations

from src.tasks import lending_pr_list_sunbiz as task


def test_companies_are_looked_up_and_people_trusts_and_estates_are_not():
    assert task.is_company("QUICK FLIP LLC") and task.is_company("Sunshine Homes, Inc.")
    assert not task.is_company("JOHN SMITH")
    assert not task.is_company("SMITH FAMILY TRUST") and not task.is_company("ESTATE OF JANE DOE")


def test_each_company_is_looked_up_once_and_never_again():
    names = ["QUICK FLIP LLC", "Quick Flip, LLC", "JOHN SMITH", "BAY HOMES INC", "OLD CO LLC"]
    out = task.to_look_up(names, already_done={"BAY HOMES INC"}, in_snapshots={"OLD CO LLC"})
    assert out == ["QUICK FLIP LLC"]   # one spelling, the person, the done and the cached ones are skipped


def test_outcomes_land_on_disk_and_a_rerun_retries_only_failures(tmp_path):
    path = task.lookups_path("wholesaler", root=tmp_path)
    task.append_outcome(path, {"name": "A LLC", "normalized_name": "A LLC", "status": "found"})
    task.append_outcome(path, {"name": "B LLC", "normalized_name": "B LLC", "status": "not_found"})
    task.append_outcome(path, {"name": "C LLC", "normalized_name": "C LLC", "status": "failed"})
    assert task.done_names(path) == {"A LLC", "B LLC", "C LLC"}
    task._retry_failed_names(path)
    assert task.done_names(path) == {"A LLC", "B LLC"}      # C is looked up again next run, A and B never
