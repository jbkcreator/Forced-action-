"""PropertyRadar runner: dry run writes nothing; --apply stages, links, hands off."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from unittest.mock import MagicMock

from src.tasks.property_radar_runner import Stages, run, to_contract


def _record(n: int) -> dict:
    return {"radar_id": f"TEST-RUN-{n}"}


def _stages(calls: list[str], pulled: list[list[dict]] | None = None) -> Stages:
    def pull(session, mode, state, campaign):
        calls.append("pull")
        yield from (pulled or [])

    return Stages(
        count=lambda s, mode, state, campaign: calls.append("count") or 42,
        pull=pull,
        mark_seen=lambda s, state, campaign, ids: calls.append(f"seen:{len(ids)}"),
        stage=lambda s, batch: calls.append(f"stage:{len(batch)}") or (len(batch), 0, 0),
        link=lambda s: calls.append("link") or {"linked": 1},
        handoff=lambda s, campaign, apply, commit: calls.append(f"handoff:{apply}:{commit is not None}")
        or {"handed_off": 1},
    )


def test_dry_run_counts_without_buying_or_writing():
    session, calls = MagicMock(), []
    summary = run(session, _stages(calls, [[_record(1)]]), mode="daily", state="FL",
                  campaign="maturity_target_lender", apply=False)
    assert calls == ["count", "link", "handoff:False:False"]
    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    assert summary.would_fetch == 42 and summary.staged["inserted"] == 0
    assert "DRY RUN" in summary.render()


def test_apply_stages_links_hands_off_and_commits():
    session, calls = MagicMock(), []
    batches = [[_record(1), _record(2)], [_record(3)]]
    summary = run(session, _stages(calls, batches), mode="daily", state="FL",
                  campaign="maturity_target_lender", apply=True)
    assert calls == ["count", "pull", "stage:2", "seen:2", "stage:1", "seen:1", "link", "handoff:True:True"]
    assert summary.staged == {"inserted": 3, "updated": 0, "skipped": 0}
    session.rollback.assert_not_called()
    assert session.commit.call_count >= 5


def test_records_marked_seen_only_after_their_batch_commits():
    session, order = MagicMock(), []
    session.commit.side_effect = lambda: order.append("commit")
    stages = _stages(order, [[_record(1)]])
    run(session, stages, mode="daily", state="FL", campaign="maturity_target_lender", apply=True)
    assert order.index("seen:1") > order.index("stage:1") + 1  # a commit sits between them


def test_failed_stage_leaves_batch_unseen():
    session, calls = MagicMock(), []
    stages = _stages(calls, [[_record(1)]])

    def boom(s, batch):
        raise RuntimeError("stage failed")

    stages.stage = boom
    try:
        run(session, stages, mode="daily", state="FL", campaign="maturity_target_lender", apply=True)
    except RuntimeError:
        pass
    assert not any(c.startswith("seen") for c in calls)


@dataclass
class _Normalized:
    radar_id: str = "P1"
    state_fips: str = "12"
    county_fips: str = "12057"
    apn: str = "0339761858"
    state: str = "FL"
    county_name: str = "HILLSBOROUGH"
    address: str = "6607 BUCKINGHAM PALMS WAY"
    city: str = "TAMPA"
    zip_code: str = "33647"
    property_type: str = "SFR"
    owner_name: str = "ACME LLC"
    ownership_type: str = "Corporate"
    lender_original: str = "KIAVI FNDG INC"
    loan_date: date = date(2025, 1, 15)
    loan_amount: int = 300000
    loan_term_years: int = 1
    est_maturity_date: date = date(2026, 1, 15)
    raw: dict = None
    loan_doc_number: None = None
    principal_name: str = "JANE DOE"
    campaign: str = "maturity_target_lender"


def test_to_contract_maps_dev1_fields_to_staging_names():
    c = to_contract(_Normalized())
    assert c["property_address"] == "6607 BUCKINGHAM PALMS WAY"
    assert c["zip"] == "33647"
    assert c["lender_name"] == "KIAVI FNDG INC"
    assert c["loan_recorded_date"] == "2025-01-15"
    assert c["est_maturity_date"] == "2026-01-15"
    assert c["loan_term_years"] == "1"
    assert c["principal_name"] == "JANE DOE"
    assert not {"address", "zip_code", "lender_original", "loan_date"} & c.keys()
