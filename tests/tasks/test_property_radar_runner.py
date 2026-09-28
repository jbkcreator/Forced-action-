"""PropertyRadar runner: dry run writes nothing; --apply stages, links, hands off."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

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


def test_dry_run_leaves_nothing_in_the_database(pg_engine):
    """A stage that writes during a dry run must be rolled back, not committed."""
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured")
    ref = "test-runner-dry-run-marker"
    calls: list[str] = []
    stages = _stages(calls)

    def writing_link(s):
        s.execute(text("INSERT INTO fa_max_persons (source, source_reference) VALUES ('maturity', :r)"), {"r": ref})
        return {"linked": 1}

    stages.link = writing_link
    session = Session(bind=pg_engine)
    try:
        run(session, stages, mode="daily", state="FL", campaign="maturity_target_lender", apply=False)
    finally:
        session.close()
    with pg_engine.connect() as conn:
        left = conn.execute(text("SELECT COUNT(*) FROM fa_max_persons WHERE source_reference = :r"), {"r": ref}).scalar()
    assert left == 0


class _FakePort:
    def __init__(self, radar_ids):
        self.radar_ids = radar_ids

    def count(self, criteria):
        return len(self.radar_ids)

    def purchase(self, criteria):
        for rid in self.radar_ids:
            yield _Normalized(radar_id=rid)


def _patched_stages(monkeypatch, radar_ids, seen=frozenset()):
    import config.property_radar_campaigns as campaigns
    import src.services.property_radar_normalizer as normalizer
    import src.services.property_radar_port as port_mod
    import src.tasks.property_radar_maturity_pull as pull_task
    from src.tasks import property_radar_runner as runner

    monkeypatch.setattr(port_mod, "get_property_radar_port", lambda: _FakePort(radar_ids))
    monkeypatch.setattr(campaigns, "build_campaign_criteria", lambda *a, **k: [])
    monkeypatch.setattr(normalizer, "normalize", lambda r, **k: r)
    monkeypatch.setattr(pull_task, "_check_budget", lambda *a: 0)
    monkeypatch.setattr(pull_task, "_load_seen_ids", lambda *a: seen)
    monkeypatch.setattr(pull_task, "_last_successful_run_date", lambda *a: None)
    return runner.default_stages()


def _executed_sql(session) -> list[str]:
    return [str(c.args[0]) for c in session.execute.call_args_list]


def test_pull_records_a_done_run_as_the_daily_watermark(monkeypatch):
    stages = _patched_stages(monkeypatch, ["A", "B", "C"], seen=frozenset({"B"}))
    session = MagicMock()
    session.execute.return_value.scalar.return_value = 7
    batches = list(stages.pull(session, "daily", "FL", "maturity_target_lender"))
    assert [r["radar_id"] for b in batches for r in b] == ["A", "C"]
    sql = _executed_sql(session)
    assert any("INSERT INTO property_radar_pull_runs" in s for s in sql)
    done = [c for c in session.execute.call_args_list if "status = 'done'" in str(c.args[0])]
    assert done and done[0].args[1] == {"id": 7, "fetched": 2, "exports": 3}


def test_failed_purchase_marks_the_run_failed(monkeypatch):
    stages = _patched_stages(monkeypatch, ["A"])

    class _Boom(_FakePort):
        def purchase(self, criteria):
            raise RuntimeError("api down")
            yield  # pragma: no cover

    import src.services.property_radar_port as port_mod
    monkeypatch.setattr(port_mod, "get_property_radar_port", lambda: _Boom([]))
    from src.tasks import property_radar_runner as runner
    stages = runner.default_stages()
    session = MagicMock()
    try:
        list(stages.pull(session, "daily", "FL", "maturity_target_lender"))
    except RuntimeError:
        pass
    sql = _executed_sql(session)
    assert any("status = 'failed'" in s for s in sql)
    assert not any("status = 'done'" in s for s in sql)
