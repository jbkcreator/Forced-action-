"""R1 0.5 T7: the nightly reconcile, GHL stage slot (no database, no live GHL).

GHL is a fake HTTP callable answering ``POST /opportunities/search`` and ``GET /opportunities/pipelines``; our side
(current stage per deal, and the journal heal) is an in-memory book. Slack and the Cora inbox are recorded.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from config.settings import get_settings
from src.lending.ghl_account import GhlAccount
from src.tasks import lending_reconciliation as reconcile

ACCOUNT = GhlAccount(api_key="k", location_id="loc-1")
PIPELINES = {"pipelines": [{"id": "pipe-ndl", "name": "NDL", "stages": [
    {"id": "st-sub", "name": "Submitted to lender"}, {"id": "st-fund", "name": "Funded"}]}]}
AT_2AM_ET = datetime(2026, 11, 25, 7, 10, tzinfo=timezone.utc)  # 02:10 EST
AT_3AM_ET = datetime(2026, 11, 25, 8, 10, tzinfo=timezone.utc)


def opp(opp_id, stage_id, phone="+18135550147"):
    return {"id": opp_id, "pipelineId": "pipe-ndl", "pipelineStageId": stage_id, "contact": {"phone": phone}}


class _Ghl:
    """Fake GHL: search answers from ``opportunities`` in pages of the request's ``limit``."""

    def __init__(self, opportunities=(), status=201, error=None):
        self.opportunities, self.status, self.error, self.calls = list(opportunities), status, error, []

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.error:
            raise self.error
        if url.endswith("/opportunities/pipelines"):
            return SimpleNamespace(status_code=200, json=lambda: PIPELINES)
        if self.status >= 400:
            return SimpleNamespace(status_code=self.status, json=lambda: {})
        body = kwargs["json"]
        start = (body["page"] - 1) * body["limit"]
        page = self.opportunities[start:start + body["limit"]]
        return SimpleNamespace(status_code=self.status,
                               json=lambda: {"opportunities": page, "total": len(self.opportunities)})

    def searches(self):
        return [kwargs["json"] for method, url, kwargs in self.calls if url.endswith("/opportunities/search")]


class _Ours:
    """Our current stage per deal; a heal records the payload's stage as current (what the ghl_stage handler does)."""

    def __init__(self, stages=None):
        self.stages = dict(stages or {})
        self.heals: list[tuple[str, str, dict]] = []

    def current_stages(self, opportunity_ids):
        return {o: self.stages[o] for o in opportunity_ids if o in self.stages}

    def ingest(self, source, key, payload, *, rerun_duplicates=False):
        self.heals.append((source, key, payload))
        self.stages[payload["opportunity_id"]] = payload["stage_name"]
        return SimpleNamespace(outcome="processed")


def ghl_slot(ghl, ours, pipeline_ids=("pipe-ndl",)):
    return reconcile.GhlStageSlot(ACCOUNT, list(pipeline_ids), request=ghl,
                                  current_stages=ours.current_stages, ingest=ours.ingest)


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_reconcile_enabled", True, raising=False)


@pytest.fixture
def outbox():
    posts, inbox = [], []
    return SimpleNamespace(posts=posts, inbox=inbox, post=posts.append, add_inbox=lambda **item: inbox.append(item))


def run(slots, outbox, argv=(), now=AT_2AM_ET):
    return reconcile.main(list(argv), now=now, slots=slots, post=outbox.post, add_inbox=outbox.add_inbox)


def test_a_dropped_stage_is_healed_once_and_a_second_run_heals_nothing(enabled, outbox):
    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()

    assert run([ghl_slot(ghl, ours)], outbox) == 0
    assert run([ghl_slot(ghl, ours)], outbox) == 0

    assert len(ours.heals) == 1
    source, key, payload = ours.heals[0]
    assert source == "ghl_stage" and key.startswith("reconcile:opp-1:funded:")
    assert payload == {"opportunity_id": "opp-1", "stage_name": "Funded", "pipeline_id": "pipe-ndl",
                       "phone": "+18135550147", "source": "reconcile"}
    assert len(outbox.posts) == 1 and "healed 1" in outbox.posts[0]  # only the run that healed posts
    assert "+1813" not in outbox.posts[0]
    assert outbox.inbox == []


@pytest.mark.parametrize("ghl", [_Ghl(status=401), _Ghl(status=500), _Ghl(error=ConnectionError("boom"))])
def test_a_ghl_error_alerts_raises_an_inbox_item_and_exits_1(enabled, outbox, ghl):
    ours = _Ours()

    assert run([ghl_slot(ghl, ours)], outbox) == 1

    assert ours.heals == []
    assert len(outbox.posts) == 1 and "FAILED" in outbox.posts[0]
    assert len(outbox.inbox) == 1 and outbox.inbox[0]["event_type"] == "lending_reconcile_failed"


def test_outside_2am_et_the_job_does_nothing_unless_forced(enabled, outbox):
    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()

    assert run([ghl_slot(ghl, ours)], outbox, now=AT_3AM_ET) == 0
    assert ghl.calls == [] and ours.heals == [] and outbox.posts == []

    assert run([ghl_slot(ghl, ours)], outbox, argv=["--force"], now=AT_3AM_ET) == 0
    assert len(ours.heals) == 1


def test_2am_edt_acts_too(enabled, outbox):
    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()
    assert run([ghl_slot(ghl, ours)], outbox, now=datetime(2026, 7, 1, 6, 5, tzinfo=timezone.utc)) == 0
    assert len(ours.heals) == 1


def test_flag_off_does_nothing(outbox, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_reconcile_enabled", False, raising=False)
    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()
    assert run([ghl_slot(ghl, ours)], outbox, argv=["--force"]) == 0
    assert ghl.calls == [] and ours.heals == []


def test_a_stage_we_already_have_is_left_alone_and_a_different_one_is_healed(enabled, outbox):
    ghl = _Ghl([opp("opp-same", "st-fund"), opp("opp-behind", "st-fund")])
    ours = _Ours({"opp-same": "funded", "opp-behind": "Submitted to lender"})

    assert run([ghl_slot(ghl, ours)], outbox) == 0

    assert [payload["opportunity_id"] for _, _, payload in ours.heals] == ["opp-behind"]


def test_dry_run_reports_but_heals_and_posts_nothing(enabled, outbox):
    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()
    assert run([ghl_slot(ghl, ours)], outbox, argv=["--dry-run"]) == 0
    assert ours.heals == [] and outbox.posts == []


def test_a_heal_the_handler_fails_is_reported_and_exits_1(enabled, outbox):
    ghl, ours = _Ghl([opp("opp-1", "st-fund"), opp("opp-2", "st-sub")]), _Ours()
    outcomes = iter(["failed", "processed"])
    slot = reconcile.GhlStageSlot(ACCOUNT, ["pipe-ndl"], request=ghl, current_stages=ours.current_stages,
                                  ingest=lambda *a, **k: SimpleNamespace(outcome=next(outcomes)))

    assert run([slot], outbox) == 1

    assert "healed 1" in outbox.posts[0] and "1 not healed" in outbox.posts[0]
    assert len(outbox.inbox) == 1


def test_unconfigured_ghl_or_pipeline_fails_the_slot(enabled, outbox):
    ours = _Ours()
    no_account = reconcile.GhlStageSlot(None, ["pipe-ndl"], request=_Ghl(), current_stages=ours.current_stages,
                                        ingest=ours.ingest)
    assert run([no_account], outbox) == 1
    assert run([ghl_slot(_Ghl(), ours, pipeline_ids=())], outbox) == 1
    assert "LENDING_RECONCILE_GHL_PIPELINE_IDS" in outbox.posts[1]


def test_one_failing_slot_does_not_stop_the_others(enabled, outbox):
    class _Broken:
        name = "broken"

        def find_missing(self, since):
            raise RuntimeError("x")

        def heal(self, payload):
            raise AssertionError

    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()
    assert run([_Broken(), ghl_slot(ghl, ours)], outbox) == 1
    assert len(ours.heals) == 1


def test_a_slot_warning_is_noted_without_failing(enabled, outbox):
    class _NoLog:
        name = "LendingFlow"

        def find_missing(self, since):
            raise reconcile.SlotWarning("edge log missing")

        def heal(self, payload):
            raise AssertionError

    ghl, ours = _Ghl([opp("opp-1", "st-fund")]), _Ours()
    assert run([_NoLog(), ghl_slot(ghl, ours)], outbox) == 0
    assert "warning: edge log missing" in outbox.posts[0] and outbox.inbox == []


# ---- GHL request shape -----------------------------------------------------------------------------------------

def test_search_is_the_advanced_search_filtered_by_pipeline_and_date_updated_paged_by_500(enabled, outbox):
    ghl = _Ghl([opp(f"opp-{i}", "st-fund") for i in range(501)])
    ours = _Ours()

    run([ghl_slot(ghl, ours)], outbox)

    searches = [(m, u, k) for m, u, k in ghl.calls if u.endswith("/opportunities/search")]
    assert [m for m, _, _ in searches] == ["POST", "POST"]
    assert searches[0][2]["headers"]["Version"] == "v3"
    first, second = (k["json"] for _, _, k in searches)
    assert first == {
        "locationId": "loc-1", "limit": 500, "page": 1,
        "filters": [{"field": "pipeline_id", "operator": "eq", "value": "pipe-ndl"},
                    {"field": "date_updated", "operator": "range", "value": {"gte": "2026-11-23T07:10:00Z"}}],
        "sort": [{"field": "date_updated", "direction": "asc"}]}
    assert second["page"] == 2
    assert len(ours.heals) == 501


def test_hours_sets_the_window(enabled, outbox):
    ghl = _Ghl()
    run([ghl_slot(ghl, _Ours())], outbox, argv=["--hours", "48"])
    assert ghl.searches()[0]["filters"][1]["value"] == {"gte": "2026-11-23T07:10:00Z"}


def test_each_configured_pipeline_is_searched(enabled, outbox):
    ghl = _Ghl()
    run([ghl_slot(ghl, _Ours(), pipeline_ids=("pipe-a", "pipe-b"))], outbox)
    assert [s["filters"][0]["value"] for s in ghl.searches()] == ["pipe-a", "pipe-b"]

