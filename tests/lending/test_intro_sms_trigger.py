"""T-09: the intro SMS is a GHL workflow that starts on the tag T-11's push adds for a lead with consent.

The consent decision is made here, in code: a lead with a consent certificate gets ``lendingflow-intro-ready`` in
a second add-tags call; a lead without one never gets it, so nothing can start the workflow. Renaming a tag, or
adding the ready tag for a lead without consent, would break or bypass the workflow (docs/lending/intro-sms.md),
so these assertions fail loudly instead.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.lending_lendingflow import GHL_TAG_CONSENT_MISSING, GHL_TAG_INTRO_READY, GHL_TAG_LENDINGFLOW
from config.settings import get_settings
from src.lending.ghl_account import GhlAccount
from src.lending.lendingflow_ghl import LendingFlowGhlSink, _intro_ready_tags, _tags
from src.lending.web_leads import DeliveryError
from src.services import ghl_webhook


def _lead(consent):
    return {"id": 1, "lead_uuid": "u-1", "vendor_lead_id": "LF-1", "lead_source_campaign": "60 Second Loan Match",
            "phone": "+17275550100", "email": None, "first_name": None, "last_name": None,
            "consent_status": consent, "received_at": datetime(2026, 10, 9, tzinfo=timezone.utc)}


class _Calls(list):
    status: dict


@pytest.fixture
def ghl_calls(monkeypatch):
    """Record every GHL call the sink makes; no pipeline stage is configured, whatever the local .env says."""
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_pipeline_id", None)
    monkeypatch.setattr(settings, "lending_ghl_stage_lendingflow_new", None)
    calls = _Calls()
    status = {"ready_tag": 200}

    class Resp:
        def __init__(self, code):
            self.status_code = code

        def json(self):
            return {"contact": {"id": "c1"}}

    def fake(method, url, **kw):
        step = url.rsplit("/", 1)[-1]
        body = kw.get("json")
        calls.append((method, step, body))
        is_ready_tag = step == "tags" and body and body.get("tags") == [GHL_TAG_INTRO_READY]
        return Resp(status["ready_tag"] if is_ready_tag else 200)

    monkeypatch.setattr(ghl_webhook, "_ghl_request", fake)
    calls.status = status
    return calls


def _tag_calls(calls):
    return [c[2]["tags"] for c in calls if c[1] == "tags"]


def test_workflow_trigger_and_guard_tag_names_are_stable():
    assert GHL_TAG_LENDINGFLOW == "lendingflow"
    assert GHL_TAG_CONSENT_MISSING == "lendingflow-consent-missing"
    assert GHL_TAG_INTRO_READY == "lendingflow-intro-ready"


def test_tags_list_is_unchanged_by_the_ready_tag():
    assert _tags({"consent_status": "present"}) == ["lendingflow"]
    assert _tags({"consent_status": "missing"}) == ["lendingflow", "lendingflow-consent-missing"]


@pytest.mark.parametrize("consent, expected", [("present", ["lendingflow-intro-ready"]), ("missing", []), ("", []),
                                               ("anything-else", [])])
def test_ready_tag_only_for_an_explicit_consent_certificate(consent, expected):
    assert _intro_ready_tags({"consent_status": consent}) == expected


def test_lead_with_consent_gets_the_ready_tag_in_a_second_call_after_the_first(ghl_calls):
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present"))
    assert _tag_calls(ghl_calls) == [["lendingflow"], ["lendingflow-intro-ready"]]
    steps = [c[1] for c in ghl_calls]
    assert steps.index("upsert") < steps.index("tags") < len(steps) - 1 - steps[::-1].index("tags")
    assert steps[-1] == "notes"  # the rest of the push is unchanged and still runs after the ready tag


def test_lead_without_consent_never_gets_the_ready_tag(ghl_calls):
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("missing"))
    assert _tag_calls(ghl_calls) == [["lendingflow", "lendingflow-consent-missing"]]
    assert all(GHL_TAG_INTRO_READY not in (c[2] or {}).get("tags", []) for c in ghl_calls)


def test_a_failed_ready_tag_call_fails_the_push_so_the_sweep_retries_it(ghl_calls):
    ghl_calls.status["ready_tag"] = 500
    with pytest.raises(DeliveryError):
        LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present"))
    assert [c[1] for c in ghl_calls][-1] == "tags"  # stopped there: no card, no note until the retry succeeds
