"""T-09: the intro SMS is a GHL workflow triggered by the tag T-11's push adds; pin that tag contract.

Renaming a tag, or adding the consent-missing tag in a separate call, would silently break the workflow
(docs/lending/intro-sms.md), so these assertions fail loudly instead.
"""
from __future__ import annotations

from config.lending_lendingflow import GHL_TAG_CONSENT_MISSING, GHL_TAG_LENDINGFLOW
from src.lending.lendingflow_ghl import _tags


def test_workflow_trigger_and_guard_tag_names_are_stable():
    assert GHL_TAG_LENDINGFLOW == "lendingflow"
    assert GHL_TAG_CONSENT_MISSING == "lendingflow-consent-missing"


def test_consenting_lead_gets_only_the_trigger_tag():
    assert _tags({"consent_status": "present"}) == ["lendingflow"]


def test_lead_without_consent_gets_the_guard_tag_in_the_same_list_as_the_trigger():
    assert _tags({"consent_status": "missing"}) == ["lendingflow", "lendingflow-consent-missing"]
