"""Live Aircall round trip for the lending dialer contact writes.

Creates one test contact on the configured Aircall account, re-upserts it
(which must update, not duplicate), then deletes it. Only runs when
AIRCALL_LIVE_TEST=1 is set, so a normal test run never calls Aircall. Point
it at a test account, never at an account callers work from.
"""
from __future__ import annotations

import os

import pytest

from src.services import aircall_client
from src.services.aircall_client import AircallContactFields, upsert_contact

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("AIRCALL_LIVE_TEST") != "1",
                       reason="set AIRCALL_LIVE_TEST=1 to call Aircall"),
]

TEST_PHONE = "+18135550143"


def test_upsert_creates_then_updates_without_duplicating():
    assert aircall_client.find_contacts_by_phone(TEST_PHONE) == [], "test phone already in use on this account"
    contact_id = None
    try:
        first = upsert_contact(TEST_PHONE, AircallContactFields(
            first_name="Conflict", last_name="Check Test", company_name="TEST HOLDINGS LLC",
            information="Property: 1 Test St\nCampaign: TEST",
        ))
        contact_id = first.contact_id
        assert first.created

        second = upsert_contact(TEST_PHONE, AircallContactFields(
            first_name="Conflict", last_name="Check Test", company_name="TEST HOLDINGS LLC",
            information="Property: 2 Test St\nCampaign: TEST",
        ))
        assert second.contact_id == contact_id
        assert not second.created

        found = aircall_client.find_contacts_by_phone(TEST_PHONE)
        assert [int(c["id"]) for c in found] == [contact_id]
        # Search results can lag an update, so the new values are read by id.
        assert "2 Test St" in (aircall_client.get_contact(contact_id).get("information") or "")
    finally:
        if contact_id is not None:
            aircall_client.delete_contact(contact_id)
    assert aircall_client.find_contacts_by_phone(TEST_PHONE) == []
