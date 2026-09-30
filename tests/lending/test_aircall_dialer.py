"""The Aircall adapter honours the DialerClient contract and translates Aircall errors."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.lending.aircall_dialer import AircallDialer
from src.lending.dialer_client import DialerAmbiguousContact, DialerRequestError
from src.lending.dialer_contact import display_from_record
from src.services import aircall_client
from src.services.aircall_client import AircallAmbiguousContact, AircallRequestError, ContactUpsertResult

PHONE = "+18135558101"
DISPLAY = display_from_record({"borrower_name": "Ann Lee", "entity_name": "LEE LLC"}, "DESK_CONSTRUCTION")


def test_upsert_maps_the_display_and_returns_the_neutral_result():
    with patch.object(aircall_client, "upsert_contact",
                      return_value=ContactUpsertResult(contact_id=7, created=True)) as upsert:
        result = AircallDialer().upsert_contact(PHONE, DISPLAY, "Ann@Lee.com")
    assert (result.contact_id, result.created) == (7, True)
    phone, fields = upsert.call_args.args
    assert phone == PHONE
    assert (fields.first_name, fields.last_name, fields.company_name, fields.email) == ("Ann", "Lee", "LEE LLC", "ann@lee.com")


def test_request_error_keeps_its_status():
    with patch.object(aircall_client, "update_contact", side_effect=AircallRequestError("POST", "/contacts/7", 404)):
        with pytest.raises(DialerRequestError) as err:
            AircallDialer().update_contact(7, DISPLAY, None)
    assert err.value.status == 404


def test_ambiguous_contact_is_translated():
    with patch.object(aircall_client, "upsert_contact", side_effect=AircallAmbiguousContact("2 contacts")):
        with pytest.raises(DialerAmbiguousContact):
            AircallDialer().upsert_contact(PHONE, DISPLAY, None)
