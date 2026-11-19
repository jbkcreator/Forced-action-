"""LendingFlow leads count as text-consented (client decision Nov 3) everywhere the send check does: Cora's find_lead
reports it and can draft to them, and a LendingFlow lead first reached cold gets its SMS do-not-disturb lifted."""
from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import text

from src.lending.cold_leads import warm_dnd_reconciliation
from src.lending.consent import has_text_consent, record_consent, revoke_consent
from src.lending.cora_tools import _LEAD_REF, _LEAD_SQL, CoraToolkit, DraftSmsInput

PHONE = "+18135550142"
CONTEXT = None


def _lendingflow_lead(db, phone: str = PHONE) -> int:
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, phone, first_name, last_name, "
        "email) VALUES ('v-parity', 'h-parity', '{}'::jsonb, :p, 'Pat', 'Parity', 'pat@example.com') RETURNING id"),
        {"p": phone}).scalar_one()


def _loaded_cold(db, phone: str = PHONE) -> None:
    db.execute(text("INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash) "
                    "VALUES ('run-parity', 'cold', 'ref-parity', :p, 'hash-parity')"), {"p": phone})


def _toolkit(db) -> CoraToolkit:
    toolkit = CoraToolkit(engine=None, memory=None)  # type: ignore[arg-type]

    @contextmanager
    def read_only():
        yield db

    toolkit._read_only = read_only  # type: ignore[method-assign]
    return toolkit


def _find(db, query: str = PHONE) -> list[dict]:
    from src.lending.cora_tools import FindLeadInput

    return _toolkit(db).find_lead(FindLeadInput(query=query), CONTEXT)["matches"]


# -- find_lead (needs Postgres) ---------------------------------------------------------------

def test_find_lead_returns_the_lendingflow_lead_with_text_consent(lending_db):
    lead_id = _lendingflow_lead(lending_db)
    lead = next(m for m in _find(lending_db) if m["lead_ref"] == f"lendingflow_lead:{lead_id}")
    assert lead["name"] == "Pat Parity" and lead["text_consent"] is True
    assert has_text_consent(lending_db, PHONE) is True


def test_find_lead_finds_a_lendingflow_lead_by_name(lending_db):
    lead_id = _lendingflow_lead(lending_db)
    assert f"lendingflow_lead:{lead_id}" in {m["lead_ref"] for m in _find(lending_db, "Pat Parity")}


@pytest.mark.parametrize("block", ["revoked", "suppressed", "do_not_contact"])
def test_find_lead_consent_matches_the_send_check_when_blocked(lending_db, block):
    _lendingflow_lead(lending_db)
    if block == "revoked":
        record_consent(lending_db, PHONE, "inbound_text")
        revoke_consent(lending_db, PHONE)
    elif block == "suppressed":
        lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                                "VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    else:
        lending_db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true)"), {"p": PHONE})
    assert has_text_consent(lending_db, PHONE) is False
    assert all(m["text_consent"] is False for m in _find(lending_db))


def test_a_number_with_no_lead_and_no_consent_row_reports_no_consent(lending_db):
    lending_db.execute(text("INSERT INTO lending.contacts (phone) VALUES (:p)"), {"p": PHONE})
    assert [m["text_consent"] for m in _find(lending_db)] == [False]


def test_a_draft_resolves_the_lendingflow_lead_from_the_database(lending_db):
    lead_id = _lendingflow_lead(lending_db)
    lead = _toolkit(lending_db)._lead(f"lendingflow_lead:{lead_id}")
    assert (lead.name, lead.phone, lead.email) == ("Pat Parity", PHONE, "pat@example.com")


# -- warm-up do-not-disturb (needs Postgres) ----------------------------------------------------

def test_a_lendingflow_lead_first_reached_cold_gets_sms_lifted_without_a_consent_row(lending_db):
    _loaded_cold(lending_db)
    _lendingflow_lead(lending_db)
    assert "SMS" in warm_dnd_reconciliation(lending_db, PHONE, text_consent=False)


def test_a_lendingflow_lead_that_revoked_consent_keeps_sms_blocked(lending_db):
    _loaded_cold(lending_db)
    _lendingflow_lead(lending_db)
    record_consent(lending_db, PHONE, "inbound_text")
    revoke_consent(lending_db, PHONE)
    lifted = warm_dnd_reconciliation(lending_db, PHONE, text_consent=False)
    assert "SMS" not in lifted and lifted


def test_a_website_lead_without_consent_still_keeps_sms_blocked(lending_db):
    _loaded_cold(lending_db)
    lending_db.execute(text("INSERT INTO lending.web_leads (name, phone) VALUES ('Web Lead', :p)"), {"p": PHONE})
    assert "SMS" not in warm_dnd_reconciliation(lending_db, PHONE, text_consent=False)


# -- no database -------------------------------------------------------------------------------

def test_lead_refs_cover_lendingflow_leads_and_every_kind_has_a_lookup():
    assert _LEAD_REF.match("lendingflow_lead:7") and not _LEAD_REF.match("lendingflow:7")
    assert set(_LEAD_SQL) == {"web_lead", "lendingflow_lead", "contact"}
    assert DraftSmsInput(lead_ref="lendingflow_lead:7", body="Hi there").lead_ref == "lendingflow_lead:7"
