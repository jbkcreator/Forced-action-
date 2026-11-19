import pytest
from sqlalchemy import text

from src.lending.consent import has_text_consent, record_consent, revoke_consent

PHONE = "+18135550100"


def test_no_row_means_no_consent(lending_db):
    assert has_text_consent(lending_db, PHONE) is False


def test_recorded_consent_passes_and_is_idempotent(lending_db):
    record_consent(lending_db, PHONE, "on_call_yes", call_id="c1", captured_by="Dana")
    record_consent(lending_db, PHONE, "on_call_yes", call_id="c2", captured_by="Dana")
    assert has_text_consent(lending_db, PHONE) is True
    assert lending_db.execute(text("SELECT count(*) FROM lending.text_consents")).scalar() == 1


def test_revoked_consent_no_longer_passes(lending_db):
    record_consent(lending_db, PHONE, "inbound_call", call_id="c1")
    revoke_consent(lending_db, PHONE)
    assert has_text_consent(lending_db, PHONE) is False


def test_suppressed_or_do_not_contact_number_never_has_consent(lending_db):
    record_consent(lending_db, PHONE, "on_call_yes")
    lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    assert has_text_consent(lending_db, PHONE) is False


def test_unknown_source_is_rejected(lending_db):
    with pytest.raises(ValueError):
        record_consent(lending_db, PHONE, "cold_list")


def _lendingflow_lead(db, phone=PHONE):
    db.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, phone) "
                    "VALUES ('v-consent', 'h-consent', '{}'::jsonb, :p)"), {"p": phone})


def test_a_lendingflow_lead_has_consent_without_a_consent_row(lending_db):
    _lendingflow_lead(lending_db)
    assert has_text_consent(lending_db, PHONE) is True


def test_a_lendingflow_lead_that_revoked_consent_has_none(lending_db):
    _lendingflow_lead(lending_db)
    record_consent(lending_db, PHONE, "inbound_text")
    revoke_consent(lending_db, PHONE)
    assert has_text_consent(lending_db, PHONE) is False


def test_a_suppressed_lendingflow_lead_has_no_consent(lending_db):
    _lendingflow_lead(lending_db)
    lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    assert has_text_consent(lending_db, PHONE) is False


def test_do_not_contact_flag_blocks_consent(lending_db):
    record_consent(lending_db, PHONE, "on_call_yes")
    lending_db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true)"), {"p": PHONE})
    assert has_text_consent(lending_db, PHONE) is False


def test_handoff_contract_unknown_then_yes_then_suppressed(lending_db):
    assert has_text_consent(lending_db, PHONE) is False
    record_consent(lending_db, PHONE, "on_call_yes")
    assert has_text_consent(lending_db, PHONE) is True
    lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    assert has_text_consent(lending_db, PHONE) is False
