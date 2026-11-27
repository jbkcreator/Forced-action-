"""R1 1.1 identity sources: each lending table yields the person's identifiers; one failed source never hides others."""
from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from config.lending_contact_spine import SOURCE_NAMES
from src.lending.spine.identity import Identifier
from src.lending.spine.sources import SOURCE_READERS, known_landlines, read_records
from tests.lending.contact_spine_db import SOURCE_DDL, spine_session

PHONE_A = "+18135550101"


@pytest.fixture
def db():
    with spine_session(SOURCE_DDL) as session:
        yield session


def _by_key(db):
    records, failed = read_records(db)
    return {r.key: r for r in records}, failed


def test_readers_cover_every_source_name():
    assert [name for name, _ in SOURCE_READERS] == list(SOURCE_NAMES)


def test_lendingflow_lead_yields_phone_email_and_ghl_id(db):
    db.execute(text("INSERT INTO lending.lendingflow_leads VALUES (1, 'Ann', 'Lee', '813-555-0101', 'Ann@X.com', 'G1')"))
    records, failed = _by_key(db)
    record = records["lendingflow_leads:1"]
    assert failed == []
    assert record.name == "Ann Lee"
    assert record.identifiers == {Identifier("phone", PHONE_A), Identifier("email", "ann@x.com"),
                                  Identifier("ghl_contact", "G1")}


def test_dialer_record_yields_entity_and_both_external_ids(db):
    db.execute(text("INSERT INTO lending.dialer_load_records VALUES "
                    "(7, '8135550101', 'J Smith', 'Smith Holdings, LLC', 'G7', 'BD7')"))
    record = _by_key(db)[0]["dialer_load_records:7"]
    assert record.identifiers == {Identifier("phone", PHONE_A), Identifier("entity", "smith holdings"),
                                  Identifier("ghl_contact", "G7"), Identifier("batchdialer_contact", "BD7")}


def test_sunbiz_trace_yields_every_phone_and_email(db):
    db.execute(text("INSERT INTO lending.sunbiz_llc_contacts VALUES ('L26000001', 'Bay REI LLC', 'Pat Doe', :p, :e)"),
               {"p": json.dumps(["8135550101", "8135550102"]), "e": json.dumps(["pat@x.com"])})
    record = _by_key(db)[0]["sunbiz_llc_contacts:L26000001"]
    assert record.name == "Pat Doe"
    assert record.identifiers == {Identifier("entity", "bay rei"), Identifier("phone", PHONE_A),
                                  Identifier("phone", "+18135550102"), Identifier("email", "pat@x.com")}


def test_loan_file_uses_borrower_profile_and_entity(db):
    db.execute(text("INSERT INTO lending.borrower_profiles VALUES (3, 'John', 'Smith', '8135550101', 'js@x.com')"))
    db.execute(text("INSERT INTO lending.entities VALUES (4, 'Smith Holdings LLC')"))
    db.execute(text("INSERT INTO lending.loan_files VALUES (9, NULL, NULL, NULL, NULL, 3, 4)"))
    record = _by_key(db)[0]["loan_files:9"]
    assert record.name == "John Smith"
    assert record.identifiers == {Identifier("phone", PHONE_A), Identifier("email", "js@x.com"),
                                  Identifier("entity", "smith holdings")}


def test_call_without_phone_is_skipped(db):
    db.execute(text("INSERT INTO lending.call_dispositions (id, dialer_call_id, phone, dialer_contact_id) VALUES "
                    "(1, 'c1', '8135550101', 'BD1'), (2, 'c2', NULL, 'BD2')"))
    records = _by_key(db)[0]
    assert records["call_dispositions:c1"].identifiers == {Identifier("phone", PHONE_A),
                                                           Identifier("batchdialer_contact", "BD1")}
    assert "call_dispositions:c2" not in records


def test_known_landlines_reads_only_landline_line_types(db):
    db.execute(text("INSERT INTO lending.contacts (id, phone, line_type) VALUES (1, '+18135550101', ' Landline'),"
                    " (2, '+18135550102', 'Mobile'), (3, '+18135550103', NULL), (4, '+18135550104', 'voip'),"
                    " (5, '+18135550105', 'unknown')"))
    assert known_landlines(db) == frozenset({PHONE_A})


def test_failed_source_is_reported_and_others_still_read(db):
    db.execute(text("INSERT INTO lending.lendingflow_leads VALUES (1, 'Ann', 'Lee', '8135550101', NULL, NULL)"))
    db.execute(text("DROP TABLE lending.web_leads"))
    records, failed = _by_key(db)
    assert failed == ["web_leads"]
    assert "lendingflow_leads:1" in records
