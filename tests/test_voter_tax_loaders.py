"""
Unit + integration tests for Task 4 — voter registry + tax-collector loaders (fa077).

Pure-logic tests run with no DB. Integration tests use the `fresh_db` fixture
(real Postgres, rolled back per test) and auto-skip when DATABASE_URL is unset.
"""

from datetime import date

import pandas as pd
import pytest

from src.loaders.voter_registry import VoterRegistryLoader, _FL_DOS_HEADER
from src.services.tax_collector_enrichment import (
    TaxCollectorEnrichment,
    _classify_absentee,
    _extract_billing_fields,
)
from src.services.direct_mail import (
    resolve_best_mailing_address,
    flag_direct_mail_eligible,
)
from src.services.phone_utils import normalize as normalize_phone
from src.tasks.voter_registry_refresh import _find_voter_zip_link


# ── helpers ─────────────────────────────────────────────────────────────────

def _bare_voter_loader(county_id: str = "hillsborough") -> VoterRegistryLoader:
    """Construct a loader without __init__ (skips Anthropic client) for pure tests."""
    loader = VoterRegistryLoader.__new__(VoterRegistryLoader)
    loader.county_id = county_id
    loader._affected_property_ids = set()
    return loader


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _FakeSession:
    """Returns queued rows for successive .execute().mappings().first() calls."""

    def __init__(self, rows):
        self._rows = list(rows)
        self.executed: list[tuple] = []

    def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        row = self._rows.pop(0) if self._rows else None
        return _FakeResult(row)


# ── FL DOS header injection (pure) ────────────────────────────────────────────

def test_inject_fl_dos_header_exact_field_count():
    df = pd.DataFrame([["x"] * len(_FL_DOS_HEADER)])
    out = VoterRegistryLoader.inject_fl_dos_header(df)
    assert list(out.columns) == _FL_DOS_HEADER
    assert "voter_id" in out.columns
    assert "residential_address_line_1" in out.columns


def test_inject_fl_dos_header_handles_short_row():
    df = pd.DataFrame([["a", "b", "c"]])
    out = VoterRegistryLoader.inject_fl_dos_header(df)
    assert list(out.columns) == _FL_DOS_HEADER[:3]


# ── voter record extraction (pure) ────────────────────────────────────────────

def test_extract_voter_record_combines_name_and_maps_status():
    loader = _bare_voter_loader()
    row = pd.Series({
        "source_voter_id": "V123",
        "first_name": "JOHN",
        "middle_name": "Q",
        "last_name": "PUBLIC",
        "residential_address": "123 MAIN ST",
        "residential_city": "TAMPA",
        "residential_zip": "33601",
        "registration_status": "A",
        "registration_date": "2008-05-01",
    })
    rec = loader._extract_voter_record(row)
    assert rec["source_voter_id"] == "V123"
    assert rec["voter_name"] == "JOHN Q PUBLIC"
    assert rec["first_name"] == "JOHN"
    assert rec["registration_status"] == "ACT"
    assert rec["registration_date"] == date(2008, 5, 1)
    assert rec["county_id"] == "hillsborough"


def test_extract_voter_record_inactive_status_and_phone_normalization():
    loader = _bare_voter_loader()
    expected_phone = normalize_phone("813-272-5670")
    assert expected_phone is not None  # guards against an invalid sample number
    row = pd.Series({
        "source_voter_id": "V9",
        "last_name": "DOE",
        "voter_status": "I",          # FL DOS column name
        "Phone": "813-272-5670",
        "email": "doe@example.com",
    })
    rec = loader._extract_voter_record(row)
    assert rec["registration_status"] == "INA"
    assert rec["phone_1"] == expected_phone
    assert rec["phones"] == [expected_phone]
    assert rec["email"] == "doe@example.com"


def test_extract_voter_record_assembles_mailing_address():
    loader = _bare_voter_loader()
    row = pd.Series({
        "source_voter_id": "V5",
        "last_name": "ROE",
        "mailing_address": "PO BOX 12",
        "mailing_city": "ORLANDO",
        "mailing_state": "FL",
        "mailing_zip": "32801",
    })
    rec = loader._extract_voter_record(row)
    assert rec["mailing_address"] == "PO BOX 12, ORLANDO, FL, 32801"


def test_extract_voter_record_missing_id_and_phone():
    loader = _bare_voter_loader()
    row = pd.Series({"last_name": "NOID"})
    rec = loader._extract_voter_record(row)
    assert rec["source_voter_id"] == ""
    assert rec["phone_1"] is None
    assert rec["phones"] == []


# ── absentee classification (pure) ────────────────────────────────────────────

def test_classify_absentee_out_of_state():
    assert _classify_absentee("NY", "10001", {"33601", "33602"}) == "Out-of-State"


def test_classify_absentee_out_of_county():
    assert _classify_absentee("FL", "32801", {"33601", "33602"}) == "Out-of-County"


def test_classify_absentee_in_county():
    assert _classify_absentee("FL", "33601", {"33601", "33602"}) == "In-County"


def test_classify_absentee_no_state_returns_none():
    assert _classify_absentee(None, "33601", {"33601"}) is None


def test_classify_absentee_values_match_owner_check_constraint():
    # owners.check_absentee_status allows exactly these three values.
    allowed = {"In-County", "Out-of-County", "Out-of-State"}
    results = {
        _classify_absentee("NY", "10001", {"33601"}),
        _classify_absentee("FL", "99999", {"33601"}),
        _classify_absentee("FL", "33601", {"33601"}),
    }
    assert results <= allowed


# ── billing field extraction (pure) ───────────────────────────────────────────

def test_extract_billing_fields_parses_state_and_zip():
    norm, state, zip5 = _extract_billing_fields("123 Main St, Tampa FL 33601")
    assert state == "FL"
    assert zip5 == "33601"
    assert norm  # normalized address is non-empty


def test_extract_billing_fields_handles_zip_plus_four():
    _, state, zip5 = _extract_billing_fields("9 Wall St New York NY 10005-1234")
    assert state == "NY"
    assert zip5 == "10005"


def test_extract_billing_fields_empty():
    assert _extract_billing_fields(None) == (None, None, None)
    assert _extract_billing_fields("") == (None, None, None)


# ── MediaFire zip link finder (pure) ──────────────────────────────────────────

def test_find_voter_zip_link_matches_eligible_zip():
    files = [
        {"filename": "readme.txt", "links": {"view": "http://x"}},
        {"filename": "All Eligible Voters.zip", "links": {"normal_download": "http://dl/page"}},
    ]
    assert _find_voter_zip_link(files) == "http://dl/page"


def test_find_voter_zip_link_ignores_non_eligible_and_non_zip():
    files = [
        {"filename": "All Active Voters.zip", "links": {"normal_download": "http://a"}},
        {"filename": "All Eligible Voters.xlsx", "links": {"view": "http://b"}},
    ]
    assert _find_voter_zip_link(files) is None


# ── direct-mail resolver (fake session) ───────────────────────────────────────

def test_resolve_prefers_tax_collector():
    session = _FakeSession([{"mailing_address": "TAX ADDR"}])
    res = resolve_best_mailing_address(1, session)
    assert res.address == "TAX ADDR"
    assert res.source == "tax_collector"
    assert len(session.executed) == 1  # short-circuits, no further queries


def test_resolve_falls_back_to_voter():
    session = _FakeSession([None, {"mailing_address": "VOTER ADDR"}])
    res = resolve_best_mailing_address(1, session)
    assert res.address == "VOTER ADDR"
    assert res.source == "voter"


def test_resolve_falls_back_to_owner():
    session = _FakeSession([None, None, {"mailing_address": "OWNER ADDR"}])
    res = resolve_best_mailing_address(1, session)
    assert res.address == "OWNER ADDR"
    assert res.source == "owner"


def test_resolve_returns_none_when_nothing_found():
    session = _FakeSession([None, None, None])
    res = resolve_best_mailing_address(1, session)
    assert res.address is None
    assert res.source is None


def test_flag_direct_mail_eligible_runs_update_when_address_found():
    session = _FakeSession([{"mailing_address": "TAX ADDR"}])
    assert flag_direct_mail_eligible(1, session) is True
    assert any("UPDATE owners" in stmt for stmt, _ in session.executed)


def test_flag_direct_mail_eligible_noop_when_no_address():
    session = _FakeSession([None, None, None])
    assert flag_direct_mail_eligible(1, session) is False
    assert not any("UPDATE owners" in stmt for stmt, _ in session.executed)


# ══════════════════════════════════════════════════════════════════════════════
# Integration tests (real Postgres via fresh_db; auto-skip without DATABASE_URL)
# ══════════════════════════════════════════════════════════════════════════════

def _insert_property(session, parcel_id, normalized_address, zip_code="33601", county="hillsborough"):
    from src.core.models import Property
    prop = Property(
        parcel_id=parcel_id,
        address=normalized_address.upper(),
        normalized_address=normalized_address,
        city="TAMPA",
        state="FL",
        zip=zip_code,
        county_id=county,
    )
    session.add(prop)
    session.flush()
    return prop


def test_voter_loader_match_quarantine_and_phone_history(fresh_db):
    from src.core.models import Voter

    prop = _insert_property(fresh_db, "TEST-VOTER-PARCEL-1", "456 elm st", "33602")

    phone_in = "813-272-5670"
    expected_phone = normalize_phone(phone_in)
    assert expected_phone is not None

    df = pd.DataFrame([
        {
            "source_voter_id": "VT1",
            "first_name": "JANE",
            "last_name": "DOE",
            "residential_address": "456 ELM ST",
            "residential_zip": "33602",
            "registration_status": "A",
            "phone_1": phone_in,
        },
        {
            "source_voter_id": "VT2",
            "last_name": "NOWHERE",
            "residential_address": "9999 GHOST RD",
            "residential_zip": "00000",
            "registration_status": "I",
        },
    ])

    loader = VoterRegistryLoader(fresh_db, county_id="hillsborough")
    inserted, updated, quarantined = loader.load_from_dataframe(df)
    assert inserted == 1
    assert quarantined == 1

    voters = fresh_db.execute(
        __import__("sqlalchemy").text(
            "SELECT source_voter_id, phone_1, phones, registration_status, property_id "
            "FROM voters WHERE county_id='hillsborough' AND source_voter_id='VT1'"
        )
    ).mappings().all()
    assert len(voters) == 1
    assert voters[0]["phone_1"] == expected_phone
    assert voters[0]["phones"] == [expected_phone]
    assert voters[0]["registration_status"] == "ACT"
    assert voters[0]["property_id"] == prop.id

    # Re-run with a new phone for VT1 → updated, phone history appends, no dup row
    new_phone_in = "813-274-8211"
    new_expected = normalize_phone(new_phone_in)
    assert new_expected is not None
    df2 = pd.DataFrame([{
        "source_voter_id": "VT1",
        "first_name": "JANE",
        "last_name": "DOE",
        "residential_address": "456 ELM ST",
        "residential_zip": "33602",
        "registration_status": "A",
        "phone_1": new_phone_in,
    }])
    ins2, upd2, q2 = loader.load_from_dataframe(df2)
    assert ins2 == 0
    assert upd2 == 1

    rows = fresh_db.execute(
        __import__("sqlalchemy").text(
            "SELECT phones FROM voters WHERE county_id='hillsborough' AND source_voter_id='VT1'"
        )
    ).mappings().all()
    assert len(rows) == 1
    assert rows[0]["phones"] == [expected_phone, new_expected]


def test_tax_collector_enrichment_absentee_and_contact(fresh_db, monkeypatch):
    from src.core.models import Owner

    prop = _insert_property(fresh_db, "0123456789", "100 local st", "33601")
    owner = Owner(
        property_id=prop.id,
        owner_name="SMITH JOHN",
        mailing_address="100 LOCAL ST TAMPA FL 33601",
        absentee_status=None,
        county_id="hillsborough",
    )
    fresh_db.add(owner)
    fresh_db.flush()

    df = pd.DataFrame([{
        "account_number": "0123456789",
        "tax_year": str(date.today().year - 1),
        "owner_name": "SMITH JOHN",
        "owner_address": "PO BOX 500, NEW YORK NY 10001",
    }])

    enrichment = TaxCollectorEnrichment(fresh_db, county_id="hillsborough")
    # Avoid running the heavy CDS scorer over the real DB.
    monkeypatch.setattr(enrichment, "_trigger_rescore", lambda: None)
    result = enrichment.process_upload(df)

    assert result["enriched_contacts"] == 1
    assert result["absentee_updated"] == 1

    contact = fresh_db.execute(
        __import__("sqlalchemy").text(
            "SELECT source, mailing_address, confidence, match_success "
            "FROM enriched_contacts WHERE property_id=:pid AND source='tax_collector'"
        ),
        {"pid": prop.id},
    ).mappings().all()
    assert len(contact) == 1
    assert contact[0]["match_success"] is True
    assert float(contact[0]["confidence"]) == pytest.approx(0.90)

    status = fresh_db.execute(
        __import__("sqlalchemy").text("SELECT absentee_status FROM owners WHERE id=:oid"),
        {"oid": owner.id},
    ).scalar()
    assert status == "Out-of-State"

    # Re-run is idempotent: updates the existing contact, no new row, status unchanged.
    result2 = enrichment.process_upload(df)
    assert result2["absentee_updated"] == 0
    count = fresh_db.execute(
        __import__("sqlalchemy").text(
            "SELECT COUNT(*) FROM enriched_contacts WHERE property_id=:pid AND source='tax_collector'"
        ),
        {"pid": prop.id},
    ).scalar()
    assert count == 1


def test_tax_loader_excludes_current_roll_year(fresh_db):
    from src.loaders.tax import TaxDelinquencyLoader

    prop = _insert_property(fresh_db, "0999888777", "200 roll st", "33603")
    old_year = date.today().year - 2
    current_year = date.today().year

    df = pd.DataFrame([
        {"account_number": "0999888777", "tax_year": str(old_year), "owner_name": "ROLL OWNER"},
        {"account_number": "0999888777", "tax_year": str(current_year), "owner_name": "ROLL OWNER"},
    ])

    loader = TaxDelinquencyLoader(fresh_db, county_id="hillsborough")
    matched, updated, unmatched = loader.load_from_dataframe(df)
    assert matched == 1  # current-roll-year row excluded

    rows = fresh_db.execute(
        __import__("sqlalchemy").text(
            "SELECT tax_year FROM tax_delinquencies WHERE property_id=:pid"
        ),
        {"pid": prop.id},
    ).scalars().all()
    assert rows == [old_year]
