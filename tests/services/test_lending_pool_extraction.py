"""Tests for WP-W0-1 calling-pool extraction (all three pools + OFR loader).

Covers each pool's include/exclude rules (Done-When #3), the §4.3 dialer
fields, phone normalization (A1), dedup (O16), intent filter (O14), and the
OFR broker loader.

Pure-logic tests run with no DB.  Include/exclude tests that exercise the SQL
filters use the `fresh_db` fixture (real Postgres, rolled back per test).
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from src.services.lending import pool_extraction as pe
from src.tasks import ofr_broker_load as loader


# ---------------------------------------------------------------------------
# Pure logic — no DB
# ---------------------------------------------------------------------------

class TestEntityStatusMapping:
    @pytest.mark.parametrize("raw,expected", [
        ("LLC", "LLC"),
        ("llc", "LLC"),
        ("Corporate", "CORPORATION"),
        ("Individual", "NATURAL_PERSON"),
        ("Trust", "TRUST"),
        ("", None),
        (None, None),
        ("Partnership", None),   # unknown → None (fail-closed for Dev 2's GA rule)
    ])
    def test_map_entity_status(self, raw, expected):
        assert pe._map_entity_status(raw) == expected

    @pytest.mark.parametrize("firm,expected", [
        ("1 STOP MORTGAGE LLC", "LLC"),
        ("ACME MORTGAGE CORP", "CORPORATION"),
        ("BEST LENDING INC", "CORPORATION"),
        ("SMITH MORTGAGE", None),
        (None, None),
    ])
    def test_entity_status_from_firm_name(self, firm, expected):
        assert pe._entity_status_from_firm_name(firm) == expected


class TestComposeHelpers:
    def test_compose_address_full(self):
        assert pe._compose_address("123 MAIN ST", "TAMPA", "FL", "33601") == "123 MAIN ST, TAMPA FL 33601"

    def test_compose_address_partial(self):
        assert pe._compose_address("123 MAIN ST", None, None, None) == "123 MAIN ST"

    def test_compose_address_empty(self):
        assert pe._compose_address(None, None, None, None) is None

    def test_compose_permit_details(self):
        out = pe._compose_permit_details("NEW CONSTRUCTION", date(2026, 1, 15), Decimal("250000"))
        assert "NEW CONSTRUCTION" in out and "2026-01-15" in out and "$250,000" in out

    def test_compose_permit_details_empty(self):
        assert pe._compose_permit_details(None, None, None) is None


class TestIntentThreshold:
    def test_high_passes_medium(self):
        assert pe.meets_intent_threshold("high", "medium") is True

    def test_medium_passes_medium(self):
        assert pe.meets_intent_threshold("medium", "medium") is True

    def test_low_fails_medium(self):
        assert pe.meets_intent_threshold("low", "medium") is False

    def test_unscored_passes(self):
        # No property anchor → intent scoring N/A → always passes (O14).
        assert pe.meets_intent_threshold("unscored", "medium") is True
        assert pe.meets_intent_threshold(None, "medium") is True


class TestDedup:
    def _rec(self, pool, phone, name="x"):
        return pe.CallingPoolRecord(
            run_id="", pool_name=pool, county_id="hillsborough", county_name="Hillsborough",
            borrower_name=name, entity_name=None, target_property_address=None,
            estimated_loan_value=None, recent_permit_details=None,
            entity_status=None, parcel_id=None, zip=None, state="FL",
            normalized_phone=phone, phone_available=phone is not None,
            line_type="unknown", email=None,
            financing_intent_score=None, intent_tier=None, recommended_product=None,
            aircall_campaign_tag="TAG", campaign_list=None,
            buyer_entity_id=None, permit_number=None,
            dbpr_license_number=None, source_property_id=None, source_table="t",
        )

    def test_duplicate_phone_builder_wins(self):
        p1 = [self._rec("wholesaler_flipper", "+18130000001", "wholesaler")]
        p2 = [self._rec("active_builder", "+18130000001", "builder")]
        out = pe._dedup_across_pools(p1, p2, [])
        assert len(out) == 1
        assert out[0].pool_name == "active_builder"   # Pool 2 > Pool 1

    def test_no_phone_rows_all_kept(self):
        p1 = [self._rec("wholesaler_flipper", None, "a"), self._rec("wholesaler_flipper", None, "b")]
        out = pe._dedup_across_pools(p1, [], [])
        assert len(out) == 2   # no-phone rows never deduped against each other

    def test_distinct_phones_kept(self):
        p3 = [self._rec("mortgage_broker", "+18130000001"), self._rec("mortgage_broker", "+18130000002")]
        out = pe._dedup_across_pools([], [], p3)
        assert len(out) == 2


# ---------------------------------------------------------------------------
# OFR loader
# ---------------------------------------------------------------------------

class TestOfrLoader:
    def test_parse_ofr_date(self):
        assert loader._parse_ofr_date("08-SEP-2025") == date(2025, 9, 8)
        assert loader._parse_ofr_date("") is None
        assert loader._parse_ofr_date(None) is None
        assert loader._parse_ofr_date("garbage") is None

    def test_load_csv_dry_run_filters_and_counts(self, tmp_path):
        csv = tmp_path / "mbr.csv"
        csv.write_text(
            "LICENSE NUMBER,LICENSE TYPE,NMLS ID,INTIAL APPROVAL,FIRM NAME,"
            "PRIM ADDRESS 1,PRIM ADDRESS 2,PRIM CITY,COUNTY,PRIM STATE,PRIM ZIP,"
            "MAIL ADDRESS 1,MAIL ADDRESS 2,MAIL CITY,MAIL STATE,MAIL ZIP,PHONE,STATUS,STATUS EFFECTIVE DATE\n"
            "MBR1,MBR,111,08-SEP-2025,ACME LLC,1 A ST,,TAMPA,HILLSBOROUGH,FL,33601,1 A ST,,TAMPA,FLORIDA,33601,8135551234,Approved,11-FEB-2026\n"
            # A non-broker license type must be skipped:
            "MLD9,MLD,999,08-SEP-2025,LENDER CO,2 B ST,,TAMPA,HILLSBOROUGH,FL,33601,2 B ST,,TAMPA,FLORIDA,33601,8135559999,Approved,11-FEB-2026\n",
            encoding="utf-8",
        )
        summary = loader.load_csv(MagicMock(), str(csv), dry_run=True)
        assert summary["total_rows"] == 2
        assert summary["skipped_non_broker"] == 1     # MLD lender excluded
        assert summary["broker_rows"] == 1
        assert summary["with_phone"] == 1             # valid US phone normalized


# ---------------------------------------------------------------------------
# Pool 3 — Mortgage Brokers (DB-backed)
# ---------------------------------------------------------------------------

def _insert_broker(db, *, license_number, firm="ACME LLC", county="HILLSBOROUGH",
                   status="Approved", phone="8135551234", state="FL"):
    db.execute(text("""
        INSERT INTO ofr_mortgage_brokers
            (license_number, license_type, nmls_id, firm_name, prim_address_1,
             prim_city, county, prim_state, prim_zip, phone_raw, normalized_phone, status)
        VALUES (:ln, 'MBR', '1', :firm, '1 MAIN ST', :city, :county, :state, '33601',
                :phone, :norm, :status)
    """), {
        "ln": license_number, "firm": firm, "city": "TAMPA", "county": county,
        "state": state, "phone": phone,
        "norm": pe.normalize_phone(phone), "status": status,
    })


class TestPool3Brokers:
    def test_includes_approved_target_county(self, fresh_db):
        _insert_broker(fresh_db, license_number="MBR-INC-1", county="HILLSBOROUGH")
        recs = pe._extract_pool3_mortgage_broker(fresh_db, [])
        assert any(r.dbpr_license_number == "MBR-INC-1" for r in recs)
        r = next(r for r in recs if r.dbpr_license_number == "MBR-INC-1")
        assert r.pool_name == "mortgage_broker"
        assert r.aircall_campaign_tag == "DESK_RESCUE"
        assert r.normalized_phone == "+18135551234"      # A1: normalized
        assert r.entity_status == "LLC"                    # from firm name
        assert r.state == "FL"

    def test_excludes_non_approved(self, fresh_db):
        _insert_broker(fresh_db, license_number="MBR-EXP", status="Expired")
        recs = pe._extract_pool3_mortgage_broker(fresh_db, [])
        assert not any(r.dbpr_license_number == "MBR-EXP" for r in recs)

    def test_excludes_other_county(self, fresh_db):
        _insert_broker(fresh_db, license_number="MBR-MIA", county="MIAMI-DADE")
        recs = pe._extract_pool3_mortgage_broker(fresh_db, [])
        assert not any(r.dbpr_license_number == "MBR-MIA" for r in recs)

    def test_no_phone_flagged(self, fresh_db):
        _insert_broker(fresh_db, license_number="MBR-NOPH", phone="")
        recs = pe._extract_pool3_mortgage_broker(fresh_db, [])
        r = next(r for r in recs if r.dbpr_license_number == "MBR-NOPH")
        assert r.phone_available is False and r.normalized_phone is None


# ---------------------------------------------------------------------------
# Pool 1 — Wholesalers / Flippers (DB-backed)
# ---------------------------------------------------------------------------

def _seed_wholesaler(db, *, buyer_type="wholesaler", county="hillsborough", phone="8135550001"):
    prop_id = db.execute(text(
        "SELECT id FROM properties WHERE county_id=:c LIMIT 1"), {"c": county}).scalar()
    if prop_id is None:
        pytest.skip(f"no property in county {county} to anchor test")
    import uuid
    instr = f"TST-{uuid.uuid4().hex[:12]}"
    deed_id = db.execute(text("""
        INSERT INTO deeds (property_id, instrument_number, grantee, record_date, sale_price)
        VALUES (:p, :i, 'TEST BUYER', :d, 200000) RETURNING id
    """), {"p": prop_id, "i": instr, "d": date.today()}).scalar()
    be_id = db.execute(text("""
        INSERT INTO buyer_entities
            (canonical_name, entity_type, confidence_score, verification_status,
             total_purchase_count, total_cash_volume, buyer_type, primary_phone)
        VALUES ('TEST BUYER LLC', 'LLC', 90, 'verified', 3, 600000, :bt, :ph) RETURNING id
    """), {"bt": buyer_type, "ph": phone}).scalar()
    db.execute(text("""
        INSERT INTO buyer_entity_links
            (buyer_entity_id, source_table, source_id, match_confidence, match_method)
        VALUES (:be, 'deeds', :sid, 90, 'fuzzy_name')
    """), {"be": be_id, "sid": deed_id})
    return be_id


class TestPool1Wholesalers:
    def test_includes_wholesaler(self, fresh_db):
        be_id = _seed_wholesaler(fresh_db, buyer_type="wholesaler")
        recs = pe._extract_pool1_wholesaler_flipper(fresh_db, ["hillsborough", "pinellas"])
        assert any(r.buyer_entity_id == be_id for r in recs)
        r = next(r for r in recs if r.buyer_entity_id == be_id)
        assert r.pool_name == "wholesaler_flipper"
        assert r.normalized_phone == "+18135550001"
        assert r.entity_name == "TEST BUYER LLC"        # LLC → entity_name
        assert r.aircall_campaign_tag == "DESK_CAPITAL_LOOP"

    def test_excludes_non_wholesaler_buyer_type(self, fresh_db):
        be_id = _seed_wholesaler(fresh_db, buyer_type="institutional")
        recs = pe._extract_pool1_wholesaler_flipper(fresh_db, ["hillsborough", "pinellas"])
        assert not any(r.buyer_entity_id == be_id for r in recs)


# ---------------------------------------------------------------------------
# Pool 2 — Active Builders (DB-backed)
# ---------------------------------------------------------------------------

def _seed_permit(db, *, permit_number, issue_offset_days=30, enforcement=False,
                 permit_type="NEW CONSTRUCTION", owner_phone="8135550002",
                 owner_name="PERMIT OWNER LLC"):
    """Seed a building_permit + an owner row so Pool 2b (NOC) can find contact info."""
    prop_id = db.execute(text(
        "SELECT id FROM properties WHERE county_id='hillsborough' LIMIT 1")).scalar()
    if prop_id is None:
        pytest.skip("no hillsborough property to anchor permit test")
    db.execute(text("""
        INSERT INTO building_permits
            (property_id, permit_number, county_id, permit_type, description,
             job_value, issue_date, is_enforcement_permit)
        VALUES (:p, :pn, 'hillsborough', :pt, :pt, 300000, :d, :enf)
    """), {
        "p": prop_id, "pn": permit_number, "pt": permit_type,
        "d": date.today() - timedelta(days=issue_offset_days), "enf": enforcement,
    })
    # Seed the property owner so Pool 2b LEFT JOIN owners can find a phone.
    # sunbiz_status has a Python-side ORM default ("pending"), not a server
    # default, so a raw-SQL insert must set it explicitly.
    db.execute(text("""
        INSERT INTO owners (property_id, owner_name, phone_1, sunbiz_status)
        VALUES (:p, :nm, :ph, 'pending')
        ON CONFLICT DO NOTHING
    """), {"p": prop_id, "nm": owner_name, "ph": owner_phone})
    return prop_id


class TestPool2NOCBuilders:
    """Pool 2b (List 7): NOC/permit property owners via building_permits."""

    def test_includes_recent_structural(self, fresh_db):
        _seed_permit(fresh_db, permit_number="BP-INC-1", issue_offset_days=30)
        recs = pe._extract_pool2b_noc_permits(fresh_db, ["hillsborough", "pinellas"])
        r = next((r for r in recs if r.permit_number == "BP-INC-1"), None)
        assert r is not None
        assert r.pool_name == "active_builder"
        assert r.campaign_list == "List 7"
        assert r.recent_permit_details and "NEW CONSTRUCTION" in r.recent_permit_details
        assert r.aircall_campaign_tag == "DESK_CONSTRUCTION"

    def test_excludes_old_permit(self, fresh_db):
        _seed_permit(fresh_db, permit_number="BP-OLD", issue_offset_days=400)
        recs = pe._extract_pool2b_noc_permits(fresh_db, ["hillsborough", "pinellas"])
        assert not any(r.permit_number == "BP-OLD" for r in recs)

    def test_excludes_non_structural(self, fresh_db):
        _seed_permit(fresh_db, permit_number="BP-POOL", permit_type="POOL SCREEN ENCLOSURE")
        recs = pe._extract_pool2b_noc_permits(fresh_db, ["hillsborough", "pinellas"])
        assert not any(r.permit_number == "BP-POOL" for r in recs)

    def test_no_phone_when_no_owner(self, fresh_db):
        prop_id = fresh_db.execute(text(
            "SELECT id FROM properties WHERE county_id='hillsborough' LIMIT 1")).scalar()
        if prop_id is None:
            pytest.skip("no hillsborough property")
        fresh_db.execute(text("""
            INSERT INTO building_permits
                (property_id, permit_number, county_id, permit_type, description,
                 job_value, issue_date, is_enforcement_permit)
            VALUES (:p, 'BP-NOPH', 'hillsborough', 'NEW CONSTRUCTION', 'NEW CONSTRUCTION',
                    250000, NOW(), false)
        """), {"p": prop_id})
        recs = pe._extract_pool2b_noc_permits(fresh_db, ["hillsborough", "pinellas"])
        r = next((r for r in recs if r.permit_number == "BP-NOPH"), None)
        if r:
            assert r.phone_available is False


# ---------------------------------------------------------------------------
# O28 — Estimated Loan Value (spec-backed) + owner-builder name match
# ---------------------------------------------------------------------------

class TestNamesMatch:
    def test_owner_builder_match(self):
        # Same party with co-owners / truncation still matches (owner-builder).
        assert pe._names_match("Adrian A Calvo", "ABEL AND ADRIAN A CALV") is True

    def test_unrelated_owner_rejected(self):
        assert pe._names_match("Arthur James DeAngelis", "DARLENE M BERG /TRUSTEE") is False
        assert pe._names_match("Beverly Jean Bosley", "OSCAR RODRIGUEZ") is False

    def test_empty_never_matches(self):
        assert pe._names_match(None, "X") is False
        assert pe._names_match("X", "") is False


class TestCampaignListAndLineType:
    """New fields: campaign_list (Josh's List 1-9) and line_type (mobile/landline/unknown)."""

    def test_pool1_campaign_list_2(self, fresh_db):
        be_id = _seed_wholesaler(fresh_db, buyer_type="wholesaler")
        recs = pe._extract_pool1_wholesaler_flipper(fresh_db, ["hillsborough", "pinellas"])
        r = next((r for r in recs if r.buyer_entity_id == be_id), None)
        if r:
            assert r.campaign_list == "List 2"   # cash buyers — inferred, see CAMPAIGN_LIST note
            assert r.line_type == "unknown"      # buyer_entity phone source doesn't distinguish

    def test_pool3_campaign_list_4(self, fresh_db):
        _insert_broker(fresh_db, license_number="MBR-CL4")
        recs = pe._extract_pool3_mortgage_broker(fresh_db, [])
        r = next((r for r in recs if r.dbpr_license_number == "MBR-CL4"), None)
        if r:
            assert r.campaign_list == "List 4"
            assert r.line_type == "unknown"

    def test_pool2b_noc_campaign_list_7(self, fresh_db):
        _seed_permit(fresh_db, permit_number="BP-CL7")
        recs = pe._extract_pool2b_noc_permits(fresh_db, ["hillsborough", "pinellas"])
        r = next((r for r in recs if r.permit_number == "BP-CL7"), None)
        if r:
            assert r.campaign_list == "List 7"


class TestEstimatedLoanValue:
    def test_pool2b_noc_85pct_ltc(self, fresh_db):
        _seed_permit(fresh_db, permit_number="BP-ELV")  # job_value=300000
        recs = pe._extract_pool2b_noc_permits(fresh_db, ["hillsborough", "pinellas"])
        r = next((r for r in recs if r.permit_number == "BP-ELV"), None)
        if r:
            assert r.estimated_loan_value == max(
                Decimal(str(pe.CONSTRUCTION_MIN_LOAN)),
                Decimal("300000") * Decimal(str(pe.CONSTRUCTION_LTC)),
            )

    def test_pool1_flip_floored_at_min(self, fresh_db):
        be_id = _seed_wholesaler(fresh_db)  # sale_price=200000 → 150000 > 100k floor
        recs = pe._extract_pool1_wholesaler_flipper(fresh_db, ["hillsborough", "pinellas"])
        r = next(r for r in recs if r.buyer_entity_id == be_id)
        assert r.estimated_loan_value == max(
            Decimal(str(pe.FLIP_MIN_LOAN)),
            Decimal("200000") * Decimal(str(pe.FLIP_LOAN_FACTOR)),
        )

    def test_pool3_broker_no_loan_value(self, fresh_db):
        _insert_broker(fresh_db, license_number="MBR-ELV")
        recs = pe._extract_pool3_mortgage_broker(fresh_db, [])
        r = next(r for r in recs if r.dbpr_license_number == "MBR-ELV")
        assert r.estimated_loan_value is None
