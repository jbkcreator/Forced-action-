"""Unit tests for MasterPropertyLoader.

Covers the mailing-address helpers, the fa077 hash-canonicalization layer
(_canon / compute_source_row_hash), and _parse_row — all in isolation:
no DB, no file I/O. The full insert/update integration scenario lives in
TestWeeklyRefreshIntegration and only runs when TEST_MASTER_LOADER_PG=1
(it commits real rows to the configured Postgres under a throwaway county).
"""

import os
from datetime import date, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.loaders.master import (
    HASH_VERSION,
    MasterPropertyLoader,
    _HASH_FIELDS,
    _canon,
    compute_source_row_hash,
)


@pytest.fixture
def loader():
    return MasterPropertyLoader(MagicMock(), county_id="hillsborough")


def _row(**overrides) -> pd.Series:
    """A valid canonical master CSV row; override fields per test."""
    base = {
        'FOLIO': '16-29-15-32292-019-0010',
        'OWNER': 'SMITH JOHN',
        'SITE_ADDR': '123 MAIN ST',
        'SITE_CITY': 'TAMPA',
        'SITE_ZIP': '33601-1234',
        'TYPE': 'SFR',
        'YR_BLT': '1987',
        'HEAT_AR': '1500',
        'ACREAGE': '0.25',
        'LEGAL1': 'LOT 1', 'LEGAL2': 'BLOCK 2',
        'ADDR_1': '999 OTHER RD', 'CITY': 'ATLANTA', 'STATE': 'GA', 'ZIP': '30303',
        'ASD_VAL': '150000', 'TAX_VAL': '120000',
        'SALE1_DATE': '03/07/2024', 'SALE1_PRC': '250000',
    }
    base.update(overrides)
    return pd.Series(base)


# ---------------------------------------------------------------------------
# _parse_mailing_parts
# ---------------------------------------------------------------------------

class TestParseMailing:
    def test_format_b_four_parts(self):
        # ADDR_1, CITY, STATE, ZIP
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "380 PARK PLACE BLVD STE 200, CLEARWATER, FL, 33759-4939"
        )
        assert street == "380 PARK PLACE BLVD STE 200"
        assert state == "FL"

    def test_format_b_street_with_comma(self):
        # Street itself has a comma (suite number inline)
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "123 MAIN ST, APT 4, TAMPA, FL, 33601"
        )
        assert street == "123 MAIN ST, APT 4"
        assert state == "FL"

    def test_format_three_parts(self):
        # ADDR_1, CITY, STATE — no zip
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "9625 WES KEARNEY WAY, TAMPA, FL"
        )
        assert street == "9625 WES KEARNEY WAY"
        assert state == "FL"

    def test_format_a_street_only(self):
        # Only street stored — state was blank in CSV
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "9625 WES KEARNEY WAY"
        )
        assert street == "9625 WES KEARNEY WAY"
        assert state is None

    def test_out_of_state(self):
        street, state = MasterPropertyLoader._parse_mailing_parts(
            "100 PEACHTREE ST, ATLANTA, GA, 30303"
        )
        assert street == "100 PEACHTREE ST"
        assert state == "GA"

    def test_empty_string(self):
        street, state = MasterPropertyLoader._parse_mailing_parts("")
        assert street is None
        assert state is None

    def test_state_truncated_to_two_chars(self):
        # Malformed state column — should still take first two chars
        _, state = MasterPropertyLoader._parse_mailing_parts(
            "1 MAIN ST, CITY, FLORIDA, 99999"
        )
        assert state == "FL"


# ---------------------------------------------------------------------------
# _determine_absentee_status
# ---------------------------------------------------------------------------

class TestDetermineAbsenteeStatus:
    def _call(self, mailing, prop_addr="1949 ANCLOTE VIS", prop_state="FL"):
        return MasterPropertyLoader._determine_absentee_status(
            property_address=prop_addr,
            property_state=prop_state,
            mailing_address=mailing,
        )

    # -- None / empty guards ------------------------------------------------

    def test_none_mailing_returns_none(self):
        assert self._call(None) is None

    def test_none_property_address_returns_none(self):
        result = MasterPropertyLoader._determine_absentee_status(
            property_address=None,
            property_state="FL",
            mailing_address="1949 ANCLOTE VIS",
        )
        assert result is None

    def test_empty_mailing_returns_none(self):
        assert self._call("") is None

    # -- Out-of-State -------------------------------------------------------

    def test_out_of_state_format_b(self):
        result = self._call("100 PEACHTREE ST, ATLANTA, GA, 30303")
        assert result == "Out-of-State"

    def test_out_of_state_format_three(self):
        result = self._call("100 PEACHTREE ST, ATLANTA, GA")
        assert result == "Out-of-State"

    def test_fl_state_does_not_trigger_out_of_state(self):
        # Same state — must not return Out-of-State
        result = self._call(
            "999 DIFFERENT BLVD, CLEARWATER, FL, 33759",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result != "Out-of-State"

    # -- In-County ----------------------------------------------------------

    def test_in_county_format_b_matching_street(self):
        # Full blob, mailing street matches situs street after normalization
        result = self._call(
            "1949 ANCLOTE VIS, TARPON SPRINGS, FL, 34689",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result == "In-County"

    def test_in_county_format_a_street_only(self):
        # Street-only blob, matches
        result = self._call("1949 ANCLOTE VIS", prop_addr="1949 ANCLOTE VIS")
        assert result == "In-County"

    def test_in_county_case_insensitive(self):
        # Mixed case in mailing — normalization should handle it
        result = self._call(
            "1949 Anclote Vis, Tarpon Springs, FL, 34689",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result == "In-County"

    # -- Out-of-County (fallback) ------------------------------------------

    def test_out_of_county_different_fl_street_format_b(self):
        result = self._call(
            "380 PARK PLACE BLVD STE 200, CLEARWATER, FL, 33759",
            prop_addr="1949 ANCLOTE VIS",
        )
        assert result == "Out-of-County"

    def test_out_of_county_format_a_different_street(self):
        result = self._call("999 OTHER ROAD", prop_addr="1949 ANCLOTE VIS")
        assert result == "Out-of-County"

    def test_out_of_county_when_mailing_state_unknown(self):
        # Format A — state cannot be determined; street doesn't match → Out-of-County
        result = self._call("999 DIFFERENT ST", prop_addr="1949 ANCLOTE VIS")
        assert result == "Out-of-County"


# ---------------------------------------------------------------------------
# _canon — hash canonicalization rules (fa077)
# ---------------------------------------------------------------------------

class TestCanon:
    def test_none_is_empty_string(self):
        assert _canon('address', None) == ''

    def test_string_collapses_whitespace_and_uppercases(self):
        assert _canon('address', '  123  Main \t st ') == '123 MAIN ST'

    def test_money_fields_quantized_to_two_dp(self):
        # int, float, and string-derived floats all canonicalize identically
        assert _canon('assessed_value_mkt', 150000) == '150000.00'
        assert _canon('assessed_value_mkt', 150000.0) == '150000.00'
        assert _canon('sq_ft', 1500.5) == '1500.50'

    def test_beds_baths_quantized_to_one_dp(self):
        assert _canon('beds', 3) == '3.0'
        assert _canon('beds', 3.0) == '3.0'
        assert _canon('baths', 2.5) == '2.5'

    def test_year_built_plain_int(self):
        assert _canon('year_built', 1987) == '1987'

    def test_date_is_iso(self):
        assert _canon('last_sale_date', date(2024, 3, 7)) == '2024-03-07'

    def test_datetime_drops_time_component(self):
        assert _canon('last_sale_date', datetime(2024, 3, 7, 14, 30)) == '2024-03-07'


# ---------------------------------------------------------------------------
# compute_source_row_hash
# ---------------------------------------------------------------------------

class TestComputeSourceRowHash:
    def _blank(self) -> dict:
        return {f: None for f in _HASH_FIELDS}

    def test_deterministic(self):
        parsed = {**self._blank(), 'address': '123 MAIN ST', 'beds': 3.0}
        assert compute_source_row_hash(parsed) == compute_source_row_hash(parsed)
        assert len(compute_source_row_hash(parsed)) == 32

    def test_numeric_format_drift_does_not_change_hash(self):
        # '3' vs '3.0' vs 3 — the canonical form is identical so re-parsing the
        # same source value next week must not look like a change.
        a = {**self._blank(), 'beds': 3, 'assessed_value_mkt': 150000}
        b = {**self._blank(), 'beds': 3.0, 'assessed_value_mkt': 150000.00}
        assert compute_source_row_hash(a) == compute_source_row_hash(b)

    def test_value_change_changes_hash(self):
        a = {**self._blank(), 'owner_name': 'SMITH JOHN'}
        b = {**self._blank(), 'owner_name': 'JONES MARY'}
        assert compute_source_row_hash(a) != compute_source_row_hash(b)

    def test_field_position_matters(self):
        # Same value in a different field must hash differently (ordered fields)
        a = {**self._blank(), 'address': 'X'}
        b = {**self._blank(), 'city': 'X'}
        assert compute_source_row_hash(a) != compute_source_row_hash(b)

    def test_hash_version_prefix_present(self):
        # Bumping HASH_VERSION must invalidate every stored hash — guard that
        # the version actually participates in the payload.
        import hashlib
        blank = self._blank()
        manual = hashlib.md5(
            '|'.join([HASH_VERSION] + [''] * len(_HASH_FIELDS)).encode()
        ).hexdigest()
        assert compute_source_row_hash(blank) == manual


# ---------------------------------------------------------------------------
# _parse_row
# ---------------------------------------------------------------------------

class TestParseRow:
    def test_golden_row(self, loader):
        parsed, reason = loader._parse_row(_row())
        assert reason is None
        assert parsed['parcel_id'] == '162915322920190010'   # hyphens stripped
        assert parsed['address'] == '123 MAIN ST'
        assert parsed['city'] == 'TAMPA'
        assert parsed['zip'] == '33601'                       # ZIP+4 truncated
        assert parsed['property_type'] == 'SFR'
        assert parsed['year_built'] == 1987
        assert parsed['sq_ft'] == 1500.0
        assert parsed['lot_size'] == 0.25
        assert parsed['legal_description'] == 'LOT 1 BLOCK 2'
        assert parsed['owner_name'] == 'SMITH JOHN'
        assert parsed['owner_type'] == 'Individual'
        assert parsed['absentee_status'] == 'Out-of-State'
        assert parsed['assessed_value_mkt'] == 150000.0
        assert parsed['last_sale_price'] == 250000.0
        # parse_date returns datetime; the loader must store a bare date so the
        # hash agrees with the DATE column it round-trips against.
        assert parsed['last_sale_date'] == date(2024, 3, 7)
        assert not isinstance(parsed['last_sale_date'], datetime)

    def test_missing_folio_skipped(self, loader):
        parsed, reason = loader._parse_row(_row(FOLIO=''))
        assert parsed is None and reason == 'no_folio'

    def test_nan_folio_skipped(self, loader):
        parsed, reason = loader._parse_row(_row(FOLIO=float('nan')))
        assert parsed is None and reason == 'no_folio'

    def test_missing_owner_skipped(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER=''))
        assert parsed is None and reason == 'no_owner'

    def test_address_like_owner_skipped(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER='123 FAKE STREET TAMPA'))
        assert parsed is None and reason == 'invalid_owner'

    def test_single_caps_word_owner_skipped(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER='ODESSA'))
        assert parsed is None and reason == 'invalid_owner'

    def test_llc_owner_type(self, loader):
        parsed, _ = loader._parse_row(_row(OWNER='ACME HOLDINGS LLC'))
        assert parsed['owner_type'] == 'LLC'

    # -- entity-marker exemption: address-shaped names that ARE real owners --

    def test_address_named_llc_accepted(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER='17808 LEE AVENUE LLC'))
        assert reason is None
        assert parsed['owner_type'] == 'LLC'

    def test_lp_suffix_with_leading_digits_accepted(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER='2019 1 IH BORROWER LP'))
        assert reason is None

    def test_government_entity_with_street_word_accepted(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER='PINELLAS COUNTY'))
        assert reason is None

    def test_church_with_st_abbreviation_accepted(self, loader):
        parsed, reason = loader._parse_row(
            _row(OWNER='MACEDONIA FREEWILL BAPT CHURCH ST PETERSBURG INC')
        )
        assert reason is None

    def test_single_word_bank_accepted(self, loader):
        parsed, reason = loader._parse_row(_row(OWNER='USAMERIBANK'))
        assert reason is None

    def test_pure_address_spill_still_rejected(self, loader):
        # No entity marker — this is address text in the owner column
        parsed, reason = loader._parse_row(_row(OWNER='123 MAIN ST APT 4'))
        assert parsed is None and reason == 'invalid_owner'

    def test_trust_owner_type(self, loader):
        parsed, _ = loader._parse_row(_row(OWNER='SMITH FAMILY TRUST'))
        assert parsed['owner_type'] == 'Trust'

    def test_invalid_zip_is_none(self, loader):
        parsed, _ = loader._parse_row(_row(SITE_ZIP='UNKNOWN'))
        assert parsed['zip'] is None

    def test_future_year_built_rejected(self, loader):
        parsed, _ = loader._parse_row(_row(YR_BLT='2150'))
        assert parsed['year_built'] is None

    def test_same_row_parses_to_same_hash(self, loader):
        # End-to-end determinism: parse twice, hash twice
        a, _ = loader._parse_row(_row())
        b, _ = loader._parse_row(_row())
        assert compute_source_row_hash(a) == compute_source_row_hash(b)

    # -- property_type: county label wins, DOR code translates as fallback --

    def test_county_label_type_wins_over_dor(self, loader):
        parsed, _ = loader._parse_row(_row(TYPE='0110 Single Family Home', DOR_C='0100'))
        assert parsed['property_type'] == '0110 Single Family Home'

    def test_dor_code_translated_when_type_empty(self, loader):
        parsed, _ = loader._parse_row(_row(TYPE=float('nan'), DOR_C='0100'))
        assert parsed['property_type'] == '0100 Single Family'

    def test_dor_government_code(self, loader):
        parsed, _ = loader._parse_row(_row(TYPE=float('nan'), DOR_C='8800'))
        assert parsed['property_type'] == '8800 Federal'

    def test_unknown_dor_code_falls_back_to_bare_code(self, loader):
        from src.loaders.dor_use_codes import translate_dor_code
        assert translate_dor_code('XX12') is None
        assert translate_dor_code('123') == '0123 Single Family'   # zero-padded -> major class 01
        assert translate_dor_code(None) is None

    def test_no_type_and_no_dor_is_none(self, loader):
        parsed, _ = loader._parse_row(_row(TYPE=float('nan')))
        assert parsed['property_type'] is None


# ---------------------------------------------------------------------------
# Removed legacy API
# ---------------------------------------------------------------------------

class TestRemovedDataFrameApi:
    def test_load_from_dataframe_raises(self, loader):
        with pytest.raises(NotImplementedError):
            loader.load_from_dataframe(pd.DataFrame())


# ---------------------------------------------------------------------------
# Full insert/update integration (opt-in: commits to the configured Postgres)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    os.environ.get("TEST_MASTER_LOADER_PG") != "1",
    reason="Set TEST_MASTER_LOADER_PG=1 to run (commits rows to the configured Postgres)",
)
class TestWeeklyRefreshIntegration:
    """
    End-to-end scenario against real Postgres under a throwaway county:
      run 1: all parcels new -> inserted, hash + last_seen_at stamped
      run 2 (modified file): changed parcel updated + flags set; unchanged
             parcel only re-stamped; parcel missing from the file untouched;
             enriched fields (phone_1, gohighlevel_contact_id) never clobbered.
    """

    COUNTY = "zztest_master_refresh"

    @pytest.fixture
    def pg_session(self, pg_engine):
        if pg_engine is None:
            pytest.skip("DATABASE_URL not configured")
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import text
        session = sessionmaker(bind=pg_engine)()
        yield session
        session.rollback()
        # Hard cleanup of everything the test county created
        session.execute(text(
            "DELETE FROM owners WHERE county_id = :c"), {"c": self.COUNTY})
        session.execute(text(
            "DELETE FROM financials WHERE county_id = :c"), {"c": self.COUNTY})
        session.execute(text(
            "DELETE FROM properties WHERE county_id = :c"), {"c": self.COUNTY})
        session.execute(text(
            "DELETE FROM unmatched_records WHERE county_id = :c"), {"c": self.COUNTY})
        session.commit()
        session.close()

    def _write_csv(self, tmp_path, rows):
        cols = ['FOLIO', 'OWNER', 'SITE_ADDR', 'SITE_CITY', 'SITE_ZIP', 'TYPE',
                'YR_BLT', 'HEAT_AR', 'ACREAGE', 'LEGAL1',
                'ADDR_1', 'CITY', 'STATE', 'ZIP',
                'ASD_VAL', 'TAX_VAL', 'SALE1_DATE', 'SALE1_PRC']
        df = pd.DataFrame(rows, columns=cols)
        path = tmp_path / "master_test.csv"
        df.to_csv(path, index=False)
        return str(path)

    def _run(self, session, csv_path, monkeypatch, dry_run=False):
        import src.utils.county_config as county_config
        monkeypatch.setattr(
            county_config, "get_county_config",
            lambda cid: {"display_name": "ZZ Test"},
        )
        loader = MasterPropertyLoader(session, county_id=self.COUNTY)
        # Throwaway county has no county_sources row -> mapping resolves to None
        return loader.load_from_csv(csv_path, chunksize=100, dry_run=dry_run)

    def test_insert_then_update_flow(self, pg_session, tmp_path, monkeypatch):
        from sqlalchemy import text

        base = ['ZZT-001', 'SMITH JOHN', '123 MAIN ST', 'TAMPA', '33601', 'SFR',
                '1987', '1500', '0.25', 'LOT 1',
                '999 OTHER RD', 'ATLANTA', 'GA', '30303',
                '150000', '120000', '03/07/2024', '250000']
        stable = ['ZZT-002', 'DOE JANE', '456 OAK AVE', 'TAMPA', '33602', 'SFR',
                  '1990', '1800', '0.30', 'LOT 2',
                  '456 OAK AVE, TAMPA, FL, 33602', 'TAMPA', 'FL', '33602',
                  '200000', '180000', '01/15/2020', '300000']
        vanishing = ['ZZT-003', 'BROWN BOB', '789 PINE LN', 'TAMPA', '33603', 'SFR',
                     '2000', '2000', '0.20', 'LOT 3',
                     '789 PINE LN', 'TAMPA', 'FL', '33603',
                     '250000', '230000', '06/01/2021', '350000']
        ownerless = ['ZZT-004', '', '999 EMPTY CT', 'TAMPA', '33604', 'SFR',
                     '1975', '900', '0.15', 'LOT 4',
                     '', '', '', '',
                     '80000', '70000', '', '']
        fmt_only = ['ZZT-005', 'PEREZ LUIS', '321 ELM AVE', 'TAMPA', '33605', 'SFR',
                    '1995', '1200', '0.18', 'LOT 5',
                    '321 ELM AVE, TAMPA, FL, 33605', 'TAMPA', 'FL', '33605',
                    '120000', '110000', '02/02/2018', '180000']

        # ── run 1: everything new (+ one no-owner row -> quarantine) ─────
        stats1 = self._run(
            pg_session,
            self._write_csv(tmp_path, [base, stable, vanishing, ownerless, fmt_only]),
            monkeypatch,
        )
        assert stats1.inserted == 4
        assert stats1.updated == 0 and stats1.unchanged == 0
        assert stats1.skip_reasons['no_owner'] == 1

        quarantined = pg_session.execute(text("""
            SELECT instrument_number, source_type, match_status, raw_data
            FROM unmatched_records WHERE county_id = :c"""),
            {"c": self.COUNTY}).fetchall()
        assert len(quarantined) == 1
        assert quarantined[0].instrument_number == 'ZZT004'
        assert quarantined[0].source_type == 'master_data'
        assert quarantined[0].match_status == 'unmatched'
        assert quarantined[0].raw_data['FOLIO'] == 'ZZT-004'

        rows = pg_session.execute(text(
            "SELECT parcel_id, source_row_hash, last_seen_at FROM properties "
            "WHERE county_id = :c ORDER BY parcel_id"), {"c": self.COUNTY}
        ).fetchall()
        assert len(rows) == 4
        assert all(r.source_row_hash and r.last_seen_at for r in rows)

        # Simulate post-insert enrichment that an update must never clobber
        pg_session.execute(text("""
            UPDATE properties SET sync_status = 'synced',
                                  gohighlevel_contact_id = 'zzt-ghl-1'
            WHERE county_id = :c AND parcel_id = 'ZZT001'"""), {"c": self.COUNTY})
        pg_session.execute(text("""
            UPDATE owners o SET phone_1 = '8135551234', skip_trace_success = TRUE
            FROM properties p WHERE o.property_id = p.id
              AND p.county_id = :c AND p.parcel_id IN ('ZZT001', 'ZZT005')"""),
            {"c": self.COUNTY})
        pg_session.commit()

        # ── run 2: ZZT-001 sold (new owner + price), ZZT-003 gone,
        #          ZZT-005 owner reformatted only (PEREZ LUIS -> PEREZ, LUIS) ──
        sold = list(base)
        sold[1] = 'JONES MARY'          # owner changed
        sold[14] = '175000'             # assessed value changed
        fmt_comma = list(fmt_only)
        fmt_comma[1] = 'PEREZ, LUIS'    # same person, portal punctuation drift
        stats2 = self._run(
            pg_session, self._write_csv(tmp_path, [sold, stable, fmt_comma]),
            monkeypatch,
        )
        assert stats2.inserted == 0
        assert stats2.updated == 2          # sold + the reformatted row
        assert stats2.unchanged == 1
        assert stats2.flagged_rescore == 1  # formatting-only row NOT flagged
        assert stats2.flagged_resync == 1
        assert stats2.flagged_stale_trace == 1

        # Formatting-only owner change: string updated, no flags, trace kept fresh
        fmt_row = pg_session.execute(text("""
            SELECT p.needs_rescore, o.owner_name, o.skip_trace_stale, o.phone_1
            FROM properties p JOIN owners o ON o.property_id = p.id
            WHERE p.county_id = :c AND p.parcel_id = 'ZZT005'"""),
            {"c": self.COUNTY}).one()
        assert fmt_row.owner_name == 'PEREZ, LUIS'
        assert fmt_row.needs_rescore is False
        assert fmt_row.skip_trace_stale is False
        assert fmt_row.phone_1 == '8135551234'

        changed = pg_session.execute(text("""
            SELECT p.sync_status, p.needs_rescore, p.gohighlevel_contact_id,
                   o.owner_name, o.phone_1, o.skip_trace_stale,
                   f.assessed_value_mkt
            FROM properties p
            JOIN owners o ON o.property_id = p.id
            JOIN financials f ON f.property_id = p.id
            WHERE p.county_id = :c AND p.parcel_id = 'ZZT001'"""),
            {"c": self.COUNTY}).one()
        assert changed.owner_name == 'JONES MARY'
        assert float(changed.assessed_value_mkt) == 175000.0
        assert changed.needs_rescore is True
        assert changed.sync_status == 'pending_sync'
        # Enriched fields kept, trace flagged stale instead of cleared
        assert changed.gohighlevel_contact_id == 'zzt-ghl-1'
        assert changed.phone_1 == '8135551234'
        assert changed.skip_trace_stale is True

        # Unchanged parcel: stamped but not flagged
        stable_row = pg_session.execute(text("""
            SELECT needs_rescore, last_seen_at FROM properties
            WHERE county_id = :c AND parcel_id = 'ZZT002'"""),
            {"c": self.COUNTY}).one()
        assert stable_row.needs_rescore is False
        assert stable_row.last_seen_at is not None

        # Missing parcel: last_seen_at NOT advanced past run 1
        gone = pg_session.execute(text("""
            SELECT last_seen_at FROM properties
            WHERE county_id = :c AND parcel_id = 'ZZT003'"""),
            {"c": self.COUNTY}).one()
        assert gone.last_seen_at < stable_row.last_seen_at

        # ── run 3: identical file is a complete no-op ────────────────────
        stats3 = self._run(
            pg_session, self._write_csv(tmp_path, [sold, stable, fmt_comma]),
            monkeypatch,
        )
        assert stats3.inserted == 0
        assert stats3.updated == 0
        assert stats3.unchanged == 3
        assert stats3.flagged_rescore == 0

    def test_dry_run_commits_nothing(self, pg_session, tmp_path, monkeypatch):
        from sqlalchemy import text
        row = ['ZZT-101', 'DRY RUN OWNER', '1 DRY ST', 'TAMPA', '33601', 'SFR',
               '1980', '1000', '0.10', 'LOT 9',
               '1 DRY ST', 'TAMPA', 'FL', '33601',
               '100000', '90000', '01/01/2019', '150000']
        stats = self._run(
            pg_session, self._write_csv(tmp_path, [row]), monkeypatch, dry_run=True,
        )
        assert stats.inserted == 1  # counted, not committed
        count = pg_session.execute(text(
            "SELECT count(*) FROM properties WHERE county_id = :c"),
            {"c": self.COUNTY}).scalar()
        assert count == 0
