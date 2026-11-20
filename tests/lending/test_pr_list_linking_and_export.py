"""Calling-list records linked to Forced Action properties, and the daily Drive export ("FA Exports").

No network, no database: the database and Drive are recorded in memory.
"""
from __future__ import annotations

import csv
import io
import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

from src.lending import pr_list_linking as linking
from src.tasks import lending_pr_daily_export as export

# ── linking ─────────────────────────────────────────────────────────────────


def test_a_three_digit_fips_is_a_florida_county():
    assert linking.full_fips("057") == "12057" and linking.full_fips("13121") == "13121"


def test_county_names_include_propertyradar_aliases():
    from config.property_radar_fips import county_fips

    names = linking.county_names_by_fips()
    assert ("FL", "HILLSBOROUGH") in names["12057"]
    assert ("FL", "DADE") in names[county_fips("FL", "MIAMI-DADE")]


class FakeLoader:
    def __init__(self, result):
        self.result = result

    def find_property_cascade(self, **kwargs):
        return self.result


ROW = {"id": 7, "apn": "A1", "property_address": "1 Main St", "zip": "33602", "city": "Tampa"}


def test_a_parcel_match_links_with_its_confidence_as_a_fraction():
    outcome, link = linking.match_record(FakeLoader((SimpleNamespace(id=42, county_id="hillsborough"), "parcel_id", 100)),
                                         "hillsborough", ROW)
    assert outcome == linking.LINKED
    assert link == {"id": 7, "property_id": 42, "match_method": "parcel_id", "match_confidence": 1.0}


def test_no_match_is_stamped_so_the_record_is_not_tried_every_night():
    outcome, link = linking.match_record(FakeLoader((None, None, None)), "hillsborough", ROW)
    assert outcome == linking.NO_MATCH and link["property_id"] is None and link["match_method"] == "no_match"


def test_a_match_in_another_county_is_never_linked():
    outcome, link = linking.match_record(FakeLoader((SimpleNamespace(id=9, county_id="pinellas"), "address", 95)),
                                         "hillsborough", ROW)
    assert outcome == linking.COUNTY_MISMATCH and link["property_id"] is None


class LinkDb:
    """Answers the three statements in order: linkable counties, one page of records, the update."""

    def __init__(self, counties, records):
        self.counties, self.records, self.updates, self.fetch_params = counties, records, [], []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM counties" in sql:
            return [SimpleNamespace(county_id=slug, fips=fips) for slug, fips in self.counties]
        if "FROM lending.pr_list_records" in sql:
            self.fetch_params.append(params)
            page, self.records = self.records, []
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: page))
        self.updates.append(json.loads(params["rows"]))
        return None


def test_only_counties_with_parcels_are_linked_and_the_linkable_fips_are_reported():
    record = {**ROW, "state": "FL", "county": "HILLSBOROUGH"}
    db = LinkDb(counties=[("hillsborough", "12057")], records=[record])
    summary = linking.link_unlinked_records(
        db, loader_for=lambda slug: FakeLoader((SimpleNamespace(id=42, county_id=slug), "address", 96)))
    assert summary["linked"] == 1 and summary["linkable_county_fips"] == ["12057"]
    assert "FL:HILLSBOROUGH" in db.fetch_params[0]["keys"] and not any(k.startswith("GA:") for k in db.fetch_params[0]["keys"])
    assert db.updates == [[{"id": 7, "property_id": 42, "match_method": "address", "match_confidence": 0.96}]]


def test_nothing_is_fetched_when_no_county_has_parcels():
    db = LinkDb(counties=[], records=[{**ROW, "state": "GA", "county": "FULTON"}])
    summary = linking.link_unlinked_records(db, loader_for=lambda slug: FakeLoader((None, None, None)))
    assert summary == {"linked": 0, "no_match": 0, "county_mismatch": 0, "linkable_county_fips": []}
    assert db.fetch_params == []


# ── daily Drive export ──────────────────────────────────────────────────────

DAY = date(2026, 11, 20)


def _row(**values):
    return {**dict.fromkeys(export.COLUMNS), "radar_id": "R1", "state": "FL", "phone_last4": "0142", **values}


def test_the_export_never_holds_a_full_phone_or_an_email():
    assert not {"phone", "email", "phones", "emails"} & set(export.COLUMNS)
    files = export.build_files({"verified_maturity": [_row()]})
    text_ = files["maturing_loans.csv"].decode()
    rows = list(csv.DictReader(io.StringIO(text_)))
    assert rows[0]["phone_last4"] == "0142" and "@" not in text_


def test_every_signal_gets_its_file_even_when_empty():
    files = export.build_files({})
    assert set(files) == {"maturing_loans.csv", "cash_purchases.csv", "investor_purchases.csv"}
    assert all(data.decode().splitlines()[0].startswith("radar_id,state") for data in files.values())


def test_the_manifest_is_removed_first_and_uploaded_last(monkeypatch):
    monkeypatch.setattr(export, "collect", lambda db, day: {"verified_maturity": [_row()], "cash_buyer": [_row(), _row()]})
    calls = []
    counts = export.run(None, day=DAY, folder_id="FOLDER", now=datetime(2026, 11, 20, 15, tzinfo=timezone.utc),
                        upload=lambda folder, sub, name, data: calls.append(("upload", sub, name)),
                        remove=lambda folder, sub, name: calls.append(("remove", sub, name)))
    assert calls[0] == ("remove", "2026-11-20", "manifest.json")
    assert calls[-1] == ("upload", "2026-11-20", "manifest.json")
    assert counts == {"maturing_loans.csv": 1, "cash_purchases.csv": 2, "investor_purchases.csv": 0}


def test_a_dry_run_uploads_nothing(monkeypatch):
    monkeypatch.setattr(export, "collect", lambda db, day: {})
    export.run(None, day=DAY, folder_id="", now=datetime.now(timezone.utc), dry_run=True,
               upload=lambda *a: (_ for _ in ()).throw(AssertionError("uploaded on a dry run")))


def test_the_day_is_the_et_night_the_pipeline_ran():
    start, end = export.day_window(DAY)
    assert (start.isoformat(), end.isoformat()) == ("2026-11-20T05:00:00+00:00", "2026-11-21T05:00:00+00:00")


def test_the_export_is_off_without_the_switch_and_folder(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_pr_export_enabled", False, raising=False)
    monkeypatch.setattr(export, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("ran while off")))
    assert export.main([]) == 0
