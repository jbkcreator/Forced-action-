"""WP-7 WI-2 — pre-fill assembly: field allowlist, missing-field degradation,
source labels. Uses `fresh_db` (real Postgres, rolled back after each test)."""
from sqlalchemy import text

from src.services.prefill_assembly import assemble_prefill

_BANNED_KEYS = {
    "estimated_income", "credit_score_tier", "phone_1", "phone_2", "phone_3",
    "email_1", "email_2", "id", "parcel_id", "arv", "est_repair_cost",
}


def _make_property(db, county_id="hillsborough"):
    row = db.execute(
        text(
            "INSERT INTO properties (parcel_id, address, city, state, zip, county_id, "
            "beds, baths, sq_ft, year_built, created_at, updated_at) "
            "VALUES (:parcel, '123 Test St', 'Tampa', 'FL', '33601', :county, 3, 2, 1500, 1990, now(), now()) "
            "RETURNING id"
        ),
        {"parcel": f"TEST-{id(db)}", "county": county_id},
    ).first()
    return row.id


def test_known_property_returns_allowlisted_fields_only(fresh_db):
    prop_id = _make_property(fresh_db)
    fresh_db.execute(
        text("INSERT INTO owners (property_id, owner_name, mailing_address, estimated_income, credit_score_tier) "
             "VALUES (:pid, 'JOHN SMITH', '456 Mail Ave', 90000, 'A')"),
        {"pid": prop_id},
    )
    fresh_db.execute(
        text("INSERT INTO financials (property_id, assessed_value_mkt, arv) VALUES (:pid, 250000, 400000)"),
        {"pid": prop_id},
    )
    fresh_db.flush()

    payload = assemble_prefill(fresh_db, prop_id).to_dict()
    fields = payload["fields"]

    assert fields["owner_name"]["value"] == "JOHN SMITH"
    assert fields["tax_assessed_value"]["value"] == 250000
    for banned in _BANNED_KEYS:
        assert banned not in fields, f"banned field {banned!r} leaked into prefill payload"


def test_missing_property_returns_empty_payload_not_error(fresh_db):
    payload = assemble_prefill(fresh_db, property_id=999_999_999).to_dict()
    assert payload["fields"] == {}


def test_property_with_no_deed_or_permit_has_absent_sections(fresh_db):
    prop_id = _make_property(fresh_db)
    fresh_db.flush()

    payload = assemble_prefill(fresh_db, prop_id).to_dict()
    fields = payload["fields"]
    assert "last_deed_transfer" not in fields
    assert "open_permits" not in fields
    # Core property facts still present — the page still reads as complete.
    assert fields["address"]["value"] == "123 Test St"


def test_borrower_screen_escapes_scraped_values(fresh_db):
    """Owner/address values come from scraped county portals — they must never
    be interpolated into the borrower page unescaped."""
    from src.api.selfserve_router import _render_field_row

    row = _render_field_row("owner_name", '"><script>alert(1)</script>')
    assert "<script>" not in row
    assert "&lt;script&gt;" in row
