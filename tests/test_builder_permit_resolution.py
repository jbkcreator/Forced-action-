"""WP-T2-8 Stage B — permit principal resolution (fresh_db).

Covers the review finding that contractor-only permits were never resolved:
the extractor now resolves COALESCE(holder_name, contractor_name).
"""
from sqlalchemy import text

from unittest import mock

from src.services.buyer_entity_resolution import (
    _build_exceptions_blocks,
    _build_exceptions_message,
    _emit_exceptions_alerts,
    _infer_entity_type_from_name,
    extract_permit_candidates,
)


def test_exceptions_message_fallback_renders_links():
    msg = _build_exceptions_message([
        ("permit_staging", 1, "ACME HOMES LLC", 62, 900),
        ("building_permits", 2, "BUILDPRO INC", 55, 901),
    ])
    assert "2 low-confidence link(s) need review" in msg
    assert "ACME HOMES LLC" in msg
    assert "confidence 62 (unverified)" in msg


def test_exceptions_blocks_structure():
    blocks = _build_exceptions_blocks([
        ("permit_staging", 1, "ACME HOMES LLC", 62, 900),   # 🟠
        ("building_permits", 2, "BUILDPRO INC", 55, 901),   # 🔴
    ])
    assert blocks[0]["type"] == "header"
    assert "Builder Permit Resolution" in blocks[0]["text"]["text"]
    # one section per link, each with the two-field confidence/status layout
    sections = [b for b in blocks if b["type"] == "section"]
    assert len(sections) == 2
    assert "*ACME HOMES LLC*" in sections[0]["text"]["text"]
    assert any("Confidence" in f["text"] for f in sections[0]["fields"])
    assert "🟠" in sections[0]["text"]["text"]   # 62 → below floor, not critical
    assert "🔴" in sections[1]["text"]["text"]   # 55 → critical


def test_exceptions_blocks_cap_and_overflow():
    links = [("permit_staging", i, f"BUILDER {i} LLC", 60, 1000 + i) for i in range(20)]
    blocks = _build_exceptions_blocks(links)
    sections = [b for b in blocks if b["type"] == "section"]
    assert len(sections) == 15                       # capped
    assert any("5" in e["text"] and "more" in e["text"]
               for b in blocks if b["type"] == "context"
               for e in b["elements"])               # overflow note present


def test_exceptions_alert_noops_when_slack_unconfigured():
    # No token/channel → must not raise, must not attempt a post.
    fake = mock.MagicMock()
    fake.slack_bot_token = None
    fake.fa_max_slack_channel_exceptions = ""
    with mock.patch("src.services.buyer_entity_resolution.get_settings", return_value=fake):
        with mock.patch("slack_sdk.WebClient") as web:
            _emit_exceptions_alerts([("permit_staging", 1, "X LLC", 60, 1)])
            web.assert_not_called()


def _staging(db, permit_number, *, holder=None, contractor=None, county="hillsborough"):
    return db.execute(
        text("""
            INSERT INTO permit_staging
                (permit_number, permit_type, county_id, holder_name, contractor_name,
                 completion_status, status, issue_date, matched)
            VALUES (:pn, 'New Construction', :cty, :h, :c, 'issued', 'issued', CURRENT_DATE, FALSE)
            RETURNING id
        """),
        {"pn": permit_number, "cty": county, "h": holder, "c": contractor},
    ).fetchone().id


def test_contractor_only_permit_is_extracted(fresh_db):
    db = fresh_db
    sid = _staging(db, "CONLY-1", holder=None, contractor="BUILDPRO INC")
    db.flush()

    cands = [c for c in extract_permit_candidates(db, only_unresolved=True)
             if c.source_table == "permit_staging" and c.source_id == sid]
    assert len(cands) == 1
    assert cands[0].raw_name == "BUILDPRO INC"   # contractor used as principal


def test_holder_preferred_over_contractor(fresh_db):
    db = fresh_db
    sid = _staging(db, "BOTH-1", holder="ACME HOMES LLC", contractor="BUILDPRO INC")
    db.flush()

    cands = [c for c in extract_permit_candidates(db, only_unresolved=True)
             if c.source_table == "permit_staging" and c.source_id == sid]
    assert len(cands) == 1
    assert cands[0].raw_name == "ACME HOMES LLC"  # holder preferred


def test_permit_with_no_party_is_skipped(fresh_db):
    db = fresh_db
    sid = _staging(db, "NONE-1", holder=None, contractor=None)
    db.flush()

    cands = [c for c in extract_permit_candidates(db, only_unresolved=True)
             if c.source_table == "permit_staging" and c.source_id == sid]
    assert cands == []   # no holder, no contractor → not a candidate (no "nan")


def test_entity_type_inference_from_name():
    assert _infer_entity_type_from_name("ACME HOMES LLC") == "LLC"
    assert _infer_entity_type_from_name("BUILDPRO INC") == "Corporate"
    assert _infer_entity_type_from_name("SMITH FAMILY REVOCABLE TRUST") == "Trust"
    assert _infer_entity_type_from_name("JOHN Q SMITH") == "Individual"
    assert _infer_entity_type_from_name("") == "Individual"


def test_permit_company_gets_company_entity_type(fresh_db):
    db = fresh_db
    sid = _staging(db, "TYPE-1", holder="ACME HOMES LLC")
    db.flush()

    cand = next(c for c in extract_permit_candidates(db, only_unresolved=True)
                if c.source_table == "permit_staging" and c.source_id == sid)
    assert cand.entity_type_hint == "LLC"   # not the old hardcoded "Individual"


def test_staged_permit_is_removed_when_it_later_matches(fresh_db):
    """Stage-then-rematch: a permit first staged, then ingested again and matched
    to a property, must not leave a duplicate staging row (double-count guard)."""
    from types import MethodType, SimpleNamespace
    from unittest.mock import Mock
    from datetime import date
    import pandas as pd
    from src.loaders.permits import BuildingPermitLoader

    db = fresh_db
    # permit first exists only in staging (no property match yet)
    sid = _staging(db, "REMATCH-1", holder="STAGED HOLDER LLC")
    db.flush()
    assert db.execute(
        text("SELECT COUNT(*) AS n FROM permit_staging WHERE permit_number='REMATCH-1'")
    ).fetchone().n == 1

    # same permit re-ingested; this time it matches a property
    prop = db.execute(
        text("INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) "
             "VALUES ('REMATCH-PARCEL', false, now(), now()) RETURNING id")
    ).fetchone().id
    loader = SimpleNamespace(
        session=db,
        county_id="hillsborough",
        _thresholds=SimpleNamespace(address_floor=75),
        find_property_by_address=Mock(return_value=(SimpleNamespace(id=prop), 100)),
        parse_date=Mock(return_value=date(2026, 9, 1)),
        safe_add=Mock(return_value=True),
    )
    loader._promote_from_staging = MethodType(BuildingPermitLoader._promote_from_staging, loader)
    frame = pd.DataFrame([{
        "Record Number": "REMATCH-1",
        "Record Type": "New Construction",
        "Status": "Issued",
        "Address": "1 Test Way, Tampa, FL 33602",
        "Date": "2026-09-01",
        "Expiration Date": "2027-09-01",
    }])
    # skip_duplicates=False → straight to the match branch (no building_permits dup SELECT)
    BuildingPermitLoader.load_from_dataframe(loader, frame, skip_duplicates=False)

    remaining = db.execute(
        text("SELECT COUNT(*) AS n FROM permit_staging WHERE permit_number='REMATCH-1'")
    ).fetchone().n
    assert remaining == 0   # staging row removed on promotion
