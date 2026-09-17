"""WP-T2-8 Stage B — permit principal resolution (fresh_db).

Covers the review finding that contractor-only permits were never resolved:
the extractor now resolves COALESCE(holder_name, contractor_name).
"""
from sqlalchemy import text

from src.services.buyer_entity_resolution import extract_permit_candidates


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
