"""B0-01 — founder portfolio import (pure helpers + DB orchestrator)."""
import uuid

import pytest

from src.services import founder_portfolio_import as fpi


def test_bucket_for_profit_thresholds():
    assert fpi.bucket_for_profit(5000) == "5_10k"
    assert fpi.bucket_for_profit(9999) == "5_10k"
    assert fpi.bucket_for_profit(10000) == "10_25k"
    assert fpi.bucket_for_profit(25000) == "10_25k"
    assert fpi.bucket_for_profit(25001) == "25k_plus"


def test_stage_for_outcome():
    assert fpi.stage_for_outcome("won") == "closed_won"
    assert fpi.stage_for_outcome("WON") == "closed_won"
    assert fpi.stage_for_outcome("lost") == "closed_lost"
    with pytest.raises(ValueError):
        fpi.stage_for_outcome("maybe")


def test_source_ref_is_stable_and_distinct():
    a = fpi.source_ref(parcel_id="U-1", address="1 Main", deal_date="2024-03-14", profit_amount=18500)
    b = fpi.source_ref(parcel_id="U-1", address="1 Main", deal_date="2024-03-14", profit_amount=18500)
    c = fpi.source_ref(parcel_id="U-2", address="1 Main", deal_date="2024-03-14", profit_amount=18500)
    assert a == b          # deterministic
    assert a != c          # keyed on identity
    assert len(a) <= 64    # fits the column


def test_parse_rows_validates_and_flags_bad_rows():
    csv_text = (
        "parcel_id,address,city,zip,deal_date,profit_amount,outcome,vertical,days_to_close,notes\n"
        ",123 Main St,Tampa,33607,2024-03-14,18500,won,fix_flip,21,BRRRR\n"      # ok
        ",,,,,,,,,\n"                                                             # empty → error
        ",9 Oak,Tampa,33607,2024-03-14,NaN,won,fix_flip,,\n"                      # bad amount → error
        ",5 Elm,Tampa,33607,2024-03-14,12000,won,not_a_vertical,,\n"             # bad vertical → error
    )
    ok, errors = fpi.parse_rows(csv_text)
    assert len(ok) == 1
    assert ok[0]["deal_size_bucket"] == "10_25k"
    assert ok[0]["pipeline_stage"] == "closed_won"
    assert ok[0]["vertical"] == "fix_flip"
    assert len(errors) == 3           # empty, bad amount, bad vertical


def test_lost_deal_gets_skip_bucket():
    """A lost deal is the 'skip' loss sentinel, not a dollar bucket — so
    pipeline_stage and deal_size_bucket never contradict each other."""
    csv_text = (
        "parcel_id,address,city,zip,deal_date,profit_amount,outcome,vertical,days_to_close,notes\n"
        ",1 Loss St,Tampa,33607,2024-03-14,0,lost,fix_flip,,walked away\n"
    )
    ok, errors = fpi.parse_rows(csv_text)
    assert not errors
    assert len(ok) == 1
    assert ok[0]["pipeline_stage"] == "closed_lost"
    assert ok[0]["deal_size_bucket"] == "skip"


def test_bad_days_to_close_is_a_hard_error():
    """A present-but-invalid days_to_close errors the row (not silently None)."""
    csv_text = (
        "parcel_id,address,city,zip,deal_date,profit_amount,outcome,vertical,days_to_close,notes\n"
        ",2 Main St,Tampa,33607,2024-03-14,18500,won,fix_flip,soon,\n"
    )
    ok, errors = fpi.parse_rows(csv_text)
    assert not ok
    assert len(errors) == 1
    assert "days_to_close" in errors[0]["reason"]


def test_import_portfolio_matches_upserts_and_reports(fresh_db):
    """Matched row imports as founder_verified w/ property_id; unmatched reported;
    re-run is idempotent (update, not duplicate)."""
    from src.core.models import DealOutcome, Property

    uid = uuid.uuid4().hex[:8]
    parcel = f"FND-{uid}"
    prop = Property(
        parcel_id=parcel, address=f"{uid} Founder Way",
        city="Tampa", state="FL", zip="33607", county_id="hillsborough",
    )
    fresh_db.add(prop)
    fresh_db.flush()
    fresh_db.commit()

    csv_text = (
        "parcel_id,address,city,zip,deal_date,profit_amount,outcome,vertical,days_to_close,notes\n"
        f"{parcel},{uid} Founder Way,Tampa,33607,2024-03-14,18500,won,fix_flip,21,BRRRR\n"
        f",NOWHERE {uid} nonexistent,Nowhere,00000,2024-04-01,30000,won,wholesalers,,\n"  # unmatched
    )

    summary = fpi.import_portfolio(fresh_db, csv_text)
    assert summary["imported"] == 1
    assert summary["updated"] == 0
    assert len(summary["unmatched"]) == 1
    assert not summary["errors"]

    row = fresh_db.execute(
        DealOutcome.__table__.select().where(DealOutcome.property_id == prop.id)
    ).mappings().first()
    assert row["confidence_tier"] == "founder_verified"
    assert row["outcome_source"] == "founder_import"
    assert row["subscriber_id"] is None
    assert row["source_ref"] is not None

    # Re-run same file → idempotent update, no duplicate.
    summary2 = fpi.import_portfolio(fresh_db, csv_text)
    assert summary2["imported"] == 0
    assert summary2["updated"] == 1
    n = fresh_db.execute(
        DealOutcome.__table__.select().where(DealOutcome.property_id == prop.id)
    ).all()
    assert len(n) == 1

    # cleanup
    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.property_id == prop.id))
    fresh_db.delete(prop)
    fresh_db.commit()
