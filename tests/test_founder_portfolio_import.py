"""B0-01 — founder portfolio import (pure helpers; no DB)."""
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
