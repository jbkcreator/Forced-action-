"""
Unit tests for TaxDelinquencyLoader._build_tax_values' derived-field logic
(fix/july-three-2026) — no DB required.

Regression coverage for a falsy-zero bug: `values.get("account_balance_amount")
or values.get("face_amount")` treated a real $0.00 account balance as absent
(0.0 is falsy in Python) and substituted the certificate's original face
amount instead, making a paid-down-to-zero account look like it still owed
the full face amount.
"""

import pandas as pd

from src.loaders.tax import TaxDelinquencyLoader


def _bare_loader(county_id: str = "hillsborough") -> TaxDelinquencyLoader:
    """Construct a loader without __init__ (no DB session needed for _build_tax_values)."""
    loader = TaxDelinquencyLoader.__new__(TaxDelinquencyLoader)
    loader.county_id = county_id
    return loader


def test_zero_account_balance_is_not_overwritten_by_face_amount():
    """A real $0.00 balance (e.g. a paid-down certificate) must be preserved,
    not replaced by the larger, stale face_amount."""
    loader = _bare_loader()
    row = pd.Series({
        "Account Balance Amount": "0.00",
        "Face Amount": "1200.00",
        "Tax Yr": "2020",
    })
    values = loader._build_tax_values(row)
    assert values["total_amount_due"] == 0.0


def test_missing_account_balance_falls_back_to_face_amount():
    loader = _bare_loader()
    row = pd.Series({"Face Amount": "1200.00", "Tax Yr": "2020"})
    values = loader._build_tax_values(row)
    assert values["total_amount_due"] == 1200.0


def test_explicit_total_amount_due_is_never_overwritten():
    loader = _bare_loader()
    row = pd.Series({
        "Total Due": "999.00",
        "Account Balance Amount": "0.00",
        "Face Amount": "1200.00",
        "Tax Yr": "2020",
    })
    values = loader._build_tax_values(row)
    assert values["total_amount_due"] == 999.0


def test_years_delinquent_derived_from_tax_year_when_absent():
    loader = _bare_loader()
    row = pd.Series({"Face Amount": "1200.00", "Tax Yr": "2020"})
    values = loader._build_tax_values(row)
    assert values["years_delinquent"] > 0
