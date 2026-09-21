"""Stage E — loader persists detail fields when present in DataFrame."""
import pandas as pd
import pytest

from src.loaders.permits import _extract_detail_fields


def _row(**kwargs):
    """Build a minimal DataFrame row-like dict."""
    defaults = {
        "Record Number": "HC-BTR-26-TEST001",
        "Record Type": "Building Trade - Roofing",
        "Address": "123 Main St Tampa FL 33601",
        "Status": "Issued",
        "Date": "01/15/2026",
        "Expiration Date": "01/15/2027",
        "Description": None,
    }
    defaults.update(kwargs)
    df = pd.DataFrame([defaults])
    return df.iloc[0]


def test_extract_detail_fields_all_none_when_absent():
    row = _row()
    fields = _extract_detail_fields(row)
    assert all(v is None for v in fields.values())


def test_extract_detail_fields_reads_contractor_name():
    row = _row(contractor_name="KEVIN WELLS")
    fields = _extract_detail_fields(row)
    assert fields["contractor_name"] == "KEVIN WELLS"


def test_extract_detail_fields_reads_multiple():
    row = _row(
        contractor_name="KEVIN WELLS",
        contractor_license="CFC055692",
        contractor_phone="8135551234",
        owner_name="SMITH JOHN",
    )
    fields = _extract_detail_fields(row)
    assert fields["contractor_name"] == "KEVIN WELLS"
    assert fields["contractor_license"] == "CFC055692"
    assert fields["contractor_phone"] == "8135551234"
    assert fields["owner_name"] == "SMITH JOHN"


def test_extract_detail_fields_empty_string_becomes_none():
    row = _row(contractor_name="   ")
    fields = _extract_detail_fields(row)
    assert fields["contractor_name"] is None


def test_extract_detail_fields_nan_becomes_none():
    import numpy as np
    row = _row(contractor_name=np.nan)
    fields = _extract_detail_fields(row)
    assert fields["contractor_name"] is None
