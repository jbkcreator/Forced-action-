from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.lending.milo import mapping
from src.lending.milo.package import PackageError, load_package, package_from_dict
from src.lending.milo.packet import packet_context

TEST_PACKAGE = Path(__file__).resolve().parents[2] / "config" / "lending_milo_test_package.json"


def _data(**over):
    data = {
        "package_ref": "P1", "product": "fix_and_flip",
        "address": {"line1": "1 Main St", "city": "Tampa", "state": "fl", "zip_code": "33602"},
        "purchase_price": "200,000", "rehab": 50000, "arv": 350000,
        "guarantors": [{"name": "A", "fico": 720, "owned_rentals": 2, "fix_flip_exits_24m": 3}],
    }
    data.update(over)
    return data


def test_fix_and_flip_maps_to_milo_step_fields_and_matches_milo_calculations():
    app = mapping.map_package(package_from_dict(_data()))
    assert app.step1["product"] == "Fix & flip / fix to rent"
    assert app.step1["state"] == "FL"
    assert app.step2_property["Purchase Price"] == "200,000"
    assert app.step2_property["Property Type"] == "1-4 Unit"
    assert app.step2_borrower["Fix & Flip Exits (Last 24 Months)"] == "3"
    # Milo showed 250,000 / 100,000 / 1.40 for these inputs.
    assert app.checks == {"Total Project Cost": "250,000", "Gross Profit": "100,000", "Viability Ratio": "1.40"}


def test_pricing_uses_lowest_fico_and_strongest_track_record():
    app = mapping.map_package(package_from_dict(_data(guarantors=[
        {"name": "A", "fico": 790, "fix_flip_exits_24m": 0},
        {"name": "B", "fico": 665, "fix_flip_exits_24m": 6},
    ])))
    assert app.step2_borrower["Est. FICO Score"] == mapping.fico_band(665)
    assert app.step2_borrower["Fix & Flip Exits (Last 24 Months)"] == "6"


def test_unconfirmed_lists_and_missing_comps_are_flagged():
    flags = mapping.map_package(package_from_dict(_data())).flags
    assert mapping.FLAG_FICO_BAND_UNCONFIRMED in flags
    assert mapping.FLAG_NO_COMPS in flags


def test_arv_below_cost_is_flagged():
    assert mapping.FLAG_ARV_BELOW_COST in mapping.map_package(package_from_dict(_data(arv=240000))).flags


@pytest.mark.parametrize("over", [
    {"product": "dscr"},
    {"property_type": "condo_tower"},
])
def test_unmapped_product_or_property_type_is_refused(over):
    with pytest.raises(PackageError):
        mapping.map_package(package_from_dict(_data(**over)))


@pytest.mark.parametrize("over", [
    {"guarantors": []},
    {"arv": None},
    {"purchase_price": "abc"},
    {"guarantors": [{"name": "A", "fico": 900}]},
])
def test_incomplete_package_is_refused(over):
    with pytest.raises(PackageError):
        package_from_dict(_data(**over))


def test_test_package_file_loads_and_maps():
    package = load_package(TEST_PACKAGE)
    assert package.is_test and package.purchase_price == Decimal("200000")
    assert mapping.map_package(package).checks["Viability Ratio"] == "1.40"


def test_packet_context_carries_deal_facts_and_comps():
    package = package_from_dict(_data(comps={"low": "330000", "point": "350000", "high": "365000", "confidence": "medium",
                                             "comps": [{"address": "3 Oak St", "sale_date": "2026-08-01", "sale_price": "352000"}]}))
    ctx = packet_context(package, mapping.map_package(package), today=date(2026, 11, 17))
    assert ("Gross profit", "$100,000") in ctx["deal_rows"]
    assert ctx["comps"]["comp_count"] == 1 and ctx["comps"]["comps"][0]["address"] == "3 Oak St"
