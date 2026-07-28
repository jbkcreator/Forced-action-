from src.scrappers.dbpr.dbpr_engine import _map_vertical


def test_map_vertical_cvc_is_solar():
    assert _map_vertical("CVC") == "solar"


def test_map_vertical_unknown_returns_none():
    assert _map_vertical("ZZZ") is None
