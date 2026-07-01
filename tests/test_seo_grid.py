"""Unit tests for SEO grid pure functions (city_to_slug, vertical_to_slug, is_eligible)."""
from src.services.seo.grid import city_to_slug, vertical_to_slug, is_eligible, VERTICALS


# --- is_eligible ---

def test_is_eligible_at_floor():
    assert is_eligible(25, 25) is True


def test_is_eligible_above_floor():
    assert is_eligible(100, 25) is True


def test_is_eligible_one_below_floor():
    assert is_eligible(24, 25) is False


def test_is_eligible_zero():
    assert is_eligible(0, 25) is False


# --- city_to_slug ---

def test_city_to_slug_simple():
    assert city_to_slug("Tampa") == "tampa"


def test_city_to_slug_two_words():
    assert city_to_slug("Plant City") == "plant-city"


def test_city_to_slug_st_pete():
    assert city_to_slug("St. Petersburg") == "st-petersburg"


def test_city_to_slug_tarpon_springs():
    assert city_to_slug("Tarpon Springs") == "tarpon-springs"


def test_city_to_slug_leading_trailing_whitespace():
    assert city_to_slug("  Tampa  ") == "tampa"


def test_city_to_slug_already_lower():
    assert city_to_slug("brandon") == "brandon"


# --- vertical_to_slug ---

def test_vertical_to_slug_fix_flip():
    assert vertical_to_slug("fix_flip") == "fix-flip"


def test_vertical_to_slug_public_adjusters():
    assert vertical_to_slug("public_adjusters") == "public-adjusters"


def test_vertical_to_slug_no_underscore():
    assert vertical_to_slug("wholesalers") == "wholesalers"


def test_vertical_to_slug_attorneys():
    assert vertical_to_slug("attorneys") == "attorneys"


# --- VERTICALS constant ---

def test_verticals_has_six():
    assert len(VERTICALS) == 6


def test_verticals_contains_all_expected():
    expected = {"wholesalers", "fix_flip", "restoration", "roofing", "public_adjusters", "attorneys"}
    assert set(VERTICALS) == expected
