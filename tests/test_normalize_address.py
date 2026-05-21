"""Regression tests for BaseLoader.normalize_address per-county city strip."""
from unittest.mock import patch

import pytest

from src.loaders.base import BaseLoader


HILLS_TOKENS = [
    'tampa', 'riverview', 'valrico', 'gibsonton', 'lithia',
    'brandon', 'seffner', 'plant city', 'sun city center',
    'thonotosassa', 'odessa', 'lutz', 'ruskin', 'wimauma',
    'apollo beach', 'dover', 'citrus park', 'new tampa',
]
PINELLAS_TOKENS = [
    'st petersburg', 'saint petersburg', 'clearwater', 'largo',
    'pinellas park', 'dunedin', 'tarpon springs', 'palm harbor',
    'seminole', 'oldsmar', 'safety harbor', 'gulfport',
    'treasure island', 'tierra verde', 'south pasadena',
    'st pete beach', 'belleair', 'kenneth city',
]


def _fake_get_county_config(county_id: str):
    return {
        "hillsborough": {"address_city_tokens": HILLS_TOKENS},
        "pinellas":     {"address_city_tokens": PINELLAS_TOKENS},
    }[county_id]


@pytest.fixture
def patched_config():
    with patch("src.utils.county_config.get_county_config", side_effect=_fake_get_county_config) as p:
        yield p


def test_hillsborough_strips_tampa(patched_config):
    assert BaseLoader.normalize_address("5017 LOWELL RD TAMPA 33624", "hillsborough") == "5017 lowell rd"


def test_hillsborough_strips_multiword_sun_city_center(patched_config):
    assert BaseLoader.normalize_address("123 MAIN ST SUN CITY CENTER 33573", "hillsborough") == "123 main st"


def test_pinellas_strips_st_petersburg(patched_config):
    assert BaseLoader.normalize_address("2448 45TH ST S ST PETERSBURG 33711", "pinellas") == "2448 45th st s"


def test_pinellas_strips_clearwater(patched_config):
    # USPS abbreviation for WAY is WAY (WY = Wyoming, the state) — fixed in
    # the shared normalize_street_address module.
    assert BaseLoader.normalize_address("1340 HOMESTEAD WAY CLEARWATER", "pinellas") == "1340 homestead way"


def test_pinellas_input_with_other_county_city(patched_config):
    # usaddress strips well-known city tokens regardless of county config.
    # Cross-county isolation is enforced at the SQL filter level
    # (Property.county_id == self.county_id), not the normalizer.
    out = BaseLoader.normalize_address("100 MAIN ST TAMPA", "pinellas")
    assert out == "100 main st"


def test_hillsborough_input_with_other_county_city(patched_config):
    out = BaseLoader.normalize_address("100 MAIN ST CLEARWATER", "hillsborough")
    assert out == "100 main st"


def test_unknown_county_falls_back_to_usaddress_strip(patched_config):
    # KeyError from config lookup → tokens default to [] → usaddress
    # still strips PlaceName components.
    out = BaseLoader.normalize_address("100 MAIN ST TAMPA", "nonexistent_county")
    assert out == "100 main st"


def test_none_county_id_falls_back_to_usaddress_strip(patched_config):
    out = BaseLoader.normalize_address("100 MAIN ST TAMPA", None)
    assert out == "100 main st"


def test_longest_token_wins(patched_config):
    # "sun city center" must beat any shorter overlapping token.
    out = BaseLoader.normalize_address("1 ELM RD SUN CITY CENTER", "hillsborough")
    assert out == "1 elm rd"


def test_abbreviations_still_applied(patched_config):
    # Verifies street-suffix replacement still runs regardless of city tokens.
    out = BaseLoader.normalize_address("123 OAK STREET TAMPA", "hillsborough")
    assert out == "123 oak st"


def test_empty_and_invalid_unchanged(patched_config):
    assert BaseLoader.normalize_address("", "hillsborough") == ""
    assert BaseLoader.normalize_address("RIGHT OF WAY", "hillsborough") == ""
