"""
Integration tests — real Postgres, no mocks.

1. DB config read: counties.address_city_tokens reaches get_county_config().
2. Pinellas loader smoke: a real property's address, decorated with a city
   suffix the way scraped source records embed it, round-trips through
   find_property_by_address() and resolves to the original Property row.
"""
import pytest
from sqlalchemy import text

from src.loaders.base import BaseLoader
from src.utils.county_config import get_county_config, invalidate_cache


class _SmokeLoader(BaseLoader):
    """Minimal concrete loader so BaseLoader can be instantiated."""
    def load_from_dataframe(self, df, skip_duplicates=True):
        return (0, 0, 0)


def test_db_config_returns_address_city_tokens(fresh_db):
    """Real DB read — column is plumbed through get_county_config()."""
    invalidate_cache()  # force a fresh DB hit
    pin = get_county_config("pinellas")
    hil = get_county_config("hillsborough")

    pin_tokens = pin.get("address_city_tokens")
    hil_tokens = hil.get("address_city_tokens")

    assert isinstance(pin_tokens, list) and pin_tokens, "pinellas tokens missing"
    assert isinstance(hil_tokens, list) and hil_tokens, "hillsborough tokens missing"

    # Spot-check expected entries
    assert "st petersburg" in pin_tokens
    assert "clearwater" in pin_tokens
    assert "tampa" in hil_tokens
    # Cross-county isolation
    assert "tampa" not in pin_tokens
    assert "clearwater" not in hil_tokens


def test_pinellas_loader_round_trips_address(fresh_db):
    """
    Pull one real pinellas property, simulate a source record by appending
    a city suffix to its address, and assert find_property_by_address resolves
    back to the same property — proving the new DB-backed strip works end to
    end through the matching waterfall.
    """
    invalidate_cache()

    row = fresh_db.execute(text("""
        select id, address, zip
        from properties
        where county_id = 'pinellas'
          and address is not null
          and address ~ '^[0-9]+ '
          and zip is not null
        limit 1
    """)).first()
    assert row is not None, "no pinellas properties available for smoke test"

    prop_id, street_addr, zip_code = row

    # Decorate the way source records do: street + city + zip
    source_style = f"{street_addr} ST PETERSBURG {zip_code}"

    loader = _SmokeLoader(session=fresh_db, county_id="pinellas")

    # 1. normalize_address strips the appended city via DB-backed tokens
    normalized = loader.normalize_address(source_style, "pinellas")
    assert "st petersburg" not in normalized
    assert "petersburg" not in normalized
    assert normalized  # non-empty

    # 2. Cross-county sanity: Hillsborough tokens must NOT strip "st petersburg"
    hil_norm = loader.normalize_address(source_style, "hillsborough")
    assert "petersburg" in hil_norm, "hillsborough tokens incorrectly stripped a pinellas city"

    # 3. End-to-end: matching waterfall resolves source_style → original property
    match = loader.find_property_by_address(source_style, threshold=85, zip_code=zip_code)
    assert match is not None, f"no match for source-style address {source_style!r}"
    matched_prop, score = match
    assert matched_prop.id == prop_id, (
        f"matched wrong property: expected id={prop_id}, got id={matched_prop.id} "
        f"(score={score}, addr={matched_prop.address!r})"
    )
