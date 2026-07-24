"""
Hero deal service tests — T-B12-01.

Unit tests mock the DB session; no Postgres required.
Run: pytest tests/test_hero_deal.py -v
"""
from unittest.mock import MagicMock

from src.services.hero_deal import _serialize_deal, get_hero_deal


def _make_prop(**kwargs):
    prop = MagicMock()
    prop.id = kwargs.get("id", 1)
    prop.address = kwargs.get("address", "1234 Oak Street")
    prop.city = kwargs.get("city", "Tampa")
    prop.state = kwargs.get("state", "FL")
    prop.zip = kwargs.get("zip", "33612")
    prop.county_id = kwargs.get("county_id", "hillsborough")
    return prop


def _make_score(**kwargs):
    score = MagicMock()
    score.final_cds_score = kwargs.get("final_cds_score", 88.0)
    score.vertical_scores = kwargs.get("vertical_scores", {"roofing": 90.0})
    score.distress_types = kwargs.get("distress_types", ["foreclosure", "code_violation"])
    score.lead_tier = kwargs.get("lead_tier", "Gold")
    score.urgency_level = kwargs.get("urgency_level", "high")
    return score


class TestSerializeDeal:
    def test_blurs_address_and_omits_pii(self):
        prop = _make_prop()
        score = _make_score()
        result = _serialize_deal(prop, score, "roofing")

        assert result["address_masked"].startswith("1234")
        assert "*" in result["address_masked"]
        assert "phone" not in result
        assert "owner_name" not in result
        assert result["unlocked"] is False
        assert result["vertical_score"] == 90.0
        assert result["distress_types"] == ["foreclosure", "code_violation"]

    def test_dict_distress_types_flattened_to_keys(self):
        prop = _make_prop()
        score = _make_score(distress_types={"foreclosure": True, "lien": True})
        result = _serialize_deal(prop, score, "roofing")
        assert set(result["distress_types"]) == {"foreclosure", "lien"}


class TestGetHeroDeal:
    def test_zip_match_returns_deal(self, monkeypatch):
        prop = _make_prop(zip="33612")
        score = _make_score()

        def fake_query(db, vertical, zip_code=None):
            if zip_code == "33612":
                return (prop, score)
            raise AssertionError("should not query statewide when zip matched")

        monkeypatch.setattr("src.services.hero_deal._query_top_lead", fake_query)

        result = get_hero_deal("33612", "roofing", db=MagicMock())
        assert result["status"] == "zip_match"
        assert result["deal"]["zip"] == "33612"
        assert result["nearest_label"] is None

    def test_nearest_fallback_when_zip_has_no_deal(self, monkeypatch):
        prop = _make_prop(zip="34999", county_id="hillsborough")
        score = _make_score()
        calls = []

        def fake_query(db, vertical, zip_code=None):
            calls.append(zip_code)
            if zip_code == "00000":
                return None
            return (prop, score)

        monkeypatch.setattr("src.services.hero_deal._query_top_lead", fake_query)
        monkeypatch.setattr(
            "src.services.hero_deal.get_county",
            lambda county_id: {"display_name": "Hillsborough"},
        )

        result = get_hero_deal("00000", "roofing", db=MagicMock())
        assert result["status"] == "nearest"
        assert result["nearest_label"] == "Nearest to you: Hillsborough"
        assert calls == ["00000", None]

    def test_empty_when_nothing_statewide(self, monkeypatch):
        monkeypatch.setattr("src.services.hero_deal._query_top_lead", lambda db, vertical, zip_code=None: None)

        result = get_hero_deal("00000", "roofing", db=MagicMock())
        assert result["status"] == "empty"
        assert result["deal"] is None
        assert "00000" in result["message"]
