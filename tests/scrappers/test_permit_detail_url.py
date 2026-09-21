"""Stage A — detail URL derivation: unit tests."""
import pytest
from src.scrappers.permit.detail_url import build_detail_url, parse_cap_ids

BASE = "https://aca-prod.accela.com"


class TestBuildDetailUrl:
    def test_hillsborough_known_permit(self):
        url = build_detail_url("HCFL", "26CAP", "00000", "02829")
        assert "aca-prod.accela.com/HCFL/Cap/CapDetail.aspx" in url
        assert "capID1=26CAP" in url
        assert "capID2=00000" in url
        assert "capID3=02829" in url
        assert "agencyCode=HCFL" in url

    def test_module_and_tabname_present(self):
        url = build_detail_url("HCFL", "26CAP", "00000", "02829")
        assert "Module=Building" in url
        assert "TabName=Building" in url

    def test_different_agency(self):
        url = build_detail_url("PINELLAS", "25BTR", "00001", "11111")
        assert "PINELLAS/Cap/CapDetail.aspx" in url
        assert "agencyCode=PINELLAS" in url

    def test_returns_string(self):
        url = build_detail_url("HCFL", "26CAP", "00000", "02829")
        assert isinstance(url, str)
        assert url.startswith("https://")


class TestParseCapIds:
    def test_full_href_with_query_string(self):
        href = (
            "/HCFL/Cap/CapDetail.aspx?Module=Building&TabName=Building"
            "&capID1=26CAP&capID2=00000&capID3=02829&agencyCode=HCFL"
        )
        result = parse_cap_ids(href)
        assert result is not None
        agency, id1, id2, id3 = result
        assert agency == "HCFL"
        assert id1 == "26CAP"
        assert id2 == "00000"
        assert id3 == "02829"

    def test_href_without_capids_returns_none(self):
        href = "/HCFL/Cap/CapHome.aspx?module=Building"
        assert parse_cap_ids(href) is None

    def test_absolute_url(self):
        href = (
            "https://aca-prod.accela.com/HCFL/Cap/CapDetail.aspx"
            "?Module=Building&capID1=26CAP&capID2=00000&capID3=02829&agencyCode=HCFL"
        )
        result = parse_cap_ids(href)
        assert result is not None
        agency, id1, id2, id3 = result
        assert agency == "HCFL"
        assert id1 == "26CAP"

    def test_empty_href_returns_none(self):
        assert parse_cap_ids("") is None

    def test_roundtrip(self):
        href = (
            "/HCFL/Cap/CapDetail.aspx?Module=Building&TabName=Building"
            "&capID1=26CAP&capID2=00000&capID3=02829&agencyCode=HCFL"
        )
        agency, id1, id2, id3 = parse_cap_ids(href)
        url = build_detail_url(agency, id1, id2, id3)
        assert "capID1=26CAP" in url
        assert "agencyCode=HCFL" in url
