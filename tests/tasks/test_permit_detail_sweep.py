"""Stage D — permit_detail_sweep unit tests (no network, no DB)."""
import pytest

from src.tasks.permit_detail_sweep import _pasco_detail_url, _update_permit
from src.scrappers.permit.detail_parse import PermitDetail


# ---------------------------------------------------------------------------
# _pasco_detail_url
# ---------------------------------------------------------------------------

def test_pasco_url_valid():
    url = _pasco_detail_url("REC26-00000-01VUT")
    assert "capID1=REC26" in url
    assert "capID2=00000" in url
    assert "capID3=01VUT" in url
    assert "agencyCode=PASCO" in url


def test_pasco_url_valid_second_sample():
    url = _pasco_detail_url("REC26-00000-01VUR")
    assert "capID1=REC26" in url
    assert "capID3=01VUR" in url


def test_pasco_url_returns_none_for_bad_format():
    assert _pasco_detail_url("HC-BTR-26-0338191-EXTRA") is None


def test_pasco_url_returns_none_for_two_segment():
    assert _pasco_detail_url("HC-BTR26") is None
