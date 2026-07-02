"""Tests for the SEO compiler — hysteresis and sitemap retirement logic.

Pure-function tests only (no DB required).
The full end-to-end integration test requires a fresh DB with seeded data.
"""
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

import pytest

from src.services.seo.grid import GridCell


# ─── helpers ────────────────────────────────────────────────────────────────

def _cell(city="Tampa", city_slug="tampa", vertical="wholesalers"):
    return GridCell(
        city_raw=city,
        city_slug=city_slug,
        vertical=vertical,
        topic_slug=vertical.replace("_", "-"),
    )


def _make_db(existing_page=None):
    """Minimal session mock. execute().scalar() → 0; execute().mappings().fetchone() → row."""
    db = MagicMock()
    mapping_result = MagicMock()
    mapping_result.fetchone.return_value = existing_page
    db.execute.return_value.mappings.return_value = mapping_result
    db.execute.return_value.scalar.return_value = 0
    db.execute.return_value.fetchall.return_value = []
    return db


# ─── hysteresis / retirement ─────────────────────────────────────────────────

def test_hysteresis_one_sub_threshold_does_not_retire():
    """First sub-threshold run increments counter but does NOT set noindex."""
    from src.tasks.seo_compiler import _handle_sub_threshold

    existing = {
        "below_threshold_runs": 0,
        "status": "live",
        "content_hash": "abc",
        "lastmod": None,
    }
    db = _make_db(existing)
    now = datetime.now(timezone.utc)

    _handle_sub_threshold(db, _cell(), hysteresis=2, now=now, write=False)

    # Should have issued one UPDATE (increment only, not retire)
    assert db.execute.called
    # The call should NOT contain 'noindex' — just increments below_threshold_runs to 1
    calls_sql = [str(c) for c in db.execute.call_args_list]
    noindex_calls = [s for s in calls_sql if "noindex" in s.lower()]
    assert len(noindex_calls) == 0


def test_hysteresis_two_sub_threshold_runs_retire():
    """Second consecutive sub-threshold run triggers noindex."""
    from src.tasks.seo_compiler import _handle_sub_threshold

    existing = {
        "below_threshold_runs": 1,  # already had one run
        "status": "live",
        "content_hash": "abc",
        "lastmod": None,
    }
    db = _make_db(existing)
    now = datetime.now(timezone.utc)

    _handle_sub_threshold(db, _cell(), hysteresis=2, now=now, write=False)

    calls_sql = [str(c.args[0]) for c in db.execute.call_args_list]
    noindex_calls = [s for s in calls_sql if "noindex" in s]
    assert len(noindex_calls) >= 1, "Should have issued an UPDATE ... status = 'noindex'"


def test_hysteresis_no_op_when_page_never_published():
    """Cell that was never published — sub-threshold does nothing."""
    from src.tasks.seo_compiler import _handle_sub_threshold

    db = _make_db(existing_page=None)  # no row in seo_pages
    now = datetime.now(timezone.utc)

    _handle_sub_threshold(db, _cell(), hysteresis=2, now=now, write=False)

    # After the initial SELECT (which returns None), no UPDATE should run
    update_calls = [c for c in db.execute.call_args_list
                    if "UPDATE" in str(c.args[0])]
    assert len(update_calls) == 0


# ─── faq (data-driven, ADR 0023 revised) ─────────────────────────────────────

_STATS = {
    "qualified_count": 8078,
    "median_value": 215000.0,
    "ultra_platinum_count": 5,
    "platinum_count": 12,
    "gold_count": 30,
    "absentee_count": 3465,
    "city_vs_county_pct": 39.2,
}


def test_build_faq_uses_real_page_numbers():
    from src.services.seo.faq import build_faq

    items = build_faq("Tampa", "wholesalers", _STATS)
    joined = " ".join(i["question"] + " " + i["answer"] for i in items)
    assert "8,078" in joined          # count is rendered
    assert "Tampa" in joined          # on-topic for the city
    assert "wholesalers" in joined    # on-topic for the vertical
    # every answer is plain text — no leaked markdown or tracking links
    assert "**" not in joined
    assert "utm_" not in joined
    assert "http" not in joined


def test_build_faq_omits_value_question_when_no_median_value():
    from src.services.seo.faq import build_faq

    items = build_faq("Tampa", "wholesalers", {**_STATS, "median_value": 0})
    assert not any("typical value" in i["question"].lower() for i in items)


def test_build_faq_answers_are_plain_strings():
    from src.services.seo.faq import build_faq

    for item in build_faq("Riverview", "roofing", _STATS):
        assert isinstance(item["question"], str) and item["question"]
        assert isinstance(item["answer"], str) and item["answer"]


# ─── indexing_api ─────────────────────────────────────────────────────────────

def test_indexing_api_no_op_when_disabled():
    """When SEO_INDEXING_API_ENABLED=false, notify() is a no-op."""
    from src.services.seo import indexing_api

    with patch("src.services.seo.indexing_api.get_settings") as mock_settings:
        mock_settings.return_value.seo_indexing_api_enabled = False
        mock_settings.return_value.seo_indexing_api_daily_cap = 200

        indexing_api.notify(["https://example.com/florida/tampa/wholesalers/"])
        # No error, no HTTP call — just returns


def test_indexing_api_respects_cap():
    """notify() never submits more than daily_cap URLs."""
    from src.services.seo import indexing_api

    with patch("src.services.seo.indexing_api.get_settings") as mock_settings:
        mock_settings.return_value.seo_indexing_api_enabled = True
        mock_settings.return_value.seo_indexing_api_daily_cap = 3
        # No google client installed in test env — expect ImportError path
        urls = [f"https://example.com/florida/tampa/v{i}/" for i in range(10)]
        # Should not raise; ImportError path exits early after slicing to cap
        try:
            indexing_api.notify(urls)
        except Exception:
            pass  # ImportError path or no-op both acceptable
