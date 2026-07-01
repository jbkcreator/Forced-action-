"""Tests for SEO page rendering (render_page, content_hash)."""
from src.services.seo.render import render_page, content_hash

_FAQ_TITLE = "How do wholesalers find distressed properties in Florida?"

_SAMPLE = {
    "city": "Tampa",
    "city_slug": "tampa",
    "vertical": "wholesalers",
    "topic_slug": "wholesalers",
    "stats": {
        "qualified_count": 120,
        "avg_value": 215_000.0,
        "ultra_platinum_count": 5,
        "platinum_count": 12,
        "gold_count": 30,
        "absentee_count": 45,
        "city_vs_county_pct": 24.5,
    },
    "faq": {
        "title": _FAQ_TITLE,
        "answer_draft": {"body": "Wholesalers use direct mail and bandit signs to find deals."},
    },
    "status": "live",
    "canonical_url": "https://www.forcedaction.com/florida/tampa/wholesalers/",
}


def test_render_contains_city():
    assert "Tampa" in render_page(_SAMPLE)


def test_render_contains_qualified_count():
    assert "120" in render_page(_SAMPLE)


def test_render_no_noindex_when_live():
    html = render_page(_SAMPLE)
    assert "noindex" not in html


def test_render_noindex_meta_when_status_noindex():
    data = {**_SAMPLE, "status": "noindex"}
    html = render_page(data)
    assert 'content="noindex"' in html


def test_render_canonical_url_present():
    html = render_page(_SAMPLE)
    assert _SAMPLE["canonical_url"] in html


def test_render_faq_title_in_both_visible_and_jsonld():
    html = render_page(_SAMPLE)
    count = html.count(_FAQ_TITLE)
    assert count >= 2, f"FAQ title should appear in visible FAQ + JSON-LD, found {count} times"


def test_render_no_faq_section_when_faq_is_none():
    data = {**_SAMPLE, "faq": None}
    html = render_page(data)
    assert "FAQPage" not in html


def test_content_hash_stable():
    html = "<html>stable</html>"
    assert content_hash(html) == content_hash(html)


def test_content_hash_changes_on_diff_content():
    assert content_hash("<html>a</html>") != content_hash("<html>b</html>")


def test_content_hash_is_16_chars():
    assert len(content_hash("<html>test</html>")) == 16
