"""Tests for SEO page rendering (render_page, content_hash)."""
from src.services.seo.render import render_page, content_hash

_FAQ_Q = "How many distressed properties are available in Tampa, FL for wholesalers?"
_FAQ_A = "There are currently 120 qualifying properties in Tampa."

_SAMPLE = {
    "city": "Tampa",
    "city_slug": "tampa",
    "vertical": "wholesalers",
    "topic_slug": "wholesalers",
    "stats": {
        "qualified_count": 120,
        "median_value": 215_000.0,
        "ultra_platinum_count": 5,
        "platinum_count": 12,
        "gold_count": 30,
        "absentee_count": 45,
        "city_vs_county_pct": 24.5,
    },
    "faq_items": [
        {"question": _FAQ_Q, "answer": _FAQ_A},
    ],
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


def test_render_no_pixel_when_meta_pixel_id_unset():
    html = render_page(_SAMPLE)
    assert "fbevents.js" not in html
    assert "facebook.com/tr" not in html


def test_render_includes_pixel_when_meta_pixel_id_set():
    data = {**_SAMPLE, "meta_pixel_id": "123456789"}
    html = render_page(data)
    assert "fbevents.js" in html
    assert "fbq('init', '123456789')" in html
    assert "fbq('track', 'PageView')" in html
    assert "facebook.com/tr?id=123456789" in html


def test_render_cta_uses_app_cta_url_not_signup_path():
    """The React app has no /signup route — that link 404's via the catch-all.
    Both CTAs (header + page body) must use the resolved app_cta_url instead."""
    data = {**_SAMPLE, "app_cta_url": "https://app.forcedactionleads.com/?utm_source=seo&utm_medium=organic&utm_campaign=wholesalers"}
    html = render_page(data)
    assert "/signup" not in html
    assert html.count('href="https://app.forcedactionleads.com/?utm_source=seo&amp;utm_medium=organic&amp;utm_campaign=wholesalers"') == 2


def test_render_noindex_meta_when_status_noindex():
    data = {**_SAMPLE, "status": "noindex"}
    html = render_page(data)
    assert 'content="noindex"' in html


def test_render_canonical_url_present():
    html = render_page(_SAMPLE)
    assert _SAMPLE["canonical_url"] in html


def test_render_faq_question_in_both_visible_and_jsonld():
    html = render_page(_SAMPLE)
    count = html.count(_FAQ_Q)
    assert count >= 2, f"FAQ question should appear in visible FAQ + JSON-LD, found {count} times"


def test_render_faq_answer_text_appears_in_visible_faq():
    html = render_page(_SAMPLE)
    assert _FAQ_A in html, "FAQ answer must render as visible text"


def test_render_faq_answer_text_appears_in_jsonld():
    html = render_page(_SAMPLE)
    assert f'"text": "{_FAQ_A}"' in html


def test_render_no_faq_section_when_no_faq_items():
    data = {**_SAMPLE, "faq_items": []}
    html = render_page(data)
    assert "FAQPage" not in html


def test_content_hash_stable():
    html = "<html>stable</html>"
    assert content_hash(html) == content_hash(html)


def test_content_hash_changes_on_diff_content():
    assert content_hash("<html>a</html>") != content_hash("<html>b</html>")


def test_content_hash_is_16_chars():
    assert len(content_hash("<html>test</html>")) == 16
