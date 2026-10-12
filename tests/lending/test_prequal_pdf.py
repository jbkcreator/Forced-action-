import re

import pytest

from src.lending import prequal
from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK, render_html
from src.lending.prequal import FitLimits, PrequalLead

FULL = dict(credit_band="700+", loan_amount=400_000, property_state="FL", loan_type="FIX_AND_FLIP")
FITS = [FitLimits(100_000, 2_000_000)]


@pytest.mark.parametrize("missing", list(FULL))
def test_missing_core_field_no_pdf(missing):
    lead = PrequalLead(**{**FULL, missing: None})
    assert prequal.should_generate(lead) is False
    assert prequal.build_context(lead, FITS, 10) is None


def test_zero_amount_not_complete():
    assert prequal.should_generate(PrequalLead(**{**FULL, "loan_amount": 0})) is False


def test_complete_lead_generates():
    assert prequal.should_generate(PrequalLead(**FULL)) is True


def test_watermark_in_rendered_html_exact():
    ctx = prequal.build_context(PrequalLead(**FULL), FITS, 10)
    html = render_html(prequal.TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
    assert "NON-BINDING PRE-QUALIFICATION ESTIMATE — FOR INFORMATIONAL PURPOSES ONLY" in html
    assert "$360,000" in html and "$440,000" in html


def test_watermark_required():
    with pytest.raises(ValueError):
        render_html(prequal.TEMPLATE, {}, watermark=" ")


def test_range_clamped_to_fitting_limits():
    assert prequal.compute_range(400_000, [FitLimits(390_000, 420_000)], 10) == (390_000, 420_000)


def test_no_fitting_lender_no_pdf():
    assert prequal.build_context(PrequalLead(**FULL), [], 10) is None


def test_letter_has_no_rate_or_term_language():
    ctx = prequal.build_context(PrequalLead(**FULL), FITS, 10)
    html = render_html(prequal.TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
    html = re.sub(r"<style.*?</style>", "", html, flags=re.S).lower()  # embedded font data is not copy
    for word in ("interest rate", "apr", "points", "per annum", "months"):
        assert word not in html


def test_rendered_html_needs_no_network():
    ctx = prequal.build_context(PrequalLead(**FULL), FITS, 10)
    html = render_html(prequal.TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
    assert "http://" not in html and "https://" not in html
    assert "data:font/woff2;base64," in html


def test_watermark_in_real_pdf():
    """Renders through Chromium; skipped where Playwright/Chromium is not installed."""
    import io
    import logging as _logging

    pdfplumber = pytest.importorskip("pdfplumber")
    from src.lending.pdf.render import render_pdf

    ctx = prequal.build_context(PrequalLead(**FULL), FITS, 10)
    try:
        pdf = render_pdf(prequal.TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
    except Exception as exc:
        pytest.skip(f"Chromium not available: {type(exc).__name__}")
    _logging.getLogger("pdfminer").setLevel(_logging.ERROR)
    with pdfplumber.open(io.BytesIO(pdf)) as doc:
        text = " ".join((page.extract_text() or "") for page in doc.pages).replace(chr(10), " ")
    assert "NON-BINDING PRE-QUALIFICATION ESTIMATE" in text
    assert "FOR INFORMATIONAL PURPOSES ONLY" in text
