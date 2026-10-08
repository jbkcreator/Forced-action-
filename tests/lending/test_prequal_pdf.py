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
    html = render_html(prequal.TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK).lower()
    for word in ("interest rate", "apr", "points", "per annum", "months"):
        assert word not in html


class FakeSink:
    def __init__(self):
        self.calls = []

    def deliver(self, lead_id, contact_id, pdf):
        self.calls.append((lead_id, contact_id, pdf))


def test_flag_off_no_delivery():
    sink = FakeSink()
    assert prequal.generate_and_deliver(1, "c1", PrequalLead(**FULL), FITS, sink, enabled=False, pct=10) is False
    assert sink.calls == []


def test_delivery_when_enabled(monkeypatch):
    monkeypatch.setattr(prequal, "render_pdf", lambda t, c, watermark: b"%PDF-fake")
    sink = FakeSink()
    assert prequal.generate_and_deliver(7, "c7", PrequalLead(**FULL), FITS, sink, enabled=True, pct=10) is True
    assert sink.calls == [(7, "c7", b"%PDF-fake")]
