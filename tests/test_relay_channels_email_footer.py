"""
WP-T2-1 go-live review (2026-09) — build_passthrough_body's optional
phone/disclaimer footer lines (SOT.md client Q9, Josh's confirmed draft
for fa_max_lending).
"""
from __future__ import annotations

from types import SimpleNamespace

from src.services.relay.channels_email import build_passthrough_body


def _venture(**overrides):
    base = dict(
        brand_name="Forced Action",
        postal_address="123 Main St",
        outbound_contact_phone=None,
        outbound_disclaimer=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_venture_with_no_phone_or_disclaimer_renders_unchanged_footer():
    body = build_passthrough_body("hello", "a@example.com", _venture())
    assert "Forced Action" in body
    assert "123 Main St" in body
    assert "Unsubscribe" in body


def test_venture_with_phone_renders_it_after_address():
    body = build_passthrough_body("hello", "a@example.com", _venture(outbound_contact_phone="(813) 361-8927"))
    assert "(813) 361-8927" in body
    assert body.index("123 Main St") < body.index("(813) 361-8927")


def test_venture_with_disclaimer_renders_it_before_unsubscribe():
    disclaimer = "This message is not an offer of credit."
    body = build_passthrough_body("hello", "a@example.com", _venture(outbound_disclaimer=disclaimer))
    assert disclaimer in body
    assert body.index(disclaimer) < body.index("Unsubscribe")


def test_fa_max_confirmed_footer_renders_all_four_lines():
    venture = _venture(
        brand_name="Josh Kantor, Forced Action",
        postal_address="1320 W. Lemon St., Tampa, FL 33606",
        outbound_contact_phone="(813) 361-8927",
        outbound_disclaimer=(
            "This message is not an offer of credit and is not a solicitation "
            "to originate a loan. Reply STOP to opt out of future messages."
        ),
    )
    body = build_passthrough_body("Hi there", "lead@example.com", venture)
    assert "Josh Kantor, Forced Action" in body
    assert "1320 W. Lemon St., Tampa, FL 33606" in body
    assert "(813) 361-8927" in body
    assert "not an offer of credit" in body
    assert "Reply STOP" in body


def test_disclaimer_is_html_escaped():
    body = build_passthrough_body(
        "hello", "a@example.com", _venture(outbound_disclaimer="A <script>bad</script> & co")
    )
    assert "<script>" not in body
    assert "&amp;" in body
