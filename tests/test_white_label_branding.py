"""
Unit tests for white-label branding (Stage 12 / fa056).
Covers:
  - PATCH /account stores valid hex colors, rejects invalid
  - CSV export includes company name in header row
  - PDF rendering injects WL CSS vars when wl_branding is present
"""

import io
import re
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Color validation (via router endpoint PATCH /account)
# ---------------------------------------------------------------------------

def test_invalid_color_rejected():
    """PATCH /api/wl/account with a non-hex color string raises 400."""
    from fastapi import HTTPException
    from src.api.white_label_router import UpdateAccountRequest, update_account

    req = UpdateAccountRequest(primary_color="not-a-color")
    user = MagicMock()
    user.client_id = 1
    user.role = "admin"
    db = MagicMock()

    with pytest.raises(HTTPException) as exc:
        update_account(req, user=user, db=db)
    assert exc.value.status_code == 400


def test_valid_hex_color_accepted():
    """PATCH /api/wl/account with a valid hex color completes without error."""
    from src.api.white_label_router import UpdateAccountRequest, update_account

    req = UpdateAccountRequest(primary_color="#a1b2c3")
    user = MagicMock()
    user.client_id = 1
    user.role = "admin"
    db = MagicMock()
    db.execute.return_value.rowcount = 1

    result = update_account(req, user=user, db=db)
    assert result["message"] == "Account updated"
    db.commit.assert_called_once()


def test_short_hex_rejected():
    """3-digit shorthand hex (#abc) is not accepted — requires 6 digits."""
    from fastapi import HTTPException
    from src.api.white_label_router import UpdateAccountRequest, update_account

    req = UpdateAccountRequest(primary_color="#abc")
    user = MagicMock()
    user.client_id = 1
    user.role = "admin"
    db = MagicMock()

    with pytest.raises(HTTPException) as exc:
        update_account(req, user=user, db=db)
    assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# CSV export includes company name
# ---------------------------------------------------------------------------

def test_csv_header_includes_company_name():
    """GET /reports/leads.csv header row contains the client's company name."""
    from src.api.white_label_router import download_leads_csv

    user = MagicMock()
    user.client_id = 1
    db = MagicMock()

    client_row = MagicMock()
    client_row.display_name = None
    client_row.company_name = "Acme Roofing"
    client_row.counties_enabled = ["hillsborough"]

    # First DB call returns client; second returns empty leads
    db.execute.return_value.fetchone.return_value = client_row
    db.execute.return_value.fetchall.return_value = []

    response = download_leads_csv(county_id=None, min_score=40.0, user=user, db=db)
    content = response.body.decode()
    assert "Acme Roofing" in content


# ---------------------------------------------------------------------------
# PDF rendering injects WL CSS vars
# ---------------------------------------------------------------------------

def test_pdf_context_has_wl_branding():
    """generate_dashboard_pdf with wl_client sets wl_branding in Jinja2 context."""
    from src.tasks.daily_dashboard import generate_dashboard_pdf

    wl_client = {
        "display_name": "Test Corp",
        "company_slug": "test-corp",
        "company_name": "Test Corporation",
        "logo_url": "/static/logos/test-corp.png",
        "primary_color": "#ff0000",
        "secondary_color": "#0000ff",
    }

    captured_context = {}

    def fake_render_html(ctx):
        captured_context.update(ctx)
        return "<html></html>"

    with patch("src.tasks.daily_dashboard.collect_dashboard_data", return_value={}), \
         patch("src.tasks.daily_dashboard.render_html", side_effect=fake_render_html), \
         patch("src.tasks.daily_dashboard.html_to_pdf"), \
         patch("src.tasks.daily_dashboard.prune_old_dashboards", return_value=0):
        generate_dashboard_pdf(wl_client=wl_client)

    assert captured_context.get("wl_branding") is not None
    assert captured_context["wl_branding"]["primary_color"] == "#ff0000"
    assert captured_context["wl_branding"]["company_name"] == "Test Corp"


def test_pdf_no_wl_branding_by_default():
    """generate_dashboard_pdf without wl_client sets wl_branding to None."""
    from src.tasks.daily_dashboard import generate_dashboard_pdf

    captured_context = {}

    def fake_render_html(ctx):
        captured_context.update(ctx)
        return "<html></html>"

    with patch("src.tasks.daily_dashboard.collect_dashboard_data", return_value={}), \
         patch("src.tasks.daily_dashboard.render_html", side_effect=fake_render_html), \
         patch("src.tasks.daily_dashboard.html_to_pdf"), \
         patch("src.tasks.daily_dashboard.prune_old_dashboards", return_value=0):
        generate_dashboard_pdf()

    assert captured_context.get("wl_branding") is None


def test_wl_css_vars_in_template_style_block():
    """
    The branding CSS block is emitted when wl_branding is set.
    We test only the <style> section so we don't need to render the full template.
    """
    from jinja2 import Environment

    # Minimal template that contains only the branding style block
    STYLE_SNIPPET = """
{% if wl_branding %}
:root {
  --fa-color-primary: {{ wl_branding.primary_color }};
  --fa-color-accent:  {{ wl_branding.secondary_color }};
}
{% endif %}
"""
    env = Environment(autoescape=True)
    tmpl = env.from_string(STYLE_SNIPPET)
    html = tmpl.render(
        wl_branding={"company_name": "Acme", "logo_url": None, "primary_color": "#cc0000", "secondary_color": "#0000cc"},
    )
    assert "--fa-color-primary: #cc0000" in html
    assert "--fa-color-accent:  #0000cc" in html


def test_wl_css_vars_absent_without_branding():
    """The branding block is NOT emitted when wl_branding is None/falsy."""
    from jinja2 import Environment

    STYLE_SNIPPET = """
{% if wl_branding %}
:root { --fa-color-primary: {{ wl_branding.primary_color }}; }
{% endif %}
"""
    env = Environment(autoescape=True)
    tmpl = env.from_string(STYLE_SNIPPET)
    html = tmpl.render(wl_branding=None)
    assert "--fa-color-primary" not in html
