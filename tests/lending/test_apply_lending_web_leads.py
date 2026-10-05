"""WP-GL-11: migrations/apply_lending_web_leads.py is idempotent and creates the expected table."""
from __future__ import annotations

import pytest
from sqlalchemy import inspect, text

from migrations.apply_lending_web_leads import apply_to


def test_apply_creates_web_leads_and_is_safe_to_rerun(lending_db):
    conn = lending_db.get_bind()
    apply_to(conn)
    apply_to(conn)
    columns = {c["name"] for c in inspect(conn).get_columns("web_leads", schema="lending")}
    assert {"phone", "sms_consent", "deal_drop_optin", "consent_text", "ip_address", "page_url",
            "ghl_status", "ghl_attempts", "ghl_contact_id"} <= columns


def test_consent_columns_default_to_false(web_leads_db):
    web_leads_db.execute(text("INSERT INTO lending.web_leads (name, phone) VALUES ('A', '+18135550100')"))
    assert web_leads_db.execute(text("SELECT sms_consent, deal_drop_optin, ghl_status FROM lending.web_leads")).one() == (False, False, "pending")


def test_unknown_delivery_status_is_rejected(web_leads_db):
    with pytest.raises(Exception):
        web_leads_db.execute(text("INSERT INTO lending.web_leads (name, phone, ghl_status) VALUES ('A', '+18135550100', 'bogus')"))
