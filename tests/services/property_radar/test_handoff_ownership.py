"""The real handoff store claims lead ownership for every lead it creates.

Real Postgres via fresh_db (rolled back).
"""
from __future__ import annotations

from sqlalchemy import text

from src.services.property_radar.lead_handoff import SqlHandoffStore, StagedLead
from src.services.property_radar.trace_contacts import LeadContacts


def test_created_lead_is_tagged_and_owned(fresh_db):
    lead = StagedLead(
        radar_id="TEST-HANDOFF-OWN-001", campaign="maturity_target_lender", status="active",
        state="FL", county_fips="12057", county_name="HILLSBOROUGH", owner_name="QA OWNER LLC",
    )
    person_id, opportunity_id = SqlHandoffStore(fresh_db).create_lead(
        lead, LeadContacts(emails=("test-handoff-own@example.com",)), contact_rules_enabled=False,
    )
    row = fresh_db.execute(text(
        "SELECT campaign, source, radar_id, status, CAST(opportunity_id AS text) AS opp "
        "FROM lead_campaign_assignments WHERE person_id = CAST(:p AS uuid)"), {"p": person_id}).mappings().one()
    assert (row["campaign"], row["source"], row["radar_id"], row["status"]) == (
        "maturity_target_lender", "property_radar", "TEST-HANDOFF-OWN-001", "active",
    )
    assert row["opp"] == str(opportunity_id)
