"""
Tests for the SentLead.source column — added so the lead_unlock webhook
can mark a $4-purchased lead as paid-and-delivered, which the feed/sample
endpoints then surface as unlocked=true.
"""

from src.core.models import SentLead


class TestSentLeadSourceAttribute:
    def test_source_column_is_mapped(self):
        # ORM-level proof that the column exists; without it the webhook
        # falls back to the bare-except path and silently drops the row.
        col = SentLead.__table__.columns.get("source")
        assert col is not None, "SentLead.source column missing"
        assert str(col.type).startswith("VARCHAR"), f"Unexpected type: {col.type}"

    def test_source_can_be_set_via_constructor(self):
        # Pre-fix this raised TypeError because `source` wasn't a mapped attr
        row = SentLead(subscriber_id=1, property_id=2, source="lead_unlock_payment")
        assert row.source == "lead_unlock_payment"

    def test_source_default_at_orm_level(self):
        row = SentLead(subscriber_id=1, property_id=2)
        # SQLAlchemy default fires only on insert/flush — but the column must accept None
        # without raising. We're not flushing here, just verifying the attr is reachable.
        assert hasattr(row, "source")

    def test_source_index_declared(self):
        # The migration adds idx_sent_leads_source; the model also declares it
        # so autogenerate stays clean.
        index_names = {ix.name for ix in SentLead.__table__.indexes}
        assert "idx_sent_leads_source" in index_names
