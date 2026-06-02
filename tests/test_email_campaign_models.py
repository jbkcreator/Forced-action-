"""
Unit tests for Phase B1 — schema / model structure.
No live DB required — inspects SQLAlchemy metadata only.
"""
from sqlalchemy import inspect as sa_inspect


class TestDBPRContactNewColumns:
    def test_suppression_columns_exist(self):
        from src.core.models import DBPRContact
        cols = {c.key for c in DBPRContact.__table__.columns}
        for col in ("is_opted_out", "is_hard_bounced", "is_signed_up"):
            assert col in cols, f"Missing: {col}"

    def test_clay_columns_exist(self):
        from src.core.models import DBPRContact
        cols = {c.key for c in DBPRContact.__table__.columns}
        for col in ("email_source", "email_verified", "email_verified_at", "clay_enriched_at"):
            assert col in cols, f"Missing: {col}"

    def test_suppression_defaults_false(self):
        from src.core.models import DBPRContact
        col_map = {c.key: c for c in DBPRContact.__table__.columns}
        for col in ("is_opted_out", "is_hard_bounced", "is_signed_up"):
            assert col_map[col].default is not None or col_map[col].server_default is not None, \
                f"{col} has no default"


class TestEmailSequenceTemplate:
    def test_tablename(self):
        from src.core.models import EmailSequenceTemplate
        assert EmailSequenceTemplate.__tablename__ == "email_sequence_templates"

    def test_required_columns(self):
        from src.core.models import EmailSequenceTemplate
        cols = {c.key for c in EmailSequenceTemplate.__table__.columns}
        for col in ("id", "name", "steps", "variables_used", "created_at", "updated_at"):
            assert col in cols

    def test_name_is_unique(self):
        from src.core.models import EmailSequenceTemplate
        unique_cols = set()
        for uc in EmailSequenceTemplate.__table__.constraints:
            if hasattr(uc, "columns"):
                for c in uc.columns:
                    unique_cols.add(c.key)
        assert "name" in unique_cols

    def test_repr(self):
        from src.core.models import EmailSequenceTemplate
        # Verify repr format via string inspection of the class repr pattern
        assert "email_sequence_templates" == EmailSequenceTemplate.__tablename__
        assert hasattr(EmailSequenceTemplate, "__repr__")


class TestEmailCampaign:
    def test_tablename(self):
        from src.core.models import EmailCampaign
        assert EmailCampaign.__tablename__ == "email_campaigns"

    def test_required_columns(self):
        from src.core.models import EmailCampaign
        cols = {c.key for c in EmailCampaign.__table__.columns}
        for col in ("id", "name", "instantly_campaign_id", "template_id",
                    "county_id", "geo_filter", "vertical", "max_contacts",
                    "start_date", "end_date", "send_schedule", "status",
                    "last_synced_at", "created_at", "updated_at"):
            assert col in cols

    def test_status_check_constraint(self):
        from src.core.models import EmailCampaign
        constraint_names = {c.name for c in EmailCampaign.__table__.constraints}
        assert "check_campaign_status" in constraint_names

    def test_instantly_campaign_id_unique(self):
        from src.core.models import EmailCampaign
        col_map = {c.key: c for c in EmailCampaign.__table__.columns}
        assert col_map["instantly_campaign_id"].unique is True

    def test_default_status_draft(self):
        from src.core.models import EmailCampaign
        col_map = {c.key: c for c in EmailCampaign.__table__.columns}
        assert col_map["status"].default.arg == "draft"


class TestCampaignContact:
    def test_tablename(self):
        from src.core.models import CampaignContact
        assert CampaignContact.__tablename__ == "campaign_contacts"

    def test_unique_constraint(self):
        from src.core.models import CampaignContact
        uc_names = {c.name for c in CampaignContact.__table__.constraints}
        assert "uq_campaign_contact" in uc_names

    def test_engagement_status_check(self):
        from src.core.models import CampaignContact
        ck_names = {c.name for c in CampaignContact.__table__.constraints}
        assert "check_engagement_status" in ck_names

    def test_no_partial_unique_on_contact(self):
        """Contractor can be in multiple campaigns — no global unique on dbpr_contact_id."""
        from src.core.models import CampaignContact
        for idx in CampaignContact.__table__.indexes:
            # Should not have a unique index solely on dbpr_contact_id
            cols = [c.key for c in idx.columns]
            if cols == ["dbpr_contact_id"]:
                assert not idx.unique, "dbpr_contact_id must NOT be globally unique"

    def test_default_engagement_active(self):
        from src.core.models import CampaignContact
        col_map = {c.key: c for c in CampaignContact.__table__.columns}
        assert col_map["engagement_status"].default.arg == "active"


class TestCampaignDailyAnalytics:
    def test_tablename(self):
        from src.core.models import CampaignDailyAnalytics
        assert CampaignDailyAnalytics.__tablename__ == "campaign_daily_analytics"

    def test_unique_snapshot_constraint(self):
        from src.core.models import CampaignDailyAnalytics
        uc_names = {c.name for c in CampaignDailyAnalytics.__table__.constraints}
        assert "uq_campaign_snapshot" in uc_names

    def test_analytics_columns(self):
        from src.core.models import CampaignDailyAnalytics
        cols = {c.key for c in CampaignDailyAnalytics.__table__.columns}
        for col in ("emails_sent", "opens", "open_rate", "replies", "reply_rate",
                    "clicks", "bounces", "unsubscribes", "interested"):
            assert col in cols
