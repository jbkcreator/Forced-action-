"""
Staging integration test for the Email Campaign backend (Phases B0-B8).

This is the executable form of the *Recommended staging test order*. It walks
the full lifecycle end-to-end against a REAL Postgres database, mocking only the
Instantly.ai HTTP boundary (so no live emails are sent and no Instantly billing
is touched).

    1.  Apply DB migration / DDL script        -> test_01_apply_migration
    2.  Seed test dbpr_contacts                 -> test_02_seed_contacts
    3.  Template create / update validation     -> test_03_template_create / _04
    4.  Campaign create                         -> test_05_create_campaign
    5.  Eligible count                          -> test_06_eligible_count
    6.  Run manual add-contacts (top-up)        -> test_07_add_contacts
    7.  Verify campaign_contacts rows           -> test_08_verify_campaign_contacts
    8.  Verify Instantly leads were added       -> test_09_verify_instantly_leads
    9.  Run analytics sync                       -> test_10_sync_analytics
    10. Verify analytics + engagement status    -> test_11_sync_lead_statuses
    11. Pause / resume / duplicate              -> test_12_pause / _13_resume / _14_duplicate
    12. Global suppression (unsub / bounce)     -> test_15_global_suppression
        + lifecycle check                       -> test_16_lifecycle_no_complete

WHY REAL POSTGRES (not the in-memory fixture):
    The service layer (create_campaign, topup_campaign, sync task) opens its own
    `get_db_context()` which COMMITS on exit. A rollback-isolated session cannot
    observe those commits, and the models use JSONB/Numeric which SQLite can't
    represent. So this suite runs only when DATABASE_URL points at a (staging)
    Postgres, commits real rows, and deletes everything it created in teardown.

RUNNING:
    # against staging Postgres, Instantly mocked (default — safe):
    pytest tests/test_email_campaign_staging.py -v

    # the whole file is ORDERED — run it as one unit, do not -k individual steps
    # out of sequence (later steps depend on rows created by earlier ones).

ISOLATION:
    Every seeded row carries a recognizable marker:
        dbpr_contacts.license_number  starts with  STGTEST-
        email_sequence_templates.name starts with  __STAGING_TEST__
        email_campaigns.name          starts with  [STAGING TEST]
    Teardown deletes strictly by those markers, so a crashed mid-run leaves
    nothing behind that a re-run won't clean up first.
"""

import os
import pytest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from src.core.database import get_db_context
from src.core.models import (
    CampaignContact,
    CampaignDailyAnalytics,
    DBPRContact,
    EmailCampaign,
    EmailSequenceTemplate,
)


# ============================================================================
# Markers & shared constants
# ============================================================================

LICENSE_PREFIX = "STGTEST-"
TEMPLATE_NAME = "__STAGING_TEST__ Roofing Sequence"
TEMPLATE_NAME_RENAMED = "__STAGING_TEST__ Roofing Sequence v2"
CAMPAIGN_NAME = "[STAGING TEST] Hillsborough Roofing"

# A vertical that will NOT collide with any real staging data, so the
# eligibility count is deterministic (== number of rows we seed).
TEST_VERTICAL = "staging_test_vertical"
TEST_COUNTY = "hillsborough"          # patched to 'launched' below
UNLAUNCHED_COUNTY = "zzz_not_launched"

FAKE_INSTANTLY_CAMPAIGN_ID = "inst_camp_STAGINGTEST"
FAKE_INSTANTLY_DUP_ID = "inst_camp_STAGINGTEST_dup"

# State threaded between ordered steps.
STATE: dict = {}


# ============================================================================
# Hard gate: real Postgres required
# ============================================================================

def _pg_url():
    try:
        from config.settings import get_settings
        url = get_settings().database_url
        return str(url) if url else None
    except Exception:
        return None


pytestmark = pytest.mark.skipif(
    _pg_url() is None,
    reason="DATABASE_URL not set — staging integration test requires real Postgres",
)


# ============================================================================
# Seed definitions
# ============================================================================
#
# 5 ELIGIBLE rows + 5 INELIGIBLE rows. Eligibility predicate (see
# email_campaigns._eligibility_filters):
#   enrichment_status='enriched' AND email NOT NULL AND email_verified
#   AND NOT is_opted_out AND NOT is_hard_bounced AND NOT is_signed_up
#   AND (license_expiry NULL OR >= today) AND vertical match
#   AND county match AND county IN launched
#
# Eligible emails are stgtest-elig-{0..4}@example.com.

FUTURE = date.today() + timedelta(days=365)
PAST = date.today() - timedelta(days=10)


def _eligible_seed():
    rows = []
    for i in range(5):
        rows.append(dict(
            license_number=f"{LICENSE_PREFIX}E{i}",
            license_type_code="CCC",
            license_type_desc="Roofing Contractor",
            full_name=f"SMITH{i}, JOHN",
            email=f"stgtest-elig-{i}@example.com",
            email_verified=True,
            enrichment_status="enriched",
            vertical=TEST_VERTICAL,
            county_id=TEST_COUNTY,
            zip_code="33601",
            city="Tampa",
            company_name=f"Smith{i} Roofing LLC",
            phone="8135551000",
            license_expiry=FUTURE,
        ))
    return rows


def _ineligible_seed():
    """Each row violates exactly one eligibility predicate."""
    base = dict(
        license_type_code="CCC",
        license_type_desc="Roofing Contractor",
        full_name="DOE, JANE",
        email_verified=True,
        enrichment_status="enriched",
        vertical=TEST_VERTICAL,
        county_id=TEST_COUNTY,
        zip_code="33601",
        license_expiry=FUTURE,
    )
    variants = [
        # opted out
        {**base, "license_number": f"{LICENSE_PREFIX}X_OPTOUT", "email": "stgtest-x-optout@example.com", "is_opted_out": True},
        # hard bounced
        {**base, "license_number": f"{LICENSE_PREFIX}X_BOUNCE", "email": "stgtest-x-bounce@example.com", "is_hard_bounced": True},
        # already signed up
        {**base, "license_number": f"{LICENSE_PREFIX}X_SIGNED", "email": "stgtest-x-signed@example.com", "is_signed_up": True},
        # email not verified
        {**base, "license_number": f"{LICENSE_PREFIX}X_UNVERIF", "email": "stgtest-x-unverif@example.com", "email_verified": False},
        # license expired
        {**base, "license_number": f"{LICENSE_PREFIX}X_EXPIRED", "email": "stgtest-x-expired@example.com", "license_expiry": PAST},
        # not enriched
        {**base, "license_number": f"{LICENSE_PREFIX}X_PENDING", "email": "stgtest-x-pending@example.com", "enrichment_status": "pending"},
        # unlaunched county
        {**base, "license_number": f"{LICENSE_PREFIX}X_COUNTY", "email": "stgtest-x-county@example.com", "county_id": UNLAUNCHED_COUNTY},
        # no email
        {**base, "license_number": f"{LICENSE_PREFIX}X_NOEMAIL", "email": None},
    ]
    return variants


# ============================================================================
# Fixtures — county patch + Instantly mock, both module-scoped & autouse
# ============================================================================

@pytest.fixture(scope="module", autouse=True)
def _patch_launched_county():
    """Mark the test county as 'launched' so eligibility includes our rows."""
    counties = [{"county_id": TEST_COUNTY, "status": "launched"}]
    with patch("src.services.email_campaigns.list_counties", return_value=counties):
        yield


@pytest.fixture(scope="module", autouse=True)
def mock_instantly():
    """
    Replace the Instantly HTTP boundary with canned responses. Both
    email_campaigns and email_campaign_sync reference the module object
    (`from ... import instantly_service as instantly`), so patching attributes
    on the module affects both. map_analytics / map_lead_status stay REAL —
    we want to exercise the actual field mapping.
    """
    import src.services.instantly_service as inst

    add_leads_mock = MagicMock(name="add_leads",
                               return_value={"leads_created": 5, "leads_skipped": 0})
    create_mock = MagicMock(name="create_campaign",
                            return_value={"id": FAKE_INSTANTLY_CAMPAIGN_ID})
    activate_mock = MagicMock(name="activate_campaign", return_value=True)
    pause_mock = MagicMock(name="pause_campaign", return_value=True)

    # One raw analytics row in Instantly's native field names.
    analytics_mock = MagicMock(name="get_daily_analytics", return_value=[{
        "emails_sent_count": 5,
        "open_count_unique": 3,
        "reply_count_unique": 1,
        "link_click_count": 2,
        "bounced_count": 1,
        "unsubscribed_count": 1,
    }])

    # list_leads returns one page mapping our seeded eligible emails to statuses.
    # elig-0 -> interested, elig-1 -> unsubscribed, elig-2 -> bounced,
    # elig-3/4 -> active. Single page (next cursor None) stops the loop.
    leads_page = {
        "leads": [
            {"email": "stgtest-elig-0@example.com", "interest_status": "interested"},
            {"email": "stgtest-elig-1@example.com", "interest_status": "unsubscribed"},
            {"email": "stgtest-elig-2@example.com", "interest_status": "bounced"},
            {"email": "stgtest-elig-3@example.com", "interest_status": "active"},
            {"email": "stgtest-elig-4@example.com", "interest_status": "active"},
        ],
        "next_starting_after": None,
    }
    list_leads_mock = MagicMock(name="list_leads", return_value=leads_page)

    patches = {
        "_is_configured": MagicMock(return_value=True),
        "create_campaign": create_mock,
        "activate_campaign": activate_mock,
        "pause_campaign": pause_mock,
        "add_leads": add_leads_mock,
        "list_leads": list_leads_mock,
        "get_daily_analytics": analytics_mock,
    }
    started = []
    for name, m in patches.items():
        p = patch.object(inst, name, m)
        p.start()
        started.append(p)

    STATE["instantly"] = patches
    yield patches
    for p in started:
        p.stop()


REQUIRED_TABLES = [
    "email_sequence_templates",
    "email_campaigns",
    "campaign_contacts",
    "campaign_daily_analytics",
]


def _existing_tables() -> set:
    with get_db_context() as db:
        return {
            r[0] for r in db.execute(sa.text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )).all()
        }


def _ensure_schema():
    """Apply fa062 idempotently if a fresh DB hasn't been migrated.
    upgrade() is guarded by IF NOT EXISTS except one ADD CONSTRAINT — tolerate
    that one if it already exists."""
    if all(t in _existing_tables() for t in REQUIRED_TABLES):
        return
    import importlib.util
    mig_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "alembic", "versions", "fa062_email_campaigns.py",
    )
    spec = importlib.util.spec_from_file_location("fa062_email_campaigns", mig_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    upgrade = mod.upgrade
    with get_db_context() as db:
        try:
            upgrade(db.connection())
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise


@pytest.fixture(scope="module", autouse=True)
def _cleanup():
    """Delete everything this suite creates — before (in case of a prior crash)
    and after the run. Deletes campaigns first (cascades contacts + analytics),
    then templates, then dbpr_contacts."""
    _ensure_schema()   # purge queries fa062 tables, so they must exist first
    _purge()
    yield
    _purge()


def _purge():
    with get_db_context() as db:
        camp_ids = [r[0] for r in db.query(EmailCampaign.id).filter(
            EmailCampaign.name.like("[STAGING TEST]%")
        ).all()]
        if camp_ids:
            db.query(CampaignDailyAnalytics).filter(
                CampaignDailyAnalytics.campaign_id.in_(camp_ids)
            ).delete(synchronize_session=False)
            db.query(CampaignContact).filter(
                CampaignContact.campaign_id.in_(camp_ids)
            ).delete(synchronize_session=False)
            db.query(EmailCampaign).filter(
                EmailCampaign.id.in_(camp_ids)
            ).delete(synchronize_session=False)
        db.query(EmailSequenceTemplate).filter(
            EmailSequenceTemplate.name.like("__STAGING_TEST__%")
        ).delete(synchronize_session=False)
        db.query(DBPRContact).filter(
            DBPRContact.license_number.like(f"{LICENSE_PREFIX}%")
        ).delete(synchronize_session=False)


# ============================================================================
# STEP 1 — Apply DB migration / DDL script
# ============================================================================

class TestEmailCampaignStaging:

    def test_01_apply_migration(self):
        """fa062 tables + suppression columns must exist. Apply idempotently
        if a fresh staging DB hasn't been migrated yet."""
        _ensure_schema()
        existing = _existing_tables()
        for t in REQUIRED_TABLES:
            assert t in existing, f"migration did not create table {t}"

        # suppression columns present on dbpr_contacts
        with get_db_context() as db:
            cols = {
                r[0] for r in db.execute(sa.text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'dbpr_contacts'"
                )).all()
            }
        for c in ("email_source", "email_verified", "is_opted_out",
                  "is_hard_bounced", "is_signed_up", "clay_enriched_at"):
            assert c in cols, f"dbpr_contacts missing column {c}"

    # ------------------------------------------------------------------
    # STEP 2 — Seed test dbpr_contacts
    # ------------------------------------------------------------------

    def test_02_seed_contacts(self):
        now = datetime.now(timezone.utc)
        with get_db_context() as db:
            for spec in _eligible_seed() + _ineligible_seed():
                db.add(DBPRContact(created_at=now, updated_at=now, **spec))

        with get_db_context() as db:
            seeded = db.query(DBPRContact).filter(
                DBPRContact.license_number.like(f"{LICENSE_PREFIX}%")
            ).count()
        assert seeded == 13  # 5 eligible + 8 ineligible

        # capture the dbpr ids for the eligible rows (used later for suppression)
        with get_db_context() as db:
            STATE["elig_ids"] = {
                dc.email: dc.id
                for dc in db.query(DBPRContact).filter(
                    DBPRContact.license_number.like(f"{LICENSE_PREFIX}E%")
                ).all()
            }
        assert len(STATE["elig_ids"]) == 5

    # ------------------------------------------------------------------
    # STEP 3 — Template create / update validation
    # ------------------------------------------------------------------

    def test_03_template_create_and_validation(self):
        from src.services import email_templates as tmpl_svc

        good_steps = [
            {"step_number": 1, "delay_days": 0,
             "subject": "Hi {{firstName}}", "body": "Your company {{company}} in {{city}}"},
            {"step_number": 2, "delay_days": 4,
             "subject": "Following up", "body": "About your {{licenseType}} license"},
        ]
        bad_steps = [
            {"step_number": 1, "delay_days": 0,
             "subject": "Hi {{firstName}}", "body": "Your {{license_expiry}} is due"},
        ]

        # validation rejects unknown vars
        assert tmpl_svc.validate_variables(bad_steps) == ["license_expiry"]
        # validation passes for whitelisted vars
        assert tmpl_svc.validate_variables(good_steps) == []
        used = tmpl_svc.collect_variables_used(good_steps)
        assert set(used) == {"firstName", "company", "city", "licenseType"}

        now = datetime.now(timezone.utc)
        with get_db_context() as db:
            tmpl = EmailSequenceTemplate(
                name=TEMPLATE_NAME,
                steps=good_steps,
                variables_used=used,
                created_at=now, updated_at=now,
            )
            db.add(tmpl)
            db.flush()
            STATE["template_id"] = tmpl.id
        assert STATE["template_id"]

    def test_04_template_update(self):
        from src.services import email_templates as tmpl_svc
        new_steps = [
            {"step_number": 1, "delay_days": 0,
             "subject": "Hello {{firstName}}", "body": "Quick note for {{company}}"},
        ]
        with get_db_context() as db:
            tmpl = db.get(EmailSequenceTemplate, STATE["template_id"])
            tmpl.name = TEMPLATE_NAME_RENAMED
            tmpl.steps = new_steps
            tmpl.variables_used = tmpl_svc.collect_variables_used(new_steps)
            tmpl.updated_at = datetime.now(timezone.utc)
        with get_db_context() as db:
            tmpl = db.get(EmailSequenceTemplate, STATE["template_id"])
            assert tmpl.name == TEMPLATE_NAME_RENAMED
            assert set(tmpl.variables_used) == {"firstName", "company"}

    # ------------------------------------------------------------------
    # STEP 4 — Campaign create (Instantly orchestration mocked)
    # ------------------------------------------------------------------

    def test_05_create_campaign(self):
        from src.services import email_campaigns as svc
        from src.api.email_campaign_router import CampaignCreateIn

        body = CampaignCreateIn(
            name=CAMPAIGN_NAME,
            template_id=STATE["template_id"],
            county_id=TEST_COUNTY,
            zips=[],
            vertical=TEST_VERTICAL,
            max_contacts=None,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=30),
        )
        out = svc.create_campaign(body)

        assert out.status == "active", "should activate when Instantly succeeds"
        assert out.instantly_campaign_id == FAKE_INSTANTLY_CAMPAIGN_ID
        STATE["campaign_id"] = out.id

        # Instantly was driven: create + activate called once each
        assert STATE["instantly"]["create_campaign"].call_count == 1
        assert STATE["instantly"]["activate_campaign"].call_count == 1
        # the sequence payload was built from the template steps
        _, kwargs = STATE["instantly"]["create_campaign"].call_args
        assert kwargs["sequence_steps"][0]["type"] == "email"

    # ------------------------------------------------------------------
    # STEP 5 — Eligible count
    # ------------------------------------------------------------------

    def test_06_eligible_count(self):
        from src.services import email_campaigns as svc

        # Unique vertical -> only our 5 eligible rows match anywhere in the DB.
        total = svc.count_eligible(
            county_id=TEST_COUNTY, zips=[], vertical=TEST_VERTICAL,
            exclude_campaign_id=None,
        )
        assert total == 5, "exactly the 5 eligible seed rows"

        # Excluding the (still empty) campaign changes nothing yet.
        excl = svc.count_eligible(
            county_id=TEST_COUNTY, zips=[], vertical=TEST_VERTICAL,
            exclude_campaign_id=STATE["campaign_id"],
        )
        assert excl == 5

    # ------------------------------------------------------------------
    # STEP 6 — Run manual add-contacts (top-up)
    # ------------------------------------------------------------------

    def test_07_add_contacts(self):
        from src.services import email_campaigns as svc
        added = svc.topup_campaign(STATE["campaign_id"])
        assert added == 5, "all 5 eligible contacts pushed"

        # idempotency: a second top-up adds nothing (no eligible remain)
        again = svc.topup_campaign(STATE["campaign_id"])
        assert again == 0

    # ------------------------------------------------------------------
    # STEP 7 — Verify campaign_contacts rows
    # ------------------------------------------------------------------

    def test_08_verify_campaign_contacts(self):
        with get_db_context() as db:
            rows = db.query(CampaignContact).filter(
                CampaignContact.campaign_id == STATE["campaign_id"]
            ).all()
            assert len(rows) == 5
            assert all(r.engagement_status == "active" for r in rows)
            # all map back to our eligible dbpr ids
            cc_dbpr_ids = {r.dbpr_contact_id for r in rows}
            assert cc_dbpr_ids == set(STATE["elig_ids"].values())

        # eligible-count excluding the campaign should now be 0
        from src.services import email_campaigns as svc
        remaining = svc.count_eligible(
            county_id=TEST_COUNTY, zips=[], vertical=TEST_VERTICAL,
            exclude_campaign_id=STATE["campaign_id"],
        )
        assert remaining == 0

    # ------------------------------------------------------------------
    # STEP 8 — Verify Instantly leads were added
    # ------------------------------------------------------------------

    def test_09_verify_instantly_leads(self):
        add_leads = STATE["instantly"]["add_leads"]
        assert add_leads.call_count == 1, "single batch (5 < 1000)"
        camp_id_arg, leads_arg = add_leads.call_args[0]
        assert camp_id_arg == FAKE_INSTANTLY_CAMPAIGN_ID
        assert len(leads_arg) == 5

        # leads carry parsed first/last name from "LAST, FIRST" + company/email
        sample = leads_arg[0]
        assert set(sample).issuperset(
            {"email", "first_name", "last_name", "company_name", "phone"}
        )
        assert all(ld["first_name"] == "JOHN" for ld in leads_arg)
        assert {ld["email"] for ld in leads_arg} == set(STATE["elig_ids"].keys())

    # ------------------------------------------------------------------
    # STEP 9 — Run analytics sync
    # ------------------------------------------------------------------

    def test_10_sync_analytics(self):
        from src.tasks.email_campaign_sync import _sync_analytics
        today = date.today()
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])

        _sync_analytics(camp, today, dry_run=False)

        with get_db_context() as db:
            snap = db.query(CampaignDailyAnalytics).filter_by(
                campaign_id=STATE["campaign_id"], snapshot_date=today,
            ).first()
        assert snap is not None, "analytics snapshot upserted"
        assert snap.emails_sent == 5
        assert snap.opens == 3
        assert snap.replies == 1
        assert snap.clicks == 2
        assert snap.bounces == 1
        assert snap.unsubscribes == 1
        # derived rates: opens/sent = 3/5, replies/sent = 1/5
        assert float(snap.open_rate) == pytest.approx(0.6, abs=1e-4)
        assert float(snap.reply_rate) == pytest.approx(0.2, abs=1e-4)
        # total_contacts backfilled from campaign_contacts count
        assert snap.total_contacts == 5

        # second run upserts the SAME row (unique campaign_id + date), no dup
        _sync_analytics(camp, today, dry_run=False)
        with get_db_context() as db:
            count = db.query(CampaignDailyAnalytics).filter_by(
                campaign_id=STATE["campaign_id"], snapshot_date=today,
            ).count()
        assert count == 1

    # ------------------------------------------------------------------
    # STEP 10 — Verify analytics + per-contact engagement status
    # ------------------------------------------------------------------

    def test_11_sync_lead_statuses(self):
        from src.tasks.email_campaign_sync import _sync_lead_statuses
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])

        result = _sync_lead_statuses(camp, dry_run=False)
        assert result["synced"] == 5

        with get_db_context() as db:
            rows = {
                dc.email: cc.engagement_status
                for cc, dc in db.query(CampaignContact, DBPRContact)
                .join(DBPRContact, CampaignContact.dbpr_contact_id == DBPRContact.id)
                .filter(CampaignContact.campaign_id == STATE["campaign_id"])
                .all()
            }
        assert rows["stgtest-elig-0@example.com"] == "interested"
        assert rows["stgtest-elig-1@example.com"] == "unsubscribed"
        assert rows["stgtest-elig-2@example.com"] == "bounced"
        assert rows["stgtest-elig-3@example.com"] == "active"

    # ------------------------------------------------------------------
    # STEP 12 (part) — Global suppression from unsub / bounce
    # ------------------------------------------------------------------

    def test_15_global_suppression(self):
        """The unsubscribed lead -> is_opted_out; the bounced lead ->
        is_hard_bounced. These survive on dbpr_contacts (global)."""
        with get_db_context() as db:
            unsub = db.query(DBPRContact).filter_by(
                email="stgtest-elig-1@example.com").first()
            bounce = db.query(DBPRContact).filter_by(
                email="stgtest-elig-2@example.com").first()
            interested = db.query(DBPRContact).filter_by(
                email="stgtest-elig-0@example.com").first()

        assert unsub.is_opted_out is True
        assert unsub.is_hard_bounced is False
        assert bounce.is_hard_bounced is True
        assert bounce.is_opted_out is False
        assert interested.is_opted_out is False and interested.is_hard_bounced is False

        # suppressed contacts are now globally ineligible (they're already
        # campaign members so count excludes them anyway; assert the flag path
        # by counting a fresh campaign-less query): opted_out + bounced removed.
        from src.services import email_campaigns as svc
        fresh = svc.count_eligible(
            county_id=TEST_COUNTY, zips=[], vertical=TEST_VERTICAL,
            exclude_campaign_id=None,
        )
        # started at 5 eligible; 1 opted_out + 1 bounced now suppressed -> 3
        assert fresh == 3

    # ------------------------------------------------------------------
    # STEP 11 — Pause / resume / duplicate
    # ------------------------------------------------------------------

    def test_12_pause(self):
        from src.services import email_campaigns as svc
        svc.pause_campaign(STATE["campaign_id"])
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
        assert camp.status == "paused"
        assert STATE["instantly"]["pause_campaign"].call_count >= 1

    def test_13_resume(self):
        from src.services import email_campaigns as svc
        svc.resume_campaign(STATE["campaign_id"])
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
        assert camp.status == "active"
        # resume re-activates in Instantly
        assert STATE["instantly"]["activate_campaign"].call_count >= 2

    def test_14_duplicate(self):
        from src.services import email_campaigns as svc
        out = svc.duplicate_campaign(STATE["campaign_id"])
        assert out.status == "draft", "duplicate is a fresh draft"
        assert out.id != STATE["campaign_id"]
        assert out.instantly_campaign_id is None, "no Instantly campaign yet"
        STATE["dup_campaign_id"] = out.id

        with get_db_context() as db:
            dup = db.get(EmailCampaign, out.id)
            # copies template / filters / cap / dates ...
            assert dup.template_id == STATE["template_id"]
            assert dup.vertical == TEST_VERTICAL
            assert "(copy)" in dup.name
            # ... but NOT contacts or analytics
            cc = db.query(CampaignContact).filter_by(campaign_id=out.id).count()
            an = db.query(CampaignDailyAnalytics).filter_by(campaign_id=out.id).count()
        assert cc == 0 and an == 0

    # ------------------------------------------------------------------
    # Lifecycle check — should NOT auto-complete (active members remain)
    # ------------------------------------------------------------------

    def test_16_lifecycle_no_complete(self):
        from src.tasks.email_campaign_sync import _lifecycle_check
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
        _lifecycle_check(camp, dry_run=False)
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
        # end_date is 30 days out and 'active'/'interested' members remain
        assert camp.status == "active"

    def test_17_lifecycle_completes_past_end_date(self):
        """Force end_date into the past -> lifecycle marks completed +
        pauses the Instantly campaign."""
        from src.tasks.email_campaign_sync import _lifecycle_check
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
            camp.end_date = PAST
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
        _lifecycle_check(camp, dry_run=False)
        with get_db_context() as db:
            camp = db.get(EmailCampaign, STATE["campaign_id"])
        assert camp.status == "completed"
