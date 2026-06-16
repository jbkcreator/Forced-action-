"""
Tests for Sprint S0 reactivation_scheduler.

Coverage:
  - _dispatch: dry-run, SMS, email fallback, no contact, MessageOutcome, cooldown update
  - County-Live cohort: eligible dispatch, cooldown skip, geo-interest skip, limit, filter, dedup
  - Sold-Out cohort: eligible dispatch, zero-supply skip, dedup, lifecycle stop, limit, filter,
                      next-vertical fallback
  - run(): cohort routing, result aggregation, dry_run flag, global limit split
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


# ── Factories ─────────────────────────────────────────────────────────────────

def _sub(
    sub_id=1,
    status="churned",
    churned_at=None,
    is_trial=False,
    trial_ends_at=None,
    last_reactivation_attempt_at=None,
    email="user@example.com",
    phone="+13055551234",
    name="John Doe",
    vertical="roofing",
    county_id="hillsborough",
):
    sub = MagicMock()
    sub.id = sub_id
    sub.status = status
    sub.churned_at = churned_at or datetime.now(timezone.utc) - timedelta(days=30)
    sub.is_trial = is_trial
    sub.trial_ends_at = trial_ends_at
    sub.last_reactivation_attempt_at = last_reactivation_attempt_at
    sub.email = email
    sub.phone = phone
    sub.name = name
    sub.vertical = vertical
    sub.county_id = county_id
    return sub


def _zip_row(zip_code="33701", county_id="hillsborough"):
    r = MagicMock()
    r.zip_code = zip_code
    r.county_id = county_id
    return r


def _scalars_mock(values):
    """Mock for db.execute(...).scalars().all() → values."""
    m = MagicMock()
    m.scalars.return_value.all.return_value = values
    return m


def _all_mock(values):
    """Mock for db.execute(...).all() → values."""
    m = MagicMock()
    m.all.return_value = values
    return m


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DISPATCH
# ═══════════════════════════════════════════════════════════════════════════════

class TestDispatch:
    def _db(self):
        db = MagicMock()
        db.add = MagicMock()
        db.execute = MagicMock(return_value=MagicMock())
        db.flush = MagicMock()
        return db

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_dry_run_does_not_call_send_sms(self, mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        result = _dispatch(_sub(), "county_live", self._db(), dry_run=True, county_id="hillsborough")
        assert result is True
        mock_sms.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_dry_run_does_not_write_message_outcome(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        db = self._db()
        _dispatch(_sub(), "county_live", db, dry_run=True, county_id="hillsborough")
        db.add.assert_not_called()
        db.flush.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_dry_run_does_not_update_cooldown(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        db = self._db()
        _dispatch(_sub(), "county_live", db, dry_run=True, county_id="hillsborough")
        update_calls = [
            c for c in db.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper()
        ]
        assert len(update_calls) == 0

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_sms_sent_when_phone_present(self, mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        sub = _sub(phone="+13055551234")
        result = _dispatch(sub, "county_live", self._db(), dry_run=False, county_id="hillsborough")
        assert result is True
        mock_sms.assert_called_once()
        assert mock_sms.call_args.args[0] == "+13055551234"

    @patch("src.tasks.reactivation_scheduler.send_sms")
    @patch("src.tasks.reactivation_scheduler.send_email", return_value=True)
    def test_email_fallback_when_no_phone(self, mock_email, mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        sub = _sub(phone=None, email="user@example.com")
        result = _dispatch(sub, "county_live", self._db(), dry_run=False, county_id="hillsborough")
        assert result is True
        mock_sms.assert_not_called()
        mock_email.assert_called_once()
        assert mock_email.call_args.args[0] == "user@example.com"

    @patch("src.tasks.reactivation_scheduler.send_sms")
    @patch("src.tasks.reactivation_scheduler.send_email")
    def test_no_contact_info_returns_false(self, mock_email, mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        sub = _sub(phone=None, email=None)
        result = _dispatch(sub, "county_live", self._db(), dry_run=False, county_id="hillsborough")
        assert result is False
        mock_sms.assert_not_called()
        mock_email.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_message_outcome_written_with_correct_template(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch, CAMPAIGN_COUNTY_LIVE
        from src.core.models import MessageOutcome
        db = self._db()
        _dispatch(_sub(), "county_live", db, dry_run=False, county_id="hillsborough")
        db.add.assert_called_once()
        outcome = db.add.call_args.args[0]
        assert isinstance(outcome, MessageOutcome)
        assert outcome.template_id == CAMPAIGN_COUNTY_LIVE
        assert outcome.subscriber_id == 1
        assert outcome.county_id == "hillsborough"

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_sold_out_outcome_has_zip_and_vertical(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch, CAMPAIGN_SOLD_OUT
        from src.core.models import MessageOutcome
        db = self._db()
        _dispatch(
            _sub(), "sold_out", db, dry_run=False,
            county_id="hillsborough", zip_code="33701", vertical="roofing",
        )
        outcome = db.add.call_args.args[0]
        assert isinstance(outcome, MessageOutcome)
        assert outcome.template_id == CAMPAIGN_SOLD_OUT
        assert outcome.context_snapshot["zip_code"] == "33701"
        assert outcome.context_snapshot["vertical"] == "roofing"

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_cooldown_updated_with_current_timestamp(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        db = self._db()
        before = datetime.now(timezone.utc)
        _dispatch(_sub(), "county_live", db, dry_run=False, county_id="hillsborough")
        update_calls = [
            c for c in db.execute.call_args_list
            if "UPDATE" in str(c.args[0]).upper()
        ]
        assert len(update_calls) == 1
        params = update_calls[0].args[1]
        assert "now" in params
        assert params["now"] >= before

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=False)
    def test_returns_false_when_send_fails(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        db = self._db()
        result = _dispatch(_sub(), "county_live", db, dry_run=False, county_id="hillsborough")
        assert result is False
        db.add.assert_not_called()
        db.flush.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    def test_flush_called_after_outcome_write(self, _mock_sms):
        from src.tasks.reactivation_scheduler import _dispatch
        db = self._db()
        _dispatch(_sub(), "county_live", db, dry_run=False, county_id="hillsborough")
        db.flush.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. COUNTY-LIVE COHORT
# ═══════════════════════════════════════════════════════════════════════════════

class TestCountyLiveCohort:
    def _db(self, counties=("hillsborough",), subs=None):
        if subs is None:
            subs = [_sub()]
        db = MagicMock()
        db.execute.side_effect = [
            _scalars_mock(list(counties)),  # expansion_candidates
            _all_mock(subs),                # _fetch_subscribers
            MagicMock(),                    # UPDATE last_reactivation_attempt_at
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        return db

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1])
    def test_eligible_sub_is_sent(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        result = _run_county_live(self._db(), dry_run=False, limit=None, county_id_filter=None)
        assert result["sent"] == 1
        assert result["eligible"] == 1
        mock_sms.assert_called_once()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility", return_value=(False, "on_cooldown"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1])
    def test_cooldown_subscriber_is_skipped(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        db = self._db()
        result = _run_county_live(db, dry_run=False, limit=None, county_id_filter=None)
        assert result["sent"] == 0
        assert result["skipped"] == 1
        mock_sms.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility",
           return_value=(False, "no_county_interest:hillsborough"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1])
    def test_no_geo_interest_is_skipped(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        result = _run_county_live(self._db(), dry_run=False, limit=None, county_id_filter=None)
        assert result["skipped"] == 1
        mock_sms.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1, 2, 3, 4, 5])
    def test_limit_caps_sends(self, _geo, _elig, _sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        subs = [_sub(sub_id=i) for i in range(1, 6)]
        db = MagicMock()
        db.execute.side_effect = [
            _scalars_mock(["hillsborough"]),
            _all_mock(subs),
            MagicMock(), MagicMock(),  # UPDATEs for sends
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        result = _run_county_live(db, dry_run=False, limit=2, county_id_filter=None)
        assert result["sent"] <= 2

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1])
    def test_county_id_filter_bound_in_query(self, _geo, _elig, _sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        db = self._db()
        _run_county_live(db, dry_run=False, limit=None, county_id_filter="pinellas")
        params = db.execute.call_args_list[0].args[1]
        assert params["county_filter"] == "pinellas"

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1])
    def test_dedup_prevents_double_send_across_two_counties(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        sub = _sub(sub_id=1)
        db = MagicMock()
        # Two counties both return sub_id=1; after first send, sub is in sent_this_run
        # so second county fetch is skipped entirely
        db.execute.side_effect = [
            _scalars_mock(["hillsborough", "pinellas"]),
            _all_mock([sub]),   # fetch for hillsborough
            MagicMock(),        # UPDATE after hillsborough send
            # pinellas: new_ids=[] → _fetch_subscribers returns early → no db.execute call
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        result = _run_county_live(db, dry_run=False, limit=None, county_id_filter=None)
        assert result["sent"] == 1
        assert mock_sms.call_count == 1

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_county_live_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_county", return_value=[1])
    def test_dry_run_county_live_counts_without_sending(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_county_live
        db = self._db()
        result = _run_county_live(db, dry_run=True, limit=None, county_id_filter=None)
        assert result["sent"] == 1   # dry_run counts as "would send"
        mock_sms.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SOLD-OUT COHORT
# ═══════════════════════════════════════════════════════════════════════════════

class TestSoldOutCohort:
    def _db(self, zip_rows=None, verticals=("roofing",), subs=None):
        if zip_rows is None:
            zip_rows = [_zip_row()]
        if subs is None:
            subs = [_sub()]
        db = MagicMock()
        db.execute.side_effect = [
            _all_mock(zip_rows),            # gold_plus_zip_snapshots
            _scalars_mock(list(verticals)), # _get_zip_verticals
            _all_mock(subs),                # _fetch_subscribers
            MagicMock(),                    # UPDATE last_reactivation_attempt_at
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        return db

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_eligible_sub_is_sent(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        result = _run_sold_out(self._db(), dry_run=False, limit=None, zip_code_filter=None)
        assert result["sent"] == 1
        mock_sms.assert_called_once()

    @patch("src.tasks.reactivation_scheduler.send_sms")
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip")
    def test_zip_with_zero_supply_not_fetched(self, mock_geo, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        # SQL WHERE gold_plus_lead_count > 0 filters out zero-supply rows
        db = MagicMock()
        db.execute.side_effect = [_all_mock([])]
        result = _run_sold_out(db, dry_run=False, limit=None, zip_code_filter=None)
        assert result["sent"] == 0
        mock_geo.assert_not_called()
        mock_sms.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_dedup_prevents_double_send_across_two_zips(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        sub = _sub(sub_id=1)
        db = MagicMock()
        db.execute.side_effect = [
            _all_mock([_zip_row("33701"), _zip_row("33702")]),
            _scalars_mock(["roofing"]),   # verticals for 33701
            _all_mock([sub]),             # fetch for 33701
            MagicMock(),                  # UPDATE after 33701 send
            _scalars_mock(["roofing"]),   # verticals for 33702
            # sub already in sent_this_run → sub_verticals empty → no fetch
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        result = _run_sold_out(db, dry_run=False, limit=None, zip_code_filter=None)
        assert result["sent"] == 1
        assert mock_sms.call_count == 1

    @patch("src.tasks.reactivation_scheduler.send_sms")
    @patch("src.tasks.reactivation_scheduler.send_email", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_email_fallback_when_no_phone(self, _geo, _elig, mock_email, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        sub = _sub(phone=None, email="user@example.com")
        result = _run_sold_out(self._db(subs=[sub]), dry_run=False, limit=None, zip_code_filter=None)
        assert result["sent"] == 1
        mock_sms.assert_not_called()
        mock_email.assert_called_once()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(False, "on_cooldown"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_lifecycle_cooldown_skips_subscriber(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        result = _run_sold_out(self._db(), dry_run=False, limit=None, zip_code_filter=None)
        assert result["sent"] == 0
        assert result["skipped"] == 1
        mock_sms.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1, 2, 3, 4])
    def test_limit_caps_sends(self, _geo, _elig, _sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        subs = [_sub(sub_id=i) for i in range(1, 5)]
        db = MagicMock()
        db.execute.side_effect = [
            _all_mock([_zip_row()]),
            _scalars_mock(["roofing"]),
            _all_mock(subs),
            MagicMock(), MagicMock(),  # UPDATEs
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        result = _run_sold_out(db, dry_run=False, limit=2, zip_code_filter=None)
        assert result["sent"] <= 2

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_zip_code_filter_bound_in_query(self, _geo, _elig, _sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        db = self._db()
        _run_sold_out(db, dry_run=False, limit=None, zip_code_filter="33701")
        params = db.execute.call_args_list[0].args[1]
        assert params["zip_filter"] == "33701"

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch(
        "src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility",
        side_effect=[(False, "no_supply:33701"), (True, "eligible")],
    )
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_tries_next_vertical_when_first_has_no_supply(self, _geo, mock_elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        sub = _sub()
        db = MagicMock()
        db.execute.side_effect = [
            _all_mock([_zip_row()]),
            _scalars_mock(["roofing", "restoration"]),
            _all_mock([sub]),
            MagicMock(),
        ]
        db.add = MagicMock()
        db.flush = MagicMock()
        result = _run_sold_out(db, dry_run=False, limit=None, zip_code_filter=None)
        assert result["sent"] == 1
        assert mock_elig.call_count == 2  # tried roofing (fail), then restoration (pass)

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_dry_run_sold_out_counts_without_sending(self, _geo, _elig, mock_sms):
        from src.tasks.reactivation_scheduler import _run_sold_out
        result = _run_sold_out(self._db(), dry_run=True, limit=None, zip_code_filter=None)
        assert result["sent"] == 1
        mock_sms.assert_not_called()

    @patch("src.tasks.reactivation_scheduler.send_sms", return_value=True)
    @patch("src.tasks.reactivation_scheduler.check_sold_out_zip_eligibility", return_value=(True, "eligible"))
    @patch("src.tasks.reactivation_scheduler.get_subscribers_interested_in_zip", return_value=[1])
    def test_campaign_logged_as_sold_out(self, _geo, _elig, _sms):
        from src.tasks.reactivation_scheduler import _run_sold_out, CAMPAIGN_SOLD_OUT
        from src.core.models import MessageOutcome
        db = self._db()
        _run_sold_out(db, dry_run=False, limit=None, zip_code_filter=None)
        outcome = db.add.call_args.args[0]
        assert isinstance(outcome, MessageOutcome)
        assert outcome.template_id == CAMPAIGN_SOLD_OUT


# ═══════════════════════════════════════════════════════════════════════════════
# 4. RUN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunOrchestrator:
    _CL_RESULT = {"checked": 3, "eligible": 2, "sent": 2, "skipped": 1, "errors": 0}
    _SO_RESULT = {"checked": 2, "eligible": 1, "sent": 1, "skipped": 1, "errors": 0}

    @patch("src.tasks.reactivation_scheduler._run_sold_out", return_value=_SO_RESULT)
    @patch("src.tasks.reactivation_scheduler._run_county_live", return_value=_CL_RESULT)
    def test_run_all_calls_both_cohorts_and_aggregates(self, mock_cl, mock_so):
        from src.tasks.reactivation_scheduler import run
        result = run(cohort="all", db=MagicMock())
        mock_cl.assert_called_once()
        mock_so.assert_called_once()
        assert result["sent"] == 3
        assert result["checked"] == 5

    @patch("src.tasks.reactivation_scheduler._run_sold_out")
    @patch("src.tasks.reactivation_scheduler._run_county_live", return_value=_CL_RESULT)
    def test_run_county_live_only(self, mock_cl, mock_so):
        from src.tasks.reactivation_scheduler import run
        result = run(cohort="county_live", db=MagicMock())
        assert result["cohort"] == "county_live"
        mock_cl.assert_called_once()
        mock_so.assert_not_called()

    @patch("src.tasks.reactivation_scheduler._run_county_live")
    @patch("src.tasks.reactivation_scheduler._run_sold_out", return_value=_SO_RESULT)
    def test_run_sold_out_only(self, mock_so, mock_cl):
        from src.tasks.reactivation_scheduler import run
        result = run(cohort="sold_out", db=MagicMock())
        assert result["cohort"] == "sold_out"
        mock_so.assert_called_once()
        mock_cl.assert_not_called()

    @patch("src.tasks.reactivation_scheduler._run_sold_out", return_value={"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0})
    @patch("src.tasks.reactivation_scheduler._run_county_live", return_value={"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0})
    def test_dry_run_flag_set_in_result(self, _cl, _so):
        from src.tasks.reactivation_scheduler import run
        result = run(cohort="all", dry_run=True, db=MagicMock())
        assert result["dry_run"] is True

    @patch("src.tasks.reactivation_scheduler._run_sold_out", return_value={"checked": 2, "eligible": 2, "sent": 2, "skipped": 0, "errors": 0})
    @patch("src.tasks.reactivation_scheduler._run_county_live", return_value=_CL_RESULT)
    def test_global_limit_splits_correctly_across_cohorts(self, mock_cl, mock_so):
        """County-live runs first; remaining capacity is forwarded to sold-out."""
        from src.tasks.reactivation_scheduler import run
        run(cohort="all", limit=5, db=MagicMock())
        # county_live received full limit=5
        cl_limit = mock_cl.call_args.args[2]
        assert cl_limit == 5
        # sold_out received remaining = 5 - 2 (sent by county_live) = 3
        so_limit = mock_so.call_args.args[2]
        assert so_limit == 3

    @patch("src.tasks.reactivation_scheduler._run_sold_out", return_value={"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0})
    @patch("src.tasks.reactivation_scheduler._run_county_live", return_value={"checked": 0, "eligible": 0, "sent": 0, "skipped": 0, "errors": 0})
    def test_result_contains_all_expected_keys(self, _cl, _so):
        from src.tasks.reactivation_scheduler import run
        result = run(cohort="all", db=MagicMock())
        assert set(result.keys()) == {"cohort", "checked", "eligible", "sent", "skipped", "errors", "dry_run"}
