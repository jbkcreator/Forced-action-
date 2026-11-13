"""P2.1 Speed-to-Lead: instant text, Josh -> David call cascade, Voice AI only on consented leads.

Our code decides and tags; GHL workflows text and dial. These tests pin every gate before a tag, the order of the
tags, and that nothing ever fails a lead's delivery."""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from config.lending_speed_to_lead import (
    CALL_BLOCKED,
    CALL_FAILED,
    CALL_OFF,
    CALL_QUEUED,
    CALL_TAGGED,
    GHL_TAG_AI_VOICE_OK,
    GHL_TAG_SPEED_TO_LEAD,
    GHL_TAG_WEB_INTRO_READY,
    TEXT_BY_LENDINGFLOW,
    TEXT_HELD,
    TEXT_NOT_ALLOWED,
    TEXT_OFF,
    TEXT_TAGGED,
)
from src.lending import speed_to_lead

NOW = datetime(2026, 11, 13, 15, 0, tzinfo=timezone.utc)
PHONE = "+18135550100"


class FakeTags:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def add_tag(self, contact_id, tag):
        if self.fail:
            raise RuntimeError("GHL down")
        self.calls.append(("add", contact_id, tag))

    def remove_tag(self, contact_id, tag):
        if self.fail:
            raise RuntimeError("GHL down")
        self.calls.append(("remove", contact_id, tag))


class _Result:
    def __init__(self, row=None, scalar=None):
        self._row, self._scalar = row, scalar

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalar(self):
        return self._scalar


class FakeDb:
    """Answers start()'s statements: the lead, the claim, the queue lock, the "must wait" read and the record."""

    def __init__(self, lead, *, claimed=True, busy=False, locked_elsewhere=False):
        self.lead, self.claimed, self.busy, self.records, self.locks = lead, claimed, busy, [], 0
        self.locked_elsewhere = locked_elsewhere
        self.marks: list[dict] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM lending.lendingflow_leads WHERE id" in sql or "FROM lending.web_leads WHERE id" in sql:
            return _Result(row=self.lead)
        if "pg_try_advisory_xact_lock" in sql:
            assert params == {"key": 7_302_020_811}
            self.locks += 1
            return _Result(scalar=not self.locked_elsewhere)
        if "call_tagged_at > :spacing_since" in sql:
            assert self.locks == 1  # read under the queue lock
            return _Result(scalar=self.busy)
        if "INSERT INTO lending.speed_to_lead" in sql:
            return _Result(scalar=9 if self.claimed else None)
        if "SET call_status = :starting" in sql:
            self.marks.append(params)
            return _Result()
        if "UPDATE lending.speed_to_lead" in sql:
            self.records.append(params)
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    @contextmanager
    def begin_nested(self):
        yield


def web_lead(**over):
    lead = {"id": 7, "phone": PHONE, "email": "lead@example.com", "property_state": "FL", "property_zip": None,
            "received_at": NOW - timedelta(seconds=3), "ghl_contact_id": "c-7", "suppressed": False,
            "handled_in_ghl_at": None, "intro_sms_tagged_at": None, "sms_consent": True, "call_consent": True, "ai_call_consent": True,
            "ai_call_consent_text_matches": True}
    lead.update(over)
    return lead


def lendingflow_lead(**over):
    lead = web_lead(id=5, ghl_contact_id="c-5", sms_consent=False, call_consent=False, ai_call_consent=False,
                    ai_call_consent_text_matches=False,
                    intro_sms_tagged_at=NOW - timedelta(seconds=1))
    lead.update(over)
    return lead


@pytest.fixture
def gates(monkeypatch):
    """Everything allowed: switches on, consent, window open, call gate clear, not halted. Tests flip one."""
    state = SimpleNamespace(speed=True, intro=True, intro_field="cf-intro", carrier=True, halted=False, consent=True,
                            window=True, block=None, scrubbed=[], scrub_fail=False, hours=True)
    monkeypatch.setattr(speed_to_lead, "callers_hours_open",
                        lambda now: state.hours(now) if callable(state.hours) else state.hours)
    monkeypatch.setattr(speed_to_lead, "get_settings", lambda: SimpleNamespace(
        lending_speed_to_lead_enabled=state.speed, lending_intro_sms_enabled=state.intro,
        lending_ghl_cf_intro_name=state.intro_field, lending_sms_carrier_approved=state.carrier))
    monkeypatch.setattr(speed_to_lead, "is_globally_halted", lambda: state.halted)
    monkeypatch.setattr(speed_to_lead, "has_text_consent", lambda db, phone: state.consent)
    monkeypatch.setattr(speed_to_lead, "text_window_open", lambda phone, **kw: state.window)

    def blocked(db, phones, *, now, halted):
        return {phones[0]: state.block} if state.block else {}

    def scrub(db, phones, *, scrubber, max_credits, now=None):
        if state.scrub_fail:
            raise RuntimeError("tracerfy down")
        state.scrubbed.append((tuple(phones), max_credits))

    monkeypatch.setattr("src.lending.ghl_call_gate.blocked_reasons", blocked)
    monkeypatch.setattr("src.lending.ghl_list_scrub.list_scrub", scrub)
    return state


def run(lead, source="web", tags=None, **kw):
    tags = tags or FakeTags()
    db = FakeDb(lead, **kw)
    outcome = speed_to_lead.start(db, source, lead["id"], tags=tags, scrubber=lambda phones: [], now=NOW)
    return outcome, tags, db


# ── the happy paths ──────────────────────────────────────────────────────────

def test_a_consented_website_lead_gets_the_text_then_the_cascade_with_voice_ai(gates):
    outcome, tags, db = run(web_lead())
    assert tags.calls == [
        ("remove", "c-7", GHL_TAG_WEB_INTRO_READY),  # re-armed: a returning contact still carries it
        ("add", "c-7", GHL_TAG_WEB_INTRO_READY),
        ("add", "c-7", GHL_TAG_AI_VOICE_OK),        # before the cascade tag: the workflow reads it at 2 minutes
        ("remove", "c-7", GHL_TAG_SPEED_TO_LEAD),    # removed then added, so GHL's tag trigger fires again
        ("add", "c-7", GHL_TAG_SPEED_TO_LEAD),
    ]
    assert (outcome.text_status, outcome.call_status, outcome.voice_eligible) == (TEXT_TAGGED, CALL_TAGGED, True)
    assert gates.scrubbed == [((PHONE,), 1)]  # one fresh DNC scrub, one credit
    record = db.records[0]
    assert record["text_tagged_at"] == NOW and record["call_tagged_at"] == NOW and record["voice"] is True


def test_a_lendingflow_lead_gets_the_cascade_but_never_voice_ai_and_no_scrub(gates):
    outcome, tags, db = run(lendingflow_lead(), source="lendingflow")
    assert tags.calls == [("remove", "c-5", GHL_TAG_AI_VOICE_OK), ("remove", "c-5", GHL_TAG_SPEED_TO_LEAD),
                          ("add", "c-5", GHL_TAG_SPEED_TO_LEAD)]
    assert outcome.text_status == TEXT_BY_LENDINGFLOW and outcome.voice_eligible is False
    assert db.records[0]["text_tagged_at"] == NOW - timedelta(seconds=1)  # T-09's own tag time, for the report
    assert gates.scrubbed == []  # LendingFlow leads need no scrub (client decision)


# ── Voice AI only on a recorded call consent ─────────────────────────────────

@pytest.mark.parametrize("over", [{"ai_call_consent_text_matches": False}, {"suppressed": True},
                                  {"ai_call_consent": False}])
def test_voice_ai_needs_the_exact_consent_text_and_no_opt_out(gates, over):
    assert speed_to_lead.voice_eligible(web_lead(**over)) is False


def test_the_call_box_alone_is_not_consent_to_the_ai_call(gates):
    outcome, tags, _ = run(web_lead(ai_call_consent=False, ai_call_consent_text_matches=False))
    assert outcome.call_status == CALL_TAGGED and outcome.voice_eligible is False  # Josh and David still rung
    assert ("add", "c-7", GHL_TAG_AI_VOICE_OK) not in tags.calls


def test_the_ai_box_alone_starts_no_calls_at_all(gates):
    outcome, tags, _ = run(web_lead(call_consent=False))
    assert (outcome.call_status, outcome.call_block_reason) == (CALL_BLOCKED, "NO_CALL_CONSENT")
    assert not [c for c in tags.calls if c[2] in (GHL_TAG_SPEED_TO_LEAD, GHL_TAG_AI_VOICE_OK)]


def test_a_contact_without_call_consent_text_match_is_called_but_its_old_voice_tag_is_removed(gates):
    outcome, tags, _ = run(web_lead(ai_call_consent_text_matches=False))
    assert ("remove", "c-7", GHL_TAG_AI_VOICE_OK) in tags.calls and ("add", "c-7", GHL_TAG_AI_VOICE_OK) not in tags.calls
    assert outcome.call_status == CALL_TAGGED and outcome.voice_eligible is False


# ── every gate before the cascade ────────────────────────────────────────────

def test_a_website_lead_without_the_call_box_gets_no_cascade_and_no_scrub(gates):
    outcome, tags, _ = run(web_lead(call_consent=False))
    assert (outcome.call_status, outcome.call_block_reason) == (CALL_BLOCKED, "NO_CALL_CONSENT")
    assert not [c for c in tags.calls if c[2] in (GHL_TAG_SPEED_TO_LEAD, GHL_TAG_AI_VOICE_OK)]
    assert gates.scrubbed == []


@pytest.mark.parametrize("reason", ["SUPPRESSED", "NATIONAL_DNC", "REACHED_IN_BATCHDIALER", "LITIGATOR",
                                    "ATTEMPT_HISTORY_EXCEEDED", "SOMETHING_NEW"])
def test_a_lasting_gate_reason_blocks_the_cascade_for_good(gates, reason):
    gates.block = reason
    outcome, tags, db = run(web_lead())
    assert (outcome.call_status, outcome.call_block_reason) == (CALL_BLOCKED, reason)  # unknown reasons fail closed
    assert not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD] and db.records[0]["queued_at"] is None


@pytest.mark.parametrize("reason", ["OUTSIDE_CALL_WINDOW", "ATTEMPT_CAP_REACHED", "NO_FRESH_SCRUB", "SCRUB_FAILED"])
def test_a_not_now_gate_reason_queues_the_lead(gates, reason):
    gates.block = reason
    outcome, tags, db = run(web_lead())
    assert (outcome.call_status, outcome.queue_reason, outcome.call_block_reason) == (CALL_QUEUED, reason, None)
    assert not [c for c in tags.calls if c[2] in (GHL_TAG_SPEED_TO_LEAD, GHL_TAG_AI_VOICE_OK)]
    assert db.records[0]["queued_at"] == NOW and db.records[0]["queue_reason"] == reason


def test_a_lead_already_handled_in_ghl_is_never_called(gates):
    outcome, tags, _ = run(web_lead(handled_in_ghl_at=NOW))
    assert (outcome.call_status, outcome.call_block_reason) == (CALL_BLOCKED, "HANDLED_IN_GHL")


def test_a_failed_scrub_is_left_to_the_gate_which_queues_an_unscrubbed_number(gates):
    gates.scrub_fail, gates.block = True, "NO_FRESH_SCRUB"
    outcome, tags, _ = run(web_lead())
    assert outcome.call_status == CALL_QUEUED and not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD]


def test_the_emergency_halt_withholds_every_tag_and_the_call_waits_in_the_queue(gates):
    gates.halted = True
    outcome, tags, _ = run(web_lead())
    assert tags.calls == []
    assert (outcome.text_status, outcome.call_status, outcome.queue_reason) == (
        TEXT_NOT_ALLOWED, CALL_QUEUED, "EMERGENCY_STOP")  # like the leads queued before the halt


def test_switches_off_mean_no_tags(gates):
    gates.speed = gates.intro = False
    outcome, tags, _ = run(web_lead())
    assert tags.calls == [] and (outcome.text_status, outcome.call_status) == (TEXT_OFF, CALL_OFF)


def test_the_intro_text_needs_the_greeting_field_too(gates):
    gates.intro_field = None
    outcome, tags, _ = run(web_lead())
    assert outcome.text_status == TEXT_OFF and ("add", "c-7", GHL_TAG_WEB_INTRO_READY) not in tags.calls


def test_no_website_intro_text_before_carrier_approval(gates):
    gates.carrier = False
    outcome, tags, _ = run(web_lead())
    assert outcome.text_status == TEXT_OFF and ("add", "c-7", GHL_TAG_WEB_INTRO_READY) not in tags.calls
    assert outcome.call_status == CALL_TAGGED  # calls are not texts: the cascade still runs


def test_a_lead_that_reaches_ghl_late_is_queued_not_skipped(gates):
    outcome, tags, db = run(web_lead(received_at=NOW - timedelta(minutes=6)))
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "delayed")
    assert not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD] and db.locks == 0
    assert outcome.text_status == TEXT_TAGGED  # the intro text still goes now


def test_a_lead_is_fresh_for_five_minutes(gates):
    on_the_edge, _, _ = run(web_lead(received_at=NOW - timedelta(minutes=5)))
    past_it, _, _ = run(web_lead(received_at=NOW - timedelta(minutes=5, seconds=1)))
    assert (on_the_edge.call_status, past_it.call_status) == (CALL_TAGGED, CALL_QUEUED)


def test_outside_the_callers_hours_the_call_is_queued_and_the_text_still_goes(gates):
    gates.hours = False
    outcome, tags, _ = run(web_lead())
    assert (outcome.call_status, outcome.queue_reason, outcome.text_status) == (CALL_QUEUED, "outside_hours", TEXT_TAGGED)
    assert tags.calls == [("remove", "c-7", GHL_TAG_WEB_INTRO_READY), ("add", "c-7", GHL_TAG_WEB_INTRO_READY)]


def test_a_fresh_lead_waits_its_turn_while_another_cascade_runs(gates):
    outcome, tags, db = run(web_lead(), busy=True)
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "waiting_turn") and db.locks == 1
    assert not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD]


def test_a_fresh_lead_never_waits_for_the_lock_it_queues_instead(gates):
    outcome, tags, db = run(web_lead(), locked_elsewhere=True)  # a delivery batch or the queue job holds it
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "waiting_turn")
    assert not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD]


def test_the_spacing_is_measured_after_the_lock_not_at_arrival(gates):
    times = iter([NOW, NOW + timedelta(seconds=30)])  # the scrub took 30 s
    tags, db = FakeTags(), FakeDb(web_lead())
    outcome = speed_to_lead.start(db, "web", 7, tags=tags, scrubber=lambda p: [], clock=lambda: next(times))
    assert outcome.call_status == CALL_TAGGED and db.records[0]["call_tagged_at"] == NOW + timedelta(seconds=30)


def test_the_fresh_start_takes_the_queue_lock_before_tagging(gates):
    outcome, _, db = run(web_lead())
    assert outcome.call_status == CALL_TAGGED and db.locks == 1


def test_voice_ai_is_not_limited_by_how_long_the_lead_waited(gates):
    assert speed_to_lead.voice_eligible(web_lead(received_at=NOW - timedelta(hours=20))) is True


def test_the_texts_last_chance_is_24_hours(gates):
    on_the_edge, _, _ = run(web_lead(received_at=NOW - timedelta(hours=24)))
    past_it, tags, _ = run(web_lead(received_at=NOW - timedelta(hours=24, seconds=1)))
    assert (on_the_edge.text_status, past_it.text_status) == (TEXT_TAGGED, "too_late")
    assert ("add", "c-7", GHL_TAG_WEB_INTRO_READY) not in tags.calls


def test_a_missed_target_is_logged(gates, caplog):
    run(web_lead(received_at=NOW - timedelta(seconds=90)))  # inside the last chance, past the 60 s target
    assert any("call missed its 60s target" in r.getMessage() for r in caplog.records)
    assert any("text missed its 5s target" in r.getMessage() for r in caplog.records)


def test_lendingflow_s_own_intro_text_also_stops_after_24_hours(monkeypatch):
    from src.lending import lendingflow

    monkeypatch.setattr(lendingflow, "_intro_sms_on", lambda: True)
    monkeypatch.setattr(lendingflow, "text_window_open", lambda *a, **k: True)
    monkeypatch.setattr(lendingflow, "_sms_clear", lambda db, lead: True)
    lead = {"id": 5, "phone": PHONE, "property_state": "FL", "property_zip": None}
    assert lendingflow._may_text_now(None, {**lead, "received_at": NOW - timedelta(hours=24)}, NOW) is True
    assert lendingflow._may_text_now(None, {**lead, "received_at": NOW - timedelta(hours=25)}, NOW) is False


# ── the intro text gates ─────────────────────────────────────────────────────

@pytest.mark.parametrize("over,consent", [({"sms_consent": False}, True), ({"suppressed": True}, True), ({}, False)])
def test_no_intro_text_without_consent(gates, over, consent):
    gates.consent = consent
    outcome, tags, _ = run(web_lead(**over))
    assert outcome.text_status == TEXT_NOT_ALLOWED and ("add", "c-7", GHL_TAG_WEB_INTRO_READY) not in tags.calls


def test_an_intro_text_outside_the_window_is_held(gates):
    gates.window = False
    outcome, tags, _ = run(web_lead())
    assert outcome.text_status == TEXT_HELD and ("add", "c-7", GHL_TAG_WEB_INTRO_READY) not in tags.calls


# ── once per lead, and never in the way of a delivery ────────────────────────

def test_a_retried_delivery_never_starts_a_second_cascade(gates):
    outcome, tags, _ = run(web_lead(), claimed=False)
    assert outcome is None and tags.calls == []


def test_a_lead_not_yet_in_ghl_is_skipped(gates):
    outcome, tags, _ = run(web_lead(ghl_contact_id=None))
    assert outcome is None and tags.calls == []


def test_a_failed_ghl_tag_is_recorded_and_never_raised(gates):
    outcome, _, db = run(web_lead(), tags=FakeTags(fail=True))
    assert outcome.call_status == CALL_FAILED and outcome.text_status == TEXT_NOT_ALLOWED
    assert db.records[0]["call_status"] == CALL_FAILED


def test_start_safely_never_raises(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("db down: +18135550100")

    monkeypatch.setattr(speed_to_lead, "start", broken)
    assert speed_to_lead.start_safely(FakeDb(web_lead()), "web", 7, tags=FakeTags()) is None
    assert speed_to_lead.start_safely(FakeDb(web_lead()), "web", 7, tags=None) is None


def test_an_unknown_source_is_refused():
    with pytest.raises(ValueError):
        speed_to_lead.start(FakeDb(web_lead()), "partner", 7, tags=FakeTags())


# ── live wiring ──────────────────────────────────────────────────────────────

def test_the_live_step_is_off_until_a_switch_is_on(gates, monkeypatch):
    monkeypatch.setattr(speed_to_lead, "live_tag_client", lambda: FakeTags())
    gates.speed = gates.intro = False
    assert speed_to_lead.live_step("web") is None and speed_to_lead.live_step("lendingflow") is None
    gates.intro = True  # the website intro text alone is enough for website leads, not for LendingFlow ones
    assert speed_to_lead.live_step("web") is not None and speed_to_lead.live_step("lendingflow") is None
    gates.speed = True
    assert speed_to_lead.live_step("lendingflow") is not None


def test_the_live_step_needs_the_ghl_account(gates, monkeypatch):
    monkeypatch.setattr(speed_to_lead, "live_tag_client", lambda: None)
    assert speed_to_lead.live_step("web") is None


def test_web_delivery_runs_the_step_only_for_leads_that_reached_ghl(monkeypatch):
    from src.lending import web_leads

    rows = [{"id": 1}, {"id": 2}]

    class Db:
        def execute(self, statement, params=None):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    monkeypatch.setattr(web_leads, "_deliver_one", lambda db, sink, lead, now: 1 if lead["id"] == 1 else 0)
    seen = []
    assert web_leads.deliver_pending(Db(), object(), after_delivery=lambda db, lead_id: seen.append(lead_id)) == 1
    assert seen == [1]


def test_lendingflow_follow_ups_run_the_step_before_the_event(monkeypatch):
    from src.lending import lendingflow

    order = []
    monkeypatch.setattr(lendingflow, "_emit_event", lambda db, lead: order.append("event") or False)
    db = SimpleNamespace(commit=lambda: order.append("commit"))
    lendingflow.run_followups(db, [lendingflow.FollowUp({"id": 5})],
                              after_delivery=lambda db, lead_id: order.append(("step", lead_id)))
    assert order == [("step", 5), "commit", "event"]


def test_a_failed_speed_to_lead_commit_does_not_skip_the_rest_of_the_batch(monkeypatch):
    from src.lending import lendingflow

    emitted, rolled_back = [], []

    def commit():
        raise RuntimeError("connection lost")

    monkeypatch.setattr(lendingflow, "_emit_event", lambda db, lead: emitted.append(lead["id"]) or False)
    db = SimpleNamespace(commit=commit, rollback=lambda: rolled_back.append(1))
    lendingflow.run_followups(db, [lendingflow.FollowUp({"id": 1}), lendingflow.FollowUp({"id": 2})],
                              after_delivery=lambda db, lead_id: None)
    assert emitted == [1, 2] and len(rolled_back) == 2


def test_the_live_scrub_waits_about_thirty_seconds_at_most(monkeypatch):
    from src.lending import compliance

    seen = {}
    monkeypatch.setattr(compliance, "tracerfy_scrub", lambda phones, max_polls=120: seen.update(polls=max_polls) or [])
    speed_to_lead.live_scrubber()([PHONE])
    assert seen["polls"] == 6


def test_a_bounded_tracerfy_poll_gives_up_after_its_attempts(monkeypatch):
    from src.tasks import dnc_refresh

    polls = []
    monkeypatch.setattr(dnc_refresh, "_POLL_INTERVAL", 0)
    monkeypatch.setattr(dnc_refresh.requests, "get", lambda *a, **k: polls.append(1) or SimpleNamespace(
        ok=True, json=lambda: {"pending": True}))
    with pytest.raises(RuntimeError, match="within 0 seconds"):
        dnc_refresh._poll_queue("q1", "key", max_attempts=6)
    assert len(polls) == 6


def test_the_website_upsert_sets_the_greeting_for_the_intro_text(monkeypatch):
    from src.lending import web_lead_ghl

    monkeypatch.setattr(web_lead_ghl, "get_settings", lambda: SimpleNamespace(
        lending_ghl_cf_intro_name="cf-intro", lending_ghl_cf_sms_consent=None, lending_ghl_cf_call_consent=None,
        lending_ghl_cf_deal_drop_optin=None, lending_ghl_cf_loan_amount=None, lending_ghl_cf_credit_band=None,
        lending_ghl_cf_property_address=None))
    base = {"sms_consent": False, "call_consent": False, "deal_drop_optin": False, "suppressed": False,
            "loan_amount": None, "credit_band": None, "property_address": None}
    assert {"id": "cf-intro", "value": "Mark"} in web_lead_ghl._custom_fields({**base, "name": "Mark Lee"})
    assert {"id": "cf-intro", "value": "there"} in web_lead_ghl._custom_fields({**base, "name": " "})


# ── emergency halt and the timing report ─────────────────────────────────────

class ActiveDb:
    def __init__(self, rows):
        self.rows, self.halted = rows, []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM lending.speed_to_lead" in sql and "FOR UPDATE" in sql:
            assert params["since"] == NOW - timedelta(minutes=5)
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: self.rows))
        if "SET call_status" in sql:
            self.halted.append(params["id"])
            return SimpleNamespace()
        raise AssertionError(sql[:80])


def test_the_halt_removes_both_tags_from_cascades_still_running():
    db = ActiveDb([{"id": 1, "source": "web", "lead_id": 7, "ghl_contact_id": "c-7"}])
    tags = FakeTags()
    assert speed_to_lead.stop_active_cascades(db, tags, now=NOW) == 1
    assert tags.calls == [("remove", "c-7", GHL_TAG_AI_VOICE_OK), ("remove", "c-7", GHL_TAG_SPEED_TO_LEAD)]
    assert db.halted == [1]


def test_a_halt_that_lands_while_the_cascade_starts_undoes_it(gates, monkeypatch):
    checks = iter([False, True])  # clear at the start, set by the time the tags are in
    monkeypatch.setattr(speed_to_lead, "is_globally_halted", lambda: next(checks))
    outcome, tags, db = run(web_lead())
    assert tags.calls[-2:] == [("remove", "c-7", GHL_TAG_AI_VOICE_OK), ("remove", "c-7", GHL_TAG_SPEED_TO_LEAD)]
    assert (outcome.call_status, outcome.call_block_reason) == ("halted", "EMERGENCY_STOP")
    assert db.records[0]["call_tagged_at"] is None


def test_a_cascade_the_halt_could_not_stop_stays_tagged():
    db = ActiveDb([{"id": 1, "source": "web", "lead_id": 7, "ghl_contact_id": "c-7"}])
    assert speed_to_lead.stop_active_cascades(db, FakeTags(fail=True), now=NOW) == 0 and db.halted == []


def test_the_halt_hook_does_nothing_while_speed_to_lead_is_off(gates):
    gates.speed = False
    assert speed_to_lead.stop_active_cascades_live() is None


class ReportDb:
    def __init__(self, stored=()):
        self.stored = list(stored)

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM lending.speed_to_lead s" in sql:
            row = {"source": "web", "lead_id": 7, "received_at": NOW, "text_status": "tagged",
                   "text_tagged_at": NOW + timedelta(seconds=2), "call_status": "tagged", "call_block_reason": None,
                   "call_tagged_at": NOW + timedelta(seconds=3), "voice_eligible": True, "ghl_contact_id": "c-7",
                   "phone": PHONE}
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [row]))
        return SimpleNamespace(mappings=lambda: self.stored)


def test_the_report_reads_the_lead_s_calls_live_from_ghl():
    calls = [{"dateAdded": (NOW + timedelta(seconds=21)).isoformat(), "direction": "outbound",
              "meta": {"call": {"status": "completed", "duration": 95}}},
             {"dateAdded": (NOW + timedelta(minutes=20)).isoformat(), "direction": "outbound", "status": "no-answer"}]
    lines = speed_to_lead.report_lines(ReportDb(), source="web", lead_id=7, read_calls=lambda contact: calls)
    assert "text tagged +2s" in lines[0] and "call cascade tagged +3s" in lines[0]
    assert "target 5s missed" not in lines[0] and "target 60s missed" not in lines[0]
    assert lines[1:] == ["    GHL call +21s outbound completed talk 95s"]  # the 20-minute call is outside the window
    assert PHONE not in "".join(lines)


def test_a_failed_live_read_falls_back_to_the_stored_calls():
    def broken(contact):
        raise RuntimeError("GHL down")

    stored = [{"call_started_at": NOW + timedelta(seconds=30), "direction": "outbound", "caller_name": "Josh",
               "talk_duration_sec": 60, "disposition": "CONNECTED", "disposition_raw": None}]
    lines = speed_to_lead.report_lines(ReportDb(stored), source="web", lead_id=7, read_calls=broken)
    assert lines[1] == "    GHL call +30s outbound Josh talk 60s result CONNECTED"


# ── impact: the weekly DNC refresh keeps its 120-poll default (passes on dev too) ──
def test_the_default_poll_still_tries_120_times(monkeypatch):
    from src.tasks import dnc_refresh

    polls = []
    monkeypatch.setattr(dnc_refresh, "_POLL_INTERVAL", 0)
    monkeypatch.setattr(dnc_refresh.requests, "get", lambda *a, **k: polls.append(1) or SimpleNamespace(
        ok=True, json=lambda: {"pending": True}))
    with pytest.raises(RuntimeError):
        dnc_refresh._poll_queue("q1", "key")
    assert len(polls) == 120


def test_tracerfy_scrub_called_with_phones_only_uses_the_default_poll(monkeypatch):
    from src.lending import compliance
    from src.tasks import dnc_refresh

    seen = {}
    monkeypatch.setattr(compliance, "get_settings", lambda: SimpleNamespace(
        tracerfy_api_key=SimpleNamespace(get_secret_value=lambda: "k")), raising=False)
    monkeypatch.setattr("config.settings.get_settings", lambda: SimpleNamespace(
        tracerfy_api_key=SimpleNamespace(get_secret_value=lambda: "k")))
    monkeypatch.setattr(dnc_refresh, "_submit_scrub_batch", lambda phones, key: "q1")

    def poll(queue_id, key, *args, **kwargs):
        seen["args"], seen["kwargs"] = args, kwargs
        return []

    monkeypatch.setattr(dnc_refresh, "_poll_queue", poll)
    assert compliance.tracerfy_scrub(["+18135550100"]) == []
    polls = seen["kwargs"].get("max_attempts", seen["args"][0] if seen["args"] else 120)
    assert polls == 120


# ── against Postgres (rolled back; skipped without one) ──────────────────────

def test_a_website_lead_end_to_end_on_postgres(lending_db, monkeypatch):
    from sqlalchemy import text

    from migrations.apply_lending_speed_to_lead import apply_to
    from migrations.apply_lending_web_leads import apply_to as apply_web
    from src.lending.models import LendingBase

    conn = lending_db.connection()
    LendingBase.metadata.create_all(conn, checkfirst=True)
    apply_web(conn)
    # an existing table from before P2.1: the migration adds the AI-call columns, and a re-run changes nothing
    conn.execute(text("ALTER TABLE lending.web_leads DROP COLUMN IF EXISTS ai_call_consent, "
                      "DROP COLUMN IF EXISTS ai_call_consent_text, DROP COLUMN IF EXISTS ai_call_consent_text_matches"))
    conn.execute(text("ALTER TABLE lending.speed_to_lead DROP COLUMN IF EXISTS queued_at, "
                      "DROP COLUMN IF EXISTS queue_reason, DROP COLUMN IF EXISTS released_at"))  # the first P2.1 table
    apply_to(conn)
    apply_to(conn)  # idempotent
    queue_columns = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'lending' "
        "AND table_name = 'speed_to_lead' AND column_name IN ('queued_at', 'queue_reason', 'released_at')"))}
    assert queue_columns == {"queued_at", "queue_reason", "released_at"}
    assert conn.execute(text("SELECT count(*) FROM pg_indexes WHERE schemaname = 'lending' AND indexname IN "
                             "('ix_speed_to_lead_queued', 'ix_speed_to_lead_call_tagged_at')")).scalar() == 2
    added = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'lending' AND table_name = 'web_leads' "
        "AND column_name LIKE 'ai_call_consent%'"))}
    assert added == {"ai_call_consent", "ai_call_consent_text", "ai_call_consent_text_matches"}
    monkeypatch.setattr(speed_to_lead, "get_settings", lambda: SimpleNamespace(
        lending_speed_to_lead_enabled=True, lending_intro_sms_enabled=True, lending_ghl_cf_intro_name="cf",
        lending_sms_carrier_approved=True))
    monkeypatch.setattr(speed_to_lead, "is_globally_halted", lambda: False)
    monkeypatch.setattr(speed_to_lead, "callers_hours_open", lambda now: True)
    monkeypatch.setattr("src.lending.ghl_list_scrub.list_scrub", lambda *a, **k: None)
    monkeypatch.setattr("src.lending.ghl_call_gate.blocked_reasons", lambda db, phones, **k: {})
    phone = f"+1813555{uuid.uuid4().int % 10000:04d}"
    now = datetime.now(timezone.utc)
    lead_id = lending_db.execute(text(
        "INSERT INTO lending.web_leads (received_at, name, phone, ghl_contact_id, sms_consent, call_consent, "
        "ai_call_consent, ai_call_consent_text_matches) VALUES (:r, 'Test Lead', :p, 'c-pg', true, true, true, true) "
        "RETURNING id"),
        {"r": now - timedelta(seconds=2), "p": phone}).scalar()
    lending_db.execute(text("INSERT INTO lending.text_consents (phone, source, captured_at) VALUES (:p, 'web_form', :r)"),
                       {"p": phone, "r": now})

    monkeypatch.setattr(speed_to_lead, "text_window_open", lambda *a, **k: False)  # arrives at night: text held
    tags = FakeTags()
    outcome = speed_to_lead.start(lending_db, "web", lead_id, tags=tags, scrubber=lambda p: [], now=now)
    assert (outcome.text_status, outcome.call_status, outcome.voice_eligible) == (TEXT_HELD, CALL_TAGGED, True)
    assert speed_to_lead.start(lending_db, "web", lead_id, tags=tags, scrubber=lambda p: [], now=now) is None  # once

    monkeypatch.setattr(speed_to_lead, "text_window_open", lambda *a, **k: True)  # 8am: the sweep releases it
    released = FakeTags()
    assert speed_to_lead.release_held_web_intro(lending_db, released, now=now) == 1
    assert released.calls == [("remove", "c-pg", GHL_TAG_WEB_INTRO_READY), ("add", "c-pg", GHL_TAG_WEB_INTRO_READY)]
    assert speed_to_lead.release_held_web_intro(lending_db, FakeTags(), now=now) == 0

    lines = speed_to_lead.report_lines(lending_db, source="web", lead_id=lead_id)
    assert lines and f"web:{lead_id}" in lines[0] and "call cascade tagged" in lines[0] and phone not in "".join(lines)



# ── bug fixed: the cascade start is durable before any tag is sent ───────────

class RecordingClaims:
    """A ClaimStore that records the order of its writes next to the GHL tag calls."""

    def __init__(self, order, claimed=True):
        self.order, self.claimed = order, claimed

    def claim(self, params):
        self.order.append("claim committed")
        return 9 if self.claimed else None

    def mark_starting(self, row_id, at):
        self.order.append(("starting committed", row_id, at))


class OrderedTags(FakeTags):
    def __init__(self, order):
        super().__init__()
        self.order = order

    def add_tag(self, contact_id, tag):
        self.order.append(("tag", tag))
        super().add_tag(contact_id, tag)


def test_the_claim_and_the_starting_mark_are_written_before_any_cascade_tag(gates):
    order: list = []
    outcome = speed_to_lead.start(FakeDb(web_lead()), "web", 7, tags=OrderedTags(order), scrubber=lambda p: [],
                                  now=NOW, claims=RecordingClaims(order))
    assert outcome.call_status == CALL_TAGGED
    first_cascade_tag = order.index(("tag", GHL_TAG_SPEED_TO_LEAD))
    assert order.index("claim committed") < order.index(("starting committed", 9, NOW)) < first_cascade_tag


def test_a_failed_starting_mark_sends_no_cascade_tag(gates):
    class Broken(RecordingClaims):
        def mark_starting(self, row_id, at):
            raise RuntimeError("db down")

    tags = FakeTags()
    assert speed_to_lead.start_safely(FakeDb(web_lead()), "web", 7, tags=tags, scrubber=lambda p: [],
                                      claims=Broken([])) is None
    assert not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD]


def test_a_queued_lead_is_claimed_but_never_marked_starting(gates):
    order: list = []
    gates.hours = False
    speed_to_lead.start(FakeDb(web_lead()), "web", 7, tags=OrderedTags(order), scrubber=lambda p: [], now=NOW,
                        claims=RecordingClaims(order))
    assert order[0] == "claim committed" and not [o for o in order if isinstance(o, tuple) and o[0] != "tag"]


def test_the_live_step_claims_in_its_own_committed_session(gates, monkeypatch):
    seen = {}
    monkeypatch.setattr(speed_to_lead, "live_tag_client", lambda: FakeTags())
    monkeypatch.setattr(speed_to_lead, "start_safely", lambda db, source, lead_id, **kw: seen.update(kw))
    speed_to_lead.live_step("lendingflow")(object(), 5)
    assert isinstance(seen["claims"], speed_to_lead.OwnSessionClaims)



# ── review fixes: real start time, whole cascade inside the hours, intro re-armed ──

def test_a_returning_borrower_s_new_inquiry_gets_the_intro_text_again(gates):
    first, tags, _ = run(web_lead(id=7))
    second, _, _ = run(web_lead(id=8), tags=tags)  # same GHL contact, a new lead after the dedup window
    intro = [c for c in tags.calls if c[2] == GHL_TAG_WEB_INTRO_READY]
    assert intro == [("remove", "c-7", GHL_TAG_WEB_INTRO_READY), ("add", "c-7", GHL_TAG_WEB_INTRO_READY)] * 2
    assert first.text_status == second.text_status == TEXT_TAGGED


def test_no_voice_ai_when_its_call_would_fall_after_the_cutoff(gates):
    gates.hours = lambda at: at < NOW + timedelta(seconds=90)  # cutoff 90 s after the start
    outcome, tags, _ = run(web_lead())
    assert outcome.call_status == CALL_TAGGED and outcome.voice_eligible is False  # Josh and David still fit
    assert ("remove", "c-7", GHL_TAG_AI_VOICE_OK) in tags.calls and ("add", "c-7", GHL_TAG_AI_VOICE_OK) not in tags.calls


def test_no_cascade_when_david_s_call_would_fall_after_the_cutoff(gates):
    gates.hours = lambda at: at < NOW + timedelta(seconds=30)
    outcome, tags, _ = run(web_lead())
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "outside_hours")
    assert not [c for c in tags.calls if c[2] == GHL_TAG_SPEED_TO_LEAD]


def test_the_lead_s_own_window_is_checked_at_the_real_start_time(gates, monkeypatch):
    def window(db, phones, *, now, halted):  # the lead's 8pm closes 20 s after the arrival check
        return {phones[0]: "OUTSIDE_CALL_WINDOW"} if now >= NOW + timedelta(seconds=20) else {}

    monkeypatch.setattr("src.lending.ghl_call_gate.blocked_reasons", window)
    times = iter([NOW, NOW + timedelta(seconds=30)])  # the DNC scrub took 30 s
    outcome = speed_to_lead.start(FakeDb(web_lead()), "web", 7, tags=FakeTags(), scrubber=lambda p: [],
                                  clock=lambda: next(times))
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "OUTSIDE_CALL_WINDOW")


def test_a_scrub_that_crosses_the_fresh_limit_queues_the_lead(gates):
    lead = web_lead(received_at=NOW - timedelta(minutes=4, seconds=50))
    times = iter([NOW, NOW + timedelta(seconds=30)])  # 4:50 at the check, 5:20 after the scrub
    outcome = speed_to_lead.start(FakeDb(lead), "web", 7, tags=FakeTags(), scrubber=lambda p: [],
                                  clock=lambda: next(times))
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "delayed")


def test_a_scrub_that_crosses_the_cutoff_queues_the_lead(gates):
    gates.hours = lambda at: at < NOW + timedelta(seconds=10)  # open at the check, closed after the scrub
    times = iter([NOW, NOW + timedelta(seconds=30)])
    outcome = speed_to_lead.start(FakeDb(web_lead()), "web", 7, tags=FakeTags(), scrubber=lambda p: [],
                                  clock=lambda: next(times))
    assert (outcome.call_status, outcome.queue_reason) == (CALL_QUEUED, "outside_hours")
