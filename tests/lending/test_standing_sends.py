"""P1.4 Standing Sends Tracker: card edits stored as before/after, counts per template version, graduation after 25
approvals with under 10% edited, and the morning card section."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from src.lending import standing_sends
from src.lending.standing_sends import TemplateStats, field_diff, morning_card_lines, record_card_edit, tracker_line


# ── card edits ───────────────────────────────────────────────────────────────

class CaptureEngine:
    def __init__(self, returning=1, fail=False):
        self.params, self.returning, self.fail = [], returning, fail

    @contextmanager
    def begin(self):
        yield self

    @contextmanager
    def connect(self):
        yield self

    def execute(self, statement, params=None):
        if self.fail:
            raise RuntimeError("db down: Hi Mark")
        self.params.append(params)
        return SimpleNamespace(scalar=lambda: self.returning)


def action(revisions=1, **payload):
    base = {"to_phone": "+18135550100", "to_email": "mark@example.com", "body": "Hi Mark, quick call this week?",
            "template_version": "LF-D2-SMS-v1", "job_id": "j1"}
    base.update(payload)
    history = [{"payload": {**base, "body": f"Hi Mark, version {i}"}, "summary": "", "note": f"make it shorter {i}",
                "revised_by": "U_JOSH", "revised_at": "2026-11-12T10:00:00+00:00"} for i in range(revisions)]
    return SimpleNamespace(action_id=42, tool_name="agent_job", channel="ghl_sms", payload=base,
                           revisions=tuple(history))


def test_an_edit_is_stored_as_before_and_after_with_a_diff():
    engine = CaptureEngine()
    assert record_card_edit(engine, action()) == 1
    row = engine.params[0]
    assert json.loads(row["before"]) == {"body": "Hi Mark, version 0"}
    assert json.loads(row["after"]) == {"body": "Hi Mark, quick call this week?"}
    assert "-Hi Mark, version 0" in row["diff"] and "+Hi Mark, quick call this week?" in row["diff"]
    assert row["revision_no"] == 1 and row["template_version"] == "LF-D2-SMS-v1"
    assert row["instruction"] == "make it shorter 0" and row["edited_by"] == "U_JOSH"


def test_recipient_details_are_never_copied_into_the_edit():
    engine = CaptureEngine()
    record_card_edit(engine, action())
    stored = engine.params[0]["before"] + engine.params[0]["after"] + engine.params[0]["diff"]
    assert "+18135550100" not in stored and "mark@example.com" not in stored


def test_every_revise_is_its_own_row_and_each_after_is_the_next_before():
    engine = CaptureEngine()
    assert record_card_edit(engine, action(revisions=2)) == 2
    first, second = engine.params
    assert (first["revision_no"], second["revision_no"]) == (1, 2)
    assert json.loads(first["after"]) == {"body": "Hi Mark, version 1"} == json.loads(second["before"])
    assert json.loads(second["after"]) == {"body": "Hi Mark, quick call this week?"}


def test_an_edit_already_stored_is_not_counted_again():
    assert record_card_edit(CaptureEngine(returning=None), action(revisions=2)) == 0


def test_the_sweep_backfills_an_edit_the_revise_handler_missed():
    base = action(revisions=1)

    class Engine(CaptureEngine):
        def execute(self, statement, params=None):
            if "NOT EXISTS (SELECT 1 FROM lending.card_edits" in str(statement):
                row = {"action_id": 42, "tool_name": "agent_job", "channel": "ghl_sms",
                       "payload": json.dumps(base.payload), "revisions": json.dumps(list(base.revisions))}
                return SimpleNamespace(mappings=lambda: [row])
            return super().execute(statement, params)

    engine = Engine()
    assert standing_sends.backfill_card_edits(engine) == 1
    assert engine.params[-1]["action_id"] == 42 and engine.params[-1]["revision_no"] == 1


def test_a_card_with_no_template_version_is_still_recorded():
    engine = CaptureEngine()
    record_card_edit(engine, action(template_version=""))
    assert engine.params[0]["template_version"] is None


def test_an_action_without_revisions_records_nothing():
    engine = CaptureEngine()
    assert record_card_edit(engine, action(revisions=0)) == 0 and engine.params == []
    assert record_card_edit(engine, None) == 0


def test_an_email_diff_covers_subject_and_body():
    diff = field_diff({"subject": "Re: funding", "body": "Hello"}, {"subject": "Re: your deal", "body": "Hello"})
    assert "subject (before)" in diff and "+Re: your deal" in diff and "body" not in diff


def test_a_failed_edit_write_never_breaks_the_revise(caplog):
    record = standing_sends.make_edit_recorder(CaptureEngine(fail=True), lambda action_id: action())
    with caplog.at_level("ERROR"):
        record(42)
    assert "not recorded (RuntimeError)" in caplog.text and "Hi Mark" not in caplog.text


def test_coras_revise_handler_records_the_edit_only_after_a_successful_revise():
    from packages.agent_core.revision import RevisionFailed
    from src.lending.cora_agent import CoraConversation

    recorded, posted = [], []
    chat = SimpleNamespace(post=lambda **kwargs: posted.append(kwargs))
    message = SimpleNamespace(text="shorter", user_id="U_JOSH", channel="C1", reply_thread_ts="1.0")
    ok = SimpleNamespace(revise=lambda *a: SimpleNamespace(card_posted=True))
    CoraConversation(config=None, chat=chat, loop=None, reviser=ok, bot_user_id="B",
                     on_edit=recorded.append).on_revision(SimpleNamespace(action_id=7), message)
    assert recorded == [7]

    def refuse(*a):
        raise RevisionFailed("no")

    CoraConversation(config=None, chat=chat, loop=None, reviser=SimpleNamespace(revise=refuse), bot_user_id="B",
                     on_edit=recorded.append).on_revision(SimpleNamespace(action_id=8), message)
    assert recorded == [7] and len(posted) == 2


# ── graduation rule ──────────────────────────────────────────────────────────

def stats(approved, edited, rejected=0, auto_sent=0, **kw):
    return TemplateStats("LF-D2-SMS-v1", approved, edited, rejected, auto_sent, **kw)


@pytest.mark.parametrize("approved, edited, rejected, ready", [
    (24, 0, 0, False),   # not 25 approvals yet
    (25, 2, 0, True),    # 8% edited
    (25, 3, 0, False),   # 12% edited
    (30, 3, 0, False),   # exactly 10%: "under 10%" means strictly under
    (31, 3, 0, True),    # 9.7%
    (25, 1, 20, True),   # rejections neither help nor block (client decision, literal brief)
    (10, 0, 30, False),  # rejections never count as approvals
])
def test_graduation_needs_25_approvals_with_under_10_percent_edited(approved, edited, rejected, ready):
    assert stats(approved, edited, rejected).meets_rule is ready


@pytest.mark.parametrize("template", ["CORA-SMS-v1", "CORA-EMAIL-v1", "INSTANTLY-REPLY-v1"])
def test_coras_own_draft_types_are_counted_but_never_graduate(template):
    row = TemplateStats(template, 40, 0, 0, 0)
    assert row.meets_rule is False and row.state == "approval_only"
    assert tracker_line(row) == f"• {template}: drafted by Cora, always needs approval · 40 approved · 0% edited"


def test_coras_sms_and_email_drafts_carry_a_template_version():
    from src.lending.cora_tools import CoraToolkit

    toolkit = CoraToolkit.__new__(CoraToolkit)
    lead = SimpleNamespace(ref="web_lead:7", phone="+17275550100", email="sam@example.com", first_name="Sam",
                           name="Sam", ghl_contact_id=None)
    toolkit._lead = lambda ref: lead
    sms = toolkit.draft_sms(SimpleNamespace(lead_ref="web_lead:7", body="Hi Sam"), None)
    email = toolkit.draft_email(SimpleNamespace(lead_ref="web_lead:7", subject="Hello", body="Hi Sam"), None)
    assert sms.payload["template_version"] == "CORA-SMS-v1" and email.payload["template_version"] == "CORA-EMAIL-v1"


def test_states_on_the_tracker():
    assert stats(5, 0).state == "collecting" and stats(25, 0).state == "ready"
    assert stats(25, 0, graduated=True).state == "graduated"
    assert stats(25, 0, graduated=True, revoked=True).state == "revoked"


def test_tracker_lines_show_counts_and_readiness():
    assert tracker_line(stats(18, 1, 2)) == "• LF-D2-SMS-v1: 18/25 approvals · 6% edited · 2 rejected"
    assert "ready, graduates on the next check" in tracker_line(stats(25, 1))
    assert "sending on its own (4 approved automatically)" in tracker_line(stats(25, 1, auto_sent=4, graduated=True))
    assert "graduation revoked" in tracker_line(stats(25, 1, graduated=True, revoked=True))


def test_the_morning_card_shows_at_most_ten_types():
    lines = morning_card_lines([TemplateStats(f"T-{i}", i, 0, 0, 0) for i in range(12)])
    assert len(lines) == 11 and lines[-1] == "…and 2 more message types"


def test_the_graduation_check_fails_closed():
    assert standing_sends.make_is_graduated(CaptureEngine(fail=True))("LF-D2-SMS-v1") is False
    assert standing_sends.make_is_graduated(CaptureEngine(returning=True))("") is False
    assert standing_sends.make_is_graduated(CaptureEngine(returning=True))("LF-D2-SMS-v1") is True


def test_the_morning_card_has_the_standing_sends_section():
    from src.lending.cora_morning_digest import DigestData, render

    data = DigestData(halted=False, halt_reason="", pending_drafts=[], pending_total=0, due_confirmations=[],
                      due_confirmations_total=0, open_inbox=0, new_leads=[], new_leads_total=0, web_leads=0,
                      web_leads_not_in_ghl=0, todays_calls=[], dialer_queues=[], standing_sends=[stats(18, 1)])
    _, blocks = render(date(2026, 11, 12), data, now=datetime(2026, 11, 12, 12, 30, tzinfo=timezone.utc))
    rendered = json.dumps(blocks)
    assert "Standing sends" in rendered and "18/25 approvals" in rendered


# ── against Postgres (shared database; skipped without one) ──────────────────

def _approve(db, template, *, status="sent", decided_by="U_JOSH", revised=False):
    from sqlalchemy import text

    db.execute(text("""
        INSERT INTO lending.pending_actions (tool_name, channel, payload, status, decided_by, revisions)
        VALUES ('agent_job', 'ghl_sms', CAST(:payload AS jsonb), :status, :decided_by, CAST(:revisions AS jsonb))
    """), {"payload": json.dumps({"template_version": template, "body": "Hi"}), "status": status,
           "decided_by": decided_by, "revisions": json.dumps([{"payload": {"body": "x"}}] if revised else [])})


def test_counts_and_graduation_on_postgres(lending_db):
    from migrations.apply_lending_cora_agent_core import apply_to as apply_core
    from migrations.apply_lending_cora_inbox import apply_to as apply_inbox
    from migrations.apply_lending_pending_action_safeguards import apply_to as apply_safeguards
    from migrations.apply_lending_standing_sends import apply_to

    conn = lending_db.connection()
    apply_core(conn)
    apply_safeguards(conn)
    apply_inbox(conn)
    apply_to(conn)
    apply_to(conn)  # idempotent
    for i in range(25):
        _approve(lending_db, "P14-TEST-v1", revised=i < 2)
    _approve(lending_db, "P14-TEST-v1", status="rejected")
    _approve(lending_db, "P14-TEST-v1", decided_by="auto:graduated")
    _approve(lending_db, "P14-OTHER-v1")
    rows = {s.template_version: s for s in standing_sends.template_stats(conn) if s.template_version.startswith("P14-")}
    assert (rows["P14-TEST-v1"].approved, rows["P14-TEST-v1"].edited, rows["P14-TEST-v1"].rejected,
            rows["P14-TEST-v1"].auto_sent) == (25, 2, 1, 1)
    assert rows["P14-OTHER-v1"].approved == 1 and rows["P14-OTHER-v1"].state == "collecting"

    engine = SimpleNamespace(begin=lambda: _same(conn), connect=lambda: _same(conn))
    from sqlalchemy import text as sql_text

    since = conn.execute(sql_text("SELECT now() - interval '1 minute'")).scalar()
    assert "P14-TEST-v1" in [t.template_version for t in standing_sends.template_stats(conn, ["P14-TEST-v1"])]
    assert standing_sends.template_stats(conn, ["P14-NONE-v1"]) == []
    # the two revised drafts have no card_edits rows yet: the incremental backfill records them, once
    assert standing_sends.backfill_card_edits(engine, since=since) >= 2
    assert standing_sends.backfill_card_edits(engine, since=since) == 0
    graduated = [s.template_version for s in standing_sends.evaluate_graduations(engine)]
    assert "P14-TEST-v1" in graduated and "P14-OTHER-v1" not in graduated
    assert "P14-TEST-v1" not in [s.template_version for s in standing_sends.evaluate_graduations(engine)]  # once
    assert standing_sends.make_is_graduated(engine)("P14-TEST-v1") is True
    assert standing_sends.set_revoked(engine, "P14-TEST-v1", revoked=True, by="test") is True
    assert standing_sends.make_is_graduated(engine)("P14-TEST-v1") is False
    assert "P14-TEST-v1" not in [s.template_version for s in standing_sends.evaluate_graduations(engine)]


@contextmanager
def _same(conn):
    yield conn


# ── performance: the sweep only looks at what changed ────────────────────────

def test_the_sweep_step_runs_full_first_then_incremental_then_full_once_a_day(monkeypatch):
    from datetime import timedelta

    calls = []
    monkeypatch.setattr(standing_sends, "backfill_card_edits", lambda engine, since=None: calls.append(("edits", since)))
    monkeypatch.setattr(standing_sends, "evaluate_graduations",
                        lambda engine, since=None: calls.append(("graduate", since)) or [])
    now = [datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)]
    step = standing_sends.StandingSendsStep(engine=None, clock=lambda: now[0])

    step()  # first run after a start: full
    assert calls == [("edits", None), ("graduate", None)]
    now[0] += timedelta(minutes=1)
    calls.clear()
    step()  # next minute: only what changed since the last run, with the overlap
    expected_since = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc) - standing_sends.INCREMENTAL_OVERLAP
    assert calls == [("edits", expected_since), ("graduate", expected_since)]
    now[0] += standing_sends.FULL_PASS_EVERY
    calls.clear()
    step()  # a day later: full again
    assert calls == [("edits", None), ("graduate", None)]


def test_an_incremental_check_with_no_new_decisions_counts_nothing():
    class Engine(CaptureEngine):
        def execute(self, statement, params=None):
            self.params.append((str(statement), params))
            return iter([])  # no template decided since

    engine = Engine()
    assert standing_sends.evaluate_graduations(engine, since=datetime(2026, 11, 12, tzinfo=timezone.utc)) == []
    assert len(engine.params) == 1 and "decided_at >= :since" in engine.params[0][0]


def test_the_migration_adds_the_lookup_indexes_only_when_missing():
    from migrations import apply_lending_standing_sends as migration

    names = [name for name, _ in migration._PENDING_ACTION_INDEXES]
    assert names == ["ix_pending_actions_decided_at", "ix_pending_actions_updated_at",
                     "ix_pending_actions_template_version"]
