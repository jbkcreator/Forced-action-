"""R1 1.3 D8/D16/D18: who gets a morning draft, what it says, and that every draft goes to the approval gate (fake
writer and fake gate; no LLM, no Slack, no database)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from config.lending_morning_list import ACTIVE_DEAL, CALLBACK, COLD_TOP, NEW_WARM_LEAD
from src.lending.morning_list.drafts import DraftContext, morning_drafts
from src.lending.morning_list.items import ListItem

NOW = datetime(2026, 11, 25, 12, 47, tzinfo=timezone.utc)


def lead(ref: str = "lf:1", *, reason: str = NEW_WARM_LEAD, age: timedelta = timedelta(days=2), phone="+18135551111",
         email: str | None = "ann@x.com") -> ListItem:
    return ListItem(ref=ref, reason=reason, name="Ann Lee", phone=phone, line_type="Mobile", email=email,
                    brief="Fix and flip, Tampa", received_at=NOW - age)


def ctx(**kw) -> DraftContext:
    base = dict(now=NOW, text_consent=lambda phone: True, recently_contacted=set(), open_drafts=set())
    return DraftContext(**{**base, **kw})


def writer(text: str):
    return lambda request: text


def test_a_consented_warm_lead_older_than_a_day_gets_one_sms_in_joshs_words() -> None:
    drafts = morning_drafts([lead()], writer("Hi Ann, Josh here. Got a minute today for your Tampa flip?"), ctx())
    assert len(drafts) == 1
    d = drafts[0]
    assert d.key == "morning:2026-11-25:lf:1"
    assert d.draft.channel == "ghl_sms"
    assert d.draft.payload["to_phone"] == "+18135551111"
    assert d.draft.payload["body"].startswith("Hi Ann, Josh here.")
    assert d.draft.contact_ref == "lendingflow_lead:1"


def test_speed_to_lead_owns_the_first_day() -> None:
    assert morning_drafts([lead(age=timedelta(hours=20))], writer("Hi"), ctx()) == []


def test_no_draft_when_we_contacted_them_recently_or_a_draft_is_open() -> None:
    assert morning_drafts([lead()], writer("Hi"), ctx(recently_contacted={"+18135551111"})) == []
    assert morning_drafts([lead()], writer("Hi"), ctx(open_drafts={"+18135551111"})) == []


def test_no_text_consent_means_email_and_no_email_means_nothing() -> None:
    no_text = ctx(text_consent=lambda phone: False)
    drafts = morning_drafts([lead()], writer("Hi Ann"), no_text)
    assert drafts[0].draft.channel == "ghl_email"
    assert drafts[0].draft.payload["to_email"] == "ann@x.com" and drafts[0].draft.payload["subject"]
    assert morning_drafts([lead(email=None)], writer("Hi Ann"), no_text) == []


def test_only_warm_leads_and_callbacks_get_drafts() -> None:
    items = [lead("loan:1", reason=ACTIVE_DEAL, phone="+18135550001"),
             lead("dlr:1", reason=COLD_TOP, phone="+18135550002"),
             lead("cb:1", reason=CALLBACK, phone="+18135550003", age=timedelta(0))]
    assert [d.item.ref for d in morning_drafts(items, writer("Hi Ann"), ctx())] == ["cb:1"]


def test_a_draft_that_quotes_numbers_falls_back_to_the_template() -> None:
    drafts = morning_drafts([lead()], writer("Hi Ann, rates start at 9.5% with 2 points"), ctx())
    body = drafts[0].draft.payload["body"]
    assert "9.5%" not in body and "points" not in body
    assert body.startswith("Hi Ann, this is Josh with Next Deal Lending.")
    assert drafts[0].draft.payload["template_version"] == "morning_list_v1"


def test_a_failed_writer_falls_back_to_the_template() -> None:
    def broken(request):
        raise RuntimeError("model down")

    drafts = morning_drafts([lead()], broken, ctx())
    assert drafts[0].draft.payload["body"].startswith("Hi Ann, this is Josh")


def test_the_writer_gets_first_name_and_brief_but_never_contact_details() -> None:
    seen = []

    def spy(request):
        seen.append(request)
        return "Hi Ann"

    morning_drafts([lead()], spy, ctx())
    assert seen[0].first_name == "Ann" and seen[0].brief == "Fix and flip, Tampa" and seen[0].channel == "sms"
    assert "+1813" not in repr(seen[0]) and "ann@x.com" not in repr(seen[0])


@dataclass
class FakeGate:
    calls: list = field(default_factory=list)

    def submit(self, draft, *, tool_name, context, ttl):
        self.calls.append((draft, tool_name, context.tool_use_id))
        return type("Action", (), {"action_id": len(self.calls)})()


def test_submit_sends_each_draft_to_the_gate_with_its_key() -> None:
    from src.lending.morning_list.drafts import submit_all

    gate = FakeGate()
    drafts = morning_drafts([lead()], writer("Hi Ann"), ctx())
    assert submit_all(gate, drafts, approvals_channel="C_APPROVALS") == 1
    assert gate.calls[0][1] == "morning_list" and gate.calls[0][2] == "morning:2026-11-25:lf:1"


def test_a_draft_quoting_a_loan_term_falls_back_to_the_template() -> None:
    drafts = morning_drafts([lead()], writer("Hi Ann, let's talk about the 12-month term"), ctx())
    assert drafts[0].draft.payload["body"].startswith("Hi Ann, this is Josh with Next Deal Lending.")


def _queue_engine():
    from sqlalchemy import create_engine, event, text
    from sqlalchemy.pool import StaticPool

    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE lending.pending_actions (action_id INTEGER PRIMARY KEY, channel TEXT,"
                          " status TEXT, recipient_phone TEXT, executed_at TIMESTAMP, idempotency_key TEXT,"
                          " payload TEXT, revised_by TEXT)"))
    return eng


def test_open_and_recent_contact_follow_the_real_queue_statuses() -> None:
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    from src.lending.morning_list.drafts import touched_phones

    eng = _queue_engine()
    recent = NOW - timedelta(days=1)
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO lending.pending_actions (channel, status, recipient_phone, executed_at) VALUES"
                          " ('ghl_sms', 'revising', '+1001', NULL),"
                          " ('ghl_tag', 'sent', '+1002', :t),"
                          " ('ghl_sms', 'blocked', '+1003', :t),"
                          " ('ghl_sms', 'sent', '+1004', :t)"), {"t": recent})
    with Session(eng) as db:
        contacted, open_drafts = touched_phones(db, ["+1001", "+1002", "+1003", "+1004"], NOW)
    assert open_drafts == {"+1001"}
    assert contacted == {"+1001", "+1004"}


def test_drafts_step_waits_for_the_posted_digest_and_never_drafts_twice() -> None:
    from types import SimpleNamespace

    from sqlalchemy import text

    from src.lending.morning_list.step import make_morning_drafts

    eng = _queue_engine()
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE lending.cora_morning_digests (digest_date DATE PRIMARY KEY,"
                          " posted_at TIMESTAMP)"))
        for ddl in ("CREATE TABLE lending.text_consents (phone TEXT, revoked_at TIMESTAMP)",
                    "CREATE TABLE lending.lendingflow_leads (id INTEGER PRIMARY KEY, phone TEXT)",
                    "CREATE TABLE lending.suppression_list (phone TEXT)",
                    "CREATE TABLE lending.contacts (phone TEXT, do_not_contact BOOLEAN)"):
            conn.execute(text(ddl))
        conn.execute(text("INSERT INTO lending.text_consents VALUES ('+18135551111', NULL)"))

    class Gate:
        def __init__(self):
            self.keys = []

        def submit(self, draft, *, tool_name, context, ttl):
            self.keys.append(context.tool_use_id)
            with eng.begin() as conn:
                conn.execute(text("INSERT INTO lending.pending_actions (channel, status, recipient_phone,"
                                  " idempotency_key) VALUES (:c, 'rejected', :p, :k)"),
                             {"c": draft.channel, "p": draft.recipient_phone, "k": context.tool_use_id})

    settings = SimpleNamespace(lending_morning_list_enabled=True, lending_morning_drafts_enabled=True,
                               lending_morning_digest_time="07:47", lending_morning_list_size=50,
                               lending_morning_list_lead_days=14, lending_david_slack_user_id="",
                               lending_ghl_app_url="", lending_ghl_location_id="")
    gate, clock = Gate(), [NOW]
    step = make_morning_drafts(eng, settings, gate=gate, writer=writer("Hi Ann"), clock=lambda: clock[0])
    import src.lending.morning_list.step as step_module

    original = step_module.read_candidates
    step_module.read_candidates = lambda *a, **k: ([lead()], [])
    try:
        assert step() is None  # digest not posted yet
        with eng.begin() as conn:
            conn.execute(text("INSERT INTO lending.cora_morning_digests VALUES (:d, :t)"),
                         {"d": NOW.date(), "t": NOW})
        clock[0] = NOW + timedelta(minutes=15)
        assert step() == 1
        clock[0] = NOW + timedelta(minutes=30)
        assert step() == 0  # already drafted today (even though Josh rejected it)
    finally:
        step_module.read_candidates = original
    assert gate.keys == ["morning:2026-11-25:lf:1"]
