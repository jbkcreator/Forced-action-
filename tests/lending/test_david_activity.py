"""David's daily activity: message counting, GHL paging, flagged leads, the Slack text. GHL is faked; no database."""
from datetime import date, datetime, timedelta, timezone

from src.lending import david_activity as da

DAVID, OTHER = "u-david", "u-josh"
DAY = date(2026, 10, 28)
START, END = da.day_bounds(DAY)
NOON = START + timedelta(hours=12)


def _msg(kind, *, user=DAVID, direction="outbound", at=NOON, status=None, duration=None, mid="m"):
    meta = {"call": {"status": status, "duration": duration}} if kind == da.CALL else {}
    return {"id": mid, "messageType": kind, "direction": direction, "dateAdded": at.isoformat(),
            "meta": meta, **({"userId": user} if user else {})}


class _Resp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def test_counts_only_davids_messages_of_the_day():
    a = da.DavidActivity(DAY)
    da.count_messages(a, [
        _msg(da.CALL, status="completed", duration=180),
        _msg(da.CALL, status="voicemail", duration=20),
        _msg(da.CALL, status="no-answer"),
        _msg(da.CALL, direction="inbound", status="completed", duration=60),
        _msg(da.SMS), _msg(da.SMS, direction="inbound"),
        _msg(da.EMAIL),
        _msg(da.SMS, user=OTHER),                 # Josh's text
        {**_msg(da.SMS), "status": "failed"},     # not sent (live case: number not A2P registered)
        {**_msg(da.EMAIL), "status": "undelivered"},
        _msg(da.EMAIL, user=None),                # workflow email: no userId
        _msg(da.CALL, at=START - timedelta(minutes=1), status="completed"),  # yesterday
        _msg(da.CALL, at=END, status="completed"),                           # tomorrow
    ], DAVID, START, END)
    assert (a.dials, a.contacts_reached, a.voicemails, a.inbound_answered) == (3, 1, 1, 1)
    assert (a.texts, a.emails, a.talk_seconds) == (1, 1, 260)


def test_no_user_id_configured_counts_nothing():
    a = da.DavidActivity(DAY)
    da.count_messages(a, [_msg(da.SMS, user="")], "", START, END)
    assert a.texts == 0


def test_show_rate_is_none_without_bookings():
    assert da.DavidActivity(DAY, booked=0, held=2).show_rate is None
    assert da.DavidActivity(DAY, booked=4, held=3).show_rate == 0.75


def test_conversation_walk_stops_at_the_first_conversation_before_the_day():
    ms = lambda t: int(t.timestamp() * 1000)
    page = [{"id": "c1", "lastMessageDate": ms(NOON)}, {"id": "c2", "lastMessageDate": ms(START)},
            {"id": "c3", "lastMessageDate": ms(START - timedelta(seconds=1))}, {"id": "c4", "lastMessageDate": ms(NOON)}]
    calls = []
    get = lambda url, **kw: calls.append(kw["params"]) or _Resp({"conversations": page})
    assert list(da.conversations_since(get, "B", {}, "loc", START)) == ["c1", "c2"]
    assert len(calls) == 1 and calls[0]["sortBy"] == "last_message_date" and calls[0]["sort"] == "desc"


def test_conversation_walk_follows_pages(monkeypatch):
    monkeypatch.setattr(da, "CONVERSATION_PAGE_SIZE", 2)
    t = int(NOON.timestamp() * 1000)
    pages = [[{"id": "a", "lastMessageDate": t}, {"id": "b", "lastMessageDate": t - 1}],
             [{"id": "c", "lastMessageDate": t - 2}]]
    seen = []
    get = lambda url, **kw: seen.append(kw["params"].get("startAfterDate")) or _Resp({"conversations": pages[len(seen) - 1]})
    assert list(da.conversations_since(get, "B", {}, "loc", START)) == ["a", "b", "c"]
    assert seen == [None, t - 1]


def test_conversation_repeated_across_a_page_boundary_is_yielded_once(monkeypatch):
    monkeypatch.setattr(da, "CONVERSATION_PAGE_SIZE", 2)
    t = int(NOON.timestamp() * 1000)
    pages = [[{"id": "a", "lastMessageDate": t}, {"id": "b", "lastMessageDate": t - 1}],
             [{"id": "b", "lastMessageDate": t - 1}, {"id": "c", "lastMessageDate": t - 2}],
             [{"id": "d", "lastMessageDate": t - 3}]]
    calls = []
    get = lambda url, **kw: calls.append(1) or _Resp({"conversations": pages[len(calls) - 1]})
    assert list(da.conversations_since(get, "B", {}, "loc", START)) == ["a", "b", "c", "d"]


def test_messages_stop_at_the_first_message_before_the_day():
    page = {"messages": {"messages": [_msg(da.SMS, mid="new"), _msg(da.SMS, mid="old", at=START - timedelta(hours=1)),
                                      _msg(da.SMS, mid="older", at=START - timedelta(days=1))],
                         "nextPage": True, "lastMessageId": "older"}}
    calls = []
    got = da.messages_since(lambda url, **kw: calls.append(url) or _Resp(page), "B", {}, "c1", START)
    assert [m["id"] for m in got] == ["new"] and len(calls) == 1


def test_messages_follow_the_cursor():
    pages = [{"messages": [_msg(da.SMS, mid="1")], "nextPage": True, "lastMessageId": "1"},
             {"messages": [_msg(da.SMS, mid="2")], "nextPage": False}]
    cursors = []
    get = lambda url, **kw: cursors.append(kw["params"].get("lastMessageId")) or _Resp(pages[len(cursors) - 1])
    assert [m["id"] for m in da.messages_since(get, "B", {}, "c1", START)] == ["1", "2"]
    assert cursors == [None, "1"]


def test_flagged_leads_searches_by_tag():
    bodies = []
    post = lambda url, **kw: bodies.append(kw["json"]) or _Resp({"contacts": [
        {"id": "c1", "firstName": "Ann", "lastName": "Lee"}, {"id": "c2", "contactName": "Bob"}, {"firstName": "no id"}]})
    leads = da.flagged_leads(post, "B", {}, "loc", "flag-for-josh")
    assert leads == [da.FlaggedLead("c1", "Ann Lee"), da.FlaggedLead("c2", "Bob")]
    assert bodies[0]["filters"] == [{"field": "tags", "operator": "contains", "value": "flag-for-josh"}]


class _Db:
    def __init__(self, row=(4, 3), fail=False):
        self.row, self.fail = row, fail

    def begin_nested(self):
        class _Ctx:
            def __enter__(self):
                return None

            def __exit__(self, *exc):
                return False
        return _Ctx()

    def execute(self, *a, **k):
        if self.fail:
            raise RuntimeError("no table")
        return type("R", (), {"one": lambda s: self.row})()


def test_stage_counts_report_zero_when_the_query_fails():
    assert da.stage_counts(_Db(fail=True), DAY) == (0, 0)
    assert da.stage_counts(_Db(), DAY) == (4, 3)


def test_build_marks_a_failed_ghl_read_and_keeps_the_stage_counts():
    def get(url, **kw):
        raise ConnectionError("down")
    post = lambda url, **kw: _Resp({"contacts": []})
    a = da.build_activity(_Db(), DAY, user_id=DAVID, flagged_tag="flag-for-josh", get=get, post=post,
                          base="B", headers={}, location_id="loc")
    assert a.ghl_read_failed and (a.booked, a.held) == (4, 3)


def test_slack_text_lists_numbers_and_flagged_leads():
    a = da.DavidActivity(DAY, dials=12, contacts_reached=4, voicemails=5, texts=3, emails=1, booked=2, held=1,
                         talk_seconds=3725, flagged=[da.FlaggedLead("c1", "Ann <Lee>")])
    out = da.format_slack(a, lambda cid: f"https://ghl/{cid}")
    assert "Dials 12" in out and "Voicemails 5" in out and "Show rate 50%" in out and "Talk time 1h 2m" in out
    assert "• <https://ghl/c1|Ann Lee>" in out and ":warning:" not in out


def test_slack_text_without_flags_or_bookings():
    out = da.format_slack(da.DavidActivity(DAY, ghl_read_failed=True))
    assert "No leads flagged." in out and "Show rate –" in out and ":warning:" in out and "Talk time 0s" in out
