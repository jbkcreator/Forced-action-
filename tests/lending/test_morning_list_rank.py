"""R1 1.3: the morning list's ranking, padding, owner split and line text (pure; no database)."""
from __future__ import annotations

from config.lending_morning_list import (
    ACTIVE_DEAL, BOOKED_TODAY, CALLBACK, CLOSING_BLOCKER, COLD_TOP, DAVID, JOSH, NEW_WARM_LEAD,
)
from src.lending.morning_list.items import ListItem, build, render_main, render_thread


def item(ref: str, reason: str, *, phone: str = "+18135550100", line_type: str | None = "Mobile",
         sort_key: tuple = (), **kw) -> ListItem:
    return ListItem(ref=ref, reason=reason, name=kw.pop("name", "Ann Lee"), phone=phone, line_type=line_type,
                    sort_key=sort_key, **kw)


def test_tiers_rank_blockers_then_deals_then_calls_then_warm_then_padding() -> None:
    items = build([
        item("dlr:1", COLD_TOP, phone="+18135550001"),
        item("lf:1", NEW_WARM_LEAD, phone="+18135550002"),
        item("cb:1", CALLBACK, phone="+18135550003"),
        item("loan:1", ACTIVE_DEAL, phone="+18135550004"),
        item("opp:1", CLOSING_BLOCKER, phone="+18135550005"),
    ], size=50)
    assert [i.reason for i in items] == [CLOSING_BLOCKER, ACTIVE_DEAL, CALLBACK, NEW_WARM_LEAD, COLD_TOP]


def test_within_a_tier_the_reader_sort_key_decides() -> None:
    items = build([item("lf:old", NEW_WARM_LEAD, phone="+18135550001", sort_key=(2,)),
                   item("lf:best", NEW_WARM_LEAD, phone="+18135550002", sort_key=(1,))], size=50)
    assert [i.ref for i in items] == ["lf:best", "lf:old"]


def test_padding_only_fills_what_real_work_leaves_up_to_the_size() -> None:
    real = [item(f"lf:{n}", NEW_WARM_LEAD, phone=f"+1813555{n:04d}") for n in range(3)]
    cold = [item(f"dlr:{n}", COLD_TOP, phone=f"+1727555{n:04d}", sort_key=(n,)) for n in range(10)]
    items = build(real + cold, size=5)
    assert [i.ref for i in items] == ["lf:0", "lf:1", "lf:2", "dlr:0", "dlr:1"]


def test_warm_leads_and_padding_need_a_mobile_but_deals_are_kept_and_flagged() -> None:
    items = build([item("lf:1", NEW_WARM_LEAD, line_type="Landline", phone="+18135550001"),
                   item("dlr:1", COLD_TOP, line_type=None, phone="+18135550002"),
                   item("loan:1", ACTIVE_DEAL, line_type="Landline", phone="+18135550003")], size=50)
    assert [(i.ref, i.has_mobile) for i in items] == [("loan:1", False)]


def test_one_person_appears_once_at_their_highest_tier() -> None:
    items = build([item("dlr:1", COLD_TOP), item("lf:1", NEW_WARM_LEAD), item("loan:1", ACTIVE_DEAL)], size=50)
    assert [i.ref for i in items] == ["loan:1"]


def test_owner_split_josh_gets_deals_and_booked_david_gets_the_rest() -> None:
    items = build([
        item("opp:1", CLOSING_BLOCKER, phone="+18135550001"), item("loan:1", ACTIVE_DEAL, phone="+18135550002"),
        item("bk:1", BOOKED_TODAY, phone="+18135550003"), item("cb:1", CALLBACK, phone="+18135550004"),
        item("cb:2", CALLBACK, phone="+18135550005", caller_name="Josh Kantor"),
        item("lf:1", NEW_WARM_LEAD, phone="+18135550006"), item("dlr:1", COLD_TOP, phone="+18135550007"),
    ], size=50)
    assert {i.ref: i.owner for i in items} == {"opp:1": JOSH, "loan:1": JOSH, "bk:1": JOSH, "cb:1": DAVID,
                                               "cb:2": JOSH, "lf:1": DAVID, "dlr:1": DAVID}


def _thread_text(items) -> str:
    return "\n".join(b["text"]["text"] for message in render_thread(items) for b in message)


def test_davids_lines_go_to_the_thread_with_first_name_last4_brief_and_no_contact_details() -> None:
    items = build([item("lf:1", NEW_WARM_LEAD, phone="+18135551234", email="ann@example.com",
                        brief="Fix and flip, $250k, Tampa")], size=50)
    text = _thread_text(items)
    assert ":star: *Ann* (…1234)  Fix and flip, $250k, Tampa" in text and "*New leads · 1*" in text
    assert "Lee" not in text and "8135551234" not in text and "example.com" not in text
    josh, david = render_main(items, david_user_id="U_DAVID")
    assert josh.lines == [] and david.title == "David · 1"
    assert david.lines[0] == "<@U_DAVID>" and "New leads: 1" in david.lines[1]


def test_a_listed_deal_without_a_mobile_says_so_and_names_are_escaped() -> None:
    items = build([item("loan:1", ACTIVE_DEAL, line_type="Landline", name="<!channel>", brief="Closing, 3 days")],
                  size=50)
    josh, _ = render_main(items, david_user_id="")
    assert josh.lines == [":page_facing_up: *&lt;!channel&gt;* (…0100, no mobile)  Closing, 3 days"]


def test_email_found_by_clay_is_flagged_not_shown() -> None:
    items = build([item("dlr:1", COLD_TOP, email="x@y.com", email_source="clay", group="Partners")], size=50)
    text = _thread_text(items)
    assert "*Top of queue · Partners · 1*" in text and "(…0100)  email found" in text and "x@y.com" not in text


def test_each_line_links_to_ghl_and_batchdialer_when_we_hold_the_ids() -> None:
    items = build([item("dlr:1", COLD_TOP, group="Partners",
                        ghl_url="https://app.gohighlevel.com/v2/location/L1/contacts/detail/C1",
                        dialer_url="https://app.batchdialer.com/contacts/view/77"),
                   item("dlr:2", COLD_TOP, phone="+18135550002", group="Partners")], size=50)
    text = _thread_text(items)
    assert ("<https://app.gohighlevel.com/v2/location/L1/contacts/detail/C1|GHL ↗> · "
            "<https://app.batchdialer.com/contacts/view/77|BatchDialer ↗>") in text
    assert text.count("GHL ↗") == 1  # no link without an id


def test_tier_3_lists_todays_booked_calls_before_callbacks() -> None:
    items = build([item("cb:1", CALLBACK, phone="+18135550001", sort_key=(1, 100.0)),
                   item("bk:1", BOOKED_TODAY, phone="+18135550002", sort_key=(0, 900.0))], size=50)
    assert [i.ref for i in items] == ["bk:1", "cb:1"]


def test_fifty_long_lines_fit_slack_limits_and_none_is_cut() -> None:
    brief = "verified maturity, Demo Holdings Long Name LLC, Port Charlotte, $450k, " + "x" * 80
    items = build([item(f"dlr:{n}", COLD_TOP, phone=f"+1727555{n:04d}", brief=brief, sort_key=(n,), group="Partners",
                        ghl_url=f"https://app.gohighlevel.com/v2/location/L1/contacts/detail/C{n}",
                        dialer_url=f"https://app.batchdialer.com/contacts/view/{n}") for n in range(50)], size=50)
    messages = render_thread(items)
    texts = [b["text"]["text"] for message in messages for b in message]
    assert all(len(message) <= 50 for message in messages)
    assert all(len(t) <= 3000 for t in texts)
    assert sum(t.count(":small_blue_diamond:") for t in texts) == 50


def test_a_warm_lead_with_no_line_type_yet_is_kept_and_flagged() -> None:
    items = build([item("lf:1", NEW_WARM_LEAD, line_type=None, phone="+18135550001", brief="DSCR"),
                   item("lf:2", NEW_WARM_LEAD, line_type="Landline", phone="+18135550002"),
                   item("dlr:1", COLD_TOP, line_type=None, phone="+18135550003")], size=50)
    assert [i.ref for i in items] == ["lf:1"]
    assert "(…0001, mobile not checked)" in _thread_text(items)
