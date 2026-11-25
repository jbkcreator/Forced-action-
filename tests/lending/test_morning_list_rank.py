"""R1 1.3: the morning list's ranking, padding, owner split and line text (pure; no database)."""
from __future__ import annotations

from config.lending_morning_list import (
    ACTIVE_DEAL, BOOKED_TODAY, CALLBACK, CLOSING_BLOCKER, COLD_TOP, DAVID, JOSH, NEW_WARM_LEAD,
)
from src.lending.morning_list.items import ListItem, build, render_sections


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


def test_lines_show_label_name_last4_and_brief_never_full_phone_or_email() -> None:
    items = build([item("lf:1", NEW_WARM_LEAD, phone="+18135551234", email="ann@example.com",
                        brief="Fix and flip, $250k, Tampa")], size=50)
    josh, david = render_sections(items, david_user_id="U_DAVID")
    line = david.lines[-1]
    assert line == "• *New lead*: Ann Lee (…1234). Fix and flip, $250k, Tampa"
    assert "8135551234" not in "".join(david.lines) and "example.com" not in "".join(david.lines)
    assert david.lines[0] == "<@U_DAVID>"
    assert josh.lines == []


def test_a_listed_deal_without_a_mobile_says_so_and_names_are_escaped() -> None:
    items = build([item("loan:1", ACTIVE_DEAL, line_type="Landline", name="<!channel>", brief="Closing, 3 days")],
                  size=50)
    josh, _ = render_sections(items, david_user_id="")
    assert josh.lines == ["• *Active deal*: &lt;!channel&gt; (…0100, no mobile). Closing, 3 days"]


def test_email_found_by_clay_is_flagged_not_shown() -> None:
    items = build([item("dlr:1", COLD_TOP, email="x@y.com", email_source="clay")], size=50)
    _, david = render_sections(items, david_user_id="")
    assert david.lines[-1].endswith("(…0100). email found")
