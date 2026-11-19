"""G5: the calling rules on Josh's GHL call lists, applied as the ndl-do-not-call-now tag."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from config.lending_compliance import GHL_CALL_LIST_TAGS, GHL_DO_NOT_CALL_NOW_TAG, ReasonCode
from src.lending import ghl_call_gate as gate
from src.lending.ghl_call_gate import (
    EMERGENCY_STOP,
    GhlListError,
    ListedContact,
    apply_changes,
    blocked_reasons,
    clear_tags,
    decide,
    read_listed_contacts,
    record_outcome,
    run_gate,
)

NOW = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)  # 10:00 ET, inside the calling window
TAG = GHL_DO_NOT_CALL_NOW_TAG
PHONE_A, PHONE_B, PHONE_C = "+18135550111", "+18135550112", "+18135550113"


def _contact(contact_id: str, phone, *tags: str) -> ListedContact:
    return ListedContact(contact_id=contact_id, phone=phone, tags=frozenset(tags))


class FakeLists:
    """GHL in memory: contacts by id with their tags; records every tag write in order."""

    def __init__(self, contacts: dict[str, tuple[str, set[str]]], failing_lists=(), failing_writes=()):
        self.contacts = contacts
        self.failing_lists, self.failing_writes = set(failing_lists), set(failing_writes)
        self.writes: list[tuple[str, str, str]] = []

    def contacts_with_tag(self, tag):
        if tag in self.failing_lists:
            raise GhlListError("search contacts: HTTP 500")
        return [{"id": cid, "phone": phone, "tags": sorted(tags)}
                for cid, (phone, tags) in self.contacts.items() if tag in tags]

    def add_tag(self, contact_id, tag):
        if contact_id in self.failing_writes:
            raise GhlListError("add tag: HTTP 500")
        self.writes.append(("add", contact_id, tag))
        self.contacts[contact_id][1].add(tag)

    def remove_tag(self, contact_id, tag):
        if contact_id in self.failing_writes:
            raise GhlListError("remove tag: HTTP 500")
        self.writes.append(("remove", contact_id, tag))
        self.contacts[contact_id][1].discard(tag)


# ── decide ────────────────────────────────────────────────────────────────────


def test_a_blocked_contact_without_the_tag_is_tagged_and_an_allowed_tagged_one_is_untagged():
    blocked = _contact("c1", PHONE_A, "lendingflow")
    allowed_tagged = _contact("c2", PHONE_B, "web-lead", TAG)
    changes = decide([blocked, allowed_tagged], {PHONE_A: "ATTEMPT_CAP_REACHED"})
    assert changes.to_add == [blocked]
    assert changes.to_remove == [allowed_tagged]


def test_unchanged_contacts_need_no_write():
    changes = decide([_contact("c1", PHONE_A, "lendingflow", TAG), _contact("c2", PHONE_B, "web-lead")],
                     {PHONE_A: "OUTSIDE_CALL_WINDOW"})
    assert changes.to_add == [] and changes.to_remove == []


def test_a_contact_with_no_usable_phone_is_left_as_it_is():
    changes = decide([_contact("c1", None, "lendingflow", TAG), _contact("c2", None, "web-lead")], {})
    assert changes.to_add == [] and changes.to_remove == []


# ── reading the lists ─────────────────────────────────────────────────────────


def test_a_contact_on_two_lists_is_read_once_with_lower_case_tags_and_a_normalized_phone():
    client = FakeLists({"c1": ("(813) 555-0111", {"lendingflow", "ndl-nurture", "Other-Tag"})})
    contacts, failed = read_listed_contacts(client)
    assert failed == []
    assert len(contacts) == 1
    assert contacts[0].phone == PHONE_A
    assert "other-tag" in contacts[0].tags
    assert contacts[0].lists == frozenset({"lendingflow", "ndl-nurture"})


def test_an_unreadable_list_is_reported_and_the_other_lists_are_still_read():
    client = FakeLists({"c1": (PHONE_A, {"lendingflow"}), "c2": (PHONE_B, {"web-lead"})}, failing_lists={"web-lead"})
    contacts, failed = read_listed_contacts(client)
    assert failed == ["web-lead"]
    assert [c.contact_id for c in contacts] == ["c1"]


# ── the rules ────────────────────────────────────────────────────────────────


def test_while_halted_every_phone_is_blocked_without_reading_the_database():
    assert blocked_reasons(None, [PHONE_B, PHONE_A], now=NOW, halted=True) == {
        PHONE_A: EMERGENCY_STOP, PHONE_B: EMERGENCY_STOP}


def test_rules_order_suppression_then_dial_blocks_then_dnc_result_and_lendingflow_skips_the_scrub(monkeypatch):
    calls = []

    def dial_blocks(db, phones, *, now, check_scrub=True):
        calls.append((sorted(phones), check_scrub))
        return {PHONE_B: ReasonCode.NO_FRESH_SCRUB} if PHONE_B in phones and check_scrub else {}

    monkeypatch.setattr(gate, "suppressed_phones", lambda db, phones: {PHONE_C})
    monkeypatch.setattr(gate, "reached_in_batchdialer_phones", lambda db, phones, *, now: set())
    monkeypatch.setattr(gate, "lendingflow_lead_phones", lambda db, phones: {PHONE_A})
    monkeypatch.setattr(gate, "dial_blocks", dial_blocks)
    verdict_inputs = []
    monkeypatch.setattr(gate, "scrub_verdict_blocks",
                        lambda db, phones: verdict_inputs.append(sorted(phones)) or {})

    reasons = blocked_reasons(object(), [PHONE_A, PHONE_B, PHONE_C], now=NOW, halted=False)

    assert reasons == {PHONE_C: "SUPPRESSED", PHONE_B: "NO_FRESH_SCRUB"}
    assert ([PHONE_B], True) in calls          # the scrub rule applies to the non-LendingFlow number
    assert ([PHONE_A], False) in calls         # and never to the LendingFlow lead
    assert verdict_inputs == [[]]              # only unblocked non-LendingFlow numbers reach the DNC check


def test_a_fresh_scrub_that_found_the_number_on_a_dnc_list_blocks_it(monkeypatch):
    monkeypatch.setattr(gate, "suppressed_phones", lambda db, phones: set())
    monkeypatch.setattr(gate, "reached_in_batchdialer_phones", lambda db, phones, *, now: set())
    monkeypatch.setattr(gate, "lendingflow_lead_phones", lambda db, phones: set())
    monkeypatch.setattr(gate, "dial_blocks", lambda db, phones, *, now, check_scrub=True: {})
    monkeypatch.setattr(gate, "scrub_verdict_blocks", lambda db, phones: {PHONE_A: ReasonCode.NATIONAL_DNC})
    assert blocked_reasons(object(), [PHONE_A], now=NOW, halted=False) == {PHONE_A: "NATIONAL_DNC"}


def test_a_number_the_isa_reached_in_batchdialer_is_held_off_joshs_lists(monkeypatch):
    """One calling rule: once the ISA gets through in BatchDialer, the GHL lists stop showing the number,
    whatever its lead type (LendingFlow included); suppression still wins as the reason."""
    monkeypatch.setattr(gate, "suppressed_phones", lambda db, phones: {PHONE_C})
    monkeypatch.setattr(gate, "reached_in_batchdialer_phones",
                        lambda db, phones, *, now: {PHONE_A, PHONE_B} & set(phones))
    monkeypatch.setattr(gate, "lendingflow_lead_phones", lambda db, phones: {PHONE_A} & set(phones))
    monkeypatch.setattr(gate, "dial_blocks", lambda db, phones, *, now, check_scrub=True: {})
    monkeypatch.setattr(gate, "scrub_verdict_blocks", lambda db, phones: {})
    assert blocked_reasons(object(), [PHONE_A, PHONE_B, PHONE_C], now=NOW, halted=False) == {
        PHONE_A: "REACHED_IN_BATCHDIALER", PHONE_B: "REACHED_IN_BATCHDIALER", PHONE_C: "SUPPRESSED"}


# ── a run ────────────────────────────────────────────────────────────────────


def test_a_run_tags_the_blocked_untags_the_allowed_and_never_touches_lead_type_tags(monkeypatch):
    client = FakeLists({"c1": (PHONE_A, {"lendingflow"}), "c2": (PHONE_B, {"web-lead", TAG}),
                        "c3": (PHONE_C, {"ndl-wholesaler"})})
    monkeypatch.setattr(gate, "blocked_reasons", lambda db, phones, *, now, halted: {PHONE_A: "ATTEMPT_CAP_REACHED"})

    result = run_gate(object(), client, halted=False, now=NOW)

    assert client.writes == [("add", "c1", TAG), ("remove", "c2", TAG)]
    assert all(tag == TAG for _, _, tag in client.writes)
    assert (result.added, result.removed, result.ok) == (1, 1, True)


def test_a_dry_run_writes_nothing(monkeypatch):
    client = FakeLists({"c1": (PHONE_A, {"lendingflow"})})
    monkeypatch.setattr(gate, "blocked_reasons", lambda db, phones, *, now, halted: {PHONE_A: "OUTSIDE_CALL_WINDOW"})
    result = run_gate(object(), client, halted=False, now=NOW, dry_run=True)
    assert client.writes == []
    assert result.blocked == 1


def test_when_the_rules_cannot_be_read_nothing_is_written(monkeypatch):
    client = FakeLists({"c2": (PHONE_B, {"web-lead", TAG})})

    def broken(db, phones, *, now, halted):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(gate, "blocked_reasons", broken)
    with pytest.raises(RuntimeError):
        run_gate(object(), client, halted=False, now=NOW)
    assert client.writes == []


def test_a_failed_write_is_counted_the_others_still_happen_and_the_run_is_not_ok(monkeypatch):
    client = FakeLists({"c1": (PHONE_A, {"lendingflow"}), "c2": (PHONE_B, {"web-lead"})}, failing_writes={"c1"})
    monkeypatch.setattr(gate, "blocked_reasons",
                        lambda db, phones, *, now, halted: dict.fromkeys(phones, EMERGENCY_STOP))
    result = run_gate(object(), client, halted=True, now=NOW)
    assert client.writes == [("add", "c2", TAG)]
    assert result.write_failures == 1 and not result.ok


def test_adds_are_written_before_removes():
    client = FakeLists({"c1": (PHONE_A, {"lendingflow", TAG}), "c2": (PHONE_B, {"web-lead"})})
    contacts, _ = read_listed_contacts(client)
    changes = decide(sorted(contacts, key=lambda c: c.contact_id), {PHONE_B: EMERGENCY_STOP})
    apply_changes(client, changes)
    assert [w[0] for w in client.writes] == ["add", "remove"]


def test_clear_removes_the_tag_from_every_contact_that_has_it():
    client = FakeLists({"c1": (PHONE_A, {"lendingflow", TAG}), "c2": (PHONE_B, {TAG}), "c3": (PHONE_C, {"web-lead"})})
    assert clear_tags(client) == (2, 0)
    assert all(TAG not in tags for _, tags in client.contacts.values())


def test_every_list_tag_is_read():
    client = FakeLists({f"c{i}": (f"+1813555{1000 + i}", {tag}) for i, tag in enumerate(GHL_CALL_LIST_TAGS)})
    contacts, failed = read_listed_contacts(client)
    assert len(contacts) == len(GHL_CALL_LIST_TAGS) and failed == []


# ── main ─────────────────────────────────────────────────────────────────────


class _Settings:
    def __init__(self, enabled):
        self.lending_ghl_call_gate_enabled = enabled


def test_main_is_idle_while_the_switch_is_off(monkeypatch):
    client = FakeLists({"c1": (PHONE_A, {"lendingflow"})})
    monkeypatch.setattr(gate, "lending_ghl_account", lambda: object())
    monkeypatch.setattr(gate, "GhlCallLists", lambda account: client)
    monkeypatch.setattr(gate, "get_settings", lambda: _Settings(False))
    monkeypatch.setattr(gate, "_run_once", lambda c: pytest.fail("ran while switched off"))
    assert gate.main([]) == 0


def test_clear_works_while_the_switch_is_off(monkeypatch):
    client = FakeLists({"c1": (PHONE_A, {"lendingflow", TAG})})
    monkeypatch.setattr(gate, "lending_ghl_account", lambda: object())
    monkeypatch.setattr(gate, "GhlCallLists", lambda account: client)
    monkeypatch.setattr(gate, "get_settings", lambda: _Settings(False))
    assert gate.main(["--clear"]) == 0
    assert client.writes == [("remove", "c1", TAG)]


def test_main_fails_without_the_lending_ghl_account(monkeypatch):
    monkeypatch.setattr(gate, "lending_ghl_account", lambda: None)
    assert gate.main([]) == gate.EXIT_FAILED


# ── database: the rules and the failure alert ─────────────────────────────────


@pytest.fixture
def gate_db(lending_db):
    from migrations.apply_lending_ghl_call_gate_state import apply_to

    apply_to(lending_db.connection())
    return lending_db


def test_the_alert_fires_once_after_ten_minutes_of_failing_and_a_success_resets_it(gate_db):
    assert record_outcome(gate_db, ok=False, now=NOW) is False
    assert record_outcome(gate_db, ok=False, now=NOW + timedelta(minutes=9)) is False
    assert record_outcome(gate_db, ok=False, now=NOW + timedelta(minutes=10)) is True
    assert record_outcome(gate_db, ok=False, now=NOW + timedelta(minutes=11)) is False
    assert record_outcome(gate_db, ok=True, now=NOW + timedelta(minutes=12)) is False
    assert record_outcome(gate_db, ok=False, now=NOW + timedelta(minutes=13)) is False
    assert record_outcome(gate_db, ok=False, now=NOW + timedelta(minutes=23)) is True


def _scrub(db, phone, *, age_days=1, national_dnc=False):
    db.execute(text("INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
                    "VALUES (:p, :dnc, false, false, :at) ON CONFLICT (phone) DO UPDATE SET "
                    "national_dnc = EXCLUDED.national_dnc, checked_at = EXCLUDED.checked_at"),
               {"p": phone, "dnc": national_dnc, "at": NOW - timedelta(days=age_days)})


def _lendingflow_lead(db, phone):
    db.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, phone) "
                    "VALUES (:v, :h, '{}'::jsonb, :p)"), {"v": f"v-{phone}", "h": f"h-{phone}", "p": phone})


def test_rules_against_the_database(lending_db):
    unscrubbed_web_lead, dnc_hit, clean, lendingflow, opted_out = (
        "+18135550201", "+18135550202", "+18135550203", "+18135550204", "+18135550205")
    _scrub(lending_db, dnc_hit, national_dnc=True)
    _scrub(lending_db, clean)
    _scrub(lending_db, opted_out)
    _lendingflow_lead(lending_db, lendingflow)
    lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                            "VALUES (:p, 'OPT_OUT', 'sms')"), {"p": opted_out})

    reasons = blocked_reasons(lending_db, [unscrubbed_web_lead, dnc_hit, clean, lendingflow, opted_out],
                              now=NOW, halted=False)

    assert reasons == {
        unscrubbed_web_lead: "NO_FRESH_SCRUB",
        dnc_hit: "NATIONAL_DNC",
        opted_out: "SUPPRESSED",
    }


def test_three_calls_in_24_hours_block_a_lendingflow_lead(lending_db):
    lendingflow = "+18135550206"
    _lendingflow_lead(lending_db, lendingflow)
    for n in range(3):
        lending_db.execute(text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, call_ended_at, raw_event) "
            "VALUES (:id, 'ghl', 'outbound', :p, :at, '{}'::jsonb)"),
            {"id": f"ghl:gate-{n}", "p": lendingflow, "at": NOW - timedelta(hours=n + 1)})
    assert blocked_reasons(lending_db, [lendingflow], now=NOW, halted=False) == {lendingflow: "ATTEMPT_CAP_REACHED"}
