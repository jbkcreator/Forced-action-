"""Every lending Slack post names the person next to the record id (src.lending.slack_names)."""
from __future__ import annotations

from src.lending.slack_names import UNKNOWN, full_name, name_and_ref, names_by_phone, readable_ref, shared_phone_label


def test_name_comes_first_and_the_id_follows_in_brackets() -> None:
    assert name_and_ref("Daniel Smith", "lead 1424") == "Daniel Smith (lead 1424)"


def test_a_missing_name_says_name_not_on_file_and_keeps_the_id() -> None:
    assert name_and_ref(None, "call 1222017054") == f"{UNKNOWN} (call 1222017054)"
    assert name_and_ref("   ", "web lead 7") == f"{UNKNOWN} (web lead 7)"


def test_names_are_escaped_so_they_cannot_ping_the_channel() -> None:
    shown = name_and_ref("<!channel> & Co", "lead 1")
    assert "<!channel>" not in shown and "&lt;!channel&gt; &amp; Co (lead 1)" == shown


def test_unescaped_form_is_for_text_agent_core_escapes_later() -> None:
    assert name_and_ref("A & B", "contact 9", escape=False) == "A & B (contact 9)"


def test_phone_numbers_in_a_name_field_stay_masked() -> None:
    assert "8135550147" not in name_and_ref("call 813-555-0147", "lead 1")


def test_full_name_joins_first_and_last_and_skips_blanks() -> None:
    assert full_name("Ana", "Lopez") == "Ana Lopez"
    assert full_name("Ana", None) == "Ana" and full_name(None, " ") is None


def test_refs_read_the_way_the_client_reads_them() -> None:
    assert readable_ref("contact:18008") == "contact 18008"
    assert readable_ref("web_lead:12") == "web lead 12"
    assert readable_ref("f0084183") == "f0084183"


def test_a_failed_lookup_drops_names_but_never_raises() -> None:
    class BrokenSession:
        def begin_nested(self):
            raise RuntimeError("no database")

    assert names_by_phone(BrokenSession(), ["+18135550147"]) == {}
    assert names_by_phone(BrokenSession(), [None, ""]) == {}


def test_the_fallback_wording_suits_files_and_callers_alike() -> None:
    assert UNKNOWN == "Name not on file"
    assert name_and_ref(None, "File #82") == "Name not on file (File #82)"


def test_people_sharing_a_phone_are_all_named_never_guessed() -> None:
    assert shared_phone_label(["Ana Lopez"]) == "Ana Lopez"
    assert shared_phone_label(["Ana Lopez", "Ben Ray"]) == "Ana Lopez or Ben Ray"
    assert shared_phone_label(["Ana Lopez", "ana lopez", "Ben Ray"]) == "Ana Lopez or Ben Ray"  # same person once
    assert shared_phone_label(["A B", "C D", "E F", "G H"]) == "A B or C D (+2 more)"


def test_a_shared_phone_lookup_returns_every_name_in_priority_order(lending_db) -> None:
    from sqlalchemy import text

    phone = "+18135550990"
    lending_db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, first_name, last_name, phone)"
        " VALUES ('sn-1', 'sn-hash-1', '{}', 'Ana', 'Lopez', :p)"), {"p": phone})
    lending_db.execute(text(
        "INSERT INTO lending.web_leads (name, phone) VALUES ('Ben Ray', :p)"), {"p": phone})
    assert names_by_phone(lending_db, [phone]) == {phone: "Ana Lopez or Ben Ray"}
