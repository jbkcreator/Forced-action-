"""
Unit tests for the Sunbiz detail-page parser. All tests run against committed
fixture HTML — no network, no DB, no Playwright.

Test taxonomy: each fixture exercises a different real-world shape (member-managed,
manager-managed, dissolved, foreign, corporation, recursive LLC member, single-member,
partial filing, no-results page, garbage HTML).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.scrappers.sunbiz.parser import (
    PARSER_VERSION,
    SunbizSnapshot,
    parse_sunbiz_detail,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sunbiz"


def _load(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ── Active LLC with two managing members and an agent email ─────────────────

def test_active_member_managed_full_capture():
    snap = parse_sunbiz_detail(_load("active_member_managed.html"))

    assert snap.status == "ok"
    assert snap.doc_number == "L21000123456"
    assert snap.name == "ACME HOLDINGS LLC"
    assert snap.fei_ein == "87-1234567"
    assert snap.entity_status == "ACTIVE"
    assert snap.formation_date == date(2021, 3, 15)
    assert "123 MAIN ST" in (snap.principal_address or "")
    assert "TAMPA, FL 33602" in (snap.principal_address or "")
    assert snap.mailing_address and "PO BOX 999" in snap.mailing_address
    assert snap.registered_agent_name == "SMITH, JOHN A"
    assert snap.registered_agent_address and "500 BAYSHORE BLVD" in snap.registered_agent_address
    assert snap.registered_agent_email == "john.smith@example.com"

    members = snap.managing_members
    assert len(members) == 2
    assert {m.name for m in members} == {"SMITH, JOHN A", "DOE, JANE"}
    assert all(m.role == "MGRM" for m in members)
    assert all(m.address for m in members)


# ── Manager-managed LLC, NONE FEI, registered-agent service co ──────────────

def test_active_manager_managed_none_fei_treated_as_null():
    snap = parse_sunbiz_detail(_load("active_manager_managed.html"))

    assert snap.status == "ok"
    assert snap.doc_number == "L19000999888"
    assert snap.fei_ein is None, "literal 'NONE' should become null, not a stored value"
    assert snap.entity_status == "ACTIVE"
    assert snap.registered_agent_name == "CT CORPORATION SYSTEM"
    assert snap.registered_agent_email is None
    assert len(snap.managing_members) == 1
    assert snap.managing_members[0].role == "MGR"
    assert snap.managing_members[0].name == "ROBERTS, MICHAEL"


# ── Administratively dissolved LLC ──────────────────────────────────────────

def test_dissolved_llc_status_and_no_members_section():
    snap = parse_sunbiz_detail(_load("dissolved_llc.html"))

    assert snap.status == "ok"
    assert snap.entity_status == "INACTIVE"
    assert snap.doc_number == "L15000111222"
    assert snap.managing_members == []
    assert snap.formation_date == date(2015, 1, 2)


# ── Foreign LLC (F-prefix doc number) ───────────────────────────────────────

def test_foreign_llc_f_prefix_captured():
    snap = parse_sunbiz_detail(_load("foreign_llc.html"))

    assert snap.status == "ok"
    assert snap.doc_number == "F20000001234"
    assert snap.doc_number.startswith("F"), "foreign LLCs use F-prefix doc numbers"
    assert snap.name == "DELAWARE HOLDINGS LP"


# ── Single-member LLC, no agent email ───────────────────────────────────────

def test_single_member_no_email_status_ok():
    snap = parse_sunbiz_detail(_load("single_member_no_email.html"))

    assert snap.status == "ok"
    assert snap.registered_agent_email is None
    assert len(snap.managing_members) == 1
    assert snap.managing_members[0].name == "WILLIAMS, SARA"
    assert snap.fei_ein is None  # not present in fixture
    assert snap.mailing_address is None  # section absent


# ── Corporation with officers (P / VP / D) ──────────────────────────────────

def test_corp_officers_parsed_into_officers_not_members():
    snap = parse_sunbiz_detail(_load("corp_with_officers.html"))

    assert snap.status == "ok"
    assert snap.doc_number == "P05000088888"
    assert snap.managing_members == []
    assert len(snap.officers) == 3
    roles = sorted(o.role for o in snap.officers)
    assert roles == ["D", "P", "VP"]


# ── Recursive: managing member is itself an LLC ─────────────────────────────

def test_member_is_another_llc():
    snap = parse_sunbiz_detail(_load("member_is_another_llc.html"))

    assert snap.status == "ok"
    assert len(snap.managing_members) == 1
    member = snap.managing_members[0]
    assert member.name == "PARENT HOLDINGS LLC"
    assert member.role == "MGRM"
    # Sanity: parser doesn't try to be smart about LLC-vs-person — caller decides.


# ── Partial: missing principal_address → status='partial' ───────────────────

def test_partial_when_required_field_missing():
    snap = parse_sunbiz_detail(_load("partial_no_principal.html"))

    assert snap.status == "partial"
    assert snap.doc_number == "L24000222111"
    assert snap.principal_address is None
    assert any("missing_required" in w for w in snap.warnings)
    assert "principal_address" in next(w for w in snap.warnings if "missing_required" in w)


# ── No-results page → parser_failed ─────────────────────────────────────────

def test_no_detail_sections_returns_parser_failed():
    snap = parse_sunbiz_detail(_load("no_results_page.html"))

    assert snap.status == "parser_failed"
    assert "no_detail_sections" in snap.warnings
    assert snap.doc_number is None


# ── Garbage HTML → parser_failed, no crash ──────────────────────────────────

def test_garbage_html_does_not_raise():
    snap = parse_sunbiz_detail(_load("garbage_html.html"))
    assert snap.status == "parser_failed"


# ── Empty / None input ──────────────────────────────────────────────────────

@pytest.mark.parametrize("html", ["", "   ", "\n\n"])
def test_empty_input(html):
    snap = parse_sunbiz_detail(html)
    assert snap.status == "parser_failed"
    assert "empty_html" in snap.warnings


# ── Snapshot serialization round-trips for JSONB storage ────────────────────

def test_to_jsonb_serializes_date_and_members():
    snap = parse_sunbiz_detail(_load("active_member_managed.html"))
    jb = snap.to_jsonb()

    assert jb["status"] == "ok"
    assert jb["formation_date"] == "2021-03-15"
    assert jb["parser_version"] == PARSER_VERSION
    assert len(jb["managing_members"]) == 2
    assert all("name" in m and "role" in m for m in jb["managing_members"])
    # JSONB-safe: no datetime / dataclass instances leak through.
    import json
    json.dumps(jb)


# ── Parser is pure: same input → same output, no side effects ───────────────

def test_parser_is_pure():
    html = _load("active_member_managed.html")
    a = parse_sunbiz_detail(html).to_jsonb()
    b = parse_sunbiz_detail(html).to_jsonb()
    assert a == b


# ── parser_version constant ─────────────────────────────────────────────────

def test_parser_version_is_semver_like():
    assert PARSER_VERSION.startswith("sunbiz-parser/")
    assert SunbizSnapshot().parser_version == PARSER_VERSION
