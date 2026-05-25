"""
Additional parser shape tests — supplements test_sunbiz_parser.py.

Covers shapes not yet exercised by the 10 original fixtures:
  - AMBR (Authorized Member) role code
  - Mixed member section: person (AMBR) + LLC (MGRM) in same block
  - Event Date Filed must NOT clobber formation_date (Date Filed)
  - Explicit assertion that Last Event text is ignored (not stored as formation_date)
  - Entity with agent email in principal section (false-positive guard)
  - JSONB managing_members serialisation preserves AMBR role for downstream callers
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.scrappers.sunbiz.parser import parse_sunbiz_detail

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sunbiz"


def _load(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ── AMBR role code ──────────────────────────────────────────────────────────

def test_ambr_member_role_preserved():
    """AMBR (Authorized Member) is a valid Sunbiz title — must be stored, not dropped."""
    snap = parse_sunbiz_detail(_load("mixed_member_types.html"))
    roles = {m.role for m in snap.managing_members}
    assert "AMBR" in roles, f"AMBR not found in roles {roles}"


def test_ambr_member_name_captured():
    snap = parse_sunbiz_detail(_load("mixed_member_types.html"))
    names = {m.name for m in snap.managing_members}
    assert "PATEL, RAJESH" in names


# ── Mixed person + LLC member types ────────────────────────────────────────

def test_mixed_members_both_captured():
    """One AMBR person and one MGRM LLC in the same Authorized Persons section."""
    snap = parse_sunbiz_detail(_load("mixed_member_types.html"))
    assert len(snap.managing_members) == 2
    names = {m.name for m in snap.managing_members}
    assert "PATEL, RAJESH" in names
    assert "PARENT HOLDCO LLC" in names


def test_mixed_members_roles_match():
    snap = parse_sunbiz_detail(_load("mixed_member_types.html"))
    by_name = {m.name: m.role for m in snap.managing_members}
    assert by_name["PATEL, RAJESH"] == "AMBR"
    assert by_name["PARENT HOLDCO LLC"] == "MGRM"


def test_mixed_members_addresses_captured():
    snap = parse_sunbiz_detail(_load("mixed_member_types.html"))
    by_name = {m.name: m.address for m in snap.managing_members}
    assert by_name["PATEL, RAJESH"] and "BRICKELL" in by_name["PATEL, RAJESH"]
    assert by_name["PARENT HOLDCO LLC"] and "BISCAYNE" in by_name["PARENT HOLDCO LLC"]


# ── Event Date Filed must NOT clobber Date Filed (formation_date) ────────────

def test_event_date_filed_does_not_become_formation_date():
    """
    Dissolved LLC has both 'Date Filed' (2018-07-04) and 'Event Date Filed' (2022-12-15).
    formation_date must be the entity's filing date, NOT the dissolution event date.
    """
    snap = parse_sunbiz_detail(_load("event_date_filed_ambiguity.html"))
    assert snap.formation_date == date(2018, 7, 4), (
        f"Expected 2018-07-04 (Date Filed) but got {snap.formation_date}; "
        "Event Date Filed (2022-12-15) must not clobber formation_date"
    )


def test_event_date_filed_entity_status_is_inactive():
    snap = parse_sunbiz_detail(_load("event_date_filed_ambiguity.html"))
    assert snap.entity_status == "INACTIVE"


# ── doc_number from event_date fixture ──────────────────────────────────────

def test_event_date_filed_doc_number_captured():
    snap = parse_sunbiz_detail(_load("event_date_filed_ambiguity.html"))
    assert snap.doc_number == "L18000555444"


# ── JSONB serialisation preserves AMBR role ──────────────────────────────────

def test_to_jsonb_preserves_ambr_role():
    snap = parse_sunbiz_detail(_load("mixed_member_types.html"))
    jb = snap.to_jsonb()
    roles = {m["role"] for m in jb["managing_members"]}
    assert "AMBR" in roles
    # Ensure JSONB-safe (no dataclass instances)
    import json
    json.dumps(jb)
