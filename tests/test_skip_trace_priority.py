"""
Unit tests for the v1 skip-trace priority chain (fa031): filing party →
managing member → registered agent → skip.

The full `run_skip_trace` orchestrator hits BatchData; these tests target the
pure deterministic `_build_entity_payload` helper directly. Inputs are simple
namespace objects matching the (owner, prop) attribute surface the helper
expects — no DB, no network.
"""

from __future__ import annotations

from types import SimpleNamespace

from src.services.skip_trace import _build_entity_payload


def _owner(**kw) -> SimpleNamespace:
    defaults = dict(
        owner_type="LLC",
        owner_name="ACME HOLDINGS LLC",
        registered_agent_name=None,
        registered_agent_address=None,
        managing_members=None,
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


def _prop(**kw) -> SimpleNamespace:
    defaults = dict(
        id=1, address="500 MAIN ST", city="Tampa", state="FL", zip="33602",
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


def _no_party(_prop_id: int):
    return None, None


def _party(name: str, source: str = "probate"):
    def _resolve(_prop_id: int):
        return name, source
    return _resolve


MEMBER_VALID = {
    "name": "SMITH, JOHN A",
    "address": "500 BAYSHORE BLVD\nTAMPA, FL 33606",
    "role": "MGRM",
}
MEMBER_LLC = {  # recursive — should be skipped (entity, not a person)
    "name": "PARENT HOLDINGS LLC",
    "address": "1 BISCAYNE TOWER\nMIAMI, FL 33131",
    "role": "MGRM",
}
MEMBER_BAD_ADDRESS = {
    "name": "DOE, JANE",
    "address": "garbage",  # unparseable
    "role": "MGRM",
}
AGENT_FIXTURE = {
    "registered_agent_name": "CT CORPORATION SYSTEM",
    "registered_agent_address": "1200 SOUTH PINE ISLAND ROAD\nPLANTATION, FL 33324",
}


# ── Tier 1: filing-derived party wins over everything ───────────────────────

def test_filing_party_beats_managing_member_and_agent():
    owner = _owner(managing_members=[MEMBER_VALID], **AGENT_FIXTURE)
    payload = _build_entity_payload(owner, _prop(), _party("ROBERTS, MICHAEL"))

    assert payload is not None
    assert payload["firstName"] == "MICHAEL"
    assert payload["lastName"] == "ROBERTS"
    # Filing party uses property address, not member or agent address.
    assert "propertyAddress" in payload
    assert payload["propertyAddress"]["street"] == "500 MAIN ST"
    assert "address" not in payload


# ── Tier 2: managing member used when no filing party ───────────────────────

def test_managing_member_used_when_no_filing_party():
    owner = _owner(managing_members=[MEMBER_VALID], **AGENT_FIXTURE)
    payload = _build_entity_payload(owner, _prop(), _no_party)

    assert payload["firstName"] == "JOHN A"
    assert payload["lastName"] == "SMITH"
    # Member address is used, not property or agent.
    assert payload["address"]["street"] == "500 BAYSHORE BLVD"
    assert payload["address"]["zip"] == "33606"


def test_managing_member_skipped_when_entity_name_falls_through_to_agent():
    """If the only member is another LLC, fall through to agent."""
    owner = _owner(managing_members=[MEMBER_LLC], **AGENT_FIXTURE)
    payload = _build_entity_payload(owner, _prop(), _no_party)

    # No comma in "CT CORPORATION SYSTEM" → split on space: first="CT".
    assert payload["firstName"] == "CT"
    assert payload["lastName"] == "CORPORATION SYSTEM"
    assert payload["address"]["street"] == "1200 SOUTH PINE ISLAND ROAD"


def test_managing_member_unparseable_address_falls_back_to_property_address():
    owner = _owner(managing_members=[MEMBER_BAD_ADDRESS], **AGENT_FIXTURE)
    payload = _build_entity_payload(owner, _prop(), _no_party)

    # First viable member wins even with broken address — use property address.
    assert payload["firstName"] == "JANE"
    assert payload["lastName"] == "DOE"
    assert payload["propertyAddress"]["street"] == "500 MAIN ST"


def test_first_viable_member_wins_skipping_llc_member_ahead():
    """LLC member at index 0, person at index 1 — person should be picked."""
    owner = _owner(
        managing_members=[MEMBER_LLC, MEMBER_VALID],
        **AGENT_FIXTURE,
    )
    payload = _build_entity_payload(owner, _prop(), _no_party)

    assert payload["lastName"] == "SMITH"


# ── Tier 3: registered agent fallback ───────────────────────────────────────

def test_agent_used_when_no_party_and_no_members():
    owner = _owner(managing_members=None, **AGENT_FIXTURE)
    payload = _build_entity_payload(owner, _prop(), _no_party)

    assert payload["firstName"] == "CT"
    assert payload["lastName"] == "CORPORATION SYSTEM"
    assert payload["address"]["state"] == "FL"
    assert payload["address"]["zip"] == "33324"


def test_agent_used_when_members_all_entities():
    owner = _owner(managing_members=[MEMBER_LLC, MEMBER_LLC], **AGENT_FIXTURE)
    payload = _build_entity_payload(owner, _prop(), _no_party)

    assert payload["lastName"] == "CORPORATION SYSTEM"  # agent picked


# ── Returns None when nothing usable ────────────────────────────────────────

def test_returns_none_when_no_party_no_members_no_agent():
    owner = _owner(managing_members=None)
    payload = _build_entity_payload(owner, _prop(), _no_party)
    assert payload is None


def test_returns_none_when_agent_address_unparseable_and_nothing_else():
    owner = _owner(
        registered_agent_name="JONES, ROBERT",
        registered_agent_address="garbage",
    )
    payload = _build_entity_payload(owner, _prop(), _no_party)
    assert payload is None


def test_returns_none_when_property_missing_address_for_filing_party():
    """Filing-party requires a valid property address — fall through if missing."""
    owner = _owner(managing_members=[MEMBER_VALID])
    payload = _build_entity_payload(
        owner, _prop(address="", zip=""), _party("ROBERTS, MICHAEL"),
    )
    # Filing-party tier rejected (no address) but member tier still works.
    assert payload is not None
    assert payload["lastName"] == "SMITH"
