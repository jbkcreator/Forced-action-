"""
Unit tests (pure, no DB): src.agents.hunter.auction_resolution's
_guess_entity_type, and the normalization-based dedup property that makes
resolve_tax_deed_winners collapse differently-formatted appearances of the
same winner onto one entity.

DB-touching integration coverage (exact-match/ambiguous/no-match/dedup/stale
against real TaxDeedAuction + buyer_entities rows) lives in
tests/scenarios/test_auction_resolution_scenarios.py -- resolve_tax_deed_winners
self-commits, so per this repo's own precedent
(tests/scenarios/test_hunter_resolution_fixes.py) that coverage belongs under
tests/scenarios/ behind the `scenario` marker.
"""
from __future__ import annotations

from src.agents.hunter.auction_resolution import PROVISIONAL_CONFIDENCE, _guess_entity_type
from src.agents.hunter.gating import UNVERIFIED_FLOOR
from src.loaders.base import BaseLoader


class TestGuessEntityType:
    def test_llc_token(self):
        assert _guess_entity_type(BaseLoader.normalize_owner_name("SUNSHINE PROPERTY GROUP LLC")) in ("LLC", "Individual")
        # normalize_owner_name may itself strip the LLC token -- assert on
        # the pre-normalization guess path directly instead, which is what
        # resolve_tax_deed_winners actually calls the guesser with.
        assert _guess_entity_type("SUNSHINE PROPERTY GROUP LLC") == "LLC"

    def test_inc_token(self):
        assert _guess_entity_type("ACME HOLDINGS INC") == "LLC"

    def test_trust_token(self):
        assert _guess_entity_type("SMITH FAMILY TRUST") == "Trust"

    def test_plain_name_defaults_to_individual(self):
        assert _guess_entity_type("JOHN SMITH") == "Individual"


class TestProvisionalConfidenceBelowUnverifiedFloor:
    def test_provisional_confidence_is_unverified(self):
        """A provisional entity created from an auction win alone must never
        surface in a Lifecycle draft until corroborated -- confidence must
        stay below gating.UNVERIFIED_FLOOR."""
        assert PROVISIONAL_CONFIDENCE < UNVERIFIED_FLOOR


class TestNormalizationDedup:
    def test_differently_formatted_same_winner_normalizes_identically(self):
        """The exact-match dedup step relies on this: two raw spellings of
        the same winner must collapse to the same normalized key, or the
        same LLC re-winning auctions would spawn a new entity every time."""
        a = BaseLoader.normalize_owner_name("Sunshine Property Group, LLC")
        b = BaseLoader.normalize_owner_name("SUNSHINE PROPERTY GROUP LLC")
        assert a == b

    def test_different_winners_normalize_differently(self):
        a = BaseLoader.normalize_owner_name("Sunshine Property Group LLC")
        b = BaseLoader.normalize_owner_name("Moonlight Holdings LLC")
        assert a != b
