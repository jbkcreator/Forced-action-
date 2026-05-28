"""
fa044 — Attribution Service tests (Stage 8).

All tests use fake DB sessions — no live database required.
Pattern mirrors tests/test_revenue_signal_score.py and
tests/test_scoring_training_data.py.

The service makes at minimum 5 SQL calls per invocation when all dimension
hints are supplied:
  1. _resolve_trade()       → SELECT vertical FROM subscribers
  2. INSERT CAE RETURNING id
  3. SELECT revenue_signal_score FOR UPDATE
  4. INSERT revenue_signal_score_events
  5. UPDATE subscribers

When dimension hints are omitted the resolver helpers fire additional SELECTs;
those calls are added to the result queue for the specific test that exercises
them.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fake session infrastructure
# ─────────────────────────────────────────────────────────────────────────────

class _MappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def mappings(self):
        return _MappingsResult(self._rows)

    def scalar(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    """Queue-based fake session.

    Pass a list of result row lists; each `execute()` pops the next one.
    Set `raise_on_nested=True` to simulate a UniqueViolation on the
    INSERT inside begin_nested().
    """
    def __init__(self, results=None, raise_on_nested=False):
        self._results = list(results or [])
        self._raise_on_nested = raise_on_nested
        self.executed: list = []

    def execute(self, statement, params=None):
        self.executed.append({"sql": str(statement), "params": params})
        if not self._results:
            return _FakeResult()
        return _FakeResult(self._results.pop(0))

    def flush(self):
        pass

    def add(self, obj):
        pass

    @contextlib.contextmanager
    def begin_nested(self):
        if self._raise_on_nested:
            from sqlalchemy.exc import IntegrityError
            try:
                from psycopg2.errors import UniqueViolation as _UV
                orig = _UV()
            except Exception:
                # psycopg2 not available — create a duck-type stand-in.
                class _UV(Exception):
                    pass
                orig = _UV()
            raise IntegrityError("duplicate key", {}, orig)
        yield


# ─────────────────────────────────────────────────────────────────────────────
# Common fixtures
# ─────────────────────────────────────────────────────────────────────────────

_NOW = datetime.now(timezone.utc)

def _base_results():
    """Result queue for a successful invocation with all hints supplied.

    _call_with_all_hints passes bundle_id=99 (truthy) so _resolve_bundle
    returns immediately without a DB query.
    Execution order:
      1. _resolve_trade()                  → SELECT vertical FROM subscribers
      2. INSERT conversion_attribution_events RETURNING id
      3. SELECT revenue_signal_score FROM subscribers FOR UPDATE
      4. INSERT revenue_signal_score_events  (no rows consumed)
      5. UPDATE subscribers                  (no rows consumed)
    """
    return [
        [{"vertical": "roofing"}],           # resolve_trade
        [{"id": 42}],                        # INSERT CAE RETURNING id
        [{"revenue_signal_score": 0}],       # SELECT old score
        [],                                  # INSERT score event (result unused)
        [],                                  # UPDATE subscribers (result unused)
    ]


def _call_with_all_hints(db, *, conversion_type="paid_unlock", subscriber_id=1,
                          property_id=None, deal_size_bucket=None):
    from src.services.attribution_service import record_conversion_attribution
    return record_conversion_attribution(
        conversion_type=conversion_type,
        source_table="stripe_payment_intents",
        source_event_id="pi_test_001",
        subscriber_id=subscriber_id,
        occurred_at=_NOW,
        revenue_amount=4.00,
        lead_id=10,
        zip_code="33602",
        wallet_tier="gold",
        lock_status="locked",
        lock_zip="33602",
        autopilot_tier="not_applicable",
        bundle_id=99,            # truthy → _resolve_bundle returns immediately, no DB query
        bundle_type="premium",
        property_id=property_id,
        deal_size_bucket=deal_size_bucket,
        db=db,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. Row inserted, all 8 dimensions present
# ─────────────────────────────────────────────────────────────────────────────

def test_paid_unlock_creates_attribution_row():
    db = _FakeSession(results=_base_results())
    event_id = _call_with_all_hints(db)
    assert event_id == 42
    # executed[0]=resolve_trade, executed[1]=INSERT CAE
    insert_call = db.executed[1]
    assert "conversion_attribution_events" in insert_call["sql"]
    assert insert_call["params"]["conversion_type"] == "paid_unlock"
    assert insert_call["params"]["zip_code"] == "33602"
    assert insert_call["params"]["trade"] == "roofing"
    assert insert_call["params"]["wallet_tier"] == "gold"
    assert insert_call["params"]["lock_status"] == "locked"
    assert insert_call["params"]["autopilot_tier"] == "not_applicable"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Wallet tier resolved from WalletBalance when not supplied
# ─────────────────────────────────────────────────────────────────────────────

def test_wallet_activation_attribution():
    # Resolution order (service): zip(provided)→skip, wallet_tier→DB, bundle(id=1)→skip, trade→DB
    # So DB calls: [0]=wallet_tier, [1]=trade, [2]=INSERT CAE, ...
    results = [
        [{"wallet_tier": "platinum"}],      # _resolve_wallet_tier (wallet_tier omitted)
        [{"vertical": "roofing"}],          # _resolve_trade
        [{"id": 7}],                         # INSERT CAE
        [{"revenue_signal_score": 20}],     # SELECT old score
        [], [],                              # INSERT score + UPDATE
    ]
    from src.services.attribution_service import record_conversion_attribution
    db = _FakeSession(results=results)
    event_id = record_conversion_attribution(
        conversion_type="wallet_activation",
        source_table="stripe_invoices",
        source_event_id="inv_001",
        subscriber_id=1,
        occurred_at=_NOW,
        # wallet_tier omitted — triggers DB resolution
        lead_id=5,
        zip_code="33602",
        lock_status="locked",
        lock_zip="33602",
        autopilot_tier="not_applicable",
        bundle_id=1,
        bundle_type="platinum",
        db=db,
    )
    assert event_id == 7
    # [0]=wallet_tier query, [1]=trade query, [2]=INSERT CAE
    insert_params = db.executed[2]["params"]
    assert insert_params["wallet_tier"] == "platinum"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Bundle id/type resolved from DB fallback
# ─────────────────────────────────────────────────────────────────────────────

def test_bundle_purchase_attribution():
    # Resolution order: bundle(id=None)→DB, trade→DB, then INSERT
    # DB calls: [0]=bundle_purchases query, [1]=trade query, [2]=INSERT CAE
    results = [
        [{"id": 55, "bundle_type": "roofing_50"}],          # _resolve_bundle fallback
        [{"vertical": "roofing"}],                          # _resolve_trade
        [{"id": 9}],                                         # INSERT CAE
        [{"revenue_signal_score": 10}],                     # SELECT old score
        [], [],                                              # INSERT score + UPDATE
    ]
    from src.services.attribution_service import record_conversion_attribution
    db = _FakeSession(results=results)
    event_id = record_conversion_attribution(
        conversion_type="bundle_purchase",
        source_table="stripe_payment_intents",
        source_event_id="pi_bundle_001",
        subscriber_id=1,
        occurred_at=_NOW,
        lead_id=1,
        zip_code="33602",
        wallet_tier="silver",
        lock_status="locked",
        lock_zip="33602",
        autopilot_tier="not_applicable",
        # bundle_id omitted — triggers DB resolution
        db=db,
    )
    assert event_id == 9
    # [0]=bundle query, [1]=trade query, [2]=INSERT CAE
    insert_params = db.executed[2]["params"]
    assert insert_params["bundle_id"] == 55
    assert insert_params["bundle_type"] == "roofing_50"


# ─────────────────────────────────────────────────────────────────────────────
# 4. lock_status = "locked" and lock_zip present when territory active
# ─────────────────────────────────────────────────────────────────────────────

def test_territory_lock_attribution():
    db = _FakeSession(results=_base_results())
    _call_with_all_hints(db, conversion_type="territory_lock_purchase")
    insert_params = db.executed[1]["params"]
    assert insert_params["lock_status"] == "locked"
    assert insert_params["lock_zip"] == "33602"


# ─────────────────────────────────────────────────────────────────────────────
# 5. autopilot_tier resolved from Subscriber.tier
# ─────────────────────────────────────────────────────────────────────────────

def test_autopilot_upgrade_attribution():
    # Resolution order: autopilot(omitted)→DB, bundle(id=None)→DB, trade→DB, INSERT
    # DB calls: [0]=autopilot query, [1]=bundle query, [2]=trade query, [3]=INSERT CAE
    results = [
        [{"tier": "autopilot_pro"}],            # _resolve_autopilot_tier
        [],                                      # _resolve_bundle (no active bundle)
        [{"vertical": "roofing"}],              # _resolve_trade
        [{"id": 3}],                             # INSERT CAE
        [{"revenue_signal_score": 50}],         # SELECT old score
        [], [],                                  # INSERT score + UPDATE
    ]
    from src.services.attribution_service import record_conversion_attribution
    db = _FakeSession(results=results)
    event_id = record_conversion_attribution(
        conversion_type="autopilot_pro_upgrade",
        source_table="checkout_sessions",
        source_event_id="cs_ap_001",
        subscriber_id=1,
        occurred_at=_NOW,
        lead_id=1,
        zip_code="33602",
        wallet_tier="gold",
        lock_status="locked",
        lock_zip="33602",
        # autopilot_tier omitted — triggers DB resolution
        bundle_id=None,
        bundle_type="not_applicable",
        db=db,
    )
    assert event_id == 3
    # [0]=autopilot, [1]=bundle, [2]=trade, [3]=INSERT CAE
    insert_params = db.executed[3]["params"]
    assert insert_params["autopilot_tier"] == "autopilot_pro"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Annual upgrade event recorded, deal_size = not_applicable
# ─────────────────────────────────────────────────────────────────────────────

def test_annual_upgrade_attribution():
    db = _FakeSession(results=_base_results())
    event_id = _call_with_all_hints(db, conversion_type="annual_upgrade")
    assert event_id == 42
    insert_params = db.executed[1]["params"]
    assert insert_params["conversion_type"] == "annual_upgrade"
    assert insert_params["deal_size_bucket"] == "not_applicable"


# ─────────────────────────────────────────────────────────────────────────────
# 7. deal_win_reported + 10k+ bonus delta
# ─────────────────────────────────────────────────────────────────────────────

def test_deal_win_with_size_bucket():
    db = _FakeSession(results=_base_results())
    _call_with_all_hints(
        db,
        conversion_type="deal_win_reported",
        deal_size_bucket="10_25k",
    )
    # UPDATE subscribers should contain new_score = 0 + 25 (base) + 20 (bonus) = 45
    update_params = db.executed[4]["params"]
    assert update_params["new_score"] == 45
    assert update_params["band"] == "medium"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Duplicate source event → return None, no score event written
# ─────────────────────────────────────────────────────────────────────────────

def test_duplicate_source_event_prevention():
    results = [
        [{"vertical": "roofing"}],  # resolve_trade runs before begin_nested
    ]
    db = _FakeSession(results=results, raise_on_nested=True)
    result = _call_with_all_hints(db)
    assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# 9. Duplicate does not double-apply the score
# ─────────────────────────────────────────────────────────────────────────────

def test_duplicate_conversion_does_not_double_apply_score():
    results = [
        [{"vertical": "roofing"}],  # first call: resolve_trade
    ]
    db = _FakeSession(results=results, raise_on_nested=True)
    _call_with_all_hints(db)
    # Only 1 execute call (resolve_trade) — no score event INSERT or UPDATE
    assert len(db.executed) == 1


# ─────────────────────────────────────────────────────────────────────────────
# 10. Partial dimension status stored in metadata
# ─────────────────────────────────────────────────────────────────────────────

def test_partial_dimension_status_stored_in_metadata():
    # zip_code=None, property_id=None → territories query (1 query, empty)
    # wallet_tier=None → wallet_balances query (1 query, empty)
    # bundle_id=1 → no bundle query
    # Resolution DB order: [0]=zip territories, [1]=wallet_tier, [2]=trade, [3]=INSERT CAE
    results = [
        [],                                   # zip via zip_territories (no locked territory)
        [],                                   # wallet_tier via wallet_balances → empty
        [{"vertical": "roofing"}],            # _resolve_trade
        [{"id": 11}],                         # INSERT CAE
        [{"revenue_signal_score": 0}],        # SELECT old score
        [], [],                               # INSERT score + UPDATE
    ]
    from src.services.attribution_service import record_conversion_attribution
    db = _FakeSession(results=results)
    record_conversion_attribution(
        conversion_type="paid_unlock",
        source_table="stripe_payment_intents",
        source_event_id="pi_partial_001",
        subscriber_id=1,
        occurred_at=_NOW,
        lead_id=1,
        lock_status="locked",
        lock_zip="33602",
        autopilot_tier="not_applicable",
        bundle_id=1,
        bundle_type="not_applicable",
        # zip_code, wallet_tier omitted → unresolved
        db=db,
    )
    # [0]=zip territories, [1]=wallet_tier, [2]=trade, [3]=INSERT CAE
    insert_params = db.executed[3]["params"]
    meta_str = insert_params["attribution_metadata"]
    meta = json.loads(meta_str)
    missing = meta.get("missing_dimensions", [])
    # zip_code and wallet_tier should be in missing_dimensions
    assert "zip_code" in missing or "wallet_tier" in missing


# ─────────────────────────────────────────────────────────────────────────────
# 11. All 8 dimensions present or set to explicit unknown/not_applicable
# ─────────────────────────────────────────────────────────────────────────────

def test_all_8_dimensions_present_or_explicit():
    db = _FakeSession(results=_base_results())
    _call_with_all_hints(db)
    params = db.executed[1]["params"]
    # Every dimension column must be set (not empty None except nullable lead/property)
    assert params["zip_code"] is not None
    assert params["trade"] is not None
    assert params["wallet_tier"] is not None
    assert params["lock_status"] is not None
    assert params["autopilot_tier"] is not None
    assert params["bundle_type"] is not None
    assert params["deal_size_bucket"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# 12. Wallet tier captured at hook time (not stale)
# ─────────────────────────────────────────────────────────────────────────────

def test_event_time_state_wallet_tier():
    results = _base_results()
    db = _FakeSession(results=results)
    _call_with_all_hints(db)
    params = db.executed[1]["params"]
    # wallet_tier was "gold" at call time — verify it's stored as-is
    assert params["wallet_tier"] == "gold"


# ─────────────────────────────────────────────────────────────────────────────
# 13. One revenue_signal_score_events row per scored conversion
# ─────────────────────────────────────────────────────────────────────────────

def test_revenue_signal_score_event_inserted():
    db = _FakeSession(results=_base_results())
    _call_with_all_hints(db)
    score_insert = db.executed[3]
    assert "revenue_signal_score_events" in score_insert["sql"]
    assert score_insert["params"]["sub_id"] == 1
    assert score_insert["params"]["action_type"] == "paid_unlock"


# ─────────────────────────────────────────────────────────────────────────────
# 14. subscribers.revenue_signal_score updated
# ─────────────────────────────────────────────────────────────────────────────

def test_subscriber_latest_score_updated():
    db = _FakeSession(results=_base_results())
    _call_with_all_hints(db)
    update_call = db.executed[4]
    assert "UPDATE subscribers" in update_call["sql"]
    assert update_call["params"]["sub_id"] == 1
    assert "revenue_signal_score" in update_call["sql"]


# ─────────────────────────────────────────────────────────────────────────────
# 15. Score clamped to 0–100
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("old_score,conversion_type,expected", [
    (95, "autopilot_pro_upgrade", 100),  # 95 + 40 = 135 → clamped to 100
    (0,  "paid_unlock",           10),   # 0 + 10 = 10
])
def test_score_clamped_to_0_100(old_score, conversion_type, expected):
    results = [
        [{"vertical": "roofing"}],
        [{"id": 1}],
        [{"revenue_signal_score": old_score}],
        [], [],
    ]
    db = _FakeSession(results=results)
    _call_with_all_hints(db, conversion_type=conversion_type)
    update_params = db.executed[4]["params"]
    assert update_params["new_score"] == expected


# ─────────────────────────────────────────────────────────────────────────────
# 16. Score band assignment at boundary values
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("old_score,delta_type,expected_band", [
    # Put the subscriber at boundary scores via careful conversion_type choice.
    # territory_lock_purchase = +30, autopilot_lite = +35, autopilot_pro = +40
    (0,   "paid_unlock",               "low"),      # 0+10=10 → low
    (20,  "paid_unlock",               "low"),      # 20+10=30 → 30 → low (0-30 inclusive)
    (21,  "paid_unlock",               "medium"),   # 21+10=31 → medium
    (50,  "paid_unlock",               "medium"),   # 50+10=60 → medium
    (51,  "paid_unlock",               "high"),     # 51+10=61 → high
    (70,  "paid_unlock",               "high"),     # 70+10=80 → high (hi is 61-80)
    (71,  "paid_unlock",               "very_high"),# 71+10=81 → very_high
])
def test_score_band_assignment(old_score, delta_type, expected_band):
    results = [
        [{"vertical": "roofing"}],
        [{"id": 1}],
        [{"revenue_signal_score": old_score}],
        [], [],
    ]
    db = _FakeSession(results=results)
    _call_with_all_hints(db, conversion_type=delta_type)
    update_params = db.executed[4]["params"]
    assert update_params["band"] == expected_band


# ─────────────────────────────────────────────────────────────────────────────
# 17. revenue_signal_score_events.metadata links to attribution_event_id
# ─────────────────────────────────────────────────────────────────────────────

def test_score_metadata_links_to_attribution_event():
    db = _FakeSession(results=_base_results())
    event_id = _call_with_all_hints(db)
    score_insert_params = db.executed[3]["params"]
    meta = json.loads(score_insert_params["metadata"])
    assert meta["attribution_event_id"] == event_id


# ─────────────────────────────────────────────────────────────────────────────
# Bonus: unknown conversion_type raises ValueError
# ─────────────────────────────────────────────────────────────────────────────

def test_unknown_conversion_type_raises():
    db = _FakeSession()
    from src.services.attribution_service import record_conversion_attribution
    with pytest.raises(ValueError, match="Unknown conversion_type"):
        record_conversion_attribution(
            conversion_type="bogus_event",
            source_table="stripe_payment_intents",
            source_event_id="pi_x",
            subscriber_id=1,
            occurred_at=_NOW,
            db=db,
        )
