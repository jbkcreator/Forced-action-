"""
E2E tests for Phase 3 A3: Warm-Start Priors & Heuristics Tuning.

Hits the live server at http://localhost:8001 with real DB state.
No Claude API calls are made — zero inference cost.

Run with:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest \
        tests/scenarios/test_e2e_heuristics.py -v -m scenario
"""
from __future__ import annotations

import time

import pytest
import requests
from sqlalchemy import text as sa_text

from config.scoring import VERTICAL_WEIGHTS
from config.settings import settings
from src.core.database import get_db_context

BASE = "http://localhost:8001"

# Shared state between ordered tests
_s: dict = {}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def admin_headers():
    r = requests.post(
        f"{BASE}/api/admin/login",
        json={
            "username": settings.admin_username,
            "password": settings.admin_password.get_secret_value(),
        },
        timeout=10,
    )
    assert r.status_code == 200, f"Admin login failed: {r.text}"
    token = r.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(scope="module")
def probate_property_id():
    """Return a Gold+ property_id whose distress_types includes 'probate'."""
    with get_db_context() as db:
        pid = db.execute(
            sa_text("""
                SELECT ds.property_id
                FROM distress_scores ds
                WHERE ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                  AND ds.distress_types @> '["probate"]'::jsonb
                ORDER BY ds.score_date DESC
                LIMIT 1
            """)
        ).scalar_one_or_none()
    return pid


# ---------------------------------------------------------------------------
# T1 — Seed and verify overrides
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t1_seed_and_verify(admin_headers):
    """POST seed → 8 rows; GET overrides → correct base/delta/effective for each."""
    r = requests.post(
        f"{BASE}/api/admin/scoring/heuristics/seed",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["seeded_rows"] == 8, f"Expected 8 rows, got {body['seeded_rows']}"

    r = requests.get(
        f"{BASE}/api/admin/scoring/weight-overrides",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 8, f"Expected 8 overrides, got {body['total']}"

    items = body["items"]

    # Every row must have a non-null base_weight and effective_weight
    for item in items:
        assert item["base_weight"] is not None, f"Missing base_weight for {item['vertical']}/{item['signal_type']}"
        assert item["effective_weight"] is not None, f"Missing effective_weight for {item['vertical']}/{item['signal_type']}"

    # Spot-check probate/wholesalers: base=70, delta=+5, effective=75
    probate_item = next(
        (i for i in items if i["vertical"] == "wholesalers" and i["signal_type"] == "probate"),
        None,
    )
    assert probate_item is not None, "probate/wholesalers override not found"
    assert probate_item["base_weight"] == VERTICAL_WEIGHTS["wholesalers"]["probate"]
    assert probate_item["delta"] == 5.0
    assert probate_item["effective_weight"] == probate_item["base_weight"] + 5.0

    # Spot-check fix_flip/code_violations: delta=-5, effective = base - 5
    cv_item = next(
        (i for i in items if i["vertical"] == "fix_flip" and i["signal_type"] == "code_violations"),
        None,
    )
    assert cv_item is not None, "code_violations/fix_flip override not found"
    assert cv_item["delta"] == -5.0
    assert cv_item["effective_weight"] == cv_item["base_weight"] - 5.0

    _s["seed_items"] = items


# ---------------------------------------------------------------------------
# T2 — Filter by vertical
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t2_filter_by_vertical(admin_headers):
    """GET weight-overrides?vertical=wholesalers returns exactly 3 rows."""
    r = requests.get(
        f"{BASE}/api/admin/scoring/weight-overrides",
        params={"vertical": "wholesalers"},
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # heuristics.json wholesalers: probate, judgment_liens, deed_transfers
    assert body["total"] == 3, f"Expected 3 wholesalers overrides, got {body['total']}"
    for item in body["items"]:
        assert item["vertical"] == "wholesalers"


# ---------------------------------------------------------------------------
# T3 — Delta applied in CDS computation
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t3_delta_applied_in_scoring(probate_property_id):
    """In-process scoring: wholesaler/probate base == VERTICAL_WEIGHTS base + 5."""
    if probate_property_id is None:
        pytest.skip("No Gold+ property with probate signal found in DB")

    from src.core.models import Property
    from src.services.cds_engine import MultiVerticalScorer
    from src.services.heuristic_loader import invalidate_cache

    # Force a fresh cache load so the seeded delta (+5) is guaranteed visible
    invalidate_cache()

    with get_db_context() as db:
        prop = db.get(Property, probate_property_id)
        assert prop is not None, f"Property {probate_property_id} not found"
        scorer = MultiVerticalScorer(db)
        result = scorer.score_property(prop)

    factor = result.get("factor_scores", {})
    wholesalers = factor.get("vertical_breakdown", {}).get("wholesalers", {})
    probate_component = wholesalers.get("signals", {}).get("probate")

    if probate_component is None:
        pytest.skip(f"Property {probate_property_id} has probate in distress_types but it was filtered out of wholesalers scoring (stacking-only or missing)")

    raw_base = VERTICAL_WEIGHTS["wholesalers"]["probate"]
    expected_base = min(100, max(0, raw_base + 5))

    assert probate_component["base"] == expected_base, (
        f"Expected base={expected_base} (raw {raw_base} + delta 5), "
        f"got {probate_component['base']} — delta not applied in scoring"
    )


# ---------------------------------------------------------------------------
# T4 — Cache hit (no redundant DB queries)
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t4_cache_hit():
    """Second load_overrides() call within TTL returns instantly from cache."""
    from src.services.heuristic_loader import invalidate_cache, load_overrides

    invalidate_cache()

    with get_db_context() as db:
        first = load_overrides(db)
        t0 = time.monotonic()
        second = load_overrides(db)
        elapsed_ms = (time.monotonic() - t0) * 1000

    assert first == second, "Cache returned different data on second call"
    assert elapsed_ms < 5, f"Cache hit took {elapsed_ms:.2f}ms — expected < 5ms (no DB call)"


# ---------------------------------------------------------------------------
# T5 — Tuner dry-run leaves DB unchanged
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t5_tuner_dry_run(admin_headers):
    """POST run-tuner?dry_run=true → no feedback rows written to DB."""
    r = requests.post(
        f"{BASE}/api/admin/scoring/heuristics/run-tuner",
        params={"dry_run": "true"},
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dry_run"] is True
    assert "rows_evaluated" in body
    assert "rows_updated" in body

    with get_db_context() as db:
        count = db.execute(
            sa_text("""
                SELECT count(*) FROM scoring_weight_overrides
                WHERE source IN ('loss_feedback', 'win_feedback')
            """)
        ).scalar()

    assert count == 0, f"dry_run wrote {count} feedback rows — expected 0"


# ---------------------------------------------------------------------------
# T6 — Tuner live run respects min_sample_size
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t6_tuner_live_run(admin_headers):
    """POST run-tuner → DB consistent with rows_updated; no spurious writes."""
    r = requests.post(
        f"{BASE}/api/admin/scoring/heuristics/run-tuner",
        headers=admin_headers,
        timeout=30,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dry_run"] is False
    rows_updated = body["rows_updated"]

    with get_db_context() as db:
        count = db.execute(
            sa_text("""
                SELECT count(*) FROM scoring_weight_overrides
                WHERE source IN ('loss_feedback', 'win_feedback')
            """)
        ).scalar()

    if rows_updated == 0:
        assert count == 0, f"rows_updated=0 but {count} feedback rows exist — spurious write"
    else:
        assert count == rows_updated, (
            f"rows_updated={rows_updated} but found {count} feedback rows in DB"
        )

    _s["feedback_count"] = rows_updated


# ---------------------------------------------------------------------------
# T7 — Reset removes feedback rows, seed rows intact
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t7_reset(admin_headers):
    """POST reset → feedback rows deleted; 8 seed rows remain."""
    r = requests.post(
        f"{BASE}/api/admin/scoring/heuristics/reset",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["deleted_rows"] == _s["feedback_count"], (
        f"Expected to delete {_s['feedback_count']} rows, deleted {body['deleted_rows']}"
    )

    with get_db_context() as db:
        seed_count = db.execute(
            sa_text("SELECT count(*) FROM scoring_weight_overrides WHERE source = 'seed'")
        ).scalar()
        feedback_count = db.execute(
            sa_text("""
                SELECT count(*) FROM scoring_weight_overrides
                WHERE source IN ('loss_feedback', 'win_feedback')
            """)
        ).scalar()

    assert seed_count == 8, f"Expected 8 seed rows after reset, got {seed_count}"
    assert feedback_count == 0, f"Expected 0 feedback rows after reset, got {feedback_count}"


# ---------------------------------------------------------------------------
# T8 — Re-seed restores full state
# ---------------------------------------------------------------------------


@pytest.mark.scenario
def test_t8_reseed_idempotent(admin_headers):
    """POST seed after reset → 8 rows restored; probate/wholesalers effective=75."""
    r = requests.post(
        f"{BASE}/api/admin/scoring/heuristics/seed",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    assert r.json()["seeded_rows"] == 8

    r = requests.get(
        f"{BASE}/api/admin/scoring/weight-overrides",
        headers=admin_headers,
        timeout=10,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 8

    probate_item = next(
        (i for i in body["items"] if i["vertical"] == "wholesalers" and i["signal_type"] == "probate"),
        None,
    )
    assert probate_item is not None
    assert probate_item["delta"] == 5.0
    assert probate_item["effective_weight"] == probate_item["base_weight"] + 5.0
