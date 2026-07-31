"""
Verifies the QUALITY-v2.2 Q3 fix: get_ranked_whales() must surface
confidence_score, since nothing downstream could apply
gating.is_citable() to Hunter's real data without it (verified gap,
see hunter_to_cora.py's module docstring).
"""
from src.services.whale_ranking import get_ranked_whales


def test_get_ranked_whales_includes_confidence_score(fresh_db):
    # Uses whatever whale rows already exist in the real (rolled-back) DB —
    # this is a read-only contract-shape test, not a data-seeding test.
    rows = get_ranked_whales(fresh_db, limit=5)
    if not rows:
        import pytest
        pytest.skip("no whale rows in this DB snapshot to assert the shape against")
    assert "confidence_score" in rows[0]
    assert isinstance(rows[0]["confidence_score"], int)
