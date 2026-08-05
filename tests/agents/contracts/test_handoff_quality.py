import pytest
from sqlalchemy import text

from src.agents.contracts.handoff_quality import get_average_rating, rate_handoff


def test_rate_handoff_writes_a_row(fresh_db):
    rating_id = rate_handoff(
        fresh_db, boundary="hunter_to_cora", rater_seat="cora", ratee_seat="hunter",
        reference_id="OPP-2026-00042", score=4, notes="Good why_now, contact confidence was low",
    )
    fresh_db.commit()

    row = fresh_db.execute(
        text("SELECT boundary, rater_seat, ratee_seat, score FROM handoff_quality_ratings WHERE id = :id"),
        {"id": rating_id},
    ).mappings().first()
    assert row["boundary"] == "hunter_to_cora"
    assert row["rater_seat"] == "cora"
    assert row["ratee_seat"] == "hunter"
    assert row["score"] == 4


def test_rate_handoff_rejects_score_out_of_range(fresh_db):
    with pytest.raises(ValueError):
        rate_handoff(
            fresh_db, boundary="hunter_to_cora", rater_seat="cora", ratee_seat="hunter",
            reference_id="OPP-2026-00042", score=6,
        )


def test_rate_handoff_rejects_unknown_boundary(fresh_db):
    with pytest.raises(ValueError):
        rate_handoff(
            fresh_db, boundary="cora_to_relay", rater_seat="cora", ratee_seat="relay",
            reference_id="idem-1", score=3,
        )


def test_get_average_rating(fresh_db):
    for score in (5, 3, 4):
        rate_handoff(
            fresh_db, boundary="hunter_to_cora", rater_seat="cora", ratee_seat="hunter",
            reference_id=f"OPP-2026-{score:05d}", score=score,
        )
    fresh_db.commit()

    avg = get_average_rating(fresh_db, boundary="hunter_to_cora", ratee_seat="hunter")
    assert avg == pytest.approx(4.0, abs=0.34)  # 5+3+4 / 3, tolerant of other rows in a shared DB


def test_get_average_rating_none_when_no_ratings(fresh_db):
    avg = get_average_rating(fresh_db, boundary="dev_to_vera", ratee_seat="dev_shop_never_rated_xyz")
    assert avg is None


def test_rate_handoff_idempotent_on_duplicate_reference(fresh_db):
    # First write succeeds and returns a real id.
    id1 = rate_handoff(
        fresh_db, boundary="hunter_to_cora", rater_seat="cora", ratee_seat="hunter",
        reference_id="OPP-2026-99999", score=3,
    )
    fresh_db.commit()
    assert id1 != -1

    # Retry for same (boundary, reference_id) is silently skipped — no duplicate row.
    id2 = rate_handoff(
        fresh_db, boundary="hunter_to_cora", rater_seat="cora", ratee_seat="hunter",
        reference_id="OPP-2026-99999", score=5,
    )
    fresh_db.commit()
    assert id2 == -1

    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM handoff_quality_ratings WHERE reference_id = 'OPP-2026-99999'"),
    ).scalar()
    assert count == 1
