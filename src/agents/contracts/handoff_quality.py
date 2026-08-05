"""
Handoff quality ratings (spec §1.4, folded into QUALITY-v2.2 Q3 per decision
D3): "Cora rates Hunter's enrichment 1-5; Vera rates findings packets."

Read literally: Cora rates the Hunter->Cora boundary (she's the receiver).
"Vera rates findings packets" is read as Vera rating the Dev->Vera boundary
-- the only boundary where Vera receives rather than sends (she sends
findings to Dev via the Vera->Dev boundary; Dev sends closure packets back
to her via Dev->Vera). Hence the two allowed boundary values on the
handoff_quality_ratings CHECK constraint (Task 1's migration).
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

_RATABLE_BOUNDARIES = frozenset({"hunter_to_cora", "dev_to_vera"})


def rate_handoff(
    session: Session,
    *,
    boundary: str,
    rater_seat: str,
    ratee_seat: str,
    reference_id: str,
    score: int,
    notes: Optional[str] = None,
) -> int:
    if boundary not in _RATABLE_BOUNDARIES:
        raise ValueError(f"rate_handoff: boundary={boundary!r} is not ratable (allowed: {sorted(_RATABLE_BOUNDARIES)})")
    if not (1 <= score <= 5):
        raise ValueError(f"rate_handoff: score={score!r} must be between 1 and 5")

    row = session.execute(
        sa_text("""
            INSERT INTO handoff_quality_ratings
                (boundary, rater_seat, ratee_seat, reference_id, score, notes)
            VALUES
                (:boundary, :rater_seat, :ratee_seat, :reference_id, :score, :notes)
            ON CONFLICT (boundary, reference_id) DO NOTHING
            RETURNING id
        """),
        {
            "boundary": boundary, "rater_seat": rater_seat, "ratee_seat": ratee_seat,
            "reference_id": reference_id, "score": score, "notes": notes,
        },
    ).fetchone()
    return row.id if row else -1


def get_average_rating(
    session: Session, *, boundary: str, ratee_seat: str, since=None,
) -> Optional[float]:
    """Average score for a ratee within a boundary. `since` (a datetime),
    when given, filters to ratings on/after that time -- for a monthly
    scorecard read, pass the start of the reporting month."""
    where_since = "AND rated_at >= :since" if since is not None else ""
    row = session.execute(
        sa_text(f"""
            SELECT AVG(score)::float AS avg_score
            FROM handoff_quality_ratings
            WHERE boundary = :boundary AND ratee_seat = :ratee_seat
            {where_since}
        """),
        {"boundary": boundary, "ratee_seat": ratee_seat, **({"since": since} if since is not None else {})},
    ).fetchone()
    return row.avg_score if row and row.avg_score is not None else None
