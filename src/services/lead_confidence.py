"""A2 — Lead Confidence computation (pure, no DB).

Given the signal records that contribute to a property's distress score, compute
a 0-1 Lead Confidence and decide whether the lead is a Guess Lead.

Lead Confidence combines two factors and deliberately EXCLUDES staleness (already
handled in CDS scoring by age_decay / signal_coverage_pct — see CONTEXT.md):

  1. match_component       — how well the strongest contributing record matched
                             the parcel (reuses config/matching.py bands).
  2. corroboration_component — how many distinct signals corroborate within the
                             stacking window (thin file vs solid file).

The DB read that builds the SignalRecord list lives separately so this function
stays trivially unit-testable against hand-built fixtures.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.confidence import (
    CORROBORATION_CURVE,
    CORROBORATION_MAX,
    MIN_CONFIDENCE_THRESHOLD,
    W_CORR,
    W_MATCH,
)
from config.matching import MatchingThresholds, THRESHOLDS, for_county
from config.scoring import STACKING_WINDOW_DAYS

logger = logging.getLogger(__name__)

# Maps a CDS signal type to the spoke table that stores its per-record
# match_confidence. Spokes WITHOUT a stored confidence (tax_delinquencies,
# building_permits, incidents) map to None -> treated as a clean match (1.0),
# since a persisted record already cleared the loader's match gate.
_SIGNAL_TYPE_TO_SPOKE = {
    "foreclosures": "foreclosures",
    "code_violations": "code_violations",
    "judgment_liens": "legal_and_liens",
    "code_lien": "legal_and_liens",
    "hoa_liens": "legal_and_liens",
    "mechanics_liens": "legal_and_liens",
    "irs_tax_liens": "legal_and_liens",
    "deed_transfers": "deeds",
    "probate": "legal_proceedings",
    "evictions": "legal_proceedings",
    "bankruptcy": "legal_proceedings",
    "divorce_filings": "legal_proceedings",
}


@dataclass(frozen=True)
class SignalRecord:
    """One contributing distress signal for a property.

    match_confidence is None when the source spoke table stores no per-record
    match confidence (tax_delinquencies, building_permits, incidents). Absence
    means the record passed the loader's match gate to be persisted at all, so
    it is treated as a clean match (1.0), not as low confidence.
    """

    signal_type: str
    signal_date: date | None
    match_confidence: float | None


@dataclass(frozen=True)
class ConfidenceResult:
    lead_confidence: float
    is_guess_lead: bool


def _match_component(mc: float | None, thresholds: MatchingThresholds) -> float:
    if mc is None or mc >= thresholds.auto_match:
        return 1.0
    if mc >= thresholds.review_min:
        span = thresholds.auto_match - thresholds.review_min
        return (mc - thresholds.review_min) / span
    return 0.0


def _corroboration_component(n: int) -> float:
    if n >= 4:
        return CORROBORATION_MAX
    return CORROBORATION_CURVE.get(n, 0.0)


def compute_lead_confidence(
    signals: Sequence[SignalRecord],
    *,
    as_of: date,
    thresholds: MatchingThresholds = THRESHOLDS,
) -> ConfidenceResult:
    in_window = [
        s
        for s in signals
        if s.signal_date is None
        or (as_of - s.signal_date).days <= STACKING_WINDOW_DAYS
    ]

    best_match = max(
        (_match_component(s.match_confidence, thresholds) for s in in_window),
        default=0.0,
    )
    corroborating = {
        s.signal_type
        for s in in_window
        if s.match_confidence is None or s.match_confidence >= thresholds.review_min
    }
    n_distinct = len(corroborating)

    lead_confidence = (
        W_MATCH * best_match + W_CORR * _corroboration_component(n_distinct)
    )
    return ConfidenceResult(
        lead_confidence=lead_confidence,
        is_guess_lead=lead_confidence < MIN_CONFIDENCE_THRESHOLD,
    )


def _best_match_confidence(records) -> Optional[float]:
    values = [
        float(r.match_confidence)
        for r in (records or [])
        if r.match_confidence is not None
    ]
    return max(values) if values else None


def build_signal_records(prop, session: Session, scorer=None) -> list[SignalRecord]:
    """Read-only: derive the SignalRecords contributing to a property's score.

    Reuses the CDS engine's _collect_signals so A2's notion of "which signals"
    stays identical to what actually scored the lead (resolved-violation skips,
    eviction-direction rules, nominal-deed filtering, hard cutoff), then attaches
    the best per-spoke match_confidence. Does NOT mutate scoring state.

    Pass a shared `scorer` when looping (run_lead_confidence_pass) so the engine
    is built once, not per property.
    """
    if scorer is None:
        from src.services.cds_engine import MultiVerticalScorer
        scorer = MultiVerticalScorer(session)

    raw = scorer._collect_signals(prop)

    mc_by_spoke = {
        "foreclosures": _best_match_confidence(prop.foreclosures),
        "code_violations": _best_match_confidence(prop.code_violations),
        "legal_and_liens": _best_match_confidence(prop.legal_and_liens),
        "legal_proceedings": _best_match_confidence(prop.legal_proceedings),
        "deeds": _best_match_confidence(prop.deeds),
    }

    records: list[SignalRecord] = []
    for s in raw:
        spoke = _SIGNAL_TYPE_TO_SPOKE.get(s["type"])
        match_conf = mc_by_spoke.get(spoke) if spoke else None
        d = s["date"]
        if isinstance(d, datetime):
            d = d.date()
        records.append(SignalRecord(s["type"], d, match_conf))
    return records


def evaluate_lead_confidence(prop, session: Session, *, as_of: Optional[date] = None, scorer=None) -> ConfidenceResult:
    """Compute Lead Confidence for a property, persist it onto the latest
    distress_scores row, and flag a guess lead for direct mail when a mailing
    address exists. Read-only on the CDS score itself — only the two A2 columns
    are written. Returns the result for logging/inspection.
    """
    as_of = as_of or date.today()
    thresholds = for_county(getattr(prop, "county_id", None))

    records = build_signal_records(prop, session, scorer=scorer)
    result = compute_lead_confidence(records, as_of=as_of, thresholds=thresholds)

    session.execute(
        text(
            """
            UPDATE distress_scores
               SET lead_confidence = :lc,
                   is_guess_lead   = :guess
             WHERE id = (
                   SELECT id FROM distress_scores
                    WHERE property_id = :pid
                    ORDER BY score_date DESC
                    LIMIT 1
             )
            """
        ),
        {
            "lc": round(result.lead_confidence, 3),
            "guess": result.is_guess_lead,
            "pid": prop.id,
        },
    )

    if result.is_guess_lead:
        from src.services.direct_mail import flag_direct_mail_eligible

        flag_direct_mail_eligible(prop.id, session)

    return result


def run_lead_confidence_pass(
    session: Session,
    *,
    scoring_run_id: Optional[int] = None,
    as_of: Optional[date] = None,
) -> int:
    """Flag Guess Leads across a scoring run (or today's rows when no run id).

    Runs as its own step after the CDS engine (cron stagger: CDS -> this ->
    skip-trace), so the hot scoring loop is left untouched. Per-property failures
    are logged and skipped — a bad lead never aborts the pass. Returns the count
    of properties evaluated.
    """
    from sqlalchemy.orm import selectinload

    from src.core.models import Property
    from src.services.cds_engine import MultiVerticalScorer

    as_of = as_of or date.today()
    if scoring_run_id is not None:
        rows = session.execute(
            text("SELECT DISTINCT property_id FROM distress_scores WHERE scoring_run_id = :rid"),
            {"rid": scoring_run_id},
        )
    else:
        rows = session.execute(
            text(
                "SELECT DISTINCT property_id FROM distress_scores "
                "WHERE score_date >= :start"
            ),
            {"start": datetime.combine(as_of, datetime.min.time())},
        )
    property_ids = [r[0] for r in rows]

    # ponytail: build the scoring engine once (was per-property) and commit per
    # chunk so a crash mid-run keeps prior progress and locks aren't held for the
    # whole 80k-property pass. Bump CHUNK if commit overhead dominates.
    CHUNK = 500
    scorer = MultiVerticalScorer(session)
    evaluated = 0
    for i in range(0, len(property_ids), CHUNK):
        chunk = property_ids[i : i + CHUNK]
        props = (
            session.query(Property)
            .filter(Property.id.in_(chunk))
            .options(
                selectinload(Property.owner),  # _collect_signals eviction-direction check
                selectinload(Property.foreclosures),
                selectinload(Property.code_violations),
                selectinload(Property.legal_and_liens),
                selectinload(Property.legal_proceedings),
                selectinload(Property.deeds),
                selectinload(Property.tax_delinquencies),
                selectinload(Property.building_permits),
            )
            .all()
        )
        for prop in props:
            try:
                evaluate_lead_confidence(prop, session, as_of=as_of, scorer=scorer)
                evaluated += 1
            except Exception:
                logger.error("Lead Confidence eval failed for property_id=%s", prop.id, exc_info=True)
        session.commit()

    logger.info("Lead Confidence pass complete: %d properties evaluated", evaluated)
    return evaluated


def main() -> int:
    import argparse

    from src.core.database import get_db_context

    parser = argparse.ArgumentParser(description="A2 Lead Confidence pass")
    parser.add_argument("--scoring-run-id", type=int, default=None)
    args = parser.parse_args()

    with get_db_context() as session:
        run_lead_confidence_pass(session, scoring_run_id=args.scoring_run_id)
        session.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
