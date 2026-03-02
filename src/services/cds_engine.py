"""
CDS Multi-Vertical Scoring Engine

Scores properties across 6 buyer verticals (Wholesalers, Fix & Flip, Restoration,
Roofing, Public Adjusters, Attorneys) using 14 real-time signal sources.

All weights, thresholds, and routing rules live in config/scoring.py.
To retune weights: edit config/scoring.py and run:
    python -m src.services.cds_engine --rescore-all
No code changes required.

DATABASE RELATIONSHIP:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DistressScore has a 1:Many relationship with Property:
  • One property can have multiple DistressScore records (historical tracking)
  • UPSERT logic: only ONE score per property per day
  • If score unchanged from last record → skip (no identical rows accumulate)
  • New day with changed score → new record

SCORING ALGORITHM (per vertical):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. primary_score  = base_weight[best_signal] + recency_bonus(best_signal_date)
2. stacking_bonus = min((signals_within_60_days - 1) * 20, 40)
3. absentee_bonus: Out-of-State +15, Out-of-County +8
4. contact_bonus: verified phone +15, verified email +10
5. equity_bonus: equity_pct > 50% → +20, 30-50% → +10 (wholesalers + fix_flip only)
6. vertical_score = min(100, primary_score + stacking_bonus + bonuses)

final_cds_score = max across all 6 verticals

SIGNAL SOURCES (14 total):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Model           → Signal key
CodeViolation   → code_violations
LegalAndLien    → judgment_liens, tampa_code_liens, county_code_liens,
                  hoa_liens, mechanics_liens, irs_tax_liens
Deed            → deed_transfers
LegalProceeding → probate, evictions, bankruptcy
TaxDelinquency  → tax_delinquencies
Foreclosure     → foreclosures
BuildingPermit  → building_permits
"""

import logging
import sys
from datetime import datetime, date
from typing import Dict, List, Optional

from sqlalchemy.orm import Session, joinedload

from src.core.models import (
    BuildingPermit,
    CodeViolation,
    Deed,
    DistressScore,
    Foreclosure,
    LegalAndLien,
    LegalProceeding,
    Owner,
    Financial,
    Property,
    TaxDelinquency,
)
from config.scoring import (
    ABSENTEE_BONUS,
    CONTACT_EMAIL_BONUS,
    CONTACT_PHONE_BONUS,
    EQUITY_BONUS_HIGH,
    EQUITY_BONUS_MID,
    EQUITY_HIGH_THRESH,
    EQUITY_MID_THRESH,
    EQUITY_VERTICALS,
    LEAD_TIER_THRESHOLDS,
    RECENCY_BONUSES,
    ROUTING_THRESHOLDS,
    SCORE_CAP,
    STACKING_BONUS_CAP,
    STACKING_BONUS_PER_SIGNAL,
    STACKING_WINDOW_DAYS,
    VERTICAL_WEIGHTS,
)

logger = logging.getLogger(__name__)

# ── LegalAndLien.document_type → signal key ───────────────────────────────────
_DOCUMENT_TYPE_TO_SIGNAL: Dict[str, str] = {
    "TAMPA CODE LIENS (TCL)":  "tampa_code_liens",
    "COUNTY CODE LIENS (CCL)": "county_code_liens",
    "HOA LIENS (HL)":          "hoa_liens",
    "MECHANICS LIENS (ML)":    "mechanics_liens",
    "TAX LIENS (TL)":          "irs_tax_liens",
}

# ── LegalProceeding.record_type → signal key ─────────────────────────────────
_PROCEEDING_TYPE_TO_SIGNAL: Dict[str, str] = {
    "Probate":    "probate",
    "Eviction":   "evictions",
    "Bankruptcy": "bankruptcy",
}


class MultiVerticalScorer:
    """
    6-vertical CDS scoring engine.

    Scores properties at ingestion time using all 14 real-time signal sources.
    Weights are driven entirely by config/scoring.py — no code rebuild required
    when tuning.
    """

    def __init__(self, session: Session):
        self.session = session

    # ── Signal collection ─────────────────────────────────────────────────────

    def _collect_signals(self, prop: Property) -> List[Dict]:
        """
        Gather all distress signals for a property from all 14 sources.

        Returns a flat list of:
            {"type": str, "date": date|None, "amount": float|None}
        """
        signals: List[Dict] = []

        # 1. Code violations
        for v in (prop.code_violations or []):
            signals.append({"type": "code_violations", "date": v.opened_date, "amount": None})

        # 2. Legal and Liens (liens + judgments)
        for lien in (prop.legal_and_liens or []):
            if lien.record_type == "Judgment":
                sig_type = "judgment_liens"
            else:
                sig_type = _DOCUMENT_TYPE_TO_SIGNAL.get(lien.document_type or "")
                if not sig_type:
                    logger.debug(f"Unknown LegalAndLien document_type: '{lien.document_type}' — skipping")
                    continue
            signals.append({"type": sig_type, "date": lien.filing_date, "amount": lien.amount})

        # 3. Deed transfers
        for deed in (prop.deeds or []):
            signals.append({"type": "deed_transfers", "date": deed.record_date, "amount": deed.sale_price})

        # 4. Legal proceedings (probate / eviction / bankruptcy)
        for proc in (prop.legal_proceedings or []):
            sig_type = _PROCEEDING_TYPE_TO_SIGNAL.get(proc.record_type)
            if not sig_type:
                continue
            signals.append({"type": sig_type, "date": proc.filing_date, "amount": proc.amount})

        # 5. Tax delinquencies
        for tax in (prop.tax_delinquencies or []):
            signals.append({"type": "tax_delinquencies", "date": tax.deed_app_date, "amount": tax.total_amount_due})

        # 6. Foreclosures
        for fc in (prop.foreclosures or []):
            signals.append({"type": "foreclosures", "date": fc.filing_date, "amount": fc.judgment_amount})

        # 7. Building permits
        for bp in (prop.building_permits or []):
            signals.append({"type": "building_permits", "date": bp.issue_date, "amount": None})

        return signals

    # ── Recency bonus ─────────────────────────────────────────────────────────

    def _recency_bonus(self, sig_date) -> int:
        """Return the recency bonus for a signal date (0 if no date)."""
        if not sig_date:
            return 0
        if isinstance(sig_date, datetime):
            sig_date = sig_date.date()
        days_old = (date.today() - sig_date).days
        for max_days, bonus in RECENCY_BONUSES:
            if max_days is None or days_old <= max_days:
                return bonus
        return 0

    # ── Per-vertical scorer ────────────────────────────────────────────────────

    def _score_vertical(
        self,
        vertical: str,
        signals: List[Dict],
        owner: Optional[Owner],
        financial: Optional[Financial],
    ) -> Dict:
        """
        Score a single vertical per spec. Returns a result dict.

        Formula:
          primary_score  = base_weight[best_signal] + recency_bonus(best_signal_date)
          stacking_bonus = min((signals_within_60_days - 1) * 20, 40)
          final_score    = min(100, primary_score + stacking_bonus + absentee + contact + equity)

        Equity bonus applies to wholesalers and fix_flip only.
        """
        weights = VERTICAL_WEIGHTS[vertical]
        today   = date.today()

        # Group by signal type — each type counts once, using its most recent date.
        latest_by_type: Dict[str, object] = {}
        for sig in signals:
            sig_type = sig["type"]
            if sig_type not in weights:
                continue
            d = sig["date"]
            if isinstance(d, datetime):
                d = d.date()
            existing = latest_by_type.get(sig_type)
            if existing is None or (d and (existing is None or d > existing)):
                latest_by_type[sig_type] = d

        if not latest_by_type:
            return {
                "score":             0.0,
                "primary_signal":    None,
                "primary_score":     0.0,
                "stacking_bonus":    0,
                "signals_within_60": 0,
                "signals":           {},
                "absentee_bonus":    0,
                "contact_bonus":     0,
                "equity_bonus":      0,
            }

        # Build per-signal components and find the primary (highest base + recency)
        signal_components: Dict[str, Dict] = {}
        best_type  = None
        best_total = -1
        for sig_type, sig_date in latest_by_type.items():
            base    = weights[sig_type]
            recency = self._recency_bonus(sig_date)
            total   = base + recency
            signal_components[sig_type] = {
                "base":    base,
                "recency": recency,
                "total":   total,
            }
            if total > best_total:
                best_total = total
                best_type  = sig_type

        primary_score = float(best_total)

        # Stacking: count distinct signal types with a date within 60 days
        signals_within_60 = sum(
            1 for sig_date in latest_by_type.values()
            if sig_date and (today - sig_date).days <= STACKING_WINDOW_DAYS
        )
        stacking_bonus = min(
            max(0, signals_within_60 - 1) * STACKING_BONUS_PER_SIGNAL,
            STACKING_BONUS_CAP,
        )

        # Universal bonuses
        absentee_bonus = 0
        if owner and owner.absentee_status:
            absentee_bonus = ABSENTEE_BONUS.get(owner.absentee_status, 0)

        contact_bonus = 0
        if owner:
            if owner.phone_1 or owner.phone_2 or owner.phone_3:
                contact_bonus += CONTACT_PHONE_BONUS
            if owner.email_1 or owner.email_2:
                contact_bonus += CONTACT_EMAIL_BONUS

        # Equity bonus — wholesalers and fix_flip ONLY per spec
        equity_bonus = 0
        if vertical in EQUITY_VERTICALS and financial and financial.equity_pct is not None:
            eq = float(financial.equity_pct)
            if eq > EQUITY_HIGH_THRESH:
                equity_bonus = EQUITY_BONUS_HIGH
            elif eq > EQUITY_MID_THRESH:
                equity_bonus = EQUITY_BONUS_MID

        final_score = min(
            primary_score + stacking_bonus + absentee_bonus + contact_bonus + equity_bonus,
            float(SCORE_CAP),
        )

        logger.debug(
            f"    [{vertical}] primary={best_type}({primary_score:.0f})"
            f" stack=+{stacking_bonus}({signals_within_60} signals/60d)"
            f" absentee=+{absentee_bonus} contact=+{contact_bonus} equity=+{equity_bonus}"
            f" → {final_score:.1f}"
        )

        return {
            "score":             final_score,
            "primary_signal":    best_type,
            "primary_score":     primary_score,
            "stacking_bonus":    stacking_bonus,
            "signals_within_60": signals_within_60,
            "signals":           signal_components,
            "absentee_bonus":    absentee_bonus,
            "contact_bonus":     contact_bonus,
            "equity_bonus":      equity_bonus,
        }

    # ── Score a single property ────────────────────────────────────────────────

    def score_property(self, prop: Property) -> Dict:
        """
        Score a property across all 6 verticals.

        Returns a score dict with:
            property_id, parcel_id, address, owner_name,
            final_cds_score, vertical_scores, urgency_level, lead_tier,
            qualified, signal_count, distress_types, factor_scores
        """
        signals = self._collect_signals(prop)
        owner = prop.owner
        financial = prop.financial

        vertical_results = {
            v: self._score_vertical(v, signals, owner, financial)
            for v in VERTICAL_WEIGHTS
        }
        vertical_scores = {v: r["score"] for v, r in vertical_results.items()}

        final_score = max(vertical_scores.values()) if any(vertical_scores.values()) else 0.0

        # Routing / urgency
        if final_score >= ROUTING_THRESHOLDS["immediate"]:
            urgency = "Immediate"
        elif final_score >= ROUTING_THRESHOLDS["daily"]:
            urgency = "High"
        elif final_score >= ROUTING_THRESHOLDS["weekly"]:
            urgency = "Medium"
        else:
            urgency = "Low"

        # Lead tier
        lead_tier = "Bronze"
        for threshold, tier in LEAD_TIER_THRESHOLDS:
            if final_score >= threshold:
                lead_tier = tier
                break

        # Qualified = eligible for any routing tier
        qualified = final_score >= ROUTING_THRESHOLDS["weekly"]

        # Compact per-property debug line
        if signals:
            best_v = max(vertical_scores, key=vertical_scores.get)
            sig_types = sorted({s["type"] for s in signals})
            logger.debug(
                f"  {prop.parcel_id} | score={final_score:.0f} | {lead_tier} | {urgency} | "
                f"best={best_v}({vertical_scores[best_v]:.0f}) | "
                f"signals={len(signals)} [{', '.join(sig_types)}]"
            )

        return {
            "property_id":     prop.id,
            "parcel_id":       prop.parcel_id,
            "address":         prop.address,
            "owner_name":      owner.owner_name if owner else None,
            "final_cds_score": round(final_score, 2),
            "vertical_scores": {k: round(v, 2) for k, v in vertical_scores.items()},
            "urgency_level":   urgency,
            "lead_tier":       lead_tier,
            "qualified":       qualified,
            "signal_count":    len(signals),
            "distress_types":  list({s["type"] for s in signals}),
            "factor_scores":   self._build_factor_scores(signals, vertical_results),
        }

    def _build_factor_scores(self, signals: List[Dict], vertical_results: Dict) -> Dict:
        """
        Build the factor_scores JSONB payload with full per-component breakdown.

        Stored structure:
          signals[]           — all raw signal occurrences with recency info
          vertical_breakdown  — per-vertical: signal_score%, bonuses, and per-signal
                                base/recency/total contributions
        """
        today = date.today()

        # Raw signal list — all occurrences, each annotated with recency
        signal_list = []
        for sig in signals:
            d = sig["date"]
            if isinstance(d, datetime):
                d = d.date()
            days_old = (today - d).days if d else None
            signal_list.append({
                "type":         sig["type"],
                "date":         str(d) if d else None,
                "amount":       float(sig["amount"]) if sig["amount"] is not None else None,
                "recency_days": days_old,
                "recency_bonus": self._recency_bonus(sig["date"]),
            })

        # Per-vertical component breakdown
        vertical_breakdown = {}
        for v, result in vertical_results.items():
            vertical_breakdown[v] = {
                "final_score":       round(result["score"], 2),
                "primary_signal":    result["primary_signal"],
                "primary_score":     round(result["primary_score"], 2),
                "stacking_bonus":    result["stacking_bonus"],
                "signals_within_60": result["signals_within_60"],
                "signals":           result["signals"],   # {type: {base, recency, total}}
                "bonuses": {
                    "absentee": result["absentee_bonus"],
                    "contact":  result["contact_bonus"],
                    "equity":   result["equity_bonus"],
                },
            }

        return {
            "signals":            signal_list,
            "vertical_breakdown": vertical_breakdown,
        }

    # ── Database persistence ───────────────────────────────────────────────────

    def save_score_to_database(self, score_data: Dict, upsert: bool = True) -> Optional[DistressScore]:
        """
        UPSERT a DistressScore record.

        - If a score already exists for this property today → update it.
        - If not, check the most recent score; if unchanged → skip.
        - Otherwise → create new record.

        Returns the DistressScore record, or None if skipped (unchanged).
        """
        from sqlalchemy import cast, Date as SADate

        property_id   = score_data["property_id"]
        final_score   = score_data["final_cds_score"]
        lead_tier     = score_data["lead_tier"]
        urgency       = score_data["urgency_level"]
        qualified     = score_data["qualified"]
        factor_json   = score_data["factor_scores"]
        vertical_json = score_data["vertical_scores"]
        distress_list = score_data["distress_types"]
        today         = date.today()

        # Check for existing today's record (UPSERT)
        existing = None
        if upsert:
            existing = self.session.query(DistressScore).filter(
                DistressScore.property_id == property_id,
                cast(DistressScore.score_date, SADate) == today,
            ).first()

        if existing:
            existing.score_date      = datetime.utcnow()
            existing.final_cds_score = final_score
            existing.lead_tier       = lead_tier
            existing.urgency_level   = urgency
            existing.qualified       = qualified
            existing.factor_scores   = factor_json
            existing.vertical_scores = vertical_json
            existing.distress_types  = distress_list
            logger.debug(f"Updated score for property {score_data['parcel_id']}: {final_score} ({lead_tier})")
            return existing

        # Check most recent score to avoid accumulating identical rows
        latest = self.session.query(DistressScore).filter(
            DistressScore.property_id == property_id,
        ).order_by(DistressScore.score_date.desc()).first()

        if latest and float(latest.final_cds_score) == float(final_score):
            logger.debug(f"Score unchanged for property {score_data['parcel_id']}: {final_score} — skipping")
            return None

        record = DistressScore(
            property_id=property_id,
            score_date=datetime.utcnow(),
            final_cds_score=final_score,
            lead_tier=lead_tier,
            urgency_level=urgency,
            qualified=qualified,
            factor_scores=factor_json,
            vertical_scores=vertical_json,
            distress_types=distress_list,
        )
        self.session.add(record)
        logger.debug(f"Created score for property {score_data['parcel_id']}: {final_score} ({lead_tier})")
        return record

    # ── Batch scoring ──────────────────────────────────────────────────────────

    def _load_properties(self, property_ids: Optional[List[int]] = None) -> List[Property]:
        """Load properties with all signal relationships eager-loaded."""
        q = self.session.query(Property).options(
            joinedload(Property.owner),
            joinedload(Property.financial),
            joinedload(Property.code_violations),
            joinedload(Property.legal_and_liens),
            joinedload(Property.deeds),
            joinedload(Property.legal_proceedings),
            joinedload(Property.tax_delinquencies),
            joinedload(Property.foreclosures),
            joinedload(Property.building_permits),
        )
        if property_ids:
            q = q.filter(Property.id.in_(property_ids))
        return q.all()

    def score_all_properties(
        self,
        save_to_db: bool = True,
        property_ids: Optional[List[int]] = None,
    ) -> List[Dict]:
        """
        Score all properties (or only specific IDs).

        Args:
            save_to_db:   Persist scores to the database.
            property_ids: If provided, only rescore these property IDs.

        Returns:
            List of score dicts (one per property).
        """
        label = f"{len(property_ids)} properties" if property_ids else "all properties"
        logger.info(f"Loading {label} for scoring...")
        properties = self._load_properties(property_ids)
        logger.info(f"Scoring {len(properties)} properties across 6 verticals...")

        scores: List[Dict] = []
        saved_count = 0
        unchanged_count = 0
        no_signal_count = 0

        for prop in properties:
            try:
                score_data = self.score_property(prop)
                scores.append(score_data)

                if score_data["signal_count"] == 0:
                    no_signal_count += 1
                    continue

                if save_to_db:
                    result = self.save_score_to_database(score_data)
                    if result is None:
                        unchanged_count += 1
                    else:
                        saved_count += 1

            except Exception as e:
                logger.error(f"Error scoring property {prop.id} ({prop.parcel_id}): {e}")

        if save_to_db:
            logger.info(
                f"Scoring complete — {saved_count} saved, "
                f"{unchanged_count} unchanged, "
                f"{no_signal_count} no signals"
            )
        return scores

    def score_properties_by_ids(self, property_ids: List[int], save_to_db: bool = True) -> List[Dict]:
        """
        Fast path: rescore only specific properties.
        Used by the ingestion-time hook after a scraper run.
        """
        return self.score_all_properties(save_to_db=save_to_db, property_ids=property_ids)

    # ── Query helpers ──────────────────────────────────────────────────────────

    def get_saved_qualified_scores(self, min_score: float = ROUTING_THRESHOLDS["weekly"]):
        """Return saved DistressScore records at or above min_score, ordered by score desc."""
        return self.session.query(DistressScore).options(
            joinedload(DistressScore.property).joinedload(Property.owner),
            joinedload(DistressScore.property).joinedload(Property.financial),
        ).filter(
            DistressScore.final_cds_score >= min_score
        ).order_by(DistressScore.final_cds_score.desc()).all()

    def get_saved_scores_by_lead_tier(self, lead_tier: str):
        """Return saved DistressScore records matching a lead tier."""
        return self.session.query(DistressScore).options(
            joinedload(DistressScore.property).joinedload(Property.owner),
        ).filter(
            DistressScore.lead_tier == lead_tier
        ).order_by(DistressScore.final_cds_score.desc()).all()

    def get_latest_score_for_property(self, property_id: int) -> Optional[DistressScore]:
        """Get the most recent DistressScore record for a property."""
        return self.session.query(DistressScore).filter(
            DistressScore.property_id == property_id,
        ).order_by(DistressScore.score_date.desc()).first()

    def get_todays_score_for_property(self, property_id: int) -> Optional[DistressScore]:
        """Get today's DistressScore for a property (if it exists)."""
        from sqlalchemy import cast, Date as SADate
        return self.session.query(DistressScore).filter(
            DistressScore.property_id == property_id,
            cast(DistressScore.score_date, SADate) == date.today(),
        ).first()


# ── CLI entry point ────────────────────────────────────────────────────────────

def main():
    """
    Entry point for CLI / cron execution.

    Usage:
        python -m src.services.cds_engine                      # daily run (all properties)
        python -m src.services.cds_engine --rescore-all        # rescore all after weight change
        python -m src.services.cds_engine --property-id 12345  # rescore single property
    """
    import argparse
    from src.utils.logger import setup_logging, get_logger
    from src.core.database import get_db_context

    setup_logging()
    log = get_logger(__name__)

    parser = argparse.ArgumentParser(description="CDS Multi-Vertical Scoring Engine")
    parser.add_argument(
        "--rescore-all",
        action="store_true",
        help="Rescore every property in the database (use after changing config/scoring.py weights)",
    )
    parser.add_argument(
        "--property-id",
        type=int,
        metavar="ID",
        help="Rescore a single property by database ID",
    )
    args = parser.parse_args()

    property_ids = None
    if args.property_id:
        property_ids = [args.property_id]

    run_label = (
        f"property {args.property_id}" if args.property_id
        else "all properties (rescore)" if args.rescore_all
        else "daily run (all properties)"
    )

    log.info("=" * 60)
    log.info("CDS Multi-Vertical Scoring Engine")
    log.info(f"Mode:    {run_label}")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        with get_db_context() as session:
            scorer = MultiVerticalScorer(session)
            scores = scorer.score_all_properties(save_to_db=True, property_ids=property_ids)
            session.commit()

        from collections import Counter

        total = len(scores)
        with_signals = [s for s in scores if s["signal_count"] > 0]
        qualified = sum(1 for s in with_signals if s["qualified"])

        log.info("=" * 60)
        log.info("CDS SCORING COMPLETE")
        log.info(f"  Properties loaded:   {total:>7,}")
        log.info(f"  With signals:        {len(with_signals):>7,}")
        log.info(f"  No signals (skipped):{total - len(with_signals):>7,}")
        log.info(f"  Qualified (≥{ROUTING_THRESHOLDS['weekly']}):       {qualified:>7,}")

        if with_signals:
            scores_only = [s["final_cds_score"] for s in with_signals]
            avg = sum(scores_only) / len(scores_only)
            top = max(scores_only)
            log.info(f"  Avg score:           {avg:>7.1f}")
            log.info(f"  Top score:           {top:>7.1f}")

            # ── Lead tier distribution ────────────────────────────────────────
            log.info("")
            log.info("LEAD TIER DISTRIBUTION:")
            tier_counts = Counter(s["lead_tier"] for s in with_signals)
            for tier in ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]:
                bar = "█" * min(30, tier_counts.get(tier, 0))
                log.info(f"  {tier:<15} {tier_counts.get(tier, 0):>5}  {bar}")

            # ── Urgency distribution ──────────────────────────────────────────
            log.info("")
            log.info("URGENCY / ROUTING DISTRIBUTION:")
            urgency_counts = Counter(s["urgency_level"] for s in with_signals)
            for urgency, label in [
                ("Immediate", f"SMS  (≥{ROUTING_THRESHOLDS['immediate']})"),
                ("High",      f"Email(≥{ROUTING_THRESHOLDS['daily']})"),
                ("Medium",    f"Digest(≥{ROUTING_THRESHOLDS['weekly']})"),
                ("Low",       "Not routed"),
            ]:
                count = urgency_counts.get(urgency, 0)
                log.info(f"  {urgency:<10} {label:<18} {count:>5}")

            # ── Vertical driving max score ────────────────────────────────────
            log.info("")
            log.info("TOP VERTICAL (driving final_cds_score):")
            top_v_counts = Counter(
                max(s["vertical_scores"], key=s["vertical_scores"].get)
                for s in with_signals
            )
            for v, count in top_v_counts.most_common():
                bar = "█" * min(30, count)
                log.info(f"  {v:<20} {count:>5}  {bar}")

            # ── Signal type frequency ─────────────────────────────────────────
            log.info("")
            log.info("SIGNAL TYPE FREQUENCY (properties carrying each type):")
            sig_counts = Counter(t for s in with_signals for t in s["distress_types"])
            for sig_type, count in sig_counts.most_common():
                bar = "█" * min(30, count)
                log.info(f"  {sig_type:<25} {count:>5}  {bar}")

            # ── Top 10 scored properties ──────────────────────────────────────
            log.info("")
            log.info("TOP 10 SCORED PROPERTIES:")
            log.info(f"  {'Parcel':<20} {'Score':>6} {'Tier':<15} {'Urgency':<10} {'Best Vertical':<20} {'Signals'}")
            log.info(f"  {'-'*20} {'-'*6} {'-'*15} {'-'*10} {'-'*20} {'-'*7}")
            for s in sorted(with_signals, key=lambda x: x["final_cds_score"], reverse=True)[:10]:
                best_v = max(s["vertical_scores"], key=s["vertical_scores"].get)
                best_v_score = s["vertical_scores"][best_v]
                log.info(
                    f"  {(s['parcel_id'] or 'N/A'):<20} "
                    f"{s['final_cds_score']:>6.1f} "
                    f"{s['lead_tier']:<15} "
                    f"{s['urgency_level']:<10} "
                    f"{best_v}({best_v_score:.0f}) "
                    f"  [{s['signal_count']} signals]"
                )

        log.info("")
        log.info(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60)

    except Exception as e:
        log.error(f"CDS scoring failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
