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
from collections import Counter
from datetime import datetime, date, timezone
from typing import Dict, List, Optional

from src.services.ghl_webhook import push_lead_to_ghl
from config.settings import settings

# Can be overridden at runtime via --no-ghl CLI flag; default comes from GHL_PUSH_ENABLED env var
_GHL_PUSH_ENABLED: bool = settings.ghl_push_enabled

# GHL batching — push leads in chunks with a pause between batches to stay under rate limits
_GHL_BATCH_SIZE: int = 25   # leads per batch
_GHL_BATCH_DELAY: float = 3.0  # seconds between batches

from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from src.core.models import (
    DistressScore,
    Owner,
    Financial,
    Property,
    PlatformDailyStats,
    ScraperRunStats,
)
from config.scoring import (
    ABSENTEE_BONUS,
    AGE_DECAY_1Y,
    AGE_DECAY_2Y,
    CONTACT_EMAIL_BONUS,
    CONTACT_PHONE_BONUS,
    DAYS_OPEN_MODIFIERS,
    EQUITY_BONUS_BY_VERTICAL,
    EQUITY_HIGH_THRESH,
    EQUITY_MID_THRESH,
    LEAD_TIER_THRESHOLDS,
    PERSISTENCE_ESCALATION_KEYWORDS,
    PERSISTENCE_RESOLVED_KEYWORDS,
    PERSISTENCE_SCOPE_BONUSES,
    PERSISTENCE_STATUS_ACTIVE,
    PERSISTENCE_STATUS_ESCALATED,
    PERSISTENCE_STATUS_RESOLVED,
    PRIOR_VIOLATIONS_MODIFIERS,
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

# Guard against empty VERTICAL_WEIGHTS misconfiguration at import time
if not VERTICAL_WEIGHTS:
    raise RuntimeError(
        "VERTICAL_WEIGHTS is empty — check config/scoring.py. "
        "At least one vertical must be configured."
    )


class MultiVerticalScorer:
    """
    6-vertical CDS scoring engine.

    Scores properties at ingestion time using all 14 real-time signal sources.
    Weights are driven entirely by config/scoring.py — no code rebuild required
    when tuning.
    """

    def __init__(self, session: Session):
        self.session = session
        self._ghl_push_queue: List[Dict] = []

    # ── GHL batch flush ───────────────────────────────────────────────────────

    def _flush_ghl_queue(self) -> None:
        """Push queued leads to GHL in batches to avoid rate-limit spikes."""
        import time
        queue = self._ghl_push_queue
        if not queue:
            return
        total = len(queue)
        pushed = failed = 0
        logger.info("[GHL] Flushing %d queued leads in batches of %d", total, _GHL_BATCH_SIZE)
        for i in range(0, total, _GHL_BATCH_SIZE):
            batch = queue[i: i + _GHL_BATCH_SIZE]
            for score_data in batch:
                try:
                    push_lead_to_ghl(score_data)
                    pushed += 1
                except Exception:
                    failed += 1
                    logger.warning(
                        "[GHL] push failed for property %s",
                        score_data.get("parcel_id"), exc_info=True,
                    )
            if i + _GHL_BATCH_SIZE < total:
                logger.debug("[GHL] batch %d/%d done — sleeping %.1fs", i // _GHL_BATCH_SIZE + 1, -(-total // _GHL_BATCH_SIZE), _GHL_BATCH_DELAY)
                time.sleep(_GHL_BATCH_DELAY)
        logger.info("[GHL] Flush complete — pushed=%d failed=%d", pushed, failed)
        self._ghl_push_queue.clear()

    # ── Signal collection ─────────────────────────────────────────────────────

    def _collect_signals(self, prop: Property) -> List[Dict]:
        """
        Gather all distress signals for a property from all 14 sources.

        Returns a flat list of:
            {"type": str, "date": date|None, "amount": float|None}
        """
        signals: List[Dict] = []

        # 1. Code violations — pass fine_amount and opened_date for modifier calculations
        for v in (prop.code_violations or []):
            signals.append({
                "type":        "code_violations",
                "date":        v.opened_date,
                "amount":      float(v.fine_amount) if v.fine_amount is not None else None,
                "opened_date": v.opened_date,   # used for days-open modifier
            })

        # 2. Legal and Liens (liens + judgments)
        for lien in (prop.legal_and_liens or []):
            if lien.record_type == "Judgment":
                sig_type = "judgment_liens"
            else:
                sig_type = _DOCUMENT_TYPE_TO_SIGNAL.get(lien.document_type or "")
                if not sig_type:
                    logger.debug(
                        "Unknown LegalAndLien document_type: '%s' on property %s — skipping",
                        lien.document_type, prop.parcel_id,
                    )
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

        # 8. Incidents (insurance_claim, fire, storm_damage, flood_damage)
        _INCIDENT_SIGNAL_TYPES = {"insurance_claim", "Fire", "storm_damage", "flood_damage"}
        for inc in (prop.incidents or []):
            if inc.incident_type in _INCIDENT_SIGNAL_TYPES:
                signals.append({"type": inc.incident_type, "date": inc.incident_date, "amount": None})

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

    def _age_decay(self, sig_date) -> int:
        """Return a negative modifier for stale signals (older than 1 year)."""
        if not sig_date:
            return 0
        if isinstance(sig_date, datetime):
            sig_date = sig_date.date()
        days_old = (date.today() - sig_date).days
        if days_old > 730:
            return AGE_DECAY_2Y
        if days_old > 365:
            return AGE_DECAY_1Y
        return 0

    def _days_open_modifier(self, opened_date) -> int:
        """Return the days-open modifier for a code violation."""
        if not opened_date:
            return 0
        if isinstance(opened_date, datetime):
            opened_date = opened_date.date()
        days_open = (date.today() - opened_date).days
        for max_days, modifier in DAYS_OPEN_MODIFIERS:
            if max_days is None or days_open <= max_days:
                return modifier
        return 0

    def _persistence_modifier(self, persistence_data: Dict) -> int:
        """
        Violation Persistence Score — proxy for owner inaction, replacing fine_amount.

        fine_amount is not captured from Accela so this uses two available fields:

        Component A — Status escalation (max +12):
            Derived from the 'status' of the property's most-recent open violation.
            Escalated  ("hearing", "abatement", "order", "lien" …): +12
            Active     (open/issued, not yet resolved):               +6
            Resolved   ("complied", "closed", "withdrawn" …):         0

        Component B — Violation type diversity (max +8):
            Count of distinct violation_type values across ALL violations.
            1 type  → +0   (single-issue, may be a one-time event)
            2 types → +4   (multi-issue — broader neglect)
            3+ types → +8  (chronic multi-domain neglect)

        Total max: 20 — same cap as former fine_mod for score stability.
        Only fires when code_violations is the primary signal for the vertical.
        """
        if not persistence_data:
            return 0

        # Component A: status-based escalation
        status = (persistence_data.get("latest_status") or "").lower()
        if any(kw in status for kw in PERSISTENCE_ESCALATION_KEYWORDS):
            status_score = PERSISTENCE_STATUS_ESCALATED
        elif any(kw in status for kw in PERSISTENCE_RESOLVED_KEYWORDS):
            status_score = PERSISTENCE_STATUS_RESOLVED
        elif status:                              # any non-empty, non-resolved status
            status_score = PERSISTENCE_STATUS_ACTIVE
        else:
            status_score = 0

        # Component B: violation type diversity across the property
        distinct_types = persistence_data.get("distinct_types", 0)
        scope_score = 0
        for max_count, bonus in PERSISTENCE_SCOPE_BONUSES:
            if max_count is None or distinct_types <= max_count:
                scope_score = bonus
                break

        return status_score + scope_score

    def _prior_violations_modifier(self, violation_count: int) -> int:
        """Return the prior violations count modifier."""
        for max_count, modifier in PRIOR_VIOLATIONS_MODIFIERS:
            if max_count is None or violation_count <= max_count:
                return modifier
        return 0

    # ── Routing Gate Helper ───────────────────────────────────────────────────

    def _is_within_window(self, sig_date) -> bool:
        """
        Check if a signal date falls within the STACKING_WINDOW_DAYS.
        Used by the 2-signal minimum routing gate.
        """
        if not sig_date:
            return False

        # Normalise datetime → date
        if isinstance(sig_date, datetime):
            sig_date = sig_date.date()

        days_old = (date.today() - sig_date).days
        return days_old <= STACKING_WINDOW_DAYS

    # ── Per-vertical scorer ────────────────────────────────────────────────────

    def _score_vertical(
        self,
        vertical: str,
        signals: List[Dict],
        owner: Optional[Owner],
        financial: Optional[Financial],
        violation_count: int = 0,
        persistence_data: Optional[Dict] = None,
    ) -> Dict:
        """
        Score a single vertical per spec. Returns a result dict.

        Formula:
          primary_score    = base_weight[best_signal] + recency_bonus - age_decay
          days_open_mod    = days-open modifier  (code_violations primary only)
          persistence_mod  = persistence score   (code_violations primary only)
                             replaces fine_amount — uses Accela status + type diversity
          prior_viol_mod   = prior violations count modifier (any code_violation signal)
          stacking_bonus   = min((signals_within_window - 1) * 20, 40)
          final_score      = min(100, primary_score + days_open_mod + persistence_mod
                                 + prior_viol_mod + stacking_bonus
                                 + absentee + contact + equity)

        Equity bonus applies to all verticals with per-vertical rates.
        """
        weights = VERTICAL_WEIGHTS.get(vertical)
        if weights is None:
            raise KeyError(
                f"Vertical '{vertical}' not found in VERTICAL_WEIGHTS. "
                f"Available: {list(VERTICAL_WEIGHTS.keys())}"
            )

        today = date.today()

        # Group by signal type — each type counts once, using its most recent signal.
        # For code_violations keep the most recent opened_date + associated fine_amount.
        latest_by_type: Dict[str, Dict] = {}
        for sig in signals:
            sig_type = sig["type"]
            if sig_type not in weights:
                continue
            d = sig["date"]
            if isinstance(d, datetime):
                d = d.date()
            existing = latest_by_type.get(sig_type)
            if existing is None or (d and (existing["date"] is None or d > existing["date"])):
                latest_by_type[sig_type] = {
                    "date":        d,
                    "opened_date": sig.get("opened_date"),
                    "fine_amount": sig.get("amount") if sig_type == "code_violations" else None,
                }

        if not latest_by_type:
            return {
                "score":                 0.0,
                "primary_signal":        None,
                "primary_score":         0.0,
                "stacking_bonus":        0,
                "signals_within_window": 0,
                "signals":               {},
                "absentee_bonus":        0,
                "contact_bonus":         0,
                "equity_bonus":          0,
                "days_open_mod":         0,
                "persistence_mod":       0,
                "prior_viol_mod":        0,
            }

        # Build per-signal components — apply age decay to each
        signal_components: Dict[str, Dict] = {}
        best_type  = None
        best_total = -999
        for sig_type, sig_info in latest_by_type.items():
            sig_date = sig_info["date"]
            base    = weights[sig_type]
            recency = self._recency_bonus(sig_date)
            decay   = self._age_decay(sig_date)
            total   = base + recency + decay   # decay is negative
            signal_components[sig_type] = {
                "base":      base,
                "recency":   recency,
                "age_decay": decay,
                "total":     total,
            }
            if total > best_total:
                best_total = total
                best_type  = sig_type

        primary_score = float(best_total)

        # Code violation extra modifiers — applied only when code_violations is primary
        days_open_mod   = 0
        persistence_mod = 0
        if best_type == "code_violations":
            viol_info     = latest_by_type["code_violations"]
            days_open_mod = self._days_open_modifier(viol_info.get("opened_date"))
            persistence_mod = self._persistence_modifier(persistence_data or {})

        # Prior violations modifier — applies whenever any code_violation signal is present
        prior_viol_mod = 0
        if "code_violations" in latest_by_type and violation_count > 0:
            prior_viol_mod = self._prior_violations_modifier(violation_count)

        # Stacking: count distinct signal types with a date within the window
        signals_within_window = sum(
            1 for si in latest_by_type.values()
            if si["date"] and (today - si["date"]).days <= STACKING_WINDOW_DAYS
        )
        stacking_bonus = min(
            max(0, signals_within_window - 1) * STACKING_BONUS_PER_SIGNAL,
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

        # Equity bonus — all verticals, per-vertical rate
        equity_bonus = 0
        rate = EQUITY_BONUS_BY_VERTICAL.get(vertical, 0)
        if rate and financial and financial.equity_pct is not None:
            try:
                eq = float(financial.equity_pct)
                if eq > EQUITY_HIGH_THRESH:
                    equity_bonus = rate
                elif eq > EQUITY_MID_THRESH:
                    equity_bonus = rate // 2
            except (TypeError, ValueError):
                logger.warning(
                    "Could not parse equity_pct '%s' for vertical %s — equity bonus skipped",
                    financial.equity_pct, vertical,
                )

        final_score = min(
            primary_score + days_open_mod + persistence_mod + prior_viol_mod
            + stacking_bonus + absentee_bonus + contact_bonus + equity_bonus,
            float(SCORE_CAP),
        )

        logger.debug(
            "    [%s] primary=%s(%.0f) days_open=+%d persistence=+%d prior_viol=+%d"
            " stack=+%d(%d sigs/%dd) absentee=+%d contact=+%d equity=+%d → %.1f",
            vertical, best_type, primary_score,
            days_open_mod, persistence_mod, prior_viol_mod,
            stacking_bonus, signals_within_window, STACKING_WINDOW_DAYS,
            absentee_bonus, contact_bonus, equity_bonus,
            final_score,
        )

        return {
            "score":                 final_score,
            "primary_signal":        best_type,
            "primary_score":         primary_score,
            "stacking_bonus":        stacking_bonus,
            "signals_within_window": signals_within_window,
            "signals":               signal_components,
            "absentee_bonus":        absentee_bonus,
            "contact_bonus":         contact_bonus,
            "equity_bonus":          equity_bonus,
            "days_open_mod":         days_open_mod,
            "persistence_mod":       persistence_mod,
            "prior_viol_mod":        prior_viol_mod,
        }

    # ── Score a single property ────────────────────────────────────────────────

    def score_property(self, prop: Property) -> Dict:
        """
        Score a property across all 6 verticals with a 2-signal routing gate.
        """
        signals = self._collect_signals(prop)
        owner = prop.owner
        financial = prop.financial
        violations = prop.code_violations or []
        violation_count = len(violations)

        # Persistence data: derived from all violations for this property.
        if violations:
            # Normalise opened_date to date for comparison — guard mixed date/datetime types
            def _to_date(v):
                d = v.opened_date
                if isinstance(d, datetime):
                    return d.date()
                return d or date.min

            latest_viol = max(violations, key=_to_date)
            persistence_data: Dict = {
                "latest_status":  latest_viol.status,
                "distinct_types": len({v.violation_type for v in violations if v.violation_type}),
            }
        else:
            persistence_data = {}

        vertical_results = {
            v: self._score_vertical(v, signals, owner, financial, violation_count, persistence_data)
            for v in VERTICAL_WEIGHTS
        }
        vertical_scores = {v: r["score"] for v, r in vertical_results.items()}

        # Guard: if no verticals produced any score, default to 0.0
        score_values = [s for s in vertical_scores.values() if s]
        final_score = max(score_values) if score_values else 0.0

        # Calculate distinct signal types within the window for the routing gate
        distinct_signals_count = len({s["type"] for s in signals if self._is_within_window(s["date"])})

        # Routing / urgency with Option C gate
        if final_score >= ROUTING_THRESHOLDS["immediate"]:
            # GATE: Require 2+ distinct signals for Immediate SMS
            urgency = "Immediate" if distinct_signals_count >= 2 else "High"
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

        # Tax delinquency alone is a cumulative snapshot, not a fresh distress event.
        # Require at least one corroborating signal before qualifying as a lead.
        if qualified and set(s["type"] for s in signals) == {"tax_delinquencies"}:
            qualified = False
            lead_tier = "Bronze"

        # Compact per-property debug line
        if signals:
            best_v = max(vertical_scores, key=vertical_scores.get)
            sig_types = sorted({s["type"] for s in signals})
            logger.debug(
                "  %s | score=%.0f | %s | %s | best=%s(%.0f) | signals=%d [%s]",
                prop.parcel_id, final_score, lead_tier, urgency,
                best_v, vertical_scores[best_v],
                len(signals), ", ".join(sig_types),
            )

        # Collect owner contact info for CRM push
        owner_phone = None
        owner_email = None
        if owner:
            owner_phone = owner.phone_1 or owner.phone_2 or owner.phone_3 or None
            owner_email = owner.email_1 or owner.email_2 or None

        return {
            "property_id":     prop.id,
            "parcel_id":       prop.parcel_id,
            "address":         prop.address,
            "city":            prop.city,
            "state":           prop.state,
            "zip":             prop.zip,
            # Property specs
            "sq_ft":           float(prop.sq_ft) if prop.sq_ft else None,
            "beds":            prop.beds,
            "baths":           prop.baths,
            "year_built":      prop.year_built,
            "lot_size":        float(prop.lot_size) if prop.lot_size else None,
            # Owner
            "ghl_contact_id":  prop.gohighlevel_contact_id,
            "owner_name":      owner.owner_name if owner else None,
            "owner_type":      owner.owner_type if owner else None,
            "absentee_status": owner.absentee_status if owner else None,
            "mailing_address": owner.mailing_address if owner else None,
            "ownership_years": owner.ownership_years if owner else None,
            "owner_phone":     owner_phone,
            "owner_email":     owner_email,
            # Financial
            "assessed_value_mkt": float(financial.assessed_value_mkt) if financial and financial.assessed_value_mkt else None,
            "homestead_exempt":   financial.homestead_exempt if financial else None,
            "est_equity":         float(financial.est_equity) if financial and financial.est_equity else None,
            "equity_pct":         float(financial.equity_pct) if financial and financial.equity_pct else None,
            "last_sale_price":    float(financial.last_sale_price) if financial and financial.last_sale_price else None,
            "last_sale_date":     str(financial.last_sale_date) if financial and financial.last_sale_date else None,
            # Scoring
            "final_cds_score": round(final_score, 2),
            "vertical_scores": {k: round(v, 2) for k, v in vertical_scores.items()},
            "urgency_level":   urgency,
            "lead_tier":       lead_tier,
            "qualified":       qualified,
            "signal_count":    len(signals),
            "distress_types":  list({s["type"] for s in signals}),
            "factor_scores":   self._build_factor_scores(signals, vertical_results),
            "signal_summaries": self._build_signal_summaries(prop),
        }

    def _build_signal_summaries(self, prop: "Property") -> Dict[str, str]:
        """Build one-liner summary strings per signal type for CRM display."""
        summaries: Dict[str, str] = {}

        violations = prop.code_violations or []
        if violations:
            open_count = sum(1 for v in violations if (v.status or "").lower() == "open")
            types = sorted({v.violation_type for v in violations if v.violation_type})
            latest = max((v.opened_date for v in violations if v.opened_date), default=None)
            parts = [f"{len(violations)} violation(s)"]
            if open_count:
                parts.append(f"{open_count} open")
            if types:
                parts.append(", ".join(types[:2]))
            if latest:
                parts.append(str(latest))
            summaries["code_violations_summary"] = " — ".join(parts)

        all_legal = prop.legal_and_liens or []

        # Judgments
        judgments = [r for r in all_legal if r.record_type == "Judgment"]
        if judgments:
            total = sum(float(r.amount) for r in judgments if r.amount)
            latest = max((r.filing_date for r in judgments if r.filing_date), default=None)
            parts = [f"{len(judgments)} judgment(s)"]
            if total:
                parts.append(f"${total:,.0f} total")
            if latest:
                parts.append(str(latest))
            summaries["judgment_summary"] = " — ".join(parts)

        # Mechanics liens
        def _lien_subtype(records, doc_keywords):
            return [r for r in records if r.record_type == "Lien" and
                    any(k in (r.document_type or "") for k in doc_keywords)]

        mechanics = _lien_subtype(all_legal, ["MECHANICS", "ML"])
        if mechanics:
            total = sum(float(r.amount) for r in mechanics if r.amount)
            latest = max((r.filing_date for r in mechanics if r.filing_date), default=None)
            parts = [f"{len(mechanics)} mechanics lien(s)"]
            if total:
                parts.append(f"${total:,.0f} total")
            if latest:
                parts.append(str(latest))
            summaries["mechanics_lien_summary"] = " — ".join(parts)

        tax_liens = _lien_subtype(all_legal, ["TAX LIEN", "TL"])
        if tax_liens:
            total = sum(float(r.amount) for r in tax_liens if r.amount)
            latest = max((r.filing_date for r in tax_liens if r.filing_date), default=None)
            parts = [f"{len(tax_liens)} tax lien(s)"]
            if total:
                parts.append(f"${total:,.0f} total")
            if latest:
                parts.append(str(latest))
            summaries["tax_lien_summary"] = " — ".join(parts)

        hoa_liens = _lien_subtype(all_legal, ["HOA", "HL"])
        if hoa_liens:
            total = sum(float(r.amount) for r in hoa_liens if r.amount)
            latest = max((r.filing_date for r in hoa_liens if r.filing_date), default=None)
            parts = [f"{len(hoa_liens)} HOA lien(s)"]
            if total:
                parts.append(f"${total:,.0f} total")
            if latest:
                parts.append(str(latest))
            summaries["hoa_lien_summary"] = " — ".join(parts)

        code_liens = _lien_subtype(all_legal, ["CODE LIEN", "TCL", "CCL"])
        if code_liens:
            total = sum(float(r.amount) for r in code_liens if r.amount)
            latest = max((r.filing_date for r in code_liens if r.filing_date), default=None)
            parts = [f"{len(code_liens)} code lien(s)"]
            if total:
                parts.append(f"${total:,.0f} total")
            if latest:
                parts.append(str(latest))
            summaries["code_lien_summary"] = " — ".join(parts)

        foreclosures = prop.foreclosures or []
        if foreclosures:
            fc = foreclosures[0]
            parts = [f"{len(foreclosures)} foreclosure(s)"]
            if fc.plaintiff:
                parts.append(fc.plaintiff)
            if fc.judgment_amount:
                parts.append(f"${float(fc.judgment_amount):,.0f} judgment")
            if fc.auction_date:
                parts.append(f"auction {fc.auction_date}")
            summaries["foreclosure_summary"] = " — ".join(parts)

        taxes = prop.tax_delinquencies or []
        if taxes:
            total = sum(float(t.total_amount_due) for t in taxes if t.total_amount_due)
            max_years = max((t.years_delinquent for t in taxes if t.years_delinquent), default=None)
            parts = [f"{len(taxes)} tax record(s)"]
            if total:
                parts.append(f"${total:,.0f} due")
            if max_years:
                parts.append(f"{max_years}yr delinquent")
            summaries["tax_delinquency_summary"] = " — ".join(parts)

        proceedings = prop.legal_proceedings or []
        for ptype, key in [("Probate", "probate_summary"), ("Eviction", "eviction_summary"), ("Bankruptcy", "bankruptcy_summary")]:
            group = [p for p in proceedings if p.record_type == ptype]
            if group:
                latest = max((p.filing_date for p in group if p.filing_date), default=None)
                parts = [f"{len(group)} {ptype.lower()}(s)"]
                if latest:
                    parts.append(str(latest))
                # Include case status/party from first record
                first = group[0]
                if first.associated_party:
                    parts.append(first.associated_party)
                summaries[key] = " — ".join(parts)

        deeds = prop.deeds or []
        if deeds:
            d = deeds[0]
            parts = [f"{len(deeds)} deed(s)"]
            if d.deed_type:
                parts.append(d.deed_type)
            if d.sale_price:
                parts.append(f"${float(d.sale_price):,.0f}")
            if d.record_date:
                parts.append(str(d.record_date))
            summaries["deed_summary"] = " — ".join(parts)

        permits = prop.building_permits or []
        if permits:
            ptypes = sorted({p.permit_type for p in permits if p.permit_type})
            latest = max((p.issue_date for p in permits if p.issue_date), default=None)
            parts = [f"{len(permits)} permit(s)"]
            if ptypes:
                parts.append(ptypes[0])
            if latest:
                parts.append(str(latest))
            summaries["permit_summary"] = " — ".join(parts)

        return summaries

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
                "final_score":           round(result["score"], 2),
                "primary_signal":        result["primary_signal"],
                "primary_score":         round(result["primary_score"], 2),
                "stacking_bonus":        result["stacking_bonus"],
                "signals_within_window": result["signals_within_window"],
                "signals":               result["signals"],   # {type: {base, recency, age_decay, total}}
                "bonuses": {
                    "absentee":     result["absentee_bonus"],
                    "contact":      result["contact_bonus"],
                    "equity":       result["equity_bonus"],
                    "days_open":    result["days_open_mod"],
                    "persistence":  result["persistence_mod"],
                    "prior_viol":   result["prior_viol_mod"],
                },
            }

        return {
            "signals":            signal_list,
            "vertical_breakdown": vertical_breakdown,
        }

    # ── Database persistence ───────────────────────────────────────────────────

    def save_score_to_database(self, score_data: Dict, upsert: bool = True):
        """
        UPSERT a DistressScore record.

        - If a score already exists for this property today → update it.
        - If not, check the most recent score; if unchanged → skip.
        - Otherwise → create new record.

        Returns a tuple (record_or_None, status) where status is one of:
            'new'       — first-ever score for this property today
            'updated'   — existing today's record was refreshed
            'unchanged' — score identical to last recorded; skipped
        Raises SQLAlchemyError on DB failure — caller must handle rollback.
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
            # Guard against NULL stored in DB column
            try:
                prev_score = float(existing.final_cds_score) if existing.final_cds_score is not None else 0.0
            except (TypeError, ValueError):
                prev_score = 0.0

            prev_tier = existing.lead_tier
            score_changed = prev_score != float(final_score)
            existing.score_date      = datetime.now(timezone.utc)
            existing.final_cds_score = final_score
            existing.lead_tier       = lead_tier
            existing.urgency_level   = urgency
            existing.qualified       = qualified
            existing.factor_scores   = factor_json
            existing.vertical_scores = vertical_json
            existing.distress_types  = distress_list
            logger.debug(
                "Updated score for property %s: %.2f (%s)",
                score_data.get("parcel_id"), final_score, lead_tier,
            )
            is_new_contact = not score_data.get("ghl_contact_id")
            if _GHL_PUSH_ENABLED and (score_changed or is_new_contact):
                self._ghl_push_queue.append(score_data)
            # Detect tier upgrade for stats (tiers ordered best→worst)
            _TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
            upgraded = (
                prev_tier in _TIER_ORDER and lead_tier in _TIER_ORDER
                and _TIER_ORDER.index(lead_tier) < _TIER_ORDER.index(prev_tier)
            )
            return existing, 'updated', upgraded

        # Check most recent score to avoid accumulating identical rows
        latest = self.session.query(DistressScore).filter(
            DistressScore.property_id == property_id,
        ).order_by(DistressScore.score_date.desc()).first()

        try:
            latest_score = float(latest.final_cds_score) if latest and latest.final_cds_score is not None else None
        except (TypeError, ValueError):
            latest_score = None

        latest_tier = latest.lead_tier if latest else None
        if latest_score is not None and latest_score == float(final_score) and latest_tier == lead_tier:
            logger.debug(
                "Score unchanged for property %s: %.2f — skipping",
                score_data.get("parcel_id"), final_score,
            )
            return None, 'unchanged', False

        record = DistressScore(
            property_id=property_id,
            score_date=datetime.now(timezone.utc),
            final_cds_score=final_score,
            lead_tier=lead_tier,
            urgency_level=urgency,
            qualified=qualified,
            factor_scores=factor_json,
            vertical_scores=vertical_json,
            distress_types=distress_list,
        )
        self.session.add(record)
        # Flush to get the DB-assigned primary key before pushing to GHL
        self.session.flush()

        logger.debug(
            "Created score for property %s: %.2f (%s)",
            score_data.get("parcel_id"), final_score, lead_tier,
        )
        if _GHL_PUSH_ENABLED:
            self._ghl_push_queue.append(score_data)
        return record, 'new', False

    # ── Batch scoring ──────────────────────────────────────────────────────────

    def _load_properties(self, property_ids: Optional[List[int]] = None) -> List[Property]:
        """Load properties with all signal relationships eager-loaded."""
        try:
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
        except OperationalError as exc:
            logger.error(
                "Database error loading properties for scoring (ids=%s): %s",
                property_ids, exc, exc_info=True,
            )
            raise

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
            List of score dicts for successfully scored properties.
        """
        label = f"{len(property_ids)} properties" if property_ids else "all properties"
        logger.info("Loading %s for scoring...", label)
        properties = self._load_properties(property_ids)
        logger.info("Scoring %d properties across 6 verticals...", len(properties))

        scores: List[Dict] = []
        new_count       = 0
        updated_count   = 0
        unchanged_count = 0
        upgraded_count  = 0
        no_signal_count = 0
        failed_count    = 0
        qualified_count = 0
        tier_counts: Counter = Counter()

        for prop in properties:
            try:
                score_data = self.score_property(prop)
                scores.append(score_data)

                if score_data["signal_count"] == 0:
                    no_signal_count += 1
                    continue

                if save_to_db:
                    _record, status, upgraded = self.save_score_to_database(score_data)
                    if status == 'new':
                        new_count += 1
                    elif status == 'updated':
                        updated_count += 1
                    else:
                        unchanged_count += 1
                    if upgraded:
                        upgraded_count += 1
                    if score_data.get("qualified"):
                        qualified_count += 1
                    tier_counts[score_data["lead_tier"]] += 1

            except SQLAlchemyError as exc:
                # DB error mid-batch: roll back the failed unit so the session
                # stays usable for subsequent properties.
                failed_count += 1
                logger.error(
                    "Database error scoring property %s (%s) — rolling back and continuing",
                    prop.id, prop.parcel_id, exc_info=True,
                )
                try:
                    self.session.rollback()
                except Exception:
                    logger.error("Rollback failed after DB error on property %s", prop.id, exc_info=True)

            except Exception as exc:
                failed_count += 1
                logger.error(
                    "Unexpected error scoring property %s (%s): %s",
                    prop.id, prop.parcel_id, exc, exc_info=True,
                )

        if save_to_db:
            logger.info(
                "Scoring complete — %d new, %d updated, %d unchanged, %d no signals, %d failed",
                new_count, updated_count, unchanged_count, no_signal_count, failed_count,
            )
            if failed_count:
                logger.warning(
                    "%d properties failed to score — check logs above for details",
                    failed_count,
                )
            # Flush GHL push queue in batches
            if _GHL_PUSH_ENABLED:
                self._flush_ghl_queue()

            # Persist platform-level daily stats
            try:
                self._record_platform_stats(
                    properties_scored=len(scores),
                    properties_with_signals=len(scores) - no_signal_count,
                    score_runs_total=new_count + updated_count + unchanged_count,
                    leads_new=new_count,
                    leads_updated=updated_count,
                    leads_unchanged=unchanged_count,
                    leads_qualified=qualified_count,
                    leads_upgraded=upgraded_count,
                    tier_counts=tier_counts,
                )
            except Exception as stats_err:
                logger.warning("⚠ Could not record platform daily stats (non-critical): %s", stats_err)

        return scores

    def _record_platform_stats(
        self,
        properties_scored: int,
        properties_with_signals: int,
        score_runs_total: int,
        leads_new: int,
        leads_updated: int,
        leads_unchanged: int,
        leads_qualified: int,
        leads_upgraded: int,
        tier_counts: "Counter",
        county_id: str = 'hillsborough',
    ) -> None:
        """
        Upsert a row in platform_daily_stats for today.

        Signal totals (signals_scraped/matched/skipped) are pulled live from
        scraper_run_stats for today so they reflect all scrapers that have run,
        regardless of whether they ran before or after the CDS engine.
        """
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from sqlalchemy import func

        today = date.today()

        # Roll up signal counts from scraper_run_stats for today
        signal_row = self.session.query(
            func.coalesce(func.sum(ScraperRunStats.total_scraped), 0).label('scraped'),
            func.coalesce(func.sum(ScraperRunStats.matched), 0).label('matched'),
            func.coalesce(func.sum(ScraperRunStats.skipped), 0).label('skipped'),
        ).filter(
            ScraperRunStats.run_date == today,
            ScraperRunStats.county_id == county_id,
        ).one()

        stmt = pg_insert(PlatformDailyStats).values(
            run_date=today,
            county_id=county_id,
            signals_scraped=int(signal_row.scraped),
            signals_matched=int(signal_row.matched),
            signals_skipped=int(signal_row.skipped),
            properties_scored=properties_scored,
            properties_with_signals=properties_with_signals,
            score_runs_total=score_runs_total,
            leads_new=leads_new,
            leads_updated=leads_updated,
            leads_unchanged=leads_unchanged,
            leads_qualified=leads_qualified,
            leads_upgraded=leads_upgraded,
            tier_ultra_platinum=tier_counts.get('Ultra Platinum', 0),
            tier_platinum=tier_counts.get('Platinum', 0),
            tier_gold=tier_counts.get('Gold', 0),
            tier_silver=tier_counts.get('Silver', 0),
            tier_bronze=tier_counts.get('Bronze', 0),
        ).on_conflict_do_update(
            constraint='uq_platform_daily_stats',
            set_=dict(
                signals_scraped=int(signal_row.scraped),
                signals_matched=int(signal_row.matched),
                signals_skipped=int(signal_row.skipped),
                properties_scored=properties_scored,
                properties_with_signals=properties_with_signals,
                score_runs_total=score_runs_total,
                leads_new=leads_new,
                leads_updated=leads_updated,
                leads_unchanged=leads_unchanged,
                leads_qualified=leads_qualified,
                leads_upgraded=leads_upgraded,
                tier_ultra_platinum=tier_counts.get('Ultra Platinum', 0),
                tier_platinum=tier_counts.get('Platinum', 0),
                tier_gold=tier_counts.get('Gold', 0),
                tier_silver=tier_counts.get('Silver', 0),
                tier_bronze=tier_counts.get('Bronze', 0),
                updated_at=datetime.now(timezone.utc),
            )
        )
        self.session.execute(stmt)
        self.session.flush()
        logger.info(
            "✓ Platform daily stats recorded: scored=%d new=%d updated=%d qualified=%d upgraded=%d",
            properties_scored, leads_new, leads_updated, leads_qualified, leads_upgraded,
        )

    def score_properties_by_ids(self, property_ids: List[int], save_to_db: bool = True) -> List[Dict]:
        """
        Fast path: rescore only specific properties.
        Used by the ingestion-time hook after a scraper run.
        """
        return self.score_all_properties(save_to_db=save_to_db, property_ids=property_ids)

    # ── Query helpers ──────────────────────────────────────────────────────────

    def get_saved_qualified_scores(self, min_score: float = ROUTING_THRESHOLDS["weekly"]):
        """Return saved DistressScore records at or above min_score, ordered by score desc."""
        try:
            return self.session.query(DistressScore).options(
                joinedload(DistressScore.property).joinedload(Property.owner),
                joinedload(DistressScore.property).joinedload(Property.financial),
            ).filter(
                DistressScore.final_cds_score >= min_score
            ).order_by(DistressScore.final_cds_score.desc()).all()
        except OperationalError:
            logger.error("Database error in get_saved_qualified_scores", exc_info=True)
            raise

    def get_saved_scores_by_lead_tier(self, lead_tier: str):
        """Return saved DistressScore records matching a lead tier."""
        try:
            return self.session.query(DistressScore).options(
                joinedload(DistressScore.property).joinedload(Property.owner),
            ).filter(
                DistressScore.lead_tier == lead_tier
            ).order_by(DistressScore.final_cds_score.desc()).all()
        except OperationalError:
            logger.error("Database error in get_saved_scores_by_lead_tier(tier=%s)", lead_tier, exc_info=True)
            raise

    def get_latest_score_for_property(self, property_id: int) -> Optional[DistressScore]:
        """Get the most recent DistressScore record for a property."""
        try:
            return self.session.query(DistressScore).filter(
                DistressScore.property_id == property_id,
            ).order_by(DistressScore.score_date.desc()).first()
        except OperationalError:
            logger.error("Database error in get_latest_score_for_property(id=%s)", property_id, exc_info=True)
            raise

    def get_todays_score_for_property(self, property_id: int) -> Optional[DistressScore]:
        """Get today's DistressScore for a property (if it exists)."""
        from sqlalchemy import cast, Date as SADate
        try:
            return self.session.query(DistressScore).filter(
                DistressScore.property_id == property_id,
                cast(DistressScore.score_date, SADate) == date.today(),
            ).first()
        except OperationalError:
            logger.error("Database error in get_todays_score_for_property(id=%s)", property_id, exc_info=True)
            raise


# ── CLI entry point ────────────────────────────────────────────────────────────

def main():
    """
    Entry point for CLI / cron execution.

    Usage:
        python -m src.services.cds_engine                      # daily run (all properties)
        python -m src.services.cds_engine --rescore-all        # rescore all after weight change
        python -m src.services.cds_engine --property-id 12345  # rescore single property

    Exit codes:
        0 — success
        1 — database / infrastructure error (retryable)
        2 — configuration error (do not retry — fix config first)
        3 — unhandled / unexpected error
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
    parser.add_argument(
        "--no-ghl",
        action="store_true",
        help="Skip GHL CRM push (useful for bulk rescores to avoid rate limits)",
    )
    args = parser.parse_args()

    if args.no_ghl:
        global _GHL_PUSH_ENABLED
        _GHL_PUSH_ENABLED = False
        log.info("[GHL] Push disabled via --no-ghl flag")

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
    log.info("Mode:    %s", run_label)
    log.info("Started: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    interrupted = False
    scores = []

    try:
        with get_db_context() as session:
            scorer = MultiVerticalScorer(session)
            scores = scorer.score_all_properties(save_to_db=True, property_ids=property_ids)
            session.commit()

    except KeyboardInterrupt:
        interrupted = True
        log.warning("Interrupted by operator — partial results may have been committed")
        # Fall through to stats output so the operator sees what ran before interrupt

    except (OperationalError, SQLAlchemyError) as exc:
        log.error("Database error — scoring aborted: %s", exc, exc_info=True)
        sys.exit(1)

    except (KeyError, ValueError, RuntimeError) as exc:
        log.error("Configuration error — scoring aborted: %s", exc, exc_info=True)
        sys.exit(2)

    except Exception as exc:
        log.error("Unexpected error — scoring aborted: %s", exc, exc_info=True)
        sys.exit(3)

    # ── Stats output ──────────────────────────────────────────────────────────
    total = len(scores)
    with_signals = [s for s in scores if s["signal_count"] > 0]
    qualified = sum(1 for s in with_signals if s["qualified"])

    log.info("=" * 60)
    log.info("CDS SCORING COMPLETE%s", " (INTERRUPTED)" if interrupted else "")
    log.info("  Properties loaded:   %7d", total)
    log.info("  With signals:        %7d", len(with_signals))
    log.info("  No signals (skipped):%7d", total - len(with_signals))
    log.info("  Qualified (≥%s):       %7d", ROUTING_THRESHOLDS["weekly"], qualified)

    if with_signals:
        scores_only = [s["final_cds_score"] for s in with_signals]
        avg = sum(scores_only) / len(scores_only)
        top = max(scores_only)
        log.info("  Avg score:           %7.1f", avg)
        log.info("  Top score:           %7.1f", top)

        # ── Lead tier distribution ────────────────────────────────────────
        log.info("")
        log.info("LEAD TIER DISTRIBUTION:")
        tier_counts = Counter(s["lead_tier"] for s in with_signals)
        for tier in ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]:
            bar = "█" * min(30, tier_counts.get(tier, 0))
            log.info("  %-15s %5d  %s", tier, tier_counts.get(tier, 0), bar)

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
            log.info("  %-10s %-18s %5d", urgency, label, urgency_counts.get(urgency, 0))

        # ── Vertical driving max score ────────────────────────────────────
        log.info("")
        log.info("TOP VERTICAL (driving final_cds_score):")
        top_v_counts = Counter(
            max(s["vertical_scores"], key=s["vertical_scores"].get)
            for s in with_signals
        )
        for v, count in top_v_counts.most_common():
            bar = "█" * min(30, count)
            log.info("  %-20s %5d  %s", v, count, bar)

        # ── Signal type frequency ─────────────────────────────────────────
        log.info("")
        log.info("SIGNAL TYPE FREQUENCY (properties carrying each type):")
        sig_counts = Counter(t for s in with_signals for t in s["distress_types"])
        for sig_type, count in sig_counts.most_common():
            bar = "█" * min(30, count)
            log.info("  %-25s %5d  %s", sig_type, count, bar)

        # ── Top 10 scored properties ──────────────────────────────────────
        log.info("")
        log.info("TOP 10 SCORED PROPERTIES:")
        log.info("  %-20s %6s %-15s %-10s %-20s %s", "Parcel", "Score", "Tier", "Urgency", "Best Vertical", "Signals")
        log.info("  %s %s %s %s %s %s", "-"*20, "-"*6, "-"*15, "-"*10, "-"*20, "-"*7)
        for s in sorted(with_signals, key=lambda x: x["final_cds_score"], reverse=True)[:10]:
            best_v = max(s["vertical_scores"], key=s["vertical_scores"].get)
            best_v_score = s["vertical_scores"][best_v]
            log.info(
                "  %-20s %6.1f %-15s %-10s %s(%.0f)  [%d signals]",
                s.get("parcel_id") or "N/A",
                s["final_cds_score"],
                s["lead_tier"],
                s["urgency_level"],
                best_v, best_v_score,
                s["signal_count"],
            )

    log.info("")
    log.info("Finished: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    if interrupted:
        sys.exit(130)  # conventional exit code for SIGINT


if __name__ == "__main__":
    main()
