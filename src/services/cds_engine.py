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
LegalAndLien    → judgment_liens, code_lien (unified TCL/CCL/Pinellas CODE LIEN),
                  hoa_liens, mechanics_liens, irs_tax_liens
Deed            → deed_transfers
LegalProceeding → probate, evictions, bankruptcy
TaxDelinquency  → tax_delinquencies
Foreclosure     → foreclosures
BuildingPermit  → building_permits
"""

import heapq
import json
import logging
import sys
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from types import SimpleNamespace
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.services.ghl_webhook import push_lead_to_ghl
from src.services.heuristic_loader import load_overrides as _load_weight_overrides
from config.settings import settings

# Can be overridden at runtime via --no-ghl CLI flag; default comes from GHL_PUSH_ENABLED env var
_GHL_PUSH_ENABLED: bool = settings.ghl_push_enabled

# Property loading batch size — keyset pagination chunk for full-table scoring runs
_BULK_BATCH_SIZE: int = 500

import psycopg2.extras
from sqlalchemy import text as sa_text
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
    CONTACT_PHONE_BONUS_BY_CONFIDENCE,
    DAYS_OPEN_MODIFIERS,
    EQUITY_BONUS_BY_VERTICAL,
    EQUITY_HIGH_THRESH,
    EQUITY_MID_THRESH,
    LONG_TENURE_YEARS,
    TENURE_EQUITY_BONUS,
    HCPA_AGE_YEARS,
    HCPA_LONG_TERM_YEARS,
    HCPA_PASSIVE_WEIGHTS,
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
    SIGNAL_HARD_CUTOFF_DAYS,
    STACKING_BONUS_CAP,
    STACKING_BONUS_PER_SIGNAL,
    STACKING_ONLY_SIGNALS,
    OWNER_OCCUPIED_EXCLUSION_VERTICALS,
    DEAD_LEAD_DEED_DAYS,
    STACKING_MIN_WEIGHT,
    STACKING_WINDOW_DAYS,
    VERTICAL_WEIGHTS,
    # Stage 2/3 — per-county overrides + signal-coverage normalizer
    for_county,
    signal_coverage_pct,
)

logger = logging.getLogger(__name__)

# ── LegalAndLien.document_type → signal key ───────────────────────────────────
# All code-lien doc-type variants collapse to a single `code_lien` signal so
# Pinellas's bare "CODE LIEN" doc type contributes to scoring just like
# Hillsborough's TCL/CCL variants. Per-county filer identity (Tampa city vs
# unincorporated county) is preserved on the LegalAndLien record itself —
# only the scoring signal name is unified. Previously TCL/CCL produced two
# distinct signal keys whose weights existed globally in config/scoring.py;
# Pinellas's "CODE LIEN" matched neither and silently contributed zero,
# concentrating Pinellas leads in Ultra Platinum from missing-signal-as-
# not-a-deduction bias.
_DOCUMENT_TYPE_TO_SIGNAL: Dict[str, str] = {
    "TAMPA CODE LIENS (TCL)":  "code_lien",
    "COUNTY CODE LIENS (CCL)": "code_lien",
    "CODE LIEN":               "code_lien",
    "HOA LIENS (HL)":          "hoa_liens",
    "MECHANICS LIENS (ML)":    "mechanics_liens",
    "TAX LIENS (TL)":          "irs_tax_liens",
    # Bare "TAX LIEN" variant — present in both Pinellas (24 rows) and
    # Hillsborough (11 rows). Without this entry these rows scored zero.
    "TAX LIEN":                "irs_tax_liens",
}

# ── LegalProceeding.record_type → signal key ─────────────────────────────────
_PROCEEDING_TYPE_TO_SIGNAL: Dict[str, str] = {
    "Probate":    "probate",
    "Eviction":   "evictions",
    "Bankruptcy": "bankruptcy",
    "Divorce":    "divorce_filings",
}

# Guard against empty VERTICAL_WEIGHTS misconfiguration at import time
if not VERTICAL_WEIGHTS:
    raise RuntimeError(
        "VERTICAL_WEIGHTS is empty — check config/scoring.py. "
        "At least one vertical must be configured."
    )


class _Profiler:
    """Per-phase wall-clock timer for scoring runs.

    Off by default → context manager is a zero-cost no-op. Enabled via
    the --profile CLI flag to surface where time is spent across:
      properties_fetch, signals_fetch, score_python, persist_batch,
      commit, ghl_flush.

    NOTE: the original ANY(:ids) → unnest()/temp-table refactor question
    was answered on 2026-05-25 — production runs scale to ~50k IDs per
    batch where ANY() risks suboptimal plans. All 13 hot-path queries
    were converted to `WHERE x IN (SELECT unnest(CAST(:ids AS bigint[])))` which
    gives the planner a known-small driving relation + indexed inner
    lookup. The profiler remains useful for monitoring overall scoring
    cost, but the unnest refactor itself is no longer pending.
    """
    __slots__ = ("enabled", "phases", "start", "_batch_count")

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled
        self.phases: Dict[str, List[float]] = defaultdict(list)
        self.start = time.perf_counter()
        self._batch_count = 0

    @contextmanager
    def phase(self, name: str):
        if not self.enabled:
            yield
            return
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.phases[name].append(time.perf_counter() - t0)

    def mark_batch(self) -> None:
        if self.enabled:
            self._batch_count += 1

    def report(self, log: logging.Logger) -> None:
        if not self.enabled:
            return
        total_wall = time.perf_counter() - self.start
        log.info("")
        log.info("=" * 70)
        log.info("PROFILING REPORT (--profile)")
        log.info("  Wall time:         %7.2f s", total_wall)
        log.info("  Batches processed: %7d", self._batch_count)
        log.info("")
        log.info(
            "  %-20s %7s %9s %10s %10s %7s",
            "Phase", "calls", "total_s", "mean_ms", "max_ms", "%wall",
        )
        log.info("  " + "-" * 68)
        rows = []
        for name, samples in self.phases.items():
            if not samples:
                continue
            total = sum(samples)
            rows.append((total, name, samples))
        rows.sort(reverse=True)  # by total desc
        for total, name, samples in rows:
            mean_ms = (total / len(samples)) * 1000
            max_ms = max(samples) * 1000
            pct = (total / total_wall * 100) if total_wall else 0
            log.info(
                "  %-20s %7d %9.2f %10.1f %10.1f  %5.1f%%",
                name, len(samples), total, mean_ms, max_ms, pct,
            )
        log.info("=" * 70)


class _ScoreRef:
    """Lightweight proxy returned by save_score_to_database for new score rows.

    Only exists so that the Gold flash_scarcity hook can read .id without
    requiring a full ORM-hydrated DistressScore object.
    """
    __slots__ = ("id",)

    def __init__(self, id_: int) -> None:
        self.id = id_


# ── A6: Teaching Correction dampener helpers ──────────────────────────────────
# Pure functions — no DB access, testable in isolation.
# Called from MultiVerticalScorer.score_property() when active corrections exist.

def _filter_signals_for_teaching(
    signals: List[Dict],
    corrections: List[Dict],
) -> List[Dict]:
    """Pre-scoring: drop signals suppressed by active wrong_distress corrections.

    A wrong_distress correction on signal_type=X removes signals of type X whose
    date is not newer than the correction.  If a newer instance of the same signal
    exists, the correction has been overtaken (lifts automatically).

    All other correction reasons leave signals intact; vertical zeroing is handled
    post-scoring by _apply_vertical_dampener().
    """
    from config.closer import CORRECTION_REASON_WRONG_DISTRESS

    wrong_corrections = [
        c for c in corrections
        if c["correction_reason"] == CORRECTION_REASON_WRONG_DISTRESS
    ]
    if not wrong_corrections:
        return signals

    def _as_date(d):
        if d is None:
            return None
        if isinstance(d, datetime):
            return d.date()
        return d

    filtered: List[Dict] = []
    for sig in signals:
        keep = True
        for corr in wrong_corrections:
            if sig["type"] != corr["signal_type"]:
                continue
            sig_date = _as_date(sig.get("date"))
            corr_date = _as_date(corr["created_at"])
            if sig_date is None or corr_date is None or sig_date <= corr_date:
                keep = False
                break
            # sig_date > corr_date → newer instance → correction lifts → keep
        if keep:
            filtered.append(sig)
    return filtered


def _apply_vertical_dampener(
    vertical_scores: Dict[str, float],
    vertical_results: Dict[str, Dict],
    corrections: List[Dict],
    signals: List[Dict],
) -> None:
    """Post-scoring: zero verticals for non_residential and owner_not_motivated.

    Mutates vertical_scores and vertical_results in place (mirrors existing gate
    pattern in score_property).  wrong_distress suppression is handled pre-scoring
    by _filter_signals_for_teaching; bad_contact/other produce no dampener.

    non_residential dominates — if present, all verticals zeroed, short-circuits.
    owner_not_motivated lifts when any signal is newer than the correction.
    """
    from config.closer import (
        CORRECTION_REASON_NON_RESIDENTIAL,
        CORRECTION_REASON_OWNER_NOT_MOTIVATED,
    )

    def _as_date(d):
        if d is None:
            return None
        if isinstance(d, datetime):
            return d.date()
        return d

    has_non_residential = any(
        c["correction_reason"] == CORRECTION_REASON_NON_RESIDENTIAL
        for c in corrections
    )
    if has_non_residential:
        for v in list(vertical_scores):
            vertical_scores[v] = 0.0
            if v in vertical_results:
                vertical_results[v]["score"] = 0.0
        return

    for corr in corrections:
        if corr["correction_reason"] != CORRECTION_REASON_OWNER_NOT_MOTIVATED:
            continue
        corr_date = _as_date(corr["created_at"])
        has_newer = corr_date is not None and any(
            _as_date(s.get("date")) is not None and _as_date(s.get("date")) > corr_date
            for s in signals
        )
        if has_newer:
            continue
        for v in OWNER_OCCUPIED_EXCLUSION_VERTICALS:
            vertical_scores[v] = 0.0
            if v in vertical_results:
                vertical_results[v]["score"] = 0.0


class MultiVerticalScorer:
    """
    6-vertical CDS scoring engine.

    Scores properties at ingestion time using all 14 real-time signal sources.
    Weights are driven entirely by config/scoring.py — no code rebuild required
    when tuning.
    """

    # Live destination table. Shadow rescore runs (Stage E of the cross-county
    # retune) swap this to "distress_scores_shadow" so the live feed, GHL push,
    # and Cora flows are unaffected while the proposed weights are being
    # evaluated. The schema of the two tables is identical (see migration
    # fa032_distress_scores_shadow.py), so every SQL string below references
    # this attribute via f-strings and routes transparently.
    _scores_table_name: str = "distress_scores"

    # When True, side effects that mutate live operational state are skipped:
    # the sync_status='pending_sync' bulk UPDATE and the GHL push queue. Set
    # by the CLI when --shadow is passed.
    _shadow_mode: bool = False

    def __init__(self, session: Session):
        self.session = session
        self._ghl_push_queue: List[Dict] = []
        self._total_scored: int = 0
        # Default no-op profiler; CLI replaces with enabled instance under --profile.
        self._profiler: _Profiler = _Profiler(enabled=False)
        # A3: warm-start priors — additive deltas loaded from scoring_weight_overrides (5-min cache).
        self._weight_overrides: Dict[tuple, float] = _load_weight_overrides(session)

    # ── GHL batch flush ───────────────────────────────────────────────────────

    def _flush_ghl_queue(self) -> None:
        """
        Mark queued leads for async GHL sync by setting sync_status='pending_sync'.

        Previously this called push_lead_to_ghl() synchronously for every lead,
        which caused two compounding problems:
          1. Thousands of sequential GHL API calls (4 calls × 0.5s each per lead)
             blocked the process for hours after scoring finished.
          2. Each contact write opened a separate DB session and committed an
             individual UPDATE on the properties table — creating thousands of
             long-running transactions that blocked all other scrapers.

        Now the flush is a single batch SQL UPDATE (milliseconds). The actual
        GHL API calls are handled by src/tasks/ghl_sync.py, which runs as a
        separate process after scraping completes and commits DB writes in
        batches of 100.
        """
        queue = self._ghl_push_queue
        if not queue:
            return

        # Stage E — shadow runs don't touch live operational state. Skip the
        # bulk pending_sync UPDATE so subscriber feeds and GHL sync don't pick
        # up shadow-only scores.
        if self._shadow_mode:
            self._ghl_push_queue.clear()
            logger.debug("[shadow] sync_status update suppressed; cleared %d queued leads", len(queue))
            return

        property_ids = [sd["property_id"] for sd in queue if sd.get("property_id")]
        if property_ids:
            # Chunk into batches of 1000 — avoids unbounded IN-list that degrades
            # PostgreSQL query planning and can lock large table ranges.
            _CHUNK = 1000
            for i in range(0, len(property_ids), _CHUNK):
                chunk = property_ids[i : i + _CHUNK]
                self.session.execute(
                    sa_text(
                        "UPDATE properties SET sync_status = 'pending_sync' "
                        "WHERE id IN (SELECT unnest(CAST(:ids AS bigint[])))"
                    ),
                    {"ids": chunk},
                )
            logger.info(
                "[GHL] Marked %d properties as pending_sync for async push "
                "(run `python -m src.tasks.ghl_sync` to flush to CRM)",
                len(property_ids),
            )
        self._ghl_push_queue.clear()

    # ── Signal collection ─────────────────────────────────────────────────────

    def _collect_signals(self, prop: Property) -> List[Dict]:
        """
        Gather all distress signals for a property from all 14 sources.

        Returns a flat list of:
            {"type": str, "date": date|None, "amount": float|None}
        """
        signals: List[Dict] = []

        # 1. Code violations — skip resolved/closed violations (no longer active distress).
        #    PERSISTENCE_RESOLVED_KEYWORDS covers most cases; the supplemental set below
        #    catches statuses whose wording doesn't substring-match any keyword
        #    (e.g. "In Compliance" ≠ "complied", "No Violation" has no keyword match).
        _SUPPLEMENTAL_RESOLVED = frozenset({"no violation", "compliance"})
        for v in (prop.code_violations or []):
            if v.status:
                s = v.status.lower()
                if any(kw in s for kw in PERSISTENCE_RESOLVED_KEYWORDS):
                    continue
                if any(kw in s for kw in _SUPPLEMENTAL_RESOLVED):
                    continue
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

        # 3. Deed transfers — skip mortgage instruments (a recorded mortgage/refinance
        #    is debt, not an ownership change) and nominal/intra-family transfers (< $1,000)
        for deed in (prop.deeds or []):
            if getattr(deed, "mortgage_amount", None) is not None:
                continue
            if deed.sale_price is not None and deed.sale_price < 1000:
                continue
            signals.append({"type": "deed_transfers", "date": deed.record_date, "amount": deed.sale_price})

        # 4. Legal proceedings (probate / eviction / bankruptcy)
        for proc in (prop.legal_proceedings or []):
            sig_type = _PROCEEDING_TYPE_TO_SIGNAL.get(proc.record_type)
            if not sig_type:
                continue
            # Skip "Wills on Deposit" — not an active estate proceeding
            if sig_type == "probate" and proc.case_status and "wills on deposit" in proc.case_status.lower():
                continue
            # Eviction direction check: only score as distress when the property
            # OWNER is the defendant (being evicted from their own home).
            # Landlord-vs-tenant filings (owner evicting a tenant) are routine
            # property management — not an owner-in-distress signal.
            if sig_type == "evictions":
                owner = prop.owner
                if owner and owner.owner_name:
                    def _tokens(s: str):
                        import re
                        return {t for t in re.sub(r"[^a-z\s]", "", s.lower()).split() if len(t) > 2}
                    # No party info → direction unknown → skip to avoid false positives.
                    # (previously null associated_party bypassed this check entirely,
                    # causing all null-party evictions to score as owner-distress.)
                    if not proc.associated_party:
                        continue
                    if not (_tokens(proc.associated_party) & _tokens(owner.owner_name)):
                        continue  # defendant is a tenant, not the property owner
            signals.append({"type": sig_type, "date": proc.filing_date, "amount": proc.amount})

        # 5. Tax delinquencies — skip null rows (scraper placeholder with no real data)
        for tax in (prop.tax_delinquencies or []):
            if tax.total_amount_due is None and tax.years_delinquent is None:
                continue
            sig_date = tax.deed_app_date or tax.date_added
            signals.append({"type": "tax_delinquencies", "date": sig_date, "amount": tax.total_amount_due})

        # 6. Foreclosures — use lis_pendens_date as fallback when filing_date is absent
        #    (LP-only rows created by lis_pendens loader have filing_date=None)
        for fc in (prop.foreclosures or []):
            sig_date = fc.filing_date or fc.lis_pendens_date
            signals.append({"type": "foreclosures", "date": sig_date, "amount": fc.judgment_amount})

        # 7. Building permits — enforcement permits get their own higher-weighted signal type.
        #    Skip non-enforcement permits whose work is already complete (no ongoing distress).
        _CLOSED_PERMIT_STATUSES = {"issued", "final", "finaled", "closed", "completed"}
        for bp in (prop.building_permits or []):
            if (
                not bp.is_enforcement_permit
                and bp.status
                and bp.status.strip().lower() in _CLOSED_PERMIT_STATUSES
            ):
                continue
            sig_type = "enforcement_permit" if bp.is_enforcement_permit else "building_permits"
            signals.append({"type": sig_type, "date": bp.issue_date, "amount": None})

        # 8. Incidents (insurance_claim, fire, storm_damage, flood_damage)
        _INCIDENT_SIGNAL_TYPES = {"insurance_claim", "Fire", "storm_damage", "flood_damage"}
        for inc in (prop.incidents or []):
            if inc.incident_type in _INCIDENT_SIGNAL_TYPES:
                signals.append({"type": inc.incident_type, "date": inc.incident_date, "amount": None})

        # Hard expiry: drop signals whose date is known and older than SIGNAL_HARD_CUTOFF_DAYS.
        # Signals without a date are kept — we cannot determine their age.
        # This prevents resolved/abandoned records from keeping a property in Gold+ indefinitely.
        _cutoff = date.today() - timedelta(days=SIGNAL_HARD_CUTOFF_DAYS)
        signals = [
            s for s in signals
            if s["date"] is None
            or (s["date"].date() if isinstance(s["date"], datetime) else s["date"]) >= _cutoff
        ]

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
        missing_signals: frozenset = frozenset(),
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

        `missing_signals` (Stage D — feature suppression): signal types the
        property's county does not load. These are excluded entirely from
        scoring — they cannot be the primary signal AND don't contribute to
        the stacking count. Treating them as "absent" (zero contribution but
        still counted) caused the Pinellas inversion: a Pinellas lead with
        one strong primary signal scored at the engine cap because the
        engine considered "no code_violation = clean property" rather than
        "no code_violation = we don't observe this for this county."
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
        # Stage D: signals listed in missing_signals are skipped entirely. These are
        # signal types this county does not load (e.g. Pinellas lacks code_violations),
        # so any value treated as "absent" would be a false zero.
        latest_by_type: Dict[str, Dict] = {}
        for sig in signals:
            sig_type = sig["type"]
            if sig_type not in weights:
                continue
            if sig_type in missing_signals:
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
                "tenure_bonus":          0,
                "days_open_mod":         0,
                "persistence_mod":       0,
                "prior_viol_mod":        0,
            }

        # Guard: if ALL signals are stacking-only, this property cannot be scored.
        # Stacking-only signals (insurance_claim, fire, storm_damage, flood_damage)
        # require a primary signal to be present before they contribute.
        primary_signal_types = set(latest_by_type.keys()) - STACKING_ONLY_SIGNALS
        if not primary_signal_types:
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
                "tenure_bonus":          0,
                "days_open_mod":         0,
                "persistence_mod":       0,
                "prior_viol_mod":        0,
            }

        # Build per-signal components — apply age decay to each.
        # Stacking-only signals (incidents) are computed for the component map
        # but are EXCLUDED from primary (best_type) selection — they can never
        # be the primary signal even if their recency-boosted score is higher.
        signal_components: Dict[str, Dict] = {}
        best_type  = None
        best_total = -999
        for sig_type, sig_info in latest_by_type.items():
            sig_date = sig_info["date"]
            _delta  = self._weight_overrides.get((vertical, sig_type), 0.0)
            base    = max(0, min(100, weights[sig_type] + _delta))
            recency = self._recency_bonus(sig_date)
            decay   = self._age_decay(sig_date)
            total   = base + recency + decay   # decay is negative
            signal_components[sig_type] = {
                "base":      base,
                "recency":   recency,
                "age_decay": decay,
                "total":     total,
            }
            # Only non-stacking-only signals compete for primary
            if sig_type not in STACKING_ONLY_SIGNALS and total > best_total:
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

        # Stacking: count only signals that are meaningful for this vertical.
        # Exclude stacking-only types (building_permits, fire, storm/flood) — they
        # are context signals, not independent distress events, and must not inflate
        # the stacking count. Also exclude low-weight filler signals (weight < 30,
        # e.g. insurance_claim at 10 in roofing/restoration) — they represent
        # tangential correlation, not genuine co-occurring distress.
        signals_within_window = sum(
            1 for sig_type, si in latest_by_type.items()
            if sig_type not in STACKING_ONLY_SIGNALS
            and weights.get(sig_type, 0) >= STACKING_MIN_WEIGHT
            and si["date"] and (today - si["date"]).days <= STACKING_WINDOW_DAYS
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
                # getattr: bulk path builds owners as SimpleNamespace from a
                # column list — older bundles may lack the label entirely.
                confidence = getattr(owner, "contact_info_confidence", None)
                if settings.cds_use_contactability and confidence:
                    contact_bonus += CONTACT_PHONE_BONUS_BY_CONFIDENCE.get(
                        confidence, CONTACT_PHONE_BONUS
                    )
                else:
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

        tenure_bonus = 0
        if owner and owner.ownership_years is not None:
            try:
                if float(owner.ownership_years) >= LONG_TENURE_YEARS:
                    tenure_bonus = TENURE_EQUITY_BONUS
            except (TypeError, ValueError):
                pass

        final_score = min(
            primary_score + days_open_mod + persistence_mod + prior_viol_mod
            + stacking_bonus + absentee_bonus + contact_bonus + equity_bonus + tenure_bonus,
            float(SCORE_CAP),
        )

        logger.debug(
            "    [%s] primary=%s(%.0f) days_open=+%d persistence=+%d prior_viol=+%d"
            " stack=+%d(%d sigs/%dd) absentee=+%d contact=+%d equity=+%d tenure=+%d → %.1f",
            vertical, best_type, primary_score,
            days_open_mod, persistence_mod, prior_viol_mod,
            stacking_bonus, signals_within_window, STACKING_WINDOW_DAYS,
            absentee_bonus, contact_bonus, equity_bonus, tenure_bonus,
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
            "tenure_bonus":          tenure_bonus,
            "days_open_mod":         days_open_mod,
            "persistence_mod":       persistence_mod,
            "prior_viol_mod":        prior_viol_mod,
        }

    # ── Score a single property ────────────────────────────────────────────────

    def _load_all_teaching_corrections(
        self,
        property_ids: Optional[List[int]] = None,
    ) -> Dict[int, List[Dict]]:
        """Load all active teaching corrections, keyed by property id.

        If property_ids is given, restricts to those IDs (avoids full-table scan
        for targeted single/batch rescores).  Called once per scoring run to avoid
        N+1 queries across the property batch.
        """
        from collections import defaultdict as _defaultdict
        where = "WHERE subject_type = 'property' AND dampener_active = TRUE"
        params: Dict = {}
        if property_ids:
            where += " AND subject_ref = ANY(:pids)"
            params["pids"] = [str(pid) for pid in property_ids]

        try:
            rows = self.session.execute(
                sa_text(
                    f"SELECT subject_ref, correction_reason, signal_type, created_at"
                    f" FROM cora_training_overrides {where}"
                ),
                params,
            ).fetchall()
        except Exception:
            # Table may not exist yet (pre-migration environment) — degrade gracefully.
            logger.debug("cora_training_overrides not yet available — skipping dampener")
            return {}

        result: Dict[int, List[Dict]] = _defaultdict(list)
        for r in rows:
            result[int(r.subject_ref)].append({
                "correction_reason": r.correction_reason,
                "signal_type": r.signal_type,
                "created_at": r.created_at,
            })
        return dict(result)

    def score_property(
        self,
        prop: Property,
        teaching_corrections: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Score a property across all 6 verticals with a 2-signal routing gate.

        teaching_corrections: pre-loaded list of active CoraTrainingOverride rows
        for this property (as dicts).  If None, loads from DB (standalone calls).
        Pass an empty list to skip DB fetch when no corrections exist for the batch.
        """
        if teaching_corrections is None:
            teaching_corrections = self._load_all_teaching_corrections(
                [prop.id]
            ).get(prop.id, [])

        signals = self._collect_signals(prop)

        # Pre-scoring: filter signals suppressed by wrong_distress corrections.
        if teaching_corrections:
            signals = _filter_signals_for_teaching(signals, teaching_corrections)
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

        # Stage D — resolve county config once and pass missing_signals into
        # every per-vertical scoring call. Missing signals are dropped before
        # primary selection and stacking counts; the multiplicative coverage
        # discount that used to live at the end of this function is gone.
        _cfg = for_county(prop.county_id)
        _missing = _cfg.missing_signals

        vertical_results = {
            v: self._score_vertical(
                v, signals, owner, financial, violation_count, persistence_data,
                missing_signals=_missing,
            )
            for v in VERTICAL_WEIGHTS
        }
        vertical_scores = {v: r["score"] for v, r in vertical_results.items()}

        # Cross-sell suppression: if the property already has a routine roofing permit
        # (is_enforcement_permit=False), the owner is actively fixing the roof and is
        # NOT a roofing lead. Zero out roofing vertical score.
        # Enforcement roofing permits (stop work, after-the-fact, etc.) still score positively.
        _routine_roofing = any(
            not bp.is_enforcement_permit
            and bp.permit_type
            and "roof" in bp.permit_type.lower()
            for bp in (prop.building_permits or [])
        )
        if _routine_roofing:
            vertical_scores["roofing"] = 0.0
            if "roofing" in vertical_results:
                vertical_results["roofing"]["score"] = 0.0

        # Dead lead gate (per-vertical): a property sold within DEAD_LEAD_DEED_DAYS is
        # off-market for investment verticals — the new owner won't resell immediately.
        # Contractor verticals (roofing, restoration, public_adjusters) are unaffected —
        # the new owner may still need physical work done on the property.
        _deed_cutoff = date.today() - timedelta(days=DEAD_LEAD_DEED_DAYS)
        _has_recent_deed = any(
            s["type"] == "deed_transfers"
            and s["date"] is not None
            and (s["date"].date() if isinstance(s["date"], datetime) else s["date"]) >= _deed_cutoff
            for s in signals
        )
        # Fallback: check raw deeds directly — catches two bypass cases:
        #   1. Nominal-price transfers (sale_price < $1,000) excluded from signals as
        #      non-arm's-length but still represent a recent ownership change.
        #   2. Deeds with record_date=None in the signal dict (date check fails silently).
        if not _has_recent_deed and prop.deeds:
            for _deed in prop.deeds:
                if getattr(_deed, "mortgage_amount", None) is not None:
                    continue  # mortgage instrument, not an ownership transfer
                if _deed.record_date is None:
                    continue
                _rd = _deed.record_date.date() if isinstance(_deed.record_date, datetime) else _deed.record_date
                if _rd >= _deed_cutoff:
                    _has_recent_deed = True
                    break
        if _has_recent_deed:
            for v in OWNER_OCCUPIED_EXCLUSION_VERTICALS:
                vertical_scores[v] = 0.0
                if v in vertical_results:
                    vertical_results[v]["score"] = 0.0

        # Owner-occupied suppression: zero out investment verticals if the owner
        # lives at the property (mailing_address == property address).
        # Contractor verticals (restoration, roofing, public_adjusters) are unaffected —
        # homeowners are valid leads for contractors.
        _owner_occupied = (
            owner is not None
            and owner.mailing_address is not None
            and prop.address is not None
            and owner.mailing_address.strip().lower() == prop.address.strip().lower()
        )
        if _owner_occupied:
            for v in OWNER_OCCUPIED_EXCLUSION_VERTICALS:
                vertical_scores[v] = 0.0
                if v in vertical_results:
                    vertical_results[v]["score"] = 0.0

        # Teaching Correction dampener (A6): non_residential / owner_not_motivated.
        # wrong_distress was already handled pre-scoring via _filter_signals_for_teaching.
        if teaching_corrections:
            _apply_vertical_dampener(
                vertical_scores, vertical_results, teaching_corrections, signals
            )

        # HCPA passive signal bonuses — boost verticals that already have a primary signal.
        # These never act as primary signals; they only add weight when a vertical is already scored.
        today_year = date.today().year
        _hcpa_passive: List[str] = []

        if prop.year_built and prop.year_built < (today_year - HCPA_AGE_YEARS):
            _hcpa_passive.append("property_age_30plus")

        fin = prop.financial
        if fin and fin.last_sale_date:
            _sale_date = fin.last_sale_date
            if isinstance(_sale_date, datetime):
                _sale_date = _sale_date.date()
            if (date.today() - _sale_date).days > HCPA_LONG_TERM_YEARS * 365:
                _hcpa_passive.append("long_term_owner")

        if fin and fin.value_change_yoy is not None:
            try:
                if float(fin.value_change_yoy) < 0:
                    _hcpa_passive.append("declining_value")
            except (TypeError, ValueError):
                pass

        for passive_sig in _hcpa_passive:
            per_vertical = HCPA_PASSIVE_WEIGHTS.get(passive_sig, {})
            for v, bonus in per_vertical.items():
                if vertical_scores.get(v, 0.0) > 0.0:
                    vertical_scores[v] = min(float(SCORE_CAP), vertical_scores[v] + bonus)
                    if v in vertical_results:
                        vertical_results[v]["score"] = vertical_scores[v]

        if _hcpa_passive:
            logger.debug("  HCPA passive signals: %s", _hcpa_passive)

        # Stage D — missing-signal handling now happens upstream in
        # _score_vertical via the missing_signals frozenset. Those signal
        # types are dropped before primary selection and stacking counts, so
        # there is no multiplicative discount to apply here anymore. The
        # signal_coverage_pct function in config/scoring.py is unused and
        # will be removed at the Stage F cutover.

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
            "county_id":       prop.county_id,
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
            "factor_scores":   self._build_factor_scores(
                signals, vertical_results,
                contact_info_confidence=getattr(owner, "contact_info_confidence", None),
            ),
            # Skip expensive summary/estimation work for zero-signal properties —
            # these fields are only consumed by the CRM push path (qualified leads).
            "signal_summaries": self._build_signal_summaries(prop) if signals else {},
            "est_job_value":    self._estimate_job_value(prop, signals) if signals else {
                "low": 0, "high": 0, "display": "N/A", "method": "skipped"
            },
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

    def _estimate_job_value(self, prop: "Property", signals: List[Dict]) -> Dict:
        """Estimate job value using signal types and property specs."""
        try:
            from src.services.job_estimator import estimate_job_value
            distress_types = list({s["type"] for s in signals})
            # Use the highest-scoring vertical for this property
            return estimate_job_value(prop, distress_types)
        except Exception:
            logger.debug("Job value estimation failed for property %s", prop.id, exc_info=True)
            return {"low": 0, "high": 0, "display": "N/A", "method": "error"}

    def _build_factor_scores(self, signals: List[Dict], vertical_results: Dict,
                             contact_info_confidence: Optional[str] = None) -> Dict:
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
            "signals":                  signal_list,
            "vertical_breakdown":       vertical_breakdown,
            "contact_info_confidence":  contact_info_confidence,
        }

    # ── Database persistence ───────────────────────────────────────────────────

    def save_score_to_database(self, score_data: Dict, upsert: bool = True, scoring_run_id: Optional[int] = None):
        """
        UPSERT a DistressScore record using raw SQL for performance.

        - If a score already exists for this property today → update it (raw SQL UPDATE).
        - If not, check the most recent score; if unchanged → skip.
        - Otherwise → INSERT via raw SQL RETURNING id (no ORM flush needed).

        Returns a tuple (record_or_None, status, upgraded) where status is one of:
            'new'       — first-ever score for this property today
            'updated'   — existing today's record was refreshed
            'unchanged' — score identical to last recorded; skipped
        Raises SQLAlchemyError on DB failure — caller must handle rollback.
        """
        property_id   = score_data["property_id"]
        final_score   = score_data["final_cds_score"]
        lead_tier     = score_data["lead_tier"]
        urgency       = score_data["urgency_level"]
        qualified     = score_data["qualified"]
        factor_json   = score_data["factor_scores"]
        vertical_json = score_data["vertical_scores"]
        distress_list = score_data["distress_types"]
        today         = date.today()
        now           = datetime.now(timezone.utc)

        # Range filter instead of CAST(score_date AS DATE) — allows the composite
        # index on (property_id, score_date DESC) to be used without a function call.
        today_start    = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        tomorrow_start = today_start + timedelta(days=1)

        # --- Today's score lookup (raw SQL, hits composite index) ---
        existing_row = None
        if upsert:
            existing_row = self.session.execute(
                sa_text(f"""
                    SELECT id, final_cds_score, lead_tier
                    FROM {self._scores_table_name}
                    WHERE property_id = :pid
                      AND score_date >= :start
                      AND score_date  < :end
                    LIMIT 1
                """),
                {"pid": property_id, "start": today_start, "end": tomorrow_start},
            ).first()

        if existing_row:
            try:
                prev_score = float(existing_row.final_cds_score) if existing_row.final_cds_score is not None else 0.0
            except (TypeError, ValueError):
                prev_score = 0.0

            prev_tier     = existing_row.lead_tier
            score_changed = prev_score != float(final_score)

            # Raw SQL UPDATE — no ORM object load, no per-row flush
            self.session.execute(
                sa_text(f"""
                    UPDATE {self._scores_table_name} SET
                        score_date      = :now,
                        final_cds_score = :score,
                        lead_tier       = :tier,
                        urgency_level   = :urgency,
                        qualified       = :qualified,
                        factor_scores   = CAST(:factor AS jsonb),
                        vertical_scores = CAST(:vertical AS jsonb),
                        distress_types  = CAST(:distress AS jsonb),
                        scoring_run_id  = :run_id
                    WHERE id = :id
                """),
                {
                    "now":      now,
                    "score":    final_score,
                    "tier":     lead_tier,
                    "urgency":  urgency,
                    "qualified": qualified,
                    "factor":   json.dumps(factor_json),
                    "vertical": json.dumps(vertical_json),
                    "distress": json.dumps(distress_list),
                    "run_id":   scoring_run_id,
                    "id":       existing_row.id,
                },
            )
            logger.debug(
                "Updated score for property %s: %.2f (%s)",
                score_data.get("parcel_id"), final_score, lead_tier,
            )
            is_new_contact = not score_data.get("ghl_contact_id")
            if _GHL_PUSH_ENABLED and (score_changed or is_new_contact):
                self._ghl_push_queue.append(score_data)
            _TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
            upgraded = (
                prev_tier in _TIER_ORDER and lead_tier in _TIER_ORDER
                and _TIER_ORDER.index(lead_tier) < _TIER_ORDER.index(prev_tier)
            )
            return None, 'updated', upgraded

        # --- Latest historical score lookup (raw SQL, hits composite index) ---
        latest_row = self.session.execute(
            sa_text(f"""
                SELECT final_cds_score, lead_tier
                FROM {self._scores_table_name}
                WHERE property_id = :pid
                ORDER BY score_date DESC
                LIMIT 1
            """),
            {"pid": property_id},
        ).first()

        try:
            latest_score = float(latest_row.final_cds_score) if latest_row and latest_row.final_cds_score is not None else None
        except (TypeError, ValueError):
            latest_score = None

        latest_tier = latest_row.lead_tier if latest_row else None
        if latest_score is not None and latest_score == float(final_score) and latest_tier == lead_tier:
            logger.debug(
                "Score unchanged for property %s: %.2f — skipping",
                score_data.get("parcel_id"), final_score,
            )
            return None, 'unchanged', False

        # --- Insert new score, get PK via RETURNING (no ORM flush needed) ---
        new_id_row = self.session.execute(
            sa_text(f"""
                INSERT INTO {self._scores_table_name} (
                    property_id, county_id, score_date, final_cds_score,
                    lead_tier, urgency_level, qualified,
                    factor_scores, vertical_scores, distress_types, scoring_run_id
                ) VALUES (
                    :pid, :county, :now, :score,
                    :tier, :urgency, :qualified,
                    CAST(:factor AS jsonb), CAST(:vertical AS jsonb), CAST(:distress AS jsonb), :run_id
                )
                RETURNING id
            """),
            {
                "pid":      property_id,
                "county":   score_data.get("county_id", "hillsborough"),
                "now":      now,
                "score":    final_score,
                "tier":     lead_tier,
                "urgency":  urgency,
                "qualified": qualified,
                "factor":   json.dumps(factor_json),
                "vertical": json.dumps(vertical_json),
                "distress": json.dumps(distress_list),
                "run_id":   scoring_run_id,
            },
        ).first()
        new_id = new_id_row.id if new_id_row else None

        logger.debug(
            "Created score for property %s: %.2f (%s)",
            score_data.get("parcel_id"), final_score, lead_tier,
        )
        if _GHL_PUSH_ENABLED:
            self._ghl_push_queue.append(score_data)

        # Return a lightweight proxy so flash_scarcity can read .id for Gold leads
        # without requiring a full ORM-hydrated DistressScore object.
        return _ScoreRef(new_id) if new_id else None, 'new', False

    # ── Batch scoring ──────────────────────────────────────────────────────────

    #deprecated: not used in Phase 2 — consider removal or repurposing for a future batch scoring mode  

    def _load_properties(
        self,
        property_ids: Optional[List[int]] = None,
        county_id: Optional[str] = None,
    ) -> List[Property]:
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
                joinedload(Property.incidents),
            )
            if property_ids:
                q = q.filter(Property.id.in_(property_ids))
            if county_id:
                q = q.filter(Property.county_id == county_id)
            return q.all()
        except OperationalError as exc:
            logger.error(
                "Database error loading properties for scoring (ids=%s): %s",
                property_ids, exc, exc_info=True,
            )
            raise

    # ── Raw SQL property loading (Phase 2) ───────────────────────────────────

    _PROP_COLS = """
        id, parcel_id, address, city, state, zip, county_id,
        year_built, sq_ft, beds, baths, lot_size, gohighlevel_contact_id
    """

    def _fetch_properties_chunk(
        self,
        last_id: int,
        batch_size: int,
        county_id: Optional[str] = None,
    ) -> list:
        """Keyset pagination: next batch of properties after last_id."""
        with self._profiler.phase("properties_fetch"):
            sql = f"SELECT {self._PROP_COLS} FROM properties WHERE id > :last_id"
            params: dict = {"last_id": last_id, "n": batch_size}
            if county_id:
                sql += " AND county_id = :county"
                params["county"] = county_id
            sql += " ORDER BY id LIMIT :n"
            return self.session.execute(sa_text(sql), params).fetchall()

    def _fetch_properties_by_ids(self, ids: List[int]) -> list:
        """Fetch property rows for a specific list of IDs."""
        with self._profiler.phase("properties_fetch"):
            return self.session.execute(
                sa_text(f"SELECT {self._PROP_COLS} FROM properties WHERE id IN (SELECT unnest(CAST(:ids AS bigint[]))) ORDER BY id"),
                {"ids": ids},
            ).fetchall()

    def _fetch_signals_for_batch(self, property_ids: List[int]) -> "defaultdict":
        """
        10 raw SQL queries for all signal data belonging to a batch of properties.

        Returns a defaultdict keyed by property_id. Each value is a dict with keys:
          owner, financial, code_violations, legal_and_liens, deeds, legal_proceedings,
          tax_delinquencies, foreclosures, building_permits, incidents.
        """
        p = {"ids": property_ids}

        def _q(sql: str) -> list:
            with self._profiler.phase("signals_fetch"):
                return self.session.execute(sa_text(sql), p).fetchall()

        def _ns(row) -> SimpleNamespace:
            return SimpleNamespace(**dict(row._mapping))

        owner_rows = _q("""
            SELECT property_id, owner_name, owner_type, absentee_status, mailing_address,
                   ownership_years, phone_1, phone_2, phone_3, email_1, email_2,
                   contact_info_confidence
            FROM owners WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        fin_rows = _q("""
            SELECT property_id, assessed_value_mkt, homestead_exempt, est_equity,
                   equity_pct, last_sale_price, last_sale_date, value_change_yoy
            FROM financials WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        cv_rows = _q("""
            SELECT property_id, status, violation_type, opened_date, fine_amount
            FROM code_violations WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        lal_rows = _q("""
            SELECT property_id, record_type, document_type, filing_date, amount
            FROM legal_and_liens WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        deed_rows = _q("""
            SELECT property_id, sale_price, record_date, deed_type, mortgage_amount
            FROM deeds WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        lp_rows = _q("""
            SELECT property_id, record_type, case_status, associated_party, filing_date, amount
            FROM legal_proceedings WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        td_rows = _q("""
            SELECT property_id, total_amount_due, years_delinquent, deed_app_date, date_added
            FROM tax_delinquencies WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        fc_rows = _q("""
            SELECT property_id, filing_date, lis_pendens_date, judgment_amount, plaintiff, auction_date
            FROM foreclosures WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        bp_rows = _q("""
            SELECT property_id, is_enforcement_permit, status, issue_date, permit_type
            FROM building_permits WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)
        inc_rows = _q("""
            SELECT property_id, incident_type, incident_date
            FROM incidents WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
        """)

        signal_map: defaultdict = defaultdict(lambda: {
            "owner": None, "financial": None,
            "code_violations": [], "legal_and_liens": [], "deeds": [],
            "legal_proceedings": [], "tax_delinquencies": [], "foreclosures": [],
            "building_permits": [], "incidents": [],
        })

        for row in owner_rows:
            signal_map[row.property_id]["owner"] = _ns(row)
        for row in fin_rows:
            signal_map[row.property_id]["financial"] = _ns(row)
        for row in cv_rows:
            signal_map[row.property_id]["code_violations"].append(_ns(row))
        for row in lal_rows:
            signal_map[row.property_id]["legal_and_liens"].append(_ns(row))
        for row in deed_rows:
            signal_map[row.property_id]["deeds"].append(_ns(row))
        for row in lp_rows:
            signal_map[row.property_id]["legal_proceedings"].append(_ns(row))
        for row in td_rows:
            signal_map[row.property_id]["tax_delinquencies"].append(_ns(row))
        for row in fc_rows:
            signal_map[row.property_id]["foreclosures"].append(_ns(row))
        for row in bp_rows:
            signal_map[row.property_id]["building_permits"].append(_ns(row))
        for row in inc_rows:
            signal_map[row.property_id]["incidents"].append(_ns(row))

        return signal_map

    def _build_property_bundle(self, prop_row, signal_map: "defaultdict") -> SimpleNamespace:
        """Assemble a duck-typed SimpleNamespace that score_property() can consume."""
        pid = prop_row.id
        sigs = signal_map[pid]
        return SimpleNamespace(
            id=prop_row.id,
            parcel_id=prop_row.parcel_id,
            address=prop_row.address,
            city=prop_row.city,
            state=prop_row.state,
            zip=prop_row.zip,
            county_id=prop_row.county_id,
            year_built=prop_row.year_built,
            sq_ft=prop_row.sq_ft,
            beds=prop_row.beds,
            baths=prop_row.baths,
            lot_size=prop_row.lot_size,
            gohighlevel_contact_id=prop_row.gohighlevel_contact_id,
            owner=sigs["owner"],
            financial=sigs["financial"],
            code_violations=sigs["code_violations"],
            legal_and_liens=sigs["legal_and_liens"],
            deeds=sigs["deeds"],
            legal_proceedings=sigs["legal_proceedings"],
            tax_delinquencies=sigs["tax_delinquencies"],
            foreclosures=sigs["foreclosures"],
            building_permits=sigs["building_permits"],
            incidents=sigs["incidents"],
        )

    def _collect_changed_property_ids(
        self,
        county_id: Optional[str] = None,
    ) -> List[int]:
        """
        Return IDs of properties that either have never been scored or have at
        least one signal row with date_added > their latest score_date.

        Uses the (property_id, date_added) composite indexes from fa005 for
        efficient LEFT JOIN lookups instead of correlated EXISTS scans.
        When county_id is supplied the result is further filtered via a JOIN to
        the properties table (uses idx_properties_county_id from fa006).
        """
        _SIGNAL_TABLES = [
            "code_violations",
            "legal_and_liens",
            "deeds",
            "legal_proceedings",
            "tax_delinquencies",
            "foreclosures",
            "building_permits",
            "incidents",
        ]

        union_branches = "\n    UNION ALL\n    ".join(
            f"SELECT t.property_id FROM {tbl} t "
            f"LEFT JOIN latest_scores ls ON ls.property_id = t.property_id "
            f"WHERE ls.property_id IS NULL OR t.date_added > ls.score_date"
            for tbl in _SIGNAL_TABLES
        )
        # Master-data updates don't create signal rows — the weekly master
        # refresh sets properties.needs_rescore instead (fa077). The partial
        # index idx_properties_needs_rescore keeps this branch O(flagged).
        union_branches += (
            "\n    UNION ALL\n    "
            "SELECT p2.id AS property_id FROM properties p2 WHERE p2.needs_rescore"
        )

        if county_id:
            outer = (
                "SELECT DISTINCT s.property_id\n"
                "FROM (\n"
                f"    {union_branches}\n"
                ") s\n"
                "JOIN properties p ON p.id = s.property_id\n"
                "WHERE p.county_id = :county"
            )
            params: dict = {"county": county_id}
        else:
            outer = (
                "SELECT DISTINCT s.property_id\n"
                "FROM (\n"
                f"    {union_branches}\n"
                ") s\n"
                "WHERE s.property_id IS NOT NULL"
            )
            params = {}

        sql = (
            "WITH latest_scores AS (\n"
            "    SELECT DISTINCT ON (property_id)\n"
            "        property_id, score_date\n"
            f"    FROM {self._scores_table_name}\n"
            "    ORDER BY property_id, score_date DESC\n"
            ")\n"
            f"{outer}"
        )

        rows = self.session.execute(sa_text(sql), params).fetchall()
        return [row[0] for row in rows]

    def _iter_property_batches(
        self,
        property_ids: Optional[List[int]] = None,
        county_id: Optional[str] = None,
        batch_size: int = _BULK_BATCH_SIZE,
    ):
        """
        Yield batches of property bundles.

        For targeted lists (property_ids set): chunk the IDs directly.
        For full / county runs: use keyset pagination on the primary key —
          never loads the whole table into memory.
        """
        if property_ids is not None:
            for i in range(0, len(property_ids), batch_size):
                chunk_ids = property_ids[i : i + batch_size]
                prop_rows = self._fetch_properties_by_ids(chunk_ids)
                if not prop_rows:
                    continue
                pid_list = [r.id for r in prop_rows]
                signal_map = self._fetch_signals_for_batch(pid_list)
                yield [self._build_property_bundle(r, signal_map) for r in prop_rows]
        else:
            last_id = 0
            while True:
                prop_rows = self._fetch_properties_chunk(last_id, batch_size, county_id)
                if not prop_rows:
                    break
                pid_list = [r.id for r in prop_rows]
                signal_map = self._fetch_signals_for_batch(pid_list)
                yield [self._build_property_bundle(r, signal_map) for r in prop_rows]
                last_id = prop_rows[-1].id

    # ── Batch score persistence (Phase 3) ────────────────────────────────────

    _TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]

    def _bulk_update_distress_scores(self, updates_params: List[Dict]) -> None:
        """Single-round-trip bulk UPDATE via VALUES, replacing executemany.

        Why: with a remote DB, SQLAlchemy's executemany sends N statements
        over the wire and waits one RTT per row. At ~250ms RTT and 800
        rows/batch the network tax alone is ~200s/batch. Collapsing to
        one `UPDATE ... FROM (VALUES %s)` statement makes it one RTT.

        Explicit casts on the SET clause defend against VALUES type
        inference picking the wrong type from the first row.
        """
        if not updates_params:
            return
        rows = [
            (
                row["id"], row["now"], row["score"], row["tier"], row["urgency"],
                row["qualified"], row["factor"], row["vertical"], row["distress"],
                row["run_id"],
            )
            for row in updates_params
        ]
        raw_conn = self.session.connection().connection
        with raw_conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""
                UPDATE {self._scores_table_name} ds SET
                    score_date      = v.score_date::timestamp,
                    final_cds_score = v.final_score::numeric,
                    lead_tier       = v.tier,
                    urgency_level   = v.urgency,
                    qualified       = v.qualified::boolean,
                    factor_scores   = v.factor::jsonb,
                    vertical_scores = v.vertical::jsonb,
                    distress_types  = v.distress::jsonb,
                    scoring_run_id  = v.run_id::integer
                FROM (VALUES %s) AS v(
                    id, score_date, final_score, tier, urgency,
                    qualified, factor, vertical, distress, run_id
                )
                WHERE ds.id = v.id::bigint
                """,
                rows,
                page_size=5000,
            )

    def _persist_score_batch(
        self,
        scored_batch: List[Dict],
        scoring_run_id: int,
        today_start: datetime,
        tomorrow_start: datetime,
    ) -> Dict[str, Any]:
        """
        Persist a batch of with-signal score dicts with two reads + two writes
        instead of N×4 per-property round trips.

        Read path:
          1. Fetch today's existing rows for ALL batch PIDs in one query.
          2. Fetch latest historical row for PIDs WITHOUT a today row (DISTINCT ON).

        Write path:
          3. Batch UPDATE existing today rows (executemany).
          4. Batch INSERT new rows (executemany, no RETURNING).
             → Gold new rows: follow-up SELECT by scoring_run_id to get IDs
               for flash_scarcity hook.

        Returns a dict with aggregate counters and side-effect queues.
        """
        _GOLD_PLUS = {"Ultra Platinum", "Platinum", "Gold"}

        if not scored_batch:
            return {
                "new": 0, "updated": 0, "unchanged": 0, "upgraded": 0,
                "qualified": 0, "ghl_queued": [], "new_gold_records": [],
                "new_gold_plus_entering": [],
            }

        now          = datetime.now(timezone.utc)
        property_ids = [sd["property_id"] for sd in scored_batch]

        # ── 1. Today's rows ───────────────────────────────────────────────
        with self._profiler.phase("persist_read_today"):
            today_rows = self.session.execute(sa_text(f"""
                SELECT id, property_id, final_cds_score, lead_tier
                FROM {self._scores_table_name}
                WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
                  AND score_date >= :start
                  AND score_date  < :end
            """), {"ids": property_ids, "start": today_start, "end": tomorrow_start}).fetchall()
            today_by_pid: Dict[int, Any] = {r.property_id: r for r in today_rows}

        # ── 2. Latest historical rows (only for PIDs without a today row) ─
        needs_latest = [pid for pid in property_ids if pid not in today_by_pid]
        latest_by_pid: Dict[int, Any] = {}
        if needs_latest:
            with self._profiler.phase("persist_read_latest"):
                latest_rows = self.session.execute(sa_text(f"""
                    SELECT DISTINCT ON (property_id)
                        id, property_id, final_cds_score, lead_tier
                    FROM {self._scores_table_name}
                    WHERE property_id IN (SELECT unnest(CAST(:ids AS bigint[])))
                    ORDER BY property_id, score_date DESC
                """), {"ids": needs_latest}).fetchall()
                latest_by_pid = {r.property_id: r for r in latest_rows}

        # ── 3. Classify each score ────────────────────────────────────────
        # (Python work — includes json.dumps for the UPDATE-path payloads.)
        updates_params: List[Dict] = []
        inserts_data:   List[Dict] = []
        new_count = updated_count = unchanged_count = upgraded_count = qualified_count = 0
        ghl_queued: List[Dict] = []
        new_gold_plus_entering: List[Dict] = []  # (property_id, county_id, tier, zip, scoring_run_id)

        with self._profiler.phase("persist_classify"):
          for sd in scored_batch:
            pid         = sd["property_id"]
            final_score = float(sd["final_cds_score"])
            lead_tier   = sd["lead_tier"]

            today_row = today_by_pid.get(pid)
            if today_row:
                prev_score = float(today_row.final_cds_score) if today_row.final_cds_score is not None else 0.0
                prev_tier  = today_row.lead_tier or ""
                updates_params.append({
                    "now":      now,
                    "score":    final_score,
                    "tier":     lead_tier,
                    "urgency":  sd["urgency_level"],
                    "qualified": sd["qualified"],
                    "factor":   json.dumps(sd["factor_scores"]),
                    "vertical": json.dumps(sd["vertical_scores"]),
                    "distress": json.dumps(sd["distress_types"]),
                    "run_id":   scoring_run_id,
                    "id":       today_row.id,
                })
                updated_count += 1
                if sd.get("qualified"):
                    qualified_count += 1
                upgraded = (
                    prev_tier in self._TIER_ORDER and lead_tier in self._TIER_ORDER
                    and self._TIER_ORDER.index(lead_tier) < self._TIER_ORDER.index(prev_tier)
                )
                if upgraded:
                    upgraded_count += 1
                # Entering Gold+: intraday upgrade from below-Gold to Gold/Platinum/Ultra Platinum.
                # Separate from flash-scarcity (Gold-only). Gold→Platinum not emitted (already Gold+).
                if (
                    lead_tier in _GOLD_PLUS
                    and prev_tier not in _GOLD_PLUS
                ):
                    new_gold_plus_entering.append({
                        "property_id":   pid,
                        "county_id":     sd.get("county_id", "hillsborough"),
                        "lead_tier":     lead_tier,
                        "zip":           sd.get("zip"),
                        "scoring_run_id": scoring_run_id,
                    })
                score_changed = prev_score != final_score
                if _GHL_PUSH_ENABLED and (score_changed or not sd.get("ghl_contact_id")):
                    ghl_queued.append(sd)
            else:
                latest_row = latest_by_pid.get(pid)
                try:
                    latest_score = float(latest_row.final_cds_score) if latest_row and latest_row.final_cds_score is not None else None
                except (TypeError, ValueError):
                    latest_score = None
                latest_tier = latest_row.lead_tier if latest_row else None

                if latest_score is not None and latest_score == final_score and latest_tier == lead_tier:
                    unchanged_count += 1
                else:
                    inserts_data.append(sd)
                    new_count += 1
                    if sd.get("qualified"):
                        qualified_count += 1
                    if _GHL_PUSH_ENABLED:
                        ghl_queued.append(sd)
                    # Entering Gold+: new insert at Gold/Platinum/Ultra Platinum where
                    # prior tier (if any) was below Gold (or no prior score at all).
                    if (
                        lead_tier in _GOLD_PLUS
                        and (latest_tier is None or latest_tier not in _GOLD_PLUS)
                    ):
                        new_gold_plus_entering.append({
                            "property_id":   pid,
                            "county_id":     sd.get("county_id", "hillsborough"),
                            "lead_tier":     lead_tier,
                            "zip":           sd.get("zip"),
                            "scoring_run_id": scoring_run_id,
                        })

        # ── 4. Batch UPDATE ───────────────────────────────────────────────
        # Routed through _bulk_update_distress_scores so the N-row UPDATE is
        # a single round-trip (VALUES join), not N executemany statements.
        if updates_params:
            with self._profiler.phase("persist_update"):
                self._bulk_update_distress_scores(updates_params)

        # ── 5. Batch INSERT ───────────────────────────────────────────────
        new_gold_records: List[tuple] = []
        if inserts_data:
            # Split insert-side JSON serialization from the SQL exec so each is
            # measured separately — JSON cost grows with factor_scores depth.
            with self._profiler.phase("persist_insert_build"):
                insert_params = [
                    {
                        "pid":      sd["property_id"],
                        "county":   sd.get("county_id", "hillsborough"),
                        "now":      now,
                        "score":    sd["final_cds_score"],
                        "tier":     sd["lead_tier"],
                        "urgency":  sd["urgency_level"],
                        "qualified": sd["qualified"],
                        "factor":   json.dumps(sd["factor_scores"]),
                        "vertical": json.dumps(sd["vertical_scores"]),
                        "distress": json.dumps(sd["distress_types"]),
                        "run_id":   scoring_run_id,
                    }
                    for sd in inserts_data
                ]
            with self._profiler.phase("persist_insert_exec"):
                self.session.execute(
                    sa_text(f"""
                        INSERT INTO {self._scores_table_name} (
                            property_id, county_id, score_date, final_cds_score,
                            lead_tier, urgency_level, qualified,
                            factor_scores, vertical_scores, distress_types, scoring_run_id
                        ) VALUES (
                            :pid, :county, :now, :score,
                            :tier, :urgency, :qualified,
                            CAST(:factor AS jsonb), CAST(:vertical AS jsonb),
                            CAST(:distress AS jsonb), :run_id
                        )
                    """),
                    insert_params,
                )

                # Flash scarcity needs the new row ID for Gold leads — retrieve via
                # (property_id, scoring_run_id) after the insert completes.
                gold_inserts = [sd for sd in inserts_data if sd["lead_tier"] == "Gold"]
                if gold_inserts:
                    gold_pids = [sd["property_id"] for sd in gold_inserts]
                    gold_id_rows = self.session.execute(sa_text(f"""
                        SELECT id, property_id
                        FROM {self._scores_table_name}
                        WHERE property_id IN (SELECT unnest(CAST(:pids AS bigint[])))
                          AND scoring_run_id = :run_id
                    """), {"pids": gold_pids, "run_id": scoring_run_id}).fetchall()
                    gold_id_map = {r.property_id: r.id for r in gold_id_rows}
                    for sd in gold_inserts:
                        if sd["property_id"] in gold_id_map:
                            new_gold_records.append((sd, gold_id_map[sd["property_id"]]))

        return {
            "new":                    new_count,
            "updated":                updated_count,
            "unchanged":              unchanged_count,
            "upgraded":               upgraded_count,
            "qualified":              qualified_count,
            "ghl_queued":             ghl_queued,
            "new_gold_records":       new_gold_records,
            "new_gold_plus_entering": new_gold_plus_entering,
        }

    def score_all_properties(
        self,
        save_to_db: bool = True,
        property_ids: Optional[List[int]] = None,
        county_id: Optional[str] = None,
        batch_size: int = _BULK_BATCH_SIZE,
    ) -> List[Dict]:
        """
        Score all properties (or only specific IDs / county).

        Uses raw SQL keyset pagination + batched signal loading + batch
        persistence — never loads the full property table into memory and
        replaces N×4 per-property DB round trips with 2 reads + 2 writes
        per batch.

        For targeted runs (property_ids given): returns ALL with-signal score
        dicts (callers such as verify_cds_scores.py need the full list).
        For full / county runs: returns only the top-10 by score to avoid
        growing an unbounded list at 500k+ scale. Aggregate stats for all
        properties are always available in self._last_run_stats after the
        call returns.

        Args:
            save_to_db:   Persist scores to the database.
            property_ids: If provided, only rescore these property IDs.
            county_id:    If provided, restrict scoring to this county.
            batch_size:   Properties per fetch+score cycle (default _BULK_BATCH_SIZE).

        Returns:
            All with-signal score dicts (targeted run) or top-10 (full run).
        """
        scoring_run_id = int(datetime.now(timezone.utc).timestamp())
        logger.info("Scoring run ID: %d", scoring_run_id)

        today          = date.today()
        today_start    = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        tomorrow_start = today_start + timedelta(days=1)

        label = (
            f"{len(property_ids)} properties" if property_ids
            else f"all properties{' (' + county_id + ')' if county_id else ''}"
        )
        logger.info("Scoring %s (batch_size=%d)...", label, batch_size)

        # ── Persistence counters ──────────────────────────────────────────
        new_count       = 0
        updated_count   = 0
        unchanged_count = 0
        upgraded_count  = 0
        no_signal_count = 0
        failed_count    = 0
        qualified_db    = 0   # qualified from persistence (new+updated), for platform stats
        self._total_scored = 0

        # ── Running stats for CLI display (never hold all score dicts) ────
        stats_with_signals   = 0
        stats_qualified      = 0     # all with-signal qualified, for CLI
        stats_tier_counts:   Counter = Counter()
        stats_urgency:       Counter = Counter()
        stats_top_vertical:  Counter = Counter()
        stats_signal_types:  Counter = Counter()
        stats_score_sum      = 0.0
        stats_score_max      = 0.0
        top10_heap: list     = []    # (score, pid, score_data) min-heap, capped at 10

        # ── Return-value collection ───────────────────────────────────────
        _targeted = property_ids is not None
        collected_scores: List[Dict] = []   # filled for targeted runs only

        # Preload all active teaching corrections for this run in one query.
        # Avoids N+1 reads at score_property() time across a 500k-property batch.
        _corrections_by_pid = self._load_all_teaching_corrections(property_ids)

        for batch in self._iter_property_batches(property_ids, county_id, batch_size):
            self._profiler.mark_batch()
            with_signal_batch: List[Dict] = []

            for prop in batch:
                self._total_scored += 1
                try:
                    with self._profiler.phase("score_python"):
                        score_data = self.score_property(
                            prop,
                            teaching_corrections=_corrections_by_pid.get(prop.id, []),
                        )

                    # A property with an active Teaching Correction (A6) must be
                    # persisted even when the dampener drives it to zero — otherwise
                    # its stale high score remains the latest row and the dampener
                    # is invisible downstream. Uncorrected zero/no-signal properties
                    # are still skipped (the nightly run never writes empty rows).
                    _is_zero = score_data["signal_count"] == 0 or score_data["final_cds_score"] == 0
                    _is_dampened = bool(_corrections_by_pid.get(prop.id))
                    if _is_zero and not _is_dampened:
                        no_signal_count += 1
                    else:
                        with_signal_batch.append(score_data)

                        # Update running stats incrementally
                        stats_with_signals += 1
                        sc = score_data["final_cds_score"]
                        stats_score_sum += sc
                        if sc > stats_score_max:
                            stats_score_max = sc
                        stats_tier_counts[score_data["lead_tier"]] += 1
                        stats_urgency[score_data["urgency_level"]] += 1
                        if score_data["vertical_scores"]:
                            best_v = max(score_data["vertical_scores"], key=score_data["vertical_scores"].get)
                            stats_top_vertical[best_v] += 1
                        for t in score_data["distress_types"]:
                            stats_signal_types[t] += 1
                        if score_data.get("qualified"):
                            stats_qualified += 1

                        # Maintain top-10 min-heap (score, pid as tiebreaker, dict)
                        pid_key = score_data["property_id"]
                        if len(top10_heap) < 10:
                            heapq.heappush(top10_heap, (sc, pid_key, score_data))
                        elif sc > top10_heap[0][0]:
                            heapq.heapreplace(top10_heap, (sc, pid_key, score_data))

                except SQLAlchemyError:
                    failed_count += 1
                    logger.error(
                        "Database error scoring property %s (%s) — rolling back and continuing",
                        prop.id, prop.parcel_id, exc_info=True,
                    )
                    try:
                        self.session.rollback()
                    except Exception:
                        logger.error(
                            "Rollback failed after DB error on property %s",
                            prop.id, exc_info=True,
                        )

                except Exception as exc:
                    failed_count += 1
                    logger.error(
                        "Unexpected error scoring property %s (%s): %s",
                        prop.id, prop.parcel_id, exc, exc_info=True,
                    )

            # ── Batch persistence ─────────────────────────────────────────
            if save_to_db and with_signal_batch:
                with self._profiler.phase("persist_batch"):
                    result = self._persist_score_batch(
                        with_signal_batch, scoring_run_id, today_start, tomorrow_start,
                    )
                new_count       += result["new"]
                updated_count   += result["updated"]
                unchanged_count += result["unchanged"]
                upgraded_count  += result["upgraded"]
                qualified_db    += result["qualified"]

                self._ghl_push_queue.extend(result["ghl_queued"])

                for sd, new_id in result["new_gold_records"]:
                    try:
                        from src.services.flash_scarcity import open_window_if_spike
                        zip_code = sd.get("zip")
                        vertical = sd.get("top_vertical")
                        if zip_code and vertical:
                            open_window_if_spike(self.session, new_id, zip_code, vertical)
                    except Exception as _fse:
                        logger.debug("flash_scarcity hook error: %s", _fse)

            if save_to_db:
                try:
                    with self._profiler.phase("commit"):
                        self.session.commit()
                    logger.info(
                        "Scoring progress: %d scored — new=%d updated=%d no_signal=%d",
                        self._total_scored, new_count, updated_count, no_signal_count,
                    )
                except SQLAlchemyError as batch_exc:
                    logger.error(
                        "Batch commit failed at property %d: %s",
                        self._total_scored, batch_exc, exc_info=True,
                    )

                # Emit gold_lead_scored events AFTER commit (ADR 0016).
                # Guard by ENRICHMENT_CASCADE_ENABLED so the nightly batch remains
                # the sole trigger until the consumer is enabled in ops.
                # Publish failure must never fail scoring — nightly batch is backstop.
                if with_signal_batch and result.get("new_gold_plus_entering"):
                    try:
                        from config.settings import get_settings as _get_settings
                        _settings = _get_settings()
                        if _settings.enrichment_cascade_enabled:
                            from src.agents.events.ingestion import publish_cora_event
                            for _entry in result["new_gold_plus_entering"]:
                                try:
                                    publish_cora_event({
                                        "event_type": "gold_lead_scored",
                                        "payload":    _entry,
                                        "idempotency_key": (
                                            f"gold_lead_scored:"
                                            f"{_entry['property_id']}:"
                                            f"{_entry['scoring_run_id']}"
                                        ),
                                    })
                                except Exception as _pub_exc:
                                    logger.warning(
                                        "gold_lead_scored publish failed property_id=%s: %s",
                                        _entry.get("property_id"), _pub_exc,
                                    )
                    except Exception as _emit_exc:
                        logger.warning("gold_lead_scored batch emit failed: %s", _emit_exc)

            if save_to_db and _GHL_PUSH_ENABLED:
                with self._profiler.phase("ghl_flush"):
                    self._flush_ghl_queue()

            if _targeted:
                collected_scores.extend(with_signal_batch)

        # ── Post-run ──────────────────────────────────────────────────────
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
            if _GHL_PUSH_ENABLED:
                self._flush_ghl_queue()

            try:
                self._record_platform_stats(
                    properties_scored=self._total_scored,
                    properties_with_signals=stats_with_signals,
                    score_runs_total=new_count + updated_count + unchanged_count,
                    leads_new=new_count,
                    leads_updated=updated_count,
                    leads_unchanged=unchanged_count,
                    leads_qualified=qualified_db,
                    leads_upgraded=upgraded_count,
                    tier_counts=stats_tier_counts,
                    county_id=county_id,
                )
            except Exception as stats_err:
                logger.warning("⚠ Could not record platform daily stats (non-critical): %s", stats_err)

        # ── Expose aggregate stats for CLI / callers ──────────────────────
        top10 = [sd for _, _, sd in sorted(top10_heap, key=lambda x: -x[0])]
        self._last_run_stats: Dict[str, Any] = {
            "with_signals":       stats_with_signals,
            "qualified":          stats_qualified,
            "tier_counts":        stats_tier_counts,
            "urgency_counts":     stats_urgency,
            "top_vertical_counts": stats_top_vertical,
            "signal_type_counts": stats_signal_types,
            "score_sum":          stats_score_sum,
            "score_max":          stats_score_max,
            "new":                new_count,
            "updated":            updated_count,
            "unchanged":          unchanged_count,
            "upgraded":           upgraded_count,
            "failed":             failed_count,
            "no_signal":          no_signal_count,
            "top10":              top10,
        }

        return collected_scores if _targeted else top10

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
        county_id: Optional[str] = None,
    ) -> None:
        """
        Upsert a row in platform_daily_stats for today.

        Signal totals (signals_scraped/matched/skipped) are pulled live from
        scraper_run_stats for today so they reflect all scrapers that have run,
        regardless of whether they ran before or after the CDS engine.

        Requires county_id — without one, stats can't be attributed correctly
        across multi-county pools, so the call is skipped with a warning.
        """
        if not county_id:
            logger.warning(
                "Skipping platform_daily_stats — county_id missing. "
                "Per-property distress_scores are still correct; only the "
                "daily roll-up was skipped."
            )
            return

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
        )
        # ON CONFLICT rules:
        #   ACCUMULATE  — leads_new, leads_updated, leads_qualified, leads_upgraded:
        #     these are events. Each scraper-triggered scoring run (score_properties_by_ids)
        #     may add genuinely new/updated leads during the day; we want the daily total.
        #   OVERWRITE   — everything else:
        #     tier counts and properties_scored are snapshots of the current pool.
        #     A --rescore-all run re-evaluates all 523k properties; accumulating that
        #     across 3 runs would show 1.57M "scored" and 3× inflated tier counts.
        #     The latest run is always the most accurate picture.
        stmt = stmt.on_conflict_do_update(
            constraint='uq_platform_daily_stats',
            set_=dict(
                # Signal totals — always overwrite (rolled up from scraper_run_stats)
                signals_scraped=int(signal_row.scraped),
                signals_matched=int(signal_row.matched),
                signals_skipped=int(signal_row.skipped),
                # Snapshot counters — overwrite with latest run
                properties_scored=stmt.excluded.properties_scored,
                properties_with_signals=stmt.excluded.properties_with_signals,
                leads_unchanged=stmt.excluded.leads_unchanged,
                # Run counter — accumulate
                score_runs_total=PlatformDailyStats.score_runs_total + stmt.excluded.score_runs_total,
                # Event counters — accumulate across scraper-triggered runs
                leads_new=PlatformDailyStats.leads_new + stmt.excluded.leads_new,
                leads_updated=PlatformDailyStats.leads_updated + stmt.excluded.leads_updated,
                leads_qualified=PlatformDailyStats.leads_qualified + stmt.excluded.leads_qualified,
                leads_upgraded=PlatformDailyStats.leads_upgraded + stmt.excluded.leads_upgraded,
                # Tier counts — overwrite (snapshot of current pool, not a running total)
                tier_ultra_platinum=stmt.excluded.tier_ultra_platinum,
                tier_platinum=stmt.excluded.tier_platinum,
                tier_gold=stmt.excluded.tier_gold,
                tier_silver=stmt.excluded.tier_silver,
                tier_bronze=stmt.excluded.tier_bronze,
                updated_at=datetime.now(timezone.utc),
            )
        )
        self.session.execute(stmt)
        self.session.flush()
        logger.info(
            "✓ Platform daily stats recorded: scored=%d new=%d updated=%d qualified=%d upgraded=%d",
            properties_scored, leads_new, leads_updated, leads_qualified, leads_upgraded,
        )

    def score_properties_by_ids(
        self,
        property_ids: List[int],
        save_to_db: bool = True,
        county_id: Optional[str] = None,
        batch_size: int = _BULK_BATCH_SIZE,
    ) -> List[Dict]:
        """
        Fast path: rescore only specific properties.
        Used by the ingestion-time hook after a scraper run.
        """
        return self.score_all_properties(
            save_to_db=save_to_db,
            property_ids=property_ids,
            county_id=county_id,
            batch_size=batch_size,
        )

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
        today          = date.today()
        today_start    = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        tomorrow_start = today_start + timedelta(days=1)
        try:
            return self.session.query(DistressScore).filter(
                DistressScore.property_id == property_id,
                DistressScore.score_date >= today_start,
                DistressScore.score_date  < tomorrow_start,
            ).first()
        except OperationalError:
            logger.error("Database error in get_todays_score_for_property(id=%s)", property_id, exc_info=True)
            raise


# ── CLI entry point ────────────────────────────────────────────────────────────

def _apply_fit_artifact(path: str, log: logging.Logger) -> None:
    """Load a Stage C JSON artifact and overwrite the module-level
    ``VERTICAL_WEIGHTS`` dict in-place with the fitted proposals.

    Mutates the existing dict instead of rebinding so any closure/import
    that already captured the reference (e.g. ``from config.scoring import
    VERTICAL_WEIGHTS`` elsewhere) sees the updated values. Only signals
    that exist in both the artifact and the current weights are touched —
    artifact entries for unknown signals are ignored to avoid silently
    extending the scoring surface.

    Raises SystemExit(2) on any malformed artifact — Stage E must fail
    loudly rather than score against partial/wrong data.
    """
    import json
    from pathlib import Path

    artifact_path = Path(path)
    if not artifact_path.is_file():
        log.error("Fit artifact not found: %s", artifact_path)
        sys.exit(2)

    try:
        artifact = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        log.error("Fit artifact is not valid JSON (%s): %s", artifact_path, exc)
        sys.exit(2)

    proposals = artifact.get("proposals")
    if not isinstance(proposals, list):
        log.error("Fit artifact missing 'proposals' list: %s", artifact_path)
        sys.exit(2)

    applied = 0
    skipped: List[str] = []
    for proposal in proposals:
        vertical = proposal.get("vertical")
        weights = proposal.get("vertical_weights") or {}
        if vertical not in VERTICAL_WEIGHTS:
            skipped.append(vertical or "<no-vertical>")
            continue
        # Mutate in place — preserves dict identity across all importers.
        target = VERTICAL_WEIGHTS[vertical]
        for sig, w in weights.items():
            if sig in target:
                target[sig] = int(w)
                applied += 1
    log.info(
        "[fit-artifact] loaded %s — applied %d (signal, vertical) weights%s",
        artifact_path.name, applied,
        f", skipped verticals: {skipped}" if skipped else "",
    )


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
        "--rescore-new-signals",
        action="store_true",
        help="Only rescore properties with new signal data since their last score",
    )
    parser.add_argument(
        "--county-id",
        dest="county_id",
        default=None,
        metavar="COUNTY",
        help="Restrict scoring to a single county (e.g. hillsborough, pinellas). Default: all counties",
    )
    parser.add_argument(
        "--no-ghl",
        action="store_true",
        help="Skip GHL CRM push (useful for bulk rescores to avoid rate limits)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Emit a per-phase wall-clock breakdown (properties_fetch, "
            "signals_fetch, score_python, persist_batch, commit, ghl_flush) "
            "at the end of the run. Useful for monitoring scoring throughput "
            "and validating that no single phase regresses after schema changes."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_BULK_BATCH_SIZE,
        metavar="N",
        dest="batch_size",
        help=(
            f"Properties per scoring batch (default {_BULK_BATCH_SIZE}). "
            "Increase on high-RAM servers for fewer round-trips; decrease to reduce peak memory."
        ),
    )
    parser.add_argument(
        "--shadow",
        action="store_true",
        help=(
            "Stage E shadow-rescore mode. Writes scores to distress_scores_shadow "
            "instead of distress_scores, skips the pending_sync GHL flush, and "
            "leaves the live subscriber-facing state untouched. Pair with "
            "--fit-artifact to evaluate proposed Stage C weights before cutover."
        ),
    )
    parser.add_argument(
        "--fit-artifact",
        type=str,
        default=None,
        metavar="PATH",
        dest="fit_artifact",
        help=(
            "Path to a Stage C JSON artifact (data/scoring_fit/<id>.json). "
            "Overrides VERTICAL_WEIGHTS in-memory with the fitted proposals. "
            "Requires --shadow — must not be used against the live tables."
        ),
    )
    args = parser.parse_args()

    if args.fit_artifact and not args.shadow:
        log.error(
            "--fit-artifact requires --shadow. Refusing to score the live table "
            "with proposed weights — use Stage F's cutover edit to config/scoring.py instead."
        )
        sys.exit(2)

    if args.no_ghl:
        global _GHL_PUSH_ENABLED
        _GHL_PUSH_ENABLED = False
        log.info("[GHL] Push disabled via --no-ghl flag")

    # ── Resolve target counties ───────────────────────────────────────────
    # --county-id wins. Otherwise, --property-id resolves to that property's
    # own county (so platform_daily_stats is attributed correctly). With
    # neither, iterate every active county from the `counties` DB table.
    from src.utils.county_config import list_counties

    if args.county_id:
        target_counties: List[str] = [args.county_id]
    elif args.property_id:
        try:
            with get_db_context() as _lookup:
                row = _lookup.execute(
                    sa_text("SELECT county_id FROM properties WHERE id = :id"),
                    {"id": args.property_id},
                ).first()
            if not row or not row.county_id:
                log.error("Property id=%s not found or has no county_id", args.property_id)
                sys.exit(2)
            target_counties = [row.county_id]
        except (OperationalError, SQLAlchemyError) as exc:
            log.error("Database error looking up property county: %s", exc, exc_info=True)
            sys.exit(1)
    else:
        target_counties = list_counties()
        if not target_counties:
            log.error("No active counties found in `counties` table — nothing to score")
            sys.exit(2)
        log.info(
            "No --county-id given — iterating %d active counties: %s",
            len(target_counties), target_counties,
        )

    county_label = f" [{','.join(target_counties)}]"
    if args.property_id:
        run_label = f"property {args.property_id}"
    elif args.rescore_new_signals:
        run_label = f"new-signals run{county_label}"
    elif args.rescore_all:
        run_label = f"all properties (rescore){county_label}"
    else:
        run_label = f"daily run (all properties){county_label}"

    log.info("=" * 60)
    log.info("CDS Multi-Vertical Scoring Engine")
    log.info("Mode:    %s", run_label)
    log.info("Started: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    interrupted = False
    scorer = None

    # ── Cross-county aggregate stats (for CLI summary) ────────────────────
    # platform_daily_stats is written per-county inside the loop; this dict
    # only feeds the operator's terminal output.
    combined_total = 0
    combined_stats: Dict[str, Any] = {
        "with_signals":        0,
        "qualified":           0,
        "tier_counts":         Counter(),
        "urgency_counts":      Counter(),
        "top_vertical_counts": Counter(),
        "signal_type_counts":  Counter(),
        "score_sum":           0.0,
        "score_max":           0.0,
        "new":                 0,
        "updated":             0,
        "unchanged":           0,
        "upgraded":            0,
        "failed":              0,
        "no_signal":           0,
        "top10":               [],
    }

    # Stage E — apply shadow-mode and fit-artifact overrides before any
    # scoring work happens. fit_artifact mutates the global VERTICAL_WEIGHTS
    # so the engine's _score_vertical reads the proposed numbers; shadow mode
    # routes writes to the shadow table and suppresses pending_sync flushes.
    if args.fit_artifact:
        _apply_fit_artifact(args.fit_artifact, log)
    if args.shadow:
        # Mutate the module-level flag directly. `global _GHL_PUSH_ENABLED`
        # was already declared earlier in this function under --no-ghl, so
        # redeclaring it here would be a SyntaxError. The bare assignment
        # below still hits the global because of that earlier declaration.
        _GHL_PUSH_ENABLED = False  # noqa: F841 — global is in scope from earlier
        log.info("[shadow] writing to distress_scores_shadow; GHL push disabled")

    try:
        with get_db_context() as session:
            scorer = MultiVerticalScorer(session)
            if args.shadow:
                scorer._scores_table_name = "distress_scores_shadow"
                scorer._shadow_mode = True
            if args.profile:
                scorer._profiler = _Profiler(enabled=True)
                log.info("[profile] enabled — phase timings will be reported at end of run")

            for cid in target_counties:
                if len(target_counties) > 1:
                    log.info("─" * 60)
                    log.info("County: %s", cid)
                    log.info("─" * 60)

                if args.property_id:
                    this_property_ids: Optional[List[int]] = [args.property_id]
                elif args.rescore_new_signals:
                    log.info("[%s] Collecting properties with new signals…", cid)
                    this_property_ids = scorer._collect_changed_property_ids(county_id=cid)
                    log.info("[%s] Found %d properties with new signals to rescore",
                             cid, len(this_property_ids))
                    if not this_property_ids:
                        log.info("[%s] up to date — skipping", cid)
                        continue
                else:
                    this_property_ids = None

                scorer.score_all_properties(
                    save_to_db=True,
                    property_ids=this_property_ids,
                    county_id=cid,
                    batch_size=args.batch_size,
                )
                session.commit()

                # Master-refresh flags (fa077) are consumed by this run — clear
                # them so the partial index stays small. A scoring crash above
                # leaves the flags set, so they're retried on the next run.
                # Shadow runs must not mutate live state.
                if not args.shadow:
                    if this_property_ids is not None:
                        for i in range(0, len(this_property_ids), 10_000):
                            session.execute(
                                sa_text(
                                    "UPDATE properties SET needs_rescore = FALSE "
                                    "WHERE id IN (SELECT unnest(CAST(:ids AS bigint[]))) "
                                    "AND needs_rescore"
                                ),
                                {"ids": this_property_ids[i:i + 10_000]},
                            )
                    else:
                        session.execute(
                            sa_text(
                                "UPDATE properties SET needs_rescore = FALSE "
                                "WHERE county_id = :county AND needs_rescore"
                            ),
                            {"county": cid},
                        )
                    session.commit()

                # Roll up per-county stats into the combined summary.
                rs_county = getattr(scorer, "_last_run_stats", {}) or {}
                combined_total += scorer._total_scored
                combined_stats["with_signals"]        += rs_county.get("with_signals", 0)
                combined_stats["qualified"]           += rs_county.get("qualified", 0)
                combined_stats["tier_counts"]         += rs_county.get("tier_counts", Counter())
                combined_stats["urgency_counts"]      += rs_county.get("urgency_counts", Counter())
                combined_stats["top_vertical_counts"] += rs_county.get("top_vertical_counts", Counter())
                combined_stats["signal_type_counts"]  += rs_county.get("signal_type_counts", Counter())
                combined_stats["score_sum"]           += rs_county.get("score_sum", 0.0)
                combined_stats["score_max"]            = max(
                    combined_stats["score_max"], rs_county.get("score_max", 0.0),
                )
                combined_stats["new"]                 += rs_county.get("new", 0)
                combined_stats["updated"]             += rs_county.get("updated", 0)
                combined_stats["unchanged"]           += rs_county.get("unchanged", 0)
                combined_stats["upgraded"]            += rs_county.get("upgraded", 0)
                combined_stats["failed"]              += rs_county.get("failed", 0)
                combined_stats["no_signal"]           += rs_county.get("no_signal", 0)
                combined_stats["top10"].extend(rs_county.get("top10", []))

            # Trim the merged top-10 across counties to the global top 10.
            combined_stats["top10"].sort(
                key=lambda sd: sd.get("final_cds_score", 0.0), reverse=True,
            )
            combined_stats["top10"] = combined_stats["top10"][:10]

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
    # combined_stats merges _last_run_stats across each county processed in
    # the loop above. platform_daily_stats already got per-county rows; this
    # output is just the operator's terminal summary.
    if scorer is None:
        log.info("Scoring did not start — no stats available.")
        sys.exit(1)

    rs    = combined_stats
    total = combined_total

    log.info("=" * 60)
    log.info("CDS SCORING COMPLETE%s", " (INTERRUPTED)" if interrupted else "")
    log.info("  Properties scored:   %7d", total)
    _ws = rs.get("with_signals", 0)
    log.info("  With signals:        %7d", _ws)
    log.info("  No signals (skipped):%7d", total - _ws)
    log.info("  Qualified (≥%s):       %7d", ROUTING_THRESHOLDS["weekly"], rs.get("qualified", 0))

    if _ws:
        avg = rs.get("score_sum", 0) / _ws
        log.info("  Avg score:           %7.1f", avg)
        log.info("  Top score:           %7.1f", rs.get("score_max", 0))

        # ── Lead tier distribution ────────────────────────────────────────
        log.info("")
        log.info("LEAD TIER DISTRIBUTION:")
        for tier in ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]:
            cnt = rs.get("tier_counts", Counter()).get(tier, 0)
            bar = "█" * min(30, cnt)
            log.info("  %-15s %5d  %s", tier, cnt, bar)

        # ── Urgency distribution ──────────────────────────────────────────
        log.info("")
        log.info("URGENCY / ROUTING DISTRIBUTION:")
        urgency_counts = rs.get("urgency_counts", Counter())
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
        for v, count in rs.get("top_vertical_counts", Counter()).most_common():
            bar = "█" * min(30, count)
            log.info("  %-20s %5d  %s", v, count, bar)

        # ── Signal type frequency ─────────────────────────────────────────
        log.info("")
        log.info("SIGNAL TYPE FREQUENCY (properties carrying each type):")
        for sig_type, count in rs.get("signal_type_counts", Counter()).most_common():
            bar = "█" * min(30, count)
            log.info("  %-25s %5d  %s", sig_type, count, bar)

        # ── Top 10 scored properties ──────────────────────────────────────
        log.info("")
        log.info("TOP 10 SCORED PROPERTIES:")
        log.info("  %-20s %6s %-15s %-10s %-20s %s", "Parcel", "Score", "Tier", "Urgency", "Best Vertical", "Signals")
        log.info("  %s %s %s %s %s %s", "-"*20, "-"*6, "-"*15, "-"*10, "-"*20, "-"*7)
        for s in rs.get("top10", []):
            if not s.get("vertical_scores"):
                continue
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

    # ── Profiling report ──────────────────────────────────────────────────────
    if args.profile and scorer is not None:
        scorer._profiler.report(log)

    log.info("")
    log.info("Finished: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    if interrupted:
        sys.exit(130)  # conventional exit code for SIGINT


if __name__ == "__main__":
    main()
