"""Financing-Intent Scoring Engine (Sprint S1).

Identifies property owners likely to need hard-money, bridge, renovation,
refinance, or buyout financing by scoring 5 signal sources:

    1. Fresh Deeds          — recent arm's-length purchase
    2. Active Permits       — open structural or roofing work
    3. Early Lis Pendens    — LP filed, auction not yet scheduled
    4. Divorce Filing       — matched divorce proceeding
    5. Equity Proxy         — high equity / long-tenure / free-and-clear

All weights, thresholds, and keyword sets live in config/financing_intent.py.

Batch architecture mirrors cds_engine.py:
  • Keyset pagination on properties (BATCH_SIZE rows per cycle)
  • 5 raw SQL queries per batch — one per signal source — not per-property
  • unnest(CAST(:ids AS bigint[])) for all IN-list lookups
  • psycopg2.extras.execute_values for bulk UPDATE (single round-trip)
  • defaultdict signal map + SimpleNamespace duck typing
  • Counter for O(1) stats accumulation
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import psycopg2.extras
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.financing_intent import (
    BATCH_SIZE,
    DIVORCE_EXCLUDE_STATUSES,
    DIVORCE_MIN_CONF,
    EQUITY_MEDIUM_PCT,
    EQUITY_STRONG_PCT,
    EQUITY_TENURE_YEARS,
    FRESH_DEED_EXCLUDE_TYPES,
    FRESH_DEED_MEDIUM_DAYS,
    FRESH_DEED_MIN_CONF,
    FRESH_DEED_STRONG_DAYS,
    LP_LATE_STATUSES,
    LP_LOOKBACK_DAYS,
    PERMIT_EXCLUDE_STATUSES,
    PRODUCT_PRIORITY,
    ROOFING_KEYWORDS,
    SCORE_CAP,
    SIGNAL_TO_PRODUCT,
    SIGNAL_WEIGHTS,
    STRUCTURAL_KEYWORDS,
    TIER_THRESHOLDS,
)

logger = logging.getLogger(__name__)

_UNNEST = "SELECT unnest(CAST(:ids AS bigint[]))"
_PROP_COLS = "id, parcel_id, county_id"


def _classify_permit(permit_type_lower: str) -> Optional[str]:
    """Return signal key or None. Caller must lower() the input once."""
    if any(kw in permit_type_lower for kw in STRUCTURAL_KEYWORDS):
        return "active_structural_permit"
    if any(kw in permit_type_lower for kw in ROOFING_KEYWORDS):
        return "active_roofing_permit"
    return None


class FinancingIntentScorer:
    """Stateless per-run scorer. Instantiate with a live session."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ── Batch signal fetch ─────────────────────────────────────────────────────

    def _fetch_signals_for_batch(self, property_ids: List[int]) -> "defaultdict":
        """
        5 raw SQL queries for all financing-intent signal data for a batch of
        properties — never queries inside a loop.

        Returns defaultdict keyed by property_id. Each value is:
          {
            "deeds":        [SimpleNamespace, ...],
            "permits":      [SimpleNamespace, ...],
            "foreclosures": [SimpleNamespace, ...],
            "proceedings":  [SimpleNamespace, ...],
            "financials":   SimpleNamespace | None,
          }
        """
        p: Dict[str, Any] = {"ids": property_ids}

        def _q(sql: str, extra: Optional[Dict] = None) -> list:
            params = {**p, **(extra or {})}
            return self.session.execute(sa_text(sql), params).fetchall()

        def _ns(row) -> SimpleNamespace:
            return SimpleNamespace(**dict(row._mapping))

        deed_rows = _q(
            f"""
            SELECT property_id, id AS deed_id, record_date, deed_type, match_confidence
            FROM deeds
            WHERE property_id IN ({_UNNEST})
              AND record_date >= CURRENT_DATE - :window
              AND match_confidence >= :min_conf
            """,
            {"window": FRESH_DEED_MEDIUM_DAYS, "min_conf": FRESH_DEED_MIN_CONF},
        )
        permit_rows = _q(
            f"""
            SELECT property_id, id AS permit_id, permit_type, status, is_enforcement_permit
            FROM building_permits
            WHERE property_id IN ({_UNNEST})
              AND is_enforcement_permit = false
            """,
        )
        fc_rows = _q(
            f"""
            SELECT property_id, id AS fc_id, lis_pendens_date, auction_date, case_status
            FROM foreclosures
            WHERE property_id IN ({_UNNEST})
              AND lis_pendens_date >= CURRENT_DATE - :lookback
            """,
            {"lookback": LP_LOOKBACK_DAYS},
        )
        lp_rows = _q(
            f"""
            SELECT property_id, id AS lp_id, filing_date, case_status, match_confidence
            FROM legal_proceedings
            WHERE property_id IN ({_UNNEST})
              AND record_type = 'Divorce'
            """,
        )
        fin_rows = _q(
            f"""
            SELECT f.property_id, f.equity_pct, f.est_mortgage_bal, o.ownership_years
            FROM financials f
            LEFT JOIN owners o ON o.property_id = f.property_id
            WHERE f.property_id IN ({_UNNEST})
            """,
        )

        sm: "defaultdict" = defaultdict(lambda: {
            "deeds": [], "permits": [], "foreclosures": [],
            "proceedings": [], "financials": None,
        })
        for r in deed_rows:    sm[r.property_id]["deeds"].append(_ns(r))
        for r in permit_rows:  sm[r.property_id]["permits"].append(_ns(r))
        for r in fc_rows:      sm[r.property_id]["foreclosures"].append(_ns(r))
        for r in lp_rows:      sm[r.property_id]["proceedings"].append(_ns(r))
        for r in fin_rows:     sm[r.property_id]["financials"] = _ns(r)
        return sm

    # ── Per-property scoring ───────────────────────────────────────────────────

    def _score_bundle(self, prop_row, sm: "defaultdict") -> Dict[str, Any]:
        """
        Pure Python scoring from pre-fetched signal data. No DB calls.

        Single pass over each signal list; O(1) dict/set lookups throughout.
        Returns a score dict or raises on unexpected data errors.
        """
        today = date.today()
        pid   = prop_row.id
        sigs  = sm[pid]

        flags:    Dict[str, bool] = {}
        scores:   Dict[str, int]  = {}
        details:  Dict[str, dict] = {}
        sources:  Dict[str, Any]  = {}
        excluded: Dict[str, str]  = {}

        # ── Signal 1: Fresh Deed ─────────────────────────────────────────────
        # Single pass — track the most recent qualifying deed.
        best_deed      = None
        best_deed_days = FRESH_DEED_MEDIUM_DAYS + 1
        for d in sigs["deeds"]:
            if d.record_date is None:
                continue
            dt  = d.deed_type or ""
            if any(ex in dt.lower() for ex in FRESH_DEED_EXCLUDE_TYPES):
                continue
            rd   = d.record_date if isinstance(d.record_date, date) else d.record_date.date()
            days = (today - rd).days
            if days < best_deed_days:
                best_deed      = d
                best_deed_days = days
        if best_deed is not None:
            flag = (
                "fresh_deed_0_30_days"
                if best_deed_days <= FRESH_DEED_STRONG_DAYS
                else "fresh_deed_31_60_days"
            )
            flags[flag]   = True
            scores[flag]  = SIGNAL_WEIGHTS[flag]
            details[flag] = {"record_date": str(best_deed.record_date), "days_ago": best_deed_days}
            sources[flag] = best_deed.deed_id

        # ── Signal 2: Active Permit ──────────────────────────────────────────
        # Single pass — structural beats roofing (higher weight wins).
        best_permit_flag: Optional[str] = None
        best_permit_id:   Optional[int] = None
        for bp in sigs["permits"]:
            status_lower = (bp.status or "").lower().strip()
            if status_lower in PERMIT_EXCLUDE_STATUSES:
                continue
            pt_lower = (bp.permit_type or "").lower()
            flag     = _classify_permit(pt_lower)
            if flag is None:
                continue
            if (
                best_permit_flag is None
                or SIGNAL_WEIGHTS[flag] > SIGNAL_WEIGHTS[best_permit_flag]
            ):
                best_permit_flag = flag
                best_permit_id   = bp.permit_id
        if best_permit_flag:
            flags[best_permit_flag]   = True
            scores[best_permit_flag]  = SIGNAL_WEIGHTS[best_permit_flag]
            details[best_permit_flag] = {}
            sources[best_permit_flag] = best_permit_id

        # ── Signal 3: Early Lis Pendens ──────────────────────────────────────
        # Rows already filtered by lis_pendens_date window in SQL.
        # Check auction_date + case_status in Python; first qualifying row wins.
        for fc in sigs["foreclosures"]:
            cs_lower = (fc.case_status or "").lower()
            if fc.auction_date is not None:
                excluded["early_lis_pendens"] = "auction_date_set"
                break
            if any(s in cs_lower for s in LP_LATE_STATUSES):
                excluded["early_lis_pendens"] = f"late_stage:{fc.case_status}"
                break
            lpd = (
                fc.lis_pendens_date
                if isinstance(fc.lis_pendens_date, date)
                else fc.lis_pendens_date.date()
            )
            flags["early_lis_pendens"]   = True
            scores["early_lis_pendens"]  = SIGNAL_WEIGHTS["early_lis_pendens"]
            details["early_lis_pendens"] = {"lis_pendens_date": str(lpd)}
            sources["early_lis_pendens"] = fc.fc_id
            break

        # ── Signal 4: Divorce ────────────────────────────────────────────────
        for lp in sigs["proceedings"]:
            conf     = float(lp.match_confidence or 0)
            cs_lower = (lp.case_status or "").lower()
            if conf < DIVORCE_MIN_CONF:
                continue
            if any(s in cs_lower for s in DIVORCE_EXCLUDE_STATUSES):
                excluded["divorce_match"] = f"case_status:{lp.case_status}"
                continue
            flags["divorce_match"]   = True
            scores["divorce_match"]  = SIGNAL_WEIGHTS["divorce_match"]
            details["divorce_match"] = {
                "filing_date":      str(lp.filing_date),
                "match_confidence": conf,
            }
            sources["divorce_match"] = lp.lp_id
            break

        # ── Signal 5: Equity Proxy ───────────────────────────────────────────
        # Stacking-only: only boosts score when at least one other signal fired.
        fin = sigs["financials"]
        if fin is not None and scores:
            eq_pct = float(fin.equity_pct) if fin.equity_pct is not None else None
            tenure = int(fin.ownership_years) if fin.ownership_years is not None else None
            no_mtg = fin.est_mortgage_bal is None

            if eq_pct is not None and eq_pct >= EQUITY_STRONG_PCT:
                flag = "equity_proxy_strong"
                flags[flag]   = True
                scores[flag]  = SIGNAL_WEIGHTS[flag]
                details[flag] = {"equity_pct": eq_pct}
            elif eq_pct is not None and eq_pct >= EQUITY_MEDIUM_PCT:
                flag = "equity_proxy_medium"
                flags[flag]   = True
                scores[flag]  = SIGNAL_WEIGHTS[flag]
                details[flag] = {"equity_pct": eq_pct}
            elif tenure is not None and tenure >= EQUITY_TENURE_YEARS and no_mtg:
                flag = "equity_proxy_medium"
                flags[flag]   = True
                scores[flag]  = SIGNAL_WEIGHTS[flag]
                details[flag] = {"ownership_years": tenure, "no_mortgage": True}

        # ── Assemble ─────────────────────────────────────────────────────────
        raw_score = min(SCORE_CAP, sum(scores.values()))
        tier      = next(t for thresh, t in TIER_THRESHOLDS if raw_score >= thresh)
        product   = next(
            (SIGNAL_TO_PRODUCT[f] for f in PRODUCT_PRIORITY if flags.get(f)),
            None,
        )

        return {
            "property_id":            pid,
            "county_id":              prop_row.county_id,
            "financing_intent_score": raw_score,
            "intent_tier":            tier,
            "recommended_product":    product,
            "signal_flags":           flags,
            "signal_scores":          scores,
            "signal_details":         details,
            "source_ids":             sources,
            "excluded_reasons":       excluded,
        }

    # ── Batch persistence ──────────────────────────────────────────────────────

    def _persist_batch(
        self,
        scored: List[Dict[str, Any]],
        today: date,
        dry_run: bool,
    ) -> Dict[str, Any]:
        """
        2 reads + 2 writes — not per-property.

        Read 1 : today's existing rows for all PIDs.
        Write 1: bulk UPDATE via psycopg2.extras.execute_values (single round-trip).
        Write 2: batch INSERT with ON CONFLICT DO UPDATE.

        Returns aggregate counters.
        """
        tier_counter:   Counter = Counter(s["intent_tier"] for s in scored)
        signal_counter: Counter = Counter(f for s in scored for f in s["signal_flags"])

        if dry_run:
            return {
                "new": 0, "updated": 0,
                "tier_counts": tier_counter, "signal_counts": signal_counter,
            }

        pids = [s["property_id"] for s in scored]
        now  = datetime.now(timezone.utc)

        today_rows = self.session.execute(sa_text(f"""
            SELECT id, property_id
            FROM financing_intent_scores
            WHERE property_id IN ({_UNNEST})
              AND score_date = :today
        """), {"ids": pids, "today": today}).fetchall()
        today_by_pid: Dict[int, Any] = {r.property_id: r for r in today_rows}

        updates: List[tuple] = []
        inserts: List[Dict]  = []

        for sd in scored:
            pid     = sd["property_id"]
            row_id  = today_by_pid.get(pid)
            payload = {
                "pid":      pid,
                "county":   sd["county_id"],
                "today":    today,
                "score":    sd["financing_intent_score"],
                "tier":     sd["intent_tier"],
                "product":  sd["recommended_product"],
                "flags":    json.dumps(sd["signal_flags"]),
                "scores":   json.dumps(sd["signal_scores"]),
                "details":  json.dumps(sd["signal_details"]),
                "sources":  json.dumps(sd["source_ids"]),
                "excluded": json.dumps(sd["excluded_reasons"]),
                "now":      now,
            }
            if row_id:
                updates.append((
                    row_id.id,
                    payload["score"], payload["tier"], payload["product"],
                    payload["flags"], payload["scores"], payload["details"],
                    payload["sources"], payload["excluded"], payload["now"],
                ))
            else:
                inserts.append(payload)

        if updates:
            raw_conn = self.session.connection().connection
            with raw_conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    UPDATE financing_intent_scores fis SET
                        financing_intent_score = v.score::numeric,
                        intent_tier            = v.tier,
                        recommended_product    = v.product,
                        signal_flags           = v.flags::jsonb,
                        signal_scores          = v.scores::jsonb,
                        signal_details         = v.details::jsonb,
                        source_ids             = v.sources::jsonb,
                        excluded_reasons       = v.excluded::jsonb,
                        updated_at             = v.now::timestamptz
                    FROM (VALUES %s) AS v(
                        id, score, tier, product,
                        flags, scores, details, sources, excluded, now
                    )
                    WHERE fis.id = v.id::bigint
                    """,
                    updates,
                    page_size=5000,
                )

        if inserts:
            self.session.execute(
                sa_text("""
                    INSERT INTO financing_intent_scores (
                        property_id, county_id, score_date,
                        financing_intent_score, intent_tier, recommended_product,
                        signal_flags, signal_scores, signal_details,
                        source_ids, excluded_reasons, updated_at
                    ) VALUES (
                        :pid, :county, :today,
                        :score, :tier, :product,
                        CAST(:flags AS jsonb), CAST(:scores AS jsonb),
                        CAST(:details AS jsonb),
                        CAST(:sources AS jsonb), CAST(:excluded AS jsonb),
                        :now
                    )
                    ON CONFLICT (property_id, score_date) DO UPDATE SET
                        financing_intent_score = EXCLUDED.financing_intent_score,
                        intent_tier            = EXCLUDED.intent_tier,
                        recommended_product    = EXCLUDED.recommended_product,
                        signal_flags           = EXCLUDED.signal_flags,
                        signal_scores          = EXCLUDED.signal_scores,
                        signal_details         = EXCLUDED.signal_details,
                        source_ids             = EXCLUDED.source_ids,
                        excluded_reasons       = EXCLUDED.excluded_reasons,
                        updated_at             = EXCLUDED.updated_at
                """),
                inserts,
            )

        return {
            "new":           len(inserts),
            "updated":       len(updates),
            "tier_counts":   tier_counter,
            "signal_counts": signal_counter,
        }

    # ── Public batch sweep ─────────────────────────────────────────────────────

    def score_properties(
        self,
        county_id: Optional[str] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
        rescore_all: bool = False,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Keyset-paginate properties; batch-fetch 5 signal sources; score in
        Python; persist in one round-trip batch.

        Returns aggregate stats dict.
        """
        today = date.today()
        page_size = batch_size if batch_size is not None else BATCH_SIZE

        totals: Dict[str, Any] = {
            "checked":   0,
            "scored":    0,
            "skipped":   0,
            "errors":    0,
            "new":       0,
            "updated":   0,
            "by_tier":   Counter(),
            "by_signal": Counter(),
        }

        last_id = 0
        while True:
            if limit is not None and totals["scored"] >= limit:
                break

            sql    = f"SELECT {_PROP_COLS} FROM properties WHERE id > :last_id"
            params: Dict[str, Any] = {"last_id": last_id, "n": page_size}
            if county_id:
                sql += " AND county_id = :county"
                params["county"] = county_id
            sql += " ORDER BY id LIMIT :n"

            prop_rows = self.session.execute(sa_text(sql), params).fetchall()
            if not prop_rows:
                break

            pids = [r.id for r in prop_rows]
            totals["checked"] += len(pids)

            # Skip today-already-scored properties unless rescore_all.
            if not rescore_all:
                done_pids: frozenset[int] = frozenset(
                    r.property_id
                    for r in self.session.execute(sa_text(f"""
                        SELECT property_id FROM financing_intent_scores
                        WHERE property_id IN ({_UNNEST})
                          AND score_date = :today
                    """), {"ids": pids, "today": today}).fetchall()
                )
                active_rows = [r for r in prop_rows if r.id not in done_pids]
                totals["skipped"] += len(done_pids)
            else:
                active_rows = prop_rows

            last_id = pids[-1]

            if not active_rows:
                continue

            active_pids = [r.id for r in active_rows]
            sm          = self._fetch_signals_for_batch(active_pids)
            scored_batch: List[Dict] = []

            for prop_row in active_rows:
                try:
                    sd = self._score_bundle(prop_row, sm)
                    if sd["signal_scores"]:
                        scored_batch.append(sd)
                except Exception:
                    totals["errors"] += 1
                    logger.exception("scoring error property_id=%s", prop_row.id)

            if scored_batch:
                result = self._persist_batch(scored_batch, today, dry_run)
                totals["scored"]    += len(scored_batch)
                totals["new"]       += result["new"]
                totals["updated"]   += result["updated"]
                totals["by_tier"]   += result["tier_counts"]
                totals["by_signal"] += result["signal_counts"]
                if not dry_run:
                    self.session.commit()

            logger.info(
                "progress: checked=%d scored=%d skipped=%d errors=%d",
                totals["checked"], totals["scored"],
                totals["skipped"], totals["errors"],
            )

        totals["by_tier"]   = dict(totals["by_tier"])
        totals["by_signal"] = dict(totals["by_signal"])
        return totals


# ── Public entry point ─────────────────────────────────────────────────────────

def score_properties_for_financing(
    session: Session,
    county_id: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    rescore_all: bool = False,
    batch_size: Optional[int] = None,
    auto_seed_lanes: bool = True,
) -> Dict[str, Any]:
    """Score properties for financing intent. Entry point for the CLI task.

    When auto_seed_lanes=True (default), newly qualified properties that do not
    yet have a lane are entered into the broker pool after scoring completes.
    Pass auto_seed_lanes=False for dry-run scoring passes.
    """
    totals = FinancingIntentScorer(session).score_properties(
        county_id=county_id,
        limit=limit,
        dry_run=dry_run,
        rescore_all=rescore_all,
        batch_size=batch_size,
    )

    if not dry_run and auto_seed_lanes and totals.get("scored", 0) > 0:
        from src.services.loan_lane_service import enter_lane
        from sqlalchemy import text as _text

        new_candidates = session.execute(
            _text("""
                WITH latest AS (
                    SELECT DISTINCT ON (property_id)
                        property_id, financing_intent_score
                    FROM financing_intent_scores
                    WHERE score_date = CURRENT_DATE
                    ORDER BY property_id, score_date DESC
                )
                SELECT l.property_id
                FROM latest l
                WHERE NOT EXISTS (
                    SELECT 1 FROM lanes ln
                    WHERE ln.property_id = l.property_id
                      AND ln.lane_type = 'distressed-payoff'
                )
                ORDER BY l.financing_intent_score DESC
            """),
        ).fetchall()

        lanes_created = 0
        for row in new_candidates:
            try:
                enter_lane(session, lane_type="distressed-payoff", property_id=row.property_id)
                lanes_created += 1
            except Exception:
                logger.exception("[LoanLane] auto-seed failed property_id=%s", row.property_id)

        if lanes_created:
            session.commit()
            logger.info("[LoanLane] auto-seeded %d new lanes from financing intent run", lanes_created)

        totals["lanes_seeded"] = lanes_created

    if not dry_run:
        try:
            from src.services.lane_description_service import run_batch
            desc_counts = run_batch(session)
            totals["descriptions_generated"] = desc_counts["generated"]
            totals["descriptions_failed"] = desc_counts["failed"]
        except Exception:
            logger.exception("[LaneDesc] batch run failed — descriptions skipped")

    return totals
