"""
Probate multi-heir prevalence audit.

Answers the gate question behind MULTI_HEIR_ENRICHMENT_ENABLED:
  "What fraction of probate-derived properties name 2+ skip-traceable heirs?"

If that fraction is >20%, enabling multi-heir fan-out in skip_trace materially
widens reachable contacts and is worth the extra BatchData spend.

Counting mirrors skip_trace.py exactly so the number matches what the
enrichment will actually fan out on:
  - Probate LegalProceeding rows with a non-empty secondary_party.
  - Most recent filing per property wins (order by filing_date desc).
  - Heirs read from meta_data->'heirs', deduped case-insensitively, with a
    fallback to secondary_party when the list is absent.
  - Entity-looking names (LLC/Estate/Trust/...) excluded via _is_entity_name,
    since BatchData cannot trace an organization.

Usage:
    python -m scripts._probate_multi_heir_audit
    python -m scripts._probate_multi_heir_audit --county-id hillsborough --show-examples 10
"""

import argparse
from collections import Counter

from src.core.database import get_db_context
from src.core.models import LegalProceeding
from src.services.skip_trace import _is_entity_name
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

THRESHOLD_PCT = 20.0


def _valid_heirs_from_meta(meta, secondary_party) -> list:
    """Replicate skip_trace's heir extraction: dedup + entity filter."""
    heirs: list = []
    if isinstance(meta, dict):
        raw_heirs = meta.get("heirs")
        if isinstance(raw_heirs, list):
            seen = set()
            for h in raw_heirs:
                if not h:
                    continue
                name = str(h).strip()
                key = name.upper()
                if not name or key in seen:
                    continue
                seen.add(key)
                heirs.append(name)
    if not heirs and secondary_party:
        heirs = [str(secondary_party).strip()]
    return [h for h in heirs if h and not _is_entity_name(h)]


def run_audit(county_id: str | None = None, show_examples: int = 0) -> dict:
    with get_db_context() as session:
        q = session.query(
            LegalProceeding.property_id,
            LegalProceeding.case_number,
            LegalProceeding.secondary_party,
            LegalProceeding.meta_data,
        ).filter(
            LegalProceeding.record_type == "Probate",
            LegalProceeding.secondary_party.isnot(None),
        ).order_by(LegalProceeding.filing_date.desc())
        if county_id:
            q = q.filter(LegalProceeding.county_id == county_id)
        rows = q.all()

        # Most-recent filing per property wins (matches skip_trace dedup).
        per_property: dict = {}
        for pid, case_number, secondary_party, meta in rows:
            if pid in per_property:
                continue
            per_property[pid] = _valid_heirs_from_meta(meta, secondary_party)

        total = len(per_property)
        dist = Counter(len(h) for h in per_property.values())
        multi = sum(1 for h in per_property.values() if len(h) >= 2)
        pct = (multi / total * 100.0) if total else 0.0

        logger.info("=" * 60)
        logger.info("PROBATE MULTI-HEIR AUDIT%s", f" — {county_id}" if county_id else "")
        logger.info("=" * 60)
        logger.info("Probate properties (deduped):        %d", total)
        logger.info("Properties with 2+ traceable heirs:  %d", multi)
        logger.info("Multi-heir prevalence:               %.1f%%  (threshold %.0f%%)", pct, THRESHOLD_PCT)
        logger.info("-" * 60)
        logger.info("Heir-count distribution (valid, non-entity heirs):")
        for n in sorted(dist):
            logger.info("  %2d heir(s): %5d propert%s", n, dist[n], "y" if dist[n] == 1 else "ies")
        logger.info("-" * 60)
        verdict = "ABOVE" if pct > THRESHOLD_PCT else "below"
        logger.info("VERDICT: %.1f%% is %s the %.0f%% gate — %s",
                    pct, verdict, THRESHOLD_PCT,
                    "enabling multi-heir fan-out is justified."
                    if pct > THRESHOLD_PCT else
                    "fan-out adds little; keep the flag off.")

        if show_examples:
            logger.info("-" * 60)
            logger.info("Sample multi-heir properties (up to %d):", show_examples)
            shown = 0
            for pid, heirs in per_property.items():
                if len(heirs) >= 2:
                    logger.info("  property_id=%s: %s", pid, heirs)
                    shown += 1
                    if shown >= show_examples:
                        break

        return {
            "total": total,
            "multi_heir": multi,
            "prevalence_pct": round(pct, 1),
            "above_threshold": pct > THRESHOLD_PCT,
            "distribution": dict(dist),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit probate multi-heir prevalence")
    parser.add_argument("--county-id", dest="county_id", default=None,
                        help="Restrict to a single county (default: all)")
    parser.add_argument("--show-examples", dest="show_examples", type=int, default=0,
                        help="Print N sample multi-heir properties")
    args = parser.parse_args()
    run_audit(county_id=args.county_id, show_examples=args.show_examples)
