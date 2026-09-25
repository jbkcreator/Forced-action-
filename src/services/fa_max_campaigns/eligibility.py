"""Campaign eligibility rules — plan Section 6.3.

One function per v1 campaign. Each runs a single bounded SQL query per
sweep (never one query per person) and returns candidate dicts with enough
context for selection.py to enroll and for content.resolve_merge_values()
to fill the writer's templates.

A candidate always carries a resolved fa_max_persons row with a usable
email or phone — a linked property/entity with no resolved person or
contact is counted (unresolved_count) and logged, never enrolled (plan
Section 6.3: "That number shows how much identity or skip-trace work
remains"). No new identity-resolution logic is built here; this task reads
whatever WP-3/WP-4's resolver and partner mining have already produced.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config import fa_max_campaigns as cfg

# Mirrors src.services.partner_mining.investor_txn._ENTITY_TOKENS — kept as
# a local SQL-friendly copy (that module's list is private and used inside
# Python object checks, not composable into a WHERE clause) rather than a
# second, silently-diverging definition of "looks like an entity buyer".
_ENTITY_SUFFIX_PATTERNS = (
    "% llc", "% l l c", "%llc", "% corp", "% corporation", "% inc",
    "% incorporated", "% trust", "% lp", "% ltd",
)


@dataclass
class EligibilityCandidate:
    person_id: str
    audience: str  # investor | partner
    trigger_type: str
    trigger_reason: str
    property_id: Optional[int] = None
    county_id: Optional[str] = None
    state: Optional[str] = "FL"
    extra: dict = field(default_factory=dict)  # merge-field context (entity_name, purchase_date, ...)


@dataclass
class EligibilityResult:
    candidates: list[EligibilityCandidate]
    unresolved_count: int  # matched the pattern but no resolved person/contact


def _entity_grantee_clause(alias: str) -> str:
    ors = " OR ".join(f"LOWER({alias}.grantee) LIKE '{pat}'" for pat in _ENTITY_SUFFIX_PATTERNS)
    return f"({ors})"


# ── Capital Desk Loop ────────────────────────────────────────────────────────

_CASH_BUYER_SQL = text(
    f"""
    SELECT
        fpp.person_id,
        d.property_id,
        p.county_id,
        d.grantee AS entity_name,
        d.record_date AS purchase_date
    FROM deeds d
    JOIN properties p ON p.id = d.property_id
    JOIN buyer_entity_links bel ON bel.source_table = 'deeds' AND bel.source_id = d.id
    JOIN fa_max_person_profiles fpp ON fpp.buyer_entity_id = bel.buyer_entity_id
    WHERE d.sale_price IS NOT NULL AND d.sale_price > 0
      AND d.mortgage_amount IS NULL
      AND d.record_date IS NOT NULL
      AND d.record_date >= :since
      AND {_entity_grantee_clause('d')}
      AND NOT EXISTS (
          SELECT 1 FROM deeds d2
          WHERE d2.property_id = d.property_id
            AND d2.mortgage_amount IS NOT NULL AND d2.mortgage_amount > 0
            AND d2.record_date IS NOT NULL
            AND d2.record_date >= d.record_date
            AND d2.record_date < d.record_date + (:no_mortgage_hours || ' hours')::interval
      )
    """
)

_ACTIVE_INVESTOR_SQL = text(
    """
    SELECT
        fpp.person_id,
        COUNT(*) AS purchase_count,
        MAX(d.property_id) AS property_id,
        MAX(p.county_id) AS county_id
    FROM deeds d
    JOIN properties p ON p.id = d.property_id
    JOIN buyer_entity_links bel ON bel.source_table = 'deeds' AND bel.source_id = d.id
    JOIN fa_max_person_profiles fpp ON fpp.buyer_entity_id = bel.buyer_entity_id
    WHERE d.record_date IS NOT NULL
      AND d.record_date >= :since
      AND d.sale_qualified IS TRUE
      AND d.sale_price IS NOT NULL AND d.sale_price > 0
    GROUP BY fpp.person_id
    HAVING COUNT(*) >= :min_purchases
    """
)

_WHOLESALER_SQL = text(
    """
    SELECT partner_id, person_id, county_id
    FROM fa_max_partners
    WHERE partner_class = :partner_class
      AND rank IS NOT NULL AND rank <= :top_n
    """
)


def capital_desk_loop_candidates(session: Session) -> EligibilityResult:
    if not cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_CAPITAL_DESK_LOOP]:
        return EligibilityResult([], 0)

    candidates: dict[str, EligibilityCandidate] = {}
    unresolved = 0

    since_cash = datetime.now(timezone.utc) - timedelta(days=cfg.CASH_BUYER_RECENT_DAYS)
    cash_rows = session.execute(
        _CASH_BUYER_SQL,
        {"since": since_cash, "no_mortgage_hours": cfg.CASH_BUYER_NO_MORTGAGE_WINDOW_HOURS},
    ).mappings().all()
    for row in cash_rows:
        candidates[str(row["person_id"])] = EligibilityCandidate(
            person_id=str(row["person_id"]),
            audience="investor",
            trigger_type="cash_purchase",
            trigger_reason=f"Recent cash purchase, no mortgage within {cfg.CASH_BUYER_NO_MORTGAGE_WINDOW_HOURS}h",
            property_id=row["property_id"],
            county_id=row["county_id"],
            extra={"entity_name": row["entity_name"], "purchase_date": row["purchase_date"]},
        )

    since_investor = datetime.now(timezone.utc) - timedelta(days=30 * cfg.ACTIVE_INVESTOR_WINDOW_MONTHS)
    investor_rows = session.execute(
        _ACTIVE_INVESTOR_SQL,
        {"since": since_investor, "min_purchases": cfg.ACTIVE_INVESTOR_MIN_PURCHASES},
    ).mappings().all()
    for row in investor_rows:
        pid = str(row["person_id"])
        if pid in candidates:
            continue
        candidates[pid] = EligibilityCandidate(
            person_id=pid,
            audience="investor",
            trigger_type="active_investor",
            trigger_reason=f"{row['purchase_count']} purchases in {cfg.ACTIVE_INVESTOR_WINDOW_MONTHS} months",
            property_id=row["property_id"],
            county_id=row["county_id"],
            extra={"purchase_count_24m": row["purchase_count"]},
        )

    wholesaler_rows = session.execute(
        _WHOLESALER_SQL,
        {"partner_class": cfg.WHOLESALER_PARTNER_CLASS, "top_n": cfg.WHOLESALER_TOP_N},
    ).mappings().all()
    for row in wholesaler_rows:
        pid = str(row["person_id"])
        if pid in candidates:
            continue
        candidates[pid] = EligibilityCandidate(
            person_id=pid,
            audience="partner",
            trigger_type="wholesaler",
            trigger_reason="Ranked wholesaler partner (top N by observed transaction volume)",
            county_id=row["county_id"],
        )

    resolved, unresolved_extra = _filter_reachable(session, list(candidates.values()))
    unresolved += unresolved_extra
    return EligibilityResult(resolved, unresolved)


# ── Exit Desk (off until bought data lands — plan Gap C / D-1) ──────────────

def exit_desk_candidates(session: Session) -> EligibilityResult:
    if not cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK]:
        return EligibilityResult([], 0)

    lender_types = tuple(cfg.LENDING_MORTGAGE_LENDER_TYPES)
    sql = text(
        f"""
        SELECT
            fpp.person_id,
            m.property_id,
            m.county_id,
            m.state,
            m.borrower_entity AS entity_name,
            m.recording_date,
            -- EXTRACT(MONTH FROM AGE(...)) alone only returns 0-11 (the
            -- month component of the interval) and wraps every 12 months —
            -- a 14-month-old loan would report as "2 months old" in the
            -- outbound message. Total elapsed months is years*12 + months.
            (EXTRACT(YEAR FROM AGE(NOW(), m.recording_date)) * 12
                + EXTRACT(MONTH FROM AGE(NOW(), m.recording_date)))::int AS loan_age_months
        FROM {cfg.LENDING_MORTGAGE_RECORDS_TABLE} m
        JOIN buyer_entity_links bel ON bel.source_table = 'deeds' AND bel.source_id = m.property_id
        JOIN fa_max_person_profiles fpp ON fpp.buyer_entity_id = bel.buyer_entity_id
        WHERE m.lender_type IN :lender_types
          AND m.satisfied_bool IS NOT TRUE
          AND m.recording_date <= NOW() - (:min_months || ' months')::interval
          AND m.recording_date >= NOW() - (:max_months || ' months')::interval
        """
    ).bindparams(bindparam("lender_types", expanding=True))

    try:
        rows = session.execute(
            sql,
            {
                "lender_types": list(lender_types),
                "min_months": cfg.EXIT_DESK_MIN_LOAN_AGE_MONTHS,
                "max_months": cfg.EXIT_DESK_MAX_LOAN_AGE_MONTHS,
            },
        ).mappings().all()
    except Exception:
        # The bought-data table doesn't exist yet in most environments —
        # switched off in config (CAMPAIGN_ENABLED), this is the fixture/
        # test-time guard for the same condition.
        return EligibilityResult([], 0)

    candidates = [
        EligibilityCandidate(
            person_id=str(row["person_id"]),
            audience="investor",
            trigger_type="maturity_rescue",
            trigger_reason=f"Unsatisfied {row['county_id']} hard-money mortgage, {int(row['loan_age_months'])}mo old",
            property_id=row["property_id"],
            county_id=row["county_id"],
            state=row["state"],
            extra={"entity_name": row["entity_name"], "loan_age_months": int(row["loan_age_months"])},
        )
        for row in rows
    ]
    resolved, unresolved = _filter_reachable(session, candidates)
    return EligibilityResult(resolved, unresolved)


# ── Rescue Circuit ───────────────────────────────────────────────────────────

_RESCUE_CIRCUIT_SQL = text(
    """
    SELECT partner_id, person_id, county_id, partner_class
    FROM fa_max_partners
    WHERE partner_class = ANY(:classes)
      AND source = :source
    """
)


def rescue_circuit_candidates(session: Session) -> EligibilityResult:
    if not cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_RESCUE_CIRCUIT]:
        return EligibilityResult([], 0)

    rows = session.execute(
        _RESCUE_CIRCUIT_SQL,
        {
            "classes": list(cfg.RESCUE_CIRCUIT_PARTNER_CLASSES),
            "source": cfg.RESCUE_CIRCUIT_PARTNER_SOURCE,
        },
    ).mappings().all()

    candidates = [
        EligibilityCandidate(
            person_id=str(row["person_id"]),
            audience="partner",
            trigger_type="rescue_circuit_partner",
            trigger_reason=f"Imported partner list — {row['partner_class']}",
            county_id=row["county_id"],
            extra={"partner_type": row["partner_class"]},
        )
        for row in rows
    ]
    resolved, unresolved = _filter_reachable(session, candidates)
    return EligibilityResult(resolved, unresolved)


# ── Shared: only resolved, reachable people become real candidates ─────────

def _filter_reachable(
    session: Session, candidates: list[EligibilityCandidate],
) -> tuple[list[EligibilityCandidate], int]:
    """A candidate qualifies only with a non-merged fa_max_persons row and a
    usable email or phone (plan Section 6.3). Everything else is dropped and
    counted, not enrolled."""
    if not candidates:
        return [], 0

    person_ids = list({c.person_id for c in candidates})
    rows = session.execute(
        text(
            "SELECT person_id::text AS person_id FROM fa_max_persons "
            "WHERE person_id::text = ANY(:ids) AND merged_into_id IS NULL "
            "AND (email IS NOT NULL OR phone IS NOT NULL)"
        ),
        {"ids": person_ids},
    ).scalars().all()
    reachable_ids = set(rows)

    resolved = [c for c in candidates if c.person_id in reachable_ids]
    unresolved = len(candidates) - len(resolved)
    return resolved, unresolved
