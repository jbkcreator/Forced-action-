"""
Cross-source contact triangulation engine (ADR 0015).

Cross-checks contact points across three tiers — paid skip-trace providers
(enriched_contacts), the SOE voter registry (voters), and identity anchors
(appraiser/tax-collector mailing data) — and feeds the corroboration verdict
into the fa073 contact-freshness label (owners.contact_info_confidence).
Evidence is persisted to owners.contactability_detail.

Split (same shape as cds_engine):
  compute_corroboration()        pure classifier, no DB access
  TriangulationService           set-based sweep + single-owner path

CLI:
  python -m src.services.contact_triangulation --sweep [--county-id X] [--dry-run]
  python -m src.services.contact_triangulation --owner-id 123
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.triangulation import (
    EMAIL_DISPOSABLE_DOMAINS,
    EMAIL_MATCH_IS_IDENTITY_ANCHOR,
    EMAIL_WEAK_LOCALPARTS,
    HISTORICAL_VOTER_PHONE_IS_WEAK,
    INA_VOTER_IS_WEAK,
    NAME_AGREEMENT_MIN,
)
from src.services.contact_freshness import compute_contact_freshness
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

GOLD_PLUS_TIERS = ("Ultra Platinum", "Platinum", "Gold")

# Used to detect label downgrades (plan step 8: a downgrade marks the owner
# due for re-trace even when the new label alone wouldn't).
_LABEL_RANK = {"high": 3, "medium": 2, "low": 1, "stale": 0}

# Tracerfy raw_response phone/email field names (confirmed 2026-06-03 in
# tracerfy_fallback._parse_trace_row). Other providers degrade gracefully —
# missing keys simply contribute nothing.
_RAW_PHONE_KEYS = (
    "primary_phone",
    *(f"mobile_{i}" for i in range(1, 6)),
    *(f"landline_{i}" for i in range(1, 4)),
)
_RAW_MOBILE_KEYS = frozenset({"primary_phone", *(f"mobile_{i}" for i in range(1, 6))})
_RAW_EMAIL_KEYS = tuple(f"email_{i}" for i in range(1, 6))


# ─── pure classifier ─────────────────────────────────────────────────────────

def _natural_name(name: Optional[str]) -> str:
    """Flip 'Last, First M' (voter file) to 'First M Last'."""
    if not name:
        return ""
    if "," in name:
        last, _, rest = name.partition(",")
        return f"{rest.strip()} {last.strip()}".strip()
    return name.strip()


def _name_agrees(candidate: Optional[str], owner_names: List[str],
                 min_ratio: int = NAME_AGREEMENT_MIN) -> bool:
    """Fuzzy name agreement after the loaders' canonical name normalization."""
    if not candidate or not owner_names:
        return False
    from rapidfuzz import fuzz

    from src.loaders.base import BaseLoader

    cand = BaseLoader.normalize_owner_name(_natural_name(candidate))
    if not cand:
        return False
    for owner_name in owner_names:
        ref = BaseLoader.normalize_owner_name(_natural_name(owner_name))
        if ref and fuzz.token_set_ratio(cand, ref) >= min_ratio:
            return True
    return False


def _email_is_weak_identity(email: str) -> bool:
    local, _, domain = email.partition("@")
    return local.lower() in EMAIL_WEAK_LOCALPARTS or domain.lower() in EMAIL_DISPOSABLE_DOMAINS


def _addresses_agree(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return False
    from rapidfuzz import fuzz
    return fuzz.token_set_ratio(a.casefold().strip(), b.casefold().strip()) >= 90


@dataclass
class PersonEvidence:
    """All contact points attributed to one traced person."""
    name: Optional[str] = None                      # traced_name; None = assessor owner
    phones: List[Dict[str, str]] = field(default_factory=list)   # {phone, source, kind}
    emails: List[Dict[str, str]] = field(default_factory=list)   # {email, source}
    mailing_address: Optional[str] = None           # from the trace result


def compute_corroboration(
    persons: List[PersonEvidence],
    voters: List[Dict[str, Any]],
    owner_names: List[str],
    anchor_addresses: List[str],
    phone_meta_by_number: Optional[Dict[str, dict]] = None,
    *,
    name_agreement_min: int = NAME_AGREEMENT_MIN,
) -> Tuple[str, Dict[str, Any]]:
    """
    Pure cross-source classifier. Returns (corroboration, detail) where
    corroboration is 'strong' | 'weak' | 'none'.

    voters entries: {name, phone_current, phones_history, email, active}.
    All phones must already be strict E.164 (junk is rejected upstream by
    phone_utils.normalize returning None).
    """
    phone_meta_by_number = phone_meta_by_number or {}
    best: Tuple[int, str, Dict[str, Any]] = (0, "none", {})   # (rank, level, detail)

    def consider(rank: int, level: str, detail: Dict[str, Any]) -> None:
        nonlocal best
        if rank > best[0]:
            best = (rank, level, detail)

    for person in persons:
        sources_by_phone: Dict[str, set] = {}
        for entry in person.phones:
            sources_by_phone.setdefault(entry["phone"], set()).add(entry["source"])

        # email corroboration for this person (evaluated first — it can also
        # serve as the identity anchor for the single-source-mobile rule)
        email_corr, matched_email = "none", None
        voter_emails = {
            (v.get("email") or "").lower().strip(): v
            for v in voters if v.get("email")
        }
        for entry in person.emails:
            email = entry["email"].lower().strip()
            voter = voter_emails.get(email)
            if not voter:
                continue
            if _email_is_weak_identity(email) or not _name_agrees(
                voter.get("name"), owner_names, name_agreement_min
            ):
                if email_corr == "none":
                    email_corr, matched_email = "weak", email
            else:
                email_corr, matched_email = "strong", email
                break

        base_detail = {
            "person": person.name,
            "email_corroboration": email_corr,
            "matched_email": matched_email,
        }

        # Rule 1 — same phone from two independent paid providers
        for phone, sources in sources_by_phone.items():
            if len(sources) >= 2:
                consider(5, "strong", {
                    **base_detail, "matched_phone": phone,
                    "sources": sorted(sources), "rule_fired": "cross_provider_match",
                })

        # Rule 2 — phone confirmed by the voter registry
        for voter in voters:
            v_current = voter.get("phone_current")
            v_history = set(voter.get("phones_history") or [])
            v_active = bool(voter.get("active", True))
            names_ok = _name_agrees(voter.get("name"), owner_names, name_agreement_min)
            for phone, sources in sources_by_phone.items():
                if v_current and phone == v_current:
                    if names_ok and v_active:
                        consider(5, "strong", {
                            **base_detail, "matched_phone": phone,
                            "sources": sorted(sources) + ["voter_current"],
                            "rule_fired": "cross_source_name_match",
                        })
                    elif names_ok and not v_active and INA_VOTER_IS_WEAK:
                        consider(2, "weak", {
                            **base_detail, "matched_phone": phone,
                            "sources": sorted(sources) + ["voter_current"],
                            "rule_fired": "inactive_voter",
                        })
                    else:
                        consider(2, "weak", {
                            **base_detail, "matched_phone": phone,
                            "sources": sorted(sources) + ["voter_current"],
                            "rule_fired": "phone_match_no_name_agreement",
                        })
                elif phone in v_history and HISTORICAL_VOTER_PHONE_IS_WEAK:
                    consider(1, "weak", {
                        **base_detail, "matched_phone": phone,
                        "sources": sorted(sources) + ["voter_history"],
                        "rule_fired": "historical_voter_phone",
                    })

        # Rule 3 — identity anchor (mailing-address or strong-email agreement)
        # lifts a single-source mobile that isn't known-unreachable
        anchors_agree = any(
            _addresses_agree(person.mailing_address, anchor)
            for anchor in anchor_addresses
        )
        identity_proven = anchors_agree or (
            EMAIL_MATCH_IS_IDENTITY_ANCHOR and email_corr == "strong"
        )
        if identity_proven:
            for entry in person.phones:
                if entry.get("kind") != "mobile":
                    continue
                meta = phone_meta_by_number.get(entry["phone"], {})
                if meta.get("reachable") is False:
                    continue
                consider(4, "strong", {
                    **base_detail, "matched_phone": entry["phone"],
                    "sources": [entry["source"]] + (["mailing_anchor"] if anchors_agree else ["email_anchor"]),
                    "rule_fired": "identity_anchor_match",
                })
                break

        # Email-only weak signal (no phone rules fired for this person)
        if email_corr != "none":
            consider(1 if email_corr == "weak" else 2, "weak", {
                **base_detail,
                "matched_phone": None,
                "sources": ["email"],
                "rule_fired": "email_agreement_only",
            })

    rank, level, detail = best
    if rank == 0:
        detail = {"rule_fired": "no_corroboration", "person": None,
                  "matched_phone": None, "sources": [],
                  "email_corroboration": "none", "matched_email": None}
    return level, detail


# ─── service ─────────────────────────────────────────────────────────────────

_SWEEP_QUERY = text("""
    SELECT ec.property_id,
           ec.source, ec.mobile_phone, ec.landline, ec.email,
           ec.traced_name, ec.mailing_address AS ec_mailing, ec.llc_owner_name,
           ec.raw_response, ec.enriched_at, ec.confidence, ec.verification_status,
           o.id AS owner_id, o.owner_name, o.managing_members,
           o.registered_agent_name,
           o.mailing_address AS owner_mailing, o.phone_metadata,
           o.phone_1, o.phone_2, o.phone_3, o.email_1,
           o.contact_info_confidence AS prev_label
    FROM enriched_contacts ec
    JOIN owners o ON o.property_id = ec.property_id
    JOIN LATERAL (
        SELECT ds.lead_tier
        FROM distress_scores ds
        WHERE ds.property_id = ec.property_id
        ORDER BY ds.score_date DESC
        LIMIT 1
    ) latest ON latest.lead_tier = ANY(:tiers)
    WHERE ec.match_success
      AND ec.superseded_at IS NULL
      AND (CAST(:county AS varchar) IS NULL OR ec.county_id = :county)
      AND (CAST(:owner_id AS integer) IS NULL OR o.id = :owner_id)
    ORDER BY ec.property_id, ec.enriched_at DESC
""")

_VOTERS_QUERY = text("""
    SELECT property_id, voter_name, registration_status, phone_1, phones, email
    FROM voters
    WHERE property_id = ANY(:pids)
      AND (phone_1 IS NOT NULL OR phones IS NOT NULL OR email IS NOT NULL)
""")

_WRITE_QUERY = text("""
    UPDATE owners AS o SET
        contact_info_confidence       = v.label,
        contact_info_confidence_score = CAST(v.score AS numeric),
        contact_last_verified_at      = CAST(v.verified_at AS timestamptz),
        contact_next_refresh_at       = CAST(v.refresh_at AS timestamptz),
        contact_refresh_status        = v.refresh_status,
        contact_refresh_reason        = v.reason,
        contactability_detail         = CAST(v.detail AS jsonb)
    FROM (VALUES (:owner_id, :label, :score, :verified_at, :refresh_at,
                  :refresh_status, :reason, :detail))
         AS v(owner_id, label, score, verified_at, refresh_at,
              refresh_status, reason, detail)
    WHERE o.id = v.owner_id
""")


class _OwnerView:
    """Duck-typed owner for compute_contact_freshness (bulk path avoids ORM loads)."""

    def __init__(self, row):
        self.phone_1 = row.phone_1
        self.phone_2 = row.phone_2
        self.phone_3 = row.phone_3
        self.phone_metadata = row.phone_metadata


class _ContactView:
    def __init__(self, confidence, enriched_at, verification_status):
        self.confidence = confidence
        self.enriched_at = enriched_at
        self.verification_status = verification_status


def _extract_phones(row) -> List[Dict[str, str]]:
    """Phones from parsed columns + the provider's full raw_response lists."""
    out: List[Dict[str, str]] = []
    seen = set()

    def add(raw: Optional[str], kind: str) -> None:
        phone = normalize_phone(raw)
        if phone and phone not in seen:
            seen.add(phone)
            out.append({"phone": phone, "source": row.source, "kind": kind})

    add(row.mobile_phone, "mobile")
    add(row.landline, "landline")
    raw = row.raw_response if isinstance(row.raw_response, dict) else {}
    for key in _RAW_PHONE_KEYS:
        add(raw.get(key), "mobile" if key in _RAW_MOBILE_KEYS else "landline")
    return out


def _extract_emails(row) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen = set()

    def add(raw: Optional[str]) -> None:
        email = (raw or "").lower().strip()
        if email and "@" in email and email not in seen:
            seen.add(email)
            out.append({"email": email, "source": row.source})

    add(row.email)
    raw = row.raw_response if isinstance(row.raw_response, dict) else {}
    for key in _RAW_EMAIL_KEYS:
        add(raw.get(key))
    return out


class TriangulationService:
    """Set-based sweep driven from the enriched_contacts side (the small one)."""

    def __init__(self, session: Session):
        self.session = session

    # ── evidence assembly ────────────────────────────────────────────────────

    def _fetch(self, county_id: Optional[str], owner_id: Optional[int]):
        rows = self.session.execute(
            _SWEEP_QUERY,
            {"tiers": list(GOLD_PLUS_TIERS), "county": county_id, "owner_id": owner_id},
        ).fetchall()
        pids = sorted({r.property_id for r in rows})
        voters_by_pid: Dict[int, List[dict]] = {}
        if pids:
            for v in self.session.execute(_VOTERS_QUERY, {"pids": pids}).fetchall():
                voters_by_pid.setdefault(v.property_id, []).append({
                    "name": v.voter_name,
                    "phone_current": normalize_phone(v.phone_1),
                    "phones_history": [
                        p for p in (normalize_phone(x) for x in (v.phones or [])) if p
                    ],
                    "email": (v.email or "").lower().strip() or None,
                    "active": (v.registration_status or "ACT") == "ACT",
                })
        return rows, voters_by_pid

    @staticmethod
    def _group_by_property(rows) -> Dict[int, List]:
        grouped: Dict[int, List] = {}
        for r in rows:
            grouped.setdefault(r.property_id, []).append(r)
        return grouped

    @staticmethod
    def _build_evidence(prop_rows) -> Tuple[List[PersonEvidence], List[str], List[str], Dict[str, dict]]:
        head = prop_rows[0]
        owner_names = [n for n in (
            head.owner_name,
            head.registered_agent_name,
        ) if n]
        owner_names.extend({r.llc_owner_name for r in prop_rows if r.llc_owner_name})
        for member in (head.managing_members or []):
            if isinstance(member, dict) and member.get("name"):
                owner_names.append(member["name"])

        anchors = [a for a in {head.owner_mailing} if a]
        persons: Dict[Optional[str], PersonEvidence] = {}
        for r in prop_rows:
            if r.source == "tax_collector":
                if r.ec_mailing:
                    anchors.append(r.ec_mailing)
                continue
            key = r.traced_name or None
            person = persons.setdefault(key, PersonEvidence(name=r.traced_name))
            person.phones.extend(_extract_phones(r))
            person.emails.extend(_extract_emails(r))
            if r.ec_mailing and not person.mailing_address:
                person.mailing_address = r.ec_mailing

        meta_by_number: Dict[str, dict] = {}
        meta = head.phone_metadata if isinstance(head.phone_metadata, dict) else {}
        for slot in ("phone_1", "phone_2", "phone_3"):
            number = normalize_phone(getattr(head, slot))
            slot_meta = meta.get(slot)
            if number and isinstance(slot_meta, dict):
                meta_by_number[number] = slot_meta
        return list(persons.values()), owner_names, anchors, meta_by_number

    # ── sweep ────────────────────────────────────────────────────────────────

    def run_sweep(
        self,
        county_id: Optional[str] = None,
        dry_run: bool = False,
        owner_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        logger.info("[Triangulation-Debug] sweep start county=%s dry_run=%s owner_id=%s",
                    county_id or "all", dry_run, owner_id)
        fetch_started = datetime.now(timezone.utc)
        rows, voters_by_pid = self._fetch(county_id, owner_id)
        logger.info(
            "[Triangulation-Debug] fetch done rows=%d voter_properties=%d elapsed_ms=%d",
            len(rows),
            len(voters_by_pid),
            int((datetime.now(timezone.utc) - fetch_started).total_seconds() * 1000),
        )
        grouped = self._group_by_property(rows)
        logger.info("[Triangulation] %d EC rows across %d properties (county=%s)",
                    len(rows), len(grouped), county_id or "all")

        stats = {"evaluated": 0, "changed": 0, "downgrades": 0,
                 "distribution": {}, "corroboration": {"strong": 0, "weak": 0, "none": 0},
                 "rules": {}, "changed_property_ids": []}
        updates: List[dict] = []

        for pid, prop_rows in grouped.items():
            persons, owner_names, anchors, meta_by_number = self._build_evidence(prop_rows)
            if not persons:
                continue
            if stats["evaluated"] and stats["evaluated"] % 500 == 0:
                logger.info(
                    "[Triangulation-Debug] progress evaluated=%d changed=%d downgrades=%d",
                    stats["evaluated"],
                    stats["changed"],
                    stats["downgrades"],
                )
            level, detail = compute_corroboration(
                persons, voters_by_pid.get(pid, []), owner_names, anchors,
                meta_by_number,
            )

            head = prop_rows[0]
            latest = max(prop_rows, key=lambda r: r.enriched_at or datetime.min)
            freshness = compute_contact_freshness(
                _OwnerView(head),
                _ContactView(latest.confidence, latest.enriched_at, latest.verification_status),
                now=now,
                corroboration=level if level != "none" else None,
            )

            stats["evaluated"] += 1
            stats["corroboration"][level] += 1
            rule = detail.get("rule_fired", "unknown")
            stats["rules"][rule] = stats["rules"].get(rule, 0) + 1
            stats["distribution"][freshness.level] = (
                stats["distribution"].get(freshness.level, 0) + 1
            )
            if freshness.level != head.prev_label:
                stats["changed"] += 1
                stats["changed_property_ids"].append(pid)

            # A downgrade is a contradiction signal — force the owner back
            # into the refresh queue even if the new label is still 'medium'.
            downgraded = bool(
                head.prev_label
                and _LABEL_RANK.get(freshness.level, 0) < _LABEL_RANK.get(head.prev_label, 0)
            )
            if downgraded:
                stats["downgrades"] += 1

            detail.update({
                "corroboration": level,
                "computed_at": now.isoformat(),
                "prev_label": head.prev_label,
            })
            updates.append({
                "owner_id": head.owner_id,
                "label": freshness.level,
                "score": freshness.score,
                "verified_at": freshness.last_verified_at,
                "refresh_at": now if downgraded else freshness.next_refresh_at,
                "refresh_status": "due" if downgraded else freshness.refresh_status,
                "reason": "label_downgrade" if downgraded else freshness.reason[:120],
                "detail": json.dumps(detail),
            })

        if updates and not dry_run:
            logger.info("[Triangulation-Debug] writing_updates count=%d", len(updates))
            self.session.execute(_WRITE_QUERY, updates)
            self.session.commit()
            logger.info("[Triangulation] wrote %d labels (%d changed, %d downgraded)",
                        len(updates), stats["changed"], stats["downgrades"])
            # Sweep mode only — the single-owner inline hook (run_for_owner)
            # must not publish per-owner events or fire per-owner rescores.
            if owner_id is None:
                dispatched = self._rescore_if_enabled(stats["changed_property_ids"])
                self._publish_sweep_event(county_id, stats, now, dispatched)
        elif dry_run:
            logger.info("[Triangulation] DRY-RUN: would write %d labels (%d changed) | %s",
                        len(updates), stats["changed"], stats["distribution"])
        return stats

    def _rescore_if_enabled(self, changed_property_ids: list) -> bool:
        from config.settings import get_settings
        if not get_settings().cds_use_contactability or not changed_property_ids:
            return False
        try:
            from src.core.database import get_db_context
            from src.services.cds_engine import MultiVerticalScorer
            with get_db_context() as rescore_session:
                MultiVerticalScorer(rescore_session).score_properties_by_ids(changed_property_ids)
            logger.info("[Triangulation] delta-rescore fired for %d properties",
                        len(changed_property_ids))
            return True
        except Exception:
            logger.warning("[Triangulation] delta-rescore failed", exc_info=True)
            return False

    @staticmethod
    def _publish_sweep_event(county_id, stats: dict, now: datetime,
                             scoring_delta_dispatched: bool) -> None:
        try:
            from src.agents.events.ingestion import publish_cora_event
            publish_cora_event({
                "event_type": "contactability_sweep_completed",
                "payload": {
                    "county_id": county_id,
                    "distribution": stats["distribution"],
                    "corroboration": stats["corroboration"],
                    "changed": stats["changed"],
                    "downgrades": stats["downgrades"],
                    "scoring_delta_dispatched": scoring_delta_dispatched,
                    "run_at": now.isoformat(),
                },
            })
        except Exception:
            logger.warning("[Triangulation] Cora event publish failed", exc_info=True)

    def run_for_owner(self, owner_id: int) -> Optional[Dict[str, Any]]:
        """Single-owner recompute — the waterfall inline hook (PR4 wiring)."""
        return self.run_sweep(owner_id=owner_id)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    from src.core.database import get_db_context

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Cross-source contact triangulation")
    parser.add_argument("--sweep", action="store_true", help="run the full sweep")
    parser.add_argument("--owner-id", type=int, help="recompute a single owner")
    parser.add_argument("--county-id", help="restrict to one county")
    parser.add_argument("--dry-run", action="store_true", help="compute, write nothing")
    args = parser.parse_args()

    if not args.sweep and not args.owner_id:
        parser.error("one of --sweep / --owner-id is required")

    with get_db_context() as session:
        service = TriangulationService(session)
        stats = service.run_sweep(
            county_id=args.county_id, dry_run=args.dry_run, owner_id=args.owner_id,
        )
    logger.info("[Triangulation] done: %s", {k: v for k, v in stats.items()
                                             if k != "changed_property_ids"})


if __name__ == "__main__":
    main()
