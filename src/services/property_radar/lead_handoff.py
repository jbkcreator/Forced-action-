"""Hand staged PropertyRadar records to FA Max as people and opportunities.

Flow per page of staged records:
  1. Load screening facts for the whole page in a few batch queries
     (already handed off, opt-outs, Backflip campaigns, existing FA Max people).
  2. Decide each record with the pure `decide()` rules — no database access.
  3. With --apply, create the FA Max person + opportunity (+ property link)
     through the state engine's write paths and record every decision.

A record is handed off at most once (unique index on the decisions table).
Suppressed and skipped records are re-evaluated on the next run, so a lead
that was skipped for missing contacts is handed off once contacts arrive.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Iterable, Iterator, Optional, Protocol

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.property_radar_handoff import (
    CAMPAIGN_OPPORTUNITY_TYPES,
    CAMPAIGN_SOURCE_TYPES,
    CONSENT_SOURCE,
    CONSENTED_CHANNELS,
    STAGED_RECORD_PAGE_SIZE,
    THIN_PATH_COUNTY_FIPS,
)
from src.services.property_radar.trace_contacts import LeadContacts

logger = logging.getLogger(__name__)

SOURCE_REFERENCE_PREFIX = "property_radar"
ACTIVE_STATUS = "active"


class HandoffOutcome(str, Enum):
    HANDED_OFF = "handed_off"
    SUPPRESSED = "suppressed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class StagedLead:
    """The fields of a staged PropertyRadar record the handoff needs."""

    radar_id: str
    campaign: str
    status: str
    state: str
    county_fips: str
    county_name: str
    owner_name: Optional[str] = None
    principal_name: Optional[str] = None
    property_address: Optional[str] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    loan_amount: Optional[int] = None
    est_maturity_date: Optional[str] = None
    property_id: Optional[int] = None

    @property
    def display_address(self) -> Optional[str]:
        parts = [p for p in (self.property_address, self.city, self.state, self.zip) if p]
        return ", ".join(parts) or None

    @property
    def contact_name(self) -> Optional[str]:
        """The person behind the entity when known, otherwise the owner of record."""
        return self.principal_name or self.owner_name


@dataclass
class ScreeningFacts:
    """Everything the rules need about one page of records, loaded in batch.

    Mutable on purpose: after a lead is handed off its contacts are added to
    the known sets, so a second record for the same person in the same page
    is treated as an existing person rather than creating a duplicate.
    """

    handed_off_radar_ids: set[str] = field(default_factory=set)
    opted_out_emails: set[str] = field(default_factory=set)
    opted_out_phone_digits: set[str] = field(default_factory=set)
    known_emails: set[str] = field(default_factory=set)
    known_phone_digits: set[str] = field(default_factory=set)
    backflip_feed_fresh: bool = False
    backflip_active_identifiers: set[str] = field(default_factory=set)

    def remember(self, radar_id: str, contacts: LeadContacts) -> None:
        self.handed_off_radar_ids.add(radar_id)
        self.known_emails.update(contacts.emails)
        self.known_phone_digits.update(phone_digits(p) for p in contacts.phones)


@dataclass(frozen=True)
class HandoffDecision:
    radar_id: str
    campaign: str
    county_fips: str
    outcome: HandoffOutcome
    reason: str
    person_id: Optional[str] = None
    opportunity_id: Optional[str] = None


@dataclass
class HandoffReport:
    applied: bool
    decisions: list[HandoffDecision] = field(default_factory=list)

    def counts(self) -> Counter:
        return Counter((d.outcome.value, d.reason) for d in self.decisions)

    def summary(self) -> str:
        outcome_totals = Counter(d.outcome.value for d in self.decisions)
        lines = [
            f"PropertyRadar handoff ({'applied' if self.applied else 'dry run'}): "
            f"{len(self.decisions)} records",
            *(f"  {outcome}: {outcome_totals[outcome]}" for outcome in (o.value for o in HandoffOutcome)),
            "  by reason:",
            *(f"    {outcome} / {reason}: {n}" for (outcome, reason), n in sorted(self.counts().items())),
        ]
        return "\n".join(lines)


def phone_digits(phone: str) -> str:
    """Last ten digits: matches opt-out rows stored in any phone format."""
    return "".join(ch for ch in phone if ch.isdigit())[-10:]


def decide(
    lead: StagedLead,
    contacts: LeadContacts,
    facts: ScreeningFacts,
    *,
    thin_path_only: bool,
) -> tuple[HandoffOutcome, str]:
    """Apply the handoff rules to one record. Pure: no I/O."""
    if lead.campaign not in CAMPAIGN_OPPORTUNITY_TYPES or lead.campaign not in CAMPAIGN_SOURCE_TYPES:
        return HandoffOutcome.SKIPPED, "campaign_not_configured"
    if lead.status != ACTIVE_STATUS:
        return HandoffOutcome.SKIPPED, f"record_{lead.status}"
    if thin_path_only and lead.county_fips not in THIN_PATH_COUNTY_FIPS:
        return HandoffOutcome.SKIPPED, "outside_thin_path"
    if lead.radar_id in facts.handed_off_radar_ids:
        return HandoffOutcome.SKIPPED, "already_handed_off"
    if contacts.is_empty:
        return HandoffOutcome.SKIPPED, "no_contact_data"
    if any(email in facts.opted_out_emails for email in contacts.emails):
        return HandoffOutcome.SUPPRESSED, "email_opt_out"
    if any(phone_digits(p) in facts.opted_out_phone_digits for p in contacts.phones):
        return HandoffOutcome.SUPPRESSED, "sms_opt_out"
    if not facts.backflip_feed_fresh:
        return HandoffOutcome.SKIPPED, "backflip_feed_stale"
    if any(i in facts.backflip_active_identifiers for i in (*contacts.emails, *contacts.phones)):
        return HandoffOutcome.SUPPRESSED, "backflip_active_campaign"
    if any(email in facts.known_emails for email in contacts.emails) or any(
        phone_digits(p) in facts.known_phone_digits for p in contacts.phones
    ):
        return HandoffOutcome.SKIPPED, "existing_fa_max_person"
    return HandoffOutcome.HANDED_OFF, "new_lead"


class HandoffStore(Protocol):
    """Database side of the handoff; the SQL implementation is SqlHandoffStore."""

    def screening_facts(
        self, leads: list[StagedLead], contacts_by_radar: dict[str, LeadContacts],
    ) -> ScreeningFacts: ...

    def create_lead(
        self, lead: StagedLead, contacts: LeadContacts, *, contact_rules_enabled: bool,
    ) -> tuple[str, str]: ...

    def record_decisions(self, decisions: list[HandoffDecision]) -> None: ...


def run_handoff(
    *,
    store: HandoffStore,
    pages: Iterable[list[StagedLead]],
    contacts_by_radar: dict[str, LeadContacts],
    thin_path_only: bool,
    contact_rules_enabled: bool,
    apply: bool,
    commit: Optional[Callable[[], None]] = None,
) -> HandoffReport:
    """Decide every staged record and, when `apply`, write the handoffs.

    A failure creating one lead is isolated by the store (savepoint) and
    recorded as skipped/handoff_error; it never aborts the rest of the page.
    """
    report = HandoffReport(applied=apply)
    empty = LeadContacts()
    for page in pages:
        facts = store.screening_facts(page, contacts_by_radar)
        page_decisions: list[HandoffDecision] = []
        for lead in page:
            contacts = contacts_by_radar.get(lead.radar_id, empty)
            outcome, reason = decide(lead, contacts, facts, thin_path_only=thin_path_only)
            person_id = opportunity_id = None
            if outcome is HandoffOutcome.HANDED_OFF:
                if apply:
                    try:
                        person_id, opportunity_id = store.create_lead(
                            lead, contacts, contact_rules_enabled=contact_rules_enabled,
                        )
                    except Exception:
                        logger.exception("lead_handoff: failed to create lead radar_id=%s", lead.radar_id)
                        outcome, reason = HandoffOutcome.SKIPPED, "handoff_error"
                if outcome is HandoffOutcome.HANDED_OFF:
                    facts.remember(lead.radar_id, contacts)
            page_decisions.append(HandoffDecision(
                radar_id=lead.radar_id, campaign=lead.campaign, county_fips=lead.county_fips,
                outcome=outcome, reason=reason, person_id=person_id, opportunity_id=opportunity_id,
            ))
        if apply:
            store.record_decisions(page_decisions)
            if commit:
                commit()
        report.decisions.extend(page_decisions)
    logger.info("lead_handoff: %s", report.summary().replace("\n", " | "))
    return report


# ---------------------------------------------------------------------------
# Database implementation
# ---------------------------------------------------------------------------

_STAGED_PAGE_SQL = text("""
    SELECT id, radar_id, campaign, status, state, county_fips, county_name,
           owner_name, principal_name, property_address, city, zip,
           loan_amount, est_maturity_date, property_id
    FROM property_radar_records
    WHERE campaign = :campaign AND id > :after_id
    ORDER BY id
    LIMIT :limit
""")


def iter_staged_leads(
    session: Session, *, campaign: str, page_size: int = STAGED_RECORD_PAGE_SIZE,
) -> Iterator[list[StagedLead]]:
    """Yield staged records for a campaign in id order, one page at a time."""
    after_id = 0
    while True:
        rows = session.execute(
            _STAGED_PAGE_SQL, {"campaign": campaign, "after_id": after_id, "limit": page_size},
        ).mappings().all()
        if not rows:
            return
        after_id = rows[-1]["id"]
        yield [
            StagedLead(**{k: (str(v) if k in ("radar_id", "county_fips") else v)
                          for k, v in row.items() if k != "id"})
            for row in rows
        ]


class SqlHandoffStore:
    """HandoffStore backed by the FA database. Writes go through state_engine."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def screening_facts(
        self, leads: list[StagedLead], contacts_by_radar: dict[str, LeadContacts],
    ) -> ScreeningFacts:
        radar_ids = [lead.radar_id for lead in leads]
        emails = sorted({e for lead in leads for e in contacts_by_radar.get(lead.radar_id, LeadContacts()).emails})
        phones = sorted({p for lead in leads for p in contacts_by_radar.get(lead.radar_id, LeadContacts()).phones})
        digits = sorted({phone_digits(p) for p in phones})
        return ScreeningFacts(
            handed_off_radar_ids=self._handed_off(radar_ids),
            opted_out_emails=self._scalar_set(
                "SELECT lower(email) FROM email_opt_outs WHERE lower(email) = ANY(:values)", emails),
            opted_out_phone_digits=self._scalar_set(
                "SELECT right(regexp_replace(phone, '\\D', '', 'g'), 10) FROM sms_opt_outs "
                "WHERE right(regexp_replace(phone, '\\D', '', 'g'), 10) = ANY(:values)", digits),
            known_emails=self._known_emails(emails),
            known_phone_digits=self._known_phone_digits(digits),
            backflip_feed_fresh=self._backflip_feed_fresh(),
            backflip_active_identifiers=self._backflip_active(emails, phones),
        )

    def create_lead(
        self, lead: StagedLead, contacts: LeadContacts, *, contact_rules_enabled: bool,
    ) -> tuple[str, str]:
        from src.services import state_engine
        from src.services.fa_max_send_governance import set_consent

        source_type = CAMPAIGN_SOURCE_TYPES[lead.campaign]
        source_reference = f"{SOURCE_REFERENCE_PREFIX}:{lead.radar_id}"
        savepoint = self._session.begin_nested()
        try:
            person_id = str(self._session.execute(
                text(
                    "INSERT INTO fa_max_persons (source, source_reference, full_name, email, phone) "
                    "VALUES (:source, :reference, :name, :email, :phone) RETURNING person_id"
                ),
                {
                    "source": source_type,
                    "reference": source_reference,
                    "name": lead.contact_name,
                    "email": contacts.emails[0] if contacts.emails else None,
                    "phone": contacts.phones[0] if contacts.phones else None,
                },
            ).scalar_one())
            state_engine.ensure_entity_registry(
                session=self._session, entity_type="person", native_id=person_id,
            )
            opportunity_id = state_engine.create_fa_max_opportunity(
                session=self._session,
                person_id=person_id,
                opportunity_type=CAMPAIGN_OPPORTUNITY_TYPES[lead.campaign],
                source=source_type,
                source_reference=source_reference,
                idempotency_key=f"{SOURCE_REFERENCE_PREFIX}:{lead.radar_id}:{lead.campaign}",
            )
            self._session.execute(
                text(
                    "UPDATE fa_max_opportunities SET property_address = :address, "
                    "loan_amount_cents = :amount_cents, "
                    "expected_need_date = CAST(:maturity AS date), updated_at = now() "
                    "WHERE opportunity_id = CAST(:opportunity_id AS uuid)"
                ),
                {
                    "address": lead.display_address,
                    "amount_cents": lead.loan_amount * 100 if lead.loan_amount else None,
                    "maturity": lead.est_maturity_date or None,
                    "opportunity_id": opportunity_id,
                },
            )
            if lead.property_id is not None:
                state_engine.write_property_association(
                    session=self._session, person_id=person_id, property_id=lead.property_id,
                    role="subject", opportunity_id=opportunity_id, source=SOURCE_REFERENCE_PREFIX,
                )
            if contact_rules_enabled:
                for channel in CONSENTED_CHANNELS:
                    if channel == "email" and not contacts.emails:
                        continue
                    set_consent(
                        self._session, person_id=person_id, channel=channel,
                        consented=True, source=CONSENT_SOURCE,
                    )
            savepoint.commit()
        except Exception:
            savepoint.rollback()
            raise
        return person_id, opportunity_id

    def record_decisions(self, decisions: list[HandoffDecision]) -> None:
        if not decisions:
            return
        self._session.execute(
            text(
                "INSERT INTO property_radar_handoff_decisions "
                "(radar_id, campaign, county_fips, outcome, reason, person_id, opportunity_id) "
                "VALUES (:radar_id, :campaign, :county_fips, :outcome, :reason, "
                "CAST(:person_id AS uuid), CAST(:opportunity_id AS uuid)) "
                "ON CONFLICT (radar_id) WHERE outcome = 'handed_off' DO NOTHING"
            ),
            [
                {
                    "radar_id": d.radar_id, "campaign": d.campaign, "county_fips": d.county_fips,
                    "outcome": d.outcome.value, "reason": d.reason,
                    "person_id": d.person_id, "opportunity_id": d.opportunity_id,
                }
                for d in decisions
            ],
        )

    # -- screening queries -------------------------------------------------

    def _scalar_set(self, sql: str, values: list[str]) -> set[str]:
        if not values:
            return set()
        return {row[0] for row in self._session.execute(text(sql), {"values": values}) if row[0]}

    def _handed_off(self, radar_ids: list[str]) -> set[str]:
        return self._scalar_set(
            "SELECT radar_id FROM property_radar_handoff_decisions "
            "WHERE outcome = 'handed_off' AND radar_id = ANY(:values)", radar_ids,
        )

    def _known_emails(self, emails: list[str]) -> set[str]:
        return self._scalar_set(
            "SELECT lower(email) FROM fa_max_persons "
            "WHERE merged_into_id IS NULL AND lower(email) = ANY(:values) "
            "UNION SELECT identifier_value FROM fa_max_person_contact_identifiers "
            "WHERE identifier_kind = 'email' AND identifier_value = ANY(:values)", emails,
        )

    def _known_phone_digits(self, digits: list[str]) -> set[str]:
        return self._scalar_set(
            "SELECT right(regexp_replace(phone, '\\D', '', 'g'), 10) FROM fa_max_persons "
            "WHERE merged_into_id IS NULL AND phone IS NOT NULL "
            "AND right(regexp_replace(phone, '\\D', '', 'g'), 10) = ANY(:values) "
            "UNION SELECT right(regexp_replace(identifier_value, '\\D', '', 'g'), 10) "
            "FROM fa_max_person_contact_identifiers WHERE identifier_kind = 'phone' "
            "AND right(regexp_replace(identifier_value, '\\D', '', 'g'), 10) = ANY(:values)", digits,
        )

    def _backflip_feed_fresh(self) -> bool:
        from config.settings import get_settings

        fresh = self._session.execute(
            text(
                "SELECT last_success_at >= now() - make_interval(hours => :max_age) "
                "FROM fa_max_backflip_campaign_feed WHERE id = 1"
            ),
            {"max_age": get_settings().fa_max_backflip_feed_max_age_hours},
        ).scalar_one_or_none()
        return bool(fresh)

    def _backflip_active(self, emails: list[str], phones: list[str]) -> set[str]:
        if not emails and not phones:
            return set()
        rows = self._session.execute(
            text(
                "SELECT identifier_value FROM fa_max_backflip_campaign_contacts WHERE active AND ("
                "(identifier_kind = 'email' AND identifier_value = ANY(:emails)) OR "
                "(identifier_kind = 'phone' AND identifier_value = ANY(:phones)))"
            ),
            {"emails": emails, "phones": phones},
        )
        return {row[0] for row in rows}
