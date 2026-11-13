"""P2.4 closing blocker card: deals stuck in an active GHL stage, who holds them up, and a drafted chase.

``upsert_current_stage`` keeps ``lending.ghl_opportunity_stage`` from the GHL stage webhook. Cora's minute sweep
calls ``post_blocker_card``: once a day between 7:00 and noon ET it posts one card to Cora's channel. A borrower
deal with no document chase gets a check-in draft (an approval card in #approvals); a deal with a chase running is
left to the chase; lender and title deals show a ready-to-copy line, never sent by Cora (no contacts held). The card
shows first names and the last 4 phone digits only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_closing import (
    ACTIVE_STAGE_KEYS,
    CARD_LAST_TIME,
    CARD_TIME,
    LENDER_TITLE_DRAFTS,
    OWNER_LABELS,
    STUCK_AFTER,
)
from config.lending_compliance import DEFAULT_TZ
from packages.agent_core.briefing import BriefSection, render_brief_blocks
from packages.agent_core.chatport import ChatPort
from packages.agent_core.send_gate import SendGate
from src.lending.closing.drafts import Recipient, chase_draft, submit_draft
from src.lending.closing.rules import Owner, blame
from src.lending.slack_names import names_by_phone

logger = logging.getLogger(__name__)

_UPSERT_STAGE = text("""
    INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, entered_at, phone, updated_at)
    VALUES (:opp, :stage, :key, :now, :phone, :now)
    ON CONFLICT (ghl_opportunity_id) DO UPDATE SET
        stage_name = EXCLUDED.stage_name,
        stage_key = EXCLUDED.stage_key,
        phone = COALESCE(EXCLUDED.phone, lending.ghl_opportunity_stage.phone),
        updated_at = EXCLUDED.updated_at,
        entered_at = CASE WHEN lending.ghl_opportunity_stage.stage_key = EXCLUDED.stage_key
                          THEN lending.ghl_opportunity_stage.entered_at ELSE EXCLUDED.entered_at END
""")


def upsert_current_stage(db, *, opportunity_id: str, stage_name: str, phone: Optional[str], now: datetime) -> None:
    """Record that the opportunity is in ``stage_name``; a repeat of its current stage keeps the original entry time."""
    db.execute(_UPSERT_STAGE, {"opp": opportunity_id, "stage": stage_name, "key": stage_name.strip().lower(),
                               "now": now, "phone": phone})


# ---- the 7 AM card -------------------------------------------------------------------------------------------------

_STUCK = text("""
    WITH missing AS (
        SELECT chase_id, array_agg(label ORDER BY id) AS labels
          FROM lending.doc_chase_items WHERE received_at IS NULL GROUP BY chase_id
    )
    SELECT s.ghl_opportunity_id, s.stage_name, s.stage_key, s.entered_at, s.phone,
           c.id AS chase_id, c.first_name, c.email, c.ghl_contact_id, m.labels,
           (SELECT max(t.chase_day) FROM lending.doc_chase_touches t WHERE t.chase_id = c.id) AS last_chase_day,
           EXISTS (SELECT 1 FROM lending.loan_files f
                    WHERE f.borrower_phone = s.phone AND f.partner_id IS NOT NULL) AS from_partner
      FROM lending.ghl_opportunity_stage s
      LEFT JOIN LATERAL (
           SELECT * FROM lending.doc_chases d
            WHERE d.status = 'open'
              AND (d.ghl_opportunity_id = s.ghl_opportunity_id OR (s.phone IS NOT NULL AND d.phone = s.phone))
            ORDER BY d.id DESC LIMIT 1) c ON true
      LEFT JOIN missing m ON m.chase_id = c.id
     WHERE s.stage_key = ANY(:stages) AND s.entered_at < :cutoff
     ORDER BY s.entered_at, s.ghl_opportunity_id
""")
_CLAIM = text("INSERT INTO lending.closing_blocker_cards (card_day) VALUES (:day) "
              "ON CONFLICT (card_day) DO NOTHING RETURNING card_day")
_RECORD_POST = text("UPDATE lending.closing_blocker_cards SET slack_channel = :channel, slack_ts = :ts, "
                    "posted_at = CURRENT_TIMESTAMP WHERE card_day = :day")

_EASTERN = ZoneInfo(DEFAULT_TZ)
NUDGE_TTL = timedelta(hours=12)


@dataclass(frozen=True)
class StuckDeal:
    opportunity_id: str
    stage_name: str
    stage_key: str
    entered_at: datetime
    days: int
    first_name: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    ghl_contact_id: Optional[str]
    owner: Owner
    missing: tuple[str, ...]
    chase_id: Optional[int]
    last_chase_day: Optional[int]

    @property
    def phone_last4(self) -> Optional[str]:
        return self.phone[-4:] if self.phone else None


def _first_names(db, phones: list[Optional[str]]) -> dict[str, str]:
    """First names from the lead records; an unreadable lookup leaves names out rather than stopping the card."""
    try:
        with db.begin_nested():
            return {phone: name.split()[0] for phone, name in names_by_phone(db, phones).items() if name.strip()}
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.warning("[closing-blockers] name lookup failed (%s); card shows last 4 digits only", type(exc).__name__)
        return {}


def stuck_deals(db, now: datetime) -> list[StuckDeal]:
    """Deals in an active stage for more than STUCK_AFTER, oldest first. P2.5's daily blockers read this too."""
    rows = db.execute(_STUCK, {"stages": sorted(ACTIVE_STAGE_KEYS), "cutoff": now - STUCK_AFTER}).mappings().all()
    names = _first_names(db, [row["phone"] for row in rows if not row["first_name"]])
    deals = []
    for row in rows:
        missing = tuple(row["labels"] or ())
        deals.append(StuckDeal(
            opportunity_id=row["ghl_opportunity_id"], stage_name=row["stage_name"], stage_key=row["stage_key"],
            entered_at=row["entered_at"], days=(now - row["entered_at"]).days,
            first_name=row["first_name"] or names.get(row["phone"]), phone=row["phone"], email=row["email"],
            ghl_contact_id=row["ghl_contact_id"],
            owner=blame(row["stage_key"], missing_items=len(missing), from_partner=bool(row["from_partner"])),
            missing=missing, chase_id=row["chase_id"], last_chase_day=row["last_chase_day"]))
    return deals


def card_day(now: datetime) -> Optional[date]:
    local = now.astimezone(_EASTERN)
    return local.date() if CARD_TIME <= local.time() < CARD_LAST_TIME else None


def _who(deal: StuckDeal) -> str:
    last4 = f" (…{deal.phone_last4})" if deal.phone_last4 else ""
    return f"{deal.first_name or 'Borrower'}{last4}"


def _nudge_key(deal: StuckDeal) -> str:
    """At most one check-in per deal every STUCK_AFTER days in the same stage, even if Josh rejects the card."""
    period = deal.days // STUCK_AFTER.days
    return f"blocker:{deal.opportunity_id}:{deal.entered_at.date().isoformat()}:{period}"


def _draft_nudge(deal: StuckDeal, gate: SendGate, approvals_channel: str,
                 text_consent: Callable[[str], bool]) -> bool:
    recipient = Recipient(deal.first_name, deal.phone, deal.email, deal.ghl_contact_id)
    draft = chase_draft("blocker_borrower", recipient, link=None, missing=deal.missing,
                        text_ok=bool(deal.phone) and text_consent(deal.phone),
                        contact_ref=f"ghl:{deal.ghl_contact_id}" if deal.ghl_contact_id else f"phone:…{deal.phone_last4}",
                        deal_ref=f"ghl_opportunity:{deal.opportunity_id}")
    if draft is None:
        return False
    submit_draft(gate, draft, key=_nudge_key(deal), channel=approvals_channel, ttl=NUDGE_TTL)
    return True


def _deal_line(deal: StuckDeal, action: str) -> str:
    line = (f"🔴 *{_who(deal)}* · {deal.stage_name} · {deal.days} days · "
            f"waiting on {OWNER_LABELS[deal.owner]}")
    if deal.missing:
        line += f"\n      Missing: {', '.join(deal.missing)}"
    return f"{line}\n      {action}"


def _action(deal: StuckDeal, gate: SendGate, approvals_channel: str, text_consent: Callable[[str], bool]) -> str:
    if deal.owner in ("lender", "title"):
        draft = LENDER_TITLE_DRAFTS[deal.owner].format(name=deal.first_name or "the borrower", stage=deal.stage_name,
                                                       days=deal.days)
        return f"Draft for you to send: _{draft}_"
    if deal.chase_id is not None:
        step = "request sent" if deal.last_chase_day in (None, 0) else f"day {deal.last_chase_day} reminder sent"
        return f"Document chase running ({step}); next reminder goes out on schedule"
    if _draft_nudge(deal, gate, approvals_channel, text_consent):
        return f"Check-in drafted for approval in <#{approvals_channel}>"
    return "No text consent or email on file: call them"


def render_blocker_card(day: date, lines: list[str]) -> tuple[str, list[dict]]:
    header = f"Stuck deals, {day.strftime('%a %b')} {day.day}"
    section = BriefSection(f"Deals stuck more than {STUCK_AFTER.days} days ({len(lines)})", lines,
                           empty_text=f"No deals stuck more than {STUCK_AFTER.days} days.")
    return header, render_brief_blocks(header, [section])


def post_blocker_card(db, chat: ChatPort, gate: SendGate, *, channel: Optional[str], approvals_channel: str,
                      now: Optional[datetime] = None, text_consent: Callable[[str], bool]) -> bool:
    """Post today's card if due and not yet posted; True when posted. Runs in a savepoint on ``db``: a refused post
    raises and rolls the day's claim back, so the next sweep tries again. Drafts are keyed, so a retry never
    queues a second check-in."""
    now = now or datetime.now(timezone.utc)
    day = card_day(now)
    if day is None or not channel:
        return False
    with db.begin_nested():
        if db.execute(_CLAIM, {"day": day}).scalar() is None:
            return False
        deals = stuck_deals(db, now)
        header, blocks = render_blocker_card(
            day, [_deal_line(deal, _action(deal, gate, approvals_channel, text_consent)) for deal in deals])
        result = chat.post(text=header, blocks=blocks, channel=channel)
        if not result.ok:
            raise RuntimeError(f"closing blocker card not posted ({result.error or 'no Slack response'})")
        db.execute(_RECORD_POST, {"channel": result.channel or channel, "ts": result.ts, "day": day})
    logger.info("[closing-blockers] card posted for %s with %s stuck deal(s)", day.isoformat(), len(deals))
    return True
