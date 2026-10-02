# WP-GL-9 Text-Back & Texting Number (GoHighLevel) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Everything in WP-GL-9 that can be built in code before GoHighLevel access exists: a consented, one-per-day, within-60-seconds missed-call text sent through GoHighLevel on a Next Deal Lending number, plus the consent/STOP feeds, flags, tests and the runbook for the GHL-side setup that remains.

**Architecture:** The single BatchDialer poller (`src.lending.cdr_poller`, PR #319) already flags every unanswered outbound lending call into `lending.missed_call_events`. A new `src/lending/text_back.py` consumes `status='pending'` rows each poller cycle: claims them, gates each on lateness (60 s) / consent / quiet hours / feature flag / sender configuration, renders one of the three client-approved templates, sends through a new GHL sender (`src/lending/ghl_sms.py`; interim: the existing Bay Street Capital `GHL_API_KEY`/`GHL_LOCATION_ID` from `.env`, switched to the Next Deal Lending sub-account later by setting two env vars), and records the outcome on the event row. GHL workflows post inbound-text / web-form consent and STOP into FA through a new webhook on the existing lending GHL router. PR #320's second CDR reader (`missed_call_poller.py`, Telnyx path) is retired so there is exactly one poller.

**Tech Stack:** Python 3.11+, SQLAlchemy `text()` (no ORM queries), FastAPI router, pydantic-settings, `requests` via `ghl_webhook._ghl_request`, pytest. No new dependencies.

**Spec:** `tasks/Lending_engine/Forced_Action_Go_Live_Pack_4_Developer_Split_v2.md` (WP-GL-9, sections 6-8), `tasks/Lending_engine/Client_Comments_Clarification_Questions_and_Responses.md` (Q24, Q27, 5.2, 5.5, Part 7 D2-D5), `tasks/Lending_engine/Next_Deal_Lending_Questionnaire_with_Answers.md` (B1-B4, C3, F2, G4, G6, "Additional Notes"), `tasks/Lending_engine/Forced_Action_Go_Live_Brief_Dev.md` (compliance rails).

## Branch and base (decision)

- **Create** `feat/lending-gl9-ghl-text-back` from **`origin/feat/lending-disposition-logging` (PR #319 tip)**, not from `dev` and not from the `#326` branch.
  - `dev` has none of the lending stack (#320 compliance floor, #319 CDR poller/consent/missed-call events are all unmerged).
  - #319 is stacked on #320, so its tip contains both. Its tip also has three commits dated 2026-10-02 that our current checkout lacks (`c0ecd589` "Abandoned never queues a missed-call text", `969cc457` inbound-call consent, `f446303b` review findings). `feat/lending-scoreboard-ghl-showed` (#326) is behind #319, CONFLICTING, and GL-9 does not need it.
- **Open the PR against `feat/lending-disposition-logging`** (stacked). Retarget to `dev` after the merge order lands: #320 -> #319 -> this PR (and #326 independently).
- Use a worktree so the dirty main checkout (`docker-compose.yml`, untracked files) is not dragged along:

```bash
git fetch origin
git worktree add ../Forced-action-gl9 -b feat/lending-gl9-ghl-text-back origin/feat/lending-disposition-logging
cd ../Forced-action-gl9
```

All paths below are relative to that worktree. Copy this plan file into it (`docs/superpowers/plans/`) and commit it with Task 1.

## Open-PR map (what already exists for this task)

| PR | Branch | Relevance to GL-9 | What we do with it |
|---|---|---|---|
| #320 | `feat/lending-w0-dev2-compliance-floor` | Compliance floor, `suppression_list`, `propagate_opt_out`, GHL DND leg (`ghl_dnd.py`), `POST /webhooks/lending/ghl-opt-out`, **an earlier GL-9 pipeline**: `missed_call_text.py`, `missed_call_poller.py`, `lending.missed_call_texts`, `MISSED_CALL_TEXT_ENABLED`, `config/lending_missed_call.py` | Reuse constants and the opt-out webhook; **retire** the second poller and the Telnyx `send_sms` sender (Task 5). Leave `missed_call_text.py` / `lending.missed_call_texts` in place (tests + migration cover them); mark superseded. |
| #319 | `feat/lending-disposition-logging` | **Base.** CDR poller (single instance), `call_dispositions`, `missed_call_events` + `queue_missed_call()` (no consumer), `text_consents` + `has_text_consent()`, on-call consent capture, `deploy.sh` poller install | Consume `missed_call_events`; extend its schema (Task 1). |
| #326 | `feat/lending-scoreboard-ghl-showed` | Scoreboard "showed" from GHL stage webhook; same router file | Not a base. Follow-up: scoreboard counts texts actually sent (Task 9, after #326 merges). |
| #325 | `feat/lending-card-to-dialer` | Card fields in BatchDialer (first name, county, hook), conflicting | Not a dependency. We read first name / county directly (Task 2/4). |
| #324 | `feat/lending-caller-floor` | Scoring, snapshot, extraction (GL-3/GL-4) | No overlap. |
| #323 | `wp-gl-5-booking-gate-nurture` | GHL calendar client; **also reads `GHL_API_KEY`/`GHL_LOCATION_ID`** | No code change here. **Flag to team lead:** it must move to the Next Deal Lending sub-account credentials too (see Cross-PR notes). |
| #322 | `feat/lending-dialer-load` | Dialer load, `dialer_load_records` | Read-only: borrower name / address lookup by phone. |
| #318 | `feat/calling-pool-extraction-intent-filter` | `lending.calling_pool_staging` (`county_name`, `normalized_phone`) | Read-only: county lookup. |
| #317 | `feat/backflip-borrower-conflict-check` | Redundant (close) | None. |

## Global Constraints

- Automated lending-engine texts go **only through GoHighLevel**, never FA's Telnyx path (client Q24: "Send every automated text from GHL on a Next Deal Lending number, not FA's Telnyx path"). The rest of FA keeps Telnyx / `sms_compliance.send_sms`; this is a lending-only exception that Task 7 records in CLAUDE.md.
- **Interim GHL account (team-lead decision 2026-10-02):** until the client provides the Next Deal Lending sub-account, the sender and the opt-out (DND) leg use the existing Bay Street Capital credentials already in `.env` (`GHL_API_KEY`, `GHL_LOCATION_ID`). Setting `LENDING_GHL_API_KEY` + `LENDING_GHL_LOCATION_ID` later switches both to the right sub-account with no code change. The resolver logs one WARNING per process while it is on the fallback.
- **One number:** the calling number and the texting number are the same number (`LENDING_GHL_SMS_FROM_NUMBER`); it is both the GHL `fromNumber` and the number printed in "call or text me back at [number]".
- **Outbound calls only** trigger a text-back at launch. An unanswered inbound callback is handled by the callback queue (client section 3), not by an automated text; inbound speed-to-lead text is week-two scope (brief section 4, month-two list).
- **Consented contacts only** (inbound callers and texters, website form with the box checked, verbal yes on the call, logged). No automated text to a cold number until counsel signs off. Gate = `src.lending.consent.has_text_consent` (it already returns false for suppressed / do-not-contact numbers).
- **Within 60 seconds** of the unanswered call (`MAX_LATE_SECONDS = 60` in `config/lending_missed_call.py`). An older event is logged late, never texted.
- **One text per contact per Eastern calendar day.**
- Every text names the property or reason and carries `Reply STOP to opt out.`
- Brand is **Next Deal Lending** in every text. Wording is the client-approved F2 text (Option 1 Verified maturity, Option 2 Transaction ready and Builders, Option 3 Nurture and anything without an address), verbatim.
- Texting stays **off** (`MISSED_CALL_TEXT_ENABLED=false`) until the GHL number shows **"A2P Verified"**; until then calls-only with live voicemail.
- No AI voice calls to cells, no ringless voicemail (nothing here touches that).
- Phones: every read/write goes through `src.services.phone_utils.normalize`. Logs carry `phone_hash(...)[:12]`, never a phone, name, or message body.
- SQL: `sqlalchemy.text()` with bind parameters only; no ORM queries; `session.execute(...)`; batch rather than per-row where practical.
- Settings only via `config/settings.py` (`get_settings()`); never `os.environ`. Secrets are `SecretStr`, never hard-coded or logged.
- New migration script in `migrations/apply_<name>.py`, idempotent, plus the `src/lending/models.py` update. No Alembic.
- Every external call (GHL) wrapped with try/except and a log line carrying status/class only; no response bodies (they can carry phone numbers).
- Python 3.11+, stdlib `logging`, no `print()` in `src/`.
- Client-facing wording (texts, templates) is a draft for team review before it goes live; legal questions (cold-number consent, multi-line, Florida DNC) belong to the client's counsel: we implement what the client confirmed, we do not advise.

## Review Focus

Inputs/conditions the spec implies but no happy-path test would exercise, most likely first. Each has a pinning test in the task named.

1. **A skipped text must not burn the day's slot.** First missed call at 9 am (no consent) is skipped; consent is captured at 2 pm; the next missed call that day should text. The old `queue_missed_call` marks the second call `duplicate_day` forever. (Task 1 + Task 4)
2. **Borrower name that is not a person** ("ACME HOLDINGS LLC", blank, all caps "SAM", initials) must not produce "Hi ACME," or "Hi ,". Falls back to plain "Hi". Missing caller name -> "our team"; missing address/county -> the general Option 3 text. (Task 2)
3. **Crash or HTTP failure after the claim** must never double-text: a failed send is `failed` (not retried); a row stuck in `sending` for more than 5 minutes becomes `send_unknown` and **keeps** the day slot. (Task 4)
4. **Inbound "STOP" must never register as consent**, an unchecked web-form box must record nothing, and a number with a consent row that is also suppressed/DND must not be texted. (Task 4 + Task 6)
5. **Backlog after a poller outage:** every pending event older than 60 s is `skipped_late`; no burst of stale texts when the poller comes back. Also: evening call near the 7:15 pm stop still texts; an event outside 8 am-8 pm ET (misconfigured hours) is `skipped_quiet_hours`. (Task 4)
6. **Over-long property/county/name** never truncates the STOP language and the body stays <= 320 characters. (Task 2)

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `config/lending_text_back.py` | Create | Approved templates, queue->template map, field caps, quiet-hours window, slot-holding statuses, STOP keywords |
| `migrations/apply_lending_gl9_text_back.py` | Create | Idempotent: `decided_at`, `template_key`, `provider_message_id` on `missed_call_events`; new day-slot unique index |
| `src/lending/models.py` | Modify | `LendingMissedCallEvent` columns + index predicate |
| `src/lending/dispositions.py` | Modify | `queue_missed_call` "taken" predicate uses slot-holding statuses |
| `src/lending/text_back.py` | Create | `PendingText`, name/template/render helpers, claim, decide, `process_pending`, `run_text_back_cycle` |
| `src/lending/ghl_sms.py` | Create | Next Deal Lending GHL account resolution, `GhlSmsSender`, `get_sender()`, `--send-test` smoke CLI |
| `src/lending/ghl_dnd.py` | Modify | Use the same account resolver as the sender (so DND lands where texts are sent) |
| `config/settings.py` | Modify | `LENDING_GHL_API_KEY`, `LENDING_GHL_LOCATION_ID` (optional overrides of `GHL_*`), `LENDING_GHL_SMS_FROM_NUMBER` |
| `src/lending/cdr_poller.py` | Modify | Run the text-back cycle after each ingest cycle |
| `src/lending/missed_call_poller.py` | Modify | Refuse to start (superseded) |
| `src/api/lending_ghl_router.py` | Modify | `POST /webhooks/lending/ghl-text-consent` |
| `deploy/systemd/README.md` | Modify | Remove missed-call-poller from the enable list; note supersession |
| `docs/lending/text-back-runbook.md` | Create | GHL-side setup checklist, workflows, env vars, A2P gate, live test script, booking-close line |
| `docs/lending/dialer-disposition-runbook.md` | Modify | Replace the "Handoff to the text-back task" section |
| `CLAUDE.md` | Modify | Lending section: GHL text-back, env vars, SMS rule exception |
| `tests/lending/test_apply_lending_gl9_text_back.py` | Create | Migration + queue-slot semantics |
| `tests/lending/test_text_back_render.py` | Create | Templates, names, selection, rendering (no DB) |
| `tests/lending/test_ghl_sms.py` | Create | Sender request shapes, errors, account resolution (no network) |
| `tests/lending/test_text_back.py` | Create | Processor outcomes and end-to-end "done when" scenarios (DB) |
| `tests/lending/test_ghl_text_consent_webhook.py` | Create | Consent/STOP webhook |

Test commands need `DATABASE_URL` for the DB tests (they skip without it, same as the rest of `tests/lending`). Run the pure tests anywhere.

---

### Task 1: Schema - decision columns and a day slot that skipped texts do not hold

**Files:**
- Create: `config/lending_text_back.py` (constants only for now)
- Create: `migrations/apply_lending_gl9_text_back.py`
- Modify: `src/lending/models.py` (class `LendingMissedCallEvent`)
- Modify: `src/lending/dispositions.py:368-395` (`queue_missed_call`)
- Test: `tests/lending/test_apply_lending_gl9_text_back.py`

**Interfaces:**
- Produces: `config.lending_text_back.SLOT_HOLDING_STATUSES: tuple[str, ...]` = `("pending", "sending", "sent", "send_unknown")`; `migrations.apply_lending_gl9_text_back.apply_to(conn, schema="lending")`; new columns `decided_at timestamptz`, `template_key varchar(20)`, `provider_message_id varchar(100)` on `lending.missed_call_events`. Event `status` vocabulary after this task: `pending, sending, sent, dry_run, failed, send_unknown, blocked, duplicate_day, skipped_late, skipped_no_consent, skipped_quiet_hours, skipped_not_configured`.

- [ ] **Step 1: Create the constants module**

```python
"""WP-GL-9 text-back: client-approved wording and the rules around it (values only, no logic)."""
from __future__ import annotations

# Statuses that occupy a contact's one text slot for the Eastern day. A skipped, dry-run or
# failed event does NOT hold it, so a later call the same day can still be texted once consent
# exists. send_unknown (crash after the claim) holds it: we cannot prove nothing went out.
SLOT_HOLDING_STATUSES = ("pending", "sending", "sent", "send_unknown")
SLOT_HOLDING_SQL = "status IN (" + ", ".join(f"'{s}'" for s in SLOT_HOLDING_STATUSES) + ")"
```

- [ ] **Step 2: Write the failing tests**

```python
"""WP-GL-9 schema: decision columns and the day slot (skipped texts free it)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_gl9_text_back import apply_to
from src.lending.dispositions import queue_missed_call

PHONE = "+18135558601"
ENDED = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)  # 11:00 ET


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def _status(db, call_id):
    return db.execute(text("SELECT status FROM lending.missed_call_events WHERE dialer_call_id = :c"), {"c": call_id}).scalar()


def test_migration_is_idempotent_and_adds_the_decision_columns(db):
    apply_to(db.connection())
    cols = {r[0] for r in db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'lending' AND table_name = 'missed_call_events'"))}
    assert {"decided_at", "template_key", "provider_message_id"} <= cols


def test_a_pending_event_holds_the_days_slot(db):
    assert queue_missed_call(db, "c1", PHONE, None, None, ENDED) == "pending"
    assert queue_missed_call(db, "c2", PHONE, None, None, ENDED) == "duplicate_day"


@pytest.mark.parametrize("terminal", ["skipped_no_consent", "skipped_late", "dry_run", "failed", "skipped_quiet_hours"])
def test_a_skipped_failed_or_dry_run_event_frees_the_slot(db, terminal):
    queue_missed_call(db, "c1", PHONE, None, None, ENDED)
    db.execute(text("UPDATE lending.missed_call_events SET status = :s WHERE dialer_call_id = 'c1'"), {"s": terminal})
    assert queue_missed_call(db, "c2", PHONE, None, None, ENDED) == "pending"


@pytest.mark.parametrize("holding", ["sent", "sending", "send_unknown"])
def test_sent_sending_and_unknown_events_keep_the_slot(db, holding):
    queue_missed_call(db, "c1", PHONE, None, None, ENDED)
    db.execute(text("UPDATE lending.missed_call_events SET status = :s WHERE dialer_call_id = 'c1'"), {"s": holding})
    assert queue_missed_call(db, "c2", PHONE, None, None, ENDED) == "duplicate_day"
```

- [ ] **Step 3: Run to verify they fail**

Run: `pytest tests/lending/test_apply_lending_gl9_text_back.py -v`
Expected: FAIL (`ModuleNotFoundError: migrations.apply_lending_gl9_text_back`).

- [ ] **Step 4: Write the migration**

```python
"""WP-GL-9: text-back decision columns and the day-slot index on ``lending.missed_call_events``.

Adds decided_at / template_key / provider_message_id, and replaces the one-sendable-event-per-
phone-per-day unique index so only pending / sending / sent / send_unknown events hold the slot
(a skipped, dry-run or failed text frees it). Idempotent; run after
apply_lending_call_dispositions_dialer.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_gl9_text_back.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_text_back import SLOT_HOLDING_SQL
from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    table = f'"{schema}".missed_call_events'
    for ddl in (
        f'DROP INDEX IF EXISTS "{schema}".uq_lending_missed_call_phone_day',
        f"ALTER TABLE {table} ALTER COLUMN status TYPE varchar(24)",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS decided_at timestamptz",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS template_key varchar(20)",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS provider_message_id varchar(100)",
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_missed_call_phone_day ON {table} (phone, event_date_et) "
        f"WHERE {SLOT_HOLDING_SQL}",
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_gl9_text_back complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
```

- [ ] **Step 5: Update the model and `queue_missed_call`**

In `src/lending/models.py`, add the import `from config.lending_text_back import SLOT_HOLDING_SQL` and change `LendingMissedCallEvent`:

```python
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending", server_default="pending")
    # pending / sending / sent / send_unknown / dry_run / failed / blocked / duplicate_day / skipped_*
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    template_key: Mapped[Optional[str]] = mapped_column(String(20))
    provider_message_id: Mapped[Optional[str]] = mapped_column(String(100))

    __table_args__ = (
        Index("uq_lending_missed_call_phone_day", "phone", "event_date_et", unique=True, postgresql_where=text(SLOT_HOLDING_SQL)),
        Index("idx_lending_missed_call_events_pending", "created_at", postgresql_where=text("status = 'pending'")),
    )
```

(keep any other existing columns/indexes on the class as they are; only the `status` length, the three new columns, and the first index predicate change.) The existing `status` column was `varchar(20)` but `skipped_not_configured` is 22 characters, which is why the migration in Step 4 widens it to `varchar(24)` and the model uses `String(24)`.

In `src/lending/dispositions.py` `queue_missed_call`, add `from config.lending_text_back import SLOT_HOLDING_SQL` and change the "taken" query:

```python
        taken = db.execute(
            text("SELECT 1 FROM lending.missed_call_events WHERE phone = :p AND event_date_et = :d "
                 f"AND {SLOT_HOLDING_SQL} AND dialer_call_id <> :c"),
            {"p": phone, "d": day, "c": call_id},
        ).first()
```

- [ ] **Step 6: Run the tests (new and existing missed-call event tests)**

Run: `pytest tests/lending/test_apply_lending_gl9_text_back.py tests/lending/test_dialer_events.py tests/lending/test_apply_lending_call_dispositions_dialer.py -v`
Expected: PASS (the existing `c1 pending / c2 duplicate_day` test still passes).

- [ ] **Step 7: Commit**

```bash
git add config/lending_text_back.py migrations/apply_lending_gl9_text_back.py src/lending/models.py src/lending/dispositions.py tests/lending/test_apply_lending_gl9_text_back.py docs/superpowers/plans/2026-10-02-wp-gl-9-ghl-text-back.md
git commit -m "feat(lending): WP-GL-9 text-back decision columns; skipped texts no longer hold the day's slot"
```

---

### Task 2: Approved wording, name rules and rendering

**Files:**
- Modify: `config/lending_text_back.py`
- Create: `src/lending/text_back.py` (render half)
- Test: `tests/lending/test_text_back_render.py`

**Interfaces:**
- Consumes: `config.lending_queues` constants (`VERIFIED_MATURITY`, `TRANSACTION_READY`, `BUILDERS`, `NURTURE`).
- Produces (in `config/lending_text_back.py`): `TEMPLATES: dict[str, str]` (keys `maturity`, `deal_drop`, `general`), `QUEUE_TEMPLATES: dict[str, str]`, `MAX_TEXT_CHARS = 320`, `CAPS: dict[str, int]`, `ENTITY_TOKENS: frozenset[str]`, `FALLBACK_CALLER = "our team"`.
- Produces (in `src/lending/text_back.py`): `PendingText` dataclass, `first_name_of(borrower_name) -> Optional[str]`, `choose_template(item) -> str`, `render_body(item, template_key, *, number) -> str`, `format_number(e164) -> str`.

- [ ] **Step 1: Add the wording and rules to `config/lending_text_back.py`**

Append:

```python
from config.lending_queues import BUILDERS, NURTURE, TRANSACTION_READY, VERIFIED_MATURITY

MAX_TEXT_CHARS = 320

# Client-approved wording (questionnaire F2: "Option 1 for Verified maturity, Option 2 for Transaction
# ready and Builders, Option 3 for Nurture and anything without an address"). Verbatim except the
# greeting, which is "Hi <first name>" or just "Hi" when the borrower has no usable first name.
TEMPLATES: dict[str, str] = {
    "maturity": (
        "{greeting}, it's {caller} with Next Deal Lending. Sorry I missed you. "
        "I was calling about the loan on {property}. "
        "Call or text me back at {number} when it suits you. Reply STOP to opt out."
    ),
    "deal_drop": (
        "{greeting}, {caller} from Next Deal Lending here. Just tried you about {property}. "
        "We help investors in {county} fund their next deal. Text back if you'd like to chat. "
        "Reply STOP to opt out."
    ),
    "general": (
        "{greeting}, this is {caller} with Next Deal Lending. Sorry we missed each other. "
        "Reply here or call {number} whenever works. Reply STOP to opt out."
    ),
}
GENERAL = "general"
QUEUE_TEMPLATES: dict[str, str] = {
    VERIFIED_MATURITY: "maturity",
    TRANSACTION_READY: "deal_drop",
    BUILDERS: "deal_drop",
    NURTURE: "general",
}
TEMPLATE_NEEDS: dict[str, tuple[str, ...]] = {  # fields a template cannot be sent without
    "maturity": ("property_address",),
    "deal_drop": ("property_address", "county"),
    "general": (),
}

# Per-field caps keep the worst case under MAX_TEXT_CHARS so the STOP language is never cut.
CAPS = {"first_name": 20, "caller": 30, "property": 50, "county": 30}
FALLBACK_CALLER = "our team"
ENTITY_TOKENS = frozenset({
    "llc", "inc", "corp", "corporation", "co", "company", "ltd", "lp", "llp", "pllc", "trust",
    "holdings", "properties", "investments", "capital", "group", "partners", "enterprises", "realty",
})
```

- [ ] **Step 2: Write the failing tests**

```python
"""WP-GL-9 wording, borrower-name rules and template choice (no DB)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.lending_text_back import MAX_TEXT_CHARS, TEMPLATES
from src.lending.text_back import PendingText, choose_template, first_name_of, format_number, render_body

NUMBER = "+18135550100"


def _item(**over) -> PendingText:
    base = dict(event_id=1, call_id="c1", phone="+18135558601", ended_at=datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc),
                property_address="123 Main St, Tampa FL 33602", queue="verified_maturity", caller_name="Alex",
                borrower_name="Sam Jones", county="Hillsborough")
    base.update(over)
    return PendingText(**base)


def test_the_three_client_approved_texts_are_unchanged():
    assert TEMPLATES["maturity"] == ("{greeting}, it's {caller} with Next Deal Lending. Sorry I missed you. "
                                     "I was calling about the loan on {property}. "
                                     "Call or text me back at {number} when it suits you. Reply STOP to opt out.")
    assert TEMPLATES["deal_drop"] == ("{greeting}, {caller} from Next Deal Lending here. Just tried you about {property}. "
                                      "We help investors in {county} fund their next deal. Text back if you'd like to chat. "
                                      "Reply STOP to opt out.")
    assert TEMPLATES["general"] == ("{greeting}, this is {caller} with Next Deal Lending. Sorry we missed each other. "
                                    "Reply here or call {number} whenever works. Reply STOP to opt out.")


@pytest.mark.parametrize("over,expected", [
    ({}, "maturity"),
    ({"queue": "transaction_ready"}, "deal_drop"),
    ({"queue": "builders"}, "deal_drop"),
    ({"queue": "nurture"}, "general"),
    ({"queue": None}, "general"),
    ({"queue": "verified_maturity", "property_address": None}, "general"),
    ({"queue": "builders", "county": None}, "general"),
    ({"queue": "builders", "property_address": "  "}, "general"),
])
def test_template_follows_the_queue_and_falls_back_to_general_without_an_address_or_county(over, expected):
    assert choose_template(_item(**over)) == expected


@pytest.mark.parametrize("name,expected", [
    ("Samuel Jones", "Samuel"), ("SAM JONES", "Sam"), ("mary-ann lee", "Mary-Ann"), ("McDonald Lee", "McDonald"),
    ("Acme Holdings LLC", None), ("Smith Family Trust", None), ("J. Smith", None), ("  ", None), (None, None), ("X", None),
])
def test_first_name_is_a_real_first_name_or_nothing(name, expected):
    assert first_name_of(name) == expected


def test_maturity_text_renders_exactly():
    assert render_body(_item(), "maturity", number=NUMBER) == (
        "Hi Sam, it's Alex with Next Deal Lending. Sorry I missed you. I was calling about the loan on 123 Main St. "
        "Call or text me back at (813) 555-0100 when it suits you. Reply STOP to opt out.")


def test_no_first_name_or_caller_still_reads_naturally():
    body = render_body(_item(borrower_name="Acme Holdings LLC", caller_name=None), "general", number=NUMBER)
    assert body.startswith("Hi, this is our team with Next Deal Lending.")


def test_format_number_shows_a_us_number_and_passes_anything_else_through():
    assert format_number("+18135550100") == "(813) 555-0100"
    assert format_number("+442071838750") == "+442071838750"


@pytest.mark.parametrize("key", ["maturity", "deal_drop", "general"])
def test_worst_case_fields_keep_the_text_short_and_the_stop_language_whole(key):
    long = "x" * 300
    body = render_body(_item(property_address=long, county=long, caller_name=long, borrower_name=long + " Jones"), key, number=NUMBER)
    assert len(body) <= MAX_TEXT_CHARS and body.endswith("Reply STOP to opt out.")
```

- [ ] **Step 3: Run to verify they fail**

Run: `pytest tests/lending/test_text_back_render.py -v`
Expected: FAIL (`ImportError: cannot import name 'PendingText'`).

- [ ] **Step 4: Implement the render half of `src/lending/text_back.py`**

```python
"""WP-GL-9 missed-call text-back (GoHighLevel).

Consumes ``lending.missed_call_events`` rows that the single BatchDialer CDR poller queues for an
unanswered outbound lending call. Each event gets exactly one decision: sent, or why not. Texts
go out only through GHL, only to consented numbers, once per Eastern day, within 60 seconds of
the call. Logs carry phone hashes, never phones, names or message bodies.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from config.lending_text_back import (
    CAPS, ENTITY_TOKENS, FALLBACK_CALLER, GENERAL, QUEUE_TEMPLATES, TEMPLATES, TEMPLATE_NEEDS,
)

_NAME = re.compile(r"^[A-Za-z][A-Za-z'\-]+$")


@dataclass(frozen=True)
class PendingText:
    event_id: int
    call_id: str
    phone: str
    ended_at: datetime
    property_address: Optional[str]
    queue: Optional[str]
    caller_name: Optional[str]
    borrower_name: Optional[str]
    county: Optional[str]


def first_name_of(borrower_name: Optional[str]) -> Optional[str]:
    """A person's first name, or None for blanks, initials and entity-looking names."""
    tokens = (borrower_name or "").split()
    if not tokens or any(t.strip(".,").lower() in ENTITY_TOKENS for t in tokens):
        return None
    first = tokens[0]
    if not _NAME.match(first):
        return None
    return first.title() if first.isupper() or first.islower() else first


def choose_template(item: PendingText) -> str:
    key = QUEUE_TEMPLATES.get(item.queue or "", GENERAL)
    for field in TEMPLATE_NEEDS[key]:
        if not (getattr(item, field) or "").strip():
            return GENERAL
    return key


def format_number(e164: str) -> str:
    digits = re.sub(r"\D", "", e164)
    if len(digits) == 11 and digits.startswith("1"):
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return e164


def render_body(item: PendingText, template_key: str, *, number: str) -> str:
    first = first_name_of(item.borrower_name)
    street = (item.property_address or "").split(",")[0].strip()
    return TEMPLATES[template_key].format(
        greeting=f"Hi {first[:CAPS['first_name']]}" if first else "Hi",
        caller=(item.caller_name or "").strip()[:CAPS["caller"]] or FALLBACK_CALLER,
        property=street[:CAPS["property"]],
        county=(item.county or "").strip()[:CAPS["county"]],
        number=format_number(number),
    )
```

- [ ] **Step 5: Run to verify they pass**

Run: `pytest tests/lending/test_text_back_render.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add config/lending_text_back.py src/lending/text_back.py tests/lending/test_text_back_render.py
git commit -m "feat(lending): WP-GL-9 approved text-back wording, name rules and rendering"
```

---

### Task 3: Settings and the GoHighLevel sender (Bay Street interim, Next Deal Lending later)

**Files:**
- Modify: `config/settings.py` (next to `lending_ghl_webhook_secret`, ~line 876)
- Create: `src/lending/ghl_sms.py`
- Modify: `src/lending/ghl_dnd.py`
- Test: `tests/lending/test_ghl_sms.py`

**Interfaces:**
- Consumes: `src.services.ghl_webhook._ghl_request(method, url, **kwargs) -> requests.Response` (retries 429/network, default 15 s timeout) and `_GHL_BASE`.
- Produces: `GhlAccount(api_key: str, location_id: str)`; `lending_ghl_account() -> Optional[GhlAccount]` (`LENDING_GHL_*` when both set, else `GHL_API_KEY`/`GHL_LOCATION_ID`, else None; WARNING once on the fallback); `ghl_headers(api_key, version="2021-07-28") -> dict`; `GhlSmsError(RuntimeError)`; `GhlSmsSender(account, from_number, request=_ghl_request)` callable as `sender(phone, body, first_name=None) -> str` (provider message id); `get_sender() -> Optional[GhlSmsSender]`; `texting_number() -> Optional[str]` (normalized `LENDING_GHL_SMS_FROM_NUMBER`).

New settings (all optional, default `None`): `lending_ghl_api_key: SecretStr` (env `LENDING_GHL_API_KEY`) and `lending_ghl_location_id: str` (`LENDING_GHL_LOCATION_ID`) are the Next Deal Lending overrides; `lending_ghl_sms_from_number: str` (`LENDING_GHL_SMS_FROM_NUMBER`) is the single calling+texting number.

- [ ] **Step 1: Add the settings**

```python
	lending_ghl_api_key: Optional[SecretStr] = Field(default=None, env="LENDING_GHL_API_KEY")  # Next Deal Lending sub-account; unset = fall back to GHL_API_KEY (Bay Street, interim)
	lending_ghl_location_id: Optional[str] = Field(default=None, env="LENDING_GHL_LOCATION_ID")
	lending_ghl_sms_from_number: Optional[str] = Field(default=None, env="LENDING_GHL_SMS_FROM_NUMBER")  # E.164; the one number used for calling and texting
```

- [ ] **Step 2: Write the failing tests**

```python
"""WP-GL-9 GHL sender: request shapes and error handling, with no network."""
from __future__ import annotations

import pytest
from pydantic import SecretStr

from config.settings import get_settings
from src.lending.ghl_sms import GhlAccount, GhlSmsError, GhlSmsSender, get_sender, lending_ghl_account, texting_number

PHONE = "+18135558601"
ACCOUNT = GhlAccount(api_key="k-secret", location_id="loc-ndl")


class Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class Recorder:
    def __init__(self, *responses):
        self.calls, self._responses = [], list(responses)

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self._responses.pop(0)


def test_sender_upserts_the_contact_then_sends_an_sms_from_the_texting_number():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1", "conversationId": "cv1"}))
    message_id = GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam")
    assert message_id == "m1"
    (m1, u1, k1), (m2, u2, k2) = request.calls
    assert (m1, u1.endswith("/contacts/upsert")) == ("POST", True)
    assert k1["json"] == {"locationId": "loc-ndl", "phone": PHONE, "firstName": "Sam"}
    assert (m2, u2.endswith("/conversations/messages")) == ("POST", True)
    assert k2["json"] == {"type": "SMS", "contactId": "ct1", "message": "hello", "fromNumber": "+18135550100"}
    assert k2["headers"]["Authorization"] == "Bearer k-secret"


def test_upsert_omits_first_name_when_unknown():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None)
    assert "firstName" not in request.calls[0][2]["json"]


@pytest.mark.parametrize("responses,fragment", [
    ([Resp(422, {"message": f"bad {PHONE}"})], "contact upsert failed: HTTP 422"),
    ([Resp(200, {"contact": {}})], "contact upsert returned no contact id"),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(400, {"message": f"DND {PHONE}"})], "message send failed: HTTP 400"),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {})], "message send returned no message id"),
])
def test_failures_raise_without_leaking_the_response_body(responses, fragment):
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=Recorder(*responses))(PHONE, "hello", None)
    assert fragment in str(err.value) and PHONE not in str(err.value)


def test_a_network_error_becomes_a_ghl_sms_error():
    def boom(*a, **k):
        raise ConnectionError(f"reset {PHONE}")
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=boom)(PHONE, "hello", None)
    assert PHONE not in str(err.value)


def _clear(monkeypatch, *names):
    s = get_settings()
    for name in names:
        monkeypatch.setattr(s, name, None, raising=False)
    return s


ALL = ("lending_ghl_api_key", "lending_ghl_location_id", "lending_ghl_sms_from_number", "ghl_api_key", "ghl_location_id")


def test_no_account_when_nothing_is_configured(monkeypatch):
    _clear(monkeypatch, *ALL)
    assert lending_ghl_account() is None and get_sender() is None


def test_it_falls_back_to_the_bay_street_account_until_next_deal_lending_exists(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    assert lending_ghl_account() == GhlAccount("bay-key", "loc-bay")


def test_the_next_deal_lending_settings_win_once_both_are_set(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    monkeypatch.setattr(s, "lending_ghl_api_key", SecretStr("ndl-key"), raising=False)
    assert lending_ghl_account() == GhlAccount("bay-key", "loc-bay")  # key alone is not enough: both or neither
    monkeypatch.setattr(s, "lending_ghl_location_id", "loc-ndl", raising=False)
    assert lending_ghl_account() == GhlAccount("ndl-key", "loc-ndl")


def test_a_sender_needs_an_account_and_the_texting_number(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    assert get_sender() is None and texting_number() is None
    monkeypatch.setattr(s, "lending_ghl_sms_from_number", "(813) 555-0100", raising=False)
    assert isinstance(get_sender(), GhlSmsSender) and texting_number() == "+18135550100"
```

- [ ] **Step 3: Run to verify they fail**

Run: `pytest tests/lending/test_ghl_sms.py -v`
Expected: FAIL (`ModuleNotFoundError: src.lending.ghl_sms`).

- [ ] **Step 4: Implement `src/lending/ghl_sms.py`**

```python
"""GoHighLevel SMS sender for the Next Deal Lending sub-account (WP-GL-9).

Credentials: LENDING_GHL_API_KEY + LENDING_GHL_LOCATION_ID (the Next Deal Lending sub-account,
client answer B1) when both are set, else the shared GHL_API_KEY / GHL_LOCATION_ID, which today
point at the Bay Street Capital sub-account. The fallback is an interim, team-lead-approved
decision until the client provides the Next Deal Lending sub-account; switching is two env vars.

Request shapes follow the public GHL v2 API reference (contacts/upsert, conversations/messages)
and are NOT yet confirmed against a live round trip. Run ``--send-test`` once with real
credentials before enabling texting, and correct field names here if the live call disagrees.
Error text carries the HTTP status only: GHL response bodies can echo phone numbers.

Usage:
    python -m src.lending.ghl_sms --send-test +1813XXXXXXX   # sends one real text to YOUR phone
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from config.settings import get_settings
from src.services import ghl_webhook
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

Request = Callable[..., Any]


class GhlSmsError(RuntimeError):
    """A text could not be handed to GHL. The message never contains a phone or response body."""


@dataclass(frozen=True)
class GhlAccount:
    api_key: str
    location_id: str


_warned_fallback = False


def lending_ghl_account() -> Optional[GhlAccount]:
    """Next Deal Lending credentials when both are set, else the shared (Bay Street) ones, else None."""
    global _warned_fallback
    s = get_settings()
    if s.lending_ghl_api_key is not None and s.lending_ghl_location_id:
        return GhlAccount(s.lending_ghl_api_key.get_secret_value(), s.lending_ghl_location_id)
    if s.ghl_api_key is None or not s.ghl_location_id:
        return None
    if not _warned_fallback:
        logger.warning("[lending-ghl] LENDING_GHL_* not set: using the shared GHL_* account (interim, Bay Street Capital)")
        _warned_fallback = True
    return GhlAccount(s.ghl_api_key.get_secret_value(), s.ghl_location_id)


def ghl_headers(api_key: str, version: str = "2021-07-28") -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Version": version,
            "Content-Type": "application/json", "Accept": "application/json"}


def texting_number() -> Optional[str]:
    """The one number used for calling and texting (E.164), or None when not configured."""
    raw = get_settings().lending_ghl_sms_from_number
    return (normalize(raw) or raw) if raw else None


class GhlSmsSender:
    def __init__(self, account: GhlAccount, from_number: str, request: Request = ghl_webhook._ghl_request) -> None:
        self._account, self._from, self._request = account, from_number, request

    def _post(self, path: str, body: dict, what: str, version: str = "2021-07-28") -> dict:
        try:
            response = self._request("POST", f"{ghl_webhook._GHL_BASE}{path}",
                                     headers=ghl_headers(self._account.api_key, version), json=body)
        except Exception as exc:  # class only: the message can carry request detail
            logger.warning("[lending-ghl-sms] %s request error: %s", what, type(exc).__name__)
            raise GhlSmsError(f"GHL {what} request error ({type(exc).__name__})") from None
        if response.status_code >= 400:
            logger.warning("[lending-ghl-sms] %s failed: HTTP %s", what, response.status_code)
            raise GhlSmsError(f"GHL {what} failed: HTTP {response.status_code}")
        try:
            return response.json() or {}
        except ValueError:
            return {}

    def __call__(self, phone: str, body: str, first_name: Optional[str] = None) -> str:
        contact = {"locationId": self._account.location_id, "phone": phone}
        if first_name:
            contact["firstName"] = first_name
        contact_id = (self._post("/contacts/upsert", contact, "contact upsert").get("contact") or {}).get("id")
        if not contact_id:
            raise GhlSmsError("GHL contact upsert returned no contact id")
        sent = self._post("/conversations/messages",
                          {"type": "SMS", "contactId": contact_id, "message": body, "fromNumber": self._from},
                          "message send", version="2021-04-15")  # conversations endpoints use this version
        message_id = sent.get("messageId") or sent.get("id")
        if not message_id:
            raise GhlSmsError("GHL message send returned no message id")
        return str(message_id)


def get_sender() -> Optional[GhlSmsSender]:
    """None until a GHL account and the texting number are both configured."""
    account, number = lending_ghl_account(), texting_number()
    if account is None or not number:
        return None
    return GhlSmsSender(account, number)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--send-test", metavar="PHONE", required=True, help="send ONE real text to this (your own) phone")
    phone = normalize(parser.parse_args(argv).send_test)
    sender = get_sender()
    if sender is None or not phone:
        logger.error("[lending-ghl-sms] GHL account / LENDING_GHL_SMS_FROM_NUMBER not configured or phone invalid; nothing sent")
        return 2
    message_id = sender(phone, "Next Deal Lending test message. Reply STOP to opt out.", None)
    logger.info("[lending-ghl-sms] test text accepted by GHL (message id %s)", message_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Make the opt-out (DND) leg use the same account resolver**

In `src/lending/ghl_dnd.py` import `from src.lending.ghl_sms import ghl_headers, lending_ghl_account` and replace `set_ghl_dnd` / `get_ghl_dnd`, so opt-outs land in the same GHL account the texts are sent from (Bay Street today, Next Deal Lending after the env switch):

```python
def set_ghl_dnd(phone: str) -> bool:
    """True when GHL accepted the DND update. Never raises; logs the status only."""
    from src.services import ghl_webhook

    account = lending_ghl_account()
    if account is None:
        return False
    body = {
        "locationId": account.location_id,
        "phone": phone,
        "dnd": True,
        "dndSettings": {channel: {"status": "active", "message": "Lending opt-out"} for channel in GHL_DND_CHANNELS},
        "tags": [GHL_OPT_OUT_TAG],
    }
    try:
        response = ghl_webhook._ghl_request(
            "POST", f"{ghl_webhook._GHL_BASE}/contacts/upsert", headers=ghl_headers(account.api_key), json=body,
        )
    except Exception as exc:
        logger.warning("[lending-ghl] DND upsert failed: %s", type(exc).__name__)
        return False
    if response.status_code >= 400:
        logger.warning("[lending-ghl] DND upsert failed: HTTP %s", response.status_code)
        return False
    return True


def get_ghl_dnd() -> Optional[GhlDnd]:
    """The GHL DND writer, or None when GHL is not configured (the sync then waits)."""
    return set_ghl_dnd if lending_ghl_account() is not None else None
```

(Remove the `get_settings` import from `ghl_dnd.py` if it becomes unused.) Add this test to `tests/lending/test_ghl_sms.py`:

```python
def test_dnd_uses_the_same_account_as_the_sender(monkeypatch):
    from src.lending import ghl_dnd
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    seen = {}

    def fake(method, url, **kw):
        seen.update(kw)
        return Resp(200, {})
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    assert seen["json"]["locationId"] == "loc-bay" and seen["headers"]["Authorization"] == "Bearer bay-key"
    assert seen["headers"]["Version"] == "2021-07-28"
```

(`tests/lending/conftest.py` autouse patches `get_ghl_dnd` only; `set_ghl_dnd` is called directly here with the request stubbed, so no real GHL call is made.)

- [ ] **Step 6: Run the tests, including the existing opt-out ones**

Run: `pytest tests/lending/test_ghl_sms.py tests/lending/test_ghl_opt_out_webhook.py tests/lending/test_stop_propagation.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add config/settings.py src/lending/ghl_sms.py src/lending/ghl_dnd.py tests/lending/test_ghl_sms.py
git commit -m "feat(lending): WP-GL-9 GHL SMS sender (Bay Street interim, env switch to Next Deal Lending); opt-outs follow it"
```

---

### Task 4: The text-back processor (claim, gates, send, record)

**Files:**
- Modify: `src/lending/text_back.py` (add the processing half)
- Test: `tests/lending/test_text_back.py`

**Interfaces:**
- Consumes: `has_text_consent(db, phone) -> bool` (`src.lending.consent`); `phone_hash(phone)` (`src.lending.compliance`); `MAX_LATE_SECONDS`, `TIMEZONE` (`config.lending_missed_call`); `get_sender`, `texting_number`, `GhlSmsError` (Task 3); `PendingText`, `choose_template`, `render_body`, `first_name_of` (Task 2).
- Produces:
  - `TextSender = Callable[[str, str, Optional[str]], str]`
  - `process_pending(db, *, sender: Optional[TextSender], enabled: bool, number: Optional[str], now: Optional[datetime] = None, limit: int = 50) -> dict[str, int]` (counts by outcome; commits itself: claim, then one commit per event)
  - `run_text_back_cycle(*, now=None) -> dict[str, int]` (opens `lending_session()`, reads settings, calls `process_pending`)
  - Outcomes written to `missed_call_events.status`: `sent`, `dry_run`, `failed`, `skipped_late`, `skipped_no_consent`, `skipped_quiet_hours`, `skipped_not_configured`; stale `sending` -> `send_unknown`.
  - Constants added to `config/lending_text_back.py`: `QUIET_START_HOUR = 8`, `QUIET_END_HOUR = 20` (send only when `8 <= ET hour < 20`), `STALE_SENDING_SECONDS = 300`.

- [ ] **Step 1: Add the constants**

Append to `config/lending_text_back.py`:

```python
# Safety net only: calls already stop at 7:15 pm ET and start at 9 am, and a text must go out within
# 60 s of the call, so a legitimate text is always inside this window. It guards a misconfigured
# dialer schedule. (Florida's texting law is a counsel question; this does not advise on it.)
QUIET_START_HOUR = 8
QUIET_END_HOUR = 20
STALE_SENDING_SECONDS = 300
```

- [ ] **Step 2: Write the failing tests**

```python
"""WP-GL-9 processor: the spec's 'done when' scenarios and the failure modes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_gl9_text_back import apply_to
from src.lending.consent import record_consent
from src.lending.dispositions import queue_missed_call
from src.lending.ghl_sms import GhlSmsError
from src.lending.text_back import process_pending

NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
PHONE = "+18135558601"
NUMBER = "+18135550100"


class Sender:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def __call__(self, phone, body, first_name=None):
        if self.fail:
            raise GhlSmsError("GHL message send failed: HTTP 500")
        self.sent.append((phone, body, first_name))
        return f"msg-{len(self.sent)}"


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def seed(db, call_id="c1", *, phone=PHONE, age=10, queue="verified_maturity", caller="Alex", address="123 Main St, Tampa FL"):
    ended = NOW - timedelta(seconds=age)
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, queue, caller_name, raw_event) "
                    "VALUES (:c, :p, 'outbound', :e, :q, :n, '{}'::jsonb)"), {"c": call_id, "p": phone, "e": ended, "q": queue, "n": caller})
    record = {"property_address": address} if address else None
    return queue_missed_call(db, call_id, phone, None, record, ended)


def status(db, call_id="c1"):
    return db.execute(text("SELECT status FROM lending.missed_call_events WHERE dialer_call_id = :c"), {"c": call_id}).scalar()


def run(db, sender, *, enabled=True, number=NUMBER, now=NOW):
    return process_pending(db, sender=sender, enabled=enabled, number=number, now=now)


def test_an_unanswered_call_to_a_consented_contact_gets_exactly_one_text(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    sender = Sender()
    assert run(db, sender) == {"sent": 1}
    assert [(p, "123 Main St" in b, b.endswith("Reply STOP to opt out.")) for p, b, _ in sender.sent] == [(PHONE, True, True)]
    row = db.execute(text("SELECT status, template_key, provider_message_id, decided_at FROM lending.missed_call_events")).one()
    assert row[:3] == ("sent", "maturity", "msg-1") and row[3] is not None
    assert run(db, sender) == {} and len(sender.sent) == 1  # nothing pending: never a second text


def test_a_contact_without_consent_gets_no_text(db):
    seed(db)
    sender = Sender()
    assert run(db, sender) == {"skipped_no_consent": 1} and sender.sent == []


def test_consent_plus_suppression_still_gets_no_text(db):
    record_consent(db, PHONE, "web_form")
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    seed(db)  # queue_missed_call itself marks a suppressed number 'blocked'
    sender = Sender()
    assert run(db, sender) == {} and sender.sent == [] and status(db) == "blocked"


def test_an_event_older_than_sixty_seconds_is_never_texted(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db, age=61)
    sender = Sender()
    assert run(db, sender) == {"skipped_late": 1} and sender.sent == []


def test_a_backlog_after_an_outage_sends_nothing_stale(db):
    record_consent(db, PHONE, "on_call_yes")
    for i in range(3):
        seed(db, f"c{i}", phone=f"+1813555870{i}", age=600)
        record_consent(db, f"+1813555870{i}", "on_call_yes")
    sender = Sender()
    assert run(db, sender) == {"skipped_late": 3} and sender.sent == []


def test_flag_off_records_a_dry_run_and_sends_nothing(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    sender = Sender()
    assert run(db, sender, enabled=False) == {"dry_run": 1} and sender.sent == []


def test_no_sender_configured_fails_closed(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    assert run(db, None) == {"skipped_not_configured": 1}


def test_outside_eight_to_eight_et_is_not_texted(db):
    record_consent(db, PHONE, "on_call_yes")
    late_night = datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc)  # 9:00 pm ET
    ended = late_night - timedelta(seconds=10)
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, raw_event) "
                    "VALUES ('c1', :p, 'outbound', :e, '{}'::jsonb)"), {"p": PHONE, "e": ended})
    queue_missed_call(db, "c1", PHONE, None, None, ended)
    sender = Sender()
    assert run(db, sender, now=late_night) == {"skipped_quiet_hours": 1} and sender.sent == []


def test_a_failed_send_is_recorded_and_never_retried(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    assert run(db, Sender(fail=True)) == {"failed": 1}
    assert status(db) == "failed" and run(db, Sender()) == {}


def test_a_crash_after_the_claim_becomes_send_unknown_and_keeps_the_slot(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    db.execute(text("UPDATE lending.missed_call_events SET status = 'sending', decided_at = :t"), {"t": NOW - timedelta(minutes=6)})
    assert run(db, Sender()) == {} and status(db) == "send_unknown"
    assert queue_missed_call(db, "c2", PHONE, None, None, NOW) == "duplicate_day"


def test_a_skipped_text_does_not_burn_the_days_slot(db):
    seed(db, "c1")                       # 9 am, no consent -> skipped
    assert run(db, Sender()) == {"skipped_no_consent": 1}
    record_consent(db, PHONE, "on_call_yes")
    seed(db, "c2")                       # later call, consent now exists
    sender = Sender()
    assert run(db, sender) == {"sent": 1} and len(sender.sent) == 1


def test_a_second_missed_call_after_a_sent_text_is_a_duplicate_day(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db, "c1")
    sender = Sender()
    run(db, sender)
    assert seed(db, "c2") == "duplicate_day" and run(db, sender) == {} and len(sender.sent) == 1


def test_nurture_queue_and_no_address_use_the_general_text(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db, queue="nurture")
    sender = Sender()
    run(db, sender)
    assert "the loan on" not in sender.sent[0][1] and "Sorry we missed each other" in sender.sent[0][1]


def test_the_borrowers_first_name_comes_from_the_load_table(db):
    record_consent(db, PHONE, "on_call_yes")
    db.execute(text("INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, campaign_tag, borrower_name) "
                    "VALUES ('r', 'transaction_ready', 'ref', :p, 'h', 'Transaction ready', 'Dana Cruz')"), {"p": PHONE})
    seed(db, queue="transaction_ready")
    sender = Sender()
    run(db, sender)
    assert sender.sent[0][2] == "Dana" and sender.sent[0][1].startswith("Hi Dana,")


def test_the_county_comes_from_the_calling_pool_staging_table(db):
    record_consent(db, PHONE, "on_call_yes")
    try:
        with db.begin_nested():
            db.execute(text("INSERT INTO lending.calling_pool_staging (run_id, pool_name, county_name, normalized_phone, "
                            "aircall_campaign_tag, source_table) VALUES (gen_random_uuid(), 'active_builder', 'Hillsborough', :p, 'x', 't')"),
                       {"p": PHONE})
    except Exception:
        pytest.skip("lending.calling_pool_staging shape differs on this DB (PR #318 migration)")
    seed(db, queue="builders")
    sender = Sender()
    run(db, sender)
    assert "investors in Hillsborough fund their next deal" in sender.sent[0][1]
```

- [ ] **Step 3: Run to verify they fail**

Run: `pytest tests/lending/test_text_back.py -v`
Expected: FAIL (`ImportError: cannot import name 'process_pending'`).

- [ ] **Step 4: Implement the processing half of `src/lending/text_back.py`**

Add these imports at the top of the file: `import logging`, `from datetime import timezone`, `from typing import Callable, Optional`, `from zoneinfo import ZoneInfo`, `from sqlalchemy import text`, plus `from config.lending_missed_call import MAX_LATE_SECONDS, TIMEZONE`, `from config.lending_text_back import QUIET_END_HOUR, QUIET_START_HOUR, STALE_SENDING_SECONDS`, `from src.lending.compliance import phone_hash`, `from src.lending.consent import has_text_consent`, `from src.lending.ghl_sms import GhlSmsError`, and `logger = logging.getLogger(__name__)`. Then append:

```python
TextSender = Callable[[str, str, Optional[str]], str]

_ET = ZoneInfo(TIMEZONE)


def _release_stale_claims(db, now: datetime) -> int:
    """A row left in 'sending' (crash after the claim) is unknown: it keeps the day's slot and is never resent."""
    stale = db.execute(
        text("UPDATE lending.missed_call_events SET status = 'send_unknown' "
             "WHERE status = 'sending' AND decided_at < :cutoff RETURNING id"),
        {"cutoff": now - timedelta(seconds=STALE_SENDING_SECONDS)},
    ).scalars().all()
    if stale:
        logger.error("[text-back] %d event(s) stuck in 'sending' marked send_unknown; check GHL for duplicates", len(stale))
    return len(stale)


def _claim(db, limit: int) -> list[PendingText]:
    ids = db.execute(
        text("WITH picked AS (SELECT id FROM lending.missed_call_events WHERE status = 'pending' "
             "ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED) "
             "UPDATE lending.missed_call_events e SET status = 'sending', decided_at = now() "
             "FROM picked WHERE e.id = picked.id RETURNING e.id"),
        {"limit": limit},
    ).scalars().all()
    if not ids:
        return []
    rows = db.execute(
        text("SELECT e.id, e.dialer_call_id, e.phone, e.property_address, cd.call_ended_at, cd.queue, cd.caller_name "
             "FROM lending.missed_call_events e JOIN lending.call_dispositions cd ON cd.dialer_call_id = e.dialer_call_id "
             "WHERE e.id = ANY(:ids) ORDER BY e.created_at"),
        {"ids": ids},
    ).all()
    phones = sorted({r[2] for r in rows})
    names, counties = _borrower_names(db, phones), _counties(db, phones)
    return [PendingText(event_id=r[0], call_id=r[1], phone=r[2], ended_at=r[4], property_address=r[3], queue=r[5],
                        caller_name=r[6], borrower_name=names.get(r[2]), county=counties.get(r[2])) for r in rows]


def _borrower_names(db, phones: list[str]) -> dict[str, str]:
    if db.execute(text("SELECT to_regclass('lending.dialer_load_records') IS NOT NULL")).scalar() is not True:
        return {}
    rows = db.execute(
        text("SELECT DISTINCT ON (phone) phone, borrower_name FROM lending.dialer_load_records "
             "WHERE phone = ANY(:p) ORDER BY phone, loaded_at DESC"), {"p": phones}).all()
    return {r[0]: r[1] for r in rows if r[1]}


def _counties(db, phones: list[str]) -> dict[str, str]:
    if db.execute(text("SELECT to_regclass('lending.calling_pool_staging') IS NOT NULL")).scalar() is not True:
        return {}
    rows = db.execute(
        text("SELECT DISTINCT ON (normalized_phone) normalized_phone, county_name FROM lending.calling_pool_staging "
             "WHERE normalized_phone = ANY(:p) ORDER BY normalized_phone, id DESC"), {"p": phones}).all()
    return {r[0]: r[1] for r in rows if r[1]}


def _record(db, event_id: int, status: str, template_key: Optional[str] = None, message_id: Optional[str] = None) -> None:
    db.execute(
        text("UPDATE lending.missed_call_events SET status = :s, decided_at = now(), "
             "template_key = COALESCE(:t, template_key), provider_message_id = COALESCE(:m, provider_message_id) WHERE id = :id"),
        {"s": status, "t": template_key, "m": message_id, "id": event_id},
    )


def _decide(db, item: PendingText, *, sender: Optional[TextSender], enabled: bool, now: datetime) -> str:
    """The outcome for one claimed event; 'send' means every gate passed and a sender exists."""
    if (now - item.ended_at).total_seconds() > MAX_LATE_SECONDS:
        return "skipped_late"
    if not has_text_consent(db, item.phone):
        return "skipped_no_consent"
    if not QUIET_START_HOUR <= now.astimezone(_ET).hour < QUIET_END_HOUR:
        return "skipped_quiet_hours"
    if not enabled:
        return "dry_run"
    if sender is None:
        return "skipped_not_configured"
    return "send"


def process_pending(db, *, sender: Optional[TextSender], enabled: bool, number: Optional[str],
                    now: Optional[datetime] = None, limit: int = 50) -> dict[str, int]:
    """Decide every pending missed-call event once. Commits: the claim first (so a crash cannot make
    a second worker send the same text), then each event's outcome on its own."""
    now = now or datetime.now(timezone.utc)
    _release_stale_claims(db, now)
    items = _claim(db, limit)
    db.commit()
    counts: dict[str, int] = {}
    for item in items:
        outcome, template_key = "failed", None
        try:
            outcome = _decide(db, item, sender=sender, enabled=enabled, now=now)
            if outcome == "send":
                if not number:
                    outcome = "skipped_not_configured"
                else:
                    template_key = choose_template(item)
                    body = render_body(item, template_key, number=number)
                    message_id = sender(item.phone, body, first_name_of(item.borrower_name))
                    _record(db, item.event_id, "sent", template_key, message_id)
                    outcome = "sent"
            if outcome != "sent":
                _record(db, item.event_id, outcome, template_key)
        except GhlSmsError:
            outcome = "failed"
            _record(db, item.event_id, "failed", template_key)
        except Exception as exc:  # class only: SQL / HTTP errors can embed phones
            logger.error("[text-back] event %s crashed (%s); left for the stale-claim sweep", item.event_id, type(exc).__name__)
            db.rollback()
            continue
        db.commit()
        counts[outcome] = counts.get(outcome, 0) + 1
        logger.info("[text-back] call=%s phone_hash=%s outcome=%s", item.call_id, phone_hash(item.phone)[:12], outcome)
    return counts


def run_text_back_cycle(*, now: Optional[datetime] = None) -> dict[str, int]:
    """One pass for the poller loop: real settings, own session."""
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.ghl_sms import get_sender, texting_number

    with lending_session() as db:
        return process_pending(db, sender=get_sender(), enabled=get_settings().missed_call_text_enabled,
                               number=texting_number(), now=now)
```

Also add `from datetime import timedelta` to the datetime import. Note `_record(... "sent")` and the commit happen after GHL accepted the text; if the DB write fails after a successful send the `except Exception` branch rolls back and the row stays `sending`, so the stale sweep turns it into `send_unknown` (slot held, never resent).

- [ ] **Step 5: Run to verify they pass**

Run: `pytest tests/lending/test_text_back.py tests/lending/test_text_back_render.py tests/lending/test_apply_lending_gl9_text_back.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/lending/text_back.py config/lending_text_back.py tests/lending/test_text_back.py
git commit -m "feat(lending): WP-GL-9 text-back processor - consent, 60 s, one-per-day, GHL send"
```

---

### Task 5: One poller - run the text-back cycle inside it, retire the second reader

**Files:**
- Modify: `src/lending/cdr_poller.py:main` (loop)
- Modify: `src/lending/missed_call_poller.py:main`
- Modify: `deploy/systemd/README.md`
- Test: `tests/lending/test_text_back.py` (append)

**Interfaces:**
- Consumes: `run_text_back_cycle()` (Task 4).
- Produces: `src.lending.cdr_poller.text_back_step() -> None` (swallows and logs any error so ingestion is never blocked).

- [ ] **Step 1: Write the failing tests (append to `tests/lending/test_text_back.py`)**

```python
def test_a_text_back_error_never_stops_the_poller_cycle(monkeypatch, caplog):
    from src.lending import cdr_poller
    monkeypatch.setattr("src.lending.text_back.run_text_back_cycle", lambda **k: (_ for _ in ()).throw(RuntimeError(PHONE)))
    cdr_poller.text_back_step()  # must not raise
    assert PHONE not in caplog.text and "text-back cycle failed" in caplog.text


def test_the_old_second_poller_refuses_to_start(caplog):
    from src.lending import missed_call_poller
    missed_call_poller.main(["--once"])
    assert "superseded" in caplog.text
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/lending/test_text_back.py -k "poller" -v`
Expected: FAIL (`AttributeError: module 'src.lending.cdr_poller' has no attribute 'text_back_step'`).

- [ ] **Step 3: Implement**

In `src/lending/cdr_poller.py` add above `main`:

```python
def text_back_step() -> None:
    """WP-GL-9: send the missed-call texts queued by the cycle that just ran. Never blocks ingestion."""
    from src.lending.text_back import run_text_back_cycle

    try:
        counts = run_text_back_cycle()
        if counts:
            logger.info("[lending-cdr-poller] text-back %s", counts)
    except Exception as exc:  # class only: bodies carry phone numbers
        logger.error("[lending-cdr-poller] text-back cycle failed (%s); retrying next interval", type(exc).__name__)
```

and in the `while True:` loop, immediately after the `try/except` that runs `run_cycle` (before `if args.once: return 0`), add `text_back_step()`.

In `src/lending/missed_call_poller.py`, at the top of `main()` (before parsing args):

```python
    logger.error("[missed-call-poller] superseded by the text-back step in src.lending.cdr_poller (WP-GL-9, GHL sender); "
                 "not starting. Running two readers of the call feed would double-count attempts.")
    return
```

and replace its module docstring first line with `"""SUPERSEDED (WP-GL-9): the missed-call text now runs inside src.lending.cdr_poller. Kept for its tests; do not run."""`.

In `deploy/systemd/README.md` remove `fa-lending-missed-call-poller` from the `cp` / `enable --now` / `journalctl` commands (lines ~37-40) and replace the description line (~27) with: `- fa-lending-missed-call-poller — SUPERSEDED by WP-GL-9: the text-back runs inside fa-lending-cdr-poller. Do not install.`

- [ ] **Step 4: Run to verify they pass, plus the existing poller tests**

Run: `pytest tests/lending/test_text_back.py tests/lending/test_cdr_poller.py tests/lending/test_missed_call_text.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lending/cdr_poller.py src/lending/missed_call_poller.py deploy/systemd/README.md tests/lending/test_text_back.py
git commit -m "feat(lending): WP-GL-9 run text-back inside the single CDR poller; retire the second reader"
```

---

### Task 6: Consent and STOP feeds from GoHighLevel (webhook)

**Files:**
- Modify: `src/api/lending_ghl_router.py`
- Modify: `config/lending_text_back.py` (add `STOP_KEYWORDS`)
- Test: `tests/lending/test_ghl_text_consent_webhook.py`

**Interfaces:**
- Consumes: `record_consent(db, phone, source, *, call_id=None, captured_by=None, at=None)`, `revoke_consent(db, phone)` (`src.lending.consent`); `_verify_secret`, `_field` (router).
- Produces: `POST /webhooks/lending/ghl-text-consent` with body `{"source": "web_form" | "inbound_text", "phone": str, "consent": bool|str (web_form), "message": str (inbound_text), "contact_id": str?}`; header `X-Webhook-Secret` = `LENDING_GHL_WEBHOOK_SECRET`; returns `{"recorded": bool, "revoked": bool}`; `422` for unknown source / missing or unparseable phone; `401`/`503` per existing `_verify_secret`. `config.lending_text_back.STOP_KEYWORDS`.

- [ ] **Step 1: Add the keywords**

Append to `config/lending_text_back.py`:

```python
# Carrier standard opt-out keywords (a text that is only one of these is a STOP, never consent).
STOP_KEYWORDS = frozenset({"stop", "stopall", "unsubscribe", "cancel", "end", "quit"})
```

- [ ] **Step 2: Write the failing tests**

```python
"""GHL workflow webhook -> lending text consent (website form checkbox, inbound texts, STOP)."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from src.lending.consent import has_text_consent, record_consent

PHONE = "+18135559402"
SECRET = "test-ghl-secret"
URL = "/webhooks/lending/ghl-text-consent"
HEADERS = {"X-Webhook-Secret": SECRET}


@pytest.fixture
def client(lending_db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: lending_db
    return TestClient(app)


def _sources(db):
    return [r[0] for r in db.execute(text("SELECT source FROM lending.text_consents WHERE phone = :p AND revoked_at IS NULL"), {"p": PHONE})]


@pytest.mark.parametrize("flag", [True, "true", "Yes", "1", "checked"])
def test_a_checked_web_form_box_records_consent(client, lending_db, flag):
    r = client.post(URL, headers=HEADERS, json={"source": "web_form", "phone": "(813) 555-9402", "consent": flag, "contact_id": "ct1"})
    assert r.status_code == 200 and r.json()["recorded"] is True
    assert _sources(lending_db) == ["web_form"] and has_text_consent(lending_db, PHONE)


@pytest.mark.parametrize("flag", [False, "false", "", "no", None, "unchecked"])
def test_an_unchecked_or_missing_box_records_nothing(client, lending_db, flag):
    r = client.post(URL, headers=HEADERS, json={"source": "web_form", "phone": PHONE, "consent": flag})
    assert r.status_code == 200 and r.json()["recorded"] is False and _sources(lending_db) == []


def test_an_inbound_text_is_consent_to_reply(client, lending_db):
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": "Yes call me tomorrow"})
    assert r.json()["recorded"] is True and _sources(lending_db) == ["inbound_text"]


@pytest.mark.parametrize("message", ["STOP", " stop ", "Stop.", "UNSUBSCRIBE", "cancel", "END", "Quit", "stopall"])
def test_a_stop_text_is_never_consent_and_revokes_what_exists(client, lending_db, message):
    record_consent(lending_db, PHONE, "on_call_yes")
    record_consent(lending_db, PHONE, "web_form")
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": message})
    assert r.json() == {"recorded": False, "revoked": True}
    assert _sources(lending_db) == [] and not has_text_consent(lending_db, PHONE)


def test_a_sentence_containing_stop_is_still_a_normal_message(client, lending_db):
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": "please don't stop calling"})
    assert r.json()["recorded"] is True


@pytest.mark.parametrize("body,code", [
    ({"source": "carrier_pigeon", "phone": PHONE}, 422),
    ({"source": "web_form", "consent": True}, 422),
    ({"source": "web_form", "phone": "nope", "consent": True}, 422),
])
def test_bad_requests_are_rejected(client, body, code):
    assert client.post(URL, headers=HEADERS, json=body).status_code == code


def test_the_secret_is_required(client, lending_db):
    assert client.post(URL, json={"source": "web_form", "phone": PHONE, "consent": True}).status_code == 401
    assert _sources(lending_db) == []
```

- [ ] **Step 3: Run to verify they fail**

Run: `pytest tests/lending/test_ghl_text_consent_webhook.py -v`
Expected: FAIL (404 from the missing route).

- [ ] **Step 4: Implement in `src/api/lending_ghl_router.py`**

Add imports `from config.lending_text_back import STOP_KEYWORDS` and `from src.lending.consent import record_consent, revoke_consent`, update the module docstring's "Endpoints" line to list `POST /webhooks/lending/ghl-text-consent`, and append:

```python
_TRUTHY = frozenset({"true", "yes", "y", "1", "on", "checked"})
_CONSENT_SOURCES = frozenset({"web_form", "inbound_text"})


def _is_stop(message: str) -> bool:
    return message.strip().strip(".!").strip().lower() in STOP_KEYWORDS


@router.post("/ghl-text-consent")
def ghl_text_consent(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow reports text consent. ``web_form``: the lead form's consent box (only a checked
    box counts). ``inbound_text``: the contact texted the Next Deal Lending number; a bare STOP
    keyword revokes instead. Body: source, phone, consent (web_form), message (inbound_text), contact_id."""
    _verify_secret(x_webhook_secret)
    source = _field(body, "source")
    phone = normalize(_field(body, "phone"))
    if source not in _CONSENT_SOURCES or not phone:
        raise HTTPException(status_code=422, detail="source (web_form or inbound_text) and a valid phone are required")
    contact_id = _field(body, "contact_id") or _field(body, "id")
    try:
        if source == "inbound_text" and _is_stop(_field(body, "message") or ""):
            revoke_consent(db, phone)
            db.commit()
            return {"recorded": False, "revoked": True}
        if source == "web_form" and str(_field(body, "consent") or "").strip().lower() not in _TRUTHY:
            return {"recorded": False, "revoked": False}
        record_consent(db, phone, source, captured_by=f"ghl:{contact_id}" if contact_id else None)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] consent webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Consent could not be recorded") from exc
    return {"recorded": True, "revoked": False}
```

Note `_field` returns `str(value) if value else None`, so a JSON `false`/`""`/`null` becomes `None` (-> not truthy) and `true` becomes `"True"` (-> truthy after `.lower()`).

- [ ] **Step 5: Run to verify they pass, plus the router's existing tests**

Run: `pytest tests/lending/test_ghl_text_consent_webhook.py tests/lending/test_ghl_opt_out_webhook.py tests/lending/test_consent.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add config/lending_text_back.py src/api/lending_ghl_router.py tests/lending/test_ghl_text_consent_webhook.py
git commit -m "feat(lending): WP-GL-9 consent feed from GHL (web form box, inbound text, STOP revokes)"
```

---

### Task 7: Runbook, hand-off docs and CLAUDE.md (the GHL-side work that remains)

**Files:**
- Create: `docs/lending/text-back-runbook.md`
- Modify: `docs/lending/dialer-disposition-runbook.md` ("Handoff to the text-back task" section)
- Modify: `CLAUDE.md` (Lending section; keep it short)

- [ ] **Step 1: Write `docs/lending/text-back-runbook.md`** with exactly these sections (content below; this is the checklist that remains once GHL access arrives):

1. **What it does / the rules** - 60 s, consented only, one per ET day, STOP language, GHL only, off until A2P Verified, calls-only + live voicemail until then. Statuses table (`sent, dry_run, failed, send_unknown, skipped_late, skipped_no_consent, skipped_quiet_hours, skipped_not_configured, blocked, duplicate_day`).
2. **Environment** (server `.env`, never committed): **interim** - the existing `GHL_API_KEY` / `GHL_LOCATION_ID` (Bay Street Capital sub-account) are used automatically; set `LENDING_GHL_SMS_FROM_NUMBER` (E.164 of the one calling+texting number; it is also the number printed in "call or text me back at") and `LENDING_GHL_WEBHOOK_SECRET` (exists). **When the client provides the Next Deal Lending sub-account:** set `LENDING_GHL_API_KEY` (Private Integration token of that sub-account) and `LENDING_GHL_LOCATION_ID`, restart `fa-api` and `fa-lending-cdr-poller` - nothing else changes (texts and the DND leg both follow). `MISSED_CALL_TEXT_ENABLED` stays `false` until step 3.6.
3. **GHL-side setup, in order** (client gives `hari@heu.ai` Agency Admin and a card first; client answer B2/B1):
   1. Agency Settings -> Phone Integration -> Switch to LeadConnector Phone; Sub Account Settings -> Next Deal Lending -> Link to LeadConnector; wait for the green LC Phone badge. Add a card (Settings -> Billing).
   2. Send the client the expected monthly cost (number rental, per-message, 10DLC one-time + monthly campaign fee) **before** buying (client question 10).
   3. Settings -> Phone Numbers -> Add Number: US, Local, SMS + Voice, area code 813 or 727; complete identity verification; record the number in `LENDING_GHL_SMS_FROM_NUMBER`.
   4. Credentials. Interim: confirm the existing Bay Street token in `.env` has the scopes contacts write and conversations/messages write (the `--send-test` in section 5 proves it). Later: create a Private Integration token with those scopes in the Next Deal Lending sub-account and set `LENDING_GHL_API_KEY` + `LENDING_GHL_LOCATION_ID`.
   5. **10DLC (Trust Center)**, only after `nextdeallending.com` is public (WP-GL-11): Standard Brand, brand **Next Deal Lending**, legal entity HEU AI LLC, address 971 US Highway 202N, Ste N, Branchburg, NJ 08876 (client C3), website URL, use case = missed-call text-back / booking confirmations / reminders / replies; sample messages = the three templates in `config/lending_text_back.py` plus the three approved confirmation/reminder texts; opt-in description = website form checkbox (unchecked by default) + verbal consent on the call ("Is it okay if we text you the confirmation?"); opt-out = "Reply STOP". **Confirm with the client whether the existing HEU AI LLC registration is updated or a new GHL registration is filed** (open question H3 / v2 Q18) and whether HEU AI LLC is the legal entity or Next Deal Lending is a DBA.
   6. Wait for approval; the number must show **A2P Verified** before any live text. If not approved by the launch date: calls-only with live voicemail; texting switches on the day it clears.
4. **GHL workflows to create** (merge-field names must be confirmed in the GHL UI; the shapes below are the contract):
   - *Contact DND changed* -> Webhook `POST https://<fa-api host>/webhooks/lending/ghl-opt-out`, header `X-Webhook-Secret`, body `{"contact_id": "{{contact.id}}", "phone": "{{contact.phone}}", "email": "{{contact.email}}"}` (already built, PR #320; **confirm the client's request "GHL wired into the #320 opt-out sync"** - this workflow is that wiring).
   - *Customer replied (SMS)* -> Webhook `POST .../webhooks/lending/ghl-text-consent`, body `{"source": "inbound_text", "contact_id": "{{contact.id}}", "phone": "{{contact.phone}}", "message": "{{message.body}}"}`. GHL's built-in STOP handling still sets DND; this call also revokes consent.
   - *Form submitted* (website lead form, WP-GL-11) -> Webhook `POST .../webhooks/lending/ghl-text-consent`, body `{"source": "web_form", "contact_id": "{{contact.id}}", "phone": "{{contact.phone}}", "consent": "{{<consent checkbox custom field>}}"}`. The checkbox must be unchecked by default on the form; only a checked value records consent.
   - Pipeline stage changed -> `.../ghl-stage` (exists, PR #326).
5. **Go-live sequence** - (a) `python migrations/apply_lending_gl9_text_back.py`; (b) set env vars; (c) `python -m src.lending.ghl_sms --send-test <your own phone>`; confirm it arrives from the Next Deal Lending number and fix `src/lending/ghl_sms.py` field names if GHL disagrees; (d) restart `fa-lending-cdr-poller`; with the flag still off, make a test unanswered call to a consented test contact and confirm the event ends `dry_run`; (e) when the number is A2P Verified set `MISSED_CALL_TEXT_ENABLED=true`, restart; (f) run the verification script below. **Rollback:** set the flag `false` and restart; events return to `dry_run`.
6. **Verification (the spec's "done when")** against a consented test contact: unanswered call -> exactly one text within 60 s from the Next Deal Lending number; second unanswered call same day -> none (`duplicate_day`); non-consented number -> none (`skipped_no_consent`); reply STOP -> no further text and the contact is DND in GHL and suppressed in FA; query: `SELECT dialer_call_id, status, template_key, decided_at - created_at FROM lending.missed_call_events ORDER BY id DESC LIMIT 20;`
7. **Caller script lines** (add to the booking close and the call): "Is it okay if we text you the confirmation?" - the caller sets the BatchDialer contact field `text_consent` = `yes`; the CDR poller records `on_call_yes` with date/time/caller (client G6, Q27). Answered inbound calls count as `inbound_call` consent automatically.
8. **Decisions taken (team lead, 2026-10-02):** interim Bay Street Capital GHL account, switched by env vars later; one number for calling and texting (a number normally lives with one provider - confirm with the client how the same number serves BatchDialer calls and GHL SMS, v2 Q8; texting stays off until the sending number is A2P Verified, and while on the Bay Street account the A2P registration belongs to that account, not Next Deal Lending); missing first name -> "Hi"; missing caller -> "our team"; Option 1/2 fall back to Option 3 without address/county; **outbound calls only** - an unanswered inbound callback creates no automated text (inbound speed-to-lead is month-two scope in the brief, an unanswered inbound call is not consent evidence under #319, and the client's callback queue already rings the original caller, then any open caller, then voicemail, with voicemails becoming next-morning calls); 8 am-8 pm ET safety window; consent is stored in `lending.text_consents` (+ BatchDialer `text_consent`), not as a GHL contact field (client D4).
9. **Not in WP-GL-9** (so nobody looks for it here): confirmation/reminder texts, reply agent, Conversation AI, email fallback for non-consenters (WP-GL-10); website and consent checkbox (WP-GL-11); Deal Drop marketing opt-in checkbox (client "Monday Deal Drop opt in", first drop Oct 12 - week-two scope); counsel sign-off on cold-number texting (client/counsel).

- [ ] **Step 2: Replace the "Handoff to the text-back task" section of `docs/lending/dialer-disposition-runbook.md`** with a short pointer: the text-back is built in `src/lending/text_back.py` (WP-GL-9), consumes `lending.missed_call_events`, sends through GHL, and `missed_call_poller` / `missed_call_texts` (PR #320) are superseded; link to `docs/lending/text-back-runbook.md`. Keep the `revoke_consent` / `text_consent` field warning (the dialer field is still `yes`, so a STOP is enforced by suppression, not by clearing the field).

- [ ] **Step 3: Update `CLAUDE.md` Lending section** by appending one sentence: `WP-GL-9 text-back: unanswered lending calls queue in lending.missed_call_events; src/lending/text_back.py (run from fa-lending-cdr-poller) sends consented, once-per-ET-day texts within 60 s through GoHighLevel (account = LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID, falling back to GHL_API_KEY / GHL_LOCATION_ID until the Next Deal Lending sub-account exists; number = LENDING_GHL_SMS_FROM_NUMBER), gated by MISSED_CALL_TEXT_ENABLED; consent feed POST /webhooks/lending/ghl-text-consent. Lending-engine SMS go through GoHighLevel only; every other FA SMS (subscribers, alerts) stays on Telnyx via sms_compliance.send_sms. Runbook: docs/lending/text-back-runbook.md.` Add `python migrations/apply_lending_gl9_text_back.py` to the migrations command list.

- [ ] **Step 4: Commit**

```bash
git add docs/lending/text-back-runbook.md docs/lending/dialer-disposition-runbook.md CLAUDE.md
git commit -m "docs(lending): WP-GL-9 runbook - GHL number, 10DLC, workflows, go-live and verification"
```

---

### Task 8: Whole-feature verification

**Files:** none new.

- [ ] **Step 1: Run the whole lending suite and the touched neighbours**

Run: `pytest tests/lending tests/services/test_lending_pool_extraction.py -q`
Expected: all PASS (DB tests skip if `DATABASE_URL` is unset; run them against a non-production database with outbound integrations stubbed, as the release package requires).

- [ ] **Step 2: Grep guards**

Run: `grep -rn "send_sms" src/lending/ | grep -v missed_call_text.py` -> expected: no output (nothing new uses the Telnyx path).
Run: `grep -rn "print(" src/lending/text_back.py src/lending/ghl_sms.py` -> expected: no output.

- [ ] **Step 3: Migration dry check on a scratch/test DB, twice** (idempotency): `PYTHONPATH=. python migrations/apply_lending_gl9_text_back.py` run two times; both succeed.

- [ ] **Step 4: Open the PR** against `feat/lending-disposition-logging`, title `feat(lending): WP-GL-9 GoHighLevel text-back (consent, 60 s, one per day)`, description listing: what is built, what needs GHL access (runbook sections 3-5), the superseded #320 poller, and the cross-PR note below. End the body with the attribution line from the session reminder.

---

### Task 9 (follow-up, after #326 merges): count real sends on the scoreboard

The WP-GL-8 checklist says "Sent to nurture = texts actually sent" (today it counts `GATE_FAILED_NURTURE`). After #326 is merged, change the scoreboard query in `src/lending/scoreboard.py` to count `lending.missed_call_events WHERE status = 'sent'` by ET day (and by `template_key`). Not part of this branch because #326 is not in its base; file it under WP-GL-8 for the same developer.

---

## Cross-PR notes to raise with the team lead (not done in this branch)

1. **GHL account (interim).** Everything lending-side in GHL currently runs on the Bay Street Capital account in `.env` (`GHL_API_KEY`/`GHL_LOCATION_ID`: #323 calendar, #326 stage webhook, the DND leg, and now this text-back). That is the agreed interim. When the client provides the Next Deal Lending sub-account, #323/#326 still read `GHL_*` directly and must be pointed at the new account too; this branch's pieces switch with `LENDING_GHL_*` alone.
2. **#320's `missed_call_poller.py` / `missed_call_text.py` / `lending.missed_call_texts`** are now dead code. Delete in a cleanup PR once #320 merges and the owner agrees (they are kept here to avoid editing another developer's PR beyond the guard).
3. **#319's runbook** proposed "the #320 pipeline gets a GHL sender". This plan instead consumes #319's `missed_call_events` through the single poller (what to do: nothing to change in #319's code; this PR rewrites that runbook section (Task 7), tell the #320 owner their missed-call poller is retired, and delete the dead #320 files in a cleanup PR after #320 merges), because the #320 reader pages the whole account feed unfiltered (non-lending campaigns could be texted), duplicates attempt recording, and only recognises "no answer" statuses while the client's approved list also treats LEFT_VOICEMAIL and CALL_FAILED as unanswered and excludes Abandoned.
4. **Live shape check (also confirms the Bay Street token's scopes).** `ghl_sms.py` request shapes are documented-API best effort; the `--send-test` step in the runbook is the confirmation.

---

## Self-Review (spec coverage, placeholders, types, review focus)

**Spec / comment coverage** (every GL-9 requirement and related client comment -> where it lands):

| Requirement | Source | Covered by |
|---|---|---|
| Text within 60 s of an unanswered call | v2 GL-9; brief 5.2 | `MAX_LATE_SECONDS` gate (Task 4), poll cadence 15 s (existing), tests `late`, `backlog` |
| From a Next Deal Lending number in GHL, not FA Telnyx | Q24, 5.5, G4 | `ghl_sms.py` (Task 3), Global Constraints, CLAUDE.md note (Task 7) |
| Names the property or reason | v2 GL-9 | Templates + `choose_template` fallbacks (Task 2) |
| One per contact per day | v2 GL-9 | Slot index + `queue_missed_call` (Task 1), tests (Tasks 1, 4) |
| Stop language | v2 GL-9; F2 | All templates; worst-case length test (Task 2) |
| Consented contacts only; none to cold numbers | Q27; v2 GL-9 | `has_text_consent` gate (Task 4) |
| Consent sources: inbound callers, inbound texters, web form checked, verbal yes logged | Q27 | existing `inbound_call`/`on_call_yes` (#319) + `inbound_text`/`web_form` via webhook (Task 6) |
| Caller asks "OK if I text you the confirmation?", yes logged (date/time/caller) | Q27, G6 | Runbook script line + existing recorder with `captured_at`/`captured_by` (Task 7) |
| Wording: Option 1/2/3 per queue, verbatim | F2 | `TEMPLATES`/`QUEUE_TEMPLATES` + verbatim test (Task 2) |
| Lists 2 and 4 / no address -> Option 3 | F2 | `nurture` + fallback tests (Tasks 2, 4) |
| Buy the NDL local number in GHL | v2 GL-9; B2 | Runbook step 3.3 (GHL-side) |
| Link LeadConnector Phone + billing / Agency Admin | v2; B2 | Runbook 3.1 (GHL-side) |
| Register 10DLC in GHL Trust Center after site public; new registration | v2 Q18; 5.2 | Runbook 3.5 (incl. HEU AI LLC vs new-registration question) |
| "A2P Verified" before any live text; calls-only + voicemail until then | v2; B3 note | Flag off by default, runbook 3.6/5 |
| Opt-out everywhere incl. GHL STOP -> BatchDialer/GHL/FA | 5.2; Q19 | Existing `/ghl-opt-out` + workflow in runbook 4; DND leg follows the NDL sub-account (Task 3); consent revoke on STOP (Task 6) |
| Sub-account = Next Deal Lending (client B1); interim Bay Street per team lead | B1 | `LENDING_GHL_*` override with `GHL_*` fallback (Task 3); cross-PR note 1 |
| Conversation AI cost estimate before spending | B2, question 10 | Runbook 3.2 (cost to client before buying); Conversation AI itself is GL-10 |
| One poller only | v2 section 6 contract | Task 5 |
| Shared consent flag read by every automated text | v2 section 6 | `has_text_consent` unchanged and reused; GL-10 reuses it |
| Abandoned never queues a missed-call text | #319 tip | Inherited from base (not reimplemented) |
| Scoreboard "sent" count | WP-GL-8 checklist | Task 9 (follow-up after #326) |
| Texting wording drafts reviewed by team before live | Org rule; Q26 | Pre-approved by client F2; runbook 8 lists defaults to confirm |
| 5pm go/no-go excludes text-back | "Additional Notes" section 9 | Runbook 3.6: texting switches on when A2P clears; not a launch gate |

**Deliberately not in this plan:** WP-GL-10 items (confirmation/reminder texts, email fallback, reply agent, Conversation AI, confirmation-call task); WP-GL-11 website/form/DNS; Deal Drop marketing opt-in (week two); counsel sign-off on cold-number texting; Florida/Georgia DNC; multi-line.

**Placeholder scan:** no TBD/TODO; the only unresolved items are GHL UI merge-field names (runbook 4) and the live GHL response shapes, both called out with a concrete verification step (`--send-test`).

**Type consistency:** `PendingText` fields (`event_id, call_id, phone, ended_at, property_address, queue, caller_name, borrower_name, county`) are identical in Tasks 2 and 4 and in the tests. `process_pending(db, *, sender, enabled, number, now, limit)` matches its use in Task 4 tests and `run_text_back_cycle`. `GhlSmsSender.__call__(phone, body, first_name)` matches the `TextSender` alias and the test `Sender`. Status strings match the Task 1 vocabulary (`skipped_not_configured` is 22 chars, hence `varchar(24)`).

**Review Focus:** items 1-6 each have a named test (1: Tasks 1 & 4 slot tests; 2: `first_name_of`/no-caller tests; 3: failed / send_unknown tests; 4: STOP, unchecked-box, consent+suppression tests; 5: late/backlog/quiet-hours tests; 6: worst-case length test).

**Known risks to watch during execution: (0) texts are branded Next Deal Lending but, while on the Bay Street account, leave from that account's number and A2P registration - keep `MISSED_CALL_TEXT_ENABLED=false` until the sending number is A2P Verified;** (a) if BatchDialer's CDR appears more than ~45 s after hang-up the 60 s rule will skip real texts - measure on the first real call; (b) `to_regclass`-guarded lookups return `None` names/county if `calling_pool_staging` / `dialer_load_records` are absent, which degrades to the general text rather than failing; (c) the base branch may move (#319 gets new commits) - rebase before opening the PR.
