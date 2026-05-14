# PRD — Compliance Baseline Hardening (Phase 2B v9)

Source decisions: `docs/compliance_answers.md`. Audit: `docs/compliance_baseline_audit.md`. Grilling Q&A: `docs/compliance_baseline_grill.md`.

---

## Problem Statement

We sell distressed-property SMS leads. Carriers, the FCC, and CTIA all expect us to (a) honor STOP immediately, (b) only message numbers with provable consent, (c) respect 8am–9pm quiet hours, (d) keep an auditable record of every send and suppression, and (e) never double-charge a customer when Stripe replays a webhook.

The Phase 2B compliance baseline is mostly built but has gaps that block launch:

- **YES is overloaded.** It's both a TCPA opt-in keyword and a PAUSE/offer confirmation command. Today the inbound webhook never calls the opt-in handler at all, so YES replies from new numbers are silently dropped.
- **Marketing SMS can be sent to numbers with zero recorded consent.** The opt-in gate only runs inside the Cora agent path. Every other caller (`send_opt_in_prompt`, sample-leads SMS, sms_commands replies, missed-call welcome, NWS storm notify) skips it.
- **Quiet-hours suppressions crash the DLQ insert.** `sms_compliance.send_sms` writes `reason="quiet_hours"` but the table CHECK constraint only allows four other reasons. The row is lost on flush.
- **Product commands are non-functional.** `sms_commands._find_subscriber` is a stub that returns None for every inbound, so LOCK/BALANCE/TOPUP/etc. all reply "Reply HELP to get started" no matter who texts.
- **Phone numbers are stored in mixed formats.** `_normalize` is whitespace-strip only. `+18135550100` and `18135550100` are different keys; an opt-out under one will miss the other.
- **Direct (non-agent) SMS sends have no audit row.** Only Cora's path writes `MessageOutcome`. Ops can't answer "did Forced Action send anything to this number today?" without sifting through logs.

If we ship as-is we risk TCPA fines, carrier filtering on the brand, and ops blindness on suppression events.

## Solution

A bounded compliance hardening pass that:

1. Locks down inbound routing so STOP, opt-in (YES/START/JOIN/SUBSCRIBE/UNSTOP), and product commands each go to the right handler exactly once.
2. Makes `sms_compliance.send_sms` default-deny for marketing — every caller must declare `message_type` and a number must have `has_opted_in` to receive marketing.
3. Adds a single canonical phone-normalization helper used by every read and write of phone columns, plus a one-shot data migration to backfill existing rows.
4. Adds a single `SmsSendLog` audit row per send attempt (sent / suppressed / dry-run / failed), with the suppression reason and the vendor message id.
5. Fixes the DLQ CHECK constraint and resolves `_find_subscriber` via the existing `SmsOptIn` table — no new schema for either.
6. Adds the scenario and unit tests that prove (1)–(5).

Stripe webhook idempotency and duplicate-payment prevention are already correct; no work needed there.

## User Stories

1. As a subscriber, I want to reply STOP and stop receiving SMS immediately, so that I can opt out without friction.
2. As a subscriber, I want to reply YES to a prompt and get a one-time confirmation, so that I know my consent was recorded.
3. As a subscriber, I want to reply YES to a PAUSE prompt and have my subscription paused, so that the same word does the right thing in two different contexts.
4. As a new lead, I want my consent to be recorded only when I was actually asked, so that a casual "yes" in conversation doesn't get treated as marketing consent.
5. As a subscriber with no recorded consent, I do not want to receive marketing SMS, so that the platform respects TCPA.
6. As a subscriber, I want transactional SMS (payment receipts, pause confirmations, dispute notices) even if I haven't opted into marketing, so that I'm informed about my account.
7. As a new subscriber, I want to receive the opt-in prompt even though I haven't opted in yet, so that the consent flow can start.
8. As a subscriber, I want SMS only between 8am and 9pm in my local time, so that I'm not woken up.
9. As a subscriber on a 850 Panhandle number that's actually CST, I want the quiet-hours gate to over-suppress rather than risk a TCPA-violating send between 8pm and 9pm CST.
10. As an ops engineer, I want every suppressed SMS to appear in `sms_dead_letters` with an accurate reason, so that I can review and resolve them.
11. As an ops engineer, I want every send attempt (sent, suppressed, dry-run, failed) to appear in a single audit log, so that I can answer "did we send anything to this number today" with one query.
12. As a subscriber, I want my phone number to be looked up the same way whether I texted in as `8135550100` or our system stored it as `+18135550100`, so that opt-out applies consistently.
13. As an ops engineer, I want a dry-run collision report before the phone-normalization backfill runs, so that I can spot duplicate rows that would violate the unique constraint.
14. As a subscriber, I want LOCK, BALANCE, BOOST, AUTO ON, AUTO OFF, PAUSE, RESUME, TOPUP, REPORT, YEARLY, SAVE CARD commands to resolve me by phone and reply with a real link, so that the product commands actually work.
15. As an unknown caller, I want product commands to return the "Reply HELP to get started" fallback rather than crash, so that the system fails gracefully.
16. As an ops engineer, I want to be sure the Telnyx webhook only ever creates an opt-in row when there's a pending opt-in prompt sentinel in Redis, so that arbitrary YES texts can't pollute the consent log.
17. As an ops engineer, I want the Redis-backed STOP/DLQ deferral decision to be documented in code and in CLAUDE.md, so that the next engineer doesn't waste time hunting for it.
18. As an ops engineer, I want a one-line query to count quiet-hours suppressions per day, so that I can detect timezone-mapping bugs.
19. As a developer, I want scenario tests that POST a real Telnyx-shaped envelope to the inbound webhook and assert the right side effects, so that wiring bugs (like the current YES drop) can't ship again.
20. As a developer, I want unit tests for `has_opted_in` enforcement, `message_type="opt_in_prompt"` bypass behaviour, the new `SmsSendLog` rows, and phone normalization round-trips.
21. As a developer, I want the canonical phone normalizer in one module so I can't accidentally call a one-off `phone.strip()` somewhere else.
22. As a finance ops person, I want Stripe webhook idempotency to keep blocking duplicate payment events, so that customers aren't double-charged. (Already implemented; verified.)
23. As a customer, I want my Stripe payment to fulfil exactly once even if Stripe retries the webhook six times. (Already implemented; verified.)
24. As an ops engineer reading the codebase, I want any remaining Twilio references to be flagged for context but not necessarily rewritten if they're only doc/comment text, so that the diff stays scoped.

## Implementation Decisions

Resolved via grilling. Locked answers below.

### Routing and consent

- **Q1: YES disambiguation — Option B.** `send_opt_in_prompt` sets a Redis sentinel `opt_in_pending:{phone}` with a 15-minute TTL when it fires. `handle_opt_in_reply` returns the confirmation reply and records the opt-in only if the sentinel is present; otherwise returns None and lets the message fall through to `sms_commands.parse`. This matches CTIA double-opt-in semantics — consent only counts when we asked.
- **Q2: Quiet-hours DLQ — Option A.** Widen the `sms_dead_letters.reason` CHECK constraint to include `quiet_hours` (and `no_opt_in` while we're touching it, for Q3). New Alembic migration.
- **Q3: Marketing vs transactional — Option A.** `sms_compliance.send_sms` grows a `message_type` parameter (default `"marketing"`). Marketing sends are blocked when `has_opted_in()` returns False. Every existing caller in `src/` is audited and explicitly tagged as `marketing`, `transactional`, or `opt_in_prompt`.
- **Q4: Opt-in prompt bypass — Option B.** New value `message_type="opt_in_prompt"`. Bypasses the opt-in gate. Still runs opt-out, quiet-hours, and creds gates. Allowed values: `{marketing, transactional, opt_in_prompt}`.
- **Q12: Gate ordering inside `send_sms`** — opt-out → opt-in → quiet hours → creds → dispatch. Each gate writes its own DLQ row with a distinct `reason` value. Hard suppression first, consent second, timing third.

### Subscriber lookup

- **Q5: `_find_subscriber` source — Option B.** Use the existing `sms_opt_ins` table: latest row by `opted_in_at DESC LIMIT 1` for the phone, then load `Subscriber` by `subscriber_id`. No new `Subscriber.phone` column. Subscribers without an opt-in row remain unresolvable — which is correct, since they can't legally receive marketing anyway.

### Audit and observability

- **Q6: Per-send audit — Option B.** New `sms_send_logs` table. One row per `sms_compliance.send_sms` call. Columns: `id`, `phone`, `subscriber_id` (nullable), `task_type`, `message_type`, `outcome` ∈ {sent, suppressed, dry_run, failed}, `suppress_reason` (nullable), `vendor_message_id` (nullable), `vendor` (`telnyx`), `campaign` (nullable), `variant_id` (nullable), `decision_id` (nullable), `body_preview` (first 160 chars), `created_at`. `MessageOutcome` stays as-is for Cora learning attribution. Agent path writes both.
- **Q11: Test strategy — Option B.** Wiring concerns (inbound webhook routing, end-to-end suppression) as scenario tests in `tests/scenarios/`. Pure logic (gate ordering, normalizer, opt-in sentinel) as unit tests in `tests/`.

### Phone normalization

- **Q7: Canonical format — Option A.** Strict E.164 `+1XXXXXXXXXX` (US-only for now, since the `_AREA_CODE_TZ` map is FL-only). Use the `phonenumbers` library (PyPI). Single `normalize_phone()` helper in `sms_compliance` (or a new `phone_utils` module — see Modules below). Every read and write of any `phone` column goes through it.
- **Q8: Backfill — Option A.** Three-step Alembic migration:
  1. Dry-run script (`scripts/audit/phone_normalize_collision_report.py`) reports rows that would normalize to the same value across `sms_opt_ins`, `sms_opt_outs`, `sms_dead_letters`, `message_outcomes`, `owners.phone_1`, `enriched_contacts` (and any future caller table). Hand-resolve collisions.
  2. Alembic data migration normalizes all phone columns in those tables (use SQL `UPDATE` with a Python-side normalize loop in batches of 1000).
  3. Re-assert unique constraints on `sms_opt_ins.phone` and `sms_opt_outs.phone`.

### Deferred / paperwork

- **Q9: Redis STOP cache — Option A. Defer.** Document in `CLAUDE.md` and update the `sms_compliance.can_send` docstring (which currently lies about "Redis in 2B-2"). Revisit when A2P approval lands and we have throughput data.
- **Q10: Twilio→Telnyx cleanup — code-only check.** No runtime Twilio left (verified: grep finds zero `from twilio` or `import twilio` in `src/`). Twilio appears only in docstrings, comments, one CHECK constraint that allows both `twilio` and `telnyx` for `ApiUsageLog.service`, and the default value `source="twilio_inbound"` on `SmsOptOut`. Leave doc/comment refs (out of scope per the answer). Keep the `ApiUsageLog` CHECK accepting both for historical rows. Change the `SmsOptOut.source` default to `"inbound_sms"` going forward, but keep `"twilio_inbound"` as a valid value for historical rows.

### Stripe (already correct)

No changes needed. Verified: `StripeWebhookEvent.event_id` is unique; `handle_webhook` inserts first then dispatches; stale-checkout guard skips events older than 24h; `PremiumPurchase.stripe_payment_intent_id` and `BundlePurchase.stripe_payment_intent_id` are unique; `WalletTransaction` dedup is on `(subscriber_id, stripe_charge_id)`. Replay tests cover every handler.

### Modules — new and modified

- **New: `src/services/phone_utils.py`** — thin module wrapping `phonenumbers`. Exposes `normalize(raw: str) -> str | None`. Returns `None` on unparseable input. Used everywhere a phone is read or written. Pure function; trivial to test in isolation. *Deep module candidate.*
- **New: `src/services/sms_send_log.py`** — owns `SmsSendLog` writes. Single function `log_send(...)` called from inside `sms_compliance.send_sms` after every outcome decision. Pure DB write; no side effects. *Deep module candidate.*
- **New: `src/services/opt_in_sentinel.py`** — owns the `opt_in_pending:{phone}` Redis key. Two functions: `mark_pending(phone)` (called by `send_opt_in_prompt`) and `consume_pending(phone) -> bool` (called by `handle_opt_in_reply`; atomic GETDEL). *Deep module candidate.*
- **New: `SmsSendLog` ORM model in `src/core/models.py`** plus Alembic migration `fa016_compliance_baseline.py` containing: SmsSendLog table, widened DLQ CHECK, phone backfill, `SmsOptOut.source` default change.
- **Modified: `src/services/sms_compliance.py`** — `send_sms` gains `message_type` parameter; gate ordering updated; calls `phone_utils.normalize`; calls `sms_send_log.log_send` on every exit; `handle_opt_in_reply` calls `opt_in_sentinel.consume_pending`; `send_opt_in_prompt` calls `opt_in_sentinel.mark_pending` then sends with `message_type="opt_in_prompt"`; `_normalize` replaced with delegate to `phone_utils.normalize`.
- **Modified: `src/api/main.py` `/webhooks/telnyx/inbound` (around line 2475-2567)** — routing order: signature verify → parse → STOP via `handle_inbound` → opt-in via `handle_opt_in_reply` (NEW) → product command via `sms_commands.parse` + `dispatch`.
- **Modified: `src/services/sms_commands.py` `_find_subscriber`** — implement via `SmsOptIn.phone == phone` → `Subscriber` join, replacing the always-None stub. `_handle_yes` keeps its existing pause/offer/fallback chain (unchanged) since the opt-in sentinel handles consent before `sms_commands` is even reached.
- **Modified: every caller of `sms_compliance.send_sms`** — explicit `message_type`. Inventory below.
- **Modified: `src/agents/tools/write_tools.py`** — pass `message_type` through to `sms_compliance.send_sms` so the agent's marketing/transactional classification reaches the gate.
- **Modified: `CLAUDE.md`** — update SMS line from "Twilio" to "Telnyx (replaced Twilio 2026-05-11)"; document Q9 deferral; add `phone_utils` to Tooling Rules.
- **Modified: `requirements.txt`** — add `phonenumbers`.

### Caller inventory for `message_type`

Every existing call to `sms_compliance.send_sms` must be tagged:

- `sms_compliance.send_opt_in_prompt` → `opt_in_prompt`
- `src/api/main.py` `telnyx_inbound` command-reply path → `transactional` (the reply to a product command)
- `src/services/sms_commands.*` reply paths (LOCK URL, BALANCE summary, TOPUP URL, etc.) → `transactional`
- `src/services/sample_leads_sms.py` → `marketing`
- `src/services/signup_engine.py` missed-call welcome SMS → `transactional` (account-event) **— flag for review; some carriers treat welcome as marketing**
- `src/services/referral_notifier.py` referral-success SMS → `transactional`
- `src/services/proof_moment.py` / `flash_scarcity.py` / `bundle_engine.py` upsell SMS paths → `marketing`
- `src/services/nws_webhook.py` storm activation SMS → `transactional` (alert, not promo) **— flag for review**
- `src/tasks/revenue_pulse.py` `_send_sms` (founder alerts) → `transactional`
- `src/tasks/cora_anomaly_check.py` ops pages → `transactional`
- `src/agents/tools/write_tools.py` (Cora graphs) → forward whatever the graph passed (default `marketing`)

(Use `Grep` for `sms_compliance.send_sms\(` and `compliance.send_sms\(` to confirm coverage at PR time.)

### Schema changes — Alembic migration `fa016_compliance_baseline`

`revision = "fa016_compliance_baseline"`, `down_revision = "fa015_api_telnyx"`.

Upgrade:
1. Drop and recreate the `check_dlq_reason` CHECK on `sms_dead_letters` to include `quiet_hours` and `no_opt_in`.
2. Create `sms_send_logs` table with columns listed in Q6 above. Indexes: `(phone)`, `(subscriber_id, created_at)`, `(outcome, created_at)`, `(vendor_message_id)`.
3. Data migration: normalize phone columns in `sms_opt_ins`, `sms_opt_outs`, `sms_dead_letters`, `message_outcomes`, `owners.phone_1`, `enriched_contacts.phone` (any column named `phone` or `phone_1`). Batched 1000 rows per commit. Skip rows that already match canonical form.
4. Alter `SmsOptOut.source` server default from `"twilio_inbound"` to `"inbound_sms"`. (Historical rows keep their value.)

Downgrade: reverse all four. Drop the new table. Restore old CHECK. (Phone re-denormalization is a no-op — we don't restore.)

### API contracts

- `sms_compliance.send_sms(to, body, db, *, message_type: Literal["marketing","transactional","opt_in_prompt"] = "marketing", subscriber_id=None, task_type=None, campaign=None, variant_id=None, decision_id=None) -> bool`
- `sms_compliance.handle_opt_in_reply(from_number, body, db) -> Optional[str]` — unchanged signature; behaviour now gated by sentinel.
- `sms_compliance.send_opt_in_prompt(phone, db, subscriber_id=None) -> bool` — now also marks Redis sentinel.
- `phone_utils.normalize(raw: str | None) -> str | None`
- `sms_send_log.log_send(*, db, phone, subscriber_id, task_type, message_type, outcome, suppress_reason=None, vendor_message_id=None, campaign=None, variant_id=None, decision_id=None, body_preview=None) -> int`
- `opt_in_sentinel.mark_pending(phone: str) -> None`
- `opt_in_sentinel.consume_pending(phone: str) -> bool` — atomic; True iff sentinel existed and was deleted.

### Detailed implementation plan — change list by file

1. `requirements.txt` — add `phonenumbers>=8.13`.
2. `src/services/phone_utils.py` — new, ~30 LOC. `normalize` wraps `phonenumbers.parse(raw, "US")` → `format_number(..., E164)`; returns None for unparseable / invalid.
3. `src/services/opt_in_sentinel.py` — new, ~25 LOC. Wraps `src.core.redis_client` (uses the existing `redis_available`, `rset`, `rget`, `rdelete` helpers). `mark_pending(phone)` → `rset(f"opt_in_pending:{phone}", "1", ttl_seconds=900)`. `consume_pending(phone)` → atomic GETDEL via Lua, fallback to `rget` + `rdelete` if Lua unavailable in fakeredis.
4. `src/services/sms_send_log.py` — new, ~40 LOC. Single `log_send` function. Best-effort: catches exceptions, logs warning, never raises.
5. `src/core/models.py` — add `SmsSendLog` model (around line 2040, near `SandboxOutbox`). Update `SmsDeadLetter` CHECK constraint string to include `quiet_hours`, `no_opt_in`. Update `SmsOptOut.source` default to `"inbound_sms"` and docstring.
6. `alembic/versions/fa016_compliance_baseline.py` — new migration described above.
7. `scripts/audit/phone_normalize_collision_report.py` — new. Run before migration on a prod DB snapshot. Prints collision groups per table. Exits non-zero if any collisions found.
8. `src/services/sms_compliance.py` — substantive rewrite of `send_sms` and `handle_opt_in_reply`:
   - Add `message_type` kwarg to `send_sms`. Validate in `{marketing, transactional, opt_in_prompt}`.
   - Replace `_normalize` body with delegation to `phone_utils.normalize`. Keep the function name so existing imports work.
   - Gate order: (1) opt-out via `can_send`, write DLQ + SmsSendLog with `suppress_reason="opt_out"`; (2) opt-in via `has_opted_in` IF `message_type == "marketing"`, write DLQ row with `reason="no_opt_in"` + SmsSendLog; (3) quiet hours, write DLQ with `reason="quiet_hours"` + SmsSendLog; (4) creds check; (5) dispatch.
   - Every exit point writes one SmsSendLog row with the appropriate `outcome` and `suppress_reason`.
   - `handle_opt_in_reply`: extract opt-in keyword (existing). If keyword found, call `opt_in_sentinel.consume_pending(from_number)`. If True → `record_opt_in`, return confirmation TwiML. If False → return None (fall through to product commands).
   - `send_opt_in_prompt`: call `opt_in_sentinel.mark_pending(phone)` BEFORE calling `send_sms(..., message_type="opt_in_prompt")`. If the underlying send returns False, optionally clear the sentinel (best-effort).
9. `src/api/main.py` lines ~2475–2567 — insert `handle_opt_in_reply` call between the existing STOP handler and `sms_commands.parse`:
   ```
   stop_reply = handle_inbound(from_number, body, db)
   if stop_reply: return Response(stop_reply, ...)
   opt_in_reply = handle_opt_in_reply(from_number, body, db)   # NEW
   if opt_in_reply: return Response(opt_in_reply, ...)         # NEW
   command = sms_commands.parse(body)
   ...
   ```
   Pass `message_type="transactional"` to the `send_sms` call for the command-reply path.
10. `src/services/sms_commands.py` — replace `_find_subscriber` body:
    ```
    row = db.execute(
        select(Subscriber).join(SmsOptIn, SmsOptIn.subscriber_id == Subscriber.id)
        .where(SmsOptIn.phone == phone_utils.normalize(phone))
        .order_by(SmsOptIn.opted_in_at.desc())
    ).scalars().first()
    return row
    ```
    Delete the Owner-lookup dead code.
11. `src/agents/tools/write_tools.py` — pass `message_type=message_type` through to `sms_compliance.send_sms` on the dispatch call (currently doesn't forward it).
12. Every other caller in the inventory list — add explicit `message_type=...`.
13. `CLAUDE.md` — update SMS line, add Q9 deferral note, add `phone_utils` to tooling rules.

### Rollout sequence

PR 1 (foundation, independently shippable):
- `phone_utils.py`, `opt_in_sentinel.py`, `sms_send_log.py`, `SmsSendLog` model, Alembic migration up to schema parts (skip the data backfill).
- Tests for the three new modules.

PR 2 (data migration):
- `scripts/audit/phone_normalize_collision_report.py`. Run on staging. Hand-resolve collisions.
- Alembic data backfill step. Run in maintenance window.

PR 3 (compliance gate enforcement, the meaty one):
- `sms_compliance.send_sms` rewrite with `message_type`, gate ordering, `sms_send_log` writes.
- `handle_opt_in_reply` + `send_opt_in_prompt` sentinel integration.
- `telnyx_inbound` webhook routing fix.
- `_find_subscriber` real lookup.
- Inventory pass: every caller tagged with `message_type`.
- Scenario + unit tests.

PR 4 (paperwork):
- `CLAUDE.md` updates, `SmsOptOut.source` default change, docstring sweep.

## Testing Decisions

### What makes a good test here

Test external behavior, not internals. For wiring concerns (inbound webhook routing, gate ordering, sentinel state machine) tests must hit the real entry point with a real envelope and assert on the observable side effects (DB rows, TwiML reply text, return value of `send_sms`). For pure logic (normalizer, sentinel get/set, log writer) unit tests with mocks are fine.

### Per-module

- **`phone_utils.normalize`** — unit tests: `+18135550100`, `8135550100`, `18135550100`, `(813) 555-0100`, `813.555.0100`, `+1 813 555 0100`, `8135550100x123`, invalid lengths, empty string, None, non-US numbers (returns None for now), unicode garbage. Round-trip test: `normalize(normalize(x)) == normalize(x)`.
- **`opt_in_sentinel`** — unit tests against fakeredis: mark→consume returns True and clears; consume without mark returns False; double-consume returns False on second call; TTL respected (use freezegun or fakeredis time control).
- **`sms_send_log.log_send`** — integration test against `fresh_db`: writes one row per call, all columns populated, exceptions in DB don't propagate.
- **`sms_compliance.send_sms` gate ordering** — unit tests with mocked DB and patched settings: opt-out short-circuits before opt-in; opt-in short-circuits before quiet hours; marketing-with-no-opt-in returns False and writes DLQ+SmsSendLog; transactional-with-no-opt-in proceeds; `opt_in_prompt` bypasses opt-in but still hits opt-out and quiet hours; quiet-hours writes DLQ row with reason=`quiet_hours` (regression test for the CHECK fix).
- **`handle_opt_in_reply`** — unit tests: sentinel present + YES → records + returns TwiML; sentinel absent + YES → returns None; non-opt-in keyword → returns None.
- **`sms_commands._find_subscriber`** — integration: returns subscriber for known opted-in phone; returns None for unknown phone; returns None for opted-out phone (no opt-in row exists).
- **Telnyx inbound webhook scenario** (`tests/scenarios/test_platform_webhooks.py`):
  - POST envelope with `text="STOP"` → opt-out recorded, TwiML returned.
  - POST envelope with `text="YES"` after `send_opt_in_prompt` fired → opt-in recorded, TwiML returned.
  - POST envelope with `text="YES"` with no sentinel → no opt-in row created, response empty (falls through; pause flow handles it).
  - POST envelope with `text="BALANCE"` from known subscriber → reply sent through `send_sms` with `message_type="transactional"` (assert SmsSendLog row).
  - POST envelope with `text="BALANCE"` from unknown phone → "Reply HELP" fallback.
- **End-to-end suppression scenario**:
  - Marketing send to opted-out number → False; one SmsSendLog with `outcome="suppressed"`, `suppress_reason="opt_out"`; one DLQ row.
  - Marketing send to number with no opt-in → False; DLQ row with `reason="no_opt_in"`; SmsSendLog row.
  - Marketing send during quiet hours → False; DLQ row with `reason="quiet_hours"` (this test currently CANNOT run because of the CHECK bug — proves we fixed it).
  - `opt_in_prompt` send to non-consented number → True (dispatch happens or dry-run); SmsSendLog with `outcome="sent"` (or `dry_run`).
- **Stripe replay regression** — existing `tests/test_stripe_replay.py` continues to pass unchanged.

### Prior art

- `tests/test_sms_compliance.py` — unit + integration pattern, mixes mocked DB with `fresh_db` fixture. Continue the same split.
- `tests/test_stripe_replay.py` — `_post` helper that drives a real `handle_webhook` with a stubbed Stripe SDK; same pattern works for Telnyx (`_post_telnyx` that signs a fake envelope and hits `/webhooks/telnyx/inbound` via TestClient).
- `tests/scenarios/test_platform_webhooks.py` — already has the seed_subscriber fixture and `dispatch`/`read_outbox` helpers. New scenarios live here.
- `tests/test_telnyx_signature.py` — signature stubbing pattern for webhook tests.

## Out of Scope

- **A2P 10DLC brand + campaign registration.** External dependency on Telnyx portal + client decisions (`docs/Client.md:30-32`). Track in `PENDING_TASKS.md`.
- **Redis STOP cache.** Deferred per Q9. Document only.
- **`check_dnc` cleanup.** It's dead code (no caller). Decide later whether to wire it into the marketing path or delete it. Not blocking launch.
- **Cron / scraper / CDS-engine changes.** None needed for compliance baseline.
- **Voice-channel compliance** (Synthflow). Same TCPA rules apply but voice has its own gating in `synthflow_service.py`; covered separately.
- **Twilio docstring / comment cleanup.** Per Q10 decision — no runtime Twilio left; comments stay. (Exception: the `SmsOptOut.source` default value is changed, since it's runtime-visible.)
- **International phone support.** US-only. `phone_utils.normalize` uses `"US"` as the default region.
- **CTIA / carrier-specific filtering rules.** Outside our control; we comply with TCPA primitives, A2P handles the rest.
- **Subscriber.phone column.** Decision Q5 makes it unnecessary; revisit only if we discover a query pattern that `SmsOptIn` can't serve.

## Vertical PR Phases

Each phase below ships one **observable behavior** end-to-end (schema + code + wiring + tests + caller updates as needed). Split is intentionally **not** by layer ("a migrations PR", "a tests PR") — that produces dead code on `main` and is hard to revert. Dependencies were verified against the current code: `sms_compliance.py` is the high-traffic shared file (touched by V2/V3/V4/V5), `models.py` is touched by V1/V3/V7, `api/main.py:2475-2567` only by V6. Migrations stack `fa016 → fa017 → fa018 → fa019` and must merge in order.

Twilio runtime audit (re-verified): zero `from twilio` / `import twilio` lines under `src/`; no `twilio` package in `requirements.txt` (only `telnyx>=2.1.0`, `PyNaCl` for Ed25519). Only doc/comment references remain — out of scope per Q10.

### V1 — Quiet-hours suppression actually persists (must-fix, foundational)

**Behavior shipped:** When `sms_compliance.send_sms` blocks an SMS for quiet hours, a `sms_dead_letters` row is written with `reason="quiet_hours"` instead of crashing the flush.

- Alembic `fa016_dlq_reason_widen`: drop and recreate `check_dlq_reason` to include `quiet_hours` and `no_opt_in` (the latter is added now so V4 doesn't need its own migration).
- `src/core/models.py`: update the `CheckConstraint` literal on `SmsDeadLetter` to match.
- Test: scenario test that patches `is_quiet_hours=True`, calls `send_sms`, asserts a DLQ row with `reason="quiet_hours"` exists.

**Depends on:** nothing. **Blocks:** V4 (V4 writes `no_opt_in` rows).
**Risk:** lowest. **Should-delay:** no — ship first.
**Shared-file collision:** `models.py` (line ~1734).

### V2 — Strict E.164 phone normalization on every read/write (forward-only)

**Behavior shipped:** Any new `SmsOptIn`/`SmsOptOut`/`SmsDeadLetter`/`MessageOutcome` row stores phone in `+1XXXXXXXXXX`. Lookups normalize before querying.

- Add `phonenumbers>=8.13` to `requirements.txt`.
- New module: `src/services/phone_utils.py` exposing `normalize(raw) -> str | None`.
- `src/services/sms_compliance.py`: replace `_normalize` body with delegate to `phone_utils.normalize`. (Function name preserved — no caller breakage.)
- Tests: unit on `phone_utils.normalize` (formats, garbage, idempotence); integration that opt-out via `handle_inbound` stores E.164 regardless of inbound format.

**Depends on:** nothing. **Blocks:** V2b (backfill), V6 (`_find_subscriber` join needs the normalizer).
**Existing rows in mixed format will continue to mismatch until V2b runs** — known temporary regression risk. Mitigation: until V2b lands, leave `can_send` queries permissive by also checking the digits-only form via OR clause (revert in V2b).
**Shared-file collision:** `sms_compliance.py`. Land before V3/V4/V5 to minimize rebase pain.

### V2b — Phone normalization data backfill

**Behavior shipped:** Every existing phone column matches the canonical form V2 writes.

- `scripts/audit/phone_normalize_collision_report.py`: dry-run report across `sms_opt_ins`, `sms_opt_outs`, `sms_dead_letters`, `message_outcomes`, `owners.phone_1`, `enriched_contacts`. Exits non-zero on collision.
- Alembic `fa018_phone_normalize_backfill`: in-place UPDATE in 1000-row batches.
- Remove the V2 mitigation OR-clause in `can_send`.
- Test: integration that pre-seeded mixed-format rows are all canonical after the migration.

**Depends on:** V2. **Blocks:** nothing strictly — V6 will work without V2b but with degraded match rate.
**Should-delay:** only until a maintenance window is available + collision audit hand-resolved.
**Risk:** medium — touches production data. Run on a prod snapshot first.

### V3 — Per-send audit row in `sms_send_logs`

**Behavior shipped:** Every call to `sms_compliance.send_sms` writes exactly one `sms_send_logs` row recording `outcome ∈ {sent, suppressed, dry_run, failed}` and `suppress_reason`. Ops can answer "did we attempt anything to this number today" with one query.

- Alembic `fa017_sms_send_logs`: create table with indexes on `(phone)`, `(subscriber_id, created_at)`, `(outcome, created_at)`, `(vendor_message_id)`.
- New ORM `SmsSendLog` model.
- New module `src/services/sms_send_log.py` exposing `log_send(...)`.
- `sms_compliance.send_sms`: at every exit point, call `log_send` with the current outcome. (No suppression-logic change yet; behavior identical except for the new audit row.)
- Tests: integration that `send_sms` writes one log row per call across each outcome.

**Depends on:** nothing (writes work today; `suppress_reason="quiet_hours"` will be set but won't crash since it's a free string in this table). **Blocks:** V4 audit assertions ("marketing-blocked produced a `suppressed/no_opt_in` row").
**Shared-file collision:** `sms_compliance.py` + `models.py`. Sequence after V2 to avoid stomping on `_normalize` edits.
**Risk:** low — additive only.

### V4 — Default-deny marketing (`message_type` gate + opt-in enforcement)

**Behavior shipped:** `sms_compliance.send_sms(..., message_type="marketing")` to a number with no `SmsOptIn` returns False and writes one DLQ row (`reason="no_opt_in"`) and one `SmsSendLog` row (`outcome="suppressed"`). `message_type="transactional"` skips the opt-in gate. `message_type="opt_in_prompt"` bypasses opt-in but still hits opt-out and quiet hours.

- `sms_compliance.send_sms`: add validated kwarg `message_type: Literal["marketing","transactional","opt_in_prompt"] = "marketing"`. Insert opt-in gate between opt-out and quiet hours (final order: opt-out → opt-in → quiet hours → creds → dispatch). Write DLQ + SmsSendLog for each suppression branch.
- **Caller inventory pass** (the slow part of this PR). `Grep` for `sms_compliance.send_sms(`. Tag every existing caller in the file list from "Caller inventory for `message_type`" section: `sample_leads_sms` (marketing), `referral_notifier` (transactional), `proof_moment`/`flash_scarcity`/`bundle_engine` (marketing), `nws_webhook` (transactional, flag), `signup_engine` welcome (transactional, flag), `revenue_pulse._send_sms` (transactional), `cora_anomaly_check` (transactional), `sms_commands` reply path in `main.py:2565` (transactional), `send_opt_in_prompt` (`opt_in_prompt`).
- `src/agents/tools/write_tools.py`: forward `message_type` from agent state into `sms_compliance.send_sms` (currently it doesn't pass it through).
- Unit tests for each gate-order branch + each `message_type` value.

**Depends on:** V1 (`no_opt_in` CHECK), V3 (SmsSendLog audit assertions). **Blocks:** V5 (sentinel uses `message_type="opt_in_prompt"`), V6 (webhook product-command reply uses `message_type="transactional"`).
**Shared-file collision:** `sms_compliance.py` + every caller listed above. **Highest blast radius of any PR in this plan.**
**Should-delay:** no, but cut the PR after the caller inventory is verified — splitting callers across multiple PRs leaves `main` in a half-default-deny state where some paths silently get blocked.

### V5 — Opt-in sentinel state machine

**Behavior shipped:** A YES inbound creates an `SmsOptIn` row only when `send_opt_in_prompt` set the `opt_in_pending:{phone}` Redis key in the last 15 minutes. Without the sentinel, `handle_opt_in_reply` returns None and the inbound falls through (preserving the PAUSE-confirm YES flow).

- New module `src/services/opt_in_sentinel.py` (`mark_pending`, `consume_pending` using `src.core.redis_client`).
- `sms_compliance.send_opt_in_prompt`: call `opt_in_sentinel.mark_pending(phone)` before dispatching with `message_type="opt_in_prompt"`.
- `sms_compliance.handle_opt_in_reply`: gate the `record_opt_in` + confirmation reply on `consume_pending`. Return None when sentinel absent.
- Unit tests against `fakeredis`: mark→consume returns True; consume without mark returns False; TTL respected.
- Scenario test: `send_opt_in_prompt` followed by a webhook YES creates one `SmsOptIn`; YES without prompt creates zero rows.

**Depends on:** V4 (`message_type="opt_in_prompt"` value). **Blocks:** V6 (webhook needs sentinel-gated `handle_opt_in_reply`).
**Shared-file collision:** `sms_compliance.py` (rebase on V4).
**Risk:** medium — Redis dependency adds a failure mode. Mitigation: `redis_available()` guard already exists in `sms_commands._handle_pause`; reuse the same pattern so a Redis outage degrades to "YES does nothing" rather than "everything breaks".

### V6 — Inbound webhook routing + real subscriber lookup

**Behavior shipped:** POSTing a Telnyx-shaped envelope to `/webhooks/telnyx/inbound` correctly handles STOP → opt-in (sentinel-gated) → product command. Product commands resolve the sender via `SmsOptIn → Subscriber` and reply with real URLs/data instead of always returning "Reply HELP."

- `src/api/main.py:2475-2567` (`telnyx_inbound`): insert `handle_opt_in_reply` between the STOP handler and `sms_commands.parse`. Pass `message_type="transactional"` to the reply-send call.
- `src/services/sms_commands.py:65-76` (`_find_subscriber`): replace stub with `SmsOptIn` join, using `phone_utils.normalize` on the input.
- Scenario tests in `tests/scenarios/test_platform_webhooks.py` covering: STOP, YES-with-sentinel, YES-without-sentinel, BALANCE-from-known-subscriber, BALANCE-from-unknown.

**Depends on:** V2 (normalizer), V4 (`message_type` arg), V5 (sentinel-gated reply). **Blocks:** nothing.
**Shared-file collision:** `api/main.py` is huge (3900 lines) but `telnyx_inbound` is localized; collision risk is low. `sms_commands.py` collision risk also low — only `_find_subscriber` is touched.

### V7 — Paperwork (defaults, docs, deferral notes)

**Behavior shipped:** Future engineers reading the code understand the deferred Redis decision and the Telnyx vendor switch.

- `src/core/models.py`: `SmsOptOut.source` server default → `"inbound_sms"`. Existing rows untouched.
- `src/services/sms_compliance.py`: update `can_send` docstring (currently says "Redis in 2B-2") and the module-level docstring.
- `CLAUDE.md`: SMS line Twilio → Telnyx, document Q9 Redis deferral, add `phone_utils` to Tooling Rules.
- Alembic `fa019_sms_opt_out_source_default` (server-default change only).

**Depends on:** nothing. **Blocks:** nothing.
**Should-delay:** can ship anytime; bundle with V1 if it lands first, or as its own one-line PR.

### Parallelization matrix

| Phase | Can start when | Can run in parallel with | Sequential rebase risk |
|---|---|---|---|
| V1 | day 0 | V2, V3, V7 | none |
| V2 | day 0 | V1, V3 (loose), V7 | low — different file regions |
| V2b | V2 merged + collision audit clean | V4/V5/V6 dev branches | low |
| V3 | day 0 | V1, V2, V7 | medium — both V2 and V3 touch `sms_compliance.send_sms`; land V2 first |
| V4 | V1 + V3 merged | none (high blast radius) | **high** — full rewrite of `send_sms`; freeze other PRs to `sms_compliance.py` |
| V5 | V4 merged | none | medium — touches `sms_compliance.py` |
| V6 | V2 + V4 + V5 merged | V2b (optional) | low — `api/main.py` + `sms_commands.py` |
| V7 | day 0 | everything | none |

**Day-0 launches (in parallel):** V1, V2, V3, V7. Different files (`models.py` is touched by V1 and V3 but in non-overlapping regions: V1 = `SmsDeadLetter` CHECK at ~1734, V3 = new `SmsSendLog` class appended near `SandboxOutbox` at ~2040). V2 is in `sms_compliance.py` but only inside `_normalize` body.

**Serial after day 0:** V4 → V5 → V6 on the critical path. V2b interleaved when ops window allows.

### Risky shared files and hidden dependencies

- **`src/services/sms_compliance.py`** is the highest-risk file: V2 (normalize delegate), V3 (log_send wiring), V4 (gate rewrite + message_type), V5 (sentinel calls). Sequence strictly. The `_capture_sandbox_attempt` helper writes `SandboxOutbox` at every exit today; V3 must add `log_send` calls at the same exit points without duplicating the loop — pulling both into one helper inside V3 would reduce V4's diff.
- **`src/core/models.py`** has three migrations queued (`fa016` V1, `fa017` V3, `fa018` V2b, `fa019` V7). Migrations must merge in numeric order, but Alembic's linear history will reject out-of-order merges — coordinate the merge train.
- **`src/agents/tools/write_tools.py`** `send_sms` tool is the only agent caller; V4 must not regress the agent path (current Cora flow passes `decision_id`, `variant_id`, `campaign` — must continue to). The agent's `compliance_check` tool will become redundant with V4's in-`send_sms` gate but should stay (it's a pre-flight check that prevents wasted Claude tokens before composing copy). Document as "belt and suspenders."
- **`src/services/sms_commands.py`** `_handle_yes` currently treats YES as a PAUSE/offer confirm. V5 sentinel routing assumes YES with no sentinel falls through to here. If `_handle_pause_yes` is reached with no `pause_pending` Redis key, it returns "No pending pause request" — that's a leak of internal state to anyone who texts YES. Cosmetic, but V6 scenario tests should snapshot the message to make sure it's not too leaky.
- **`src/services/telnyx_sms.py`** (the actual sender) is not touched by any of these PRs. Verified runtime-only on Telnyx — no V-phase needs to touch it.
- **`config/settings.py`** `telnyx_messaging_profile_id` is an A2P placeholder; out of scope.
- **`src/api/sandbox_router.py`** simulates inbound via the same handlers; V6 webhook routing change automatically applies to the sandbox harness. Existing sandbox tests should still pass without modification.
- **Redis dependency for V5** — `src/core/redis_client.py` already provides `redis_available()`, `rget`, `rset`, `rdelete`. `fakeredis` is in requirements (line 48). No new infra dependency.

### Suggested ship sequence

1. **Week 1, day 1:** V1 + V7 (each <50 LOC diff, no behavior change risk). Unblocks `quiet_hours` correctness and clears doc drift.
2. **Week 1, day 2-3:** V2 + V3 (parallel, different file regions). Ship V2 just before V3 to keep `sms_compliance.py` merge clean.
3. **Week 1, day 4:** V2b collision audit dry-run on staging. Resolve any collisions.
4. **Week 2:** V4 — single dedicated PR with full caller inventory. Freeze other compliance work during review.
5. **Week 2 late:** V5 — sentinel state machine.
6. **Week 3:** V6 — webhook routing + lookup. The flagship "compliance works end-to-end" PR.
7. **Week 3:** V2b backfill in maintenance window.

## Further Notes

- `_AREA_CODE_TZ` (FL area codes) is hardcoded. When the platform expands beyond FL, this needs a real lookup table or `phonenumbers.geocoder`. Out of scope here but flag in the Stage 6 expansion plan.
- The `_capture_sandbox_attempt` function in `sms_compliance.py` already writes a comprehensive `SandboxOutbox` row when `TELNYX_SANDBOX=true`. `SmsSendLog` is the production analogue; in sandbox, both rows get written. This is intentional — sandbox keeps developer-facing context, production gets the audit row.
- The Cora agent path's `MessageOutcome` writes stay. Cora needs them for learning-card attribution. The new `SmsSendLog` is for ops and compliance.
- Phone normalization migration: if collisions exist (e.g. two `SmsOptIn` rows for the same number in different formats), prefer the most recent `opted_in_at` and delete the older row. Document the resolution rule in the migration's docstring.
- Sentinel TTL of 15 minutes was chosen to match a reasonable "I just got a prompt and want to reply" window. Adjust if data shows the median YES-after-prompt latency is different.
- The change to `SmsOptOut.source` default does not invalidate the existing `source IN ('manual', 'import')` DNC filter in `check_dnc` — neither default value is in that filter set.
- One verification step before merge: confirm zero `from twilio` / `import twilio` lines exist in `src/` (currently true). If anyone reintroduced the Twilio SDK during PR review, block.
