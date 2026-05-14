# Compliance Baseline — Grilling Q&A

Branches walked one at a time. Each question has options + a recommendation. User answer captured per question as we go.

---

## Q1. How to disambiguate inbound `YES` (TCPA opt-in keyword vs PAUSE/offer confirm command)?

**Context.** `_OPT_IN_KEYWORDS = {yes, start, join, subscribe, unstop}` (sms_compliance.py:331). `sms_commands._handle_yes` (sms_commands.py:129) also uses YES for pending PAUSE confirmation and `lock_close` checkout offers via Redis keys `pause_pending:{sub.id}` and `fa:pending_offer:{sub.id}`. `handle_opt_in_reply` (sms_compliance.py:403) does NOT check `has_opted_in` first — it always records (idempotent) and always returns "you're confirmed". Wiring it before `sms_commands` naively breaks the PAUSE-confirm flow.

| Opt | Rule | Pros | Cons |
|---|---|---|---|
| A | Webhook checks Redis for `pause_pending` / `pending_offer` first → product command, else → opt-in handler. | Cheapest patch, no new state. | Needs `_find_subscriber` working first (circular with item 4). Strangers texting YES still record consent. Bad TCPA hygiene. |
| **B (rec.)** | `send_opt_in_prompt` sets Redis sentinel `opt_in_pending:{phone}` (TTL ~15 min). `handle_opt_in_reply` only records when sentinel exists; otherwise returns None and falls through to product commands. | True double-opt-in semantics — consent only counts when prompted. Clean state machine. Independent of `_find_subscriber`. | Small added code in `send_opt_in_prompt`. |
| C | Always record opt-in on YES (idempotent), but never short-circuit; continue to product commands. | Simplest. | Same TCPA hygiene risk as A. Loses explicit confirmation reply. |

**Recommendation:** **B.** Matches CTIA/TCPA double-opt-in semantics. Unblocks items 2 and 4 independently.

**Answer:** _pending_

---

## Q2. How to fix the `sms_dead_letters.reason` CHECK constraint vs `quiet_hours` insert?

**Context.** `sms_compliance.send_sms:211` writes `reason="quiet_hours"`. `sms_dead_letters.reason` CHECK allows only `{opt_out, delivery_failed, error, unresolvable}`. Insert raises → DLQ row lost.

| Opt | Fix | Pros | Cons |
|---|---|---|---|
| A | Widen CHECK constraint to include `quiet_hours`. Alembic migration. | Honest about the reason; ops can filter `WHERE reason='quiet_hours'`. | Migration overhead. |
| B | Map quiet-hours in the service layer to `reason='error'` with `payload={"suppression": "quiet_hours", ...}`. | No migration. | Conflates real errors with expected suppressions — ops dashboards become noisy. |
| C | Don't DLQ quiet-hours at all. It's expected suppression, not failure. Log to `MessageOutcome`/`SandboxOutbox` only. | DLQ stays semantically "things needing review." | Loses queryable record outside sandbox until per-send audit (Q6) lands. |

**Recommendation:** **A.** It's the cheapest correct fix. DLQ is the right place to count suppressions; ops filter by reason. Add `quiet_hours` (and consider adding `no_opt_in` ahead of Q3 enforcement).

**Answer:** _pending_

---

## Q3. Where does the marketing-vs-transactional classification live for the `has_opted_in()` gate?

**Context.** `sms_compliance.send_sms` takes `task_type` (free-string, e.g. `"tcpa_opt_in_prompt"`). `compose_and_send` subgraph passes a separate `message_type` ∈ `{marketing, transactional}` to `compliance_check`. Direct callers (`send_opt_in_prompt`, `sample_leads_sms`, `sms_commands` reply, `_send_welcome_email`'s SMS twin, NWS storm notify) all use `send_sms` directly with no `message_type`.

| Opt | Where the classification lives | Pros | Cons |
|---|---|---|---|
| A | Add `message_type` param to `sms_compliance.send_sms` (default `"marketing"` = strict). Every existing caller must pass `"transactional"` where appropriate or get blocked. | Default-deny is the safe default for TCPA. Forces every callsite to declare intent. | Touches many call sites; risk of accidentally blocking real notifications on first deploy. Audit every caller. |
| B | Allowlist of transactional `task_type` strings inside `send_sms` (e.g. `{tcpa_opt_in_prompt, payment_receipt, pause_confirm, ...}`). Anything else = marketing. | Zero callsite churn. | Hidden coupling — adding a transactional task means editing the allowlist; easy to forget. Free-string `task_type` is fragile. |
| C | Hybrid: new `message_type` param, but default `"marketing"`, and an internal small allowlist that auto-classifies known transactional `task_type` values when `message_type` not passed. | Safe default + zero immediate churn. | Two ways to do the same thing — pick one over time. |

**Recommendation:** **A** with an explicit one-shot migration: grep every `sms_compliance.send_sms(` call, classify, add the arg. Default `"marketing"`. Forces clarity; we want this loud, not implicit.

**Answer:** _pending_

---

## Q4. How does `send_opt_in_prompt` bypass its own `has_opted_in` gate? (Chicken-and-egg)

**Context.** Once Q3 lands, `send_sms` blocks marketing to numbers without `has_opted_in`. But the opt-in PROMPT itself is sent to numbers that by definition haven't opted in.

| Opt | Mechanism | Pros | Cons |
|---|---|---|---|
| A | `send_opt_in_prompt` passes `message_type="transactional"`. | Reuses the existing escape hatch. | Stretches the definition of "transactional" — the prompt is solicitation, not a receipt. Carriers/CTIA may disagree. |
| B | New `message_type="opt_in_prompt"` value, only valid for this one path; bypasses opt-in check but still runs opt-out + quiet-hours + DNC. | Honest classification. Auditable. | One extra enum value. |
| C | `send_opt_in_prompt` calls Telnyx directly, bypassing `send_sms`. | Cleanest separation. | Duplicates compliance plumbing (opt-out, quiet hours, DLQ). Drift risk. |

**Recommendation:** **B.** Distinct semantic value; easy to count/audit; still runs the other gates. Add to the `message_type` enum and to any new `MessageOutcome.message_type` if needed.

**Answer:** _pending_

---

## Q5. Fix `_find_subscriber` — where does the phone↔subscriber mapping live?

**Context.** `sms_commands._find_subscriber:65` is a stub returning None. Comment says "will be wired when subscriber.phone field is added in 2B-2." But `SmsOptIn` already carries `phone` + `subscriber_id` and is enforced unique on phone. `Owner.phone_1` exists for property owners (not subscribers).

| Opt | Source of truth | Pros | Cons |
|---|---|---|---|
| A | Add `Subscriber.phone` column (1:1). Backfill from latest `SmsOptIn`. Migrate forward. | Direct, indexable. Matches code comment. | New migration; duplicated state with `SmsOptIn.phone`; must keep in sync on opt-in/opt-out. |
| **B (rec.)** | Use existing `SmsOptIn` row: `SELECT subscriber_id FROM sms_opt_ins WHERE phone=:p ORDER BY opted_in_at DESC LIMIT 1`. | Zero migration. Single source of truth. Already used by `write_tools.send_sms` and `compliance_check`. | Subscribers without an opt-in row can't be looked up — but they also can't legally receive marketing, so this is correct by construction. |
| C | Use `Owner.phone_1` waterfall (already started in the stub). | Catches more numbers. | Owner ≠ subscriber. Wrong join semantics. |

**Recommendation:** **B.** Reuse `SmsOptIn`. The "no opt-in row" case correctly returns None → "Reply HELP" — which is the right answer for unknown senders anyway.

**Answer:** _pending_

---

## Q6. Per-send audit logging — where does every `send_sms` call write its row?

**Context.** Today: Cora agent path writes `MessageOutcome` (write_tools.py:151). Sandbox flag writes `SandboxOutbox`. Suppressed sends write `SmsDeadLetter`. Successful direct-caller sends write nothing.

| Opt | Strategy | Pros | Cons |
|---|---|---|---|
| A | Write `MessageOutcome` inside `sms_compliance.send_sms` for every outcome (sent / suppressed / dry-run / failed). Add a `status` column. | Single source of truth for sends. Subsumes DLQ for suppression. | Schema change on `MessageOutcome` (add status). Bigger table. Possible double-write where agent path also writes. |
| **B (rec.)** | Keep `MessageOutcome` agent-only (for learning attribution). `sms_compliance.send_sms` writes a new lightweight `SmsSendLog` row for every outcome. `SmsDeadLetter` stays for review-needed cases only (delivery_failed, error, unresolvable). | Clean separation of concerns. Doesn't perturb agent learning. Cheap. | New table + migration. |
| C | Repurpose `webhook_events`-style audit log: add an "outbound" direction and log every send there. | Reuses existing table. | Conflates webhook receipts with outbound; index/query patterns mismatch. |

**Recommendation:** **B.** Keep `MessageOutcome` semantically "for learning." Add `SmsSendLog` (id, phone, subscriber_id?, task_type, message_type, outcome ∈ {sent,suppressed,dry_run,failed}, suppress_reason, vendor_message_id, created_at). Agent path writes both `MessageOutcome` and `SmsSendLog`; everyone else writes only `SmsSendLog`.

**Answer:** _pending_

---

## Q7. Phone normalization — what's the canonical format?

**Context.** Today `_normalize` is whitespace-strip only. Mixed `+18135550100`, `18135550100`, `8135550100` storage will split opt-out keys. US-only for now.

| Opt | Canonical | Pros | Cons |
|---|---|---|---|
| A | Strict E.164 `+1XXXXXXXXXX` enforced everywhere; reject anything that can't normalize. | Standard. International-ready. Telnyx already uses it. | Need `phonenumbers` library or careful homegrown logic. |
| B | Digits-only `1XXXXXXXXXX` (11-digit) internally; format on egress. | Slightly simpler. | Diverges from Telnyx/Stripe E.164 conventions. |
| C | Defer normalization; add a DB function/index on a normalized expression. | No code churn. | Postgres-specific; harder to test. |

**Recommendation:** **A** using the `phonenumbers` library (it's the de-facto standard, handles country code and validity). One normalize helper in `sms_compliance` used by every read+write path.

**Answer:** _pending_

---

## Q8. Phone-normalization rollout — backfill vs forward-only?

**Context.** Existing rows in `sms_opt_outs`, `sms_opt_ins`, `sms_dead_letters`, `message_outcomes` may have mixed formats. Once Q7 lands and reads normalize, a number stored as `8135550100` will miss an opt-out stored as `+18135550100`.

| Opt | Strategy | Pros | Cons |
|---|---|---|---|
| A | Alembic data migration: normalize all existing phone columns once. Then unique-constraint reassertion. | Clean state going forward. | Migration risk on prod data; need to handle unique-constraint collisions (two rows that normalize to the same value). |
| B | Forward-only: normalize-on-write from now. Add a one-off script in `scripts/` to backfill when convenient. | Lower migration risk. | Lookups still split until backfill runs. |
| C | Read-side defense: every lookup tries both raw and normalized values. | Zero migration risk. | Slow, ugly, permanent tech debt. |

**Recommendation:** **A**, with a dry-run report first (count collisions per table). If collisions are zero or small, run the migration. If large, fall back to B + targeted resolution.

**Answer:** _pending_

---

## Q9. Redis DLQ — implement now or defer?

**Context.** Phase 2B plan said "Redis STOP cache + DLQ." Current implementation is Postgres only (functional). Hot-path opt-out check is one PG SELECT per outbound — not measured but unlikely to be a bottleneck pre-launch.

| Opt | Decision | Pros | Cons |
|---|---|---|---|
| A | Defer until measured. Document the deviation. | No new infra dependency before launch. | Plan/code drift; doc burden. |
| B | Add Redis read-through cache for `sms_opt_outs.phone` (TTL forever, invalidated on opt-in). DLQ stays in PG. | Cheap perf win at scale; minimal code. | Adds a cache-coherence concern (stale opt-outs = TCPA risk if Redis miss). |
| C | Full Redis-backed STOP + DLQ as originally planned. | Matches plan. | Big rebuild; no measured need. |

**Recommendation:** **A**. Defer until you have throughput numbers that justify it. Document in CLAUDE.md and update `sms_compliance.py` docstring (it still says "Redis in 2B-2"). Revisit once A2P is approved and real volume hits.

**Answer:** _pending_

---

## Q10. Twilio→Telnyx doc drift — bundle into this PR or separate?

**Context.** CLAUDE.md still names Twilio. Docstrings in `sms_compliance.py`, `models.py`, `stripe_webhooks.py` reference Twilio. `webhook_log.py` enum may still list `twilio_inbound` as a source.

| Opt | Bundle | Pros | Cons |
|---|---|---|---|
| A | Bundle into the compliance PR. Find/replace in same change. | One sweep. Less drift while you're already in here. | Larger diff. |
| B | Separate doc-only PR after compliance code lands. | Smaller compliance PR. | Drift persists until done; two reviews. |

**Recommendation:** **A**. You're already touching `sms_compliance.py` and `models.py`; sweep docstrings in the same PR. Defer CLAUDE.md only if its rewrite is bigger than `s/Twilio/Telnyx/g`.

**Answer:** _pending_

---

## Q11. Test strategy — per-feature units vs end-to-end scenario?

**Context.** Plan lists 6 missing tests (quiet-hours DLQ, YES via webhook, marketing-blocked-no-opt-in, transactional-allowed, subscriber lookup, audit logging).

| Opt | Strategy | Pros | Cons |
|---|---|---|---|
| A | All six as unit tests in `test_sms_compliance.py`. | Fast, deterministic. | Easy to mock around the actual webhook routing — won't catch wiring bugs (which is exactly the YES bug we're fixing). |
| **B (rec.)** | YES-via-webhook + marketing-blocked as scenario tests in `tests/scenarios/test_platform_webhooks.py`; the rest as units. | Scenario test catches the wiring failure mode (the bug class that caused this audit). | A bit slower. |
| C | Single end-to-end test covering all 6. | Fewest test files. | Fails opaquely; hard to localize regressions. |

**Recommendation:** **B.** Wiring bugs need wiring tests.

**Answer:** _pending_

---

## Q12. Where exactly does the `has_opted_in` gate run inside `send_sms`?

**Context.** `sms_compliance.send_sms` currently checks: (1) `can_send` opt-out, (2) `is_quiet_hours`, (3) creds, (4) dispatch. Where does `has_opted_in` slot in?

| Opt | Position | Pros | Cons |
|---|---|---|---|
| A | Before `can_send` (first gate). | Cheapest fail-fast for marketing. | Doesn't matter much — both are PG SELECTs. |
| **B (rec.)** | After `can_send`, before `is_quiet_hours`. | Logical ordering: hard suppression (opt-out) > consent (opt-in) > timing (quiet hours). Suppressed reason hierarchy stays readable. | None significant. |
| C | After `is_quiet_hours`. | Lets quiet-hours suppress first. | Wastes a DB read when the call is going to fail consent anyway. |

**Recommendation:** **B.** Order = opt-out → opt-in → quiet hours → creds → dispatch. Each gate writes its own DLQ row with distinct `reason` (assuming Q2-A widens the CHECK).

**Answer:** _pending_

---

## Cross-cutting dependencies

- Q3 (default-deny marketing) depends on Q4 (opt-in prompt bypass).
- Q5 (subscriber lookup) depends on nothing else here — can ship first.
- Q1 (YES routing) depends on nothing — can ship first; independent of Q5 if option B chosen.
- Q2 (DLQ CHECK widen) is independent — ship first; unblocks the quiet-hours DLQ row, which Q12 enforcement order assumes.
- Q6 (`SmsSendLog`) depends on Q2 + Q3 being landed so the schema captures the right `reason` set.
- Q7 → Q8 (normalize then backfill) — one PR, two steps.
- Q9 (Redis defer) and Q10 (doc sweep) are paperwork; bundle.

Suggested ship order:
1. Q2, Q5, Q1, Q10 (independent, low-risk)
2. Q4, Q3, Q12 (the opt-in enforcement set — must ship together)
3. Q7, Q8 (normalization + backfill)
4. Q6 (audit log)
5. Q11 tests landing alongside each PR
6. Q9 documented, deferred

**Answer for Q1:** _pending_ — your turn.
