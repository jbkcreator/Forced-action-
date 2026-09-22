# WP-1 / WP-2 / WP-5B — Real End-to-End Evidence

Run date: 2026-09-22, against the live server (postgres `distress_db`, redis, real
`FA_MAX_SLACK_BOT_TOKEN` posting into the real FA Max Slack channels used as this
box's test destinations). No mocks. All synthetic data used `zztest`/`ZZTEST`/`e2e_test`
markers and has been deleted after evidence capture (see Cleanup section). No real
borrower/partner/contact record was read, modified, or messaged.

Two evidence sources:
1. `scripts/e2e_fa_max_test.py` — an existing script already in the repo covering the
   bulk of WP-1/WP-2/WP-5B. Run as-is; **26/26 checks passed**.
2. Additional scripts written for this run to cover the three Done-When criteria the
   existing script does not exercise: WP-1 crash/resume, WP-2 suppressed-contact
   no-bypass, and WP-2 "full operational day" (enqueue → real Slack card → real human
   decision → durable audit).

---

## WP-1 — Durable State Engine & Event Spine

### Done When (a): a worker killed mid-execution can be resumed by another instance without losing the workflow

**PASS — two independent tests, both at DB-transaction level and at real OS-process level.**

**Test 1 — in-process simulated crash (open a real `transition()` call, then drop the
connection without committing):**
```
worker A in-memory result before crash: TransitionResult(outcome=succeeded, current_state=enriched, event_id=01a0c7b9-8a4b-...)
state immediately after simulated crash (must be untouched, still 'identified'): {'lifecycle_state': 'identified', 'state_version': 0, ...}
event rows for the crashed attempt's idempotency_key (must be 0): 0
worker B resume result: TransitionResult(outcome=succeeded, current_state=enriched, event_id=01a0c7b9-8a50-...)
final state: {'lifecycle_state': 'enriched', 'state_version': 1, ...}
full ordered history (1 events):
  seq=191 identified->enriched actor=system:worker_B_resumed occurred_at=2026-09-22 06:07:00.686743+00:00
```
Worker A's transition *appeared* to succeed in memory (the `TransitionResult` object),
but because the transaction was never committed, Postgres rolled back the advisory
lock, the state row, and the event row together — zero partial state, zero orphan
event, exactly as `state_engine.py`'s docstring claims. Worker B then re-ran the
identical transition against the real durable state and completed it exactly once.

**Test 2 — real OS-level `kill -9` on a work-queue worker process:**
```
$ python worker_proc.py zztest_crash_q_ec619159 worker-A &
CLAIMED: {'work_item_id': '01a0c7be-0ae3-...', 'status': 'claimed', 'worker_id': 'worker-A', 'lease_expires_at': ...}
$ kill -9 <pid>
Killed
$ ps -p <pid>          # confirmed: process is dead

# 16s later (past the 15s lease):
before reclaim: ('claimed', 'worker-A', <expired lease>)
reclaimed count: 1
worker B claimed: {'work_item_id': '01a0c7be-...', 'attempt_count': 3, 'worker_id': 'worker-B-resumed', ...}
worker B completed cleanly: True
final row: ('done', 'worker-B-resumed', attempt_count=3, done_at=2026-09-22 06:12:30...)
```
A real Python process was started, claimed a real `fa_max_work_queue` row, and was
`kill -9`'d while "holding" it. The item correctly stayed `claimed` until its lease
expired, `reclaim_expired_work_items()` returned it to `available`, and a second,
independent process claimed and completed it. No item was lost or double-processed.

**Verdict: PASS.**

### Done When (b): the complete ordered history of a borrower can be queried

**PASS.**
```
WP-1.5 event log entry: from=identified to=enriched actor=system:e2e_test ts=2026-09-22 06:03:48.694353+00:00
```
`get_person_history()` / `get_borrower_timeline()` were exercised and return
cursor-paginated, `seq`-ordered (not `occurred_at`-ordered — see code comment on why
that distinction matters under lock contention) history merging state transitions,
interactions, and property associations. Confirmed against the real DB.

### Other WP-1 checks (all PASS, from `scripts/e2e_fa_max_test.py`)
- Person + entity_registry creation, `get_person_state`, chained transition, post-transition state read.
- `write_interaction` + **DB-enforced immutability trigger** — a direct `UPDATE` against
  `fa_max_interactions` outside `transition()`'s session-local GUC genuinely raises a
  Postgres exception (`trg_fa_max_interactions_immutable`). Verified this trigger is real
  by hitting it accidentally during test cleanup (see Notes below).
- Durable work queue: `enqueue_work_item` → `claim_next_work_item` (FOR UPDATE SKIP LOCKED)
  → second claim correctly returns `None` when queue is empty → `complete_work_item`.

### Finding — not a Done-When failure, but worth fixing
`ensure_entity_registry()` is **never called anywhere in production code** (confirmed
by repo-wide grep). The real person-creation path (`src/services/selfserve_sessions.py:
resolve_or_create_person`) inserts directly into `fa_max_persons` and never touches
`fa_max_entity_registry`. `state_engine.transition()`'s `_derive_person_id()` for
`entity_type='person'` requires the registry's `native_id` column to equal the
person's own UUID — i.e., the registry is usable today, but only if a caller follows a
non-obvious convention (`native_id` must be the target `person_id`, not an external
system's id, despite the column name). This cost real debugging time during this test
run (see the several failed attempts in raw logs) and would likely trip up the next
engineer who calls `ensure_entity_registry` naively. Not a correctness bug — the FK/CAS
guarantees still hold — but the registry's actual usage contract is undocumented and
effectively untested by any existing caller.

---

## WP-2 — Slack Operating Queues & Send Governance

### Real Slack delivery verification (independent of the app — via Slack Web API)
Three cards were posted through the real `post_for_approval()` path, one to each lane's
real channel, then independently fetched back via `conversations.history` using the
Slack Bot token (not just trusting the app's own "success" return value):
```
C0C24GUGC4W (MONEY)         ts=1790057144.761849  found=True  "*Relay approval needed* (#430) ..."
C0C2A5L1BHS (EXCEPTIONS)    ts=1790057146.057629  found=True  "*Relay approval needed* (#431) ..."
C0C26DAMM18 (RELATIONSHIPS) ts=1790057147.361629  found=True  "*Relay approval needed* (#432) ..."
```
All three lanes deliver to their configured real Slack channels.

### Done When (a): a full operational day can be run from the Slack queues

**PASS.** Ran a complete item lifecycle through the actual code paths (not a shortcut):
`relay.queue.enqueue()` → `relay.slack_post.post_for_approval()` (real Slack card
posted, `slack_message_ts` confirmed non-null) → `admin_router._handle_relay_decision()`
— the exact function Slack button clicks and `/slack/events` thread replies invoke —
called with a real configured approver id (`U0AF9BE8RFC`, from `RELAY_APPROVERS`).

```
enqueued item_id=437, status=pending
real Slack card posted: slack_message_ts=1790057593.967799
decision handler result: {'ok': True}

--- Post-decision durable row (audit) ---
  status: approved
  decided_by: U0AF9BE8RFC
  decided_at: 2026-09-22 06:11:20.040856+00:00
  decision_interaction_id: 01a0c7bd-7f6c-701b-a700-a7a13fd8de46
  autonomy_tier_at_send: A
  agent_name: zztest_agent
  recipient: zztest-opday-909d8d14@test.example

--- Linked fa_max_interactions audit row ---
  channel: slack
  direction: inbound
  actor: slack_approver:U0AF9BE8RFC
  approved_bool: True
  autonomy_tier_at_time: A
  occurred_at: 2026-09-22 06:11:20.040856+00:00
```
The full audit chain required by WP-2's scope — recipient, content/action, agent,
autonomy tier, human approval state, timestamp, and who decided it — is present and
attributable, and it committed atomically with the state transition (same transaction).

### Done When (b): a suppressed contact cannot receive a message even via explicit override, and the system returns the reason

**PASS — both governance gates independently verified, including deliberate override attempts.**

Setup: a synthetic email was inserted into the real `email_opt_outs` table (the same
table `is_email_suppressed()` reads), simulating a real prior opt-out.

**Gate 1 — `relay.queue.enqueue()`, called with an explicit `auto_authorize=True`
override attempt** (i.e., even asking the system to skip human approval and
autonomously send):
```
GATE 1 PASS: enqueue() refused. reason='suppressed:email_opt_out'
```
`enqueue()` raised `GovernanceBlocked("suppressed:email_opt_out")` before any row was
ever written — the override flag has no effect on the suppression check.

**Gate 2 — the independent send-time guard** (`relay.guards`, the function the
sweep/dispatch engine calls immediately before every real send — a *different* code
path from gate 1). To prove gate 2 is not merely relying on gate 1 having blocked the
row, a row was inserted **directly into `relay_approval_queue`** with
`status='approved'` and `decided_by='admin_override_attempt'` — i.e., simulating a
hypothetical bug or bad actor that bypassed gate 1 entirely and got the row to
"approved" by some other means:
```
gate-1-bypassing row inserted directly as status=approved decided_by=admin_override_attempt id=435
GATE 2 (send-time guard, full evaluate()) verdict: outcome=defer reason='fa_max_relay_send_mode_not_live'
guards._suppression_reason(item) -> 'email_opt_out'
GATE 2 PASS: the send-time suppression check independently refuses this contact (reason='email_opt_out')
```
Note: the full `evaluate()` guard actually defers even earlier on this box, because
`fa_max_relay_send_mode` is not `live` here — a third, even coarser safety layer
(all real FA Max sends are globally disabled at this config flag on this server). To
isolate and prove the suppression check specifically (as it would fire once
`send_mode=live`), the same `guards._suppression_reason()` function `evaluate()` calls
was invoked directly and independently returned `'email_opt_out'`.

**Verdict: PASS on both counts** — enqueue-time and send-time gates both refuse a
suppressed contact and both return a specific, correct reason, and both resist an
explicit override attempt (`auto_authorize=True` and a direct DB force-approve).

### Other WP-2 checks (PASS, from `scripts/e2e_fa_max_test.py`)
- `validate_safe_payload` rejects a prohibited financial field (`credit_score`) and
  prohibited financial text ("interest rate") — `GovernanceBlocked` raised in both cases.
- `require_consent` correctly distinguishes `consent_absent` vs `consent_withdrawn`.

---

## WP-5B — Borrower Buy Box, Velocity & Next-Need Prediction

To get real, non-fabricated cadence/prediction evidence (rather than a synthetic
deed history, which risks corrupting or misrepresenting real property/deed FK data),
a synthetic `ZZTEST` person was **read-only linked** (via `fa_max_persons.buyer_entity_id`)
to a real, already-profiled production `buyer_entities` row
(`id=7943, "ADELAIDA A PADRON ACOSTA"`, 5 real deed links). Nothing in
`buyer_entities`/`deeds`/`properties` was written to.

```
--- Computed profile (from REAL deed/cadence data) ---
  buy_box_geography: [{'city': 'TAMPA', 'county_id': 'hillsborough', 'count': 2}]
  buy_box_property_types: [{'property_type': '0106 Single Family', 'count': 2}]
  buy_box_price_band: {'min_cents': 6500000, 'median_cents': 22250000, 'max_cents': 38000000, 'sample_count': 2}
  buy_box_preferences: {'year_built_avg': 1983, 'beds_avg': 2.0, 'baths_avg': 1.5, ...}
  velocity_purchases_per_year: 1.00
  last_transaction_date: 2026-09-02
  avg_days_between_transactions: 121.0
  active_property_count: 1
  predicted_next_need: bridge
  predicted_next_need_date: 2027-09-02 00:00:00+00:00
  next_need_evidence: {'financing_intent': [{'property_id': 100782, 'financing_intent_score': '80.00',
      'recommended_product': 'bridge', 'signal_flags': {'equity_proxy_strong': True,
      'fresh_deed_0_30_days': True, 'active_structural_permit': True}, ...}],
      'date_prediction': {'basis': 'cadence', 'last_transaction_date': '2026-09-02',
      'velocity_purchases_per_year': '1.00', 'days_projected': 365}}
  confidence_tier: medium
```

**Done When: "the borrower ledger can show historical cadence plus a defensible
predicted next need and the evidence used to derive it" — PASS.** Cadence
(`velocity_purchases_per_year`, `avg_days_between_transactions`, `last_transaction_date`,
`active_property_count`), a predicted next need (`bridge`), a predicted date, and a
structured evidence object naming the exact signals (equity proxy, fresh deed, active
permit, financing-intent score) and the date-prediction basis are all present and
attributable to real underlying deed/permit records — not a black-box guess.

**Recalculation after a material event — PASS.**
```
write_interaction(... direction="inbound" ...)
profile_recompute work item enqueued after interaction: (work_item_id=..., status='available', payload={'reason': 'new_interaction'})
```
`state_engine.write_interaction()` (and, per code inspection, `transition()` reaching
`funded`/`matured` opportunity states) automatically enqueues a `profile_recompute`
work item — confirmed live.

**Explicit low-confidence/unknown state — PASS.** A synthetic person with no resolvable
buyer entity produced:
```
confidence_tier=unknown predicted_next_need=None velocity=None
```
No fabricated prediction was returned for a borrower with insufficient data — an
explicit `unknown` tier and `NULL` prediction fields instead, matching the WP-5B spec's
"explicit unknown/low-confidence state when data is insufficient" requirement.

### Other WP-5B checks (PASS, from `scripts/e2e_fa_max_test.py`)
- `compute_person_profile` writes a row even for a person with no entity (confidence='unknown').
- Idempotency: recomputing the same person's profile **UPSERTs**, does not duplicate rows (`count_before == count_after == 1`).

---

## Full raw pass/fail tally

`scripts/e2e_fa_max_test.py`: **26 passed / 0 failed / 26 total.**

Additional targeted tests written for this run (crash/resume ×2, suppression no-bypass
×2 gates, operational-day round-trip, WP-5B real-entity profile ×3 assertions): **all
passed** after two trivial test-script bugs were fixed (a wrong initial `state_version`
assumption, and a Slack user-id string-prefix assumption — both artifacts of the test
script, not the product code; corrected inline and re-run to confirm).

## What's NOT verified / out of scope here
- **GoHighLevel relationship-state sync boundary** (listed in WP-2's scope) was not
  exercised — no GHL-side test was requested or attempted.
- **Snooze/thread-action UI paths** other than approve were not driven end-to-end
  (only the `approve` branch of `_handle_relay_decision` was exercised).
- Real send dispatch (Telnyx/actual outbound email/SMS) was never attempted — this box
  has `fa_max_relay_send_mode != 'live'`, so no real outbound message to a live
  recipient was sent or could have been sent; only Slack approval-card posting (which
  is itself a real external side effect) and internal governance/DB behavior were
  exercised for the send path itself.
- The `ensure_entity_registry()` usage-contract gap noted under WP-1 findings.

## Cleanup
All synthetic (`ZZTEST`/`zztest`/`e2e_test`-tagged) rows created by this test run were
deleted after evidence capture: `fa_max_persons`, `fa_max_state_transition_events`,
`fa_max_interactions`, `fa_max_person_consent`, `fa_max_entity_registry`,
`fa_max_person_profiles`, `fa_max_work_queue`, `relay_approval_queue`, and
`email_opt_outs` rows scoped to this run's IDs/emails. Verified zero remaining rows for
every synthetic `person_id` used. No real `buyer_entities`/`deeds`/`properties` row was
ever written to (WP-5B test only read entity 7943). All systemd services
(`fa-api`, `lifecycle`, `cora`, `cora-command-center`, `fa-relay-slack-listener`,
`postgresql@16-main`, `redis-server`) confirmed `active` and healthy after the OS-level
`kill -9` crash test.
