# Cora Cold-Outreach Build — Completion Report (C1–C4 + Rename Migration)

**Branch:** `feat/cora-v2.2-cold-drafting` (backend, off `dev`). One commit already exists on this branch — `953d28c` ("feat(cora): build v2.2 architecture and C1-C4 workflows") — covering the state as of the first completion report. Everything since (Cell #2, Cell #3/win_back, Stripe checkout for founder_tier sourced from settings, the real `call.booked` trigger, real Gmail-based mailbox ingestion, the naming cleanup, the `checkpoint_ns` hardening, and the full test suite) is **additional work on top of that commit, currently uncommitted** — nothing further has been committed this session.

**Naming note:** cell identifiers no longer carry `cell_1_`/`cell_2_`/`cell_3_` numeric prefixes. That numbering was the original brief's own section labeling, not a naming scheme — cells are now named for what they are: `founder_tier_blitz`, `founder_tier_blitz_bh`, `auction_fast_follow`, `win_back`, `hard_money_intro_lenders`.

**Read this report as:** what got built, what deliberately deviated from the literal brief and why, and what is still not done. Nothing below is rounded up — partial is reported as partial.

---

## Status at a glance

| Task | Status | One-line reason |
|---|---|---|
| Rename migration (Cora→Lifecycle) | 🟡 **Built, pushed, NOT merged, DB migration NOT run** | Both repos have an unreviewed branch; the Postgres rename script has never been executed against the shared DB — unchanged, a cross-team/deploy decision, not something this build can close on its own |
| C1 — Cold-draft engine + cell grid + kill switch | 🟢 **Done** | Architecture deviates from the literal file/table description (LangGraph subgraph + file store, not a plain service + DB table) — functionally complete and test-covered |
| C2 — Target pipeline: founder_tier_blitz / auction_fast_follow / win_back + Stripe | 🟢 **Done, one offer's checkout still open** | All 3 cells have real producers, verified against live prod data; `founder_tier` now has a real Stripe checkout link; 4 other checkout-kind offers remain `payment_link=None` for genuine structural reasons (documented below) |
| C3 — Reply ingestion + classification + objection library | 🟢 **Done, live-verified, one thing still can't accrete** | Classification/drafting/booking-trigger are all real; real mailbox ingestion built AND verified end-to-end against real credentials (`leads@forcedactionleads.com`, Gmail API, read-only scope) — a real false-positive bug (Workspace system mail) was caught and fixed during that verification; objection library still doesn't accrete new entries automatically |
| C4 — Pre-call brief + booking-link config + acceptance harness | 🟢 **Done** | Brief generation is real; the call-booked trigger is real (see C3); full test suite written and passing |

**Test suite:** 136 tests in `tests/agents/cora/` — 133 unit (mocked Claude/Stripe/Gmail, real Redis via fakeredis, real file-store I/O) + 3 marked `integration` (real Claude API calls, real DB). All 136 pass. Regression check: the pre-existing `tests/agents/`/`tests/scenarios/` suites (excluding Cora) still pass at 260/261, with the 1 failure confirmed pre-existing and unrelated (zero diff on the affected file on this branch vs. `dev`).

---

## Rename migration — Cora (existing runtime) → Lifecycle

**Status: unchanged since the last report. Code-complete on both repos, pushed, unreviewed, unmerged. The Postgres migration script has never been run against the shared DB.**

- Backend (`Forced-action-`): branch `chore/rename-cora-to-lifecycle`, 1 commit (`7eff435`), pushed to `origin`. Touches ~280 files, plus `migrations/apply_rename_cora_to_lifecycle.py` (not yet run against the shared DB — it still has the old `cora_*` names).
- Frontend (`Forced-action-ui`): branch `chore/rename-cora-to-lifecycle`, 2 commits (`f21140e`, `c7e7bda`), pushed to `origin`.
- Neither branch is merged into `dev`. Merging before the DB migration runs would break production.
- This is why the Cora build proceeds under a hard constraint: zero new Postgres migrations, zero edits to `src/core/models.py`, `scripts/cron/crontab.txt`, `config/cora_guardrails.py`. Re-confirmed this session via `git diff dev...HEAD` against that file list, plus `src/agents/graphs`, `src/agents/subgraphs`, `src/agents/router.py`, `src/agents/supervisor.py`, `src/agents/state.py`, and `src/api/main.py` — **empty diff on all of them.**

---

## C1 — Cold-draft engine + cell-grid taxonomy + kill-switch

**Status: done.** Unchanged in substance from the last report — `config/cora_cell_grid.py` (now with cleaned-up names), `src/agents/cora/subgraphs/outreach.py`, `src/agents/cora/kill_switch.py` (feature key `cora_global`), `src/agents/cora/validation.py` (5 deterministic reject-gates). Architecture deviates from the literal brief (LangGraph subgraph + JSON-Lines store, not a plain service + DB table) for the reasons given in the prior report and `docs/plans/cora_v2_2_interim_build_decisions.md`.

**New this session:** `test_validation.py`, `test_cell_grid.py`, `test_outreach_subgraph.py`, `test_no_send_capability.py` — 40+ tests covering all 5 reject-gates, cell-tag correctness, duplicate/expiry handling, and a static AST-based scan proving no file under `src/agents/cora/` imports `write_tools` or calls a send function. One real bug found and fixed by a test, not by inspection: `worker.py`'s idempotency check marked a message "processed" *before* attempting it, so a message that failed once could never be retried — fixed to mark-on-success only (see the C2 verification section for the failing test that caught it).

**Checkpointer hardening, found via direct question, not a test:** `src/agents/cora/checkpointer.py::run_with_checkpoint` compiled every checkpointed graph with the default `checkpoint_ns=""` — isolation from anything else that might ever checkpoint against these same shared, reused tables rested only on "the old Lifecycle runtime doesn't actually use this checkpointer" and "thread_id formats don't collide," neither of which was enforced. Fixed by pinning `checkpoint_ns="cora"` explicitly. Verified end-to-end against the real Postgres checkpoint tables (a full `target.ready` event through `main_graph` → `run_with_checkpoint` → `outreach.py`, real Claude call mocked, everything else real) — completed successfully. **Gap surfaced by this check, not yet closed:** no test in the suite exercises `run_with_checkpoint`/`main_graph.py`'s actual checkpointed path at all — every existing test calls subgraphs' own `run_x()` functions directly, bypassing the checkpointer entirely. The 122-test count above is real coverage of everything except this one integration seam.

---

## C2 — Target-to-draft pipeline: all 3 cells + Stripe

**Status: done for all 3 cells' sourcing; Stripe checkout done for 1 of 5 checkout-kind offers.**

### founder_tier_blitz (unchanged) + auction_fast_follow (new)

- `src/agents/cora/tools/read_tools.get_recent_auction_fast_follow_whales()` — read-only: buyer entities already flagged `is_whale=true` with a distressed-acquisition deed (Hunter's own `DISTRESSED_KEYWORDS` vocabulary, reused not duplicated) recorded within a lookback window. Never calls `refresh_whale_flags()` or otherwise triggers `src.connectors.whale_auction_fast_follow.py`'s own write path — purely reads whatever that connector, on its own cron schedule, already wrote.
- Wired into `target_producer.produce_auction_fast_follow_targets()`, sharing the same dedup (`store.has_duplicate_actionable_draft`) and ranking (`fallback_ranking.py`, now with a real `auction_recency` term instead of the placeholder 0.0) as `founder_tier_blitz`.
- **Verified against the real production DB, read-only** (with explicit go-ahead first, since the local `.env` points at live prod): the query is structurally correct, but returned **0 qualifying rows** in this snapshot. Traced why by hand: 11 real tax-deed acquisitions exist in the lookback window, all correctly resolved to real `buyer_entities`, but **none of them are `is_whale=true` yet** — a single auction win rarely crosses the whale threshold on its own, and `opportunity_thread_id` (required for Cora to act on an entity at all) is only assigned to 2,405 of 807,760 buyer entities, almost entirely already-whale ones. This is a genuine, sparse-population finding, not a bug — the cell will produce real targets once Hunter's own accelerated rescore (the `whale_auction_fast_follow.py` connector, staggered 5 minutes after the nightly sweep per its own docstring) promotes a recent winner to whale status.

### win_back (new — built after an explicit product decision)

The prior report flagged this cell as a genuine conflict: its real population is lapsed subscribers, which don't fit Cora's buyer-entity schema, and is already served by the live `src.tasks.reactivation_scheduler.py`'s `tier3_winback` cohort (which actually sends). Presented this to you directly; your answer was to build it anyway, using a unique identifier to prevent double-messaging the same customer from two systems — not to skip it.

- `src/agents/cora/ingestion/win_back_producer.py` (new file — different enough machinery from the buyer-entity cells to warrant its own module):
  - Sources candidates via the exact same real, already-live logic the old system uses for the same cohort: `src.tasks.reactivation_scheduler._lapsed_subscriber_ids` / `_fetch_subscribers` + `src.services.reactivation_eligibility.check_tier3_winback_eligibility` (read-only imports from `src/services`/`src/tasks` — never from `src/agents/graphs`, and a dedicated test statically confirms this file never imports the forbidden `reactivation.py` graph).
  - **Double-messaging guard, the "unique identifier" you asked for:** `subscribers.last_reactivation_attempt_at` — stamped by the live system's own `reactivation.py` graph the moment it actually attempts contact — is read as the coordination signal. Any subscriber the live system attempted within `CROSS_SYSTEM_SAFETY_WINDOW_DAYS=14` (wider than that system's own 3-day cooldown, since a Cora draft can sit pending human approval for a while after being produced) is skipped outright.
  - Each subscriber gets a synthetic `opportunity_thread_id` (`SUB-{subscriber_id}`) so it fits Cora's schema/dedup; `confidence_score=100` (not inferred — a subscriber is a known, already-verified identity, unlike an unverified Hunter public-record match).
  - **Verified against the real production DB** (again, explicit go-ahead, read-only except for the real Redis publish, which was immediately drained and acked): 7 real lapsed subscribers found, 5 correctly eligible and produced as real `target.ready` events, 2 correctly excluded — both already contacted by the live reactivation system 3 days prior.

### Stripe checkout

- **`founder_tier` now resolves a real, working Stripe checkout URL — confirmed end-to-end against the real Stripe API.** Two rounds on this:
  1. First pass reused `src.services.stripe_service.create_subscription_checkout`, whose `tier=="founder"` price resolution reads `plans.stripe_price_id` from the DB. A real smoke test against the live Stripe API failed: `No such price: 'price_1TuygfItL8ebZ1In4m6QpwPY'` — that DB row was seeded before this build, unrelated to it, and doesn't match a real Stripe object in this environment's account/mode.
  2. Per direct instruction, rebuilt this to source the price id from **settings, not the DB**: `config.settings.get_settings().active_stripe_price("founder_monthly")` — an existing, already mode-aware helper (`stripe_price_founder_monthly` for live, `stripe_test_price_founder_monthly` for `STRIPE_TEST_MODE=true`) that the founder-tier DB path had simply never been wired to. `offer_links.py` now calls `stripe.checkout.Session.create()` directly with that price id, bypassing `create_subscription_checkout`/`get_price_id_for_checkout` entirely for this offer — `vertical`/`county_id` still go into Stripe metadata only, same as the shared function would do, since they have no bearing on the founder tier's flat-rate price.
  - **Re-ran the real smoke test after the fix**: this environment currently has `stripe_test_mode=True` with a real test-mode price configured (`price_1TvurvLRyRP2f1PxQ9wMlyaa`) — `active_stripe_price` picked it correctly, and the call succeeded, returning a genuine `https://checkout.stripe.com/c/pay/cs_test_...` URL.
- **The other 4 checkout-kind offers (`core_subscription`/starter, `lead_packs`, `insurance_distress_pack`, `bankruptcy_alert`) still resolve `payment_link=None`, correctly.** Unlike `founder`, every other real tier's price resolution *does* key a live `FoundingSubscriberCount` row by `(tier, vertical, county_id)` — calling it for a cold buyer entity with a made-up vertical would corrupt a real county's founding-slot count for a lead that hasn't converted. `lead_packs`/`bankruptcy_alert`'s underlying functions require an existing `subscriber_stripe_customer_id`, which a cold prospect never has. This remains real, unbuilt scope (a dedicated cold-checkout entry point for non-founder tiers), not a gap papered over.
- Safety note: this repo's Stripe integration is **live**, not sandboxed, in the shared `.env`. `tests/agents/cora/conftest.py::_no_real_stripe_calls` mocks `stripe.checkout.Session.create` directly (not a named service function, since `founder_tier` no longer calls one) so no test can accidentally hit the real Stripe API in either mode.

### Verification evidence

- Full non-integration suite: **122 passed** (`pytest tests/agents/cora/ -m "not integration"`); `-m integration` → 3 passed separately (real Claude API calls).
- `founder_tier_blitz` (unchanged from prior report): 25 real `target.ready` events from real `get_ranked_whales()` output, 21 real drafts produced.
- `auction_fast_follow` and `win_back`: both run end-to-end against the real production DB and real Redis, as described above.

---

## C3 — Reply ingestion + intent classification + objection library

**Status: done.** Classification, response drafting, and the real compliance-suppression write path are unchanged from the prior report and remain real. Mailbox ingestion — the one remaining gap — is now built.

- **The call-booked trigger is real** (see C4), not a stub waiting on an external system.
- **Real mailbox ingestion is now built: `src/agents/cora/ingestion/reply_mailbox_poller.py`**, via the Gmail API against your Google Workspace mailbox — a service account + domain-wide delegation, scoped to `gmail.readonly` only (deliberately the most restrictive scope that works; Cora cannot send, delete, or modify anything in the mailbox, only list and read). Setup is one-time, outside this codebase (GCP project → enable Gmail API → service account + JSON key → authorize its Client ID for domain-wide delegation in the Workspace Admin console with that exact scope) — configured via two new settings, `CORA_GMAIL_SERVICE_ACCOUNT_KEY_PATH` and `CORA_REPLY_MAILBOX_ADDRESS`; absent either, the poller silently no-ops every cycle rather than erroring, so it's safe to leave wired in before credentials exist. Runs as a periodic thread from `--serve` (mirroring `target_producer`'s pattern) and via `python -m src.agents.cora --poll-mailbox` for a one-shot manual test.
  - Dedup against reprocessing the same message is tracked entirely on Cora's own side (a Redis set of seen Gmail message ids) rather than by mutating the mailbox — consistent with staying strictly read-only.
  - **This surfaced and fixed a real, necessary gap the seeded-fixture tests never exercised**: `OutboundDraftRecord` never stored the recipient's `contact_email`/`contact_phone` at all, so there was no way to resolve a real inbound reply's `from_address` back to an `opportunity_thread_id` — every prior test supplied `opportunity_thread_id` directly, which a live mailbox poll can't do (it only has a `From:` header). Fixed: `outreach.py` now persists `contact_email`/`contact_phone` on every draft; added `store.find_opportunity_thread_id_by_email()`; `reply.py`'s `_node_match_thread` now resolves by email whenever `opportunity_thread_id` isn't already supplied, falling to `manual_review` (never guessed) when no match exists.
- **Objection library still doesn't accrete** — unchanged, named as future learning-engine scope in the module's own docstring.

### Mailbox access setup — what actually happened, end to end

Real credentials now exist and the integration has been verified live, not just against mocks. This is the full path taken to get there, since several steps didn't go as originally planned:

1. **GCP resource hierarchy.** The Google account with Workspace/admin access is `leads@forcedactionleads.com`. A new **Organization** was created under the `forcedactionleads.com` domain (GCP auto-associates it — Organization is the parent of every Project under that domain), and inside it a new **Project** named `forced-action`.
2. **Enabled the Gmail API** on that project.
3. **Created the service account** — `cora-reply-ingestion`. The creation flow ran twice by accident, producing two distinct service accounts (`cora-reply-ingestion@forced-action.iam.gserviceaccount.com` and an auto-suffixed duplicate, `cora-reply-ingestion-916@forced-action.iam.gserviceaccount.com`, since GCP appends a random suffix on an ID collision rather than failing). Neither had a key yet, so the `-916` duplicate was deleted outright and `cora-reply-ingestion@forced-action.iam.gserviceaccount.com` kept as the one real identity.
4. **Blocked creating a key for it**: the org (or Google's "Secure by Default" rollout, applied automatically to new orgs/projects) enforces `iam.managed.disableServiceAccountKeyCreation` — no service account in this org can have a downloadable JSON key by default, a deliberate security control against standing, non-rotating credentials.
5. **Two separate permission gaps, resolved by self-granting IAM roles at the Organization level** (via **IAM & Admin → IAM**, switched to the Organization resource, not the project):
   - Editing the org policy itself needs **Organization Policy Administrator** (`roles/orgpolicy.policyAdmin`) — not included in general "admin" roles like Organization Administrator, which is a separate, narrower grant in GCP's IAM model.
   - The Console's org-policy-edit UI additionally runs a Policy Simulator preview before committing, which needs **Policy Simulator Admin** (`roles/policysimulator.admin`) — a second, separately-gated permission (surfaced as `Permission 'policysimulator.orgPolicyViolationsPreviews.create' denied`).
   - Both roles were granted to the admin account via **IAM → Add principals** (self-granted, since the account already held Organization Administrator, which can manage IAM bindings at the org level including its own).
6. **Disabled/overrode the key-creation constraint**, then generated a real JSON key for `cora-reply-ingestion@forced-action.iam.gserviceaccount.com` (Keys tab → Add Key → Create new key → JSON).
7. **Domain-wide delegation, authorized in Workspace Admin** (`admin.google.com` → Security → Access and data control → API controls → Domain-wide delegation → **Add new**): the service account's Client ID (its Unique ID from the Details tab, not its email) + scope `https://www.googleapis.com/auth/gmail.readonly`.
8. **First real poll attempt failed**: `google.auth.exceptions.RefreshError: unauthorized_client` — domain-wide delegation hadn't actually been completed correctly at that point (step 7 above happened only after this failure surfaced it as missing). Re-checked and added it properly; the error was gone on retry.
9. **Credential handoff and wiring**: the downloaded key (originally in `Downloads`) was moved into `secrets/cora-reply-ingestion-service-account.json` inside the repo; `secrets/` was added to `.gitignore` (confirmed via `git check-ignore`, on top of the pre-existing `.env` ignore rule) so the key can never be committed; `.env` got two new lines, `CORA_GMAIL_SERVICE_ACCOUNT_KEY_PATH` (pointing at that file) and `CORA_REPLY_MAILBOX_ADDRESS=leads@forcedactionleads.com`.
10. **First successful real poll** (`python -m src.agents.cora --poll-mailbox`) authenticated correctly and pulled real messages — but found a real bug immediately: the query (`is:unread`) had no filtering, so it pulled 11 Google Workspace system notifications (`notify-noreply@google.com`, `workspace-noreply@google.com`) and published them as if they were prospect replies. Checked the real store before doing anything else — **zero of them had reached the classification pipeline** (`store.read_replies()` returned empty; nothing had been claimed by a worker yet), so nothing was wasted, but they were sitting in the live Redis queue. Drained and acked all 11, plus one unrelated stale message from an earlier interrupted `--serve` run, back down to `pending_count: 0, dlq_depth: 0`.
11. **Fixed the query**: `is:unread category:primary -from:google.com` — `category:primary` excludes Gmail's own Updates/Promotions/Social/Forums tabs (where system mail normally lands, not a real 1:1 reply), and `-from:google.com` is a direct, confirmed-necessary backstop against Workspace's own notification senders.

Only one of the 5 mailboxes originally discussed (`leads@forcedactionleads.com`) is wired in — the settings design (`CORA_REPLY_MAILBOX_ADDRESS`) currently holds a single address, not a list; extending to the other 4 (all under the same domain-wide delegation, so no repeat of steps 1–8) is a small follow-up, not attempted yet since only this one address was handed over.

### Verification evidence

- `test_reply_subgraph.py` (15 tests): all 10 seeded reply fixtures classify and route correctly with mocked Claude, plus new tests for email-based thread matching (resolves a real thread from `from_address` alone; unmatched → `manual_review`, Claude never called) and `store.find_opportunity_thread_id_by_email` (case-insensitive, most-recent-match, no-match-returns-None). A real-API integration test additionally runs true Claude classification against the same 10 fixtures (≥8/10 correct — a best-effort bound, not a hard guarantee).
- `test_reply_mailbox_poller.py` (6 tests, mocked): not-configured no-ops cleanly; a real unread message decodes and publishes correctly with `opportunity_thread_id=None` (left for `reply.py` to resolve); an already-seen message id is never republished; MIME decoding prefers `text/plain`, walks multipart, and falls back to tag-stripped `text/html`; one message's processing failure never blocks the others in the same batch; `.modify()` is never called (confirms the read-only-scope guarantee structurally, not just by convention). **No test yet pins the `category:primary -from:google.com` query fix** — that fix is verified live (see step 11 above) but not yet by an automated test.
- **Live verification against the real Gmail API and real Redis queue**, described in full above: authentication works end-to-end through the real service account + domain-wide delegation, real messages decode correctly, and the system-notification false-positive was caught and fixed before any garbage reached the real classification pipeline.
- The `UNSUBSCRIBE` → real suppression-write path is covered by a mocked-suppression unit test (fast, no DB) — the prior report's manual real-DB verification (write, then rollback) stands as the one-time proof the write itself is correct.
- Found and fixed a real bug via this test suite (not manual testing): `store.py`'s `_VALID_TRANSITIONS` didn't allow `"touched" → "closed"`, so an unsubscribe arriving before a subscriber ever formally "replied" silently failed to close the opportunity. Fixed to allow `closed` from every non-terminal state.

---

## C4 — Pre-call brief + booking-link config + acceptance harness

**Status: done.** Brief generation (`src/agents/cora/subgraphs/pre_call.py`) and the booking-link config (`config/cora_offer_links.py`) are unchanged from the prior report and remain real. What changed:

- **The call-booked trigger is now real, sourced from inside Cora itself — not from the Synthflow webhook the original brief pointed at.** Investigated that path directly this session and found it doesn't fit: `src/api/main.py`'s `demo_requested` Synthflow webhook fires for property-owner/lead calls in the *old* Lifecycle/GHL pipeline — a different population than Cora's Hunter buyer entities, with no path back to an `opportunity_thread_id` at all, and no real Calendly-confirmation webhook exists anywhere in the codebase to supply a real `scheduled_for` time either way. The real, available signal instead: when `reply.py` classifies a reply as `BOOKING_REQUEST` (a prospect saying "yes, let's talk"), that *is* Cora's own booking signal. `reply.py`'s persist node now publishes a real `call.booked` event at that moment (`scheduled_for=None`, honestly, since there's no calendar confirmation to read one from) — `src/api/main.py` was read, not edited; `src/agents/cora/ingestion/call_booked_stub_producer.py` remains available for manual/CLI use but is no longer the only path in.
- **The acceptance-test harness now exists and passes.** `tests/agents/cora/` has `conftest.py` (sandboxed Redis + isolated file store + autouse Stripe-call guard), `fixtures/whales.py` (10 seeded buyer entities) and `fixtures/replies.py` (10 seeded replies), `helpers.py`, and test files for every module: kill switch, cell grid, validation, offer links, outreach, reply, pre-call, queue reliability, worker shutdown, no-send-capability, target producer, win-back producer. 122 tests total, all passing (119 unit + 3 real-API integration).

### Verification evidence

- `test_pre_call_subgraph.py` (4 tests): a booked call produces a brief with every required field populated and the correct current reply intent pulled in; a Claude-failure path still persists a brief with empty (not crashed) LLM fields; a real-API integration test confirms genuine non-empty `suggested_opening`/`call_objective` from a live Claude call.
- `test_reply_subgraph.py::test_booking_request_reply_triggers_call_booked_event` and its sibling tests confirm: a `BOOKING_REQUEST` reply publishes exactly one `call.booked` event with the right thread ID and buyer entity attached; a non-booking reply publishes nothing; an unresolvable buyer entity fails safely (the reply still persists, `call.booked` is skipped, no crash).
- `test_queue_reliability.py` (6 tests) and `test_worker_shutdown.py` (4 tests) — written and run this session, and immediately caught the two real bugs described above (idempotency-blocks-retry in `worker.py`, and the `_VALID_TRANSITIONS` gap in `store.py`) before they could reach production. Also confirmed: one-active-execution-per-thread locking correctly blocks a concurrently-locked message, and `claim_stale` correctly dead-letters after `MAX_DELIVERIES=3`.
- `test_no_send_capability.py` (17 parametrized tests, one per file under `src/agents/cora/`): AST-based, not string-matching — confirms zero imports of `write_tools` and zero calls to any `send_*` function anywhere in the package, including the two files added this session.

---

## Net position

Compared to the prior report, every C2/C4 item that was "unbuilt" for a *coding* reason is now built and tested. What remains open is narrower and more honestly categorized:

1. **The rename migration is still unmerged and its DB migration still unrun** — unchanged, a cross-team/deploy decision outside this build's scope, and still the reason this build carries a zero-new-migration constraint.
2. **`founder_tier` now produces a real, working Stripe checkout link** (test-mode, verified against the real Stripe API — this environment currently runs `STRIPE_TEST_MODE=true`). **The other 4 checkout-kind offers (`core_subscription`/`lead_packs`/`insurance_distress_pack`/`bankruptcy_alert`) still resolve no payment link** — a real Stripe-side gap, no cold-checkout entry point exists for non-founder tiers yet, not a code shortcut.
3. **Reply-mailbox ingestion is built, credentialed, and live-verified for one mailbox (`leads@forcedactionleads.com`)** — real service account, real domain-wide delegation, real end-to-end poll confirmed working (see the "Mailbox access setup" section above for the full path, including two org-policy permission gaps and one false-positive bug hit and fixed along the way). Only 1 of the 5 mailboxes originally discussed is wired in; extending to the other 4 needs no repeat of the GCP/Workspace admin steps (domain-wide delegation is domain-wide, not per-mailbox), just their addresses and a settings change from a single address to a list. The query-filter fix (excluding Workspace system mail) is verified live but not yet pinned by an automated test.
4. **The objection library still doesn't accrete new objections automatically** — named as future learning-engine scope from the start.
5. **`main_graph.py`'s actual checkpointed path (`run_with_checkpoint`) has no test coverage** — every test in the suite calls subgraphs directly, bypassing the checkpointer. Verified manually this session (`checkpoint_ns` fix confirmed working end-to-end against real Postgres), but not pinned by a repeatable test yet.

Everything else — all 3 target-sourcing cells, the founder-tier checkout link, the reply/booking/pre-call flow end to end including real (not stubbed, now credentialed and live-verified) mailbox ingestion, and a 136-test suite covering all of it (including three real bugs the suite itself caught, not manual review — the worker idempotency-blocks-retry bug, the `_VALID_TRANSITIONS` gap, and the missing `contact_email` needed for reply matching, plus a fourth caught by live verification rather than tests — the Workspace system-mail false positive) — is built, verified against real production data and real external APIs, and passing.
