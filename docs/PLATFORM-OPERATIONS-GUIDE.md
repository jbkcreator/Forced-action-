# Platform Operations Guide — How Each Phase 2B Item Actually Works

**Purpose:** walk through the 41 priority-list items as they behave in **real production**, not in tests. For each item: what triggers it, what the user (or founder, or system) actually experiences, what data it writes, what other items it chains with, and which existing services it extends.

**How to read this:** grouped by trigger type — config / cron / webhook / user API action / behavioural rule / SMS command / agent. Each section explains the common pattern once, then lists the items that follow it.

---

## Section 1 — Configuration artefacts (always loaded at process start)

These aren't "features that fire" — they're the tables and YAML / Python configs the rest of the platform reads. They exist before any event happens.

### 1.1 Revenue ladder config
**Lives at:** `config/revenue_ladder.py` — 12-step definition with prices, triggers, billing models.
**How it works in production:** Loaded once at process start, cached in memory. Every engine that offers a product (checkout endpoint, wallet engine, annual push, Data-Only save tier) reads from this file. Price change = commit new value + restart. The agents layer reads these same prices when composing messages ("Lock is $197/mo" — the number comes from `revenue_ladder.ANNUAL_PLAN['price_yearly']`).
**Chains with:** every pricing-surface feature. Stripe products mirror this config — `scripts/seed_stripe.py` reads this file and creates prices in Stripe.

### 1.2 Wallet tiers + dynamic enrollment triggers
**Lives at:** `config/revenue_ladder.WALLET_TIERS` + `src/services/wallet_engine.py::check_enrollment_triggers`.
**How it works in production:** The tier definitions (Starter $49 / Growth $99 / Power $199) are config. The enrollment **triggers** are a function called on every significant subscriber event — they run live, querying Postgres for recent unlocks, spend, and ZIP activity. When a trigger fires the wallet engine creates a Stripe subscription and inserts a WalletBalance row. The user sees: "You've been enrolled in the Growth Wallet — here are 50 credits."
**Chains with:** accelerated wallet push (§5.8), saved-card bonus credits (§5.7), Annual push (§2.1).

### 1.3 Auto Mode config
**Lives at:** `config/revenue_ladder.AUTO_MODE` + `src/services/auto_mode.py`.
**How it works in production:** Flag on `Subscriber.auto_mode_enabled`. Activated via `AUTO ON` SMS command (§6). Current implementation logs a queued intent; the execution layer (skip-trace + text + VM) depends on the LangGraph Auto Mode graph which is Wave 2 scope.
**Chains with:** SMS commands (§6), Auto Mode Execution graph (not built — Wave 2).

### 1.4 Bundle configs
**Lives at:** `config/revenue_ladder.BUNDLES` defining Weekend / Storm / ZIP Booster / Monthly Reload.
**How it works in production:** Bundle availability rules are code — Weekend checks `datetime.now().weekday()`, Storm checks `storm_active:{zip}` Redis key, ZIP Booster is always available, Monthly Reload is a recurring Stripe subscription. When a user opens their dashboard the bundle engine returns currently-eligible bundles. Purchase runs through the Payment Sheet endpoint and inserts a BundlePurchase row with an expiry timestamp.
**Chains with:** NWS webhook (§3.1 activates Storm Pack), Payment Sheet (§4.3), Bundle expiry sweep (daily cron).

### 1.5 Segmentation + revenue signal score
**Lives at:** `config/scoring.py` (weights) + `src/services/segmentation_engine.py` + `src/services/revenue_signal.py`.
**How it works in production:** Event-driven — every time a subscriber takes a significant action (unlock, signup, deal capture, wallet enrollment, saved-card save) the classifier re-runs and writes to `user_segments`. Revenue signal score is computed on the same cadence using five weighted inputs (spend velocity, engagement recency, wallet/lock status, lead interaction rate, ZIP competition). Every Cora graph reads both at its first node.
**Chains with:** every LangGraph graph, FOMO routing (finds next-best-fit subscriber), Retention summaries (picks tier cohorts).

### 1.6 Cora guardrail config
**Lives at:** `config/cora_guardrails.py` — 13 numerical bounds, 7 expansion gates, 9 kill-switch metrics.
**How it works in production:** Read by the `guardrail_check` and `kill_switch_status` gating tools. Every Cora decision checks bounds before composing — a proposed $300/mo Lock price gets rejected at `guardrail_check` because the max is $247. Changes to bounds require a config update + agents restart, on purpose (guardrails are policy, not runtime-mutable).
**Chains with:** every autonomous Cora decision.

### 1.7 Claude routing + prompt caching + batch + cost tracking
**Lives at:** `src/services/claude_router.py`.
**How it works in production:** Every Claude call in the codebase goes through `call_claude()` or `call_claude_with_usage()`. The router picks Haiku / Sonnet / Opus based on a task-type → tier mapping. Prompt caching is enabled by passing `cache_system=True`; repeated system prompts hit cache at ~10% of normal input cost. Every call writes one row to `api_usage_logs` with model, tokens, cost, task_type, subscriber_id. Batch API used for non-realtime tasks (learning cards, weekly summaries).
**Chains with:** every LangGraph graph (they all compose through the router), cost monitoring, LangSmith tracing.

### 1.8 Revenue Pulse config
**Lives at:** `config/revenue_pulse.py`.
**How it works in production:** Defines kill-switch colour thresholds, the metric list, and the SMS body template. Read by the daily + weekly Revenue Pulse cron jobs at the moment they compose the founder SMS. Changes require a config update + restart.
**Chains with:** Revenue Pulse daily + weekly (§2.3, §2.4), kill-switch discipline.

### 1.9 Learning card schema
**Lives at:** `src/core/models.py::LearningCard` + `alembic/versions/m3n4o5p6q7r8_*.py`.
**How it works in production:** The `learning_cards` table has a `(card_date, card_type)` unique constraint — Sunday job upserts; Cora graphs query by type ("give me the latest `message_perf` card") and inject the JSONB payload into prompt context. Cards never delete — 52 weeks of history accumulates.
**Chains with:** Learning card Sunday cron (§2.5), every Cora decision (reads latest card at decision-hierarchy step).

### 1.10 Launch compliance baseline
**Lives at:** `src/services/sms_compliance.py` + `src/core/models.py::SmsOptIn/SmsOptOut/SmsDeadLetter`.
**How it works in production:** `can_send()` check runs on every outbound SMS — no bypass. TCPA double opt-in records the exact prompt text for audit. Opt-out wins over opt-in (STOP after YES = STOP wins). DNC list entries block proactive marketing. Dead-letter queue captures every failed send for admin review at `GET /admin/dlq`. These are not new features — they're the guardrails every feature in this doc runs through.
**Chains with:** every outbound SMS, Twilio inbound webhook.

### 1.11 Schemas: message_outcomes / deal_outcomes / learning_cards / referral_events
**Lives at:** `src/core/models.py` + phase 2B migration.
**How it works in production:** These four tables are the intelligence substrate. **message_outcomes** — one row per outbound SMS with conversion attribution (4h / 24h / 48h windows). **deal_outcomes** — one row per user-reported deal. **learning_cards** — weekly aggregates written by the Sunday cron. **referral_events** — pending → confirmed → rewarded state machine. All four are append-only from the app's perspective.
**Chains with:** every learning / attribution / payout mechanism in the system.

---

## Section 2 — Cron-driven jobs (run on schedule)

Every cron reads from the DB, acts on matching rows, and writes results back. No human involvement.

### 2.1 Annual push triggers (daily, 8 AM UTC)
**Entry point:** `src/tasks/annual_push.py::run_annual_push`.
**Real-world flow:**
1. 8 AM UTC, server cron fires the job.
2. Job queries all `status='active'` subscribers.
3. For each, checks 6 triggers in order: Day 7 charter · Days 10–14 · 2+ deals · $250+ cumulative spend · $10K+ deal win · Day 60 auto-switch.
4. First trigger that fires → email + SMS goes out via the compliance-gated outbound path. If the user replies `YEARLY`, the SMS dispatcher calls `switch_to_annual()` which updates the Stripe subscription with proration.
5. Job logs a row to `annual_push_log` (trigger name, sent/skipped, outcome).
**How it adds to existing:** Uses existing Stripe subscription update APIs, existing SMS compliance gate, existing `DealOutcome` table for the 2-deals and $10K triggers.

### 2.2 Data-Only save tier (daily, 10 AM UTC)
**Entry point:** `src/tasks/proactive_save.py::run_proactive_save`.
**Real-world flow:**
1. Identifies at-risk subscribers: inactivity exactly 5–7 days with no wallet activity, OR Stripe payment failure Day 5+.
2. Offers Data-Only at $97/mo via email + SMS.
3. On acceptance (reply `DATAONLY` or SMS confirmation click), downgrades Stripe subscription.
4. Free and Data-Only subscribers skipped (never offer someone a downgrade they're already on).
**How it adds to existing:** Existing subscription lifecycle (the `Subscriber.status` machine), existing Stripe reconciliation, existing SMS gate. Brand-new is the 5–7 day inactivity heuristic driving the offer.

### 2.3 Revenue Pulse daily SMS (7:30 AM UTC)
**Entry point:** `src/tasks/revenue_pulse.py::run_daily_pulse`.
**Real-world flow:**
1. Queries yesterday's metrics: qualified lead count, wallet-active subs, top deal reported, latest alert, kill-switch colour.
2. Composes a short SMS body (<160 chars).
3. Sends to `FOUNDER_PHONE` — bypasses TCPA gate because it's the operator's verified number.
4. Founder sees on phone every morning: one number, one alert, one trend.
**How it adds to existing:** It IS the ops surface — replaces every previous "log in to dashboard to see numbers" flow with push.

### 2.4 Revenue Pulse weekly (Monday 9 AM UTC)
**Entry point:** `src/tasks/revenue_pulse.py::run_weekly_pulse`.
**Real-world flow:**
1. Aggregates the past 7 days: estimated MRR, new subs, churn, kill-switch colour across all 9 metrics, latest learning card summary.
2. Sends to founder phone as a longer SMS (up to 320 chars / 2 segments).
3. First message each Monday that sets the week's priorities.
**How it adds to existing:** Monday weekly recap that previously existed only as a manual Slack message.

### 2.5 Learning card Sunday cron (Sunday midnight UTC)
**Entry point:** `src/tasks/learning_card_job.py::run`.
**Real-world flow:**
1. Runs 4 generators in sequence: message performance · deal patterns · A/B results · churn signals.
2. Each generator queries the past 7 days, aggregates, and upserts one row into `learning_cards` keyed on `(card_date, card_type)`.
3. Minimum-sample thresholds prevent noisy cards (e.g., at least 10 messages before emitting a `message_perf` card).
4. Cora graphs read these cards at the start of every decision tree during the following week.
**How it adds to existing:** Creates the feedback loop — outbound SMS this week affects outbound SMS next week.

---

## Section 3 — Webhook-driven flows (external triggers)

### 3.1 NWS webhook (weather alert)
**Entry point:** `POST /webhooks/nws/alert` → `src/services/nws_webhook.py::process_alert`.
**Real-world flow:**
1. Weather service sends a CAP alert payload.
2. Handler filters for qualifying events (Hurricane, Tropical Storm, Severe Thunderstorm, Tornado, Hail).
3. Extracts affected ZIPs from the CAP `properties.geocode`.
4. For each ZIP, sets `storm_active:{zip}` Redis key with 72-hour TTL.
5. Notifies locked-territory holders via compliance-gated SMS ("STORM ALERT: High-value leads now available in 33647").
6. Queues the storm scraper to enrich new leads in affected ZIPs.
**How it adds to existing:** Takes a free external signal and monetizes it within the hour. Directly activates the $39 Storm Pack bundle because the bundle system reads the same Redis flag.

### 3.2 Stripe idempotent webhooks
**Entry point:** `POST /webhooks/stripe` → `src/services/stripe_webhooks.py::handle_webhook`.
**Real-world flow:**
1. Stripe sends an event (they often send the same event multiple times for reliability).
2. Handler verifies the signature first.
3. Looks up `event.id` in `processed_events` — if already there, return 200 immediately (idempotent no-op).
4. Otherwise, records the event ID and routes to the appropriate handler (`_on_checkout_completed`, `_on_payment_succeeded`, `_on_card_saved`, `_on_subscription_updated`, etc.).
5. Handlers mutate subscriber state, wallet balances, referral credits, annual switches.
**How it adds to existing:** The idempotency check is what makes it safe to layer retry-heavy logic on top — every downstream event handler can assume each event fires exactly once.

### 3.3 Missed-call auto-signup (Twilio Voice inbound)
**Entry point:** `POST /webhooks/twilio/voice`.
**Real-world flow:**
1. Prospect calls the business number and either answers or hangs up.
2. Twilio calls this webhook with call metadata.
3. Handler creates a free-tier subscriber, generates a signed feed UUID, and sends a welcome SMS with the dashboard link.
4. TwiML response plays a short recorded greeting.
5. If the welcome SMS is suppressed (DNC etc.), the account is still created — founder can manually follow up.
**How it adds to existing:** Lowest-friction signup path. Doesn't replace the web signup form; it's an alternative entry.

### 3.4 Twilio inbound (SMS)
**Entry point:** `POST /webhooks/twilio/inbound`.
**Real-world flow:**
1. User sends any SMS to the business number.
2. Handler verifies signature, then routes:
   - STOP keywords → compliance opt-out + TwiML reply
   - YES / START / JOIN / SUBSCRIBE / UNSTOP → opt-in recording
   - BALANCE / LOCK / BOOST / AUTO ON / AUTO OFF / PAUSE / TOPUP / REPORT / YEARLY / SAVE CARD → command dispatcher
   - Anything else → dead-letter queue for admin review
3. Each handler returns a reply string that the webhook sends back as TwiML.
**How it adds to existing:** The unified inbound entry point. Previously there was nothing listening for inbound — now STOP compliance, TCPA opt-in, and all 10 product commands share one route.

### 3.5 STOP / dead-letter queue hardening
**Entry point:** inside the Twilio inbound handler.
**Real-world flow:**
1. STOP / STOPALL / UNSUBSCRIBE / CANCEL / END / QUIT detected.
2. Compliance service writes to `sms_opt_outs` immediately — before any reply is sent.
3. Confirmation reply goes out (legally required, exact wording audit-logged).
4. Any message that fails to parse or dispatch is captured in `sms_dead_letters` with reason + payload.
5. Admin reviews the DLQ at `GET /admin/dlq` to triage edge cases (e.g. user texting "plz stop" — the human can add an opt-out manually).
**How it adds to existing:** The DLQ is the operational safety net. Not new semantics but new visibility.

---

## Section 4 — User-action APIs (endpoint-driven)

### 4.1 Landing page updates (existing signup flow)
**Entry points:** `POST /api/signup`, `POST /api/waitlist`, `POST /api/checkout`.
**Real-world flow:** Unchanged from M1. User picks tier, enters email + ZIPs, runs through Stripe Checkout in the embedded modal.
**Frontend:** `Forced-action-ui/src/pages/LandingPage.jsx` — handled by the existing React SPA.
**What Phase 2B adds:** post-signup users see the Proof Moment on their dashboard (§4.2), can unlock individual leads via the Payment Sheet (§4.3), and see the monetization wall countdown (§4.4).

### 4.2 Proof moment flow
**Endpoint:** `GET /api/proof-leads?feed_uuid=...`.
**Real-world flow:**
1. Freshly-signed-up user hits their dashboard for the first time.
2. Frontend calls `/api/proof-leads` with the signed feed UUID.
3. Backend queries already-scored leads, picks 1 top-tier + 2 next-best-matched, returns with blurring applied server-side (street name masked, contact hidden on the 2 blurred ones).
4. User sees: one full lead with owner + phone, two blurred teases.
**Chains with:** Monetization wall (§4.4 opens on the same page load), Payment Sheet (§4.3 unlocks the blurred ones).

### 4.3 Payment Sheet flow
**Endpoint:** `POST /api/payment-intent`.
**Real-world flow:**
1. User taps "Unlock $4" on a blurred lead (or "Activate Storm Pack $39" on a bundle).
2. Frontend calls `/api/payment-intent` with purchase type + amount + `save_card: true`.
3. Backend creates a Stripe PaymentIntent with `setup_future_usage='off_session'` and returns the client secret.
4. Frontend opens the Stripe Payment Sheet SDK (Apple Pay / Google Pay / card).
5. User confirms in 3 seconds. Stripe webhook `payment_intent.succeeded` flips `has_saved_card=true` and grants the purchased item.
6. Frontend polls the purchase state and unlocks the lead on success.
**How it adds to existing:** Supplements (not replaces) Stripe Checkout. Checkout = full subscription signup; Payment Sheet = one-tap in-app purchase for anyone already signed up.

### 4.4 First-session monetization wall
**Endpoint:** `POST /api/wall/session` + `GET /api/wall/{session_id}`.
**Real-world flow:**
1. On first dashboard load, frontend calls `POST /api/wall/session` with the subscriber ID.
2. Backend creates a Redis-backed session with 24h TTL. Returns: countdown expires at X, ROI frame for the user's vertical, live qualified-lead count for their area.
3. Frontend renders the countdown timer (15 min) alongside the proof leads.
4. Frontend polls `GET /api/wall/{session_id}` every 30 sec to keep the countdown live and check `converted` flag.
5. When the user pays, the Stripe webhook handler calls `mark_converted()` on the session; next poll returns `converted=true` and the wall disappears.
**Chains with:** Proof moment (§4.2) renders alongside; Abandonment Pressure SMS (§7.3) fires 10–15 min after wall opens if no payment.

### 4.5 Default card save + saved-card bonus
**Endpoints:** Same Payment Sheet path — driven by `setup_future_usage=off_session` on the PaymentIntent.
**Real-world flow:**
1. User pays. Stripe retains the card per the off-session flag.
2. `payment_intent.succeeded` webhook handler calls `_on_card_saved`.
3. `has_saved_card=true` flips on the subscriber; `stripe_payment_method_id` is stored for future charges.
4. A 10-min Redis bonus window key is set: `saved_card_bonus:{subscriber_id}` with TTL 600.
5. Any purchase inside that 10-min window awards 2 extra credits automatically — the wallet engine checks the key at transaction time.
6. User sees: "+2 bonus credits for saving your card" on the success toast.
**How it adds to existing:** Turns a one-time card save into recurring revenue — saved cards unlock auto-reload and accelerated wallet push.

### 4.6 New lead hold
**Endpoint:** `POST /api/leads/{lead_id}/hold`.
**Real-world flow:**
1. Subscriber opens a Gold-tier lead in their feed.
2. Frontend POSTs the hold endpoint.
3. Service writes `lead_hold:{lead_id}={subscriber_id}` to Redis with 20-min TTL.
4. For 20 minutes, other subscribers see the lead as "being worked" — they cannot simultaneously contact.
5. Redis TTL auto-expires; no cleanup job needed.
**How it adds to existing:** Converts "first to see it wins" into "first to commit wins" — reduces the race-condition feeling for high-value leads.

### 4.7 Urgency on strong leads
**Mechanism:** TTL windows + ZIP sorted-set viewer counter.
**Real-world flow:**
1. Gold lead is surfaced in any subscriber's feed.
2. Urgency engine opens a 30-min window (guardrail-bounded 10–60 min) by writing a TTL key.
3. ZIP sorted-set increments: `urgency_zips:{zip_code}` with score = now_ts.
4. Dashboard renders "X contractors currently viewing 33647" — a zcard query.
5. When the window expires the user sees the lead downgrade to "open" status.
**Chains with:** FOMO engine (§7.2 reads ZIP activity before composing), Lead hold (§4.6) runs on top of this.

### 4.8 Deal-size capture
**Endpoint:** `POST /api/deal-capture`.
**Real-world flow:**
1. After a user confirms a deal closed, they tap one of 4 buckets: <$10K / $10–25K / $25K+ / Skip. Optionally they enter the dollar amount and days-to-close.
2. Feed-UUID authenticates the request (no login needed — click-through from SMS).
3. Handler writes `deal_outcomes` row, re-computes revenue signal score, queues annual push re-evaluation, flags the deal for learning-card aggregation.
4. User sees confirmation: "Thanks — we're tracking your wins."
**Chains with:** Annual push triggers ($10K+ deal win is one of 6 triggers), learning card deal-pattern generator, revenue signal score.

### 4.9 Storm pack
**Delivery:** Buyable via `/api/payment-intent` with `purpose='storm_pack'` when Redis flag is set.
**Real-world flow:**
1. NWS alert activates storm flag (§3.1).
2. Subscriber opens dashboard — storm banner appears: "Storm Pack available, $39 for 10 storm-affected leads in 33647, 72 hours remaining".
3. User taps → Payment Sheet → 10 leads reserved in their account.
4. Bundle row inserted with 72-hour expiry; expiry sweep removes if not used.
**Chains with:** NWS webhook (§3.1), Bundle system (§1.4), Payment Sheet (§4.3).

---

## Section 5 — Behavioural rules (event-driven, no human request)

These fire when a subscriber's state crosses a threshold. No user tap, no cron, no webhook.

### 5.1 Segmentation re-classification
**Trigger:** Every significant subscriber event.
**Real-world flow:** Unlock, payment success, deal capture, wallet enrollment → event handler calls `segmentation_engine.classify()` → new bucket written to `user_segments`. The next outbound message uses the new bucket.

### 5.2 Revenue signal score recompute
**Trigger:** Same events as segmentation + a nightly full-refresh cron.
**Real-world flow:** Score computed from 5 weighted inputs, written back to `user_segments.revenue_signal_score`. Any Cora graph reading the score gets the latest value.

### 5.3 Wallet auto-enrollment
**Trigger:** 5 independent paths checked on every significant event.
**Real-world flow:**
1. Subscriber hits 2 unlocks in 24h → wallet engine enrolls them in Starter.
2. Or hits 3 unlocks total → Starter.
3. Or spends $8+ in a single day → pick tier by average-cost-per-unlock.
4. Or repeats a ZIP within 48h → Starter.
5. Or has a saved card AND recent activity → accelerated to Growth.
On enrollment: Stripe subscription created, 20/50/120 credits granted, welcome SMS goes out via compliance gate.

### 5.4 Wallet auto-reload
**Trigger:** Credit balance drops below 5.
**Real-world flow:** `debit()` checks the post-debit balance; if < 5 and `auto_reload_enabled` and `has_saved_card` → Stripe charge off-session, credits granted, `last_reload_at` stamped.

### 5.5 Free allotment rules
**Trigger:** Free-tier subscriber attempts a skip-trace / outbound text / voicemail.
**Real-world flow:** `allotment_engine.can_perform()` reads the weekly counter. If under limit (3/3/1), call succeeds; counter increments. If over, call returns "Free limit reached — add a wallet to continue." Wallet holders bypass.

### 5.6 Accelerated wallet push
**Trigger:** Saved-card user crosses 70% of wallet capacity in 14 days.
**Real-world flow:** Daily sweep flags the subscriber for next-tier upsell. Cora's next outbound to them (any campaign) references the tier upgrade.

### 5.7 Saved-card bonus credits
**Trigger:** Purchase inside a 10-min window after card-save event.
**Real-world flow:** Wallet engine's credit-grant path checks for `saved_card_bonus:{id}` Redis key before inserting the transaction. If present + not used, adds 2 bonus credits, logs reason.

### 5.8 Referral core loop baseline
**Trigger:** Referral link click + paid purchase by referee.
**Real-world flow:**
1. Subscriber's referral code is `REF` + base36 of their ID (in `Subscriber.referral_code`).
2. New signup via `/signup?ref=REFAB12` writes a `referral_events` row with `status='pending'`.
3. When referee makes their first paid purchase, the Stripe webhook handler promotes the row to `confirmed`, credits the referrer 20, credits the referee 10, sends referrer an SMS notification.
4. Row promoted to `rewarded` after SMS delivered.

---

## Section 6 — SMS command dispatcher (inbound keyword → reply)

### 6.1 SMS-only product commands
**Entry point:** Inbound Twilio webhook → `sms_commands.dispatch()`.
**10 commands:**
- `BALANCE` — reply with remaining wallet credits
- `LOCK` — initiate the lock purchase flow (reply with Payment Sheet link)
- `BOOST` — activate ZIP Booster bundle
- `AUTO ON` / `AUTO OFF` — toggle Auto Mode flag
- `PAUSE` — temporarily pause the subscription (60-day)
- `TOPUP` — manual wallet top-up link
- `REPORT` — send the latest Revenue Pulse-style summary
- `YEARLY` — accept the pending annual push offer
- `SAVE CARD` — link to card-save flow

**Real-world flow:** Every command resolves the sender to a subscriber, runs the command handler, returns a reply under 160 chars so it fits in a single SMS segment. All replies go back as TwiML.

---

## Section 7 — Cora LangGraph layer (autonomous decisions)

### 7.1 LangGraph supervisor setup
**Entry point:** `src/agents/supervisor.py::dispatch_event`.
**Real-world flow:**
1. Event arrives from any source (Redis Pub/Sub, Postgres LISTEN, cron, admin API).
2. Supervisor runs 4 checks in order: global kill switch → unknown event type → per-graph kill switch → idempotency-by-decision-id.
3. Routes to the matching graph via `EVENT_TO_GRAPH` dict.
4. Graph runs, writes audit row, returns.
5. Supervisor returns the outcome dict to the event source.

### 7.2 FOMO engine
**Entry point:** `competitor_acted_on_lead` event → FOMO graph.
**Real-world flow:** Within 60 seconds of a competitor contacting a Gold lead in a non-locked ZIP, Cora identifies the next-best-fit wallet-active subscriber (highest revenue signal score in segment), composes a Haiku-generated SMS with live ZIP data, and dispatches through the compliance gate. User sees: "Mike, another contractor just contacted a Gold lead in 33647. 2 more Gold leads are still open. [link]"

### 7.3 Abandonment pressure SMS flow
**Entry points:** `wall_session_abandoned` (Wave 1) and `abandonment_click_no_complete` (Wave 2).
**Real-world flow:**
- **Wave 1:** 10–15 min after wall opens with no payment → Cora composes a single-CTA SMS referencing live ZIP scarcity. Schedules Wave 2 intent in Redis.
- **Wave 2:** User taps the Wave 1 link but doesn't pay within ~20 min → Cora composes a scarcity-framed follow-up. If the user paid between waves, Wave 2 exits silently.

### 7.4 Cora SMS outbound
**Mechanism:** `send_sms` write tool in the agents layer.
**Real-world flow:** Every proactive SMS Cora sends runs through this tool — phone resolution, 24-hour idempotency check (don't send the same campaign-variant twice), compliance gate, Twilio dispatch, `message_outcomes` row inserted with attribution windows set.

### 7.5 A/B offer testing within guardrails
**Mechanism:** `ab_variant_assign` gating tool.
**Real-world flow:**
1. When Cora proposes a message, it first assigns the subscriber to a variant via deterministic md5 hash (same user always on same variant).
2. Traffic is capped at 10% of segment — outside the cap the user sees the control.
3. `record_outcome()` called from the Stripe webhook when the user converts.
4. A nightly `should_rollback()` check runs a two-proportion z-test; losing variants are auto-retired after 200 sends with a >2σ deficit.

### 7.6 Retention summaries
**Entry point:** `retention_summary_due` event, fired by cron per tier cadence.
**Real-world flow:** Sonnet composes a tier-specific long-form SMS referencing the user's recent activity and "what you would have missed" scarcity. Target subscribers: wallet / lock / AutoPilot holders with activity in the window. Skipped for churned or inactive.

---

## Section 8 — External integrations (partial / pending)

### 8.1 DBPR outbound integration
**Current:** Partial references in tax engine. Full outbound (licensed-contractor discovery, Clay enrichment, Instantly outreach) is pending a Clay account from the client. When Clay is set up: monthly DBPR refresh → Clay enrichment → Instantly multi-sequence → landing page link → signup.

---

## Section 9 — How items chain together in a real user journey

A composite of most of the above: **Mike the roofer** from first contact to paying subscriber to Cora-driven lock holder.

| Time | Event | Items triggered |
|---|---|---|
| T+0 | DBPR monthly refresh picks Mike's license | §8.1 |
| T+1 day | Instantly email sequence sends outreach | §8.1 |
| T+3 days | Mike clicks, lands on the landing page | §4.1 |
| T+3 min | Picks Pro tier, enters email + ZIPs | §4.1 |
| T+4 min | Stripe Checkout completes | §3.2, §1.1 |
| T+4 min | Post-signup webhook handler: create subscriber, send welcome SMS, TCPA opt-in prompt | §1.10, §3.2 |
| T+5 min | Dashboard opens | — |
| T+5 min | Proof moment renders 1 revealed + 2 blurred | §4.2 |
| T+5 min | Monetization wall opens with 15-min countdown + ROI frame | §4.4 |
| T+5 min | Urgency engine surfaces "3 contractors viewing 33647" | §4.7 |
| T+8 min | Mike taps "Unlock $4" on a blurred lead | §4.3 |
| T+8 min | Payment Sheet completes, card saved | §4.3, §4.5 |
| T+8 min | Wall flips to converted, saved-card bonus window opens (10 min) | §4.4, §4.5 |
| T+10 min | Mike unlocks a second lead, gets +2 bonus credits | §4.5 |
| T+12 min | Wallet engine auto-enrolls Mike in Starter ($49/mo) | §5.3 |
| T+4 days | A storm hits 33647. NWS webhook fires | §3.1 |
| T+4 days | Storm Pack banner appears in Mike's dashboard | §4.9 |
| T+4 days | Cora sends storm SMS through compliance gate | §1.10, §7.4 |
| T+7 days | Day 7 annual push cron fires for charter cohort | §2.1 |
| T+14 days | Mike has spent 40+ credits in 33647 → segment flips to `lock_candidate` | §5.1 |
| T+14 days | Competitor contacts a Gold lead in 33647. FOMO graph fires within 60s | §7.2 |
| T+14 days | Mike replies `LOCK`. SMS command dispatcher opens Payment Sheet | §6.1, §4.3 |
| T+14 days | $197/mo Lock subscription created via Stripe webhook | §3.2 |
| T+14 days | RLS territory lock excludes other users from 33647 leads | — |
| T+30 days | Mike reports his first deal via Deal-Size Capture | §4.8 |
| T+30 days | Annual push re-evaluation triggers $10K deal-win offer | §2.1 |
| T+30 days | Revenue signal score → 94 | §5.2 |
| T+45 days | Retention Summary for lock tier fires | §7.6 |
| T+60 days | 60-day annual auto-switch cron sends final offer | §2.1 |
| T+90 days | Mike has become a routine weekly user. Learning card Sunday job picks up his pattern | §2.5 |

Every arrow in that timeline is code that already exists (or is scheduled to exist as Wave 2). The items aren't siloed — they compose into one continuous commercial journey.

---

## Section 10 — How it adds to the current platform

**Before Phase 2B:** the platform was a scoring engine + Stripe Checkout + basic email/SMS notifications. Users signed up once, got periodic lead digests, canceled or renewed manually.

**After Phase 2B:** the platform is an autonomous commercial engine. The same scoring output feeds proof moments, bundle availability, storm packs, FOMO triggers, retention summaries. Stripe Checkout is supplemented by Payment Sheet one-taps. One-off renewals become 5-path wallet auto-enrollment + auto-reload. Email-only nurture becomes cron + event + agent-driven multi-channel dispatch. Every outbound message flows through one compliance gate. Every autonomous decision writes to a shared audit log. Every week a learning card rewrites Cora's next-week behaviour.

Concretely, 12 new tables, 5 new crons, 4 new webhook handlers, 7 new API endpoints, 10 inbound SMS commands, 1 agents process, 6 LangGraph graphs, 19 agent tools, 7 prompt templates. None of these replaced existing code — they compose on top of it.

---

## Section 11 — What's not yet live (and what unlocks it)

| Item | What unlocks it |
|---|---|
| Cora Conversational Lock Close (Sonnet live data) | Wave 2 LangGraph pass |
| Synthflow voice drop | Wave 2 + Synthflow outbound API wired |
| Dynamic Flash Scarcity SMS | Wave 2 |
| Dynamic Script Mutation | Wave 3 |
| Predictive churn scoring | Wave 3 |
| Thin human backup closer routing | Wave 2 |
| County launch automation | Gate monitoring + n8n workflow |
| DBPR outbound end-to-end | Clay account activation |
| Twilio A2P 10DLC live sends | Brand + campaign registration (2–4 week approval window) |
| Redis Pub/Sub event source | Redis provisioning on the server |
| Kill-switch observed values | Metrics aggregator job (n8n or cron) |
| Frontend for monetization wall + Payment Sheet + deal capture | React components (being built in this session) |

Everything above sits behind a clear gate. Flipping each gate is a contained task.

---

*This guide is a live document — update as behaviour changes or new items land.*
