# Phase 2B — Pending Implementation Items

**Source:** FA-2B-v9-FINAL.docx
**Audit date:** 2026-04-20
**Last updated:** 2026-04-21
**Status:** 3/41 done, 8 partial, 30 not built

**Completed since audit:**
- Item 8 — Claude API routing + cost tracking (`claude_router.py`) ✓
- Item 16 — SMS compliance core (`sms_compliance.py`, `sms_opt_outs`, `sms_dead_letters`) ✓ *(Twilio webhook endpoint + Redis pending)*
- Settings: Twilio creds, Claude model vars, Redis URL placeholder, FOUNDER_PHONE added to `config/settings.py` ✓
- Migration `2d2dd1371479` applied — `sms_opt_outs`, `sms_dead_letters`, `api_usage_logs` tables live ✓

---

## STAGE 1 — ECONOMICS

### 1. Revenue Ladder Config
**Status:** NOT BUILT
**Priority:** #1 (foundation for all monetization)

Current state: Only 3 subscription tiers exist (starter/pro/dominator) with fixed Stripe prices. No extended revenue ladder.

**What to build:**
- Define the full 12-step revenue ladder in config:
  1. Free Signup ($0)
  2. Proof Moment ($0 — value demo)
  3. Paid Unlock ($2.50–$7/unlock)
  4. Wallet auto-enroll ($49/$99/$199/mo)
  5. Auto Mode ($79–$99/mo add-on or included in Growth/Power)
  6. Territory Lock ($197/mo) — exists as current subscription model
  7. AutoPilot Lite ($299/mo)
  8. AutoPilot Pro ($497/mo)
  9. Annual Lock ($1,970/yr = $164/mo effective)
  10. Data-Only Save ($97/mo)
  11. Partner ($2,000/mo)
  12. White-Label ($2,500–$5,000/mo) — Stage 12+
- Create `config/revenue_ladder.py` with tier definitions, pricing, transition rules, and Stripe price IDs per tier
- Each step must define: trigger condition, price, who/what sells it (automated/Cora/product)

**Files to create/modify:**
- `config/revenue_ladder.py` (new)
- `config/settings.py` (new Stripe price env vars per tier)
- `src/core/models.py` (extend Subscriber model tier choices)

---

### 2. Wallet Tiers and Dynamic Enrollment Triggers
**Status:** NOT BUILT
**Priority:** #3

Current state: No wallet/credit system exists. Subscribers have fixed subscription tiers only.

**What to build:**
- **Wallet tiers:** Starter ($49/mo, 20 credits), Growth ($99/mo, 50 credits), Power ($199/mo, 120 credits)
- **Credit balance tracking:** Postgres-based balance per subscriber (not Stripe metered — faster, more control)
- **Auto-reload:** When balance < 5 credits, auto-charge saved card for tier-level reload
- **Dynamic enrollment triggers** (earliest of):
  - 2 paid unlocks in 24 hours
  - 3 total paid unlocks
  - $8+ spend in a single day
  - Repeat ZIP activity within 48 hours
- Saved-card users pre-qualified for wallet (skip proof-of-intent gate)

**Schema additions:**
```
wallet_balances:
  id, subscriber_id, tier (starter/growth/power), credits_remaining,
  credits_used_total, auto_reload_enabled, last_reload_at, created_at

wallet_transactions:
  id, subscriber_id, type (credit/debit/reload/bonus), amount,
  description, stripe_charge_id, created_at
```

**Files to create/modify:**
- `src/core/models.py` — WalletBalance, WalletTransaction models
- `src/services/wallet_engine.py` (new) — credit tracking, auto-reload, enrollment triggers
- `config/revenue_ladder.py` — wallet tier definitions
- Alembic migration for new tables

---

### 3. Auto Mode Config
**Status:** NOT BUILT
**Priority:** #8

Current state: No Auto Mode exists. Subscribers manually act on leads.

**What to build:**
- Auto Mode: automated lead outreach on subscriber's behalf
  - Auto skip-trace (if not already enriched)
  - Auto first-text to new leads matching subscriber's territory
  - Auto voicemail if no reply within 24 hours
- Pricing: $79–$99/mo add-on for Starter wallet tier; included free in Growth/Power wallet tiers
- Toggle via SMS command `AUTO ON` / `AUTO OFF` or dashboard setting
- Per-subscriber auto_mode_enabled flag + action limits

**Files to create/modify:**
- `src/core/models.py` — add `auto_mode_enabled` to Subscriber
- `src/services/auto_mode.py` (new) — scheduled job that processes queued auto-actions
- `config/revenue_ladder.py` — auto mode pricing rules

---

### 4. Bundle Configs
**Status:** PARTIAL (LeadPackPurchase exists for $99/5 leads; no other bundles)
**Priority:** #6

Current state: Single lead pack product ($99, 5 exclusive leads, 72-hour exclusivity).

**What to build:**
- **Weekend Pack ($19):** 5 bonus leads, available Friday–Sunday only
- **Storm Pack ($39):** NWS-triggered, storm-affected ZIP leads (ties to NWS webhook — item #22)
- **ZIP Booster ($29):** 10 additional leads in subscriber's existing ZIP for 48 hours
- **Monthly Reload ($89):** Auto-recurring credit bundle (alternative to wallet for pay-as-you-go users)
- Each bundle: Stripe PaymentIntent, delivery logic, expiry, idempotent purchase tracking
- Cora A/B tests bundle pricing within guardrail ranges (item #17)

**Files to create/modify:**
- `src/core/models.py` — extend LeadPackPurchase or create BundlePurchase model with `bundle_type` enum
- `src/services/bundle_engine.py` (new) — bundle creation, delivery, expiry
- `src/services/stripe_webhooks.py` — handle bundle payment_intent.succeeded
- `config/revenue_ladder.py` — bundle definitions and pricing

---

### 5. Segmentation Engine and Revenue Signal Score
**Status:** NOT BUILT
**Priority:** #5

Current state: Users are classified only by subscription tier. No behavioral segmentation or commercial scoring.

**What to build:**
- **8-bucket segmentation:** Classify every user into one of 8 segments based on behavior:
  1. New (< 24 hrs)
  2. Browsing (viewed leads, no action)
  3. Engaged (unlocked leads, no wallet)
  4. Wallet Active (wallet enrolled, using credits)
  5. High-Intent (repeat ZIP, high spend rate)
  6. Lock Candidate (wallet user + ZIP with 10+ uncontacted leads)
  7. At-Risk (5–7 days inactive)
  8. Churned (cancelled/expired)
- **Event-driven re-classification:** Each significant action (unlock, wallet enroll, lock purchase, 48hr inactivity) triggers segment re-evaluation
- **Revenue signal score (0–100):** Lightweight commercial score combining:
  - Spend velocity (credits/day)
  - Engagement recency (last action timestamp)
  - ZIP competition level
  - Lead interaction rate
  - Wallet tier / lock status
- Score feeds Cora routing decisions, urgency levels, upsell timing
- Daily sweep for new users in first 14 days (re-classify daily)

**Schema additions:**
```
user_segments:
  id, subscriber_id, segment, revenue_signal_score,
  last_classified_at, classification_reason, created_at, updated_at
```

**Files to create/modify:**
- `src/core/models.py` — UserSegment model
- `src/services/segmentation_engine.py` (new)
- `src/services/revenue_signal.py` (new) — score computation
- Alembic migration

---

### 6. Annual Push Triggers
**Status:** NOT BUILT
**Priority:** #9

Current state: No annual plan exists. Only monthly subscriptions.

**What to build:**
- Annual Lock plan: $1,970/yr ($164/mo effective) = 2 months free
- Automated push triggers (Cora sends offer when ANY condition is met):
  1. Day 7 for first 50 charter users (manually toggled)
  2. Day 10–14 for all users
  3. 2 confirmed deals
  4. $250 cumulative spend
  5. Deal-win reported at $10K+ (ties to deal-size capture — item #38)
- **Annual auto-switch at 60 days:** Automated GHL/Stripe sequence offers annual plan at Day 60 mark. Stripe switches subscription with zero human involvement.
- Stripe subscription update API for monthly-to-annual switch

**Files to create/modify:**
- `src/tasks/annual_push.py` (new) — scheduled job checking trigger conditions
- `src/services/stripe_service.py` — annual plan price IDs, subscription switch logic
- `config/revenue_ladder.py` — annual pricing and trigger thresholds

---

### 7. Data-Only Save Tier
**Status:** NOT BUILT
**Priority:** #9

Current state: Cancelled subscribers go to grace (48hr) then churned. No save/downgrade path.

**What to build:**
- $97/mo Data-Only tier: access to lead data feed only (no enrichment, no auto-mode, no VM)
- Proactive save trigger: subscriber inactive 5–7 days → Cora sends Data-Only offer BEFORE cancel
- Also offered as Day 5 of Stripe failed payment recovery sequence
- Stripe subscription downgrade to $97 price ID

**Files to create/modify:**
- `src/core/models.py` — add `data_only` to tier choices
- `src/services/stripe_webhooks.py` — handle downgrade
- `src/tasks/proactive_save.py` (new) — detect inactive subscribers, send offers

---

### 8. Claude API Routing, Prompt Caching, Batch Usage, Cost Tracking
**Status:** COMPLETE (2026-04-21)
**Priority:** #8 (LangSmith in Stage 8)

**What was built:**
- **Model routing:** `src/services/claude_router.py` — `call_claude(task_type, ...)` routes to Haiku/Sonnet/Opus based on task type. 15 task types mapped. Nothing calls `anthropic.messages.create()` directly.
  - Haiku: `sms_copy`, `classification`, `command_parsing`, `batch_summarization`, `address_matching`, `keyword_extraction`
  - Sonnet: `conversational_close`, `complex_reasoning`, `lead_analysis`, `learning_card`, `retention_copy`
  - Opus: `edge_case` (explicit override only)
- **Prompt caching:** `cache_system=True` flag on `call_claude()` attaches `cache_control: ephemeral` to system prompt blocks
- **Batch API:** `call_claude_batch(task_type, requests)` — submits to Anthropic Batch API, returns job ID for polling
- **Cost tracking:** Every call writes to `api_usage_logs` (model, input/output tokens, cost_usd, task_type, subscriber_id)
- **Model IDs in config:** `CLAUDE_HAIKU_MODEL`, `CLAUDE_SONNET_MODEL`, `CLAUDE_OPUS_MODEL` env vars — update model versions without code changes

**Files created/modified:**
- `src/services/claude_router.py` ✓ (new)
- `src/core/models.py` ✓ — `ApiUsageLog` model added
- `config/settings.py` ✓ — model-specific env vars added
- `alembic/versions/2d2dd1371479_add_compliance_observability_tables.py` ✓ — migration applied

**Remaining (2B-2):**
- LangSmith tracing integration

---

### 9. Revenue Pulse Config
**Status:** NOT BUILT
**Priority:** #12

**What to build:**
- Revenue Pulse = daily SMS to founder (Josh) with 3 items:
  1. One action to take
  2. One alert (metric breach, kill-switch trigger)
  3. One learning from Cora's experiments
- Config defines: recipient phone, send time, metric thresholds, alert conditions
- Data pulled from: PlatformDailyStats, wallet metrics, conversion funnels, A/B results

**Files to create/modify:**
- `config/revenue_pulse.py` (new)
- `src/tasks/revenue_pulse.py` (new) — daily cron job

---

### 10. Learning Card Schema
**Status:** NOT BUILT
**Priority:** #11

**What to build:**
- `learning_cards` table — weekly summary of what Cora learned:
  ```
  learning_cards:
    id, card_date, card_type (message_perf/deal_pattern/ab_result/churn_signal),
    summary_text, data_json (raw metrics backing the insight),
    action_taken (what Cora did based on this learning),
    created_at
  ```
- **Sunday midnight LangGraph job:** Summarizes past week's `message_outcomes`, `deal_outcomes`, and A/B results into a new learning card
- Cora reads the most recent cards at the start of every decision tree (warm context)

**Files to create/modify:**
- `src/core/models.py` — LearningCard model
- `src/tasks/learning_card_job.py` (new) — Sunday cron
- Alembic migration

---

### 11. Cora Guardrail Config
**Status:** NOT BUILT
**Priority:** #5 (enables all autonomous Cora decisions)

**What to build:**
- Config file defining hard bounds for every autonomous Cora decision:

| Decision | Allowed Range | Rollback Trigger |
|---|---|---|
| Lock pricing | $147–$247/mo | Conv rate drops >2 std devs for 48hr |
| Wallet tier pricing | $39–$249/mo | Same |
| Bundle pricing | +/-25% of base | Margin drops below 60% |
| Discount max | 20% off list | Never exceed |
| Credit bonus max | 10 credits | Never exceed per event |
| A/B test traffic cap | 10% of segment | Auto-rollback if losing >2 std devs |
| Message variant swap | Retire lowest of 3 after 200 sends | New must beat retired in 200 sends |
| Urgency window duration | 10–60 minutes | Never shorten below 10 min |
| Save offer | $97 Data-Only or 60-day pause | No lower without approval |
| Annual discount | 2 months free max | No deeper |
| Auto-reload threshold | <5 credits | Never change without approval |
| Paid acquisition spend | $500–$2,000/wk/channel | Pause if CAC >$25 for 7 days |
| County activation | All 7 gates green | Never override gates |

- Loaded as config at app startup; Cora checks bounds before every decision

**Files to create/modify:**
- `config/cora_guardrails.py` (new)

---

## STAGE 2 — CORA CORE

### 12. LangGraph Supervisor Setup
**Status:** NOT BUILT
**Priority:** #2 (Cora's brain)

**What to build:**
- LangGraph supervisor on DigitalOcean Docker (or AWS ECS)
- `langgraph-checkpoint-postgres` for state persistence
- Supervisor orchestrates all Cora decision trees:
  - Signup → proof moment → unlock → wallet enrollment
  - FOMO/scarcity trigger → lock close
  - Retention/save flows
  - A/B test routing
  - Revenue Pulse generation
- LangSmith tracing for observability
- Prometheus + Grafana metrics

**Dependencies:** Redis (item #13), Claude routing (item #8), Guardrails (item #11)

**Files to create/modify:**
- `src/cora/supervisor.py` (new) — LangGraph supervisor graph definition
- `src/cora/nodes/` (new directory) — individual decision nodes
- `docker-compose.yml` or deployment config
- `requirements.txt` — add `langgraph`, `langgraph-checkpoint-postgres`, `langsmith`

---

### 13. Redis Pub/Sub and Queueing
**Status:** NOT BUILT
**Priority:** #2

Current state: No Redis, no PGMQ, no async messaging. Everything is synchronous.

**What to build:**
- **Redis instance** for:
  - Pub/Sub: all real-time events (lead scored, ZIP activity, payment completed, referral chain)
  - Sessions + rate limiting
  - Credit balance caching (read from Redis, write-through to Postgres)
  - Allotment tracking (free tier usage counters)
  - ZIP activity counters (for FOMO engine)
  - STOP dead-letter queue
  - Referral chain tracking + milestone counters
  - New lead hold reservations (20-min TTL keys)
- **PGMQ (Tembo):** Postgres-native message queue for durable async tasks:
  - Webhook processing
  - Email/SMS delivery
  - Cora decision requests
- **NOT** using Postgres LISTEN/NOTIFY (per spec)

**Files to create/modify:**
- `src/core/redis_client.py` (new)
- `src/core/queue.py` (new) — PGMQ wrapper
- `config/settings.py` — Redis URL, PGMQ config
- `docker-compose.yml` — Redis service
- `requirements.txt` — add `redis`, `tembo-pgmq-python`

---

### 14. FOMO Engine
**Status:** NOT BUILT
**Priority:** #7

**What to build:**
- Triggered when: competitor (another wallet/free user) acts on a lead in a non-locked ZIP
- Flow: competitor action → Redis Pub/Sub event → FOMO engine evaluates → Cora sends SMS to all wallet users with that ZIP within 60 seconds
- **Dynamic Flash Scarcity:** Gold+ lead spike in a non-locked ZIP → SMS to wallet users in that ZIP within 60 minutes
- ZIP counter: real-time count of active users per ZIP (Redis sorted set)
- Visible competition pressure in dashboard: "3 other contractors are viewing leads in your ZIP"

**Files to create/modify:**
- `src/services/fomo_engine.py` (new)
- `src/cora/nodes/fomo_node.py` (new) — LangGraph node for FOMO decisions

---

### 15. Wallet Engine
**Status:** NOT BUILT
**Priority:** #3

(Covered in detail under item #2. This is the service layer.)

**What to build:**
- `wallet_engine.py`: credit debit/credit, balance check, auto-reload trigger, tier upgrade/downgrade
- Integration with Stripe for auto-reload charges
- Integration with Redis for cached balance reads
- All 12 conversion triggers that move users up the ladder:
  1. First unlock
  2. 2 unlocks in 24hr
  3. 3 total unlocks
  4. $8+ spend/day
  5. Repeat ZIP 48hr
  6. Saved card within 10 min
  7. Wallet balance < 5
  8. 10+ uncontacted leads in user's ZIP
  9. Revenue signal score > 70
  10. 5–7 day inactivity (save trigger)
  11. 40+ credits/mo in one ZIP (lock upgrade trigger)
  12. 10+ manual actions/week (AP Lite trigger)

**Files to create/modify:**
- `src/services/wallet_engine.py` (new)

---

### 16. STOP / Dead-Letter Queue Hardening
**Status:** PARTIAL — core compliance built (2026-04-21); Twilio webhook endpoint + Redis pending
**Priority:** #2

**What was built:**
- `src/services/sms_compliance.py` — full STOP compliance service:
  - `can_send(phone, db)` — pre-send gate; every outbound SMS must call this first
  - `handle_inbound(from_number, body, db)` — detects STOP/UNSUBSCRIBE/CANCEL/QUIT/END, writes opt-out, returns TwiML reply
  - `send_sms(to, body, db)` — central dispatcher: gate → Twilio send → DLQ on failure
  - `record_opt_out()` / `add_to_dead_letter()` — called internally and by admin flows
- `src/core/models.py` — `SmsOptOut` and `SmsDeadLetter` models added
- `config/settings.py` — `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `TWILIO_ENABLED` added
- Migration `2d2dd1371479` applied — `sms_opt_outs` and `sms_dead_letters` tables live in DB
- Suppression backed by Postgres for 2B-1 (Redis migration in 2B-2)

**Remaining:**
- `src/api/main.py` — wire Twilio inbound webhook endpoint to `handle_inbound()` (needed before any bulk SMS)
- `src/core/redis_client.py` — Redis STOP set (2B-2, replaces Postgres lookup for speed)
- Admin DLQ review endpoint (`GET /admin/dlq`) — 2B-2

---

### 17. A/B Offer Testing Within Guardrails
**Status:** NOT BUILT
**Priority:** #10

**What to build:**
- A/B test framework for Cora's autonomous experiments:
  - Test: timing, bundle vs wallet, bonus vs discount, urgency vs ROI framing
  - Traffic cap: 10% of segment per test (from guardrails)
  - Auto-rollback: if losing variant is >2 std devs worse after 200 sends
  - Retire lowest of 3 message variants after 200 sends; replacement must beat retired in 200 sends
- Schema:
  ```
  ab_tests:
    id, test_name, segment, variant_a, variant_b,
    traffic_pct, status (active/completed/rolled_back),
    started_at, ended_at, winner

  ab_assignments:
    id, test_id, subscriber_id, variant, outcome, created_at
  ```

**Files to create/modify:**
- `src/core/models.py` — AbTest, AbAssignment models
- `src/services/ab_engine.py` (new)
- Alembic migration

---

### 18. Revenue Pulse Daily SMS
**Status:** NOT BUILT
**Priority:** #12

(Service layer for item #9.)

**What to build:**
- Daily cron job (e.g., 8 AM EST) that:
  1. Queries PlatformDailyStats, wallet metrics, conversion rates, A/B results
  2. Identifies: top action, top alert, top learning
  3. Composes SMS via Claude Haiku (< 160 chars per segment)
  4. Sends to founder phone via Twilio
- Include kill-switch Green/Yellow/Red scoring summary on Mondays

**Files to create/modify:**
- `src/tasks/revenue_pulse.py` (new)

---

### 19. DBPR Outbound Integration
**Status:** PARTIAL (some DBPR refs in tax engines)
**Priority:** #1 (primary acquisition channel)

**What to build:**
- Monthly DBPR license data refresh for all 67 Florida counties
- Data flow: DBPR license file → Instantly/Clay enrichment (email + phone) → segmentation → Cora SMS outbound
- License verification: confirm contractor license is active and matches trade vertical within 24 hours of signup
- Multi-sequence outbound: different messaging per trade (roofing, restoration, etc.)
- Waitlist reactivation waves when new county goes live

**Files to create/modify:**
- `src/services/dbpr_integration.py` (new) — license lookup, enrichment pipeline
- `src/tasks/dbpr_refresh.py` (new) — monthly cron

---

### 20. Cora SMS Outbound (Conversational Close)
**Status:** PARTIAL (synthflow_service.py exists for outbound agent; no Claude Sonnet conversational)
**Priority:** #7

**What to build:**
- All high-intent SMS sequences rewritten as Claude Sonnet contextual conversations:
  - Pull live ZIP data (lead count, tier breakdown, competitor timing)
  - Pull subscriber's spend rate, revenue signal score
  - "I Found You a Deal" framing on new lead holds
  - Reply YES → Payment Sheet link < 5 sec
- Synthflow voice drop for revenue signal score > 70 users who haven't converted in 48 hrs:
  - 20-sec personalized voicemail
  - SMS follow-up within 60 sec of voice drop

**Files to create/modify:**
- `src/cora/nodes/conversational_close.py` (new) — Sonnet-powered SMS composer
- `src/services/synthflow_service.py` — add outbound voice drop trigger
- Cora prompt templates in `config/prompts/`

---

### 21. Missed-Call Signup
**Status:** NOT BUILT
**Priority:** #6

**What to build:**
- Synthflow inbound call handling: missed call to Forced Action number → webhook → auto-create free account
- Flow:
  1. Caller's phone number captured via Synthflow inbound webhook
  2. DBPR license lookup on phone number
  3. If valid contractor: auto-create account, send proof lead SMS within 60 sec
  4. If not found: send "text us your license number" reply
- One of 4 signup paths (DBPR email, Cora SMS, missed call, referral)

**Files to create/modify:**
- `src/api/main.py` — Synthflow inbound webhook endpoint
- `src/services/signup_engine.py` (new) — handles all 4 signup paths

---

### 22. NWS Webhook
**Status:** PARTIAL (storm engine/scraper exist; no NWS webhook)
**Priority:** #6

**What to build:**
- Subscribe to NWS weather alert API on Hillsborough County FIPS code (12057)
- Webhook receives severe weather alerts (hurricane, tropical storm, hail, tornado)
- On alert:
  1. Trigger storm pack offers to all active subscribers in affected ZIP
  2. Queue storm damage scraper for post-event data collection
  3. FOMO: "Storm just hit your ZIP — X leads incoming"
- Free NWS API (no cost)

**Files to create/modify:**
- `src/services/nws_webhook.py` (new)
- `src/api/main.py` — NWS webhook endpoint
- Config: FIPS codes per county

---

## STAGE 3 — SIGNUP + REVENUE ENGINE

### 23. Landing Page Updates
**Status:** PARTIAL (static SPA + React frontend exist)
**Priority:** Deploy-blocking

**What to build:**
- Integrate 2B features into React frontend:
  - Proof moment UI (1 enriched + 2 blurred leads)
  - Monetization wall countdown timer
  - Payment Sheet (Apple Pay / Google Pay)
  - Wallet tier selection
  - Competition/FOMO indicators on ZIP checker
  - Bundle purchase flows
  - SMS command reference

---

### 24. Proof Moment Flow
**Status:** NOT BUILT
**Priority:** #1

**What to build:**
- On signup, every free user gets:
  - 1 fully enriched lead (pre-traced: owner name + phone + distress signals)
  - 2 blurred leads (address + tier visible, contact info hidden behind paywall)
- "Tap to unlock" on blurred leads → Apple Pay / Payment Sheet → card saved → $2.50–$7 charged
- Proof lead must render in < 30 seconds from signup
- This IS the first payment moment — everything starts here

**Files to create/modify:**
- `src/api/main.py` — proof moment endpoint (GET /api/proof-leads)
- `src/services/proof_moment.py` (new) — lead selection, blurring logic
- React frontend components

---

### 25. First-Session Monetization Wall
**Status:** NOT BUILT
**Priority:** #1

**What to build:**
- Every free user hits a payment moment in Session 1
- Countdown timer on blurred-lead access (e.g., "Unlock in next 10:00 for 2 bonus credits")
- ROI frame before payment: "Contractors in 33613 close $12K avg from leads like these"
- Backend enforcement: Redis TTL or Postgres timestamp tracking session start, enforce wall at configured time
- If user doesn't pay within countdown: leads remain blurred, next visit resets countdown

**Files to create/modify:**
- `src/services/monetization_wall.py` (new)
- React frontend — countdown component, ROI frame
- Redis — session TTL keys

---

### 26. Payment Sheet Flow
**Status:** NOT BUILT
**Priority:** #1

**What to build:**
- Stripe Payment Sheet integration (replaces embedded checkout for micro-payments):
  - Apple Pay / Google Pay on first paid action (lead unlock)
  - One-tap payment, card automatically saved
  - Works for: lead unlocks ($2.50–$7), bundle purchases, wallet top-ups
- Stripe PaymentIntent with `setup_future_usage: 'off_session'` to save card for future charges
- Payment Sheet deep links from SMS ("Reply YES" → payment link < 5 sec)

**Files to create/modify:**
- `src/api/main.py` — PaymentIntent creation endpoint
- `src/services/payment_sheet.py` (new)
- React frontend — Payment Sheet component
- `config/settings.py` — Stripe Payment Sheet config

---

### 27. Default Card Save
**Status:** NOT BUILT
**Priority:** #1

**What to build:**
- Card saved by default on first payment (opt-out, not opt-in)
- Stripe `setup_future_usage: 'off_session'` on every PaymentIntent
- Saved card enables: auto-reload, wallet auto-enrollment, one-tap purchases
- Subscriber model: `has_saved_card` boolean, `stripe_payment_method_id`

**Files to create/modify:**
- `src/core/models.py` — add card fields to Subscriber
- `src/services/stripe_webhooks.py` — capture payment method on successful charge

---

### 28. Saved-Card Bonus Credits
**Status:** NOT BUILT
**Priority:** #6

**What to build:**
- Save card within 10 minutes of first payment → bonus 2 credits
- Tracked via Redis TTL: set key on first payment, check on card save event
- Credits added to wallet balance (or tracked separately if wallet not yet enrolled)

**Files to create/modify:**
- `src/services/wallet_engine.py` — bonus credit logic
- `src/services/stripe_webhooks.py` — detect card save timing

---

### 29. Abandonment Pressure SMS Flow
**Status:** NOT BUILT
**Priority:** #1

**What to build:**
- 10–15 minutes after signup with no payment:
  - Single CTA SMS: "Your proof lead is waiting. Unlock now → [link]"
- Click on link but no payment completion:
  - Scarcity follow-up: "2 other contractors in [ZIP] viewed this lead in the last hour"
- NOT nurture — stop messaging after 2 attempts
- Trigger: Redis TTL expiry (set on signup, fires if no payment event within 10–15 min)
  - Or: scheduled scan of recent signups without payment

**Files to create/modify:**
- `src/tasks/abandonment_pressure.py` (new)
- Redis — signup TTL keys

---

### 30. Free Allotment Rules
**Status:** NOT BUILT
**Priority:** #6

**What to build:**
- Free tier weekly limits:
  - 3 skip-traces
  - 3 outbound texts
  - 1 voicemail/week
- Tracked in Redis (fast read/write, weekly reset)
- Enforced at action time: if limit hit → "Upgrade to unlock more" CTA
- Free-tier cost cap: $6.50/user (if cost exceeds, tighten allotment)

**Files to create/modify:**
- `src/services/allotment_engine.py` (new)
- `src/core/redis_client.py` — allotment counter operations

---

### 31. Accelerated Wallet Push
**Status:** NOT BUILT
**Priority:** #3

**What to build:**
- Saved-card users are pre-qualified for wallet — skip the normal trigger threshold
- Immediately after repeated usage (2+ unlocks), offer wallet enrollment
- "Missing leads" framing: "You missed 4 leads in 33613 yesterday because you ran out of credits"
- Wallet disengagement → smaller commitment offer (save flow)

**Files to create/modify:**
- `src/services/wallet_engine.py` — accelerated enrollment logic
- `src/cora/nodes/wallet_push_node.py` (new)

---

### 32. SMS-Only Product Commands
**Status:** NOT BUILT
**Priority:** #6

**What to build:**
- Twilio inbound SMS keyword parsing:
  - `LOCK` → lock current ZIP territory ($197/mo)
  - `BOOST` → ZIP Booster bundle ($29)
  - `AUTO ON` / `AUTO OFF` → toggle Auto Mode
  - `PAUSE` → pause subscription (60-day hold)
  - `BALANCE` → reply with current credit balance
  - `TOPUP` → one-tap wallet reload
  - `REPORT` → send latest lead summary
  - `YEARLY` → switch to annual plan
  - `SAVE CARD` → send Payment Sheet link
- Each command: Twilio webhook → keyword router → action handler → reply SMS

**Files to create/modify:**
- `src/api/main.py` — Twilio inbound webhook endpoint
- `src/services/sms_commands.py` (new) — keyword parser + command handlers

---

### 33. Referral Core Loop Baseline
**Status:** NOT BUILT
**Priority:** #8

**What to build:**
- **60-second referral notification:** Referrer gets SMS within 60 sec of referee signing up (Redis Pub/Sub)
- **Milestone escalation:**
  - 1 referral = 5 bonus credits
  - 3 referrals = free month
  - 5 referrals = Lock upgrade
- **Forward pack:** Claude-written referral copy per trade (roofing vs restoration vs investor)
- **referral_events table:**
  ```
  referral_events:
    id, referrer_subscriber_id, referee_subscriber_id,
    referral_code, status (pending/confirmed/rewarded),
    reward_type, reward_value, created_at, confirmed_at
  ```
- Share link with unique referral code per subscriber

**Files to create/modify:**
- `src/core/models.py` — ReferralEvent model, add `referral_code` to Subscriber
- `src/services/referral_engine.py` (new)
- Alembic migration

---

### 34. Storm Pack
**Status:** NOT BUILT (storm scraper exists but no pack product)
**Priority:** #6

**What to build:**
- NWS webhook triggers storm pack availability (ties to item #22)
- $39 one-time purchase: 10 storm-affected property leads in subscriber's ZIP
- Leads sourced from: storm_damage, insurance_claims, flood_damage incidents in affected area
- Available only during active weather events + 72 hours post-event
- Cora sends proactive offer: "Storm warning for 33613 — storm pack available now"

**Files to create/modify:**
- `src/services/bundle_engine.py` — storm pack logic
- `src/services/nws_webhook.py` — trigger storm pack availability

---

### 35. New Lead Hold
**Status:** NOT BUILT
**Priority:** #6

**What to build:**
- Strong lead (Gold+) in subscriber's ZIP → reserved for 20 minutes
- "I Found You a Deal" SMS with lead preview
- One-tap unlock within 20-min window
- If not unlocked: lead released to pool
- Implementation: Redis key with 20-min TTL per (lead_id, subscriber_id)

**Files to create/modify:**
- `src/services/lead_hold.py` (new)
- Redis — hold reservation keys

---

### 36. Urgency on Strong Leads
**Status:** PARTIAL (urgency_level exists in scoring; no time-based windows)
**Priority:** #7

**What to build:**
- Every strong lead (Gold+) creates a short-lived urgency window (10–60 min, configurable per guardrails)
- Dashboard shows: "This lead was scored 12 minutes ago — 3 contractors are viewing"
- Auto lock messaging: "Your ZIP 33613 has 5 new leads — lock it before someone else does"
- Instant monetization on every weather event (ties to NWS webhook)

**Files to create/modify:**
- `src/services/urgency_engine.py` (new)
- Redis — urgency window TTL keys

---

### 37. Deal-Size Capture
**Status:** NOT BUILT
**Priority:** #12

**What to build:**
- After subscriber confirms a deal win: one-tap deal-size capture
- Options: $5–10K / $10–25K / $25K+ / Skip
- Feeds: revenue signal score, annual push triggers ($10K+ → immediate annual offer), attribution
- `deal_outcomes` table (see item #40)

**Files to create/modify:**
- `src/api/main.py` — deal capture endpoint
- `src/core/models.py` — DealOutcome model

---

### 38. Retention Summaries
**Status:** NOT BUILT
**Priority:** #10

**What to build:**
- Tier-specific value summaries sent periodically:
  - **Wallet users:** "You unlocked 12 leads worth $47K in potential jobs this month"
  - **Lock holders:** "Your exclusive territory generated 34 leads — 8 marked as high-urgency"
  - **AutoPilot users:** "Auto Mode sent 23 first-texts and booked 3 callbacks this week"
- "What you would have missed" for at-risk users (5–7 days inactive):
  - "While you were away, 6 new distressed properties appeared in 33613"
- Deal-win → immediate ROI upsell: "You just closed $15K from a Forced Action lead — lock your ZIP to get every lead exclusively"

**Files to create/modify:**
- `src/tasks/retention_summaries.py` (new)
- Email/SMS templates per tier

---

### 39. Message Outcomes, Deal Outcomes, Learning Card, Referral Events Schemas
**Status:** NOT BUILT
**Priority:** #4 (ground truth for all learning — "useless if delayed")

**What to build:**
- **message_outcomes table:**
  ```
  message_outcomes:
    id, subscriber_id, message_type (sms/email/voice),
    template_id, variant_id, channel,
    sent_at, delivered_at, opened_at, clicked_at, replied_at,
    conversion_type (unlock/wallet/lock/annual/none),
    conversion_within_4h, conversion_within_24h, conversion_within_48h,
    revenue_attributed, created_at
  ```
- **deal_outcomes table:**
  ```
  deal_outcomes:
    id, subscriber_id, property_id, lead_id,
    deal_size_bucket (5-10k/10-25k/25k+/skip),
    deal_amount, deal_date, lead_source,
    days_to_close, created_at
  ```
- **learning_cards table:** (see item #10)
- **referral_events table:** (see item #33)
- All tables must exist and log from Day 1 — schema designed now, populated as events occur

**Files to create/modify:**
- `src/core/models.py` — MessageOutcome, DealOutcome, LearningCard, ReferralEvent models
- Alembic migration for all 4 tables

---

### 40. Launch Compliance Baseline
**Status:** NOT BUILT
**Priority:** #2

**What to build:**
- **TCPA compliance:** Proper opt-in tracking for all SMS outbound. Express written consent via signup flow.
- **10DLC registration:** Twilio A2P 10DLC campaign registration (lead time: 2–4 weeks)
- **STOP/opt-out:** Immediate suppression on STOP keyword (ties to item #16)
- **DNC list check:** Before outbound SMS, check against internal suppression list
- **CAN-SPAM:** Email unsubscribe links (already partially handled in email templates)
- **DBPR compliance:** License verification before sending contractor-specific content

**Files to create/modify:**
- `src/services/sms_compliance.py` (new) — TCPA, DNC, opt-in tracking
- `src/core/models.py` — consent tracking fields
- Twilio 10DLC registration (external process, not code)

---

## SUMMARY BY BUILD PRIORITY

| Priority | Items | Category |
|---|---|---|
| #1 | Proof moment + paid unlock + Payment Sheet + monetization wall + abandonment pressure + default card save | First money |
| #2 | Stripe idempotency (DONE) + STOP/DLQ core (DONE) + compliance + LangGraph + Redis | Protect the business |
| #3 | Wallet auto-enrollment + auto-reload + accelerated push | Recurring revenue |
| #4 | message_outcomes + deal_outcomes + learning_cards + referral_events schemas | Ground truth |
| #5 | Segmentation + revenue signal score + Cora guardrails | Right message, right user |
| #6 | SMS commands + new lead hold + storm pack + urgency + missed-call + NWS + allotments + bonus credits | Impulse revenue |
| #7 | FOMO + flash scarcity + Cora conversational close | Lock conversion engine |
| #8 | AP Lite + Auto Mode + referral 60-sec + milestone + Claude routing (DONE) | ARPU growth |
| #9 | Lock + annual push + proactive save + Stripe failure recovery + Data-Only tier | Recurring MRR + churn |
| #10 | Retention summaries + voice drop + "what you missed" + A/B testing | Stickiness |
| #11 | Full attribution + LangSmith + Learning Card Sunday job | Compound intelligence |
| #12 | Gate monitoring + county launch + vendor monitoring + Revenue Pulse + deal-size | Expansion infra |

---

## INFRASTRUCTURE REQUIRED (Pre-Stage 1)

1. **Redis instance** — DigitalOcean managed Redis or self-hosted in Docker
2. **PGMQ setup** — Tembo PGMQ extension on existing Postgres
3. **LangGraph deployment** — DigitalOcean Docker or AWS ECS (2x 4-core min)
4. **LangSmith account** — for tracing and debugging Cora decisions
5. **Twilio 10DLC** — register A2P campaign (2–4 week lead time — start NOW)
6. **Synthflow inbound** — configure missed-call webhook
7. **NWS API** — register for weather alert webhook on county FIPS codes
8. **Stripe Payment Sheet** — configure for Apple Pay / Google Pay
9. **Prometheus + Grafana** — monitoring stack for Cora and infrastructure metrics
