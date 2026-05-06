# FA Phase 2B v9 FINAL — Developer Orientation

Source: `FA-2B-v9-FINAL (1).docx` (April 2026, supersedes v7/v8)
Cross-referenced against: `docs/2B-PENDING-IMPLEMENTATION.md` (state at 2026-04-22)

---

## 1. What this document actually is

A **commercial architecture spec**, not a product spec. It defines the monetization engine, Cora (the autonomous revenue operator), guardrails, conversion funnel math, and a 3-wave build sequence to reach **$100K MRR in 9–14 weeks** (base case). v9 is the successor to v7/v8 — the canonical implementation target.

**One-line mission:** *Monetize intent earlier, convert spend into defaults faster, let Cora change offers, routing, and automation based on behavior.*

**Core stack it assumes:**
- Claude API (Haiku/Sonnet/Opus routing + prompt caching + batch)
- LangGraph supervisor on DigitalOcean Docker
- PostgreSQL 16 + pgvector + RLS + PgBouncer + PGMQ
- Redis (Pub/Sub, rate limits, counters, TTLs, dead-letter queue) — **not** LISTEN/NOTIFY
- Stripe (idempotent webhooks, Payment Sheet, metered + subscription)
- Twilio (A2P 10DLC) + Synthflow (voice) + NWS webhook
- n8n for monitoring/automation, LangSmith + Prometheus + Grafana for ops

---

## 2. What's already built vs. what this spec still requires

From `docs/2B-PENDING-IMPLEMENTATION.md` (state at 2026-04-22):
- **33 of 41 items complete** (runtime services, webhooks, cron jobs)
- **6 partial** (Auto Mode, Redis infra, landing page, DBPR, Cora SMS close, compliance)
- **4 not built** — all LangGraph-dependent: **LangGraph Supervisor, FOMO Engine, Cora Conversational Close, Abandonment Pressure SMS, Retention Summaries**

### Already done (matches v9 spec)

Revenue ladder config · wallet engine ($49/99/199) · 8-bucket segmentation · revenue signal score · proof moment · monetization wall backend · payment sheet · new lead hold · TCPA/STOP/DLQ · A/B engine · bundles (Weekend/Storm/ZIP Booster/Monthly Reload) · free allotment · urgency engine · referral core loop · NWS webhook · 10 SMS commands · saved-card bonus · annual push cron · data-only save cron · revenue pulse (daily/weekly) · missed-call auto-signup · learning card Sunday cron · deal-size capture · accelerated wallet push · 137 new tests

### v9 adds/tightens that are NOT yet built

1. **Cora Guardrail Ranges** as runtime config + enforcement (price ranges, discount caps, rollback triggers) — config exists but enforcement hooks into A/B, pricing, urgency windows aren't wired
2. **Cora Conversational Lock Close** — Sonnet contextual SMS with live ZIP data, lead counts, competitor activity, revenue signal score
3. **Synthflow outbound voice drop** — only inbound missed-call exists
4. **Stripe Failed Payment Recovery** 3-step SMS (Day 1/3/5 → downgrade to $97)
5. **First-session monetization wall frontend** — countdown timer (backend exists)
6. **Abandonment Pressure SMS** — 10–15 min no-payment trigger
7. **Gate Monitoring via n8n** (6-hr poll + Slack 1-tap) — 7 expansion gates defined, not polled
8. **Vendor cost monitoring** n8n job
9. **Paid ad creative automation** (Haiku-generated, 1-tap approval)
10. **County launch sequence** (T+0/+2/+4/+6/+24 cascade)
11. **Self-healing ops** — auto-incident response when first-payment <25% for 48h
12. **Kill-switch automation** — Green/Yellow/Red scoring injected into Monday pulse
13. **Thin human backup closer routing** (GHL/Slack/SMS for score >85 + 3+ interactions, $397+ deals)
14. **Annual auto-switch at Day 60** — cron exists; needs Stripe subscription swap via webhook trigger confirmed
15. **Dynamic script mutation** (retire lowest of 3 after 200 sends, generate replacement)
16. **Predictive churn scoring** nightly job
17. **Trade + county pricing cohorts** (Stage 10, after 6+ wks data)
18. **Expansion ICP channels** (REI, insurance, hard-money, PMs, attorneys, title, bankruptcy alert, white-label) — gated on $50K MRR
19. **Frontend (React) components** across the funnel
20. **Twilio A2P 10DLC brand/campaign submission** — 2–4 wk lead time, **critical path**

---

## 3. Why each block matters (the business logic)

### Revenue ladder (12 steps)

Free → Proof Moment → Paid Unlock ($2.50–7) → Wallet (auto) → Auto Mode → Territory Lock ($197) → AP Lite ($299) → AP Pro ($497) → Annual ($1,970/yr) → Data-Only save ($97) → Partner ($2,000) → White-Label ($2.5–5K). Each step has an explicit **trigger** and **sold-by** (product vs. Cora vs. automated). Everything below paid unlock is scaffolding for the rest.

### Conversion funnel math (the $100K stack)

Base case demands:

| Step | Strong | Base | Down | Action if weak |
|---|---|---|---|---|
| Signup → First Payment | 35–40% | 30% | 15–20% | Simplify proof, increase urgency, cut friction |
| Payer → Saved Card | 80% | 70% | 50% | Default card save, bonus credits, fewer steps |
| Saved Card → Wallet | 25% | 15% | 8% | Trigger wallet sooner, "missing leads" frame |
| Free → Lock (60 days) | 8–10% | 5% | 2–3% | Live-data close, urgency windows, voice drop |
| Lock → AP Lite | 40% | 33% | 20% | Show manual action count, time-savings frame |
| Lock → Annual | 20% | 14% | 8% | Deal-win annual push, stronger ROI frame |
| 30-Day Retention | 80% | 70% | 55% | Earlier save flows, missed-opportunity summaries |
| Free-tier cost ratio | ≤30% | ≤40% | ≤50% | Tighten free cap, move payment wall earlier |

If signup→first-payment lands at 15% instead of 30%, Week 14 MRR drops from ~$100K to ~$55K. **This single rate is the biggest assumption.**

### Cora Guardrails (runtime bounds, not prompts)

| Decision | Allowed Range | Rollback Trigger |
|---|---|---|
| Lock pricing | $147–$247/mo | Conv rate drops >2 std devs vs control for 48 hrs |
| Wallet tier pricing | $39–$249/mo | Same |
| Bundle pricing | ±25% of base | Margin drops below 60% |
| Discount max | 20% off list | Never exceed |
| Credit bonus max | 10 credits | Never exceed per event |
| A/B traffic cap | 10% of segment | Auto-rollback if losing variant >2 std devs |
| Message variant swap | Retire lowest of 3 after 200 sends | New variant must beat retired within 200 sends or revert |
| Urgency window | 10–60 minutes | Never shorter than 10 min |
| Save offer | $97 Data-Only or 60-day pause | No lower offers without approval |
| Annual discount | 2 months free ($1,970/yr) max | No deeper |
| Auto-reload threshold | <5 credits | Never change without approval |
| Paid acq spend | $500–$2,000/wk per channel | Pause if CAC >$25 for 7 days |
| County activation | Only when all 7 gates green | Never override |

### Kill-switch discipline

Every feature gets a 4-week window. 7 red days after adjustment → kill/pivot. Nine metrics tracked weekly: first-payment rate, saved-card rate, wallet adoption, lock conv, 30-day retention, SMS reply rate, CAC, free-tier cost ratio, Twilio cost/signup.

### Expansion gates (7 hard gates)

1. First-payment rate ≥30% of free users within 30 days
2. Saved-card rate ≥70% of payers within 7 days
3. Wallet adoption ≥15% of saved-card users within 30 days
4. Lock conversion ≥5% of free users within 60 days
5. 30-day payer retention ≥70%
6. Free-tier cost ratio ≤40% of revenue
7. County profitability net positive monthly

All gates must be green before expansion ICP channels activate (**≥$50K contractor MRR required**). Cora cannot override.

---

## 4. Three-wave build sequence

| Wave | Weeks | Goal | What ships |
|---|---|---|---|
| **1: Prove Paid Intent** | 1–3 | $0 → $10K MRR | Stages 1+2+3 **in parallel** — proof + unlock + card save + wallet + monetization wall + abandonment pressure + NWS + Stripe idempotency + STOP DLQ + referral loop + learning card schema. Launch end of Wk 2. |
| **2: Lock Revenue** | 3–7 | $10K → $50K MRR | Stage 6 (Lock + AP Lite + FOMO + Flash Scarcity + conversational close + voice drop + Stripe failure recovery + gate monitoring + human backup closer) → Stage 5 (Premium + AP Pro + bundles + Auto Mode + annual at deal-win + referral team + 60-day auto-switch). Paid ads activate IF first-payment ≥30%. |
| **3: Compound + Expand** | 5–11+ | $50K → $100K+ MRR | Stage 8 attribution · Stage 7 autonomous acquisition · Stage 9–10 compliance + A/B + self-healing + predictive churn + script mutation + cohort pricing · Stage 11 expansion · Stages 12–13 white-label + bankruptcy alert + benchmarks + supplier intelligence. |

---

## 5. Infrastructure requirements

Already scaffolded: PostgreSQL, Stripe (idempotent + Payment Sheet), Redis client (graceful degradation, **no prod `REDIS_URL`**), Twilio inbound, NWS webhook, `message_outcomes` + `deal_outcomes` + `learning_cards` + `referral_events` tables, LangSmith/Prometheus placeholders.

Still needs provisioning:

- Redis server in prod (blocks urgency windows, allotments, lead holds, storm flags, wall sessions, saved-card bonus)
- LangGraph supervisor on DO Docker with `langgraph-checkpoint-postgres`
- PGMQ queuing
- Synthflow outbound integration
- n8n workflows (vendor costs, ad creative, gate poll, county launch, leaderboard, kill-switch)
- Prometheus + Grafana dashboards + alertmanager

---

## 6. Database signal tables mandated from Day 1

- `message_outcomes` — every SMS with conversion attribution (4h/24h/48h windows)
- `deal_outcomes` — one-tap deal-size capture (<$10K / $10–25K / $25K+ / skip)
- `learning_cards` — Sunday midnight aggregation, read at start of every Cora decision tree
- `referral_events` — chain tracking + milestone counters (1 / 3 / 5 refs)
- `processed_events` — Stripe webhook dedup (idempotency key)
- STOP dead-letter queue — Redis with manual review fallback

All four status-surface tables exist per the implementation report.

---

## 7. The 52 open questions for Hari

The doc ends with 52 unanswered engineering questions — **do not start Wave 2 work without answers**. Split:

- **27 architecture** — LangGraph, PGMQ, prompt caching, Stripe metered/Payment Sheet, Redis patterns, 10DLC
- **9 timing** — hours/week, parallel dependencies, payment structure, DO vs AWS
- **16 v9-specific** — Sonnet sync vs batch, Synthflow outbound wiring, Redis referral chain, learning_cards store, script mutation engine, alertmanager routing, n8n vendor APIs, Slack interactive components, wall TTL enforcement, abandonment trigger pattern, county landing page gen, annual switch webhook vs cron, closer routing path, kill-switch aggregation

---

## 8. Documentation to produce

1. **Delta analysis** — map each of the 41 items in `2B-PENDING-IMPLEMENTATION.md` onto v9 section numbers (this doc is the diff).
2. **Guardrail enforcement matrix** — for each Cora autonomous decision, which service/module enforces the bound and where the rollback trigger fires.
3. **Funnel instrumentation plan** — every conversion rate in §3 must have a query + Revenue Pulse line + Slack alert on Red threshold.
4. **Cron inventory** — 5 cron jobs wired, 4+ more needed (gate monitor 6h, vendor cost daily, ad creative weekly, script mutation, churn prediction nightly, kill-switch weekly).
5. **Webhook inventory** — Twilio inbound/voice, NWS, Stripe (payment_intent.succeeded, invoice.payment_failed, subscription.updated), Synthflow inbound/outbound, Slack interactive.
6. **Frontend spec** — monetization wall countdown, proof moment UI, Payment Sheet integration, deal-capture form, bundle CTAs, FOMO indicators, referral share.
7. **Test plan** — ≥472 tests currently passing; v9 additions should target >550 with guardrail boundary tests specifically.
8. **Launch critical path** — A2P 10DLC (2–4 wk), Redis provisioning (1 day), iubenda (client), Apple Pay domain verification, NWS subscription.

---

## 9. Single biggest risk

The spec's financial model rests on a **30% free→paid conversion assumption that is ~10× industry benchmark for cold outreach**. If it lands at 15%, the timeline slips 3–5 weeks and base-case MRR halves. The entire Wave 1 (proof moment + monetization wall + abandonment pressure + accelerated saved-card push) exists to beat that benchmark. Instrument from Day 1, track daily — rollback triggers are defined (`Week 3 → revert to standard free allotment if <25%`).

---

# 10. Complete Example Flow — One User, End to End

This traces a real user from DBPR list to annual upgrade, showing every system component, the database tables they write, and the Cora decisions at each step. Times are wall-clock from first contact.

## T+0: Outbound acquisition

**Who:** Mike, a roofer in Tampa (ZIP 33647), licensed in DBPR.

**What happens:**
1. DBPR monthly refresh cron ingests 67 counties of licensed contractors → Postgres `contractors_outbound` table.
2. LangGraph supervisor routes Mike to the `roofing` trade sequence. Claude Haiku generates a cold SMS using the winning variant from `message_outcomes` (chosen by Thompson sampling within the 10% A/B cap).
3. Twilio sends: *"Mike — 3 new foreclosures in 33647 this week. Reply YES for a free look."*
4. Row written: `message_outcomes(subscriber_id=null, phone, variant_id, sent_at, campaign='dbpr_roofing_v4')`.

**Services touched:** DBPR loader, Cora supervisor (LangGraph), Claude Haiku router, A/B engine, Twilio outbound, compliance pre-check (opt-in state).

## T+5 min: Reply → Signup

Mike replies `YES`.

1. Twilio inbound webhook hits `/webhooks/twilio/inbound`. Signature verified.
2. Compliance layer records TCPA opt-in in `sms_opt_ins` (keyword=YES, timestamp, exact prompt text stored).
3. Subscriber created: free tier, `feed_uuid` generated, `has_saved_card=false`.
4. Segmentation engine classifies Mike → bucket=`new`. Revenue signal score = 5 (baseline).
5. Welcome SMS with dashboard link (signed JWT) + STOP reminder.
6. Referral code minted (base36 of subscriber_id, 8-char padded).

**Row updates:** `subscribers`, `sms_opt_ins`, `message_outcomes.outcome='converted_to_signup'`.

## T+6 min: Proof Moment

Mike taps the dashboard link on his phone.

1. `GET /api/proof-leads?feed_uuid=...` runs one query against already-scored `distress_scores` filtered to roofing vertical + nearby ZIPs.
2. Response: **1 fully revealed** (owner, phone, address, urgency score 87, foreclosure + storm damage) + **2 blurred** (street name masked, score visible).
3. Frontend renders the three cards. The two blurred ones have a *"Unlock for $4"* CTA.
4. Simultaneously, `POST /api/wall/session` creates a 24h monetization-wall session in Redis (`wall:sess:{sid}` with 15-min countdown TTL). ROI frame loaded: roofing avg job value $8.5K, avg monthly revenue $34K.

**Services touched:** Proof moment, monetization wall, Redis.

## T+7 min: Hover, no action

Mike stares at the blurred leads. The countdown ticks. Frontend polls `GET /api/wall/{session_id}` every 30 sec. Nothing happens.

## T+12 min: Abandonment Pressure (v9 new — not yet built)

Redis TTL on `wall:abandon:{sid}` expires at 10 min. n8n picks up the expiry event (or LangGraph scheduler).

1. Cora supervisor reads Mike's session state (clicked proof, no payment), plus live ZIP data (2 other contractors viewing this ZIP right now).
2. Haiku generates abandonment SMS: *"Mike — 2 roofers are looking at 33647 right now. The Smith Dr lead expires in 30 min. Unlock: [link]"*
3. Twilio sends. `message_outcomes` row written with `campaign='abandonment_wave1'`.

## T+13 min: First Payment (Proof → Paid Unlock)

Mike taps the link. Stripe Payment Sheet opens (Apple Pay pre-filled).

1. `POST /api/payment-intent` creates a PaymentIntent with `amount=400`, `setup_future_usage='off_session'` (card-save default).
2. Mike confirms. Apple Pay completes in 3 seconds.
3. Stripe webhook `payment_intent.succeeded` fires → `/webhooks/stripe` → idempotency check on `processed_events.event_id` → recorded → credits Mike's account with 1 unlock → flips `has_saved_card=true` → stores payment method ID.
4. Redis key `saved_card_bonus:{subscriber_id}` set with 10-min TTL.
5. Unlock revealed: owner name, phone, age-of-distress, exact address.
6. Monetization wall session flips `converted=true`.
7. Revenue signal score recomputes: 5 → 28 (spend velocity +15, engagement +8).
8. Segmentation bucket: `new` → `high_intent`.

**Funnel metric:** Signup → First Payment in 8 minutes. Contributes to the daily 30% target.

## T+18 min: Saved-Card Bonus triggers accelerated wallet push

Mike views another lead within the 10-min bonus window.

1. Credit balance check → 0 unlocks remaining. Redis bonus key still live.
2. Wallet engine detects: saved-card + repeated ZIP interest + 70%-of-typical-wallet-usage pattern → **Accelerated Wallet Push** fires.
3. Cora (Haiku) sends: *"Mike — you've looked at 2 roofs in 33647 in 15 min. $49/mo gets you 20 unlocks. Your card is ready — reply WALLET to activate."*
4. Mike replies `WALLET`. SMS command dispatcher routes → wallet enrollment → Stripe subscription created at $49/mo → 2 bonus credits awarded (within the 10-min window).

**State:** Starter Wallet active. 22 credits. Segment → `wallet_active`. Score → 44.

## T+2 days: Wallet usage, urgency window

Mike burns through 14 unlocks in 2 days. Urgency engine sets a 30-min window each time he views a lead ("2 others viewing this ZIP"). ZIP sorted-set counter in Redis powers the FOMO display. Saved-card rate metric ticks up.

## T+4 days: New Lead Hold + "I Found You a Deal"

NWS CAP feed fires a Severe Thunderstorm warning for FIPS 12057.

1. `POST /webhooks/nws/alert` → `storm_active:33647` Redis key, 72h TTL.
2. Within 2 hours, scrapers surface 6 new storm-damage roofs in 33647.
3. Cora detects: Mike is wallet-active in this ZIP. Fires the *"I Found You a Deal"* SMS with 20-min reservation: *"Mike — storm hit 33647 last night. I'm holding a Gold lead for you for 20 min. [link]"*
4. Redis `lead_hold:{lead_id}={subscriber_id}` with 20-min TTL blocks other wallet users from this lead.
5. Storm Pack bundle ($39, 10 leads) also becomes available in affected ZIPs. Banner in dashboard.

Mike unlocks the lead. Hold released. Revenue signal score → 61.

## T+7 days: Charter Annual Push

Annual push cron runs at 8 AM UTC. Mike matches Trigger #1 (Day 7 charter cohort — first 50 users get an extra bonus).

1. Email + SMS: *"Mike — you're in the founding 50. Annual Lock is $1,970/yr instead of $2,364. 2 months free. Reply YEARLY."*
2. Mike ignores. Row written: `annual_push_log(subscriber_id, trigger='day_7_charter', accepted=false)`.

## T+14 days: Repeat ZIP behavior triggers Lock Close

Wallet engine counts: 40+ credits spent, 22 in ZIP 33647. Segmentation promotes Mike → `lock_candidate`.

**Cora Conversational Lock Close (v9 new, Sonnet, not yet built):**

1. LangGraph supervisor assembles context: live ZIP 33647 data (14 active leads, 3 Gold-tier, 2 other contractors viewing), Mike's spend rate ($67 in 14 days), revenue signal score (72), competitor timing (last lock sold in adjacent ZIP 2 days ago).
2. Sonnet (not Haiku — synchronous, $0.003/call per Q37) generates the close:

   *"Mike — you've spent $67 in 33647 this month. 3 Gold leads came up yesterday and I watched one get contacted by someone else. Territory Lock is $197/mo — exclusive access to every new lead in 33647, nobody else. One deal pays for a year. Reply LOCK."*

3. Message goes via Twilio. Stored in `message_outcomes` with `sent_by='cora_sonnet_close'`, variant_id.

## T+14 days + 2 min: No reply — Voice Drop

Revenue signal score 72 > voice drop threshold of 70. Mike hasn't replied in 2 min (or 48 hrs per the spec, depending on the tuned threshold).

1. LangGraph triggers Synthflow outbound (v9 new, not built) with a personalized 20-second voicemail script generated by Sonnet using the same context.
2. Synthflow drops VM (no ring — direct to voicemail). Follow-up SMS within 60 sec: *"Just left you a voicemail — check when you can."*

## T+14 days + 8 min: Lock converts

Mike replies `LOCK`. SMS dispatcher → Payment Sheet deep link → Apple Pay → Stripe subscription created at $197/mo → Stripe webhook → RLS policy update excludes all other wallet users from ZIP 33647.

**State:** Territory Lock active. Segment → `lock_holder`. Score → 85.

## T+30 days: AP Lite upsell

Mike has 34 manual skip-trace actions this week. AP Lite upsell fires (10+ manual actions threshold).

1. Cora: *"Mike — you've done 34 skip-traces this week. AP Lite does that automatically plus first-text and voicemail — $299/mo. Reply LITE."*
2. Mike converts. Stripe sub upgraded. Auto Mode flag flipped. `queue_action()` now queues real jobs (once LangGraph layer exists).

## T+45 days: Deal Win → Annual Push

Mike one-taps the Deal-Size Capture form: *$10–25K bucket, 18 days to close*.

1. `POST /api/deal-capture` writes `deal_outcomes(subscriber_id, bucket, amount, days_to_close, closed_at)`.
2. Trigger cascade:
   - Annual push re-fires at deal-win moment with ROI frame: *"Mike — $18K job just closed from a $4 lead. Annual Lock = 1 job pays for 10 years. [link]"*
   - Deal-win graphic generated (Claude HTML → image) → social proof wall.
3. Revenue signal score → 94. Already locked, so no further upsell. Score feeds back into Cora's trade-benchmark outputs.

Mike accepts annual. Stripe subscription updated (from $197/mo monthly → $1,970/yr), prorated credit. `subscription_events(old_plan='lock_monthly', new_plan='lock_annual')`.

## T+60 days: 60-Day Annual Auto-Switch (for users who didn't convert at deal-win)

For every other Lock holder who hasn't taken annual, the Day-60 cron fires the auto-switch offer. If no response → automated Stripe subscription modification at Day 60+7 (if consent gates allow).

## T+90 days: Intelligence compounds

1. **Sunday Learning Card job** (LangGraph, runs every Sunday midnight UTC) aggregates:
   - `message_outcomes` → best-performing variant per trade (roofing abandonment SMS converts 14%, retire the 4% variant after 200 sends).
   - `deal_outcomes` → average deal size per ZIP + trade.
   - A/B results → preliminary winner on the $197 vs $217 Lock price test.
   - Churn signals → 3 at-risk users flagged (5–7 days inactive).
2. Row written: `learning_cards(card_date, card_type, payload_json)`.
3. Cora reads this card at the start of every decision tree the following week.

## T+120 days: At-risk save

Mike goes 6 days without activity. Predictive churn scoring (nightly, not yet built) flags him at risk.

1. Cora sends: *"Mike — 7 Gold leads came through 33647 this week that you didn't touch. $97/mo Data-Only keeps you alerted without the full lock. Or pause 60 days — your choice."*
2. If Mike accepts `DATAONLY`, Stripe downgrade. If he engages without replying, flag cleared. If he replies `PAUSE`, 60-day subscription pause.

## T+continuous: Kill-switch monitoring

Every Monday, n8n (not yet built) aggregates Green/Yellow/Red scoring across 9 metrics:

- First-payment rate this week: 31% → GREEN
- Saved-card rate: 68% → YELLOW (adjust: stronger default framing)
- Lock conv: 4.1% → YELLOW
- etc.

Revenue Pulse Monday SMS to founder includes the scoring. If any metric stays RED for 7 days after adjustment → auto-pause channel/feature (kill-switch).

## T+continuous: Gate monitoring

n8n polls Postgres every 6 hours for the 7 expansion gates (not yet built). When all 7 green + contractor MRR ≥ $50K → Slack 1-tap: *"All gates green for Pinellas launch. Tap to fire."*

Josh taps → T+0 waitlist SMS wave, T+2 Clay enrichment, T+4 landing page auto-generated, T+6 Meta ads go live, T+24 first Revenue Pulse with new-county metrics.

---

## 11. Cora decision hierarchy (summary)

For every autonomous decision, Cora consults in order:

1. **Hard guardrails** (§3) — if the action is out of range, abort or escalate.
2. **Current Learning Card** (latest Sunday aggregate).
3. **Live Redis state** (ZIP activity, storm flags, urgency windows, lead holds, saved-card bonus).
4. **Subscriber segment + revenue signal score**.
5. **Active A/B variant** (capped at 10% traffic).
6. **Kill-switch color** for the feature being invoked (if RED → fall back to simpler version).

Any action outside these bounds surfaces in Revenue Pulse for Josh's one-tap approval.

---

## 12. TL;DR for engineering

- **Wave 1 backend is 80% done.** Frontend, LangGraph, and the conversational layer are the gap.
- **The monetization wall → abandonment pressure → accelerated wallet push → conversational lock close** chain is what makes or breaks the $100K target. Instrument it end-to-end from Day 1.
- **Cora is runtime-configured bounds + LangGraph decisions, not magic.** Every autonomous action has a numeric guardrail and a rollback trigger.
- **Four signal tables are the whole intelligence substrate** (`message_outcomes`, `deal_outcomes`, `learning_cards`, `referral_events`). They exist — just keep writing to them.
- **The 20 gaps in §2 are the build backlog.** Order by Wave (1 > 2 > 3) and by the Hari Priority ranking in the source doc.

---

# 13. Glossary / Component Reference

Each table below defines a class of components, what they do in the platform, what they connect to, their importance to the $100K target, how complex they are to build, and the tech stack they sit on. **Importance:** Critical = funnel-blocking · High = directly moves revenue · Med = optimization · Low = nice-to-have. **Complexity:** High = new system / agentic logic · Med = integration work · Low = config or SQL.

## 13.1 Revenue Ladder Products (the 12 things you can buy)

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| Free Signup | Zero-cost tier giving access to proof moment, 3 skips / 3 texts / 1 VM per week | Top of the funnel; feeds every other product | Critical | Low | Postgres, JWT feed_uuid |
| Proof Moment | 1 fully enriched lead + 2 blurred leads shown at signup | Demonstrates value in Session 1; gates the monetization wall | Critical | Low | Postgres (`distress_scores`), FastAPI |
| Paid Unlock ($2.50–$7) | One-tap Apple Pay to reveal a blurred lead | First dollar; flips `has_saved_card` | Critical | Med | Stripe Payment Sheet, Apple Pay, webhook |
| Wallet — Starter ($49/mo, 20cr) | Monthly credit subscription entry tier | Recurring revenue floor; unlocks bundles | Critical | Med | Stripe subscription, wallet engine, Redis balance |
| Wallet — Growth ($99/mo, 50cr) | Mid-tier wallet with Auto Mode included | Core ARPU driver | Critical | Med | Stripe subscription, wallet engine |
| Wallet — Power ($199/mo, 120cr) | Top wallet tier with Auto Mode included | Pre-lock step | High | Med | Stripe subscription, wallet engine |
| Auto Mode ($79–$99 or included) | Auto skip + first text + VM if no reply 24 hrs | Time-savings framing; upsell lever | High | High | LangGraph (pending), BatchSkip, Twilio |
| Territory Lock ($197/mo) | Exclusive ZIP access; all leads routed only to lock holder | Highest-margin recurring revenue; FOMO anchor | Critical | High | Postgres RLS, Stripe sub, ZIP counter |
| AutoPilot Lite ($299/mo) | Auto skip + text + VM + 3-touch + weekly summary | Post-lock ARPU growth | High | High | LangGraph, Twilio, Synthflow |
| AutoPilot Pro ($497/mo) | Lite + 5-touch + premium routing + appointment setting | Top of pyramid for power users | Med | High | LangGraph, full automation stack |
| Annual Lock ($1,970/yr) | Lock sub prepaid annually = 2 months free | Cash flow + churn prevention | High | Med | Stripe subscription swap, cron |
| Data-Only Save ($97/mo) | Property data feed only, no enrichment / auto-mode | Floor save offer before cancel | High | Low | Stripe downgrade, cron |
| Partner ($2,000/mo) | Multi-ZIP access for power users | High-value tier, low volume | Med | Low | Stripe, self-serve page |
| White-Label ($2.5–5K/mo) | Proptech platforms reselling branded distressed data | Expansion channel, Stage 12+ | Low | High | Multi-tenant, RLS, custom domains |
| Bankruptcy Alert Product ($297/mo) | Daily PACER filings → attorneys / investors | Expansion channel, Stage 12+ | Low | Med | PACER API, email delivery |
| Bundles (Weekend $19 / Storm $39 / ZIP Booster $29 / Monthly Reload $89) | One-time or recurring lead packs with context triggers | Impulse revenue, contextual urgency | High | Low | Stripe one-time, bundle expiry cron |

## 13.2 Monetization Triggers (the funnel pressure layer)

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| First-Session Monetization Wall | 15-min countdown + vertical-specific ROI frame on blurred leads | Forces payment decision in Session 1 | Critical | Med | Redis TTL, frontend countdown, FastAPI |
| Abandonment Pressure SMS | 10–15 min no-payment → single-CTA SMS; click-no-complete → scarcity | Recovers Session 1 abandoners | Critical | Med | Redis TTL expiry, LangGraph, Twilio |
| Accelerated Wallet Push | Saved-card users get immediate wallet offer on repeated usage | Saved-card → wallet rate (target 15%) | Critical | Low | Wallet engine, Redis, Twilio |
| Saved-Card Bonus Window | 10-min window after card save → +2 credits on next spend | Defaults card-save behavior | High | Low | Redis TTL, Stripe webhook |
| New Lead Hold ("I Found You a Deal") | 20-min Redis reservation on a Gold lead for a specific user | Personalized urgency; conversion to unlock | High | Low | Redis TTL, LangGraph, Twilio |
| Urgency Engine | Per-lead TTL window (10–60 min guardrail) + "X contractors viewing" ZIP counter | FOMO on every strong lead | Critical | Low | Redis sorted sets, frontend poll |
| Dynamic Flash Scarcity | Gold lead spike in non-locked ZIP → SMS to wallet users within 60 min | Event-driven conversion to lock | High | Med | Redis pub/sub, LangGraph, Twilio |
| FOMO Engine | Competitor acts on lead → SMS to non-locked user within 60 sec | Primary lock-conversion lever | Critical | High | Redis pub/sub, LangGraph (pending), Twilio |
| Proactive Save ("What You Would Have Missed") | 5–7 days inactive → offer Data-Only or PAUSE before cancel | Churn prevention | High | Med | Cron, Cora, Twilio |
| Annual Push Triggers | Day 7 charter / Day 10–14 / 2 deals / $250 spend / $10K+ deal-win / Day 60 auto-switch | Cash flow + churn prevention | High | Med | Daily cron, Stripe sub update |
| Deal-Win Annual Push | User confirms $10K+ job → immediate annual offer with ROI frame | Highest-conversion annual moment | High | Low | Deal-capture webhook, Cora |

## 13.3 Cora Intelligence Layer

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| LangGraph Supervisor | Agentic state machine that routes every Cora decision | Central brain for all autonomous actions | Critical | High | LangGraph, langgraph-checkpoint-postgres, DO Docker |
| Claude Router (Haiku / Sonnet / Opus) | Cost-aware model selection (target 75–80% Haiku) | Every AI call in the platform | Critical | Med | Anthropic SDK, LangSmith |
| Prompt Caching | cache_control on repeated system prompts (>80% hit target) | 30–40% API cost reduction | High | Low | Anthropic SDK |
| Cora Conversational Lock Close | Sonnet message using live ZIP data + lead counts + competitor activity + score | Primary lock-conversion message | Critical | High | Claude Sonnet, LangGraph, Redis live data |
| Cora SMS (outbound generic) | Context-aware messaging across all trade/county/behavior combinations | Every outbound proactive SMS | Critical | Med | Claude, Twilio, LangGraph |
| Synthflow Voice Drop | 20-sec personalized VM to high-intent non-converters (score >70, 48 hr no convert) | Two-channel conversion insurance | High | Med | Synthflow outbound API, LangGraph |
| Synthflow Missed-Call Signup | Inbound missed call → auto free-tier account + welcome SMS | Zero-friction acquisition path | Med | Low | Synthflow inbound webhook |
| Revenue Signal Score (0–100) | RFM-weighted score: spend / engagement / wallet / interaction / ZIP competition | Drives urgency, upsell timing, closer routing | Critical | Low | Postgres, scheduled recompute |
| 8-Bucket Segmentation | Classifier: churned, new, at_risk, wallet_active, high_intent, lock_candidate, engaged, browsing | Routes every message + offer | Critical | Low | Postgres, event-driven trigger |
| Event-Driven Re-Classification | Every significant action triggers immediate segment recomputation | Keeps segment fresh within minutes | High | Low | Postgres triggers or app-level hooks |
| A/B Offer Engine | Deterministic variant assignment, 10% traffic cap, 2σ rollback | Tests offers / copy / timing / pricing within guardrails | High | Med | Postgres, md5 hash, two-proportion z-test |
| Dynamic Script Mutation | Retire lowest-of-3 SMS variant after 200 sends; Haiku generates replacement | Self-improving copy loop | Med | High | LangGraph, Claude Haiku |
| Predictive Churn Scoring | Nightly score on behavioral signals; save offer 2–3 days before going inactive | Proactive retention | Med | High | Postgres, LangGraph nightly job |
| Thin Human Backup Closer | Commission-only closer receives Cora-flagged leads (score >85, 3+ interactions, $397+ deals) | Conversion insurance on hottest 1–2% | Med | Low | GHL / Slack / SMS routing |

## 13.4 Guardrails & Control Plane

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| Cora Guardrail Ranges | Runtime config: 13 numeric bounds on every autonomous decision | Every Cora action is gated here first | Critical | Low | YAML / Postgres config, enforced in services |
| Kill-Switch Discipline | 9 metrics scored Green/Yellow/Red weekly; 7 red days → auto-kill feature/channel | Prevents runaway losers | Critical | Med | n8n, Postgres aggregation, Slack |
| 7 Expansion Gates | Hard thresholds: first-payment, saved-card, wallet, lock conv, retention, cost ratio, county profit | Blocks all expansion until all green | Critical | Med | n8n 6-hr poll, Postgres, Slack 1-tap |
| Rollback Criteria | Per-feature success metric + deadline; revert to simpler version on miss | Kills v9 additions that don't pay | High | Low | Metric aggregation + feature flags |
| A/B Traffic Cap | Never more than 10% of segment in experiments | Prevents revenue loss from bad variants | High | Low | A/B engine config |
| Paid Acquisition Gate | Meta/Google ads blocked until first-payment rate ≥30% | Prevents spending on broken funnel | Critical | Low | Config flag + gate monitor |
| ICP Expansion Gate | Contractor base must reach ≥$50K MRR before any expansion ICP launches | Prevents premature dilution | High | Low | Metric check + config flag |

## 13.5 Signal Tables (the intelligence substrate — Day 1 non-negotiable)

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| `message_outcomes` | Every SMS logged with variant, campaign, 4h/24h/48h conversion attribution | Ground truth for A/B + Cora learning | Critical | Low | Postgres |
| `deal_outcomes` | One-tap deal-size capture: bucket, amount, days-to-close | Drives annual push, cohort pricing, benchmarks | Critical | Low | Postgres + FastAPI endpoint |
| `learning_cards` | Sunday-midnight aggregated summary of the week's outcomes | Read at start of every Cora decision tree | Critical | Med | Postgres, LangGraph Sunday job |
| `referral_events` | Referral chain state: pending → confirmed → rewarded | Milestone escalation, leaderboard | High | Low | Postgres + Redis sorted set |
| `processed_events` | Stripe webhook dedup by event_id | Webhook idempotency (non-negotiable) | Critical | Low | Postgres unique constraint |
| STOP Dead-Letter Queue | Opt-out keywords captured; failed opt-outs surface for manual review | TCPA compliance guarantee | Critical | Low | Redis list + admin endpoint |
| `sms_opt_ins` | TCPA double-opt-in record with exact prompt + keyword + timestamp | Legal shield on every proactive SMS | Critical | Low | Postgres |

## 13.6 Communications Infrastructure

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| Twilio Inbound Webhook | Signature-verified webhook for STOP + product commands + YES opt-in | Every inbound SMS | Critical | Low | Twilio, FastAPI |
| Twilio Outbound | All proactive SMS sending | Every Cora decision that messages a user | Critical | Low | Twilio, compliance gate |
| Twilio A2P 10DLC | Brand + campaign registration for deliverability | Required before any scale outbound | Critical | Low (process-wise) | Twilio console submission — **2–4 wk lead time** |
| SMS Product Commands | LOCK, BOOST, AUTO ON/OFF, PAUSE, BALANCE, TOPUP, REPORT, YEARLY, SAVE CARD | Self-serve actions by SMS | High | Low | Twilio inbound, keyword dispatcher |
| NWS CAP Webhook | National Weather Service alert → storm flag + scraper trigger + Storm Pack | Event-driven monetization on weather | High | Low | NWS ATOM feed, FastAPI webhook, Redis TTL |
| Stripe Webhooks | payment_intent.succeeded, invoice.payment_failed, subscription.updated | All revenue events | Critical | Med | Stripe, idempotent handler |
| Stripe Failed Payment Recovery | Day 1 soft / Day 3 urgency / Day 5 downgrade to $97 | Payment recovery pipeline | High | Med | Stripe webhook → scheduled SMS |
| Annual Auto-Switch at Day 60 | Automated Stripe subscription swap to annual at the 60-day mark | Opt-out annual capture | High | Med | Cron, Stripe sub update |
| DBPR Outbound | 67-county licensed contractor list monthly refresh | Top of funnel acquisition | High | Med | DBPR loader, Clay enrichment, Instantly |

## 13.7 Revenue / Acquisition Automation

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| Referral Core Loop | 60-sec notification via Redis; milestone rewards at 1 / 3 / 5 refs | Organic acquisition compounding | High | Low | Redis sorted set, Twilio, Postgres |
| Referral Team Mechanic | 3 refs in same county + trade → shared ZIP view | Team-based acquisition multiplier | Med | Med | Postgres, Redis |
| Paid Ad Creative Automation | Weekly Haiku-generated ad script from top converting ZIPs/trades → 1-tap approval | Autonomous paid acquisition | Med | Med | n8n, Claude Haiku, Slack interactive |
| County Launch Sequence | T+0 waitlist SMS → T+2 Clay → T+4 landing page → T+6 ads → T+24 pulse | Autonomous county activation | Med | High | n8n, gate monitor, Clay, Claude HTML gen |
| Waitlist Reactivation Waves | Reference wait time + scarcity at county launch | Converts waitlist → paying users | Med | Low | SMS campaign, Postgres |

## 13.8 Ops & Monitoring

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| Revenue Pulse (Daily SMS) | Founder gets one action, one alert, one learning each morning | Primary ops surface — 15 min/wk founder time | Critical | Low | Cron, Twilio, Postgres query |
| Revenue Pulse (Weekly Monday) | Week-summary pulse with kill-switch scoring | Weekly review artifact | High | Low | Cron, Twilio, n8n aggregation |
| Gate Monitoring | n8n polls 7 gates every 6 hrs → Slack 1-tap when all green | Triggers county launch without founder | High | Med | n8n, Postgres, Slack interactive |
| Vendor Cost Monitoring | Daily n8n job tracking Twilio / Stripe / DO / Claude API spend | Auto-pause on cost anomaly | Med | Med | n8n, vendor APIs |
| LangSmith | Per-agent cost tracking + model routing optimization | Cost management; proves Haiku/Sonnet ratio | High | Low | LangSmith SaaS |
| Prometheus + Grafana | Metric collection + dashboards | Ops visibility | High | Med | Prometheus, Grafana, alertmanager |
| Self-Healing Ops | Auto-pause variant or feature when metric breaches threshold | Reduces founder ops time | Med | High | alertmanager → n8n → Postgres / LangGraph |
| Kill-Switch Metric Automation | n8n weekly aggregator injects Green/Yellow/Red into Monday pulse | Automates kill/pivot decisions | High | Med | n8n, Postgres, Twilio |

## 13.9 Core Data & Infrastructure

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| PostgreSQL 16 + pgvector | Primary database with vector search | Everything (leads, subs, outcomes, embeddings) | Critical | Low | Postgres, pgvector |
| RLS (Row-Level Security) | Tenant isolation; Lock holder ZIP exclusivity | Data isolation + territory lock enforcement | Critical | Med | Postgres RLS policies |
| PgBouncer | Connection pooler | Handles concurrent FastAPI + LangGraph + cron workers | High | Low | PgBouncer |
| PGMQ (Tembo) | Postgres-native message queue | Async job queueing without Redis fragility | High | Med | Postgres PGMQ extension |
| Redis | Pub/Sub, TTL counters, sessions, rate limits, DLQ | Urgency, allotments, holds, storm flags, wall sessions | Critical | Low | Redis |
| GoHighLevel (GHL) | CRM for pipeline sync, human closer routing | Closer queue, subscription events, drip campaigns |  High | Med | GHL API, pipeline/stage IDs |
| Scrapers (Playwright / Puppeteer) | Daily county data pulls with proxy rotation + stale detection | Raw distress data for scoring | Critical | High | Playwright, Oxylabs, YAML config |
| CDS Engine | Scores 6 verticals × 14 signals into lead tiers (Platinum / Gold / etc.) | Every lead shown anywhere | Critical | Med | Python, Postgres, `config/scoring.py` |

## 13.10 Compliance & Legal

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| TCPA Double Opt-In | Confirmed YES/START reply before any proactive marketing SMS | Legal shield | Critical | Low | Postgres `sms_opt_ins`, compliance gate |
| STOP Keyword Handling | STOP / STOPALL / UNSUBSCRIBE / QUIT → immediate opt-out + DLQ on failure | Legal requirement | Critical | Low | Twilio inbound, Redis DLQ |
| DNC List Check | State + federal Do-Not-Call registry blocking | Legal shield | Critical | Low | Postgres import + lookup |
| iubenda (TOS + Privacy) | Legal copy + cookie banner | Launch requirement | High | Low | iubenda widget (client action) |
| Apple Pay Domain Verification | Required for Payment Sheet Apple Pay | Payment Sheet activation | High | Low | Stripe Dashboard setup |

## 13.11 Expansion / Future Channels (all gated on ≥$50K contractor MRR)

| Item | What it is | Used for / with what | Importance | Complexity | Tech stack |
|---|---|---|---|---|---|
| REI Investor channel | Distressed + bankruptcy properties feed @ $197/mo | Expansion ICP #1 | Med | Low | Existing data, new landing page |
| Insurance Adjuster channel | High-damage post-storm property lists @ $97/mo | Expansion ICP #2 | Med | Low | NWS storm flags, new feed |
| Hard Money Lender channel | Distressed deal flow pipeline @ $397/mo | Expansion ICP #3 | Med | Low | Existing data, new feed |
| Property Manager channel | Vacancy + distressed rental data @ $197/mo | Expansion ICP #4 | Low | Low | Existing data |
| Attorney (Bankruptcy) channel | Bankruptcy estate lead gen @ $197/mo | Expansion ICP #5 | Low | Low | PACER data |
| Title Company channel | Distressed title + lien alerts @ $97/mo | Expansion ICP #6 | Low | Low | Existing data |
| Contractor Benchmark Reports | Peer benchmarks for contractors; drives AP upsell | Stickiness + upsell lever | Low | Med | Aggregated deal_outcomes |
| Supplier Intelligence | Supplier-facing product @ $500–1,500/mo | Stage 13 channel | Low | High | Aggregated data, B2B sales |
| Public Market Report (quarterly) | Brand-building market report | Moat / PR | Low | Low | Aggregated data, LLM draft |

