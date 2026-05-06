# LangGraph in the Platform — Role, Graphs, and Tools

**Purpose:** Explain what LangGraph does in the Forced Action platform, which decision graphs we need, what tools those graphs call, and where the boundaries sit between the LangGraph process and the existing FastAPI process. This is the "what" document — the architecture doc comes next.

---

## Key Concepts — Cora, Monetization Wall, FOMO

Three terms used heavily throughout this document and the v9 spec. Read these first before the numbered sections.

### Cora

Cora is the name given to the **autonomous revenue operator** in this platform. She is not a person and not a single service — she is the collective intelligence layer responsible for every proactive commercial decision the platform makes without founder involvement.

Cora is composed of:

- **Brain** — the LangGraph supervisor and all sub-graphs
- **Hands** — the tool layer (thin wrappers over the platform's existing services)
- **Senses** — read tools, live Redis state, Postgres queries
- **Memory** — the weekly `learning_cards`, `message_outcomes`, and `deal_outcomes` tables
- **Voice** — Claude Haiku, Sonnet, and Opus routed by task through the Claude router

Cora composes and sends every proactive message, makes autonomous decisions inside the guardrail ranges, runs A/B tests, retires losing variants, and escalates anything outside her bounds to the founder through the Revenue Pulse. The v9 spec targets a founder workload of roughly 15 minutes per week — Cora is what makes that number possible.

### Monetization Wall

The monetization wall is the mechanism that **forces every free user to reach a payment moment during their first session**. When a new user lands on the dashboard:

1. A 24-hour session opens with a **15-minute countdown** displayed in the UI
2. Three real leads are shown — one fully revealed and two blurred, each blurred lead wearing an "Unlock for $X" CTA
3. A **vertical-specific ROI frame** appears alongside the leads (for roofers: "avg job $8.5K · typical monthly revenue: $34K")
4. A **live qualified-lead count** for the user's ZIP and trade adds credibility
5. If the user pays, the session flips to converted and the wall disappears
6. If the user does not pay, the Abandonment Pressure SMS fires at 10–15 minutes

It is called a "wall" because it is structured to make "maybe later" hard. The user is not blocked from the app — they just cannot extract the thing they came for (owner contact on a real lead) without paying. This is the single biggest lever in the v9 plan for hitting the 30% signup-to-first-payment target against the 2–5% SaaS industry benchmark.

### FOMO

FOMO (fear of missing out) is both a **concept threading through multiple features** and a **specific LangGraph graph**.

As a concept, FOMO shows up in:

- The "X contractors currently viewing this ZIP" counter (urgency engine)
- The 20-minute personalized lead hold ("I Found You a Deal")
- Short-lived urgency windows on every Gold lead (10–60 minutes, bounded by guardrail)
- Dynamic Flash Scarcity SMS on Gold lead spikes in non-locked ZIPs
- Lock-close messages that show live ZIP activity and competitor timing

As a specific graph, the **FOMO Engine** is the 60-second competitor reaction. When another subscriber contacts a Gold lead in a non-locked ZIP:

1. A Redis Pub/Sub event is raised the moment the competitor action fires
2. The graph identifies the next-best-fit subscriber for that ZIP and trade (highest revenue signal score in segment)
3. A Haiku-generated SMS is composed ("Another contractor just contacted a Gold lead in 33647. The Smith Dr lead is still open. [link]")
4. The message is dispatched within 60 seconds, under the compliance gate

The underlying logic is: the moment a competitor demonstrates the ZIP is hot, the platform uses that fact as live proof-of-value to convert the next-best-fit user into a lock.

### How They Connect

**Cora is the operator. The monetization wall is Cora's first move on every new user. FOMO is how Cora turns one contractor's action into another contractor's conversion.** These three are the spine of the commercial engine — everything else in this document describes the scaffolding that lets them run.

---

## 1. What LangGraph Does in the Platform

LangGraph is Cora's brain. Every **autonomous** decision the platform makes — not the user-initiated ones, but the ones the platform initiates on its own — runs through a LangGraph graph.

A user-initiated action (a contractor tapping "unlock lead," a Stripe payment webhook firing, an inbound STOP message) is handled synchronously by the FastAPI process the way it is today. LangGraph does not touch those paths.

A Cora-initiated action (a conversational lock close message, a FOMO nudge when a competitor acts, an abandonment SMS at 12 minutes of no payment, a weekly retention summary, an Auto Mode execution) starts with an event, enters the supervisor, and runs through a graph that:

1. **Assembles live context** — subscriber segment, revenue signal score, wallet state, ZIP activity, competitor timing, latest learning card, active A/B variant
2. **Checks the six-step decision hierarchy** — hard guardrails → current learning card → live Redis state → subscriber segment and score → active A/B variant → kill-switch colour for the feature
3. **Chooses an action** — compose a message, reserve a lead, flip a tier, offer a bundle, queue an auto action, send a voice drop, no-op
4. **Calls the right Claude model** — Haiku for classification and templated output, Sonnet for conversational depth with live context, Opus only for the rare reasoning-heavy task
5. **Passes through the compliance gate** — TCPA check, DNC check, rate-limit check — no message leaves without all three green
6. **Executes the action** — calls the same tools that FastAPI calls (send SMS, create PaymentIntent, update subscription, write a row)
7. **Writes its own outcome** — every fired action records to `message_outcomes`, `deal_outcomes`, or the agent audit log so the next Sunday's learning card can see what worked

The key property of LangGraph for this platform is **suspend and resume**. A graph that sends an SMS and then waits 10 minutes for the user to click-or-not is a single graph with a suspension point — not two separate systems with a shared database state. This matters because most of Cora's work is *conversational over time*, not *one-shot on a trigger*.

---

## 2. Why LangGraph Specifically

Three properties make LangGraph the right framework for this layer:

- **Stateful graphs with Postgres checkpointing.** Every decision's intermediate state is persisted. If the worker crashes mid-decision, the next worker picks up at the last checkpoint — no double-charge, no double-SMS, no orphaned state.
- **Human-readable flow.** A graph is a set of nodes and edges. Someone reading the code can see the entire decision flow visually. This matters for a system where the founder needs to audit *why* Cora made a particular call.
- **Native tool-calling and streaming.** Tools are Python functions with typed signatures. Claude's tool-use response is parsed into structured calls against those functions without boilerplate. Streaming lets us watch a decision unfold in LangSmith during development.

Alternatives we rejected: a pile of Celery tasks (no checkpointing, no conversational state), a bespoke state machine (reinventing every wheel LangGraph ships), plain function-chained agents (no suspend/resume, hard to audit).

---

## 3. The Decision Hierarchy Every Graph Enforces

Before any graph fires an action, it consults these six things in order. A "no" at any step either aborts the decision or escalates to a safer fallback:

| Step | Question | If no |
|---|---|---|
| 1. Hard guardrails | Is the proposed action within Cora's numeric bounds (price range, discount cap, urgency window, A/B traffic cap)? | Abort. Escalate via Revenue Pulse. |
| 2. Learning card | Does the latest Sunday learning card indicate this action is currently working? | Fall back to the safer previous-best action. |
| 3. Live Redis state | Is the user in a cooling-off window? Storm flag active? Lead already on hold? | Respect the state. Skip or delay. |
| 4. Subscriber segment and score | Does this segment/score actually warrant this action? | Swap to a more appropriate action for the segment. |
| 5. A/B variant | Which variant is this subscriber assigned to? | Use the assigned variant's copy/timing. |
| 6. Kill-switch colour | Is this feature RED right now? | Fall back to the simpler pre-v9 version of the message. |

This hierarchy is implemented as a **shared subgraph** — every top-level graph composes it at the top of its flow rather than re-implementing the checks.

---

## 4. Graphs We Need (Initial Scope)

For Phase 2B, six graphs cover the pending and partial items. Each graph lists its nodes in the order they execute.

### 4.1 Supervisor Graph (top-level router)

The entry point. Receives an event from any source (Redis Pub/Sub, Postgres trigger, cron, inbound webhook), classifies it, and routes it to the right subgraph.

**Nodes (4):**
1. `receive_event` — parses the event envelope, extracts subscriber ID and event type
2. `classify_event` — maps event type to target subgraph
3. `route_to_subgraph` — dispatches, passing assembled state
4. `checkpoint_root` — persists the root decision before the subgraph runs

**Why it's separate:** Isolates routing from decision logic, so new graphs can be added without touching existing ones.

---

### 4.2 Cora Conversational Lock Close Graph

Fires when a wallet-active subscriber with a lock_candidate bucket hits the trigger (40+ credits in one ZIP, revenue signal score ≥ 72, no lock yet). This is the Sonnet conversational close — the highest-value graph in the platform.

**Nodes (9):**
1. `load_subscriber_context` — pulls profile, segment, score, wallet state, recent deals
2. `assemble_live_zip_data` — active leads, Gold-tier breakdown, competitor viewing count, spend in ZIP, time since last lock in adjacent ZIP
3. `decision_hierarchy_check` — the six-step subgraph
4. `compose_sonnet_message` — Claude Sonnet call with full context; enforces max-200-word output and guardrail copy rules
5. `compliance_gate` — TCPA + DNC + rate limit
6. `send_sms` — dispatches via the compliance-gated outbound service, logs the variant ID
7. `wait_for_reply` — suspends for up to 48 hours, watching for inbound SMS (`LOCK`, `NO`, other)
8. `handle_reply` — branches: `LOCK` → payment sheet link; `NO` → log and exit; other → hand off to support routing
9. `log_outcome` — writes `message_outcomes` with conversion attribution window

**Why this is Sonnet, not Haiku:** The message has to weave six live data points into a coherent, non-templated paragraph that pushes the user to respond `LOCK`. Haiku produces usable text; Sonnet produces the close.

---

### 4.3 FOMO Graph

Fires when a competitor acts on a Gold lead in a non-locked ZIP. The goal is an SMS to the next-best-fit subscriber within 60 seconds.

**Nodes (6):**
1. `receive_competitor_event` — reads from Redis Pub/Sub the moment a lead action fires in a non-locked ZIP
2. `find_next_best_subscriber` — queries segmentation engine for the wallet-active user with the highest revenue signal score for this ZIP + trade
3. `decision_hierarchy_check` — same shared subgraph
4. `compose_haiku_message` — Claude Haiku call with a tight prompt ("Another contractor just contacted a Gold lead in {zip}. The {urgency_signal} lead is still open. [link]")
5. `send_sms_within_60s` — dispatches with priority flag
6. `log_outcome` — writes row keyed by the competitor event ID so we can measure counter-action rate

**Why Haiku here:** Time-critical, templated structure, minimal live context. Haiku hits the latency budget.

---

### 4.4 Abandonment Pressure Graph

Fires when a monetization wall session hits 10–15 minutes with no payment. Catches Session 1 abandoners with a single-CTA SMS. Has a secondary branch for users who click but don't complete.

**Nodes (11):**
1. `redis_ttl_expiry_trigger` — event from wall session approaching abandonment threshold
2. `load_wall_session` — pulls session state, verifies no payment yet
3. `check_converted_state` — if user paid between trigger and execution, exit
4. `load_live_zip_context` — current lead scarcity, competitor count
5. `decision_hierarchy_check` — shared subgraph
6. `compose_abandonment_sms` — Haiku call, single CTA, under 160 characters
7. `compliance_gate`
8. `send_sms`
9. `wait_for_click_or_expiry` — suspends up to 20 minutes
10. `branch_on_click_no_complete` — if user clicked the link but didn't pay, fire a scarcity follow-up; if no click, exit
11. `log_outcome`

**Notable:** This graph demonstrates LangGraph's suspend-and-resume cleanly — the wait-for-click node is not a scheduled job, it's a paused graph.

---

### 4.5 Retention Summaries Graph

Per-tier scheduled summaries (wallet / lock / AutoPilot each on their own cadence). Sends a tier-specific "what you spent, what you got, what you would have missed" message.

**Nodes (7):**
1. `scheduled_trigger` — fires from cron per tier cadence
2. `determine_target_cohort` — queries all subscribers in tier, filters for activity in the window
3. `assemble_user_history` — spend, leads unlocked, ZIPs worked, deals reported
4. `compose_tier_summary` — Sonnet call (the message is long-form and tier-specific)
5. `compliance_gate`
6. `send_via_preferred_channel` — SMS or email based on subscriber preference
7. `log_outcome`

**Notable:** This graph is batched — it fans out to every subscriber in the target cohort in one run. Concurrency is capped by the `AGENTS_WORKER_CONCURRENCY` config.

---

### 4.6 Auto Mode Execution Graph

Runs the automated actions (skip-trace, first-text, 24hr voicemail follow-up) on behalf of subscribers with `auto_mode=on`. Consumes the queued intents the FastAPI process has been writing since the command surface was built.

**Nodes (11):**
1. `pull_queued_actions` — reads queued intents from the database, batched
2. `for_each_action` — fan-out per queued lead
3. `execute_skip_trace` — calls the existing skip-trace service
4. `wait_for_trace_result` — suspends up to 30 minutes
5. `compose_first_text` — Haiku call, trade-specific template
6. `send_text`
7. `wait_24hr_for_reply` — suspends 24 hours (LangGraph checkpoints trivially here)
8. `branch_on_reply_state` — if replied, log and exit; if silent, continue
9. `compose_voicemail_script` — Haiku call, short and personalized
10. `deliver_voicemail` — Synthflow outbound
11. `update_queue_and_log_outcome`

**Notable:** This is the graph that demonstrates why LangGraph beats a Celery/cron approach. Three suspension points (trace result, 24hr wait, reply monitor) over possibly 48+ hours, with full checkpoint recovery on every node.

---

## 5. Graph Count Summary

| Graph | Nodes | Shared subgraphs used | Primary model |
|---|---|---|---|
| Supervisor | 4 | — | Haiku (classification only) |
| Conversational Lock Close | 9 | decision_hierarchy, compose-and-send | Sonnet |
| FOMO | 6 | decision_hierarchy, compose-and-send | Haiku |
| Abandonment Pressure | 11 | decision_hierarchy, compose-and-send | Haiku |
| Retention Summaries | 7 | decision_hierarchy, compose-and-send | Sonnet |
| Auto Mode Execution | 11 | compose-and-send | Haiku (+ Sonnet for voicemail script) |

**Totals:** 6 graphs · 48 nodes · 2 shared subgraphs.

The shared subgraphs are:

- **`decision_hierarchy_check`** (6 nodes) — enforces the six-step gate. Used by 4 of 6 graphs.
- **`compose_and_send_compliant_sms`** (4 nodes: compose → compliance → dispatch → log) — Used by 5 of 6 graphs.

Reusing shared subgraphs is what keeps the node count manageable. Without them we'd be looking at ~80+ nodes duplicated across graphs.

---

## 6. Tools the Graphs Can Call

Tools are Python functions the agents invoke to read platform state or take action. They split into three categories.

### 6.1 Read tools (query platform state) — 12

| Tool | Returns | Used by |
|---|---|---|
| `get_subscriber_profile(id)` | Full subscriber record, tier, flags, preferences | All graphs |
| `get_segment_and_score(id)` | Current bucket + 0–100 revenue signal score | All graphs |
| `get_wallet_state(id)` | Tier, balance, auto-reload, usage in last 14 days | Lock close, retention |
| `get_zip_activity(zip)` | Live lead count per tier, competitors viewing, recent actions | Lock close, FOMO, abandonment |
| `get_lead_pool(zip, trade, min_score)` | Available leads matching filters | Lock close, FOMO |
| `get_competition_status(zip)` | Lock holder, active wallet users, last lock in adjacent ZIP | Lock close |
| `get_recent_messages(id, window)` | What we've already said; prevents repetition | All graphs |
| `get_learning_card(type='latest')` | Current week's insights from Sunday aggregation | All graphs |
| `get_guardrail(key)` | Numeric bound for a decision type | All graphs |
| `get_ab_variant(id, test_name)` | Assigned variant for this user + test | All graphs |
| `get_deal_history(id)` | Reported deals, sizes, days-to-close | Retention, lock close |
| `check_opt_in(phone)` | TCPA status + last opt-in timestamp | All graphs (via compliance gate) |

### 6.2 Write / action tools — 12

| Tool | Effect | Used by |
|---|---|---|
| `send_sms(id, body, campaign, variant)` | Queues outbound SMS through compliance gate | All graphs |
| `send_voicemail_drop(id, script)` | Synthflow outbound VM | Lock close, Auto Mode |
| `reserve_lead(lead_id, subscriber_id, ttl_min)` | 20-min Redis hold | Lock close, FOMO |
| `create_payment_intent(id, amount, purpose)` | Stripe PI for payment sheet link | Lock close, abandonment |
| `flag_for_upsell(id, target_tier)` | Marks subscriber for next wallet tier | Retention |
| `queue_auto_action(id, action_type, payload)` | Adds to Auto Mode queue | Auto Mode |
| `offer_bundle(id, bundle_type)` | Surfaces bundle in dashboard and SMS | FOMO (storm bundle), retention |
| `enroll_wallet(id, tier)` | Auto-enrollment via Stripe subscription | (future — not initial scope) |
| `update_segment(id, reason)` | Forces re-classification | All graphs (on action) |
| `start_save_flow(id, tier_offer)` | Kicks off proactive save | Retention (at-risk branch) |
| `skip_trace(lead_id)` | Calls existing skip-trace service | Auto Mode |
| `log_decision(graph, id, action, context_json)` | Agent audit trail — separate from message_outcomes | All graphs |

### 6.3 Gating / safety tools — 5

| Tool | Returns | Blocks / allows |
|---|---|---|
| `guardrail_check(decision_type, proposed_value)` | `allowed: bool, reason: str` | Any action outside bounds |
| `compliance_check(id, message_type)` | `can_send: bool, reason: str` | SMS without opt-in, to DNC, in rate-limited window |
| `kill_switch_status(feature)` | `green` / `yellow` / `red` | RED → falls back to simpler path |
| `budget_check(graph_name)` | `tokens_remaining, cost_remaining_usd` | Circuit-breaker when graph exceeds budget |
| `ab_variant_assign(id, test_name)` | Variant ID (deterministic by hash) | Ensures same-user consistency |

**Tool count: 29.** Nearly all are **thin wrappers over existing services** — the tool layer is not "new code," it's "typed function signatures over services we already have."

This matters for two reasons: (1) the tools behave identically whether called from FastAPI or from LangGraph, so business logic only lives in one place, and (2) testing is easy — mock the tool, not the service.

---

## 7. Event Sources — What Triggers a Graph

Graphs don't run on their own. Something has to initiate them. The sources:

| Source | Example event | Graph triggered |
|---|---|---|
| **Redis Pub/Sub** | Competitor acts on a lead in non-locked ZIP | FOMO |
| **Redis TTL expiry** | Monetization wall session hits 10-min mark | Abandonment |
| **Postgres trigger or polling** | Subscriber crosses lock-candidate threshold | Lock close |
| **Cron** | Weekly tier cadence fires | Retention summaries |
| **Queued intent** | Auto Mode actions accumulated from SMS toggles | Auto Mode |
| **Explicit API call** | Founder-initiated action via admin endpoint | Any (for manual overrides) |

The Supervisor graph has one job: accept an event from any of these sources, normalize it, and route it to the right subgraph. That isolation means adding a new event source (say, NWS storm → personalized storm-pack outreach) is a matter of registering a new handler in the Supervisor, not touching the subgraphs.

---

## 8. What's NOT in LangGraph

Important to be explicit about the boundary, because the temptation to put everything in agents is real.

**Stays in FastAPI (synchronous, user-initiated):**
- All HTTP endpoints (signup, payment sheet, deal capture, wall session, proof moment)
- All webhook handlers (Stripe, Twilio inbound, NWS, Synthflow)
- All SMS command dispatch (BALANCE, LOCK, etc.)
- Stripe webhook idempotency and processing
- Compliance gate as a callable function
- Segmentation classifier (called from both FastAPI and LangGraph)
- Guardrail config loader

**Stays as straight cron jobs (scheduled, not agentic):**
- Annual push daily cron — templated offers on rules, no live composition needed
- Data-Only save daily cron — same reason
- Revenue Pulse daily + weekly — straight aggregation + SMS template
- Learning Card Sunday aggregation — pure SQL aggregation

The rule of thumb: if the decision needs **live context assembly and Claude composition**, it's a graph. If the decision is **templated output based on state**, it's a cron or an endpoint.

---

## 9. Cost and Performance Notes

- **Target Haiku/Sonnet ratio: 75–80% Haiku.** Most decisions (FOMO, abandonment, Auto Mode texts) run on Haiku. Sonnet is reserved for the lock close and retention summaries, where the composition quality directly moves revenue.
- **Per-decision token budget.** Each graph has a hard cap (e.g., lock close: 2,000 tokens total across all Claude calls in one decision). Exceeding triggers the budget circuit breaker in `budget_check`.
- **Latency targets.**
  - FOMO: end-to-end under 60 seconds (this is the spec requirement)
  - Lock close: under 5 seconds from wait-for-reply node receiving `LOCK` to payment sheet link being sent
  - Abandonment: under 10 seconds from TTL expiry to SMS out
  - Auto Mode, retention: not latency-sensitive — batched
- **Concurrency.** Initial target is 5 concurrent graphs running on the worker process. Bumps up after we see real load.

---

## 10. Observability

- Every graph run produces a LangSmith trace. Traces are keyed by subscriber ID so debugging "why did Cora do X for user Y" is one search.
- Every fired action writes a row to the agent audit log (separate from `message_outcomes`, which is outcome-focused).
- Prometheus metrics: graphs started, graphs completed, graphs failed, tokens used per graph, latency per graph.
- Per-feature kill switch: a single config flag can disable any graph at runtime without a deploy.

---

## 11. Summary

LangGraph handles Cora's autonomous decisions. Everything else stays where it is. The initial Phase 2B scope is:

- **6 graphs** (Supervisor, Lock Close, FOMO, Abandonment, Retention, Auto Mode)
- **48 nodes** across those graphs
- **2 shared subgraphs** (decision hierarchy, compose-and-send)
- **29 tools** wrapping existing services
- **5 event sources** feeding into the Supervisor

This scope closes all 4 PENDING items and completes the 2 LangGraph-dependent PARTIAL items (Auto Mode automation, Cora SMS Conversational Close) in one engineering block.

Architecture — process topology, deployment, repo layout, config split, failure modes, and testing — is the next document.
