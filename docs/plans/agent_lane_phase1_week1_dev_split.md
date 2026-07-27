# Agent Lane v2.2 — Phase 1 · Week 1 Task Breakdown

Source documents: `Forced_Action_Agent_Lane_v2.2_Development_Task_Order.pdf`, `FA-Agent-Lane-Build-Spec-TEAM.pdf`, `FA-Agent-Lane-Build-Spec-Questions.md` (client answers, 2026-07-21/22).

Scope: the four remaining Phase 1 · Week 1 gating tasks — **VERA-v2.2, HUNTER-01, HUNTER-02, RELAY-v2.2**. P1-CRED (vault/credential rotation) is excluded — already done.

Grounded against the current `Forced-action-` codebase (read directly, not assumed) as of 2026-07-23.

Each task below is broken into 4 independent subtasks. **These are pickable units, not pre-assigned roles** — whoever is available can take any subtask, in any combination, as long as the dependency ordering in §6 is respected. Within each task, two subtasks are naturally "build the core mechanism" work and two are "verify/harness/downstream-contract" work — that grouping is there to help whoever's picking see how the task decomposes, not to imply a fixed two-person split.

---

## 0. Why the split is shaped this way

Real dependency graph between the four tasks:

- **VERA** and **RELAY** are infrastructure agents with no dependency on buyer data — they can be built entirely independently of Hunter, by anyone, in any order.
- **HUNTER-02 (whale detection) hard-depends on HUNTER-01 (entity resolution)** — the task order calls this out explicitly ("Entity resolution is the hard dependency"), and the client's Q3 answer confirms items 1 and 2 must be quoted/sequenced together, in that order.
- Within each task, the work splits cleanly into a **builder half** (schema, engine, core matching/scoring logic) and a **verifier/contract half** (accuracy harness, acceptance tests, downstream data contracts, reporting). This mirrors the fleet's own design philosophy (Vera verifies everyone; every agent carries acceptance criteria) and keeps merge conflicts low since each half touches different files — useful to know even if one person ends up doing both halves of a task themselves.

**Sequencing note:** the HUNTER-01 subtasks (H1, H2 — entity resolution) must land before the HUNTER-02 subtasks (W1–W4 — whale detection) can do real work, regardless of who picks up which piece. Everything else in this plan can proceed in parallel from day one.

---

## 1. VERA-v2.2 — Truth & Verification Agent

**Current state of the system (grounded):** No Vera process exists. Nothing in `scripts/cron/crontab.txt` references a Vera/Hunter/Relay job. The closest existing building blocks are: `src/tasks/stripe_reconcile.py` (a one-way *repair* task — finds Stripe subs missing from the DB and replays the webhook to fix them; it does not check the reverse direction and does not produce a report), and `src/api/operator_dashboard_router.py` + `src/services/operator_dashboard.py` + `src/services/action_queue.py` (Block 8 — an existing KPI-aggregation and action-queue API surface, JWT-protected, read-only). Both are useful raw material but neither is read-only-by-construction, neither writes to a facts directory, and neither is framed as an independent audit layer distinct from the system it's checking.

| # | Subtask | What it changes | Current state | State after |
|---|---|---|---|---|
| V1 | **Agent scaffolding + read-only DB role + facts directory** | New `src/vera/` package (own scheduled entry point, separate from Cora's LangGraph runtime since Vera is deterministic/read-only, not conversational); new Postgres role granted SELECT-only across all tables; new `/shared/facts/` writer (one fact per line, dated, source+method, freshness class per §1.1.15). | All existing DB access (API, tasks, Cora) uses the same read/write app role — there is no SELECT-only role scoped to an audit-only agent. No facts-directory convention exists in the repo. | Vera runs as its own process under a DB role that structurally cannot write to business tables (per her immutable core); every check she runs lands as a dated, sourced fact other agents can later read from. |
| V2 | **Live-state report: prod-hash vs dev HEAD, cron freshness, deploy drift** | New checker module reading `ScraperRunStats`/`ScraperAlertLog`, `scripts/cron/crontab.txt`, and deploy/git metadata; formats the "before 8am" report Vera's constitution requires (THE ONE NUMBER placeholder until V3 lands, then live). | `scraper_run_stats` + `ScraperAlertLog` already track individual scraper run outcomes, and `daily_report.py`/`daily_dashboard.py` already produce daily ops reports — but nothing cross-checks the deployed prod hash against dev HEAD, and nothing flags the "enabled-but-unscheduled" / "scheduled-but-writing-nothing" failure modes Vera's spec calls out by name. | A daily pre-8am report independently verifies deploy state and cron health rather than trusting whatever the dev shop claims — this is half of Vera's build-acceptance criteria. |
| V3 | **Stripe two-way revenue reconciliation (read-only)** | New reconciliation module inside `src/vera/`; reads Stripe via the existing `stripe_service.py` client + `Subscriber`/`PlatformRevenueLedger`; reports both directions (paying-but-no-access AND access-but-not-paying) plus new/failed-payment and refund/dispute tracking. Does **not** modify `src/tasks/stripe_reconcile.py` — that stays a separate auto-repair task. | `stripe_reconcile.py` exists but checks one direction only (Stripe active / DB missing) and *auto-fixes* it by replaying the webhook — it's a repair script, not an audit report, and it never flags the reverse mismatch (DB says active, Stripe says cancelled). | A daily, read-only, both-directions Stripe-vs-DB discrepancy report exists, independent of the self-healing repair task — Josh gets revenue truth that isn't filtered through a script that's already silently "fixing" things. |
| V4 | **Discrepancy/promise digest + seeded-discrepancy acceptance test** | New promise-tracking store (age, owner, MRR-at-risk) and discrepancy-report formatter ("Doc claims X; live state shows Y"); a build-acceptance test fixture that plants a false claim against real DB/deploy state and asserts Vera's checkers (V2/V3) catch it. | No promise-tracking mechanism, no discrepancy-report format, and no acceptance test of any kind exists for this. | Vera has a working, repeatable "catches a deliberately seeded discrepancy" gate — the literal build sign-off criterion in her constitution — plus a standing promise/overdue digest. |

*V1/V2 and V3/V4 are independent halves (deploy-and-cron truth vs. revenue truth) — they can be picked up separately and merged into one daily report at the end.*

---

## 2. HUNTER-01 — Buyer Entity Model & Resolution

**Current state of the system (grounded):** No buyer/entity concept exists anywhere in the schema. `Owner` rows are one row per property (never deduplicated across a buyer's multiple properties). `Deed.grantee` is raw free text with no link back to any canonical identity. `Owner` already carries Sunbiz LLC-piercing data per owner-row (`managing_members` JSONB/GIN-indexed, `sunbiz_doc_number`, `registered_agent_name`, `principal_address`) from a prior build (fa031) — but nothing collapses that into "one person controls LLCs A, B, C" as a single addressable entity, and nothing matches `Deed.grantee` text back to an `Owner`/Sunbiz identity. Per the client's Q3 answer, this is confirmed net-new build, and per Q2, there is no buyer/investor roster of any kind today.

| # | Subtask | What it changes | Current state | State after |
|---|---|---|---|---|
| H1 | **Buyer entity schema + migration** | New `BuyerEntity` (canonical person/LLC) and `BuyerEntityLink` (which raw `Owner`/`Deed.grantee`/`SunbizSnapshot` rows map to which entity, with confidence) ORM models in `src/core/models.py`; new idempotent `migrations/apply_<n>_buyer_entities.py` per the repo's scripts-only migration convention (no Alembic). Confidence stored as an integer 0–100 (per Hunter's constitution wording, "<70 = UNVERIFIED") — a deliberate divergence from `Deed.match_confidence`'s existing `Numeric(4,3)` 0.000–1.000 scale. | No entity table exists. `owners` is per-property; `deeds.grantee` is unlinked free text. | A canonical `buyer_entities` table exists that whale detection (HUNTER-02), buyer-type classification (HUNTER-03), and every future Hunter/Cora feature can join against, with full traceability back to source rows. |
| H2 | **Identity resolution matching logic — person↔LLC↔mailing address** | New `src/services/buyer_entity_resolution.py`, structured as a *pure classifier + DB-sweep service* split (same shape as `cds_engine` and `contact_triangulation.py`). Reuses `BaseLoader.normalize_owner_name()` (existing trust/suffix/punctuation normalizer) and the `fuzz.token_set_ratio` name/address-agreement pattern already proven in `contact_triangulation.py`'s `_name_agrees`/`_addresses_agree`, rather than writing new fuzzy-match logic. Blocks candidates first (normalized surname + mailing ZIP) to avoid O(n²) comparison across ~522K owner rows; assembles matched pairs into entities via union-find; routes only genuinely ambiguous pairs (name matches, address doesn't corroborate, or vice versa) to a single cheap-tier Claude tie-break call. Nightly runs match new/changed `deeds`/`owners` rows against **existing** `buyer_entities` first (not a full re-cluster), so entity IDs stay stable across runs — whale flags and Cell #1's list reference these IDs directly. Writes `buyer_entities`/`buyer_entity_links`. | Sunbiz LLC-piercing data exists per-owner-row but nothing traverses it into a person→LLC graph; no grantee-name-to-identity matching exists anywhere. `contact_triangulation.py` already solves a near-identical corroboration problem (cross-source identity agreement) with this exact pure-classifier/DB-sweep split, which is why this is an assembly job, not new infrastructure. | Running the resolver against the existing ~522K-property dataset produces a deduplicated buyer roster — the exact prerequisite the client's answer calls "everything depends on this." |
| H3 | **Nightly enrichment sweep wiring — portfolio size & purchase cadence** | Two scripts: a one-time `scripts/backfill_buyer_entities.py` (builds the initial roster from the full historical dataset) and a nightly `src/tasks/hunter_nightly_sweep.py` (cron entry, following the existing stagger convention in `scripts/cron/crontab.txt`) that calls H2's resolver incrementally and additionally derives per-entity portfolio size and purchase cadence (count + dates of linked deeds); writes to `/shared/facts/enriched/` in Hunter's constitution format (one record per target, source-linked, dated, confidence-scored). | No nightly buyer-enrichment job exists. The closest analogues (`run_enrichment.py`, the skip-trace waterfall) enrich *seller-side* owner contact data, not buyer identity or portfolio. | Every night, new/changed deed activity flows through resolution and lands as dated, sourced buyer-entity facts — the "fuel line" Cora reads from once she's built in Phase 2. |
| H4 | **Confidence scoring + spot-check accuracy harness** | Confidence scorer (0–100) inside the resolution service, `<70 = UNVERIFIED` and excluded from any downstream draft per Hunter's constitution; a founder-spot-check sampling/reporting script and test fixtures with known-correct entity groupings for the ≥90%-on-20-records acceptance test. | No confidence-scoring convention exists for buyer/identity matching (contact-freshness confidence exists for owner phone/email reachability — a different axis entirely). | Every resolved entity carries a confidence score, low-confidence entities are structurally excluded from anything customer-facing, and there's a repeatable test proving the ≥90% accuracy bar before HUNTER-01 is called done. |

**Hand-off note:** H3/H4 consume H1/H2's schema and resolver output. It's fine to start H3/H4's query shape and acceptance-harness scaffolding against a stub or hand-built sample of `buyer_entities` before H1/H2 fully land, then wire to the real resolver once it's ready. This is the one real sequencing dependency inside Hunter — see §6.

---

## 3. HUNTER-02 — Whale Detection Module

**Current state of the system (grounded):** No whale concept exists anywhere. `Deed.sale_price` is captured per-deed but never summed per buyer. `DorSale`/`Foreclosure` tables and their loaders already ingest Hillsborough/Pinellas auction results, but nothing currently classifies a winner as NEW-BUYER vs. REPEAT-BUYER or scores them. **This entire task is blocked on HUNTER-01 (H1/H2) landing** — per the client's Q2 answer, this is why Cell #1 ("25 whales") cannot run day one as the original spec assumed; it lands after entity resolution.

| # | Subtask | What it changes | Current state | State after |
|---|---|---|---|---|
| W1 | **Whale scoring rule engine — 3+ purchases/18mo OR >$500K cash** | New `src/services/whale_detection.py` reading `buyer_entities`/`buyer_entity_links`/`deeds`; implements the rule (3+ purchases in trailing 18 months OR summed linked `sale_price` >$500K); writes a `whale_flag` + one-line fact per Hunter's standing run #3. | Nothing — no per-buyer purchase aggregation of any kind exists today. | Every buyer entity carries a live, continuously-recalculated whale flag — the direct input Cell #1 needs. |
| W2 | **Auction-winner fast-follow tie-in (<24h)** | Hooks W1's scorer into the existing `Foreclosure`/`DorSale` ingestion path so a fresh auction win by a tracked entity re-scores it and reaches the facts directory within 24h, per Hunter's standing run #2 latency SLA. | `DorSale`/`Foreclosure` loaders already ingest auction results, but nothing classifies a winner as new-vs-repeat buyer or whale-scores them today. | An auction win by a known or newly-qualifying whale surfaces same-day — satisfies the <24h fast-follow rule the client's answers flagged as launch-gating. |
| W3 | **"25 biggest whales" ranked output + Hunter→Cora data contract** | New query/service producing the ranked top-N whale list (by purchase count + cash volume) in the exact shape Cora will consume once built in Phase 2 — name, resolved contact-channel confidence (via the entity link back to `Owner.phone_metadata`/email fields), portfolio summary, "why now" catalyst. Documents the handoff-contract fields per §9.5 (required fields + acceptance test at the Hunter→Cora boundary). | Nothing — per the client's Q2 answer, "there is no investor or buyer roster in the platform." | A file/table exists that literally *is* Cell #1's "25 biggest whales" input the moment Cora is built — the direct unblock for the launch-gating cell. |
| W4 | **Whale-detection accuracy + latency acceptance tests** | Test fixtures validating whale flags against a founder spot-check sample; a latency test proving auction-winner scoring lands <24h on two consecutive real sale dates; a source-freshness board entry for the auction feed. | No test coverage exists for any of this since the underlying feature doesn't exist yet. | A repeatable, dated proof that whale detection meets its accuracy and latency bars — HUNTER-02's sign-off gate. |

---

## 4. RELAY-v2.2 — Relay Execution Service

**Current state of the system (grounded):** No deterministic batch-execution engine exists. Sends today happen through two separate, unrelated paths: (1) **Instantly** (`src/services/instantly_service.py`) — a cold-email campaign platform client with its own warm-up/rate-limit handling, used for `email_campaign_topup`/DBPR contractor outreach; (2) **Cora's LangGraph runtime** (`src/agents/subgraphs/compose_and_send_email.py`) — an agentic lifecycle-messaging flow for retention/FOMO/abandonment, gated by `kill_switch_service.py`'s green/yellow/red per-decision switch. Neither is a "Josh approves once, engine executes exactly that, produces a receipt" engine — that pattern doesn't exist. Suppression enforcement (`EmailOptOut`, `SmsOptOut`, `CoraSuppression`, `DncPhoneCheck`, `email_suppression.py`, `sms_compliance.send_sms`) already exists and is already enforced inside each existing send path — but there's no single execution-time gate a new engine can call generically across channels.

| # | Subtask | What it changes | Current state | State after |
|---|---|---|---|---|
| R1 | **Relay core engine + idempotency keys + kill command** | New `src/services/relay_engine.py`: takes an approved batch, executes exactly what was approved, one idempotency key per action (retries can't double-send); wires the fleet-wide "STOP ALL"/"STOP [agent]" kill command so it halts within one cycle. | No execution/dispatch engine of this shape exists. A kill-switch concept exists (`kill_switch_service.py`) but it gates individual Cora agent decisions, not a batch-send engine. | A single deterministic engine exists that a future batch-approval UI calls into, with guaranteed no-double-send and an instant kill — the piece that removes the founder as the send-copy-paste bottleneck. |
| R2 | **Connected-inbox send-channel integration** | New send-channel adapter for the channel the client asked to use (Q5: a connected inbox / warmed domain, not personal Gmail) — most likely extending the existing Instantly integration (which already has warm-up + per-mailbox limits, per ADR 0011) rather than building Gmail/Workspace OAuth from scratch, pending the client's confirmation of "is the warming domain the right vehicle" (open item from Q5). | No Gmail/Workspace OAuth send integration exists. `instantly_service.py` is the closest existing authenticated-outbound-email building block. | Relay has one real, working send channel wired end-to-end (not a stub), matching the client's stated preference for a connected inbox over a personal account. **Flagged dependency:** confirm the connected-inbox vehicle with the client before calling this subtask done — the engine-level plumbing (R1) does not need to wait on that answer. |
| R3 | **Quiet hours + per-channel daily ceilings + execution-time DNC recheck** | New guard logic inside/around the Relay engine enforcing quiet hours and per-channel caps (Gmail 20/day pre-warm-up per spec); re-checks `EmailOptOut`/`SmsOptOut`/`CoraSuppression`/`DncPhoneCheck` at the moment of execution, not just at draft time. | Suppression checks exist and are enforced today, but only inside each individual existing send path (Instantly, Cora's subgraph) — there's no shared, execution-time gate a new generic engine can call. | Every Relay-executed send re-verifies suppression/quiet-hours/caps immediately before firing, independent of what was true when the draft was originally approved — closes the "state changed since approval" gap the spec calls out. |
| R4 | **Completion receipts + batch-intake contract** | New Action Completion Receipt record (sent timestamp, channel, thread ID); documents and tests the intake contract for "Josh approves a batch" (the shape Relay expects from the founder-throughput layer, even though that approval UI itself is Phase 2 work); end-to-end acceptance test: a real batch executes, produces receipts, and a forced retry proves idempotency held. | No receipt concept and no batch-intake contract exist. | A tested, documented contract for how the future approval UI hands work to Relay, plus proof that a real send produces a verifiable receipt trail — "approved" vs. "receipted" per the spec's own distinction. |

*R1/R2 (engine + one real channel) and R3/R4 (guards + receipts/contract) are close to independent and can be picked up separately, converging into one tested Relay service.*

---

## 5. Subtask index — pick any, respecting §6

All 16 subtasks at a glance. None are pre-assigned; pick based on availability and the dependency notes.

| Task | Subtasks | Blocked by |
|---|---|---|
| VERA | V1, V2, V3, V4 | Nothing — all four are ready to start immediately. |
| HUNTER-01 | H1, H2 | Nothing — ready to start immediately. |
| HUNTER-01 | H3, H4 | H1 + H2 (can be scaffolded against a stub before H1/H2 land, per §2). |
| HUNTER-02 | W1, W2, W3, W4 | H1 + H2 (hard block — see §3). |
| RELAY | R1, R3, R4 | Nothing — ready to start immediately. |
| RELAY | R2 | Nothing internally, but not "done" until the client confirms the connected-inbox vehicle (§6c). |

---

## 6. Dependencies

### 6a. Task-level (who blocks whom)

| Depends on → | Blocks | Nature |
|---|---|---|
| H1 (buyer entity schema) | H2, H3, H4, W1, W2, W3, W4 | Hard — nothing in Hunter works without the table existing. |
| H2 (resolver) | H3, H4, W1, W2, W3, W4 | Hard — this is "the hard dependency" the task order names. Until the resolver actually runs and produces entities, everything downstream is stub-only. |
| HUNTER-01 (H1+H2) as a whole | HUNTER-02 (W1–W4) | Hard, task-to-task — confirmed by the client's Q2 answer (Cell #1 lands after Hunter items 1&2, not day one). |
| VERA | *nothing in Hunter or Relay* | None — Vera can ship a day early or a day late with zero effect on the other two tasks. |
| RELAY | *nothing in Hunter or Vera* | None — same. Relay's only real dependency is external (below). |
| RELAY internally: R1 (engine) | R3, R4 | Soft — R3/R4 can be designed/tested against a stub engine, but need the real one to fully pass acceptance. |
| RELAY internally: R2 (send channel) | *nothing else in Relay* | None — R1/R3/R4 don't need a working send channel to be built or tested; R2 is the one piece that can lag without blocking the rest of Relay. |

**Net effect:** VERA, HUNTER-01 (H1/H2), and RELAY (R1/R3/R4) can all start on day one, by anyone, in parallel. The only hard gate is HUNTER-01 → HUNTER-02. The only place two independently-picked-up tasks actually touch each other is §6b below.

### 6b. Cross-cutting shared-infrastructure dependencies (the ones the task-by-task tables don't surface)

These are pieces of fleet-wide infrastructure that **more than one task's subtasks reference as if already defined**, but nothing in the current plan explicitly assigns ownership of defining them once. Build these independently per-task and you get schema drift; agree on them once and both tracks conform.

| Shared artifact | Referenced by | Current state | Who should define it first | Risk if skipped |
|---|---|---|---|---|
| **Facts-directory schema** (`/shared/facts/...`) | Vera's V1 (writer) + V2/V3 (writes), Hunter's H3 (`/shared/facts/enriched/`) and W1/W2 (whale one-line facts) | Doesn't exist in the repo at all today. | Whoever ships first — likely Hunter's H3, since it's not gated on anything else — or a quick sync between whoever's on Vera and whoever's on Hunter before either writes to it. | Vera and Hunter each invent an incompatible "fact record" shape; nothing downstream (a future Cora reading facts) can rely on a consistent format. |
| **Kill-switch registration** (fleet-wide "STOP ALL" / per-agent "STOP [agent]") | Relay's R1 explicitly; Vera and Hunter's own processes *should* check it too per §1.1.11/§1.8, but no V/H subtask currently wires this in | `config/agents.py`'s `agents_global_kill_switch` + `graph_is_enabled(graph_name)` exists today but is scoped to Cora graphs only — nothing generic for a non-graph process like Vera's scheduler or Hunter's sweep to check. | Not currently assigned to anyone — **gap**. Cheapest fix: extend the existing `kill_switch_service.get_kill_switch_status(feature, ...)` (already feature-keyed, already generic) with a `"vera_global"` / `"hunter_global"` / `"relay_global"` feature key each process checks before running, rather than building three bespoke mechanisms. |
| **Data-access matrix document** (§1.1.12 — "a one-page table in the repo") | Implied by Vera's V1 (SELECT-only role) and Hunter's H1 (write scope on new tables) and Relay's write scope on receipt/log tables | No such document exists (`docs/` has no data-access-matrix file). | Nobody currently — **gap**, but low effort (a single markdown table listing each agent's DB role and what it can read/write). Whoever finishes their DB-role subtask first (V1 or H1) should start the doc; whoever picks up the other adds their row. | Without it, DB grants live only in migration scripts scattered across whoever touched them, with no single place to audit "who can write what" — exactly the artifact the spec asks for by name. |
| **Opportunity Thread IDs** (`OPP-YYYY-#####`) | Hunter's W1/W3 (mints one when an entity becomes an opportunity), Relay's R4 (stamps it on completion receipts) | Doesn't exist anywhere in the codebase — confirmed by grep, no existing convention to reuse. | Hunter, since it's the first task in Phase 1 to actually need one (an entity flagged as a whale is the first "opportunity" this build creates). A simple ID-generation utility, not a big lift. | Relay's R4 acceptance test ("stamp thread ID on receipt") has nothing real to stamp; Vera's later revenue-coupling work (Phase 2+) has no stable ID to trace a close back to the decision that produced it. |

### 6c. External / client dependencies

- **R2 (connected-inbox send channel):** blocked on the client confirming the send vehicle (Instantly-based vs. Workspace OAuth) — flagged from Q5. Does not block R1/R3/R4.
- Nothing in VERA or HUNTER-01/02 has an open external dependency — both can proceed on existing data (owners/deeds/Sunbiz already scraped, Stripe already live) without further client input.

## 7. Sequencing across the week

1. **Day 1–2:** VERA (V1–V4), HUNTER-01 (H1, H2), and RELAY (R1, R3, R4) all start immediately, in parallel, regardless of who's picked up what — none of them block each other.
2. **Day 2–3 (gate):** H2 (resolver) lands → H3/H4 and W1–W4 can now run on real data instead of stubs. Before either H3 or Vera's facts-writer code goes further than a first draft, a quick sync on the facts-record shape (§6b) avoids a rework.
3. **Day 3–5:** HUNTER-02 (W1–W4) proceeds once H1/H2 are done, in order (whale scoring is the last thing to land, since it needs H2's entities). VERA and RELAY's remaining subtasks finish in parallel — no interaction with Hunter.
4. **Open item before RELAY is fully done:** client confirmation on the connected-inbox vehicle (Q5) — flagged on R2 only.
5. **Gate closure:** all five Phase 1 items (P1-CRED already done + these four) must be green before the Cell #1 ("25 Whales") go/no-go call — per the task order, that call issues the moment Phase 1 closes.
