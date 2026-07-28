# VERA-v2.2 — Agent Lane Task Updates

Running client-facing update log for the VERA-v2.2 Truth & Verification Agent build. Each sub-task is appended below as it completes.

---

## Task VERA-v2.2 — V1 — Agent Scaffolding, Read-Only DB Role & Facts Store

Status: Complete

Backend Status: Complete

Frontend Integration: Not applicable — this sub-task is backend/infrastructure only, laying the foundation Vera's later reporting sub-tasks (live-state, revenue reconciliation, promise digest) build on.

### What We Built

Vera is the Agent Lane's truth-and-verification agent — her one job is to independently confirm what is actually true about the platform's systems and revenue, rather than relying on what any dashboard or report claims. This sub-task builds the three foundations everything else she does depends on: a dedicated process for her to run in, a database role that makes her "read-only, always" promise a real, enforced guarantee rather than a convention, and a single shared table where every fact she verifies gets recorded.

**Agent scaffolding**

Vera now runs as her own process (`python -m src.agents.vera`), entirely separate from the existing Lifecycle/Lifecycle agent runtime — no shared code path, no shared event loop. A health check (`--health`) confirms her database connection, her facts table, and her kill-switch gate are all reachable before any standing job runs.

**Read-only database role**

A dedicated `vera_readonly` database role was provisioned on the shared environment, granted SELECT access across every table and nothing else — no ability to insert, update, or delete data of any kind, enforced by the database itself rather than by application code. This was verified directly: a live connection as `vera_readonly` successfully read from the `properties` table, and a write attempt was correctly rejected by the database. This means Vera's read-only guarantee holds even in the case of a future bug in her own code — the database itself will not permit a write.

**Facts store**

A new `vera_facts` table is the single place Vera records every fact she verifies — dated, sourced, and tagged with how fresh that type of fact is expected to stay (for example, revenue numbers are treated as current for 24 hours before they're flagged as needing re-verification). Records are never overwritten, only re-confirmed, so there's always a full history of what was verified and when. This table is the one and only thing Vera's code is able to write to — everything else she touches, she only reads.

**Data-access documentation**

A one-page data-access reference (`docs/agent-lane-data-access-matrix.md`) now documents exactly what each agent role can read and write, giving the team a single place to see this at a glance as Hunter and Relay are added in upcoming sub-tasks.

### Validation

- Database: `vera_readonly` confirmed able to read live production data (verified against the `properties` table).
- Database: a write attempt through `vera_readonly` was correctly rejected at the database level, confirming the read-only guarantee holds independent of application code.
- Facts store: a fact was written and successfully read back, confirming the write/read path functions end-to-end.
- Process: `python -m src.agents.vera --health` passes all checks against the live shared database.
- Provisioning: the database role was set up through a single, reversible, one-time administrative step on the shared database, coordinated in advance and confirmed to have zero impact on any existing table, role, or data.

### Evidence

| File | Description |
|---|---|
| `src/agents/vera/__main__.py` | Process entry point and health check |
| `src/agents/vera/db.py` | Read-only database connection (`vera_readonly` role) |
| `src/agents/vera/facts.py` | Write/read path for verified facts |
| `src/agents/vera/config.py` | Shared constants (freshness classes, kill-switch key) |
| `src/core/models.py` | `VeraFact` schema definition |
| `migrations/apply_vera_facts.py` | Facts table migration, applied to the shared environment |
| `migrations/apply_vera_readonly_role.py` | Read-only role provisioning, applied to the shared environment |
| `config/settings.py` | Vera's database connection settings |
| `docs/agent-lane-data-access-matrix.md` | Per-agent database access reference |

---

## Task VERA-v2.2 — V2 — Live-State Report (deploy drift, cron freshness, silent-failure detection)

Status: Complete

Backend Status: Complete

Frontend Integration: Not applicable — this is a backend verification agent delivered as an automated daily email report; no dashboard or UI component was in scope.

### What We Built

Built Vera's first standing job: a daily, fully automated report that independently verifies three things about the live platform every morning, checking the actual system state directly rather than trusting any dashboard, cron log, or developer claim. This is the foundation the rest of Vera's verification work builds on — nobody else on the fleet is trusted to quote a number until Vera has checked it herself.

**Deploy drift check**

Compares the exact code commit running in production against the tip of the development branch, and separately verifies every pending database migration's real-world status by checking the live schema directly — does the table or column it's supposed to create actually exist — rather than relying on a migration log. This catches the two most common ways a deploy silently goes stale: code that was merged but never reached production, and a migration that was supposed to run but didn't.

**Cron freshness check**

Checks every scraper and data source against its expected freshness window (for example, "must have run successfully in the last 25 hours"), surfacing exactly which sources have gone stale and for how long.

**Silent-failure detection**

Two additional checks that a simple success/fail status can miss entirely: a source that ran and reported "success" but ingested zero records that day, and a source the platform expects to be collecting data but which has no active schedule at all.

**Report delivery**

All three checks write dated, sourced facts into Vera's permanent record, then render into a single daily email report, delivered automatically before 8am. The report's headline number ("new MRR added yesterday") is wired to read directly from the companion Revenue Truth Report (V3) once that runs each morning.

**Report formatting (client follow-up)**

Following review of the first live report emails, two readability improvements were made: cron freshness now displays age in a plain hours/days format instead of raw minutes, with a one-line explanation of what "SLA" means in this context; and the deploy-drift section now states clearly when it cannot check production from wherever it's running (for example, during testing), instead of leaving that unexplained. The email itself was also rebuilt in HTML, matching the platform's existing daily/weekly report styling, in place of the original plain-text version.

### Validation

- Live-tested against the actual production database, git repository, and cron configuration — not simulated data.
- Confirmed genuine, real findings on first run: correctly identified stale data sources and zero-ingest days against real system state.
- 15 automated tests covering every decision the report makes (deploy-status classification, migration verification, freshness formatting, report content) — all passing.
- Verified the rendered email, both plain-text and HTML, end-to-end before any report was sent for real.

### Evidence

| File | Description |
|---|---|
| `src/agents/vera/checks/live_state.py` | Deploy drift, cron freshness, and silent-failure checks; report renderer |
| `src/agents/vera/checks/_shared.py` | Shared HTML email formatting used by both the Live-State and Revenue Truth reports |
| `src/agents/vera/__main__.py` | `--live-state` command entry point |
| `scripts/cron/crontab.txt` | Daily 07:47 UTC schedule entry |
| `tests/agents/test_vera_live_state.py` | 15 automated tests covering the report's logic |

---

## Task VERA-v2.2 — V3 — Revenue Truth Report (Stripe-vs-database reconciliation, real MRR, payments, refunds & disputes)

Status: Complete

Backend Status: Complete

Frontend Integration: Not applicable — delivered as an automated daily email report; no dashboard or UI component was in scope.

### What We Built

Built Vera's second standing job: a daily report that verifies the platform's revenue picture directly against Stripe — the single source of truth for what's actually being paid — rather than relying on the platform's own database or its own record of past events.

**Two-way subscriber reconciliation**

Checks both directions between Stripe and the subscriber database: customers Stripe shows as actively paying but who don't have matching access in our system, and customers with active access in our system whose Stripe subscription is no longer active. The second direction is a genuine gap closed by this work — the platform's existing reconciliation only checked the first direction.

**Real MRR**

Computes monthly recurring revenue two ways — once from live Stripe subscription data, once from the database — and reports both side by side, so any drift between them is visible rather than hidden behind a single number. Also computes "new MRR added yesterday," the headline figure the Live-State Report (V2) displays each morning.

**New/failed payments**

Reports today's successful and failed payments directly from Stripe, split by subscription renewals versus one-time purchases (lead packs, premium reports).

**Refunds & disputes**

Reports every refund and dispute today, across every product — closing a gap where the existing dispute-handling logic only recognized disputes tied to one specific product type.

**Stripe data-reliability hardening (client follow-up)**

Following initial validation against a live Stripe test account, additional hardening was added so that every check clearly distinguishes "Stripe could not be reached today" from "everything checked out fine" — a Stripe outage now shows explicitly in the report rather than reading as a clean result. A related fix ensures the day-over-day MRR comparison always compares against the correct prior day, even if the report is run more than once on the same day.

### Validation

- Live-tested against the actual production database and a live Stripe test account — not simulated data.
- Confirmed genuine, real findings on first run: correctly identified a real dollar drift between Stripe and database MRR, and flagged subscriber records pointing to Stripe subscriptions that no longer exist.
- 39 automated tests covering every decision the report makes (reconciliation logic, MRR calculations, payment classification, Stripe outage handling) — all passing.
- Verified the rendered email, both plain-text and HTML, end-to-end before any report was sent for real.

### Evidence

| File | Description |
|---|---|
| `src/agents/vera/checks/revenue_truth.py` | Reconciliation, MRR, payment-activity, and refund/dispute checks; report renderer |
| `src/agents/vera/checks/_shared.py` | Shared HTML email formatting used by both the Live-State and Revenue Truth reports |
| `src/agents/vera/__main__.py` | `--revenue-truth` command entry point |
| `scripts/cron/crontab.txt` | Daily 07:36 UTC schedule entry |
| `tests/agents/test_vera_revenue_truth.py` | 39 automated tests covering the report's logic |

---

## Task VERA-v2.2 — V4 — Promise & Discrepancy Digest + Seeded-Discrepancy Acceptance Test

Status: Complete

Backend Status: Complete

Frontend Integration: Not applicable — delivered as an automated daily email report. No dashboard or UI component was in scope.

Scope note (please read): The automatic source of promises — a reply-forwarding mailbox that reads commitments out of email/Slack threads — is **not part of this build**. It belongs to a separate, later piece of work (Phase 2, tracked under its own plan), per the agreed build split. Because that forwarding pipeline does not exist yet, promises in this sub-task are **entered manually / seeded** through a command-line path for now, which is sufficient to build, validate, and demonstrate the digest and its acceptance gate. This is deliberate and expected — not a gap. We built the store with a single, stable entry point specifically so that when the Phase 2 forwarding work is delivered, it connects to that same entry point with **no change** to anything built here.

### What We Built

Built Vera's final two standing jobs and her formal build sign-off gate. This is the sub-task that completes the VERA-v2.2 agent: with it merged, Vera runs a full daily verification cycle (deploy/cron truth, revenue truth, and now promises/discrepancies) and can prove — on demand — that her checks actually catch a lie.

**Promise tracking**

Vera now keeps a live record of open commitments — who owns each one, how old it is, when it's due, and how much monthly recurring revenue is at risk if it slips. A new dedicated store holds these (separate from her append-only facts record, because a commitment's status changes over time as it moves from open to closed). Every commitment enters through a single, stable entry point.

As noted in the scope note above, the automatic feed for that entry point — the reply-forwarding mailbox — is a separate, later piece of work (Phase 2) and is intentionally not part of this build. For this sub-task, commitments are seeded manually through a command-line path, which is all that's needed to build and prove out the digest. When the Phase 2 forwarding work is delivered it will feed the same entry point automatically, with no change to anything built here — and no frontend work is required either now or later.

**Discrepancy report ("Doc claims X; live state shows Y")**

A daily report that reads the findings Vera's earlier reports (V2 deploy/cron, V3 revenue) already recorded that morning, and restates every mismatch in her constitution's plain claim-versus-reality voice — for example, "Claim: every paying customer has access. Live: 2 paying but no access." Critically, when a check could not be run (for example, deploy state cannot be verified from a non-production host), the report says so explicitly and never reports "couldn't verify" as if it were "all clear." It reports the earlier checks' verdicts rather than re-running them, so there is one authoritative result per morning, not two that could disagree.

**Overdue-promise digest**

The same report lists open commitments — overdue ones first, then the rest — each ordered by the revenue at risk, so the most consequential slip is always at the top.

**Seeded-discrepancy acceptance test (the build sign-off)**

Vera's constitution defines her single, literal build-acceptance criterion: she must catch a deliberately planted false claim checked against real system state. This sub-task delivers exactly that as a runnable check — it plants a false claim about which code commit production is running, runs Vera's real deploy checker against the actual live state, and confirms the discrepancy is caught. It is deterministic on any host (production or not) and writes nothing to the database. This gate now passes.

### Validation

- Seeded-discrepancy acceptance gate: verified end-to-end — the planted false claim was correctly caught by Vera's real deploy checker (exit code 0). This is Vera's formal build sign-off criterion.
- 14 automated tests covering every decision the report makes (claim-versus-reality translation, the "couldn't verify" versus "all clear" distinction, overdue/pending split and revenue-at-risk ordering, and the acceptance-gate comparator) — all passing.
- Full Vera regression: all 67 Vera tests (V2 + V3 + V4) passing; the change adds a new table only and has no impact on any existing test.
- Database migration validated via dry-run against the shared environment (creates one new table plus its read-only grant; no change to any existing table).
- During audit, one correctness issue was found and fixed before sign-off: an unverifiable deploy state ("couldn't check") was initially being reported as a confirmed discrepancy; it now correctly reports as unchecked, in line with Vera's accuracy-over-speed rule. A regression test was added for this case.
- Remaining deploy-time step: applying the new-table migration to the shared environment and the first scheduled live run — a single, reversible administrative step with zero impact on existing data, coordinated the same way as the earlier sub-tasks.

### Evidence

| File | Description |
|---|---|
| `src/agents/vera/checks/discrepancy_digest.py` | Discrepancy builder, promise digest, report renderer, and the seeded-discrepancy acceptance harness |
| `src/agents/vera/promises.py` | Promise store — the single entry point (record), close, and read paths |
| `src/core/models.py` | `VeraPromise` schema definition |
| `migrations/apply_vera_promises.py` | Promises table migration (table + index + read-only grant) |
| `src/agents/vera/__main__.py` | `--promise-digest`, `--seed-check`, and `--add-promise` command entry points |
| `scripts/cron/crontab.txt` | Daily 07:52 UTC schedule entry |
| `tests/agents/test_vera_discrepancy_digest.py` | 14 automated tests covering the report's logic and the acceptance-gate comparator |

---
