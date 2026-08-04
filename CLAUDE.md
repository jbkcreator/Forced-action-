# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AI-powered distressed property intelligence platform for Hillsborough County, FL. Scrapes public records (foreclosures, tax delinquencies, liens, code violations, permits, probate, evictions, bankruptcy, sunbiz, fire/flood/storm, insurance, divorce, roofing permits, property appraiser), loads into PostgreSQL hub-and-spoke DB centered on `properties`, scores via 6 buyer verticals, pushes leads to GoHighLevel + Synthflow, monetized via Stripe through FastAPI. Phase 2B adds LangGraph-driven Lifecycle agent runtime for retention/FOMO/abandonment flows.

## Common Commands

```bash
# API server
uvicorn src.api.main:app --reload --port 8000

# Lifecycle agents (separate process — must run alongside API)
python -m src.agents --serve          # production
python -m src.agents --health         # pre-flight check
python -m src.agents --migrate        # run LangGraph checkpoint migration

# Docker (production — both API + Lifecycle share same image)
docker compose build
docker compose up -d
docker compose logs -f lifecycle

# Run a single scraper
python -m src.scrappers.foreclosures.foreclosure_engine

# Rescore all properties
python -m src.services.cds_engine --rescore-all

# DB migrations (scripts-only — Alembic retired, see docs/adr/0024)
PYTHONPATH=. python migrations/apply_<name>.py  # NEW scripts go here; apply one idempotent DDL script to the shared DB
PYTHONPATH=. python scripts/apply_<name>.py     # pre-existing scripts only — stay in scripts/, do not move

# Tests
pytest tests/                                  # default (excludes scenario)
pytest -m scenario                             # opt-in sandbox e2e
pytest -m scenario_lifecycle                        # Lifecycle/LangGraph scenarios
pytest tests/test_foo.py::test_specific_function
```

## Architecture

### Data Pipeline
Scrapers (`src/scrappers/`) → CSV/DataFrame → Loaders (`src/loaders/`) → PostgreSQL → CDS Engine (`src/services/cds_engine.py`) → GHL/Synthflow CRM push.

**CDS = Composite Distress Score.** Scores 0–100 across 6 verticals. `config/scoring.py` is source of truth — `cds_engine.py` docstring is stale (wrong stacking window/cap/equity scope).

### Hub-and-Spoke Database
Central `properties` table (~522k parcels). 1:Many → foreclosures, tax_delinquencies, code_violations, legal_and_liens, building_permits, legal_proceedings, incidents, deeds, **tax_payment_history**. 1:1 → owners, financials. Scoring → `distress_scores` (one row per property per day, JSONB `vertical_scores`). `lifecycle_event_queue` — durable fallback table for Lifecycle events published when Redis is unavailable (fa072). All ORM models in `src/core/models.py`.

### Subsystems

- **Scrapers** (`src/scrappers/`): Playwright + playwright-stealth, AI fallback via browser-use + Anthropic Claude, Firecrawl for static. **Directory spelled `scrappers` (double p).** Proxy (Oxylabs) commented out from all scrapers **except** `foreclosures/`. Fire incidents: Tampa Fire Rescue JSON API (`ncapps.tampagov.net/callsforservice/TFR/GetTFRCallsForService`) — not HCSO portal. Storm/flood/fire use NWS forecast zones from `County.nws_zone` DB column (comma-separated for multi-zone counties, e.g. Hillsborough `FLZ151,FLZ251`). Both Hillsborough and Pinellas are in cron for all weather scrapers.

- **Loaders** (`src/loaders/`): Inherit `BaseLoader` (`base.py`). Property matching waterfall: (1) exact parcel_id, (2) address: ILIKE house# prefix → pg_trgm similarity → rapidfuzz token_sort_ratio ≥75%, (3) owner name: exact ilike → LIKE pattern → pg_trgm ≥75%. Three-tier outcome: ≥0.92 → **matched**; 0.75–0.92 → **pending_review**; <0.75 → **unmatched**. Thresholds in `config/matching.py`. Pinellas stopgap: `review_min=0.65`.

- **Connectors** (`src/connectors/`): Lifecycle Data Engine outcome-mining pipeline. Reads already-ingested, already-matched tables (`foreclosures`, `tax_deed_auctions`, appraiser `financials`, etc.) and stages labeled events into `outcome_candidates` (`OutcomeCandidate` model). `label_layer.py` (CDE-10) then promotes unconsumed candidates into `DealOutcome` (subscriber_id NULL, confidence_tier `public_record_inferred`, deterministic `source_ref` idempotency, terminal events only — cancelled/unqualified are consumed without promotion) and routes through the CDE-11 `deal_outcome_effects` seam. `registry.py` is static per-connector metadata (source_type, cadence, SLA); `runner.py` wraps a connector's run with `record_scraper_stats()` bookkeeping (no new scheduler — connectors are plain `scripts/cron/crontab.txt` entries like every other scraper); `resolve.py` (`resolve_or_quarantine`) is a thin wrapper over `BaseLoader.find_property_cascade`/`quarantine_unmatched` for the rare connector reading a genuinely new raw file (most connectors reuse the `property_id` already set at ingestion and never call this); `outcomes.py` defines `OutcomeCandidateData` + `upsert_outcome_candidate()`.

- **CDS Engine** (`src/services/cds_engine.py`): 6 verticals × 14+ signals. Formula: primary_score + stacking_bonus (STACKING_WINDOW_DAYS=180, cap=60) + absentee/contact/equity bonuses. Stacking-only signals: `insurance_claim`, `fire`, `storm_damage`, `flood_damage`, `building_permits` non-enforcement. Dead lead gate: deed transfer <45 days → zero investment verticals. Tiers: Ultra Platinum(95+) → Platinum(83+) → Gold(57+) → Silver(40+) → Bronze.

- **API** (`src/api/`): FastAPI. **Static files (React SPA) are served by Nginx — not FastAPI.** `main.py` mounts all routers. `deps.py` — shared primitives imported by all routers: `get_db`, `VALID_TIERS`, `VALID_VERTICALS`, `ZIP_RE`. Every router imports `get_db` from `deps.py`. Stripe webhooks at `/webhooks/stripe`, all API routes under `/api/`.

- **Services** (`src/services/`): `stripe_service.py` = outgoing. `stripe_webhooks.py` = incoming. `kill_switch_service.py` — `get_cached_metric()` + `get_kill_switch_status()` for non-agents code (use this, never import from `src.agents.tools` in services). `lead_pool_service.py` — `get_lead_pool()` + `get_zip_activity()` wrappers for API use. Skip-trace waterfall: Tracerfy → BatchData → PDL. **`_on_checkout_completed` (2026-07-20) is split fast/deferred**: the fast, synchronous half (subscriber tier/status update, ZIP-lock, founding count) commits and lets Stripe's ack return quickly; everything non-critical (GHL push, welcome/upgrade/first-leads/founder-alert emails, trial+saved-card Stripe lookups, referral/segmentation/attribution/A-B-holdout/Meta-CAPI/campaign-attribution/affiliate/subscriber-memory bookkeeping) runs in `_checkout_completed_deferred()`, scheduled via FastAPI `BackgroundTasks` from `handle_webhook()`/`stripe_webhook()` so it executes after the response is sent. One-shot, best-effort, no retry by design — see the docstring on `_on_checkout_completed` for the full rationale (this fixed both a ~30s+ webhook latency and a cascading-transaction-abort bug where one failed best-effort side-effect, missing `db.rollback()`, could silently roll back the whole subscriber upgrade). Every deferred DB-touching block uses `db.begin_nested()` (savepoints), not bare `try/except` — a bare except stops the Python exception but leaves Postgres's transaction aborted for every later statement. `background_tasks=None` (any non-HTTP caller — scripts, tests, admin replay) runs the deferred half inline on the same session instead, unchanged from pre-split behavior.

- **Agents** (`src/agents/`): LangGraph (Lifecycle) runtime. **Runs as a separate process/container from FastAPI.** Entry point: `python -m src.agents --serve`. API and Lifecycle communicate **exclusively** through Redis Queue (`lifecycle:queue` key, LPUSH/BRPOP) and Postgres NOTIFY (`lifecycle_events` channel). **Never call `dispatch_event()` directly from API/services/tasks** — use `publish_lifecycle_event()` from `src.agents.events.ingestion`. If Redis is unavailable, events fall back to `lifecycle_event_queue` Postgres table with 60s sweep. Supervisor routes events via dict lookup (`src/agents/router.py`). 10 graphs. Kill switch colors: green=send, yellow=fallback template, red=block. All decisions logged to `agent_decisions`. Guardrails in `config/lifecycle_guardrails.py`. `kill_switch_metric_ingest.get_cached_metric` re-exports from `kill_switch_service` — import from service layer, not tasks.

- **Tasks** (`src/tasks/`): Scheduled jobs. `daily_report.py` — CSV ops report (runs 08:10 UTC for both Hillsborough and Pinellas). `daily_dashboard.py` — 10-section PDF (23:30 UTC Mon-Sat), separate from daily_report. `dnc_refresh` — monthly Tracerfy DNC re-scrub. `learning_hygiene_sweep.py` — daily 10:15 UTC lesson-hygiene sweep (LEARN Layer 4), **dry-run unless `--apply`**.

### Lesson Hygiene (LEARN-v2.2 Layer 4)
Garbage collection for `lifecycle_playbook`. `src/services/learning_hygiene.py` = pure `decide(LessonStats) -> Verdict` + `sweep(db, *, now, dry_run, limit)`; thresholds in `config/learning_hygiene.py`; cron driver `src/tasks/learning_hygiene_sweep.py`. Calls the existing `supersede_recommendation` / `mark_contradicted` from `playbook_writer.py` — it decides *when*, never what a lesson means.

**Only two verdicts mutate.** Staleness is report-only: the two tools express "replaced" and "proven wrong", and old ≠ wrong. Other named, stored, digest-led states: `skip_unmeasurable` (agent_domain with no evidence feed), `skip_orphaned_source` (`source_id` resolves to nothing), `skip_excluded_kind`, `skip_untested`.

- **Evidence feed is `agent_decisions.playbook_id`** — `terminal_status` / `autonomy_class` / `overridden_at`, bucketed by an exclusive `CASE` so each decision counts once. **Non-evidence outranks contradiction**: a `failed` decision produced no outcome and is excluded from both numerator and denominator, else one outage retires the corpus. The sweep's own audit rows carry `playbook_id`, so the evidence query **must** filter `graph_name <> 'learning_hygiene'` or each run manufactures support for the lesson it just judged.
- **`CONTRADICTION_MIN_COUNT = 3` is constitutional**, not tunable (docs/constitutions/*.md, cited in `mark_contradicted`'s docstring). `CONTRADICTION_MIN_RATE_PCT` defaults to `0.0` (disabled) and can only ever spare a lesson.
- **Supersession is successor-driven** because `supersede_recommendation(session, old_id, new_id)` requires a FK-enforced `new_id` — a timer cannot call it. Successor identity is non-NULL `scope` equality; NULL never matches.
- **`anti_playbook` excluded, not inverted** — counter-evidence against a documented failure means the failure stopped, which is a different terminal state.
- Rails: schema precondition (verifies `apply_lifecycle_playbook_lessons_versioning.py` ran; **never applies it**), global feed health, blast radius `max(3, 20%)` of the *measurable* population. Audit rows go to `agent_decisions` — no new migration. See ADR 0034 + `config/learning_hygiene.py:validate_hygiene_config()`.
- **Tasks** (`src/tasks/`): Scheduled jobs. `daily_report.py` — CSV ops report (runs 08:10 UTC for both Hillsborough and Pinellas). `daily_dashboard.py` — 10-section PDF (23:30 UTC Mon-Sat), separate from daily_report. `dnc_refresh` — monthly Tracerfy DNC re-scrub. `venture_ladder_evaluator.py` — daily 09:30 UTC venture-ladder walk (CL4).

### County Config
County config is **DB-backed** via `counties` + `county_sources` tables — **not** `config/counties.json`. Read via `src/utils/county_config.py:get_county(county_id)` (5-min cache). `County.nws_zone` supports comma-separated values for multi-zone counties. Hillsborough: `FLZ151,FLZ251`. Pinellas: `FLZ050`.

### Venture Config (CLONE-v2.2 / CL3)
A **venture** is one business on this fleet: its Relay sending identity (Slack channel, Instantly campaign, sender, send window, daily ceiling, kill-switch key, brand) plus its geography (state, bankruptcy court) and its counties. One row per venture in `ventures`; every county belongs to one via `counties.venture_key`, and every Relay queue row via `relay_approval_queue.venture_key`. Venture #1 is `hillsborough_distress`.

Read via `src/utils/venture_config.py:get_venture_config(venture_key)` (5-min cache) — **never query `ventures` directly**. Missing/inactive row, or any NULL column, falls back to the matching `config/settings.py` value, so venture #1 is unchanged from pre-CL3. `county_config` derives `state` and `court` from the venture (both were hardcoded to Florida before CL3).

Relay is per-venture end to end: `run_sweep(venture_key=...)` filters the batch, the daily-ceiling Redis key is `relay_daily_sent:{venture}:{channel}:{date}`, and the Slack channel / Instantly campaign / footer brand come from the item's venture. **One sweep run = one venture** (a batch is homogeneous) — a second venture needs its own `--sweep --venture <key>` cron line and its own Instantly passthrough campaign. Onboard a new venture with `python -m src.services.venture_provisioning` (`--emit-template` → `--dry-run` → `--apply`); `playwright_code` is never cloned between counties and column mappings are opt-in. Runbook: `docs/venture-onboarding.md`.

### Venture Ladder (CLONE-v2.2 / CL4)
Seven-rung state machine over `ventures.ladder_stage`: `radar → probe → pilot → unit_economics → cell → spin_up → portfolio`. Venture #1 is seeded at `portfolio`. **A radar candidate is a real `ventures` row with `is_active=false`** — the CL3 resolver falls back to env settings for an inactive venture, so an unproven candidate structurally cannot govern sends.

`src/services/venture_ladder.py` = `evaluate` / `advance` / `presell_gate_blocked` / `maybe_auto_double` / `maybe_auto_double_cell` / `cell_reply_rates` / `record_evidence`. `src/services/clone_pack.py:assemble()` = the read-only "can this venture run?" object. `src/tasks/venture_ladder_evaluator.py` = cron driver (09:30 UTC) — **`cell → spin_up` is never advanced unattended** (needs `--advance-spin-up`; spin-up spends real money). Acceptance PASS/FAIL: `python scripts/harness/venture_spinup_acceptance.py` (one always-rolled-back transaction).

**Every gate declares `no_metric_behavior`** and `_gate_color()` honours it — never a blanket red for a missing metric. This is ADR 0006's lesson: `EXPANSION_GATES` treats `None` as red, requires all-green, and has therefore never permitted a launch. Gates are also named for what they measure (`send_failure_pct` not `bounce_pct`; `platform_cost_per_acquisition_usd` not `cac_usd`) — per-venture bounce and ad-spend data do not exist. Presell evidence = **refundable Stripe deposit**, `verified` only by machine check; accepted kinds in config. `advance()` sets `is_active=true` (same transaction as the stage update, never clearing it back to false) exactly on the `spin_up → portfolio` transition — the explicit go-live rung. Auto-double is two knobs: venture-level doubles `relay_daily_ceiling` (the only real send cap), cell-level doubles a cell's target *production* count — both gated on `AUTO_DOUBLE_ELIGIBLE_STAGES` (`is_active` AND stage ∈ {cell, spin_up, portfolio}), checked inside the service functions themselves so nothing bypasses it. Cooldown/idempotency read `venture_ladder_events`, never a Redis flag. See ADR 0033 + `docs/venture-ladder.md`.

### Configuration (`config/`)
- `settings.py`: Pydantic BaseSettings from `.env`, accessed via `get_settings()`.
- `agents.py`: `AgentsSettings(AppSettings)` — LangGraph-specific keys. `AGENTS_EVENT_SOURCE_REDIS=true`, `AGENTS_EVENT_SOURCE_POSTGRES=true` required for full event routing.
- `scoring.py`: CDS weights/thresholds — source of truth (not cds_engine.py docstring).
- `matching.py`: match thresholds.
- `learning_hygiene.py`: lesson-hygiene verdicts, evidence vocabularies (default-deny over the `agent_decisions` CHECK sets), `validate_hygiene_config()`, `config_snapshot()` frozen into every audit row.
- `venture_template.py`: `DEFAULT_VENTURE_KEY`, the copy-and-fill `VENTURE_TEMPLATE`, and `validate_venture_config()`.
- `venture_ladder.py`: `LADDER_STAGES`, per-stage `STAGE_GATES` (each with `direction` + `no_metric_behavior`), presell + auto-double constants, `validate_ladder_config()`.

### Deployment
Single `Dockerfile` at project root. `docker-compose.yml` runs `api` and `lifecycle` as two services from the same image with `network_mode: host` (Postgres + Redis run on the host). Nginx serves React SPA static files and proxies `/api/` + `/webhooks/` to FastAPI on port 8000.

## Tooling Rules (strict)

- **Language/runtime**: Python 3.11+.
- **Web framework**: FastAPI (no Flask/Django).
- **ORM**: SQLAlchemy 2.0 style. **Migrations**: scripts-only (Alembic retired — ADR 0024). A schema change = (1) update `src/core/models.py` (tests' `create_all` source of truth), (2) write an idempotent `migrations/apply_<name>.py` (`CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`), (3) run it once against the shared DB. **New migration scripts go in `migrations/`, not `scripts/`** — the ~200 pre-ADR-0024 scripts already in `scripts/apply_*.py` stay where they are; only new ones move. No new alembic revisions; git history + idempotency is the record. Legacy migrations archived under `legacy/alembic/`.
- **Settings**: Pydantic v2 + pydantic-settings. Never read `os.environ` directly outside `config/settings.py`.
- **HTTP**: `requests` for sync (`requests_get_with_retry` from `src/utils/http_helpers.py`), `httpx` if async. No `urllib`.
- **Scraping**: Playwright + playwright-stealth. Browser-use + Anthropic for AI fallback. Firecrawl for static. No Selenium.
- **Fuzzy matching**: rapidfuzz. No fuzzywuzzy.
- **Agents**: LangGraph 1.x with Postgres checkpointer. LangSmith for tracing. No raw Anthropic SDK loops for agent flows.
- **Lifecycle event dispatch**: `publish_lifecycle_event(event_dict)` from `src.agents.events.ingestion` — never `dispatch_event()` from API/services/tasks.
- **SMS**: Telnyx. All sends via `src/services/sms_compliance.send_sms` with explicit `message_type`. **Voice/AI calls**: Synthflow.
- **Phone numbers**: every read/write of a phone column MUST go through `src/services/phone_utils.normalize`.
- **Payments**: Stripe SDK ≥11. All webhook handlers in `src/services/stripe_webhooks.py`.
- **Cache/rate-limit**: Redis (server). Use `fakeredis` in tests/sandbox.
- **Testing**: pytest only. Markers: `scenario`, `scenario_lifecycle`, `scenario_platform`, `scenario_chat`. Unit tests in `tests/`, scenario in `tests/scenarios/`, agents in `tests/agents/`.
- **Logging**: stdlib `logging` via `config/logging.yaml`. No `print()` in `src/`.

## Important Notes

- Scrapers dir is `src/scrappers/` (double p) — do not rename.
- **Tax delinquency scraper DISABLED** — load via `POST /api/admin/upload/tax-delinquency` with admin JWT.
- **`cds_engine.py` docstring is stale** — always trust `config/scoring.py`.
- **Cron ordering (hard stagger):** scrapers 04:00–06:30 → CDS 07:00 → skip trace 07:30 → GHL sync 08:00.
- **GHL sync_status:** `pending_sync` → `synced` / `sync_failed`. Never lost.
- **Alembic is retired** (ADR 0024) — schema changes go through an idempotent apply script against the single shared DB, never new alembic revisions. **Write new apply scripts to `migrations/apply_*.py`** (pre-existing ones stay in `scripts/apply_*.py` — do not relocate them). Old migrations live in `legacy/alembic/`; the orphaned `alembic_version` table is left in place, harmless.
- **Lead Pack MVP status**: Partially sellable. Missing: county launch gate at checkout, minimum 5-lead count enforcement, 80% enrichment threshold check, `SentLead` rows in webhook fulfillment. Zero test coverage for lead pack flow.
- Required env: `DATABASE_URL`, `ANTHROPIC_API_KEY`, `REDIS_URL`. Feature-gated: Stripe, GHL, Synthflow, Telnyx, Oxylabs, LangSmith.

## Implementation Standards

### Code Quality
- All code must be production-grade: typed, structured, and readable without inline comments explaining what is obvious from the code itself.
- Log at the right level: `INFO` for normal flow milestones, `WARNING` for recoverable issues, `ERROR` for failures requiring attention. Never log raw secrets, tokens, passwords, phone numbers, or PII — log IDs and masked representations only.
- Every external call (Stripe, Telnyx, Anthropic, GHL, NWS, FEMA, Redis, Postgres) must be wrapped in a try/except with a meaningful log message that includes enough context to diagnose the failure without exposing internals.
- Functions must have a single clear responsibility. If a function needs a paragraph comment to explain what it does, split it.

### API Error Responses
- Return the semantically correct HTTP status code — never leak exception messages, stack traces, or internal field names to the client.
- `400` — malformed request or failed validation. `401` — missing/invalid auth. `403` — authenticated but not authorised. `404` — resource not found. `409` — conflict (duplicate, state violation). `402` — payment required / insufficient funds. `422` — valid JSON but business rule violation. `500` — unexpected server error (log the real cause, return a generic message).
- Error response shape must be consistent: `{"detail": "<human-readable message>"}`. Never include Python exception text in the response body.

### Data Structures and Algorithms
- **Choose the right structure for the access pattern before writing any loop.** If you are checking membership or deduplicating → `set`. If you are looking up by key → `dict`. If you are counting or grouping → `Counter` or `defaultdict`. Never iterate a list to check membership when a set exists.
- **Eliminate O(n²) before it ships.** When joining two collections in code (not SQL), build a lookup dict from the smaller one and iterate the larger once — O(n+m), not O(n×m). If you see a loop inside a loop over DB-sourced data, stop and redesign.
- **Pre-compute once, reuse many times.** Build lookup tables outside loops. If the same value is derived repeatedly inside a loop (regex compile, dict key construction, string formatting), move it above the loop.
- **Sort once.** If a collection needs to be accessed in order multiple times, sort it once and use `bisect` for subsequent range queries. Never call `sorted()` inside a loop.
- **Stream large result sets.** When processing all rows of a large table, use `yield_per(1000)` on the SQLAlchemy query or paginate with `LIMIT/OFFSET` in raw SQL. Never `.fetchall()` on a result set that could exceed tens of thousands of rows.
- **Batch I/O operations.** Group DB writes, API calls, and Redis operations into batches. A loop that calls `session.execute()` or an external API once per item is always wrong at scale — collect, then execute once.
- **Use generators for pipelines.** When transforming a large sequence through multiple steps, use generator functions (`yield`) to avoid materialising intermediate lists. Only collect into a list when random access or length is required.
- **Measure before optimising**, but design for efficiency from the start. If a function processes more than ~1k items, its time and space complexity must be considered, not assumed acceptable.

### Database and SQLAlchemy
- All DB access must go through SQLAlchemy. Direct `psycopg2` calls or raw connection string queries are forbidden outside migration scripts (`migrations/apply_*.py` for new ones, `scripts/apply_*.py` for the pre-existing set).
- **Use `sqlalchemy.text()` for all queries — do not use the SQLAlchemy ORM query API (`select(Model).where(...)`, `session.query(...)`) for data retrieval.** Write SQL directly via `session.execute(text("SELECT ..."), {"param": value})`. ORM is used only for `session.add()` / `session.delete()` on individual model instances and for Alembic schema definitions. Never concatenate user input into `text()` — always use named bind parameters.
- Minimise round trips: fetch all required data in one query using joins or CTEs rather than issuing multiple sequential queries. Never query inside a loop.
- Batch writes with `session.execute(insert(Model).values([...]))` when inserting more than ~10 rows. Commit once per batch, not once per row.
- Filter, sort, and paginate in SQL — not in Python after fetching all rows.
- Use `with_for_update(skip_locked=True)` for queue-style processing to avoid contention.
- Index columns that appear in `WHERE`, `ORDER BY`, or `JOIN` clauses on hot paths. Add the index in the same `migrations/apply_*.py` that adds the column.

## Self-Maintenance

Update after: new `src/` package, dependency change affecting tooling rules, new `config/*.py`, new pytest marker, new external integration, schema change in `models.py`, new scheduled task. Keep file <200 lines.
