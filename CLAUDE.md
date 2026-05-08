# CLAUDE.md

Guidance for Claude Code working in this repo.

## Project Overview

AI-powered distressed property intelligence platform for Hillsborough County, FL. Scrapes public records (foreclosures, tax delinquencies, liens, code violations, permits, probate, evictions, bankruptcy, sunbiz, fire/flood/storm, insurance, divorce, roofing permits), loads into PostgreSQL hub-and-spoke DB centered on `properties`, scores via 6 buyer verticals, pushes leads to GoHighLevel + Synthflow, monetized via Stripe through FastAPI. Phase 2B adds LangGraph-driven Cora agent runtime for retention/FOMO/abandonment flows.

## Common Commands

```bash
# API server
uvicorn src.api.main:app --reload --port 8000

# Run a single scraper (scheduled via scripts/cron/crontab.txt)
python -m src.scrappers.foreclosures.foreclosure_engine

# Rescore all properties
python -m src.services.cds_engine --rescore-all

# Load data
python scripts/load_data.py --init-db
python scripts/load_data.py --type violations
python scripts/load_data.py --all

# DB migrations (Alembic)
alembic revision --autogenerate -m "Description"
alembic upgrade head

# Tests
pytest tests/                                  # default (excludes scenario)
pytest -m scenario                             # opt-in sandbox e2e
pytest -m scenario_cora                        # Cora/LangGraph scenarios
pytest -m scenario_platform                    # cron/webhook/API scenarios
pytest tests/test_foo.py::test_specific_function

# Install
pip install -r requirements.txt
playwright install
```

## Architecture

### Data Pipeline
Scrapers (`src/scrappers/`) → CSV/DataFrame → Loaders (`src/loaders/`) → PostgreSQL → CDS Engine (`src/services/cds_engine.py`) → GHL/Synthflow CRM push.

**CDS = Composite Distress Score.** Scores 0–100 across 6 verticals. `config/scoring.py` is source of truth — `cds_engine.py` docstring is stale (wrong stacking window/cap/equity scope).

### Hub-and-Spoke Database
Central `properties` table (~522k parcels). 1:Many → foreclosures, tax_delinquencies, code_violations, legal_and_liens, building_permits, legal_proceedings, incidents, deeds. 1:1 → owners, financials. Scoring → `distress_scores` (one row per property per day, JSONB `vertical_scores`).

### Subsystems

- **Scrapers** (`src/scrappers/`): Playwright + playwright-stealth, AI fallback via browser-use + Anthropic Claude, Firecrawl for static. Each scraper its own subpackage. **Directory spelled `scrappers` (double p).**
- **Loaders** (`src/loaders/`): Inherit `BaseLoader` (`base.py`). Property matching waterfall: (1) exact parcel_id, (2) address: ILIKE house# prefix → pg_trgm similarity → rapidfuzz token_sort_ratio ≥85%, (3) owner name: exact ilike → LIKE pattern both word orders → pg_trgm ≥75%. LLM tiebreaker (`llm_matcher.py`) only for borderline name matches, hard budget cap per run. No match → `quarantine_unmatched()` → `unmatched_records` table. Rematch job: Sunday 03:30 UTC. Both trgm steps wrapped in `begin_nested()` savepoint — pg_trgm absence won't abort outer transaction.
- **CDS Engine** (`src/services/cds_engine.py`): 6 verticals × 14+ signals. Weights/thresholds in `config/scoring.py`. Formula: primary_score (base+recency-age_decay) + stacking_bonus (STACKING_WINDOW_DAYS=180, cap=60) + absentee/contact/equity bonuses. Equity bonus applies to ALL 6 verticals (per-vertical rates). Stacking-only signals (`insurance_claim`, `fire`, `storm_damage`, `flood_damage`, `building_permits` non-enforcement) cannot be primary. Dead lead gate: deed transfer <45 days → zero investment verticals. Owner-occupied → zero wholesalers/fix_flip/attorneys. HCPA passive boosts (old building/long-term owner/declining value) stack on top. Batch commit every 1000 properties. After scoring: bulk UPDATE `sync_status='pending_sync'` — no GHL API calls in loop. Tiers: Ultra Platinum(95+) → Platinum(83+) → Gold(57+) → Silver(40+) → Bronze.
- **API** (`src/api/main.py` + `admin_router.py` + `sandbox_router.py`): FastAPI. Stripe webhooks, checkout, lead feed, JWT-protected admin upload, sandbox scenario harness, static SPA from `src/static/`.
- **Services** (`src/services/`): `stripe_service.py` = outgoing (create checkout sessions, founding vs regular price via SELECT FOR UPDATE). `stripe_webhooks.py` = incoming (5 events: checkout.session.completed → activate subscriber+lock ZIP+GHL; invoice.payment_failed → retry; subscription.deleted → 48hr grace). Idempotency via `stripe_webhook_events` table. GHL push decoupled: `ghl_webhook.py` does 4 API calls per lead (upsert contact → save contact_id → upsert opportunity → tags), 0.5s throttle + 429 backoff. `skip_trace.py` = BatchData API (200/day); `idi_fallback.py` = IDI fallback for BatchData misses (100/day) — both write to `enriched_contacts` + update `Owner.phone_1/email_1`. Monetization: wallet, allotment, bundle, lead_hold, wall, referral, ab, urgency, segmentation, revenue_signal, proactive_save.
- **Agents** (`src/agents/`): LangGraph (Cora) runtime. Supervisor routes events via dict lookup (no Claude call for routing). 3 graphs: `fomo` (competitor_acted_on_lead → SMS <60s), `abandonment` Wave1+Wave2 (bounce recovery, shared decision_id), `retention` (weekly summary per subscriber tier). Every graph passes through `decision_hierarchy` subgraph (6-step gate: guardrail → learning_card → segment+score → A/B → kill_switch) then `compose_and_send` (Claude writes SMS → compliance check → Twilio). Kill switch colors: green=send, yellow=fallback template, red=block. All decisions logged to `agent_decisions`. Guardrails in `config/cora_guardrails.py`.
- **Tasks** (`src/tasks/`): Scheduled jobs — scraper orchestration, Stripe reconcile, grace_expiry, rematch_unmatched, GHL sync, daily/weekly/monthly/annual reports, learning_card_job, lead_quality_monitor, match_rate_monitor, price_escalation, cora_anomaly_check, health_check, run_enrichment, revenue_pulse, weekly_one_pager.

### Database Access
```python
from src.core.database import Database
db = Database()
with db.session_scope() as session:
    # Auto-commit on success, auto-rollback on exception
    session.add(obj)
```
All ORM models in `src/core/models.py`. Polymorphic `record_type` on LegalAndLien. JSONB metadata on legal_proceedings + distress_scores. Check constraints on enum fields.

### Configuration (`config/`)
- `settings.py`: Pydantic BaseSettings from `.env`, accessed via `get_settings()` (cached `@lru_cache`).
- `constants.py`: dir paths, scraper URLs, file patterns.
- `scoring.py`: CDS weights, thresholds, tier defs.
- `counties.json` + `src/utils/county_config.py`: per-county portal URLs (Hillsborough only currently).
- `prompts/`: YAML prompt templates for AI scraping.
- `agents.py`, `cora_guardrails.py`, `revenue_ladder.py`, `revenue_pulse.py`, `logging.yaml`.

## Tooling Rules (strict)

- **Language/runtime**: Python 3.11+.
- **Web framework**: FastAPI (no Flask/Django).
- **ORM**: SQLAlchemy 2.0 style. **Migrations**: Alembic only — never edit schema by hand.
- **Settings**: Pydantic v2 + pydantic-settings. Never read `os.environ` directly outside `config/settings.py`.
- **HTTP**: `requests` for sync, `httpx` if async needed. No `urllib`.
- **Scraping**: Playwright + playwright-stealth. Browser-use + Anthropic for AI fallback. Firecrawl for static pages. No Selenium, no BeautifulSoup-only scrapers (BS4 is for parsing only).
- **Fuzzy matching**: rapidfuzz. No fuzzywuzzy.
- **Agents**: LangGraph 1.x with Postgres checkpointer. LangSmith for tracing. No raw Anthropic SDK loops for agent flows.
- **SMS**: Twilio. **Voice/AI calls**: Synthflow.
- **Payments**: Stripe SDK ≥11. All webhook handlers in `src/services/stripe_webhooks.py`.
- **Cache/rate-limit**: Redis (server). Use `fakeredis` in tests/sandbox.
- **Auth (admin)**: python-jose JWT.
- **Testing**: pytest only. Markers: `scenario`, `scenario_cora`, `scenario_platform`. Place unit tests in `tests/`, scenario tests in `tests/scenarios/`, agent tests in `tests/agents/`. Use `conftest.py` fixtures; no ad-hoc DB setup in tests.
- **Logging**: stdlib `logging` configured via `config/logging.yaml`. No `print()` in `src/`.
- **File format readers**: dbfread (DBF), xlrd (legacy XLS), pandas (CSV/XLSX).

## Important Notes

- Scrapers dir is `src/scrappers/` (double p) — do not rename.
- `.gitignore` excludes `data/`, `*.txt`, `*.sh`, `reports/` (local scraping output).
- Required env: `DATABASE_URL`, `ANTHROPIC_API_KEY`. Feature-gated: Stripe, GHL, Synthflow, Twilio, Oxylabs proxy, IDI/skip-trace APIs, LangSmith.
- Oxylabs proxy used for IP rotation when configured.
- County-specific config in `config/counties.json` + `src/utils/county_config.py`.
- Redis is server-only; locally use fakeredis shim.
- **Tax delinquency scraper DISABLED** (county portal blocks it). Load via `POST /api/admin/upload/tax-delinquency` with admin JWT instead.
- **Cron ordering dependency (hard-coded stagger, no inter-job signaling):** scrapers 04:00–06:30 → CDS scoring 07:00 → skip trace 07:30 → GHL sync 08:00. Overrun = stale data that day.
- **GHL sync_status state machine:** `pending_sync` (set by CDS bulk UPDATE) → `synced` (push OK) / `sync_failed` (retried next run). Never lost.
- **Subscriber feed:** `GET /api/feed/{uuid}` — UUID acts as auth token. Leads filtered by subscriber's ZIP territory + vertical + score threshold. No login required.
- **`cds_engine.py` docstring is stale** — wrong stacking window (says 60, is 180), wrong cap (says 40, is 60), wrong equity scope (says wholesalers/fix_flip only, is all 6). Always trust `config/scoring.py`.

## Self-Maintenance

Update this file automatically after any major architectural change. Triggers:

1. New top-level package under `src/` (e.g. new `src/agents/`-style subsystem) → add to **Subsystems**.
2. Dependency added/removed in `requirements.txt` that changes a **Tooling Rule** (e.g. swap ORM, add framework) → update **Tooling Rules**.
3. New `config/*.py` or `config/*.json` → add to **Configuration**.
4. New pytest marker in `pytest.ini` → add to commands + Tooling Rules.
5. New external integration (payments, CRM, telephony, LLM provider) → update **Subsystems** + **Important Notes**.
6. Schema-shape change in `src/core/models.py` (new hub-and-spoke table, new JSONB field, new polymorphic type) → update **Hub-and-Spoke Database**.
7. New scheduled task in `src/tasks/` that owns a recurring business flow → add to **Tasks** bullet.

When updating: keep file <200 lines, prefer specifics over generics, delete stale rules in the same edit.
