# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AI-powered distressed property intelligence platform for Hillsborough County, Florida. Scrapes public records (foreclosures, tax delinquencies, liens, code violations, permits, probate, evictions, bankruptcy, fire/flood/storm), loads them into a PostgreSQL hub-and-spoke database centered on `properties`, scores properties across 6 buyer verticals, and pushes leads to GoHighLevel CRM. Monetized via Stripe subscriptions served through a FastAPI API.

## Common Commands

```bash
# API server
uvicorn src.api.main:app --reload --port 8000

# Run all daily scrapers
python -m src.tasks.run_scrapers hillsborough

# Run a single scraper
python -m src.scrappers.foreclosures.foreclosure_engine

# Rescore all properties
python -m src.services.cds_engine --rescore-all

# Load data (init DB, load specific type, load all)
python scripts/load_data.py --init-db
python scripts/load_data.py --type violations
python scripts/load_data.py --all

# Database migrations
alembic revision --autogenerate -m "Description"
alembic upgrade head

# Tests
pytest tests/
pytest tests/test_foo.py::test_specific_function

# Install dependencies
pip install -r requirements.txt
```

## Architecture

### Data Pipeline Flow
Scrapers (`src/scrappers/`) → CSV/DataFrame → Loaders (`src/loaders/`) → PostgreSQL → CDS Engine (`src/services/cds_engine.py`) → GHL CRM push

### Hub-and-Spoke Database
Central `properties` table (~522k parcels) with 1:Many relationships to all distress signal tables (foreclosures, tax_delinquencies, code_violations, legal_and_liens, building_permits, legal_proceedings, incidents, deeds) plus 1:1 to `owners` and `financials`. Scoring results in `distress_scores` (one per property per day, with JSONB `vertical_scores`).

### Key Subsystems

- **Scrapers** (`src/scrappers/`): Playwright-based with AI fallback (browser-use + Claude). Each scraper is its own subpackage. Note the directory is spelled `scrappers` (double p).
- **Loaders** (`src/loaders/`): Inherit from `BaseLoader` in `base.py`. Use 3-strategy address matching (exact ILIKE → pg_trgm → rapidfuzz at 85% threshold). Owner matching at 75%.
- **CDS Engine** (`src/services/cds_engine.py`): Scores 6 verticals × 14 signals. Weights/thresholds in `config/scoring.py`. Recency bonuses, stacking bonuses, equity/absentee/contact bonuses. Lead tiers: Ultra Platinum → Platinum → Gold.
- **API** (`src/api/main.py`): FastAPI with Stripe webhooks, checkout, lead feed endpoints, static SPA frontend from `src/static/`.
- **Services**: `stripe_webhooks.py`, `stripe_service.py`, `ghl_webhook.py` handle external integrations.
- **Tasks** (`src/tasks/`): Scheduled jobs — scraper orchestration, Stripe reconciliation, grace period expiry, unmatched record rematching.

### Database Access Pattern
```python
from src.core.database import Database
db = Database()
with db.session_scope() as session:
    # Auto-commit on success, auto-rollback on exception
    session.add(obj)
```

### Configuration
- `config/settings.py`: Pydantic BaseSettings loading from `.env` (accessed via `get_settings()`, cached with `@lru_cache`)
- `config/constants.py`: Directory paths, scraper URLs, file patterns
- `config/scoring.py`: CDS engine weights, thresholds, tier definitions
- `config/counties.json`: Per-county portal URLs (currently Hillsborough only)
- `config/prompts/`: YAML prompt templates for AI-assisted scraping

### Models
All ORM models in `src/core/models.py`. Key patterns: polymorphic `record_type` on LegalAndLien, JSONB metadata on legal_proceedings and distress_scores, check constraints on enum fields.

## Important Notes

- The scrapers directory is `src/scrappers/` (not `scrapers`)
- `.gitignore` excludes `data/`, `*.txt`, `*.sh`, and `reports/` — these contain local scraping output
- Environment variables are required for operation — see `config/settings.py` for the full list. Core required: `DATABASE_URL`, `ANTHROPIC_API_KEY`. Optional feature-gated: Stripe, GHL, Oxylabs proxy, contact enrichment APIs
- Scrapers use Oxylabs proxy for IP rotation when configured
- County-specific configuration lives in `config/counties.json` and `src/utils/county_config.py`
