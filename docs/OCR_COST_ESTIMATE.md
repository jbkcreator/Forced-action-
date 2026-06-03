# Court Records Enrichment — Cost Estimate

**Date:** 2026-06-03
**Scope:** Hillsborough + Pinellas counties
**Status:** Draft v2 — per-record rates measured in live bake-off (2026-06-03); fallback rates to be validated by 3–5 day shadow run
**Volumes:** exact counts from production database

---

## 1. Executive Summary

| | Estimated | Worst case |
|---|---|---|
| **One-time backlog** (19,291 docket lookups + 16,520 OCR extractions) | **~$445** | ~$965 |
| **Ongoing operations** (~189 docket lookups + ~153 OCR extractions per day) | **~$125 / month** | ~$250 / month |

The pipeline enriches distressed-property leads with court case data across four document types — evictions, probate, divorce, and judgments — plus lien match-quality upgrades. Costs are LLM API spend only; scraping infrastructure (Playwright browser time) carries no per-record fee.

---

## 2. Pipeline Overview

Each county exposes two separate portals:

| Portal role | Hillsborough | Pinellas |
|---|---|---|
| **Official Records** — recorded PDFs, keyed by instrument # | publicaccess.hillsclerk.com | officialrecords.mypinellasclerk.gov |
| **Court Dockets** — case data, keyed by case # | hover.hillsclerk.com (HOVER) | courtrecords.mypinellasclerk.gov |

Three cost-bearing stages:

```
Stage 1 — OCR (judgments + liens only)
  Official Records PDF → LLM extraction → case# / parcel# / address

Stage 2 — Docket lookup (all four doc types)
  case# → court portal → Summary / Parties / Events (structured data)

Stage 3 — Property re-match (liens + judgments)
  parcel#/address → properties table   [$0 — database queries only]
```

Key facts that shape the cost model:

- **Evictions, probate, and divorce already have case numbers in our DB** — they skip OCR entirely and go straight to docket lookup.
- **Judgments only have instrument numbers** — OCR on the recorded judgment PDF is the bridge to a case number.
- **Liens** get OCR for parcel/address match upgrades (82% of records currently match on owner name only); no docket lookup applies.
- Docket portals return structured data — no OCR needed there. Probate/divorce documents are sealed/confidential anyway; the structured Summary/Parties/Events is the entire product.

---

## 3. Architecture & Unit Costs

### Stage 1 — OCR extraction

**Haiku-primary + Sonnet-fallback** (head-to-head validated 2026-06-03: Haiku extracted the identical case number as Sonnet at 4× lower cost).

| Model | Role | Pricing | Measured $/record |
|---|---|---|---|
| claude-haiku-4-5 (PDF vision) | Primary | $1 / $5 per MTok | $0.002 – $0.008 |
| claude-sonnet-4-6 (PDF vision) | Fallback when confidence < 0.5 | $3 / $15 per MTok | $0.014 – $0.034 |

Blended unit cost at an assumed 20% fallback rate:

| Doc type | Typical pages | Blended $/record |
|---|---|---|
| Judgment (case # extraction) | 1–4 | **$0.008** |
| Lien (parcel / address / legal description) | 1–2 | **$0.005** |

### Stage 2 — Docket lookup

**Playwright-primary + browser-use AI fallback (Sonnet)** for both counties — the deterministic scraper handles the standard flow (search case# → Summary → Parties → Court Events) at $0 LLM; the AI agent fires only when the deterministic path fails.

| Path | Share (assumed) | $/case |
|---|---|---|
| Playwright (deterministic) | ~95% | $0.00 |
| browser-use + Sonnet (8–15 agent steps) | ~5% | $0.25 – $0.35 |

---

## 4. Volumes (production database, exact)

### Backlog (counties combined)

| Doc type | Records | Needs OCR | Needs docket lookup |
|---|---|---|---|
| Evictions | 2,253 | — | 2,253 |
| Probate | 2,455 | — | 2,455 |
| Divorce | 343 | — | 343 |
| Judgments | 14,240 | 14,240 | 14,240 |
| Liens | 2,280 | 2,280 | — |
| **Total** | **21,571** | **16,520** | **19,291** |

### Daily intake (30-day average, counties combined)

| Doc type | Per day |
|---|---|
| Evictions | 25.0 |
| Probate | 37.5 |
| Divorce | 8.6 |
| Judgments | 117.7 |
| Liens | 35.3 |
| **Total** | **224.1** |

---

## 5. Cost Detail

### 5.1 One-time backlog

**Stage 1 — OCR (16,520 records):**

| Component | Calculation | Cost |
|---|---|---|
| Judgments | 14,240 × $0.008 | $114 |
| Liens | 2,280 × $0.005 | $11 |
| **OCR subtotal** | | **$125** |

**Stage 2 — Docket lookups (19,291 cases, 5% AI fallback):**

| Doc type | Cases | Fallback $/case | Cost |
|---|---|---|---|
| Evictions | 2,253 | $0.30 | $34 |
| Probate | 2,455 | $0.25 | $31 |
| Divorce | 343 | $0.25 | $4 |
| Judgments | 14,240 | $0.35 | $249 |
| **Docket subtotal** | | | **$318** |

**Backlog total: ~$445** (worst case ~$965: all-Sonnet OCR + 10% fallback rate)

Runtime note: Hillsborough Official Records is HTTP-only (~10 s/record, parallelizable). Pinellas requires a browser session (~30 s/record, single Cloudflare-warmed profile) → its 1,056 OCR records take ~9 hours. Plan: batched nightly runs, newest-first, ~8 nights to clear.

### 5.2 Ongoing operations

**Stage 1 — OCR (~153 records/day):**

| Component | Calculation | Daily | Monthly |
|---|---|---|---|
| Judgments | 117.7 × $0.008 | $0.94 | $28 |
| Liens | 35.3 × $0.005 | $0.18 | $5 |
| **OCR subtotal** | | **$1.12** | **$33** |

**Stage 2 — Docket lookups (~189 cases/day, 5% AI fallback):**

| Doc type | Cases/day | Daily | Monthly |
|---|---|---|---|
| Evictions | 25.0 | $0.38 | $11 |
| Probate | 37.5 | $0.47 | $14 |
| Divorce | 8.6 | $0.11 | $3 |
| Judgments | 117.7 | $2.06 | $62 |
| **Docket subtotal** | | **$3.02** | **$90** |

**Ongoing total: ~$4.15/day ≈ $125/month** (worst case ~$250/month)

---

## 6. Reference Scenario — 100% AI-Agent Scraping (upper bound)

If the docket stage ran entirely on browser-use + Sonnet with no deterministic Playwright path:

| Doc type | Cases/day | $/case | Monthly |
|---|---|---|---|
| Evictions | 25.0 | $0.30 | $225 |
| Probate | 37.5 | $0.25 | $280 |
| Divorce | 8.6 | $0.25 | $65 |
| Judgments | 117.7 | $0.35 | $1,235 |
| **Total** | **188.8** | | **~$1,805/month** |

Backlog at 100% AI-agent: ~$5,800 one-time (19,291 lookups). The recommended Playwright-primary architecture delivers the same coverage at ~$90/month — **roughly 20× cheaper** — which is why the deterministic path is built first and the AI agent is reserved for failures.

---

## 7. Assumptions & Risks

| # | Assumption | Impact if wrong |
|---|---|---|
| 1 | OCR Sonnet-fallback rate 20% (3-doc sample) | 100% fallback raises OCR cost to ~$330 backlog / $72 month |
| 2 | Docket browser-use fallback rate 5%, $0.25–0.35/session (unmeasured — biggest uncertainty) | 10% fallback doubles docket-stage costs |
| 3 | PDF page counts 1–4 observed; certified copies can run longer | +$0.002/page (Haiku) per extra page |
| 4 | All test PDFs were scanned images — cheap text-layer path never fired | If some docs have text layers, real costs come in **below** estimate |
| 5 | Pinellas Cloudflare bypass stable (verified 2026-06-03); profile re-warming overhead not costed | Occasional manual re-warm; no per-record fee |
| 6 | Shadow-runner cost-calc bug (`_estimate_cost` prices fallback tokens at primary-model rate) | Fix required before shadow-run cost columns are trusted |

---

## 8. Next Steps

1. Fix `_estimate_cost` in `runner.py` to price tokens at the actual model used.
2. Switch `extractor.py` to Haiku-vision-primary + Sonnet-fallback (currently Sonnet-primary).
3. Build by-case# docket lookup module (Playwright primary, browser-use fallback) for HOVER + courtrecords.
4. Run 3–5 day shadow validation → replace assumed fallback rates with measured rates → finalize this estimate.
5. Begin batched backlog processing (nightly, newest-first).
