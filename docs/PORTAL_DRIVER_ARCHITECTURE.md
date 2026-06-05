# Portal-Driver Architecture — Scalable Multi-County Court Scraping

**Status:** Plan (2026-06-05). Evolves the foundation in
[`MULTI_COUNTY_SCRAPING_ARCHITECTURE.md`](./MULTI_COUNTY_SCRAPING_ARCHITECTURE.md)
(DB-backed county config + the three `scrape_mode` strategies) into a
**portal-family driver** model so new counties are added by *config, not code*.
Terminology is canonical in [`/CONTEXT.md`](../CONTEXT.md).

**Why now:** the Pinellas court-portal work (eviction/probate/divorce via
`courtrecords.mypinellasclerk.gov` + 2captcha, plus per-case docket extraction)
is complete but lives in county-named folders (`court_docket/pinellas/…`). That
shape implies "code per county." Hillsborough is incomplete in two specific
ways. This doc defines the structure that fixes both and scales to county #3+.

---

## 1. Goals & non-goals

**Goals**
- Adding a county that reuses a known **Portal Family** = a DB **County Source** row, **zero new Python**.
- Adding a genuinely new kind of portal = **one new Portal Driver**, nothing else.
- **Docket Extraction (Stage 2)** becomes a scheduled, first-class pass for *every* county — not a manual probe.
- Each **Portal Driver** declares its own **access policy** (cadence / stealth / proxy), so fragile portals (HOVER + PerimeterX) and cheap ones (Pinellas reCAPTCHA) coexist safely.

**Non-goals (deferred, see §9)**
- Judgments on the court portal (no filterable case type — stays on Official Records).
- Replacing the Official-Records (ORI) source for probate/divorce (dual-source dedup decision parked).
- Big-bang reorg — we migrate family-by-family (strangler).

---

## 2. Core concepts (see `/CONTEXT.md`)

| Term | One-liner |
|---|---|
| **Signal** | A record category (eviction, probate, divorce, judgment, lien…). |
| **County Source** | DB row binding `(County, Signal)` → a Portal + URL + format. |
| **Portal** | A specific county website we scrape. |
| **Portal Family** | A *class* of portal scraped by the same code (by access wall + data shape). |
| **Portal Driver** | The reusable code for one Portal Family; may expose **Bulk Filing Scrape** (Stage 1) and **Docket Extraction** (Stage 2). |
| **Court Case Number** vs **Instrument Number** | Court-lookup-able id (enables Stage 2) vs Official-Records recording id (does not). |

---

## 3. Layered architecture

```
┌──────────────────────────────────────────────────────────────────┐
│ Signal Engine            (county-agnostic orchestrator, per signal)│
│  evictions / probate / divorce / liens / …                         │
│  pipeline: select driver → bulk_scrape → process → filter →        │
│            dedup → load.  NO portal-specific branching.            │
└───────────────┬───────────────────────────────────────────────────┘
                │ REGISTRY[source.portal_family]
                ▼
┌──────────────────────────────────────────────────────────────────┐
│ Portal Driver            (one per Portal Family, serves N counties)│
│  court_records_recaptcha │ court_records_perimeterx │              │
│  official_records        │ static_csv               │              │
│  exposes: bulk_scrape(signal, county_source, window) -> Path       │
│           extract_docket(case_number, county_source) -> dict       │
│           access_policy (cadence / stealth / proxy / headful)      │
└───────────────┬───────────────────────────────────────────────────┘
                │ reads
                ▼
┌──────────────────────────────────────────────────────────────────┐
│ County Source (DB)   portal_family + url + output_format + flags   │
└──────────────────────────────────────────────────────────────────┘
```

**Routing key:** `County Source.portal_family`. The existing `scrape_mode` /
`playwright_code` / `output_format` demote to **driver-internal hints** (a court
driver may still run cached Playwright code internally).

---

## 4. Target file structure

Strangler migration — `portals/` is new; families move in one at a time.

```
src/scrapers/                              (existing dir is `scrappers/` — keep that spelling)
├── portals/                               ← NEW: one package per Portal Family
│   ├── __init__.py
│   ├── base.py                            ← PortalDriver ABC + AccessPolicy + BulkResult
│   ├── registry.py                        ← {portal_family → Driver}, get_driver(family)
│   │
│   ├── court_records_recaptcha/           ← Pinellas family (first migrated)
│   │   ├── __init__.py
│   │   ├── driver.py                      ← implements PortalDriver (bulk + docket)
│   │   ├── session.py                     ← stealth Chromium + 2captcha  (was court_session.py)
│   │   ├── bulk.py                         ← Stage 1  (was court_docket/pinellas/civil_filing.py)
│   │   ├── docket.py                       ← Stage 2  (was court_docket/pinellas/court_scraper.py)
│   │   └── agent_fallback.py               ← drift fallback (was court_agent_fallback.py)
│   │
│   ├── court_records_perimeterx/          ← Hillsborough HOVER family (WRAPPED, not rewritten)
│   │   ├── __init__.py
│   │   ├── driver.py                       ← thin adapter over existing hover code
│   │   ├── session.py                      ← (was court_docket/hillsborough/hover_session.py)
│   │   └── docket.py                       ← (was hover_scraper.py)  bulk via static_csv today
│   │
│   ├── official_records/                  ← ORI family (liens/deeds/judgments) — later pass
│   │   └── driver.py                       ← wraps lien_engine's ORI download + DocType routing
│   │
│   └── static_csv/                        ← Hillsborough clerk dailyfilings — later pass
│       └── driver.py                       ← wraps the requests-based directory listing
│
├── evictions/evictions_engine.py          ← becomes thin: get_driver(family).bulk_scrape(...)
├── probate/probate_engine.py              ← same
├── divorce/divorce_engine.py              ← same
├── liens/lien_engine.py                   ← later: delegate ORI download to official_records driver
└── court_docket/                          ← EMPTIED as families migrate; delete when done

src/tasks/
└── docket_extraction.py                   ← NEW: scheduled Stage 2 (see §6)

config/
└── constants.py                           ← PINELLAS_CASE_TYPE_KEYWORDS (done) +
                                             per-family defaults

docs/
├── PORTAL_DRIVER_ARCHITECTURE.md          ← this file
└── adr/                                    ← ADRs from §10
```

Note: repo dir is spelled `scrappers/` (double-p, do not rename — CLAUDE.md rule).
`portals/` is created **inside** `src/scrappers/`.

---

## 5. The PortalDriver contract (`portals/base.py`)

```python
@dataclass(frozen=True)
class AccessPolicy:
    headful: bool = False            # HOVER must be headed; Pinellas headless
    use_proxy: bool = False          # Pinellas off (US egress); per-family
    max_runs_per_day: int | None = None     # throttle; HOVER low, Pinellas high
    min_seconds_between_calls: float = 0.0   # per-case spacing for docket
    persistent_profile: str | None = None    # HOVER warmed Edge profile name
    notes: str = ""                  # e.g. "_pxvid flags if hammered" (hover-perimeterx)

class PortalDriver(ABC):
    family: str                      # registry key, e.g. "court_records_recaptcha"
    access: AccessPolicy

    @abstractmethod
    async def bulk_scrape(self, *, signal: str, county_source: dict,
                          start: str, end: str, dest_dir: Path) -> Path:
        """Stage 1 — return path to a downloaded export for `signal`."""

    def supports_docket(self) -> bool: return False

    async def extract_docket(self, *, case_number: str,
                             county_source: dict) -> dict:
        """Stage 2 — full case detail. Override in court families."""
        raise NotImplementedError
```

- `court_records_recaptcha`: implements both; `access` = headless, proxy off, high cadence, cheap captcha.
- `court_records_perimeterx` (HOVER): implements both; `access` = **headful, persistent Edge profile, `max_runs_per_day` low, spacing high** — encodes `[[hover-perimeterx]]` so the scheduler never hammers it.
- `official_records` / `static_csv`: bulk only, `supports_docket() == False`.

Signal engines never see these details — they call `get_driver(family).bulk_scrape(...)`.

---

## 6. Stage 2 — scheduled Docket Extraction (the Hillsborough gap (a))

New task `src/tasks/docket_extraction.py`, scheduled **after** the daily Stage 1 window:

1. Query `legal_proceedings` for **new rows that carry a Court Case Number** (UCN-shaped), per county, not yet docket-extracted.
2. Group by `(county, portal_family)`; for each, load the driver and **respect `access`** (`max_runs_per_day`, spacing, one warm session/profile per batch).
3. `driver.extract_docket(case_number=…)` per case → persist (see §7).
4. Skip Signals whose source yields only an **Instrument Number** (judgments/liens/deeds) — `supports_docket()`/case-number shape gates this automatically.

Cadence per family from `AccessPolicy`:
- Pinellas (cheap, robust): daily, full new-case set.
- HOVER (fragile): capped daily batch, wide spacing, single warm profile — **never** the firehose.

This is what makes Pinellas's "Stage 2 built but manual" and Hillsborough's
(a)+(c) gaps disappear: depth runs automatically *within each family's safe limits*.

---

## 7. Where Docket Extraction output lands

Stage 2 returns parsed `parties / events / documents / financial` (no per-tab CSV
on these portals). Decision for the plan:

- **Write back onto the existing case** (`legal_proceedings` row) the high-value
  fields the bulk list lacks — chiefly **party mailing address** (feeds skip-trace),
  plus a JSONB `docket_detail` blob (events/documents/financial) on the row's
  metadata. Avoids a new table family until volume justifies it.
- Keep a `docket_status` marker (`pending / extracted / no_case_number / failed`)
  so the scheduler is idempotent and the heartbeat monitor can watch it.

(If/when docket volume or query needs grow, promote `docket_detail` to normalized
`court_*` tables — an explicit later decision, not now.)

---

## 8. Migration plan (strangler — decision Q6)

**Phase 0 — scaffolding (no behavior change)**
- Add `portals/base.py` (ABC + AccessPolicy) and `portals/registry.py`.
- Add `portal_family` as a **`special_flags.portal_family`** key on County Sources
  (no migration). Write a **derivation shim** that back-fills it from current
  `(scrape_mode, output_format, special_flags, url-host)` for every existing row.

**Phase 1 — Pinellas courtrecords family (lowest risk, freshest code)**
- Move `court_docket/pinellas/{civil_filing,court_scraper,court_session,court_agent_fallback}.py`
  → `portals/court_records_recaptcha/{bulk,docket,session,agent_fallback}.py`.
- Wrap them in `driver.py` implementing `PortalDriver`.
- Repoint `evictions/probate/divorce` engines: replace the in-engine excel→2captcha
  branch with `get_driver(family).bulk_scrape(...)`.
- Register `court_records_recaptcha`.

**Phase 2 — Stage 2 scheduler**
- Ship `tasks/docket_extraction.py`; wire cron after the Stage-1 window.
- Pinellas runs at full cadence; verify against the 3 validated eviction cases.

**Phase 3 — Hillsborough HOVER, wrapped (not rewritten)**
- `portals/court_records_perimeterx/driver.py` adapts existing hover code as-is.
- Encode the PerimeterX `AccessPolicy` (headful, warm profile, low cap, spacing).
- HOVER joins the Stage-2 scheduler under its throttle → closes Hillsborough (a)+(c).

**Phase 4 — Official Records + static CSV (cleanup pass)**
- Wrap `lien_engine`'s ORI download as `official_records` driver; Hillsborough
  dailyfilings as `static_csv`. Remove duplicated routing from signal engines.
- Delete the emptied `court_docket/`.

**Phase 5 (optional) — promote `portal_family` to a real CHECK-constrained column**
- Once stable, migrate the JSONB key to a first-class column (per `[[db-changes-via-script]]`: write migration file, apply DDL via python script).

Each phase ships independently; the registry tolerates mixed migrated/un-migrated
families (un-migrated resolve to their current code path).

---

## 9. Adding county / signal / family — the payoff

- **New county, known family** (e.g. Polk evictions on a courtrecords-clone):
  one **County Source** row `portal_family="court_records_recaptcha"` + URL. **No code.**
- **New signal on a known family**: add its case-type keywords (e.g.
  `PINELLAS_CASE_TYPE_KEYWORDS`) + a County Source row.
- **New portal family** (a portal that behaves unlike any existing one): write one
  `portals/<family>/driver.py`, register it. Signal engines unchanged.

---

## 10. Deferred / open

- **Judgments on court portal** — no filterable case type; stays on Official Records.
- **ORI vs courtrecords for probate/divorce** — dual-source dedup parked; cut over once new feed proven (today both would double-count: ~660 probate / ~23 divorce ORI rows are Instrument-keyed, won't dedup against new UCN rows).
- **`portal_family` real column** — Phase 5.
- **ADR numbering collision** — two `0004-*` files in `docs/adr/`; renumber during this work.

## 11. ADRs to record (candidates)

These meet the hard-to-reverse + surprising + real-trade-off bar:
1. **Config-first portal-driver routing** — counties are config; portals are drivers keyed by family (not county). Rejected: per-county code folders.
2. **Driver carries AccessPolicy; scheduler obeys per-family throttle** — why HOVER isn't run at Pinellas cadence (PerimeterX fragility).
3. **Docket Extraction writes back onto `legal_proceedings` (+ JSONB), not new `court_*` tables (yet)** — chosen to avoid premature schema; revisit on volume.
```
