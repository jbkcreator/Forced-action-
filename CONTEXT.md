# Forced Action — Public-Records Scraping Context

The language for how this platform collects public-records leads across Florida
counties. This is a glossary, not a spec — it defines what terms *mean*, not how
they're implemented.

## Language

### Collection model

**Signal**:
A category of public record we collect (eviction, probate, divorce, judgment, lien, deed, foreclosure, permit, …). The unit a scraper produces and a buyer vertical consumes.
_Avoid_: "record type" (that's the DB column), "scraper" (a Signal is the *what*, a scraper is the *how*).

**County Source**:
A configuration entry binding one **(County, Signal)** pair to the **Portal** that serves it, plus its URL and data format. Lives in the database, admin-editable.
_Avoid_: "config", "feed".

**Portal**:
A specific external website operated by a county (or its clerk) that we scrape — e.g. Pinellas `courtrecords.mypinellasclerk.gov`.
_Avoid_: "site", "source" (a Source is the config row; a Portal is the website).

**Portal Family**:
A *class* of Portal that behaves the same way and can be scraped by the same code, regardless of county — defined by its access wall + data shape. Current families: court-docket+reCAPTCHA (Pinellas courtrecords), court-docket+PerimeterX (Hillsborough HOVER), Official-Records/instrument# (ORI bulk export), static CSV directory.
_Avoid_: "portal type" used loosely.

**Portal Driver**:
The reusable code that knows how to scrape one **Portal Family**. One Driver serves many counties. Adding a county that reuses a known family = config only; a genuinely new family = one new Driver.
_Avoid_: "scraper strategy", "adapter" (pick Driver).

### Two-stage scraping

**Bulk Filing Scrape**:
Stage 1 — the daily pass that lists *which cases were filed* for a Signal in a date window (case number, parties' names, case type), and loads them. Breadth, not depth.
_Avoid_: "the scraper" (ambiguous with Stage 2).

**Docket Extraction**:
Stage 2 — opening one specific case by its **Court Case Number** to read the full docket (parties *with mailing address*, events, documents, financials). Depth, per case. Requires a real Court Case Number.
_Avoid_: "enrichment" (that word is already used for skip-trace/phone-email and HCPA).

**Court Case Number**:
A real court-assigned case identifier (e.g. Pinellas UCN `26-005543-ES`) that **Docket Extraction** can look up. Distinct from an **Instrument Number** (a recording id from an Official-Records Portal, e.g. `2026151201`) which is *not* court-lookup-able.
_Avoid_: conflating the two — this distinction decides whether Stage 2 is possible.

## Relationships

- A **County Source** binds one **(County, Signal)** to one **Portal**.
- A **Portal** belongs to exactly one **Portal Family**.
- A **Portal Family** is scraped by exactly one **Portal Driver**.
- A **Portal Driver** may expose both a **Bulk Filing Scrape** and a **Docket Extraction** for its family (they share the access/session code).
- A **Bulk Filing Scrape** produces cases; **Docket Extraction** deepens each case that carries a **Court Case Number**.
- Signals sourced from an Official-Records Portal carry an **Instrument Number**, so they cannot be **Docket-Extracted** until re-sourced from a court-docket Portal.

## Example dialogue

> **Dev:** "To add Polk County evictions, do I write a new scraper?"
> **Domain expert:** "No — Polk's eviction Portal is the same **Portal Family** as Pinellas's (court-docket + reCAPTCHA). You add a **County Source** row pointing the Polk eviction **Signal** at that Portal. The **Portal Driver** already exists."
> **Dev:** "And the defendant's mailing address?"
> **Domain expert:** "That's **Docket Extraction**, Stage 2 — only works because the **Bulk Filing Scrape** gave us a real **Court Case Number**. Judgments still come from Official Records with only an **Instrument Number**, so no Stage 2 for them yet."

## Flagged ambiguities

- "Enrichment" was overloaded (skip-trace phone/email, HCPA property data, *and* court docket). Resolved: Stage 2 is **Docket Extraction**; "enrichment" stays reserved for contact/property enrichment.
- "Source" meant both the config row and the website. Resolved: **County Source** = config; **Portal** = website.
