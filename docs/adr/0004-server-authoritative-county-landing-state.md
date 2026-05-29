# 0004 — County Landing Page state is server-authoritative

## Status
Accepted 2026-05-28.

## Context

The county waitlist feature introduces a public **County Landing Page** at
`/landing/{county_id}` for two purposes:

1. **Coming Soon** — collect waitlist signups for a **Candidate County**
   that has not yet launched. These entries reactivate on the County Launch
   Approval trigger (Stage 7 / `county_launch_runner`).
2. **Sold Out** — collect waitlist signups in a launched county that has
   one or more `(zip_code, vertical)` pairs currently `taken` or `grace`.
   These entries reactivate when a matching ZIP transitions to `available`.

Each Waitlist Entry row carries `waitlist_type ∈ {coming_soon, sold_out}`,
which decides **which reactivation runner consumes the row**. Getting that
value wrong sends entries to the wrong runner — a `sold_out` entry waiting
on ZIP 33701 would sit forever if mis-tagged `coming_soon` (no county
launch event is ever coming for an already-launched county), and a
`coming_soon` entry mis-tagged `sold_out` would never fire (no specific
ZIP transition is the trigger).

The obvious URL design is to encode the variant in the URL or query
string — e.g. `/landing/pinellas?state=sold-out` or
`/landing/pinellas/coming-soon`. This is SEO-friendly (distinct
canonicalisable URLs per state) and matches the shape of paid-ad
campaigns, which want to point at a stable URL per creative.

The problem: the URL is client-supplied. An ad creative built when a
county was `coming_soon` continues pointing at the `coming_soon` URL after
the county launches. A scraper or curious user can rewrite the query
string. In both cases the form would write a `waitlist_type` that
contradicts current reality.

## Decision

The County Landing Page state is computed **server-side** from current
county and ZIP territory status, and exposed via a single endpoint:

```
GET /api/counties/{county_id}/landing
→ 200 { county_id, county_display_name, waitlist_type, zip_count,
        vertical_waitlist_counts }
→ 404 if the county is neither a Candidate County nor a launched county
      with ≥1 taken/grace ZIP.
```

Resolution rules:

- County is a **Candidate County** (queued/approved, not launched)
  → `waitlist_type = "coming_soon"`.
- County is launched **and** ≥1 `(zip_code, vertical)` is `taken` or
  `grace` → `waitlist_type = "sold_out"`. The page renders as an
  ad-funnel wrapper around the existing `ZipChecker` (the user still
  enters a ZIP; the form posts a Waitlist Entry with the resolved
  `waitlist_type`).
- Any other state (launched and 100% available; unknown county) → 404.

The URL is **only** `/landing/{county_id}`. No `?state=` query parameter
is honoured. The form payload's `waitlist_type` field is echoed from the
landing endpoint's response; if the client sends a contradicting value,
the server rewrites it from its own resolution and logs the mismatch.

## Consequences

**Positive**
- `waitlist_type` cannot drift from reality. Reactivation routing is
  always against the current county state.
- A county that transitions `coming_soon → launched` mid-ad-flight
  automatically switches the landing page to `sold_out` (or 404 if no
  ZIPs are taken yet) without any campaign URL change.
- The form is dumb: it cannot be tricked into writing wrong-state rows
  by query-string manipulation or stale ad URLs.

**Negative**
- One URL per county, not one per variant. Paid-search/SEO loses the
  ability to point at distinct canonical URLs for the two variants. Ad
  creative copy must be generic enough to fit either variant, or the
  ad must check the landing API and bail to a different creative if the
  state changed.
- A county that flips from `coming_soon` to `launched` mid-day will
  show different page chrome to two visitors minutes apart. This is
  intentional but may surprise marketers reviewing analytics.
- A live race between the landing endpoint resolution and the County
  Launch Approval is possible: a `coming_soon` form submission can land
  immediately after the county is approved. The reactivation runner
  treats those as `coming_soon` (their stored type) and fires them on
  the launch event — correct.

**Neutral**
- `/landing/{county_id}/coming-soon` and `/landing/{county_id}/sold-out`
  may be added later as **redirect aliases** to `/landing/{county_id}`
  if SEO needs them, without changing the resolution rule. They would
  redirect, not branch.

## Alternatives considered

- **URL path variant** (`/landing/{slug}/sold-out`,
  `/landing/{slug}/coming-soon`): rejected — state drift, see above.
- **Query-string variant** (`?state=sold-out`): rejected for the same
  reason, with the added downside that query strings are easier to
  manipulate than path segments.
- **Client-side resolution** (the page calls a status API and renders
  accordingly, but trusts the URL to seed the form's `waitlist_type`):
  rejected — leaves the form payload spoofable.
- **Drop the page entirely, keep only `ZipChecker`**: rejected because
  paid-ad funnels need a county-scoped destination, not a generic
  homepage with a ZIP input.
