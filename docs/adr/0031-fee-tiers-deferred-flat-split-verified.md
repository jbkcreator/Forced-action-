# Deal-size fee-tier mechanism built; tier numbers deferred to product owner

Task 3.2 asked for tier logic on broker commission splits (vary the
platform/broker cut by deal size, lender, or another dimension). We **build the
full deal-size tier mechanism** and **defer only the tier numbers** (boundaries
and per-tier percentages), which are a product-owner input we do not have.

Tier basis was grilled to **deal size** (bands on `gross_amount_cents`), not
lender: lender tiers are combinatorial across lenders/programs and need a mapping
that does not exist, whereas deal size scales broker economics directly and is a
handful of rows.

## What ships

- **Schema (config-as-data):** `commission_splits` gains `min_gross_cents`
  (NOT NULL default 0) and `max_gross_cents` (nullable = unbounded). A split is a
  tier scoped to `[min_gross_cents, max_gross_cents)`. A new tier is a new row —
  no schema change, no deploy. `migrations/apply_fa_s3_fee_tiers.py` (idempotent).
- **Resolver:** `commission_ledger.resolve_split_config(session, gross)` maps a
  deal size to a `split_config_id`. Most-specific band wins — highest floor first,
  then narrowest ceiling — so a real tier beats the catch-all default. Returns
  None when no tier matches, so the caller can fall back to an explicit split.
- **Authoritative resolution at the money-write:** `post_commission()` accepts
  `split_config_id=None` and resolves from `gross_amount_cents`. This is the one
  place gross → `net_lines` happens, so the ledger always reflects the tier in
  force when the entry is posted. `broker_state_machine.transition(closed_won)`
  also resolves (fail-fast: a deal that can't be priced can't close), and the
  consumer passes an omitted split through.
- **Numbers deferred, safely:** the existing placeholder `platform_50_broker_50`
  is set to the catch-all band `[0, +inf)`, so every deal still resolves to 50/50
  until a PO inserts narrower tiers with signed-off percentages. No invented
  thresholds enter prod; behavior is unchanged until real tiers land.

Tests: `tests/test_loan_lane.py` — flat-split E2E, resolve-by-size,
specific-band-beats-catch-all, auto-resolve at `post_commission` (asserts tiered
`net_lines`), and `closed_won` without an explicit split.

## Considered Options

- **Ship a resolver with hardcoded placeholder thresholds** — rejected. Invented
  cutoffs baked into a money path force a code change the moment the PO supplies
  real numbers, and misstate margin in the meantime.
- **Defer everything (mechanism + numbers), only E2E-verify the flat split** —
  rejected. The task's revenue driver is the tier mechanism; a pure deferral
  delivers zero margin and does not satisfy the request. Only the *numbers*
  genuinely require the PO.
- **Lender-based tiers** — rejected as the basis (combinatorial, no mapping).
- **Build the mechanism, defer only the numbers (chosen)** — the tier machinery
  is live; adding a fee tier is a one-row insert. The catch-all preserves today's
  50/50 until the PO's bands and percentages are signed off.

## Consequences

- Adding a fee tier is a data operation: insert a `commission_splits` row with a
  `[min_gross_cents, max_gross_cents)` band and its `parties` percentages. Margin
  changes with no code or deploy.
- Overlapping bands resolve deterministically (highest floor, then narrowest
  ceiling); non-overlapping bands are the intended invariant. The catch-all
  `[0, +inf)` is the floor that guarantees every deal resolves to *something*.
- `post_commission` and `transition` resolve independently from the same table.
  Absent a tier edit in the seconds between close and post they agree; if a tier
  is edited in that window, the post-time value wins — correct, since that is the
  money-write. (`ponytail:` acceptable drift; add a persisted resolved-split
  column on `broker_transitions` only if audit needs close-time and post-time to
  be provably identical.)
- `commission_splits.parties` JSONB shape (`[{"party","pct"}]` →
  `[{"party","amount_cents"}]`) is unchanged; seeded tiers must follow it.
- `tests/test_broker_state_machine_e2e.py` is a **pre-v6 dead test file** (patches
  a removed `emit_event`, uses the old `prospect_id` lane signature); 10/11 of its
  tests already fail independent of this change. `tests/test_loan_lane.py` is the
  current v6 suite. Left as-is — a separate cleanup ticket.
