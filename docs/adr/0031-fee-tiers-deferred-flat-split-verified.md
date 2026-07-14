# Fee tiers deferred; flat commission split verified end-to-end

Task 3.2 asked for tier logic on broker commission splits (vary the
platform/broker cut by deal size, lender, or another dimension). We **defer all
tier logic** and instead E2E-verify the existing single flat split.

Rationale: no signed-off numbers exist to tier against. The only seeded split is
`platform_50_broker_50`, whose own apply script marks it "PLACEHOLDER (50/50),
pending business sign-off" (`scripts/apply_fa_s1_commission_ledger.py:54`). There
is no historical `closed_won` deal-size distribution to derive thresholds from,
so any cutoff we shipped would be invented fiction in a money path. Tier basis
was grilled to **deal size** (thresholds on `gross_amount_cents`) as the eventual
design, but the boundaries and per-tier percentages are a product-owner input we
do not have.

What ships now: a new E2E test
(`tests/test_loan_lane.py::test_flat_split_posts_correct_net_lines_e2e`)
that drives the full `enter_lane → assign → … → closed_won` chain through the real
`handle_commission_poster` consumer and asserts the persisted `net_lines` are the
correct 50/50 split of gross with no cents lost. `split_config_id` stays
caller-supplied; `compute_net_lines` / `post_commission` are untouched.

Note: `tests/test_broker_state_machine_e2e.py` is a **pre-v6, dead test file** —
it patches `emit_event` on modules that no longer emit events and calls
`enter_lane` with the old `prospect_id`-anchored signature (v6 anchors lanes on
`property_id`). 10 of its 11 tests already fail on `AttributeError` independent
of this change. `tests/test_loan_lane.py` is the current, v6-rewritten suite
(see its module docstring) and is where this new test was added.

## Considered Options

- **Ship a deal-size resolver with placeholder thresholds now** — rejected.
  Hardcoding invented cutoffs (e.g. <$5k / $5k–15k / >$15k) into a commission
  path bakes fiction into money math and forces a code change the moment the PO
  supplies real numbers.
- **Build a range-based resolver skeleton (min/max `gross_amount_cents` columns
  on `commission_splits`, PO fills rows later)** — deferred, not rejected. This is
  the recorded upgrade path. Rejected *for now* because it adds a schema column
  and resolver code with zero rows to resolve against, i.e. dead flexibility until
  the PO decision lands.
- **Lender-based tiers** — rejected as the basis: combinatorial across
  lenders/programs, needs a mapping we do not have. Deal size is the chosen
  eventual basis.
- **Defer tiers, E2E-verify the flat split (chosen)** — smallest correct change.
  The flat path is proven end-to-end; no invented thresholds enter prod.

## Consequences

- No tier resolver exists. Every closed_won deal uses the caller-supplied
  `split_config_id` (today only `platform_50_broker_50`).
- **Upgrade path when the PO supplies deal-size bands + per-tier splits:** add the
  new tiers as `commission_splits` rows via an idempotent
  `migrations/apply_fa_s3_fee_tiers.py`; add a resolver mapping
  `gross_amount_cents → split_config_id` (range match), called at
  `broker_state_machine.transition` before it reads the split. New tiers are new
  rows, not a schema change to `commission_splits` (its JSONB `parties` already
  models any split shape).
- `commission_splits.parties` JSONB shape is confirmed to match what
  `compute_net_lines` reads (`[{"party", "pct"}]` → `[{"party", "amount_cents"}]`);
  future seeded rows must follow it.
