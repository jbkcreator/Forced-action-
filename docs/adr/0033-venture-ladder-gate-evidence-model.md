# ADR 0033 — Venture-ladder gate/evidence model

**Status:** Accepted
**Date:** 2026-08-03
**Supersedes nothing. Directly informed by [[0006-expansion-cost-gate-computation]].**

## Context

CLONE-v2.2 CL3 made a second venture *configurable*: one row in `ventures`, a
5-minute resolver, per-venture Relay, cloned counties and sources. It did not
make one *decidable*. Three gaps remained:

1. **Nothing decided whether a venture should exist.** `venture_provisioning
   --apply` will provision any slug. There was no market score, no demand
   evidence, no cost check — the decision was a founder's judgement made in
   Slack. Workable for venture #2, not for venture #6.
2. **Nothing verified a venture worked after provisioning.** `--apply` returning
   `{"county_created": true, "cloned": 7}` can be true while the venture cannot
   send: source URLs silently inherited the template county's portals,
   `playwright_code` correctly dropped to `ai_only` and was never regenerated,
   the Instantly campaign was never created, the cron line was never added. The
   only check was a human running `--health` and reading the output.
3. **Nothing responded to performance.** `relay_daily_ceiling` is a static
   integer. The guardrail machinery in `config/lifecycle_guardrails.py` only
   ever throttles *down* (`sms_reply_rate` dropping falls back to static copy);
   nothing scaled *up* on success.

We also had a cautionary example close at hand. `EXPANSION_GATES` +
`_gate_color()` + `_all_green()` is a working gate machine, but per ADR 0006 it
is **inert**: `free_tier_cost_ratio` is hardcoded `None`, `_gate_color(None)`
returns `"red"`, `_all_green()` requires every gate green, so no county launch
or ICP channel can ever fire. A second finding in that ADR: `county_profitability`
reports "presence of revenue" under a name that says "net positive".

Reusing that machine's shape for venture creation would have reproduced both
failures one level up, where the blast radius is a whole business.

## Decision

### 1. A gate must declare what a missing metric means

Every entry in `config/venture_ladder.py:STAGE_GATES` carries
`no_metric_behavior: "green" | "yellow" | "red"`, and
`venture_ladder._gate_color()` honours it instead of defaulting to red.
`validate_ladder_config()` fails if any gate omits it, and a test asserts the
same, so the ADR-0006 failure cannot be reintroduced silently.

A `GateResult` also carries `imputed: bool`, true when the colour came from
`no_metric_behavior` rather than a measurement. "Passing" and "not measured,
treated as passing" are different facts and the digest shows which is which.

### 2. A gate is named for what it measures, never for what we wish it measured

Two gates were renamed during implementation rather than shipped as lies:

- `bounce_pct` → **`send_failure_pct`**. Nothing in this repo ingests
  per-venture Instantly bounce webhooks (`email_campaign_snapshots.bounces`
  belongs to the DBPR contractor campaigns, a different sending system). The
  available number is the share of dispatched Relay items that came back
  `failed`. That is a deliverability proxy, so it is named as one.
- `cac_usd` → **`platform_cost_per_acquisition_usd`**. Blended CAC needs ad
  spend, and `marketing_spend` is keyed by channel with no venture dimension —
  there is no correct way to split a channel's spend across ventures.

### 3. Cost attribution reports its own coverage, and refuses below a floor

`api_usage_logs` carries `subscriber_id`, never `venture_key`. Venture cost is
reached via `subscribers.county_id → counties.venture_key`, and rows with
`subscriber_id IS NULL` (scraping, batch jobs) cannot be attributed at all.

When attributable spend falls below `MIN_COST_ATTRIBUTION_RATIO` (40%) of fleet
spend in the window, the unit-economics metrics return `None` and log the
excluded share. A cost gate that silently ignores most of the spend is worse
than no gate.

### 4. Gates compute where they can, and only self-report where they must

The Clone-Pack is not a parallel artifact the ladder happens to sit next to —
two rungs read it directly:

- `probe`'s coverage gate calls `clone_pack.source_coverage()`, the single
  source of coverage truth. An earlier draft had near-identical coverage SQL in
  both modules, which is a gate and a readiness report free to drift apart about
  what "covered" means.
- `spin_up`'s `clone_pack_complete` gate calls `assemble()` at evaluation time.

Consequently there is **no** `cron_registered` evidence type. Whether a venture
has its own sweep cron line is checked against `scripts/cron/crontab.txt` live,
because a recorded claim goes stale the moment someone edits the crontab. The
only self-reported gate left at `spin_up` is `harness_pass`, which is
legitimate: it attests that an acceptance run happened, which nothing else can
observe after the fact.

`CLONE_PACK_IO` records what each rung consumes from and produces into the pack,
and `validate_ladder_config()` fails if a stage is missing from it — so a new
rung cannot be added without declaring the relationship.

### 5. One evidence table, typed by `evidence_type`

`venture_ladder_evidence` holds market scores, scrape samples, presell
commitments and harness results in one table with a JSONB `payload`. Every rung
records something, the gates only ever count rows and sum a field, and a new
evidence kind must not need a migration. Same idiom as
`connectors/outcomes.py`. (Anything the system can observe for itself is not
evidence at all — see decision 4.)

`verified` separates a claim from evidence: only rows set true by a machine
check count toward a gate. `UNIQUE (venture_key, evidence_type, source_ref)` is
what stops a Stripe webhook retry inflating a presell count; a NULL
`source_ref` stays insertable, which is correct for repeatable evidence.

### 6. A presell commitment is a refundable Stripe deposit

Real money (so it is real evidence), refundable (so people will agree to one — a
gate nobody can pass is the same as no venture), a Stripe object (so a webhook
verifies it with no human in the loop), and it carries an amount, so the
threshold is "5 people **and** $2,500" rather than a bare count.

`PRESELL_ACCEPTED_KINDS` is config, and each payload carries a `kind`
(`deposit` / `first_month` / `saved_card` / `letter_of_intent`). Widening what
counts as demand is a one-line change, not a rewrite. Built for deposits, door
left open.

### 7. A radar candidate is a real `ventures` row with `is_active = false`

CL3 already makes the resolver fall back to env settings for an inactive
venture, so an unproven candidate **structurally cannot** govern sends. That
gives one identity and one join key from radar to portfolio, with no separate
candidate table and no promotion step. `validate_venture_config()` becomes a
gate at spin-up rather than an insert-time constraint.

### 8. Auto-double is two rules on two different knobs

- **Venture-level** doubles `ventures.relay_daily_ceiling`, the real cap that
  `relay/guards.py:reserve_daily_slot()` enforces through
  `relay_daily_sent:{venture}:{channel}:{date}`.
- **Cell-level** has no ceiling to double — there is no per-cell send
  allocation. It doubles that cell's target *production* count (`limit` in
  `target_producer.produce_targets`), shifting the mix toward what works. The
  venture ceiling still caps total sends.

Multipliers are **derived** from the audit log (`2 ** doublings`, capped) rather
than stored, so cooldown, idempotency and the multiplier all read the same rows.

Guards, all load-bearing: minimum sample (8% of a dozen sends is one reply),
cooldown from the audit table not a Redis flag (a flag expires independently of
the ceiling it guards), a send-failure ceiling (a high reply rate next to a high
failure rate means the list is dirty), a hard max, and a config-cache flush
(without it a new ceiling silently does not apply for up to 5 minutes).

### 9. A forced advance exists, and is never silent

`advance(..., force=True)` promotes despite red gates, recording `actor` as
`"<actor> (forced)"`. Without an escape hatch, a gate whose metric cannot yet be
computed strands a venture forever — the ADR-0006 failure repeated. With one
that is unaudited, the gates are decoration. The audit row is the compromise.

## Consequences

**Good.** Venture creation is scored, gated and reversible. Every decision —
including every refusal — is a `venture_ladder_events` row with the computed
numbers frozen into `gate_results`, so a decision is reconstructable months
later without re-querying moved data. `scripts/harness/venture_spinup_acceptance.py`
returns a real PASS/FAIL, so a broken venture no longer looks like a working one.

**Costs and known gaps.**

- `send_failure_pct` is a proxy. A true per-venture bounce feed needs Instantly
  webhook ingestion; the threshold is where it plugs in.
- Per-venture ad-spend attribution does not exist, so there is no blended CAC
  gate — only the platform-cost slice.
- The unit-economics rung will report "no metric" in environments where ambient
  `subscriber_id IS NULL` spend dominates. That is the gate working; the harness
  reports it as WARN and force-advances that one rung rather than failing.
- Reply rate now depends on `outbound_drafts.replied_at`, dual-written by
  `opportunity_state.mark_replied()`. Cora's append-only file store stays
  canonical for opportunity state; the column exists because the file store is
  gitignored, single-process-locked and read by de-duplicating transitions, and
  scaling send volume off a number derived that way is a correctness bug.
