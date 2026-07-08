# CDS Cross-County Retune — Status & Outstanding Analysis

> ## ⏰ REVISIT 2026-09-01
>
> **Owner: dev@heu.ai** — re-evaluate the data-driven cutover when 90+ days of
> scoring history exists in `distress_scores` (earliest score_date today is
> 2026-04-25, so 90d window opens ~2026-07-24; September 1 gives a buffer).
>
> **Step 1 — check the monthly diagnostic snapshots**
>
> The cron entry at `scripts/cron/crontab.txt` (1st of each month @ 04:00 UTC)
> writes `data/diag/signal_predict_YYYY-MM.json`. By Sept you should have
> Jun / Jul / Aug snapshots. Look at the 90-day-window lifts across the
> months and check for three things:
>
> 1. ≥ 3 signals show **lift > 1.5 at 90 days** (foreclosure should be the
>    first to cross — currently at 0.54 at 30d due to FL's 8-14mo timeline).
> 2. **Base event rate at 90d ≥ 3%** (currently 1.24% at 30d).
> 3. **deed_transfers lift is no longer dominant** at 90d (currently 3.71
>    at 30d; if other signals catch up, it stops monopolizing the fit).
>
> If all three hold → green-light the data-driven cutover (Stage F).
> If only 1 or 2 hold → ship another round of hand-tunes based on what the
> diagnostic clarified, postpone Stage F by another 60-90 days.
> If none hold → the signals genuinely don't predict on our timescales;
> revisit signal collection / outcome definition / model approach.
>
> **Step 2 — when ready, run Stage B/C/E/F:**
>
> ```bash
> # 1. Regenerate training data with the now-viable 90-day window
> python -m src.services.scoring_training_data --outcome-window-days 90
>
> # 2. Fit per-vertical weights against the new CSV
> python -m src.services.scoring_fit --training-csv data/scoring_training/<id>.csv
>
> # 3. Shadow rescore with the fitted weights (writes to distress_scores_shadow)
> python -m src.services.cds_engine --rescore-all --shadow \
>     --fit-artifact data/scoring_fit/<id>.json
>
> # 4. Run the validation report — gate for Stage F
> python -m src.tasks.scoring_validation_report --window-days 90
> #    Exit 0 = all PASS (safe to cut over)
> #    Exit 1 = WARN (review carefully)
> #    Exit 2 = FAIL (do not cut over)
>
> # 5. If validation passes: paste fitted weights into config/scoring.py,
> #    flip Pinellas tier_visibility from "internal" to "public", commit.
>
> # 6. Apply live
> python -m src.services.cds_engine --rescore-all
> ```
>
> **Upstream data-pipeline gaps to fix before Stage F** (these are operational,
> not retune work — but they're blocking signal coverage):
>
> - `tax_delinquencies` — 17,531 of 17,534 rows have null amount AND null
>   years_delinquent (scraper placeholders). Source: admin upload at
>   `POST /api/admin/upload/tax-delinquency`. Confirm the upload is happening
>   *and* populating `total_amount_due` + `years_delinquent`.
> - `fire`, `storm_damage`, `flood_damage` — zero rows for these
>   `incident_type` values in the `incidents` table. The dedicated scrapers
>   (`fire_engine`, `storm_engine`, `flood_engine`) are scheduled but not
>   writing matching rows. Diagnose where they're actually writing.
> - Property-matching gap — only 1,554 of 60k+ `incidents` rows attach to a
>   scored property. Check `unmatched_records` for incidents.
>
> Run `python -m scripts.diagnose_zero_signals` periodically to re-check.

---

## Checkpoint 2026-07-03 — client asked to schedule the retune

Fresh diagnostic run (30d/60d windows, JSON in session scratchpad; monthly cron
confirmed installed and producing `data/diag/` snapshots on the server):

- **Trigger criteria: 1 of 3 met.** Only deed_transfers (2.91 @30d / 2.16 @60d)
  and hoa_liens (1.66 / 1.41) exceed lift 1.5 — need ≥3 signals at 90d. Base
  event rate 1.31% @30d / 1.81% @60d — needs ≥3% at 90d. Encouraging:
  foreclosures climbing 0.66→0.84 and irs_tax_liens 1.03→1.25 as the window
  widens, consistent with the timescale thesis; deed_transfers dominance is
  fading (2.91→2.16).
- **Win/loss layer is live but thin: 19 closed outcomes** (10 won / 9 lost;
  11 linked to scored properties). Not usable as a fit label (Stage C needs
  ≥30 positives per vertical). Use as a Stage E validation overlay; it becomes
  a co-label at ~hundreds of outcomes.
- **Data gaps still blocking signal coverage** (re-verified in prod DB):
  tax_delinquencies 3 of 50,002 rows have amount/years data; storm_damage /
  flood_damage zero rows; fire has 1,050 incident rows but only **7** attach to
  scored properties — the incidents→property matching gap is the bottleneck,
  not the scraper.

**Agreed schedule:**

| When | What |
|---|---|
| Now → mid-July | Fix data gaps: tax-delinquency upload must populate amounts; storm/flood scraper writes; incidents property-matching (fire 1,050→7). These raise the retune ceiling regardless of timing. |
| Aug 1 | First diagnostic snapshot with a meaningful 90d cohort (scores from Apr 15–May 3). |
| Sept 1 (unchanged) | Evaluate the 3 trigger criteria on Jun/Jul/Aug snapshots. If green → Stage B (90d) → Stage C fit → Stage E shadow + validation (incl. win/loss overlay) → Stage F cutover. If 1–2 hold → another hand-tune round, re-evaluate +60d. |

---

Working doc for the multi-stage retune triggered by:
1. **Tier inversion** — Bronze leads convert at 2.34%, Ultra Platinum at 0.91%. The score ranks leads in the opposite direction of conversion.
2. **Pinellas 94% Ultra Platinum** — the county-aware coverage normalizer was meant to discount Pinellas leads for missing signal types; in practice the label correlates more with "this county doesn't scrape code violations" than with distress.

Approach: keep the additive scoring formula; refit its knob values from observed conversion data; reframe the missing-signals discount as feature suppression so a Pinellas Ultra Platinum and a Hillsborough Ultra Platinum carry the same calibrated event probability.

Source plan: `~/.claude/plans/the-bronze-at-2-34-vs-ultra-platinum-at-happy-pretzel.md`.

---

## Current decision (2026-05-25)

**Ship the structural fix + two evidence-backed hand-tunes. Defer the full data-driven cutover until outcome history matures (~Sept 2026).**

What the diagnostic showed at 30 days (the only window we have data for):

```
deed_transfers        lift 3.71   ← only meaningful positive predictor at 30d
hoa_liens             lift 1.73   ← confirms existing boosted weight (68 in wholesalers)
judgment_liens        lift 1.27
building_permits      lift 1.20
                      ─────
mechanics_liens       lift 0.23   ← anti-predicts at 30d
code_violations       lift 0.35   ← anti-predicts at 30d
foreclosures          lift 0.54   ← timescale issue (FL foreclosure → sale is 8-14 months)
probate               lift 0.41   ← timescale issue (6-12 months)
tax_delinquencies     n=0         ← data load gap, investigate
fire/storm/flood      n=0         ← data load gap, investigate
```

The Stage C fit produced AUC 0.77 but **only because deed_transfers (lift 3.71) dominated everything**. The classical distress signals (foreclosure, probate, tax_delinquency) genuinely don't predict 30-day transactions — their natural timescales are months. So the fitted weights aren't ship-ready, **not because the approach is wrong, but because the outcome data is too young to fairly score them.**

We deferred the cutover until ~September 2026 when 90-day outcome history will exist for re-evaluation.

### Hand-tunes shipped in this PR (config/scoring.py)

Two weights had clear evidence for adjustment from the 30-day diagnostic. Both cut in the three investment verticals only (wholesalers / fix_flip / attorneys) where the "arms-length sale within window" outcome matches the lead intent. Contractor verticals left alone because their natural outcome is "permit / restoration work," not sale.

| Signal | Vertical | Before | After | Evidence |
|---|---|---|---|---|
| `code_violations` | wholesalers | 35 | 20 | 30d lift 0.35; weak alone in industry priors |
| `code_violations` | fix_flip | 50 | 30 | same |
| `code_violations` | attorneys | 30 | 20 | same |
| `mechanics_liens` | wholesalers | 55 | 35 | 30d lift 0.23; anti-predicts short-term |
| `mechanics_liens` | fix_flip | 60 | 40 | same |
| `mechanics_liens` | attorneys | 50 | 35 | same |

Both signals stacking-only contribution still functions (still ≥ STACKING_MIN_WEIGHT=30 for wholesalers / fix_flip mechanics_liens). The signals don't lose all influence — they just stop dominating when they shouldn't.

Hand-tunes that were **considered and rejected**:
- `foreclosures` down — 30d lift 0.54 is misleading because FL foreclosure timeline is 8-14 months. Leaving at 68/75/55 across investment verticals.
- `probate` down — same timescale concern (6-12 months to sale).
- `hoa_liens` up — already at boosted weight 68; diagnostic lift 1.73 confirms existing weight is right.

### What's live, what's deferred

| Piece | Status |
|---|---|
| Stage A — Pinellas tier-label suppression | ✅ Live. Keep until full retune ships. |
| Stage D — Engine reframe (the structural Pinellas fix) | ✅ Code merged, **ready to deploy** |
| Two hand-tunes in `config/scoring.py` | ✅ Merged in this PR |
| Stage B + C (training + fit infrastructure) | ✅ Code merged, **do not run for cutover yet** |
| Stage E (shadow + validation report) | ✅ Code merged, **do not run for cutover yet** |
| Stage F — Cutover with fitted weights | 🕒 **Deferred to ~Sept 2026** |
| Per-signal predictability diagnostic | ✅ Live — `src/tasks/signal_predictability_report.py` |

### Re-fit trigger criteria (revisit Stage F when ALL hold)

1. ≥ 90 days of scoring history accumulated in `distress_scores` (track via earliest `score_date`).
2. At least 3 signals show lift > 1.5 at 90-day window in the per-signal diagnostic — foreclosures should be the first to cross.
3. Diagnostic base event rate at 90 days ≥ 3% (current 30d base is 1.24%).

When all three hold:
- Re-run Stage B with `--outcome-window-days 90`.
- Re-run Stage C against the new CSV.
- Confirm AUC stays ≥ 0.75 *after* deed_transfers is no longer the dominant signal.
- Run Stage E shadow rescore + validation report.
- If validation passes, do Stage F cutover.

### Monthly diagnostic cadence

Schedule once per month (suggest via cron / scheduled GitHub Action):

```bash
python -m src.tasks.signal_predictability_report \
    --windows 30,60,90,180 \
    --json data/diag/signal_predict_$(date +%Y-%m).json
```

The JSON snapshots become the time-series record of "when did each signal turn predictive?" That history is what tells us a re-fit is worth running.

---

## Implementation status

| Stage | Scope | Code | Run / Cutover |
|---|---|---|---|
| **A — Tier-visibility stopgap** | Config flag + suppression in feed API, GHL webhook, Cora prompts | ✅ Done | ✅ Live (Pinellas hidden) |
| **B — Training dataset builder** | `src/services/scoring_training_data.py`, CSV output, per-county NaN mask | ✅ Done | ✅ Run once (96,408 rows, 30d window) |
| **C — Per-vertical fit** | `src/services/scoring_fit.py`, logistic regression, `coefs_to_knobs` mapper, JSON artifact | ✅ Done | ✅ Run once — AUC 0.77 but deed_transfers-dominated, not cutover-ready |
| **D — Engine reframe** | Removed `signal_coverage_pct` multiplier; `_score_vertical` now skips signals in `cfg.missing_signals` | ✅ Done | ✅ Ready to deploy (no rescore required to take effect) |
| **E — Shadow rescore + validation** | `--shadow` + `--fit-artifact` flags on engine CLI, `distress_scores_shadow` migration, validation report task | ✅ Done | ⏸️ Deferred — no point validating weights we know aren't ready |
| **F — Cutover** | Replace knobs in `config/scoring.py`, run `--rescore-all`, lift Pinellas suppression | 🕒 Deferred to ~Sept 2026 | Awaiting 90d outcome history |
| **Diagnostic — per-signal predictability** | `src/tasks/signal_predictability_report.py` (new) | ✅ Done | ✅ Run once (2026-05-25); schedule monthly going forward |
| **Hand-tunes — investment verticals** | `code_violations` and `mechanics_liens` cuts in wholesalers / fix_flip / attorneys | ✅ Done | ✅ In this PR |

### What's actually merged

- `config/scoring.py` — `tier_visibility` field on `ScoringConfig`, Pinellas override set to `"internal"`. **Two hand-tunes** (code_violations and mechanics_liens cuts in investment verticals — see "Hand-tunes shipped" table above).
- `src/api/main.py` — `_visible_tier_fields()` helper, applied at 4 lead-construction sites + `feed_stats` tier distribution.
- `src/services/ghl_webhook.py` — tier custom field and `cds-*` / `synthflow-*` tags suppressed for internal counties.
- `src/tasks/ghl_sync.py` — `county_id` plumbed into `score_data`.
- `src/agents/graphs/fomo.py`, `abandonment.py` — `lead_tier` swapped for a neutral phrase (`"matching"`) when suppressed.
- `src/services/scoring_training_data.py` — new module, CLI entrypoint, CSV writer.
- `src/services/scoring_fit.py` — new module, lazy sklearn import, CLI entrypoint, JSON artifact writer. **NaN handling**: signal columns drop rows (per-county mask), non-signal columns impute to 0. **Per-vertical population**: filters to `vertical_score > 0` so each vertical fits on a distinct training set.
- `src/services/cds_engine.py` — Stage D engine reframe: `_score_vertical` skips `cfg.missing_signals`; `signal_coverage_pct` multiplier removed; `--shadow` + `--fit-artifact` CLI flags.
- `src/tasks/scoring_validation_report.py` — new task with three gating checks (tier distribution / monotonicity / cross-county UP parity).
- `src/tasks/signal_predictability_report.py` — new diagnostic (per-signal lift across configurable windows).
- `alembic/versions/fa032_distress_scores_shadow.py` — mirror table migration.
- `alembic/versions/fa033_shadow_composite_index.py` — adds `(property_id, score_date DESC)` composite index to the shadow table; mirrors the live-table index from fa005. Without it, shadow rescore writes fall off a cliff once the shadow table grows past a few thousand rows.
- `src/services/cds_engine.py` — **DB-side optimization (2026-05-25)**: converted all 14 hot-path SQL sites from `WHERE x = ANY(:ids)` to `WHERE x IN (SELECT unnest(:ids::bigint[]))`. Production batches scale to ~50k IDs where `ANY()` can degenerate to sequential scans with array-membership tests; the unnest form gives the planner a known-small driving relation plus indexed inner lookup. Touched: `_flush_ghl_queue` (sync_status UPDATE), `_fetch_properties_by_ids`, all 10 queries in `_fetch_signals_for_batch` (owners, financials, code_violations, legal_and_liens, deeds, legal_proceedings, tax_delinquencies, foreclosures, building_permits, incidents), `_persist_score_batch` today + latest lookups, and the Gold-tier flash-scarcity ID lookup.
- `requirements.txt` — added `scikit-learn>=1.5.0`.
- Tests — 5 retune-test files, **111+ tests** across `tests/test_tier_visibility.py`, `tests/test_scoring_training_data.py`, `tests/test_scoring_fit.py`, `tests/test_engine_missing_signals.py`, `tests/test_scoring_validation_report.py`.

### Run history (production data)

| Date | Command | Output |
|---|---|---|
| 2026-05-25 | `scoring_training_data --outcome-window-days 30` | 96,408 training rows, 280 outcome events, 1.74% event rate |
| 2026-05-25 | `scoring_fit --training-csv <30d.csv>` (after per-vertical bug fix) | AUC 0.745-0.769 per vertical; deed_transfers dominant predictor |
| 2026-05-25 | `signal_predictability_report --windows 30` | Only deed_transfers (lift 3.71) + hoa_liens (lift 1.73) genuinely predict at 30d; classical distress signals all show lift < 1 due to timescale mismatch |

### What still has to ship

Almost nothing — most of the code is done. Remaining work is operational:

1. **Investigate the n=0 signals** (~10 min SQL). Four signals showed zero with-signal observations in the diagnostic: `tax_delinquencies`, `fire`, `storm_damage`, `flood_damage`. Either the data isn't loading or the SQL where-clause in `signal_predictability_report.py` is wrong. Quick check:
   ```sql
   SELECT COUNT(*) FROM tax_delinquencies WHERE total_amount_due IS NOT NULL OR years_delinquent IS NOT NULL;
   SELECT DISTINCT incident_type, COUNT(*) FROM incidents GROUP BY incident_type;
   ```
   If incident_type is `"Fire"` (capitalized) instead of `"fire"`, one-line fix in the diagnostic SQL.

2. **Schedule the monthly diagnostic** in cron / scheduled jobs. See "Monthly diagnostic cadence" above.

3. **At the re-fit trigger point (~September 2026)**: re-run Stage B with `--outcome-window-days 90`, Stage C against the new CSV, Stage E shadow rescore + validation report, then Stage F cutover.

---

## Analysis work that should happen *before* trusting the fit

The current implementation refits weights, but it assumes the underlying signals are predictive of conversion. That assumption is unverified — the only data point we have is the inverted tier conversion rates, which prove the *formula* is wrong but don't prove the *signals* are right. The following diagnostic checks should happen before Stage F cutover (or be acknowledged as deferred):

### 1. Per-signal predictability — single-axis conversion rate

For each signal type in isolation, compute: *of all properties carrying signal X at scoring time, what fraction transacted within 90 / 180 / 365 days?*

Compare to:
- The overall pool's transaction rate in that window (the "scored-leads baseline").
- An untriggered baseline if possible — properties WITHOUT the signal, matched on county/zip/value range. If we can't construct this, document the limitation; selection bias makes naive rates hard to interpret.

Expected outcomes from industry priors:
- **Foreclosure filing**: 30–50% transaction rate within 6 months.
- **Probate**: 20–40% within 12 months.
- **Tax delinquency**: 10–20% within 24 months.
- **Code violations alone**: weak signal individually; useful only in combination.
- **Liens + judgments**: distress correlation but slow timelines.

If actual numbers come in dramatically lower than these priors, the problem isn't the formula — it's that the signals don't predict on our timescale, our outcome proxy is mis-specified, or our data is dirty. In that case Stage F will produce a flat fit and the retune has lower ceiling than hoped.

This is implementable in <100 lines of SQL by extending `src/tasks/conversion_report.py` (`event_rate_per_signal` table). Recommend doing this before sklearn install — it tells us whether the fit is worth running.

### 2. Outcome window sensitivity

Current default outcome window is 90 days. Industry timelines vary by signal:
- Foreclosure → sale: 60–180 days.
- Probate → sale: 180–365 days.
- Tax delinquency → tax-deed sale: 730+ days.

Run the per-signal analysis above at 90/180/365 days. If a signal's predictiveness only emerges at 365 days, the fit needs a longer window for that signal type — otherwise its coefficient comes out near zero because the label hasn't materialized yet.

Decision needed: do we run the fit with a single window per vertical, or per-signal windows? Current implementation assumes a single window per fit run. Per-signal windows would require Stage B to emit multiple outcome columns and Stage C to consume them.

### 3. Selection bias — base rate among unscored properties

`conversion_report.py` compares scored leads' outcomes to *other scored leads' outcomes*. We don't know the base rate among the 522k parcels that *aren't* triggering distress signals. Without it, "UP converts at 0.91%" is ambiguous:
- If unscored properties transact at 0.1% in 90 days → UP at 0.91% is 9× baseline. Score works, just not as strongly as the tier label promised.
- If unscored properties transact at 1.5% → UP at 0.91% is **worse than random**. Score is anti-predictive.

Need a one-off SQL query that samples N unscored properties with similar county/zip/value attributes and computes their 90/180/365-day transaction rate. If we don't have property-appraiser data on all parcels (we do — HCPA enrichment), the comparison is straightforward.

### 4. Per-county per-vertical signal yield

Two questions:
- **Does Pinellas's data actually support a fit?** Pinellas is missing 6 signal types. The remaining 4–5 (probate, foreclosure, judgment_liens, irs_tax_liens, hoa_liens, mechanics_liens, deed_transfers) may be sparse enough that a per-vertical fit gets <30 positive events and the `coverage_warning` fires. If so, the fitted weights come from Hillsborough alone and we should be explicit that Pinellas inference is extrapolation.
- **Are signals vertical-specific?** Today every signal has a weight in every vertical. The fit may produce near-zero weights for many (signal, vertical) pairs, which would mean the per-vertical map is mostly noise. Worth tabulating: which (signal, vertical) coefficients come out significant.

### 5. Signal redundancy / collinearity

Many of our signals correlate with each other: a foreclosure filing often comes with a lis_pendens, a tax_delinquency, and a code_lien on the same property over time. Logistic regression with L2 will split a strong combined effect across the correlated coefficients. The fit might say "foreclosure: weight 30, tax_delinquency: weight 30" when the truth is "either signal alone: weight 50."

Implementable check: compute pairwise correlation between signal indicators on the training set. If two signals are >0.7 correlated, flag the pair in the Stage C artifact. The fit coefficients should be interpreted with that flag in mind.

### 6. Tier threshold calibration once weights land

The plan says `derive_lead_tier_thresholds` is a Stage F responsibility — currently a passthrough. Once we have a fitted model and shadow-rescored distributions, the tier breakpoints (currently 92 / 78 / 55 / 40) should be re-derived so that:
- The top X% of the shadow-scored population gets Ultra Platinum.
- The next Y% gets Platinum.
- etc.

X and Y values come from desired event-rate targets per tier (e.g. "Ultra Platinum should be the score band where Pr(event in 90d) ≥ 8%"). The targets are a business decision, not a statistical one — they should be set explicitly before the cutover.

---

## Decisions still open

These were not resolved during planning and will affect the fit's interpretability:

1. **Single outcome window vs per-signal windows.** Default: single 90-day window. Per-signal needs Stage B + Stage C extension.
2. **Fit on Hillsborough only vs all counties (with NaN drop for missing axes).** Current implementation: rows are dropped when ANY feature column is NaN, which effectively means Hillsborough-only when Pinellas has missing signals. Acceptable for v1; revisit when more counties launch.
3. **Tier threshold targets.** What event-rate floor defines each tier? Numbers needed before cutover.
4. **What to do if AUC comes in low (e.g., < 0.65).** Acceptable outcomes: ship anyway (still better than current), defer cutover and improve signals, or rebuild the scoring approach. Decide ahead of seeing the number, not after — bias risk.

---

## Quick reference

| Thing | Where |
|---|---|
| Stage B CSV (when produced) | `data/scoring_training/<run_id>.csv` |
| Stage C JSON artifact (when produced) | `data/scoring_fit/<run_id>.json` |
| Lift Pinellas tier suppression | Remove `"tier_visibility": "internal"` from `COUNTY_OVERRIDES["pinellas"]` in `config/scoring.py` |
| Current tier thresholds | `config/scoring.py` `LEAD_TIER_THRESHOLDS` (92/78/55/40) |
| Arms-length deed filter | `src/tasks/conversion_report.py:55-60` `_INTRA_FAMILY_DEED_PATTERNS` |
| Test suites | `tests/test_tier_visibility.py`, `tests/test_scoring_training_data.py`, `tests/test_scoring_fit.py` |
