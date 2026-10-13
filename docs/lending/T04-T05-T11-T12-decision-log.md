# Decision log: T-04, T-05, T-11, T-12

Running log of what was done and what was decided for each task. Append a dated entry after every piece of work. Never rewrite earlier entries; add a correction entry instead.

Sources: `Chunk1_Developer_Split.md`, `Next_Deal_Lending-Questions_for_Josh_2026-10-08_deduplicated.md`.

## Related client questions per task

| Task | Questions |
|---|---|
| T-04 | A1, A3, B1(c), D-1, D-3, F4 |
| T-05 | A1, A2, A3, B1(c), B12, D-3, D4 |
| T-11 | D-1, D-2, D-3, D-4, D1, D3, D6, F5 |
| T-12 | F1, F2, F3, F4, F5, F6, F7, R6, A1, B8, D6, J10, J12 |

## Entry format

```
### YYYY-MM-DD, <what was done>
- Done: <changes, files, PR/branch>
- Decision: <choice made, and why>
- Questions applied: <Q ids and the answer used>
- Assumed defaults: <anything assumed>
- Open: <still unanswered or blocked>
```

## T-04: Lender Box + intake contracts

### 2026-10-13, task analysis (no code changed)
- Done: read `Chunk1_Developer_Split.md`, the Oct 8 questions doc, `src/lending/contracts.py`, `fakes.py`, `tests/lending/test_contracts.py`. The split says "NOT STARTED", but T-04 is already on `origin/dev` (commits `88b4fcf6`, `699880db`) and is imported by T-05, T-07, T-08, T-09, T-11, T-12 and R-1 code. The skill's `SOT.md` does not exist in this repo (it is FA Max), so sources used were the split, the questions doc and existing code.
- Decision: treat T-04 as a gap-fix pass, not a rebuild. Every change must be additive and optional, because six task modules already import these types. Acceptance stays: types importable, Fake test passes, no logic.
- Questions applied (gaps found against answers):
  - A1: no per-field status (confirmed / placeholder / confirm at submission) and no "subject to lender confirmation" marker on the result.
  - A3: result has only `fitting` / `non_fitting`; the agreed third outcome "possible, confirm on the call" has no place. Credit band is an int hack (`0` = below 640, `None` = unknown).
  - B1(c): reasons are free strings. Josh's formulas need a binding-cap field and flags (upside down, ARV over 3x), and his decline enum includes `loan_too_small` and others.
  - D-1: `LendingFlowLeadCreated` has no vendor id, and requires `email`. Josh said: missing lead id is stored and flagged (not rejected), dedupe on phone + email.
  - D-3: credit band and loan type spellings still open. No change possible; keep the int band.
  - F4: `RoutingTag` already matches (out-of-window and past dates go to NURTURE). No change.
- Assumed defaults: new fields are `Optional` with `None` defaults; no existing constructor call may break.
- Open: whether T-05 / T-08 already carry A1 status, the cap and the flags outside `contracts.py` (not yet verified); D-3; David's schema.

### 2026-10-13, production-execution — additive contract patch
- Done: patched `src/lending/contracts.py` and `tests/lending/test_contracts.py` on branch `feat/ndl-t04-contracts-gap-fix` (cut from `origin/dev`). 42/42 tests pass. No existing callers broken (verified imports + LendingFlowLeadCreated with and without email).
- Decision: all changes additive with `None`/`False`/empty defaults. No logic added (T-04 constraint).
- Questions applied:
  - G1 (A1): added `FieldStatus` enum (confirmed/placeholder/confirm_at_submission) and `LenderFit.subject_to_confirmation: bool = False`.
  - G2 (A3): added `LenderPossible` model and `LenderFitResult.possible: list[LenderPossible] = []`.
  - G3 (B1c): added `DeclineReason` enum (Josh's exact 14-code list from the Oct 8 addendum), `BindingCap` enum (requested/ltc/arv/max_loan), `SizingFlag` enum (upside_down/arv_over_3x), and `LenderMiss.reason_codes: list[DeclineReason] = []` alongside existing `reasons` strings.
  - G4 (D-1): added `LendingFlowLeadCreated.vendor_lead_id: Optional[str] = None` and `.needs_review: bool = False`; made `email` `Optional[str]` with default `None`.
  - F4: `RoutingTag` already correct. No change.
  - D-3: blocked on David's schema. No change.
- Assumed defaults: `subject_to_confirmation=False` (existing lenders unaffected); `email=None` (str still accepted).
- Open: A2 (cost formula, affects `total_borrower_cost` ranking); D-3 (David's schema); real lender rules due Oct 14 (T-05). T-05 must move unknown credit/eligibility to `possible` (not `non_fitting`). T-08 must use `BindingCap`/`SizingFlag` and Josh's sizing formulas. T-11 must set `needs_review=True` and relax the DB NOT NULL on `vendor_lead_id`.

### 2026-10-13, reconcile client answers with code (no code changed)
- Done: checked the six T-04 questions (A1, A3, B1(c), D-1, D-3, F4) against `origin/dev`. None has a Sarath dev comment, so all are Rule B (client answer is the requirement).
- Decision: verdicts: A1 Change, A3 Change, B1(c) Change, D-1 Change, D-3 Needs clarification, F4 Validated.
- Evidence found:
  - A1/A3: `lender_fit.py` puts unverified lenders and unknown credit into `non_fitting` with a reason (lines ~103-107, ~269-275). Josh wants "possible, confirm on the call" and placeholder values marked "subject to lender confirmation". Contracts have no place for either.
  - B1(c): `soft_approval/calc.py` rounds to $1 (Josh: down to $1,000), solves max price in closed form (Josh: step down in $1,000 steps), has no binding-cap, flags or `loan_too_small`.
  - D-1: `lendingflow.py:225-226` raises `ParseError("lead_id is required")`; Josh said store and flag, dedupe on phone + email. DB column `vendor_lead_id` is NOT NULL and UNIQUE (`models.py:461,501`).
- Assumed defaults: contract fields stay optional and additive. Calc, parser and DB changes belong to T-08 / T-11 / T-05, not T-04.
- Open: B1(a)/(b) (range letter) unanswered; D-3; A2 cost formula; real lender rules due Oct 14.

### 2026-10-13, testing-verification — T-04 contracts gap-fix
- Done: ran `testing-verification` skill against branch `feat/ndl-t04-contracts-gap-fix`.
- Done-When evidence:
  - DW1 (all new types importable): `py -c "from src.lending.contracts import FieldStatus, DeclineReason, BindingCap, SizingFlag, LenderPossible, ..."` → **PASS**
  - DW2 (FakeLenderFitEvaluator unit test passes): `tests/lending/test_contracts.py::TestFakeLenderFitEvaluator` — 5/5 PASSED → **PASS**
  - DW3 (no logic added): structural grep — only `def evaluate` (Protocol stub, no body), no conditionals/loops/returns in `contracts.py` → **PASS**
- Test categories: unit tests 42/42 PASSED. Categories 2-12 N/A (types-only PR, no DB, no sends, no migration, no external calls).
- Compliance-boundary (category 13): no DB write path, no borrower-financial fields, no send path reachable from contracts.py → **PASS**
- Full regression (category 14): `tests/lending/` — 81 passed, 49 skipped (200 s). Skips are pre-existing DB-backed tests; the 49 skip names are unchanged from `origin/dev`. → **PASS**
- Open: nothing; ready for `/code-review`.

### 2026-10-13, code-review — T-04 contracts gap-fix
- Done: reviewed diff `origin/dev...17bc30d6` across all 13 review sections. Decision: **Approved**.
- Findings:
  - Important (out of diff, T-11 must fix): `lendingflow_events.py:37` passes `email=lead.get("email") or ""`. With email now `Optional[str]=None`, consumers checking `is None` get `False` for absent-email leads. T-11 fix: change to `email=lead.get("email") or None`.
  - Minor (T-05 to action): `LenderPossible.why_unknown` has no `min_length=1` validator; docstring says always non-empty but Pydantic will not enforce it.
- No Critical findings. No logic in contracts.py. No borrower financial data. No send path.

### 2026-10-13, correction to the testing-verification entry
- The "full regression" line above overstated what ran. The 81 passed / 49 skipped result came from three files only (`test_lender_fit.py`, `test_enrichment.py`, `test_lendingflow.py`), not all of `tests/lending/`. The full `tests/lending/` run (background shell `b3f7uyuvs`) was never read, and `pytest tests/` was never run. The 49 skips were not checked against a Docker test DB (port 58123). Category 14 is therefore **PARTIAL**, not PASS.

### 2026-10-13, T-04 gaps and still-unanswered questions
- Decision: no PR opened yet. This entry only lists what is open for T-04.
- Assumed (not Josh's wording): `BindingCap` values (requested/ltc/arv/max_loan) and the two `SizingFlag` names are my reading of his B1(c) text.

**Gaps (not done or not verified)**
- Nothing yet reads the new types: T-05 (`possible`, `FieldStatus`, `reason_codes`), T-08 (`BindingCap`, `SizingFlag`), T-11 (`needs_review`, `vendor_lead_id`). `lender_fit.py` still puts unknown credit and unverified lenders into `non_fitting` (lines ~103-107, ~269-275).
- `lendingflow_events.py:37` passes `email=... or ""`, not `None`. T-11 must fix.
- `lendingflow.py:226` still raises on a missing lead id, and `models.py:584` keeps `vendor_lead_id` NOT NULL UNIQUE. Contract and DB disagree until T-11.
- `LenderPossible.why_unknown` has no non-empty validator (Minor, T-05).
- `FieldStatus` has no carrier field on any model yet; T-05 must decide where per-field status lives.
- Not run: full `pytest tests/`, full `tests/lending/`, the 49 skipped DB tests.

**Still unanswered (needs Josh / David)**
- **A2** cost formula — *Currently:* `total_borrower_cost` is Optional and unranked. *Question:* what is the formula? *If it differs:* T-05 ranking.
- **D-3** credit band and loan type spellings — *Currently:* int lower bound, `LoanType` enum as-is. *Question:* David's exact value set. *If it differs:* `credit_band_min_fico`, `LoanType` and the T-11 parser.
- **B1(a)/(b)** range letter — unanswered; blocks T-08 wording.
- **Real lender rules (due Oct 14)** — T-05 uses a placeholder box until then.
- **David's LendingFlow schema** — `vendor_lead_id` shape and whether it is always present.

### 2026-10-13, `LenderPossible.why_unknown` non-empty check: not added
- Done: checked `Internal_Engineering_Specification_and_Sprint_Directive.pdf` section 4.3. It asks only for fitting and non-fitting lenders, with explicit failure reasons for non-fitting ones. It has no "possible" bucket and no `why_unknown`, so a non-empty rule is not a client requirement.
- Decision: do not add `min_length=1` in T-04. The `possible` bucket itself comes from Josh's A3 answer; any rule on its contents belongs to T-05 once the engine fills it.
- Open: the `LenderPossible` docstring says "Always non-empty" but nothing enforces it. Either T-05 enforces it or the docstring is softened.

### 2026-10-13, full `tests/lending/` run on commit `17bc30d6`
- Done: `py -m pytest tests/lending/ -q` finished with **906 passed, 779 skipped, 0 failed**, 87 warnings, 33 min 30 s. This supersedes the 3-file figure in the testing-verification entry and covers its category 14 for the lending folder.
- Not verified: all 779 skips are DB-backed tests. `DATABASE_URL` is `localhost:55500`, db `local`, and Docker Desktop is not running, so Postgres was unreachable and the fixtures skip. Those tests did not execute. Full `pytest tests/` (outside lending) was not run.
- Open: start Docker, bring up the throwaway DB on port 58123, and rerun the skipped files against it.

### 2026-10-13, lending suite rerun against the throwaway DB (r1-test-pg, 127.0.0.1:58123, db `r1test`)
- Done: started Docker, started the existing `r1-test-pg` container (empty before: no lending tables; target host/port/db printed before running). `py -m pytest tests/lending/ -q` on commit `17bc30d6`: **1447 passed, 31 skipped, 191 failed, 16 errors** in 3 min 28 s. The DB-backed tests that were skipped before now ran.
- Cause of the failures: the empty DB lacks core app tables (`dnc_phone_checks` 65, `fa_max_persons` 48, `sms_opt_outs` 32, `subscribers` 15 undefined-table errors). They cluster in 18 files (consent/opt-out webhooks, dialer load/sweep, reminder worker, booking, compliance filter).
- Baseline check: the same 18 files on clean `origin/dev` (temporary worktree, same DB): **191 failed, 16 errors**, identical. So none of the failures come from the T-04 change. Worktree removed afterwards.
- Not verified: those 18 files have still never passed on this DB. They need the core app schema applied to the throwaway DB first (not done; no schema was created or altered, all test DDL is rolled back). Full `pytest tests/` outside lending was not run.

## T-05: Lender rules matrix + `evaluate_lender_fit`

### 2026-10-13, reconcile client answers with code (no code changed)
- Done: checked A1, A2, A3, B1(c), B12, D-3, D4 against `lender_fit.py`, `config/lender_matrix.py`, `soft_approval/service.py` and `config/lending_soft_approval.py`. T-05 already exists on this branch, so this is a gap pass. Only B12 has a Sarath dev comment (Rule A); the rest are Rule B.
- Decision (verdicts): Change: A1, A2 ranking rule, A3, B1(c) decline codes. Needs clarification: A1 experience window, A2 cost formula, B1(c) cap-versus-decline, D-3, D4. Validated: B12.
- Evidence found:
  - A1: unverified lender goes to `non_fitting` (`lender_fit.py:103-109`); `verified` is one flag per lender, not per field; rules live in a Python file (edit means deploy); no placeholder box; missing `max_ltv`/`max_ltc` skips the check; score shows with partial rules; purchase and rehab shares live in `config/lending_soft_approval.py`, not in the lender rules.
  - A2: sort is by cost only (`lender_fit.py:131`); Backflip-first not implemented; Backflip is two rows with no grouping.
  - A3: unknown credit is a decline reason (`lender_fit.py:268-282`); band 640 against floor 680 is declined though it straddles; `LenderFitResult.possible` (T-04) is never filled.
  - B1(c): `LenderMiss.reason_codes` (T-04) is never set; no `loan_too_small`. Sizing math stays in T-08.
  - B12: `SOFT_APPROVAL_LOAN_TYPES = {FIX_AND_FLIP}` (`config/lending_soft_approval.py:20`), gated at `soft_approval/service.py:134`.
- Assumed defaults: keep the provisional cost formula (points + spread x 12 months), labelled provisional; keep the 36-month experience window as a placeholder; keep the int credit-band encoding (0 = below 640, None = unknown).
- Open (plan questions, answers pending): see the task-analysis entry below.
- Open: A2 cost definition; A1 experience window; B1(c) whether the pre-call check caps or declines; D-3 (David); D4 (counsel's blocked states); Josh's real lender rules due Oct 14. R-1 lender routing tables not found on this branch (unchecked elsewhere). Callers to keep working: `soft_approval/service.py`, `enrichment/fit_card.py`, `src/services/lender_box.py`.

### 2026-10-13, task analysis for the T-05 gap-fix (no code changed)
- Done: planned the four Change items (A1, A2 ranking, A3, B1(c) decline codes). Branch: stack on `feat/ndl-t04-contracts-gap-fix` (real dependency: PR #357 not merged); cherry-pick or rebase, no merge commits.
- Findings: `enrichment/fit_card.py` (T-12) already re-sorts Backflip first, withholds the score until all rows are verified and reports band straddles, but the evaluator itself still sorts by cost and the soft-approval picker follows the evaluator's order, so the rules move into `lender_fit.py` and `fit_card.py` stays untouched. Josh's answers contradict the evaluator in four places (unverified auto-decline, unknown credit declines, missing cap skips the check, score over partial rules); `test_unverified_lender_never_in_fitting` encodes the old behaviour and gets rewritten. Spec Test 2 needs exact decline reasons, so known placeholder values still decline.
- Decision (plan): per-field status on `LenderRules` plus a generic placeholder box; evaluate unverified lenders into fitting (flagged `subject_to_confirmation`), `possible` or `non_fitting`; unknown or straddling eligibility goes to `possible`; set `reason_codes`; Backflip first in the evaluator; score None until all lenders confirmed. Sizing math and soft-approval scope unchanged (T-08, B12).
- Assumed defaults (pending answers): Q1 a known placeholder value declines, with "(unconfirmed rule)" in the reason; Q2 the four empty lenders inherit the placeholder box field by field; Q3 keep the unsourced shell loan types, marked placeholder; Q4 unknown deal facts (purchase, rehab, ARV, property type) give `possible` with `missing_fields`; Q5 ship config-based rules for Oct 14, DB-backed loader only after a team decision (may overlap R-1 tables); Q6 loan-type mismatch uses `other` (no enum code).
- Open: Q1 to Q6 above; A2 cost; A1 experience window; B1(c) cap versus decline; D-3; D4; real rules due Oct 14. A `credit_band_max_fico` contract field was considered and deferred until D-3.

### 2026-10-13, production-execution, testing-verification and code-review for the T-05 gap-fix (branch `feat/ndl-t05-lender-fit-gap-fix`, uncommitted)
- Done: `config/lender_matrix.py` (per-field `field_status`, `PARAMETER_FIELDS`, `PLACEHOLDER_BOX`, `field_status_of`, `fully_confirmed`, `effective_rules`, validation), `src/lending/lender_fit.py` (rewritten: three outcomes fitting / possible / non_fitting, `reason_codes`, `subject_to_confirmation`, Backflip-first ranking, score None until every lender is confirmed), `tests/lending/test_lender_fit.py` (old behaviour tests rewritten, about 20 new). `soft_approval/`, `fit_card.py` and `lender_box.py` untouched.
- Josh's answers applied: A1 (status per field, placeholder box, unknown cap uses placeholder not skipped, "subject to lender confirmation", score hidden until all confirmed), A2 (Backflip first, others cheapest first), A3 (unknown or straddling credit/state goes to `possible`), B1(c) (decline codes incl. `loan_too_small`; sizing math stays T-08), B12 unchanged.
- Assumed defaults (not Josh's words): a known placeholder value still declines, reason ends "(unconfirmed rule)"; unknown deal facts (purchase, rehab, ARV, property type, experience) give `possible`; a positive credit band below a floor straddles it (band read as "this value or higher"); loan-type mismatch and min purchase price use code `other`.
- Evidence: `test_lender_fit.py`, `test_contracts.py`, `test_enrichment.py`, `test_soft_approval.py` = 137 passed, 63 skipped (no DB). Full `tests/lending/` on the throwaway DB (127.0.0.1:58123, db `r1test`): 1467 passed, 31 skipped, 191 failed, 16 errors; the T-04 baseline on the same DB was 1447 passed, 191 failed, 16 errors (+20 passed = the new tests). One failing test name differs (`test_the_endpoint_is_rate_limited_per_ip` instead of `test_deploy_preflight_passes_when_nginx_routes_lending_to_8010`); the rate-limit test also fails alone with the T-05 change stashed, so it is order-dependent and pre-existing. Not run: full `pytest tests/` outside lending.
- Review findings (Important, outside or beyond this diff): (1) `enrichment/fit_card.py` ignores `possible`, so "possible, confirm on the call" never reaches the card (T-12). (2) A1 "data edit, not a code deploy" is NOT met: rules still live in `config/lender_matrix.py`, and per-lender purchase/rehab shares stay in `config/lending_soft_approval.py` (Q5). Minor: `PLACEHOLDER_BOX` is a mutable module dict; a closed band such as 620-679 against a 680 floor is `possible`, not a decline, until D-3 gives band bounds.
- Open: Q1 to Q6 from the plan, A2 cost, A1 window, B1(c) cap versus decline, D-3, D4, Josh's real rules due Oct 14.

### 2026-10-13, fit card now shows `possible` lenders (closes review finding 1; small T-12 change on the T-05 branch)
- Question asked: is showing "possible" on the card an internal engineering choice or client side? Client side: Josh's A3 answer says an unknown-eligibility lender shows as "possible, confirm on the call", not a fit or a decline. The internal spec (section 4.3) is silent on it (only a fit score on the card).
- Done: `enrichment/fit_card.py` (`FitView.possible`, one entry per lender with Backflip's two rows as one, never listed when that lender also fits, not counted in the score), `enrichment/service.py` (card line "Possible, confirm on the call: ..."), `tests/lending/test_enrichment.py` (3 new tests). Stored fit JSON gains an additive `possible` key.
- Evidence: `test_enrichment.py` + `test_lender_fit.py` 67 passed, 20 skipped (no DB); `test_enrichment.py` + `test_soft_approval.py` on the throwaway DB (127.0.0.1:58123, r1test) 116 passed, 0 skipped.
- Open: review finding 2 (rules still need a deploy to change; purchase/rehab shares stay in `config/lending_soft_approval.py`) and the Q1 to Q6 plan questions.

### 2026-10-14, lender rules as data (closes review finding 2 / plan Q5; stacked on the T-05 gap-fix, uncommitted)
- Question asked: new table or extend R-1's `lender_product_boxes`? R-1 (`feat/ndl-r1-overflow-router`) will not merge now, and Josh is unsure of the fields and values. Decision: a new table, one row per lender key and field (value JSON + status), so a changed field is a row edit, never a migration. Not added to the R-1 tables (unmerged, fixed columns, no per-field status).
- Done: `lending.lender_rule_values` (model in `src/lending/models.py`, `migrations/apply_lending_lender_rule_values.py` creates it and seeds one row per lender and field from `config/lender_matrix.py`, never overwriting an edit), loader `src/lending/lender_rules_store.py` (`apply_rows`, `load_matrix`, `current_matrix`), flag `LENDING_RULES_FROM_DB` (default off). `evaluate_lender_fit`, `fit_card.evaluate_fit` and `soft_approval/service.py` now default to `current_matrix()`. Runbook: `docs/lending/lender-rules-runbook.md`.
- Behaviour: a row overrides that field's value and status; a lender is confirmed only when all its fields are. An invalid row is skipped and logged at ERROR (the field keeps its config value); an invalid merged matrix, a missing table or an unreachable DB falls back to the config. A new lender or field still needs a code change.
- Assumed defaults (not Josh's words): edit route for now is SQL (Slack form or Google Sheet sync is later, per his R1.4 note); the flag stays off until the migration is applied to the shared DB (not applied yet).
- Evidence: `test_lender_rules_store.py` 33 passed on the throwaway DB (127.0.0.1:58123, r1test), including a seed, rerun, edit and fallback round trip; with `test_lender_fit.py`, `test_enrichment.py`, `test_soft_approval.py`, `test_contracts.py`: 235 passed, 1 failed. The failure (`test_soft_approval.py::TestRoute::test_invalid_submission_returns_inline_errors_and_stores_nothing`) fails the same on the T-05 branch without these changes.
- 2026-10-14, applied: user said to use the live DB. `apply_lending_lender_rule_values.py` ran on the live `distress_db` (`lending` schema present, table absent beforehand): 84 seed rows inserted (6 lenders x 14 fields, all placeholder), a re-run inserted 0, and `load_matrix` on the live table equals the config. `LENDING_RULES_FROM_DB` is still off: it is a server env setting plus a `lending-api` restart, not done from here.
- Open: turn the flag on (server `.env` + restart `lending-api`); purchase and rehab shares still in `config/lending_soft_approval.py` (move in the T-08 PR; `soft_approval/service.py` changed here, so the T-08 branch needs a rebase); reconcile with R-1's tables when R-1 merges; tell the R-1 owner.

### 2026-10-14, review finding: a closed credit band below a floor is a decline, not "possible" (T-05 branch)
- Finding (Medium): intake stores a closed band such as 600-639 with 600 as its lower bound; the evaluator read it as "600 or higher", so Backflip's 640 floor showed "possible" although the whole band is below it.
- Done: `BorrowerProfile.credit_band_max_fico` (optional; None = open-ended or unknown); the credit check declines (`credit_below_min`, reason "credit band 600-639 below lender floor 640") when that upper bound is below the floor, and stays `possible` when the band crosses the floor or is open-ended. The fit card (`evaluate_fit`) and the soft-approval lead source pass the upper bound, parsed from the stored `credit_band` text with the fit card's existing `parse_band_bounds`; no schema change. Only these two sites build a `BorrowerProfile` from a stored lead.
- Evidence: new cases in `test_lender_fit.py` (600-639 vs 640 and 640-679 vs 680 decline; 620-679 vs 640, 640+ vs 680 and 660-719 vs 680 possible; 680-719 vs 680 fits) and `test_enrichment.py` (card does not list Backflip for 600-639). 249 passed, 1 failed (the known stale `lending.soft_approvals` row on the throwaway DB) across `test_lender_fit`, `test_enrichment`, `test_soft_approval`, `test_contracts`, `test_lender_rules_store`.
- Open: the band text is LendingFlow's stand-in vocabulary until D-3; `lendingflow_events.build_event` does not carry the upper bound (it builds the event, not the profile).

## T-11: LendingFlow webhook, dedupe, consent vault

_No entries yet._

## T-12: Enrichment card + pipeline routing

### 2026-10-13 — T12-2: fit_card partial-rule model wired to T-05 (feat/ndl-t12-fit-card-partial-rules)

**Done:** Wired `fit_card.evaluate_fit` to consume the T-05 partial-rule model (Josh A1, Oct 8).

**Decisions applied:**
- **T12-1 (rebase) already done**: `feat/ndl-t12-enrichment-card` commits are all in `feat/ndl-t05-lender-fit-gap-fix` history (T-12 is an ancestor). No rebase needed; new branch `feat/ndl-t12-fit-card-partial-rules` stacks on T-05.
- **Score computation kept grouped** (`fully_confirmed` replaces `r.verified`; Backflip rows still counted as one lender). The T-05 evaluator's `lender_fit_score` counts raw rows (Q4 pending) and would give the wrong denominator (6 vs 5), so fit_card.py continues to compute the score itself on grouped lenders.
- **`_straddles` uses `field_status_of(rules, "credit_floor") is FieldStatus.CONFIRMED`** instead of `rules.verified`. An unconfirmed floor is never straddled here; the evaluator already puts those lenders in `possible`.
- **Note priority (A1):** `FIT_SUBJECT_TO_CONFIRMATION` ("subject to lender confirmation") takes priority over `FIT_RULES_PENDING` when at least one lender fits using placeholder cap values (`result.fitting` has a row with `subject_to_confirmation=True`). This is the case when a lender's eligibility is known but its cap fields are still placeholder.

**Open confirmations (carry to PR):**
- Q1 (task-analysis): Is the fit % kept hidden while any lender is unconfirmed? (assumed yes per original design; Josh hasn't contradicted this).
- All F1–F7, R6, B8, D6 questions remain open per the task-analysis table.

### 2026-10-13 — F1–F7 team-lead answers applied (feat/ndl-t12-fit-card-partial-rules)

Answers are the team lead's approved defaults, not Josh's; each stays an open confirmation for Josh.

| # | Team-lead answer | Code | Status |
|---|---|---|---|
| F1 | DB + Slack thread in dial-tasks channel; no GHL writes | Already so (`ports.default_deal_thread`) | Matches, no change |
| F2 | Assign to Josh, post in Slack; no GHL task/stage/tag | Card now reads "closer priority, assigned to Josh" (`FULL_MACHINE_ASSIGNEE_NOTE`); test added | Changed |
| F3 | No answer | Hillsborough + Pinellas only; Pasco not matched | **Open** |
| F4 | >30 days and past → NURTURE; exactly 30 is in the window | Already so (`routing.route_lead`) | Matches |
| F5 | We define fields and connect both; Slack form via PR #354; booking form waits on R4 | PR #354 is merged (not open); booking form unconnected | Matches; booking form blocked on R4 |
| F6 | Condition 3 (Average) | `COMPS_AFTER_REPAIR_CONDITION = 3`, comment updated | Matches |
| F7/R6 | Needs Josh; card shows "Not available" | `UnavailableStreetView` fallback | Matches; blocked on Josh |

Still open: F3 (owner + decision), Q1 (fit % hidden while any lender unconfirmed — assumed yes), Josh's sign-off on F2 and F4.
