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

_No entries yet._

## T-11: LendingFlow webhook, dedupe, consent vault

_No entries yet._

## T-12: Enrichment card + pipeline routing

_No entries yet._
