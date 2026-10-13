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

## T-05: Lender rules matrix + `evaluate_lender_fit`

_No entries yet._

## T-11: LendingFlow webhook, dedupe, consent vault

_No entries yet._

## T-12: Enrichment card + pipeline routing

_No entries yet._
