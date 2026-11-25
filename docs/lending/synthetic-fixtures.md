# Synthetic loan fixtures and the health commit SHA (R1 0.10)

## 1. The matrix

`tests/fixtures/synthetic_loans.py` builds 21 fake loan files: 3 loan types x 7 edge states. They are the same on every
call (values come from the type and state index), so a failure is always reproducible. `tests/lending/test_synthetic_loan_matrix.py`
runs each one through the check that already exists.

| Edge state | What the fixture holds | Checked by |
|---|---|---|
| `clean` | normal figures, one current document of each kind | instant quote engine (a refinance quote is refused with `REFINANCE_RULE_PENDING` while `REFINANCE_QUOTES_ENABLED` is off) |
| `incomplete` | loan amount, price, rehab, ARV and payoff all `None`, no email, no entity, no documents | strict contract round trip; stored as NULL, never 0 |
| `expired_documents` | bank statement 75 days old, pricing sheet expired yesterday | data shape only; the validator is R1 1.7 |
| `conflicting_fico` | two guarantors, FICO 760 and 640 | a quote equals the 640-only quote and differs from the 760-only one |
| `cross_borrower_document` | a bank statement in another person's name | data shape only; the validator is R1 1.7 |
| `dnc_flagged` | a scrub flag per loan type: Fix & Flip national DNC, Refinance TCPA litigator, Ground-Up state DNC | `compliance.filter_loadable` blocks with `NATIONAL_DNC`, `LITIGATOR`, `STATE_DNC` |
| `backflip_tagged` | borrower email at `backflip.com` and a draft that names Backflip | `lane_violation()`; no other fixture trips it (the word "flip" is safe) |

Loan types: Fix & Flip = product `flip` / quote purpose `purchase`; Refinance = product `other` / `refinance` with a payoff
balance (the contract has no refinance product); Ground-Up = `ground_up` / `new_construction`.

Database tests (also in that file): all 21 fixtures ingest through `state_machine.ingest_loan_file` into one throwaway
schema (21 files, unknown amounts stored NULL), and the DNC fixtures go through the real load filter. Both roll back.

## 2. Using a fixture in another test

```python
from tests.fixtures.synthetic_loans import loans

for loan in loans(product="ground_up", edge_case="expired_documents"):
    ...  # loan.loan_file, loan.quote_request, loan.documents, loan.dnc_flag, loan.outbound_draft
```

R1 1.7's packet validator tests start from `loans(edge_case="expired_documents")` and `loans(edge_case="cross_borrower_document")`.

Add a case or a loan type by editing `EDGE_CASES` / `PRODUCTS`; `test_matrix_is_complete_and_deterministic` fails until every
type x state pair has a fixture.

## 3. CI

`.github/workflows/deploy.yml` runs the matrix as its own step, "Synthetic loan matrix (R1 0.10)", on every pull request into
`dev`, and it is left out of the main "Run tests" step so it does not run twice. It runs even when "Run tests" fails
(`if: ${{ !cancelled() }}`), so one red step never hides the other's result.

A database test that cannot reach Postgres **fails** in CI (`CI=true`) instead of skipping, so the step cannot pass green on
skipped tests. On a laptop without `TEST_DATABASE_URL` they skip.

Run locally against a throwaway database (its name must contain `test`):

```bash
TEST_DATABASE_URL=postgresql://<user>@localhost:5432/fa_matrix_test pytest tests/lending/test_synthetic_loan_matrix.py -v -rs
```

## 4. Health: the deployed commit

| Route | Who calls it | Body |
|---|---|---|
| `GET /health` on `127.0.0.1:8010` | `deploy.sh` gate, the 0.3 watchdog | `{"status": "ok", "commit_sha": "<40 hex>", "migration_head": "apply_..."}` |
| `GET /api/lending/health` (public, nginx `/api/lending/`) | people, the Done When check | the same body |

The spec calls this route `GET /api/health`. It is served as `/api/lending/health` on lending-api (team decision): nginx already
routes that prefix, and fa-api is unchanged. It is public and needs no login; it shows only the commit and a migration file name.

- `commit_sha` is `git rev-parse HEAD` of the checkout, read **once when the process starts**. After a `git pull` without a
  restart it still shows the commit that is running, not the one on disk.
- `migration_head` is the newest `migrations/apply_*.py` file the checkout ships (newest commit that added one; the last name
  in sort order if one commit added several). Alembic is retired (ADR 0024) and no table records applied migrations, so this is
  **not proof the migration ran**.
- `"unknown"` means git could not be read (not a repository, git missing, a timeout, or a git "dubious ownership" refusal).
  `migration_head` is also `"unknown"` in a shallow clone (`git rev-parse --is-shallow-repository` is `true`, e.g. GitHub's
  default checkout), where git cannot tell which commit added a migration. git is looked up outside `PATH` too
  (`/usr/bin/git`), because the systemd units set `PATH` to the venv's `bin` only.
  Each field fails alone and the route still answers 200, so the deploy gate and the watchdog keep working.
- No database access: the route answers while the database is slow.

## 5. Done When (for the screen recording)

1. On the pull request, Checks tab: open "Synthetic loan matrix (R1 0.10)"; the 21-key matrix and the database tests show
   PASSED and none SKIPPED.
2. After merge and deploy, on the server: `git -C /root/Forced-action- rev-parse HEAD`, then
   `curl -s 127.0.0.1:8010/health` and `curl -s https://<host that serves /api/lending/>/api/lending/health`. Both
   `commit_sha` values equal the `rev-parse` output.

## 6. Decisions

- D1/D2: Refinance modelled as product `other` + quote purpose `refinance`; no schema change.
- D3: refinance quotes stay switched off; the matrix pins the refusal, and the FICO test switches them on with a monkeypatch.
- D5: expired and cross-borrower documents are data only until R1 1.7 builds the packet validator.
- D8: migration head is the newest migration file shipped, not the newest applied.
- D10: `/api/lending/health` on lending-api instead of `/api/health` on fa-api.
