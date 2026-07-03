# Retire Alembic in favour of idempotent per-change DDL scripts

---
Status: accepted
---

The Alembic migration tree had become unusable: **211 revision files, ~24 heads,
and duplicate revision IDs** (`fa062`, `fa063`, `fa103` each defined twice). The
graph is corrupt — `alembic upgrade head` cannot run, and the live
`alembic_version` table already holds three stamped rows from the multi-head
state. In practice Alembic was already dead: nothing runs `alembic upgrade` at
deploy (Docker/compose only launch uvicorn + the Cora agents), tests build their
schema from `Base.metadata.create_all()` off `src/core/models.py`, and every
recent schema change was applied through hand-written idempotent
`scripts/apply_*.py` DDL against the single shared Postgres. This ADR ratifies
that reality as the one and only migration path.

## Decisions

- **Scripts-only, one path.** A schema change is: (1) update `src/core/models.py`
  (the tests' `create_all` source of truth), (2) write an idempotent
  `scripts/apply_<name>.py` (`CREATE TABLE IF NOT EXISTS`,
  `ADD COLUMN IF NOT EXISTS`, `ON CONFLICT DO NOTHING`), (3) run it once against
  the shared DB. No new Alembic revisions, ever.

- **No `apply_all.py`, no ledger table.** We never rebuild the DB from scratch —
  the existing shared Postgres is carried forward. So there is no aggregate
  bootstrap runner, and no tracking table. **Idempotency + git history is the
  record**: re-running any `apply_*.py` is safe, so "was it applied?" is answered
  by just running it again.

- **`models.py` stays mandatory.** It is the only always-accurate, human-readable
  schema description, kept honest by the test suite (`create_all` every run). It
  is *not* optional shadow work — it is the schema of record for tests.

- **Legacy migrations archived, not deleted.** The whole `alembic/` tree and
  `alembic.ini` moved to `legacy/alembic/` to preserve provenance without
  inviting anyone to run the corrupt graph. Each historical migration's
  `upgrade()` was rendered to verbatim Postgres SQL (via Alembic offline
  `as_sql` mode) into `scripts/apply_<slug>.py` so every schema change also lives
  in `scripts/`. 160 converted; 23 already had a script; 27 were data
  migrations / merge revisions that cannot render to static SQL — listed in
  `scripts/CONVERSION_SKIPPED.md` for manual reference. Converted SQL is verbatim
  and **not** idempotency-massaged (it is a historical record, never re-run).

- **`alembic_version` table left in place.** Dropping three dead rows from the
  live prod DB is a pointless DDL with nonzero risk; it is orphaned and harmless.

## Consequences

- Fresh-DB reproduction is *not* a supported one-command operation. If the shared
  DB is ever lost, rebuild = `models.py` `create_all` + re-run the raw-DDL bits
  it does not capture (e.g. `generate_uuidv7()`, seed rows). Acceptable given the
  DB is treated as durable and never rebuilt in normal operation.
- The two `models.py` + `apply script` steps must stay in sync by discipline —
  there is no tool enforcing that the script matches the model. The test suite
  catches model/schema mismatches only for what `create_all` produces.
