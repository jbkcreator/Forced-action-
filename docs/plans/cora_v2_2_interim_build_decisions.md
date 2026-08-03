# Cora — Interim Build Decisions (branch feat/cora-v2.2-cold-drafting)

Written before implementation, per instruction. Covers why this branch was built the way it was, and what's changed since — corrected to match current, verified reality, not the original assumptions.

## 1. Branch provenance

- Branched from `dev` at commit `a4a87f0` (tip of `dev` at the time, already includes Vera and Hunter merged).
- Later rebased onto the current `dev` (after the rename below merged) — clean, zero conflicts, confirmed via `git diff` against every file in §3 below returning empty even against the new, larger `dev`.

## 2. The Cora→Lifecycle rename — status corrected

**Original assumption (no longer current):** the rename branch (`chore/rename-cora-to-lifecycle`) was pushed and awaiting review, not yet merged.

**Current, verified status:** the rename's code is merged into `dev`, and its DB migration has been run against the shared database. Verified directly: zero remaining `cora_*` tables, columns, indexes, or constraints anywhere in the `public` schema; `lifecycle_playbook`/`subscribers.signup_source` backfills applied; `check_subscriber_signup_source` rebuilt and validated. Two old, empty tables (`cora_event_queue`, `cora_incident`) that collided by name with already-existing `lifecycle_*` tables were dropped first (confirmed empty, zero data loss) to let the migration complete.

This means the *original reason* this branch avoided `src/core/models.py`/`scripts/cron/crontab.txt`/the guardrails file (risk of conflicting with an in-flight, unmerged rename) no longer applies — see §3.

## 3. Files this branch avoided, and current status

| File | Original reason avoided | Current status |
|---|---|---|
| `src/core/models.py` | Rename branch touched 148 scattered lines — risk of merge conflict while unmerged | Rename merged, this branch rebased cleanly on top. **Safe to edit now** — no longer a live constraint. |
| `scripts/cron/crontab.txt` | Rename branch touched 56 lines | Same — **safe to edit now**. |
| `config/cora_guardrails.py` | Renamed to `config/lifecycle_guardrails.py` on the rename branch | Already renamed on `dev`; this branch never referenced the old path. Nothing to reconcile. |

**Correction to the original version of this doc:** it named the kill-switch service path as `config/kill_switch_service.py` — that path never existed. The real, correct path, confirmed against the actual codebase, is `src/services/kill_switch_service.py`. Cora's `kill_switch.py` has always imported from the correct path; only this doc's earlier prose had it wrong.

## 4. Interim decisions — what's built differently than the Week 2 plan describes, and why

| Plan says | This build does instead | Status |
|---|---|---|
| New cell-grid model/config (offer × avenue × angle) | Plain Python config module (`config/cora_cell_grid.py`), same pattern as `config/revenue_ladder.py` | **Permanent** — a DB table was never actually required for this. |
| New `OutboundDraft` table | File-based interim store (append-only JSON-Lines, `src/agents/cora/store.py`) | **Still a stand-in — not yet migrated.** The condition that justified deferring it (unsafe to edit `models.py`) is now cleared (§2/§3), but the actual migration to a real Postgres table has not been done. This is the one remaining piece of interim-vs-permanent debt from the original plan. |
| New kill-switch key | Registered against `src/services/kill_switch_service.py` via `get_kill_switch_status("cora_global")`, following Hunter's exact pattern | **Permanent.** Note: the key is `cora_global`, not `cora_cold_global` as an earlier version of this doc said — that was always a documentation error, not a code discrepancy. |

**Note on the facts-directory convention:** the original Week 1/2 planning assumed a filesystem `/shared/facts/` directory. That's not what got built anywhere in this codebase — Vera's real implementation writes to a real DB table (`vera_facts`), explicitly documented as Vera-owned, not a shared store. Cora's interim draft store is a plain local file, unrelated to Vera's mechanism.

## 5. Follow-ups — current status

- [x] Re-run the file-conflict check against `src/core/models.py` and `scripts/cron/crontab.txt` once the rename branch merged — done, confirmed clean via the rebase (§2/§3).
- [ ] Move the cold-draft store from the interim JSON-Lines file into a real `OutboundDraft` table in `src/core/models.py`, with a proper `migrations/apply_*.py` script. **No longer blocked — just not yet done.**
- [ ] Wire the nightly draft-generation job into `scripts/cron/crontab.txt` (currently runs manually/on-demand only). **No longer blocked — just not yet done.**
- [ ] Confirm whether `cora_global` should get a manual-override note added wherever the fleet's kill-switch runbook lives (mirroring `hunter_global`'s documentation).

## 6. What was never deferred — built for real from the start

- The cell-grid taxonomy (offer × avenue × angle config).
- The kill-switch registration against `src/services/kill_switch_service.py`.
- The drafting logic itself (pure functions, no send capability) — doesn't depend on where drafts are stored.
