# ADR 0034 — Lesson hygiene: what counts as evidence, and which terminal states exist

**Status:** Accepted
**Date:** 2026-08-03
**Note:** numbered 0034 because CLONE-v2.2/CL4's open PR already claims 0033 on an unmerged branch.
**Scope:** LEARN-v2.2 Layer 4, Step 12 — `config/learning_hygiene.py`,
`src/services/learning_hygiene.py`, `src/tasks/learning_hygiene_sweep.py`

## Context

`lifecycle_playbook` accumulates lessons. Nothing prunes them. The task was
stated as "the tools already exist — decide the rules, then call them on a
schedule", which is true of the code and not true of the environment. Reading
the branch and the shared DB changed several design decisions, so this ADR
records what the existing code and data determined rather than what was
preferred.

Ground truth at time of writing (verified read-only against the shared DB):

| Fact | Consequence |
|---|---|
| `lifecycle_playbook` has 7 rows, all `source_id = 'test_rollback_<hex>'` resolving to no `ab_tests` row | the entire live corpus is test pollution; there are no real lessons to prune |
| live `check_lifecycle_playbook_status` permits only `recommended, adopted, rejected, retired` | `mark_contradicted` would raise a CHECK violation in production today |
| `confidence`, `version`, `scope`, `superseded_by_id` absent from the shared DB | the LEARN Layer 4 migration has not been applied there |
| `agent_decisions.playbook_id` exists; 0 rows populate it | the evidence feed is live as schema, empty as data |
| `agent_lane_experiment_assignments` does not exist | the per-observation outcome table the task implies is not there |

## Decision

### 1. Three terminal outcomes, not two — because the tools can only express two

`supersede_recommendation` and `mark_contradicted` cover "replaced" and
"proven wrong". A lesson that is merely *old* is neither: nothing replaced it,
and old is not wrong. Expressing that would need a new status and a widened
CHECK constraint, which this task did not ask for.

So staleness **reports and never mutates**. This also honours the
constitutions' "expired = 'unknown because stale'" — stale means unknown, and
retiring on unknown would remove imperfect guidance in favour of none, which
is usually a downgrade.

### 2. Supersession is successor-driven, because the signature says so

`supersede_recommendation(session, old_id, new_id)` takes a mandatory,
FK-enforced `new_id`. A timer cannot call it — a timer has no successor to
name. Age is therefore a filter on candidates, never a trigger. This was
decided by the existing API, not by preference.

Successor identity is `scope` equality (LEARN's portability dimensions), and
**both scopes must be non-NULL**. `source_key` is UNIQUE so two lessons can
never share `(source_type, source_id)`, and no other column declares subject.
A NULL scope means "no declared subject"; matching NULL to NULL would
supersede unrelated lessons against each other. No caller populates `scope`
yet, so this path is inert — reported, not silent.

### 3. Non-evidence outranks contradiction

A decision that failed produced no outcome. It is absence of evidence, not
evidence against. Rows classified non-evidence are excluded from **both**
numerator and denominator: counting them as contradictions lets one
infrastructure outage retire the whole corpus; counting them as support
dilutes real contradiction rates toward zero and makes the rule unreachable.

Each decision is bucketed by an exclusive `CASE`, not by three independent
`FILTER` predicates. The first implementation used `OR` across both dimensions
and a `terminal_status='failed'` row with `autonomy_class='autonomous'`
matched non-evidence *and* support simultaneously — an outage read as a stream
of confirmations. Precedence is: explicit human reversal → non-evidence →
contradiction → support → (default) non-evidence.

The vocabularies are exhaustive over the live `CHECK` constraints on
`agent_decisions`, and a test asserts that coverage against
`pg_get_constraintdef`. A value added to either constraint later fails that
test rather than silently falling through as unclassified.

### 4. `anti_playbook` is excluded, not inverted

An `anti_playbook` row is already the record of 3+ failures. Counter-evidence
against a documented failure means the failure **stopped reproducing** — the
warning is obsolete, not false. That is a different terminal state, not an
inverted threshold, and `mark_contradicted` is the wrong tool for it.

The asymmetry settles it: a false positive here retires a warning and the
fleet resumes doing what it learned not to do. That is the most expensive
mistake available in this corpus — far worse than wrongly retiring a "this
copy performs better" lesson, which costs some performance.

Excluded via `HYGIENE_EXCLUDED_KINDS` so it is discoverable in config rather
than buried in a loop body. Removing the entry is deliberately *not*
sufficient to enable the path: `decide()` has no inverted branch to fall into.
The follow-up is "decide what a disproven anti-pattern becomes", not "add a
flag".

### 5. Unmeasurable domains are named and counted, never touched

Only `agent_domain='lifecycle'` has a writer. The other values in the
vocabulary are namespace-reserved forward-provisioning with no writer and no
feed. Contradiction cannot fire for them anyway; the dangerous half is
staleness, where "no supporting evidence" is permanently true, so an age rule
would flag 100% of them on every run forever — a false positive by
construction, not a mistuned threshold.

They get a stored state (`skip_unmeasurable`), are **excluded from the
blast-radius denominator** so they cannot inflate the mutation budget for
lessons that do have evidence, and their count leads the digest. Building
their feed is a precondition for sweeping them, not optional polish.

A lesson whose `source_id` resolves to no source row gets its own state
(`skip_orphaned_source`) for the same reason — it is unverifiable by
construction. All 7 live rows are in this state.

### 6. Hard cliffs in v1; decay is a v2 proposal

`mark_contradicted` writes `status` and nothing else — it never touches
`confidence`. Implementing graduated decay would mean this module owning the
score, which is well past "call the tools at the right time". `confidence` is
also absent from the shared DB and NULL for every row that will exist when it
arrives, so decay has no value to decay *from*.

Every audit row therefore persists the counts, the window, and the threshold
set in force, which makes decay a retrofit rather than a rewrite. `decide()`
does not read `confidence` at all, which sidesteps the ADR 0006 trap of a
NULL metric being read as a low one.

### 7. `CONTRADICTION_MIN_COUNT = 3` is constitutional, not a preference

The constitutions state "anti-playbooks at 3+ failures", and
`mark_contradicted`'s docstring cites that rule as its authority. A rate
threshold was argued for and rejected on those grounds: changing 3 amends a
constitution. `CONTRADICTION_MIN_RATE_PCT` exists but defaults to `0.0`
(disabled), because 3-of-300 is a lesson worth protecting and a lead may want
that guard — and enabling it can only ever *spare* a lesson, never retire one
the count rule would have kept. "Recent outweighs old" is honoured by
windowing the count.

### 8. Own audit rows are excluded from the evidence feed

`agent_decisions.playbook_id` is both the evidence feed and the audit sink.
Without filtering `graph_name = 'learning_hygiene'` out of the evidence query,
every run would manufacture one supporting-evidence row for the lesson it had
just judged — a self-poisoning loop that grows monotonically and makes a
lesson look better the more often it is examined. Tested explicitly.

### 9. No new migration; audit goes to `agent_decisions`

Neither tool records an actor, a reason, or a terminal timestamp — both set
`status` and bump `updated_at`, which any other write also bumps. So "why did
this lesson die?" is unanswerable from `lifecycle_playbook` alone, and the
first false positive would be unexplainable.

Audit rows go to `agent_decisions` (CLAUDE.md already mandates every agent
decision land there, and `lifecycle_playbook.decision_id` already FKs to it)
rather than into new columns. Adding `contradicted_at`/`superseded_at` would
widen a table that the LEARN foundations branch is concurrently changing, for
no benefit beyond what a JSONB summary already carries.

### 10. This job verifies the migration; it does not apply it

`migrations/apply_lifecycle_playbook_lessons_versioning.py` belongs to the
LEARN foundations branch's rollout. Running it from here would apply another
branch's schema change ahead of its merge. Instead the sweep checks the
required columns and status values up front and reports `schema_not_ready`,
exiting non-zero without touching anything — a reported refusal rather than a
constraint-violation crash that would read as this job's bug.

### 11. Dry-run is the default

`--apply` is opt-in and the cron line omits it. This job mutates learned state
and neither tool has an inverse. Combined with the blast-radius cap
(`max(3, 20%)` of the *measurable* population — a pure percentage yields 0 at
the corpus size that actually exists) and the global feed-health check, a
malfunction fails toward inaction.

## Consequences

- On the current shared DB the sweep does nothing but report, and every path
  is inert for a specific, named, logged reason: contradiction has an empty
  feed, supersession has no populated `scope`, staleness never mutates by
  design, and all 7 rows are orphaned anyway. This is correct behaviour, not
  an incomplete build — but "flagged automatically" only becomes true once
  agents attribute decisions to lessons via `playbook_id`.
- Three follow-ups are created and deliberately visible in the daily digest
  rather than buried: populate `agent_decisions.playbook_id` at decision time;
  decide the terminal state for a disproven anti-playbook; build an evidence
  feed for non-`lifecycle` domains.
- The 7 `test_rollback_*` rows need deleting by a human. They are the same
  class of dev-DB pollution as the `testco_*` counties, and letting this sweep
  age them out would validate the job against garbage — a passing digest that
  proves nothing.

## Alternatives rejected

**Count "3 disagreements" from a per-lesson outcome stream.** No such stream
exists; nothing was keyed to a playbook row until `agent_decisions.playbook_id`
was found. The task's stated rule assumed a table that is not there.

**Retire stale lessons with `mark_contradicted`.** Conflates old with wrong,
and the constitutions explicitly separate them.

**A flat 30-day staleness clock reusing Vera's `is_stale`.** That function
governs *facts* (revenue 24h / deed 90d / market 30d) and hardcodes
`datetime.now()`, so it is both semantically wrong for a lesson — which
outlives any single observation — and untestable at a boundary.
