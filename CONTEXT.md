# Forced Action — Cora Attribution Rollout

Glossary for the staged, self-rolling-back release of attribution-driven Cora
decisions. Terms here disambiguate the A/B, attribution, and guardrail
vocabularies that overlap in this area.

## Language

**Attribution-driven path** (a.k.a. _variant_):
The Cora decision path that uses the merged attribution context and revenue
signal to shape routing, urgency framing, upsell/offer timing, and message
selection.
_Avoid_: "new path", "treatment" (use **variant**).

**Control path**:
The existing Cora decision path that ignores attribution context.
_Avoid_: "baseline path", "old path".

**Eligible traffic**:
A Cora-touched subscriber for whom `get_attribution_context()` returns
non-empty. Only eligible subscribers are hashed into the 10/90 split;
subscribers with no attribution data stay outside the experiment.
_Avoid_: "all users", "active traffic".

**Rollout arm**:
The group an eligible subscriber is deterministically assigned to —
`variant` (attribution-driven, ~10%) or `control` (~90%). Distinct from an
A/B **variant** (`a`/`b`), which is a message-copy split inside a test's
traffic slice.
_Avoid_: using "variant" alone when you mean an arm.

**Rollout test**:
The single global `AbTest` named `cora_attribution_v1` governing the rollout.
One subscriber-level assignment is read by every Cora decision, so a subscriber
stays in one arm for the whole test.
_Avoid_: "experiment", "flag" (the flag is **AbTest.status**).

**Rollback**:
Setting `AbTest.status='rolled_back'` (a terminal status distinct from
`completed`), which returns every assigned subscriber to the **control path**
on their next decision. The single kill switch for this feature.
_Avoid_: conflating with the step-6 **kill-switch** (which blocks an action
entirely rather than falling back to control).

**Losing variant**:
The **variant** arm whose conversion rate is below the **control** arm by more
than 2 standard deviations (one-sided proportion z-test) within the rolling
48-hour window, once each arm has ≥30 assignments. Triggers **rollback**.

## Relationships

- A **rollout test** assigns each **eligible** subscriber to exactly one **rollout arm**.
- The **variant** arm follows the **attribution-driven path**; the **control** arm follows the **control path**.
- A **losing variant** triggers a **rollback**, which flips **AbTest.status** and returns all arms to the **control path**.
- An **AbAssignment** is marked `converted` when the merged **attribution service** records a `conversion_attribution_event` for that subscriber.

## Flagged ambiguities

- "variant" meant both the A/B copy split (`a`/`b`) and the rollout arm
  (`variant`/`control`) — resolved: **rollout arm** is the rollout concept;
  **variant** (`a`/`b`) stays the message-swap concept. A dedicated
  `assign_rollout_arm` keeps the two assignment functions separate.
- "kill switch" meant both the decision-hierarchy step-6 gate (blocks an
  action) and the rollout off-switch — resolved: the rollout off-switch is
  **AbTest.status='rolled_back'**, not the step-6 gate.
