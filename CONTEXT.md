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
  **AbTest.status='rolled_back'`, not the step-6 gate.

---

# Forced Action — Cora Touch Timeline

Glossary for the per-subscriber UI timeline that aggregates every Cora
interaction. Built as a read layer over existing tables; no new write paths.

## Language

**Cora touch**:
One `agent_decisions` row — the canonical "why did Cora do X for subscriber Y"
event. Outbound SMS/voice/chat replies composed by that decision render as
child artifacts of the same touch, not as separate touches. Concierge Chat
turns that do _not_ run through a graph (pre-signup chat with no
`agent_decisions` row) are tracked separately and are not Cora touches.
_Avoid_: "Cora interaction", "Cora event", "agent action" (overloaded).

## Relationships

- A **Cora touch** belongs to exactly one subscriber (`agent_decisions.subscriber_id`).
- A **Cora touch** belongs to exactly one **graph** (`fomo` / `abandonment` /
  `retention`) — see Phase 2B graphs in CLAUDE.md.

---

# Forced Action — Concierge Chat

Glossary for the AI chat widget that runs on the landing page (pre-signup) and
dashboard (post-signup). Terms here disambiguate chat modes, guardrail behavior,
and buying-intent flows.

## Language

**Chat mode**:
The context in which the Concierge Chat is mounted — `pre_signup` (landing page,
anonymous visitor) or `post_signup` (dashboard, authenticated subscriber).
_Avoid_: "anonymous mode", "logged-in mode".

**Chat guardrail**:
One of four enforced response rules baked into the system prompt that constrain
what the Concierge may say: refund redirect, ZIP availability, competitive
comparison, and abusive-user handling.
_Avoid_: "safety rule", "restriction".

**Buying-intent trigger**:
A keyword pattern detected by the frontend in a chat message that causes the
frontend to initiate a checkout or support redirect action without waiting for
the LLM reply.
_Avoid_: "intent signal", "action keyword".

**Soft end**:
The LLM-level response pattern for abusive sessions: one warning turn, then a
canned refusal on every subsequent turn. No session flag is set; the session
remains open in the database.
_Avoid_: "session termination", "ban".

**Wallet trigger**:
A **buying-intent trigger** active in both chat modes; fires a Stripe checkout
for a wallet plan (Starter $49 / Growth $99 / Power $199).
_Avoid_: "credits trigger", "top-up trigger".

**Lock trigger**:
A **buying-intent trigger** active in `post_signup` mode only; fires a Stripe
checkout for the Territory Lock add-on ($197/mo per ZIP). Suppressed in
`pre_signup` mode because eligibility requires an existing subscription.
_Avoid_: "ZIP trigger", "territory trigger".

## Relationships

- A **buying-intent trigger** fires only on the frontend; the chat backend
  (`concierge_chat.py`) has no awareness of it.
- A **wallet trigger** is active in both **chat modes**; a **lock trigger** is
  active in `post_signup` only.
- A **chat guardrail** is enforced by the LLM via the system prompt; a
  **buying-intent trigger** is enforced by the frontend independently of the LLM.
- A **soft end** does not set any DB flag — it is purely a response-text
  constraint applied by the LLM after the warning turn.

## Flagged ambiguities

- "end the conversation" was used to mean both a hard session block and a
  soft LLM refusal — resolved: v1 uses **soft end** only; no `blocked_at`
  column exists on `chat_sessions`.
- "annual trigger" was considered as a **buying-intent trigger** — resolved:
  annual billing requires tier + ZIP count qualification that the chat cannot
  determine; annual intent routes to a support mailto redirect, not a Stripe
  session. Not a trigger in v1.
