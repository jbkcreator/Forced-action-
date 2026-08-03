# Cora ↔ Relay follow-up scheduling — Q&A

Answers from Cora's dev to Relay/Throughput's dev, ahead of building either side. Stored so neither side has to re-derive this later.

## 1. Architecture confirmation — who computes follow-up eligibility?

**Cora does, entirely on her own side.** A periodic sweep inside Cora (`src/agents/cora/followup_scheduler.py`) reads Cora's own `opportunity_state` store to find threads still sitting at `touched` (never advanced to `replied`), reads `relay_approval_queue.dispatched_at` **read-only** to know when the parent touch actually went out, and — once a thread is eligible — drafts the follow-up through the normal `outreach.py` path and calls Relay's existing `queue.enqueue()`. Same call, same shape as any other draft; Relay does not need a new scheduling mechanism, a new endpoint, or any awareness that a given row is a "follow-up" versus a first touch.

**Zero changes needed on Relay's side.**

## 2. How does Cora know "no reply yet" for a cold prospect with no `subscriber_id`?

Cora tracks this herself — it does not depend on `message_outcomes.replied_at` or any subscriber-keyed table at all. Cora's `opportunity_state` (keyed on `opportunity_thread_id`, Hunter's existing stable ID) is the source of truth: C3 (the reply workflow) is what advances a thread's status to `replied` the moment an inbound reply is matched and classified. "No reply yet" = the thread's status in Cora's own store has never left `touched`.

**Named honestly: this is bounded by reply-ingestion reliability, which is still open.** Reply ingestion (C3) currently runs against a documented stub payload, not the real monitored-mailbox format — that's still gated on Josh confirming the forwarding address/format (the same open item as Q4 in the original client Q&A). Until that lands, "no reply yet" is only as accurate as whatever is fed into the stub producer. This isn't a gap in the follow-up design — it's the same, single upstream dependency the whole reply pipeline already has, not a new one introduced by scheduling.

## 3. How does a follow-up link back to its parent send?

Cora's own linking key is `opportunity_thread_id` — not a specific queue row. As long as `relay_approval_queue.thread_id` is populated with `opportunity_thread_id`'s value, free-text matching is sufficient for everything Cora needs to do (find the opportunity, check its state, decide eligibility).

**No `parent_queue_item_id` FK requested from Relay.** If Relay/Throughput want a stricter guarantee against double-scheduling or need per-send attribution for their own reasons, that's a decision for that table's owner — not something Cora's design requires.

## 4. Cadence rules

Not previously decided — resolved now as a launch default, explicitly a placeholder:

- **Fixed offsets**: day-2 and day-5 after the parent touch.
- **One universal rule** for launch — not per-vertical or per-cell yet.
- **2 follow-ups total** (a 2nd and 3rd touch on the opportunity; no more).
- **Not A/B tested yet.** The existing Lifecycle runtime's `followup_cadence_v1` arm is the right pattern to adopt later, once REVINT/the Learning Engine (Week 3-era work) is in place to actually run and score the test. Building that now would be optimizing a number nobody has evidence for yet.

## 5. Does a reply cancel all future scheduled touches?

**Yes — same behavior as the existing precedent.** The moment a thread's state advances to `replied`, the follow-up scheduler's eligibility query naturally excludes it (eligibility is defined as "still at `touched`"), so no future touch gets drafted or enqueued for that thread. No separate cancellation step needed — it falls out of the eligibility check itself.

## 6. Does every follow-up need fresh Josh approval?

**Yes, always — there is no Stage 2+ for this agent, period.** This is stronger than "confirming we're not assuming Stage 2+ yet": the new Cora is *permanently* drafts-only by design (per the task's own framing), unlike the old Lifecycle Cora's spec'd authority ladder, which explicitly grows toward standing-order autonomy on in-thread follow-ups #2/#3 over time. That progression does not exist for this agent at all. Every touch — first send or fifth — is a fresh draft requiring fresh approval through Relay's normal Stage-1 path, forever.

## 7. Suppression check before drafting

**Both — efficiency check now, Relay's recheck stays authoritative.** Cora's existing validation gate (`is_email_suppressed()` / `validate_outbound()`) already runs before any draft is created, including follow-ups — so Cora doesn't spend an LLM call and a store write on a thread that's already opted out. This is purely an efficiency measure. Relay's execution-time recheck (R3) remains the actual gate of record, since state can change between draft time and send time — Cora's check never substitutes for it.

---

**Net new scope this Q&A adds to the build** (not in the original plan before this exchange): `src/agents/cora/followup_scheduler.py` — the periodic eligibility sweep + day-2/day-5 cadence logic described in Q1/Q4/Q5. Everything else here confirms or clarifies existing plan decisions rather than adding new ones.
