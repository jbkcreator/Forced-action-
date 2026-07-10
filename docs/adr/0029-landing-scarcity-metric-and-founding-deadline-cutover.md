# Landing scarcity metric and founding-deadline cutover scope

---
Status: accepted
---

Task 8 (landing conversion features) surfaced two decisions with real money/UX
trade-offs a future reader would otherwise question.

## Decision 1: ZIP scarcity counter is exclusivity, not lead volume

The landing page's scarcity signal is a **county roll-up of open vs. taken
ZIPs** (`zip_territories` status), not the `lead_count` value already
returned by `/api/zip-availability`.

**Rejected alternative:** show exact lead count per ZIP as the urgency
number (the original plan draft). Rejected because `zip_territories` is a
binary lock — one ZIP, one subscriber, per vertical — there is no "spots
left" pool to count. Lead count answers "is this ZIP worth buying," not "how
much of this county is still available." Using it as the scarcity number
would have been factually wrong (it can go up over time as new leads land,
which is the opposite of scarcity) and misleading once corrected. Lead count
stays as a secondary value signal on `available` ZIPs only.

## Decision 2: Founding-deadline cutover applies to new checkouts only

`County.founding_price_deadline_at` gates whether **new** checkouts are
quoted founding vs. regular price. It never re-prices a subscriber who
already has `rate_locked_at` set — those subscribers stay on their own
independent 6-month escalation timer regardless of when the county-level
deadline passes.

**Rejected alternative:** treat the deadline as a hard cutover that also
escalates every founding subscriber in the county at once. Rejected because
`rate_locked_at`/`escalated_at` is a per-subscriber promise made at checkout
(build for Task 6) — retroactively moving it based on a county-wide deadline
would break that promise and could trigger surprise price increases for
existing customers. The two mechanisms are deliberately independent: the
deadline controls what *new* visitors see; the per-subscriber timer controls
what *existing* founders eventually pay.
