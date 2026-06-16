# Lead Pack deferred fulfillment with a post-payment 100% quality floor

A Lead Pack purchase splits into two phases. The `payment_intent.succeeded`
webhook performs a **Lead Reservation** — under the ZIP advisory lock it selects
the top 5 un-locked qualified leads, writes `purchase.lead_ids` and their
`lead_exclusivity` rows, sets `status='enriching'`, returns 200 immediately, and
publishes a `lead_pack_reserved` event.

Fulfillment is then driven by a **hybrid worker**:

- **Primary (event-driven, low-latency):** the always-on agents process consumes
  `lead_pack_reserved` (Redis Pub/Sub, Postgres-NOTIFY fallback — the existing
  Cora bus), and the supervisor hands it to the fulfillment worker on a daemon
  thread for near-instant pickup.
- **Backstop (durable):** a cron sweep (`lead_pack_fulfillment_sweep`, every
  2 min) re-scans `status='enriching'` rows in case the event was dropped (Redis
  down, agents process restarting).

Both paths funnel through one **atomic claim** (`UPDATE … WHERE status='enriching'
AND (enrichment_submitted_at IS NULL OR stale) RETURNING id`), so whichever fires
first wins and the other skips — no double-fulfillment.

The claimed worker runs **Lead Pack Hot-Enrichment**: a fresh Tracerfy `/trace/`
batch over exactly those 5 properties, gated by a **100% Quality Floor** (every
lead must return a phone OR email). Pass → `delivered`, `SentLead` rows
(`source='lead_pack'`), delivery email. Fail → `refunded`, the reservation's
exclusivity rows are deleted (leads released), Stripe refund, refund email.

## Considered Options

- **Synchronous hot-enrichment inside the webhook** — rejected. Tracerfy's batch
  `/trace/` is async (submit → poll, 30s–5min wall-clock). Blocking the webhook
  that long breaches Stripe's fast-ack contract and triggers webhook retries.
  The instant `/trace/lookup/` endpoint ($0.10/hit) would be fast enough but is
  unimplemented and still carries third-party-latency risk inside the 200 path.
- **Reserve leads only at delivery (in the sweep)** — rejected. With delivery
  deferred minutes, two buyers paying seconds apart both select the same top 5,
  causing a double-sell. The at-payment reservation is what makes the
  simultaneous-purchase guard hold across the gap.
- **Keep the pre-payment 80% confidence-label gate alongside the 100% floor** —
  rejected. Three overlapping thresholds let a pack pass checkout then fail the
  live floor, producing charge-then-refund churn. The 80% gate is dropped; the
  post-payment 100% Tracerfy floor is the single authoritative quality bar.
- **Cron-sweep only** — simplest and fully durable, but adds up to ~2 min pickup
  latency. Acceptable on its own because Tracerfy (30s–5min) dominates and
  delivery is by email, but not the lowest-latency option.
- **FastAPI `BackgroundTasks`** — rejected; dies on restart/deploy, no retry, no
  durability for money-critical work.
- **Reserve at payment, hybrid event + cron-backstop worker (chosen)** — closes
  the double-sell race, keeps the webhook fast, gives near-instant fulfillment
  via the existing Cora event bus (mirroring the ADR 0016 enrichment consumer:
  a non-graph pipeline event), and keeps the durable cron sweep as the backstop
  so a dropped event never strands a paid pack.

## Consequences

- The 72h exclusivity clock starts at **reservation (payment)** time, not
  delivery; the few-minutes enrichment delay is negligible against 72h. A
  refunded buyer holds nothing because the rows are deleted.
- Exclusivity is written **optimistically** at payment and **released** on
  enrichment failure — reads already filter on `exclusive_until > now`, and the
  delete on refund prevents a failed pack from blocking the pool.
- Pre-payment keeps only the cheap filters (≥5 leads each with a contact on
  file, via `has_contact_filter`) so we rarely charge for an empty ZIP; the real
  guarantee lives post-payment.
- A delivered pack now writes `SentLead` rows, so the buyer's own leads are
  excluded from their blurred stack / `$4` unlock CTAs and are counted by the
  lead-quality monitor.
- The event path requires the agents process (`python -m scripts.run_agents
  --serve`) to be running. If it is down, packs are not stranded — the cron sweep
  fulfills them within ~2 min. The webhook publishes the event *before* its own
  commit, so the listener waits briefly for row visibility before claiming.
- The Hot-Enrichment runs on a daemon thread (not the listener thread) because a
  Tracerfy poll is multi-minute and must not block other event processing.
- This Hot-Enrichment is distinct from the standing Enrichment Cascade
  (ADR 0016): purchase-triggered and scoped to the 5 reserved leads, not
  event-driven at scoring. Both ultimately call Tracerfy via the shared client.
- Insufficient inventory at checkout returns **422** (business-rule violation),
  not the spec's suggested 402 — 402 is reserved for payment/funds failures per
  the API error-code standard.
