# Event-driven enrichment cascade triggered at scoring

The enrichment skip-trace cascade runs as a standing, automated stage triggered
when a property *enters* Gold+ at scoring time, rather than as the legacy daily
batch (`run_enrichment.py` at 07:30). The scoring persist path publishes a
`gold_lead_scored` event via `publish_cora_event` (durable: Redis + the
`cora_event_queue` Postgres fallback); a dedicated enrichment consumer — **not**
a Cora subscriber graph and **not** in `EVENT_TO_GRAPH` — drains events, batches
per provider, and runs the shared cascade function.

## Considered Options

- **Synchronous inline in the scoring loop** — rejected. Tracerfy's batch
  `/trace/` API is asynchronous (submit queue → poll 5s → 31s delay between
  batches for the 10-POST/5-min rate limit); a single hit is 30s–5min
  wall-clock. Running it inline would stall scoring of ~522k parcels for hours
  and blow the rate limit.
- **Micro-batch via a queue table + short-interval drain** — viable but is the
  batch model in event clothing; diverges from the "dynamic per-lead at the
  moment of scoring" requirement.
- **Event-driven (chosen)** — honors per-lead/standing semantics, keeps scoring
  fast, and lets the consumer batch Tracerfy submissions.

## Consequences

- Scoring is a once-daily bulk burst, so the consumer mostly receives one wave
  of Gold+ events at ~07:00 and batches it.
- The nightly batch (07:30) is **retained as a reconciliation sweep** sharing
  the same cascade function — it mops up any lead whose event was dropped
  (Redis down, consumer crash, stage error). Both paths invoke one cascade
  implementation; the ordering is not forked.
- Emission fires on *entry* into Gold/Platinum/Ultra Platinum (new or
  upgraded-from-below-Gold), not for already-Gold+ leads, so daily re-scoring
  does not re-trigger the cascade; the `already_traced` / `no_phone` candidate
  filters are the idempotency backstop.
