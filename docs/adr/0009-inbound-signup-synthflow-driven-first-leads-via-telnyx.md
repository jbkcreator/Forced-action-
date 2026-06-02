---
status: accepted
---

# Inbound missed-call signup is Synthflow-driven; First Leads sent via Telnyx `send_sms` (marketing), not GHL

## Context

The codebase already had two inbound/lead-delivery surfaces that a reasonable
reader would expect us to reuse:

1. `POST /webhooks/telnyx/voice` (`main.py`) — a Telnyx Programmable Voice
   webhook that fired `signup_engine.handle_missed_call()` on `call.initiated`.
2. `send_sample_leads()` (`sample_leads_sms.py`) — sends top-3 teaser leads via
   the **GHL conversations API**, keyed by a GHL `contact_id`.

The DoD requires: Synthflow receives the inbound missed-call/signup event, posts
it to Forced Action, and the backend creates/resolves the account and **delivers
the first leads within 60 seconds** of the Synthflow event.

## Decision

- **Inbound is Synthflow-driven.** A new dedicated endpoint
  `POST /webhooks/synthflow/inbound` is the authoritative trigger (auth via
  `X-Synthflow-Secret` / `Bearer`; persisted `call_id`/`event_id` idempotency
  key). The Synthflow agent captures **ZIP + vertical** on the call and includes
  them in the payload; the backend writes them onto the subscriber and uses them
  to select leads. Missing ZIP/vertical → the row is marked **Incomplete
  Capture** and First Leads fall back to county-wide top-scored leads.
- **`handle_missed_call` is refactored into a provider-agnostic core service**
  (resolve/create account → write `SmsOptIn` → select leads → send First Leads →
  send welcome+link). The legacy Telnyx voice webhook delegates to the same core.
- **First Leads are sent via `sms_compliance.send_sms(message_type="marketing")`
  (Telnyx), not GHL.** The inbound call itself is the consent event: an
  `SmsOptIn` row (with consent source + `call_id` audit) is written at signup, so
  the marketing send passes the compliance gate.

## Considered Options

- **Send First Leads via GHL** (reuse `send_sample_leads`) — rejected: requires a
  GHL contact-creation round-trip inside the 60-second budget, bypasses the
  mandatory `message_type`/`SmsOptIn` compliance system, and produces no
  `message_outcomes` row for the SLA measurement. We reuse only its lead
  *selection/formatting* (`get_sample_leads`, `format_sms_body`).
- **Keep Telnyx voice as the primary inbound path** — rejected: the DoD names
  Synthflow as the inbound channel. Telnyx voice is retained only as a delegating
  fallback.

## Consequences

- "First Leads delivered" is measurable: the SLA clock runs from
  `webhook_log` receipt to the First Leads `message_outcomes.sent_at`, both
  inside one process, no external GHL dependency.
- Lead *content* (3 teaser leads, owner contact withheld) is pushed by SMS; the
  signed dashboard link is sent as a separate `transactional` message because the
  two have different `message_type`s and cannot be one send.
- Owner phone stays gated behind subscription, unchanged from the GHL path.
