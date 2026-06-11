# Voter/tax-collector phones and emails are isolated from the auto-send path

**Status:** accepted

## Decision

Contact data ingested from the Supervisor of Elections voter registry
(`voters.phones` JSONB, `voters.phone_1`, `voters.email`) and from Tax
Collector records (`enriched_contacts` rows with `source='tax_collector'`)
is **never auto-promoted** into `owners.phone_1/2/3`, `owners.email_1/2`,
or any SMS (Telnyx) / voice (Synthflow) / email send path.

These contacts:

- **count toward** the contactability profile baseline,
- **feed** the best-mailing-address resolver and the `direct_mail_eligible`
  flag (direct-mail fallback when the skip-trace waterfall misses),
- **do not** enter outbound voice/SMS/email targeting.

Routing one of these phones/emails into outreach requires first passing it
through the skip-trace waterfall's Tracerfy DNC/litigator scrub — i.e. it
must re-enter via the normal `EnrichedContact` promotion flow, not a bulk
backfill.

## Why

- **TCPA exposure.** Owner phones from skip trace pass Tracerfy's inline
  DNC/litigator scrub before they are written to `owners.phone_*`. Voter and
  tax-collector phones would bypass that scrub entirely if promoted at load.
- **Wrong-party risk.** A voter row is matched to a property by residential
  address. Multiple voters per property are *intended* (they are the
  "alternative contact network") — tenants, adult children, co-residents.
  Their phones frequently belong to non-consenting non-owners; auto-dialing
  them is both a compliance and a brand problem.
- **Mail is the safe channel.** Direct mail has no TCPA consent requirement,
  which is why these sources route to the mail fallback instead.

## Why this is surprising (read before "fixing" it)

A future engineer will see thousands of phone numbers sitting in `voters`
and `enriched_contacts(source='tax_collector')` next to owners who have
`skip_trace_success = false` and no phone, and will be tempted to write the
obvious backfill (`UPDATE owners SET phone_1 = ...`). That backfill is the
exact thing this ADR forbids. The gap is deliberate: contactability ≠
sendability. If you need these numbers in outreach, build the scrub-first
promotion path; do not copy columns.

## Considered alternatives

- **Path 1 — promote at load, scrub at send time.** Rejected: send-time
  scrubbing is enforced per-channel (`sms_compliance.send_sms`) but owner
  phone columns are read by GHL sync, Synthflow dispatch, and lead-pack
  exports — too many consumers to guarantee the scrub holds everywhere.
- **Path 3 — don't store phones/emails at all.** Rejected: throws away free
  signal; the data is public record and useful as *context* (e.g. operator
  manually verifying a lead) even when never auto-dialed.
