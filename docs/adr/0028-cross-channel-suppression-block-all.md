# Cross-channel Do-Not-Contact: block-all-channels, two linked tables

---
Status: accepted
---

Email had no suppression enforcement at all — `send_email()` and the DBPR
marketing sender (`dbpr_email_sender.py`) had zero opt-out check, and the
only existing suppression flags (`DBPRContact.is_opted_out`/`is_hard_bounced`)
were scoped to campaign-membership bookkeeping for one Instantly integration,
not enforced as a real send-time gate. Meanwhile `sms_opt_outs` already
enforces SMS/voice suppression platform-wide via `compliance_gator.py`. This
ADR covers the two decisions made building a matching email-side mechanism
that a future reader will otherwise have to reverse-engineer.

## Decision 1: An opt-out blocks every channel, no exceptions

Opting out on any channel (email unsubscribe link, SMS STOP keyword) cascades
to suppress **every** channel for that contact — including transactional
email (receipts, payment-failed notices, login/magic-link mail).

**Rejected alternative:** marketing-only suppression, where transactional
mail keeps flowing after an opt-out (the industry-standard CAN-SPAM-minimum
approach, and what was initially recommended). Rejected because the business
wants a single, simple Do-Not-Contact promise to the contact: "opt out" means
opt out, full stop, not "opt out of some categories we define." The known
cost — a suppressed subscriber can no longer receive their own login link or
payment-failure notice via email — was explicitly accepted rather than
building a category-aware exemption system.

## Decision 2: Two linked per-channel tables, not one polymorphic table

Suppression lives in two tables: the existing `sms_opt_outs` (phone-keyed,
unchanged) and a new `email_opt_outs` (email-keyed, same shape/pattern).
They are not merged into a single `opt_outs(channel, identifier)` table.
Cross-channel cascade is application logic (`suppress_contact()` in
`src/services/email_suppression.py`), resolving the sibling identifier across
both populations that carry a phone+email pair — `Subscriber` (post-conversion)
and `DBPRContact` (pre-conversion marketing, incl. its `work_email` fallback) —
not a DB-level join or trigger. The cascade is bidirectional: the email
unsubscribe endpoint and the Instantly sync call it with an email; the SMS/IVR
opt-out path (`sms_compliance.record_opt_out`, which `record_ivr_opt_out`
delegates to) calls it with a phone. The national-DNC scrub (`dnc_refresh`)
is deliberately phone-only — a regulatory suppression list, not a per-contact
opt-out, so it does not cascade to email.

**Rejected alternative:** one polymorphic table keyed by `(channel,
identifier)`. Rejected because every existing enforcement point
(`compliance_gator.py` for SMS/voice) already queries `sms_opt_outs` by a
typed `phone` column, and every future email enforcement point queries by a
typed `email` column — a polymorphic table would require every caller to
filter on `channel` and would break the existing, already-shipped
`sms_opt_outs` contract for no real benefit. Keeping them separate also means
`sms_opt_outs` needed zero schema or caller changes — only new callers
(the cascade) were added.

## Consequences

- A contact with no sibling identifier on file (in either subscribers or
  dbpr_contacts) gets suppressed on the opt-out channel only — acceptable,
  since there is nothing to cascade to.
- `DBPRContact.is_opted_out`/`is_hard_bounced` keep their narrow, pre-existing
  meaning (Instantly campaign-membership bookkeeping) and are left as dead
  weight for enforcement purposes — `email_opt_outs` is the actual gate now.
  This avoids touching the existing Instantly sync's write path.
- If email-category-aware suppression is ever wanted (e.g. "still send
  receipts"), it requires a new decision to walk back Decision 1 — this is
  the explicitly-accepted hard-to-reverse cost of choosing simplicity now.
