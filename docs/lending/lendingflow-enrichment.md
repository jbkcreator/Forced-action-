# LendingFlow background enrichment card + routing (T-12)

Closed behind `LENDING_ENRICHMENT_ENABLED` (default false). Stacks on T-11 (PR #346): it needs `lending.lendingflow_leads` and the `LendingFlowLeadCreated` event.

## What it does
- Builds a card per lead in `lending.lendingflow_enrichment`: Sunbiz standing and officers, prior deeds, permits (Forced Action Hillsborough/Pinellas only, address match at the 0.92 auto-match tier), and the lender-fit view.
- Routes: address AND target close date within 30 Eastern calendar days (inclusive) = `FULL_MACHINE` (`closer_priority` set, card posted to the deal thread); otherwise `NURTURE` (a DB value only, not the `lending.contacts.nurture` DNC flag, no GHL stage move, no send). A past close date is `NURTURE`.
- When the address was captured on the call, the thread also gets a Street View link and Forced Action comps (WP-8B ARV engine, internal estimate).
- Re-runs update the row quietly; the thread is posted once per (address, close date, tag).

## Triggers
1. `LendingFlowLeadCreated` handler (subscribed in `lending-api`).
2. `POST /webhooks/lending/lendingflow-deal-facts` (`X-Webhook-Secret` = `LENDING_GHL_WEBHOOK_SECRET`): `{"phone", "source": "booking_form"|"slack_form", "property_address"?, "target_close_date"? (YYYY-MM-DD)}`.
3. Cron `*/5` `src.tasks.lending_enrichment_sweep` (creates rows for leads whose event was missed, retries failures with backoff, capped at 8 attempts).

## Josh's answers applied
A2 Backflip ranks first whenever it fits, the rest cheapest first, all others shown when Backflip misses. A3 fit score = percent of the 5 lenders that fit (Backflip's rows are one lender). A4 a band straddling a floor shows "needs confirmation on the call". B3 quiet updates, one letter per lead (T-07 unchanged). F3 close date comes from the booking form. B5 after-call entry is the Slack form.

## Not yet real (read before enabling)
- **Fit score is withheld** ("Lender rules pending confirmation") while any lender is unverified: all five are (A1 pending). Straddle notes likewise use verified lenders only.
- **Nothing posts to the deal-facts webhook yet.** The GHL booking-form workflow (close date) and the T-08 Slack form (address) must be wired to it; the T-08 form is on a separate branch. Payload names are our contract, unverified against GHL.
- **Deal thread** = Slack thread in `LENDING_DIAL_TASKS_CHANNEL` (default approved, not confirmed by Josh). Without Slack config the card is saved and not posted (warning logged).
- **Street View** needs `GOOGLE_MAPS_API_KEY`; without it the link reads "Not available". Posted links carry the pano id, never the key.
- **NURTURE cadence content** is unspecified: no messages are sent.
- Routing is not written to GHL tags (pending Josh's OK).
- Comps use after-repair condition 3 ("Average"); not specified by Josh.

## Deploy
`PYTHONPATH=. python migrations/apply_lending_lendingflow_enrichment.py` (after `apply_lending_lendingflow.py`), install the cron line, set the flag, restart `lending-api`.
