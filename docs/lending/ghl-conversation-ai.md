# GoHighLevel Conversation AI: reply agent, confirmations and reminders (WP-GL-10)

Status: configuration spec. The agent itself is set up in GoHighLevel, which needs Agency Admin on the
Next Deal Lending sub-account. Josh approved Conversation AI on condition that he sees the expected monthly
cost first (answer B2); do not switch it on before that. Texting also needs the 10DLC registration cleared
(the number must show A2P Verified).

## What the agent does

- Answers any text reply in seconds, day or night, and offers a booking slot.
- Books only into the **Next Deal Lending - Borrower Calls** GHL calendar (30-minute slots, 9:00 AM to
  7:15 PM ET, up to 8 held calls a day, October 18, 19 and 20 blocked). An agent booking is created
  **pending**; a caller completes the booking gate before it counts (WP-GL-5).
- Hands off to Josh on any rate, terms, pricing, points, fee or payment question, on anything it cannot
  answer from the approved copy, on a request to speak to a person, and on any complaint or legal threat.
- Texts always come from the Next Deal Lending number. Brand is "Next Deal Lending" everywhere.

## Hard rules (put these in the agent's instructions verbatim)

1. Never quote a rate, term, point, fee, ratio, payment or any range or ballpark. Not even "typically".
2. Never ask for or accept a credit score, SSN, bank or tax information, or documents. Credit is only ever
   a caller-asked "above or below 640" on a call, never in a text.
3. Never promise approval, funding, timing or terms.
4. Never mention Backflip or any other lender, and never discuss lender channel rules.
5. Honour STOP, "unsubscribe" and "wrong number" at once; do not reply to a STOP.
6. No AI voice calls. Text only.

## The rate / terms reply (from the caller playbook, adapted to text)

> Honestly, that's exactly why I'd like to get you on a call with Josh instead of guessing over text. It
> depends on the deal, your experience and the property, and he'll give you real numbers on your actual
> situation. Want me to set up a time? I have [slot] or [slot].

Then hand the conversation to Josh. The workflow below posts it to Slack.

## FAQ and website copy

Not final: the nextdeallending.com copy is not published yet (design export still outstanding), so the FAQ
cannot be loaded. Load the published copy and an FAQ Josh has read; the agent may use only that text.
Anything outside it is a handoff.

## GHL workflows to create (all send `X-Webhook-Secret` = `LENDING_GHL_WEBHOOK_SECRET`)

| When | Webhook | Purpose |
|---|---|---|
| Customer replied | `POST /webhooks/lending/ghl-reply` | A rate or terms question (or an agent handoff) is posted to Slack |
| Conversation AI sent a message | `POST /webhooks/lending/ghl-reply` with `direction: outbound` | An AI message containing a money amount, percentage or points raises an alert |
| Appointment status changed | `POST /webhooks/lending/ghl-appointment` | A cancelled or rescheduled appointment cancels its pending reminders |
| Contact DND changed | `POST /webhooks/lending/ghl-opt-out` (WP-GL-9) | STOP reaches the lending suppression list |

Slack: set `LENDING_REPLIES_CHANNEL` (channel ID) and invite the Cora Lending bot (`LENDING_SLACK_BOT_TOKEN`)
to it. Open: how Josh wants to be alerted and his business hours for the one-hour reply.

## Confirmations and reminders

Sent by `python -m src.lending.reminder_worker`, not by a GHL workflow, so every text passes the lending
suppression and consent checks: confirmation right after booking, reminder the evening before (6 PM ET,
confirmed by Josh, Oct 4), reminder 90 minutes before. Wording is the client-approved text in
`config/lending_reminders.py`. Switch on with `BOOKING_REMINDER_TEXT_ENABLED=true` only after 10DLC clears.

## Entry point and task list

A confirmed booking reaches the scheduler through `POST /webhooks/lending/booking-confirmed` (header `X-Webhook-Secret`, payload documented in `src/lending/booking_messages.py`; idempotent per `booking_ref`). Open: the GL-5 owner must call it when a booking is confirmed. Confirmation calls due are posted at 9am ET to `LENDING_DIAL_TASKS_CHANNEL` by `src.tasks.lending_confirmation_tasks`.

## Done-when checks (use a consented test contact)

1. Reply "Thursday works" -> the agent answers within seconds and offers a slot.
2. Reply "What's your rate?" -> no number in the answer, the Slack post appears, no alert is raised.
3. Make a booking through the caller flow -> a confirmation text arrives; `lending.booking_messages` shows the
   night-before and 90-minute rows pending.
4. Cancel the appointment in GHL -> both pending rows become `cancelled`.
5. Reply STOP -> no later text is sent to that number.
