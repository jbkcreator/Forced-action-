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

## GHL account check (Oct 5, read-only API calls)

The configured lending account is the **Next Deal Lending** sub-account (not Bay Street). Already in place: calendar
"NDL Calender" (30-minute slots; id matches `LENDING_GHL_CALENDAR_ID`), pipeline "Booked Calls" with the stages Booked,
Held, No Show, Application, Submitted to Lender, Funded, Nurture and Lost (ids match `LENDING_GHL_PIPELINE_ID`,
`LENDING_GHL_STAGE_BOOKED`, `LENDING_GHL_STAGE_NURTURE`), and the contact fields Property Address and Text Consent.
**Not yet in place:** no workflows (so none of the four webhooks below exist), no texting number found, Conversation AI
not checked (the public API does not expose it). GHL's API cannot create workflows, pipelines or the AI agent, so those
are built in the GHL screens.

### Creating the workflows (GHL: Automation > Workflows > Webhook action)

Every webhook action: method POST, header `X-Webhook-Secret` = the value of `LENDING_GHL_WEBHOOK_SECRET` (generate one with
`python -c "import secrets; print(secrets.token_urlsafe(32))"`, set it in the server `.env`, and paste it only into GHL).
In the webhook action use **Custom Data** to send the keys the code reads, so the names are not guesswork:

| Workflow trigger | URL (`<API>` = the public API host) | Custom data keys the code reads |
|---|---|---|
| Customer replied / Conversation AI sent a message | `<API>/webhooks/lending/ghl-reply` | `messageId`, `body`, `direction` (inbound/outbound), `contactId`, `firstName`, `phone`; `userId` only on a message a person typed |
| Appointment status changed (new, confirmed, rescheduled, cancelled) | `<API>/webhooks/lending/ghl-appointment` | `appointmentId`, `appointmentStatus`, `startTime` (ISO 8601 with offset, e.g. 2026-10-07T10:00:00-04:00), `firstName`, `phone`, `email` |
| Pipeline stage changed (add as a second webhook on the stage workflow) | `<API>/webhooks/lending/ghl-nurture` | `stage_name`, `phone` |
| Contact DND changed | `<API>/webhooks/lending/ghl-opt-out` (WP-GL-9) | per the WP-GL-9 runbook |

The code also accepts GHL's default nested payloads (`contact.*`, `calendar.*`), but custom data is unambiguous. To confirm
what GHL really sends, set `LENDING_GHL_LOG_PAYLOAD_SHAPE=true` on the server, fire one test event per workflow, and read
the `[ghl-payload-shape]` log lines (key paths and value types only, never values). Turn it off afterwards.

## The agent as built in GHL (Oct 6) and where it is served

**Served by:** the lending-api service (`127.0.0.1:8010`, systemd `lending-api`), behind its own nginx site for
`api.nextdeallending.com` (and the server IP, for tests). Forced Action's site does not serve lending paths. Workflow URLs
are `https://api.nextdeallending.com/webhooks/lending/<path>`; they need the DNS record and the certificate first (the secret
travels in a header, so never use plain http in production).

**Agent "NDL Reply Agent"** (Conversation AI, Prompt Based, started from scratch; model GPT 4.1; SMS only; Mode Off until the
deploy, the texting number and 10DLC are done):
- *Instructions:* the six hard rules and the rate and terms reply above; offer only open calendar times, at most three; book
  30-minute calls with Josh; hand off anything it cannot answer, a request for a person, a complaint or a lawyer mention; never
  change, cancel or move a call itself (hand off to Josh).
- *Actions:* **Appointment Booking** on "NDL Calender" (pause the bot for 7 days after booking; the bot may not cancel or
  reschedule); **Stop Bot** with three scenarios, each with *Reactivate bot after* off:
  1. *Rate or terms question* (rates, interest, points, fees, payments, costs, pricing, loan terms, LTV, LTC, ARV, how much they
     can borrow or you can lend, any quote): final message "Honestly, I'd rather get you on a call with Josh than guess over text.
     He'll give you real numbers on your deal and follow up shortly." (GHL caps the final message at 150 characters, so it cannot
     also offer slots).
  2. *Wants Josh* (a person, a complaint, a lawyer, or to reschedule, cancel or move the call): "Thanks for letting us know. Josh
     will follow up with you shortly."
  3. *Opt-out* (wrong number, don't text me again, remove me): "Understood, sorry for the mix-up. We won't text you again." (GHL
     requires a non-empty final message; a bare STOP is handled by GHL's own DND and never reaches the bot).
- *Timing:* 2-second wait, at most 15 messages a conversation, sleep on a manual message (Josh or a caller typing), not on
  workflow messages (so it still answers replies to our automated texts). Response behaviour: images and voice notes off.
- *Knowledge base:* empty until the website copy and an FAQ Josh has read exist.

**Tested in GHL's test panel (no SMS sent):** a day request offers three real slots; rate, fee, LTV, LTC, "how much can you
lend me", a ballpark push and "what do other lenders charge" all hand off with no number; "can I get approved with bad credit"
promises nothing; an SSN is refused and not repeated; "do you use Backflip" is not confirmed; a call request, a lawyer mention
and a reschedule request hand off; "wrong number" gets the single confirmation. **The test panel books a real appointment**
on the live calendar (cancel it afterwards); it is not a sandbox.

**Slot release on a failed check:** when a contact enters the Nurture stage before an AI-booked call, `/ghl-nurture` also cancels
the GHL appointment (`LENDING_GHL_RELEASE_SLOT_ENABLED=true`, off by default; turn it on after the end-to-end test). If GHL
refuses, a message is posted to the replies channel so the slot is cancelled by hand.

## Entry point and task list

A confirmed booking reaches the scheduler through `POST /webhooks/lending/booking-confirmed` (header `X-Webhook-Secret`, payload documented in `src/lending/booking_messages.py`; idempotent per `booking_ref`). Open: the GL-5 owner must call it when a booking is confirmed. Confirmation calls due are posted at 9am ET to `LENDING_DIAL_TASKS_CHANNEL` by `src.tasks.lending_confirmation_tasks`.

## Failed caller check on an AI-booked call

Josh approved (Oct 4): release the slot, send the contact one short message (text if they consented, email otherwise), move them to nurture, cancel their reminders. GL-10 does the message and the reminder cancel via `POST /webhooks/lending/booking-gate-failed` (`{"booking_ref": ...}`, same secret); The trigger is a GHL workflow action: when a caller moves an AI-booked contact to the Nurture stage before the call, post that stage entry to `POST /webhooks/lending/ghl-nurture` (`stage_name`, `phone`; add it as a second webhook on the workflow that feeds `/ghl-stage`). Releasing the GHL appointment is the caller's action in GHL. Wording: `GATE_FAIL_TEXT` in `config/lending_reminders.py`. Email goes through GHL (`src/lending/ghl_email.py`) from `LENDING_GHL_EMAIL_FROM` once `BOOKING_REMINDER_EMAIL_ENABLED` is on; that needs the sub-account's email domain verified first. Check it once with `python -m src.lending.ghl_email --send-test <your address>`.

## Done-when checks (use a consented test contact)

1. Reply "Thursday works" -> the agent answers within seconds and offers a slot.
2. Reply "What's your rate?" -> no number in the answer, the Slack post appears, no alert is raised.
3. Make a booking through the caller flow -> a confirmation text arrives; `lending.booking_messages` shows the
   night-before and 90-minute rows pending.
4. Cancel the appointment in GHL -> both pending rows become `cancelled`.
5. Reply STOP -> no later text is sent to that number.
