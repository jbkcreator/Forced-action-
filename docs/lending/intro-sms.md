# LendingFlow intro SMS (T-09) runbook

Client spec: `Internal_Engineering_Specification_and_Sprint_Directive.md` §4.4 "10-Second Automated Intro SMS" and §5 Test 1.

## Design: a GHL workflow triggered by the tag T-11 already adds

There is **no T-09 code**. A borrower asks for a loan on LendingFlow; T-11 pushes the lead into the Next Deal Lending GHL sub-account (`src/lending/lendingflow_ghl.py`). The push does, in order: upsert the contact, **add the tags in one call**, create the pipeline card, add a note. The workflow starts on that tag and sends the text, so the text leaves seconds after the contact exists, before the rest of T-11's push and before our own event fires.

Why this and not a webhook: GHL's *Inbound Webhook* trigger (and the Custom Webhook action) are premium actions this sub-account does not use. A tag trigger is a standard trigger. It is also the fastest option: nothing of ours sits between the push and the workflow.

| Piece | Where | Owner |
|---|---|---|
| Message wording, calendar link, "From" number | GHL workflow | Josh approves the copy; Sarath builds the workflow |
| Pushing the contact and its tags | T-11 (`lendingflow_ghl._tags`, `push`) | already built |
| The SMS number, 10DLC approval | GHL / carriers | Josh |

The SMS number is a GHL number Josh buys (ID verification, client only). It is **not** the BatchDialer calling number (727) 436-9951: calling and texting are separate numbers.

## What the workflow relies on (do not change without updating the workflow)

- T-11 adds the tag `lendingflow` to every LendingFlow contact it pushes, and also `lendingflow-consent-missing` **in the same call** when the lead has no consent certificate. A test pins both names and the tag list (`tests/lending/test_intro_sms_trigger.py`; the single-call behaviour is pinned by `test_ghl_sink_tags_missing_consent_and_skips_card_without_stage` in `tests/lending/test_lendingflow.py`).
- T-11 never pushes a suppressed lead (`ghl_status = 'skipped'`), so a suppressed number never reaches GHL and never gets a text.
- A duplicate LendingFlow lead is not pushed again. A second lead from the same phone is upserted onto the same GHL contact; the tag is already there, so GHL does not trigger again and no second intro is sent.
- T-11's sweep re-pushes a lead that failed to deliver. Adding a tag that is already present does not trigger the workflow again, so a retry never double-texts; a lead whose first push failed before the tag step gets its text when the retry adds the tag.

## Workflow build steps (Next Deal Lending sub-account only)

Workflow name: `NDL - Lead Intro SMS`.

1. **Trigger:** *Contact tag* -> "Tag added" includes `lendingflow`. One trigger only. (Do **not** use *Contact created*: it fires before the tags exist, so the consent check below would run too early, and it also fires for web-form leads and every other contact.)
2. **If/Else:** if the contact has the tag `lendingflow-consent-missing` -> end. This is the consent gate: no certificate, no text.
3. **Send SMS:** From = the number Josh buys; body = the copy Josh approves. Add this action only after Josh's number exists. Draft D3 (not final): `Hi {{contact.first_name | default: 'there'}}, this is Josh Kantor with Next Deal Lending. Thanks for your loan request. I'd like to talk it through. Pick a time that works here: https://api.leadconnectorhq.com/widget/bookings/ndl-calender. Reply STOP to opt out.` Confirm the exact fallback syntax in GHL's merge-field picker. No property details. Keep the apostrophe in "I'd" a straight `'` (a curly one changes the SMS character set and shortens each segment). The text is about 226 characters: two SMS segments, which is fine.
4. **Workflow settings:** leave **Allow re-entry off** so one contact can never be texted twice by this workflow (confirm GHL's default for your account). Do **not** add a Remove-tag step: the tag staying on the contact is what stops a retried push from triggering again. Leave any wait or time-window step off: the text must go out immediately, at any hour (client decision 2026-10-12).
5. Leave GHL's DND handling on (the Send SMS action skips contacts with DND set).
6. **Publish last**, only after the go-live checklist below. A published workflow starts texting real borrowers on the next LendingFlow lead.

**Kill switch:** unpublish the workflow in GHL.

## Test 1 evidence (screen recording)

Show: a synthetic LendingFlow payload posted to `/api/lending/lendingflow`; the lead row (`lending.lendingflow_leads`: `received_at`, `ghl_contact_id`); the workflow's **Execution logs** / **Enrollment history** with the Send SMS timestamp; the text on a handset. Time to send = Send SMS timestamp minus `received_at`. Run this at least 10 times before the recording and keep the worst case: the 10 seconds is measured, not enforced.

```sql
SELECT lead_uuid, received_at, ghl_status, ghl_contact_id FROM lending.lendingflow_leads ORDER BY id DESC LIMIT 10;
```

## Known limits

- **No record in our database and no alert.** If the workflow fails or the number is not approved, it fails in GHL's execution log only. Check the log during the first live days.
- **One intro per contact, ever.** A returning borrower (same phone, new LendingFlow lead) keeps the old tag, so they get no new intro.
- **Anyone adding the `lendingflow` tag by hand in GHL triggers a text.**
- **Texts go out at any hour, including overnight** (client decision 2026-10-12). This is outside the repo's own 8 AM-8 PM ET floor used by GL-9/GL-10, and Josh's D1(b) is unanswered.
- **Consent:** `consent_status = 'present'` means the LendingFlow payload carried a non-empty consent field; its content is not validated. **DNC:** no national or state DNC scrub is run (a Tracerfy scrub is a minutes-long batch job on frozen credits); T-11 checks only our own suppression list and the contact's `do_not_contact` flag. Both are open compliance questions for Josh (D1(a), D1(c)).
- **UNVERIFIED until the live check:** trigger latency, the merge-field fallback syntax, and GHL's default for "Allow re-entry".
- Two places still assume one number and are reported, not changed: the GL-9 text-back message prints `LENDING_GHL_SMS_FROM_NUMBER` after "call or text me back at", and `src/lending/prequal_ghl.py:32` prints (727) 436-9951 as "Call or text".

## Go-live checklist (the workflow stays unpublished until all are done)

- [ ] Sarath's task update report to Josh says Test 1 should read "fires from the GHL SMS number", not (727) 436-9951
- [ ] Follow-up (Sarath with Josh; not a build blocker): Josh's email received and the "NDL Calender" assigned to his GHL user, with his availability set; open the booking link once and confirm a slot can be picked
- [ ] GHL SMS number bought and ID-verified (Josh); set as the workflow's "From"
- [ ] 10DLC / A2P approved for it, or the D2 fallback evidence agreed
- [ ] Josh-approved copy and calendar link in the workflow; placeholder wording gone
- [ ] D1 answers applied (consent, DNC); known limits above re-read
- [ ] One live test lead through T-11: the text arrives on a handset in under 10 s; repeat 10 times and record the worst case
- [ ] A human publishes the workflow

## Alternative kept on a branch

A code path that tags the contact only after our own checks, with a per-lead audit table, one retry and Slack alerts, exists on the local branch `feat/ndl-t09-intro-sms-controlled-path`. It is not merged. Reach for it if GHL-only visibility turns out to be too thin.
