# LendingFlow intro SMS (T-09) runbook

Client spec: `Internal_Engineering_Specification_and_Sprint_Directive.md` §4.4 "10-Second Automated Intro SMS" and §5 Test 1.

## Design: our code decides, a GHL workflow sends

A borrower asks for a loan on LendingFlow; T-11 pushes the lead into the Next Deal Lending GHL sub-account (`src/lending/lendingflow_ghl.py`). The push does, in order: upsert the contact; add the tags in one call (`lendingflow`, plus `lendingflow-consent-missing` when the lead has no consent certificate); **if the lead has a consent certificate, add the tag `lendingflow-intro-ready` in a second call**; create the pipeline card; add a note.

The GHL workflow `NDL - Lead Intro SMS` starts on `lendingflow-intro-ready` and sends the text. **The consent decision is made in our code, never inside GHL**: a lead without a consent certificate never gets the ready tag, so nothing can start the workflow for it.

Why a tag trigger and not a webhook: GHL's *Inbound Webhook* trigger (and the Custom Webhook action) are premium actions this sub-account does not use. A tag trigger is a standard trigger and is the fastest option: nothing of ours sits between the push and the workflow.

| Piece | Where | Owner |
|---|---|---|
| Message wording, calendar link, "From" number | GHL workflow | Josh approves the copy; Sarath builds the workflow |
| Pushing the contact and its tags, deciding consent | T-11 (`lendingflow_ghl.push`, `_tags`, `_intro_ready_tags`) | built |
| The SMS number, 10DLC approval | GHL / carriers | Josh |

The SMS number is a GHL number Josh buys (ID verification, client only). It is **not** the BatchDialer calling number (727) 436-9951: calling and texting are separate numbers.

## What we verified in GHL on 2026-10-12 (test contacts, tags added through the API as T-11 does)

- **The tag trigger fires for tags added through the API**: contacts entered the workflow about one second after the tag call.
- **A GHL If/Else on a tag did NOT work as expected**: contacts carrying `lendingflow-consent-missing` still took the "Consent present" branch, even when the tag was added six seconds before the trigger tag. We did not find the cause, so the workflow has **no consent condition**: consent is decided by the ready tag.

## What the workflow relies on (do not change without updating the workflow)

- T-11 adds `lendingflow-intro-ready` **only** for a lead with `consent_status = 'present'`, in its own add-tags call after the first. Pinned by `tests/lending/test_intro_sms_trigger.py` (tag names, consent / no consent, call order, failure behaviour).
- If the ready-tag call fails, the push fails and T-11's sweep retries it (re-adding a tag that is already present does not trigger the workflow again, so a retry never double-texts). The lead's enrichment card and pre-qual letter wait for that retry, like any other failed GHL call in the push.
- T-11 never pushes a suppressed lead (`ghl_status = 'skipped'`), so a suppressed number never reaches GHL and never gets a text.
- A duplicate LendingFlow lead is not pushed again. A second lead from the same phone is upserted onto the same GHL contact; the ready tag is already there, so GHL does not trigger again and no second intro is sent.

## Workflow build steps (Next Deal Lending sub-account only)

Workflow name: `NDL - Lead Intro SMS`. Create the tag `lendingflow-intro-ready` first (Settings -> Tags; it already exists in this sub-account).

1. **Trigger:** *Contact tag* -> "Tag added" includes **`lendingflow-intro-ready`**. One trigger only, no extra filters (extra filters combine with AND). Do **not** use *Contact created* or the `lendingflow` tag.
2. **No If/Else.** Delete the "Consent check" step. Consent is decided before the tag is added.
3. **Send SMS:** body = the copy Josh approves. This action has **no From-number field**: it sends from the sub-account's **default SMS number**, so Josh's number must be set as the default (Settings -> Phone Numbers) and no other number may be the default. Build the action now but keep the workflow in Draft until Josh's number exists. Do not click the action's "Send test SMS" button until then: it sends a real text from the default number. Draft D3 (not final): `Hi {{contact.first_name}}, this is Josh Kantor with Next Deal Lending. Thanks for your loan request. I'd like to talk it through. Pick a time that works here: https://api.leadconnectorhq.com/widget/bookings/ndl-calender. Reply STOP to opt out.` The first-name merge field has **no fallback** in this account, so a lead with no first name would read "Hi , this is Josh Kantor...": ask David whether LendingFlow always sends a first name (his Oct 10 schema), and if not, use an If/Else on "first name is empty" with two Send SMS variants (test that condition before relying on it: the tag If/Else did not match). No property details. Keep the apostrophe in "I'd" a straight `'`. About 226 characters: two SMS segments, which is fine.
4. **Add tag `intro-sms-sent`** directly after Send SMS (check that it does not run when Send SMS errors). The Smart List `LendingFlow - not texted` (Tags include `lendingflow-intro-ready` and do not include `intro-sms-sent`) then shows leads that were never texted.
5. **Workflow settings:** **Allow re-entry off**, **Allow multiple opportunities off**, **Stop on response off**, no time window, no wait step. Do **not** add a Remove-tag step: the ready tag staying on the contact is what stops a retried push from triggering again.
6. Leave GHL's DND handling on (the Send SMS action skips contacts with DND set).
7. **Publish last**, only after the go-live checklist below. A published workflow with Send SMS starts texting real borrowers on the next LendingFlow lead, and adding `lendingflow-intro-ready` by hand to any contact texts that contact.

**Kill switch:** unpublish the workflow in GHL.

## Test 1 evidence (screen recording)

Show: a synthetic LendingFlow payload posted to `/api/lending/lendingflow`; the lead row (`lending.lendingflow_leads`: `received_at`, `ghl_contact_id`); the workflow's **Execution logs** / **Enrollment history** with the Send SMS timestamp; the text on a handset. Time to send = Send SMS timestamp minus `received_at`. Run it at least 10 times before the recording and keep the worst case: the 10 seconds is measured, not enforced.

```sql
SELECT lead_uuid, received_at, ghl_status, ghl_contact_id FROM lending.lendingflow_leads ORDER BY id DESC LIMIT 10;
```

## Known limits

- **No record in our database and no alert.** If the workflow fails or the number is not approved, it fails in GHL's execution log only. Check the log during the first live days and use the `LendingFlow - not texted` Smart List.
- **One intro per contact, ever.** A returning borrower (same phone, new LendingFlow lead) keeps the old ready tag, so they get no new intro.
- **Anyone adding `lendingflow-intro-ready` by hand in GHL triggers a text.**
- **Texts go out at any hour, including overnight** (client decision 2026-10-12). This is outside the repo's own 8 AM-8 PM ET floor used by GL-9/GL-10, and Josh's D1(b) is unanswered.
- **Consent:** `consent_status = 'present'` means the LendingFlow payload carried a non-empty consent field; its content is not validated. **DNC:** no national or state DNC scrub is run (a Tracerfy scrub is a minutes-long batch job on frozen credits); T-11 checks only our own suppression list and the contact's `do_not_contact` flag. Both are open compliance questions for Josh (D1(a), D1(c)).
- **UNVERIFIED until the live check:** the Send SMS step itself (no number yet), the Add-tag-after-SMS behaviour, and the real send time. A lead with no first name reads "Hi , this is Josh Kantor..." (no fallback).
- Two places still assume one number and are reported, not changed: the GL-9 text-back message prints `LENDING_GHL_SMS_FROM_NUMBER` after "call or text me back at", and `src/lending/prequal_ghl.py:32` prints (727) 436-9951 as "Call or text".

## Go-live checklist (the workflow stays unpublished-with-SMS until all are done)

- [ ] Sarath's task update report to Josh says Test 1 should read "fires from the GHL SMS number", not (727) 436-9951
- [ ] Follow-up (Sarath with Josh; not a build blocker): Josh's email received and the "NDL Calender" assigned to his GHL user, with his availability set; open the booking link once and confirm a slot can be picked
- [ ] GHL SMS number bought and ID-verified (Josh); set as the sub-account's **default** SMS number (the Send SMS action has no From picker); no other number is the default
- [ ] 10DLC / A2P approved for it, or the D2 fallback evidence agreed
- [ ] Josh-approved copy and calendar link in the workflow; placeholder wording gone
- [ ] First name: David confirms LendingFlow always sends it, or the two-variant If/Else is built and tested (the merge field has no fallback)
- [ ] D1 answers applied (consent, DNC); known limits above re-read
- [ ] One live test lead through T-11: the text arrives on a handset in under 10 s; repeat 10 times and record the worst case
- [ ] A human publishes the workflow with the Send SMS step

## Alternative kept on a branch

A code path that sends the SMS from our own code with a per-lead audit table, one retry and Slack alerts exists on the local branch `feat/ndl-t09-intro-sms-controlled-path` (it currently posts to a webhook URL; it would need the existing `GhlSmsSender` swapped in). It is not merged. Reach for it if GHL-only visibility turns out to be too thin.
