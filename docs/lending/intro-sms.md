# LendingFlow intro SMS (T-09) runbook

Client spec: `Internal_Engineering_Specification_and_Sprint_Directive.md` §4.4 "10-Second Automated Intro SMS" and §5 Test 1.

## Design: our code decides, a GHL workflow sends

A borrower asks for a loan on LendingFlow; T-11 pushes the lead into the Next Deal Lending GHL sub-account (`src/lending/lendingflow_ghl.py`). The push does, in order: upsert the contact; add the tags in one call (`lendingflow`, plus `lendingflow-consent-missing` when the lead has no consent certificate); **if the lead has a consent certificate, add the tag `lendingflow-intro-ready` in a second call**; create the pipeline card; add a note.

The GHL workflow `NDL - Lead Intro SMS` starts on `lendingflow-intro-ready` and sends the text. **The consent decision, the send window and the off switch are all in our code, never inside GHL**: a lead without a consent certificate, or one we may not text right now, never gets the ready tag, so nothing can start the workflow for it.

**Off switch:** `LENDING_INTRO_SMS_ENABLED` (default `false`) on the server. While it is off the ready tag is never added, so no lead is texted whatever state the workflow is in. Turn it on only after Josh confirms LendingFlow's consent covers this text and that a DNC/litigator check runs (D1). It also needs `LENDING_GHL_CF_INTRO_NAME` (below); with the switch on and that unset, nobody is texted and an error is logged.

**Send window (Josh, 2026-10-13: "8am to 8pm borrower local time, anything outside waits until 8am"):** `compliance.text_window_open()` requires 08:00-20:00 in **every** timezone we can derive: the phone's area code (or a Hillsborough/Pinellas ZIP) and each zone the property's state touches (`config/lending_compliance.STATE_TZ`; a state spanning zones lists them all). A borrower may have moved since their number was issued, so the strictest reading wins: a Pacific number on a Florida property is texted only 11:00-20:00 Eastern. No derivable timezone, no text. Outside the window the contact is still pushed to GHL, but the ready tag is withheld (`lending.lendingflow_leads.intro_sms_tagged_at` stays NULL). Each run of `lending_lendingflow_sweep` (every 5 minutes) tags such leads once the window is open, the borrower is still not suppressed, and the lead is under `INTRO_SMS_HOLD_MAX_HOURS` (14) old, so a text is at most about 5 minutes after 8am local. Do **not** add a time window or Wait step in GHL.

**Greeting:** GHL's first-name merge field has no fallback, so the text uses the contact custom field `intro_name` instead (`LENDING_GHL_CF_INTRO_NAME` = its id). The upsert sets it to the lead's first name, or `there` when the lead has none, before the tag is added. A missing name never overwrites the name already on the contact.

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

- T-11 adds `lendingflow-intro-ready` **only** for a lead with `consent_status = 'present'` **and** a clear suppression re-check, in its own add-tags call after the first. `deliver_pending` re-checks the current suppression list (phone or email) and `lending.contacts.do_not_contact` on **every push, including retries**, because an opt-out after intake does not change the lead's stored `suppressed` flag and a lead can sit in retry for up to 72 hours. If the re-check finds an opt-out, or the lookup itself fails, the contact is still delivered to GHL but the ready tag is withheld (fail closed); the sink adds the tag only when the flag is explicitly true. Pinned by `tests/lending/test_intro_sms_trigger.py` (tag names, consent / no consent, the explicit clear flag, call order, failure behaviour, and a retry after a phone-suppression, email-only-suppression and `do_not_contact` opt-out).
- If the ready-tag call fails, the push fails and T-11's sweep retries it (re-adding a tag that is already present does not trigger the workflow again, so a retry never double-texts). The lead's enrichment card and pre-qual letter wait for that retry, like any other failed GHL call in the push.
- A lead suppressed **at intake** (`ghl_status = 'skipped'`) is never pushed, so it never reaches GHL. A lead suppressed **after** intake and still waiting in retry is still delivered to GHL as a contact (existing T-11 behaviour) but never gets the ready tag, so it is never texted by this workflow.
- A duplicate LendingFlow lead is not pushed again. A second lead from the same phone is upserted onto the same GHL contact; the ready tag is already there, so GHL does not trigger again and no second intro is sent.

## Workflow build steps (Next Deal Lending sub-account only)

Workflow name: `NDL - Lead Intro SMS`. Create the tag `lendingflow-intro-ready` first (Settings -> Tags; it already exists in this sub-account).

1. **Trigger:** *Contact tag* -> "Tag added" includes **`lendingflow-intro-ready`**. One trigger only, no extra filters (extra filters combine with AND). Do **not** use *Contact created* or the `lendingflow` tag.
2. **No If/Else.** Delete the "Consent check" step. Consent is decided before the tag is added.
3. **Send SMS:** body = the copy Josh approved (2026-10-13), no company name: `Hi {{contact.intro_name}}, this is Josh Kantor, loan officer. Thanks for your loan request, I'd love to help you get it funded. Grab a time that works for you here: https://api.leadconnectorhq.com/widget/bookings/josh-kantor. Reply STOP to opt out.` Use the custom field `intro_name` (our upsert sets it to the first name, or `there`), **not** `{{contact.first_name}}`, which has no fallback in this account. Create the field first (Settings -> Custom Fields, text) and set its id in `LENDING_GHL_CF_INTRO_NAME`. The booking calendar is "Josh's Calendar" with the slug `josh-kantor` (the old `ndl-calender` slug had a typo and the company initials); open the link and book a test slot before go-live. Keep the body to Josh's wording only (no pasted review comments). This action has **no From-number field**: it sends from the sub-account's **default SMS number**, (656) 221-1100 "Next Deal Lending Main" (bought and ID-verified by Josh; no other number may be the default). Keep the workflow in Draft until go-live and do not click the action's "Send test SMS" button except to your own handset: it sends a real text. No property details. Keep the apostrophe in "I'd" a straight `'`.
4. **Add tag `intro-sms-sent`** directly after Send SMS (check that it does not run when Send SMS errors). The Smart List `LendingFlow - not texted` (Tags include `lendingflow-intro-ready` and do not include `intro-sms-sent`) then shows leads that were never texted.
5. **Workflow settings:** **Allow re-entry off**, **Allow multiple opportunities off**, **Stop on response off**, **no time window and no Wait step** (the send window is enforced in our code, see above). Do **not** add a Remove-tag step: the ready tag staying on the contact is what stops a retried push from triggering again.
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
- **A small window remains between our suppression check and GHL's send** (seconds): an opt-out landing in that gap relies on GHL's own do-not-disturb handling, which can lag for phone-based opt-outs and does not exist for email-only opt-outs.
- **Send window:** 8am-8pm borrower local time (Josh, 2026-10-13), enforced in code with the strictest reading of every timezone we can derive (see Design). It is not Eastern-time based like GL-9/GL-10. A lead whose timezone cannot be derived is never texted. The release step runs every 5 minutes, so a held text goes out within about 5 minutes after the window opens. Whether this window is the legally required one is Josh's call (consent and hours are D1).
- **Consent:** `consent_status = 'present'` means the LendingFlow payload carried a non-empty consent field; its content is not validated. **DNC:** no national or state DNC scrub is run (a Tracerfy scrub is a minutes-long batch job on frozen credits); T-11 checks only our own suppression list and the contact's `do_not_contact` flag. Both are open compliance questions for Josh (D1(a), D1(c)).
- **UNVERIFIED until the live check:** the Send SMS step itself, the Add-tag-after-SMS behaviour, the real send time, and that the GHL upsert accepts `customFields: [{id, value}]` for `intro_name` (the same shape `web_lead_ghl` uses for its consent fields; test it once on a throwaway contact).
- Two places still assume one number and are reported, not changed: the GL-9 text-back message prints `LENDING_GHL_SMS_FROM_NUMBER` after "call or text me back at", and `src/lending/prequal_ghl.py:32` prints (727) 436-9951 as "Call or text". The texting number is now (656) 221-1100 (calls forward to Josh's work line): set `LENDING_GHL_SMS_FROM_NUMBER` to it on the server.

## Go-live checklist (the workflow stays unpublished-with-SMS until all are done)

- [ ] Sarath's task update report to Josh says Test 1 should read "fires from the GHL SMS number", not (727) 436-9951
- [ ] Calendar: "Josh's Calendar", slug `josh-kantor`, no company name or branding on the booking page or its confirmations; assigned to Josh's GHL user (jbkantor@gmail.com) so he can connect Google Calendar; his availability set (weekdays only?: ask); open the link and book a test slot
- [ ] GHL SMS number (656) 221-1100 is the sub-account's **default** SMS number (the Send SMS action has no From picker); A2P / 10DLC registration approved for it (the legal business name is for carriers only, not shown to borrowers), or the D2 fallback evidence agreed
- [ ] 10DLC / A2P approved for it, or the D2 fallback evidence agreed
- [ ] Josh's approved copy and the `josh-kantor` link are in the workflow, with `{{contact.intro_name}}`; no pasted comments in the body
- [ ] `intro_name` custom field created in GHL, its id set in `LENDING_GHL_CF_INTRO_NAME`, and one throwaway lead with no first name reads "Hi there"
- [ ] Josh confirms in writing that LendingFlow's consent covers this text and that a DNC/litigator check runs (D1); only then set `LENDING_INTRO_SMS_ENABLED=true` on the server. Known limits above re-read
- [ ] One live test lead through T-11: the text arrives on a handset in under 10 s; repeat 10 times and record the worst case
- [ ] A human publishes the workflow with the Send SMS step

## Alternative kept on a branch

A code path that sends the SMS from our own code with a per-lead audit table, one retry and Slack alerts exists on the local branch `feat/ndl-t09-intro-sms-controlled-path` (it currently posts to a webhook URL; it would need the existing `GhlSmsSender` swapped in). It is not merged. Reach for it if GHL-only visibility turns out to be too thin.
