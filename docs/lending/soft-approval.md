# Call-One soft approval PDF (T-08)

Source: Internal Engineering Specification §4.3 "Call-One Soft Approval PDF Generator" and §5 Test 2; Chunk 1 developer split, T-08.
Status: draft. **Closed in production** (`LENDING_SOFT_APPROVAL_ENABLED=false`) until Josh (B4) and a qualified professional review the template and the calculation inputs.

## What it does

After a finished call, a card is posted to `LENDING_DIAL_TASKS_CHANNEL` (#dial-tasks). The caller opens the form from the card and enters the property facts. The service then computes three estimated figures, renders a non-binding PDF with the shared PDF helper (T-07, `src/lending/pdf/`), and stores it against the lead in `lending.soft_approvals`.

```
call ends (CDR poller) -> call_pipeline.follow_up -> post_soft_approval_card   (slack_card.py)
caller clicks "Enter deal facts" -> POST /webhooks/lending/slack-interactivity   (soft_approval_webhook.py, lending-api)
form submit -> acknowledged at once -> background: generate_soft_approval        (soft_approval/service.py)
  lead profile (LendingFlow, by phone) -> evaluate_lender_fit -> per lender: figures (calc.py), re-fit at the computed loan
  -> render_pdf -> lending.soft_approvals (one row per lead) -> thread reply on the card (status only, no amounts)
```

Nothing is sent to the borrower by this feature. The borrower email is T-07's GHL flow.

## The Slack form collects only property facts

Property address, purchase price, rehab budget, estimated ARV, property type (optional), target close date (optional). Every core fact is required; a missing one is an inline error and no PDF is made.

## Assumptions and open items (also listed in the PR)

1. **Borrower profile and core loan fields come from the LendingFlow lead (T-11).** `LendingFlowLeadProfileSource` (`src/lending/soft_approval/lead_source.py`) reads credit band (`credit_band_min_fico`), loan type, loan amount and state from the most recent `lending.lendingflow_leads` row for the call's normalized phone. T-11 already stores the loan type as a `LoanType` value (an unrecognised spelling is NULL) and the phone normalized. A missing or unrecognised value is never guessed (the service stops with `lead_missing_core_fields`), and a suppressed latest lead is never looked past (no lead). A borrower who calls from a different number will not match. Field names and loan type spellings are not yet confirmed against David's LendingFlow schema.
2. **The calculation follows Josh's B1(c) formulas** (Oct 8 answer, `src/lending/soft_approval/calc.py`), in Decimal with every percentage stored 0 to 1: total cost = purchase + rehab; requested loan = purchase x purchase share + rehab x rehab share; loan = the smallest of requested, LTC cap, ARV cap and max loan, rounded down to the nearest $1,000; below the lender's minimum it is no fit with reason `loan_too_small`; rehab stays whole and the purchase advance is cut first; rehab funding = min(rehab x share, loan); purchase advance = loan - rehab funding (never below zero); cash at closing = purchase - purchase advance (before closing costs); max allowable purchase price = the highest price, stepped down in $1,000 steps, at which no cap cuts the loan. The cap that cut the loan (`requested`, `ltc`, `arv`, `max_loan`) is stored on every result, with the flags `upside_down` (ARV below total cost) and `arv_over_3x`, which never reject. The PDF's "estimated loan amount" is the total loan (Josh's "loan"; the spec's "net loan amount" is not defined in his answer, so this is our default: **Q6**). Which lender's rules feed the PDF is still unanswered (B1(c)): the provisional rule is the first ranked fit, Backflip first (A2).
3. **Lender inputs are placeholders until Josh confirms them (A1).** A lender with no confirmed purchase advance or rehab share takes his placeholder box (`PLACEHOLDER_PARAMS`: 85% of the purchase, 100% of the rehab), and a cap the lender has no value for takes the T-05 placeholder from `effective_rules`, so the pipeline runs end to end. The result is stored flagged `subject_to_confirmation` (in `lending.soft_approvals.figures`) and the Slack reply says so; the PDF itself is not marked (**Q4**, our default). A lender is only treated as confirmed when every share and every rule is.
4. **Lender choice:** the first ranked lender, Backflip first, whose fit still holds at its own computed loan. A lender the evaluator declined only on the LTV or LTC cap is still sized, because the formulas cut the loan to the caps (B1(c)); the re-fit at the computed loan decides. The PDF never names the lender. "Possible, confirm on the call" lenders (T-05) are not used.
5. **Scope:** fix-and-flip only. DSCR has no rehab or ARV, construction has no stated ARV cap, and bridge sizing is unconfirmed.
6. **Who may submit (B11):** only the Slack users on `LENDING_SOFT_APPROVAL_USER_IDS` (comma-separated), falling back to `LENDING_CORA_APPROVER_USER_IDS`. Josh only until he names callers. Empty means nobody (fails closed). Checked when the form is opened (an ephemeral notice) and again on submit. Josh's Slack user id is in none of our documents: set it before enabling. The Slack user id is stored in `submitted_by`.
7. **Slack must be in HTTP mode.** Socket Mode must be off in the Next Deal Lending Slack app, the Interactivity Request URL must point at `/webhooks/lending/slack-interactivity` on lending-api, and `LENDING_SLACK_SIGNING_SECRET` must be set. The route returns 503 while the secret is unset.
8. **Interim deal record and viewing (B7, D6):** `lending.soft_approvals` (one row per lead, keyed by normalized phone; a changed resubmission updates it in place and appends the replaced values to `history`). Josh shows the PDF on the call: when one is generated it is uploaded to the card's Slack thread (a file named `soft-approval-<last four digits>.pdf`, internal, never sent to the borrower). This needs the `files:write` scope on the lending Slack app; if the upload fails the PDF is still stored and the thread reply still posts. **Q2:** neither the Q&A nor the spec says where Josh opens it (spec 4.3: "attached to the deal record"); the Slack file is our interim until the Deal record exists. **D6:** Person/Property/Deal records are T-11's (spec 4.4); link the PDF to the Deal when they exist.
9. **Card retry:** the poller posts the card once. `src.tasks.lending_soft_approval_card_retry` (cron */5, no-op while the flag is off) posts it for finished calls from the last 6 hours (older than 2 minutes) that have no row in `lending.soft_approval_cards`, up to 50 per run, so a Slack outage does not leave a call without its form. Switching the flag on can therefore post cards for calls that ended in the previous 6 hours. If a post succeeds but the `message_ts` update then fails, the claim rolls back and a retry posts a second card for that call (internal card, accepted).
10. **No card for DNC calls or calls with no talk time.** The card is internal; the feature sends nothing to a borrower, so it adds no outbound path.
11. **Copy safety:** the PDF shows three dollar estimates plus the facts the borrower gave. It carries no percentages, rates, points, terms or lender names (a test enforces this). The rule comes from Consolidated §5.2 (message generation) and the org policy; applying it to this PDF is our extension. Wording is a placeholder pending review.
12. **Branding (B2):** Josh's Oct 8 answer: no company name on anything the borrower sees, signed "Josh Kantor, Loan Officer". This is done by T-07 on `dev` (shared `BORROWER_SIGNER` in `base.html`, passed by `render.py`), so this PR does not touch `base.html`. What this PR changes in `soft_approval.html`: it no longer says "reply to this email" (B7: nothing is emailed), and it prints "These figures are subject to lender confirmation" when the result is flagged (A1). The hello@ address stays (B5). The Q&A line "PDF branding: Next Deal Lending name only" is stale.
12a. **Slack card:** after each submit the card shows the last result with its time ("PDF ready", "No PDF: <reason>") and the button reads "Re-enter deal facts" once a PDF exists. Best-effort `chat_update` with the existing `chat:write` scope; the thread reply and the stored PDF stay the record.
13. **Deal facts reach the lead card (F5):** a submitted form calls `record_deal_facts(source="slack_form")` so the address and close date feed Street View, comps and routing, only while `LENDING_ENRICHMENT_ENABLED` is on, then re-runs the enrichment. The booking form's field names are still unanswered.
14. **Still open (build defaults, not decisions):** Q1 Josh's Slack user id; Q2 where he opens the PDF; Q3 which lender feeds it; Q4 whether the PDF itself says "subject to lender confirmation"; Q6 what "net loan" means; D6 the Deal record.

## Operating it

- Migration (idempotent): `PYTHONPATH=. python migrations/apply_lending_soft_approvals.py`
- Settings: `LENDING_SOFT_APPROVAL_ENABLED`, `LENDING_SOFT_APPROVAL_USER_IDS`, `LENDING_SLACK_SIGNING_SECRET` (the Slack app also needs `files:write`), plus the existing `LENDING_SLACK_BOT_TOKEN` and `LENDING_DIAL_TASKS_CHANNEL` (#dial-tasks).
- Tests: `pytest tests/lending/test_soft_approval.py`
