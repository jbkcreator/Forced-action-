# Call-One soft approval PDF (T-08)

Source: Internal Engineering Specification §4.3 "Call-One Soft Approval PDF Generator" and §5 Test 2; Chunk 1 developer split, T-08.
Status: draft. **Closed in production** (`LENDING_SOFT_APPROVAL_ENABLED=false`) until Josh and a qualified professional review the template and the calculation inputs.

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

1. **Borrower profile and core loan fields come from the LendingFlow lead (T-11).** Credit band, loan type, loan amount and state are assumed to be on the lead, matched to the call by normalized phone. This is not confirmed against David's schema (due Oct 10). T-11 is not built, so the production source (`UnavailableLeadProfileSource`) finds nothing and the feature produces no PDF until a real source is wired. A borrower who calls from a different number will not match.
2. **The calculation formulas are provisional**, not confirmed by Josh (`src/lending/soft_approval/calc.py`). They are our proposal: cap = the smallest of the ARV cap, the LTC cap and the max loan; rehab funding = min(rehab x funding share, cap); net loan = min(price x purchase advance + rehab funding, cap); max purchase price = the smallest of the price each limit allows. Amounts are rounded down to whole dollars, "borrower cash needed" up; that rounding is our assumption. Open questions for Josh: purchase advance share, LTC cap, what is cut first when the cap binds, whether "net loan" is the total loan or the closing advance (and whether fees are subtracted), rounding and display.
3. **Lender inputs are unconfirmed.** Every lender in `config/lender_matrix.py` is `verified=False`, and `config/lending_soft_approval.py` has no purchase-advance share for any lender. A lender without both shares is never used. No real PDF can be produced until Josh sends the lender rules.
4. **Lender choice:** the evaluator's first-ranked fitting lender whose fit still holds at its own computed loan amount. The PDF never names the lender. "Needs confirmation on the call" lenders are expected to be excluded by the evaluator (Josh's Oct 9 answer A4; T-05 PR #341 owns that change).
5. **Scope:** fix-and-flip only. DSCR has no rehab or ARV, construction has no stated ARV cap, and bridge sizing is unconfirmed.
6. **Who may submit:** anyone who can see the card. The Slack user id is stored in `submitted_by`. An allow-list is a small later change.
7. **Slack must be in HTTP mode.** Socket Mode must be off in the Next Deal Lending Slack app, the Interactivity Request URL must point at `/webhooks/lending/slack-interactivity` on lending-api, and `LENDING_SLACK_SIGNING_SECRET` must be set. The route returns 503 while the secret is unset.
8. **Interim deal record:** `lending.soft_approvals` (one row per lead, keyed by normalized phone; a changed resubmission updates it in place and appends the replaced values to `history`). It should be linked to the Deal record when T-11 builds it. The PDF is stored but not yet shown in Slack or attached in GHL.
9. **No card for DNC calls or calls with no talk time.** The card is internal; the feature sends nothing to a borrower, so it adds no outbound path.
10. **Copy safety:** the PDF shows three dollar estimates plus the facts the borrower gave. It carries no percentages, rates, points, terms or lender names (a test enforces this). The rule comes from Consolidated §5.2 (message generation) and the org policy; applying it to this PDF is our extension. Wording is a placeholder pending review.
11. **Header block:** `base.html` gained a `header_tag` block so this PDF does not say "Pre-Qualification Estimate". The same one-line change should be made in PR #342 so the two do not conflict.

## Operating it

- Migration (idempotent): `PYTHONPATH=. python migrations/apply_lending_soft_approvals.py`
- Settings: `LENDING_SOFT_APPROVAL_ENABLED`, `LENDING_SLACK_SIGNING_SECRET`, plus the existing `LENDING_SLACK_BOT_TOKEN` and `LENDING_DIAL_TASKS_CHANNEL` (#dial-tasks).
- Tests: `pytest tests/lending/test_soft_approval.py`
