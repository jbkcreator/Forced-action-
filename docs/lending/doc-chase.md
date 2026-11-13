# P2.4: closing blocker card and document chase

Code: `src/lending/closing/` (rules, drafts, blockers, doc_chase), upload page `src/api/lending_doc_upload_router.py`
on `lending-api`, settings and copy in `config/lending_closing.py`. Both parts run on Cora's minute sweep
(`fa-lending-cora`) and are **off** until their flag is set.

## Setup

1. `PYTHONPATH=. python migrations/apply_lending_doc_chase.py` (idempotent; **before** deploying: the stage webhook
   writes `lending.ghl_opportunity_stage`). It backfills each deal's current stage from `lending.ghl_stage_events`.
2. GHL: the "Pipeline Stage Changed" workflow that posts to `POST /webhooks/lending/ghl-stage` must fire for
   **every** stage of the lending pipeline, including moves made through the API (P2.3 relies on the same).
3. Env: `LENDING_CLOSING_BLOCKERS_ENABLED=true`, `LENDING_DOC_CHASE_ENABLED=true`, `LENDING_DOC_UPLOAD_DIR`
   (default `data/doc_uploads`, relative to the app directory), and `LENDING_PARTNER_LINK_BASE_URL` (https host of
   the upload links; already set for the partner door). Cora needs `LENDING_CORA_CHANNEL` and
   `LENDING_CORA_APPROVALS_CHANNEL` (already required).
4. The upload folder holds borrowers' financial documents: folder `0700`, files `0600`, never served by nginx,
   never copied to Drive or Slack. Include it in the server backup. There is no automatic deletion yet: it is built
   once Josh sets the retention period.

## 7 AM closing blocker card (#cora)

Once a day between 7:00 and noon ET (`lending.closing_blocker_cards` stops a second post). Lists every deal in
Application, Ready to fund or Submitted to lender (`ACTIVE_STAGE_KEYS`) for more than 3 days, oldest first, red.
Who holds it up: missing documents mean the borrower (the partner when the deal came from a partner's file);
otherwise Application = borrower, Submitted to lender = lender, Ready to fund = closing (title).

- Borrower deal with a document chase running: the chase does the reminding, the card shows the last step.
- Borrower deal without a chase: a check-in draft goes to #approvals (at most one per deal every 3 days in a stage).
- No text consent and no email: the card says to call.
- Lender or title: the card shows a ready-to-copy line for Josh. Cora never contacts lenders or title companies.

`stuck_deals(db, now)` in `src/lending/closing/blockers.py` is the shared query (P2.5 "daily blockers").

## Document chase

Start: an approver tells Cora the borrower accepted the term sheet, e.g. "Mark accepted the term sheet, start the
document chase" (tool `start_document_chase`; optional P2.2 `quote_id`, whose `INTEREST_RESERVE_FULL_TERM`
condition adds a document). One open chase per phone; asking again keeps the open one.

Checklist: Central Lending's list from Josh's Term Sheet Intake Checklist (Nov 2). Backflip has none (blocked).

Messages (approval cards in #approvals; text with consent, else email; the relay still checks consent, the 8 AM to
8 PM window, suppression, the lane deny list and the halt): the request on acceptance day, reminders on ET days 2, 5
and 8 while anything is missing. After an outage only the latest due step goes out. Each message carries its own
upload link; every link works for 30 days or until the chase closes. Template versions `DOCS-*` and `BLOCKER-*`
are counted by the P1.4 tracker like any other.

Closes: `complete` when every document is in, `cancelled` when the deal enters Funded, Lost or Nurture. Links are
revoked and Cora's inbox gets one item. Each upload also raises an inbox item ("3 of 6 received").
`get_document_chases` answers "who still owes documents?".

## Open with Josh

Active stages, the owner per stage, the message wording (`TEMPLATES`) and the document retention period.
