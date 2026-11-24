# R1 0.2: who can open borrower documents (Backflip Wall)

Code: `src/lending/security/` (roles, staff tokens), `src/lending/closing/doc_access.py` (search, document list,
15-minute links), routes in `src/api/lending_doc_upload_router.py` on `lending-api`, Cora tool `send_document_link`
in `src/lending/cora_tools.py`. Builds on the P2.4 document chase (`docs/lending/doc-chase.md`).

## Who can open what

| Role | Who | Documents |
|---|---|---|
| admin | a Slack user in `LENDING_CORA_APPROVER_USER_IDS` (Josh), or a staff token issued with `--role admin` | can open any borrower upload through a 15-minute link |
| caller | everyone else: David, Cora operators, `--role caller` tokens | **none**. Callers see status only ("bank statement received / missing"), as today |

Removing someone from `LENDING_CORA_APPROVER_USER_IDS` (restart `fa-lending-cora`) or revoking their token stops
their open links at once: a link re-checks its issuer every time it is opened.

## Josh in Slack

In #cora (or any channel Cora answers in): *"Send me Mark Smith's bank statement."*

- One match: Cora sends an **only-visible-to-you** message with a link that opens the newest upload for 15 minutes.
  Nobody else in the channel sees it and Slack does not keep it in the history. The thread only says it was sent.
- Names match at the start of a word: "ann" finds Ann Lee, never Joanne.
- Several matches (two Marks): no link is made. Cora lists them as `Mark S. …0101` / `Mark T. …4432`, with whether
  each uploaded that document, and asks which one. Answer "the 0101 one".
- More than 10 matches: Cora asks for a last name or the last 4 digits.
- Link expired: *"This document link has expired. Ask Cora for a new one."*
- A caller asking gets *"only an admin can open borrower documents"*.

Documents: `bank_statements`, `government_id`, `purchase_agreement`, `valuation`, `scope_of_work`, `entity_docs`,
`interest_reserve_funds` (the Central Lending checklist in `config/lending_closing.py`). Partner Door uploads have
no links.

## Staff API

Base: `https://<LENDING_PARTNER_LINK_BASE_URL host>/api/lending/docs/staff/`, header
`Authorization: Bearer <staff token>`. Admin tokens only. A caller token gets **403** on every endpoint.

```bash
# find a borrower (body, not URL, so the name never reaches an access log)
curl -s -X POST "$BASE/borrowers/search" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"query": "Mark"}'
# what they uploaded (upload ids)
curl -s "$BASE/borrowers/web_lead:12/documents" -H "Authorization: Bearer $TOKEN"
# a 15-minute link to one upload -> {"url": ..., "expires_at": ...}
curl -s -X POST "$BASE/documents/345/view-link" -H "Authorization: Bearer $TOKEN"
```

The `url` opens or downloads the file (`GET /api/lending/docs/view/<token>`, no login, 15 minutes).

| Status | Meaning |
|---|---|
| 401 | no token, wrong token, revoked or older than 90 days |
| 403 | caller token; or a view link that is unknown or whose issuer is no longer admin |
| 404 | no such borrower / document, or the file is gone from disk |
| 410 | view link older than 15 minutes |

## Tokens

```bash
python -m src.lending.security.staff_tokens --issue "Josh" --role admin --by sarath   # printed once
python -m src.lending.security.staff_tokens --list
python -m src.lending.security.staff_tokens --revoke 3
```

Each token lasts 90 days; issue a new one before then. Never paste a token in Slack or email.

## The two walls

1. **API / Cora:** the role is checked when a link is asked for and again when it is opened.
2. **Database:** a caller's work runs as the Postgres role `lending_caller` (`SET LOCAL ROLE`), which has no
   privilege on `lending.doc_chase_uploads`, `doc_view_links`, `partner_file_uploads`, `staff_api_tokens` or
   `doc_chase_links` (`rbac.DOCUMENT_TABLES`: stored documents and the hashes of the tokens that open them). This covers every staff API request made with a caller token (set before the role
   check) and every free-form SQL lookup a non-admin asks Cora for. If the role is missing, those lookups are
   refused ("document access controls are not set up yet"), never run as the app's own login.

Cora's SQL tool also refuses any query naming `stored_path` or `token_hash`, and blanks them in `SELECT *`, for
admins too: Slack never shows where a file is stored. It also refuses `set_config(...)`, `current_setting(...)` and
the `*_to_xml(...)` functions, which could otherwise switch a caller back from `lending_caller` or run SQL hidden in
a string.

## Setup (in order)

1. `PYTHONPATH=. python migrations/apply_lending_document_access.py` (tables `doc_view_links`, `staff_api_tokens`).
2. A **Postgres superuser** (the app's own login has no CREATEROLE) runs
   `PYTHONPATH=. python migrations/apply_lending_caller_db_role.py --app-role <the DATABASE_URL user>`.
   `--dry-run` prints the statements. The script then checks the result (`has_table_privilege`): if a caller could
   still read any document table (for example through a PUBLIC grant), or could not read document status, it stops
   with an error and commits nothing.
3. Deploy `lending-api` and `fa-lending-cora`. No new setting and no new Slack scope (`chat:write` covers the
   private message). Links need `LENDING_PARTNER_LINK_BASE_URL` (already set for the upload links).
4. Issue Josh's and David's tokens.

Adding a table that holds documents or token hashes later: add it to `rbac.DOCUMENT_TABLES`, call
`rbac.revoke_from_caller_role` in its migration (as `apply_lending_document_access.py` does), and re-run step 2.

## Outside this wall (decided)

- The **Drive copy** ("NDL Borrower Documents" Shared Drive) has no 15-minute limit: access is Shared Drive
  membership. Check its members when people change.
- The **borrower upload link** keeps its 30 days. It uploads only and cannot open a stored file.

## Done When demo (record with timestamps)

1. David's caller token: `POST .../documents/<id>/view-link` → **403**.
2. David asks Cora for a bank statement → refused.
3. David asks Cora for `SELECT count(*) FROM lending.doc_chase_uploads` → refused by Postgres.
4. Josh's admin token → link → the PDF opens.
5. The same link after 15 minutes → **410**.
6. Josh in #cora with two borrowers named Mark → list → picks one → private link.
