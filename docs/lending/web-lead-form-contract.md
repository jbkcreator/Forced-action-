# Website lead form: contract for nextdeallending.com

`POST /api/lending/web-leads` (multipart form, served by `lending-api`; code in `src/api/lending_web_router.py` and
`src/lending/web_leads.py`). This is what the static page (separate `forced-action-ui` repo, branch
`next-deal-lending-site`) must send after the A2P resubmission changes
(Engineering Pod Action List, section 2).

## Fields

| Field | Required | Notes |
|---|---|---|
| `name` | yes | |
| `phone` | yes | any common US format; stored normalised |
| `email` | only if the Deal Drop box is ticked | the Deal Drop is a weekly **email**, so a ticked box without an email is a 422 |
| `property_city`, `deal_type`, `completed_projects_3y` | no | unchanged |
| `loan_amount` | no | whole dollars. Accepts `$250,000`, `250000`, `250k`, `$1.5M`. Blank is fine; anything that is not an amount (or is zero or above $100,000,000) is a 422 "Please check the loan amount." |
| `property_address` | no | max 200 characters |
| `property_state` | no | a USPS code or full state name is stored as the USPS code (`Florida` -> `FL`); anything else is stored as sent |
| `credit_band` | no | stored as sent, max 20 characters (the column's width). Not validated here. The partner form's bands (`Below 640`, `640-679`, `680-719`, `720-759`, `760+`) are in `config/lending_partners.py`; an open PR (#383) validates the web form against that list, so settle the page's values with that PR before it merges |
| `sms_consent` | no | box 1: **text messages only**. `yes` when ticked |
| `consent_text` | no | the exact label shown next to box 1; compared with `SMS_CONSENT_TEXT` |
| `call_consent` | no | box 2: **calls only**. `yes` when ticked |
| `call_consent_text` | no | the exact label shown next to box 2; compared with `CALL_CONSENT_TEXT` |
| `deal_drop_optin` | no | box 3: the weekly Deal Drop **by email**. It is not a text opt-in |
| `page_url` | no | |
| `company_website` | no | honeypot: hidden on the page, must stay empty |

All three boxes are unticked by default, none is required, and none is combined with accepting the terms.
Post `yes` for a ticked box (or omit it / send `no`).

## Consent wording

The server stores the label text the page actually showed and records whether it equals the constants in
`config/lending_web.py`:

- `SMS_CONSENT_TEXT` (box 1) is final in structure but still omits "(operated by HEU AI LLC)", which the client has not approved.
- `CALL_CONSENT_TEXT` (box 2) is the team-approved wording (option B, with how to stop calls). The client asked for the call-only box but supplied no wording of their own.

A page whose label differs from the constant is still accepted and stored verbatim, with the `*_text_matches` flag false.
Change the constant and the page together.

## Response

`200 {"received": true, "will_contact": <bool>}` where `will_contact` is true when the text box or the call box was ticked
and the number is not suppressed. A repeat inside 10 minutes returns `{"received": true, "duplicate": true}`.
`422 {"detail": "<plain message>"}` for an unusable submission.

## Deploy order

1. Re-run `PYTHONPATH=. python migrations/apply_lending_web_leads.py` against the shared DB (adds the new columns).
2. Deploy this repo (`lending-api`).
3. Deploy the page.

The page can only be photographed for the GHL campaign (screenshot) after step 3.
