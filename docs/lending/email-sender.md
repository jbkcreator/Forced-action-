# Lending email sender: From David, BCC Josh

Every Next Deal Lending email, including the LendingFlow first touch, is sent **From
`david.willis@nextdeallending.com`** with **`josh@nextdeallending.com` in BCC**. There is never a CC.

## Server (code)

One identity, read only through `src/lending/email_identity.py`:

| Setting | Value |
|---|---|
| `LENDING_EMAIL_FROM` | `david.willis@nextdeallending.com` |
| `LENDING_EMAIL_BCC` | `josh@nextdeallending.com` (comma-separated if more are ever added) |

If either setting is unset or invalid, **no lending email is sent**. An ERROR is logged, and each sender refuses
before any GHL or Instantly call. The settings it replaces (`LENDING_GHL_EMAIL_FROM`, `LENDING_PREQUAL_EMAIL_FROM`,
`LENDING_INSTANTLY_SENDER_EMAIL`) are no longer read. They can be removed from `.env`.

| Email | Code | How From / BCC are set |
|---|---|---|
| Cora-approved emails, document chase, booking reminders, gate-fail message | `ghl_email.py` | `emailFrom` + `emailBcc` on each GHL send |
| Pre-qual letter, partner letter | `prequal_ghl.py`, `partner_ghl.py` | `emailFrom` + `emailBcc` on each GHL send |
| Instantly thread replies | `instantly_client.send_reply` | `eaccount` = David, `bcc_address_email_list` = Josh |
| Instantly cold email | `instantly_email.py` | Set on the campaign (`email_list`, `bcc_list`) by `--setup-campaign`. Each send reads the campaign back first and refuses if it differs. |

Deploy order:

1. Set `LENDING_EMAIL_FROM` and `LENDING_EMAIL_BCC` in the production `.env`.
2. Deploy, then restart `fa-lending-cora`, `fa-lending-reminder-worker`, `lending-api` and the lending crons' host.
3. Connect David's mailbox in NDL's Instantly workspace, then run
   `PYTHONPATH=. python -m src.lending.instantly_email --setup-campaign`.
4. Run `python -m src.lending.ghl_email --send-test <your inbox>`. Check that it arrives From David and that Josh
   received the hidden copy.

Not yet confirmed with a live send: GHL's `emailBcc` (public API reference) and Instantly's `bcc_list` /
`bcc_address_email_list`. The Instantly names were checked against its OpenAPI spec on 2026-11-17.

## GHL (by hand)

GHL workflow emails are not sent by our code. Change them in the Next Deal Lending sub-account:

1. Settings → Email Services: `david.willis@nextdeallending.com` is a verified sender on the domain.
2. In every lending workflow with a **Send Email** step (the first-touch "60 Second Loan Match" email, the
   14-day follow-up Emails 0 / 3 / 10, and any other), set:
   - From Name: David Willis
   - From Email: `david.willis@nextdeallending.com`
   - BCC: `josh@nextdeallending.com`
   - CC: empty
3. Save and **Publish** each workflow.
4. Put a test contact through each workflow and check the From and the BCC.

The email bodies are still signed "Next Deal Lending" (client Q18/Q56). Changing the signature is a separate wording
decision.
