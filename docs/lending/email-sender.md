# LendingFlow email sender: From David, BCC Josh

Every email to a **LendingFlow lead**, including the first touch, is sent **From
`david.willis@nextdeallending.com`** with **`josh@nextdeallending.com` in BCC**. There is never a CC. Other lending
email (website leads, partners, cold Instantly email) is unchanged.

## Server (code)

| Setting | Value |
|---|---|
| `LENDING_LENDINGFLOW_EMAIL_FROM` | `david.willis@nextdeallending.com` |
| `LENDING_LENDINGFLOW_EMAIL_BCC` | `josh@nextdeallending.com` (comma-separated if more are ever added) |

`src/lending/email_identity.py` reads both. `is_lendingflow_recipient` checks whether an address or phone belongs to a
row in `lending.lendingflow_leads`.

| Email | Code | Who gets David + BCC Josh |
|---|---|---|
| Cora-approved emails, document chase, booking reminders, gate-fail message | `ghl_email.py` | Recipient is a LendingFlow lead (by email or phone). Others keep `LENDING_GHL_EMAIL_FROM` and no BCC. |
| Pre-qual letter | `prequal_ghl.py` | Always: pre-qual letters are only queued for LendingFlow leads |
| Partner letter, Instantly cold email and replies | unchanged | Nobody: these are not LendingFlow email |

A LendingFlow email is **not sent** while either setting is unset or invalid, or while the lead check cannot be read.
An ERROR is logged and nothing reaches GHL. A pre-qual letter keeps its attempts and goes out once the settings are
fixed.

Deploy order:

1. Set `LENDING_LENDINGFLOW_EMAIL_FROM` and `LENDING_LENDINGFLOW_EMAIL_BCC` in the production `.env`.
2. Deploy, then restart `fa-lending-cora`, `fa-lending-reminder-worker` and `lending-api`.
3. Check once with a real LendingFlow test lead: the email arrives From David and Josh gets the hidden copy. GHL's
   `emailBcc` follows its public API reference and has not yet been confirmed with a live send.

## GHL (by hand)

LendingFlow's first-touch and follow-up emails are GHL workflow steps, not our code. Change them in the Next Deal
Lending sub-account:

1. Settings → Email Services: the nextdeallending.com sending domain is verified.
2. In every LendingFlow workflow with a **Send Email** step (the first-touch "60 Second Loan Match" email in
   "NDL - LendingFlow Speed to Lead", "NDL - Josh LendingFlow 30 Day Cadence", "NDL – 14-Day Follow-Up" and any
   other), set:
   - From Name: David Willis, Next Deal Lending
   - From Email: `david.willis@nextdeallending.com`
   - BCC: `josh@nextdeallending.com`
   - CC: empty
3. Save each action, save the workflow, and check it is still **Published**.
4. Put a test contact through each workflow and check the From and the BCC.

Internal alerts to Josh (for example "Email alert to Josh") are not borrower email; leave them as they are.
