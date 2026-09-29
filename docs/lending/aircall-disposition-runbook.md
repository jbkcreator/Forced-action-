# Aircall call disposition logging — setup runbook

Callers finish a call by adding **one result tag** in Aircall. Aircall sends the
event to `POST /webhooks/aircall/disposition`; each call is saved as one row in
`lending.call_dispositions`, then posted to Slack `#dial-tasks` and written to the
Google Sheet.

## 1. Aircall

1. Create five tags with **exactly** these names: `CONNECTED`, `LEFT_VOICEMAIL`,
   `BAD_NUMBER`, `DNC_REQUEST`, `QUALIFIED_APPOINTMENT`. The tag name is the disposition.
2. If the plan allows it, make tagging mandatory after each call.
3. Note the **number ID** of every lending line. Set `LENDING_AIRCALL_LINE_IDS`
   (comma-separated). Events from any other line are ignored; an empty setting ignores everything.
4. Recording stays **off** on the lending lines.
5. Register a webhook: URL `https://<domain>/webhooks/aircall/disposition`, events
   `call.ended`, `call.tagged`, `call.untagged`. Put its token in `LENDING_AIRCALL_WEBHOOK_TOKEN`.
   The endpoint accepts either a valid `X-Aircall-Signature` HMAC or the token in the body;
   after the first live event, keep whichever Aircall actually sends.

The Aircall account also feeds the Closer Cockpit webhook (`/webhooks/aircall`); both receive every call.

## 2. Database

Run once each, in this order (the role script needs a Postgres superuser):

```bash
PYTHONPATH=. python migrations/apply_lending_compliance.py
PYTHONPATH=. python migrations/apply_lending_call_dispositions.py
PYTHONPATH=. python migrations/apply_lending_app_role.py --admin-url postgresql://<superuser>@<host>/<db>
```

Then set `LENDING_DATABASE_URL` (the `lending_app` login) and `LENDING_DB_PASSWORD` in `.env`.

## 3. Slack and Google Sheet

- Slack: a lending-only app with the `chat:write` scope, invited to `#dial-tasks`.
  Set `LENDING_SLACK_BOT_TOKEN` and `LENDING_DIAL_TASKS_CHANNEL` (channel ID).
- Sheet: create it in the Workspace, share it with the service account as Editor, and do
  **not** share it with caller accounts (it holds caller pay). Set
  `LENDING_SHEETS_SERVICE_ACCOUNT_KEY_PATH`, `LENDING_DISPOSITION_SHEET_ID`,
  `LENDING_DISPOSITION_SHEET_TAB`. The header row is written on first use.
- A missing Slack or Sheet setting skips that sink; the call is still saved and retried later.

## 4. Service and Nginx

```bash
sudo cp deploy/systemd/lending-api.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now lending-api
curl -s http://127.0.0.1:8010/health
```

Check the port is free first (`ss -ltnp | grep 8010`). Add to the Nginx server block (more
specific than `/webhooks/`, so it wins):

```nginx
location /webhooks/aircall/disposition {
    proxy_pass http://127.0.0.1:8010;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
```

Install the crontab so the delivery retry runs every 5 minutes.

## 5. Acceptance check (spec §12)

1. Make a test call on a lending line and tag it.
2. Within 30 seconds of tagging: the row exists, the Sheet has a row for the call ID, and
   `#dial-tasks` has the post. The journal logs the delivery time for each
   (`journalctl -u lending-api | grep delivered`).
3. Re-send the same event: still one row. Change the tag: the Sheet row and Slack message update.
4. Tag a call `DNC_REQUEST`: the contact is blocked on every channel.

## Behaviour to know

- A call with two result tags counts the latest and posts a supervisor warning.
- Removing the only result tag clears it in the database, blanks the Sheet row's disposition and edits the Slack message to "Result removed".
- A `DNC_REQUEST` on a call whose number can't be normalized cannot be propagated automatically; `#dial-tasks` gets a "DNC request NOT propagated" alert naming the call ID and caller seat, and the contact must be added to the suppression list by hand. The alert can repeat if Aircall redelivers the event.
- A real failure (database down, compliance hook error) returns 5xx so Aircall redelivers; redelivery is safe.
