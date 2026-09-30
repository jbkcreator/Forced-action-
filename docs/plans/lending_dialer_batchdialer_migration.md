# Lending dialer: migrate from Aircall to BatchDialer

The client's go-live brief names BatchDialer (Pro tier, three seats, account in the
client's name) as the dialer. The lending dialer load on this branch was built for
Aircall. This plan moves the dialer-facing parts to BatchDialer and leaves the
compliance filter, the Backflip conflict check and the gating logic unchanged.

Out of scope here: call and disposition intake (the `lending.call_dispositions`
work), call-time enforcement design (7:15pm stop, attempt cap wiring), new context
card fields, and queue-to-campaign mapping. The closer-cockpit Aircall integration
(`src/services/aircall_client.py` read functions and `/webhooks/aircall`) is a
separate product and stays.

## Phase 0 — prerequisites

| # | Item | Blocks |
|---|---|---|
| 0.1 | BatchDialer API docs (developer.batchservice.com fails TLS from the dev box; export from a browser) | Phase 2 payloads |
| 0.2 | `/api/dispositions` and `/api/recordings` return 403 "The API key not found" for our key | Call intake, transcripts |
| 0.3 | At least one BatchDialer campaign exists (account is empty) | Live load, smoke test |
| 0.4 | Agree `dialer_contact_id` as the neutral column name with the call-disposition owner, who reads it | Column rename |
| 0.5 | Whether `migrations/apply_lending_dialer_load_records.py` has run on prod | Rename as code edit vs rename migration |
| 0.6 | How a number is made non-dialable: remove from campaign, or DNC list add/delete | Removal, attempt-cap block |

Confirmed from the live API (read-only, 2026-09-30): base `https://app.batchdialer.com/api`,
`X-ApiKey` header auth; `GET /campaigns`, `/contacts`, `/lists`, `/cdrs` (paginated
`items/page/totalPages`) respond; `/campaign` and `/contact` exist (405 on GET);
`/contacts/updated` exists and expects a batch parameter.

## Phase 1 — dialer-neutral refactor (no behaviour change)

1. `src/lending/dialer_client.py`: `DialerClient` protocol, `ContactUpsertResult`,
   `DialerRequestError`, `DialerAmbiguousContact`.
2. `src/lending/dialer_load.py`: depends only on `DialerClient`; `aircall=` becomes `dialer=`.
3. `src/lending/dialer_contact.py`: provider-neutral display helpers; the Aircall field
   mapping moves to the adapter.
4. `src/lending/aircall_dialer.py`: temporary adapter wrapping the existing Aircall
   contact writes behind `DialerClient`.
5. `src/lending/dialer_removal.py`: neutral `ContactRemoval`.
6. Column rename `aircall_contact_id` -> `dialer_contact_id` — held until 0.4 and 0.5.
7. Compliance default dialer remover points at `src/lending/dialer_removal.py` directly.
8. Tests: names and imports only; behaviour unchanged.

## Phase 2 — BatchDialer client and adapter (needs 0.1, 0.3, 0.6)

- `batchdialer_api_key` and base URL in `config/settings.py` (`BATCHDIALER_API_KEY` is set in `.env`).
- `src/services/batchdialer_client.py`: paced requests, retry/backoff, no phones or key in logs.
  Endpoint wrappers written only from the docs.
- `src/lending/batchdialer_dialer.py`: `DialerClient` adapter mapping `DialerDisplay` to
  BatchDialer contact fields (custom fields if supported, otherwise notes).
- Mocked-HTTP tests.

## Phase 3 — switch over and clean up

- `src/tasks/lending_dialer_load.py` uses the BatchDialer adapter.
- Remove `src/lending/aircall_dialer.py`, the lending contact-write section of
  `aircall_client.py`, its Aircall limits and tests.
- Neutral wording in compliance and opt-out poller docstrings; update CLAUDE.md.

## Phase 4 — verification

1. Lending test files only (an unscoped `pytest tests/` posts real Slack messages).
2. Dry-run load on a small pool export — reads the shared prod DB and rolls back; confirm first.
3. Live smoke test with explicit approval: one internal test contact into a test campaign,
   verify in the BatchDialer UI, then remove it.

## BatchDialer campaigns and the go-live work

Contacts load into a campaign, so the pool mapping (`POOL_CAMPAIGN_TAGS`) becomes pool ->
BatchDialer campaign ID. The brief's three queues (Verified maturity, Transaction ready,
Builders) map naturally to three campaigns, with the nine source tags kept on each contact.
Campaign names and settings are a client decision; nothing is created in the live account
without confirmation.
