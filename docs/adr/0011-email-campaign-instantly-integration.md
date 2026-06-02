---
status: accepted
---

# Email Campaign acquisition via Instantly.ai — DB-mediated, M:N concurrent membership

## Context

We are adding automated email outreach whose **goal is converting DBPR-licensed
contractors into paying Forced Action subscribers** (success metric = signups,
not open/reply rate). Contractor contacts live in `dbpr_contacts`.

**Enrichment status (verified against code, June 2026):**
- `dbpr_contacts` enrichment is the `dbpr_enrichment.py` BatchData stage. Its
  IDI fallback is **dead** — `idi_api_key` has been removed from `settings.py`,
  and the property-owner waterfall (`skip_trace_waterfall.py`) now runs
  **BatchData → Whitepages → PDL** (the "→ IDI →" docstrings are stale). Do not
  rely on IDI.
- The DBPR enrichment job is **not currently scheduled** in cron (only the
  weekly DBPR file sync + DBA scrape are). Standing up a scheduled email-send-
  ready enrichment for `dbpr_contacts` is a prerequisite this feature assumes.
- **Clay already exists**, but for the white-label product
  (`clay_service.py` → `white_label_contractor_enrichments`), not the DBPR
  email path. A `CLAY_DBPR_WEBHOOK_URL` setting is defined but **has no
  consumer yet** — the Clay→`dbpr_contacts` write is configured-but-unbuilt.
  This feature assumes that path lands Clay-verified emails into
  `dbpr_contacts` (precedence: Clay-verified > BatchData > raw).

Email delivery, warmup, and sequencing are handled by **Instantly.ai**
(Growth plan).

The platform never lets Clay or Instantly talk to each other directly: Clay
writes enrichment into our DB; our platform reads the DB and pushes eligible
contractors into Instantly via API; Instantly results are pulled back daily.

## Decision

1. **DB is the single source of truth.** Clay enrichment lands in
   `dbpr_contacts`; campaigns read eligibility from the DB. Instantly is a
   delivery engine, not a system of record.
2. **Contractor ↔ Campaign is many-to-many and concurrent**, modeled by a
   `campaign_contacts` junction. A contractor may be active in multiple
   campaigns simultaneously. Only constraint: `UNIQUE(campaign_id,
   dbpr_contact_id)` (no duplicate inside one campaign). **No** partial-unique
   guard on the contractor.
3. **Global suppression is contact-level and absolute.** `is_opted_out`,
   `is_hard_bounced`, and `is_signed_up` (conversion) on `dbpr_contacts` exclude
   a contractor from *every* campaign, present and future, and survive the
   weekly DBPR file replace.
4. **FA Campaign ↔ Instantly Campaign = 1:1.** The same contractor becomes a
   distinct Instantly lead per campaign (`instantly_lead_id` per membership).
5. **Sequences are authored via API from FA-side reusable templates.** The
   platform *creates* the Instantly campaign + sequence on campaign creation
   (§4.2's "select a pre-built Instantly campaign" is superseded). Personalization
   variables are validated at author time against a whitelist of real
   `dbpr_contacts`-backed fields; invalid variables block creation.
6. **Conversion attribution via signed token.** The email CTA carries a signed
   token encoding `campaign_contact_id` (not just the contact), threaded through
   signup/Stripe checkout so the converting campaign is attributed
   deterministically (last-clicked wins). Email-match is fallback only.
7. **Analytics are daily snapshots.** A daily pull writes one
   `campaign_daily_analytics` row per campaign per day (enables "last 30 days"
   without live calls); per-contact engagement status is last-write-wins on
   `campaign_contacts`.
8. **All Instantly I/O is centralized** in `src/services/instantly_service.py`
   with throttle + 429 backoff (mirrors `ghl_webhook._ghl_request`). Instantly's
   documented limits are **100 req/sec and 6000 req/min per workspace** (429 on
   exceed); the service stays well under and backs off. v2 + Bearer token only.
9. **Backend ships before frontend.**

## Scope deferred (v1 explicit no-s)

- Inbox **OAuth** connection (admin connects inboxes in Instantly UI; FA reads
  the list only).
- Warmup **write** controls (read-only health view in v1).
- Warmup-ramp config and DNS (SPF/DKIM/DMARC) — runbook documentation only.
- ZIP-**range** filter (county + explicit ZIP list only in v1).
- Real-time Instantly webhooks (daily pull only); a webhook endpoint is a
  future optimization.
- Per-event engagement history (last-write-wins suffices).
- `EmailOptIn` consent gate (B2B cold email is CAN-SPAM-lawful; footer must
  carry physical address + one-click unsubscribe).

## APIs

### A. Instantly.ai v2 endpoints we consume (via `instantly_service.py`)

**Validated against developer.instantly.ai (base `https://api.instantly.ai`),
June 2026.** Bearer-token auth. Paths corrected from the design doc's
assumptions — note the ones marked ⚠ that differ from a naive REST guess.

| Purpose | Method / Path | Notes |
| --- | --- | --- |
| Create campaign | `POST /api/v2/campaigns` | Sequence **and** schedule go in the body: `name`, `campaign_schedule` (required — `schedules[]` with `timing`/`days`/`timezone`, plus `start_date`/`end_date`), `sequences` (array, **only first element used** — holds the steps). |
| List campaigns | `GET /api/v2/campaigns` | filter by status enum (`ACTIVE`/`PAUSED`) |
| Get campaign | `GET /api/v2/campaigns/{id}` | |
| Update campaign / sequence | `PATCH /api/v2/campaigns/{id}` | |
| Delete campaign | `DELETE /api/v2/campaigns/{id}` | |
| Activate / resume | `POST /api/v2/campaigns/{id}/activate` | ✓ confirmed |
| Pause | `POST /api/v2/campaigns/{id}/pause` | ✓ confirmed |
| Duplicate campaign | `POST /api/v2/campaigns/{id}/duplicate` | ✓ confirmed |
| **Add contacts (bulk ≤1000)** | ⚠ `POST /api/v2/leads/add` | **NOT** `/campaigns/{id}/leads`. `campaign_id` in body; server-side validates emails, checks blocklist, **skips leads already in the campaign** (secondary idempotency). |
| **Per-contact status (paged)** | ⚠ `POST /api/v2/leads/list` | **POST, not GET** (complex filters). Filter by `campaign_id`; `interest_status` carries Interested/Not-Interested; cursor pagination via `next_starting_after`. |
| Remove a contact | `DELETE /api/v2/leads/{id}` (bulk: `DELETE /api/v2/leads`) | |
| Move leads to a campaign | `POST /api/v2/leads/move` | |
| **Campaign analytics** | ⚠ `GET /api/v2/campaigns/analytics` | **NOT** `/campaigns/{id}/analytics`. Query: `id`/`ids`, `start_date`, `end_date`, `exclude_total_leads_count`. Fields: `open_count(_unique)`, `reply_count(_unique)`, `link_click_count`, … |
| Daily campaign analytics | `GET /api/v2/campaigns/analytics/daily` | for snapshot/trend |
| Analytics overview | `GET /api/v2/campaigns/analytics/overview` | dashboard widget |
| List sending inboxes | `GET /api/v2/accounts` | ✓ |
| **Warmup health** | ⚠ `POST /api/v2/accounts/warmup-analytics` | **POST, account-level** (takes a list of inbox emails) — NOT `GET /accounts/{id}/warmup-analytics`. |
| Warmup enable/disable (deferred) | `POST /api/v2/accounts/warmup/enable` \| `/disable` | v1 = read-only, not called |

Sources: [Create campaign](https://developer.instantly.ai/api/v2/campaign/createcampaign),
[Add leads in bulk](https://developer.instantly.ai/api/v2/lead/bulkaddleads),
[List leads](https://developer.instantly.ai/api/v2/lead/listleads),
[Campaign analytics](https://developer.instantly.ai/api/v2/analytics/getcampaignanalytics),
[Get warmup analytics](https://developer.instantly.ai/api/v2/account/getwarmupanalytics),
[Accounts](https://developer.instantly.ai/api/v2/account).

### B. New FA backend endpoints to create (all under existing admin JWT)

**Templates**
- `GET    /api/admin/email-templates` — list
- `POST   /api/admin/email-templates` — create (validates variables)
- `GET    /api/admin/email-templates/{id}` — detail
- `PUT    /api/admin/email-templates/{id}` — update (validates variables)
- `DELETE /api/admin/email-templates/{id}` — delete (block if referenced by a campaign)
- `GET    /api/admin/email-templates/variables` — allowed personalization variable whitelist

**Campaigns**
- `GET    /api/admin/email-campaigns` — list (filters: status, date range)
- `POST   /api/admin/email-campaigns` — create → builds + activates Instantly campaign
- `GET    /api/admin/email-campaigns/{id}` — detail (config + analytics cards)
- `GET    /api/admin/email-campaigns/eligible-count` — live eligible count for filters (county + ZIP list + vertical)
- `POST   /api/admin/email-campaigns/{id}/pause`
- `POST   /api/admin/email-campaigns/{id}/resume`
- `POST   /api/admin/email-campaigns/{id}/duplicate` — clones template+filters+cap+dates → fresh draft Instantly campaign
- `POST   /api/admin/email-campaigns/{id}/add-contacts` — manual top-up (one campaign)
- `GET    /api/admin/email-campaigns/{id}/contacts` — membership list (filter by status, search name/email)
- `GET    /api/admin/email-campaigns/{id}/contacts/export` — CSV
- `GET    /api/admin/email-campaigns/summary` — dashboard widget (last-30d aggregates from snapshots)

**Inboxes / warmup (read-only)**
- `GET    /api/admin/email-inboxes` — inbox list + health score + <70 warning flag

**Conversion attribution (public path, not admin)**
- Signed token decode + threading into the existing signup/Stripe-checkout flow;
  `stripe_webhooks` sets `subscriber_id`, `signed_up_at`, `is_signed_up`,
  `acquisition_source='dbpr_email'`, and stamps `campaign_contacts.converted_at`
  on the attributed membership. (No new public route if the token rides the
  existing dashboard signup URL + checkout metadata; otherwise a thin
  `GET /go/c/{token}` redirector.)

### C. New scheduled tasks (cron)

- `src/tasks/email_campaign_topup.py` — daily, **after** DBPR/Clay enrichment;
  per-Active-campaign eligibility top-up, batches of 1000, idempotent via
  `campaign_contacts`.
- `src/tasks/email_campaign_sync.py` — daily, separate slot; pulls campaign
  analytics (→ snapshot rows) + per-contact status (→ engagement + global
  suppression mapping), then runs lifecycle check (auto-complete / end-date
  auto-pause). Retries + `send_alert` on failure.

## Data model

New tables: `email_sequence_templates`, `email_campaigns`, `campaign_contacts`,
`campaign_daily_analytics`. `dbpr_contacts` gains `email_source`,
`email_verified`, `email_verified_at`, `clay_enriched_at`, and global-suppression
booleans `is_opted_out` / `is_hard_bounced` / `is_signed_up` (replacing the
per-campaign meaning of `email_status`). See the implementation plan for column
detail. Migration written as `fa062_email_campaigns.py` but **applied via a
Python DDL script** (alembic CLI unusable — multi-head tree).

## Consequences

- Concurrent unbounded membership means a "hot" contractor can receive several
  parallel sequences from our domains — a deliverability/spam risk we accept in
  v1 (no max-concurrent guard). Revisit if complaint rates rise.
- Email-source precedence is **Clay-verified > BatchData > raw**; BatchData is
  retained as fallback. **IDI is not used** (deprecated in code).
- Retiring `email_status`'s per-campaign value is a one-way data migration;
  global flags are derived from existing rows during the migration.
