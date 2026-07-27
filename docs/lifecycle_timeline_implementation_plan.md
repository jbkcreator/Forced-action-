# Lifecycle Touch Timeline — Implementation Plan

Read-only UI surface that aggregates every Lifecycle interaction for a single
subscriber. Backend in `Forced-action-`, frontend in `Forced-action-ui`.

## Glossary (already added to `CONTEXT.md`)

- **Lifecycle touch** = one `agent_decisions` row. Composed SMS/voice/chat replies
  hang off the touch as child artifacts, not as separate touches.
- Pre-signup chat (no `agent_decisions` row) is **not** a Lifecycle touch and is
  out of scope for v1.

## Resolved decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Touch unit | one `agent_decisions` row |
| 2 | Scope | `subscriber_id IS NOT NULL` only |
| 3 | Audience | internal ops/admin (JWT) |
| 4 | Backend mount | `src/api/lifecycle_incidents_router.py` |
| 5 | Pagination | cursor `(started_at DESC, decision_id)`, page 50 |
| 6 | Artifacts | `summary` JSONB + joined `sms_send_logs`. No `message_outcomes` join (no FK; deferred). |
| 7 | Frontend mount | `Forced-action-ui` (React 19 / Vite), `src/api/phase2b.js` |
| 8 | Route | `/admin/lifecycle-timeline/:subscriberId` (deep-link) |
| 9 | Row grouping | one row per decision, day-header dividers |
| 10 | Index | composite `(subscriber_id, started_at DESC)` via `CREATE INDEX CONCURRENTLY` |

## Schema gap (flagged, deferred)

`message_outcomes` has no `decision_id` FK to `agent_decisions`. v1 ships
without delivery/open/click/conversion enrichment on the timeline.
Follow-up: add `message_outcomes.decision_id` + backfill, then enrich the
timeline serializer.

---

## Backend (`Forced-action-`)

### 1. Migration — new index on `agent_decisions`

File: `alembic/versions/<rev>_add_agent_decisions_subscriber_started_idx.py`

```python
revision = "<rev>"
down_revision = "<current-head>"
branch_labels = None
depends_on = None

def upgrade():
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "idx_agent_decisions_subscriber_started "
        "ON agent_decisions (subscriber_id, started_at DESC)"
    )

def downgrade():
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS "
        "idx_agent_decisions_subscriber_started"
    )
```

- Mirror the index on `AgentDecision.__table_args__` in `src/core/models.py`
  so autogenerate doesn't try to recreate it.
- Migration must run **outside a transaction** for `CONCURRENTLY`:
  set `transactional_ddl = False` in the migration (or use Alembic's
  `op.execute` inside an autocommit block) — pattern matches existing
  `b1c2d3e4f5a6_add_trgm_indexes_for_matching.py`.
- Per saved preference: write the migration file, apply DDL via a
  one-off Python script (Alembic CLI unusable on this repo's multi-head tree).

### 2. New endpoint in `lifecycle_incidents_router.py`

```
GET /api/admin/lifecycle/subscribers/{subscriber_id}/timeline
    ?cursor=<base64>     -- (started_at_iso, decision_id) tuple
    &limit=50            -- default 50, max 200
    &graph_name=         -- optional filter (fomo|abandonment|retention)
    &status=             -- optional filter on terminal_status
    &since=<iso8601>     -- optional lower bound on started_at
```

Auth: existing admin JWT dependency used by sibling routes in the same file.

Query plan:

1. Validate `subscriber_id` exists (404 if not). Cheap `SELECT 1` on
   `subscribers`.
2. Build base query:
   ```python
   q = (
       session.query(AgentDecision)
       .filter(AgentDecision.subscriber_id == subscriber_id)
       .order_by(AgentDecision.started_at.desc(), AgentDecision.decision_id.desc())
       .limit(limit + 1)   # +1 to detect next_cursor
   )
   ```
3. Apply optional filters (`graph_name`, `status`, `since`).
4. Apply cursor as `(started_at, decision_id) < (cursor_started_at, cursor_decision_id)`
   tuple comparison.
5. Pull child `sms_send_logs` in one follow-up query:
   ```python
   session.query(SmsSendLog)
       .filter(SmsSendLog.decision_id.in_([d.decision_id for d in rows]))
       .order_by(SmsSendLog.created_at.asc())
   ```
   Group in Python by `decision_id`.

Response shape:

```json
{
  "subscriber_id": 123,
  "items": [
    {
      "decision_id": "…",
      "graph_name": "fomo",
      "event_type": "competitor_acted_on_lead",
      "started_at": "2026-05-27T18:42:11Z",
      "completed_at": "2026-05-27T18:42:14Z",
      "terminal_status": "completed",
      "autonomy_class": "autonomous",
      "was_autonomous": true,
      "variant_id": "fomo_urgency_a",
      "cost_usd": 0.0123,
      "tokens_used": 412,
      "override": null,
      "summary": { … },
      "sms_sends": [
        {
          "id": 998,
          "outcome": "sent",
          "message_type": "marketing",
          "vendor": "telnyx",
          "vendor_message_id": "…",
          "body_preview": "…",
          "created_at": "…"
        }
      ]
    }
  ],
  "next_cursor": "<base64>|null"
}
```

Cursor encoding: `base64(f"{started_at_iso}|{decision_id}")`. Reject malformed
cursors with 400.

### 3. Tests

- Unit: cursor encode/decode round-trip, filter combinations.
- Integration (`tests/`): seed 3 subscribers × 5 decisions each with
  `sms_send_logs`; assert correct subscriber isolation, ordering,
  pagination boundary, filter narrowing, 404 on missing subscriber.
- No new pytest marker — runs under default unit suite.

---

## Frontend (`Forced-action-ui`)

### 4. API client — `src/api/phase2b.js`

Add:

```js
export async function fetchLifecycleTimeline(token, subscriberId, opts = {}) {
  const params = new URLSearchParams();
  if (opts.cursor) params.set('cursor', opts.cursor);
  if (opts.limit) params.set('limit', opts.limit);
  if (opts.graphName) params.set('graph_name', opts.graphName);
  if (opts.status) params.set('status', opts.status);
  if (opts.since) params.set('since', opts.since);
  return api.get(
    `/api/admin/lifecycle/subscribers/${subscriberId}/timeline?${params}`,
    { token }
  );
}
```

### 5. Route & page

- Register `/admin/lifecycle-timeline/:subscriberId` as a child of `/admin` in
  `src/App.jsx` (uses existing `<Outlet />` shell).
- New page: `src/pages/admin/LifecycleTimelinePage.jsx`.
- Sidebar entry in `AdminSidebar`: "Lifecycle Timeline" → prompts for subscriber
  ID, navigates to `/admin/lifecycle-timeline/<id>` on submit.

### 6. Page structure (`LifecycleTimelinePage.jsx`)

- Header: subscriber summary line (id, email if cheap to fetch — out of
  scope otherwise; v1 can show just `Subscriber #123`).
- Filter bar: graph_name select, status select, `since` date input. Updates
  URL search params (`useSearchParams`).
- Timeline body: vertical list grouped under day headers
  (`Today`, `Yesterday`, `Mon May 26`, …). Group key is calendar day in
  America/New_York (county TZ).
- Each row component `<LifecycleTouchRow />`:
  - Collapsed: `HH:mm` · graph badge · event_type · status pill · autonomy
    pill · 1-line summary headline (from `summary.headline` if present,
    else first `sms_sends[].body_preview`).
  - Expanded (click to toggle): full `summary` JSONB rendered as
    `<dl>`-style key/value; `sms_sends[]` table with outcome + body preview
    + vendor_message_id; meta footer (cost, tokens, variant, override info).
- Infinite scroll: bottom sentinel triggers `fetchLifecycleTimeline` with
  `cursor` from previous response. Stop when `next_cursor === null`.
- Loading: shimmer rows. Empty state: "No Lifecycle touches for this subscriber."

### 7. Theme & a11y

- All colors from `--fa-*` Tailwind tokens — no hex.
- Day headers as `<h2>`, rows as `<article>`. Expand toggle is a `<button>`
  with `aria-expanded`.
- Keyboard: `↑/↓` between rows, `Enter`/`Space` to expand.

### 8. CLAUDE.md updates (frontend)

- Add `/admin/lifecycle-timeline/:subscriberId` to the Routes table.
- No new dep, no new env var, no new top-level dir.

---

## Out of scope for v1

- `message_outcomes` join (delivery/open/click/conversion enrichment).
- Concierge Chat turn timeline integration (pre-signup chat has no decision).
- Subscriber-facing variant.
- Export / CSV.
- Subscriber search/list UI (entry is by ID only).

## Order of work

1. Migration file + apply via python script.
2. Update `AgentDecision.__table_args__` with the new index.
3. Endpoint + unit + integration tests.
4. Frontend API client function.
5. Route + page + sidebar entry.
6. Manual smoke against a live subscriber with real `agent_decisions` rows.
7. Frontend CLAUDE.md route table update.
