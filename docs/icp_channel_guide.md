# ICP Channel Management Guide

## 1. Core Concepts: ICP vs Vertical

**ICP (Ideal Customer Profile)** = the customer group. Examples: `contractor`, `rei_investor`, `hard_money_lender`, `property_manager`.

**Vertical** = the product category inside the lead product. Examples: `roofing`, `restoration`, `wholesalers`, `fix_flip`, `public_adjusters`, `attorneys`.

These are **completely separate axes**. A vertical like `wholesalers` can belong to BOTH the `contractor` ICP (as a contractor buying distressed-property leads) and a future `rei_investor` ICP (as a real estate investor buying the same leads). Verticals cannot be safely used to scope ICP subscribers, metrics, or campaigns.

**All ICP attribution, metrics, gating, and admin reporting use `icp_channel_key` explicitly.**

---

## 2. Default Contractor ICP

The `contractor` ICP is the platform's default:
- `is_default = True` — never gate-required, never blocked, never killable via the API
- All existing subscribers have `icp_channel_key = 'contractor'` (backfilled by fa066)
- `global_contractor_mrr()` in `contractor_mrr.py` sums MRR for `contractor` subscribers only
- The `VALID_VERTICALS` set is derived from `config/scoring.py:VERTICAL_WEIGHTS` — the contractor ICP's 6 verticals remain unchanged

**The contractor flow is fully backward-compatible.** All existing endpoints, scoring, Stripe, territory-lock, and pricing behavior is unchanged.

---

## 3. Adding a New ICP Channel

### Step 1: Register in `config/icp_channels.py`
```python
"hard_money_lender": {
    "display_name": "Hard Money Lender",
    "slug": "hard-money-lender",
    "status": "draft",           # always start as draft
    "is_default": False,
    "gate_required": True,
    "verticals": [],             # ICP-specific verticals, if any (do NOT add to scoring engine unless needed)
    "description": "...",
    "target_audience": "Hard money lenders seeking distressed property collateral",
    "lead_filter_summary": "Properties in pre-foreclosure or distress; high equity.",
}
```

### Step 2: Seed the DB row
```sql
INSERT INTO expansion_icp_channels
    (key, display_name, price_monthly, persona, feed_scope, landing_slug, status)
VALUES
    ('hard_money_lender', 'Hard Money Lender', 97.00,
     'Lenders seeking high-LTV distressed collateral', 'single_county',
     'hard-money-lender', 'gated');
```
Or use the PATCH admin API once the channel card appears in the UI.

### Step 3: Wait for gates to turn green (or force-activate with reason)

### Step 4: Activate via admin UI or API
```bash
curl -X POST /api/admin/icp-channels/hard_money_lender/activate \
  -H "Authorization: Bearer <admin-token>" \
  -d '{}' 
```

### DO NOT do these unless the scoring engine genuinely needs it:
- Add the new ICP as a CDS scoring vertical
- Hardcode the ICP's verticals in any business logic

---

## 4. Gate Criteria and Thresholds

Every non-contractor ICP must clear **8 gates** before activation:

**7 platform Expansion Gates** (from `config/lifecycle_guardrails.py:EXPANSION_GATES`):
| Gate | Threshold |
|------|-----------|
| first_payment_rate | ≥ 30% |
| saved_card_rate | ≥ 70% |
| wallet_adoption | ≥ 15% |
| lock_conversion | ≥ 5% |
| payer_retention_30d | ≥ 70% |
| free_tier_cost_ratio | ≤ 40% |
| county_profitability | net positive |

**1 ICP-specific gate:**
| Gate | Threshold |
|------|-----------|
| contractor_mrr_usd | ≥ $50,000 global contractor MRR |

If any metric is missing (no data yet), the gate shows **"N/A / Insufficient data"** — it does not block but does not count as green. All gates must be explicitly green for normal activation.

**Green/Yellow/Red scoring:**
- Derived from raw counts in `icp_daily_stats` at read time (not stored as percentages)
- Auditable: raw counts are always stored; rates are always computable
- `color="unknown"` means no data — shown as N/A in UI, never raises an error

---

## 5. Admin Workflow

```
draft → (seed DB row) → gated → (all gates green OR force) → live → paused → live
                                                                    ↘ killed (terminal)
```

**State transitions:**
- `draft` → DB row not yet seeded (config-only)
- `gated` → in DB, awaiting gate clearance
- `approved` → manually approved by admin (intermediate, optional)
- `live` → active; subscribers can sign up
- `paused` → temporarily suspended; returns to `gated`
- `retired` → terminal; cannot be reactivated

**Force activation** (for testing or special cases):
- Requires non-empty `reason` in request body
- Always writes an audit row with `is_force_activate=true`, `force_reason`, `actor`, `gate_snapshot`
- Cannot bypass contractor-ICP checks (contractor is always active)

---

## 6. Kill-Switch Scoring

The 4-week kill-switch panel is shown for each active ICP channel in the admin UI:

1. `icp_metrics_ingest` runs daily at 07:15 UTC and writes raw counts to `icp_daily_stats`
2. `icp_kill_switch.compute_icp_gate_snapshot()` derives rates from raw counts + reads Redis cache
3. Admin UI shows per-gate: value | threshold | 🟢🟡🔴 | N/A
4. "N/A" = missing data; never fails or defaults to red

Audit trail: every status change (including force activations) is logged to `icp_channel_launch_audit` with `gate_snapshot JSONB` so historical gate state is always recoverable.

---

## 7. Backend API Reference

All endpoints require admin JWT.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/admin/icp-channels` | List all channels |
| GET | `/api/admin/icp-channels/{key}` | Detail + gate + audit |
| POST | `/api/admin/icp-channels/{key}/activate` | Activate (or `?force=true`) |
| POST | `/api/admin/icp-channels/{key}/pause` | Pause active channel |
| POST | `/api/admin/icp-channels/{key}/kill` | Kill (terminal) |
| PATCH | `/api/admin/icp-channels/{key}` | Update config fields |
| GET | `/api/admin/icp-channels/{key}/metrics` | 30-day metric trend |
| GET | `/api/admin/icp-channels/{key}/subscribers` | Channel subscribers |

---

## 8. Frontend Admin UI

Route: `/admin/icp`

- Channel cards showing status, gate health, and action buttons
- Gate health grid: metric | value | threshold | color chip (N/A if no data)
- 30-day metric table: raw counts + derived rates per day
- Force-activate flow: shows reason textarea when gates not all green
- Kill flow: confirm with optional reason

---

## 9. Phase 2 Items (not implemented in Phase 1)

These are intentionally deferred:

1. **Public ICP landing pages** (`/rei-investors`) — Phase 1 is admin-only management
2. **Per-ICP Stripe prices** — Phase 1 uses existing tier pricing
3. **Lifecycle message sequences per ICP** — Phase 1 channels use shared sequences with vertical filtering
4. **Synthflow per-ICP agents** — separate taxonomy, not changed here
5. **Per-ICP signup flow** — Phase 1 subscribers attribute to an ICP at signup but the public flow is still vertical-based

---

## 10. Known Limitations

- `payer_retention_30d` requires a 30-day lookback that is not yet implemented in `icp_metrics_ingest.py` — shows N/A until added
- `icp_daily_stats` is backfilled forward from the migration date; historical data is not available
- The `WaitlistEntry.vertical` CHECK constraint was dropped in fa066 to allow new verticals — the app layer validates via `VALID_VERTICALS`
