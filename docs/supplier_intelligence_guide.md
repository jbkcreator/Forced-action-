# Supplier Intelligence Foundation Guide

## Status: Phase 1 Foundation Only

This is an **explicitly Phase 1** implementation. Advanced analytics sections (deal benchmarks, recommendations) require substantial deal outcome data that does not yet exist. Those sections return "Insufficient data" or "Phase 2" placeholders — **no fake intelligence is shown**.

---

## 1. Product Scope

Supplier Intelligence is a separate subscription product ($497–$1,497/mo) providing market intelligence reports to suppliers (contractors, vendors, property professionals) covering their territories.

**Phase 1 (this implementation):**
- Admin-provisioned accounts (no self-signup)
- Subscription management via Stripe
- On-demand and monthly scheduled report generation
- PDF and CSV export
- Supplier-facing dashboard (`/supplier/:accessToken`)
- Honest N/A sections where data is insufficient

**Phase 2 (future, not implemented):**
- AI-driven recommendations
- Self-signup flow
- Per-ICP pricing
- Real-time data feeds
- API access tier

---

## 2. ICP vs Supplier Intelligence

**Supplier Intelligence is NOT an ICP channel.** These are different concepts:
- **ICP channel** = who buys the lead product (contractor, rei_investor, etc.)
- **Supplier Intelligence** = a separate analytics subscription product sold TO suppliers

Do not mix these.

---

## 3. Data Readiness Rules

All gated sections check `config/supplier_intel_config.DATA_READINESS_THRESHOLDS`:

| Threshold | Default | Controls |
|---|---|---|
| `min_deals_for_benchmarks` | 50 | Closed-deal benchmarks |
| `min_subs_for_trend` | 5 | Contractor demand trend |
| `min_days_of_data` | 30 | Minimum data window |
| `min_leads_for_zip_map` | 10 | Per-ZIP appearance in map |

When a section is below threshold: `{"status": "insufficient_data", "current_count": N, "minimum_required": M, "message": "..."}` is returned instead of any data.

**Never change these thresholds to fake data availability.** These are correctness guards, not style choices.

---

## 4. Report Sections

| Section | Availability | Data Source |
|---|---|---|
| Market Activity | Always | `distress_scores`, property signals |
| Top ZIPs | Always | `distress_scores` grouped by ZIP |
| Signal Movement | Always | `foreclosures`, `tax_delinquencies`, etc. |
| Property Tier Distribution | Always | `distress_scores.lead_tier` |
| Trade Coverage | Always | `subscribers`, `sent_leads` |
| Closed-Deal Benchmarks | Gated (50+ deals) | `deal_outcomes` |
| Contractor Demand | Gated (5+ subs) | `subscribers`, `sent_leads` |
| Recommendations | Phase 2 — always N/A | — |

---

## 5. Backend API Reference

### Admin Endpoints (JWT required)
```
GET    /api/admin/supplier-intel/accounts                            List accounts
POST   /api/admin/supplier-intel/accounts                            Create account
GET    /api/admin/supplier-intel/accounts/{id}                       Detail + subscription + reports
PATCH  /api/admin/supplier-intel/accounts/{id}                       Update counties/verticals/status
POST   /api/admin/supplier-intel/accounts/{id}/reports/generate      Trigger report generation
GET    /api/admin/supplier-intel/accounts/{id}/reports/{rid}/export  Download PDF/CSV
```

### Supplier-Facing Endpoints (X-Access-Token header)
```
GET    /api/supplier/status            Account + subscription status
GET    /api/supplier/reports/latest    Most recent generated report
GET    /api/supplier/reports           Report history
GET    /api/supplier/reports/{id}/export?format=pdf|csv   Download export
```

### Stripe Webhook
No separate webhook secret. Supplier Intelligence events are identified by `metadata.product == "supplier_intel"` in the shared `/webhooks/stripe` handler (same pattern as Bankruptcy Filing Alerts). `resolve_handler()` in `src/services/supplier_intel/subscription.py` claims these events before the property-subscriber path.

---

## 6. Adding a New Supplier Account (Admin)

1. POST `/api/admin/supplier-intel/accounts` with:
   ```json
   {
     "company_name": "Acme Roofing Co",
     "contact_email": "owner@acme.com",
     "contact_name": "Jane Smith",
     "counties": ["hillsborough"],
     "verticals": ["roofing", "restoration"]
   }
   ```
2. Response includes `access_token` (UUID) — share with supplier for dashboard access
3. Dashboard URL: `https://app.forcedaction.io/supplier/{access_token}`
4. Create Stripe checkout to activate subscription (or set up manually in Stripe dashboard)

---

## 7. Stripe Setup

Three Stripe products/prices are registered via `scripts/seed_stripe.py`:

| Tier | Price | Stripe Key |
|---|---|---|
| Foundation | $497/mo | `fa_supplier_intel_foundation` |
| Standard | $997/mo | `fa_supplier_intel_standard` |
| Premium | $1,497/mo | `fa_supplier_intel_premium` |

Run `python scripts/seed_stripe.py` to create them in test mode.
Set these env vars after seeding:
- `STRIPE_TEST_PRICE_SUPPLIER_INTEL_FOUNDATION`
- `STRIPE_TEST_PRICE_SUPPLIER_INTEL_STANDARD`
- `STRIPE_TEST_PRICE_SUPPLIER_INTEL_PREMIUM`
- (live equivalents for production)

---

## 8. Report Generation & Scheduling

**Manual:** POST `/api/admin/supplier-intel/accounts/{id}/reports/generate` with `{"county_id": "hillsborough"}`

**Monthly cron:** `0 8 1 * *` (`src.tasks.supplier_report_monthly`)
- Runs 1st of every month at 08:00 UTC
- Generates report → renders PDF → emails to contact_email
- Idempotent: skips accounts already reported this month

---

## 9. Frontend

**Admin UI:** `/admin/supplier-intel` — account list, create form, report generation, export links
**Supplier dashboard:** `/supplier/:accessToken` — status banner, section cards, export buttons

The dashboard clearly marks:
- ✓ Available sections in green
- ⚠ "Insufficient data" sections in yellow with counts
- 🟣 "Phase 2" sections in purple

---

## 10. Phase 2 Roadmap (not implemented)

- AI recommendations engine (requires validated deal volume)
- Supplier self-signup flow with Stripe embedded checkout
- Per-ICP pricing variants
- API access for data integration
- Real-time signal alerts
- Comparative benchmarking across counties
