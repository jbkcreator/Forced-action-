# FA Max campaign sequences — writer reference

_Generated 2026-09-25 from src/services/fa_max_campaigns/content.py — do not hand-edit this file._

## CSV format (one file per campaign)

One row per step: `campaign, step, days_after_previous, channel, subject, body, notes`

- `channel` is `email` or `sms` only.
- `step` starts at 1 with no gaps; `days_after_previous` is whole days from the prior step (step 1 counts from enrollment).
- Use `{{field_name}}` for a merge field, or `{{field_name|fallback text}}` for an optional one.
- Every `sms` step must include "Reply STOP" — nothing else adds it automatically.
- No pricing, rates, terms, or commitments in any subject or body.

## Fields on every campaign

- `{{first_name}}` (optional) — Contact's first name
- `{{sender_name}}` (required) — Josh's outreach display name (placeholder until confirmed)
- `{{sender_title}}` (required) — "Loan Officer" per client 18/9 #11
- `{{calendar_link}}` (required) — Booking link (spec item 9: on every outbound)

## capital_desk_loop

- `{{entity_name}}` (required) — The buying LLC/entity name from the deed
- `{{property_street}}` (required) — Street address of the purchased property
- `{{property_city}}` (required) — City of the purchased property
- `{{purchase_date}}` (required) — Date of the recorded cash purchase
- `{{county}}` (required) — County of the purchased property
- `{{buy_box_zips}}` (optional) — Zip codes this investor has bought in
- `{{buy_box_price_range}}` (optional) — This investor's typical purchase price range
- `{{purchase_count_24m}}` (optional) — Number of purchases in the trailing 24 months

## exit_desk

- `{{entity_name}}` (required) — The entity borrower on the aging mortgage
- `{{property_street}}` (required) — Street address of the mortgaged property
- `{{property_city}}` (required) — City of the mortgaged property
- `{{county}}` (required) — County of the mortgaged property
- `{{loan_age_months}}` (required) — Months since the current mortgage was recorded

## rescue_circuit

- `{{company_name}}` (required) — The partner's company name (title co., firm, brokerage)
- `{{partner_type}}` (required) — title_rep / closing_attorney / broker / loan_officer
- `{{state}}` (required) — Partner's state (FL or GA)
