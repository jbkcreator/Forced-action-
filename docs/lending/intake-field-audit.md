# Intake field audit (Runbook § 4.4, Item 4)

Runbook § 4.4 asks that the intake captures deal depth so lender fit can be checked on every file without manual
entry: `requested_loan_amount`, `property_address` (street, city, state, ZIP), `credit_band` (740+, 700–739,
660–699, 640–659, <640) and `cash_available` / verified liquidity. This is the audit only (2026-10-20); the mapping
waits on LendingFlow's final payload (D-1 to D-4) and on what the "Sprint 1 intake form" is (Q-19).

## Where each field is today

| Field (§ 4.4) | LendingFlow leads (`lending.lendingflow_leads`) | Website leads (`lending.web_leads`) | Calls (`lending.call_extractions`) | Lender-fit contracts (`src/lending/contracts.py`) |
|---|---|---|---|---|
| `requested_loan_amount` | `loan_amount_range` (raw band) + `loan_amount_min` / `_max`; `loan_amount` = the low end | **missing** (the form asks no amount) | not extracted | `LoanRequest.loan_amount` (Decimal, required) |
| `property_address` | `property_address`, `property_city`, `property_state`, `property_zip` | `property_city` only | `property_address` or `target_market` | `LoanRequest.property_address`, `LoanRequest.state` |
| `credit_band` | `credit_band` (raw) + `credit_band_min_fico` | **missing** | above or below 640 only | `BorrowerProfile.credit_band_min_fico` / `_max_fico` |
| `cash_available` / liquidity | **missing** (LendingFlow does not send it) | **missing** | `has_liquidity` (yes / no, no amount) | `BorrowerProfile.has_liquidity` (boolean) |

## Findings

1. **No source sends a cash amount.** Liquidity exists only as a yes / no from the call transcript, and the
   lender-fit contract only takes a boolean. A `cash_available` amount needs (a) a field LendingFlow or the form
   actually sends and (b) a decision whether it replaces or sits beside `has_liquidity`. Adding a column now would
   stay empty.
2. **The "<640" band is read as unknown.** `credit_band_min_fico()` (`src/lending/lendingflow.py`) returns 740, 700,
   660 and 640 for the other four Runbook bands (hyphen or en dash), but `None` for "<640" (and "Under 640"). A
   sub-640 borrower therefore looks like "credit unknown" rather than "credit below 640". The fix waits on
   LendingFlow's real band spellings (D-3); the parser's docstring already calls itself a stand-in.
3. **The website form has no deal-depth fields** (no amount, address, state or credit band). Adding them is a change
   to the separate website repo (`forced-action-ui`, branch `next-deal-lending-site`) plus `web_leads` columns.
4. **Calls already capture deal depth.** `lending.call_extractions` stores `deal_status` (`under_contract` /
   `actively_looking` / `no_deal`), `completed_projects`, `has_liquidity`, credit above or below 640 and the property
   address. This is also the only current source for the borrower score's "under contract" (+4) and experience (+3)
   criteria (`docs/lending/borrower-score.md`, Q-17); using it is a decision, not yet wired.

## Open

- Q-19: what the "Sprint 1 intake form" is.
- LendingFlow D-1 to D-4: final payload, band and loan-type spellings, a real test lead.
- Whether a `cash_available` amount replaces or sits beside `has_liquidity`.
- Whether the website form should collect amount, address, state and credit band.
