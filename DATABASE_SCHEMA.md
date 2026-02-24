# Master Database Schema Documentation

**Project:** Distressed Property Intelligence Platform (MVP)  
**Architecture:** Relational Hub-and-Spoke (3rd Normal Form)  
**Database:** PostgreSQL 15+

---

## 1. The Central Hub (Anchor)

### Table: `properties`

The primary table from which all other data radiates.

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique internal record ID. |
| `parcel_id` | Varchar | **Required.** Unique County Folio/PIN. |
| `address` | Varchar | Full site address. |
| `city` | Varchar | Standardized geography. |
| `state` | Varchar | Standardized geography. |
| `zip` | Varchar | Standardized geography. |
| `jurisdiction` | Varchar | Unincorporated, Tampa, Temple Terrace, Plant City. |
| `property_type` | Varchar | SFH, Multi-Family, Commercial, etc. |
| `year_built` | Integer | Original construction year. |
| `sq_ft` | Numeric | Physical characteristics - Square footage. |
| `beds` | Numeric | Physical characteristics - Bedrooms. |
| `baths` | Numeric | Physical characteristics - Bathrooms. |
| `lot_size` | Numeric | Total acreage or square footage. |
| `lat` | Numeric | Geolocation coordinates (API Gap). |
| `lon` | Numeric | Geolocation coordinates (API Gap). |
| `legal_description` | Text | Full legal boundary text. |
| `gohighlevel_contact_id` | Varchar | External CRM link (Duplicate prevention). |
| `sync_status` | Varchar | Pending, Synced, or Error. |
| `last_crm_sync` | Timestamp | Date of last successful export. |
| `created_at` | Timestamp | System audit timestamp. |
| `updated_at` | Timestamp | System audit timestamp. |

---

## 2. Property Extensions (1:1 Relationships)

### Table: `owners`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `owner_name` | Varchar | Current legal owner(s). |
| `mailing_address` | Varchar | Address for tax bills. |
| `owner_type` | Varchar | Individual, LLC, Trust, Estate, Corporate. |
| `ownership_years` | Numeric | Time since last title transfer. |
| `absentee_status` | Varchar | In-County, Out-of-County, Out-of-State. |
| `phone_1` | Varchar | Skip-traced mobile/landlines (API Gap). |
| `phone_2` | Varchar | Skip-traced mobile/landlines (API Gap). |
| `phone_3` | Varchar | Skip-traced mobile/landlines (API Gap). |
| `email_1` | Varchar | Skip-traced email addresses (API Gap). |
| `email_2` | Varchar | Skip-traced email addresses (API Gap). |
| `linkedin_url` | Varchar | Social profile link (API Gap). |
| `employer_name` | Varchar | Owner's employer (API Gap). |
| `estimated_income` | Numeric | Owner's income tier (API Gap). |
| `credit_score_tier` | Varchar | Credit health category (API Gap). |
| `skip_trace_success` | Boolean | Y/N - Indicator of successful contact data. |

### Table: `financials`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `assessed_value_mkt` | Numeric | County market valuation. |
| `assessed_value_tax` | Numeric | County taxable valuation. |
| `last_sale_price` | Numeric | Last transaction data. |
| `last_sale_date` | Date | Last transaction data. |
| `value_change_yoy` | Numeric | Year-over-year market change %. |
| `est_mortgage_bal` | Numeric | Total estimated debt (API Gap). |
| `mtg_1` | Numeric | First position balance (API Gap). |
| `mtg_2` | Numeric | Second position balance (API Gap). |
| `total_lien_amount` | Numeric | Aggregate of all recorded liens. |
| `total_debt` | Numeric | Mortgage + Liens. |
| `est_equity` | Numeric | Profit potential (API Gap). |
| `equity_%` | Numeric | Profit potential percentage (API Gap). |
| `price_per_sq_ft` | Numeric | Market efficiency metric. |
| `annual_tax_amount` | Numeric | Yearly property tax liability. |
| `homestead_exempt` | Boolean | Y/N - Primary residence status. |
| `est_repair_cost` | Numeric | Derived from local cost-estimate JSON. |
| `arv` | Numeric | After Repair Value (API Gap). |

---

## 3. Distress Signal Tables (1:Many)

### Table: `code_violations`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `record_number` | Varchar | **Unique.** Case ID (e.g., CE-2024). |
| `violation_type` | Varchar | The specific code infraction. |
| `description` | Text | Inspector notes. |
| `opened_date` | Date | Date filed. |
| `status` | Varchar | Current case state. |
| `severity_tier` | Varchar | Critical, Major, or Minor mapping. |
| `fine_amount` | Numeric | Daily accumulation or fixed fine. |
| `is_lien` | Boolean | Flag if escalated to a legal lien. |

### Table: `legal_and_liens`

**Purpose:** Financial claims against property (liens and judgments).

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `record_type` | Varchar | **Type:** 'Lien' or 'Judgment'. |
| `instrument_number` | Varchar | **Unique.** Official recording number. |
| `creditor` | Varchar | Party owed money (bank, contractor, IRS). |
| `debtor` | Varchar | Property owner at time of filing. |
| `amount` | Numeric | Financial value of the lien/judgment. |
| `filing_date` | Date | When recorded with county clerk. |
| `book_type` | Varchar | Official records or court records. |
| `book_number` | Varchar | Volume reference in public records. |
| `page_number` | Varchar | Page reference in public records. |
| `document_type` | Varchar | Mechanics Lien, IRS Tax Lien, Certified Judgment, etc. |
| `legal_description` | Text | Property description from instrument. |

### Table: `deeds`

**Purpose:** Ownership transfer transactions (market activity, not distress).

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `instrument_number` | Varchar | **Unique.** Official recording number. |
| `grantor` | Varchar | Seller/transferor of property. |
| `grantee` | Varchar | Buyer/recipient of property. |
| `record_date` | Date | Date deed was recorded. |
| `sale_price` | Numeric | Transaction amount (if disclosed). |
| `deed_type` | Varchar | Warranty Deed, Quitclaim, Tax Deed, etc. |
| `doc_type` | Varchar | Document classification. |
| `book_type` | Varchar | Official records or court records. |
| `book_number` | Varchar | Volume reference in public records. |
| `page_number` | Varchar | Page reference in public records. |
| `legal_description` | Text | Property description from deed. |

### Table: `legal_proceedings`

**Purpose:** Court proceedings related to property (probate, evictions, bankruptcy).

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `record_type` | Varchar | **Type:** 'Probate', 'Eviction', or 'Bankruptcy'. |
| `case_number` | Varchar | **Unique.** Court case identifier. |
| `filing_date` | Date | When case was filed in court. |
| `case_status` | Varchar | Current state of case (Active, Closed, etc.). |
| `associated_party` | Varchar | Decedent (Probate), Tenant (Eviction), or Debtor (Bankruptcy). |
| `secondary_party` | Varchar | Beneficiary, Landlord, or Trustee. |
| `amount` | Numeric | Financial value if applicable. |
| `meta_data` | JSONB | **Flex-Bucket:** Case-specific fields (Chapter #, Attorney, Title). |

### Table: `tax_delinquencies`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `tax_year` | Integer | The year taxes were missed. |
| `years_delinquent` | Integer | Total count of unpaid years. |
| `total_amount_due` | Numeric | Total live debt (Firecrawl Sniper). |
| `certificate_data` | Varchar | Certificate # and Holder Name. |
| `deed_app_date` | Date | Tax Deed Application date (High Urgency). |

### Table: `foreclosures`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `case_number` | Varchar | **Unique.** Court case identifier. |
| `plaintiff` | Varchar | The bank or lender suing for title. |
| `filing_date` | Date | When action was initiated. |
| `lis_pendens_date` | Date | Public notice of litigation. |
| `judgment_amount` | Numeric | Final amount required to redeem property. |
| `auction_date` | Timestamp | Date of public sale. |

### Table: `building_permits`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `permit_number` | Varchar | Unique permit identifier. |
| `permit_type` | Varchar | Plumbing, Electrical, Structural, etc. |
| `issue_date` | Date | Used to track "Expired" status. |
| `expire_date` | Date | Used to track "Expired" status. |
| `status` | Varchar | Specifically flags "Missing Final Inspection." |

### Table: `incidents`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `incident_type` | Varchar | Arrest, Police Dispatch, or Fire. |
| `incident_date` | Date | When the event occurred. |
| `arrest_count_12m` | Integer | Rolling total for scoring. |
| `crime_types` | JSONB | Tag list: ["Drug Activity", "Vandalism"]. |
| `problem_prop_flag` | Boolean | Chronic nuisance property indicator. |

---

## 4. Scoring & Intelligence

### Table: `distress_scores`

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | Serial (PK) | Unique record ID. |
| `property_id` | Int (FK) | Links to `properties.id`. |
| `score_date` | Timestamp | When the CDS Engine ran. |
| `final_cds_score` | Numeric | Final 0-100 Score. |
| `lead_tier` | Varchar | Ultra Platinum, Platinum, Gold, etc. |
| `distress_types` | JSONB | Tag list: ["Code", "Tax", "HOA"]. |
| `urgency_level` | Varchar | Immediate, High, Medium, Low. |
| `multiplier` | Numeric | Compound multiplier applied (1.5x, 2x). |
| `factor_scores` | JSONB | Individual points for the 6 Factors. |
| `qualified` | Boolean | If >= Threshold for CRM export. |

---

## Database Relationships

### Hub-and-Spoke Architecture

```
                                    ┌─────────────────┐
                                    │   properties    │ (Central Hub)
                                    │      (PK)       │
                                    └────────┬────────┘
                                             │
                 ┌───────────────────────────┼───────────────────────────┐
                 │                           │                           │
        ┌────────▼────────┐         ┌────────▼────────┐         ┌────────▼────────┐
        │     owners      │         │   financials    │         │ distress_scores │
        │  (1:1 with PK)  │         │  (1:1 with PK)  │         │  (1:Many)       │
        └─────────────────┘         └─────────────────┘         └─────────────────┘
                 
                 ┌───────────────────────────┼───────────────────────────┐
                 │                           │                           │
        ┌────────▼────────┐         ┌────────▼────────┐         ┌────────▼────────┐
        │code_violations  │         │legal_and_liens  │         │     deeds       │
        │    (1:Many)     │         │ (Liens/Judgments│         │ (Transactions)  │
        │                 │         │    1:Many)      │         │    (1:Many)     │
        └─────────────────┘         └─────────────────┘         └─────────────────┘
                 
                 ┌───────────────────────────┼───────────────────────────┐
                 │                           │                           │
        ┌────────▼────────┐         ┌────────▼────────┐         ┌────────▼────────┐
        │legal_proceedings│         │tax_delinquencies│         │  foreclosures   │
        │(Court Cases)    │         │    (1:Many)     │         │    (1:Many)     │
        │    (1:Many)     │         └─────────────────┘         └─────────────────┘
        └─────────────────┘                  │
                                     ┌────────▼────────┐
                                     │building_permits │
                                     │    (1:Many)     │
                                     └─────────────────┘
                                             │
                                     ┌────────▼────────┐
                                     │   incidents     │
                                     │    (1:Many)     │
                                     └─────────────────┘
```

---

## Key Design Principles

1. **Normalization:** All tables follow 3rd Normal Form (3NF) to eliminate redundancy
2. **Hub-and-Spoke:** The `properties` table is the central anchor for all relationships
3. **Domain Separation:** Legal claims (liens/judgments), transactions (deeds), and court proceedings (probate/evictions/bankruptcy) are separated into dedicated tables for better data integrity
4. **API Gap Fields:** Clearly marked fields that require external data enrichment
5. **Audit Trail:** All tables include timestamps for data lineage tracking
6. **CRM Integration:** Built-in fields for GoHighLevel synchronization and duplicate prevention
7. **Scoring Ready:** Dedicated table for distress scoring with embedded metadata
8. **Flexible Metadata:** JSONB fields only in `legal_proceedings` where case-specific data varies by type

