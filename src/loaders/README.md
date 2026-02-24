# Database Insertion Scripts

Modular scripts for inserting distressed property data into the database with fuzzy matching.

## Overview

The insertion pipeline is completely **decoupled from scrapers** and works with any CSV source (scraped data, manual uploads, or external sources).

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   SCRAPER   │ ───> │   MATCHER   │ ───> │  INSERTER   │
│ (Raw CSVs)  │      │ (Link to    │      │ (Database)  │
│             │      │  Master)    │      │             │
└─────────────┘      └─────────────┘      └─────────────┘
```

## Files

### `property_matchers.py`
Core matching utilities:
- `normalize_address()` - Standardize addresses for fuzzy matching
- `normalize_owner_name()` - Standardize owner names
- `find_property_by_parcel_id()` - Direct parcel ID matching (100% accuracy)
- `find_property_by_address()` - Address fuzzy matching (70-80% accuracy)
- `find_property_by_owner_name()` - Owner name fuzzy matching (40-60% accuracy)
- `insert_master_properties()` - Insert hub properties
- `match_and_insert_tax_delinquencies()` - Tax records
- `match_and_insert_foreclosures()` - Foreclosure records
- `match_and_insert_violations()` - Code violation records
- `match_and_insert_liens_judgments()` - Lien/judgment records

### `insert_to_database.py`
Master orchestrator with CLI interface:
- Validates all CSV files before insertion
- 4-phase insertion pipeline (master → direct matching → address fuzzy → name fuzzy)
- Flexible options for custom files and selective insertion
- Comprehensive summary reporting

### `test_insertion.py`
Test suite for validation:
- Test normalization functions
- Test master property insertion
- Test fuzzy matching accuracy
- Test full insertion pipeline on sample data

### `check_db_health.py` ✨ **NEW**
Database health monitoring:
- Connection status check
- Table existence validation
- Record counts and statistics
- Foreign key integrity (orphaned records)
- Duplicate detection
- Data quality metrics
- Relationship analysis
- Overall health score (0-100)

## Usage

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Required: `rapidfuzz>=3.10.0` for fuzzy matching

### 2. Initialize Database (First Time Only)

```bash
python scripts/insert_to_database.py --init-db
```

### 3. Insert All Data (Default Paths)

```bash
python scripts/insert_to_database.py
```

This will:
1. Validate all CSV files
2. Insert 10 master properties
3. Match and insert ~4 tax records (parcel ID - 100% accuracy)
4. Match and insert ~1 foreclosure (parcel ID - 100% accuracy)
5. Match and insert ~9 violations (address fuzzy - 75% accuracy)
6. Match and insert ~1,900 liens/judgments (name fuzzy - 50% accuracy)
7. Match and insert ~90 probate records (address + name fuzzy - 70% accuracy)
8. Match and insert ~8 building permits (address fuzzy - 75% accuracy)
9. Match and insert ~300 evictions (address fuzzy - 70% accuracy)
10. Match and insert ~1-2 bankruptcy (name-only - 10-30% accuracy)

**Expected Results:**
- Total matched: ~2,300 records
- Match rate: ~50% (due to name fuzzy matching and bankruptcy low rate)

### 4. Validate Files Only (No Insertion)

```bash
python scripts/insert_to_database.py --validate-only
```

Checks:
- File exists and is readable
- Required columns present
- CSV is parseable

### 5. Insert Specific Data Types

```bash
# Insert only violations
python scripts/insert_to_database.py --types violations

# Insert only violations and liens
python scripts/insert_to_database.py --types violations,liens

# Insert all distress signals except bankruptcy
python scripts/insert_to_database.py --types violations,liens,foreclosures,tax,probate,evictions,permits
```

### 6. Use Custom File Paths

```bash
# Insert violations from custom file
python scripts/insert_to_database.py --types violations --violations-file data/my_custom_violations.csv

# Insert all with custom paths
python scripts/insert_to_database.py \
  --master-file data/my_master.csv \
  --liens-file data/my_liens.csv
```

### 7. Fail on Duplicates

By default, the script skips duplicate records. To fail instead:

```bash
python scripts/insert_to_database.py --fail-on-duplicates
```

### 8. Test on Sample Data

```bash
# Test with existing database
python scripts/test_insertion.py

# Reset database and test
python scripts/test_insertion.py --reset-db
```

### 9. Check Database Health ✨

```bash
# Basic health check
python scripts/check_db_health.py

# Detailed analysis (includes data quality metrics)
python scripts/check_db_health.py --detailed
```

**Health Check Reports**:
- ✓ Database connection status
- ✓ Table existence (all 10 tables)
- ✓ Record counts per table
- ✓ Foreign key integrity (orphaned records)
- ✓ Duplicate detection
- ✓ Data quality metrics (missing fields)
- ✓ Relationship analysis (distress signals per property)
- ✓ Overall health score (0-100)

**Exit Codes**:
- `0` - Excellent/Good health (score ≥70)
- `1` - Fair/Poor health (score 30-69)
- `2` - Critical issues (score <30)

## Expected File Formats

### Master Properties (`data/raw/master/master.csv`)
Required columns:
- `FOLIO` - Parcel ID (primary key)
- `OWNER` - Owner name
- `SITE_ADDR` - Property address
- `ASD_VAL`, `LAND`, `BLDG`, etc. - Financial data

### Tax Delinquencies (`data/raw/tax_delinquencies/tax_deliquencies.csv`)
Required columns:
- `Account Number` - Parcel ID for matching
- `Tax Yr` - Tax year
- `total_amount_due` - Delinquent amount

### Foreclosures (`data/raw/foreclosures/hillsborough_realforeclose_20260218.csv`)
Required columns:
- `Case Number` - Unique case identifier
- `Parcel ID` - For direct matching
- `Property Address` - For fuzzy matching fallback
- `Plaintiff`, `Defendant`, `Judgment Amount`, etc.

### Violations (`data/raw/violations/hcfl_code_enforcement_violations.csv`)
Required columns:
- `Record Number` - Unique case identifier
- `Address` - For fuzzy matching
- `Status`, `Date`, `Description`, etc.

### Liens/Judgments (`data/raw/liens/all_liens_judgments.csv`)
Required columns:
- `Instrument` - Unique instrument number
- `Grantor` - Owner name for fuzzy matching
- `RecordDate` - Recording date
- `document_type` - Type (CCL, TCL, ML, TL, Judgments, etc.)

### Building Permits (`data/raw/permits/building_permits_sample.csv`)
Required columns:
- `Record Number` - Unique permit number
- `Address` - For fuzzy matching
- `Status` - Permit status
- `Date`, `Record Type`, `Expiration Date` - Additional data

### Probate (`data/raw/probate/probate_leads.csv`)
Required columns:
- `CaseNumber` - Unique case identifier
- `PartyAddress` - For fuzzy matching
- `FilingDate` - Filing date
- `FirstName`, `MiddleName`, `LastName/CompanyName` - For name matching
- `PartyType` - Decedent, Petitioner, Beneficiary

### Evictions (`data/processed/eviction_leads_20260220.csv`)
Required columns:
- `CaseNumber` - Unique case identifier
- `PartyAddress` - For fuzzy matching (defendant address)
- `FilingDate` - Filing date
- `PartyType` - Plaintiff, Defendant
- `CaseTypeDescription` - Eviction type

### Bankruptcy (`data/raw/bankruptcy/tampa_bankruptcy_leads.csv`)
Required columns:
- `Docket Number` - Unique identifier
- `Lead Name` - Owner name (ONLY field for matching - no address!)
- `Date Filed` - Filing date
- `Case Type`, `Division`, `Court ID` - Additional data

**Note:** Bankruptcy has NO ADDRESS data, so matching is name-only with very low accuracy (10-30%).

## Fuzzy Matching Details

### Address Matching (Threshold: 85%)
1. Convert to uppercase
2. Remove directional prefixes (N, S, E, W, etc.)
3. Standardize street types (ST → STREET, RD → ROAD, etc.)
4. Remove unit/apt numbers
5. Use RapidFuzz ratio for similarity scoring

**Example:**
```
Input:  "123 N Main ST APT 5"
Output: "123 MAIN STREET"
```

### Owner Name Matching (Threshold: 75%)
1. Convert to uppercase
2. Remove legal suffixes (LLC, INC, TRUSTEE, etc.)
3. Remove punctuation
4. Use RapidFuzz token_sort_ratio (word order independent)

**Example:**
```
Input:  "SMITH JOHN LLC"
Output: "SMITH JOHN"
```

## Integration with Scrapers (Future)

Each scraper can optionally call insertion functions after scraping:

```python
# In lien_engine.py (example - DO NOT implement now)
from scripts.property_matchers import match_and_insert_liens_judgments
from src.core.database import get_db_context

# ... scraping logic ...
df.to_csv('data/raw/liens/all_liens_judgments.csv', index=False)

# Optional: Auto-insert after scraping
if args.auto_insert:
    with get_db_context() as session:
        matched, unmatched, skipped = match_and_insert_liens_judgments(session)
        print(f"✓ Inserted {matched} liens into database")
```

## Troubleshooting

### "Database connection failed"
Check your `.env` file has correct `DATABASE_URL`:
```
DATABASE_URL=postgresql://user:password@localhost:5432/distressed_properties
```

### "Missing required columns"
Verify CSV structure matches expected format. Use `--validate-only` to check.

### Low match rates
- Check data quality (addresses normalized correctly?)
- Review threshold values in `property_matchers.py`
- Check logs for details on unmatched records

### Duplicate key errors
Use `--skip-duplicates` (default) or clean database before re-inserting.

## Logs

All operations are logged to:
- Console (INFO level)
- `logs/database_insertion.log` (full details)

## Next Steps

1. ✅ Test with sample data: `python scripts/test_insertion.py`
2. ✅ Validate all files: `python scripts/insert_to_database.py --validate-only`
3. ✅ Insert master properties: `python scripts/insert_to_database.py --types master`
4. ✅ Insert all data: `python scripts/insert_to_database.py`
5. ✅ Check database health: `python scripts/check_db_health.py --detailed`
6. ⏳ Review unmatched records in logs
7. ⏳ Integrate with scrapers (when user approves)
8. ⏳ Implement CDS scoring engine
