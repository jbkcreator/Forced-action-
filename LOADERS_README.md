# Data Loaders Module

## Overview

The `src/loaders` module provides a clean, reusable API for inserting scraped data into the database. Each loader handles:
- Fuzzy matching to master properties
- Duplicate detection
- Data validation and normalization
- Flexible input (CSV or DataFrame)

## Architecture

```
src/loaders/
├── __init__.py              # Public API exports
├── base.py                  # BaseLoader with matching utilities
├── master.py                # MasterPropertyLoader
├── violations.py            # ViolationLoader
├── liens.py                 # LienLoader
├── deeds.py                 # DeedLoader
├── legal_proceedings.py     # Probate/Eviction/BankruptcyLoader
├── tax.py                   # TaxDelinquencyLoader
├── foreclosures.py          # ForeclosureLoader
└── permits.py               # BuildingPermitLoader
```

## Usage

### 1. From Scrapers (Production)

Scrapers can call loaders directly after scraping:

```python
# Example: Violation scraper with auto-load
from src.core.database import get_db_context
from src.loaders import ViolationLoader

# Scrape data into DataFrame
df = scrape_violations(start_date, end_date)

# Save CSV (for audit/debugging)
df.to_csv("data/raw/violations/violations_20260224.csv", index=False)

# Load into database immediately
with get_db_context() as session:
    loader = ViolationLoader(session)
    matched, unmatched, skipped = loader.load_from_dataframe(df)
    session.commit()
    
    logger.info(f"Loaded violations: {matched} matched, {unmatched} unmatched")
```

### 2. From CSV (Testing/Manual)

Use the CLI script for manual loading:

```bash
# Load specific type
python scripts/load_data.py --type violations

# Load multiple types
python scripts/load_data.py --types master,violations,liens

# Load all data
python scripts/load_data.py --all

# Custom file
python scripts/load_data.py --type violations --file data/my_violations.csv
```

## Available Loaders

### MasterPropertyLoader
Inserts properties with owners and financials.

```python
from src.loaders import MasterPropertyLoader

loader = MasterPropertyLoader(session)
inserted, _, skipped = loader.load_from_csv("data/raw/master/master.csv")
```

**Match Method:** Creates new records (no matching - uses parcel_id as unique key)  
**Expected Columns:** FOLIO, OWNER, SITE_ADDR, SITE_CITY, etc.

---

### ViolationLoader
Inserts code enforcement violations.

```python
from src.loaders import ViolationLoader

loader = ViolationLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/violations/file.csv")
```

**Match Method:** Address fuzzy matching (85% threshold)  
**Expected Columns:** Record Number, Address, Status, Violation Type, etc.

---

### LienLoader
Inserts liens and certified judgments.

```python
from src.loaders import LienLoader

loader = LienLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/liens/all_liens_judgments.csv")
```

**Match Method:** Owner name fuzzy matching (75% threshold)  
**Expected Columns:** Instrument, Grantor, Grantee, RecordDate, document_type, etc.  
**Record Types:** Automatically determines 'Lien' or 'Judgment' based on document_type

---

### DeedLoader
Inserts property deeds (ownership transfers).

```python
from src.loaders import DeedLoader

loader = DeedLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/deeds/all_deeds.csv")
```

**Match Method:** Owner name fuzzy matching (grantor or grantee)  
**Expected Columns:** Instrument, Grantor, Grantee, RecordDate, Deed Type, etc.

---

### ProbateLoader
Inserts probate court cases.

```python
from src.loaders import ProbateLoader

loader = ProbateLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/probate/probate_leads.csv")
```

**Match Method:** Address matching → fallback to name matching  
**Expected Columns:** CaseNumber, PartyAddress, FilingDate, PartyType, FirstName, LastName/CompanyName, etc.  
**Note:** Groups rows by CaseNumber (multiple parties per case)

---

### EvictionLoader
Inserts eviction court cases.

```python
from src.loaders import EvictionLoader

loader = EvictionLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/evictions/eviction_leads.csv")
```

**Match Method:** Address fuzzy matching (defendant address)  
**Expected Columns:** CaseNumber, PartyAddress, FilingDate, PartyType, FirstName, LastName/CompanyName, etc.  
**Note:** Groups rows by CaseNumber (plaintiff + defendant)

---

### BankruptcyLoader
Inserts bankruptcy court cases.

```python
from src.loaders import BankruptcyLoader

loader = BankruptcyLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/bankruptcy/bankruptcy_leads.csv")
```

**Match Method:** Owner name matching only (NO ADDRESS DATA)  
**Expected Columns:** Docket Number, Lead Name, Date Filed, Case Type, Division, Court ID  
**Note:** Very low match rate (10-30%) due to name-only matching

---

### TaxDelinquencyLoader
Inserts tax delinquency records.

```python
from src.loaders import TaxDelinquencyLoader

loader = TaxDelinquencyLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/tax_delinquencies/tax_deliquencies.csv")
```

**Match Method:** Exact parcel ID matching (100% accuracy)  
**Expected Columns:** Account Number, Tax Yr, Owner Name, total_amount_due, years_delinquent_scraped, etc.

---

### ForeclosureLoader
Inserts foreclosure court cases.

```python
from src.loaders import ForeclosureLoader

loader = ForeclosureLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/foreclosures/file.csv")
```

**Match Method:** Parcel ID → fallback to address matching  
**Expected Columns:** Case Number, Parcel ID, Property Address, Plaintiff, Defendant, Filing Date, etc.

---

### BuildingPermitLoader
Inserts building permits.

```python
from src.loaders import BuildingPermitLoader

loader = BuildingPermitLoader(session)
matched, unmatched, skipped = loader.load_from_csv("data/raw/permits/permits.csv")
```

**Match Method:** Address fuzzy matching (85% threshold)  
**Expected Columns:** Record Number, Address, Status, Type, Description, Issue Date, etc.

---

## BaseLoader Utilities

All loaders inherit from `BaseLoader` which provides:

### Normalization Methods
```python
loader.normalize_address("123 N Main ST APT 5")  # → "123 MAIN STREET"
loader.normalize_owner_name("SMITH JOHN LLC")     # → "SMITH JOHN"
loader.parse_amount("$1,234.56")                   # → 1234.56
loader.parse_date("02/24/2026")                    # → datetime(2026, 2, 24)
```

### Matching Methods
```python
# Exact parcel ID match (100% accuracy)
property = loader.find_property_by_parcel_id("A0000100000")

# Fuzzy address match (85% threshold)
property, score = loader.find_property_by_address("123 Main St, Tampa, FL")

# Fuzzy owner name match (75% threshold)
property, score = loader.find_property_by_owner_name("John Smith")
```

### Duplicate Checking
```python
# Check if record exists
exists = loader.check_duplicate(CodeViolation, {'case_number': 'CE-2024-001'})
```

---

## Integration Examples

### Scraper with Auto-Load (Recommended)

```python
# violation_engine.py
from src.core.database import get_db_context
from src.loaders import ViolationLoader

def scrape_and_load_violations(start_date, end_date):
    """Scrape violations and load into database."""
    
    # 1. Scrape data
    df = scrape_violations_from_website(start_date, end_date)
    logger.info(f"Scraped {len(df)} violations")
    
    # 2. Save CSV (for audit trail)
    csv_path = f"data/raw/violations/violations_{start_date}_{end_date}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved to {csv_path}")
    
    # 3. Load into database
    with get_db_context() as session:
        loader = ViolationLoader(session)
        matched, unmatched, skipped = loader.load_from_dataframe(df, skip_duplicates=True)
        session.commit()
        
        logger.info(f"Loaded: {matched} matched, {unmatched} unmatched, {skipped} skipped")
    
    return matched, unmatched, skipped
```

### Scraper with Separate Load Step

```python
# violation_engine.py
def scrape_violations(start_date, end_date):
    """Scrape violations and save to CSV."""
    df = scrape_violations_from_website(start_date, end_date)
    csv_path = f"data/raw/violations/violations_{start_date}_{end_date}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved {len(df)} violations to {csv_path}")
    return csv_path

# Later, manually load:
# python scripts/load_data.py --type violations --file data/raw/violations/violations_2026-02-24.csv
```

---

## Error Handling

All loaders handle:
- Missing/malformed data (skips gracefully)
- Duplicate records (checks before insert)
- Database errors (logs and continues)
- Match failures (logs unmatched records)

```python
try:
    with get_db_context() as session:
        loader = ViolationLoader(session)
        matched, unmatched, skipped = loader.load_from_csv("violations.csv")
        session.commit()
except Exception as e:
    logger.error(f"Failed to load violations: {e}")
    # CSV is still saved for manual inspection/retry
```

---

## Testing

### Run Syntax Checks
```bash
python -m py_compile src/loaders/base.py
python -m py_compile src/loaders/violations.py
# ... etc
```

### Test Individual Loader
```bash
python scripts/load_data.py --type violations --file data/test_violations.csv
```

### Test Full Pipeline
```bash
# 1. Initialize database
python scripts/load_data.py --init-db

# 2. Load master properties first
python scripts/load_data.py --type master

# 3. Load distress data
python scripts/load_data.py --types violations,liens,deeds

# 4. Check results
python scripts/check_db_health.py --detailed
```

---

## Performance

- **Matching Speed:** ~1000 records/minute (address fuzzy matching)
- **Database Commits:** Batch commits at end of each data type
- **Memory Usage:** Processes DataFrames in-memory (efficient for 10K-100K records)

For larger datasets (>100K records), consider chunking:

```python
chunk_size = 10000
for chunk in pd.read_csv("large_file.csv", chunksize=chunk_size):
    loader.load_from_dataframe(chunk)
    session.commit()  # Commit each chunk
```

---

## Migration from Old Scripts

### Old Way (scripts/property_matchers.py)
```python
from scripts.property_matchers import match_and_insert_violations
matched, unmatched, skipped = match_and_insert_violations(session, "violations.csv")
```

### New Way (src/loaders)
```python
from src.loaders import ViolationLoader
loader = ViolationLoader(session)
matched, unmatched, skipped = loader.load_from_csv("violations.csv")
```

**Benefits:**
- ✅ Cleaner imports (from `src`, not `scripts`)
- ✅ Object-oriented API
- ✅ Reusable across scrapers and scripts
- ✅ Easier to test and mock
- ✅ Supports DataFrame input (not just CSV)

---

## Troubleshooting

### Import Errors
```bash
# Ensure src is in Python path
export PYTHONPATH="."  # Linux/Mac
$env:PYTHONPATH = "."  # PowerShell
```

### Low Match Rates
- **Address Matching:** Check address normalization with `loader.normalize_address()`
- **Name Matching:** Check name normalization with `loader.normalize_owner_name()`
- **Thresholds:** Adjust in loader code (default: 85% address, 75% name)

### Duplicate Errors
- Use `skip_duplicates=True` (default) to skip existing records
- Or use `skip_duplicates=False` with `--fail-on-duplicates` for strict mode

---

## Future Enhancements

- [ ] Add batch processing for large files (>100K records)
- [ ] Add progress bars for long-running loads
- [ ] Add more sophisticated matching algorithms (ML-based)
- [ ] Add validation rules per data type
- [ ] Add rollback on partial failures

---

## Support

For issues or questions:
1. Check logs in `logs/data_loading.log`
2. Run health check: `python scripts/check_db_health.py --detailed`
3. Review unmatched records in logs
4. Adjust matching thresholds if needed
