# Database Implementation Guide

## Overview

This project uses **SQLAlchemy 2.0** as the ORM (Object-Relational Mapper) with **PostgreSQL** as the database backend. The schema implements a **Hub-and-Spoke architecture** with the `properties` table as the central hub.

## Architecture

```
properties (Central Hub)
    ├── owners (1:1)
    ├── financials (1:1)
    ├── code_violations (1:Many)
    ├── legal_and_liens (1:Many, Polymorphic)
    ├── tax_delinquencies (1:Many)
    ├── foreclosures (1:Many)
    ├── building_permits (1:Many)
    ├── incidents (1:Many)
    └── distress_scores (1:Many)
```

## Setup

### 1. Install Dependencies

```bash
# Install all dependencies including SQLAlchemy
pip install -e .
```

### 2. Configure Database

Create a `.env` file in the project root:

```env
# Database Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/distressed_properties
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10

# API Keys (existing)
ANTHROPIC_API_KEY=your_key_here
FIRECRAWL_API_KEY=your_key_here
COURT_LISTENER_API_KEY=your_key_here
```

### 3. Create the Database

```bash
# Using psql
psql -U postgres
CREATE DATABASE distressed_properties;
\q
```

### 4. Initialize Tables

```python
from src.core.database import init_database

# This creates all tables
init_database()
```

Or use the example script:

```bash
python scripts/database_examples.py
```

## Usage Examples

### Basic Property Creation

```python
from src.core.database import db
from src.core.models import Property, Owner, Financial

with db.session_scope() as session:
    # Create property
    prop = Property(
        parcel_id="12345-ABC-67890",
        address="123 Main St",
        city="Tampa",
        state="FL",
        zip="33602",
        property_type="SFH"
    )
    
    # Create owner
    owner = Owner(
        property=prop,
        owner_name="John Doe",
        owner_type="Individual"
    )
    
    session.add(prop)
    # Session automatically commits on success
```

### Querying Properties

```python
from src.core.db_queries import (
    get_property_by_parcel_id,
    get_qualified_leads,
    get_properties_with_code_violations
)

with db.session_scope() as session:
    # Get single property
    prop = get_property_by_parcel_id(session, "12345-ABC-67890")
    
    # Get qualified leads
    leads = get_qualified_leads(session, lead_tier="Platinum", limit=50)
    
    # Get properties with critical violations
    violated = get_properties_with_code_violations(session, severity="Critical")
```

### Adding Distress Signals

```python
from datetime import date
from src.core.models import CodeViolation, TaxDelinquency

with db.session_scope() as session:
    prop = get_property_by_parcel_id(session, "12345-ABC-67890")
    
    # Add code violation
    violation = CodeViolation(
        property=prop,
        record_number="CE-2024-001234",
        violation_type="Overgrown Vegetation",
        opened_date=date(2024, 1, 15),
        severity_tier="Minor"
    )
    
    # Add tax delinquency
    tax_delinq = TaxDelinquency(
        property=prop,
        tax_year=2023,
        years_delinquent=2,
        total_amount_due=7500.00
    )
    
    session.add(violation)
    session.add(tax_delinq)
```

### Using the Polymorphic LegalAndLien Table

```python
from src.core.models import LegalAndLien

with db.session_scope() as session:
    # HOA Lien
    hoa_lien = LegalAndLien(
        property=prop,
        record_type="HOA",
        filing_date=date(2023, 6, 1),
        amount=5000.00,
        associated_party="Sunset Ridge HOA",
        meta_data={
            "status": "Unpaid",
            "attorney": "Smith & Associates"
        }
    )
    
    # Bankruptcy
    bankruptcy = LegalAndLien(
        property=prop,
        record_type="Bankruptcy",
        filing_date=date(2024, 2, 10),
        associated_party="John Doe",
        meta_data={
            "chapter": 13,
            "case_number": "24-BK-12345",
            "status": "Active"
        }
    )
    
    session.add(hoa_lien)
    session.add(bankruptcy)
```

### Calculating Distress Scores

```python
from src.core.models import DistressScore
from datetime import datetime

with db.session_scope() as session:
    score = DistressScore(
        property=prop,
        score_date=datetime.utcnow(),
        final_cds_score=72.5,
        lead_tier="Gold",
        distress_types=["Code", "Tax", "HOA"],
        urgency_level="High",
        multiplier=1.5,
        factor_scores={
            "financial_distress": 15.0,
            "code_violations": 12.0,
            "tax_delinquency": 18.0,
            "legal_issues": 10.0,
            "property_condition": 8.5,
            "owner_profile": 9.0
        },
        qualified=True
    )
    session.add(score)
```

## Key Components

### 1. Models (`src/core/models.py`)

Contains all SQLAlchemy ORM models:
- `Property` - Central hub table
- `Owner` - Owner information (1:1)
- `Financial` - Financial data (1:1)
- `CodeViolation` - Code violations (1:Many)
- `LegalAndLien` - Polymorphic liens/legal issues (1:Many)
- `TaxDelinquency` - Tax delinquencies (1:Many)
- `Foreclosure` - Foreclosure cases (1:Many)
- `BuildingPermit` - Building permits (1:Many)
- `Incident` - Police/fire incidents (1:Many)
- `DistressScore` - CDS scoring results (1:Many)

### 2. Database Management (`src/core/database.py`)

Provides:
- Singleton `Database` class for connection management
- Session factory and context managers
- Connection pooling
- Transaction management

Key functions:
- `db.session_scope()` - Context manager for transactions
- `init_database()` - Create all tables
- `check_connection()` - Test database connectivity
- `get_table_counts()` - Get record counts

### 3. Query Utilities (`src/core/db_queries.py`)

Pre-built query functions:
- Property searches and lookups
- Distress signal filtering
- Lead qualification queries
- Statistical aggregations
- Bulk operations

## Database Migrations with Alembic

### Initialize Alembic

```bash
# Initialize alembic
alembic init alembic

# Edit alembic.ini to point to your database
# Edit alembic/env.py to import your models
```

### Create a Migration

```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "Initial tables"

# Apply migration
alembic upgrade head
```

### Manual Migrations

```bash
# Create empty migration
alembic revision -m "Add custom index"

# Edit the generated file in alembic/versions/
# Then apply
alembic upgrade head
```

## Best Practices

### 1. Always Use Context Managers

```python
# ✅ GOOD - Auto-commit/rollback
with db.session_scope() as session:
    session.add(obj)

# ❌ BAD - Manual management
session = db.get_session()
session.add(obj)
session.commit()  # What if this fails?
session.close()
```

### 2. Eager Loading for Related Data

```python
from sqlalchemy.orm import joinedload

# Load property with all relations
prop = session.query(Property) \
    .options(joinedload(Property.owner)) \
    .options(joinedload(Property.financial)) \
    .filter(Property.id == 123) \
    .first()
```

### 3. Use Query Helper Functions

```python
# ✅ GOOD - Use helpers
from src.core.db_queries import get_qualified_leads
leads = get_qualified_leads(session, lead_tier="Platinum")

# ❌ BAD - Write complex queries inline
leads = session.query(Property).join(DistressScore) \
    .filter(DistressScore.qualified == True) \
    .filter(DistressScore.lead_tier == "Platinum") \
    # ... complex query ...
```

### 4. Bulk Operations for Performance

```python
# For inserting many records
session.bulk_insert_mappings(Property, list_of_dicts)

# For updating many records
from src.core.db_queries import bulk_update_sync_status
bulk_update_sync_status(session, property_ids, "synced")
```

## Troubleshooting

### Connection Issues

```python
from src.core.database import check_connection

if not check_connection():
    print("Database connection failed!")
    # Check DATABASE_URL in .env
```

### View Current Schema

```sql
-- In psql
\dt          -- List tables
\d properties  -- Describe a table
```

### Check Table Counts

```python
from src.core.database import get_table_counts

counts = get_table_counts()
print(counts)
```

### View Query SQL

```python
# Enable SQL echo
# In .env file:
DB_ECHO=true

# Or programmatically
from config.settings import settings
settings.db_echo = True
```

## Performance Tips

1. **Use indexes** - Already defined on foreign keys and common query fields
2. **Batch operations** - Use bulk operations for large inserts/updates
3. **Connection pooling** - Already configured with sensible defaults
4. **Eager loading** - Use `joinedload()` to avoid N+1 queries
5. **JSONB indexes** - GIN indexes on JSONB columns for fast queries

## Next Steps

1. **Set up Alembic** for production-grade migrations
2. **Create seed data** scripts for testing
3. **Add database backup** procedures
4. **Monitor queries** with logging and performance analysis
5. **Implement data validators** for business rules

## References

- [SQLAlchemy 2.0 Docs](https://docs.sqlalchemy.org/en/20/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- See `DATABASE_SCHEMA.md` for detailed schema specification
