"""
Example usage of the database models and queries.
This script demonstrates how to interact with the database.
"""

from datetime import datetime, date

from src.core.database import db, init_database, check_connection, get_table_counts
from src.core.models import (
    Property,
    Owner,
    Financial,
    CodeViolation,
    LegalAndLien,
    TaxDelinquency,
    Foreclosure,
    DistressScore,
)
from src.core.db_queries import (
    get_property_by_parcel_id,
    get_qualified_leads,
    get_distress_statistics,
    get_properties_with_code_violations,
)


def example_1_create_property():
    """Example: Create a new property with owner and financial information."""
    print("\n=== Example 1: Creating a new property ===")

    with db.session_scope() as session:
        # Create a property
        prop = Property(
            parcel_id="12345-ABC-67890",
            address="123 Main Street",
            city="Tampa",
            state="FL",
            zip="33602",
            jurisdiction="Tampa",
            property_type="SFH",
            year_built=1985,
            sq_ft=1500.0,
            beds=3.0,
            baths=2.0,
            lot_size=7500.0,
        )

        # Create owner information
        owner = Owner(
            property=prop,
            owner_name="John Doe",
            mailing_address="456 Oak Ave, Miami, FL 33101",
            owner_type="Individual",
            absentee_status="Out-of-County",
            ownership_years=15.5,
        )

        # Create financial information
        financial = Financial(
            property=prop,
            assessed_value_mkt=250000.00,
            assessed_value_tax=240000.00,
            last_sale_price=200000.00,
            last_sale_date=date(2010, 3, 15),
            annual_tax_amount=3500.00,
            homestead_exempt=False,
        )

        session.add(prop)
        print(f"Created property: {prop}")
        print(f"Created owner: {owner}")
        print(f"Created financial: {financial}")


def example_2_add_distress_signals():
    """Example: Add distress signals to an existing property."""
    print("\n=== Example 2: Adding distress signals ===")

    with db.session_scope() as session:
        # Find a property
        prop = get_property_by_parcel_id(session, "12345-ABC-67890")
        if not prop:
            print("Property not found. Run example_1_create_property first.")
            return

        # Add a code violation
        violation = CodeViolation(
            property=prop,
            record_number="CE-2024-001234",
            violation_type="Overgrown Vegetation",
            description="Grass exceeds 12 inches in height",
            opened_date=date(2024, 1, 15),
            status="Open",
            severity_tier="Minor",
            fine_amount=250.00,
            is_lien=False,
        )
        session.add(violation)

        # Add a tax delinquency
        tax_delinq = TaxDelinquency(
            property=prop,
            tax_year=2023,
            years_delinquent=2,
            total_amount_due=7500.00,
            certificate_data="Cert #2023-12345, Holder: XYZ Investment Trust",
        )
        session.add(tax_delinq)

        # Add a lien (HOA lien as record_type='Lien')
        hoa_lien = LegalAndLien(
            property=prop,
            record_type="Lien",
            instrument_number="HOA-2023-12345",
            creditor="Sunset Ridge HOA",
            debtor=owner.owner_name,
            filing_date=date(2023, 6, 1),
            amount=5000.00,
            document_type="HOA Lien",
            legal_description="Lien for unpaid HOA dues and assessments"
        )
        session.add(hoa_lien)

        print(f"Added code violation: {violation}")
        print(f"Added tax delinquency: {tax_delinq}")
        print(f"Added HOA lien: {hoa_lien}")


def example_3_calculate_distress_score():
    """Example: Calculate and save a distress score for a property."""
    print("\n=== Example 3: Calculating distress score ===")

    with db.session_scope() as session:
        prop = get_property_by_parcel_id(session, "12345-ABC-67890")
        if not prop:
            print("Property not found. Run example_1_create_property first.")
            return

        # Create a distress score
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
                "owner_profile": 9.0,
            },
            qualified=True,
        )
        session.add(score)

        print(f"Added distress score: {score}")


def example_4_query_properties():
    """Example: Query properties using helper functions."""
    print("\n=== Example 4: Querying properties ===")

    with db.session_scope() as session:
        # Get qualified leads
        leads = get_qualified_leads(session, limit=10)
        print(f"\nFound {len(leads)} qualified leads")
        for prop in leads:
            print(f"  - {prop.address} (Parcel: {prop.parcel_id})")

        # Get properties with code violations
        violated_props = get_properties_with_code_violations(session, severity="Critical")
        print(f"\nFound {len(violated_props)} properties with critical violations")

        # Get statistics
        stats = get_distress_statistics(session)
        print("\nDistress Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")


def example_5_update_property():
    """Example: Update property information."""
    print("\n=== Example 5: Updating property ===")

    with db.session_scope() as session:
        prop = get_property_by_parcel_id(session, "12345-ABC-67890")
        if not prop:
            print("Property not found.")
            return

        # Update property information
        prop.lat = 27.9506
        prop.lon = -82.4572
        prop.sync_status = "pending"

        # Update owner contact info (skip trace results)
        if prop.owner:
            prop.owner.phone_1 = "813-555-1234"
            prop.owner.email_1 = "johndoe@example.com"
            prop.owner.skip_trace_success = True

        print(f"Updated property: {prop}")
        print(f"Updated owner contact info")


def example_6_foreclosure_case():
    """Example: Add a foreclosure case."""
    print("\n=== Example 6: Adding foreclosure case ===")

    with db.session_scope() as session:
        prop = get_property_by_parcel_id(session, "12345-ABC-67890")
        if not prop:
            print("Property not found.")
            return

        foreclosure = Foreclosure(
            property=prop,
            case_number="2024-CA-001234",
            plaintiff="ABC Mortgage Company",
            filing_date=date(2024, 1, 10),
            lis_pendens_date=date(2024, 1, 15),
            judgment_amount=185000.00,
            auction_date=datetime(2024, 6, 15, 10, 0, 0),
        )
        session.add(foreclosure)

        print(f"Added foreclosure: {foreclosure}")


def main():
    """Run all examples."""
    print("=" * 60)
    print("Database Usage Examples")
    print("=" * 60)

    # Check database connection
    print("\nChecking database connection...")
    if not check_connection():
        print("ERROR: Cannot connect to database!")
        print("Please check your DATABASE_URL in .env file")
        return

    print("✓ Database connection successful!")

    # Initialize database (create tables)
    print("\nInitializing database tables...")
    init_database()
    print("✓ Database tables created/verified")

    # Run examples
    try:
        example_1_create_property()
        example_2_add_distress_signals()
        example_3_calculate_distress_score()
        example_4_query_properties()
        example_5_update_property()
        example_6_foreclosure_case()

        # Show table counts
        print("\n=== Final Table Counts ===")
        counts = get_table_counts()
        for table, count in counts.items():
            print(f"  {table}: {count}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
