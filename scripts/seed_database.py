"""
Database seed script for creating test/sample data.
Useful for development and testing.
"""

from datetime import datetime, date, timedelta
import random

from src.core.database import db, init_database
from src.core.models import (
    Property,
    Owner,
    Financial,
    CodeViolation,
    LegalAndLien,
    TaxDelinquency,
    Foreclosure,
    BuildingPermit,
    Incident,
    DistressScore,
)


def seed_sample_properties(num_properties: int = 10):
    """Create sample properties with various distress signals."""
    print(f"Seeding {num_properties} sample properties...")

    cities = ["Tampa", "Temple Terrace", "Plant City"]
    property_types = ["SFH", "Multi-Family", "Commercial", "Condo"]
    owner_types = ["Individual", "LLC", "Trust", "Estate", "Corporate"]
    jurisdictions = ["Tampa", "Temple Terrace", "Plant City", "Unincorporated"]

    with db.session_scope() as session:
        for i in range(num_properties):
            # Create property
            prop = Property(
                parcel_id=f"TEST-{i+1:06d}-{random.randint(100, 999)}",
                address=f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Maple'])} {'Street' if random.random() > 0.5 else 'Avenue'}",
                city=random.choice(cities),
                state="FL",
                zip=f"336{random.randint(0, 9):02d}",
                jurisdiction=random.choice(jurisdictions),
                property_type=random.choice(property_types),
                year_built=random.randint(1960, 2020),
                sq_ft=random.randint(800, 3500),
                beds=random.randint(2, 5),
                baths=random.randint(1, 3) + random.choice([0, 0.5]),
                lot_size=random.randint(5000, 15000),
            )

            # Create owner
            owner = Owner(
                property=prop,
                owner_name=f"Owner {i+1} {'LLC' if random.random() > 0.6 else ''}",
                owner_type=random.choice(owner_types),
                absentee_status=random.choice(
                    ["In-County", "Out-of-County", "Out-of-State"]
                ),
                ownership_years=random.randint(1, 30),
                skip_trace_success=random.random() > 0.3,
            )

            if owner.skip_trace_success:
                owner.phone_1 = f"813-555-{random.randint(1000, 9999)}"
                owner.email_1 = f"owner{i+1}@example.com"

            # Create financial
            market_value = random.randint(150000, 500000)
            financial = Financial(
                property=prop,
                assessed_value_mkt=market_value,
                assessed_value_tax=market_value * 0.95,
                last_sale_price=market_value * random.uniform(0.7, 0.9),
                last_sale_date=date.today() - timedelta(days=random.randint(365, 3650)),
                annual_tax_amount=market_value * 0.014,
                homestead_exempt=random.random() > 0.4,
                equity_pct=random.uniform(20, 80),
            )

            session.add(prop)

            # Randomly add distress signals
            # Code violations (40% chance)
            if random.random() > 0.6:
                violation = CodeViolation(
                    property=prop,
                    record_number=f"CE-2024-{random.randint(100000, 999999)}",
                    violation_type=random.choice(
                        [
                            "Overgrown Vegetation",
                            "Structural Damage",
                            "Trash Accumulation",
                            "Missing Permits",
                        ]
                    ),
                    opened_date=date.today() - timedelta(days=random.randint(30, 365)),
                    status=random.choice(["Open", "In Progress", "Closed"]),
                    severity_tier=random.choice(["Critical", "Major", "Minor"]),
                    fine_amount=random.uniform(100, 5000),
                )
                session.add(violation)

            # Tax delinquency (30% chance)
            if random.random() > 0.7:
                tax_delinq = TaxDelinquency(
                    property=prop,
                    tax_year=random.randint(2020, 2023),
                    years_delinquent=random.randint(1, 4),
                    total_amount_due=random.uniform(2000, 15000),
                )
                session.add(tax_delinq)

            # HOA lien (25% chance)
            if random.random() > 0.75:
                hoa_lien = LegalAndLien(
                    property=prop,
                    record_type="Lien",
                    instrument_number=f"HOA-{year}-{random.randint(10000, 99999)}",
                    creditor=f"Sample HOA #{random.randint(1, 50)}",
                    debtor=owner.owner_name,
                    filing_date=date.today() - timedelta(days=random.randint(90, 730)),
                    amount=random.uniform(1000, 10000),
                    document_type="HOA Lien",
                    legal_description="HOA dues and assessments lien"
                )
                session.add(hoa_lien)

            # Foreclosure (15% chance)
            if random.random() > 0.85:
                foreclosure = Foreclosure(
                    property=prop,
                    case_number=f"2024-CA-{random.randint(100000, 999999)}",
                    plaintiff=f"Bank #{random.randint(1, 20)}",
                    filing_date=date.today() - timedelta(days=random.randint(60, 365)),
                    judgment_amount=random.uniform(100000, 300000),
                )
                session.add(foreclosure)

            # Calculate distress score
            base_score = random.uniform(40, 90)
            score = DistressScore(
                property=prop,
                score_date=datetime.utcnow(),
                final_cds_score=base_score,
                lead_tier=(
                    "Ultra Platinum"
                    if base_score >= 85
                    else "Platinum"
                    if base_score >= 75
                    else "Gold"
                    if base_score >= 65
                    else "Silver"
                    if base_score >= 55
                    else "Bronze"
                ),
                urgency_level=(
                    "Immediate"
                    if base_score >= 85
                    else "High"
                    if base_score >= 70
                    else "Medium"
                    if base_score >= 55
                    else "Low"
                ),
                multiplier=random.uniform(1.0, 2.0),
                qualified=base_score >= 60,
                distress_types=random.sample(
                    ["Code", "Tax", "HOA", "Foreclosure", "Liens"], k=random.randint(1, 3)
                ),
                factor_scores={
                    "financial_distress": random.uniform(5, 20),
                    "code_violations": random.uniform(5, 20),
                    "tax_delinquency": random.uniform(5, 20),
                    "legal_issues": random.uniform(5, 20),
                    "property_condition": random.uniform(5, 20),
                    "owner_profile": random.uniform(5, 20),
                },
            )
            session.add(score)

        print(f"✓ Successfully seeded {num_properties} properties with distress signals")


def clear_all_data():
    """Clear all data from all tables. USE WITH CAUTION!"""
    print("WARNING: Clearing all data from database...")
    from src.core.models import (
        Property,
        Owner,
        Financial,
        CodeViolation,
        LegalAndLien,
        TaxDelinquency,
        Foreclosure,
        BuildingPermit,
        Incident,
        DistressScore,
    )

    with db.session_scope() as session:
        # Delete in reverse order of dependencies
        session.query(DistressScore).delete()
        session.query(Incident).delete()
        session.query(BuildingPermit).delete()
        session.query(Foreclosure).delete()
        session.query(TaxDelinquency).delete()
        session.query(LegalAndLien).delete()
        session.query(CodeViolation).delete()
        session.query(Financial).delete()
        session.query(Owner).delete()
        session.query(Property).delete()

    print("✓ All data cleared")


def main():
    """Main seeding function."""
    print("=" * 60)
    print("Database Seeding Script")
    print("=" * 60)

    # Initialize database
    print("\nInitializing database...")
    init_database()
    print("✓ Database initialized")

    # Clear existing data (optional)
    clear_choice = input(
        "\nClear existing data before seeding? (yes/no) [no]: "
    ).lower()
    if clear_choice == "yes":
        clear_all_data()

    # Seed properties
    num_properties = input("\nHow many properties to seed? [10]: ")
    num_properties = int(num_properties) if num_properties else 10

    seed_sample_properties(num_properties)

    # Show final counts
    from src.core.database import get_table_counts

    print("\n=== Final Table Counts ===")
    counts = get_table_counts()
    for table, count in counts.items():
        print(f"  {table}: {count}")

    print("\n" + "=" * 60)
    print("Seeding completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
