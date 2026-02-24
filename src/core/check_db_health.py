"""
Database health check script.

This script performs comprehensive health checks on the database:
- Connection status
- Table existence and counts
- Data integrity (foreign keys, relationships)
- Duplicate detection
- Data quality metrics
- Match rate analysis

Usage:
    python scripts/check_db_health.py
    python scripts/check_db_health.py --detailed
    python scripts/check_db_health.py --fix-issues
"""

import argparse
import logging
import sys
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError

from src.core.database import get_db_context, check_connection
from src.core.models import (
    Base, Property, Owner, Financial, CodeViolation, LegalAndLien,
    Deed, LegalProceeding, TaxDelinquency, Foreclosure, BuildingPermit, 
    Incident, DistressScore
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONNECTION CHECKS
# ============================================================================

def check_database_connection() -> bool:
    """Check if database connection is working."""
    logger.info("=" * 70)
    logger.info("DATABASE CONNECTION CHECK")
    logger.info("=" * 70)
    
    try:
        if check_connection():
            logger.info("✓ Database connection successful")
            return True
        else:
            logger.error("✗ Database connection failed")
            return False
    except Exception as e:
        logger.error(f"✗ Database connection error: {e}")
        return False


# ============================================================================
# SCHEMA CHECKS
# ============================================================================

def check_tables_exist() -> Tuple[bool, Dict[str, bool]]:
    """Check if all required tables exist."""
    logger.info("\n" + "=" * 70)
    logger.info("TABLE EXISTENCE CHECK")
    logger.info("=" * 70)
    
    required_tables = [
        'properties', 'owners', 'financials', 'code_violations',
        'legal_and_liens', 'deeds', 'legal_proceedings',
        'tax_delinquencies', 'foreclosures',
        'building_permits', 'incidents', 'distress_scores'
    ]
    
    table_status = {}
    all_exist = True
    
    try:
        with get_db_context() as session:
            inspector = inspect(session.bind)
            existing_tables = inspector.get_table_names()
            
            for table in required_tables:
                exists = table in existing_tables
                table_status[table] = exists
                
                if exists:
                    logger.info(f"  ✓ {table:<25} EXISTS")
                else:
                    logger.error(f"  ✗ {table:<25} MISSING")
                    all_exist = False
        
        if all_exist:
            logger.info("\n✓ All required tables exist")
        else:
            logger.error("\n✗ Some tables are missing. Run: python scripts/insert_to_database.py --init-db")
        
        return all_exist, table_status
        
    except Exception as e:
        logger.error(f"✗ Error checking tables: {e}")
        return False, {}


# ============================================================================
# DATA COUNTS
# ============================================================================

def get_table_counts() -> Dict[str, int]:
    """Get record counts for all tables."""
    logger.info("\n" + "=" * 70)
    logger.info("TABLE RECORD COUNTS")
    logger.info("=" * 70)
    
    counts = {}
    
    try:
        with get_db_context() as session:
            tables_models = {
                'properties': Property,
                'owners': Owner,
                'financials': Financial,
                'code_violations': CodeViolation,
                'legal_and_liens': LegalAndLien,
                'deeds': Deed,
                'legal_proceedings': LegalProceeding,
                'tax_delinquencies': TaxDelinquency,
                'foreclosures': Foreclosure,
                'building_permits': BuildingPermit,
                'incidents': Incident,
                'distress_scores': DistressScore,
            }
            
            total_records = 0
            
            for table_name, model in tables_models.items():
                count = session.query(model).count()
                counts[table_name] = count
                total_records += count
                logger.info(f"  {table_name:<25} {count:>8,} records")
            
            logger.info(f"\n  {'TOTAL':<25} {total_records:>8,} records")
            
        return counts
        
    except Exception as e:
        logger.error(f"✗ Error getting counts: {e}")
        return {}


# ============================================================================
# DATA INTEGRITY CHECKS
# ============================================================================

def check_foreign_key_integrity() -> Dict[str, int]:
    """Check for orphaned records (foreign key violations)."""
    logger.info("\n" + "=" * 70)
    logger.info("FOREIGN KEY INTEGRITY CHECK")
    logger.info("=" * 70)
    
    issues = {}
    
    try:
        with get_db_context() as session:
            # Check owners without properties
            orphaned_owners = session.query(Owner).filter(
                ~Owner.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_owners'] = orphaned_owners
            
            # Check financials without properties
            orphaned_financials = session.query(Financial).filter(
                ~Financial.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_financials'] = orphaned_financials
            
            # Check violations without properties
            orphaned_violations = session.query(CodeViolation).filter(
                ~CodeViolation.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_violations'] = orphaned_violations
            
            # Check liens without properties
            orphaned_liens = session.query(LegalAndLien).filter(
                ~LegalAndLien.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_liens'] = orphaned_liens
            
            # Check tax delinquencies without properties
            orphaned_tax = session.query(TaxDelinquency).filter(
                ~TaxDelinquency.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_tax'] = orphaned_tax
            
            # Check foreclosures without properties
            orphaned_foreclosures = session.query(Foreclosure).filter(
                ~Foreclosure.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_foreclosures'] = orphaned_foreclosures
            
            # Check permits without properties
            orphaned_permits = session.query(BuildingPermit).filter(
                ~BuildingPermit.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_permits'] = orphaned_permits
            
            # Check incidents without properties
            orphaned_incidents = session.query(Incident).filter(
                ~Incident.property_id.in_(session.query(Property.id))
            ).count()
            issues['orphaned_incidents'] = orphaned_incidents
            
            total_orphans = sum(issues.values())
            
            for issue_type, count in issues.items():
                if count > 0:
                    logger.warning(f"  ⚠ {issue_type:<30} {count:>6} orphaned records")
                else:
                    logger.info(f"  ✓ {issue_type:<30} {count:>6} orphaned records")
            
            if total_orphans == 0:
                logger.info("\n✓ No foreign key integrity issues found")
            else:
                logger.warning(f"\n⚠ Found {total_orphans} total orphaned records")
        
        return issues
        
    except Exception as e:
        logger.error(f"✗ Error checking foreign keys: {e}")
        return {}


def check_duplicate_records() -> Dict[str, int]:
    """Check for duplicate records based on unique constraints."""
    logger.info("\n" + "=" * 70)
    logger.info("DUPLICATE RECORD CHECK")
    logger.info("=" * 70)
    
    duplicates = {}
    
    try:
        with get_db_context() as session:
            # Check duplicate properties (parcel_id)
            dup_properties = session.execute(text("""
                SELECT parcel_id, COUNT(*) as count
                FROM properties
                GROUP BY parcel_id
                HAVING COUNT(*) > 1
            """)).fetchall()
            duplicates['properties'] = len(dup_properties)
            
            # Check duplicate violations (case_number)
            dup_violations = session.execute(text("""
                SELECT case_number, COUNT(*) as count
                FROM code_violations
                GROUP BY case_number
                HAVING COUNT(*) > 1
            """)).fetchall()
            duplicates['violations'] = len(dup_violations)
            
            # Check duplicate liens (instrument_number)
            dup_liens = session.execute(text("""
                SELECT instrument_number, COUNT(*) as count
                FROM legal_and_liens
                GROUP BY instrument_number
                HAVING COUNT(*) > 1
            """)).fetchall()
            duplicates['liens'] = len(dup_liens)
            
            # Check duplicate foreclosures (case_number)
            dup_foreclosures = session.execute(text("""
                SELECT case_number, COUNT(*) as count
                FROM foreclosures
                GROUP BY case_number
                HAVING COUNT(*) > 1
            """)).fetchall()
            duplicates['foreclosures'] = len(dup_foreclosures)
            
            # Check duplicate permits (permit_number)
            dup_permits = session.execute(text("""
                SELECT permit_number, COUNT(*) as count
                FROM building_permits
                GROUP BY permit_number
                HAVING COUNT(*) > 1
            """)).fetchall()
            duplicates['permits'] = len(dup_permits)
            
            total_duplicates = sum(duplicates.values())
            
            for table, count in duplicates.items():
                if count > 0:
                    logger.warning(f"  ⚠ {table:<25} {count:>6} duplicate groups")
                else:
                    logger.info(f"  ✓ {table:<25} {count:>6} duplicates")
            
            if total_duplicates == 0:
                logger.info("\n✓ No duplicate records found")
            else:
                logger.warning(f"\n⚠ Found {total_duplicates} duplicate groups across tables")
        
        return duplicates
        
    except Exception as e:
        logger.error(f"✗ Error checking duplicates: {e}")
        return {}


# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def check_data_quality() -> Dict[str, Dict[str, int]]:
    """Check data quality metrics."""
    logger.info("\n" + "=" * 70)
    logger.info("DATA QUALITY CHECK")
    logger.info("=" * 70)
    
    quality_metrics = {}
    
    try:
        with get_db_context() as session:
            # Properties data quality
            total_properties = session.query(Property).count()
            if total_properties > 0:
                properties_no_address = session.query(Property).filter(
                    (Property.address == None) | (Property.address == '')
                ).count()
                properties_no_parcel = session.query(Property).filter(
                    (Property.parcel_id == None) | (Property.parcel_id == '')
                ).count()
                
                quality_metrics['properties'] = {
                    'total': total_properties,
                    'missing_address': properties_no_address,
                    'missing_parcel_id': properties_no_parcel,
                }
                
                logger.info(f"\n  Properties ({total_properties} total):")
                logger.info(f"    Missing address:   {properties_no_address:>6} ({properties_no_address/total_properties*100:.1f}%)")
                logger.info(f"    Missing parcel_id: {properties_no_parcel:>6} ({properties_no_parcel/total_properties*100:.1f}%)")
            
            # Owners data quality
            total_owners = session.query(Owner).count()
            if total_owners > 0:
                owners_no_name = session.query(Owner).filter(
                    (Owner.name == None) | (Owner.name == '')
                ).count()
                absentee_owners = session.query(Owner).filter(
                    Owner.is_absentee == True
                ).count()
                
                quality_metrics['owners'] = {
                    'total': total_owners,
                    'missing_name': owners_no_name,
                    'absentee': absentee_owners,
                }
                
                logger.info(f"\n  Owners ({total_owners} total):")
                logger.info(f"    Missing name:      {owners_no_name:>6} ({owners_no_name/total_owners*100:.1f}%)")
                logger.info(f"    Absentee owners:   {absentee_owners:>6} ({absentee_owners/total_owners*100:.1f}%)")
            
            # Violations data quality
            total_violations = session.query(CodeViolation).count()
            if total_violations > 0:
                violations_open = session.query(CodeViolation).filter(
                    CodeViolation.status.in_(['OPEN', 'In Process', 'In Review'])
                ).count()
                violations_closed = session.query(CodeViolation).filter(
                    CodeViolation.status.in_(['CLOSED', 'Closed', 'Dismissed', 'Complied'])
                ).count()
                
                quality_metrics['violations'] = {
                    'total': total_violations,
                    'open': violations_open,
                    'closed': violations_closed,
                }
                
                logger.info(f"\n  Code Violations ({total_violations} total):")
                logger.info(f"    Open:              {violations_open:>6} ({violations_open/total_violations*100:.1f}%)")
                logger.info(f"    Closed:            {violations_closed:>6} ({violations_closed/total_violations*100:.1f}%)")
            
            # Liens/Judgments breakdown
            total_liens = session.query(LegalAndLien).count()
            if total_liens > 0:
                liens_by_type = session.execute(text("""
                    SELECT record_type, COUNT(*) as count
                    FROM legal_and_liens
                    GROUP BY record_type
                    ORDER BY count DESC
                """)).fetchall()
                
                logger.info(f"\n  Legal & Liens ({total_liens} total):")
                for record_type, count in liens_by_type:
                    logger.info(f"    {record_type or 'Unknown':<25} {count:>6} ({count/total_liens*100:.1f}%)")
        
        return quality_metrics
        
    except Exception as e:
        logger.error(f"✗ Error checking data quality: {e}")
        return {}


# ============================================================================
# RELATIONSHIP CHECKS
# ============================================================================

def check_property_relationships() -> Dict[str, int]:
    """Check property relationship statistics."""
    logger.info("\n" + "=" * 70)
    logger.info("PROPERTY RELATIONSHIP ANALYSIS")
    logger.info("=" * 70)
    
    stats = {}
    
    try:
        with get_db_context() as session:
            total_properties = session.query(Property).count()
            
            if total_properties == 0:
                logger.warning("  ⚠ No properties in database")
                return {}
            
            # Properties with distress indicators
            props_with_violations = session.query(Property).join(CodeViolation).distinct().count()
            props_with_liens = session.query(Property).join(LegalAndLien).distinct().count()
            props_with_tax = session.query(Property).join(TaxDelinquency).distinct().count()
            props_with_foreclosures = session.query(Property).join(Foreclosure).distinct().count()
            props_with_permits = session.query(Property).join(BuildingPermit).distinct().count()
            props_with_incidents = session.query(Property).join(Incident).distinct().count()
            
            stats = {
                'total_properties': total_properties,
                'with_violations': props_with_violations,
                'with_liens': props_with_liens,
                'with_tax': props_with_tax,
                'with_foreclosures': props_with_foreclosures,
                'with_permits': props_with_permits,
                'with_incidents': props_with_incidents,
            }
            
            logger.info(f"\n  Total Properties: {total_properties}")
            logger.info(f"\n  Properties with distress indicators:")
            logger.info(f"    Code Violations:   {props_with_violations:>6} ({props_with_violations/total_properties*100:.1f}%)")
            logger.info(f"    Liens/Judgments:   {props_with_liens:>6} ({props_with_liens/total_properties*100:.1f}%)")
            logger.info(f"    Tax Delinquent:    {props_with_tax:>6} ({props_with_tax/total_properties*100:.1f}%)")
            logger.info(f"    Foreclosures:      {props_with_foreclosures:>6} ({props_with_foreclosures/total_properties*100:.1f}%)")
            logger.info(f"    Building Permits:  {props_with_permits:>6} ({props_with_permits/total_properties*100:.1f}%)")
            logger.info(f"    Incidents:         {props_with_incidents:>6} ({props_with_incidents/total_properties*100:.1f}%)")
            
            # Properties with multiple distress signals
            props_multiple_signals = session.execute(text("""
                SELECT property_id, COUNT(DISTINCT signal_type) as signal_count
                FROM (
                    SELECT property_id, 'violation' as signal_type FROM code_violations
                    UNION ALL
                    SELECT property_id, 'lien' as signal_type FROM legal_and_liens
                    UNION ALL
                    SELECT property_id, 'tax' as signal_type FROM tax_delinquencies
                    UNION ALL
                    SELECT property_id, 'foreclosure' as signal_type FROM foreclosures
                ) combined
                GROUP BY property_id
                HAVING COUNT(DISTINCT signal_type) >= 2
            """)).fetchall()
            
            stats['multiple_signals'] = len(props_multiple_signals)
            
            logger.info(f"\n  Properties with multiple distress signals: {len(props_multiple_signals)} ({len(props_multiple_signals)/total_properties*100:.1f}%)")
        
        return stats
        
    except Exception as e:
        logger.error(f"✗ Error checking relationships: {e}")
        return {}


# ============================================================================
# OVERALL HEALTH SCORE
# ============================================================================

def calculate_health_score(
    connection_ok: bool,
    tables_ok: bool,
    orphaned_records: Dict[str, int],
    duplicates: Dict[str, int]
) -> Tuple[int, str]:
    """Calculate overall database health score (0-100)."""
    logger.info("\n" + "=" * 70)
    logger.info("DATABASE HEALTH SCORE")
    logger.info("=" * 70)
    
    score = 100
    issues = []
    
    # Connection check (critical)
    if not connection_ok:
        score -= 50
        issues.append("Database connection failed")
    
    # Tables check (critical)
    if not tables_ok:
        score -= 30
        issues.append("Missing required tables")
    
    # Orphaned records (moderate)
    total_orphans = sum(orphaned_records.values())
    if total_orphans > 0:
        score -= min(10, total_orphans // 10)
        issues.append(f"{total_orphans} orphaned records")
    
    # Duplicates (moderate)
    total_duplicates = sum(duplicates.values())
    if total_duplicates > 0:
        score -= min(10, total_duplicates // 5)
        issues.append(f"{total_duplicates} duplicate groups")
    
    score = max(0, score)
    
    # Determine status
    if score >= 90:
        status = "EXCELLENT"
        emoji = "✓"
    elif score >= 70:
        status = "GOOD"
        emoji = "✓"
    elif score >= 50:
        status = "FAIR"
        emoji = "⚠"
    elif score >= 30:
        status = "POOR"
        emoji = "⚠"
    else:
        status = "CRITICAL"
        emoji = "✗"
    
    logger.info(f"\n  {emoji} Overall Health Score: {score}/100 ({status})")
    
    if issues:
        logger.info(f"\n  Issues found:")
        for issue in issues:
            logger.info(f"    • {issue}")
    else:
        logger.info(f"\n  ✓ No major issues found")
    
    return score, status


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check database health and integrity",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed analysis (slower)'
    )
    
    parser.add_argument(
        '--fix-issues',
        action='store_true',
        help='Attempt to fix common issues (EXPERIMENTAL)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info(f"DATABASE HEALTH CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # Run checks
    connection_ok = check_database_connection()
    
    if not connection_ok:
        logger.error("\n✗ Cannot proceed without database connection")
        sys.exit(1)
    
    tables_ok, table_status = check_tables_exist()
    table_counts = get_table_counts()
    orphaned_records = check_foreign_key_integrity()
    duplicates = check_duplicate_records()
    
    if args.detailed:
        data_quality = check_data_quality()
        property_stats = check_property_relationships()
    
    # Calculate health score
    score, status = calculate_health_score(
        connection_ok,
        tables_ok,
        orphaned_records,
        duplicates
    )
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("HEALTH CHECK COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Database Status: {status} ({score}/100)")
    logger.info("=" * 70 + "\n")
    
    # Exit with appropriate code
    if score >= 70:
        sys.exit(0)
    elif score >= 30:
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
