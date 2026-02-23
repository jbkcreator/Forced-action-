"""
Test CDS (Compliance Distress Score) data availability and scoring logic.

This script tests the scoring engine against all_parcels_with_violations.csv
to determine if we have enough data to calculate meaningful scores.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.constants import PROCESSED_DATA_DIR
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# Scoring weights (max 80 points without fines and prior violations)
WEIGHTS = {
    'violation_severity': 25.0,
    'days_open': 20.0,
    'fine_accumulation': 20.0,  # Skip - not available
    'absentee_ownership': 15.0,
    'prior_violations': 10.0,   # Skip - needs DB
    'equity_vs_repair': 10.0
}

# Delivery threshold
THRESHOLD = 70.0


def map_record_type_to_severity(record_type: str) -> int:
    """
    Map Record Type to severity points based on rule-based classification.
    
    Args:
        record_type: The violation record type from portal
        
    Returns:
        Severity points (0-25)
    """
    if pd.isna(record_type) or not record_type:
        return 0
    
    record_type_lower = str(record_type).lower()
    
    # Critical/Structural (25 points)
    if any(keyword in record_type_lower for keyword in [
        'structural condemnation',
        'unsafe',
        'condemned'
    ]):
        return 25
    
    # Major/Safety (20 points)
    if any(keyword in record_type_lower for keyword in [
        'fire marshal',
        'enforcement complaint',
        'generalized housing'
    ]):
        return 20
    
    # Minor/Aesthetic (8 points) - Check BEFORE Moderate to avoid proactive false match
    if any(keyword in record_type_lower for keyword in [
        'water enforcement',
        'fertilizer',
        'right of way'
    ]):
        return 8
    
    # Moderate/Habitual (15 points)
    if any(keyword in record_type_lower for keyword in [
        'citizen board support code',
        'proactive',
        'community outreach'
    ]):
        return 15
    
    # Administrative (3 points)
    if any(keyword in record_type_lower for keyword in [
        'consumer protection',
        'locksmith',
        'vehicle for hire',
        'trespass tow',
        'false alarm',
        'ada gas pumping'
    ]):
        return 3
    
    # Unknown - log warning and return 0
    logger.warning(f"Unmapped violation type: {record_type}")
    return 0


def calculate_days_open_score(opened_date: str, status: str) -> int:
    """
    Calculate days open / escalation score.
    
    Args:
        opened_date: Date violation was opened (MM/DD/YYYY format)
        status: Current status of violation
        
    Returns:
        Days open points (0-20)
    """
    # Check escalation status first (highest priority)
    if pd.isna(status):
        status_str = ''
    else:
        status_str = str(status).lower()
    
    # Court/judgment (20 pts)
    if any(keyword in status_str for keyword in ['judgment', 'court', 'legal']):
        return 20
    
    # Hearing scheduled (18 pts)
    if 'hearing' in status_str:
        return 18
    
    # Lien filed (16 pts)
    if 'lien' in status_str:
        return 16
    
    # Calculate days based on opened date
    if pd.isna(opened_date) or not opened_date:
        logger.warning("Opened date is missing")
        return 0
    
    try:
        opened_dt = datetime.strptime(str(opened_date).strip(), '%m/%d/%Y')
        days_open = (datetime.now() - opened_dt).days
        
        if days_open > 365:
            return 14
        elif days_open >= 181:
            return 10
        elif days_open >= 91:
            return 7
        elif days_open >= 31:
            return 4
        else:
            return 2
    except Exception as e:
        logger.warning(f"Could not parse date '{opened_date}': {e}")
        return 0


def calculate_absentee_score(owner_mailing_addr: str, owner_state: str, owner_zip: str, 
                             property_addr: str, property_zip: str) -> int:
    """
    Calculate absentee ownership score.
    
    Args:
        owner_mailing_addr: Owner's mailing address
        owner_state: Owner's state
        owner_zip: Owner's ZIP code
        property_addr: Property site address
        property_zip: Property ZIP code
        
    Returns:
        Absentee points (0-15)
    """
    # Out-of-state (15 pts)
    if not pd.isna(owner_state) and str(owner_state).strip().upper() != 'FL':
        return 15
    
    # Different ZIP in FL (8 pts)
    if not pd.isna(owner_zip) and not pd.isna(property_zip):
        owner_zip_str = str(owner_zip).strip()
        property_zip_str = str(property_zip).strip()
        if owner_zip_str != property_zip_str and owner_zip_str != '' and property_zip_str != '':
            return 8
    
    # Same address (0 pts)
    if not pd.isna(owner_mailing_addr) and not pd.isna(property_addr):
        owner_addr_norm = str(owner_mailing_addr).lower().strip()
        property_addr_norm = str(property_addr).lower().strip()
        if owner_addr_norm == property_addr_norm:
            return 0
    
    # Cannot determine (5 pts)
    return 5


def calculate_equity_score(assessed_value: float) -> int:
    """
    Calculate equity vs repair cost score.
    Since we don't have repair cost data, use assessed value as proxy.
    
    Args:
        assessed_value: Property assessed value
        
    Returns:
        Equity points (0-10)
    """
    if pd.isna(assessed_value) or assessed_value == 0:
        logger.warning("Assessed value missing")
        return 3  # Default when data missing
    
    # Use assessed value tiers as proxy for equity potential
    if assessed_value >= 300000:
        return 10  # High value = likely has equity
    elif assessed_value >= 150000:
        return 7
    elif assessed_value >= 75000:
        return 5
    else:
        return 3


def calculate_cds_score(row: pd.Series) -> dict:
    """
    Calculate CDS score for a single property.
    
    Args:
        row: DataFrame row with property and violation data
        
    Returns:
        Dictionary with score breakdown
    """
    warnings = []
    
    # Factor 1: Violation Severity (0-25 pts)
    severity_score = map_record_type_to_severity(row.get('violation_types', ''))
    
    # Factor 2: Days Open / Escalation (0-20 pts)
    days_open_score = calculate_days_open_score(
        row.get('violation_opened_dates', ''),
        row.get('violation_statuses', '')
    )
    
    # Factor 3: Fine Accumulation (SKIP - not available)
    fine_score = 0
    warnings.append("Fine data not available")
    
    # Factor 4: Absentee Ownership (0-15 pts)
    absentee_score = calculate_absentee_score(
        row.get('addr_1', ''),
        row.get('state', ''),
        row.get('zip', ''),
        row.get('site_addr', ''),
        row.get('site_zip', '')
    )
    
    # Factor 5: Prior Violations (SKIP - needs DB)
    prior_score = 0
    warnings.append("Prior violation count needs database")
    
    # Factor 6: Equity vs Repair Cost (0-10 pts)
    equity_score = calculate_equity_score(row.get('asd_val', 0))
    
    # Calculate total
    total_score = (
        severity_score +
        days_open_score +
        fine_score +
        absentee_score +
        prior_score +
        equity_score
    )
    
    # Determine if qualified for enrichment
    qualified = total_score >= THRESHOLD
    
    return {
        'folio': row.get('folio', 'N/A'),
        'address': row.get('site_addr', 'N/A'),
        'owner': row.get('owner', 'N/A'),
        'violation_type': row.get('violation_types', 'N/A'),
        'severity_score': severity_score,
        'days_open_score': days_open_score,
        'fine_score': fine_score,
        'absentee_score': absentee_score,
        'prior_score': prior_score,
        'equity_score': equity_score,
        'total_score': total_score,
        'qualified': qualified,
        'warnings': warnings
    }


def test_cds_scoring():
    """
    Test CDS scoring on all_parcels_with_violations.csv.
    """
    logger.info("=" * 60)
    logger.info("CDS Data Availability Test")
    logger.info("=" * 60)
    
    # Load data
    input_file = PROCESSED_DATA_DIR / "all_parcels_with_violations.csv"
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        logger.error("Please run match_violations_to_all_parcels.py first")
        return
    
    logger.info(f"Reading data from: {input_file}")
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} properties with violations")
    
    # Calculate scores
    logger.info("\nCalculating CDS scores...")
    scores = []
    
    for idx, row in df.iterrows():
        score_result = calculate_cds_score(row)
        scores.append(score_result)
    
    # Convert to DataFrame
    scores_df = pd.DataFrame(scores)
    
    # Save results
    output_file = PROCESSED_DATA_DIR / "cds_test_scores.csv"
    scores_df.to_csv(output_file, index=False)
    logger.info(f"\nScores saved to: {output_file}")
    
    # Summary statistics
    logger.info("=" * 60)
    logger.info("SCORING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total properties scored: {len(scores_df)}")
    logger.info(f"Qualified for enrichment (≥{THRESHOLD}): {scores_df['qualified'].sum()}")
    logger.info(f"Not qualified (<{THRESHOLD}): {(~scores_df['qualified']).sum()}")
    logger.info(f"\nScore range: {scores_df['total_score'].min():.1f} - {scores_df['total_score'].max():.1f}")
    logger.info(f"Average score: {scores_df['total_score'].mean():.1f}")
    logger.info(f"Median score: {scores_df['total_score'].median():.1f}")
    
    # Factor breakdown
    logger.info("\n" + "=" * 60)
    logger.info("FACTOR BREAKDOWN (Averages)")
    logger.info("=" * 60)
    logger.info(f"Severity (max 25):      {scores_df['severity_score'].mean():.1f}")
    logger.info(f"Days Open (max 20):     {scores_df['days_open_score'].mean():.1f}")
    logger.info(f"Fines (max 20):         {scores_df['fine_score'].mean():.1f} [NOT AVAILABLE]")
    logger.info(f"Absentee (max 15):      {scores_df['absentee_score'].mean():.1f}")
    logger.info(f"Prior Count (max 10):   {scores_df['prior_score'].mean():.1f} [NEEDS DB]")
    logger.info(f"Equity (max 10):        {scores_df['equity_score'].mean():.1f}")
    
    # Show top 5 qualified properties
    qualified_df = scores_df[scores_df['qualified']].sort_values('total_score', ascending=False)
    if len(qualified_df) > 0:
        logger.info("\n" + "=" * 60)
        logger.info("TOP QUALIFIED PROPERTIES")
        logger.info("=" * 60)
        for idx, row in qualified_df.head(5).iterrows():
            logger.info(f"\n{row['address']}")
            logger.info(f"  Owner: {row['owner']}")
            logger.info(f"  Violation: {row['violation_type']}")
            logger.info(f"  Score: {row['total_score']:.0f} (Sev:{row['severity_score']} Days:{row['days_open_score']} Abs:{row['absentee_score']} Eq:{row['equity_score']})")
    else:
        logger.warning("\nNo properties qualified for enrichment with current scoring.")
    
    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    test_cds_scoring()
