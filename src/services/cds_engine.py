"""
CDS (Compliance Distress Score) scoring service.

Calculates distress scores for properties based on code violations,
ownership status, and financial factors using database queries.
\
DATABASE RELATIONSHIP:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DistressScore has a 1:Many relationship with Property:
  • One property can have multiple DistressScore records (historical tracking)
  • UPSERT logic prevents duplicates: Only ONE score per property per day
  • When violations change, existing today's score is UPDATED (not duplicated)
  • Historical tracking preserved: Each day gets one score record
  
Example timeline for property #123:
  Feb 1: Score = 65 (2 violations)
  Feb 5: Score = 72 (3 violations) ← New record (different day)
  Feb 5: Score = 75 (4 violations) ← UPDATED same record (same day)
  Feb 10: Score = 80 (5 violations) ← New record (different day)

SCORING METHODOLOGY (100 points total):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Violation Severity (25 points)
   - Critical/Structural: 25 pts (condemned, unsafe)
   - Major/Safety: 20 pts (fire marshal, enforcement)
   - Moderate/Habitual: 15 pts (proactive code)
   - Minor/Aesthetic: 8 pts (water, fertilizer)
   - Administrative: 3 pts (permits, alarms)

2. Days Open (20 points)
   - Court/Judgment Status: 20 pts
   - Hearing Scheduled: 18 pts
   - Lien Filed: 16 pts
   - 365+ days open: 14 pts
   - 181-364 days: 10 pts
   - 91-180 days: 7 pts
   - 31-90 days: 4 pts
   - <30 days: 2 pts

3. Violation Persistence (20 points) [BEHAVIORAL DISTRESS]
   Component A - Days Open (8 pts):
     • 365+ days: 8 pts
     • 180-364 days: 6 pts
     • 90-179 days: 4 pts
     • 30-89 days: 2 pts
     • <30 days: 1 pt
   
   Component B - Escalation Status (8 pts):
     • Judgment/Court: 8 pts
     • Hearing Scheduled: 6 pts
     • Lien Filed: 5 pts
     • Notice Issued: 2 pts
   
   Component C - Repeat Violations (4 pts):
     • 5+ violations: 4 pts
     • 3-4 violations: 3 pts
     • 2 violations: 2 pts

4. Absentee Ownership (15 points)
   - Out-of-State: 15 pts
   - Out-of-County: 8 pts
   - In-County: 0 pts

5. Prior Violations (10 points)
   - 5+ violations: 10 pts
   - 3-4 violations: 7 pts
   - 2 violations: 4 pts
   - 1 violation: 0 pts

6. Equity vs Repair (10 points)
   - $300K+ assessed value: 10 pts (high equity potential)
   - $150K-$300K: 7 pts
   - $75K-$150K: 5 pts
   - <$75K: 3 pts

DELIVERY TIERS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Ultra Platinum: 85-100 (Immediate urgency)
• Platinum: 75-84 (High urgency)
• Gold: 65-74 (Medium urgency)
• Silver: 55-64 (Medium-Low urgency)
• Bronze: 0-54 (Low urgency)

Minimum CDS Threshold: 70 points
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
from sqlalchemy.orm import Session

from src.core.models import Property, Owner, Financial, CodeViolation, DistressScore

logger = logging.getLogger(__name__)


# Scoring weights (max 100 points)
SCORING_WEIGHTS = {
    'violation_severity': 25.0,
    'days_open': 20.0,
    'violation_persistence': 20.0,  # Behavioral distress: days open + escalation + repeat pattern
    'absentee_ownership': 15.0,
    'prior_violations': 10.0,
    'equity_vs_repair': 10.0
}

# Delivery threshold
CDS_THRESHOLD = 70.0


class CDSCalculator:
    """Calculator for Compliance Distress Scores."""
    
    def __init__(self, session: Session):
        """
        Initialize CDS calculator.
        
        Args:
            session: SQLAlchemy database session
        """
        self.session = session
    
    def calculate_violation_severity(self, violation: CodeViolation) -> int:
        """
        Calculate severity score based on violation type.
        
        Args:
            violation: CodeViolation record
            
        Returns:
            Severity points (0-25)
        """
        if not violation.violation_type:
            return 0
        
        violation_type_lower = violation.violation_type.lower()
        
        # Critical/Structural (25 points)
        if any(keyword in violation_type_lower for keyword in [
            'structural condemnation',
            'unsafe',
            'condemned'
        ]):
            logger.debug(f"[Severity] Critical/Structural violation: '{violation.violation_type}' = 25 pts")
            return 25
        
        # Major/Safety (20 points)
        if any(keyword in violation_type_lower for keyword in [
            'fire marshal',
            'enforcement complaint',
            'generalized housing'
        ]):
            logger.debug(f"[Severity] Major/Safety violation: '{violation.violation_type}' = 20 pts")
            return 20
        
        # Minor/Aesthetic (8 points)
        if any(keyword in violation_type_lower for keyword in [
            'water enforcement',
            'fertilizer',
            'right of way'
        ]):
            logger.debug(f"[Severity] Minor/Aesthetic violation: '{violation.violation_type}' = 8 pts")
            return 8
        
        # Moderate/Habitual (15 points)
        if any(keyword in violation_type_lower for keyword in [
            'citizen board support code',
            'proactive',
            'community outreach'
        ]):
            logger.debug(f"[Severity] Moderate/Habitual violation: '{violation.violation_type}' = 15 pts")
            return 15
        
        # Administrative (3 points)
        if any(keyword in violation_type_lower for keyword in [
            'consumer protection',
            'locksmith',
            'vehicle for hire',
            'trespass tow',
            'false alarm',
            'ada gas pumping'
        ]):
            logger.debug(f"[Severity] Administrative violation: '{violation.violation_type}' = 3 pts")
            return 3
        
        # Unknown type
        logger.warning(f"[Severity] UNMAPPED violation type: '{violation.violation_type}' = 0 pts")
        return 0
    
    def calculate_days_open_score(self, violation: CodeViolation) -> int:
        """
        Calculate score based on how long violation has been open.
        
        Args:
            violation: CodeViolation record
            
        Returns:
            Days open points (0-20)
        """
        # Check escalation status first (highest priority)
        if violation.status:
            status_lower = violation.status.lower()
            
            # Court/judgment (20 pts)
            if any(keyword in status_lower for keyword in ['judgment', 'court', 'legal']):
                logger.debug(f"[Days Open] Court/Judgment status: '{violation.status}' = 20 pts")
                return 20
            
            # Hearing scheduled (18 pts)
            if 'hearing' in status_lower:
                logger.debug(f"[Days Open] Hearing scheduled: '{violation.status}' = 18 pts")
                return 18
            
            # Lien filed (16 pts)
            if 'lien' in status_lower or violation.is_lien:
                logger.debug(f"[Days Open] Lien filed: '{violation.status}' = 16 pts")
                return 16
        
        # Calculate based on opened date
        if not violation.opened_date:
            logger.debug(f"[Days Open] No opened_date available = 0 pts")
            return 0
        
        days_open = (datetime.now().date() - violation.opened_date).days
        
        if days_open > 365:
            logger.debug(f"[Days Open] {days_open} days (>365) = 14 pts")
            return 14
        elif days_open >= 181:
            logger.debug(f"[Days Open] {days_open} days (181-364) = 10 pts")
            return 10
        elif days_open >= 91:
            logger.debug(f"[Days Open] {days_open} days (91-180) = 7 pts")
            return 7
        elif days_open >= 31:
            logger.debug(f"[Days Open] {days_open} days (31-90) = 4 pts")
            return 4
        else:
            logger.debug(f"[Days Open] {days_open} days (<31) = 2 pts")
            return 2
    
    def calculate_absentee_score(self, owner: Owner, property: Property) -> int:
        """
        Calculate absentee ownership score.
        
        Args:
            owner: Owner record
            property: Property record
            
        Returns:
            Absentee points (0-15)
        """
        # Use pre-calculated absentee status if available
        if owner.absentee_status:
            status = owner.absentee_status
            if status == 'Out-of-State':
                logger.debug(f"[Absentee] Out-of-State owner = 15 pts")
                return 15
            elif status == 'Out-of-County':
                logger.debug(f"[Absentee] Out-of-County owner = 8 pts")
                return 8
            elif status == 'In-County':
                logger.debug(f"[Absentee] In-County owner = 0 pts")
                return 0
        
        # Fallback to manual calculation if status not set
        if owner.mailing_address and property.address:
            # Simple comparison - could be enhanced
            if owner.mailing_address.upper() == property.address.upper():
                return 0
            
            # Check if different zip codes
            if property.zip:
                # Extract zip from mailing address
                import re
                zip_match = re.search(r'\b\d{5}\b', owner.mailing_address)
                if zip_match:
                    owner_zip = zip_match.group()
                    if owner_zip != property.zip[:5]:
                        return 8
        
        # Cannot determine - default score
        logger.debug(f"[Absentee] Cannot determine status = 5 pts (default)")
        return 5
    
    def calculate_prior_violations_score(self, property: Property) -> int:
        """
        Calculate score based on prior violation count.
        
        Args:
            property: Property record
            
        Returns:
            Prior violations points (0-10)
        """
        violation_count = len(property.code_violations)
        
        if violation_count >= 5:
            logger.debug(f"[Prior Violations] {violation_count} violations (5+) = 10 pts")
            return 10
        elif violation_count >= 3:
            logger.debug(f"[Prior Violations] {violation_count} violations (3-4) = 7 pts")
            return 7
        elif violation_count >= 2:
            logger.debug(f"[Prior Violations] {violation_count} violations (2) = 4 pts")
            return 4
        elif violation_count == 1:
            logger.debug(f"[Prior Violations] {violation_count} violation (1) = 0 pts")
            return 0
        else:
            logger.debug(f"[Prior Violations] {violation_count} violations = 0 pts")
            return 0
    
    def calculate_equity_score(self, financial: Optional[Financial]) -> int:
        """
        Calculate equity vs repair cost score.
        
        Uses assessed value as proxy for equity potential.
        
        Args:
            financial: Financial record (may be None)
            
        Returns:
            Equity points (0-10)
        """
        if not financial or not financial.assessed_value_mkt:
            logger.debug(f"[Equity] No financial data = 3 pts (default)")
            return 3  # Default when data missing
        
        assessed_value = float(financial.assessed_value_mkt)
        
        if assessed_value >= 300000:
            logger.debug(f"[Equity] Assessed value ${assessed_value:,.0f} (300K+) = 10 pts")
            return 10  # High value = likely has equity
        elif assessed_value >= 150000:
            logger.debug(f"[Equity] Assessed value ${assessed_value:,.0f} (150K-300K) = 7 pts")
            return 7
        elif assessed_value >= 75000:
            logger.debug(f"[Equity] Assessed value ${assessed_value:,.0f} (75K-150K) = 5 pts")
            return 5
        else:
            logger.debug(f"[Equity] Assessed value ${assessed_value:,.0f} (<75K) = 3 pts")
            return 3
    
    def calculate_violation_persistence_score(self, violation: CodeViolation, property: Property) -> int:
        """
        Calculate Violation Persistence Score based on owner inaction patterns.
        
        This replaces fine accumulation (unavailable) with behavioral indicators:
        - Days violation has been open (8 points max)
        - Escalation status (court/hearing/lien) (8 points max)
        - Repeat violations at same property (4 points max)
        
        Args:
            violation: Current CodeViolation record
            property: Property record (for repeat violation check)
            
        Returns:
            Persistence points (0-20)
        """
        persistence_score = 0
        
        # Component 1: Days Open (8 points max)
        if violation.opened_date:
            days_open = (datetime.now().date() - violation.opened_date).days
            
            if days_open >= 365:
                persistence_score += 8
                logger.debug(f"[Persistence-DaysOpen] {days_open} days (365+) +8 pts")
            elif days_open >= 180:
                persistence_score += 6
                logger.debug(f"[Persistence-DaysOpen] {days_open} days (180-364) +6 pts")
            elif days_open >= 90:
                persistence_score += 4
                logger.debug(f"[Persistence-DaysOpen] {days_open} days (90-179) +4 pts")
            elif days_open >= 30:
                persistence_score += 2
                logger.debug(f"[Persistence-DaysOpen] {days_open} days (30-89) +2 pts")
            else:
                persistence_score += 1
                logger.debug(f"[Persistence-DaysOpen] {days_open} days (<30) +1 pt")
        else:
            logger.debug(f"[Persistence-DaysOpen] No opened_date +0 pts")
        
        # Component 2: Escalation Status (8 points max)
        if violation.status:
            status_lower = violation.status.lower()
            
            if any(keyword in status_lower for keyword in ['judgment', 'court', 'legal action']):
                persistence_score += 8
                logger.debug(f"[Persistence-Escalation] Judgment/Court status +8 pts")
            elif 'hearing' in status_lower or 'scheduled' in status_lower:
                persistence_score += 6
                logger.debug(f"[Persistence-Escalation] Hearing scheduled +6 pts")
            elif 'lien' in status_lower or violation.is_lien:
                persistence_score += 5
                logger.debug(f"[Persistence-Escalation] Lien filed +5 pts")
            elif 'notice' in status_lower or 'warning' in status_lower:
                persistence_score += 2
                logger.debug(f"[Persistence-Escalation] Notice issued +2 pts")
            else:
                logger.debug(f"[Persistence-Escalation] Status '{violation.status}' +0 pts")
        else:
            logger.debug(f"[Persistence-Escalation] No status +0 pts")
        
        # Component 3: Repeat Violations (4 points max)
        if property.code_violations:
            violation_count = len(property.code_violations)
            
            if violation_count >= 5:
                persistence_score += 4
                logger.debug(f"[Persistence-Repeat] {violation_count} violations (5+) +4 pts")
            elif violation_count >= 3:
                persistence_score += 3
                logger.debug(f"[Persistence-Repeat] {violation_count} violations (3-4) +3 pts")
            elif violation_count >= 2:
                persistence_score += 2
                logger.debug(f"[Persistence-Repeat] {violation_count} violations (2) +2 pts")
            else:
                logger.debug(f"[Persistence-Repeat] {violation_count} violation(s) +0 pts")
        
        logger.debug(f"[Persistence TOTAL] {persistence_score}/20 pts")
        return min(persistence_score, 20)  # Cap at 20 points
    
    def calculate_property_score(self, property: Property) -> Dict:
        """
        Calculate comprehensive CDS score for a property.
        
        Args:
            property: Property record with relationships loaded
            
        Returns:
            Dictionary with score breakdown
        """
        # Initialize scores
        severity_score = 0
        days_open_score = 0
        persistence_score = 0
        prior_score = 0
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Scoring Property: {property.address or 'No Address'}")
        logger.info(f"Parcel ID: {property.parcel_id}")
        logger.info(f"Owner: {property.owner.owner_name if property.owner else 'No Owner'}")
        logger.info(f"Violations: {len(property.code_violations) if property.code_violations else 0}")
        logger.info(f"{'='*80}")
        
        # Process all violations for this property
        if property.code_violations:
            logger.info(f"Processing {len(property.code_violations)} violation(s)...")
            # Get highest severity, longest open, and strongest persistence signal
            for idx, violation in enumerate(property.code_violations, 1):
                logger.info(f"\n  Violation #{idx}: {violation.record_number}")
                logger.info(f"  Type: {violation.violation_type}")
                logger.info(f"  Status: {violation.status}")
                logger.info(f"  Opened: {violation.opened_date}")
                
                severity = self.calculate_violation_severity(violation)
                days_open = self.calculate_days_open_score(violation)
                persistence = self.calculate_violation_persistence_score(violation, property)
                
                severity_score = max(severity_score, severity)
                days_open_score = max(days_open_score, days_open)
                persistence_score = max(persistence_score, persistence)
            
            # Calculate prior violations count
            prior_score = self.calculate_prior_violations_score(property)
        else:
            logger.warning(f"  No violations found for this property!")
        
        # Calculate absentee and equity scores
        absentee_score = 0
        if property.owner:
            absentee_score = self.calculate_absentee_score(property.owner, property)
        else:
            logger.warning(f"  No owner record found!")
        
        equity_score = self.calculate_equity_score(property.financial)
        
        # Calculate total
        total_score = (
            severity_score +
            days_open_score +
            persistence_score +
            absentee_score +
            prior_score +
            equity_score
        )
        
        # Determine qualification
        qualified = total_score >= CDS_THRESHOLD
        
        # Log final breakdown
        logger.info(f"\n{'─'*80}")
        logger.info(f"FINAL SCORE BREAKDOWN:")
        logger.info(f"  Violation Severity:     {severity_score:>3.0f} / 25")
        logger.info(f"  Days Open:              {days_open_score:>3.0f} / 20")
        logger.info(f"  Violation Persistence:  {persistence_score:>3.0f} / 20")
        logger.info(f"  Absentee Ownership:     {absentee_score:>3.0f} / 15")
        logger.info(f"  Prior Violations:       {prior_score:>3.0f} / 10")
        logger.info(f"  Equity vs Repair:       {equity_score:>3.0f} / 10")
        logger.info(f"  {'─'*40}")
        logger.info(f"  TOTAL CDS SCORE:        {total_score:>3.0f} / 100")
        logger.info(f"  Qualified (70+):        {'YES ✓' if qualified else 'NO ✗'}")
        logger.info(f"{'='*80}\n")
        
        return {
            'property_id': property.id,
            'parcel_id': property.parcel_id,
            'address': property.address,
            'owner_name': property.owner.owner_name if property.owner else None,
            'violation_count': len(property.code_violations) if property.code_violations else 0,
            'severity_score': severity_score,
            'days_open_score': days_open_score,
            'persistence_score': persistence_score,
            'absentee_score': absentee_score,
            'prior_score': prior_score,
            'equity_score': equity_score,
            'total_score': total_score,
            'qualified': qualified
        }
    
    def save_score_to_database(self, score_data: Dict, upsert: bool = True) -> DistressScore:
        """
        Save calculated CDS score to the database with UPSERT logic.
        
        By default, checks if a score exists for this property TODAY.
        If found, updates it. If not found, creates new record.
        This prevents duplicate scores when re-running scoring on the same day.
        
        Args:
            score_data: Score dictionary from calculate_property_score()
            upsert: If True (default), update existing today's score instead of creating duplicate.
                   If False, always create new record (for historical tracking).
            
        Returns:
            Saved DistressScore record (updated or newly created)
        """
        property_id = score_data['property_id']
        today_date = datetime.now().date()
        
        # Check for existing score from today (UPSERT logic)
        existing_score = None
        if upsert:
            from sqlalchemy import func, cast, Date
            existing_score = self.session.query(DistressScore).filter(
                DistressScore.property_id == property_id,
                cast(DistressScore.score_date, Date) == today_date
            ).first()
        
        # Determine lead tier based on score
        total_score = score_data['total_score']
        if total_score >= 85:
            lead_tier = 'Ultra Platinum'
        elif total_score >= 75:
            lead_tier = 'Platinum'
        elif total_score >= 65:
            lead_tier = 'Gold'
        elif total_score >= 55:
            lead_tier = 'Silver'
        else:
            lead_tier = 'Bronze'
        
        # Determine urgency level
        if total_score >= 85:
            urgency_level = 'Immediate'
        elif total_score >= 75:
            urgency_level = 'High'
        elif total_score >= 60:
            urgency_level = 'Medium'
        else:
            urgency_level = 'Low'
        
        # Build factor scores JSONB
        factor_scores = {
            'violation_severity': score_data['severity_score'],
            'days_open': score_data['days_open_score'],
            'violation_persistence': score_data['persistence_score'],
            'absentee_ownership': score_data['absentee_score'],
            'prior_violations': score_data['prior_score'],
            'equity_vs_repair': score_data['equity_score']
        }
        
        if existing_score:
            # UPDATE existing record for today (avoid intra-day duplicates)
            existing_score.score_date = datetime.now()
            existing_score.final_cds_score = total_score
            existing_score.lead_tier = lead_tier
            existing_score.urgency_level = urgency_level
            existing_score.qualified = score_data['qualified']
            existing_score.factor_scores = factor_scores

            logger.debug(f"Updated CDS score for property {score_data['parcel_id']}: {total_score} ({lead_tier})")
            return existing_score
        else:
            # Check the most recent score ever recorded for this property.
            # Only create a new record if the score has actually changed — this
            # prevents accumulating identical rows across days when nothing changes.
            latest_score = self.session.query(DistressScore).filter(
                DistressScore.property_id == property_id
            ).order_by(DistressScore.score_date.desc()).first()

            if latest_score and float(latest_score.final_cds_score) == float(total_score):
                logger.debug(f"Score unchanged for property {score_data['parcel_id']}: {total_score} — skipping")
                return None

            # Score changed (or no prior score exists) — record the change
            distress_score = DistressScore(
                property_id=property_id,
                score_date=datetime.now(),
                final_cds_score=total_score,
                lead_tier=lead_tier,
                urgency_level=urgency_level,
                qualified=score_data['qualified'],
                factor_scores=factor_scores
            )

            self.session.add(distress_score)
            logger.debug(f"Created CDS score for property {score_data['parcel_id']}: {total_score} ({lead_tier})")
            return distress_score
    
    def score_all_properties_with_violations(self, save_to_db: bool = False) -> List[Dict]:
        """
        Calculate CDS scores for all properties with code violations.
        
        Args:
            save_to_db: If True, save scores to DistressScore table
        
        Returns:
            List of score dictionaries
        """
        logger.info("Fetching properties with code violations from database...")
        
        # Query properties with violations, eager load relationships
        from sqlalchemy.orm import joinedload
        
        properties = self.session.query(Property).options(
            joinedload(Property.code_violations),
            joinedload(Property.owner),
            joinedload(Property.financial)
        ).join(Property.code_violations).distinct().all()

        logger.info(f"Found {len(properties)} properties with violations")
        
        # Calculate scores
        scores = []
        saved_count = 0
        unchanged_count = 0
        for property in properties:
            try:
                score_result = self.calculate_property_score(property)
                scores.append(score_result)

                # Save to database if requested
                if save_to_db:
                    result = self.save_score_to_database(score_result)
                    if result is None:
                        unchanged_count += 1
                    else:
                        saved_count += 1

            except Exception as e:
                logger.error(f"Error scoring property {property.parcel_id}: {e}")
                continue

        # Commit all saved scores
        if save_to_db:
            try:
                self.session.commit()
                logger.info(
                    f"CDS scoring complete — {saved_count} saved (score changed), "
                    f"{unchanged_count} skipped (score unchanged)"
                )
            except Exception as e:
                logger.error(f"Error committing scores to database: {e}")
                self.session.rollback()
                raise
        
        return scores
    
    def get_qualified_properties(self, min_score: float = CDS_THRESHOLD) -> List[Dict]:
        """
        Get properties that meet the minimum CDS threshold.
        
        Args:
            min_score: Minimum CDS score to qualify
            
        Returns:
            List of qualified property scores
        """
        all_scores = self.score_all_properties_with_violations()
        qualified = [s for s in all_scores if s['total_score'] >= min_score]
        
        # Sort by score descending
        qualified.sort(key=lambda x: x['total_score'], reverse=True)
        
        return qualified
    
    def get_saved_qualified_scores(self, min_score: float = CDS_THRESHOLD):
        """
        Query saved DistressScore records for qualified properties.
        
        Args:
            min_score: Minimum CDS score to qualify (default: CDS_THRESHOLD = 70)
            
        Returns:
            Query result of DistressScore records with score >= min_score
        """
        from sqlalchemy.orm import joinedload
        
        return self.session.query(DistressScore).options(
            joinedload(DistressScore.property).joinedload(Property.owner),
            joinedload(DistressScore.property).joinedload(Property.financial)
        ).filter(
            DistressScore.final_cds_score >= min_score
        ).order_by(
            DistressScore.final_cds_score.desc()
        ).all()
    
    def get_saved_scores_by_lead_tier(self, lead_tier: str):
        """
        Query saved DistressScore records by lead tier.
        
        Args:
            lead_tier: One of 'Ultra Platinum', 'Platinum', 'Gold', 'Silver', 'Bronze'
            
        Returns:
            Query result of DistressScore records matching the lead tier
        """
        from sqlalchemy.orm import joinedload
        
        return self.session.query(DistressScore).options(
            joinedload(DistressScore.property).joinedload(Property.owner),
            joinedload(DistressScore.property).joinedload(Property.financial)
        ).filter(
            DistressScore.lead_tier == lead_tier
        ).order_by(
            DistressScore.final_cds_score.desc()
        ).all()
    
    def get_latest_score_for_property(self, property_id: int):
        """
        Get the most recent DistressScore record for a property.
        
        Args:
            property_id: Property database ID
            
        Returns:
            Latest DistressScore record or None
        """
        return self.session.query(DistressScore).filter(
            DistressScore.property_id == property_id
        ).order_by(
            DistressScore.score_date.desc()
        ).first()
    
    def get_todays_score_for_property(self, property_id: int):
        """
        Get today's DistressScore record for a property (if exists).
        
        Useful for checking if a score was already calculated today before
        triggering CRM exports or recalculation.
        
        Args:
            property_id: Property database ID
            
        Returns:
            Today's DistressScore record or None
        """
        from sqlalchemy import func, cast, Date
        today_date = datetime.now().date()
        
        return self.session.query(DistressScore).filter(
            DistressScore.property_id == property_id,
            cast(DistressScore.score_date, Date) == today_date
        ).first()


def main():
    """
    Entry point for cron execution.
    Scores all properties with violations and saves results to the database.
    """
    import sys
    from src.utils.logger import setup_logging, get_logger
    from src.core.database import get_db_context

    setup_logging()
    log = get_logger(__name__)

    log.info("=" * 60)
    log.info("CDS Scoring Engine — Daily Run")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        with get_db_context() as session:
            calculator = CDSCalculator(session)
            scores = calculator.score_all_properties_with_violations(save_to_db=True)

        total = len(scores)
        qualified = sum(1 for s in scores if s['qualified'])

        log.info("=" * 60)
        log.info("CDS SCORING COMPLETE")
        log.info(f"  Properties scored:  {total}")
        log.info(f"  Qualified (≥70):    {qualified}")
        log.info(f"  Not qualified:      {total - qualified}")
        if total:
            avg = sum(s['total_score'] for s in scores) / total
            log.info(f"  Average score:      {avg:.1f}")
        log.info(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60)

    except Exception as e:
        log.error(f"CDS scoring failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
