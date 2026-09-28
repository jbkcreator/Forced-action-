"""Lead ownership table + loan_paid_off loss reason.

  lead_campaign_assignments -- one row per (person, campaign, radar_id) claim;
                               at most one `active` owner per person (partial
                               unique index). Carries the lead tag (source,
                               radar_id). See src/core/models.py:LeadCampaignAssignment.
  agent_lane_opportunity_outcomes.ck_alo_reason_code -- widened to allow
                               'loan_paid_off' (dial/outcome disposition).

Idempotent — IF NOT EXISTS guards; the CHECK is dropped and re-added with the
full code list, so re-running converges on the same constraint.

Usage:
    PYTHONPATH=. python migrations/apply_lead_campaign_assignments.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS lead_campaign_assignments (
        id             BIGSERIAL PRIMARY KEY,
        person_id      UUID        NOT NULL,
        opportunity_id UUID,
        campaign       VARCHAR(60) NOT NULL,
        source         VARCHAR(40) NOT NULL,
        radar_id       VARCHAR(50),
        status         VARCHAR(20) NOT NULL,
        displaced_by   VARCHAR(60),
        created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        ended_at       TIMESTAMPTZ,
        CONSTRAINT fk_lca_person FOREIGN KEY (person_id) REFERENCES fa_max_persons (person_id),
        CONSTRAINT ck_lca_status CHECK (status IN ('active','preempted','blocked'))
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_lca_one_active_owner "
    "ON lead_campaign_assignments (person_id) WHERE status = 'active';",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_lca_claim "
    "ON lead_campaign_assignments (person_id, campaign, COALESCE(radar_id, ''));",
    "CREATE INDEX IF NOT EXISTS ix_lca_campaign ON lead_campaign_assignments (campaign);",
    "ALTER TABLE agent_lane_opportunity_outcomes DROP CONSTRAINT IF EXISTS ck_alo_reason_code;",
    """
    ALTER TABLE agent_lane_opportunity_outcomes ADD CONSTRAINT ck_alo_reason_code CHECK (
        (outcome = 'won' AND reason_code IS NULL) OR
        (outcome = 'lost' AND reason_code IN
            ('timing','price','trust','fit','no_urgency','wrong_contact','competitor','no_response','loan_paid_off'))
    );
    """,
]


def run(conn) -> None:
    for i, stmt in enumerate(DDL, 1):
        logger.info("DDL step %d/%d", i, len(DDL))
        conn.execute(text(stmt))
    logger.info("apply_lead_campaign_assignments complete.")


if __name__ == "__main__":
    engine = create_engine(str(get_settings().database_url))
    with engine.begin() as conn:
        run(conn)
