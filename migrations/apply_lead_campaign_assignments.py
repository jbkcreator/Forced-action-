"""Lead ownership table + loan_paid_off loss reason.

  lead_campaign_assignments -- one row per (person, campaign, radar_id) claim;
                               at most one `active` owner per person (partial
                               unique index). Carries the lead tag (source,
                               radar_id). See src/core/models.py:LeadCampaignAssignment.
  agent_lane_opportunity_outcomes.ck_alo_reason_code -- rebuilt from
                               LOSS_REASON_CODES, so it now allows 'loan_paid_off'.
  dial_list_touch.ix_dial_list_touch_property_id -- for the pilot rollup join.

Idempotent — IF NOT EXISTS guards; the CHECK is dropped and re-added with the
full code list, so re-running converges on the same constraint.

Usage:
    PYTHONPATH=. python migrations/apply_lead_campaign_assignments.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings
from src.services.opportunity_outcome import LOSS_REASON_CODES

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
    # Pilot rollup joins dial touches to people by property.
    "CREATE INDEX IF NOT EXISTS ix_dial_list_touch_property_id ON dial_list_touch (property_id);",
    # Drop + re-add NOT VALID in one step (both instant, never a moment without the
    # CHECK); VALIDATE runs as its own step and scans without blocking writes.
    f"""
    ALTER TABLE agent_lane_opportunity_outcomes DROP CONSTRAINT IF EXISTS ck_alo_reason_code;
    ALTER TABLE agent_lane_opportunity_outcomes ADD CONSTRAINT ck_alo_reason_code CHECK (
        (outcome = 'won' AND reason_code IS NULL) OR
        (outcome = 'lost' AND reason_code IN ({", ".join(f"'{c}'" for c in LOSS_REASON_CODES)}))
    ) NOT VALID;
    """,
    "ALTER TABLE agent_lane_opportunity_outcomes VALIDATE CONSTRAINT ck_alo_reason_code;",
]


def run(engine) -> None:
    # One transaction per step, so the DROP's lock is released before VALIDATE scans.
    for i, stmt in enumerate(DDL, 1):
        logger.info("DDL step %d/%d", i, len(DDL))
        with engine.begin() as conn:
            conn.execute(text(stmt))
    logger.info("apply_lead_campaign_assignments complete.")


if __name__ == "__main__":
    run(create_engine(str(get_settings().database_url)))
