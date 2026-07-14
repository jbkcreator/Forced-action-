"""Create lane_fee_config_audit — durable record of RESPA fee_config_flag flips.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_lane_fee_config_audit.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    """
    CREATE TABLE IF NOT EXISTS lane_fee_config_audit (
        id                BIGSERIAL PRIMARY KEY,
        lane_id           UUID NOT NULL REFERENCES lanes(lane_id),
        previous_enabled  BOOLEAN NOT NULL,
        new_enabled       BOOLEAN NOT NULL,
        actor             VARCHAR(255) NOT NULL,
        occurred_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_lfca_lane_occurred
        ON lane_fee_config_audit (lane_id, occurred_at)
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("lane_fee_config_audit created.")


if __name__ == "__main__":
    main()
