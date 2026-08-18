"""Add probe_emails JSONB column to vertical_probes.

Stores the list of contact emails sent through the Relay passthrough campaign
for this probe, used by refresh_probe_replies to attribute replies to the
correct probe without a dedicated per-probe Instantly campaign.

Idempotent — safe to re-run. Run once against the shared DB:
    PYTHONPATH=. python migrations/apply_vertical_probe_emails.py
"""
from sqlalchemy import create_engine, text

from config.settings import get_settings


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE vertical_probes "
                "ADD COLUMN IF NOT EXISTS probe_emails JSONB"
            )
        )
    print("apply_vertical_probe_emails: done")


if __name__ == "__main__":
    main()
