"""Add probe_emails JSONB column to vertical_probes.

Stores the list of contact emails added to the Relay campaign for this probe,
used by refresh_probe_replies to attribute replies to the correct probe without
creating a dedicated per-probe Instantly campaign.
"""
import os
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE vertical_probes
                    ADD COLUMN IF NOT EXISTS probe_emails JSONB;
                    """
                )
        print("apply_vertical_probe_emails: done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
