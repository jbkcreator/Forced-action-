"""Phase 3 DDL — affiliate attribution on subscribers.

Adds subscribers.affiliate_ref and extends check_subscriber_signup_source to
allow 'affiliate'. Idempotent. Part of the fa081 affiliate program; applied via
script because the Alembic CLI is unusable on this repo (multi-head tree).
"""
import sys

from sqlalchemy import create_engine, text

from config.settings import get_settings

_ALLOWED = (
    "direct", "landing_page", "dbpr_email", "lifecycle_sms",
    "missed_call", "referral", "admin", "unknown", "affiliate",
)


def main() -> int:
    engine = create_engine(str(get_settings().database_url))
    in_list = ", ".join(f"'{s}'" for s in _ALLOWED)
    with engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS affiliate_ref VARCHAR(40)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_subscribers_affiliate_ref "
            "ON subscribers (affiliate_ref)"
        ))
        conn.execute(text(
            "ALTER TABLE subscribers DROP CONSTRAINT IF EXISTS check_subscriber_signup_source"
        ))
        conn.execute(text(
            f"ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_signup_source "
            f"CHECK (signup_source IN ({in_list}))"
        ))
    print("applied: affiliate_ref column + extended signup_source CHECK")
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
