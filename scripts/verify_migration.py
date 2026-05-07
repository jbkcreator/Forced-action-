from src.core.database import get_db_context
from sqlalchemy import text

with get_db_context() as db:
    tables = db.execute(text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='public' "
        "AND table_name IN ('manual_action_log','human_close_escalations','partner_subscriptions') "
        "ORDER BY table_name"
    )).fetchall()
    for t in tables:
        print("TABLE OK:", t[0])

    cols = db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='subscribers' "
        "AND column_name IN ('lock_candidate_zip','paused_at','pause_resume_at','escalation_routed_at','ap_lite_candidate_at') "
        "ORDER BY column_name"
    )).fetchall()
    for c in cols:
        print("COLUMN OK:", c[0])

    zip_col = db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='wallet_transactions' AND column_name='zip_code'"
    )).fetchall()
    print("COLUMN OK: wallet_transactions.zip_code" if zip_col else "MISSING: wallet_transactions.zip_code")
