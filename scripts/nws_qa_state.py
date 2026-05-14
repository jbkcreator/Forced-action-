"""Quick DB state probe for NWS QA."""
import io, sys
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from src.core.database import get_db_context
from sqlalchemy import text

CHOSEN_ZIP = "33559"
CHOSEN_SUB = 4509

with get_db_context() as db:
    print("--- GOLD+ LEADS IN ZIP", CHOSEN_ZIP, "---")
    leads = db.execute(text(
        "SELECT ds.id, ds.lead_tier, ds.final_cds_score, p.zip, p.address "
        "FROM distress_scores ds "
        "JOIN properties p ON p.id = ds.property_id "
        "WHERE p.zip = :zip "
        "AND ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum') "
        "ORDER BY ds.final_cds_score DESC NULLS LAST LIMIT 5"
    ), {"zip": CHOSEN_ZIP}).fetchall()
    print(f"  Count: {len(leads)}")
    for l in leads:
        print(" ", dict(l._mapping))

    print()
    print("--- SUBSCRIBER", CHOSEN_SUB, "TERRITORIES ---")
    zips = db.execute(text(
        "SELECT zip_code, status FROM zip_territories WHERE subscriber_id = :sid"
    ), {"sid": CHOSEN_SUB}).fetchall()
    for z in zips:
        print(" ", dict(z._mapping))

    print()
    print("--- NWS_ALERTS TABLE ---")
    alerts = db.execute(text(
        "SELECT alert_id, event, storm_pack_triggered, cora_urgency_sent, "
        "subscriber_count, processed_at FROM nws_alerts ORDER BY processed_at DESC LIMIT 5"
    )).fetchall()
    if not alerts:
        print("  (empty)")
    else:
        for a in alerts:
            print(" ", dict(a._mapping))

    print()
    print("--- WEBHOOK_EVENTS NWS ---")
    wes = db.execute(text(
        "SELECT event_type, payload_summary::text, created_at FROM webhook_events "
        "WHERE source = 'nws' ORDER BY created_at DESC LIMIT 10"
    )).fetchall()
    if not wes:
        print("  (none)")
    else:
        for we in wes:
            print(" ", dict(we._mapping))

    print()
    print("--- AGENT_DECISIONS nws_urgency ---")
    ads = db.execute(text(
        "SELECT decision_id, subscriber_id, terminal_status, started_at "
        "FROM agent_decisions WHERE graph_name = 'nws_urgency' "
        "ORDER BY started_at DESC LIMIT 5"
    )).fetchall()
    if not ads:
        print("  (none)")
    else:
        for ad in ads:
            print(" ", dict(ad._mapping))
