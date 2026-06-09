"""
Applies rescue_queue_98799.json to DB.
Uses parameterized bulk insert with ON CONFLICT DO NOTHING — safe against
special characters and idempotent (safe to re-run).
"""
import json
from datetime import datetime, timezone
from sqlalchemy import text as sa_text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.core.database import get_db_context
from src.core.models import EnrichedContact, EnrichmentUsageLog
from src.services.tracerfy_fallback import _parse_trace_row

COUNTY_ID    = "hillsborough"
QUEUE_ID     = "98799"
COST_PER_HIT = 2

with open("rescue_queue_98799.json") as f:
    results = json.load(f)
print(f"Loaded {len(results)} rows")

with get_db_context() as session:
    addr_rows = session.execute(sa_text("""
        SELECT p.address, o.id AS owner_id, o.property_id,
               o.county_id, o.phone_1, o.email_1
        FROM owners o
        JOIN properties p ON p.id = o.property_id
        WHERE o.county_id = :county_id
          AND p.address IS NOT NULL AND p.address != ''
    """), {"county_id": COUNTY_ID}).fetchall()
    addr_map = {r.address.upper().strip(): r for r in addr_rows}

    committed = set(
        r[0] for r in session.execute(sa_text("""
            SELECT property_id FROM enriched_contacts
            WHERE source = 'tracerfy' AND match_success = TRUE
        """)).fetchall()
    )
    print(f"Already committed hits: {len(committed)}")

    now = datetime.now(timezone.utc)
    ec_rows, usage_rows = [], []
    phone_updates = {}   # owner_id → phone
    email_updates = {}   # owner_id → email
    hit_owner_ids = []

    for row in results:
        addr_key = (row.get("address") or "").upper().strip()
        db_row = addr_map.get(addr_key)
        if not db_row or db_row.property_id in committed:
            continue
        parsed = _parse_trace_row(row)
        if not parsed["match_success"]:
            continue

        ec_rows.append({
            "property_id":   db_row.property_id,
            "county_id":     db_row.county_id or COUNTY_ID,
            "mobile_phone":  parsed["mobile_phone"],
            "landline":      parsed["landline"],
            "email":         parsed["email"],
            "mailing_address": parsed["mailing_address"],
            "source":        "tracerfy",
            "match_success": True,
            "enriched_at":   now,
        })
        usage_rows.append({
            "vendor":      "tracerfy",
            "purpose":     "skip_trace",
            "success":     True,
            "cost_cents":  COST_PER_HIT,
            "property_id": db_row.property_id,
            "request_ref": QUEUE_ID,
            "created_at":  now,
        })
        hit_owner_ids.append(db_row.owner_id)
        phone = parsed["mobile_phone"] or parsed["landline"]
        if phone and not db_row.phone_1:
            phone_updates[db_row.owner_id] = phone
        if parsed["email"] and not db_row.email_1:
            email_updates[db_row.owner_id] = parsed["email"]

    print(f"New hits to apply: {len(ec_rows)}")
    if not ec_rows:
        print("Nothing new to apply.")
    else:
        # ON CONFLICT DO NOTHING — safe if background task already inserted some
        session.execute(
            pg_insert(EnrichedContact.__table__)
            .values(ec_rows)
            .on_conflict_do_nothing()
        )
        session.execute(
            pg_insert(EnrichmentUsageLog.__table__)
            .values(usage_rows)
            .on_conflict_do_nothing()
        )

        if hit_owner_ids:
            session.execute(sa_text("""
                UPDATE owners SET skip_trace_success = TRUE
                WHERE id = ANY(:ids)
            """), {"ids": hit_owner_ids})

        if phone_updates:
            session.execute(
                sa_text("""
                    UPDATE owners SET phone_1 = v.phone
                    FROM (SELECT unnest(:ids) AS id, unnest(:phones) AS phone) v
                    WHERE owners.id = v.id AND owners.phone_1 IS NULL
                """),
                {"ids": list(phone_updates.keys()), "phones": list(phone_updates.values())}
            )

        if email_updates:
            session.execute(
                sa_text("""
                    UPDATE owners SET email_1 = v.email
                    FROM (SELECT unnest(:ids) AS id, unnest(:emails) AS email) v
                    WHERE owners.id = v.id AND owners.email_1 IS NULL
                """),
                {"ids": list(email_updates.keys()), "emails": list(email_updates.values())}
            )

        session.commit()
        print(f"Done. {len(ec_rows)} hits committed.")
