"""
Applies rescue_queue_98799.json to DB — fully bulk, pure SQL, no ORM overhead.
"""
import json
from datetime import datetime, timezone
from sqlalchemy import text as sa_text
from src.core.database import get_db_context
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

    now = datetime.now(timezone.utc).isoformat()
    ec_values, usage_values, phone_pairs, email_pairs, hit_owner_ids = [], [], [], [], []

    for row in results:
        addr_key = (row.get("address") or "").upper().strip()
        db_row = addr_map.get(addr_key)
        if not db_row:
            continue
        if db_row.property_id in committed:
            continue
        parsed = _parse_trace_row(row)
        if not parsed["match_success"]:
            continue

        ec_values.append(
            f"({db_row.property_id}, '{db_row.county_id or COUNTY_ID}', "
            f"{('NULL' if not parsed['mobile_phone'] else repr(parsed['mobile_phone']))}, "
            f"{('NULL' if not parsed['landline'] else repr(parsed['landline']))}, "
            f"{('NULL' if not parsed['email'] else repr(parsed['email']))}, "
            f"'tracerfy', TRUE, '{now}')"
        )
        usage_values.append(
            f"('tracerfy','skip_trace',TRUE,{COST_PER_HIT},{db_row.property_id},'{QUEUE_ID}','{now}')"
        )
        hit_owner_ids.append(str(db_row.owner_id))
        phone = parsed["mobile_phone"] or parsed["landline"]
        if phone and not db_row.phone_1:
            phone_pairs.append(f"({db_row.owner_id}, {repr(phone)})")
        if parsed["email"] and not db_row.email_1:
            email_pairs.append(f"({db_row.owner_id}, {repr(parsed['email'])})")

    print(f"New hits to apply: {len(ec_values)}")
    if not ec_values:
        print("Nothing new.")
    else:
        session.execute(sa_text(f"""
            INSERT INTO enriched_contacts
              (property_id, county_id, mobile_phone, landline, email, source, match_success, enriched_at)
            VALUES {','.join(ec_values)}
        """))
        session.execute(sa_text(f"""
            INSERT INTO enrichment_usage_logs
              (vendor, purpose, success, cost_cents, property_id, request_ref, created_at)
            VALUES {','.join(usage_values)}
        """))
        if hit_owner_ids:
            session.execute(sa_text(f"""
                UPDATE owners SET skip_trace_success = TRUE
                WHERE id IN ({','.join(hit_owner_ids)})
            """))
        if phone_pairs:
            session.execute(sa_text(f"""
                UPDATE owners SET phone_1 = v.phone
                FROM (VALUES {','.join(phone_pairs)}) AS v(id, phone)
                WHERE owners.id = v.id AND owners.phone_1 IS NULL
            """))
        if email_pairs:
            session.execute(sa_text(f"""
                UPDATE owners SET email_1 = v.email
                FROM (VALUES {','.join(email_pairs)}) AS v(id, email)
                WHERE owners.id = v.id AND owners.email_1 IS NULL
            """))

        session.commit()
        print(f"Done. {len(ec_values)} hits committed.")
