"""
NWS Weather QA probe -- collects all state needed for the QA report.
Run:  python -m scripts.nws_qa_probe
"""
import io
import json
import sys
import traceback
from datetime import datetime, timezone, timedelta

# Force UTF-8 output on Windows consoles that default to cp1252
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.redis_client import redis_available, get_redis
from sqlalchemy import text


def section(title):
    print()
    print("=" * 72)
    print("  " + title)
    print("=" * 72)


COUNTY_SAME = {
    "hillsborough": ("012057", "FLC057"),
    "pinellas":     ("012103", "FLC103"),
    "pasco":        ("012101", "FLC101"),
    "polk":         ("012105", "FLC105"),
    "manatee":      ("012081", "FLC081"),
}


def main():
    settings = get_settings()

    # ------------------------------------------------------------------
    # 1. Feature flags
    # ------------------------------------------------------------------
    section("1. FEATURE FLAGS")
    flags = {
        "NWS_WEATHER_ENABLED":         settings.nws_weather_enabled,
        "NWS_REVENUE_POLLING_ENABLED":  settings.nws_revenue_polling_enabled,
        "STORM_PACK_ENABLED":           settings.storm_pack_enabled,
        "NWS_CORA_URGENCY_ENABLED":     settings.nws_cora_urgency_enabled,
        "NWS_POLL_INTERVAL_SECONDS":    settings.nws_poll_interval_seconds,
        "NWS_SUPPORTED_STATES":         settings.nws_supported_states,
        "NWS_RELEVANT_EVENTS (count)":  len(settings.nws_relevant_events),
    }
    all_enabled = True
    for k, v in flags.items():
        ok = v if isinstance(v, bool) else True
        tag = "[OK]" if ok else "[!!]"
        if isinstance(v, bool) and not v:
            all_enabled = False
        print(f"  {tag}  {k}: {v}")
    if not all_enabled:
        print()
        print("  WARNING: one or more NWS feature flags are disabled.")

    chosen = None

    with get_db_context() as db:

        # ------------------------------------------------------------------
        # 2. Eligible subscribers
        # ------------------------------------------------------------------
        section("2. ELIGIBLE SUBSCRIBERS (active/grace, locked FL territory)")
        rows = db.execute(text("""
            SELECT
                s.id              AS subscriber_id,
                s.status          AS sub_status,
                s.tier            AS plan_tier,
                s.phone,
                s.email,
                zt.zip_code,
                zt.county_id,
                zt.locked_at,
                EXISTS (
                    SELECT 1 FROM sms_opt_outs soo WHERE soo.phone = s.phone
                ) AS sms_opted_out
            FROM subscribers s
            JOIN zip_territories zt ON zt.subscriber_id = s.id
            WHERE
                s.status IN ('active', 'grace')
                AND zt.status = 'locked'
                AND zt.county_id IN ('hillsborough','pinellas','pasco','polk','manatee')
            ORDER BY
                CASE zt.county_id
                    WHEN 'hillsborough' THEN 1
                    WHEN 'pinellas'     THEN 2
                    WHEN 'pasco'        THEN 3
                    WHEN 'polk'         THEN 4
                    WHEN 'manatee'      THEN 5
                END, s.id
            LIMIT 40
        """)).fetchall()

        if not rows:
            print("  WARN: No eligible subscribers found in supported FL counties.")
        else:
            for r in rows:
                m = dict(r._mapping)
                phone_tag = "phone=OK" if m["phone"] else "phone=MISSING"
                email_tag = "email=OK" if m["email"] else "email=MISSING"
                sms_tag   = "sms=OPT-OUT" if m["sms_opted_out"] else "sms=OK"
                print(
                    f"  sub={m['subscriber_id']:>5}  {m['sub_status']:<6}  "
                    f"{str(m['plan_tier'] or '?'):<20}  "
                    f"ZIP={m['zip_code']}  county={m['county_id']:<14}  "
                    f"{phone_tag}  {email_tag}  {sms_tag}"
                )

        # ------------------------------------------------------------------
        # 3. Best test candidate
        # ------------------------------------------------------------------
        section("3. SELECTED TEST SUBSCRIBER")
        for r in rows:
            m = dict(r._mapping)
            if m["phone"] and not m["sms_opted_out"]:
                chosen = m
                break
        if not chosen and rows:
            chosen = dict(rows[0]._mapping)

        if chosen:
            same, ugc = COUNTY_SAME.get(chosen["county_id"], ("?", "?"))
            print(f"  subscriber_id : {chosen['subscriber_id']}")
            print(f"  ZIP           : {chosen['zip_code']}")
            print(f"  county        : {chosen['county_id']}")
            print(f"  SAME/FIPS     : {same}")
            print(f"  UGC           : {ugc}")
            print(f"  phone         : {chosen['phone'] or 'N/A'}")
            print(f"  email         : {chosen['email'] or 'N/A'}")
            print(f"  sms_opted_out : {chosen['sms_opted_out']}")
            print(f"  plan_tier     : {chosen['plan_tier']}")
        else:
            print("  WARN: No eligible subscriber found.")

        # ------------------------------------------------------------------
        # 4. Gold+ leads in chosen ZIP
        # ------------------------------------------------------------------
        if chosen:
            section(f"4. GOLD+ LEADS IN ZIP {chosen['zip_code']}")
            lead_rows = db.execute(text("""
                SELECT
                    ds.id, ds.lead_tier, ds.final_cds_score,
                    p.zip, p.address
                FROM distress_scores ds
                JOIN properties p ON p.id = ds.property_id
                WHERE p.zip = :zip
                  AND ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                ORDER BY ds.final_cds_score DESC NULLS LAST
                LIMIT 10
            """), {"zip": chosen["zip_code"]}).fetchall()

            if not lead_rows:
                print(f"  NOTE: No Gold+ leads in ZIP {chosen['zip_code']}.")
                print("        Cora urgency will correctly skip (lead_count=0).")
            else:
                print(f"  Found {len(lead_rows)} Gold+ leads:")
                for lr in lead_rows:
                    m2 = dict(lr._mapping)
                    print(f"    ds_id={m2['id']}  tier={str(m2['lead_tier']):<14}  "
                          f"score={m2['final_cds_score']}  addr={m2['address']}")

        # ------------------------------------------------------------------
        # 5. nws_alerts table
        # ------------------------------------------------------------------
        section("5. NWS ALERTS TABLE -- last 10 rows")
        try:
            alert_rows = db.execute(text("""
                SELECT alert_id, event, affected_zips::text,
                       storm_pack_triggered, cora_urgency_sent,
                       subscriber_count, processed_at
                FROM nws_alerts
                ORDER BY processed_at DESC
                LIMIT 10
            """)).fetchall()
            if not alert_rows:
                print("  (empty -- no alerts processed yet)")
            else:
                for ar in alert_rows:
                    m3 = dict(ar._mapping)
                    print(
                        f"  alert_id : {str(m3['alert_id'])[:70]}"
                        f"\n    event            : {m3['event']}"
                        f"\n    storm_triggered  : {m3['storm_pack_triggered']}"
                        f"\n    cora_sent        : {m3['cora_urgency_sent']}"
                        f"\n    subscriber_count : {m3['subscriber_count']}"
                        f"\n    affected_zips    : {str(m3['affected_zips'])[:80]}"
                        f"\n    processed_at     : {m3['processed_at']}"
                        f"\n"
                    )
        except Exception as e:
            print(f"  ERROR querying nws_alerts: {e}")

        # ------------------------------------------------------------------
        # 6. webhook_events -- NWS source
        # ------------------------------------------------------------------
        section("6. WEBHOOK_EVENTS -- NWS source (last 20)")
        try:
            we_rows = db.execute(text("""
                SELECT source, event_type, payload_summary::text, created_at
                FROM webhook_events
                WHERE source = 'nws'
                ORDER BY created_at DESC
                LIMIT 20
            """)).fetchall()
            if not we_rows:
                print("  (no NWS events logged yet)")
            else:
                for we in we_rows:
                    m4 = dict(we._mapping)
                    print(f"  [{m4['created_at']}]  {m4['event_type']}")
                    if m4["payload_summary"]:
                        print(f"    {str(m4['payload_summary'])[:100]}")
        except Exception as e:
            print(f"  ERROR: {e}")

        # ------------------------------------------------------------------
        # 7. agent_decisions -- nws_urgency
        # ------------------------------------------------------------------
        section("7. AGENT_DECISIONS -- nws_urgency graph (last 10)")
        try:
            ad_rows = db.execute(text("""
                SELECT decision_id, subscriber_id, terminal_status,
                       tokens_used, cost_usd, summary::text, started_at
                FROM agent_decisions
                WHERE graph_name = 'nws_urgency'
                ORDER BY started_at DESC
                LIMIT 10
            """)).fetchall()
            if not ad_rows:
                print("  (no nws_urgency decisions yet)")
            else:
                for ad in ad_rows:
                    m5 = dict(ad._mapping)
                    print(
                        f"  decision : {m5['decision_id']}"
                        f"\n    sub_id  : {m5['subscriber_id']}"
                        f"\n    status  : {m5['terminal_status']}"
                        f"\n    tokens  : {m5['tokens_used']}   cost: ${m5['cost_usd']}"
                        f"\n    summary : {str(m5['summary'])[:100]}"
                        f"\n    at      : {m5['started_at']}"
                        f"\n"
                    )
        except Exception as e:
            print(f"  ERROR: {e}")

    # ------------------------------------------------------------------
    # 8. Redis
    # ------------------------------------------------------------------
    section("8. REDIS -- storm_active keys")
    if not redis_available():
        print("  WARN: Redis not available in this environment.")
    else:
        try:
            client = get_redis()
            if client:
                keys = client.keys("storm_active:*")
                if not keys:
                    print("  (no storm_active keys present)")
                else:
                    for k in sorted(keys):
                        key_str = k.decode() if isinstance(k, bytes) else str(k)
                        val     = client.get(k)
                        ttl_s   = client.ttl(k)
                        hours   = ttl_s // 3600
                        mins    = (ttl_s % 3600) // 60
                        print(f"  {key_str}  val={val}  TTL={ttl_s}s ({hours}h {mins}m)")
            else:
                print("  WARN: Redis client not initialised.")
        except Exception as e:
            print(f"  ERROR: {e}")

    # ------------------------------------------------------------------
    # 9. Simulated alert payloads
    # ------------------------------------------------------------------
    if chosen:
        same, ugc = COUNTY_SAME.get(chosen["county_id"], ("?", "?"))
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        exp_iso = (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ts      = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        zip_str = chosen["zip_code"]
        county_name = chosen["county_id"].title() + " County"

        qualifying_payload = {
            "id":          f"qa-nws-alert-{ts}-{zip_str}",
            "event":       "Severe Thunderstorm Warning",
            "severity":    "Severe",
            "urgency":     "Immediate",
            "certainty":   "Likely",
            "headline":    f"Severe Thunderstorm Warning issued for {county_name}",
            "description": "QA simulated alert for NWS Weather revenue trigger testing.",
            "instruction": "This is a QA test alert. Do not use for real emergency messaging.",
            "areaDesc":    county_name,
            "geocode":     {"SAME": [same], "UGC": [ugc]},
            "effective":   now_iso,
            "expires":     exp_iso,
        }

        nq_payload = {
            "id":       f"qa-nws-nonqualifying-{ts}NQ",
            "event":    "Frost Advisory",
            "severity": "Minor",
            "areaDesc": county_name,
            "geocode":  {"SAME": [same], "UGC": [ugc]},
        }

        section("9. QUALIFYING ALERT PAYLOAD (use for simulate-nws-alert)")
        print("  POST /api/admin/sandbox/simulate-nws-alert")
        print("  Body:")
        print(json.dumps(qualifying_payload, indent=4))

        section("9b. NON-QUALIFYING ALERT PAYLOAD (Frost Advisory)")
        print(json.dumps(nq_payload, indent=4))

        section("9c. DUPLICATE-TEST PAYLOAD (re-submit qualifying payload)")
        print("  Re-submit the same payload above with the same 'id' field.")
        print(f"  Expected: status='duplicate', no new nws_alerts row created.")

    section("PROBE COMPLETE")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
