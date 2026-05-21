"""Transfer county config data from distress_dev -> distress_db (prod).

Tables transferred (in FK order):
  1. county_sources  — upsert on (county_id, signal_type)
  2. county_column_mappings — insert/update, source_id remapped via FK lookup
  3. playwright_code_history — insert new, source_id remapped
  4. cf_bypass_profiles — upsert on profile_name

Run:  python scripts/transfer_county_config.py [--dry-run]
"""
import sys
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from urllib.parse import urlparse

DEV  = "postgresql://distress_user:distressadmin123@5.78.184.159:5432/distress_dev"
PROD = "postgresql://distress_user:distressadmin123@5.78.184.159:5432/distress_db"

DRY_RUN = "--dry-run" in sys.argv


def make_conn(url):
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port, dbname=p.path[1:],
        user=p.username, password=p.password, cursor_factory=RealDictCursor,
    )


def main():
    dev  = make_conn(DEV)
    prod = make_conn(PROD)
    dev.autocommit  = False
    prod.autocommit = False

    try:
        dcur = dev.cursor()
        pcur = prod.cursor()

        # ── 1. county_sources ─────────────────────────────────────────────────
        dcur.execute("""
            SELECT id, county_id, signal_type, source_name, url, description,
                   navigation_hint, output_format, date_range_available, frequency,
                   is_active, special_flags, scrape_mode, playwright_code,
                   playwright_code_version, playwright_code_approved
            FROM county_sources
            ORDER BY county_id, signal_type
        """)
        dev_sources = dcur.fetchall()
        print(f"\nSyncing {len(dev_sources)} county_sources …")

        dev_id_to_prod_id: dict[int, int] = {}

        for src in dev_sources:
            d = dict(src)
            if d.get("special_flags") is not None:
                d["special_flags"] = Json(d["special_flags"])
            pcur.execute("""
                UPDATE county_sources SET
                    source_name              = %(source_name)s,
                    url                      = %(url)s,
                    description              = %(description)s,
                    navigation_hint          = %(navigation_hint)s,
                    output_format            = %(output_format)s,
                    date_range_available     = %(date_range_available)s,
                    frequency                = %(frequency)s,
                    is_active                = %(is_active)s,
                    special_flags            = %(special_flags)s,
                    scrape_mode              = %(scrape_mode)s,
                    playwright_code          = %(playwright_code)s,
                    playwright_code_version  = %(playwright_code_version)s,
                    playwright_code_approved = %(playwright_code_approved)s,
                    updated_at               = NOW()
                WHERE county_id = %(county_id)s AND signal_type = %(signal_type)s
                RETURNING id
            """, d)
            row = pcur.fetchone()

            if row:
                prod_id = row["id"]
                label = "UPDATE"
            else:
                pcur.execute("""
                    INSERT INTO county_sources (
                        county_id, signal_type, source_name, url, description,
                        navigation_hint, output_format, date_range_available, frequency,
                        is_active, special_flags, scrape_mode, playwright_code,
                        playwright_code_version, playwright_code_approved
                    ) VALUES (
                        %(county_id)s, %(signal_type)s, %(source_name)s, %(url)s,
                        %(description)s, %(navigation_hint)s, %(output_format)s,
                        %(date_range_available)s, %(frequency)s, %(is_active)s,
                        %(special_flags)s, %(scrape_mode)s, %(playwright_code)s,
                        %(playwright_code_version)s, %(playwright_code_approved)s
                    )
                    RETURNING id
                """, d)
                prod_id = pcur.fetchone()["id"]
                label = "INSERT"

            dev_id_to_prod_id[src["id"]] = prod_id
            print(f"  [{label}] ({src['county_id']}, {src['signal_type']})  "
                  f"dev={src['id']} -> prod={prod_id}")

        # ── 2. county_column_mappings ──────────────────────────────────────────
        dcur.execute("""
            SELECT source_id, source_columns, mapping, is_approved, mapped_by,
                   approved_by, approved_at, sample_rows, reject_feedback,
                   post_processors, value_maps, row_routing, created_at
            FROM county_column_mappings
            ORDER BY source_id, id
        """)
        dev_mappings = dcur.fetchall()
        print(f"\nSyncing {len(dev_mappings)} county_column_mappings …")

        for m in dev_mappings:
            prod_src_id = dev_id_to_prod_id.get(m["source_id"])
            m = dict(m)
            for jsonb_col in ("source_columns", "mapping", "sample_rows", "post_processors", "value_maps", "row_routing"):
                if m.get(jsonb_col) is not None:
                    m[jsonb_col] = Json(m[jsonb_col])
            if not prod_src_id:
                print(f"  [SKIP mapping] unknown dev source_id={m.get('source_id')}")
                continue

            # Match on (source_id, is_approved) — one approved + one pending per source
            pcur.execute(
                "SELECT id FROM county_column_mappings "
                "WHERE source_id=%s AND is_approved=%s",
                (prod_src_id, m["is_approved"]),
            )
            existing = pcur.fetchone()

            if existing:
                pcur.execute("""
                    UPDATE county_column_mappings SET
                        source_columns  = %(source_columns)s,
                        mapping         = %(mapping)s,
                        mapped_by       = %(mapped_by)s,
                        approved_by     = %(approved_by)s,
                        approved_at     = %(approved_at)s,
                        sample_rows     = %(sample_rows)s,
                        reject_feedback = %(reject_feedback)s,
                        post_processors = %(post_processors)s,
                        value_maps      = %(value_maps)s,
                        row_routing     = %(row_routing)s,
                        updated_at      = NOW()
                    WHERE id = %(id)s
                """, {**dict(m), "id": existing["id"]})
                print(f"  [UPDATE mapping] prod_src={prod_src_id} approved={m['is_approved']}")
            else:
                pcur.execute("""
                    INSERT INTO county_column_mappings (
                        source_id, source_columns, mapping, is_approved, mapped_by,
                        approved_by, approved_at, sample_rows, reject_feedback,
                        post_processors, value_maps, row_routing, created_at
                    ) VALUES (
                        %(source_id)s, %(source_columns)s, %(mapping)s, %(is_approved)s,
                        %(mapped_by)s, %(approved_by)s, %(approved_at)s, %(sample_rows)s,
                        %(reject_feedback)s, %(post_processors)s, %(value_maps)s,
                        %(row_routing)s, %(created_at)s
                    )
                """, {**dict(m), "source_id": prod_src_id})
                print(f"  [INSERT mapping] prod_src={prod_src_id} approved={m['is_approved']}")

        # ── 3. playwright_code_history ─────────────────────────────────────────
        dcur.execute("""
            SELECT source_id, county_id, code, prompt_version, reason,
                   is_approved, generated_at
            FROM playwright_code_history
            ORDER BY source_id, generated_at
        """)
        dev_history = dcur.fetchall()
        print(f"\nSyncing {len(dev_history)} playwright_code_history rows …")

        for h in dev_history:
            prod_src_id = dev_id_to_prod_id.get(h["source_id"])
            if not prod_src_id:
                print(f"  [SKIP history] unknown dev source_id={h['source_id']}")
                continue

            # Dedup on (source_id, generated_at, reason)
            pcur.execute(
                "SELECT id FROM playwright_code_history "
                "WHERE source_id=%s AND generated_at=%s AND reason=%s",
                (prod_src_id, h["generated_at"], h["reason"]),
            )
            if pcur.fetchone():
                print(f"  [SKIP dup history] prod_src={prod_src_id} reason={h['reason']}")
                continue

            pcur.execute("""
                INSERT INTO playwright_code_history (
                    source_id, county_id, code, prompt_version, reason, is_approved, generated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (prod_src_id, h["county_id"], h["code"], h["prompt_version"],
                  h["reason"], h["is_approved"], h["generated_at"]))
            print(f"  [INSERT history] prod_src={prod_src_id} reason={h['reason']}")

        # ── 4. cf_bypass_profiles ──────────────────────────────────────────────
        dcur.execute("""
            SELECT profile_name, county_id, portal_url, status,
                   last_warmed_at, last_validated_at, last_failure_at, last_failure_reason,
                   profile_dir_path, validation_ttl_minutes,
                   profile_blob, profile_blob_size, profile_blob_at
            FROM cf_bypass_profiles
        """)
        dev_profiles = dcur.fetchall()
        print(f"\nSyncing {len(dev_profiles)} cf_bypass_profiles …")

        for p in dev_profiles:
            d = dict(p)
            pcur.execute(
                "SELECT id FROM cf_bypass_profiles WHERE profile_name=%s",
                (p["profile_name"],),
            )
            if pcur.fetchone():
                pcur.execute("""
                    UPDATE cf_bypass_profiles SET
                        county_id=%(county_id)s, portal_url=%(portal_url)s,
                        status=%(status)s,
                        last_warmed_at=%(last_warmed_at)s,
                        last_validated_at=%(last_validated_at)s,
                        last_failure_at=%(last_failure_at)s,
                        last_failure_reason=%(last_failure_reason)s,
                        profile_dir_path=%(profile_dir_path)s,
                        validation_ttl_minutes=%(validation_ttl_minutes)s,
                        profile_blob=%(profile_blob)s,
                        profile_blob_size=%(profile_blob_size)s,
                        profile_blob_at=%(profile_blob_at)s,
                        updated_at=NOW()
                    WHERE profile_name=%(profile_name)s
                """, d)
                print(f"  [UPDATE cf_profile] {p['profile_name']}")
            else:
                pcur.execute("""
                    INSERT INTO cf_bypass_profiles (
                        profile_name, county_id, portal_url, status,
                        last_warmed_at, last_validated_at, last_failure_at, last_failure_reason,
                        profile_dir_path, validation_ttl_minutes,
                        profile_blob, profile_blob_size, profile_blob_at
                    ) VALUES (
                        %(profile_name)s, %(county_id)s, %(portal_url)s, %(status)s,
                        %(last_warmed_at)s, %(last_validated_at)s, %(last_failure_at)s,
                        %(last_failure_reason)s, %(profile_dir_path)s,
                        %(validation_ttl_minutes)s, %(profile_blob)s,
                        %(profile_blob_size)s, %(profile_blob_at)s
                    )
                """, d)
                print(f"  [INSERT cf_profile] {p['profile_name']}")

        # ── Commit ─────────────────────────────────────────────────────────────
        if DRY_RUN:
            prod.rollback()
            print("\n[DRY RUN] rolled back -- no changes written.")
        else:
            prod.commit()
            print("\nDONE: All changes committed to prod.")

    except Exception as e:
        prod.rollback()
        print(f"\nROLLED BACK due to error: {e}")
        raise
    finally:
        dev.close()
        prod.close()


if __name__ == "__main__":
    main()
