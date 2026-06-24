"""
Fetch full property detail: owner + distress score + enriched contact.

Usage:
    PYTHONPATH=. .venv/Scripts/python.exe scripts/property_detail.py <property_id>
"""

import json
import sys

from sqlalchemy import text as sa_text

from src.core.database import get_db_context

QUERY = sa_text("""
SELECT
    -- Property
    p.id              AS p_id,
    p.parcel_id,
    p.address,
    p.city,
    p.zip,
    p.state,
    p.jurisdiction,
    p.property_type,
    p.year_built,
    p.sq_ft,
    p.beds,
    p.baths,
    p.lot_size,
    p.building_condition,
    p.building_class,
    p.heated_sq_ft,
    p.subdivision,
    p.hcpa_neighborhood_code,
    p.building_details,
    p.lat,
    p.lon,
    p.legal_description,
    p.property_use_code,
    p.county_id       AS p_county_id,
    p.gohighlevel_contact_id,
    p.sync_status,
    p.last_crm_sync,
    p.needs_rescore,
    p.created_at      AS p_created_at,
    p.updated_at      AS p_updated_at,

    -- Owner
    o.id              AS o_id,
    o.owner_name,
    o.owner_type,
    o.absentee_status,
    o.ownership_years,
    o.mailing_address,
    o.phone_1,
    o.phone_2,
    o.phone_3,
    o.email_1,
    o.email_2,
    o.linkedin_url,
    o.phone_metadata,
    o.employer_name,
    o.estimated_income,
    o.credit_score_tier,
    o.skip_trace_success,
    o.skip_trace_stale,
    o.direct_mail_eligible,
    o.contact_info_confidence,
    o.contact_info_confidence_score,
    o.contact_last_verified_at,
    o.contact_next_refresh_at,
    o.contact_refresh_status,
    o.contact_refresh_reason,
    o.contactability_detail,
    o.registered_agent_name,
    o.registered_agent_address,
    o.sunbiz_doc_number,
    o.principal_address,
    o.registered_agent_email,
    o.entity_status,
    o.formation_date,
    o.managing_members,
    o.sunbiz_enriched_at,
    o.sunbiz_status,
    o.county_id       AS o_county_id,

    -- Enriched contact (most recent successful trace)
    ec.id             AS ec_id,
    ec.source         AS trace_source,
    ec.mobile_phone,
    ec.landline,
    ec.email          AS traced_email,
    ec.mailing_address AS traced_mailing,
    ec.llc_owner_name,
    ec.relative_contacts,
    ec.raw_response   AS trace_raw_response,
    ec.match_success,
    ec.traced_name,
    ec.trace_type,
    ec.verification_status,
    ec.enriched_at,
    ec.superseded_at,

    -- Distress score (most recent)
    ds.id             AS ds_id,
    ds.final_cds_score,
    ds.lead_tier,
    ds.urgency_level,
    ds.score_date,
    ds.distress_types,
    ds.vertical_scores,
    ds.factor_scores,
    ds.qualified,
    ds.lead_confidence,
    ds.is_guess_lead,
    ds.multiplier,
    ds.scoring_run_id,
    ds.county_id      AS ds_county_id

FROM properties p
LEFT JOIN owners o
    ON o.property_id = p.id
LEFT JOIN LATERAL (
    SELECT *
    FROM enriched_contacts
    WHERE property_id = p.id
      AND match_success = true
    ORDER BY enriched_at DESC
    LIMIT 1
) ec ON true
LEFT JOIN LATERAL (
    SELECT *
    FROM distress_scores
    WHERE property_id = p.id
    ORDER BY score_date DESC
    LIMIT 1
) ds ON true
WHERE p.id = :property_id
""")

SECTIONS = [
    ("PROPERTY", [
        "p_id", "parcel_id", "address", "city", "zip", "state", "jurisdiction",
        "property_type", "year_built", "sq_ft", "beds", "baths", "lot_size",
        "building_condition", "building_class", "heated_sq_ft", "subdivision",
        "hcpa_neighborhood_code", "building_details", "lat", "lon",
        "legal_description", "property_use_code", "p_county_id",
        "gohighlevel_contact_id", "sync_status", "last_crm_sync",
        "needs_rescore", "p_created_at", "p_updated_at",
    ]),
    ("OWNER", [
        "o_id", "owner_name", "owner_type", "absentee_status", "ownership_years",
        "mailing_address", "phone_1", "phone_2", "phone_3", "email_1", "email_2",
        "linkedin_url", "phone_metadata", "employer_name", "estimated_income",
        "credit_score_tier", "skip_trace_success", "skip_trace_stale",
        "direct_mail_eligible", "contact_info_confidence",
        "contact_info_confidence_score", "contact_last_verified_at",
        "contact_next_refresh_at", "contact_refresh_status",
        "contact_refresh_reason", "contactability_detail",
        "registered_agent_name", "registered_agent_address", "sunbiz_doc_number",
        "principal_address", "registered_agent_email", "entity_status",
        "formation_date", "managing_members", "sunbiz_enriched_at",
        "sunbiz_status", "o_county_id",
    ]),
    ("ENRICHED CONTACT", [
        "ec_id", "trace_source", "mobile_phone", "landline", "traced_email",
        "traced_mailing", "llc_owner_name", "relative_contacts",
        "trace_raw_response", "match_success", "traced_name", "trace_type",
        "verification_status", "enriched_at", "superseded_at",
    ]),
    ("DISTRESS SCORE", [
        "ds_id", "final_cds_score", "lead_tier", "urgency_level", "score_date",
        "distress_types", "vertical_scores", "factor_scores", "qualified",
        "lead_confidence", "is_guess_lead", "multiplier", "scoring_run_id",
        "ds_county_id",
    ]),
]


def _fmt(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=6, default=str)
    return str(value)


def print_result(row: dict) -> None:
    for section, fields in SECTIONS:
        print(f"\n  {'-' * 52}")
        print(f"  {section}")
        print(f"  {'-' * 52}")
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            formatted = _fmt(value)
            if "\n" in formatted:
                print(f"  {field}:")
                for line in formatted.splitlines():
                    print(f"    {line}")
            else:
                print(f"  {field:<35} {formatted}")


def main() -> None:
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print("Usage: python scripts/property_detail.py <property_id>")
        sys.exit(1)

    property_id = int(sys.argv[1])

    with get_db_context() as db:
        row = db.execute(QUERY, {"property_id": property_id}).mappings().first()

    if row is None:
        print(f"No property found with id={property_id}")
        sys.exit(1)

    print(f"\n{'=' * 56}")
    print(f"  PROPERTY DETAIL  id={property_id}")
    print(f"{'=' * 56}")
    print_result(dict(row))
    print(f"\n  {'-' * 52}\n")


if __name__ == "__main__":
    main()
