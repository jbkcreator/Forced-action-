"""
Venture provisioning — CLONE-v2.2 / CL3.

Turns a filled-in copy of config/venture_template.py into `ventures`,
`counties` and `county_sources` rows, so venture #2 is configured by copying
and updating configuration values rather than by writing new code.

    python -m src.services.venture_provisioning --emit-template > venture2.json
    # edit venture2.json
    python -m src.services.venture_provisioning --config venture2.json --dry-run
    python -m src.services.venture_provisioning --config venture2.json --apply

Every write is idempotent, so a partially-completed run is safe to repeat:
`ventures` upserts on venture_key, `counties` and `county_sources` insert with
ON CONFLICT DO NOTHING.

WHAT IS DELIBERATELY NOT CLONED. `playwright_code` (and its version/approved
flags) is never copied — cached Playwright selectors are written against one
specific portal's DOM, and carrying them to a different county's portal would
scrape the wrong page while looking like it worked. Cloned sources land on
scrape_mode='ai_only' so the AI path regenerates code against the real portal.
Column mappings are opt-in for the same reason one level up: same-vendor
portals (e.g. two Accela counties) share headers and benefit, different
vendors do not, so clones land unapproved for an admin to review.

See docs/venture-onboarding.md for the runbook.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Optional

from sqlalchemy import text

from config.venture_template import (
    COUNTY_TEMPLATE,
    REQUIRED_SIGNAL_TYPES,
    VENTURE_TEMPLATE,
    validate_venture_config,
)

logger = logging.getLogger(__name__)

# Columns copied verbatim from the template county's source rows. Excludes
# id/county_id (per-row identity), the playwright_* trio (portal-specific,
# see the module docstring) and created_at/updated_at (set fresh).
_CLONED_SOURCE_COLUMNS = (
    "signal_type",
    "source_name",
    "description",
    "navigation_hint",
    "output_format",
    "date_range_available",
    "frequency",
    "special_flags",
)

# Scrape modes that depend on cached Playwright code. Since the code itself is
# never cloned, a source in one of these modes is downgraded to 'ai_only' so
# the engine regenerates against the new portal instead of running with no
# code (playwright_only) or silently falling back every time.
_PLAYWRIGHT_DEPENDENT_MODES = ("playwright_only", "playwright_then_ai")


def clone_county_sources(
    session,
    *,
    from_county_id: str,
    to_county_id: str,
    url_overrides: Optional[dict[str, str]] = None,
    activate: bool = True,
) -> dict[str, Any]:
    """Copy every ACTIVE county_sources row from one county to another.

    `url_overrides` maps signal_type -> the destination county's portal URL.
    Anything not overridden inherits the template county's URL, which is
    almost always wrong for a different county — those signal_types come back
    in the returned `missing_urls` so the caller can report them rather than
    let a scraper quietly hit the wrong county's portal.

    `missing_signals` is a separate list: REQUIRED_SIGNAL_TYPES the template
    county has no active source row for at all, so nothing is cloned for
    them regardless of `url_overrides`. Computing this only from
    `template_rows` (as `missing_urls` does) would silently under-report —
    a template missing an entire signal_type produces no row to loop over,
    so a partially-configured template county would otherwise come back
    with `missing_urls: []` and read as fully healthy.

    Returns {"cloned": n, "skipped": n, "missing_urls": [signal_type, ...],
    "missing_signals": [signal_type, ...]}. `skipped` counts source rows the
    destination already had (ON CONFLICT), which is what makes a re-run a
    no-op.
    """
    overrides = url_overrides or {}

    template_rows = session.execute(
        text("""
            SELECT signal_type, url, scrape_mode
            FROM county_sources
            WHERE county_id = :from_county AND is_active = true
            ORDER BY signal_type
        """),
        {"from_county": from_county_id},
    ).mappings().all()

    template_signal_types = {row["signal_type"] for row in template_rows}
    missing_signals = sorted(set(REQUIRED_SIGNAL_TYPES) - template_signal_types)
    if missing_signals:
        logger.warning(
            "[venture] template county %s has no active source for required "
            "signal type(s) %s — county %s will have no source for them either",
            from_county_id, missing_signals, to_county_id,
        )

    if not template_rows:
        logger.warning(
            "[venture] template county %s has no active sources — nothing to clone to %s",
            from_county_id, to_county_id,
        )
        return {
            "cloned": 0, "skipped": 0, "missing_urls": [],
            "missing_signals": missing_signals,
        }

    missing_urls = [
        row["signal_type"] for row in template_rows
        if row["signal_type"] not in overrides
    ]

    # One statement for the whole clone: the override map is joined in as a
    # VALUES list so URL substitution happens in SQL rather than in a
    # per-source Python loop.
    #
    # Everything interpolated into the SQL below comes from module constants
    # (_CLONED_SOURCE_COLUMNS, _PLAYWRIGHT_DEPENDENT_MODES) or a loop index —
    # never from caller input. Every caller-supplied value, including each
    # override's signal_type and URL, goes through a named bind parameter.
    columns_sql = ", ".join(_CLONED_SOURCE_COLUMNS)
    override_values = ", ".join(
        f"(:ov_key_{i}, :ov_url_{i})" for i in range(len(overrides))
    )
    override_cte = (
        f"overrides (signal_type, url) AS (VALUES {override_values})"
        if overrides else
        "overrides (signal_type, url) AS (SELECT NULL::varchar, NULL::text WHERE false)"
    )

    params: dict[str, Any] = {
        "from_county": from_county_id,
        "to_county": to_county_id,
        "activate": activate,
        "playwright_modes": list(_PLAYWRIGHT_DEPENDENT_MODES),
    }
    for i, (signal_type, url) in enumerate(overrides.items()):
        params[f"ov_key_{i}"] = signal_type
        params[f"ov_url_{i}"] = url

    result = session.execute(
        text(f"""
            WITH {override_cte}
            INSERT INTO county_sources (
                county_id, {columns_sql}, url, scrape_mode, is_active, created_at
            )
            SELECT
                :to_county,
                {', '.join(f's.{col}' for col in _CLONED_SOURCE_COLUMNS)},
                COALESCE(o.url, s.url),
                CASE WHEN s.scrape_mode = ANY(:playwright_modes)
                     THEN 'ai_only' ELSE s.scrape_mode END,
                :activate,
                now()
            FROM county_sources s
            LEFT JOIN overrides o ON o.signal_type = s.signal_type
            WHERE s.county_id = :from_county AND s.is_active = true
            ON CONFLICT (county_id, signal_type) DO NOTHING
        """),
        params,
    )

    cloned = result.rowcount if result.rowcount is not None else 0
    skipped = len(template_rows) - cloned
    logger.info(
        "[venture] cloned %d source(s) from %s to %s (%d already existed, "
        "%d without a URL override)",
        cloned, from_county_id, to_county_id, skipped, len(missing_urls),
    )
    return {
        "cloned": cloned, "skipped": skipped, "missing_urls": missing_urls,
        "missing_signals": missing_signals,
    }


def clone_column_mappings(
    session,
    *,
    from_county_id: str,
    to_county_id: str,
    signal_types: Optional[list[str]] = None,
) -> int:
    """Copy approved column mappings between two counties' matching sources.

    OPT-IN, and every clone lands is_approved=false: a mapping encodes the
    portal's actual CSV headers, so it only transfers when both counties run
    the same portal vendor. An admin must approve each one in the existing
    mapping UI before a loader will use it.

    Returns the number of mappings written.
    """
    params: dict[str, Any] = {"from_county": from_county_id, "to_county": to_county_id}
    signal_filter = ""
    if signal_types:
        signal_filter = "AND src_s.signal_type = ANY(:signal_types)"
        params["signal_types"] = signal_types

    result = session.execute(
        text(f"""
            INSERT INTO county_column_mappings (
                source_id, source_columns, mapping, is_approved, mapped_by,
                post_processors, value_maps, row_routing, created_at
            )
            SELECT
                dst_s.id, m.source_columns, m.mapping, false, m.mapped_by,
                m.post_processors, m.value_maps, m.row_routing, now()
            FROM county_column_mappings m
            JOIN county_sources src_s ON src_s.id = m.source_id
            JOIN county_sources dst_s
              ON dst_s.county_id = :to_county
             AND dst_s.signal_type = src_s.signal_type
            WHERE src_s.county_id = :from_county
              AND m.is_approved = true
              {signal_filter}
              AND NOT EXISTS (
                  SELECT 1 FROM county_column_mappings existing
                  WHERE existing.source_id = dst_s.id
              )
        """),
        params,
    )
    written = result.rowcount if result.rowcount is not None else 0
    logger.info(
        "[venture] cloned %d column mapping(s) from %s to %s — all unapproved, "
        "review before use", written, from_county_id, to_county_id,
    )
    return written


_UPSERT_VENTURE = """
INSERT INTO ventures (
    venture_key, display_name, brand_name, postal_address, state,
    bankruptcy_court_code, default_bankruptcy_division, template_county_id,
    relay_slack_channel, relay_approvers, relay_instantly_campaign_id,
    relay_instantly_sender_email, relay_send_window_start,
    relay_send_window_end, relay_send_window_timezone, relay_daily_ceiling,
    kill_switch_feature, is_active
)
VALUES (
    :venture_key, :display_name, :brand_name, :postal_address, :state,
    :bankruptcy_court_code, :default_bankruptcy_division, :template_county_id,
    :relay_slack_channel, CAST(:relay_approvers AS jsonb), :relay_instantly_campaign_id,
    :relay_instantly_sender_email, :relay_send_window_start,
    :relay_send_window_end, :relay_send_window_timezone, :relay_daily_ceiling,
    :kill_switch_feature, true
)
ON CONFLICT (venture_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    brand_name = EXCLUDED.brand_name,
    postal_address = EXCLUDED.postal_address,
    state = EXCLUDED.state,
    bankruptcy_court_code = EXCLUDED.bankruptcy_court_code,
    default_bankruptcy_division = EXCLUDED.default_bankruptcy_division,
    template_county_id = EXCLUDED.template_county_id,
    relay_slack_channel = EXCLUDED.relay_slack_channel,
    relay_approvers = EXCLUDED.relay_approvers,
    relay_instantly_campaign_id = EXCLUDED.relay_instantly_campaign_id,
    relay_instantly_sender_email = EXCLUDED.relay_instantly_sender_email,
    relay_send_window_start = EXCLUDED.relay_send_window_start,
    relay_send_window_end = EXCLUDED.relay_send_window_end,
    relay_send_window_timezone = EXCLUDED.relay_send_window_timezone,
    relay_daily_ceiling = EXCLUDED.relay_daily_ceiling,
    kill_switch_feature = EXCLUDED.kill_switch_feature,
    updated_at = now()
RETURNING id
"""

_INSERT_COUNTY = """
INSERT INTO counties (
    county_id, display_name, venture_key, fips, nws_zone, parcel_id_format,
    bankruptcy_division, zip_prefixes, city_filer_keywords,
    address_city_tokens, is_active, created_at
)
VALUES (
    :county_id, :display_name, :venture_key, :fips, :nws_zone, :parcel_id_format,
    :bankruptcy_division, CAST(:zip_prefixes AS jsonb), CAST(:city_filer_keywords AS jsonb),
    CAST(:address_city_tokens AS jsonb), true, now()
)
ON CONFLICT (county_id) DO NOTHING
"""


def upsert_venture(session, venture_cfg: dict[str, Any]) -> int:
    """Write (or overwrite) one `ventures` row. Raises ValueError with every
    problem listed if the config does not validate — better one message
    naming all the gaps than one round trip per gap."""
    problems = validate_venture_config(venture_cfg)
    if problems:
        raise ValueError(
            "invalid venture config: " + "; ".join(problems)
        )

    params = {key: venture_cfg.get(key, default) for key, default in VENTURE_TEMPLATE.items()}
    params["relay_approvers"] = json.dumps(params.get("relay_approvers") or [])
    venture_id = session.execute(text(_UPSERT_VENTURE), params).scalar_one()
    logger.info("[venture] upserted venture %s (id=%d)", venture_cfg["venture_key"], venture_id)
    return venture_id


def _validate_counties(counties: list[dict[str, Any]]) -> list[str]:
    """Problems with the counties block. Catches the mistake the emitted
    template invites: running it without editing the placeholder values, which
    would otherwise create a real county literally named CHANGE_ME."""
    problems: list[str] = []
    placeholders = {COUNTY_TEMPLATE["county_id"], COUNTY_TEMPLATE["display_name"]}

    seen: set[str] = set()
    for index, county in enumerate(counties):
        for field in ("county_id", "display_name"):
            value = county.get(field)
            if not value or not str(value).strip():
                problems.append(f"counties[{index}]: {field} is required")
            elif value in placeholders:
                problems.append(
                    f"counties[{index}]: {field} is still the template "
                    f"placeholder {value!r} — fill it in"
                )

        county_id = county.get("county_id")
        if county_id in seen:
            problems.append(f"counties[{index}]: duplicate county_id {county_id!r}")
        elif county_id:
            seen.add(county_id)

        overrides = county.get("source_url_overrides") or {}
        if not isinstance(overrides, dict):
            problems.append(f"counties[{index}]: source_url_overrides must be an object")

    return problems


def provision_venture(
    session,
    *,
    venture_cfg: dict[str, Any],
    counties: Optional[list[dict[str, Any]]] = None,
    include_column_mappings: bool = False,
) -> dict[str, Any]:
    """Create/update a venture and its counties, cloning each county's source
    set from the venture's `template_county_id`.

    Idempotent end to end. Flushes both config caches on the way out so a
    long-running process picks the new venture up immediately rather than
    after the 5-minute TTL.

    Returns a per-county report, which --dry-run prints and --apply commits.
    """
    from src.utils import county_config, venture_config

    county_problems = _validate_counties(counties or [])
    if county_problems:
        raise ValueError("invalid counties config: " + "; ".join(county_problems))

    upsert_venture(session, venture_cfg)

    venture_key = venture_cfg["venture_key"]
    template_county_id = venture_cfg.get("template_county_id")
    report: dict[str, Any] = {"venture_key": venture_key, "counties": {}}

    for county in counties or []:
        county_id = county["county_id"]

        # _INSERT_COUNTY is ON CONFLICT (county_id) DO NOTHING, which makes a
        # re-run for THIS venture idempotent — but says nothing about a
        # county_id that already belongs to a DIFFERENT venture. Without this
        # check an operator's copy-paste mistake would leave that county
        # attached to its original owner while still falling through to
        # clone_county_sources() below and mixing this venture's template
        # sources into it, corrupting the other venture's scraper config.
        existing_owner = session.execute(
            text("SELECT venture_key FROM counties WHERE county_id = :cid"),
            {"cid": county_id},
        ).scalar()
        if existing_owner is not None and existing_owner != venture_key:
            raise ValueError(
                f"county_id {county_id!r} already belongs to venture "
                f"{existing_owner!r} — refusing to attach it to {venture_key!r} "
                f"or clone sources into it"
            )

        county_params = {
            key: county.get(key, default)
            for key, default in COUNTY_TEMPLATE.items()
            if key != "source_url_overrides"
        }
        county_params["venture_key"] = venture_key
        for json_field in ("zip_prefixes", "city_filer_keywords", "address_city_tokens"):
            county_params[json_field] = json.dumps(county_params.get(json_field) or [])

        inserted = session.execute(text(_INSERT_COUNTY), county_params).rowcount
        county_report: dict[str, Any] = {"county_created": bool(inserted)}

        if template_county_id:
            county_report.update(clone_county_sources(
                session,
                from_county_id=template_county_id,
                to_county_id=county_id,
                url_overrides=county.get("source_url_overrides") or {},
            ))
            if include_column_mappings:
                county_report["column_mappings_cloned"] = clone_column_mappings(
                    session,
                    from_county_id=template_county_id,
                    to_county_id=county_id,
                )
        else:
            county_report["sources_skipped"] = (
                "no template_county_id on the venture — this county's sources "
                "must be created by hand"
            )

        report["counties"][county_id] = county_report

    venture_config.invalidate_cache()
    county_config.invalidate_cache()
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _emit_template() -> int:
    """Print a fillable config to stdout — the start of the copy-and-update
    workflow. Includes one county stub since a venture with no counties has
    nothing to scrape."""
    print(json.dumps(
        {"venture": VENTURE_TEMPLATE, "counties": [COUNTY_TEMPLATE]},
        indent=2,
    ))
    return 0


def _load_config(path: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if "venture" not in payload:
        raise ValueError(
            f"{path}: expected a top-level 'venture' object — start from "
            "`--emit-template`"
        )
    return payload["venture"], payload.get("counties") or []


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.services.venture_provisioning",
        description="Provision a venture's Relay + county/source configuration",
    )
    parser.add_argument(
        "--emit-template", action="store_true",
        help="Print a fillable venture+counties JSON config to stdout and exit",
    )
    parser.add_argument("--config", help="Path to a filled-in JSON config")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate and roll back without committing (default when neither "
             "--dry-run nor --apply is given)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Commit the provisioning writes",
    )
    parser.add_argument(
        "--include-column-mappings", action="store_true",
        help="Also clone the template county's approved column mappings "
             "(lands unapproved — only correct when both counties run the "
             "same portal vendor)",
    )
    args = parser.parse_args(argv)

    # Validator messages contain em-dashes; a Windows console defaults to a
    # codepage that mangles them.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    from src.utils.logger import setup_logging

    setup_logging()

    if args.emit_template:
        return _emit_template()

    if not args.config:
        parser.error("--config is required unless --emit-template is given")

    try:
        venture_cfg, counties = _load_config(args.config)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    problems = validate_venture_config(venture_cfg) + _validate_counties(counties)
    if problems:
        for problem in problems:
            print(f"  invalid: {problem}", file=sys.stderr)
        return 2

    # Explicit session rather than get_db_context(): session_scope() commits
    # on normal exit, and a --dry-run that guarantees nothing is written must
    # own the commit/rollback decision itself rather than rely on a rollback
    # happening to leave the outer commit with nothing to do.
    from src.core.database import db as database

    session = database.get_session()
    try:
        report = provision_venture(
            session,
            venture_cfg=venture_cfg,
            counties=counties,
            include_column_mappings=args.include_column_mappings,
        )
    except Exception as exc:
        session.rollback()
        session.close()
        print(f"provisioning failed: {exc}", file=sys.stderr)
        logger.error("[venture] provisioning failed", exc_info=True)
        return 1

    try:
        if args.apply:
            session.commit()
            print(f"APPLIED:\n{json.dumps(report, indent=2)}")
        else:
            session.rollback()
            print(f"DRY RUN (rolled back — pass --apply to commit):\n{json.dumps(report, indent=2)}")
    finally:
        session.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
