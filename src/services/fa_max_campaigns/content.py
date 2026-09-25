"""Sequence content: merge-field registry, resolver, and CSV loader.

Plan Section 6.8. The merge-field registry is the single source of truth
for both the loader's validation and the writer's one-page reference doc
(render_merge_field_doc(), Build Step 12), so the two can never disagree.

D-3 (resolved by reading src/services/relay/channels_sms.py): the SMS
dispatcher sends `payload["body"]` verbatim with no footer appended (unlike
the email channel, which adds an unsubscribe link automatically in
src/services/relay/channels_email.py). The loader therefore REQUIRES STOP
language inside every SMS step's own body text (plan Section 6.6) — there
is nothing else that will add it.

This module never sends anything and never writes consent.
"""
from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.fa_max_send_governance import GovernanceBlocked, validate_safe_payload

_VALID_CHANNELS = ("email", "sms")
_MERGE_FIELD_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)(?:\|([^}]*))?\s*\}\}")
_STOP_PATTERN = re.compile(r"\bstop\b", re.IGNORECASE)
_SMS_MAX_LEN = 480  # generous multi-segment ceiling; a genuinely long SMS is a content bug, not a platform one


@dataclass(frozen=True)
class MergeField:
    name: str
    required: bool
    description: str = ""


# Fields available on every campaign (plan Section 6.8 table). calendar_link
# and sender_* stay placeholders until Josh confirms his lender seat and the
# outreach domain/footer (client, 23/9) — resolve_merge_values() below
# surfaces that as a normal "missing required" hold, not a code path.
COMMON_MERGE_FIELDS: tuple[MergeField, ...] = (
    MergeField("first_name", required=False, description="Contact's first name"),
    MergeField("sender_name", required=True, description="Josh's outreach display name (placeholder until confirmed)"),
    MergeField("sender_title", required=True, description='"Loan Officer" per client 18/9 #11'),
    MergeField("calendar_link", required=True, description="Booking link (spec item 9: on every outbound)"),
)

# lender_name is intentionally NOT listed for exit_desk yet — plan Q-C3 is
# open (can Exit Desk emails name the person's current lender?). Add it here
# once the client answers; until then the loader rejects any use of it.
CAMPAIGN_MERGE_FIELDS: dict[str, tuple[MergeField, ...]] = {
    "capital_desk_loop": (
        MergeField("entity_name", required=True, description="The buying LLC/entity name from the deed"),
        MergeField("property_street", required=True, description="Street address of the purchased property"),
        MergeField("property_city", required=True, description="City of the purchased property"),
        MergeField("purchase_date", required=True, description="Date of the recorded cash purchase"),
        MergeField("county", required=True, description="County of the purchased property"),
        MergeField("buy_box_zips", required=False, description="Zip codes this investor has bought in"),
        MergeField("buy_box_price_range", required=False, description="This investor's typical purchase price range"),
        MergeField("purchase_count_24m", required=False, description="Number of purchases in the trailing 24 months"),
    ),
    "exit_desk": (
        MergeField("entity_name", required=True, description="The entity borrower on the aging mortgage"),
        MergeField("property_street", required=True, description="Street address of the mortgaged property"),
        MergeField("property_city", required=True, description="City of the mortgaged property"),
        MergeField("county", required=True, description="County of the mortgaged property"),
        MergeField("loan_age_months", required=True, description="Months since the current mortgage was recorded"),
    ),
    "rescue_circuit": (
        MergeField("company_name", required=True, description="The partner's company name (title co., firm, brokerage)"),
        MergeField("partner_type", required=True, description="title_rep / closing_attorney / broker / loan_officer"),
        MergeField("state", required=True, description="Partner's state (FL or GA)"),
    ),
}


def allowed_merge_fields(campaign_key: str) -> set[str]:
    return {f.name for f in COMMON_MERGE_FIELDS} | {f.name for f in CAMPAIGN_MERGE_FIELDS.get(campaign_key, ())}


def required_merge_fields(campaign_key: str) -> set[str]:
    fields = list(COMMON_MERGE_FIELDS) + list(CAMPAIGN_MERGE_FIELDS.get(campaign_key, ()))
    return {f.name for f in fields if f.required}


# ── Row / file validation ────────────────────────────────────────────────────

@dataclass
class LoadError:
    row: Optional[int]
    message: str


def _validate_row(campaign_key: str, row: dict, row_num: int) -> list[LoadError]:
    errors: list[LoadError] = []
    channel = (row.get("channel") or "").strip().lower()
    if channel not in _VALID_CHANNELS:
        errors.append(LoadError(row_num, f"unknown channel {channel!r} — must be one of {_VALID_CHANNELS}"))

    body = row.get("body") or ""
    subject = row.get("subject") or ""
    allowed = allowed_merge_fields(campaign_key)

    for field_text, source in ((body, "body"), (subject, "subject")):
        for match in _MERGE_FIELD_RE.finditer(field_text):
            name = match.group(1)
            if name not in allowed:
                errors.append(LoadError(row_num, f"unknown merge field {{{{{name}}}}} in {source}"))
        try:
            validate_safe_payload({source: field_text})
        except GovernanceBlocked:
            errors.append(LoadError(row_num, f"{source} contains prohibited pricing/terms/financial wording"))

    if channel == "sms":
        if not _STOP_PATTERN.search(body):
            errors.append(LoadError(row_num, 'sms step body must include "Reply STOP" opt-out language'))
        if len(body) > _SMS_MAX_LEN:
            errors.append(LoadError(row_num, f"sms body exceeds {_SMS_MAX_LEN} characters"))
    elif channel == "email" and not subject.strip():
        errors.append(LoadError(row_num, "email step requires a subject"))

    try:
        step = int(row.get("step", ""))
        if step < 1:
            errors.append(LoadError(row_num, "step must be >= 1"))
    except (TypeError, ValueError):
        errors.append(LoadError(row_num, f"step {row.get('step')!r} is not a valid integer"))

    try:
        gap = int(row.get("days_after_previous", ""))
        if gap < 0:
            errors.append(LoadError(row_num, "days_after_previous must be >= 0"))
    except (TypeError, ValueError):
        errors.append(LoadError(row_num, f"days_after_previous {row.get('days_after_previous')!r} is not a valid integer"))

    return errors


def _validate_step_sequence(rows: list[dict]) -> list[LoadError]:
    errors: list[LoadError] = []
    steps = sorted(int(r["step"]) for r in rows if str(r.get("step", "")).strip().lstrip("-").isdigit())
    expected = list(range(1, len(steps) + 1))
    if steps != expected:
        errors.append(LoadError(None, f"step numbers must be 1..N with no gaps — got {steps}"))
    return errors


def parse_sequence_csv(file_path: str, campaign_key: str) -> tuple[list[dict], list[LoadError]]:
    """Read and validate a sequence CSV. Returns (rows, errors). Any error
    means the whole file is refused — plan Section 6.8."""
    with open(file_path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))

    if not rows:
        return [], [LoadError(None, "file has no data rows")]

    errors: list[LoadError] = []
    for idx, row in enumerate(rows, start=2):  # header is row 1
        row_campaign = (row.get("campaign") or "").strip().lower()
        if row_campaign != campaign_key:
            errors.append(LoadError(idx, f"campaign column {row_campaign!r} does not match {campaign_key!r}"))
            continue
        errors.extend(_validate_row(campaign_key, row, idx))

    errors.extend(_validate_step_sequence(rows))
    return rows, errors


def load_sequences(session: Session, file_path: str, campaign_key: str, *, loaded_by: str, dry_run: bool = False) -> int:
    """Load one campaign's sequence CSV as a new version. Refuses the whole
    file on any validation error (returns -1, nothing written). Returns the
    new sequence_version on success, or 0 for a --dry-run that passed."""
    rows, errors = parse_sequence_csv(file_path, campaign_key)
    if errors:
        for err in errors:
            prefix = f"row {err.row}: " if err.row else ""
            print(f"  REJECTED — {prefix}{err.message}", file=sys.stderr)
        return -1

    if dry_run:
        print(f"  OK (dry-run) — {len(rows)} steps validated for {campaign_key!r}, nothing written")
        return 0

    next_version = session.execute(
        text(
            "SELECT COALESCE(MAX(sequence_version), 0) + 1 "
            "FROM fa_max_campaign_sequence_steps WHERE campaign_key = :ck"
        ),
        {"ck": campaign_key},
    ).scalar_one()

    for row in rows:
        session.execute(
            text(
                "INSERT INTO fa_max_campaign_sequence_steps "
                "(campaign_key, sequence_version, step, days_after_previous, channel, subject, body_template, loaded_by) "
                "VALUES (:ck, :ver, :step, :gap, :channel, :subject, :body, :by)"
            ),
            {
                "ck": campaign_key,
                "ver": next_version,
                "step": int(row["step"]),
                "gap": int(row["days_after_previous"]),
                "channel": row["channel"].strip().lower(),
                "subject": (row.get("subject") or "").strip() or None,
                "body": row["body"],
                "by": loaded_by,
            },
        )
    session.commit()
    print(f"  loaded {campaign_key!r} sequence_version={next_version} ({len(rows)} steps)")
    return next_version


# ── Merge value resolution (used by the due-step sweep) ─────────────────────

def resolve_merge_values(session: Session, *, campaign_key: str, enrollment: dict) -> tuple[dict, list[str]]:
    """Resolve every merge field this campaign's steps may reference for one
    enrollment. Returns (values, missing_required). An optional field with
    no resolvable value is simply omitted — the template's own `|fallback`
    supplies the display text (plan Section 6.8)."""
    person_id = enrollment["person_id"]
    values: dict[str, str] = {}

    person = session.execute(
        text("SELECT full_name FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    if person and person[0]:
        values["first_name"] = person[0].split()[0]

    # Sender identity / calendar link: placeholders until Josh confirms his
    # lender seat and the outreach domain/footer (client, 23/9). Deliberately
    # NOT hardcoded here — a settings lookup so the real values can land
    # without touching this module. Missing settings are surfaced as a
    # normal missing-required hold, not a crash.
    from config.settings import get_settings

    settings = get_settings()
    sender_name = getattr(settings, "fa_max_outreach_sender_name", None)
    if sender_name:
        values["sender_name"] = sender_name
    sender_title = getattr(settings, "fa_max_outreach_sender_title", None) or "Loan Officer"
    values["sender_title"] = sender_title
    calendar_link = getattr(settings, "fa_max_calendar_booking_url", None)
    if calendar_link:
        values["calendar_link"] = calendar_link

    if campaign_key in ("capital_desk_loop", "exit_desk"):
        row = session.execute(
            text(
                "SELECT p.address, p.city, p.county_id "
                "FROM properties p WHERE p.id = :pid"
            ),
            {"pid": enrollment.get("property_id")},
        ).fetchone() if enrollment.get("property_id") else None
        if row:
            values["property_street"] = row[0] or ""
            values["property_city"] = row[1] or ""
            values["county"] = row[2] or ""
        entity_name = enrollment.get("entity_name")
        if entity_name:
            values["entity_name"] = entity_name
        if campaign_key == "capital_desk_loop":
            if enrollment.get("purchase_date"):
                values["purchase_date"] = str(enrollment["purchase_date"])
            if enrollment.get("purchase_count_24m") is not None:
                values["purchase_count_24m"] = str(enrollment["purchase_count_24m"])
        if campaign_key == "exit_desk" and enrollment.get("loan_age_months") is not None:
            values["loan_age_months"] = str(enrollment["loan_age_months"])

    if campaign_key == "rescue_circuit":
        for key in ("company_name", "partner_type"):
            if enrollment.get(key):
                values[key] = enrollment[key]
        if enrollment.get("state"):
            values["state"] = enrollment["state"]

    missing_required = sorted(required_merge_fields(campaign_key) - set(values))
    return values, missing_required


def render_template(template: str, values: dict) -> str:
    """Fill {{field}} / {{field|fallback}} placeholders. Caller must have
    already confirmed no required field is missing (resolve_merge_values)."""
    def _sub(match: "re.Match") -> str:
        name, fallback = match.group(1), match.group(2)
        return values.get(name, fallback if fallback is not None else "")
    return _MERGE_FIELD_RE.sub(_sub, template)


# ── Writer-facing reference doc ──────────────────────────────────────────────

def render_merge_field_doc() -> str:
    """One-page reference for the content writer — the format the client
    asked for (23/9): "the merge fields available on each lead type, and the
    format you need sequences in so they load cleanly." Generated from the
    same registry the loader validates against, so the two can't drift."""
    lines = [
        "# FA Max campaign sequences — writer reference",
        "",
        f"_Generated {datetime.now(timezone.utc).date().isoformat()} from "
        "src/services/fa_max_campaigns/content.py — do not hand-edit this file._",
        "",
        "## CSV format (one file per campaign)",
        "",
        "One row per step: `campaign, step, days_after_previous, channel, subject, body, notes`",
        "",
        "- `channel` is `email` or `sms` only.",
        "- `step` starts at 1 with no gaps; `days_after_previous` is whole days from the prior step "
        "(step 1 counts from enrollment).",
        "- Use `{{field_name}}` for a merge field, or `{{field_name|fallback text}}` for an optional one.",
        "- Every `sms` step must include \"Reply STOP\" — nothing else adds it automatically.",
        "- No pricing, rates, terms, or commitments in any subject or body.",
        "",
        "## Fields on every campaign",
        "",
    ]
    for f in COMMON_MERGE_FIELDS:
        req = "required" if f.required else "optional"
        lines.append(f"- `{{{{{f.name}}}}}` ({req}) — {f.description}")

    for campaign_key, fields in CAMPAIGN_MERGE_FIELDS.items():
        lines += ["", f"## {campaign_key}", ""]
        for f in fields:
            req = "required" if f.required else "optional"
            lines.append(f"- `{{{{{f.name}}}}}` ({req}) — {f.description}")

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    load_p = sub.add_parser("load", help="Load a campaign sequence CSV")
    load_p.add_argument("file")
    load_p.add_argument("campaign", choices=list(CAMPAIGN_MERGE_FIELDS))
    load_p.add_argument("--dry-run", action="store_true")

    sub.add_parser("print-doc", help="Print the writer's merge-field reference doc")

    args = parser.parse_args()

    if args.cmd == "print-doc":
        print(render_merge_field_doc())
    elif args.cmd == "load":
        from src.core.database import get_db_context

        with get_db_context() as _session:
            result = load_sequences(
                _session, args.file, args.campaign, loaded_by="cli", dry_run=args.dry_run,
            )
        sys.exit(0 if result >= 0 else 1)
