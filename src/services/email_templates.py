"""
Email sequence template service.

Handles variable validation, template CRUD logic, and expansion of a
template's steps into the Instantly API sequence payload.

Allowed personalization variables (map to dbpr_contacts fields):
    {{firstName}}   — Primary Name first token
    {{lastName}}    — Primary Name last token
    {{company}}     — Company Name / DBA (falls back to Primary Name)
    {{city}}        — dbpr_contacts.city
    {{licenseType}} — dbpr_contacts.license_type_desc
"""

import re
from typing import Optional

# Canonical whitelist: template variable → dbpr_contacts field
ALLOWED_VARIABLES: dict[str, str] = {
    "firstName":   "full_name (first token)",
    "lastName":    "full_name (last token)",
    "company":     "company_name / full_name fallback",
    "city":        "city",
    "licenseType": "license_type_desc",
}

_VAR_RE = re.compile(r"\{\{(\w+)\}\}")


def extract_variables(text: str) -> list[str]:
    """Return unique variable names found in text (e.g. ['firstName', 'company'])."""
    return list(dict.fromkeys(_VAR_RE.findall(text)))


def validate_variables(steps: list[dict]) -> list[str]:
    """
    Check all {{var}} occurrences in every step's subject + body against the
    whitelist. Returns list of unknown variable names (empty = valid).
    """
    unknown: list[str] = []
    for step in steps:
        for field in ("subject", "body"):
            for var in extract_variables(step.get(field, "")):
                if var not in ALLOWED_VARIABLES and var not in unknown:
                    unknown.append(var)
    return unknown


def collect_variables_used(steps: list[dict]) -> list[str]:
    """Return sorted unique list of all whitelisted variables found in steps."""
    found: set[str] = set()
    for step in steps:
        for field in ("subject", "body"):
            for var in extract_variables(step.get(field, "")):
                if var in ALLOWED_VARIABLES:
                    found.add(var)
    return sorted(found)


def build_instantly_sequence(steps: list[dict]) -> list[dict]:
    """
    Expand template steps into the Instantly sequence steps format.
    Input step: {step_number, delay_days, subject, body}
    Output:     {step_number, type, delay_days, subject, body}
    """
    return [
        {
            "step_number": step.get("step_number", idx + 1),
            "type":        "email",
            "delay_days":  step.get("delay_days", 0),
            "subject":     step.get("subject", ""),
            "body":        step.get("body", ""),
        }
        for idx, step in enumerate(steps)
    ]


def resolve_contact_variables(template_str: str, contact: dict) -> str:
    """
    Resolve {{var}} placeholders in a string using contact field values.
    contact dict keys: full_name, company_name, city, license_type_desc.
    Falls back gracefully when fields are absent.
    """
    full_name = (contact.get("full_name") or "").strip()
    parts = full_name.split(",", 1)  # LAST, FIRST format
    if len(parts) == 2:
        first = parts[1].strip().split()[0] if parts[1].strip() else ""
        last = parts[0].strip()
    else:
        tokens = full_name.split()
        first = tokens[0] if tokens else ""
        last = tokens[-1] if len(tokens) > 1 else ""

    values: dict[str, str] = {
        "firstName":   first,
        "lastName":    last,
        "company":     (contact.get("company_name") or first or full_name).strip(),
        "city":        (contact.get("city") or "").strip(),
        "licenseType": (contact.get("license_type_desc") or "").strip(),
    }

    def replacer(match: re.Match) -> str:
        return values.get(match.group(1), match.group(0))

    return _VAR_RE.sub(replacer, template_str)
