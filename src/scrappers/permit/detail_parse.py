"""Stage B — Accela CapDetail page parser (pure, no network).

Parses the HTML of a single permit detail page and returns a PermitDetail dataclass.
All fields optional — missing sections yield None rather than guessing.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup, Tag

# Pattern: license code like CFC055692, EC13001234, CGC1234567, etc.
_LICENSE_RE = re.compile(r"\b([A-Z]{2,4}\d{5,10})\b")
# Phone digits — 10 consecutive digits
_PHONE_RE = re.compile(r"\b(\d{10})\b")
# Email
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}", re.IGNORECASE)


@dataclass
class PermitDetail:
    licensed_professional_name: Optional[str] = None
    contractor_license: Optional[str] = None
    contractor_license_type: Optional[str] = None
    contractor_phone: Optional[str] = None
    contractor_email: Optional[str] = None
    applicant_name: Optional[str] = None
    owner_name: Optional[str] = None
    job_value: Optional[str] = None
    work_location: Optional[str] = None
    project_description: Optional[str] = None
    completion_status: Optional[str] = None


def parse_permit_detail(html: str) -> PermitDetail:
    soup = BeautifulSoup(html, "html.parser")
    result = PermitDetail()

    result.completion_status = _extract_record_status(soup)
    result.project_description = (
        _extract_section_text(soup, "Project Description:")
        or _extract_section_text(soup, "Description")
    )
    result.work_location = _extract_work_location(soup)
    result.owner_name = _extract_owner_name(soup)

    app_container = _find_section_container(soup, "Applicant:")
    if app_container:
        result.applicant_name = _first_name_line(app_container, skip="Applicant:")

    lp_container = _find_section_container(soup, "Licensed Professional:")
    if lp_container:
        text = lp_container.get_text(" ", strip=True)
        result.licensed_professional_name = _first_name_line(lp_container, skip="Licensed Professional:")
        result.contractor_email = _extract_email(text) or _extract_email(
            (app_container.get_text(" ") if app_container else "")
        )
        result.contractor_phone = _extract_phone(text)
        license_match = _LICENSE_RE.search(text)
        if license_match:
            result.contractor_license = license_match.group(1)
            result.contractor_license_type = _extract_license_type(text, license_match.group(1))

    return result


# ---------------------------------------------------------------------------
# Section helpers
# ---------------------------------------------------------------------------

def _find_section_container(soup: BeautifulSoup, label: str) -> Optional[Tag]:
    # Try exact match first, then without trailing colon (Pasco omits it)
    span = soup.find("span", string=label)
    if not span and label.endswith(":"):
        span = soup.find("span", string=label.rstrip(":"))
    if not span:
        return None
    return span.find_parent(["div", "td"])


def _extract_section_text(soup: BeautifulSoup, label: str) -> Optional[str]:
    container = _find_section_container(soup, label)
    if not container:
        return None
    text = container.get_text(" ", strip=True)
    text = text.replace(label, "").strip()
    return text or None


def _extract_record_status(soup: BeautifulSoup) -> Optional[str]:
    # <span id="...lblRecordStatus">Complete</span>
    tag = soup.find("span", id=re.compile(r"lblRecordStatus$", re.IGNORECASE))
    if tag:
        return tag.get_text(strip=True) or None
    return None


def _extract_work_location(soup: BeautifulSoup) -> Optional[str]:
    container = _find_section_container(soup, "Work Location")
    if not container:
        # Try label-only search
        span = soup.find("span", string=re.compile(r"Work Location", re.IGNORECASE))
        if span:
            container = span.find_parent(["div", "td"])
    if container:
        text = container.get_text(" ", strip=True)
        text = re.sub(r"Work Location:?\s*", "", text, flags=re.IGNORECASE).strip()
        return text or None
    return None


def _extract_owner_name(soup: BeautifulSoup) -> Optional[str]:
    container = _find_section_container(soup, "Owner:")
    if not container:
        return None
    return _first_name_line(container, skip="Owner:")


_STOP_LINE_RE = re.compile(
    r"^(Phone|Mobile|Business|Home|Email|Fax|License|Certified|Registered"
    r"|Contractor|Plumbing|Electric|Roofing|HVAC|Mechanical)",
    re.IGNORECASE,
)
_STATE_CODES = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
                "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
                "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
                "VA","WA","WV","WI","WY"}


def _first_name_line(container: Tag, skip: str) -> Optional[str]:
    """Extract the person/entity name from an Accela section container.

    Accela emits one word (or phrase) per <br/>-delimited line for name fields.
    Strategy:
    1. Replace <br/> → \\n, split into lines.
    2. Skip the label line.
    3. Accumulate word-tokens until a stop condition (phone label, address digit,
       state code, email, license keyword) — max 4 tokens (First [MI] Last [Suffix]).
    4. Join and return.
    """
    for br in container.find_all("br"):
        br.replace_with("\n")
    raw = container.get_text("\n")
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

    # Build label variants to strip (with and without trailing colon)
    skip_variants = [skip, skip.rstrip(":")]

    name_tokens: list[str] = []
    past_label = False

    for ln in lines:
        # Strip label occurrences (with or without colon)
        cleaned = ln
        for sv in skip_variants:
            cleaned = re.sub(re.escape(sv), "", cleaned)
        cleaned = cleaned.strip()
        cleaned = _EMAIL_RE.sub("", cleaned).strip()
        cleaned = cleaned.rstrip("*, ").strip()

        if not cleaned:
            continue

        if not past_label:
            past_label = True
            if not cleaned:
                continue

        # Stop conditions
        if _STOP_LINE_RE.match(cleaned):
            break
        if re.match(r"^\d{2,}", cleaned):  # address number
            break
        if cleaned.upper() in _STATE_CODES:
            break
        digits = re.sub(r"\D", "", cleaned)
        if digits and len(digits) >= len(cleaned) * 0.6:  # phone digits
            break
        # Company separator: Hillsborough uses "COMPANY/PERSON" or "NAME/COMPANY"
        # If we already have a name, a "/" line is the company — stop.
        # If this is the first content line with "/", take the longer part as company
        # and skip (the name was already the previous line or is inline above).
        if "/" in cleaned:
            if name_tokens:
                break  # already have name; "/" line is COMPANY/PERSON — stop
            # No name yet: Hillsborough format "COMPANY/PERSON" — name not usable here
            break

        # Multi-word line after first token = company name, not person name
        if len(cleaned.split()) > 1 and name_tokens:
            break

        # Multi-word first content line (e.g., "KEVIN WELLS" from Hillsborough) = full name
        name_tokens.append(cleaned)
        if len(cleaned.split()) > 1:
            break  # multi-word = complete name token, don't accumulate further

        # Hard cap: 4 tokens (First MI Last Suffix) — stop accumulating
        if len(name_tokens) == 4:
            break

    name = " ".join(name_tokens).strip().rstrip("*,").strip()
    return name if len(name) > 2 else None


def _extract_phone(text: str) -> Optional[str]:
    # Prefer Mobile Phone: number (handles (XXX)XXX-XXXX and XXXXXXXXXX formats)
    m = re.search(r"Mobile Phone[:\s]+([\d\(\)\s\-\.]{7,16})", text, re.IGNORECASE)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if len(digits) >= 10:
            return digits[:10]
    # Fallback: any 10-digit run or (XXX)XXX-XXXX pattern
    m = re.search(r"\((\d{3})\)\s*(\d{3})[-\s]?(\d{4})", text)
    if m:
        return m.group(1) + m.group(2) + m.group(3)
    m = re.search(r"\b(\d{10})\b", text)
    if m:
        return m.group(1)
    return None


def _extract_email(text: str) -> Optional[str]:
    m = _EMAIL_RE.search(text)
    return m.group(0) if m else None


def _extract_license_type(text: str, license_code: str) -> Optional[str]:
    # Accela layout: "Certified Plumbing  CFC055692" — type words precede the code
    idx = text.find(license_code)
    if idx == -1:
        return None
    before = text[:idx].strip()
    # Grab the last word-run before the code (may be multi-word: "Certified Plumbing")
    # Split on 2+ whitespace or phone/email boundaries
    segments = re.split(r"\s{2,}|Mobile Phone[:\s]*|Business Phone[:\s]*", before)
    for seg in reversed(segments):
        seg = seg.strip()
        if seg and not _PHONE_RE.search(seg.replace(" ", "")) and "@" not in seg and len(seg) > 3:
            # seg might be "2708 BROADWAY CENTER BLVD BRANDON, FL, 33510 Certified Plumbing"
            # Take the last run of non-address words
            words = seg.split()
            type_words: list[str] = []
            for w in reversed(words):
                if w.isdigit() or re.search(r"\d{4,}", w):
                    break
                if w.rstrip(",") in ("FL", "GA", "TX", "NC", "SC"):
                    break
                type_words.insert(0, w)
            candidate = " ".join(type_words).strip().rstrip(",")
            if len(candidate) > 3:
                return candidate
    return None
