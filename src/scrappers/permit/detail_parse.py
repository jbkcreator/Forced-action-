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
    result.project_description = _extract_section_text(soup, "Project Description:")
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
    span = soup.find("span", string=label)
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


def _first_name_line(container: Tag, skip: str) -> Optional[str]:
    """Extract the person/entity name from a section container.

    Strategy: after stripping the label, collect words until we hit a token that
    looks like an address number, phone digit run, or email — all of which signal
    the name portion has ended.
    """
    text = container.get_text(" ", strip=True)
    text = text.replace(skip, "").strip()
    # Remove email tokens first (they can appear inline with name on LP section)
    text = _EMAIL_RE.sub("", text).strip()
    words = text.split()
    name_words: list[str] = []
    for word in words:
        clean = word.rstrip("*,").strip()
        if not clean:
            continue
        # Stop at: standalone digit string (phone), address number (all digits), zip
        digits_only = re.sub(r"\D", "", clean)
        if len(digits_only) >= 7:  # phone or long number
            break
        if clean.isdigit() and len(clean) >= 4:  # address number or zip
            break
        # Stop at company separator on LP line (name/company)
        if "/" in clean and name_words:
            break
        name_words.append(clean)
        # After accumulating a few words, stop at FL/state abbreviation (address start)
        if len(name_words) >= 2 and clean in ("FL", "GA", "TX", "NC", "SC", "CA", "NY"):
            name_words.pop()
            break
    name = " ".join(name_words).strip().rstrip("*,").strip()
    return name if len(name) > 2 else None


def _extract_phone(text: str) -> Optional[str]:
    # Prefer Mobile Phone: number
    m = re.search(r"Mobile Phone[:\s]+(\d[\d\s\-]{8,12}\d)", text, re.IGNORECASE)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if len(digits) >= 10:
            return digits[:10]
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
