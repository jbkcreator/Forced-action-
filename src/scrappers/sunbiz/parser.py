"""
Sunbiz detail-page parser — pure function, no I/O.

The scraper engine handles browser orchestration, anti-bot, rate limiting, and
DB writes. This module owns one thing only: turning a Sunbiz entity detail-page
HTML string into a structured `SunbizSnapshot`. It must remain side-effect-free
so the same function can be reused to reparse historical `sunbiz_snapshots.raw_html`
rows whenever the parser improves — without re-hitting the Sunbiz portal.

Status field semantics:
  - 'ok'            — all required fields populated (doc_number, name,
                      registered_agent_name, principal_address)
  - 'partial'       — at least one detailSection parsed, but one or more
                      required fields missing
  - 'parser_failed' — zero detailSections found; HTML did not look like
                      a Sunbiz detail page at all

Bump PARSER_VERSION on any logic change so reparse jobs can identify snapshots
that need reprocessing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal, Optional

from bs4 import BeautifulSoup, Tag

PARSER_VERSION = "sunbiz-parser/1.0.0"

Status = Literal["ok", "partial", "parser_failed"]

_REQUIRED_FIELDS = ("doc_number", "name", "registered_agent_name", "principal_address")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y")


@dataclass
class Member:
    """One row from Authorized Person(s) / Officer/Director / Manager-Member Detail."""
    name: Optional[str] = None
    address: Optional[str] = None
    role: Optional[str] = None   # MGR | MGRM | AMBR | P | VP | DIR | etc. (Sunbiz title code)
    title: Optional[str] = None  # Human-readable role label as displayed


@dataclass
class SunbizSnapshot:
    doc_number: Optional[str] = None
    name: Optional[str] = None
    fei_ein: Optional[str] = None
    entity_status: Optional[str] = None
    formation_date: Optional[date] = None
    principal_address: Optional[str] = None
    mailing_address: Optional[str] = None
    registered_agent_name: Optional[str] = None
    registered_agent_address: Optional[str] = None
    registered_agent_email: Optional[str] = None
    managing_members: list[Member] = field(default_factory=list)
    officers: list[Member] = field(default_factory=list)
    status: Status = "parser_failed"
    parser_version: str = PARSER_VERSION
    warnings: list[str] = field(default_factory=list)

    def to_jsonb(self) -> dict[str, Any]:
        """Serialize to the shape stored in `sunbiz_snapshots.raw_jsonb`."""
        return {
            "doc_number": self.doc_number,
            "name": self.name,
            "fei_ein": self.fei_ein,
            "entity_status": self.entity_status,
            "formation_date": self.formation_date.isoformat() if self.formation_date else None,
            "principal_address": self.principal_address,
            "mailing_address": self.mailing_address,
            "registered_agent_name": self.registered_agent_name,
            "registered_agent_address": self.registered_agent_address,
            "registered_agent_email": self.registered_agent_email,
            "managing_members": [m.__dict__ for m in self.managing_members],
            "officers": [o.__dict__ for o in self.officers],
            "status": self.status,
            "parser_version": self.parser_version,
            "warnings": self.warnings,
        }


# ── public entry point ──────────────────────────────────────────────────────


def parse_sunbiz_detail(html: str) -> SunbizSnapshot:
    """
    Parse a Sunbiz entity detail page. Never raises on malformed input —
    returns a SunbizSnapshot with status='parser_failed' instead.
    """
    snap = SunbizSnapshot()
    if not html or not html.strip():
        snap.warnings.append("empty_html")
        return snap

    soup = BeautifulSoup(html, "html.parser")
    sections = soup.select("div.detailSection")
    if not sections:
        snap.warnings.append("no_detail_sections")
        return snap

    for section in sections:
        header = _section_header(section)
        if not header:
            continue
        h = header.lower()
        try:
            if "filing information" in h:
                _parse_filing_information(section, snap)
            elif "principal address" in h:
                snap.principal_address = _multiline_block(section)
            elif "mailing address" in h:
                snap.mailing_address = _multiline_block(section)
            elif "registered agent" in h:
                _parse_registered_agent(section, snap)
            elif "authorized person" in h or "manager/member detail" in h:
                snap.managing_members.extend(_parse_person_section(section))
            elif "officer/director" in h:
                snap.officers.extend(_parse_person_section(section))
        except Exception as e:    # noqa: BLE001 — never abort whole parse
            snap.warnings.append(f"section_error:{header}:{type(e).__name__}")

    # Entity name often lives in a top-level header outside detailSections.
    if not snap.name:
        snap.name = _extract_entity_name(soup)

    snap.status = _compute_status(snap, sections_found=len(sections))
    return snap


# ── section parsers ─────────────────────────────────────────────────────────


def _parse_filing_information(section: Tag, snap: SunbizSnapshot) -> None:
    """
    Filing Information section. Sunbiz lays this out as label/value pairs in
    pairs of spans or label/value paragraphs. Be tolerant: scan all text for
    known label tokens and grab the next text node.
    """
    pairs = _label_value_pairs(section)
    for label, value in pairs:
        l = label.lower().strip().rstrip(":")
        v = value.strip()
        if not v:
            continue
        if "document number" in l:
            snap.doc_number = v
        elif "fei" in l or "ein number" in l or "fei/ein" in l:
            snap.fei_ein = v if v.upper() not in ("NONE", "N/A", "") else None
        elif l in ("date filed", "filing date"):
            snap.formation_date = _parse_date(v)
        elif l == "status" or l.startswith("status"):
            snap.entity_status = v.upper()


def _parse_registered_agent(section: Tag, snap: SunbizSnapshot) -> None:
    """
    Spans layout: spans[0] = header, spans[1] = agent name, spans[2] = address.
    Email (when present) is typically an <a href="mailto:..."> inside the section.
    """
    spans = section.find_all("span")
    if len(spans) >= 2:
        snap.registered_agent_name = _clean_text(spans[1].get_text())
    if len(spans) >= 3:
        snap.registered_agent_address = _clean_multiline(spans[2].get_text())

    mailto = section.find("a", href=lambda v: v and v.lower().startswith("mailto:"))
    if mailto:
        href = mailto["href"]
        snap.registered_agent_email = href.split(":", 1)[1].strip() or None
    else:
        # Fallback: any email-shaped string in the section text.
        m = _EMAIL_RE.search(section.get_text(" ", strip=True))
        if m:
            snap.registered_agent_email = m.group(0)


def _parse_person_section(section: Tag) -> list[Member]:
    """
    Authorized Person(s) Detail / Officer/Director Detail.

    Sunbiz renders each person as a sub-block with:
      - "Title" line  (e.g. "Title MGR", "Title MGRM", "Title P")
      - name line
      - multi-line address

    Block boundaries are inconsistent across pages; we group by detecting
    each "Title <code>" marker as the start of a new person.
    """
    out: list[Member] = []
    text_lines = [
        l.strip()
        for l in section.get_text("\n", strip=False).split("\n")
        if l.strip()
    ]
    # Drop the section header (first non-empty line containing the section title).
    if text_lines and ("detail" in text_lines[0].lower() or "person" in text_lines[0].lower()):
        text_lines = text_lines[1:]

    current: Optional[Member] = None
    addr_buf: list[str] = []
    for line in text_lines:
        title_match = re.match(r"^Title\s+([A-Z]{1,6})$", line)
        if title_match:
            if current:
                if addr_buf:
                    current.address = "\n".join(addr_buf).strip()
                out.append(current)
            current = Member(role=title_match.group(1), title=title_match.group(1))
            addr_buf = []
            continue
        if not current:
            continue
        if current.name is None:
            current.name = line
        else:
            addr_buf.append(line)

    if current:
        if addr_buf:
            current.address = "\n".join(addr_buf).strip()
        out.append(current)
    return out


# ── helpers ─────────────────────────────────────────────────────────────────


def _section_header(section: Tag) -> Optional[str]:
    first_span = section.find("span")
    if not first_span:
        return None
    return _clean_text(first_span.get_text())


def _multiline_block(section: Tag) -> Optional[str]:
    """Principal/Mailing address sections: spans[1] is the multi-line address."""
    spans = section.find_all("span")
    if len(spans) < 2:
        return None
    return _clean_multiline(spans[1].get_text())


def _label_value_pairs(section: Tag) -> list[tuple[str, str]]:
    """
    Extract label/value pairs from a section. Sunbiz uses both
    <label>label</label><span>value</span> and span-pair layouts depending
    on the page. Walk both.
    """
    pairs: list[tuple[str, str]] = []

    # Layout A: <label>...</label><span>...</span> adjacency.
    for label in section.find_all("label"):
        sibling = label.find_next(["span", "div"])
        if sibling:
            pairs.append((label.get_text(strip=True), sibling.get_text(strip=True)))

    # Layout B: text-node label followed by a span value, all in flat children.
    # Scan the full text and split on common labels.
    text = section.get_text("\n", strip=True)
    label_tokens = (
        "Document Number", "FEI/EIN Number", "FEI Number", "Date Filed",
        "Filing Date", "Status", "Last Event", "Event Date Filed",
    )
    for tok in label_tokens:
        m = re.search(rf"(?<!\w){re.escape(tok)}\s*[:\n]\s*([^\n]+)", text)
        if m:
            pairs.append((tok, m.group(1)))

    return pairs


def _extract_entity_name(soup: BeautifulSoup) -> Optional[str]:
    """Entity name is typically in an <h1>, <h2>, or .corporationName element."""
    for sel in (".corporationName", "h1", "h2", ".search-result-heading"):
        el = soup.select_one(sel)
        if el:
            txt = _clean_text(el.get_text())
            if txt:
                return txt
    return None


def _parse_date(value: str) -> Optional[date]:
    v = value.strip()
    if not v:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    return None


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _clean_multiline(s: str) -> str:
    # Collapse runs of blank lines but preserve line breaks for addresses.
    lines = [re.sub(r"\s+", " ", line).strip() for line in (s or "").splitlines()]
    lines = [l for l in lines if l]
    return "\n".join(lines)


def _compute_status(snap: SunbizSnapshot, sections_found: int) -> Status:
    if sections_found == 0:
        return "parser_failed"
    missing = [f for f in _REQUIRED_FIELDS if not getattr(snap, f)]
    if missing:
        snap.warnings.append(f"missing_required:{','.join(missing)}")
        return "partial"
    return "ok"
