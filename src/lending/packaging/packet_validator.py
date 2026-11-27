"""The packet validator: one check every borrower document passes before it can reach a lender package.

``validate`` runs every check (type, injection, readability, names, age); ``validate_injection_only`` is the
upload-time scan. Outcomes: pass, block (an admin may override per document per package) and quarantine (released
only by an explicit admin action). Text comes from pdfplumber only: no OCR, no LLM. Any scan error quarantines.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Optional

from config.lending_packaging import (
    AGED_ITEM_CODES,
    HIDDEN_FONT_PT,
    INJECTION_PHRASES,
    NEAR_WHITE,
    SUSPICIOUS_PDF_MARKERS,
)
from src.services.fa_max_backflip_feed import normalize_entity_name

DOC_CHASE_UPLOADS = "doc_chase_uploads"
PARTNER_FILE_UPLOADS = "partner_file_uploads"
UPLOAD_TABLES = frozenset({DOC_CHASE_UPLOADS, PARTNER_FILE_UPLOADS})

_MIN_HIDDEN_CHARS = 3  # a stray invisible glyph is common in generated PDFs; a hidden sentence is not
_LEGAL_SUFFIXES = frozenset({"LLC", "LLLP", "LLP", "LP", "INC", "INCORPORATED", "CORP", "CORPORATION", "CO",
                             "COMPANY", "LTD", "LIMITED", "PLLC", "PA", "PC"})
_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), start=1)}
_NUMERIC_DATE = re.compile(r"\b(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2}|\d{4})\b")
_ISO_DATE = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")
_NAMED_DATE = re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})\b",
                         re.IGNORECASE)


class Outcome(str, Enum):
    PASS = "pass"
    BLOCK = "block"
    QUARANTINE = "quarantine"


@dataclass(frozen=True)
class DocInput:
    upload_id: int
    upload_table: str
    item_code: Optional[str]  # None: an untagged partner upload
    stored_path: str
    quarantined: Optional[str] = None  # the stored quarantine reason: the file is never opened again
    released: bool = False  # an admin released it from quarantine: the injection scan is not repeated

    @property
    def key(self) -> str:
        return doc_key(self.upload_table, self.upload_id)


@dataclass(frozen=True)
class DocVerdict:
    upload_id: int
    upload_table: str
    item_code: Optional[str]
    outcome: Outcome
    reasons: tuple[str, ...] = ()
    overridden: bool = False

    @property
    def key(self) -> str:
        return doc_key(self.upload_table, self.upload_id)

    @property
    def counts_as_received(self) -> bool:
        return self.outcome is Outcome.PASS and self.item_code is not None


@dataclass(frozen=True)
class Verdict:
    outcome: Outcome
    per_doc: tuple[DocVerdict, ...]

    @property
    def blocked(self) -> list[DocVerdict]:
        return [d for d in self.per_doc if d.outcome is Outcome.BLOCK]

    @property
    def quarantined(self) -> list[DocVerdict]:
        return [d for d in self.per_doc if d.outcome is Outcome.QUARANTINE]

    def to_json(self) -> dict[str, Any]:
        return {"outcome": self.outcome.value, "per_doc": [
            {"upload_id": d.upload_id, "upload_table": d.upload_table, "item_code": d.item_code,
             "outcome": d.outcome.value, "reasons": list(d.reasons), "overridden": d.overridden}
            for d in self.per_doc]}


@dataclass(frozen=True)
class FileContext:
    persons: tuple[str, ...]  # full names of the borrower and guarantors
    entities: tuple[str, ...] = ()
    other_persons: tuple[str, ...] = ()  # full names on other files we hold
    other_entities: tuple[str, ...] = ()


@dataclass(frozen=True)
class AgeLimits:
    bank_statement_days: int = 60
    pricing_sheet_days: int = 30


@dataclass(frozen=True)
class _Scan:
    text: str
    is_pdf: bool
    hidden_chars: int = 0
    side_text: str = ""  # annotation, form-field and metadata strings
    markers: tuple[str, ...] = field(default_factory=tuple)
    hidden_layer: bool = False


class _ScanError(Exception):
    pass


def doc_key(upload_table: str, upload_id: int) -> str:
    return f"{upload_table}:{upload_id}"


def _verdict(doc: DocInput, outcome: Outcome, reasons: Iterable[str] = (), *, overridden: bool = False) -> DocVerdict:
    return DocVerdict(doc.upload_id, doc.upload_table, doc.item_code, outcome, tuple(reasons), overridden)


# ── scanning ──────────────────────────────────────────────────────────────────

def _is_white(color: Any) -> bool:
    if not isinstance(color, (list, tuple)) or not color:
        return False
    try:
        values = [float(c) for c in color]
    except (TypeError, ValueError):
        return False
    if len(values) == 4:  # CMYK: white is no ink
        return all(v <= 1 - NEAR_WHITE for v in values)
    return all(v >= NEAR_WHITE for v in values)


def _is_hidden(char: dict, width: float, height: float) -> bool:
    if not str(char.get("text") or "").strip():
        return False
    size = float(char.get("size") or 0)
    if 0 < size < HIDDEN_FONT_PT:
        return True
    x0, top = float(char.get("x0") or 0), float(char.get("top") or 0)
    if x0 < 0 or top < 0 or x0 > width or top > height:
        return True
    return _is_white(char.get("non_stroking_color"))


def _strings(value: Any) -> list[str]:
    if isinstance(value, bytes):
        return [value.decode("latin-1", "ignore")]
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in _strings(v)]
    if isinstance(value, (list, tuple)):
        return [s for v in value for s in _strings(v)]
    return []


def _hidden_layer(raw: bytes) -> bool:
    """An optional-content layer switched off by default (``/OCProperties`` ... ``/OFF [ refs ]``)."""
    if b"/OCProperties" not in raw:
        return False
    return re.search(rb"/OFF\s*\[\s*\d", raw) is not None


def _scan(path: str) -> _Scan:
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise _ScanError(type(exc).__name__) from exc
    if b"%PDF" not in raw[:1024]:
        return _Scan(text="", is_pdf=False)
    try:
        import pdfplumber
    except ImportError as exc:
        raise _ScanError("pdfplumber unavailable") from exc
    texts: list[str] = []
    side: list[str] = []
    hidden = 0
    try:
        with pdfplumber.open(path) as pdf:
            side.extend(_strings(pdf.metadata))
            for page in pdf.pages:
                texts.append(page.extract_text() or "")
                width, height = float(page.width), float(page.height)
                hidden += sum(1 for char in page.chars if _is_hidden(char, width, height))
                for annot in page.annots or []:
                    side.extend(_strings(annot.get("contents")))
                    data = annot.get("data") or {}
                    for name in ("V", "TU", "T", "Contents"):
                        side.extend(_strings(data.get(name)))
    except Exception as exc:
        raise _ScanError(type(exc).__name__) from exc
    markers = tuple(m.decode() for m in SUSPICIOUS_PDF_MARKERS if re.search(re.escape(m) + rb"(?![A-Za-z])", raw))
    return _Scan(text="\n".join(texts), is_pdf=True, hidden_chars=hidden, side_text="\n".join(side), markers=markers,
                 hidden_layer=_hidden_layer(raw))


def _injection_reasons(scan: _Scan) -> list[str]:
    reasons: list[str] = []
    haystack = f"{scan.text}\n{scan.side_text}".lower()
    reasons.extend(f"instruction phrase: {phrase!r}" for phrase in INJECTION_PHRASES if phrase in haystack)
    if scan.hidden_chars >= _MIN_HIDDEN_CHARS:
        reasons.append(f"hidden text ({scan.hidden_chars} invisible, tiny or off-page characters)")
    if scan.hidden_layer:
        reasons.append("hidden layer (optional content switched off)")
    if scan.markers:
        reasons.append("active or embedded content: " + ", ".join(scan.markers))
    return reasons


# ── names ─────────────────────────────────────────────────────────────────────

def _norm(value: Optional[str]) -> str:
    return normalize_entity_name(value) or ""


def _entity_forms(name: str) -> set[str]:
    full = _norm(name)
    tokens = full.split()
    while tokens and tokens[-1] in _LEGAL_SUFFIXES:
        tokens.pop()
    forms = {full} if full else set()
    if len(tokens) >= 2 or (len(tokens) == 1 and len(tokens[0]) >= 4):
        forms.add(" ".join(tokens))
    return forms


def _person_forms(name: str) -> set[str]:
    full = _norm(name)
    return {full} if len(full.split()) >= 2 else set()  # a bare first or last name never identifies anyone


def _phrases(persons: Iterable[str], entities: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for person in persons:
        out |= _person_forms(person)
    for entity in entities:
        out |= _entity_forms(entity)
    return out


def _mentions(norm_text: str, phrases: set[str]) -> list[str]:
    padded = f" {norm_text} "
    return sorted(p for p in phrases if f" {p} " in padded)


def name_reasons(text: str, ctx: FileContext) -> list[str]:
    norm_text = _norm(text)
    own = _phrases(ctx.persons, ctx.entities)
    others = _phrases(ctx.other_persons, ctx.other_entities) - own
    reasons: list[str] = []
    if not _mentions(norm_text, own):
        reasons.append("name not found: the document does not name the file's borrower, entity or a guarantor")
    if _mentions(norm_text, others):
        reasons.append("names another borrower: the document names a party on a different file")
    return reasons


# ── dates ─────────────────────────────────────────────────────────────────────

def _year(raw: str) -> int:
    year = int(raw)
    return year + 2000 if year < 100 else year


def latest_date(text: str, *, not_after: date) -> Optional[date]:
    """The latest calendar date in ``text`` that is not after ``not_after`` (a due date ahead is not a statement date)."""
    found: list[date] = []
    candidates: list[tuple[int, int, int]] = []
    candidates += [(_year(y), int(m), int(d)) for m, d, y in _NUMERIC_DATE.findall(text)]
    candidates += [(int(y), int(m), int(d)) for y, m, d in _ISO_DATE.findall(text)]
    candidates += [(int(y), _MONTHS[m.lower()[:3]], int(d)) for m, d, y in _NAMED_DATE.findall(text)]
    for year, month, day in candidates:
        try:
            value = date(year, month, day)
        except ValueError:
            continue
        if 2000 <= value.year and value <= not_after:
            found.append(value)
    return max(found) if found else None


def age_reasons(text: str, item_code: Optional[str], today: date, limits: AgeLimits) -> list[str]:
    if item_code not in AGED_ITEM_CODES:
        return []
    dated = latest_date(text, not_after=today)
    if dated is None:
        return [f"no readable date in the {item_code.replace('_', ' ')}"]
    age = (today - dated).days
    limit = limits.bank_statement_days if item_code == "bank_statements" else limits.pricing_sheet_days
    if age > limit:
        return [f"{item_code.replace('_', ' ')} dated {dated} is {age} days old (limit {limit})"]
    return []


# ── entry points ──────────────────────────────────────────────────────────────

def validate_injection_only(doc: DocInput) -> DocVerdict:
    """The upload-time scan: quarantine on injection-like content or any scan error, else pass."""
    try:
        scan = _scan(doc.stored_path)
    except _ScanError as exc:
        return _verdict(doc, Outcome.QUARANTINE, [f"scan failed ({exc})"])
    reasons = _injection_reasons(scan)
    return _verdict(doc, Outcome.QUARANTINE, reasons) if reasons else _verdict(doc, Outcome.PASS)


def _check(doc: DocInput, ctx: FileContext, today: date, limits: AgeLimits) -> DocVerdict:
    if doc.quarantined is not None:
        return _verdict(doc, Outcome.QUARANTINE, [f"quarantined: {doc.quarantined}"])
    if doc.item_code is None:
        return _verdict(doc, Outcome.BLOCK, ["unclassified: staff have not set the document type"])
    try:
        scan = _scan(doc.stored_path)
    except _ScanError as exc:
        return _verdict(doc, Outcome.QUARANTINE, [f"scan failed ({exc})"])
    injection = [] if doc.released else _injection_reasons(scan)
    if injection:
        return _verdict(doc, Outcome.QUARANTINE, injection)
    if not scan.text.strip():
        why = "an image" if not scan.is_pdf else "a PDF with no text layer"
        return _verdict(doc, Outcome.BLOCK, [f"unreadable: {why} (no OCR)"])
    reasons = name_reasons(scan.text, ctx) + age_reasons(scan.text, doc.item_code, today, limits)
    return _verdict(doc, Outcome.BLOCK, reasons) if reasons else _verdict(doc, Outcome.PASS)


def validate(ctx: FileContext, docs: Iterable[DocInput], *, today: date, limits: AgeLimits = AgeLimits(),
             overrides: frozenset[str] = frozenset()) -> Verdict:
    """Every check on every document. ``overrides`` holds the doc keys an admin has cleared for this package; an
    override turns a block into a pass (reasons kept) and never touches a quarantine."""
    per_doc: list[DocVerdict] = []
    for doc in docs:
        checked = _check(doc, ctx, today, limits)
        if checked.outcome is Outcome.BLOCK and doc.key in overrides:
            checked = _verdict(doc, Outcome.PASS, checked.reasons, overridden=True)
        per_doc.append(checked)
    outcomes = {d.outcome for d in per_doc}
    worst = next((o for o in (Outcome.QUARANTINE, Outcome.BLOCK) if o in outcomes), Outcome.PASS)
    return Verdict(worst, tuple(per_doc))
