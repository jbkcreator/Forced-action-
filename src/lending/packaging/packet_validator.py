"""R1 1.7 — Packet validator (D6-D12, D19).

Single entry point: ``validate(file_ctx, docs) -> Verdict``.

Three outcome levels (fail-closed):
  pass       — all checks passed; doc may go in a package
  block      — a name, age or readability check failed; admin can override per doc per package
  quarantine — injection-like content detected; only an explicit admin release clears it

The validator is ALWAYS ON.  The injection scan runs at upload time.
Name + age checks run at package compile and again before surfacing (card, view link).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Config knobs (tuneable without code changes — D33)
# ---------------------------------------------------------------------------

# Phrases that trigger quarantine when found anywhere in the extracted text.
# Lower-cased before comparison.
_DEFAULT_INJECTION_PHRASES: tuple[str, ...] = (
    "ignore previous instructions",
    "you are an ai",
    "system prompt",
    "approve this loan",
    "disregard the above",
    "forget everything",
    "act as if",
    "new instructions",
)

# Font size below which text is considered hidden (points).
_HIDDEN_FONT_PT_THRESHOLD = 2.0

# Near-white fill RGB components (each 0-1); above this is considered visible.
_NEAR_WHITE_THRESHOLD = 0.9


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class Outcome(str, Enum):
    PASS = "pass"
    BLOCK = "block"
    QUARANTINE = "quarantine"


@dataclass(frozen=True)
class DocVerdict:
    upload_id: int
    upload_table: str  # 'doc_chase_uploads' | 'partner_file_uploads'
    item_code: Optional[str]
    outcome: Outcome
    reasons: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Verdict:
    """Aggregate verdict for all docs in a package or a single upload."""
    outcome: Outcome  # worst of all per-doc outcomes
    per_doc: tuple[DocVerdict, ...]

    @property
    def quarantined(self) -> list[DocVerdict]:
        return [d for d in self.per_doc if d.outcome is Outcome.QUARANTINE]

    @property
    def blocked(self) -> list[DocVerdict]:
        return [d for d in self.per_doc if d.outcome is Outcome.BLOCK]


@dataclass(frozen=True)
class FileContext:
    """Names we expect to find in documents belonging to this file."""
    file_id: int
    source_type: str  # 'loan_file' | 'lf_lead'
    borrower_names: tuple[str, ...]  # first+last, entity names, guarantors
    # names from OTHER files in the system (to catch cross-contamination — D19)
    other_known_names: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DocInput:
    upload_id: int
    upload_table: str
    item_code: Optional[str]  # None = unclassified partner upload
    stored_path: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _ScanError(Exception):
    """Raised when pdfplumber cannot open or parse the file (fail-closed path)."""


def _extract_text(path: str) -> tuple[str, bool]:
    """Return (full_text, readable).  readable=False when pdfplumber finds no text at all.
    Raises _ScanError when the file cannot be opened (missing, corrupt, permission denied)."""
    try:
        import pdfplumber  # lazy import — not every caller has it in the hot path
    except ImportError:
        raise _ScanError("pdfplumber not installed")

    if not __import__("os.path", fromlist=["exists"]).exists(path):
        raise _ScanError(f"file not found: {path}")

    text_parts: list[str] = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                text_parts.append(t)
    except _ScanError:
        raise
    except Exception as exc:
        raise _ScanError(str(exc)) from exc

    full = "\n".join(text_parts)
    return full, bool(full.strip())


def _has_hidden_text(path: str) -> bool:
    """True when the PDF has white/near-white, tiny-font or off-page text (D10)."""
    try:
        import pdfplumber
    except ImportError:
        return False

    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                width = float(page.width or 612)
                height = float(page.height or 792)
                words = page.extract_words(extra_attrs=["fontname", "size", "non_stroking_color"])
                for w in words:
                    # tiny font
                    size = w.get("size") or 0
                    if float(size) < _HIDDEN_FONT_PT_THRESHOLD and size != 0:
                        return True
                    # off-page
                    x0 = w.get("x0", 0)
                    top = w.get("top", 0)
                    if x0 < 0 or top < 0 or x0 > width or top > height:
                        return True
                    # near-white fill
                    color = w.get("non_stroking_color")
                    if color and isinstance(color, (list, tuple)) and len(color) >= 3:
                        r, g, b = float(color[0]), float(color[1]), float(color[2])
                        if r >= _NEAR_WHITE_THRESHOLD and g >= _NEAR_WHITE_THRESHOLD and b >= _NEAR_WHITE_THRESHOLD:
                            return True
    except Exception:
        return False
    return False


def _has_suspicious_pdf_structure(path: str) -> bool:
    """True when the raw PDF bytes contain JavaScript, embedded files or suspicious annotations."""
    try:
        raw = Path(path).read_bytes()
    except OSError:
        return False

    indicators = (
        b"/JS ",
        b"/JavaScript",
        b"/EmbeddedFile",
        b"/Launch",
        b"/SubmitForm",
        b"/GoToR",
        b"/RichMedia",
        b"/XFA",
    )
    for ind in indicators:
        if ind in raw:
            return True
    return False


def _normalize(name: str) -> str:
    """Strip entity suffixes, punctuation and extra whitespace for fuzzy comparison (reuse D8)."""
    name = name.lower()
    for suffix in ("llc", "inc", "corp", "ltd", "lp", "l.l.c.", "incorporated"):
        name = name.replace(suffix, " ")
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def _names_match(text_lower: str, names: tuple[str, ...]) -> bool:
    """True when at least one normalized name appears in the text."""
    for name in names:
        normalized = _normalize(name)
        if normalized and normalized in text_lower:
            return True
    return False


def _extract_latest_date(text: str) -> Optional[date]:
    """Return the most recent calendar date found in the text (simple regex, no LLM)."""
    patterns = [
        r"\b(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})\b",  # MM/DD/YYYY etc.
        r"\b(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})\b",    # YYYY-MM-DD
    ]
    found: list[date] = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            g = m.groups()
            try:
                if len(g[0]) == 4:  # YYYY-MM-DD
                    d = date(int(g[0]), int(g[1]), int(g[2]))
                else:
                    year = int(g[2])
                    if year < 100:
                        year += 2000
                    d = date(year, int(g[0]), int(g[1]))
                if 2000 <= d.year <= 2100:
                    found.append(d)
            except ValueError:
                pass
    return max(found) if found else None


# ---------------------------------------------------------------------------
# Core checks
# ---------------------------------------------------------------------------

def _injection_scan(path: str, text: str, phrases: tuple[str, ...]) -> list[str]:
    """Return list of quarantine reasons (empty = clean)."""
    reasons: list[str] = []
    text_lower = text.lower()
    for phrase in phrases:
        if phrase in text_lower:
            reasons.append(f"injection phrase: {phrase!r}")
    if _has_hidden_text(path):
        reasons.append("hidden text detected")
    if _has_suspicious_pdf_structure(path):
        reasons.append("suspicious PDF structure (JS/embedded/launch)")
    return reasons


def _name_check(
    text: str,
    file_ctx: FileContext,
) -> list[str]:
    """Return block reasons for name mismatches."""
    reasons: list[str] = []
    text_lower = text.lower()
    if not _names_match(text_lower, file_ctx.borrower_names):
        reasons.append("name not found: document does not name the file's borrower or entity")
    elif _names_match(text_lower, file_ctx.other_known_names):
        reasons.append("names another borrower: document also names a different file's party")
    return reasons


def _age_check(text: str, item_code: Optional[str], today: date, *, bank_max_days: int, pricing_max_days: int) -> list[str]:
    """Return block reasons for stale date, or [] when the item type has no age rule."""
    if item_code not in ("bank_statements", "pricing_sheet"):
        return []
    latest = _extract_latest_date(text)
    if latest is None:
        return [f"no readable date found in {item_code}"]
    age = (today - latest).days
    if item_code == "bank_statements" and age > bank_max_days:
        return [f"bank statement dated {latest} is {age} days old (max {bank_max_days})"]
    if item_code == "pricing_sheet" and age > pricing_max_days:
        return [f"pricing sheet dated {latest} is {age} days old (max {pricing_max_days})"]
    return []


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_injection_only(doc: DocInput, *, phrases: Optional[tuple[str, ...]] = None) -> DocVerdict:
    """Upload-time scan: injection check only. Fail-closed on scan error."""
    _phrases = phrases or _DEFAULT_INJECTION_PHRASES
    try:
        text, readable = _extract_text(doc.stored_path)
    except _ScanError as exc:
        return DocVerdict(
            upload_id=doc.upload_id,
            upload_table=doc.upload_table,
            item_code=doc.item_code,
            outcome=Outcome.QUARANTINE,
            reasons=(f"scan failed: {exc}",),
        )
    except Exception as exc:
        return DocVerdict(
            upload_id=doc.upload_id,
            upload_table=doc.upload_table,
            item_code=doc.item_code,
            outcome=Outcome.QUARANTINE,
            reasons=(f"scan failed: {exc}",),
        )

    q_reasons = _injection_scan(doc.stored_path, text, _phrases)
    if q_reasons:
        return DocVerdict(
            upload_id=doc.upload_id,
            upload_table=doc.upload_table,
            item_code=doc.item_code,
            outcome=Outcome.QUARANTINE,
            reasons=tuple(q_reasons),
        )
    return DocVerdict(
        upload_id=doc.upload_id,
        upload_table=doc.upload_table,
        item_code=doc.item_code,
        outcome=Outcome.PASS,
        reasons=(),
    )


def validate(
    file_ctx: FileContext,
    docs: list[DocInput],
    *,
    today: Optional[date] = None,
    bank_max_days: int = 60,
    pricing_max_days: int = 30,
    phrases: Optional[tuple[str, ...]] = None,
) -> Verdict:
    """Full validation: injection + name + age checks.

    Called at package compile and before surfacing (Slack card, view link).
    Unclassified docs (item_code=None) are BLOCKED from packages as 'unclassified'.
    A scan error quarantines the doc (fail-closed, D10).
    """
    _today = today or date.today()
    _phrases = phrases or _DEFAULT_INJECTION_PHRASES
    per_doc: list[DocVerdict] = []

    for doc in docs:
        # unclassified partner uploads are blocked (D7)
        if doc.item_code is None:
            per_doc.append(DocVerdict(
                upload_id=doc.upload_id,
                upload_table=doc.upload_table,
                item_code=None,
                outcome=Outcome.BLOCK,
                reasons=("unclassified: document type not set by staff",),
            ))
            continue

        # scan
        try:
            text, readable = _extract_text(doc.stored_path)
        except (_ScanError, Exception) as exc:
            per_doc.append(DocVerdict(
                upload_id=doc.upload_id,
                upload_table=doc.upload_table,
                item_code=doc.item_code,
                outcome=Outcome.QUARANTINE,
                reasons=(f"scan failed: {exc}",),
            ))
            continue

        # injection (quarantine)
        q_reasons = _injection_scan(doc.stored_path, text, _phrases)
        if q_reasons:
            per_doc.append(DocVerdict(
                upload_id=doc.upload_id,
                upload_table=doc.upload_table,
                item_code=doc.item_code,
                outcome=Outcome.QUARANTINE,
                reasons=tuple(q_reasons),
            ))
            continue

        # no readable text → block
        if not readable:
            per_doc.append(DocVerdict(
                upload_id=doc.upload_id,
                upload_table=doc.upload_table,
                item_code=doc.item_code,
                outcome=Outcome.BLOCK,
                reasons=("unreadable: no text found (scanned document without OCR)",),
            ))
            continue

        # name check
        block_reasons: list[str] = _name_check(text, file_ctx)

        # age check (bank statements and pricing sheets)
        block_reasons.extend(_age_check(text, doc.item_code, _today,
                                        bank_max_days=bank_max_days,
                                        pricing_max_days=pricing_max_days))

        if block_reasons:
            per_doc.append(DocVerdict(
                upload_id=doc.upload_id,
                upload_table=doc.upload_table,
                item_code=doc.item_code,
                outcome=Outcome.BLOCK,
                reasons=tuple(block_reasons),
            ))
        else:
            per_doc.append(DocVerdict(
                upload_id=doc.upload_id,
                upload_table=doc.upload_table,
                item_code=doc.item_code,
                outcome=Outcome.PASS,
                reasons=(),
            ))

    # aggregate
    if any(d.outcome is Outcome.QUARANTINE for d in per_doc):
        agg = Outcome.QUARANTINE
    elif any(d.outcome is Outcome.BLOCK for d in per_doc):
        agg = Outcome.BLOCK
    else:
        agg = Outcome.PASS

    return Verdict(outcome=agg, per_doc=tuple(per_doc))
