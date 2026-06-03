"""OCR v2 — Two extraction methods for court docket PDFs.

Method A (extract_sonnet_pdf):
  PDF bytes → Anthropic PDF document block → claude-sonnet-4-6 → structured JSON.
  Layout-agnostic; works on scanned and native PDFs.

Method B (extract_text_haiku):
  pdfplumber embedded text → regex pre-parse → claude-haiku-4-5-20251001 → JSON.
  Falls back to Method A when text layer is absent or validation is weak.

Both return ExtractionResult. Record extraction_method in meta_data for bake-off.
"""
import base64
import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Model constants per project model list
_SONNET = "claude-sonnet-4-6"
_HAIKU = "claude-haiku-4-5-20251001"

_EXTRACTION_SCHEMA = """
Return a JSON object with EXACTLY these keys (null if not found):
{
  "case_number": string or null,
  "parcel_id": string or null,
  "property_address": string or null,
  "legal_description": string or null,
  "creditor": string or null,
  "debtor": string or null,
  "amount": number or null,
  "filing_date": string (YYYY-MM-DD) or null,
  "instrument_number": string or null,
  "book_number": string or null,
  "page_number": string or null,
  "court": string or null,
  "parties": [{"name": string, "role": string}] or null,
  "events": [{"date": string, "description": string}] or null,
  "extraction_notes": string or null
}

Rules:
- case_number: Florida court case number (e.g. "2022-CA-001234")
- parcel_id: Florida folio/parcel number (e.g. "29-30-27-0000-00000-0001" or "00/00/00/00000/000/0000")
- property_address: full street address of the subject property
- legal_description: full legal description as written in the document
- creditor: plaintiff, lien holder, or judgment creditor name
- debtor: defendant, property owner, or judgment debtor name
- amount: judgment or lien amount as a number (no currency symbol)
- instrument_number: the document/instrument number on the recorded document
- Return ONLY the JSON object, no commentary.
"""

_SONNET_PROMPT = (
    "You are a legal document data extractor. Extract property and case identifiers "
    "from this Florida court judgment or lien document.\n\n" + _EXTRACTION_SCHEMA
)

_HAIKU_PROMPT_TMPL = (
    "Extract property and case identifiers from this Florida court document text.\n\n"
    "DOCUMENT TEXT:\n{text}\n\n" + _EXTRACTION_SCHEMA
)


class ExtractionMethod(str, Enum):
    SONNET_PDF = "sonnet_pdf"          # Method A
    HAIKU_TEXT = "haiku_text"          # Method B (text layer succeeded)
    SONNET_PDF_FALLBACK = "sonnet_pdf_fallback"  # Method B fallback to A


@dataclass
class ExtractionResult:
    success: bool
    method: Optional[ExtractionMethod] = None
    case_number: Optional[str] = None
    parcel_id: Optional[str] = None
    property_address: Optional[str] = None
    legal_description: Optional[str] = None
    creditor: Optional[str] = None
    debtor: Optional[str] = None
    amount: Optional[float] = None
    filing_date: Optional[str] = None
    instrument_number: Optional[str] = None
    book_number: Optional[str] = None
    page_number: Optional[str] = None
    court: Optional[str] = None
    parties: Optional[list] = None
    events: Optional[list] = None
    extraction_notes: Optional[str] = None
    llm_raw_response: Optional[str] = None
    error: Optional[str] = None
    input_tokens: int = 0
    output_tokens: int = 0

    def to_dict(self) -> dict:
        return {
            "case_number": self.case_number,
            "parcel_id": self.parcel_id,
            "property_address": self.property_address,
            "legal_description": self.legal_description,
            "creditor": self.creditor,
            "debtor": self.debtor,
            "amount": self.amount,
            "filing_date": self.filing_date,
            "instrument_number": self.instrument_number,
            "book_number": self.book_number,
            "page_number": self.page_number,
            "court": self.court,
            "parties": self.parties,
            "events": self.events,
            "extraction_notes": self.extraction_notes,
        }


def _get_anthropic_client():
    try:
        from anthropic import Anthropic
    except ImportError:
        raise ImportError("anthropic package required: pip install anthropic")
    # API key comes from pydantic settings (.env), not raw os.environ
    from config.settings import get_settings
    key = get_settings().anthropic_api_key
    if hasattr(key, "get_secret_value"):  # pydantic SecretStr
        key = key.get_secret_value()
    return Anthropic(api_key=key)


def _parse_llm_json(raw: str) -> dict:
    """Extract JSON from LLM response (strips markdown fences if present)."""
    text = raw.strip()
    # Strip ```json ... ``` fences
    fence = re.search(r'```(?:json)?\s*(.*?)```', text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    # Find first { ... } block
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        text = text[start:end + 1]
    return json.loads(text)


def _result_from_parsed(parsed: dict, method: ExtractionMethod, raw: str, usage) -> ExtractionResult:
    return ExtractionResult(
        success=True,
        method=method,
        case_number=parsed.get("case_number"),
        parcel_id=parsed.get("parcel_id"),
        property_address=parsed.get("property_address"),
        legal_description=parsed.get("legal_description"),
        creditor=parsed.get("creditor"),
        debtor=parsed.get("debtor"),
        amount=float(parsed["amount"]) if parsed.get("amount") is not None else None,
        filing_date=parsed.get("filing_date"),
        instrument_number=parsed.get("instrument_number"),
        book_number=parsed.get("book_number"),
        page_number=parsed.get("page_number"),
        court=parsed.get("court"),
        parties=parsed.get("parties"),
        events=parsed.get("events"),
        extraction_notes=parsed.get("extraction_notes"),
        llm_raw_response=raw,
        input_tokens=getattr(usage, "input_tokens", 0),
        output_tokens=getattr(usage, "output_tokens", 0),
    )


def extract_sonnet_pdf(pdf_path: Path) -> ExtractionResult:
    """Method A: send PDF directly to Sonnet via PDF document block."""
    try:
        pdf_bytes = pdf_path.read_bytes()
        b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

        client = _get_anthropic_client()
        response = client.messages.create(
            model=_SONNET,
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": _SONNET_PROMPT},
                ],
            }],
        )
        raw = response.content[0].text
        parsed = _parse_llm_json(raw)
        return _result_from_parsed(parsed, ExtractionMethod.SONNET_PDF, raw, response.usage)

    except json.JSONDecodeError as exc:
        logger.warning("Method A JSON parse failed: %s", exc)
        return ExtractionResult(success=False, method=ExtractionMethod.SONNET_PDF, error=str(exc))
    except Exception as exc:
        logger.warning("Method A extraction failed: %s", exc)
        return ExtractionResult(success=False, method=ExtractionMethod.SONNET_PDF, error=str(exc))


def _extract_embedded_text(pdf_path: Path) -> Optional[str]:
    """Extract selectable text from PDF using pdfplumber. Returns None if no text layer."""
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            combined = "\n".join(pages_text).strip()
            return combined if len(combined) > 50 else None
    except ImportError:
        logger.warning("pdfplumber not installed; Method B text extraction unavailable")
        return None
    except Exception as exc:
        logger.warning("pdfplumber extraction failed: %s", exc)
        return None


def _regex_preparse(text: str) -> dict:
    """Quick regex pre-parse to give Haiku clean signals before LLM call."""
    hints = {}

    # Florida UCN / case number
    ucn = re.search(
        r'\b(\d{4}-(?:CA|CC|CF|CJ|CU|DR|MM|CT|SC|MH|CP|GD|GA|GT|AW|RE|AP)-\d{4,8})\b',
        text, re.IGNORECASE,
    )
    if ucn:
        hints["hint_case_number"] = ucn.group(1)

    # Hillsborough parcel
    hb = re.search(r'\b(\d{2}-\d{2}-\d{2}-\d{4}-\d{4,5}-\d{4})\b', text)
    if hb:
        hints["hint_parcel_id"] = hb.group(1)

    # Pinellas parcel
    pin = re.search(r'\b(\d{2}/\d{2}/\d{2}/\d{5}/\d{3}/\d{4})\b', text)
    if pin:
        hints["hint_parcel_id"] = pin.group(1)

    # Dollar amounts
    amounts = re.findall(r'\$[\d,]+(?:\.\d{2})?', text)
    if amounts:
        hints["hint_amounts"] = amounts[:5]

    return hints


def extract_text_haiku(pdf_path: Path, fallback_to_sonnet: bool = True) -> ExtractionResult:
    """Method B: pdfplumber text → Haiku → Sonnet-PDF fallback if weak/no text."""
    text = _extract_embedded_text(pdf_path)

    if not text:
        logger.info("Method B: no text layer in %s; falling back to Method A", pdf_path.name)
        if fallback_to_sonnet:
            result = extract_sonnet_pdf(pdf_path)
            if result.success:
                result.method = ExtractionMethod.SONNET_PDF_FALLBACK
            return result
        return ExtractionResult(
            success=False,
            method=ExtractionMethod.HAIKU_TEXT,
            error="No embedded text layer; fallback disabled",
        )

    # Pre-parse hints to help Haiku
    hints = _regex_preparse(text)
    hint_text = ""
    if hints:
        hint_text = "\n\nPRE-PARSED HINTS (verify against document):\n"
        hint_text += "\n".join(f"  {k}: {v}" for k, v in hints.items())

    # Truncate text to avoid Haiku token limits
    truncated = text[:6000] + ("...[truncated]" if len(text) > 6000 else "")

    try:
        client = _get_anthropic_client()
        prompt = _HAIKU_PROMPT_TMPL.format(text=truncated + hint_text)
        response = client.messages.create(
            model=_HAIKU,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text
        parsed = _parse_llm_json(raw)
        result = _result_from_parsed(parsed, ExtractionMethod.HAIKU_TEXT, raw, response.usage)

        # Fall back to Method A if Haiku produced weak extraction
        if fallback_to_sonnet and _is_weak_extraction(result):
            logger.info(
                "Method B: Haiku extraction weak for %s; falling back to Method A",
                pdf_path.name,
            )
            fallback = extract_sonnet_pdf(pdf_path)
            if fallback.success:
                fallback.method = ExtractionMethod.SONNET_PDF_FALLBACK
                # Merge: use Haiku token count + fallback tokens
                fallback.input_tokens += result.input_tokens
                fallback.output_tokens += result.output_tokens
                return fallback

        return result

    except json.JSONDecodeError as exc:
        logger.warning("Method B JSON parse failed: %s", exc)
        if fallback_to_sonnet:
            result = extract_sonnet_pdf(pdf_path)
            if result.success:
                result.method = ExtractionMethod.SONNET_PDF_FALLBACK
            return result
        return ExtractionResult(success=False, method=ExtractionMethod.HAIKU_TEXT, error=str(exc))
    except Exception as exc:
        logger.warning("Method B extraction failed: %s", exc)
        if fallback_to_sonnet:
            result = extract_sonnet_pdf(pdf_path)
            if result.success:
                result.method = ExtractionMethod.SONNET_PDF_FALLBACK
            return result
        return ExtractionResult(success=False, method=ExtractionMethod.HAIKU_TEXT, error=str(exc))


def _is_weak_extraction(result: ExtractionResult) -> bool:
    """True if extraction produced fewer than 2 of the key identifiers."""
    key_fields = [result.parcel_id, result.property_address, result.case_number]
    populated = sum(1 for f in key_fields if f)
    return populated < 2


def extract(
    pdf_path: Path,
    method: str = "A",
    fallback_to_sonnet: bool = True,
) -> ExtractionResult:
    """Dispatch to the configured extraction method.

    Args:
        pdf_path: Local path to the downloaded PDF.
        method: "A" for Sonnet-direct, "B" for text-first-Haiku.
        fallback_to_sonnet: For Method B, whether to fall back to A when weak.
    """
    if method == "A":
        return extract_sonnet_pdf(pdf_path)
    elif method == "B":
        return extract_text_haiku(pdf_path, fallback_to_sonnet=fallback_to_sonnet)
    else:
        raise ValueError(f"Unknown extraction method: {method!r}. Use 'A' or 'B'.")
