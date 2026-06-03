"""OCR v2 confidence validation.

Composite ocr_confidence (0.0–1.0) built from four independent checks:
  1. Parcel ID matches county-specific regex        (+0.25)
  2. Case number matches FL court pattern           (+0.25)
  3. Property address parses to a valid US address  (+0.25)
  4. Extracted instrument# matches CSV instrument#  (+0.25)

LLM self-reported confidence is stored in meta_data only, not used here.
"""
import re
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Florida UCN pattern: YYYY-CA-NNNNNN, YYYY-CC-NNNNNN, etc.
# Also matches short forms (22-CA-2345) and full undashed 20-char UCNs as
# printed on Pinellas recorded judgments, e.g. 522026CT006851000APC
# (CC=county YYYY=year TT=court-type NNNNNN=sequence NNN=party + suffix).
_FL_UCN_RE = re.compile(
    r'\b(?:\d{4}|\d{2})-(?:CA|CC|CF|CJ|CU|DR|MM|CT|SC|MH|CP|GD|GA|GT|AW|RE|AP)-\d{4,8}\b'
    r'|\b\d{6}(?:CA|CC|CF|CJ|CU|DR|MM|CT|SC|MH|CP|GD|GA|GT|AW|RE|AP)\d{6}\w{0,6}\b',
    re.IGNORECASE,
)
# Hillsborough parcel: NN-NN-NN-NNNN-NNNNN-NNNN
_HB_PARCEL_RE = re.compile(r'\b\d{2}-\d{2}-\d{2}-\d{4}-\d{4,5}-\d{4}\b')
# Pinellas parcel: NN/NN/NN/NNNNN/NNN/NNNN
_PINELLAS_PARCEL_RE = re.compile(r'\b\d{2}/\d{2}/\d{2}/\d{5}/\d{3}/\d{4}\b')

_COUNTY_PARCEL_RE = {
    "hillsborough": _HB_PARCEL_RE,
    "pinellas": _PINELLAS_PARCEL_RE,
}


@dataclass
class ValidationResult:
    ocr_confidence: float           # 0.0–1.0 composite
    parcel_valid: bool = False
    case_number_valid: bool = False
    address_valid: bool = False
    instrument_match: bool = False
    notes: list = field(default_factory=list)


def _check_parcel(parcel_id: Optional[str], county_id: str) -> bool:
    if not parcel_id:
        return False
    regex = _COUNTY_PARCEL_RE.get(county_id, _HB_PARCEL_RE)
    return bool(regex.search(parcel_id))


def _check_case_number(case_number: Optional[str]) -> bool:
    if not case_number:
        return False
    return bool(_FL_UCN_RE.search(case_number))


def _check_address(address: Optional[str]) -> bool:
    if not address or len(address) < 5:
        return False
    try:
        import usaddress
        tagged, _ = usaddress.tag(address)
        return "AddressNumber" in tagged
    except ImportError:
        # usaddress not installed — basic heuristic: starts with a number
        return bool(re.match(r'^\d+\s+\w', address.strip()))
    except Exception:
        return False


def _check_instrument_match(
    extracted_instrument: Optional[str],
    csv_instrument: Optional[str],
) -> bool:
    if not extracted_instrument or not csv_instrument:
        return False
    # Normalize: strip all non-alphanumeric chars and compare case-insensitively
    norm = lambda s: re.sub(r'[^A-Za-z0-9]', '', s).upper()
    return norm(extracted_instrument) == norm(csv_instrument)


def compute_confidence(
    extracted: dict,
    csv_instrument_number: Optional[str],
    county_id: str,
) -> ValidationResult:
    """Compute composite ocr_confidence from extracted fields.

    Args:
        extracted: Dict from extractor.extract() (keys: parcel_id, case_number,
                   property_address, instrument_number, ...)
        csv_instrument_number: The instrument number from the source CSV row.
        county_id: 'hillsborough' or 'pinellas'

    Returns:
        ValidationResult with composite score and per-check flags.
    """
    result = ValidationResult(ocr_confidence=0.0)
    score = 0.0

    # 1. Parcel ID regex
    result.parcel_valid = _check_parcel(extracted.get("parcel_id"), county_id)
    if result.parcel_valid:
        score += 0.25
    else:
        result.notes.append("parcel_id missing or invalid format")

    # 2. Case number FL UCN
    result.case_number_valid = _check_case_number(extracted.get("case_number"))
    if result.case_number_valid:
        score += 0.25
    else:
        result.notes.append("case_number missing or not FL UCN format")

    # 3. Address parse
    result.address_valid = _check_address(extracted.get("property_address"))
    if result.address_valid:
        score += 0.25
    else:
        result.notes.append("property_address missing or not parseable")

    # 4. Instrument# cross-check (prevents wrong-document and hallucinated extractions)
    result.instrument_match = _check_instrument_match(
        extracted.get("instrument_number"), csv_instrument_number
    )
    if result.instrument_match:
        score += 0.25
    else:
        result.notes.append(
            f"instrument# mismatch: extracted={extracted.get('instrument_number')!r} "
            f"vs csv={csv_instrument_number!r}"
        )

    result.ocr_confidence = round(score, 4)
    return result
