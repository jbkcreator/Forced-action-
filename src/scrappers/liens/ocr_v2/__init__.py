"""OCR v2 — Court docket PDF extraction module.

Two extraction methods behind a config flag:
  Method A: PDF → Sonnet (vision, layout-agnostic)
  Method B: embedded text (pdfplumber) → Haiku → Sonnet fallback if weak

Both methods share download, validation, CSV output, and matching code.
"""
from .extractor import extract, ExtractionResult, ExtractionMethod
from .validation import compute_confidence, ValidationResult

__all__ = [
    "extract",
    "ExtractionResult",
    "ExtractionMethod",
    "compute_confidence",
    "ValidationResult",
]
