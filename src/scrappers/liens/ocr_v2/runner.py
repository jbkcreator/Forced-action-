"""OCR v2 runner — post-stage of lien_engine, shadow mode bake-off.

Processes pending legal_and_liens records:
  1. Download PDF by instrument_number from Clerk portal
  2. Extract identifiers using chosen method (A or B)
  3. Validate extraction → composite ocr_confidence
  4. Re-match using OCR evidence (parcel_id → address → legal_desc)
  5. Shadow mode: write enriched CSV only, no DB writes
  6. Live mode: update legal_and_liens + re-match property_id if stronger evidence

Shadow-mode comparison CSV columns (per ADR 0001):
  instrument_number, county_id, method, ocr_status, ocr_confidence,
  extracted_case_number, extracted_parcel_id, extracted_address, extracted_legal_desc,
  current_match_method, current_property_id, current_match_confidence,
  shadow_match_method, shadow_property_id, shadow_match_confidence,
  is_owner_name_upgrade, cost_usd

Usage:
    python -m src.scrappers.liens.ocr_v2.runner \
        --method A \
        --county hillsborough \
        --shadow \
        --limit 50
"""

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.core.database import Database
from src.core.models import LegalAndLien
from src.scrappers.liens.docket_pdf_downloader import download_pdf, cleanup_pdf, DownloadError
from src.scrappers.liens.ocr_v2.extractor import extract, ExtractionResult
from src.scrappers.liens.ocr_v2.validation import compute_confidence

logger = logging.getLogger(__name__)

# Cost estimates (USD per 1k tokens) as of 2026-06-03
_COST_PER_1K = {
    "claude-sonnet-4-6": {"input": 0.003, "output": 0.015},
    "claude-haiku-4-5-20251001": {"input": 0.001, "output": 0.005},
}
# Map the extraction method actually used (per ExtractionResult.method) to the
# model that produced the billed tokens — Method B may fall back to Sonnet.
_EXTRACTION_METHOD_MODEL = {
    "sonnet_pdf": "claude-sonnet-4-6",
    "sonnet_pdf_fallback": "claude-sonnet-4-6",
    "haiku_text": "claude-haiku-4-5-20251001",
}

# Low-confidence threshold — row written as low_confidence, never forced to DB
OCR_MIN_CONFIDENCE = 0.50

# PDF work directory
_PDF_DIR = Path(os.getenv("OCR_V2_PDF_DIR", "data/ocr_v2_pdfs"))


def _estimate_cost(result: ExtractionResult, method: str) -> float:
    """Price tokens at the model that actually ran (fallback-aware)."""
    used = result.method.value if result.method else None
    model = _EXTRACTION_METHOD_MODEL.get(used, "claude-sonnet-4-6")
    rates = _COST_PER_1K.get(model, {"input": 0.003, "output": 0.015})
    return (result.input_tokens / 1000) * rates["input"] + \
           (result.output_tokens / 1000) * rates["output"]


def _is_owner_name_upgrade(
    current_match_method: Optional[str],
    shadow_match_method: Optional[str],
) -> bool:
    """True if this row upgrades from owner_name match to a stronger identifier."""
    strong = {"parcel_id", "normalized_address", "legal_desc", "llm_verified"}
    return (
        current_match_method in {"owner_name", "owner_name_zip", "owner_name_city"}
        and shadow_match_method in strong
    )


def _try_rematch(loader, result: ExtractionResult, confidence: float) -> tuple:
    """Attempt re-match using OCR evidence. Returns (property, match_method, match_confidence)."""
    if confidence < OCR_MIN_CONFIDENCE:
        return None, None, None

    from src.loaders.base import BaseLoader

    prop, method, score = loader.find_property_cascade(
        parcel_id=result.parcel_id,
        address=result.property_address,
        legal_desc=result.legal_description,
        # owner_name intentionally omitted — OCR evidence only
    )
    return prop, method, score


def run_shadow(
    county_id: str,
    method: str,
    limit: int,
    output_csv: Optional[str] = None,
    write_to_db: bool = False,
) -> dict:
    """Run OCR v2 pipeline over pending records.

    Args:
        county_id: 'hillsborough' or 'pinellas'
        method: 'A' or 'B'
        limit: max records to process this run
        output_csv: path to write shadow comparison CSV (auto-generated if None)
        write_to_db: False = shadow mode (CSV only), True = live mode (DB writes)

    Returns:
        Summary stats dict.
    """
    if output_csv is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_csv = f"data/ocr_v2_shadow_{county_id}_method{method}_{ts}.csv"

    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "total": 0, "downloaded": 0, "extracted": 0,
        "low_confidence": 0, "failed_download": 0, "failed_extraction": 0,
        "owner_name_upgrades": 0, "total_cost_usd": 0.0,
        "shadow_mode": not write_to_db,
    }

    db = Database()

    csv_fieldnames = [
        "instrument_number", "county_id", "method", "record_id",
        "ocr_status", "ocr_confidence",
        "extracted_case_number", "extracted_parcel_id",
        "extracted_address", "extracted_legal_desc",
        "current_match_method", "current_property_id", "current_match_confidence",
        "shadow_match_method", "shadow_property_id", "shadow_match_confidence",
        "is_owner_name_upgrade", "cost_usd", "error",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=csv_fieldnames)
        writer.writeheader()

        with db.session_scope() as session:
            # Import here to avoid circular imports
            from src.loaders.base import BaseLoader

            class _TempLoader(BaseLoader):
                def load_from_dataframe(self, df, skip_duplicates=True):
                    return 0, 0, 0

            loader = _TempLoader(session, county_id=county_id)

            # Fetch pending records for this county
            query = (
                session.query(LegalAndLien)
                .filter(
                    LegalAndLien.county_id == county_id,
                    LegalAndLien.instrument_number.isnot(None),
                    LegalAndLien.ocr_status.in_(["pending", None]),
                )
                .order_by(LegalAndLien.id)
                .limit(limit)
            )
            records = query.all()
            logger.info("Processing %d records for %s method=%s", len(records), county_id, method)

            for record in records:
                stats["total"] += 1
                row: dict = {
                    "instrument_number": record.instrument_number,
                    "county_id": county_id,
                    "method": method,
                    "record_id": record.id,
                    "current_match_method": record.match_method,
                    "current_property_id": record.property_id,
                    "current_match_confidence": float(record.match_confidence) if record.match_confidence else None,
                    "error": None,
                }

                # ── 1. Download PDF ────────────────────────────────────────
                try:
                    pdf_url, pdf_path = download_pdf(county_id, record.instrument_number, _PDF_DIR)
                    if write_to_db:
                        record.pdf_url = pdf_url
                        record.ocr_status = "downloaded"
                    stats["downloaded"] += 1
                except DownloadError as exc:
                    logger.warning("Download failed %s: %s", record.instrument_number, exc)
                    row.update({
                        "ocr_status": "failed_download", "ocr_confidence": 0.0,
                        "error": str(exc),
                    })
                    writer.writerow(row)
                    stats["failed_download"] += 1
                    if write_to_db:
                        record.ocr_status = "failed_download"
                    continue

                # ── 2. Extract ─────────────────────────────────────────────
                result: ExtractionResult = extract(pdf_path, method=method)

                if not result.success:
                    logger.warning("Extraction failed %s: %s", record.instrument_number, result.error)
                    row.update({
                        "ocr_status": "failed_extraction", "ocr_confidence": 0.0,
                        "error": result.error,
                    })
                    writer.writerow(row)
                    stats["failed_extraction"] += 1
                    if write_to_db:
                        record.ocr_status = "failed_extraction"
                    continue

                # ── 3. Validate ────────────────────────────────────────────
                validation = compute_confidence(
                    result.to_dict(), record.instrument_number, county_id
                )
                cost = _estimate_cost(result, method)
                stats["total_cost_usd"] += cost

                ocr_status = "extracted" if validation.ocr_confidence >= OCR_MIN_CONFIDENCE else "low_confidence"
                if ocr_status == "low_confidence":
                    stats["low_confidence"] += 1
                else:
                    stats["extracted"] += 1

                row.update({
                    "ocr_status": ocr_status,
                    "ocr_confidence": validation.ocr_confidence,
                    "extracted_case_number": result.case_number,
                    "extracted_parcel_id": result.parcel_id,
                    "extracted_address": result.property_address,
                    "extracted_legal_desc": result.legal_description,
                    "cost_usd": round(cost, 6),
                })

                # ── 4. Re-match ────────────────────────────────────────────
                shadow_prop, shadow_method, shadow_score = _try_rematch(
                    loader, result, validation.ocr_confidence
                )
                row["shadow_match_method"] = shadow_method
                row["shadow_property_id"] = shadow_prop.id if shadow_prop else None
                row["shadow_match_confidence"] = shadow_score
                row["is_owner_name_upgrade"] = _is_owner_name_upgrade(
                    record.match_method, shadow_method
                )
                if row["is_owner_name_upgrade"]:
                    stats["owner_name_upgrades"] += 1

                writer.writerow(row)

                # ── 5. DB writes (live mode only) ──────────────────────────
                if write_to_db and validation.ocr_confidence >= OCR_MIN_CONFIDENCE:
                    record.case_number = result.case_number
                    record.parcel_id = result.parcel_id
                    record.property_address = result.property_address
                    record.normalized_property_address = result.property_address  # TODO normalize
                    record.legal_description = result.legal_description or record.legal_description
                    record.ocr_status = ocr_status
                    record.ocr_confidence = validation.ocr_confidence
                    record.ocr_extracted_at = datetime.now(timezone.utc)

                    # Store extraction method + LLM self-score in meta_data
                    meta = dict(record.meta_data or {})
                    meta["ocr_extraction_method"] = result.method.value if result.method else None
                    meta["ocr_validation_notes"] = validation.notes
                    meta["ocr_parties"] = result.parties
                    meta["ocr_events"] = result.events
                    record.meta_data = meta

                    # Re-match: override if OCR evidence is stronger
                    if shadow_prop and shadow_method and shadow_method not in {
                        "owner_name", "owner_name_zip", "owner_name_city"
                    }:
                        old_method = record.match_method
                        old_pid = record.property_id
                        record.property_id = shadow_prop.id
                        record.match_method = shadow_method
                        record.match_confidence = (shadow_score / 100.0) if shadow_score else None
                        # Audit displaced value
                        meta["ocr_displaced_match"] = {
                            "old_property_id": old_pid,
                            "old_match_method": old_method,
                        }
                        record.meta_data = meta

                elif write_to_db:
                    record.ocr_status = "low_confidence"
                    record.ocr_confidence = validation.ocr_confidence
                    record.ocr_extracted_at = datetime.now(timezone.utc)

                # ── 6. Cleanup local PDF on success ───────────────────────
                if ocr_status == "extracted":
                    cleanup_pdf(pdf_path)
                # Keep PDF on low_confidence or failure for debugging

        # End session scope (commits if write_to_db, no-op in shadow)

    # Print bake-off summary
    total_processed = stats["downloaded"]
    upgrade_rate = (
        stats["owner_name_upgrades"] / total_processed * 100
        if total_processed else 0
    )
    extraction_rate = (
        stats["extracted"] / total_processed * 100
        if total_processed else 0
    )
    avg_cost = (
        stats["total_cost_usd"] / total_processed
        if total_processed else 0
    )

    logger.info(
        "OCR v2 run complete | county=%s method=%s shadow=%s | "
        "total=%d downloaded=%d extracted=%d low_conf=%d "
        "failed_dl=%d failed_ext=%d | "
        "owner_name_upgrades=%d (%.1f%%) | "
        "extraction_rate=%.1f%% | avg_cost=$%.5f | total_cost=$%.4f",
        county_id, method, not write_to_db,
        stats["total"], stats["downloaded"], stats["extracted"],
        stats["low_confidence"], stats["failed_download"], stats["failed_extraction"],
        stats["owner_name_upgrades"], upgrade_rate,
        extraction_rate, avg_cost, stats["total_cost_usd"],
    )
    logger.info("Shadow CSV written to: %s", output_csv)

    stats["upgrade_rate_pct"] = round(upgrade_rate, 2)
    stats["extraction_rate_pct"] = round(extraction_rate, 2)
    stats["output_csv"] = output_csv
    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="OCR v2 runner")
    parser.add_argument("--method", choices=["A", "B"], default="A")
    parser.add_argument("--county", default="hillsborough")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--shadow", action="store_true", default=True)
    parser.add_argument("--live", action="store_true", default=False,
                        help="Write results to DB (disables shadow mode)")
    parser.add_argument("--output-csv", default=None)
    args = parser.parse_args()

    write_to_db = args.live and not args.shadow
    stats = run_shadow(
        county_id=args.county,
        method=args.method,
        limit=args.limit,
        output_csv=args.output_csv,
        write_to_db=write_to_db,
    )
    print("\nBake-off stats:")
    for k, v in stats.items():
        if k != "output_csv":
            print(f"  {k}: {v}")
    sys.exit(0)
