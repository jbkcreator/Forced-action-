"""
Convert PARCEL_SPREADSHEET.xls to CSV format.

This script reads the entire parcel spreadsheet and converts it to CSV format
WITHOUT any filtering. All records and all columns are preserved.
"""

import math
import os
from decimal import Decimal
from pathlib import Path
from typing import List, Dict
import sys

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# Configuration
RAW_FILE = project_root / "data" / "reference" / "PARCEL_SPREADSHEET.xls"
OUTPUT_CSV = project_root / "data" / "processed" / "all_parcels.csv"
RAW_FILE_ENCODING = "cp1252"
CHUNK_SIZE = 50000

DBF_SIGNATURES = {0x02, 0x03, 0x04, 0x05, 0x83, 0x8B, 0x8C}


def _normalize_label(label: str) -> str:
    """Normalize column labels to lowercase with underscores."""
    normalized = ''.join(ch.lower() if ch.isalnum() else '_' for ch in str(label).strip())
    return '_'.join(filter(None, normalized.split('_')))


def _format_cell_value(value) -> str:
    """Format cell values for consistent CSV output."""
    if value is None:
        return ""
    if isinstance(value, Decimal):
        formatted = format(value, 'f').rstrip('0').rstrip('.')
        return formatted or "0"
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
    return str(value)


def _detect_file_type(file_path: Path) -> str:
    """Detect input file type by signature."""
    with open(file_path, 'rb') as fh:
        signature = fh.read(8)

    if not signature:
        raise ValueError(f"Input file {file_path} is empty.")

    first_byte = signature[0]
    if first_byte in DBF_SIGNATURES:
        return 'dbf'
    if signature.startswith(b'\xD0\xCF\x11\xE0'):
        return 'xls'
    if signature.startswith(b'PK\x03\x04'):
        return 'xlsx'
    return 'text'


def _excel_chunk_reader(file_path: Path, chunk_size: int):
    """Read Excel file in chunks, preserving all columns."""
    import xlrd

    logger.info(f"Opening Excel workbook: {file_path}")
    book = xlrd.open_workbook(str(file_path), on_demand=True)
    sheet = book.sheet_by_index(0)
    
    # Read header row
    header = [sheet.cell_value(0, col) for col in range(sheet.ncols)]
    logger.info(f"Found {len(header)} columns in Excel file")
    logger.debug(f"Columns: {header[:10]}..." if len(header) > 10 else f"Columns: {header}")
    
    row_buffer: List[Dict[str, str]] = []
    total_rows = sheet.nrows - 1  # Exclude header
    
    for row_idx in range(1, sheet.nrows):
        row_data: Dict[str, str] = {}
        # Read ALL columns from the Excel file
        for col_idx, col_name in enumerate(header):
            cell_value = sheet.cell_value(row_idx, col_idx)
            row_data[col_name] = _format_cell_value(cell_value)
        row_buffer.append(row_data)

        if len(row_buffer) >= chunk_size:
            df = pd.DataFrame(row_buffer)
            # Normalize column names
            df.columns = [_normalize_label(col) for col in df.columns]
            yield df
            row_buffer = []

    if row_buffer:
        df = pd.DataFrame(row_buffer)
        df.columns = [_normalize_label(col) for col in df.columns]
        yield df

    logger.info(f"Processed {total_rows} rows from Excel file")
    book.release_resources()


def _dbf_chunk_reader(file_path: Path, chunk_size: int):
    """Read DBF file in chunks, preserving all columns."""
    from dbfread import DBF

    logger.info(f"Opening DBF file: {file_path}")
    table = DBF(str(file_path), encoding=RAW_FILE_ENCODING, load=False)
    
    logger.info(f"Found {len(table.field_names)} fields in DBF file")
    logger.debug(f"Fields: {table.field_names[:10]}..." if len(table.field_names) > 10 else f"Fields: {table.field_names}")
    
    row_buffer: List[Dict[str, str]] = []
    
    for record in table:
        # Read ALL fields from the DBF file
        row_data: Dict[str, str] = {name: _format_cell_value(record.get(name)) for name in table.field_names}
        row_buffer.append(row_data)

        if len(row_buffer) >= chunk_size:
            df = pd.DataFrame(row_buffer)
            # Normalize column names
            df.columns = [_normalize_label(col) for col in df.columns]
            yield df
            row_buffer = []

    if row_buffer:
        df = pd.DataFrame(row_buffer)
        df.columns = [_normalize_label(col) for col in df.columns]
        yield df


def _text_chunk_reader(file_path: Path, chunk_size: int):
    """Read text file (CSV, TSV, or fixed-width) in chunks."""
    logger.info(f"Opening text file: {file_path}")
    
    # Try tab-delimited first
    try:
        logger.info("Attempting to read as tab-delimited file...")
        reader = pd.read_csv(
            file_path,
            sep='\t',
            chunksize=chunk_size,
            encoding=RAW_FILE_ENCODING,
            on_bad_lines='skip'
        )
        
        # Test first chunk to verify it works
        first_chunk = next(reader)
        logger.info(f"Successfully read as tab-delimited with {len(first_chunk.columns)} columns")
        
        # Normalize and yield first chunk
        first_chunk.columns = [_normalize_label(col) for col in first_chunk.columns]
        yield first_chunk
        
        # Continue with remaining chunks
        for chunk in reader:
            chunk.columns = [_normalize_label(col) for col in chunk.columns]
            yield chunk
        return
    except Exception as e:
        logger.debug(f"Tab-delimited read failed: {e}")
    
    # Try fixed-width with auto-detection
    try:
        logger.info("Attempting to read as fixed-width file with auto-detection...")
        reader = pd.read_fwf(
            file_path,
            chunksize=chunk_size,
            encoding=RAW_FILE_ENCODING,
            infer_nrows=1000
        )
        
        for chunk in reader:
            chunk.columns = [_normalize_label(col) for col in chunk.columns]
            yield chunk
        return
    except Exception as e:
        logger.debug(f"Fixed-width auto-detection failed: {e}")
    
    # Fallback to comma-delimited
    try:
        logger.info("Attempting to read as comma-delimited file...")
        reader = pd.read_csv(
            file_path,
            chunksize=chunk_size,
            encoding=RAW_FILE_ENCODING,
            on_bad_lines='skip'
        )
        
        for chunk in reader:
            chunk.columns = [_normalize_label(col) for col in chunk.columns]
            yield chunk
        return
    except Exception as e:
        logger.error(f"All text file parsing methods failed: {e}")
        raise ValueError(f"Could not parse text file: {file_path}")


def convert_parcel_to_csv():
    """Convert parcel spreadsheet to CSV format."""
    logger.info("=" * 60)
    logger.info("Parcel Spreadsheet to CSV Converter")
    logger.info("=" * 60)
    
    # Verify input file exists
    if not RAW_FILE.exists():
        logger.error(f"Input file not found: {RAW_FILE}")
        logger.error("Please ensure PARCEL_SPREADSHEET.xls is in data/reference/")
        return
    
    # Create output directory
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    
    # Detect file type
    file_type = _detect_file_type(RAW_FILE)
    logger.info(f"Detected file type: {file_type.upper()}")
    
    # Select appropriate reader
    if file_type == 'dbf':
        logger.info("Using DBF reader (preserving all fields)...")
        reader = _dbf_chunk_reader(RAW_FILE, CHUNK_SIZE)
    elif file_type == 'xls':
        logger.info("Using Excel reader (preserving all columns)...")
        reader = _excel_chunk_reader(RAW_FILE, CHUNK_SIZE)
    elif file_type == 'xlsx':
        logger.error("XLSX files are not supported. Please save as XLS, DBF, or CSV.")
        return
    else:
        logger.info("Using text file reader...")
        reader = _text_chunk_reader(RAW_FILE, CHUNK_SIZE)
    
    # Process chunks
    processed_count = 0
    chunk_count = 0
    
    logger.info(f"Starting conversion to CSV: {OUTPUT_CSV}")
    logger.info(f"Processing in chunks of {CHUNK_SIZE} rows...")
    
    for i, chunk in enumerate(reader):
        chunk_count = i + 1
        
        # Clean up whitespace in string columns
        for col in chunk.columns:
            if chunk[col].dtype == 'object':
                chunk[col] = chunk[col].astype(str).str.strip()
        
        # Write/Append to CSV
        # First chunk creates the file with headers; others append without headers
        write_header = (i == 0)
        write_mode = 'w' if i == 0 else 'a'
        
        chunk.to_csv(OUTPUT_CSV, mode=write_mode, index=False, header=write_header)
        
        processed_count += len(chunk)
        
        # Show column count on first chunk
        if i == 0:
            logger.info(f"Output will contain {len(chunk.columns)} columns")
            logger.info(f"Column names (first 10): {list(chunk.columns[:10])}")
        
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: Processed {processed_count:,} rows ({chunk_count} chunks)...")
    
    # Calculate output file size
    size_mb = OUTPUT_CSV.stat().st_size / (1024 ** 2)
    
    logger.info("=" * 60)
    logger.info("CONVERSION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total Rows Processed: {processed_count:,}")
    logger.info(f"Total Chunks: {chunk_count}")
    logger.info(f"Output File: {OUTPUT_CSV}")
    logger.info(f"Output Size: {size_mb:.2f} MB")
    logger.info(f"All columns preserved in output")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        convert_parcel_to_csv()
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)
