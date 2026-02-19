import math
import os
from decimal import Decimal
from typing import Dict, List, Union

import pandas as pd

# 1. Configuration
RAW_FILE = os.path.abspath("data/reference/PARCEL_SPREADSHEET.xls")
OUTPUT_CSV = os.path.abspath("data/processed/absentee_owners.csv")
RAW_FILE_ENCODING = "cp1252"
CHUNK_SIZE = 50000

# Exact character positions from the HCPA fixed-width format
COL_SPECS = [
    (0, 10),    # FOLIO
    (68, 143),  # OWNER
    (143, 218), # ADDR_1 (Mailing Address Line 1)
    (344, 419), # SITE_ADDR (Physical Property Address)
    (647, 666), # JUST (Market Value)
]

COL_NAMES = ['folio', 'owner', 'mailing_addr', 'site_addr', 'market_value']
DBF_SIGNATURES = {0x02, 0x03, 0x04, 0x05, 0x83, 0x8B, 0x8C}
COLUMN_ALIAS_MAP: Dict[str, str] = {
    'folio': 'folio',
    'folio_id': 'folio',
    'folio_number': 'folio',
    'folio_no': 'folio',
    'owner': 'owner',
    'owner_name': 'owner',
    'mailing_addr': 'mailing_addr',
    'mailing_address': 'mailing_addr',
    'mailing_address_line_1': 'mailing_addr',
    'mailing_address1': 'mailing_addr',
    'addr_1': 'mailing_addr',
    'site_addr': 'site_addr',
    'site_address': 'site_addr',
    'site_address_line_1': 'site_addr',
    'situs_address': 'site_addr',
    'physical_address': 'site_addr',
    'just': 'market_value',
    'just_value': 'market_value',
    'market_value': 'market_value',
    'marketvalue': 'market_value',
}
LookupValue = Union[int, str]


def _normalize_label(label: str) -> str:
    normalized = ''.join(ch.lower() if ch.isalnum() else '_' for ch in str(label).strip())
    return '_'.join(filter(None, normalized.split('_')))


def _resolve_column_targets(normalized_lookup: Dict[str, LookupValue]) -> Dict[str, LookupValue]:
    resolved: Dict[str, LookupValue] = {}

    for alias, target in COLUMN_ALIAS_MAP.items():
        if alias in normalized_lookup and target not in resolved:
            resolved[target] = normalized_lookup[alias]

    missing = [col for col in COL_NAMES if col not in resolved]
    if missing:
        raise KeyError(f"Input is missing required columns: {missing}")

    return resolved


def _resolve_excel_columns(header_row: List[str]) -> Dict[str, int]:
    normalized_header = {_normalize_label(name): idx for idx, name in enumerate(header_row)}
    return _resolve_column_targets(normalized_header)


def _format_cell_value(value) -> str:
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


def _excel_chunk_reader(file_path: str, chunk_size: int):
    import xlrd

    book = xlrd.open_workbook(file_path, on_demand=True)
    sheet = book.sheet_by_index(0)
    header = [sheet.cell_value(0, col) for col in range(sheet.ncols)]
    column_map = _resolve_excel_columns(header)

    row_buffer: List[Dict[str, str]] = []
    for row_idx in range(1, sheet.nrows):
        row_data: Dict[str, str] = {}
        for target, col_idx in column_map.items():
            cell_value = sheet.cell_value(row_idx, col_idx)
            row_data[target] = _format_cell_value(cell_value)
        row_buffer.append(row_data)

        if len(row_buffer) >= chunk_size:
            yield pd.DataFrame(row_buffer)[COL_NAMES]
            row_buffer = []

    if row_buffer:
        yield pd.DataFrame(row_buffer)[COL_NAMES]

    book.release_resources()


def _detect_file_type(file_path: str) -> str:
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


def _dbf_chunk_reader(file_path: str, chunk_size: int):
    from dbfread import DBF

    table = DBF(file_path, encoding=RAW_FILE_ENCODING, load=False)
    column_map = _resolve_column_targets({_normalize_label(name): name for name in table.field_names})

    row_buffer: List[Dict[str, str]] = []
    for record in table:
        row_data: Dict[str, str] = {}
        for target, source_name in column_map.items():
            row_data[target] = _format_cell_value(record.get(source_name))
        row_buffer.append(row_data)

        if len(row_buffer) >= chunk_size:
            yield pd.DataFrame(row_buffer)[COL_NAMES]
            row_buffer = []

    if row_buffer:
        yield pd.DataFrame(row_buffer)[COL_NAMES]


def _build_chunk_reader():
    file_type = _detect_file_type(RAW_FILE)

    if file_type == 'dbf':
        print("[*] Detected DBF input; streaming rows via dbfread...")
        return _dbf_chunk_reader(RAW_FILE, CHUNK_SIZE)
    if file_type == 'xls':
        print("[*] Detected Excel input; streaming rows directly from workbook...")
        return _excel_chunk_reader(RAW_FILE, CHUNK_SIZE)
    if file_type == 'xlsx':
        raise ValueError("XLSX files are not supported. Save as XLS, DBF, or fixed-width text before running the filter.")

    print("[*] Detected fixed-width text input; reading via pandas.read_fwf()...")
    return pd.read_fwf(
        RAW_FILE,
        colspecs=COL_SPECS,
        names=COL_NAMES,
        chunksize=CHUNK_SIZE,
        dtype={'folio': str},
        encoding=RAW_FILE_ENCODING
    )

def run_absentee_filter():
    print(f"[*] Starting the Great Filter: 500k rows -> Absentee Leads")
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # 2. The Chunking Engine
    reader = _build_chunk_reader()

    processed_count = 0
    absentee_count = 0

    # 3. Processing Loop
    for i, chunk in enumerate(reader):
        # Clean up whitespace from upstream formatting quirks
        for col in ['owner', 'mailing_addr', 'site_addr']:
            chunk[col] = chunk[col].str.strip()

        # APPLY LOGIC: If addresses don't match, it's an absentee owner
        is_absentee_mask = chunk['mailing_addr'] != chunk['site_addr']
        absentee_chunk = chunk[is_absentee_mask].copy()

        # Write/Append to CSV
        # First chunk creates the file with headers; others append without headers
        write_header = (i == 0)
        write_mode = 'w' if i == 0 else 'a'
        
        absentee_chunk.to_csv(OUTPUT_CSV, mode=write_mode, index=False, header=write_header)

        processed_count += len(chunk)
        absentee_count += len(absentee_chunk)
        print(f"    Progress: {processed_count} rows processed. Found {absentee_count} absentee leads so far...")

    print(f"\n[+] FILTERING COMPLETE.")
    print(f"[+] Total Rows Processed: {processed_count}")
    print(f"[+] Absentee Leads Saved: {absentee_count}")
    print(f"[+] Output Location: {OUTPUT_CSV}")

if __name__ == "__main__":
    run_absentee_filter()