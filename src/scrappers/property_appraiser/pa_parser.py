"""
HCPA Property Appraiser Parser — pure parsing, no I/O.

Converts raw scraped content (HCPA page text/HTML, TRIM PDF path,
tax collector text) into a single-row canonical DataFrame ready for
PropertyAppraiserLoader.load_from_dataframe().

Key techniques:
  HCPA page:   regex on visible text + BeautifulSoup on HTML for table data
  TRIM PDF:    pdfplumber form field ANNOTATIONS (not extract_text — the text
               layer has no values in TRIM PDFs; all data is in T/V annot fields)
  Tax page:    structured text parsing
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(s) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    return s if s else None


def _parse_amount(s) -> Optional[float]:
    if not s:
        return None
    s = re.sub(r"[,$\s]", "", str(s))
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _after_label(text: str, label: str, max_chars: int = 80) -> Optional[str]:
    """Return the first non-whitespace token after 'label:' in visible page text."""
    pattern = re.compile(re.escape(label) + r"\s*:?\s*(.{1," + str(max_chars) + r"})", re.IGNORECASE)
    m = pattern.search(text)
    return _clean(m.group(1).split("\n")[0]) if m else None


# ---------------------------------------------------------------------------
# 1. HCPA property page
# ---------------------------------------------------------------------------

def parse_hcpa_page(text: str, html: str) -> dict:
    """
    Extract canonical fields from the HCPA property page.

    Field patterns derived from observed live page text structure:
      - Tab-separated key\tvalue pairs for property identifiers
      - Tab-separated building characteristics table
      - Fixed sales history table (columns: book/page, number, MM, YYYY, type, qual, vac/imp, price)
      - Building sub-areas totals line: "Totals\tGROSS\tHEATED\t$VALUE"
      - Land lines: "LN\tUseCode\tDesc\tZone\tFront\tDepth\tUnitType\tTotalUnits\tLandValue"
    """
    from bs4 import BeautifulSoup

    data: dict = {}
    t = text or ""
    soup = BeautifulSoup(html or "", "html.parser")

    # --- Owner name: ALL-CAPS lines between "GOOGLE STREET VIEW" and "Mailing Address" ---
    m = re.search(r"GOOGLE STREET VIEW\s*\n((?:[A-Z][A-Z &'\-]+\n)+)Mailing Address", t)
    if m:
        names = [l.strip() for l in m.group(1).strip().split("\n") if l.strip()]
        data["owner_name"] = " & ".join(names)

    # --- Addresses: lines immediately after their label on the next non-blank line ---
    m = re.search(r"Mailing Address\s*\n\s*\n(.+?)\n(.+?)\n\s*\nSite Address", t, re.DOTALL)
    if m:
        data["mailing_address"] = m.group(1).strip()

    m = re.search(r"Site Address\s*\n\s*\n(.+?)\n", t)
    if m:
        data["site_address"] = m.group(1).strip()

    # --- Tab-separated identifier fields (e.g. "Property Use:\t0100 SINGLE FAMILY R") ---
    m = re.search(r"Property Use:\s+(\d{4})", t)
    if m:
        data["property_use_code"] = m.group(1)

    m = re.search(r"Neighborhood:\s+(.+)", t)
    if m:
        data["neighborhood_code"] = m.group(1).strip()

    m = re.search(r"Subdivision:\s+(.+)", t)
    if m:
        data["subdivision"] = m.group(1).strip()

    # --- Market value from value summary table: "County\t$314,080\t..." ---
    m = re.search(r"^County\s+\$([\d,]+)", t, re.MULTILINE)
    if m:
        data["market_value"] = _parse_amount(m.group(1))

    # County assessed value (second column in County row)
    m = re.search(r"^County\s+\$[\d,]+\s+\$([\d,]+)", t, re.MULTILINE)
    if m:
        data["county_assessed_value"] = _parse_amount(m.group(1))

    # School taxable value (second column in Public Schools row)
    m = re.search(r"^Public Schools\s+\$[\d,]+\s+\$[\d,]+\s+\$[\d,]+\s+\$([\d,]+)", t, re.MULTILINE)
    if m:
        data["school_taxable_value"] = _parse_amount(m.group(1))

    # --- Homestead ---
    data["homestead_exempt"] = bool(re.search(r"\bHX\b|\bHomestead\b", t, re.IGNORECASE))

    # --- Building characteristics (tab-separated: "Label\tCode\tDescription") ---
    data["year_built"] = _parse_int(_tab_field(t, "Year Built"))

    m = re.search(r"^Class\s+([A-Z])\s", t, re.MULTILINE)
    if m:
        data["building_class"] = m.group(1)

    m = re.search(r"^Condition\s+\d+\s+(.+?)(?:\t|\n|$)", t, re.MULTILINE)
    if m:
        data["building_condition"] = m.group(1).strip()

    # Beds/baths: "Bedrooms\t4.0" — take integer part of the first number
    m = re.search(r"^Bedrooms\s+(\d+)", t, re.MULTILINE)
    if m:
        data["beds"] = int(m.group(1))

    m = re.search(r"^Bathrooms\s+([\d.]+)", t, re.MULTILINE)
    if m:
        data["baths"] = _parse_float(m.group(1))

    # --- Building sub-area totals: "Totals\t2,344\t1,808\t$205,352" ---
    m = re.search(r"^Totals\s+([\d,]+)\s+([\d,]+)\s+\$", t, re.MULTILINE)
    if m:
        data["gross_sq_ft"] = _parse_float(m.group(1).replace(",", ""))
        data["heated_sq_ft"] = _parse_float(m.group(2).replace(",", ""))

    # --- Land lines: last column is land value, second-to-last is total units ---
    # Format: "1\tREJ0\tDesc\tZone\tFront\tDepth\tUnitType\tTotalUnits\t$LandValue"
    m = re.search(r"Total Land Units\s+Land Value\s*\n\d+\s+\S+\s+[^\t\n]+\t\S+\t[\d.]+\t\d+\t[^\t\n]+\t([\d,]+\.?\d*)\t\$([\d,]+\.?\d*)", t)
    if m:
        data["lot_size"] = _parse_float(m.group(1))
        data["land_value"] = _parse_amount(m.group(2))

    # --- Legal description: strip leading row number ---
    m = re.search(r"LN\s+Legal Description\s*\n\d+\s+(.+?)(?:\n|$)", t)
    if m:
        data["legal_description"] = m.group(1).strip()

    # --- Sales history: find most recent Qualified sale ---
    sale = _parse_most_recent_qualified_sale(t)
    if sale:
        data.update(sale)

    # --- Extra building features ---
    data["building_details"] = _extract_building_details_bs4(soup, t)

    return {k: v for k, v in data.items() if v is not None}


# ---------------------------------------------------------------------------
# 1b. PCPAO property page (Pinellas)
# ---------------------------------------------------------------------------

def parse_pcpao_page(text: str, html: str) -> dict:
    """
    Extract canonical fields from the Pinellas County Property Appraiser page.

    PCPAO renders stable section/table IDs in the property detail HTML, so this
    parser reads those sections with BeautifulSoup and emits the same canonical
    keys the HCPA loader already consumes.
    """
    from bs4 import BeautifulSoup

    data: dict = {}
    soup = BeautifulSoup(html or "", "html.parser")

    def by_id(node_id: str) -> Optional[str]:
        node = soup.find(id=node_id)
        if not node:
            return None
        value = node.get("value") if node.name == "input" else None
        return _clean(value or node.get_text(" ", strip=True))

    data["owner_name"] = by_id("first_second_owner")
    data["site_address"] = _normalize_inline_address(by_id("site_address"))
    data["mailing_address"] = _normalize_inline_address(by_id("mailling_add"))
    data["legal_description"] = by_id("legal_full_desc") or by_id("legal_desc") or by_id("lLegal")

    property_use = by_id("property_use")
    if property_use:
        m = re.search(r"\b(\d{4})\b", property_use)
        if m:
            data["property_use_code"] = m.group(1)

    data["year_built"] = _parse_first_int(by_id("Yrb"))
    data["heated_sq_ft"] = _parse_float(by_id("tls"))
    data["gross_sq_ft"] = _parse_float(by_id("tgs"))

    last_year = _first_table_row(soup, "tblLastYearValue")
    if last_year:
        data["market_value"] = _parse_amount(last_year.get("Just/Market Value"))
        data["county_assessed_value"] = _parse_amount(last_year.get("Assessed Value/SOH Cap"))
        data["county_taxable_value"] = _parse_amount(last_year.get("County Taxable Value"))
        data["school_taxable_value"] = _parse_amount(last_year.get("School Taxable Value"))

    exemptions = _table_rows(soup, "tblExemptions")
    if exemptions:
        current_exemption = _highest_year_row(exemptions)
        hs = (current_exemption or {}).get("Homestead")
        if hs is not None:
            data["homestead_exempt"] = str(hs).strip().upper().startswith(("Y", "YES"))

    history = _table_rows(soup, "tblValueHistory")
    if history:
        latest_history = _highest_year_row(history)
        if latest_history:
            data.setdefault("prior_year_market_value", _parse_amount(latest_history.get("Just/Market Value")))
            if "homestead_exempt" not in data:
                hs = latest_history.get("Homestead Exemption")
                if hs is not None:
                    data["homestead_exempt"] = str(hs).strip().upper().startswith(("Y", "YES"))

    sale = _parse_pcpao_latest_sale(soup)
    if sale:
        data.update(sale)

    details = _extract_pcpao_building_details(soup, text or "")
    if details:
        data["building_details"] = details
        if "quality" in details:
            data["building_condition"] = details["quality"]
        if "heated_sq_ft" not in data and details.get("sub_area_totals", {}).get("heated_sq_ft"):
            data["heated_sq_ft"] = details["sub_area_totals"]["heated_sq_ft"]
        if "gross_sq_ft" not in data and details.get("sub_area_totals", {}).get("gross_sq_ft"):
            data["gross_sq_ft"] = details["sub_area_totals"]["gross_sq_ft"]

    lot_size = _parse_pcpao_lot_size(soup, text or "")
    if lot_size is not None:
        data["lot_size"] = lot_size

    return {k: v for k, v in data.items() if v is not None}


def _normalize_inline_address(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip()


def _parse_first_int(s) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"\d+", str(s))
    return int(m.group(0)) if m else None


def _table_rows(soup, table_id: str) -> list[dict]:
    table = soup.find("table", id=table_id)
    if not table:
        return []
    headers = [re.sub(r"\s+", " ", th.get_text(" ", strip=True)).strip() for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr"):
        cells = [re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip() for td in tr.find_all("td")]
        if not cells:
            continue
        row = {}
        for idx, cell in enumerate(cells):
            key = headers[idx] if idx < len(headers) and headers[idx] else f"col_{idx}"
            row[key] = cell
        rows.append(row)
    return rows


def _first_table_row(soup, table_id: str) -> Optional[dict]:
    rows = _table_rows(soup, table_id)
    return rows[0] if rows else None


def _highest_year_row(rows: list[dict]) -> Optional[dict]:
    best = None
    best_year = -1
    for row in rows:
        year = _parse_int(row.get("Year"))
        if year is not None and year > best_year:
            best = row
            best_year = year
    return best


def _parse_pcpao_latest_sale(soup) -> dict:
    data = {}
    for row in _table_rows(soup, "tblSalesHistory"):
        sale_date = _parse_date(row.get("Sale Date"))
        if not sale_date:
            continue
        if data.get("last_sale_date") and sale_date <= data["last_sale_date"]:
            continue
        data = {
            "last_sale_date": sale_date,
            "last_sale_price": _parse_amount(row.get("Price")),
            "last_sale_qualified": _parse_qualified(row.get("Qualified / Unqualified")),
            "last_sale_vacant_improved": _clean(row.get("Vacant / Improved")),
        }
    return {k: v for k, v in data.items() if v is not None}


def _extract_pcpao_building_details(soup, text: str) -> Optional[dict]:
    details = {}

    structural = soup.find(id=re.compile(r"^structural_\d+$"))
    if structural:
        structural_table = None
        for table in structural.find_all("table"):
            headers = " ".join(th.get_text(" ", strip=True).lower() for th in table.find_all("th"))
            if "structural elements" in headers:
                structural_table = table
                break
        for tr in (structural_table.find_all("tr") if structural_table else []):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue
            key = re.sub(r"[^a-z0-9]+", "_", cells[0].strip(": ").lower()).strip("_")
            val = _clean(cells[1])
            if key and val:
                details[key] = val

    sub_area_rows = []
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if "sub area" not in " ".join(headers):
            continue
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cells) >= 3:
                row = {
                    "sub_area": cells[0].strip(),
                    "heated_sq_ft": _parse_float(cells[1]),
                    "gross_sq_ft": _parse_float(cells[2]),
                }
                if row["sub_area"].lower().startswith("total area"):
                    details["sub_area_totals"] = {
                        "heated_sq_ft": row["heated_sq_ft"],
                        "gross_sq_ft": row["gross_sq_ft"],
                    }
                else:
                    sub_area_rows.append(row)
    if sub_area_rows:
        details["sub_areas"] = sub_area_rows

    extra = _table_rows(soup, "tblExtraFeatures")
    if extra:
        details["extra_features"] = extra

    permits = _table_rows(soup, "tblPermit")
    if permits:
        details["permits"] = permits

    land_rows = _table_rows(soup, "tblLandInformation")
    if land_rows:
        details["land_information"] = land_rows

    parcel_info = _table_rows(soup, "tblParcelInformation")
    if parcel_info:
        details["parcel_information"] = parcel_info

    return details if details else None


def _parse_pcpao_lot_size(soup, text: str) -> Optional[float]:
    land_text = ""
    node = soup.find(id="sw")
    if node:
        land_text = node.get_text(" ", strip=True)
    if not land_text:
        land_text = text

    m = re.search(r"([\d,.]+)\s*acres?", land_text, re.IGNORECASE)
    if m:
        return _parse_float(m.group(1))

    m = re.search(r"([\d,.]+)\s*sf\b", land_text, re.IGNORECASE)
    if m:
        sf = _parse_float(m.group(1))
        return round(sf / 43560, 4) if sf is not None else None

    return None


def _tab_field(text: str, label: str) -> Optional[str]:
    """Extract value from a tab-separated 'Label:\tValue' or 'Label\tValue' line."""
    m = re.search(r"^" + re.escape(label) + r":?\s+(.+?)(?:\t|\n|$)", text, re.MULTILINE)
    return _clean(m.group(1)) if m else None


def _code_desc(text: str, label: str) -> Optional[str]:
    """Extract description from construction table rows: 'Label\tCode\tDescription'."""
    m = re.search(r"^" + re.escape(label) + r"\s+\S+\s+(.+?)(?:\t|\n|$)", text, re.MULTILINE)
    return _clean(m.group(1)) if m else None


def _parse_most_recent_qualified_sale(text: str) -> dict:
    """
    Parse the sales history table. Returns the most recent Qualified sale.

    Table columns (tab-separated):
      Book/Page  |  Instrument  |  Month  |  Year  |  Type  |  Qual/Unqual  |  Vac/Imp  |  Price
    """
    data = {}
    # Each row: optional "book / page\tnumber\tMM\tYYYY\tTYPE\tQualified\tImproved\t$PRICE"
    row_re = re.compile(
        r"(\d+\s*/\s*\d+|/)\s+"        # book/page (or just "/" if empty)
        r"(\d*)\s+"                      # instrument number
        r"(\d{1,2})\s+"                  # month
        r"(20\d{2})\s+"                  # year
        r"(\w+)\s+"                      # deed type
        r"(Qualified|Unqualified)\s+"    # qualification
        r"(Improved|Vacant)\s+"          # vacancy status
        r"\$([\d,]+)",                   # sale price
        re.IGNORECASE,
    )
    for m in row_re.finditer(text):
        qual = m.group(6).lower() == "qualified"
        if not qual:
            continue
        try:
            dt = date(int(m.group(4)), int(m.group(3)), 1)
        except ValueError:
            continue
        if "last_sale_date" not in data or dt > data["last_sale_date"]:
            data["last_sale_date"]         = dt
            data["last_sale_price"]        = _parse_amount(m.group(8))
            data["last_sale_type"]         = m.group(5)
            data["last_sale_qualified"]    = True
            data["last_sale_vacant_improved"] = m.group(7).title()
    return data


def _parse_int(s) -> Optional[int]:
    if not s:
        return None
    try:
        return int(re.sub(r"\D", "", str(s)))
    except (ValueError, TypeError):
        return None


def _parse_float(s) -> Optional[float]:
    if not s:
        return None
    s = re.sub(r"[,$\s]", "", str(s))
    try:
        return float(s)
    except ValueError:
        return None


def _parse_qualified(s) -> Optional[bool]:
    if not s:
        return None
    s = str(s).upper()
    if "Q" in s or "QUAL" in s or "YES" in s or "ARM" in s:
        return True
    if "U" in s or "UNQUAL" in s or "NO" in s:
        return False
    return None


def _extract_building_details_bs4(soup, text: str) -> Optional[dict]:
    """
    Extract extra features (roof, walls, sub-areas) from HCPA tables.
    Returns a dict or None if nothing useful was found.
    """
    details = {}

    # Construction table rows have format: "Label\tCode\tDescription"
    # Use _code_desc to skip the code and return just the description.
    for src_label, dest_key in [
        ("Roof Cover",     "roof_cover"),
        ("Roof Type",      "roof_cover"),
        ("Exterior Wall",  "exterior_wall"),
        ("Wall Type",      "exterior_wall"),
        ("Foundation",     "foundation"),
        ("Roof Structure", "roof_structure"),
        ("Interior Walls", "interior_walls"),
        ("Heat/Ac",        "heat_ac"),
    ]:
        val = _code_desc(text, src_label)
        if val and dest_key not in details:
            details[dest_key] = val

    m = re.search(r"^Stories\s+([\d.]+)", text, re.MULTILINE)
    if m:
        details["stories"] = m.group(1)

    m = re.search(r"^Units\s+(\d+)", text, re.MULTILINE)
    if m:
        details["units"] = int(m.group(1))

    # Sub-area table (garage, pool, etc.)
    sub_area_rows = []
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if any("sub" in h or "area" in h or "description" in h for h in headers):
            for tr in tbl.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(cells) >= 2:
                    sub_area_rows.append(cells[:3])
    if sub_area_rows:
        details["sub_areas"] = sub_area_rows

    return details if details else None


# ---------------------------------------------------------------------------
# 2. TRIM PDF (form field annotations)
# ---------------------------------------------------------------------------

def parse_trim_pdf(pdf_path: str) -> dict:
    """
    Extract TRIM notice values from PDF form field annotations.

    pdfplumber stores AcroForm field data inside annot["data"] as bytes:
      annot["data"]["T"] = field name (bytes)
      annot["data"]["V"] = field value (bytes)
    The top-level annot dict does NOT have "T"/"V" keys.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not installed — cannot parse TRIM PDF")
        return {}

    if not pdf_path:
        return {}

    raw: dict = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                for annot in (page.annots or []):
                    inner = annot.get("data") or {}
                    T = inner.get("T", b"")
                    V = inner.get("V", b"")
                    if isinstance(T, bytes):
                        T = T.decode("utf-8", errors="replace")
                    if isinstance(V, bytes):
                        V = V.decode("utf-8", errors="replace")
                    T = T.strip()
                    V = V.strip()
                    if T and V:
                        raw[T] = V
    except Exception as e:
        logger.warning("Could not parse TRIM PDF %s: %s", pdf_path, e)
        return {}

    return _map_trim_fields(raw)


def _map_trim_fields(raw: dict) -> dict:
    """
    Map raw HCPA TRIM field names (verified against live PDF) to canonical keys.

    Naming convention in the PDF:
      c_ = current certified values (= prior year for CDS purposes)
      p_ = proposed values for the coming tax year
      _jst_val  = just (market) value
      _asd_val  = assessed value (after SOH cap)
      _tax_val  = taxable value (after exemptions)
      _assmt_red_1 = SOH assessment reduction amount
    """
    data: dict = {}

    def amt(key):
        return _parse_amount(raw.get(key))

    # c_county_jst_val = prior year certified market value (TRIM c_ = last certified)
    # current market_value comes from the HCPA page, not TRIM

    # County assessed (after SOH cap)
    cav = amt("c_county_asd_val")
    if cav:
        data["county_assessed_value"] = cav

    # School assessed (no SOH cap applies)
    sav = amt("c_school_asd_val")
    if sav:
        data["school_assessed_value"] = sav

    # Taxable values (after exemptions)
    ctv = amt("c_county_tax_val")
    if ctv:
        data["county_taxable_value"] = ctv

    stv = amt("c_school_tax_val")
    if stv:
        data["school_taxable_value"] = stv

    # SOH reduction (market - county assessed)
    soh = amt("c_county_assmt_red_1")
    if soh:
        data["soh_assessment_reduction"] = soh

    # Exemption code (e.g. "HX HB" for homestead + additional)
    ex = _clean(raw.get("ex"))
    if ex:
        data["exemption_code"] = ex

    # Prior year market = certified values from last TRIM = c_ values above
    # "proposed_next_assessed" = proposed assessed for coming year
    pav = amt("p_county_asd_val")
    if pav:
        data["proposed_next_assessed"] = pav

    # Prior year market value: the c_ just value is the currently-certified
    # (prior-year) market value in TRIM terminology
    prior_mv = amt("c_county_jst_val")
    if prior_mv:
        data["prior_year_market_value"] = prior_mv

    # Annual tax (proposed total for this year)
    annual = amt("p_tax_tot")
    if annual:
        data["annual_tax_amount"] = annual

    return data


# ---------------------------------------------------------------------------
# 3. Tax Collector page
# ---------------------------------------------------------------------------

def parse_tax_collector(text: str) -> dict:
    """
    Extract tax payment status and history from county tax collector page text.

    Returns:
        dict with canonical keys: tax_status, tax_last_paid_amount,
        tax_last_paid_date, tax_payment_history (list of dicts)
    """
    data: dict = {}
    t = text or ""

    # Current status
    if re.search(r"paid in full|no amount due", t, re.IGNORECASE):
        data["tax_status"] = "paid_in_full"
    elif re.search(r"delinquent|past due|overdue", t, re.IGNORECASE):
        data["tax_status"] = "delinquent"
    elif re.search(r"partial|balance due", t, re.IGNORECASE):
        data["tax_status"] = "partial"

    # Last payment
    amt = _after_label(t, "Amount Paid") or _after_label(t, "Payment Amount")
    if amt:
        data["tax_last_paid_amount"] = _parse_amount(amt)

    pd_str = _after_label(t, "Payment Date") or _after_label(t, "Date Paid")
    if pd_str:
        data["tax_last_paid_date"] = _parse_date(pd_str)

    # Payment history rows — parse table-like structure in visible text
    history = _parse_tax_history_table(t)
    if history:
        data["tax_payment_history"] = history

    return data


def _parse_tax_history_table(text: str) -> list:
    """
    Extract annual tax payment rows from visible text.

    Looks for lines that match patterns like:
      2024  Annual  $3,421.00  11/27/2024  Receipt# 12345678
    Returns a list of dicts matching TaxPaymentHistory columns.
    """
    rows = []
    # Pattern: year (4-digit) followed by bill type and dollar amount
    line_re = re.compile(
        r"(?P<year>20\d{2})"
        r".{0,40}"
        r"(?P<bill_type>Annual|Homestead Penalty|Tangible Personal Property)"
        r".{0,60}"
        r"\$?(?P<amount>[\d,]+\.\d{2})"
        r".{0,80}"
        r"(?P<date>\d{1,2}/\d{1,2}/\d{4})",
        re.IGNORECASE,
    )
    for m in line_re.finditer(text):
        row = {
            "tax_year":   int(m.group("year")),
            "bill_type":  m.group("bill_type").title(),
            "amount_paid": _parse_amount(m.group("amount")),
            "payment_date": _parse_date(m.group("date")),
        }
        receipt_m = re.search(r"Receipt\s*#?\s*(\w+)", text[m.start():m.start() + 200], re.IGNORECASE)
        if receipt_m:
            row["receipt_number"] = receipt_m.group(1)

        # days_late: relative to Nov 1 early-pay window
        if row["payment_date"]:
            nov1 = date(row["tax_year"], 11, 1)
            row["days_late"] = (row["payment_date"] - nov1).days

        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# 4. Canonical DataFrame assembly
# ---------------------------------------------------------------------------

def to_canonical_dataframe(
    hcpa: dict,
    trim: dict,
    tax: dict,
    parcel_id: str,
) -> pd.DataFrame:
    """
    Merge parsed dicts from all three sources into a single canonical row.

    HCPA is authoritative for property/building fields.
    TRIM supplements/overrides valuation fields (more precise) and adds SOH/exemption.
    Tax collector adds payment status and history.
    """
    row: dict = {"parcel_id": parcel_id}

    # Merge: HCPA first, then TRIM overwrites valuations (more precise), tax adds status
    row.update(hcpa)
    row.update(trim)       # TRIM values win for overlapping valuation keys
    row.update({k: v for k, v in tax.items() if k != "tax_payment_history"})

    # Promote list keys that don't belong in a flat row
    tax_history = tax.get("tax_payment_history", [])

    df = pd.DataFrame([row])
    # Attach payment history as a Python list in a special metadata column
    # that PropertyAppraiserLoader reads and inserts separately.
    df["_tax_payment_history"] = [tax_history]

    return df
