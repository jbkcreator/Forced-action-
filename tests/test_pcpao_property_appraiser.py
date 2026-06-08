from src.scrappers.property_appraiser.pa_parser import parse_pcpao_page
from src.scrappers.property_appraiser.pa_scraper import hyphenate_pinellas_parcel


PCPAO_HTML = """
<div id="property_summary">
  <h2 id="pacel_no">16-29-15-32292-019-0010</h2>
  <span id="first_second_owner">USA POSTAL SERVICE</span>
  <label id="property_use">8810 Federal Gov't - Non-residential (commercial) only</label>
  <label id="site_address">636 CLEVELAND ST<br/>CLEARWATER, FL 33755</label>
  <div id="mailling_add">4000 DEKALB TECHNOLOGY PKY 550<br/>ATLANTA, GA 30340-2779</div>
  <input id="legal_full_desc" value="GOULD &amp; EWING'S 2ND ADD BLK 19, LOTS 1,2 AND 3">
  <label id="Yrb">1933</label>
  <h2 id="tls">25,116</h2>
  <h2 id="tgs">25,116</h2>
</div>
<table id="tblExemptions">
  <thead><tr><th>Year</th><th>Homestead</th><th>Use %</th><th>Status</th></tr></thead>
  <tbody><tr><td>2025</td><td>No</td><td>0%</td><td></td></tr></tbody>
</table>
<table id="tblLastYearValue">
  <thead><tr><th>Year</th><th>Just/Market Value</th><th>Assessed Value/SOH Cap</th><th>County Taxable Value</th><th>School Taxable Value</th><th>Municipal Taxable Value</th></tr></thead>
  <tbody><tr><td>2025</td><td>$4,514,807</td><td>$4,426,894</td><td>$0</td><td>$0</td><td>$0</td></tr></tbody>
</table>
<table id="tblValueHistory">
  <thead><tr><th>Year</th><th>Homestead Exemption</th><th>Just/Market Value</th><th>Assessed Value/SOH Cap</th><th>County Taxable Value</th><th>School Taxable Value</th><th>Municipal Taxable Value</th></tr></thead>
  <tbody><tr><td>2024</td><td>N</td><td>$4,024,449</td><td>$4,024,449</td><td>$0</td><td>$0</td><td>$0</td></tr></tbody>
</table>
<table id="tblSalesHistory">
  <thead><tr><th>Sale Date</th><th>Price</th><th>Qualified / Unqualified</th><th>Vacant / Improved</th><th>Grantor</th><th>Grantee</th><th>Book / Page</th></tr></thead>
  <tbody><tr><td>01/05/2023</td><td>$500,000</td><td>Qualified</td><td>Improved</td><td>A</td><td>B</td><td>1 / 2</td></tr></tbody>
</table>
<span id="sw">Land Area: 37,231 sf | 0.85 acres</span>
<div id="structural_1">
  <table>
    <tr><th>Structural Elements</th><th></th></tr>
    <tr><td>Foundation: </td><td>Spread/Mono Footing</td></tr>
    <tr><td>Building Type: </td><td>Offices</td></tr>
    <tr><td>Quality: </td><td>Above Average</td></tr>
  </table>
  <table>
    <tr><th>Sub Area</th><th>Heated Area SF</th><th>Gross Area SF</th></tr>
    <tr><td>Base (BAS): </td><td>12,558</td><td>12,558</td></tr>
    <tr><td><b>Total Area SF</b>: </td><td><b>25,116</b></td><td><b>25,116</b></td></tr>
  </table>
</div>
<table id="tblExtraFeatures">
  <thead><tr><th>Description</th><th>Value/Unit</th><th>Units</th><th>Total Value as New</th><th>Depreciated Value</th><th>Year</th></tr></thead>
  <tbody><tr><td>LOAD DOCK</td><td>$26.00</td><td>408.0</td><td>$10,608</td><td>$10,608</td><td>1933</td></tr></tbody>
</table>
<table id="tblPermit">
  <thead><tr><th>Permit Number</th><th>Description</th><th>Issue Date</th><th>Estimated Value</th></tr></thead>
  <tbody><tr><td>BCP2023-060841</td><td>WINDOWS/DOORS</td><td>07/14/2023</td><td>$2,500</td></tr></tbody>
</table>
"""


def test_hyphenate_pinellas_compact_parcel():
    assert hyphenate_pinellas_parcel("162915322920190010") == "16-29-15-32292-019-0010"
    assert hyphenate_pinellas_parcel("16-29-15-32292-019-0010") == "16-29-15-32292-019-0010"


def test_parse_pcpao_page_extracts_canonical_fields():
    parsed = parse_pcpao_page("", PCPAO_HTML)

    assert parsed["owner_name"] == "USA POSTAL SERVICE"
    assert parsed["site_address"] == "636 CLEVELAND ST CLEARWATER, FL 33755"
    assert parsed["mailing_address"] == "4000 DEKALB TECHNOLOGY PKY 550 ATLANTA, GA 30340-2779"
    assert parsed["property_use_code"] == "8810"
    assert parsed["legal_description"].startswith("GOULD & EWING'S 2ND ADD")
    assert parsed["year_built"] == 1933
    assert parsed["heated_sq_ft"] == 25116.0
    assert parsed["gross_sq_ft"] == 25116.0
    assert parsed["lot_size"] == 0.85
    assert parsed["market_value"] == 4514807.0
    assert parsed["county_assessed_value"] == 4426894.0
    assert parsed["county_taxable_value"] == 0.0
    assert parsed["prior_year_market_value"] == 4024449.0
    assert parsed["homestead_exempt"] is False
    assert parsed["last_sale_price"] == 500000.0
    assert parsed["last_sale_qualified"] is True
    assert parsed["building_condition"] == "Above Average"
    assert parsed["building_class"] == "Offices"
    assert parsed["building_details"]["foundation"] == "Spread/Mono Footing"
    assert parsed["building_details"]["extra_features"][0]["Description"] == "LOAD DOCK"
    assert parsed["building_details"]["permits"][0]["Permit Number"] == "BCP2023-060841"
