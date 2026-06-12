"""
Florida DOR land-use code translation.

Every Florida county appraiser classifies every parcel with a standardized
Department of Revenue use code (Florida Administrative Code 12D-8.008) — the
first two digits carry the major class, the last two are county sub-codes.
Counties differ in how they publish it: Pinellas ships "0110 Single Family
Home" (code + label fused), Hillsborough ships only the bare code in DOR_C.

translate_dor_code() turns a bare code into the same "code + label" convention
Pinellas already uses, so properties.property_type is format-consistent across
counties. Because the coding is a statewide standard, this single table covers
every Florida county; only an out-of-state expansion would need a second one.
"""

from typing import Optional

# Major-class labels keyed by the first two digits of the DOR use code.
_DOR_MAJOR_LABELS = {
    "00": "Vacant Residential",
    "01": "Single Family",
    "02": "Mobile Home",
    "03": "Multi-Family 10+ Units",
    "04": "Condominium",
    "05": "Cooperative",
    "06": "Retirement Home",
    "07": "Miscellaneous Residential",
    "08": "Multi-Family Under 10 Units",
    "09": "Residential Common Elements",
    "10": "Vacant Commercial",
    "11": "Store One Story",
    "12": "Mixed Use Store/Office/Residential",
    "13": "Department Store",
    "14": "Supermarket",
    "15": "Regional Shopping Center",
    "16": "Community Shopping Center",
    "17": "Office Building One Story",
    "18": "Office Building Multi-Story",
    "19": "Professional Services Building",
    "20": "Airport/Bus/Marine Terminal",
    "21": "Restaurant/Cafeteria",
    "22": "Drive-In Restaurant",
    "23": "Financial Institution",
    "24": "Insurance Company Office",
    "25": "Repair Service Shop",
    "26": "Service Station",
    "27": "Auto Sales/Repair",
    "28": "Parking Lot/Mobile Home Park",
    "29": "Wholesale Outlet",
    "30": "Florist/Greenhouse",
    "31": "Drive-In Theater/Open Stadium",
    "32": "Enclosed Theater/Auditorium",
    "33": "Nightclub/Bar",
    "34": "Bowling/Skating/Pool Hall",
    "35": "Tourist Attraction",
    "36": "Camp",
    "37": "Race Track",
    "38": "Golf Course",
    "39": "Hotel/Motel",
    "40": "Vacant Industrial",
    "41": "Light Manufacturing",
    "42": "Heavy Industrial",
    "43": "Lumber Yard/Sawmill",
    "44": "Packing Plant",
    "45": "Cannery/Bottler",
    "46": "Food Processing",
    "47": "Mineral Processing",
    "48": "Warehouse/Distribution",
    "49": "Open Storage",
    "50": "Improved Agricultural",
    "51": "Cropland Class I",
    "52": "Cropland Class II/III",
    "53": "Cropland Class IV+",
    "54": "Timberland",
    "55": "Timberland Site Index",
    "56": "Timberland Other",
    "57": "Timberland Not Classified",
    "58": "Timberland Other",
    "59": "Timberland Other",
    "60": "Grazing Land Class I",
    "61": "Grazing Land Class II",
    "62": "Grazing Land Class III",
    "63": "Grazing Land Class IV",
    "64": "Grazing Land Class V",
    "65": "Grazing Land Class VI",
    "66": "Orchard/Grove/Citrus",
    "67": "Poultry/Bees/Fish",
    "68": "Dairy/Feed Lot",
    "69": "Ornamentals/Miscellaneous Agricultural",
    "70": "Vacant Institutional",
    "71": "Church",
    "72": "Private School/College",
    "73": "Private Hospital",
    "74": "Home for the Aged",
    "75": "Orphanage/Non-Profit Service",
    "76": "Mortuary/Cemetery",
    "77": "Club/Lodge/Union Hall",
    "78": "Sanitarium/Convalescent/Rest Home",
    "79": "Cultural Organization",
    "80": "Vacant Governmental",
    "81": "Military",
    "82": "Forest/Park/Recreational",
    "83": "Public County School",
    "84": "Public College",
    "85": "Public Hospital",
    "86": "County",
    "87": "State",
    "88": "Federal",
    "89": "Municipal",
    "90": "Government Leasehold Interest",
    "91": "Utility",
    "92": "Mining/Petroleum/Gas",
    "93": "Subsurface Rights",
    "94": "Right-of-Way",
    "95": "Rivers/Lakes/Submerged Land",
    "96": "Sewage/Solid Waste/Borrow Pit",
    "97": "Outdoor Recreational/Parkland",
    "98": "Centrally Assessed",
    "99": "Non-Agricultural Acreage",
}


def translate_dor_code(raw: Optional[str]) -> Optional[str]:
    """Bare DOR use code -> "code + label" (Pinellas-style), e.g.
    '0100' -> '0100 Single Family'. Unknown or malformed codes fall back to
    the bare code so the classification is never silently dropped."""
    if raw is None:
        return None
    code = str(raw).strip()
    if not code or not code.isdigit():
        return None
    code = code.zfill(4)
    label = _DOR_MAJOR_LABELS.get(code[:2])
    return f"{code} {label}" if label else code
