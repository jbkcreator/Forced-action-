"""Registry of all known macro signal sources and their series metadata.

Each entry describes the source, available series, and normalization hints
(signal_key, unit, frequency, geography). Used by clients to avoid hardcoding
metadata in multiple places.
"""
from __future__ import annotations

SOURCES: dict[str, dict] = {
    "fred": {
        "name": "Federal Reserve Economic Data (FRED)",
        "base_url": "https://api.stlouisfed.org/fred",
        "key_required": True,
        "free": True,
        "key_signup_url": "https://fred.stlouisfed.org/docs/api/api_key.html",
        "update_frequency": "varies by series",
        "series": {
            "MORTGAGE30US": {
                "signal_key": "mortgage_rate_30yr",
                "description": "30-Year Fixed Rate Mortgage Average",
                "unit": "percent",
                "frequency": "weekly",
                "geography_scope": "national",
                "geography_id": "US",
            },
            "MORTGAGE15US": {
                "signal_key": "mortgage_rate_15yr",
                "description": "15-Year Fixed Rate Mortgage Average",
                "unit": "percent",
                "frequency": "weekly",
                "geography_scope": "national",
                "geography_id": "US",
            },
            "DFF": {
                "signal_key": "fed_funds_rate",
                "description": "Federal Funds Effective Rate",
                "unit": "percent",
                "frequency": "daily",
                "geography_scope": "national",
                "geography_id": "US",
            },
            "T10YIE": {
                "signal_key": "inflation_breakeven_10yr",
                "description": "10-Year Breakeven Inflation Rate",
                "unit": "percent",
                "frequency": "daily",
                "geography_scope": "national",
                "geography_id": "US",
            },
        },
    },
    "bls": {
        "name": "Bureau of Labor Statistics",
        "base_url": "https://api.bls.gov/publicAPI/v2",
        "key_required": False,
        "free": True,
        "key_signup_url": "https://data.bls.gov/registrationEngine/",
        "update_frequency": "monthly",
        "notes": "No key: 25 years history, 25 series/query. With key: 50+ years, higher limits.",
        "series": {
            "LNS14000000": {
                "signal_key": "unemployment_rate_national",
                "description": "Unemployment Rate (seasonally adjusted)",
                "unit": "percent",
                "frequency": "monthly",
                "geography_scope": "national",
                "geography_id": "US",
            },
            "CUUR0000SA0": {
                "signal_key": "cpi_all_urban",
                "description": "CPI-U All Items (not seasonally adjusted)",
                "unit": "index_1982_84_100",
                "frequency": "monthly",
                "geography_scope": "national",
                "geography_id": "US",
            },
            "CUSR0000SEHA": {
                "signal_key": "cpi_rent_primary_residence",
                "description": "CPI Rent of Primary Residence (seasonally adjusted)",
                "unit": "index_1982_84_100",
                "frequency": "monthly",
                "geography_scope": "national",
                "geography_id": "US",
            },
        },
    },
    "fhfa": {
        "name": "Federal Housing Finance Agency — House Price Index",
        "base_url": "https://www.fhfa.gov",
        "key_required": False,
        "free": True,
        "update_frequency": "quarterly",
        "access_method": "CSV download",
        "series": {
            "HPI_master": {
                "signal_key": "house_price_index",
                "description": "FHFA HPI — all transaction types, all geographies",
                "unit": "index",
                "frequency": "quarterly",
                "geography_scope": "multi",
                "geography_id": "varies",
                "geography_levels": ["national", "state", "metro", "county", "zip3"],
            },
        },
    },
    "census_acs5": {
        "name": "US Census Bureau — American Community Survey 5-Year",
        "base_url": "https://api.census.gov/data",
        "key_required": True,
        "free": True,
        "key_signup_url": "https://api.census.gov/data/key_signup.html",
        "update_frequency": "annual (5-year estimates)",
        "series": {
            "B19013_001E": {
                "signal_key": "median_household_income",
                "description": "Median Household Income in the Past 12 Months",
                "unit": "dollars",
                "frequency": "annual",
                "geography_scope": "county",
                "geography_id": "varies",
            },
            "B25002_001E": {
                "signal_key": "total_housing_units",
                "description": "Total Housing Units",
                "unit": "count",
                "frequency": "annual",
                "geography_scope": "county",
                "geography_id": "varies",
            },
            "B25002_003E": {
                "signal_key": "vacant_housing_units",
                "description": "Vacant Housing Units",
                "unit": "count",
                "frequency": "annual",
                "geography_scope": "county",
                "geography_id": "varies",
            },
            "B01003_001E": {
                "signal_key": "total_population",
                "description": "Total Population",
                "unit": "count",
                "frequency": "annual",
                "geography_scope": "county",
                "geography_id": "varies",
            },
            "B25077_001E": {
                "signal_key": "median_home_value",
                "description": "Median Value (Dollars) — Owner-Occupied Housing Units",
                "unit": "dollars",
                "frequency": "annual",
                "geography_scope": "county",
                "geography_id": "varies",
            },
        },
    },
}
