"""Seed county config into distress_dev (or whichever DB is in DATABASE_URL)."""
import os
import sys
from datetime import datetime, timezone

# Allow running with a different DB without changing .env
# Usage: DATABASE_URL=... python scripts/seed_dev_db.py

from src.core.database import Database
from src.core.models import County, CountySource, CountyColumnMapping

db = Database()

with db.session_scope() as session:
    session.add(County(
        county_id='hillsborough', display_name='Hillsborough County',
        fips='12057', nws_zone='FLZ151', parcel_id_format='folio',
        bankruptcy_division='8',
        city_filer_keywords=['CITY OF TAMPA', 'HILLSBOROUGH COUNTY'],
        code_lien_type_map={'TCL': 'TAMPA', 'CCL': None},
        is_active=True,
    ))
    session.add(County(
        county_id='pinellas', display_name='Pinellas County',
        fips='12103', nws_zone='FLZ050', parcel_id_format='strap',
        bankruptcy_division='8',
        city_filer_keywords=[
            'PINELLAS COUNTY', 'CITY OF ST. PETERSBURG', 'CITY OF CLEARWATER',
            'CITY OF LARGO', 'CITY OF PINELLAS PARK', 'CITY OF DUNEDIN',
            'CITY OF TARPON SPRINGS',
        ],
        code_lien_type_map={},
        is_active=True,
    ))

with db.session_scope() as session:
    sources = [
        # ── Hillsborough ──────────────────────────────────────────────────
        dict(county_id='hillsborough', signal_type='foreclosures',
             source_name='Hillsborough RealForeclose',
             url='https://www.hillsborough.realforeclose.com/index.cfm',
             description='Hillsborough County foreclosure auction portal.',
             navigation_hint='Navigate to search, set date range, extract all auction listings.',
             output_format='table', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='hillsborough', signal_type='liens',
             source_name='Hillsborough Clerk ORI',
             url='https://publicaccess.hillsclerk.com/oripublicaccess/',
             description='Hillsborough County Official Records portal.',
             navigation_hint='Search by recording date range. Download CSV export.',
             output_format='csv', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='hillsborough', signal_type='violations',
             source_name='Hillsborough Accela Enforcement',
             url='https://aca-prod.accela.com/HCFL/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement',
             description='Hillsborough County code enforcement via Accela.',
             navigation_hint='Search by date range in Enforcement module, export CSV.',
             output_format='csv', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='hillsborough', signal_type='permits',
             source_name='Hillsborough Accela Building',
             url='https://aca-prod.accela.com/HCFL/Cap/CapHome.aspx?module=Building',
             description='Hillsborough County building permit portal via Accela.',
             navigation_hint='Navigate to Building module, search by date range, download CSV.',
             output_format='csv', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='hillsborough', signal_type='court_records',
             source_name='Hillsborough Clerk Court',
             url='https://publicrec.hillsclerk.com/Civil/dailyfilings/',
             description='Hillsborough County clerk civil filings.',
             navigation_hint='Search by filed date range, download results.',
             output_format='csv', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='hillsborough', signal_type='tax_delinquency',
             source_name='Hillsborough Tax Delinquency',
             url='https://hillsborough.county-taxes.com',
             description='Hillsborough County tax delinquency data.',
             navigation_hint='Access tax report at /reports/real-estate.',
             output_format='csv', date_range_available=False, frequency='weekly', special_flags={}),
        dict(county_id='hillsborough', signal_type='master_data',
             source_name='HCPA Bulk Data',
             url='https://downloads.hcpafl.org/',
             description='Hillsborough County Property Appraiser bulk parcel data. Nightly refresh.',
             navigation_hint="Find and click 'PARCEL_SPREADSHEET.xls' link on the page (it is a large 536 MB XLS file). Click it to start the download and wait for it to complete.",
             output_format='xls', date_range_available=False, frequency='daily', special_flags={}),
        # ── Pinellas ──────────────────────────────────────────────────────
        dict(county_id='pinellas', signal_type='foreclosures',
             source_name='Pinellas RealForeclose',
             url='https://pinellas.realforeclose.com/index.cfm',
             description='Pinellas County foreclosure auction portal.',
             navigation_hint='Navigate to search, set date range, extract all auction listings.',
             output_format='table', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='pinellas', signal_type='liens',
             source_name='Pinellas Clerk ORI',
             url='https://officialrecords.mypinellasclerk.gov',
             description='Pinellas County official records portal.',
             navigation_hint='Search by recording date range. Download CSV export.',
             output_format='csv', date_range_available=True, frequency='daily',
             special_flags={},
             ori_column_map={
                 'DirectName': 'Grantor',
                 'IndirectName': 'Grantee',
                 'InstrumentNumber': 'Instrument',
                 'Comments': 'Legal',
                 'DocTypeDescription': 'DocType',
             },
             ori_book_page_col='BookPage',
             ori_doc_type_map={
                 'JUDGEMENT LIEN': 'JUDGMENT',
                 'JUDGEMENT': 'JUDGMENT',
                 'LIEN (IRS)': 'TAX LIEN',
                 'CERTIFIED COPY OF A COURT JUDGMENT OR ORDER': 'JUDGMENT',
             }),
        dict(county_id='pinellas', signal_type='violations',
             source_name='Pinellas GovQA (PRR)',
             url='https://pinellas.govqa.us',
             description='PRR-only source. Submit public records request for violation data.',
             navigation_hint=None, output_format='csv', date_range_available=False,
             frequency='manual', special_flags={'prr_only': True}),
        dict(county_id='pinellas', signal_type='permits',
             source_name='Pinellas Accela Building',
             url='https://aca-prod.accela.com/PINELLAS',
             description='Pinellas County Accela building permit portal.',
             navigation_hint='Navigate to Building module, search by date range, download CSV.',
             output_format='csv', date_range_available=True, frequency='daily', special_flags={}),
        dict(county_id='pinellas', signal_type='court_records',
             source_name='Pinellas Clerk Court',
             url='https://courtrecords.mypinellasclerk.gov',
             description='Pinellas County clerk court records.',
             navigation_hint='Search by filed date range, download Excel export.',
             output_format='excel', date_range_available=True, frequency='daily',
             special_flags={'style_col': 'Style/Description'}),
        dict(county_id='pinellas', signal_type='tax_delinquency',
             source_name='Pinellas County Taxes',
             url='https://pinellas.county-taxes.com/public/search/property_tax',
             description='Pinellas County tax delinquency search portal.',
             navigation_hint='Search for delinquent tax accounts, extract all results.',
             output_format='table', date_range_available=False, frequency='weekly', special_flags={}),
        dict(county_id='pinellas', signal_type='master_data',
             source_name='PCPAO Bulk Data',
             url='https://www.pcpao.gov/tools-data/data-downloads/raw-database-files',
             description='Pinellas County Property Appraiser bulk data. 15 CSV tables, nightly refresh.',
             navigation_hint='Direct CSV download links.',
             output_format='csv', date_range_available=False, frequency='daily',
             special_flags={'bulk_tables': ['RP_PROPERTY_INFO']}),
    ]
    for s in sources:
        session.add(CountySource(**s))

# ORI column mapping for Pinellas liens source → used by _load_ori_legal_proceedings
# to bridge ORI instrument format to ProbateLoader/DivorceLoader column format.
# One approved mapping row covers both probate and divorce_filings lookups.
_ORI_COLS = ['Instrument', 'Grantor', 'Grantee', 'RecordDate', 'Legal',
             'DocType', 'document_type', 'Book', 'Page', 'Filing Amt']
_ORI_MAPPING = {
    'Instrument':    'CaseNumber',
    'Grantor':       'LastName/CompanyName',
    'RecordDate':    'FilingDate',
    'Legal':         'PartyAddress',
    # pass-throughs
    'Grantee':       'Grantee',
    'DocType':       'DocType',
    'document_type': 'document_type',
    'Book':          'Book',
    'Page':          'Page',
    'Filing Amt':    'Filing Amt',
}

with db.session_scope() as session:
    pinellas_liens = (
        session.query(CountySource)
        .filter_by(county_id='pinellas', signal_type='liens')
        .first()
    )
    if pinellas_liens:
        session.add(CountyColumnMapping(
            source_id=pinellas_liens.id,
            source_columns=sorted(_ORI_COLS),
            mapping=_ORI_MAPPING,
            is_approved=True,
            mapped_by='human',
            approved_by='seed_dev_db',
            approved_at=datetime.now(timezone.utc),
            sample_rows=[],
        ))
        print("Seeded CountyColumnMapping for Pinellas liens (ORI → legal_proceedings bridge)")
    else:
        print("WARNING: Pinellas liens source not found — skipping CountyColumnMapping seed")

print(f"Seeded 2 counties, {len(sources)} sources into {os.environ.get('DATABASE_URL', '(from .env)')}")
