# Scraper Status

## Legend
- ✅ Verified & working
- 🔄 In progress / needs testing
- ❌ Not configured
- ➖ Not applicable for this county
- ⬆️ Manual upload only

---

## Hillsborough County

| Scraper | Status | Notes |
|---------|--------|-------|
| master | ✅ Verified | |
| permits | ✅ Verified | Download mode, 54% match rate (22 unmatched = new Apollo Beach subdivision not yet in parcels) |
| foreclosures | ✅ Verified | |
| liens | ✅ Verified | Playwright selector mode, download works, empty-result handled |
| violations | ✅ Verified | Playwright DOM extraction, pagination fixed, address from hidden spans |
| evictions | ✅ Verified | static_download mode, case_type_col config-driven |
| divorce | ✅ Verified | Directory listing + dissolution filter, 1/3 matched (100%) |
| probate | ✅ Verified | static_download mode |
| bankruptcy | ✅ Verified | CourtListener API |
| tax_delinquencies | ⬆️ Manual upload | |
| fire | 🔄 Run needed | browser-use, HCSO GIS portal |
| flood | ✅ Verified | FEMA + NFIP + NWS API |
| storm | ✅ Verified | NOAA/NWS API |
| insurance | ✅ Verified | FEMA IA + Accela permits query |
| roofing_permits | ✅ Verified | SQL classifier on building_permits |

---

## Pinellas County

| Scraper | Status | Notes |
|---------|--------|-------|
| master | ✅ Verified | |
| permits | ✅ Verified | Span-ID extraction, pagination fixed |
| foreclosures | ✅ Verified | |
| liens | ✅ Verified | Playwright selector mode with CF Edge profile, covers all ORI doc types |
| violations | 🔄 Deferred | Needs playwright code — portal TBD |
| evictions | ✅ Verified | playwright_only, case_type_col=Case Type, style_col=Style/Description |
| divorce | ✅ Verified | Covered by Pinellas liens ORI scraper (routed via ColumnMapper) |
| probate | ✅ Verified | Covered by Pinellas liens ORI scraper (routed via ColumnMapper) |
| bankruptcy | ✅ Verified | CourtListener API (same Tampa Division as Hillsborough) |
| tax_delinquencies | ⬆️ Manual upload | |
| fire | ❌ Not configured | No confirmed bulk source for Pinellas |
| flood | ✅ Verified | FEMA + NFIP + NWS API |
| storm | ✅ Verified | NOAA/NWS API |
| insurance | ✅ Verified | FEMA IA API |
| roofing_permits | ✅ Verified | SQL classifier on building_permits |


---

## Remaining work

- **Hillsborough fire** — browser-use scraper against HCSO GIS portal (calls-for-service CSV)
- **Pinellas violations** — deferred, needs portal research + playwright code
- **Pinellas fire** — no confirmed source yet
- **Tax delinquencies** — manual upload both counties (by design)

---

## Run commands

  Hillsborough:
  python -m src.scrappers.foreclosures.foreclosure_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.liens.lien_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.violation.violation_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.evictions.evictions_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.divorce.divorce_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.probate.probate_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.bankruptcy.bankruptcy_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.deliquencies.tax_delinquent_engine --county-id hillsborough --load-to-db
  python -m src.scrappers.fire.fire_engine --county-id hillsborough
  python -m src.scrappers.flood.flood_engine --county-id hillsborough
  python -m src.scrappers.storm.storm_engine --county-id hillsborough
  python -m src.scrappers.insurance.insurance_engine --county-id hillsborough
  python -m src.scrappers.roofing_permits.roofing_permit_engine --county-id hillsborough

  Pinellas:
  python -m src.scrappers.foreclosures.foreclosure_engine --county-id pinellas --load-to-db
  python -m src.scrappers.liens.lien_engine --county-id pinellas --load-to-db
  python -m src.scrappers.violation.violation_engine --county-id pinellas --load-to-db
  python -m src.scrappers.evictions.evictions_engine --county-id pinellas --load-to-db --headful
  python -m src.scrappers.divorce.divorce_engine --county-id pinellas --load-to-db --headful
  python -m src.scrappers.probate.probate_engine --county-id pinellas --load-to-db
  python -m src.scrappers.bankruptcy.bankruptcy_engine --county-id pinellas --load-to-db
  python -m src.scrappers.deliquencies.tax_delinquent_engine --county-id pinellas --load-to-db
  python -m src.scrappers.flood.flood_engine --county-id pinellas
  python -m src.scrappers.storm.storm_engine --county-id pinellas
  python -m src.scrappers.insurance.insurance_engine --county-id pinellas
  python -m src.scrappers.roofing_permits.roofing_permit_engine --county-id pinellas
