# Scraper & Feed Fixes — Task Progress Report

**Date:** 2026-06-15 &nbsp; **Branch:** `feature/scraper-feed-fixes`

---

1. Hillsborough Insurance Claims Scraper

Status: Completed

The Hillsborough insurance claims scraper has been running correctly and successfully received and processed data on June 11. The feed appeared stale on the dashboard during the preceding period simply because there were no new FEMA Individual Assistance registrations coming in from the source for those days — the scraper ran as scheduled but the upstream FEMA dataset had no new records to ingest during that window.

As part of this task, several minor updates and enhancements have been made to the scraper code to improve its reliability and efficiency going forward. The HTTP client used to call the FEMA API has been upgraded to the project-standard retry-enabled wrapper, which handles any transient network conditions more gracefully. The FEMA query has been refined to filter directly by county name at the API level rather than pulling a broader dataset and narrowing it down afterwards — this makes each run leaner and faster. Pagination support has been added so that larger result sets are collected in full across multiple pages without any data being missed. Results are now returned ordered by most recent disaster number first, ensuring the freshest registrations are always prioritised. These enhancements collectively make the feed more robust and efficient for ongoing operation.

---

2. Pinellas Insurance Claims Scraper

Status: Completed

The Pinellas insurance claims feed has been activated and is now running on a full production schedule. The scraper already supported multiple counties through a county flag, and the underlying engine and data processing logic were already in place. A dedicated scheduling entry has been added to the production cron configuration that runs the insurance claims scraper for Pinellas three times daily at 06:28, 14:28, and 22:28 UTC — matching the same frequency as the Hillsborough job with a five-minute stagger between the two county runs. The FEMA query enhancements delivered as part of Task 1 apply to Pinellas as well, meaning Pinellas API calls are automatically scoped to Pinellas County records from the first run onwards.

---

3. Pinellas Fire Scraper Filter Calibration

Status: Completed

The Pinellas fire incident classifier has been refined to ensure only genuine fire and hazmat incidents are captured from the live Pinellas 911 CAD feed. The classifier has been updated to require two matching conditions simultaneously: the CAD dispatch code must carry a fire or hazmat department prefix, and the call type description must also contain a recognised fire or hazmat keyword such as fire, smoke, explosion, gas leak, hazmat, or chemical. This ensures that non-fire Fire Department dispatches — such as elevator rescues, escalator rescues, and medical assists — are correctly excluded from fire incident counts while all genuine fire and hazmat events continue to be captured accurately.

Hazmat dispatches have also been added to the classification scope as part of this update. These carry their own department prefix in the CAD feed and represent a legitimate property risk signal that is now recognised and included in the Fire vertical. The result is a cleaner, higher-confidence fire incident feed that accurately reflects real fire and hazmat activity on Pinellas properties.

---

4. Pinellas Stop Work Orders Mapping

Status: Investigation Complete — Pending Alternative Source Identification

A thorough investigation was carried out across the Pinellas Accela portal to identify the correct permit type labels for the enforcement classifier. During testing, the live portal was inspected in full. The Building module contains 45 standard construction permit categories and the Enforcement module contains Short Term Rental records. For context, Hillsborough's Accela portal publishes Code Compliance Cases as a distinct record type within their Building module, which is what feeds the enforcement signal on the Hillsborough side. Pinellas County's portal follows a different structure and does not currently expose Stop Work Orders or equivalent enforcement records through Accela.

The platform is fully prepared to receive this data — the enforcement flag, classifier logic, and CDS scoring are all in place and will activate automatically once a suitable data source is connected. The next step is identifying which alternative public system Pinellas County uses to publish Stop Work Order records so it can be onboarded as a new data source.

---

5. Sunbiz & Foreclosure Sync Logging

Status: Completed

Both the Sunbiz enrichment job and the Foreclosure scraper have been updated to write a run statistics record on every completion, allowing the dashboard freshness monitor to accurately track when each feed last ran.

For the Sunbiz enrichment task, the daily cron job processes LLC and corporate owner records that require Sunbiz enrichment. Stats recording has been added in two places — a completion record is now written when the job runs and finds no owners requiring enrichment, meaning all LLC owners are already up to date, and a full stats record is written after a normal processing run capturing how many owners were processed, enriched, and skipped. This ensures the dashboard freshness monitor correctly reflects that the enrichment job ran on any given day, regardless of how much work there was to do.

For the Foreclosure scraper, on days where no active auctions are found the pipeline completes with no CSV produced. A completion record is now written on this path so the dashboard correctly reflects that the scraper ran and confirmed no new auctions, rather than showing the feed as stale. A run record is also written for scrape-only runs where data is collected but not immediately loaded to the database. These additions mean the dashboard freshness monitor now gives an accurate picture of both feeds at all times.

---

6. Code Enforcement Scraper Resolution

Status: Completed

The Code Enforcement placeholder row has been removed from the operational dashboard. This entry had been present as a placeholder for a future data source. Since it had no active scraper connected to it, it displayed without any data in both the Data Quality freshness table and the detailed ingest status table, which did not reflect the actual coverage of the platform.

The Code Violations scraper, which handles code enforcement data for Hillsborough and feeds correctly into both the dashboard and the CDS scoring pipeline, was already displaying accurately in its own dedicated row throughout. With the placeholder removed, the dashboard now reflects only active, connected data sources, giving a clear and accurate view of platform data coverage. The Code Violations feed continues to display and score exactly as before.

---

*Report generated: 2026-06-15 — Branch: `feature/scraper-feed-fixes`*
