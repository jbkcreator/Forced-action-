"""Custom exceptions for the scraper layer."""


class ScraperNoDataError(Exception):
    """
    Raised when a scraper ran correctly but the source had no records for the
    requested date — not a system failure. Examples: no court filings published
    today, no storm events in the NWS feed.

    load_validator treats this as 'no_data' (log only) rather than 'scraper_error'
    (alert). This prevents spurious alerts on legitimate empty-filing days.
    """
