"""The daily Florida real estate LLC export (src/tasks/sunbiz_re_llc_export.py).

Source: the Florida Division of Corporations daily corporate filings file on its public SFTP server (``doc/cor``,
``yyyymmddc.txt``: the filings entered into the state's database that day, generated on work days and weekends with
filings; none on days without). Layout: https://dos.sunbiz.org/data-definitions/cor.html (fixed-width, 1440
characters per record). Checked against the real file of Nov 15, 2026 (1357 records, all 1440 characters): dates are
MMDDYYYY, and the principal-address state (position 333) is blank in every record; the mailing, agent and officer
states are filled. The state posts each day's file at about 11:30 PM ET that night.
"""
from __future__ import annotations

# The brief's keywords, matched as whole words in the entity name, case-insensitive ("REI" does not match "REIGN").
# More can be added here once the client confirms the list.
KEYWORDS: tuple[str, ...] = ("REI", "HOMES", "INVESTMENTS", "CAPITAL")
# "FLAL" = Florida Limited Liability Co. (the brief: "newly formed real estate LLCs"; foreign LLCs registering in
# Florida are "FORL" and are not included).
FILING_TYPES: frozenset[str] = frozenset({"FLAL"})
ACTIVE_STATUS = "A"
# "New" = filed within this many days of the state file's date (future-effective filings count). The daily file also
# carries a few older entities (8 real days, Nov 9-16, 2026: 6 matched LLCs filed more than 30 days earlier);
# no LLC appeared in two daily files (0 of 286), so there is no cross-day dedup.
MAX_FILING_AGE_DAYS = 30
# The normal run also exports these earlier days when their CSV is not in Drive yet (a file the state posted late).
CATCH_UP_DAYS = 3

SFTP_DIR = "doc/cor"
SFTP_FILE = "{day:%Y%m%d}c.txt"
SFTP_PORT = 22
SFTP_TIMEOUT_SECONDS = 60
# The state server's SSH host key, checked on every connection so a spoofed server cannot feed a fake file into the
# outreach CSV. Read from sftp.floridados.gov on Nov 16, 2026 (SHA256:xTpc6X81is5Nyh2QXR6hIRhS7ayx5/BMr162zB73xyQ).
# If the state changes its key the export fails (alert on the --retry run); set SUNBIZ_SFTP_HOST_KEY to the new key.
SFTP_HOST_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIFRdq4ekBu4hkT7QRm+frcyLZFQZplBp63gRcEm0Kwq"
RECORD_LENGTH = 1440
FILE_ENCODING = "latin-1"  # the state's ASCII export; latin-1 never fails on a stray byte

# Drive: one file per day in a dated subfolder of LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID (same layout as the ledger exports).
EXPORT_FILE = "sunbiz_re_llcs_{day}.csv"
OFFICER_SLOTS = 6

# Contact lookup (src/lending/sunbiz_llc_trace.py). The state marks an officer or agent "P" (person) or "C" (company).
PERSON_TYPE = "P"
# Skip a lookup address that more than this many of the day's LLCs share: formation services and registered agents
# put hundreds of LLCs at one address, and one person there is not each LLC's owner.
MAX_LLCS_PER_ADDRESS = 2
