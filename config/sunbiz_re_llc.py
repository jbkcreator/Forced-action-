"""P3.2: the daily Florida real estate LLC export (src/tasks/sunbiz_re_llc_export.py).

Source: the Florida Division of Corporations daily corporate filings file on its public SFTP server (``doc/cor``,
``yyyymmddc.txt``: the filings entered into the state's database that day, generated on work days and weekends with
filings; none on days without). Layout: https://dos.sunbiz.org/data-definitions/cor.html (fixed-width, 1440
characters per record). Checked against the real file of Nov 15, 2026 (1357 records, all 1440 characters): dates are
MMDDYYYY, and the principal-address state (position 333) is blank in every record; the mailing, agent and officer
states are filled. The state posts each day's file at about 11:30 PM ET that night.
"""
from __future__ import annotations

# The brief's keywords (P3.2), matched as whole words in the entity name, case-insensitive ("REI" does not match
# "REIGN"). Josh may add more (PR Client blockers).
KEYWORDS: tuple[str, ...] = ("REI", "HOMES", "INVESTMENTS", "CAPITAL")
# "FLAL" = Florida Limited Liability Co. (the brief: "newly formed real estate LLCs"; foreign LLCs registering in
# Florida are "FORL" and are not included).
FILING_TYPES: frozenset[str] = frozenset({"FLAL"})
ACTIVE_STATUS = "A"

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
