"""T-07 Minute-5 pre-qualification letter: delivery rule values."""
from __future__ import annotations

# Minutes to wait after the Nth failed send (the last value repeats) before the sweep retries.
RETRY_BACKOFF_MINUTES = (2, 5, 15, 60)
# A "Minute-5" letter that could not go out within a day is not sent late: the row stays failed.
GIVE_UP_AFTER_HOURS = 24
SWEEP_BATCH_SIZE = 50
# A letter still "sending" this long after it was claimed has an unknown outcome (the email may have
# gone out): it is flagged "uncertain" and never resent automatically. Well above one batch's run time.
SENDING_STALE_MINUTES = 30

# Who the borrower sees on every borrower-facing PDF and pre-qual email (client: no company name,
# sign as the loan officer). One place to change the signer or the callback number.
BORROWER_SIGNER = "Josh Kantor, Loan Officer"
BORROWER_CALLBACK_PHONE = "(727) 436-9951"
