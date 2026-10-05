"""Josh's List 1-9 taxonomy — single source of truth for list_key display names.

This is the only place the numbered-list names are spelled out; both the
``lending.list_catalog`` reference table (migrations/apply_lending_list_catalog.py)
and anything that reports on source_tag read from here, so the two can never drift.

pool_extraction.py's ``source_tag_for()`` is the only place that *assigns* a
list_key to a staged row — this catalog only documents what each key means and
which pool(s) and queue it belongs to; it does not change any gate's behavior.
"""
from __future__ import annotations

from typing import NamedTuple, Optional


class ListCatalogEntry(NamedTuple):
    display_name: str
    pool_name: Optional[str]  # None: no extractor produces this list yet (F1 gap)
    queue: str  # see config/lending_queues.py


LIST_CATALOG: dict[str, ListCatalogEntry] = {
    # F1 (pending-tasks doc): no extractor exists for these three yet — List 5 (GA)
    # needs GSCCCA, List 1/8 need FL maturity/private-money data verification.
    "list_1": ListCatalogEntry("Verified Maturity — FL", None, "verified_maturity"),
    "list_2": ListCatalogEntry("Cash Buyers", "wholesaler_flipper", "transaction_ready"),
    "list_3": ListCatalogEntry("Builders — DBPR", "active_builder", "builders"),
    "list_4": ListCatalogEntry("Brokers and LOs", "mortgage_broker", "partners"),
    "list_5": ListCatalogEntry("Verified Maturity — GA", None, "verified_maturity"),
    "list_6": ListCatalogEntry("Auction Winners", "auction_winner", "transaction_ready"),
    "list_7": ListCatalogEntry("NOCs and Permits", "permit_owner", "builders"),
    "list_8": ListCatalogEntry("Verified Maturity — Private Money", None, "verified_maturity"),
    "list_9": ListCatalogEntry("Stalled Flips", "wholesaler_flipper", "transaction_ready"),
}
