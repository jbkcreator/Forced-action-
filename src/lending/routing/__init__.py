"""R-1 Overflow Router — public surface.

Production entry points:
  route_file(lead_id, db)  — called by the enrichment service on lead creation
                             and by the routing sweep for unrouted files.
  record_decline(event_id, reason_code, actor, db, free_text)
                           — called by the decline endpoint.

All other modules in this package are internal.
"""
from src.lending.routing.router import route_file as route_file  # noqa: F401 (re-export)
from src.lending.routing.decline import record_decline as record_decline  # noqa: F401
