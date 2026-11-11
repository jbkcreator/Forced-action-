"""Lane guard (brief section 7, "Lanes"): Cora is Next Deal Lending only. Nothing she sends may carry a Backflip
name, number or domain. Shared by the P1.1 job checks and the P1.5 send_gate deny list."""
from __future__ import annotations

BACKFLIP_DENY_WORDS: tuple[str, ...] = ("backflip",)
BACKFLIP_DENY_DOMAINS: tuple[str, ...] = ("backflip.com",)
# E.164, as src.services.phone_utils.normalize returns them: (656) 209-6700 and (656) 207-6500.
BACKFLIP_DENY_PHONES: tuple[str, ...] = ("+16562096700", "+16562076500")
