"""T-07 demo for acceptance Test 2: run sample borrowers through the pre-qual letter.

Renders one PDF per scenario into ``--out``. The four scenarios mirror spec §5 Test 2 (fix and flip,
experienced builder, credit-challenged investor, and a non-qualifying borrower that gets no letter).
Lender limits are FIXTURES for the demo, not real lender parameters, until T-05 is wired through
``src.lending.prequal_fit``. No database is touched.

``--send-to CONTACT_ID`` also emails the first qualifying letter through the live Next Deal Lending GHL
account to that contact. Use only a test contact whose email you own.

    PYTHONPATH=. python scripts/lending_prequal_demo.py --out demo_out
    PYTHONPATH=. python scripts/lending_prequal_demo.py --out demo_out --send-to <test contact id>
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from config.settings import get_settings
from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK, render_pdf
from src.lending.prequal import TEMPLATE, FitLimits, PrequalLead, build_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# name -> (lead, fitting-lender limits). Fixture values for the demo only.
SCENARIOS: dict[str, tuple[PrequalLead, list[FitLimits]]] = {
    "1_fix_and_flip": (PrequalLead("700+", 400_000, "FL", "FIX_AND_FLIP"),
                       [FitLimits(100_000, 2_000_000), FitLimits(75_000, 1_500_000)]),
    "2_experienced_builder": (PrequalLead("740+", 1_200_000, "FL", "GROUND_UP_CONSTRUCTION"),
                              [FitLimits(500_000, 1_250_000)]),
    "3_credit_challenged_investor": (PrequalLead("640-679", 250_000, "GA", "DSCR_RENTAL"),
                                     [FitLimits(100_000, 3_000_000)]),
    "4_non_qualifying": (PrequalLead("Below 620", 350_000, "FL", "GROUND_UP_CONSTRUCTION"), []),
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="prequal_demo_out")
    parser.add_argument("--send-to", metavar="CONTACT_ID", help="email the first qualifying letter to this GHL test contact")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    pct = get_settings().lending_prequal_range_pct
    first_pdf: bytes | None = None
    for name, (lead, fits) in SCENARIOS.items():
        ctx = build_context(lead, fits, pct)
        if ctx is None:
            logger.info("%s: no letter (no fitting lender)", name)
            continue
        pdf = render_pdf(TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
        (out / f"{name}.pdf").write_bytes(pdf)
        logger.info("%s: letter %s - %s -> %s", name, ctx["range_low"], ctx["range_high"], out / f"{name}.pdf")
        first_pdf = first_pdf or pdf

    if args.send_to and first_pdf:
        from src.lending.prequal_ghl import get_live_sink

        sink = get_live_sink()
        if sink is None:
            raise SystemExit("LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID not set")
        sink.deliver(0, args.send_to, first_pdf)
        logger.info("emailed scenario 1 letter to the test contact")


if __name__ == "__main__":
    main()
