"""Acceptance Test 2: sample borrowers through the Lender Box engine and the pre-qual letter.

Runs the four profiles in ``src.lending.prequal_acceptance`` against the current lender matrix
(config, or ``lending.lender_rule_values`` when LENDING_RULES_FROM_DB is on). Each qualifying profile
gets the watermarked pre-qual letter PDF in ``--out``; every profile is written to
``test2_report.json`` with its ranked fitting lenders (each marked confirmed or "subject to lender
confirmation") or, for a non-qualifying profile, the engine's structured failure codes.
The ranking and codes are internal output; the letter PDF is unchanged.

``--send-to CONTACT_ID`` also emails the first qualifying letter through the live Next Deal Lending GHL
account to that contact. Use only a test contact whose email you own.

    python scripts/lending_prequal_demo.py --out demo_out
    python scripts/lending_prequal_demo.py --out demo_out --send-to <test contact id>
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_settings
from config.lender_matrix import LENDER_MATRIX
from src.lending.lender_rules_store import current_matrix
from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK, render_pdf
from src.lending.prequal import TEMPLATE, build_context
from src.lending.prequal_acceptance import TEST2_PROFILES, evaluate_profile

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="prequal_demo_out")
    parser.add_argument("--config-matrix", action="store_true",
                        help="use config/lender_matrix.py even when LENDING_RULES_FROM_DB is on (no DB read)")
    parser.add_argument("--send-to", metavar="CONTACT_ID", help="email the first qualifying letter to this GHL test contact")
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    pct = get_settings().lending_prequal_range_pct
    from_db = get_settings().lending_rules_from_db and not args.config_matrix
    matrix = current_matrix() if from_db else LENDER_MATRIX
    logger.info("lender matrix source: %s", "lending.lender_rule_values (LENDING_RULES_FROM_DB)" if from_db
                else "config/lender_matrix.py")
    first_pdf: bytes | None = None
    report = []
    for profile in TEST2_PROFILES:
        outcome = evaluate_profile(profile, matrix=matrix)
        ctx = build_context(profile.lead, outcome.limits, pct) if outcome.qualifies else None
        entry = {k: v for k, v in asdict(outcome).items() if k != "limits"}
        if ctx is None:
            logger.info("%s: no letter; failure codes %s", profile.name, outcome.failure_codes)
        else:
            pdf = render_pdf(TEMPLATE, ctx, watermark=NON_BINDING_PREQUAL_WATERMARK)
            (out / f"{profile.name}.pdf").write_bytes(pdf)
            entry["letter"] = {"pdf": f"{profile.name}.pdf", "range_low": ctx["range_low"], "range_high": ctx["range_high"]}
            logger.info("%s: letter %s - %s; %d ranked lender(s), top %s (%s)", profile.name, ctx["range_low"],
                        ctx["range_high"], len(outcome.ranking), outcome.ranking[0].lender_name, outcome.ranking[0].status)
            first_pdf = first_pdf or pdf
        report.append(entry)
    (out / "test2_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("report -> %s", out / "test2_report.json")

    if args.send_to and first_pdf:
        from src.lending.prequal_ghl import get_live_sink

        sink = get_live_sink()
        if sink is None:
            raise SystemExit("LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID not set")
        sink.deliver(0, args.send_to, first_pdf)
        logger.info("emailed the first qualifying letter to the test contact")


if __name__ == "__main__":
    main()
