"""The deal-facts and comps PDF uploaded with a Milo application."""
from __future__ import annotations

from datetime import date

from config.lending_milo import MILO_PACKET_TEMPLATE, MILO_PACKET_WATERMARK
from src.lending.milo.mapping import MiloApplication, has_comps
from src.lending.milo.package import MiloPackage


def _comp_row(raw: dict) -> dict:
    return {
        "address": raw.get("address") or "",
        "sale_date": raw.get("sale_date") or raw.get("sale") or "",  # the comps service gives the month as "sale"
        "sale_price": raw.get("sale_price") or "",
    }


def packet_context(package: MiloPackage, application: MiloApplication, *, today: date) -> dict:
    a = package.address
    comps = None
    if has_comps(package.comps):
        comps = {
            "low": package.comps.get("low") or "n/a",
            "high": package.comps.get("high") or "n/a",
            "point": package.comps.get("point") or "n/a",
            "comp_count": package.comps.get("comp_count") or len(package.comps.get("comps") or []),
            "confidence": package.comps.get("confidence") or "n/a",
            "comps": [_comp_row(c) for c in package.comps.get("comps") or []],
        }
    deal_rows = [
        ("Product", application.step1["product"]),
        ("Purchase price", f"${application.step2_property['Purchase Price']}"),
        ("Rehab budget", f"${application.step2_property['Estimated Cost of Rehab']}"),
        ("After-repair value", f"${application.step2_property['After Repair Value (ARV)']}"),
        ("Total project cost", f"${application.checks['Total Project Cost']}"),
        ("Gross profit", f"${application.checks['Gross Profit']}"),
        ("ARV / cost", application.checks["Viability Ratio"]),
    ]
    if package.entity_name:
        deal_rows.insert(0, ("Borrowing entity", package.entity_name))
    return {
        "generated_date": today.strftime("%B %d, %Y"),
        "package_ref": package.package_ref,
        "address": ", ".join(p for p in (a.line1, a.line2, a.city, f"{a.state} {a.zip_code}") if p),
        "deal_rows": deal_rows,
        "guarantors": list(package.guarantors),
        "comps": comps,
        "extra_facts": sorted((str(k), str(v)) for k, v in package.deal_facts.items()),
    }


def render_packet(package: MiloPackage, application: MiloApplication, *, today: date) -> bytes:
    from src.lending.pdf.render import render_pdf

    return render_pdf(MILO_PACKET_TEMPLATE, packet_context(package, application, today=today), watermark=MILO_PACKET_WATERMARK)
