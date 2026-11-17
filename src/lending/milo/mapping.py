"""MiloPackage -> the values to enter on Milo's new-loan form (steps 1 and 2).

Output is transport-neutral: the same ``MiloApplication`` feeds an API client or a browser filler.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

from config.lending_milo import (
    MILO_FICO_BANDS,
    MILO_FICO_BANDS_CONFIRMED,
    MILO_GEO_RESTRICTION_DEFAULT,
    MILO_PRODUCTS,
    MILO_PROPERTY_TYPES,
    MILO_PROPERTY_TYPES_CONFIRMED,
    MILO_SUPPORTED_PRODUCTS,
)
from src.lending.milo.package import MiloPackage, PackageError

FLAG_FICO_BAND_UNCONFIRMED = "fico_band_unconfirmed"
FLAG_PROPERTY_TYPE_UNCONFIRMED = "property_type_unconfirmed"
FLAG_NO_COMPS = "no_comps"
FLAG_ARV_BELOW_COST = "arv_below_total_cost"


@dataclass(frozen=True)
class MiloApplication:
    package_ref: str
    step1: dict[str, str]
    step2_property: dict[str, str]
    step2_borrower: dict[str, str]
    checks: dict[str, str]
    flags: tuple[str, ...] = field(default_factory=tuple)

    def to_json(self) -> dict:
        return {
            "package_ref": self.package_ref, "step1": self.step1, "step2_property": self.step2_property,
            "step2_borrower": self.step2_borrower, "checks": self.checks, "flags": list(self.flags),
        }


def fico_band(fico: int) -> str:
    for floor, label in MILO_FICO_BANDS:
        if fico >= floor:
            return label
    return MILO_FICO_BANDS[-1][1]


def _whole(value: Decimal) -> str:
    return f"{value.quantize(Decimal('1'), rounding=ROUND_HALF_UP):,}"


def _ratio(arv: Decimal, cost: Decimal) -> str:
    if cost == 0:
        return "n/a"
    return str((arv / cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def map_package(package: MiloPackage) -> MiloApplication:
    if package.product not in MILO_SUPPORTED_PRODUCTS:
        raise PackageError(f"product {package.product!r} is not mapped yet")
    if package.property_type not in MILO_PROPERTY_TYPES:
        raise PackageError(f"property_type {package.property_type!r} has no Milo option")

    flags: list[str] = []
    if not MILO_FICO_BANDS_CONFIRMED:
        flags.append(FLAG_FICO_BAND_UNCONFIRMED)
    if not MILO_PROPERTY_TYPES_CONFIRMED:
        flags.append(FLAG_PROPERTY_TYPE_UNCONFIRMED)
    if not package.comps:
        flags.append(FLAG_NO_COMPS)

    # Pricing follows the weakest credit and the strongest track record, as in the instant quote.
    pricing_fico = min(g.fico for g in package.guarantors)
    lead = max(package.guarantors, key=lambda g: g.fix_flip_exits_24m + g.ground_up_exits_24m + g.owned_rentals)
    total_cost = package.purchase_price + package.rehab
    if package.arv < total_cost:
        flags.append(FLAG_ARV_BELOW_COST)

    addr = package.address
    return MiloApplication(
        package_ref=package.package_ref,
        step1={
            "product": MILO_PRODUCTS[package.product],
            "address": addr.line1,
            "address_line2": addr.line2,
            "city": addr.city,
            "state": addr.state,
            "zip": addr.zip_code,
            "borrower_entity": package.entity_name or "",
        },
        step2_property={
            "Product": MILO_PRODUCTS[package.product],
            "Property state": addr.state,
            "Refinance": "Yes" if package.refinance else "No",
            "Property Type": MILO_PROPERTY_TYPES[package.property_type],
            "GEO Restrictions": MILO_GEO_RESTRICTION_DEFAULT,
            "Purchase Price": _whole(package.purchase_price),
            "Estimated Cost of Rehab": _whole(package.rehab),
            "After Repair Value (ARV)": _whole(package.arv),
        },
        step2_borrower={
            "Est. FICO Score": fico_band(pricing_fico),
            "Foreign National": "Yes" if any(g.foreign_national for g in package.guarantors) else "No",
            "Currently Owned Rentals": str(lead.owned_rentals),
            "Fix & Flip Exits (Last 24 Months)": str(lead.fix_flip_exits_24m),
            "Ground-Up Exits (Last 24 Months)": str(lead.ground_up_exits_24m),
        },
        # What Milo calculates itself; compared after filling to prove the entry.
        checks={
            "Total Project Cost": _whole(total_cost),
            "Gross Profit": _whole(package.arv - total_cost),
            "Viability Ratio": _ratio(package.arv, total_cost),
        },
        flags=tuple(flags),
    )
