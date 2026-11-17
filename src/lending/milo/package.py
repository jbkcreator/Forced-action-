"""The loan package one Milo application is built from.

A package is plain data: it loads from a JSON file (a test package) or from a soft approval's ``facts``.
It knows nothing about Milo; ``mapping.py`` turns it into Milo's fields.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional


class PackageError(ValueError):
    """The package is missing a value Milo needs, or a value is not usable."""


@dataclass(frozen=True)
class Guarantor:
    name: str
    fico: int
    owned_rentals: int = 0
    fix_flip_exits_24m: int = 0
    ground_up_exits_24m: int = 0
    foreign_national: bool = False


@dataclass(frozen=True)
class PropertyAddress:
    line1: str
    city: str
    state: str
    zip_code: str
    line2: str = ""


@dataclass(frozen=True)
class MiloPackage:
    package_ref: str
    product: str
    address: PropertyAddress
    property_type: str
    purchase_price: Decimal
    rehab: Decimal
    arv: Decimal
    guarantors: tuple[Guarantor, ...]
    refinance: bool = False
    entity_name: Optional[str] = None
    comps: Optional[dict] = None
    deal_facts: dict = field(default_factory=dict)
    is_test: bool = False


def _money(raw: Any, name: str) -> Decimal:
    try:
        value = Decimal(str(raw).replace(",", "").replace("$", ""))
    except (InvalidOperation, AttributeError) as exc:
        raise PackageError(f"{name} is not a number") from exc
    if value < 0:
        raise PackageError(f"{name} is negative")
    return value


def _required(data: dict, key: str) -> Any:
    value = data.get(key)
    if value in (None, ""):
        raise PackageError(f"{key} is required")
    return value


def _guarantor(raw: dict) -> Guarantor:
    fico = int(_required(raw, "fico"))
    if not 300 <= fico <= 850:
        raise PackageError("guarantor fico must be 300-850")
    return Guarantor(
        name=str(_required(raw, "name")),
        fico=fico,
        owned_rentals=int(raw.get("owned_rentals") or 0),
        fix_flip_exits_24m=int(raw.get("fix_flip_exits_24m") or 0),
        ground_up_exits_24m=int(raw.get("ground_up_exits_24m") or 0),
        foreign_national=bool(raw.get("foreign_national", False)),
    )


def package_from_dict(data: dict) -> MiloPackage:
    addr = _required(data, "address")
    guarantors = tuple(_guarantor(g) for g in data.get("guarantors") or [])
    if not guarantors:
        raise PackageError("at least one guarantor is required")
    return MiloPackage(
        package_ref=str(_required(data, "package_ref")),
        product=str(_required(data, "product")),
        address=PropertyAddress(
            line1=str(_required(addr, "line1")),
            line2=str(addr.get("line2") or ""),
            city=str(_required(addr, "city")),
            state=str(_required(addr, "state")).upper(),
            zip_code=str(_required(addr, "zip_code")),
        ),
        property_type=str(data.get("property_type") or "1_4_unit"),
        purchase_price=_money(_required(data, "purchase_price"), "purchase_price"),
        rehab=_money(data.get("rehab") or 0, "rehab"),
        arv=_money(_required(data, "arv"), "arv"),
        guarantors=guarantors,
        refinance=bool(data.get("refinance", False)),
        entity_name=data.get("entity_name") or None,
        comps=data.get("comps") or None,
        deal_facts=dict(data.get("deal_facts") or {}),
        is_test=bool(data.get("is_test", False)),
    )


def load_package(path: Path) -> MiloPackage:
    return package_from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
