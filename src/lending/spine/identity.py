"""R1 1.1: identifiers that tie a source row to a person, normalized so equal values compare equal."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from config.lending_contact_spine import EMAIL, ENTITY, MIN_ENTITY_CHARS, PHONE
from src.lending.state_machine import _ENTITY_SUFFIX, normalize_entity_name
from src.services.phone_utils import normalize


@dataclass(frozen=True, order=True)
class Identifier:
    kind: str
    value: str


def phone_identifier(raw: Optional[str]) -> Optional[Identifier]:
    phone = normalize(raw) if raw else None
    return Identifier(PHONE, phone) if phone else None


def email_identifier(raw: Optional[str]) -> Optional[Identifier]:
    email = (raw or "").strip().lower()
    return Identifier(EMAIL, email) if "@" in email else None


def entity_identifier(raw: Optional[str]) -> Optional[Identifier]:
    if not raw or not raw.strip():
        return None
    name = normalize_entity_name(raw)
    # a bare suffix ("LLC") normalizes to itself and would join every unnamed entity
    if len(name.replace(" ", "")) < MIN_ENTITY_CHARS or _ENTITY_SUFFIX.fullmatch(name):
        return None
    return Identifier(ENTITY, name)


def external_identifier(kind: str, raw: Optional[str]) -> Optional[Identifier]:
    value = (raw or "").strip()
    return Identifier(kind, value) if value else None


@dataclass(frozen=True)
class SourceRecord:
    source: str
    ref: str
    name: Optional[str]
    identifiers: frozenset[Identifier]

    @property
    def key(self) -> str:
        return f"{self.source}:{self.ref}"
