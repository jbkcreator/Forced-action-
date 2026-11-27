"""R1 1.1 identity resolution (pure): union-find over records, merge-key identifiers and existing contact ids.

Owners are strings: ``"id:<n>"`` for an existing contact, ``"seed:<seed_key>"`` for one the sync will create.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Hashable, Iterable, Mapping

from config.lending_contact_spine import NAME_RANK, NAMELESS_SOURCES, PHONE, PHONE_KEYED_KINDS
from src.lending.spine.identity import Identifier, SourceRecord


@dataclass(frozen=True)
class ResolvePlan:
    new_seeds: tuple[str, ...]
    identifier_owner: Mapping[Identifier, str]
    landline_links: tuple[tuple[Identifier, str], ...]
    source_owner: Mapping[str, str]
    merges: tuple[tuple[int, int], ...]
    display_name: Mapping[str, str]


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[Hashable, Hashable] = {}

    def find(self, node: Hashable) -> Hashable:
        self.parent.setdefault(node, node)
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[node] != root:
            self.parent[node], node = root, self.parent[node]
        return root

    def union(self, a: Hashable, b: Hashable) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _merge_keys(record: SourceRecord, landlines: frozenset[str]) -> frozenset[Identifier]:
    """A known landline never merges; on a record with one, a GHL or BatchDialer id may stand for that line."""
    on_landline = any(i.kind == PHONE and i.value in landlines for i in record.identifiers)
    return frozenset(i for i in record.identifiers
                     if not (i.kind == PHONE and i.value in landlines)
                     and not (on_landline and i.kind in PHONE_KEYED_KINDS))


def resolve(records: Iterable[SourceRecord], *, owners: Mapping[Identifier, int],
            source_owners: Mapping[str, int], landlines: frozenset[str]) -> ResolvePlan:
    merge_keys = {}
    kept: list[SourceRecord] = []
    for record in records:
        keys = _merge_keys(record, landlines)
        # a nameless row with only a landline would be one more empty contact per call to that line
        if keys or record.key in source_owners or record.source not in NAMELESS_SOURCES:
            merge_keys[record.key] = keys
            kept.append(record)
    records = kept
    uf = _UnionFind()
    for record in records:
        node = ("r", record.key)
        uf.find(node)
        for identifier in merge_keys[record.key]:
            uf.union(node, ("i", identifier))
            if identifier in owners:
                uf.union(node, ("c", owners[identifier]))
        if record.key in source_owners:
            uf.union(node, ("c", source_owners[record.key]))

    components: dict[Hashable, list[Hashable]] = defaultdict(list)
    for node in list(uf.parent):
        components[uf.find(node)].append(node)

    owner_of_root: dict[Hashable, str] = {}
    new_seeds: list[str] = []
    merges: list[tuple[int, int]] = []
    for root, nodes in components.items():
        contact_ids = sorted(value for kind, value in nodes if kind == "c")
        if contact_ids:
            survivor = contact_ids[0]
            merges.extend((survivor, other) for other in contact_ids[1:])
            owner_of_root[root] = f"id:{survivor}"
        else:
            seed = min(value for kind, value in nodes if kind == "r")
            new_seeds.append(seed)
            owner_of_root[root] = f"seed:{seed}"

    identifier_owner: dict[Identifier, str] = {}
    source_owner: dict[str, str] = {}
    landline_links: set[tuple[Identifier, str]] = set()
    best_name: dict[str, tuple[int, str, str]] = {}
    for record in records:
        owner = owner_of_root[uf.find(("r", record.key))]
        source_owner[record.key] = owner
        for identifier in record.identifiers:
            if identifier in merge_keys[record.key]:
                identifier_owner[identifier] = owner
            else:
                landline_links.add((identifier, owner))
        if record.name and record.name.strip():
            candidate = (NAME_RANK.get(record.source, len(NAME_RANK)), record.key, record.name.strip())
            if owner not in best_name or candidate < best_name[owner]:
                best_name[owner] = candidate

    return ResolvePlan(
        new_seeds=tuple(sorted(new_seeds)),
        identifier_owner=identifier_owner,
        landline_links=tuple(sorted(landline_links)),
        source_owner=source_owner,
        merges=tuple(sorted(merges)),
        display_name={owner: name for owner, (_, _, name) in best_name.items()},
    )
