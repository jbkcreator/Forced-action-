"""R1 1.1 Unified Contact Spine sync: resolve every source row to one canonical contact per person.

One pass reads every identity source, loads the current spine, resolves (``resolver.resolve``) and writes only what
changed: new contacts, merges (flattened, so ``merged_into`` always names the final survivor), identifiers, source
links and display names. A source that could not be read means nothing is written that run: a partial read could
miss the link between two records. ``lending.contacts`` (compliance, per phone) is read for line types only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, Optional, Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config.lending_contact_spine import PHONE
from src.lending.spine.identity import Identifier
from src.lending.spine.resolver import ResolvePlan, resolve
from src.lending.spine.sources import SOURCE_READERS, Reader, known_landlines, read_records

logger = logging.getLogger(__name__)

_CHUNK = 1000


@dataclass(frozen=True)
class SyncResult:
    records: int
    contacts_created: int
    merges: int
    identifiers_written: int
    sources_written: int
    failed_sources: tuple[str, ...]


@dataclass(frozen=True)
class _Current:
    merge_keys: dict[Identifier, int]          # merge-key identifier -> contact id
    links: set[tuple[Identifier, int]]         # non-merge (landline) identifier rows
    sources: dict[str, int]                    # "source:ref" -> contact id
    names: dict[int, Optional[str]]            # live contact id -> display name


def _chunks(items: Sequence, size: int = _CHUNK) -> Iterator[Sequence]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _load_current(db: Session) -> _Current:
    merge_keys: dict[Identifier, int] = {}
    links: set[tuple[Identifier, int]] = set()
    for kind, value, contact_id, merge_key in db.execute(text(
            "SELECT kind, value, contact_id, merge_key FROM lending.contact_identifiers")):
        if merge_key:
            merge_keys[Identifier(kind, value)] = contact_id
        else:
            links.add((Identifier(kind, value), contact_id))
    sources = {f"{s}:{r}": c for s, r, c in db.execute(text(
        "SELECT source, source_ref, contact_id FROM lending.contact_sources"))}
    names = dict(db.execute(text(
        "SELECT id, display_name FROM lending.contact_spine WHERE merged_into IS NULL")).all())
    return _Current(merge_keys, links, sources, names)


@dataclass(frozen=True)
class _Changes:
    demote: list[Identifier]                   # merge-key phones now known as landlines
    promote: list[Identifier]                  # landline rows whose phone is a merge key again
    merge_rows: list[tuple[Identifier, str]]   # merge-key identifier -> owner, new or moved
    link_rows: list[tuple[Identifier, str]]    # landline link -> owner, new
    source_rows: list[tuple[str, str]]         # "source:ref" -> owner, new or moved
    names: dict[str, str]                      # owner -> display name, changed


def _owner_id(owner: str) -> Optional[int]:
    return int(owner[3:]) if owner.startswith("id:") else None


def _diff(plan: ResolvePlan, current: _Current, landlines: frozenset[str]) -> _Changes:
    survivor = {merged: kept for kept, merged in plan.merges}

    def now_owned(contact_id: int) -> int:
        return survivor.get(contact_id, contact_id)

    def changed(owner: str, existing: Optional[int]) -> bool:
        return existing is None or _owner_id(owner) != now_owned(existing)

    demote = sorted(i for i in current.merge_keys if i.kind == PHONE and i.value in landlines)
    links = {(i, now_owned(c)) for i, c in current.links}
    links.update((i, now_owned(current.merge_keys[i])) for i in demote)
    promote = sorted({i for i, _ in current.links if i.kind == PHONE and i.value not in landlines})
    return _Changes(
        demote=demote,
        promote=promote,
        merge_rows=sorted((i, o) for i, o in plan.identifier_owner.items()
                          if i in promote or changed(o, current.merge_keys.get(i))),
        link_rows=sorted((i, o) for i, o in plan.landline_links
                         if _owner_id(o) is None or (i, _owner_id(o)) not in links),
        source_rows=sorted((k, o) for k, o in plan.source_owner.items() if changed(o, current.sources.get(k))),
        names={o: n for o, n in plan.display_name.items()
               if _owner_id(o) is None or current.names.get(now_owned(_owner_id(o))) != n},
    )


def sync_contact_spine(db: Session, *, now: datetime, readers: Sequence[tuple[str, Reader]] = SOURCE_READERS,
                       dry_run: bool = False) -> SyncResult:
    """Resolve and write the spine in the caller's transaction (the caller commits). Writes nothing on
    ``dry_run`` or when any source failed; the counts are what the run would write."""
    records, failed = read_records(db, tuple(readers))
    landlines = known_landlines(db)
    current = _load_current(db)
    owners = {i: c for i, c in current.merge_keys.items() if not (i.kind == PHONE and i.value in landlines)}
    plan = resolve(records, owners=owners, source_owners=current.sources, landlines=landlines)
    changes = _diff(plan, current, landlines)
    result = SyncResult(
        records=len(records),
        contacts_created=len(plan.new_seeds),
        merges=len(plan.merges),
        identifiers_written=len(changes.demote) + len(changes.merge_rows) + len(changes.link_rows),
        sources_written=len(changes.source_rows),
        failed_sources=tuple(failed),
    )
    if failed:
        logger.warning("contact spine: nothing written, sources unreadable: %s", ", ".join(failed))
        return result
    if not dry_run:
        _write(db, plan, changes, now)
    return result


def _write(db: Session, plan: ResolvePlan, changes: _Changes, now: datetime) -> None:
    seed_ids = _insert_seeds(db, plan, now)

    def resolve_owner(owner: str) -> int:
        return _owner_id(owner) if owner.startswith("id:") else seed_ids[owner[5:]]

    if changes.demote:
        db.execute(text("UPDATE lending.contact_identifiers SET merge_key = false "
                        "WHERE merge_key AND kind = :k AND value = :v"),
                   [{"k": i.kind, "v": i.value} for i in changes.demote])
    if changes.promote:
        db.execute(text("DELETE FROM lending.contact_identifiers WHERE NOT merge_key AND kind = :k AND value = :v"),
                   [{"k": i.kind, "v": i.value} for i in changes.promote])
    _apply_merges(db, plan.merges, now)
    if changes.merge_rows:
        db.execute(text("""
            INSERT INTO lending.contact_identifiers (contact_id, kind, value, merge_key, first_seen_at, last_seen_at)
            VALUES (:c, :k, :v, true, :now, :now)
            ON CONFLICT (kind, value) WHERE merge_key
            DO UPDATE SET contact_id = excluded.contact_id, last_seen_at = excluded.last_seen_at"""),
            [{"c": resolve_owner(o), "k": i.kind, "v": i.value, "now": now} for i, o in changes.merge_rows])
    if changes.link_rows:
        db.execute(text("""
            INSERT INTO lending.contact_identifiers (contact_id, kind, value, merge_key, first_seen_at, last_seen_at)
            VALUES (:c, :k, :v, false, :now, :now)
            ON CONFLICT (kind, value, contact_id) DO NOTHING"""),
            [{"c": resolve_owner(o), "k": i.kind, "v": i.value, "now": now} for i, o in changes.link_rows])
    if changes.source_rows:
        db.execute(text("""
            INSERT INTO lending.contact_sources (source, source_ref, contact_id, seen_at) VALUES (:s, :r, :c, :now)
            ON CONFLICT (source, source_ref) DO UPDATE SET contact_id = excluded.contact_id, seen_at = excluded.seen_at"""),
            [{"s": k.split(":", 1)[0], "r": k.split(":", 1)[1], "c": resolve_owner(o), "now": now}
             for k, o in changes.source_rows])
    named = [{"i": resolve_owner(o), "n": n[:200], "now": now} for o, n in changes.names.items() if o.startswith("id:")]
    if named:
        db.execute(text("UPDATE lending.contact_spine SET display_name = :n, updated_at = :now WHERE id = :i"), named)
    db.flush()


def _insert_seeds(db: Session, plan: ResolvePlan, now: datetime) -> dict[str, int]:
    if not plan.new_seeds:
        return {}
    names = plan.display_name
    db.execute(text("INSERT INTO lending.contact_spine (seed_key, display_name, created_at, updated_at) "
                    "VALUES (:s, :n, :now, :now) ON CONFLICT (seed_key) DO NOTHING"),
               [{"s": seed, "n": (names.get(f"seed:{seed}") or "")[:200] or None, "now": now}
                for seed in plan.new_seeds])
    ids: dict[str, int] = {}
    select = text("SELECT seed_key, COALESCE(merged_into, id) FROM lending.contact_spine "
                  "WHERE seed_key IN :seeds").bindparams(bindparam("seeds", expanding=True))
    for chunk in _chunks(plan.new_seeds):
        ids.update(db.execute(select, {"seeds": list(chunk)}).all())
    return ids


def _apply_merges(db: Session, merges: Iterable[tuple[int, int]], now: datetime) -> None:
    params = [{"s": kept, "m": merged, "now": now} for kept, merged in merges]
    if not params:
        return
    db.execute(text("UPDATE lending.contact_spine SET merged_into = :s, updated_at = :now "
                    "WHERE id = :m OR merged_into = :m"), params)
    db.execute(text("""
        UPDATE lending.contact_identifiers SET contact_id = :s
         WHERE contact_id = :m
           AND NOT EXISTS (SELECT 1 FROM lending.contact_identifiers o
                            WHERE o.contact_id = :s AND o.kind = lending.contact_identifiers.kind
                              AND o.value = lending.contact_identifiers.value)"""), params)
    db.execute(text("DELETE FROM lending.contact_identifiers WHERE contact_id = :m"), params)
    db.execute(text("UPDATE lending.contact_sources SET contact_id = :s WHERE contact_id = :m"), params)
    db.execute(text("INSERT INTO lending.contact_spine_merges (survivor_id, merged_id, merged_at) "
                    "VALUES (:s, :m, :now)"), params)


def canonical_id(db: Session, contact_id: int) -> Optional[int]:
    """The live contact a (possibly merged) id resolves to; None when the id is unknown."""
    row = db.execute(text("SELECT COALESCE(merged_into, id) FROM lending.contact_spine WHERE id = :i"),
                     {"i": contact_id}).first()
    return row[0] if row else None
