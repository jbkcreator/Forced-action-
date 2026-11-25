"""R1 0.7: 1-click rollback of the lender underwriting rules to any earlier version.

    python scripts/rollback_lender_rules.py --list                    # versions, newest first
    python scripts/rollback_lender_rules.py --show 14                 # what rolling back to v14 would change
    python scripts/rollback_lender_rules.py --to 14 --yes             # restore v14 (written as a new version)
    python scripts/rollback_lender_rules.py --export 14 > rules.json  # staging mirror: take a version out ...
    python scripts/rollback_lender_rules.py --import rules.json --yes #   ... and load it into another database

A rollback never rewrites history: it copies the old rules back and records them as the next version
("rollback to v14"), posts to Cora's channel (who, from which version, to which), and does nothing when the table
already holds those rules.  Run it from the repo root with PYTHONPATH=. (needs the lending database in .env).
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys
from typing import Optional, Sequence

from config.lender_matrix import LENDER_MATRIX
from src.lending import lender_rules_store as store
from src.lending.db import lending_session

_ROW_KEYS = ("lender_key", "field", "value", "status")


def _confirmed(args: argparse.Namespace, question: str) -> bool:
    if args.yes:
        return True
    if not sys.stdin.isatty():
        print("Refusing without --yes (not a terminal).", file=sys.stderr)
        return False
    return input(f"{question} [y/N] ").strip().lower() == "y"


def _show_diff(lines: list[str]) -> None:
    for line in lines:
        print(f"  {line}")
    if not lines:
        print("  (no differences)")


def _load_import(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        rows = json.load(handle)
    if not isinstance(rows, list) or not all(isinstance(r, dict) and all(k in r for k in _ROW_KEYS) for r in rows):
        raise SystemExit(f"{path}: expected a JSON list of rows with {', '.join(_ROW_KEYS)}")
    for row in rows:
        try:
            store.check_rule_change(LENDER_MATRIX, row["lender_key"], row["field"], row["value"], row["status"])
        except store.RuleValueError as exc:
            raise SystemExit(f"{path}: {row['lender_key']}.{row['field']}: {exc}") from exc
    return rows


def run(args: argparse.Namespace) -> int:
    with lending_session() as db:
        if args.list:
            for snap in store.list_snapshots(db, args.limit):
                print(f"v{snap['version_no']:<4} {snap['created_at']:%Y-%m-%d %H:%M}  {snap['created_by']:<14} {snap['reason']}")
            return 0
        if args.export is not None:
            json.dump(store.get_snapshot(db, args.export), sys.stdout, indent=1, default=str)
            sys.stdout.write("\n")
            return 0
        if args.show is not None:
            print(f"Rolling back to v{args.show} would change:")
            _show_diff(store.diff_rules(store.current_rules(db), store.get_snapshot(db, args.show)))
            return 0
        if args.to is not None:
            target = store.get_snapshot(db, args.to)
            print(f"Current rules are v{store.current_rule_version(db)}. Rolling back to v{args.to} changes:")
            _show_diff(store.diff_rules(store.current_rules(db), target))
            if not _confirmed(args, f"Roll back to v{args.to}?"):
                return 1
            result = store.rollback_to(db, args.to, actor=args.actor, reason=args.reason)
            print(f"Done: v{result.new_version} now holds the rules of v{args.to}." if result.changed
                  else "Nothing to do: the rules already match.")
            return 0
        rows = _load_import(args.import_file)
        print(f"Importing {len(rows)} rules from {args.import_file} replaces the table:")
        _show_diff(store.diff_rules(store.current_rules(db), rows))
        if not _confirmed(args, "Replace the rules with this file?"):
            return 1
        version = store.replace_rules(db, rows, actor=args.actor, reason=args.reason or f"imported from {args.import_file}")
        print(f"Done: rules are now v{version}.")
        return 0


def parse(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--list", action="store_true", help="list versions, newest first")
    action.add_argument("--show", type=int, metavar="N", help="show what rolling back to version N would change")
    action.add_argument("--to", type=int, metavar="N", help="roll back to version N")
    action.add_argument("--export", type=int, metavar="N", help="print version N as JSON")
    action.add_argument("--import", dest="import_file", metavar="FILE", help="replace the rules with a JSON export")
    parser.add_argument("--actor", default=getpass.getuser(), help="who is doing this (default: the OS user)")
    parser.add_argument("--reason", help="why (default: 'rollback to vN')")
    parser.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    parser.add_argument("--limit", type=int, default=20, help="versions shown by --list")
    return parser.parse_args(argv)


if __name__ == "__main__":
    try:
        sys.exit(run(parse()))
    except store.SnapshotNotFound as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
