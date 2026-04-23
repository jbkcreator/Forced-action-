"""
Local entry point for the Cora agents runtime.

Usage:
	python scripts/run_agents.py --migrate             # run checkpoint migration
	python scripts/run_agents.py --hello-world <id>    # run the smoke-test graph

This is a dev-only entry. In production the agents supervisor runs via its
own long-lived process; `scripts/run_agents.py` exists so developers can
exercise the stack without a full deploy.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Ensure the project root is on sys.path so `config` and `src` resolve when
# this script is run directly (e.g. `python scripts/run_agents.py`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
	sys.path.insert(0, str(_ROOT))

from config.agents import get_agents_settings  # noqa: E402


def _configure_logging() -> None:
	settings = get_agents_settings()
	logging.basicConfig(
		level=getattr(logging, settings.agents_log_level.upper(), logging.INFO),
		format="%(asctime)s  %(levelname)-7s  %(name)s: %(message)s",
	)


def cmd_migrate() -> int:
	from src.agents.checkpoint import run_checkpoint_migration
	run_checkpoint_migration()
	print("Checkpoint migration complete.")
	return 0


def cmd_hello_world(subscriber_id: int) -> int:
	from src.agents.graphs.hello_world import run
	final = run(subscriber_id)
	# Pretty-print the resulting state so a developer can eyeball it.
	print(json.dumps(final, indent=2, default=str))
	return 0


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(description="Cora agents runtime — dev entry point")
	parser.add_argument(
		"--migrate",
		action="store_true",
		help="Run the Postgres checkpoint migration (idempotent)",
	)
	parser.add_argument(
		"--hello-world",
		type=int,
		metavar="SUBSCRIBER_ID",
		help="Run the hello-world graph against the given subscriber ID",
	)

	args = parser.parse_args(argv)
	_configure_logging()

	if not any([args.migrate, args.hello_world is not None]):
		parser.print_help()
		return 2

	if args.migrate:
		rc = cmd_migrate()
		if rc != 0:
			return rc

	if args.hello_world is not None:
		return cmd_hello_world(args.hello_world)

	return 0


if __name__ == "__main__":
	sys.exit(main())
