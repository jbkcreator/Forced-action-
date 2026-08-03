"""
Venture ladder evaluator — CLONE-v2.2 / CL4.

The cron driver that makes the ladder autonomous rather than a library. Once
per run it walks every venture, evaluates its current rung, advances the ones
whose gates are all green, runs the auto-double rule, and posts a digest to
Slack.

Mirrors src/tasks/county_launch_evaluator.py: the service computes and returns
data, this decides to act and notifies. No business logic here — thresholds
live in config/venture_ladder.py and every decision is made by
src/services/venture_ladder.py.

WHAT IT WILL AND WILL NOT DO ON ITS OWN

Advancing up to `cell` is fully autonomous: those rungs are reversible and
nothing there spends money that cannot be recovered. `cell -> spin_up` is NOT,
because spin-up buys a domain, a mailbox warmup, an Instantly seat and proxy
capacity. That transition is proposed to Slack and needs `--advance-spin-up`
(or a human running the CLI) — the presell gate authorises the spend, a person
still releases it. Same division county_launch_evaluator already draws: gates
compute automatically, the launch itself is one-tap.

Cron (staggered clear of the 04:00-08:00 scraper/CDS/GHL chain):
    30 9 * * *  src.tasks.venture_ladder_evaluator

Usage:
    python -m src.tasks.venture_ladder_evaluator [--dry-run] [--venture KEY]
                                                 [--advance-spin-up] [--no-auto-double]
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from sqlalchemy import text

from config.settings import get_settings
from config.venture_ladder import TERMINAL_STAGE
from src.core.database import get_db_context
from src.services import venture_ladder

try:
    from slack_sdk import WebClient
except ImportError:  # pragma: no cover
    WebClient = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

ACTOR = "venture_ladder_evaluator"

# Transitions this task will never make unattended — see the module docstring.
HUMAN_RELEASED_TRANSITIONS = frozenset({("cell", "spin_up")})

_GATE_EMOJI = {"green": ":white_check_mark:", "yellow": ":warning:", "red": ":x:"}


def _all_venture_keys(db) -> list[str]:
    """Every venture, active or not.

    Inactive ventures are included on purpose: a radar candidate is an inactive
    row, and excluding it would mean the evaluator never looks at the ventures
    it exists to assess.
    """
    rows = db.execute(text(
        "SELECT venture_key FROM ventures ORDER BY ladder_stage, venture_key"
    )).fetchall()
    return [row.venture_key for row in rows]


def _format_gate_lines(evaluation) -> list[str]:
    lines = []
    for gate in evaluation.gates:
        shown = "N/A" if gate.value is None else f"{gate.value:g}"
        suffix = " _(no metric — imputed)_" if gate.imputed else ""
        emoji = _GATE_EMOJI.get(gate.color, ":question:")
        lines.append(
            f"• {gate.name} {shown} (threshold {gate.threshold:g}) {emoji}{suffix}"
        )
    return lines


def _slack_blocks(results: list[dict]) -> list[dict]:
    sections: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*Venture ladder — daily evaluation*"},
    }]

    for result in results:
        header = f"*{result['venture_key']}* — `{result['stage']}`"
        if result["action"] == "advanced":
            header += f" :arrow_up: advanced to `{result['to_stage']}`"
        elif result["action"] == "awaiting_release":
            header += (
                f" :hourglass: ready for `{result['to_stage']}` — needs a human "
                "(spin-up spends real money)"
            )
        elif result["action"] == "terminal":
            header += " :checkered_flag: steady state"
        else:
            header += " :lock: blocked"

        body = [header]
        if result["gate_lines"]:
            body.append("```\n" + "\n".join(result["gate_lines"]) + "\n```")
        if result["blocked_reasons"]:
            body.append("_" + "; ".join(result["blocked_reasons"][:4]) + "_")
        if result["auto_double"]:
            body.append(f":chart_with_upwards_trend: {result['auto_double']}")

        sections.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(body)},
        })

    return sections


def _post_to_slack(blocks: list[dict]) -> None:
    settings = get_settings()
    token = getattr(settings, "slack_bot_token", None)
    channel = getattr(settings, "relay_slack_channel", None)

    if WebClient is None or not token or not channel:
        logger.info(
            "[venture_ladder_evaluator] Slack not configured — digest not posted "
            "(evaluation and advances still applied)"
        )
        return

    try:
        # settings.slack_bot_token is a pydantic SecretStr. Passing it straight
        # to WebClient sends the literal "**********" and every post 401s
        # silently, so it must be unwrapped — same as
        # county_launch_evaluator.py does.
        raw_token = token.get_secret_value() if hasattr(token, "get_secret_value") else token
        WebClient(token=raw_token).chat_postMessage(
            channel=channel,
            text="Venture ladder — daily evaluation",
            blocks=blocks,
        )
    except Exception:
        logger.error(
            "[venture_ladder_evaluator] could not post the ladder digest to Slack",
            exc_info=True,
        )


def evaluate_venture(
    db,
    venture_key: str,
    *,
    dry_run: bool,
    advance_spin_up: bool,
    auto_double: bool,
) -> dict:
    """Evaluate, conditionally advance, and conditionally auto-double one
    venture. Returns a digest row."""
    evaluation = venture_ladder.evaluate(db, venture_key)
    result = {
        "venture_key": venture_key,
        "stage": evaluation.current_stage,
        "to_stage": evaluation.next_stage,
        "action": "blocked",
        "gate_lines": _format_gate_lines(evaluation),
        "blocked_reasons": list(evaluation.blocked_reasons),
        "auto_double": None,
    }

    if evaluation.current_stage == TERMINAL_STAGE:
        result["action"] = "terminal"
    elif evaluation.next_stage is None:
        result["action"] = "unknown_stage"
    elif evaluation.may_advance:
        transition = (evaluation.current_stage, evaluation.next_stage)
        if transition in HUMAN_RELEASED_TRANSITIONS and not advance_spin_up:
            result["action"] = "awaiting_release"
            logger.info(
                "[venture_ladder_evaluator] %s is clear for %s but that transition "
                "spends real money — awaiting a human",
                venture_key, evaluation.next_stage,
            )
        elif dry_run:
            result["action"] = "would_advance"
        else:
            advanced = venture_ladder.advance(db, venture_key, actor=ACTOR)
            result["action"] = "advanced"
            result["to_stage"] = advanced.current_stage
    elif not dry_run:
        # Record the refusal so a venture stuck for weeks is visible in the
        # audit table rather than only in a Slack scrollback.
        venture_ladder.advance(db, venture_key, actor=ACTOR)

    # Auto-double is independent of advancement: a venture sitting at `cell`
    # for months should still scale on a good reply rate.
    if auto_double and not dry_run:
        outcome = venture_ladder.maybe_auto_double(db, venture_key, actor=ACTOR)
        if outcome.fired:
            result["auto_double"] = (
                f"ceiling {outcome.previous_ceiling} -> {outcome.new_ceiling} "
                f"({outcome.reason})"
            )

    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.tasks.venture_ladder_evaluator",
        description="Evaluate every venture's ladder rung and advance the clear ones",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Evaluate and report without advancing, auto-doubling, or writing audit rows",
    )
    parser.add_argument("--venture", help="Evaluate one venture instead of all")
    parser.add_argument(
        "--advance-spin-up", action="store_true",
        help="Also make the cell -> spin_up transition, which spends real money",
    )
    parser.add_argument(
        "--no-auto-double", action="store_true",
        help="Skip the auto-double rule this run",
    )
    args = parser.parse_args(argv)

    from src.utils.logger import setup_logging

    setup_logging()

    results: list[dict] = []
    with get_db_context() as db:
        keys = [args.venture] if args.venture else _all_venture_keys(db)
        if not keys:
            logger.warning(
                "[venture_ladder_evaluator] no ventures found — has "
                "migrations/apply_cl3_venture_config.py run?"
            )
            return 0

        for venture_key in keys:
            try:
                results.append(evaluate_venture(
                    db, venture_key,
                    dry_run=args.dry_run,
                    advance_spin_up=args.advance_spin_up,
                    auto_double=not args.no_auto_double,
                ))
            except LookupError:
                logger.error(
                    "[venture_ladder_evaluator] no ventures row for %r — skipped",
                    venture_key,
                )
            except Exception:
                # One venture's bad data must not stop the rest of the fleet
                # being evaluated.
                logger.error(
                    "[venture_ladder_evaluator] evaluation failed for %s — skipped",
                    venture_key, exc_info=True,
                )

        if args.dry_run:
            db.rollback()
        else:
            db.commit()

    advanced = [r for r in results if r["action"] == "advanced"]
    waiting = [r for r in results if r["action"] == "awaiting_release"]
    doubled = [r for r in results if r["auto_double"]]
    logger.info(
        "[venture_ladder_evaluator] evaluated %d venture(s): %d advanced, "
        "%d awaiting release, %d auto-doubled%s",
        len(results), len(advanced), len(waiting), len(doubled),
        " (dry run)" if args.dry_run else "",
    )

    if results and not args.dry_run:
        _post_to_slack(_slack_blocks(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())
