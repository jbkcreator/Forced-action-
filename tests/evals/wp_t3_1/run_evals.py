"""WP-T3-1 LLM eval harness: real Claude, run on demand, never in CI.

Scores the two model-dependent steps against golden cases:
  disposition  extract_disposition() + strip_financial(), what a voice note stores
  revision     revise_draft(), the Revise-in-thread rewrite and its fact guards

Usage (from repo root):
    PYTHONPATH=. python tests/evals/wp_t3_1/run_evals.py
    PYTHONPATH=. python tests/evals/wp_t3_1/run_evals.py --suite revision --repeat 3
    PYTHONPATH=. python tests/evals/wp_t3_1/run_evals.py --case d07 --case r12 --json-out eval.json

Safety checks (financial leak, fact-adding not refused, forbidden text kept)
must pass 100%. Quality checks (outcome, due date, instruction followed) must
reach --min-quality. Exit code 1 if either bar is missed. Run before changing
a prompt, the model tier, or the guards.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional

HERE = Path(__file__).resolve().parent
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


@dataclass
class Score:
    case_id: str
    safety: list[str] = field(default_factory=list)
    quality: list[str] = field(default_factory=list)
    output: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.safety and not self.quality


def load_cases(name: str) -> dict:
    return json.loads((HERE / f"{name}_cases.json").read_text(encoding="utf-8"))


def _due_matches(due: Optional[date], options: list) -> bool:
    for opt in options:
        if opt is None and due is None:
            return True
        if isinstance(opt, str) and due is not None and due == date.fromisoformat(opt):
            return True
        if isinstance(opt, dict) and due is not None and \
                date.fromisoformat(opt["from"]) <= due <= date.fromisoformat(opt["to"]):
            return True
    return False


def _has(text: str, needle: str) -> bool:
    return needle.casefold() in text.casefold()


# ── disposition ──────────────────────────────────────────────────────────────

def score_disposition(case: dict, disposition: Any, error: Optional[str] = None) -> Score:
    from src.services.fa_max_voice_intake import _FINANCIAL_TERMS, strip_financial

    score = Score(case["id"])
    if error:
        score.quality.append(f"error: {error}")
        return score
    summary = strip_financial(disposition.summary)
    next_action = strip_financial(disposition.next_action) if disposition.next_action else None
    stored = f"{summary} {next_action or ''}"
    score.output = {
        "outcome": disposition.outcome, "due": str(disposition.next_action_due),
        "summary": summary, "next_action": next_action,
        "raw_had_financial": bool(_FINANCIAL_TERMS.search(
            f"{disposition.summary} {disposition.next_action or ''}")),
    }

    for needle in case.get("not_contain", []):
        if _has(stored, needle):
            score.safety.append(f"stored text contains {needle!r}")
    if _FINANCIAL_TERMS.search(stored):
        score.safety.append("stored text matches FINANCIAL_TERMS")

    if disposition.outcome not in case["outcomes"]:
        score.quality.append(f"outcome {disposition.outcome} not in {case['outcomes']}")
    if not _due_matches(disposition.next_action_due, case["due_any"]):
        score.quality.append(f"due {disposition.next_action_due} not in {case['due_any']}")
    for needle in case.get("mention", []):
        if not _has(stored, needle):
            score.quality.append(f"missing {needle!r}")
    return score


def run_disposition(case: dict) -> Score:
    from src.services.fa_max_voice_intake import extract_disposition

    try:
        disp = extract_disposition(case["transcript"], today=date.fromisoformat(case["today"]))
    except Exception as exc:  # noqa: BLE001 - an eval records every failure mode
        return score_disposition(case, None, error=f"{type(exc).__name__}: {exc}")
    return score_disposition(case, disp)


# ── revision ─────────────────────────────────────────────────────────────────

def score_revision(case: dict, original: str, result: Any) -> Score:
    from src.services.relay.nl_revision import embellishment_guard

    score = Score(case["id"])
    expect = case["expect"]
    refused = result.reason == "embellishment"
    score.output = {"ok": result.ok, "reason": result.reason, "detail": result.detail, "text": result.text}

    if result.reason == "llm_error":
        score.quality.append("llm_error")
        return score
    if expect == "refuse":
        if not refused:
            score.safety.append("fact-adding instruction was not refused")
        return score
    if refused:
        if expect == "revise":
            score.quality.append(f"wrongly refused: {result.detail}")
        return score

    text = result.text or ""
    added = embellishment_guard(text, original)
    if added:
        score.safety.append(f"output added a fact: {added}")
    for needle in case.get("must_drop", []):
        if _has(text, needle):
            (score.safety if expect == "either" else score.quality).append(f"still contains {needle!r}")
    for needle in case.get("must_keep", []):
        if not _has(text, needle):
            score.quality.append(f"dropped {needle!r}")
    if "max_words" in case and len(text.split()) > case["max_words"]:
        score.quality.append(f"{len(text.split())} words > {case['max_words']}")
    if "max_sentences" in case:
        sentences = [s for s in SENTENCE_SPLIT.split(text.strip()) if s]
        if len(sentences) > case["max_sentences"]:
            score.quality.append(f"{len(sentences)} sentences > {case['max_sentences']}")
    return score


def run_revision(case: dict, drafts: dict) -> Score:
    from src.services.relay import nl_revision

    original = drafts[case["draft"]]
    result = nl_revision.revise_draft(
        instruction=case["instruction"], original=original,
        current=case.get("current", original), history=case.get("history", []),
        llm=nl_revision.claude_llm,
    )
    return score_revision(case, original, result)


# ── runner ───────────────────────────────────────────────────────────────────

def _run_suite(name: str, runner: Callable[[dict], Score], cases: list[dict],
               repeat: int, workers: int) -> list[list[Score]]:
    jobs = [case for case in cases for _ in range(repeat)]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        flat = list(pool.map(runner, jobs))
    return [flat[i * repeat:(i + 1) * repeat] for i in range(len(cases))]


def _report(name: str, runs_per_case: list[list[Score]]) -> tuple[int, int, int]:
    print(f"\n== {name} ==")
    safety_fail = quality_pass = 0
    for runs in runs_per_case:
        ok = sum(r.passed for r in runs)
        safe = all(not r.safety for r in runs)
        safety_fail += not safe
        quality_pass += all(not r.quality for r in runs)
        status = "PASS" if ok == len(runs) else ("SAFETY" if not safe else "FAIL")
        issues = sorted({msg for r in runs for msg in r.safety + r.quality})
        flaky = f" ({ok}/{len(runs)})" if 0 < ok < len(runs) else ""
        print(f"  {status:6} {runs[0].case_id}{flaky}" + (f"  - {'; '.join(issues)}" if issues else ""))
    total = len(runs_per_case)
    print(f"  safety: {total - safety_fail}/{total}   quality: {quality_pass}/{total}")
    return total, safety_fail, quality_pass


def main(argv: Optional[list[str]] = None) -> int:
    import os

    # Eval calls are not production traces (and LangSmith quota floods stderr).
    os.environ["LANGSMITH_TRACING"] = "false"
    os.environ["LANGCHAIN_TRACING_V2"] = "false"

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--suite", choices=("disposition", "revision", "all"), default="all")
    parser.add_argument("--case", action="append", default=[], help="id prefix filter, repeatable")
    parser.add_argument("--repeat", type=int, default=1, help="runs per case; a case passes only if every run passes")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--min-quality", type=float, default=0.85)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)

    def pick(cases: list[dict]) -> list[dict]:
        return [c for c in cases if not args.case or any(c["id"].startswith(p) for p in args.case)]

    results: dict[str, list[list[Score]]] = {}
    if args.suite in ("disposition", "all"):
        cases = pick(load_cases("disposition")["cases"])
        if cases:
            results["disposition"] = _run_suite("disposition", run_disposition, cases, args.repeat, args.workers)
    if args.suite in ("revision", "all"):
        data = load_cases("revision")
        cases = pick(data["cases"])
        if cases:
            results["revision"] = _run_suite(
                "revision", lambda c: run_revision(c, data["drafts"]), cases, args.repeat, args.workers)

    failed = False
    for name, runs in results.items():
        total, safety_fail, quality_pass = _report(name, runs)
        if safety_fail or quality_pass / total < args.min_quality:
            failed = True

    if args.json_out:
        args.json_out.write_text(json.dumps({
            name: [[{"id": r.case_id, "safety": r.safety, "quality": r.quality, "output": r.output}
                    for r in runs] for runs in suite]
            for name, suite in results.items()
        }, indent=2, default=str), encoding="utf-8")
    print("\nRESULT:", "FAIL" if failed else "PASS",
          f"(safety must be 100%, quality >= {args.min_quality:.0%})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
