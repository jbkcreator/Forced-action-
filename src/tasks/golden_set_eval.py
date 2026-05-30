"""
Golden-set evaluation harness — D4.

Curates representative inputs and evaluates Haiku vs Sonnet quality
to justify routing decisions. Run locally before changing _TASK_ROUTING.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)

GOLDEN_SET_DIR = Path(__file__).resolve().parents[2] / "config" / "golden_sets"

EVAL_TASK_TYPES = [
    "sms_copy",
    "email_copy",
    "chat_response",
]


@dataclass
class EvalCase:
    task_type: str
    graph_name: str
    system_prompt: str
    user_prompt: str


@dataclass
class EvalResult:
    case_id: str
    task_type: str
    graph_name: str
    haiku_text: str
    sonnet_text: str
    haiku_score: Optional[float] = None
    sonnet_score: Optional[float] = None
    verdict: Optional[str] = None


def load_golden_set(task_type: str) -> list[EvalCase]:
    """Load golden set cases for a task type from JSON file."""
    path = GOLDEN_SET_DIR / f"{task_type}.json"
    if not path.exists():
        logger.warning("Golden set not found: %s", path)
        return []

    with open(path, "r") as f:
        data = json.load(f)

    return [
        EvalCase(
            task_type=item["task_type"],
            graph_name=item.get("graph_name", "eval"),
            system_prompt=item.get("system_prompt", ""),
            user_prompt=item["user_prompt"],
        )
        for item in data
    ]


def run_eval(
    case: EvalCase,
    max_tokens: int = 256,
    dry_run: bool = False,
) -> tuple[str, str]:
    """Run a single case through Haiku and Sonnet, return both outputs."""
    messages = [{"role": "user", "content": case.user_prompt}]

    haiku_result = call_claude_with_usage(
        task_type=case.task_type,
        messages=messages,
        system=case.system_prompt or None,
        max_tokens=max_tokens,
        force_tier="haiku",
    )
    sonnet_result = call_claude_with_usage(
        task_type=case.task_type,
        messages=messages,
        system=case.system_prompt or None,
        max_tokens=max_tokens,
        force_tier="sonnet",
    )

    return haiku_result["text"], sonnet_result["text"]


def score_response(text: str, rubric: str) -> float:
    """
    Score a response using LLM-as-judge.
    
    Simple heuristic scoring: 0-100 based on length, structure, keyword presence.
    For production, replace with actual LLM judge call.
    """
    if not text or text.startswith("[BLOCKED]"):
        return 0.0

    score = 50.0
    text_lower = text.lower()

    if len(text) >= 20:
        score += 10
    if text_lower.endswith((".", "?", "!")):
        score += 10
    if any(kw in text_lower for kw in ["help", "assist", "support", "can ", "able to"]):
        score += 15
    if "http" in text_lower or "www" in text_lower:
        score += 15

    return min(score, 100.0)


_EVAL_RUBRIC = """
Score the response on a scale of 0-100:
- 0-30: Unacceptable - incomplete, irrelevant, or broken
- 31-60: Poor - some useful content but significant issues
- 61-80: Good - relevant and helpful with minor issues
- 81-100: Excellent - fully addresses the prompt, well-written
"""


def evaluate_golden_set(
    task_type: str,
    dry_run: bool = False,
    max_tokens: int = 256,
) -> dict:
    """Run full evaluation for a task type and return results."""
    cases = load_golden_set(task_type)
    if not cases:
        return {"task_type": task_type, "error": "No golden set cases found"}

    results = []
    haiku_wins = 0
    sonnet_wins = 0

    for i, case in enumerate(cases):
        haiku_text, sonnet_text = run_eval(case, max_tokens=max_tokens, dry_run=dry_run)

        haiku_score = score_response(haiku_text, _EVAL_RUBRIC)
        sonnet_score = score_response(sonnet_text, _EVAL_RUBRIC)

        if haiku_score >= sonnet_score:
            haiku_wins += 1
            verdict = "haiku_sufficient"
        else:
            sonnet_wins += 1
            verdict = "upgrade_to_sonnet"

        results.append({
            "case_id": f"{task_type}_{i:03d}",
            "task_type": task_type,
            "graph_name": case.graph_name,
            "haiku_score": round(haiku_score, 1),
            "sonnet_score": round(sonnet_score, 1),
            "verdict": verdict,
            "haiku_text": haiku_text[:100] + "..." if len(haiku_text) > 100 else haiku_text,
            "sonnet_text": sonnet_text[:100] + "..." if len(sonnet_text) > 100 else sonnet_text,
        })

    return {
        "task_type": task_type,
        "total_cases": len(cases),
        "haiku_wins": haiku_wins,
        "sonnet_wins": sonnet_wins,
        "recommendation": "haiku" if haiku_wins >= sonnet_wins else "sonnet",
        "results": results,
    }


def run_all_evals(output_path: Optional[Path] = None) -> dict:
    """Run evaluation for all task types and save results."""
    all_results = {}

    for task_type in EVAL_TASK_TYPES:
        logger.info("Evaluating task_type=%s", task_type)
        result = evaluate_golden_set(task_type, dry_run=False)
        all_results[task_type] = result

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(all_results, f, indent=2)
        logger.info("Results written to %s", output_path)

    return all_results


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else GOLDEN_SET_DIR / "eval_results.json"
    results = run_all_evals(output)

    for task_type, data in results.items():
        if "error" in data:
            print(f"{task_type}: {data['error']}")
        else:
            rec = data["recommendation"]
            wins = data["haiku_wins"]
            total = data["total_cases"]
            print(f"{task_type}: {rec} ({wins}/{total} Haiku wins)")