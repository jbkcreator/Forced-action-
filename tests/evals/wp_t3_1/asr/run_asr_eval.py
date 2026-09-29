"""ASR eval for WP-T3-1 voice clips.

Transcribes each clip with the configured transcriber (default: FasterWhisper small.en),
runs extract_disposition, then checks ground-truth facts against the transcript and
strips financial terms from the stored text.

Usage (from repo root):
    PYTHONPATH=. python tests/evals/wp_t3_1/asr/run_asr_eval.py --clips-dir ~/Downloads
    PYTHONPATH=. python tests/evals/wp_t3_1/asr/run_asr_eval.py --clips-dir ~/Downloads --json-out asr_eval.json

The script matches each clip definition to an MP3/M4A/OGG file by looking for
clip['clip_hint'] anywhere in the filename.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

HERE = Path(__file__).resolve().parent
CASES_FILE = HERE / "scripts.json"
AUDIO_EXTS = {".mp3", ".m4a", ".ogg", ".wav", ".webm"}


def _find_clip(clips_dir: Path, hint: str) -> Optional[Path]:
    for f in clips_dir.iterdir():
        if f.suffix.lower() in AUDIO_EXTS and hint in f.name:
            return f
    return None


def _has(text: str, needle: str) -> bool:
    return needle.casefold() in text.casefold()


def score_clip(clip_def: dict, transcript: str, disposition, error: Optional[str]) -> dict:
    from src.services.fa_max_voice_intake import strip_financial

    result = {
        "id": clip_def["id"],
        "label": clip_def["label"],
        "transcript_chars": len(transcript),
        "transcript_preview": transcript[:120] + ("…" if len(transcript) > 120 else ""),
        "transcript_fail": [],
        "strip_fail": [],
        "outcome_ok": False,
        "disposition_error": error,
        "outcome": None,
    }

    for needle in clip_def.get("must_contain", []):
        if not _has(transcript, needle):
            result["transcript_fail"].append(needle)
    for variants in clip_def.get("must_contain_any", []):
        if not any(_has(transcript, v) for v in variants):
            result["transcript_fail"].append(f"any({variants})")

    if disposition is not None:
        stored = f"{disposition.summary or ''} {disposition.next_action or ''}"
        stored_clean = strip_financial(stored)
        result["outcome"] = disposition.outcome
        allowed = clip_def.get("expected_outcomes") or ([clip_def["expected_outcome"]] if "expected_outcome" in clip_def else [])
        result["outcome_ok"] = not allowed or disposition.outcome in allowed
        for needle in clip_def.get("must_strip", []):
            if _has(stored_clean, needle):
                result["strip_fail"].append(needle)

    return result


def run(clips_dir: Path, today_override: Optional[date] = None) -> list[dict]:
    from src.services.fa_max_voice_intake import FasterWhisperTranscriber, extract_disposition

    cases = json.loads(CASES_FILE.read_text(encoding="utf-8"))["clips"]
    transcriber = FasterWhisperTranscriber("small.en", 2)
    results = []

    for c in cases:
        clip_path = _find_clip(clips_dir, c["clip_hint"])
        if clip_path is None:
            print(f"  SKIP {c['id']} — no file matching hint '{c['clip_hint']}' in {clips_dir}")
            results.append({"id": c["id"], "skipped": True})
            continue

        print(f"  Transcribing {clip_path.name} …", end="", flush=True)
        t0 = time.monotonic()
        audio = clip_path.read_bytes()
        mimetype = "audio/mpeg" if clip_path.suffix == ".mp3" else "audio/ogg"
        transcript = transcriber.transcribe(audio, clip_path.name, mimetype)
        elapsed = time.monotonic() - t0
        print(f" {elapsed:.1f}s  ({len(transcript)} chars)")

        disposition = error = None
        try:
            disposition = extract_disposition(transcript, today=today_override or date.today())
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        results.append(score_clip(c, transcript, disposition, error))

    return results


def _print_results(results: list[dict]) -> bool:
    any_fail = False
    print("\n" + "=" * 72)
    print(f"{'ID':<22} {'Transcript':^10} {'Strip':^8} {'Outcome':^8}  {'Missed facts'}")
    print("-" * 72)
    for r in results:
        if r.get("skipped"):
            print(f"  {r['id']:<20} SKIP")
            continue
        t_ok = not r["transcript_fail"]
        s_ok = not r["strip_fail"]
        o_ok = r["outcome_ok"]
        status = "PASS" if (t_ok and s_ok and o_ok) else "FAIL"
        if status == "FAIL":
            any_fail = True
        missed = ", ".join(r["transcript_fail"]) or "—"
        strip_issues = ", ".join(r["strip_fail"]) or "—"
        print(f"  {r['id']:<20} {'✓' if t_ok else '✗':^10} {'✓' if s_ok else '✗':^8} "
              f"{'✓' if o_ok else '✗':^8}  {missed}")
        if not t_ok:
            print(f"    transcript missing: {missed}")
        if not s_ok:
            print(f"    strip leaked: {strip_issues}")
        if r["disposition_error"]:
            print(f"    disposition error: {r['disposition_error']}")
        print(f"    preview: {r['transcript_preview']}")
        print()
    print("=" * 72)
    print("RESULT:", "PASS" if not any_fail else "FAIL")
    return any_fail


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--clips-dir", type=Path, default=Path.home() / "Downloads",
                        help="Directory containing MP3 files (default: ~/Downloads)")
    parser.add_argument("--today", help="Override today date YYYY-MM-DD for disposition extraction")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)

    today = date.fromisoformat(args.today) if args.today else None
    print(f"Clips dir: {args.clips_dir}")
    results = run(args.clips_dir, today)

    failed = _print_results(results)

    if args.json_out:
        args.json_out.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
        print(f"Results written to {args.json_out}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
