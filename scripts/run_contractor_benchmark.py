"""
Stage 12 — Contractor Benchmark Report CLI.

Usage:
    python scripts/run_contractor_benchmark.py
    python scripts/run_contractor_benchmark.py --window 30
    python scripts/run_contractor_benchmark.py --vertical roofing --county hillsborough
    python scripts/run_contractor_benchmark.py --csv-only
    python scripts/run_contractor_benchmark.py --dry-run

Outputs both CSV and PDF to reports/contractor_benchmark/.
Pass --csv-only when running in environments without Playwright.
"""
import sys

if __name__ == "__main__":
    from src.tasks.contractor_benchmark_report import main
    raise SystemExit(main(sys.argv[1:]))
