"""Build the FAQ section for an SEO page from the page's own data.

Data-driven (ADR 0023 revised): the Q&A is generated from the same distress
stats shown on the page, so it is always on-topic for the city×vertical, always
accurate, and needs no external content. Returns plain-text answers (no markdown)
so the visible FAQ and the FAQPage JSON-LD render byte-identically from one source.
"""
from __future__ import annotations

_VERTICAL_LABELS = {
    "wholesalers": "wholesalers",
    "fix_flip": "fix-and-flip investors",
    "restoration": "restoration contractors",
    "roofing": "roofing contractors",
    "public_adjusters": "public adjusters",
    "attorneys": "real-estate attorneys",
}


def _label(vertical: str) -> str:
    return _VERTICAL_LABELS.get(vertical, vertical.replace("_", " "))


def build_faq(city: str, vertical: str, stats: dict) -> list[dict]:
    """Return a list of {"question", "answer"} plain-text pairs for the page.

    Each answer is derived from `stats` (the same numbers shown above the FAQ),
    so nothing here can drift from the visible page or go off-topic.
    """
    label = _label(vertical)
    count = stats.get("qualified_count", 0)
    absentee = stats.get("absentee_count", 0)
    median_value = stats.get("median_value") or 0
    high_priority = stats.get("ultra_platinum_count", 0) + stats.get("platinum_count", 0)
    pct = stats.get("city_vs_county_pct", 0)

    items: list[dict] = [
        {
            "question": f"How many distressed properties are available in {city}, FL for {label}?",
            "answer": (
                f"There are currently {count:,} distressed properties in {city}, FL "
                f"that qualify for {label}. "
                f"Of these, {absentee:,} are owned by absentee landlords, a common "
                f"indicator of a motivated seller."
            ),
        },
    ]

    if median_value:
        items.append({
            "question": f"What is the typical value of a distressed property in {city}, FL?",
            "answer": (
                f"Across the {count:,} qualifying properties in {city}, the median "
                f"estimated market value is ${median_value:,.0f}."
            ),
        })

    items.append({
        "question": f"How many high-priority leads are in {city}, FL?",
        "answer": (
            f"{high_priority:,} properties in {city} are rated Platinum tier or higher, "
            f"meaning they carry the strongest combination of distress signals for {label}."
        ),
    })

    items.append({
        "question": f"How does {city} compare to the rest of the county?",
        "answer": (
            f"{city} accounts for {pct}% of all qualifying {label} leads across the county."
        ),
    })

    return items
