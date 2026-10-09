"""Render a brief (ordered titled sections) as Slack Block Kit. The host agent decides the content."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

_SECTION_TEXT_LIMIT = 2900
_HEADER_LIMIT = 150


@dataclass(frozen=True)
class BriefSection:
    title: str
    lines: Sequence[str] = field(default_factory=tuple)
    empty_text: str = "Nothing to report."


def _section_text(section: BriefSection) -> str:
    body = "\n".join(section.lines) if section.lines else f"_{section.empty_text}_"
    text = f"*{section.title}*\n{body}"
    return text if len(text) <= _SECTION_TEXT_LIMIT else text[:_SECTION_TEXT_LIMIT] + "\n…"


def render_brief_blocks(header: str, sections: Sequence[BriefSection]) -> list[dict]:
    blocks: list[dict] = [{"type": "header", "text": {"type": "plain_text", "text": header[:_HEADER_LIMIT]}}]
    for section in sections:
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _section_text(section)}})
    return blocks
