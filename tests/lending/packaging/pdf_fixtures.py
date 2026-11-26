"""Synthetic PDFs for the packet validator tests: built byte by byte, no real borrower data."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Run:
    text: str
    size: float = 11.0
    rgb: tuple[float, float, float] = (0.0, 0.0, 0.0)
    x: float = 72.0
    y: float = 720.0


def _escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf(runs: list[Run], *, annotation: Optional[str] = None, title: Optional[str] = None,
              hidden_layer: bool = False, javascript: bool = False) -> bytes:
    lines = []
    for run in runs:
        r, g, b = run.rgb
        lines.append(f"BT /F1 {run.size:g} Tf {r:g} {g:g} {b:g} rg {run.x:g} {run.y:g} Td ({_escape(run.text)}) Tj ET")
    stream = "\n".join(lines).encode("latin-1")

    objects: list[bytes] = []
    catalog = b"<< /Type /Catalog /Pages 2 0 R"
    if hidden_layer:
        catalog += b" /OCProperties << /OCGs [7 0 R] /D << /OFF [7 0 R] >> >>"
    if javascript:
        catalog += b" /OpenAction << /S /JavaScript /JS (app.alert\\(1\\)) >>"
    objects.append(catalog + b" >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    page = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >>"
    if annotation is not None:
        page += b" /Annots [6 0 R]"
    objects.append(page + b" >>")
    objects.append(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(b"<< /Type /Annot /Subtype /Text /Rect [0 0 10 10] /Contents (" + _escape(annotation or "").encode("latin-1") + b") >>")
    objects.append(b"<< /Type /OCG /Name (Layer) >>")
    info_ref = b""
    if title is not None:
        objects.append(b"<< /Title (" + _escape(title).encode("latin-1") + b") >>")
        info_ref = f" /Info {len(objects)} 0 R".encode()

    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref_at = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R".encode() + info_ref + b" >>\n"
    out += f"startxref\n{xref_at}\n%%EOF\n".encode()
    return bytes(out)


def write_pdf(folder: Path, name: str, runs: list[Run], **kwargs) -> str:
    path = folder / name
    path.write_bytes(build_pdf(runs, **kwargs))
    return str(path)


def lines(*texts: str) -> list[Run]:
    return [Run(text, y=720 - 16 * i) for i, text in enumerate(texts)]
