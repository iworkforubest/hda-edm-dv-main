#!/usr/bin/env python3
"""Derive the reference-ID types Logan's DCDDs demand, from the DCDDs themselves.

WHY DERIVED AND NOT TYPED OUT. The customer-invoice DCDD alone carries 108
CHECKREFERENCES fields. A hand-kept list would drift from the workbook the moment a
corrected DCDD lands, and the drift would be invisible: a missing type does not fail,
it silently fails to validate.

NOT EVERY `Type Value` ON A CHECKREFERENCES ROW IS A REFERENCE TYPE. The column also
carries plain data types -- Text, Boolean, Date -- on rows that are reference-validated
for other reasons. Those are excluded by name, and the exclusion is a closed set rather
than a heuristic so that a genuinely new reference type is never dropped by a pattern
that happened to match it.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from _xlsx import load  # noqa: E402

DCDD_DIR = ROOT / "metadata" / "workday" / "dcdd"
OUT_PATH = ROOT / "metadata" / "workday" / "reference_types.json"

#: Values that appear in `Type Value` but name a DATA type, not a reference type.
NOT_REFERENCE_TYPES = frozenset({
    "Text", "Boolean", "Date", "DateTime", "Numeric", "Decimal", "Integer",
})


def reference_types(sheets: dict) -> dict[str, list[str]]:
    """DCDD sheet -> sorted reference-ID type names it validates against."""
    out: dict[str, list[str]] = {}
    for name, rows in sheets.items():
        if name in ("Summary", "Comments") or not rows:
            continue
        header = {v: k for k, v in rows[0][1].items()}
        vcol, tcol = header.get("VALIDATIONS"), header.get("Type Value")
        if not vcol or not tcol:
            continue
        found: set[str] = set()
        for _r, cells in rows[1:]:
            if "CHECKREFERENCES" not in (cells.get(vcol) or ""):
                continue
            for raw in (cells.get(tcol) or "").split(","):
                t = raw.strip()
                if t and t not in NOT_REFERENCE_TYPES:
                    found.add(t)
        out[name] = sorted(found)
    return out


def render() -> str:
    by_dcdd: dict[str, list[str]] = {}
    for path in sorted(DCDD_DIR.glob("*.xlsx")):
        for sheet, types in reference_types(load(str(path))).items():
            by_dcdd[sheet] = types
    every = sorted({t for ts in by_dcdd.values() for t in ts})
    return json.dumps({
        "generated_from": "metadata/workday/dcdd/*.xlsx -- see PROVENANCE.json",
        "types": every,
        "by_dcdd": by_dcdd,
    }, indent=2) + "\n"


if __name__ == "__main__":
    OUT_PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)}")
