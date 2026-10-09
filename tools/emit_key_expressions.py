#!/usr/bin/env python3
"""Render tests/golden_key_expressions.json from the model.

THE BASELINE HAS TO BE REPRODUCIBLE OR IT IS NOT EVIDENCE. The golden file records the
hash-key expression every (entity, binding, column) in the model derives, and the check in
tests/test_accelerator.py ("not one hash-key expression moved") compares the live model
against it. Until now the file was hand-rendered once and there was no way to re-render it,
so "is this file still the model, or has it drifted into a second opinion?" could only be
answered by the check itself -- which is circular when the question is whether the check's
own input is sound.

This script answers it. Run it before a change that renames or splits an entity, diff the
output against the committed file, and a zero diff proves the committed file is the model
as it stands RIGHT NOW -- which is exactly what makes it usable as a before-picture.

  python tools/emit_key_expressions.py            # print to stdout
  python tools/emit_key_expressions.py --write    # overwrite the golden file

IT DOES NOT RUN IN CI AND IS NOT BYTE-GATED, DELIBERATELY. A byte-gate on this file would
regenerate the baseline on every model change, which is the one thing a baseline must not
do: the check would then compare the model against itself and pass forever. The file moves
only when a human decides a key legitimately moved, and that decision is the review.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import hashing, spec  # noqa: E402

GOLDEN = ROOT / "tests" / "golden_key_expressions.json"


def render() -> str:
    model = spec.load_model(ROOT / "metadata" / "entities")
    rows = []
    for entity in model.entities:
        for src in entity.sources:
            for column, (components, scope) in spec.hash_key_columns(
                    model, entity, src).items():
                rows.append({
                    "entity": entity.name,
                    "kind": entity.kind,
                    "binding": src.name,
                    "column": column,
                    "expression": hashing.hash_key(components, source_scope=scope),
                })
    # SORTED, so a re-render of an unchanged model is byte-identical and the diff is the
    # review. Dict iteration order would make every run a different file.
    rows.sort(key=lambda r: (r["entity"], r["binding"], r["column"]))
    return json.dumps(
        {"rulebook_version": hashing.RULEBOOK_VERSION, "keys": rows}, indent=2) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true",
                    help="overwrite tests/golden_key_expressions.json in place")
    args = ap.parse_args()
    text = render()
    if args.write:
        GOLDEN.write_text(text, encoding="utf-8")
        print(f"wrote {GOLDEN.relative_to(ROOT)} ({len(json.loads(text)['keys'])} keys)")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
