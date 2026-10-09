#!/usr/bin/env python3
"""Emit every Archify document this repo publishes, from the model and the job file.

Generated, committed and gated byte-identical like every other artefact here, so the diff
is the review. See src/accelerator/archify.py for why the IR is DERIVED rather than
described, and vendor/archify/README.md for the schema copies it validates against.

ONE EMITTER, AND THAT IS A FIX. This replaced tools/emit_archify_dataflow.py and
tools/emit_archify_workflow.py, which were the same 90 lines twice, differing only in a
constant -- to the point that each carried the other's dead branch as `if "dataflow" ==
"dataflow":`. Two copies of one emitter is the drift this repo keeps finding, and it had
grown one in its own tooling.

VALIDATION HERE IS THE SHAPE CHECK, NOT THE RENDER CHECK. archify.validate reads the
vendored JSON Schemas, which do not describe the renderer's geometry -- see the module
docstring. Run tools/archify_render_gate.py, or verify_repo.py, to ask Archify itself.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

# Imported for its pyspark stub: accelerator.factory imports pyspark at module level and
# the offline CI job has none. emit_data_contract installs the same minimal stub the test
# suite does, so the deferral has one home rather than two.
import emit_data_contract as _edc_stub  # noqa: E402,F401

import yaml  # noqa: E402

from accelerator import archify, contract, spec  # noqa: E402

OUT_DIR = ROOT / "diagram"


def documents() -> list[tuple[str, str, dict]]:
    """(diagram type, path relative to the repo root, document), in emission order.

    THE DOMAIN LIST IS READ, NOT WRITTEN. archify.domains() derives it from the entities,
    so a new domain gets a document by existing -- and a domain that disappears takes its
    document with it, which the byte-gate in verify_repo notices as a stale file.
    """
    model = spec.load_model(ROOT / "metadata" / "entities")
    out: list[tuple[str, str, dict]] = []
    for domain in archify.domains(model):
        out.append(("architecture", f"diagram/hfig_{domain}.archify.json",
                    archify.architecture(model, contract, domain)))
    out.append(("workflow", "diagram/hfig_workflow.archify.json",
                archify.workflow(yaml.safe_load(
                    (ROOT / "resources" / "vault_job.yml").read_text("utf-8")))))
    return out


def render(doc: dict) -> str:
    # sort_keys=False so the field order stays the schema's reading order, and a trailing
    # newline so the file ends like every other text artefact in this repo.
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


def _strict(doc: dict, kind: str) -> list[str]:
    try:
        import jsonschema
        from referencing import Registry, Resource
    except ImportError:
        return []
    schema, common = archify.load_schema(kind)
    reg = Registry().with_resource("common.schema.json", Resource.from_contents(common))
    return [f"{e.message} at {list(e.absolute_path)[:4]}"
            for e in sorted(jsonschema.Draft202012Validator(schema, registry=reg)
                            .iter_errors(doc), key=lambda e: list(e.absolute_path))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strict", action="store_true",
                    help="also validate with jsonschema, if installed -- the complete "
                         "SCHEMA check, as against archify.validate's subset. Neither is "
                         "the render check; see tools/archify_render_gate.py")
    args = ap.parse_args()

    failed = False
    written = []
    for kind, rel, doc in documents():
        findings = archify.validate(doc, kind)
        if args.strict:
            findings += [f"JSONSCHEMA {f}" for f in _strict(doc, kind)]
        if findings:
            failed = True
            for f in findings[:10]:
                print(f"  SCHEMA  {rel}: {f}")
            print(f"{len(findings)} finding(s) against "
                  f"vendor/archify/schemas/{kind}.schema.json")
            continue
        (ROOT / rel).write_text(render(doc), encoding="utf-8")
        written.append(rel)

    if failed:
        return 1
    for rel in written:
        print(f"wrote {rel}")

    # STALE DOCUMENTS ARE REMOVED, not left to be believed. A domain that is renamed or
    # merged leaves its old document behind, and a diagram nobody regenerates is exactly
    # the artefact this whole module exists to prevent.
    keep = {ROOT / rel for _, rel, _ in documents()}
    for path in sorted(OUT_DIR.glob("hfig_*.archify.json")):
        if path not in keep:
            path.unlink()
            print(f"removed stale {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc:
        sys.exit(_rc)
