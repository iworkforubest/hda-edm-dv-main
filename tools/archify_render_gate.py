#!/usr/bin/env python3
"""Ask Archify itself whether the committed documents can be drawn.

WHY THIS IS A SEPARATE, HARDER GATE THAN THE SCHEMA ONE. The vendored JSON Schemas do not
describe the renderer's geometry. `dataflow.schema.json` bounds a node's `row` at
`minimum: 0` with no maximum; the dataflow renderer has exactly five rows per stage.
Nothing in `architecture.schema.json` says a component whose label is wider than its box
is a hard error. So a document can satisfy every schema this repo vendors and still be
undrawable -- which is not a hypothesis. On 6 September, when the renderer was installed
for the first time, both committed documents failed it: seven of twenty-nine nodes had
been emitted at rows 5..12 and produced non-finite coordinates. They had passed the
schema gate on every run since they were added.

WHY IT IS NOT IN checks/. Everything in checks/ is exec()'d by a serverless
spark_python_task with no `__file__` in globals (DEF-12). This needs paths and a
subprocess, and it has nothing to do with a load. It is repo tooling, so it lives here.

WHY IT SKIPS RATHER THAN FAILS WHEN THE RENDERER IS ABSENT, AND WHAT THAT COSTS. Archify
is a globally-installed agent skill, not a dependency this repo can pin or vendor -- the
renderer is 7MB of Node across five renderers, and vendoring it would mean maintaining a
fork. CI has neither Node nor the skill, so this gate cannot run there and the schema gate
is all CI gets. That is a real hole and it is stated rather than papered over: a machine
with the skill installed catches a geometry regression, and CI does not. See the entry in
docs/superpowers/OPEN_ITEMS.md.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# WHERE THE SKILL LANDS, in the order the agent hosts install it. ARCHIFY_HOME wins so a
# CI job or a container can point at an unpacked copy without any of this guessing.
_CANDIDATES = (
    Path(os.environ["ARCHIFY_HOME"]) if os.environ.get("ARCHIFY_HOME") else None,
    Path.home() / ".claude" / "skills" / "archify",
    ROOT / ".claude" / "skills" / "archify",
    Path.home() / ".agents" / "skills" / "archify",
    Path.home() / ".config" / "opencode" / "skills" / "archify",
)


def renderer() -> Path | None:
    """The installed Archify CLI, or None -- in which case this gate does not run."""
    for base in _CANDIDATES:
        if base is not None and (base / "bin" / "archify.mjs").is_file():
            return base / "bin" / "archify.mjs"
    return None


def renderer_version() -> str | None:
    """The installed skill's version, e.g. "2.17.0-dev.1", or None if it cannot be read.

    READ, NOT ASSUMED. The committed HTML in diagram/html/ is byte-gated against a fresh
    render, and `deliver` output is only deterministic for a FIXED renderer version -- a
    different Archify would fail that gate on every run for a reason that has nothing to
    do with this repo. So the byte-gate asks the version first and stands down when it
    does not match what the emitter was written against.
    """
    cli = renderer()
    if cli is None:
        return None
    marker = cli.parent.parent / "skill-release.json"
    if not marker.is_file():
        return None
    try:
        return str(json.loads(marker.read_text("utf-8"))["version"])
    except (json.JSONDecodeError, KeyError):
        return None


def deliver(document: Path, diagram_type: str, out: Path) -> str | None:
    """Render one document to `out`. Returns an error string, or None on success."""
    cli = renderer()
    assert cli is not None, "callers check unavailable() first"
    proc = subprocess.run(
        ["node", str(cli), "deliver", diagram_type, str(document), str(out)],
        capture_output=True, text=True, cwd=str(cli.parent.parent), timeout=300)
    if proc.returncode:
        return (proc.stderr or proc.stdout)[:300]
    return None


def unavailable() -> str | None:
    """Why this gate cannot run, or None when it can."""
    if shutil.which("node") is None:
        return "node is not on PATH"
    if renderer() is None:
        return ("the Archify skill is not installed -- `npx -y skills add tt-a1i/archify "
                "--skill archify --agent claude-code --global --copy --yes`, or set "
                "ARCHIFY_HOME")
    return None


def check_document(path: Path, diagram_type: str) -> list[str]:
    """Archify's own findings for one document. Empty means it renders."""
    cli = renderer()
    assert cli is not None, "callers check unavailable() first"
    proc = subprocess.run(
        ["node", str(cli), "validate", diagram_type, str(path), "--json"],
        capture_output=True, text=True, cwd=str(cli.parent.parent), timeout=120)
    try:
        result = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return [f"the renderer produced no JSON receipt (exit {proc.returncode}): "
                f"{(proc.stderr or proc.stdout)[:300]}"]
    if result.get("ok"):
        return []
    # The receipt carries both a rendered `error` blob and structured `diagnostics`; the
    # blob is the one a person can act on, so it is what gets reported.
    return [line.strip().lstrip("- ")
            for line in str(result.get("error", "")).splitlines()
            if line.strip().startswith(("- ", "/"))] or [str(result.get("error", ""))[:300]]


def documents() -> list[tuple[str, Path]]:
    """(diagram type, path) for every committed Archify document, read off the emitter."""
    sys.path.insert(0, str(ROOT / "tools"))
    import emit_archify                                        # noqa: PLC0415
    return [(kind, ROOT / rel) for kind, rel, _ in emit_archify.documents()]


def main() -> int:
    reason = unavailable()
    if reason:
        print(f"SKIP  the Archify render gate did not run: {reason}")
        return 0
    failed = 0
    for kind, path in documents():
        findings = check_document(path, kind)
        if findings:
            failed += 1
            print(f"FAIL  {path.relative_to(ROOT)}")
            for f in findings[:6]:
                print(f"        {f[:180]}")
        else:
            print(f"PASS  {path.relative_to(ROOT)} renders ({kind})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
