#!/usr/bin/env python3
"""Render the committed Archify documents to HTML, and capture a PNG of each for GitHub.

WHY A PNG EXISTS AT ALL. GitHub serves a committed `.html` as a source blob, never as a
page -- so the rendered diagrams, which are the whole point of the documents, were
invisible to anyone reading the repo in a browser. GitHub Pages is not the answer here:
this repository is private, and Pages on a private repository publishes publicly unless
the account is on Enterprise. A PNG in the tree renders inline in a README and leaks
nothing.

WHAT IS COMMITTED AND WHAT IS NOT.

  * `diagram/html/` is GIT-IGNORED. Each page inlines the whole viewer runtime -- roughly
    700KB apiece -- and CI can neither regenerate nor gate it.
  * `diagram/img/` IS committed, at roughly 90KB apiece. It is the only form of these
    diagrams a person can see without installing Node and the skill.

AND THE IMAGES ARE NOT GATED FOR CONTENT, which is stated here rather than left to be
discovered. verify_repo asserts that there is exactly one image per document, so a domain
that appears or disappears is caught. It cannot assert that an image MATCHES its document,
because regenerating one needs a browser. An image is therefore the one artefact in this
repo that can silently go stale -- the same standing caveat `diagram/hfig_data_vault.
dbdiagram` carries. Re-run this tool whenever the model changes.

THE CAPTURE IS FULL-PAGE, NOT A VIEWPORT SCREENSHOT. `archify visual-check` writes PNGs
too, but they are clipped to the viewport, so the twelve-lane workflow came out at Stage 4.
The height here is measured from the delivered page and the window is sized to it.

DARK, BECAUSE THE VIEWER IS. The page resolves its own theme and defaults to dark;
Chrome's `--force-prefers-color-scheme=light` does not override it (measured 6 Sep -- the
corner pixel stays (2, 6, 23)). A dark card reads correctly on either GitHub theme, so
this is left alone rather than fought.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import archify_render_gate as gate  # noqa: E402

HTML_DIR = ROOT / "diagram" / "html"
IMG_DIR = ROOT / "diagram" / "img"
CAPTURE_WIDTH = 1920          # matches a viewport visual-check already measures
CHROME_NAMES = ("google-chrome-stable", "google-chrome", "chromium", "chromium-browser")


def chrome() -> str | None:
    for name in CHROME_NAMES:
        found = shutil.which(name)
        if found:
            return found
    return None


def _page_height(cli: Path, page: Path) -> int:
    """The delivered page's own scroll height at CAPTURE_WIDTH, from Archify's receipt."""
    proc = subprocess.run(["node", str(cli), "visual-check", str(page), "--json"],
                          capture_output=True, text=True, cwd=str(cli.parent.parent),
                          timeout=300)
    try:
        receipt = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return 0
    heights = [v.get("scrollHeight", 0) for v in receipt["containment"]["viewports"]
               if v.get("width") == CAPTURE_WIDTH]
    # visual-check drops screenshot and receipt sidecars beside the page; they are its
    # working notes, not artefacts of ours.
    for junk in page.parent.glob(f"{page.stem}.visual-check.*"):
        junk.unlink()
    return max(heights or [0])


def main() -> int:
    reason = gate.unavailable()
    if reason:
        print(f"cannot render: {reason}")
        return 1
    browser = chrome()
    cli = gate.renderer()
    assert cli is not None

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    for kind, path in gate.documents():
        page = HTML_DIR / f"{path.stem.replace('.archify', '')}.html"
        proc = subprocess.run(["node", str(cli), "deliver", kind, str(path), str(page)],
                              capture_output=True, text=True, cwd=str(cli.parent.parent),
                              timeout=300)
        if proc.returncode:
            print(f"FAIL  deliver {path.name}: {(proc.stderr or proc.stdout)[:300]}")
            return 1
        print(f"wrote {page.relative_to(ROOT)}")

        if browser is None:
            continue
        height = _page_height(cli, page) or 2400
        image = IMG_DIR / f"{page.stem}.png"
        subprocess.run([browser, "--headless", "--disable-gpu", "--hide-scrollbars",
                        f"--screenshot={image}",
                        f"--window-size={CAPTURE_WIDTH},{height}",
                        page.as_uri()], capture_output=True, timeout=300)
        print(f"wrote {image.relative_to(ROOT)} ({CAPTURE_WIDTH}x{height})")

    if browser is None:
        print("no Chrome on PATH -- pages rendered, images not captured")
    return 0


if __name__ == "__main__":
    sys.exit(main())
