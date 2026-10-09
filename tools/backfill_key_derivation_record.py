"""Attach the derivation map to a ctl_key_derivation record that predates it.

WHY THIS IS AN IDENTIFICATION AND NOT A SEED. The gate's whole job is to refuse a lake
whose stored keys were built a different way from the model. The obvious way out of an
UNMAPPED verdict -- record today's digest and move on -- asserts exactly the thing in
question, and is the habit that turns a hard gate into a formality.

This does not assert anything. The record already stores a digest, and that digest is the
sha256 of some committed metadata/key_derivation.json. So the map is not chosen, it is
FOUND: every commit that touched that file is hashed, and the one whose bytes hash to the
recorded digest IS the map that produced it. If no commit matches, there is no map to
attach and this refuses -- which is itself a finding, because it means the record was
written from an uncommitted file.

The attachment is checkable by anyone afterwards, without trusting this script or the
person who ran it: re-hash the stored derivation_json and compare it to the
derivation_sha256 stored beside it. The gate does exactly that on every read
(verdict -> CORRUPT_RECORD), so a hand-edited map cannot pose as evidence.

FILED IN GIT, NOT WRITTEN TO THE RECORD TABLE. ctl_key_derivation carries
delta.appendOnly and is owned by the service principal the job runs as, so completing a row
means either a permission the operator may not hold (measured 25 September:
PERMISSION_DENIED on MODIFY) or an append that reads as a new assertion about the lake. The
archive is better evidence anyway: it sits in version control, it is diffable, and its
provenance is a commit rather than a row someone wrote. The gate re-hashes what it finds
there before believing it, so a misfiled or edited map is ignored rather than trusted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

REL = "metadata/key_derivation.json"
ARCHIVE = ROOT / "metadata" / "key_derivation_archive"


def commits_touching(rel: str = REL) -> list[str]:
    out = subprocess.run(["git", "-C", str(ROOT), "log", "--format=%H", "--", rel],
                         capture_output=True, text=True, check=True)
    return [line for line in out.stdout.split("\n") if line]


def blob_at(commit: str, rel: str = REL) -> str | None:
    got = subprocess.run(["git", "-C", str(ROOT), "show", f"{commit}:{rel}"],
                         capture_output=True, text=True)
    return got.stdout if got.returncode == 0 else None


def find_map_for(digest: str, rel: str = REL) -> tuple[str, str] | None:
    """(commit, text) whose bytes hash to `digest`, or None.

    Hashed as BYTES exactly as key_derivation_guard.current_digest does, so a match here is
    a match there. The working tree is tried first: the common case is a record written
    from the file as it stands.
    """
    candidates = [("<working tree>", (ROOT / rel).read_text(encoding="utf-8"))]
    candidates += [(c, t) for c in commits_touching(rel) if (t := blob_at(c, rel)) is not None]
    for where, text in candidates:
        if hashlib.sha256(text.encode("utf-8")).hexdigest() == digest:
            return where, text
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--digest", required=True,
                    help="the derivation_sha256 the record carries. Read it with: SELECT "
                         "derivation_sha256 FROM <catalog>.<control>.ctl_key_derivation "
                         "ORDER BY recorded_at DESC LIMIT 1")
    ap.add_argument("--dry-run", action="store_true",
                    help="find and report the map without filing it")
    args = ap.parse_args()

    digest = args.digest.strip().lower()
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        print(f"  * NOT A SHA-256: {args.digest!r}. Pass the full 64-character digest the "
              f"record stores, not a prefix -- a prefix would make the match approximate, "
              f"and an approximate match is a guess.")
        return 1

    existing = ARCHIVE.glob(f"{digest[:16]}*.json") if ARCHIVE.is_dir() else []
    for path in existing:
        if hashlib.sha256(path.read_text(encoding="utf-8").encode("utf-8")).hexdigest() == digest:
            print(f"  already on file: {path.relative_to(ROOT)}")
            return 0

    found = find_map_for(digest)
    if found is None:
        print(f"\n  * NO COMMITTED metadata/key_derivation.json HASHES TO {digest[:16]}…\n"
              f"    Every commit touching that file was checked, and the working tree. The\n"
              f"    record was therefore written from a file that was never committed, so\n"
              f"    there is no map to identify and none to file. Nothing written.")
        return 1

    where, text = found
    n_keys = len(json.loads(text)["keys"])
    print(f"  FOUND at {where}: {n_keys} key(s), hashing to {digest[:16]}… exactly")
    if args.dry_run:
        print("  --dry-run: nothing written")
        return 0

    ARCHIVE.mkdir(parents=True, exist_ok=True)
    out = ARCHIVE / f"{digest[:16]}.json"
    out.write_text(text, encoding="utf-8")
    print(f"  filed {out.relative_to(ROOT)} ({n_keys} keys)\n"
          f"  It asserts nothing about the lake. The gate re-hashes it and uses it only if "
          f"it still equals the recorded digest, then compares key by key -- so a re-key, "
          f"if there was one, still fails.\n"
          f"  Commit it: the evidence is the commit, not this run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
