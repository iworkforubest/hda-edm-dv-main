#!/usr/bin/env python3
"""Retrieve one Workday reference set and write it as flat rows, ready to land as a table.

WHY THIS IS SEPARATE FROM fetch_workday_references.py. That tool answers "what does the
tenant hold" -- a person runs it, reads the census, and stops. This one produces an
ARTEFACT something else loads: one line per row, stable columns, provenance on every row.
The two overlap in the call and differ entirely in what they are for, and merging them
would give one tool two audiences and a --shape flag nobody remembers.

NDJSON, NOT PARQUET, and the reason is a dependency rather than a preference. Parquet needs
pyarrow, which is not in this repo's lockfile and would be a new dependency for a landing
format nobody has agreed yet. Spark reads NDJSON natively, `head` reads it too, and a
diff on it means something. Revisit when a destination is chosen: if that destination wants
Parquet, adding pyarrow becomes a decision taken for a reason.

IT LANDS NOTHING ITSELF. Where these files go is unowned -- see the plan at
docs/superpowers/plans/2026-09-07-workday-reference-source.md, Task 3. This writes to a
path you give it and stops, which is the honest boundary until somebody owns the
destination.

THE CREDENTIAL RULES ARE NOT RE-IMPLEMENTED HERE. read_password_file lives in the other
tool and refuses a world-readable file and a file the bundle would ship; importing it means
those refusals cannot drift apart from this caller.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

import fetch_workday_references as fetch  # noqa: E402

from accelerator import workday  # noqa: E402

# Which service each kind comes from. `reference` is the index in Integrations; the other
# two are one Human_Resources retrieval read two ways -- the organisations themselves, and
# the edges between them.
KIND_SERVICE = {"reference": "Integrations",
                "organisation": "Human_Resources",
                "membership": "Human_Resources"}


def retrieve(kind: str, args, password: str) -> list[dict]:
    """Every page of one retrieval, parsed but not yet flattened."""
    url = workday.endpoint(args.host, args.tenant, args.version, KIND_SERVICE[kind])
    rows: list[dict] = []
    page, total = 1, 1
    while page <= total:
        if kind == "reference":
            body = workday.build_request(args.reference_id_type, page=page,
                                         count=args.count)
            parse = workday.parse_response
        else:
            body = workday.build_organizations_request(page=page, count=args.count)
            parse = workday.parse_organizations
        got, results = parse(
            fetch.fetch_page(url, body, args.username, password, args.timeout))
        rows.extend(got)
        total = results.get("total_pages") or 1
        print(f"  page {page}/{total}  +{len(got)}", file=sys.stderr)
        if args.max_pages and page >= args.max_pages:
            print(f"  stopping at --max-pages {args.max_pages} of {total}", file=sys.stderr)
            break
        page += 1
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--kind", required=True, choices=sorted(KIND_SERVICE),
                    help="reference | organisation | membership")
    ap.add_argument("--type", dest="reference_id_type", default=None,
                    help="required for --kind reference; the schema has it at minOccurs=1")
    ap.add_argument("--out", required=True, help="NDJSON destination")
    ap.add_argument("--host", default=fetch.DEFAULTS["host"])
    ap.add_argument("--tenant", default=fetch.DEFAULTS["tenant"])
    ap.add_argument("--version", default=fetch.DEFAULTS["version"])
    # RESOLVED AFTER PARSING, like the fetch tool: the credentials file is a source
    # argparse cannot see, and a username that ignores it drifts from the password
    # sitting on the next line of that same file the moment the tenant moves.
    ap.add_argument("--username", default=None)
    ap.add_argument("--password-file", default=None)
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--max-pages", type=int, default=0)
    ap.add_argument("--timeout", type=int, default=180)
    args = ap.parse_args()

    if args.kind == "reference" and not args.reference_id_type:
        print("--type is required for --kind reference: Reference_ID_Type is minOccurs=1 "
              "in the schema and the tenant refuses the call without it", file=sys.stderr)
        return 2

    import os
    creds = fetch.read_credentials_file(args.password_file)
    password = os.environ.get("WD_PASSWORD", "") or creds.get("WD_PASSWORD", "")
    args.username = (args.username
                     or os.environ.get("WD_USERNAME")
                     or creds.get("WD_USERNAME")
                     or fetch.DEFAULTS["username"])
    tenant_suffix = args.username.rpartition("@")[2]
    if tenant_suffix and tenant_suffix != args.tenant:
        print(f"WARNING  username {args.username} is qualified for tenant "
              f"{tenant_suffix!r} but --tenant is {args.tenant!r}. Workday authenticates "
              f"the pair, so one of these is wrong.", file=sys.stderr)
    if not password:
        print("No password. Set WD_PASSWORD or pass --password-file. See "
              "docs/workday_reference_service_access.md", file=sys.stderr)
        return 2

    if "impl-" in args.host:
        print(f"NOTE  {args.host} is an IMPLEMENTATION tenant. WIDs from it identify "
              f"nothing in production; this output must not be promoted as reference data.",
              file=sys.stderr)

    # ONE TIMESTAMP FOR THE WHOLE RETRIEVAL, taken before the first call rather than per
    # row. Every row of one retrieval is one batch, and rows stamped microseconds apart
    # would let dedup_order split a batch that arrived together.
    retrieved_at = _dt.datetime.now(_dt.timezone.utc).replace(
        microsecond=0).isoformat().replace("+00:00", "Z")

    parsed = retrieve(args.kind, args, password)
    rows = workday.landing_rows(args.kind, parsed, tenant=args.tenant, host=args.host,
                                version=args.version, retrieved_at=retrieved_at)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    print(f"wrote {len(rows)} row(s) from {len(parsed)} record(s) to {out}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
