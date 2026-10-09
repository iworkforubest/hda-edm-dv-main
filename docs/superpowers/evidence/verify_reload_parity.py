"""Compare the vault after a drop-and-reload against the state captured before it.

The question a reload has to answer is not "did rows come back" -- it is "are these
the SAME rows, under the same keys". Row counts cannot answer that: two different
hashing rulebooks produce identical counts. So the comparison is over the hash keys
themselves, using the fingerprints recorded before the drop.

The aggregates reproduce the pre-drop capture exactly, and each is order-independent
so it cannot be fooled by a different physical layout:

    n_rows           count(*)
    n_distinct       count(distinct hk)
    sum_all          sum(crc32(hex(hk)))
    md5sum_all       sum(conv(substr(md5(hex(hk)), 1, 15), 16, 10))
    md5sum_distinct  the same over the DISTINCT key set
    min_hk / max_hk  min(hex(hk)) / max(hex(hk))

COLUMN COUNTS ARE EXPECTED TO DIFFER and are reported, never asserted: narrowing the
tables to their declared shape is the whole point of the reload. Everything else must
match to the digit.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

AGG = """
SELECT count(*) AS n_rows,
       count(DISTINCT {c}) AS n_distinct,
       sum(crc32(hex({c}))) AS sum_all,
       cast(sum(cast(conv(substr(md5(hex({c})), 1, 15), 16, 10) AS decimal(38,0))) AS string) AS md5sum_all,
       min(hex({c})) AS min_hk,
       max(hex({c})) AS max_hk
FROM {cat}.{tbl}
"""

AGG_DISTINCT = """
SELECT cast(sum(cast(conv(substr(md5(hex({c})), 1, 15), 16, 10) AS decimal(38,0))) AS string) AS md5sum_distinct
FROM (SELECT DISTINCT {c} FROM {cat}.{tbl})
"""

FIELDS = ("n_rows", "n_distinct", "sum_all", "md5sum_all", "min_hk", "max_hk",
          "md5sum_distinct")


def query(sql: str, profile: str) -> dict:
    out = subprocess.run(
        ["databricks", "experimental", "aitools", "tools", "query", sql,
         "--profile", profile, "-o", "json"],
        capture_output=True, text=True, check=True,
    ).stdout
    rows = json.loads(out)
    if isinstance(rows, dict):
        rows = rows.get("rows") or rows.get("data") or [rows]
    return {k: (str(v) if v is not None else None) for k, v in rows[0].items()}


def capture(tables, catalog: str, profile: str) -> dict:
    """Record the fingerprints a later reload will be compared against.

    Written as a MODE OF THE COMPARER, not a separate script, so the capture and the
    comparison cannot drift into computing different aggregates -- which would make
    every future reload unverifiable in exactly the way that is hardest to notice.
    """
    out = {"hk": {}, "counts": {}}
    for table in tables:
        full = f"{catalog}.{table}"
        n = query(f"SELECT count(*) AS n FROM {full}", profile)["n"]
        out["counts"][table] = n
        cols = query(
            f"SELECT concat_ws(',', collect_list(column_name)) AS c FROM "
            f"{catalog}.information_schema.columns WHERE "
            f"table_schema='{table.split('.')[0]}' AND table_name='{table.split('.')[1]}' "
            f"AND right(column_name, 3) = '_hk'", profile)["c"]
        for column in [c for c in (cols or "").split(",") if c]:
            g = query(AGG.format(c=column, cat=catalog, tbl=table), profile)
            g |= query(AGG_DISTINCT.format(c=column, cat=catalog, tbl=table), profile)
            out["hk"][f"{table.split('.')[1]}.{column}"] = g
            print(f"  captured {table}.{column}  {g['n_rows']} rows")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", metavar="OUT",
                    help="write a fresh before-state to OUT instead of comparing")
    ap.add_argument("--tables", default=None,
                    help="comma-separated schema.table list, for --capture")
    ap.add_argument("--before", required=False)
    ap.add_argument("--catalog", default="02_usnc_silver_edm_dev")
    ap.add_argument("--profile", required=True)
    args = ap.parse_args()

    if args.capture:
        tables = [t.strip() for t in (args.tables or "").split(",") if t.strip()]
        if not tables:
            print("--capture needs --tables"); return 1
        state = capture(tables, args.catalog, args.profile)
        with open(args.capture, "w") as fh:
            json.dump(state, fh, indent=1, sort_keys=True)
        print(f"\ncaptured {len(state['hk'])} hash-key column(s) over "
              f"{len(state['counts'])} table(s) -> {args.capture}")
        return 0

    if not args.before:
        print("need --before (or --capture)"); return 1
    before = json.loads(open(args.before).read())
    mismatches, checked, missing = [], 0, []

    print(f"reload parity :: catalog={args.catalog}\n")
    for key, want in sorted(before["hk"].items()):
        table, column = key.rsplit(".", 1)
        table = table if "." in table else f"raw_vault.{table}"
        try:
            got = query(AGG.format(c=column, cat=args.catalog, tbl=table), args.profile)
            got |= query(AGG_DISTINCT.format(c=column, cat=args.catalog, tbl=table),
                         args.profile)
        except subprocess.CalledProcessError as exc:
            missing.append(f"{key}: {exc.stderr.strip().splitlines()[-1:]}")
            print(f"  ERROR {key}")
            continue

        bad = [f for f in FIELDS if str(want.get(f)) != str(got.get(f))]
        checked += 1
        if bad:
            mismatches.append((key, {f: (want.get(f), got.get(f)) for f in bad}))
            print(f"  FAIL  {key}")
            for f in bad:
                print(f"          {f}: before={want.get(f)} after={got.get(f)}")
        else:
            print(f"  PASS  {key}  {want['n_rows']} rows / "
                  f"{want['n_distinct']} distinct, digests identical")

    # A capture made by --capture records counts and fingerprints, not column widths.
    # Absent is a normal state, not an error: skip the section rather than dying after
    # every hash has already been compared, which is what a bare KeyError did here.
    print("\ncolumn counts -- REPORTED, not asserted (narrowing is the point):")
    if "cols" not in before:
        print("  (not recorded in this capture)")
    for key, was in sorted(before.get("cols", {}).items()):
        if "__materialization" in key or not key.startswith("raw_vault."):
            continue
        schema, name = key.split(".", 1)
        try:
            now = query(f"SELECT count(*) AS n FROM {args.catalog}."
                        f"information_schema.columns WHERE "
                        f"table_schema='{schema}' AND table_name='{name}'",
                        args.profile)["n"]
        except subprocess.CalledProcessError:
            now = "?"
        arrow = "->" if str(now) != str(was) else "=="
        print(f"  {name:42} {str(was):>4} {arrow} {str(now):>4}")

    print("\n" + "=" * 66)
    if missing:
        print(f"INCOMPLETE: {len(missing)} table(s) could not be read:")
        for m in missing:
            print(f"  {m}")
    if mismatches:
        print(f"RELOAD PARITY FAILED: {len(mismatches)} of {checked} key(s) differ")
        return 1
    if not checked:
        print("RELOAD PARITY VACUOUS: nothing was compared")
        return 1
    print(f"RELOAD PARITY PASSED: {checked} hash-key column(s) byte-identical "
          f"to the pre-drop capture")
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
