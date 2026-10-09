"""
CROSS-REGION CONFORMANCE GATE.

Run this before deploying region two, and on a schedule afterwards. It is the only
thing that turns "the four lakes are aligned" from an aspiration into a fact.

"Aligned" is decomposed into five properties, in descending order of how expensive
they are to discover late:

  1. HASH RULEBOOK IDENTICAL. Every vault table in every region carries the same
     hfig.hash.* table properties. If the rulebook diverges, the same worker resolves
     to different keys per lake and the registry can never reconcile them. This is
     unrecoverable without a full re-key, so it is checked first.

  2. TABLE SET IDENTICAL. Same objects, same names, per schema.

  3. COLUMN SET AND ORDER IDENTICAL. Order matters because hashdiff depends on
     payload order; a region whose satellite has columns in a different order is
     computing different checksums from the same data.

  4. TYPES IDENTICAL. A widened type in one region changes hash input via CAST.

  5. MASK FUNCTIONS PRESENT AND IDENTICAL. Column masks are Unity Catalog functions,
     and a UC metastore is regional -- so the "same" mask exists four times and can
     drift four ways. Compares the routine definition, not just its presence.

Usage (from CI, with a profile per region):
    python checks/conformance_check.py --regions eu,uk,de,na --schema raw_vault

Requires databricks-sdk and one configured profile per region named hfig-<region>.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
# Needed here since 5 Sep, when this module gained a route to src/ so it could read the
# vault prefixes from naming instead of keeping its own hand-typed copy.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# This module had no route to the model at all -- which is why its vault prefixes were a
# hand-typed copy rather than a read of the declaration.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from accelerator import naming as _naming  # noqa: E402

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
except ImportError:  # pragma: no cover
    print("databricks-sdk is required: pip install databricks-sdk")
    raise

# Bundle target -> (catalog, CLI profile). Mirrors databricks.yml exactly, so this
# tool compares the same things the deploy creates.
#
# It answers TWO questions with one mechanism:
#   * cross-region drift  -- weu vs uks vs usnc vs aue (same environment)
#   * environment drift   -- weu_tds vs weu (same region)
#
# The second matters as much as the first: if staging and production diverge, every
# test run in TDS is evidence about a system that does not exist in production.
TARGETS = {
    # target        silver catalog             profile
    "usnc_tds": ("02_usnc_silver_edm_dev", "hfig-usnc-tds"),
    "usnc":     ("02_usnc_silver_edm",     "hfig-usnc"),
    "weu_tds":  ("02_weu_silver_edm_dev",  "hfig-weu-tds"),
    "weu":      ("02_weu_silver_edm",      "hfig-weu"),
    "uks_tds":  ("02_uks_silver_edm_dev",  "hfig-uks-tds"),
    "uks":      ("02_uks_silver_edm",      "hfig-uks"),
    "aue_tds":  ("02_aue_silver_edm_dev",  "hfig-aue-tds"),
    "aue":      ("02_aue_silver_edm",      "hfig-aue"),
}

# Non-EEA targets. Structural comparison is metadata only and carries no personal
# data, so it is always safe. Resolving IDENTITY across these boundaries is not --
# see the residency note in databricks.yml.
NON_EEA = ("usnc_tds", "usnc", "aue_tds", "aue")

# DERIVED, NOT RETYPED. This was a second hand-typed copy of the same concept and it
# disagreed with the first: append_only_check listed 10 prefixes, this one 8, and the
# module that derives them from naming.PREFIX listed 13 and was read by neither. Three
# authorities on "what is a vault table", none of them agreeing.
#
# THE CONTENTS ARE UNCHANGED, and that is the point: the eight prefixes here were
# already right. Widening this to every generated prefix was attempted first and
# verify_repo refused it, correctly -- factory.build skips the quarantine twin for a
# target with no active binding, so regions with different active_sources legitimately
# hold different qtn_ tables and comparing them reports drift that is the model working.
# naming.CONFORMANCE_TABLE_PREFIXES derives these from GENERATABLE, so the set stays the
# same and a new modelled kind joins it without anyone remembering to.
VAULT_PREFIXES = _naming.CONFORMANCE_TABLE_PREFIXES


def query(client: WorkspaceClient, warehouse_id: str, sql: str) -> list[dict[str, Any]]:
    result = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    if not result.manifest or not result.manifest.schema or not result.result:
        return []
    names = [c.name for c in result.manifest.schema.columns or []]
    return [dict(zip(names, row)) for row in (result.result.data_array or [])]


def snapshot(target: str, schema: str, warehouse_id: str) -> dict[str, Any]:
    """Everything about one workspace's vault that must match its peers."""
    catalog, profile = TARGETS[target]
    client = WorkspaceClient(config=Config(profile=profile))

    columns = query(client, warehouse_id, f"""
        SELECT table_name, column_name, ordinal_position, full_data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
    """)
    tables = sorted({c["table_name"] for c in columns
                     if str(c["table_name"]).startswith(VAULT_PREFIXES)})

    # hash rulebook properties, straight off the tables
    props = query(client, warehouse_id, f"""
        SELECT table_name, property_key, property_value
        FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}'
    """) if False else []   # table_tags shape varies; fall back to SHOW TBLPROPERTIES
    rulebook: dict[str, dict[str, str]] = {}
    for table in tables:
        rows = query(client, warehouse_id,
                     f"SHOW TBLPROPERTIES `{catalog}`.`{schema}`.`{table}`")
        rulebook[table] = {
            r["key"]: r["value"] for r in rows if str(r["key"]).startswith("hfig.hash.")
        }

    masks = query(client, warehouse_id, f"""
        SELECT routine_name, routine_definition
        FROM {catalog}.information_schema.routines
        WHERE routine_schema = 'governance' AND routine_name LIKE 'mask_%'
        ORDER BY routine_name
    """)

    layout: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    for c in columns:
        if str(c["table_name"]).startswith(VAULT_PREFIXES):
            layout[c["table_name"]].append(
                (int(c["ordinal_position"]), c["column_name"], c["full_data_type"])
            )

    return {
        "region": target,
        "tables": tables,
        "layout": {k: sorted(v) for k, v in layout.items()},
        "rulebook": rulebook,
        "masks": {m["routine_name"]: m["routine_definition"] for m in masks},
    }


def compare(baseline: dict[str, Any], other: dict[str, Any]) -> list[str]:
    drift: list[str] = []
    b, o = baseline["region"], other["region"]

    # 1 -- hash rulebook
    for table, props in baseline["rulebook"].items():
        theirs = other["rulebook"].get(table)
        if theirs is None:
            continue
        if props != theirs:
            drift.append(
                f"HASH RULEBOOK DRIFT on {table}: {b}={props} vs {o}={theirs} "
                f"-- keys are not comparable across these regions"
            )

    # 2 -- table set
    missing = sorted(set(baseline["tables"]) - set(other["tables"]))
    extra = sorted(set(other["tables"]) - set(baseline["tables"]))
    if missing:
        drift.append(f"tables present in {b} but missing in {o}: {missing}")
    if extra:
        drift.append(f"tables present in {o} but not in {b}: {extra}")

    # 3 and 4 -- column set, order and types
    for table, cols in baseline["layout"].items():
        theirs = other["layout"].get(table)
        if theirs is None:
            continue
        if cols != theirs:
            b_names = [c[1] for c in cols]
            o_names = [c[1] for c in theirs]
            if b_names != o_names:
                drift.append(f"{table}: column order/set differs -- {b}={b_names} {o}={o_names}")
            else:
                for (_, name, btype), (_, _, otype) in zip(cols, theirs):
                    if btype != otype:
                        drift.append(f"{table}.{name}: type {btype} in {b}, {otype} in {o}")

    # 5 -- masks
    for name, definition in baseline["masks"].items():
        if name not in other["masks"]:
            drift.append(f"MASK MISSING in {o}: {name} -- sensitive columns are unprotected")
        elif other["masks"][name] != definition:
            drift.append(f"MASK DRIFT on {name}: definition differs between {b} and {o}")

    return drift


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--targets",
        default="usnc_tds,weu_tds,uks_tds,aue_tds",
        help="bundle targets to compare. Compare like with like: all TDS, or all PROD, "
             "or one region's TDS against its PROD.",
    )
    ap.add_argument("--schema", default="raw_vault")
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--baseline", default="usnc_tds", help="the target others must match")
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    regions = [r.strip() for r in args.targets.split(",") if r.strip()]
    unknown = [r for r in regions if r not in TARGETS]
    if unknown:
        print(f"unknown target(s) {unknown}. Known: {sorted(TARGETS)}")
        return 1
    if args.baseline not in regions:
        print(f"baseline {args.baseline} not in {regions}")
        return 1

    # Comparing a TDS workspace against a PROD one is legitimate and useful, but
    # mixing them silently in a four-way comparison hides which axis drifted.
    envs = {("tds" if r.endswith("_tds") else "prod") for r in regions}
    if len(envs) > 1 and len(regions) > 2:
        print(f"refusing to compare {len(regions)} targets across {sorted(envs)} at once: "
              f"drift would be ambiguous between region and environment. Compare all "
              f"TDS, or all PROD, or exactly one region's two environments.")
        return 1
    if any(r in NON_EEA for r in regions):
        print("note: comparison includes non-EEA targets. This reads metadata only "
              "(information_schema, table properties) and moves no personal data.")

    snapshots = {r: snapshot(r, args.schema, args.warehouse_id) for r in regions}
    baseline = snapshots[args.baseline]
    print(f"baseline {args.baseline}: {len(baseline['tables'])} vault tables, "
          f"{len(baseline['masks'])} mask functions")

    all_drift: dict[str, list[str]] = {}
    for region in regions:
        if region == args.baseline:
            continue
        drift = compare(baseline, snapshots[region])
        all_drift[region] = drift
        status = "ALIGNED" if not drift else f"{len(drift)} DIVERGENCE(S)"
        print(f"  {args.baseline} vs {region}: {status}")
        for d in drift:
            print(f"      * {d}")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump({"baseline": args.baseline, "drift": all_drift}, fh, indent=2)

    total = sum(len(v) for v in all_drift.values())
    if total:
        print(f"\nCONFORMANCE GATE FAILED: {total} divergence(s). Deploy is blocked.")
        return 1
    print("\nCONFORMANCE GATE PASSED: all regions structurally identical.")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
