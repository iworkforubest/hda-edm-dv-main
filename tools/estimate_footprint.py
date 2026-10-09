#!/usr/bin/env python3
"""
Storage and compute footprint estimator.

WHAT THIS IS, AND WHAT IT IS NOT
--------------------------------
It reads the REAL table structure from metadata/entities/*.yml -- column counts, key
widths, how many satellites each entity fans out to, which structures are insert-only
versus immutable -- and combines that with volumes declared in metadata/volumes.yml.

The structure half is accurate. The volume half is entirely your inputs. Every number in
volumes.yml is a placeholder chosen to make the arithmetic legible, not an estimate from
anyone who knows your business. The point of the output is to show WHICH INPUTS THE ANSWER
IS SENSITIVE TO, so the conversation moves from "how big will it be" to "we need three
numbers and here is how much each one matters".

    python tools/estimate_footprint.py
    python tools/estimate_footprint.py --pit-grain month_end   # test a lever
    python tools/estimate_footprint.py --region uks

HOW TO REPLACE THE GUESSES WITH FACTS
Load one domain into weu_tds and read the actuals out of the metrics vault:
compression achieved, bytes per row per table, DBU per load. Then put those in
volumes.yml. A measured single-domain pilot beats any estimate, including this one.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import naming, spec  # noqa: E402

SNAPSHOTS_PER_YEAR = {"daily": 365, "weekly": 52, "month_end": 12}
GB = 1024 ** 3


def human(bytes_: float) -> str:
    if bytes_ >= 1024 ** 4:
        return f"{bytes_ / 1024 ** 4:,.2f} TB"
    return f"{bytes_ / GB:,.1f} GB"


def row_bytes(entity: spec.Entity, cfg: dict) -> int:
    """Uncompressed bytes per stored row, from the entity's real shape."""
    a = cfg["assumptions"]
    keys = 1 + len(entity.parents)          # own key plus one FK per parent
    if entity.kind in naming.SATELLITE_KINDS:
        keys = 1                            # satellites carry only the parent key
    width = keys * a["hash_key_bytes"] + a["system_column_bytes"]
    width += len(entity.payload) * a["avg_payload_column_bytes"]
    if entity.kind in naming.SATELLITE_KINDS:
        width += a["hash_key_bytes"]        # hashdiff
    if entity.kind == "msat":
        width += 32                         # mas_key
    return width


def stored_rows_per_day(entity: spec.Entity, vol: dict) -> float:
    """Insert-only means satellites grow with CHANGES; NHLs store every delivered row."""
    if entity.kind == "hub":
        return vol["rows_per_day"] * 0.02 if vol["population"] else vol["rows_per_day"]
    if entity.kind in naming.LINK_KINDS:
        return vol["rows_per_day"]
    return vol["rows_per_day"] * vol["change_rate"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--volumes", default=str(ROOT / "metadata" / "volumes.yml"))
    ap.add_argument("--pit-grain", default=None, choices=list(SNAPSHOTS_PER_YEAR))
    ap.add_argument("--region", default=None, help="scale to one lake's share")
    ap.add_argument("--top", type=int, default=8)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.volumes).read_text(encoding="utf-8"))
    a = cfg["assumptions"]
    if args.pit_grain:
        cfg["pit"]["grain"] = args.pit_grain
    share = cfg["regions"].get(args.region, 1.0) if args.region else 1.0

    model = spec.load_model(ROOT / "metadata" / "entities")
    horizons = a["horizon_years"]

    print("=" * 78)
    print("FOOTPRINT ESTIMATE -- structure is real, VOLUMES ARE PLACEHOLDERS")
    print("=" * 78)
    scope = f"region={args.region} (share {share:.0%})" if args.region else "all regions combined"
    print(f"scope: {scope}   PIT grain: {cfg['pit']['grain']}   "
          f"compression assumed: {a['compression_ratio']}x\n")

    rows: list[tuple[str, float, dict[int, float]]] = []
    missing: list[str] = []

    for e in model.entities:
        vol = cfg["entities"].get(e.name)
        if vol is None:
            missing.append(e.name)
            continue
        per_day = stored_rows_per_day(e, vol) * share
        width = row_bytes(e, cfg)
        n_tables = len(e.tables())
        # fan-out splits the SAME source rows across tables; it does not duplicate them.
        # The cost of splitting is the repeated key+system columns per table, not the data.
        split_overhead = 1.0 + (n_tables - 1) * 0.08 if n_tables > 1 else 1.0
        by_year = {}
        for years in horizons:
            raw = per_day * 365 * years * width * split_overhead
            by_year[years] = raw / a["compression_ratio"] * (1 + a["time_travel_overhead_pct"] / 100)
        rows.append((f"{e.base_table} ({n_tables} table{'s' if n_tables > 1 else ''})",
                     per_day, by_year))

    # PIT: rows = population x snapshot dates. The single biggest lever.
    pit_rows = []
    snaps = SNAPSHOTS_PER_YEAR[cfg["pit"]["grain"]]
    pit_width = a["hash_key_bytes"] + 8 * cfg["pit"]["satellites_per_pit"] + 16
    for name in cfg["pit"]["entities"]:
        vol = cfg["entities"].get(name)
        if not vol:
            continue
        pop = vol["population"] * share
        by_year = {}
        for years in horizons:
            raw = pop * snaps * years * pit_width
            by_year[years] = raw / a["compression_ratio"]
        pit_rows.append((f"pit_{name}  [{cfg['pit']['grain']}]", pop * snaps / 365, by_year))

    all_rows = rows + pit_rows
    all_rows.sort(key=lambda r: r[2][horizons[-1]], reverse=True)

    hdr = f"{'table':<52}{'rows/day':>12}" + "".join(f"{f'yr {y}':>12}" for y in horizons)
    print(hdr)
    print("-" * len(hdr))
    for name, per_day, by_year in all_rows[: args.top]:
        line = f"{name:<52}{per_day:>12,.0f}"
        line += "".join(f"{human(by_year[y]):>12}" for y in horizons)
        print(line)
    if len(all_rows) > args.top:
        rest = {y: sum(r[2][y] for r in all_rows[args.top:]) for y in horizons}
        line = f"{f'... {len(all_rows) - args.top} smaller tables':<52}{'':>12}"
        line += "".join(f"{human(rest[y]):>12}" for y in horizons)
        print(line)

    print("-" * len(hdr))
    totals = {y: sum(r[2][y] for r in all_rows) for y in horizons}
    line = f"{'TOTAL (Silver vault)':<52}{'':>12}"
    line += "".join(f"{human(totals[y]):>12}" for y in horizons)
    print(line)

    # --- the levers, quantified ------------------------------------------------
    print("\n" + "=" * 78)
    print("WHAT THE ANSWER IS SENSITIVE TO")
    print("=" * 78)

    last = horizons[-1]
    pit_total = sum(r[2][last] for r in pit_rows)
    print(f"\nPIT grain -- currently {cfg['pit']['grain']}: {human(pit_total)} of the "
          f"{human(totals[last])} total at year {last}")
    for grain, n in SNAPSHOTS_PER_YEAR.items():
        scaled = pit_total * n / snaps
        print(f"    {grain:<10} {human(scaled):>12}"
              f"   ({scaled / max(pit_total, 1):.2f}x current)")
    print("    The single biggest storage lever, and it is a governed decision in the CTL")
    print("    calendar rather than a technical one.")

    print(f"\nCompression -- assumed {a['compression_ratio']}x. This is the widest unknown:")
    for ratio in (2.0, 4.0, 8.0):
        print(f"    {ratio}x -> {human(totals[last] * a['compression_ratio'] / ratio)}")
    print("    MEASURE this in the pilot. It moves the total by ~4x across the range.")

    top_share = all_rows[0][2][last] / max(totals[last], 1)
    print(f"\nConcentration -- {all_rows[0][0].split(' (')[0]} is {top_share:.0%} of the "
          f"year-{last} total.")
    print("    Sizing effort belongs on the top two or three tables. The many small")
    print("    satellites the per-source split produces are not the cost driver.")

    print("\nAll 8 workspaces:")
    prod = sum(cfg["regions"].values())
    print(f"    production lakes carry {prod:.0%} of the totals above, distributed "
          f"{', '.join(f'{k} {v:.0%}' for k, v in cfg['regions'].items())}")
    print(f"    TDS carries a synthetic subset at {cfg['tds_fraction_of_prod']:.0%} of prod, "
          f"not a copy")

    if missing:
        print(f"\nNO VOLUMES DECLARED for {len(missing)} entities: {', '.join(missing)}")
        print("    These are excluded from the totals -- add them to volumes.yml.")

    print("\n" + "=" * 78)
    print("BEFORE QUOTING ANY OF THIS")
    print("=" * 78)
    print("""
  * Structure is derived from the model and is accurate. Volumes are placeholders.
  * Compute is NOT estimated here. It depends on load frequency, flow count and cluster
    shape, and the honest way to get it is to measure one domain's DBU per load in
    weu_tds. The pipelines are serverless and triggered, so cost tracks work done rather
    than uptime.
  * Serverless compute does not draw on your Azure subscription vCPU quota. If expanding
    UK compute was slow before, confirm whether that was Azure regional SKU capacity
    rather than anything to do with Databricks -- different team, different lead time.
  * Start the Azure capacity conversation in parallel with the pilot. Lead time, not the
    number, is the constraint.
""")
    return 0


if __name__ == "__main__":
    sys.exit(main())
