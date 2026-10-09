"""Has every feed loaded recently enough to be trusted?

A HUMAN NOTICES A STALE NUMBER; NOTHING ELSE DOES. Every other gate in this repo asks
whether what landed is correct. None of them asks whether anything landed at all, so a
feed that quietly stopped delivering passes every one of them: the keys still join, the
history is still append-only, the reconciliation still balances, and the vault serves
yesterday's answer with no indication that it is yesterday's.

MEASURED AGAINST THE LAST SUCCESSFUL LOAD, NEVER THE LAST ROW, and that choice is the
whole gate. `aud_table_load` records a row per run per table whether or not any row landed
-- staged may be 0 and the row still exists -- so:

    a feed that legitimately delivered nothing   -> the audit row is recent -> FRESH
    a pipeline that stopped running              -> no recent audit row     -> STALE

Keying this to `max(load_dts)` on the vault table instead would measure when a row last
ARRIVED. Steady reference data would read as stale within a day, everyone would learn to
ignore the gate, and a genuinely stalled pipeline would be indistinguishable from a quiet
one. That is the failure this gate exists to catch, rebuilt inside the gate.

WHAT IT DOES NOT COVER, stated because a gate believed to cover more than it does is worse
than none. `aud_table_load` is keyed on the TABLE, and a hub fed by several bindings has
one table -- so for `hub_job_request`, fed by BULLHORN_EU and FIELDGLASS_US, this proves
the hub loaded and cannot prove that BOTH feeds delivered. One feed going quiet while the
other keeps running is NOT caught here. Satellites fan out to one table per source, so for
them the measure is exact. Closing the hub case needs a per-binding signal the audit does
not record today; it is written down in OPEN_ITEMS rather than implied by silence.

PURE DECISIONS ABOVE, SPARK IN main() ONLY -- the shape source_conformance_check.py uses,
so fabricated audit rows exercise the same decisions the live path makes.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "checks"))

from accelerator import naming, spec  # noqa: E402

# IMPORTED, NOT REDEFINED -- the same reason source_conformance_check imports it. A second
# copy of "is this catalog even there" is a second authority on the not-instrumented state.
from control_conformance_check import catalog_missing  # noqa: E402,F401

GATE = "freshness"


def sla_bindings(model, active) -> list[tuple[str, str, str, int]]:
    """(entity, binding, table, sla_hours) for every ACTIVE binding declaring a live SLA.

    Exemptions -- `freshness_sla_hours: 0` -- are dropped here rather than compared against
    zero, because a zero threshold would fail instantly and permanently. An exemption is a
    decision to not measure, not a decision to measure with an impossible bar.

    A binding with NO declaration at all is also absent, and that is deliberate: this
    function reports what CAN be measured. verify_repo separately refuses an active binding
    that declares nothing, so an omission fails the build rather than quietly narrowing the
    gate here.
    """
    out: list[tuple[str, str, str, int]] = []
    for entity in model.entities:
        for src in entity.sources:
            if not spec.active_table_bindings(entity, src, active):
                continue
            hours = src.freshness_sla_hours
            if not hours:          # None (undeclared) or 0 (exempt)
                continue
            # entity.tables() yields (binding, table), and binding is None for an entity
            # with ONE table serving every source -- a hub. That is not a quirk to code
            # around; it is the coverage limit named in the module docstring, made visible
            # here: a hub's SLA is measured on the hub's table, so it proves the hub loaded
            # and cannot prove which feed did. A satellite yields one table per binding and
            # is exact.
            for binding, table in entity.tables():
                if binding is None or binding.name == src.name:
                    out.append((entity.name, src.name, table, hours))
    return sorted(out)


def stale(last_loaded: dict, wanted: list, now: datetime) -> tuple[list, list]:
    """(problems, not_evaluated) for the declared SLAs against observed load times.

    A TABLE WITH NO AUDIT ROW AT ALL IS NOT_EVALUATED, NOT A BREACH, and the distinction
    matters on a lake where a binding is active but has never run. Reporting it as stale
    would make the gate red on every fresh deployment and train people to skip it; leaving
    it silent would hide a feed that has never once delivered. It is reported, counted, and
    kept out of the pass/fail decision -- the same three-state shape every other gate here
    uses.
    """
    problems, not_evaluated = [], []
    for entity, binding, table, hours in wanted:
        seen = last_loaded.get(table)
        if seen is None:
            not_evaluated.append(
                f"{table} ({entity}/{binding}): no row in aud_table_load -- this binding is "
                f"active but has never recorded a load, so there is nothing to measure"
            )
            continue
        age = now - seen
        if age > timedelta(hours=hours):
            problems.append(
                f"{table} ({entity}/{binding}): last loaded {seen.isoformat()}, "
                f"{age.total_seconds() / 3600:.1f}h ago, SLA is {hours}h -- the feed has "
                f"stopped or the pipeline has, and every downstream answer is that old"
            )
    return problems, not_evaluated


def gate_status(problems: list, not_evaluated: list) -> str:
    if problems:
        return "FAILED"
    return "PASSED" if not not_evaluated else "PASSED_WITH_GAPS"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument(
        "--active-sources", default="",
        help="the target's active_sources value; empty means every binding is active",
    )
    ap.add_argument("--table-prefix", default="")
    args = ap.parse_args()

    from pyspark.sql import SparkSession  # noqa: PLC0415

    spark = SparkSession.builder.getOrCreate()

    model = spec.load_model(ROOT / "metadata" / "entities")
    active = frozenset(
        x.strip() for x in args.active_sources.split(",") if x.strip()
    ) or None
    wanted = sla_bindings(model, active)
    if not wanted:
        print("no active binding declares a live freshness SLA")
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        return 0

    audit = f"`{args.catalog}`.`{args.control_schema}`.{args.table_prefix}aud_table_load"
    try:
        rows = spark.sql(
            f"SELECT table_name, max(recorded_at) AS last_loaded FROM {audit} "
            f"GROUP BY table_name"
        ).collect()
    except Exception as exc:  # noqa: BLE001
        # FAIL, NOT SKIP. An unreadable audit table is the same evidential state as a
        # stalled pipeline -- we cannot say anything landed -- and a gate that skips when
        # its evidence is missing is a gate that passes when it matters most.
        print(f"cannot read {audit}: {type(exc).__name__}: {exc}")
        print(f"GATE SUMMARY :: {GATE} :: status=FAILED asserted=0 not_evaluated=0")
        return 1

    last_loaded = {r["table_name"]: r["last_loaded"] for r in rows if r["last_loaded"]}
    now = datetime.now(timezone.utc)
    # The audit's timestamps are naive UTC; compare like with like rather than letting
    # Python raise on a naive/aware subtraction at the moment a feed goes stale.
    last_loaded = {
        t: (v if v.tzinfo else v.replace(tzinfo=timezone.utc)) for t, v in last_loaded.items()
    }
    problems, not_evaluated = stale(last_loaded, wanted, now)

    for p in problems:
        print(f"  STALE  {p}")
    for n in not_evaluated:
        print(f"  ----   {n}")
    if not problems and not not_evaluated:
        print(f"every one of {len(wanted)} feed(s) loaded within its SLA")

    print(f"GATE SUMMARY :: {GATE} :: status={gate_status(problems, not_evaluated)} "
          f"asserted={len(wanted) - len(not_evaluated)} not_evaluated={len(not_evaluated)}")
    return 1 if problems else 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for exit code 0. Exit
    # explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
