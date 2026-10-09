"""Does Bronze still provide what Silver requires of it?

source_contracts/<target>.yaml states what Silver reads from each Bronze table. This checks
whether the lake still provides it -- before a pipeline run finds out. The contract spec named
this as where the value actually lands, and left it to a separate spec deliberately.

THE COVERAGE IS UNEVEN AND SAYS SO. Measured on great_plains_raw.gl20000: the contract names 19
required columns and a type for FOUR of them, because Bronze's own column types are not modelled
in this repository -- a type appears only where Silver declares a `cast`. So existence is
asserted for every required column, castability only for the columns carrying a declared cast,
and the actual Bronze type of the rest is REPORTED and never asserted.

PURE DECISIONS, SPARK IN main() ONLY -- the shape checks/control_conformance_check.py uses, so a
fabricated information_schema row and a fabricated cast count exercise the same decisions the
live path makes.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
from datetime import datetime, timezone
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "checks"))

# IMPORTED, NOT REDEFINED. catalog_missing exists because control_conformance_check.main() once
# CRASHED on a nonexistent catalog instead of reporting the not-instrumented state its own
# docstring promised. A second copy here would be the duplicate-authority trap this repo has been
# bitten by three times, and tests/test_accelerator.py asserts this module does not define one --
# AND that main() actually calls it on the catalog-absent path, not merely imports it unused.
from control_conformance_check import catalog_missing  # noqa: E402,F401

GATE = "source_conformance"


def missing_columns(required: set, deployed: set) -> list:
    """Required columns the lake does not have. Sorted.

    EXTRA columns in Bronze are NOT a finding: the contract states what Silver reads, never what
    Bronze may not hold, so a one-directional comparison is the correct one here.
    """
    return sorted(set(required) - set(deployed))


def required_columns(requires: dict) -> set:
    """Every column the contract requires this table to provide.

    Every LIST-valued role -- business_keys, dedup_by, parent_keys, payload, applied_dts,
    transaction_key, cdc_op, manifest -- contributes its columns. required_casts is a DICT
    (column -> list of required types), not a list, so it needs its own term: a cast target must
    exist before castability can even be measured, and `set(requires.get("payload") or [])` alone
    would drop it along with every other non-payload role, quietly shrinking 19 required columns
    for gl20000 down to whatever `payload` alone names.
    """
    required = {c for role, v in requires.items() if isinstance(v, list) for c in v}
    required |= set((requires.get("required_casts") or {}))
    return required


def cast_probes(requires: dict) -> list:
    """[(column, required type)] to measure, one per required type.

    required_casts maps a column to a sorted LIST of types, per Ruling T2-B: two bindings casting
    one column to two types is a conjunction of requirements, so Bronze must supply a column
    castable to both. Every list holds exactly one type as of 28 Aug 2026, which is why a
    per-column implementation would pass and then silently skip the second type the day one
    appears.
    """
    out = []
    for column, types in sorted((requires.get("required_casts") or {}).items()):
        for required_type in sorted(types):
            out.append((column, required_type))
    return out


def probe_sql(table: str, col: str, required_type: str) -> str:
    """The SQL that counts rows a cast to `required_type` would destroy.

    `col IS NOT NULL` is load-bearing: try_cast(...) IS NULL is ALSO true for a legitimate NULL,
    which is not a lossy cast -- only a NOT NULL value that try_cast turns into NULL is a row this
    cast would destroy. Removing the null guard would count every null row as a would-be loss and
    redden a correct build.
    """
    return (f"SELECT COUNT(*) n FROM {table} "
            f"WHERE `{col}` IS NOT NULL "
            f"AND try_cast(`{col}` AS {required_type}) IS NULL")


FINDINGS: tuple = ("ABSENT", "MISSING COLUMN", "LOSSY CAST")


def finding(kind: str, detail: str) -> str:
    """One problem line, labelled by kind.

    THREE KINDS, KEPT DISTINCT. An ABSENT table and a wrong-columns table are different problems
    with different owners; control_conformance_check learned that collapsing them sends the wrong
    person to look. A kind outside FINDINGS raises rather than becoming a silent fourth class.
    """
    if kind not in FINDINGS:
        raise ValueError(f"unknown finding kind {kind!r}; expected one of {FINDINGS}")
    return f"{kind}: {detail}"


def gate_status(problems: list, not_evaluated: list) -> str:
    """FAILED / NOT_EVALUATED / PASSED, from those two inputs and nothing else.

    A SKIPPED PROBE IS NOT A PASS. --skip-cast-probes leaves the castability half unmeasured, and
    a green result that concealed that would be the gate-goes-quiet failure this repo keeps
    finding. A real problem still outranks a skip.

    Pure, and deliberately blind to everything else main() prints -- which is what makes the
    REPORTED-not-asserted block unable to change the exit status.
    """
    if problems:
        return "FAILED"
    if not_evaluated:
        return "NOT_EVALUATED"
    return "PASSED"


def lossy_casts(rows) -> list:
    """One LOSSY CAST finding per (table, column, type) whose cast would destroy rows.

    `rows` is already collected -- dicts shaped {"table","column","type","lossy"} -- so a
    fabricated list exercises the same decision the live count does. Routed through finding()
    like the other two kinds, so a run producing all three never leaves one unlabelled.
    """
    problems = []
    for r in rows:
        if r["lossy"]:
            problems.append(finding(
                "LOSSY CAST",
                f"{r['table']}.{r['column']}: try_cast to {r['type']} destroys "
                f"{r['lossy']} row(s) that are not null today. Silver casts this column, so "
                f"those rows reach the vault as NULL"
            ))
    return problems


# THE FOUR THINGS A MEASURED TABLE CAN BE. Declared, so a typo becomes an error rather than a
# silent fifth class that every dashboard filter then misses.
STATUSES: tuple = ("CONFORMANT", "ABSENT", "NON_CONFORMANT", "NOT_EVALUATED")

# SPELLED OUT, NOT INFERRED. createDataFrame over a list of dicts infers the schema from the
# first row, so an all-clean run -- every findings array empty -- infers ARRAY<NULL> and the
# append fails against the declared ARRAY<STRING>. That is precisely the run this table is most
# likely to see first, and the failure would arrive on the day everything was fine.
_RECORD_FIELDS: tuple = (
    ("job_run_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("target", "STRING"),
    ("contract_table", "STRING"), ("status", "STRING"), ("missing_columns", "BIGINT"),
    ("lossy_casts", "BIGINT"), ("findings", "ARRAY<STRING>"),
)
RECORD_SCHEMA: str = ", ".join(f"{n} {t}" for n, t in _RECORD_FIELDS)


def table_status(findings: list, unmeasured: int) -> str:
    """What one contracted table was found to be.

    A REAL PROBLEM OUTRANKS A SKIP, the same precedence gate_status() applies to the run as a
    whole: a table with both findings and unmeasured probes is NON_CONFORMANT, because the
    findings are true regardless of what the skipped probes would have said.

    ABSENT OUTRANKS EVERYTHING. main() stops at the first ABSENT for a table and reports no
    columns, so a row claiming NON_CONFORMANT would imply column-level findings that were never
    collected.

    CONFORMANT REQUIRES unmeasured == 0. A table whose cast probes were skipped has not been
    shown to conform -- recording it as conformant is how a dashboard comes to report safety
    nobody measured.
    """
    if any(f.startswith("ABSENT: ") for f in findings):
        return "ABSENT"
    if findings:
        return "NON_CONFORMANT"
    if unmeasured:
        return "NOT_EVALUATED"
    return "CONFORMANT"


def conformance_rows(per_table: dict, target: str, job_run_id: str, recorded_at) -> list:
    """One row per contracted table, for ctl_source_conformance.

    THE COUNTS ARE DERIVED FROM THE FINDINGS, not carried alongside them. A row whose
    missing_columns disagreed with its own findings array would make the dashboard's summary
    and its detail contradict each other, and there would be no way to tell which was right.
    Counting by the prefix finding() writes keeps one authority for both.

    EVERY CONTRACTED TABLE GETS A ROW, including the clean ones. A table that only appears when
    it fails cannot be counted, so coverage -- how much of Bronze is measured at all -- would
    have no denominator, and an unmeasured estate would be indistinguishable from a clean one.
    """
    rows = []
    for table in sorted(per_table):
        findings = list(per_table[table].get("findings") or [])
        unmeasured = int(per_table[table].get("unmeasured") or 0)
        rows.append({
            "job_run_id": job_run_id,
            "recorded_at": recorded_at,
            "target": target,
            "contract_table": table,
            "status": table_status(findings, unmeasured),
            "missing_columns": sum(1 for f in findings if f.startswith("MISSING COLUMN: ")),
            "lossy_casts": sum(1 for f in findings if f.startswith("LOSSY CAST: ")),
            "findings": findings,
        })
    return rows


def report_lines(tables: dict, observed: dict) -> list:
    """Lines reporting the observed Bronze type of every required column the contract does not
    type -- the REPORTED, not asserted half described in the module docstring.

    Pure: `observed` is already-collected {(table_key, column): type}, the same shape main()
    builds from information_schema. Extracted so this is asserted directly rather than carried
    by a comment substring: §8 requires the report to be provably PRESENT, not merely provably
    harmless, and a report a comment alone stands for can vanish while the comment survives.
    """
    lines = []
    for table in sorted(tables):
        key = table.lower()
        requires = tables[table].get("requires") or {}
        typed = {c for c, _t in cast_probes(requires)}
        required = required_columns(requires)
        for col in sorted(required - typed):
            lines.append(f"  {table}.{col}: {observed.get((key, col), 'not present')}")
    return lines


def contract_catalog(tables) -> str:
    """The single Bronze catalog every table in this contract block belongs to.

    Contract table names are already fully qualified as "<catalog>.<schema>.<table>", and the
    source-contract branch already gates that every table a target's contract names sits under
    that target's own bronze_catalog -- exactly one, never a mix. So there is no `--catalog` flag
    to mistype here: the catalog is READ OFF the contract itself, which is what eliminates the
    old failure mode (an operator passing the prod catalog `01_usnc_bronze` instead of
    `01_usnc_bronze_dev`, or any other catalog, silently making every `deployed` key
    f"{catalog}.{schema}.{table}" fail to match) rather than merely detecting it after the fact.

    If a contract ever named tables under more than one catalog, that upstream gate has failed,
    and this must FAIL LOUDLY rather than silently picking one and quietly dropping every table
    under the catalog not picked.
    """
    catalogs = {t.split(".")[0] for t in tables if t.count(".") >= 2}
    if len(catalogs) > 1:
        raise ValueError(
            f"contract names tables under more than one catalog: {sorted(catalogs)} -- exactly "
            f"one is required; the source-contract branch's own bronze_catalog gate should have "
            f"caught this before it was committed")
    if not catalogs:
        raise ValueError("contract names no fully qualified bronze table; cannot derive a "
                          "catalog to query")
    return next(iter(catalogs))


def contracts() -> dict:
    """{target: parsed contract} for every committed source contract.

    GLOBS THE DIRECTORY; it deliberately does NOT import emit_source_contract to reuse its path
    helpers. Measured 29 Aug: that import chain is emit_source_contract -> emit_data_contract ->
    accelerator.factory -> pyspark.pipelines, and importing pyspark.pipelines outside a DLT
    pipeline trips Databricks' import hook and kills the task with
    `Py4JJavaError ... NoSuchElementException: None.get`, before any of this file's logic runs.

    Globbing is EQUIVALENT, and the equivalence is gated rather than assumed: the source-contract
    branch asserts "a contract exists for every target declaring active_sources, and for no
    other", so the files on disk ARE the configured targets. Relying on that invariant is what
    decouples a job task from pipeline code -- reusing the shared function is what coupled them.
    """
    import yaml  # noqa: PLC0415 -- kept local; the pure half must import nothing heavy

    out = {}
    for path in sorted((ROOT / "source_contracts").glob("*.yaml")):
        out[path.stem] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--skip-cast-probes", action="store_true",
                    help="report the cast columns as not_evaluated instead of measuring them; "
                         "for a caller without SELECT on the _raw tables")
    # RECORDING IS OPT-IN BY ARGUMENT, NOT BY GUESS. The gate runs by hand as well as in the
    # job, and a hand run should not append a row that a dashboard will read as a scheduled
    # measurement. The job passes all three.
    ap.add_argument("--record-catalog", help="catalog holding the control schema to record into")
    ap.add_argument("--record-schema", help="control schema to record into")
    ap.add_argument("--job-run-id", default="", help="the run id to stamp on recorded rows")
    args = ap.parse_args()

    # NO CONTRACT, NO SPARK. Checked before the session is even started: a target with no
    # committed contract has nothing to conform to, and this path must be reachable without a
    # workspace so it is testable offline (Ruling T3-A) -- starting a Spark session to discover
    # there is nothing to do would be wrong even setting testability aside.
    all_contracts = contracts()
    if args.target not in all_contracts:
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        print(f"{args.target}: no committed source contract. not instrumented -- a target that "
              f"has not declared its Bronze has nothing to conform to.")
        return 0

    tables = all_contracts[args.target].get("bronze_tables") or {}

    # NO --catalog FLAG: THE CONTRACT NAMES ITS OWN CATALOG, so there is nothing for an operator
    # to mistype. contract_catalog() reads it off the contract and FAILS LOUDLY, before Spark is
    # even imported, in the one case where that would be unsafe -- a contract naming tables under
    # more than one catalog, which the source-contract branch's own gate should already prevent.
    try:
        catalog = contract_catalog(tables)
    except ValueError as exc:
        print(f"GATE SUMMARY :: {GATE} :: status=FAILED asserted=0 not_evaluated=0")
        print(f"{args.target}: {exc}")
        return 1

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # COLLECT FIRST, THEN DECIDE. information_schema carries every column of every table in the
    # catalog; one query, then the pure functions. full_data_type, NOT data_type: the latter
    # gives "decimal" with precision and scale stripped, and precision/scale are exactly the raw
    # material §2 says the report exists to provide (checks/conformance_check.py selects
    # full_data_type for the identical reason).
    try:
        rows = spark.sql(
            f"SELECT lower(table_schema) s, lower(table_name) t, lower(column_name) c, "
            f"lower(full_data_type) d "
            f"FROM `{catalog}`.information_schema.columns").collect()
    except Exception as exc:  # noqa: BLE001 -- narrowed immediately by catalog_missing()
        if not catalog_missing(exc):
            raise
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        print(f"{catalog} does not exist. not instrumented -- this is the expected state "
              f"until the owning team stands the lake up.")
        return 0

    # KEYED LOWERCASE ON THE TABLE SIDE. The contract states a table name as the model declares
    # it; information_schema is queried lowered, so the table key is normalised on both sides
    # before comparison -- an earlier draft compared `deployed` case-insensitively while
    # `observed` was not, so the REPORTED block silently printed "not present" for every column.
    # Contract-side COLUMN names are not separately normalised here: they are lowercase already in
    # every committed contract, so this is dormant rather than covering a real mixed-case source
    # today.
    deployed: dict = {}
    observed: dict = {}
    for r in rows:
        key = f"{catalog}.{r['s']}.{r['t']}".lower()
        deployed.setdefault(key, set()).add(r["c"])
        observed[(key, r["c"])] = r["d"]

    # VACUITY GUARD: A CALLER WITHOUT SELECT ON THE _raw TABLES gets zero rows back for every
    # schema the contract names -- the catalog itself resolves fine, so catalog_missing() above
    # never fires, and without this guard every table would be reported ABSENT, which is false.
    # Distinguished from a genuine absence by checking whether ANY row landed in ANY schema the
    # contract actually references; a real per-table gap still reports ABSENT normally.
    contract_schemas = {t.split(".")[1] for t in tables if t.count(".") >= 2}
    observed_schemas = {r["s"] for r in rows}
    schemas_unreachable = bool(contract_schemas) and not (contract_schemas & observed_schemas)

    # SPEC §6: VERIFY try_cast EXISTS, DO NOT ASSUME IT. A silently-unsupported function would
    # turn every cast measurement into a false zero -- the worst outcome available here, because
    # it reports safety it never measured. One cheap probe, before any real one.
    cast_probes_usable = not args.skip_cast_probes
    try_cast_unavailable = False
    if cast_probes_usable:
        try:
            spark.sql("SELECT try_cast('x' AS INT) AS probe").collect()
        except Exception as exc:  # noqa: BLE001
            cast_probes_usable = False
            try_cast_unavailable = True
            print(f"try_cast is unavailable in this runtime ({type(exc).__name__}), so "
                  f"castability was NOT measured: {exc}")

    problems: list = []
    not_evaluated: list = []
    probe_rows: list = []
    # PER-TABLE, ALONGSIDE the flat lists rather than replacing them. problems/not_evaluated
    # decide the exit status and their shape is asserted elsewhere; this is the same
    # information keyed by table, which is the grain ctl_source_conformance records at.
    per_table: dict = {t: {"findings": [], "unmeasured": 0} for t in tables}

    for table in sorted(tables):
        requires = tables[table].get("requires") or {}
        required = required_columns(requires)
        key = table.lower()
        if key not in deployed:
            if schemas_unreachable:
                # NOT_EVALUATED, NOT ABSENT: nothing came back for this table's schema at all,
                # which is what "no SELECT on the _raw tables" looks like from here -- reporting
                # it as a missing table would be a false, and misleading, positive.
                schema = table.split(".")[1] if table.count(".") >= 2 else "?"
                not_evaluated.append(
                    f"{table}: 0 rows visible in information_schema for schema {schema!r} -- "
                    f"not evaluated, likely missing SELECT on the _raw tables rather than an "
                    f"absent table")
                per_table[table]["unmeasured"] += 1
                continue
            # ABSENT ends this table. Reporting its columns as missing too would bury one real
            # problem under nineteen derived ones.
            _absent = finding("ABSENT", f"{table} is named in the contract and does not exist")
            problems.append(_absent)
            per_table[table]["findings"].append(_absent)
            continue
        have = deployed[key]
        for col in missing_columns(required, have):
            _missing = finding("MISSING COLUMN", f"{table}.{col}")
            problems.append(_missing)
            per_table[table]["findings"].append(_missing)

        for col, required_type in cast_probes(requires):
            if col not in have:
                continue  # already reported as MISSING COLUMN
            if not cast_probes_usable:
                # NAME THE ACTUAL CAUSE. --skip-cast-probes and "try_cast turned out to be
                # unavailable" are different operator realities -- one chosen, one discovered --
                # and reporting the wrong one tells the operator to look in the wrong place.
                cause = ("try_cast is unavailable in this runtime" if try_cast_unavailable
                         else "--skip-cast-probes was passed")
                not_evaluated.append(f"{table}.{col} -> {required_type} ({cause})")
                per_table[table]["unmeasured"] += 1
                continue
            try:
                lossy = spark.sql(
                    probe_sql(table, col, required_type)).collect()[0]["n"]
            except Exception as exc:  # noqa: BLE001 -- reported, mirroring catalog_missing()
                reason = f"{type(exc).__name__}: {exc}"
                print(f"  {table}.{col} -> {required_type}: cast probe failed, not evaluated "
                      f"({reason})")
                not_evaluated.append(f"{table}.{col} -> {required_type} (probe failed: {reason})")
                per_table[table]["unmeasured"] += 1
                continue
            probe_rows.append({"table": table, "column": col,
                               "type": required_type, "lossy": lossy})

    # ATTRIBUTED BY RE-DERIVING PER TABLE, not by parsing the finding text back apart. Each
    # probe row already knows its table, so one call per table gives the same findings
    # lossy_casts() produces for the whole set -- and the assertion below proves the two agree
    # rather than trusting that they do.
    _lossy_all = lossy_casts(probe_rows)
    for table in per_table:
        for f in lossy_casts([r for r in probe_rows if r["table"] == table]):
            per_table[table]["findings"].append(f)
    _attributed = sum(len([f for f in v["findings"] if f.startswith("LOSSY CAST: ")])
                      for v in per_table.values())
    if _attributed != len(_lossy_all):
        raise AssertionError(
            f"attributed {_attributed} LOSSY CAST finding(s) per table but lossy_casts() "
            f"produced {len(_lossy_all)} over the same probe rows -- a probe row whose table "
            f"is not a contracted table would silently drop its finding from the record")
    problems += _lossy_all

    # REPORTED, NOT ASSERTED. The contract states a type only where Silver declares a cast, so
    # every other required column's Bronze type is raw material for a future decision and nothing
    # more. Printed unconditionally, and it cannot change the exit status -- `problems` is never
    # appended to from here, and gate_status() never reads this print, so this block cannot make
    # the gate PASS or FAIL either.
    print(f"REPORTED (not asserted) -- observed Bronze types for columns the contract does not "
          f"type:")
    for line in report_lines(tables, observed):
        print(line)

    for line in problems:
        print(f"  {line}")
    if not_evaluated:
        print(f"  NOT EVALUATED ({len(not_evaluated)} item(s)):")
        for line in not_evaluated:
            print(f"    {line}")

    # RECORD BEFORE THE SUMMARY, so a write that fails cannot be mistaken for a run that had
    # nothing to say. The verdict itself is decided above and is not touched here.
    if args.record_catalog and args.record_schema:
        rows_to_write = conformance_rows(
            per_table, args.target, args.job_run_id,
            datetime.now(timezone.utc).replace(tzinfo=None))
        target_table = f"`{args.record_catalog}`.`{args.record_schema}`.ctl_source_conformance"
        # A FAILED WRITE IS FATAL, and deliberately so. The whole point of recording is that a
        # dashboard reads it; one that silently stopped being written would keep rendering the
        # last run's numbers as though they were current, which is the green-over-nothing
        # failure this record exists to prevent. Louder than the gate's own verdict is correct
        # here -- a conformant run whose record was lost is not a run anybody can cite.
        try:
            spark.createDataFrame(rows_to_write, schema=RECORD_SCHEMA) \
                 .write.format("delta").mode("append").saveAsTable(
                     f"{args.record_catalog}.{args.record_schema}.ctl_source_conformance")
        except Exception as exc:  # noqa: BLE001 -- re-raised; never swallowed
            print(f"RECORDING FAILED -- could not append {len(rows_to_write)} row(s) to "
                  f"{target_table}: {type(exc).__name__}: {exc}")
            raise
        print(f"recorded {len(rows_to_write)} conformance row(s) to {target_table}")
    elif args.record_catalog or args.record_schema:
        # HALF A DESTINATION IS AN OPERATOR ERROR, not a reason to skip quietly. Skipping would
        # look identical to a run that was never asked to record.
        print(f"GATE SUMMARY :: {GATE} :: status=FAILED asserted=0 not_evaluated=0")
        print("--record-catalog and --record-schema must be given together; got only one.")
        return 1

    print(f"GATE SUMMARY :: {GATE} :: status={gate_status(problems, not_evaluated)} "
          f"asserted={len(tables)} "
          f"not_evaluated={len(not_evaluated)}")
    return 1 if problems else 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for exit code 0. Exit explicitly
    # only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
