"""Rebuild one union view per tenant-partitioned Bronze source, and record what it covered.

WHAT PROBLEM THIS SOLVES. Fieldglass delivers one table PER TENANT, not one per entity --
30 live io_distributed_jobposting_<tenant> tables measured 4 September 2026, and one more
per client onboarded. Silver binds a table. This creates the table it binds.

WHY A VIEW AT ALL, AND WHY IT WORKS. Measured on usnc_tds serverless: spark.readStream on a
union view was ACCEPTED and EXECUTED, resolving to one DeltaSource per underlying table (2
sources, 5,605 rows), with a single-table control passing beside it. So a union view is a
valid streaming source and factory._stage_full binds it unchanged -- no generator feature,
no binding per tenant.

WHY THE TENANT LIST IS DISCOVERED EVERY RUN. A committed list of 30 tables silently omits
client 31. Onboarding would drop a client out of the vault with nothing failing, which is
the unfailable-gate shape this repo has met a dozen times. Rebuilding from
information_schema means a new tenant is picked up on the next run.

AND WHY THAT DISCOVERY IS RECORDED. Automatic inclusion without a record would make the
vault's input surface change with nobody able to say when or to what. Every run writes the
tenant list it actually used into ctl_source_union, so "when did this client start
loading" has an answer, and a tenant DISAPPEARING -- a dropped table, a renamed feed -- is
visible in the same place.

ZERO MATCHES IS A FAILURE, NOT AN EMPTY VIEW. A pattern that matches nothing would create a
view over nothing, and every gate downstream would pass by measuring an empty table. That
is DEF-48 exactly, so it fails here instead.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does not define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yaml  # noqa: E402

UNIONS_PATH = Path(__file__).resolve().parents[1] / "metadata" / "source_unions.yml"

# The control table this task writes. Declared in control_standard so
# control_conformance_check and append_only_check cover it with no new code.
RECORD_TABLE = "ctl_source_union"


def load_unions(path: Path = UNIONS_PATH) -> list[dict]:
    """The declared unions. A malformed file is fatal -- see the module docstring."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    unions = raw.get("unions") or []
    if not isinstance(unions, list):
        raise ValueError(f"{path.name}: `unions` must be a list, got {type(unions).__name__}")
    return unions


def matching_tables(rows: list[tuple[str, str]], pattern: str, exclude: str = "") -> list[str]:
    """Table names matching `pattern` and not `exclude`, sorted.

    `rows` is [(table_name, table_schema)] as read from information_schema, so the SQL
    filtering happens in one place and this stays a pure function the tests can drive.

    SQL LIKE semantics, implemented here rather than delegated, because the exclusion has to
    be applied to the SAME list the view is built from. An earlier design filtered
    `exclude_pattern` in the query and the coverage assertion then compared against a
    differently-filtered list -- so the assertion could pass while the view was short.
    """
    def like(name: str, pat: str) -> bool:
        # Only % is used by the declarations, and supporting only what is used keeps this
        # honest: a declaration using _ or [] would silently mean something else.
        import re
        return re.fullmatch(re.escape(pat).replace("%", ".*"), name) is not None

    out = [t for t, _s in rows if like(t, pattern)]
    if exclude:
        out = [t for t in out if not like(t, exclude)]
    return sorted(set(out))


#: The keys that only mean something for a profile read out of BRONZE.
_BRONZE_ONLY_KEYS = ("source_schema", "table_pattern", "exclude_pattern", "view_schema",
                     "tenant_column", "required_columns")


def is_vault_sourced(union: dict) -> bool:
    """Is this profile's input the VAULT rather than Bronze?

    WHY THE DISTINCTION EXISTS AT ALL. metadata/source_unions.yml began as a list of
    Bronze unions and became the home for every conform profile, including inline
    single-table ones. DEF-58 adds the first whose input is not Bronze: a business-vault
    satellite (csat_invoice_line_gie) derives its payload from nhl_invoice_line, in our
    own silver catalog, and needs derived_columns -- which only a profile can carry.

    WHAT GOES WRONG WITHOUT THE FLAG, and neither failure is visible offline. main()
    reads union["source_schema"] unconditionally, so an unflagged vault profile is a
    KeyError that kills the task. Declaring a bronze-shaped source_schema instead is
    worse: it matches zero tables in the bronze catalog, coverage_findings emits
    NO_TABLES, main() returns 1, and vault_job's ALL_SUCCESS dependency takes raw_vault
    down with the live job-posting load -- the exact landmine the invoice union's
    deferral existed for.

    EXPLICIT, NOT INFERRED FROM A MISSING KEY. A typo in `source_schema` would make a
    Bronze profile look vault-sourced and skip its coverage gate silently, which is the
    unfailable shape this repo keeps meeting. declaration_findings() below asserts both
    directions, so neither a flagged profile carrying Bronze keys nor an unflagged one
    missing them can pass.
    """
    return bool(union.get("vault_sourced"))


def declaration_findings(union: dict) -> list[str]:
    """Everything wrong with the DECLARATION itself, before the lake is consulted.

    Both directions, because one alone is a half-check: a flagged profile that still
    carries `source_schema` is ambiguous about where it reads from, and an unflagged one
    without it is the KeyError.
    """
    findings: list[str] = []
    name = union.get("name", "?")
    if is_vault_sourced(union):
        stray = sorted(k for k in _BRONZE_ONLY_KEYS if union.get(k))
        if stray:
            findings.append(
                f"VAULT_PROFILE_WITH_BRONZE_KEYS: {name} declares vault_sourced but also "
                f"{stray}. Those keys are only meaningful against the Bronze catalog, and "
                f"a profile that claims both is one nobody can say the input of"
            )
    elif not union.get("source_schema"):
        findings.append(
            f"NO_SOURCE_SCHEMA: {name} declares no source_schema and is not marked "
            f"vault_sourced. This task reads that key unconditionally, so the profile "
            f"is a KeyError in the workspace and nothing offline would see it"
        )
    return findings


def coverage_findings(union: dict, tables: list[str], columns: dict[str, set]) -> list[str]:
    """Everything wrong with this union, in the gate's own words.

    Checked BEFORE the view is created, so a broken declaration never reaches a CREATE.
    """
    findings: list[str] = []
    name = union.get("name", "?")

    if not tables:
        findings.append(
            f"NO_TABLES: {name} matched no table for pattern "
            f"{union.get('table_pattern')!r} in {union.get('source_schema')!r}. A view over "
            f"nothing makes every downstream gate pass by measuring an empty table"
        )
        return findings

    required = list(union.get("required_columns") or ())
    tenant = union.get("tenant_column")
    if tenant and tenant not in required:
        required.append(tenant)

    for col in required:
        missing = sorted(t for t in tables if col not in columns.get(t, set()))
        if missing:
            findings.append(
                f"MISSING_COLUMN: {name} requires {col!r} but {len(missing)} table(s) lack "
                f"it ({missing[:4]}). The view would be unkeyable, and a stream of "
                f"unjoinable rows loads perfectly"
            )
    return findings


def type_findings(union: dict, types: dict[str, dict[str, str]]) -> list[str]:
    """Columns that carry more than one type across the unioned tables.

    NOT FATAL, AND DELIBERATELY SO. `UNION ALL BY NAME` resolves a common type, so the view
    still builds -- but a column that is STRING in one tenant and DOUBLE in another means a
    value rendered two ways, which reaches the hashdiff and makes one tenant's rows churn.
    The declarations exclude the tables where this happens (BRZ-14: every DOUBLE in this
    estate is in a _backfill table), so this should report NOTHING. It exists to say so
    out loud, and to fire the day that stops being true.
    """
    per_column: dict[str, set] = {}
    for _table, cols in types.items():
        for col, typ in cols.items():
            per_column.setdefault(col, set()).add(typ)
    return [f"MIXED_TYPE: {col} is {sorted(ts)} across the unioned tables"
            for col, ts in sorted(per_column.items()) if len(ts) > 1]


def consumes(src, view_fqn_suffix: str, profile_name: str) -> bool:
    """Does this binding read the profile -- through its view, or inline?

    TWO WAYS SINCE 25 September. A profile spanning many tables is still a view and its
    consumers name it in bronze_table; a single-table profile is applied in the flow and its
    consumers name it in `conform`. Anything deriving from "who reads this profile" has to
    ask both, or it silently sees no consumers and concludes the profile has none.
    """
    return ((src.bronze_table or "").endswith(view_fqn_suffix)
            or getattr(src, "conform", "") == profile_name)


def columns_needed(model, view_fqn_suffix: str, union: dict) -> set:
    """Every SOURCE column any consumer of this union view actually reads.

    WHY THE VIEW IS NARROWED. It emitted every column of every matched table: 182 for the
    invoice union, of which the model reads 34. Each column is an expression in the view
    definition, the view is read by six bindings, and every read re-analyses it -- on the
    DRIVER, at pipeline definition time, before a row moves.

    MEASURED 25 September. With the raw vault split by domain, five domains loaded and
    pay_bill alone sat at 50-52% driver GC for 35 minutes across three attempts and never
    got past INITIALIZING. pay_bill is the only domain that reads this view.

    DERIVED FROM THE BINDINGS, so it cannot fall behind them: a binding that starts reading
    a new column brings it into the view by saying so. Every field a binding can name a
    source column in is enumerated here, and a column this misses does NOT corrupt anything
    -- the view simply lacks it and the load fails by name, loudly, at definition time.

    Derived columns are NOT included and do not need to be: a derived expression may
    reference any column of the FROM table whether or not the SELECT projects it, which is
    what lets line_sibling_ordinal partition by eleven columns the view no longer emits.
    """
    keep = set(union.get("required_columns") or ())
    if union.get("tenant_column"):
        keep.add(union["tenant_column"])
    for entity in model.entities:
        for src in entity.sources:
            if not consumes(src, view_fqn_suffix, union.get("name", "")):
                continue
            keep |= set(src.key_columns or ())
            keep |= set(src.payload or entity.payload or ())
            keep |= set(src.dedup_by or ())
            keep |= set(src.dedup_order or ())
            keep |= {c for _p, cols in src.parent_keys for c in cols}
            keep |= {c for c, _t in src.cast}
            keep |= set(entity.transaction_key or ())
            keep |= set(getattr(entity, "mas_key", ()) or ())
            for attr in ("applied_dts_column", "cdc_op_column", "manifest_column"):
                col = getattr(src, attr, None)
                if col:
                    keep.add(col)
    return keep


def view_sql(union: dict, catalog: str, schema: str, source_catalog: str,
             tables: list[str], types: dict[str, dict[str, str]] | None = None,
             keep: set | frozenset | None = None) -> str:
    """The CREATE OR REPLACE VIEW statement, with an EXPLICIT column list per table.

    NOT `UNION ALL BY NAME`, and that is not a style choice. This used to emit BY NAME --
    the correct idiom, and the one Spark's DataFrame API expresses as unionByName -- and
    Databricks rejected it outright:

        [PARSE_SYNTAX_ERROR] Syntax error at or near 'BY'. SQLSTATE: 42601

    SQL-level `UNION ... BY NAME` is not available on this runtime. Worse, the offline test
    asserted the generated string CONTAINED "UNION ALL BY NAME", so it verified this
    module's own text rather than whether any engine would accept it -- the text passed and
    the workspace failed.

    So every branch selects the SAME column list in the SAME order, and a table that lacks
    a column contributes a typed NULL for it. That is what BY NAME would have done, written
    out. It is more verbose and strictly better as an artefact: the view's column contract
    is visible in its own definition instead of resolved implicitly at parse time, and
    reading the view tells you which tenants are missing which columns.

    ALL, never DISTINCT: de-duplication is the loader's job and its rules are declared per
    binding. A DISTINCT here would collapse rows the vault is meant to see.
    """
    types = types or {}
    src = f"`{source_catalog}`.`{union['source_schema']}`"

    # The union's column list: every column any table contributes, in a stable order so the
    # view definition is deterministic and its diff is readable.
    every: dict[str, str] = {}
    for table in tables:
        for col, typ in (types.get(table) or {}).items():
            every.setdefault(col, typ)

    # NARROWED TO WHAT THE MODEL READS. `keep` is matched case-insensitively because that is
    # how Spark resolves identifiers and how the source spells things differently from the
    # metadata -- `Buyer_Code` against `buyer_code`. None means emit everything, which keeps
    # a caller that has no model (the type and coverage checks) working unchanged.
    if keep is not None:
        wanted = {k.lower() for k in keep}
        every = {c: t for c, t in every.items() if c.lower() in wanted}
    columns = sorted(every)

    # TWO OPTIONAL CLAUSES, BOTH DECLARED IN metadata/source_unions.yml.
    #
    # `row_filter` and `derived_columns` exist because a SOURCE BINDING CANNOT EXPRESS
    # EITHER. A binding names a table, its key columns and its payload -- there is no
    # `where`, and no way to compute a column. Adding both to spec.py and factory.py would
    # change every source in the estate to serve one feed; a view is where this repo
    # already puts per-source conformance.
    #
    # They are SQL from configuration, like `table_pattern` above. That is deliberate and
    # bounded: this file is read from the repository, not from user input, and the same
    # trust already applies to the pattern that decides which tables are unioned at all.
    # A DERIVED ALIAS MUST NOT COLLIDE WITH A SOURCE COLUMN, AND SPARK DECIDES THAT
    # CASE-INSENSITIVELY. Refused here, by name, rather than by Databricks later: the view
    # is built from every source column PLUS the derived ones, so an alias that matches an
    # existing column in any casing emits it twice and the CREATE fails with
    # COLUMN_ALREADY_EXISTS -- a message that names neither the union nor the alias.
    #
    # MEASURED 25 September, and this is the second time this class has reached the
    # workspace. The invoice union aliased `buyer: Buyer_Code` while the source carries
    # `Buyer`, `Buyer_Code` and `Buyer_Name`; the load stopped at task five with 1,213
    # offline checks green. The first time, an all-columns alias block was mostly
    # case-only and Databricks answered AMBIGUOUS_REFERENCE. That one was fixed by
    # stripping the aliases and adding no gate, so the lesson was learned and not kept.
    # Checked against `every` -- the exact column set the SELECT below emits -- and not
    # against a second walk of `types`. Two derivations of "the source's columns" could
    # drift, and the one that decides the refusal must be the one that builds the view.
    existing = {c.lower(): c for c in every}
    for alias in sorted((union.get("derived_columns") or {})):
        clash = existing.get(alias.lower())
        if clash is not None:
            raise ValueError(
                f"union {union['name']}: derived alias {alias!r} collides with source "
                f"column {clash!r} -- Spark resolves identifiers case-insensitively, so "
                f"the view would emit that column twice and Databricks refuses it. "
                f"Rename the alias to something no source column matches in any casing.")

    where = f" WHERE {union['row_filter']}" if union.get("row_filter") else ""
    derived = "".join(
        f", {expr} AS `{name}`"
        for name, expr in sorted((union.get("derived_columns") or {}).items()))

    parts = []
    for table in tables:
        have = types.get(table) or {}
        cols = ", ".join(
            f"`{c}`" if c in have else f"CAST(NULL AS {every[c]}) AS `{c}`"
            for c in columns)
        parts.append(f"SELECT {cols}{derived} FROM {src}.`{table}`{where}")
    body = "\nUNION ALL\n".join(parts)
    return (f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{view_name(union)}` AS\n"
            f"{body}")


def view_name(union: dict) -> str:
    return f"v_{union['name']}"


def union_schema(union: dict) -> str:
    """The schema the view is created in, from the DECLARATION.

    Read from metadata/source_unions.yml rather than taken as a job parameter because
    verify_repo's active_sources gate needs the same value: its rule is "a binding reading
    01_usnc_bronze_dev. is real in this lake and must be active", and a union view we build
    in our own catalog is real too. Two copies of this name would be a duplicate authority.
    """
    return union["view_schema"]


def require_record_table(spark, fq: str) -> None:
    """Fail if the record table is absent, instead of letting saveAsTable create it.

    THIS IS A FIX, NOT A PRECAUTION. `df.write.mode("append").saveAsTable(...)` CREATES a
    missing table -- silently, and WITHOUT the TBLPROPERTIES its DDL declares. On 4 September
    apply_source_unions was run with `--only apply_source_unions`, which does not run
    upstream tasks, so create_control_objects never made ctl_source_union and saveAsTable
    made it instead. The table came out with delta.appendOnly unset, and assert_append_only
    failed the load with exactly that: "delta.appendOnly is not set -- Delta will permit
    mutation". A control table that can be rewritten is not evidence of what a past load did.
    (`--only +task` runs upstream and would have been fine, which is precisely why this
    cannot be left to how someone invoked the job.)

    The table's existence is create_control_objects' responsibility. A writer that quietly
    provisions its own storage produces an object nothing verified -- the same reason this
    task stopped issuing CREATE SCHEMA.
    """
    if not spark.catalog.tableExists(fq):
        raise SystemExit(
            f"  * MISSING_RECORD_TABLE: {fq} does not exist. create_control_objects owns it "
            f"and has not run -- run the job, or `--only +<task>` so upstream runs too. "
            f"Writing anyway would let saveAsTable create it WITHOUT delta.appendOnly, and a "
            f"rewritable control table is not evidence."
        )


def gate_status(findings: list[str]) -> str:
    return "FAILED" if findings else "APPLIED"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True, help="where the views are created (ours)")
    ap.add_argument("--bronze-catalog", required=True, help="where the tenant tables live")
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    unions = load_unions()
    if not unions:
        print("  * NO_UNIONS: metadata/source_unions.yml declares none. This task is wired "
              "into the job, so an empty declaration is a mistake, not a no-op")
        return 1

    # THE MODEL DECIDES WHAT THE VIEW PROJECTS, so it is loaded here rather than passed as a
    # parameter: the columns a view must carry are a property of the bindings that read it,
    # and the one place that knows them is the metadata.
    from accelerator import spec  # noqa: E402

    _model = spec.load_model(UNIONS_PATH.parent / "entities")

    from pyspark.sql import SparkSession  # noqa: E402

    spark = SparkSession.builder.getOrCreate()
    # The schema is per-union now, so it belongs on each union's line rather than in this
    # header. An earlier version printed it here and referenced `union` before the loop that
    # binds it -- an UnboundLocalError that no offline test could see, because the tests
    # exercise the pure decision functions and never enter main().
    print(f"source unions :: target={args.target} bronze={args.bronze_catalog} "
          f"views in {args.catalog}\n")

    all_findings: list[str] = []
    records: list[tuple] = []

    for union in unions:
        name = union["name"]

        # A VAULT-SOURCED PROFILE GETS NO VIEW AND NO BRONZE LOOKUP. Its input is a table
        # this pipeline BUILDS (nhl_invoice_line), not one Bronze delivers, so there is no
        # information_schema in the bronze catalog that could describe it and nothing here
        # to create. Its row_filter and derived_columns are applied in the flow by
        # spec._resolve_conformance and factory._stage_full, exactly as a single-table
        # Bronze profile's are. The declaration is still validated, and the run is still
        # recorded, so a profile that stops becoming an object does not stop being visible.
        if is_vault_sourced(union):
            findings = declaration_findings(union)
            print(f"  {name}: vault-sourced -- conformance applied in the flow, no view "
                  f"created and no Bronze lookup made")
            for f in findings:
                print(f"      FAIL  {f}")
            all_findings += findings
            records.append((args.job_run_id, args.target, name, "(vault-sourced)",
                            0, [], [], gate_status(findings)))
            continue

        schema = union["source_schema"]
        rows = [(r["table_name"], r["table_schema"]) for r in spark.sql(
            f"SELECT table_name, table_schema FROM `{args.bronze_catalog}`."
            f"information_schema.tables WHERE table_schema = '{schema}'").collect()]
        tables = matching_tables(rows, union["table_pattern"],
                                 union.get("exclude_pattern", ""))

        col_rows = spark.sql(
            f"SELECT table_name, column_name, data_type FROM `{args.bronze_catalog}`."
            f"information_schema.columns WHERE table_schema = '{schema}'").collect()
        columns: dict[str, set] = {}
        types: dict[str, dict[str, str]] = {}
        for r in col_rows:
            if r["table_name"] in tables:
                columns.setdefault(r["table_name"], set()).add(r["column_name"])
                types.setdefault(r["table_name"], {})[r["column_name"]] = r["data_type"]

        findings = declaration_findings(union) + coverage_findings(union, tables, columns)
        warnings = type_findings(union, types) if tables else []
        print(f"  {name}: {len(tables)} table(s) matched")
        for w in warnings:
            print(f"      note  {w}")
        for f in findings:
            print(f"      FAIL  {f}")

        # THE SCHEMA IS PROVISIONED, NOT CREATED HERE, and that is a correction rather than
        # a preference. This task used to issue CREATE SCHEMA IF NOT EXISTS every run and
        # failed on the workspace with UNAUTHORIZED_ACCESS: the job's run_as identity has no
        # CREATE SCHEMA on our own silver catalog, even though the schema owner does. The
        # same run-as privilege gap already blocks assert_journal_integrity.
        #
        # It was over-reach anyway. Schema DDL is create_control_objects' job; this task
        # creates VIEWS. So it requires the schema and says so plainly when it is missing,
        # rather than trying to conjure one with rights it does not hold.
        # A SINGLE-TABLE PROFILE IS NOT A UNION AND GETS NO VIEW. Its filter and renames
        # are applied in the flow instead (spec._resolve_conformance, factory._stage_full),
        # because a masked entity reading a view cannot be built -- measured 25 September,
        # raw_vault_pay_bill never left INITIALIZING in four attempts while every other
        # domain finished in 1.0 to 17 minutes. The coverage and type checks above still
        # run: the profile is still validated against the lake, it just stops becoming an
        # object.
        if len(tables) < 2:
            print(f"      single table -- conformance applied in the flow, no view created")
            all_findings += findings
            records.append((args.job_run_id, args.target, name,
                            f"(inline) {schema}.{union['table_pattern']}",
                            len(tables), tables, warnings, gate_status(findings)))
            continue

        if not findings and not args.dry_run:
            _keep = columns_needed(
                _model, f".{union_schema(union)}.{view_name(union)}", union)
            spark.sql(view_sql(union, args.catalog, union_schema(union),
                               args.bronze_catalog, tables, types, keep=_keep))
            print(f"      projecting {len(_keep)} declared column(s) of "
                  f"{len({c for t in tables for c in (types.get(t) or {})})} in source")
            print(f"      created {args.catalog}.{union_schema(union)}.{view_name(union)}")

        all_findings += findings
        records.append((args.job_run_id, args.target, name,
                        f"{args.catalog}.{union_schema(union)}.{view_name(union)}",
                        len(tables), tables, warnings, gate_status(findings)))

    # THE RECORD IS WRITTEN EVEN WHEN THE GATE FAILS. A failed apply that left no trace
    # would be indistinguishable from a run that was never asked to build anything.
    if not args.dry_run:
        from pyspark.sql import functions as F

        require_record_table(
            spark, f"{args.catalog}.{args.control_schema}.{RECORD_TABLE}")

        df = spark.createDataFrame(
            records,
            "job_run_id string, target string, union_name string, view_name string, "
            "tables_covered bigint, tables array<string>, findings array<string>, "
            "status string",
        ).withColumn("recorded_at", F.current_timestamp())
        df.write.mode("append").saveAsTable(
            f"`{args.catalog}`.`{args.control_schema}`.`{RECORD_TABLE}`")
        print(f"\n  recorded {len(records)} row(s) into {args.control_schema}.{RECORD_TABLE}")

    print("\n" + "=" * 66)
    if all_findings:
        print(f"SOURCE UNION APPLY FAILED -- {len(all_findings)} finding(s)")
        return 1
    print(f"SOURCE UNIONS APPLIED: {len(unions)} view(s)")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a task failure, for 0 as readily as 1.
    _rc = main()
    if _rc:
        sys.exit(_rc)
