"""
HARD GATE: prove the vault is append-only.

This is the check that makes the audit narrative true rather than intended. It runs
after every load and fails the job, because an UPDATE against a satellite is not
something to discover during an audit.

What it asserts, per vault table:
  1. DESCRIBE HISTORY shows no UPDATE, DELETE, MERGE, TRUNCATE or RESTORE, ever.
  2. No operation records a full refresh / overwrite write mode.
  3. delta.appendOnly is set on the table itself, so Delta refuses mutation.
  4. Hash keys are unique where they must be:
       - hub: one row per hash key
       - nhl: one row per hash key (the dedup guard held across batches)
       - sat: one row per (parent key, load_dts, sub_seq[, mas_key])

CONTROL TABLES ARE IN SCOPE, WITH A NARROWER ASSERTION. The job also passes the
control schema, which holds no vault table at all -- see CONTROL_PREFIXES below for
which of its tables carry the append-only property and which cannot.

Exit code 1 fails the task. There is deliberately no override flag.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import control_standard as _cs  # noqa: E402
from accelerator import naming as _naming  # noqa: E402

MUTATING = {
    "UPDATE", "DELETE", "MERGE", "TRUNCATE", "RESTORE",
    "REPLACE TABLE", "CREATE OR REPLACE TABLE",
}

# DEF-42: `stg_` is here so the STAGING LOGS are covered by the mutation assertion --
# they hold vault data and must be append-only like everything else. They are excluded
# from the UNIQUENESS assertion below, because holding several rows for one key is
# precisely what a log is for; the hub built from it is where uniqueness is asserted.
# DERIVED, NOT RETYPED -- and this list is the reason the rule exists. It was hand-typed
# and patched twice after the fact: DEF-42 added `stg_`, and the note below records `qtn_`
# arriving on 30 August after nine quarantine twins went unchecked. Measured 5 Sep 2026 it
# was STILL short by five -- `sal_`, `pit_`, `br_`, `rsat_`, `ssat_` -- so a same-as link
# table could have been UPDATEd and this gate, whose whole job is to refuse a mutation on
# a vault table, would never have looked at it.
#
# naming.GENERATED_TABLE_PREFIXES is every prefix this repo puts on a table it creates,
# derived from naming.PREFIX. A new kind now lands here without anyone remembering to
# come and add it, which is the property this file already argued for -- see the DEF-55
# note below: "the declaration wins and this file reads it". That was applied to the
# control split and not to this line.
VAULT_PREFIXES = _naming.GENERATED_TABLE_PREFIXES
STAGING_PREFIX = "stg_"

# ADDED 30 Aug 2026. `qtn_` was absent, so NINE quarantine twins in usnc_tds were never
# checked by the one gate that exists to notice a vault table shrinking -- and the harm was
# already written down elsewhere. OPEN_ITEMS: "A shrinking twin cannot be reconciled. A full
# refresh of raw_vault truncates qtn_* while ctl_quarantine_superseded is delta.appendOnly,
# so orphaned_records() and over_subtracted() would fire PERMANENTLY." So the failure mode was
# known, the recovery was known to be a manual control-table cleanup, and the gate that would
# have caught it was looking the other way.
#
# It is the same argument DEF-42 made for stg_ -- a table holding vault data must be
# append-only like everything else -- applied to the one prefix that argument missed.
QUARANTINE_PREFIX = "qtn_"

# DEF-55: THE CONTROL SCHEMA IS PASSED TO THIS GATE AND HOLDS NO VAULT TABLE.
# resources/vault_job.yml passes --schema ${var.control_schema}, whose tables are named
# ctl_, ref_ and aud_. Matched against VAULT_PREFIXES alone, that schema yielded zero
# tables, landed in empty_schemas, and the gate printed "no vault tables found" and
# returned 1 on EVERY run -- blocking the grant sweep, governance, mask survival, audit
# completeness and the run's own 'completed' row. No run of this job could reach green.
#
# The fix is scope, not a relaxation. History is asserted on the aud_ tables and on
# nothing else in that schema:
#
#   aud_*  -- written ONLY by this repo, insert-only by construction, two rows per run
#             rather than one row updated. An audit that can be rewritten is not an
#             audit, so DESCRIBE HISTORY showing no UPDATE/DELETE/MERGE/TRUNCATE/RESTORE
#             is exactly the property that makes it worth reading.
#   ctl_*  -- ctl_approval_manifest is populated by an EXTERNAL producer; its own table
#             comment says so, because populating it here would make the vault
#             self-certifying. A legitimate producer MERGE would redden OUR build for a
#             table we do not own.
#   ref_*  -- ref_dq_expectation is curated configuration. A rule is superseded by
#             editing it, so it is not insert-only either.
#
# THE SCOPE IS STILL RIGHT; ITS SOURCE WAS NOT. Deciding by PREFIX made the rationale above
# cover a table it never reasoned about. ctl_quarantine_superseded shares the ctl_ prefix but is
# written by checks/supersede_quarantine.py -- by us -- and control_standard declares it
# append-only. governance/control_objects.sql says of it: "THIS TABLE CAN DISARM A HARD GATE,
# which is why it is append-only and asserted." It was not asserted. A spurious supersede record
# REDUCES loop-1's quarantined count and makes that gate pass on a real variance, so of every
# control table this is the one where the unbacked claim mattered most.
#
# So the split now comes from control_standard, BY NAME. Two authorities stating the same
# property by different means is the shape this repo has been bitten by twice
# (BUSINESS_KINDS, the system-column set); the declaration wins and this file reads it.
#
# UNDECLARED FAILS CLOSED. A control table the declaration names in neither set is not skipped:
# it is a problem. Nobody has decided what it is, and a control table nobody has decided about
# is exactly where silence is dangerous -- adding it to the declaration is one line.
#
# Uniqueness is skipped for every control table -- see check_uniqueness.
AUDIT_PREFIXES = ("aud_",)
CONTROL_PREFIXES = ("ctl_", "ref_") + AUDIT_PREFIXES


def classify_control_table(table: str) -> str:
    """"assert" | "mutable" | "undeclared" for one control table, by NAME.

    Pure and Spark-free, so a fabricated name exercises the same decision the live sweep
    makes -- the shape checks/schema_grant_check.py uses for its decision functions.
    """
    if table in _cs.APPEND_ONLY:
        return "assert"
    if table in _cs.MUTABLE:
        return "mutable"
    return "undeclared"


def _tables(spark, catalog: str, schema: str, prefixes: tuple) -> list[str]:
    rows = spark.sql(
        f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        -- DEF-26: EVERY VAULT TABLE IS A STREAMING_TABLE, NOT 'MANAGED'.
        -- This gate filtered on MANAGED and therefore matched NOTHING: the only MANAGED
        -- rows in the schema are SDP's internal __materialization_* tables, whose names
        -- do not start with a vault prefix. So the sharpest assertion in the repo -- NHL
        -- uniqueness, the one real test of the staging dedup -- could never see a single
        -- table. It failed loudly only because of the explicit "no vault tables found"
        -- guard below; without that it would have exited 0 having asserted nothing,
        -- which is exactly the pathology this sub-project exists to prevent.
        WHERE table_schema = '{schema}'
          AND table_type IN ('MANAGED', 'STREAMING_TABLE')
        ORDER BY table_name
        """
    ).collect()
    return [
        r["table_name"]
        for r in rows
        if r["table_name"].startswith(prefixes)
        and not r["table_name"].endswith("_v1")
    ]


def vault_tables(spark, catalog: str, schema: str) -> list[str]:
    return _tables(spark, catalog, schema, VAULT_PREFIXES)


def control_tables(spark, catalog: str, schema: str) -> list[str]:
    """The control tables in a schema, whether or not history is asserted on them.

    Returned for ALL THREE prefixes rather than for aud_ only, because main() has to be
    able to say which control tables it deliberately did not assert over. A silent skip
    is how a schema ends up under a green gate with nothing checked in it.
    """
    return _tables(spark, catalog, schema, CONTROL_PREFIXES)


def check_history(spark, fq: str) -> list[str]:
    problems: list[str] = []
    hist = spark.sql(f"DESCRIBE HISTORY {fq}").select("version", "operation", "operationParameters")
    for row in hist.collect():
        op = (row["operation"] or "").upper()
        if op in MUTATING:
            problems.append(f"{fq}: version {row['version']} performed {op}")
        params = row["operationParameters"] or {}
        mode = str(params.get("mode", "")).lower()
        if mode in ("overwrite", "complete"):
            problems.append(f"{fq}: version {row['version']} wrote with mode={mode}")
    return problems


def check_append_only_property(spark, fq: str) -> list[str]:
    props = {
        r["key"]: r["value"]
        for r in spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()
    }
    if props.get("delta.appendOnly", "false").lower() != "true":
        return [f"{fq}: delta.appendOnly is not set -- Delta will permit mutation"]
    return []


def check_uniqueness(spark, fq: str, table: str) -> list[str]:
    # DEF-55: A CONTROL TABLE HAS NO HASH KEY AND NO VAULT GRAIN. own_hk below is derived
    # from the table name, so `aud_table_load` yields `table_load_hk` -- a column that
    # does not exist -- and the check reports a bogus "expected own hash key not found",
    # or worse builds a SELECT over a column list that is not there. Uniqueness is not a
    # property these tables have: aud_table_load holds one row per (run, table) and
    # ctl_approval_manifest's grain belongs to its external producer. Returned BEFORE the
    # schema is read so this branch can be fired without a session.
    if table.startswith(CONTROL_PREFIXES):
        return []

    cols = {f.name for f in spark.table(fq).schema.fields}

    # DEF-27: THE GRAIN IS DERIVED FROM THE TABLE NAME, NOT SNIFFED FROM THE COLUMNS.
    # This previously did `next(c for c in cols if c.endswith("_hk"))` over a SET, so it
    # picked an ARBITRARY hash key -- and non-deterministically. A hub has only one, so it
    # was right by luck there; an NHL or link carries its own key AND one per parent, and
    # the check landed on a PARENT key. It then reported "794888 duplicate rows" for
    # nhl_general_journal_line at grain (accounting_journal_hk) -- which is not a defect
    # at all: an NHL has many lines per journal by definition. The sharpest assertion in
    # the repo was asserting something that is SUPPOSED to be false.
    #
    # `hub_accounting_journal` -> `accounting_journal_hk`, `nhl_journal_line` ->
    # `journal_line_hk`. This is naming.hk() applied to the table's own entity name.
    # THE VERSION SUFFIX IS NOT PART OF THE ENTITY NAME, AND THIS DERIVATION IS FROM THE
    # ENTITY. Measured 26 September, on the first load after the versioning migration:
    # `hub_job_request_rev1` yielded `job_request_rev1_hk`, a column that exists nowhere,
    # and this gate failed on SIXTEEN correct tables at once. A hash key is named from the
    # entity (naming.hk), so `hub_job_request_rev1` and `hub_job_request_rev2` both carry
    # `job_request_hk` -- the version qualifies the TABLE, never the key inside it.
    #
    # _naming.stable() raises on a name with no suffix, and this gate is also handed
    # control tables and anything else a schema holds, so the unversioned case falls
    # through unchanged rather than blowing up.
    try:
        _entity_table = _naming.stable(table)
    except ValueError:
        _entity_table = table
    own_hk = f"{_entity_table.split('_', 1)[1]}_hk"

    if table.startswith(STAGING_PREFIX):
        # DEF-42: a staging log holds every row every flow appended, duplicates included.
        # That is what the hub loader's anti-join reads. Asserting uniqueness here would
        # fail permanently and would prove nothing about the vault -- the hub built from
        # this log carries the assertion instead, and does so on the object consumers
        # actually join to.
        return []

    if table.startswith(QUARANTINE_PREFIX):
        # A QUARANTINE TWIN IS NOT UNIQUE BY KEY, AND MUST NOT BE ASSERTED AS IF IT WERE.
        # The same key is rejected again every time a bad row is re-delivered, and loop-1's
        # arithmetic -- landed + (quarantined - superseded) = approved -- counts those
        # rejections individually. Asserting one row per key here would fail on correct
        # behaviour, exactly as it would on a staging log.
        #
        # The MUTATION assertion above still applies, and it is the one that matters: a twin
        # that shrinks breaks loop-1 permanently.
        return []

    if table.startswith(("hub_", "nhl_", "lnk_", "hal_")):
        if own_hk not in cols:
            return [f"{fq}: expected own hash key {own_hk} not found in {sorted(cols)[:8]}"]
        grain = [own_hk]
    else:
        # a satellite hangs off its parent, so the parent key is the right grain here --
        # but it is still derived, not guessed: the satellite's own name carries it.
        parent_hk = next((c for c in sorted(cols) if c.endswith("_hk")), None)
        if not parent_hk:
            return [f"{fq}: no parent *_hk column found"]
        grain = [parent_hk, "load_dts", "sub_seq"]
        if "mas_key" in cols:
            grain.append("mas_key")

    grain = [g for g in grain if g in cols]
    key = ", ".join(grain)
    dupes = spark.sql(
        f"SELECT COUNT(*) AS n FROM (SELECT {key} FROM {fq} GROUP BY {key} HAVING COUNT(*) > 1)"
    ).collect()[0]["n"]
    if dupes:
        return [f"{fq}: {dupes} duplicate rows at grain ({key})"]
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    # DEF-44: REPEATABLE. This took a single --schema and the job passed only the raw
    # vault, so the BUSINESS vault -- its own streaming tables and their materialization
    # twins -- was never checked for a mutating operation at all. Not a weaker assertion:
    # no assertion, on half the estate, under a green gate. Every vault schema is passed
    # now, and a run that inspected only one of two is not a pass.
    ap.add_argument("--schema", action="append", required=True,
                    help="a vault schema; repeatable, and ALL of them must be given")
    args = ap.parse_args()

    spark = SparkSession.builder.getOrCreate()

    problems: list[str] = []
    inspected = 0
    audited = 0
    not_asserted: list[str] = []
    empty_schemas = []
    for schema in args.schema:
        tables = vault_tables(spark, args.catalog, schema)
        controls = control_tables(spark, args.catalog, schema)
        asserted_here = len(tables)
        for table in tables:
            fq = f"`{args.catalog}`.`{schema}`.`{table}`"
            problems += check_history(spark, fq)
            problems += check_append_only_property(spark, fq)
            problems += check_uniqueness(spark, fq, table)
            print(f"  checked {schema}.{table}")
        inspected += len(tables)
        for table in controls:
            # DEF-55: history AND the delta.appendOnly property, the same two assertions
            # every vault table gets. Not uniqueness -- see check_uniqueness.
            #
            # BY NAME, FROM THE DECLARATION, not by prefix -- see classify_control_table.
            verdict = classify_control_table(table)
            if verdict == "mutable":
                not_asserted.append(f"{schema}.{table}")
                continue
            if verdict == "undeclared":
                problems.append(
                    f"{schema}.{table} is a control table the standard does not classify. "
                    f"src/accelerator/control_standard.py names it in neither APPEND_ONLY nor "
                    f"MUTABLE, so nobody has decided whether it may be rewritten -- and an "
                    f"unclassified control table is skipped silently by every gate that reads "
                    f"the declaration. Add it to one set"
                )
                continue
            fq = f"`{args.catalog}`.`{schema}`.`{table}`"
            problems += check_history(spark, fq)
            problems += check_append_only_property(spark, fq)
            print(f"  checked {schema}.{table} (declared append-only)")
            audited += 1
            asserted_here += 1
        if not asserted_here:
            empty_schemas.append(f"{args.catalog}.{schema}")

    if empty_schemas:
        # A schema that was named and holds nothing this gate can assert over is a defect,
        # not a quiet skip: the caller believed it was asserting over it. A control schema
        # holding only ctl_/ref_ tables lands here too -- if the aud_ tables are gone,
        # this gate is asserting nothing about the audit and must say so rather than pass.
        print(f"FAIL no vault or audit table found in {', '.join(empty_schemas)}")
        return 1

    print(f"\n{inspected} vault table(s) and {audited} audit table(s) inspected across "
          f"{len(args.schema)} schema(s)")
    for name in not_asserted:
        # Announced, never silent. These two are out of scope BY DECISION (see
        # CONTROL_PREFIXES), and a decision nobody can read in the run log is a gap.
        print(f"  ~ NOT ASSERTED {name}: written by an external producer or curated as "
              f"configuration, so insert-only is not its property")
    if problems:
        print(f"\nAPPEND-ONLY GATE FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        return 1
    print(f"APPEND-ONLY GATE PASSED: zero mutating operations across {inspected} "
          f"vault table(s) and {audited} audit table(s) in {', '.join(args.schema)}")
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
