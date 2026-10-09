"""
HARD GATE: accounting integrity of the journal domain.

WHY THIS IS A SEPARATE CHECK AND NOT AN EXPECTATION
---------------------------------------------------
Pipeline expectations are ROW-LEVEL. They can assert that a journal line has exactly one
of debit or credit populated. They cannot assert that

    SUM(debits) = SUM(credits)   for a journal

because that is an AGGREGATE invariant over a set of rows that arrive as independent
records. The same is true of the control total a source supplies on the header, which
states what the journal should come to. That is the loop-1 reconciliation pattern
arriving for free -- and it is only checkable after the set has landed.

CORRECTED 27 Aug: this paragraph used to add "and that neither is zero", and that advice
is WRONG FOR A MULTI-CURRENCY LEDGER. The `accounting:` block declares the FUNCTIONAL
amounts only -- for GP, debitamt and crdtamnt -- and a foreign-currency line may
legitimately carry zero in both while ordbtamt / orcrdamt hold the real originating value.
Measured against Bronze: of 7,523 history lines with zero functional amounts, 7,366 (98%)
carry a non-zero originating amount. An expectation reading "neither is zero" would have
quarantined all 7,366, silently, because `drop` is the only implemented tier.

So a zero-amount rule must consider EVERY currency's amount columns, and the model does not
help you find them: ordbtamt and orcrdamt sit in the entity's `payload` with no declared
role, so nothing here or in the metadata tells a rule author they exist. See
governance/dq_expectations_proposed.sql for the corrected form and its measured effect.

This is a general point worth remembering when reading the expectations in the metadata:
if an invariant spans rows, it belongs here, not in an expectation. Shipping it as an
expectation would look like coverage and provide none.

METADATA-DRIVEN, NOT HARDCODED -- AND WHY THAT IS NOT A REFACTOR
----------------------------------------------------------------
This gate used to name `line_order`, `debit_amount` and `credit_amount` in its SQL, and
to default `--line-table` to one table. Those are the retired Workday-shaped binding's
column names. NO SOURCE THE VAULT NOW BINDS DELIVERS ANY OF THEM: Dynamics GP calls them
`seqnumbr`, `debitamt` and `crdtamnt`; UKG calls the amounts `debit` and `credit` and has
no line ordinal at all. The generator projects nothing and renames nothing, so the gate
was naming columns that do not exist -- against two of the three journal-line tables it
now has to cover, it asserted nothing whatsoever while reading like full coverage.

So the names come from the model, the same way checks/aggregate_reconciliation_check.py
takes its pairs from `grain` / `aggregates_from`. Each journal-line entity declares an
`accounting:` block (src/accelerator/spec.py, validated there) naming which of its own
columns play which role:

    journal        which PARENT hub groups lines into one journal
    debit, credit  the two amount columns
    line_order     the dense line ordinal -- OPTIONAL, absent where the source has none
    control_total  declared on the entity that CARRIES the control figure: the journal
                   header satellite, not the line

Add a journal entity later and this file covers it without being edited. Rename a source
column and the metadata moves with it.

WHAT IT ASSERTS, per journal, for every entity declaring debit and credit
  1. Every journal balances: total debits = total credits, within currency tolerance.
  2. Where a header supplies a control total, the lines agree with it.
  3. The line ordering column is dense and unique -- it is a dependent child key, so a
     gap or a duplicate means lines were lost or double-loaded.
  4. Every line resolves to a real parent hub row (no ghost keys in production data,
     which would mean a reference arrived before its master).

WHAT IT DOES *NOT* DO IS SKIP QUIETLY
-------------------------------------
A property that cannot be evaluated for an entity -- this source declares no line
ordinal, no entity declares a control total for this journal, the table is empty -- is
reported by name under NOT EVALUATED and counted in the summary line. It is never
silently dropped, and no assertion is weakened to make it pass. The gate that says
"PASSED" tells you exactly how much it looked at.

An entity with NO ACTIVE SOURCE BINDING in this lake is skipped the same way, before any
SQL runs. Its table exists (D5 keeps the inventory identical) but is created from its
ghost flow alone, and its _v1 projection is not created at all -- so a control-total join
against a deferred header satellite would raise TABLE_OR_VIEW_NOT_FOUND, and this file
used to route that to a problem with a misleading "check mask privileges" message.
Activity comes from `--active-sources` and is resolved through spec.active_table_bindings,
the same function factory.build emits from.

Two evaluability traps are treated as FAILURES rather than as absences, because both
produce a green gate over nothing:
  * every amount reading NULL on a non-empty table -- the refresh-as-owner trap: the
    run-as identity is not privileged under mask_money, so every sum is 0 and every
    journal "balances" (DEPLOY.md Phase 6b);
  * any SQL error, which is reported rather than swallowed.

Exit 1 fails the task. A journal that does not balance is not a data quality nuance; it
is an accounting error that must not reach Gold.
"""

from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define
# __file__, so every Path(__file__) below raised NameError and the gate died before
# asserting anything. compile() still records the real path in the code object.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import naming, spec  # noqa: E402
from accelerator.hashing import zero_key_sql  # noqa: E402

# Money comparison tolerance. Not zero: ledger and transaction amounts are stored at
# different scales (the XSD allows 6 fraction digits on transaction amounts and 3 on
# ledger amounts), so an exact equality test would fail on legitimate rounding.
TOLERANCE = "0.005"

# The ghost row is a zero-key placeholder appended once per table so PIT joins stay
# equi-joins. It carries no amounts and no parent keys, so it is excluded everywhere
# rather than quietly forming a NULL journal that trivially balances.
NOT_GHOST = f"{naming.COL['rec_src']} <> 'SYSTEM'"


class Report:
    """Findings, and -- just as important -- what could not be looked at."""

    def __init__(self) -> None:
        self.problems: list[str] = []
        self.evaluated = 0
        self.not_evaluated: list[str] = []
        self.skipped_inactive = 0

    def asserted(self) -> None:
        self.evaluated += 1

    def skipped(self, what: str, why: str, inactive: bool = False) -> None:
        """A property that could not be asserted. `inactive` marks the ones explained by
        a binding this lake declares inactive -- see the vacuity rule in main()."""
        self.not_evaluated.append(f"{what}: {why}")
        if inactive:
            self.skipped_inactive += 1


# --------------------------------------------------------------------------- #
# THE LAST LINE EVERY GATE PRINTS, in the same shape in all four.
#
# A gate that exits 0 under a "GATE NOT EVALUATED" banner is honest in its own log and
# INVISIBLE in the job run: Databricks shows a succeeded task as green, and there is no
# supported "green with a warning" state. Someone scanning a successful run sees four
# green tasks and reasonably infers four gates asserted something.
#
# This does not fix that -- see DEPLOY.md Phase 5f for what it does fix. It makes the
# outcome MACHINE-READABLE and puts it last, so one grep over the four task outputs
# answers "what did this run actually prove", and the runbook can require that check
# after a green run rather than hoping someone opens each task.
#
#     GATE SUMMARY :: <gate> :: status=<PASSED|NOT_EVALUATED|FAILED> asserted=<n> not_evaluated=<m>
#
# status is the gate's own verdict, not the exit code: NOT_EVALUATED exits 0 (a declared
# dormancy is a correct outcome for a deliberately partial lake) but must never be read
# as PASSED.
# --------------------------------------------------------------------------- #
GATE = "journal_integrity"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def count_and_sample(spark, sql: str, max_report: int, cte: str = ""):
    """(how many rows the query returns, up to max_report of them).

    The count is computed in SQL and the sample is only fetched when there is something
    to fetch, so a load that goes wrong across millions of journals reports the true
    figure without collecting millions of rows into the driver -- and a healthy run costs
    exactly one scan.

    `cte` is any WITH clause the query needs; it is prefixed to BOTH statements rather
    than nested inside the COUNT's derived table, which keeps the SQL to the plainest
    shape every engine agrees on.
    """
    total = spark.sql(f"{cte} SELECT COUNT(*) AS n FROM ({sql})").collect()[0]["n"]
    rows = spark.sql(f"{cte} {sql} LIMIT {int(max_report)}").collect() if total else []
    return total, rows


def line_entities(model: spec.Model) -> list[spec.Entity]:
    """Every entity declaring both amount roles -- the journal lines, from metadata."""
    return [e for e in model.entities
            if {"debit", "credit"} <= set(e.accounting_map)]


def control_total_entities(model: spec.Model, journal_parent: str) -> list[spec.Entity]:
    """Entities carrying a control total for the same journal these lines hang off."""
    return [e for e in model.entities
            if "control_total" in e.accounting_map and journal_parent in e.parents]


def check_entity(spark, catalog: str, schema: str, model: spec.Model,
                 entity: spec.Entity, src, table: str, max_report: int, report: Report,
                 active) -> None:
    acct = entity.accounting_map
    if not spec.active_table_bindings(entity, src, active):
        report.skipped(
            f"{table}: all properties",
            "no active source binding in this lake, so the table is created from its "
            "ghost flow alone and holds no journal lines. Declared, not loaded",
            inactive=True,
        )
        return
    lines = naming.qualified(catalog, schema, table)
    journal_hk = naming.hk(acct["journal"])
    debit, credit = acct["debit"], acct["credit"]

    print(f"\n{table}  (journal={journal_hk}, debit={debit}, credit={credit}, "
          f"line_order={acct.get('line_order', '-- none declared --')})")

    # ---- 0. can anything be asserted at all? --------------------------------
    # A table with no rows and a table whose every amount reads NULL both make every
    # aggregate assertion below pass vacuously. They are different situations and get
    # different treatment: empty is a stated absence, all-NULL is the refresh-as-owner
    # trap and fails the gate.
    #
    # The ghost-key counts (property 4) ride along on this same scan. Every parent the
    # entity declares, from the metadata: hardcoding the list is how this gate came to
    # name company_hk, a column that stopped existing when hub_company folded into
    # hub_organisation.
    ghost_counts = ", ".join(
        f"SUM(CASE WHEN {naming.hk(p)} = {zero_key_sql()} THEN 1 ELSE 0 END) AS ghost_{p}"
        for p in entity.parents
    )
    try:
        visibility = spark.sql(f"""
            SELECT COUNT(*) AS lines,
                   COUNT({debit}) + COUNT({credit}) AS amounts_visible,
                   {ghost_counts}
            FROM {lines} WHERE {NOT_GHOST}
        """).collect()[0]
    except Exception as exc:  # noqa: BLE001
        report.problems.append(
            f"{table}: could not be read at all: {exc}. The columns this gate names come "
            f"from the entity's accounting: block -- if this is an AnalysisException, the "
            f"metadata and the delivered table disagree about a column name."
        )
        return

    if visibility["lines"] == 0:
        report.skipped(f"{table}: all properties",
                       "the table holds no non-ghost rows -- nothing to assert. If this "
                       "entity is meant to be loading here, its source binding may be "
                       "inactive (active_sources) or its flow may not have run")
        return
    if visibility["amounts_visible"] == 0:
        report.problems.append(
            f"{table}: {visibility['lines']} line(s) but EVERY {debit}/{credit} value "
            f"reads NULL. Balance and control-total assertions would both pass on zeros. "
            f"This is almost certainly the refresh-as-owner trap: the run-as identity is "
            f"not privileged under mask_money -- see DEPLOY.md Phase 6b."
        )
        return

    # ---- 1. debits = credits, per journal -----------------------------------
    total_unbalanced, unbalanced = count_and_sample(spark, f"""
        SELECT {journal_hk} AS journal_hk,
               SUM(COALESCE({debit}, 0))  AS debits,
               SUM(COALESCE({credit}, 0)) AS credits,
               COUNT(*)                   AS lines
        FROM {lines} WHERE {NOT_GHOST}
        GROUP BY {journal_hk}
        HAVING ABS(SUM(COALESCE({debit}, 0)) - SUM(COALESCE({credit}, 0))) > {TOLERANCE}
    """, max_report)
    report.asserted()
    print(f"  unbalanced journals: {total_unbalanced}")
    if total_unbalanced > len(unbalanced):
        report.problems.append(
            f"{table}: {total_unbalanced} journals do not balance; the first "
            f"{len(unbalanced)} are listed"
        )
    for row in unbalanced:
        report.problems.append(
            f"{table}: journal {row['journal_hk']!r} does not balance: "
            f"debits={row['debits']} credits={row['credits']} over {row['lines']} lines"
        )

    # ---- 2. lines agree with a declared control total ------------------------
    headers = control_total_entities(model, acct["journal"])
    if not headers:
        report.skipped(
            f"{table}: control total",
            f"no entity declares an accounting.control_total for parent "
            f"{acct['journal']!r}. Dynamics GP supplies no control figure, so there is "
            f"nothing to reconcile the lines against -- this is an absent CONTROL, not a "
            f"passing one",
        )
    for header in headers:
        for header_src, header_table in header.tables():
            if not spec.active_table_bindings(header, header_src, active):
                # Its _v1 is not created for an inactive satellite, so joining to it
                # raises rather than returning nothing. An absent control is an absent
                # control, not a permissions problem and not a passing comparison.
                report.skipped(
                    f"{table}: control total vs {header_table}",
                    f"{header.name} has no active source binding in this lake, so its "
                    f"table holds no control totals and its _v1 projection is not "
                    f"created. The satellites are deferred until Bronze can supply a "
                    f"change stream (3a section 3)",
                    inactive=True,
                )
                continue
            # a satellite's current row lives in the derived _v1 view, not the base table
            view = naming.v1_view(header_table) if header.kind in naming.SATELLITE_KINDS \
                else header_table
            control = header.accounting_map["control_total"]
            header_ref = naming.qualified(catalog, schema, view)
            line_totals = f"""WITH line_totals AS (
                    SELECT {journal_hk} AS journal_hk,
                           SUM(COALESCE({debit}, 0) + COALESCE({credit}, 0)) / 2 AS line_total
                    FROM {lines} WHERE {NOT_GHOST}
                    GROUP BY {journal_hk}
                )"""
            comparable = f"""
                SELECT h.{journal_hk} AS journal_hk, h.{control} AS control_total,
                       l.line_total
                FROM {header_ref} h
                JOIN line_totals l ON l.journal_hk = h.{journal_hk}
                WHERE h.is_current AND h.{control} IS NOT NULL
            """
            try:
                compared = spark.sql(
                    f"{line_totals} SELECT COUNT(*) AS n FROM ({comparable})"
                ).collect()[0]["n"]
            except Exception as exc:  # noqa: BLE001
                # A masked control total reads as NULL for an unprivileged identity, so a
                # failure here may mean the RUN-AS identity is not privileged under
                # mask_money -- the refresh-as-owner trap. Never "no problems found".
                report.problems.append(
                    f"{table}: control-total reconciliation against {view} could not run: "
                    f"{exc}. If this is a permissions error, check that the pipeline "
                    f"run-as identity is privileged under mask_money -- DEPLOY.md Phase 6b."
                )
                continue
            if not compared:   # nothing was actually compared -- see below
                report.skipped(
                    f"{table}: control total vs {view}",
                    "the join produced no journal carrying a non-null control total -- "
                    "the header table is empty or holds no control figures for these "
                    "journals, so nothing was actually compared",
                )
                continue
            report.asserted()
            total_off, off = count_and_sample(spark, f"""
                SELECT * FROM ({comparable})
                WHERE ABS(control_total - line_total) > {TOLERANCE}
            """, max_report, cte=line_totals)
            print(f"  {view}: {compared} control total(s) compared, "
                  f"{total_off} disagreeing")
            if total_off > len(off):
                report.problems.append(
                    f"{table}: {total_off} journals disagree with their {view} control "
                    f"total; the first {len(off)} are listed"
                )
            for row in off:
                report.problems.append(
                    f"{table}: journal {row['journal_hk']!r}: {view} control total "
                    f"{row['control_total']} but lines total {row['line_total']}"
                )

    # ---- 3. the line ordering column is dense and unique per journal ---------
    order_col = acct.get("line_order")
    if not order_col:
        report.skipped(
            f"{table}: line ordering",
            f"{entity.name} declares no accounting.line_order. Its transaction_key is "
            f"{list(entity.transaction_key)}, which is not a line ordinal -- asserting "
            f"density over it would assert something false about the source",
        )
    else:
        total_gapped, ordering = count_and_sample(spark, f"""
            SELECT {journal_hk} AS journal_hk,
                   COUNT(*)                       AS lines,
                   COUNT(DISTINCT {order_col})    AS distinct_orders,
                   MAX(CAST({order_col} AS INT))  AS max_order
            FROM {lines} WHERE {NOT_GHOST}
            GROUP BY {journal_hk}
            HAVING COUNT(*) <> COUNT(DISTINCT {order_col})
                OR MAX(CAST({order_col} AS INT)) <> COUNT(*)
        """, max_report)
        report.asserted()
        print(f"  journals with gapped or duplicated {order_col}: {total_gapped}")
        if total_gapped > len(ordering):
            report.problems.append(
                f"{table}: {total_gapped} journals have a gapped or duplicated "
                f"{order_col}; the first {len(ordering)} are listed"
            )
        for row in ordering:
            report.problems.append(
                f"{table}: journal {row['journal_hk']!r}: {row['lines']} lines but "
                f"{row['distinct_orders']} distinct {order_col} values, "
                f"max={row['max_order']}. {order_col} is a dependent child key -- a gap "
                f"means lines were lost, a duplicate means they were double-loaded."
            )

    # ---- 4. no ghost keys on real lines -------------------------------------
    # Counted on the visibility scan above, one aggregate per declared parent.
    for parent in entity.parents:
        parent_hk = naming.hk(parent)
        ghosts = visibility[f"ghost_{parent}"]
        report.asserted()
        if ghosts:
            report.problems.append(
                f"{table}: {ghosts} line(s) resolve {parent_hk} to the ghost key. A "
                f"reference arrived before its master, or the parent_keys mapping is wrong."
            )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument(
        "--entities", default="",
        help="comma-separated entity names to check; default is every entity whose "
             "metadata declares accounting debit and credit roles",
    )
    ap.add_argument(
        "--active-sources", default="",
        help="the target's active_sources value; empty means every binding is active",
    )
    ap.add_argument("--max-report", type=int, default=25)
    args = ap.parse_args()

    meta = Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)

# ---------------------------------------------------------------------------
# THE VACUITY RULE, IDENTICAL IN ALL FOUR GATES.
#
# A gate that asserted nothing must never print PASSED -- that is the outcome the NOT
# EVALUATED section exists to make visible, and printing success over it is how a green
# job comes to mean nothing. But the EXIT CODE has to distinguish two very different
# emptinesses, because one of them is today's declared state:
#
#   DECLARED DORMANT  every skip is attributable to a binding this lake declares
#                     inactive, and an --active-sources list was actually supplied.
#                     A stated absence. Exit 0, banner says NOT EVALUATED, never PASSED.
#
#   UNEXPLAINED       anything else -- no declared list, an empty ACTIVE table, a table
#                     that could not be read. Exit 1: the gate cannot account for why it
#                     proved nothing.
#
# Without the first case the aggregate gate fails every run until the payroll sources
# land -- and it sits ahead of apply_governance, assert_mask_survival and
# publish_model_metadata in the job, so it would block governance over a dormancy the
# 3a spec already states. Without the second, a misconfigured gate reports success.
# ---------------------------------------------------------------------------

    wanted = [n.strip() for n in args.entities.split(",") if n.strip()]
    entities = line_entities(model)
    if wanted:
        known = {e.name for e in entities}
        unknown = sorted(set(wanted) - known)
        if unknown:
            print(f"--entities names {unknown}, which declare no accounting debit/credit "
                  f"roles. Journal-line entities are {sorted(known)}.")
            return finish("FAILED", 0, 0, 1)
        entities = [e for e in entities if e.name in wanted]

    if not entities:
        # Not a pass. The gate exists because the journal domain is in the model; if the
        # model says no entity is a journal line, the metadata and this task disagree.
        print("no entity declares accounting debit/credit roles -- nothing to check. "
              "If the journal domain is deployed, its metadata is missing its "
              "accounting: block and this gate is asserting nothing.")
        return finish("FAILED", 0, 0, 1)

    print(f"{len(entities)} journal-line entit(ies) from metadata: "
          f"{', '.join(e.name for e in entities)}")

    # PYSPARK IS IMPORTED HERE, NOT AT MODULE LEVEL, and that is deliberate: it lets
    # tests/test_accelerator.py import this module with no Spark installed and assert
    # directly that line_entities / control_total_entities derive the right entities and
    # column names from the real metadata. The alternative -- a substring check over this
    # file's source -- would only ever verify that the words appear, not that the
    # derivation is right. Same reasoning, and the same precedent, as
    # checks/conformance_check.py's lazy databricks-sdk import.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    report = Report()
    for entity in entities:
        for src, table in entity.tables():
            check_entity(spark, args.catalog, args.schema, model, entity, src, table,
                         args.max_report, report, active)

    print("\n" + "=" * 68)
    if report.not_evaluated:
        print(f"NOT EVALUATED -- {len(report.not_evaluated)} propert(ies) could not be "
              f"asserted:")
        for n in report.not_evaluated:
            print(f"  ~ {n}")
        print()
    if report.problems:
        print(f"JOURNAL INTEGRITY GATE FAILED -- {len(report.problems)} problem(s):")
        for p in report.problems:
            print(f"  * {p}")
        return finish("FAILED", report.evaluated, len(report.not_evaluated), 1)
    if not report.evaluated:
        if active is not None and report.skipped_inactive == len(report.not_evaluated):
            print("JOURNAL INTEGRITY GATE NOT EVALUATED: no journal-line table has an "
                  "active source binding in this lake, so this run asserted nothing. "
                  "Dormant by declaration (active_sources), not passing.")
            return finish("NOT_EVALUATED", 0, len(report.not_evaluated), 0)
        print("JOURNAL INTEGRITY GATE FAILED: every property was skipped, so this run "
              "asserted nothing, and not every skip is explained by a declared-inactive "
              "binding. See NOT EVALUATED above -- an ACTIVE table that is empty, or one "
              "that could not be read, is a load problem, not a stated absence.")
        return finish("FAILED", 0, len(report.not_evaluated), 1)
    print(f"JOURNAL INTEGRITY GATE PASSED: {report.evaluated} propert(ies) asserted, "
          f"{len(report.not_evaluated)} NOT EVALUATED (listed above).")
    return finish("PASSED", report.evaluated, len(report.not_evaluated), 0)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
