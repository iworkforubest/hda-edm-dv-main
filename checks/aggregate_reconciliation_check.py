"""
HARD GATE: every aggregate reconciles to the transaction-grain data beneath it.

WHY THIS EXISTS
---------------
The GL journal is an aggregate: one line carries every worker's HSA deduction for a pay
batch summed into a single figure. The payroll register is the transaction grain: one row
per worker, per period, per code.

Both are loaded faithfully, and both are correct in their own terms. The risk is that they
silently stop agreeing -- a code remapped to a different ledger account, a late correction
posted to the register but not to the GL, a worker excluded from one feed and not the
other. Nothing about either table's shape reveals that, and the numbers stay plausible.

So the reconciliation is the control: rolled up by mapped ledger account and period, the
raw detail must equal the journal line. When it does not, one of them is wrong and Finance
needs to know which before anything is reported.

METADATA-DRIVEN, NOT HARDCODED
------------------------------
The pairs come from the model: any entity with grain: aggregate declares aggregates_from
and aggregate_drops. Add a second aggregate later and this check covers it without being
edited -- which is the point of putting grain in the metadata rather than in a comment.

The AMOUNT COLUMNS come from the model too, off the aggregate entity's `accounting:`
block. They used to be literals -- `debit_amount` and `credit_amount`, from the retired
Workday-shaped binding -- while nhl_journal_line actually delivers `debit` and `credit`.
Nothing noticed, because this pair is dormant: its raw counterpart has no active source.
It would have failed on a column that does not exist the first day it woke up.

TWO SCHEMAS, NOT ONE
--------------------
The classification satellite is a `csat`: it is computed FROM the Raw Vault and is written
by the SECOND pipeline into `business_vault`, while the aggregate and its raw counterpart
live in `raw_vault`. This check was pointed at one schema for all three and would have
failed looking for the csat in the wrong place -- separately from, and in addition to, the
activity problem below. `--business-vault-schema` names the second one.

KNOWN DEFECT -- THIS GATE IS NOT CORRECT WHEN IT WAKES UP. READ BEFORE ACTIVATING.
-----------------------------------------------------------------------------------
Recorded, not fixed, because the pair is dormant under usnc_tds (see ACTIVE SOURCES
below) and fixing it needs data nobody can look at yet. Whoever activates
payroll_detail/UKG_US and payroll_line_classification/BUSINESS_VAULT owns this:

  1. THE VARIANCE JOIN CANNOT MATCH. `ON d.ledger_account = j.ledger_account_hk` joins
     the classification satellite's mapped ledger account -- a STRING, the account
     reference as the mapping rule produced it -- to the journal line's
     ledger_account_hk, which is a BINARY(32) hash key. The two are never equal, so
     `detail_total` is always NULL, every journal bucket falls outside TOLERANCE, and the
     gate reports "aggregate of nothing" for every single line. It will look like a
     catastrophic reconciliation failure and it will be this join.
     The fix is to hash the mapped account the way hub_ledger_account's own loader does
     (spec.hub_key_components + hashing.hash_key, company-scoped -- DEF-21), or to carry
     the hash key on the classification satellite so no re-derivation is needed. The
     second is preferable: a re-derivation that drifts from the hub is the silent-FK
     failure spec.parent_key_components exists to prevent.
  2. THE PAY PERIOD IS GROUPED AND NEVER USED. `pay_period_hk` is in the `detail` CTE's
     GROUP BY and appears in no join or filter, so detail rows from DIFFERENT pay periods
     are compared against one journal bucket. Either join it (the aggregate declares
     pay_period as a parent) or drop it from the grouping -- carrying it and ignoring it
     is how a period-crossing variance hides.
  3. `nhl_payroll_detail.amount` is still a literal in this file's SQL, unlike the
     aggregate's own debit/credit columns, which now come from its accounting: block.
     Declaring a role for it means a new entry in spec.ACCOUNTING_ROLES and a metadata
     change to an entity with no live source -- deferred deliberately.

ACTIVE SOURCES
--------------
A pair whose aggregate, raw counterpart or classification satellite has no active source
binding in this lake is NOT EVALUATED, announced by name. Under usnc_tds all three are
inactive: `nhl_payroll_detail` is a ghost-only table with no `amount` column, and
`csat_payroll_line_classification` has no `_v1` projection at all, because the factory
creates neither for a table nothing loads into. Both landed in the exception handler
below, which reported a problem, failed the gate, and blocked apply_governance,
assert_mask_survival and publish_model_metadata behind it in the job.

Activity is resolved through spec.active_table_bindings -- the same function
factory.build emits from, and the same one loop1_reconciliation.py,
mask_survival_check.py and journal_integrity_check.py use. Four gates, one definition.

WHAT IT ASSERTS, per (period, ledger account)
  1. SUM(raw detail) = the journal line amount, within tolerance.
  2. No journal line has no detail behind it at all (an aggregate of nothing).
  3. No detail rolls up to a ledger account that appears in no journal line.
  4. The mapping used is the one the Business Vault computed, not one inferred here --
     so a mapping change shows up as a reconciliation break rather than being absorbed.
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

# Ledger amounts allow 3 fraction digits, transaction amounts 6, and an aggregate of many
# rows accumulates rounding. Per-line tolerance times line count would be too loose, so a
# flat tolerance per (period, account) bucket is used and stated.
TOLERANCE = "0.05"


def reconcile(spark, catalog: str, schema: str, business_schema: str,
              agg: spec.Entity, raw: spec.Entity, cls: spec.Entity,
              max_report: int) -> list[str]:
    problems: list[str] = []
    acct = agg.accounting_map
    debit, credit = acct["debit"], acct["credit"]
    agg_t = naming.qualified(catalog, schema, agg.base_table)
    raw_t = naming.qualified(catalog, schema, raw.base_table)
    # the csat is written by the Business Vault pipeline, into its own schema
    cls_t = naming.qualified(catalog, business_schema, naming.v1_view(cls.base_table))

    print(f"\nreconciling {agg.base_table}  <-  {raw.base_table}")
    print(f"  aggregation drops: {', '.join(agg.aggregate_drops)}")
    print(f"  amount columns from metadata: {debit} / {credit}")
    print(f"  classification: {cls_t}")

    # The mapped ledger account comes from the Business Vault classification, so a change
    # to the mapping rule surfaces here instead of being silently absorbed.
    rolled_up = f"""
        SELECT c.mapped_ledger_account AS ledger_account,
               d.pay_period_hk,
               SUM(d.amount) AS detail_total,
               COUNT(*)      AS detail_lines
        FROM {raw_t} d
        JOIN {cls_t} c
          ON c.payroll_detail_hk = d.payroll_detail_hk
         AND c.is_current
        GROUP BY c.mapped_ledger_account, d.pay_period_hk
    """

    try:
        variance = spark.sql(f"""
            WITH detail AS ({rolled_up}),
            journal AS (
                SELECT ledger_account_hk,
                       accounting_journal_hk,
                       SUM(COALESCE({debit}, 0) + COALESCE({credit}, 0)) AS journal_total,
                       COUNT(*) AS journal_lines
                FROM {agg_t}
                GROUP BY ledger_account_hk, accounting_journal_hk
            )
            SELECT j.ledger_account_hk,
                   j.accounting_journal_hk,
                   j.journal_total,
                   COALESCE(d.detail_total, 0) AS detail_total,
                   COALESCE(d.detail_lines, 0) AS detail_lines
            FROM journal j
            -- KNOWN DEFECT, see the header: d.ledger_account is the mapped account
            -- STRING and j.ledger_account_hk is a BINARY(32) hash key. This join can
            -- never match, so every bucket reports "aggregate of nothing" the day this
            -- pair activates. Do not treat that output as a reconciliation failure.
            LEFT JOIN detail d
              ON d.ledger_account = j.ledger_account_hk
            WHERE ABS(j.journal_total - COALESCE(d.detail_total, 0)) > {TOLERANCE}
        """).collect()
    except Exception as exc:  # noqa: BLE001
        # Amounts on both sides are masked. If the run-as identity is not privileged under
        # mask_money, every figure reads NULL and this check would "pass" on nothing --
        # so an error here is reported as a failure, never swallowed.
        return [
            f"reconciliation of {agg.base_table} could not run: {exc}. If this is a "
            f"permissions error, the pipeline run-as identity is probably not privileged "
            f"under mask_money and every amount is reading as NULL -- DEPLOY.md Phase 6b."
        ]

    print(f"  buckets out of tolerance: {len(variance)}")
    for row in variance[:max_report]:
        if row["detail_lines"] == 0:
            problems.append(
                f"journal {row['accounting_journal_hk']!r} account "
                f"{row['ledger_account_hk']!r} totals {row['journal_total']} but NO payroll "
                f"detail rolls up to it. An aggregate of nothing: either the register feed "
                f"is missing rows, or the code-to-account mapping changed."
            )
        else:
            problems.append(
                f"journal {row['accounting_journal_hk']!r} account "
                f"{row['ledger_account_hk']!r}: journal {row['journal_total']} vs detail "
                f"{row['detail_total']} over {row['detail_lines']} lines "
                f"(difference {float(row['journal_total']) - float(row['detail_total']):.2f})"
            )

    # detail that reaches no journal line at all -- the opposite direction, equally wrong
    try:
        orphan = spark.sql(f"""
            WITH detail AS ({rolled_up})
            SELECT d.ledger_account, d.detail_total, d.detail_lines
            FROM detail d
            LEFT ANTI JOIN (SELECT DISTINCT ledger_account_hk FROM {agg_t}) j
              ON d.ledger_account = j.ledger_account_hk
        """).collect()
        print(f"  detail buckets with no journal line: {len(orphan)}")
        for row in orphan[:max_report]:
            problems.append(
                f"account {row['ledger_account']!r}: {row['detail_lines']} payroll detail "
                f"line(s) totalling {row['detail_total']} reach no journal line. Money was "
                f"paid and never posted to the GL."
            )
    except Exception as exc:  # noqa: BLE001
        problems.append(f"orphan-detail check could not run: {exc}")

    return problems


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
GATE = "aggregate_reconciliation"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def inactive_reason(model: spec.Model, entity: spec.Entity, active) -> str:
    """Why this entity cannot be read here, or "" if every one of its tables loads."""
    dead = [table for src, table in entity.tables()
            if not spec.active_table_bindings(entity, src, active)]
    if not dead:
        return ""
    return (f"{entity.base_table} has no active source binding in this lake "
            f"({', '.join(dead)} is created from its ghost flow alone, with no payload "
            f"column and no _v1 projection)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument(
        "--business-vault-schema", default="business_vault",
        help="the csat classification satellite is computed FROM the Raw Vault by the "
             "second pipeline, so it lives in its own schema, not in --schema",
    )
    ap.add_argument("--classification-table", default="csat_payroll_line_classification")
    ap.add_argument(
        "--active-sources", default="",
        help="the target's active_sources value; empty means every binding is active",
    )
    ap.add_argument("--max-report", type=int, default=25)
    args = ap.parse_args()

    meta = Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)

    aggregates = [e for e in model.entities if e.grain == "aggregate"]
    if not aggregates:
        # Not a pass. The model is supposed to carry the GL journal as an aggregate; if
        # it declares none, the metadata and this gate disagree about the model.
        print("no aggregate-grain entity is declared, so this gate asserted nothing. "
              "The GL journal is an aggregate of the payroll register -- if that "
              "labelling has been removed, the reconciliation control has been removed "
              "with it.")
        return finish("FAILED", 0, 0, 1)
    print(f"{len(aggregates)} aggregate/transaction pair(s) declared in metadata")

    cls = next((e for e in model.entities
                if e.base_table == args.classification_table), None)
    if cls is None:
        print(f"--classification-table {args.classification_table!r} is not an entity "
              f"the metadata generates. Known csat tables: "
              f"{sorted(e.base_table for e in model.entities if e.kind == 'csat')}")
        return finish("FAILED", 0, 0, 1)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    problems: list[str] = []
    not_evaluated: list[str] = []
    skipped_inactive = 0
    reconciled = 0

    for agg in aggregates:
        raw = model.get(agg.aggregates_from)
        # Every one of the three tables must be loading, or there is nothing to compare.
        blocked = [inactive_reason(model, e, active) for e in (agg, raw, cls)]
        blocked = [b for b in blocked if b]
        if blocked:
            skipped_inactive += 1
            not_evaluated.append(
                f"{agg.base_table} <- {raw.base_table}: " + "; ".join(blocked) +
                ". Not a reconciliation that holds -- a reconciliation that was not made."
            )
            continue
        if not {"debit", "credit"} <= set(agg.accounting_map):
            not_evaluated.append(
                f"{agg.base_table} <- {raw.base_table}: the aggregate declares no "
                f"accounting debit/credit roles, so this gate cannot know which columns "
                f"carry the amount it is meant to reconcile."
            )
            continue
        reconciled += 1
        problems += reconcile(
            spark, args.catalog, args.schema, args.business_vault_schema,
            agg, raw, cls, args.max_report,
        )

    print("\n" + "=" * 70)
    if not_evaluated:
        print(f"NOT EVALUATED -- {len(not_evaluated)} pair(s):")
        for n in not_evaluated:
            print(f"  ~ {n}")
        print()
    if problems:
        print(f"AGGREGATE RECONCILIATION FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        print("\nThis is an accounting discrepancy between the payroll register and the GL, "
              "not a data-quality nuance. Escalate to Finance before anything is reported.")
        return finish("FAILED", reconciled, len(not_evaluated), 1)
    if not reconciled:
    # THE VACUITY RULE, IDENTICAL IN ALL FOUR GATES: a run that asserted nothing never
    # prints PASSED. It exits 0 only when EVERY skip is explained by a binding this lake
    # declares inactive AND an --active-sources list was supplied -- a stated absence.
    # Anything else (no declared list, an empty ACTIVE table, a table that could not be
    # read) exits 1, because the gate cannot account for why it proved nothing. See
    # checks/journal_integrity_check.py for the reasoning in full.
        if active is not None and skipped_inactive == len(not_evaluated):
            # TODAY'S STATE under usnc_tds, and the reason this branch exists at all:
            # the payroll pair is deferred with the satellites (3a section 3.1), this
            # task sits ahead of apply_governance / assert_mask_survival /
            # publish_model_metadata in the job, and failing here would block governance
            # over a dormancy the spec already states.
            print("AGGREGATE RECONCILIATION GATE NOT EVALUATED: no declared pair has an "
                  "active source binding on all three of its tables, so this run "
                  "asserted nothing. Dormant by declaration (active_sources), not "
                  "passing -- 3a section 3.1 defers the payroll sources.")
            return finish("NOT_EVALUATED", 0, len(not_evaluated), 0)
        print("AGGREGATE RECONCILIATION FAILED: every declared pair was skipped, so this "
              "run asserted nothing, and not every skip is explained by a "
              "declared-inactive binding. Pass --active-sources so a deliberate "
              "deferral is a stated absence rather than an unexplained silence.")
        return finish("FAILED", 0, len(not_evaluated), 1)
    print(f"AGGREGATE RECONCILIATION PASSED: every GL journal line agrees with the payroll "
          f"detail beneath it, across {reconciled} pair(s); {len(not_evaluated)} NOT "
          f"EVALUATED (listed above).")
    return finish("PASSED", reconciled, len(not_evaluated), 0)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
