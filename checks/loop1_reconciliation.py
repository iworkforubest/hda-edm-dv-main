"""
HARD GATE: loop-1 reconciliation. landed + (quarantined - superseded) must equal approved.

This is the check that makes quarantine meaningful. A load that reports success while
silently discarding rows is worse than a load that fails, so the identity is asserted
per manifest and the job fails on any unexplained variance.

THE SUBTRACTION, AND WHY IT IS BOUNDED
---------------------------------------
A reject that a later run legitimately re-accepted is recorded by
checks/supersede_quarantine.py in control.ctl_quarantine_superseded, and this gate
subtracts it from the quarantined count so a superseded reject is not double-counted
against the manifest that first rejected it. THIS CHANGE CAN ONLY EVER MAKE THE GATE
WEAKER: every other adjustment here adds a reason to fail, and this one subtracts from
the count a failure is measured against. If the subtraction is ever wrong or too large,
the gate can go GREEN on a REAL variance -- the exact failure mode quarantine exists to
catch. `over_subtracted()` below is the bound on that: a table where superseded records
exceed what was ever quarantined for a manifest is reported as its own finding, never
silently absorbed into the arithmetic.

WHICH TABLE IT RECONCILES, AND WHY THAT USED TO BE A TRAP
---------------------------------------------------------
`--entity` names the vault table(s) to reconcile, and resources/vault_job.yml now passes
it explicitly. It used to pass nothing, and this file defaulted to `nhl_timesheet_line` --
an entity whose only binding is STRIIVE_EU, inactive in the US lake. The gate therefore
reconciled a table nobody was loading, which "passed" on an empty table for as long as
that table existed, and began FAILING with TABLE_OR_VIEW_NOT_FOUND the moment the factory
stopped creating quarantine twins for tables with no active binding.

Both halves are fixed here, because either alone leaves the trap standing:

  * the job names an active table, so the gate asserts something real; and
  * the gate SKIPS any table with no active binding, announcing it as NOT EVALUATED. A
    wrong or stale default must not be able to fail a hard gate, and must not be able to
    pass one silently either.

Activity comes from the same declared list the generator uses (`--active-sources`, the
bundle's `active_sources` variable) and is resolved through
spec.active_table_bindings -- ONE definition of "inactive" shared with factory.build, not
a second one invented here.

THE MANIFEST IS THE CONTROL AND IS NEVER SKIPPED. If the approval manifest cannot be
read, the gate fails: everything below is a comparison against it, and a run that cannot
read it has asserted nothing at all.
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
GATE = "loop1_reconciliation"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


# DEF-48: the kinds for which `landed + (quarantined - superseded) = approved` is
# TRUE. The superseded term is a correction to the quarantined one and does not
# change which kinds the identity holds for.
#
# The identity assumes one landed row per approved source row. That holds for a
# transaction: an NHL, a link and a HAL each take the source row as it comes. It does NOT
# hold anywhere the loader legitimately writes fewer rows than it read:
#
#   hub    deduplicates. 4,444,172 GP rows become 2,759,294 staged rows and then
#          2,221,108 hub rows. A source count will never equal either, and should not.
#   sat    stores only rows whose hashdiff changed.
#
# The job has always passed --entity naming the three NHLs, so in practice the gate has
# only ever run where the identity holds. That was a CONVENTION in a parameter list, not
# a property: with --entity omitted, targets() returned every table including the hubs,
# and the gate would have reported false variances in the millions on correct data.
# Refused here instead, so the convention cannot be lost.
RECONCILABLE_KINDS = frozenset({"nhl", "link", "hal"})


def targets(model: spec.Model, named: list[str]):
    """[(entity, src, table)] to reconcile: the named tables, or every RECONCILABLE one.

    `table` in the returned tuples is ALWAYS THE PHYSICAL name (tables()) -- every SQL
    statement built from it below reads and writes the physical table (and its staging
    log / quarantine twin), never the view.

    `named` -- resources/vault_job.yml's --entity value -- is matched by the STABLE
    (unversioned) name instead. A job config is a durable reference: naming the
    physical `_rev1` table would need re-editing at every cutover, exactly what the
    unversioned stable view exists to prevent. tests/test_accelerator.py's "every
    entity the loop-1 task names is a table the metadata generates" check reads that
    same --entity list against e.stable_tables() -- if this resolver and that check
    ever disagree about which list --entity is drawn from, the check goes green while
    the gate SystemExits on the first real run. Keep both in sync.

    stable_tables() and tables() are the same length and in the same order BY
    CONSTRUCTION (spec.Entity._base_tables() is what both derive from), so the named
    stable name is paired with its physical table positionally, one zip, no name
    surgery.

    A deduplicating kind is excluded from the default set and REFUSED if named
    explicitly -- naming one is a mistake with a clear cause, and silently skipping it
    would leave the caller believing it had been reconciled.
    """
    all_tables = [
        (e, stable_src, stable, phys)
        for e in model.entities
        for (stable_src, stable), (_phys_src, phys) in zip(e.stable_tables(), e.tables())
    ]
    if not named:
        return [(e, src, phys) for e, src, _stable, phys in all_tables
                if e.kind in RECONCILABLE_KINDS]

    by_name = {stable: (e, src, phys) for e, src, stable, phys in all_tables}
    unknown = [n for n in named if n not in by_name]
    if unknown:
        raise SystemExit(
            f"--entity names {unknown}, which the metadata does not generate. Known "
            f"vault tables: {sorted(by_name)}"
        )
    wrong_kind = [(n, by_name[n][0].kind) for n in named
                  if by_name[n][0].kind not in RECONCILABLE_KINDS]
    if wrong_kind:
        raise SystemExit(
            f"--entity names {[n for n, _k in wrong_kind]}, of kind(s) "
            f"{sorted({k for _n, k in wrong_kind})}, for which landed + (quarantined "
            f"- superseded) = approved is NOT TRUE. A hub deduplicates and a satellite "
            f"stores only "
            f"changed rows, so the loader writes fewer rows than it read -- by design. "
            f"Reconciling one would report a variance in the millions on correct data. "
            f"Only {sorted(RECONCILABLE_KINDS)} take one row per approved source row."
        )
    return [by_name[n] for n in named]


def _over_subtracted_sort_key(row):
    """Sort key for a (manifest_id, quarantined, superseded) row.

    BRZ-12 means manifest_id can be None (an all-NULL twin) sitting alongside a
    real-keyed supersede row -- `USING (manifest_id)` does not match NULL to NULL, so
    a None-keyed row and a string-keyed row can land in the same `rows` list, and
    `None < "m1"` raises TypeError. Mirrors checks/supersede_quarantine.py's own
    `_sort_key` (not imported: that module already imports THIS one, so importing it
    back would be circular) -- the boolean isolates the None group first so
    manifest_id is only ever compared within a group where every value is the same
    type. Sorting here is only for stable output ordering, not for correctness.
    """
    manifest_id, quarantined, superseded = row
    return (manifest_id is None, manifest_id, quarantined, superseded)


def over_subtracted(rows) -> list[str]:
    """Manifests superseding more rejects than they quarantined. Pure.

    The identity is landed + (quarantined - superseded) = approved. If superseded exceeds
    quarantined the parenthesised term goes negative, and the gate can then pass on a real
    variance -- it would go green exactly when it should not be. That is a failure of the
    supersede mechanism, not a data variance, so it is reported separately and never
    silently absorbed into the arithmetic.
    """
    problems = []
    for manifest_id, quarantined, superseded in sorted(rows, key=_over_subtracted_sort_key):
        if superseded > quarantined:
            problems.append(
                f"manifest {manifest_id}: {superseded} superseded record(s) against "
                f"{quarantined} quarantined row(s). The identity's quarantined term goes "
                f"negative, so this gate could pass on a real variance. Investigate "
                f"checks/supersede_quarantine.py before trusting any loop-1 result."
            )
    return problems


def manifest_producer_exists(model: spec.Model, active) -> bool:
    """Could anything populate ctl_approval_manifest in this lake?

    True when ANY active reconcilable binding declares a manifest_column. The manifest is keyed
    on manifest_id and joined `USING (manifest_id)`, so a binding that stamps no manifest onto
    its rows can never contribute a row to it -- and if no active binding stamps one, nothing can
    write the manifest at all.

    MEASURED 29 Aug: all three active reconcilable bindings in usnc_tds
    (general_journal_line/GP_US, general_journal_line_closed_year/GP_US_HIST,
    journal_line/UKG_US) declare manifest_column=None. That is BRZ-12, and it is why the manifest
    is empty.

    This is what lets main() tell a DECLARED absence from a broken producer, and it is computed
    from the model rather than hardcoded, so it self-heals: the day an active binding gains a
    manifest_column, an empty manifest becomes a hard failure again with nobody having to
    remember to change it back.

    BUT NOT BY BRZ-12 ALONE, for these three. spec.validate REFUSES a reconcilable binding that
    declares both dedup_by and manifest_column (loop-1 compares a delivered count against what
    the loader accepted, and dedup makes those different numbers). All three active bindings here
    dedup, so stamping manifest_id onto their rows is necessary and NOT sufficient: loop-1 also
    needs a count taken AFTER deduplication.

    THAT COUNT IS OURS, NOT BRONZE'S -- corrected 31 Aug. An earlier version of this docstring
    said "ask Bronze for that count", and docs/bronze_layer_work_requests.html BRZ-12 was
    amended on 29 Aug to ask them for it. Both were wrong, and
    docs/loop1_control_table_request.html had already said so on 27 Aug: "the count it compares
    against has to be taken after our deduplication, which is ours to compute, not yours."
    dedup_by is OUR modelling choice -- (company, batchid, account, userdefined1) for UKG -- and
    asking a delivering system to count distinct values of a key we chose couples their delivery
    to our internal model. What Bronze owes is the manifest_id stamp; the post-dedup count is
    computed here, from the staging log.
    """
    for entity, src, _table in targets(model, []):
        # targets() yields src=None for the DEFAULT set, so the binding must come from
        # active_table_bindings -- which is the single definition of "active" anyway, and returns
        # the bindings that actually load into this table.
        for binding in spec.active_table_bindings(entity, src, active):
            if binding.manifest_column:
                return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument(
        "--entity", default="",
        help="comma-separated vault table name(s) to reconcile; default is every table "
             "the metadata generates, minus those with no active source binding",
    )
    ap.add_argument(
        "--active-sources", default="",
        help="the target's active_sources value; empty means every binding is active",
    )
    ap.add_argument("--manifest-table", default=None)
    ap.add_argument(
        "--control-schema", required=True,
        help="schema holding control.ctl_quarantine_superseded, the record of rejects "
             "a later run accepted -- loop-1 subtracts these from the quarantined count "
             "so a superseded reject is not double-counted against the manifest that "
             "first rejected it",
    )
    args = ap.parse_args()

    meta = Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)
    named = [n.strip() for n in args.entity.split(",") if n.strip()]

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # DEF-55: the manifest lives in the CONTROL schema, not in governance. governance
    # holds masks and grants; control holds load control and the audit, and
    # governance/control_objects.sql creates ctl_approval_manifest there. This default was
    # the last reference left pointing at the old home -- and being a DEFAULT it would
    # have surfaced only as "the approval manifest could not be read" at run time.
    #
    # --manifest-table still overrides it, and deliberately: it is what lets the manifest
    # live in another catalog later without a code change. The schema name in the
    # default above is STILL a literal, not args.control_schema: this default predates
    # --control-schema, --manifest-table remains its own override/seam, and the two
    # knobs are deliberately independent. --control-schema exists only to locate
    # control.ctl_quarantine_superseded, below, for the supersede subtraction.
    manifest = args.manifest_table or f"{args.catalog}.control.ctl_approval_manifest"

    problems: list[str] = []
    not_evaluated: list[str] = []
    skipped_inactive = 0
    reconciled = 0

    # ---- the control itself --------------------------------------------------
    try:
        approved = spark.sql(f"SELECT COUNT(*) AS n FROM {manifest}").collect()[0]["n"]
    except Exception as exc:  # noqa: BLE001
        print(f"LOOP-1 GATE FAILED -- the approval manifest could not be read: {exc}")
        print(f"  {manifest} is the control every comparison below is made against. A "
              f"run that cannot read it has asserted nothing.")
        return finish("FAILED", 0, 0, 1)
    print(f"{manifest}: {approved} manifest row(s)")
    if not approved:
        # DEF-48: an EMPTY manifest is not a quiet baseline, it is a gate that cannot
        # fail. `expected` would be empty, every variance query would return no rows,
        # `reconciled` would still increment per table, and the run would print PASSED
        # having compared nothing. Creating this table without populating it would have
        # converted a loud, honest failure into a silent green one.
        # DEF-48 STANDS: an empty manifest never reads as PASSED. But WHY it is empty decides
        # between a declared absence and a broken producer, and the gate now asks instead of
        # assuming -- the same distinction supersede_quarantine draws with its empty twin.
        if not manifest_producer_exists(model, active):
            print(f"LOOP-1 GATE NOT EVALUATED -- {manifest} holds no rows, and nothing in this "
                  f"lake can write one.")
            print( "  No ACTIVE reconcilable binding declares a manifest_column, so no row "
                   "carries a manifest_id to group by and no producer can exist (BRZ-12). This "
                   "is a declared absence, not a silent pass: the identity was NOT checked, and "
                   "this gate asserts nothing until Bronze stamps the four feeds (BRZ-12). For the three feeds that dedup on load a stamped id is necessary and not sufficient -- loop-1 also "
                   "needs a count taken after deduplication, and THAT COUNT IS OURS to compute from the staging log, not Bronze's to supply.")
            return finish("NOT_EVALUATED", 0, 1, 0)
        print(f"LOOP-1 GATE FAILED -- {manifest} exists but holds NO manifest rows.")
        print( "  Every comparison below joins to it, so an empty control means every "
               "table would reconcile trivially and this run would report PASSED having "
               "asserted nothing. An active binding DOES declare a manifest_column, so a "
               "producer should have written here and did not.")
        return finish("FAILED", 0, 0, 1)

    for entity, src, table in targets(model, named):
        if not spec.active_table_bindings(entity, src, active):
            skipped_inactive += 1
            not_evaluated.append(
                f"{table}: no active source binding in this lake, so nothing landed and "
                f"nothing was rejected. The factory emits no quarantine twin for it "
                f"either -- there is no identity to assert, not an identity that holds."
            )
            continue

        # DEF-42: for a STAGED kind, "landed" is the staging log, not the vault table.
        # Every approved row lands in the log; the hub is a deduplicated projection of
        # it, so counting the hub would report fewer rows than were approved and the
        # arithmetic would fail on correct behaviour -- 538,186 short on
        # hub_accounting_journal alone. The quarantine twin is unchanged: it is named
        # from the logical table, so it is qtn_accounting_journal either way.
        landed_table = naming.pipeline_table(entity.kind, table)
        landed = f"{args.catalog}.{args.schema}.{landed_table}"
        # qtn_name is the BARE twin name, as checks/supersede_quarantine.py writes it
        # into ctl_quarantine_superseded.table_name -- derived by the SAME expression as
        # `quarantined` below (and as supersede_quarantine.py's own twin_table), so both
        # sides name the twin identically. It is not itself a table reference.
        qtn_name = f"qtn_{table.split('_', 1)[1]}"
        quarantined = f"{args.catalog}.{args.schema}.{qtn_name}"
        superseded_table = f"{args.catalog}.{args.control_schema}.ctl_quarantine_superseded"

        # THE OVER-SUBTRACTION BOUND, run BEFORE the identity is evaluated and against
        # its OWN unfiltered query -- not derived from the variance rows below. Deriving
        # it from those rows would miss exactly the dangerous case: a manifest whose
        # over-subtraction makes the identity balance BY COINCIDENCE, which is the gate
        # going green on a real variance, which is what this bound exists to catch.
        # FULL OUTER JOIN (not LEFT JOIN off quarantined) so a manifest with a
        # superseded record but ZERO quarantined rows for this table -- the most
        # extreme over-subtraction -- is not silently invisible to this query either.
        try:
            supersede_rows = spark.sql(f"""
                WITH rejected AS (
                    SELECT manifest_id, COUNT(*) AS n FROM {quarantined} GROUP BY manifest_id
                ),
                superseded AS (
                    SELECT manifest_id, COUNT(*) AS n
                    FROM {superseded_table}
                    WHERE table_name = '{qtn_name}'
                    GROUP BY manifest_id
                )
                SELECT COALESCE(r.manifest_id, s.manifest_id) AS manifest_id,
                       COALESCE(r.n, 0) AS quarantined,
                       COALESCE(s.n, 0) AS superseded
                FROM rejected r
                FULL OUTER JOIN superseded s USING (manifest_id)
            """).collect()
        except Exception as exc:  # noqa: BLE001
            not_evaluated.append(
                f"{table}: the supersede-bound check could not run: {exc}. Without it an "
                f"over-subtraction against {quarantined} could pass a real variance "
                f"silently, so this table is not counted as reconciled."
            )
            continue

        over = over_subtracted(
            [(row["manifest_id"], row["quarantined"], row["superseded"])
             for row in supersede_rows]
        )
        if over:
            problems.extend(f"{table}: {o}" for o in over)
            continue

        try:
            variance = spark.sql(f"""
                WITH expected AS (
                    SELECT manifest_id, approved_count FROM {manifest}
                ),
                landed AS (
                    SELECT manifest_id, COUNT(*) AS n FROM {landed} GROUP BY manifest_id
                ),
                rejected AS (
                    SELECT manifest_id, COUNT(*) AS n FROM {quarantined} GROUP BY manifest_id
                ),
                superseded AS (
                    SELECT manifest_id, COUNT(*) AS n
                    FROM {superseded_table}
                    WHERE table_name = '{qtn_name}'
                    GROUP BY manifest_id
                )
                SELECT e.manifest_id,
                       e.approved_count,
                       COALESCE(l.n, 0) AS landed,
                       COALESCE(r.n, 0) AS quarantined,
                       COALESCE(s.n, 0) AS superseded
                FROM expected e
                LEFT JOIN landed     l USING (manifest_id)
                LEFT JOIN rejected   r USING (manifest_id)
                LEFT JOIN superseded s USING (manifest_id)
                WHERE COALESCE(l.n, 0) + COALESCE(r.n, 0) - COALESCE(s.n, 0)
                      <> e.approved_count
            """).collect()
        except Exception as exc:  # noqa: BLE001
            # An ACTIVE table that cannot be read is reported, never swallowed -- but it
            # is reported as unevaluated rather than as a variance, because no
            # reconciliation was performed. The commonest cause is pointing --schema at
            # the wrong vault layer: a csat lives in business_vault, not raw_vault.
            not_evaluated.append(
                f"{table}: could not be reconciled: {exc}. It has an active binding, so "
                f"this is not an expected absence -- check --schema names the layer this "
                f"entity is generated into, and that the load actually ran."
            )
            continue

        # DEF-48: THE OTHER HALF OF THE VACUITY. The variance query drives FROM the
        # manifest, so a row whose manifest_id is NULL -- or names a manifest the control
        # does not hold -- is never compared at all. It is not reported as a variance; it
        # is simply absent, while the table still counts as reconciled.
        #
        # Measured 26 Aug 2026: nhl_general_journal_line holds 2,453,132 rows and ZERO
        # non-null manifest_id, because none of the four active bronze feeds supplies one.
        # Without this check, populating the manifest table would turn the gate green
        # while every vault row sat outside the comparison.
        try:
            unaccounted = spark.sql(f"""
                SELECT
                  (SELECT COUNT(*) FROM {landed}
                    WHERE manifest_id IS NULL
                       OR manifest_id NOT IN (SELECT manifest_id FROM {manifest}))
                  AS landed_unaccounted,
                  (SELECT COUNT(*) FROM {quarantined}
                    WHERE manifest_id IS NULL
                       OR manifest_id NOT IN (SELECT manifest_id FROM {manifest}))
                  AS quarantined_unaccounted
            """).collect()[0]
        except Exception as exc:  # noqa: BLE001
            not_evaluated.append(
                f"{table}: the unaccounted-rows check could not run: {exc}. Without it a "
                f"reconciliation over this table cannot be trusted, so it is not counted "
                f"as reconciled."
            )
            continue

        n_un = (unaccounted["landed_unaccounted"] or 0) + \
               (unaccounted["quarantined_unaccounted"] or 0)
        if n_un:
            problems.append(
                f"{table}: {n_un} row(s) belong to NO manifest in {manifest} "
                f"({unaccounted['landed_unaccounted']} landed, "
                f"{unaccounted['quarantined_unaccounted']} quarantined). They carry a "
                f"NULL manifest_id or one the control does not hold, so they are outside "
                f"the reconciliation entirely -- not a variance, simply not compared. "
                f"The source binding must supply a manifest column."
            )

        reconciled += 1
        print(f"  {table}: {len(variance)} manifest(s) out of balance, "
              f"{n_un} row(s) unaccounted")
        for row in variance[:25]:
            problems.append(
                f"{table} / {row['manifest_id']}: approved={row['approved_count']} "
                f"landed={row['landed']} quarantined={row['quarantined']} "
                f"superseded={row['superseded']}"
            )

    print("\n" + "=" * 68)
    if not_evaluated:
        print(f"NOT EVALUATED -- {len(not_evaluated)} table(s):")
        for n in not_evaluated:
            print(f"  ~ {n}")
        print()
    if problems:
        print(f"LOOP-1 GATE FAILED -- {len(problems)} manifest(s) do not reconcile:")
        for p in problems:
            print(f"  * {p}")
        return finish("FAILED", reconciled, len(not_evaluated), 1)
    if not reconciled:
    # THE VACUITY RULE, IDENTICAL IN ALL FOUR GATES: a run that asserted nothing never
    # prints PASSED. It exits 0 only when EVERY skip is explained by a binding this lake
    # declares inactive AND an --active-sources list was supplied -- a stated absence.
    # Anything else (no declared list, an empty ACTIVE table, a table that could not be
    # read) exits 1, because the gate cannot account for why it proved nothing. See
    # checks/journal_integrity_check.py for the reasoning in full.
    #
    # checks/supersede_quarantine.py prints the same GATE SUMMARY line and admits ONE
    # extra dormancy on top of this rule -- an active table whose quarantine twin holds
    # no rows -- and states the difference in full at its own vacuity rule. It is an
    # extra declared dormancy, not a weaker treatment of failure: a table that could not
    # be read is a FAILURE there as it is here.
        if active is not None and skipped_inactive == len(not_evaluated):
            print("LOOP-1 GATE NOT EVALUATED: every table named or declared has no "
                  "active source binding in this lake, so this run asserted nothing. "
                  "Dormant by declaration (active_sources), not passing.")
            return finish("NOT_EVALUATED", 0, len(not_evaluated), 0)
        print("LOOP-1 GATE FAILED: no table was reconciled, so this run asserted "
              "nothing, and not every skip is explained by a declared-inactive binding. "
              "Name an active table with --entity, or check that --active-sources "
              "matches the deployed target.")
        return finish("FAILED", 0, len(not_evaluated), 1)
    print(f"LOOP-1 GATE PASSED: landed + (quarantined - superseded) = approved for "
          f"every manifest, across {reconciled} table(s); {len(not_evaluated)} NOT "
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
