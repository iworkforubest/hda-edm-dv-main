"""
THE STABLE VIEW FOR PIPELINE-OWNED TABLES: link, NHL, HAL.

Spec 4.4 / 6.5. A hub or a satellite needs anti-join dedup or hashdiff change
detection, which a streaming table cannot express, so it is written by a BATCH LOADER
(checks/load_hubs.py, checks/load_satellites.py) and those two publish the stable,
unversioned view over what they build. A link, an NHL or a HAL is append-only by
nature, so the SDP pipeline writes it directly -- and until this task, NOTHING
published a stable view for those three kinds. Eleven entities, including the estate's
two largest tables (nhl_general_journal_line_closed_year, 14.6M rows;
nhl_general_journal_line, 2.45M), were named by the data contract, the DBML diagram and
the source-to-target mapping as though a view existed. It did not: the name resolved to
whatever object held it, which after a pipeline rebuild is the ORPHANED pre-migration
table -- frozen and silently stale. This closes that gap.

ONE RULE, THREE READERS. accelerator.stable_views.stable_view_action() and
stable_view_sql() are the SAME functions checks/load_hubs.py and
checks/load_satellites.py call -- moved there rather than copied a third time (R1).
The three cases are unchanged:
  view absent           -> create it pointing at the table the pipeline just built
  view points here       -> leave it
  view points ELSEWHERE -> LEAVE IT, and report both names
THIS TASK BOOTSTRAPS; IT NEVER MOVES A VIEW. Only checks/cutover_vault_version.py
repoints one that already exists and points elsewhere -- that is a deliberate cutover
or a deliberate rollback, made there, deliberately, with its own gating.

SCOPE: naming.GENERATABLE minus naming.STAGED_KINDS, DERIVED, not the hand-typed
{"link", "nhl", "hal"} -- a kind added to either set moves this task's scope with it
rather than needing someone to remember a third place to update it. It must NOT touch
a staged kind: load_hubs.py and load_satellites.py already publish those, and two
writers for one object is how a rolled-back cutover gets undone by whichever runs last.

INACTIVE TABLES ARE SKIPPED, LIKE checks/load_satellites.py's OWN GUARD.
checks/mask_survival_check.py's INACTIVE guard (`spec.active_table_bindings`) applies
uniformly across every kind, staged or not: a table with no active source binding is
built from its ghost flow alone (hash key and system columns, no payload), and nothing
here publishes a view or writes an audit row for it -- there is nothing to expose and
nothing that loaded.

WHY staged=0, accepted=0 IN THE AUDIT ROW. This task writes no row into any vault
table -- the SDP pipeline already did that, in its own run, and that arithmetic is the
pipeline's concern, not this one's. Recording a row count here would either duplicate
the pipeline's own count (a second source of truth for a fact this task never measured)
or require an extra COUNT(*) against a table this task has no other reason to read. The
row this task writes is not a load record, it is COVERAGE: proof that the stable view
for this table was considered this run, which is exactly the evidence
checks/freshness_check.py and checks/cutover_vault_version.py --gated-by-run need and
could not see at all before this task existed. audit.check_arithmetic(0, 0, {}) balances
trivially, so this never contributes a false discard.
"""

from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import audit, naming, spec  # noqa: E402
from accelerator.stable_views import stable_view_action, stable_view_sql  # noqa: E402

GATE = "publish_stable_views"

# R2: derived, not hand-typed. Today this is {hal, link, nhl}; a kind added to
# naming.GENERATABLE without a batch loader of its own falls in here automatically,
# and a kind moved INTO naming.STAGED_KINDS falls back out, because load_hubs.py or
# load_satellites.py would then own its view instead.
PIPELINE_OWNED_KINDS = frozenset(naming.GENERATABLE) - naming.STAGED_KINDS


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def pipeline_owned_entities(model) -> list:
    """The entities THIS task publishes a stable view for, in table order.

    Derived by subtraction (PIPELINE_OWNED_KINDS), never a literal kind tuple -- see
    the module docstring's R2 note.
    """
    return sorted(
        (e for e in model.entities if e.kind in PIPELINE_OWNED_KINDS),
        key=lambda e: e.base_table,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="raw_vault")
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the load audit. Required, not defaulted: a "
                         "default would let the audit silently go somewhere else.")
    ap.add_argument("--job-run-id", required=True,
                    help="{{job.run_id}}, the key tying one run's audit rows together")
    ap.add_argument(
        "--active-sources", default="",
        help="the target's active_sources value; empty means every binding is active",
    )
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="render the statements without a workspace")
    args = ap.parse_args()

    meta = Path(args.metadata) if args.metadata else (
        Path(__file__).resolve().parents[1] / "metadata" / "entities")
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)
    entities = pipeline_owned_entities(model)

    if not entities:
        # Not a quiet success: an empty PIPELINE_OWNED_KINDS means every generatable
        # kind is now staged, and the gap this task exists to close no longer applies
        # to anything -- worth seeing, not worth hiding behind a green run that looks
        # identical to "ran and did nothing".
        print("GATE NOT EVALUATED: naming.GENERATABLE minus naming.STAGED_KINDS is "
              "empty -- no pipeline-owned kind is declared in the model.")
        return finish("NOT_EVALUATED", 0, 0, 1)

    if args.dry_run:
        for e in entities:
            for src, table in e.tables():
                stable = naming.stable(table)
                print(f"\n-- {stable} (over {table})")
                print(stable_view_sql(args.catalog, args.schema, stable, table) + ";")
        print(f"\ndry run only -- {len(entities)} pipeline-owned entity/ies, nothing "
              f"executed")
        return 0

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    published, skipped, elsewhere, failed = 0, [], [], []
    for e in entities:
        for src, table in e.tables():
            if not spec.active_table_bindings(e, src, active):
                skipped.append(f"{table}: no active source binding in this lake -- "
                                f"built from its ghost flow alone, nothing to publish")
                continue
            stable = naming.stable(table)
            try:
                view_rows = spark.sql(
                    f"SELECT view_definition FROM `{args.catalog}`.information_schema"
                    f".views WHERE table_schema = '{args.schema}' AND table_name = "
                    f"'{stable}'"
                ).collect()
                view_exists = bool(view_rows)
                view_definition = (
                    view_rows[0]["view_definition"] if view_rows else "")
                action = stable_view_action(view_exists, view_definition, table)
                if action == "elsewhere":
                    print(f"  LEFT ALONE {stable}: it already points elsewhere, not "
                          f"at {table} -- current definition: {view_definition!r}. "
                          f"This task bootstraps a stable view, it never moves it; "
                          f"run checks/cutover_vault_version.py to repoint it "
                          f"deliberately.")
                    elsewhere.append(stable)
                elif action == "bootstrap":
                    spark.sql(stable_view_sql(
                        args.catalog, args.schema, stable, table))
                    print(f"  {action:13} {stable:34} -> {table}")
                else:
                    # already_here. NOT re-issued: CREATE OR REPLACE VIEW replaces the
                    # securable and takes its grants with it, so a no-op re-publish
                    # silently revokes SELECT on the consumer-facing name. The view
                    # already points at `table`; there is nothing to write.
                    print(f"  {action:13} {stable:34} -> {table} "
                          f"(left as-is; re-issuing would drop its grants)")

                # DEF-56: guard on existence, the same idiom the vault writers use, so
                # a repair run that reuses {{job.run_id}} does not double-audit.
                audited = spark.sql(audit.table_load_exists_sql(
                    args.catalog, args.control_schema, job_run_id=args.job_run_id,
                    table_name=table)).collect()[0]["n"]
                if audited:
                    print(f"  ~    {table:34} already audited for run "
                          f"{args.job_run_id} ({audited} row(s)) -- retry, the audit "
                          f"is not rewritten")
                else:
                    spark.sql(audit.table_load_sql(
                        args.catalog, args.control_schema, job_run_id=args.job_run_id,
                        pipeline_update_id=None, table_name=table,
                        written_by="checks/publish_stable_views.py",
                        staged=0, accepted=0))
                published += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  FAIL {table}: {exc}")
                failed.append(table)

    print(f"\n{published} table(s) considered, {len(elsewhere)} left alone "
          f"(view points elsewhere), {len(skipped)} skipped (inactive)")
    for s in skipped:
        print(f"  ~ {s}")
    if failed:
        print(f"\nPUBLISH STABLE VIEWS FAILED -- {len(failed)} problem(s):")
        for f in failed:
            print(f"  * {f}")
        return finish("FAILED", published, len(skipped), 1)

    if published == 0:
        # Every pipeline-owned table was inactive (or there were none) -- a dormant
        # lake is a correct outcome here, same as load_satellites.py's own
        # "every satellite is inactive" case, but it must not print as PASSED: nothing
        # was actually published or audited this run.
        print("\nGATE NOT EVALUATED: every pipeline-owned table was inactive in this "
              "lake -- nothing published, nothing audited.")
        return finish("NOT_EVALUATED", 0, len(skipped), 0)

    print(f"\nPUBLISH STABLE VIEWS PASSED: {published} table(s) considered")
    return finish("PASSED", published, len(skipped), 0)


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for code 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
