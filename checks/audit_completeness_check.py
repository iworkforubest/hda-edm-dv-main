"""HARD GATE: every discard is attributed. REPORTED BESIDE IT: every prior run is closed.

Two assertions over the control schema with DELIBERATELY DIFFERENT consequences, because
they can enforce different amounts:

  1. HARD -- staged - accepted must equal SUM(discarded) for every audited table, the
     in-flight run included. That is arithmetic over each run's OWN rows, so a violation
     is a real, actionable defect: a writer quietly dropped rows without recording a
     reason, which is precisely what loop-1 exists to make impossible. Non-zero exit.

  2. REPORTED, NOT FAILED -- a run that wrote aud_table_load rows should also have both an
     'opened' and a 'completed' row in aud_load_run. Three reasons this is a report:

       * THIS GATE RUNS BEFORE THE TASK THAT CLOSES THE RUN. publish_model_metadata writes
         the 'completed' row and depends on this gate, so the in-flight run is unclosed BY
         CONSTRUCTION here. Asserted as a failure, the gate was red on every single run --
         verified against the exact state it sees at its position in the graph:
             runs  = [('RUN1', 'opened')]                  # create_control_objects
             loads = [('RUN1', 'hub_accounting_journal')]  # the loaders
         --job-run-id names the in-flight run and excludes it from the report, because
         expected state reported as an anomaly is how a report stops being read.

       * A DIED-MID-WAY RUN ALREADY FAILED LOUDLY. Its task exited non-zero and the job
         went red at that task. Closure adds no detection power on top of that.

       * IT WOULD BE UNCLEARABLE. aud_load_run is append-only (append_only_check asserts
         it), so a dead run's missing 'completed' row can never be supplied afterwards.
         As a hard gate this stays red for ever after one bad run, and an unclearable gate
         gets switched off -- which is strictly worse than a report nobody can silence.

     It stays visible: printed by name below, counted in the GATE SUMMARY line, and
     readable in aud_load_run itself.

Both predicates are pure and Spark-free so the failing case can be fired offline in both
directions -- this repo has shipped five checks that could never fail.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys

GATE = "audit_completeness"


def finish(status: str, asserted: int, not_evaluated: int, code: int,
           unclosed: int = 0) -> int:
    # unclosed_prior_runs is on this line and not only in the body of the report because
    # DEPLOY.md Phase 5f greps the summary lines: an observation that appears nowhere a
    # machine reads it is an observation nobody notices.
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated} unclosed_prior_runs={unclosed}")
    return code


def unclosed_runs(run_rows, load_rows, in_flight: str | None = None) -> list[str]:
    """Runs that wrote audit rows without both phases recorded.

    in_flight names THIS run, whose 'completed' row is written by the task that depends on
    this gate. It is excluded rather than reported: at this point in the graph it is
    unclosed by construction, and expected state announced as an anomaly on every run is
    how a report stops being read. Omit it and the in-flight run is reported -- which is
    why resources/vault_job.yml passes {{job.run_id}}.

    Still a pure predicate, and still the same one: only its CONSEQUENCE changed (see the
    module docstring). Reported, not failed.
    """
    phases: dict[str, set] = {}
    for run_id, phase in run_rows:
        phases.setdefault(run_id, set()).add(phase)
    problems = []
    for run_id in sorted({r for r, _t in load_rows}):
        if in_flight is not None and run_id == in_flight:
            continue
        have = phases.get(run_id, set())
        missing = {"opened", "completed"} - have
        if missing:
            problems.append(
                f"run {run_id}: wrote aud_table_load rows but aud_load_run is missing "
                f"{sorted(missing)}. Either that run died between the first writer and "
                f"the last -- in which case it already exited non-zero and went red at "
                f"the task that died -- or a writer ran before create_control_objects "
                f"opened the run. aud_load_run is append-only, so the phase row cannot "
                f"be supplied after the fact: this is reported, not failed."
            )
    return problems


def unbalanced_tables(load_rows, discard_rows) -> list[str]:
    """Tables where the attributed discards do not account for staged - accepted."""
    attributed: dict[tuple, int] = {}
    for run_id, table, discarded in discard_rows:
        attributed[(run_id, table)] = attributed.get((run_id, table), 0) + discarded
    problems = []
    for run_id, table, staged, accepted in sorted(load_rows):
        gap = (staged - accepted) - attributed.get((run_id, table), 0)
        if gap:
            problems.append(
                f"run {run_id}, {table}: staged={staged} accepted={accepted} leaves "
                f"{staged - accepted} discarded, but only "
                f"{attributed.get((run_id, table), 0)} row(s) are attributed to a reason "
                f"-- {gap} unaccounted for. A discard with no recorded reason is a silent "
                f"drop, which is what loop-1 exists to make impossible."
            )
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", required=True)
    # NOT A FILTER. This names the IN-FLIGHT run so the closure REPORT can exclude it;
    # every query below still reads every run on record, because the arithmetic is a hard
    # assertion and narrowing it would be a loss for nothing. Omit the flag and the
    # in-flight run is merely reported as unclosed -- never failed.
    ap.add_argument("--job-run-id", default=None,
                    help="{{job.run_id}}: the in-flight run, excluded from the closure "
                         "report because the task that closes it runs after this gate")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    c, s = args.catalog, args.control_schema

    # NO WHERE CLAUSE, deliberately. The arithmetic below is asserted over EVERY run on
    # record, the in-flight one included: staged - accepted == SUM(discarded) is a
    # property of one run's own rows, so filtering to the current run would shrink a hard
    # assertion and catch nothing extra. A prior run's unattributed discard is still a
    # silent drop and still fails this gate.
    runs = [(r["job_run_id"], r["phase"]) for r in spark.sql(
        f"SELECT job_run_id, phase FROM `{c}`.`{s}`.aud_load_run").collect()]
    loads = [(r["job_run_id"], r["table_name"], r["staged"], r["accepted"])
             for r in spark.sql(
                 f"SELECT job_run_id, table_name, staged, accepted "
                 f"FROM `{c}`.`{s}`.aud_table_load").collect()]
    discards = [(r["job_run_id"], r["table_name"], r["discarded"]) for r in spark.sql(
        f"SELECT job_run_id, table_name, discarded "
        f"FROM `{c}`.`{s}`.aud_table_discard").collect()]

    # THE HARD HALF.
    problems = unbalanced_tables(loads, discards)
    # THE REPORTED HALF -- see the module docstring for why it is not a failure.
    unclosed = unclosed_runs(runs, [(r, t) for r, t, _s, _a in loads],
                             in_flight=args.job_run_id)
    print(f"  {len(runs)} run phase row(s), {len(loads)} table row(s), "
          f"{len(discards)} discard row(s)")
    if unclosed:
        print(f"\n{len(unclosed)} PRIOR RUN(S) NOT CLOSED -- reported, not failed:")
        for u in unclosed:
            print(f"  ~ {u}")

    if problems:
        print(f"\nAUDIT COMPLETENESS GATE FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        return finish("FAILED", 1, 0, 1, len(unclosed))
    if not loads:
        print("\nGATE NOT EVALUATED: no aud_table_load rows to assert over.")
        return finish("NOT_EVALUATED", 0, 1, 1, len(unclosed))
    print("\nAUDIT COMPLETENESS GATE PASSED: every discard is attributed to a reason.")
    return finish("PASSED", 1, 0, 0, len(unclosed))


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
