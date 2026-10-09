"""Record the rejects that a later run accepted, so loop-1 can stop counting them twice.

WHY THIS IS A BATCH TASK AND NOT PART OF A LOADER. Both sides of the comparison are
written by the PIPELINE, from one staged frame, in one run: the accepted rows go to the
staging log and the rejected rows to the quarantine twin, and `_valid`/`_invalid` are
exact complements of the same `_violation_expr`. So this task waits on the pipeline and
on nothing else -- resources/vault_job.yml gives it the six raw_vault domain tasks as its
depends_on, not load_hubs.

IT DELIBERATELY DOES NOT WAIT FOR A LOADER, and since 29 September that is a choice rather
than a consequence. Until then every reconcilable kind was unstaged, so the pipeline WAS
the only writer and there was no loader to wait for; the argument held by accident. Now
every keyed kind is staged (naming.STAGED_KINDS = KEYED_KINDS | SATELLITE_KINDS) and a
loader does run afterwards -- and this task still must not wait for it. `landed` here means
"the row was accepted by the pipeline", which is the log. The vault table is a deduplicated
projection of that log, so comparing against it would credit fewer rows than were accepted
and report a variance on correct behaviour, for exactly the reason
checks/loop1_reconciliation.py states about hub_accounting_journal.

WHAT THIS CAN BREAK IF IT IS WRONG. loop-1's identity becomes
landed + (quarantined - superseded) = approved. A spurious record REDUCES the quarantined
count and makes the gate pass on a real variance -- green exactly when it should not be. So
this task also asserts the negative: every record already on file must match a row still in
the SAME manifest's slice of the twin it names, and no (manifest, digest) may be superseded
more times than it was quarantined.

KEYED ON (manifest_id, digest), NOT digest alone. A digest can collide across manifests
(two rows with identical content, rejected in different batches), and a row that landed
under manifest M2 must never be credited as superseding a reject filed under manifest M1 --
that subtracts from a manifest that genuinely rejected the row, which is loop-1 green on a
real variance for that manifest even though the table-wide digest arithmetic looks correct.
manifest_id is safe to key on: it is in naming.SYSTEM_COLUMNS, so it is excluded from the
digest INPUT (src/accelerator/reject_digest.py), while `factory._system_columns` sets it
from `F.col(src.manifest_column)` on the staged frame and the quarantine twin is
`_project(df.where(failed), ...)` off that SAME frame -- so for one source row the value is
bit-identical on both sides of the comparison.

BRZ-12: manifest_id IS NULL FOR EVERY QUARANTINED ROW IN THIS ESTATE TODAY (measured:
nhl_general_journal_line, 2,453,132 rows, zero non-null manifest_id -- see
docs/superpowers/OPEN_ITEMS.md). Writing a supersede record with a fabricated manifest_id
would silently break loop-1's `USING (manifest_id)` join rather than merely leave it
unevaluated, so this task never coerces a NULL to anything: an empty twin is NOT EVALUATED
(nothing to supersede is not a failure), but a NON-empty twin with a NULL or empty
manifest_id is a hard FAILURE naming BRZ-12, because then there genuinely is something to
attribute and no way to attribute it.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import naming, reject_digest, spec  # noqa: E402
import loop1_reconciliation  # noqa: E402

GATE = "supersede_quarantine"

# BRZ-12, referenced in the FAILED message below. docs/superpowers/OPEN_ITEMS.md is the
# live record; this string names the row, not a fresh diagnosis, so anyone chasing the
# failure lands in the right place instead of re-deriving what is already an open ask.
BRZ_12 = (
    "BRZ-12 (docs/superpowers/OPEN_ITEMS.md): manifest_id on four Bronze feeds"
)


# THE BRZ-12 WAIVER -- TEMPORARY, NAMED, CAPPED, AND IT STILL FAILS ON GROWTH.
#
# Taken 26 September on instruction, to stop two GL entities blocking five downstream
# tasks while BRZ-12 sits with the Bronze team. It is NOT "ignore NULL manifest_ids": that
# would be a standing licence, and the next feed to lose its manifest_id would be absorbed
# in silence. It is a waiver of a MEASURED number of rows in NAMED tables.
#
# Each entry is `table: rows measured on 26 Sep`. Three ways this still fails:
#
#   * MORE rows than the cap  -> FAILURE. The problem grew; that is new information and
#     the whole point of a cap rather than a boolean.
#   * A table NOT in this map -> FAILURE, exactly as before. The waiver covers two GL
#     entities and nothing else.
#   * A waived table with NO bad rows -> FAILURE. BRZ-12 was fixed and the record rotted;
#     clearing the entry is part of the fix, or this map starts lying about the estate.
#
# WHAT IS GIVEN UP WHILE IT STANDS. These rows are not superseded, so loop-1 for these two
# entities is NOT EVALUATED rather than passing -- absent, like the journal gate withdrawn
# the same day, and in the same GL domain. Nothing here writes a fabricated manifest_id;
# the refusal that made this gate fail was correct and is untouched for every other table.
#
# REMOVE THIS MAP WHEN BRZ-12 LANDS. It is not a design, it is a dated exception.
_BRZ12_WAIVED = {
    "nhl_general_journal_line": 4,
    "nhl_general_journal_line_closed_year": 4,
}


def brz12_verdict(table: str, bad_manifest: int, waived: dict[str, int] | None = None):
    """What to do about NULL manifest_ids in one table. PURE, so it can be proven offline.

    `main()` needs Spark and a workspace; this decision does not, and keeping it inline is
    how a waiver ships without anybody demonstrating it can still fail. Returns one of:

      ("ok",     None)  no bad rows and no waiver -- carry on and compare
      ("waived", cap )  bad rows, at or under a named cap -- dormant, NOT passing
      ("grown",  cap )  bad rows ABOVE the cap -- the problem grew since it was measured
      ("stale",  cap )  a waiver whose table no longer has the defect. REPORTED LOUDLY
                        AND THEN COMPARED NORMALLY -- never a failure. Bronze fixing
                        BRZ-12 must not break this load; punishing the good outcome is
                        how a safety net teaches people to cut it down.
      ("fail",   None)  bad rows in a table nobody waived -- the original refusal
    """
    waived = _BRZ12_WAIVED if waived is None else waived
    cap = waived.get(table)
    if cap is None:
        return ("fail", None) if bad_manifest else ("ok", None)
    if not bad_manifest:
        return ("stale", cap)
    return ("waived", cap) if bad_manifest <= cap else ("grown", cap)


def finish(status: str, asserted: int, not_evaluated: int, code: int,
           waived: int = 0) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}"
          + (f" waived_brz12={waived}" if waived else ""))
    return code


def _sort_key(item):
    """Sort key for a {(manifest_id, digest): count} item.

    BRZ-12 means manifest_id is None for every quarantined row in this estate today, but
    a future partial fix could mix None and real strings -- and `None < "m1"` raises
    TypeError. The boolean isolates the None group first so the actual manifest_id is
    only ever compared within a group where every value is the same type.
    """
    (manifest_id, digest), _count = item
    return (manifest_id is None, manifest_id, digest)


def shortfall(landed: dict, quarantined: dict, recorded: dict) -> dict:
    """How many NEW supersede records to write, per (manifest_id, digest).

    Pure. `landed`, `quarantined` and `recorded` are {(manifest_id, digest): count}.

    CAPPED TWICE, and both caps are load-bearing. Capped by `quarantined` because five
    landed rows cannot supersede two rejects -- an uncapped count would subtract more than
    was ever quarantined. Capped by what is already `recorded` because this task runs on
    every job run while the landed rows stay landed, so an uncapped count would grow the
    subtraction on every run until loop-1 reported more superseded than quarantined.
    """
    out = {}
    for key, q_count in sorted(quarantined.items(), key=_sort_key):
        if key not in landed:
            continue
        want = min(landed[key], q_count)
        have = recorded.get(key, 0)
        if want > have:
            out[key] = want - have
    return out


def orphaned_records(recorded: dict, quarantined: dict) -> list[str]:
    """Supersede records that account for no reject. Pure.

    A record whose (manifest_id, digest) is absent from the twin, or whose count exceeds
    what the twin holds for that exact manifest, subtracts from loop-1's quarantined
    count for that manifest while superseding nothing. That makes a hard gate pass on a
    real variance, so it is a failure and not an observation.
    """
    problems = []
    for (manifest_id, digest), r_count in sorted(recorded.items(), key=_sort_key):
        q_count = quarantined.get((manifest_id, digest), 0)
        if r_count > q_count:
            problems.append(
                f"manifest {manifest_id!r}, digest {digest}: {r_count} supersede "
                f"record(s) on file but the quarantine twin holds {q_count} matching "
                f"row(s) for that manifest. The excess subtracts from loop-1's "
                f"quarantined count while superseding nothing, which makes loop-1 pass "
                f"on a real variance -- green exactly when it should not be."
            )
    return problems


def hashable(value):
    """A value usable as a dict key, whatever Spark handed back.

    A BINARY column arrives in Python as `bytearray`, and a bytearray is UNHASHABLE -- it
    is mutable, so it cannot be a dict key or a set member. The reject digest is BINARY --
    hashing.BINARY_OUTPUT is RATIFIED True -- so every dict keyed on a digest raises:

        TypeError: unhashable type: 'bytearray'

    MEASURED 25 September, on the first run in which this gate ever had two loaded tables
    to compare. nhl_general_journal_line and nhl_general_journal_line_closed_year both
    reported "could not be compared" -- the gate's own message correctly said this was NOT
    a dormancy, because both have active bindings and 8.5m rows between them.

    `bytes` is the immutable twin and compares equal to the same bytes, so converting
    changes no comparison -- only whether the value may be a key. Anything else is returned
    untouched: manifest_id is a string and must stay one.
    """
    return bytes(value) if isinstance(value, (bytearray, memoryview)) else value


def digest_counts(spark, fq: str, entity, src) -> dict:
    """{(manifest_id, digest): count} for one table, hash keys hex-rendered before
    digesting.

    manifest_id is added back only to GROUP BY: reject_digest.digest_columns() already
    excludes it from the digest INPUT because it is a system column, but the twin and the
    landed table share it bit-for-bit for one source row (both are projected from the
    same staged frame's F.col(src.manifest_column) -- see this module's docstring), and
    it is the join key checks/loop1_reconciliation.py subtracts against.
    """
    cols = reject_digest.digest_columns(entity, src)
    hexed = set(reject_digest.hex_columns(cols))
    select = ", ".join(
        f"lower(hex(`{c}`)) AS `{c}`" if c in hexed else f"`{c}`" for c in cols)
    expr = reject_digest.digest_sql(entity, src)
    rows = spark.sql(
        f"SELECT manifest_id, d AS digest, COUNT(*) AS n FROM ("
        f"  SELECT manifest_id, {expr} AS d "
        f"  FROM (SELECT manifest_id, {select} FROM {fq})"
        f") GROUP BY manifest_id, d").collect()
    return {(hashable(r.asDict()["manifest_id"]), hashable(r.asDict()["digest"])):
            r.asDict()["n"] for r in rows}


def _lit(value: str) -> str:
    """A SQL string literal, refusing anything that could close it.

    Refuses rather than escapes -- the same stance accelerator.audit._lit takes,
    duplicated locally rather than imported because that helper is private to the
    load-audit writers and this control table is not one of them. Never called on a
    NULL manifest_id: main() fails before reaching an INSERT if one is found (BRZ-12).
    """
    text = "" if value is None else str(value)
    if "'" in text or "\\" in text or "\x00" in text:
        raise ValueError(
            f"value {text!r} contains a quote, backslash or NUL and cannot be written "
            f"as a SQL literal. Refusing to escape it -- fix the value."
        )
    return f"'{text}'"


def _recorded_counts(spark, catalog: str, control_schema: str,
                     table_name: str) -> tuple[dict, list[str]]:
    """({(manifest_id, digest): count}, [stale-version report]) for one quarantine twin.

    control.ctl_quarantine_superseded is append-only, so a COUNT(*) per (manifest_id,
    reject_digest) is exactly what has been recorded so far -- shortfall()'s third
    argument and orphaned_records()'s first, at the same grain loop-1 subtracts at.

    rulebook_version IS READ BACK, and that is the point of writing it. It used to be
    written and never read: the GROUP BY was (manifest_id, reject_digest) alone, so a
    record written under a SUPERSEDED rulebook version was silently counted as recorded.
    Two consequences, both silent, and spec section 4's last paragraph and section 6 row 4
    require neither:

      * it SUPPRESSED a legitimate new write -- shortfall() saw the reject as already
        superseded, so nothing was ever recorded under the current rulebook; and
      * it made the stale record an ORPHAN once the bump changed every digest, because
        orphaned_records() would find no matching row in the twin for the old digest --
        turning a planned rulebook bump into a hard gate failure.

    So a record whose version differs from reject_digest.RULEBOOK_VERSION is EXCLUDED
    from the returned counts and REPORTED by name instead. Excluded, so it neither
    suppresses a write nor reaches orphaned_records(); reported, so it does not silently
    participate in loop-1's subtraction either. Reported and not failed: a rulebook bump
    is a planned event and its outstanding records are EXPECTED to be stale.
    """
    fq = f"`{catalog}`.`{control_schema}`.ctl_quarantine_superseded"
    rows = spark.sql(
        f"SELECT manifest_id, reject_digest, rulebook_version, COUNT(*) AS n FROM {fq} "
        f"WHERE table_name = {_lit(table_name)} "
        f"GROUP BY manifest_id, reject_digest, rulebook_version"
    ).collect()
    counts: dict = {}
    stale: dict = {}
    for row in rows:
        r = row.asDict()
        key = (hashable(r["manifest_id"]), hashable(r["reject_digest"]))
        if r["rulebook_version"] == reject_digest.RULEBOOK_VERSION:
            counts[key] = counts.get(key, 0) + r["n"]
        else:
            stale_key = (*key, r["rulebook_version"])
            stale[stale_key] = stale.get(stale_key, 0) + r["n"]
    reports = [
        f"manifest {manifest_id!r}, digest {digest}: {n} supersede record(s) written "
        f"under rulebook version {version!r}, but this repository is at "
        f"{reject_digest.RULEBOOK_VERSION!r}. Excluded from the recorded count, so it "
        f"neither suppresses a legitimate new write nor silently participates in "
        f"loop-1's subtraction. A rulebook bump re-keys the estate and its outstanding "
        f"records are expected to be stale, so this is REPORTED and not failed -- but "
        f"loop-1 still counts the row, so a bump needs an approved control-table cleanup "
        f"(see the spec's known limitations)."
        for (manifest_id, digest, version), n in sorted(
            stale.items(), key=lambda kv: (kv[0][0] is None, *map(str, kv[0])))
    ]
    return counts, reports


def _insert_sql(catalog: str, control_schema: str, units, reason: str | None = None) -> str:
    """One INSERT ... VALUES writing every (manifest_id, table_name, digest,
    rulebook_version, superseded_by) unit as its own append-only row.

    `reason` is the operator's note about why the rule was wrong -- spec section 2's
    "auditable record of what we wrongly rejected" and section 8's human-written reason,
    which were unreachable while this function hardcoded NULL and no flag existed. It is
    OPTIONAL: the task runs unattended on every job run, so a required note would fail
    every one. Absent, the column stays NULL -- the absence of a reason, not an empty
    string, which is a reason someone wrote.
    """
    reason_sql = "NULL" if reason is None else _lit(reason)
    values = ", ".join(
        f"({_lit(manifest_id)}, {_lit(table_name)}, {_lit(digest)}, "
        f"{_lit(rulebook_version)}, {_lit(superseded_by)}, {reason_sql}, "
        f"current_timestamp())"
        for manifest_id, table_name, digest, rulebook_version, superseded_by in units
    )
    return (
        f"INSERT INTO `{catalog}`.`{control_schema}`.ctl_quarantine_superseded "
        f"(manifest_id, table_name, reject_digest, rulebook_version, superseded_by, "
        f"reason, recorded_at) VALUES {values}"
    )


def targets(model: spec.Model, named: list[str]):
    """[(entity, src, table)] to check: the named tables, or every reconcilable one.

    Reuses loop1_reconciliation.RECONCILABLE_KINDS -- ONE definition of which kinds take
    one row per approved source row, shared with the gate this task feeds.
    """
    all_tables = [(e, src, table) for e in model.entities for src, table in e.tables()
                  if e.kind in loop1_reconciliation.RECONCILABLE_KINDS]
    if not named:
        return all_tables
    by_name = {table: (e, src, table) for e, src, table in all_tables}
    unknown = [n for n in named if n not in by_name]
    if unknown:
        raise SystemExit(
            f"--entity names {unknown}, which is not a reconcilable "
            f"(nhl/link/hal) vault table. Known: {sorted(by_name)}"
        )
    return [by_name[n] for n in named]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--active-sources", required=True)
    ap.add_argument(
        "--entity", default="",
        help="comma-separated vault table name(s) to check; default is every "
             "reconcilable (nhl/link/hal) table with an active source binding",
    )
    ap.add_argument("--dry-run", action="store_true",
                     help="compare and report, but write no supersede record")
    ap.add_argument(
        "--reason", default=None,
        help="the operator's note about WHY the rule that rejected these rows was "
             "wrong, written to ctl_quarantine_superseded.reason. Optional -- the task "
             "runs unattended on every job run, so a required note would fail every "
             "one -- but it is the whole audit value of the table (spec sections 2 "
             "and 8), so supply it whenever a human is driving the correction.",
    )
    args = ap.parse_args()

    meta = Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)
    named = [n.strip() for n in args.entity.split(",") if n.strip()]

    try:
        wanted = targets(model, named)
    except SystemExit as exc:
        print(str(exc))
        return finish("FAILED", 0, 0, 1)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    problems: list[str] = []
    not_evaluated: list[str] = []
    # REPORTED, not failed, and deliberately NOT in not_evaluated: a stale-rulebook
    # record neither makes the run vacuous nor fails it, and putting it in not_evaluated
    # would corrupt the vacuity rule below.
    stale_version: list[str] = []
    # BRZ-12 waivers whose table no longer has the defect. Reported, never fatal.
    stale_waivers: list[str] = []
    skipped_inactive = 0
    skipped_empty_twin = 0
    waived_brz12 = 0
    asserted = 0
    written = 0
    would_write = 0

    for entity, src, table in wanted:
        if not spec.active_table_bindings(entity, src, active):
            skipped_inactive += 1
            not_evaluated.append(
                f"{table}: no active source binding in this lake, so nothing landed "
                f"and nothing was rejected. There is no reject digest to compare."
            )
            continue

        # DEF-42: for a STAGED kind, "landed" is the STAGING LOG, not the vault table.
        # Every accepted row lands in the log; the vault table is a deduplicated
        # projection of it, so counting the vault table would credit fewer rows than the
        # pipeline accepted and report a variance on correct behaviour. Written through
        # pipeline_table() rather than as `table`, which is what makes that sentence
        # true of whatever the set holds: it read as the identity while nhl/link/hal
        # were unstaged, and resolved to stg_nhl_... the moment they were, with nothing
        # here needing to change. Same expression, same reasoning, as
        # checks/loop1_reconciliation.py's own `landed_table`.
        landed_table = naming.pipeline_table(entity.kind, table)
        landed_fq = f"{args.catalog}.{args.schema}.{landed_table}"
        # checks/loop1_reconciliation.py's `qtn_name` idiom (line 357 today), verbatim,
        # so both sides name the quarantine twin identically. The symbol is the anchor --
        # the line number is a courtesy, and it has already gone stale once.
        twin_table = f"qtn_{table.split('_', 1)[1]}"
        twin_fq = f"{args.catalog}.{args.schema}.{twin_table}"

        try:
            quarantined = digest_counts(spark, twin_fq, entity, src)
        except Exception as exc:  # noqa: BLE001
            # AN EXCEPTION IS A FAILURE, NOT A DORMANCY. This used to land in
            # not_evaluated, which made a crash indistinguishable from a legitimately
            # empty twin: every table could raise and the run still exited 0 under a
            # "dormant by declaration" banner. That is exactly how the digest path being
            # dead for all five NHLs (an AttributeError out of factory._projection)
            # survived four reviews -- the gate reported it as "could not be compared ...
            # check that raw_vault has run". The table HAS an active binding, so there is
            # nothing declared about this absence.
            problems.append(
                f"{table}: could not be compared: {exc!r}. It has an active binding, so "
                f"this is not an expected absence and NOT a dormancy -- check that "
                f"raw_vault has run, that --schema names the layer this entity is "
                f"generated into, and that the digest projection resolves for this "
                f"entity's kind."
            )
            continue

        if not quarantined:
            # Nothing to supersede is not a failure -- every qtn_ table in this estate is
            # empty today, and a gate that failed on an empty quarantine twin would fail
            # every run. Counted separately from skipped_inactive because the vacuity
            # rule below has to be able to state that EVERY not_evaluated entry is one of
            # these two declared reasons and nothing else.
            skipped_empty_twin += 1
            not_evaluated.append(
                f"{table}: the quarantine twin holds no rows. Nothing has ever been "
                f"quarantined, so there is nothing to supersede."
            )
            continue

        bad_manifest = sum(n for (m, _d), n in quarantined.items() if not m)
        verdict, waived_cap = brz12_verdict(table, bad_manifest)

        if verdict == "stale":
            # THE WAIVER OUTLIVED THE DEFECT, which is the OUTCOME WE WANT. Reported, and
            # then this table is compared exactly as any other -- deliberately NOT a
            # failure. Failing here would mean the moment Bronze fixes BRZ-12 our load
            # breaks until somebody edits a dict, which is punishing the good outcome and
            # is how a safety net teaches people to cut it down. Growth still fails; a fix
            # never does.
            stale_waivers.append(
                f"{table}: in _BRZ12_WAIVED (cap {waived_cap}) but every quarantined row "
                f"now carries a manifest_id -- {BRZ_12} appears FIXED here. Remove this "
                f"entry; the waiver is dated and this table no longer needs it."
            )

        if verdict in ("waived", "grown", "fail"):
            if verdict == "waived":
                waived_brz12 += 1
                not_evaluated.append(
                    f"{table}: {bad_manifest} quarantined row(s) carry a NULL or empty "
                    f"manifest_id and are WAIVED under {BRZ_12} (cap {waived_cap}, "
                    f"measured 26 Sep). They are NOT superseded and loop-1 for this "
                    f"entity is unevaluated, not passing. The waiver is dated and is "
                    f"removed when BRZ-12 lands."
                )
                continue
            grew = (f" -- ABOVE the waived cap of {waived_cap}, so the problem has GROWN "
                    f"since 26 Sep and is new information"
                    if verdict == "grown" else "")
            problems.append(
                f"{table}: {bad_manifest} quarantined row(s) carry a NULL or empty "
                f"manifest_id ({BRZ_12}){grew}. A supersede record cannot be attributed "
                f"to a manifest it does not have -- writing one with a fabricated value "
                f"would silently break loop-1's USING (manifest_id) join rather than "
                f"merely leave it unevaluated, so this task refuses instead."
            )
            continue

        try:
            landed = digest_counts(spark, landed_fq, entity, src)
            recorded, stale = _recorded_counts(
                spark, args.catalog, args.control_schema, twin_table)
        except Exception as exc:  # noqa: BLE001
            # Same rule as the twin comparison above: an exception is a FAILURE. Here it
            # is even less excusable -- the twin was read and holds rows, so the only
            # things left to fail are the landed table and the control table, and neither
            # being readable is a dormancy.
            problems.append(
                f"{table}: could not be compared: {exc!r}. It has an active binding and "
                f"a non-empty quarantine twin, so this is not an expected absence and "
                f"NOT a dormancy -- check that raw_vault has run, that --schema names "
                f"the layer this entity is generated into, and that "
                f"--control-schema names the schema holding "
                f"ctl_quarantine_superseded."
            )
            continue

        stale_version.extend(f"{table}: {s}" for s in stale)

        orphans = orphaned_records(recorded, quarantined)
        if orphans:
            problems.extend(f"{table}: {o}" for o in orphans)
            # Do not write MORE supersede records against a table whose existing
            # records already fail to reconcile with the twin -- that would compound a
            # gate-disarming condition rather than merely report it.
            continue

        asserted += 1
        need = shortfall(landed, quarantined, recorded)
        if not need:
            continue

        units = [
            (manifest_id, twin_table, digest, reject_digest.RULEBOOK_VERSION,
             args.job_run_id)
            for (manifest_id, digest), count in need.items()
            for _ in range(count)
        ]
        if args.dry_run:
            would_write += len(units)
            print(f"  {table}: DRY RUN -- would write {len(units)} supersede record(s), "
                  f"and wrote none")
            continue

        spark.sql(
            _insert_sql(args.catalog, args.control_schema, units, args.reason))
        written += len(units)
        print(f"  {table}: wrote {len(units)} supersede record(s)")

    print("\n" + "=" * 68)
    if stale_version:
        print(f"REPORTED (not a failure) -- {len(stale_version)} supersede record "
              f"group(s) written under a SUPERSEDED rulebook version:")
        for s in stale_version:
            print(f"  ! {s}")
        print()
    if stale_waivers:
        print(f"BRZ-12 WAIVER NO LONGER NEEDED -- {len(stale_waivers)} table(s). This is "
              f"good news and is NOT a failure; the entry is now dead weight:")
        for w in stale_waivers:
            print(f"  ! {w}")
    if not_evaluated:
        print(f"NOT EVALUATED -- {len(not_evaluated)} table(s):")
        for n in not_evaluated:
            print(f"  ~ {n}")
        print()
    if problems:
        print(f"SUPERSEDE_QUARANTINE GATE FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        return finish("FAILED", asserted, len(not_evaluated), 1)
    if not asserted:
        # THE VACUITY RULE. The other four gates (checks/loop1_reconciliation.py,
        # journal_integrity_check.py, aggregate_reconciliation_check.py,
        # mask_survival_check.py) share one rule verbatim:
        #
        #     active is not None and skipped_inactive == len(not_evaluated)
        #
        # THIS GATE'S RULE DIFFERS, IN ONE STATED WAY AND NO OTHER, and the difference is
        # an EXTRA declared-dormancy reason, never a weaker treatment of failure:
        #
        #   * the four gates admit exactly one dormancy -- a binding this lake declares
        #     inactive. This gate admits a second AND, from 26 September, a third.
        #
        #     THE THIRD IS A DATED WAIVER (_BRZ12_WAIVED) and is the weakest of the
        #     three, because unlike the other two it rests on a decision rather than on
        #     a fact about the data. It is capped at a measured row count per named
        #     table: more rows than the cap FAILS, a table nobody named FAILS. It exists
        #     so two GL entities stop blocking five downstream tasks while BRZ-12 sits
        #     with the Bronze team, and it is removed when BRZ-12 lands.
        #
        #     The second: an ACTIVE table whose quarantine twin holds no rows. Nothing has ever been quarantined there, so there is nothing
        #     to supersede. That is a fact about the data, true or false regardless of
        #     what --active-sources says, so it needs no declaration to be trusted -- and
        #     every qtn_ table in this estate is empty today, so a gate that failed on it
        #     would fail every run.
        #
        # In every other respect the rule is the four gates' rule. In particular a table
        # that COULD NOT BE READ is a FAILURE here exactly as it is there: both except
        # blocks above append to `problems`, so the `if problems:` branch has already
        # returned FAILED before this point and an exception can never reach this rule.
        # It used to reach it, via not_evaluated, and that is what let a dead digest path
        # exit 0 under this banner.
        #
        # Both dormancy counters are summed against len(not_evaluated) so the rule cannot
        # go vacuous: a future third skip reason that forgot to bump a counter fails here
        # rather than being absorbed as a dormancy.
        if (skipped_inactive + skipped_empty_twin + waived_brz12
                == len(not_evaluated) == len(wanted)
                and (active is not None or skipped_inactive == 0)):
            print("SUPERSEDE_QUARANTINE GATE NOT EVALUATED: every table named or "
                  "declared has no active source binding, its quarantine twin holds no "
                  "rows, or its NULL manifest_ids are WAIVED under BRZ-12, so this run "
                  "asserted nothing. Dormant by declaration, by an empty twin or by a "
                  "dated waiver -- not passing.")
            if waived_brz12:
                print(f"  {waived_brz12} table(s) WAIVED under BRZ-12. Those rows are "
                      f"NOT superseded and loop-1 for them is unevaluated. This is an "
                      f"exception with a date on it, not a clean run.")
            return finish("NOT_EVALUATED", 0, len(not_evaluated), 0, waived_brz12)
        print("SUPERSEDE_QUARANTINE GATE FAILED: no table was compared, so this run "
              "asserted nothing, and not every skip is explained by a "
              "declared-inactive binding, an empty quarantine twin or a BRZ-12 waiver. "
              "A table that "
              "could not be read is a FAILURE, not a dormancy -- see the problems "
              "above.")
        return finish("FAILED", 0, len(not_evaluated), 1)
    summary = (f"SUPERSEDE_QUARANTINE GATE PASSED: {written} new supersede record(s) "
               f"written across {asserted} table(s); {len(not_evaluated)} NOT EVALUATED "
               f"(listed above).")
    if args.dry_run:
        # "0 new supersede record(s) written" on its own reads as "nothing was owed",
        # which is the opposite of what a dry run over an outstanding shortfall means.
        # --dry-run is exactly how the deferred live probe will be driven, so this is the
        # line a human will read first.
        summary += (f" DRY RUN: no record was written because --dry-run was given -- "
                    f"{would_write} record(s) were OWED and remain outstanding. The 0 "
                    f"above is not a clean bill of health.")
    print(summary)
    return finish("PASSED", asserted, len(not_evaluated), 0, waived_brz12)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
