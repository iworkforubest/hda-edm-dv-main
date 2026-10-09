"""
HARD GATE: no vault schema, and no vault catalog, carries a broad SELECT.

DEF-40. A vault schema holds more than the tables this repo declares. SDP creates a
`__materialization_mat_<pipeline-id>_<table>_1` backing table for every streaming
table, in the SAME schema, as the runtime's own storage. The MASK lives on the
streaming table. The backing table carries the same values with no mask, and the
generator cannot fix that: it never creates those tables, and
`ALTER TABLE ... SET MASK` against a pipeline-owned table does not survive the next
update (governance/apply_masks.sql).

Measured 25 Aug 2026, as an identity for which is_account_group_member(
'hfig_commercials_reader') is FALSE:

    raw_vault.nhl_general_journal_line        count(debitamt) = 0          (masked)
    its __materialization_* twin              count(debitamt) = 2,453,131  (cleartext)

So a single `GRANT SELECT ON SCHEMA` hands cleartext money to exactly the group
`mask_money` exists to stop. checks/apply_governance.py grants SELECT per TABLE from
the declared model instead, where a backing table can never appear.

THIS GATE ASSERTS THE NEGATIVE, because per-table grants are only worth anything while
no broader grant exists beside them, and one hand-run statement re-opens the hole
silently and permanently:

  1. no SELECT (or ALL PRIVILEGES) assignment ON SCHEMA, for any vault schema;
  2. no SELECT (or ALL PRIVILEGES) assignment ON CATALOG, for the vault catalog --
     a catalog grant cascades into every schema and would bypass (1) entirely.

USE SCHEMA and USE CATALOG are NOT flagged: they convey no data access on their own,
and the per-table SELECT is useless without them.

It runs BEFORE apply_governance, not after. The point is to catch a broad grant that
already exists, before more are layered on top of it.
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

from accelerator import gold_layout  # noqa: E402

GATE = "schema_grant"

# Privileges that convey reading DATA. USE SCHEMA / USE CATALOG / BROWSE do not, and a
# gate that flagged them would fail on the grants the design requires.
READING = ("SELECT", "ALL PRIVILEGES", "ALL_PRIVILEGES")


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


# --------------------------------------------------------------------------- #
def offending(assignments, securable: str, name: str) -> list[str]:
    """Problems for every assignment on this securable that conveys reading data.

    Pure and Spark-free: `assignments` is an iterable of (principal, privilege), so the
    predicate can be fired in BOTH directions offline. A gate whose failing case has
    never been executed is the failure mode this project has already hit four times.
    """
    problems = []
    for principal, privilege in sorted(assignments):
        if privilege.upper().replace("_", " ") not in {p.replace("_", " ")
                                                       for p in READING}:
            continue
        problems.append(
            f"{securable} {name}: `{principal}` holds {privilege}. A {securable.lower()}"
            f"-level read grant covers the __materialization_* backing tables, which "
            f"carry no mask -- so this grants cleartext to every masked column beneath "
            f"it. Grant SELECT per TABLE instead: checks/apply_governance.py generates "
            f"that list from the declared model."
        )
    return problems


def unauthorised_table_readers(rows, catalog: str, schema: str,
                               allowed) -> list[str]:
    """Problems for every TABLE-level read grant held by a principal not in `allowed`.

    DECIDED 26 Aug 2026 (DEPLOY.md Phase 6STOP): the raw vault is not consumer-readable,
    and the business vault only through the gold catalog -- never directly. Before this,
    the posture was true only by configuration: `vault_privileged_group` happened to name
    an engineering group, and setting it to a consumer group would have granted the vault
    away with nothing objecting. The variable is renamed to say so; this asserts it.

    It is the per-TABLE counterpart to offending(). That one fails a grant that is too
    BROAD (schema, catalog); this one fails a grant of the right shape held by the wrong
    PRINCIPAL. Both are needed: per-table grants to the wrong group defeat the masks just
    as completely as a schema grant to the right one, because a vault grant carries the
    unmasked __materialization_* twin along with the masked table (DEF-40).

    Pure and Spark-free, like offending(), so the failing case can be fired offline in
    both directions. `rows` is an iterable of (grantee, table_name, privilege).

    OWNERS DO NOT APPEAR IN THESE ROWS -- AND THAT IS NOT THE SAME AS "OWNERS ARE NOT A
    HOLE", WHICH IS WHAT THIS USED TO SAY. Unity Catalog does not record ownership as a grant,
    so an owner never shows up here and never needs allow-listing. True, and a narrow point
    about this function's input.

    The platform team put the other half in writing on 2 Sep 2026: "object owners bypass masks,
    exclusions and grants entirely". So an owner IS a hole in the masking posture, and this gate
    cannot see it -- neither can mask_survival_check, which reads mask DECLARATIONS and not who
    can ignore them. Measured 31 Aug: all four silver schemas are owned by a named individual,
    so today one personal account can read every masked column in the vault and no gate in this
    repo would object.

    That is being fixed by transferring ownership to the EDM pipeline service principal (their
    ask 2). Until it lands, read this function's silence about owners as a limit of scope, not
    as an assurance.
    """
    allowed_lower = {a.lower() for a in allowed}
    problems = []
    for grantee, table, privilege in sorted(rows):
        if privilege.upper().replace("_", " ") not in {p.replace("_", " ")
                                                       for p in READING}:
            continue
        if grantee.lower() in allowed_lower:
            continue
        problems.append(
            f"TABLE {catalog}.{schema}.{table}: `{grantee}` holds {privilege}, and is "
            f"not the privileged group. The vault has no consumer readers -- the raw "
            f"vault is not consumer-readable and the business vault is reached through "
            f"the gold catalog (decided 26 Aug 2026). A vault grant also conveys the "
            f"unmasked __materialization_* twin, so this defeats every column mask on "
            f"the table. Revoke it, and expose the data through gold instead."
        )
    return problems


SYSTEM_OWNERS = ("system user",)


def undeclared_schemas(rows, declared) -> list[str]:
    """Problems for every schema in the catalog that this repo does not declare.

    silver_vault is why this exists. It was the vault's schema name before the raw/business
    split, and verify_repo.py already asserted "silver_vault survives nowhere in the
    bundle" -- which PASSED, correctly: the name was gone from the CONFIG. The schema was
    still in the lake, holding a probe table, and no gate looked at it because
    schema_grant_check was passed raw_vault and business_vault only. A config-level
    assertion cannot see lake state.

    SYSTEM SCHEMAS ARE EXCLUDED BY OWNER, NOT BY NAME. information_schema is always present
    and is owned by `System user`; everything this repo creates is owned by the deploying
    principal. A name-based exclusion list would be the place the next orphan hides,
    because a name the check has been taught to ignore is indistinguishable from one it
    should have caught.

    Pure and Spark-free, like offending() and unauthorised_table_readers(), so the failing
    case can be fired offline in both directions.
    """
    declared_lower = {d.lower() for d in declared}
    problems = []
    for schema, owner in sorted(rows):
        if schema.lower() in declared_lower:
            continue
        if (owner or "").strip().lower() in SYSTEM_OWNERS:
            continue
        problems.append(
            f"SCHEMA {schema}: undeclared, and owned by `{owner}` rather than the system. "
            f"This repo declares {sorted(declared_lower)}. An undeclared schema is outside "
            f"every gate pointed at the declared ones -- no append-only check, no grant "
            f"sweep, no mask survival. Either declare it as a bundle variable or drop it."
        )
    return problems


CONTROL_OBJECT_PROPERTY = "hfig.control_object"


def misplaced_control_objects(rows, control_schema: str) -> list[str]:
    """Problems for any control object outside `control_schema`, or non-control inside it.

    Spec section 6 called this assertion "available for free", because control_objects.sql
    already stamps every table it creates with TBLPROPERTIES ('hfig.control_object' =
    'true'). Free to assert is not the same as asserted: the marker existed for weeks and
    nothing read it, which is how ctl_approval_manifest and ref_dq_expectation sat in
    `governance` next to the mask functions long enough for "where does load control live?"
    to have two answers.

    TWO DIRECTIONS, because each catches a different mistake:

      * a table carrying the marker OUTSIDE the control schema is one left behind by a
        move, or newly added to the wrong place. This is the direction that would have
        caught the original split.
      * a table inside the control schema WITHOUT the marker is something that drifted in
        -- a scratch table, a stray CTAS -- and it matters because the control schema is
        inside the Phase 6STOP posture and append_only_check's audit sweep. An unmarked
        table there is governed by accident rather than by declaration.

    Pure and Spark-free, like offending(), unauthorised_table_readers() and
    undeclared_schemas(), so both directions can be fired offline. `rows` is an iterable of
    (schema, table, has_marker).
    """
    problems = []
    for schema, table, has_marker in sorted(rows):
        in_control = schema.lower() == control_schema.lower()
        if in_control and not has_marker:
            problems.append(
                f"TABLE {schema}.{table}: sits in the control schema but does not declare "
                f"{CONTROL_OBJECT_PROPERTY}. The control schema is inside the Phase 6STOP "
                f"posture and the audit append-only sweep, so an undeclared table there is "
                f"governed by accident. Declare it in governance/control_objects.sql or "
                f"move it out."
            )
        elif not in_control and has_marker:
            problems.append(
                f"TABLE {schema}.{table}: declares {CONTROL_OBJECT_PROPERTY} but lives in "
                f"{schema}, not {control_schema}. Load control has one home. This is the "
                f"shape of the original defect -- the manifest and the expectation "
                f"reference sat in `governance` beside the mask functions."
            )
    return problems


SYNTHETIC_PREFIX = "tst_"


def synthetic_objects(rows, allow: bool) -> list[str]:
    """Problems for every synthetic table left behind after a dashboard exercise.

    governance/control_test_objects.sql creates tst_ tables holding FABRICATED numbers so
    the quality dashboard's tiles can be proven to render. They are deliberately built to
    slip past the other gates -- outside CONTROL_PREFIXES so append_only_check does not
    sweep them, and carrying hfig.control_object so misplaced_control_objects passes -- and
    the consequence is that NOTHING otherwise notices one that is never dropped.

    A survivor is worse than an undeclared table. It sits in the control schema, wears the
    control_object marker, and reports invented discards. Someone reading the audit a month
    later has no way to tell it from a real load.

    Pure and Spark-free, like offending() and undeclared_schemas(), so the failing case is
    testable without a workspace.

    `allow` is the operator saying an exercise is in progress. It exists so a sanctioned
    run does not have to disable the gate wholesale -- a gate people learn to switch off
    is a gate that is off when it matters.
    """
    if allow:
        return []
    return [
        f"TABLE {schema}.{table}: a synthetic object from "
        f"governance/control_test_objects.sql was never dropped. It holds FABRICATED "
        f"numbers, sits in the control schema, and carries hfig.control_object, so no "
        f"other gate objects to it. Apply "
        f"governance/control_test_objects_drop.sql, or pass --allow-test-objects if an "
        f"exercise is genuinely in progress."
        for schema, table in sorted(rows)
        if table.lower().startswith(SYNTHETIC_PREFIX)
    ]


def control_object_rows(spark, catalog: str, schemas):
    """(schema, table, has_marker) for every table in `schemas`.

    One SHOW TBLPROPERTIES per table, because TBLPROPERTIES are NOT exposed through
    information_schema -- there is no set-based way to read them. Scoped to the schemas
    passed rather than the whole catalog to keep that bounded: the spec's assertion is
    about the control schema and the one it moved out of.
    """
    out = []
    for schema in schemas:
        tables = [r.asDict()["table_name"] for r in spark.sql(
            f"SELECT table_name FROM `{catalog}`.information_schema.tables "
            f"WHERE table_schema = '{schema}'").collect()]
        for table in tables:
            props = {r.asDict().get("key"): r.asDict().get("value")
                     for r in spark.sql(
                         f"SHOW TBLPROPERTIES `{catalog}`.`{schema}`.`{table}`").collect()}
            out.append((schema, table, props.get(CONTROL_OBJECT_PROPERTY) == "true"))
    return out


def table_privileges(spark, catalog: str, schema: str):
    """(grantee, table_name, privilege) for every table-level grant in one schema.

    One query per schema rather than SHOW GRANTS per table: the sweep has to cover every
    table including ones this repo never declared, which is the whole point -- a grant on
    a __materialization_* twin is exactly what would not appear in a declared-model list.
    """
    rows = spark.sql(
        f"SELECT grantee, table_name, privilege_type "
        f"FROM `{catalog}`.information_schema.table_privileges "
        f"WHERE table_schema = '{schema}'"
    ).collect()
    out = []
    for r in rows:
        d = r.asDict()
        grantee, table, priv = (d.get("grantee"), d.get("table_name"),
                                d.get("privilege_type"))
        if not grantee or not table or not priv:
            raise ValueError(
                f"table_privileges for {catalog}.{schema} returned a row this gate "
                f"cannot read: {d}. Refusing to treat it as 'no grant'."
            )
        out.append((grantee, table, priv))
    return out


def grants(spark, securable: str, name: str):
    """(principal, privilege) pairs currently assigned on one securable.

    RAISES if a returned row cannot be parsed, and never silently yields fewer pairs
    than rows. `SHOW GRANTS` column names differ between the SQL surface and the REST
    API (`Principal`/`ActionType` vs `principal`/`action_type`), and a reader that
    quietly returned [] on an unrecognised shape would make this gate pass over any
    grant at all -- vacuously, permanently, and invisibly. That is the precise failure
    this repo has already shipped four times.
    """
    rows = spark.sql(f"SHOW GRANTS ON {securable} {name}").collect()
    out = []
    for r in rows:
        d = r.asDict()
        principal = d.get("Principal") or d.get("principal")
        privilege = (d.get("ActionType") or d.get("action_type")
                     or d.get("Privilege") or d.get("privilege"))
        if not principal or not privilege:
            raise ValueError(
                f"SHOW GRANTS ON {securable} {name} returned a row this gate cannot "
                f"read: {d}. Refusing to treat it as 'no grant' -- fix the column "
                f"names in grants() rather than let the gate pass over it."
            )
        out.append((principal, privilege))
    return out


def schema_is_absent(exc: Exception) -> bool:
    """True when exc means the securable does not exist yet -- NOT that it exists and
    could not be read.

    gold_build is manually triggered and, as of this gate, has never run: the gold
    catalog holds only information_schema, so every one of gold_layout.SCHEMAS is
    absent, not ungranted. A schema that does not exist cannot carry a broad grant, so
    its absence is nothing to assert -- but a schema that DOES exist and raises on read
    (a permission error, a transient failure, anything else) must still fail this gate.
    Databricks surfaces the "does not exist" case as SCHEMA_NOT_FOUND, as a
    NoSuchDatabaseException/NoSuchNamespaceException, or as a message containing "not
    found" / "does not exist" -- matched narrowly and case-insensitively so a real
    "cannot read" failure is never swallowed here.
    """
    msg = str(exc)
    cls = type(exc).__name__
    upper = msg.upper()
    low = msg.lower()
    return (
        "SCHEMA_NOT_FOUND" in upper
        or "NOSUCHDATABASEEXCEPTION" in cls.upper()
        or "NOSUCHNAMESPACEEXCEPTION" in cls.upper()
        or "not found" in low
        or "does not exist" in low
    )


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", action="append", default=None,
                    help="a vault schema; repeatable. Defaults to raw_vault and "
                         "business_vault.")
    # The gate's own run on 27 September inspected silver, control and bronze -- and
    # NOT gold, so a catalog- or schema-level SELECT there would have passed unseen.
    # NOT defaulted: a guessed gold catalog name is exactly the kind of wrong-but-
    # plausible default that lands a grant in the wrong place quietly. Omit it and the
    # gold sweep is reported NOT_EVALUATED, never skipped in silence.
    ap.add_argument("--gold-catalog", default=None, metavar="CATALOG",
                    help="the gold catalog; sweeps it and every schema in "
                         "gold_layout.SCHEMAS through the same securable-inspection "
                         "path as the vault catalog. Omit to skip (reported, not "
                         "silent).")
    # DEF-46: a catalog this repo READS and does not own, whose posture it depends on.
    # apply_masks.sql used to REVOKE on the bronze catalog to enforce this. That was
    # wrong twice over: Bronze belongs to another team and has its own reader groups, and
    # two teams writing grants to one catalog means whichever ran last wins with neither
    # one's code saying so. The dependency is real -- the `<source>_raw` schemas hold
    # UNMASKED PII under decision D3, and the vault reads them so business keys hash true
    # identifiers -- so it is asserted here instead. If this fails, the fix is a
    # conversation with the team that owns the catalog, not a grant from us.
    ap.add_argument("--assert-not-world-readable", action="append", default=None,
                    metavar="CATALOG",
                    help="a catalog this repo depends on NOT being readable by "
                         "`account users`; repeatable. Asserted, never changed.")
    # DEF-47: the catalog's isolation mode. Not set here -- DESCRIBE CATALOG EXTENDED
    # reports this catalog as "Managed by Terraform", and the ALTER that used to attempt
    # it was the one statement of 49 that failed, with a parse error, because
    # isolation-mode DDL is not valid on this runtime. Separate catalogs are NOT
    # isolation: the metastore is shared across workspaces, so without the binding a
    # production catalog is queryable from a staging workspace by anyone holding the
    # catalog grant. Asserted here so that stops being an assumption.
    ap.add_argument("--assert-isolated", action="append", default=None,
                    metavar="CATALOG",
                    help="a catalog that must have isolation_mode ISOLATED; repeatable. "
                         "Asserted, never changed.")
    # DECIDED 26 Aug 2026 (Phase 6STOP). Repeatable, and NOT defaulted: a default here
    # would be a guess at which group is privileged in this lake, and a wrong guess
    # allow-lists the very grant this exists to catch. Omitted entirely, the table sweep
    # is reported NOT EVALUATED rather than passing vacuously.
    ap.add_argument("--allow-table-select", action="append", default=None,
                    metavar="PRINCIPAL",
                    help="a principal permitted to hold table-level SELECT in the vault "
                         "schemas; repeatable. Anything else holding it fails the gate. "
                         "Omit to skip the table sweep (reported, not silent).")
    ap.add_argument("--allow-test-objects", action="store_true",
                    help="permit tst_ synthetic objects; use only while a dashboard "
                         "exercise is actually in progress")
    ap.add_argument("--declared-schema", action="append", default=None,
                    metavar="SCHEMA",
                    help="a schema this repo declares; repeatable. Any other non-system "
                         "schema in the catalog fails the gate. Omit to skip (reported).")
    # Spec section 6. Repeatable pair: --control-object-schema names every schema to sweep
    # for the hfig.control_object marker, and --control-schema names which of them is the
    # one control objects belong in. Neither defaulted: a default would guess which schema
    # is authoritative, and a wrong guess inverts the assertion.
    ap.add_argument("--control-schema", default=None, metavar="SCHEMA",
                    help="the schema control objects belong in. Required for the "
                         "control-object sweep; omit to skip it (reported, not silent).")
    ap.add_argument("--control-object-schema", action="append", default=None,
                    metavar="SCHEMA",
                    help="a schema to sweep for the hfig.control_object marker; "
                         "repeatable. Include the control schema itself.")
    args = ap.parse_args()
    schemas = args.schema or ["raw_vault", "business_vault"]

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    problems: list[str] = []
    asserted = 0
    not_evaluated = 0

    # The catalog first: a read grant here cascades into every schema below and would
    # make the per-schema assertions moot.
    try:
        _g = grants(spark, "CATALOG", f"`{args.catalog}`")
        problems += offending(_g, "CATALOG", args.catalog)
        asserted += 1
        print(f"  checked CATALOG {args.catalog}: {len(_g)} assignment(s) -- "
              f"{sorted({p for _pr, p in _g}) or 'none'}")
    except Exception as exc:  # noqa: BLE001
        print(f"  ERROR reading grants on CATALOG {args.catalog}: {exc}")
        problems.append(f"CATALOG {args.catalog}: grants could not be read ({exc}). "
                        f"A run that cannot read them has asserted nothing.")

    for schema in schemas:
        target = f"`{args.catalog}`.`{schema}`"
        try:
            _g = grants(spark, "SCHEMA", target)
            problems += offending(_g, "SCHEMA", f"{args.catalog}.{schema}")
            asserted += 1
            print(f"  checked SCHEMA {args.catalog}.{schema}: {len(_g)} assignment(s) "
                  f"-- {sorted({p for _pr, p in _g}) or 'none'}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR reading grants on SCHEMA {args.catalog}.{schema}: {exc}")
            problems.append(
                f"SCHEMA {args.catalog}.{schema}: grants could not be read ({exc}). "
                f"A run that cannot read them has asserted nothing.")

        # The per-TABLE sweep, same schema. It follows the schema check rather than
        # replacing it: a broad grant and a misdirected narrow one are different holes.
        if args.allow_table_select:
            try:
                _tp = table_privileges(spark, args.catalog, schema)
                problems += unauthorised_table_readers(
                    _tp, args.catalog, schema, args.allow_table_select)
                asserted += 1
                print(f"  checked TABLES in {args.catalog}.{schema}: {len(_tp)} "
                      f"table-level assignment(s) over "
                      f"{len({tb for _g2, tb, _p2 in _tp})} table(s); allowed "
                      f"{sorted(args.allow_table_select)}")
            except Exception as exc:  # noqa: BLE001
                print(f"  ERROR reading table grants in {args.catalog}.{schema}: {exc}")
                problems.append(
                    f"TABLES in {args.catalog}.{schema}: grants could not be read "
                    f"({exc}). A run that cannot read them has asserted nothing.")
        else:
            not_evaluated += 1
            print(f"  NOT EVALUATED: table-level grants in {args.catalog}.{schema} "
                  f"-- no --allow-table-select given, so the Phase 6STOP posture "
                  f"(no consumer reads the vault) is unasserted for this schema")

    # Gold: the gate's own run on 27 September listed silver, control and bronze --
    # NOT gold, so a catalog- or schema-level SELECT there would have passed unseen.
    # Same securable-inspection path as the vault catalog above: a catalog grant here
    # cascades into every gold schema and would make the per-schema sweep moot too.
    if args.gold_catalog:
        try:
            _g = grants(spark, "CATALOG", f"`{args.gold_catalog}`")
            problems += offending(_g, "CATALOG", args.gold_catalog)
            asserted += 1
            print(f"  checked CATALOG {args.gold_catalog}: {len(_g)} assignment(s) -- "
                  f"{sorted({p for _pr, p in _g}) or 'none'}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR reading grants on CATALOG {args.gold_catalog}: {exc}")
            problems.append(
                f"CATALOG {args.gold_catalog}: grants could not be read ({exc}). A run "
                f"that cannot read them has asserted nothing.")

        for gold_schema in gold_layout.SCHEMAS:
            target = f"`{args.gold_catalog}`.`{gold_schema}`"
            try:
                _g = grants(spark, "SCHEMA", target)
                problems += offending(_g, "SCHEMA", f"{args.gold_catalog}.{gold_schema}")
                asserted += 1
                print(f"  checked SCHEMA {args.gold_catalog}.{gold_schema}: "
                      f"{len(_g)} assignment(s) -- {sorted({p for _pr, p in _g}) or 'none'}")
            except Exception as exc:  # noqa: BLE001
                if schema_is_absent(exc):
                    not_evaluated += 1
                    print(f"  NOT EVALUATED: SCHEMA {args.gold_catalog}.{gold_schema} "
                          f"does not exist yet -- gold_build has not created it, so "
                          f"there is nothing to assert (absence is not a broad grant)")
                else:
                    print(f"  ERROR reading grants on SCHEMA "
                          f"{args.gold_catalog}.{gold_schema}: {exc}")
                    problems.append(
                        f"SCHEMA {args.gold_catalog}.{gold_schema}: grants could not be "
                        f"read ({exc}). A run that cannot read them has asserted nothing.")
    else:
        not_evaluated += 1
        print(f"  NOT EVALUATED: gold -- no --gold-catalog given, so a catalog- or "
              f"schema-level SELECT in gold would pass this gate unseen. Subsystem C "
              f"puts real money data there.")

    if args.declared_schema:
        try:
            _rows = [(r.asDict()["schema_name"], r.asDict()["schema_owner"])
                     for r in spark.sql(
                         f"SELECT schema_name, schema_owner FROM "
                         f"system.information_schema.schemata "
                         f"WHERE catalog_name = '{args.catalog}'").collect()]
            problems += undeclared_schemas(_rows, args.declared_schema)
            asserted += 1
            print(f"  checked SCHEMAS in {args.catalog}: {len(_rows)} present, "
                  f"{len(args.declared_schema)} declared")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR listing schemas in {args.catalog}: {exc}")
            problems.append(f"SCHEMAS in {args.catalog}: could not be listed ({exc}). "
                            f"A run that cannot list them has asserted nothing.")
    else:
        not_evaluated += 1
        print(f"  NOT EVALUATED: undeclared schemas in {args.catalog} -- no "
              f"--declared-schema given")

    # Spec section 6: load control has one home, and the marker that says so is read.
    if args.control_schema and args.control_object_schema:
        try:
            _co = control_object_rows(spark, args.catalog, args.control_object_schema)
            problems += misplaced_control_objects(_co, args.control_schema)
            problems += synthetic_objects(
                [(_s, _tbl) for _s, _tbl, _marker in _co], args.allow_test_objects)
            asserted += 1
            _marked = [f"{s}.{tb}" for s, tb, m in _co if m]
            print(f"  checked CONTROL OBJECTS across "
                  f"{sorted(args.control_object_schema)}: {len(_co)} table(s), "
                  f"{len(_marked)} marked -- {sorted(_marked) or 'none'}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR reading control-object markers: {exc}")
            problems.append(
                f"CONTROL OBJECTS in {args.catalog}: markers could not be read ({exc}). "
                f"A run that cannot read them has asserted nothing.")
    else:
        not_evaluated += 1
        # NAMES BOTH GATES THIS BLOCK CARRIES. The skipped block runs
        # misplaced_control_objects() AND synthetic_objects(), so a run without these two
        # arguments has also asserted nothing about leftover tst_ tables -- and that is the
        # only gate that notices one. An operator told only about "control-object
        # placement" would reasonably conclude the synthetic sweep had run and passed.
        print(f"  NOT EVALUATED: control-object placement AND the synthetic tst_ object "
              f"sweep in {args.catalog} -- --control-schema and --control-object-schema "
              f"are both required for them. Neither has been asserted: a leftover tst_ "
              f"table would go unnoticed by every other gate")

    # DEF-46: catalogs we depend on but do not govern.
    for other in (args.assert_not_world_readable or []):
        try:
            g = grants(spark, "CATALOG", f"`{other}`")
            world = [(pr, pv) for pr, pv in g
                     if pr.lower() in ("account users", "users")
                     and pv.upper().replace("_", " ") in {p.replace("_", " ")
                                                          for p in READING}]
            asserted += 1
            print(f"  checked CATALOG {other} (not ours): {len(g)} assignment(s) -- "
                  f"{sorted({p for _pr, p in g}) or 'none'}")
            for principal, privilege in world:
                problems.append(
                    f"CATALOG {other}: `{principal}` holds {privilege}. This repo READS "
                    f"this catalog and does not govern it -- its `_raw` schemas hold "
                    f"unmasked PII (decision D3), and the vault reads them so business "
                    f"keys hash true identifiers. Raise it with the team that owns the "
                    f"catalog. Do NOT add a REVOKE here: two teams writing grants to one "
                    f"catalog means whichever ran last wins."
                )
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR reading grants on CATALOG {other}: {exc}")
            problems.append(f"CATALOG {other}: grants could not be read ({exc}). A run "
                            f"that cannot read them has asserted nothing.")

    # DEF-47: isolation mode is not exposed to SQL -- DESCRIBE CATALOG EXTENDED does not
    # carry it -- so this reads the API. A run that cannot read it is a FAILURE, not a
    # skip: "we could not check" and "it is isolated" must never look the same.
    for cat in (args.assert_isolated or []):
        try:
            from databricks.sdk import WorkspaceClient

            mode = str(WorkspaceClient().catalogs.get(cat).isolation_mode or "")
            asserted += 1
            if "ISOLATED" not in mode.upper():
                problems.append(
                    f"CATALOG {cat}: isolation_mode is {mode or 'unset'}, not ISOLATED. "
                    f"The metastore is shared across workspaces, so an OPEN catalog is "
                    f"queryable from every workspace on it by anyone holding the catalog "
                    f"grant. This catalog is Terraform-managed: fix it there, not here."
                )
            else:
                print(f"  checked CATALOG {cat} isolation: {mode}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR reading isolation mode for {cat}: {exc}")
            problems.append(
                f"CATALOG {cat}: isolation mode could not be read ({exc}). A run that "
                f"cannot read it has asserted nothing -- do not read this as isolated."
            )

    print(f"\n{asserted} securable(s) inspected")

    if problems:
        print(f"\nSCHEMA GRANT GATE FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        return finish("FAILED", asserted, not_evaluated, 1)

    if not asserted:
        print("\nGATE NOT EVALUATED: no securable could be read.")
        return finish("NOT_EVALUATED", 0, not_evaluated, 1)

    print(f"\nSCHEMA GRANT GATE PASSED: no catalog- or schema-level read grant on "
          f"{args.catalog} ({asserted} securable(s) checked). The "
          f"__materialization_* backing tables are reachable only by their owner.")
    if not_evaluated:
        print(f"  ...but {not_evaluated} table-level sweep(s) were NOT EVALUATED. The "
              f"Phase 6STOP posture is unasserted for those schemas -- pass "
              f"--allow-table-select to close that.")
    return finish("PASSED", asserted, not_evaluated, 0)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
