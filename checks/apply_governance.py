"""
Apply Unity Catalog governance: mask functions, mask bindings, grants.

WHY THIS IS A PYTHON TASK AND NOT A SQL TASK
--------------------------------------------
governance/apply_masks.sql is the authoritative DDL and stays readable and reviewable
as SQL. But it contains ${catalog} / ${vault_schema} placeholders, and parameter-marker
syntax for Jobs SQL-file tasks is not something to assume: an unresolved placeholder
fails silently as a syntax error at 03:00, or worse, resolves against the wrong catalog.

Substituting explicitly here makes the resolution visible, testable without a
workspace (see --dry-run) and identical in all four regions.

Runs after every deploy because generated pipelines create and replace tables, and a
recreated table loses attached policies. Idempotent by construction: CREATE OR REPLACE
FUNCTION, SET MASK, GRANT.
"""

from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define
# __file__, so every Path(__file__) below raised NameError and the gate died before
# asserting anything. compile() still records the real path in the code object.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import re
import sys
from pathlib import Path

SQL_FILE = Path(__file__).resolve().parents[1] / "governance" / "apply_masks.sql"

PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")


def render(sql: str, bindings: dict[str, str]) -> str:
    """Substitute ${name} placeholders, refusing to leave any unresolved."""
    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in bindings:
            raise KeyError(name)
        value = bindings[name]
        # identifiers only: reject anything that could carry SQL
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise ValueError(
                f"binding {name}={value!r} is not a bare identifier -- refusing to "
                f"interpolate it into DDL"
            )
        return value

    try:
        rendered = PLACEHOLDER.sub(_sub, sql)
    except KeyError as exc:
        raise SystemExit(
            f"unresolved placeholder ${{{exc.args[0]}}} in apply_masks.sql -- "
            f"add it to the task parameters"
        ) from exc

    left = PLACEHOLDER.findall(rendered)
    if left:
        raise SystemExit(f"placeholders still unresolved after substitution: {left}")
    return rendered


def statements(sql: str) -> list[str]:
    """Split on semicolons, dropping comments and blanks. No semicolons inside the
    DDL bodies in this file; the split is checked by the dry run before deploy."""
    out: list[str] = []
    for chunk in sql.split(";"):
        lines = [
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        body = "\n".join(lines).strip()
        if body:
            out.append(body)
    return out



# --------------------------------------------------------------------------- #
# DEF-20: THE FUNCTION/GRANT SPLIT, AND WHY IT IS A FILTER RATHER THAN A SECOND FILE
#
# raw_vault cannot define a masked table until governance.mask_money EXISTS, but
# apply_governance -- which creates it -- runs after the load in the job graph. The
# ordering was inverted, and the naive fix (move apply_governance earlier) is dangerous:
# the same file also REVOKEs ALL PRIVILEGES on the SHARED bronze catalog and grants to
# groups that DO NOT EXIST in this workspace (README RECONCILE #3, still open). The
# REVOKE is unconditional and lands; the GRANT then fails; nobody can read Bronze.
#
# So the load's prerequisite is separated from the permission changes. It is a FILTER
# over the same rendered SQL, not a copy: governance/apply_masks.sql stays the one
# definition of what a mask function is, and a function added there is picked up here
# without being added anywhere else.
#
# The selection is allowlist-first AND denylist-verified. The allowlist decides what is
# included; the denylist then re-reads what was selected and raises if anything
# permission-bearing slipped through. Belt and braces on purpose: the entire value of the
# split is that this half CANNOT do what the other half does.
# --------------------------------------------------------------------------- #
FUNCTION_ONLY_ALLOWED = (
    "USE CATALOG",                  # so the function lands in the right catalog
    "CREATE SCHEMA",                # governance schema may not exist yet
    "CREATE OR REPLACE FUNCTION",   # the mask functions themselves
)

# Anything that changes a permission or a policy. ALTER CATALOG is here because
# SET ISOLATION MODE changes catalog visibility, which is a governance decision and not
# a prerequisite of creating a function.
FUNCTION_ONLY_FORBIDDEN = (
    "REVOKE", "GRANT", "ALTER CATALOG", "SET MASK", "ALTER TABLE", "DROP ",
)


# --------------------------------------------------------------------------- #
# DEF-40: PER-TABLE SELECT GRANTS, AND WHY THERE IS NO SCHEMA-LEVEL ONE
#
# `raw_vault` holds a `__materialization_mat_<pipeline-id>_<table>_1` backing table for
# every streaming table -- the runtime's own storage, in the same schema. The MASK lives
# on the streaming table. The backing table carries the same values with none.
#
# Measured 25 Aug 2026 as an identity for which is_account_group_member(
# 'hfig_commercials_reader') is FALSE: 0 readable debitamt through the masked table,
# 2,453,131 readable through its twin. The mask is not weak, it is stepped around.
#
# `GRANT SELECT ON SCHEMA` covers every table in the schema, twins included, so it hands
# cleartext money to precisely the group mask_money exists to stop. Unity Catalog has no
# DENY and grants are additive, so it cannot be granted broadly and carved back.
#
# The grant list is therefore derived from the DECLARED MODEL. That is the whole safety
# property, and it is structural rather than vigilant: the generator never declares a
# backing table, so no backing table can appear in this list however the model changes.
# mask_survival_check.exemption() reached the same conclusion and named this file as the
# owner of the problem; this is that ownership discharged.
# --------------------------------------------------------------------------- #
def data_access_grants(model, catalog: str, vault_schema: str,
                       business_vault_schema: str, group: str,
                       active=None) -> list[str]:
    """EVERY data grant this repo would make, in one place -- and by default it makes none.

    DEFAULT OFF SINCE 2 SEP 2026, at the platform team's request: "Role groups on this
    platform deliberately carry no data grants. They define capability (workspace + compute);
    data access flows only through scope groups and explicit grants managed in the access
    repo. Manual grants are also invisible to review and will eventually fight Terraform."

    They were right that ours would fight it. Measured 2 Sep, this emitter had put 26
    table-level SELECT grants and 2 USE SCHEMA grants on `us_tds_data_engineer` -- a role
    group -- and a hand revoke would have been undone by our next successful run.

    WHY THE MACHINERY SURVIVES RATHER THAN BEING DELETED. What it encodes is not the grants
    but their SHAPE: per TABLE, derived from the declared model, never per schema or catalog,
    with quarantine twins and staging logs included because loop-1 reads them. That is DEF-40,
    it is asserted against this function's output, and it is what is worth keeping if any lake
    ever does ask us to grant. The caller decides WHETHER; this decides WHAT, and cannot
    produce a broader shape.

    THE USE SCHEMA GRANTS MOVED HERE from governance/apply_masks.sql. They were static SQL, so
    switching the per-table SELECT off would have left a schema traversal granted to a role
    group the platform team asked us to stop touching, with the SELECT that made it useful
    gone. One flag governs both, because they are one decision.

    Nothing of ours depends on them. Measured: the masks do NOT reference
    `us_tds_data_engineer`. The three PII functions admit `pii_cleared_us` and
    `global_dataplatform_pipeline_job_runners`; the two money functions admit
    `usnc_data_analyst_finance` and `scope_unmask_currency_values`. None of those four is
    the `--privileged-group` this function grants to -- that is a GRANT group
    (`scope_tds_edm_vault_read`), and the pipeline reaches the vault by OWNERSHIP rather
    than by either.
    """
    return ([f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{vault_schema}` TO `{group}`",
             f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{business_vault_schema}` "
             f"TO `{group}`"]
            + table_select_grants(model, catalog, vault_schema, business_vault_schema,
                                  group, active))


def table_select_grants(model, catalog: str, vault_schema: str,
                        business_vault_schema: str, group: str,
                        active=None) -> list[str]:
    """One `GRANT SELECT ON TABLE` per declared table, and nothing else.

    Quarantine twins are included: they hold rejected rows, loop-1 reconciliation reads
    them, and they carry their target's masks by construction. They exist only for
    bindings ACTIVE in this lake, and a grant against a table that does not exist fails
    the run -- so activity is read from the same declared list factory.build uses, and
    the two cannot disagree about which tables exist.

    WHY NO GUARD AGAINST `__materialization_*` HERE. There was one, and it was dead
    code: every name comes from Entity.base_table, which always carries its kind's
    prefix (`hub_`, `nhl_`, `qtn_`, ...), so a name beginning `__` cannot be produced
    from a declared model at all. A branch nothing can execute is not protection -- this
    project has already shipped four checks that could never fail. The real invariant is
    that this list and factory.build's emitted inventory come from the same metadata,
    and the test asserts the two are equal rather than trusting either alone.
    """
    from accelerator import naming as _naming
    from accelerator import spec as _spec

    tables: set[tuple[str, str]] = set()
    for entity in model.entities:
        schema = _naming.vault_schema_for(entity.kind, vault_schema,
                                          business_vault_schema)
        for (src, table), (_stable_src, stable) in zip(entity.tables(),
                                                        entity.stable_tables()):
            tables.add((schema, table))
            # DEF-42: a staged kind has TWO real tables -- the log the pipeline appends
            # to and the vault table the loader builds from it. Both are granted. The log
            # is not an internal like a __materialization_ twin: it is declared by this
            # model, carries the same MASK clauses as its target, and is the object
            # loop-1 reconciliation counts, so a data engineer who cannot read it cannot
            # explain a reconciliation variance.
            if entity.kind in _naming.STAGED_KINDS:
                tables.add((schema, _naming.stg(table)))
            # The twin exists exactly when that TABLE has an active binding -- decided
            # per table, not per entity, because a satellite emits one table per source
            # and they need not all be active in the same lake.
            if active is not None and _spec.active_table_bindings(entity, src, active):
                tables.add((schema, f"{_naming.PREFIX['quarantine']}"
                                    f"{table.split('_', 1)[1]}"))
                # THE STABLE VIEW IS GRANTED TOO, and it is granted under exactly the
                # same activity condition as the twin above and for the same reason: it
                # exists only where a loader or publish_stable_views ran, and a grant
                # against an object that does not exist fails the run.
                #
                # Added 26 September, after the views shipped and NOBODY COULD READ THEM.
                # This loop granted on entity.tables() alone -- the physical _rev<N>
                # names -- so the unversioned view a consumer is told to bind to (by the
                # data contract, the DBML diagram and the source-to-target mapping)
                # carried no grant at all. The mechanism was decorative: the one name
                # that survives a cutover was the one name nobody could select from.
                #
                # BOTH, not views alone, on Adrian's decision. Granting only the view
                # would make binding to a _rev<N> name impossible rather than merely
                # discouraged, which is the stronger end state -- but it would break
                # anything reading a physical name today, and that is a change to make
                # deliberately rather than as a side effect of fixing this.
                tables.add((schema, stable))

    return [
        f"GRANT SELECT ON TABLE `{catalog}`.`{schema}`.`{table}` TO `{group}`"
        for schema, table in sorted(tables)
    ]


# --------------------------------------------------------------------------- #
# TASK 5: THE TDS VISIBILITY GRANT, AND WHY IT STOPS AT USE SCHEMA
#
# Gold's five schemas exist in DDL and gold_build applies them, but nobody can see
# them: on this platform, visibility is granted, not implied by existence. This
# function is the minimum grant that makes the layer visible in the TDS test lake
# while the real access model -- who reads gold, and through which per-object list --
# is designed separately (subsystem B).
#
# IT EMITS NO SELECT BECAUSE GOLD HAS NO TABLES YET. When it does, the SELECT grants
# come per object from a declared list, exactly as table_select_grants does for the
# vault -- never ON SCHEMA. DEF-40 measured what the broader form costs: a
# schema-level grant covers SDP's unmasked __materialization_* twins alongside the
# masked table it was meant to expose, defeating every column mask. Gold will hold
# streaming tables the day subsystem C lands, so this guard has to already be here
# before there is anything in gold to leak.
#
# governance and control are NOT granted. That is silver's posture: the control
# surface is not read directly.
#
# Adrian, 27 September: the security model gets defined and tested before prod, and
# TDS should not be blind meanwhile. THIS IS A TDS CONVENIENCE AND IT IS LABELLED AS
# ONE -- subsystem B replaces it with the declared reader list (data engineers, data
# modelers, the data architect, data analysts) before production.
# --------------------------------------------------------------------------- #
def gold_access_grants(gold_catalog: str, group: str) -> list[str]:
    """USE CATALOG and USE SCHEMA on gold's READABLE schemas. No SELECT, ever.

    THIS IS A TDS CONVENIENCE AND IT IS LABELLED AS ONE. Subsystem B defines who may read
    gold -- data engineers, data modelers, the data architect, data analysts -- and
    replaces this before production. Adrian, 27 September: the security model gets defined
    and tested before prod, and TDS should not be blind meanwhile.

    IT EMITS NO SELECT BECAUSE GOLD HAS NO TABLES. When it does, the SELECT grants come
    per object from a declared list, exactly as data_access_grants does for the vault --
    never ON SCHEMA. DEF-40 measured the cost of the broader form: a schema-level grant
    covers SDP's unmasked __materialization_* twins, and gold will hold streaming tables
    the day subsystem C lands.

    governance and control are NOT granted. That is silver's posture: the control surface
    is inside the Phase 6 STOP and is not read directly.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from accelerator import gold_layout  # noqa: PLC0415

    grants = [f"GRANT USE CATALOG ON CATALOG `{gold_catalog}` TO `{group}`"]
    for schema in sorted(gold_layout.READABLE):
        grants.append(
            f"GRANT USE SCHEMA ON SCHEMA `{gold_catalog}`.`{schema}` TO `{group}`")
    return grants


def function_statements(stmts: list[str]) -> list[str]:
    """The subset needed to CREATE the mask functions, and nothing that grants."""
    selected = [
        s for s in stmts
        if s.lstrip().upper().startswith(FUNCTION_ONLY_ALLOWED)
    ]
    for stmt in selected:
        upper = stmt.upper()
        for banned in FUNCTION_ONLY_FORBIDDEN:
            if banned in upper:
                raise SystemExit(
                    f"REFUSING to run the function-only path: a selected statement "
                    f"contains {banned!r}, which this path must never execute.\n  "
                    f"{stmt.splitlines()[0][:100]}"
                )
    if not any("CREATE OR REPLACE FUNCTION" in s.upper() for s in selected):
        raise SystemExit(
            "the function-only path selected no CREATE FUNCTION statement -- "
            "apply_masks.sql has changed shape; fix the selection rather than "
            "shipping tables whose MASK clause names a function that does not exist"
        )
    return selected


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--vault-schema", default="raw_vault")
    # required, not defaulted: the Business Vault needs the same governance
    # treatment as the Raw Vault, and a guessed-wrong schema silently grants
    # on the wrong object.
    ap.add_argument("--business-vault-schema", required=True)
    # DEF-46: kept, and no longer used for a GRANT. apply_masks.sql no longer issues any
    # statement against the bronze catalog -- this repo reads Bronze and does not govern
    # it. The argument stays only so the job's parameter list is unchanged across the
    # change and so the value remains available if a future statement needs it; nothing
    # interpolates it today, and checks/schema_grant_check.py is what asserts Bronze's
    # posture now.
    ap.add_argument("--bronze-catalog", required=True)
    ap.add_argument("--gold-catalog", required=True)
    ap.add_argument("--dry-run", action="store_true",
                    help="render and split without executing; needs no workspace")
    ap.add_argument("--functions-only", action="store_true",
                    help="DEF-20: create the mask functions and NOTHING else. No GRANT, "
                         "no REVOKE, no isolation change. This is raw_vault's "
                         "prerequisite; the permission half stays in the later task.")
    # DEF-40: the SELECT grants are per TABLE and derived from the model, so this file
    # needs the model and the same activity list the generator was given.
    ap.add_argument("--metadata", default=None,
                    help="entity metadata directory (default: <repo>/metadata/entities)")
    ap.add_argument("--active-sources", default=None,
                    help="the same value passed to the pipeline. A quarantine twin "
                         "exists only for an active binding, and a grant against a "
                         "table that does not exist fails the run.")
    # DEF-46: no default. It used to default to `hfig_data_engineering`, which exists
    # nowhere in this estate, so the default was a value that could only ever fail --
    # and being a default, it failed at APPLY time rather than at configuration time.
    # The group is environment-specific (us_tds_data_engineer in TDS, something else in
    # each production workspace), so it is a per-target setting, not a constant.
    #
    # DECIDED 26 Aug 2026, and it is why this is no longer called --reader-group: the
    # vault has no consumer readers. The raw vault is not consumer-readable, and the
    # business vault only through the gold catalog. This group is the PRIVILEGED one --
    # engineering -- and passing a consumer group defeats every mask, because a vault
    # grant carries the unmasked __materialization_* twins with it (DEF-40).
    # DEFAULT OFF. Passing this makes us emit data grants again, which the platform
    # team asked us to stop on 2 Sep 2026. A flag, so the decision is visible in the
    # job definition rather than buried in whether a function gets called.
    ap.add_argument("--emit-data-grants", action="store_true",
                    help="emit USE SCHEMA and per-table SELECT to --privileged-group. OFF "
                         "by default: on this platform data access is granted through the "
                         "access repo, not by this job")
    ap.add_argument("--privileged-group", required=True,
                    help="the ONE group permitted to read vault tables directly, "
                         "receiving USE SCHEMA and per-table SELECT. NOT a consumer "
                         "group -- consumers read gold. Set it per target.")
    args = ap.parse_args()

    bindings = {
        "catalog": args.catalog,
        "vault_schema": args.vault_schema,
        "business_vault_schema": args.business_vault_schema,
        "bronze_catalog": args.bronze_catalog,
        "gold_catalog": args.gold_catalog,
        "vault_privileged_group": args.privileged_group,
    }
    rendered = render(SQL_FILE.read_text(encoding="utf-8"), bindings)
    stmts = statements(rendered)

    # DEF-40: apply_masks.sql deliberately contains no SELECT grant. A schema-level one
    # would cover the __materialization_* twins, which carry no mask; these are per
    # table and come from the declared model, where a twin cannot appear.
    if not args.functions_only:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        from accelerator import spec as _spec

        meta = Path(args.metadata) if args.metadata else (
            Path(__file__).resolve().parents[1] / "metadata" / "entities")
        _model = _spec.load_model(meta)
        _active = _spec.resolve_active_sources(_model, args.active_sources)
        if args.emit_data_grants:
            grants = data_access_grants(
                _model, args.catalog, args.vault_schema, args.business_vault_schema,
                args.privileged_group, _active)
            print(f"{len(grants)} data grant(s) derived from the model "
                  f"(no schema-level SELECT is emitted -- see DEF-40)")
            stmts = stmts + grants

            # Task 5: the TDS visibility grant. Gold has no tables yet, so this adds
            # USE CATALOG / USE SCHEMA only -- no SELECT is possible, and none is
            # emitted. Said out loud, not silently skipped: a run that grants nothing
            # must not look like a run that granted successfully (same principle as
            # the "NO DATA GRANTS EMITTED" message below).
            # TENSION, RECORDED NOT RESOLVED: these gold USE CATALOG / USE SCHEMA grants
            # ride the same --emit-data-grants flag the platform team asked us to stop
            # passing (2 Sep 2026, see the else-branch below). Whether gold's visibility
            # grants belong on a different flag, a different job, or somewhere else
            # entirely is a DESIGN decision for the deferred access-model subsystem
            # (Subsystem B, who may read gold) to resolve -- not this fix wave's to make.
            try:
                gold_grants = gold_access_grants(args.gold_catalog, args.privileged_group)
            except Exception as _gg_exc:  # noqa: BLE001 -- must not abort the run
                print(f"gold grants NOT emitted -- gold_access_grants raised {_gg_exc!r}")
            else:
                print(f"{len(gold_grants)} gold grant(s) added "
                      f"(USE CATALOG / USE SCHEMA only -- gold declares no tables yet, "
                      f"so NO GOLD SELECT WAS EMITTED)")
                stmts = stmts + gold_grants
        else:
            # SAID OUT LOUD, not silently skipped. A run that grants nothing must
            # not look like a run that granted successfully.
            print("NO DATA GRANTS EMITTED -- --emit-data-grants was not passed. "
                  "On this platform data access is granted per table through the "
                  "access repo, not by this job (platform team, 2 Sep 2026). The "
                  "vault stays reachable to the pipeline by OWNERSHIP.")

    if args.functions_only:
        stmts = function_statements(stmts)
        print(f"{len(stmts)} statement(s) rendered for catalog={args.catalog} "
              f"(FUNCTIONS ONLY -- no GRANT, no REVOKE, no isolation change)")
    else:
        print(f"{len(stmts)} statement(s) rendered for catalog={args.catalog}")

    if args.dry_run:
        for i, s in enumerate(stmts, 1):
            first = s.splitlines()[0][:88]
            print(f"  {i:2d}. {first}")
        print("\ndry run only -- nothing executed")
        return 0

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    failed: list[str] = []
    for i, stmt in enumerate(stmts, 1):
        first = stmt.splitlines()[0][:70]
        try:
            spark.sql(stmt)
            print(f"  ok   {i:2d}. {first}")
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {i:2d}. {first}\n         {exc}")
            failed.append(first)

    if failed:
        print(f"\nGOVERNANCE APPLICATION FAILED -- {len(failed)} statement(s)")
        return 1
    what = "MASK FUNCTIONS CREATED" if args.functions_only else "GOVERNANCE APPLIED"
    print(f"\n{what}: {len(stmts)} statement(s), catalog={args.catalog}")
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
