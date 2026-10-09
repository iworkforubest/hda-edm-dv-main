"""
HARD GATE: prove masks exist AND survive projection.

Three assertions:
  1. Every column the metadata marks personal/financial/restricted carries a mask.
     Derived from metadata, so a new sensitive satellite cannot ship unmasked.
  2. The mask still applies when the column is read through a derived object
     (the _v1 view, a PIT table, a Gold view).
  3. CATALOGUE-WIDE (DEF-26): a column NAME declared masked anywhere in the metadata
     carries a mask on EVERY table in the vault schema where that name appears --
     quarantine twins included. Assertions 1 and 2 walk only the entities that DECLARE
     masks, so they reported PASSED over a total bypass: hubs carried the whole staged
     Bronze row, including debitamt, and a hub declares no masks. See unmasked_elsewhere.

Assertion 2 is the one to run first on day 1 and the one whose result may change the
model. If a mask does not propagate through a materialized view, sensitive columns
must live in satellites that no Gold object projects.

INACTIVE TABLES ARE ANNOUNCED, NOT ASSERTED OVER. A table whose every source binding is
inactive in this lake is created from its ghost flow alone: hash key plus the six system
columns, no payload. The factory therefore emits no MASK clause on it and no _v1
projection -- there is no column to mask and no data to protect. Walking those tables
here would report a missing mask on a column that does not exist, and failing the gate on
it would block a deploy over nothing. Activity is read from the same declared list the
generator uses (`--active-sources`) through spec.active_table_bindings, so this file and
factory.build cannot disagree about which tables are inactive.

The metadata-level assertion -- declared sensitive means declared masks -- still runs for
EVERY entity, active or not. That one is about the model, not about a lake.
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

SENSITIVE = ("personal", "financial", "restricted")

# DEF-53: populated in main() from information_schema before the sweep runs. Module-level
# so exemption() stays a pure name -> reason function, matching its three siblings.
_PLAIN_VIEWS: set[str] = set()


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
GATE = "mask_survival"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def masked_columns(spark, catalog: str, schema: str) -> set[tuple[str, str]]:
    rows = spark.sql(f"""
        SELECT table_name, column_name
        FROM {catalog}.information_schema.column_masks
        WHERE table_schema = '{schema}'
    """).collect()
    return {(r["table_name"], r["column_name"]) for r in rows}


def mask_remediation(kind: str) -> str:
    """WHERE the missing mask should have come from, for this kind. Pure.

    THE TWO ANSWERS ARE OPPOSITES, and until 29 September this gate gave the
    pipeline-owned one to everybody:

        "the mask must come from the TABLE DEFINITION (the factory emits it) -- an
         ALTER TABLE would not survive a pipeline update."

    That is true of a table the PIPELINE owns, and false of a STAGED kind. For a staged
    kind the pipeline owns the staging LOG; the vault table is created by a batch loader
    with `CREATE TABLE ... AS SELECT * ... WHERE 1=0`, a CTAS copies the SHAPE and NOT the
    masks, and nothing ever rewrites that table's definition -- so the mask is an ALTER,
    issued by the loader between create and insert, and an ALTER is exactly right.

    An operator handed the wrong sentence goes looking in src/accelerator/factory.py and
    finds nothing wrong there, because nothing IS wrong there. naming.STAGED_KINDS is read
    rather than a kind list spelled here, so a kind that becomes staged later moves its own
    remediation text with it.
    """
    if kind in naming.STAGED_KINDS:
        return (
            "This kind is STAGED: the pipeline owns stg_<table> and the vault table is "
            "created from it by a BATCH LOADER (checks/load_hubs.py for a keyed kind, "
            "checks/load_satellites.py for a satellite) with CREATE TABLE ... AS SELECT "
            "* ... WHERE 1=0. A CTAS copies the shape and NOT the masks, so the mask is "
            "an ALTER TABLE ... ALTER COLUMN ... SET MASK issued by that loader between "
            "the create and the insert -- load_satellites.mask_statements(). Nothing in "
            "the factory emits it for this table; do not go looking there. Check the "
            "loader's task log for the `masked ...` lines, and that it was handed "
            "--active-sources."
        )
    return (
        "These are streaming tables and materialized views, so the mask must come from "
        "the TABLE DEFINITION (the factory emits it) -- an ALTER TABLE would not survive "
        "a pipeline update."
    )


def v1_problems(entity, table: str, have, plain_views) -> list:
    """Findings for the `_v1` projection of a masked satellite. Pure, so it is testable.

    DEF-53 is stated in exemption() and was not applied here, so this demanded a mask a
    plain view cannot carry. That comment predicted the consequence exactly -- "blocks
    every load once a masked satellite is activated" -- and on 25 September satellites
    were activated for the first time and it did: seven findings against
    sat_invoice_header_fieldglass_us_v1 and sat_invoice_line_details_fieldglass_us_v1,
    raised the moment the base tables were correctly masked.

    MEASURED 26 August in this workspace
    (docs/superpowers/evidence/2026-08-26-mask-propagation-through-views.md): a view
    CANNOT declare a mask -- `CREATE VIEW (col MASK fn)` and `ALTER VIEW ... SET MASK` are
    both syntax errors on this runtime -- and it does not need to, because reading an
    unprivileged identity through a plain view returned empty exactly as the base table
    did.

    A MATERIALIZED VIEW IS STILL A FINDING, and that distinction carries the whole safety
    argument: an MV stores a COPY, which the base table's mask does not reach. plain_views()
    selects table_type = 'VIEW' precisely and must not be widened to MATERIALIZED_VIEW.
    """
    if not entity.masks or entity.kind not in ("sat", "msat", "csat"):
        return []
    v1 = f"{table}_v1"
    if v1 in plain_views:
        return []
    return [
        f"{v1}.{column} is a projection of masked {table}.{column} but carries no mask "
        f"of its own, and it is not a plain view. A mask on the base table does not "
        f"reach a stored copy."
        for column, _fn in entity.masks if (v1, column) not in have
    ]


def stable_view_problems(entity, table: str, stable: str, plain_views) -> list:
    """Findings for the STABLE view over a masked table -- the object
    checks/load_hubs.py and checks/load_satellites.py publish after every load, and the
    one every consumer now reads instead of `table`.

    SCOPED TO naming.STAGED_KINDS, DELIBERATELY, not to every entity this gate walks.
    The scope is "a loader publishes this kind's view", and naming.STAGED_KINDS is the
    set that answers it: checks/load_hubs.py owns the keyed kinds and
    checks/load_satellites.py the satellite kinds, and between them they cover it exactly.
    Asserting existence for a kind neither loader touches would fail this gate on a lake
    working exactly as built, which is why the guard is a scope test and not a formality.

    WHAT THAT SET COVERS CHANGED ON 29 SEPTEMBER, and this function did not. Until then
    a link, NHL or HAL was written straight into its physical table by the SDP pipeline
    and got no stable view at all, so it fell outside this guard; staging every keyed kind
    put it inside. The expectation in tests/test_accelerator.py flipped with it -- a masked
    NHL whose stable view is missing is a finding again -- without a line moving here,
    because the scope was always read from naming rather than typed out. A kind that is
    still genuinely outside it (`pit`, platform-owned, loaded by nothing) is what the
    suite mutates against, so the guard cannot rot into a constant that says yes to
    everything.

    THE CHECK IS EXISTENCE, NOT A DECLARED MASK -- DEF-53 already settled that half: a
    plain view carries no mask and needs none, because the mask on the underlying column
    applies to anything selecting from it, view included. What DEF-53 did not measure is
    that the view is actually THERE: a missing stable view over masked data leaves every
    consumer reading the unversioned name with TABLE_OR_VIEW_NOT_FOUND while the mask
    sits, correctly but unreachably, on a table nobody is pointed at. And a MATERIALIZED
    stable view -- this repo's own loaders never emit one, but nothing at this layer
    stops a hand-run CREATE MATERIALIZED VIEW from shadowing the plain one -- WOULD be a
    genuine leak, exactly as for sat_x_v1: an MV stores a copy the base table's mask does
    not reach.
    """
    if not entity.masks or entity.kind not in naming.STAGED_KINDS:
        return []
    if stable not in plain_views:
        return [
            f"{stable} is the stable view over masked {table} but does not exist as a "
            f"plain view. Consumers read {stable}, never {table} directly -- a missing "
            f"or materialized stable view over masked data is the same leak DEF-53 "
            f"describes for the _v1 view."
        ]
    return []


# F3: nothing asserted the stable view EXISTS for anything except the intersection of
# "declares a mask" and "is a staged kind" -- 2 tables in usnc_tds, measured. Every hub,
# every unmasked satellite, and every masked satellite whose kind naming.STAGED_KINDS
# does not cover had its view's existence asserted by NOTHING. That matters beyond
# masking: if load_hubs.py's or load_satellites.py's CREATE OR REPLACE VIEW statement
# silently stopped firing -- a name collision, a permission error swallowed by a broad
# except, or the F2 rewrite itself regressing -- every consumer reading the unversioned
# name would get TABLE_OR_VIEW_NOT_FOUND while every gate that reads .tables() (the
# PHYSICAL name, never the view) stayed green, because none of the four hard gates
# reads the view at all.
#
# WIDENED AGAIN, 26 September, to close the OTHER half of the same gap: link, NHL and
# HAL got no stable view from anything until checks/publish_stable_views.py existed
# (spec 4.4). That was a known, separately-handled exclusion -- the message below used
# to print it by name and by count so a green run could not be misread as "every table
# has a view". Now every generatable kind has a publisher, the exclusion no longer
# exists, and the message must not go on claiming one.
def stable_view_existence_problems(entity, table: str, stable: str, plain_views) -> list:
    """Findings for a MISSING stable view over ANY table SOMETHING publishes one for --
    masked or not. See the module-level F3 comment above for why stable_view_problems()
    alone was not enough: it only ever fires for an entity that DECLARES a mask.

    COVERS EVERY KIND IN naming.GENERATABLE, not just naming.STAGED_KINDS. Until
    checks/publish_stable_views.py existed, link, NHL and HAL were pipeline-owned and
    got NO stable view from anything -- neither load_hubs.py nor load_satellites.py
    runs for those kinds, and that WAS a known, separately-handled gap (spec 4.4, 6.5).
    publish_stable_views.py closes it: every kind this model can generate now has a
    publisher for its stable view (load_hubs.py / load_satellites.py for a staged
    kind, publish_stable_views.py for a pipeline-owned one), so this function's scope
    widens to match rather than continuing to name an exclusion that no longer exists.
    A kind outside naming.GENERATABLE -- none is declared today -- stays out of scope,
    defensively: asserting existence over an object nothing in this codebase claims to
    produce would fail a lake working exactly as built.
    """
    if entity.kind not in naming.GENERATABLE:
        return []
    if stable not in plain_views:
        return [
            f"{stable} is the stable view over {table} but does not exist as a plain "
            f"view. Consumers read {stable}, never {table} directly -- a publisher "
            f"whose view statement silently stopped firing would leave every consumer "
            f"reading {stable} with TABLE_OR_VIEW_NOT_FOUND while every hard gate, "
            f"which reads .tables() (the physical name), stayed green."
        ]
    return []


def plain_views(spark, catalog: str, schema: str) -> set[str]:
    """Object names in this schema that STORE NOTHING -- plain views only.

    DEF-53. A mask propagates through a plain view and does not propagate into a
    materialized one. Measured 26 Aug 2026
    (docs/superpowers/evidence/2026-08-26-mask-propagation-through-views.md): reading a
    masked column as an identity the mask denies returns empty through the base table AND
    through a plain view over it, because a view copies nothing and the base table's mask
    applies at read time.

    `table_type = 'VIEW'` is the whole test, and it must NOT be widened to
    'MATERIALIZED_VIEW': an MV stores a copy, which is 3a section 1.3's finding and the
    reason the pipeline's own `_v1` redeclares its masks.
    """
    rows = spark.sql(f"""
        SELECT table_name FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}' AND table_type = 'VIEW'
    """).collect()
    return {r["table_name"] for r in rows}


def schema_columns(spark, catalog: str, schema: str) -> set[tuple[str, str]]:
    """Every (table, column) in the vault schema -- tables, views, quarantine twins."""
    rows = spark.sql(f"""
        SELECT table_name, column_name
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
    """).collect()
    return {(r["table_name"], r["column_name"]) for r in rows}


# --------------------------------------------------------------------------- #
# ASSERTION 3, CATALOGUE-WIDE: a masked column NAME must be masked EVERYWHERE.
#
# THE ASSERTIONS ABOVE COULD NOT HAVE CAUGHT DEF-26, and that is why this one exists.
# They walk the entities that DECLARE masks and look up those entities' own tables. They
# never ask the opposite question -- does this column name appear, unmasked, on some
# table that declares nothing? -- so a total bypass reported PASSED:
#
#     nhl_general_journal_line   2,453,132 rows            0 readable debitamt
#     hub_accounting_journal     2,759,294 rows    2,759,292 readable debitamt
#
# One column, one set of values, the control applied on one table and absent on the
# other, because factory._stage appended the whole staged Bronze row to every vault
# table and factory._emit_target emits MASK clauses only for an entity that DECLARES
# masks -- which a hub, holding only keys, never does. The projection (DEF-26) removes
# the columns; this removes the blind spot, and the reviewer was explicit that the second
# matters more: the projection is a bug, this is why nothing caught it.
#
# IT IS A NAME-LEVEL ASSERTION, DELIBERATELY. It does not ask whether the values are the
# same values, or whether the entity meant the same thing by the name. `debitamt` is
# GP's name for a debit amount wherever it appears, and a column name declared sensitive
# ONCE in this model is sensitive under that name everywhere in this schema. The false
# positive -- an unrelated column that happens to share a masked name -- costs a mask
# nobody needed. The false negative cost 2.7 million readable amounts.
#
# TWO KINDS OF OBJECT ARE EXCLUDED, BOTH LISTED, neither silently skipped -- see
# exemption() for the reasons and for the one that is a recorded limitation rather than
# a clean boundary.
# --------------------------------------------------------------------------- #
def exemption(table: str) -> str | None:
    """Why this object sits outside the sweep, or None. Every reason is PRINTED.

    PLATFORM-OWNED -- `ref_` / `ctl_` / `reg_` / `agg_` / `doc_` (naming.PLATFORM_OWNED,
    the ARB boundary rule). This generator reads them by join and never creates them, so
    a same-named column there is a different concern with a different owner.

    SDP MATERIALIZATION BACKING TABLES -- `__materialization_mat_<uuid>_<table>_N`, the
    runtime's own storage for a streaming table. This one is a RECORDED LIMITATION, not
    a clean boundary, and it must not be read as an all-clear: measured on 25 Aug 2026,
    the backing table for the ALREADY-MASKED nhl_general_journal_line carried debitamt
    and crdtamnt with no mask of their own. The mask lives on the streaming table, not on
    the storage underneath it. That is a property of the platform which predates this
    gate and is identical for every masked table in every lake -- so asserting over these
    would fail permanently and prove nothing about this model. What it means in practice
    is that read access to raw_vault's internal objects must be treated as access to
    unmasked values, which is a GRANT question: checks/apply_governance.py owns it.
    """
    if table.startswith("__"):
        return "SDP-internal materialization backing table (see exemption())"
    if table in _PLAIN_VIEWS:
        # DEF-53: a plain view stores nothing, so it cannot hold an unmasked copy -- the
        # base table's mask applies when the view is read, probed and recorded. Requiring
        # a declared mask here would be a false positive that blocks every load once a
        # masked satellite is activated, because a view CANNOT declare one on this runtime.
        #
        # This is safe for a reason worth stating: a view exposes only what some STORING
        # object holds, and every storing object is still asserted below. A missing mask
        # therefore fails the gate on the table -- the root cause -- not on a view over it.
        return "plain view: stores nothing, inherits the base table's mask (DEF-53)"
    if naming.is_platform_owned(table):
        return "platform-owned, read by join and never generated here"
    return None
def unmasked_elsewhere(declared_masked, columns, masked, is_exempt=None):
    """Problems for every occurrence of a declared-masked NAME that carries no mask.

    Pure and Spark-free so it can be fired in both directions offline -- including
    against a snapshot of the vault as DEF-26 left it, which is what proves this gate is
    not vacuous.

      declared_masked  {column_name} declared masked ANYWHERE in the metadata
      columns          {(table, column)} in the vault schema
      masked           {(table, column)} carrying a mask in information_schema
      is_exempt        table -> bool; True excludes the table (platform-owned)
    """
    problems = []
    for table, column in sorted(columns):
        if column not in declared_masked:
            continue
        if is_exempt is not None and is_exempt(table):
            continue
        if (table, column) in masked:
            continue
        problems.append(
            f"{table}.{column} carries a column name this model declares masked "
            f"elsewhere, and has no mask of its own. A masked name must be masked on "
            f"EVERY table in the vault schema where it appears -- quarantine twins "
            f"included. Either the column does not belong in {table} (the declared "
            f"model is the contract: see factory._projection), or {table}'s entity must "
            f"declare the mask."
        )
    return problems


def unexplained_emptiness(declared: int, expected: int, skipped_inactive: int,
                          active) -> bool:
    """Whether "this run asserted nothing" is a DEFECT rather than a stated absence.

    THIS FUNCTION EXISTS BECAUSE ITS PREDECESSOR COULD NOT FIRE. The guard was
    `skipped_inactive == len(not_evaluated)`, and `not_evaluated` has exactly one append
    site which increments `skipped_inactive` on the same line -- a tautology. Whenever an
    --active-sources list was supplied, which is every target that sets one, the
    unexplained branch was unreachable, so a model that stopped declaring masks on
    loading tables would have produced `expected=0`, `not_evaluated=0`, NOT_EVALUATED,
    exit 0: the PII control gone, under a green run. Its three sibling gates hard-fail
    the equivalent "the model declares nothing" case.

    Three ways nothing-asserted is a defect, and each must be able to fire on its own:

      declared == 0        the MODEL declares no masked column anywhere. The control has
                           been removed, not deferred, and no lake configuration explains
                           that.
      skipped_inactive == 0 nothing was asserted AND nothing was skipped for a declared
                           reason -- there is no stated absence to point at.
      active is None       no activity was declared at all, so "dormant by declaration"
                           is not available as an explanation.

    Only when none of the three holds is the emptiness a genuine declared dormancy.
    """
    if expected:
        return False
    return declared == 0 or skipped_inactive == 0 or active is None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    # DEF-44: REPEATABLE. This took a single --schema and the job passed only the raw
    # vault, while the loop below walks EVERY entity in the model -- including the
    # computed satellites, which live in the business vault. So csat_payroll_line_
    # classification's two masks were looked for in a schema that cannot contain it, and
    # would have reported MISSING the day its binding activates. The catalogue sweep had
    # the same blind spot: it swept one schema and reported "masked everywhere".
    ap.add_argument("--schema", action="append", required=True,
                    help="a vault schema; repeatable, and ALL of them must be given")
    ap.add_argument("--metadata", default=None)
    ap.add_argument(
        "--active-sources", default="",
        help="the target's active_sources value; empty means every binding is active",
    )
    args = ap.parse_args()

    meta_dir = Path(args.metadata) if args.metadata else \
        Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta_dir)
    active = spec.resolve_active_sources(model, args.active_sources)

    # imported here, not at module level, so tests/test_accelerator.py can import this
    # module with no Spark installed and fire unexplained_emptiness() in both directions.
    # Same precedent as the other three gates.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # One view of the estate, unioned across every vault schema. Table names are keyed
    # by kind (naming.vault_schema_for), so the schemas' table sets are disjoint by
    # construction -- asserted below rather than assumed, because a collision would make
    # a mask found in one schema vouch for a same-named table in the other.
    global _PLAIN_VIEWS
    for sch in args.schema:
        _PLAIN_VIEWS |= plain_views(spark, args.catalog, sch)
    per_schema = {sch: schema_columns(spark, args.catalog, sch) for sch in args.schema}
    overlap = set()
    seen_tables: dict[str, str] = {}
    for sch, cols in per_schema.items():
        for t in {t for t, _c in cols}:
            if t in seen_tables and seen_tables[t] != sch:
                overlap.add(f"{t} in both {seen_tables[t]} and {sch}")
            seen_tables[t] = sch

    have = set()
    for sch in args.schema:
        have |= masked_columns(spark, args.catalog, sch)

    problems: list[str] = []
    not_evaluated: list[str] = []
    skipped_inactive = 0
    expected = 0
    # F3: how many stable views this run asserted EXIST, over naming.GENERATABLE --
    # independent of whether the entity declares a mask. Counted separately from
    # `expected` (masked-column occurrences) so the print below stays honest about
    # which number is which; folded into `expected` afterwards because it is every
    # bit as much "something this gate asserted over".
    view_asserted = 0
    for entity in model.entities:
        # Every generatable kind now publishes a stable view from SOMETHING --
        # load_hubs.py / load_satellites.py for a staged kind,
        # checks/publish_stable_views.py for a pipeline-owned one (link, NHL, HAL) --
        # so this is no longer scoped to naming.STAGED_KINDS. See
        # stable_view_existence_problems()'s docstring for the history.
        has_publisher = entity.kind in naming.GENERATABLE
        # A satellite entity generates one table per source, so a mask declared once in
        # metadata must be present on EVERY generated table. Miss one and exactly one
        # source's payload ships unmasked -- the hardest kind of gap to notice.
        # zip(), not two independent loops: tables() and stable_tables() are the SAME
        # order and the SAME length by construction (Entity.stable_tables()'s own
        # docstring), so the i-th physical table always pairs with the i-th stable view.
        for (_src, table), (_src2, stable) in zip(entity.tables(),
                                                   entity.stable_tables()):
            # F3: activity is now checked for EVERY staged-kind table, not only a
            # masked one -- stable_view_existence_problems() below needs the same
            # guard the mask assertion always had: an inactive table has no loader run
            # and no view to find, and asserting existence over it would fail a lake
            # working exactly as built.
            # THE SKIP IS ABOUT ACTIVITY, NOT ABOUT KIND. It was `is_staged and not
            # active`, and an nhl/link/hal is not staged, so an INACTIVE one fell through
            # to the mask assertion below and was reported as "declares mask ... but no
            # mask is present" for a column THAT DOES NOT EXIST.
            #
            # Measured 26 September, from assert_append_only's own column dump on the
            # same run:
            #   nhl_payroll_detail_rev1 -> applied_dts, batch_id, load_dts, manifest_id,
            #                              payroll_detail_hk, rec_src
            # No gross_amount, no amount, no rate. factory._emit_ghost builds the ghost
            # from the declared field list ONLY when a schema was derived, which needs an
            # ACTIVE binding; with none it emits the hash key and the system columns
            # alone. So the factory is right -- there is no column, therefore no mask --
            # and the gate was asserting over nothing.
            if not spec.active_table_bindings(entity, _src, active):
                skipped_inactive += 1
                not_evaluated.append(
                    f"{table}: no active source binding in this lake, so the table is "
                    f"created from its ghost flow alone -- no payload column exists to "
                    f"carry {[c for c, _f in entity.masks]}, the factory emits no MASK "
                    f"clause, no _v1 and no stable view. Nothing is unmasked here; "
                    f"there is nothing here."
                )
                continue
            for column, fn in entity.masks:
                expected += 1
                if (table, column) not in have:
                    problems.append(
                        f"{table}.{column} declares mask {fn} in metadata but no mask is "
                        f"present on the table. {mask_remediation(entity.kind)}"
                    )
            problems += v1_problems(entity, table, have, _PLAIN_VIEWS)
            problems += stable_view_problems(entity, table, stable, _PLAIN_VIEWS)
            if has_publisher:
                view_asserted += 1
                problems += stable_view_existence_problems(
                    entity, table, stable, _PLAIN_VIEWS)
        if entity.sensitivity in SENSITIVE and not entity.masks:
            problems.append(
                f"{entity.base_table} is declared {entity.sensitivity} with no masks "
                f"declared in metadata"
            )

    # F3: no exclusion to print any more -- spec 4.4 / 6.5 closed it. Every kind in
    # naming.GENERATABLE now has a publisher (load_hubs.py / load_satellites.py for a
    # staged kind, checks/publish_stable_views.py for a pipeline-owned one), so a green
    # run genuinely means every table's stable view was checked, not "every table
    # except the ones nothing publishes for".
    print(f"{view_asserted} stable view(s) checked for existence, over kind(s) "
          f"{sorted(naming.GENERATABLE)} (published by load_hubs.py / "
          f"load_satellites.py for a staged kind, checks/publish_stable_views.py for a "
          f"pipeline-owned one)")

    # TWO DIFFERENT NUMBERS, and conflating them under-reports the model: `expected`
    # counts only the columns this gate could actually assert over -- masks on tables
    # that load here -- while the model declares more, on tables this lake does not load.
    # (`view_asserted` is folded in below, AFTER this print, for the same reason the
    # catalogue sweep's own count is folded in after its own print further down: this
    # message is specifically about masked COLUMNS, and folding view checks in first
    # would let their count read as if it were a subset of `declared`.)
    declared = sum(len(e.masks) for e in model.entities for _s, _t in e.tables())
    print(f"{declared} masked column(s) declared across the model; {expected} of them on "
          f"tables that load in this lake; {len(have)} masked in UC")
    expected += view_asserted

    # ---- assertion 3: catalogue-wide, by column NAME ---------------------------
    declared_masked = {c for e in model.entities for c, _fn in e.masks}
    catalogue = set()
    for _cols in per_schema.values():
        catalogue |= _cols
    if overlap:
        problems.append(
            f"two vault schemas hold a table of the same name ({sorted(overlap)}). This "
            f"gate keys its sweep by table name, so a mask on one would silently vouch "
            f"for the other. Fix the collision or key this sweep by (schema, table)."
        )
    _exempt = lambda t: exemption(t) is not None  # noqa: E731
    exempt: dict[str, int] = {}
    for _t in {t for t, _c in catalogue}:
        _why = exemption(_t)
        if _why:
            exempt[_why] = exempt.get(_why, 0) + 1
    occurrences = [(t, c) for t, c in catalogue
                   if c in declared_masked and not _exempt(t)]
    problems.extend(
        unmasked_elsewhere(declared_masked, catalogue, have, is_exempt=_exempt))
    print(f"catalogue sweep: {len(declared_masked)} distinct masked column name(s) "
          f"appear {len(occurrences)} time(s) across {len(catalogue)} column(s) in "
          f"{', '.join(args.schema)}")
    for _why, _n in sorted(exempt.items()):
        print(f"  ~ {_n} object(s) excluded: {_why}")
    # A sweep that found nothing is either an empty schema or a query that matched
    # nothing, and neither proves the control holds. It counts as asserted only when it
    # actually looked at something.
    if declared_masked and not catalogue:
        problems.append(
            f"the catalogue sweep read ZERO columns from "
            f"{args.catalog}.[{', '.join(args.schema)}].information_schema.columns. It cannot have "
            f"proven that a masked name is masked everywhere, because it saw nowhere. "
            f"Check the catalog/schema arguments before reading this run as a pass."
        )
    expected += len(occurrences)

    # ---- projection survival --------------------------------------------------
    # Recorded, not asserted, until the behaviour is confirmed in this workspace.
    _schema_list = ", ".join(f"'{sch}'" for sch in args.schema)
    derived = spark.sql(f"""
        SELECT table_name FROM {args.catalog}.information_schema.views
        WHERE table_schema IN ({_schema_list})
    """).collect()
    print(f"{len(derived)} derived object(s) in {', '.join(args.schema)}")
    print()
    print("MANUAL STEPS THIS CHECK CANNOT PERFORM:")
    print("  1. Read a masked column as an UNPRIVILEGED principal, through the base")
    print("     table AND through _v1 AND through any Gold view. See DEPLOY.md 6a.")
    print("  2. Confirm the pipeline RUN-AS identity is privileged under every mask --")
    print("     which since 28 Sep 2026 is TWO groups, not one. Money masks admit")
    print("     usnc_data_analyst_finance or scope_unmask_currency_values; PII masks admit")
    print("     pii_cleared_us or global_dataplatform_pipeline_job_runners. Membership of")
    print("     the job-runner group unmasks NO amount. Mask functions evaluate with the")
    print("     pipeline owner's rights on refresh, so an unprivileged run-as identity")
    print("     MATERIALISES NULLS into the vault. See DEPLOY.md 6b, which also lists the")
    print("     four targets that declare no run_as and load as whoever pressed Deploy.")

    if not_evaluated:
        print(f"\nNOT EVALUATED -- {len(not_evaluated)} table(s):")
        for n in not_evaluated:
            print(f"  ~ {n}")
    if problems:
        print(f"\nMASK GATE FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        return finish("FAILED", expected, len(not_evaluated), 1)
    # THE VACUITY RULE, IDENTICAL IN ALL FOUR GATES: a run that asserted nothing never
    # prints PASSED. It exits 0 only when the emptiness is a stated absence -- see
    # unexplained_emptiness() above for the three ways it is not, and for why that
    # predicate is a named function with its own tests rather than an inline condition.
    if not expected:
        if unexplained_emptiness(declared, expected, skipped_inactive, active):
            print(f"\nMASK GATE FAILED: no masked column was asserted over. "
                  f"{declared} masked column(s) are declared across the model, "
                  f"{skipped_inactive} table(s) were skipped as declared-inactive, and "
                  f"active_sources was {'not supplied' if active is None else 'supplied'}. "
                  f"That combination does not explain why this run proved nothing. If "
                  f"the model has stopped declaring masks, the control is GONE, not "
                  f"deferred -- this gate is the one that would otherwise never say so.")
            return finish("FAILED", 0, len(not_evaluated), 1)
        print("\nMASK GATE NOT EVALUATED: no masked column sits on a table that "
              "loads in this lake, so this run asserted nothing. Dormant by "
              "declaration (active_sources), not passing.")
        return finish("NOT_EVALUATED", 0, len(not_evaluated), 0)
    print(f"\nMASK GATE PASSED: {expected} sensitive column occurrence(s) are masked -- "
          f"declared masks on loading tables AND every appearance of a masked column "
          f"NAME anywhere in {', '.join(args.schema)}; {len(not_evaluated)} inactive table(s) NOT "
          f"EVALUATED (listed above).")
    return finish("PASSED", expected, len(not_evaluated), 0)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
