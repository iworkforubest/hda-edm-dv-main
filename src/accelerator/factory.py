"""
The factory. Turns entity metadata into Lakeflow SDP streaming tables and flows.

DESIGN DECISIONS, AND WHY
=========================

1. APPEND FLOWS ONLY IN THE VAULT.
   Every vault object is a streaming table whose flows are append flows. Append is
   the default flow type for a streaming table, so insert-only is the native path
   here rather than something we fight for. AUTO CDC / create_auto_cdc_flow is used
   in BRONZE ONLY and never for a vault table: it updates and end-dates rows in
   place, which is a direct violation of the insert-only rule the audit posture
   rests on. checks/append_only_check.py enforces this after every run.

2. MULTI-SOURCE HUBS ARE N FLOWS INTO ONE TABLE.
   create_streaming_table() declares the target, then one append_flow per source
   binding writes into it. Seven sources feeding hub_job_request is seven flows,
   not a union in a view -- so a single source's schema change or backfill affects
   exactly one flow and the others keep running.

3. CHANGE DETECTION IS DELEGATED TO BRONZE (change_detection: "cdc", the default).
   A streaming table cannot read itself, so a satellite loader cannot compare an
   incoming hashdiff against the current active row in its own target. Rather than
   reintroduce a stateful lookup (and with it a load order, killing restartability),
   Bronze produces a change stream: transactional sources deliver cdc_op natively,
   and snapshot sources are converted by AUTO CDC FROM SNAPSHOT in the Bronze
   pipeline. The satellite then appends only rows the change stream marks as
   changed. hashdiff is still computed and stored -- for audit, for cross-source
   comparison, and so that two sources supplying the same row produce the same value.

   THERE IS NO SECOND MODE. change_detection: "antijoin" was described here, and
   accepted by spec.validate, until 29 September 2026. It was implemented in no code
   path -- no flow read it, no entity declared it, and its only effect was to leave
   `changed_only` False -- so it never ran, and the flow shape it described was never
   verified. spec.validate now refuses any value but "cdc", naming its replacement.

   THE ANTI-JOIN IT NAMED IS REAL, AND IT LIVES IN A BATCH LOADER, NOT IN A FLOW.
   checks/load_hubs.py inserts into the vault table with NOT EXISTS against the keys
   already there, for every kind in naming.KEYED_KINDS, reading the `stg_` staging
   log this pipeline appends to; checks/load_satellites.py does the hashdiff
   comparison for satellites. That is the separate-job-task fallback this paragraph
   used to hold in reserve, and it is now the only path -- which is why a streaming
   table never has to read itself.

4. TYPE 2 IS A VIEW, NEVER A TABLE.
   Satellites store versions. valid_from / valid_to / is_current are computed in
   the generated <table>_v1 view. Nothing in Silver stores an end date.

5. QUARANTINE IS A SECOND FLOW, NOT A DROPPED ROW.
   Each source binding produces a valid flow and an invalid flow, so that
   landed + (quarantined - superseded) = approved reconciles for loop 1 --
   superseded being the rejects a later run legitimately re-accepted, recorded by
   checks/supersede_quarantine.py. Expectations come from the Unity Catalog
   expectations table, not from code.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Callable, Sequence

# DEF-12-adjacent, measured 29 Aug: `from pyspark import pipelines` is DEFERRED into the
# five emitters below, never imported at module level. Importing it outside a DLT pipeline
# trips Databricks' import hook and raises `Py4JJavaError o34.get`. That killed
# supersede_quarantine, which never imports factory directly -- it reaches it through
# reject_digest.digest_columns()'s own call-time `from .factory import _projection`, and
# _projection is PURE METADATA that needs no pyspark at all. Every dp.* use runs inside
# pipeline execution, so the import belongs there and nowhere else.
from pyspark.sql import DataFrame, functions as F

from . import naming
from . import VERSION
from .hashing import (
    hash_key,
    key_type_sql,
    hashdiff as hashdiff_expr,
    key_safety_rules,
    rulebook_properties,
    zero_key_sql,
)
from .spec import (
    Entity,
    Model,
    SourceBinding,
    SpecError,
    active_table_bindings,
    binding_id,
    hash_key_columns,
    hub_key_components,
    parent_legs,
    table_bindings,
)


# --------------------------------------------------------------------------- #
# Table properties
# --------------------------------------------------------------------------- #
def _properties(entity: Entity, layer: str) -> dict[str, str]:
    props = {
        "quality": layer,
        "hfig.accelerator.version": VERSION,
        "hfig.entity": entity.name,
        "hfig.grain": entity.grain,
        "hfig.kind": entity.kind,
        "hfig.domain": entity.domain,
        "hfig.sensitivity": entity.sensitivity,
        # The append-only contract, asserted post-run by checks/append_only_check.py
        "hfig.append_only": "true",
        "delta.enableChangeDataFeed": "false",
        "delta.appendOnly": "true",
    }
    props.update(rulebook_properties())
    return props


def _cluster_by(entity: Entity) -> list[str]:
    """Liquid clustering columns -- `load_dts` since 26 Aug 2026. History worth keeping.

    **This returned [] until 26 Aug 2026, and the two refusals below are why.** Refusal 1
    still stands and always will under the ratified rulebook. Refusal 2 STOPPED APPLYING:
    DEF-24 measured `load_dts` outside the statistics window because a GP journal table was
    85 columns wide, and DEF-26's projection plus DEF-41's narrowing took those tables to
    11-24 columns. `load_dts` now sits at position 7 of 11 on a hub and 20 of 24 on an NHL,
    inside the 32-column window on every entity in the model -- asserted in the suite.

    So the decision became available without the `delta.dataSkippingNumIndexedCols` change
    it had been waiting on, and it was taken. Adding it does NOT require a full refresh:
    changing clustering columns leaves existing data alone, and only new writes and
    incremental OPTIMIZE use the new layout. The 13.2M rows already loaded stay as they
    are unless someone runs `OPTIMIZE ... FULL`, which is a separate and expensive call.
    OPTIMIZE is not a mutating operation, so `append_only_check` is unaffected either way.

    WHAT THIS ACTUALLY REACHES, measured rather than assumed: the link, the NHLs and every
    `stg_` table -- staging goes through this same emit path, which is correct, since the
    loaders anti-join and LAG over the log in load order. The `qtn_` quarantine twins go
    through a separate path that does not consult this function, deliberately: rejected
    rows are not on any join access path.

    IT DOES NOT REACH THE HUBS OR THE SATELLITES. Those are not created here at all --
    `checks/load_hubs.py` and `checks/load_satellites.py` create them with
    `CREATE TABLE IF NOT EXISTS`, and they declare `CLUSTER BY` themselves for the same
    decision. Clustering only the tables this file emits would have left the largest
    tables in the vault as the only unclustered ones.

    DEPLOY.md Phase 3 assumption 1 asks whether `create_streaming_table(cluster_by=...)`
    is accepted. It is. What is NOT accepted is either column this design would cluster
    on, and the two refusals are independent:

    1. **The join keys.** DEF-23:
       `[DELTA_CLUSTERING_COLUMNS_DATATYPE_NOT_SUPPORTED] ... ledger_account_hk : BINARY`
       Hash keys are BINARY by the ratified rulebook (`BINARY_OUTPUT = True`, chosen as
       "smaller, faster joins"), and Delta will not cluster on a BINARY column.

    2. **`load_dts`, the fallback.** DEF-24:
       `[DELTA_CLUSTERING_COLUMN_MISSING_STATS] Liquid clustering requires clustering
       columns to have stats. Couldn't find clustering column(s) 'load_dts' in stats
       schema`
       Delta collects statistics for the first 32 columns only. A GP journal table is 85
       columns wide and the system columns are appended last, so `load_dts` falls outside
       the stats window on exactly the tables this project loads.

    So the argument is dropped, which is the fallback DEPLOY.md names for assumption 1.

    CLUSTERING IS A LAYOUT HINT, NOT A CORRECTNESS PROPERTY. No gate reads it, no join
    depends on it, and nothing about what is stored changes. But it is a REAL REGRESSION
    against the design's intent -- hash-key joins are the access path a Data Vault is
    built around -- so it is recorded rather than quietly dropped.

    Two ways back, both decisions rather than fixes:
      * `delta.dataSkippingNumIndexedCols` raised (or the system columns moved to the
        front of the projection) would bring `load_dts` into the stats window. That is a
        write-cost trade on every table.
      * Clustering on the key itself needs `BINARY_OUTPUT = False`, which is a rulebook
        change: a RULEBOOK_VERSION bump and every stored key rewritten -- and it is now
        RULED OUT: hash keys are BINARY(32) as stored, decided 25 Aug 2026.
    """
    return [c for c in _cluster_candidates(entity)
            if not _cluster_refusal(entity, entity.sources[0], c)]


# Delta collects file statistics for the first N columns only. Raising
# `delta.dataSkippingNumIndexedCols` is the one remaining route to clustering on
# load_dts, and it is a table property rather than a rulebook change.
STATS_COLUMNS = 32


def _cluster_candidates(entity: Entity) -> list[str]:
    """What this design WOULD cluster on, before either refusal is applied.

    It is a separate function from _cluster_by so the REFUSALS can be fired against a
    candidate in a test: with the two folded together the guard filtered a list that was
    always empty and could not fail, which is the fifth instance of that pattern found in
    this project.

    DECIDED 26 Aug 2026: `load_dts`. It was empty until then, and the reason it could be
    filled is DEF-41's narrowing rather than any change here -- see _cluster_by.

    Returned for EVERY entity, deliberately. This function states the intent; whether a
    given table can honour it is _cluster_refusal's judgement, applied per entity in
    _cluster_by. Pre-filtering here would put the refusal logic in two places and hide
    the case where a table stops being clusterable.
    """
    return [naming.COL["load_dts"]]


def _cluster_refusal(entity: Entity, src: SourceBinding, column: str) -> str | None:
    """Why `column` cannot be a clustering column on this table, or None if it can.

    DEF-44: the rule, as code rather than as a docstring. Both refusals are real and
    independent, and the second one is no longer a constant -- it depends on where the
    column sits in the DECLARED shape, which DEF-41 changed.
    """
    if column == entity.hk_column or column.endswith("_hk"):
        if key_type_sql() == "BINARY":
            return (f"{column} is a hash key, and the ratified rulebook stores keys as "
                    f"BINARY, which Delta will not cluster on (DEF-23)")

    shape = [c for c, _kind, _value in _projection(entity, src)]
    if column not in shape:
        return f"{column} is not a column of {entity.base_table}"
    position = shape.index(column)
    if position >= STATS_COLUMNS:
        return (f"{column} sits at position {position + 1} of {len(shape)} and Delta "
                f"collects statistics for the first {STATS_COLUMNS} columns only, so it "
                f"has no stats to cluster by (DEF-24)")
    return None


# --------------------------------------------------------------------------- #
# Staging: the one place hash keys, hashdiffs and system columns are computed
# --------------------------------------------------------------------------- #
# DEF-17: `current_pipeline_update_id()` DOES NOT EXIST on this runtime.
# DEPLOY.md Phase 3 assumption 4, refuted: the flow fails with
#   [UNRESOLVED_ROUTINE] Cannot resolve routine `current_pipeline_update_id`
# on search path [system.session, system.builtin, system.ai, hive_metastore.default].
#
# The fallback that entry names is "pass a batch id via pipeline configuration and read
# it with spark.conf.get". Taken with one correction: a value hardcoded in the bundle
# would be IDENTICAL for every update, and a load-run id that never changes is worse than
# no load-run id -- it looks like provenance and records nothing. So the update id the
# runtime already keeps is preferred, and an explicit `hfig.batch_id` is honoured as an
# operator override.
_BATCH_ID_CONF_KEYS = (
    "hfig.batch_id",                        # explicit operator override
    "pipelines.updateId",
    "pipelines.update.id",
    "pipelines.updateContext.updateId",
    "spark.databricks.pipelines.updateId",
)

# Resolved ONCE per update, in build(), and reused by every flow. It must be resolved
# there and not inside a flow body: discovering it needs `SET`, and SQL refuses that
# inside a query definition with UNSUPPORTED_COMMAND_IN_QUERY_DEFINITION. Resolving once
# is also the correct semantics -- one update is one load run, not one per table.
_RESOLVED_BATCH_ID: str | None = None


def _batch_id(spark) -> str:
    """The load-run id for this update, from Spark conf rather than a SQL routine."""
    tried = []
    for key in _BATCH_ID_CONF_KEYS:
        try:
            value = spark.conf.get(key)
        except Exception:  # some runtimes raise rather than returning None
            value = None
        tried.append(f"{key}={value!r}")
        if value:
            return str(value)

    # Nothing known carried it, and the runtime will not let us look for it: `SET` is
    # rejected both inside a flow (UNSUPPORTED_COMMAND_IN_QUERY_DEFINITION) and from
    # spark.sql() at build scope (UNSUPPORTED_SPARK_SQL_COMMAND, "not supported in
    # spark.sql(...) API in SDP Python"), and sparkContext.getConf() is a blocked API on
    # serverless. So the platform's update id is simply NOT REACHABLE from inside an SDP
    # Python pipeline on this runtime.
    #
    # Fall back to a generator-assigned id, resolved ONCE per update in build(). It keeps
    # the column's contract -- one distinct value per load run, constant across every
    # table in that run -- which is what hub_load_run needs. It is deliberately prefixed
    # `gen-` so it can never be mistaken for the platform's update id. To correlate a
    # load run to a platform update, join on the pipeline event log, which
    # vault_pipeline.yml already materialises.
    generated = f"gen-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    print(f"[accelerator] no platform update id reachable ({'; '.join(tried)}); "
          f"using generated batch id {generated}")
    return generated


def _system_columns(df: DataFrame, entity: Entity, src: SourceBinding) -> DataFrame:
    applied = (
        F.col(src.applied_dts_column).cast("timestamp")
        if src.applied_dts_column
        else F.lit(None).cast("timestamp")
    )
    manifest = F.col(src.manifest_column) if src.manifest_column else F.lit(None).cast("string")
    cdc_op = F.upper(F.col(src.cdc_op_column)) if src.cdc_op_column else F.lit("I")
    return (
        df.withColumn(naming.COL["load_dts"], F.current_timestamp())
        .withColumn(naming.COL["applied_dts"], applied)
        .withColumn(naming.COL["sub_seq"], F.lit(0).cast("int"))
        .withColumn(naming.COL["rec_src"], F.lit(src.name))
        .withColumn(naming.COL["batch_id"], F.lit(_RESOLVED_BATCH_ID).cast("string"))
        .withColumn(naming.COL["manifest_id"], manifest)
        .withColumn(naming.COL["cdc_op"], cdc_op)
    )


def _stage_full(entity: Entity, src: SourceBinding, spark, model: Model,
                for_schema: bool = False) -> DataFrame:
    """Read one Bronze source and derive everything the vault loaders need.

    `for_schema` READS IN BATCH INSTEAD OF STREAMING, and it exists for one reason: a
    SCHEMA does not need a stream. _derived_schema_fields calls this at pipeline DEFINITION
    time purely to read `.schema`, and doing that over `spark.readStream` makes the analyser
    plan an entire streaming query -- including the stateful dropDuplicates -- on the driver,
    before a flow is defined.

    MEASURED 25 September, across five domain pipelines, and the correlation is exact:

        payroll    no view, no schema derivation       1.0 min
        reference  reads a view, no derivation         1.2 min
        job        reads a view, no derivation         1.7 min
        finance    derives 3 schemas, reads TABLES    16.5 min
        pay_bill   derives 2 schemas, reads a VIEW    never left INITIALIZING

    pay_bill is SMALLER than finance on every structural axis -- 4 entities against 8, 3
    active flows against 8, 8 parents against 13 -- and it is the only domain that does
    both. Its update emitted seven events in 27 minutes and never defined a flow; the
    driver sat at 50-52% GC throughout.

    THE FRAME IS OTHERWISE IDENTICAL, which is what makes this safe: the same dedup, casts,
    hash expressions and projection run over it, and none of them produces a different
    SCHEMA in batch than in streaming. Only the read differs, and only when the caller wants
    a schema rather than rows. The flow bodies never pass this.

    THE WHOLE SOURCE ROW IS STILL HERE, deliberately, and this frame is NOT what lands.
    Expectations are governed configuration (checks/apply_governance.py writes them into
    UC) and may name ANY source column, not only a modelled one, so the violation
    predicate has to be evaluated while every source column is still in scope. The
    projection to the declared shape therefore runs AFTER the filter --
    see `_project` and `_register_source_flows`.
    """
    df = (spark.read.table(src.bronze_table) if for_schema
          else spark.readStream.table(src.bronze_table))

    # SOURCE CONFORMANCE, APPLIED HERE RATHER THAN IN A VIEW.
    #
    # A binding cannot express a WHERE or compute a column, which is why per-source
    # conformance used to be a view. A view is still right when the profile genuinely spans
    # many tables -- the job-posting one unions 30 and that pipeline runs in 1.7 minutes --
    # but wrong for a profile matching ONE table, because a MASKED entity reading a view
    # cannot be built: SDP reconciles the flow's streaming plan against the declared schema
    # at definition time, and through a view that never finished. Measured 25 September
    # across five domain pipelines; every combination worked except masked-through-a-view.
    #
    # THE FILTER RUNS BEFORE THE RENAMES, so it is written against the SOURCE's own column
    # names -- the same names it had when this was a view's WHERE clause, so moving a
    # profile inline does not rewrite its filter.
    if src.row_filter:
        df = df.where(src.row_filter)
    for _name, _expr in src.derived_columns:
        df = df.withColumn(_name, F.expr(_expr))

    if src.dedup_by:
        # _raw receives overlapping full extracts -- gl20000 carries ~1.8 copies of
        # every business row across 7 deliveries, ukg_raw.gl exactly 2 across 2.
        #
        # dropDuplicates, NOT a row_number() window: a partitioned ranking window is
        # not a supported streaming operation and raises at pipeline analysis time.
        # dropDuplicates is stateful but streaming-legal.
        #
        # CONSEQUENCE, measured: of 2,453,131 gl20000 keys, only 22 have copies that
        # differ in any business column. For those 22 the surviving row is arbitrary
        # rather than latest-wins, so dedup_order is DECLARED but not applied here.
        # That is acceptable while the vault holds only hubs and NHLs -- neither
        # hashes payload, and both key on columns identical across copies.
        # Latest-wins needs either Bronze-side dedup (BRZ-3) or CDF (BRZ-1).
        #
        # 29 SEPTEMBER: THAT JUSTIFICATION IS NOW LOAD-BEARING FOR A SECOND MECHANISM, and
        # its failure mode has hardened. An NHL is staged, so checks/load_hubs.py inserts
        # with NOT EXISTS over `row_number() OVER (PARTITION BY hk ORDER BY load_dts,
        # rec_src) = 1`. That ORDER BY is not a total order -- copies of one key delivered
        # in one batch share a load_dts and a rec_src -- so the winner is still arbitrary,
        # and NOT EXISTS makes it PERMANENT: first write wins per key, for ever, and a
        # later delivery carrying the corrected payload is not applied. Before staging an
        # arbitrary winner could at least be superseded by a rebuild.
        #
        # It stays acceptable on the same measured grounds -- 22 keys of 2.45M, and no
        # keyed kind hashes payload, so identity is unaffected either way. But if a keyed
        # kind ever does hash payload, or those 22 rows are ever queried for their
        # business columns, this is the line to come back to: the fix is still BRZ-3 or
        # BRZ-1, not a change to the loader.
        #
        # DEF-52: AND IT IS NOT ACCEPTABLE ON A SATELLITE. dropDuplicates keys on
        # dedup_by alone and its state is retained indefinitely (no watermark), so a key
        # seen in ANY earlier batch is dropped on sight -- before its hashdiff is
        # computed. A satellite's whole job is to record that the PAYLOAD changed under a
        # key that did not, so this would store one version per key for ever and the
        # loader's A -> B -> A guarantee would cover a B the feed could never present.
        # No satellite binding declares dedup_by (asserted in tests/test_accelerator.py);
        # within-batch duplicates go to the staging log instead, where
        # checks/load_satellites.py's LAG(hashdiff) collapses only CONSECUTIVE ones.
        df = df.dropDuplicates(list(src.dedup_by))

    # Per-binding type coercion runs here: after dedup (which must see the raw _raw
    # values -- a source may distinguish rows a cast would collapse) and before system
    # columns (stamped on the surviving, now-coerced rows only).
    #
    # This is for hashdiff stability, not the journal integrity gate: GP's debitamt/
    # crdtamnt arrive as DOUBLE, and a DOUBLE's string rendering is not stable across
    # loads, so identical amounts can produce different hashdiffs and therefore
    # spurious satellite rows forever. Casting to DECIMAL(18,2) before the payload is
    # hashed fixes that.
    #
    # withColumn(col, ...) REPLACES the existing column rather than adding a new one --
    # hashdiff_expr (hashing.py) hashes payload by column name, so a cast into a
    # differently-named column would be invisible to it.
    for _col, _typ in src.cast:
        df = df.withColumn(_col, F.col(_col).cast(_typ))

    df = _system_columns(df, entity, src)

    # WHAT GOES INTO EACH KEY IS spec.hash_key_columns' CALL, NOT THIS FUNCTION'S.
    # It returns {column: (hash components, source scope)} for every _hk column this
    # binding derives -- the exact arguments handed to hash_key below. It lives in spec.py
    # because spec.py imports without pyspark and this module does not, so a describer of
    # the model (the source-to-target mapping, a contract, a diagram) can read the real
    # derivation instead of re-deriving it by hand and drifting.
    keys = hash_key_columns(model, entity, src)

    if entity.kind == "hub":
        key_parts = hub_key_components(entity, src)
        # literals are quoted as SQL string constants for the hash expression -- they
        # normalise exactly as a column value would (CAST -> TRIM -> UPPER), never as a
        # bare column reference
        _cols, _scope = keys[entity.hk_column]
        df = df.withColumn(
            entity.hk_column, F.expr(hash_key(_cols, source_scope=_scope))
        )
        # the readable business key: never joined on, always inspectable
        bk_parts = [
            F.lit(v).cast("string") if is_literal else F.col(v).cast("string")
            for is_literal, v in key_parts
        ]
        df = df.withColumn(naming.bk(entity.name), F.concat_ws("||", *bk_parts))

    elif entity.kind in naming.LINK_KINDS:
        # one FK column per LEG, each hashed as that hub's own loader would. Legs, not
        # parents: a hierarchical link names one hub twice, and iterating parents would
        # write the second leg over the first.
        for parent, role in parent_legs(entity):
            _cols, _scope = keys[naming.hk(parent, role)]
            df = df.withColumn(naming.hk(parent, role),
                               F.expr(hash_key(_cols, source_scope=_scope)))
        _cols, _scope = keys[entity.hk_column]
        df = df.withColumn(entity.hk_column, F.expr(hash_key(_cols, source_scope=_scope)))

    else:  # satellites -- parent may be a hub or a link
        # A link-parented satellite (worktags, external codes) keys on the journal LINE,
        # not on a hub; a hub-parented one keys as the hub's own loader does when it
        # declares parent_keys, and on its own key_columns when it does not.
        # hash_key_columns makes that choice; this branch only writes the column.
        parent = entity.parents[0]
        _cols, _scope = keys[naming.hk(parent)]
        df = df.withColumn(naming.hk(parent), F.expr(hash_key(_cols, source_scope=_scope)))
        df = df.withColumn(naming.COL["hashdiff"], F.expr(hashdiff_expr(src.payload or entity.payload)))
        if entity.kind == "msat":
            df = df.withColumn(
                naming.COL["mas_key"], F.concat_ws("|", *[F.col(c) for c in entity.mas_key])
            )

    return df


# --------------------------------------------------------------------------- #
# PROJECTION -- the declared model is the table's shape
#
# DEF-26 (CRITICAL). _stage used to return the WHOLE staged Bronze frame and every flow
# appended it verbatim, so a hub declaring four business keys shipped 92 columns and an
# NHL declaring fourteen payload columns shipped 83. README.md's "what validation refuses
# to build" lists "a hub carrying descriptive attributes" -- enforced on the DECLARATION
# (an entity cannot declare a payload on a hub) and broken in the IMPLEMENTATION.
#
# IT DEFEATED THE MASK CONTROL, which is why this is Critical rather than untidy.
# _emit_target applies MASK clauses only to entities that DECLARE masks, and a hub
# declares none -- a hub is meant to hold keys. So the same column, carrying the same
# values, was masked on one table and clear on another:
#
#     nhl_general_journal_line   2,453,132 rows            0 readable debitamt
#     hub_accounting_journal     2,759,294 rows    2,759,292 readable debitamt
#
# _emit_quarantine had the same gap from the other direction: it passed no schema=, so no
# MASK clause, while being fed the identical frame.
#
# THE RULE, PER KIND. Nothing outside this list reaches a vault table:
#
#   hub          hash key, one column per DECLARED business key, the readable _bk,
#                system columns.
#   link / hal   hash key, each parent's hash key, the transaction key (empty for a
#                plain link), system columns.
#   nhl          the same, plus the declared payload. An NHL legitimately carries
#                transaction detail -- 14 declared payload columns is right, 72 source
#                columns is not.
#   sat/msat/    parent hash key, hashdiff, the declared payload, system columns, plus
#   esat/csat    the generated mas_key for a multi-active satellite.
#   qtn_ twin    exactly what its target holds, plus failure_rule and failure_detail.
#                It must never be wider than the table it shadows.
#
# A BINDING'S payload MAY DIFFER FROM THE ENTITY'S and the binding wins where it declares
# one -- the same `src.payload or entity.payload` rule hashdiff_expr already uses, so the
# columns that are hashed and the columns that are stored cannot drift apart. gl20000's
# binding names `openyear` where gl30000's names `hstyear`; they are separate tables.
#
# A HUB'S BUSINESS KEYS ARE RENAMED AND CAST TO STRING, and both halves are required.
# `business_keys` are the MODEL's names (reference_type, fiscal_year); the frame carries
# the SOURCE's (input_db, openyear, jrnentry) -- or nothing at all where the binding
# supplies a key_literal. Six bindings feed hub_organisation under six different column
# names for one business key position, so a hub keyed on source names could not have one
# shape at all. STRING because the positions must agree across bindings: fiscal_year is
# `openyear` from GP (INT) and the literal 'NOT_APPLICABLE' from UKG. CAST-to-STRING is
# also exactly what the hash rulebook does to a key component before hashing it
# (hashing.normalise), so the stored key and the hashed key are the same value.
# --------------------------------------------------------------------------- #
_AS_IS, _BK_COLUMN, _BK_LITERAL = "as_is", "bk_column", "bk_literal"


def _projection(entity: Entity, src: SourceBinding) -> list[tuple[str, str, str]]:
    """The table's declared shape for this binding, as ordered (name, kind, source).

    Pure metadata: no Spark, no frame. That is deliberate -- the shape of a vault table
    is a MODELLING fact, so it is decided from the declaration alone and can be asserted
    in a test with no Spark installed, exactly as spec.hub_key_components is.

    Duplicates collapse to their FIRST position: an NHL's transaction_key is normally
    also in its payload (seqnumbr), and one column cannot appear twice in a select.
    """
    cols: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    def add(name: str, kind: str = _AS_IS, value: str | None = None) -> None:
        if name in seen:
            return
        seen.add(name)
        cols.append((name, kind, name if value is None else value))

    if entity.kind == "hub":
        add(entity.hk_column)
        parts = hub_key_components(entity, src)
        for business_key, (is_literal, value) in zip(entity.business_keys, parts):
            add(business_key, _BK_LITERAL if is_literal else _BK_COLUMN, value)
        add(naming.bk(entity.name))

    elif entity.kind in naming.LINK_KINDS:
        add(entity.hk_column)
        for parent, role in parent_legs(entity):
            add(naming.hk(parent, role))
        for column in entity.transaction_key:
            add(column)
        if entity.kind == "nhl":
            for column in (src.payload or entity.payload):
                add(column)

    else:  # satellites -- parent may be a hub or a link
        add(naming.hk(entity.parents[0]))
        add(naming.COL["hashdiff"])
        if entity.kind == "msat":
            add(naming.COL["mas_key"])
        for column in (src.payload or entity.payload):
            add(column)

    # A declared column that collides with a system column would WIN the dedup above and
    # the system column would silently vanish from the table -- load_dts, rec_src or
    # cdc_op quietly replaced by a payload value. Refuse it here, named.
    collisions = sorted(seen & set(naming.SYSTEM_COLUMNS))
    if collisions:
        raise SpecError(
            f"{entity.name}/{src.name}: declared column(s) {collisions} collide with the "
            f"system column set {list(naming.SYSTEM_COLUMNS)}. The generator has no "
            f"rename capability, so one of the two would be lost from the table -- and "
            f"it would be the system column, silently. Rename the source column in "
            f"Bronze, or drop it from the declaration."
        )

    # DEF-41: per KIND, not one set for everything. A hub/link/NHL has no versions to
    # order and no update to record, so it carries neither sub_seq nor cdc_op. The
    # collision guard above deliberately still tests the FULL set: _system_columns writes
    # all seven onto the staged frame whatever the kind, so a declared column named
    # `cdc_op` would be overwritten there even on a hub that never projects it.
    for column in naming.system_columns_for(entity.kind):
        add(column)
    return cols


def _project(df: DataFrame, entity: Entity, src: SourceBinding,
             extra: Sequence[tuple[str, "F.Column"]] = ()) -> DataFrame:
    """Narrow a staged frame to the entity's declared shape, plus any `extra` columns.

    ONE select(), not a chain of withColumn/drop calls: every expression is evaluated
    against the INPUT frame, so a business key whose model name collides with a different
    source column cannot read a value some earlier step already overwrote.

    DEF-39: `extra` exists for the quarantine twin's two rejection columns, and carries
    the same guarantee. Those expressions are built by _violation_expr() from the
    EXPECTATION SQL, so they may name any source column -- which means they can only be
    evaluated against the staged frame, never against the projection's output, where
    those names no longer exist. Appending them here evaluates them in the same select
    as the projection itself. Applying them afterwards instead is what broke the whole
    pipeline at graph analysis: `name 'input_db' cannot be resolved`, 24 flows dead.

    They are appended LAST, in the caller's order, because _emit_quarantine_table
    declares the twin's schema as the target's DDL with the two reason columns appended
    -- so column order here and column order there are the same fact.
    """
    columns = []
    declared = set()
    for name, kind, value in _projection(entity, src):
        if kind == _BK_LITERAL:
            column = F.lit(value).cast("string")
        elif kind == _BK_COLUMN:
            column = F.col(value).cast("string")
        else:
            column = F.col(value)
        declared.add(name)
        columns.append(column.alias(name))

    for name, column in extra:
        if name in declared:
            # Silently, the later alias would win and the declared column would be gone.
            raise SpecError(
                f"{entity.name}/{src.name}: extra projected column {name!r} collides "
                f"with a declared column of the same name. One of the two would be lost "
                f"from the frame."
            )
        columns.append(column.alias(name))
    return df.select(*columns)


def _stage(entity: Entity, src: SourceBinding, spark, model: Model,
           for_schema: bool = False) -> DataFrame:
    """The frame the vault actually receives: staged, then projected to the declaration.

    _derived_schema_ddl reads THIS function's .schema, so a declared schema is the
    declared model's shape by construction rather than by agreement with a second
    description. The flow bodies do not call it -- they stage, filter on the full row
    (expectations may name any source column) and project afterwards, which produces the
    identical frame with the rejects removed.
    """
    return _project(_stage_full(entity, src, spark, model, for_schema), entity, src)


# --------------------------------------------------------------------------- #
# Expectations from Unity Catalog
# --------------------------------------------------------------------------- #
def _expectations(spark, expectations_table: str, dataset: str) -> dict[str, str]:
    """Expectations are governed configuration, held in UC, not hard-coded here."""
    try:
        rows = (
            spark.table(expectations_table)
            .where(F.col("dataset") == dataset)
            .where(F.col("is_current"))
            .select("rule_name", "rule_sql")
            .collect()
        )
    except Exception:  # table absent in a fresh workspace: fail closed on keys only
        return {}
    return {r["rule_name"]: r["rule_sql"] for r in rows}


def _mandatory_rules(entity: Entity, src: SourceBinding) -> dict[str, str]:
    """Rules that hold regardless of configuration.

    Includes the key-component safety rules from the hash rulebook: a NULL business
    key is a defect rather than a value to hash, and a key component containing the
    delimiter would silently collide with a different key. Both quarantine with a
    named reason rather than producing a wrong identity.
    """
    if entity.kind == "hub":
        rules = {"hub_key_present": f"{entity.hk_column} IS NOT NULL"}
        rules.update(key_safety_rules(src.key_columns, mandatory=True))
        return rules

    if entity.kind in naming.LINK_KINDS:
        rules = {"link_key_present": f"{entity.hk_column} IS NOT NULL"}
        # THE RULE NAME CARRIES THE ROLE TOO. Two legs over one hub would otherwise
        # produce one rule name for two columns, and a dict keeps the last -- so the
        # parent leg would go unchecked while the report still listed a rule for it.
        for p, role in parent_legs(entity):
            rules[f"parent_{role + '_' if role else ''}{p}_present"] = \
                f"{naming.hk(p, role)} IS NOT NULL"
        # the parents' source key columns, plus (for an nhl) the transaction key and
        # any dependent child key -- all of which enter the hash
        rules.update(key_safety_rules(src.key_columns, mandatory=True))
        if entity.kind == "nhl":
            rules.update(key_safety_rules(entity.transaction_key, mandatory=True))
        return rules

    parent = entity.parents[0]
    rules = {
        "parent_key_present": f"{naming.hk(parent)} IS NOT NULL",
        "hashdiff_present": f"{naming.COL['hashdiff']} IS NOT NULL",
    }
    rules.update(key_safety_rules(src.key_columns, mandatory=True))
    if entity.kind == "msat":
        # the multi-active sub-key is part of the row's uniqueness, so it is a key
        rules.update(key_safety_rules(entity.mas_key, mandatory=True))
    return rules


# --------------------------------------------------------------------------- #
# Emitters
# --------------------------------------------------------------------------- #
def _mask_clauses(entity: Entity, bindings: Sequence[SourceBinding]) -> str:
    """Column MASK clauses for the table definition, as `<col> <TYPE> MASK <fn>`.

    THE TYPE IS REQUIRED AND IS NOT INFERRED. A column entry in a table definition needs
    a data type: `debitamt MASK governance.mask_money` is not valid DDL, and the parser
    reports it obscurely, as a syntax error at the mask function's name (DEF-16). It is
    taken from the `cast:` declaration on the binding that loads this table.

    THIS IS NOT A GENERAL TYPE SYSTEM, AND MUST NOT GROW INTO ONE. The generator does not
    know payload column types in general -- they are inferred from Bronze at runtime, and
    for an inactive binding that Bronze table is not even in this lake. That gap is real
    and is spec section 2b's open decision; nothing here closes it. It is simply not
    reached: a type is needed only for a MASKED column, only on an ACTIVE binding, and
    there it is already declared, because a masked money column is cast to
    DECIMAL(18,2) to match `governance.mask_money(v DECIMAL(18,2))`. A mask function's
    parameter type must agree with the column it masks, so the cast is not merely
    available -- it is the only value that can be correct.

    An active masked column with no declared cast RAISES, deliberately. That is someone
    adding a mask without a cast, and it must fail loudly at build time rather than emit
    invalid DDL or, far worse, let SDP infer a type and write it permanently into an
    insert-only vault.

    WHY HERE AND NOT IN A POST-DEPLOY ALTER TABLE:
    row filters and column masks on streaming tables and materialized views must be
    added, updated or dropped through the table definition / CREATE OR REFRESH. An
    ALTER TABLE ... SET MASK against a pipeline-owned table is the wrong mechanism:
    the next pipeline update owns the table definition and the policy does not survive.

    THE REFRESH-AS-OWNER TRAP -- read before changing a mask function:
    when a pipeline refreshes a streaming table or materialized view, mask functions
    run with the PIPELINE OWNER's rights, and user-context functions such as
    CURRENT_USER and IS_ACCOUNT_GROUP_MEMBER evaluate against the pipeline's run-as
    identity -- not the querying user's. If the run-as identity is not privileged under
    a mask, a downstream materialized view MATERIALISES THE MASKED VALUE: NULLs are
    written into the vault as fact, permanently, and insert-only means they stay.

    So the pipeline run-as identity MUST be privileged under every mask applied to a
    table it reads -- and since 28 Sep 2026 that is no longer ONE group. The money masks
    (mask_money, mask_money_double) admit usnc_data_analyst_finance and
    scope_unmask_currency_values; the three PII masks admit pii_cleared_us and
    global_dataplatform_pipeline_job_runners. An identity privileged under one is not
    thereby privileged under the other, so "privileged under every mask" now means
    membership of one group PER MASK the tables it reads actually declare.

    checks/mask_survival_check.py asserts that the declared masks survive onto the tables,
    NOT that the run-as identity can read them -- it says so in its own closing notes, and
    that half stays manual. DEPLOY.md 6b makes it a named prerequisite and lists the four
    targets that declare no run_as at all, where the identity is whoever pressed Deploy.
    """
    declared: dict[str, set[str]] = {}
    for binding in bindings:
        for col, sql_type in binding.cast:
            declared.setdefault(col, set()).add(sql_type)

    clauses, missing, ambiguous = [], [], []
    for col, fn in entity.masks:
        types = declared.get(col) or set()
        if not types:
            missing.append(col)
        elif len(types) > 1:
            ambiguous.append(f"{col} -> {sorted(types)}")
        else:
            clauses.append(f"{col} {types.pop()} MASK {_qualified_mask_fn(fn)}")

    if missing or ambiguous:
        detail = []
        if missing:
            detail.append(
                f"no cast: declared for masked column(s) {sorted(missing)}. A masked "
                f"column on an active binding MUST declare its type, because the MASK "
                f"clause is emitted into the table definition. Add it to the binding's "
                f"cast: block -- for governance.mask_money that is DECIMAL(18,2), to "
                f"match the function's parameter type."
            )
        if ambiguous:
            detail.append(
                f"active bindings disagree on the cast type for {ambiguous}. One table "
                f"cannot have two types for one column; reconcile the cast: blocks."
            )
        raise SpecError(f"{entity.name}: " + " ".join(detail))

    return ", ".join(clauses)


def _quote_ident(name: str) -> str:
    """Backtick-quote a column name. GP delivers columns called `timestamp`."""
    return "`" + name.replace("`", "``") + "`"


_RESOLVED_CATALOG: str = ""


def _qualified_mask_fn(fn: str) -> str:
    """Catalog-qualify a mask function name.

    DEF-21: metadata declares `governance.mask_money` -- schema-qualified, because the
    catalog differs per lake and must not be baked into the model. But inside a pipeline a
    two-part name resolves against `spark_catalog`, not the pipeline's catalog:

        [SCHEMA_NOT_FOUND] The schema `spark_catalog`.`governance` cannot be found

    The catalog is taken from `hfig.catalog`, which the bundle sets from ${var.catalog}.
    A name that is already three-part is left alone. The catalog is backtick-quoted
    because these are named `02_usnc_silver_edm_dev` -- a leading digit (DEF-1).
    """
    if fn.count(".") >= 2 or not _RESOLVED_CATALOG:
        return fn
    return f"{_quote_ident(_RESOLVED_CATALOG)}.{fn}"


def _derived_schema_ddl(entity: Entity, bindings: Sequence[SourceBinding],
                        spark, model: Model) -> str:
    """The table's FULL schema, read from the staged DataFrame, with MASK clauses added.

    DEF-19: `create_streaming_table(schema=...)` is NOT an overlay. Whatever is passed IS
    the table's schema and must match what the flows produce, so declaring a mask means
    declaring every column -- 26 of them for a GP journal line since DEF-26 projected the
    frame to the declared model; 83 before it, which was the defect.

    THE SCHEMA IS READ, NOT RECONSTRUCTED, and that is the whole point. `_stage()` returns
    the exact DataFrame the flow will append, so its `.schema` carries the real names,
    types, ORDER and NULLABILITY (`load_dts` is nullable=False) by construction rather
    than by agreement with a second description that could drift. It is lazy: `.schema`
    resolves at definition time without executing the query or scanning a row.

    This is safe only because masks are emitted for ACTIVE bindings alone, and an active
    binding is by definition readable in this lake -- so the source whose schema is needed
    is always present. No type is invented; the cast in `_stage` has already been applied
    (it runs before `_system_columns`), so `debitamt` arrives here as DECIMAL(18,2),
    matching `governance.mask_money(v DECIMAL(18,2))`.

    Multi-binding tables are UNIONED by column name. If two active bindings disagree on a
    column's type or nullability this RAISES: a silent widening would write a type nobody
    chose into a table nobody can alter. No table needs this today -- every masked table
    has exactly one active binding, and the multi-binding hubs carry no masks -- but the
    rule has to be right before it is first needed, not after.
    """
    masks = dict(entity.masks)
    fields = _derived_schema_fields(entity, bindings, spark, model)

    absent = [c for c in masks if c not in {n for n, _t, _null in fields}]
    if absent:
        raise SpecError(
            f"{entity.name}: masked column(s) {sorted(absent)} are not produced by any "
            f"active binding, so the MASK clause would name a column that does not "
            f"exist. Columns available: {sorted(n for n, _t, _null in fields)[:12]}..."
        )

    parts = []
    for name, sql_type, nullable in fields:
        piece = f"{_quote_ident(name)} {sql_type}"
        if not nullable:
            piece += " NOT NULL"
        if name in masks:
            piece += f" MASK {_qualified_mask_fn(masks[name])}"
        parts.append(piece)
    return ", ".join(parts)


def _derived_schema_fields(entity: Entity, bindings: Sequence[SourceBinding],
                           spark, model: Model) -> list[tuple[str, str, bool]]:
    """The ordered (name, sql_type, nullable) the flows produce, unioned by name.

    DEF-26: `_stage` now PROJECTS, so what comes back is the declared model's shape --
    hash key, keys, declared payload, system columns -- and the declared schema is that
    shape. The ordering is the projection's (spec.hub_key_components order for a hub,
    parents-then-payload for a link/NHL, system columns last), and a masked column still
    arrives with the type its `cast:` block declares because the cast runs inside
    `_stage_full` before `_system_columns` and the projection preserves it.

    Split out of _derived_schema_ddl so the GHOST flow can be built from the same field
    list. DEF-25's lesson -- declaring a schema converts every previously-cosmetic
    inconsistency between flows into a hard failure -- applies to the projection too:
    the ghost has to supply what the declaration promises, and the only way it cannot
    drift is to read the same list.
    """
    fields: dict[str, tuple[str, bool]] = {}
    order: list[str] = []
    origin: dict[str, str] = {}

    for binding in bindings:
        # BATCH, because this wants a schema and not rows -- see _stage_full's for_schema.
        for field in _stage(entity, binding, spark, model,
                            for_schema=True).schema.fields:
            sql_type = field.dataType.simpleString()
            if field.name in fields:
                if fields[field.name] != (sql_type, field.nullable):
                    prev_type, prev_null = fields[field.name]
                    raise SpecError(
                        f"{entity.name}: active bindings disagree on column "
                        f"{field.name!r} -- {origin[field.name]} produces "
                        f"{prev_type} (nullable={prev_null}) and {binding.name} produces "
                        f"{sql_type} (nullable={field.nullable}). Reconcile the cast: "
                        f"blocks; this is not widened automatically, because the result "
                        f"is written permanently into an insert-only table."
                    )
            else:
                fields[field.name] = (sql_type, field.nullable)
                origin[field.name] = binding.name
                order.append(field.name)

    return [(name, fields[name][0], fields[name][1]) for name in order]


def _table_comment(entity: Entity) -> str:
    """The comment Unity Catalog shows for this table.

    entity.notes is APPENDED, not left in the YAML. A modelling consequence that only
    exists as a comment in metadata/entities/*.yml is invisible to the person who meets
    the table in the catalog and writes a query against it -- which is precisely the
    audience the general-journal union-and-deduplicate warning is for.
    """
    comment = f"{entity.kind} :: {entity.domain} :: generated -- do not hand-edit"
    if entity.notes.strip():
        comment = f"{comment} || {' '.join(entity.notes.split())}"
    return comment


def _emit_target(entity: Entity, table: str,
                 active_bindings: Sequence[SourceBinding] = (),
                 spark=None, model: Model | None = None) -> None:
    from pyspark import pipelines as dp  # noqa: PLC0415 -- see the module note
    kwargs = dict(
        name=table,
        table_properties=_properties(entity, "silver"),
        comment=_table_comment(entity),
    )
    # Still conditional, and that is not vestigial: _cluster_by returns [] for any entity
    # whose load_dts falls back outside the statistics window, and an empty list must be
    # OMITTED rather than passed empty (DEF-23/DEF-24). Every entity clusters today.
    _clustering = _cluster_by(entity)
    if _clustering:
        kwargs["cluster_by"] = _clustering
    if entity.masks and active_bindings:
        # Do NOT fall back to ALTER TABLE, which does not survive an update, and do NOT
        # ship the table unmasked -- under parent decision D3 the column mask is the
        # vault's only PII defence, so there is no acceptable window without one.
        #
        # TWO INDEPENDENT CHECKS ON THE SAME PROPERTY, deliberately. _mask_clauses
        # asserts every masked column declares a cast: type (DEF-16); the derived schema
        # then reads the type the flow actually produces (DEF-19). The first is what
        # catches a masked column the second would silently carry through with whatever
        # Bronze happened to supply.
        _mask_clauses(entity, active_bindings)
        kwargs["schema"] = _derived_schema_ddl(entity, active_bindings, spark, model)
    dp.create_streaming_table(**kwargs)


def _quarantine_table(table: str) -> str:
    """The twin's name, derived from the LOGICAL table, not the pipeline's object.

    DEF-42: a staged hub's flows write to `stg_hub_accounting_journal`, and naming the
    twin off that string would rename it to `qtn_hub_accounting_journal` -- orphaning the
    existing table and silently starting a new one. The staging prefix is stripped first,
    so the twin keeps the name it has always had.
    """
    return f"{naming.PREFIX['quarantine']}{naming.unstg(table).split('_', 1)[1]}"


def _emit_quarantine(entity: Entity, table: str,
                     active_bindings: Sequence[SourceBinding] = (),
                     spark=None, model: Model | None = None) -> None:
    """The twin that holds what the target rejected -- SAME SHAPE, SAME MASKS, plus why.

    DEF-26: this used to pass no `schema=`, so a quarantine table carried no MASK clause
    at all while being fed the identical frame as its target. A rejected row is not a
    less sensitive row -- it is the same source row, kept -- so the same control applies.
    The columns come from the target's derived schema, which after the projection is the
    declared model's shape, with the two rejection columns appended: the twin cannot be
    wider than the table it shadows.
    """
    from pyspark import pipelines as dp  # noqa: PLC0415 -- see the module note
    kwargs = dict(
        name=_quarantine_table(table),
        table_properties=_properties(entity, "quarantine"),
        comment="rejected rows retained verbatim with reason -- loop-1 reconciliation",
    )
    if entity.masks and active_bindings:
        _mask_clauses(entity, active_bindings)
        target = _derived_schema_ddl(entity, active_bindings, spark, model)
        reasons = ", ".join(
            f"{_quote_ident(c)} string"
            for c in (naming.COL["failure_rule"], naming.COL["failure_detail"])
        )
        kwargs["schema"] = f"{target}, {reasons}"
    dp.create_streaming_table(**kwargs)


def _flow_name(table: str, src: SourceBinding, suffix: str = "") -> str:
    """The flow's name, which IS the identity of its checkpoint.

    SDP keys a flow's checkpoint by this string. A name it has not seen before gets a
    fresh, empty checkpoint and re-reads its source from the start; an unchanged name
    resumes exactly where it left off. That is the whole mechanism behind
    `stream_generation` -- see SourceBinding.stream_generation for why replacing the
    checkpoint is the available move and a full refresh is not.

    GENERATION 1 EMITS NOTHING. Every flow in the estate is generation 1 today, so every
    one of them keeps the name it already has and the checkpoint behind it. A suffix that
    appeared at generation 1 would rename all of them at once and silently re-read every
    source in the lake.

    The generation sits before `suffix`, so the quarantine twin of a bumped binding is
    bumped with it -- `f_stg_x_fieldglass_us_g2` and `f_stg_x_fieldglass_us_g2_qtn`. Both
    flows read the same recreated source, so both checkpoints are equally invalid, and
    resetting one without the other would leave loop-1 reconciling a full re-read against
    a resumed quarantine stream.
    """
    gen = f"_g{src.stream_generation}" if src.stream_generation > 1 else ""
    return f"f_{table}_{src.name.lower()}{gen}{suffix}"


def _violation_expr(rules: dict[str, str]):
    """The ONE definition of "this row violates an expectation", used by both flows.

    Returns (failed, reason). A row violates if the rule's SQL is FALSE **or NULL** --
    an expectation that cannot be evaluated is not a pass. `failed` is therefore never
    itself null, so `~failed` is a safe predicate for the valid flow.

    Both flows read this, so the rows that land and the rows that are quarantined are
    exact complements. loop-1 reconciliation asserts landed + (quarantined -
    superseded) = approved; two separately-maintained predicates would be a way for
    that to go quietly wrong.
    """
    failed = F.lit(False)
    reason = F.lit(None).cast("string")
    for name, sql in rules.items():
        violates = ~F.expr(sql) | F.expr(sql).isNull()
        failed = failed | violates
        reason = F.when(violates & reason.isNull(), F.lit(name)).otherwise(reason)
    return failed, reason


def _register_source_flows(
    entity: Entity, src: SourceBinding, table: str, spark, expectations_table: str,
    model: Model,
) -> None:
    from pyspark import pipelines as dp  # noqa: PLC0415 -- see the module note
    rules = dict(_mandatory_rules(entity, src))
    rules.update(_expectations(spark, expectations_table, table))

    changed_only = entity.kind in ("sat", "msat", "csat") and entity.change_detection == "cdc"

    # DEF-18: `@dp.expect_all_or_drop` CANNOT be stacked under `@dp.append_flow`.
    # DEPLOY.md Phase 3 assumption 3, refuted -- the inner decorator returns a
    # DatasetOrExpectationDecoratorResult and the outer one then fails with
    #   AttributeError: 'DatasetOrExpectationDecoratorResult' has no attribute '__globals__'
    # taking every flow in the pipeline down at graph analysis.
    #
    # The fallback that entry names is taken: the expectations become a FILTER in the
    # query, and the quarantine flow stays the record of rejects. _violation_expr() is
    # shared by both flows and is the ONE definition of "violates", so the kept rows and
    # the quarantined rows are complements BY CONSTRUCTION. That matters beyond tidiness:
    # loop-1 asserts landed + (quarantined - superseded) = approved, and two
    # independently-written predicates could silently make that arithmetic wrong.
    #
    # COST, recorded rather than hidden: expectation metrics no longer appear in the
    # pipeline event log, because no expectation is declared to the runtime. The
    # quarantine table is now the only record of what was rejected and why.
    @dp.append_flow(target=table, name=_flow_name(table, src))
    def _valid(entity=entity, src=src, changed_only=changed_only, model=model,
               rules=rules):
        # DEF-26: stage the FULL row, filter, then project. The order matters in both
        # directions -- an expectation is governed configuration and may name any source
        # column, so it must be evaluated before the narrowing; and only the declared
        # shape may land, so the narrowing must happen before the append.
        df = _stage_full(entity, src, spark, model)
        failed, _reason = _violation_expr(rules)
        df = df.where(~failed)
        if changed_only:
            # Bronze already decided what changed. Deletes are recorded by the
            # status-tracking satellite, not by mutating this one.
            df = df.where(F.col(naming.COL["cdc_op"]).isin("I", "U"))
        if entity.kind == "nhl":
            # NHLs have no hashdiff, so within-batch re-run safety comes from a dedup
            # guard on the transaction key.
            #
            # THIS OPERATOR IS NOT THE CROSS-BATCH BACKSTOP, AND THE COMMENT HERE USED TO
            # SAY IT WAS -- "cross-batch duplicates are caught by the uniqueness check in
            # checks/append_only_check.py". That was false twice over by 29 September.
            # An NHL's pipeline target is stg_nhl_... now, and append_only_check SKIPS the
            # uniqueness assertion for stg_ on purpose: holding several rows per key is
            # what a log is for. And the operator itself is the thing that failed -- it is
            # stateful with no watermark, its memory lives in the CHECKPOINT, and a
            # checkpoint reset on 28 September re-read the whole feed against an empty
            # memory and duplicated 1,164 rows into nhl_invoice_line_rev2.
            #
            # THE BACKSTOP IS THE ANTI-JOIN in checks/load_hubs.py, which asks the target
            # which keys it already holds. That is strictly better: it is state in a
            # table rather than state in a file, so it survives a checkpoint reset, a
            # generation bump and a rebuild.
            #
            # KEPT ANYWAY, because the log is what loop-1 and supersede_quarantine count
            # as "landed". Dropping it would let one batch write the same key twice into
            # stg_nhl_..., which the anti-join would still collapse to one vault row --
            # so the vault stays correct while the accepted count the log reports goes up
            # by the duplicates, and loop-1's `landed + (quarantined - superseded) =
            # approved` fails on a delivery that was handled correctly. It is a
            # within-batch tidy-up whose failure mode is now merely noisy, not corrupting.
            df = df.dropDuplicates([entity.hk_column])
        return _project(df, entity, src)

    # the OTHER flow: the same rows that failed, kept with their reason
    @dp.append_flow(
        target=_quarantine_table(table),
        name=_flow_name(table, src, "_qtn"),
    )
    def _invalid(entity=entity, src=src, rules=rules, model=model):
        df = _stage_full(entity, src, spark, model)
        failed, reason = _violation_expr(rules)
        # The twin is its target's shape plus exactly two columns -- never the whole
        # source row (DEF-26). The two reason columns are handed to _project as `extra`
        # rather than added with withColumn afterwards, because `reason` is built from
        # the expectation SQL and may name any SOURCE column: after the projection those
        # names are gone, and the flow cannot be analysed at all (DEF-39).
        return _project(
            df.where(failed), entity, src,
            extra=(
                (naming.COL["failure_rule"], reason),
                (naming.COL["failure_detail"], F.lit("expectation violated")),
            ),
        )


_GHOST_SYSTEM_SQL = {
    naming.COL["load_dts"]: "TIMESTAMP '1900-01-01 00:00:00'",
    naming.COL["applied_dts"]: "TIMESTAMP '1900-01-01 00:00:00'",
    naming.COL["sub_seq"]: "CAST(0 AS INT)",
    naming.COL["rec_src"]: "'SYSTEM'",
    naming.COL["batch_id"]: "'GHOST'",
    naming.COL["manifest_id"]: "CAST(NULL AS STRING)",
    # DEF-25: cdc_op was the one system column the ghost row did not supply.
    naming.COL["cdc_op"]: "'I'",
}


def _ghost_column_sql(entity: Entity, name: str, sql_type: str) -> str:
    """The ghost row's value for one DECLARED column.

    A hash key gets the zero key -- INCLUDING a link's parent hash keys, which is what
    makes a PIT join to the parent hub an equi-join rather than an outer one. A system
    column gets its literal. Everything else -- a business key, a payload column, the
    hashdiff -- gets a NULL of the column's own declared type: a ghost row asserts an
    identity that is deliberately absent, not a value.
    """
    if name == entity.hk_column or name.endswith("_hk"):
        return zero_key_sql()
    if name in _GHOST_SYSTEM_SQL:
        return _GHOST_SYSTEM_SQL[name]
    return f"CAST(NULL AS {sql_type})"


def _ghost_system_exprs(entity: Entity) -> list[str]:
    """The ghost's system-column expressions for a table with NO declared schema.

    DEF-41: pure, so the property that matters can be asserted without Spark -- the
    ghost supplies exactly the system columns this KIND declares, no more. The list this
    replaced was hand-written and was a second definition of the set: when hubs and NHLs
    stopped carrying sub_seq and cdc_op it kept supplying them, and because SDP infers an
    undeclared table from the union of its flows, both reappeared at the end of every
    unmasked table. The masked ones, which declare a schema, narrowed correctly -- so the
    defect was invisible in exactly the tables no schema assertion covered.
    """
    return [
        f"{_GHOST_SYSTEM_SQL[c]} AS {c}"
        for c in naming.system_columns_for(entity.kind)
    ]


def _ghost_columns_sql(entity: Entity,
                       schema_fields: Sequence[tuple[str, str, bool]]) -> list[str]:
    """The ghost row's full select list for a DECLARED schema, in declared order.

    Pure: no Spark, so the property that matters -- the ghost supplies EVERY declared
    column -- is asserted against the declared field list itself rather than inferred
    from a flow nobody can run offline.
    """
    unsuppliable = [
        name for name, _t, nullable in schema_fields
        if not nullable and name not in _GHOST_SYSTEM_SQL
        and name != entity.hk_column and not name.endswith("_hk")
    ]
    if unsuppliable:
        raise SpecError(
            f"{entity.name}: the declared schema makes {sorted(unsuppliable)} NOT "
            f"NULL, but a ghost row has no value for a business key, a payload column "
            f"or a hashdiff -- it asserts an absent identity. The append would fail "
            f"with DELTA_MISSING_NOT_NULL_COLUMN_VALUE on first load. Either the "
            f"column must not be NOT NULL, or it must not be declared."
        )
    return [
        f"{_ghost_column_sql(entity, name, sql_type)} AS {_quote_ident(name)}"
        for name, sql_type, _nullable in schema_fields
    ]


def _emit_ghost(entity: Entity, table: str, spark,
                schema_fields: Sequence[tuple[str, str, bool]] | None = None) -> None:
    """One zero-key row per structure, appended once, so PIT joins stay equi-joins.

    DEF-26: when the table DECLARES a schema, the ghost is built from that same field
    list rather than from a hand-kept subset. DEF-25 is the reason: declaring a schema
    converts every previously-cosmetic inconsistency between flows into a hard failure,
    and the projection makes every declared table's shape explicit, so the ghost and the
    source flow have to agree by construction. A declared NOT NULL column the ghost
    cannot supply is refused HERE, at definition time, with the column named -- not at
    the first append with DELTA_MISSING_NOT_NULL_COLUMN_VALUE.

    With no declared schema (no masks, or no active binding at all) there is nothing to
    agree with: SDP infers the table from its flows, and the ghost stays the hash key
    plus the system columns.
    """
    from pyspark import pipelines as dp  # noqa: PLC0415 -- see the module note
    if schema_fields is not None:
        # Built HERE, not inside the flow body, so a column the ghost cannot supply
        # fails at definition time with the column named rather than on first append.
        exprs = _ghost_columns_sql(entity, schema_fields)

        @dp.append_flow(target=table, name=f"f_{table}_ghost", once=True)
        def _ghost_declared(exprs=exprs):
            return spark.range(1).select(*[F.expr(e) for e in exprs])

        return

    @dp.append_flow(target=table, name=f"f_{table}_ghost", once=True)
    def _ghost(entity=entity):
        cols = [F.expr(f"{zero_key_sql()} AS {entity.hk_column}")]
        if entity.kind in naming.SATELLITE_KINDS:
            cols = [F.expr(f"{zero_key_sql()} AS {naming.hk(entity.parents[0])}")]
        # DEF-41: driven by _GHOST_SYSTEM_SQL and naming.system_columns_for(kind), NOT
        # by a hand-written list. The list that used to be here was a SECOND definition
        # of the system column set, and when hubs and NHLs stopped carrying sub_seq and
        # cdc_op it went on supplying them: SDP infers an undeclared table from the union
        # of its flows, so the ghost silently added both back at the end of every
        # unmasked table. Measured after the 25 Aug reload -- hub_accounting_journal came
        # back as 11 correct columns plus `sub_seq` and `cdc_op` appended at positions 12
        # and 13, while the MASKED NHLs, which declare their schema, narrowed correctly.
        #
        # DEF-25 is preserved by construction rather than by repetition: cdc_op is in
        # _GHOST_SYSTEM_SQL, so every kind whose declared shape includes it still gets
        # it, and no kind whose shape excludes it can be given it back.
        cols += [F.expr(e) for e in _ghost_system_exprs(entity)]
        return spark.range(1).select(*cols)


def _emit_v1_view(entity: Entity, table: str, spark,
                  active_bindings: Sequence[SourceBinding] = ()) -> None:
    """Derived type-2. End-dating is computed here and stored nowhere.

    UNREACHABLE TODAY, and deliberately so. DEF-52 made every satellite kind staged, and
    build() no longer calls this for a staged kind: `table` would be the staging LOG, and
    a type-2 view of the log makes every re-delivery a version -- the defect
    checks/load_satellites.py exists to remove. The view cannot be pointed at the vault
    table from here either, because this pipeline runs before the loader that creates it.

    This body is kept, not deleted, because it is the SQL the loader-side view needs:
    checks/load_satellites.py must create sat_x_v1 after it builds sat_x, with these same
    windows and with the mask clauses `_mask_clauses` supplies below, BEFORE any satellite
    is activated. See the comment at the call site in build().
    """
    from pyspark import pipelines as dp  # noqa: PLC0415 -- see the module note
    parent_hk = naming.hk(entity.parents[0])
    partition = [parent_hk] + ([naming.COL["mas_key"]] if entity.kind == "msat" else [])
    part_sql = ", ".join(partition)
    # DEF-56: end-date on applied_dts, the source's own statement of when the row
    # changed. load_dts is one timestamp per batch and collapses every intra-batch
    # interval to zero length. The ORDER moves with it, because applied_dts is not
    # monotonic with load_dts -- a backfill would otherwise emit valid_to <
    # valid_from. Kept identical to checks/load_satellites.py:v1_sql, which is the
    # implementation that actually runs today; a check asserts the two agree.
    order_sql = (f"{naming.COL['applied_dts']}, {naming.COL['load_dts']}, "
                 f"{naming.COL['sub_seq']}")

    mv_kwargs = dict(
        name=naming.v1_view(table),
        comment="derived type-2 view -- valid_to and is_current are computed, never stored",
    )
    if entity.masks:
        # A mask on the base satellite does not automatically protect this projection.
        # Declare it here too, and verify empirically with an unprivileged principal
        # (DEPLOY.md Phase 6a) rather than assuming inheritance.
        #
        # NOT YET DEF-19-CORRECT, and unreachable today. This still passes only the mask
        # clauses, which `create_streaming_table` rejects as a partial schema. Every
        # satellite is deferred (spec 3.1) so no _v1 is emitted in this lake and the path
        # cannot run; it is left rather than rewritten blind, because an MV's schema is
        # its projection's, not `_stage`'s, and deriving it needs the real thing to test
        # against. WHOEVER RE-ACTIVATES SATELLITES MEETS THIS FIRST -- see DEF-19.
        mv_kwargs["schema"] = _mask_clauses(entity, active_bindings)

    @dp.materialized_view(**mv_kwargs)
    def _v1(entity=entity):
        return spark.sql(
            f"""
            SELECT *,
                   {naming.COL['applied_dts']} AS valid_from,
                   LEAD({naming.COL['applied_dts']}) OVER (
                       PARTITION BY {part_sql} ORDER BY {order_sql}
                   ) AS valid_to,
                   LEAD({naming.COL['applied_dts']}) OVER (
                       PARTITION BY {part_sql} ORDER BY {order_sql}
                   ) IS NULL AS is_current
            FROM {table}
            """
        )


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def build(
    model: Model, spark, expectations_table: str,
    active_sources: frozenset[str] | None = None,
    only: frozenset[str] | set[str] | None = None,
) -> list[str]:
    """Declare every object in the model. Returns the object names for logging.

    `only` NAMES WHAT TO BUILD; THE MODEL STAYS WHOLE FOR LOOKUPS, and the distinction is
    not a nicety. A hash key is composed from its PARENT HUB's declaration --
    spec.parent_key_components does `model.get(parent)` -- so a caller that wants a subset
    must not hand over a subset. Pruning `model.entities` first raises

        SpecError: unknown parent entity 'organisation'

    for any link or NHL whose parent hub was pruned away. MEASURED 25 September, on the
    first run of the domain-split topology: raw_vault_finance and raw_vault_pay_bill both
    died at definition time, because nhl_general_journal_line's parent `organisation` lives
    in the party domain and nhl_invoice_line's `supplier` and `worker` do too. Thirteen
    cross-domain parent references exist in the model today.

    THE LAYER FILTER HAD THE SAME LATENT BUG and nobody had met it: a business-vault csat's
    parent is a raw-layer NHL, so pruning the raw entities away before building the business
    layer would fail identically the moment such a csat became active. One `only` set, both
    filters, one place where the difference between "what this pipeline declares" and "what
    the model knows" is stated.

    Everything here runs at pipeline-definition time. The number of flows scales
    with the metadata, not with the amount of code: adding a source to a hub is one
    entry in one YAML file.

    THE CREATE / FLOW SPLIT (parent decision D5, spec.resolve_active_sources).
    `create_streaming_table()` is emitted for EVERY declared binding in every lake, so
    the table inventory is identical everywhere and cross-region conformance compares
    like with like. `append_flow()` is emitted only for bindings ACTIVE in this lake, so
    a binding whose Bronze table is not present here cannot fail the pipeline at
    definition time and take every other flow down with it. An inactive binding yields
    an EMPTY table, not a missing one.

    `active_sources=None` means every binding is active -- the behaviour before D5 was
    implemented, so a target that declares nothing is unaffected.

    NOTE the ghost flow is emitted regardless. It reads spark.range(1), never Bronze, so
    it cannot fail for an absent source -- and it means no generated vault table is left
    with zero flows attached.

    THREE THINGS ARE SUPPRESSED FOR A TABLE WITH NO ACTIVE BINDING, all on the same
    active-set test (spec.active_table_bindings) so there is one notion of "inactive":

      * the QUARANTINE TWIN -- see below;
      * the MASK CLAUSES on the table definition. _emit_target passes no schema, so SDP
        infers the columns from the flows. A table with only the ghost flow has the hash
        key and the six system columns and NO PAYLOAD, so a `col MASK fn` clause would
        name a column that does not exist -- very likely a definition-time error, failing
        the deploy outright. There is no data to mask and no column to mask it on;
      * the _v1 MATERIALIZED VIEW, which projects that same absent payload and would
        redeclare the same mask over it.

    THE QUARANTINE TWIN, in full. Quarantine tables are fed exclusively by
    _register_source_flows, so a table whose every binding is inactive would otherwise be
    declared with no flows at all -- the one place this split could hand SDP a flowless
    streaming table.

    Skipping it costs nothing that D5 protects:

      * conformance_check.py's VAULT_PREFIXES is ("hub_", "lnk_", "nhl_", "hal_", "sat_",
        "msat_", "esat_", "csat_"). `qtn_` is NOT in it, so quarantine tables are never
        compared across lakes and cannot cause conformance drift. The identical-inventory
        guarantee is about vault tables, and every vault table is still emitted everywhere.
      * loop1_reconciliation.py names the table it reconciles (--entity) and SKIPS a table
        with no active binding, announcing it as NOT EVALUATED. That skip is not
        cosmetic: this docstring previously claimed the gate "only ever runs for an
        entity actually being loaded", which was false -- resources/vault_job.yml passed
        no --entity at all, so the gate fell back to a default of nhl_timesheet_line,
        whose only binding is inactive here, and read a qtn_ table this function had just
        declined to create. The job now passes an explicit active table AND the gate
        skips inactive ones; either alone leaves the trap.

    The rejected alternative -- attaching a ghost-shaped flow to the quarantine table --
    is worse twice over: a quarantine table holds REJECTED rows, and a synthetic row that
    was never rejected lies about what the table means; and it would permanently inflate
    the quarantined side of loop1's `landed + (quarantined - superseded) = approved`
    arithmetic.

    THE CONDITION IS PER TABLE, WHICH IS WHAT MAKES IT PRECISE. A hub, link or NHL is one
    table fed by every binding, so it keeps its quarantine twin while ANY binding is
    active -- one active binding among three inactive ones can still reject rows into it.
    A satellite is one table PER source, so its quarantine twin follows that single
    binding: deactivating one satellite source drops that source's quarantine table and
    leaves the entity's other satellite tables untouched.
    """
    # DEF-17: one update is one load run. Resolve the batch id here, at build scope,
    # because discovering it needs `SET` and SQL refuses that inside a flow body.
    global _RESOLVED_BATCH_ID, _RESOLVED_CATALOG
    _RESOLVED_BATCH_ID = _batch_id(spark)
    try:
        _RESOLVED_CATALOG = (spark.conf.get("hfig.catalog", "") or "").strip()
    except Exception:
        _RESOLVED_CATALOG = ""
    print(f"[accelerator] batch id for this update: {_RESOLVED_BATCH_ID}")

    built: list[str] = []
    for entity in model.entities:
        if only is not None and entity.name not in only:
            continue
        for src, table in entity.tables():
            # hub / link / nhl: one conformed table, one append flow per source.
            # satellite: this table belongs to exactly one source, by construction.
            #
            # DEF-42: a STAGED kind's flows write to its log, not to the vault table. The
            # vault table is created and loaded by checks/load_hubs.py, whose anti-join
            # needs to read the target -- which is exactly what a streaming flow cannot
            # do. Everything below is unchanged otherwise: the log has the vault table's
            # declared shape, its masks and its quarantine twin.
            target = naming.pipeline_table(entity.kind, table)
            active = active_table_bindings(entity, src, active_sources)
            for binding in table_bindings(entity, src):
                if binding not in active:
                    print(f"[accelerator] inactive in this lake, no flow emitted: "
                          f"{binding_id(entity, binding)} -> {target} "
                          f"({binding.bronze_table})")
            # ALWAYS: the vault table itself exists in every lake, empty or not. Its MASK
            # clauses are suppressed when nothing loads into it -- the masked columns do
            # not exist on a ghost-only table.
            _emit_target(entity, target, active_bindings=active,
                         spark=spark, model=model)
            # DEF-26: the ghost is built from the DECLARED field list wherever one
            # exists, so it cannot drift from what _emit_target promised.
            schema_fields = (
                _derived_schema_fields(entity, active, spark, model)
                if entity.masks and active else None
            )
            if active:
                _emit_quarantine(entity, target, active_bindings=active,
                                 spark=spark, model=model)
                for binding in active:
                    _register_source_flows(entity, binding, target, spark,
                                           expectations_table, model)
            else:
                print(f"[accelerator] no active binding for {target}: quarantine table "
                      f"skipped, since nothing can be rejected into it")
            _emit_ghost(entity, target, spark, schema_fields=schema_fields)
            # The _v1 projection needs a payload to project and, for a masked satellite,
            # columns to redeclare the mask over. A ghost-only table has neither.
            #
            # DEF-52: AND NO _v1 AT ALL FOR A STAGED KIND. `target` here is the object the
            # PIPELINE writes, which for a staged kind is the LOG -- so this call would
            # declare stg_sat_x_v1 OVER stg_sat_x, a type-2 view of the staging log in
            # which every re-delivery is a version. That is precisely the defect the batch
            # loader exists to remove, rebuilt as a view; and sat_x, the table consumers
            # actually name, would get no view at all.
            #
            # It cannot simply be pointed at sat_x instead: this pipeline runs BEFORE
            # checks/load_satellites.py, so on a first run sat_x does not exist yet and an
            # MV reading it fails at graph analysis, taking the whole pipeline down.
            #
            # LEFT UNDONE, DELIBERATELY, AND IT IS A PREREQUISITE FOR ACTIVATING ANY
            # SATELLITE: sat_x_v1 must be created by checks/load_satellites.py, after it
            # has built sat_x, carrying the same mask clauses _mask_clauses() supplies
            # here (a mask on the base table does not protect a derived view --
            # checks/mask_survival_check.py asserts exactly that). Consumers already name
            # sat_x_v1: checks/journal_integrity_check.py, checks/mask_survival_check.py
            # and checks/aggregate_reconciliation_check.py. No satellite is active in any
            # lake today, so suppressing here creates nothing and removes a wrong object;
            # building the loader-side view with correct mask handling is its own piece of
            # work with its own evidence, not something to rush into a fix wave.
            if entity.kind in (naming.SATELLITE_KINDS - naming.STAGED_KINDS) and active:
                _emit_v1_view(entity, target, spark, active_bindings=active)
            built.append(target)
    return built
