"""
THE SATELLITE LOADER: staging log -> satellite, by hashdiff comparison.

DEF-52. A satellite inserts a new version only when a row's hashdiff differs from the
LATEST stored version for its key, and "latest stored" is the table being written -- which
a streaming flow cannot read. Without the comparison every satellite re-appends every row
on every run, and no gate objects: a re-delivery arrives with a fresh load_dts and is
unique at (parent_hk, load_dts, sub_seq), the grain append_only_check asserts.

COMPARE TO THE LATEST, NOT TO SET MEMBERSHIP. A value moving A -> B -> A stores three rows
under the first rule and two under the second, and under the second _v1's
LEAD(load_dts) window reports B as current for ever. That is silent, permanent data loss
that passes every gate.

THE JOIN ON mas_key IS NOT `USING`. Probed live against this workspace (DBSQL 2026.32,
see docs/superpowers/evidence/2026-08-26-satellite-sql-probe.md): `USING (parent_hk,
mas_key)` compiles to `=`, and `NULL = NULL` is not true, so it silently drops the match
for every plain `sat` -- mas_key is NULL on both sides for every one of them, since only
`msat` populates it. The join below uses `c.<parent_hk> = t.<parent_hk> AND c.mas_key IS
NOT DISTINCT FROM t.mas_key` instead, which the same probe confirmed matches correctly
when both sides are NULL.

TWO LAYERS, TWO RUNS. csat lives in business_vault and is computed from a msat or nhl the
RAW vault pipeline writes; its own staging log (stg_csat_*) is produced by the SEPARATE
business_vault pipeline. A single run covering sat/msat/csat together, placed before that
pipeline runs, would look for stg_csat_* before it exists. --layer therefore restricts one
run to raw kinds (sat, msat) or business kinds (csat), mirroring the LAYER filter
src/pipelines/silver_vault.py already applies with naming.BUSINESS_KINDS -- one definition
of the raw/business split, read here rather than re-derived.

NOT DONE HERE, AND REQUIRED BEFORE ANY SATELLITE IS ACTIVATED: the _v1 view.
src/accelerator/factory.py used to emit `<target>_v1`, but for a staged kind `<target>` is
the staging LOG -- a type-2 view of the log makes every re-delivery a version, which is the
defect this file exists to remove -- and the factory cannot emit it over the vault table
either, because the pipeline runs BEFORE this script and sat_x does not exist on a first
run. So the emission is suppressed in the factory and sat_x_v1 must be created HERE,
after create_sql/insert_sql have built and loaded sat_x: the same LEAD(load_dts) windows
factory._emit_v1_view still carries, plus the MASK clauses factory._mask_clauses supplies,
because a mask on the base table does not protect a derived view (checks/mask_survival_
check.py asserts that). Consumers already name sat_x_v1: checks/journal_integrity_check.py,
checks/mask_survival_check.py, checks/aggregate_reconciliation_check.py. No satellite is
active in any lake today, so nothing is broken by its absence right now -- but the first
activation is blocked on it.

FIX ROUND 1 (review of commit 7181d37 against docs/superpowers/specs/2026-08-26-satellite-
change-detection-design.md, section 3). Two Criticals in the brief's own SQL, transcribed
faithfully the first time and wrong both times:

  * `SELECT * EXCEPT (prev) FROM incoming t LEFT JOIN current c` -- a star over a JOIN, not
    a subquery, so it expands across BOTH sides: |log| + 2 columns (+3 for msat) against a
    target created |log| columns wide. Every satellite raised an arity mismatch on first
    real execution. Fixed by rendering the EXPLICIT declared column list (declared_columns
    below), qualified `t.` throughout, per spec section 3's `SELECT <declared columns>`.
  * `LAG` was computed AFTER the `NOT EXISTS` anti-join filtered the log down to only
    not-yet-loaded rows -- so a row's predecessor was whatever the FILTERED subset put
    before it, not whatever the FULL log put before it. Traced (and confirmed the
    reviewer's trace) against a cumulative log A@t1, A@t2, B@t3, A@t4 on one key: with the
    anti-join applied first, A@t2 loses its true predecessor A@t1 (already filtered out),
    is seeded from the GLOBAL latest instead, and inserts in the wrong order -- and A@t4
    never gets evaluated a second time once its row is filtered on a later idle run,
    breaking idempotency too. LAG must see the WHOLE log, unfiltered; NOT EXISTS decides
    only what to INSERT, so it moved to the final WHERE, ANDed with the hashdiff test.

One Important, same review: the msat anti-join grain was missing `mas_key`.
append_only_check.py asserts uniqueness at (parent_hk, load_dts, sub_seq[, mas_key]) for an
msat, but this file's NOT EXISTS matched only the first three -- and because sub_seq is a
literal 0 and load_dts is one current_timestamp() per batch, a worker with three skills
delivered in one batch shares one (parent_hk, load_dts, sub_seq): once ANY one mas_key was
stored there, its never-loaded siblings matched on the truncated grain and were silently
dropped. Fixed by adding `x.mas_key IS NOT DISTINCT FROM t.mas_key` to the anti-join when
is_msat -- mas_key does not exist on a plain sat, so the grains already agreed there.
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
from accelerator.stable_views import stable_view_action, stable_view_sql  # noqa: E402,F401

GATE = "load_satellites"

# ONE definition, two readers: naming.SATELLITE_KINDS (src/accelerator/naming.py) is what
# checks/load_satellites.py needs (every satellite kind, never hub) for its hashdiff
# compare, kept as its own name rather than inlined into naming.STAGED_KINDS because
# the hub anti-join and this anti-join are different SQL shapes keyed off different
# sets. Read from naming, not redeclared here: BUSINESS_KINDS was once duplicated
# between naming.py and src/pipelines/silver_vault.py and drifted, which is exactly
# the failure a second SAT_KINDS tuple here would repeat.
SAT_KINDS = naming.SATELLITE_KINDS


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def kinds_for_layer(layer: str) -> frozenset[str]:
    """Which satellite kinds ONE run of this script processes.

    raw     -> naming.SATELLITE_KINDS minus naming.BUSINESS_KINDS (sat, msat, esat today)
    business -> naming.SATELLITE_KINDS intersected with naming.BUSINESS_KINDS (csat today)

    Mirrors the split src/pipelines/silver_vault.py makes with the same two sets, so a
    kind added to BUSINESS_KINDS moves here too without this file changing.
    """
    if layer == "business":
        return SAT_KINDS & naming.BUSINESS_KINDS
    return SAT_KINDS - naming.BUSINESS_KINDS


def staged_satellites(model) -> list:
    """[(entity, table)] for every satellite table the model declares, any layer."""
    out = []
    for e in model.entities:
        if e.kind not in SAT_KINDS:
            continue
        for _src, table in e.tables():
            out.append((e, table))
    return sorted(out, key=lambda p: p[1])


def qualified_mask_fn(catalog: str, fn: str) -> str:
    """`catalog`.`schema`.`function`, because a two-part name resolves to spark_catalog.

    DEF-21, the same trap factory._qualified_mask_fn exists for: inside the pipeline a
    two-part `governance.mask_money` resolves against spark_catalog rather than the
    vault's catalog and fails with SCHEMA_NOT_FOUND. A batch ALTER runs in a different
    session with a different current catalog, so it needs the same treatment.
    """
    parts = [p for p in fn.split(".") if p]
    if len(parts) == 3:
        return ".".join(f"`{p}`" for p in parts)
    return ".".join(f"`{p}`" for p in [catalog, *parts])


def mask_statements(catalog: str, schema: str, table: str, entity, bindings,
                    already_masked) -> list[str]:
    """ALTER statements putting every DECLARED mask onto a STAGE-LOADED table. Pure.

    ONE IMPLEMENTATION, TWO LOADERS. checks/load_hubs.py imports this function (the
    checks/ sibling import checks/landing_integrity_check.py already uses for
    loop1_reconciliation). It is not satellite-specific and never was: every staged kind
    is created here by CTAS from its log, and every one of them loses its masks that way.
    The parameter is `table`, not `sat`, because the caller may be loading
    nhl_general_journal_line_rev1. A second copy of this rule in the keyed loader is the
    defect shape this repo has been bitten by repeatedly -- see the note above
    HUB_LOADER_KINDS in checks/load_hubs.py, and accelerator.stable_views for the
    precedent.

    `bindings` IS EVERY BINDING THAT FEEDS THIS TABLE, not one source. It was a single
    `src` while only satellites called this, and a satellite is one table per source. A
    keyed table is ONE table fed by N bindings, so the casts have to be collected across
    all of them -- exactly as factory._mask_clauses does, from the same
    spec.active_table_bindings() list. Pass the ACTIVE ones: an inactive binding declares
    no cast in this lake and the factory emits no column for it either.

    WHY A STAGE-LOADED TABLE NEEDS THIS AT ALL, AND WHY IT IS AN ALTER.
    create_sql builds the table with `AS SELECT * ... WHERE 1=0`. A CTAS copies the
    SHAPE and NOT the column masks, so the staging log carries the masks the factory
    emitted into its streaming-table definition and the vault table -- the one holding
    the money -- is created with none.

    MEASURED 25 September, on the first run that ever reached this loader:
    assert_mask_survival reported 21 columns, among them
    sat_invoice_header_fieldglass_us.gross_invoice_amount. Under parent decision D3 a
    column mask is the vault's ONLY PII defence, so an unmasked satellite is the control
    absent rather than degraded. It was not introduced that day -- every earlier run
    stopped before load_satellites, so the table had never existed to be looked at.

    AN ALTER, NOT A COLUMN LIST IN THE CREATE, and the distinction matters twice.
    factory._mask_clauses puts masks in the table DEFINITION because a pipeline-owned
    streaming table is rewritten on every update and an ALTER would not survive it. A
    staged kind's vault table is created here, by a batch job, and nothing rewrites its
    definition -- so the ALTER holds. (checks/mask_survival_check.py's remediation text
    says which of the two a reader should look for, per kind; before 29 September it
    named the factory for every kind and would have sent an operator hunting in the one
    place that is now correctly silent.) And `CREATE TABLE IF NOT EXISTS` is a no-op on the tables that
    already exist unmasked, while an ALTER repairs them. A fix that only protected
    tables created after today would have left the 21 exactly as they are.

    THE TABLE IS EMPTY WHEN THIS RUNS. create_sql selects WHERE 1=0 and the INSERT
    follows, so no row is ever readable through an unmasked column.

    The type comes from the binding's cast: block, as it does in the factory -- the mask
    function's parameter type must agree with the column it masks, so the cast is the
    only value that can be right. A declared mask with no cast RAISES rather than
    silently skipping: that is someone adding a mask without a type, and spec.validate
    does not catch it for a satellite whose binding is inactive elsewhere.
    """
    if not entity.masks:
        return []
    declared: dict = {}
    for binding in (bindings or ()):
        for col, sql_type in binding.cast:
            declared.setdefault(col, set()).add(sql_type)

    out, missing = [], []
    for col, fn in entity.masks:
        if col in already_masked:
            continue
        if not declared.get(col):
            missing.append(col)
            continue
        out.append(
            f"ALTER TABLE {q(catalog, schema, table)} "
            f"ALTER COLUMN `{col}` SET MASK {qualified_mask_fn(catalog, fn)}")
    if missing:
        raise ValueError(
            f"{entity.name}: no cast: declared for masked column(s) {sorted(missing)}. "
            f"A masked column must declare its type -- the mask function's parameter type "
            f"has to agree with the column it masks. For governance.mask_money that is "
            f"DECIMAL(18,2).")
    return out


def masked_columns(spark, catalog: str, schema: str, table: str) -> set:
    """Columns on this table that already carry a mask, from Unity Catalog.

    Asked rather than assumed, because SET MASK on a column that already has one is an
    error. A missing information_schema view is treated as "nothing masked" -- the ALTER
    then either succeeds or fails loudly, which is better than skipping silently.
    """
    try:
        rows = spark.sql(
            f"SELECT column_name FROM `{catalog}`.information_schema.column_masks "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'").collect()
    except Exception:  # noqa: BLE001 -- absence of the view is not a reason to stop
        return set()
    return {r.asDict()["column_name"] for r in rows}


def create_sql(catalog: str, schema: str, sat: str, log: str) -> str:
    """Create the satellite with the LOG's exact shape, or leave an existing one alone.

    `SELECT *` is safe here -- unlike in insert_sql -- because this SELECT has no join:
    the log already has the table's declared shape (factory._projection built it), one
    table, no second side for a star to expand across.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {q(catalog, schema, sat)} "
        # DECIDED 26 Aug 2026. The factory clusters the tables IT creates (link, NHL,
        # staging); these are created here, so the decision has to be declared here too
        # or the largest tables in the vault end up the only ones without it.
        # factory._cluster_refusal is the authority on whether load_dts is clusterable
        # -- it is, on every entity, since DEF-41 narrowed the tables into the
        # 32-column statistics window -- and the suite asserts these two agree.
        #
        # Before TBLPROPERTIES only for conventional order; table_clauses accept any.
        #
        # CREATE TABLE IF NOT EXISTS means this reaches NEW tables ONLY. Every table
        # already loaded needs an explicit ALTER TABLE ... CLUSTER BY, which is a live
        # workspace step held in OPEN_ITEMS.md, not something this script does.
        f"CLUSTER BY ({naming.COL['load_dts']}) "
        f"TBLPROPERTIES ("
        f"'delta.appendOnly' = 'true', "
        # ONE WORD FOR FOUR KINDS, deliberately: this loader is one loader, and the
        # property answers "which loader built this". The vocabulary across both loaders
        # is hub | link | nhl | hal | satellite -- checks/load_hubs.py passes the kind
        # through verbatim, this one collapses sat/msat/esat/csat. Nothing reads it yet;
        # see that file's create_sql docstring for the asymmetry.
        f"'hfig.vault_kind' = 'satellite', "
        f"'hfig.loaded_by' = 'checks/load_satellites.py') "
        f"AS SELECT * FROM {q(catalog, schema, log)} WHERE 1=0"
    )


def declared_columns(entity, src) -> list[str]:
    """The satellite's declared column order, computed WITHOUT factory or Spark.

    Deliberately a second, independent computation of the same fact factory._projection
    computes -- spec section 6 (docs/superpowers/specs/2026-08-26-satellite-change-
    detection-design.md) asks for the loader's rendered column list to be COMPARED
    against factory._projection's, not derived from it, so a change to either one the
    other misses fails a test instead of drifting silently the way BUSINESS_KINDS once
    did between naming.py and silver_vault.py.

    Mirrors _projection's satellite branch exactly: parent hk, hashdiff, mas_key (msat
    only), then payload (a binding's own payload override if it declares one, else the
    entity's), each deduped to its FIRST occurrence, then every system column. No
    collision guard -- factory._projection raises on a genuine collision at build time,
    before this function would ever see that metadata; this one only needs to match its
    result on the metadata that reaches production.
    """
    seen: set[str] = set()
    cols: list[str] = []

    def add(name: str) -> None:
        if name not in seen:
            seen.add(name)
            cols.append(name)

    add(naming.hk(entity.parents[0]))
    add(naming.COL["hashdiff"])
    if entity.kind == "msat":
        add(naming.COL["mas_key"])
    payload = (src.payload if src is not None and src.payload else None) or entity.payload
    for column in payload:
        add(column)
    for column in naming.system_columns_for(entity.kind):
        add(column)
    return cols


def insert_sql(catalog: str, schema: str, sat: str, log: str,
               parent_hk: str, is_msat: bool, columns: list[str]) -> str:
    """Insert only rows whose hashdiff differs from the latest stored version.

    Two comparisons compose here, not one:
      * LAG(hashdiff), computed over the WHOLE staged log, collapses consecutive
        duplicates delivered in the SAME run (A, A -> keep the first A only). It must
        see every row, not just the ones NOT EXISTS has not yet loaded -- LAG computed
        over a FILTERED subset gives a row the wrong predecessor (or none), which is
        Critical defect #2 from fix round 1: seeded from the wrong version, inserted in
        the wrong order, and not idempotent on a later idle run;
      * coalesce(prev, stored latest) SEEDS the first row of the batch from what is
        already on disk, so a batch that opens with the same value the table already
        ends on is not mistaken for a change. Without this seed every batch's first row
        looks new regardless of what came before it.
    Both compare to the LATEST version, never to (key, hashdiff) set membership: A -> B
    -> A must store three rows, and set membership would store two while the derived
    _v1 view reports B as current for ever.

    NOT EXISTS decides only what to INSERT -- it is ANDed into the final WHERE, never
    inside the CTE that computes LAG, so it cannot change what any row is compared
    against, only whether the row already loaded is re-inserted. Its grain is
    (parent_hk, load_dts, sub_seq[, mas_key for an msat]) -- exactly what
    append_only_check.py asserts uniqueness at. Omitting mas_key there (Important
    defect #3, fix round 1) let one worker's stored skill silently absorb its
    never-loaded siblings, because sub_seq is a literal 0 and load_dts is one
    current_timestamp() per batch -- every row of one msat batch, for one key, shares
    that grain except for mas_key.

    `mas_key` never appears in the join, partition or anti-join unless is_msat: it is
    NULL for every plain `sat`, and `USING (parent_hk, mas_key)` -- proved wrong live
    against this workspace -- would drop the match for all of them. `IS NOT DISTINCT
    FROM` matches two NULLs, `=` does not.

    `columns` is the table's EXPLICIT declared shape (declared_columns(), compared by
    test against factory._projection's). `SELECT t.<columns>` rather than `SELECT
    t.* EXCEPT (prev)` -- Critical defect #1, fix round 1: a star expands across BOTH
    sides of `... FROM staged t LEFT JOIN current c`, not just `t`, so the old form
    inserted |log| + 2 (+3 for msat) columns into a target created |log| columns wide,
    an arity mismatch on every satellite. Every column is qualified `t.` so a name
    `current` also carries (parent_hk, mas_key, hashdiff) can never resolve ambiguously.
    """
    hd = naming.COL["hashdiff"]
    ld = naming.COL["load_dts"]
    ss = naming.COL["sub_seq"]
    mk = naming.COL["mas_key"]
    part = f"s.`{parent_hk}`" + (f", s.`{mk}`" if is_msat else "")
    cpart = f"`{parent_hk}`" + (f", `{mk}`" if is_msat else "")
    join = f"c.`{parent_hk}` = t.`{parent_hk}`" + (
        f" AND c.`{mk}` IS NOT DISTINCT FROM t.`{mk}`" if is_msat else "")
    anti_join = f"x.`{parent_hk}` = t.`{parent_hk}`\n" \
                f"      AND x.`{ld}` = t.`{ld}` AND x.`{ss}` = t.`__sub_seq`" + (
                    f"\n      AND x.`{mk}` IS NOT DISTINCT FROM t.`{mk}`" if is_msat else "")
    ad = naming.COL["applied_dts"]
    # DEF-55: the ORDER of versions within one batch, and the sub_seq that makes them
    # distinct. `_stage` stamps load_dts with one current_timestamp() per batch and sub_seq
    # with a literal 0, so every version of a key loaded together was indistinguishable at
    # the grain append_only_check asserts -- 1,358 collisions on the first real satellite
    # load -- and the LAG below tied on (load_dts, sub_seq), leaving which hashdiff counted
    # as the predecessor arbitrary.
    #
    # applied_dts is the SOURCE's own statement of when a row changed, so it is the
    # semantically right order. `hashdiff` is appended purely as a TIEBREAK: applied_dts is
    # not unique (45,226 distinct values over 69,208 staged Bullhorn rows), and without a
    # stable second key row_number() could assign different sub_seq values on a re-run,
    # which would break the NOT EXISTS below and make the loader non-idempotent. The
    # tiebreak carries no meaning and is not claimed to.
    version_order = f"s.`{ad}`, s.`{hd}`"
    col_list = ", ".join(
        (f"t.`__sub_seq`" if c == ss else f"t.`{c}`") for c in columns)
    # DEF-54: the INSERT NAMES its target columns. An inferred streaming table's
    # column order is the union of its FLOWS -- the ghost flow first, so hk and the
    # system columns lead and hashdiff lands ninth -- while this SELECT is in
    # _projection order, where hashdiff is second. A positional INSERT therefore put
    # a BINARY hashdiff into a TIMESTAMP load_dts and failed with
    # DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION on the first real load. Naming the
    # columns makes the statement independent of both orders.
    col_names = ", ".join(f"`{c}`" for c in columns)
    return (
        f"INSERT INTO {q(catalog, schema, sat)} ({col_names})\n"
        f"WITH current AS (\n"
        f"  SELECT {cpart}, `{hd}` FROM (\n"
        f"    SELECT {cpart}, `{hd}`, row_number() OVER (\n"
        f"      PARTITION BY {cpart} ORDER BY `{ld}` DESC, `{ss}` DESC) AS rn\n"
        f"    FROM {q(catalog, schema, sat)}) WHERE rn = 1),\n"
        f"staged AS (\n"
        f"  -- EVERY log row, unfiltered: LAG must see the whole log, not just the\n"
        f"  -- rows not yet loaded, or a row gets the wrong predecessor.\n"
        f"  SELECT s.*,\n"
        f"    CAST(ROW_NUMBER() OVER (\n"
        f"      PARTITION BY {part}, s.`{ld}` ORDER BY {version_order}) - 1 AS INT)\n"
        f"      AS `__sub_seq`,\n"
        f"    LAG(s.`{hd}`) OVER (\n"
        f"      PARTITION BY {part} ORDER BY s.`{ld}`, {version_order}) AS prev\n"
        f"  FROM {q(catalog, schema, log)} s)\n"
        f"SELECT {col_list} FROM staged t\n"
        f"LEFT JOIN current c ON {join}\n"
        f"WHERE t.`{hd}` IS DISTINCT FROM coalesce(t.prev, c.`{hd}`)\n"
        f"  AND NOT EXISTS (\n"
        f"    SELECT 1 FROM {q(catalog, schema, sat)} x\n"
        f"    WHERE {anti_join})"
    )


def v1_sql(catalog: str, schema: str, sat: str, parent_hk: str, is_msat: bool) -> str:
    """The derived type-2 view over a satellite the LOADER owns.

    DEF-53. The pipeline cannot emit this one. `_emit_v1_view` runs at pipeline-definition
    time, and for a staged kind the vault table does not exist yet -- the batch loader
    creates it afterwards -- so a materialized view over it fails at graph analysis on a
    first run. DEF-52 therefore suppressed `_v1` for staged kinds and left this gap open.

    A PLAIN VIEW, NOT A MATERIALIZED ONE, AND THAT IS WHY IT NEEDS NO MASK CLAUSE.
    Measured 26 Aug 2026 (docs/superpowers/evidence/2026-08-26-mask-propagation-through-views.md):
    reading a masked column as an identity the mask denies returns empty through the base
    table AND through a plain view over it. A view copies nothing, so the base table's mask
    applies at read time.

    A materialized view is different, and 3a section 1.3's finding still stands: an MV
    projection of a masked column that declares no mask of its own IS an unmasked copy,
    because an MV stores data. That is why the pipeline's `_v1` must redeclare and this one
    must not -- it cannot anyway, both `CREATE VIEW (col MASK fn)` and
    `ALTER VIEW ... SET MASK` are syntax errors on this runtime, probed and recorded.

    End-dating is computed, never stored. It runs on applied_dts, the source's own
    statement of when a row changed, because load_dts is one timestamp per batch and
    would collapse every intra-batch interval to zero length (DEF-56).
    """
    ld = naming.COL["load_dts"]
    ss = naming.COL["sub_seq"]
    ad = naming.COL["applied_dts"]
    part = f"`{parent_hk}`" + (f", `{naming.COL['mas_key']}`" if is_msat else "")
    # DEF-56: END-DATE ON BUSINESS TIME, NOT ON OUR OWN CLOCK.
    #
    # This used to read valid_from = load_dts and valid_to = LEAD(load_dts). _stage stamps
    # ONE current_timestamp() per batch, so every version of a key that arrived together
    # carried the same load_dts and the interval collapsed: 1,373 of 46,889 rows on the
    # first real satellite -- exactly every superseded version -- had valid_to ==
    # valid_from. The view reported that a vacancy was "Accepting Candidates" for zero
    # duration, when the source says it held that state for nineteen days.
    #
    # applied_dts is the SOURCE's statement of when the row changed, so it is the only
    # column that can carry a real interval. load_dts still answers "when did we learn
    # this", and remains on the row for audit; it is no longer mistaken for "when was
    # this true".
    #
    # THE ORDER MOVES WITH IT, and that is not cosmetic. applied_dts is not monotonic with
    # load_dts: a backfill or a late-arriving correction delivers, in a later batch, a row
    # whose effective time is EARLIER than a version already stored. End-dating on
    # business time while ordering by arrival time would then emit valid_to < valid_from
    # -- a negative interval. Ordering by applied_dts makes the interval monotonic by
    # construction, and makes is_current mean "the version the source says is newest"
    # rather than "the row that happened to load last".
    #
    # load_dts then sub_seq break ties, so the order is total and the view is
    # deterministic. Measured on the first satellite: no version of any key shares an
    # applied_dts with another version of that key, so every interval is real.
    order = f"`{ad}`, `{ld}`, `{ss}`"
    return (
        f"CREATE OR REPLACE VIEW {q(catalog, schema, naming.v1_view(sat))}\n"
        f"COMMENT 'derived type-2 view -- valid_to and is_current are computed, never "
        f"stored. Built by checks/load_satellites.py, not by the pipeline: the vault "
        f"table it reads does not exist at pipeline-definition time.'\n"
        f"AS SELECT *,\n"
        f"       `{ad}` AS valid_from,\n"
        f"       LEAD(`{ad}`) OVER (PARTITION BY {part} ORDER BY {order}) AS valid_to,\n"
        f"       LEAD(`{ad}`) OVER (PARTITION BY {part} ORDER BY {order}) IS NULL "
        f"AS is_current\n"
        f"FROM {q(catalog, schema, sat)}"
    )


# stable_view_sql() and stable_view_action() used to be defined here, byte-identical to
# checks/load_hubs.py's pair of the same name (only a parameter name differed). Moved to
# accelerator.stable_views (R1, 26 September) rather than adding the third copy
# checks/publish_stable_views.py would otherwise have needed -- see that module's
# docstring for the three-case rule (bootstrap / already_here / elsewhere) and why the
# caller bootstraps a stable view and never moves it. Imported above; this loader still
# calls stable_view_sql(...) and stable_view_action(...) exactly where it always did, in
# main() below. v1_sql() above is a SEPARATE, unmoved object -- the derived type-2 view
# (DEF-52, DEF-53) is a satellite-only concern, not part of the shared stable-view rule.


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="raw_vault")
    ap.add_argument("--business-vault-schema", default="business_vault")
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--active-sources", default="")
    # DEF-52 follow-up: csat's staging log is produced by the business_vault pipeline,
    # which runs AFTER raw_vault. A single run covering every satellite kind, placed
    # before business_vault, would look for stg_csat_* before it exists -- so the job
    # calls this script twice, once per layer, --layer raw before business_vault runs
    # and --layer business after.
    ap.add_argument("--layer", choices=("raw", "business"), default="raw",
                    help="which satellite kinds this run processes -- raw (sat, msat) "
                         "or business (csat); see kinds_for_layer()")
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the load audit. Required, not defaulted.")
    ap.add_argument("--job-run-id", required=True,
                    help="{{job.run_id}}, the key tying one run's audit rows together")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    meta = Path(args.metadata) if args.metadata else (
        Path(__file__).resolve().parents[1] / "metadata" / "entities")
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)
    layer_kinds = kinds_for_layer(args.layer)
    targets = [(e, t) for e, t in staged_satellites(model) if e.kind in layer_kinds]

    if not targets:
        print(f"GATE NOT EVALUATED: the model declares no {args.layer}-layer satellite "
              f"(kinds {sorted(layer_kinds)}). That is not a dormant lake, it is a model "
              f"with no descriptive history in this layer at all.")
        return finish("NOT_EVALUATED", 0, 0, 1)

    loaded, skipped, failed = 0, [], []
    spark = None
    if not args.dry_run:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

    for entity, sat in targets:
        src = next((s for s, t in entity.tables() if t == sat), None)
        # THE ACTIVE BINDINGS, COMPUTED ONCE. The skip below and mask_statements() must
        # ask the SAME question -- which bindings actually load into THIS table -- or the
        # loader could mask a table from a binding this lake does not load, or skip one it
        # does. checks/load_hubs.py holds the same variable for the same reason.
        bindings = spec.active_table_bindings(entity, src, active)
        if not bindings:
            skipped.append(f"{sat}: no active source binding in this lake")
            continue
        schema = naming.vault_schema_for(entity.kind, args.schema,
                                         args.business_vault_schema)
        log = naming.stg(sat)
        parent_hk = naming.hk(entity.parents[0])
        # entity.tables() hands back src=None for a one-table-per-entity kind (csat is
        # one table fed by its single declared source, not one table per source like
        # sat/msat) -- declared_columns() needs the real binding for its payload, so
        # fall back to the entity's own (spec.validate-enforced single) source.
        proj_src = src if src is not None else (entity.sources[0] if entity.sources
                                                  else None)
        columns = declared_columns(entity, proj_src)
        # WHAT IS ALREADY MASKED, asked of Unity Catalog rather than assumed. SET MASK on a
        # column that already carries one is an error, so a loader that set them blindly
        # would fail on its second run -- and a loader that skipped them blindly would
        # never repair a table created before this existed.
        already = masked_columns(spark, args.catalog, schema, sat) if not args.dry_run \
            else set()
        stmts = [create_sql(args.catalog, schema, sat, log),
                 *mask_statements(args.catalog, schema, sat, entity, bindings, already),
                 insert_sql(args.catalog, schema, sat, log, parent_hk,
                            entity.kind == "msat", columns)]
        if args.dry_run:
            print(f"\n-- {schema}.{sat} <- {log}")
            for s_ in stmts:
                print(s_ + ";")
            print(v1_sql(args.catalog, schema, sat, parent_hk,
                         entity.kind == "msat") + ";")
            # Rendered unconditionally: there is no live workspace here to read
            # information_schema.views against, so this preview cannot show
            # stable_view_action()'s decision -- only what the statement WOULD be if
            # the view is bootstrapped or already points here. The live run below never
            # issues this statement when the view points elsewhere.
            print(stable_view_sql(args.catalog, schema, naming.stable(sat), sat) + ";")
            loaded += 1
            continue
        try:
            spark.sql(stmts[0])                      # create_sql
            for _mask_stmt in stmts[1:-1]:           # mask_statements, possibly none
                spark.sql(_mask_stmt)
                print(f"      masked {_mask_stmt.split('ALTER COLUMN ')[1][:60]}")
            sat_before = spark.sql(
                f"SELECT count(*) AS n FROM {q(args.catalog, schema, sat)}"
            ).collect()[0]["n"]
            spark.sql(stmts[-1])                     # insert_sql
            # DEF-53: the view comes after the load, because it reads the table the two
            # statements above create and populate.
            spark.sql(v1_sql(args.catalog, schema, sat, parent_hk,
                             entity.kind == "msat"))
            # THE STABLE VIEW, A SEPARATE OBJECT FROM sat_x_v1 ABOVE -- see
            # stable_view_sql's docstring. Also published only now, for the same reason:
            # it reads the table the insert just populated.
            #
            # BUT ONLY BOOTSTRAP OR NO-OP -- NEVER A MOVE. Read the view's own current
            # definition first: if it already exists and points somewhere other than
            # `sat`, that is a deliberate cutover or a deliberate rollback, and this
            # loader must not repoint it -- see stable_view_action()'s docstring.
            _stable = naming.stable(sat)
            _view_rows = spark.sql(
                f"SELECT view_definition FROM `{args.catalog}`.information_schema.views "
                f"WHERE table_schema = '{schema}' AND table_name = '{_stable}'"
            ).collect()
            _view_exists = bool(_view_rows)
            _view_definition = _view_rows[0]["view_definition"] if _view_rows else ""
            _action = stable_view_action(_view_exists, _view_definition, sat)
            if _action == "elsewhere":
                print(f"  LEFT ALONE {_stable}: it already points elsewhere, not at "
                      f"{sat} -- current definition: {_view_definition!r}. A loader "
                      f"bootstraps the stable view, it never moves it; run "
                      f"checks/cutover_vault_version.py to repoint it deliberately.")
            elif _action == "bootstrap":
                spark.sql(stable_view_sql(args.catalog, schema, _stable, sat))
            # already_here: DO NOTHING. CREATE OR REPLACE VIEW replaces the securable
            # and every grant on it, so re-issuing an identical statement silently
            # revokes SELECT on the stable name -- see stable_view_sql()'s docstring.
            n = spark.sql(f"SELECT count(*) AS n FROM {q(args.catalog, schema, sat)}"
                          ).collect()[0]["n"]
            m = spark.sql(f"SELECT count(*) AS n FROM {q(args.catalog, schema, log)}"
                          ).collect()[0]["n"]
            accepted = n - sat_before
            discards = {"unchanged_hashdiff": m - accepted}
            problem = audit.check_arithmetic(m, accepted, discards)
            if problem:
                raise ValueError(f"{sat}: audit arithmetic does not balance -- {problem}")
            # DEF-56: A REPAIR RUN REUSES {{job.run_id}}, so a satellite that already
            # succeeded is re-processed here -- the hashdiff compare inserts nothing, but a
            # second audit row would say it did. Guard on existence, the same idiom the
            # vault insert uses, and skip the discard row with it: a duplicate discard set
            # makes unbalanced_tables report a gap against BOTH rows, and aud_table_load is
            # append-only so the completeness gate would then be red for ever.
            audited = spark.sql(audit.table_load_exists_sql(
                args.catalog, args.control_schema, job_run_id=args.job_run_id,
                table_name=sat)).collect()[0]["n"]
            if audited:
                print(f"  ~    {sat:44} already audited for run {args.job_run_id} "
                      f"({audited} row(s)) -- retry, the audit is not rewritten")
            else:
                spark.sql(audit.table_load_sql(
                    args.catalog, args.control_schema, job_run_id=args.job_run_id,
                    pipeline_update_id=None, table_name=sat,
                    written_by="checks/load_satellites.py", staged=m, accepted=accepted))
                if discards["unchanged_hashdiff"]:
                    spark.sql(audit.table_discard_sql(
                        args.catalog, args.control_schema, job_run_id=args.job_run_id,
                        table_name=sat, reason="unchanged_hashdiff",
                        discarded=discards["unchanged_hashdiff"]))
            print(f"  ok   {sat:44} log={m:>9}  sat={n:>9} "
                  f"| +{accepted} versions, {discards['unchanged_hashdiff']} unchanged")
            loaded += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {sat}: {exc}")
            failed.append(sat)

    for s_ in skipped:
        print(f"  ~ {s_}")
    if args.dry_run:
        print(f"\ndry run only -- {loaded} satellite(s), layer={args.layer}, "
              f"nothing executed")
        return 0
    if failed:
        print(f"\nSATELLITE LOAD FAILED -- {len(failed)}: {failed}")
        return finish("FAILED", loaded, len(skipped), 1)
    if not loaded:
        print("\nGATE NOT EVALUATED: every satellite is inactive in this lake.")
        return finish("NOT_EVALUATED", 0, len(skipped), 0)
    print(f"\nSATELLITE LOAD PASSED: {loaded} satellite(s), layer={args.layer}")
    return finish("PASSED", loaded, len(skipped), 0)


if __name__ == "__main__":
    _rc = main()
    if _rc:
        sys.exit(_rc)
