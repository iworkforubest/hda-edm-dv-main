"""
THE KEYED LOADER: staging log -> vault table, by anti-join.

The MECHANISM is unchanged and is still the classical Data Vault hub loader. The SCOPE
is not: this task now loads every KEYED kind -- hub, link, NHL and HAL -- because every
one of them needs exactly one row per hash key, and none of them can establish that from
inside a streaming flow.

DEF-42. A hub is a conformed identity, so several sources legitimately supply the same
business key. Measured on the loaded vault, 25 Aug 2026:

    hub_accounting_journal   2,759,294 rows over 2,221,108 keys -- 538,186 duplicates
      within GP_US                                                          0 duplicates
      within GP_US_HIST                                                     0 duplicates

Every duplicate was a key supplied by TWO sources. The staging dedup was never the
problem: a hub needs insert-if-not-exists ACROSS flows, and a streaming table cannot read
its own contents, so no append flow can do it.

This is the classical Data Vault hub loader, which is an INSERT with a lookup. The lookup
is the part a streaming flow cannot do; a batch task can. The target therefore stays
strictly append-only -- DESCRIBE HISTORY shows only WRITE -- so append_only_check keeps
its present meaning instead of being relaxed for the kinds loaded here.

LINKS, NHLs AND HALs FOR A DIFFERENT DUPLICATE. A hub is here because two sources supply
one key. A link, NHL or HAL is here because ONE source can supply one key twice: their
only previous protection was an in-stream dropDuplicates whose memory lives in the
CHECKPOINT, and a checkpoint reset re-reads the whole staging log against an empty memory.
Measured 28 Sep 2026: exactly that duplicated 1,164 rows into nhl_invoice_line_rev2. The
anti-join asks the TARGET instead, and the target does not forget.

IDEMPOTENT BY CONSTRUCTION. `NOT EXISTS` means a second run inserts nothing, and
`row_number() OVER (PARTITION BY hk ORDER BY load_dts, rec_src) = 1` makes the surviving
row deterministic, so first-seen provenance is stable across re-runs rather than being
whichever flow happened to win a race.

EXCEPT WHERE THAT ORDER IS A TIE, which is what ambiguous_grain_refusal() guards. Rows
sharing (hash key, load_dts, rec_src) are not ordered by it at all, and NOT EXISTS then
makes the shuffle's pick permanent. Measured 29 Sep 2026: the tie is real (5,266 groups
in stg_hub_job_request_rev1) and in every one of them the tied rows are IDENTICAL, so the
refusal fires on divergence rather than on duplicate counts.
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
# THE SIBLING CHECKS DIRECTORY, so mask_statements() can be IMPORTED rather than copied.
# checks/landing_integrity_check.py does exactly this for loop1_reconciliation, and that
# task is deployed, so the mechanism is proven in a serverless spark_python_task.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "checks"))

from accelerator import audit, naming, spec  # noqa: E402
from accelerator.stable_views import stable_view_action, stable_view_sql  # noqa: E402,F401
from load_satellites import mask_statements, masked_columns  # noqa: E402

GATE = "load_hubs"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


# --------------------------------------------------------------------------- #
def create_sql(catalog: str, schema: str, hub: str, log: str, kind: str = "hub") -> str:
    """Create the target with the LOG's exact shape, or leave it alone if it exists.

    `kind` IS A PARAMETER AND NOT THE LITERAL 'hub', because this loader now creates
    lnk_/nhl_/hal_ tables too and hfig.vault_kind is a table property a catalog reader
    takes at face value. The default is "hub" so the property can never end up ABSENT --
    but a default that silently restores the defect is worth nothing on its own, so
    tests/test_accelerator.py asserts both call sites below pass `e.kind`.

    THE PROPERTY'S VOCABULARY IS hub | link | nhl | hal | satellite, and nothing defines
    it: the first four are naming.KEYED_KINDS, passed through verbatim, and the fifth is
    the literal checks/load_satellites.py writes for every satellite kind (it collapses
    sat/msat/esat/csat into one word deliberately -- the loader is one loader). Nothing
    reads the property today. If anything ever does, that asymmetry is what it will trip
    over, and this sentence is the only place it is written down.

    `AS SELECT * ... WHERE 1=0` rather than a rendered column list: the log already has
    the declared shape (factory._projection built it), so taking the shape FROM the log
    means the two cannot disagree. A rendered DDL would be a second description of the
    same thing, free to drift.

    THE SHAPE IS ALL IT COPIES -- A CTAS DOES NOT COPY THE COLUMN MASKS. That is why
    main() issues load_satellites.mask_statements() between this statement and the
    INSERT; see the block above the call. It was harmless while this loader owned only
    `hub` (no hub declares a mask) and stopped being harmless the moment link, NHL and
    HAL were staged into it: five of the six NHLs declare 17 masked money columns
    between them, and every one of them would have landed on a table created here with
    no mask at all.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {q(catalog, schema, hub)} "
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
        f"'hfig.vault_kind' = '{kind}', "
        f"'hfig.loaded_by' = 'checks/load_hubs.py') "
        f"AS SELECT * FROM {q(catalog, schema, log)} WHERE 1=0"
    )


def insert_sql(catalog: str, schema: str, hub: str, log: str, hk: str) -> str:
    """Insert only keys the hub does not already hold, one row per key.

    `* EXCEPT (rn)` keeps the column list identical to the log's by construction; naming
    columns here would be a third place for the declared shape to be described.
    """
    return (
        f"INSERT INTO {q(catalog, schema, hub)}\n"
        f"SELECT * EXCEPT (rn) FROM (\n"
        f"  SELECT s.*, row_number() OVER (\n"
        f"    PARTITION BY s.`{hk}` ORDER BY s.`{naming.COL['load_dts']}`, "
        f"s.`{naming.COL['rec_src']}`) AS rn\n"
        f"  FROM {q(catalog, schema, log)} s\n"
        f") t\n"
        f"WHERE t.rn = 1\n"
        f"  AND NOT EXISTS (\n"
        f"    SELECT 1 FROM {q(catalog, schema, hub)} h WHERE h.`{hk}` = t.`{hk}`)"
    )


# --------------------------------------------------------------------------- #
# THE TIE-BREAK, AND THE ONE THING IT CANNOT ORDER.
#
# insert_sql keeps `row_number() OVER (PARTITION BY hk ORDER BY load_dts, rec_src) = 1`.
# That is deterministic exactly as far as the ordering triple is: two rows sharing
# (hash key, load_dts, rec_src) are TIED, and Spark then keeps whichever the shuffle
# happened to put first. NOT EXISTS makes that pick permanent -- the next run sees the
# key already present and inserts nothing, so a wrong survivor is never corrected by a
# re-run. There is no second chance to notice, which is why the guard runs BEFORE the
# insert rather than auditing after it.
#
# MEASURED BEFORE ANYTHING WAS INVENTED. usnc_tds, 29 Sep 2026, across all eleven hub
# staging logs: exactly ONE holds groups sharing the triple -- stg_hub_job_request_rev1,
# with 5,266 of them -- and the other ten hold none. In ZERO of those 5,266 groups does
# any column differ: job_request_bk, job_request_tenant, job_request_reference,
# applied_dts and manifest_id are each single-valued within every group. They are exact
# duplicates, which is what a staging log is for, and the anti-join already drops them.
#
# SO THE REFUSAL IS ON DIVERGENCE, NOT ON COUNT. A gate written against duplicate COUNTS
# would have fired on 5,266 groups on its first run and been switched off within a day --
# the classic gate that cries wolf and gets disabled. This one fires only when tied rows
# actually DISAGREE: when there is a real choice to make and no stated rule for making it.
#
# WHAT COUNTS AS DISAGREEMENT, AND WHY "NOT A SYSTEM COLUMN" IS THE WRONG RULE.
# naming.system_columns_for() reports five system columns for every keyed kind
# (load_dts, applied_dts, rec_src, batch_id, manifest_id), but two of those five carry
# SOURCE meaning rather than run metadata and must be compared:
#
#   * applied_dts -- when the SOURCE says the fact happened. Two rows tied on the
#     ordering triple but differing here are genuinely different versions of the fact,
#     and choosing between them arbitrarily is exactly what this refuses.
#   * manifest_id -- which delivery asserted it.
#
# batch_id is the opposite, and is the only one excluded: it differs BY CONSTRUCTION
# across runs, so comparing it would make every ordinary re-read of a log look divergent
# and would reproduce the cries-wolf failure from the other direction.
#
# The rule is therefore: group on (hash key, load_dts, rec_src), and compare every
# column EXCEPT load_dts, rec_src and batch_id. The first two are the grain itself --
# constant inside a group by construction -- and the third is run metadata. All three
# are read from naming.COL rather than spelled here, so a rename moves them together.
GRAIN_COLUMNS = (naming.COL["load_dts"], naming.COL["rec_src"])
RUN_LOCAL_COLUMNS = (naming.COL["batch_id"],)
UNCOMPARED_COLUMNS = GRAIN_COLUMNS + RUN_LOCAL_COLUMNS

# How much of a pathological log may be pulled into the driver. One WHOLE group is all a
# refusal needs, and the probe orders by the grain before it limits, so the rows that come
# back are contiguous and the first group is complete.
AMBIGUITY_ROW_LIMIT = 200

# The window alias the probe projects away again. Same idiom, and same reason, as
# insert_sql's `rn`: naming a column list here would be a second description of the log's
# declared shape, free to drift from it.
_N_VERSIONS = "n_versions"


def ambiguity_probe_sql(catalog: str, schema: str, log: str, hk: str,
                        limit: int = AMBIGUITY_ROW_LIMIT) -> str:
    """Rows whose (hash key, load_dts, rec_src) group holds more than one DISTINCT payload.

    THE DISTINCT IS THE WHOLE ECONOMY OF THIS QUERY. It collapses the exact duplicates in
    the cluster, so the 5,266 measured groups in stg_hub_job_request_rev1 leave `versions`
    as one row each and nothing about them ever reaches the driver. What survives a
    partition count above 1 is genuine disagreement: rows the INSERT's ORDER BY cannot
    separate and that the loader would otherwise have to guess between.

    `payload` drops only batch_id. load_dts and rec_src STAY -- they are the grain the
    window partitions on, and they are constant inside a group anyway, so keeping them
    cannot change what the DISTINCT collapses.

    ORDER BY the grain BEFORE the LIMIT, so the returned rows are contiguous and the first
    group is whole. The limit is a driver guard, not a sample size: one whole group is all
    a refusal needs, and every row returned belongs to a group of at least two.

    THREE NAMED CTEs RATHER THAN NESTED SUBQUERIES, and `SELECT DISTINCT *` over a
    `* EXCEPT` rather than `SELECT DISTINCT * EXCEPT` in one select list: the suite has no
    Spark, so it is written in the plainest form that can be, rather than the shortest.
    Proven against live Spark 29 Sep 2026 -- it parses, returns nothing on
    stg_hub_job_request_rev1 and on the 2.76M-row stg_hub_accounting_journal_rev1, and
    over synthetic rows ignores a batch_id-only difference while returning BOTH versions
    of a genuinely divergent group.

    AND THAT REHEARSAL CANNOT BE MOVED INTO CI, WHICH IS WHY THE TEXT IS ASSERTED INSTEAD.
    tests/test_spark_derivation.py is the obvious vehicle -- a local OSS Spark session, run
    by the `spark-derivation` job -- and it CANNOT host this statement. `SELECT * EXCEPT
    (...)` is Spark 4 / Databricks-SQL syntax; OSS Spark 3.5's parser rejects it outright.
    MEASURED 29 Sep 2026 on Spark 3.5.9, the version pyproject.toml's `pyspark>=3.5,<4`
    resolves to and the version CI runs:

        SELECT * EXCEPT (b) FROM t
        [PARSE_SYNTAX_ERROR] Syntax error at or near 'b'.(line 1, pos 17)

    It fails at PARSE, so nothing downstream of it is rehearsable either -- not the window,
    not the DISTINCT, not the divergence filter. Do NOT rewrite this statement into a form
    OSS Spark accepts: it is proven against the engine that actually runs it, and weakening
    it to suit a CI-only engine is the tail wagging the dog. This is the QUALIFY precedent
    exactly (checks/invoice_issue.py, checks/invoice_export.py: Databricks SQL accepts
    QUALIFY, OSS Spark 3.5's parser does not) -- with the difference that QUALIFY had a
    ranked-subquery equivalent and `* EXCEPT` over an unknown column list does not.

    Closing this properly needs an engine that speaks Databricks SQL in CI, which this repo
    has no credential for. Until then the TEXT assertions below are the whole guard, and
    they are load-bearing rather than belt-and-braces.

    THE RENDERED TEXT IS ASSERTED, not just its acceptance. Review found that replacing
    this function's whole tail with `SELECT * FROM ambiguity_counted` -- no divergence
    filter, no ORDER BY, no LIMIT -- left the suite fully green while shipping a probe
    that collects every staged row into the driver on every entity on every run
    (nhl_general_journal_line is 2.45M rows). tests/test_accelerator.py now asserts the
    LIMIT, the ORDER-BY-before-LIMIT order, the `> 1` divergence filter, a non-empty
    EXCEPT list and the backtick-quoted hash key in the PARTITION BY.
    """
    dropped = ", ".join(f"`{c}`" for c in RUN_LOCAL_COLUMNS)
    grain = ", ".join(f"`{c}`" for c in (hk,) + GRAIN_COLUMNS)
    return (
        f"WITH ambiguity_payload AS (\n"
        f"  SELECT * EXCEPT ({dropped}) FROM {q(catalog, schema, log)}\n"
        f"),\n"
        f"ambiguity_versions AS (\n"
        f"  SELECT DISTINCT * FROM ambiguity_payload\n"
        f"),\n"
        f"ambiguity_counted AS (\n"
        f"  SELECT v.*, count(*) OVER (PARTITION BY {grain}) AS {_N_VERSIONS}\n"
        f"  FROM ambiguity_versions v\n"
        f")\n"
        f"SELECT * EXCEPT ({_N_VERSIONS}) FROM ambiguity_counted\n"
        f"WHERE {_N_VERSIONS} > 1\n"
        f"ORDER BY {grain}\n"
        f"LIMIT {int(limit)}"
    )


def probe_is_needed(staged_rows: int, distinct_keys: int) -> bool:
    """Whether the ambiguity probe can possibly return anything. PURE.

    IT IS ARITHMETIC, NOT A HEURISTIC. `staged_rows` is count(*) over the log and
    `distinct_keys` is count(DISTINCT <hk>) over the same log. When they are equal, every
    hash key appears exactly ONCE, so no (hash key, load_dts, rec_src) group can hold two
    rows and the probe is provably empty. Running it anyway is a second full shuffle over
    the log to rediscover something the two numbers already settled.

    BOTH NUMBERS ARE ALREADY IN HAND. main() reads them immediately above the guard for
    the audit arithmetic, so this gate costs no query at all -- and it skips the probe on
    ten of the eleven measured hub logs. (The unskipped probe measured 2 seconds over
    2,759,330 rows on 29 Sep 2026, so this is about structure, not urgency.)

    NULLS DO NOT OPEN A HOLE: count(DISTINCT) ignores a NULL hash key while count(*)
    counts its row, so a log carrying one can never make these two equal, and the probe
    runs.
    """
    return staged_rows != distinct_keys


def _as_mapping(row) -> dict:
    """One collected row as a plain dict, whether it is a Spark Row or already a dict.

    pyspark.sql.Row has no .get(), so the pure functions below are never handed one.
    """
    return row.asDict() if hasattr(row, "asDict") else dict(row)


def grouped_by_grain(rows, hk_column: str) -> list:
    """The collected rows bucketed by (hash key, load_dts, rec_src), in arrival order.

    PURE and Spark-free. repr() rather than the values themselves, so a grain carrying an
    unhashable value buckets instead of raising.
    """
    groups: dict = {}
    for row in rows:
        mapping = _as_mapping(row)
        key = tuple(repr(mapping.get(c)) for c in (hk_column,) + GRAIN_COLUMNS)
        groups.setdefault(key, []).append(mapping)
    return list(groups.values())


def compared_columns(rows) -> tuple:
    """The columns whose disagreement makes a group ambiguous, from the rows themselves.

    PURE. The UNION of the rows' columns, so a row missing one is a difference rather
    than a column silently dropped from the comparison.
    """
    present = {c for row in rows for c in _as_mapping(row)}
    return tuple(sorted(present - set(UNCOMPARED_COLUMNS)))


def _payload(row: dict, columns: tuple) -> tuple:
    """A hashable stand-in for one row's compared payload. PURE.

    A column ABSENT from the row is not the same as one holding None -- it is omitted,
    so the two payloads differ and the group is treated as divergent. Conservative on
    purpose: this decides whether a permanent choice may be made silently.

    repr() IS NOT AN EQUALITY ORACLE, and is not claimed as one. It disagrees with `==`
    in BOTH directions -- repr(0.0) != repr(-0.0) though the values are equal, and
    repr(nan) == repr(nan) though they are not -- so it cannot be argued to fail safe on
    its own. What makes it safe here is WHERE it sits: ambiguity_probe_sql's DISTINCT is
    the gatekeeper, and Python only ever sees groups Spark has already flagged as holding
    two or more distinct payloads. So this function can only soften a flagged group into
    silence; it can never manufacture a refusal Spark did not raise.
    """
    return tuple((c, repr(row[c])) for c in columns if c in row)


def ambiguous_grain_refusal(table: str, hk_column: str, rows) -> str:
    """Why these rows cannot be reduced to one survivor, or "" if they can. PURE.

    Spark-free, so a fabricated group exercises the same decision the live loader makes --
    the shape classify_control_table() and retire_refusal() already use.

    `rows` is one group: every row sharing a (hash key, load_dts, rec_src). Two or more
    rows that agree on every compared column are EXACT DUPLICATES, which is the ordinary
    state of a staging log and what the anti-join is for -- refusing those is the
    cries-wolf failure this function exists to avoid.

    ONE TEST, NOT TWO. An explicit `len(rows) < 2` guard stood here and was deleted in
    review: it was unreachable as a decision, because a group of nought or one rows
    yields fewer than two distinct payloads and is already answered by the test below.
    A branch no single-point mutation can red is a branch nothing is checking, and this
    repo does not keep those -- the empty and single-row cases are asserted against the
    ONE test that now decides them.
    """
    rows = [_as_mapping(r) for r in rows]
    columns = compared_columns(rows)
    versions = {_payload(r, columns) for r in rows}
    if len(versions) < 2:
        return ""
    first = rows[0]
    load_dts, rec_src = GRAIN_COLUMNS
    return (
        f"{table}: {len(rows)} staged row(s) share "
        f"({hk_column}, {load_dts}, {rec_src}) = "
        f"({first.get(hk_column)!r}, {first.get(load_dts)!r}, {first.get(rec_src)!r}) "
        f"and carry {len(versions)} DIFFERENT payloads across the compared columns "
        f"{list(columns)}. row_number() ORDER BY {load_dts}, {rec_src} cannot order rows "
        f"tied on both, so the survivor would be whichever the shuffle produced -- an "
        f"ARBITRARY choice. And because the insert is NOT EXISTS, first write wins: the "
        f"next run finds the key already present and inserts nothing, so that arbitrary "
        f"choice is also PERMANENT and no re-run corrects it. Give the grain a stated "
        f"tie-break, or fix the feed -- the loader must not guess."
    )


# stable_view_sql() and stable_view_action() used to be defined here. Moved to
# accelerator.stable_views (R1, 26 September): checks/load_satellites.py carried a
# byte-identical pair (only a parameter name differed), and a third copy was about to
# be added for checks/publish_stable_views.py. One rule, three readers now -- see that
# module's docstring for the three-case rule and why the loader bootstraps and never
# moves the view. Imported above; this loader still calls stable_view_sql(...) and
# stable_view_action(...) exactly where it always did, in main() below.

# The kinds THIS loader owns: naming.KEYED_KINDS, stated outright.
#
# It was `naming.STAGED_KINDS - naming.SATELLITE_KINDS` for as long as `hub` was the only
# staged keyed kind, and the subtraction was chosen so that a staged kind added later
# "is forced to declare which loader owns it" rather than defaulting in here. THAT
# DECISION HAS NOW BEEN MADE for link, NHL and HAL -- they are staged precisely so this
# anti-join can run over them -- so the code says so instead of inheriting it from an
# arithmetic accident. The subtraction and naming.KEYED_KINDS evaluate identically today;
# only one of them is a claim.
#
# NOT naming.STAGED_KINDS: DEF-52 also put every satellite kind in that set, and a
# satellite is staged for a DIFFERENT anti-join -- compare the hashdiff to the latest
# stored version, done by checks/load_satellites.py. Handing one to this loader renders a
# keyed `row_number() = 1 per key` INSERT into sat_/msat_/csat_, which stores exactly one
# version per key and is the opposite of a satellite; and it reads stg_sat_<entity>, a
# table that never exists, because a satellite is one table PER SOURCE
# (stg_sat_x_bullhorn_eu, ...) while e.base_table names the entity. So the first satellite
# in this set fails the task and exits 1, taking the whole vault job down behind it.
HUB_LOADER_KINDS = naming.KEYED_KINDS


def staged_entities(model) -> list:
    """The entities THIS loader builds from a staging log, in table order.

    Every KEYED kind -- hub, link, NHL and HAL -- and no satellite; see HUB_LOADER_KINDS.
    Read from naming.KEYED_KINDS rather than written out here, so that a fifth keyed kind
    (`sal` is the next likely arrival) is loaded by this task the moment it is declared,
    instead of being the one keyed table that quietly writes itself.
    """
    return sorted(
        (e for e in model.entities if e.kind in HUB_LOADER_KINDS),
        key=lambda e: e.base_table,
    )


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="raw_vault")
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the load audit. Required, not defaulted: a "
                         "default would let the audit silently go somewhere else.")
    ap.add_argument("--job-run-id", required=True,
                    help="{{job.run_id}}, the key tying one run's audit rows together")
    ap.add_argument("--active-sources", default="",
                    help="the bindings that actually load in this lake (parent spec "
                         "decision D5). Needed here because a MASK is applied only for a "
                         "binding this lake loads -- an inactive keyed table is created "
                         "from its ghost flow alone and has no payload column to mask. "
                         "Empty means every declared binding is active, which is the "
                         "same default the factory reads.")
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="render the statements without a workspace")
    args = ap.parse_args()

    meta = Path(args.metadata) if args.metadata else (
        Path(__file__).resolve().parents[1] / "metadata" / "entities")
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources or None)
    entities = staged_entities(model)

    if not entities:
        # Not a quiet success: an empty HUB_LOADER_KINDS means every keyed table -- hub,
        # link, NHL and HAL alike -- is being written directly again, which is the defect
        # this file exists to close.
        print("GATE NOT EVALUATED: no staged keyed entity in the model. "
              "naming.KEYED_KINDS no longer reaches this loader, or the model declares "
              "no hub, link, NHL or HAL -- those tables would be carrying duplicate "
              "keys, which fans out every join to them.")
        return finish("NOT_EVALUATED", 0, 0, 1)

    if args.dry_run:
        for e in entities:
            # THE PHYSICAL TABLE THIS LOADER WRITES IS entity.tables()'s name, version
            # suffix included (naming.physical()) -- NOT e.base_table, which is the
            # UNVERSIONED stable name naming.stable() inverts it to. A keyed entity has
            # exactly one table (no keyed kind is in RAW_SATELLITE_KINDS, which is what
            # makes a table per source), so index [0] is safe.
            _src, hub = e.tables()[0]
            _src2, stable = e.stable_tables()[0]
            log = naming.stg(hub)
            print(f"\n-- {hub} <- {log}")
            print(create_sql(args.catalog, args.schema, hub, log, e.kind) + ";")
            # already_masked is empty here: there is no workspace to ask, so the preview
            # shows what a FRESH table needs. A live run asks Unity Catalog and skips the
            # columns that already carry a mask -- SET MASK on one of those is an error.
            _bindings = spec.active_table_bindings(e, _src, active)
            if _bindings:
                for _stmt in mask_statements(args.catalog, args.schema, hub, e,
                                             _bindings, set()):
                    print(_stmt + ";")
            elif e.masks:
                print(f"-- no active binding in this lake: the {len(e.masks)} declared "
                      f"mask(s) have no column to sit on")
            print(ambiguity_probe_sql(args.catalog, args.schema, log,
                                      e.hk_column) + ";")
            print(insert_sql(args.catalog, args.schema, hub, log,
                             e.hk_column) + ";")
            # Rendered unconditionally: there is no live workspace here to read
            # information_schema.views against, so this preview cannot show
            # stable_view_action()'s decision -- only what the statement WOULD be if
            # the view is bootstrapped or already points here. The live run below never
            # issues this statement when the view points elsewhere.
            print(stable_view_sql(args.catalog, args.schema, stable, hub) + ";")
        print(f"\ndry run only -- {len(entities)} keyed table(s), nothing executed")
        return 0

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    loaded, failed = 0, []
    for e in entities:
        # See the dry-run branch above: hub is the VERSIONED physical name this loader
        # actually creates and loads; stable is the unversioned name consumers read.
        _src, hub = e.tables()[0]
        _src2, stable = e.stable_tables()[0]
        log = naming.stg(hub)
        try:
            before = spark.sql(
                f"SELECT count(*) AS n FROM {q(args.catalog, args.schema, log)}"
            ).collect()[0]["n"]
        except Exception as exc:  # noqa: BLE001
            print(f"  SKIP {hub}: its log {log} does not exist ({exc})")
            failed.append(hub)
            continue

        try:
            spark.sql(create_sql(args.catalog, args.schema, hub, log, e.kind))

            # THE MASKS, AFTER THE CREATE AND BEFORE THE INSERT. create_sql is a CTAS and
            # a CTAS copies the SHAPE and NOT the column masks, so the table it just made
            # (or made on some earlier run) holds the declared money columns with no mask
            # on them. The same hazard checks/load_satellites.py measured on 25 September
            # -- 21 columns, reported by assert_mask_survival only AFTER the rows were in
            # -- and the same fix, IMPORTED from it rather than written again here.
            #
            # IT REACHED THIS LOADER ON 29 SEPTEMBER, when link, NHL and HAL joined `hub`
            # in naming.STAGED_KINDS. No hub declares a mask, so `hub` alone was safe; the
            # NHLs are not. Measured over the model: nhl_general_journal_line and
            # nhl_general_journal_line_closed_year (debitamt, crdtamnt via mask_money;
            # ordbtamt, orcrdamt via mask_money_double), nhl_journal_line (debit, credit),
            # nhl_payroll_detail (gross_amount, amount, taxable_amount, ytd_amount, rate)
            # and nhl_timesheet_line (rate, amount) -- 17 masked money columns across five
            # of the six NHLs. Every fresh target creates those tables here, and so does
            # every _rev<NEW> table the Task 6a runbook has an operator build.
            #
            # BEFORE THE INSERT IS THE WHOLE POINT. assert_mask_survival would catch an
            # unmasked table, but only once the money is already in it, and
            # apply_governance's grant branch sits on a PARALLEL arm of the job graph --
            # a SELECT grant can land on the unmasked table in the same run that fails.
            # The table is empty until the INSERT below, so no row is ever readable
            # through an unmasked column.
            #
            # ONLY FOR THE BINDINGS THIS LAKE LOADS. An inactive keyed table is created
            # from its ghost flow alone -- hash key plus the system columns, no payload --
            # so there is no column to mask and the factory emits no MASK clause either
            # (checks/mask_survival_check.py skips exactly the same tables, from exactly
            # this function). With --active-sources unset every declared binding counts as
            # active, and a masked column with no declared cast: then RAISES here, which
            # is the same refusal factory._mask_clauses makes over the same model: such a
            # lake cannot build its pipeline either.
            #
            # A NOTE ON THE OTHER HALF OF THIS, so the next reader does not rediscover it.
            # insert_sql reads the MASKED staging log as the JOB'S RUN-AS IDENTITY, and a
            # mask function evaluates against that identity, not the eventual reader's. An
            # identity outside scope_unmask_currency_values would therefore read NULL and
            # write NULLs permanently into an append-only target. For usnc_tds this is
            # settled: its run_as is service principal
            # 7732b208-8366-4aef-af09-60e9dec9cf86, which the platform team placed in that
            # group on 28 September. The four targets that declare no run_as at all
            # (weu_tds, uks_tds, aue_tds, dev) would load as whoever pressed Deploy, and
            # checks/preflight_target.py's second refusal already blocks them.
            #
            # THE GUARD IS AT THE CALL SITE, exactly as factory.build's is
            # (`if entity.masks and active_bindings: _mask_clauses(...)`). mask_statements
            # RAISES on a masked column with no declared cast, which is right for a
            # column that exists and wrong for one that does not -- and with no active
            # binding none of them exists. Putting the test inside the pure function
            # instead would turn "you gave me no bindings" into a silent empty list, and
            # a mask rule that can go quiet on an argument mistake is the shape this
            # whole finding is about. The skip is PRINTED for the same reason.
            _bindings = spec.active_table_bindings(e, _src, active)
            if _bindings:
                _already = masked_columns(spark, args.catalog, args.schema, hub)
                for _mask_stmt in mask_statements(args.catalog, args.schema, hub, e,
                                                  _bindings, _already):
                    spark.sql(_mask_stmt)
                    print(f"      masked {_mask_stmt.split('ALTER COLUMN ')[1][:60]}")
            elif e.masks:
                print(f"  ~    {hub:34} no active source binding in this lake, so the "
                      f"{len(e.masks)} declared mask(s) "
                      f"{[c for c, _f in e.masks]} have no column to sit on -- the table "
                      f"is created from its ghost flow alone (hash key plus the system "
                      f"columns), which is the same table "
                      f"checks/mask_survival_check.py skips")

            # AFTER create (so the table exists) and BEFORE insert: count(hub) taken after
            # the insert is the hub's running total, not what this run contributed.
            hub_before = spark.sql(
                f"SELECT count(*) AS n FROM {q(args.catalog, args.schema, hub)}"
            ).collect()[0]["n"]
            distinct_in_batch = spark.sql(
                f"SELECT count(DISTINCT `{e.hk_column}`) AS d "
                f"FROM {q(args.catalog, args.schema, log)}"
            ).collect()[0]["d"]

            # THE TIE-BREAK GUARD, BEFORE THE INSERT. See GRAIN_COLUMNS above for why
            # this refuses DIVERGENCE and not duplicate COUNTS. It has to run first:
            # NOT EXISTS makes the survivor permanent, so an audit taken afterwards would
            # be reporting a choice nothing can now undo.
            #
            # The two counts above already decide whether the probe can return anything;
            # see probe_is_needed. The SKIP IS PRINTED, because a guard that quietly did
            # not run is indistinguishable from one that ran and found nothing -- the
            # same reason the audit retry above says so in the task log.
            if probe_is_needed(before, distinct_in_batch):
                for _group in grouped_by_grain(
                        spark.sql(ambiguity_probe_sql(
                            args.catalog, args.schema, log, e.hk_column)).collect(),
                        e.hk_column):
                    _refusal = ambiguous_grain_refusal(hub, e.hk_column, _group)
                    if _refusal:
                        raise ValueError(_refusal)
            else:
                print(f"  ~    {hub:34} grain probe skipped: {before} staged row(s) over "
                      f"{distinct_in_batch} distinct key(s), so no key repeats and no "
                      f"(key, {naming.COL['load_dts']}, {naming.COL['rec_src']}) group "
                      f"can hold two rows")

            spark.sql(insert_sql(args.catalog, args.schema, hub, log, e.hk_column))

            # THE VIEW COMES AFTER THE INSERT, because it reads the table the two
            # statements above create and populate -- publishing it first would expose
            # an empty table to every reader for the length of the load.
            #
            # BUT ONLY BOOTSTRAP OR NO-OP -- NEVER A MOVE. Read the view's own current
            # definition first: if it already exists and points somewhere other than
            # `hub`, that is a deliberate cutover or a deliberate rollback, and this
            # loader must not repoint it -- see stable_view_action()'s docstring.
            _view_rows = spark.sql(
                f"SELECT view_definition FROM `{args.catalog}`.information_schema.views "
                f"WHERE table_schema = '{args.schema}' AND table_name = '{stable}'"
            ).collect()
            _view_exists = bool(_view_rows)
            _view_definition = _view_rows[0]["view_definition"] if _view_rows else ""
            _action = stable_view_action(_view_exists, _view_definition, hub)
            if _action == "elsewhere":
                print(f"  LEFT ALONE {stable}: it already points elsewhere, not at "
                      f"{hub} -- current definition: {_view_definition!r}. A loader "
                      f"bootstraps the stable view, it never moves it; run "
                      f"checks/cutover_vault_version.py to repoint it deliberately.")
            elif _action == "bootstrap":
                spark.sql(stable_view_sql(args.catalog, args.schema, stable, hub))
            # already_here: DO NOTHING. CREATE OR REPLACE VIEW replaces the securable
            # and every grant on it, so re-issuing an identical statement silently
            # revokes SELECT on the stable name -- see stable_view_sql()'s docstring.

            rows = spark.sql(
                f"SELECT count(*) AS n, count(DISTINCT `{e.hk_column}`) AS d "
                f"FROM {q(args.catalog, args.schema, hub)}"
            ).collect()[0]
            n, d = rows["n"], rows["d"]
            accepted = n - hub_before
            discards = {
                "duplicate_in_batch": before - distinct_in_batch,
                "already_present": distinct_in_batch - accepted,
            }
            problem = audit.check_arithmetic(before, accepted, discards)
            if problem:
                raise ValueError(f"{hub}: audit arithmetic does not balance -- {problem}")
            # DEF-56: A REPAIR RUN REUSES {{job.run_id}}, so a hub that already
            # succeeded is re-processed here -- the INSERT above adds nothing (NOT EXISTS),
            # but a second audit row would say it did. Guard on existence, the same idiom
            # the vault insert uses, and skip the discard rows with it: a duplicate
            # discard set makes unbalanced_tables report a gap against BOTH rows and the
            # completeness gate is then red for ever, aud_table_load being append-only.
            audited = spark.sql(audit.table_load_exists_sql(
                args.catalog, args.control_schema, job_run_id=args.job_run_id,
                table_name=hub)).collect()[0]["n"]
            if audited:
                print(f"  ~    {hub:34} already audited for run {args.job_run_id} "
                      f"({audited} row(s)) -- retry, the audit is not rewritten")
            else:
                spark.sql(audit.table_load_sql(
                    args.catalog, args.control_schema, job_run_id=args.job_run_id,
                    pipeline_update_id=None, table_name=hub,
                    written_by="checks/load_hubs.py",
                    staged=before, accepted=accepted))
                for reason, count in sorted(discards.items()):
                    if count:
                        spark.sql(audit.table_discard_sql(
                            args.catalog, args.control_schema,
                            job_run_id=args.job_run_id, table_name=hub,
                            reason=reason, discarded=count))

            state = "ok  " if n == d else "DUPE"
            print(f"  {state} {hub:34} log={before:>9}  hub={n:>9} rows / {d:>9} keys "
                  f"| +{accepted} accepted, {sum(discards.values())} discarded")
            if n != d:
                failed.append(f"{hub}: {n - d} duplicate key(s) AFTER the anti-join")
            loaded += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {hub}: {exc}")
            failed.append(hub)

    print(f"\n{loaded} keyed table(s) loaded")
    if failed:
        print(f"\nKEYED LOAD FAILED -- {len(failed)} problem(s):")
        for f in failed:
            print(f"  * {f}")
        return finish("FAILED", loaded, 0, 1)

    print(f"\nKEYED LOAD PASSED: {loaded} table(s), each exactly one row per key")
    return finish("PASSED", loaded, 0, 0)


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for code 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
