"""The Bronze conformance dashboard's definition: what it asks, and what it refuses to claim.

ONE DEFINITION, TWO RENDERINGS, exactly as quality.py does it. datasets() takes a table
prefix so the synthetic dashboard is the same tiles against tst_ tables. A second
hand-written definition would let the two drift, and then exercising the synthetic
dashboard would be evidence about a lookalike rather than about the real one.

WHAT THIS DASHBOARD IS ABOUT. checks/source_conformance_check.py asks whether Bronze
delivers what Silver's contracts require -- columns present, values castable -- and writes
one row per contracted table per run into ctl_source_conformance. This reads that record.
It does NOT recompute conformance: a second derivation of the same judgement would be free
to disagree with the gate, which is the duplicate-authority shape this repo has been bitten
by three times (BUSINESS_KINDS, the system-column set, RECONCILABLE_KINDS).

WHY COVERAGE IS THE HEADLINE, and not a conformance rate. Measured 29 Aug: the gate passes,
and source contracts exist for two of nine targets. A "100% conformant" tile would be true,
thin, and indistinguishable from an estate nobody measures. How much of Bronze is under
contract at all is the honest statement, and RUNS_RECORDED sits beside it so an empty table
reads as empty rather than as perfect.

WHY THE LATEST RUN IS A WINDOW FUNCTION AND NOT max(recorded_at). Two targets write into one
table, and their runs interleave. A global max would show whichever target ran last and
silently drop the other, so "latest" is per (target, contract_table).
"""

from __future__ import annotations

import re

# The one table this dashboard reads. Named here so a dataset cannot quietly query something
# outside it; the test asserts the containment, as quality.CONTROL_TABLES does for silver.
CONFORMANCE_TABLE: str = "ctl_source_conformance"

# EVERY RATE MUST CARRY THIS. A bare `a / b` renders a rate over a zero denominator as either
# NULL or a division error, and a NULL formatted as a percentage reads as "no problem" to a
# business user. Same guard, same wording, as quality.RATE_GUARD -- the test asserts they are
# the same string rather than two lookalikes.
RATE_GUARD = "CASE WHEN denominator = 0 THEN NULL ELSE"

# A counter tile may read ONLY a dataset named here. A counter over a multi-row dataset
# renders an arbitrary row while its title implies an aggregate -- confidently wrong, which is
# the failure class this dashboard exists to avoid.
SINGLE_ROW_DATASETS: frozenset[str] = frozenset(
    {"ds_conformance_summary", "ds_runs_recorded"}
)


def _t(prefix: str, table: str, catalog: str = "", control_schema: str = "") -> str:
    """The conformance table reference: prefixed for the synthetic rendering, and fully
    qualified when a catalog is supplied.

    QUALIFICATION IS NOT OPTIONAL FOR A DEPLOYED ARTEFACT. Measured 27 Aug against the
    DEPLOYED silver dashboard: dataset_catalog / dataset_schema do not reach the datasets,
    which stay bare, and the warehouse resolves them against hive_metastore.default -- every
    tile failed with TABLE_OR_VIEW_NOT_FOUND. See quality._t for the full account; this
    inherits the conclusion rather than rediscovering it.
    """
    name = f"{prefix}{table}"
    return f"{catalog}.{control_schema}.{name}" if catalog and control_schema else name


def datasets(table_prefix: str = "",
             catalog: str = "",
             control_schema: str = "") -> list[dict]:
    """The dashboard's datasets.

    LATEST IS PER (target, contract_table). See the module docstring: a global max over
    recorded_at would show whichever target ran last and drop the other entirely.
    """
    p = table_prefix
    cat, cs = catalog, control_schema
    src = _t(p, CONFORMANCE_TABLE, cat, cs)
    # The latest row per contracted table, reused by three datasets. Written once here rather
    # than three times, because three copies of a window function drift.
    latest = [
        "WITH ranked AS (",
        "  SELECT *, row_number() OVER (",
        "    PARTITION BY target, contract_table ORDER BY recorded_at DESC",
        "  ) AS rn",
        f"  FROM {src}",
        "),",
        "latest AS (SELECT * FROM ranked WHERE rn = 1)",
    ]
    return [
        {
            "name": "ds_conformance_summary",
            "displayName": "Conformance headline",
            # THE HEADLINE, AND IT MUST BE ONE ROW. A counter over a per-table dataset renders
            # whichever row arrives first, so a counter reading ds_conformance_by_table would
            # show one arbitrary table's status under a title claiming it counted tables.
            # SINGLE_ROW_DATASETS plus the counter check make that unrepresentable.
            #
            # tables_not_conformant COUNTS NOT_EVALUATED TOO. A table whose cast probes were
            # skipped has not been shown to conform, and folding it into the conformant count
            # would report safety nobody measured.
            "queryLines": latest + [
                "SELECT",
                "  count(*) AS tables_measured,",
                "  sum(CASE WHEN status = 'CONFORMANT' THEN 1 ELSE 0 END) AS tables_conformant,",
                "  sum(CASE WHEN status <> 'CONFORMANT' THEN 1 ELSE 0 END)"
                " AS tables_not_conformant,",
                "  count(DISTINCT target) AS targets_under_contract",
                "FROM latest",
            ],
        },
        {
            "name": "ds_runs_recorded",
            "displayName": "Runs recorded",
            # THE HONESTY TILE. Without it an empty ctl_source_conformance renders every other
            # tile as a clean zero, which is indistinguishable from an estate in perfect
            # health. This is the dashboard-scale answer to DEF-48, and it is the reason this
            # dashboard can be deployed while the gate happens to be passing.
            "queryLines": [
                "SELECT",
                "  count(DISTINCT job_run_id) AS runs_recorded,",
                "  count(*) AS rows_recorded,",
                "  max(recorded_at) AS last_recorded_at",
                f"FROM {src}",
            ],
        },
        {
            "name": "ds_conformance_by_table",
            "displayName": "Conformance by contracted table, latest run",
            "queryLines": latest + [
                "SELECT",
                "  target,",
                "  contract_table,",
                "  status,",
                "  missing_columns,",
                "  lossy_casts,",
                "  recorded_at",
                "FROM latest",
                # NOT-CONFORMANT FIRST. The reader is here to find what is wrong, and a table
                # sorted by name buries three findings under forty clean rows.
                "ORDER BY CASE WHEN status = 'CONFORMANT' THEN 1 ELSE 0 END, contract_table",
            ],
        },
        {
            "name": "ds_findings",
            "displayName": "Findings, in the gate's own words",
            # EXPLODED, NOT PARAPHRASED. The array holds the exact "KIND: detail" lines
            # checks/source_conformance_check.py printed, so what a reader sees here and what
            # an operator reads in the job log are the same sentence. A summary rewritten here
            # would be a second description of the same fact, free to drift from it.
            "queryLines": latest + [
                "SELECT",
                "  target,",
                "  contract_table,",
                "  finding",
                "FROM latest",
                "LATERAL VIEW explode(findings) f AS finding",
                "ORDER BY contract_table, finding",
            ],
        },
        {
            "name": "ds_conformance_trend",
            "displayName": "Conformance by run",
            # WHY A TREND AT ALL. The Bronze asks (BRZ-1, BRZ-8, BRZ-12) land one at a time,
            # and the question after each is "did that move conformance". A single latest-run
            # view cannot answer it; this can, without anybody re-running an old gate.
            "queryLines": [
                "SELECT",
                "  job_run_id,",
                "  target,",
                "  min(recorded_at) AS recorded_at,",
                "  count(*) AS denominator,",
                "  sum(CASE WHEN status = 'CONFORMANT' THEN 1 ELSE 0 END) AS conformant,",
                # RATE_GUARD, and it is load-bearing here: a run that recorded no rows at all
                # would otherwise render a NULL rate that a reader takes for 'fine'.
                f"  {RATE_GUARD} round(100.0 *",
                "    sum(CASE WHEN status = 'CONFORMANT' THEN 1 ELSE 0 END) / count(*), 1)"
                " END AS conformant_rate",
                f"FROM {src}",
                "GROUP BY job_run_id, target",
                "ORDER BY recorded_at DESC",
            ],
        },
    ]


def tiles() -> list[dict]:
    """The tiles, in reading order. The headline and the honesty tile share the top row:
    'how much is wrong' is only meaningful beside 'how much was measured at all'."""
    return [
        {"name": "w_not_conformant", "widgetType": "counter",
         "dataset": "ds_conformance_summary", "fields": ["tables_not_conformant"],
         "title": "Contracted tables NOT conformant",
         "position": {"x": 0, "y": 0, "width": 2, "height": 3}},
        {"name": "w_measured", "widgetType": "counter",
         "dataset": "ds_conformance_summary", "fields": ["tables_measured"],
         "title": "Contracted tables measured",
         "position": {"x": 2, "y": 0, "width": 2, "height": 3}},
        {"name": "w_runs_recorded", "widgetType": "counter",
         "dataset": "ds_runs_recorded", "fields": ["runs_recorded"],
         "title": "Runs recorded (0 means NOTHING is measured)",
         "position": {"x": 4, "y": 0, "width": 2, "height": 3}},
        {"name": "w_by_table", "widgetType": "table",
         "dataset": "ds_conformance_by_table",
         "fields": ["target", "contract_table", "status", "missing_columns", "lossy_casts",
                    "recorded_at"],
         "title": "Conformance by contracted table, latest run",
         "position": {"x": 0, "y": 3, "width": 6, "height": 7}},
        {"name": "w_findings", "widgetType": "table",
         "dataset": "ds_findings",
         "fields": ["target", "contract_table", "finding"],
         "title": "Findings, in the gate's own words",
         "position": {"x": 0, "y": 10, "width": 6, "height": 7}},
        {"name": "w_trend", "widgetType": "table",
         "dataset": "ds_conformance_trend",
         "fields": ["job_run_id", "target", "recorded_at", "denominator", "conformant",
                    "conformant_rate"],
         "title": "Conformance by run",
         "position": {"x": 0, "y": 17, "width": 6, "height": 6}},
    ]


def unguarded_rate_findings(datasets_list: list[dict]) -> list[str]:
    """Dataset names computing a rate without RATE_GUARD.

    Mirrors quality.cross_layer_subtraction_findings' shape: a pure function over the
    datasets, so the check reads the real definition rather than a copy of it. A rate over a
    zero denominator renders NULL, and a NULL formatted as a percentage reads as 'no problem'
    -- the guard forces the not-evaluated branch to be explicit.
    """
    findings = []
    for ds in datasets_list:
        sql = " ".join(ds["queryLines"])
        # A division whose left side is not already inside the guard.
        if re.search(r"/\s*count\(\*\)", sql) and RATE_GUARD not in sql:
            findings.append(ds["name"])
    return findings
