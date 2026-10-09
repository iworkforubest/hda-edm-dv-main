"""The Gold load-quality dashboard's definition: what it asks, and what it refuses to claim.

NOTHING DEPLOYS THIS, AND THAT IS DELIBERATE. Measured 29 August 2026:
`03_usnc_gold_edm_dev` does not exist -- `databricks schemas list` answers "Catalog
'03_usnc_gold_edm_dev' does not exist." No job task produces gold; `--gold-catalog` reaches
only apply_governance and schema_grant_check, and only to GRANT on it.
`governance/control_objects_gold.sql` is generated, committed, gated against drift, and never
applied.

So this module follows that file exactly: the artefact is emitted and kept un-driftable, and
NO bundle resource declares it. Declaring one today would either fail the deploy on a missing
catalog or publish a dashboard whose every tile fails with TABLE_OR_VIEW_NOT_FOUND -- which is
not hypothetical, it is what the silver dashboard actually did on 27 August when its names did
not resolve. verify_repo.py fails the build if a declaration appears while the layer is still
undeployed.

WHAT IT MEASURES, AND WHY THAT IS ALL. Gold declares no tables of its own --
control_standard.LAYER_TABLES["gold"] is {} -- so the three CORE audit tables are the whole
surface. That leaves load volume, acceptance, discards and unfinished runs, and it leaves
them meaning something specific: STAGED_MEANING["gold"] declares staged = rows read from the
vault, accepted = rows published to the projection.

WHAT IT DELIBERATELY OMITS.
  * NO COVERAGE TILE. Coverage is the silver dashboard's headline because ref_dq_expectation
    exists there to be empty. Gold declares no expectations at all, so a coverage tile would
    have nothing to count and would read 0% -- describing a rule set nobody has written as
    though it were a rule set nobody follows.
  * NO SUPERSEDE TILE. A projection layer has no rejects to supersede. There is no
    ctl_quarantine_superseded in gold and there should not be one.
Both absences are the honest statement about a projection layer, not gaps waiting to be
filled with a lookalike of silver's tiles.

WHY THE HONESTY TILE LEADS. Gold will hold zero rows for as long as nothing produces it, and
zero staged over zero accepted renders as a clean, empty, entirely healthy-looking dashboard.
The runs-recorded counter is what stops that reading, and on this dashboard it is not a
precaution against a future emptiness -- it is describing today.
"""

from __future__ import annotations

import re

# The tables this dashboard may read: gold's control surface, which is the mandatory core and
# nothing else. Named here so a dataset cannot quietly query something outside it, and derived
# from the standard rather than restated -- a fourth copy of the core's names is a fourth thing
# to keep in step.
from .control_standard import tables_for as _tables_for

CONTROL_TABLES: tuple[str, ...] = tuple(sorted(_tables_for("gold")))

# EVERY RATE MUST CARRY THIS. A bare `a / b` renders a rate over a zero denominator as either
# NULL or a division error, and a NULL formatted as a percentage reads as "no problem" to a
# business user. The same string as quality.RATE_GUARD and bronze_quality.RATE_GUARD; the
# checks assert they are one wording rather than three lookalikes.
RATE_GUARD = "CASE WHEN denominator = 0 THEN NULL ELSE"

# A counter tile may read ONLY a dataset named here. A counter over a multi-row dataset renders
# an arbitrary row while its title implies an aggregate.
SINGLE_ROW_DATASETS: frozenset[str] = frozenset({"ds_runs_recorded"})


def _t(prefix: str, table: str, gold_catalog: str = "", control_schema: str = "") -> str:
    """A gold control table reference, qualified with the GOLD catalog when one is supplied.

    THE CATALOG IS GOLD'S, NOT SILVER'S, and that is the one thing this family can get wrong
    in a way nothing else would notice. Every other dashboard in this repo qualifies with
    ${var.catalog}; this one must use ${var.gold_catalog}, or it would silently report the
    VAULT's audit under a page titled Gold -- numbers that are real, that render without
    error, and that describe the wrong layer. A check asserts the emitted text names the gold
    catalog and not the silver one.
    """
    name = f"{prefix}{table}"
    return f"{gold_catalog}.{control_schema}.{name}" if gold_catalog and control_schema else name


def datasets(table_prefix: str = "",
             gold_catalog: str = "",
             control_schema: str = "") -> list[dict]:
    """The dashboard's datasets, over gold's three core audit tables."""
    p = table_prefix
    cat, cs = gold_catalog, control_schema
    load = _t(p, "aud_table_load", cat, cs)
    discard = _t(p, "aud_table_discard", cat, cs)
    run = _t(p, "aud_load_run", cat, cs)
    return [
        {
            "name": "ds_runs_recorded",
            "displayName": "Runs recorded",
            # THE HEADLINE, AND ON THIS DASHBOARD IT IS THE WHOLE STORY TODAY. Nothing
            # produces gold, so every tile below renders empty. Read as "0 runs recorded"
            # that is correct and informative; read as four blank tables it looks like a
            # layer that ran cleanly.
            "queryLines": [
                "SELECT",
                "  count(DISTINCT job_run_id) AS runs_recorded,",
                "  count(*) AS load_rows_recorded,",
                "  max(recorded_at) AS last_recorded_at",
                f"FROM {load}",
            ],
        },
        {
            "name": "ds_load_volume",
            "displayName": "Rows read from the vault, and published",
            # THE COLUMN NAMES ARE THE AUDIT'S, THE MEANING IS GOLD'S. staged and accepted
            # are the same columns silver writes, and they do NOT mean the same thing here:
            # control_standard.STAGED_MEANING declares staged = rows read from the vault and
            # accepted = rows published to the projection. The tile titles say so, because a
            # reader comparing this page with the silver one would otherwise subtract one
            # from the other and report a loss that is a projection working correctly.
            "queryLines": [
                "SELECT",
                "  job_run_id, table_name, written_by,",
                "  staged, accepted,",
                "  staged - accepted AS not_published,",
                "  recorded_at",
                f"FROM {load}",
                "ORDER BY recorded_at DESC",
            ],
        },
        {
            "name": "ds_publish_rate",
            "displayName": "Published rate, or not evaluated",
            "queryLines": [
                "WITH totals AS (",
                "  SELECT table_name,",
                "         sum(staged) AS denominator,",
                "         sum(accepted) AS accepted",
                f"  FROM {load}",
                "  GROUP BY table_name",
                ")",
                "SELECT table_name, denominator, accepted,",
                # RATE_GUARD, and on a layer with no producer it is the difference between an
                # empty page and a page claiming a division error is a quality signal.
                f"  {RATE_GUARD} accepted / denominator END AS publish_rate,",
                "  CASE WHEN denominator = 0",
                "       THEN 'not evaluated' ELSE 'evaluated' END AS state",
                "FROM totals",
                "ORDER BY table_name",
            ],
        },
        {
            "name": "ds_discard_reason",
            "displayName": "Rows read but not published, by reason",
            "queryLines": [
                "SELECT",
                "  table_name, discard_reason,",
                "  sum(discarded) AS discarded,",
                "  count(DISTINCT job_run_id) AS runs",
                f"FROM {discard}",
                "GROUP BY table_name, discard_reason",
                "ORDER BY discarded DESC",
            ],
        },
        {
            "name": "ds_incomplete_runs",
            "displayName": "Runs that never completed",
            "queryLines": [
                "SELECT job_run_id, target,",
                "       max(CASE WHEN phase = 'opened' THEN recorded_at END) AS opened_at",
                f"FROM {run}",
                "GROUP BY job_run_id, target",
                "HAVING sum(CASE WHEN phase = 'completed' THEN 1 ELSE 0 END) = 0",
                "ORDER BY opened_at DESC",
            ],
        },
    ]


def tiles() -> list[dict]:
    """The tiles, in reading order. Runs-recorded first: on a layer nothing produces, it is
    the only tile that distinguishes 'clean' from 'absent'."""
    return [
        {"name": "w_runs_recorded", "widgetType": "counter",
         "dataset": "ds_runs_recorded", "fields": ["runs_recorded"],
         "title": "Runs recorded (0 means gold has never been loaded)",
         "position": {"x": 0, "y": 0, "width": 3, "height": 3}},
        {"name": "w_publish_rate", "widgetType": "table",
         "dataset": "ds_publish_rate",
         "fields": ["table_name", "denominator", "accepted", "publish_rate", "state"],
         "title": "Vault rows published to the projection, or not evaluated",
         "position": {"x": 0, "y": 3, "width": 6, "height": 6}},
        {"name": "w_load_volume", "widgetType": "table",
         "dataset": "ds_load_volume",
         "fields": ["job_run_id", "table_name", "staged", "accepted", "not_published",
                    "recorded_at"],
         "title": "Rows read from the vault (staged) and published (accepted)",
         "position": {"x": 0, "y": 9, "width": 6, "height": 6}},
        {"name": "w_discard_reason", "widgetType": "table",
         "dataset": "ds_discard_reason",
         "fields": ["table_name", "discard_reason", "discarded", "runs"],
         "title": "Read but not published, by reason",
         "position": {"x": 0, "y": 15, "width": 6, "height": 6}},
        {"name": "w_incomplete_runs", "widgetType": "table",
         "dataset": "ds_incomplete_runs",
         "fields": ["job_run_id", "target", "opened_at"],
         "title": "Runs that never completed",
         "position": {"x": 0, "y": 21, "width": 6, "height": 5}},
    ]


def unguarded_rate_findings(datasets_list: list[dict]) -> list[str]:
    """Dataset names computing a rate without RATE_GUARD.

    Same shape as bronze_quality.unguarded_rate_findings: a pure function over the real
    datasets, so the check reads the definition rather than a copy of it.
    """
    findings = []
    for ds in datasets_list:
        sql = " ".join(ds["queryLines"])
        if re.search(r"/\s*denominator", sql) and RATE_GUARD not in sql:
            findings.append(ds["name"])
    return findings


def cross_layer_subtraction_findings(datasets_list: list[dict]) -> list[str]:
    """Dataset names subtracting one LAYER's count from another's.

    THE TILE A STAKEHOLDER ASKS FOR BY NAME, and the one this family is most exposed to. Gold
    reads from the vault, so "vault rows minus gold rows, labelled rows lost" is the obvious
    next tile and it is wrong: a projection selects, aggregates and filters, so a drop between
    the two is the projection working. staged - accepted WITHIN one audit row is a different
    thing and stays allowed -- that is one layer's own arithmetic, which is exactly what the
    audit exists to record.
    """
    findings = []
    for ds in datasets_list:
        sql = " ".join(ds["queryLines"])
        # A reference to a non-gold control table alongside a subtraction is the shape that
        # matters; within-row `staged - accepted` names no second table.
        if "-" in sql and re.search(r"\b\d+_\w*(silver|bronze)\w*\b", sql):
            findings.append(ds["name"])
    return findings
