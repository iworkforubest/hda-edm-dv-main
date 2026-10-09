"""The quality dashboard's definition: what it asks, and what it refuses to claim.

ONE DEFINITION, TWO RENDERINGS. datasets() takes a table prefix so the synthetic
dashboard is the same tiles against tst_ tables. A second hand-written definition would
let the two drift, and then exercising the synthetic dashboard would be evidence about a
lookalike rather than about the real one.

WHY COVERAGE IS THE HEADLINE. ref_dq_expectation holds no rules, so every quality
statement but key safety is vacuous. A pass-rate tile would read 100% over an empty
denominator and be indistinguishable from a genuinely clean pipeline. Coverage says how
much is measured at all, which is the true and useful statement today.

WHY COVERAGE EXPLODES generated_tables RATHER THAN READING table_name DIRECTLY.
checks/publish_metadata.py writes meta_vault_model.table_name as e.base_table -- one row
per ENTITY (21 rows) -- and puts the real physical target table names in the separate
array column generated_tables (one entity can produce several tables: a multi-source
satellite has one physical table per source binding). base_table carries no source
suffix, while ref_dq_expectation.dataset stores the real target table, e.g.
sat_job_request_details_bullhorn_eu. Joining ref_dq_expectation against table_name
directly can therefore never match for a multi-source satellite -- every such table would
read "not evaluated" forever regardless of what rules exist, and the total would silently
undercount (21 entities where the model actually produces 25 tables). Exploding
generated_tables first gives the coverage datasets the real per-table grain.
"""

from __future__ import annotations

import re

# WHAT THE SILVER DASHBOARD MAY READ. Named here so a dataset cannot quietly query something
# outside the control schema; the test asserts the containment.
#
# NOT "every table control_objects.sql creates", which is what this said until
# ctl_source_conformance was added on 29 Aug and made that description false. That table is
# the Bronze conformance record, read by bronze_quality.py and deliberately NOT by this
# dashboard -- a silver load-quality tile reading it would be reporting a different subject
# under the same title. The list is a permission, not an inventory.
CONTROL_TABLES: tuple[str, ...] = (
    "ctl_approval_manifest",
    "ref_dq_expectation",
    "aud_table_load",
    "aud_table_discard",
    "aud_load_run",
    "ctl_quarantine_superseded",
)

# EVERY RATE MUST CARRY THIS. A bare `a / b` renders a rate over a zero denominator as
# either NULL or a division error, and a NULL formatted as a percentage reads as "no
# problem" to a business user. The guard forces the not-evaluated branch to be explicit.
RATE_GUARD = "CASE WHEN denominator = 0 THEN NULL ELSE"

# A counter tile may read ONLY a dataset named here. A counter over a multi-row dataset
# renders an arbitrary row while its title implies an aggregate -- confidently wrong, which
# is the failure class this dashboard exists to avoid.
SINGLE_ROW_DATASETS: frozenset[str] = frozenset({"ds_coverage_summary"})


def _t(prefix: str, table: str, catalog: str = "", control_schema: str = "") -> str:
    """A control table reference: prefixed for the synthetic rendering, and fully qualified
    when a catalog is supplied.

    QUALIFICATION IS NOT OPTIONAL FOR A DEPLOYED ARTEFACT. An earlier version left these
    bare and relied on the bundle's dataset_catalog / dataset_schema to bind them at deploy
    time. Measured 27 Aug against the DEPLOYED dashboard: those fields do not reach it, the
    datasets stay bare, and the warehouse resolves them against its own default --
    hive_metastore.default -- so EVERY tile failed with TABLE_OR_VIEW_NOT_FOUND. The check
    that was meant to cover this asserted the resource YAML CONTAINED
    `dataset_catalog: ${var.catalog}`, which proved the file said it, not that it did
    anything.
    """
    name = f"{prefix}{table}"
    return f"{catalog}.{control_schema}.{name}" if catalog and control_schema else name


def _gov(catalog: str, governance_schema: str) -> str:
    """The governance schema reference, qualified when a catalog is supplied."""
    return f"{catalog}.{governance_schema}" if catalog else governance_schema


def datasets(table_prefix: str = "",
             governance_schema: str = "governance",
             catalog: str = "",
             control_schema: str = "") -> list[dict]:
    """The dashboard's datasets.

    CONTROL TABLES ARE UNQUALIFIED: the bundle's dataset_catalog and dataset_schema set the
    catalog and schema at deploy time, which is what lets ONE artefact serve all nine
    targets.

    meta_vault_model IS QUALIFIED, and must be. It is written by checks/publish_metadata.py
    into the GOVERNANCE schema, while dataset_schema pins the default to CONTROL. A bare
    reference would resolve to control.meta_vault_model, which does not exist, and the
    coverage tile -- the headline -- would fail at view time in front of a user. A two-part
    name still resolves inside dataset_catalog, so this stays parameterised per target.

    THE COVERAGE DATASETS EXPLODE generated_tables, NOT table_name. table_name is
    base_table -- one row per entity -- while generated_tables carries the real physical
    target table names, several per entity for a multi-source satellite. Joining on
    table_name directly would never match ref_dq_expectation.dataset for those tables. See
    the module docstring for the full failure mode this avoids.
    """
    p = table_prefix
    g = governance_schema
    cat, cs = catalog, control_schema
    return [
        {
            "name": "ds_coverage_summary",
            "displayName": "Coverage headline",
            # THE HEADLINE, AND IT MUST BE ONE ROW. A counter over a per-table dataset
            # renders whichever row arrives first, so a counter reading ds_coverage would
            # show one arbitrary table's rule count under a title claiming it counted
            # tables. SINGLE_ROW_DATASETS plus the counter check below make that
            # unrepresentable.
            "queryLines": [
                "WITH target_table AS (",
                "  SELECT explode(generated_tables) AS table_name",
                f"  FROM {_gov(cat, g)}.meta_vault_model",
                "),",
                "per_table AS (",
                "  SELECT t.table_name, count(e.rule_name) AS rules_declared",
                "  FROM target_table t",
                f"  LEFT JOIN {_t(p, 'ref_dq_expectation', cat, cs)} e",
                "    ON e.dataset = t.table_name AND e.is_current",
                "  GROUP BY t.table_name",
                ")",
                "SELECT",
                "  count(*) AS tables_total,",
                # tables_with_a_rule, not tables_covered -- see coverage_state below for
                # why 'covered' is the wrong word: a declared rule validates rows that
                # ARRIVE, never rows already landed, and the vault cannot be reprocessed.
                "  sum(CASE WHEN rules_declared > 0 THEN 1 ELSE 0 END) AS tables_with_a_rule,",
                "  sum(CASE WHEN rules_declared = 0 THEN 1 ELSE 0 END)"
                " AS tables_with_no_rule",
                "FROM per_table",
            ],
        },
        {
            "name": "ds_coverage",
            "displayName": "Expectation coverage by target table",
            # THE DENOMINATOR IS TARGET TABLES, NOT ENTITIES. 21 entities resolve to 25
            # physical tables because a satellite has one table per source binding.
            # meta_vault_model.table_name is base_table -- one row per entity, with no
            # source suffix -- so joining on it directly would never match
            # ref_dq_expectation.dataset for a multi-source satellite. generated_tables
            # carries the real per-table names; exploding it first is what gives this
            # dataset the true 25-table grain instead of silently undercounting at 21.
            "queryLines": [
                "WITH target_table AS (",
                "  SELECT explode(generated_tables) AS table_name",
                f"  FROM {_gov(cat, g)}.meta_vault_model",
                ")",
                "SELECT",
                "  t.table_name,",
                "  count(e.rule_name) AS rules_declared,",
                "  CASE WHEN count(e.rule_name) = 0",
                # 'rule declared', NOT 'covered'. MEASURED 27 Aug: expectations are
                # evaluated as rows ARRIVE, so declaring a rule does not validate the rows
                # already in the table -- and it cannot, because the vault is
                # delta.appendOnly and refuses the refresh that reprocessing would need.
                # When these four rules were declared, the tables they name already held
                # rows carrying the very violations the rules describe: 8 negative GL
                # amounts and 5 negative bullhorn openings, measured in Bronze, already
                # landed. 'covered' would have read as 'checked' and been wrong for every
                # row that predates its rule.
                "       THEN 'no rule' ELSE 'rule declared' END AS coverage_state",
                "FROM target_table t",
                f"LEFT JOIN {_t(p, 'ref_dq_expectation', cat, cs)} e",
                "  ON e.dataset = t.table_name AND e.is_current",
                "GROUP BY t.table_name",
                "ORDER BY rules_declared, t.table_name",
            ],
        },
        {
            "name": "ds_load_volume",
            "displayName": "Rows staged and accepted per table per run",
            "queryLines": [
                "SELECT",
                "  job_run_id, table_name, written_by,",
                "  staged, accepted,",
                "  staged - accepted AS not_accepted,",
                "  recorded_at",
                f"FROM {_t(p, 'aud_table_load', cat, cs)}",
                "ORDER BY recorded_at DESC",
            ],
        },
        {
            "name": "ds_discard_reason",
            "displayName": "Discards by reason",
            "queryLines": [
                "SELECT",
                "  table_name, discard_reason,",
                "  sum(discarded) AS discarded,",
                "  count(DISTINCT job_run_id) AS runs",
                f"FROM {_t(p, 'aud_table_discard', cat, cs)}",
                "GROUP BY table_name, discard_reason",
                "ORDER BY discarded DESC",
            ],
        },
        {
            "name": "ds_accept_rate",
            "displayName": "Acceptance rate, or not evaluated",
            # The rate that must never lie. denominator is named so RATE_GUARD reads
            # literally, and the test asserts the guard is present wherever '/' appears.
            "queryLines": [
                "WITH totals AS (",
                "  SELECT table_name,",
                "         sum(staged) AS denominator,",
                "         sum(accepted) AS accepted",
                f"  FROM {_t(p, 'aud_table_load', cat, cs)}",
                "  GROUP BY table_name",
                ")",
                "SELECT table_name, denominator, accepted,",
                f"  {RATE_GUARD} accepted / denominator END AS accept_rate,",
                "  CASE WHEN denominator = 0",
                "       THEN 'not evaluated' ELSE 'evaluated' END AS state",
                "FROM totals",
                "ORDER BY table_name",
            ],
        },
        {
            "name": "ds_incomplete_runs",
            "displayName": "Runs that opened and never completed",
            "queryLines": [
                "SELECT job_run_id, target,",
                "       max(CASE WHEN phase = 'opened' THEN recorded_at END) AS opened_at",
                f"FROM {_t(p, 'aud_load_run', cat, cs)}",
                "GROUP BY job_run_id, target",
                "HAVING sum(CASE WHEN phase = 'completed' THEN 1 ELSE 0 END) = 0",
                "ORDER BY opened_at DESC",
            ],
        },
        {
            "name": "ds_supersede",
            "displayName": "Rejects later accepted",
            "queryLines": [
                "SELECT manifest_id, table_name, superseded_by, reason, recorded_at",
                f"FROM {_t(p, 'ctl_quarantine_superseded', cat, cs)}",
                "ORDER BY recorded_at DESC",
            ],
        },
        {
            "name": "ds_manifest",
            "displayName": "Approval manifests",
            "queryLines": [
                "SELECT manifest_id, source_system, approved_count, approved_at, approved_by",
                f"FROM {_t(p, 'ctl_approval_manifest', cat, cs)}",
                "ORDER BY approved_at DESC",
            ],
        },
    ]


# THE TILE SPEC SECTION 5 FORBIDS BY NAME: bronze's `accepted` minus silver's `accepted`,
# labelled "rows lost". A drop between layers is EXPECTED and correct -- hubs deduplicate
# (4,444,172 GP rows become 2,221,108 hub rows) and satellites store only changed rows -- so
# a cross-layer subtraction reports a fabricated catastrophe, not a real one. Section 8
# calls this "the only gate standing between this design and the tile section 5 forbids".
#
# WHAT COUNTS AS CROSS-LAYER: two FROM/JOIN references, in the SAME query, to the SAME bare
# control table name (e.g. aud_table_load) but qualified with two DIFFERENT prefixes -- in
# production, two different catalogs, one per layer -- aliased, with a subtraction between
# the aliases' columns anywhere in the query text.
#
# WHAT DOES NOT COUNT: `staged - accepted` in ds_load_volume is a difference between two
# DIFFERENT columns of the SAME row of the SAME table reference -- legitimate WITHIN-layer
# arithmetic per section 5 (it is exactly the per-layer invariant section 3 states), and
# this scan never sees it, because there is only one qualified table reference in that
# query, not two differently-qualified ones.
_CROSS_LAYER_ALIAS_RE = re.compile(r"(?:FROM|JOIN)\s+([`\w.${}]+)\s+(?:AS\s+)?(\w+)\b", re.I)
_CROSS_LAYER_SUBTRACT_RE = re.compile(r"(\w+)\.(\w+)\s*-\s*(\w+)\.(\w+)")


def cross_layer_subtraction_findings(datasets_list: list[dict]) -> list[str]:
    """Datasets whose SQL subtracts one alias's column from a DIFFERENTLY-QUALIFIED alias
    of the SAME control table -- the forbidden cross-layer "rows lost" tile, in whatever
    form it takes. Pure and Spark-free: it reads the query text datasets() already produced,
    the same offline-decision shape as control_conformance_check.py's conformance().
    """
    findings = []
    for dataset in datasets_list:
        sql = "\n".join(dataset["queryLines"])
        aliases: dict[str, str] = {}
        for match in _CROSS_LAYER_ALIAS_RE.finditer(sql):
            table_ref, alias = match.group(1), match.group(2)
            aliases[alias] = table_ref.strip("`")
        for match in _CROSS_LAYER_SUBTRACT_RE.finditer(sql):
            a_alias, a_col, b_alias, b_col = match.groups()
            a_ref, b_ref = aliases.get(a_alias), aliases.get(b_alias)
            if not a_ref or not b_ref or a_ref == b_ref:
                continue
            a_bare, b_bare = a_ref.rsplit(".", 1)[-1], b_ref.rsplit(".", 1)[-1]
            if a_bare == b_bare:
                findings.append(
                    f"{dataset['name']}: {a_ref}.{a_col} - {b_ref}.{b_col} subtracts one "
                    f"layer's {a_bare} from another's -- forbidden by spec section 5"
                )
    return findings


def tiles() -> list[dict]:
    """The tiles, in reading order. Coverage first: it is the headline."""
    return [
        {"name": "w_coverage_counter", "widgetType": "counter",
         "dataset": "ds_coverage_summary", "fields": ["tables_with_no_rule"],
         "title": "Target tables with NO quality rule",
         "position": {"x": 0, "y": 0, "width": 2, "height": 3}},
        {"name": "w_coverage_table", "widgetType": "table",
         "dataset": "ds_coverage",
         "fields": ["table_name", "rules_declared", "coverage_state"],
         "title": "Coverage by target table",
         "position": {"x": 2, "y": 0, "width": 4, "height": 6}},
        {"name": "w_accept_rate", "widgetType": "table",
         "dataset": "ds_accept_rate",
         "fields": ["table_name", "denominator", "accepted", "accept_rate", "state"],
         "title": "Acceptance rate, or not evaluated",
         "position": {"x": 0, "y": 6, "width": 6, "height": 6}},
        {"name": "w_discard_reason", "widgetType": "table",
         "dataset": "ds_discard_reason",
         "fields": ["table_name", "discard_reason", "discarded", "runs"],
         "title": "Discards by reason",
         "position": {"x": 0, "y": 12, "width": 6, "height": 6}},
        {"name": "w_load_volume", "widgetType": "table",
         "dataset": "ds_load_volume",
         "fields": ["job_run_id", "table_name", "staged", "accepted",
                    "not_accepted", "recorded_at"],
         "title": "Rows staged and accepted",
         "position": {"x": 0, "y": 18, "width": 6, "height": 6}},
        {"name": "w_incomplete_runs", "widgetType": "table",
         "dataset": "ds_incomplete_runs",
         "fields": ["job_run_id", "target", "opened_at"],
         "title": "Runs that never completed",
         "position": {"x": 0, "y": 24, "width": 3, "height": 5}},
        {"name": "w_supersede", "widgetType": "table",
         "dataset": "ds_supersede",
         "fields": ["manifest_id", "table_name", "superseded_by", "reason", "recorded_at"],
         "title": "Rejects later accepted",
         "position": {"x": 3, "y": 24, "width": 3, "height": 5}},
    ]
