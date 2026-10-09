"""Render the quality definition into the artefacts the bundle deploys.

GENERATED, NEVER HAND-EDITED. verify_repo.py asserts that regenerating produces byte-
identical files, so the diff is the review -- the same discipline
metadata/key_composition.json and data_contracts/ already carry.

ONE ARTEFACT FOR NINE TARGETS. The bundle's dataset_catalog sets the default catalog for
every dataset in the dashboard, overriding anything a dataset names, so the queries stay
unqualified and the catalog is a deploy-time concern. That is why this file emits two
dashboards (business and synthetic) rather than eighteen.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import bronze_quality, gold_quality, quality  # noqa: E402

sys.path.insert(0, str(ROOT / "tools"))
from emit_data_contract import targets_and_variables  # noqa: E402
from emit_source_contract import configured_targets  # noqa: E402

OUT_DIR = ROOT / "dashboards"

# artefact filename -> the table prefix it is rendered with.
# stem -> table prefix. The filename gains `_<target>` because the names inside are
# qualified with that target's catalog. See main().
ARTEFACTS: dict[str, str] = {
    "quality_silver": "",
    "quality_silver_synthetic": "tst_",
}

# THE BRONZE FAMILY, and it is deliberately a SECOND family rather than a second page on the
# silver one. Different subject, different audience, and the silver artefact is named for the
# layer it reports on -- a bronze page inside it would make that name a lie.
#
# EMITTED ONLY FOR TARGETS THAT HAVE A CONTRACT. A dashboard for a lake that has not declared
# its Bronze would read every tile as zero, which is indistinguishable from a lake that
# conforms perfectly. configured_targets() is the same gate emit_source_contract.py applies to
# the contracts themselves, so the two can never disagree about which targets are in scope.
BRONZE_ARTEFACTS: dict[str, str] = {
    "quality_bronze": "",
    "quality_bronze_synthetic": "tst_",
}

# THE GOLD FAMILY, EMITTED AND DEPLOYED BY NOTHING. Measured 29 Aug: 03_usnc_gold_edm_dev does
# not exist, no task produces gold, and governance/control_objects_gold.sql is generated and
# never applied. This follows that file exactly -- committed, gated against drift, declared by
# no bundle resource. verify_repo.py fails the build if a declaration appears.
#
# Emitted for every target that declares a gold_catalog, which is all of them: when gold does
# land it lands per target, and a generated artefact costs nothing to keep right in the
# meantime because regeneration is asserted byte-identical.
GOLD_ARTEFACTS: dict[str, str] = {
    "quality_gold": "",
    "quality_gold_synthetic": "tst_",
}

GENIE_ARTEFACT = "quality_silver.geniespace.json"

_COUNTER_SPEC_VERSION = 2
_GENIE_SPACE_VERSION = 2  # required; 1 and 2 both normalise to 2
_TABLE_SPEC_VERSION = 2  # v1 is accepted but renders no columns


def _widget(tile: dict) -> dict:
    """One layout entry. Field expressions are bare backtick-quoted identifiers and
    nothing else -- the emitter never interpolates an expression."""
    fields = [{"name": f, "expression": f"`{f}`"} for f in tile["fields"]]
    query = {
        "name": "main_query",
        "query": {
            "datasetName": tile["dataset"],
            "fields": fields,
            "disaggregated": True,
        },
    }
    if tile["widgetType"] == "counter":
        spec = {
            "version": _COUNTER_SPEC_VERSION,
            "widgetType": "counter",
            "encodings": {
                "value": {"fieldName": tile["fields"][0],
                          "displayName": tile["title"]},
            },
        }
    else:
        # TABLE WIDGETS ARE VERSION 2 AND NEED A `data.queryName`. Version 1 with a
        # 22-field column spec was ACCEPTED by the API and rendered
        # "Visualization has no fields selected" on the live dashboard. This shape is
        # copied from a working dashboard rather than derived -- junTaniguchi/
        # dab_data_platform, table_mutation_audit.lvdash.json -- after four attempts that
        # the API accepted and the client would not draw.
        spec = {
            "version": _TABLE_SPEC_VERSION,
            "widgetType": "table",
            "encodings": {
                "columns": [
                    {"fieldName": f, "type": "string", "displayAs": "string", "title": f}
                    for f in tile["fields"]
                ],
            },
            "data": {"queryName": "main_query"},
            "frame": {"showTitle": True, "title": tile["title"]},
        }
    return {
        "widget": {"name": tile["name"], "queries": [query], "spec": spec},
        "position": tile["position"],
    }


def _newline_terminated(query_lines: list) -> list:
    """EVERY LINE ENDS WITH A NEWLINE, and that is not cosmetic. The service CONCATENATES
    queryLines with no separator, so a line ending in a bare token fuses into the next one:
    `recorded_at` + `FROM ...` deployed as `recorded_atFROM ...` and EVERY tile failed to
    render on invalid SQL. Measured 27 Aug by reading the deployed serialized_dashboard back.

    Shared by both families, because the service does this to both and a second copy of the
    rule is a second place for it to be forgotten."""
    return [ln if ln.endswith("\n") else ln + "\n" for ln in query_lines]


def bronze_dashboard(table_prefix: str = "",
                     catalog: str = "",
                     control_schema: str = "") -> dict:
    """The Bronze conformance .lvdash.json structure.

    Same shape as dashboard() and deliberately so: one _widget(), one newline rule, one
    render(). Only the definition module and the page name differ."""
    return {
        "datasets": [
            {**d, "queryLines": _newline_terminated(d["queryLines"])}
            for d in bronze_quality.datasets(table_prefix, catalog=catalog,
                                             control_schema=control_schema)
        ],
        "pages": [
            {
                "name": "page_bronze_conformance",
                "displayName": "Bronze conformance",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": [_widget(t) for t in bronze_quality.tiles()],
            }
        ],
    }


def gold_dashboard(table_prefix: str = "",
                   gold_catalog: str = "",
                   control_schema: str = "") -> dict:
    """The Gold load-quality .lvdash.json structure.

    QUALIFIED WITH THE GOLD CATALOG. Every other family here qualifies with ${var.catalog};
    this one takes ${var.gold_catalog}, and getting it wrong would render the VAULT's audit
    under a page titled Gold -- real numbers, no error, wrong layer."""
    return {
        "datasets": [
            {**d, "queryLines": _newline_terminated(d["queryLines"])}
            for d in gold_quality.datasets(table_prefix, gold_catalog=gold_catalog,
                                           control_schema=control_schema)
        ],
        "pages": [
            {
                "name": "page_gold_quality",
                "displayName": "Gold load quality",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": [_widget(t) for t in gold_quality.tiles()],
            }
        ],
    }


def dashboard(table_prefix: str = "",
              catalog: str = "",
              control_schema: str = "") -> dict:
    """The full .lvdash.json structure.

    pageType is emitted because the service adds PAGE_TYPE_CANVAS on create; emitting it
    keeps the committed file identical to what the workspace considers canonical.
    """
    return {
        "datasets": [
            # EVERY LINE ENDS WITH A NEWLINE, and that is not cosmetic. The service
            # CONCATENATES queryLines with no separator, so a line ending in a bare token
            # fuses into the next one: `recorded_at` + `FROM ...` deployed as
            # `recorded_atFROM ...` and EVERY tile failed to render on invalid SQL.
            # Measured 27 Aug by reading the deployed serialized_dashboard back. The
            # earlier API probe used a single-line query, so it could not surface this.
            {**d, "queryLines": [ln if ln.endswith("\n") else ln + "\n"
                                 for ln in d["queryLines"]]}
            for d in quality.datasets(table_prefix, catalog=catalog,
                                      control_schema=control_schema)
        ],
        "pages": [
            {
                "name": "page_quality",
                "displayName": "Load quality",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": [_widget(t) for t in quality.tiles()],
            }
        ],
    }


def genie_space() -> dict:
    """The .geniespace.json structure.

    THE SHAPE BELOW WAS PROVED AGAINST THE LIVE API on 27 Aug 2026, not guessed. An earlier
    version of this function emitted {tables, instructions, sample_questions}, which the
    bundle accepted at `validate` and the service REJECTED at deploy:

        Invalid JSON in field 'serialized_space': Expected an object, not an array

    Probing POST /api/2.0/genie/spaces established:
      * `version` is REQUIRED and must be 1 or 2; the service normalises both to 2.
      * the table list lives at `data_sources.tables`, an OBJECT containing an array --
        `tables` at top level is rejected as an unknown field, and that array-where-an-
        object-was-expected is what the deploy error meant.
      * each entry is keyed `identifier`, holding a three-part name. `name`, `full_name`
        and catalog/schema/table triples are all rejected.

    STILL UNPROVED, AND WHY THE RESOURCE IS DISABLED: `instructions` is accepted as a key
    but its inner field is not `content` or `text`, and probing stopped there. Those
    instructions are the ONLY safeguard against Genie answering "0 rejects" to "how many
    rows failed" and implying the pipeline is clean, when the truth is that no rule exists
    to evaluate -- see the spec's section 2. Deploying a Genie space WITHOUT them would ship
    exactly the misleading answer the design exists to prevent, so
    resources/quality_dashboard.yml keeps the genie_spaces block commented out until the
    field is known. This emitter therefore produces a structurally valid space, and nothing
    deploys it.

    CONTROL TABLES ONLY, AND EMITTED BARE -- which is the SECOND unresolved problem. Unlike
    a dashboard, a Genie space has no `dataset_catalog` equivalent, so a bare name has
    nothing to resolve against and cannot be parameterised per target at deploy time. The
    probe that succeeded used a three-part name. So a deployable space needs either one
    artefact per target with names qualified from that target's variables, or a
    per-target-qualified `identifier` built here. Neither is done, and the resource is
    disabled, so this emits the structure without pretending the names resolve.
    """
    return {
        "version": _GENIE_SPACE_VERSION,
        "data_sources": {
            "tables": [{"identifier": t} for t in quality.CONTROL_TABLES],
        },
    }


def render(structure: dict) -> str:
    """Deterministic text. sort_keys so key order cannot drift between runs, and exactly
    one trailing newline so the committed file does not diff on whitespace."""
    return json.dumps(structure, indent=2, sort_keys=True) + "\n"


def main() -> None:
    """One artefact PER TARGET, with fully-qualified table names.

    NOT one artefact for nine targets, which is what this emitted until 27 Aug. That
    depended on the bundle's dataset_catalog / dataset_schema binding the bare names at
    deploy time, and measurement against the deployed dashboard showed they do not reach it:
    every tile failed with TABLE_OR_VIEW_NOT_FOUND because the warehouse resolved the bare
    names against hive_metastore.default. The catalog differs per target, so a qualified
    artefact is necessarily per target -- the same shape data_contracts/ already has, and for
    the same reason.
    """
    OUT_DIR.mkdir(exist_ok=True)
    for target, variables in targets_and_variables():
        catalog = variables.get("catalog", "")
        control_schema = variables.get("control_schema", "control")
        if not catalog:
            raise ValueError(
                f"target {target!r} resolves no `catalog` variable, so its dashboard's table "
                f"names cannot be qualified. A bare name resolves against the warehouse "
                f"default and every tile fails."
            )
        for stem, prefix in ARTEFACTS.items():
            fn = f"{stem}_{target}.lvdash.json"
            (OUT_DIR / fn).write_text(
                render(dashboard(prefix, catalog, control_schema)), encoding="utf-8")
            print(f"wrote dashboards/{fn}")
    # BRONZE: only the targets that have committed a source contract. See BRONZE_ARTEFACTS.
    for target, variables in configured_targets():
        catalog = variables.get("catalog", "")
        control_schema = variables.get("control_schema", "control")
        if not catalog:
            raise ValueError(
                f"target {target!r} resolves no `catalog` variable, so its bronze dashboard's "
                f"table names cannot be qualified. A bare name resolves against the warehouse "
                f"default and every tile fails."
            )
        for stem, prefix in BRONZE_ARTEFACTS.items():
            fn = f"{stem}_{target}.lvdash.json"
            (OUT_DIR / fn).write_text(
                render(bronze_dashboard(prefix, catalog, control_schema)), encoding="utf-8")
            print(f"wrote dashboards/{fn}")

    # GOLD: every target that declares a gold_catalog. Nothing deploys these -- see
    # GOLD_ARTEFACTS.
    for target, variables in targets_and_variables():
        gold_catalog = variables.get("gold_catalog", "")
        control_schema = variables.get("control_schema", "control")
        if not gold_catalog:
            raise ValueError(
                f"target {target!r} resolves no `gold_catalog` variable, so its gold "
                f"dashboard's table names cannot be qualified. A bare name resolves against "
                f"the warehouse default and every tile fails."
            )
        for stem, prefix in GOLD_ARTEFACTS.items():
            fn = f"{stem}_{target}.lvdash.json"
            (OUT_DIR / fn).write_text(
                render(gold_dashboard(prefix, gold_catalog, control_schema)), encoding="utf-8")
            print(f"wrote dashboards/{fn}")

    (OUT_DIR / GENIE_ARTEFACT).write_text(render(genie_space()), encoding="utf-8")
    print(f"wrote dashboards/{GENIE_ARTEFACT}")


if __name__ == "__main__":
    main()
