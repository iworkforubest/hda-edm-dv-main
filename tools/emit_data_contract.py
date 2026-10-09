#!/usr/bin/env python3
"""
Emit one datacontract.com-shaped YAML per bundle target, generated from
metadata/entities/*.yml and nothing else.

THE MODEL IS THE ONLY AUTHORITY. This file never invents a fact about an entity --
every derivation it needs (classification, column type, uniqueness grain, foreign
keys, clustering keys) already lives in accelerator/contract.py and is called here,
not restated. See docs/superpowers/specs/2026-08-27-data-contract-export-design.md
for why: the alternative (hand-authoring the contract and generating the model from
it) was rejected on what an exemplar contract got WRONG for this estate -- SHA-1
STRING(40) hash keys and clustering on them -- and generation makes both errors
impossible by construction rather than by review discipline.

    uv run python tools/emit_data_contract.py

Writes data_contracts/<target>.yaml for every target in databricks.yml. Review the
diff before committing -- verify_repo.py's no-op gate (Task 4) then asserts that
regenerating is always a no-op, exactly as tools/refresh_key_composition.py does for
metadata/key_composition.json.

THE PYSPARK STUB, AND WHY IT IS HERE. factory.py imports pyspark at module level
(`from pyspark import pipelines as dp`), and this repository has no pyspark
dependency -- see pyproject.toml. Every other offline caller of factory internals
(contract.clustering, reject_digest.digest_columns) keeps its `from .factory import
...` INSIDE a function for exactly this reason, deferring the problem to whoever
calls them. This tool is the first caller that runs standalone, outside
tests/test_accelerator.py's own stub, so the deferral has to stop somewhere: this
module installs the same minimal stub tests/test_accelerator.py does, via
sys.modules.setdefault so a stub already installed by a caller (the test suite,
or a future verify_repo.py Task 4 import) always wins over this one.

Only entity metadata is read through the stubbed functions below (_projection,
_cluster_by, _mandatory_rules build plain dicts of column names and SQL strings from
declared metadata) -- nothing here needs a real Spark session or a real DataFrame.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

if "pyspark" not in sys.modules:
    _stub = types.ModuleType("pyspark")
    _stub_pipelines = types.ModuleType("pyspark.pipelines")
    _stub_sql = types.ModuleType("pyspark.sql")
    _stub_functions = types.ModuleType("pyspark.sql.functions")

    def _stub_create_streaming_table(**kwargs):  # noqa: ANN001, ANN201
        return None

    def _stub_append_flow(target, name, once=False):  # noqa: ANN001, ANN201
        def _capture(fn):
            return fn
        return _capture

    def _stub_decorator(*a, **kw):  # noqa: ANN001, ANN201
        return lambda fn: fn

    def _stub_materialized_view(**kwargs):  # noqa: ANN001, ANN201
        return lambda fn: fn

    class _FakeCol:
        """Inert chainable stand-in for a pyspark Column. Nothing evaluates it --
        the metadata-only functions this tool calls never inspect a value."""

        def __init__(self, alias=None):
            self._alias = alias

        def _same(self, *a, **kw):  # noqa: ANN001, ANN201
            return self

        cast = isNull = isNotNull = otherwise = eqNullSafe = _same
        __and__ = __or__ = __invert__ = __eq__ = __ne__ = _same

        def alias(self, name):  # noqa: ANN001, ANN201
            return _FakeCol(name)

        def isin(self, *a):  # noqa: ANN001, ANN201
            return self

    def _fake_fn(*a, **kw):  # noqa: ANN001, ANN201
        return _FakeCol()

    _stub_pipelines.create_streaming_table = _stub_create_streaming_table
    _stub_pipelines.append_flow = _stub_append_flow
    _stub_pipelines.expect_all_or_drop = _stub_decorator
    _stub_pipelines.materialized_view = _stub_materialized_view
    _stub_functions.__getattr__ = lambda _name: _fake_fn
    _stub_sql.DataFrame = object
    _stub_sql.functions = _stub_functions
    _stub.pipelines = _stub_pipelines
    _stub.sql = _stub_sql

    sys.modules.setdefault("pyspark", _stub)
    sys.modules.setdefault("pyspark.pipelines", _stub_pipelines)
    sys.modules.setdefault("pyspark.sql", _stub_sql)
    sys.modules.setdefault("pyspark.sql.functions", _stub_functions)

from accelerator import VERSION, contract, factory, naming, spec  # noqa: E402

__all__ = ["emit", "render", "targets_and_variables"]

DATABRICKS_YML = ROOT / "databricks.yml"
OUTPUT_DIR = ROOT / "data_contracts"

CONTRACT_SPEC_VERSION = "1.1.0"  # the datacontract.com spec version this shape targets


# --------------------------------------------------------------------------- #
# Reading databricks.yml -- every bundle target and its resolved variables.
# --------------------------------------------------------------------------- #
def targets_and_variables() -> list[tuple[str, dict]]:
    """Every bundle target, with its variables resolved the way the bundle resolves
    them: a target's own `variables:` block overrides the top-level `variables:`
    defaults, and a variable with no default (catalog, among others) is present only
    where a target sets it.

    Read in file order -- weu_tds, weu, dev, uks_tds, uks, usnc_tds, usnc, aue_tds,
    aue -- which is deterministic for a given databricks.yml, so this needs no
    explicit sort to be reproducible.

    DELIBERATELY NOT A FULL BUNDLE VARIABLE RESOLVER: `${var.x}` interpolation inside
    a value (there is none among vault_schema/business_vault_schema/catalog today)
    is not expanded. If a future default or override introduces one, the emitted
    physical_name would carry the literal `${var.x}` rather than a resolved value,
    visibly wrong rather than silently wrong.
    """
    data = yaml.safe_load(DATABRICKS_YML.read_text(encoding="utf-8")) or {}
    global_defaults = {
        name: cfg.get("default")
        for name, cfg in (data.get("variables") or {}).items()
        if isinstance(cfg, dict) and "default" in cfg
    }
    result = []
    for target_name, target_cfg in (data.get("targets") or {}).items():
        resolved = dict(global_defaults)
        resolved.update((target_cfg or {}).get("variables") or {})
        result.append((target_name, resolved))
    return result


# --------------------------------------------------------------------------- #
# Per-entity derivation -- calls contract.py for everything it knows how to derive.
# --------------------------------------------------------------------------- #
def _projection_binding(entity: spec.Entity, src):
    """The binding whose payload a table's projection is built from.

    entity.tables() gives a real SourceBinding only for sat/msat/esat (one physical
    table per source); every other kind -- hub, link, nhl, hal, csat -- gives
    (None, base_table), and factory._projection dereferences src.payload
    unconditionally on the nhl and satellite branches. Falling back to
    entity.sources[0] (spec.validate guarantees at least one) is the same fallback
    contract.column_type carries internally and checks/load_satellites.py's
    `proj_src` uses -- not a second, competing rule.
    """
    return src if src is not None else (entity.sources[0] if entity.sources else None)


def _projected_columns(entity: spec.Entity, src) -> list[str]:
    """The column names of ONE physical table of `entity`, for the binding `src`.

    A CONTRACT DESCRIBES TABLES, NOT ENTITIES, and for a satellite those are not the
    same thing: entity.tables() yields one physical table PER SOURCE BINDING, so
    job_request_details is four tables (sat_job_request_details_striive_eu,
    _bullhorn_eu, _fieldglass_eu, _prounity_eu) and worker_skills is two. An earlier
    version of this emitter took entity.tables()[0] and keyed every entry on
    entity.base_table, which published seven table names that DO NOT EXIST and omitted
    eleven that do -- and, worse, described the bullhorn/fieldglass/prounity tables
    with the striive projection, whose payload columns genuinely differ
    (job_title/positions/closing_date against title/numopenings/dateclosed).

    checks/mask_survival_check.py:303 and tests/test_accelerator.py both iterate
    e.tables() in full for exactly this reason; this emitter now follows that pattern
    rather than inventing a second one.
    """
    return [c for c, _kind, _value in factory._projection(entity,
                                                          _projection_binding(entity, src))]


def _qualify_reference(ref: str, by_base_table: dict, catalog: str,
                        vault_schema: str, business_vault_schema: str) -> str:
    """Turn contract.foreign_keys()'s entity-relative `base_table.column` into the
    four-part `catalog.schema.table.column` this emitter has, and contract.py
    deliberately does not: it runs offline and knows neither. The schema half of the
    answer still depends on the REFERENCED entity's kind (a csat could in principle
    be a parent), not the referencing entity's, so the lookup is by base_table.
    """
    table, _, column = ref.partition(".")
    parent = by_base_table.get(table)
    schema = (naming.vault_schema_for(parent.kind, vault_schema, business_vault_schema)
              if parent is not None else vault_schema)
    return f"{catalog}.{schema}.{table}.{column}"


_UNIMPLEMENTED_SEVERITIES = frozenset({"fail", "warn"})
_KNOWN_SEVERITIES = frozenset({"drop", "fail", "warn"})


def _refuse_unimplemented_severity(expectations: dict | None, by_table_name: dict) -> None:
    """Refuse a governed expectation this pipeline cannot honour, or one named
    against a dataset the model does not have.

    `by_table_name` is keyed on PHYSICAL TABLE names, not on entity base_tables,
    because quality.expectations is keyed the same way -- a satellite has one table
    per source binding, so `sat_job_request_details` is not a dataset a rule can name
    and `sat_job_request_details_bullhorn_eu` is.

    DEF-18 (factory.py:988): `@dp.expect_all_or_drop` CANNOT be stacked under
    `@dp.append_flow` and is never actually invoked. What every generated flow
    really does (factory.py:1004-1043) is share one `_violation_expr(rules)`
    between two flows: `_valid` keeps `df.where(~failed)`, and `_invalid` appends
    the complement to the quarantine twin with its reason. A violating row is
    DROPPED and quarantined; the load CONTINUES. That is the 'drop' tier
    (governance/control_objects.sql's severity comment) and it is the ONLY tier
    with any implementation. 'fail' (the load stops) and 'warn' (advisory, the row
    still loads) are both declarable in ref_dq_expectation's schema and neither has
    ANY mechanism behind it, so both are refused here for the identical reason:
    publishing either would promise a consumer a behaviour this pipeline does not
    have. A missing or unrecognised severity (case-insensitively matched, so
    'WARN' cannot slip through as an unmapped value) is refused rather than
    silently treated as though it were 'drop'.

    control.ref_dq_expectation lives in a lake this offline emitter cannot read, so
    today `expectations` is always None and none of this fires. It exists for a
    future caller that CAN read that table.
    """
    for dataset, rules in (expectations or {}).items():
        if dataset not in by_table_name:
            raise ValueError(
                f"{dataset!r} is not a table this model declares -- no physical table "
                f"matches it. A governed expectation naming an unknown dataset would "
                f"silently create a quality.expectations entry for a table that does "
                f"not exist."
            )
        for rule in rules:
            severity = str(rule.get("severity", "")).strip().lower()
            if severity not in _KNOWN_SEVERITIES:
                raise ValueError(
                    f"{dataset}/{rule.get('rule_name', '?')}: severity "
                    f"{rule.get('severity')!r} is not one of the three tiers "
                    f"governance/control_objects.sql declares (fail/drop/warn). A "
                    f"missing or unrecognised severity must not silently pass "
                    f"through unrefused."
                )
            if severity in _UNIMPLEMENTED_SEVERITIES:
                raise ValueError(
                    f"{dataset}/{rule.get('rule_name', '?')}: severity {severity!r} "
                    f"is declared, but this pipeline has no mechanism to honour it "
                    f"-- every generated flow filters violating rows with "
                    f"`df.where(~failed)` and writes them to the quarantine twin "
                    f"(factory.py's _valid/_invalid flows, DEF-18), which is the "
                    f"'drop' tier only. Emitting this expectation would promise a "
                    f"consumer a behaviour we cannot deliver."
                )


def emit(model: spec.Model, target: str, variables: dict, expectations: dict | None = None) -> dict:
    """The contract structure for one target. Pure: same (model, target, variables,
    expectations) in, same dict out, no I/O and no clock read.

    `expectations` is None in every artefact this repo commits today -- see
    _refuse_unimplemented_severity. It exists so a future caller with real access
    to control.ref_dq_expectation has somewhere to pass governed rules through
    without this signature changing again.
    """
    # NAMED, NOT A BARE KeyError. `catalog` is the one variable databricks.yml gives no
    # global default -- only a target's own `variables:` block sets it -- so a target
    # added without one reaches here with the key simply absent. verify_repo.py's
    # staleness loop calls emit() once per target at module level, and a KeyError
    # escaping mid-loop aborts that file outright, turning every check after the
    # contracts gate silently ABSENT. One red check carrying this message is the outcome
    # this repo wants instead.
    if "catalog" not in variables:
        raise ValueError(
            f"target {target!r} resolves no `catalog` variable, so no physical_name can "
            f"be built for it. `catalog` has no global default in databricks.yml -- add "
            f"one to this target's `variables:` block."
        )
    catalog = variables["catalog"]
    vault_schema = variables.get("vault_schema", "raw_vault")
    business_vault_schema = variables.get("business_vault_schema", "business_vault")

    by_base_table = {e.base_table: e for e in model.entities}
    # Keyed on the PHYSICAL tables, which is what the contract entries and
    # quality.expectations are keyed on -- see _projected_columns.
    by_table_name = {t: e for e in model.entities if e.kind in naming.GENERATABLE
                     for _s, t in e.tables()}
    _refuse_unimplemented_severity(expectations, by_table_name)

    entities: dict = {}
    quality_expectations: dict = {}

    for entity in model.entities:
        # A second, independent guard against a PIT/BRIDGE (or any other
        # non-generatable) entity, not a restatement of spec.load_model's own
        # refusal of an ungeneratable kind at load time. If a model somehow
        # reached this function carrying one anyway -- constructed directly
        # rather than through load_model, as a test can do -- it is dropped here
        # rather than emitted or crashing _projection, which assumes hub/link/
        # nhl/hal or a satellite-shaped kind.
        if entity.kind not in naming.GENERATABLE:
            continue
        schema = naming.vault_schema_for(entity.kind, vault_schema, business_vault_schema)

        # PER ENTITY, because every table of an entity shares these: the parents the
        # loaders hash against, the masks the entity declares, the sensitivity the
        # model carries, and the clustering the runtime will apply.
        #
        # THIS IS THE ONE PLACE IN THIS DOCUMENT THAT TELLS A READER WHERE TO GO NEXT,
        # and contract.foreign_keys() already resolves it through the parent's
        # unversioned base_table (Task 7, R2) -- a consumer follows a foreign key to
        # JOIN, and a reference naming a physical _rev<N> table would bind them to a
        # name the next cutover moves. _qualify_reference below keeps that unversioned
        # table verbatim; it must never be rewritten to the physical name.
        fk_raw = contract.foreign_keys(entity, model)
        fk_qualified = {
            col: _qualify_reference(ref, by_base_table, catalog, vault_schema,
                                     business_vault_schema)
            for col, ref in fk_raw.items()
        }
        masked = dict(entity.masks)
        clustering_keys = contract.clustering(entity)

        # PER TABLE, AND DELIBERATELY PHYSICAL. entity.tables() yields one physical
        # table per source binding for a satellite and exactly one for every other
        # kind -- iterated in full, never [0], because the entries below name real
        # tables. Unlike the foreign keys above, `physical_name` and this entry's own
        # key describe WHERE THE LOADER WRITES, not what a consumer joins to: this is
        # the real, physical object that gets created, and a reader auditing storage
        # or reconciling what actually exists needs that identity, not the view's.
        # A CONSUMER STILL BINDS TO THE VIEW, so the entry also carries `stable_name`
        # (entity.stable_tables(), zipped with tables() by position -- same length,
        # same order, by construction) -- the unversioned name the view answers on.
        # `physical_name` alone would read as "query this", and a consumer who bound
        # to it would keep reading the superseded version silently at the next
        # cutover; foreign keys already resolve through the unversioned name, but a
        # reader auditing a single entity's own row has nothing else to bind to.
        for (src, table_name), (_ssrc, stable_table) in zip(entity.tables(),
                                                              entity.stable_tables()):
            columns = _projected_columns(entity, src)
            physical_name = f"{catalog}.{schema}.{table_name}"
            # THE NAME A CONSUMER SHOULD BIND TO. physical_name above is the versioned
            # table, which is what this contract DESCRIBES; stable_name is the view, which
            # is what a reader should QUERY. Both, because a contract that gave only the
            # physical name would point every consumer at a name the next cutover moves --
            # and they would keep reading the superseded version with nothing failing.
            stable_name = f"{catalog}.{schema}.{stable_table}"
            grain_cols = contract.grain(entity, columns)

            col_defs: dict = {}
            for c in columns:
                entry = {
                    # `src` is the REAL binding for a satellite table and None for
                    # every other kind, where column_type falls back to sources[0].
                    "type": contract.column_type(entity, src, c),
                    "classification": contract.classify(entity, c),
                }
                # WHAT THE COLUMN MEANS, where anything can say. Omitted rather than
                # emitted empty: a `description: ""` on 163 columns would read as "we
                # looked and there is nothing to say", which is not what it means.
                _desc = contract.description(entity, c, model)
                if _desc:
                    entry["description"] = _desc
                if c in grain_cols:
                    entry["primaryKey"] = True
                if c in fk_qualified:
                    entry["references"] = fk_qualified[c]
                if c in masked:
                    entry["maskFunction"] = masked[c]
                col_defs[c] = entry

            entity_entry = {
                "physical_name": physical_name,
                "stable_name": stable_name,
                "kind": entity.kind,
                "domain": entity.domain,
                "sensitivity": entity.sensitivity,
                "grain": entity.grain,
                "description": entity.notes,
                "primary_key": grain_cols,
                "columns": col_defs,
                # COPIED PER TABLE, never shared. Assigning the SAME list/dict object
                # into several entries makes yaml.safe_dump emit an anchor on the first
                # and `*id00N` aliases on the rest -- 72 alias lines across the nine
                # artefacts when this was first written. These are human-readable
                # published governance artefacts: a reader of
                # sat_job_request_details_prounity_eu would have had to scroll to the
                # bullhorn entry to learn its foreign keys, which is most of what C1 was
                # for, and a consumer whose YAML reader disables aliases (a common
                # billion-laughs mitigation) could not load the file at all. safe_load
                # resolves aliases, so no round-trip or set-equality check could see it.
                "storage": {"clustering_keys": list(clustering_keys)},
            }
            if fk_qualified:
                entity_entry["foreign_keys"] = dict(fk_qualified)
            entities[table_name] = entity_entry

            # THE COMPILED-IN KEY-SAFETY RULES. control.ref_dq_expectation cannot be
            # read offline (it lives in a lake), so no governed expectation is emitted
            # -- only the rulebook rules the loaders always run regardless of
            # configuration: factory._mandatory_rules(), which is hub/link/nhl-key
            # presence plus hashing.key_safety_rules() (present, no delimiter, not a
            # null token) per key component. Built from THIS table's binding, since a
            # satellite's key columns come from the binding it is fed by. Tagged
            # 'drop', not 'fail': DEF-18 (factory.py:988) means
            # `@dp.expect_all_or_drop` is never actually invoked, and these rules feed
            # the SAME `_violation_expr` / `df.where(~failed)` filter as every other
            # rule (factory.py:1004-1043) -- a violating row is dropped and written to
            # the quarantine twin, and the load continues. Nothing in this pipeline
            # currently stops a load.
            mandatory = factory._mandatory_rules(entity,
                                                 _projection_binding(entity, src))
            quality_expectations[table_name] = [
                {"rule_name": name, "rule_sql": sql, "severity": "drop",
                 "source": "compiled-in"}
                for name, sql in sorted(mandatory.items())
            ]

    for dataset, rules in (expectations or {}).items():
        quality_expectations.setdefault(dataset, [])
        quality_expectations[dataset].extend(
            {**rule, "source": "governed"} for rule in rules)

    return {
        "dataContractSpecification": CONTRACT_SPEC_VERSION,
        "id": f"hfig-dv-accelerator-{target}",
        "target": target,
        "info": {
            "title": f"HFIG Data Vault Accelerator -- {target}",
            "version": VERSION,
            "description": (
                "Generated by tools/emit_data_contract.py from metadata/entities/*.yml. "
                "Never hand-edited -- regenerate and commit the diff. The `entities` "
                "section below is keyed on PHYSICAL TABLE names, one entry per table: a "
                "satellite has one table per source binding, so job_request_details "
                "appears as sat_job_request_details_striive_eu, _bullhorn_eu, "
                "_fieldglass_eu and _prounity_eu, each carrying that binding's own "
                "columns, which genuinely differ. Two targets can resolve the identical "
                "catalog and schemas (their entries below then match byte for byte); the "
                "`target` field above is how a reader tells that apart from a copy error."
            ),
        },
        "servers": {
            target: {
                "type": "databricks",
                "catalog": catalog,
                "schemas": {
                    "raw_vault": vault_schema,
                    "business_vault": business_vault_schema,
                },
            },
        },
        "entities": entities,
        "quality": {
            "description": (
                "control.ref_dq_expectation holds governed data-quality rules but "
                "lives in a lake this offline emitter cannot read, so no governed "
                "expectation is included here -- only the rulebook's compiled-in "
                "key-safety rules (factory._mandatory_rules / "
                "hashing.key_safety_rules), which the loaders always run regardless "
                "of configuration. Every rule below is 'drop' severity: a "
                "violating row is dropped and written to the quarantine twin, and "
                "the load continues (see quality.severities for what the other "
                "two declarable tiers actually do -- nothing, today)."
            ),
            "severities": {
                "drop": "honoured -- a violating row is dropped and written to "
                        "the quarantine twin; the load continues "
                        "(factory.py's _valid/_invalid append flows)",
                "fail": "declarable in control.ref_dq_expectation's schema, but "
                        "not implemented -- this emitter refuses it if passed "
                        "in, because @dp.expect_all_or_drop is never actually "
                        "invoked (DEF-18) and nothing stops a load",
                "warn": "declarable in control.ref_dq_expectation's schema, but "
                        "not implemented -- this emitter refuses it if passed "
                        "in, because no advisory-only mechanism exists",
            },
            "expectations": quality_expectations,
        },
    }


def render(structure: dict) -> str:
    """The YAML text for one contract. yaml.safe_dump(..., sort_keys=True) so the
    output is byte-stable between runs -- Task 4's no-op gate regenerates every
    contract and fails the build on any difference, so an emitter whose key order
    varied between runs would make that gate flap regardless of whether the model
    actually changed.
    """
    return yaml.safe_dump(structure, sort_keys=True, default_flow_style=False,
                           allow_unicode=True)


def main() -> None:
    model = spec.load_model(ROOT / "metadata" / "entities")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for target, variables in targets_and_variables():
        text = render(emit(model, target, variables))
        out_path = OUTPUT_DIR / f"{target}.yaml"
        # newline="\n" PINNED, not left to os.linesep. verify_repo.py's no-op gate
        # compares with read_text(), which is universal-newlines and silently folds
        # CRLF to LF -- so a regeneration on Windows would write CRLF into the
        # committed artefact and the gate would report it as an exact match. The bytes
        # on disk are the artefact.
        out_path.write_text(text, encoding="utf-8", newline="\n")
        print(f"wrote {out_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
