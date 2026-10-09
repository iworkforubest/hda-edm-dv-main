# Quality Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate an AI/BI quality dashboard and a Genie space over the silver control
schema from one definition, with coverage — not pass rate — as the headline, and prove every
tile can render non-zero using disposable `tst_` objects.

**Architecture:** A pure-Python definition module (`src/accelerator/quality.py`) states the
datasets and tiles. An emitter (`tools/emit_quality_dashboard.py`) renders that definition
into one committed `.lvdash.json` and one `.geniespace.json`, both parameterised at deploy
time rather than per target. Bundle resources declare them. Eight gates assert the artefacts
against the definition and against the governance boundary.

**Tech Stack:** Python 3.11+ (3.11 is pyproject's floor — CI runs it), `json`, `yaml`,
Databricks Declarative Automation Bundles, AI/BI (Lakeview) dashboards, Genie spaces.

**Spec:** `docs/superpowers/specs/2026-08-27-quality-dashboard-design.md`

## Global Constraints

* **Python floor is 3.11.** No backslash inside an f-string expression part — it is a
  SyntaxError before 3.12 and `verify_repo` gates it. Hoist into a named local.
* **Tests are NOT pytest.** Both suites are scripts of `check(name, condition, detail)`
  calls: `tests/test_accelerator.py` (structural) and `verify_repo.py` (repo invariants).
  Run them with `python tests/test_accelerator.py` and `python verify_repo.py`.
* **Every new check must be PROVEN ABLE TO FAIL.** Mutate the input, confirm the suite
  reports `FAIL` for that named check and still runs to completion — `ABSENT` is not a pass.
  Restore afterwards.
* **No task asserts an absolute check count.** Counts differ between a clean checkout and a
  worktree with untracked files.
* **This plan is entirely offline.** No task deploys, runs a job, or writes to a workspace.
  Task 6 produces the SQL and the DDL for the synthetic exercise; running it is a separate,
  explicitly-consented act.
* **Coverage is counted per TARGET TABLE, not per entity.** 21 entities resolve to 25
  physical tables. `ref_dq_expectation.dataset` names a target table.
* **No artefact may read `data_quality` from the pipeline event log.** DEF-18 means this
  pipeline declares no SDP expectations, so that field is permanently zero.
* **The business dashboard may query the control schema only** — never `raw_vault` or
  `business_vault`.
* **`embed_credentials` is committed as `false`, and a gate fails the build if it is true
  while `bundle_root_prefix` is `/Workspace/Shared`.** The deployment folder is
  world-writable (`users CAN_MANAGE inherited=True`, measured live 27 Aug 2026); an
  embedded-credentials dashboard there runs as the publisher, whose masks resolve to
  cleartext, so any workspace user could edit it into a vault read. See the spec's
  27 Aug amendment.
* **Do not introduce a `warehouse_id` variable.** `sql_warehouse_id` already exists at
  `databricks.yml:154`. Two variables for one concept is a duplicate authority.
* **`tst_` is the synthetic prefix.** It is deliberately outside
  `append_only_check.CONTROL_PREFIXES = ("ctl_", "ref_", "aud_")` and absent from
  `naming.PREFIX`.

### Verified external facts (do not re-derive)

The `.lvdash.json` shape below was validated by POSTing it to
`/api/2.0/lakeview/dashboards` on 27 Aug 2026 and reading back `serialized_dashboard`. The
service accepted it verbatim and added exactly one field, `"pageType": "PAGE_TYPE_CANVAS"`
on each page. **Emit `pageType` yourself** so the committed artefact is already canonical.

```json
{
  "datasets": [
    {"name": "ds_probe", "displayName": "probe", "queryLines": ["SELECT 1 AS n"]}
  ],
  "pages": [
    {
      "name": "page_1",
      "displayName": "Probe",
      "pageType": "PAGE_TYPE_CANVAS",
      "layout": [
        {
          "widget": {
            "name": "w_counter",
            "queries": [
              {"name": "main_query",
               "query": {"datasetName": "ds_probe",
                         "fields": [{"name": "n", "expression": "`n`"}],
                         "disaggregated": true}}
            ],
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": "n", "displayName": "n"}}}
          },
          "position": {"x": 0, "y": 0, "width": 2, "height": 3}
        }
      ]
    }
  ]
}
```

A `table` widget uses `"version": 1, "widgetType": "table"` and
`"encodings": {"columns": [{"fieldName": ..., "displayName": ...}]}`.

The bundle `dashboards` resource takes `display_name`, `file_path`, `parent_path`,
`warehouse_id`, `permissions`, `embed_credentials`, `dataset_catalog`, `dataset_schema`.
`dataset_catalog` *"sets the default catalog for all datasets in this dashboard… overrides
the catalog specified in individual dataset definitions"* — which is why **one** artefact
serves all nine targets. The `genie_spaces` resource takes `title`, `description`,
`warehouse_id`, `parent_path`, `file_path`, `serialized_space`, `permissions`.

---

## File Structure

| file | responsibility |
|---|---|
| `src/accelerator/quality.py` (new) | The definition: dataset SQL, tile specs, and the rate-guard rule. Pure, importable, no I/O. |
| `tools/emit_quality_dashboard.py` (new) | Renders the definition to `.lvdash.json` and `.geniespace.json`. Mirrors `tools/emit_data_contract.py`. |
| `dashboards/quality_silver.lvdash.json` (new, generated) | Committed artefact. Never hand-edited. |
| `dashboards/quality_silver_synthetic.lvdash.json` (new, generated) | Same tiles against `tst_` tables. |
| `dashboards/quality_silver.geniespace.json` (new, generated) | Genie space definition. |
| `resources/quality_dashboard.yml` (new) | Bundle resources: the two dashboards and the Genie space. |
| `governance/control_test_objects.sql` (new) | The six `tst_` tables plus seed rows, and a drop script. |
| `tests/test_accelerator.py` (modify) | Structural checks over the definition and the artefacts. |
| `verify_repo.py` (modify) | Regeneration-is-a-no-op and boundary gates. |

---

## Task 1: The quality definition module

**Files:**
- Create: `src/accelerator/quality.py`
- Test: `tests/test_accelerator.py` (append a new section)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `CONTROL_TABLES: tuple[str, ...]` — the six control table base names.
  - `datasets(table_prefix: str = "", governance_schema: str = "governance") -> list[dict]`
    — each `{"name","displayName","queryLines"}`.
  - `tiles() -> list[dict]` — each `{"name","widgetType","dataset","fields","title","position"}`.
  - `RATE_GUARD: str` — the SQL fragment every rate must use.
  - `SINGLE_ROW_DATASETS: frozenset[str]` — datasets that return exactly one row. A
    `counter` tile may read only these.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accelerator.py`:

```python
# --------------------------------------------------------------------------- #
print("\n[quality] the dashboard definition")

from accelerator import quality  # noqa: E402

_q_ds = quality.datasets()
_q_names = [d["name"] for d in _q_ds]

check("every dataset has a unique name",
      len(_q_names) == len(set(_q_names)),
      f"duplicates in {_q_names}")

_q_sql = " ".join(" ".join(d["queryLines"]) for d in _q_ds)

check("no dataset reads data_quality from the event log -- DEF-18 makes it permanently zero",
      "data_quality" not in _q_sql,
      "a dataset references data_quality; the pipeline declares no SDP expectations so "
      "that field reads 0 for ever and the tile would be authoritative and wrong")

check("no dataset queries raw_vault or business_vault -- the embedded-credentials boundary",
      "raw_vault" not in _q_sql and "business_vault" not in _q_sql,
      "a dataset reaches into a vault schema; the dashboard is published with embedded "
      "credentials so its viewers hold no grant and would see vault rows")

check("every dataset queries only tables quality.CONTROL_TABLES names",
      all(any(t in " ".join(d["queryLines"]) for t in quality.CONTROL_TABLES)
          for d in _q_ds),
      f"a dataset queries none of {quality.CONTROL_TABLES}")

_q_div = [d["name"] for d in _q_ds
          if "/" in " ".join(d["queryLines"])
          and quality.RATE_GUARD not in " ".join(d["queryLines"])]
check("every dataset computing a rate uses RATE_GUARD -- no bare division",
      not _q_div,
      f"{_q_div} divide without the guard; a zero denominator would render a rate over "
      "nothing, which is the false-confidence failure the spec exists to prevent")

_q_tiles = quality.tiles()
_q_tile_ds = {t["dataset"] for t in _q_tiles}
check("every tile references a dataset that exists",
      _q_tile_ds <= set(_q_names),
      f"tiles reference {sorted(_q_tile_ds - set(_q_names))} which no dataset defines")

# D1: meta_vault_model is in the GOVERNANCE schema while dataset_schema pins the default to
# CONTROL, so a bare reference resolves to a table that does not exist.
_q_mvm_bare = _q_sql.replace("governance.meta_vault_model", "")
check("meta_vault_model is always schema-qualified -- it lives in governance while "
      "dataset_schema pins the default to control",
      "meta_vault_model" not in _q_mvm_bare,
      "a bare meta_vault_model reference resolves to control.meta_vault_model, which does "
      "not exist; the coverage tile would fail at view time in front of a user")

# D2: a counter over a multi-row dataset renders an arbitrary row under an aggregate title.
_q_counters = [t for t in quality.tiles() if t["widgetType"] == "counter"]
_q_bad_counter = [t["name"] for t in _q_counters
                  if t["dataset"] not in quality.SINGLE_ROW_DATASETS]
check("every counter tile reads a single-row dataset",
      not _q_bad_counter,
      f"{_q_bad_counter} read a multi-row dataset. A counter renders whichever row arrives "
      f"first, so the tile would show one arbitrary table's number under a title claiming "
      f"an aggregate -- confidently wrong, which is the failure this dashboard exists to "
      f"avoid")

check("the first tile is coverage -- the spec's headline metric",
      _q_tiles and "coverage" in _q_tiles[0]["name"],
      f"first tile is {_q_tiles[0]['name'] if _q_tiles else '(none)'}; the spec makes "
      "coverage the headline, not pass rate")

_q_pref = quality.datasets(table_prefix="tst_")
_q_pref_sql = " ".join(" ".join(d["queryLines"]) for d in _q_pref)
check("a table prefix reaches every control table reference",
      all(f"tst_{t}" in _q_pref_sql for t in quality.CONTROL_TABLES),
      "prefixing left a bare control table name; the synthetic dashboard would read the "
      "real audit")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python tests/test_accelerator.py`
Expected: `ModuleNotFoundError: No module named 'accelerator.quality'`

- [ ] **Step 3: Write the module**

Create `src/accelerator/quality.py`:

```python
"""The quality dashboard's definition: what it asks, and what it refuses to claim.

ONE DEFINITION, TWO RENDERINGS. datasets() takes a table prefix so the synthetic
dashboard is the same tiles against tst_ tables. A second hand-written definition would
let the two drift, and then exercising the synthetic dashboard would be evidence about a
lookalike rather than about the real one.

WHY COVERAGE IS THE HEADLINE. ref_dq_expectation holds no rules, so every quality
statement but key safety is vacuous. A pass-rate tile would read 100% over an empty
denominator and be indistinguishable from a genuinely clean pipeline. Coverage says how
much is measured at all, which is the true and useful statement today.
"""

from __future__ import annotations

# The six tables governance/control_objects.sql creates. Named here so a dataset cannot
# quietly query something outside the control schema; the test asserts the containment.
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


def _t(prefix: str, table: str) -> str:
    """A control table reference, prefixed for the synthetic rendering."""
    return f"{prefix}{table}"


def datasets(table_prefix: str = "",
             governance_schema: str = "governance") -> list[dict]:
    """The dashboard's datasets.

    CONTROL TABLES ARE UNQUALIFIED: the bundle's dataset_catalog and dataset_schema set the
    catalog and schema at deploy time, which is what lets ONE artefact serve all nine
    targets.

    meta_vault_model IS QUALIFIED, and must be. It is written by checks/publish_metadata.py
    into the GOVERNANCE schema, while dataset_schema pins the default to CONTROL. A bare
    reference would resolve to control.meta_vault_model, which does not exist, and the
    coverage tile -- the headline -- would fail at view time in front of a user. A two-part
    name still resolves inside dataset_catalog, so this stays parameterised per target.
    """
    p = table_prefix
    g = governance_schema
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
                "WITH per_table AS (",
                "  SELECT m.table_name, count(e.rule_name) AS rules_declared",
                f"  FROM {g}.meta_vault_model m",
                f"  LEFT JOIN {_t(p, 'ref_dq_expectation')} e",
                "    ON e.dataset = m.table_name AND e.is_current",
                "  GROUP BY m.table_name",
                ")",
                "SELECT",
                "  count(*) AS tables_total,",
                "  sum(CASE WHEN rules_declared > 0 THEN 1 ELSE 0 END) AS tables_covered,",
                "  sum(CASE WHEN rules_declared = 0 THEN 1 ELSE 0 END)"
                " AS tables_not_evaluated",
                "FROM per_table",
            ],
        },
        {
            "name": "ds_coverage",
            "displayName": "Expectation coverage by target table",
            # THE DENOMINATOR IS TARGET TABLES, NOT ENTITIES. 21 entities resolve to 25
            # physical tables because a satellite has one table per source binding.
            # Keying this on entities would call an entity covered when one of its four
            # bindings carried a rule -- the defect that reached nine published data
            # contracts before review caught it.
            "queryLines": [
                "SELECT",
                "  m.table_name,",
                "  count(e.rule_name) AS rules_declared,",
                "  CASE WHEN count(e.rule_name) = 0",
                "       THEN 'not evaluated' ELSE 'covered' END AS coverage_state",
                f"FROM {g}.meta_vault_model m",
                f"LEFT JOIN {_t(p, 'ref_dq_expectation')} e",
                "  ON e.dataset = m.table_name AND e.is_current",
                "GROUP BY m.table_name",
                "ORDER BY rules_declared, m.table_name",
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
                f"FROM {_t(p, 'aud_table_load')}",
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
                f"FROM {_t(p, 'aud_table_discard')}",
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
                f"  FROM {_t(p, 'aud_table_load')}",
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
                f"FROM {_t(p, 'aud_load_run')}",
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
                f"FROM {_t(p, 'ctl_quarantine_superseded')}",
                "ORDER BY recorded_at DESC",
            ],
        },
        {
            "name": "ds_manifest",
            "displayName": "Approval manifests",
            "queryLines": [
                "SELECT manifest_id, source_system, approved_count, approved_at, approved_by",
                f"FROM {_t(p, 'ctl_approval_manifest')}",
                "ORDER BY approved_at DESC",
            ],
        },
    ]


def tiles() -> list[dict]:
    """The tiles, in reading order. Coverage first: it is the headline."""
    return [
        {"name": "w_coverage_counter", "widgetType": "counter",
         "dataset": "ds_coverage_summary", "fields": ["tables_not_evaluated"],
         "title": "Target tables NOT evaluated",
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Prove three of the new checks can fail**

For each mutation: apply it, run `python tests/test_accelerator.py`, confirm the named
check reports **FAIL** (not absent) and the suite still finishes, then restore.

1. In `ds_accept_rate`, replace `f"  {RATE_GUARD} accepted / denominator END AS accept_rate,"`
   with `"  accepted / denominator AS accept_rate,"`.
   Expected FAIL: `every dataset computing a rate uses RATE_GUARD -- no bare division`
2. In `ds_load_volume`, change `f"FROM {_t(p, 'aud_table_load')}"` to
   `"FROM raw_vault.qtn_job_request"`.
   Expected FAIL: `no dataset queries raw_vault or business_vault`
3. In `_t`, change `return f"{prefix}{table}"` to `return table`.
   Expected FAIL: `a table prefix reaches every control table reference`
4. In `ds_coverage`, change `f"FROM {g}.meta_vault_model m"` to
   `"FROM meta_vault_model m"`.
   Expected FAIL: `meta_vault_model is always schema-qualified`
5. In `tiles()`, change the counter's `"dataset"` from `"ds_coverage_summary"` to
   `"ds_coverage"`.
   Expected FAIL: `every counter tile reads a single-row dataset`

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/quality.py tests/test_accelerator.py
git commit -m "Define the quality dashboard, with coverage as the headline"
```

---

## Task 2: The emitter

**Files:**
- Create: `tools/emit_quality_dashboard.py`
- Test: `tests/test_accelerator.py` (extend the quality section)

**Interfaces:**
- Consumes: `quality.datasets(table_prefix)`, `quality.tiles()`, `quality.CONTROL_TABLES`.
- Produces:
  - `dashboard(table_prefix: str = "") -> dict` — the full `.lvdash.json` structure.
  - `genie_space() -> dict` — the `.geniespace.json` structure.
  - `render(structure: dict) -> str` — deterministic JSON text, trailing newline.
  - `ARTEFACTS: dict[str, str]` — artefact filename → the prefix it is rendered with.
  - `main() -> None` — writes every artefact under `dashboards/`.

- [ ] **Step 1: Write the failing test**

Append to the quality section of `tests/test_accelerator.py`:

```python
sys.path.insert(0, str(ROOT / "tools"))
import emit_quality_dashboard as _eqd  # noqa: E402

_eqd_dash = _eqd.dashboard()

check("the dashboard has exactly one page and it declares pageType",
      len(_eqd_dash["pages"]) == 1
      and _eqd_dash["pages"][0].get("pageType") == "PAGE_TYPE_CANVAS",
      f"pages={_eqd_dash.get('pages')} -- the service adds pageType on create, so "
      "emitting it keeps the committed artefact canonical")

_eqd_widgets = [e["widget"] for e in _eqd_dash["pages"][0]["layout"]]
check("every tile in the definition becomes exactly one widget",
      len(_eqd_widgets) == len(quality.tiles()),
      f"{len(_eqd_widgets)} widgets for {len(quality.tiles())} tiles")

_eqd_ds_names = {d["name"] for d in _eqd_dash["datasets"]}
_eqd_refs = {w["queries"][0]["query"]["datasetName"] for w in _eqd_widgets}
check("every widget's datasetName resolves to an emitted dataset",
      _eqd_refs <= _eqd_ds_names,
      f"dangling: {sorted(_eqd_refs - _eqd_ds_names)}")

check("every widget field expression is a backtick-quoted identifier",
      all(f["expression"] == f"`{f['name']}`"
          for w in _eqd_widgets for f in w["queries"][0]["query"]["fields"]),
      "a field expression is not a bare quoted identifier; the emitter must not "
      "interpolate anything else into a query")

check("render() is deterministic -- two calls produce identical text",
      _eqd.render(_eqd.dashboard()) == _eqd.render(_eqd.dashboard()),
      "render() is not stable, so the no-op regeneration gate would flap")

check("render() ends with exactly one newline",
      _eqd.render(_eqd_dash).endswith("}\n")
      and not _eqd.render(_eqd_dash).endswith("}\n\n"),
      "a committed artefact without a stable trailing newline diffs on every run")

_eqd_syn = _eqd.dashboard(table_prefix="tst_")
_eqd_a = json.dumps(_eqd_dash, sort_keys=True)
_eqd_b = json.dumps(_eqd_syn, sort_keys=True)
# THE POINT OF THE SYNTHETIC ARTEFACT: it must differ ONLY in table identifiers. If it
# differed in tiles, exercising it would be evidence about a lookalike.
_eqd_normalised = _eqd_b.replace("tst_", "")
check("the synthetic dashboard differs from the business one ONLY in table identifiers",
      _eqd_normalised == _eqd_a,
      "the two renderings diverge beyond the tst_ prefix, so proving a tile on the "
      "synthetic dashboard would prove nothing about the real one")

_eqd_genie = _eqd.genie_space()
check("the Genie space names at least every control table",
      all(t in json.dumps(_eqd_genie) for t in quality.CONTROL_TABLES),
      f"missing from the Genie space: "
      f"{[t for t in quality.CONTROL_TABLES if t not in json.dumps(_eqd_genie)]}")

check("the Genie space carries instructions stating that zero can mean not evaluated",
      "not evaluated" in json.dumps(_eqd_genie).lower(),
      "an engineer asking Genie 'how many rejects' must be told a zero may mean no rule "
      "exists; without that instruction Genie will answer 0 and imply cleanliness")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python tests/test_accelerator.py`
Expected: `ModuleNotFoundError: No module named 'emit_quality_dashboard'`

- [ ] **Step 3: Write the emitter**

Create `tools/emit_quality_dashboard.py`:

```python
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

from accelerator import quality  # noqa: E402

OUT_DIR = ROOT / "dashboards"

# artefact filename -> the table prefix it is rendered with.
ARTEFACTS: dict[str, str] = {
    "quality_silver.lvdash.json": "",
    "quality_silver_synthetic.lvdash.json": "tst_",
}

GENIE_ARTEFACT = "quality_silver.geniespace.json"

_COUNTER_SPEC_VERSION = 2
_TABLE_SPEC_VERSION = 1


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
        spec = {
            "version": _TABLE_SPEC_VERSION,
            "widgetType": "table",
            "encodings": {
                "columns": [{"fieldName": f, "displayName": f}
                            for f in tile["fields"]],
            },
        }
    return {
        "widget": {"name": tile["name"], "queries": [query], "spec": spec},
        "position": tile["position"],
    }


def dashboard(table_prefix: str = "") -> dict:
    """The full .lvdash.json structure.

    pageType is emitted because the service adds PAGE_TYPE_CANVAS on create; emitting it
    keeps the committed file identical to what the workspace considers canonical.
    """
    return {
        "datasets": quality.datasets(table_prefix),
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

    The instructions carry the ONE thing a natural-language interface cannot infer: that a
    zero may mean no rule exists. Without it Genie answers "0 rejects" to "how many rows
    failed", which reads as cleanliness rather than as absence of measurement.
    """
    return {
        "tables": [{"name": t} for t in quality.CONTROL_TABLES],
        "instructions": [
            "A count of zero may mean the rule passed OR that no rule exists. "
            "ref_dq_expectation is the authority: if it declares no current rule for a "
            "table, that table is NOT EVALUATED, not clean. Say so explicitly.",
            "Coverage is counted per target table, not per entity. A satellite has one "
            "physical table per source binding, so 21 entities resolve to 25 tables.",
            "Never answer a quality question from the pipeline event log's data_quality "
            "field. This pipeline declares no SDP expectations, so it reads zero always.",
        ],
        "sample_questions": [
            "Which target tables have no declared expectation?",
            "Which runs opened and never completed?",
            "How many rows were discarded per reason last week?",
        ],
    }


def render(structure: dict) -> str:
    """Deterministic text. sort_keys so key order cannot drift between runs, and exactly
    one trailing newline so the committed file does not diff on whitespace."""
    return json.dumps(structure, indent=2, sort_keys=True) + "\n"


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    for filename, prefix in ARTEFACTS.items():
        (OUT_DIR / filename).write_text(render(dashboard(prefix)), encoding="utf-8")
        print(f"wrote dashboards/{filename}")
    (OUT_DIR / GENIE_ARTEFACT).write_text(render(genie_space()), encoding="utf-8")
    print(f"wrote dashboards/{GENIE_ARTEFACT}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Generate the artefacts and confirm the tests pass**

```bash
python tools/emit_quality_dashboard.py
python tests/test_accelerator.py
```
Expected: three files written; `ALL CHECKS PASSED`.

- [ ] **Step 5: Prove two checks can fail**

1. In `dashboard()`, delete the `"pageType"` line.
   Expected FAIL: `the dashboard has exactly one page and it declares pageType`
2. In `dashboard()`, change the synthetic rendering by making `datasets()` ignore the
   prefix for one dataset — in `quality.datasets`, hard-code
   `"FROM aud_table_load"` in `ds_load_volume` instead of the `_t(p, ...)` call.
   Expected FAIL: `the synthetic dashboard differs from the business one ONLY in table
   identifiers`

Restore after each, regenerate, and confirm green.

- [ ] **Step 6: Commit**

```bash
git add tools/emit_quality_dashboard.py dashboards/ tests/test_accelerator.py
git commit -m "Emit the quality dashboard and Genie space from the definition"
```

---

## Task 3: Bundle resources

**Files:**
- Create: `resources/quality_dashboard.yml`
- Test: `tests/test_accelerator.py` (extend the quality section)

**Interfaces:**
- Consumes: the three artefacts in `dashboards/`.
- Produces: resource keys `quality_silver`, `quality_silver_synthetic` (dashboards) and
  `quality_silver_genie` (Genie space).

- [ ] **Step 1: Write the failing test**

```python
_bundle_yaml = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8")) or {}
_qr_path = ROOT / "resources" / "quality_dashboard.yml"
_qr = yaml.safe_load(_qr_path.read_text(encoding="utf-8")) if _qr_path.is_file() else {}
_qr_res = (_qr.get("resources") or {})
_qr_dash = _qr_res.get("dashboards") or {}
_qr_genie = _qr_res.get("genie_spaces") or {}

# THE GATE THAT MAKES THE EXPOSURE UNREPRESENTABLE. Measured live 27 Aug 2026 on the
# bundle deployment directory: `users CAN_MANAGE inherited=True`. An embedded-credentials
# dashboard deployed there is editable by every workspace user and runs its queries as the
# PUBLISHER, whose privileged-group membership resolves column masks to CLEARTEXT -- so any
# user could repoint a dataset at raw_vault and read masked payroll data with NO grant.
# DEF-43 records that bundle `permissions:` cannot fix it, because the CAN_MANAGE is
# inherited from /Shared and is not revocable on a child.
_qr_root_prefix = ((_bundle_yaml.get("variables") or {})
                   .get("bundle_root_prefix", {}) or {}).get("default", "")
_qr_embedded = [k for k, v in _qr_dash.items() if v.get("embed_credentials") is True]
check("no dashboard embeds credentials while the deployment folder is world-writable",
      not (_qr_embedded and _qr_root_prefix == "/Workspace/Shared"),
      f"{_qr_embedded} embed credentials while bundle_root_prefix is {_qr_root_prefix!r}. "
      f"Every workspace user holds CAN_MANAGE there by inheritance, so they could edit the "
      f"dashboard and read the masked vault in cleartext through the publisher's identity. "
      f"Flip embed_credentials only once bundle_root_prefix names a restricted folder")

check("the business dashboard is declared, with embedded credentials committed OFF",
      "quality_silver" in _qr_dash
      and _qr_dash["quality_silver"].get("embed_credentials") is False,
      "embed_credentials must be committed as false while the deployment folder is "
      "world-writable. Business users therefore cannot use this dashboard yet -- that is "
      "the accepted consequence, recorded in the spec's amendment of 27 Aug")

check("the dashboards use the EXISTING sql_warehouse_id variable, not a second one",
      all(d.get("warehouse_id") == "${var.sql_warehouse_id}"
          for d in _qr_dash.values()),
      "a dashboard names a warehouse variable other than sql_warehouse_id, which "
      "databricks.yml:154 already declares. Two variables for one concept is the "
      "duplicate-authority trap BUSINESS_KINDS and the system-column set already cost "
      "this repo")

check("no new warehouse variable was introduced in databricks.yml",
      "warehouse_id" not in (_bundle_yaml.get("variables") or {}),
      "a bare `warehouse_id` variable was added alongside the existing sql_warehouse_id")

check("the business dashboard sets dataset_catalog from the target's catalog variable",
      _qr_dash.get("quality_silver", {}).get("dataset_catalog")
      == "${var.catalog}",
      "dataset_catalog is not ${var.catalog}; one artefact serves nine targets only "
      "because the catalog is a deploy-time override")

check("the business dashboard sets dataset_schema to the control schema",
      _qr_dash.get("quality_silver", {}).get("dataset_schema")
      == "${var.control_schema}",
      "dataset_schema is not ${var.control_schema}")

check("the GENIE space does NOT embed credentials -- Genie evaluates access per user",
      "embed_credentials" not in (_qr_genie.get("quality_silver_genie") or {}),
      "a Genie space carries embed_credentials; Genie has no such mode and the field "
      "would either be rejected or silently imply an exposure that does not exist")

_qr_files = [d.get("file_path", "") for d in _qr_dash.values()]
check("every declared dashboard points at a committed artefact",
      all((ROOT / "resources" / f).resolve().is_file() for f in _qr_files),
      f"a file_path resolves to nothing: {_qr_files}")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python tests/test_accelerator.py`
Expected: four FAILs, because `resources/quality_dashboard.yml` does not exist.

- [ ] **Step 3: Write the resource file**

Create `resources/quality_dashboard.yml`:

```yaml
# Quality reporting over the control schema.
#
# TWO MECHANISMS, DELIBERATELY DIFFERENT.
#
# The DASHBOARD publishes with embedded credentials, so a viewer sees the data through the
# publisher's identity and needs no UC grant of their own. That is the only way business
# users get quality reporting without a grant on a Phase 6STOP schema -- and it is why the
# datasets query the control schema ONLY. A raw_vault dataset here would put vault rows in
# front of ungranted viewers. tests/test_accelerator.py asserts the datasets stay inside
# the control schema.
#
# The GENIE SPACE embeds nothing. Genie evaluates data access as the ASKING USER, so a
# question about data the user cannot read returns an empty response. It is therefore safe
# to point at the vault as well, and it is the engineers' surface: only the group that
# already holds vault grants sees anything.
#
# ONE ARTEFACT, NINE TARGETS: dataset_catalog overrides the catalog for every dataset, so
# the committed queries stay unqualified and the catalog is resolved per target here.
resources:
  dashboards:
    quality_silver:
      display_name: "Load quality -- ${bundle.target}"
      file_path: ../dashboards/quality_silver.lvdash.json
      parent_path: ${workspace.root_path}/dashboards
      warehouse_id: ${var.sql_warehouse_id}
      # COMMITTED FALSE ON PURPOSE, AND GATED. Measured live on the deployment directory:
      # `users CAN_MANAGE inherited=True`. Every workspace user can edit what is deployed
      # there, and an embedded-credentials dashboard runs its queries as the PUBLISHER --
      # who holds the privileged group, so masks resolve to CLEARTEXT. A user could repoint
      # a dataset at raw_vault and read masked payroll data with no grant at all. DEF-43
      # records that bundle `permissions:` cannot fix it: the CAN_MANAGE is inherited from
      # /Shared and is not revocable on a child. tests/test_accelerator.py FAILS THE BUILD
      # if this is true while bundle_root_prefix is still /Workspace/Shared.
      embed_credentials: false
      dataset_catalog: ${var.catalog}
      dataset_schema: ${var.control_schema}

    # The mutation test for the tiles. Reads tst_ tables only, and is deployed only while
    # the synthetic objects exist. See governance/control_test_objects.sql.
    quality_silver_synthetic:
      display_name: "Load quality (SYNTHETIC -- delete after testing) -- ${bundle.target}"
      file_path: ../dashboards/quality_silver_synthetic.lvdash.json
      parent_path: ${workspace.root_path}/dashboards
      warehouse_id: ${var.sql_warehouse_id}
      # False for the same reason as above, and doubly so: this one reads fabricated numbers.
      embed_credentials: false
      dataset_catalog: ${var.catalog}
      dataset_schema: ${var.control_schema}

  genie_spaces:
    quality_silver_genie:
      title: "Vault load quality -- ${bundle.target}"
      description: >-
        Load quality and reject investigation over the control schema and the vault.
        Access is evaluated per user, so this shows nothing to anyone without a grant.
      file_path: ../dashboards/quality_silver.geniespace.json
      parent_path: ${workspace.root_path}/dashboards
      warehouse_id: ${var.sql_warehouse_id}
```

- [ ] **Step 4: Set the EXISTING warehouse variable — do not add a new one**

`databricks.yml:154` already declares `sql_warehouse_id` ("Serverless SQL warehouse for the
governance and conformance tasks", `default: ""`). **Do not add a second warehouse
variable.** A duplicate authority for one concept is the trap this repo has been bitten by
twice — `BUSINESS_KINDS` and the system-column set — and `metadata/key_composition.json`
exists to catch that class.

`sql_warehouse_id` is currently unset for every target. Set it for `usnc_tds` only, under
that target's `variables:` block (the shape to follow is the `vault_privileged_group` line
already there):

```yaml
      # Read from the workspace 27 Aug 2026 via `aitools get-default-warehouse`.
      sql_warehouse_id: c7e665f6d5b96d55
```

Leave every other target unset. A wrong warehouse runs queries against the wrong lake and
fails as an EMPTY DASHBOARD rather than an error, so an unset value that fails the deploy
loudly is the safer default.

- [ ] **Step 5: Verify the bundle still validates and the tests pass**

```bash
python tests/test_accelerator.py
python verify_repo.py
```
Expected: `ALL CHECKS PASSED`; `VERIFICATION PASSED`.

Do NOT run `databricks bundle validate` — it requires a workspace and this plan is offline.

- [ ] **Step 6: Prove the new checks can fail**

Every check you add must be proven able to fail. For each: apply the mutation, run
`/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`, confirm the named
check reports **FAIL** while the suite still completes, then restore exactly.

1. Set `embed_credentials: true` on `quality_silver`.
   Expected FAIL: **both** `no dashboard embeds credentials while the deployment folder is
   world-writable` and `the business dashboard is declared, with embedded credentials
   committed OFF`. This is the important one — it is the gate standing between this config
   and a cleartext route into the masked vault.
2. Change `quality_silver`'s `warehouse_id` to `${var.warehouse_id}`.
   Expected FAIL: `the dashboards use the EXISTING sql_warehouse_id variable, not a second
   one`
3. Add a `warehouse_id:` entry to `databricks.yml`'s `variables:` block.
   Expected FAIL: `no new warehouse variable was introduced in databricks.yml`
4. Change `dataset_schema` on `quality_silver` to `${var.vault_schema}`.
   Expected FAIL: `the business dashboard sets dataset_schema to the control schema`
5. Add `embed_credentials: true` to the Genie space entry.
   Expected FAIL: `the GENIE space does NOT embed credentials -- Genie evaluates access per
   user`
6. Point a dashboard's `file_path` at a non-existent file.
   Expected FAIL: `every declared dashboard points at a committed artefact`

- [ ] **Step 7: Commit**

```bash
git add resources/quality_dashboard.yml databricks.yml tests/test_accelerator.py
git commit -m "Declare the quality dashboard and Genie space as bundle resources"
```

---

## Task 4: The regeneration and boundary gates in verify_repo

**Files:**
- Modify: `verify_repo.py` — append a new section after the data-contract gate
  (which ends around line 1975; place this after it)

**Interfaces:**
- Consumes: `emit_quality_dashboard.dashboard`, `.genie_space`, `.render`, `.ARTEFACTS`,
  `.GENIE_ARTEFACT`; `accelerator.quality.CONTROL_TABLES`.
- Produces: nothing importable.

- [ ] **Step 1: Write the gate**

Append to `verify_repo.py`:

```python
# --------------------------------------------------------------------------- #
print("\n[quality] the dashboard artefacts match the definition")

import emit_quality_dashboard as _eqd  # noqa: E402

from accelerator import quality as _ql  # noqa: E402

_ql_dir = ROOT / "dashboards"

# ANTI-VACUITY, the same guard the data-contract gate carries: an empty ARTEFACTS mapping
# would make every staleness check below compare nothing and still report green.
check("emit_quality_dashboard.ARTEFACTS is non-empty and names both renderings",
      set(_eqd.ARTEFACTS) == {"quality_silver.lvdash.json",
                              "quality_silver_synthetic.lvdash.json"},
      f"got {sorted(_eqd.ARTEFACTS)} -- an empty or short mapping lets the staleness "
      f"checks below pass having compared nothing")

_ql_stale = []
for _fn, _prefix in _eqd.ARTEFACTS.items():
    _p = _ql_dir / _fn
    try:
        _want = _eqd.render(_eqd.dashboard(_prefix))
    except Exception as _exc:  # noqa: BLE001
        # CAUGHT for the same reason the contract gate catches: an exception escaping this
        # module-level loop aborts verify_repo.py and turns every later check ABSENT,
        # which is worse than one red check carrying the message.
        _ql_stale.append(f"{_fn}: {type(_exc).__name__}: {_exc}")
        continue
    if not _p.is_file() or _p.read_text(encoding="utf-8") != _want:
        _ql_stale.append(_fn)

_ql_gp = _ql_dir / _eqd.GENIE_ARTEFACT
if not _ql_gp.is_file() or _ql_gp.read_text(encoding="utf-8") != _eqd.render(
        _eqd.genie_space()):
    _ql_stale.append(_eqd.GENIE_ARTEFACT)

check("every committed quality artefact matches what the definition generates",
      not _ql_stale,
      f"stale: {_ql_stale} -- run tools/emit_quality_dashboard.py and review the diff")

# THE BOUNDARY GATES, read off the COMMITTED files rather than off dashboard(). The
# staleness check above proves committed == generated; these prove the committed text obeys
# the governance boundary, so a hand-edit that slipped past regeneration is still caught.
_ql_business = _ql_dir / "quality_silver.lvdash.json"
_ql_text = _ql_business.read_text(encoding="utf-8") if _ql_business.is_file() else ""

check("the committed business dashboard reads no vault schema",
      "raw_vault" not in _ql_text and "business_vault" not in _ql_text,
      "the committed artefact references a vault schema. It publishes with embedded "
      "credentials, so its viewers hold no UC grant and this would show them vault rows")

check("the committed business dashboard reads no data_quality field",
      "data_quality" not in _ql_text,
      "the committed artefact reads the event log's data_quality. DEF-18 means this "
      "pipeline declares no SDP expectations, so that field is 0 for ever and the tile "
      "would be confidently wrong")

check("the committed business dashboard names no tst_ table",
      "tst_" not in _ql_text,
      "the business artefact reads a synthetic table -- the two renderings have been "
      "crossed, and business users would see fabricated numbers")

_ql_syn = _ql_dir / "quality_silver_synthetic.lvdash.json"
_ql_syn_text = _ql_syn.read_text(encoding="utf-8") if _ql_syn.is_file() else ""
_ql_bare = [t for t in _ql.CONTROL_TABLES
            if f'"{t}' in _ql_syn_text or f" {t}" in _ql_syn_text]
check("the committed synthetic dashboard names no UNPREFIXED control table",
      not _ql_bare,
      f"{_ql_bare} appear without the tst_ prefix, so exercising the synthetic dashboard "
      f"would read and report the REAL audit")

# Every table an artefact queries must be a table the DDL creates. Otherwise a dataset can
# name something that never existed and the dashboard fails at view time, in front of a user.
_ql_ddl = (ROOT / "governance" / "control_objects.sql").read_text(encoding="utf-8")
_ql_undeclared = [t for t in _ql.CONTROL_TABLES if t not in _ql_ddl]
check("every control table the definition names is created by control_objects.sql",
      not _ql_undeclared,
      f"{_ql_undeclared} are queried but never created -- the tile would fail at view "
      f"time, in front of a user")
```

- [ ] **Step 2: Run to verify it passes**

Run: `python verify_repo.py`
Expected: `VERIFICATION PASSED`, with a check count 8 higher than before this task.

- [ ] **Step 3: Prove three of the gates can fail**

1. Edit `dashboards/quality_silver.lvdash.json` by hand — change any `displayName`.
   Expected FAIL: `every committed quality artefact matches what the definition generates`.
   Regenerate to restore.
2. Edit `dashboards/quality_silver.lvdash.json`, replacing one `aud_table_load` with
   `raw_vault.qtn_job_request`.
   Expected FAIL: **both** `the committed business dashboard reads no vault schema` and the
   staleness check. Regenerate to restore.
3. In `src/accelerator/quality.py`, add `"tst_never_created"` to `CONTROL_TABLES`.
   Expected FAIL: `every control table the definition names is created by
   control_objects.sql`. Restore.

- [ ] **Step 4: Commit**

```bash
git add verify_repo.py
git commit -m "Gate the quality artefacts against the definition and the boundary"
```

---

## Task 5: The synthetic objects and their seed

**Files:**
- Create: `governance/control_test_objects.sql`
- Create: `governance/control_test_objects_drop.sql`
- Test: `tests/test_accelerator.py` (extend the quality section)

**Interfaces:**
- Consumes: `quality.CONTROL_TABLES`.
- Produces: six `tst_`-prefixed tables and seed rows; a drop script.

- [ ] **Step 1: Write the failing test**

```python
_ts_path = ROOT / "governance" / "control_test_objects.sql"
_ts = _ts_path.read_text(encoding="utf-8") if _ts_path.is_file() else ""
_ts_drop_path = ROOT / "governance" / "control_test_objects_drop.sql"
_ts_drop = _ts_drop_path.read_text(encoding="utf-8") if _ts_drop_path.is_file() else ""

check("the test DDL creates a tst_ twin for every control table",
      all(f"tst_{t}" in _ts for t in quality.CONTROL_TABLES),
      f"missing: {[t for t in quality.CONTROL_TABLES if f'tst_{t}' not in _ts]}")

check("no tst_ table is append-only -- they must be droppable",
      "appendOnly" not in _ts,
      "a tst_ table declares delta.appendOnly, so its synthetic rows could never be "
      "removed and the point of using a disposable prefix is lost")

check("every tst_ table declares hfig.control_object",
      _ts.count("'hfig.control_object' = 'true'") >= len(quality.CONTROL_TABLES),
      "a tst_ table omits the marker. schema_grant_check.misplaced_control_objects "
      "fails the build on an unmarked table in the control schema, so the synthetic "
      "exercise would turn a gate red")

check("no tst_ table name would be swept by append_only_check",
      all(not f"tst_{t}".startswith(("ctl_", "ref_", "aud_"))
          for t in quality.CONTROL_TABLES),
      "a tst_ name starts with a CONTROL_PREFIXES value, so append_only_check would "
      "sweep it and demand append-only history")

check("the test DDL seeds rows into every tst_ table",
      all(f"INSERT INTO" in _ts and f"tst_{t}" in _ts
          for t in quality.CONTROL_TABLES),
      "a tst_ table is created but never seeded, so the tiles reading it stay unproven")

check("the drop script drops every tst_ table the DDL creates",
      all(f"tst_{t}" in _ts_drop for t in quality.CONTROL_TABLES),
      f"the drop script misses: "
      f"{[t for t in quality.CONTROL_TABLES if f'tst_{t}' not in _ts_drop]} -- a "
      f"leftover synthetic table is a permanent lie in the control schema")

check("the drop script drops NOTHING that is not tst_ prefixed",
      all(line.strip().startswith("--") or "tst_" in line or not line.strip()
          for line in _ts_drop.splitlines() if "DROP" in line.upper()),
      "the drop script names an object without the tst_ prefix; a teardown that can "
      "reach a real control table is a teardown that will eventually delete the audit")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python tests/test_accelerator.py`
Expected: FAILs for all seven checks, because neither file exists.

- [ ] **Step 3: Write the DDL**

Create `governance/control_test_objects.sql`:

```sql
-- SYNTHETIC OBJECTS FOR PROVING THE QUALITY DASHBOARD'S TILES. NOT PART OF THE ESTATE.
--
-- Applied ON DEMAND and dropped afterwards with control_test_objects_drop.sql. The
-- standing create_control_objects task must never apply this file.
--
-- WHY tst_ AND NOT A SEPARATE SCHEMA. The prefix is chosen so three existing gates behave
-- correctly without being weakened:
--
--   * append_only_check selects on CONTROL_PREFIXES = ('ctl_', 'ref_', 'aud_'). tst_ is
--     outside it, so these tables are never swept and may be dropped. That is the whole
--     point: the four real audit tables are append-only, so a synthetic row written into
--     one of them could never be removed, and a DELETE to tidy up would put the table
--     outside append-only for ever.
--   * schema_grant_check.misplaced_control_objects fails the build on any table in the
--     control schema NOT declaring hfig.control_object. So these declare it. Nothing ties
--     the marked set to control_objects.sql -- the gate's message advises declaring there
--     but asserts no such thing -- which is why a second file is legitimate.
--   * audit_completeness_check reads aud_load_run, aud_table_load and aud_table_discard by
--     EXACT NAME, and loop1_reconciliation reads ctl_approval_manifest by exact name.
--     No tst_ name matches, so the synthetic rows are invisible to both.
--
-- NOTHING SYNTHETIC GOES IN ref_dq_expectation. The pipeline reads that table to build its
-- expectations, so seeding it is not a test -- it is a production change that would
-- evaluate fabricated rules against real data. tst_ref_dq_expectation is a separate table
-- and the pipeline never reads it.
--
-- No semicolon may appear inside a COMMENT literal: the statement splitter cuts on ';'
-- regardless of quoting.

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_ctl_approval_manifest (
  manifest_id     STRING  NOT NULL,
  approved_count  BIGINT  NOT NULL,
  source_system   STRING,
  approved_at     TIMESTAMP,
  approved_by     STRING
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_ref_dq_expectation (
  dataset     STRING  NOT NULL,
  rule_name   STRING  NOT NULL,
  rule_sql    STRING  NOT NULL,
  is_current  BOOLEAN NOT NULL,
  severity    STRING  NOT NULL
)
COMMENT 'SYNTHETIC -- the pipeline never reads this, unlike ref_dq_expectation'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_load (
  job_run_id          STRING     NOT NULL,
  pipeline_update_id  STRING,
  table_name          STRING     NOT NULL,
  written_by          STRING     NOT NULL,
  staged              BIGINT     NOT NULL,
  accepted            BIGINT     NOT NULL,
  recorded_at         TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_discard (
  job_run_id      STRING     NOT NULL,
  table_name      STRING     NOT NULL,
  discard_reason  STRING     NOT NULL,
  discarded       BIGINT     NOT NULL,
  recorded_at     TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_aud_load_run (
  job_run_id      STRING     NOT NULL,
  phase           STRING     NOT NULL,
  target          STRING     NOT NULL,
  active_sources  STRING,
  recorded_at     TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_ctl_quarantine_superseded (
  manifest_id       STRING     NOT NULL,
  table_name        STRING     NOT NULL,
  reject_digest     STRING     NOT NULL,
  rulebook_version  STRING     NOT NULL,
  superseded_by     STRING     NOT NULL,
  reason            STRING,
  recorded_at       TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

-- SEED. Shaped so EVERY tile renders a non-zero value, and so the two states the spec
-- cares about are both present: a table with a rule and discards, and a table with no
-- rule at all which must render as 'not evaluated' rather than as 100% clean.

INSERT INTO `${catalog}`.`${control_schema}`.tst_ref_dq_expectation VALUES
  ('nhl_general_journal_line', 'debit_credit_balanced',
   'debitamt IS NOT NULL AND crdtamnt IS NOT NULL', true, 'drop'),
  ('nhl_general_journal_line', 'positive_amount', 'debitamt >= 0', true, 'drop'),
  ('hub_job_request', 'business_key_present', 'jobrequestid IS NOT NULL', true, 'drop');

INSERT INTO `${catalog}`.`${control_schema}`.tst_aud_table_load VALUES
  ('SYNTH-001', NULL, 'nhl_general_journal_line', 'load_hubs.py',
   1000, 940, TIMESTAMP '2026-08-27 09:00:00'),
  ('SYNTH-001', NULL, 'hub_job_request', 'load_hubs.py',
   500, 500, TIMESTAMP '2026-08-27 09:01:00'),
  ('SYNTH-002', NULL, 'sat_job_request_details_bullhorn_eu', 'load_satellites.py',
   250, 200, TIMESTAMP '2026-08-27 10:00:00');

INSERT INTO `${catalog}`.`${control_schema}`.tst_aud_table_discard VALUES
  ('SYNTH-001', 'nhl_general_journal_line', 'debit_credit_balanced',
   45, TIMESTAMP '2026-08-27 09:00:00'),
  ('SYNTH-001', 'nhl_general_journal_line', 'positive_amount',
   15, TIMESTAMP '2026-08-27 09:00:00'),
  ('SYNTH-002', 'sat_job_request_details_bullhorn_eu', 'hashdiff_null',
   50, TIMESTAMP '2026-08-27 10:00:00');

-- SYNTH-002 is opened and never completed, so the incomplete-runs tile has a row.
INSERT INTO `${catalog}`.`${control_schema}`.tst_aud_load_run VALUES
  ('SYNTH-001', 'opened', 'usnc_tds', '', TIMESTAMP '2026-08-27 08:59:00'),
  ('SYNTH-001', 'completed', 'usnc_tds', '', TIMESTAMP '2026-08-27 09:05:00'),
  ('SYNTH-002', 'opened', 'usnc_tds', '', TIMESTAMP '2026-08-27 09:59:00');

INSERT INTO `${catalog}`.`${control_schema}`.tst_ctl_approval_manifest VALUES
  ('SYNTH-MANIFEST-1', 1000, 'GP_US', TIMESTAMP '2026-08-27 08:50:00', 'synthetic');

INSERT INTO `${catalog}`.`${control_schema}`.tst_ctl_quarantine_superseded VALUES
  ('SYNTH-MANIFEST-1', 'qtn_general_journal_line', 'abc123', '1.0.0',
   'SYNTH-002', 'corrected at source', TIMESTAMP '2026-08-27 10:30:00');
```

- [ ] **Step 4: Write the drop script**

Create `governance/control_test_objects_drop.sql`:

```sql
-- TEARDOWN for control_test_objects.sql.
--
-- Every statement names a tst_ object. tests/test_accelerator.py asserts that no DROP in
-- this file names anything without the prefix: a teardown able to reach a real control
-- table is a teardown that will eventually delete the audit.
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_ctl_approval_manifest;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_ref_dq_expectation;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_load;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_discard;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_aud_load_run;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_ctl_quarantine_superseded;
```

- [ ] **Step 5: Run to verify the tests pass**

Run: `python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 6: Prove two checks can fail**

1. Add `TBLPROPERTIES ('delta.appendOnly' = 'true')` to `tst_aud_table_load`.
   Expected FAIL: `no tst_ table is append-only -- they must be droppable`. Restore.
2. In the drop script, add
   `DROP TABLE IF EXISTS \`${catalog}\`.\`${control_schema}\`.aud_table_load;`
   Expected FAIL: `the drop script drops NOTHING that is not tst_ prefixed`. **Remove it.**

- [ ] **Step 7: Commit**

```bash
git add governance/control_test_objects.sql governance/control_test_objects_drop.sql \
        tests/test_accelerator.py
git commit -m "Add the disposable tst_ objects that prove every tile can render"
```

---

## Task 6: The gate that refuses a leftover synthetic object

**Files:**
- Modify: `checks/schema_grant_check.py` — add one pure function beside
  `misplaced_control_objects` (line 171) and wire it into `main()`
- Test: `tests/test_accelerator.py` (extend the quality section)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `schema_grant_check.synthetic_objects(rows, allow: bool) -> list[str]`, where
  `rows` is an iterable of `(schema, table)` tuples.

**Why this is a task and not a line.** A `tst_` table that survives the exercise is a
permanent lie in the control schema: it looks like load control, it carries the
`hfig.control_object` marker so no existing gate objects, and its numbers are fabricated.
Nothing currently notices. This gate makes leaving one behind fail the build.

It follows the file's established shape: the decision is a **pure, Spark-free function**
over rows, exactly like `offending()`, `unauthorised_table_readers()`,
`undeclared_schemas()` and `misplaced_control_objects()`, so it is testable offline with
fabricated rows while `main()` feeds it live ones.

- [ ] **Step 1: Write the failing test**

Append to the quality section of `tests/test_accelerator.py`:

```python
sys.path.insert(0, str(ROOT / "checks"))
import schema_grant_check as _sgc_q  # noqa: E402

_sq_rows = [("control", "aud_table_load"), ("control", "tst_aud_table_load")]

check("synthetic_objects() reports a tst_ table when it is not allowed",
      len(_sgc_q.synthetic_objects(_sq_rows, allow=False)) == 1,
      f"got {_sgc_q.synthetic_objects(_sq_rows, allow=False)} -- a leftover tst_ table "
      f"is fabricated data wearing the control_object marker, and no other gate sees it")

check("synthetic_objects() names the offending table in its message",
      "tst_aud_table_load" in " ".join(
          _sgc_q.synthetic_objects(_sq_rows, allow=False)),
      "the message does not name the table, so an operator cannot act on it")

check("synthetic_objects() is silent when the exercise is explicitly allowed",
      _sgc_q.synthetic_objects(_sq_rows, allow=True) == [],
      "the gate fires during a sanctioned synthetic exercise, which would make the "
      "flag useless and train people to ignore the gate")

check("synthetic_objects() reports NOTHING when no tst_ table exists",
      _sgc_q.synthetic_objects([("control", "aud_table_load")], allow=False) == [],
      "the gate fires on a clean schema -- a gate that cries wolf gets suppressed")

check("synthetic_objects() is pure -- no Spark, no I/O",
      _sgc_q.synthetic_objects([], allow=False) == [],
      "an empty input raised or returned non-empty, so the function is not pure over "
      "its rows the way the other four decision functions in this file are")
```

- [ ] **Step 2: Run to verify it fails**

Run: `python tests/test_accelerator.py`
Expected: `AttributeError: module 'schema_grant_check' has no attribute
'synthetic_objects'`

- [ ] **Step 3: Add the function**

Insert into `checks/schema_grant_check.py`, immediately after
`misplaced_control_objects()`:

```python
SYNTHETIC_PREFIX = "tst_"


def synthetic_objects(rows, allow: bool) -> list[str]:
    """Problems for every synthetic table left behind after a dashboard exercise.

    governance/control_test_objects.sql creates tst_ tables holding FABRICATED numbers so
    the quality dashboard's tiles can be proven to render. They are deliberately built to
    slip past the other gates -- outside CONTROL_PREFIXES so append_only_check does not
    sweep them, and carrying hfig.control_object so misplaced_control_objects passes -- and
    the consequence is that NOTHING otherwise notices one that is never dropped.

    A survivor is worse than an undeclared table. It sits in the control schema, wears the
    control_object marker, and reports invented discards. Someone reading the audit a month
    later has no way to tell it from a real load.

    Pure and Spark-free, like offending() and undeclared_schemas(), so the failing case is
    testable without a workspace.

    `allow` is the operator saying an exercise is in progress. It exists so a sanctioned
    run does not have to disable the gate wholesale -- a gate people learn to switch off
    is a gate that is off when it matters.
    """
    if allow:
        return []
    return [
        f"TABLE {schema}.{table}: a synthetic object from "
        f"governance/control_test_objects.sql was never dropped. It holds FABRICATED "
        f"numbers, sits in the control schema, and carries hfig.control_object, so no "
        f"other gate objects to it. Apply "
        f"governance/control_test_objects_drop.sql, or pass --allow-test-objects if an "
        f"exercise is genuinely in progress."
        for schema, table in sorted(rows)
        if table.lower().startswith(SYNTHETIC_PREFIX)
    ]
```

- [ ] **Step 4: Wire it into `main()`**

Add the flag beside the existing `--allow-table-select` argument:

```python
    ap.add_argument("--allow-test-objects", action="store_true",
                    help="permit tst_ synthetic objects; use only while a dashboard "
                         "exercise is actually in progress")
```

And call it immediately after the `misplaced_control_objects` call, which is at
`checks/schema_grant_check.py:429`, inside the same block that binds `_co` on line 428:

```python
            _co = control_object_rows(spark, args.catalog, args.control_object_schema)
            problems += misplaced_control_objects(_co, args.control_schema)
            problems += synthetic_objects(
                [(_s, _tbl) for _s, _tbl, _marker in _co], args.allow_test_objects)
```

Read those two lines before editing — the variable is `_co`, not a name of your own, and
`control_object_rows()` yields `(schema, table, has_marker)` triples. The marker is dropped
here deliberately: this gate keys on the NAME, precisely because a survivor DOES carry the
marker and so slips past `misplaced_control_objects`.

- [ ] **Step 5: Run to verify it passes**

```bash
python tests/test_accelerator.py
python verify_repo.py
```
Expected: `ALL CHECKS PASSED`; `VERIFICATION PASSED`.

- [ ] **Step 6: Prove the gate can fail**

Change `if table.lower().startswith(SYNTHETIC_PREFIX)` to
`if table.lower().startswith("zzz_")`.
Expected FAIL: `synthetic_objects() reports a tst_ table when it is not allowed` **and**
`synthetic_objects() names the offending table in its message`. Restore.

- [ ] **Step 7: Commit**

```bash
git add checks/schema_grant_check.py tests/test_accelerator.py
git commit -m "Fail the build on a synthetic object left behind after an exercise"
```

---

## Task 7: Document the exercise and record the open items

**Files:**
- Modify: `docs/superpowers/OPEN_ITEMS.md`
- Create: `docs/quality_dashboard_runbook.md`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing importable.

- [ ] **Step 1: Write the runbook**

Create `docs/quality_dashboard_runbook.md`:

```markdown
# Quality dashboard: how to exercise it, and how to take it down

## What it shows today, and why that looks wrong

Near-total absence of measurement. That is correct, not broken:

* `ref_dq_expectation` holds **0 rows**, so no business rule has ever been evaluated.
* All nine `qtn_` twins hold **0 rows**. The two compiled-in key-safety rules genuinely
  passed on 4.7M loaded rows, which is the only real quality signal the estate has.
* `aud_table_load` holds **0 rows**: the loaders pass `--control-schema` already but have
  not run since that instrumentation landed.
* Three of the six control tables do not exist in the lake yet.

The coverage tile therefore reads `0 of 25 tables`. A pass-rate dashboard would have read
`100%` and meant nothing.

## Making it show real data

1. Run `create_control_objects` — three tables are missing.
2. Run `publish_model_metadata` — `governance.meta_vault_model` is the coverage
   denominator and does not exist yet.
3. Run a real load, so the loaders write `aud_table_load` and `aud_table_discard`.
4. Declare expectations in `ref_dq_expectation`. Until then only key safety is measured.

## Exercising the tiles with synthetic data

Every tile is unproven until it has rendered a non-zero value. To prove them:

1. Apply `governance/control_test_objects.sql` against the target's catalog and control
   schema. It creates six `tst_` tables and seeds them.
2. Deploy, and open the dashboard named
   `Load quality (SYNTHETIC -- delete after testing)`. You will need your own UC grant on
   the control schema to see anything: embedded credentials are gated off (see below).
3. Confirm every tile renders. In particular confirm that
   `sat_job_request_details_bullhorn_eu` — which the seed gives discards but **no rule** —
   shows `not evaluated` and not a pass rate.
4. Apply `governance/control_test_objects_drop.sql`.
5. Remove `quality_silver_synthetic` from `resources/quality_dashboard.yml` and redeploy,
   or the bundle will recreate a dashboard whose tables no longer exist.

**Never seed `ref_dq_expectation` itself.** The pipeline reads it to build expectations, so
a fabricated rule there is a production change that would evaluate against real data. That
is why `tst_ref_dq_expectation` exists as a separate table.

## What the dashboard may never do

It queries the control schema only, and `verify_repo.py` fails the build if the committed
artefact names a vault schema.

**Embedded credentials are committed OFF, and gated.** The bundle deployment folder is
world-writable (`users CAN_MANAGE inherited=True`, measured 27 Aug 2026), so an
embedded-credentials dashboard there would be editable by every workspace user and would run
its queries as the publisher — whose privileged-group membership resolves column masks to
cleartext. A gate fails the build if `embed_credentials` is true while `bundle_root_prefix`
is still `/Workspace/Shared`. **Until a restricted folder exists, business users cannot use
this dashboard**; engineers use the Genie space, which evaluates access per user.

Row-level investigation belongs in the Genie space, which evaluates access per user and so
shows nothing to anyone without a grant.
```

- [ ] **Step 2: Record it in OPEN_ITEMS**

Insert a section immediately before the line
`## MERGED 27 Aug: a data contract per target, generated from the model`:

```markdown
## MERGED 27 Aug: quality reporting over the silver control schema

Spec `specs/2026-08-27-quality-dashboard-design.md`, plan
`plans/2026-08-27-quality-dashboard.md`. Runbook `docs/quality_dashboard_runbook.md`.
Silver only; bronze and gold have no control schema and no signal.

**Nothing is deployed.** The artefacts are generated and gated; no dashboard, Genie space
or `tst_` table exists in any lake.

### The finding that shaped it

Three sources look like quality signal and are not. The SDP event log's `data_quality`
payload reads `dropped_records: 0` across all 226 events that carry one and holds no
`expectations` array, because DEF-18 means this pipeline declares **no SDP expectations at
all** — quality is a hand-rolled filter plus a parallel quarantine flow. That field will
read zero for ever. `ref_dq_expectation` holds no rules, so every quality claim but key
safety is vacuous. Only the nine empty `qtn_` twins are real: key safety genuinely passed
on 4.7M rows.

So coverage is the headline, not pass rate, and no tile may compute a rate over a zero
denominator. `checks/audit_completeness_check.py:165` already set that precedent by
printing `GATE NOT EVALUATED` rather than passing.

### Two things to know before touching it

**Coverage counts target tables, not entities.** 21 entities resolve to 25 physical
tables. Keying on entities would call an entity covered when one of four bindings had a
rule — the defect that reached nine published data contracts before review caught it.

**The dashboard and the Genie space have deliberately different exposure.** A dashboard
published with embedded credentials shows data to viewers holding no UC grant, which is
why it queries the control schema ONLY and why a gate fails the build if the committed
artefact names a vault schema. Genie evaluates access per user, so it may point at the
vault and still expose nothing.

### Synthetic data is the mutation test

Every tile is unproven while the estate reports zeros. `tst_` tables in the control schema
carry the seed: outside `CONTROL_PREFIXES` so `append_only_check` cannot sweep them and
they stay droppable, marked `hfig.control_object` so `misplaced_control_objects` passes,
and invisible to `audit_completeness_check` and `loop1_reconciliation`, which read their
tables by exact name. **Never seed `ref_dq_expectation` itself** — the pipeline reads it.
```

- [ ] **Step 3: Verify both suites are green**

```bash
python tests/test_accelerator.py
python verify_repo.py
```
Expected: `ALL CHECKS PASSED`; `VERIFICATION PASSED`.

- [ ] **Step 4: Commit**

```bash
git add docs/quality_dashboard_runbook.md docs/superpowers/OPEN_ITEMS.md
git commit -m "Document the quality dashboard exercise and its prerequisites"
```

---

## Notes for the executor

* **The spec is the authority.** Where this plan and the spec disagree, the spec wins.
* **Spec §8's eighth gate needs no task.** "The emitter is covered by the same file-level
  invariants as the other tools" is satisfied automatically: `verify_repo.py` sweeps
  `py_files` three times — the parse check in section [1], the "builds no hash of its own"
  check, and the f-string floor check — and `ROOT.rglob("*.py")` picks up anything new
  under `tools/`. Do NOT add the emitter to `REQUIRED_FILES`:
  `tools/emit_data_contract.py` is not there either, so that is the established pattern for
  an emitter rather than an omission.
* **Do not deploy.** No task runs `databricks bundle deploy`, `bundle run`, or any SQL
  against a workspace. Task 5 writes the DDL; applying it is a separate consented act.
* **Do not run `databricks bundle validate`** — it needs a workspace.
* **`warehouse_id` for `usnc_tds` is `c7e665f6d5b96d55`**, read from the workspace on
  27 Aug 2026. Other targets have no value yet; leave them unset rather than guessing, and
  the deploy will fail loudly on the target rather than silently querying the wrong lake.
