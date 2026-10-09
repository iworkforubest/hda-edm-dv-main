# Data Contract Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish what `metadata/entities/*.yml` already declares as a datacontract.com-shaped YAML per target, generated and asserted so it cannot drift from the model it describes.

**Architecture:** A pure emitter reads the model and writes one contract per bundle target. Per-column classification is *derived* from `entity.sensitivity` plus the per-column mask bindings — no new model field. `severity` is added to `ref_dq_expectation`, the one thing genuinely missing. A gate asserts regeneration is a no-op, on the precedent of `metadata/key_composition.json`.

**Tech Stack:** Python 3.11/3.13, PyYAML, Declarative Automation Bundles. No new dependencies, no Spark.

**Spec:** `docs/superpowers/specs/2026-08-27-data-contract-export-design.md`

## Global Constraints

- **This repo's tests are not pytest.** Two suites, run as plain scripts: `uv run --frozen python tests/test_accelerator.py` and `uv run --frozen python verify_repo.py`. Add assertions as `check("name", condition, "detail")` calls, immediately BEFORE the final `print("\n" + "=" * 62)` block. Never introduce pytest.
- **Every new check must be proven able to fail.** Mutate the code it guards, confirm it reports FAIL and **not ABSENT** — an ABSENT means the suite aborted and every later check silently stopped — restore, and record the mutation in the commit message. This repo has shipped five checks that could never fail, and the last two plans produced seven more between them.
- **Never use `str.index`, or `[0]`/`[i+1]` indexing that can run past the end, inside a check.** All raise and abort the suite. Use `.find()` compared against `-1`, or the guarded `(result or [""])[0]`.
- **Ordering assertions on source text use the chained `-1 < src.find(A) < src.find(B)` form.** Without the leading `-1 <` the assertion is TRUE when A is absent.
- **No semicolon anywhere in a `.sql` file except as a statement terminator, including inside `COMMENT '...'` string literals.** `apply_governance.statements()` splits on `;` regardless of quoting. One task edits a `COMMENT` literal.
- **`src/accelerator/hashing.py` is a RATIFIED rulebook. Do not modify it.** `RULEBOOK_VERSION` is `1.0.0`; a change re-keys 13.2M rows. Import from it only.
- **No test may assert an absolute check count** — it differs between checkouts.
- Schema and catalog names come from bundle variables, never literals.
- Do not run anything against a live Databricks workspace. Everything here is offline.

---

## The constraint that shapes this plan: types are not fully knowable offline

Measured before writing:

```
hashing.key_type_sql()      -> "BINARY"        known offline
naming.SYSTEM_COLUMN_TYPES  -> NOT DECLARED    no offline source
entity payload types        -> not in the model at all
src.cast                    -> {'debitamt': 'DECIMAL(18,2)', 'crdtamnt': 'DECIMAL(18,2)'}
factory._derived_schema_ddl -> requires Spark
```

So a fully-typed contract needs a workspace, and a workspace-dependent artefact cannot be
regenerated in CI — which would kill the no-op gate that is the whole point.

**Ruling, and it is the plan's central trade:** the emitter is **offline and honest**. It emits
the types the model genuinely knows — `BINARY(32)` for hash keys, declared types for system
columns (Task 1 adds them, they are fixed and knowable), and the `cast` type where a binding
declares one. Every other payload column is emitted with `type: source-derived` and a comment
saying so.

A contract that says "I do not know this type" is worth more than one that guesses, and the
alternative — omitting the column — would understate the schema. **Declaring payload types in
`metadata/entities/*.yml` is the natural follow-up that would complete this**, and it is out of
scope here because it is a model change with its own review surface.

---

## File Structure

| file | responsibility |
|---|---|
| `src/accelerator/naming.py` | **+** `SYSTEM_COLUMN_TYPES` — the fixed types of the seven system columns |
| `src/accelerator/contract.py` | **new** — pure: derive classification, PK, FK, clustering, types. No I/O |
| `tools/emit_data_contract.py` | **new** — renders `contract.py`'s output to YAML, one file per target |
| `data_contracts/<target>.yaml` | **new, generated and committed** — the artefacts |
| `governance/control_objects.sql` | **+** `severity` on `ref_dq_expectation` |
| `verify_repo.py` | the no-op gate and the derivation assertions |
| `tests/test_accelerator.py` | checks for the pure derivations |

**Why `contract.py` is separate from the emitter.** The derivations — what a column's
classification is, which columns form the grain — are the part worth testing and the part that
can be wrong. YAML rendering is not. Splitting them keeps every interesting decision in a pure
module the offline suite can fire in both directions, exactly as `reject_digest.py` is separate
from `supersede_quarantine.py`.

---

### Task 1: System column types, and severity

**Files:**
- Modify: `src/accelerator/naming.py`
- Modify: `governance/control_objects.sql`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Produces: `naming.SYSTEM_COLUMN_TYPES: dict[str, str]` covering every member of `naming.SYSTEM_COLUMNS`; `control.ref_dq_expectation.severity`.

- [ ] **Step 1: Write the failing test**

```python
# The seven system columns have fixed, knowable types. They were never declared, so the
# contract emitter had no offline source for them and would have had to guess or omit.
check("every system column has a declared type",
      set(naming.SYSTEM_COLUMN_TYPES) == set(naming.SYSTEM_COLUMNS),
      f"undeclared: {sorted(set(naming.SYSTEM_COLUMNS) - set(naming.SYSTEM_COLUMN_TYPES))}; "
      f"unknown: {sorted(set(naming.SYSTEM_COLUMN_TYPES) - set(naming.SYSTEM_COLUMNS))}")
check("load_dts and applied_dts are timestamps, not strings",
      naming.SYSTEM_COLUMN_TYPES[naming.COL["load_dts"]] == "TIMESTAMP"
      and naming.SYSTEM_COLUMN_TYPES[naming.COL["applied_dts"]] == "TIMESTAMP",
      "a contract typing a timestamp as STRING misleads every consumer that reads it")
check("sub_seq is an integer, since it orders intra-batch versions",
      naming.SYSTEM_COLUMN_TYPES[naming.COL["sub_seq"]] in ("INT", "BIGINT"),
      str(naming.SYSTEM_COLUMN_TYPES.get(naming.COL["sub_seq"])))

# severity on the expectation table
_sev_chunk = ([c for c in _ctl_sql.split("CREATE TABLE IF NOT EXISTS")
               if "ref_dq_expectation" in c.split("(")[0]] or [""])[0]
check("ref_dq_expectation declares a severity column",
      "severity" in _sev_chunk, _sev_chunk[:200])
check("and its comment names the three tiers",
      all(t in _sev_chunk for t in ("fail", "drop", "warn")),
      "a severity whose vocabulary is not written down invites a fourth value")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -iE "system column has a declared type|severity"`
Expected: FAIL — neither exists.

- [ ] **Step 3: Declare the system column types**

In `src/accelerator/naming.py`, immediately after `SYSTEM_COLUMNS`:

```python
# The system columns' types, declared because nothing else declares them. The contract
# emitter runs OFFLINE -- factory._derived_schema_ddl reads the staged frame and needs
# Spark -- so without this the emitter would have to guess a timestamp's type or omit it,
# and a contract that types load_dts as STRING misleads every consumer that reads it.
#
# These are fixed by _system_columns() in factory.py, not by any source, so they are
# knowable without a workspace. If that function's output types ever change, this must
# change with it -- the suite asserts the two sets of NAMES agree, which catches a column
# added or removed but not a type silently altered.
SYSTEM_COLUMN_TYPES: dict[str, str] = {
    COL["load_dts"]:    "TIMESTAMP",
    COL["applied_dts"]: "TIMESTAMP",
    COL["sub_seq"]:     "INT",
    COL["rec_src"]:     "STRING",
    COL["batch_id"]:    "STRING",
    COL["manifest_id"]: "STRING",
    COL["cdc_op"]:      "STRING",
}
```

**Verify each against `factory._system_columns` before writing it.** Read that function and
confirm the type it produces for each column. If any disagrees with the table above, the
function wins — change the table and say so in your report.

- [ ] **Step 4: Add severity to the expectation table**

In `governance/control_objects.sql`, add to `ref_dq_expectation`. **No semicolon inside the
COMMENT literal** — the splitter cuts on `;` regardless of quoting:

```sql
  severity    STRING  NOT NULL COMMENT 'fail stops the load, drop quarantines the row, warn is advisory'
```

Add a comment block above the table explaining the tiers and that `warn` has no pipeline
support yet:

```sql
-- SEVERITY, added 27 Aug 2026. Before it, a consumer could not tell a rule that DROPS a row
-- from one that is advisory, because there was only one behaviour: expect_all_or_drop, which
-- drops and quarantines.
--
--   fail  the load stops. The two compiled-in key-safety rules are this tier, because
--         continuing would write keys that can never join.
--   drop  the row is dropped and written to the quarantine twin. Every rule behaves this
--         way today, so it is the default and existing rows need no migration.
--   warn  the row loads and the violation is recorded. THE PIPELINE CANNOT DO THIS YET.
--         The column can express it so the contract can describe it once it exists; until
--         then tools/emit_data_contract.py REFUSES a warn row rather than promising a
--         consumer a behaviour the pipeline lacks.
```

- [ ] **Step 5: Run both suites, then prove the checks can fail**

```bash
uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3
uv run --frozen python verify_repo.py 2>&1 | tail -3
cp src/accelerator/naming.py /tmp/nm.bak
sed -i 's/    COL\["cdc_op"\]:      "STRING",//' src/accelerator/naming.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "every system column has a declared type"
# Expected: FAIL, naming cdc_op as undeclared
cp /tmp/nm.bak src/accelerator/naming.py
```

The pre-existing "exactly 8 statements" check still holds — a column addition does not change
the statement count. Confirm it in your report rather than assuming.

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/naming.py governance/control_objects.sql tests/test_accelerator.py
git commit -m "Declare the system column types, and give an expectation a severity"
```

---

### Task 2: The derivations

**Files:**
- Create: `src/accelerator/contract.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `naming`, `hashing`, `spec`, and `factory._projection` (lazily — see below).
- Produces:
  - `classify(entity, column) -> str` — `internal` | `personal` | `financial` | `restricted`
  - `column_type(entity, src, column) -> str` — `BINARY(32)`, a declared type, or `source-derived`
  - `grain(entity, columns) -> list[str]` — the columns forming the uniqueness grain, mirroring `append_only_check`'s rule over the same column list
  - `foreign_keys(entity, model) -> dict[str, str]` — column → `catalog.schema.table.column`
  - `clustering(entity) -> list[str]` — from `factory._cluster_by`

**The lazy import is load-bearing, not style.** `factory.py` imports pyspark at module level,
and the suite imports this module at the top of the file before installing its pyspark stub. A
module-level `from .factory import ...` would abort the whole suite at import time. Keep it
inside the function body. `src/accelerator/reject_digest.py` does exactly this and says why.

- [ ] **Step 1: Write the failing test**

```python
from accelerator import contract as _ct  # near the other accelerator imports

_ct_pay = _am.get("payroll_detail")        # sensitivity: restricted, 5 masked columns
_ct_gjl = _am.get("general_journal_line")  # sensitivity: financial, masks debitamt/crdtamnt

# CLASSIFICATION IS DERIVED, NOT DECLARED. The model already carries entity.sensitivity;
# adding a per-column field would be a second authority for one concept, which is the
# duplicate-definition trap this repo has been bitten by twice.
check("a masked column takes the entity's sensitivity",
      _ct.classify(_ct_gjl, "debitamt") == "financial",
      "the entity is financial and the column is masked, so the column is financial")
check("an unmasked column on the same entity is internal",
      _ct.classify(_ct_gjl, naming.COL["load_dts"]) == "internal",
      "load_dts carries no mask and is not sensitive")
check("a restricted entity's masked columns are restricted",
      all(_ct.classify(_ct_pay, c) == "restricted" for c, _fn in _ct_pay.masks),
      str([(c, _ct.classify(_ct_pay, c)) for c, _fn in _ct_pay.masks]))
check("classification never invents a level the model does not validate",
      all(_ct.classify(e, c) in ("internal", "personal", "financial", "restricted")
          for e in _am.entities for c, _fn in e.masks),
      "spec.py:583 validates exactly these four, so a fifth would be unmappable")

# The derivation must be TOTAL IN BOTH DIRECTIONS -- see spec section 6.
_ct_above = [(e.base_table, c) for e in _am.entities
             for c, _fn in e.masks if _ct.classify(e, c) == "internal"]
check("no masked column classifies as internal",
      not _ct_above,
      f"{_ct_above} -- understating protection on real financial data")
_ct_unmasked_sensitive = [
    (e.base_table, c) for e in _am.entities
    for c, _k, _v in _ct_projection_cols(e)
    if c not in {m for m, _f in e.masks} and _ct.classify(e, c) != "internal"]
check("no unmasked column classifies above internal",
      not _ct_unmasked_sensitive,
      f"{_ct_unmasked_sensitive} -- would alarm a consumer over an unprotected-but-"
      f"unclassified column, which is the opposite error and just as wrong")

# TYPES: what the model genuinely knows, and an honest marker for what it does not.
check("a hash key is typed BINARY(32), never STRING",
      _ct.column_type(_ct_gjl, None, naming.hk("general_journal_line")) == "BINARY(32)",
      "the exemplar contract types hash keys as STRING(40) under SHA-1; this estate is "
      "RATIFIED sha2_256 stored BINARY, and a contract saying otherwise is simply wrong")
check("a system column takes its declared type",
      _ct.column_type(_ct_gjl, None, naming.COL["load_dts"]) == "TIMESTAMP",
      "declared in naming.SYSTEM_COLUMN_TYPES by Task 1")
check("a cast column takes the cast the binding declares",
      _ct.column_type(_ct_gjl, _ct_gjl.sources[0], "debitamt") == "DECIMAL(18,2)",
      "the binding casts it for hashdiff stability, so the type is known")
check("an uncast payload column is marked source-derived, not guessed",
      _ct.column_type(_ct_gjl, _ct_gjl.sources[0], "jrnentry") == "source-derived",
      "payload types live in Bronze and _derived_schema_ddl needs Spark; a guess in a "
      "contract is worse than an honest gap")

# CLUSTERING must never contradict what the runtime will do.
# GRAIN AND FOREIGN KEYS were defined but unasserted in an earlier draft of this plan --
# untested code published into a governance artefact, which is the same family as a check
# that cannot fail. Both are asserted here, and the grain is asserted AGAINST THE GATE
# rather than against a restatement of it.
_ct_ao = _ilu2.spec_from_file_location("ao", ROOT / "checks" / "append_only_check.py")
_ct_aom = _ilu2.module_from_spec(_ct_ao); _ct_ao.loader.exec_module(_ct_aom)
check("the satellite grain mirrors append_only_check's rule, not a restatement of it",
      _ct.grain(_am.get("job_request_details"),
                [c for c, _k, _v in _ct_projection_cols(_am.get("job_request_details"))])
      [:1] == [next((c for c in sorted(
          c for c, _k, _v in _ct_projection_cols(_am.get("job_request_details"))
      ) if c.endswith("_hk")), None)],
      "append_only_check:175 takes the FIRST _hk in sorted order, not naming.hk(parents[0]); "
      "publishing the second would declare a grain that gate never checks")
check("a non-satellite's grain is its own hash key",
      _ct.grain(_ct_gjl, [c for c, _k, _v in _ct_projection_cols(_ct_gjl)])
      == [naming.hk("general_journal_line")],
      "an NHL is unique at its own key")
check("foreign keys name every declared parent and nothing else",
      set(_ct.foreign_keys(_ct_gjl, _am)) == {naming.hk(p) for p in _ct_gjl.parents},
      f"declared parents {_ct_gjl.parents}, emitted {sorted(_ct.foreign_keys(_ct_gjl, _am))}")
check("and an entity with no parents emits no foreign keys",
      _ct.foreign_keys(_am.get("accounting_journal"), _am) == {}
      or not _am.get("accounting_journal").parents,
      "a hub anchors an identity and references nothing")

check("no entity's clustering keys include a hash key",
      not [c for e in _am.entities for c in _ct.clustering(e) if c.endswith("_hk")],
      "DEF-23: Delta REFUSES a BINARY hash key as a clustering column, so a contract "
      "declaring one describes something this runtime will not do")
```

Add this helper next to the other test helpers, since two checks need the projected columns
and the lazy-import rule applies here too:

```python
def _ct_projection_cols(entity):
    """The entity's projected column names, via its first table's binding shape."""
    src = ([s for s, _t in entity.tables()] or [None])[0]
    from accelerator import factory as _f
    return _f._projection(entity, src)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -iE "masked column|hash key is typed|source-derived|clustering keys include"`
Expected: FAIL — the module does not exist. Create it with a stub first if the suite aborts on import.

- [ ] **Step 3: Write the module**

```python
"""What the data contract says about a table, derived from the model that already says it.

Separate from tools/emit_data_contract.py deliberately: the derivations here are the part
that can be WRONG, and the part the offline suite can fire in both directions. YAML
rendering is neither. Same split as reject_digest.py against supersede_quarantine.py.

CLASSIFICATION IS DERIVED, NOT DECLARED. The model already carries entity.sensitivity,
validated at spec.py:583 as one of internal/personal/financial/restricted, and
verify_repo.py refuses a sensitive entity that declares no masks. An earlier draft of this
work proposed adding a per-column security_classification field; that would have been a
SECOND AUTHORITY for one concept, which is the trap this repo has been bitten by twice
(BUSINESS_KINDS, the system-column set) and which key_composition.json exists to catch.

The derivation is total only because two gates make it so: a sensitive entity must declare
masks, and a marked column must carry one. If a future decision ever permits a sensitive
but deliberately UNMASKED column, this derivation becomes wrong and the model would then
need the per-column field after all. The suite asserts totality in both directions so that
day announces itself.
"""

from __future__ import annotations

from . import naming
from .hashing import key_type_sql

__all__ = ["classify", "column_type", "grain", "foreign_keys", "clustering"]

SOURCE_DERIVED = "source-derived"


def classify(entity, column: str) -> str:
    """The column's security classification.

    A column carrying a mask takes the entity's declared sensitivity; every other column is
    internal. Nothing here invents a level: spec.py validates exactly four, so a fifth
    would be unmappable by any consumer reading the contract.
    """
    masked = {c for c, _fn in entity.masks}
    return entity.sensitivity if column in masked else "internal"


def column_type(entity, src, column: str) -> str:
    """The column's type, or SOURCE_DERIVED where the model genuinely does not know it.

    THREE THINGS ARE KNOWN OFFLINE and one is not. Hash keys are whatever the ratified
    rulebook stores -- BINARY(32) today, and the exemplar contract's STRING(40) under SHA-1
    would simply be false here. System columns are fixed by factory._system_columns and
    declared in naming.SYSTEM_COLUMN_TYPES. A binding's `cast` gives the type for the few
    columns cast for hashdiff stability.

    Everything else is a payload column whose type comes from Bronze at run time.
    factory._derived_schema_ddl reads it from the staged frame and needs Spark, so an
    OFFLINE emitter cannot know it. It is marked, not guessed: a contract that states a
    wrong type is worse than one that admits a gap, and omitting the column entirely would
    understate the schema.
    """
    if column.endswith("_hk"):
        return f"{key_type_sql()}(32)" if key_type_sql() == "BINARY" else key_type_sql()
    if column in naming.SYSTEM_COLUMN_TYPES:
        return naming.SYSTEM_COLUMN_TYPES[column]
    if src is not None:
        for cast_column, cast_type in src.cast:
            if cast_column == column:
                return cast_type.upper()
    return SOURCE_DERIVED


def grain(entity, columns) -> list[str]:
    """The columns whose combination append_only_check asserts unique.

    MIRRORS THAT GATE'S RULE, and the mirroring is the point: a contract whose primary_key
    claims a grain the gate does not police is worse than one that claims none.

    Read checks/append_only_check.py:173-180 before changing this. Its satellite rule picks
    the parent key as `next((c for c in sorted(cols) if c.endswith("_hk")), None)` -- the
    FIRST _hk column in sorted order -- and NOT naming.hk(entity.parents[0]). Those two can
    differ, and an earlier draft of this module used the second, which would have published
    a grain the gate never checks. `columns` is the projected column list so the same rule
    can be applied to the same input.
    """
    if entity.kind in naming.SATELLITE_KINDS:
        parent_hk = next((c for c in sorted(columns) if c.endswith("_hk")), None)
        if parent_hk is None:
            raise ValueError(
                f"{entity.base_table}: no parent hash key among {sorted(columns)[:6]}, so "
                f"append_only_check has no grain to assert and the contract has no "
                f"primary key to declare."
            )
        cols = [parent_hk, naming.COL["load_dts"], naming.COL["sub_seq"]]
        if naming.COL["mas_key"] in columns:
            cols.append(naming.COL["mas_key"])
        return cols
    return [naming.hk(entity.name)]


def foreign_keys(entity, model) -> dict:
    """column -> the parent hub's hash key it references.

    Derived from `parents`, which is what the loaders actually hash against, so the
    contract cannot declare a relationship the vault does not compute.
    """
    return {naming.hk(p): f"{naming.hk(p)}@{model.get(p).base_table}"
            for p in entity.parents if model.get(p) is not None}


def clustering(entity) -> list[str]:
    """The clustering keys the factory will actually declare for this entity.

    Read from factory rather than restated. The exemplar contract clusters on hash keys;
    DEF-23 measured that Delta REFUSES a BINARY hash key as a clustering column, and
    factory._cluster_refusal enforces it, so a restatement here could describe something
    the runtime will not do.

    The import is lazy on purpose: factory imports pyspark at module level and the suite
    imports this module before installing its stub. Hoisting it aborts the whole suite at
    import time. reject_digest.py does the same and for the same reason.
    """
    from .factory import _cluster_by

    return list(_cluster_by(entity))
```

**Check `naming.COL["mas_key"]` exists before using it.** If the key is spelled differently,
use the real name and say so in your report — do not invent a constant.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Prove each derivation can fail**

```bash
cp src/accelerator/contract.py /tmp/ct.bak
sed -i 's/    return entity.sensitivity if column in masked else "internal"/    return "internal"/' src/accelerator/contract.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "masked column takes|no masked column classifies"
# Expected: both FAIL
cp /tmp/ct.bak src/accelerator/contract.py
sed -i 's/    return entity.sensitivity if column in masked else "internal"/    return entity.sensitivity/' src/accelerator/contract.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "unmasked column on the same entity|no unmasked column classifies"
# Expected: both FAIL -- the opposite direction
cp /tmp/ct.bak src/accelerator/contract.py
sed -i 's|        return f"{key_type_sql()}(32)" if key_type_sql() == "BINARY" else key_type_sql()|        return "STRING"|' src/accelerator/contract.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "hash key is typed BINARY"
# Expected: FAIL
cp /tmp/ct.bak src/accelerator/contract.py
```

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/contract.py tests/test_accelerator.py
git commit -m "Derive the contract's classification, types and grain from the model"
```

---

### Task 3: The emitter and the artefacts

**Files:**
- Create: `tools/emit_data_contract.py`
- Create: `data_contracts/<target>.yaml` (generated, committed)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: everything from `contract.py`.
- Produces, and Task 4's gate calls all three, so none is optional:
  - `emit(model, target, variables) -> dict` — pure, the contract structure
  - `render(structure) -> str` — the YAML text, `yaml.safe_dump(..., sort_keys=True)` so output is byte-stable between runs
  - `targets_and_variables() -> list[tuple[str, dict]]` — every bundle target and its resolved variables, read from `databricks.yml`
  A `__main__` writes `render(emit(...))` to `data_contracts/<target>.yaml` for each.

- [ ] **Step 1: Write the failing test**

```python
_ec_spec = _ilu2.spec_from_file_location(
    "ec", ROOT / "tools" / "emit_data_contract.py")
_ec = _ilu2.module_from_spec(_ec_spec); _ec_spec.loader.exec_module(_ec)

_ec_out = _ec.emit(_am, "usnc_tds", {"catalog": "02_usnc_silver_edm_dev",
                                     "vault_schema": "raw_vault",
                                     "business_vault_schema": "business_vault"})
check("the contract names every entity in the model",
      set(_ec_out["entities"]) == {e.base_table for e in _am.entities},
      str(set(_ec_out["entities"]) ^ {e.base_table for e in _am.entities}))
check("physical names come from the target's variables, not literals",
      all(v["physical_name"].startswith("02_usnc_silver_edm_dev.")
          for v in _ec_out["entities"].values()),
      "a hardcoded catalog would be wrong in eight of nine targets")
check("no emitted hash key is typed STRING",
      not [(t, c) for t, v in _ec_out["entities"].items()
           for c, cv in v["columns"].items()
           if c.endswith("_hk") and cv["type"] != "BINARY(32)"],
      "the estate is RATIFIED sha2_256 stored BINARY(32)")
check("no emitted clustering key is a hash key",
      not [(t, c) for t, v in _ec_out["entities"].items()
           for c in v.get("storage", {}).get("clustering_keys", [])
           if c.endswith("_hk")],
      "DEF-23: this runtime refuses to cluster on a BINARY hash key")
check("the contract declares NO service level agreement",
      "serviceLevelAgreement" not in _ec_out and "sla" not in _ec_out,
      "we measure no freshness, latency or uptime; publishing an unmeasured promise in a "
      "signed artefact is a claim a consumer can hold us to and we cannot evidence")
check("and declares no PIT or bridge entity",
      not [t for t in _ec_out["entities"] if t.startswith(("pit_", "bridge_"))],
      "naming.PREFIX knows both kinds and the model declares neither, so emitting one "
      "would describe a table that does not exist")
check("emit() is pure -- calling it twice gives an identical structure",
      _ec.emit(_am, "usnc_tds", {"catalog": "02_usnc_silver_edm_dev",
                                 "vault_schema": "raw_vault",
                                 "business_vault_schema": "business_vault"}) == _ec_out,
      "a generated artefact that varies between runs cannot be asserted as a no-op")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -iE "contract names every entity|physical names come from"`
Expected: FAIL — the module does not exist.

- [ ] **Step 3: Write the emitter**

`tools/emit_data_contract.py`. It reads the model, calls `contract.py` for every derivation,
and renders. Two rules it must follow, both of which the exemplar contract gets wrong for this
estate and which the tests above pin:

* every physical name is built from that target's `catalog`, `vault_schema` and
  `business_vault_schema` variables — `naming.vault_schema_for(entity.kind, ...)` picks which
  schema, exactly as the loaders do;
* nothing is emitted that the model does not hold. No SLA block, no `delta_max_file_size`, no
  PIT, no bridge.

**Refuse a `warn` severity.** Read `control.ref_dq_expectation`'s declared severities only if
they are available offline — they are not, the table is in a lake — so instead: if a future
caller passes expectations in, the emitter raises on `severity == "warn"` with a message
saying the pipeline cannot honour it yet. State in your report whether expectations are
emitted at all in this version; if the table is unreadable offline, emit the two compiled-in
key-safety rules only and say so in the contract.

Write one YAML per target under `data_contracts/`, using `yaml.safe_dump` with
`sort_keys=True` so the output is deterministic — a generated artefact that reorders between
runs cannot be asserted as a no-op.

- [ ] **Step 4: Generate and commit the artefacts**

```bash
uv run --frozen python tools/emit_data_contract.py
git add data_contracts/
```

Inspect one before committing. Read `data_contracts/usnc_tds.yaml` yourself and confirm the
hash keys say `BINARY(32)`, the clustering keys say `load_dts`, the system columns carry their
declared types, and the masked financial columns classify as `financial`. **If any of that is
wrong, the derivation is wrong — fix it rather than adjusting the test.**

- [ ] **Step 5: Run both suites and prove the checks fail**

```bash
uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3
uv run --frozen python verify_repo.py 2>&1 | tail -3
cp tools/emit_data_contract.py /tmp/ec.bak
# make it emit an SLA and confirm the refusal fires
python3 - <<'EOF'
from pathlib import Path
p = Path("tools/emit_data_contract.py"); t = p.read_text()
p.write_text(t.replace('    return {', '    return {\n        "serviceLevelAgreement": {"uptime": "99.9%"},', 1))
EOF
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "declares NO service level agreement"
# Expected: FAIL
cp /tmp/ec.bak tools/emit_data_contract.py
```

- [ ] **Step 6: Commit**

```bash
git add tools/emit_data_contract.py data_contracts/ tests/test_accelerator.py
git commit -m "Emit one data contract per target, from the model and nothing else"
```

---

### Task 4: The no-op gate

**Files:**
- Modify: `verify_repo.py`
- Test: covered by `verify_repo.py` itself

**Interfaces:**
- Consumes: `tools/emit_data_contract.py`.
- Produces: a build failure when a committed contract differs from what the model generates.

**This is the whole discipline of the task, and it has a precedent.** `verify_repo.py:1853-1858`
regenerates `metadata/key_composition.json` and fails on any difference, with the comment "the
diff IS the review". The contract gets the same treatment: committed so its diff is reviewable,
regenerated so it cannot drift.

- [ ] **Step 1: Write the failing check**

In `verify_repo.py`, near the `key_composition` block so the two read together:

```python
# THE DIFF IS THE REVIEW, exactly as for key_composition.json above. A generated artefact
# that is committed but never re-checked is a document that describes what the model USED
# to say. Regenerate every contract and compare; a difference means someone changed the
# model without regenerating, or edited the contract by hand -- and the contract is not
# hand-editable, because the model is the only authority.
import emit_data_contract as _edc  # noqa: E402

_dc_dir = ROOT / "data_contracts"
check("data_contracts/ exists and is populated",
      _dc_dir.is_dir() and any(_dc_dir.glob("*.yaml")),
      "run tools/emit_data_contract.py and review the diff")
_dc_stale = []
for _t, _vars in _edc.targets_and_variables():
    _want = _edc.render(_edc.emit(model, _t, _vars))
    _path = _dc_dir / f"{_t}.yaml"
    if not _path.is_file() or _path.read_text(encoding="utf-8") != _want:
        _dc_stale.append(_t)
check("every committed data contract matches what the model generates",
      not _dc_stale,
      f"stale: {_dc_stale} -- run tools/emit_data_contract.py and review the diff")
```

- [ ] **Step 2: Run it to verify it fails**

Hand-edit one committed contract — change a classification from `financial` to `internal` —
and run `uv run --frozen python verify_repo.py`. Expected: FAIL naming that target. Restore it.

That mutation is the one that matters: it is exactly the drift the gate exists to catch, and
it is the shape a well-meaning hand edit would take.

- [ ] **Step 3: Prove the gate is not vacuous in the other direction**

Change the model instead — add a mask to an entity that has none — regenerate nothing, and
confirm the gate FAILS. Then regenerate, confirm it passes, and revert both.

This is the direction that matters more: a gate that only catches hand-edits but not model
changes would let the contract silently describe an old model.

- [ ] **Step 4: Commit**

```bash
git add verify_repo.py
git commit -m "Gate that every committed contract matches the model, the diff being the review"
```

---

## Notes for whoever executes this

**The one thing that would make this worthless.** If the classification derivation is not total
in both directions, the contract lies in one of two ways: a masked column reported as
`internal` understates protection on real financial data, and an unmasked column reported above
`internal` alarms a consumer over nothing. Task 2 asserts both directions across every entity
in the model, and both assertions must be proven able to fail. Treat any weakening of those two
as a change to a published governance artefact, not a test tidy-up.

**What this plan does not build.** It does not declare payload types in the model — that is the
follow-up that would let the contract type every column instead of marking most
`source-derived`, and it is a model change with its own review surface. It does not implement
the `warn` severity tier in the pipeline; the column can express it and the emitter refuses to
promise it. It does not publish the contracts anywhere: where the generated YAML goes is a
decision worth making after the first one exists and can be looked at.

**A contract describes; it does not govern.** Every control it names is enforced by a gate that
already exists — masks by `mask_survival_check`, grain by `append_only_check`, key composition
by the digest, grants by `schema_grant_check`. Nothing downstream should read this file and
conclude a rule is enforced because the contract says so.
