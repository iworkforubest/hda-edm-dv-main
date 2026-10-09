# Cross-Layer Control Standard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one declaration the authority for what a control schema must contain in any
layer — verified against silver, generating gold's DDL, and specifying plus verifying bronze —
and revise the Bronze request to ask for their own control schema.

**Architecture:** A pure declaration module (`src/accelerator/control_standard.py`) states the
mandatory core and the layer-specific tables. Silver's existing `control_objects.sql` is
verified against it rather than rewritten. Gold's DDL is generated from it. A published
contract plus a live conformance check cover bronze, which we do not own.

**Tech Stack:** Python 3.11 (pyproject's floor, and what CI runs), `yaml`, Databricks CLI /
SQL warehouse for the live check.

**Spec:** `docs/superpowers/specs/2026-08-27-cross-layer-control-standard-design.md`

**Scope note:** This is Plan 1 of two. The cross-layer dashboard is Plan 2 and is deliberately
not planned yet — it should be written once bronze or gold has a control schema to show, which
is what this plan asks for.

**What of the spec belongs to Plan 2, so it is not read as a gap here:** section 5 entirely
(the cross-layer view and the forbidden subtraction tile), section 6's *rendering* of
"not instrumented" as a tile — Task 5 covers the check's `NOT_EVALUATED` path, which is the
same property one layer down — and two rows of section 8's gate table: "no dashboard dataset
subtracts one layer's count from another's" and "a layer absent from the live lake renders the
literal `not instrumented`".

**One section-8 row needs no task.** "`append_only_check` … extended to any layer whose control
schema exists" is already satisfied: that check takes `--catalog` and a repeatable `--schema`,
so pointing it at another layer is job wiring, not code. There is no bronze or gold job to wire
it into yet, and inventing one before the schema exists would be scaffolding. Task 5's
`append_only_conformance()` covers the property in the meantime, from the standard's side.

## Global Constraints

* **Python floor is 3.11.** No backslash inside an f-string expression part — a SyntaxError
  before 3.12, and `verify_repo` gates it. Hoist into a named local.
* **Tests are NOT pytest.** Two script suites of `check(name, condition, detail)` calls:
  `tests/test_accelerator.py` (structural) and `verify_repo.py` (repo invariants). Append new
  sections at the END, before the final summary block beginning `print("\n" + "=" * 62)`.
* **Every new check must be PROVEN ABLE TO FAIL.** Mutate, confirm the *named* check reports
  FAIL while the suite still runs to completion, restore exactly. `ABSENT` is not a pass.
  **Commit before mutating** — a `git checkout` to undo a mutation discards uncommitted work,
  which happened twice on 27 Aug.
* **No task asserts an absolute check count.**
* **Silver's `control_objects.sql` is NOT rewritten.** It is verified against the declaration.
  Two authorities for one concept is this repo's recurring defect; a verification closes it
  without risking deployed, gated code.
* **The mandatory core is three tables**, identical in shape in every layer: `aud_load_run`,
  `aud_table_load`, `aud_table_discard`.
* **Core tables are `delta.appendOnly`; config tables deliberately are not.** An audit records
  what happened; a rule records what should happen. A conforming layer must get the split
  right, not set the property everywhere.
* **No dataset or query may subtract one layer's count from another's.** A drop between layers
  is expected: hubs deduplicate 4,444,172 GP rows into 2,221,108.
* **This plan is entirely offline except Task 5**, which reads a live lake and is the only task
  needing a profile. Never auto-select a profile; `hfig-usnc-tds` is the only non-production
  one.

### Verified facts (do not re-derive)

Measured 27 Aug 2026 against `hfig-usnc-tds`:

* Silver `control` holds **6 tables**: `ctl_approval_manifest`, `ref_dq_expectation`,
  `aud_table_load`, `aud_table_discard`, `aud_load_run`, `ctl_quarantine_superseded`.
* Append-only: `aud_table_load`, `aud_table_discard`, `aud_load_run`,
  `ctl_quarantine_superseded`. Mutable: `ctl_approval_manifest`, `ref_dq_expectation`.
* Bronze `01_usnc_bronze_dev` has **47 schemas, all `<source>` / `<source>_raw` pairs, and no
  control, audit, governance or meta schema.** DATED 24 AUG, not 27 -- this block's header says
  27 Aug and that is wrong for THIS line only. The figure traces to
  `specs/2026-08-24-usnc-tds-retarget-design.md:432` ("Full bronze inventory: 47 schemas, 21 of
  them `_raw`") under a section headed 2026-08-24, and no re-count on the 27th exists anywhere in
  the repo -- searched git log, OPEN_ITEMS and the sdd workspace. The silver facts in this block
  DO carry independent 27 Aug corroboration; this one does not. It reached
  docs/loop1_control_table_request.html as "measured 27 August" and was corrected there.
* Gold `03_usnc_gold_edm_dev` **does not exist**.
* The invariant `staged = accepted + sum(discarded)` held across all six silver tables with
  zero imbalance.
* `checks/apply_control_objects.py` already exposes `declared_columns(sql_text) -> dict` and
  `column_drift(declared, deployed) -> list`, added 27 Aug. **Reuse `declared_columns`; do not
  write a second SQL parser.**

---

## File Structure

| file | responsibility |
|---|---|
| `src/accelerator/control_standard.py` (new) | The declaration. Pure, importable, no I/O. The single authority. |
| `tools/emit_control_contract.py` (new) | Renders the bronze contract and gold's DDL from the declaration. Mirrors `tools/emit_data_contract.py`. |
| `control_contracts/bronze.yaml` (new, generated) | The published contract for the Bronze team. |
| `governance/control_objects_gold.sql` (new, generated) | Gold's DDL. Never hand-edited. |
| `checks/control_conformance_check.py` (new) | Live check: does a layer's control schema match the contract? Pure decision functions plus a Spark-free `main()` shape. |
| `tests/test_accelerator.py` (modify) | Structural checks over the declaration and both generated artefacts. |
| `verify_repo.py` (modify) | Regeneration-is-a-no-op, and silver verified against the declaration. |
| `docs/loop1_control_table_request.html` (modify) | The revised Bronze ask. |

---

## Task 1: The declaration

**Files:**
- Create: `src/accelerator/control_standard.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `CORE: dict[str, dict[str, str]]` — mandatory table name → column name → SQL type.
  - `LAYER_TABLES: dict[str, dict[str, dict[str, str]]]` — layer → table → columns.
  - `APPEND_ONLY: frozenset[str]` — tables that must be `delta.appendOnly`.
  - `MUTABLE: frozenset[str]` — tables that must NOT be.
  - `LAYERS: tuple[str, ...]` — `("bronze", "silver", "gold")`.
  - `STAGED_MEANING: dict[str, str]` — layer → what `staged`/`accepted` mean there.
  - `tables_for(layer: str) -> dict[str, dict[str, str]]` — core plus that layer's own.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accelerator.py`:

```python
# --------------------------------------------------------------------------- #
print("\n[standard] the cross-layer control declaration")

from accelerator import control_standard as _cs  # noqa: E402

check("the mandatory core is exactly three audit tables",
      set(_cs.CORE) == {"aud_load_run", "aud_table_load", "aud_table_discard"},
      f"core is {sorted(_cs.CORE)} -- the spec mandates three, identical in shape in every "
      f"layer, and the invariant staged = accepted + sum(discarded) needs all three")

check("every layer the standard names has a table set",
      _cs.LAYERS == ("bronze", "silver", "gold")
      and all(l in _cs.LAYER_TABLES for l in _cs.LAYERS),
      f"LAYERS={_cs.LAYERS}, LAYER_TABLES keys={sorted(_cs.LAYER_TABLES)}")

check("tables_for() returns the core plus that layer's own, for every layer",
      all(set(_cs.tables_for(l)) >= set(_cs.CORE) for l in _cs.LAYERS),
      "a layer whose table set omits a core table would let a conforming layer skip the "
      "invariant")

check("bronze owns the delivery manifest and silver does not",
      "ctl_delivery_manifest" in _cs.LAYER_TABLES["bronze"]
      and "ctl_delivery_manifest" not in _cs.LAYER_TABLES["silver"],
      "each layer records what IT did; the manifest belongs to the team that knows what a "
      "delivery is, which is why the Bronze ask changes")

check("silver keeps the expectation reference and the supersede ledger",
      {"ref_dq_expectation", "ctl_quarantine_superseded"} <= set(_cs.LAYER_TABLES["silver"]),
      f"silver declares {sorted(_cs.LAYER_TABLES['silver'])}")

# THE SPLIT IS THE DESIGN, not an inconsistency: an audit records what happened and must never
# be rewritten; a rule records what should happen and must be correctable.
check("every core table is append-only",
      set(_cs.CORE) <= _cs.APPEND_ONLY,
      f"core tables not marked append-only: {sorted(set(_cs.CORE) - _cs.APPEND_ONLY)} -- an "
      f"audit that can be rewritten is an audit nobody can rely on")

check("the expectation reference and the manifest are MUTABLE, not append-only",
      {"ref_dq_expectation", "ctl_delivery_manifest"} <= _cs.MUTABLE
      and not (_cs.MUTABLE & _cs.APPEND_ONLY),
      f"MUTABLE={sorted(_cs.MUTABLE)}, overlap with APPEND_ONLY="
      f"{sorted(_cs.MUTABLE & _cs.APPEND_ONLY)} -- an append-only expectation reference means "
      f"a mistyped rule can never be withdrawn")

check("every layer declares what staged and accepted MEAN there",
      set(_cs.STAGED_MEANING) == set(_cs.LAYERS)
      and all(_cs.STAGED_MEANING[l].strip() for l in _cs.LAYERS),
      f"missing meanings for {sorted(set(_cs.LAYERS) - set(_cs.STAGED_MEANING))} -- three "
      f"layers share one column shape and three meanings, and an undeclared meaning invites "
      f"a cross-layer comparison that is not valid")
```

- [ ] **Step 2: Run to verify it fails**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: `ModuleNotFoundError: No module named 'accelerator.control_standard'`

- [ ] **Step 3: Write the module**

Create `src/accelerator/control_standard.py`:

```python
"""What a control schema must contain, in any layer.

THE SINGLE AUTHORITY. Silver's governance/control_objects.sql is VERIFIED against this rather
than generated from it -- that DDL is deployed, gated and working, and rewriting it to prove a
point about authority would risk it for no functional gain. Gold's DDL and the bronze contract
ARE generated from here, because gold does not exist yet and bronze is another team's catalog.

WHY CONTROL AND NOT GOVERNANCE. The audit is uniform: every layer takes rows in, writes some
out, and drops the difference for a reason. Masking is not. databricks.yml records that
bronze's `<source>` schemas carry PHYSICAL PII masking -- values rewritten -- while silver uses
Unity Catalog COLUMN masks, values preserved and access evaluated per reader. A
physically-masked value cannot be revealed to a privileged reader and a column-masked one can.
Standardising those together would define that difference away.
"""

from __future__ import annotations

LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")

# THE MANDATORY CORE. Identical in shape in every layer, because the invariant
# staged = accepted + sum(discarded) per (job_run_id, table_name) is what makes the core worth
# mandating rather than suggesting -- verified live across all six silver tables on 27 Aug with
# zero imbalance.
CORE: dict[str, dict[str, str]] = {
    "aud_load_run": {
        "job_run_id": "STRING",
        "phase": "STRING",
        "target": "STRING",
        "active_sources": "STRING",
        "recorded_at": "TIMESTAMP",
    },
    "aud_table_load": {
        "job_run_id": "STRING",
        "pipeline_update_id": "STRING",
        "table_name": "STRING",
        "written_by": "STRING",
        "staged": "BIGINT",
        "accepted": "BIGINT",
        "recorded_at": "TIMESTAMP",
    },
    "aud_table_discard": {
        "job_run_id": "STRING",
        "table_name": "STRING",
        "discard_reason": "STRING",
        "discarded": "BIGINT",
        "recorded_at": "TIMESTAMP",
    },
}

# LAYER-SPECIFIC, declared but not mandated everywhere.
#
# The manifest is BRONZE'S. Each layer records what IT did, and the downstream layer reads
# upstream's -- so Bronze records deliveries in its own control schema rather than reaching
# into silver's, and silver's loop-1 reads it from there. That is why the Bronze request
# changes; see the spec's section 9.
#
# Gold declares nothing of its own yet: a projection layer has no rejects to supersede and no
# expectations until someone declares them. An empty dict is the honest statement, not an
# omission.
LAYER_TABLES: dict[str, dict[str, dict[str, str]]] = {
    "bronze": {
        "ctl_delivery_manifest": {
            "manifest_id": "STRING",
            "source_system": "STRING",
            "delivered_count": "BIGINT",
            "delivered_at": "TIMESTAMP",
            "delivered_by": "STRING",
        },
    },
    "silver": {
        "ctl_approval_manifest": {
            "manifest_id": "STRING",
            "approved_count": "BIGINT",
            "source_system": "STRING",
            "approved_at": "TIMESTAMP",
            "approved_by": "STRING",
        },
        "ref_dq_expectation": {
            "dataset": "STRING",
            "rule_name": "STRING",
            "rule_sql": "STRING",
            "is_current": "BOOLEAN",
            "severity": "STRING",
        },
        "ctl_quarantine_superseded": {
            "manifest_id": "STRING",
            "table_name": "STRING",
            "reject_digest": "STRING",
            "rulebook_version": "STRING",
            "superseded_by": "STRING",
            "reason": "STRING",
            "recorded_at": "TIMESTAMP",
        },
    },
    "gold": {},
}

# THE SPLIT IS THE DESIGN. An audit records WHAT HAPPENED and must never be rewritten --
# append_only_check enforces it, and a layer whose audit can be edited has an audit nobody can
# rely on. A config table records WHAT SHOULD HAPPEN: an expectation gets corrected, a manifest
# gets superseded. Making those append-only would mean a mistyped rule could never be
# withdrawn. A conforming layer must get the split right, not set the property everywhere.
APPEND_ONLY: frozenset[str] = frozenset(
    set(CORE) | {"ctl_quarantine_superseded"}
)
MUTABLE: frozenset[str] = frozenset(
    {"ctl_approval_manifest", "ref_dq_expectation", "ctl_delivery_manifest"}
)

# ONE COLUMN SHAPE, THREE MEANINGS -- declared, never assumed. Without this a reader compares
# bronze's accepted with silver's and concludes rows were lost, when the difference is
# deduplication working as designed.
STAGED_MEANING: dict[str, str] = {
    "bronze": "staged = rows read from the delivered file; "
              "accepted = rows written to <source>_raw",
    "silver": "staged = rows read from the staging log; "
              "accepted = rows inserted into the vault table",
    "gold": "staged = rows read from the vault; "
            "accepted = rows published to the projection",
}


def tables_for(layer: str) -> dict[str, dict[str, str]]:
    """The core plus that layer's own tables. Raises on an unknown layer rather than
    returning the bare core, because a typo would otherwise look like a conforming layer
    that simply declares nothing extra."""
    if layer not in LAYER_TABLES:
        raise KeyError(
            f"{layer!r} is not a layer this standard covers: {LAYERS}. Returning just the "
            f"core for an unknown name would make a typo indistinguishable from a layer that "
            f"declares no tables of its own."
        )
    return {**CORE, **LAYER_TABLES[layer]}
```

- [ ] **Step 4: Run to verify it passes**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Prove every check can fail**

For each: apply, confirm the *named* check reports **FAIL** with the suite still completing,
restore exactly. **Commit first** so a restore cannot lose work.

1. Remove `"aud_load_run"` from `CORE` → FAIL `the mandatory core is exactly three audit tables`
2. Change `LAYERS` to `("bronze", "silver")` → FAIL `every layer the standard names has a table set`
3. Make `tables_for` return `LAYER_TABLES[layer]` without the core → FAIL `tables_for() returns the core plus that layer's own`
4. Move `ctl_delivery_manifest` from `bronze` to `silver` → FAIL `bronze owns the delivery manifest and silver does not`
5. Remove `ref_dq_expectation` from `silver` → FAIL `silver keeps the expectation reference and the supersede ledger`
6. Remove `aud_load_run` from `APPEND_ONLY` → FAIL `every core table is append-only`
7. Add `"ref_dq_expectation"` to `APPEND_ONLY` → FAIL `the expectation reference and the manifest are MUTABLE, not append-only`
8. Delete the `"gold"` entry from `STAGED_MEANING` → FAIL `every layer declares what staged and accepted MEAN there`

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/control_standard.py tests/test_accelerator.py
git commit -m "Declare the cross-layer control standard"
```

---

## Task 2: Verify silver against the declaration

**Files:**
- Modify: `verify_repo.py` — append a new section at the end
- Test: the checks ARE the deliverable

**Interfaces:**
- Consumes: `control_standard.CORE`, `.LAYER_TABLES`, `.APPEND_ONLY`, `.MUTABLE`,
  `.tables_for`; and `apply_control_objects.declared_columns` (already exists — do NOT write a
  second SQL parser).
- Produces: nothing importable.

- [ ] **Step 1: Write the gate**

Append to `verify_repo.py`:

```python
# --------------------------------------------------------------------------- #
print("\n[standard] silver's control DDL matches the declaration")

from accelerator import control_standard as _cs  # noqa: E402

sys.path.insert(0, str(ROOT / "checks"))
import apply_control_objects as _aco2  # noqa: E402

# VERIFIED, NOT GENERATED. control_objects.sql is deployed, gated and working; rewriting it to
# make the declaration authoritative would risk it for no functional gain. Verifying agreement
# gets the same guarantee -- and this is the check that makes the declaration authoritative,
# so if it ever goes vacuous the standard becomes decoration.
_cs_silver_want = _cs.tables_for("silver")
_cs_silver_have = _aco2.declared_columns(
    (ROOT / "governance" / "control_objects.sql").read_text(encoding="utf-8"))

check("declared_columns() parsed silver's DDL at all",
      len(_cs_silver_have) == len(_cs_silver_want),
      f"parsed {sorted(_cs_silver_have)} against a declaration of "
      f"{sorted(_cs_silver_want)} -- every check below compares these two, and comparing "
      f"nothing would report agreement")

_cs_missing_tables = sorted(set(_cs_silver_want) - set(_cs_silver_have))
_cs_extra_tables = sorted(set(_cs_silver_have) - set(_cs_silver_want))
check("silver's control DDL declares exactly the tables the standard names for silver",
      not _cs_missing_tables and not _cs_extra_tables,
      f"missing from the DDL: {_cs_missing_tables}; in the DDL but not the standard: "
      f"{_cs_extra_tables}. The standard is the authority, so a difference means one of the "
      f"two is wrong -- decide which, do not silence the check")

_cs_col_drift = []
for _t in sorted(set(_cs_silver_want) & set(_cs_silver_have)):
    _want_cols = set(_cs_silver_want[_t])
    _have_cols = set(_cs_silver_have[_t])
    if _want_cols != _have_cols:
        _cs_col_drift.append(
            f"{_t}: standard-only {sorted(_want_cols - _have_cols)}, "
            f"DDL-only {sorted(_have_cols - _want_cols)}")
check("every silver control table has exactly the columns the standard declares",
      not _cs_col_drift,
      f"{_cs_col_drift} -- the column that went missing for a day on 27 Aug (`severity`) is "
      f"exactly this class, and it was found by an INSERT failing rather than by a check")

# THE APPEND-ONLY SPLIT, read off the DDL text. An audit that can be rewritten and a rule that
# cannot be corrected are both wrong, in opposite directions.
_cs_ao_wrong = []
_cs_ddl_text = (ROOT / "governance" / "control_objects.sql").read_text(encoding="utf-8")
for _t in sorted(_cs_silver_want):
    _block = _cs_ddl_text.split(f".{_t} (", 1)
    _decl = _block[1].split(";", 1)[0] if len(_block) > 1 else ""
    _is_ao = "delta.appendOnly' = 'true'" in _decl
    if _t in _cs.APPEND_ONLY and not _is_ao:
        _cs_ao_wrong.append(f"{_t}: must be append-only and is not")
    if _t in _cs.MUTABLE and _is_ao:
        _cs_ao_wrong.append(f"{_t}: must be mutable and is append-only")
check("silver's control tables match the standard's append-only split",
      _cs_silver_want and not _cs_ao_wrong,
      f"{_cs_ao_wrong} -- an audit records what happened and must never be rewritten; a rule "
      f"records what should happen and must be correctable")
```

- [ ] **Step 2: Run to verify it passes**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py`
Expected: `VERIFICATION PASSED`

If any check fails, **the declaration is wrong, not silver** — silver is the reference
implementation and it is deployed. Correct `control_standard.py` to match the DDL, and say so
in the commit message.

- [ ] **Step 3: Prove the gate can fail**

1. In `control_standard.py`, add `"note": "STRING"` to `CORE["aud_load_run"]` →
   FAIL `every silver control table has exactly the columns the standard declares`
2. Add a fourth table `"aud_probe": {"x": "STRING"}` to `CORE` →
   FAIL `silver's control DDL declares exactly the tables the standard names for silver`
3. Move `"ref_dq_expectation"` from `MUTABLE` to `APPEND_ONLY` →
   FAIL `silver's control tables match the standard's append-only split`

Restore after each.

- [ ] **Step 4: Commit**

```bash
git add verify_repo.py
git commit -m "Verify silver's control DDL against the declaration"
```

---

## Task 3: Emit gold's DDL and the bronze contract

**Files:**
- Create: `tools/emit_control_contract.py`
- Create (generated): `governance/control_objects_gold.sql`, `control_contracts/bronze.yaml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `control_standard.tables_for`, `.APPEND_ONLY`, `.MUTABLE`, `.STAGED_MEANING`,
  `.CORE`.
- Produces:
  - `gold_ddl() -> str` — the full text of gold's control DDL.
  - `bronze_contract() -> dict` — the contract structure.
  - `render(structure: dict) -> str` — deterministic YAML, trailing newline.
  - `GOLD_DDL_PATH: pathlib.Path`, `BRONZE_CONTRACT_PATH: pathlib.Path`
  - `main() -> None` — writes both.

- [ ] **Step 1: Write the failing test**

```python
sys.path.insert(0, str(ROOT / "tools"))
import emit_control_contract as _ecc  # noqa: E402

_ecc_gold = _ecc.gold_ddl()

check("gold's DDL creates the core and nothing gold does not declare",
      all(f".{t} (" in _ecc_gold for t in _cs.CORE)
      and "ctl_delivery_manifest" not in _ecc_gold
      and "ref_dq_expectation" not in _ecc_gold,
      "gold declares no tables of its own, so its DDL is the core alone -- a manifest or an "
      "expectation reference appearing there would be a layer claiming another's role")

check("gold's DDL parameterises the catalog and schema, never a literal",
      "${gold_catalog}" in _ecc_gold and "${control_schema}" in _ecc_gold
      and "03_usnc" not in _ecc_gold,
      "a literal catalog in shared DDL is how one lake's objects land in another -- the "
      "bundle resolves these per target")

check("every core table in gold's DDL is append-only",
      _ecc_gold.count("'delta.appendOnly' = 'true'") == len(_cs.CORE),
      f"{_ecc_gold.count(chr(39) + 'delta.appendOnly' + chr(39) + ' = ' + chr(39) + 'true' + chr(39))} "
      f"append-only markers for {len(_cs.CORE)} core tables")

_ecc_dupe_props = []
for _seg in _ecc_gold.split("TBLPROPERTIES (")[1:]:
    _keys = [k.split("=")[0].strip() for k in _seg.split(")")[0].split(",")]
    if len(_keys) != len(set(_keys)):
        _ecc_dupe_props.append(_keys)
check("no TBLPROPERTIES clause in gold's DDL repeats a key",
      _ecc_gold.count("TBLPROPERTIES (") == len(_cs.CORE) and not _ecc_dupe_props,
      f"{_ecc_dupe_props} -- a repeated key is what an earlier draft produced for any table "
      f"that was not append-only, by using the control-object marker as the conditional "
      f"property and then appending it again")

check("gold's DDL carries no semicolon inside a COMMENT literal",
      all(";" not in seg.split("'")[1] for seg in _ecc_gold.split("COMMENT ")[1:]
          if "'" in seg),
      "the statement splitter cuts on a semicolon regardless of quoting, so one inside a "
      "COMMENT splits the file into fragments -- it has happened twice in this repo")

_ecc_contract = _ecc.bronze_contract()

check("the bronze contract requires the core and the delivery manifest",
      set(_ecc_contract["tables"]) == set(_cs.tables_for("bronze")),
      f"contract names {sorted(_ecc_contract['tables'])} against "
      f"{sorted(_cs.tables_for('bronze'))}")

check("the bronze contract states what staged and accepted mean in BRONZE",
      _cs.STAGED_MEANING["bronze"] in json.dumps(_ecc_contract),
      "three layers share one column shape and three meanings; a contract that omits the "
      "meaning invites bronze's accepted being compared with silver's")

check("the bronze contract states the append-only split per table",
      all(_ecc_contract["tables"][t]["append_only"] is (t in _cs.APPEND_ONLY)
          for t in _ecc_contract["tables"]),
      "a conforming layer must get the split right, not set the property everywhere -- so the "
      "contract has to say which is which")

check("the bronze contract carries the invariant it must satisfy",
      "staged = accepted + sum(discarded)" in json.dumps(_ecc_contract),
      "the invariant is what makes the core worth mandating; a contract listing columns "
      "without it asks for a shape rather than a guarantee")

check("render() is deterministic and ends with exactly one newline",
      _ecc.render(_ecc.bronze_contract()) == _ecc.render(_ecc.bronze_contract())
      and _ecc.render(_ecc_contract).endswith("\n")
      and not _ecc.render(_ecc_contract).endswith("\n\n"),
      "an unstable render makes the no-op regeneration gate flap")
```

- [ ] **Step 2: Run to verify it fails**

Expected: `ModuleNotFoundError: No module named 'emit_control_contract'`

- [ ] **Step 3: Write the emitter**

Create `tools/emit_control_contract.py`:

```python
"""Render the control standard into what each layer needs.

GENERATE WHAT WE OWN, CONTRACT WHAT WE DO NOT. Gold's catalog is ours, so its DDL is generated
here and verify_repo asserts regeneration is a no-op. Bronze's catalog belongs to another team,
so they get a published contract and checks/control_conformance_check.py verifies the live lake
against it. Silver is neither: its DDL already exists and is verified against the declaration
rather than replaced -- see verify_repo's [standard] section.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import control_standard as cs  # noqa: E402

GOLD_DDL_PATH = ROOT / "governance" / "control_objects_gold.sql"
BRONZE_CONTRACT_PATH = ROOT / "control_contracts" / "bronze.yaml"

INVARIANT = "staged = accepted + sum(discarded)"


def gold_ddl() -> str:
    """Gold's control DDL: the core alone, because gold declares nothing of its own.

    NO SEMICOLON MAY APPEAR INSIDE A COMMENT LITERAL. The statement splitter in
    checks/apply_control_objects.py cuts on that character regardless of quoting, so one inside
    a COMMENT splits the file into fragments. It has caught exactly that twice in this repo.
    """
    lines = [
        "-- GENERATED from src/accelerator/control_standard.py. Do not hand-edit.",
        "-- verify_repo.py fails the build if regenerating this produces a diff.",
        "--",
        "-- Gold declares no control tables of its own: a projection layer has no rejects to",
        "-- supersede and no expectations until someone declares them. This is the mandatory",
        f"-- core alone, and the invariant it must satisfy is {INVARIANT}",
        "--",
        f"-- In gold, {cs.STAGED_MEANING['gold']}",
        "",
    ]
    for table, columns in cs.tables_for("gold").items():
        cols = ",\n".join(f"  {name:<20} {sql_type}"
                          for name, sql_type in columns.items())
        # THE MARKER IS UNCONDITIONAL, THE APPEND-ONLY PROPERTY IS NOT. An earlier draft set
        # `prop` to the marker when a table was not append-only and then appended the marker
        # again, producing a duplicate TBLPROPERTIES key. Latent while gold declares only
        # append-only core tables, and broken the moment it declares a config table.
        props = ["'hfig.control_object' = 'true'"]
        if table in cs.APPEND_ONLY:
            props.insert(0, "'delta.appendOnly' = 'true'")
        lines.append(
            f"CREATE TABLE IF NOT EXISTS `${{gold_catalog}}`.`${{control_schema}}`.{table} (\n"
            f"{cols}\n"
            f")\n"
            f"TBLPROPERTIES ({', '.join(props)});\n"
        )
    return "\n".join(lines)


def bronze_contract() -> dict:
    """The published contract for the Bronze team.

    Says what to build, what each column means in BRONZE specifically, which tables are
    append-only and which must stay mutable, and the invariant a conforming layer satisfies.
    A contract that lists columns without the invariant asks for a shape rather than a
    guarantee.
    """
    return {
        "layer": "bronze",
        "owner": "Bronze / ingestion team",
        "generated_from": "src/accelerator/control_standard.py",
        "schema_name": "control",
        "invariant": f"{INVARIANT}, per (job_run_id, table_name)",
        "column_meanings": cs.STAGED_MEANING["bronze"],
        "why_bronze_owns_the_manifest": (
            "Each layer's control schema records what THAT layer did, and the downstream layer "
            "reads upstream's. Bronze records deliveries here rather than writing into "
            "silver's control schema, and silver's loop-1 reconciliation reads them from here."
        ),
        "tables": {
            table: {
                "columns": dict(columns),
                "append_only": table in cs.APPEND_ONLY,
                "note": ("records what happened and must never be rewritten"
                         if table in cs.APPEND_ONLY
                         else "records what should happen and must stay correctable"),
            }
            for table, columns in cs.tables_for("bronze").items()
        },
    }


def render(structure: dict) -> str:
    """Deterministic text: sort_keys so key order cannot drift, one trailing newline so the
    committed file does not diff on whitespace."""
    return yaml.safe_dump(structure, sort_keys=True, default_flow_style=False) + ""


def main() -> None:
    GOLD_DDL_PATH.write_text(gold_ddl(), encoding="utf-8")
    print(f"wrote {GOLD_DDL_PATH.relative_to(ROOT)}")
    BRONZE_CONTRACT_PATH.parent.mkdir(exist_ok=True)
    BRONZE_CONTRACT_PATH.write_text(render(bronze_contract()), encoding="utf-8")
    print(f"wrote {BRONZE_CONTRACT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Generate and confirm**

```bash
/mnt/projects/hda-edm-dv/.venv/bin/python tools/emit_control_contract.py
/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py
```
Expected: two files written; `ALL CHECKS PASSED`.

If `render()` does not end in exactly one newline, fix it in `render()` rather than in the
check — `yaml.safe_dump` already appends one, so `+ ""` is deliberate; if your yaml version
does not, use `+ "\n"` and re-run.

- [ ] **Step 5: Prove four checks can fail**

1. In `gold_ddl()`, replace `${gold_catalog}` with `03_usnc_gold_edm_dev` →
   FAIL `gold's DDL parameterises the catalog and schema, never a literal`
2. In `control_standard.py`, add `"ctl_delivery_manifest"` to `LAYER_TABLES["gold"]` →
   FAIL `gold's DDL creates the core and nothing gold does not declare`
3. In `bronze_contract()`, delete the `"invariant"` key →
   FAIL `the bronze contract carries the invariant it must satisfy`
3a. **The COMMENT-semicolon check is VACUOUS today and must still be proven.** `gold_ddl()`
   emits its commentary as `--` lines and uses no `COMMENT '...'` literal, so that check
   currently passes by having nothing to examine. Prove it anyway: add
   `COMMENT 'a; b'` to one table's DDL in `gold_ddl()` and confirm the named check reports
   FAIL. A guard never seen to fire is indistinguishable from an absent one — and the
   statement splitter cutting on a semicolon regardless of quoting has bitten this repo twice.
3b. In `gold_ddl()`, set `props = ["'hfig.control_object' = 'true'", "'hfig.control_object' = 'true'"]`
   → FAIL `no TBLPROPERTIES clause in gold's DDL repeats a key`
4. In `bronze_contract()`, hard-code `"append_only": True` for every table →
   FAIL `the bronze contract states the append-only split per table`

Restore after each and regenerate.

- [ ] **Step 6: Commit**

```bash
git add tools/emit_control_contract.py governance/control_objects_gold.sql \
        control_contracts/bronze.yaml tests/test_accelerator.py
git commit -m "Emit gold's control DDL and the bronze contract from the declaration"
```

---

## Task 4: Gate regeneration of both artefacts

**Files:**
- Modify: `verify_repo.py` — extend the `[standard]` section from Task 2

**Interfaces:**
- Consumes: `emit_control_contract.gold_ddl`, `.bronze_contract`, `.render`,
  `.GOLD_DDL_PATH`, `.BRONZE_CONTRACT_PATH`.
- Produces: nothing importable.
- **Depends on Task 2 having run:** this code uses `_cs` and `sys.path`'s `checks` entry, both
  established by Task 2's block in the same `[standard]` section of `verify_repo.py`. That is
  why no `control_standard` import appears below. Do NOT add a second one — append after
  Task 2's block.

- [ ] **Step 1: Write the gate**

```python
import emit_control_contract as _ecc2  # noqa: E402

# THE DIFF IS THE REVIEW -- the same discipline metadata/key_composition.json and
# data_contracts/ already carry. CAUGHT rather than raised: an exception escaping a
# module-level block aborts verify_repo.py and turns every later check silently ABSENT, which
# is worse than one red check carrying the message.
_ecc_stale = []
try:
    if _ecc2.GOLD_DDL_PATH.read_text(encoding="utf-8") != _ecc2.gold_ddl():
        _ecc_stale.append(_ecc2.GOLD_DDL_PATH.name)
except Exception as _exc:  # noqa: BLE001
    _ecc_stale.append(f"{_ecc2.GOLD_DDL_PATH.name}: {type(_exc).__name__}: {_exc}")
try:
    if (_ecc2.BRONZE_CONTRACT_PATH.read_text(encoding="utf-8")
            != _ecc2.render(_ecc2.bronze_contract())):
        _ecc_stale.append(_ecc2.BRONZE_CONTRACT_PATH.name)
except Exception as _exc:  # noqa: BLE001
    _ecc_stale.append(f"{_ecc2.BRONZE_CONTRACT_PATH.name}: {type(_exc).__name__}: {_exc}")

check("regenerating gold's DDL and the bronze contract is a no-op",
      not _ecc_stale,
      f"stale: {_ecc_stale} -- run tools/emit_control_contract.py and review the diff")

# The bronze contract is a document another team acts on. A stale one is worse than none.
_ecc_contract_text = (_ecc2.BRONZE_CONTRACT_PATH.read_text(encoding="utf-8")
                      if _ecc2.BRONZE_CONTRACT_PATH.is_file() else "")
check("the committed bronze contract names every core table",
      _ecc_contract_text and all(t in _ecc_contract_text for t in _cs.CORE),
      f"missing from the committed contract: "
      f"{[t for t in _cs.CORE if t not in _ecc_contract_text]} -- another team acts on this "
      f"file, and it is empty or short")
```

- [ ] **Step 2: Run to verify it passes**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py`
Expected: `VERIFICATION PASSED`

- [ ] **Step 3: Prove it can fail**

1. Hand-edit `control_contracts/bronze.yaml` — change the `owner` value →
   FAIL `regenerating gold's DDL and the bronze contract is a no-op`. Regenerate to restore.
2. Move `control_contracts/bronze.yaml` aside →
   FAIL **both** the no-op check and `the committed bronze contract names every core table`.
   Restore.

- [ ] **Step 4: Commit**

```bash
git add verify_repo.py
git commit -m "Gate regeneration of gold's DDL and the bronze contract"
```

---

## Task 5: The live conformance check

**Files:**
- Create: `checks/control_conformance_check.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `control_standard.tables_for`, `.APPEND_ONLY`, `.MUTABLE`.
- Produces:
  - `conformance(layer: str, deployed: dict[str, set[str]]) -> list[str]` — pure.
  - `append_only_conformance(layer: str, append_only_tables: set[str]) -> list[str]` — pure.
  - `main() -> int`.

**This is the only task that reads a live lake.** Its pure functions are testable offline
against fabricated rows, in the shape `schema_grant_check.py` uses for its four decision
functions. **Do not run it against a workspace as part of this task** — running it is a
separate, consented act, and the profile is the human's choice.

- [ ] **Step 1: Write the failing test**

```python
import control_conformance_check as _ccc  # noqa: E402

_ccc_full = {t: set(c) for t, c in _cs.tables_for("bronze").items()}

check("conformance() is silent on a layer that matches the standard",
      _ccc.conformance("bronze", _ccc_full) == [],
      "a check that fires on a conforming layer gets suppressed")

check("conformance() reports an ABSENT table distinctly from a wrong-columns table",
      (len(_ccc.conformance("bronze", {})) >= 1
       and any("absent" in p.lower() for p in _ccc.conformance("bronze", {}))),
      "a layer that has built nothing and a layer that built it wrong need different "
      "answers -- one is 'start here', the other is 'you are close'")

_ccc_wrong = {t: set(c) for t, c in _ccc_full.items()}
_ccc_wrong["aud_table_load"] = _ccc_wrong["aud_table_load"] - {"staged"}
check("conformance() names the missing column, not just the table",
      any("staged" in p for p in _ccc.conformance("bronze", _ccc_wrong)),
      "an operator cannot act on 'aud_table_load is wrong'")

check("conformance() reports an extra column too",
      any("stray" in p for p in _ccc.conformance(
          "bronze", {**_ccc_full, "aud_load_run": _ccc_full["aud_load_run"] | {"stray"}})),
      "a column the standard does not name is drift in the other direction, and is governed "
      "by accident")

# tables_for() RAISES on an unknown layer, deliberately, so a typo cannot read as a layer
# that simply declares nothing extra. Asserted as a raise, not as a truthy expression: an
# earlier draft of this check read `... != [] or True`, which can never fail.
_ccc_raised = False
try:
    _ccc.conformance("silvr", _ccc_full)
except KeyError:
    _ccc_raised = True
check("conformance() RAISES on an unknown layer rather than treating it as conforming",
      _ccc_raised,
      "a typo'd layer name returned a result instead of raising -- and a layer that returns "
      "no problems is indistinguishable from a conforming one")

check("append_only_conformance() catches an audit table that is not append-only",
      any("aud_load_run" in p for p in _ccc.append_only_conformance(
          "bronze", {"ctl_delivery_manifest"})),
      "an audit that can be rewritten is an audit nobody can rely on")

check("append_only_conformance() catches a config table that IS append-only",
      any("ctl_delivery_manifest" in p for p in _ccc.append_only_conformance(
          "bronze", set(_cs.CORE) | {"ctl_delivery_manifest"})),
      "an append-only manifest means a wrong delivery count can never be corrected")

check("append_only_conformance() is silent when the split is right",
      _ccc.append_only_conformance("bronze", set(_cs.CORE)) == [],
      "the correct split must not fire")
```

- [ ] **Step 2: Run to verify it fails**

Expected: `ModuleNotFoundError: No module named 'control_conformance_check'`

- [ ] **Step 3: Write the check**

Create `checks/control_conformance_check.py`:

```python
"""HARD GATE: does a layer's control schema match the published standard?

WHY A CHECK AND NOT GENERATED DDL. Bronze's catalog belongs to another team. We specify what a
control schema must contain and verify whether theirs does; we do not deploy into it. Gold's
DDL is generated because that catalog is ours, and silver's is verified against the declaration
because it already exists and works.

THE DECISION FUNCTIONS ARE PURE AND SPARK-FREE, like offending(), unauthorised_table_readers(),
undeclared_schemas() and misplaced_control_objects() in schema_grant_check.py. That is what
makes the FAILING case testable offline against fabricated rows rather than only against a live
lake -- a gate whose red path can only be seen in production is a gate nobody exercises.

NOT-INSTRUMENTED IS A REPORTED STATE, NOT A FAILURE. Bronze has no control schema and gold has
no catalog as of 27 Aug 2026, and that is expected for some time. A layer that has built nothing
is reported as absent so an operator sees where to start; it does not fail the build, because
failing on the normal case trains people to ignore the gate.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import control_standard as cs  # noqa: E402

GATE = "control_conformance"


def conformance(layer: str, deployed: dict) -> list:
    """Problems for a layer whose control schema does not match the standard.

    ABSENT and WRONG-COLUMNS are reported differently on purpose: a team that has built nothing
    needs 'start here', and a team that built it slightly wrong needs 'you are close'. Reporting
    both as 'non-conforming' tells neither of them what to do.
    """
    want = cs.tables_for(layer)
    problems = []
    for table in sorted(want):
        if table not in deployed:
            problems.append(
                f"{layer}: table {table} is ABSENT. The standard requires it -- see "
                f"control_contracts/{layer}.yaml for its columns and meaning."
            )
            continue
        missing = sorted(set(want[table]) - set(deployed[table]))
        extra = sorted(set(deployed[table]) - set(want[table]))
        if missing:
            problems.append(
                f"{layer}: {table} is missing column(s) {missing}. The invariant "
                f"staged = accepted + sum(discarded) cannot be evaluated without them."
            )
        if extra:
            problems.append(
                f"{layer}: {table} has column(s) {extra} the standard does not name. Either "
                f"add them to the standard or drop them -- an undeclared column in a control "
                f"schema is governed by accident."
            )
    return problems


def append_only_conformance(layer: str, append_only_tables: set) -> list:
    """Problems where a layer's append-only split disagrees with the standard.

    Both directions are wrong. An audit that can be rewritten cannot be relied on. A config
    table that cannot be rewritten means a mistyped rule or a wrong delivery count can never be
    corrected.
    """
    want = cs.tables_for(layer)
    problems = []
    for table in sorted(want):
        should = table in cs.APPEND_ONLY
        does = table in append_only_tables
        if should and not does:
            problems.append(
                f"{layer}: {table} records what happened and MUST be delta.appendOnly -- an "
                f"audit that can be rewritten is an audit nobody can rely on."
            )
        if not should and does:
            problems.append(
                f"{layer}: {table} records what should happen and must stay MUTABLE -- "
                f"append-only would mean a wrong entry could never be corrected."
            )
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", default="control")
    ap.add_argument("--layer", required=True, choices=list(cs.LAYERS))
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    rows = spark.sql(
        f"SELECT lower(table_name) t, lower(column_name) c "
        f"FROM `{args.catalog}`.information_schema.columns "
        f"WHERE lower(table_schema) = '{args.control_schema.lower()}'").collect()
    deployed: dict = {}
    for r in rows:
        deployed.setdefault(r["t"], set()).add(r["c"])

    if not deployed:
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        print(f"{args.layer}: no control schema in {args.catalog}.{args.control_schema}. "
              f"NOT INSTRUMENTED -- this is the expected state until the owning team stands "
              f"it up. See control_contracts/{args.layer}.yaml.")
        return 0

    ao = {r["t"] for r in spark.sql(
        f"SELECT lower(table_name) t FROM `{args.catalog}`.information_schema.tables "
        f"WHERE lower(table_schema) = '{args.control_schema.lower()}'").collect()
        if spark.sql(
            f"SHOW TBLPROPERTIES `{args.catalog}`.`{args.control_schema}`.`{r['t']}`"
        ).where("key = 'delta.appendOnly' AND value = 'true'").count() > 0}

    problems = conformance(args.layer, deployed) + append_only_conformance(args.layer, ao)
    for p in problems:
        print(f"  * {p}")
    status = "FAILED" if problems else "PASSED"
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={len(cs.tables_for(args.layer))} "
          f"not_evaluated=0")
    return 1 if problems else 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for exit code 0. Exit
    # explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
```

- [ ] **Step 4: Run to verify the tests pass**

```bash
/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py
/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py
```
Expected: `ALL CHECKS PASSED`; `VERIFICATION PASSED`.

- [ ] **Step 5: Prove the pure functions can fail**

1. In `conformance()`, change the absent branch to `continue` without appending →
   FAIL `conformance() reports an ABSENT table distinctly from a wrong-columns table`
2. In `conformance()`, drop the `missing` branch →
   FAIL `conformance() names the missing column, not just the table`
3. In `conformance()`, drop the `extra` branch →
   FAIL `conformance() reports an extra column too`
4. In `append_only_conformance()`, drop the `should and not does` branch →
   FAIL `append_only_conformance() catches an audit table that is not append-only`
5. In `append_only_conformance()`, drop the `not should and does` branch →
   FAIL `append_only_conformance() catches a config table that IS append-only`

Restore after each.

- [ ] **Step 6: Commit**

```bash
git add checks/control_conformance_check.py tests/test_accelerator.py
git commit -m "Add the live control-schema conformance check"
```

---

## Task 6: Revise the Bronze request

**Files:**
- Modify: `docs/loop1_control_table_request.html`

**Interfaces:**
- Consumes: `control_contracts/bronze.yaml` (the contract the page now points at).
- Produces: nothing importable.

**This page is going to another team. Accuracy outranks completeness.** Its current text asks
Bronze to write a manifest row into `02_<lake>_silver_edm.control.ctl_approval_manifest` — into
*our* control schema. That was corrected once already on 27 Aug (it previously named the
`governance` schema, which does not hold that table). It now changes again, for a reason.

- [ ] **Step 1: Read the page's current ask**

```bash
grep -n "control.ctl_approval_manifest\|What we need\|_manifest_id" \
  docs/loop1_control_table_request.html | head
```

Note the exact wording before editing. The `_manifest_id` column ask on the four feeds is
**unchanged** and must survive the edit intact.

- [ ] **Step 2: Rewrite the ask**

Replace the "Write one row to `02_usnc_silver_edm_dev.control.ctl_approval_manifest`" bullet
with a two-part ask, and add a short section explaining the change. The substance to convey,
in the page's existing voice:

* **What changes:** record deliveries in `01_<lake>_bronze.control.ctl_delivery_manifest` —
  Bronze's own control schema — instead of writing into silver's. Silver's loop-1
  reconciliation reads them from there.
* **Why:** each layer's control schema records what that layer did. Bronze stops reaching into
  a schema it does not own, and the manifest sits with the team that knows what a delivery is.
* **What does not change:** the `_manifest_id` column on the four bound feeds, and the
  measurement — `nhl_general_journal_line` holds 2,453,132 rows and zero non-null manifest ids,
  re-measured 27 Aug.
* **What is new and larger:** standing up a `control` schema in the bronze catalog at all.
  Bronze has 47 schemas today and none of them is a control schema — measured 27 Aug. Point at
  `control_contracts/bronze.yaml` for the exact tables and columns, and say that the three
  audit tables are the mandatory core while the manifest is bronze-specific.
* **Honest about the trade:** this is a bigger ask than the previous version of the page made.
  Say so rather than letting them discover it.

- [ ] **Step 3: Confirm the page still parses and the suites are green**

```bash
python3 -c "from html.parser import HTMLParser; HTMLParser().feed(open('docs/loop1_control_table_request.html').read()); print('parses')"
/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py
```
Expected: `parses`; `VERIFICATION PASSED`.

- [ ] **Step 4: Confirm no stale reference to the old ask survives**

```bash
grep -c "silver_edm_dev.control.ctl_approval_manifest" docs/loop1_control_table_request.html
```
Expected: `0` outside any section that explicitly describes the change. If a count above zero
is inside a "what changed" note, that is correct; anywhere else it is a leftover.

- [ ] **Step 5: Commit**

```bash
git add docs/loop1_control_table_request.html
git commit -m "Revise the Bronze ask: own your control schema, not ours"
```

---

## Task 7: Record the standard in OPEN_ITEMS

**Files:**
- Modify: `docs/superpowers/OPEN_ITEMS.md`

- [ ] **Step 1: Add the section**

Insert before the most recent dated section. Cover:

* What landed: the declaration as the single authority; silver verified against it rather than
  rewritten; gold's DDL and the bronze contract generated; the live conformance check.
* **What it does NOT do:** it does not instrument bronze or gold. It specifies and verifies.
  Standing them up is the owning team's work.
* The Bronze ask changed — the manifest moves to bronze's own control schema, and the request
  page was revised.
* Gold's DDL exists but the gold catalog does not, so it is unapplied by design.
* Plan 2, the cross-layer dashboard, is deliberately unwritten until a second layer has a
  control schema to show.
* That governance is out of scope, and why: bronze masks physically, silver uses UC column
  masks, and whether those are interchangeable is a data-protection judgement with no owner.

- [ ] **Step 2: Confirm the suites are green and commit**

```bash
/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py
git add docs/superpowers/OPEN_ITEMS.md
git commit -m "Record the cross-layer control standard"
```

---

## Notes for the executor

* **The spec is the authority.** Where this plan and the spec disagree, the spec wins.
* **If Task 2 fails, the declaration is wrong, not silver.** Silver is deployed, gated and
  working; it is the reference implementation. Correct `control_standard.py`.
* **Commit before mutation-testing.** A `git checkout` to undo a mutation discards the whole
  uncommitted file — that happened twice on 27 Aug.
* **Do not deploy, and do not run Task 5's `main()` against a workspace.** The profile is the
  human's choice and every profile but `hfig-usnc-tds` is EU production.
* **Do not build any cross-layer subtraction.** A drop between layers is expected; the tile
  that reports it as loss is forbidden by the spec and belongs to Plan 2's gates regardless.
