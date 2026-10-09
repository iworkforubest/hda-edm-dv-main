# Workday Reference Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring Workday's ReferenceID/TypeID values into the vault as one generic hub with an effectivity satellite, so the 108 `CHECKREFERENCES` fields in the customer-invoice DCDD can be satisfied.

**Architecture:** A committed inventory of reference types is derived from Logan's DCDDs by an emitter and byte-gated. A hand-authored exclusion list names the types already modelled as first-class entities. `hub_wd_reference` is keyed by `(reference_id_type, reference_id)`; `esat_wd_reference` hangs off that hub and is driven on its hash key. Because `Get_References` returns a complete set per type, effectivity is derived by diffing consecutive snapshots in a pure function, so `checks/load_satellites.py` is never modified and the esat sees ordinary delivered rows.

**Tech Stack:** Python 3.11 and 3.13 (both must pass), stdlib only for new code, PyYAML (already a dependency), Databricks serverless `spark_python_task`, Unity Catalog, Delta.

**Spec:** `docs/superpowers/specs/2026-09-27-workday-reference-data-design.md`

## Global Constraints

- **No `ref_` table is ever created.** `ref_` is in `naming.PLATFORM_OWNED`; this accelerator reads such objects by join only. This is the ARB boundary named in the spec.
- **`stg_` is never used for landing.** `naming.stg()` means "the SDP pipeline's append log for a staged entity" and DEF-42 rests on it. The interim schema is `workday_landing`.
- **Hashing is untouched.** SHA-256 as `BINARY(32)`, `HASHDIFF_UPPERCASE = False`. No task changes `src/accelerator/hashing.py`, so no task bumps `RULEBOOK_VERSION` or touches `tests/golden_hash_vectors.json`.
- **Tests are not pytest.** They are `check(name, condition, detail)` calls in `tests/test_accelerator.py` and `verify_repo.py`, run as plain scripts. An exception raised *inside* a check's condition aborts the whole suite, which is worse than a red check — guard every expression that can raise.
- **Every new check must be proven red.** Mutate the code it guards, observe the FAIL, restore, observe green. A check never demonstrated failing is assumed broken.
- **`verify_repo.py`'s check count never decreases.** It is 1358 at the start of this plan.
- **No new third-party dependencies.** `openpyxl` is not installed and must not be added; an `.xlsx` is a zip of XML and is read with `zipfile` + `xml.etree`.
- **Generated artefacts follow emit → commit → byte-gate.** The emitter writes the file, the file is committed, and a check fails the build if the two disagree.

## Review Focus

1. **A DCDD row whose `Type Value` is a data type, not a reference type.** `Text`, `Boolean` and `Date` appear in that column on `CHECKREFERENCES` rows. They must not enter the inventory. — Task 1.
2. **An empty or failed snapshot must never close every window.** A fetch that errors and writes zero rows would, under a naive diff, retire every reference Workday has. This is the most damaging failure in the plan. — Task 4.
3. **Two identical consecutive snapshots must emit nothing.** Re-running a fetch without change must not produce close-then-reopen churn in an insert-only satellite. — Task 4.
4. **A type in both the exclusion list and the inventory, or in neither.** Either is a silent modelling error: double-keyed, or unmodelled. — Task 2.
5. **An exclusion naming an entity that does not exist.** A typo disables landing for a whole type and looks like a deliberate exclusion. — Task 2.

---

### Task 1: Derive the reference-type inventory from the DCDDs

**Files:**
- Create: `tools/_xlsx.py`
- Create: `tools/emit_workday_reference_types.py`
- Create: `metadata/workday/reference_types.json` (generated)
- Modify: `verify_repo.py` (append checks near the other emitter byte-gates)

**Interfaces:**
- Produces: `tools._xlsx.load(path: str) -> dict[str, list[tuple[int, dict[str, str]]]]` — sheet name to `(row_number, {column_letter: value})`.
- Produces: `tools.emit_workday_reference_types.reference_types(sheets) -> dict[str, list[str]]` — DCDD file stem to sorted reference-type names.
- Produces: `metadata/workday/reference_types.json` with keys `generated_from`, `types` (sorted list), `by_dcdd` (mapping).

- [ ] **Step 1: Write the stdlib xlsx reader**

`tools/_xlsx.py`:

```python
"""Minimal .xlsx reader. STDLIB ONLY, deliberately.

openpyxl is not a dependency of this repo and must not become one for a file we read
five times in a build. An .xlsx is a zip of XML; that is all this needs to know.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
import zipfile

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
      "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}


def load(path: str) -> dict[str, list[tuple[int, dict[str, str]]]]:
    """Sheet name -> [(row number, {column letter: cell text})]. Empty cells omitted."""
    z = zipfile.ZipFile(path)
    shared: list[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % NS["m"])))
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = {r.get("Id"): r.get("Target")
            for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
    out: dict[str, list[tuple[int, dict[str, str]]]] = {}
    for sh in wb.find("m:sheets", NS):
        target = rels[sh.get("{%s}id" % NS["r"])]
        if not target.startswith("xl/"):
            target = "xl/" + target.lstrip("/")
        rows: list[tuple[int, dict[str, str]]] = []
        for row in ET.fromstring(z.read(target)).iter("{%s}row" % NS["m"]):
            cells: dict[str, str] = {}
            for c in row.findall("m:c", NS):
                col = re.match(r"[A-Z]+", c.get("r")).group()
                v, isx = c.find("m:v", NS), c.find("m:is", NS)
                if c.get("t") == "s" and v is not None:
                    val = shared[int(v.text)]
                elif isx is not None:
                    val = "".join(t.text or "" for t in isx.iter("{%s}t" % NS["m"]))
                elif v is not None:
                    val = v.text
                else:
                    val = None
                if val:
                    cells[col] = val
            if cells:
                rows.append((int(row.get("r")), cells))
        out[sh.get("name")] = rows
    return out
```

- [ ] **Step 2: Write the emitter, with the data-type exclusion**

`tools/emit_workday_reference_types.py`:

```python
#!/usr/bin/env python3
"""Derive the reference-ID types Logan's DCDDs demand, from the DCDDs themselves.

WHY DERIVED AND NOT TYPED OUT. The customer-invoice DCDD alone carries 108
CHECKREFERENCES fields. A hand-kept list would drift from the workbook the moment a
corrected DCDD lands, and the drift would be invisible: a missing type does not fail,
it silently fails to validate.

NOT EVERY `Type Value` ON A CHECKREFERENCES ROW IS A REFERENCE TYPE. The column also
carries plain data types -- Text, Boolean, Date -- on rows that are reference-validated
for other reasons. Those are excluded by name, and the exclusion is a closed set rather
than a heuristic so that a genuinely new reference type is never dropped by a pattern
that happened to match it.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from _xlsx import load  # noqa: E402

DCDD_DIR = ROOT / "metadata" / "workday" / "dcdd"
OUT_PATH = ROOT / "metadata" / "workday" / "reference_types.json"

#: Values that appear in `Type Value` but name a DATA type, not a reference type.
NOT_REFERENCE_TYPES = frozenset({
    "Text", "Boolean", "Date", "DateTime", "Numeric", "Decimal", "Integer",
})


def reference_types(sheets: dict) -> dict[str, list[str]]:
    """DCDD sheet -> sorted reference-ID type names it validates against."""
    out: dict[str, list[str]] = {}
    for name, rows in sheets.items():
        if name in ("Summary", "Comments") or not rows:
            continue
        header = {v: k for k, v in rows[0][1].items()}
        vcol, tcol = header.get("VALIDATIONS"), header.get("Type Value")
        if not vcol or not tcol:
            continue
        found: set[str] = set()
        for _r, cells in rows[1:]:
            if "CHECKREFERENCES" not in (cells.get(vcol) or ""):
                continue
            for raw in (cells.get(tcol) or "").split(","):
                t = raw.strip()
                if t and t not in NOT_REFERENCE_TYPES:
                    found.add(t)
        out[name] = sorted(found)
    return out


def render() -> str:
    by_dcdd: dict[str, list[str]] = {}
    for path in sorted(DCDD_DIR.glob("*.xlsx")):
        for sheet, types in reference_types(load(str(path))).items():
            by_dcdd[sheet] = types
    every = sorted({t for ts in by_dcdd.values() for t in ts})
    return json.dumps({
        "generated_from": "metadata/workday/dcdd/*.xlsx -- see PROVENANCE.json",
        "types": every,
        "by_dcdd": by_dcdd,
    }, indent=2) + "\n"


if __name__ == "__main__":
    OUT_PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)}")
```

- [ ] **Step 3: Generate the artefact and commit it**

```bash
.venv/bin/python tools/emit_workday_reference_types.py
.venv/bin/python -c "import json;d=json.load(open('metadata/workday/reference_types.json'));print(len(d['types']),'types across',len(d['by_dcdd']),'DCDDs')"
```

Expected: a count well under 293, because the data types are now excluded.

- [ ] **Step 4: Write the failing checks in `verify_repo.py`**

Append near the other emitter byte-gates:

```python
# --- Workday reference-type inventory (subsystem E, Task 1) --------------------------
_wrt_path = ROOT / "metadata" / "workday" / "reference_types.json"
_wrt_committed = _wrt_path.read_text(encoding="utf-8") if _wrt_path.exists() else ""
sys.path.insert(0, str(ROOT / "tools"))
try:
    import emit_workday_reference_types as _wrt_mod
    _wrt_rendered = _wrt_mod.render()
except Exception as _wrt_exc:  # noqa: BLE001 -- must not abort the suite
    _wrt_mod, _wrt_rendered = None, f"EMITTER RAISED: {_wrt_exc!r}"

check("reference_types.json is byte-identical to what its emitter renders",
      bool(_wrt_committed) and _wrt_committed == _wrt_rendered,
      "emit -> commit -> byte-gate: run tools/emit_workday_reference_types.py and "
      "commit the result, or the inventory and Logan's DCDDs have diverged")

_wrt_types = json.loads(_wrt_committed)["types"] if _wrt_committed else []
check("no DATA type leaked into the reference-type inventory",
      _wrt_mod is not None
      and not (set(_wrt_types) & set(_wrt_mod.NOT_REFERENCE_TYPES)),
      f"Text/Boolean/Date appear in the DCDD's Type Value column on CHECKREFERENCES "
      f"rows and are not reference types; found "
      f"{sorted(set(_wrt_types) & set(_wrt_mod.NOT_REFERENCE_TYPES)) if _wrt_mod else '?'}")

check("the inventory is not empty and every entry is a non-blank string",
      bool(_wrt_types) and all(isinstance(t, str) and t.strip() for t in _wrt_types),
      f"{len(_wrt_types)} type(s) -- an empty inventory means the extraction silently "
      f"matched nothing, which reads identically to 'no references needed'")
```

- [ ] **Step 5: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -3
```
Expected: `VERIFICATION PASSED`, count above 1358.

- [ ] **Step 6: Prove each new check can fail**

```bash
cp metadata/workday/reference_types.json /tmp/rt.bak
.venv/bin/python -c "
import json;p='metadata/workday/reference_types.json';d=json.load(open(p))
d['types'].append('Text');open(p,'w').write(json.dumps(d,indent=2)+'\n')"
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head
cp /tmp/rt.bak metadata/workday/reference_types.json
.venv/bin/python verify_repo.py 2>&1 | tail -2
```
Expected: the byte-gate AND the data-type check both go red, then both green again.

- [ ] **Step 7: Commit**

```bash
git add tools/_xlsx.py tools/emit_workday_reference_types.py \
        metadata/workday/reference_types.json verify_repo.py
git commit -m "Derive the Workday reference-type inventory from Logan's DCDDs"
```

---

### Task 2: The exclusion list, and the three checks that hold it honest

**Files:**
- Create: `metadata/workday/reference_exclusions.yml`
- Create: `src/accelerator/wd_reference.py`
- Modify: `tests/test_accelerator.py` (append)

**Interfaces:**
- Consumes: `metadata/workday/reference_types.json` from Task 1.
- Produces: `wd_reference.load_exclusions(path: Path) -> dict[str, str]` — reference type to the entity name that already models it.
- Produces: `wd_reference.classify(types: list[str], exclusions: dict[str, str]) -> tuple[list[str], list[str]]` — `(landed, excluded)`, both sorted.

- [ ] **Step 1: Author the exclusion list**

`metadata/workday/reference_exclusions.yml`:

```yaml
# REFERENCE TYPES THIS MODEL ALREADY CARRIES AS FIRST-CLASS ENTITIES.
#
# A type named here is NOT landed into hub_wd_reference. It already has a hub, and two
# answers to "what is this customer's key" is worse than none.
#
# THE VALUE IS THE ENTITY THAT MODELS IT, and it is checked against the loaded model --
# a typo here silently disables landing for a whole type and looks deliberate.
exclusions:
  Customer_ID: organisation
  Customer_Reference_ID: organisation
  Company_Reference_ID: legal_entity
  Organization_Reference_ID: organisation
  Supplier_ID: supplier
  Supplier_Reference_ID: supplier
  Employee_ID: worker
  Contingent_Worker_ID: worker
```

- [ ] **Step 2: Write the module**

`src/accelerator/wd_reference.py`:

```python
"""Which Workday reference types this vault lands, and which it already models.

PURE AND SPARK-FREE. Deciding what to land is a modelling decision, and a modelling
decision that can only be tested by running a pipeline does not get tested.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml


def load_types(path: Path) -> list[str]:
    """The generated inventory's `types` list."""
    return list(json.loads(path.read_text(encoding="utf-8"))["types"])


def load_exclusions(path: Path) -> dict[str, str]:
    """Reference type -> the entity name that already models it."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(raw.get("exclusions") or {})


def classify(types: list[str], exclusions: dict[str, str]
             ) -> tuple[list[str], list[str]]:
    """(landed, excluded). Every type goes to exactly one side -- never both, never
    neither. That totality is the property Task 2's checks assert."""
    landed = sorted(t for t in types if t not in exclusions)
    excluded = sorted(t for t in types if t in exclusions)
    return landed, excluded
```

- [ ] **Step 3: Write the failing checks**

Append to `tests/test_accelerator.py`:

```python
print("\n== Workday reference types: landed, excluded, never both or neither ==")
from accelerator import wd_reference as _wdr  # noqa: E402

_wdr_types = _wdr.load_types(ROOT / "metadata" / "workday" / "reference_types.json")
_wdr_excl = _wdr.load_exclusions(
    ROOT / "metadata" / "workday" / "reference_exclusions.yml")
_wdr_landed, _wdr_excluded = _wdr.classify(_wdr_types, _wdr_excl)
_wdr_entities = {e.name for e in model.entities}

check("every exclusion names an entity the model actually declares",
      all(v in _wdr_entities for v in _wdr_excl.values()),
      f"unknown: {sorted(v for v in _wdr_excl.values() if v not in _wdr_entities)} -- "
      f"a typo here disables landing for a whole reference type and is indistinguishable "
      f"from a deliberate exclusion")

check("no reference type is both landed and excluded",
      not (set(_wdr_landed) & set(_wdr_excluded)),
      f"both: {sorted(set(_wdr_landed) & set(_wdr_excluded))} -- it would be keyed twice, "
      f"in hub_wd_reference and in its own hub")

check("no reference type is neither landed nor excluded",
      set(_wdr_landed) | set(_wdr_excluded) == set(_wdr_types),
      f"neither: {sorted(set(_wdr_types) - set(_wdr_landed) - set(_wdr_excluded))} -- "
      f"a type must be modelled here or deliberately modelled elsewhere, never missing "
      f"from both")

check("the exclusion list is not empty -- Customer/Supplier/Company/Worker are modelled",
      bool(_wdr_excl),
      "an empty list means every Workday key lands in the generic hub, including the "
      "four this vault already has hubs for")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -3
```
Expected: `ALL CHECKS PASSED`.

- [ ] **Step 5: Prove the checks can fail**

```bash
cp metadata/workday/reference_exclusions.yml /tmp/ex.bak
sed -i 's/Customer_ID: organisation/Customer_ID: no_such_entity/' \
    metadata/workday/reference_exclusions.yml
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -3
cp /tmp/ex.bak metadata/workday/reference_exclusions.yml
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
```
Expected: the "names an entity the model actually declares" check goes red, then green.

- [ ] **Step 6: Commit**

```bash
git add metadata/workday/reference_exclusions.yml src/accelerator/wd_reference.py \
        tests/test_accelerator.py
git commit -m "Exclusion list: reference types this vault already models, held honest by three checks"
```

---

### Task 3: `hub_wd_reference` and `esat_wd_reference`

**Files:**
- Create: `metadata/entities/hub_wd_reference.yml`
- Create: `metadata/entities/esat_wd_reference.yml`
- Modify: `tests/test_accelerator.py` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks at runtime; the entities stand alone.
- Produces: entity names `wd_reference` (kind `hub`) and `wd_reference_effectivity` (kind `esat`), hash key column `wd_reference_hk`.

- [ ] **Step 1: Write the hub**

`metadata/entities/hub_wd_reference.yml`:

```yaml
# ONE HUB FOR EVERY WORKDAY REFERENCE VALUE, QUALIFIED BY ITS TYPE.
#
# The customer-invoice DCDD alone names hundreds of reference-ID types across its 108
# CHECKREFERENCES fields. A hub each would mean hundreds of hubs, hundreds of pipeline
# branches and hundreds of tables carrying one column of interest.
#
# FOLDING UNDER A TYPE QUALIFIER IS ALREADY AN ACCEPTED CALL HERE. metadata/volumes.yml
# records it: "hub_company and hub_client folded into ONE hub_organisation: legal
# entities (11 input_db codes) plus clients, qualified by reference_type."
#
# THE GRAIN IS HONEST. A Tax_Code_ID and a Currency_Reference_ID are not the same
# business object, but they are the same KIND of object: an identifier Workday issues,
# and may later withdraw. The natural key genuinely is the pair.
#
# TYPES THIS VAULT ALREADY MODELS ARE NOT LANDED HERE -- see
# metadata/workday/reference_exclusions.yml and the three checks that hold it to the
# inventory.
name: wd_reference
kind: hub
domain: reference
key_style: authored
business_keys: [reference_id_type, reference_id]
sensitivity: internal

sources:
  - name: WORKDAY
    # INTERIM LANDING. Bronze has no workday schema yet; when it does this binding moves
    # to 01_usnc_bronze_dev.workday_raw.reference_values and workday_landing is dropped.
    # NOT stg_: that prefix means the SDP pipeline's append log for a staged entity, and
    # DEF-42 rests on it.
    bronze_table: 02_usnc_silver_edm_dev.workday_landing.wd_reference_snapshot
    key_columns: [reference_id_type, reference_id]
    applied_dts_column: snapshot_dts
    dedup_by: [reference_id_type, reference_id]
    dedup_order: [snapshot_dts]
```

- [ ] **Step 2: Write the effectivity satellite**

`metadata/entities/esat_wd_reference.yml`:

```yaml
# WHEN EACH WORKDAY REFERENCE VALUE WAS OFFERED, driven on the reference itself.
#
# Get_References returns the COMPLETE current set for a type, so each retrieval is a
# snapshot and absence carries meaning: a value that stops being returned has been
# withdrawn. Without this, a Company or Tax Code inactivated in Workday would go on
# validating new rows forever, because nothing ever said otherwise.
#
# DRIVEN ON THE HUB'S OWN KEY: at most one window open per reference at a time.
# spec.py requires exactly one thing of an esat -- that it names a driving_key -- and
# exempts esat from needing a payload. Nothing requires an esat's parent to be a link;
# every existing one here happens to hang off one.
#
# THE CLOSE ROWS ARE DERIVED, NOT DELIVERED. accelerator.wd_reference.effectivity_rows()
# diffs consecutive snapshots and emits the retirement row, so this satellite sees
# ordinary delivered rows and checks/load_satellites.py is unchanged.
name: wd_reference_effectivity
kind: esat
domain: reference
sensitivity: internal

parents: [wd_reference]
payload: [reference_status]
driving_key: wd_reference_hk

notes: >
  Validity window per Workday reference value. Opened when a value first appears in a
  snapshot, closed when a later snapshot for the same type no longer returns it.

descriptions:
  reference_status: >-
    Whether Workday still offers this reference value. A withdrawal is delivered as a
    new row saying so, never as an update.

sources:
  - name: WORKDAY
    bronze_table: 02_usnc_silver_edm_dev.workday_landing.wd_reference_effectivity
    parent_keys:
      wd_reference: [reference_id_type, reference_id]
    payload: [reference_status]
    applied_dts_column: effective_from
```

- [ ] **Step 3: Write the failing checks**

Append to `tests/test_accelerator.py`:

```python
print("\n== the Workday reference hub and its effectivity satellite ==")
_wdr_m = spec.load_model(ROOT / "metadata" / "entities")
_wdr_hub = next((e for e in _wdr_m.entities if e.name == "wd_reference"), None)
_wdr_es = next((e for e in _wdr_m.entities
                if e.name == "wd_reference_effectivity"), None)

check("hub_wd_reference is keyed by (reference_id_type, reference_id), in that order",
      _wdr_hub is not None
      and list(_wdr_hub.business_keys) == ["reference_id_type", "reference_id"],
      f"business_keys={list(_wdr_hub.business_keys) if _wdr_hub else None} -- the pair "
      f"IS the natural key; either column alone is ambiguous across types")

check("the effectivity satellite is driven on the HUB's hash key, not a source column",
      _wdr_es is not None and _wdr_es.driving_key == "wd_reference_hk",
      f"driving_key={_wdr_es.driving_key if _wdr_es else None} -- driving on anything "
      f"else lets two windows sit open for one reference, which is the whole failure "
      f"this satellite exists to prevent")

check("the effectivity satellite's parent is the reference hub",
      _wdr_es is not None and list(_wdr_es.parents) == ["wd_reference"],
      f"parents={list(_wdr_es.parents) if _wdr_es else None}")

check("neither new entity binds to a ref_ table -- the ARB boundary holds",
      all(not any(part.startswith("ref_")
                  for part in (s.bronze_table or "").split("."))
          for e in (_wdr_hub, _wdr_es) if e is not None
          for s in e.sources),
      "ref_ is naming.PLATFORM_OWNED: this accelerator reads such objects by join and "
      "never creates or binds one as its own source")

check("neither new entity lands in a stg_ table",
      all("stg_" not in (s.bronze_table or "")
          for e in (_wdr_hub, _wdr_es) if e is not None
          for s in e.sources),
      "stg_ means the SDP pipeline's append log for a staged entity (DEF-42); landing "
      "here would put non-entity tables inside a namespace the gates assert over")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -3
.venv/bin/python verify_repo.py 2>&1 | tail -2
```
Expected: both pass. If `load_model` raises, the YAML violates a rule `spec.py` enforces — read the `SpecError` message, which names the entity and the rule.

- [ ] **Step 5: Prove the driving-key check can fail**

```bash
cp metadata/entities/esat_wd_reference.yml /tmp/es.bak
sed -i 's/^driving_key: wd_reference_hk/driving_key: reference_id/' \
    metadata/entities/esat_wd_reference.yml
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
cp /tmp/es.bak metadata/entities/esat_wd_reference.yml
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
```
Expected: the driving-key check goes red, then green.

- [ ] **Step 6: Commit**

```bash
git add metadata/entities/hub_wd_reference.yml metadata/entities/esat_wd_reference.yml \
        tests/test_accelerator.py
git commit -m "hub_wd_reference and its effectivity satellite, driven on the hub key"
```

---

### Task 4: Snapshot diffing — the effectivity rows, and the empty-snapshot guard

**Files:**
- Modify: `src/accelerator/wd_reference.py` (append)
- Modify: `tests/test_accelerator.py` (append)

**Interfaces:**
- Consumes: `src/accelerator/wd_reference.py` from Task 2, and the `_wdr` module alias Task 2 already bound in `tests/test_accelerator.py` — do not re-import it.
- Produces: `wd_reference.effectivity_rows(previous: dict[str, set[str]], current: dict[str, set[str]], snapshot_dts: str) -> list[dict[str, str]]` — rows with keys `reference_id_type`, `reference_id`, `reference_status`, `effective_from`. `reference_status` is `"OFFERED"` or `"WITHDRAWN"`.
- Produces: `wd_reference.EmptySnapshotError`.

This is the task Review Focus items 2 and 3 belong to.

- [ ] **Step 1: Write the failing checks FIRST**

Append to `tests/test_accelerator.py`:

```python
print("\n== Workday reference effectivity: opens, closes, and refuses empty snapshots ==")

_e_prev = {"Company_Reference_ID": {"LE001", "LE002"}, "Tax_Code_ID": {"T1"}}
_e_same = {"Company_Reference_ID": {"LE001", "LE002"}, "Tax_Code_ID": {"T1"}}
_e_gone = {"Company_Reference_ID": {"LE001"}, "Tax_Code_ID": {"T1"}}
_e_new = {"Company_Reference_ID": {"LE001", "LE002", "LE003"}, "Tax_Code_ID": {"T1"}}

_e_first = _wdr.effectivity_rows({}, _e_prev, "2026-09-27T00:00:00")
check("a first snapshot opens a window for every value it returns",
      sorted((r["reference_id"], r["reference_status"]) for r in _e_first)
      == [("LE001", "OFFERED"), ("LE002", "OFFERED"), ("T1", "OFFERED")],
      f"{_e_first!r}")

_e_noop = _wdr.effectivity_rows(_e_prev, _e_same, "2026-09-28T00:00:00")
check("an identical snapshot emits NOTHING -- no close-then-reopen churn",
      _e_noop == [],
      f"{_e_noop!r} -- the satellite is insert-only, so a re-run that changed nothing "
      f"must write nothing, or every fetch inflates history")

_e_closed = _wdr.effectivity_rows(_e_prev, _e_gone, "2026-09-28T00:00:00")
check("a value that stops being returned is WITHDRAWN, and only that value",
      [(r["reference_id"], r["reference_status"]) for r in _e_closed]
      == [("LE002", "WITHDRAWN")],
      f"{_e_closed!r}")

_e_added = _wdr.effectivity_rows(_e_prev, _e_new, "2026-09-28T00:00:00")
check("a newly appearing value is OFFERED, and only that value",
      [(r["reference_id"], r["reference_status"]) for r in _e_added]
      == [("LE003", "OFFERED")],
      f"{_e_added!r}")

# THE ONE THAT MATTERS. A fetch that fails and writes nothing must not read as
# "Workday withdrew everything" and retire the entire reference set.
check("an EMPTY current snapshot raises rather than withdrawing everything",
      _raises(lambda: _wdr.effectivity_rows(_e_prev, {}, "2026-09-28T00:00:00")),
      "a failed fetch produces an empty file; diffing it naively would close every "
      "window Workday has ever offered, and the load would look successful")

check("a type MISSING from the current snapshot is left alone, not withdrawn",
      _wdr.effectivity_rows(_e_prev, {"Company_Reference_ID": {"LE001", "LE002"}},
                            "2026-09-28T00:00:00") == [],
      "types are fetched one call at a time, so a snapshot that covers only some types "
      "says nothing about the others -- absence of a TYPE is not absence of its VALUES")
```

- [ ] **Step 2: Run to verify they fail**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL|Traceback" | head
```
Expected: the suite aborts with `AttributeError: module 'accelerator.wd_reference' has no attribute 'effectivity_rows'`. That abort is itself the signal the function does not exist yet.

- [ ] **Step 3: Implement**

Append to `src/accelerator/wd_reference.py`:

```python
class EmptySnapshotError(ValueError):
    """A current snapshot with no types at all.

    A failed Get_References call writes an empty file. Diffed naively that is
    indistinguishable from "Workday withdrew every reference value in existence", and
    the resulting load would close every window and report success. Refusing is the
    only safe reading: an empty snapshot is evidence of a broken fetch, never of a
    mass withdrawal.
    """


def effectivity_rows(previous: dict[str, set[str]], current: dict[str, set[str]],
                     snapshot_dts: str) -> list[dict[str, str]]:
    """Rows recording what opened and what closed between two snapshots.

    ONLY TYPES PRESENT IN `current` ARE COMPARED. Types are fetched one call at a time,
    so a snapshot covering some types says nothing about the rest -- treating a missing
    TYPE as the withdrawal of all its VALUES would retire a whole domain because one
    call was not made.
    """
    if not current:
        raise EmptySnapshotError(
            "current snapshot is empty; refusing to read that as a mass withdrawal")
    rows: list[dict[str, str]] = []
    for ref_type in sorted(current):
        was = previous.get(ref_type, set())
        now = current[ref_type]
        for ref_id in sorted(now - was):
            rows.append({"reference_id_type": ref_type, "reference_id": ref_id,
                         "reference_status": "OFFERED", "effective_from": snapshot_dts})
        for ref_id in sorted(was - now):
            rows.append({"reference_id_type": ref_type, "reference_id": ref_id,
                         "reference_status": "WITHDRAWN",
                         "effective_from": snapshot_dts})
    return rows
```

- [ ] **Step 4: Run to verify they pass**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -3
```
Expected: `ALL CHECKS PASSED`.

- [ ] **Step 5: Prove the empty-snapshot guard can fail**

```bash
cp src/accelerator/wd_reference.py /tmp/wdr.bak
.venv/bin/python - <<'PY'
p="src/accelerator/wd_reference.py"; s=open(p).read()
s=s.replace('    if not current:\n        raise EmptySnapshotError(\n            "current snapshot is empty; refusing to read that as a mass withdrawal")\n', '')
open(p,"w").write(s)
PY
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -3
cp /tmp/wdr.bak src/accelerator/wd_reference.py
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
```
Expected: the empty-snapshot check goes red, then green.

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/wd_reference.py tests/test_accelerator.py
git commit -m "Derive reference effectivity by diffing snapshots, and refuse an empty one"
```

---

### Task 5: The `workday_landing` schema and its two tables

**Files:**
- Create: `tools/emit_workday_landing.py`
- Create: `governance/workday_landing.sql` (generated)
- Modify: `verify_repo.py` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `emit_workday_landing.render() -> str` — the DDL text.
- Produces: `02_usnc_silver_edm_dev.workday_landing.wd_reference_snapshot` and `.wd_reference_effectivity`, matching the `bronze_table` bindings written in Task 3.

- [ ] **Step 1: Write the emitter**

`tools/emit_workday_landing.py`:

```python
#!/usr/bin/env python3
"""The INTERIM landing schema for Workday reference values.

INTERIM, AND BUILT TO BE DELETED. Bronze has no workday schema yet (measured 27
September: 01_usnc_bronze_dev has no schema matching workday or wd_). When it does, the
two bindings in metadata/entities/ move to 01_usnc_bronze_dev.workday_raw.* and this
schema is dropped -- one change, because nothing else references it.

NOT raw_vault, AND NOT stg_. stg_ means the SDP pipeline's append log for a staged
entity; DEF-42 rests on a staged kind having exactly two real tables. Landing here would
leave assert_append_only, assert_no_broad_grant and publish_stable_views asserting over
objects that are not entities -- gates quietly checking the wrong thing, which is worse
than gates failing.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "governance" / "workday_landing.sql"

SCHEMA = "workday_landing"
TABLES = {
    "wd_reference_snapshot": [
        ("reference_id_type", "STRING NOT NULL"),
        ("reference_id", "STRING NOT NULL"),
        ("descriptor", "STRING"),
        ("snapshot_dts", "TIMESTAMP NOT NULL"),
    ],
    "wd_reference_effectivity": [
        ("reference_id_type", "STRING NOT NULL"),
        ("reference_id", "STRING NOT NULL"),
        ("reference_status", "STRING NOT NULL"),
        ("effective_from", "TIMESTAMP NOT NULL"),
    ],
}


def render() -> str:
    lines = [
        "-- GENERATED by tools/emit_workday_landing.py. Do not edit.",
        "-- Interim landing for Workday reference values; see the emitter for why this",
        "-- is neither Bronze nor a stg_ table in raw_vault.",
        "",
        f"CREATE SCHEMA IF NOT EXISTS `${{catalog}}`.`{SCHEMA}`;",
        "",
    ]
    for table, cols in TABLES.items():
        body = ",\n".join(f"  `{n}` {t}" for n, t in cols)
        lines.append(
            f"CREATE TABLE IF NOT EXISTS `${{catalog}}`.`{SCHEMA}`.`{table}` (\n"
            f"{body}\n) USING DELTA;")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    OUT_PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)}")
```

- [ ] **Step 2: Generate and inspect**

```bash
.venv/bin/python tools/emit_workday_landing.py
cat governance/workday_landing.sql
```
Expected: one `CREATE SCHEMA IF NOT EXISTS` and two `CREATE TABLE IF NOT EXISTS`, all qualified with `${catalog}`.

- [ ] **Step 3: Write the failing checks in `verify_repo.py`**

```python
# --- Workday interim landing schema (subsystem E, Task 5) ----------------------------
_wl_path = ROOT / "governance" / "workday_landing.sql"
_wl_committed = _wl_path.read_text(encoding="utf-8") if _wl_path.exists() else ""
try:
    import emit_workday_landing as _wl_mod
    _wl_rendered = _wl_mod.render()
except Exception as _wl_exc:  # noqa: BLE001
    _wl_mod, _wl_rendered = None, f"EMITTER RAISED: {_wl_exc!r}"

check("workday_landing.sql is byte-identical to what its emitter renders",
      bool(_wl_committed) and _wl_committed == _wl_rendered,
      "run tools/emit_workday_landing.py and commit the result")

check("every workday_landing statement is CREATE ... IF NOT EXISTS -- never a replace",
      bool(_wl_committed)
      and "CREATE OR REPLACE" not in _wl_committed
      and "DROP " not in _wl_committed
      and _wl_committed.count("IF NOT EXISTS") == 1 + len(_wl_mod.TABLES),
      "DEF-61: replacing a securable discards every grant held against it, and a schema "
      "is a securable too")

check("workday_landing carries no object with a vault table prefix",
      bool(_wl_committed)
      and not any(f"`{p}" in _wl_committed
                  for p in naming.GENERATED_TABLE_PREFIXES),
      f"a landing table named like a vault table would be picked up by the loaders and "
      f"the gates; prefixes are {list(naming.GENERATED_TABLE_PREFIXES)}")

check("workday_landing is qualified with ${catalog}, the SILVER one, never ${gold_catalog}",
      bool(_wl_committed)
      and "${catalog}" in _wl_committed and "${gold_catalog}" not in _wl_committed,
      "landing Workday reference values in gold would put a source table in the "
      "consumer layer")

check("the landing tables are exactly the two the entity bindings name",
      _wl_mod is not None
      and set(_wl_mod.TABLES) == {"wd_reference_snapshot", "wd_reference_effectivity"},
      f"{sorted(_wl_mod.TABLES) if _wl_mod else '?'} -- these names appear in "
      f"metadata/entities/hub_wd_reference.yml and esat_wd_reference.yml; a rename here "
      f"silently unbinds them")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -3
```
Expected: `VERIFICATION PASSED`.

- [ ] **Step 5: Prove two of the new checks can fail**

```bash
cp tools/emit_workday_landing.py /tmp/wl.bak
sed -i 's/"wd_reference_snapshot"/"stg_wd_reference_snapshot"/' tools/emit_workday_landing.py
.venv/bin/python tools/emit_workday_landing.py
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -3
cp /tmp/wl.bak tools/emit_workday_landing.py
.venv/bin/python tools/emit_workday_landing.py
.venv/bin/python verify_repo.py 2>&1 | tail -2
```
Expected: the "exactly the two names" check goes red (and the byte-gate, until regenerated), then green.

- [ ] **Step 6: Commit**

```bash
git add tools/emit_workday_landing.py governance/workday_landing.sql verify_repo.py
git commit -m "Interim workday_landing schema: generated, byte-gated, deliberately not stg_"
```

---

### Task 6: The gold `reference_data` view

**Files:**
- Create: `tools/emit_gold_reference_views.py`
- Create: `governance/gold_reference_views.sql` (generated)
- Modify: `verify_repo.py` (append)

**Interfaces:**
- Consumes: `metadata/workday/reference_exclusions.yml` and `reference_types.json` indirectly — the view is generic and needs neither at runtime.
- Produces: `${gold_catalog}.reference_data.wd_reference`, a view over currently-offered reference values.

**Depends on subsystem A** for the `reference_data` schema to exist. The DDL is generated and gated by this task regardless; applying it requires A to have run.

- [ ] **Step 1: Write the emitter**

`tools/emit_gold_reference_views.py`:

```python
#!/usr/bin/env python3
"""The gold view over currently-offered Workday reference values.

A VIEW, NOT A TABLE. Adrian's framing, 27 September: "propagated in gold reference or
master data (it could be a view)". A view keeps the vault as the single copy, so a
withdrawal recorded in the effectivity satellite is visible in gold immediately and
cannot drift.

ONE GENERIC VIEW, NOT ONE PER TYPE. There are hundreds of candidate types and a mapping
will use a small fraction. Per-type views are added when a DCDD mapping actually
consumes one -- generating hundreds against which nothing is written would be hundreds
of objects to govern, publish and keep in step, for the convenience of a name.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "governance" / "gold_reference_views.sql"


def render() -> str:
    return """-- GENERATED by tools/emit_gold_reference_views.py. Do not edit.
--
-- Currently-offered Workday reference values. A reference appears here while its most
-- recent effectivity row says OFFERED; a WITHDRAWN row removes it without deleting any
-- history, because the satellite is insert-only.
CREATE OR REPLACE VIEW `${gold_catalog}`.`reference_data`.`wd_reference` AS
SELECT
  h.reference_id_type,
  h.reference_id,
  e.reference_status,
  e.effective_from
FROM `${catalog}`.`raw_vault`.`hub_wd_reference` h
JOIN (
  SELECT
    wd_reference_hk,
    reference_status,
    effective_from,
    ROW_NUMBER() OVER (PARTITION BY wd_reference_hk
                       ORDER BY effective_from DESC, load_dts DESC) AS rn
  FROM `${catalog}`.`raw_vault`.`esat_wd_reference_effectivity_workday`
) e
  ON e.wd_reference_hk = h.wd_reference_hk AND e.rn = 1
WHERE e.reference_status = 'OFFERED';
"""


if __name__ == "__main__":
    OUT_PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)}")
```

- [ ] **Step 2: Generate**

```bash
.venv/bin/python tools/emit_gold_reference_views.py
cat governance/gold_reference_views.sql
```

- [ ] **Step 3: Write the failing checks in `verify_repo.py`**

```python
# --- gold reference view (subsystem E, Task 6) ---------------------------------------
_grv_path = ROOT / "governance" / "gold_reference_views.sql"
_grv = _grv_path.read_text(encoding="utf-8") if _grv_path.exists() else ""
try:
    import emit_gold_reference_views as _grv_mod
    _grv_rendered = _grv_mod.render()
except Exception as _grv_exc:  # noqa: BLE001
    _grv_mod, _grv_rendered = None, f"EMITTER RAISED: {_grv_exc!r}"

check("gold_reference_views.sql is byte-identical to what its emitter renders",
      bool(_grv) and _grv == _grv_rendered,
      "run tools/emit_gold_reference_views.py and commit the result")

check("the gold reference view selects only OFFERED references",
      "reference_status = 'OFFERED'" in _grv,
      "without this the view republishes values Workday has withdrawn, which is exactly "
      "what the effectivity satellite exists to prevent")

check("it takes the LATEST effectivity row per reference, not any row",
      "ROW_NUMBER() OVER" in _grv and "rn = 1" in _grv,
      "the satellite is insert-only, so a withdrawn-then-reoffered reference has several "
      "rows; joining them all would return a reference once per state it has ever held")

check("the view is created in the GOLD catalog and reads from the SILVER one",
      "`${gold_catalog}`.`reference_data`" in _grv
      and "`${catalog}`.`raw_vault`" in _grv,
      "gold is the consumer layer and the vault is the source; swapping them publishes "
      "a vault object into gold or a gold object into the vault")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -3
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
```
Expected: both pass.

- [ ] **Step 5: Prove the OFFERED check can fail**

```bash
cp tools/emit_gold_reference_views.py /tmp/grv.bak
sed -i "s/WHERE e.reference_status = 'OFFERED';/;/" tools/emit_gold_reference_views.py
.venv/bin/python tools/emit_gold_reference_views.py
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -2
cp /tmp/grv.bak tools/emit_gold_reference_views.py
.venv/bin/python tools/emit_gold_reference_views.py
.venv/bin/python verify_repo.py 2>&1 | tail -2
```
Expected: the OFFERED check goes red, then green.

- [ ] **Step 6: Final full run and commit**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
.venv/bin/python verify_repo.py 2>&1 | tail -2
git add tools/emit_gold_reference_views.py governance/gold_reference_views.sql verify_repo.py
git commit -m "Gold reference_data view over currently-offered Workday references"
```

---

## What this plan does NOT do, and why

- **It does not transport the file into `workday_landing`.** `tools/fetch_workday_references.py` writes a local file and stops at the ARB boundary. Moving those rows into the landing table needs either the Bronze `workday` schema (a Bronze-team request that has not been made) or a UC Volume (no volume exists). Every piece this plan builds is offline-testable without it, and the transport is one wiring change once that decision lands. Writing it now would mean inventing a path nobody has agreed to.
- **It does not apply any DDL.** `governance/workday_landing.sql` and `governance/gold_reference_views.sql` are generated and gated, exactly as `governance/control_objects_gold.sql` has been since 29 August. Applying them is a job task, and the gold one additionally needs subsystem A's `reference_data` schema.
- **It does not map a single DCDD field.** That is subsystem C.
- **It does not add a descriptive satellite.** Until a mapping needs reference descriptors, adding one would invent a shape for data nobody has read.
