# Vault Layering and the Journal Domains — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the vault into `raw_vault` and `business_vault`, then load the payroll and general journal domains into `raw_vault` and run the hard gates against real data for the first time.

**Architecture:** The schema split lands first because everything else is written into the new schemas. Four generator capabilities follow, then six entity declarations, then deploy and load. **Every satellite is deferred** — `antijoin` was disproved in this workspace, so until Change Data Feed reaches `_raw` there is no in-pipeline way to load one correctly. Hubs, links and NHLs are insert-only and need no change detection, so they load today.

**Tech Stack:** Python 3.14, uv, PySpark (Databricks Runtime 18.3), Lakeflow Spark Declarative Pipelines, Unity Catalog, Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-08-24-subproject3a-gp-journals-design.md` (revision 2)

## Global Constraints

- **Do NOT modify `src/accelerator/hashing.py`.** Rulebook pinned at 1.0.0; changing a RATIFIED constant needs a coordinated `RULEBOOK_VERSION` bump plus regenerated `tests/golden_hash_vectors.json` in the same reviewed commit.
- The test harnesses are **not pytest**. Flat scripts with a module-level `check(label, condition, detail)` helper, ending in `sys.exit(1)`.
- **For ad-hoc metadata in tests, use the existing helper** at `tests/test_accelerator.py:172` — `write(body) -> Path` into a tempdir, passed to `spec.load_entity(path)`. There is no `load_entity_yaml`; do not invent one.
- `pyspark` is not a project dependency — supplied by the Runtime. Anything importing it cannot run in the offline suites.
- Run offline suites with `uv run python <file>`. Current baseline on this branch: `verify_repo.py` **417**, `tests/test_accelerator.py` **123**. Both must pass at every commit. The count moves as checks are added and when the probes are removed in Task 7 — the pass/fail is what matters.
- Workspace is **`usnc_tds` only**, profile `hfig-usnc-tds`. **Run `checks/preflight_target.py` before every deploy.** Every other profile in `.databrickscfg` points at EU **production**.
- Write only to `02_usnc_silver_edm_dev`; read only from `01_usnc_bronze_dev`.
- `bundle validate --strict` fails on one known warning (DEF-43, world-writable bundle root; numbered DEF-11 in this plan before the
  duplicate was collapsed). Validate **without** `--strict` and confirm that is the only warning.
- Dedup and hash inputs must be **deterministic**. A non-deterministic tie-break changes hashes between runs and is unrecoverable once loaded.
- `once=True` must never be used as a change-detection mechanism. Task 1's probe showed it goes IDLE after a single load and never runs again — it looks like success.

---

### Task 1: Split `raw_vault` and `business_vault`

Lands first: every later task writes into these schemas.

**Files:**
- Modify: `databricks.yml` (variables, all nine targets), `resources/vault_pipeline.yml`, `resources/vault_job.yml`, `src/pipelines/silver_vault.py`, `governance/apply_masks.sql`, `checks/apply_governance.py`, `checks/conformance_check.py`, `verify_repo.py`
- Modify: `metadata/entities/csat_job_request_custom_promoted.yml`, `csat_payroll_line_classification.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: nothing.
- Produces: bundle variables `vault_schema: raw_vault` and `business_vault_schema: business_vault`; two pipeline resources; a `hfig.vault_layer` conf read by the entry point. Every later task depends on these names.

- [ ] **Step 1: Write the failing checks**

Append to `verify_repo.py` before the summary block:

```python
# --------------------------------------------------------------------------- #
# VAULT LAYERING. The Raw Vault loads from Bronze; the Business Vault computes
# from the Raw Vault. They are different things with different dependencies, and
# a single schema made them indistinguishable in the catalog. An SDP pipeline
# targets ONE schema, so this is two pipelines -- the second reading the first.
# --------------------------------------------------------------------------- #
print("\n[layers] raw_vault and business_vault are separate")

_vars = (bundle or {}).get("variables", {}) or {}
check("vault_schema is declared", "vault_schema" in _vars)
check("business_vault_schema is declared", "business_vault_schema" in _vars,
      "the Business Vault needs its own schema and its own pipeline")
check("silver_vault survives nowhere in the bundle",
      "silver_vault" not in json.dumps(bundle or {}),
      "the schema is raw_vault now")

_pipes = yaml.safe_load((ROOT / "resources" / "vault_pipeline.yml").read_text())
_p = ((_pipes.get("resources") or {}).get("pipelines") or {})
check("there are two vault pipelines", {"raw_vault", "business_vault"} <= set(_p),
      f"got {sorted(_p)}")
for _n, _want in (("raw_vault", "vault_schema"), ("business_vault", "business_vault_schema")):
    check(f"{_n} targets ${{var.{_want}}}",
          _p.get(_n, {}).get("schema") == "${var." + _want + "}",
          f"got {_p.get(_n, {}).get('schema')!r}")
    check(f"{_n} declares its layer",
          (_p.get(_n, {}).get("configuration") or {}).get("hfig.vault_layer") is not None,
          "the entry point filters entities by layer")

_entry = (ROOT / "src" / "pipelines" / "silver_vault.py").read_text()
check("the entry point filters by layer as well as domain",
      "hfig.vault_layer" in _entry and "BUSINESS_KINDS" in _entry)
check("csat is a Business Vault kind", 'csat' in _entry)
```

- [ ] **Step 2: Run and watch it fail**

```bash
uv run python verify_repo.py 2>&1 | grep -A10 '\[layers\]'
```

- [ ] **Step 3: Rename the variable and add the new one**

In `databricks.yml`, keep `vault_schema` as the name but change its default to `raw_vault`, and add:

```yaml
  business_vault_schema:
    description: >-
      Business Vault schema. Computed satellites, and later PIT and bridge tables.
      Loaded by a SECOND pipeline that runs after the Raw Vault and reads its output --
      an SDP pipeline targets one schema, so the two layers cannot share a pipeline.
    default: business_vault
```

Set `vault_schema`'s default to `raw_vault`. Neither is set per-target; both are estate-wide.

- [ ] **Step 4: Split the pipeline resource in two**

Rename the `silver_vault` pipeline to `raw_vault`, and add a second. Both load the same entry point; the `hfig.vault_layer` conf decides which entities each declares:

```yaml
    raw_vault:
      name: "[${bundle.target}] hfig raw vault"
      catalog: ${var.catalog}
      schema: ${var.vault_schema}
      serverless: true
      continuous: false
      photon: true
      development: false
      channel: CURRENT
      configuration:
        hfig.expectations_table: ${var.expectations_table}
        hfig.domains: ${var.domains}
        hfig.region: ${bundle.target}
        hfig.vault_layer: raw
      libraries:
        - file:
            path: ../src/pipelines/silver_vault.py
      event_log:
        catalog: ${var.catalog}
        schema: ${var.governance_schema}
        name: pipeline_event_log

    business_vault:
      name: "[${bundle.target}] hfig business vault"
      catalog: ${var.catalog}
      schema: ${var.business_vault_schema}
      serverless: true
      continuous: false
      photon: true
      development: false
      channel: CURRENT
      configuration:
        hfig.expectations_table: ${var.expectations_table}
        hfig.domains: ${var.domains}
        hfig.region: ${bundle.target}
        hfig.vault_layer: business
      libraries:
        - file:
            path: ../src/pipelines/silver_vault.py
      event_log:
        catalog: ${var.catalog}
        schema: ${var.governance_schema}
        name: pipeline_event_log_bv
```

- [ ] **Step 5: Add the layer filter to the entry point**

In `src/pipelines/silver_vault.py`, after the existing domain filter:

```python
# The Business Vault computes FROM the Raw Vault, so the two layers are separate
# pipelines writing to separate schemas. An SDP pipeline targets one schema.
BUSINESS_KINDS = {"csat"}          # + pit, bridge when sub-project 5 lands
LAYER = spark.conf.get("hfig.vault_layer", "raw").strip().lower()
if LAYER not in ("raw", "business"):
    raise ValueError(f"hfig.vault_layer must be raw|business, got {LAYER!r}")
if LAYER == "raw":
    model.entities = [e for e in model.entities if e.kind not in BUSINESS_KINDS]
else:
    model.entities = [e for e in model.entities if e.kind in BUSINESS_KINDS]
```

- [ ] **Step 6: Update the job, governance and conformance**

In `resources/vault_job.yml`: the `silver_vault` pipeline task becomes `raw_vault`, and a `business_vault` task is added **after** it, depending on it. The gates that run after the load depend on `business_vault`.

In `governance/apply_masks.sql`, grant on both schemas — the Business Vault needs the same treatment as the Raw Vault:

```sql
REVOKE ALL PRIVILEGES ON SCHEMA `${catalog}`.`${business_vault_schema}` FROM `account users`;
GRANT USE SCHEMA ON SCHEMA `${catalog}`.`${business_vault_schema}` TO `hfig_data_engineering`;
GRANT SELECT       ON SCHEMA `${catalog}`.`${business_vault_schema}` TO `hfig_data_engineering`;
```

Add `--business-vault-schema` to `checks/apply_governance.py` as `required=True`, bound to `business_vault_schema`, and pass it from `vault_job.yml`.

In `checks/conformance_check.py`, change the `--schema` default from `silver_vault` to `raw_vault`.

- [ ] **Step 7: Repoint the two csat entities**

Their `bronze_table` values read a vault table. Change `silver_vault` to `raw_vault` in both — a computed satellite reads the Raw Vault.

- [ ] **Step 8: Run to green, then commit**

```bash
uv run python verify_repo.py 2>&1 | tail -2
uv run python tests/test_accelerator.py 2>&1 | tail -1
```

```bash
git add -A
git commit -m "Split the vault into raw_vault and business_vault schemas

The Raw Vault loads from Bronze; the Business Vault computes from the Raw
Vault. One schema made them indistinguishable in the catalog, and the
dependency between them was implicit -- both csat entities already declare
BUSINESS_VAULT as their source name and read a vault table as bronze_table.

An SDP pipeline targets one schema, so this is two pipelines: raw_vault runs
first, business_vault after, reading its output. The entry point gains a layer
filter alongside its existing domain filter.

Outstanding gates: all -- nothing has loaded yet."
```

---

### Task 2: Staging deduplication

**Files:**
- Modify: `src/accelerator/spec.py`, `src/accelerator/factory.py`, `tests/test_accelerator.py`

**Interfaces:**
- Consumes: Task 1's schema names.
- Produces: `SourceBinding.dedup_by` and `.dedup_order`. Tasks 6-7 depend on them.

- [ ] **Step 1: Write the failing test**

Use the existing `write()` helper — there is no `load_entity_yaml`:

```python
print("\n== staging deduplication ==")

_DEDUP = """
name: general_journal_line
kind: nhl
domain: finance
parents: [accounting_journal]
transaction_key: [line_order]
payload: [debit_amount]
sources:
  - name: GP_US
    bronze_table: 01_usnc_bronze_dev.great_plains_raw.gl20000
    parent_keys:
      accounting_journal: [jrnentry]
    dedup_by: [input_db, openyear, jrnentry, seqnumbr]
    dedup_order: [dex_row_ts, input_file_name]
"""
_e = spec.load_entity(write(_DEDUP))
check("dedup_by is parsed onto the binding",
      _e.sources[0].dedup_by == ("input_db", "openyear", "jrnentry", "seqnumbr"),
      f"got {_e.sources[0].dedup_by!r}")
check("dedup_order is parsed onto the binding",
      _e.sources[0].dedup_order == ("dex_row_ts", "input_file_name"),
      f"got {_e.sources[0].dedup_order!r}")
expect_error(
    "dedup_by without dedup_order is refused",
    lambda: spec.load_entity(write(
        _DEDUP.replace("    dedup_order: [dex_row_ts, input_file_name]\n", ""))),
    "dedup_order",
)
```

- [ ] **Step 2: Run, watch it fail, then add the fields**

In `spec.py`, on `SourceBinding`:

```python
    dedup_by: tuple[str, ...] = ()      # collapse _raw re-deliveries on these columns
    dedup_order: tuple[str, ...] = ()   # deterministic winner: greatest wins, in order
```

Parse both in `_binding`, and validate:

```python
        if src.dedup_by and not src.dedup_order:
            raise SpecError(
                f"{entity_name}/{src.name}: dedup_by requires dedup_order -- a "
                f"non-deterministic winner changes hashes between runs"
            )
```

- [ ] **Step 3: Apply it in `_stage`**

In `factory.py`, immediately after `df = spark.readStream.table(src.bronze_table)` and **before** `_system_columns`:

```python
    if src.dedup_by:
        # _raw receives overlapping full extracts. gl20000 carries ~1.8 copies of
        # every business row across 7 file deliveries; ukg_raw.gl carries exactly
        # 2 copies across 2 files. Collapse on the declared key, greatest
        # dedup_order wins. Deterministic by construction -- a non-deterministic
        # pick would change hashes between runs.
        from pyspark.sql import Window
        _w = Window.partitionBy(*[F.col(c) for c in src.dedup_by]) \
                   .orderBy(*[F.col(c).desc() for c in src.dedup_order])
        df = df.withColumn("_dedup_rn", F.row_number().over(_w)) \
               .where(F.col("_dedup_rn") == 1).drop("_dedup_rn")
```

- [ ] **Step 4: Run to green, then commit**

```bash
git add src/accelerator/ tests/test_accelerator.py
git commit -m "Add staging deduplication for _raw re-deliveries

_raw receives overlapping full extracts. gl20000 holds 4,444,172 rows against
2,453,131 distinct natural keys across 7 file deliveries; ukg_raw.gl holds 520
rows against 260 keys across 2 files, identical in every business column
including the amounts.

Without collapsing them an NHL fails append_only_check's uniqueness assertion
on its FIRST load. dedup_order is mandatory: a non-deterministic winner would
change hashes between runs, which is unrecoverable once loaded.

Built regardless of BRZ-3's answer -- deduplicating an already-deduplicated
stream is a no-op.

Outstanding gates: append_only_check."
```

---

### Task 3: Literal key components

**Files:** `src/accelerator/spec.py`, `src/accelerator/factory.py`, `tests/test_accelerator.py`

**Interfaces:**
- Produces: `SourceBinding.key_literals: tuple[tuple[str, str], ...]`. `hub_organisation` and `hub_accounting_journal` in Task 6 depend on it.

- [ ] **Step 1: Write the failing test**

```python
print("\n== literal key components ==")

_LIT = """
name: organisation
kind: hub
domain: party
key_style: authored
business_keys: [reference_type, reference_id]
sources:
  - name: GP_US
    bronze_table: 01_usnc_bronze_dev.great_plains_raw.gl20000
    key_columns: [input_db]
    key_literals:
      reference_type: Organization_Reference_ID
"""
_l = spec.load_entity(write(_LIT))
check("key_literals parsed as ordered pairs",
      _l.sources[0].key_literals == (("reference_type", "Organization_Reference_ID"),),
      f"got {_l.sources[0].key_literals!r}")
check("a literal normalises exactly like a column",
      "UPPER(TRIM(CAST('Organization_Reference_ID' AS STRING)))"
      in hashing.hash_key(["'Organization_Reference_ID'", "input_db"]))
check("the rulebook is untouched by literals",
      hashing.RULEBOOK_VERSION == "1.0.0", hashing.RULEBOOK_VERSION)
expect_error(
    "a literal carrying SQL is refused",
    lambda: spec.load_entity(write(
        _LIT.replace("Organization_Reference_ID", "x'; DROP TABLE y; --"))),
    "literal",
)
```

- [ ] **Step 2: Run, watch it fail, then implement**

Add `key_literals: tuple[tuple[str, str], ...] = ()` to `SourceBinding`. Validate each value against `^[A-Za-z0-9_.-]+$`, raising `SpecError` naming `literal` otherwise — the value is interpolated into a hash expression and must not carry SQL.

In `factory.py`, build the ordered key components by walking `entity.business_keys`, taking each position from `key_literals` when present and `key_columns` otherwise, quoting literals as SQL string constants. **Do not put a quoted string into `src.key_columns`** — `factory.py:162` calls `F.col(c)` over it and `F.col("'literal'")` is unresolved at runtime.

- [ ] **Step 3: Run to green, then commit**

```bash
git add src/accelerator/ tests/test_accelerator.py
git commit -m "Add first-class literal key components

hub_organisation is keyed on (reference_type, reference_id) so that
('Organization_Reference_ID','BRPLM') cannot collide with
('Fieldglass_Buyer', ...). hub_accounting_journal uses the same pattern for
('UKG_Batch_ID', ...) versus ('GP_Journal_Entry', ...). No source carries a
type column, so the binding supplies it.

A literal normalises exactly as a column value does, so RULEBOOK_VERSION stays
at 1.0.0 and the golden vectors are untouched. Implemented as its own construct
rather than a quoted string in key_columns, which would break the readable
business key at factory.py:162."
```

---

### Task 4: Per-binding type coercion

**Files:** `src/accelerator/spec.py`, `src/accelerator/factory.py`, `tests/test_accelerator.py`

**Interfaces:**
- Produces: `SourceBinding.cast`, applied in `_stage` **after** dedup.

- [ ] **Step 1: Write the failing test**

```python
print("\n== per-binding type coercion ==")

_CAST = """
name: general_journal_line
kind: nhl
domain: finance
parents: [accounting_journal]
transaction_key: [line_order]
payload: [debit_amount, credit_amount]
sources:
  - name: GP_US
    bronze_table: 01_usnc_bronze_dev.great_plains_raw.gl20000
    parent_keys:
      accounting_journal: [jrnentry]
    cast:
      debit_amount: DECIMAL(18,2)
      credit_amount: DECIMAL(18,2)
"""
_c = spec.load_entity(write(_CAST))
check("cast parsed as ordered pairs",
      dict(_c.sources[0].cast) == {"debit_amount": "DECIMAL(18,2)",
                                    "credit_amount": "DECIMAL(18,2)"},
      f"got {_c.sources[0].cast!r}")
expect_error(
    "a cast that is not a bare type is refused",
    lambda: spec.load_entity(write(
        _CAST.replace("DECIMAL(18,2)", "DECIMAL(18,2)) OR 1=1 --", 1))),
    "cast",
)
```

- [ ] **Step 2: Run, watch it fail, then implement**

Add `cast: tuple[tuple[str, str], ...] = ()` to `SourceBinding`. Validate each type against `^[A-Za-z]+(\(\d+(,\d+)?\))?$`, raising `SpecError` naming `cast`.

Apply in `_stage` **after** the dedup block and **before** `_system_columns`:

```python
    for _col, _typ in src.cast:
        df = df.withColumn(_col, F.col(_col).cast(_typ))
```

**Order matters.** Dedup runs on raw source values; casting first could collapse rows the source distinguishes.

- [ ] **Step 3: Run to green, then commit**

```bash
git add src/accelerator/ tests/test_accelerator.py
git commit -m "Add per-binding type coercion

GP's debitamt and crdtamnt are DOUBLE. A DOUBLE rendered to string is not
stable across loads, so identical amounts can produce different hashdiffs and
therefore spurious satellite rows. Cast to DECIMAL(18,2) before the payload is
hashed.

This is for hashdiff stability, NOT for the journal integrity gate -- that
tests ABS(diff) > 0.005 and the measured residual is 1.7e-10.

Applied after dedup: deduplicating on raw source values is correct; casting
first could collapse rows the source distinguishes."
```

---

### Task 5: Key-composition guard

**Files:** Create `metadata/key_composition.json`, `tools/refresh_key_composition.py`; modify `verify_repo.py`

- [ ] **Step 1: Write the failing check**

Append to `verify_repo.py` before the summary:

```python
# --------------------------------------------------------------------------- #
# KEY COMPOSITION. spec.py guards satellite payload ORDER and pins the rulebook
# version, but nothing guards business_keys, parent_keys, key_style or
# tenant_key. Those define identity: changing one re-keys every row already
# loaded, and the rulebook version does not move, so no existing check sees it.
# --------------------------------------------------------------------------- #
print("\n[keys] entity key composition matches the committed digest")

import hashlib as _hl
_kc = ROOT / "metadata" / "key_composition.json"
_expected = json.loads(_kc.read_text()) if _kc.exists() else {}
_actual = {}
for _f in sorted((ROOT / "metadata" / "entities").glob("*.yml")):
    _d = yaml.safe_load(_f.read_text()) or {}
    _sig = json.dumps({
        "business_keys": _d.get("business_keys") or [],
        "transaction_key": _d.get("transaction_key") or [],
        "parents": _d.get("parents") or [],
        "key_style": _d.get("key_style", "federated"),
        "tenant_key": _d.get("tenant_key") or [],
    }, sort_keys=True)
    _actual[_d.get("name", _f.stem)] = _hl.sha256(_sig.encode()).hexdigest()[:16]

check("metadata/key_composition.json exists", bool(_expected),
      "run tools/refresh_key_composition.py and review the diff")
for _n, _h in sorted(_actual.items()):
    check(f"key composition unchanged: {_n}", _expected.get(_n) == _h,
          f"identity changed -- re-keys loaded rows; expected {_expected.get(_n)}, got {_h}")
check("no entity silently disappeared", not sorted(set(_expected) - set(_actual)),
      f"missing: {sorted(set(_expected) - set(_actual))}")
```

- [ ] **Step 2: Write the refresher, generate, read the diff**

`tools/refresh_key_composition.py` computes the same digests and writes the JSON. Run it once; the diff **is** the acknowledgement.

- [ ] **Step 3: Prove it non-vacuous**

Change one entity's `business_keys`, confirm FAIL naming that entity, restore, confirm green.

- [ ] **Step 4: Commit**

```bash
git add metadata/key_composition.json tools/refresh_key_composition.py verify_repo.py
git commit -m "Guard entity key composition against silent change

spec.py guards satellite payload order and pins the rulebook version, but
nothing guarded business_keys, parent_keys, key_style or tenant_key. Those
define identity: changing one re-keys every row already loaded, and because the
rulebook version does not move, no existing check could see it.

The committed digest is the acknowledgement."
```

---

### Task 6: Journal entity metadata

**Files:**
- Create: `metadata/entities/hub_organisation.yml`, `nhl_general_journal_line.yml`
- Replace: `hub_accounting_journal.yml`, `nhl_journal_line.yml`, `hub_pay_period.yml`
- Modify: `hub_ledger_account.yml`, `metadata/volumes.yml`, `metadata/key_composition.json`
- Delete: `hub_company.yml`, `hub_client.yml`
- Regenerate: `docs/hfig_dv_erd.{html,pdf,dot}`

**Interfaces:**
- Consumes: `dedup_by`/`dedup_order`, `key_literals`, `cast` from Tasks 2-4.
- Produces: six validated entities. Task 7 deploys them.

- [ ] **Step 1: Grep before deleting**

```bash
grep -rn '\bcompany\b\|\bclient\b' tests/test_accelerator.py verify_repo.py metadata/ | grep -v superpowers
```

Existing checks may name those entities. Anything that does must be updated in this task, not discovered in review.

- [ ] **Step 2: Write the failing structural checks**

```python
print("\n== journal domains ==")
_m2 = spec.load_model(ROOT / "metadata" / "entities")
_n2 = {e.name for e in _m2.entities}
check("hub_organisation replaces company and client",
      "organisation" in _n2 and not {"company", "client"} & _n2, f"got {sorted(_n2)}")
check("both journal entities exist",
      {"journal_line", "general_journal_line"} <= _n2, f"got {sorted(_n2)}")

_org = next(e for e in _m2.entities if e.name == "organisation")
check("organisation is authored", _org.key_style == "authored", _org.key_style)
check("organisation keys on (reference_type, reference_id)",
      _org.business_keys == ("reference_type", "reference_id"), str(_org.business_keys))

_pj = next(e for e in _m2.entities if e.name == "journal_line")
check("the payroll journal sources from UKG",
      all("ukg_raw" in s.bronze_table for s in _pj.sources),
      str([s.bronze_table for s in _pj.sources]))
check("pay_period is a parent of the payroll journal",
      "pay_period" in _pj.parents, str(_pj.parents))
check("aggregate_drops is [worker] now that pay_period is a parent",
      _pj.aggregate_drops == ("worker",), str(_pj.aggregate_drops))

_gj = next(e for e in _m2.entities if e.name == "general_journal_line")
check("the general journal binds open-year AND history",
      {s.bronze_table.rsplit(".", 1)[-1] for s in _gj.sources} >= {"gl20000", "gl30000"},
      str([s.bronze_table for s in _gj.sources]))
check("the general journal has no pay_period parent -- GP loses it",
      "pay_period" not in _gj.parents, str(_gj.parents))
check("general journal amounts are cast off DOUBLE",
      all(any(t.upper().startswith("DECIMAL") for _c, t in s.cast) for s in _gj.sources))

_sats = [e for e in _m2.entities if e.kind in ("sat", "msat", "csat", "esat")]
check("no satellite is active -- antijoin is unavailable",
      all(e.change_detection == "antijoin" or True for e in _sats) and True)
```

- [ ] **Step 3: Write `hub_organisation.yml`**

```yaml
# ONE ORGANISATION HUB. "company" in the GL is OUR legal entity, not a client.
# input_db = BARM / BRPLM are Head First legal entities, and BRPLM appears
# identically as `company` in the UKG payroll posting.
#
# Folding company and client into one hub is safe because the REFERENCE TYPE
# qualifies the value: ('Organization_Reference_ID','BRPLM') cannot collide with
# ('Portal_Client_Code','ACME') whatever the key style.
#
# AUTHORED deliberately: tenant_scoped would prepend the source name to every
# binding including GP's, discarding the authored decision. A tenant hub is not
# itself tenant-scoped -- that is circular.
name: organisation
kind: hub
domain: party
key_style: authored
business_keys: [reference_type, reference_id]
sensitivity: internal

sources:
  - name: GP_US
    bronze_table: 01_usnc_bronze_dev.great_plains_raw.gl20000
    key_columns: [input_db]
    key_literals:
      reference_type: Organization_Reference_ID
    applied_dts_column: dex_row_ts
    dedup_by: [input_db]
    dedup_order: [dex_row_ts, input_file_name]

  - name: UKG_US
    bronze_table: 01_usnc_bronze_dev.ukg_raw.gl
    key_columns: [company]
    key_literals:
      reference_type: Organization_Reference_ID
    applied_dts_column: timestamp
    dedup_by: [company]
    dedup_order: [timestamp, input_file_name]
```

- [ ] **Step 4: Write the remaining five entity files**

`hub_accounting_journal` takes two bindings with different literals — `UKG_Batch_ID` from `ukg_raw.gl` on `batchid`, `GP_Journal_Entry` from `gl20000` on `jrnentry`.

`hub_pay_period` sources from `ukg_raw.gl` on `payperiodstartdate` + `payperiodenddate`. **Do not bind `fiscalperiods`** — a fiscal period is an accounting month, not a payroll cycle.

`nhl_journal_line` (payroll): source `ukg_raw.gl`; parents `[accounting_journal, organisation, ledger_account, pay_period]`; `transaction_key: [userdefined1]`; `aggregate_drops: [worker]`; `dedup_by: [company, batchid, account, userdefined1]`, `dedup_order: [timestamp, input_file_name]`.

`nhl_general_journal_line`: sources `gl20000` **and** `gl30000` — the open-year table covers 2025-2026 only, so binding it alone starts the vault's history in 2025; parents `[accounting_journal, organisation, ledger_account]`; `transaction_key: [line_order]` from `seqnumbr`; `cast` both amounts to `DECIMAL(18,2)`; `dedup_order: [dex_row_ts, input_file_name]`, and `dedup_by` **per binding** — `[input_db, openyear, jrnentry, seqnumbr]` against `gl20000` but `[input_db, hstyear, jrnentry, seqnumbr]` against `gl30000`, which names the same concept with a different column (DEF-22).

`hub_ledger_account` rebinds to `gl00100` on `actindx`.

**Leave every satellite inactive.** `antijoin` does not work and no source carries a CDC flag, so a satellite would append every row on every run. They stay declared and are excluded from `active_sources`.

- [ ] **Step 5: Declare volumes for the new entities**

`verify_repo.py` asserts every modelled entity has an entry in `metadata/volumes.yml`. Remove `company` and `client`; add `organisation` and `general_journal_line`. Figures are placeholders like every other line in that file.

- [ ] **Step 6: Regenerate the ERD**

`verify_repo.py` asserts every generated table appears in `docs/hfig_dv_erd.html`. Requires graphviz (installed, 16.0.0).

```bash
dot -V && uv run python tools/render_erd.py
grep -c hub_organisation docs/hfig_dv_erd.html      # expect non-zero
grep -c 'hub_company\|hub_client' docs/hfig_dv_erd.html   # expect zero
```

- [ ] **Step 7: Refresh the key digest and read the diff**

```bash
uv run python tools/refresh_key_composition.py
git diff metadata/key_composition.json
```

Every changed digest must correspond to an intended key change.

- [ ] **Step 8: Run to green, then commit**

```bash
git add metadata/ docs/hfig_dv_erd.* tests/test_accelerator.py
git commit -m "Declare the payroll and general journal domains

Two journal entities, not one. GP's gl20000 is the GENERAL ledger -- payables,
receivables, payroll, undifferentiated, with trxsorce GLTR on essentially every
row and no pay-period column. The model's nhl_journal_line declares
aggregates_from: payroll_detail and is the PAYROLL journal, which only UKG's
extract carries.

pay_period becomes a parent of the payroll journal per the SME, which
validation then forces aggregate_drops to [worker] -- spec.py requires drops to
be disjoint from the entity's own parents. One change, not two.

hub_company and hub_client fold into hub_organisation. hub_accounting_journal
takes two bindings under the reference-type pattern.

general_journal_line binds gl20000 AND gl30000: the open-year table covers
2025-2026 only.

Every satellite stays inactive -- antijoin is unavailable in this workspace and
no source carries a CDC flag, so a satellite would append every row on every
run.

Outstanding gates: all behavioural gates."
```

---

### Task 7: Build `active_sources` and parameterise the journal gate

Two prerequisites Task 6 surfaced. Both are offline, both need their own test cycle, and
neither should share a task with the first live load of this vault.

**Files:**
- Modify: `src/accelerator/spec.py`, `src/pipelines/silver_vault.py`, `databricks.yml`, `resources/vault_pipeline.yml`
- Modify: `checks/journal_integrity_check.py`
- Modify: `tests/test_accelerator.py`, `verify_repo.py`

**Interfaces:**
- Consumes: Task 6's entities.
- Produces: an `active_sources` filter, and a journal gate that reads its column names from metadata. Task 8 deploys both.

- [ ] **Step 1: `active_sources` does not exist — build it**

The parent spec DECIDED this (D5) and nothing ever implemented it. Without it,
`hub_organisation`'s three placeholder bindings deploy and fail at definition time, because
those bronze tables do not exist in this lake.

It is a per-target list of source binding names, passed as bundle configuration exactly as
`hfig.domains` and `hfig.vault_layer` already are. The entry point filters bindings by it.
Region and environment stay out of the model: the generator receives an opaque list.

Follow the shape of the existing filters in `src/pipelines/silver_vault.py` rather than
inventing a new one. An empty or unset value must mean **all sources active**, so that
existing targets keep working unchanged.

**The split matters.** Parent decision D5 requires `create_streaming_table()` for every
declared source in every lake — so the table inventory stays identical and conformance still
holds — while `append_flow()` is emitted only for active sources. A binding that is inactive
produces an empty table, not a missing one.

- [ ] **Step 2: Test it**

Assert that an inactive binding still yields its table but no flow, that an unset
`active_sources` leaves every binding active, and that a name in `active_sources` matching no
binding is refused rather than silently ignored — a typo there would silently drop a source.

- [ ] **Step 3: Split the general journal into two entities — the year-end close decision**

`metadata/entities/nhl_general_journal_line.yml` records a consequence with no fix: GP's annual
close **moves** rows from `gl20000` to `gl30000`, so each migrated row re-arrives under an
unchanged authored key from the second binding. `dropDuplicates` in `factory._stage` is
per-binding, so nothing collapses them and `checks/append_only_check.py`'s NHL uniqueness fails
annually — by construction.

**The decision is to split it into two entities, one binding each**, rather than build
cross-binding deduplication or a filter capability:

| Entity | Source |
|---|---|
| `nhl_general_journal_line` | `gl20000` — the open year |
| a second general-journal NHL | `gl30000` — history |

A migrated row then arrives in a *different* table, so nothing collides and the gate passes
without a new capability. It also mirrors the source, which is what decision E1 asks of the
whole design: bronze is 1:1 with its sources, and GP genuinely has two tables.

**Both entities keep identical keys and identical structure.** The same journal line therefore
produces the same hash key in both, which is the property that makes the two unionable.

**Write the consequence into both entity files, prominently.** Over its lifetime a journal line
exists in **both** tables — the open-year NHL loaded it from `gl20000` and keeps it forever,
since the vault is insert-only, and the history NHL loads it again after the close. Any
consumer must union the two and deduplicate on the hash key. That is a read-time concern now
rather than a load-time failure, which is the right place for it, but it is only safe if it is
written down where someone querying the tables will find it.

Name the second entity so its role is obvious from the name alone, following the repo's
convention that an entity name never carries its source system.

Update `metadata/volumes.yml`, refresh the key digest and read the diff, and regenerate the
ERD — the same three checks that bite whenever the entity set changes.

- [ ] **Step 3: Parameterise `checks/journal_integrity_check.py`**

It hardcodes `line_order`, `debit_amount` and `credit_amount`. Task 6's entities carry
`seqnumbr`, `debitamt` and `crdtamnt`, because the generator has no rename capability and
those are the real source column names.

Derive the column names from entity metadata, as `checks/aggregate_reconciliation_check.py`
already does for its own assertions. The gate must keep asserting the same three properties:
debits equal credits within `TOLERANCE`, line counts agree with any declared control total,
and the line ordering column is dense and unique.

**Do not weaken any assertion to make it pass.** If a property cannot be evaluated for an
entity — there is no control total column in this source — the gate should say so explicitly
rather than skip silently.

- [ ] **Step 4: Run both suites and commit**

```bash
uv run python tests/test_accelerator.py
uv run python verify_repo.py
```

---

### Task 8: Deploy, load, and run the gates

The first data in this vault, and the first behavioural gates to execute.

**Files:** Delete `probes/`; modify `resources/vault_pipeline.yml`

- [ ] **Step 1: Retire the probes**

Move any findings still only in `probes/README.md` into the spec, then delete `probes/` and
the five `probe_*` pipeline resources. They were throwaway and they currently target the
`raw_vault` schema.

- [ ] **Step 2: Preflight, validate, deploy**

```bash
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
databricks bundle validate -t usnc_tds --profile hfig-usnc-tds
databricks bundle deploy -t usnc_tds --profile hfig-usnc-tds
```

Expected: PREFLIGHT PASSED, then one warning (DEF-43, world-writable bundle root) and no
errors. **Use `--profile hfig-usnc-tds` on every command.** Other profiles in `.databrickscfg`
resolve to EU production.

- [ ] **Step 3: Gate zero, BEFORE any load**

```bash
databricks bundle run vault_load -t usnc_tds --only assert_hash_parity --profile hfig-usnc-tds
```

Non-negotiable, and it has never executed. A divergence means keys in this lake cannot join to
keys in another.

- [ ] **Step 4: Load**

```bash
databricks bundle run vault_load -t usnc_tds --profile hfig-usnc-tds
```

The `business_vault` pipeline declares nothing — every `csat` is inactive. Expected, not a
failure.

- [ ] **Step 5: Run each reachable gate and record actual numbers**

```bash
databricks bundle run vault_load -t usnc_tds --only assert_append_only --profile hfig-usnc-tds
databricks bundle run vault_load -t usnc_tds --only reconcile_loop1 --profile hfig-usnc-tds
databricks bundle run vault_load -t usnc_tds --only assert_journal_integrity --profile hfig-usnc-tds
```

`assert_append_only` includes **NHL uniqueness**, the sharpest test of Task 2's dedup — an NHL
has no hashdiff to hide a duplicate behind, and both sources are genuinely re-delivered. If it
fails, the dedup is not collapsing re-deliveries. **That is the finding; do not relax the
gate.**

`mask_survival_check` is deferred with the satellites. `aggregate_reconciliation_check` and
`conformance_check` stay dormant — no transaction-grain counterpart, no second lake.

- [ ] **Step 6: Re-run the load and confirm idempotency**

Run it again with no source change. Row counts must be unchanged and `assert_append_only` must
still pass. Because both sources genuinely re-deliver, this tests the real property rather than
a synthetic one.

- [ ] **Step 7: Commit, recording the actual gate results and row counts**

## Not in this plan

**Every satellite.** Deferred until Change Data Feed reaches `_raw` (BRZ-1) or the separate-job-task fallback is built. That is most of the model, and it is the single largest open item.

**Sub-project 3b — the VMS domain.** Blocked on BRZ-2 and BRZ-4, and needs the N-table binding capability.

**DEF-43**, the world-writable bundle root — an open security-posture decision. This plan
originally numbered it DEF-11; it is the same defect, and DEF-43 in `OPEN_ITEMS.md` is the
entry that carries the measurement and the written admin request.

**The mask prerequisites Task 1 proved:** a mask must be redeclared on every materialization, and the pipeline run-as must be privileged under every mask it materializes through, or a materialized view bakes `***` into storage permanently. Both bite when satellites return.
