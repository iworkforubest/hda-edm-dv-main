# Satellite Change Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load satellites through a staging log and a batch hashdiff comparison, so that every satellite can leave the dormant state without waiting for Change Data Feed.

**Architecture:** The SDP pipeline appends every delivered row to `stg_sat_x`; a batch task reads the target — which a streaming flow cannot — and inserts a row only where its `hashdiff` differs from the **latest** stored version for that key. This is the same shape as `checks/load_hubs.py` (DEF-42), one degree harder: the hub anti-join is set membership, a satellite's is order-sensitive.

**Tech Stack:** Python 3.12, PySpark / Lakeflow Spark Declarative Pipelines, Databricks Asset Bundles, Unity Catalog, Delta.

**Spec:** `docs/superpowers/specs/2026-08-26-satellite-change-detection-design.md`

## Global Constraints

- **Profile is always explicit.** Pass `--profile hfig-usnc-tds`. Every other profile in `.databrickscfg` resolves to EU **production**.
- **Write only to `02_usnc_silver_edm_dev`. Read only from `01_usnc_bronze_dev`.** Bronze belongs to another team.
- **`src/accelerator/hashing.py`, `RULEBOOK_VERSION` and `tests/golden_hash_vectors.json` must not change.** Any change requires a coordinated version bump in the same reviewed commit.
- **Gate zero (`hash_parity_check.py`) runs before any load. No exceptions.**
- **`preflight_target.py --target usnc_tds --profile hfig-usnc-tds` passes before every `databricks bundle deploy`.**
- **Run `databricks bundle run raw_vault --validate-only` before dropping any table.** It resolves the whole graph in ~90 seconds. Skipping it turned a schema change into a four-hour outage on 25 August.
- **Never `--full-refresh-all`.** Vault tables are `delta.appendOnly = true`; a full refresh is a truncate and dies on the first one. Name the tables instead.
- **`DROP TABLE` does not cascade to `__materialization_*`.** Drop the twin by name or cleartext is stranded with no masked object above it.
- **A check that cannot fail is a defect.** Every new assertion must be verified to fail when its rule is removed, and that verification stated in the commit.
- **No retention on `stg_` tables** (decided 26 Aug: 13% overhead does not justify three moving parts).
- The offline suite is `uv run python tests/test_accelerator.py`. It must end `ALL CHECKS PASSED`.

---

### Task 1: Prove the SQL runs on this runtime

The spec requires this first, and the reason is specific: `ALTER CATALOG ... SET ISOLATION MODE` sat in `governance/apply_masks.sql` for weeks and turned out to be a **parse error** on this runtime, discovered only when someone finally ran the file. Three constructs in the satellite loader are unverified: `IS DISTINCT FROM`, `LAG` inside an `INSERT ... SELECT`, and `USING` on a nullable `mas_key`.

**Files:**
- Create: `docs/superpowers/evidence/2026-08-26-satellite-sql-probe.md`

**Interfaces:**
- Consumes: nothing.
- Produces: a recorded verdict per construct. Task 3 writes SQL only in the forms proven here.

- [ ] **Step 1: Probe `IS DISTINCT FROM` and `LAG` in a subquery**

```bash
cd /mnt/projects/hda-edm-dv
databricks experimental aitools tools query "
WITH t AS (SELECT * FROM VALUES
    ('k1', 'A', TIMESTAMP'2026-01-01', 0),
    ('k1', 'A', TIMESTAMP'2026-01-02', 0),
    ('k1', 'B', TIMESTAMP'2026-01-03', 0),
    ('k1', 'A', TIMESTAMP'2026-01-04', 0)
  AS v(parent_hk, hashdiff, load_dts, sub_seq)),
w AS (SELECT *, LAG(hashdiff) OVER (PARTITION BY parent_hk ORDER BY load_dts, sub_seq) AS prev FROM t)
SELECT load_dts, hashdiff, prev, (hashdiff IS DISTINCT FROM prev) AS keep FROM w ORDER BY load_dts
" --profile hfig-usnc-tds -o json
```

Expected: four rows; `keep` is `true, false, true, true`. That is the A → A → B → A case collapsing the consecutive duplicate and keeping the return to A.

- [ ] **Step 2: Probe `USING` on a NULLABLE join column**

`mas_key` is NULL for every `sat` (only `msat` populates it). `USING` must not drop those rows.

```bash
databricks experimental aitools tools query "
WITH a AS (SELECT * FROM VALUES ('k1', CAST(NULL AS STRING)) AS v(parent_hk, mas_key)),
     b AS (SELECT * FROM VALUES ('k1', CAST(NULL AS STRING), 'X') AS v(parent_hk, mas_key, hashdiff))
SELECT a.parent_hk, b.hashdiff FROM a LEFT JOIN b USING (parent_hk, mas_key)
" --profile hfig-usnc-tds -o json
```

Expected: **one row with `hashdiff = null`**, because `NULL = NULL` is not true. If so, `USING` on `mas_key` is WRONG and Task 3 must use `a.parent_hk = b.parent_hk AND a.mas_key IS NOT DISTINCT FROM b.mas_key` instead. Record whichever it is.

- [ ] **Step 3: Probe `LAG` inside `INSERT ... SELECT`**

```bash
databricks experimental aitools tools query "CREATE TABLE IF NOT EXISTS 02_usnc_silver_edm_dev.governance._probe_lag (k STRING, h STRING, d TIMESTAMP)" --profile hfig-usnc-tds -o json
databricks experimental aitools tools query "
INSERT INTO 02_usnc_silver_edm_dev.governance._probe_lag
SELECT k, h, d FROM (
  SELECT *, LAG(h) OVER (PARTITION BY k ORDER BY d) AS prev
  FROM VALUES ('k1','A',TIMESTAMP'2026-01-01'), ('k1','A',TIMESTAMP'2026-01-02') AS v(k,h,d)
) WHERE h IS DISTINCT FROM prev
" --profile hfig-usnc-tds -o json
databricks experimental aitools tools query "SELECT count(*) AS n FROM 02_usnc_silver_edm_dev.governance._probe_lag" --profile hfig-usnc-tds -o json
databricks experimental aitools tools query "DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_lag" --profile hfig-usnc-tds -o json
```

Expected: the insert succeeds and `n = 1`.

- [ ] **Step 4: Record the verdicts**

Write `docs/superpowers/evidence/2026-08-26-satellite-sql-probe.md` with one line per construct: the statement run, the exact output, and PASS or the required alternative form. State the runtime (`databricks pipelines get`, `dbr_version` from the `create_update` event) so a future reader knows what was tested.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/evidence/2026-08-26-satellite-sql-probe.md
git commit -m "Probe the satellite loader's SQL before writing it

ALTER CATALOG ... SET ISOLATION MODE sat in apply_masks.sql for weeks and was a
parse error on this runtime, found only when the file was finally run. These
three constructs are verified before the loader depends on them."
```

---

### Task 2: Satellites become a staged kind

**Files:**
- Modify: `src/accelerator/naming.py` — `STAGED_KINDS`
- Modify: `tests/test_accelerator.py` — the DEF-42 block that asserts which kinds are staged

**Interfaces:**
- Consumes: `naming.pipeline_table(kind, table)`, already used by `factory.build`.
- Produces: `naming.STAGED_KINDS == frozenset({"hub", "sat", "msat", "csat"})`. Task 3's `staged_entities()` reads it.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_accelerator.py`, immediately after the existing `== DEF-42:` block:

```python
print("\n== DEF-52: satellites stage too ==")
check("every satellite kind is staged",
      {"sat", "msat", "csat"} <= naming.STAGED_KINDS,
      f"a satellite needs a batch hashdiff compare against its own latest row, and a "
      f"streaming table cannot read itself: {sorted(naming.STAGED_KINDS)}")
check("a satellite's pipeline object is its log",
      all(naming.pipeline_table(e.kind, t) == f"stg_{t}"
          for e in _am.entities if e.kind in ("sat", "msat", "csat")
          for _s, t in e.tables()), "")
check("its quarantine twin keeps the name it had",
      factory._quarantine_table("stg_sat_job_request_details_bullhorn_eu")
      == "qtn_job_request_details_bullhorn_eu",
      "naming the twin off the log would orphan the existing table")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL"`
Expected: FAIL on "every satellite kind is staged".

- [ ] **Step 3: Make it pass**

In `src/accelerator/naming.py`, replace the `STAGED_KINDS` line:

```python
STAGED_KINDS = frozenset({"hub", "sat", "msat", "csat"})
```

Extend the comment above it with:

```
# DEF-52: satellites join for the same reason and a different anti-join. A satellite
# inserts a version only when its hashdiff differs from the LATEST stored one, and
# "latest stored" is the table being written. Without the batch step every satellite
# re-appends every row on every run -- and no gate objects, because a re-delivery
# arrives with a fresh load_dts and is unique at the grain append_only asserts.
```

- [ ] **Step 4: Run the whole suite**

Run: `uv run python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`. Existing DEF-42 checks that assert `len(staged_entities) == 6` will now see more; update them to compare against the model rather than a literal.

- [ ] **Step 5: Verify the guard still discriminates**

```bash
sed -i 's/^STAGED_KINDS = frozenset({"hub", "sat", "msat", "csat"})$/STAGED_KINDS = frozenset({"hub"})/' src/accelerator/naming.py
uv run python tests/test_accelerator.py 2>&1 | grep -cE "^  FAIL"
git checkout src/accelerator/naming.py
```

Expected: at least 1 FAIL, then a clean run after the checkout.

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/naming.py tests/test_accelerator.py
git commit -m "DEF-52: satellites are a staged kind

A satellite inserts a version only when its hashdiff differs from the latest
stored one, and a streaming table cannot read itself. Verified the check fails
when sat/msat/csat are removed from STAGED_KINDS."
```

---

### Task 3: The satellite loader

**Files:**
- Create: `checks/load_satellites.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `naming.STAGED_KINDS`, `naming.hk(parent)`, `naming.COL["hashdiff"]`, `naming.COL["mas_key"]`, `naming.COL["load_dts"]`, `naming.COL["sub_seq"]`, `naming.stg(table)`.
- Produces: `create_sql(catalog, schema, sat, log) -> str`, `insert_sql(catalog, schema, sat, log, parent_hk, is_msat) -> str`, `staged_satellites(model) -> list[tuple[Entity, str]]`, `main() -> int`. Task 4 calls the module as a `spark_python_task`.

- [ ] **Step 1: Write the failing tests**

Add at the end of `tests/test_accelerator.py`, before the final `print("\n" + "=" * 62)`:

```python
print("\n== DEF-52: the satellite loader compares to the LATEST version ==")

import importlib.util as _ilu2  # noqa: E402
_ls_spec = _ilu2.spec_from_file_location("ls", ROOT / "checks" / "load_satellites.py")
_ls = _ilu2.module_from_spec(_ls_spec); _ls_spec.loader.exec_module(_ls)

_sat_sql = _ls.insert_sql("c", "raw_vault", "sat_job_request_details_bullhorn_eu",
                          "stg_sat_job_request_details_bullhorn_eu",
                          "job_request_hk", is_msat=False)
check("it seeds the batch from the stored latest version",
      "coalesce" in _sat_sql.lower() and "LAG(" in _sat_sql,
      "without the seed, the first row of every batch looks new")
check("it compares to the latest, NOT to set membership",
      "row_number()" in _sat_sql and "DESC" in _sat_sql,
      "set membership swallows A -> B -> A: _v1 would report B as current for ever")
check("it is idempotent at the grain append_only asserts",
      "NOT EXISTS" in _sat_sql and "sub_seq" in _sat_sql, _sat_sql[:200])
check("a sat does NOT partition by mas_key",
      "mas_key" not in _sat_sql,
      "mas_key is NULL for every sat; joining on it would drop every row")

_msat_sql = _ls.insert_sql("c", "raw_vault", "msat_worker_skills_hr_eu",
                           "stg_msat_worker_skills_hr_eu", "worker_hk", is_msat=True)
check("an msat DOES partition by mas_key",
      "mas_key" in _msat_sql,
      "five of thirteen satellite tables are msat; omitting it collapses a worker's "
      "several skills into one version")

check("the loader covers every satellite the model declares",
      {t for _e, t in _ls.staged_satellites(_am)}
      == {t for e in _am.entities if e.kind in ("sat", "msat", "csat")
          for _s, t in e.tables()},
      str(sorted(t for _e, t in _ls.staged_satellites(_am))[:4]))
check("and there are thirteen of them",
      len(_ls.staged_satellites(_am)) == 13,
      str(len(_ls.staged_satellites(_am))))
```

- [ ] **Step 2: Run and watch it fail**

Run: `uv run python tests/test_accelerator.py 2>&1 | grep -E "FAIL|Error" | head -3`
Expected: a `FileNotFoundError` or `ModuleNotFoundError` for `checks/load_satellites.py`.

- [ ] **Step 3: Write the loader**

Create `checks/load_satellites.py`, modelled on `checks/load_hubs.py`. Use the join form Task 1 proved for `mas_key` — the `USING` shown here is a placeholder ONLY if Task 1 recorded `USING` as correct; otherwise substitute `t.mas_key IS NOT DISTINCT FROM c.mas_key`.

```python
"""
THE SATELLITE LOADER: staging log -> satellite, by hashdiff comparison.

DEF-52. A satellite inserts a new version only when a row's hashdiff differs from the
LATEST stored version for its key, and "latest stored" is the table being written -- which
a streaming flow cannot read. Without the comparison every satellite re-appends every row
on every run, and no gate objects: a re-delivery arrives with a fresh load_dts and is
unique at (parent_hk, load_dts, sub_seq), the grain append_only_check asserts.

COMPARE TO THE LATEST, NOT TO SET MEMBERSHIP. A value moving A -> B -> A stores three rows
under the first rule and two under the second, and under the second _v1's
LEAD(load_dts) window reports B as current for ever. That is silent, permanent data loss
that passes every gate.
"""

from __future__ import annotations

if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import naming, spec  # noqa: E402

GATE = "load_satellites"
SAT_KINDS = ("sat", "msat", "csat")


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def staged_satellites(model) -> list:
    """[(entity, table)] for every satellite table the model declares."""
    out = []
    for e in model.entities:
        if e.kind not in SAT_KINDS:
            continue
        for _src, table in e.tables():
            out.append((e, table))
    return sorted(out, key=lambda p: p[1])


def create_sql(catalog: str, schema: str, sat: str, log: str) -> str:
    """Create the satellite with the LOG's exact shape, or leave an existing one alone."""
    return (
        f"CREATE TABLE IF NOT EXISTS {q(catalog, schema, sat)} "
        f"TBLPROPERTIES ("
        f"'delta.appendOnly' = 'true', "
        f"'hfig.vault_kind' = 'satellite', "
        f"'hfig.loaded_by' = 'checks/load_satellites.py') "
        f"AS SELECT * FROM {q(catalog, schema, log)} WHERE 1=0"
    )


def insert_sql(catalog: str, schema: str, sat: str, log: str,
               parent_hk: str, is_msat: bool) -> str:
    """Insert only rows whose hashdiff differs from the latest stored version."""
    hd = naming.COL["hashdiff"]
    ld = naming.COL["load_dts"]
    ss = naming.COL["sub_seq"]
    mk = naming.COL["mas_key"]
    part = f"s.`{parent_hk}`" + (f", s.`{mk}`" if is_msat else "")
    cpart = f"`{parent_hk}`" + (f", `{mk}`" if is_msat else "")
    join = f"c.`{parent_hk}` = t.`{parent_hk}`" + (
        f" AND c.`{mk}` IS NOT DISTINCT FROM t.`{mk}`" if is_msat else "")
    return (
        f"INSERT INTO {q(catalog, schema, sat)}\n"
        f"WITH current AS (\n"
        f"  SELECT {cpart}, `{hd}` FROM (\n"
        f"    SELECT {cpart}, `{hd}`, row_number() OVER (\n"
        f"      PARTITION BY {cpart} ORDER BY `{ld}` DESC, `{ss}` DESC) AS rn\n"
        f"    FROM {q(catalog, schema, sat)}) WHERE rn = 1),\n"
        f"incoming AS (\n"
        f"  SELECT s.*, LAG(s.`{hd}`) OVER (\n"
        f"    PARTITION BY {part} ORDER BY s.`{ld}`, s.`{ss}`) AS prev\n"
        f"  FROM {q(catalog, schema, log)} s\n"
        f"  WHERE NOT EXISTS (\n"
        f"    SELECT 1 FROM {q(catalog, schema, sat)} x\n"
        f"    WHERE x.`{parent_hk}` = s.`{parent_hk}`\n"
        f"      AND x.`{ld}` = s.`{ld}` AND x.`{ss}` = s.`{ss}`))\n"
        f"SELECT * EXCEPT (prev) FROM incoming t\n"
        f"LEFT JOIN current c ON {join}\n"
        f"WHERE t.`{hd}` IS DISTINCT FROM coalesce(t.prev, c.`{hd}`)"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="raw_vault")
    ap.add_argument("--business-vault-schema", default="business_vault")
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--active-sources", default="")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    meta = Path(args.metadata) if args.metadata else (
        Path(__file__).resolve().parents[1] / "metadata" / "entities")
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources)
    targets = staged_satellites(model)

    if not targets:
        print("GATE NOT EVALUATED: the model declares no satellite. That is not a "
              "dormant lake, it is a model with no descriptive history at all.")
        return finish("NOT_EVALUATED", 0, 0, 1)

    loaded, skipped, failed = 0, [], []
    spark = None
    if not args.dry_run:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

    for entity, sat in targets:
        src = next((s for s, t in entity.tables() if t == sat), None)
        if not spec.active_table_bindings(entity, src, active):
            skipped.append(f"{sat}: no active source binding in this lake")
            continue
        schema = naming.vault_schema_for(entity.kind, args.schema,
                                         args.business_vault_schema)
        log = naming.stg(sat)
        parent_hk = naming.hk(entity.parents[0])
        stmts = [create_sql(args.catalog, schema, sat, log),
                 insert_sql(args.catalog, schema, sat, log, parent_hk,
                            entity.kind == "msat")]
        if args.dry_run:
            print(f"\n-- {schema}.{sat} <- {log}")
            for s_ in stmts:
                print(s_ + ";")
            loaded += 1
            continue
        try:
            for s_ in stmts:
                spark.sql(s_)
            n = spark.sql(f"SELECT count(*) AS n FROM {q(args.catalog, schema, sat)}"
                          ).collect()[0]["n"]
            m = spark.sql(f"SELECT count(*) AS n FROM {q(args.catalog, schema, log)}"
                          ).collect()[0]["n"]
            print(f"  ok   {sat:44} log={m:>9}  sat={n:>9}")
            loaded += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {sat}: {exc}")
            failed.append(sat)

    for s_ in skipped:
        print(f"  ~ {s_}")
    if args.dry_run:
        print(f"\ndry run only -- {loaded} satellite(s), nothing executed")
        return 0
    if failed:
        print(f"\nSATELLITE LOAD FAILED -- {len(failed)}: {failed}")
        return finish("FAILED", loaded, len(skipped), 1)
    if not loaded:
        print("\nGATE NOT EVALUATED: every satellite is inactive in this lake.")
        return finish("NOT_EVALUATED", 0, len(skipped), 0)
    print(f"\nSATELLITE LOAD PASSED: {loaded} satellite(s)")
    return finish("PASSED", loaded, len(skipped), 0)


if __name__ == "__main__":
    _rc = main()
    if _rc:
        sys.exit(_rc)
```

- [ ] **Step 4: Run the tests**

Run: `uv run python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`.

- [ ] **Step 5: Render the SQL and read it**

Run:
```bash
uv run python checks/load_satellites.py --dry-run --catalog 02_usnc_silver_edm_dev \
  --active-sources "GP_US,GP_US_HIST" 2>&1 | head -40
```
Expected: `CREATE TABLE IF NOT EXISTS` and an `INSERT` per satellite. Read the `msat` one and confirm `mas_key` appears in both the `PARTITION BY` and the join; read a `sat` one and confirm it does not appear at all.

- [ ] **Step 6: Verify the guards discriminate**

```bash
cp checks/load_satellites.py /tmp/ls.bak
python3 - <<'EOF'
import pathlib
p = pathlib.Path("checks/load_satellites.py"); s = p.read_text()
s = s.replace("coalesce(t.prev, c.`{hd}`)", "c.`{hd}`")   # drop the batch seed
p.write_text(s)
EOF
uv run python tests/test_accelerator.py 2>&1 | grep -cE "^  FAIL"
cp /tmp/ls.bak checks/load_satellites.py
uv run python tests/test_accelerator.py 2>&1 | tail -2
```
Expected: at least 1 FAIL, then `ALL CHECKS PASSED`. If the reverted file has a syntax error the suite CRASHES and prints no FAIL lines — that is not a pass; check the file parses first with `uv run python -c "import ast;ast.parse(open('checks/load_satellites.py').read())"`.

- [ ] **Step 7: Commit**

```bash
git add checks/load_satellites.py tests/test_accelerator.py
git commit -m "DEF-52: the satellite loader, comparing to the latest version

A -> B -> A stores three rows under compare-to-latest and two under set
membership, and under the second _v1 reports B as current for ever. LAG
collapses consecutive duplicates within a batch, coalesce seeds the first row
from what is stored, and NOT EXISTS at (parent_hk, load_dts, sub_seq) makes it
idempotent at the grain append_only already asserts.

Verified the checks fail when the batch seed is removed."
```

---

### Task 4: Wire the loader into the job

**Files:**
- Modify: `resources/vault_job.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `checks/load_satellites.py` from Task 3.
- Produces: a `load_satellites` task depending on `load_hubs`; `business_vault` depends on `load_satellites`.

- [ ] **Step 1: Write the failing test**

```python
check("load_satellites runs after load_hubs and before business_vault",
      any(t["task_key"] == "load_satellites"
          and [d["task_key"] for d in t.get("depends_on", [])] == ["load_hubs"]
          for t in _tasks_list)
      and any(t["task_key"] == "business_vault"
              and "load_satellites" in [d["task_key"] for d in t.get("depends_on", [])]
              for t in _tasks_list),
      "a csat reads the raw vault, so raw satellites must land before the business "
      "vault computes over them")
```

Define `_tasks_list` once near the other job assertions:
```python
_tasks_list = yaml.safe_load(
    (ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8")
)["resources"]["jobs"]["vault_load"]["tasks"]
```

- [ ] **Step 2: Run and watch it fail**

Run: `uv run python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL"`
Expected: FAIL on "load_satellites runs after load_hubs".

- [ ] **Step 3: Add the task**

In `resources/vault_job.yml`, immediately after the `load_hubs` task:

```yaml
        # DEF-52: the satellite loader. The pipeline appends every delivered row to
        # stg_sat_*; this inserts only rows whose hashdiff differs from the LATEST
        # stored version. Without it every satellite re-appends every row on every
        # run -- and no gate objects, because a re-delivery arrives with a fresh
        # load_dts and is unique at the grain append_only asserts.
        - task_key: load_satellites
          depends_on: [{task_key: load_hubs}]
          spark_python_task:
            python_file: ../checks/load_satellites.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--schema"
              - "${var.vault_schema}"
              - "--business-vault-schema"
              - "${var.business_vault_schema}"
              - "--active-sources"
              - "${var.active_sources}"
          environment_key: checks
```

Then change `business_vault`'s `depends_on` from `[{task_key: load_hubs}]` to `[{task_key: load_satellites}]`.

- [ ] **Step 4: Run the suite and validate the bundle**

```bash
uv run python tests/test_accelerator.py 2>&1 | tail -2
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
databricks bundle validate --target usnc_tds --profile hfig-usnc-tds 2>&1 | grep -E "Validation OK|Error"
```
Expected: `ALL CHECKS PASSED`, `PREFLIGHT PASSED`, and validation clean apart from the known `/Shared` bundle-root warning (DEF-43).

- [ ] **Step 5: Commit**

```bash
git add resources/vault_job.yml tests/test_accelerator.py
git commit -m "DEF-52: run the satellite loader between the hubs and the business vault

A csat reads the raw vault, so raw satellites must land before the business
vault computes over them."
```

---

### Task 5: Rebind Bullhorn to a source that exists

`sat_job_request_details_bullhorn_eu` and `hub_job_request`'s `BULLHORN_EU` binding both name `hfig_eu.bronze.bullhorn_job_order`. **That catalog does not exist.** The real table is `01_usnc_bronze_dev.bullhorn_native_raw.joborders`, and all eight declared columns map onto it.

**Files:**
- Modify: `metadata/entities/hub_job_request.yml`
- Modify: `metadata/entities/sat_job_request_details.yml`
- Modify: `metadata/key_composition.json` (regenerated, not hand-edited)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: an activatable `BULLHORN_EU` binding. Task 6 loads it.

- [ ] **Step 1: Confirm the column mapping still holds**

```bash
databricks experimental aitools tools query "
SELECT count(*) AS n,
  count(joborderid) a, count(title) b, count(city) c, count(countrycode) d,
  count(employmenttype) e, count(numopenings) f, count(status) g, count(dateclosed) h
FROM 01_usnc_bronze_dev.bullhorn_native_raw.joborders" --profile hfig-usnc-tds -o json
```
Expected: `n` > 0 and every named column resolves. If any column is missing the mapping has drifted — stop and report rather than guessing a substitute.

- [ ] **Step 2: Rewrite the two bindings**

In both files, for the `BULLHORN_EU` source: set `bronze_table: 01_usnc_bronze_dev.bullhorn_native_raw.joborders`, set `key_columns: [joborderid]`, and map the payload to `[title, city, countrycode, employmenttype, numopenings, status, dateclosed]`.

**Remove `cdc_op_column: _cdc_op` and `manifest_column: _manifest_id`** — neither exists on `joborders`, verified 26 Aug. Leaving them names columns that are absent.

Add `dedup_by: [joborderid]` so a re-delivery within one batch collapses before hashing, matching every other binding in this model.

- [ ] **Step 3: Regenerate the identity digest and READ the diff**

```bash
uv run python tools/refresh_key_composition.py
git diff metadata/key_composition.json
```
Expected: `job_request` and `job_request_details` move. That diff IS the reviewed acknowledgement that a key definition changed — read it, do not just commit it.

- [ ] **Step 4: Run the suite**

Run: `uv run python tests/test_accelerator.py 2>&1 | tail -2`
Expected: `ALL CHECKS PASSED`. The model must still load: `spec.validate` refuses a multi-source link/NHL, and `job_request` is a hub, so three sources remain legal.

- [ ] **Step 5: Commit**

```bash
git add metadata/entities/hub_job_request.yml metadata/entities/sat_job_request_details.yml metadata/key_composition.json
git commit -m "Rebind BULLHORN_EU to a table that exists

hfig_eu.bronze.bullhorn_job_order is in a catalog that does not exist -- a
placeholder from the EU-targeted design. bullhorn_native_raw.joborders is real
and carries all eight declared columns. Dropped cdc_op_column and
manifest_column: neither exists on joborders."
```

---

### Task 6: Activate Bullhorn and prove the loader on real data

**Files:**
- Modify: `databricks.yml` — `usnc_tds`'s `active_sources`
- Create: `docs/superpowers/evidence/2026-08-26-bullhorn-first-satellite.md`

**Interfaces:**
- Consumes: everything above.
- Produces: the first satellite with real payload in this lake.

- [ ] **Step 1: Validate the graph BEFORE changing anything**

```bash
databricks bundle run raw_vault --validate-only -t usnc_tds --profile hfig-usnc-tds 2>&1 | tail -3
```
Expected: `COMPLETED`. This is the standing rule from DEF-39 — ninety seconds that would have prevented a four-hour outage.

- [ ] **Step 2: Add the binding to active_sources**

In `databricks.yml`, `usnc_tds` target, append `,job_request/BULLHORN_EU,job_request_details/BULLHORN_EU` to `active_sources`.

- [ ] **Step 3: Deploy and validate again**

```bash
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
databricks bundle deploy -t usnc_tds --profile hfig-usnc-tds 2>&1 | tail -2
databricks bundle run raw_vault --validate-only -t usnc_tds --profile hfig-usnc-tds 2>&1 | tail -2
```
Expected: `PREFLIGHT PASSED`, deploy clean, `COMPLETED`.

- [ ] **Step 4: Load**

```bash
databricks bundle run vault_load --only +load_satellites -t usnc_tds --profile hfig-usnc-tds 2>&1 | tail -25
```

`+load_satellites` runs its upstream chain: gate zero, the mask functions, `raw_vault`, `load_hubs`. It stops before `business_vault` and therefore before `apply_governance`.

Expected: `ok sat_job_request_details_bullhorn_eu  log=<N>  sat=<M>` with **M < N** — the difference is the unchanged re-deliveries the comparison suppressed.

- [ ] **Step 5: Prove idempotency and append-only**

```bash
databricks bundle run vault_load --only load_satellites -t usnc_tds --profile hfig-usnc-tds 2>&1 | grep "ok "
databricks experimental aitools tools query "
SELECT operation, count(*) n FROM (DESCRIBE HISTORY 02_usnc_silver_edm_dev.raw_vault.sat_job_request_details_bullhorn_eu)
GROUP BY operation" --profile hfig-usnc-tds -o json
```
Expected: the second run leaves `sat=` unchanged, and history shows only `CREATE TABLE AS SELECT` and `WRITE` — no `UPDATE`, `DELETE` or `MERGE`.

- [ ] **Step 6: Run the gates**

```bash
databricks bundle run vault_load --only assert_append_only -t usnc_tds --profile hfig-usnc-tds 2>&1 | tail -4
```
Expected: `APPEND-ONLY GATE PASSED`, with the table count risen by two (the satellite and its log).

- [ ] **Step 7: Confirm `_v1` reports one current row per key**

```bash
databricks experimental aitools tools query "
SELECT count(*) rows, count(DISTINCT job_request_hk) keys
FROM 02_usnc_silver_edm_dev.raw_vault.sat_job_request_details_bullhorn_eu_v1
WHERE is_current" --profile hfig-usnc-tds -o json
```
Expected: `rows = keys`. If they differ the type-2 window is wrong and the change-detection rule must be re-examined before anything else is activated.

- [ ] **Step 8: Record the evidence and commit**

Write `docs/superpowers/evidence/2026-08-26-bullhorn-first-satellite.md` with the log and satellite counts, the second-run delta, the `DESCRIBE HISTORY` operations, and the `_v1` result.

```bash
git add databricks.yml docs/superpowers/evidence/2026-08-26-bullhorn-first-satellite.md
git commit -m "Activate Bullhorn: the vault's first descriptive history

<N> staged rows became <M> satellite versions. Second run inserted zero,
DESCRIBE HISTORY shows only WRITE, and _v1 reports one current row per key."
```

---

## Self-Review

**Spec coverage.** §1 (why dormant) → Task 1's rationale and Task 2's comment. §2 (the fallback shape) → Tasks 2–4. §3 (compare to latest, the SQL, msat, verify before believing) → Tasks 1 and 3. §4 (scope, Bullhorn first) → Tasks 5–6. §5 (what it touches) → Tasks 2–4 file lists. §6 (testing) → Task 3 Step 1 and Task 6 Steps 4–7. §7 (no retention) → Global Constraints. §8 (does not make BRZ-1 unnecessary) → no task needed; it is a statement about scope.

**Gap found and closed.** §6 asks for the A → B → A case asserted against a set-membership implementation. Task 3's offline tests assert the SQL's *shape*, which is weaker. Task 1 Step 1 covers the behaviour directly on the runtime with a four-row fixture, and that is the stronger evidence because it executes rather than inspects.

**Type consistency.** `insert_sql(catalog, schema, sat, log, parent_hk, is_msat)` is used with that signature in Task 3's tests and its implementation. `staged_satellites(model) -> list[tuple[Entity, str]]` is unpacked as `(e, t)` in both. `naming.stg`, `naming.hk`, `naming.vault_schema_for` and `naming.COL[...]` all exist today.

**One deliberate omission.** Task 5 rebinds only Bullhorn. Fieldglass waits on the client mapping (its bindings key on `buyer_code`, which does not exist), UKG on API ingestion, and Striive/HR/ProUnity have no source in this lake. Rebinding them here would be guessing.
