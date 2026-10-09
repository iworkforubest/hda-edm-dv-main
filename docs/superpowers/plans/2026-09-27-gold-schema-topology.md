# Gold Schema Topology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the gold layer's five schemas — `reference_data`, `master_data`, `wd_fin_export`, `governance`, `control` — so the Workday export project has somewhere to be built and the shared schemas exist for every project schema to read.

**Architecture:** One module holds the five names. An emitter renders `CREATE SCHEMA IF NOT EXISTS` DDL from that module; the DDL is committed and byte-gated. A new manually-triggered `gold_build` job applies it, deliberately separate from the 30-task `vault_load`. A TDS-only convenience grant makes the layer visible while it is built, and `schema_grant_check` is extended to sweep gold so a broad grant there can no longer pass unseen.

**Tech Stack:** Python 3.11 and 3.13 (both must pass), stdlib plus PyYAML (already a dependency), Databricks serverless `spark_python_task`, Databricks Asset Bundles, Unity Catalog.

**Spec:** `docs/superpowers/specs/2026-09-27-gold-schema-topology-design.md`

## Global Constraints

- **Every statement is `CREATE ... IF NOT EXISTS`.** No `DROP`, no `CREATE OR REPLACE`. DEF-61 established that replacing a securable discards every grant held against it, and a schema is a securable too.
- **The DDL is qualified with `${gold_catalog}`, never `${catalog}`.** `${catalog}` is silver. Creating these there is the plausible wrong outcome.
- **No schema-level or catalog-level SELECT is ever emitted.** DEF-40 is not suspended because this is gold. `checks/schema_grant_check.py` fails the build on one.
- **`governance` and `control` are readable by nobody.** That is silver's posture: the control surface is not read directly.
- **Generated artefacts follow emit → commit → byte-gate.** The emitter writes the file, the file is committed, and a check fails the build if the two disagree.
- **Tests are NOT pytest.** `tests/test_accelerator.py` and `verify_repo.py` are plain scripts of `check(name, condition, detail)` calls. **An exception raised inside a check's condition aborts the whole suite**, which is worse than a red check — guard every expression that can raise.
- **Where checks go differs per file.** Append at the END of `verify_repo.py`. **INSERT BEFORE the PASS/FAIL summary block in `tests/test_accelerator.py`** — that block holds its only `sys.exit(1)`, and a check after it goes red while the suite prints `ALL CHECKS PASSED` and exits 0.
- **Check counts never decrease.** `tests/test_accelerator.py` is 1592 and `verify_repo.py` is 1395 at the start of this plan.
- **Every new check must be proven red** by mutating the code it guards, then restored. Verify the **exit code** explicitly with `echo $?`; never infer it from output text.
- **No new third-party dependencies.**
- **This plan applies no DDL to any workspace and creates no gold table.** Gold table generation is subsystem C.
- **Do not modify** `src/accelerator/hashing.py`, `RULEBOOK_VERSION`, `tests/golden_hash_vectors.json`, or `metadata/entities/*`.

## Review Focus

1. **A new target added without `gold_export_schema`.** The spec says it deliberately has no default, so configuration must fail naming the target that lacks it — not fall back to something plausible. — Task 1.
2. **The DDL rendered against the silver catalog.** `${catalog}` and `${gold_catalog}` differ by one word and both resolve; getting it wrong silently creates gold's schemas inside silver. — Task 2.
3. **`gold_build` acquiring a schedule, or its task being added to `vault_load`.** Either makes gold run on every vault load, which is the 30-task problem this job exists to avoid. — Task 4.
4. **A grant reaching gold at schema or catalog level.** DEF-40 one layer up: it would cover any SDP `__materialization_*` twin that later appears in gold. — Task 5.
5. **The five names drifting between `gold_layout.SCHEMAS`, the generated DDL, and `databricks.yml`.** Three copies of a list is how the fourth gets missed. — Tasks 1 and 2.

---

### Task 1: `gold_layout.py` — one source for the five names

**Files:**
- Create: `src/accelerator/gold_layout.py`
- Modify: `databricks.yml` (the stale comment above `gold_export_schema` only)
- Modify: `tests/test_accelerator.py` (insert checks before the summary block)

**Interfaces:**
- Produces: `gold_layout.SCHEMAS: dict[str, str]` — schema name to one-line purpose.
- Produces: `gold_layout.READABLE: frozenset[str]` — the subset that consumers may be granted on.
- Produces: `gold_layout.EXPORT_SCHEMA: str` — the project schema name, `"wd_fin_export"`.

- [ ] **Step 1: Write the module**

`src/accelerator/gold_layout.py`:

```python
"""The gold layer's schemas, named once.

WHY A MODULE FOR FIVE STRINGS. These names appear in the emitter, in the generated
DDL, in the verification checks, in the grant job and in databricks.yml. This repo has
already paid for restatement -- gold_quality.py's own comment warns that a duplicated
list is "a fourth thing to keep in step". This is the one source; everything else
derives from it.

FIVE, NOT THE THREE THAT WERE ASKED FOR. governance/control_objects_gold.sql already
creates the gold control schema with aud_load_run, aud_table_load and aud_table_discard.
That schema arrives the moment the file is applied whether or not anyone planned for it,
so it is planned for. `governance` joins it for symmetry with silver, where masks and
grants live apart from the load audit.

READABLE IS A SUBSET, DELIBERATELY. `governance` and `control` are readable by nobody.
That is silver's posture: the control surface is inside the Phase 6 STOP and is not read
directly. A consumer group appearing on either is a defect, not a convenience.
"""
from __future__ import annotations

#: Schema name -> what it holds. The ONE source of the gold layer's topology.
SCHEMAS: dict[str, str] = {
    "reference_data": "shared conformed reference -- calendars, code lists, currency",
    "master_data": "shared conformed master entities -- legal entity, worker, supplier",
    "wd_fin_export": "the project schema; Workday finance export tables",
    "governance": "gold's mask functions and grant DDL",
    "control": "aud_load_run, aud_table_load, aud_table_discard",
}

#: The schemas a consumer may ever be granted on. governance and control are not here.
READABLE: frozenset[str] = frozenset({"reference_data", "master_data", "wd_fin_export"})

#: The project schema. databricks.yml sets gold_export_schema to this, per target.
EXPORT_SCHEMA: str = "wd_fin_export"
```

- [ ] **Step 2: Correct the stale comment in `databricks.yml`**

Find the comment above `gold_export_schema` that reads:

```
  # WHERE GOLD TABLES LIVE. The repo has named a gold CATALOG since the start and has
  # never named a schema in it, because nothing was built there. The planned schemas
  # are reference, master, wd_fin_export, governance and control; wd_fin_export is
  # where export tables go. Gold's generated control DDL already writes to
  # ${control_schema}, so it agrees with that list and needs no change.
```

Replace the sentence naming the schemas so it reads `reference_data, master_data, wd_fin_export, governance and control`, and add one line saying the authority is now `src/accelerator/gold_layout.SCHEMAS`. Change nothing else in that file — no variable values, no targets.

- [ ] **Step 3: Write the failing checks**

Insert into `tests/test_accelerator.py` **immediately before its PASS/FAIL summary block**:

```python
print("\n== the gold layer's schemas are named once, and only once ==")
from accelerator import gold_layout as _gl  # noqa: E402

check("gold_layout declares exactly the five schemas the spec names",
      set(_gl.SCHEMAS) == {"reference_data", "master_data", "wd_fin_export",
                           "governance", "control"},
      f"{sorted(_gl.SCHEMAS)} -- five, because control_objects_gold.sql already creates "
      f"the gold control schema whether or not anyone plans for it, and governance joins "
      f"it for symmetry with silver")

check("READABLE is a strict subset of SCHEMAS, and excludes governance and control",
      _gl.READABLE < set(_gl.SCHEMAS)
      and not (_gl.READABLE & {"governance", "control"}),
      f"READABLE={sorted(_gl.READABLE)} -- the control surface is not read directly, "
      f"which is the posture silver already holds")

check("every schema has a non-empty purpose, so the list cannot decay into bare names",
      all(isinstance(v, str) and v.strip() for v in _gl.SCHEMAS.values()),
      f"{ {k: v for k, v in _gl.SCHEMAS.items() if not (v or '').strip()} }")

check("EXPORT_SCHEMA is one of the declared schemas",
      _gl.EXPORT_SCHEMA in _gl.SCHEMAS,
      f"EXPORT_SCHEMA={_gl.EXPORT_SCHEMA!r} names a schema this module does not declare")

# EVERY TARGET'S gold_export_schema MUST BE A DECLARED SCHEMA, and every target must set
# one. The spec keeps this variable defaultless on purpose: "a wrong-but-plausible default
# is how a table lands in the wrong schema quietly, and an unset one fails at configuration
# time naming the target that lacks it." This check is what keeps databricks.yml tied to
# gold_layout instead of being a second, silently diverging copy.
_gl_doc = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8")) or {}
_gl_targets = {n: (t or {}).get("variables", {}) or {}
               for n, t in (_gl_doc.get("targets") or {}).items()}
_gl_with_gold = {n: v for n, v in _gl_targets.items() if v.get("gold_catalog")}
_gl_missing = sorted(n for n, v in _gl_with_gold.items() if not v.get("gold_export_schema"))
_gl_bad = sorted(f"{n}={v.get('gold_export_schema')!r}" for n, v in _gl_with_gold.items()
                 if v.get("gold_export_schema")
                 and v["gold_export_schema"] not in _gl.SCHEMAS)

check("every target that names a gold catalog also sets gold_export_schema",
      _gl_with_gold and not _gl_missing,
      f"missing on {_gl_missing} -- the variable has NO default deliberately, so an unset "
      f"one must fail at configuration time naming the target, not fall back to something "
      f"plausible")

check("every target's gold_export_schema is a schema gold_layout declares",
      not _gl_bad,
      f"{_gl_bad} -- databricks.yml and gold_layout.SCHEMAS must not diverge; this is the "
      f"tie that stops them being two lists")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$?"
```
Expected: `ALL CHECKS PASSED`, exit 0, count above 1592.

- [ ] **Step 5: Prove the databricks.yml tie can fail**

```bash
cp databricks.yml /tmp/db.bak
sed -i '0,/gold_export_schema: wd_fin_export/s//gold_export_schema: wd_fin_exprt/' databricks.yml
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/db.bak databricks.yml
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the "is a schema gold_layout declares" check goes red, exit 1, then exit 0.

- [ ] **Step 6: Prove the missing-variable check can fail**

```bash
cp databricks.yml /tmp/db.bak
sed -i '0,/^      gold_export_schema: wd_fin_export$/s///' databricks.yml
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/db.bak databricks.yml
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the "also sets gold_export_schema" check goes red, exit 1, then exit 0.

- [ ] **Step 7: Commit**

```bash
git add src/accelerator/gold_layout.py databricks.yml tests/test_accelerator.py
git commit -m "Name the gold layer's five schemas once, in gold_layout"
```

---

### Task 2: The emitter, the generated DDL, and its byte-gate

**Files:**
- Create: `tools/emit_gold_schemas.py`
- Create: `governance/gold_schemas.sql` (generated)
- Modify: `verify_repo.py` (append at the end)

**Interfaces:**
- Consumes: `gold_layout.SCHEMAS` from Task 1.
- Produces: `emit_gold_schemas.render() -> str`, and the committed artefact `governance/gold_schemas.sql`.

- [ ] **Step 1: Write the emitter**

`tools/emit_gold_schemas.py`:

```python
#!/usr/bin/env python3
"""The gold layer's schemas, as DDL rendered from gold_layout.SCHEMAS.

SEPARATE FROM control_objects_gold.sql ON PURPOSE. That file's contract is the control
surface, and checks/control_conformance_check.py gates it against
control_standard.LAYER_TABLES["gold"]. Folding four unrelated CREATE SCHEMA statements
into it would widen a gate whose whole value is its narrowness.

CREATE SCHEMA IF NOT EXISTS, NEVER A REPLACE. DEF-61, measured 27 September 2026:
replacing a securable discards every grant held against it, and a schema is a securable.
This file is applied repeatedly by design, so it must be idempotent in the strict sense
-- running it twice must change nothing at all.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import gold_layout  # noqa: E402

OUT_PATH = ROOT / "governance" / "gold_schemas.sql"


def render() -> str:
    lines = [
        "-- GENERATED by tools/emit_gold_schemas.py from accelerator.gold_layout.SCHEMAS.",
        "-- Do not edit. Run the emitter and commit the result.",
        "--",
        "-- Applied by checks/apply_gold_schemas.py, from the gold_build job.",
        "-- CREATE ... IF NOT EXISTS only: a schema is a securable, and replacing one",
        "-- discards every grant held against it (DEF-61).",
        "",
    ]
    for name, purpose in gold_layout.SCHEMAS.items():
        lines.append(f"-- {name}: {purpose}")
        lines.append(
            f"CREATE SCHEMA IF NOT EXISTS `${{gold_catalog}}`.`{name}`;")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    OUT_PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)}")
```

- [ ] **Step 2: Generate and inspect**

```bash
.venv/bin/python tools/emit_gold_schemas.py
cat governance/gold_schemas.sql
```
Expected: five `CREATE SCHEMA IF NOT EXISTS` statements, each qualified `${gold_catalog}`, each preceded by its purpose comment.

- [ ] **Step 3: Write the failing checks**

Append at the END of `verify_repo.py`:

```python
# --- gold schema topology (subsystem A, Task 2) --------------------------------------
sys.path.insert(0, str(ROOT / "tools"))
from accelerator import gold_layout as _gl_mod  # noqa: E402

_gs_path = ROOT / "governance" / "gold_schemas.sql"
_gs = _gs_path.read_text(encoding="utf-8") if _gs_path.exists() else ""
try:
    import emit_gold_schemas as _gs_emit
    _gs_rendered = _gs_emit.render()
except Exception as _gs_exc:  # noqa: BLE001 -- must not abort the suite
    _gs_emit, _gs_rendered = None, f"EMITTER RAISED: {_gs_exc!r}"

check("gold_schemas.sql is byte-identical to what its emitter renders",
      bool(_gs) and _gs == _gs_rendered,
      "emit -> commit -> byte-gate: run tools/emit_gold_schemas.py and commit the result")

check("the DDL creates exactly the schemas gold_layout declares, no more and no fewer",
      bool(_gs)
      and {n for n in _gl_mod.SCHEMAS if f"`{n}`" in _gs} == set(_gl_mod.SCHEMAS)
      and _gs.count("CREATE SCHEMA IF NOT EXISTS") == len(_gl_mod.SCHEMAS),
      f"declared {sorted(_gl_mod.SCHEMAS)}; the DDL has "
      f"{_gs.count('CREATE SCHEMA IF NOT EXISTS')} CREATE SCHEMA statement(s)")

check("every gold DDL statement is CREATE ... IF NOT EXISTS -- never a replace or a drop",
      bool(_gs)
      and "CREATE OR REPLACE" not in _gs.upper()
      and "DROP " not in _gs.upper()
      and _gs.count("CREATE SCHEMA") == _gs.count("CREATE SCHEMA IF NOT EXISTS"),
      "DEF-61: replacing a securable discards every grant held against it, and a schema "
      "is a securable too. This file is applied repeatedly by design")

check("the gold DDL is qualified with ${gold_catalog}, never ${catalog}",
      bool(_gs) and "${gold_catalog}" in _gs and "${catalog}" not in
      _gs.replace("${gold_catalog}", ""),
      "${catalog} is SILVER. Creating gold's schemas there is the plausible wrong "
      "outcome, and it would succeed silently")

check("the gold DDL creates no TABLE -- subsystem A creates schemas and nothing else",
      bool(_gs) and "CREATE TABLE" not in _gs.upper(),
      "gold table generation is subsystem C; a table here would be built before anything "
      "declares what it should contain")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -2
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$?"
```
Expected: `VERIFICATION PASSED`, exit 0, count above 1395.

- [ ] **Step 5: Prove the silver-catalog check can fail**

```bash
cp tools/emit_gold_schemas.py /tmp/eg.bak
sed -i 's/\${gold_catalog}/${catalog}/' tools/emit_gold_schemas.py
.venv/bin/python tools/emit_gold_schemas.py
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -3
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/eg.bak tools/emit_gold_schemas.py
.venv/bin/python tools/emit_gold_schemas.py
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the `${gold_catalog}` check goes red (and the byte-gate until regenerated), exit 1, then exit 0.

- [ ] **Step 6: Prove the replace check can fail**

```bash
cp tools/emit_gold_schemas.py /tmp/eg.bak
sed -i 's/CREATE SCHEMA IF NOT EXISTS/CREATE OR REPLACE SCHEMA/' tools/emit_gold_schemas.py
.venv/bin/python tools/emit_gold_schemas.py
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -3
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/eg.bak tools/emit_gold_schemas.py
.venv/bin/python tools/emit_gold_schemas.py
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the CREATE-IF-NOT-EXISTS check goes red, exit 1, then exit 0.

- [ ] **Step 7: Commit**

```bash
git add tools/emit_gold_schemas.py governance/gold_schemas.sql verify_repo.py
git commit -m "Generate and byte-gate the gold schema DDL"
```

---

### Task 3: The applier

**Files:**
- Create: `checks/apply_gold_schemas.py`
- Modify: `verify_repo.py` (append at the end)

**Interfaces:**
- Consumes: `governance/gold_schemas.sql` from Task 2; `render` and `statements` from `checks/apply_governance.py`.
- Produces: `checks/apply_gold_schemas.py`, runnable as a `spark_python_task` with `--gold-catalog` and `--dry-run`.

- [ ] **Step 1: Write the applier**

`checks/apply_gold_schemas.py`:

```python
#!/usr/bin/env python3
"""Create the gold layer's schemas. Schemas only -- no tables, no grants, no masks.

WHY A TASK RATHER THAN A HAND-RUN FILE. governance/control_objects_gold.sql has been
generated, committed and gated since 29 August and applied by nobody, which is why the
gold catalog held only information_schema on 27 September. A step that everything else
depends on cannot be manual.

IT GRANTS NOTHING. Subsystem A creates the topology; the access model is subsystem B,
and the TDS convenience grant lives in the grant_vault_access job where it can be read
and removed as one thing. A grant emitted here would be invisible to that decision.

DEF-12: checks/ is exec()'d by a serverless spark_python_task with no __file__, so the
SQL file is located from a path this module computes at import time the same way its
siblings do.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "checks"))

from apply_governance import render, statements  # noqa: E402

SQL_FILE = ROOT / "governance" / "gold_schemas.sql"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold-catalog", required=True)
    ap.add_argument("--dry-run", action="store_true",
                    help="render and split without executing; needs no workspace")
    args = ap.parse_args()

    stmts = statements(render(SQL_FILE.read_text(encoding="utf-8"),
                              {"gold_catalog": args.gold_catalog}))
    print(f"{len(stmts)} statement(s) rendered for gold_catalog={args.gold_catalog}")

    if args.dry_run:
        for i, s in enumerate(stmts, 1):
            print(f"  {i:2d}. {s.splitlines()[0][:88]}")
        return 0

    from pyspark.sql import SparkSession  # noqa: PLC0415 -- not needed for --dry-run
    spark = SparkSession.builder.getOrCreate()
    failed = []
    for i, s in enumerate(stmts, 1):
        first = s.splitlines()[0][:88]
        try:
            spark.sql(s)
            print(f"  ok   {i:2d}. {first}")
        except Exception as exc:  # noqa: BLE001 -- report every failure, not the first
            print(f"  FAIL {i:2d}. {first}\n       {exc}")
            failed.append(first)

    if failed:
        print(f"\nGOLD SCHEMA CREATION FAILED -- {len(failed)} statement(s)")
        return 1
    print(f"\nGOLD SCHEMAS PRESENT: {len(stmts)} statement(s) applied to "
          f"{args.gold_catalog}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Exercise the dry run, which needs no workspace**

```bash
.venv/bin/python checks/apply_gold_schemas.py --dry-run --gold-catalog 03_usnc_gold_edm_dev
```
Expected: `5 statement(s) rendered for gold_catalog=03_usnc_gold_edm_dev`, then five numbered `CREATE SCHEMA IF NOT EXISTS \`03_usnc_gold_edm_dev\`...` lines. Confirm the catalog placeholder really was substituted — a rendered statement still containing `${gold_catalog}` means `render()` was not applied.

- [ ] **Step 3: Write the failing checks**

Append at the END of `verify_repo.py`:

```python
_ags_src = (ROOT / "checks" / "apply_gold_schemas.py").read_text(encoding="utf-8")

check("apply_gold_schemas reads the GENERATED file, not a SQL string of its own",
      "gold_schemas.sql" in _ags_src
      and "CREATE SCHEMA" not in _ags_src.upper().replace("CREATE SCHEMA IF NOT EXISTS", ""),
      "a second copy of the DDL inside the applier is exactly the drift the byte-gate "
      "exists to catch, moved somewhere the byte-gate cannot see it")

check("apply_gold_schemas emits no GRANT and no REVOKE",
      "GRANT" not in _ags_src.upper() and "REVOKE" not in _ags_src.upper(),
      "subsystem A creates the topology; the access model is subsystem B, and the TDS "
      "convenience grant lives in grant_vault_access where it can be removed as one thing")

check("apply_gold_schemas reports EVERY failing statement, not just the first",
      "failed" in _ags_src and "for i, s in enumerate(stmts" in _ags_src,
      "stopping at the first failure hides how much of the topology is missing, which is "
      "the thing the operator needs to know")

check("apply_gold_schemas imports pyspark lazily, so --dry-run runs with no workspace",
      "from pyspark.sql import SparkSession" in _ags_src
      and _ags_src.index("def main") < _ags_src.index("from pyspark.sql import"),
      "a module-scope pyspark import makes --dry-run impossible on any machine without "
      "a JVM, which is every machine this repo's offline suites run on")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -2
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$?"
```
Expected: `VERIFICATION PASSED`, exit 0.

- [ ] **Step 5: Prove the no-GRANT check can fail**

```bash
cp checks/apply_gold_schemas.py /tmp/ags.bak
printf '\n# GRANT SELECT ON SCHEMA placeholder\n' >> checks/apply_gold_schemas.py
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/ags.bak checks/apply_gold_schemas.py
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the no-GRANT check goes red, exit 1, then exit 0.

- [ ] **Step 6: Commit**

```bash
git add checks/apply_gold_schemas.py verify_repo.py
git commit -m "Apply the gold schema DDL from a task, not by hand"
```

---

### Task 4: The `gold_build` job

**Files:**
- Create: `resources/gold_job.yml`
- Modify: `verify_repo.py` (append at the end)

**Interfaces:**
- Consumes: `checks/apply_gold_schemas.py` from Task 3.
- Produces: bundle job `gold_build` with a single task `create_gold_schemas`.

- [ ] **Step 1: Write the job resource**

`resources/gold_job.yml`:

```yaml
# THE GOLD LAYER'S OWN JOB. Manually triggered, not scheduled.
#
# NOT A TASK IN vault_load. Adrian, 26 September: "we have a job with almost 30 task,
# this is not ok". Gold is a different layer on a different cadence, and the domain-split
# spec already rules that layered jobs are the direction. This is where subsystem C's
# export tasks will land, and where invoice_export moves when it is implemented.
#
# IT CREATES SCHEMAS AND NOTHING ELSE. No tables -- gold table generation is subsystem C.
# No grants -- the access model is subsystem B, and the TDS convenience grant lives in
# grant_vault_access so it can be read and removed as one thing.
#
# SAFE TO RE-RUN. Every statement is CREATE SCHEMA IF NOT EXISTS, so a second run changes
# nothing. That matters because a schema is a securable: replacing one would discard every
# grant held against it (DEF-61).
resources:
  jobs:
    gold_build:
      name: "[${bundle.target}] hfig gold build"
      max_concurrent_runs: 1
      email_notifications:
        on_failure: ["${var.notification_email}"]
      tasks:
        - task_key: create_gold_schemas
          spark_python_task:
            python_file: ../checks/apply_gold_schemas.py
            parameters:
              - "--gold-catalog"
              - "${var.gold_catalog}"
          environment_key: checks
      environments:
        - environment_key: checks
          spec:
            client: "3"
            dependencies: ["pyyaml"]
```

- [ ] **Step 2: Validate the bundle**

```bash
timeout 400 databricks bundle validate -t usnc_tds --profile hfig-usnc-tds \
  --var="service_principal=7732b208-8366-4aef-af09-60e9dec9cf86" 2>&1 | tail -6
```
Expected: no error. The pre-existing warning about `/Workspace/Shared` being writable by all workspace users is unrelated to this change and is expected — do not try to fix it.

- [ ] **Step 3: Write the failing checks**

Append at the END of `verify_repo.py`:

```python
_gj_path = ROOT / "resources" / "gold_job.yml"
try:
    _gj = yaml.safe_load(_gj_path.read_text(encoding="utf-8")) or {}
except Exception as _gj_exc:  # noqa: BLE001
    _gj = {}
_gj_job = ((_gj.get("resources") or {}).get("jobs") or {}).get("gold_build") or {}
_gj_tasks = _gj_job.get("tasks") or []
_vl = yaml.safe_load((ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8")) or {}
_vl_keys = {t.get("task_key") for t in
            (((_vl.get("resources") or {}).get("jobs") or {}).get("vault_load") or {})
            .get("tasks") or []}

check("gold_build exists and runs exactly one task, create_gold_schemas",
      [t.get("task_key") for t in _gj_tasks] == ["create_gold_schemas"],
      f"tasks={[t.get('task_key') for t in _gj_tasks]}")

check("gold_build carries NO schedule and NO trigger -- it is manually invoked",
      "schedule" not in _gj_job and "trigger" not in _gj_job
      and "continuous" not in _gj_job,
      f"keys={sorted(_gj_job)} -- a scheduled gold_build makes gold run on a cadence "
      f"nobody chose, and re-runs DDL nothing asked to re-run")

check("create_gold_schemas is NOT also a task in vault_load",
      "create_gold_schemas" not in _vl_keys,
      "the whole reason gold has its own job is that vault_load already carries ~30 "
      "tasks; adding this one there reintroduces the problem it was split to solve")

check("gold_build passes ${var.gold_catalog}, never ${var.catalog}",
      any("${var.gold_catalog}" in str(t.get("spark_python_task", {}).get("parameters"))
          and "${var.catalog}" not in str(t.get("spark_python_task", {})
                                          .get("parameters"))
          for t in _gj_tasks),
      "${var.catalog} is SILVER; passing it here creates gold's schemas in the vault's "
      "own catalog and nothing would complain")

check("gold_build declares no task that writes gold TABLES",
      not any("invoice_export" in str(t) or "load_" in str(t.get("task_key", ""))
              for t in _gj_tasks),
      "subsystem A creates the topology only; invoice_export moves here in subsystem C, "
      "and it is still a NOT IMPLEMENTED stub today")
```

- [ ] **Step 4: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -2
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$?"
```

- [ ] **Step 5: Prove the schedule check can fail**

```bash
cp resources/gold_job.yml /tmp/gj.bak
.venv/bin/python - <<'PY'
p="resources/gold_job.yml"; s=open(p).read()
s=s.replace("      max_concurrent_runs: 1",
            "      max_concurrent_runs: 1\n      schedule:\n"
            "        quartz_cron_expression: '0 0 3 * * ?'\n"
            "        timezone_id: UTC")
open(p,"w").write(s)
PY
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/gj.bak resources/gold_job.yml
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the no-schedule check goes red, exit 1, then exit 0.

- [ ] **Step 6: Commit**

```bash
git add resources/gold_job.yml verify_repo.py
git commit -m "A gold_build job, manually triggered and separate from vault_load"
```

---

### Task 5: The TDS visibility grant

**Files:**
- Modify: `checks/apply_governance.py` (extend the grant emitter to cover gold)
- Modify: `resources/grant_vault_access.yml` (pass the gold catalog)
- Modify: `verify_repo.py` (append at the end)

**Interfaces:**
- Consumes: `gold_layout.READABLE` from Task 1. Also consumes the module alias `_gl_mod`, which **Task 2 already bound at module scope in `verify_repo.py`** — reuse it, do not re-import.
- Produces: `apply_governance.gold_access_grants(gold_catalog: str, group: str) -> list[str]`.

**This task emits `USE CATALOG` and `USE SCHEMA` only.** Gold has no tables yet, so there is no per-object `SELECT` to emit. The function must say so out loud rather than return an empty list silently — that is the failure mode `apply_governance` already guards with its `NO DATA GRANTS EMITTED` message.

- [ ] **Step 1: Write the failing checks first**

Append at the END of `verify_repo.py`:

```python
sys.path.insert(0, str(ROOT / "checks"))
try:
    import apply_governance as _ag
    _gg = _ag.gold_access_grants("03_usnc_gold_edm_dev", "scope_tds_edm_vault_read")
except Exception as _gg_exc:  # noqa: BLE001
    _ag, _gg = None, []
    print(f"  (gold_access_grants unavailable: {_gg_exc!r})")

check("gold_access_grants emits USE CATALOG and USE SCHEMA, and nothing broader",
      bool(_gg)
      and any(s.upper().startswith("GRANT USE CATALOG") for s in _gg)
      and all("SELECT" not in s.upper() for s in _gg),
      f"{_gg} -- DEF-40 is not suspended because this is gold: a schema-level SELECT "
      f"would cover any SDP __materialization_* twin that later appears there")

check("gold_access_grants covers exactly the READABLE schemas, never governance or control",
      bool(_gg)
      and {n for n in _gl_mod.SCHEMAS if f"`{n}`" in " ".join(_gg)} == set(_gl_mod.READABLE),
      f"granted on {sorted(n for n in _gl_mod.SCHEMAS if f'`{n}`' in ' '.join(_gg))}, "
      f"READABLE is {sorted(_gl_mod.READABLE)} -- the control surface is not read directly")

check("gold_access_grants never emits GRANT SELECT ON SCHEMA or ON CATALOG",
      all("ON SCHEMA" not in s.upper() or "USE SCHEMA" in s.upper() for s in _gg)
      and all("ON CATALOG" not in s.upper() or "USE CATALOG" in s.upper() for s in _gg),
      f"{_gg}")

_gva = yaml.safe_load((ROOT / "resources" / "grant_vault_access.yml")
                      .read_text(encoding="utf-8")) or {}
_gva_params = str((((_gva.get("resources") or {}).get("jobs") or {})
                   .get("grant_vault_access") or {}).get("tasks"))
check("grant_vault_access passes the gold catalog, so the grant job can reach gold",
      "${var.gold_catalog}" in _gva_params,
      "without it the gold schemas stay invisible exactly as the vault did on 27 "
      "September, and the whole point of this grant is that they do not")
```

- [ ] **Step 2: Run to verify they fail**

```bash
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL|gold_access_grants unavailable" | head -4
```
Expected: the gold grant checks are red because `gold_access_grants` does not exist yet.

- [ ] **Step 3: Implement the emitter**

Add to `checks/apply_governance.py`, beside `data_access_grants`:

```python
def gold_access_grants(gold_catalog: str, group: str) -> list[str]:
    """USE CATALOG and USE SCHEMA on gold's READABLE schemas. No SELECT, ever.

    THIS IS A TDS CONVENIENCE AND IT IS LABELLED AS ONE. Subsystem B defines who may read
    gold -- data engineers, data modelers, the data architect, data analysts -- and
    replaces this before production. Adrian, 27 September: the security model gets defined
    and tested before prod, and TDS should not be blind meanwhile.

    IT EMITS NO SELECT BECAUSE GOLD HAS NO TABLES. When it does, the SELECT grants come
    per object from a declared list, exactly as data_access_grants does for the vault --
    never ON SCHEMA. DEF-40 measured the cost of the broader form: a schema-level grant
    covers SDP's unmasked __materialization_* twins, and gold will hold streaming tables
    the day subsystem C lands.

    governance and control are NOT granted. That is silver's posture: the control surface
    is inside the Phase 6 STOP and is not read directly.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from accelerator import gold_layout  # noqa: PLC0415

    grants = [f"GRANT USE CATALOG ON CATALOG `{gold_catalog}` TO `{group}`"]
    for schema in sorted(gold_layout.READABLE):
        grants.append(
            f"GRANT USE SCHEMA ON SCHEMA `{gold_catalog}`.`{schema}` TO `{group}`")
    return grants
```

Then wire it into `main()` beside the existing `--emit-data-grants` branch, guarded by a new `--gold-catalog` argument. When `--gold-catalog` is passed **and** `--emit-data-grants` is set, append `gold_access_grants(args.gold_catalog, args.privileged_group)` to `stmts`, and print how many gold grants were added. When `--gold-catalog` is passed and gold holds no tables, print explicitly that **no gold SELECT was emitted because gold declares no tables yet** — a run that grants nothing must not look like a run that granted successfully.

- [ ] **Step 4: Pass the gold catalog from the grant job**

In `resources/grant_vault_access.yml`, add to the task's `parameters`, after `--gold-catalog`'s siblings:

```yaml
              - "--gold-catalog"
              - "${var.gold_catalog}"
```

- [ ] **Step 5: Run and confirm green**

```bash
.venv/bin/python verify_repo.py 2>&1 | tail -2
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$?"
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "tests exit=$?"
```

- [ ] **Step 6: Dry-run the grant emitter end to end**

```bash
.venv/bin/python checks/apply_governance.py --dry-run --emit-data-grants \
  --catalog 02_usnc_silver_edm_dev \
  --vault-schema raw_vault --business-vault-schema business_vault \
  --bronze-catalog 01_usnc_bronze_dev --gold-catalog 03_usnc_gold_edm_dev \
  --privileged-group scope_tds_edm_vault_read --active-sources "" 2>&1 | \
  grep -iE "gold|USE CATALOG|USE SCHEMA" | head -8
```
Expected: one `GRANT USE CATALOG` on the gold catalog and three `GRANT USE SCHEMA` — `master_data`, `reference_data`, `wd_fin_export`. **No `SELECT` on any gold object.**

- [ ] **Step 7: Prove the no-broad-grant check can fail**

```bash
cp checks/apply_governance.py /tmp/ag.bak
.venv/bin/python - <<'PY'
p="checks/apply_governance.py"; s=open(p).read()
s=s.replace('    grants = [f"GRANT USE CATALOG ON CATALOG `{gold_catalog}` TO `{group}`"]',
            '    grants = [f"GRANT USE CATALOG ON CATALOG `{gold_catalog}` TO `{group}`",\n'
            '              f"GRANT SELECT ON SCHEMA `{gold_catalog}`.`reference_data` TO `{group}`"]')
open(p,"w").write(s)
PY
.venv/bin/python verify_repo.py 2>&1 | grep -E "^  FAIL" | head -3
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/ag.bak checks/apply_governance.py
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the "nothing broader" and "never ON SCHEMA" checks go red, exit 1, then exit 0. **This is the most important mutation in the plan** — it proves DEF-40's guard reaches gold.

- [ ] **Step 8: Commit**

```bash
git add checks/apply_governance.py resources/grant_vault_access.yml verify_repo.py
git commit -m "Let the TDS grant job reach gold, with USE SCHEMA and never SELECT"
```

---

### Task 6: Extend `schema_grant_check` to sweep gold

**Files:**
- Modify: `checks/schema_grant_check.py`
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Consumes: `gold_layout.SCHEMAS` from Task 1.
- Produces: the gold catalog and its five schemas added to the gate's swept securables.

**Why this matters:** the gate currently inspects silver, control and bronze. Its own run output on 27 September listed `CATALOG 02_usnc_silver_edm_dev`, three silver schemas and `CATALOG 01_usnc_bronze_dev` — **gold is absent**, so a broad grant there would pass unseen. Subsystem C will put real money data in gold.

- [ ] **Step 1: Read how the gate sweeps a catalog today**

```bash
grep -n "bronze_catalog\|def main\|add_argument\|securable" checks/schema_grant_check.py | head -20
```
Note the existing pattern for adding a catalog and its schemas to the swept set. Follow it exactly rather than inventing a second shape — the gate's report format is asserted by other checks.

- [ ] **Step 2: Write the failing checks**

Insert into `tests/test_accelerator.py` **immediately before its PASS/FAIL summary block**:

```python
print("\n== the broad-grant gate reaches gold, not only silver and bronze ==")
_sgc_src = (ROOT / "checks" / "schema_grant_check.py").read_text(encoding="utf-8")

check("schema_grant_check accepts a --gold-catalog to sweep",
      "--gold-catalog" in _sgc_src,
      "the gate inspected silver, control and bronze on 27 September and NOT gold, so a "
      "catalog- or schema-level SELECT in gold would have passed unseen")

check("schema_grant_check sweeps gold's schemas from gold_layout, not a hand-typed list",
      "gold_layout" in _sgc_src,
      "a second copy of the five names here is the fourth thing to keep in step; the "
      "gate must ask gold_layout what gold contains")

check("the gold sweep is not optional-by-default -- a missing gold catalog is announced",
      "gold" in _sgc_src.lower()
      and ("NOT_EVALUATED" in _sgc_src or "not_evaluated" in _sgc_src),
      "a gate that silently skips a layer reports success over a layer it never looked "
      "at, which is the failure this repo has shipped before")
```

- [ ] **Step 3: Run to verify they fail**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -3
```
Expected: all three red.

- [ ] **Step 4: Extend the gate**

Add a `--gold-catalog` argument to `checks/schema_grant_check.py`. When it is supplied, sweep the gold catalog and each schema in `gold_layout.SCHEMAS` using the same securable-inspection path the silver catalog already uses, and include them in the inspected count and the printed report. When it is **not** supplied, print an explicit `NOT_EVALUATED` line naming gold — never skip silently.

Import `gold_layout` rather than restating the names:

```python
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from accelerator import gold_layout  # noqa: E402
```

- [ ] **Step 5: Pass the gold catalog from the job**

In `resources/vault_job.yml`, add to the `assert_no_broad_grant` task's `parameters`:

```yaml
              - "--gold-catalog"
              - "${var.gold_catalog}"
```

- [ ] **Step 6: Run and confirm green**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$?"
.venv/bin/python verify_repo.py >/dev/null 2>&1; echo "verify exit=$?"
timeout 400 databricks bundle validate -t usnc_tds --profile hfig-usnc-tds \
  --var="service_principal=7732b208-8366-4aef-af09-60e9dec9cf86" 2>&1 | tail -4
```
Expected: both suites exit 0 with counts above 1592 / 1395, and the bundle validates.

- [ ] **Step 7: Prove the gold sweep check can fail**

```bash
cp checks/schema_grant_check.py /tmp/sgc.bak
sed -i 's/--gold-catalog/--gold-cat-disabled/' checks/schema_grant_check.py
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/sgc.bak checks/schema_grant_check.py
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```
Expected: the `--gold-catalog` check goes red, exit 1, then exit 0.

- [ ] **Step 8: Final full run and commit**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
.venv/bin/python verify_repo.py 2>&1 | tail -2
git add checks/schema_grant_check.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Sweep gold for broad grants, so DEF-40's gate covers the consumer layer"
```

---

## Deploying and running, after all six tasks

Not a task, because it touches a live workspace and is the repo owner's call.

```bash
# Preflight is mandatory before EVERY deploy, including redeploys of unchanged code.
.venv/bin/python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds

timeout 600 databricks bundle deploy -t usnc_tds --profile hfig-usnc-tds \
  --var="service_principal=7732b208-8366-4aef-af09-60e9dec9cf86"

timeout 900 databricks bundle run gold_build -t usnc_tds --profile hfig-usnc-tds \
  --var="service_principal=7732b208-8366-4aef-af09-60e9dec9cf86"

# Then, to make the layer visible in TDS:
timeout 900 databricks bundle run grant_vault_access -t usnc_tds --profile hfig-usnc-tds \
  --var="service_principal=7732b208-8366-4aef-af09-60e9dec9cf86"
```

`grant_vault_access` exits non-zero while any declared vault object does not exist — on 27 September that was 17 unbuilt satellites. That failure is correct and loud and is not caused by this plan.

## What this plan does NOT do, and why

- **It creates no gold table.** Subsystem C builds those from the DCDDs and the AME invoice rules. A table here would be built before anything declares what it should contain.
- **It does not define who may read gold.** That is subsystem B — data engineers, data modelers, the data architect, data analysts. Task 5 is a labelled TDS convenience that B replaces before production.
- **It does not move `invoice_export` into `gold_build`.** That task is still a `NOT IMPLEMENTED` stub; moving it is subsystem C's change.
- **It does not declare a gold dashboard.** `verify_repo.py` has a check forbidding one while gold has nothing to show, and that check stays. Creating empty schemas does not change its premise — it is scoped to `quality_gold*` resource keys and this plan adds none.
- **It does not answer the masking question.** Whether a gold write materialises masked or cleartext values depends on which side of `is_account_group_member('hfig_commercials_reader')` the writing principal sits. It is unmeasured, it does not block this plan because nothing here generates data, and it blocks subsystem C.
