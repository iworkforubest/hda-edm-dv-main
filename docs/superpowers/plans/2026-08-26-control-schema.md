# Control Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the vault a `control` schema that records what every load writer actually did — accepted rows, discarded rows and why — and make an unaudited load a failed load.

**Architecture:** A new `control` schema created by a new job task at position 1, holding the two relocated control tables plus three new audit tables. Each writer records its own numbers through one shared helper in `src/accelerator/audit.py`; nothing infers another component's counts. Four gate changes bring `control` inside the existing append-only and grant assertions and add two new checks.

**Tech Stack:** Python 3.11/3.13, PySpark on Databricks serverless, Databricks Asset Bundles, Delta/Unity Catalog. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-26-control-schema-design.md`

## Global Constraints

- **This repo's tests are not pytest.** Two suites, both run as plain scripts: `uv run --frozen python tests/test_accelerator.py` and `uv run --frozen python verify_repo.py`. Add assertions as `check("name", condition, "detail")` calls. Never introduce pytest.
- **Every new check must be proven able to fail before it is believed.** Mutate the code under test, confirm the check reports FAIL (not ABSENT — an aborted suite hides every later check), restore, and record the mutation in the commit message. This repo has shipped five checks that could never fail.
- **Never use `str.index` or `[0]` on a possibly-empty result inside a check.** Both raise and abort the suite instead of failing one check. Use `.find()` and compare against `-1`, or `" ".join(...)`.
- **No semicolon may appear inside a comment in any `.sql` file.** `apply_governance.statements()` splits on `;` regardless of comments; `verify_repo.py` fails the build on prose-leading statements.
- **Never auto-select a Databricks profile.** Pass `--profile hfig-usnc-tds` explicitly. Every other profile is EU production.
- **`preflight_target.py` must pass immediately before every `databricks bundle deploy`.**
- **`bundle validate` is run without `--strict`** — one known warning (DEF-43, world-writable bundle root) is expected and is not a failure.
- Vault tables are append-only. `control` becomes append-only too: INSERT only, never UPDATE.
- Schema names always come from bundle variables, never literals.

---

## File Structure

| file | responsibility |
|---|---|
| `databricks.yml` | new `control_schema` variable; per-target values |
| `governance/control_objects.sql` | DDL for the `control` schema and its five tables; placeholders, no literals |
| `checks/apply_control_objects.py` | **new** — renders and executes `control_objects.sql`, writes the `opened` run row |
| `src/accelerator/audit.py` | **new** — the single definition of how an audit row is written |
| `checks/load_hubs.py` | records staged / accepted / two discard reasons per hub |
| `checks/load_satellites.py` | records staged / accepted / one discard reason per satellite |
| `checks/publish_metadata.py` | writes the `completed` run row |
| `checks/schema_grant_check.py` | sweeps `control` too; new orphan-schema assertion |
| `checks/append_only_check.py` | no code change — gains `--schema control` in the job |
| `checks/audit_completeness_check.py` | **new** — every run closed, and the discard arithmetic balances |
| `resources/vault_job.yml` | the new task, the new parameters, `{{job.run_id}}` on every writer |

---

### Task 1: The `control_schema` variable and the DDL

**Files:**
- Modify: `databricks.yml` (variables block ~line 140; each of the 9 targets)
- Modify: `governance/control_objects.sql`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing.
- Produces: bundle variable `control_schema` (default `"control"`); `control_objects.sql` containing `${catalog}`, `${governance_schema}`, `${control_schema}` placeholders only, and `CREATE TABLE` statements for `ctl_approval_manifest`, `ref_dq_expectation`, `aud_load_run`, `aud_table_load`, `aud_table_discard`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_accelerator.py`, immediately before the final `print("\n" + "=" * 62)` block:

```python
# The control schema is declared by a bundle variable, never a literal. control_objects.sql
# used to hardcode `${catalog}`.governance despite var.governance_schema existing, which is
# how a schema name drifts from the variable that claims to own it.
_ctl_sql = (ROOT / "governance" / "control_objects.sql").read_text(encoding="utf-8")
check("control_objects.sql names no schema as a literal",
      ".governance." not in _ctl_sql and ".control." not in _ctl_sql,
      "both schema names must be ${governance_schema} / ${control_schema} placeholders")
# The qualified form ONLY. An earlier draft of this check had `or f"{t}" in _ctl_sql`,
# which is satisfied by the table name appearing anywhere -- including in a comment. That
# is a check that cannot fail, and this repo has shipped five of those.
_missing_tables = [t for t in ("ctl_approval_manifest", "ref_dq_expectation",
                               "aud_load_run", "aud_table_load", "aud_table_discard")
                   if f"${{control_schema}}`.{t} " not in _ctl_sql
                   and f"${{control_schema}}`.{t}\n" not in _ctl_sql]
check("it declares all five control tables, qualified by the placeholder",
      not _missing_tables, str(_missing_tables))
check("no semicolon hides inside a comment, which would split a statement",
      not [l for l in _ctl_sql.splitlines()
           if l.strip().startswith("--") and ";" in l],
      str([l for l in _ctl_sql.splitlines()
           if l.strip().startswith("--") and ";" in l][:2]))
_bundle_vars = yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]
check("control_schema is a declared bundle variable",
      "control_schema" in _bundle_vars, str(sorted(_bundle_vars)[:12]))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "control_schema|control_objects"`
Expected: FAIL on all four — the variable does not exist and the SQL still says `.governance.`

- [ ] **Step 3: Add the bundle variable**

In `databricks.yml`, in the `variables:` block after `governance_schema`:

```yaml
  control_schema:
    description: >-
      Schema holding load control and the load audit: the approval manifest, the DQ
      expectation reference, and the aud_* tables recording what each writer did.
      Separate from governance, which holds masks and grants only. Nobody reads it
      directly -- it is inside the Phase 6STOP posture like the vault schemas are.
    default: control
```

No per-target override is needed — `control` is correct in every lake, exactly as `governance` is.

- [ ] **Step 4: Rewrite the DDL**

In `governance/control_objects.sql`, replace `CREATE SCHEMA IF NOT EXISTS `${catalog}`.governance;` and both existing `CREATE TABLE` headers so that every schema is a placeholder, and append the three audit tables:

```sql
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${governance_schema}`;
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${control_schema}`;
```

Change both existing tables from `` `${catalog}`.governance.<name> `` to
`` `${catalog}`.`${control_schema}`.<name> ``, leaving their column lists, comments and
`TBLPROPERTIES ('hfig.control_object' = 'true')` untouched. Then append:

```sql
-- The load audit. One row per table per run, and the discard breakdown beside it.
--
-- TWO TABLES, NOT ONE. A single table repeating staged on one row per discard reason
-- forces the rule "sum accepted, but take max of staged, never sum it" -- a trap where
-- the first SELECT sum(staged) is wrong and no gate notices. It also puts a nullable
-- column inside the grain key, and SQL NULL does not compare equal, so that grain's
-- uniqueness cannot be asserted with an equality join.
--
-- Normalised, staged and accepted are stated once per table per run, so staged - accepted
-- is the total discarded and checks/audit_completeness_check.py can assert it equals
-- SUM(discarded). A discard the writer failed to attribute becomes an arithmetic gap
-- rather than nothing at all.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.aud_table_load (
  job_run_id          STRING     NOT NULL COMMENT 'the job run that wrote this, from {{job.run_id}}',
  pipeline_update_id  STRING              COMMENT 'ties to the pipeline event log; NULL for a batch loader',
  table_name          STRING     NOT NULL COMMENT 'the table written',
  written_by          STRING     NOT NULL COMMENT 'the script that wrote it',
  staged              BIGINT     NOT NULL COMMENT 'rows the writer read from its source',
  accepted            BIGINT     NOT NULL COMMENT 'rows it inserted',
  recorded_at         TIMESTAMP  NOT NULL COMMENT 'when the writer recorded this'
)
CLUSTER BY (job_run_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.aud_table_discard (
  job_run_id      STRING     NOT NULL COMMENT 'the job run that wrote this',
  table_name      STRING     NOT NULL COMMENT 'the table written',
  discard_reason  STRING     NOT NULL COMMENT 'why these rows were not inserted',
  discarded       BIGINT     NOT NULL COMMENT 'how many, for this reason',
  recorded_at     TIMESTAMP  NOT NULL COMMENT 'when the writer recorded this'
)
CLUSTER BY (job_run_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

-- Two rows per run, opened and completed, never one row updated. An UPDATE would put
-- this schema outside append_only_check for ever, which is the property that makes the
-- audit worth reading. A run with no completed row did not finish.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.aud_load_run (
  job_run_id      STRING     NOT NULL COMMENT 'from {{job.run_id}}',
  phase           STRING     NOT NULL COMMENT 'opened | completed',
  target          STRING     NOT NULL COMMENT 'bundle target, e.g. usnc_tds',
  active_sources  STRING              COMMENT 'the declared activity list this run was given',
  recorded_at     TIMESTAMP  NOT NULL COMMENT 'when the phase was recorded'
)
CLUSTER BY (job_run_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 6: Prove the new checks can fail**

```bash
cp governance/control_objects.sql /tmp/co.bak
sed -i 's/`${control_schema}`.aud_table_load/control.aud_table_load/' governance/control_objects.sql
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "names no schema as a literal"
# Expected: FAIL
cp /tmp/co.bak governance/control_objects.sql
```

- [ ] **Step 7: Commit**

```bash
git add databricks.yml governance/control_objects.sql tests/test_accelerator.py
git commit -m "Declare the control schema, and stop control_objects.sql naming schemas as literals"
```

---

### Task 2: A task that actually runs `control_objects.sql`

**Files:**
- Create: `checks/apply_control_objects.py`
- Modify: `resources/vault_job.yml` (insert task after `assert_hash_parity`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `apply_governance.render(sql, bindings)` and `apply_governance.statements(sql)`.
- Produces: task key `create_control_objects`; CLI `--catalog --governance-schema --control-schema --target --active-sources --job-run-id [--dry-run]`.

- [ ] **Step 1: Write the failing test**

```python
_aco_spec = _ilu2.spec_from_file_location(
    "aco", ROOT / "checks" / "apply_control_objects.py")
_aco = _ilu2.module_from_spec(_aco_spec); _aco_spec.loader.exec_module(_aco)

# It must REUSE the renderer, not carry a second one. BUSINESS_KINDS and the
# system-column set were both duplicated in this repo and both drifted.
_aco_src = (ROOT / "checks" / "apply_control_objects.py").read_text(encoding="utf-8")
check("apply_control_objects imports the renderer rather than defining one",
      "from apply_governance import" in _aco_src or "import apply_governance" in _aco_src,
      "a second render()/statements() is the duplicate-definition trap this repo has hit twice")
check("it defines no render() of its own",
      "def render(" not in _aco_src, _aco_src[:160])
check("create_control_objects runs SECOND, behind gate zero and before everything else",
      _tasks["create_control_objects"]["depends_on"] == [{"task_key": "assert_hash_parity"}],
      str(_tasks.get("create_control_objects", {}).get("depends_on")))
check("create_mask_functions now waits on it, so the schema exists before any writer",
      {"task_key": "create_control_objects"}
      in _tasks["create_mask_functions"]["depends_on"],
      str(_tasks["create_mask_functions"]["depends_on"]))
check("it is handed the resolved job run id",
      "{{job.run_id}}" in _tasks["create_control_objects"]["spark_python_task"]["parameters"],
      "the whole audit keys on it")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "create_control_objects|apply_control_objects"`
Expected: FAIL — the file and the task do not exist (the module load raises, so create the file with a stub first if the suite aborts).

- [ ] **Step 3: Write the script**

```python
"""Create the control schema and its tables, and open the run's audit record.

Nothing ran governance/control_objects.sql before this. apply_governance.py reads only
apply_masks.sql, and no job task referenced the control file at all -- the two control
tables existed because someone applied it by hand once. Four gates are now pointed at this
schema, so the step that creates it cannot be manual.

WHY THIS TASK WRITES THE 'opened' ROW. assert_hash_parity is the first task, but it runs
BEFORE this one, so at that point the control schema may not exist. Gate zero also has no
business writing anything: it proves digests and stops. The task that GUARANTEES the target
exists is the first that can write to it, so it does.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import audit  # noqa: E402
from apply_governance import render, statements  # noqa: E402

GATE = "create_control_objects"
SQL_FILE = Path(__file__).resolve().parents[1] / "governance" / "control_objects.sql"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--governance-schema", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--active-sources", default="")
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    stmts = statements(render(SQL_FILE.read_text(encoding="utf-8"), {
        "catalog": args.catalog,
        "governance_schema": args.governance_schema,
        "control_schema": args.control_schema,
    }))

    if args.dry_run:
        for i, s in enumerate(stmts, 1):
            print(f"  {i:2d}. {s.splitlines()[0][:88]}")
        print(f"{len(stmts)} statement(s) rendered -- nothing executed")
        return 0

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for s in stmts:
        spark.sql(s)
    print(f"{len(stmts)} control statement(s) applied")

    # The run is opened only after the schema exists. If this write fails the task fails,
    # so a run with no 'opened' row never loaded anything.
    spark.sql(audit.load_run_sql(
        args.catalog, args.control_schema, job_run_id=args.job_run_id,
        phase="opened", target=args.target, active_sources=args.active_sources))
    print(f"run {args.job_run_id} opened")
    return finish("PASSED", len(stmts) + 1, 0, 0)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add the task to `resources/vault_job.yml`**

Insert immediately after the `assert_hash_parity` task, and add
`{task_key: create_control_objects}` to `create_mask_functions`'s `depends_on`:

```yaml
        # 0b ------------------------------------- PREREQUISITE: the control objects
        # Nothing ran governance/control_objects.sql before this task existed -- the two
        # control tables were applied by hand once. Four gates now depend on this schema,
        # so its creation cannot be a manual step.
        #
        # It also writes aud_load_run's 'opened' row. NOT assert_hash_parity: that runs
        # before this task, when the schema may not exist, and gate zero has no business
        # writing anything. The task that guarantees the target exists is the first that
        # can write to it.
        - task_key: create_control_objects
          depends_on: [{task_key: assert_hash_parity}]
          spark_python_task:
            python_file: ../checks/apply_control_objects.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--governance-schema"
              - "${var.governance_schema}"
              - "--control-schema"
              - "${var.control_schema}"
              - "--target"
              - "${bundle.target}"
              - "--active-sources"
              - "${var.active_sources}"
              - "--job-run-id"
              - "{{job.run_id}}"
          environment_key: checks
```

- [ ] **Step 5: Verify the dry run renders**

Run: `uv run --frozen python checks/apply_control_objects.py --catalog c --governance-schema governance --control-schema control --target usnc_tds --job-run-id 1 --dry-run`
Expected: 7 statements listed (2 `CREATE SCHEMA`, 5 `CREATE TABLE`), no unresolved placeholder error.

- [ ] **Step 6: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass.

- [ ] **Step 7: Prove the ordering check can fail**

```bash
cp resources/vault_job.yml /tmp/vj.bak
python3 - <<'EOF'
from pathlib import Path
p = Path("resources/vault_job.yml"); t = p.read_text()
p.write_text(t.replace(
  "        - task_key: create_control_objects\n          depends_on: [{task_key: assert_hash_parity}]",
  "        - task_key: create_control_objects\n          depends_on: [{task_key: raw_vault}]"))
EOF
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "runs SECOND"
# Expected: FAIL
cp /tmp/vj.bak resources/vault_job.yml
```

- [ ] **Step 8: Commit**

```bash
git add checks/apply_control_objects.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Run control_objects.sql from a task, and open the run's audit record there"
```

---

### Task 3: The audit writer — one definition

**Files:**
- Create: `src/accelerator/audit.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing.
- Produces, all returning a single SQL string and none touching Spark:
  - `audit.load_run_sql(catalog, control_schema, *, job_run_id, phase, target, active_sources) -> str`
  - `audit.table_load_sql(catalog, control_schema, *, job_run_id, pipeline_update_id, table_name, written_by, staged, accepted) -> str`
  - `audit.table_discard_sql(catalog, control_schema, *, job_run_id, table_name, reason, discarded) -> str`
  - `audit.check_arithmetic(staged, accepted, discards) -> str | None` where `discards` is `dict[str, int]`; returns a problem string or `None`.

- [ ] **Step 1: Write the failing test**

```python
from accelerator import audit as _audit  # add near the other accelerator imports

# The audit writer refuses a value it cannot safely put in a literal, and refuses
# arithmetic that does not balance -- both AT THE WRITER, so a bad row is never written
# and the gate is not the first thing to notice.
check("a run row renders with the phase and the run id in it",
      "opened" in _audit.load_run_sql("c", "control", job_run_id="42",
                                      phase="opened", target="usnc_tds",
                                      active_sources="GP_US")
      and "'42'" in _audit.load_run_sql("c", "control", job_run_id="42",
                                        phase="opened", target="usnc_tds",
                                        active_sources="GP_US"),
      "the row must carry the run it belongs to")
check("a quote in a value is REFUSED, not escaped",
      _audit_raises(lambda: _audit.load_run_sql(
          "c", "control", job_run_id="4'2", phase="opened", target="t",
          active_sources="")),
      "a value that can close a literal must be rejected, never quoted through. "
      "_refusal_message is NOT used here: it catches spec.SpecError only, and audit "
      "raises ValueError, so it would always return '' and the check would be vacuous")
check("balanced arithmetic passes",
      _audit.check_arithmetic(100, 60, {"already_present": 30,
                                        "duplicate_in_batch": 10}) is None,
      "100 - 60 == 40 == 30 + 10")
check("unbalanced arithmetic is a problem, and the message shows the sums",
      "40" in (_audit.check_arithmetic(100, 60, {"already_present": 30}) or ""),
      "a discard the writer failed to attribute must not be writable")
check("accepted above staged is refused",
      _audit.check_arithmetic(10, 11, {}) is not None,
      "a writer cannot insert more rows than it read")
```

Add this helper next to `_refusal_message` at the top of the suite:

```python
def _audit_raises(fn) -> bool:
    """True if fn raises anything -- for asserting a guard rejects rather than escapes."""
    try:
        fn()
    except Exception:  # noqa: BLE001
        return True
    return False
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "audit|arithmetic"`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the module**

```python
"""Writing the load audit. ONE definition, used by every writer.

Each writer records what IT did. The rejected alternative was a single check that reads
the event log, the staging logs, the vault and the quarantine twins afterwards and
computes everything -- it would need no loader changes, but it would RECONSTRUCT the
anti-join and hashdiff arithmetic rather than observe it, and a disagreement could then
not be attributed to the load or to the audit. That is the ambiguity loop-1 exists to
remove.

The arithmetic is checked HERE, before anything is written, so an inconsistent row cannot
reach the table and the gate is not the first thing to notice.
"""

from __future__ import annotations


def _lit(value: str, field: str) -> str:
    """A SQL string literal, refusing anything that could close it.

    Refuses rather than escapes. These values come from job parameters and bundle
    variables, so a quote in one is a configuration error worth failing on, not something
    to quietly repair -- the same stance apply_governance.render() takes on identifiers.
    """
    text = "" if value is None else str(value)
    if "'" in text or "\\" in text or "\x00" in text:
        raise ValueError(
            f"audit field {field}={text!r} contains a quote, backslash or NUL and cannot "
            f"be written as a SQL literal. Refusing to escape it -- fix the value."
        )
    return f"'{text}'"


def _num(value: int, field: str) -> str:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"audit field {field}={value!r} must be an int")
    if value < 0:
        raise ValueError(f"audit field {field}={value} must not be negative")
    return str(value)


def _q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def check_arithmetic(staged: int, accepted: int, discards: dict) -> str | None:
    """Why this writer's numbers do not balance, or None if they do.

    staged - accepted must equal the sum of the attributed discards. A discard the writer
    failed to attribute then shows up as an arithmetic gap rather than as nothing at all,
    which is what checks/audit_completeness_check.py asserts across the whole run.
    """
    if accepted > staged:
        return (f"accepted={accepted} exceeds staged={staged}: a writer cannot insert "
                f"more rows than it read")
    total = sum(discards.values())
    if staged - accepted != total:
        return (f"staged - accepted = {staged - accepted} but the attributed discards "
                f"sum to {total} ({dict(sorted(discards.items()))}): "
                f"{staged - accepted - total} row(s) unaccounted for")
    return None


def load_run_sql(catalog: str, control_schema: str, *, job_run_id: str, phase: str,
                 target: str, active_sources: str) -> str:
    if phase not in ("opened", "completed"):
        raise ValueError(f"phase={phase!r} must be 'opened' or 'completed'")
    return (
        f"INSERT INTO {_q(catalog, control_schema, 'aud_load_run')} "
        f"(job_run_id, phase, target, active_sources, recorded_at) VALUES ("
        f"{_lit(job_run_id, 'job_run_id')}, {_lit(phase, 'phase')}, "
        f"{_lit(target, 'target')}, {_lit(active_sources, 'active_sources')}, "
        f"current_timestamp())"
    )


def table_load_sql(catalog: str, control_schema: str, *, job_run_id: str,
                   pipeline_update_id: str | None, table_name: str, written_by: str,
                   staged: int, accepted: int) -> str:
    update = ("NULL" if pipeline_update_id is None
              else _lit(pipeline_update_id, "pipeline_update_id"))
    return (
        f"INSERT INTO {_q(catalog, control_schema, 'aud_table_load')} "
        f"(job_run_id, pipeline_update_id, table_name, written_by, staged, accepted, "
        f"recorded_at) VALUES ("
        f"{_lit(job_run_id, 'job_run_id')}, {update}, "
        f"{_lit(table_name, 'table_name')}, {_lit(written_by, 'written_by')}, "
        f"{_num(staged, 'staged')}, {_num(accepted, 'accepted')}, current_timestamp())"
    )


def table_discard_sql(catalog: str, control_schema: str, *, job_run_id: str,
                      table_name: str, reason: str, discarded: int) -> str:
    return (
        f"INSERT INTO {_q(catalog, control_schema, 'aud_table_discard')} "
        f"(job_run_id, table_name, discard_reason, discarded, recorded_at) VALUES ("
        f"{_lit(job_run_id, 'job_run_id')}, {_lit(table_name, 'table_name')}, "
        f"{_lit(reason, 'reason')}, {_num(discarded, 'discarded')}, current_timestamp())"
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Prove the guards can fail**

```bash
cp src/accelerator/audit.py /tmp/aud.bak
sed -i 's/    if staged - accepted != total:/    if False:/' src/accelerator/audit.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "unbalanced arithmetic"
# Expected: FAIL
cp /tmp/aud.bak src/accelerator/audit.py
sed -i "s/    if \"'\" in text or/    if False and/" src/accelerator/audit.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "quote in a value"
# Expected: FAIL
cp /tmp/aud.bak src/accelerator/audit.py
```

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/audit.py tests/test_accelerator.py
git commit -m "Add the audit writer, with the arithmetic checked before anything is written"
```

---

### Task 4: Instrument the hub loader

**Files:**
- Modify: `checks/load_hubs.py` (CLI ~line 134; execution loop ~lines 168-196)
- Modify: `resources/vault_job.yml` (`load_hubs` task parameters)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `audit.table_load_sql`, `audit.table_discard_sql`, `audit.check_arithmetic`.
- Produces: two discard reasons, spelled exactly `duplicate_in_batch` and `already_present`.

**Why two reasons, and the arithmetic that ties them together.** The hub INSERT discards on
two independent grounds: `row_number() OVER (PARTITION BY hk ...) = 1` drops duplicates
*within* the batch, and `NOT EXISTS` drops keys the hub *already holds*. Both are correct by
design and they are different facts, so they are recorded separately:

```
staged             = count(*)          FROM stg_hub_x
distinct_in_batch  = count(DISTINCT hk) FROM stg_hub_x
accepted           = count(hub) after - count(hub) before
duplicate_in_batch = staged            - distinct_in_batch
already_present    = distinct_in_batch - accepted
```

which satisfies `staged - accepted == duplicate_in_batch + already_present` by construction.

- [ ] **Step 1: Write the failing test**

```python
_lh_src = (ROOT / "checks" / "load_hubs.py").read_text(encoding="utf-8")
check("the hub loader records its audit through the shared writer",
      "audit.table_load_sql(" in _lh_src and "audit.table_discard_sql(" in _lh_src,
      "a second INSERT rendered by hand here is a second definition of the audit row")
# Anchor on `spark.sql(insert_sql(`, not `insert_sql(args`: the latter also matches the
# dry-run block, which sits EARLIER in the file, so the ordering check would fail on
# correct code. Found in self-review of this plan.
check("it counts the hub BEFORE inserting, or accepted cannot be known",
      "hub_before" in _lh_src
      and -1 < _lh_src.find("hub_before") < _lh_src.find("spark.sql(insert_sql("),
      "count(hub) after the insert is the hub's running total, not this run's contribution")
check("it attributes BOTH hub discard reasons",
      "duplicate_in_batch" in _lh_src and "already_present" in _lh_src,
      "row_number() and NOT EXISTS discard on different grounds")
check("it checks the arithmetic before writing",
      "check_arithmetic(" in _lh_src, "an unbalanced row must not reach the table")
check("an audit failure fails the task",
      "audit_failed" in _lh_src or "raise" in _lh_src.split("def main")[1],
      "an unaudited load is a failed load -- spec section 5")
check("the load_hubs task is handed the run id and the control schema",
      "{{job.run_id}}" in _tasks["load_hubs"]["spark_python_task"]["parameters"]
      and "${var.control_schema}" in _tasks["load_hubs"]["spark_python_task"]["parameters"],
      str(_tasks["load_hubs"]["spark_python_task"]["parameters"]))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "hub loader|hub discard|arithmetic before"`
Expected: FAIL on all six.

- [ ] **Step 3: Add the CLI arguments**

In `checks/load_hubs.py`, after `ap.add_argument("--schema", default="raw_vault")`:

```python
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the load audit. Required, not defaulted: a "
                         "default would let the audit silently go somewhere else.")
    ap.add_argument("--job-run-id", required=True,
                    help="{{job.run_id}}, the key tying one run's audit rows together")
```

Add the import beside the existing one:

```python
from accelerator import audit, naming, spec  # noqa: E402
```

- [ ] **Step 4: Record the numbers in the execution loop**

Replace the body of the `try:` block that currently runs `create_sql` / `insert_sql` and
prints, with:

```python
        try:
            spark.sql(create_sql(args.catalog, args.schema, hub, log))
            # AFTER create (so the table exists) and BEFORE insert: count(hub) taken after
            # the insert is the hub's running total, not what this run contributed.
            hub_before = spark.sql(
                f"SELECT count(*) AS n FROM {q(args.catalog, args.schema, hub)}"
            ).collect()[0]["n"]
            distinct_in_batch = spark.sql(
                f"SELECT count(DISTINCT `{e.hk_column}`) AS d "
                f"FROM {q(args.catalog, args.schema, log)}"
            ).collect()[0]["d"]

            spark.sql(insert_sql(args.catalog, args.schema, hub, log, e.hk_column))

            rows = spark.sql(
                f"SELECT count(*) AS n, count(DISTINCT `{e.hk_column}`) AS d "
                f"FROM {q(args.catalog, args.schema, hub)}"
            ).collect()[0]
            n, d = rows["n"], rows["d"]
            accepted = n - hub_before
            discards = {
                "duplicate_in_batch": before - distinct_in_batch,
                "already_present": distinct_in_batch - accepted,
            }
            problem = audit.check_arithmetic(before, accepted, discards)
            if problem:
                raise ValueError(f"{hub}: audit arithmetic does not balance -- {problem}")
            spark.sql(audit.table_load_sql(
                args.catalog, args.control_schema, job_run_id=args.job_run_id,
                pipeline_update_id=None, table_name=hub, written_by="checks/load_hubs.py",
                staged=before, accepted=accepted))
            for reason, count in sorted(discards.items()):
                if count:
                    spark.sql(audit.table_discard_sql(
                        args.catalog, args.control_schema,
                        job_run_id=args.job_run_id, table_name=hub,
                        reason=reason, discarded=count))

            state = "ok  " if n == d else "DUPE"
            print(f"  {state} {hub:34} log={before:>9}  hub={n:>9} rows / {d:>9} keys "
                  f"| +{accepted} accepted, {sum(discards.values())} discarded")
            if n != d:
                failed.append(f"{hub}: {n - d} duplicate key(s) AFTER the anti-join")
            loaded += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {hub}: {exc}")
            failed.append(hub)
```

The audit write is inside the same `try`, so a failed audit lands in `failed` and the task
exits 1 — an unaudited load is a failed load. Retry is safe: `NOT EXISTS` means the second
run inserts nothing and records `accepted = 0`.

- [ ] **Step 5: Add the task parameters**

In `resources/vault_job.yml`, append to `load_hubs`'s `parameters`:

```yaml
              - "--control-schema"
              - "${var.control_schema}"
              - "--job-run-id"
              - "{{job.run_id}}"
```

- [ ] **Step 6: Verify the dry run still renders and both suites pass**

Run: `uv run --frozen python checks/load_hubs.py --catalog c --control-schema control --job-run-id 1 --dry-run | tail -3 && uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: dry run lists 6 hubs; both suites pass.

- [ ] **Step 7: Prove the arithmetic guard can fail**

```bash
cp checks/load_hubs.py /tmp/lh.bak
sed -i 's/"already_present": distinct_in_batch - accepted,//' checks/load_hubs.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "BOTH hub discard reasons"
# Expected: FAIL
cp /tmp/lh.bak checks/load_hubs.py
```

- [ ] **Step 8: Commit**

```bash
git add checks/load_hubs.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Record what the hub loader accepted and discarded, and why"
```

---

### Task 5: Instrument the satellite loader

**Files:**
- Modify: `checks/load_satellites.py` (CLI, and the execution loop ~lines 434-455)
- Modify: `resources/vault_job.yml` (both `load_satellites` and `load_satellites_business`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `audit.table_load_sql`, `audit.table_discard_sql`, `audit.check_arithmetic`.
- Produces: one discard reason, spelled exactly `unchanged_hashdiff`.

**One reason, not two, deliberately.** The satellite INSERT discards via `LAG(hashdiff)`
(consecutive duplicates inside the batch) and via `NOT EXISTS` (rows already loaded). Unlike
the hub's two grounds these are the *same* fact — the row's payload has not changed since the
version before it — and splitting them would require a second pass over the log to attribute
each row to one mechanism. `staged - accepted` is recorded whole under `unchanged_hashdiff`.

- [ ] **Step 1: Write the failing test**

```python
_ls_src2 = (ROOT / "checks" / "load_satellites.py").read_text(encoding="utf-8")
check("the satellite loader records its audit through the shared writer",
      "audit.table_load_sql(" in _ls_src2, "one definition of the audit row")
check("it counts the satellite BEFORE inserting",
      "sat_before" in _ls_src2
      and _ls_src2.find("sat_before") < _ls_src2.find("spark.sql(v1_sql"),
      "count(sat) after the insert is the total, not this run's versions")
check("it attributes discards to unchanged_hashdiff",
      "unchanged_hashdiff" in _ls_src2,
      "LAG and NOT EXISTS discard the same fact: the payload has not changed")
check("it checks the arithmetic before writing",
      "check_arithmetic(" in _ls_src2, "an unbalanced row must not reach the table")
for _t in ("load_satellites", "load_satellites_business"):
    check(f"{_t} is handed the run id and the control schema",
          "{{job.run_id}}" in _tasks[_t]["spark_python_task"]["parameters"]
          and "${var.control_schema}" in _tasks[_t]["spark_python_task"]["parameters"],
          str(_tasks[_t]["spark_python_task"]["parameters"]))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "satellite loader|unchanged_hashdiff|load_satellites"`
Expected: FAIL.

- [ ] **Step 3: Add the CLI arguments and import**

After `ap.add_argument("--layer", ...)` in `checks/load_satellites.py`:

```python
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the load audit. Required, not defaulted.")
    ap.add_argument("--job-run-id", required=True,
                    help="{{job.run_id}}, the key tying one run's audit rows together")
```

Change the accelerator import to `from accelerator import audit, naming, spec  # noqa: E402`.

- [ ] **Step 4: Record the numbers**

Replace the `try:` body in the execution loop with:

```python
        try:
            spark.sql(stmts[0])                      # create_sql
            sat_before = spark.sql(
                f"SELECT count(*) AS n FROM {q(args.catalog, schema, sat)}"
            ).collect()[0]["n"]
            spark.sql(stmts[1])                      # insert_sql
            # DEF-53: the view comes after the load, because it reads the table the two
            # statements above create and populate.
            spark.sql(v1_sql(args.catalog, schema, sat, parent_hk,
                             entity.kind == "msat"))
            n = spark.sql(f"SELECT count(*) AS n FROM {q(args.catalog, schema, sat)}"
                          ).collect()[0]["n"]
            m = spark.sql(f"SELECT count(*) AS n FROM {q(args.catalog, schema, log)}"
                          ).collect()[0]["n"]
            accepted = n - sat_before
            discards = {"unchanged_hashdiff": m - accepted}
            problem = audit.check_arithmetic(m, accepted, discards)
            if problem:
                raise ValueError(f"{sat}: audit arithmetic does not balance -- {problem}")
            spark.sql(audit.table_load_sql(
                args.catalog, args.control_schema, job_run_id=args.job_run_id,
                pipeline_update_id=None, table_name=sat,
                written_by="checks/load_satellites.py", staged=m, accepted=accepted))
            if discards["unchanged_hashdiff"]:
                spark.sql(audit.table_discard_sql(
                    args.catalog, args.control_schema, job_run_id=args.job_run_id,
                    table_name=sat, reason="unchanged_hashdiff",
                    discarded=discards["unchanged_hashdiff"]))
            print(f"  ok   {sat:44} log={m:>9}  sat={n:>9} "
                  f"| +{accepted} versions, {discards['unchanged_hashdiff']} unchanged")
            loaded += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL {sat}: {exc}")
            failed.append(sat)
```

- [ ] **Step 5: Add the parameters to BOTH satellite tasks**

Append to the `parameters` of `load_satellites` **and** `load_satellites_business`:

```yaml
              - "--control-schema"
              - "${var.control_schema}"
              - "--job-run-id"
              - "{{job.run_id}}"
```

- [ ] **Step 6: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass.

- [ ] **Step 7: Prove it can fail**

```bash
cp resources/vault_job.yml /tmp/vj.bak
python3 -c "
from pathlib import Path
p=Path('resources/vault_job.yml'); t=p.read_text()
p.write_text(t.replace('              - \"--job-run-id\"\n              - \"{{job.run_id}}\"\n','',1))"
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "is handed the run id"
# Expected: at least one FAIL
cp /tmp/vj.bak resources/vault_job.yml
```

- [ ] **Step 8: Commit**

```bash
git add checks/load_satellites.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Record what the satellite loader accepted and discarded"
```

---

### Task 6: Close the run

**Files:**
- Modify: `checks/publish_metadata.py`
- Modify: `resources/vault_job.yml` (`publish_model_metadata` parameters)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `audit.load_run_sql`.
- Produces: the `completed` row for the run.

- [ ] **Step 1: Write the failing test**

```python
_pm_src = (ROOT / "checks" / "publish_metadata.py").read_text(encoding="utf-8")
check("the last task closes the run",
      'phase="completed"' in _pm_src, "a run with no completed row did not finish")
check("and it is the LAST task, so nothing loads after the run is closed",
      not [k for k, v in _tasks.items()
           if any(d["task_key"] == "publish_model_metadata"
                  for d in v.get("depends_on", []))],
      "another task depending on it would run after the run was declared complete")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep "closes the run"`
Expected: FAIL.

- [ ] **Step 3: Add the arguments and the write**

Add to `publish_metadata.py`'s argument parser:

```python
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--active-sources", default="")
```

Add `from accelerator import audit` alongside the existing accelerator import, and at the
very end of `main()`, after the metadata publish succeeds and before the return:

```python
    # The run is closed only once everything before it has succeeded. If this write fails
    # the task fails, so a run with no 'completed' row is a run that did not finish --
    # which is what checks/audit_completeness_check.py asserts.
    spark.sql(audit.load_run_sql(
        args.catalog, args.control_schema, job_run_id=args.job_run_id,
        phase="completed", target=args.target, active_sources=args.active_sources))
    print(f"run {args.job_run_id} completed")
```

- [ ] **Step 4: Add the parameters**

Replace `publish_model_metadata`'s `parameters` line in `resources/vault_job.yml` with:

```yaml
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--schema"
              - "${var.governance_schema}"
              - "--control-schema"
              - "${var.control_schema}"
              - "--job-run-id"
              - "{{job.run_id}}"
              - "--target"
              - "${bundle.target}"
              - "--active-sources"
              - "${var.active_sources}"
```

- [ ] **Step 5: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add checks/publish_metadata.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Close the run's audit record in the last task"
```

---

### Task 7: Bring `control` inside the existing gates

**Files:**
- Modify: `resources/vault_job.yml` (`assert_append_only`, `assert_no_broad_grant`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing. Both checks already accept repeatable `--schema`; no code changes.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

```python
for _gate in ("assert_append_only", "assert_no_broad_grant"):
    _params = _tasks[_gate]["spark_python_task"]["parameters"]
    check(f"{_gate} covers the control schema",
          "${var.control_schema}" in _params,
          f"an audit that can be rewritten is not an audit, and nobody reads control "
          f"directly either -- {_params}")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep "covers the control schema"`
Expected: FAIL for both.

- [ ] **Step 3: Add the parameter to both tasks**

Append to each task's `parameters`:

```yaml
              - "--schema"
              - "${var.control_schema}"
```

- [ ] **Step 4: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add resources/vault_job.yml tests/test_accelerator.py
git commit -m "Point append-only and the grant sweep at the control schema"
```

---

### Task 8: The orphan-schema assertion

**Files:**
- Modify: `checks/schema_grant_check.py`
- Modify: `resources/vault_job.yml` (`assert_no_broad_grant` parameters)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `schema_grant_check.undeclared_schemas(rows, declared) -> list[str]`, pure and
  Spark-free, where `rows` is an iterable of `(schema_name, owner)`.
- New CLI flag: `--declared-schema` (repeatable).

**Why owner and not a name list.** The check must tolerate `information_schema`, which is
always present and system-owned. The naive form is "declared set plus an exclusion list" —
and the exclusion list is exactly where the next orphan hides, because a name the check has
been taught to ignore is indistinguishable from one it should have caught. So system schemas
are identified by **owner**: `information_schema` is owned by `System user`, and everything
this repo creates is owned by the deploying principal. `silver_vault` was undeclared and
owned by a real user, which is precisely the shape this must catch.

- [ ] **Step 1: Write the failing test**

```python
_DECLARED = ["raw_vault", "business_vault", "governance", "control"]
check("a declared schema is not a finding",
      _sg.undeclared_schemas([("raw_vault", "adrian.turcu@headfirst.group")],
                             _DECLARED) == [], "it is declared")
check("information_schema is not a finding, because of its OWNER not its name",
      _sg.undeclared_schemas([("information_schema", "System user")],
                             _DECLARED) == [],
      "system-owned schemas are excluded by owner, so no name list can be gamed")
check("an undeclared, user-owned schema IS a finding -- the silver_vault shape",
      len(_sg.undeclared_schemas(
          [("silver_vault", "adrian.turcu@headfirst.group")], _DECLARED)) == 1,
      "this is the one real example this check was written from")
check("and the finding names the schema and says what to do",
      "silver_vault" in (_sg.undeclared_schemas(
          [("silver_vault", "adrian.turcu@headfirst.group")], _DECLARED) or [""])[0],
      "a finding that does not name the object cannot be acted on")
check("an undeclared schema with a SYSTEM-LOOKING name is still caught",
      len(_sg.undeclared_schemas(
          [("information_schema_backup", "adrian.turcu@headfirst.group")],
          _DECLARED)) == 1,
      "the exclusion must be the owner, never a name prefix")
check("assert_no_broad_grant declares the four schemas",
      _tasks["assert_no_broad_grant"]["spark_python_task"]["parameters"].count(
          "--declared-schema") == 4,
      str(_tasks["assert_no_broad_grant"]["spark_python_task"]["parameters"]))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "declared schema|silver_vault shape|information_schema"`
Expected: FAIL — `undeclared_schemas` does not exist.

- [ ] **Step 3: Write the predicate**

Add to `checks/schema_grant_check.py`, immediately above `def table_privileges(`:

```python
SYSTEM_OWNERS = ("system user",)


def undeclared_schemas(rows, declared) -> list[str]:
    """Problems for every schema in the catalog that this repo does not declare.

    silver_vault is why this exists. It was the vault's schema name before the raw/business
    split, and verify_repo.py already asserted "silver_vault survives nowhere in the
    bundle" -- which PASSED, correctly: the name was gone from the CONFIG. The schema was
    still in the lake, holding a probe table, and no gate looked at it because
    schema_grant_check was passed raw_vault and business_vault only. A config-level
    assertion cannot see lake state.

    SYSTEM SCHEMAS ARE EXCLUDED BY OWNER, NOT BY NAME. information_schema is always present
    and is owned by `System user`; everything this repo creates is owned by the deploying
    principal. A name-based exclusion list would be the place the next orphan hides,
    because a name the check has been taught to ignore is indistinguishable from one it
    should have caught.

    Pure and Spark-free, like offending() and unauthorised_table_readers(), so the failing
    case can be fired offline in both directions.
    """
    declared_lower = {d.lower() for d in declared}
    problems = []
    for schema, owner in sorted(rows):
        if schema.lower() in declared_lower:
            continue
        if (owner or "").strip().lower() in SYSTEM_OWNERS:
            continue
        problems.append(
            f"SCHEMA {schema}: undeclared, and owned by `{owner}` rather than the system. "
            f"This repo declares {sorted(declared_lower)}. An undeclared schema is outside "
            f"every gate pointed at the declared ones -- no append-only check, no grant "
            f"sweep, no mask survival. Either declare it as a bundle variable or drop it."
        )
    return problems
```

- [ ] **Step 4: Add the flag and the sweep**

Add the argument beside `--allow-table-select`:

```python
    ap.add_argument("--declared-schema", action="append", default=None,
                    metavar="SCHEMA",
                    help="a schema this repo declares; repeatable. Any other non-system "
                         "schema in the catalog fails the gate. Omit to skip (reported).")
```

After the `for schema in schemas:` loop closes and before the
`# DEF-46: catalogs we depend on but do not govern.` comment:

```python
    if args.declared_schema:
        try:
            _rows = [(r.asDict()["schema_name"], r.asDict()["schema_owner"])
                     for r in spark.sql(
                         f"SELECT schema_name, schema_owner FROM "
                         f"system.information_schema.schemata "
                         f"WHERE catalog_name = '{args.catalog}'").collect()]
            problems += undeclared_schemas(_rows, args.declared_schema)
            asserted += 1
            print(f"  checked SCHEMAS in {args.catalog}: {len(_rows)} present, "
                  f"{len(args.declared_schema)} declared")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR listing schemas in {args.catalog}: {exc}")
            problems.append(f"SCHEMAS in {args.catalog}: could not be listed ({exc}). "
                            f"A run that cannot list them has asserted nothing.")
    else:
        not_evaluated += 1
        print(f"  NOT EVALUATED: undeclared schemas in {args.catalog} -- no "
              f"--declared-schema given")
```

- [ ] **Step 5: Add the parameters**

Append to `assert_no_broad_grant`'s `parameters`:

```yaml
              - "--declared-schema"
              - "${var.vault_schema}"
              - "--declared-schema"
              - "${var.business_vault_schema}"
              - "--declared-schema"
              - "${var.governance_schema}"
              - "--declared-schema"
              - "${var.control_schema}"
```

- [ ] **Step 6: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass.

- [ ] **Step 7: Prove it can fail**

```bash
cp checks/schema_grant_check.py /tmp/sg.bak
sed -i 's/        if (owner or "").strip().lower() in SYSTEM_OWNERS:/        if True:/' checks/schema_grant_check.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "silver_vault shape|SYSTEM-LOOKING"
# Expected: both FAIL
cp /tmp/sg.bak checks/schema_grant_check.py
sed -i 's/SYSTEM_OWNERS = ("system user",)/SYSTEM_OWNERS = ("system user", "information_schema")/' checks/schema_grant_check.py
uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3   # must still pass: owner, not name
cp /tmp/sg.bak checks/schema_grant_check.py
```

- [ ] **Step 8: Commit**

```bash
git add checks/schema_grant_check.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Fail the build on an undeclared schema, keyed on owner not name"
```

---

### Task 9: The audit-completeness gate

**Files:**
- Create: `checks/audit_completeness_check.py`
- Modify: `resources/vault_job.yml` (new task after `assert_mask_survival`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `unclosed_runs(run_rows, load_rows) -> list[str]` where `run_rows` is
    `(job_run_id, phase)` and `load_rows` is `(job_run_id, table_name)`.
  - `unbalanced_tables(load_rows, discard_rows) -> list[str]` where `load_rows` is
    `(job_run_id, table_name, staged, accepted)` and `discard_rows` is
    `(job_run_id, table_name, discarded)`.

- [ ] **Step 1: Write the failing test**

```python
_ac_spec = _ilu2.spec_from_file_location(
    "ac", ROOT / "checks" / "audit_completeness_check.py")
_ac = _ilu2.module_from_spec(_ac_spec); _ac_spec.loader.exec_module(_ac)

check("a run with an opened and a completed row is closed",
      _ac.unclosed_runs([("1", "opened"), ("1", "completed")], [("1", "hub_x")]) == [],
      "both phases present")
check("a run that wrote tables but never completed IS a finding",
      len(_ac.unclosed_runs([("1", "opened")], [("1", "hub_x")])) == 1,
      "no completed row means the run did not finish")
check("a run that never opened but wrote tables IS a finding",
      len(_ac.unclosed_runs([], [("1", "hub_x")])) == 1,
      "audit rows for a run that was never opened means the ordering broke")
check("balanced discards pass",
      _ac.unbalanced_tables([("1", "hub_x", 100, 60)],
                            [("1", "hub_x", 30), ("1", "hub_x", 10)]) == [],
      "100 - 60 == 40 == 30 + 10")
check("an unattributed discard IS a finding, and the message shows the gap",
      "10" in " ".join(_ac.unbalanced_tables([("1", "hub_x", 100, 60)],
                                             [("1", "hub_x", 30)])),
      "staged - accepted must equal SUM(discarded) or a discard went unrecorded")
check("a table with no discard rows and staged == accepted is balanced",
      _ac.unbalanced_tables([("1", "hub_x", 50, 50)], []) == [],
      "a writer that discarded nothing writes no discard rows, which is not a gap")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "unclosed|unattributed|balanced"`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the check**

```python
"""HARD GATE: every load run is closed, and every discard is attributed.

Two independent assertions over the control schema, both of which exist because the audit
is only worth what it cannot omit:

  1. A run that wrote aud_table_load rows must have BOTH an 'opened' and a 'completed' row
     in aud_load_run. An unaudited load is a failed load (spec section 5), so an unclosed
     run means a task died between the first writer and the last -- or that a writer wrote
     an audit row for a run nobody opened, which means the task ordering broke.

  2. staged - accepted must equal SUM(discarded) for every audited table. This is the half
     that makes an UNATTRIBUTED discard visible: a writer that quietly drops rows without
     recording a reason produces an arithmetic gap here rather than nothing at all.

Both predicates are pure and Spark-free so the failing case can be fired offline in both
directions -- this repo has shipped five checks that could never fail.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse

GATE = "audit_completeness"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def unclosed_runs(run_rows, load_rows) -> list[str]:
    """Runs that wrote audit rows without both phases recorded."""
    phases: dict[str, set] = {}
    for run_id, phase in run_rows:
        phases.setdefault(run_id, set()).add(phase)
    problems = []
    for run_id in sorted({r for r, _t in load_rows}):
        have = phases.get(run_id, set())
        missing = {"opened", "completed"} - have
        if missing:
            problems.append(
                f"run {run_id}: wrote aud_table_load rows but aud_load_run is missing "
                f"{sorted(missing)}. An unaudited load is a failed load -- either a task "
                f"died between the first writer and the last, or a writer ran before "
                f"create_control_objects opened the run."
            )
    return problems


def unbalanced_tables(load_rows, discard_rows) -> list[str]:
    """Tables where the attributed discards do not account for staged - accepted."""
    attributed: dict[tuple, int] = {}
    for run_id, table, discarded in discard_rows:
        attributed[(run_id, table)] = attributed.get((run_id, table), 0) + discarded
    problems = []
    for run_id, table, staged, accepted in sorted(load_rows):
        gap = (staged - accepted) - attributed.get((run_id, table), 0)
        if gap:
            problems.append(
                f"run {run_id}, {table}: staged={staged} accepted={accepted} leaves "
                f"{staged - accepted} discarded, but only "
                f"{attributed.get((run_id, table), 0)} row(s) are attributed to a reason "
                f"-- {gap} unaccounted for. A discard with no recorded reason is a silent "
                f"drop, which is what loop-1 exists to make impossible."
            )
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--job-run-id", default=None,
                    help="restrict to one run. Omit to assert over every run on record.")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    c, s = args.catalog, args.control_schema
    where = f" WHERE job_run_id = '{args.job_run_id}'" if args.job_run_id else ""

    runs = [(r["job_run_id"], r["phase"]) for r in spark.sql(
        f"SELECT job_run_id, phase FROM `{c}`.`{s}`.aud_load_run{where}").collect()]
    loads = [(r["job_run_id"], r["table_name"], r["staged"], r["accepted"])
             for r in spark.sql(
                 f"SELECT job_run_id, table_name, staged, accepted "
                 f"FROM `{c}`.`{s}`.aud_table_load{where}").collect()]
    discards = [(r["job_run_id"], r["table_name"], r["discarded"]) for r in spark.sql(
        f"SELECT job_run_id, table_name, discarded "
        f"FROM `{c}`.`{s}`.aud_table_discard{where}").collect()]

    problems = (unclosed_runs(runs, [(r, t) for r, t, _s, _a in loads])
                + unbalanced_tables(loads, discards))
    print(f"  {len(runs)} run phase row(s), {len(loads)} table row(s), "
          f"{len(discards)} discard row(s)")

    if problems:
        print(f"\nAUDIT COMPLETENESS GATE FAILED -- {len(problems)} problem(s):")
        for p in problems:
            print(f"  * {p}")
        return finish("FAILED", 2, 0, 1)
    if not loads:
        print("\nGATE NOT EVALUATED: no aud_table_load rows to assert over.")
        return finish("NOT_EVALUATED", 0, 1, 1)
    print(f"\nAUDIT COMPLETENESS GATE PASSED: every run closed, every discard attributed.")
    return finish("PASSED", 2, 0, 0)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add the task**

After `assert_mask_survival` in `resources/vault_job.yml`, and change
`publish_model_metadata`'s `depends_on` to `[{task_key: assert_audit_completeness}]`:

```yaml
        # HARD GATE: the audit omitted nothing.
        # Runs BEFORE publish_model_metadata, which writes the 'completed' row -- so this
        # asserts over the run's table rows while the run is still open, and the run is
        # declared complete only once the audit has been checked.
        - task_key: assert_audit_completeness
          depends_on: [{task_key: assert_mask_survival}]
          spark_python_task:
            python_file: ../checks/audit_completeness_check.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--control-schema"
              - "${var.control_schema}"
          environment_key: checks
```

**Note on ordering:** because this runs before the `completed` row is written,
`unclosed_runs` would fail on the current run every time if it were passed
`--job-run-id`. It is deliberately **not** passed one: it asserts over every run *on
record*, so the current run's `completed` row is written afterwards and checked by the next
run. `unbalanced_tables` covers the current run immediately.

- [ ] **Step 5: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass.

- [ ] **Step 6: Prove both predicates can fail**

```bash
cp checks/audit_completeness_check.py /tmp/ac.bak
sed -i 's/        if missing:/        if False:/' checks/audit_completeness_check.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "never completed|never opened"
# Expected: both FAIL
cp /tmp/ac.bak checks/audit_completeness_check.py
sed -i 's/        if gap:/        if False:/' checks/audit_completeness_check.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "unattributed discard"
# Expected: FAIL
cp /tmp/ac.bak checks/audit_completeness_check.py
```

- [ ] **Step 7: Commit**

```bash
git add checks/audit_completeness_check.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Gate that every load run is closed and every discard is attributed"
```

---

### Task 10: Deploy, and drop the old tables only after the new ones are proven

**Files:**
- No source changes. Live workspace only.

**Interfaces:**
- Consumes: everything above.
- Produces: the `control` schema populated in `02_usnc_silver_edm_dev`.

**The old tables are dropped LAST, and not by a script.** `governance.ctl_approval_manifest`
and `governance.ref_dq_expectation` both hold 0 rows, so nothing is migrated — but they are
dropped by hand after the new ones are confirmed present, so a failed deploy never leaves the
estate with neither.

- [ ] **Step 1: Preflight**

Run: `uv run --frozen python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds`
Expected: `PREFLIGHT PASSED`.

- [ ] **Step 2: Validate**

Run: `databricks bundle validate -t usnc_tds --profile hfig-usnc-tds`
Expected: exactly one warning, the DEF-43 world-writable bundle root. Any second warning stops here.

- [ ] **Step 3: Deploy**

Run: `databricks bundle deploy -t usnc_tds --profile hfig-usnc-tds`
Expected: `jobs.vault_load` updated.

- [ ] **Step 4: Run the control-objects task alone**

```bash
databricks jobs run-now --profile hfig-usnc-tds --no-wait \
  --json '{"job_id": 303337354608483, "only": ["create_control_objects"]}'
```

Poll with `databricks jobs get-run <run_id> --profile hfig-usnc-tds -o json` until
TERMINATED. Expected: SUCCESS, and the task log shows `7 control statement(s) applied`
followed by `run <id> opened`.

- [ ] **Step 5: Verify the schema and the opened row**

Run, via a SQL warehouse on `hfig-usnc-tds`:

```sql
SELECT table_name FROM system.information_schema.tables
WHERE table_catalog = '02_usnc_silver_edm_dev' AND table_schema = 'control'
ORDER BY table_name;

SELECT job_run_id, phase, target FROM `02_usnc_silver_edm_dev`.`control`.`aud_load_run`;
```

Expected: five tables; one `opened` row whose `job_run_id` equals the run id from Step 4.

- [ ] **Step 6: Confirm the orphan gate now sees `control` as declared**

```bash
databricks jobs run-now --profile hfig-usnc-tds --no-wait \
  --json '{"job_id": 303337354608483, "only": ["assert_no_broad_grant"]}'
```

Expected: PASSED, with `checked SCHEMAS in 02_usnc_silver_edm_dev: 5 present, 4 declared`
and no finding — `silver_vault` is already gone, so the four declared schemas plus
`information_schema` is the whole catalog.

- [ ] **Step 7: Drop the old tables**

Only after Step 5 confirmed the new ones exist:

```sql
DROP TABLE `02_usnc_silver_edm_dev`.`governance`.`ctl_approval_manifest`;
DROP TABLE `02_usnc_silver_edm_dev`.`governance`.`ref_dq_expectation`;
```

Then confirm `governance` holds only the four mask functions and the two pipeline event logs.

- [ ] **Step 8: Repoint `expectations_table` and the loop-1 default**

`databricks.yml` sets `expectations_table` to a full three-part name in **nine** targets;
each must move from `.governance.ref_dq_expectation` to `.control.ref_dq_expectation`.
`src/pipelines/silver_vault.py:56` also carries a stale `hfig_eu.governance.ref_dq_expectation`
default that must move. And `checks/loop1_reconciliation.py:156` defaults the manifest to
`{args.catalog}.governance.ctl_approval_manifest` — change `governance` to `control`.

```bash
uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3
uv run --frozen python verify_repo.py 2>&1 | tail -3
git add databricks.yml src/pipelines/silver_vault.py checks/loop1_reconciliation.py
git commit -m "Repoint the expectations table and the loop-1 manifest at the control schema"
```

- [ ] **Step 9: Redeploy and record**

Run steps 1-3 again, then add a section to `docs/superpowers/OPEN_ITEMS.md` recording: the
schema created, the five tables, the run id of the first `opened` row, the gate output from
Step 6, and that the two old tables were dropped after the new ones were confirmed. Commit.

---

## Notes for whoever executes this

**Two things this plan does not make work, by design.**

Every `qtn_*` quarantine twin holds 0 rows, so the *rejected*-row half of the picture has
never fired in production. Nothing here changes that. `staged - accepted` records
*legitimate* discards — hub dedup and satellite no-change — not quarantined rejects. When the
first real rejection happens, expect the loop-1 reconciliation and this audit to need
reconciling with each other; that is the second spec, not this one.

`ctl_approval_manifest` and `ref_dq_expectation` are still empty after all of this. The first
because populating it ourselves would make the vault self-certifying; the second because it
is a capability, and the generator falls back to its two compiled-in key-safety rules when it
is absent.

**The pipeline half of `aud_table_load` is not in this plan.** The spec's section 4 table
names a third writer — a check reading `num_output_rows` / `dropped_records` out of
`pipeline_event_log`. It is deliberately left out: only 162 of 2,759 `flow_progress` rows
carry metrics at all, so a gate asserting over them would be asserting over a sparse and
poorly understood source. Add it once the batch-loader half is running and its numbers are
trusted.
