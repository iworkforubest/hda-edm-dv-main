# usnc_tds Retarget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the accelerator's offline test suites runnable, then retarget the bundle from `weu_tds` to `usnc_tds` against the estate's three-catalog layout.

**Architecture:** Two sequential sub-projects from the spec. Sub-project 1 establishes a red-green loop that does not exist today (no dependency manifest, PyYAML absent, both suites fail at import). Sub-project 2 retargets `databricks.yml`, `governance/apply_masks.sql` and `checks/conformance_check.py`, replacing the single-catalog assumption with three catalog variables and moving the baseline off `weu_tds`. Every change is test-first against the repo's own `check()` harnesses.

**Tech Stack:** Python 3.14, uv, PyYAML, Databricks CLI (bundles), Unity Catalog SQL.

**Spec:** `docs/superpowers/specs/2026-08-24-usnc-tds-retarget-design.md`

## Global Constraints

- Hash rulebook stays at `1.0.0`. **Do not modify `src/accelerator/hashing.py`.** Any change to a RATIFIED constant requires a `RULEBOOK_VERSION` bump plus regenerated `tests/golden_hash_vectors.json` in the same reviewed commit — out of scope for this plan.
- `pyspark` is **not** a project dependency. It is supplied by the Databricks Runtime (spec D8).
- The test harnesses are **not pytest**. `tests/test_accelerator.py` and `verify_repo.py` are flat scripts using a local `check(label, condition, detail)` helper that prints `PASS`/`FAIL`, appends to a failure list, and `sys.exit(1)` at the end. Match that idiom exactly; do not introduce pytest.
- Catalog naming convention: `0N_<lake>_<layer>_<domain>_<env>`. PROD carries no `_dev` suffix.
- `weu_tds` is not used by this project. Do not reintroduce it as a default or baseline.
- No workspace is reachable during Tasks 1-5. Every gate in `checks/` that needs a live workspace stays outstanding and must be named in the commit message.

---

### Task 1: uv dependency manifest

Establishes the red-green loop. Nothing else in this plan is testable until this lands.

**Files:**
- Create: `pyproject.toml`
- Create: `uv.lock` (generated, committed)
- Modify: `.gitignore`
- Modify: `README.md:43-48` (the Getting-this-project block)

**Interfaces:**
- Consumes: nothing.
- Produces: `uv run python tests/test_accelerator.py` and `uv run python verify_repo.py` both execute. Every later task's test steps assume this.

- [ ] **Step 1: Confirm the failure you are fixing**

```bash
cd /mnt/projects/hda-edm-dv
python3 tests/test_accelerator.py
```

Expected: `ModuleNotFoundError: No module named 'yaml'` raised from `src/accelerator/spec.py:29`.

- [ ] **Step 2: Write pyproject.toml**

`pyspark` is intentionally absent — see Global Constraints.

```toml
[project]
name = "hfig-dv-accelerator"
version = "0.2.0"
description = "HFIG Data Vault Accelerator -- SDP-native, metadata-driven"
requires-python = ">=3.11"
dependencies = [
    "pyyaml>=6.0",
]

[project.optional-dependencies]
# Only checks/conformance_check.py needs this, and it imports lazily inside a
# function (conformance_check.py:42), so the offline suites never load it.
workspace = [
    "databricks-sdk>=0.30.0",
]

[tool.uv]
package = false
```

- [ ] **Step 3: Generate and verify the lockfile**

```bash
uv lock
uv run python -c "import yaml; print('pyyaml', yaml.__version__)"
```

Expected: prints a version. If `uv lock` fails, do not hand-write `uv.lock`.

- [ ] **Step 4: Run both suites — this is the green you were after**

```bash
uv run python tests/test_accelerator.py
uv run python verify_repo.py
```

Expected: `test_accelerator.py` prints `ALL CHECKS PASSED` and exits 0.

`verify_repo.py` may legitimately FAIL here — it checks repo integrity and the repo has known defects (DEF-4, DEF-5). **Record its exact findings list in the commit message.** Do not fix them in this task; Task 5 does. If it fails for a reason *not* in the spec's defect list, stop and report rather than proceeding.

- [ ] **Step 5: Ignore the venv**

Append to `.gitignore`:

```
.venv/
```

`uv.lock` is committed deliberately — it is what makes the four lakes reproducible from one commit.

- [ ] **Step 6: Update the README run instructions**

Replace the two commands in the Getting-this-project block:

```markdown
```bash
unzip hfig-dv-accelerator-v0.2.zip && cd hfig-dv-accelerator-v0.2
uv sync
uv run python verify_repo.py          # repo integrity; starts with a layout check
uv run python tests/test_accelerator.py
```
```

Note the check count is deliberately not stated here — Task 5 covers why.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock .gitignore README.md
git commit -m "Add uv dependency manifest so the offline suites can run

Neither verify_repo.py nor tests/test_accelerator.py could execute: PyYAML
was absent and the repo tracked no manifest. Both now run under uv.

pyspark is deliberately not a dependency -- it comes from the Databricks
Runtime, and installing it locally would invite running pipeline code
off-cluster against a different version. databricks-sdk is an optional
extra because its only consumer imports it lazily.

Outstanding gates: all. No workspace profile reaches usnc_tds."
```

---

### Task 2: Quote catalog identifiers in the governance SQL (DEF-1)

**Files:**
- Modify: `governance/apply_masks.sql:44,46,110-122`
- Modify: `verify_repo.py` (add a new numbered section before the final summary)

**Interfaces:**
- Consumes: Task 1's `uv run`.
- Produces: `apply_masks.sql` safe for any legal UC catalog name. Task 3 edits the same file and assumes quoting is already correct.

**Design note — this deviates slightly from the spec's phrasing.** The spec says to tighten the guard in `checks/apply_governance.py:38` so a leading digit cannot pass. On inspection that is the wrong repair: `02_usnc_silver_edm_dev` is a *legal* Unity Catalog name that must be usable, and the guard's `[A-Za-z0-9_]+` correctly does the job it exists for — rejecting backticks, semicolons and whitespace that could carry SQL. The actual defect is that the SQL interpolates unquoted. So: quote in the SQL, leave the guard alone, and prove a leading-digit name now round-trips.

- [ ] **Step 1: Write the failing checks**

Append to `verify_repo.py`, immediately before the `print("\n" + "=" * 70)` summary block at the end of the file:

```python
# --------------------------------------------------------------------------- #
# GOVERNANCE SQL: catalog identifiers must be quoted.
#
# The estate's catalogs start with a digit (02_usnc_silver_edm_dev). Spark SQL
# unquoted identifiers must match [a-zA-Z_][a-zA-Z0-9_]*, so an unquoted
# ${catalog} produces DDL that will not parse. naming.py:94 already backticks;
# this file did not.
# --------------------------------------------------------------------------- #
print("\n[governance] catalog identifiers are quoted in apply_masks.sql")

_masks_sql = (ROOT / "governance" / "apply_masks.sql").read_text()
# Skip comment lines: line 39 shows a `databricks workspace-bindings` shell
# example containing a bare ${catalog}, which is not SQL and must stay bare.
_sql_lines = [l for l in _masks_sql.splitlines() if not l.lstrip().startswith("--")]
_unquoted = re.findall(r"(?<!`)\$\{catalog\}(?!`)", "\n".join(_sql_lines))
check(
    "every ${catalog} in apply_masks.sql is backticked",
    not _unquoted,
    f"{len(_unquoted)} unquoted occurrence(s); a leading-digit catalog will not parse",
)

sys.path.insert(0, str(ROOT / "checks"))
import apply_governance as _ag  # noqa: E402

_rendered = _ag.render(
    "ALTER CATALOG `${catalog}` SET ISOLATION MODE ISOLATED;",
    {"catalog": "02_usnc_silver_edm_dev"},
)
check(
    "a leading-digit catalog renders as a quoted identifier",
    _rendered == "ALTER CATALOG `02_usnc_silver_edm_dev` SET ISOLATION MODE ISOLATED;",
    _rendered,
)

_injection_rejected = False
try:
    _ag.render("USE CATALOG `${catalog}`;", {"catalog": "x`; DROP TABLE y; --"})
except ValueError:
    _injection_rejected = True
check(
    "the identifier guard still refuses a value carrying SQL",
    _injection_rejected,
    "render() accepted a value containing a backtick and a semicolon",
)
```

- [ ] **Step 2: Run it and watch the first check fail**

```bash
uv run python verify_repo.py 2>&1 | grep -A2 'governance'
```

Expected: `FAIL  every ${catalog} in apply_masks.sql is backticked` reporting 14 unquoted occurrences. The other two checks should already PASS — they describe behaviour that is already correct.

- [ ] **Step 3: Quote every occurrence in the SQL**

```bash
sed -i 's/\${catalog}/`${catalog}`/g' governance/apply_masks.sql
```

That regex also rewrites the shell example inside the comment on line 39. Revert that one line by hand — it is a `databricks workspace-bindings` command, not SQL:

```bash
grep -n 'catalog' governance/apply_masks.sql
```

Expected after edit — note the schema-qualified forms need each part quoted separately:

```sql
ALTER CATALOG `${catalog}` SET ISOLATION MODE ISOLATED;
USE CATALOG `${catalog}`;
REVOKE ALL PRIVILEGES ON SCHEMA `${catalog}`.`${vault_schema}` FROM `account users`;
```

The check in Step 1 already ignores comment lines, so the reverted line 39 will not trip it.

- [ ] **Step 4: Run to verify all three pass**

```bash
uv run python verify_repo.py 2>&1 | grep -A4 'governance'
```

Expected: three `PASS` lines.

- [ ] **Step 5: Commit**

```bash
git add governance/apply_masks.sql verify_repo.py
git commit -m "Quote catalog identifiers in apply_masks.sql (DEF-1)

The estate's catalogs begin with a digit (02_usnc_silver_edm_dev), which
Spark SQL cannot parse unquoted. Every \${catalog} in apply_masks.sql was
interpolated bare, so the isolation statement and every grant would have
failed at execution.

The guard in apply_governance.py is left as-is, contrary to the spec's
phrasing: [A-Za-z0-9_]+ correctly rejects injection, and a leading digit
is a legal UC catalog name that must remain usable. Quoting belongs in the
SQL. verify_repo.py now asserts both.

Outstanding gate: mask_survival_check (needs a workspace)."
```

---

### Task 3: Three catalog variables (D1, DEF-2)

**Files:**
- Modify: `databricks.yml` (variables block; all eight target blocks)
- Modify: `governance/apply_masks.sql` (bronze and gold grant statements)
- Modify: `verify_repo.py` (extend the variable-declaration section)

**Interfaces:**
- Consumes: Task 2's quoting.
- Produces: bundle variables `bronze_catalog`, `catalog`, `gold_catalog`. `bronze_schema` and `gold_schema` no longer exist. Task 4 edits the same target blocks.

- [ ] **Step 1: Write the failing check**

Append to `verify_repo.py` before the summary block:

```python
# --------------------------------------------------------------------------- #
# CATALOG LAYOUT: the estate places each medallion layer in its own CATALOG,
# not in a schema of one shared catalog. bronze_schema cannot survive -- there
# is a schema per source system, carried in each entity's bronze_table value.
# --------------------------------------------------------------------------- #
print("\n[layout] three catalog variables, no layer schemas")

_vars = (bundle or {}).get("variables", {}) or {}
for _name in ("bronze_catalog", "catalog", "gold_catalog"):
    check(f"{_name} is declared", _name in _vars, "missing from databricks.yml variables")
for _dead in ("bronze_schema", "gold_schema"):
    check(
        f"{_dead} is gone",
        _dead not in _vars,
        "bronze and gold are separate catalogs; a layer schema is meaningless here",
    )

_targets = (bundle or {}).get("targets", {}) or {}
for _tname, _t in _targets.items():
    _tv = (_t.get("variables") or {})
    if "catalog" not in _tv:
        continue
    check(
        f"target {_tname} sets all three catalogs",
        {"bronze_catalog", "catalog", "gold_catalog"} <= set(_tv),
        f"has {sorted(_tv)}",
    )
```

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run python verify_repo.py 2>&1 | grep -A12 '\[layout\]'
```

Expected: `bronze_catalog` and `gold_catalog` missing; `bronze_schema` and `gold_schema` still present; every target failing the three-catalog check.

- [ ] **Step 3: Rewrite the variables block**

In `databricks.yml`, replace the `catalog` / `bronze_schema` / `gold_schema` declarations with:

```yaml
  bronze_catalog:
    description: >-
      Region-local BRONZE catalog. One catalog per lake, with one SCHEMA PER
      SOURCE SYSTEM inside it -- not a bronze schema in a shared catalog. Each
      source appears twice: <source>_raw is the append-only dump of files from
      storage, and <source> has Auto CDC and physical PII masking applied.
      THE VAULT READS <source>_raw -- see spec decision D3. Hashing a
      physically-masked identifier would stop business keys joining across
      lakes, and hash_parity_check.py cannot detect that.
  catalog:
    description: >-
      Region-local SILVER catalog -- the vault's write target. Contains
      ${vault_schema} and ${governance_schema}.
      CONFIRM against `databricks catalogs list` before first deploy.
  gold_catalog:
    description: >-
      Region-local GOLD catalog. Schemas are per project. Gold generation is
      not in this repo's scope; this exists so governance can grant on it.
```

Delete `bronze_schema` and `gold_schema` entirely. Keep `vault_schema` and `governance_schema` unchanged.

- [ ] **Step 4: Set all three in every target**

For `usnc_tds` and `usnc`, exactly:

```yaml
  usnc_tds:
    mode: production
    workspace:
      host: ${var.usnc_tds_host}
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
    variables:
      bronze_catalog: 01_usnc_bronze_dev
      catalog:        02_usnc_silver_edm_dev
      gold_catalog:   03_usnc_gold_edm_dev
      expectations_table: 02_usnc_silver_edm_dev.governance.ref_dq_expectation

  usnc:
    mode: production
    workspace:
      host: ${var.usnc_prod_host}
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
    variables:
      bronze_catalog: 01_usnc_bronze
      catalog:        02_usnc_silver_edm
      gold_catalog:   03_usnc_gold_edm
      expectations_table: 02_usnc_silver_edm.governance.ref_dq_expectation
    run_as:
      service_principal_name: ${var.service_principal}
```

Apply the same shape to `weu_tds`, `weu`, `uks_tds`, `uks`, `aue_tds`, `aue` and `dev`, substituting the lake code. TDS targets get `_dev` catalogs; PROD targets get none.

- [ ] **Step 5: Fix the bronze and gold grants (DEF-2)**

In `governance/apply_masks.sql`, the bronze grants currently read `` `${catalog}`.`${bronze_schema}` `` — which resolves to `02_usnc_silver_edm_dev.bronze`, a schema that will never exist. Bronze has a schema per source, so a single grant statement cannot cover it. Replace the three bronze lines with a catalog-level grant plus an explanatory comment:

```sql
-- Bronze is a SEPARATE CATALOG with one schema per source system, so there is
-- no single schema to grant on. Grant at catalog level; per-source restriction
-- is the bronze platform's concern, not this repo's.
GRANT USE CATALOG ON CATALOG `${bronze_catalog}` TO `hfig_data_engineering`;
GRANT SELECT       ON CATALOG `${bronze_catalog}` TO `hfig_data_engineering`;
```

Replace the two gold lines similarly:

```sql
GRANT USE CATALOG ON CATALOG `${gold_catalog}` TO `hfig_analysts`;
GRANT SELECT      ON CATALOG `${gold_catalog}` TO `hfig_analysts`;
```

- [ ] **Step 6: Verify the SQL still renders offline**

`checks/apply_governance.py:78-90` still declares `--bronze-schema` and `--gold-schema`, which no longer exist. Rename both the arguments and the `bindings` dict keys:

```python
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--vault-schema", default="silver_vault")
    ap.add_argument("--bronze-catalog", required=True)
    ap.add_argument("--gold-catalog", required=True)
    ap.add_argument("--dry-run", action="store_true",
                    help="render and split without executing; needs no workspace")
    args = ap.parse_args()

    bindings = {
        "catalog": args.catalog,
        "vault_schema": args.vault_schema,
        "bronze_catalog": args.bronze_catalog,
        "gold_catalog": args.gold_catalog,
    }
```

Both are `required=True` rather than defaulted: a bronze catalog guessed wrong is a cross-catalog grant, which is DEF-2 all over again. `render()` refuses unresolved placeholders, so a missed rename fails loudly.

```bash
uv run python checks/apply_governance.py --dry-run \
  --catalog 02_usnc_silver_edm_dev \
  --bronze-catalog 01_usnc_bronze_dev \
  --gold-catalog 03_usnc_gold_edm_dev
```

Expected: `N statement(s) rendered for catalog=02_usnc_silver_edm_dev`, a numbered statement list, then `dry run only -- nothing executed`.

- [ ] **Step 7: Run both suites**

```bash
uv run python verify_repo.py
uv run python tests/test_accelerator.py
```

Expected: the `[layout]` section fully passes; `test_accelerator.py` still `ALL CHECKS PASSED`.

- [ ] **Step 8: Commit**

```bash
git add databricks.yml governance/apply_masks.sql verify_repo.py checks/apply_governance.py
git commit -m "Replace the single-catalog assumption with three catalog variables (D1, DEF-2)

The estate puts each medallion layer in its own catalog. The bundle modelled
one catalog holding bronze/silver/gold schemas, so apply_masks.sql granted on
02_usnc_silver_edm_dev.bronze -- a schema that will never exist.

bronze_schema is deleted rather than renamed: bronze has one schema per source
system, already carried in each entity's three-part bronze_table value. Bronze
and gold grants move to catalog level.

Outstanding gates: preflight_target, mask_survival_check (both need a workspace)."
```

---

### Task 4: Move the baseline to usnc_tds (D7, DEF-7)

**Files:**
- Modify: `databricks.yml` (`default: true`; the `dev` target host; the topology and promotion comments)
- Modify: `checks/conformance_check.py:57-66` (TARGETS) and `:196` (baseline default)
- Modify: `verify_repo.py:310,315,319` (hardcoded self-test hosts)

**Interfaces:**
- Consumes: Task 3's variables.
- Produces: `usnc_tds` as bundle default and conformance baseline.

- [ ] **Step 1: Write the failing checks**

Append to `verify_repo.py` before the summary:

```python
# --------------------------------------------------------------------------- #
# BASELINE: usnc_tds is the development environment and the conformance
# baseline. weu_tds is not used by this project.
# --------------------------------------------------------------------------- #
print("\n[baseline] usnc_tds is the default target and the conformance baseline")

_defaults = [n for n, t in ((bundle or {}).get("targets", {}) or {}).items()
             if t.get("default")]
check("usnc_tds is the only default target", _defaults == ["usnc_tds"], f"got {_defaults}")

_conf = (ROOT / "checks" / "conformance_check.py").read_text()
check(
    "conformance baseline defaults to usnc_tds",
    'default="usnc_tds"' in _conf,
    "checks/conformance_check.py still defaults its --baseline elsewhere",
)
check(
    "conformance TARGETS use the estate catalog convention",
    "02_usnc_silver_edm_dev" in _conf and "hfig_usnc_tds" not in _conf,
    "TARGETS still carries hfig_* catalog names",
)

_vr = (ROOT / "verify_repo.py").read_text()
check(
    "preflight self-tests target usnc, not weu/uks",
    "adb-2593897084138079" in _vr and "adb-7405615198748199" not in _vr,
    "verify_repo.py still self-tests against weu_tds/uks_tds hosts (DEF-7)",
)
```

- [ ] **Step 2: Run it and watch all four fail**

```bash
uv run python verify_repo.py 2>&1 | grep -A6 '\[baseline\]'
```

- [ ] **Step 3: Move the default and repoint dev**

In `databricks.yml`: delete `default: true` from `weu_tds`, add it to `usnc_tds`. Change the `dev` target's host from `${var.weu_tds_host}` to `${var.usnc_tds_host}` and its catalogs to the usnc `_dev` set from Task 3.

- [ ] **Step 4: Update conformance TARGETS and baseline**

Replace the `TARGETS` dict at `checks/conformance_check.py:57-66`:

```python
TARGETS = {
    # target        silver catalog             profile
    "usnc_tds": ("02_usnc_silver_edm_dev", "hfig-usnc-tds"),
    "usnc":     ("02_usnc_silver_edm",     "hfig-usnc"),
    "weu_tds":  ("02_weu_silver_edm_dev",  "hfig-weu-tds"),
    "weu":      ("02_weu_silver_edm",      "hfig-weu"),
    "uks_tds":  ("02_uks_silver_edm_dev",  "hfig-uks-tds"),
    "uks":      ("02_uks_silver_edm",      "hfig-uks"),
    "aue_tds":  ("02_aue_silver_edm_dev",  "hfig-aue-tds"),
    "aue":      ("02_aue_silver_edm",      "hfig-aue"),
}
```

At line 196 change the baseline default to `usnc_tds`, and at line 190 reorder the `--targets` default so usnc leads:

```python
    ap.add_argument("--targets", default="usnc_tds,weu_tds,uks_tds,aue_tds")
    ...
    ap.add_argument("--baseline", default="usnc_tds", help="the target others must match")
```

Leave `NON_EEA` unchanged — usnc and aue remain outside the EEA regardless of which is the baseline.

- [ ] **Step 5: Retarget the preflight self-tests (DEF-7)**

At `verify_repo.py:310,315,319` replace the literal hosts. Use `usnc_tds` for the positive case and a different lake for the negative:

```python
                    "--actual-host", "https://adb-2593897084138079.19.azuredatabricks.net"],
```

Keep whichever assertion proves a *mismatch* is caught, but point it at a lake that is not usnc — `adb-718050136221554.14` (uks_tds) is fine — so the test still proves preflight rejects the wrong workspace.

- [ ] **Step 6: Correct the topology and promotion comments**

Two blocks in `databricks.yml` are now wrong. The topology table must gain the ninth workspace:

```
#   EU also hosts a "Datalakehouse-Global" workspace, which receives Gold from all
#   four lakes via Delta Sharing. It is not a deploy target for this bundle.
```

And the promotion order must lead with usnc:

```
# PROMOTION ORDER is by environment, then by region. Never region-first.
#   1. usnc_tds  (the development environment; first build, all gates)
#   2. weu_tds, uks_tds, aue_tds
#   3. conformance gate across all four TDS workspaces, baseline usnc_tds
#   4. usnc, then weu, uks, aue -- each preceded by prod-vs-tds conformance
#
# TDS-to-PROD promotion pipelines are deliberately not designed yet.
```

- [ ] **Step 7: Run both suites**

```bash
uv run python verify_repo.py
uv run python tests/test_accelerator.py
```

Expected: `[baseline]` fully passes, `test_accelerator.py` still clean.

- [ ] **Step 8: Commit**

```bash
git add databricks.yml checks/conformance_check.py verify_repo.py
git commit -m "Move the default target and conformance baseline to usnc_tds (D7, DEF-7)

usnc_tds is the real development environment; weu_tds will not be used.
Moves default: true, repoints the dev target, rewrites conformance TARGETS
onto the estate catalog convention, and retargets verify_repo.py's preflight
self-tests, which asserted against weu_tds and uks_tds hosts.

conformance_check.py is dormant until a second lake deploys -- it compares
two or more targets and there is nothing yet to compare. Stated, not skipped.

Outstanding gates: preflight_target, conformance_check (both need a workspace)."
```

---

### Task 5: Reconcile the documentation claims (DEF-4, DEF-5)

**Files:**
- Modify: `README.md:40` (file count), `:45` and `:135` (check counts), `:11-12` (Superpowers pin)
- Modify: `CHANGELOG.md:147` (check count)

**Interfaces:**
- Consumes: Task 1's uv commands in the README.
- Produces: documentation that `verify_repo.py` can assert against.

- [ ] **Step 1: Establish ground truth**

```bash
uv run python verify_repo.py 2>&1 | tail -3
git ls-files | wc -l
```

`verify_repo.py` already prints `<N> verification checks run` from its own counter. Record both numbers.

- [ ] **Step 2: Write the failing check**

Append to `verify_repo.py` before the summary:

```python
# --------------------------------------------------------------------------- #
# DOCUMENTATION: a hand-maintained count drifts. README stated 151 and 266,
# CHANGELOG stated 238, for the same script.
# --------------------------------------------------------------------------- #
print("\n[docs] no hand-maintained counts, no unresolved placeholders")

_readme = (ROOT / "README.md").read_text()
_changelog = (ROOT / "CHANGELOG.md").read_text()

check(
    "README does not hardcode a verify_repo check count",
    not re.search(r"\d+\s+(repo-integrity\s+)?checks", _readme),
    "the count is printed at runtime; a literal in prose will drift",
)
check(
    "README carries no unresolved version placeholder",
    "<X.Y.Z>" not in _readme,
    "DEF-5: the Superpowers pin line was committed with a literal placeholder",
)
check(
    "README references no path that does not exist",
    "tooling/superpowers" not in _readme or (ROOT / "tooling" / "superpowers").exists(),
    "DEF-5: README points at tooling/superpowers/CHANGELOG.md, which is absent",
)
```

- [ ] **Step 3: Run it and watch it fail**

```bash
uv run python verify_repo.py 2>&1 | grep -A4 '\[docs\]'
```

- [ ] **Step 4: Fix the README**

At line 40, replace `33 files in 7 directories` with the real number from Step 1.

At line 45 the count is already removed by Task 1 Step 6. At line 135, change the Layout entry:

```
verify_repo.py              repo-integrity checks, offline (count printed at runtime)
```

Replace the broken pin block (DEF-5), restoring the blank line before `---`:

```markdown
Superpowers methodology layer: `superpowers@claude-plugins-official` v6.3.0,
installed project-scoped via `.claude/settings.json`. Project-local gates that map
Superpowers phases onto this repo's hard gates live in
`.claude/skills/dv-accelerator-gates/`.

---
```

The original named `obra/superpowers` with an unresolved `v<X.Y.Z>` and pointed at
`tooling/superpowers/CHANGELOG.md`, which does not exist in this repo.

- [ ] **Step 5: Fix the CHANGELOG**

At `CHANGELOG.md:147`, change `grew from 92 to **238 offline checks**` to `grew substantially, and now reports its own count at runtime`. Leave the `tests/test_accelerator.py: **121 checks**` line — that one is consistent with the README and is a different script.

- [ ] **Step 6: Run both suites**

```bash
uv run python verify_repo.py
uv run python tests/test_accelerator.py
```

Expected: `[docs]` passes; whole run exits 0. **This is the first point at which `verify_repo.py` should be fully green.**

- [ ] **Step 7: Commit**

```bash
git add README.md CHANGELOG.md verify_repo.py
git commit -m "Reconcile documentation claims (DEF-4, DEF-5)

verify_repo.py's check count was stated three ways -- README said 151 in one
place and 266 in another, CHANGELOG said 238. The script already prints its
own count, so the literals are removed rather than corrected. File count
corrected against git ls-files.

The uncommitted Superpowers pin carried an unresolved v<X.Y.Z> placeholder,
named obra/superpowers rather than the installed plugin, and pointed at
tooling/superpowers/CHANGELOG.md, which does not exist.

verify_repo.py is green for the first time."
```

---

### Task 6: Authenticate to usnc_tds and run preflight (DEF-6)

Requires an interactive login. An agentic worker cannot complete this alone — hand Steps 1 and 2 to the user.

**Files:**
- Modify: `~/.databrickscfg` (user action, outside the repo)

**Interfaces:**
- Consumes: Task 4's target definitions.
- Produces: a working `hfig-usnc-tds` profile. Every task in the sub-project 3 plan depends on it.

- [ ] **Step 1: USER ACTION — create the profile**

All five existing profiles resolve to `adb-4750792675027163`, the EU production workspace. Nothing reaches usnc.

```bash
databricks auth login \
  --host https://adb-2593897084138079.19.azuredatabricks.net \
  --profile hfig-usnc-tds
```

The profile name must be exactly `hfig-usnc-tds` — `conformance_check.py`'s TARGETS map expects it.

- [ ] **Step 2: USER ACTION — decide what to do about `tve-dev`**

`tve-dev` is named like a development profile and points at EU **production**. With nine near-identical workspaces this is the exact failure `preflight_target.py` exists to catch. Rename it to something that says production, or remove it. This is the user's call — do not edit `~/.databrickscfg` on their behalf.

- [ ] **Step 3: Verify the profile resolves**

```bash
databricks current-user me --profile hfig-usnc-tds
databricks catalogs list --profile hfig-usnc-tds
```

Expected: the catalog list contains `01_usnc_bronze_dev`, `02_usnc_silver_edm_dev` and the domain-split catalogs.

- [ ] **Step 4: Run the first real gate**

```bash
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
```

Expected: PASS, matching declared host `adb-2593897084138079.19` against the profile's. This is the first gate in this plan that actually executes.

- [ ] **Step 5: Validate the bundle against the live workspace**

```bash
databricks bundle validate --strict -t usnc_tds --profile hfig-usnc-tds
```

Expected: clean. Undeclared-variable errors here are the class of failure the CHANGELOG records from v0.1; fix them in `databricks.yml` and re-run.

- [ ] **Step 6: Answer the spec's section 7 questions and record them**

With the profile working, resolve the five open inspections and append the answers to the spec under a new `## 10. Inspection results` heading:

```bash
databricks schemas list 01_usnc_bronze_dev --profile hfig-usnc-tds
databricks tables list 01_usnc_bronze_dev fieldglass_raw --profile hfig-usnc-tds
```

Then pick any table name from that listing and inspect its columns, to see whether an
operation-flag column is present and what it is called:

```bash
databricks experimental aitools tools discover-schema \
  01_usnc_bronze_dev.fieldglass_raw.<name-from-the-listing> --profile hfig-usnc-tds
```

Record: whether bronze table names carry the source prefix; which `_raw` schemas expose an operation-flag column and its name; whether `bullhorn_native` and `bullhorn_salesforce` are one source or two; the full schema inventory; and whether prod catalogs are visible from this workspace (which settles whether catalog isolation is already bound).

- [ ] **Step 7: Commit the inspection results**

```bash
git add docs/superpowers/specs/2026-08-24-usnc-tds-retarget-design.md
git commit -m "Record usnc_tds inspection results answering spec section 7

preflight_target passes for usnc_tds. bundle validate --strict is clean.
Resolves the five inspections the design deferred, unblocking sub-project 3
(source rebinding)."
```

---

## Not in this plan

Sub-projects 3, 4 and 5 from the spec each need their own plan, and each is blocked:

- **Sub-project 3 (rebind sources — D3, D4, D5, D6)** needs Task 6's inspection results. It rewrites all 20 entity `bronze_table:` values onto `01_usnc_bronze_dev.<source>_raw.<table>`, adds the `active_sources` mechanism and splits `create_streaming_table` from `append_flow`, adds the Beeline and Bullhorn bindings, and removes `src/pipelines/bronze_ingest.py` along with the `resources/*.yml` wiring that references it (D4 — the estate's own `_raw`-to-clean pipeline already does that work).
- **Sub-project 4 (raw vault prototype)** needs a deployed pipeline. Its first gate is gate zero — `hash_parity_check.py` — before any load.
- **Sub-project 5 (business vault: PIT and Bridge)** is net-new generator capability and needs its own design document before a plan.

**The blocking dependency for sub-project 4** is Day-1 verification task 1: mask survival through `_v1`, PIT materialized view and Gold projection. Under spec D3 the silver vault holds unmasked PII at rest defended solely by UC column masks, so if masks do not propagate, personal columns must move to satellites Gold never projects — a modelling change that must land before entities are finalised.
