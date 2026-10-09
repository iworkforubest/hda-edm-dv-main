#!/usr/bin/env python3
"""
Independent verification of the accelerator repository.

Checks things the unit tests deliberately do NOT: that files referenced across the
bundle actually exist, that every ${var.x} used is declared, that the metadata and the
governance SQL agree with each other, and that nothing in the vault path can reach a
mutating operation. Run before packaging and before any deploy.

    python verify_repo.py

Exit 1 on any finding.
"""

from __future__ import annotations

import ast
import csv
import io
import json
import re
import subprocess
from types import SimpleNamespace as _SimpleNamespace
import zipfile
import xml.etree.ElementTree as ET
import sys
import pathlib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

FINDINGS: list[str] = []
CHECKS = 0


def ok(label: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"  PASS  {label}")


def bad(label: str, detail: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"  FAIL  {label}")
    FINDINGS.append(f"{label}: {detail}")


def check(label: str, condition: bool, detail: str = "") -> None:
    ok(label) if condition else bad(label, detail)


# --------------------------------------------------------------------------- #
# [0] LAYOUT. Runs first, because everything below assumes the tree is intact.
#
# The most likely reason this fails is downloading the files individually instead of
# extracting the archive. The bundle uses RELATIVE paths (resources/*.yml refers to
# ../src and ../checks), so a flat folder cannot work no matter what it contains.
# --------------------------------------------------------------------------- #
print("[0] layout: the project tree is intact")

REQUIRED_DIRS = [
    "src/accelerator", "src/pipelines", "metadata/entities",
    "checks", "governance", "resources", "tests", "tools",
]
REQUIRED_FILES = [
    "databricks.yml", "README.md", "DEPLOY.md", "CHANGELOG.md",
    "src/accelerator/hashing.py", "src/accelerator/spec.py",
    "src/accelerator/factory.py", "src/accelerator/naming.py",
    "src/pipelines/silver_vault.py", "src/pipelines/bronze_ingest.py",
    "resources/vault_pipeline.yml", "resources/vault_job.yml",
    "governance/apply_masks.sql",
    "checks/append_only_check.py", "checks/hash_parity_check.py",
    "checks/conformance_check.py", "checks/mask_survival_check.py",
    "checks/apply_governance.py", "checks/loop1_reconciliation.py",
    "checks/publish_metadata.py", "checks/preflight_target.py",
    "checks/journal_integrity_check.py",
    "checks/aggregate_reconciliation_check.py",
    "tests/test_accelerator.py", "tests/golden_hash_vectors.json",
    "tools/render_erd.py", "tools/estimate_footprint.py",
    "metadata/volumes.yml",
]

missing_dirs = [d for d in REQUIRED_DIRS if not (ROOT / d).is_dir()]
missing_files = [f for f in REQUIRED_FILES if not (ROOT / f).is_file()]
entity_count = len(list((ROOT / "metadata" / "entities").glob("*.yml"))) \
    if (ROOT / "metadata" / "entities").is_dir() else 0

if missing_dirs or missing_files:
    print("\n" + "=" * 70)
    print("LAYOUT CHECK FAILED -- this is not a complete copy of the project.\n")
    if missing_dirs:
        print(f"  missing directories ({len(missing_dirs)}): {missing_dirs}")
    if missing_files:
        print(f"  missing files ({len(missing_files)}):")
        for f in missing_files:
            print(f"      {f}")
    flat = list(ROOT.glob("*.py")) + list(ROOT.glob("*.sql")) + list(ROOT.glob("*.yml"))
    if len(flat) >= 3 and missing_dirs:
        print("\n  Several source files are sitting in the top-level folder. That looks")
        print("  like files downloaded INDIVIDUALLY rather than the archive extracted.")
    print("\n  FIX: extract hfig-dv-accelerator-v0.2.zip and run this from inside the")
    print("  extracted hfig-dv-accelerator-v0.2/ folder. Expect 35 files in 7 directories.")
    print("  The bundle uses relative paths, so a flat folder cannot work.")
    print("=" * 70)
    sys.exit(1)

ok(f"all {len(REQUIRED_DIRS)} directories present")
ok(f"all {len(REQUIRED_FILES)} required files present")
check("metadata contains the worked entities", entity_count >= 8,
      f"found {entity_count} entity files in metadata/entities")


# --------------------------------------------------------------------------- #
print("\n[1] syntax: every file parses")


# DIRECTORIES THAT ARE NOT THIS REPO. The exclusion existed only on the conflict-marker
# sweep further down; the three parse walks below had none, so they audited whatever
# happened to be installed in .venv as though it were our code.
#
# HARMLESS UNTIL IT WAS NOT. With one small dependency the inflation went unnoticed for
# months. Adding pyspark on 4 September 2026 took `[1] syntax` from 73 real files to 623
# and the total from 979 checks to 1876 -- more than half of this gate's output was
# describing PySpark. Two things follow, and the second is the serious one: the run gets
# slower for nothing, and any "every file" assertion silently changes what it is about. A
# dependency shipping a file this repo's floor rejects, or a vendored fixture containing
# something that looks like a conflict marker, would fail the build against code nobody
# here can edit.
# THE FALLBACK denylist, for when git cannot answer. See _repo_inventory below for why it
# is the fallback and not the rule.
_NOT_OURS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".databricks",
             ".pytest_cache", ".mypy_cache", ".ruff_cache", ".superpowers"}


def _repo_inventory() -> set[Path] | None:
    """Tracked plus untracked-but-not-ignored files, per git. None if git cannot answer.

    GIT IS THE AUTHORITY ON WHAT BELONGS TO THIS REPO, because `.gitignore` is the file
    whose entire job is to say so. A denylist maintained here is a second answer to the
    same question, and it loses: the first version of it missed `.databricks/` and
    `.claude/settings.json`, both already listed in .gitignore, so this gate parsed the
    Databricks CLI's own deployment state as though it were our JSON.
    
    THAT WAS NOT A COSMETIC MISS. `.databricks/` is created BY DEPLOYING, so running a
    deploy silently changed this gate's check count -- 936 in CI, 941 locally, for the
    same commit, which is precisely the environment-dependence the walk fix claimed to
    remove. And a CLI state file that failed to parse would have failed the build on a
    file nobody here writes.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "--cached", "--others",
             "--exclude-standard", "-z"],
            capture_output=True, text=True, timeout=30, check=True).stdout
    except Exception:  # noqa: BLE001  -- no git, not a repo, or git too old
        return None
    names = [n for n in out.split("\0") if n]
    # An empty answer means this is not a working tree we can trust (a bare export, an
    # unpacked zip), not a repo with no files. Fall back rather than check nothing.
    return {(ROOT / n).resolve() for n in names} or None


_INVENTORY = _repo_inventory()


def repo_files(suffix: str) -> list[Path]:
    """Every file of one suffix that belongs to THIS repo, sorted.

    One definition, used by all four walks. Prefers git's inventory; falls back to the
    _NOT_OURS denylist when git cannot answer, because this script must still run from an
    extracted zip with no git at all -- the top-of-file layout diagnostics exist for
    exactly that case, and a walk that silently found nothing would be worse than one
    that includes a stray directory.
    """
    found = ROOT.rglob(f"*{suffix}")
    if _INVENTORY is not None:
        return sorted(f for f in found if f.resolve() in _INVENTORY)
    return sorted(f for f in found
                  if not any(part in _NOT_OURS for part in f.parts))


py_files = repo_files(".py")
# A FLOOR, BECAUSE OVER-EXCLUDING IS THE FAILURE MODE OF A DENYLIST. If a future entry
# swallowed the repo, every walk below would pass by checking nothing -- the unfailable
# gate this project has now found more than a dozen times. 40 is well under the real
# count (73 on 4 Sep) and well above anything a broken exclusion would leave.
check("the file walk still finds this repo's own Python files",
      len(py_files) >= 40,
      f"found {len(py_files)} -- the inventory is excluding too much, and every walk "
      f"below would then pass by examining nothing")
# WHICH ANSWER WE GOT IS WORTH PRINTING. The two paths can legitimately disagree by a few
# files, and a count that moves between a developer's box and CI is the first thing anyone
# investigates -- so say which rule produced it rather than leaving it to be deduced.
ok(f"file inventory from {'git ls-files' if _INVENTORY else 'the _NOT_OURS denylist'}: "
   f"{len(py_files)} python file(s)")
import py_compile

for f in py_files:
    if "__pycache__" in str(f):
        continue
    try:
        py_compile.compile(str(f), doraise=True, cfile="/tmp/_vc.pyc")
        ok(f"python parses: {f.relative_to(ROOT)}")
    except py_compile.PyCompileError as exc:
        bad(f"python parses: {f.relative_to(ROOT)}", str(exc))

# py_compile above uses the RUNNING interpreter. On a dev box that is 3.13, so it
# cannot see syntax that 3.12 legalised and pyproject's declared floor rejects. One
# such construct has already reached main twice and gone red only in CI's 3.11 leg:
# a backslash inside an f-string expression part, which PEP 701 permitted in 3.12.
# This catches that single construct locally -- it is NOT a general 3.11 audit, and
# CI's 3.11 leg remains the authority for everything else.
import ast as _ast_floor

_floor_scanned = 0
_floor_offenders: list[str] = []
for f in py_files:
    if ".venv" in f.parts or "__pycache__" in str(f):
        continue
    _floor_src = f.read_text(encoding="utf-8")
    try:
        _floor_tree = _ast_floor.parse(_floor_src)
    except SyntaxError:
        continue  # already reported by py_compile above
    _floor_scanned += 1
    for _n in _ast_floor.walk(_floor_tree):
        if isinstance(_n, _ast_floor.FormattedValue):
            _seg = _ast_floor.get_source_segment(_floor_src, _n.value)
            if _seg and "\\" in _seg:
                _floor_offenders.append(f"{f.relative_to(ROOT)}:{_n.lineno} {_seg[:60]}")

check("no f-string expression part contains a backslash -- a SyntaxError on 3.11, "
      "which pyproject declares as the floor and only CI exercises",
      not _floor_offenders,
      f"{len(_floor_offenders)} offender(s) across {_floor_scanned} source files: "
      f"{_floor_offenders[:3]} -- hoist the expression into a named local; "
      f"a8f336f-class break passes every 3.13 run and fails the 3.11 leg")

yml_files = repo_files(".yml")
docs: dict[Path, object] = {}
for f in yml_files:
    try:
        docs[f] = yaml.safe_load(f.read_text(encoding="utf-8"))
        ok(f"yaml parses: {f.relative_to(ROOT)}")
    except Exception as exc:  # noqa: BLE001
        bad(f"yaml parses: {f.relative_to(ROOT)}", str(exc))

for f in repo_files(".json"):
    try:
        json.loads(f.read_text(encoding="utf-8"))
        ok(f"json parses: {f.relative_to(ROOT)}")
    except Exception as exc:  # noqa: BLE001
        bad(f"json parses: {f.relative_to(ROOT)}", str(exc))


# --------------------------------------------------------------------------- #
print("\n[2] bundle: referenced files exist")

for f, doc in docs.items():
    if not isinstance(doc, dict):
        continue
    text = f.read_text(encoding="utf-8")
    for match in re.finditer(r"path:\s*(\.\./[^\s\"']+)", text):
        rel = match.group(1)
        # ${bundle.target} APPEARS IN A PATH when an artefact is generated per target -- the
        # quality dashboards are, because their table names are qualified with the target's
        # catalog. Resolving the literal placeholder would look for a file named with it and
        # always fail, so every declared target is substituted and ALL must exist: a target
        # whose artefact was never generated would otherwise deploy a dangling reference.
        _rels = ([rel.replace("${bundle.target}", _t2)
                  for _t2 in sorted(
                      (docs.get(ROOT / "databricks.yml") or {}).get("targets", {}) or {})]
                 if "${bundle.target}" in rel else [rel])
        for _r in _rels:
            target = (f.parent / _r).resolve()
            check(f"{f.name} -> {_r}", target.exists(), f"missing {target}")
    for match in re.finditer(r"python_file:\s*(\.\./[^\s\"']+)", text):
        rel = match.group(1)
        target = (f.parent / rel).resolve()
        check(f"{f.name} -> {rel}", target.exists(), f"missing {target}")


# --------------------------------------------------------------------------- #
print("\n[3] bundle: every ${var.x} is declared")

bundle = docs.get(ROOT / "databricks.yml")
declared = set((bundle or {}).get("variables", {}) or {})
# target-level variables blocks also count as satisfying a reference
for target in ((bundle or {}).get("targets", {}) or {}).values():
    declared |= set((target or {}).get("variables", {}) or {})

used: set[str] = set()
for f in yml_files:
    used |= set(re.findall(r"\$\{var\.([a-zA-Z0-9_]+)\}", f.read_text(encoding="utf-8")))

undeclared = sorted(used - declared)
check(
    "no undeclared bundle variables",
    not undeclared,
    f"used but not declared in databricks.yml variables: {undeclared}",
)
unused = sorted(declared - used)
if unused:
    print(f"  note  declared but unused: {unused}")


# --------------------------------------------------------------------------- #
print("\n[4] bundle: job task graph is coherent")

job_doc = docs.get(ROOT / "resources" / "vault_job.yml") or {}
jobs = (job_doc.get("resources", {}) or {}).get("jobs", {}) or {}
for job_name, job in jobs.items():
    tasks = job.get("tasks", []) or []
    keys = [t["task_key"] for t in tasks]
    check(f"{job_name}: task keys unique", len(keys) == len(set(keys)), f"{keys}")
    for t in tasks:
        for dep in t.get("depends_on", []) or []:
            check(
                f"{job_name}: {t['task_key']} depends on existing {dep['task_key']}",
                dep["task_key"] in keys,
                f"unknown task {dep['task_key']}",
            )
            check(
                f"{job_name}: {t['task_key']} does not depend on itself",
                dep["task_key"] != t["task_key"],
                f"{t['task_key']} lists itself in depends_on -- a dependency cycle "
                f"Databricks rejects at validate/deploy",
            )
        # serverless python tasks need an environment_key
        if "spark_python_task" in t:
            check(
                f"{job_name}: {t['task_key']} declares environment_key",
                "environment_key" in t,
                "spark_python_task on serverless requires environment_key",
            )
    env_keys = {e["environment_key"] for e in (job.get("environments", []) or [])}
    for t in tasks:
        if "environment_key" in t:
            check(
                f"{job_name}: {t['task_key']} environment exists",
                t["environment_key"] in env_keys,
                f"{t['environment_key']} not in {env_keys}",
            )
    # the parity gate must have no dependencies -- it is gate zero
    parity = next((t for t in tasks if t["task_key"] == "assert_hash_parity"), None)
    check(f"{job_name}: hash parity is gate zero", parity is not None and not parity.get("depends_on"))
    # nothing may run before the parity gate. Bronze ingest is out of scope
    # (spec decision D4), so the raw vault load is the first load task.
    # TRANSITIVELY, not directly. DEF-20 inserted create_mask_functions between the two,
    # because raw_vault cannot declare a MASK naming a function that does not exist yet.
    # The property that matters is "nothing loads before gate zero", which is about
    # REACHABILITY in the task graph -- asserting a direct edge would forbid ever putting
    # a prerequisite in front of the load, which is not what gate zero is for.
    _by_key = {t["task_key"]: t for t in tasks}

    def _ancestors(key, seen=None):
        seen = seen if seen is not None else set()
        for dep in _by_key.get(key, {}).get("depends_on", []) or []:
            dk = dep["task_key"]
            if dk not in seen:
                seen.add(dk)
                _ancestors(dk, seen)
        return seen

    # STATED OVER EVERY LOAD, NOT OVER ONE NAMED TASK. This asked about `raw_vault`, which
    # was the only pipeline task in the raw layer until the domain split on 25 September --
    # after it, that key names nothing, `_by_key.get` returns None, and a gate that reports
    # "gate zero must be among them" for a task that does not exist is a gate that has
    # stopped looking. The invariant never was about one task: it is that gate zero clears
    # before ANY pipeline runs, and that nothing runs a pipeline before gate zero itself.
    _loads = sorted(k for k, t in _by_key.items() if t.get("pipeline_task") is not None)
    _ungated_loads = [k for k in _loads if "assert_hash_parity" not in _ancestors(k)]
    check(
        f"{job_name}: every load waits for the parity gate",
        bool(_loads) and not _ungated_loads,
        f"{_ungated_loads or 'no pipeline task at all'} -- gate zero must be an ancestor of "
        f"every load, directly or through a prerequisite; a job with no load to gate passes "
        f"this by having nothing to check",
    )
    # and nothing that runs before gate zero may itself be a load
    _early_loads = sorted(set(_ancestors("assert_hash_parity")) & set(_loads))
    check(
        f"{job_name}: nothing before gate zero is itself a load",
        not _early_loads,
        f"{_early_loads} runs a pipeline before the parity gate has cleared it, so the keys "
        f"it writes were never checked against the reference implementation",
    )
    check(
        f"{job_name}: no bronze ingest task",
        "bronze_ingest" not in keys,
        "bronze ingestion is out of scope (D4); the estate lands <source>_raw itself",
    )


# --------------------------------------------------------------------------- #
print("\n[4b] targets: 8 workspaces, prod guarded, conformance map in step")

targets = (bundle or {}).get("targets", {}) or {}
PROD = ("weu", "uks", "usnc", "aue")
TDS = tuple(f"{r}_tds" for r in PROD)

for name in PROD + TDS:
    check(f"target {name} is declared", name in targets, f"missing from databricks.yml")

for name in PROD:
    tgt = targets.get(name, {}) or {}
    check(f"prod target {name} runs as a service principal", "run_as" in tgt,
          "a production lake must not deploy under an interactive identity")
    check(f"prod target {name} is not mode: development", tgt.get("mode") != "development",
          "development mode prefixes resources and pauses schedules")

for name in TDS:
    tgt = targets.get(name, {}) or {}
    # convention is 0N_<lake>_<layer>_<domain>_<env>: TDS carries _dev, PROD none
    for _layer in ("bronze_catalog", "catalog", "gold_catalog"):
        cat = (tgt.get("variables", {}) or {}).get(_layer, "")
        check(f"tds target {name} uses a non-prod {_layer}", cat.endswith("_dev"),
              f"catalog {cat!r} does not look like a TDS catalog")

for name in PROD:
    tgt = targets.get(name, {}) or {}
    for _layer in ("bronze_catalog", "catalog", "gold_catalog"):
        cat = (tgt.get("variables", {}) or {}).get(_layer, "")
        check(f"prod target {name} uses a prod {_layer}",
              bool(cat) and not cat.endswith("_dev"),
              f"catalog {cat!r} carries a _dev suffix on a production lake")

# LAKE BINDING. The two loops above only assert the _dev / no-_dev suffix, and the
# distinctness check further down only asserts that no two targets collide. Neither
# ties a target to ITS OWN lake: usnc_tds could declare 01_weu_bronze_dev alongside
# 03_aue_gold_edm_dev and every check would still pass. Deploying into the wrong
# regional lake is the failure this repo names as its worst -- it is a residency
# breach, not a typo -- so derive the expected triple from the target's own name and
# assert the declared values match.
#
# Convention: 0N_<lake>_<layer>_<domain>_<env>. Silver and gold carry `edm` in the
# domain slot, bronze has no domain slot, TDS carries _dev and PROD carries nothing.
_EXPECTED_LAYER_SHAPE = {
    "bronze_catalog": "01_{lake}_bronze{env}",
    "catalog": "02_{lake}_silver_edm{env}",
    "gold_catalog": "03_{lake}_gold_edm{env}",
}


def _expected_lake(target_name):
    """(lake code, env suffix) for a bundle target name.

    `dev` is a special case: it is the individual-developer target and points at the
    usnc TDS workspace, so its lake cannot be derived from its name. It is handled
    explicitly rather than by pattern, because a silent fallthrough would leave this
    check vacuous for exactly the target engineers use daily.
    """
    if target_name == "dev":
        return "usnc", "_dev"
    if target_name.endswith("_tds"):
        return target_name[: -len("_tds")], "_dev"
    return target_name, ""


for _tname in sorted(targets):
    _tvars = (targets[_tname].get("variables", {}) or {})
    if "catalog" not in _tvars:
        continue
    _lake, _env = _expected_lake(_tname)
    for _layer, _shape in _EXPECTED_LAYER_SHAPE.items():
        _want = _shape.format(lake=_lake, env=_env)
        _got = _tvars.get(_layer, "")
        check(
            f"target {_tname}: {_layer} names its own lake ({_lake})",
            _got == _want,
            f"declared {_got!r}, expected {_want!r} -- a catalog naming another lake "
            f"deploys this model into the wrong region",
        )

# every target's host variable must be distinct: 8 workspaces, 8 hosts
hosts = {n: (t_.get("workspace", {}) or {}).get("host") for n, t_ in targets.items()}
prod_hosts = [hosts[n] for n in PROD]
tds_hosts = [hosts[n] for n in TDS]
check("the four prod targets point at four different hosts",
      len(set(prod_hosts)) == 4, f"{prod_hosts}")
check("the four tds targets point at four different hosts",
      len(set(tds_hosts)) == 4, f"{tds_hosts}")
check("no prod target shares a host with a tds target",
      not (set(prod_hosts) & set(tds_hosts)),
      "prod and TDS are separate workspaces, not catalogs in one")

# the conformance tool must know about exactly the targets the bundle deploys.
# TARGETS maps each target to (silver catalog, CLI profile); the catalog follows
# the estate convention 0N_<lake>_silver_edm[_dev], not the older hfig_* naming.
conf_src = (ROOT / "checks" / "conformance_check.py").read_text(encoding="utf-8")
conf_targets = set(re.findall(r'^\s+"(\w+)":\s+\("0\d_', conf_src, re.M))
deployable = set(PROD) | set(TDS)
check("conformance_check knows every deployable target",
      deployable <= conf_targets,
      f"missing from TARGETS map: {sorted(deployable - conf_targets)}")
check("conformance_check invents no targets the bundle lacks",
      conf_targets <= set(targets),
      f"unknown to the bundle: {sorted(conf_targets - set(targets))}")

# catalogs must be distinct per target, or two lakes would write the same tables
cats = [ (t_.get("variables", {}) or {}).get("catalog") for n, t_ in targets.items()
         if n in deployable ]
check("every deployable target has its own catalog", len(set(cats)) == len(cats), f"{cats}")


# --------------------------------------------------------------------------- #
print("\n[4c] hosts: 8 workspaces, distinct, well-formed, provisioning recorded")

AZURE_HOST = re.compile(r"^https://adb-(\d+)\.(\d+)\.azuredatabricks\.net$")
host_vars = {k: (v or {}).get("default", "")
             for k, v in ((bundle or {}).get("variables", {}) or {}).items()
             if k.endswith("_host")}
check("all 8 host variables are declared", len(host_vars) == 8, f"found {sorted(host_vars)}")

populated = {k: v for k, v in host_vars.items() if v}
for name, host in sorted(populated.items()):
    check(f"{name} is a well-formed Azure Databricks URL", bool(AZURE_HOST.match(host)), host)
    check(f"{name} has no trailing slash", not host.endswith("/"), host)

check("every populated host is distinct -- no two targets share a workspace",
      len(set(populated.values())) == len(populated),
      f"{sorted(populated.values())}")

ids = [AZURE_HOST.match(h).group(1) for h in populated.values() if AZURE_HOST.match(h)]
check("every workspace id is distinct", len(set(ids)) == len(ids), f"{ids}")

unprovisioned = sorted(set(host_vars) - set(populated))
check("unprovisioned hosts are empty, not placeholder text",
      all(host_vars[u] == "" for u in unprovisioned), f"{unprovisioned}")
if unprovisioned:
    print(f"  note  not provisioned yet: {unprovisioned} -- deploy to these targets "
          f"will fail preflight by design")

# preflight must refuse an unprovisioned target and catch a wrong-lake deploy
import subprocess as _sp2  # noqa: E402

_pf = ROOT / "checks" / "preflight_target.py"
_unprov = _sp2.run([sys.executable, str(_pf), "--target", "usnc",
                    "--actual-host", "https://adb-2593897084138079.19.azuredatabricks.net"],
                   capture_output=True, text=True)
check("preflight refuses an unprovisioned target", _unprov.returncode == 1,
      _unprov.stdout[-200:])
_wrong = _sp2.run([sys.executable, str(_pf), "--target", "usnc_tds",
                   "--actual-host", "https://adb-718050136221554.14.azuredatabricks.net"],
                  capture_output=True, text=True)
check("preflight catches a wrong-lake deploy", _wrong.returncode == 1, _wrong.stdout[-200:])
_right = _sp2.run([sys.executable, str(_pf), "--target", "usnc_tds",
                   "--actual-host", "https://adb-2593897084138079.19.azuredatabricks.net"],
                  capture_output=True, text=True)
check("preflight passes a correct deploy", _right.returncode == 0, _right.stdout[-200:])


# --------------------------------------------------------------------------- #
print("\n[4d] hosts: literal workspace.host matches its variable's default (no drift)")

# DEF-10 fix: the Databricks CLI resolves auth BEFORE variable interpolation, so
# `workspace.host: ${var.x}` always fails auth resolution -- see the preflight and
# `bundle validate` findings this repo recorded. workspace.host is therefore now a
# LITERAL string on every target. The *_host variables above are kept anyway: their
# `description:` blocks carry load-bearing documentation (workspace IDs, and the note
# that EU prod is the older "Datalake"-named workspace, so nobody "corrects" that
# mapping). Keeping both the literal and the variable creates a drift risk -- this
# check makes sure they never disagree.


def _expected_host_var(target_name: str) -> str:
    """Map a target name to the *_host variable documenting its workspace.

    TDS targets and `dev` (which points at the usnc TDS workspace) use the bare
    `_host` suffix; PROD targets use `_prod_host`, not a bare `_host`.
    """
    if target_name == "dev":
        return "usnc_tds_host"
    if target_name.endswith("_tds"):
        return f"{target_name}_host"
    return f"{target_name}_prod_host"


for _tname in sorted(targets):
    _var_name = _expected_host_var(_tname)
    _literal = (targets[_tname].get("workspace", {}) or {}).get("host", "")
    _expected = host_vars.get(_var_name, "")
    check(
        f"target {_tname}: workspace.host matches {_var_name}'s default",
        _literal == _expected,
        f"target declares host {_literal!r}, but var.{_var_name} defaults to "
        f"{_expected!r} -- these have drifted apart",
    )


# --------------------------------------------------------------------------- #
print("\n[5] pipelines: no full refresh anywhere in the vault path")

for f in yml_files:
    text = f.read_text(encoding="utf-8")
    # only real YAML keys: indented, not inside a comment
    for match in re.finditer(r"(?m)^\s+full_refresh:\s*(\w+)\s*$", text):
        check(
            f"{f.name}: full_refresh is false",
            match.group(1).lower() == "false",
            f"found full_refresh: {match.group(1)}",
        )


# --------------------------------------------------------------------------- #
print("\n[6] insert-only: AUTO CDC confined to Bronze")

def _strip_comments(src: str) -> str:
    """Policy prose in docstrings and comments is not executable code."""
    src = re.sub(r'"""(?:.|\n)*?"""', "", src)
    return "\n".join(l.split("#", 1)[0] for l in src.splitlines())


vault_src = _strip_comments(
    (ROOT / "src" / "accelerator" / "factory.py").read_text(encoding="utf-8")
)
silver = (ROOT / "src" / "pipelines" / "silver_vault.py").read_text(encoding="utf-8")
forbidden = ("create_auto_cdc", "apply_changes", "AUTO CDC FROM", "APPLY CHANGES INTO")
for token in forbidden:
    check(
        f"factory.py contains no {token}",
        token.lower() not in vault_src.lower(),
        "a mutating CDC API in the vault path violates insert-only",
    )
check(
    "factory declares append flows",
    "append_flow" in vault_src,
    "no append_flow found -- the vault would not be insert-only",
)
check(
    "silver pipeline is metadata-driven only",
    "load_model" in silver and "hub_" not in silver,
    "entity names hard-coded in the pipeline defeat the generator",
)


# --------------------------------------------------------------------------- #
print("\n[7] metadata and governance agree")

from accelerator import contract, hashing, naming, spec  # noqa: E402

model = spec.load_model(ROOT / "metadata" / "entities")
ok(f"metadata loads: {len(model.entities)} entities")

masks_sql = (ROOT / "governance" / "apply_masks.sql").read_text(encoding="utf-8")
# policy prose in -- comments is not executable SQL
masks_exec = "\n".join(
    line for line in masks_sql.splitlines() if not line.strip().startswith("--")
)

# masks belong in the TABLE DEFINITION for streaming tables and materialized views,
# so the governance SQL must NOT try to ALTER them on
check(
    "governance SQL does not ALTER ... SET MASK on vault tables",
    "SET MASK" not in masks_exec,
    "row filters and column masks on streaming tables and materialized views must be "
    "set through the table definition; an ALTER TABLE does not survive a pipeline update",
)

# entities are business concepts; the source belongs in the binding and the table name
all_sources = {s.name.lower() for e in model.entities for s in e.sources}
for e in model.entities:
    leaks = [s for s in all_sources if e.name.endswith(f"_{s}") or f"_{s}_" in e.name]
    check(f"entity {e.name!r} carries no source system in its name", not leaks,
          f"{leaks} -- the same entity arrives from many systems")

# grain: an aggregate must state what it summarises and what it drops
aggregates = [e for e in model.entities if e.grain == "aggregate"]
check("at least one aggregate is labelled as such", bool(aggregates),
      "the GL journal is account-level, not per-worker; unlabelled aggregates get "
      "joined to a worker eventually")
for e in aggregates:
    raw = next((x for x in model.entities if x.name == e.aggregates_from), None)
    check(f"{e.base_table} names its transaction-grain counterpart", raw is not None)
    check(f"{e.base_table} states which grain it drops", bool(e.aggregate_drops))
    if raw is not None:
        check(f"{e.base_table} drops parents {raw.base_table} actually has",
              set(e.aggregate_drops) <= set(raw.parents),
              f"{sorted(set(e.aggregate_drops) - set(raw.parents))}")
        check(f"{e.base_table} does not also claim the grain it dropped",
              not (set(e.aggregate_drops) & set(e.parents)))
        check(f"{raw.base_table} is transaction grain", raw.grain == "transaction")
# raw payment data is per-worker and must be masked
for e in model.entities:
    if e.grain == "transaction" and "worker" in e.parents and e.payload:
        money = [c for c in e.payload if "amount" in c or c in ("rate",)]
        if money:
            check(f"{e.base_table} masks per-worker money",
                  bool(dict(e.masks)),
                  f"per-worker amounts {money[:3]} are personal AND financial data")

# links and NHLs must map every parent to source columns explicitly
for e in model.entities:
    if e.kind in naming.LINK_KINDS or (
        e.kind in naming.RAW_SATELLITE_KINDS
        and e.parents
        and any(x.name == e.parents[0] and x.kind in naming.LINK_KINDS
                for x in model.entities)
    ):
        for src in e.sources:
            mapped = set(dict(src.parent_keys))
            # WHAT MUST BE MAPPED IS A LEG, NOT A HUB. On a roled link each leg is keyed
            # by its role, and `set(e.parents)` on a hub named twice is a one-element set
            # -- so a binding that mapped only the child would satisfy a check asking
            # about both. The same collapse, one layer out from the one parent_roles
            # exists to close.
            if e.kind in naming.LINK_KINDS:
                need = {role or hub for hub, role in spec.parent_legs(e)}
            else:
                parent = next(x for x in model.entities if x.name == e.parents[0])
                need = {role or hub for hub, role in spec.parent_legs(parent)}
            check(
                f"{e.base_table}/{src.name} maps every parent key column",
                need <= mapped,
                f"unmapped parents {sorted(need - mapped)} -- a flat key_columns list "
                f"cannot say which columns belong to which parent",
            )

# a link satellite's parent key must be derivable from the link's own definition
for e in model.entities:
    if e.kind not in naming.RAW_SATELLITE_KINDS or not e.parents:
        continue
    parent = next((x for x in model.entities if x.name == e.parents[0]), None)
    if parent is not None and parent.kind in naming.LINK_KINDS:
        check(f"{e.base_table} is a link satellite on {parent.base_table}", True)
        for src in e.sources:
            check(
                f"{e.base_table}/{src.name} can rebuild {parent.base_table}'s key",
                {role or hub for hub, role in spec.parent_legs(parent)}
                <= set(dict(src.parent_keys)),
                "the satellite must derive the SAME key as its parent link, so it needs "
                "the same parent key columns -- by ROLE where the link declares roles",
            )

# satellites must fan out to one table per source
for e in model.entities:
    if e.kind in naming.RAW_SATELLITE_KINDS:
        names = [n for _s, n in e.tables()]
        check(f"{e.base_table} generates one table per source ({len(e.sources)})",
              len(names) == len(e.sources) and len(set(names)) == len(names), f"{names}")
        for src, name in e.tables():
            check(f"{name} is suffixed with its source",
                  naming.stable(name).endswith(src.name.lower()))
            check(f"{name} maps a payload for {src.name}",
                  len(src.payload) == len(e.payload),
                  f"source payload has {len(src.payload)} columns, entity declares {len(e.payload)}")

sensitive = [
    e for e in model.entities if e.sensitivity in ("personal", "financial", "restricted")
]
for e in sensitive:
    check(f"{e.base_table} ({e.sensitivity}) declares masks in metadata", bool(e.masks),
          "sensitive payload with no mask declaration would ship unmasked")

# every mask function named in metadata must exist in the governance SQL
declared_fns = {fn.split(".", 1)[1] for e in model.entities for _, fn in e.masks}
defined_fns = set(re.findall(r"CREATE OR REPLACE FUNCTION governance\.(\w+)", masks_exec))
check("every mask function used in metadata is defined in apply_masks.sql",
      declared_fns <= defined_fns,
      f"undefined: {sorted(declared_fns - defined_fns)}")

# the factory must emit masks, not rely on a post-deploy step
fac = (ROOT / "src" / "accelerator" / "factory.py").read_text(encoding="utf-8")
check("factory emits MASK clauses into the table definition",
      "MASK" in fac and "_mask_clauses" in fac,
      "masks must be declared where the table is declared")
check("factory masks the _v1 projection too",
      fac.count("_mask_clauses(entity, ") >= 2,
      "a mask on the base table does not automatically protect a derived view")
# DEF-16: the clause must carry the column TYPE. `col MASK fn` is not valid DDL, and
# Spark reports it as a syntax error at the mask function's name -- a long way from the
# cause. The type is taken from the binding's cast: block, and a masked column on an
# active binding that declares no cast must RAISE rather than emit something invalid.
check("the MASK clause is built with a column type",
      '"{col} {' in fac or "f\"{col} {" in fac,
      "a mask clause without a type is not valid DDL (DEF-16)")
check("a masked column with no declared cast is refused at build time",
      "no cast: declared for masked column" in fac,
      "it must fail loudly, not emit invalid DDL nor let SDP infer a type")
check("_mask_clauses takes the active bindings, not just the entity",
      "def _mask_clauses(entity: Entity, bindings" in fac,
      "the type comes from the bindings that actually load this table")
# DEF-19: schema= is the WHOLE schema, not an overlay, so a masked table must declare
# every column -- READ from the staged frame so names, types, order and nullability are
# what the flow produces, not a second description that can drift.
check("a masked table declares its FULL schema, derived from the staged frame",
      "_derived_schema_ddl(entity, active_bindings, spark, model)" in fac
      and "_stage(entity, binding, spark, model," in fac
      and ".schema.fields" in fac,
      "declaring only the masked columns is rejected: 'user-specified schema that is "
      "incompatible with the schema inferred from its query' (DEF-19)")
# AND IT READS IT IN BATCH. Deriving a schema through spark.readStream makes the analyser
# plan a whole streaming query on the driver per masked entity, before any flow is defined
# -- 27 minutes and no flow for raw_vault_pay_bill on 25 September. The frame is otherwise
# identical, so only the read changes, and only for the caller that wants a schema.
check("the schema is derived from a BATCH read, never a streaming one",
      "for_schema=True).schema.fields" in fac
      and "spark.read.table(src.bronze_table) if for_schema" in fac,
      "a schema does not need a stream; the flow bodies still stream, and they are lazy so "
      "they are not called while the graph is being built")
check("the derived schema carries nullability, not just names and types",
      "field.nullable" in fac and "NOT NULL" in fac,
      "load_dts is nullable=False; a declared schema that says otherwise is rejected")
check("bindings that disagree on a column raise instead of being widened",
      "disagree on column" in fac,
      "a silent widening writes a type nobody chose into an insert-only table")
check("derived identifiers are quoted",
      "_quote_ident" in fac, "GP delivers a column literally called `timestamp`")

# --------------------------------------------------------------------------- #
# DEF-26 (CRITICAL): the declared model is the table's shape.
#
# _stage appended the WHOLE staged Bronze frame, so a hub declaring four business keys
# shipped 92 columns and an NHL declaring fourteen payload columns shipped 83. That is
# the rule README.md already states -- "a hub carrying descriptive attributes" is under
# "what validation refuses to build" -- enforced on the declaration and broken in the
# implementation. It defeated the mask control outright, because _emit_target masks only
# an entity that DECLARES masks and a hub declares none:
#
#     nhl_general_journal_line   2,453,132 rows            0 readable debitamt
#     hub_accounting_journal     2,759,294 rows    2,759,292 readable debitamt
# --------------------------------------------------------------------------- #
# These three used to grep factory.py for exact function SIGNATURES. DEF-39 added an
# `extra` parameter to _project and DEF-41 refactored the ghost, so all three broke on a
# refactor that changed nothing they were asserting -- they were pinned to how the code is
# spelled, not to what it does. Re-pinning them to today's spelling only resets the clock,
# so they are parsed instead: `ast` sees the structure and survives reformatting, while
# still failing if the function is removed, renamed, or loses the argument that matters.
#
# The BEHAVIOUR behind all three is owned by tests/test_accelerator.py, which executes the
# flow bodies and inspects the resulting frames -- strictly stronger than reading source.
# What remains here is the structural precondition: the functions exist and are wired.
import ast as _ast  # noqa: E402

# Parse defensively. A SyntaxError here used to propagate and kill the run -- the same
# failure this file was just repaired for. The syntax check above already reports the
# error; this must degrade to a reported failure, not a stack trace that hides the
# remaining several hundred checks.
try:
    _fac_tree = _ast.parse(fac)
except SyntaxError as _exc:
    _fac_tree = _ast.parse("")
    bad("factory.py parses well enough to check its structure", str(_exc))
_fac_fns = {n.name: n for n in _ast.walk(_fac_tree)
            if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef))}


def _args_of(fn: str) -> list[str]:
    n = _fac_fns.get(fn)
    if n is None:
        return []
    a = n.args
    return [x.arg for x in (*a.posonlyargs, *a.args, *a.kwonlyargs)]


check("the factory parse found factory.py's functions at all",
      len(_fac_fns) > 20,
      f"parsed {len(_fac_fns)} function(s) -- a failed parse would make every structural "
      f"check below pass against an empty tree")
check("the factory projects every entity to its declared shape",
      "_projection" in _fac_fns and "_project" in _fac_fns
      and {"df", "entity", "src"} <= set(_args_of("_project")),
      "without a projection the whole source row lands on every vault table (DEF-26)")
check("the projection is decided from METADATA alone, with no Spark in reach",
      "def _projection(entity: Entity, src: SourceBinding) -> list[tuple[str, str, str]]"
      in fac,
      "a table's shape is a modelling fact; taking a frame here would make it "
      "un-assertable without a workspace")
check("_stage is the staged frame PROJECTED, which is what _derived_schema_ddl reads",
      "return _project(_stage_full(entity, src, spark, model, for_schema), entity, src)"
      in fac,
      "the declared schema must be the declared model's shape by construction, and the "
      "for_schema flag must reach the read rather than being dropped on the way")
check("the projection narrows in ONE select, not a chain of withColumn/drop",
      "return df.select(*columns)" in fac,
      "every expression must be evaluated against the input frame, or a business key "
      "whose model name collides with a source column reads an overwritten value")

# THE ORDER INSIDE THE FLOW IS LOAD-BEARING IN BOTH DIRECTIONS. Expectations are governed
# configuration and may name ANY source column, so the violation predicate must be
# evaluated while the full row is still in scope; and only the declared shape may land,
# so the narrowing has to happen before the append.
_valid_body = fac.split("def _valid(", 1)[1].split("def _invalid(", 1)[0]
check("the valid flow stages the FULL row, so an expectation can name any source column",
      "_stage_full(entity, src, spark, model)" in _valid_body,
      "projecting before the filter would break every expectation on an unmodelled column")
check("the valid flow projects LAST, so only the declared shape lands",
      "return _project(df, entity, src)" in _valid_body,
      "the append must receive the declared model's columns and nothing else")
# DEF-39 moved the reason columns INTO the _project call as `extra=`, because building
# them with withColumn afterwards resolved them against the wrong frame. So the property
# is no longer "projects, then adds" -- it is "the reasons go through the projection".
_invalid_calls = [n for n in _ast.walk(_fac_fns["_invalid"])
                  if isinstance(n, _ast.Call)] if "_invalid" in _fac_fns else []
_invalid_projects = [c for c in _invalid_calls
                     if isinstance(c.func, _ast.Name) and c.func.id == "_project"]
check("the quarantine flow projects, and its reason columns go THROUGH the projection",
      len(_invalid_projects) == 1
      and any(k.arg == "extra" for k in _invalid_projects[0].keywords),
      "a rejected row is the same source row, kept -- the twin must not be wider than "
      "the table it shadows, and a reason column added after the projection resolves "
      "against the wrong frame (DEF-39)")
check("_emit_quarantine declares a schema, so the twin carries the MASK clauses too",
      "def _emit_quarantine(entity: Entity, table: str," in fac
      and "kwargs[\"schema\"] = f\"{target}, {reasons}\"" in fac,
      "it passed no schema= at all, so quarantined rows were readable while the same "
      "rows in the target were masked (DEF-26)")

# DEF-25's lesson, which this change makes live again: declaring a schema converts every
# previously-cosmetic inconsistency between flows into a hard failure. The ghost is now
# built from the SAME field list the target declares, so the two cannot drift.
_ghost_calls = [n for n in _ast.walk(_fac_tree)
                if isinstance(n, _ast.Call) and isinstance(n.func, _ast.Name)
                and n.func.id == "_emit_ghost"]
check("the ghost is built from the declared field list, not a hand-kept subset",
      "_ghost_columns_sql" in _fac_fns and len(_ghost_calls) == 1
      and any(k.arg == "schema_fields" for k in _ghost_calls[0].keywords),
      "the union of 'the source flow supplies it' and 'the ghost does not' stops being "
      "a nullable column the moment the table declares its schema (DEF-25)")
check("a declared NOT NULL column the ghost cannot supply is refused at build time",
      "DELTA_MISSING_NOT_NULL_COLUMN_VALUE" in fac,
      "naming the column at definition time beats failing on the first append")
check("the ghost zero-keys a link's PARENT hash keys, not only its own",
      'name.endswith("_hk")' in fac,
      "a PIT join to the parent hub has to stay an equi-join")

# --------------------------------------------------------------------------- #
# THE ASSERTION THAT MATTERS MORE THAN THE PROJECTION. mask_survival_check walked only
# the entities that DECLARE masks, plus their _v1 views. It never asked whether a masked
# column NAME appears unmasked elsewhere in the schema, so it reported PASSED over a
# total bypass of the vault's only PII defence. The projection is the bug; this is why
# nothing caught it.
# --------------------------------------------------------------------------- #
_mask_gate = (ROOT / "checks" / "mask_survival_check.py").read_text(encoding="utf-8")
check("the mask gate asserts catalogue-wide, by column NAME",
      "def unmasked_elsewhere(" in _mask_gate,
      "walking only the entities that declare masks cannot see a masked column riding "
      "along on a table that declares none (DEF-26)")
check("the catalogue sweep reads every column in the schema, not only masked ones",
      "def schema_columns(" in _mask_gate
      and "information_schema.columns" in _mask_gate,
      "information_schema.column_masks alone can only confirm masks that exist")
check("quarantine twins are inside the sweep",
      "quarantine twins" in _mask_gate,
      "the twin is fed the identical frame, so it is the other half of the bypass")
check("the catalogue assertion is a pure function, so it can be fired offline",
      "def unmasked_elsewhere(declared_masked, columns, masked, is_exempt=None)"
      in _mask_gate,
      "a gate whose condition cannot be exercised without a workspace is a gate nobody "
      "proves non-vacuous")
check("a sweep that read ZERO columns is a failure, not a pass",
      "read ZERO columns" in _mask_gate,
      "an empty schema or a mistyped argument must never be reported as proof")
check("every exclusion states a reason, and the reasons are printed",
      "def exemption(table: str) -> str | None" in _mask_gate
      and "object(s) excluded: {_why}" in _mask_gate,
      "a reader has to be able to see what the gate declined to assert over, and why")
check("the SDP materialization exclusion is recorded as a LIMITATION, not a boundary",
      "RECORDED LIMITATION" in _mask_gate and "apply_governance" in _mask_gate,
      "the backing table under an already-masked streaming table is itself unmasked; "
      "that is a grant question, and saying so is the difference between a documented "
      "gap and a silent one")
_test_src = (ROOT / "tests" / "test_accelerator.py").read_text(encoding="utf-8")
check("the catalogue assertion is fired in BOTH directions offline",
      "unmasked_elsewhere" in _test_src
      and "hub_accounting_journal" in _test_src,
      "non-vacuity means firing it against the vault as DEF-26 left it, not only "
      "against the vault as the fix leaves it")
check("the same property is asserted at the METADATA level too",
      "no masked column NAME appears on a table that declares no mask for it"
      in _test_src,
      "the model can be refused before a lake is ever built")

# DEF-47 moved catalog isolation from ISSUED to ASSERTED, and this checker was not
# updated -- it went on looking for `SET ISOLATION MODE ISOLATED` in the governance SQL,
# raised ValueError on the missing substring, and took every check below it down with it.
# It has been broken on main since 26 Aug.
#
# The DEPENDENCY is unchanged and still real: PROD and TDS share one regional metastore,
# so separate catalogs are not separate access without isolation plus a workspace binding.
# What changed is who owns it. The ALTER is invalid on this runtime (PARSE_SYNTAX_ERROR,
# measured -- the one statement of 49 that failed) and the catalog is owned by the USNC
# Terraform service principal, so issuing it from here would fight another system's state
# and the last writer would win silently. Terraform sets it; we assert it.
#
# So this checks the assertion is WIRED, and that the ALTER has not crept back.
_job_yml = (ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8")
assert "task_key: assert_no_broad_grant" in _job_yml, \
    "assert_no_broad_grant task is gone -- this check has lost its subject"
# Slice to the next task at the SAME indentation. A bare "task_key:" split would stop at
# this task's own `depends_on: [{task_key: ...}]` line and read an empty body -- a check
# that fails for a reason that has nothing to do with what it is asserting.
_sgc_task = _job_yml.split("task_key: assert_no_broad_grant", 1)[1].split(
    "\n        - task_key:", 1)[0]
check(
    "catalog isolation is ASSERTED by a gate, not issued by the governance SQL",
    "--assert-isolated" in _sgc_task,
    "Terraform owns the isolation mode (DEF-47); if nothing asserts it, the catalog can "
    "silently stop being ISOLATED and prod data becomes queryable from TDS",
)
check(
    "and the gate that asserts it can actually refuse",
    "--assert-isolated" in (ROOT / "checks" / "schema_grant_check.py").read_text(
        encoding="utf-8"),
    "a flag the job passes and the checker ignores is a check that cannot fail",
)
check(
    "the governance SQL does NOT issue the isolation ALTER",
    "SET ISOLATION MODE" not in masks_exec,
    "it is invalid on this runtime and the catalog belongs to another system's state -- "
    "reissuing it would fight Terraform, and whichever ran last would win silently",
)

unused_fns = sorted(defined_fns - declared_fns)
if unused_fns:
    print(f"  note  functions defined but not yet bound to a column: {unused_fns}")


# Every ${placeholder} in the governance SQL must be supplied by the renderer.
#
# This used to infer the supplied names from the job's CLI flags with a naive
# "--privileged-group" -> "privileged_group" mapping, and so reported ${vault_privileged_group} as
# unresolved for ever. It is not: apply_governance.py binds it explicitly. The flag name
# and the placeholder name are allowed to differ -- the `bindings` dict is what decides,
# so the dict is what this reads. Inferring the contract instead of reading it made this
# check fail for a reason unrelated to the property it names.
gov_placeholders = set(re.findall(r"\$\{([a-z_]+)\}", masks_sql))
job_text = (ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8")
_ag_src = (ROOT / "checks" / "apply_governance.py").read_text(encoding="utf-8")
_bindings_block = _ag_src.split("bindings = {", 1)[1].split("}", 1)[0]
bound = dict(re.findall(r'"([a-z_]+)":\s*args\.([a-z_]+)', _bindings_block))
check(
    "the renderer's bindings dict was actually found and read",
    len(bound) >= 4,
    f"parsed {len(bound)} binding(s) -- if the dict moved, every check below reads an "
    f"empty contract and passes vacuously",
)
missing = sorted(p for p in gov_placeholders if p not in bound)
check(
    "every governance SQL placeholder is bound by the renderer",
    not missing,
    f"would render as a literal ${{...}} into a GRANT: {missing}",
)

# and the job must actually pass the argument behind every binding
_ag_task = job_text.split("task_key: apply_governance", 1)[1].split(
    "\n        - task_key:", 1)[0]
_needed = {a.replace("_", "-") for p, a in bound.items() if p in gov_placeholders}
_unpassed = sorted(f"--{f}" for f in _needed if f'"--{f}"' not in _ag_task)
check(
    "and the job passes the flag behind each one",
    not _unpassed,
    f"the renderer would fall back to a default or refuse: {_unpassed}",
)

# and the renderer must actually resolve them
import subprocess as _sp  # noqa: E402

_dry = _sp.run(
    [sys.executable, str(ROOT / "checks" / "apply_governance.py"),
     "--catalog", "02_usnc_silver_edm_dev",
     "--business-vault-schema", "business_vault",
     "--bronze-catalog", "01_usnc_bronze_dev",
     "--gold-catalog", "03_usnc_gold_edm_dev",
     # DEF-40 made --privileged-group required with NO default, so a wrong group can never be
     # granted silently. This dry-run was never updated and so failed on a parse error --
     # the group named here is a rendering placeholder, never applied.
     # A BARE IDENTIFIER on purpose: render() refuses to interpolate anything else into
     # DDL, so a hyphenated placeholder here fails the injection guard, not the check.
     "--privileged-group", "verify_repo_dry_run_group",
     "--dry-run"],
    capture_output=True, text=True,
)
check("governance SQL renders with no unresolved placeholders", _dry.returncode == 0,
      _dry.stdout[-300:] + _dry.stderr[-300:])
_n = [l for l in _dry.stdout.splitlines() if "statement(s) rendered" in l]
if _n:
    print(f"        {_n[0].strip()}")


# --------------------------------------------------------------------------- #
print("\n[7a] ERD is generated from the metadata, not maintained by hand")

erd_src = (ROOT / "tools" / "render_erd.py").read_text(encoding="utf-8")
check("the ERD generator reads the entity metadata",
      "spec.load_model" in erd_src,
      "a hand-maintained diagram drifts; a generated one can only be stale")
check("it renders satellites per source", "e.tables()" in erd_src or "tables()" in erd_src)
check("it flags aggregate grain", "aggregate" in erd_src)
check("it marks masked columns", "masks" in erd_src)
erd_html = ROOT / "docs" / "hfig_dv_erd.html"
erd_pdf = ROOT / "docs" / "hfig_dv_erd.pdf"
if erd_html.exists() and erd_pdf.exists():
    rendered = erd_html.read_text(encoding="utf-8")
    from accelerator import VERSION as _V
    check("the rendered ERD matches the current version", f"v{_V}" in rendered,
          "re-run tools/render_erd.py after changing the model")
    tables_now = {t for e in model.entities for _s, t in e.tables()}
    missing = sorted(t for t in tables_now if t not in rendered)
    check("every current table appears in the rendered ERD", not missing,
          f"absent from docs/hfig_dv_erd.html: {missing[:5]} -- the diagram is stale")

    # A TABLE THAT NO LONGER EXISTS MUST NOT STILL BE DRAWN. The check above catches a
    # MISSING table; it says nothing about a table that was renamed or deleted, so the ERD
    # could keep showing sat_job_request_details_fieldglass_eu for ever -- and did, in the
    # committed .dot, for ten days after that binding was renamed.
    _erd_prefixes = tuple(sorted(naming.PREFIX.values()))
    _erd_drawn = {m for m in re.findall(r"\b(?:" + "|".join(_erd_prefixes) + r")[a-z0-9_]+",
                                        rendered)}
    _erd_gone = sorted(t for t in _erd_drawn if t not in tables_now)
    check("the rendered ERD draws no table the model has dropped or renamed",
          not _erd_gone,
          f"still drawn but not in the model: {_erd_gone[:5]} -- re-run "
          f"tools/render_erd.py. A diagram showing a table nobody can query is worse than "
          f"one missing a table, because it is believed")

    # THE PDF IS GATED THROUGH ITS PROVENANCE, because it cannot be gated through its bytes:
    # wkhtmltopdf stamps /CreationDate, so two renders of identical input differ. Verified.
    #
    # WHY THIS EXISTS. For ten days the PDF sat at its 25 August content while the model
    # moved under it, and this section required only that the file EXIST. An artefact the
    # gate cannot see is an artefact that drifts -- the same lesson the mapping workbook
    # taught on 4 Sep, one layer along.
    _erd_prov_path = ROOT / "docs" / "hfig_dv_erd.provenance.json"
    try:
        _erd_prov = json.loads(_erd_prov_path.read_text(encoding="utf-8"))
        _erd_prov_err = ""
    except Exception as _exc:  # noqa: BLE001
        _erd_prov, _erd_prov_err = {}, f"{type(_exc).__name__}: {_exc}"
    check("the ERD records the provenance of its PDF",
          not _erd_prov_err and "html_sha256" in _erd_prov,
          f"{_erd_prov_err or 'no html_sha256'} -- run tools/render_erd.py. Without it the "
          f"PDF is ungateable and will drift again")
    if "html_sha256" in _erd_prov:
        import hashlib as _hl

        _erd_html_now = _hl.sha256(erd_html.read_bytes()).hexdigest()
        check("the committed PDF was rendered from the committed HTML",
              _erd_prov["html_sha256"] == _erd_html_now,
              f"provenance records {_erd_prov['html_sha256'][:16]}… but the HTML now hashes "
              f"to {_erd_html_now[:16]}… -- the PDF is STALE. Re-run tools/render_erd.py; it "
              f"needs wkhtmltopdf on PATH")

    # NO PAGE OF THE PDF MAY BE BLANK, and this is the check the digest could not be.
    #
    # A MATCHING DIGEST PROVES FRESHNESS, NOT THAT THE RENDER WORKED. It did not: `width:auto`
    # on an inline SVG resolves to ZERO in wkhtmltopdf's QtWebKit -- it does not implement SVG
    # intrinsic sizing from viewBox -- so the PDF carried SIX BLANK PAGES where the diagrams
    # should be, while its provenance digest matched perfectly and the same HTML rendered
    # correctly in a browser. Nobody noticed from 25 August until it was opened.
    #
    # The floor is deliberately low. A domain diagram with few tables legitimately carries
    # only a few hundred characters (305 on the smallest, measured); a page whose SVG failed
    # to render carries 15 to 20. Anything under 100 is a page with a heading and nothing
    # under it.
    _erd_pages = _erd_prov.get("pdf_page_chars") or []
    _erd_blank = [i + 1 for i, n in enumerate(_erd_pages) if n < 100]
    check("no page of the rendered PDF is blank",
          bool(_erd_pages) and not _erd_blank,
          f"page(s) {_erd_blank} carry under 100 characters "
          f"(all pages: {_erd_pages}) -- a diagram did not render. The usual cause is CSS "
          f"that gives an inline SVG an `auto` dimension, which QtWebKit resolves to zero "
          f"while every browser resolves it correctly")

    # AND THE DIAGRAMS MUST NAME EVERY ONE OF THE MODEL'S TABLES. A page can be non-blank
    # and still be wrong -- headings and legend text alone would clear the floor above.
    # The inventory at the back names every table, so this is a check on the whole
    # document; the per-page check above is what catches a failed diagram.
    #
    # EXACT, NOT 80%. It was `>= len(tables_now) * 0.8` and it hid a real defect for as
    # long as the link prefix has existed: render_erd.py scanned the PDF with a HARDCODED
    # alternation reading `...|hal|link)_`, but the link prefix is `lnk_`, so
    # lnk_client_job_request could never be counted. 27 of 28 sailed past a 22.4 floor.
    # The table had rendered correctly the whole time -- it was the measurement that was
    # blind, which is worse, because a link table that genuinely failed to render would
    # have produced exactly the same number. render_erd.py now derives the alternation
    # from naming.PREFIX.
    #
    # If this ever fires on a correct PDF, suspect line-wrapping in the inventory: the
    # longest name is 52 characters and the page is A3 landscape, so it does not wrap
    # today, but a longer entity name could. That is a real answer to investigate, not a
    # reason to soften this back into a percentage.
    _erd_named = _erd_prov.get("pdf_table_names", 0)
    check("the rendered PDF names every one of the model's tables",
          _erd_named == len(tables_now),
          f"{_erd_named} table name(s) extractable from a model of {len(tables_now)} -- "
          f"re-run tools/render_erd.py --force-pdf, and if the count is still short, find "
          f"which table is missing before assuming the renderer is at fault")
else:
    print("  note  docs/hfig_dv_erd.{html,pdf} not present -- run tools/render_erd.py")

# NO .dot MAY BE COMMITTED, and this sits OUTSIDE the block above on purpose: an orphaned
# intermediate must be caught whether or not the ERD itself rendered.
#
# render_erd unlinks its own graphviz intermediates (see render(), the dot_path.unlink()
# call), so any .dot under docs/ is a leftover from before that cleanup existed --
# regenerated by nothing, gated by nothing, and free to name tables that no longer exist.
# One did, from 25 August until 4 September, still naming a renamed binding.
_erd_stray_dot = sorted(f.name for f in (ROOT / "docs").glob("*.dot"))
check("no graphviz intermediate is committed under docs/",
      not _erd_stray_dot,
      f"{_erd_stray_dot} -- render_erd deletes its own .dot files; a committed one is an "
      f"orphan nothing regenerates")


# --------------------------------------------------------------------------- #
print("\n[7c] capacity inputs are declared, and declared as placeholders")

vol_path = ROOT / "metadata" / "volumes.yml"
volumes = yaml.safe_load(vol_path.read_text(encoding="utf-8"))
declared = set(volumes.get("entities", {}))
modelled = {e.name for e in model.entities}
check("every modelled entity has a declared volume", modelled <= declared,
      f"no volume for: {sorted(modelled - declared)} -- excluded from any estimate")
check("volumes.yml is labelled as placeholders",
      "PLACEHOLDER" in vol_path.read_text(encoding="utf-8"),
      "an unlabelled guess gets quoted as a number to a capacity request")
check("the PIT grain is explicit", volumes.get("pit", {}).get("grain") in
      ("daily", "weekly", "month_end"))
est = (ROOT / "tools" / "estimate_footprint.py").read_text(encoding="utf-8")
check("the estimator derives structure from the model, not from volumes.yml",
      "spec.load_model" in est,
      "column counts and key widths must come from the real tables")


# --------------------------------------------------------------------------- #
print("\n[7b] versioning")

from accelerator import VERSION  # noqa: E402

check("accelerator version is set", bool(VERSION) and VERSION[0].isdigit(), VERSION)
changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
check(f"CHANGELOG documents v{VERSION}", f"## v{VERSION}" in changelog,
      "a released version with no changelog entry is untraceable across 8 workspaces")
fac_src = (ROOT / "src" / "accelerator" / "factory.py").read_text(encoding="utf-8")
check("the version is stamped onto generated tables",
      "hfig.accelerator.version" in fac_src,
      "when two lakes disagree, the first question is whether the same code built them")
check("the grain is stamped onto generated tables", '"hfig.grain"' in fac_src)


# --------------------------------------------------------------------------- #
print("\n[8] hash rulebook: ratified values and golden vectors")

check("algorithm is ratified", hashing.ALGORITHM == hashing.ALGORITHM_RATIFIED)
check("hashdiff case handling is ratified",
      hashing.HASHDIFF_UPPERCASE == hashing.HASHDIFF_UPPERCASE_RATIFIED)

vectors = json.loads((ROOT / "tests" / "golden_hash_vectors.json").read_text(encoding="utf-8"))
rb = vectors["rulebook"]
check("golden vectors match the current rulebook",
      rb["algorithm"] == hashing.ALGORITHM
      and rb["version"] == hashing.RULEBOOK_VERSION
      and rb["hashdiff_uppercase"] == hashing.HASHDIFF_UPPERCASE,
      f"vectors={rb}")
drift = [c["name"] for c in vectors["keys"]
         if hashing.reference_key(c["values"], source_scope=c["source_scope"]) != c["sha256_hex"]]
drift += [c["name"] for c in vectors["hashdiffs"]
          if hashing.reference_hashdiff(c["values"]) != c["sha256_hex"]]
check("all golden digests reproduce", not drift, f"drifted: {drift}")

# only hashing.py may build a hash expression
for f in py_files:
    if "__pycache__" in str(f) or f.name in ("hashing.py", "verify_repo.py", "test_accelerator.py"):
        continue
    text = f.read_text(encoding="utf-8")
    offenders = [t for t in ("SHA2(", "SHA1(", "XXHASH64(", "MD5(") if t in text]
    check(
        f"{f.relative_to(ROOT)} builds no hash of its own",
        not offenders,
        f"found {offenders} -- only hashing.py may construct hash expressions",
    )


# --------------------------------------------------------------------------- #
print("\n[9] unit tests still pass")

import subprocess

result = subprocess.run(
    [sys.executable, str(ROOT / "tests" / "test_accelerator.py")],
    capture_output=True, text=True,
)
# Ruling 2 fix: result.stdout.count("PASS") over-counted by one, because the
# suite's final "ALL CHECKS PASSED" line also contains the substring "PASS".
# Count the delimited "  PASS  " form each individual check line actually
# emits instead -- this is the root cause of three different wrong counts
# being reported for the same suite (README, CHANGELOG, and this script).
passed = result.stdout.count("  PASS  ")
check("test_accelerator.py exits clean", result.returncode == 0, result.stdout[-400:])
print(f"        {passed} unit checks passed")


# --------------------------------------------------------------------------- #
# GOVERNANCE SQL: every interpolated identifier must be quoted.
#
# The estate's catalogs start with a digit (02_usnc_silver_edm_dev). Spark SQL
# unquoted identifiers must match [a-zA-Z_][a-zA-Z0-9_]*, so an unquoted
# ${catalog} produces DDL that will not parse. naming.py:94 already backticks;
# this file did not.
#
# The check is on EVERY ${...} placeholder, not on ${catalog} by name: more
# placeholders are quoted than a name-specific check would cover, and renaming
# one (bronze_schema -> bronze_catalog) would silently drop it out of scope.
# --------------------------------------------------------------------------- #
print("\n[governance] interpolated identifiers are quoted in apply_masks.sql")

_masks_sql = (ROOT / "governance" / "apply_masks.sql").read_text()
# Skip comment lines: line 39 shows a `databricks workspace-bindings` shell
# example containing a bare ${catalog}, which is not SQL and must stay bare.
_sql_lines = [l for l in _masks_sql.splitlines() if not l.lstrip().startswith("--")]
_unquoted = re.findall(r"(?<!`)\$\{[a-z_]+\}(?!`)", "\n".join(_sql_lines))
check(
    "every ${...} placeholder in apply_masks.sql is backticked",
    not _unquoted,
    f"{len(_unquoted)} unquoted occurrence(s) {sorted(set(_unquoted))}; a "
    f"leading-digit catalog will not parse",
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

# statements() splits apply_masks.sql on ";", so a semicolon inside a -- comment
# cuts that comment in half and emits its tail as a statement. That renders as
# valid-looking output and fails at 03:00 against a real workspace, so assert here
# that every statement the splitter produces actually starts like SQL.
_stmts = _ag.statements(
    _ag.render(
        _masks_sql,
        # vault_privileged_group must be here too: render() refuses an unresolved placeholder
        # by EXITING, so omitting it killed the script after the last PASS line and
        # printed no FAIL -- the run looked like it had simply ended. Bare identifier,
        # because render() will not interpolate anything else into DDL.
        {"catalog": "02_usnc_silver_edm_dev", "vault_schema": "raw_vault",
         "business_vault_schema": "business_vault",
         "bronze_catalog": "01_usnc_bronze_dev", "gold_catalog": "03_usnc_gold_edm_dev",
         "vault_privileged_group": "verify_repo_dry_run_group"},
    )
)
_KEYWORDS = ("ALTER", "USE", "CREATE", "GRANT", "REVOKE", "DROP", "SET")
_not_sql = [st.splitlines()[0][:60] for st in _stmts
            if not st.lstrip().upper().startswith(_KEYWORDS)]
check(
    "every statement split out of apply_masks.sql begins with SQL",
    not _not_sql,
    f"{_not_sql} -- a semicolon inside a -- comment splits it into a bogus statement",
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

# Ruling 1 fix: the naive version of this check reads verify_repo.py's own source
# into _vr and then asserts a literal host string is absent from it -- but that
# literal appears right here, in the check's own source, which is part of the file
# being read. The condition can never be true. Scan only the preflight self-test
# invocation lines (--actual-host), not the whole file.
_vr_lines = (ROOT / "verify_repo.py").read_text().splitlines()
_actual_host_lines = [l for l in _vr_lines if "--actual-host" in l]
check(
    "preflight self-tests target usnc, not weu/uks",
    any("adb-2593897084138079" in l for l in _actual_host_lines)
    and not any("adb-7405615198748199" in l for l in _actual_host_lines),
    "verify_repo.py still self-tests against weu_tds/uks_tds hosts (DEF-7)",
)


# --------------------------------------------------------------------------- #
# DOCUMENTATION: a hand-maintained count drifts. README stated 151 and 266 (and
# separately 121 for test_accelerator.py), CHANGELOG stated 238 (and separately
# 121), for two scripts that already print their own counts. Ruling 1: the
# test_accelerator.py "121 checks" claim is not consistent either -- the suite
# now emits 123 "  PASS  " lines -- so it is removed like the others rather
# than corrected to 123, which would only drift again next time a check is
# added or removed.
# --------------------------------------------------------------------------- #
print("\n[docs] no hand-maintained counts, no unresolved placeholders")

_readme = (ROOT / "README.md").read_text()
_changelog = (ROOT / "CHANGELOG.md").read_text()

check(
    "README does not hardcode a check count for either script",
    not re.search(r"\d+\s+(repo-integrity\s+)?checks", _readme),
    "the count is printed at runtime; a literal in prose will drift",
)
check(
    "CHANGELOG does not hardcode a check count for either script",
    not re.search(r"\d+\s+(offline\s+)?checks", _changelog),
    "the count is printed at runtime; a literal in prose will drift",
)
# DEPLOY.md was outside this rule and had drifted furthest: it promised "92 checks" and
# "67 checks" against actual counts of 598 and 516. It is the runbook an operator follows
# against production, so a number in it that no longer matches teaches them to ignore the
# number -- and then to ignore the gate.
_deploy = (ROOT / "DEPLOY.md").read_text(encoding="utf-8")
check(
    "DEPLOY.md does not hardcode a check count for either script",
    # (?!/) so "python3 checks/foo.py" -- which contains the literal "3 checks" -- is
    # not reported as a hardcoded count. That is a real defect, but it is the NEXT
    # check's defect, and a misleading message sends the reader to the wrong fix.
    not re.search(r"\d+\s+(repo-integrity\s+|offline\s+)?checks\b(?!/)", _deploy),
    "the count is printed at runtime; a literal in prose will drift",
)
# TWO SURFACES ESCAPED THIS RULE FOR THREE WEEKS, and both were stale when checked on
# 5 September 2026. The skill file said `tests/test_accelerator.py` had 121 checks against
# 1,129 -- an order of magnitude, in a file loaded into every session as authority.
# OPEN_ITEMS' suite table said verify_repo ran 1,000; it was corrected that morning and was
# stale again by the evening, because checks were added in between. That is the ruling
# above proving itself twice: a hand-maintained count does not need to be wrong when
# written, only to survive a day.
#
# THE FIX IS THE SAME ONE README, CHANGELOG AND DEPLOY.md GOT -- remove the number, not
# correct it. Each script prints its own count, and the run command is what a reader
# actually needs anyway.
#
# NAMED EXPLICITLY RATHER THAN GLOBBED. A directory glob over docs/ would swallow
# okf_graphify_evaluation.md and DECISION_LOG.md, where a count is EVIDENCE about a past
# defect rather than a claim about today -- and an exemption broad enough to cover those
# would be broad enough to hide the next OPEN_ITEMS. Adding a surface here is deliberate.
_COUNT_CLAIM = re.compile(r"\d[\d,]*\s+(repo-integrity\s+|offline\s+|pure-Python\s+)?checks\b(?!/)")
for _rel in (".claude/skills/dv-accelerator-gates/SKILL.md",
             "docs/superpowers/OPEN_ITEMS.md",
             # Found 6 Sep, the day after the ban: pyproject's comment said the offline
             # suite runs "121 structural checks" against 1,129. A third escapee, and the
             # reason this list is explicit rather than a glob is that each addition is a
             # deliberate reading of where a count is a CLAIM about today.
             "pyproject.toml"):
    _text = (ROOT / _rel).read_text(encoding="utf-8")
    _hits = [m.group(0) for m in _COUNT_CLAIM.finditer(_text)]
    check(f"{_rel} does not hardcode a check count",
          not _hits,
          f"{_hits[:3]} -- each script prints its own count. This file is read as current "
          f"state, so a number in it that has drifted is worse than no number: it is "
          f"believed. State the run command instead")

# Same file, same class of rot: the runbook must invoke the gates the way that WORKS.
# `python3 verify_repo.py` fails on a clean machine (no pyyaml in the ambient
# environment), which made the runbook's very first instruction a dead end.
_bare = re.findall(r"^python3 (?:checks|tools|governance)/\S+", _deploy, re.M) \
    + re.findall(r"^python3 (?:verify_repo|tests/)\S*", _deploy, re.M)
check(
    "DEPLOY.md invokes the gates through uv, not a bare python3",
    not _bare,
    f"these fail on a machine without pyyaml installed globally: {_bare}",
)
check(
    "README carries no unresolved version placeholder",
    "<X.Y.Z>" not in _readme,
    "DEF-5: the Superpowers pin line was committed with a literal placeholder",
)

# DEF-57: README.md was committed carrying `<<<<<<< Updated upstream` / `=======` /
# `>>>>>>> Stashed changes` from a stash pop, and nothing noticed for days. The version
# placeholder above was a SYMPTOM -- it was the stale half of that conflict -- so
# checking for the symptom alone would have missed the cause, and would keep missing it
# in any other file. Conflict markers are checked across every tracked text file.
#
# Anchored at column 0 with the trailing space/EOL that git writes, so prose ABOUT
# conflict markers does not trip it -- this very file contains the strings.
_CONFLICT = re.compile(r"^(<{7}|={7}|>{7})(\s|$)", re.M)
_conflicted = []
# Same walk as the parse gates above -- repo_files carries the one exclusion set, so this
# sweep can no longer disagree with them about what belongs to this repo.
for _f in sorted(repo_files(".py") + repo_files(".md") + repo_files(".yml")
                 + repo_files(".sql") + repo_files(".json")):
    if _f.resolve() == pathlib.Path(__file__).resolve():
        continue
    try:
        _txt = _f.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        continue
    if _CONFLICT.search(_txt):
        _conflicted.append(str(_f.relative_to(ROOT)))
check(
    "no tracked file carries an unresolved merge-conflict marker",
    not _conflicted,
    f"committed conflict markers in: {_conflicted}",
)
# DEF-5 is "a doc cites a path that does not exist". The first guard for it was
# hardcoded to the single path DEF-5 happened to name (tooling/superpowers), so when
# the DEF-5 FIX itself cited `.claude/settings.json` -- untracked in this branch --
# the guard saw nothing. Generalise: lift the Superpowers block out of the README and
# assert that every repo-relative path it cites actually resolves on disk.
_readme_lines = _readme.splitlines()
_sp_start = next((i for i, l in enumerate(_readme_lines)
                  if l.startswith("Superpowers methodology layer")), None)
_sp_block = ""
if _sp_start is not None:
    _rest = _readme_lines[_sp_start:]
    _sp_end = next((j for j, l in enumerate(_rest) if j and not l.strip()), len(_rest))
    _sp_block = "\n".join(_rest[:_sp_end])
check(
    "README still carries the Superpowers methodology block",
    bool(_sp_block),
    "the block naming the plugin pin and the project-local gate skill is gone -- "
    "the path guard below has nothing to check",
)

# a backticked token is a repo-relative path if it carries a separator and is not a
# URL, an absolute path, or a plugin coordinate like name@marketplace
_REPO_PATH = re.compile(r"^(?!/)(?!https?:)[\w.@-]+(?:/[\w.@-]+)+/?$")
_cited_paths = [c for c in re.findall(r"`([^`]+)`", _sp_block) if _REPO_PATH.match(c)]
_absent_paths = [c for c in _cited_paths if not (ROOT / c.rstrip("/")).exists()]
check(
    "the README cites at least one repo-relative path there, so the guard bites",
    bool(_cited_paths),
    "no path-shaped backticked token found; the absence check below would be vacuous",
)
check(
    "every repo-relative path the README's Superpowers block cites exists",
    not _absent_paths,
    f"DEF-5 class: README points at {_absent_paths}, absent from the repo",
)


# --------------------------------------------------------------------------- #
# THE STOP-GAP CREDENTIAL FILE MUST STAY INVISIBLE TO BOTH GIT AND THE BUNDLE.
#
# Until PLT-7 provides a Key Vault-backed secret scope, the Workday password may sit in a
# file so a person can test the retrieval without retyping it. That is a reasonable
# stop-gap and it rests entirely on one .gitignore line, which is why the line is asserted
# rather than trusted.
#
# ONE RULE COVERS TWO EXPOSURES, and the second is the one people forget. Databricks Asset
# Bundles exclude .gitignore'd paths from the deploy upload, so the same line that keeps the
# file out of a commit keeps it out of /Workspace/Shared/.bundle -- a folder that grants
# CAN_MANAGE to `users` by inheritance until PLT-3 lands. Delete the line and a credential
# file becomes readable by every workspace user on the next deploy, with nothing failing.
_CRED_PATHS = (".workday-credentials", ".workday/")
_gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
_cred_unignored = [name for name in _CRED_PATHS if name not in _gitignore]
check("the credential stop-gap paths are still git-ignored",
      not _cred_unignored,
      f"{_cred_unignored} absent from .gitignore -- that single line is what keeps a "
      f"credential file out of BOTH the commit and the bundle upload, and the bundle root "
      f"is world-writable until PLT-3 lands")

# AND NOTHING MATCHING THEM IS TRACKED. The ignore rule does nothing for a file already in
# the index, which is exactly how a secret gets committed once and stays.
_cred_tracked = [f for f in (_repo_inventory() or set())
                 if f.name in ("Any", ".workday-credentials")
                 or f.relative_to(ROOT).as_posix().startswith(".workday/")]
check("no credential stop-gap file is tracked",
      not _cred_tracked,
      f"{[str(f) for f in _cred_tracked]} -- an ignore rule does not apply to a file "
      f"already in the index. Remove it from git history, not just from disk")


# --------------------------------------------------------------------------- #
# THE DECISIONS PAGE ONLY CITES; IT MUST NOT OUTLIVE WHAT IT CITES.
#
# docs/business_legal_decisions.html summarises, for one programme manager, every ask that
# needs a business, legal or finance decision. It owns none of them: each LEG-, WDJ- and
# BRZ- id belongs to the document that raised it, with the evidence and the reasoning.
#
# THAT MAKES IT THE MOST FRAGILE DOCUMENT HERE. A summary of other documents drifts the
# moment one of them is renumbered or an ask is withdrawn -- and unlike a stale measurement,
# a stale CITATION is invisible: the reader follows it, finds nothing, and cannot tell
# whether the question was answered or the reference was wrong.
#
# WHAT THIS CANNOT DO, so nobody reads it as stronger than it is: it cannot tell whether a
# decision has been MADE. The live state of each is on the board. This only asserts that
# every id the summary names still exists in the document that owns it.
_DEC = ROOT / "docs" / "business_legal_decisions.html"
if _DEC.is_file():
    _dec_text = _DEC.read_text(encoding="utf-8")
    _dec_owners = {
        "LEG": ROOT / "docs" / "legal_entity_questions.html",
        "WDJ": ROOT / "docs" / "workday_journal_export_analysis.html",
        "BRZ": ROOT / "docs" / "bronze_layer_work_requests.html",
    }
    _dec_cited = {m.group(0) for m in re.finditer(r"\b(?:LEG|WDJ|BRZ)-\d+\b", _dec_text)}
    _dec_dangling = []
    for _cited in sorted(_dec_cited):
        _family = _cited.split("-", 1)[0]
        _owner = _dec_owners[_family]
        # The id must appear in the OWNING document as a raised ask, not merely mentioned.
        if f'<span class="id">{_cited}</span>' not in _owner.read_text(encoding="utf-8"):
            _dec_dangling.append(f"{_cited} (owner {_owner.name})")
    check("the decisions summary cites at least the three families, so it is not vacuous",
          len({c.split('-')[0] for c in _dec_cited}) == 3 and len(_dec_cited) >= 10,
          f"{sorted(_dec_cited)} -- a summary citing one family or a handful of ids is not "
          f"the cross-cutting view it claims to be, and the check below would pass on almost "
          f"nothing")
    check("every id the decisions summary cites is still a raised ask in its own document",
          not _dec_dangling,
          f"{_dec_dangling} -- this page owns no ids and only cites them. A citation the "
          f"reader cannot follow is worse than none: they find nothing and cannot tell "
          f"whether the question was answered or the reference was wrong")


# --------------------------------------------------------------------------- #
# THE PLANNED WORKDAY BINDINGS MUST NAME COLUMNS THE LANDING WRITER ACTUALLY EMITS.
#
# Task 4 of docs/superpowers/plans/2026-09-07-workday-reference-source.md writes a
# bronze_table name into four entity YAMLs, with key_columns and payload naming columns the
# landed table must have. Nothing would catch a rename on one side: the mismatch surfaces at
# pipeline DEFINITION time, in a workspace, on a deploy -- which is the most expensive place
# and the furthest from whoever renamed it.
#
# ASSERTED NOW, WHILE BOTH SIDES ARE STILL CHEAP TO CHANGE. Neither the binding nor the
# landed table exists yet, which is exactly why this is the moment: agreeing the column names
# before either is built costs a list, and disagreeing later costs a deploy.
#
# THIS LIST IS DELETED IN TASK 4 and replaced by reading the bindings themselves. A hand-kept
# list that outlives the moment it was needed is the second authority this repo keeps
# removing -- it is written here with its own expiry, and the check below enforces that the
# expiry is honoured.
from accelerator import workday as _wdmod  # noqa: E402

_WD_LANDED = (set(_wdmod.REFERENCE_COLUMNS) | set(_wdmod.ORGANISATION_COLUMNS)
              | set(_wdmod.MEMBERSHIP_COLUMNS))
# DERIVED FROM THE BINDINGS, NOT HAND-KEPT -- the expiry this list carried has been
# honoured. It was a dict of columns four not-yet-written Workday bindings were expected to
# read, written while neither side existed, with its own instruction: "THIS LIST IS DELETED
# IN TASK 4 and replaced by reading the bindings themselves."
#
# IT EXPIRED ON 25 September, though not the way it expected. The trigger it watched for was
# "a legal-entity binding stops naming PLACEHOLDER", which it read as Task 4 arriving. What
# actually happened is that the CLIENT half of the family found a different source entirely:
# hubspot_raw.companies. So two of its three keys -- legal_entity and
# client_legal_entity_hierarchy -- stopped describing anything planned at all, because those
# entities are now sourced, from a system the list was never about.
#
# The Workday-side bindings DO exist as placeholders and DO declare their columns, so there
# is nothing left to hand-keep: the planned columns are read off the bindings that will read
# them. A rename on either side now fails here by construction rather than by someone
# remembering to edit a dict.
_WD_SHAPES = ("wd_organization", "wd_organization_membership", "wd_reference_id")
_wd_planned_cols: dict = {}
for _e in model.entities:
    for _s in _e.sources:
        if (_s.bronze_table or "").split(".")[-1] not in _WD_SHAPES:
            continue
        _cols = set(_s.key_columns or ()) | set(_s.payload or ())
        _cols |= {_c for _p, _cc in _s.parent_keys for _c in _cc}
        if _s.applied_dts_column:
            _cols.add(_s.applied_dts_column)
        _wd_planned_cols[f"{_e.name}/{_s.name}"] = _cols

# THE PROVENANCE COLUMNS STAY DECLARED, because no binding names them yet and they are the
# one set whose absence would not show up above. dedup_order has nothing to order a later
# retrieval by without retrieved_at, and a row with no host or tenant cannot say which
# Workday tenant asserted it.
_wd_planned_cols["provenance (no binding names these yet)"] = {
    "host", "tenant", "wws_version", "retrieved_at"}

_wd_unlandable = {name: sorted(cols - _WD_LANDED)
                  for name, cols in _wd_planned_cols.items() if cols - _WD_LANDED}
check("every column the planned Workday bindings will read exists in a landed shape",
      not _wd_unlandable,
      f"{_wd_unlandable} -- these names are written into a source binding; a column the "
      f"landing writer does not emit fails at pipeline definition time on a deploy, rather "
      f"than here where both sides are still a text edit")

# A FLOOR, because a derivation that matches nothing subtracts to nothing and passes by
# comparing nothing -- the same vacuity the hand-kept version had to guard against, in its
# new form. If the Workday bindings are repointed or renamed away, this fails and says so
# rather than going quietly green.
_wd_binding_keys = [k for k in _wd_planned_cols if not k.startswith("provenance")]
check("the planned-binding column check is not vacuous",
      len(_WD_LANDED) >= 12 and len(_wd_binding_keys) >= 5
      and sum(len(_wd_planned_cols[k]) for k in _wd_binding_keys) >= 10,
      f"{len(_WD_LANDED)} landed column(s) against {len(_wd_binding_keys)} binding(s) "
      f"declaring {sum(len(_wd_planned_cols[k]) for k in _wd_binding_keys)} column(s) -- if "
      f"either side emptied, the check above would pass having compared nothing")

# THE WORKDAY HALF MUST STILL BE UNSOURCED, which is what remains to assert now that the
# hand-kept list is derived. Landing is gated on a destination nobody owns --
# tools/land_workday_references.py writes NDJSON and says so itself -- and
# 01_usnc_bronze_dev holds no Workday schema at all. A consolidation binding naming a real
# table means that changed, and everything above it was written on the assumption it had
# not.
#
# THE SWEEP LIVES IN accelerator.hierarchy AND BOTH SUITES CALL IT.
# It used to read entity.name.startswith("legal_entity") here. On 24 September the client
# hierarchy entities were renamed to client_legal_entity_hierarchy*, which does not start
# with that, so they dropped out of the sweep and this gate went on passing over entities
# it had stopped looking at -- OBSERVED, by repointing one and watching this check stay
# green. It was then corrected in place, and review found the correction was ALSO unpinned:
# the test rebuilt the predicate locally, so reverting this line left both suites green.
# One definition, two callers, and tests/test_accelerator.py exercises that definition
# directly -- so a revert now has nowhere to hide.
from accelerator import hierarchy as _hy_sweep  # noqa: E402

_wd_family = [e.name for e in model.entities if _hy_sweep.in_legal_entity_family(e.name)]
check("the legal-entity expiry sweep actually matches entities",
      len(_wd_family) >= 9,
      f"{sorted(_wd_family)} -- if the family predicate matches nothing, the check below "
      f"passes having compared nothing, which is green and blind")

_wd_unsanctioned = _hy_sweep.unsanctioned_legal_entity_repoints(model.entities)
check("no legal-entity binding leaves PLACEHOLDER without a decision behind it",
      not _wd_unsanctioned,
      f"{_wd_unsanctioned} names a real table and is not on "
      f"hierarchy.SOURCED_LEGAL_ENTITY_BINDINGS. Either add it there with the reasoning, or "
      f"this is an accidental repoint -- which is what this sweep exists to catch")

_wd_unhonoured = _hy_sweep.unhonoured_sourced_bindings(model.entities)
check("every binding the allow-list sanctions is actually sourced",
      not _wd_unhonoured,
      f"{_wd_unhonoured} -- sanctioned as deliberately repointed and is not, so the "
      f"allow-list now describes something that does not exist and the check above passes "
      f"by subtracting names nothing will ever produce")


# --------------------------------------------------------------------------- #
# NO LIVE FILE MAY POINT AT A docs/ FILE THAT IS NOT THERE.
#
# DEF-5's class is "a doc cites a path that does not exist", and the guard above covers
# exactly one block of README.md -- which is where DEF-5 happened to be found. On
# 6 September five documents in docs/ were deleted in one commit and four live files
# pointed into them: databricks.yml, governance/apply_masks.sql, a runbook, and the
# board. Nothing here would have said so. They were repointed by hand and checked with
# grep, which is not a gate.
#
# HISTORY IS EXEMPT AND THAT IS THE WHOLE DESIGN. docs/superpowers/ holds the decision
# log, the specs and the plans; they record what was true when written and MUST keep
# naming files that have since been renamed or retired. Rewriting them to keep a link
# alive would falsify the record to satisfy a link checker. So the sweep covers live
# files only, and a citation from history is left alone.
#
# THE LOOKBEHIND IS NOT COSMETIC. Without it this matched the tail of every external
# documentation URL a vendored skill pack quotes -- `https://mlflow.org/docs/latest/...`
# -- and reported fifteen "dangling" paths that are live pages on somebody else's site.
# A repo-relative citation starts at a boundary; a URL's path segment does not.
_DOC_CITE = re.compile(r"(?<![\w./-])docs/[\w./-]+\.(?:md|html|csv)")
# History, this suite's own detail strings, and vendored agent-skill packs we neither
# wrote nor maintain.
_DOC_EXEMPT = ("docs/superpowers/", "tests/", ".git/", "CHANGELOG.md",
               ".claude/skills/", ".cursor/skills/", ".github/skills/", "vendor/")
_doc_dangling: dict[str, set[str]] = {}
for _p in repo_files(".py") + repo_files(".md") + repo_files(".html") + \
        repo_files(".yml") + repo_files(".sql"):
    _rel = _p.relative_to(ROOT).as_posix()
    if any(_rel.startswith(x) or _rel == x for x in _DOC_EXEMPT):
        continue
    # A page naming the files it REPLACED is describing history, not linking to it.
    if _rel == "docs/platform_team_requests.html":
        continue
    for _cite in set(_DOC_CITE.findall(_p.read_text(encoding="utf-8", errors="ignore"))):
        if not (ROOT / _cite).exists():
            _doc_dangling.setdefault(_cite, set()).add(_rel)

# A FLOOR, because a broken sweep finds nothing and reads as a pass.
_doc_cited_total = {c for _p in repo_files(".md") + repo_files(".yml") + repo_files(".sql")
                    for c in _DOC_CITE.findall(_p.read_text(encoding="utf-8", errors="ignore"))}
check("the docs-citation sweep actually finds paths to check",
      len(_doc_cited_total) >= 5,
      f"found {len(_doc_cited_total)} docs/ path(s) cited outside history -- the walk or "
      f"the pattern is broken, and the check below would pass by examining nothing")
check("every docs/ path a live file cites exists",
      not _doc_dangling,
      f"{ {k: sorted(v) for k, v in _doc_dangling.items()} } -- DEF-5's class. A pointer "
      f"into a deleted or renamed document reads as though the reasoning is one click away")


# --------------------------------------------------------------------------- #
# HAZARD: copy-pasteable commands that target the abandoned weu_tds deployment.
# usnc_tds is this project's development environment; weu_tds is not used.
#
# README.md's "Deploy" section is a live quick-reference and has been fully
# retargeted to usnc_tds -- no `databricks bundle` / `preflight_target.py` /
# `conformance_check.py` line there may name weu_tds or hfig-weu-tds again.
# This does NOT assert weu_tds is absent from README entirely -- it legitimately
# appears in the topology table and the metastore-sharing prose just below.
#
# DEPLOY.md is different: its phased runbook (Phase 0d, 3, 4, 5, 6, 7) is a
# deliberately EU-first sequence that has not been retargeted -- that is a
# separate, larger task, so a blanket scan of the whole file would fail on
# dozens of legitimate, deliberately-unconverted commands. The one DEPLOY.md
# line this task did retarget is the general "run this before every deploy"
# preflight reminder in Phase 0b -- the first `preflight_target.py` invocation
# in the file -- so that specific line is what is checked here.
# --------------------------------------------------------------------------- #
print("\n[docs] no copy-pasteable command still targets the abandoned weu_tds")

_EXEC_MARKERS = ("databricks bundle", "preflight_target.py", "conformance_check.py")


def _weu_tds_command_lines(text):
    # A shell line wrapped with a trailing backslash puts the marker and the
    # target on different physical lines (DEPLOY.md's cross-region
    # conformance example does exactly this: "conformance_check.py \" on one
    # line, "--targets weu_tds,... --baseline weu_tds ..." on the next), so a
    # bare per-physical-line scan misses it -- the exact regression class
    # this check exists to catch. Join continuations into one logical line
    # first: collapse a trailing "\" plus the newline and any leading
    # whitespace on the next line into a single space, then scan.
    joined = re.sub(r"\\\n[ \t]*", " ", text)
    return [
        line for line in joined.splitlines()
        if any(marker in line for marker in _EXEC_MARKERS)
        and _names_weu_tds(line)
    ]


# `--targets` takes a COMPARISON SET, not a deploy destination. Under design decision
# D2 the model is identical in all four lakes, so conformance_check.py's own default
# compares all four -- weu_tds included -- and an example that drops it contradicts
# the tool it documents. Naming weu_tds there is not a hazard: nothing is deployed,
# nothing is written. Naming it as a DESTINATION still is, so `--target weu_tds`,
# `-t weu_tds` and `--baseline weu_tds` remain caught.
_TARGETS_VALUE = re.compile(r"--targets(?:=|\s+)\S+")


def _names_weu_tds(line):
    without_comparison_set = _TARGETS_VALUE.sub("--targets <set>", line)
    return ("weu_tds" in without_comparison_set
            or "hfig-weu-tds" in without_comparison_set)


# The `--targets` exemption must narrow the predicate, not blunt it. Probe both
# directions here so a later widening of _TARGETS_VALUE cannot quietly disarm the
# whole hazard scan below.
_probe_caught = _weu_tds_command_lines(
    "python checks/conformance_check.py --targets usnc_tds,weu_tds --baseline weu_tds\n"
    "databricks bundle deploy -t weu_tds\n"
    "python checks/preflight_target.py --target weu_tds --profile hfig-weu-tds\n"
)
check(
    "the weu_tds hazard predicate still catches a deploy destination",
    len(_probe_caught) == 3,
    f"caught {len(_probe_caught)}/3 of --baseline weu_tds, -t weu_tds, "
    f"--target weu_tds: {_probe_caught}",
)
_probe_ignored = _weu_tds_command_lines(
    "python checks/conformance_check.py --targets usnc_tds,weu_tds,uks_tds,aue_tds "
    "--baseline usnc_tds --warehouse-id <id>\n"
)
check(
    "the weu_tds hazard predicate ignores a --targets comparison set",
    not _probe_ignored,
    f"{_probe_ignored} -- D2 requires the model in all four lakes, so a conformance "
    f"comparison set naming weu_tds is correct, not stale",
)

_readme_hazard = _weu_tds_command_lines(_readme)
check(
    "README carries no executable command still targeting weu_tds",
    not _readme_hazard,
    f"{_readme_hazard} -- usnc_tds is the development environment, weu_tds is abandoned",
)

_deploy = (ROOT / "DEPLOY.md").read_text()
# Match the SCRIPT, not the interpreter prefix in front of it. Pinned to
# "python3 checks/..." this went blank the moment the runbook was corrected to invoke
# the gates through uv, and then reported the target as missing rather than wrong.
_deploy_preflight_0b = next(
    (line for line in _deploy.splitlines()
     if re.search(r"checks/preflight_target\.py\s+--", line)),
    "",
)
check(
    "DEPLOY.md's Phase 0b preflight reminder targets usnc_tds, not weu_tds",
    "usnc_tds" in _deploy_preflight_0b and "weu_tds" not in _deploy_preflight_0b,
    f"got {_deploy_preflight_0b!r} -- the rest of DEPLOY.md's EU-first runbook "
    f"is deliberately deferred and is not covered by this check",
)

# The rest of DEPLOY.md's runbook is deliberately left EU-first (see above),
# so the many remaining weu_tds commands are not individually checked. That
# means a reader hitting Phase 4/5/6/7 has no other signal that those
# commands are stale -- so a staleness banner at the very top is not
# cosmetic, it is the control. Assert it survives: a recognisable marker,
# near the top of the file (before any phase content), naming the design doc
# that carries the current target model.
_deploy_head = "\n".join(_deploy.splitlines()[:20])
check(
    "DEPLOY.md carries a staleness banner warning the runbook is EU-first",
    "STALE RUNBOOK" in _deploy_head,
    "the banner naming weu_tds as unused/usnc_tds as the dev environment was "
    "removed or pushed past the file's first 20 lines -- it must stay "
    "immediately visible, or the many stale weu_tds commands below it read "
    "as current",
)
check(
    "DEPLOY.md's staleness banner points at the current design doc",
    "docs/superpowers/specs/2026-08-24-usnc-tds-retarget-design.md" in _deploy_head,
    "the banner must tell the reader where the current target model lives",
)

# The banner guards against total removal, but not against a *fresh* stale
# weu_tds command being added to one of DEPLOY.md's untouched, un-retargeted
# sections -- a whole-file scan can't be used there the way it is for
# README.md, because these commands are KNOWN-STALE and deliberately
# retained behind the banner while the EU-first runbook rewrite is deferred.
# So: freeze the count instead of asserting zero. This is a count of LOGICAL
# lines, with backslash continuations joined by _weu_tds_command_lines --
# a naive physical-line grep undercounts by one (it misses the
# conformance_check.py example around DEPLOY.md:585-586, whose target list
# is wrapped onto the following line). Tracked here, not left to drift,
# until the runbook rewrite happens.
#
# LOWERED 13 -> 9 DELIBERATELY, exactly as this check's own message instructs.
# Phase 5's four gate-run commands were retargeted to usnc_tds, because
# Phase 5 assumes a target that declares active_sources and weu_tds does
# not: an empty active_sources means EVERY binding is active, placeholders
# included, so the pipeline fails at definition time before any gate in that
# phase is reached. The remaining 9 are Phase 4's deploy sequence, section
# 0c, and Phases 6-7, all still deliberately EU-first behind the staleness
# banner.
_DEPLOY_KNOWN_STALE_WEU_TDS_COMMANDS = 9

_deploy_hazard = _weu_tds_command_lines(_deploy)
check(
    "DEPLOY.md's stale weu_tds command count has not grown",
    len(_deploy_hazard) == _DEPLOY_KNOWN_STALE_WEU_TDS_COMMANDS,
    f"found {len(_deploy_hazard)} weu_tds command line(s) (logical lines, "
    f"continuations joined), expected exactly "
    f"{_DEPLOY_KNOWN_STALE_WEU_TDS_COMMANDS} -- if you are retargeting "
    f"DEPLOY.md's commands, lower this constant deliberately to match; if "
    f"this fired unexpectedly, you have added a command pointing at "
    f"weu_tds, a lake this project does not use (usnc_tds is the "
    f"development environment)",
)


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
# ONE PER RAW DOMAIN PLUS THE BUSINESS VAULT, since 25 September. It was `raw_vault` and
# `business_vault`; the raw layer is now split so that a definition-time failure in one
# domain cannot take the other five down with it. The exact set is checked against the
# model further down; here we only need the names to iterate.
_raw_pipes = sorted(n for n in _p if n.startswith("raw_vault_"))
check("there is at least one raw pipeline and a business vault",
      _raw_pipes and "business_vault" in _p,
      f"got {sorted(_p)} -- a raw layer with no pipeline loads nothing and says nothing")
for _n, _want in ([(_r, "vault_schema") for _r in _raw_pipes]
                  + [("business_vault", "business_vault_schema")]):
    check(f"{_n} targets ${{var.{_want}}}",
          _p.get(_n, {}).get("schema") == "${var." + _want + "}",
          f"got {_p.get(_n, {}).get('schema')!r}")
    check(f"{_n} declares its layer",
          (_p.get(_n, {}).get("configuration") or {}).get("hfig.vault_layer") is not None,
          "the entry point filters entities by layer")

_entry = (ROOT / "src" / "pipelines" / "silver_vault.py").read_text()
check("the entry point filters by layer as well as domain",
      "hfig.vault_layer" in _entry and "BUSINESS_KINDS" in _entry)
# A bare `'csat' in _entry` would pass on a mention in a COMMENT. Read the literal
# BUSINESS_KINDS set out of the entry point and assert membership in it, so the check
# fails if csat is ever dropped from the set while the word survives in the prose.
# The entry point no longer carries its own literal -- it delegates to naming, so there
# is ONE definition shared with apply_governance. This check was pinned to the old
# duplicated-literal shape and reported an empty set once the duplication was removed,
# i.e. it failed *because the code got better*. Follow the delegation instead.
_bk_literal = re.search(r"^BUSINESS_KINDS\s*=\s*(?:frozenset\()?\{(.*?)\}", _entry, re.M)
_bk_delegates = re.search(r"^BUSINESS_KINDS\s*=\s*naming\.BUSINESS_KINDS", _entry, re.M)
check("the entry point has ONE definition of the business kinds, not a second copy",
      bool(_bk_delegates) and not _bk_literal,
      "a second literal here can drift from naming.BUSINESS_KINDS, and the governance "
      "grant and the pipeline filter would then disagree about what a business kind is")
_business_kinds = set(naming.BUSINESS_KINDS)
check("csat is a Business Vault kind", "csat" in _business_kinds,
      f"naming.BUSINESS_KINDS is {sorted(_business_kinds)} -- a bare substring test "
      f"would have passed on a mention in a comment")


# --------------------------------------------------------------------------- #
# ACTIVE SOURCES (parent decision D5). Every lake DECLARES the whole model, but a
# source is only in Bronze in some of them, and a flow reading a table that is not
# there fails the pipeline at DEFINITION time -- taking every other flow with it.
# The factory therefore splits its two emissions: create_streaming_table() for
# every declared binding (so the inventory, and therefore conformance, is identical
# everywhere) and append_flow() only for the bindings active here.
#
# The checks below are about the two halves of that split staying split, and about
# the declared list for the one lake this project deploys to staying in step with
# which bronze tables actually exist there.
# --------------------------------------------------------------------------- #
print("\n[sources] active_sources declares which bindings load, not which exist")

_vars = (bundle or {}).get("variables", {}) or {}
check("active_sources is a declared bundle variable", "active_sources" in _vars,
      "D5 passes activity as bundle configuration, like domains and vault_layer")
check("its default is empty, meaning every binding is active",
      (_vars.get("active_sources") or {}).get("default") == "",
      "an unset value must leave existing targets working unchanged")

for _n in _raw_pipes + ["business_vault"]:
    check(f"{_n} passes hfig.active_sources to the entry point",
          (_p.get(_n, {}).get("configuration") or {}).get("hfig.active_sources")
          == "${var.active_sources}",
          f"got {(_p.get(_n, {}).get('configuration') or {}).get('hfig.active_sources')!r}")

# EVERY ACTIVE BINDING MUST READ THE TARGET'S OWN BRONZE CATALOG.
#
# WHY THIS EXISTS. `active_sources` decides which bindings load in a lake, and an
# UNSET value means EVERY binding is active. Six targets set nothing today --
# weu, weu_tds, uks, uks_tds, aue, aue_tds -- and so does usnc (PRODUCTION).
# That is currently safe in the loud way: every real binding hard-codes
# `01_usnc_bronze_dev`, so a deploy to any of them activates a flow reading a
# catalog that lake does not have, and the pipeline fails at DEFINITION time
# before reading anything. The variable's own description says so.
#
# THE HAZARD IS THE OBVIOUS FIX. Someone looking at six blank values concludes
# they were forgotten and copies usnc_tds's list across. That does not fail --
# it SUCCEEDS at declaring that an EU target loads bindings which read a US dev
# catalog, and that production loads `01_usnc_bronze_dev` rather than
# `01_usnc_bronze`. A loud failure becomes a config asserting something false,
# and on a programme with an open question about EU-hosted data reaching a US
# lake, that is the wrong direction to be wrong in.
#
# Nothing asserted this. `bronze_catalog` is a per-target variable, but every
# binding names its catalog as a LITERAL, so the two can disagree freely and no
# check compared them. This is that check.
#
# IT PASSES TRIVIALLY TODAY, deliberately: only usnc_tds and dev declare
# anything, and their bindings do read `01_usnc_bronze_dev`. Its whole value is
# the day someone populates a list for another lake.
#
# PLACEHOLDER BINDINGS ARE EXEMPT, because they are not read: a fake catalog
# (`hfig_eu.`, `hfig_usnc.`, `PLACEHOLDER.`) names a table that exists nowhere,
# which is the established way this repo declares a binding before its feed
# lands. The rule is about bindings that name a REAL catalog -- one starting
# `01_`, `02_` or `03_` -- reading the wrong lake's.
_ab_targets = ((bundle or {}).get("targets") or {})
_ab_real_prefixes = ("01_", "02_", "03_")
_ab_problems: list[str] = []
try:
    for _ab_name, _ab_t in sorted(_ab_targets.items()):
        _ab_vars = (_ab_t.get("variables") or {})
        _ab_declared = _ab_vars.get("active_sources")
        if not (_ab_declared or "").strip():
            continue                      # unset: every binding active, and that state is
                                          # guarded by the definition-time failure above
        _ab_bronze = _ab_vars.get("bronze_catalog") or ""
        _ab_silver = _ab_vars.get("catalog") or ""
        _ab_allowed = {c for c in (_ab_bronze, _ab_silver) if c}
        _ab_active = spec.resolve_active_sources(model, _ab_declared)
        if _ab_active is None:
            continue
        for _ab_e in model.entities:
            for _ab_s in _ab_e.sources:
                if spec.binding_id(_ab_e, _ab_s) not in _ab_active:
                    continue
                _ab_cat = _ab_s.bronze_table.split(".")[0]
                if not _ab_cat.startswith(_ab_real_prefixes):
                    # A PLACEHOLDER CATALOG IS FINE UNTIL THE BINDING IS SWITCHED ON, and
                    # then it is guaranteed to fail. This `continue` used to be
                    # unconditional -- "names no real table anywhere", so nothing to
                    # compare against -- and it exempted exactly the bindings that cannot
                    # survive being active.
                    #
                    # MEASURED 26 September, and it is the SAME SHAPE as the cast: defect
                    # documented immediately below. csat_invoice_line_gie was activated
                    # carrying `hfig_usnc.raw_vault.nhl_invoice_line`; hfig_usnc is a
                    # catalog in no lake. 1,307 offline checks passed. The pipeline then
                    # raised TABLE_OR_VIEW_NOT_FOUND and retried four times in six
                    # minutes, each update dying in ten seconds, while the job task
                    # reported RUNNING for nine and a half minutes against the 0.7 it
                    # takes when it works.
                    #
                    # We are only here because the binding IS active, so a placeholder is
                    # now the finding rather than the reason to skip.
                    _ab_problems.append(
                        f"{_ab_name}: {_ab_e.name}/{_ab_s.name} is ACTIVE and reads "
                        f"catalog {_ab_cat!r}, which is not a real catalog in any lake "
                        f"(expected one starting {_ab_real_prefixes}). An inactive "
                        f"binding may carry a placeholder; an active one cannot -- it "
                        f"fails at read time and retries, which costs compute and lies "
                        f"about what the task is doing")
                    continue
                if _ab_cat not in _ab_allowed:
                    _ab_problems.append(
                        f"{_ab_name}: {_ab_e.name}/{_ab_s.name} reads {_ab_cat}, "
                        f"but this target's catalogs are {sorted(_ab_allowed)}")
except Exception as _ab_exc:              # noqa: BLE001 - reported, never swallowed
    _ab_problems.append(f"the check could not run: {type(_ab_exc).__name__}: {_ab_exc}")

# AN ACTIVE BINDING ON A MASKED ENTITY MUST DECLARE ITS CAST TYPES.
#
# factory._mask_clauses raises SpecError when a masked column has no cast:, because the
# MASK clause goes INTO the CREATE STREAMING TABLE definition and a column entry needs a
# type. That refusal is correct and is tested. What nothing checked was the COMBINATION --
# an entity left untyped while its binding was switched on -- and the two halves are
# declared in different files by different changes: the cast in metadata/entities/*.yml,
# the activation in databricks.yml.
#
# MEASURED 25 September. invoice_line/FIELDGLASS_US and invoice_header/FIELDGLASS_US were
# activated for usnc_tds without cast: blocks. 1,216 offline checks passed. The pipeline
# then raised SpecError naming all five of invoice_line's masked columns -- and
# RETRY_ON_FAILURE started a fresh update every forty seconds for fifteen minutes, each
# dying in under a minute, while the job task sat there reporting RUNNING. The cost of
# finding this in the lake rather than here is a quarter hour of serverless compute and a
# task whose state lies about what it is doing.
_mc_problems: list[str] = []
try:
    for _mc_name, _mc_t in sorted(_ab_targets.items()):
        _mc_declared = ((_mc_t.get("variables") or {}).get("active_sources") or "").strip()
        if not _mc_declared:
            continue
        _mc_active = spec.resolve_active_sources(model, _mc_declared)
        if _mc_active is None:
            continue
        for _mc_e in model.entities:
            if not _mc_e.masks:
                continue
            for _mc_s in _mc_e.sources:
                if spec.binding_id(_mc_e, _mc_s) not in _mc_active:
                    continue
                _mc_typed = {c for c, _t in _mc_s.cast}
                _mc_gap = sorted(c for c, _fn in _mc_e.masks if c not in _mc_typed)
                if _mc_gap:
                    _mc_problems.append(
                        f"{_mc_name}: {_mc_e.name}/{_mc_s.name} is ACTIVE and masks "
                        f"{_mc_gap} with no cast: for them")
except Exception as _mc_exc:              # noqa: BLE001 - reported, never swallowed
    _mc_problems.append(f"the check could not run: {type(_mc_exc).__name__}: {_mc_exc}")

check("no ACTIVE binding masks a column it declares no cast type for",
      not _mc_problems,
      f"{_mc_problems[:6]} -- the pipeline refuses to build this, so the load never "
      f"starts; and because the pipeline retries on failure, it refuses it again every "
      f"forty seconds while the job task reports RUNNING. Deferring a mask type is fine "
      f"only while the binding is OFF: declare cast: for every masked column in the same "
      f"change that adds the binding to active_sources")

check("every ACTIVE binding reads a catalog the target actually declares",
      not _ab_problems,
      f"{_ab_problems[:6]} -- a binding names its catalog as a literal while "
      f"bronze_catalog is per-target, so the two can disagree with nothing "
      f"objecting. An active binding pointing at another lake does not fail: it "
      f"succeeds at reading the wrong region's data, or production at reading dev")


check("the entry point reads hfig.active_sources",
      "hfig.active_sources" in _entry)
check("it resolves activity against the WHOLE model, before the domain/layer filters",
      _entry.index("resolve_active_sources") < _entry.index("if DOMAINS:"),
      "the Business Vault pipeline declares only csat entities, so resolving after "
      "filtering would refuse every GP and UKG binding as unknown")
check("it reports the bindings it is skipping",
      "inactive_bindings" in _entry,
      "a skipped flow nobody prints is indistinguishable from one that loaded nothing")

_fac = (ROOT / "src" / "accelerator" / "factory.py").read_text()
_build = _fac[_fac.index("def build("):]
# ORDER CHECKS READ THE CODE, NOT THE DOCSTRING. build()'s docstring now explains the
# create/flow split at length and names the same functions, so indexing into the whole
# definition compares prose positions and reports nonsense. Cut the docstring off first.
_build = _build.split('"""')[2] if _build.count('"""') >= 2 else _build
check("build() emits the target table for every declared binding",
      "_emit_target" in _build and "if active:" in _build
      and _build.index("_emit_target") < _build.index("if active:"),
      "create_streaming_table() must run for EVERY declared binding, or an inactive "
      "source produces a MISSING table instead of an empty one and conformance breaks")
check("build() guards the flow registration with the activity test",
      _build.index("active_table_bindings") < _build.index("_register_source_flows"),
      "append_flow() is the half that reads Bronze and the only half that may be skipped")
# MASKS AND THE _v1 PROJECTION FOLLOW THE SAME TEST. _emit_target passes no schema, so
# SDP infers a streaming table's columns from its flows; a ghost-only table has the hash
# key and the six system columns and no payload, so a `col MASK fn` clause would name a
# column that does not exist -- very likely a definition-time error, failing the deploy.
# Read the CALL, not its spelling: DEF-52 renamed this argument from `table` to `target`
# and the old literal grep reported the guard missing when it was intact.
_emit_target_calls = [n for n in _ast.walk(_fac_tree)
                      if isinstance(n, _ast.Call) and isinstance(n.func, _ast.Name)
                      and n.func.id == "_emit_target"]
check("mask clauses are suppressed for a table with no active binding",
      len(_emit_target_calls) == 1
      and any(k.arg == "active_bindings" for k in _emit_target_calls[0].keywords)
      and "if entity.masks and active_bindings:" in fac,
      "a MASK clause on a ghost-only table names a column that does not exist. The "
      "active bindings are passed in because they also carry the cast: types the "
      "clause needs (DEF-16); an empty list still means no clause at all")
# the guard is the `if` immediately above the call
_build_lines = _build.splitlines()
_v1_at = next((i for i, l in enumerate(_build_lines) if "_emit_v1_view(" in l), 0)
_v1_cond = _build_lines[_v1_at - 1] if _v1_at else ""
check("the _v1 projection is suppressed for a table with no active binding",
      _v1_cond.strip().startswith("if ") and "and active:" in _v1_cond,
      f"guarded by {_v1_cond.strip()!r} -- it would project an absent payload and "
      f"redeclare a mask over absent columns")
check("there is ONE active-set test, shared with the gates",
      "is_source_active" not in _build,
      "build() must use spec.active_table_bindings, the same function "
      "loop1_reconciliation / mask_survival_check / journal_integrity_check resolve with")
# THE QUARANTINE TWIN IS THE DELIBERATE EXCEPTION. It is fed only by
# _register_source_flows, so a quarantine table for a target with no active binding would
# be declared with NO FLOWS AT ALL -- the one place this split could hand SDP a flowless
# streaming table. It is skipped instead, and that is safe for exactly two reasons, both
# checked here rather than asserted in a comment.
check("the quarantine twin is emitted only where a binding can reject into it",
      _build.index("active_table_bindings") < _build.index("_emit_quarantine"),
      "a qtn_ table whose every binding is inactive would be declared with no flows")
check("the guard is 'no active binding at all', not 'any inactive binding'",
      "if active:" in _build and "_emit_quarantine" in _build.split("if active:")[1],
      "a hub with one active binding among several still rejects rows, so it still needs "
      "its quarantine table")

# READ THE VALUE, NOT THE SOURCE TEXT. This used to regex a literal tuple out of
# conformance_check.py, so the moment that tuple became a derived name the regex matched
# nothing, `_prefix_set` was empty, and the check failed with "parsed []" -- it was
# asserting the shape of a line rather than the content of a set. Reading the constant
# tests the thing that matters and survives the value being computed.
_conf_src = (ROOT / "checks" / "conformance_check.py").read_text()
_prefix_set = set(naming.CONFORMANCE_TABLE_PREFIXES)
check("conformance compares the vault prefixes, and qtn_ is not one of them",
      _prefix_set and "qtn_" not in _prefix_set and "hub_" in _prefix_set,
      f"{sorted(_prefix_set)} -- if qtn_ ever joins this set, skipping a "
      f"quarantine table for a fully-inactive binding WOULD cause conformance drift, "
      f"and factory.build's exception has to be revisited")
# AND stg_ IS OUT FOR THE SAME REASON, which the old check never said. A staging log is a
# per-binding derivative like the quarantine twin, not a table the model declares.
check("nor is stg_, which is a per-binding derivative for the same reason",
      "stg_" not in _prefix_set,
      f"{sorted(_prefix_set)} -- staging logs follow activation, so comparing them "
      f"across regions with different active_sources reports drift that is not drift")
# AND conformance MUST READ THE DECLARATION. Checking the constant proves nothing about
# the file unless the file uses it -- the hand-typed copy this replaced was the failure.
check("conformance_check reads the declaration rather than keeping its own copy",
      "CONFORMANCE_TABLE_PREFIXES" in _conf_src
      and not re.search(r"^VAULT_PREFIXES\s*=\s*\(", _conf_src, re.M),
      "it declares its own tuple again -- three authorities on 'what is a vault table' "
      "disagreed on 5 Sep and the derived one was the one nobody read")
check("loop-1 reconciliation names the tables it reconciles",
      '"--entity"' in (ROOT / "checks" / "loop1_reconciliation.py").read_text(),
      "a gate that walked every entity blindly would read a missing quarantine table "
      "as a failure rather than as nothing to reconcile")

# ---- ALL FOUR post-load gates, not three ------------------------------------
# aggregate_reconciliation_check.py was the one left behind: it joins the csat
# classification _v1 and reads nhl_payroll_detail.amount, both of which the _v1 and mask
# suppression removed for an inactive table. It sits AHEAD of apply_governance,
# assert_mask_survival and publish_model_metadata in the job, so failing there blocks
# governance -- which is why "which gates are active-set aware" is checked, not assumed.
# DEF-12, ASSERTED RATHER THAN REMEMBERED. Serverless spark_python_task exec()s a check file
# and does NOT define __file__, so any reference to it dies at import -- before the file's own
# logic exists, and before any gate it contains can run.
#
# MEASURED 29 Aug, the first time source_conformance_check.py ran for real: it failed with
# `NameError: name '__file__' is not defined` and blocked the load. append_only_check.py -- a
# deployed hard gate -- carried the identical defect, armed the same day and not yet fired.
#
# NO OFFLINE TEST COULD CATCH EITHER. Every suite here loads a check with
# importlib.util.spec_from_file_location, which SETS __file__; so does the _FakeSpark harness that
# drives main() end to end. The harness and the real runtime differ in exactly one respect, and it
# is the one thing nothing exercised: module import under exec(). This check is the substitute.
_def12_unguarded = []
for _d12 in sorted((ROOT / "checks").glob("*.py")):
    _d12_src = _d12.read_text(encoding="utf-8")
    if "__file__" in _d12_src and '"__file__" not in globals()' not in _d12_src:
        _def12_unguarded.append(_d12.name)
check("every checks/*.py referencing __file__ carries the DEF-12 guard",
      not _def12_unguarded,
      f"unguarded: {_def12_unguarded} -- serverless exec()s these files without __file__, so each "
      f"dies at import with NameError before its own logic runs. A job-wired gate that cannot "
      f"import is a gate that never asserts anything")

# AND THE SAME PROPERTY BEHAVIOURALLY, because the check above scans for the guard's TEXT while
# this reproduces the condition it guards against. exec() the file with globals that deliberately
# omit __file__ -- exactly what serverless does -- and require it not to die on that. Any OTHER
# failure is fine and expected: pyspark is absent here, so most stop at that import having already
# proved the __file__ line survived.
#
# This is the check that would have caught the 29 Aug load failure. The textual one above is kept
# because it names the missing guard directly, which the exec probe cannot.
_def12_raised = []
for _d12b in sorted((ROOT / "checks").glob("*.py")):
    _d12b_src = _d12b.read_text(encoding="utf-8")
    if "__file__" not in _d12b_src:
        continue
    try:
        exec(compile(_d12b_src, str(_d12b), "exec"), {"__name__": "__def12_probe__"})
    except NameError as _d12_exc:
        if "__file__" in str(_d12_exc):
            _def12_raised.append(f"{_d12b.name}: {_d12_exc}")
    except Exception:  # noqa: BLE001 -- any other failure is not what this asserts
        pass
check("every checks/*.py survives import with no __file__ in globals, as serverless exec()s it",
      not _def12_raised,
      f"{_def12_raised} -- the exact condition that failed the 29 Aug load: the file dies at "
      f"import before its own logic exists, so a job-wired gate asserts nothing while presenting "
      f"as a task failure")

# A JOB TASK MUST NOT IMPORT PIPELINE CODE, and this is asserted because it is invisible
# otherwise. MEASURED 29 Aug: source_conformance_check.contracts() imported emit_source_contract
# to reuse its path helpers -- "reuse, do not restate" -- and that chain is
#   emit_source_contract -> emit_data_contract:106 -> accelerator.factory:56 -> pyspark.pipelines
# Importing pyspark.pipelines OUTSIDE a DLT pipeline trips Databricks' import hook and dies with
# `Py4JJavaError ... NoSuchElementException: None.get`. The task failed and blocked the load.
#
# The exec probe above cannot catch this: pyspark is absent here, so the import fails with
# ModuleNotFoundError, which that probe deliberately ignores. Only the text can be checked.
# PARSED, NOT GREPPED. A first cut matched these names as TEXT and immediately failed on the
# docstring that explains why not to import them -- the check tripping over its own rationale.
# ast sees imports; prose about imports is invisible to it, which is the correct distinction.
# factory MUST NOT IMPORT pyspark.pipelines AT MODULE LEVEL. Measured 29 Aug, on the first run
# in 25 that reached supersede_quarantine: reject_digest.digest_columns() defers
# `from .factory import _projection` to CALL time to stay pyspark-free at import -- and that call,
# inside a job task, pulled factory:56's module-level `from pyspark import pipelines`, tripping
# Databricks' DLT hook with `Py4JJavaError o34.get`. The first entity raised that; the next two
# raised TypeError, because the half-initialised factory left None where _projection should be.
#
# Every dp.* use is inside a function body and runs within pipeline execution, so the import
# belongs there too. This is the same root cause as the source-conformance failure earlier the
# same day, reached by a deferred import instead of a direct one.
_fact_src = (ROOT / "src" / "accelerator" / "factory.py").read_text(encoding="utf-8")
_fact_tree = ast.parse(_fact_src)
_fact_toplevel_pipelines = []
for _fn in _fact_tree.body:  # MODULE level only -- function bodies are not walked
    if isinstance(_fn, ast.ImportFrom) and (_fn.module or "").startswith("pyspark"):
        for _a in _fn.names:
            if _a.name == "pipelines":
                _fact_toplevel_pipelines.append(f"line {_fn.lineno}: from {_fn.module} import pipelines")
    elif isinstance(_fn, ast.Import):
        for _a in _fn.names:
            if _a.name.startswith("pyspark.pipelines"):
                _fact_toplevel_pipelines.append(f"line {_fn.lineno}: import {_a.name}")
check("factory does not import pyspark.pipelines at module level",
      not _fact_toplevel_pipelines,
      f"{_fact_toplevel_pipelines} -- any module that imports factory for a non-pipeline reason "
      f"then trips the DLT import hook. reject_digest.digest_columns() does exactly that at call "
      f"time, which is what killed supersede_quarantine on 29 Aug")

_FORBIDDEN_IMPORTS = {"emit_source_contract", "emit_data_contract", "factory", "pipelines"}
_pipeline_importers = []
for _pi in sorted((ROOT / "checks").glob("*.py")):
    for _n in ast.walk(ast.parse(_pi.read_text(encoding="utf-8"))):
        if isinstance(_n, ast.Import):
            _names = {a.name.split(".")[-1] for a in _n.names}
        elif isinstance(_n, ast.ImportFrom):
            _names = {a.name.split(".")[-1] for a in _n.names}
            _names |= {(_n.module or "").split(".")[-1]}
        else:
            continue
        for _bad in sorted(_names & _FORBIDDEN_IMPORTS):
            _pipeline_importers.append(f"{_pi.name}: imports {_bad}")
check("no checks/*.py imports the contract emitters or pipeline code",
      not _pipeline_importers,
      f"{_pipeline_importers} -- a job task that imports accelerator.factory pulls in "
      f"pyspark.pipelines, and importing that outside a DLT pipeline dies at the import hook "
      f"before the task's own logic runs")

_GATE_FILES = ("loop1_reconciliation.py", "mask_survival_check.py",
               "journal_integrity_check.py", "aggregate_reconciliation_check.py",
               "supersede_quarantine.py")
_job_doc = yaml.safe_load((ROOT / "resources" / "vault_job.yml").read_text())
_job_tasks = {t["task_key"]: t
              for t in _job_doc["resources"]["jobs"]["vault_load"]["tasks"]}
# SOURCE CONFORMANCE IS WIRED INTO THE JOB, AND BLOCKS THE LOAD. Not part of _GATE_FILES: that
# family resolves activity through spec.active_table_bindings and prints skipped_inactive, and
# this check does neither -- it reads source_contracts/, not the model's bindings. Joining that
# tuple would assert properties it does not have.
#
# It runs BEFORE raw_vault deliberately. A MISSING COLUMN means the load fails or silently writes
# nulls; a LOSSY CAST means money rows arrive NULL. Finding either after the load tells you what
# you already broke, so raw_vault depends on it and a finding stops the write.
_sc_task = _job_tasks.get("assert_source_conformance")
# EVERY RAW DOMAIN TASK, TRANSITIVELY. The split put five domains behind
# raw_vault_reference, so only that one names the gates directly; asking for a direct edge
# on all six would force every domain to repeat them, and asking only about `raw_vault`
# would look at a task that no longer exists and pass by finding nothing.
def _sc_reaches(task: str, target: str, seen: set | None = None) -> bool:
    seen = seen if seen is not None else set()
    if task in seen:
        return False
    seen.add(task)
    deps = [d.get("task_key") for d in (_job_tasks.get(task, {}).get("depends_on") or [])]
    return target in deps or any(_sc_reaches(d, target, seen) for d in deps)


_sc_raw_tasks = sorted(k for k in _job_tasks if k.startswith("raw_vault_"))
_sc_ungated = [t for t in _sc_raw_tasks if not _sc_reaches(t, "assert_source_conformance")]
_sc_raw_deps = {"assert_source_conformance"} if not _sc_ungated else set()
check("source conformance is wired into the vault job",
      _sc_task is not None
      and _sc_task.get("spark_python_task", {}).get("python_file")
      == "../checks/source_conformance_check.py",
      f"task={_sc_task!r} -- a check nothing invokes is a check that never runs, and this one "
      f"exists to catch a breaking Bronze change before a pipeline run does")
check("no raw domain writes until source conformance has passed",
      _sc_raw_tasks and not _sc_ungated,
      f"ungated: {_sc_ungated} (of {_sc_raw_tasks}) -- without this dependency the check runs "
      f"beside the load rather than gating it, and a MISSING COLUMN is discovered after the "
      f"rows it would have stopped are already written")

# GATES DELIBERATELY OUT OF THE JOB, each with the condition that brings it back. This is
# the ONLY way to be absent: an unlisted gate wired to no task fails below.
_WITHDRAWN_GATE_FILES = {
    "journal_integrity_check.py":
        "26 Sep 2026, until PLT-2 grants the run-as service principal membership of "
        "scope_unmask_currency_values -- see docs/superpowers/OPEN_ITEMS.md",
}

for _g in _GATE_FILES:
    _txt = (ROOT / "checks" / _g).read_text()
    check(f"{_g} resolves activity through spec.active_table_bindings",
          "active_table_bindings" in _txt and "resolve_active_sources" in _txt,
          "four gates, one definition of inactive -- two definitions means a gate "
          "asserting over something the generator never emitted")
    check(f"{_g} never reports success having asserted nothing",
          "asserted nothing" in _txt and "skipped_inactive" in _txt,
          "the vacuity rule: exit 0 only for a stated absence, never a bare PASSED")
    # A dormant gate exits 0, and Databricks has no "green with a warning" task state, so
    # the run page cannot distinguish it from a gate that asserted everything. The last
    # line each gate prints is what makes the difference greppable -- and the status in
    # it is the gate's VERDICT, deliberately not the exit code.
    check(f"{_g} ends with the machine-readable GATE SUMMARY line",
          "GATE SUMMARY ::" in _txt and "def finish(" in _txt,
          "DEPLOY.md Phase 5f tells the operator to read these after every green run")
    _bare = [l.strip() for l in _txt.split("def main(")[-1].splitlines()
             if l.strip() in ("return 0", "return 1")]
    check(f"{_g} routes every exit through it",
          not _bare,
          f"{len(_bare)} bare return(s) in main() -- an exit path that prints no summary "
          f"is an outcome the runbook's check cannot see")
    # LIMITATION OF THE CHECK ABOVE, recorded so nobody reads it as total coverage: it
    # sees `return` statements inside main(). It does NOT see the two exits that leave
    # by another door and print no GATE SUMMARY at all --
    #   * `raise SystemExit(...)`, which loop1_reconciliation.py uses for an --entity
    #     naming a table the metadata does not generate;
    #   * argparse's own exit 2, for a malformed or missing argument.
    # Both are configuration errors that fail the task loudly, so the run is red and
    # DEPLOY.md Phase 5f's "read the four summary lines" is not the control that matters
    # in those cases. Worth knowing before treating "every task printed a summary" as an
    # invariant.
    # every gate file must be wired to a task that passes it the declared list
    _task = next((k for k, t in _job_tasks.items()
                  if _g in (t.get("spark_python_task", {}) or {}).get("python_file", "")),
                 None)
    # A GATE MAY BE WITHDRAWN FROM THE JOB, BUT ONLY OUT LOUD.
    #
    # assert_journal_integrity was taken out on 26 September, deliberately and temporarily:
    # the run-as service principal cannot see money -- governance.mask_money and
    # mask_money_double admit scope_unmask_currency_values -- so the gate read every
    # debitamt/crdtamnt as NULL and correctly refused to assert on zeros, while blocking
    # five downstream tasks and proving nothing. Recorded in docs/superpowers/OPEN_ITEMS.md with its
    # restore condition (PLT-2).
    #
    # Asking how an absent task is wired reports `task None` and teaches nobody anything.
    # So an absent gate is checked against the withdrawal list instead: unrecorded absence
    # still fails, and a gate that IS in the job must still pass the list exactly as before.
    if _task is None:
        check(f"{_g} is absent from the job, and recorded as deliberately withdrawn",
              _g in _WITHDRAWN_GATE_FILES,
              f"{_g} is wired to no task and is not in _WITHDRAWN_GATE_FILES, so nothing "
              f"says whether it was withdrawn on purpose or dropped by accident. A gate "
              f"that quietly stops running is the failure this file exists to prevent")
    else:
        check(f"{_g} is wired to a job task that passes --active-sources",
              "--active-sources" in _job_tasks[_task]["spark_python_task"]["parameters"],
              f"task {_task!r} does not pass the active list, so the gate cannot tell a "
              f"deliberate deferral from a broken load")
        check(f"{_g} is in the job, so it must NOT be listed as withdrawn",
              _g not in _WITHDRAWN_GATE_FILES,
              f"{_g} runs and is still recorded as withdrawn -- restoring the task and "
              f"clearing the entry are one change, or the record lies about the estate")

# THE DECLARED LIST MUST MATCH THE LAKE. usnc_tds and dev both point at the usnc TDS
# workspace, whose bronze catalog is 01_usnc_bronze_dev. Every OTHER bronze_table in
# the metadata is a placeholder from an entity this sub-project defers. So the
# declared active set is exactly derivable, and drift in either direction is a defect:
# a placeholder left active fails the pipeline at definition time, and a real binding
# left out silently loads nothing.
# A UNION VIEW WE BUILD IS ALSO REAL IN THIS LAKE, and the rule above could not see that.
#
# The rule is "reads 01_usnc_bronze_dev. ⇒ real ⇒ must be in active_sources", and its
# reasoning is sound both ways: a placeholder left active fails the pipeline at definition
# time, and a real binding left out silently loads nothing. But checks/apply_source_unions.py
# creates a union view over Fieldglass's per-tenant tables in OUR catalog -- not because that
# is the natural home, but because we hold USE_CATALOG on the bronze catalog and not
# CREATE SCHEMA (measured 4 Sep). A binding reading that view is as real as one reading
# Bronze directly, and calling it a placeholder would have forced the choice between a gate
# that fires on correct config and a view nobody can create.
#
# The schema name comes from metadata/source_unions.yml, the same declaration the task reads,
# so there is one authority for it rather than a copy here.
import yaml as _su_yaml  # noqa: E402

_su_decl = (_su_yaml.safe_load(
    (ROOT / "metadata" / "source_unions.yml").read_text(encoding="utf-8")) or {})
# A PROFILE'S IDENTITY HERE IS ITS VIEW SUFFIX WHERE IT HAS ONE, AND ITS NAME WHERE IT
# DOES NOT. Not every profile becomes a view: a single-table one is applied inline
# (`conform:`), and DEF-58 adds a VAULT-SOURCED one whose input is nhl_invoice_line, a
# table this pipeline builds -- so it declares no view_schema at all and reading the key
# unconditionally is a KeyError that kills this whole gate. The fallback keeps the profile
# IN the set, so "declared but bound by nothing" still covers it, rather than exempting a
# profile from the emptiness check by the accident of having no view.
def _su_key(u: dict) -> str:
    return (f".{u['view_schema']}.v_{u['name']}" if u.get("view_schema")
            else f"(no view) {u['name']}")


_su_views = {_su_key(u) for u in (_su_decl.get("unions") or [])}
# A BUSINESS-VAULT BINDING READS THE VAULT, NOT BRONZE, so it belongs in this set
# without a Bronze table behind it. The same BUSINESS_VAULT* prefix family that
# tools/emit_source_contract.py:is_business_vault_binding() excludes from the Bronze
# contract, and for the same reason -- a Bronze team cannot act on an object we compute.
#
# Added 26 September, when invoice_line_gie/BUSINESS_VAULT_GIE became the FIRST computed
# satellite ever declared active in any lake and this check failed it as
# "declared-but-not-real". It is real; it just does not live in Bronze. Its bronze_table
# names hfig_usnc.raw_vault.nhl_invoice_line, which is the vault it recomputes from.
_BUSINESS_VAULT_PREFIX = "BUSINESS_VAULT"
_real_bindings = {
    spec.binding_id(e, s)
    for e in model.entities for s in e.sources
    if s.bronze_table.startswith("01_usnc_bronze_dev.")
    or any(s.bronze_table.endswith(v) for v in _su_views)
}
# Every business-vault binding the model declares, for the separate check below. NOT in
# _real_bindings: that set is compared for EQUALITY against what a target declares, and a
# computed satellite is a DECISION per lake -- obligatory membership would demand every
# lake switch on every csat the model happens to contain.
# COMPUTED SATELLITES DELIBERATELY LEFT INACTIVE in a lake whose parent IS active.
# Per target, per binding, with the reason. Anything not here must be switched on.
_COMPUTED_HELD_BACK = {
    "usnc_tds": {
        "invoice_line_gie/BUSINESS_VAULT_GIE":
            "26 Sep: switched ON, which revealed that the factory cannot compute a csat "
            "payload at all -- _projection names the declared columns and _project "
            "selects them off the parent, and nothing evaluates an expression. All "
            "eleven of this satellite's payload columns are AME rules that live in "
            "src/accelerator/invoice_rules.py, which no factory module imports. Switch "
            "it on in the change that implements the computation, not before.",
        "job_request_custom_promoted/BUSINESS_VAULT":
            "26 Sep: never declared for this lake, and switching it on while fixing "
            "Ameren would be inferring a decision nobody took. A csat_ table of this "
            "name exists in business_vault from an earlier pipeline definition, which "
            "makes the question worth asking rather than answering by default -- "
            "Adrian's call.",
    },
    "dev": {
        "job_request_custom_promoted/BUSINESS_VAULT": "as usnc_tds",
        "payroll_line_classification/BUSINESS_VAULT": "as usnc_tds",
        "invoice_line_gie/BUSINESS_VAULT_GIE":
            "26 Sep: dev is not the lake Ameren is being built in; usnc_tds is.",
    },
}
_computed_bindings = {
    spec.binding_id(e, s)
    for e in model.entities for s in e.sources
    if s.name.startswith(_BUSINESS_VAULT_PREFIX)
}
# AND THE UNION VIEWS MUST ACTUALLY BE READ BY SOMETHING. A declared union nothing binds is
# a view built every run for no reader -- which would pass every check above by being
# irrelevant, and is the same emptiness this repo keeps having to name.
# A PROFILE IS READ TWO WAYS SINCE 25 September: a multi-table one through its view, a
# single-table one inline via `conform:`. Counting only the view left the inline profiles
# looking like views nobody reads.
_su_by_view = {_su_key(u): u["name"] for u in (_su_decl.get("unions") or [])}
_su_bound = {v for v in _su_views
             for e in model.entities for s in e.sources
             if s.bronze_table.endswith(v) or getattr(s, "conform", "") == _su_by_view[v]}
# A UNION VIEW IS READ AS A STREAM, SO IT MAY NOT CONTAIN A RANKING WINDOW.
#
# factory.py:318 already states the rule, about the loader's own dedup:
#
#   "dropDuplicates, NOT a row_number() window: a partitioned ranking window is not a
#    supported streaming operation and raises at pipeline analysis time."
#
# It was enforced nowhere for the VIEWS those flows read, and the same construct one layer
# up behaves worse than the comment predicts: it does not raise, it pins the driver.
#
# MEASURED 25 September. `line_sibling_ordinal: row_number() OVER (PARTITION BY 11 columns
# ORDER BY Invoice_ID)` in the invoice union left raw_vault_pay_bill -- the only domain
# reading that view -- at 50-52% GC, never out of INITIALIZING, across three attempts
# totalling over an hour, on 29,426 source rows. Every domain that does not read it
# finished in 1.2 to 2.0 minutes. It was also a non-deterministic key: inside a sibling
# group every row shares the ORDER BY value, so the ranking was arbitrary and re-keyed the
# line whenever it was recomputed.
#
# The ranking functions are named explicitly rather than matched on "OVER (", because an
# aggregate window (sum, count) over a stream is a different question and this check should
# not quietly claim to have decided it.
_win_rank = ("row_number(", "rank(", "dense_rank(", "percent_rank(", "ntile(", "cume_dist(")
_win_problems: list[str] = []
for _wu in (_su_decl.get("unions") or []):
    for _wc, _we in sorted((_wu.get("derived_columns") or {}).items()):
        _wl = " ".join(str(_we).split()).lower()
        if " over (" not in _wl and not _wl.startswith("over ("):
            continue
        if any(_f in _wl for _f in _win_rank):
            _win_problems.append(f"{_wu['name']}.{_wc}")
check("no union view computes a ranking window",
      not _win_problems,
      f"{_win_problems} -- the flows that read these views are STREAMS, and a partitioned "
      f"ranking window is not a supported streaming operation. factory.py:318 says so for "
      f"the loader's dedup; in a view it does not raise, it pins the driver in "
      f"INITIALIZING. If rows need distinguishing, distinguish them with something the "
      f"source actually carries")

# A DERIVED COLUMN MUST COME OUT NULLABLE, OR THE VAULT TABLE CANNOT BE BUILT.
#
# The vault table's schema is DERIVED from the staged frame (DEF-19), nullability included,
# and every vault table carries a GHOST row that supplies CAST(NULL AS <type>) for every
# column that is not a system column. factory._ghost_columns_sql refuses the combination --
# correctly, since the alternative is DELTA_MISSING_NOT_NULL_COLUMN_VALUE on first append.
#
# A column READ from a table is nullable, so this never arises for a plain reference. A
# WINDOW FUNCTION is the construct that is not: Spark types row_number() as NOT NULL.
#
# MEASURED 25 September. `line_sibling_ordinal: row_number() OVER (...)` built the view
# fine, passed 1,216 offline checks, and stopped the pipeline at definition time with
#
#   SpecError: invoice_line: the declared schema makes ['line_sibling_ordinal'] NOT NULL
#
# then retried every forty seconds. Wrapping it in NULLIF(expr, 0) makes the type nullable
# and changes no value -- row_number() starts at 1 and is never 0.
_nn_window = ("row_number(", "rank(", "dense_rank(", "ntile(", "count(")
_nn_wrappers = ("nullif(", "case ")
_nn_problems: list[str] = []
for _nn_u in (_su_decl.get("unions") or []):
    for _nn_col, _nn_expr in sorted((_nn_u.get("derived_columns") or {}).items()):
        _nn_low = " ".join(str(_nn_expr).split()).lower()
        if " over (" not in _nn_low and not _nn_low.startswith("over ("):
            continue                      # not a window expression at all
        if not any(_nn_low.startswith(w) for w in _nn_wrappers):
            _nn_problems.append(f"{_nn_u['name']}.{_nn_col}")
check("every windowed derived column is wrapped so its type stays NULLABLE",
      not _nn_problems,
      f"{_nn_problems} -- Spark types a window function NOT NULL, the vault schema is "
      f"derived from the staged frame, and the ghost row supplies NULL for every "
      f"non-system column, so factory refuses to build the table and the pipeline retries "
      f"on failure every forty seconds. Wrap it: NULLIF(<expr>, <a value the function "
      f"never returns>) keeps every value and changes only the nullability")

check("every declared source union is read by at least one binding",
      _su_views == _su_bound,
      f"declared but bound by nothing: {sorted(_su_views - _su_bound)} -- a union view "
      f"rebuilt every run for no reader is cost with no consumer, and it would pass every "
      f"other check here by being irrelevant")
for _tname, _t in ((bundle or {}).get("targets", {}) or {}).items():
    _declared = ((_t.get("variables") or {}).get("active_sources") or "").strip()
    if not _declared:
        continue
    try:
        _resolved = spec.resolve_active_sources(model, _declared)
    except spec.SpecError as exc:
        _resolved = None
        bad(f"target {_tname}: active_sources resolves against the metadata", str(exc))
    if _resolved is None:
        continue
    ok(f"target {_tname}: active_sources resolves against the metadata")
    # Computed satellites are held out of the equality: they read the vault, so they have
    # no Bronze table to be real in, and each is a per-lake decision rather than an
    # obligation. They get their own check immediately below, so a typo still fails.
    _resolved_bronze = {b for b in _resolved
                        if b.split("/", 1)[-1].split("/")[0] not in ("",)
                        and not b.split("/")[-1].startswith(_BUSINESS_VAULT_PREFIX)}
    _resolved_computed = _resolved - _resolved_bronze
    # A COMPUTED SATELLITE WHOSE PARENT IS ACTIVE, AND WHICH IS NOT, IS ALMOST ALWAYS AN
    # OVERSIGHT. On 26 September csat_invoice_line_gie was in exactly that state: its
    # parent invoice_line/FIELDGLASS_US was active, the satellite was not declared at
    # all, load_satellites_business skipped it, said "every satellite is inactive in this
    # lake", exited 0, and a 29/29-SUCCESS run produced no GIE satellite. Nothing here
    # objected, because an OMISSION is invisible to a check that only validates what IS
    # declared -- a typo raises SpecError, an absence raises nothing.
    #
    # So the combination must be DECLARED EITHER WAY: switched on, or written down here
    # with a reason. Silence is the one thing it cannot be.
    for _cb in sorted(_computed_bindings):
        _cent = _cb.split("/", 1)[0]
        _centity = next((e for e in model.entities if e.name == _cent), None)
        if _centity is None or not _centity.parents:
            continue
        _parent_active = any(
            b.split("/", 1)[0] == _centity.parents[0] for b in _resolved)
        if not _parent_active or _cb in _resolved:
            continue
        _excuse = _COMPUTED_HELD_BACK.get(_tname, {}).get(_cb)
        check(f"target {_tname}: {_cb} is inactive while its parent "
              f"{_centity.parents[0]} is active -- so it is recorded as deliberate",
              bool(_excuse),
              f"{_cb} computes from {_centity.parents[0]}, which this lake loads, and is "
              f"not declared active. That is either an oversight -- the shape that cost "
              f"a whole green run on 26 Sep -- or a decision. If a decision, name it in "
              f"_COMPUTED_HELD_BACK with the reason; if not, add it to active_sources")
    check(
        f"target {_tname}: every active binding reads a table in this lake's bronze",
        _resolved_bronze == _real_bindings,
        f"declared-but-not-real: {sorted(_resolved_bronze - _real_bindings)}; "
        f"real-but-not-declared: {sorted(_real_bindings - _resolved_bronze)}",
    )

_targets_with = [n for n, t in ((bundle or {}).get("targets", {}) or {}).items()
                 if ((t.get("variables") or {}).get("active_sources") or "").strip()]
# THE SYNTHETIC ROWS. Bronze's invoices table carries test scaffolding mixed in with the
# real extract, and it is not marked by a column anyone would think to look at. Measured
# 24 September over the 1,207 AEE1 rows: 35 come from two synthetic loads, and they are the
# reason the first duplicate measurement read "keeping every row invents 44,050.32". It did
# not. Those were the test rows. On the real extract, keeping every row reconciles to the
# penny and deduplicating loses 304.99.
#
# The filter names FOUR marker columns because bronze offers no single flag: Is_Test is set
# on 5 of the 35, Mike_Tester on 25, PO_2 on 10, META_run_id on 30. No one of them covers
# the set; together they cover it exactly (measured: 0 disagreements against excluding the
# two load dates outright). Dropping any one clause silently readmits synthetic money into
# a vault whose whole claim is that its totals tie back, so each is asserted by name.
_SYNTHETIC_MARKERS = ("Is_Test", "Mike_Tester", "META_run_id", "PO_2")
_fg = [u for u in (_su_decl.get("unions") or []) if u["name"] == "fieldglass_us_invoice"]
check("the fieldglass_us_invoice union is still declared",
      len(_fg) == 1,
      f"found {len(_fg)} -- the synthetic-row gate below has nothing to check without it")
if len(_fg) == 1:
    _filter = _fg[0].get("row_filter") or ""
    _missing = [m for m in _SYNTHETIC_MARKERS if m not in _filter]
    check("the fieldglass_us_invoice row_filter excludes every synthetic-row marker",
          not _missing,
          f"row_filter does not mention {_missing} -- bronze mixes 35 test-scaffolding rows "
          f"(43,143.12 of invented money across 3 invoices) into the real Ameren extract, and "
          f"no single marker covers them all; a filter missing one loads synthetic invoices "
          f"into the vault")

check("the usnc targets declare their active sources",
      set(_targets_with) >= {"usnc_tds", "dev"},
      f"got {sorted(_targets_with)} -- usnc_tds is where this sub-project deploys, and "
      f"the party hubs' placeholder bindings would fail it at definition time")


# --------------------------------------------------------------------------- #
# THE YEAR-END CLOSE. GP's annual close MOVES rows from gl20000 to gl30000. Bound
# to one entity, a migrated row re-arrived under an unchanged authored key and
# tripped append_only_check's NHL uniqueness annually, by construction. Two
# entities with one binding each resolve it structurally -- but the duplicate did
# not disappear, it moved to READ time: over its lifetime a line exists in BOTH
# tables, because the vault is insert-only and the open-year table keeps it.
#
# That is only safe if it is written where someone querying the tables will find
# it, which is why this is a checked property and not a review comment.
# --------------------------------------------------------------------------- #
print("\n[journals] the two general-journal tables document that they must be unioned")

_GJ_FILES = ("nhl_general_journal_line.yml", "nhl_general_journal_line_closed_year.yml")
for _f in _GJ_FILES:
    _path = ROOT / "metadata" / "entities" / _f
    check(f"{_f} exists", _path.is_file())
    if not _path.is_file():
        continue
    _text = _path.read_text(encoding="utf-8").lower()
    check(f"{_f} says a line lives in BOTH tables",
          "both" in _text and "insert-only" in _text,
          "the consequence moved to read time; a consumer who does not know it "
          "double-counts every migrated line and the totals stay plausible")
    check(f"{_f} tells the reader to union and deduplicate on the hash key",
          "union" in _text and "general_journal_line_hk" in _text,
          "naming the column is the difference between a warning and an instruction")
    check(f"{_f} names the other table",
          all(o.replace(".yml", "") in _text for o in _GJ_FILES if o != _f),
          "a reader who finds one file must be able to find the other")

# AND IT HAS TO REACH THE CATALOG, not just the repository. Someone who meets these two
# tables in Unity Catalog and writes a query against them never opens metadata/entities.
_fac_src = (ROOT / "src" / "accelerator" / "factory.py").read_text()
check("the factory propagates entity notes into the generated table comment",
      "_table_comment" in _fac_src and "entity.notes" in _fac_src,
      "a consequence that lives only in repository YAML reaches only the people who "
      "already know it")
for _n in ("general_journal_line", "general_journal_line_closed_year"):
    _notes = " ".join(model.get(_n).notes.split())
    check(f"{_n} declares notes:, not only a YAML comment", bool(_notes),
          "notes: is the field that becomes the catalog comment")
    check(f"{_n}'s notes carry the union-and-deduplicate instruction",
          "UNION" in _notes.upper() and "general_journal_line_hk" in _notes,
          f"{_notes[:120]!r}")

_gj_open = model.get("general_journal_line")
_gj_closed = model.get("general_journal_line_closed_year")
check("each general-journal entity binds exactly one GP table",
      len(_gj_open.sources) == 1 and len(_gj_closed.sources) == 1,
      "one binding each is what makes a migrated row arrive in a DIFFERENT table")
check("the two entities are structurally identical, so the union is well-defined",
      (_gj_open.parents, _gj_open.transaction_key, _gj_open.key_style, _gj_open.payload)
      == (_gj_closed.parents, _gj_closed.transaction_key, _gj_closed.key_style,
          _gj_closed.payload),
      "identical keys and structure are what make the same line hash the same in both")


# --------------------------------------------------------------------------- #
# THE JOURNAL GATE. It named line_order / debit_amount / credit_amount / company_hk
# -- the retired Workday-shaped binding's vocabulary, and hub_company's key column.
# No bound source delivers any of them, so against two of the three journal-line
# tables it asserted nothing at all while reading like full coverage.
# --------------------------------------------------------------------------- #
print("\n[journals] the integrity gate takes its column names from the metadata")

_jic = (ROOT / "checks" / "journal_integrity_check.py").read_text()
_jic_exec = _strip_comments(_jic)
for _dead in ("debit_amount", "credit_amount", "company_hk"):
    check(f"the gate no longer hardcodes {_dead}",
          _dead not in _jic_exec,
          f"{_dead} is not delivered by any bound source; naming it asserts nothing")
# `line_order` is still a legitimate ROLE NAME -- the key an entity declares in its
# accounting: block, and the word appears in this gate's own messages. What must not
# survive is the bare identifier IN SQL, which is what the gate used to interpolate. So
# scan only the lines that carry SQL.
_SQL_MARKERS = ("SELECT ", "FROM ", "WHERE ", "GROUP BY", "HAVING ", "COUNT(", "SUM(",
                "JOIN ", "MAX(")
_sql_lines = [l for l in _jic_exec.splitlines()
              if any(m in l.upper() for m in _SQL_MARKERS)]
check("the gate's SQL carries no hardcoded journal column name",
      _sql_lines and not [l for l in _sql_lines
                          if any(d in l for d in ("line_order", "debit_amount",
                                                  "credit_amount", "company_hk"))],
      f"{[l.strip() for l in _sql_lines if any(d in l for d in ('line_order', 'debit_amount', 'credit_amount', 'company_hk'))]}"
      f" -- no bound source delivers those columns: GP calls the ordinal seqnumbr and "
      f"UKG has none at all")
check("it reads the accounting roles off the entity",
      "accounting_map" in _jic_exec,
      "the names must come from metadata/entities/*.yml, not from this file")
check("it derives the parents it ghost-checks from the entity",
      "entity.parents" in _jic_exec,
      "a hardcoded parent list is how company_hk survived hub_company being folded away")
check("it reports properties it could not evaluate instead of skipping them",
      "not_evaluated" in _jic_exec and "NOT EVALUATED" in _jic,
      "an absent control total or line ordinal must be stated, not silently dropped")
check("it still asserts all three properties",
      all(t in _jic_exec for t in ("TOLERANCE", "control_total", "distinct_orders")),
      "balance within tolerance, agreement with a declared control total, and a dense "
      "unique ordering column")
check("it treats an all-NULL amount column as a failure, not as a pass",
      "amounts_visible" in _jic_exec,
      "masked amounts read NULL for an unprivileged run-as identity, and every journal "
      "then 'balances' on zeros -- DEPLOY.md Phase 6b")
check("every entity declaring accounting roles names columns it actually carries",
      not [f"{e.name}.{r}={c}" for e in model.entities for r, c in e.accounting
           if r in ("debit", "credit", "line_order")
           and c not in set(e.payload) | set(e.transaction_key)],
      "the generator renames nothing, so a role naming an absent column would make the "
      "gate fail at SQL analysis -- or worse, assert nothing")
check("at least one entity declares a control total, so property 2 is reachable",
      any("control_total" in e.accounting_map for e in model.entities),
      "with none declared the gate can only ever report it as NOT EVALUATED")


# --------------------------------------------------------------------------- #
# KEY COMPOSITION. spec.py guards satellite payload ORDER and pins the rulebook
# version, but nothing guards business_keys, transaction_key, parents, key_style,
# tenant_key, or a source binding's key_columns, key_literals or parent_keys. Those
# define identity: changing one re-keys every row already loaded, and the rulebook
# version does not move, so no existing check sees it. spec.py's own validate() only
# checks a COUNT on key_columns/key_literals (they must add up to len(business_keys)),
# never that they are the SAME columns -- so a rewired parent_keys or a swapped
# key_columns entry passes every existing gate while quietly re-keying a foreign key.
# metadata/key_composition.json is the committed acknowledgement --
# tools/refresh_key_composition.py regenerates it, and the diff IS the review.
#
# Computed independently of spec.load_model (used elsewhere in this file) rather
# than off `model`: a disappearing hub/link that is still some other entity's
# declared parent makes spec.load_model raise SpecError -- appropriate for that
# check, but it would take this one down with it before it ever got to report
# "no entity silently disappeared" by name. tools/refresh_key_composition.py reads
# the YAML directly for exactly this reason, and is imported (not re-implemented)
# here so the checker and the refresher can never compute two different digests
# for the same metadata.
# --------------------------------------------------------------------------- #
print("\n[keys] entity key composition matches the committed digest")

sys.path.insert(0, str(ROOT / "tools"))
import refresh_key_composition as _rkc  # noqa: E402

_kc_path = ROOT / "metadata" / "key_composition.json"
_kc_exists = _kc_path.is_file()
check("metadata/key_composition.json exists", _kc_exists,
      "run tools/refresh_key_composition.py and review the diff")

if _kc_exists:
    _expected = json.loads(_kc_path.read_text(encoding="utf-8"))
    _actual = _rkc.compute()
    for _n, _h in sorted(_actual.items()):
        check(f"key composition unchanged: {_n}", _expected.get(_n) == _h,
              f"identity changed -- re-keys loaded rows; expected "
              f"{_expected.get(_n)!r}, got {_h!r}. If this is intended, run "
              f"tools/refresh_key_composition.py and review the diff")
    _vanished = sorted(set(_expected) - set(_actual))
    check("no entity silently disappeared from metadata", not _vanished,
          f"in metadata/key_composition.json but no longer in "
          f"metadata/entities/*.yml: {_vanished}")


# --------------------------------------------------------------------------- #
# DATA CONTRACTS. THE DIFF IS THE REVIEW, exactly as for key_composition.json above. A
# generated artefact that is committed but never re-checked is a document that describes
# what the model USED to say. Regenerate every contract and compare; a difference means
# someone changed the model without regenerating, or edited the contract by hand -- and
# the contract is not hand-editable, because the model is the only authority.
#
# tools/ is already on sys.path from the key_composition import above.
# --------------------------------------------------------------------------- #
print("\n[contracts] committed data contracts match what the model generates")

import emit_data_contract as _edc  # noqa: E402

_dc_dir = ROOT / "data_contracts"
_dc_targets = _edc.targets_and_variables()

# A vacuous PASS below (an empty or short list from targets_and_variables()) would mean
# the staleness check compared nothing and still reported green -- the exact hollow-gate
# failure mode this file exists to refuse. Checked here against databricks.yml directly,
# not against [4b]'s "target is declared" checks: those exist for a different reason and
# this gate must not depend on them still being there, still covering all 8, or still
# running before this point.
_declared_targets = set((bundle or {}).get("targets", {}) or {})
_dc_target_names = {_t for _t, _v in _dc_targets}
check("targets_and_variables() is non-empty and matches every target declared in "
      "databricks.yml",
      bool(_dc_targets) and _dc_target_names == _declared_targets,
      f"got {sorted(_dc_target_names)} against databricks.yml's "
      f"{sorted(_declared_targets)} -- an empty or short list here lets the staleness "
      f"check below pass having compared nothing")

_dc_missing = sorted(_t for _t, _v in _dc_targets
                      if not (_dc_dir / f"{_t}.yaml").is_file())
check("every target in scope has a committed data_contracts/<target>.yaml file",
      _dc_dir.is_dir() and not _dc_missing,
      f"missing: {_dc_missing} -- run tools/emit_data_contract.py and review the diff")

_dc_stale = []
for _t, _vars in _dc_targets:
    # emit() raises a NAMED ValueError for a target that resolves no `catalog` (the one
    # variable with no global default in databricks.yml). CAUGHT: an exception escaping
    # this module-level loop would abort verify_repo.py outright and turn every check
    # after the contracts gate silently ABSENT, which is worse than one red check
    # carrying the message.
    try:
        _want = _edc.render(_edc.emit(model, _t, _vars))
    except Exception as _exc:  # noqa: BLE001
        _dc_stale.append(f"{_t}: {type(_exc).__name__}: {_exc}")
        continue
    _path = _dc_dir / f"{_t}.yaml"
    if not _path.is_file() or _path.read_text(encoding="utf-8") != _want:
        _dc_stale.append(_t)
check("every committed data contract matches what the model generates",
      not _dc_stale,
      f"stale: {_dc_stale} -- run tools/emit_data_contract.py and review the diff")

# THE ARTEFACT NAMES REAL TABLES -- READ OFF THE COMMITTED FILE, not off emit(). The
# staleness check above proves committed == generated; this one proves generated ==
# reality, and reality is entity.tables(), which yields one physical table PER SOURCE
# BINDING for a satellite. An earlier emitter took tables()[0] and keyed on
# entity.base_table, publishing 7 names for tables that do not exist while omitting 11
# that do. Compared as SETS IN BOTH DIRECTIONS: that bug published a strict subset, so a
# one-directional "every published name is real" check would have passed on it.
_dc_pn_mismatch = []
for _t, _vars in _dc_targets:
    _path = _dc_dir / f"{_t}.yaml"
    if not _path.is_file():
        continue
    _doc = yaml.safe_load(_path.read_text(encoding="utf-8")) or {}
    _dc_vs = _vars.get("vault_schema", "raw_vault")
    _dc_bvs = _vars.get("business_vault_schema", "business_vault")
    _want_pn = {
        f"{_vars.get('catalog')}.{naming.vault_schema_for(_e.kind, _dc_vs, _dc_bvs)}.{_tab}"
        for _e in model.entities if _e.kind in naming.GENERATABLE
        for _s, _tab in _e.tables()}
    _got_pn = {(_v or {}).get("physical_name")
               for _v in ((_doc.get("entities") or {}).values())}
    if not _want_pn or _got_pn != _want_pn:
        _dc_pn_mismatch.append((_t, sorted(_got_pn - _want_pn),
                                sorted(_want_pn - _got_pn)))
check("every committed contract's physical_name set equals the set of tables the model "
      "actually produces -- no fictional table named, no real table omitted",
      not _dc_pn_mismatch,
      f"{_dc_pn_mismatch} -- target, then published-but-not-real, then "
      f"real-but-not-published")

# THE FOREIGN KEYS ARE WHAT A CONSUMER FOLLOWS TO JOIN, AND THAT MUST NAME THE VIEW.
# `entities`/`physical_name` above are deliberately physical -- a contract describes
# TABLES, not entities (tools/emit_data_contract.py, "PER TABLE, AND DELIBERATELY
# PHYSICAL"): that is the real object a loader writes, and a reader auditing storage
# needs that identity. A foreign key is the opposite kind of fact: it is the one place
# in this document that tells a reader where to go NEXT, and contract.foreign_keys()
# already resolves it through the parent's unversioned base_table (Task 7, R2) -- a
# reference naming a physical `_rev<N>` table would bind a consumer to a name the next
# cutover moves, which is the one thing the view exists to prevent.
#
# data_contracts/<target>.yaml, not a single docs/ file -- one per target. Read
# whichever exist; an empty directory is itself a finding, because the byte-gate
# below would then be comparing nothing.
_fk_contract_dir = ROOT / "data_contracts"
_fk_contract_files = sorted(_fk_contract_dir.glob("*.yaml"))
check("at least one data contract exists to check",
      bool(_fk_contract_files),
      f"{_fk_contract_dir} holds no .yaml -- run tools/emit_data_contract.py. A check "
      f"that silently examines nothing is the vacuity this repo keeps having to name")

_fk_phys_tables = {p for e in model.entities for _s, p in e.tables()}
_fk_physical_refs = []
for _fkf in _fk_contract_files:
    # CAUGHT: a malformed YAML raising here would abort the whole suite -- every later
    # check silently ABSENT -- which is worse than reporting this one file unreadable.
    try:
        _fkdoc = yaml.safe_load(_fkf.read_text(encoding="utf-8")) or {}
    except Exception as _exc:  # noqa: BLE001
        _fk_physical_refs.append(f"{_fkf.name}: {type(_exc).__name__}: {_exc}")
        continue
    for _fk_tname, _fk_ent in (_fkdoc.get("entities") or {}).items():
        for _fk_col, _fk_ref in ((_fk_ent or {}).get("foreign_keys") or {}).items():
            _fk_ref_table = _fk_ref.split(".")[2] if _fk_ref.count(".") >= 2 else _fk_ref
            if _fk_ref_table in _fk_phys_tables:
                _fk_physical_refs.append(f"{_fkf.name}:{_fk_tname}.{_fk_col} -> {_fk_ref}")
check("every data contract's foreign key resolves to the stable view a consumer reads, "
      "never a physical version",
      not _fk_physical_refs,
      f"{_fk_physical_refs[:5]} -- a consumer binding to a _revN name is bound to a "
      f"name the next cutover moves")

# A CONTRACT MUST SAY WHICH NAME TO QUERY.
#
# physical_name is the versioned table and is correct -- a contract describes tables, and
# a satellite has one per source. But a consumer reads a contract to find out what to
# query, and a fully-qualified `physical_name` is what they will bind to. At the first
# cutover that consumer keeps reading the superseded version, silently. So every entry
# also carries stable_name, the unversioned name the view answers on.
_dc_bad_stable: list[str] = []
for _dc_file in sorted((ROOT / "data_contracts").glob("*.yaml")):
    try:
        _dc = yaml.safe_load(_dc_file.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as _exc:
        _dc_bad_stable.append(f"{_dc_file.name}: unreadable: {_exc}")
        continue
    for _name, _entry in sorted((_dc.get("entities") or {}).items()):
        _phys = str((_entry or {}).get("physical_name", ""))
        _stab = str((_entry or {}).get("stable_name", ""))
        if not _stab:
            _dc_bad_stable.append(f"{_dc_file.name}:{_name}: no stable_name")
        elif re.search(r"_rev[1-9][0-9]*$", _stab):
            _dc_bad_stable.append(f"{_dc_file.name}:{_name}: stable_name {_stab} is versioned")
        elif not re.search(r"_rev[1-9][0-9]*$", _phys):
            _dc_bad_stable.append(f"{_dc_file.name}:{_name}: physical_name {_phys} is NOT versioned")
        elif _phys.rsplit("_rev", 1)[0] != _stab:
            _dc_bad_stable.append(
                f"{_dc_file.name}:{_name}: {_stab} is not the stable form of {_phys}")
check("every contract entity carries a stable_name that is the unversioned physical_name",
      not _dc_bad_stable,
      f"{_dc_bad_stable[:6]} -- a consumer binds to the name a contract gives it, and a "
      f"versioned one moves out from under them at the next cutover, silently")


# --------------------------------------------------------------------------- #
print("\n[source contracts] what Silver requires of Bronze matches the model")

import emit_source_contract as _esc2  # noqa: E402

# THE DIFF IS THE REVIEW -- the discipline metadata/key_composition.json and data_contracts/
# already carry. CAUGHT rather than raised: an exception escaping a module-level block aborts
# verify_repo.py and turns every later check silently ABSENT, which is worse than one red check
# carrying the message. Errors and staleness are reported SEPARATELY, because "run the emitter
# and review the diff" is the wrong remedy for a broken emitter and will only reproduce the
# crash.
_sc_stale, _sc_errors, _sc_missing = [], [], []
# `model` and `spec` are already loaded at verify_repo.py:551-553 -- reuse them.
# Reloading the model here would be a second parse of the same files and a
# second place for the entities directory to be named.
_sc_model = model
# CACHED, NOT RE-CALLED. emit() runs ONCE per target here; every check below that needs
# the emitted structure reads it back out of _sc_structures instead of calling emit()
# again. A first version of this section called emit() a second (and third, and fourth)
# time per target from the invented-column, dedup_by and raw_vault checks below, each
# with no try/except of its own -- so a broken emitter (mutation-proved: `import
# nonexistent_module_xyz` inside emit()) was correctly caught and reported by name here,
# but then raised AGAIN, uncaught, from one of those later loops and aborted
# verify_repo.py outright -- silencing every check after it, including the ones in this
# very section, exactly the failure mode this section's own comment above warns against.
_sc_structures: dict[str, dict] = {}
# BUILT ONCE, REUSED EVERYWHERE BELOW. targets_and_variables() is a re-export of
# emit_data_contract's resolution, not re-implemented here (see its own definition) --
# this just avoids calling it three separate times for the same result.
_sc_target_vars = dict(_esc2.targets_and_variables())

# A vacuous PASS below (an empty or short _sc_target_vars) would mean every check in
# this section compared nothing and every accumulator (_sc_stale, _sc_errors, _sc_missing,
# _sc_role_drift, _sc_key_drift, ...) stayed empty by construction -- the exact hollow-gate
# failure mode the data_contracts precedent guards against at verify_repo.py:1938-1944,
# whose own comment says this kind of gate "must not depend on [other checks] still being
# there" -- so this is an INDEPENDENT guard, checked against databricks.yml directly, not
# against the data_contracts guard above still running or still covering all targets.
_sc_declared_targets = set((bundle or {}).get("targets", {}) or {})
check("source contracts: targets_and_variables() is non-empty and matches every target "
      "declared in databricks.yml",
      bool(_sc_target_vars) and set(_sc_target_vars) == _sc_declared_targets,
      f"got {sorted(_sc_target_vars)} against databricks.yml's "
      f"{sorted(_sc_declared_targets)} -- an empty or short list here lets every check "
      f"in this section pass having compared nothing")

# ONLY THE TARGETS THAT DECLARE WHAT THEY LOAD. See emit_source_contract.configured_targets():
# a target with no active_sources has not said which bindings are real, and a contract for it
# would name every table in the model including the placeholders. The vacuity guard above still
# checks the FULL target list, so this narrowing cannot hide a missing target.
_sc_configured = dict(_esc2.configured_targets())
_sc_unconfigured = sorted(set(_sc_target_vars) - set(_sc_configured))

check("a source contract exists for every target that declares active_sources, and for no "
      "other",
      bool(_sc_configured)
      and all(_esc2.contract_path(_t).is_file() for _t in _sc_configured)
      and not [_t for _t in _sc_unconfigured if _esc2.contract_path(_t).is_file()],
      f"configured={sorted(_sc_configured)}; "
      f"missing={[_t for _t in _sc_configured if not _esc2.contract_path(_t).is_file()]}; "
      f"present but undeclared="
      f"{[_t for _t in _sc_unconfigured if _esc2.contract_path(_t).is_file()]} -- a lake that "
      f"has not declared its Bronze gets no contract, and a lake that has must have one")

for _sc_target, _sc_vars in _sc_configured.items():
    _sc_path = _esc2.contract_path(_sc_target)
    if not _sc_path.is_file():
        _sc_missing.append(_sc_target)
    try:
        _sc_structures[_sc_target] = _esc2.emit(_sc_model, _sc_target, _sc_vars)
    except Exception as _exc:  # noqa: BLE001
        _sc_errors.append(f"{_sc_path.name}: {type(_exc).__name__}: {_exc}")
        continue
    if _sc_path.is_file() and _sc_path.read_text(encoding="utf-8") != _esc2.render(
            _sc_structures[_sc_target]):
        _sc_stale.append(_sc_path.name)

check("every target in scope has a committed source_contracts/<target>.yaml",
      not _sc_missing,
      f"missing: {_sc_missing} -- run tools/emit_source_contract.py")
check("generating every source contract raises no error",
      not _sc_errors,
      f"the emitter itself is broken, not stale: {_sc_errors} -- fix "
      f"tools/emit_source_contract.py; re-running it will only reproduce this crash")
check("every committed source contract matches what the model generates",
      not _sc_stale,
      f"stale: {_sc_stale} -- run tools/emit_source_contract.py and review the diff")

# RULING T4-B: PER-TABLE, PER-ROLE SET EQUALITY, RECOMPUTED FROM THE MODEL --
# PROVENANCE, NOT MEMBERSHIP. Two earlier checks (a model-WIDE "every emitted column is
# declared somewhere in the model" check, and a since-deleted dedup_by-scoped check) both
# tested column MEMBERSHIP against a set pooled across the whole model, with no notion of
# WHICH table or WHICH binding a column has to come from. Both share that one root cause,
# and both were disproved by mutation:
#   - folding dex_row_ts into ledger_account/GP_US's dedup_by (dex_row_ts is declared
#     elsewhere in the model, just not as THIS binding's dedup_by, and not for THIS
#     table) left a model-wide check green, because dex_row_ts is declared *somewhere*.
#   - injecting paycheckdate (a real column, but declared on a UKG binding) into
#     ledger_account/GP_US's payload on great_plains_raw.gl00100 left the same check
#     green for the identical reason, and is the more consequential of the two: it is
#     Bronze-facing, telling the Bronze team gl00100 must carry a column nothing that
#     reads gl00100 actually needs.
# So this checks EXACT SET EQUALITY, per bronze table, per required role, against
# EXACTLY the bindings the model says read that table (recomputed here from spec.*
# directly -- never from the contract's own read_by list or from the emitted structure's
# column values, since the artefact under test cannot be trusted to grade itself).
# Omission and invention are therefore the same failure mode by construction: a role's
# set differs from what its own contributing bindings declare, in either direction.
#
# required_casts is covered the same way, but by KEY, not value: its keys are column
# names (required_casts is column -> sorted list of TYPE strings, Ruling T2-B) and are
# checked against the union of cast target columns across the table's bindings; the type
# strings themselves are not re-derived here.
#
# transaction_key is read from the ENTITY, not the binding (binding_requirements()'s own
# rule) -- so where a table is read by bindings from more than one entity, this unions
# transaction_key across every CONTRIBUTING entity, exactly matching how bronze_tables()
# merges it today.
_SC_SET_ROLES = ("business_keys", "parent_keys", "transaction_key", "payload", "dedup_by",
                 "applied_dts", "cdc_op", "manifest")

_SC_MISSING_ROLE = object()  # sentinel: the role key is entirely absent from the emitted
# structure (e.g. a role dropped from _SINGLE_ROLES/_MERGEABLE_LIST_ROLES), not merely
# present with an empty list. `.get(_role, _SC_MISSING_ROLE)` turns that drop into a
# reported drift below instead of a raw KeyError aborting the whole run -- the section's
# own comment above (verify_repo.py:2016-2021) exists to prevent exactly that.

# RULING (REVIEW I2): read_by IS PUBLISHED AND WAS ASSERTED BY NOTHING. MERGE_NOTE tells
# a Bronze reader to cross-reference read_by against metadata/entities/*.yml when roles
# disagree, but nothing checked read_by itself was correct -- mutation-proved: making the
# emitter publish `f"{entity.name}/BOGUS_SOURCE"` for every binding left both suites
# green. Recomputed here from spec.* directly (never from the contract's own read_by list
# or the emitted structure -- the artefact under test cannot grade itself, same principle
# as Ruling T4-B above): for each table, read_by must equal exactly the sorted set of
# "{entity.name}/{src.name}" for the bindings the MODEL says actively read that table,
# excluding NOT_BRONZE_SOURCE.
_sc_read_by_drift = []

_sc_role_drift = []
for _sc_target, _sc_struct in _sc_structures.items():
    _sc_active = spec.resolve_active_sources(
        _sc_model, _sc_target_vars[_sc_target].get("active_sources"))
    _sc_bindings_by_table: dict[str, list] = {}
    for _e in _sc_model.entities:
        for _s in _e.sources:
            if _esc2.is_business_vault_binding(_s.name):
                continue
            if not spec.active_table_bindings(_e, _s, _sc_active):
                continue
            _sc_bindings_by_table.setdefault(_s.bronze_table, []).append((_e, _s))

    for _tbl, _bindings in _sc_bindings_by_table.items():
        _slot = _sc_struct["bronze_tables"].get(_tbl)
        if _slot is None:
            continue  # an absent table is reported by name below, by the key-drift check
        _req = _slot["requires"]

        _sc_expected_read_by = sorted(f"{_e.name}/{_s.name}" for _e, _s in _bindings)
        _sc_got_read_by = _slot.get("read_by", [])
        if _sc_got_read_by != _sc_expected_read_by:
            _sc_read_by_drift.append(
                f"{_sc_target}/{_tbl}: missing "
                f"{sorted(set(_sc_expected_read_by) - set(_sc_got_read_by))}, extra "
                f"{sorted(set(_sc_got_read_by) - set(_sc_expected_read_by))}")

        # A DERIVED COLUMN IS NOT A DEPENDENCY ON BRONZE. A conformed binding computes its
        # profile's columns in the flow, so the contract must not name them -- Bronze has
        # never had them and never will. Recomputed here from the model rather than taken
        # from the emitter, so the two derivations stay independent and a drift in either
        # is still caught; the RULE is shared, the arithmetic is not.
        #
        # Measured 25 September: the first contract emitted after the profiles moved out of
        # views named eight columns the source does not carry, and assert_source_conformance
        # stopped the load reporting them "not present" -- correct by its own rule, wrong
        # about the world.
        _sc_expected = {_role: set() for _role in _SC_SET_ROLES}
        _sc_expected["required_casts"] = set()
        for _e, _s in _bindings:
            _sc_derived = {_c for _c, _x in (getattr(_s, "derived_columns", ()) or ())}

            def _sc_keep(cols, _d=_sc_derived):
                return set(cols) - _d

            _sc_expected["business_keys"].update(_sc_keep(_s.key_columns))
            for _p, _cols in _s.parent_keys:
                _sc_expected["parent_keys"].update(_sc_keep(_cols))
            _sc_expected["transaction_key"].update(_sc_keep(_e.transaction_key or ()))
            _sc_expected["payload"].update(_sc_keep(_s.payload))
            _sc_expected["dedup_by"].update(_sc_keep(_s.dedup_by))
            if _s.applied_dts_column and _s.applied_dts_column not in _sc_derived:
                _sc_expected["applied_dts"].add(_s.applied_dts_column)
            if _s.cdc_op_column:
                _sc_expected["cdc_op"].add(_s.cdc_op_column)
            if _s.manifest_column:
                _sc_expected["manifest"].add(_s.manifest_column)
            _sc_expected["required_casts"].update(
                col for col, _t in _s.cast if col not in _sc_derived)

        for _role, _exp_set in _sc_expected.items():
            _got_raw = _req.get(_role, _SC_MISSING_ROLE)
            _got_set = (set(_got_raw) if _role != "required_casts"
                        else set(_got_raw.keys())) if _got_raw is not _SC_MISSING_ROLE else set()
            if _got_set != _exp_set:
                _sc_role_drift.append(
                    f"{_sc_target}/{_tbl}/{_role}: missing {sorted(_exp_set - _got_set)}, "
                    f"extra {sorted(_got_set - _exp_set)}")

check("every required role's columns, per bronze table, equal exactly what that "
      "table's active bindings declare in the model",
      not _sc_role_drift,
      f"{_sc_role_drift} -- omission tells Bronze a dependency does not exist; an "
      f"invented or misattributed column sends them defending something nothing that "
      f"reads this table actually needs")

check("read_by, per bronze table, equals exactly the sorted set of entity/source "
      "bindings the model says actively read it",
      not _sc_read_by_drift,
      f"{_sc_read_by_drift} -- MERGE_NOTE tells a Bronze reader to cross-reference "
      f"read_by against metadata/entities/*.yml; a wrong read_by sends them to the "
      f"wrong binding, or to none")

# MERGE_NOTE (tools/emit_source_contract.py) states, as a fixed English sentence, that
# "every populated list below has exactly one entry" for applied_dts/cdc_op/manifest --
# true when it was written, checked directly against the model on 28 Aug, but nothing
# recomputes it. It is a STATIC STRING: the day two active bindings on one table disagree
# on one of these three roles, bronze_tables() unions both values into a two-entry list
# (by design -- see MERGE_NOTE and its own "TASK 3 DECISION, DEFERRED MINOR 1" comment
# above emit_source_contract.py), every one of the nine committed files keeps printing a
# now-false sentence, and no other gate here would notice, because nothing else reads
# MERGE_NOTE's CONTENT against the data -- the caveat check just above asserts the text
# survives verbatim, not that it is still true. So this asserts the fact MERGE_NOTE
# claims: if it ever fires, MERGE_NOTE's sentence is wrong and must be rewritten, not
# just regenerated.
_sc_multi_valued = []
for _sc_target, _sc_struct in _sc_structures.items():
    for _tbl, _slot in _sc_struct["bronze_tables"].items():
        for _role in ("applied_dts", "cdc_op", "manifest"):
            _vals = _slot["requires"].get(_role, ())
            if len(_vals) > 1:
                _sc_multi_valued.append(f"{_sc_target}/{_tbl}/{_role}: {_vals}")
check("no contract's applied_dts/cdc_op/manifest list has more than one entry -- the "
      "fact MERGE_NOTE states as a fixed sentence",
      not _sc_multi_valued,
      f"{_sc_multi_valued} -- two active bindings on the same table now disagree on one "
      f"of these roles; MERGE_NOTE's claim that every populated list has exactly one "
      f"entry is false and its wording in tools/emit_source_contract.py must change")

_sc_emitted_cols = set()
for _sc_target, _sc_struct in _sc_structures.items():
    for _tbl, _slot in _sc_struct["bronze_tables"].items():
        _req = _slot["requires"]
        # declared_not_required is a SIBLING of `requires` (Ruling I8), not a member of
        # it, so it never turns up in this loop at all -- there is no key to skip here
        # any more; the not-required dedup_order columns simply never enter `_req`.
        for _role, _value in _req.items():
            # Every other role's _value is column-name-shaped. The single roles
            # (applied_dts/cdc_op/manifest) and the mergeable list roles are lists of
            # column names (Task 3 normalised this, empty when unneeded);
            # required_casts is a dict keyed by column name, mapping to a sorted
            # LIST of type strings (Ruling T2-B). set.update() over a dict adds its
            # KEYS, not its values, so this one line collects column names from
            # both shapes without ever admitting a type string into the set -- there
            # is no special case to write here, only one to name.
            _sc_emitted_cols.update(_value)

# RULING T4-A: THE GENERAL PROPERTY, COMPUTED FROM THE MODEL, NOT A LITERAL LIST. A first
# version of this check was scoped to "no dedup_order column in dedup_by specifically" to
# dodge a false positive: dex_row_ts is legitimately BOTH the applied_dts column AND a
# dedup_order tiebreaker on every great_plains_raw GL table, so checking dedup_order
# against every required role fired on that correct overlap. But the real invariant was
# never about dedup_by in particular -- it is that a column whose ONLY declared role
# anywhere in the model is dedup_order must never be asked of Bronze as required,
# regardless of which required role it turns up in. Measured directly against the model:
# dedup_order names three columns across all bindings (dex_row_ts, input_file_name,
# timestamp); dex_row_ts also has an unrelated required role (applied_dts) so it is
# rightly exempt, but input_file_name and timestamp do not -- and a literal list here
# would have named only input_file_name (Spec Sec 4's own example), silently missing
# timestamp, the dedup_order tiebreaker on hub_accounting_journal, hub_operating_company and
# hub_job_request. So the set is computed, not listed: union every dedup_order column,
# union every column with any OTHER declared role (key_columns, payload, dedup_by, the
# entity's transaction_key, parent_keys values, applied_dts/cdc_op/manifest, cast
# targets), and subtract -- catching a future tiebreaker column the same way it caught
# timestamp, with nobody needing to remember to extend a list. This does NOT subsume the
# earlier dedup_by-scoped check -- that subsumption is provided by the per-role,
# per-table set-equality check above (verify_repo.py:2133), which already catches a
# dedup_order-only column folded into dedup_by as a plain extra-column drift on the
# dedup_by role. What this check covers is a different, global property that the
# per-role check cannot see: a column with no OTHER declared role anywhere in the model
# must never be required at all, regardless of which role it is folded into. Both checks
# are needed -- the per-role check catches misattribution within a table's declared
# roles, this one catches a column that has no legitimate required role in the first
# place.
_sc_other_role_cols = set()
_sc_dedup_order_cols = set()
for _e in _sc_model.entities:
    _sc_other_role_cols.update(_e.transaction_key or ())
    for _s in _e.sources:
        _sc_dedup_order_cols.update(_s.dedup_order)
        _sc_other_role_cols.update(_s.key_columns)
        _sc_other_role_cols.update(_s.payload)
        _sc_other_role_cols.update(_s.dedup_by)
        for _p, _cols in _s.parent_keys:
            _sc_other_role_cols.update(_cols)
        for _c in (_s.applied_dts_column, _s.cdc_op_column, _s.manifest_column):
            if _c:
                _sc_other_role_cols.add(_c)
        _sc_other_role_cols.update(col for col, _t in _s.cast)
_sc_dedup_order_only = _sc_dedup_order_cols - _sc_other_role_cols

check("no column whose only declared role is dedup_order appears in any contract's "
      "required roles",
      not (_sc_emitted_cols & _sc_dedup_order_only),
      f"{sorted(_sc_emitted_cols & _sc_dedup_order_only)} -- dedup_order is not applied "
      f"at runtime (factory.py:367), and a column with no other declared role is not "
      f"something Bronze actually needs to supply for this lake -- input_file_name is "
      f"Spark file metadata, not a Bronze column at all")
# SPEC SECTION 6 ROW 3, IN BOTH DIRECTIONS. A dict key cannot repeat, so "exactly once" is
# really set equality: every bronze table an active non-BUSINESS_VAULT binding reads appears,
# and nothing else does. Omission tells Bronze a dependency does not exist; an extra key sends
# them after a table this lake never reads. The self-review of this plan found this row had no
# task -- it is here because a spec requirement with no check is the gap that mutation-proving
# is structurally blind to.
_sc_key_drift = []
for _sc_target, _sc_struct in _sc_structures.items():
    _sc_active = spec.resolve_active_sources(
        _sc_model, _sc_target_vars[_sc_target].get("active_sources"))
    # The contract excludes union views for the same reason it excludes BUSINESS_VAULT:
    # it is handed to the Bronze team, and a view our own job creates in our own catalog is
    # not theirs to act on. Reuses emit_source_contract's own helper rather than repeating
    # the rule -- a second copy here could diverge from what is actually published.
    _sc_want = {s.bronze_table for e in _sc_model.entities for s in e.sources
                if not _esc2.is_business_vault_binding(s.name)
                and not any(s.bronze_table.endswith(v)
                            for v in _esc2.union_view_suffixes())
                and spec.active_table_bindings(e, s, _sc_active)}
    _sc_got = set(_sc_struct["bronze_tables"])
    if _sc_want != _sc_got:
        _sc_key_drift.append(
            f"{_sc_target}: missing {sorted(_sc_want - _sc_got)}, extra {sorted(_sc_got - _sc_want)}")
check("each target's contract names exactly the bronze tables its active bindings read",
      not _sc_key_drift,
      f"{_sc_key_drift} -- a missing table tells Bronze a dependency does not exist; an extra "
      f"one sends them after a table this lake never reads")

# THE CHECK SPEC SECTION 6 SHOULD HAVE HAD, and the one that would have caught the placeholder
# defect. Every table a contract names must live in THAT TARGET'S OWN bronze catalog. Measured
# 28 Aug before this check existed: the seven targets that declare no active_sources published 19
# tables each, of which ZERO were in their own bronze catalog -- 14 placeholders per
# DEPLOY.md:236, plus five 01_usnc_bronze_dev tables offered to WEU, UKS and AUE readers as their
# stated requirement. A contract naming a table in another lake's catalog is not something that
# lake's Bronze team can act on.
_sc_foreign = []
for _sc_ft, _sc_fv in _esc2.targets_and_variables():
    _sc_bc = _sc_fv.get("bronze_catalog")
    _sc_fp = _esc2.contract_path(_sc_ft)
    if not _sc_bc or not _sc_fp.is_file():
        continue
    _sc_fdoc = yaml.safe_load(_sc_fp.read_text(encoding="utf-8")) or {}
    for _sc_tbl in sorted(_sc_fdoc.get("bronze_tables") or {}):
        if not _sc_tbl.startswith(_sc_bc + "."):
            _sc_foreign.append(f"{_sc_ft} ({_sc_bc}): {_sc_tbl}")
check("every table a source contract names is in that target's own bronze catalog",
      not _sc_foreign,
      f"{len(_sc_foreign)} foreign or placeholder table(s), e.g. {_sc_foreign[:4]} -- a contract "
      f"naming another lake's catalog, or a table DEPLOY.md says will not exist, cannot be acted "
      f"on by the team it is handed to")


check("no source contract names a raw_vault table",
      not [t for _sc_struct in _sc_structures.values()
           for t in _sc_struct["bronze_tables"]
           if ".raw_vault." in t],
      "BUSINESS_VAULT bindings read our own vault; publishing those names tables the Bronze "
      "team does not own")

# RULING T3-C: A KEY CHECK IS BLIND TO A BLANKED VALUE. YAML always emits a key's name as
# literal text regardless of its value, so `all(k in file_text for k in LIMITS)` (the brief's
# original check) passes on a committed file whose caveat TEXT has been silently blanked --
# keys intact. The load-bearing content is the text, not the key names. Per Ruling T3-A,
# merge_note gets the same treatment: it is a caveat a reader depends on exactly like the
# limits, so a committed file losing it must redden something more specific than the
# staleness gate. STANDING gets the same treatment for the same reason: it carries the one
# claim spec Sec 7 says this repo cannot afford to lose -- that this contract is NOT an
# agreement Bronze has countersigned -- and a reader who takes a blanked `standing:` at
# face value could mistake a unilateral dependency statement for a signed-off contract.
# This is therefore a per-file PARSED comparison against the actual STANDING/LIMITS/
# MERGE_NOTE values, immune to yaml.safe_dump's ~80-column line wrapping on long strings,
# which a substring test on the caveat VALUES (not just the keys) would not be.
_sc_caveat_drift = []
for _sc_t in _sc_target_vars:
    _sc_p = _esc2.contract_path(_sc_t)
    if not _sc_p.is_file():
        continue
    try:
        _sc_parsed = yaml.safe_load(_sc_p.read_text(encoding="utf-8")) or {}
    except Exception as _exc:  # noqa: BLE001
        _sc_caveat_drift.append(f"{_sc_p.name}: {type(_exc).__name__}: {_exc}")
        continue
    if _sc_parsed.get("standing") != _esc2.STANDING:
        _sc_caveat_drift.append(f"{_sc_p.name}: standing")
    if _sc_parsed.get("limits") != _esc2.LIMITS:
        _sc_caveat_drift.append(f"{_sc_p.name}: limits")
    if _sc_parsed.get("merge_note") != _esc2.MERGE_NOTE:
        _sc_caveat_drift.append(f"{_sc_p.name}: merge_note")
check("standing, limits and the merge note all survive intact in every committed source "
      "contract",
      not _sc_caveat_drift,
      f"{_sc_caveat_drift} -- a caveat that can be silently blanked is worth little -- five "
      f"defects on the preceding branch were a claim outliving what justified it")


# --------------------------------------------------------------------------- #
print("\n[quality] the dashboard artefacts match the definition")

import emit_quality_dashboard as _eqd  # noqa: E402

from accelerator import quality as _ql  # noqa: E402

_ql_dir = ROOT / "dashboards"

# ANTI-VACUITY, the same guard the data-contract gate carries: an empty ARTEFACTS mapping
# would make every staleness check below compare nothing and still report green.
check("emit_quality_dashboard.ARTEFACTS is non-empty and names both renderings",
      set(_eqd.ARTEFACTS) == {"quality_silver", "quality_silver_synthetic"},
      f"got {sorted(_eqd.ARTEFACTS)} -- an empty or short mapping lets the staleness "
      f"checks below pass having compared nothing")

_ql_stale = []
# PER TARGET, because the table names inside are qualified with that target's catalog.
for _ql_t, _ql_vars in _dc_targets:
    _ql_cat = _ql_vars.get("catalog", "")
    _ql_cs = _ql_vars.get("control_schema", "control")
    for _stem, _prefix in _eqd.ARTEFACTS.items():
        _fn = f"{_stem}_{_ql_t}.lvdash.json"
        _p = _ql_dir / _fn
        try:
            _want = _eqd.render(_eqd.dashboard(_prefix, _ql_cat, _ql_cs))
        except Exception as _exc:  # noqa: BLE001
            # CAUGHT for the same reason the contract gate catches: an exception escaping
            # this module-level loop aborts verify_repo.py and turns every later check
            # ABSENT, which is worse than one red check carrying the message.
            _ql_stale.append(f"{_fn}: {type(_exc).__name__}: {_exc}")
            continue
        if not _p.is_file() or _p.read_text(encoding="utf-8") != _want:
            _ql_stale.append(_fn)

_ql_gp = _ql_dir / _eqd.GENIE_ARTEFACT
try:
    _ql_gp_want = _eqd.render(_eqd.genie_space())
except Exception as _exc:  # noqa: BLE001
    # CAUGHT for the same reason the staleness loop above catches: an exception escaping
    # this module-level block would abort verify_repo.py and turn every later check
    # ABSENT, which is worse than one red check carrying the message.
    _ql_stale.append(f"{_eqd.GENIE_ARTEFACT}: {type(_exc).__name__}: {_exc}")
else:
    if not _ql_gp.is_file() or _ql_gp.read_text(encoding="utf-8") != _ql_gp_want:
        _ql_stale.append(_eqd.GENIE_ARTEFACT)

check("every committed quality artefact matches what the definition generates",
      not _ql_stale,
      f"stale: {_ql_stale} -- run tools/emit_quality_dashboard.py and review the diff")

# THE BOUNDARY GATES, read off the COMMITTED files rather than off dashboard(). The
# staleness check above proves committed == generated; these prove the committed text obeys
# the governance boundary, so a hand-edit that slipped past regeneration is still caught.
# PER-TARGET ARTEFACTS. Until 27 Aug these gates read one fixed filename, because the
# artefact was meant to serve every target through the bundle's dataset_catalog. That did
# not reach the deployed dashboard, so the names are now qualified and the artefact is per
# target. A gate reading one fixed name would now examine NOTHING -- so every one below
# sweeps all of them, and the non-emptiness of the sweep is part of each condition.
_ql_business_files = sorted(_ql_dir.glob("quality_silver_*.lvdash.json"))
_ql_business_files = [f for f in _ql_business_files if "_synthetic_" not in f.name]
_ql_business = _ql_business_files[0] if _ql_business_files else _ql_dir / "__absent__"
_ql_text = "".join(f.read_text(encoding="utf-8") for f in _ql_business_files)
_ql_syn_files = sorted(_ql_dir.glob("quality_silver_synthetic_*.lvdash.json"))
_ql_syn = _ql_syn_files[0] if _ql_syn_files else _ql_dir / "__absent__"

check("a business and a synthetic dashboard artefact exist for every declared target",
      bool(_ql_business_files) and bool(_ql_syn_files)
      and len(_ql_business_files) == len(_ql_syn_files) == len(_dc_targets),
      f"{len(_ql_business_files)} business and {len(_ql_syn_files)} synthetic artefacts for "
      f"{len(_dc_targets)} targets -- the artefacts are per target because their table names "
      f"are qualified, and a gate reading one fixed filename would examine nothing")
_ql_syn_text = "".join(f.read_text(encoding="utf-8") for f in _ql_syn_files)

# ANTI-VACUITY FOR EVERY BOUNDARY CHECK BELOW. All of them are `X not in text` or a
# comprehension over `text`, and every one of those is TRUE over the empty string the
# `if .is_file() else ""` fallbacks produce. Deleting either artefact made four checks --
# including the one whose message reads "business users would see fabricated numbers" --
# report green having examined nothing. The idiom is the one tests/test_accelerator.py
# already uses for the created-tables sweep (`_ts_created and not _ts_swept`): the
# non-emptiness is asserted here BY NAME, and is also folded into each condition below so
# no single check can pass over an absent file.
check("both committed dashboard artefacts exist and are non-empty -- every boundary check "
      "below is a `not in` test that passes over an empty string",
      bool(_ql_text.strip()) and bool(_ql_syn_text.strip()),
      f"business={_ql_business.name} {len(_ql_text)} byte(s), "
      f"synthetic={_ql_syn.name} {len(_ql_syn_text)} byte(s) -- an absent or empty "
      f"artefact would let the vault-schema, data_quality, tst_ and unprefixed-table "
      f"checks all report green having read nothing")

check("the committed business dashboard reads no vault schema",
      bool(_ql_text.strip())
      and "raw_vault" not in _ql_text and "business_vault" not in _ql_text,
      "the committed artefact references a vault schema, or is absent/empty so this could "
      "not be judged. It publishes with embedded credentials, so its viewers hold no UC "
      "grant and this would show them vault rows")

check("the committed business dashboard reads no data_quality field",
      bool(_ql_text.strip()) and "data_quality" not in _ql_text,
      "the committed artefact reads the event log's data_quality, or is absent/empty so "
      "this could not be judged. DEF-18 means this pipeline declares no SDP expectations, "
      "so that field is 0 for ever and the tile would be confidently wrong")

check("the committed business dashboard names no tst_ table",
      bool(_ql_text.strip()) and "tst_" not in _ql_text,
      "the business artefact reads a synthetic table, or is absent/empty so this could not "
      "be judged -- the two renderings have been crossed, and business users would see "
      "fabricated numbers")

# SPEC 3, IN THOSE WORDS, AT THE COMMITTED ARTEFACT. RATE_GUARD only yields NULL, and a
# NULL formatted as a percentage reads as "no problem". The literal `not evaluated` and the
# column carrying it are the actual defence, and 8 calls this the assertion to get right.
# The literal is spelled out here rather than imported from quality.py: a constant shared
# with the SQL would rename both sides at once and the check could never fail.
_QL_NOT_EVALUATED = "not evaluated"
_ql_state_re = re.compile(rf"'{_QL_NOT_EVALUATED}'.*?END\s+AS\s+(\w+)", re.DOTALL)
_ql_state_cols = sorted({c for d in _ql.datasets()
                         for c in _ql_state_re.findall(" ".join(d["queryLines"]))})
_ql_state_absent = [c for c in _ql_state_cols if f'"{c}"' not in _ql_text]
check("the committed business dashboard says 'not evaluated' in those words, and names "
      "every column that carries them",
      bool(_ql_text.strip()) and _QL_NOT_EVALUATED in _ql_text
      and _ql_state_cols and not _ql_state_absent,
      f"the literal {_QL_NOT_EVALUATED!r} is "
      f"{'present' if _QL_NOT_EVALUATED in _ql_text else 'ABSENT'} and these state "
      f"columns are missing from the artefact: {_ql_state_absent} (definition declares "
      f"{_ql_state_cols}). Spec 3 requires the zero-denominator state to be said in those "
      f"words; without them a business user reads a blank rate as a clean one")

# ANTI-VACUITY for the two CONTROL_TABLES loops below: an emptied tuple would make both
# `for t in _ql.CONTROL_TABLES` loops iterate zero times and report green having examined
# nothing, the same hollow-gate shape the ARTEFACTS guard above exists to refuse.
# SIX, NOT SEVEN. control_objects.sql creates ctl_source_conformance too, and this list
# deliberately excludes it: it is the Bronze conformance record, and the silver dashboard
# reading it would report a different subject under the same title. The bronze containment
# check above asserts the converse -- that the bronze family reads ONLY that table.
check("quality.CONTROL_TABLES is non-empty and names the six control tables silver may read",
      set(_ql.CONTROL_TABLES) == {"ctl_approval_manifest", "ref_dq_expectation",
                                  "aud_table_load", "aud_table_discard",
                                  "aud_load_run", "ctl_quarantine_superseded"},
      f"got {sorted(_ql.CONTROL_TABLES)} -- an empty or short tuple lets the two checks "
      f"below pass having examined nothing")

# STRIP-THEN-CHECK, the same idiom tests/test_accelerator.py uses for this property: strip
# every correctly-prefixed occurrence of t first, then a bare t left in the residue is
# unprefixed. Fewer assumptions than matching the characters that can precede a bare name.
_ql_bare = [t for t in _ql.CONTROL_TABLES if t in _ql_syn_text.replace(f"tst_{t}", "")]
check("the committed synthetic dashboard names no UNPREFIXED control table",
      bool(_ql_syn_text.strip()) and not _ql_bare,
      f"{_ql_bare} appear without the tst_ prefix, so exercising the synthetic dashboard "
      f"would read and report the REAL audit")

# Every table an artefact queries must be a table the DDL creates. Otherwise a dataset can
# name something that never existed and the dashboard fails at view time, in front of a user.
# GUARDED, for the reason the two try/excepts above are guarded: this is a module-level
# read with no fallback, and control_objects.sql is NOT in REQUIRED_FILES, so nothing else
# in this file catches its absence. An OSError escaping here aborts verify_repo.py and
# turns every later check ABSENT -- including the whole DEF-14 sweep below -- which is
# strictly worse than one red check carrying the message.
_ql_ddl_path = ROOT / "governance" / "control_objects.sql"
try:
    _ql_ddl = _ql_ddl_path.read_text(encoding="utf-8")
    _ql_ddl_why = ""
except OSError as _exc:
    _ql_ddl = ""
    _ql_ddl_why = f"{type(_exc).__name__}: {_exc}"
_ql_undeclared = [t for t in _ql.CONTROL_TABLES if t not in _ql_ddl]
check("every control table the definition names is created by control_objects.sql",
      not _ql_ddl_why and not _ql_undeclared,
      f"{_ql_ddl_why or ''}{'; ' if _ql_ddl_why else ''}"
      f"{_ql_undeclared} are queried but never created -- the tile would fail at view "
      f"time, in front of a user. governance/control_objects.sql is not in REQUIRED_FILES, "
      f"so this check is the only thing that notices it is gone")

# SPEC SECTION 8, ROW 4: "no dashboard dataset subtracts one layer's count from another's".
# Section 5 forbids, by name, a tile reading bronze's `accepted` minus silver's `accepted`
# labelled "rows lost" -- a drop between layers is expected and correct (hubs deduplicate;
# satellites store only changed rows), so that arithmetic reports a fabricated catastrophe.
# Section 8 calls this row "the only gate standing between this design and the tile section
# 5 forbids, and that tile is the one a stakeholder will ask for by name" -- swept over every
# declared target's qualified rendering, not just the bare/default one, since a per-layer
# qualifier is what makes two references "the same table, different layers" in the first
# place.
_ql_cross_layer: list[str] = []
for _ql_t3, _ql_vars3 in _dc_targets:
    _ql_cross_layer += _ql.cross_layer_subtraction_findings(_ql.datasets(
        catalog=_ql_vars3.get("catalog", ""),
        control_schema=_ql_vars3.get("control_schema", "control")))
check("no quality dataset subtracts one layer's count from another's",
      not _ql_cross_layer,
      f"{_ql_cross_layer} -- spec section 5 forbids exactly this tile by name: a drop "
      f"between layers is expected (hubs deduplicate 4,444,172 GP rows into 2,221,108 hub "
      f"rows; satellites store only changed rows), so subtracting one layer's count from "
      f"another's reports a fabricated catastrophe, not a real one")


# --------------------------------------------------------------------------- #
# THE BRONZE CONFORMANCE FAMILY. Same gates, same reasons, over the second family. Written
# out rather than folded into a loop with the silver block: the two families read different
# tables and obey different containment rules, and a shared loop would have to be
# parameterised by exactly the things that make them different.
print("\n[bronze-quality] the conformance dashboard's artefacts and boundaries")

from accelerator import bronze_quality as _bq  # noqa: E402
from emit_source_contract import configured_targets as _bq_targets_fn  # noqa: E402

_bq_targets = list(_bq_targets_fn())

# ANTI-VACUITY, twice: an empty ARTEFACTS mapping OR an empty target list would make the
# staleness loop below compare nothing and still report green.
check("emit_quality_dashboard.BRONZE_ARTEFACTS is non-empty and names both renderings",
      set(_eqd.BRONZE_ARTEFACTS) == {"quality_bronze", "quality_bronze_synthetic"},
      f"got {sorted(_eqd.BRONZE_ARTEFACTS)} -- an empty or short mapping lets the staleness "
      f"checks below pass having compared nothing")
check("there is at least one target with a source contract to build a bronze dashboard for",
      bool(_bq_targets),
      "configured_targets() returned nothing, so the staleness sweep below would compare "
      "zero artefacts and report green -- the hollow-gate shape, again")

_bq_stale = []
for _bq_t, _bq_vars in _bq_targets:
    _bq_cat = _bq_vars.get("catalog", "")
    _bq_cs = _bq_vars.get("control_schema", "control")
    for _bq_stem, _bq_prefix in _eqd.BRONZE_ARTEFACTS.items():
        _bq_fn = f"{_bq_stem}_{_bq_t}.lvdash.json"
        _bq_p = _ql_dir / _bq_fn
        try:
            _bq_want = _eqd.render(_eqd.bronze_dashboard(_bq_prefix, _bq_cat, _bq_cs))
        except Exception as _exc:  # noqa: BLE001 -- caught so one failure cannot abort the file
            _bq_stale.append(f"{_bq_fn}: {type(_exc).__name__}: {_exc}")
            continue
        if not _bq_p.is_file() or _bq_p.read_text(encoding="utf-8") != _bq_want:
            _bq_stale.append(_bq_fn)
check("every committed bronze conformance artefact matches what the definition generates",
      not _bq_stale,
      f"stale: {_bq_stale} -- run tools/emit_quality_dashboard.py and review the diff")

_bq_files = [f for f in sorted(_ql_dir.glob("quality_bronze_*.lvdash.json"))
             if "_synthetic_" not in f.name]
_bq_syn_files = sorted(_ql_dir.glob("quality_bronze_synthetic_*.lvdash.json"))
_bq_text = "".join(f.read_text(encoding="utf-8") for f in _bq_files)
_bq_syn_text = "".join(f.read_text(encoding="utf-8") for f in _bq_syn_files)

check("a business and a synthetic bronze artefact exist for every contracted target",
      bool(_bq_files) and len(_bq_files) == len(_bq_syn_files) == len(_bq_targets),
      f"{len(_bq_files)} business and {len(_bq_syn_files)} synthetic artefacts for "
      f"{len(_bq_targets)} contracted target(s) -- every boundary check below is a `not in` "
      f"test that passes over the empty string, so this non-emptiness is the guard")

check("the committed bronze dashboard reads no vault schema",
      bool(_bq_text.strip())
      and "raw_vault" not in _bq_text and "business_vault" not in _bq_text,
      "the committed artefact references a vault schema, or is absent/empty so this could "
      "not be judged. It publishes with embedded credentials, so its viewers hold no UC "
      "grant and this would show them vault rows")

check("the committed bronze dashboard names no tst_ table",
      bool(_bq_text.strip()) and "tst_" not in _bq_text,
      "the business artefact reads a synthetic table, or is absent/empty so this could not "
      "be judged -- the two renderings have been crossed, and readers would see fabricated "
      "conformance")

_bq_bare = (_bq.CONFORMANCE_TABLE
            in _bq_syn_text.replace(f"tst_{_bq.CONFORMANCE_TABLE}", ""))
check("the committed synthetic bronze dashboard names no UNPREFIXED conformance table",
      bool(_bq_syn_text.strip()) and not _bq_bare,
      f"{_bq.CONFORMANCE_TABLE} appears without the tst_ prefix, so exercising the synthetic "
      f"dashboard would read and report the REAL conformance record")

# CONTAINMENT. The bronze dashboard reads the conformance record and nothing else. A dataset
# reaching into a vault or audit table would be a second, unasserted derivation of quality --
# which is the whole reason this dashboard reads a recorded verdict rather than recomputing.
_bq_other = sorted({t for t in _ql.CONTROL_TABLES if t in _bq_text})
check("the bronze dashboard reads ONLY ctl_source_conformance",
      bool(_bq_text.strip()) and _bq.CONFORMANCE_TABLE in _bq_text and not _bq_other,
      f"also reads {_bq_other} -- this dashboard reports what the conformance gate concluded, "
      f"and a dataset reaching into another control table would be reporting something else "
      f"under the same title")

# THE TABLE IT READS MUST BE ONE THE DDL CREATES, or every tile fails at view time in front
# of a reader -- the same failure the silver family measured on 27 Aug.
check("the conformance table the bronze dashboard names is created by control_objects.sql",
      not _ql_ddl_why and _bq.CONFORMANCE_TABLE in _ql_ddl,
      f"{_ql_ddl_why or ''}{'; ' if _ql_ddl_why else ''}{_bq.CONFORMANCE_TABLE} is queried "
      f"but never created")

# ONE GUARD, NOT TWO LOOKALIKES. Both families render a rate, and a second wording of the
# zero-denominator guard would be a second thing to keep right.
check("bronze_quality.RATE_GUARD is the SAME string as quality.RATE_GUARD",
      _bq.RATE_GUARD == _ql.RATE_GUARD,
      f"bronze={_bq.RATE_GUARD!r} silver={_ql.RATE_GUARD!r} -- two wordings of one rule is "
      f"two places for it to rot")

_bq_unguarded: list[str] = []
for _bq_t2, _bq_vars2 in _bq_targets:
    _bq_unguarded += _bq.unguarded_rate_findings(_bq.datasets(
        catalog=_bq_vars2.get("catalog", ""),
        control_schema=_bq_vars2.get("control_schema", "control")))
check("no bronze dataset computes a rate without the zero-denominator guard",
      not _bq_unguarded,
      f"{sorted(set(_bq_unguarded))} divide without RATE_GUARD -- a rate over a zero "
      f"denominator renders NULL, and a NULL formatted as a percentage reads as 'no problem'")

# A COUNTER OVER A MULTI-ROW DATASET renders whichever row arrives first under a title that
# implies an aggregate: confidently wrong, which is the failure class this dashboard exists
# to avoid. Asserted against the definition, and the declaration is checked for honesty too --
# a dataset named single-row that carries a GROUP BY is not single-row.
_bq_counter_bad = [t["name"] for t in _bq.tiles()
                   if t["widgetType"] == "counter"
                   and t["dataset"] not in _bq.SINGLE_ROW_DATASETS]
check("every bronze counter tile reads a dataset declared single-row",
      bool(_bq.tiles()) and not _bq_counter_bad,
      f"{_bq_counter_bad} -- a counter over a multi-row dataset shows one arbitrary row "
      f"under a title claiming an aggregate")
_bq_not_single = [d["name"] for d in _bq.datasets()
                  if d["name"] in _bq.SINGLE_ROW_DATASETS
                  and "GROUP BY" in " ".join(d["queryLines"])]
check("every dataset DECLARED single-row really is one (no GROUP BY)",
      bool(_bq.SINGLE_ROW_DATASETS) and not _bq_not_single,
      f"{_bq_not_single} are named in SINGLE_ROW_DATASETS but group, so the declaration that "
      f"protects the counters is itself wrong -- the guard would pass while the tile lies")

# DECLARED EXACTLY WHERE ITS ARTEFACT EXISTS, both directions. The resource interpolates
# ${bundle.target} into file_path, so a target that declares it WITHOUT an artefact fails its
# deploy on a missing file -- and a target with an artefact but no declaration silently
# deploys nothing, which looks identical to a dashboard that is simply empty. Neither
# direction can be caught by `bundle validate` here: seven of the nine targets are EU
# production and authenticating to them to check a YAML key is not something this repo does.
_bq_yml = (ROOT / "databricks.yml").read_text(encoding="utf-8")
_bq_declared = set()
_bq_cur = None
for _bq_ln in _bq_yml.split("\n"):
    _bq_m = re.match(r"^  (\w+):\s*$", _bq_ln)
    if _bq_m:
        _bq_cur = _bq_m.group(1)
    elif _bq_cur and _bq_ln.strip() == "quality_bronze:":
        _bq_declared.add(_bq_cur)
_bq_with_artefact = {t for t, _v in _bq_targets}
check("the bronze dashboard is declared for exactly the targets whose artefact exists",
      _bq_declared == _bq_with_artefact,
      f"declared for {sorted(_bq_declared)}, artefacts exist for {sorted(_bq_with_artefact)} "
      f"-- a declaration without an artefact fails that target's deploy on a missing file, "
      f"and an artefact without a declaration deploys nothing while looking like an empty "
      f"dashboard")

# THE PATH IS ROOT-RELATIVE, and that is not cosmetic either. A relative path in
# resources/*.yml resolves from that file's directory; one in databricks.yml resolves from
# the bundle root. The `../dashboards/...` that is correct in quality_dashboard.yml escapes
# the sync root from here, and validate refuses it outright -- measured 29 Aug.
check("the bronze dashboard's file_path is root-relative, not resources-relative",
      "./dashboards/quality_bronze_${bundle.target}.lvdash.json" in _bq_yml
      and "../dashboards/quality_bronze_" not in _bq_yml,
      "file_path uses ../dashboards from databricks.yml, which resolves outside the sync "
      "root: `path ... is not contained in sync root path`")

# THE DASHBOARD IS ONLY AS REAL AS THE TASK THAT FEEDS IT. Nothing else notices if the
# recording arguments are dropped from the job: the gate still runs, still asserts, still
# reports its verdict, and exits 0 -- while ctl_source_conformance quietly stops growing and
# every tile keeps rendering the last run's numbers as though they were current.
#
# PARSED, NOT GREPPED. The first version of this check searched the task's text for
# "create_control_objects" and passed while the dependency was REMOVED -- because the comment
# explaining the dependency contains the word. A check fooled by its own prose is the exact
# shape this repo keeps finding, and it had already happened once today in the factory import
# gate. Structure, not spelling.
_bq_job_doc = yaml.safe_load((ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8"))
_bq_tasks = (((_bq_job_doc or {}).get("resources") or {}).get("jobs") or {})
_bq_tasks = ((_bq_tasks.get("vault_load") or {}).get("tasks") or [])
_bq_scc_task = next((t for t in _bq_tasks
                     if t.get("task_key") == "assert_source_conformance"), None)
check("the vault job still declares an assert_source_conformance task",
      _bq_scc_task is not None,
      f"task keys: {sorted(t.get('task_key', '?') for t in _bq_tasks)} -- the two checks "
      f"below read this task, and both pass vacuously if it is not found")

_bq_params = list((_bq_scc_task or {}).get("spark_python_task", {}).get("parameters") or [])
_bq_missing_args = [a for a in ("--record-catalog", "--record-schema", "--job-run-id")
                    if a not in _bq_params]
check("the conformance job task is asked to RECORD, not merely to assert",
      _bq_scc_task is not None and not _bq_missing_args,
      f"parameters={_bq_params} missing {_bq_missing_args} -- the gate would keep passing "
      f"while the table it feeds stopped growing, and the dashboard would render stale "
      f"numbers as current")

# ORDERING, NOT PREFERENCE. The task writes into the control schema that
# create_control_objects creates. Without the edge, a fresh lake's first run writes into
# nothing -- and because a failed write is fatal, it fails the task rather than losing the
# row quietly. Either way the edge is what makes the first run work.
_bq_deps = {d.get("task_key") for d in ((_bq_scc_task or {}).get("depends_on") or [])}
check("the conformance task depends on create_control_objects, whose schema it writes into",
      _bq_scc_task is not None and "create_control_objects" in _bq_deps,
      f"depends_on={sorted(_bq_deps)} -- it records into control.ctl_source_conformance but "
      f"does not depend on the task that creates that schema")

# THE HONESTY TILE IS NOT OPTIONAL. Without a runs-recorded counter an empty conformance
# record renders every other tile as a clean zero, indistinguishable from perfect health.
# This dashboard is being deployed while the gate happens to PASS, so this is the check that
# stops it becoming a green light over an empty table.
check("the bronze dashboard carries a runs-recorded tile",
      any(t["dataset"] == "ds_runs_recorded" and t["widgetType"] == "counter"
          for t in _bq.tiles())
      and bool(_bq_text.strip()) and "runs_recorded" in _bq_text,
      "no runs-recorded counter reaches the committed artefact -- an empty "
      "ctl_source_conformance would then render as an estate in perfect health")


# --------------------------------------------------------------------------- #
# THE GOLD FAMILY -- EMITTED, GATED, AND DEPLOYED BY NOTHING.
print("\n[gold-quality] the gold dashboard's artefacts, and that nothing deploys them")

from accelerator import gold_quality as _gq  # noqa: E402
# HOISTED HERE from further down the file, which used to hold the only import of it.
# The gold checks below are the first use, and a module imported twice under one alias
# is one more thing to keep in step for no gain.
from accelerator import control_standard as _cs  # noqa: E402

check("emit_quality_dashboard.GOLD_ARTEFACTS is non-empty and names both renderings",
      set(_eqd.GOLD_ARTEFACTS) == {"quality_gold", "quality_gold_synthetic"},
      f"got {sorted(_eqd.GOLD_ARTEFACTS)} -- an empty or short mapping lets the staleness "
      f"check below pass having compared nothing")

_gq_stale = []
for _gq_t, _gq_vars in _dc_targets:
    _gq_cat = _gq_vars.get("gold_catalog", "")
    _gq_cs = _gq_vars.get("control_schema", "control")
    for _gq_stem, _gq_prefix in _eqd.GOLD_ARTEFACTS.items():
        _gq_fn = f"{_gq_stem}_{_gq_t}.lvdash.json"
        _gq_p = _ql_dir / _gq_fn
        try:
            _gq_want = _eqd.render(_eqd.gold_dashboard(_gq_prefix, _gq_cat, _gq_cs))
        except Exception as _exc:  # noqa: BLE001 -- caught so one failure cannot abort the file
            _gq_stale.append(f"{_gq_fn}: {type(_exc).__name__}: {_exc}")
            continue
        if not _gq_p.is_file() or _gq_p.read_text(encoding="utf-8") != _gq_want:
            _gq_stale.append(_gq_fn)
check("every committed gold artefact matches what the definition generates",
      not _gq_stale,
      f"stale: {_gq_stale} -- run tools/emit_quality_dashboard.py and review the diff")

_gq_files = [f for f in sorted(_ql_dir.glob("quality_gold_*.lvdash.json"))
             if "_synthetic_" not in f.name]
_gq_syn_files = sorted(_ql_dir.glob("quality_gold_synthetic_*.lvdash.json"))
_gq_text = "".join(f.read_text(encoding="utf-8") for f in _gq_files)
_gq_syn_text = "".join(f.read_text(encoding="utf-8") for f in _gq_syn_files)
check("a business and a synthetic gold artefact exist for every declared target",
      bool(_gq_files) and len(_gq_files) == len(_gq_syn_files) == len(_dc_targets),
      f"{len(_gq_files)} business and {len(_gq_syn_files)} synthetic artefacts for "
      f"{len(_dc_targets)} target(s) -- every check below is a `not in` test that passes over "
      f"the empty string, so this non-emptiness is the guard")

# THE GATE THIS FAMILY EXISTS TO CARRY. Gold has no catalog: 03_usnc_gold_edm_dev does not
# exist, nothing produces gold, and control_objects_gold.sql is generated and never applied.
# A bundle resource declaring one of these artefacts would either fail that target's deploy on
# a missing catalog, or publish a dashboard whose every tile fails with
# TABLE_OR_VIEW_NOT_FOUND -- which is not hypothetical, it is what the silver dashboard did on
# 27 August. This stays red until somebody deliberately stands gold up AND removes this check
# in the same reviewed commit.
_gq_decl_files = [ROOT / "databricks.yml"] + sorted((ROOT / "resources").glob("*.yml"))
_gq_declared_in = []
for _gq_f in _gq_decl_files:
    _gq_doc = _gq_f.read_text(encoding="utf-8")
    # Structural, not textual: a comment mentioning quality_gold must not trip this, and a
    # commented-out resource block is not a declaration. Parsed, then walked for the key.
    try:
        _gq_parsed = yaml.safe_load(_gq_doc)
    except Exception:  # noqa: BLE001 -- a malformed file is another check's problem
        continue

    def _gq_walk(node, path=""):
        found = []
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(k, str) and k.startswith("quality_gold"):
                    found.append(f"{path}/{k}")
                found += _gq_walk(v, f"{path}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                found += _gq_walk(v, f"{path}[{i}]")
        return found

    for _hit in _gq_walk(_gq_parsed):
        _gq_declared_in.append(f"{_gq_f.name}:{_hit}")
check("NO bundle resource declares a gold dashboard, because gold does not exist",
      not _gq_declared_in,
      f"declared at {_gq_declared_in} -- 03_usnc_gold_edm_dev does not exist and nothing "
      f"produces gold, so deploying this either fails on the missing catalog or publishes a "
      f"page where every tile errors. Stand gold up first, then remove this check in the same "
      f"commit that declares the resource")

# THE CATALOG MUST BE GOLD'S. This family is the only one qualified with ${var.gold_catalog},
# and using the silver one would render the VAULT's audit under a page titled Gold: real
# numbers, no error, wrong layer. Nothing else in this file would notice.
_gq_wrong_catalog = sorted({v.get("catalog", "") for _t, v in _dc_targets
                            if v.get("catalog") and v.get("catalog") in _gq_text})
check("the gold artefacts name the GOLD catalog, and no silver one",
      bool(_gq_text.strip()) and not _gq_wrong_catalog
      and all(v.get("gold_catalog", "") in _gq_text for _t, v in _dc_targets),
      f"silver catalog(s) {_gq_wrong_catalog} appear in the gold artefacts -- this would show "
      f"the vault's audit under a page titled Gold, with numbers that are real and describe "
      f"the wrong layer")

check("the committed gold dashboard reads no vault schema",
      bool(_gq_text.strip())
      and "raw_vault" not in _gq_text and "business_vault" not in _gq_text,
      "a gold dataset references a vault schema -- gold reads the vault's AUDIT, never its "
      "tables, and a dashboard publishing vault rows is the boundary these families keep")

check("the committed gold dashboard names no tst_ table",
      bool(_gq_text.strip()) and "tst_" not in _gq_text,
      "the business artefact reads a synthetic table -- the two renderings have been crossed")

_gq_bare = [t for t in _gq.CONTROL_TABLES if t in _gq_syn_text.replace(f"tst_{t}", "")]
check("the committed synthetic gold dashboard names no UNPREFIXED control table",
      bool(_gq_syn_text.strip()) and not _gq_bare,
      f"{_gq_bare} appear without the tst_ prefix, so exercising the synthetic dashboard "
      f"would read the REAL audit")

# CONTAINMENT, DERIVED FROM THE STANDARD. Gold declares no tables of its own, so its whole
# surface is the mandatory core -- and a gold dataset reaching for ref_dq_expectation or
# ctl_quarantine_superseded would be querying something gold does not have.
check("gold_quality.CONTROL_TABLES IS control_standard's gold set, not a restatement",
      set(_gq.CONTROL_TABLES) == set(_cs.tables_for("gold")),
      f"gold_quality says {sorted(_gq.CONTROL_TABLES)}, the standard says "
      f"{sorted(_cs.tables_for('gold'))} -- a fourth copy of the core's names is "
      f"a fourth thing to keep in step")
_gq_outside = sorted({t for t in _ql.CONTROL_TABLES if t not in _gq.CONTROL_TABLES
                      and t in _gq_text})
check("the gold dashboard queries only tables gold actually has",
      bool(_gq_text.strip()) and not _gq_outside,
      f"also reads {_gq_outside} -- gold declares no tables of its own, so these exist only "
      f"in silver and the tile would fail at view time")

check("gold_quality.RATE_GUARD is the SAME string as the other two families",
      _gq.RATE_GUARD == _ql.RATE_GUARD == _bq.RATE_GUARD,
      f"gold={_gq.RATE_GUARD!r} -- three wordings of one rule is three places for it to rot")

_gq_unguarded: list[str] = []
_gq_cross: list[str] = []
for _gq_t2, _gq_v2 in _dc_targets:
    _gq_ds = _gq.datasets(gold_catalog=_gq_v2.get("gold_catalog", ""),
                          control_schema=_gq_v2.get("control_schema", "control"))
    _gq_unguarded += _gq.unguarded_rate_findings(_gq_ds)
    _gq_cross += _gq.cross_layer_subtraction_findings(_gq_ds)
check("no gold dataset computes a rate without the zero-denominator guard",
      not _gq_unguarded,
      f"{sorted(set(_gq_unguarded))} divide without RATE_GUARD -- on a layer with no producer "
      f"every denominator is zero, so this is not a corner case here, it is every row")
check("no gold dataset subtracts one layer's count from another's",
      not _gq_cross,
      f"{sorted(set(_gq_cross))} -- gold reads FROM the vault, so 'vault rows minus gold rows, "
      f"labelled rows lost' is the obvious next tile and it is wrong: a projection selects and "
      f"aggregates, so the drop is the projection working")

_gq_counter_bad = [t["name"] for t in _gq.tiles()
                   if t["widgetType"] == "counter"
                   and t["dataset"] not in _gq.SINGLE_ROW_DATASETS]
check("every gold counter tile reads a dataset declared single-row",
      bool(_gq.tiles()) and not _gq_counter_bad,
      f"{_gq_counter_bad} -- a counter over a multi-row dataset shows one arbitrary row under "
      f"a title claiming an aggregate")
_gq_not_single = [d["name"] for d in _gq.datasets()
                  if d["name"] in _gq.SINGLE_ROW_DATASETS
                  and "GROUP BY" in " ".join(d["queryLines"])]
check("every gold dataset DECLARED single-row really is one (no GROUP BY)",
      bool(_gq.SINGLE_ROW_DATASETS) and not _gq_not_single,
      f"{_gq_not_single} group but are declared single-row, so the declaration protecting the "
      f"counter is itself wrong")

# THE HONESTY TILE, and on this family it describes the present rather than guarding a future.
check("the gold dashboard leads with a runs-recorded tile",
      any(t["dataset"] == "ds_runs_recorded" and t["widgetType"] == "counter"
          for t in _gq.tiles())
      and bool(_gq_text.strip()) and "runs_recorded" in _gq_text,
      "no runs-recorded counter reaches the committed artefact -- gold holds zero rows and "
      "will until something produces it, and four empty tables read as a layer that ran "
      "cleanly rather than one that has never run")

# NO COVERAGE TILE, ON PURPOSE. Silver leads with coverage because ref_dq_expectation exists
# there to be empty. Gold declares no expectations at all, so a coverage tile would count
# nothing and render 0% -- describing a rule set nobody has written as one nobody follows.
check("the gold dashboard declares no expectation-coverage tile",
      not any("coverage" in t["name"] or "coverage" in t["title"].lower()
              for t in _gq.tiles())
      and "ref_dq_expectation" not in _gq_text,
      "a coverage tile reached the gold dashboard -- gold declares no expectations, so it "
      "would read 0% and describe an unwritten rule set as an unfollowed one")


# --------------------------------------------------------------------------- #
# THE DBML DIAGRAM. Generated from metadata/entities/, gated like every other generated
# artefact so the diff is the review.
print("\n[diagram] the vault's DBML diagram matches the model")

import emit_dbml_diagram as _dbml  # noqa: E402

_dbml_model = spec.load_model(ROOT / "metadata" / "entities")
_dbml_entries = _dbml.tables(_dbml_model)

check("the diagram covers every table the model declares",
      len(_dbml_entries) == sum(1 for e in _dbml_model.entities for _s in e.tables()),
      f"{len(_dbml_entries)} table(s) in the diagram against "
      f"{sum(1 for e in _dbml_model.entities for _s in e.tables())} declared -- a diagram "
      f"missing a table is worse than none, because it looks complete")

try:
    _dbml_want = _dbml.render(_dbml_entries)
    _dbml_err = ""
except Exception as _exc:  # noqa: BLE001 -- caught so one failure cannot abort the file
    _dbml_want, _dbml_err = "", f"{type(_exc).__name__}: {_exc}"
check("regenerating the DBML diagram raises no error",
      not _dbml_err,
      f"{_dbml_err} -- the emitter itself is broken, not merely stale; re-running it would "
      f"only reproduce this")
check("the committed DBML diagram matches what the model generates",
      not _dbml_err and _dbml.OUT_PATH.is_file()
      and _dbml.OUT_PATH.read_text(encoding="utf-8") == _dbml_want,
      "run tools/emit_dbml_diagram.py and review the diff")

# THE EDGES ARE THE POINT. Databricks' own industry data models ship a DBML whose foreign
# keys live only in prose inside note: attributes, so dbdiagram draws no lines between tables
# (inspected 3 Sep). A Data Vault IS its edges -- every satellite and link hangs off a hub --
# so a diagram of one without Refs would be a picture of nothing.
_dbml_text = _dbml.OUT_PATH.read_text(encoding="utf-8") if _dbml.OUT_PATH.is_file() else ""
_dbml_refs = [ln for ln in _dbml_text.splitlines() if ln.startswith("Ref: ")]
_dbml_declared_fks = sum(len(e["refs"]) for e in _dbml_entries)
check("every declared foreign key is emitted as a real DBML Ref",
      bool(_dbml_refs) and len(_dbml_refs) == _dbml_declared_fks,
      f"{len(_dbml_refs)} Ref line(s) for {_dbml_declared_fks} declared foreign key(s) -- "
      f"a DBML with FKs only in note: text renders as unconnected boxes, which is the defect "
      f"in the reference implementation this was modelled on")

# ...AND EACH ONE MUST POINT AT A TABLE THE DIAGRAM ACTUALLY DEFINES. A Ref to a name no
# Table block declares is a dangling arrow: dbdiagram invents the table, and the picture
# shows a relationship to something that does not exist.
# STRENGTHENED 3 Sep: this validated the TABLE only, and a mutation that corrupted the
# COLUMN passed it. A Ref to a real table and a column it does not have is the same dangling
# arrow -- dbdiagram invents the column instead of the table. Both halves are checked now.
_dbml_defined = {f"{e['schema']}.{e['table']}": {c for c, *_rest in e["columns"]}
                 for e in _dbml_entries}
_dbml_dangling = []
for _ln in _dbml_refs:
    for _side in (_ln[len("Ref: "):].split(">", 1)[0], _ln.split(">", 1)[1]):
        _ref = _side.strip()
        _tbl, _col = _ref.rsplit(".", 1)
        if _tbl not in _dbml_defined:
            _dbml_dangling.append(f"{_ref} (no such table)")
        elif _col not in _dbml_defined[_tbl]:
            _dbml_dangling.append(f"{_ref} (table has no column {_col})")
check("every Ref names a table AND a column the diagram defines, on both sides",
      not _dbml_dangling,
      f"dangling: {_dbml_dangling[:4]} -- dbdiagram invents whatever a Ref names, so either "
      f"half being wrong draws a relationship to something that does not exist")

# THE TARGET SCHEMA IS DERIVED, NOT ASSUMED. Only `csat` is a business-vault kind and a csat
# is never a parent, so every FK target is in raw_vault TODAY. Hardcoding that would be
# correct now and wrong silently later.
#
# STABLE, matching _dbml.tables(): a Ref target is contract.foreign_keys()'s unversioned
# base_table, so this lookup must be keyed the same way or every lookup misses.
_dbml_schema_of = {t: naming.vault_schema_for(e.kind, _dbml.RAW_SCHEMA, _dbml.BUSINESS_SCHEMA)
                   for e in _dbml_model.entities for _s, t in e.stable_tables()}
_dbml_wrong_schema = [(c, sch, tgt) for e in _dbml_entries for c, sch, tgt in e["refs"]
                      if _dbml_schema_of.get(tgt.split(".")[0]) != sch]
check("each Ref names the schema its target entity actually declares",
      not _dbml_wrong_schema,
      f"{_dbml_wrong_schema[:3]} -- the day a business-vault kind becomes a parent, a "
      f"hardcoded raw_vault would point the arrow at a schema the table is not in")

# A MASKED COLUMN MUST SAY SO. The diagram is the most likely artefact to be shared outside
# the team, and a masked column that looks ordinary in it invites someone to plan a join on
# cleartext they will never see.
_dbml_masked = {(f"{e['schema']}.{e['table']}", c)
                for e in _dbml_entries for c in e["masks"]}
_dbml_unflagged = [f"{t}.{c}" for t, c in sorted(_dbml_masked)
                   if f"MASKED by" not in _dbml_text.split(f"Table {t} ")[1].split("\n}")[0]
                   or c not in _dbml_text]
check("every masked column is flagged as masked in the diagram",
      bool(_dbml_masked) and not _dbml_unflagged,
      f"{_dbml_unflagged[:4]} of {len(_dbml_masked)} masked column(s) carry no MASKED note "
      f"-- this artefact travels, and a masked column looking ordinary invites a join on "
      f"cleartext nobody will see")


# --------------------------------------------------------------------------- #
# THE OWL ONTOLOGY. Generated from the same model, gated the same way.
print("\n[onto] the vault's OWL ontology matches the model")

import emit_ontology as _onto  # noqa: E402

_onto_model = spec.load_model(ROOT / "metadata" / "entities")
_onto_built = _onto.build(_onto_model)

try:
    _onto_want = _onto.render(_onto_built)
    _onto_err = ""
except Exception as _exc:  # noqa: BLE001
    _onto_want, _onto_err = "", f"{type(_exc).__name__}: {_exc}"
check("regenerating the ontology raises no error",
      not _onto_err,
      f"{_onto_err} -- the emitter is broken, not merely stale")
check("the committed ontology matches what the model generates",
      not _onto_err and _onto.OUT_PATH.is_file()
      and _onto.OUT_PATH.read_text(encoding="utf-8") == _onto_want,
      "run tools/emit_ontology.py and review the diff")

_onto_text = _onto.OUT_PATH.read_text(encoding="utf-8") if _onto.OUT_PATH.is_file() else ""

# EVERY KEYED ENTITY IS A CLASS. A hub or link missing from the ontology is a concept the
# model has and the ontology denies, which is worse than an absent ontology because a tool
# will believe it.
_onto_keyed = {e.name for e in _onto_model.entities if e.kind in naming.KEYED_KINDS}
_onto_classes = {c["entity"] for c in _onto_built["classes"]}
check("every hub, link, NHL and HAL is an owl:Class",
      _onto_classes == _onto_keyed,
      f"missing {sorted(_onto_keyed - _onto_classes)}, extra "
      f"{sorted(_onto_classes - _onto_keyed)} -- a concept the model declares and the "
      f"ontology omits is a concept a tool will conclude does not exist")

# EVERY DECLARED PARENT IS AN OBJECT PROPERTY WITH BOTH ENDS. This is the half Databricks'
# own ontology leaves in prose: read 3 Sep, theirs has no rdfs:domain or rdfs:range at all,
# so a relationship is unreachable by any reasoner. A vault's parents are its meaning.
_onto_expected_edges = sum(
    1 for e in _onto_model.entities if e.kind in naming.KEYED_KINDS
    for pnt in e.parents if pnt in {x.name for x in _onto_model.entities})
check("every declared parent becomes an owl:ObjectProperty",
      len(_onto_built["object_properties"]) == _onto_expected_edges,
      f"{len(_onto_built['object_properties'])} object propert(ies) for "
      f"{_onto_expected_edges} declared parent(s)")
_onto_unbounded = [pr["name"] for pr in _onto_built["object_properties"]
                   if not pr["domain"] or not pr["range"]]
check("every object property declares BOTH rdfs:domain and rdfs:range",
      not _onto_unbounded
      and _onto_text.count("rdfs:domain") >= len(_onto_built["object_properties"]),
      f"{_onto_unbounded[:4]} -- a property without both ends cannot be followed, which is "
      f"the defect in the reference ontology this was modelled on")

# N-ARY LINKS ARE REIFIED, NOT COLLAPSED. journal_line has four parents; an owl:ObjectProperty
# is binary. If a link were emitted as a single property, three of its four parents would
# vanish -- silently, and only from the ontology.
_onto_multi = [e.name for e in _onto_model.entities
               if e.kind in naming.KEYED_KINDS and len(e.parents) > 2]
_onto_multi_props = {m: len([p for p in _onto_built["object_properties"] if p["entity"] == m])
                     for m in _onto_multi}
check("an n-ary link keeps one object property PER parent",
      bool(_onto_multi)
      and all(_onto_multi_props[m] == len({e.name: e for e in _onto_model.entities}[m].parents)
              for m in _onto_multi),
      f"{_onto_multi_props} against declared parent counts -- collapsing an n-ary link into "
      f"one binary property drops every parent but one")

# A TYPE IS NEVER INVENTED. 152 of 369 columns take their type from bronze at load time. An
# ontology that guessed xsd:string for those would assert something the model does not know.
_onto_undeclared = [pr for pr in _onto_built["datatype_properties"]
                    if pr["sql_type"] == "source-derived"]
_onto_guessed = [pr["name"] for pr in _onto_undeclared if pr["range"]]
check("a column with no declared type gets NO rdfs:range",
      bool(_onto_undeclared) and not _onto_guessed,
      f"{_onto_guessed[:4]} of {len(_onto_undeclared)} undeclared-type column(s) were given "
      f"a range -- an ontology that guesses is worse than one that is silent")

# MASKS SURVIVE INTO THE ONTOLOGY. This artefact is built for AI-agent grounding; an agent
# planning a query over a masked column needs to know it is masked before it reasons about
# the values.
_onto_masked = [pr["name"] for pr in _onto_built["datatype_properties"] if pr["mask"]]
check("every masked column carries dv:maskFunction in the ontology",
      bool(_onto_masked)
      and _onto_text.count("dv:maskFunction ") >= len(_onto_masked) + 1,
      f"{len(_onto_masked)} masked propert(ies) against "
      f"{_onto_text.count('dv:maskFunction ') - 1} annotation(s) -- an agent grounding on "
      f"this must not reason about values it can never read")

# PROPERTY NAMES ARE PREFIXED BY CONCEPT. Two concepts sharing a column name would otherwise
# produce one property with two rdfs:domain declarations, which a reasoner reads as the
# INTERSECTION of both -- neither concept, and wrong in a way nothing else would flag.
_onto_dupes = {}
for pr in _onto_built["datatype_properties"]:
    _onto_dupes.setdefault(pr["name"], set()).add(pr["domain"])
_onto_shared = {n: sorted(ds) for n, ds in _onto_dupes.items() if len(ds) > 1}
check("no property name is shared by two concepts",
      not _onto_shared,
      f"{list(_onto_shared.items())[:3]} -- one property with two domains is read as their "
      f"intersection, which is neither concept")


# --------------------------------------------------------------------------- #
# C4: THE DEF-14 EXIT PATTERN, SWEPT OVER EVERY FILE IN checks/.
# `raise SystemExit(main())` is not a style variant. On serverless the ipykernel wrapper
# around spark_python_task surfaces SystemExit as an exception and marks the task FAILED
# -- for exit code 0 as readily as for 1 -- so a PASSING script fails its own task and
# blocks every task behind it. This has now been fixed three times in this repo
# (apply_control_objects.py was the third) because it was fixed FILE BY FILE and nothing
# swept the directory. Globbed, so a file added tomorrow is covered without editing this.
print("\n[def-14] every checks/ script exits the serverless-safe way")
_def14_files = sorted((ROOT / "checks").glob("*.py"))
check("checks/ holds the scripts this sweep is meant to cover",
      len(_def14_files) >= 10,
      f"{len(_def14_files)} file(s) globbed -- a sweep over an empty list passes "
      f"vacuously, which is the one thing it must not do")
for _f in _def14_files:
    _src = _f.read_text(encoding="utf-8")
    check(f"{_f.name} never raises SystemExit from its entry point",
          "raise SystemExit(main" not in _src,
          "serverless marks the task FAILED even for exit code 0")
    # .find() against -1, never .index(): a file with no __main__ block must FAIL this
    # one check rather than raise and abort every check after it.
    _at = _src.find('if __name__ == "__main__":')
    _tail = _src[_at:] if _at != -1 else ""
    check(f"{_f.name} uses the DEF-14 block: _rc = main(), then sys.exit only if _rc",
          _at != -1 and "_rc = main()" in _tail and "if _rc:" in _tail
          and "sys.exit(_rc)" in _tail,
          f"__main__ at {_at}; tail was {_tail[-120:]!r}")
    check(f"{_f.name} imports sys, so the DEF-14 block can run",
          "import sys" in _src, "sys.exit(_rc) with no import is a NameError at exit")

# AND THE NEWEST GATE MUST ACTUALLY RUN SOMEWHERE. A gate nobody invokes is the
# unfailable shape this repo keeps meeting: it passes review, it is committed, and it
# never once measures the thing it was written for. checks/ame_parity_check.py is the
# only thing standing between metadata/source_unions.yml's SQL port of the AME rules and
# src/accelerator/invoice_rules.py drifting apart, so where it is invoked is a property
# worth asserting rather than remembering.
#
# SCOPED TO THIS ONE FILE ON PURPOSE. Two gates in checks/ are invoked by nothing today
# (control_conformance_check.py, journal_integrity_check.py) and why is not something
# this change investigated. A sweep with a two-entry allowlist would be a blanket
# exemption for reasons nobody wrote down -- worse than naming the one file this change
# is responsible for. Widen it when someone adjudicates those two.
# COMMENTS ARE STRIPPED FIRST, and that is not fussiness -- it was measured. The first
# version of this check searched the raw file text, and the CI step that invokes the gate
# carries a six-line comment block naming the file. Deleting the actual `run:` line while
# leaving the comment kept this check GREEN: it was asserting that somebody had WRITTEN
# ABOUT the gate, not that anything ran it. A check that a comment can satisfy is the
# unfailable shape it was added to prevent.
_apg_runners = "\n".join(
    _line
    for _p in [ROOT / "databricks.yml"]
    + sorted((ROOT / "resources").glob("*.yml"))
    + sorted((ROOT / ".github" / "workflows").glob("*.yml"))
    if _p.is_file()
    for _line in _p.read_text(encoding="utf-8").splitlines()
    # A `name:` VALUE IS DOCUMENTATION TOO. Stripping only comments was still not
    # enough: the CI step is called "AME rule parity, SQL against Python
    # (checks/ame_parity_check.py)", so deleting the `run:` line left the step's own
    # NAME satisfying this check. Measured -- verify_repo stayed green with nothing
    # running the gate. What counts as evidence here is a `run:` or a `python_file:`,
    # never a label.
    if not _line.lstrip().startswith(("#", "- name:", "name:")))
check("checks/ame_parity_check.py is invoked by a job or a workflow, not merely present",
      "ame_parity_check.py" in _apg_runners,
      "it is named in no resources/*.yml, no workflow and not databricks.yml -- outside "
      "a comment -- so the SQL in metadata/source_unions.yml is measured against "
      "invoice_rules.py nowhere, and the two drift with every gate still green")


# --------------------------------------------------------------------------- #
print("\n[standard] silver's control DDL matches the declaration")


sys.path.insert(0, str(ROOT / "checks"))
import apply_control_objects as _aco2  # noqa: E402

# VERIFIED, NOT GENERATED. control_objects.sql is deployed, gated and working; rewriting it to
# make the declaration authoritative would risk it for no functional gain. Verifying agreement
# gets the same guarantee -- and this is the check that makes the declaration authoritative,
# so if it ever goes vacuous the standard becomes decoration.
_cs_silver_want = _cs.tables_for("silver")
_cs_ddl_text = (ROOT / "governance" / "control_objects.sql").read_text(encoding="utf-8")
_cs_silver_have = _aco2.declared_columns(_cs_ddl_text)

# NO SEPARATE "parsed at all" CHECK. One used to stand here comparing len(have) == len(want),
# but that is logically subsumed by the table-set check immediately below: any length
# mismatch (including a total parse failure -- declared_columns() returning {}) is also a set
# mismatch, and shows up there as every table missing. A standalone check with no failure mode
# of its own is exactly the hollow-pass shape this branch keeps producing, so it was folded in
# rather than kept as decoration.
_cs_missing_tables = sorted(set(_cs_silver_want) - set(_cs_silver_have))
_cs_extra_tables = sorted(set(_cs_silver_have) - set(_cs_silver_want))
check("silver's control DDL declares exactly the tables the standard names for silver",
      not _cs_missing_tables and not _cs_extra_tables,
      f"missing from the DDL: {_cs_missing_tables}; in the DDL but not the standard: "
      f"{_cs_extra_tables}. The standard is the authority, so a difference means one of the "
      f"two is wrong -- decide which, do not silence the check")

# THE COMPARED COUNT IS PART OF THE CONDITION, not just the loop bound. Looping over the
# intersection alone would let a wrong-names parse (right count, wrong tables) report PASS
# over zero comparisons -- check 2 above would already be red in that case, but this check's
# own PASS would still be false, which is the hollow-pass shape under review here too.
_cs_compared = sorted(set(_cs_silver_want) & set(_cs_silver_have))
_cs_col_drift = []
for _t in _cs_compared:
    _want_cols = set(_cs_silver_want[_t])
    _have_cols = set(_cs_silver_have[_t])
    if _want_cols != _have_cols:
        _cs_col_drift.append(
            f"{_t}: standard-only {sorted(_want_cols - _have_cols)}, "
            f"DDL-only {sorted(_have_cols - _want_cols)}")
check("every silver control table has exactly the columns the standard declares",
      len(_cs_compared) == len(_cs_silver_want) and not _cs_col_drift,
      f"compared {len(_cs_compared)} of {len(_cs_silver_want)} expected table(s); drift: "
      f"{_cs_col_drift} -- the column that went missing for a day on 27 Aug (`severity`) is "
      f"exactly this class, and it was found by an INSERT failing rather than by a check")

# NAMES MATCHING IS NOT ENOUGH -- THE TYPE IS STATED TWICE TOO, and until now compared
# nowhere. control_standard.CORE declares `staged: BIGINT`; control_objects.sql declares
# `staged BIGINT NOT NULL` independently; declared_columns() (used above) discards the type
# down to a bare column-name set, so a column renamed to the wrong TYPE in the DDL passed
# the check above silently. Measured: editing control_objects.sql to
# `staged STRING NOT NULL` left the whole suite GREEN before this check existed. These types
# are not decoration -- they are what control_contracts/bronze.yaml publishes to the Bronze
# team and what gold's generated DDL will create, so a silver DDL that drifts from them is a
# silver DDL that disagrees with what this repo tells two other audiences is true.
_CS_TYPE_SYNONYM = {"LONG": "BIGINT", "INTEGER": "INT", "SHORT": "SMALLINT",
                    "BYTE": "TINYINT", "DEC": "DECIMAL", "NUMERIC": "DECIMAL"}
_cs_silver_have_types = _aco2.declared_column_types(_cs_ddl_text)
_cs_type_drift = []
for _ct in _cs_compared:
    _want_types = _cs_silver_want[_ct]
    _have_types = _cs_silver_have_types.get(_ct, {})
    for _col in sorted(set(_want_types) & set(_have_types)):
        # SPARK SYNONYMS ARE THE SAME TYPE. Measured by the re-review: writing `LONG` where the
        # declaration says `BIGINT` -- documented synonyms for one type -- produced a false
        # failure. Dormant today because CORE uses only canonical spellings, but a false red on
        # a correct declaration teaches people to distrust the gate.
        _want_type = _CS_TYPE_SYNONYM.get(_want_types[_col].upper(), _want_types[_col].upper())
        _have_type = _CS_TYPE_SYNONYM.get(_have_types[_col].upper(), _have_types[_col].upper())
        if _want_type != _have_type:
            _cs_type_drift.append(
                f"{_ct}.{_col}: standard says {_want_type}, DDL says {_have_type}")
check("every silver control column has the TYPE the standard declares, not just the name",
      len(_cs_compared) == len(_cs_silver_want) and not _cs_type_drift,
      f"compared {len(_cs_compared)} of {len(_cs_silver_want)} expected table(s); type "
      f"drift: {_cs_type_drift} -- a column present under the right name with the wrong "
      f"type is invisible to a name-only comparison, and these are the types published to "
      f"the Bronze team and generated into gold's DDL")


# THE APPEND-ONLY SPLIT, read off the DDL text -- NARROWED to what static text can support,
# after three fail-open attempts at doing more, each defeated in a different disguise:
#   scan to the next `;`                -- defeated by a semicolon inside a `--` comment
#   anchor on the first TBLPROPERTIES ( -- defeated by a decoy `TBLPROPERTIES (` inside a
#                                          COMMENT string naming the clause
#   OR across every occurrence          -- defeated by a marker leaked from elsewhere in the
#                                          file masking a table whose own clause has none
# That is not three separate mistakes. It is one: attributing a table's DEPLOYED
# configuration to a span of DDL TEXT is not a question text can answer, because the text is
# evidence of intent, not the deployed fact -- and every fix only moved which text fooled the
# scanner.
#
# WHAT THIS CHECK GUARANTEES: the canonical marker string is spelled correctly and appears at
# least once per table the standard requires to be append-only, counted on the DDL with `--`
# and `/* */` comments stripped (so a comment cannot inflate the count). A typo, or an
# append-only table added to the standard whose DDL entry never got the property at all, is
# caught statically, before deploy.
#
# WHAT THIS CHECK DOES NOT GUARANTEE: that any PARTICULAR table carries the marker -- it
# counts occurrences, not attribution. The per-table assertion belongs to
# checks/control_conformance_check.py's append_only_conformance() (Task 5 of this plan, not
# yet built), which reads SHOW TBLPROPERTIES against the live, deployed table -- the only
# place this property actually exists. A check that has failed open three times is worse than
# none, because its green is read as evidence; this one is honest about the weaker claim it
# can actually make.
def _cs_strip_sql_comments(text):
    text = re.sub(r"--[^\n]*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return text


_cs_ddl_stripped = _cs_strip_sql_comments(_cs_ddl_text)
_cs_ao_marker = "delta.appendOnly' = 'true'"
_cs_ao_required = sorted(_t for _t in _cs_silver_want if _t in _cs.APPEND_ONLY)
_cs_ao_marker_count = _cs_ddl_stripped.count(_cs_ao_marker)
check("every table requiring append-only has the marker spelled somewhere in the DDL",
      bool(_cs_ao_required) and _cs_ao_marker_count >= len(_cs_ao_required),
      f"{len(_cs_ao_required)} silver table(s) require append-only {_cs_ao_required}, but "
      f"the marker {_cs_ao_marker!r} appears only {_cs_ao_marker_count} time(s) in "
      f"control_objects.sql (comments stripped) -- this check cannot say WHICH table is "
      f"short, only that the count is; checks/control_conformance_check.py's "
      f"append_only_conformance(), reading the live lake, is the authoritative per-table "
      f"assertion")


# --------------------------------------------------------------------------- #
print("\n[standard] gold's control DDL and the bronze contract match the declaration")

import emit_control_contract as _ecc2  # noqa: E402

# THE DIFF IS THE REVIEW -- the same discipline metadata/key_composition.json and
# data_contracts/ already carry. CAUGHT rather than raised: an exception escaping a
# module-level block aborts verify_repo.py and turns every later check silently ABSENT, which
# is worse than one red check carrying the message.
#
# TWO SEPARATE FAILURE CATEGORIES, not one list. An exception from the emitter (a broken
# import, a bug in gold_ddl()/bronze_contract()) is not content drift -- "run
# tools/emit_control_contract.py and review the diff" does not fix it, it just reproduces the
# crash. Review found this conflated: both were funnelled into one `stale:`-labelled list, so
# a dependency failure was reported with the wrong remedy. _ecc_errors carries the former,
# _ecc_stale only ever carries an artefact whose comparison actually completed and differed.
_ecc_stale = []
_ecc_errors = []
try:
    if _ecc2.GOLD_DDL_PATH.read_text(encoding="utf-8") != _ecc2.gold_ddl():
        _ecc_stale.append(_ecc2.GOLD_DDL_PATH.name)
except Exception as _exc:  # noqa: BLE001
    _ecc_errors.append(f"{_ecc2.GOLD_DDL_PATH.name}: {type(_exc).__name__}: {_exc}")
try:
    if _ecc2.BRONZE_DDL_PATH.read_text(encoding="utf-8") != _ecc2.bronze_ddl():
        _ecc_stale.append(_ecc2.BRONZE_DDL_PATH.name)
except Exception as _exc:  # noqa: BLE001
    _ecc_errors.append(f"{_ecc2.BRONZE_DDL_PATH.name}: {type(_exc).__name__}: {_exc}")
try:
    if (_ecc2.BRONZE_CONTRACT_PATH.read_text(encoding="utf-8")
            != _ecc2.render(_ecc2.bronze_contract())):
        _ecc_stale.append(_ecc2.BRONZE_CONTRACT_PATH.name)
except Exception as _exc:  # noqa: BLE001
    _ecc_errors.append(f"{_ecc2.BRONZE_CONTRACT_PATH.name}: {type(_exc).__name__}: {_exc}")

check("regenerating gold's DDL, bronze's DDL and the bronze contract raises no error",
      not _ecc_errors,
      f"the emitter itself is broken, not just stale: {_ecc_errors} -- fix "
      f"tools/emit_control_contract.py (or a dependency it imports); re-running it will only "
      f"reproduce this crash")

check("regenerating gold's DDL, bronze's DDL and the bronze contract is a no-op",
      not _ecc_stale,
      f"stale: {_ecc_stale} -- run tools/emit_control_contract.py and review the diff")

# The bronze contract is a document another team acts on. A stale one is worse than none.
#
# STRUCTURAL, NOT SUBSTRING. A raw `t in text` test is satisfied by a core table name that
# appears only inside an unrelated comment or another table's prose -- it never actually
# checks the contract's shape. Parse the committed YAML and compare the `tables` mapping's
# keys against _cs.CORE, the same set-comparison discipline Task 3's analogous check in
# tests/test_accelerator.py already uses on the in-memory dict.
_ecc_contract_tables: set[str] = set()
if _ecc2.BRONZE_CONTRACT_PATH.is_file():
    try:
        _ecc_contract_parsed = yaml.safe_load(
            _ecc2.BRONZE_CONTRACT_PATH.read_text(encoding="utf-8"))
        _ecc_contract_tables = set((_ecc_contract_parsed or {}).get("tables") or {})
    except Exception:  # noqa: BLE001 -- see below; narrower is a suite-aborting trap
        # NOT just yaml.YAMLError. Measured: appending an invalid UTF-8 byte to the committed
        # bronze.yaml raised UnicodeDecodeError out of read_text() and ABORTED verify_repo.py
        # outright, making every later check silently absent -- precisely the failure the
        # comment above this block says it exists to avoid. The first pair of checks already
        # catches bare Exception; this one has to agree with them or the corrupt-file case
        # degrades in one place and aborts in the other.
        _ecc_contract_tables = set()
_ecc_contract_missing = sorted(set(_cs.CORE) - _ecc_contract_tables)
check("the committed bronze contract names every core table",
      not _ecc_contract_missing,
      f"missing from the committed contract's `tables` keys: {_ecc_contract_missing} -- "
      f"another team acts on this file, and it is empty, unparsable, or short")


# --------------------------------------------------------------------------- #
# WHAT GOES INTO EVERY HASH KEY. The second half of the hash contract: golden_hash_vectors
# pins the ALGORITHM (and hash_parity_check proves Spark matches the reference), while this
# pins the COMPONENTS. The right algorithm over the wrong components is a perfectly
# reproducible key for the wrong thing, and it fails silently -- the loader builds, rows
# land, and a join to the parent hub returns nothing.
#
# THIS GATE WAS ADDED BECAUSE THE REPO DID NOT HAVE ONE. Measured 4 Sep: dropping the
# source scope, reversing a link's parent order, and dropping a link's transaction key each
# passed all 121 structural checks and all 948 verification checks.
#
# IT IS NOT THE [keys] SECTION ABOVE. That one digests the entity YAML, so it fires when a
# DECLARATION changes -- and it is why those three mutations passed: they changed the code
# that READS the declarations, so no digest moved. Declaration and derivation are two
# halves, and neither gate covers the other's half.
print("\n[keys] the DERIVATION of every hash key matches the golden record")

import emit_key_derivation as _keyc  # noqa: E402

_keyc_model = spec.load_model(ROOT / "metadata" / "entities")
try:
    _keyc_want = _keyc.render(_keyc_model)
    _keyc_err = ""
except Exception as _exc:  # noqa: BLE001
    _keyc_want, _keyc_err = "", f"{type(_exc).__name__}: {_exc}"
check("regenerating the key-component record raises no error",
      not _keyc_err,
      f"{_keyc_err} -- the emitter is broken, not merely stale")
check("the committed key-component record matches what the model derives",
      not _keyc_err and _keyc.GOLDEN_PATH.is_file()
      and _keyc.GOLDEN_PATH.read_text(encoding="utf-8") == _keyc_want,
      "a hash key's composition changed. Every stored key built the old way is now "
      "unjoinable to every key built the new way. Run tools/emit_key_derivation.py, read "
      "the diff, and treat it as a re-keying of the estate -- not a regeneration")

_keyc_rec = _keyc.record(_keyc_model) if not _keyc_err else {"keys": {}}
_keyc_keys = _keyc_rec["keys"]

# THE RECORD MUST COVER EVERY BINDING THAT DERIVES A KEY. A golden record that silently
# stopped including an entity would gate nothing for it while still passing -- the DEF-48
# shape (an empty manifest makes an unfailable gate) at artefact scale.
_keyc_expected = sum(
    len(spec.hash_key_columns(_keyc_model, e, s)) for e in _keyc_model.entities
    for s in e.sources
)
check("the record holds one entry per hash-key column of every binding",
      _keyc_expected > 0 and len(_keyc_keys) == _keyc_expected,
      f"record has {len(_keyc_keys)} entr(ies), the model derives {_keyc_expected} -- a "
      f"key absent from the record is a key nothing gates")

# NO KEY MAY BE EMPTY. A hash over no components is a constant: every row in the table
# collapses onto one key. Cheap to assert, catastrophic to miss.
_keyc_empty = sorted(k for k, v in _keyc_keys.items() if not v["components"])
check("no hash key is built from an empty component list",
      not _keyc_empty,
      f"{_keyc_empty} -- a hash over nothing is a constant, so every row would share one "
      f"key and the table would have exactly one distinct identity")

# A FEDERATED KEY CARRIES ITS SOURCE, AN AUTHORED ONE DOES NOT. This is the distinction
# mutation M1 erased while every check passed. It is not cosmetic: dropping the scope makes
# two sources' rows for the same client code collide into one hub row, and adding one where
# it does not belong splits one authored identifier across sources.
_keyc_scope_wrong = sorted(
    k for k, v in _keyc_keys.items()
    if bool(v["source_scope"]) != (v["scope_key_style"] != "authored")
)
check("every federated key is source-scoped and every authored key is not",
      not _keyc_scope_wrong,
      f"{_keyc_scope_wrong} -- a federated key without its source collides two systems' "
      f"identifiers into one; an authored key with one splits a single identifier apart")

# A LINK'S OWN KEY ENDS WITH ITS TRANSACTION KEY, IN THAT ORDER. Mutations M2 and M3
# (reversed parent order, dropped transaction key) both passed everything. Order is part of
# the hash, so a reordering is a re-keying that no count or uniqueness check can see.
_keyc_txn_bad = []
for _e in _keyc_model.entities:
    if _e.kind not in naming.LINK_KINDS or not _e.transaction_key:
        continue
    for _s in _e.sources:
        _comp = _keyc.record(_keyc_model)["keys"].get(
            f"{_e.name}/{_s.name}/{_e.hk_column}", {}).get("components", [])
        if list(_comp[-len(_e.transaction_key):]) != list(_e.transaction_key):
            _keyc_txn_bad.append(f"{_e.name}/{_s.name}")
check("every link/NHL/HAL key ends with its declared transaction key, in declared order",
      not _keyc_txn_bad,
      f"{sorted(set(_keyc_txn_bad))} -- the transaction key is what makes a "
      f"non-historised "
      f"link's rows distinguishable; without it, two different transactions between the "
      f"same parents hash to one key and one of them is lost as a duplicate")

# A LINK SATELLITE'S FOREIGN KEY EQUALS ITS LINK'S OWN KEY, BY CONSTRUCTION. factory.py
# used to promise this in a docstring ("identical to the link's own by construction rather
# than by careful copying"); nothing checked it. If it ever stops holding, every link
# satellite row points at a link row that does not exist.
_keyc_sat_bad = []
for _e in _keyc_model.entities:
    if _e.kind not in naming.SATELLITE_KINDS:
        continue
    _parent = _keyc_model.get(_e.parents[0])
    if _parent.kind not in naming.LINK_KINDS:
        continue
    for _s in _e.sources:
        _sat = _keyc_keys.get(f"{_e.name}/{_s.name}/{naming.hk(_parent.name)}", {})
        _own = _keyc_keys.get(f"{_parent.name}/{_s.name}/{_parent.hk_column}")
        if _own is None:
            # The link has no binding for this source name. The satellite still derives the
            # link's key from its OWN binding, which is the documented path -- there is
            # simply no same-source link entry to compare against.
            continue
        if (_sat.get("components"), _sat.get("source_scope")) != (
                _own["components"], _own["source_scope"]):
            _keyc_sat_bad.append(f"{_e.name}/{_s.name}")
check("every link satellite's foreign key is composed exactly as its link's own key",
      not _keyc_sat_bad,
      f"{sorted(set(_keyc_sat_bad))} -- the satellite's rows would point at link rows that "
      f"were never built, and it is silent: both tables load, and the join returns nothing")

# NO ACTIVE BINDING MAY DERIVE A FOREIGN KEY ITS PARENT NEVER PRODUCES.
#
# A federated key prepends its source name as a literal, so the SCOPE alone decides
# joinability whatever columns follow: a foreign key scoped 'BUSINESS_VAULT' can never
# equal a hub key scoped 'FIELDGLASS_EU'. This walks every ACTIVE binding of every target
# and fails when a parent FK's scope is one no binding of the parent is ever built under.
#
# ACTIVE MEANS ACTIVE, AND THAT IS MOST OF THE ESTATE. Measured 4 Sep: only `dev` and
# `usnc_tds` restrict active_sources. The other seven targets leave it EMPTY, and empty
# means EVERY declared binding is active (spec.is_source_active: None means all) -- so the
# five failures below are live in seven of eight targets, not latent. Whether any of those
# lakes has run is a question about EU production, which this repo does not query.
#
# THE EXCEPTION REGISTER IS AN ACKNOWLEDGEMENT, NOT A SILENCER. Five orphans exist today
# and fixing two of them is a modelling decision about identity, not a cleanup, so they are
# listed in metadata/key_scope_exceptions.json with a reason each. A SIXTH fails this gate.
# The register is also checked for staleness: an entry that stops being an orphan must be
# deleted in the change that fixes it, so it cannot quietly outlive its reason.
_kse_path = ROOT / "metadata" / "key_scope_exceptions.json"
try:
    _kse_raw = json.loads(_kse_path.read_text(encoding="utf-8"))
except Exception as _exc:  # noqa: BLE001
    _kse_raw, _kse_err = {}, f"{type(_exc).__name__}: {_exc}"
else:
    _kse_err = ""
_kse = {
    k: v for group in _kse_raw.values() if isinstance(group, dict)
    for k, v in group.items() if not k.startswith("_")
}
check("the key-scope exception register parses and every entry carries a reason",
      not _kse_err and all(isinstance(v, str) and v.strip() for v in _kse.values()),
      f"{_kse_err or 'an entry has no reason'} -- an acknowledged orphan with no stated "
      f"reason is not an acknowledgement")

_keyc_own_scopes: dict[str, set] = {}
for _k, _v in _keyc_keys.items():
    _ent, _src, _col = _k.split("/")
    if _col == _keyc_model.get(_ent).hk_column:
        _keyc_own_scopes.setdefault(_ent, set()).add(_v["source_scope"])

_keyc_orphan, _keyc_seen = [], set()
for _tname, _tcfg in ((bundle or {}).get("targets", {}) or {}).items():
    _as = ((_tcfg.get("variables") or {}).get("active_sources") or "")
    _active = frozenset(x.strip() for x in str(_as).split(",") if x.strip()) or None
    for _e in _keyc_model.entities:
        for _s in _e.sources:
            if not spec.is_source_active(_active, _e, _s):
                continue
            # LEG -> HUB, from the model. `_col[:-3]` reads a roled FK
            # (`parent_legal_entity_hk`) as a hub named `parent_legal_entity`, which does
            # not exist -- so its own-scope set is empty and EVERY roled foreign key looks
            # like an orphan. Building the map from parent_legs resolves the leg to the
            # hub it actually points at.
            _leg_hub = {naming.hk(h, r): h for h, r in spec.parent_legs(_e)}
            for _col, (_cols, _scope) in spec.hash_key_columns(_keyc_model, _e, _s).items():
                if _col == _e.hk_column:
                    continue
                _parent = _leg_hub.get(_col, _col[:-3])
                if _scope in _keyc_own_scopes.get(_parent, set()):
                    continue
                _id = f"{_e.name}/{_s.name}/{_col}"
                _keyc_seen.add(_id)
                if _id not in _kse:
                    _keyc_orphan.append(f"{_tname}: {_id} scoped {_scope!r}, but {_parent} "
                                        f"is only ever keyed under "
                                        f"{sorted(str(x) for x in _keyc_own_scopes.get(_parent, set()))}")
check("no UNACKNOWLEDGED active binding derives a parent key its parent never produces",
      not _keyc_orphan,
      "; ".join(sorted(set(_keyc_orphan))) + " -- this foreign key cannot equal the key the "
      "parent builds for itself, so every row written points at a parent row that does not "
      "exist. Silent: both tables load and the join returns nothing. Fix it, or add it to "
      "metadata/key_scope_exceptions.json with a reason and have that diff reviewed")

# STALE ENTRIES MUST GO. A register that keeps an exception after the defect is fixed is a
# standing licence for the defect to come back unnoticed -- the same shape as a gate that
# cannot fail.
_kse_stale = sorted(set(_kse) - _keyc_seen)
check("the exception register lists no key that has stopped being an orphan",
      not _kse_stale,
      f"{_kse_stale} -- fixed, or no longer active. Delete the entr(ies) from "
      f"metadata/key_scope_exceptions.json in the change that fixed them")


# --------------------------------------------------------------------------- #
# THE SOURCE-TO-TARGET MAPPING. Generated from metadata/entities, gated the same way as the
# contracts, the diagram and the ontology.
#
# THE POINT OF GATING A DOCUMENT. A source-to-target mapping kept by hand is stale the first
# time a binding changes, and it is stale SILENTLY -- nothing fails, the document simply
# stops describing the pipeline, and it keeps being read. Byte-identical regeneration turns
# that silence into a build failure.
print("\n[s2t] the Bronze-to-Silver mapping matches the model")

import emit_source_to_target as _s2t  # noqa: E402

_s2t_model = spec.load_model(ROOT / "metadata" / "entities")
try:
    _s2t_rows = _s2t.build(_s2t_model)
    _s2t_facts = _s2t.table_facts(_s2t_model)
    _s2t_err = ""
except Exception as _exc:  # noqa: BLE001
    _s2t_rows, _s2t_facts, _s2t_err = [], [], f"{type(_exc).__name__}: {_exc}"
check("regenerating the mapping raises no error",
      not _s2t_err, f"{_s2t_err} -- the emitter is broken, not merely stale")

if not _s2t_err:
    from emit_s2t_html import render_html as _s2t_html  # noqa: E402
    check("the committed mapping CSV matches what the model generates",
          _s2t.CSV_PATH.is_file()
          and _s2t.CSV_PATH.read_text(encoding="utf-8") == _s2t.render_csv(_s2t_rows),
          "run tools/emit_source_to_target.py and review the diff")
    check("the committed mapping HTML matches what the model generates",
          _s2t.HTML_PATH.is_file()
          and _s2t.HTML_PATH.read_text(encoding="utf-8") == _s2t_html(_s2t_rows, _s2t_facts),
          "run tools/emit_source_to_target.py and review the diff")

    # Needed by both the workbook checks and the flag check further down, so it is derived
    # once here rather than twice.
    _s2t_warned = {f'{r["target_table"]}.{r["target_column"]}'
                   for r in _s2t_rows if "WILL NOT JOIN" in str(r["notes"])}

    # THE WORKBOOK IS GATED ON ITS ROWS, NOT ITS BYTES OR ITS PARTS.
    #
    # IT WAS GATED ON PARTS FOR ABOUT FOUR HOURS. Opening the published workbook in
    # LibreOffice and saving rewrote the package -- docProps/core.xml with a creation
    # timestamp, a sharedStrings table, two theme parts -- and the part-equality check
    # failed on a file whose content was untouched. A gate that fires when the document is
    # USED as intended is worse than no gate: the fix a hurried reader reaches for is to
    # delete it. (The timestamp part is the exact thing tools/xlsx_writer.py exists to
    # avoid; the reader's own application put it back.)
    #
    # So compare the ROWS, read back through xlsx_writer.read_sheets, which understands
    # both inline strings and a shared-strings table. Container churn is then free, while a
    # hand-edited cell still fails -- correctly, because a hand-edited cell in a generated
    # document is precisely the drift this discipline exists to catch.
    _s2t_want_parts = _s2t.workbook_sheets(_s2t_rows, _s2t_facts)
    import xlsx_writer as _xw  # noqa: E402

    try:
        _s2t_have_sheets = _xw.read_sheets(_s2t.XLSX_PATH)
        _s2t_xlsx_err = ""
    except Exception as _exc:  # noqa: BLE001
        _s2t_have_sheets, _s2t_xlsx_err = {}, f"{type(_exc).__name__}: {_exc}"
    check("the committed mapping workbook opens and its sheets can be read back",
          not _s2t_xlsx_err and bool(_s2t_have_sheets),
          f"{_s2t_xlsx_err} -- Excel will refuse a workbook this cannot read either")

    _s2t_want_sheets = {sh.name: [list(sh.header)] + [[str(c or "") for c in r]
                                                      for r in sh.rows]
                        for sh in _s2t_want_parts}
    check("the workbook holds the same sheets the model generates",
          set(_s2t_have_sheets) == set(_s2t_want_sheets),
          f"committed {sorted(_s2t_have_sheets)}, model {sorted(_s2t_want_sheets)}")

    _s2t_sheet_diff = []
    for _name, _want_rows in sorted(_s2t_want_sheets.items()):
        _have_rows = _s2t_have_sheets.get(_name)
        if _have_rows is None:
            continue
        if len(_have_rows) != len(_want_rows):
            _s2t_sheet_diff.append(f"{_name}: {len(_have_rows)} row(s) vs "
                                   f"{len(_want_rows)} generated")
            continue
        for _i, (_h, _w) in enumerate(zip(_have_rows, _want_rows)):
            # A trailing empty cell is absence in the file and "" in the build; compare
            # the meaningful prefix so a writer's economy is not read as a difference.
            _hh = list(_h) + [""] * (len(_w) - len(_h))
            if [x.strip() for x in _hh[:len(_w)]] != [str(x or "").strip() for x in _w]:
                _s2t_sheet_diff.append(f"{_name} row {_i}: {_hh[:3]} vs "
                                       f"{[str(x or '') for x in _w][:3]}")
                break
    check("every workbook cell matches what the model generates",
          not _s2t_sheet_diff,
          f"{_s2t_sheet_diff[:3]} -- run tools/emit_source_to_target.py. If a cell was "
          f"edited by hand, that edit is drift: the model is the source, not the workbook")

    # EVERY PART MUST BE WELL-FORMED XML. A workbook that is byte-correct against a broken
    # generator is still a workbook Excel opens with an error dialogue, and the reader's
    # conclusion is that the mapping is broken rather than the writer.
    _s2t_xml_bad = []
    for _n, _body in sorted(_xw.workbook_parts(_s2t_want_parts).items()):
        try:
            ET.fromstring(_body)
        except Exception as _exc:  # noqa: BLE001
            _s2t_xml_bad.append(f"{_n}: {type(_exc).__name__}")
    check("every workbook part is well-formed XML",
          not _s2t_xml_bad,
          f"{_s2t_xml_bad} -- Excel will show an error dialogue, and the reader will "
          f"conclude the mapping is broken rather than the writer")

    # THE SHEETS MUST HOLD THE SAME ROWS AS THE OTHER TWO RENDERINGS. Three renderings of
    # one mapping is the claim the document makes in its own opening paragraph; a workbook
    # that quietly held a subset would make that claim false.
    _s2t_map_sheet = next((x for x in _s2t_want_parts if x.name == "Mapping"), None)
    check("the workbook's Mapping sheet holds every mapping row",
          _s2t_map_sheet is not None and len(_s2t_map_sheet.rows) == len(_s2t_rows),
          f"{len(_s2t_map_sheet.rows) if _s2t_map_sheet else 0} sheet row(s) vs "
          f"{len(_s2t_rows)} built -- the workbook would describe a subset of the pipeline "
          f"while the page and the CSV describe all of it")
    _s2t_warn_sheet = next((x for x in _s2t_want_parts if x.name == "Will not join"), None)
    check("the workbook's warn sheet holds exactly the flagged rows",
          (_s2t_warn_sheet is not None) == bool(_s2t_warned)
          and (not _s2t_warn_sheet or len(_s2t_warn_sheet.rows) == len(_s2t_warned)),
          f"sheet has {len(_s2t_warn_sheet.rows) if _s2t_warn_sheet else 0}, "
          f"{len(_s2t_warned)} row(s) are flagged -- the sheet a reader opens FIRST must "
          f"not be the one that is short")

    # THE TWO RENDERINGS MUST DESCRIBE ONE MAPPING. They are built from a single row list,
    # so this cannot drift today -- and it is asserted anyway, because the day someone adds
    # a filter or a projection to one renderer is the day a reader of the CSV and a reader
    # of the page start disagreeing about what loads, with nothing to tell them.
    _s2t_csv_rows = list(csv.DictReader(io.StringIO(_s2t.render_csv(_s2t_rows))))
    check("the CSV holds exactly the rows the HTML renders, in the same order",
          len(_s2t_csv_rows) == len(_s2t_rows)
          and all(c["target_column"] == r["target_column"]
                  and c["target_table"] == r["target_table"]
                  and c["rule"] == r["rule"]
                  for c, r in zip(_s2t_csv_rows, _s2t_rows)),
          f"{len(_s2t_csv_rows)} CSV row(s) vs {len(_s2t_rows)} built -- the Excel copy and "
          f"the page would describe different pipelines")

    # EVERY TARGET COLUMN OF EVERY TABLE, OR THE DOCUMENT IS WORSE THAN ABSENT. A mapping
    # missing a column does not read as incomplete; it reads as a column with no source,
    # and the reader concludes the pipeline invents it.
    # STABLE, matching emit_source_to_target.build(): the mapping now names the view a
    # reader queries, not the physical _rev<N> table a cutover moves.
    _s2t_want = set()
    for _e in _s2t_model.entities:
        for _src, _tbl in _e.stable_tables():
            for _b in spec.table_bindings(_e, _src):
                for _c in _s2t.contract_columns(_e, _b):
                    _s2t_want.add((_tbl, _c))
    _s2t_have = {(r["target_table"], r["target_column"]) for r in _s2t_rows}
    check("every target column of every table appears in the mapping",
          _s2t_want == _s2t_have,
          f"missing {sorted(_s2t_want - _s2t_have)[:6]}, extra "
          f"{sorted(_s2t_have - _s2t_want)[:6]} -- a column with no row reads as a column "
          f"with no source")

    # NO ROW MAY BE SILENT ABOUT ITS RULE. A blank rule cell is the placeholder shape this
    # emitter was rewritten to remove: it printed "<parent key>" for twelve satellite
    # foreign keys, which is a hole that looks like content.
    _s2t_ruleless = [f'{r["target_table"]}.{r["target_column"]}'
                     for r in _s2t_rows if not str(r["rule"]).strip()]
    check("every mapping row states a rule",
          not _s2t_ruleless,
          f"{_s2t_ruleless[:6]} -- a blank rule is a hole that looks like content")
    _s2t_placeholder = [f'{r["target_table"]}.{r["target_column"]}'
                        for r in _s2t_rows if "<" in str(r["rule"])]
    check("no mapping rule contains a placeholder",
          not _s2t_placeholder,
          f"{_s2t_placeholder[:6]} -- the emitter could not reach the real derivation. It "
          f"must read spec.hash_key_columns, not guess")

    # A HASH-KEY ROW MUST NAME THE RULEBOOK VERSION. The components and the normalisation
    # together define the key, so naming one without the other describes two different keys
    # identically -- and a rulebook bump re-keys the estate.
    _s2t_keyrows = [r for r in _s2t_rows if str(r["rule"]).startswith("SHA-256")]
    _s2t_norule = [f'{r["target_table"]}.{r["target_column"]}' for r in _s2t_keyrows
                   if f"rulebook {hashing.RULEBOOK_VERSION}" not in r["rule"]]
    check("every derived-key row names the current rulebook version",
          bool(_s2t_keyrows) and not _s2t_norule,
          f"{_s2t_norule[:6]} of {len(_s2t_keyrows)} key row(s) -- components without a "
          f"rulebook version describe two different keys the same way")

    # A MASKED COLUMN MUST CARRY ITS MASK HERE TOO. Same reasoning as the diagram's MASKED
    # note: this document travels, and a sensitive column that looks ordinary in a mapping
    # invites a downstream projection that drops the mask.
    _s2t_declared_masks = {
        (_t, _c) for _e in _s2t_model.entities for _src, _t in _e.stable_tables()
        for _c, _m in (getattr(_e, "masks", ()) or ())
    }
    _s2t_row_masks = {(r["target_table"], r["target_column"])
                      for r in _s2t_rows if str(r["mask"]).strip()}
    check("every column the model declares a mask on carries that mask in the mapping",
          bool(_s2t_declared_masks) and _s2t_declared_masks <= _s2t_row_masks,
          f"unmarked: {sorted(_s2t_declared_masks - _s2t_row_masks)[:6]} -- a masked column "
          f"looking ordinary in a document that travels invites a join on cleartext")

    # AN UNJOINABLE FOREIGN KEY MUST SAY SO ON ITS OWN ROW. The [keys] section already gates
    # the model; this gates the DOCUMENT, which is what a reader actually acts on. The two
    # counts come from different code paths -- the register and the emitter's own
    # derivation -- so this also catches the two disagreeing.
    check("the mapping flags an unjoinable foreign key on every row that has one",
          len(_s2t_warned) == len(_kse),
          f"{len(_s2t_warned)} row(s) flagged, {len(_kse)} acknowledged in "
          f"metadata/key_scope_exceptions.json -- a reader acts on the document, so an "
          f"orphan the register knows about and the page does not is the dangerous direction")


# --------------------------------------------------------------------------- #
# A COLUMN DESCRIPTION MUST SAY SOMETHING THE COLUMN NAME DOES NOT.
#
# This is the gate that makes the whole feature safe to grow. 133 distinct business column
# names carry no description, and the tempting way to close that number is a sentence per
# column restating its own name -- "job_title: the job title". That reads as documentation,
# is indistinguishable from the real thing to anyone reading later, and leaves the reader
# no better off than the blank did. Worse, it converts a visible gap into an invisible one.
#
# So a description whose content words are all contained in its column's own name FAILS.
# Blank stays legal and cheap: the honest state of a column nobody has described is nothing.
_DESC_STOP = frozenset({
    "a", "an", "the", "of", "for", "to", "in", "on", "this", "that", "is", "are", "and",
    "or", "it", "its", "as", "by", "from", "with", "was", "were", "be", "been", "column",
    "field", "value", "values",
})


def _desc_words(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z]+", text.lower()) if w not in _DESC_STOP}


_desc_all: list[tuple[str, str, str]] = []       # (table, column, description)
for _e in model.entities:
    for _s, _t in _e.tables():
        # _edc.columns, NOT factory._projection: factory imports pyspark at module
        # level and the offline CI job has none. emit_data_contract installs the same
        # minimal stub tests/test_accelerator.py does and exposes this reader, so the
        # deferral already has one home and does not need a second.
        for _c in _edc._projected_columns(_e, _s):
            _d = contract.description(_e, _c, model)
            if _d:
                _desc_all.append((_t, _c, _d))

_tautologies = [
    f"{t}.{c}: {d!r}" for t, c, d in _desc_all
    if _desc_words(d) and _desc_words(d) <= _desc_words(c.replace("_", " "))
]
# A FLOOR FIRST. If the projection walk or the resolver broke, _desc_all would be empty,
# _tautologies would be empty, and this gate would pass having read nothing.
check("the description sweep actually finds descriptions to check",
      len(_desc_all) >= 200,
      f"{len(_desc_all)} described column(s) across the model -- the walk or "
      f"contract.description is broken and this gate would pass by examining nothing")
# WHAT IT CATCHES AND WHAT IT MISSES, measured rather than assumed. The test is word
# containment, so it works on an underscore-separated name -- 93 of the 133 distinct
# business columns, 69% -- and is BLIND on a concatenated one: `debitamt: the debit amount`
# passes, because {debit, amount} is not a subset of {debitamt}. Measured with exactly that
# mutation on 5 Sep 2026.
#
# Left as it is deliberately. Splitting `debitamt` would need a dictionary or a heuristic,
# and a heuristic that guesses wrong REJECTS a real description, which is a worse failure
# than letting a tautology through: it teaches the author to fight the gate. The 40
# concatenated names are legacy GP and UKG spellings, and none carries a description today.
check("no column description merely restates its own column name",
      not _tautologies,
      f"{_tautologies[:4]} -- a sentence that says only what the name says reads as "
      f"documentation and cannot be told from the real thing later. Leave it blank; the "
      f"gap is honest and visible")

# AND EVERY DESCRIPTION MUST REACH THE ARTEFACTS THAT PUBLISH IT.
#
# The contract, the diagram and the ontology all resolve through contract.description in
# one run, so they cannot disagree by accident -- but an emitter that stopped threading it
# would drop the text silently and every byte-gate would still pass, because the committed
# file would match the newly-broken output. This is the cross-check that notices, the same
# shape as the constraint DDL against the diagram's Ref: count.
# UNESCAPED BEFORE SEARCHING. DBML quotes a note in single quotes and escapes any
# apostrophe inside it, so "this row's change" is written `this row\'s change` and a
# substring test against the description as authored finds nothing. The first run of this
# check reported five false positives for exactly that -- every description containing an
# apostrophe -- which is a check failing on its own comparison rather than on the thing it
# claims to police.
_dbml_text = (ROOT / "diagram" / "hfig_data_vault.dbml").read_text(
    encoding="utf-8").replace("\\'", "'")
_desc_unpublished = sorted(
    f"{t}.{c}" for t, c, d in _desc_all if d[:40] not in _dbml_text
)[:5]
check("every column description reaches the DBML diagram",
      not _desc_unpublished,
      f"{_desc_unpublished} -- resolved by contract.description but absent from the "
      f"diagram, so emit_dbml_diagram has stopped threading it and the byte-gate cannot "
      f"see that: the committed file matches the broken output")

# THE ONTOLOGY'S SHARE IS SMALL TODAY AND THAT IS NOT A DEFECT. Its datatype properties
# are satellite PAYLOAD columns only -- precisely the business columns nobody has
# described -- so it carries a handful of comments where the diagram carries 251. Asserted
# as a floor rather than a match, so the number can grow without editing this line and a
# regression to zero still fails.
#
# COUNTED ON THE DATATYPE PROPERTIES ONLY, and the first version of this check was vacuous
# for want of that. It counted every rdfs:comment in the file against a floor of 10 -- but
# the file carries 25, from the ontology header and the object properties' role comments,
# so deleting all 10 datatype comments still cleared the floor. Mutation found it: the
# emitter was made to skip them and the check passed. That is a guarantee exceeding what
# the fixture could see, which is this repo's most common defect shape.
_ttl_text = (ROOT / "onto" / "hfig_data_vault.ttl").read_text(encoding="utf-8")
_ttl_dt_commented = sum(
    1 for _b in _ttl_text.split("rdf:type owl:DatatypeProperty")[1:]
    if "rdfs:comment" in _b.split(" .")[0]
)
check("the ontology publishes the descriptions its datatype properties can carry",
      _ttl_dt_commented >= 10,
      f"{_ttl_dt_commented} datatype propert(ies) carry rdfs:comment -- emit_ontology has "
      f"stopped emitting them. The number is small because these are satellite PAYLOAD "
      f"columns, which are exactly the ones nobody has described; it is not zero, and a "
      f"floor rather than a match so it can grow without editing this line")

# THE FREE ONES MUST ACTUALLY BE COVERED. Technical columns and keys derive their meaning
# from the model, so a blank one is a broken resolver rather than an undescribed column --
# and stating it structurally avoids writing down a count that would drift.
_desc_gap = sorted({
    f"{t}.{c}" for _e in model.entities for _s, t in _e.tables()
    for c in _edc._projected_columns(_e, _s)
    if (c in naming.SYSTEM_COLUMNS or c.endswith("_hk") or c == naming.COL["hashdiff"])
    and not contract.description(_e, c, model)
})
check("every system column, hash key and hashdiff resolves to a description",
      not _desc_gap,
      f"{_desc_gap[:5]} -- these derive their meaning from the model, so a blank one means "
      f"naming.COLUMN_DOC or contract.description stopped resolving, not that nobody has "
      f"written it")

# --------------------------------------------------------------------------- #
# THE ARCHIFY DOCUMENTS MUST BE DERIVED, CONFORMANT, DRAWABLE, AND AGREE WITH THEIR SOURCES.
#
# Archify's own workflow is "the agent creates typed JSON IR from your description", and it
# then compiles that JSON deterministically. The determinism is in the RENDER, not the
# derivation -- so its intended users get a diagram that can be beautifully drawn and wrong.
# These are emitted from metadata/entities/ and resources/vault_job.yml instead, which is
# only worth anything if it is enforced.
#
# THE FOURTH ADJECTIVE ARRIVED ON 6 SEPTEMBER AND IT COST TWO COMMITTED ARTEFACTS. Until
# the renderer was installed, "conformant" meant conformant to the vendored JSON Schemas,
# and this section said so with a straight face. The schemas do not describe the
# renderer's geometry: dataflow.schema.json bounds a node's `row` at `minimum: 0`, the
# dataflow renderer has five rows, and seven of twenty-nine nodes had been sitting at rows
# 5..12 producing non-finite coordinates while passing every check here. Schema
# conformance and drawability are different properties and this now asserts both.
sys.path.insert(0, str(ROOT / "tools"))
from accelerator import archify as _arch  # noqa: E402

import archify_render_gate as _arg  # noqa: E402
import emit_archify as _emit_arch  # noqa: E402

_arch_docs = _emit_arch.documents()
for _kind, _rel, _doc in _arch_docs:
    _committed = (ROOT / _rel).read_text(encoding="utf-8")
    check(f"the committed {_rel.split('/')[-1]} matches what the model generates",
          _emit_arch.render(_doc) == _committed,
          f"run tools/emit_archify.py and review the diff -- the diagram and the thing it "
          f"claims to describe have diverged")
    # CONFORMANCE AGAINST THE VENDORED SCHEMA, not against our idea of it. archify.validate
    # reads `required`, `const`, `enum`, `pattern` and additionalProperties straight out of
    # their JSON Schema, so there is one authority and it is theirs. It is also the WEAKER
    # of the two gates here, for the reason given above.
    _findings = _arch.validate(_doc, _kind)
    check(f"{_rel.split('/')[-1]} conforms to the vendored {_kind} schema",
          not _findings,
          f"{_findings[:3]} -- vendor/archify/schemas/{_kind}.schema.json is the contract; "
          f"if they changed it upstream, re-copy and read the diff")

# EVERY DOMAIN GETS A DOCUMENT, AND NOTHING ELSE IS LEFT ON DISK. The emitter derives the
# domain list from the entities and deletes what it did not write; this asserts the result,
# because a stale diagram is worse than a missing one -- it is read and believed.
_arch_on_disk = {p.relative_to(ROOT).as_posix()
                 for p in (ROOT / "diagram").glob("hfig_*.archify.json")}
_arch_expected = {_rel for _, _rel, _ in _arch_docs}
check("the diagram/ directory holds exactly the Archify documents the model implies",
      _arch_on_disk == _arch_expected,
      f"orphaned: {sorted(_arch_on_disk - _arch_expected)}; missing: "
      f"{sorted(_arch_expected - _arch_on_disk)} -- run tools/emit_archify.py")
# AND EACH HAS A COMMITTED IMAGE, because GitHub serves a committed .html as a source blob
# and these are the only form of the diagrams a person can see in a browser. EXISTENCE ONLY,
# and the limit is stated rather than implied: regenerating an image needs Chrome, so this
# cannot assert that an image matches its document. It catches a domain appearing or
# disappearing, which is the drift a generated set actually suffers.
_arch_images = {p.name for p in (ROOT / "diagram" / "img").glob("*.png")}
_arch_expected_images = {_rel.split("/")[-1].replace(".archify.json", ".png")
                         for _, _rel, _ in _arch_docs}
check("every Archify document has a committed image, and none is orphaned",
      _arch_images == _arch_expected_images,
      f"orphaned: {sorted(_arch_images - _arch_expected_images)}; missing: "
      f"{sorted(_arch_expected_images - _arch_images)} -- run tools/render_archify.py. "
      f"This gate cannot tell you whether an image is STALE, only whether it is there")
check("the committed images are not empty files",
      all((ROOT / "diagram" / "img" / n).stat().st_size > 10_000
          for n in _arch_expected_images & _arch_images),
      "a truncated capture renders as a broken image on GitHub and as a pass here")

check("every domain in the model has an architecture document",
      {f"diagram/hfig_{d}.archify.json" for d in _arch.domains(model)} <= _arch_expected,
      f"domains {_arch.domains(model)} against documents {sorted(_arch_expected)} -- a "
      f"domain with no diagram is a part of the vault nobody can see")

# ARCHIFY ITSELF IS ASKED WHETHER IT CAN DRAW THESE. The gate above proves the bytes match
# the model; this one proves the model produced something renderable. It needs Node and
# the installed skill, so it SKIPS where those are absent -- CI among them, which is a
# stated hole rather than a hidden one. See docs/superpowers/OPEN_ITEMS.md.
_arg_reason = _arg.unavailable()
if _arg_reason:
    print(f"  SKIP  the Archify render gate did not run: {_arg_reason}")
else:
    for _kind, _rel, _ in _arch_docs:
        _render = _arg.check_document(ROOT / _rel, _kind)
        check(f"Archify can actually draw {_rel.split('/')[-1]}",
              not _render,
              f"{_render[:2]} -- the renderer's own validator refuses this document; "
              f"schema conformance does not imply drawability, which is the defect this "
              f"gate exists for")

# THE COMMITTED PAGES MUST MATCH A FRESH RENDER. diagram/html/ is tracked because the
# documents are of no use to a reader as JSON, and a tracked generated artefact gets the
# same treatment as every other one here: regenerate, compare, fail on a diff.
#
# IT NEEDS THE RENDERER, AND THE RIGHT ONE. `deliver` is byte-deterministic for a fixed
# Archify version -- verified by rendering the set twice -- and says nothing about a
# different one. So this stands down on a version mismatch with a stated reason rather
# than failing the build for something that is not this repo's fault. Where it does run it
# is a real byte-gate: a hand-edited page, or a page left behind by an older model, fails.
_arch_html_dir = ROOT / "diagram" / "html"
_arch_html_expected = {f"{_rel.split('/')[-1].replace('.archify.json', '')}.html"
                       for _, _rel, _ in _arch_docs}
_arch_html_on_disk = {p.name for p in _arch_html_dir.glob("*.html")}
check("diagram/html/ holds exactly one page per Archify document",
      _arch_html_on_disk == _arch_html_expected,
      f"orphaned: {sorted(_arch_html_on_disk - _arch_html_expected)}; missing: "
      f"{sorted(_arch_html_expected - _arch_html_on_disk)} -- run tools/render_archify.py")

_arch_installed = _arg.renderer_version()
if _arg_reason:
    print(f"  SKIP  the committed pages were not re-rendered: {_arg_reason}")
elif _arch_installed != _arch.RENDERER_VERSION:
    print(f"  SKIP  the committed pages were not re-rendered: Archify {_arch_installed} is "
          f"installed, they were rendered with {_arch.RENDERER_VERSION} -- `deliver` output "
          f"is only byte-stable within one version. Re-run tools/render_archify.py and "
          f"bump accelerator.archify.RENDERER_VERSION in the same commit")
else:
    import tempfile as _tf

    with _tf.TemporaryDirectory() as _tmp:
        for _kind, _rel, _ in _arch_docs:
            _name = f"{_rel.split('/')[-1].replace('.archify.json', '')}.html"
            _fresh = Path(_tmp) / _name
            _err = _arg.deliver(ROOT / _rel, _kind, _fresh)
            check(f"the committed {_name} matches a fresh render",
                  _err is None and _fresh.read_bytes()
                  == (_arch_html_dir / _name).read_bytes(),
                  _err or "run tools/render_archify.py and review -- the committed page "
                          "and the document it claims to draw have diverged")

# AND EACH MUST AGREE WITH THE ARTEFACT IT SHARES A DERIVATION WITH. Both read the same
# authorities the DBML and the job file do, so a disagreement means one of them stopped --
# and the byte-gates above cannot see that, because a broken emitter's output still matches
# its own committed copy.
_arch_by_kind = {}
for _kind, _rel, _doc in _arch_docs:
    _arch_by_kind.setdefault(_kind, []).append(_doc)
_arch_wf = _arch_by_kind["workflow"][0]

# DEDUPED, BECAUSE A CROSS-DOMAIN EDGE IS DRAWN TWICE ON PURPOSE -- once in each domain's
# document, so neither lies by omission. The union is what must equal the DBML.
_arch_edges = {(c["from"], c["to"], c.get("label", ""))
               for d in _arch_by_kind["architecture"] for c in d["connections"]}
_arch_dbml_refs = (ROOT / "diagram/hfig_data_vault.dbml").read_text("utf-8").count("\nRef:")
check("the Archify documents draw exactly the edges the DBML diagram draws",
      len(_arch_edges) == _arch_dbml_refs,
      f"{len(_arch_edges)} distinct connections against {_arch_dbml_refs} Ref: lines -- "
      f"both derive from contract.foreign_keys, so they cannot legitimately differ")

# AND EXACTLY THE TABLES. A domain document draws other domains' tables for context, so
# only the non-external components count as drawn-here; together they must be every table.
# STABLE, matching archify._tables(): components are keyed on the view a consumer reads.
_arch_own = {c["id"] for d in _arch_by_kind["architecture"] for c in d["components"]
             if c["type"] != "external"}
_model_tables = {t for e in model.entities for _s, t in e.stable_tables()}
check("the Archify documents draw every table in the model, each in one domain",
      _arch_own == _model_tables,
      f"drawn but not in the model: {sorted(_arch_own - _model_tables)[:3]}; in the model "
      f"but undrawn: {sorted(_model_tables - _arch_own)[:3]} -- a table missing from every "
      f"diagram is invisible to the only picture anyone reads")

_arch_job_tasks = {t["task_key"] for t in
                   yaml.safe_load((ROOT / "resources/vault_job.yml").read_text("utf-8"))
                   ["resources"]["jobs"]["vault_load"]["tasks"]}
check("the Archify workflow draws every task in the vault job and no others",
      {n["id"] for n in _arch_wf["nodes"]} == _arch_job_tasks,
      f"drawn but absent from the job: "
      f"{sorted({n['id'] for n in _arch_wf['nodes']} - _arch_job_tasks)[:3]}; in the job "
      f"but undrawn: {sorted(_arch_job_tasks - {n['id'] for n in _arch_wf['nodes']})[:3]} "
      f"-- a task graph missing a task is worse than none, because it is believed")

# THE WORKFLOW STAYS ON SCHEMA 2, AND THAT IS NOT A PREFERENCE. Version 1 is a fixed
# geometry with six hard-coded column centres and a 92px node that refuses anything wider;
# every task key in this job is wider than 92px, so a silent downgrade to v1 makes the
# document undrawable again. The render gate catches it where it runs; this catches it
# everywhere.
check("the Archify workflow document uses the measured schema-2 layout contract",
      _arch_wf["schema_version"] == 2 and all("width" in n for n in _arch_wf["nodes"]),
      f"schema_version {_arch_wf['schema_version']}, "
      f"{sum('width' not in n for n in _arch_wf['nodes'])} node(s) with no measured width "
      f"-- v1's fixed 92px box cannot hold a single one of these task keys")

# NON-VACUITY. An emitter returning empty documents would match empty committed files,
# conform to the schemas trivially, render perfectly, and pass everything above.
check("the Archify documents are not empty",
      len(_arch_own) >= 25 and len(_arch_wf["nodes"]) >= 20
      and len(_arch_wf["edges"]) >= 20,
      f"{len(_arch_own)} tables drawn, workflow {len(_arch_wf['nodes'])} tasks / "
      f"{len(_arch_wf['edges'])} edges -- the derivation broke and these documents assert "
      f"almost nothing")

# --------------------------------------------------------------------------- #
# THE UC CONSTRAINT DDL MUST MATCH THE MODEL, AND MUST NOT BE WIRED INTO A JOB YET.
#
# Same shape as every other generated artefact: regenerate, compare, fail on a diff -- so
# the diff is the review. What is different is the second half. Two questions about this
# DDL can only be answered by a workspace: whether an SDP streaming table accepts
# ALTER TABLE ... ADD CONSTRAINT at all, and whether every key column is already NOT NULL,
# which Unity Catalog requires of a primary key. Until both are answered, a job task
# running this would fail on every load, so its ABSENCE from the job is asserted as
# deliberately as its content -- exactly the treatment governance/control_objects_gold.sql
# has had since 29 August.
sys.path.insert(0, str(ROOT / "tools"))

# --------------------------------------------------------------------------- #
# THE DOMAIN-SPLIT TOPOLOGY MATCHES THE MODEL IT IS DERIVED FROM.
#
# One raw pipeline per domain, generated. The generation is byte-gated like every other
# emitted artefact; these checks cover what generation alone cannot -- the JOB graph, which
# is hand-written because its 25 tasks carry reasoning no template should own.
#
# WHY GATES RATHER THAN MORE GENERATION. The job file is mostly prose and per-task argument
# lists. Generating it would move that reasoning into Python strings; gating it catches
# drift in BOTH directions -- a pipeline with no task, and a task with no pipeline -- which
# generation on its own does not.
try:
    import emit_pipeline_resources as _epr  # noqa: E402

    _epr_expected = _epr.render(model)
    _epr_committed = (ROOT / "resources" / "vault_pipeline.yml").read_text(encoding="utf-8")
    check("the committed pipeline resources match what the model generates",
          _epr_expected == _epr_committed,
          "run tools/emit_pipeline_resources.py and review the diff -- the declared domains "
          "and the pipelines that load them have diverged")

    _epr_domains = _epr.raw_domains(model)
    _epr_pipelines = yaml.safe_load(
        (ROOT / "resources" / "vault_pipeline.yml").read_text(encoding="utf-8"))
    _epr_declared = set((_epr_pipelines.get("resources") or {}).get("pipelines") or {})
    _epr_want = {f"raw_vault_{d}" for d in _epr_domains} | {"business_vault"}
    check("one raw pipeline per declared domain, and nothing else",
          _epr_declared == _epr_want,
          f"declared {sorted(_epr_declared)} against {sorted(_epr_want)} -- a domain with no "
          f"pipeline never loads, and a pipeline with no domain declares an empty graph and "
          f"reports success")

    # A FLOOR. A model that somehow declared no domain would make every set comparison here
    # compare nothing and pass, which is the vacuity this repo keeps finding.
    check("the domain split is not vacuous",
          len(_epr_domains) >= 2,
          f"{_epr_domains} -- one domain is not a split, and zero makes every check in this "
          f"block pass by comparing empty sets")

    _epr_job = yaml.safe_load((ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8"))
    _epr_tasks = {t["task_key"]: [d["task_key"] for d in (t.get("depends_on") or [])]
                  for t in _epr_job["resources"]["jobs"]["vault_load"]["tasks"]}
    _epr_raw_tasks = {k for k in _epr_tasks if k.startswith("raw_vault_")}
    check("the job runs exactly one task per raw pipeline",
          _epr_raw_tasks == {f"raw_vault_{d}" for d in _epr_domains},
          f"job has {sorted(_epr_raw_tasks)} against pipelines for {sorted(_epr_domains)} -- "
          f"a pipeline nothing runs is deployed and never triggered, which looks exactly "
          f"like a domain with no data")

    # REFERENCE FIRST. Stated as a requirement, so asserted rather than left to the order
    # someone happened to write the tasks in.
    check("every other domain waits on reference",
          all("raw_vault_reference" in _epr_tasks.get(f"raw_vault_{d}", [])
              for d in _epr_domains if d != "reference"),
          f"{ {d: _epr_tasks.get(f'raw_vault_{d}') for d in _epr_domains if d != 'reference'} } "
          f"-- legal entities and their hierarchies are the master data the other domains' "
          f"keys are reconciled against, so that domain loads first")

    # THE CROSS-DOMAIN EDGES, DERIVED. A binding that reads another domain's VAULT table
    # cannot run before the domain that writes it. Computed from the model rather than
    # listed, so activating such a binding cannot silently need a topology edit nobody
    # makes -- which is why DECLARED bindings count here, not just active ones.
    _epr_owner = {t: e.domain for e in model.entities for _s, t in e.tables()}
    _epr_needs: dict = {}
    for _e in model.entities:
        if _e.kind in naming.BUSINESS_KINDS or not _e.domain:
            continue
        for _s in _e.sources:
            _parts = (_s.bronze_table or "").split(".")
            if len(_parts) == 3 and _parts[1] == "raw_vault":
                _owner = _epr_owner.get(_parts[2])
                if _owner and _owner != _e.domain:
                    _epr_needs.setdefault(_e.domain, set()).add(_owner)
    _epr_missing_edges = {
        f"raw_vault_{d}": sorted(f"raw_vault_{o}" for o in owners
                                 if f"raw_vault_{o}" not in _epr_tasks.get(f"raw_vault_{d}", []))
        for d, owners in _epr_needs.items()}
    _epr_missing_edges = {k: v for k, v in _epr_missing_edges.items() if v}
    check("a domain reading another domain's vault table waits for it",
          not _epr_missing_edges,
          f"{_epr_missing_edges} -- the binding computes a parent key from a table the other "
          f"pipeline writes; running them in parallel reads it before it exists, and an "
          f"empty read loads perfectly")

    # AND THE FAN-IN IS COMPLETE. load_hubs and supersede_quarantine read the staging logs
    # of the WHOLE model, so a missing edge lets them start while a domain is still writing.
    _epr_fanin_missing = {
        t: sorted({f"raw_vault_{d}" for d in _epr_domains} - set(_epr_tasks.get(t, [])))
        for t in ("load_hubs", "supersede_quarantine")}
    _epr_fanin_missing = {k: v for k, v in _epr_fanin_missing.items() if v}
    check("everything downstream of the raw vault waits for EVERY domain",
          not _epr_fanin_missing,
          f"{_epr_fanin_missing} -- these read the whole model's staging logs, so a domain "
          f"still writing when they start is a silent partial load, not an error")
except ImportError as _epr_exc:                 # noqa: BLE001 -- reported, never swallowed
    bad("the pipeline-resource generator is importable", str(_epr_exc))

try:
    import emit_uc_constraints as _ucc  # noqa: E402

    _ucc_expected = _ucc.render(model)
    _ucc_committed = (ROOT / "governance" / "vault_constraints.sql").read_text(encoding="utf-8")
    check("the committed UC constraint DDL matches what the model generates",
          _ucc_expected == _ucc_committed,
          "run tools/emit_uc_constraints.py and review the diff -- the vault's declared "
          "relationships and the DDL that publishes them have diverged")

    # NON-VACUITY. An emitter returning nothing would match an empty committed file and
    # this gate would pass having asserted that two empty strings are equal.
    _ucc_pk = _ucc_committed.count("PRIMARY KEY (")
    _ucc_fk = _ucc_committed.count("FOREIGN KEY (")
    check("the constraint DDL actually declares the model's keys and edges",
          _ucc_pk >= 25 and _ucc_fk >= 30,
          f"{_ucc_pk} primary key(s) and {_ucc_fk} foreign key(s) -- the model has one PK "
          f"per emitted table and one FK per parent leg, so counts this low mean the "
          f"derivation broke and the file is asserting almost nothing")

    # AND THE EDGES MUST AGREE WITH THE DIAGRAM, because both read contract.foreign_keys
    # and a disagreement means one of them stopped.
    _ucc_dbml = (ROOT / "diagram" / "hfig_data_vault.dbml").read_text(encoding="utf-8")
    check("the constraint DDL declares exactly as many edges as the diagram draws",
          _ucc_fk == _ucc_dbml.count("\nRef:"),
          f"{_ucc_fk} foreign keys against {_ucc_dbml.count(chr(10) + 'Ref:')} Ref: lines -- "
          f"both derive from contract.foreign_keys, so they cannot legitimately differ")

    # NOT WIRED, AND THAT IS ASSERTED. A task running unverified DDL would redden every load.
    check("the constraint DDL is NOT yet a task in the vault job",
          "vault_constraints.sql" not in (ROOT / "resources" / "vault_job.yml").read_text(
              encoding="utf-8"),
          "it is wired in before anyone has confirmed an SDP streaming table accepts "
          "ADD CONSTRAINT and that every key column is NOT NULL -- answer both against a "
          "workspace first, then remove this check in the same change that wires it")
except ImportError as _exc:  # pragma: no cover
    check("tools/emit_uc_constraints.py imports", False, f"{_exc}")

# --------------------------------------------------------------------------- #
# BOTH SIDES OF A FOREIGN KEY NAME A PHYSICAL TABLE.
#
# A constraint is a database object, not documentation: Unity Catalog cannot declare a
# FOREIGN KEY that REFERENCES a view, and after the versioning migration every
# unversioned name IS a view. This file drifted exactly that way once, silently, because
# its ALTER TABLE side came from tables() and its REFERENCES side from
# contract.foreign_keys(), and nothing compared them.
try:
    _vc_text = (ROOT / "governance" / "vault_constraints.sql").read_text(encoding="utf-8")
except OSError as _vc_exc:  # noqa: BLE001 -- reported, never left to abort the suite
    bad("vault_constraints.sql is readable for the FK-physical-table gate", str(_vc_exc))
else:
    _vc_refs = re.findall(r"REFERENCES\s+`[^`]+`\.`[^`]+`\.`([^`]+)`", _vc_text)
    _vc_unversioned = sorted({r for r in _vc_refs if not re.search(r"_rev[1-9][0-9]*$", r)})
    check("every REFERENCES in vault_constraints.sql names a physical table",
          not _vc_unversioned,
          f"these REFERENCES name an unversioned object: {_vc_unversioned} -- after the "
          f"migration that name is a VIEW, and a FOREIGN KEY cannot reference a view, so "
          f"the statement would be rejected when the file is finally applied")
    check("and the check is not vacuous -- the file really does declare foreign keys",
          len(_vc_refs) > 0,
          "no REFERENCES clause found at all; either the file is empty or the pattern no "
          "longer matches what the emitter writes")

# --------------------------------------------------------------------------- #
# EVERY ACTIVE BINDING MUST DECLARE A FRESHNESS SLA, EVEN IF THAT DECLARATION IS "EXEMPT".
#
# Nothing else in this repo asks whether a feed still ARRIVES. Every gate asks whether what
# landed is correct, so a feed that quietly stopped passes all of them -- keys join,
# history stays append-only, reconciliation balances, and the vault serves yesterday's
# answer with nothing marking it as yesterday's.
#
# AN OMISSION AND AN EXEMPTION ARE DIFFERENT STATES and only one is a decision, so `0`
# (exempt) satisfies this and a missing key does not. GP_US_HIST is the real exemption:
# a closed-year extract delivered once, where an SLA would fail for ever and teach everyone
# to ignore the gate.
#
# SCOPED TO TARGETS THAT HAVE ACTUALLY DECIDED WHAT LOADS. An empty `active_sources` means
# every binding is active, and seven of the nine targets have one -- not because they load
# everything, but because nobody has configured them; only usnc_tds is deployed. Requiring
# an SLA for all 42 bindings on that basis would mean inventing 33 numbers for feeds nobody
# has seen, which is guessing dressed as rigour, and every one of them would be answered by
# copying the line above it.
#
# So the requirement follows the activation DECISION: a target that names its sources has
# decided what loads, and a staleness bound is part of that decision. A target that names
# none has decided nothing yet, and will land here the moment it does.
_fresh_decided = {
    _t: frozenset(x.strip() for x in
                  str(((_c.get("variables") or {}).get("active_sources") or "")).split(",")
                  if x.strip())
    for _t, _c in ((bundle or {}).get("targets", {}) or {}).items()
    if ((_c.get("variables") or {}).get("active_sources") or "").strip()
}
_fresh_missing = []
_fresh_active = 0
for _tname, _active in _fresh_decided.items():
    for _e in model.entities:
        for _s in _e.sources:
            if not spec.active_table_bindings(_e, _s, _active):
                continue
            _fresh_active += 1
            if _s.freshness_sla_hours is None:
                _fresh_missing.append(f"{_tname}: {_e.name}/{_s.name}")

# A FLOOR FIRST, because if the activation walk broke -- or every target's list were
# emptied -- _fresh_missing would be empty and this gate would pass by examining nothing.
check("the freshness sweep actually finds active bindings to check",
      len(_fresh_decided) >= 1 and _fresh_active >= 8,
      f"{_fresh_active} active binding(s) across {len(_fresh_decided)} target(s) that "
      f"declare active_sources -- the walk is broken, or every target's list was emptied, "
      f"and this gate would pass by examining nothing")
check("every active binding declares a freshness SLA, or declares itself exempt",
      not _fresh_missing,
      f"{sorted(set(_fresh_missing))[:6]} -- an undeclared SLA is not the same as an "
      f"exempt one, and only the second is a decision. Write 0 to exempt a feed that "
      f"legitimately never re-delivers")

# AND THE GATE MUST BE WIRED, because a check nobody runs asserts nothing. The same
# argument gold_quality makes about an undeployed dashboard, one layer along.
_fresh_job = (ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8")
check("the freshness gate is a task in the vault job",
      "freshness_check.py" in _fresh_job,
      "checks/freshness_check.py exists and nothing runs it -- a gate off the job graph "
      "is a file, not a control")

# --------------------------------------------------------------------------- #
# ...AND THE CONVERSE, FOR THE ONE GATE THAT IS DELIBERATELY NOT WIRED.
#
# checks/publish_stable_views.py was removed from resources/vault_job.yml on 29 September.
# Its scope is naming.GENERATABLE - naming.STAGED_KINDS, and when link, NHL and HAL joined
# STAGED_KINDS that set emptied. The gate fails closed on an empty scope BY DESIGN, so left
# wired it returned 1 on every single run and took assert_freshness and assert_mask_survival
# down behind it -- no run was ever green, and cutover_vault_version.py's --gated-by-run
# could never be satisfied.
#
# BY THE CHECK ABOVE'S OWN ARGUMENT, A GATE OFF THE JOB GRAPH IS A FILE AND NOT A CONTROL.
# That is acceptable here for exactly one reason: the gate has nothing to assert. So the
# withdrawal is asserted as a BICONDITIONAL against the live scope rather than recorded as
# a standing exemption -- the moment a generatable kind exists that is NOT staged, this
# gate owns that kind's stable view again and must be back in the job.
#
# WHY THIS IS NOT COVERED BY WHAT ALREADY EXISTS. tests/test_accelerator.py proves the
# empty-scope branch returns 1 and that the scope is genuinely empty today; it says nothing
# about whether the task is wired. Nothing else would notice a new un-staged kind: the
# symptom would surface one layer downstream as assert_mask_survival's F3 existence
# assertion failing on a view nobody published, which reads as a missing view rather than
# as a missing task.
_psv_path = ROOT / "checks" / "publish_stable_views.py"
_psv_scope = sorted(frozenset(naming.GENERATABLE) - naming.STAGED_KINDS)
# Swept over EVERY resources/*.yml, not just vault_job.yml: "wired" means some job somewhere
# runs it, and a check that only looked at one file would call it unwired while another job
# ran it.
#
# READ FROM THE PARSED python_file VALUES, NOT FROM A SUBSTRING SEARCH OF THE TEXT. The
# first draft of this check tested `"publish_stable_views.py" in y.read_text()` and went
# red on its own explanation: resources/vault_job.yml carries a comment block saying WHY
# the task was withdrawn, and that comment names the file. A gate that cannot tell a task
# from a comment about a task would force the withdrawal to be undocumented to stay green,
# which is the opposite of what this repo asks for everywhere else.
_psv_runners = []
for _psv_yml in sorted((ROOT / "resources").glob("*.yml")):
    try:
        _psv_doc = yaml.safe_load(_psv_yml.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001  -- a malformed resource file is another check's job
        continue
    for _psv_job in ((_psv_doc.get("resources") or {}).get("jobs") or {}).values():
        for _psv_task in (_psv_job or {}).get("tasks") or []:
            _psv_file = ((_psv_task.get("spark_python_task") or {}).get("python_file")
                         or "")
            if _psv_file.rsplit("/", 1)[-1] == "publish_stable_views.py":
                _psv_runners.append(f"{_psv_yml.name}:{_psv_task.get('task_key')}")
_psv_runners = sorted(set(_psv_runners))
check("publish_stable_views is a task in a job if and only if it has a scope to publish",
      bool(_psv_scope) == bool(_psv_runners),
      f"naming.GENERATABLE - naming.STAGED_KINDS = {_psv_scope}, wired in {_psv_runners}. "
      f"A non-empty scope with no runner means those kinds get no stable view and nothing "
      f"says so; an empty scope WITH a runner means the gate fails closed on every run and "
      f"blocks every task behind it. Re-add the task to resources/vault_job.yml, or take "
      f"the new kind into naming.STAGED_KINDS and give it a loader")

# AND THE FILE ITSELF MUST SURVIVE BEING UNWIRED. Nothing runs it now, so nothing else in
# this repo would fail if it were deleted as dead code -- and it is not dead: it is the
# implementation the check above says to re-wire, and its fail-closed branch is the proof
# that an emptied scope is a defect rather than a pass.
check("checks/publish_stable_views.py still exists although no job runs it",
      _psv_path.exists(),
      "the gate the check above tells a future reader to re-wire has been deleted. An "
      "unwired file looks like dead code; this one is the only implementation of the "
      "stable-view rule for a kind that is generatable but not staged")
check("and its empty-scope branch still fails closed, unwired though it is",
      _psv_path.exists()
      and 'finish("NOT_EVALUATED", 0, 0, 1)' in _psv_path.read_text(encoding="utf-8"),
      "the empty-scope branch no longer returns non-zero. Softening it to 0 was the "
      "obvious way to keep the task in the job, and it deletes the protection and the "
      "proof together -- the branch exists because a gate that asserts nothing must not "
      "report PASSED")

# --------------------------------------------------------------------------- #
# NO FILE MAY RETYPE A NAME SET naming.py ALREADY DECLARES.
#
# This is the defect family 5 September kept finding, in four separate places: a list of
# kinds or prefixes typed out by hand next to a module that derives it, drifting quietly
# apart. render_erd scanned PDFs with `...|hal|link)_` while the link prefix is `lnk_`;
# append_only_check hand-typed 10 vault prefixes where 15 exist and had already been
# patched twice after the fact; conformance_check hand-typed a third, different 8; and
# `("link", "nhl", "hal")` appeared in 22 places with nothing to read.
#
# naming.py had already written the rule down -- "Derive from this set rather than typing
# (sat, msat, esat, csat) out again -- that hand-written tuple is what let esat fall out
# in the first place" -- and 16 sites did it anyway. A rule in a comment is a rule until
# someone is in a hurry.
_NAME_SET_LITERALS = {
    '("link", "nhl", "hal")': "naming.LINK_KINDS",
    '("nhl", "link", "hal")': "naming.LINK_KINDS",
    '("sat", "msat", "esat", "csat")': "naming.SATELLITE_KINDS",
    '("sat", "msat", "esat")': "naming.RAW_SATELLITE_KINDS",
    '("hub_", "lnk_", "nhl_", "hal_", "sat_", "msat_", "esat_", "csat_")':
        "naming.CONFORMANCE_TABLE_PREFIXES",
}
# naming.py is where they are DECLARED, so the literals live there legitimately.
_NAME_SET_HOME = "src/accelerator/naming.py"
_retyped: list[str] = []
for _suffix in (".py", ".sql", ".yml"):
    for _f in repo_files(_suffix):
        _rel = _f.relative_to(ROOT).as_posix()
        if _rel in (_NAME_SET_HOME, "verify_repo.py"):
            continue          # the declaration, and this gate's own table of literals
        _text = _f.read_text(encoding="utf-8", errors="ignore")
        for _literal, _use in _NAME_SET_LITERALS.items():
            if _literal in _text:
                _retyped.append(f"{_rel}: {_literal} -- use {_use}")

# A FLOOR, so a broken walk cannot make this pass by examining nothing.
check("the name-set sweep actually reads this repo's files",
      len(repo_files(".py")) >= 40,
      "the file walk is excluding too much and this gate would pass vacuously")
check("no file retypes a name set naming.py already declares",
      not _retyped,
      f"{_retyped[:6]} -- a hand-typed copy drifts from the declaration silently, and "
      f"every instance found on 5 Sep had already drifted")

# --------------------------------------------------------------------------- #
# EVERY DEF-, BRZ-, LEG-, WDJ- AND PLT-nn CITED IN THE REPO MUST RESOLVE SOMEWHERE.
#
# These identifiers are the repo's only cross-file citation mechanism: a comment says
# "DEF-52" and expects a reader to find the reasoning. Nothing checked that the reasoning
# existed. Measured 5 September 2026, while splitting the record into two files: 18 ids
# cited across checks/, tests/ and governance/ resolved in NEITHER -- BRZ-5 through
# BRZ-11, DEF-13, DEF-15, DEF-16, DEF-17, DEF-19 through DEF-23, DEF-25, DEF-27, DEF-54.
# The split did not cause that; the same 18 were already dangling against the single file,
# and it took splitting to notice. A citation nobody can follow is worse than no citation:
# it reads as though the reasoning was written down.
#
# BOTH FILES COUNT. OPEN_ITEMS.md holds what is live and DECISION_LOG.md holds what is
# closed, so an id resolving in either is resolved -- and gating the pair together is what
# stops a future split, merge or archive from silently stranding the other half.
# LEG-nn joined on 5 Sep: the legal-entity questions put to business and legal, whose
# record is docs/legal_entity_questions.html. Same discipline as BRZ -- the page that
# gets sent IS the write-up, so it is added to the record rather than summarised into
# one.
#
# WDJ-nn joined on 6 Sep, and it joined in the same commit that INVENTED it. A new
# identifier family is a new way to leave a citation nobody can follow, and the first
# WDJ citation outside its own page already existed by then -- BRZ-17 points at WDJ-3 and
# WDJ-3 points back. Adding the family to the pattern here, rather than after the first
# dangling one, is the cheap half of the lesson the eighteen taught.
# PLT-nn joined on 6 Sep with the same reasoning, when five platform request documents
# became one and the queue gained ids for the first time. PLT-C1 and PLT-C2 are the two
# closed ones and carry a letter, so the pattern admits it.
_ID_RE = re.compile(r"\b(?:DEF|BRZ|LEG|WDJ)-\d+\b|\bPLT-C?\d+\b")
# A SET OF IDS, NOT A BLOB TO SEARCH. `"DEF-5" in record` is substring matching, and
# DEF-5 is a substring of DEF-50 through DEF-56 -- so a plain containment test reports a
# citation as resolved because a HIGHER-NUMBERED defect happens to be written up. Measured
# while mutation-proving this gate: DEF-5, cited in superpowers/reload-report.md and
# written up nowhere, passed silently. Same regex on both sides, compared as sets.
#
# THE RECORD IS WHEREVER THE WRITE-UP ACTUALLY LIVES, and defining it as a hand-picked
# list of files was wrong twice on the same day.
#
# First it was two markdown files, which reported all six of BRZ-5, 6, 7, 9, 10 and 11 as
# undocumented while each had a fuller write-up in `docs/bronze_layer_work_requests.html`
# -- a summary row and a full article, on the page Bronze actually receives. Then, with
# that added, DEF-5 was still reported dangling while being written up under a heading of
# its own in docs/superpowers/specs/2026-08-24-usnc-tds-retarget-design.md. The specs and
# plans are excluded from the CITING sweep because they are history rather than live code;
# excluding them from the RECORD as well was an unexamined consequence of that.
#
# So: the whole docs/superpowers tree, plus the Bronze page. The fix is always to
# recognise the record, never to copy write-ups into a chosen file -- a second copy is a
# second thing to keep in step, which is the failure this repo spent 5 September removing.
_RECORD_FILES = tuple(sorted((ROOT / "docs/superpowers").rglob("*.md"))) + (
    ROOT / "docs/bronze_layer_work_requests.html",
    ROOT / "docs/legal_entity_questions.html",
    ROOT / "docs/workday_journal_export_analysis.html",
    ROOT / "docs/platform_team_requests.html",
)
_RECORDED_IDS = set()
for _rf in _RECORD_FILES:
    _RECORDED_IDS |= {_m.group(0) for _m in
                      _ID_RE.finditer(_rf.read_text(encoding="utf-8"))}
# WHAT THIS DELIBERATELY DOES NOT COVER, so nobody reads it as stronger than it is. A
# record file is scanned as a citer too, so an id mentioned ONLY inside the record
# resolves itself and this check says nothing about it. That is correct -- the question
# being asked is "does code cite something a reader cannot follow", and an id living
# purely in the record has no dangling citation to follow. Proved by mutation: renaming
# BRZ-9 in the Bronze page fires nothing until a citation of it exists somewhere else,
# and then it fires immediately.
_cited_ids: dict[str, set[str]] = {}
for _suffix in (".py", ".md", ".html", ".yml", ".sql"):
    for _f in repo_files(_suffix):
        _rel = _f.relative_to(ROOT).as_posix()
        if _rel.startswith("docs/superpowers/"):
            continue          # the record itself, and the plans/specs that are its history
        for _m in _ID_RE.finditer(_f.read_text(encoding="utf-8", errors="ignore")):
            _cited_ids.setdefault(_m.group(0), set()).add(_rel)

# A FLOOR FIRST. If the walk or the regex broke, `_dangling` would be empty and this gate
# would pass by checking nothing -- the unfailable-gate shape this repo keeps finding.
check("the record itself is non-empty, so resolution is not vacuous",
      len(_RECORD_FILES) >= 5 and len(_RECORDED_IDS) >= 20,
      f"{len(_RECORD_FILES)} record file(s) holding {len(_RECORDED_IDS)} identifier(s) -- "
      f"if the record walk broke, every citation would 'resolve' against nothing")
check("the identifier sweep actually finds citations to check",
      len(_cited_ids) >= 20,
      f"found {len(_cited_ids)} distinct DEF-/BRZ- ids cited -- the walk or the pattern is "
      f"broken, and this gate would pass by examining nothing")

# KNOWN DANGLING, 5 Sep 2026. Pre-existing and NOT introduced by the split. Listed rather
# than waived wholesale so the set can only shrink: adding a new unresolvable citation
# fails the build, and writing up any of these removes a line from here.
# EMPTY, AND IT SHOULD STAY THAT WAY. All 18 were resolved on 5 September rather than
# waived: the six BRZ ids by recognising the file they were always written up in, and the
# twelve DEF ids by indexing them in DECISION_LOG against the site that explains each --
# a pointer per id, not a second copy of the reasoning.
#
# Leave this here rather than deleting it. A future defect may legitimately be cited
# before it is written up, and a named, reviewed waiver is a better answer than either
# blocking the commit or quietly widening the gate.
_DANGLING_KNOWN: set[str] = set()
_dangling = {i: sorted(v) for i, v in _cited_ids.items() if i not in _RECORDED_IDS}
_new_dangling = {i: v for i, v in _dangling.items() if i not in _DANGLING_KNOWN}
# THE MESSAGE NAMES THE RECORD BY READING IT, not by retyping it. It used to list two
# filenames in prose; a third record file was added on 6 Sep and the message would have
# gone on naming two -- telling a reader to look in the wrong places while the gate
# itself looked in the right ones. That is the drift this gate exists to catch, in the
# gate's own failure text.
_record_names = ", ".join(sorted(f.relative_to(ROOT).as_posix() for f in _RECORD_FILES
                                 if not f.as_posix().endswith(".md")))
check("every DEF-/BRZ-/LEG-/WDJ-/PLT- identifier cited in the repo resolves in the record",
      not _new_dangling,
      f"{sorted(_new_dangling)} cited in {sorted({f for v in _new_dangling.values() for f in v})} "
      f"but written up nowhere under docs/superpowers/, nor in {_record_names} -- a citation "
      f"nobody can follow reads as though the reasoning exists")

# AND THE WAIVER LIST MAY NOT ROT. An id that gets written up, or whose last citation is
# deleted, must leave this list -- otherwise the set only ever grows and stops meaning
# anything.
_stale_waivers = sorted(i for i in _DANGLING_KNOWN
                        if i not in _cited_ids or i in _RECORDED_IDS)
check("no waived identifier has been written up or dropped without leaving the list",
      not _stale_waivers,
      f"{_stale_waivers} -- these are waived as dangling but are now either documented or "
      f"no longer cited, so the waiver is describing a state that has passed")

# EVERY AME RULE MUST BE TRACEABLE FROM CODE TO THE SHEET THAT AUTHORISED IT.
#
# The thirteen rules come from a customer's workbook. A rule quietly dropped during
# implementation produces an invoice that is wrong in a way no test notices, because
# the test suite only knows about rules somebody remembered to implement.
#
# TWO SETS, NOT ONE AGGREGATE -- REVIEW FINDING, 24 Sep 2026. The first version of this
# gate concatenated all four files (three code files plus the metadata YAML) and asked
# whether an id appeared ANYWHERE in the combined text. That proves less than its own
# comment claimed: AME007 could be deleted from invoice_rules.py entirely and the gate
# stayed green, because AME007 is ALSO named in csat_invoice_line_gie.yml -- a
# documentation comment, not executing code. That reproduces exactly the failure this
# gate exists to catch. Mutation-proved below.
#
# AME003 and AME009 are DECLARATIVE BY DESIGN, not omissions: AME003 repeats the
# invoice gross via a join from sat_invoice_header (nothing to compute), and AME009's
# format is an open question -- the workbook's rule, its own example and the sample
# data disagree three ways -- so it is a passthrough with no derivation function
# either, same as AME001/AME010. Both are named in the metadata only, and that is
# where this gate expects to find them. If either later gains a real implementation,
# move its id from _AME_DECLARATIVE to _AME_IN_CODE in the SAME commit as the code
# that earns it -- that edit is what keeps the two sets honest.
#
# AND AME001 IS NEITHER IMPLEMENTED NOR DECLARATIVE -- IT IS UNDECLARED, WHICH IS A
# THIRD THING. Until 28 Sep it sat in _AME_IN_CODE and was satisfied by CHECK 1 finding
# the string "AME001" in checks/invoice_export.py, where it appeared in two docstrings
# claiming the rule was applied as a direct map "straight off the GIE satellite's
# payload". That sentence was FALSE: the GIE payload declares no `invoice_number` column
# and neither does anything else in this repo, so the rule's traceability rested entirely
# on a substring match against a claim nothing could test. That is precisely the failure
# mode the two-set split above was introduced to close, reappearing one rule along.
#
# AME001 is now traced to the REFUSAL that names it -- invoice_export.UNDECLARED_SOURCES
# and required_field_refusal() -- and the checks below adjudicate that MECHANICALLY: the
# module is parsed, its two literals are read, and the claim "no source column is
# declared for this field" is measured against metadata/entities and control_standard
# rather than believed. Declaring an `invoice_number` column without removing the entry
# goes red, and removing the entry without declaring one goes red too, so the marker
# cannot outlive the problem the way a comment can. Move AME001 to _AME_IN_CODE in the
# SAME commit as the column that earns it.
_AME_IDS = {f"AME{n:03d}" for n in range(1, 14)}
_AME_DECLARATIVE = {"AME003", "AME009"}
_AME_UNDECLARED = {"AME001"}
_AME_IN_CODE = _AME_IDS - _AME_DECLARATIVE - _AME_UNDECLARED
_AME_ID_RE = re.compile(r"AME\d{3}")

_ame_code_files = ("src/accelerator/invoice_rules.py", "checks/invoice_issue.py",
                    "checks/invoice_export.py")
_ame_yaml_file = "metadata/entities/csat_invoice_line_gie.yml"
# CAUGHT, NOT RAISED -- the same house rule as the source-contract gate above
# (verify_repo.py:2599-2604): an exception escaping a module-level read aborts this
# whole script and turns every later check silently ABSENT, which is worse than one
# red check naming the missing file. A renamed or deleted gated file must fail THIS
# check, not the entire suite.
try:
    _ame_code_text = "".join((ROOT / f).read_text(encoding="utf-8")
                              for f in _ame_code_files)
    _ame_yaml_text = (ROOT / _ame_yaml_file).read_text(encoding="utf-8")
except Exception as _exc:  # noqa: BLE001
    _ame_code_text = _ame_yaml_text = ""
    _ame_read_error = f"{type(_exc).__name__}: {_exc}"
else:
    _ame_read_error = None

check("the AME traceability gate can actually read its gated files",
      _ame_read_error is None,
      f"{_ame_read_error} -- every id check below reads as empty text and would "
      f"report every id missing, rather than the suite aborting silently")

# CHECK 1 -- IN CODE MEANS IN CODE. Each of the eleven implemented rules must appear
# in the CODE text specifically -- invoice_rules.py, invoice_issue.py or invoice_export.py. The
# YAML does not count toward this set; that is the one-line fix for the finding above.
_ame_code_missing = sorted(i for i in _AME_IN_CODE if i not in _ame_code_text)
check("every rule with a real implementation names its AME id in the code that "
      "implements it (invoice_rules.py, invoice_issue.py or invoice_export.py) -- the metadata "
      "YAML does not count toward this",
      not _ame_code_missing,
      f"{_ame_code_missing} -- a rule dropped from the implementing code is invisible "
      f"if only the YAML still mentions it, which is the gap the old aggregated "
      f"check missed")

# CHECK 2 -- THE TWO DECLARATIVE RULES STILL HAVE THEIR ONE TRACE. They have no
# implementing function BY DESIGN (see above), so the metadata comment is not a
# fallback -- it is the only place either id can legitimately be found.
_ame_declarative_missing = sorted(i for i in _AME_DECLARATIVE if i not in _ame_yaml_text)
check("every declarative-only rule (AME003, AME009) is still named in "
      "csat_invoice_line_gie.yml, its only legitimate trace",
      not _ame_declarative_missing,
      f"{_ame_declarative_missing} -- these two rules have no implementing function "
      f"by design, so losing the metadata comment loses their only trace entirely")

# CHECK 3 -- COMPLETENESS, AGAINST WHAT IS ACTUALLY WRITTEN, NOT THE STATIC LISTS.
# _AME_IN_CODE and _AME_DECLARATIVE above are hand-maintained and, by construction,
# already partition _AME_IDS -- so comparing them to EACH OTHER would be a tautology,
# never able to fail. This instead scans the real file text with the same regex for
# every AME-shaped id and asserts the ids actually found across code and YAML equal
# the canonical thirteen exactly: nothing missing from either side, and nothing extra
# (a stray AME014 typoed into any of the four files fails this, on either side).
_ame_ids_found_in_code = set(_AME_ID_RE.findall(_ame_code_text))
_ame_ids_found_in_yaml = set(_AME_ID_RE.findall(_ame_yaml_text))
_ame_ids_found = _ame_ids_found_in_code | _ame_ids_found_in_yaml
check("the ids actually cited across the code and the metadata YAML are exactly "
      "AME001-AME013 -- no id absent from both, none invented",
      _ame_ids_found == _AME_IDS,
      f"found {sorted(_ame_ids_found)} vs the canonical {sorted(_AME_IDS)} -- missing: "
      f"{sorted(_AME_IDS - _ame_ids_found)}, unexpected: "
      f"{sorted(_ame_ids_found - _AME_IDS)}")

# CHECK 4 -- THE UNDECLARED RULE IS TRACED TO A STRUCTURE, NOT TO A SENTENCE.
#
# Everything below reads checks/invoice_export.py with ast and measures what it finds
# against the entity model and the control standard. A docstring cannot satisfy any of
# it. PARSED, NOT IMPORTED: importing that module pulls in openpyxl and a sibling check
# file, and a verifier that cannot run because a gate's dependency moved is a verifier
# nobody runs.


def _ame_literal(tree, name: str):
    """The module-level literal assigned to `name`, or None if there is not exactly one.

    Handles the annotated form (`X: dict[...] = {...}`) as well as the plain one, because
    which of the two a constant is written in is a style choice and this gate must not
    silently report a constant absent because somebody added a type to it.
    """
    found = []
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) \
                and node.target.id == name and node.value is not None:
            found.append(node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    found.append(node.value)
    if len(found) != 1:
        return None
    try:
        return ast.literal_eval(found[0])
    except Exception:  # noqa: BLE001
        return None


try:
    _ame_ie_tree = ast.parse((ROOT / "checks" / "invoice_export.py")
                             .read_text(encoding="utf-8"))
    _ame_required = _ame_literal(_ame_ie_tree, "REQUIRED_FIELDS")
    _ame_undeclared = _ame_literal(_ame_ie_tree, "UNDECLARED_SOURCES")
    _ame_parse_error = None
except Exception as _exc:  # noqa: BLE001
    _ame_required = _ame_undeclared = None
    _ame_parse_error = f"{type(_exc).__name__}: {_exc}"

check("invoice_export declares REQUIRED_FIELDS and UNDECLARED_SOURCES as literals this "
      "gate can read without importing the module",
      _ame_parse_error is None and isinstance(_ame_required, tuple)
      and isinstance(_ame_undeclared, dict) and bool(_ame_required),
      f"parse={_ame_parse_error} required={type(_ame_required).__name__} "
      f"undeclared={type(_ame_undeclared).__name__} -- every check below compares "
      f"nothing if either is absent, so this one fails first rather than passing "
      f"vacuously on two Nones")

_ame_req = _ame_required if isinstance(_ame_required, tuple) else ()
_ame_und = _ame_undeclared if isinstance(_ame_undeclared, dict) else {}

# THE IDS INSIDE THE STRUCTURE, NOT ANYWHERE IN THE FILE.
_ame_und_ids = {v[0] for v in _ame_und.values()
                if isinstance(v, (tuple, list)) and v and isinstance(v[0], str)}
check("AME001's whole trace is the UNDECLARED_SOURCES entry that refuses it -- the ids "
      "that structure names are exactly the undeclared set",
      bool(_ame_und) and _ame_und_ids == _AME_UNDECLARED,
      f"UNDECLARED_SOURCES names {sorted(_ame_und_ids)}, the undeclared set is "
      f"{sorted(_AME_UNDECLARED)} -- an id here that is also in _AME_IN_CODE would be "
      f"claimed applied and refused at the same time, and one in neither has no trace "
      f"at all")

check("and every undeclared field is one REQUIRED_FIELDS actually gates on -- a marker "
      "on a field nothing gates on refuses nothing",
      bool(_ame_und) and set(_ame_und) <= set(_ame_req),
      f"{sorted(set(_ame_und) - set(_ame_req))} carry an UNDECLARED_SOURCES entry and "
      f"are not in REQUIRED_FIELDS {list(_ame_req)}")

# THE CLAIM ITSELF, MEASURED. Every column name this repo declares anywhere the export
# could reach: the entity model's keys and payloads, and the control standard's tables
# (which is where control.ctl_invoice_issuance's seven columns live).
_ame_declared_cols: set[str] = set()
for _ent in model.entities:
    _ame_declared_cols |= set(getattr(_ent, "business_keys", ()) or ())
    _ame_declared_cols |= set(getattr(_ent, "payload", ()) or ())
    _ame_declared_cols |= set(getattr(_ent, "transaction_key", ()) or ())
for _ame_tables in list(_cs.LAYER_TABLES.values()) + [_cs.CORE]:
    for _t, _cols in _ame_tables.items():
        _ame_declared_cols |= set(_cols)
# The two the ledger freezes and the export reads back are declared on ctl_invoice_issuance
# above, so they need no special case here.

_ame_still_undeclared = sorted(f for f in _ame_und if f not in _ame_declared_cols)
check("AME001's source column really is undeclared -- no entity and no control table "
      "declares a column of that name, measured rather than asserted in a docstring",
      bool(_ame_und) and _ame_still_undeclared == sorted(_ame_und),
      f"{sorted(set(_ame_und) - set(_ame_still_undeclared))} now HAVE a declared column, "
      f"so the refusal in invoice_export is describing a state that has passed: move the "
      f"id from _AME_UNDECLARED to _AME_IN_CODE, map the field, and drop the entry, in "
      f"the commit that declares the column")

# THE OTHER DIRECTION, WHICH IS THE ONE THAT CATCHES THE NEXT ONE. A required field that
# quietly loses its column would make the export refuse with no rule id, no reason and
# no owner recorded -- undeclared_required_fields() measures the projection and would
# name it, but nothing would say WHY. This is where that gets noticed.
_ame_unexplained = sorted(f for f in _ame_req
                          if f not in _ame_declared_cols and f not in _ame_und)
check("and no OTHER required gold field is undeclared without an entry saying why -- "
      "the marker cannot hide a second one behind it",
      not _ame_unexplained,
      f"{_ame_unexplained} are gated by REQUIRED_FIELDS, are declared by no entity and "
      f"no control table, and carry no UNDECLARED_SOURCES entry -- so the export would "
      f"refuse every run naming a field with no rule id and no owner attached")


# --------------------------------------------------------------------------- #
# EVERY DECLARED TABLE HAS EXACTLY ONE STABLE VIEW NAME, AND NO TWO ENTITIES SHARE ONE.
# A collision would have the second entity's view silently replace the first's, and the
# loser would keep loading a physical table that nothing reads -- green, and invisible.
_stable_names = [s for e in model.entities for _src, s in e.stable_tables()]
check("no two entities claim the same stable view name",
      len(_stable_names) == len(set(_stable_names)),
      f"duplicated: {sorted({n for n in _stable_names if _stable_names.count(n) > 1})}")
check("every physical table name round-trips to its stable name",
      all(naming.stable(p) == s
          for e in model.entities
          for (_s1, p), (_s2, s) in zip(e.tables(), e.stable_tables())),
      "physical() and stable() must stay exact inverses or a cutover repoints the "
      "wrong view")

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

try:
    _wrt_types = json.loads(_wrt_committed)["types"] if _wrt_committed else []
except Exception:  # noqa: BLE001 -- a malformed artefact must not abort the suite;
    # falling back to [] lets the "inventory is not empty" check below go red and
    # name the problem, instead of a bare artefact (invalid JSON, or valid JSON
    # missing "types") taking down the whole verify_repo.py run before it can print
    # a summary -- exactly the drift the byte-gate above exists to catch.
    _wrt_types = []
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

# --- Workday interim landing schema (subsystem E, Task 5) ----------------------------
_wl_path = ROOT / "governance" / "workday_landing.sql"
_wl_committed = _wl_path.read_text(encoding="utf-8") if _wl_path.exists() else ""
sys.path.insert(0, str(ROOT / "tools"))
try:
    import emit_workday_landing as _wl_mod
    _wl_rendered = _wl_mod.render()
except Exception as _wl_exc:  # noqa: BLE001 -- must not abort the suite
    _wl_mod, _wl_rendered = None, f"EMITTER RAISED: {_wl_exc!r}"

check("workday_landing.sql is byte-identical to what its emitter renders",
      bool(_wl_committed) and _wl_committed == _wl_rendered,
      "run tools/emit_workday_landing.py and commit the result")

check("every workday_landing statement is CREATE ... IF NOT EXISTS -- never a replace",
      bool(_wl_committed)
      and "CREATE OR REPLACE" not in _wl_committed
      and "DROP " not in _wl_committed
      and _wl_mod is not None
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

# --- gold reference view (subsystem E, Task 6) ---------------------------------------
_grv_path = ROOT / "governance" / "gold" / "gold_reference_views.sql"
_grv = _grv_path.read_text(encoding="utf-8") if _grv_path.exists() else ""
sys.path.insert(0, str(ROOT / "tools"))
try:
    import emit_gold_reference_views as _grv_mod
    _grv_rendered = _grv_mod.render()
except Exception as _grv_exc:  # noqa: BLE001 -- must not abort the suite
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

# THE FOUR CHECKS ABOVE ARE SUBSTRING ASSERTIONS OVER A STRING LITERAL, AND THAT IS A
# BLIND SPOT. They prove the committed file byte-matches what render() currently emits,
# and that certain phrases appear in it -- but a substring match cannot distinguish a
# RIGHT identifier from a WRONG one that is merely a valid-looking token. That is exactly
# how the shipped defect survived ~25 checks across six tasks: the view named
# `effective_from` -- the LANDING column name, per esat_wd_reference.yml's
# `applied_dts_column: effective_from` -- in the SELECT list, the subquery's column list
# and the ORDER BY inside ROW_NUMBER(), while the satellite's actual column is
# `applied_dts`; every existing check passed because none of them asked "does this
# identifier exist on the entity it claims to read from". This check asks that question
# against the MODEL, not against another copy of the string.
#
# GUARDED SO IT CANNOT ABORT THE SUITE: everything that can raise (model load, regex
# parsing of a SQL string the emitter may have changed shape) runs inside this try/except,
# and any failure there is recorded as a FAILED check, never an uncaught exception -- an
# exception here must not take down every check below it, which would be worse than a
# single red line.
try:
    _grvm_model = spec.load_model(ROOT / "metadata" / "entities")
    _grvm_hub = _grvm_model.get("wd_reference")
    _grvm_esat = _grvm_model.get("wd_reference_effectivity")
    _grvm_hub_table = dict(_grvm_hub.stable_tables())[None]
    _grvm_esat_tables = {name for _, name in _grvm_esat.stable_tables()}

    _grvm_hub_cols = (
        set(_grvm_hub.business_keys) | {_grvm_hub.hk_column, naming.bk(_grvm_hub.name)}
        | set(naming.SYSTEM_COLUMNS)
    )
    _grvm_parent = _grvm_esat.parents[0]
    _grvm_esat_cols = (
        set(_grvm_esat.payload) | {naming.hk(_grvm_parent), naming.COL["hashdiff"]}
        | set(naming.SYSTEM_COLUMNS)
    )

    def _grvm_strip_alias(item: str) -> str:
        return re.split(r"\s+AS\s+", item.strip(), flags=re.IGNORECASE)[0].strip()

    _grvm_findings: list[str] = []

    def _grvm_check(item: str, allowed: set, where: str) -> None:
        item = item.strip()
        if not item:
            return
        alias, _, col = item.partition(".")
        col = col if col else alias
        if not re.match(r"^[A-Za-z_]\w*$", col):
            return  # not a plain identifier (e.g. a bare expression) -- not this check's job
        if col not in allowed:
            _grvm_findings.append(
                f"{where}: {item!r} names column {col!r}, which is not among the "
                f"declared columns for that entity ({sorted(allowed)})")

    # (1) every raw_vault table this view reads from must be a stable table name of the
    # hub or the esat -- not merely present in the committed text.
    _grvm_table_refs = set(re.findall(r"`\$\{catalog\}`\.`raw_vault`\.`(\w+)`", _grv))
    _grvm_tables_ok = bool(_grvm_table_refs) and _grvm_table_refs <= (
        {_grvm_hub_table} | _grvm_esat_tables)

    # (2) the outer SELECT list (h.* / e.* qualified)
    _grvm_outer_m = re.search(
        r"reference_data`\.`wd_reference` AS\s*SELECT\s*(.*?)\s*FROM", _grv, re.S)
    _grvm_outer_items = (
        [_grvm_strip_alias(i) for i in _grvm_outer_m.group(1).split(",")]
        if _grvm_outer_m else None)
    if _grvm_outer_items:
        for _item in _grvm_outer_items:
            _grvm_check(_item, _grvm_hub_cols if _item.startswith("h.") else _grvm_esat_cols,
                        "outer SELECT list")

    # (3) the subquery's own column list, and (4) its ROW_NUMBER() PARTITION BY / ORDER BY
    # -- both read bare (unqualified) columns straight off the esat table.
    _grvm_inner_m = re.search(r"JOIN \(\s*SELECT\s*(.*?)\s*FROM", _grv, re.S)
    _grvm_rownum_m = re.search(
        r"ROW_NUMBER\(\)\s*OVER\s*\(\s*PARTITION BY\s*(.*?)\s*ORDER BY\s*(.*?)\)\s*AS\s*\w+",
        _grv, re.S)
    _grvm_inner_plain = None
    if _grvm_inner_m:
        _before_rn = re.split(r"ROW_NUMBER\(\)", _grvm_inner_m.group(1))[0]
        _grvm_inner_plain = [c.strip().rstrip(",") for c in _before_rn.split(",")
                             if c.strip().rstrip(",")]
        for _item in _grvm_inner_plain:
            _grvm_check(_item, _grvm_esat_cols, "subquery SELECT list")
    if _grvm_rownum_m:
        for _item in _grvm_rownum_m.group(1).split(","):
            _grvm_check(_item, _grvm_esat_cols, "ROW_NUMBER() PARTITION BY")
        for _item in _grvm_rownum_m.group(2).split(","):
            _item = re.sub(r"\s+(DESC|ASC)$", "", _item.strip(), flags=re.IGNORECASE)
            _grvm_check(_item, _grvm_esat_cols, "ROW_NUMBER() ORDER BY")

    _grvm_parsed_ok = (
        _grvm_outer_items is not None and _grvm_inner_plain is not None
        and _grvm_rownum_m is not None
    )
    _grvm_ident_ok = bool(_grv) and _grvm_tables_ok and _grvm_parsed_ok and not _grvm_findings
    _grvm_ident_detail = "; ".join(_grvm_findings) or (
        "" if _grvm_ident_ok else
        f"tables referenced {sorted(_grvm_table_refs)} vs allowed "
        f"{sorted({_grvm_hub_table} | _grvm_esat_tables)}, or the view's SQL could not be "
        f"parsed into its SELECT lists / ROW_NUMBER clause as expected")
except Exception as _grvm_exc:  # noqa: BLE001 -- must not abort the suite
    _grvm_ident_ok = False
    _grvm_ident_detail = f"identifier check raised {_grvm_exc!r} -- treat as a finding, not a crash"

check("every table and column the gold reference view names actually exists on the "
      "hub/esat entities the model declares",
      _grvm_ident_ok, _grvm_ident_detail)

# --- gold schema topology (subsystem A, Task 2) --------------------------------------
sys.path.insert(0, str(ROOT / "tools"))
_gl_import_error: Exception | None = None
try:
    from accelerator import gold_layout as _gl_mod  # noqa: E402
except Exception as _gl_exc:  # noqa: BLE001 -- must not abort the suite
    _gl_import_error = _gl_exc
    _gl_mod = _SimpleNamespace(SCHEMAS={}, READABLE=frozenset(), EXPORT_SCHEMA="")
    print(f"  (gold_layout unavailable: {_gl_exc!r})")

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

# --- gold schema topology (subsystem A, Task 3) ---------------------------------------
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

# THE SPLIT COUNT, EXERCISED THROUGH THE SAME SPLITTER THE APPLIER USES, AND AGAINST A
# FRESH RENDER OF THE CURRENT SOURCE -- not the possibly-stale committed file. A purpose
# string in gold_layout.SCHEMAS that happens to contain a semicolon (measured: the
# original "the project schema; Workday finance export tables" for wd_fin_export) gets
# cut in half by apply_governance.statements()'s naive split-on-";", and the real CREATE
# SCHEMA for that entry is destroyed -- while the run still reports N statements and
# looks successful. Reading _gs (the committed file) here would only catch this AFTER
# someone remembers to regenerate; reading _gs_rendered (the emitter's output from
# gold_layout.SCHEMAS as it stands RIGHT NOW) catches it the moment source and artefact
# diverge -- the actual failure sequence: edit the module, forget to regenerate.
# A check with its OWN splitter would prove nothing about the splitter the applier
# actually runs, so this reuses render()/statements() verbatim.
try:
    _gs_split_ok_source = _gs_emit is not None and isinstance(_gs_rendered, str)
    _gs_split_stmts = (
        _ag.statements(_ag.render(_gs_rendered, {"gold_catalog": "PROBE"}))
        if _gs_split_ok_source else []
    )
    _gs_split_ok = (
        _gs_split_ok_source
        and len(_gs_split_stmts) == len(_gl_mod.SCHEMAS)
        and all(s.startswith("CREATE SCHEMA IF NOT EXISTS") for s in _gs_split_stmts)
    )
    _gs_split_detail = (
        f"gold_layout.SCHEMAS declares {len(_gl_mod.SCHEMAS)} schema(s) RIGHT NOW; "
        f"rendering the emitter fresh from that source and splitting with the same "
        f"splitter the applier uses produced {len(_gs_split_stmts)} statement(s): "
        f"{[s.splitlines()[0][:60] for s in _gs_split_stmts]} -- a purpose string "
        f"containing a semicolon (or any other stray punctuation) cuts a statement in "
        f"half and the run still reports success"
    )
except Exception as _gs_split_exc:  # noqa: BLE001 -- must not abort the suite
    _gs_split_ok = False
    _gs_split_detail = f"split check raised {_gs_split_exc!r} -- treat as a finding, not a crash"

check("the gold DDL, rendered fresh from gold_layout.SCHEMAS as it stands now, splits "
      "into exactly one CREATE SCHEMA statement per declared schema",
      _gs_split_ok, _gs_split_detail)

# --------------------------------------------------------------------------- #
# Task 4: the gold_build job -- manually invoked, creates schemas only, and is not
# a task tacked onto vault_load's already-~30-task job.
_gj_path = ROOT / "resources" / "gold_job.yml"
try:
    _gj = yaml.safe_load(_gj_path.read_text(encoding="utf-8")) or {}
except Exception as _gj_exc:  # noqa: BLE001
    _gj = {}
_gj_job = ((_gj.get("resources") or {}).get("jobs") or {}).get("gold_build") or {}
_gj_tasks = _gj_job.get("tasks") or []
try:
    _vl = yaml.safe_load((ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8")) or {}
except Exception as _vl_exc:  # noqa: BLE001
    _vl = {}
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
      "and today it still runs in vault_load, behind the issuance task it reads")

# --- the TDS visibility grant (subsystem A, Task 5) -----------------------------------
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
      bool(_gg)
      and all("ON SCHEMA" not in s.upper() or "USE SCHEMA" in s.upper() for s in _gg)
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

check("gold_layout imported cleanly -- the SimpleNamespace(SCHEMAS={}) fallback above "
      "did not silently take over",
      _gl_import_error is None,
      f"import raised {_gl_import_error!r} -- every gold_layout-derived check above ran "
      f"against an empty stub instead of the real module, and would have passed "
      f"vacuously")

# Every check over gold_access_grants above (READABLE-only, no SELECT, USE CATALOG/
# SCHEMA) calls that function DIRECTLY. None of them exercises main()'s own wiring --
# `stmts = stmts + gold_grants` in checks/apply_governance.py -- so deleting that one
# line would leave gold_access_grants() itself correct, both suites green, and the TDS
# gold grant silently missing from every rendered run. This runs apply_governance.py as
# a real subprocess with --dry-run --emit-data-grants (the existing dry-run check above
# omits --emit-data-grants, so it never enters this branch) and asserts the rendered
# output actually contains the USE SCHEMA grant for the gold catalog.
try:
    _dry_gg = _sp.run(
        [sys.executable, str(ROOT / "checks" / "apply_governance.py"),
         "--catalog", "02_usnc_silver_edm_dev",
         "--business-vault-schema", "business_vault",
         "--bronze-catalog", "01_usnc_bronze_dev",
         "--gold-catalog", "03_usnc_gold_edm_dev",
         "--privileged-group", "verify_repo_dry_run_group",
         "--emit-data-grants",
         "--dry-run"],
        capture_output=True, text=True,
    )
    _gg_lines = _dry_gg.stdout.splitlines()
    _gg_ok = (
        _dry_gg.returncode == 0
        and any("GRANT USE SCHEMA ON SCHEMA" in l and "03_usnc_gold_edm_dev" in l
                for l in _gg_lines)
    )
    _gg_detail = _dry_gg.stdout[-500:] + _dry_gg.stderr[-500:]
except Exception as _gg_check_exc:  # noqa: BLE001 -- a check must fail red, never raise
    _gg_ok = False
    _gg_detail = f"subprocess invocation raised {_gg_check_exc!r}"

check("apply_governance.py's main() wiring (not just gold_access_grants() called "
      "directly) actually renders the gold USE SCHEMA grant under --emit-data-grants",
      _gg_ok, _gg_detail)

# --------------------------------------------------------------------------- #
print("\n" + "=" * 70)
print(f"{CHECKS} verification checks run")
if FINDINGS:
    print(f"\nVERIFICATION FAILED -- {len(FINDINGS)} finding(s):\n")
    for f in FINDINGS:
        print(f"  * {f}")
    sys.exit(1)
print("VERIFICATION PASSED")
