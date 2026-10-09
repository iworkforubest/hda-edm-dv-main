"""
PREFLIGHT: prove you are about to deploy to the lake you think you are.

There are 8 workspaces with near-identical names across 4 regions and 2 environments.
`db-uks-datalakehouse` and `db-uks-datalakehouse-tds` differ by four characters, and
the URLs are opaque numbers. The realistic failure is not a bug in the model -- it is
deploying TDS artefacts into a production lake, or loading UK data into the US lake,
because a CLI profile was left pointing somewhere else.

This check resolves the host the bundle declares for a target, resolves the host the
CLI is actually authenticated to, and refuses to continue unless they match.

    python checks/preflight_target.py --target usnc_tds
    python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds

SECOND REFUSAL, ADDED 28 Sep: A TARGET THAT DECLARES NO run_as LOADS AS A HUMAN.
Four targets -- weu_tds, uks_tds, aue_tds, dev -- declare no `run_as`, so their loads
run as whoever ran the deploy. Mask functions evaluate with the run-as identity's rights
during a refresh, and since the money masks moved to `scope_unmask_currency_values` the
humans who deploy are no longer privileged under them: a materialized view over a masked
column then writes NULLs into the insert-only silver vault, permanently.

That warning lived only in DEPLOY.md 6b, which a deployer reads two phases and ~300
lines AFTER deploying weu_tds at Phase 4. A warning positioned after the action it warns
about is not a control. This file is invoked at Phase 4, immediately before the deploy,
so the refusal belongs here where it can still stop something.

    python checks/preflight_target.py --target weu_tds --profile hfig-weu-tds
    ... PREFLIGHT FAILED: weu_tds declares no run_as ...

    # only once you have confirmed that identity IS in the group, by explicit request:
    python checks/preflight_target.py --target weu_tds --profile hfig-weu-tds \
        --unmask-identity adrian.turcu@vertage.com

WHY AN ACKNOWLEDGEMENT RATHER THAN A MEMBERSHIP TEST. `is_account_group_member` needs a
live workspace, and it returns false identically for a non-member and for a group that
does not exist -- so a machine test here could not distinguish "you are not in it" from
"the group is misspelt", and would fail closed on both while teaching nothing. What this
CAN do is refuse by default and make the operator name the identity they have confirmed,
which lands in the run output as a dated, attributable claim. The default is refusal.

Exit 1 on mismatch, on an unprovisioned target, on an unguarded target with no
acknowledgement, or if it cannot determine either side.
It fails closed: an inability to verify is not permission to proceed.
"""

from __future__ import annotations

# DEF-12: kept identical to the other checks so this file cannot become the one that
# breaks if it is ever run anywhere but a local shell.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import configparser
import os
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "databricks.yml"

# Azure Databricks workspace URL: https://adb-<workspace_id>.<shard>.azuredatabricks.net
AZURE_HOST = re.compile(r"^https://adb-(\d+)\.(\d+)\.azuredatabricks\.net$")


def normalise(host: str) -> str:
    return (host or "").strip().rstrip("/")


def workspace_id(host: str) -> str | None:
    m = AZURE_HOST.match(normalise(host))
    return m.group(1) if m else None


def declared_host(target: str) -> tuple[str, str]:
    """Resolve the host the bundle declares for a target. Returns (host, var_name)."""
    bundle = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    targets = bundle.get("targets", {}) or {}
    if target not in targets:
        raise SystemExit(
            f"unknown target {target!r}. Declared: {sorted(targets)}"
        )
    raw = ((targets[target].get("workspace", {}) or {}).get("host") or "").strip()
    m = re.fullmatch(r"\$\{var\.([a-zA-Z0-9_]+)\}", raw)
    if not m:
        # a literal host, unusual but legal
        return normalise(raw), "<literal>"
    var = m.group(1)
    spec = (bundle.get("variables", {}) or {}).get(var, {}) or {}
    # environment override wins, exactly as the CLI resolves it
    env = os.environ.get(f"BUNDLE_VAR_{var}")
    return normalise(env if env else spec.get("default", "")), var


def actual_host(profile: str | None) -> tuple[str, str]:
    """Resolve the host the CLI would authenticate to. Returns (host, source)."""
    env_host = os.environ.get("DATABRICKS_HOST")
    if env_host and not profile:
        return normalise(env_host), "DATABRICKS_HOST"

    cfg_path = Path(os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg"))
    if not cfg_path.exists():
        raise SystemExit(
            f"cannot determine the authenticated workspace: no {cfg_path} and no "
            f"DATABRICKS_HOST. Run `databricks auth login` or pass --profile."
        )
    parser = configparser.ConfigParser()
    parser.read(cfg_path)
    section = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"
    if section not in parser:
        raise SystemExit(
            f"profile {section!r} not found in {cfg_path}. Available: "
            f"{sorted(s for s in parser.sections())}"
        )
    host = parser[section].get("host", "")
    if not host:
        raise SystemExit(f"profile {section!r} in {cfg_path} declares no host")
    return normalise(host), f"{cfg_path.name}:[{section}]"


UNMASK_GROUP = "scope_unmask_currency_values"


def declared_run_as(target: str) -> str | None:
    """The identity a target declares in `run_as`, or None if it declares none.

    Read from the bundle rather than listed here, for the same reason the host is: a
    fifth unguarded target must be caught the day it is added, not the day somebody
    remembers to update a literal. tests/test_accelerator.py asserts that the set this
    derives agrees with the set recorded there, so the deploy gate and the build gate
    cannot drift into disagreeing about which targets are exposed.
    """
    bundle = yaml.safe_load(BUNDLE.read_text(encoding="utf-8")) or {}
    target_cfg = (bundle.get("targets", {}) or {}).get(target) or {}
    run_as = target_cfg.get("run_as") or {}
    if not isinstance(run_as, dict):
        return None
    name = run_as.get("service_principal_name") or run_as.get("user_name") or ""
    return str(name).strip() or None


def unguarded_targets() -> list[str]:
    """Every declared target that runs its loads as whoever deploys, sorted."""
    bundle = yaml.safe_load(BUNDLE.read_text(encoding="utf-8")) or {}
    return sorted(t for t in (bundle.get("targets", {}) or {})
                  if not declared_run_as(t))


def _looks_like_identity(value: str) -> bool:
    """A user principal name or an application id -- not a word meaning 'yes'.

    The acknowledgement is only worth requiring if it costs something to give. `--
    unmask-identity yes` would be a rubber stamp and would put nothing useful in the run
    log, so the value must be shaped like an identity somebody can be asked about later.
    """
    v = (value or "").strip()
    if "@" in v and "." in v.split("@")[-1] and len(v.split("@")[0]) > 0:
        return True
    return bool(re.fullmatch(r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", v))


def run_as_refusal(target: str, unmask_identity: str | None) -> str | None:
    """Why deploying to this target now risks writing NULLs into silver, or None.

    Returns None -- proceed -- in exactly two cases: the target declares a `run_as`, so
    its loads do not run as a person at all; or the operator has named a confirmed
    identity. Everything else refuses, including a malformed acknowledgement, because a
    rubber stamp that passes is worse than no gate: it puts a green tick next to a claim
    nobody made.

    NOTE WHAT THIS DELIBERATELY DOES NOT COMPUTE: whether this target actually loads a
    masked column. That calculation needs the entity model, and it lives in
    tests/test_accelerator.py where the model is already loaded and where narrowing a
    target's active_sources is exercised. Measured there: all nine declared targets reach
    at least one masked column, so \"declares no run_as\" is sufficient on its own here.
    Keeping this file to yaml + stdlib is deliberate -- a fail-closed preflight that can
    itself fail to import is a preflight that gets skipped.
    """
    if declared_run_as(target):
        return None
    if unmask_identity and _looks_like_identity(unmask_identity):
        return None
    if unmask_identity:
        return (
            f"--unmask-identity {unmask_identity!r} does not look like an identity. Give "
            f"the user principal name or the application id of the account this deploy "
            f"will load as, so the claim is attributable. A word meaning 'yes' is not a "
            f"confirmation."
        )
    return (
        f"target {target!r} declares no run_as, so its loads run as whoever runs this "
        f"deploy -- not as a service principal.\n"
        f"  Mask functions evaluate with the RUN-AS identity's rights during a refresh. "
        f"governance.mask_money and governance.mask_money_double admit\n"
        f"  {UNMASK_GROUP} (they admitted global_dataplatform_pipeline_job_runners until "
        f"the platform team refused to use an operational group as a\n"
        f"  data-access key). If the identity you are about to load as is not in "
        f"{UNMASK_GROUP}, every masked amount reads NULL, and the first\n"
        f"  materialized view over a masked column writes those NULLs into the "
        f"insert-only silver vault. That write cannot be undone.\n"
        f"  Targets with no run_as: {unguarded_targets()}. See DEPLOY.md 6b.\n"
        f"  To proceed, confirm membership and say whose it is:\n"
        f"    --unmask-identity <you@example.com>"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, help="bundle target, e.g. usnc_tds")
    ap.add_argument("--profile", default=None, help="CLI profile; defaults to the CLI's own resolution")
    ap.add_argument("--actual-host", default=None, help="override, for testing")
    ap.add_argument("--unmask-identity", default=None,
                    help="for a target with no run_as: the identity this deploy will "
                         "load as, which you have confirmed is in "
                         f"{UNMASK_GROUP}")
    args = ap.parse_args()

    want, var = declared_host(args.target)
    if not want:
        print(f"PREFLIGHT FAILED: target {args.target!r} has no host.")
        print(f"  The bundle variable {var!r} is empty, which means this workspace is")
        print(f"  not provisioned yet. Do NOT repoint this target at another workspace")
        print(f"  to get a deploy through -- create the workspace, or pick another target.")
        return 1

    if not workspace_id(want):
        print(f"PREFLIGHT WARNING: declared host {want!r} is not the expected Azure "
              f"Databricks shape. Continuing, but check it.")

    if args.actual_host:
        have, source = normalise(args.actual_host), "--actual-host"
    else:
        have, source = actual_host(args.profile)

    print(f"target        {args.target}")
    print(f"bundle says   {want}   (var {var})")
    print(f"CLI resolves  {have}   (from {source})")

    if want != have:
        print()
        print("PREFLIGHT FAILED: you are authenticated to a DIFFERENT workspace than the")
        print("target declares. Deploying now would put this target's artefacts in the")
        print("wrong lake.")
        wid_want, wid_have = workspace_id(want), workspace_id(have)
        if wid_want and wid_have:
            print(f"  intended workspace id: {wid_want}")
            print(f"  authenticated to:      {wid_have}")
        print("  Fix the profile, or the --target, not the bundle.")
        return 1

    # SECOND REFUSAL, AND IT IS AFTER THE HOST CHECK ON PURPOSE: there is no point
    # asking who you will load as until it is settled which lake you are pointed at.
    refusal = run_as_refusal(args.target, args.unmask_identity)
    if refusal:
        print()
        print(f"PREFLIGHT FAILED: {refusal}")
        return 1

    print("\nPREFLIGHT PASSED: authenticated workspace matches the target.")
    if not declared_run_as(args.target):
        print(f"  run_as:       NONE -- loading as {args.unmask_identity}, confirmed by "
              f"you to be in {UNMASK_GROUP}")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
