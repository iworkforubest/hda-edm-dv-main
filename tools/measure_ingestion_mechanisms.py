"""
MEASURE which mechanism delivers each schema of the bronze catalog.

Branch 1 of the ETL control framework gives every row landed in Bronze a run
identity. Before anything can be asked of anyone, we have to know WHO to ask --
and the bronze schema names do not say. `snaplogic` covers the Snaplogic flows
only and is empty; the estate also ingests through Synapse and through pipelines
deployed by bundles that are not this one.

WHAT THIS TOOL IS ALLOWED TO CONCLUDE
-------------------------------------
Only what the workspace shows it. Two facts are observable per schema:

  * the objects it holds (`information_schema.tables`: how many, of what type,
    created by whom), and
  * whether a Lakeflow declarative pipeline (SDP) in this workspace publishes
    into it (`spec.catalog` + `spec.target`/`spec.schema` of every pipeline).

From those, exactly two verdicts are reachable, and the split is between
LANDING and PROPAGATION:

  * `sdp` -- an SDP pipeline in this workspace publishes into the schema AND the
    schema is not a landing schema. Its rows come from inside the lake. The
    pipeline is the writer, so its owner is who to ask.

  * `unknown` -- everything else. Crucially this INCLUDES the `_raw` landing
    schemas, even though an SDP pipeline materialises them. Those pipelines do
    not deliver: `load_raw.py` in the loading bundle is a `cloudFiles`
    readStream over a MANAGED volume, so the pipeline READS files that some
    other agent already dropped. Every one of those volumes is MANAGED with a
    `__unitystorage` location, so nothing in Unity Catalog names the agent that
    writes them. Snaplogic, Synapse or a vendor push are all consistent with
    what is visible, and choosing between them from here would be a guess.

`unknown` is an OUTPUT, not a failure. It is the list to put in front of the
Bronze / ingestion team, and a guessed value would put it in front of the wrong
team instead. If every entry came back `unknown`, branch 1 would still land.

This tool does NOT read this repository's own pipelines to reach a verdict, and
must not: `resources/vault_pipeline.yml` records decision D4 -- this bundle has
no bronze-ingest pipeline and does not land bronze at all. The pipelines that do
are found in the workspace, and they belong to the `dataplatform_loading_bronze_usnc`
bundle, not to us.

IT IS READ-ONLY. It issues SELECTs against `information_schema` and GETs against
the pipelines API. It creates, alters and drops nothing in the workspace.

    .venv/bin/python tools/measure_ingestion_mechanisms.py --target usnc_tds
    .venv/bin/python tools/measure_ingestion_mechanisms.py --target usnc_tds --dry-run

The target is resolved BY HOST, never by profile name. `databricks.yml` declares
the host for a target; `~/.databrickscfg` maps profiles to hosts; exactly one
profile must match, or this refuses to run. There are eight near-identical
workspaces in this estate and profile names are not a reliable discriminator --
see checks/preflight_target.py, which fails closed for the same reason.

Writes two files, and they are deliberately two:

  * metadata/bronze_schemata.yml     -- the MEASUREMENT. What information_schema
                                        returned, classified by nothing.
  * metadata/ingestion_mechanisms.yml -- the JUDGEMENT made from it.

WHAT THE OFFLINE COVERAGE CHECK CAN AND CANNOT DO -- READ THIS BEFORE TRUSTING IT
---------------------------------------------------------------------------------
tests/test_accelerator.py asserts the judgement covers the measurement. Folding
the two into one file would make that check confirm the file agrees with itself.

But both files are outputs of ONE run of this tool, so that check compares a
measurement to a judgement made from it. It CATCHES a committed map that
disagrees with the committed readout -- a hand-edit, a partial commit, a
classifier that drops a schema. It does NOT and CANNOT catch a schema created in
the workspace since the last measurement: if Bronze creates `workday_raw`
tomorrow, neither committed file changes and the coverage check stays green while
`workday_raw` is a source nobody is asked about.

Engineering around that offline would be a false green. The CI gates are
deliberately credential-free -- .github/workflows/verify.yml says so in terms
("takes no secrets ... Do NOT add credentials here"), and the standing rule is
that a profile is passed explicitly and never auto-selected -- so no check in the
OFFLINE SUITE can see the live catalog. Workspace-side gates in checks/ do see it,
run against a target from resources/vault_job.yml, and a coverage gate there is
the real fix -- it is out of this task's scope and does not exist yet. The honest offline guarantee is therefore not
"the map is current" but "the map has been re-measured recently enough to be
worth trusting", and that is what the staleness check asserts. It can fail
offline, because time passes. Re-running this tool is the fix.
"""

from __future__ import annotations

import argparse
import configparser
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "databricks.yml"
MECHANISM_FILE = ROOT / "metadata" / "ingestion_mechanisms.yml"
SCHEMATA_FILE = ROOT / "metadata" / "bronze_schemata.yml"

# The mechanisms this estate actually has. Spec section 2a names Snaplogic, Synapse
# and SDP as the tools the identity logic must be independent of; `unknown` is the
# fourth because a mechanism we cannot see is the honest answer, not an absent one.
#
# `snaplogic` and `synapse` are in the vocabulary and unused by today's measurement.
# That is the point: they exist in the estate, no workspace evidence resolves a
# schema to either of them, and writing one down anyway would be the guess this
# tool exists to avoid.
MECHANISMS = ("sdp", "snaplogic", "synapse", "unknown")

# What the two reachable verdicts MEAN. Branch 2 reads `mechanism` programmatically, and
# the distinction is the whole point of the artefact:
#
#   sdp     -- who WRITES the bronze row. NOT who delivered the file. Asking an SDP
#              pipeline owner why a delivery was late is the wrong team, which is the
#              harm this map exists to prevent.
#   unknown -- the delivering agent is not observable from inside the lake. Ask Bronze.
VERDICT_MEANING = {
    "sdp": "who WRITES the bronze row -- not who delivered the file",
    "unknown": "the delivering agent is not observable from inside the lake -- ask Bronze",
}

# How old a measurement may be before the offline suite refuses to trust it.
#
# THIRTY DAYS, and the justification is NOT that a shorter window bounds the number of
# schemas that can be missed -- it does not. 48 schemas have been created in this catalog
# since 2025-08-27, roughly one every eight days, and five arrived in the six weeks to
# 2026-09-28 (ukg, ukg_raw, sap_fieldglass, sap_fieldglass_raw, control). No bound short
# of about a week would make the window of invisibility smaller than one arrival, and a
# week is operationally absurd.
#
# So the bound is an OPERATING CYCLE, not a correctness proof: the map is re-measured at
# least once a month, or the suite stops believing it. That is the strongest honest claim
# an offline check can make about a live catalog it cannot query.
STALENESS_DAYS = 30

# The measurement file is an ENVELOPE -- provenance beside the readout -- because a bare
# mapping has nowhere to put a date that is not mistakable for a schema name.
SCHEMATA_KEY = "schemata"

# Both artefacts carry this in their comment header. The suite parses it out of each and
# requires the two to agree, so re-committing one without the other reds rather than
# leaving a fresh map beside a stale readout.
STAMP = re.compile(r"^# measured_at: (\d{4}-\d{2}-\d{2})$", re.M)


def measured_at(text: str) -> str:
    """The date stamped in an artefact's header, or "" if it carries none."""
    m = STAMP.search(text or "")
    return m.group(1) if m else ""


def schemata_of(measurement):
    """The readout inside the envelope. {} for anything that is not one."""
    if isinstance(measurement, dict) and isinstance(measurement.get(SCHEMATA_KEY), dict):
        return measurement[SCHEMATA_KEY]
    return {}

# NOT source systems, and therefore not part of the coverage the map owes.
#
#   information_schema -- Unity Catalog's own, auto-created, owned by "System user".
#   control            -- OURS. The bronze control tables this branch extends.
#   monitoring         -- observability views over the lake, derived not delivered.
#   reporting          -- consumption views, likewise.
#   test / test_raw    -- the loading team's test harness ("Bronze Tables for the
#                         test loading"), not a source system.
#
# Everything else in the catalog is treated as a candidate source schema and must
# appear in the map, INCLUDING schemas that hold nothing yet. A declared-but-empty
# schema is a real question ("is this live, and who will deliver it?"), and dropping
# it from the map would silently answer it.
SYSTEM_SCHEMAS = frozenset(
    {"information_schema", "control", "monitoring", "reporting", "test", "test_raw"}
)

# A landing schema: where data first arrives in the lake from outside it. The loading
# bundle's convention throughout this catalog is `<source>_raw`, fed by a cloudFiles
# readStream over a volume. `great_plains` also carries a hand-created `csv` volume,
# which is why volume presence is evidence in its own right and is recorded per schema.
LANDING_SUFFIX = "_raw"


def is_landing(schema: str) -> bool:
    return schema.endswith(LANDING_SUFFIX)


def is_system(schema: str) -> bool:
    return schema in SYSTEM_SCHEMAS


def source_schemas(all_schemas) -> list[str]:
    """The non-system schemas of the bronze catalog -- what the map must cover."""
    return sorted(s for s in all_schemas if not is_system(s))


# --------------------------------------------------------------------------- target


def _normalise_host(host: str) -> str:
    return (host or "").strip().rstrip("/")


def declared_host(target: str) -> str:
    """The host databricks.yml declares for a target. Never a profile name."""
    bundle = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    targets = bundle.get("targets", {}) or {}
    if target not in targets:
        raise SystemExit(f"unknown target {target!r}. Declared: {sorted(targets)}")
    raw = ((targets[target].get("workspace", {}) or {}).get("host") or "").strip()
    m = re.fullmatch(r"\$\{var\.([a-zA-Z0-9_]+)\}", raw)
    if m:
        var = m.group(1)
        env = os.environ.get(f"BUNDLE_VAR_{var}")
        spec = (bundle.get("variables", {}) or {}).get(var, {}) or {}
        raw = env if env else (spec.get("default") or "")
    host = _normalise_host(raw)
    if not host:
        raise SystemExit(
            f"target {target!r} declares no host -- it is not provisioned. Do NOT "
            f"repoint it at another workspace to get a run through."
        )
    return host


def target_variable(target: str, name: str) -> str:
    bundle = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    tgt = (bundle.get("targets", {}) or {}).get(target, {}) or {}
    value = (tgt.get("variables", {}) or {}).get(name)
    if value is None:
        value = ((bundle.get("variables", {}) or {}).get(name, {}) or {}).get("default")
    if not value:
        raise SystemExit(f"target {target!r} declares no {name!r}")
    return str(value)


def _profiles() -> configparser.ConfigParser:
    cfg_path = Path(
        os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg")
    )
    if not cfg_path.exists():
        raise SystemExit(f"no {cfg_path}: run `databricks auth login` first")
    parser = configparser.ConfigParser()
    parser.read(cfg_path)
    return parser


def host_for_profile(profile: str) -> str:
    """The host a named profile is authenticated to. What the run ACTUALLY reads."""
    parser = _profiles()
    if profile not in parser:
        raise SystemExit(
            f"profile {profile!r} not found. Sections: {sorted(parser.sections())}"
        )
    host = _normalise_host(parser[profile].get("host", ""))
    if not host:
        raise SystemExit(f"profile {profile!r} declares no host")
    return host


def profile_for_host(host: str) -> str:
    """The ONE profile authenticated to `host`. Refuses on zero or several.

    Matching is on the host, because that is what identifies a workspace. Several
    profiles in this estate point at the same workspace under different names, and
    several workspaces have near-identical names; a name match would pick one of
    eight lakes by coincidence.
    """
    parser = _profiles()
    matches = [
        name
        for name in parser.sections()
        if _normalise_host(parser[name].get("host", "")) == host
    ]
    if not matches:
        raise SystemExit(
            f"no profile is authenticated to {host}. Sections: "
            f"{sorted(parser.sections())}"
        )
    if len(matches) > 1:
        raise SystemExit(
            f"{len(matches)} profiles point at {host}: {sorted(matches)}. Pass "
            f"--profile to say which, having checked it is the right lake."
        )
    return matches[0]


# ------------------------------------------------------------------------- workspace


def _cli(args: list[str], profile: str) -> str:
    proc = subprocess.run(
        ["databricks", *args, "--profile", profile, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"`databricks {' '.join(args)}` failed ({proc.returncode}): "
            f"{proc.stderr.strip()[:600]}"
        )
    return proc.stdout


def run_select(statement: str, warehouse_id: str, profile: str) -> list[list]:
    """Run ONE read-only SELECT. Refuses anything that is not a SELECT."""
    if not statement.lstrip().upper().startswith("SELECT"):
        raise SystemExit("this tool issues SELECTs only")
    payload = json.dumps(
        {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "50s"}
    )
    proc = subprocess.run(
        [
            "databricks", "api", "post", "/api/2.0/sql/statements",
            "--profile", profile, "--json", payload,
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        raise SystemExit(f"SQL request failed: {proc.stderr.strip()[:600]}")
    body = json.loads(proc.stdout)
    state = ((body.get("status") or {}).get("state") or "").upper()
    if state and state != "SUCCEEDED":
        raise SystemExit(f"statement {state}: {json.dumps(body.get('status'))[:600]}")
    return (body.get("result") or {}).get("data_array") or []


def measure_schemata(catalog: str, warehouse_id: str, profile: str) -> dict:
    """What information_schema says. No judgement is applied here."""
    # chr(45) is a hyphen. A literal one inside a SQL string would be fine, but the
    # repo bans semicolons in SQL and keeps string literals plain, so this stays in
    # the same house style as governance/*.sql.
    rows = run_select(
        f"SELECT schema_name, schema_owner, coalesce(comment, chr(45)) "
        f"FROM {catalog}.information_schema.schemata ORDER BY schema_name",
        warehouse_id,
        profile,
    )
    schemata = {
        str(r[0]): {"owner": str(r[1]), "comment": str(r[2])} for r in rows
    }

    tables = run_select(
        f"SELECT table_schema, count(*), "
        f"concat_ws(chr(44), sort_array(collect_set(table_type))), "
        f"concat_ws(chr(44), sort_array(collect_set(created_by))) "
        f"FROM {catalog}.information_schema.tables GROUP BY table_schema",
        warehouse_id,
        profile,
    )
    for schema, count, types, creators in tables:
        entry = schemata.setdefault(str(schema), {"owner": "", "comment": ""})
        entry["object_count"] = int(count)
        entry["object_types"] = sorted(t for t in str(types).split(",") if t)
        entry["created_by"] = sorted(c for c in str(creators).split(",") if c)

    volumes = run_select(
        f"SELECT volume_schema, count(*), "
        f"concat_ws(chr(44), sort_array(collect_set(volume_type))) "
        f"FROM {catalog}.information_schema.volumes GROUP BY volume_schema",
        warehouse_id,
        profile,
    )
    for schema, count, types in volumes:
        entry = schemata.setdefault(str(schema), {"owner": "", "comment": ""})
        entry["volume_count"] = int(count)
        entry["volume_types"] = sorted(t for t in str(types).split(",") if t)

    for entry in schemata.values():
        entry.setdefault("object_count", 0)
        entry.setdefault("object_types", [])
        entry.setdefault("created_by", [])
        entry.setdefault("volume_count", 0)
        entry.setdefault("volume_types", [])
    return dict(sorted(schemata.items()))


def measure_pipelines(catalog: str, profile: str) -> dict:
    """{schema: [pipeline descriptions]} for every SDP pipeline publishing into `catalog`."""
    listing = json.loads(_cli(["pipelines", "list-pipelines"], profile) or "[]")
    publishers: dict[str, list[str]] = {}
    for index, item in enumerate(listing, start=1):
        pid = item.get("pipeline_id")
        if not pid:
            continue
        print(f"  pipeline {index}/{len(listing)} {item.get('name', pid)}", file=sys.stderr)
        spec = (json.loads(_cli(["pipelines", "get", pid], profile)) or {}).get("spec") or {}
        if (spec.get("catalog") or "") != catalog:
            continue
        schema = spec.get("target") or spec.get("schema") or ""
        if not schema:
            continue
        bundle = ((spec.get("deployment") or {}).get("metadata_file_path") or "")
        bundle_name = ""
        m = re.search(r"/\.bundle/([^/]+)/", bundle)
        if m:
            bundle_name = m.group(1)
        publishers.setdefault(str(schema), []).append(
            f"SDP pipeline {pid} {spec.get('name', '')!r}"
            + (f" from bundle {bundle_name}" if bundle_name else "")
        )
    return {k: sorted(v) for k, v in sorted(publishers.items())}


# -------------------------------------------------------------------------- verdicts


def classify(schema: str, facts: dict, publishers: list[str]) -> tuple[str, list[str]]:
    """The mechanism and the evidence for it. Never returns empty evidence."""
    evidence: list[str] = []
    if facts.get("comment") and facts["comment"] != "-":
        evidence.append(f"schema comment: {facts['comment']}")
    evidence.append(
        f"{facts.get('object_count', 0)} object(s)"
        + (f" of type {', '.join(facts['object_types'])}" if facts.get("object_types") else "")
        + (f", {facts['volume_count']} volume(s) of type "
           f"{', '.join(facts.get('volume_types') or [])}" if facts.get("volume_count") else "")
    )
    for description in publishers:
        evidence.append(f"published into by {description}")

    if is_landing(schema) and publishers:
        evidence.append(
            "LANDING schema: that pipeline is a cloudFiles readStream over a MANAGED "
            "volume, so it READS files another agent dropped rather than delivering "
            "them. No Unity Catalog object names that agent, so the delivering "
            "mechanism is not observable here -- ask the Bronze team whether it is "
            "Snaplogic, Synapse or a vendor push"
        )
        return "unknown", evidence

    if is_landing(schema):
        evidence.append(
            "LANDING schema with no pipeline publishing into it: nothing reads it and "
            "nothing in Unity Catalog names what would write its volume -- ask the "
            "Bronze team whether it is live at all"
        )
        return "unknown", evidence

    if publishers:
        evidence.append(
            "rows arrive from inside the lake via the pipeline above, so its owner is "
            "who to ask"
        )
        return "sdp", evidence

    evidence.append(
        "no SDP pipeline in this workspace publishes into it and nothing else names a "
        "writer -- ask the Bronze team what delivers it, or whether it is dormant"
    )
    return "unknown", evidence


def build_map(schemata: dict, publishers: dict) -> dict:
    out = {}
    for schema in source_schemas(schemata):
        mechanism, evidence = classify(
            schema, schemata[schema], publishers.get(schema, [])
        )
        out[schema] = {"mechanism": mechanism, "evidence": evidence}
    return out


# ------------------------------------------------------------------------------ main


def _dump(path: Path, header: str, data: dict, write: bool) -> None:
    body = header + yaml.safe_dump(data, sort_keys=True, width=100, allow_unicode=True)
    if write:
        path.write_text(body, encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)} ({len(data)} entries)")
    else:
        print(f"--- {path.relative_to(ROOT)} (dry run) ---")
        print(body)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target", default="usnc_tds", help="bundle target, e.g. usnc_tds")
    ap.add_argument("--profile", default=None, help="override the host-resolved profile")
    ap.add_argument("--catalog", default=None, help="override the target's bronze_catalog")
    ap.add_argument("--warehouse-id", default=None)
    ap.add_argument(
        "--allow-host-mismatch",
        action="store_true",
        help="permit --profile to point somewhere other than the target's host",
    )
    ap.add_argument("--dry-run", action="store_true", help="print, do not write")
    args = ap.parse_args()

    declared = declared_host(args.target)
    profile = args.profile or profile_for_host(declared)
    # THE HOST STAMPED IS THE ONE ACTUALLY READ, never the one the target declares.
    # --profile bypasses host resolution, and five of the seven profiles in this estate
    # point at EU prod. Stamping the declared host would have produced artefacts
    # LABELLED usnc_tds while HOLDING another lake's data -- a file that lies about
    # which lake it describes is worse than no file.
    host = host_for_profile(profile)
    if host != declared and not args.allow_host_mismatch:
        raise SystemExit(
            f"profile [{profile}] is authenticated to {host}, but target "
            f"{args.target!r} declares {declared}. Refusing: the measurement would "
            f"describe one lake under another lake's name. Pass "
            f"--allow-host-mismatch only if you meant it, and expect the artefacts to "
            f"be stamped {host}."
        )
    catalog = args.catalog or target_variable(args.target, "bronze_catalog")
    warehouse = args.warehouse_id or target_variable(args.target, "sql_warehouse_id")
    print(f"target {args.target} declares {declared}")
    print(f"profile [{profile}] reads {host}"
          + ("  *** MISMATCH, ALLOWED EXPLICITLY ***" if host != declared else ""))
    print(f"catalog {catalog}, warehouse {warehouse}")

    schemata = measure_schemata(catalog, warehouse, profile)
    publishers = measure_pipelines(catalog, profile)
    mechanisms = build_map(schemata, publishers)

    unknown = [s for s, e in mechanisms.items() if e["mechanism"] == "unknown"]
    print(
        f"\n{len(schemata)} schema(s), {len(mechanisms)} non-system, "
        f"{len(unknown)} unresolved -- that is the list to ask Bronze about:"
    )
    for schema in unknown:
        print(f"  {schema}")

    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    stamp = (
        f"# GENERATED by tools/measure_ingestion_mechanisms.py -- do not hand-edit.\n"
        f"# Measured against {catalog} on {host}\n"
        f"# measured_at: {today}\n"
        f"# Both artefacts carry this date and the suite requires them to agree, so\n"
        f"# re-committing one without the other reds. The suite also refuses a\n"
        f"# measurement older than {STALENESS_DAYS} days -- re-run the tool to fix it:\n"
        f"#   .venv/bin/python tools/measure_ingestion_mechanisms.py "
        f"--target {args.target}\n"
    )
    _dump(
        SCHEMATA_FILE,
        stamp
        + "#\n"
        "# What information_schema returned. THE MEASUREMENT, classified by nothing.\n"
        "# ingestion_mechanisms.yml is checked for covering this, so the two must stay\n"
        "# separate files -- one file would make that check confirm itself.\n",
        {
            "measured_at": today,
            "catalog": catalog,
            "host": host,
            SCHEMATA_KEY: schemata,
        },
        not args.dry_run,
    )
    _dump(
        MECHANISM_FILE,
        stamp
        + "#\n"
        "# THE JUDGEMENT. What the two reachable verdicts MEAN, because a consumer\n"
        "# branching on `mechanism` must not read one as the other:\n"
        + "".join(f"#   {k:<8}-- {v}\n" for k, v in sorted(VERDICT_MEANING.items()))
        + "#\n"
        "# `unknown` is an output, not a failure: it is the list to ask the Bronze team\n"
        "# about. Do not replace one with a guess -- a guess puts the request in front\n"
        "# of the wrong team.\n"
        "#\n"
        f"# Vocabulary: {', '.join(MECHANISMS)}. `snaplogic` and `synapse` are DECLARED\n"
        "# BUT UNOBSERVED -- the estate has both (spec section 2a), no workspace evidence\n"
        "# resolves any schema to either, and no run has ever emitted one. Do not write\n"
        "# a consumer branch on them until a measurement does.\n"
        "#\n"
        "# COVERAGE IS CHECKED AGAINST bronze_schemata.yml, NOT AGAINST THE LAKE. That\n"
        "# catches a map disagreeing with the committed readout. It CANNOT catch a schema\n"
        "# created since the measurement above -- the CI gates hold no credentials. The\n"
        "# staleness bound is what stands in for it.\n",
        mechanisms,
        not args.dry_run,
    )
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc:
        sys.exit(_rc)
