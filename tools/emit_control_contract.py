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

from accelerator import control_standard as cs, sql_text  # noqa: E402

GOLD_DDL_PATH = ROOT / "governance" / "control_objects_gold.sql"
BRONZE_DDL_PATH = ROOT / "governance" / "control_objects_bronze.sql"
BRONZE_CONTRACT_PATH = ROOT / "control_contracts" / "bronze.yaml"

INVARIANT = "staged = accepted + sum(discarded)"


def _comment_safe(text: str) -> str:
    """Sanitise prose before it is interpolated into a `--` line in generated DDL.

    apply_governance.statements() splits the whole file on ';' BEFORE it strips comment
    lines, so a semicolon anywhere in a '--' comment silently cuts a CREATE TABLE apart --
    measured directly against the real splitter: it produced 3 "statements" for 3 core
    tables (a count check would have passed), but statement 0 was the orphaned comment
    fragment after the cut and aud_load_run's CREATE TABLE had vanished entirely. The emitter
    owns making its own DDL splittable, so it sanitises at the point of emission rather than
    editing the declaration's prose -- that same prose is also published in the bronze
    contract's YAML, where a semicolon is harmless and reads better.
    """
    return text.replace(";", ",")


def _literal_safe(text: str) -> str:
    """Sanitise prose before it is interpolated into a SQL COMMENT '...' literal.

    Same semicolon rule as _comment_safe -- the splitter cuts on ';' regardless of quoting --
    plus the one thing a quoted literal adds: an apostrophe would CLOSE the literal early and
    leave the rest of the sentence as bare SQL.

    BACKSLASH, NOT DOUBLING, AND THAT IS MEASURED RATHER THAN ASSUMED. This function shipped
    for one commit using `''`, which is the ANSI escape and is what most SQL dialects take.
    Spark does not: re-parsing the emitted DDL through
    sessionState().sqlParser().parsePlan() raised PARSE_SYNTAX_ERROR pointing at the second
    quote of the pair, and ctl_etl_run's CREATE TABLE was unparseable while all 1884 offline
    checks stayed green -- the offline suite compares text and cannot parse SQL. Backslash
    escapes are what Spark and Databricks accept, and the backslash itself is escaped first
    so a literal `\\` in prose cannot consume the quote after it.

    ONE ESCAPER, and it lives in accelerator.sql_text now: two parity gates were still
    doubling the quote when this was fixed here, and a rule proven in one file is not a
    rule. The semicolon stays here because it is the DDL APPLIER's constraint, not SQL's.
    """
    return sql_text.escape(_comment_safe(text))


def _own_tables_lines(layer: str, label: str) -> list[str]:
    """The `--` header lines that enumerate what THIS layer declares of its own.

    GENERATED, NOT TYPED. This sentence was hand-written prose until 28 September 2026, and it
    said bronze declared ONE table of its own -- true when it was written, and false the
    moment two run-identity tables were added. The generated DDL is byte-gated, so a stale
    header ships as a fact to the team that owns the contract. A header derived from the
    declaration cannot say a number the declaration disagrees with.
    """
    own = cs.LAYER_TABLES[layer]
    if not own:
        return [f"-- {label} declares no control tables of its own."]
    width = max(len(t) for t in own)
    lines = [f"-- {label} declares {len(own)} table(s) of its own beyond the mandatory core:"]
    for table in sorted(own):
        prop = "APPEND-ONLY" if table in cs.APPEND_ONLY else "MUTABLE"
        lines.append(f"--   {table:<{width}}  {prop}")
    return lines


def _layer_ddl(layer: str, catalog_var: str, header: list) -> str:
    """One layer's control DDL, from the standard. Shared by gold and bronze.

    EXTRACTED 31 Aug, when bronze gained a generated DDL of its own. It was gold's function
    alone until then; a second copy for bronze would have been two places for the semicolon
    rule, the append-only branch and the schema-creation statement to drift. gold_ddl()'s
    output is asserted byte-identical across the extraction, so the refactor is proven faithful
    rather than assumed.

    NO SEMICOLON MAY APPEAR INSIDE A COMMENT LITERAL OR A `--` COMMENT LINE. The statement
    splitter in checks/apply_control_objects.py cuts on that character regardless of quoting
    or comments, so one inside a COMMENT -- or a plain `--` line -- splits the file into
    fragments. It has caught exactly that four times in this repo, once in the `--` header this
    function emits (see _comment_safe) and once in a hand-written comment added to
    control_objects.sql on 30 Aug.

    THE SCHEMA IS CREATED HERE TOO, matching how control_objects.sql creates its own schemas
    before any CREATE TABLE. Neither gold's catalog nor bronze's `control` schema exists today,
    so the first apply runs against a catalog with no schema in it -- without this statement
    every CREATE TABLE would raise SCHEMA_NOT_FOUND and the DDL would be unusable on the one
    day it is needed.
    """
    lines = list(header) + [
        "",
        f"CREATE SCHEMA IF NOT EXISTS `${{{catalog_var}}}`.`${{control_schema}}`;\n",
    ]
    for table, columns in cs.tables_for(layer).items():
        # COLUMN COMMENTS TRAVEL WITH THE COLUMN. A rule stated only in control_standard.py is
        # a rule the Bronze engineer writing the row never sees -- they see DESCRIBE on the
        # deployed table. Sanitised through _literal_safe for the same reason the `--` header
        # is: the statement splitter cuts on ';' before comments are stripped.
        comments = cs.COLUMN_COMMENTS.get(table, {})
        cols = ",\n".join(
            f"  {name:<20} {sql_type}"
            + (f" COMMENT '{_literal_safe(comments[name])}'" if name in comments else "")
            for name, sql_type in columns.items())
        # THE MARKER IS UNCONDITIONAL, THE APPEND-ONLY PROPERTY IS NOT. An earlier draft set
        # `prop` to the marker when a table was not append-only and then appended the marker
        # again, producing a duplicate TBLPROPERTIES key. That was latent while only gold used
        # this code, because gold declares append-only core tables and nothing else. BRONZE IS
        # THE CASE IT WAS BROKEN FOR: ctl_delivery_manifest is MUTABLE by design -- a manifest
        # gets corrected -- so this is the first layer where the else-branch is real, and a
        # test asserts the manifest's OWN properties carry no append-only marker.
        props = ["'hfig.control_object' = 'true'"]
        if table in cs.APPEND_ONLY:
            props.insert(0, "'delta.appendOnly' = 'true'")
        lines.append(
            f"CREATE TABLE IF NOT EXISTS `${{{catalog_var}}}`.`${{control_schema}}`.{table} (\n"
            f"{cols}\n"
            f")\n"
            f"TBLPROPERTIES ({', '.join(props)});\n"
        )
    return "\n".join(lines)


def bronze_ddl() -> str:
    """Bronze's control DDL.

    WHY THIS EXISTS AT ALL, AND WHAT CHANGED. control_contracts/bronze.yaml asks the Bronze
    team to BUILD these four tables. That contract stands for the ROWS -- a delivery manifest
    records what Bronze delivered, and only Bronze can write it. What changed on 31 Aug is who
    creates the CONTAINERS: measured, the deploying identity holds CREATE_SCHEMA on
    01_usnc_bronze_dev through scope_tds_full_scopes_write, so we can stand the schema up
    ourselves instead of asking. That turns a three-part ask into a two-part one.

    IT DOES NOT MAKE US THE OWNER OF BRONZE'S AUDIT. control_standard is explicit that each
    layer records what IT did and the downstream layer reads upstream's. Creating a table is
    not writing to it: these objects stay Bronze's to populate, and loop-1 stays dormant until
    they do. This removes a dependency, not the blocker.
    """
    return _layer_ddl("bronze", "bronze_catalog", [
        "-- GENERATED from src/accelerator/control_standard.py. Do not hand-edit.",
        "-- verify_repo.py fails the build if regenerating this produces a diff.",
        "--",
    ] + _own_tables_lines("bronze", "Bronze") + [
        "--",
        "-- THE SPLIT IS THE DESIGN, and bronze is where both sides of it are real. A manifest",
        "-- records what SHOULD happen and stays correctable, while a run, its status events",
        "-- and the audit tables record what DID happen and must never be rewritten. A",
        "-- conforming layer gets that split right rather than setting the property everywhere.",
        "--",
        "-- A RETRY IS NOT A SECOND DELIVERY. A repair run re-registers the SAME etl_run_id and",
        "-- appends a status event rather than minting a new one. That rule is carried on the",
        "-- column it constrains, as a COMMENT on ctl_etl_run.etl_run_id, and it is STATED",
        "-- rather than enforced: nothing writes these tables yet.",
        "--",
        "-- ingestion_mechanism, producer and mechanism_native_run_id are OPERATOR-FACING. They",
        "-- say who to call and where their own log is. Nothing joins on them, and no identity",
        "-- or reconciliation may read them -- a control that branches on the mechanism makes",
        "-- every new mechanism a code change.",
        "--",
        f"-- The invariant a conforming layer satisfies is {_comment_safe(INVARIANT)}",
        "--",
        f"-- In bronze, {_comment_safe(cs.STAGED_MEANING['bronze'])}",
        "--",
        "-- WE CREATE THESE, BRONZE POPULATES THEM. The rows are Bronze's: only the delivering",
        "-- system knows what it delivered. See control_contracts/bronze.yaml, which is the",
        "-- ask for the rows, and docs/loop1_control_table_request.html for why loop-1 needs",
        "-- them.",
    ])


def gold_ddl() -> str:
    """Gold's control DDL: the core alone, because gold declares nothing of its own.

    The mechanics live in _layer_ddl, shared with bronze since 31 Aug. This function is now
    just gold's header -- kept as prose because the reason gold declares nothing is a
    modelling fact, not a formatting one.
    """
    return _layer_ddl("gold", "gold_catalog", [
        "-- GENERATED from src/accelerator/control_standard.py. Do not hand-edit.",
        "-- verify_repo.py fails the build if regenerating this produces a diff.",
        "--",
        "-- Gold declares no control tables of its own: a projection layer has no rejects to",
        "-- supersede and no expectations until someone declares them. This is the mandatory",
        f"-- core alone, and the invariant it must satisfy is {_comment_safe(INVARIANT)}",
        "--",
        f"-- In gold, {_comment_safe(cs.STAGED_MEANING['gold'])}",
    ])


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
                # PUBLISHED, NOT JUST GENERATED INTO THE DDL. The contract is what the Bronze
                # team reads before they build anything; the DDL is what they read after. The
                # retry rule on ctl_etl_run.etl_run_id has to be in the first of those, not
                # only the second. Always present, empty where nothing is declared, so the
                # shape does not vary per table.
                "column_comments": dict(cs.COLUMN_COMMENTS.get(table, {})),
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
    BRONZE_DDL_PATH.write_text(bronze_ddl(), encoding="utf-8")
    print(f"wrote {BRONZE_DDL_PATH.relative_to(ROOT)}")
    BRONZE_CONTRACT_PATH.parent.mkdir(exist_ok=True)
    BRONZE_CONTRACT_PATH.write_text(render(bronze_contract()), encoding="utf-8")
    print(f"wrote {BRONZE_CONTRACT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
