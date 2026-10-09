"""Render the declared vault model as a DBML diagram for dbdiagram.io.

GENERATED, NEVER HAND-EDITED. verify_repo.py asserts that regenerating produces a
byte-identical file, so the diff is the review -- the same discipline data_contracts/,
control_contracts/ and dashboards/ already carry.

WHY THIS EXISTS. There was no picture of this vault anywhere. 21 entities, 25 tables and
every relationship lived only in metadata/entities/*.yml, which is the right place for the
model and the wrong place to see its shape.

ONE FILE, NOT ONE PER TARGET, and that is a measured decision rather than a shortcut. Every
other generated artefact here is per target because its CONTENT differs per target -- a
qualified catalog name, an active-source list. This describes the DECLARED MODEL, which does
not: measured 3 Sep, all 369 columns carry byte-identical types across usnc_tds and weu_tds.
`active_sources` changes which tables a given lake CREATES, not what the model says, and a
diagram of the model should show the model.

REAL `Ref:` LINES, WHICH THE REFERENCE IMPLEMENTATION LACKS. Databricks' own industry data
models ship a DBML diagram whose foreign keys exist only as prose inside `note:` attributes
(inspected 3 Sep, payments_fintech v1 mvm) -- so dbdiagram renders tables with no edges
between them. For a Data Vault that would be worthless: a vault IS its edges, every link and
satellite hanging off a hub. The parents are emitted as `Ref:` so the graph actually draws.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

from accelerator import contract, naming, spec, sql_text  # noqa: E402
from emit_data_contract import _projected_columns  # noqa: E402

OUT_PATH = ROOT / "diagram" / "hfig_data_vault.dbml"

# The logical schema names, not a target's variables. A diagram of the model is not a
# deployment, and hardcoding one lake's catalog here would make the picture wrong everywhere
# else -- the same trap the dashboards hit when their names were left unqualified.
RAW_SCHEMA = "raw_vault"
BUSINESS_SCHEMA = "business_vault"

# DBML needs a type token per column. 152 of 369 columns are `source-derived`: the model
# declares the COLUMN but takes its TYPE from the bound bronze column at load time, so no
# type exists to print. Emitting a plausible-looking `varchar` would be inventing one, so the
# marker is carried through verbatim and explained in the column note. A reader can then see
# exactly which columns have a declared type and which do not, which is information rather
# than noise.
UNDECLARED = "source_derived"


def dbml_type(contract_type: str) -> str:
    """One column's DBML type token.

    DECIMAL(18,2) and the like survive as written -- dbdiagram accepts a parenthesised
    precision. Only the undeclared marker is rewritten, because `source-derived` contains a
    hyphen and DBML would read it as two tokens.
    """
    return UNDECLARED if contract_type == "source-derived" else contract_type


def _esc(text: str) -> str:
    """A DBML note is single-quoted, so an apostrophe in a business term would close it.

    DELEGATED TO accelerator.sql_text, AND THIS WRAPPER IS THE FORK POINT. DBML is not SQL
    -- dbdiagram is a different parser -- but its single-quoted note takes the same
    backslash escapes, and keeping a fourth hand-rolled copy of that one line here is how
    the repo ended up with two conventions in the first place. If dbdiagram ever diverges
    from Spark, this function is where it diverges, and nothing else moves.
    """
    return sql_text.escape(text)


def tables(model: spec.Model) -> list[dict]:
    """[{schema, table, entity, kind, domain, columns, pk, refs, masks}] for every declared table.

    EVERY DERIVATION COMES FROM src/accelerator/contract.py, which is the module whose whole
    job is exactly these questions -- column type, classification, uniqueness grain, foreign
    keys. Nothing here re-derives them. This repo has been bitten three times by two
    derivations of one fact (BUSINESS_KINDS, the system-column set, RECONCILABLE_KINDS), and a
    diagram that disagreed with the published data contract about what a table holds would be
    the same defect in a new place.

    The column ORDER comes from emit_data_contract._projected_columns, the same walk the
    contracts are built from, so the diagram lists columns as the contract does.
    """
    # WHICH SCHEMA EACH TABLE LIVES IN, so a Ref target is derived rather than assumed.
    # Measured 3 Sep: every FK target is in raw_vault today, because `csat` is the only
    # business-vault kind and a csat is never a parent. Hardcoding raw_vault would therefore
    # be correct AND would silently point at the wrong schema the day that changes -- the
    # shape of latent defect this repo keeps finding. Derived instead, and asserted.
    #
    # Keyed on stable_tables(), not tables(): contract.foreign_keys() names its target
    # `{parent_base_table}.{parent_hash_key}` -- the unversioned name -- and this dict is
    # looked up by that same key below. Keying it on the physical (_rev<N>) name would
    # make every lookup miss.
    schema_of = {t: naming.vault_schema_for(e.kind, RAW_SCHEMA, BUSINESS_SCHEMA)
                 for e in model.entities for _s, t in e.stable_tables()}

    out = []
    for entity in model.entities:
        # THE VIEW, NOT THE TABLE. A diagram is read by people, and they bind to the
        # name they are given. The stable name survives a cutover; the physical one is
        # what the cutover moves -- see naming.stable().
        for src, table in entity.stable_tables():
            columns = _projected_columns(entity, src)
            grain = set(contract.grain(entity, columns))
            fks = contract.foreign_keys(entity, model)
            out.append({
                "schema": naming.vault_schema_for(entity.kind, RAW_SCHEMA, BUSINESS_SCHEMA),
                "table": table,
                "entity": entity.name,
                "kind": entity.kind,
                "domain": getattr(entity, "domain", "") or "unassigned",
                "columns": [
                    (c, contract.column_type(entity, src, c), contract.classify(entity, c),
                     contract.description(entity, c, model))
                    for c in columns
                ],
                "grain": grain,
                "refs": [(col, schema_of.get(target.split(".")[0], RAW_SCHEMA), target)
                         for col, target in sorted(fks.items()) if col in columns],
                "masks": dict(getattr(entity, "masks", {}) or {}),
                "sensitivity": getattr(entity, "sensitivity", "") or "",
            })
    return sorted(out, key=lambda t: (t["schema"], t["table"]))


def render(entries: list[dict]) -> str:
    """The DBML document: a Table block per table, then every Ref, then TableGroups."""
    n_cols = sum(len(e["columns"]) for e in entries)
    n_refs = sum(len(e["refs"]) for e in entries)
    lines = [
        "// GENERATED from metadata/entities/ by tools/emit_dbml_diagram.py.",
        "// Do not hand-edit: verify_repo.py fails the build if regenerating this differs.",
        "//",
        "// Paste into https://dbdiagram.io to explore. Hubs are conformed business concepts,",
        "// links and NHLs are relationships between them, satellites carry attributes over",
        "// time. Every arrow is a parent declared in the model, not a guess.",
        "//",
        f"// {len(entries)} tables, {n_cols} columns, {n_refs} relationships.",
        "",
    ]
    for e in entries:
        note = f"{e['kind']} in the {e['domain']} domain"
        if e["sensitivity"]:
            note += f"; declared sensitivity {e['sensitivity']}"
        lines.append(f"Table {e['schema']}.{e['table']} {{")
        for col, ctype, classification, desc in e["columns"]:
            attrs = ["pk"] if col in e["grain"] else []
            # THE DESCRIPTION LEADS, because it is the only part of this note a reader
            # opening the diagram actually wants: what the column MEANS. Classification and
            # mask come after -- they are properties OF the column, and reading them first
            # is what made the old note say nothing on 409 columns out of 409.
            #
            # Absent on the 158 business columns nobody has described yet, and absent is
            # correct: the alternative is a sentence restating the column's own name, which
            # verify_repo refuses at source.
            cnotes = [desc] if desc else []
            cnotes.append(f"classification {classification}")
            if ctype == "source-derived":
                cnotes.append("type read from the bound bronze column at load time, not "
                              "declared in the model")
            if col in e["masks"]:
                cnotes.append(f"MASKED by {e['masks'][col]}")
            attrs.append(f"note: '{_esc('; '.join(cnotes))}'")
            lines.append(f"  {col} {dbml_type(ctype)} [{', '.join(attrs)}]")
        lines.append(f"  Note: '{_esc(note)}'")
        lines.append("}")
        lines.append("")

    # EVERY FOREIGN KEY AS A REAL EDGE. This is the half Databricks' own industry models leave
    # in prose, and the half a vault diagram exists for.
    lines.append("// Relationships, from contract.foreign_keys() -- the same derivation the")
    lines.append("// published data contracts use.")
    for e in entries:
        for col, target_schema, target in e["refs"]:
            lines.append(f"Ref: {e['schema']}.{e['table']}.{col} > {target_schema}.{target}")
    lines.append("")

    by_domain: dict = {}
    for e in entries:
        by_domain.setdefault(e["domain"], []).append(f"{e['schema']}.{e['table']}")
    lines.append("// Grouped by the domain each entity declares, so the diagram opens on")
    lines.append("// something legible rather than 25 tables in a row.")
    for domain in sorted(by_domain):
        lines.append(f"TableGroup {domain} {{")
        for t in sorted(by_domain[domain]):
            lines.append(f"  {t}")
        lines.append("}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    model = spec.load_model(ROOT / "metadata" / "entities")
    entries = tables(model)
    OUT_PATH.parent.mkdir(exist_ok=True)
    OUT_PATH.write_text(render(entries), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)} -- {len(entries)} table(s), "
          f"{sum(len(e['refs']) for e in entries)} relationship(s)")


if __name__ == "__main__":
    main()
