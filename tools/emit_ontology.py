"""Render the declared vault model as an RDF/OWL ontology in Turtle.

GENERATED, NEVER HAND-EDITED. verify_repo.py asserts regenerating produces a byte-identical
file, so the diff is the review -- the same discipline data_contracts/, control_contracts/,
dashboards/ and diagram/ already carry.

WHY A VAULT MAPS ONTO OWL BETTER THAN A NORMALISED SCHEMA DOES. Databricks' industry data
models ship an RDF ontology for "semantic-tooling integration and AI-agent grounding"; read on
3 Sep, theirs is tables-as-rdfs:Class nested by subClassOf, with rdfs:label and rdfs:comment
and NOTHING else -- no rdfs:domain, no rdfs:range, no owl:ObjectProperty, no classification.
Relationships survive only as English prose inside comments, which a tool cannot follow.

A Data Vault already separates the three things an ontology wants separated, so the mapping is
structural rather than interpretive:

    hub                -> owl:Class            a conformed business concept
    link / nhl / hal   -> owl:Class            PLUS one owl:ObjectProperty per parent
    satellite payload  -> owl:DatatypeProperty on the concept it describes
    masks, sensitivity -> annotation properties

WHY LINKS ARE REIFIED RATHER THAN BEING PROPERTIES. An owl:ObjectProperty is binary, and
journal_line has FOUR parents (accounting_journal, organisation, ledger_account, pay_period).
An n-ary relationship cannot be one property, so the link becomes a Class with a property per
parent -- the standard OWL reification for n-ary relations, and it happens to be exactly what
a link already is in Data Vault: an entity with its own key.

WHY SATELLITE ATTRIBUTES HANG OFF THE PARENT, NOT OFF THE SATELLITE. Physically a satellite is
a separate historised table. Semantically its payload describes the hub or link it hangs from:
what a consumer or an agent wants to know is that an Organisation HAS a name, not that a
sat_organisation_details row has one. So each payload column becomes a DatatypeProperty whose
rdfs:domain is the PARENT's class, annotated with the satellite it came from so the
historisation is not lost -- see dv:sourceTable.

WHAT IS DELIBERATELY NOT ASSERTED. 152 of 369 columns take their type from the bound bronze
column at load time rather than declaring one. Those get NO rdfs:range: inventing xsd:string
would be asserting something the model does not know, and an ontology that guesses is worse
than one that is silent. They carry dv:typeUndeclared instead.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

from accelerator import contract, naming, spec  # noqa: E402
from emit_data_contract import _projected_columns  # noqa: E402

OUT_PATH = ROOT / "onto" / "hfig_data_vault.ttl"

BASE = "https://hfig.group/ontology/data-vault#"

# SQL type -> XSD. Every type the model actually produces is mapped; `source-derived` is
# absent on purpose, because it has no type to map. A KeyError here would mean the model grew
# a type nobody decided the semantics of, which should stop the build rather than default.
XSD = {
    "BINARY(32)": "xsd:hexBinary",
    "DECIMAL(18,2)": "xsd:decimal",
    "DOUBLE": "xsd:double",
    "INT": "xsd:integer",
    "STRING": "xsd:string",
    "TIMESTAMP": "xsd:dateTime",
}
UNDECLARED = "source-derived"


def class_name(entity_name: str) -> str:
    """A business concept's class name: PascalCase, from the entity name."""
    return "".join(part.capitalize() for part in entity_name.split("_"))


def property_name(prefix: str, column: str) -> str:
    """A property name: camelCase, prefixed so two concepts may share a column name.

    WITHOUT THE PREFIX THE ONTOLOGY WOULD SILENTLY MERGE PROPERTIES. `reference` appears on
    more than one entity, and one dv:reference with two different rdfs:domain declarations is
    read by a reasoner as a property whose domain is the INTERSECTION -- which is neither
    concept, and is wrong in a way nothing would flag.
    """
    parts = column.split("_")
    camel = parts[0] + "".join(p.capitalize() for p in parts[1:])
    return f"{prefix}_{camel}"


def _lit(text: str) -> str:
    """A Turtle string literal. A quote or backslash in a business term would break it."""
    return '"' + str(text).replace("\\", "\\\\").replace('"', '\\"') + '"'


def build(model: spec.Model) -> dict:
    """{classes, object_properties, datatype_properties} from the declared model."""
    by_name = {e.name: e for e in model.entities}
    classes, obj_props, data_props = [], [], []

    for entity in model.entities:
        if entity.kind in naming.KEYED_KINDS:
            classes.append({
                "name": class_name(entity.name),
                "entity": entity.name,
                "kind": entity.kind,
                "domain": getattr(entity, "domain", "") or "unassigned",
                "sensitivity": getattr(entity, "sensitivity", "") or "",
                # A link is a reified n-ary relationship; a hub is a concept. Said in the
                # ontology so a reader knows which they are looking at.
                "is_relationship": entity.kind != "hub",
            })
            # THE ROLE IS PART OF THE PROPERTY NAME. A hierarchical link relates one
            # class to itself twice; two properties both called has_legalEntity would be
            # one property in RDF, and the hierarchy would lose its direction entirely --
            # which is the only thing a hierarchy carries.
            for parent, role in spec.parent_legs(entity):
                if parent in by_name:
                    obj_props.append({
                        "name": property_name(class_name(entity.name)[0].lower()
                                              + class_name(entity.name)[1:],
                                              f"has_{role}_{parent}" if role
                                              else f"has_{parent}"),
                        "domain": class_name(entity.name),
                        "range": class_name(parent),
                        "entity": entity.name,
                        "parent": parent,
                        "role": role,
                    })

    for entity in model.entities:
        # A satellite's payload describes its PARENT concept. A satellite declares exactly one
        # parent -- the hub or link it hangs from -- which is what makes this unambiguous.
        target = None
        if entity.kind in naming.SATELLITE_KINDS:
            target = entity.parents[0] if entity.parents else None
        elif entity.kind in naming.KEYED_KINDS:
            target = entity.name
        if target is None or target not in by_name:
            continue
        for src, table in entity.tables():
            for col in _projected_columns(entity, src):
                if col in naming.SYSTEM_COLUMNS or col.endswith("_hk") or col == "hashdiff":
                    continue
                ctype = contract.column_type(entity, src, col)
                data_props.append({
                    "name": property_name(class_name(target)[0].lower()
                                          + class_name(target)[1:], col),
                    "domain": class_name(target),
                    "range": XSD.get(ctype) if ctype != UNDECLARED else None,
                    "sql_type": ctype,
                    "column": col,
                    "source_table": table,
                    "classification": contract.classify(entity, col),
                    "comment": contract.description(entity, col, model),
                    # masks is a tuple of (column, function) pairs, not a mapping.
                    "mask": dict(getattr(entity, "masks", ()) or ()).get(col),
                })

    # A property may be derived once per source table; the same (name, domain) is one property.
    seen, unique = set(), []
    for pr in data_props:
        key = (pr["name"], pr["domain"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(pr)
    return {"classes": sorted(classes, key=lambda c: c["name"]),
            "object_properties": sorted(obj_props, key=lambda p: (p["domain"], p["name"])),
            "datatype_properties": sorted(unique, key=lambda p: (p["domain"], p["name"]))}


def render(onto: dict) -> str:
    c, o, d = onto["classes"], onto["object_properties"], onto["datatype_properties"]
    L = [
        "# GENERATED from metadata/entities/ by tools/emit_ontology.py.",
        "# Do not hand-edit: verify_repo.py fails the build if regenerating this differs.",
        "#",
        "# The HFIG Data Vault as an OWL ontology. Hubs are business concepts; links and",
        "# non-historised links are reified n-ary relationships between them; satellite",
        "# payload becomes datatype properties on the concept each satellite describes.",
        "#",
        f"# {len(c)} classes, {len(o)} object properties, {len(d)} datatype properties.",
        "",
        "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix owl:  <http://www.w3.org/2002/07/owl#> .",
        "@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .",
        f"@prefix dv:   <{BASE}> .",
        "",
        f"<{BASE.rstrip('#')}> rdf:type owl:Ontology ;",
        '    rdfs:label "HFIG Data Vault" ;',
        '    rdfs:comment "Generated from the declarative model in metadata/entities/. '
        'Hubs are concepts, links are reified relationships, satellite payload becomes '
        'datatype properties on the concept described." .',
        "",
        "# Annotation properties. These carry the facts a plain RDFS class hierarchy loses:",
        "# which physical table an attribute came from, whether it is masked, and how it is",
        "# classified.",
        "dv:sourceTable    rdf:type owl:AnnotationProperty ; rdfs:label \"source table\" .",
        "dv:sqlType        rdf:type owl:AnnotationProperty ; rdfs:label \"SQL type\" .",
        "dv:classification rdf:type owl:AnnotationProperty ; rdfs:label \"classification\" .",
        "dv:maskFunction   rdf:type owl:AnnotationProperty ; rdfs:label \"mask function\" .",
        "dv:vaultKind      rdf:type owl:AnnotationProperty ; rdfs:label \"vault kind\" .",
        "dv:businessDomain rdf:type owl:AnnotationProperty ; rdfs:label \"business domain\" .",
        "dv:typeUndeclared rdf:type owl:AnnotationProperty ; rdfs:label \"type undeclared\" .",
        "",
        "# ---------------------------------------------------------------- concepts",
    ]
    for cl in c:
        role = ("a reified relationship between concepts; it carries its own key"
                if cl["is_relationship"] else "a conformed business concept")
        L.append(f"dv:{cl['name']} rdf:type owl:Class ;")
        L.append(f"    rdfs:label {_lit(cl['entity'].replace('_', ' '))} ;")
        L.append(f"    rdfs:comment {_lit(role)} ;")
        L.append(f"    dv:vaultKind {_lit(cl['kind'])} ;")
        if cl["sensitivity"]:
            L.append(f"    dv:classification {_lit(cl['sensitivity'])} ;")
        L.append(f"    dv:businessDomain {_lit(cl['domain'])} .")
        L.append("")

    L.append("# ------------------------------------------------------- relationships")
    L.append("# One object property per declared parent. A link with four parents becomes four")
    L.append("# properties on one class, which is how OWL expresses an n-ary relation.")
    for pr in o:
        L.append(f"dv:{pr['name']} rdf:type owl:ObjectProperty ;")
        L.append(f"    rdfs:label {_lit('has ' + pr['parent'].replace('_', ' '))} ;")
        L.append(f"    rdfs:domain dv:{pr['domain']} ;")
        L.append(f"    rdfs:range dv:{pr['range']} .")
        L.append("")

    L.append("# ---------------------------------------------------------- attributes")
    L.append("# Satellite payload, expressed on the concept it describes. dv:sourceTable is")
    L.append("# the historised table it actually lives in.")
    for pr in d:
        L.append(f"dv:{pr['name']} rdf:type owl:DatatypeProperty ;")
        L.append(f"    rdfs:label {_lit(pr['column'].replace('_', ' '))} ;")
        # rdfs:comment IS THE POINT OF AN ONTOLOGY, and the Genie Ontology assessment on
        # 3 Sep said so of the reference implementation: tables became labelled classes
        # with the semantics left in prose, and the labels alone carry no more than the
        # column name already does. A label of "job title" and no comment is exactly that.
        # Emitted only where something can say it -- see contract.description.
        if pr.get("comment"):
            L.append(f"    rdfs:comment {_lit(pr['comment'])} ;")
        L.append(f"    rdfs:domain dv:{pr['domain']} ;")
        if pr["range"]:
            L.append(f"    rdfs:range {pr['range']} ;")
        else:
            L.append(f"    dv:typeUndeclared {_lit('true')} ;")
        L.append(f"    dv:sqlType {_lit(pr['sql_type'])} ;")
        L.append(f"    dv:classification {_lit(pr['classification'])} ;")
        if pr["mask"]:
            L.append(f"    dv:maskFunction {_lit(pr['mask'])} ;")
        L.append(f"    dv:sourceTable {_lit(pr['source_table'])} .")
        L.append("")
    return "\n".join(L)


def main() -> None:
    model = spec.load_model(ROOT / "metadata" / "entities")
    onto = build(model)
    OUT_PATH.parent.mkdir(exist_ok=True)
    OUT_PATH.write_text(render(onto), encoding="utf-8")
    print(f"wrote {OUT_PATH.relative_to(ROOT)} -- {len(onto['classes'])} class(es), "
          f"{len(onto['object_properties'])} object propert(ies), "
          f"{len(onto['datatype_properties'])} datatype propert(ies)")


if __name__ == "__main__":
    main()
