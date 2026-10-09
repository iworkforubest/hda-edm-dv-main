"""Archify intermediate representation, emitted FROM the model rather than described to it.

WHY THIS EXISTS AND WHAT IT DELIBERATELY DOES NOT DO. Archify is an agent skill: its own
workflow is "the agent creates typed JSON IR from your description", and it then compiles
that JSON deterministically into HTML/SVG. The determinism is in the RENDER, not in the
derivation -- so a diagram can be beautifully drawn and disagree with the system it claims
to describe, with nothing to catch it.

That is the failure this repo spent 5 September removing from its own artefacts. Every
diagram here is generated from `metadata/entities/` and fails the build the moment it
disagrees with the model, and this module keeps that property: it skips the describing
step entirely. The components come from `entity.stable_tables()` (the view a consumer
reads, not the physical `_rev<N>` table a cutover renames), the connections from
`contract.foreign_keys` -- the same authority the DBML diagram reads, and which already
resolves a parent through its unversioned base_table -- and the job graph from
`resources/vault_job.yml`. Nobody writes an Archify document by hand, so nobody can write
a wrong one.

WHAT 6 SEPTEMBER CHANGED, AND WHY. Until the renderer was actually installed, this module
validated its output against the vendored JSON Schemas and stopped there -- and the
schemas are not the contract. `dataflow.schema.json` bounds `row` at `minimum: 0` with no
maximum; the dataflow renderer has exactly five rows per stage and refuses a sixth. So
every document this module emitted passed its own gate and could not be drawn: seven of
twenty-nine nodes landed at rows 5..12 and produced non-finite coordinates. A checker
whose stated guarantee exceeds what it can detect is the defect this repo has now found
six times in its own code, and this is the sixth.

Two things follow, and both are load-bearing:

  * THE RENDERER IS THE AUTHORITY, not the schema. `checks/../tools/archify_render_gate.py`
    runs Archify's own `validate` over every committed document. `validate()` below is
    kept, but it is now honestly described: a shape check that runs everywhere, in front
    of a real check that runs where Node and the skill are installed.

  * THE DIAGRAM TYPE IS `architecture`, NOT `dataflow`. Dataflow's geometry is fixed --
    five rows, 215px between stage centres, 112px nodes -- and a vault whose table names
    run to thirty-four characters cannot be laid out inside it without truncating names.
    Architecture's grid takes `cellW`, `gapX`, `gapY` and per-component `size`, so the
    geometry is MEASURED FROM THE CONTENT rather than fitted to a constant. Nothing is
    truncated and nothing is invented.

ONE DOCUMENT PER DOMAIN. The vault's foreign keys form one connected graph -- hub_
organisation touches nearly everything -- so there is no natural seam to cut, and a single
29-table picture is unreadable whatever renderer draws it. The split is therefore by
`entity.domain`, which is declared in the model rather than chosen here, and every edge
that leaves a domain is kept: the table at the far end is drawn as a `external` context
component. No edge is dropped, so no document lies by omission. Completeness across
domains is the DBML ERD's job, and it still has it.

NOTHING HERE RUNS ARCHIFY. We emit the IR and the gate calls the renderer's validator.
Producing HTML from these documents is a separate decision this repo does not make.

WHAT IS INVENTED, AND WHY IT CANNOT BE WRONG. `col`, `row`, `size`, and the grid metrics
are layout, and Archify requires them. `col` is the table's distance from a hub, `row` an
ordinal within a column, and `size` is computed from the label's own length -- so each is
a function of the model, not a judgement about it. The one heuristic is the ordering
WITHIN a column (a barycentre pass that puts a table near its parents); it can produce an
ugly diagram, never a false one, because it moves boxes and no edges.
"""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from pathlib import Path

from . import naming

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "vendor" / "archify" / "schemas"

# WHAT ARCHIFY VERSION THESE WERE MEASURED AGAINST, exact rather than rounded. Not
# decoration, and now load-bearing twice over: the constants below mirror the renderer's
# own text estimator, so a renderer that changes its font metrics changes what fits -- and
# the committed pages in diagram/html/ are byte-gated against a fresh render, which is
# only deterministic for a fixed version. verify_repo compares this string to the
# installed skill's skill-release.json and stands the byte-gate down when they differ,
# rather than failing the build for a reason that has nothing to do with this repo.
RENDERER_VERSION = "2.17.0-dev.1"

# THE RENDERER'S TEXT ESTIMATOR, MIRRORED -- and this is the one hand-typed copy in the
# module, so it is worth being exact about the risk and the mitigation.
#
# render-architecture.mjs sizes a component label at roughly 6.6px per character and
# render-workflow.mjs at 6.8, then REFUSES a label wider than its box. We cannot read
# those numbers out of the renderer without parsing its source, so they are copied -- the
# drift this repo keeps finding. The mitigation is that drift is not silent here:
# tools/archify_render_gate.py asks the actual renderer, so a changed estimator fails the
# build with the exact label it can no longer fit. The margins below are deliberately
# generous for the same reason -- a small upstream change stays inside them.
ARCH_LABEL_PX = 6.6
ARCH_SUBLABEL_PX = 5.4
WORKFLOW_LABEL_PX = 6.8

# OUR OWN GRID, and every number is a floor rather than a fit. The renderer solves the
# rest; these only have to leave it room.
#
# BOX_H AND GRID_GAP_Y WERE MEASURED DOWN, not chosen. At 76/40 the tallest document
# (`hfig_job`, nine rows off one hub) had to be scaled so far to fit a 1440x900 desktop
# that `archify visual-check` failed it on `viewer/projected-text-readability` -- the
# sublabels went below the renderer's legibility floor. 68/28 is the most generous pitch
# at which every document passes the browser check outright; 64/24 and 60/20 also pass and
# buy nothing.
GRID_ORIGIN = (60, 108)
GRID_GAP_Y = 28
BOX_MIN_W = 120
BOX_H = 68
BOX_PAD = 20
CONNECTION_LABEL_PAD = 96
CANVAS_PAD_X = 60
CANVAS_PAD_BOTTOM = 80
MIN_VIEWBOX = 360

# THE CANVAS IS AT LEAST THIS MUCH WIDER THAN IT IS TALL, and the number was measured
# rather than chosen. Archify's viewer scales the authored viewBox up to the reading
# width, so a tall narrow canvas is magnified until two rows fill a laptop screen and the
# rest is below the fold -- `hfig_job` at 614x1192 rendered its first two boxes at
# 260x170px each and needed 2771px of scroll on a 1440x900 desktop. The renderer's own
# browser check (`archify visual-check`) fails that, and four of the six documents failed
# it on 6 Sep. At 1.6 all six fit, which is where this floor comes from; 1.4 was measured
# and three still failed.
#
# IT MOVES NO BOX RELATIVE TO ANOTHER. The padding goes into the canvas and the grid
# origin is re-centred in it, so the layout the render gate approved is the layout that
# ships -- this changes the frame, never the picture.
MIN_ASPECT = 1.6

# THE DV LAYER EACH KIND BELONGS TO, DERIVED FROM naming's kind sets rather than retyped.
# A new kind added to naming.LINK_KINDS lands in "Links" without anyone remembering to
# come here -- which is the failure mode this mapping used to have.
_LAYERS = (
    ("Hubs", frozenset({"hub"})),
    ("Links", naming.LINK_KINDS),
    ("Satellites", naming.RAW_SATELLITE_KINDS),
    ("Business vault", naming.BUSINESS_KINDS),
)


def load_schema(diagram_type: str) -> tuple[dict, dict]:
    """(schema, common) for one diagram type, read from the vendored copies.

    VENDORED, NOT FETCHED. verify_repo must run offline from an extracted zip; a gate that
    reaches the network is a gate that passes when the network is down.
    """
    schema = json.loads((SCHEMA_DIR / f"{diagram_type}.schema.json").read_text("utf-8"))
    common = json.loads((SCHEMA_DIR / "common.schema.json").read_text("utf-8"))
    return schema, common


def _resolve(node: dict, common: dict) -> dict:
    """One level of `$ref` into common.schema.json. Enough for the fields we emit."""
    ref = node.get("$ref", "")
    if ref.startswith("common.schema.json#/$defs/"):
        return common["$defs"][ref.rsplit("/", 1)[-1]]
    return node


def validate(doc: dict, diagram_type: str) -> list[str]:
    """Findings against the VENDORED schema. Empty means well-FORMED, not renderable.

    READ THE SECOND SENTENCE OF THE MODULE DOCSTRING BEFORE TRUSTING THIS. Passing here
    says the document has the right keys with the right kinds of values. It says nothing
    about whether Archify can draw it, because the schemas do not describe the renderer's
    geometry: `row` is `minimum: 0` in dataflow.schema.json and 0..4 in the renderer, and
    a label that does not fit its box is a hard render error the schema cannot express.
    On 6 September this function passed two documents the renderer rejected outright.

    IT IS KEPT ANYWAY, for one reason: it needs nothing but Python, so it runs in CI and
    on a stripped machine where Node and the Archify skill are absent. It is the cheap
    check in front of the real one. The real one is tools/archify_render_gate.py.

    DRIVEN BY THEIR SCHEMA, NOT BY A SECOND OPINION OF IT. Every rule below is read out of
    the JSON Schema file rather than restated here: `required`, `const`, `enum`,
    `additionalProperties: false`, and `pattern` on ids. A hand-written list of "the fields
    Archify wants" would be a second authority that drifts from the first.

    IT IS NOT A FULL JSON SCHEMA VALIDATOR and does not pretend to be -- no `oneOf`, no
    numeric bounds, no nested `$ref` chains. Install `jsonschema` and run
    `tools/emit_archify.py --strict` for the complete schema check.
    """
    import re

    schema, common = load_schema(diagram_type)
    out: list[str] = []

    def check_object(obj, spec, where):
        spec = _resolve(spec, common)
        if "const" in spec and obj != spec["const"]:
            out.append(f"{where}: {obj!r} but the schema requires {spec['const']!r}")
            return
        if "enum" in spec and obj not in spec["enum"]:
            out.append(f"{where}: {obj!r} not one of {spec['enum']}")
            return
        if "pattern" in spec and isinstance(obj, str) and not re.match(spec["pattern"], obj):
            out.append(f"{where}: {obj!r} does not match {spec['pattern']}")
            return
        if spec.get("type") == "object" and isinstance(obj, dict):
            for key in spec.get("required", []):
                if key not in obj:
                    out.append(f"{where}: missing required key {key!r}")
            props = spec.get("properties", {})
            if spec.get("additionalProperties") is False:
                for key in obj:
                    if key not in props:
                        out.append(f"{where}: unexpected key {key!r} -- the schema sets "
                                   f"additionalProperties: false, so this fails rather "
                                   f"than being ignored")
            for key, value in obj.items():
                if key in props:
                    check_object(value, props[key], f"{where}.{key}")
        if spec.get("type") == "array" and isinstance(obj, list):
            item = spec.get("items")
            if item:
                for i, value in enumerate(obj):
                    check_object(value, item, f"{where}[{i}]")

    check_object(doc, schema, diagram_type)
    return out


# --------------------------------------------------------------------------- #
# The vault as a graph. Everything the architecture documents draw comes from here.


def _tables(model) -> dict:
    """table name -> (entity, source binding or None), for every table the model emits.

    KEYED ON stable_tables(), THE VIEW A CONSUMER READS, not tables()'s physical
    _rev<N> name. This diagram is exactly the kind of consumer-facing artefact Task 7
    exists for, and contract.foreign_keys() (below) already resolves an edge's target
    through the parent's unversioned base_table -- keying components on the physical
    name would make every edge dangle against a component id it can never match.
    """
    return {table: (entity, src)
            for entity in model.entities for src, table in entity.stable_tables()}


def _connections(model, contract) -> list[dict]:
    """One entry per foreign key, both ends resolved to drawn tables.

    A LINK SATELLITE'S GRANDPARENT IS NOT A TABLE OF ITS OWN in some shapes, so the edge
    to it would dangle. Archify does not reject an edge to an unknown id, which is
    precisely why it is filtered here: a connection pointing at nothing draws a line into
    empty space, and a reader cannot tell that from a real relationship.
    """
    tables = _tables(model)
    out = []
    for entity in sorted(model.entities, key=lambda e: e.name):
        for _src, table in entity.stable_tables():
            for column, ref in sorted(contract.foreign_keys(entity, model).items()):
                parent = ref.rsplit(".", 1)[0]
                if parent in tables and table in tables:
                    out.append({"from": parent, "to": table, "label": column})
    return out


def _depths(tables: dict, connections: list[dict]) -> dict:
    """Distance from a hub, as the LONGEST path in -- the column each table is drawn in.

    WHY LONGEST AND NOT SHORTEST. A link satellite hangs off a link that hangs off a hub.
    Shortest-path would put it in column 1 next to the link it depends on, and the edge
    between them would run backwards. Longest-path guarantees every edge points forward,
    which is what lets the renderer route them without crossing an unrelated box -- the
    error class that made the kind-based staging unusable.
    """
    parents = defaultdict(set)
    for c in connections:
        parents[c["to"]].add(c["from"])

    depth: dict[str, int] = {}

    def depth_of(table: str, seen: frozenset = frozenset()) -> int:
        if table in depth:
            return depth[table]
        if table in seen:                    # a cycle would recurse for ever
            return 0
        up = [p for p in parents[table] if p in tables]
        depth[table] = 0 if not up else 1 + max(depth_of(p, seen | {table}) for p in up)
        return depth[table]

    for table in tables:
        depth_of(table)
    return depth


def _layer(kind: str) -> str:
    for label, kinds in _LAYERS:
        if kind in kinds:
            return label
    return kind


def _box_width(label: str, sublabel: str) -> int:
    """Wide enough for the renderer's own estimate of both lines, plus padding."""
    return max(BOX_MIN_W, math.ceil(max(len(label) * ARCH_LABEL_PX,
                                        len(sublabel) * ARCH_SUBLABEL_PX)) + BOX_PAD)


def domains(model) -> tuple[str, ...]:
    """The domains that get a document, read off the model rather than listed here."""
    return tuple(sorted({entity.domain for entity in model.entities}))


def architecture(model, contract, domain: str) -> dict:
    """One domain of the vault as an Archify architecture document."""
    tables = _tables(model)
    all_connections = _connections(model, contract)
    depth = _depths(tables, all_connections)

    own = {t for t, (entity, _) in tables.items() if entity.domain == domain}
    connections = [c for c in all_connections if c["from"] in own or c["to"] in own]
    context = {end for c in connections for end in (c["from"], c["to"])} - own
    drawn = sorted(own | context)

    def sublabel(table: str) -> str:
        entity, src = tables[table]
        return f"{entity.kind} · {src.name.lower()}" if src else entity.kind

    # COLUMNS ARE THE DEPTHS PRESENT, COMPACTED. A domain whose tables are only at depths
    # 0 and 2 draws two columns, not three with a gap -- an empty column reads as a
    # missing stage, which is a claim we would be making by accident.
    columns = sorted({depth[t] for t in drawn})
    column_of = {d: i for i, d in enumerate(columns)}

    placed: dict[str, int] = {}
    components: list[dict] = []
    for d in columns:
        group = sorted(t for t in drawn if depth[t] == d)

        def barycentre(table: str) -> tuple[float, str]:
            rows = [placed[c["from"]] for c in connections
                    if c["to"] == table and c["from"] in placed]
            return (sum(rows) / len(rows) if rows else 0.0, table)

        group.sort(key=barycentre)
        for row, table in enumerate(group):
            placed[table] = row
            entity, _ = tables[table]
            label, sub = entity.name, sublabel(table)
            components.append({
                "id": table,
                # `external` is the renderer's word for "drawn for context". Here it means
                # the table belongs to another domain's document, never that it is outside
                # the vault.
                "type": "external" if table in context else "database",
                "label": label,
                "sublabel": sub,
                "tag": entity.sensitivity,
                "col": column_of[d],
                "row": row,
                "size": [_box_width(label, sub), BOX_H],
            })

    # A CONNECTION LABEL IS THE FOREIGN KEY COLUMN, AND MOST OF THEM SAY NOTHING. An edge
    # from hub_operating_company to nhl_payroll_detail is joined on
    # `operating_company_hk`, which both endpoints already state. Drawing it costs a
    # label's width of clearance in every gutter and tells the reader nothing. A ROLED
    # key is the opposite: `parent_legal_entity_hk` and `child_legal_entity_hk` are the
    # only thing distinguishing two edges between the same pair of boxes, so those are
    # always drawn.
    def implied(c: dict) -> bool:
        return c["label"] == f"{tables[c['from']][0].name}_hk"

    parallel = Counter((c["from"], c["to"]) for c in connections)
    drawn_connections = []
    for c in connections:
        out = {"from": c["from"], "to": c["to"]}
        if not implied(c):
            out["label"] = c["label"]
            # PARALLEL EDGES GET THEIR LABELS OFF THE STUB. When two connections join the
            # same pair the renderer spreads their ports, and the first segment is a short
            # stub against the source box -- where an automatic label lands on top of it.
            # Segment 2 is the run across the gutter, which is clear by construction.
            if parallel[(c["from"], c["to"])] > 1:
                out["labelSegment"] = 2
        drawn_connections.append(out)

    cell_w = max(component["size"][0] for component in components)
    widest_label = max((len(c["label"]) for c in drawn_connections if "label" in c),
                       default=0)
    gap_x = math.ceil(widest_label * ARCH_LABEL_PX) + CONNECTION_LABEL_PAD
    rows = max(component["row"] for component in components) + 1

    content_w = len(columns) * cell_w + (len(columns) - 1) * gap_x
    height = max(MIN_VIEWBOX,
                 GRID_ORIGIN[1] + rows * BOX_H + (rows - 1) * GRID_GAP_Y + CANVAS_PAD_BOTTOM)
    width = max(MIN_VIEWBOX, CANVAS_PAD_X * 2 + content_w, math.ceil(height * MIN_ASPECT))
    origin = [round((width - content_w) / 2), GRID_ORIGIN[1]]

    return {
        "schema_version": 1,
        "diagram_type": "architecture",
        "meta": {
            "title": f"HFIG Data Vault — {domain}",
            "subtitle": "Generated from metadata/entities/. Never hand-authored.",
            "viewBox": [width, height],
        },
        "layout": {
            "mode": "grid",
            "origin": origin,
            "cols": len(columns),
            "gapX": gap_x,
            "gapY": GRID_GAP_Y,
            "cellW": cell_w,
            "cellH": BOX_H,
        },
        "components": components,
        "connections": drawn_connections,
    }


def workflow(job: dict) -> dict:
    """The vault load job as an Archify workflow document, from resources/vault_job.yml.

    SCHEMA VERSION 2, AND THE VERSION IS THE POINT. Version 1 is a fixed geometry: six
    column centres at hard-coded x positions, 92px nodes, and a refusal for anything
    wider. Every task key in this job is wider than 92px, so v1 could not draw a single
    one of them. Version 2's compiler measures the document and solves the geometry, which
    is why the only thing this function has to get right is the graph.

    LANES ARE THE PHASE OF THE LOAD, derived from the task's own dependency depth rather
    than from a hand-kept list: a task's lane is one past the deepest lane it depends on.
    So a task inserted into the middle of the graph moves itself, and every task after it.
    """
    tasks = job["resources"]["jobs"]["vault_load"]["tasks"]
    deps = {
        t["task_key"]: [d["task_key"] for d in (t.get("depends_on") or [])]
        for t in tasks
    }

    depth: dict[str, int] = {}

    def depth_of(key: str, seen: frozenset = frozenset()) -> int:
        if key in depth:
            return depth[key]
        if key in seen:                      # a cycle would recurse for ever
            return 0
        parents = [d for d in deps.get(key, []) if d in deps]
        depth[key] = 0 if not parents else 1 + max(
            depth_of(p, seen | {key}) for p in parents)
        return depth[key]

    for key in deps:
        depth_of(key)

    lanes_used = sorted(set(depth.values()))
    nodes, col = [], {}
    for key in sorted(deps, key=lambda k: (depth[k], k)):
        lane = depth[key]
        col[lane] = col.get(lane, -1) + 1
        # A gate asserts; everything else acts. `assert_` is this repo's own prefix for the
        # first, so the split is read from the task name rather than kept as a list.
        nodes.append({
            "id": key,
            "lane": f"lane{lane}",
            "col": col[lane],
            "type": "security" if key.startswith("assert_") else "backend",
            "label": key,
            # MEASURED, NOT DEFAULTED. The renderer's 92px default refuses every one of
            # these labels; v2 will honour a wider box but will not invent one.
            "width": max(92, math.ceil(len(key) * WORKFLOW_LABEL_PX) + 8),
        })

    edges = [{"from": parent, "to": key}
             for key in sorted(deps) for parent in sorted(deps[key]) if parent in deps]

    return {
        "schema_version": 2,
        "diagram_type": "workflow",
        "meta": {
            "title": "vault_load — task graph",
            "subtitle": "Generated from resources/vault_job.yml. Never hand-authored.",
        },
        # NO viewBox. v2's compiler uses intrinsic measured bounds, and an explicit box
        # here would be a number we invented for it to be constrained by.
        "lanes": [{"id": f"lane{i}", "label": f"Stage {i}"} for i in lanes_used],
        "nodes": nodes,
        "edges": edges,
    }
