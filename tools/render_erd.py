#!/usr/bin/env python3
"""
Render the Data Vault ERD from metadata.

GENERATED, NOT DRAWN. The diagram is built from metadata/entities/*.yml, so it cannot
drift from the model the way a hand-maintained diagram does. Add a source binding and the
fan-out appears; change a parent and the edge moves. If the diagram and the model disagree,
the diagram is stale by exactly one run of this script.

    python tools/render_erd.py                 # HTML + PDF into docs/
    python tools/render_erd.py --out-dir X     # elsewhere

Outputs
  docs/hfig_dv_erd.html   self-contained: inline SVG, legend, inventory, grain pairs
  docs/hfig_dv_erd.pdf    same document, A3 landscape

What the diagram encodes, beyond boxes and lines:
  * colour = structure type (hub / link / satellite / computed satellite)
  * a satellite drawn per SOURCE, because that is what actually gets created
  * AGGREGATE grain flagged, with a dashed reconciliation edge to its transaction-grain
    counterpart -- the pair a consumer must not mistake for one grain
  * masked columns marked, since that is a modelling fact and not just a config detail
  * the business key listed on every hub, because the key IS the hub
"""

from __future__ import annotations

import argparse
import html
import re
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import VERSION, naming, spec  # noqa: E402
from accelerator.hashing import RULEBOOK_VERSION  # noqa: E402

# blueprint palette, matching the reference deck
INK, NAVY, PANEL = "#16202B", "#0C3567", "#0A2A50"
CYAN, GREEN, GOLD, VIOLET, RED = "#35BEDB", "#1FA97A", "#D6A029", "#9B7BD4", "#E0685F"
PALE, MUTED, LINE = "#C6D9EE", "#82A4C7", "#2A6299"

KIND_COLOUR = {
    "hub": CYAN, "link": GREEN, "nhl": GREEN, "hal": GREEN, "sal": GREEN,
    "sat": GOLD, "msat": GOLD, "esat": GOLD, "csat": VIOLET,
}
KIND_LABEL = {
    "hub": "HUB", "link": "LINK", "nhl": "NHL", "hal": "HIER LINK",
    "sat": "SAT", "msat": "MULTI-ACTIVE SAT", "esat": "EFFECTIVITY SAT",
    "csat": "COMPUTED SAT",
}


def esc(text: str) -> str:
    return html.escape(str(text), quote=True)


# --------------------------------------------------------------------------- #
# DOT
# --------------------------------------------------------------------------- #
def _rows(lines: list[tuple[str, str]]) -> str:
    """HTML-like table rows for a graphviz record node."""
    out = []
    for text, colour in lines:
        out.append(
            f'<TR><TD ALIGN="LEFT" BALIGN="LEFT">'
            f'<FONT COLOR="{colour}" POINT-SIZE="9">{esc(text)}</FONT></TD></TR>'
        )
    return "".join(out)


def node(name: str, entity: spec.Entity, table: str, src) -> str:
    colour = KIND_COLOUR[entity.kind]
    masks = dict(entity.masks)
    body: list[tuple[str, str]] = []

    if entity.kind == "hub":
        body.append(("BK  " + " + ".join(entity.business_keys), PALE))
        body.append((f"key style: {entity.key_style}", MUTED))
    elif entity.kind in naming.LINK_KINDS:
        body.append(("→ " + "  ".join(entity.parents), PALE))
        if entity.transaction_key:
            body.append(("txn key  " + " + ".join(entity.transaction_key), GOLD))
    else:
        body.append((f"⊣ {entity.parents[0]}", PALE))
        if entity.mas_key:
            body.append(("multi-active on " + ", ".join(entity.mas_key), GOLD))

    payload = list(entity.payload)[:6]
    for col in payload:
        body.append((("🔒 " if col in masks else "   ") + col,
                     RED if col in masks else PALE))
    if len(entity.payload) > 6:
        body.append((f"   … {len(entity.payload) - 6} more", MUTED))

    if src is not None:
        body.append((f"source: {src.name}", CYAN))
    elif entity.sources:
        body.append((f"sources: {', '.join(s.name for s in entity.sources)}", CYAN))

    flags = []
    if entity.grain == "aggregate":
        flags.append("AGGREGATE — drops " + ", ".join(entity.aggregate_drops))
    if entity.sensitivity != "internal":
        flags.append(entity.sensitivity.upper())
    for flag in flags:
        body.append((flag, RED))

    header = (
        f'<TR><TD ALIGN="LEFT" BGCOLOR="{colour}">'
        f'<FONT COLOR="{INK}" POINT-SIZE="8"><B>{KIND_LABEL[entity.kind]}</B></FONT></TD></TR>'
        f'<TR><TD ALIGN="LEFT"><FONT COLOR="#FFFFFF" POINT-SIZE="11" FACE="Courier">'
        f'<B>{esc(table)}</B></FONT></TD></TR>'
    )
    label = (
        f'<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="3" '
        f'BGCOLOR="{PANEL}">{header}{_rows(body)}</TABLE>'
    )
    style = "dashed" if entity.grain == "aggregate" else "solid"
    return (
        f'  "{name}" [label=<{label}>, shape=box, style="rounded,{style}", '
        f'color="{colour}", penwidth=1.6];\n'
    )


def compact_node(name: str, entity: spec.Entity, table: str, external: bool = False) -> str:
    """Name-only node: for the overview, and for parent hubs shown on another domain's
    page. Detail there would compete with the domain actually being explained."""
    colour = KIND_COLOUR[entity.kind]
    style = "rounded,dashed" if entity.grain == "aggregate" else "rounded"
    fill = "#0A2A50" if not external else "#08203C"
    text_colour = "#FFFFFF" if not external else MUTED
    tag = f'  <FONT COLOR="{MUTED}" POINT-SIZE="7">({entity.domain})</FONT>' if external else ""
    label = (
        f'<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4" BGCOLOR="{fill}">'
        f'<TR><TD ALIGN="LEFT"><FONT COLOR="{text_colour}" POINT-SIZE="10" FACE="Courier">'
        f'{esc(table)}</FONT>{tag}</TD></TR></TABLE>'
    )
    return (f'  "{name}" [label=<{label}>, shape=box, style="{style}", '
            f'color="{colour}", penwidth=1.4];\n')


def _edges(model: spec.Model, ids: dict[str, str], include: set[str]) -> list[str]:
    """Structural edges, restricted to tables present on this page."""
    out: list[str] = []
    for e in model.entities:
        for _src, table in e.tables():
            if table not in include:
                continue
            if e.kind in naming.SATELLITE_KINDS:
                for _s, ptable in model.get(e.parents[0]).tables():
                    if ptable in include:
                        colour = VIOLET if e.kind == "csat" else GOLD
                        out.append(f'  "{ids[ptable]}" -> "{ids[table]}" '
                                   f'[color="{colour}", arrowhead=none];')
            elif e.kind in naming.LINK_KINDS:
                for pname in e.parents:
                    for _s, ptable in model.get(pname).tables():
                        if ptable in include:
                            out.append(f'  "{ids[ptable]}" -> "{ids[table]}" '
                                       f'[color="{GREEN}", arrowhead=none];')
    for e in model.entities:
        if e.grain != "aggregate":
            continue
        raw = model.get(e.aggregates_from)
        for _s, agg_t in e.tables():
            for _s2, raw_t in raw.tables():
                if agg_t in include and raw_t in include:
                    out.append(
                        f'  "{ids[raw_t]}" -> "{ids[agg_t]}" [style=dashed, color="{RED}", '
                        f'penwidth=1.6, arrowhead=vee, constraint=false, '
                        f'label=<<FONT COLOR="{RED}" POINT-SIZE="9"><B>reconciles</B></FONT>>];'
                    )
    return out


def _ids(model: spec.Model) -> dict[str, str]:
    return {table: table.replace("-", "_")
            for e in model.entities for _s, table in e.tables()}


def build_overview_dot(model: spec.Model) -> str:
    """All domains, names only. Answers 'what exists and how is it wired', nothing more."""
    ids = _ids(model)
    out = [
        "digraph overview {",
        # size + ratio let dot scale the whole graph to one A3 landscape content area.
        # Without it the overview spills across pages and the page break lands mid-cluster.
        f'  graph [bgcolor="{NAVY}", rankdir=LR, splines=spline, nodesep=0.3, '
        f'ranksep=1.0, size="15.0,8.6", ratio=compress, fontname="Helvetica", '
        f'compound=true, pad=0.3];',
        '  node [shape=box, fontname="Helvetica"];',
        f'  edge [color="{MUTED}", penwidth=1.1, arrowsize=0.6];',
    ]
    for i, domain in enumerate(sorted({e.domain for e in model.entities})):
        out.append(f'  subgraph cluster_{i} {{')
        out.append(f'    label=<<FONT COLOR="{CYAN}" POINT-SIZE="12"><B>'
                   f'{esc(domain.upper())}</B></FONT>>;')
        out.append(f'    color="{LINE}"; style="rounded"; penwidth=1; margin=14;')
        for e in sorted(model.entities, key=lambda x: (x.kind != "hub", x.kind, x.name)):
            if e.domain == domain:
                for _src, table in e.tables():
                    out.append("  " + compact_node(ids[table], e, table))
        out.append("  }")
    out.extend(_edges(model, ids, set(ids)))
    out.append("}")
    return "\n".join(out)


def build_domain_dot(model: spec.Model, domain: str) -> str:
    """One domain in full detail, with parent hubs from other domains as stubs."""
    ids = _ids(model)
    local = [e for e in model.entities if e.domain == domain]
    include = {t for e in local for _s, t in e.tables()}

    external: dict[str, spec.Entity] = {}
    for e in local:
        for pname in e.parents:
            parent = model.get(pname)
            if parent.domain != domain:
                external[pname] = parent
        if e.grain == "aggregate":
            raw = model.get(e.aggregates_from)
            if raw.domain != domain:
                external[raw.name] = raw
    for parent in external.values():
        for _s, t in parent.tables():
            include.add(t)

    sat_count = sum(
        len(e.tables()) for e in local if e.kind in naming.SATELLITE_KINDS
    )
    # More than four sibling satellites laid out top-to-bottom makes the graph wider than
    # the page, and dot then scales the text down to nothing. Left-to-right stacks them.
    direction = "LR" if sat_count > 4 else "TB"
    out = [
        f"digraph {domain} {{",
        f'  graph [bgcolor="{NAVY}", rankdir={direction}, splines=ortho, nodesep=0.35, '
        f'ranksep=0.8, size="15.4,7.8", ratio=compress, fontname="Helvetica", '
        f'compound=true, pad=0.3];',
        '  node [shape=box, fontname="Helvetica"];',
        f'  edge [color="{MUTED}", penwidth=1.2, arrowsize=0.7];',
    ]
    if external:
        out.append('  subgraph cluster_ext {')
        out.append(f'    label=<<FONT COLOR="{MUTED}" POINT-SIZE="10">'
                   f'REFERENCED FROM OTHER DOMAINS</FONT>>;')
        out.append(f'    color="{MUTED}"; style="rounded,dashed"; penwidth=1; margin=12;')
        for parent in external.values():
            for _s, t in parent.tables():
                out.append("  " + compact_node(ids[t], parent, t, external=True))
        out.append("  }")
    out.append('  subgraph cluster_main {')
    out.append(f'    label=<<FONT COLOR="{CYAN}" POINT-SIZE="13"><B>'
               f'{esc(domain.upper())}</B></FONT>>;')
    out.append(f'    color="{LINE}"; style="rounded"; penwidth=1; margin=16;')
    for e in sorted(local, key=lambda x: (x.kind != "hub", x.kind, x.name)):
        for src, table in e.tables():
            out.append("  " + node(ids[table], e, table, src))
    out.append("  }")
    out.extend(_edges(model, ids, include))
    out.append("}")
    return "\n".join(out)


def build_dot(model: spec.Model) -> str:
    domains = sorted({e.domain for e in model.entities})
    out = [
        "digraph hfig_dv {",
        f'  graph [bgcolor="{NAVY}", rankdir=TB, splines=ortho, nodesep=0.45, '
        f'ranksep=0.9, fontname="Helvetica", compound=true, pad=0.4];',
        '  node [shape=box, fontname="Helvetica"];',
        f'  edge [color="{MUTED}", penwidth=1.2, arrowsize=0.7];',
    ]

    # node id per physical table
    ids: dict[str, str] = {}
    for e in model.entities:
        for src, table in e.tables():
            ids[table] = table.replace("-", "_")

    for i, domain in enumerate(domains):
        out.append(f'  subgraph cluster_{i} {{')
        out.append(f'    label=<<FONT COLOR="{CYAN}" POINT-SIZE="13"><B>'
                   f'{esc(domain.upper())}</B></FONT>>;')
        out.append(f'    color="{LINE}"; style="rounded"; penwidth=1; margin=18;')
        for e in sorted(model.entities, key=lambda x: (x.kind != "hub", x.kind, x.name)):
            if e.domain != domain:
                continue
            for src, table in e.tables():
                out.append("  " + node(ids[table], e, table, src))
        out.append("  }")

    # edges: satellite -> parent, link -> hubs
    for e in model.entities:
        for src, table in e.tables():
            if e.kind in naming.SATELLITE_KINDS:
                parent = model.get(e.parents[0])
                for _s, ptable in parent.tables():
                    colour = VIOLET if e.kind == "csat" else GOLD
                    out.append(f'  "{ids[ptable]}" -> "{ids[table]}" '
                               f'[color="{colour}", arrowhead=none];')
            elif e.kind in naming.LINK_KINDS:
                for p in e.parents:
                    for _s, ptable in model.get(p).tables():
                        out.append(f'  "{ids[ptable]}" -> "{ids[table]}" '
                                   f'[color="{GREEN}", arrowhead=none];')

    # the reconciliation pair: aggregate <-> transaction grain
    for e in model.entities:
        if e.grain != "aggregate":
            continue
        raw = model.get(e.aggregates_from)
        for _s, agg_t in e.tables():
            for _s2, raw_t in raw.tables():
                out.append(
                    f'  "{ids[raw_t]}" -> "{ids[agg_t]}" [style=dashed, color="{RED}", '
                    f'penwidth=1.6, arrowhead=vee, constraint=false, '
                    f'label=<<FONT COLOR="{RED}" POINT-SIZE="9">reconciles</FONT>>];'
                )

    out.append("}")
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# HTML
# --------------------------------------------------------------------------- #
LEGEND = [
    ("HUB", CYAN, "business key only · inserted once · never updated"),
    ("LINK / NHL", GREEN, "relationship · keys only, or immutable transaction payload"),
    ("SATELLITE", GOLD, "one table per source · insert-only · a change is a new row"),
    ("COMPUTED SAT", VIOLET, "Business Vault · derived, rebuildable, rule-versioned"),
    ("dashed border", RED, "AGGREGATE grain — cannot answer a per-entity question"),
    ("🔒", RED, "column mask declared in metadata, emitted into the table definition"),
]


def build_html(model: spec.Model, overview: str,
               per_domain: list[tuple[str, str]]) -> str:
    entities = sorted(model.entities, key=lambda e: (e.domain, e.kind, e.name))
    tables = sum(len(e.tables()) for e in model.entities)

    inv = []
    for e in entities:
        for src, table in e.tables():
            inv.append(
                f"<tr><td class='mono'>{esc(table)}</td>"
                f"<td>{esc(KIND_LABEL[e.kind])}</td>"
                f"<td>{esc(e.domain)}</td>"
                f"<td>{esc(e.grain)}</td>"
                f"<td>{esc(e.sensitivity)}</td>"
                f"<td class='mono'>{esc(src.name if src else ', '.join(s.name for s in e.sources))}</td>"
                f"<td class='mono'>{esc(' + '.join(e.parents) or '—')}</td></tr>"
            )

    pairs = []
    for e in model.entities:
        if e.grain != "aggregate":
            continue
        raw = model.get(e.aggregates_from)
        # THE PHYSICAL TABLE, not base_table. Every other inventory in this file reads
        # through tables() (line 351, 377); base_table is the version-free name and lost
        # its meaning as "what this diagram draws" the moment tables() started carrying a
        # _rev<N> suffix (Task 3). Left on base_table, this row named a table nobody could
        # query -- exactly the ghost verify_repo's ERD gates exist to catch.
        pairs.append(
            f"<tr><td class='mono'>{esc(e.tables()[0][1])}</td>"
            f"<td class='mono'>{esc(raw.tables()[0][1])}</td>"
            f"<td class='mono'>{esc(', '.join(e.aggregate_drops))}</td>"
            f"<td>{esc(', '.join(raw.parents))}</td></tr>"
        )

    masked = []
    for e in model.entities:
        for col, fn in e.masks:
            for _s, table in e.tables():
                masked.append(
                    f"<tr><td class='mono'>{esc(table)}</td>"
                    f"<td class='mono'>{esc(col)}</td>"
                    f"<td class='mono'>{esc(fn)}</td>"
                    f"<td>{esc(e.sensitivity)}</td></tr>"
                )

    legend = "".join(
        f"<tr><td class='swc'><span class='sw' style='background:{c}'></span></td>"
        f"<td class='swn'><b>{esc(n)}</b></td><td class='lgd'>{esc(d)}</td></tr>"
        for n, c, d in LEGEND
    )
    domain_pages = "".join(
        f'<div class="pb"></div><h2>{esc(d.upper())} — detail</h2>'
        f'<div class="diagram detail">{svg}</div>'
        for d, svg in per_domain
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>HFIG Data Vault ERD · v{VERSION}</title>
<style>
  @page {{ size: A3 landscape; margin: 12mm; }}
  body {{ font-family: Calibri, Carlito, "Segoe UI", sans-serif; color:#2B3948;
         margin:0; padding:0; background:#fff; }}
  .page {{ padding: 10mm 12mm 14mm; }}
  h1 {{ font-family: Cambria, Caladea, Georgia, serif; color:{INK}; font-size:24px;
        margin:0 0 2px; }}
  .sub {{ color:#6B7A8A; font-size:12px; margin-bottom:10px; }}
  h2 {{ font-family: Cambria, Caladea, Georgia, serif; color:{INK}; font-size:16px;
        margin:16px 0 6px; }}
  /* THE SVG KEEPS ITS OWN width/height ATTRIBUTES, AND THAT IS NOT A STYLE CHOICE.
     This rule used to say `width:auto; height:auto; max-width:100%`, which is the correct
     modern idiom and produced a PDF with SIX BLANK PAGES. wkhtmltopdf's QtWebKit does not
     implement SVG intrinsic sizing from viewBox, so `auto` resolves to ZERO and every
     diagram collapsed to nothing -- while the same HTML rendered perfectly in a browser,
     which is why it went unnoticed from 25 August. Proven by probe: an inline SVG with its
     graphviz width/height attributes intact renders in wkhtmltopdf; the same SVG under
     width:auto does not.
     So the diagram is never scaled. On a narrow screen the CONTAINER scrolls instead --
     which is better for an ERD anyway, since a squeezed diagram is unreadable and a scrolled
     one is not. A3 landscape is 1191pt wide and the widest diagram is 1109pt, so nothing is
     clipped in print. */
  .diagram {{ background:{NAVY}; border-radius:6px; padding:8px; margin:8px 0 4px;
              text-align:center; overflow-x:auto; }}
  .diagram svg {{ display:block; margin:0 auto; max-width:none; }}
  table.legend {{ width:auto; font-size:11px; margin:6px 0 10px; }}
  table.legend td {{ border:none; padding:2px 10px 2px 0; background:none !important; }}
  .swc {{ width:18px; }}
  .swn {{ white-space:nowrap; }}
  .sw {{ width:13px; height:13px; border-radius:3px; display:inline-block; }}
  .lgd {{ color:#6B7A8A; }}
  table {{ border-collapse:collapse; width:100%; font-size:10.5px; margin:4px 0 12px; }}
  th {{ background:{INK}; color:#fff; text-align:left; padding:4px 6px; font-size:10px; }}
  td {{ border:1px solid #E3E9EE; padding:3px 6px; vertical-align:top; }}
  tr:nth-child(even) td {{ background:#F7F9FB; }}
  .mono {{ font-family:"DejaVu Sans Mono", Consolas, monospace; font-size:9.5px; }}
  .note {{ background:#FBF2DF; border-radius:4px; padding:8px 11px; font-size:11px;
           color:#5C4A22; margin:8px 0; }}
  footer {{ margin-top:14px; padding-top:6px; border-top:1px solid #E3E9EE;
            font-size:9.5px; color:#8A8AA0; }}
  .pb {{ page-break-before: always; }}
</style></head><body><div class="page">

<h1>HFIG Enterprise Data Model — Data Vault 2.0 ERD</h1>
<div class="sub">Accelerator v{VERSION} · hash rulebook {RULEBOOK_VERSION} ·
{len(model.entities)} entities → {tables} physical tables ·
{len({e.domain for e in model.entities})} domains</div>

<div class="note"><b>Generated from metadata, not drawn.</b> Every box comes from
<span class="mono">metadata/entities/*.yml</span>, so the diagram cannot drift from the
model — it can only be stale by one run of <span class="mono">tools/render_erd.py</span>.
Satellites appear once per source, because that is what the factory actually creates.</div>

<table class="legend">{legend}</table>
<h2>Overview — what exists and how it is wired</h2>
<div class="diagram overview">{overview}</div>
{domain_pages}

<div class="pb"></div>
<h2>Grain pairs — what must not be confused</h2>
<div class="note">An aggregate is loaded faithfully and is correct in its own terms. The
risk is silent: deriving a per-entity fact from a total. Each pair below is reconciled by
<span class="mono">checks/aggregate_reconciliation_check.py</span>, which reads these
declarations rather than hardcoded table names.</div>
<table><tr><th>Aggregate</th><th>Transaction grain</th><th>Grain dropped</th>
<th>Raw parents</th></tr>{''.join(pairs) or '<tr><td colspan=4>none declared</td></tr>'}</table>

<h2>Masked columns</h2>
<div class="note">Masks are declared per column in metadata and emitted into each table's
definition — for streaming tables and materialized views, an <span class="mono">ALTER
TABLE … SET MASK</span> would not survive a pipeline update. The pipeline run-as identity
must be privileged under every mask, or a refresh materialises NULLs into the vault.</div>
<table><tr><th>Table</th><th>Column</th><th>Mask function</th><th>Sensitivity</th></tr>
{''.join(masked) or '<tr><td colspan=4>none</td></tr>'}</table>

<div class="pb"></div>
<h2>Table inventory</h2>
<table><tr><th>Table</th><th>Structure</th><th>Domain</th><th>Grain</th>
<th>Sensitivity</th><th>Source</th><th>Parents</th></tr>{''.join(inv)}</table>

<footer>HFIG — Data &amp; AI · generated by tools/render_erd.py from accelerator v{VERSION}
· Bronze table references in the metadata are placeholders until reconciled against the
real feeds</footer>
</div></body></html>"""


# --------------------------------------------------------------------------- #
# The alternation the PDF text is scanned with, built from the one place table prefixes
# are declared. verify_repo's own ERD check already derives its prefixes this way; this
# file was the copy that drifted.
_PDF_NAME_RE = (r"\b(?:" + "|".join(sorted(p.rstrip("_") for p in naming.PREFIX.values()))
                + r")_[a-z0-9_]+")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(ROOT / "docs"))
    ap.add_argument("--keep-dot", action="store_true")
    ap.add_argument(
        "--force-pdf", action="store_true",
        help="re-render the PDF even when the HTML is unchanged. Needed after a "
             "wkhtmltopdf upgrade, which changes the output without changing the input.")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    model = spec.load_model(ROOT / "metadata" / "entities")

    def render(dot_src: str, stem: str) -> str:
        dot_path = out / f"{stem}.dot"
        dot_path.write_text(dot_src, encoding="utf-8")
        try:
            svg = subprocess.run(["dot", "-Tsvg", str(dot_path)],
                                 capture_output=True, text=True, check=True).stdout
        except FileNotFoundError:
            raise SystemExit("graphviz 'dot' not found: install graphviz")
        except subprocess.CalledProcessError as exc:
            raise SystemExit(f"dot failed on {stem}: {exc.stderr[:400]}")
        if not args.keep_dot:
            dot_path.unlink()
        # Keep dot's width/height: `size` in the graph attributes has already scaled the
        # drawing to one page. Stripping them would make the browser use the unscaled
        # viewBox and the diagram would overflow onto the next page.
        return svg[svg.index("<svg"):]

    overview = render(build_overview_dot(model), "hfig_dv_erd_overview")
    domains = sorted({e.domain for e in model.entities})
    per_domain = [(d, render(build_domain_dot(model, d), f"hfig_dv_erd_{d}"))
                  for d in domains]

    html_path = out / "hfig_dv_erd.html"
    html_path.write_text(build_html(model, overview, per_domain), encoding="utf-8")
    print(f"wrote {html_path}")

    # DO NOT RE-RENDER AN UNCHANGED PDF.
    #
    # wkhtmltopdf stamps /CreationDate, so every run produces different bytes from
    # identical input -- 142,694 bytes before and after, not one of them the same. The
    # consequence is not cosmetic: running the emitters dirties the tree with a 142 KB
    # binary diff that a reviewer cannot read and cannot distinguish from a real change to
    # the diagram. Measured twice on 5 September, both times a no-op that looked like an
    # edit.
    #
    # The HTML the PDF is rendered FROM is fully deterministic, and its digest is already
    # recorded in the provenance file for gating. So: if that digest still matches and the
    # PDF is still there, the PDF on disk is already the render of this HTML and rewriting
    # it can only add noise. This is what restores the byte-identical discipline every
    # other generated artefact here is held to.
    #
    # --force-pdf is the escape hatch, and it is not decorative: the digest covers the
    # INPUT, not the renderer, so a wkhtmltopdf upgrade legitimately changes the output
    # while every input stays identical.
    pdf_path = out / "hfig_dv_erd.pdf"
    prov_path = out / "hfig_dv_erd.provenance.json"
    _html_digest = hashlib.sha256(html_path.read_bytes()).hexdigest()
    if not args.force_pdf and pdf_path.exists() and prov_path.exists():
        try:
            _prior = json.loads(prov_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            _prior = {}
        if _prior.get("html_sha256") == _html_digest:
            print(f"{pdf_path} is already the render of this HTML "
                  f"(sha256 {_html_digest[:12]}) -- left untouched. --force-pdf overrides.")
            return 0

    result = subprocess.run(
        ["wkhtmltopdf", "--enable-local-file-access", "--page-size", "A3",
         "--orientation", "Landscape", "--margin-top", "10mm",
         "--margin-bottom", "12mm", "--margin-left", "10mm", "--margin-right", "10mm",
         "--quiet", str(html_path), str(pdf_path)],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not pdf_path.exists():
        print(f"wkhtmltopdf failed: {result.stderr[:400]}")
        return 1
    print(f"wrote {pdf_path}")

    # PROVENANCE, BECAUSE THE PDF ITSELF CANNOT BE GATED.
    #
    # wkhtmltopdf stamps /CreationDate into every render, so two runs over identical input
    # produce different bytes -- verified. So the byte-identical discipline every other
    # generated artefact in this repo is held to cannot reach this one, and for ten days it
    # did not: docs/hfig_dv_erd.pdf sat at its 25 August content while the model changed
    # under it, still naming sat_job_request_details_fieldglass_eu after that binding was
    # renamed. verify_repo required the file to EXIST and asserted nothing about it.
    #
    # What IS reproducible is the HTML the PDF was rendered from. Recording its digest here
    # ties the two together: change the model, the HTML changes, the digest stops matching,
    # and the gate fails until someone re-renders. That turns an ungateable binary into a
    # gateable one without pretending its bytes are stable.
    #
    # The renderer's version is deliberately NOT recorded. It would make this file churn
    # between machines that have different wkhtmltopdf builds, and a file that changes for
    # reasons unrelated to the model is a file people stop reading.
    # AND MEASURE WHAT THE PDF ACTUALLY CONTAINS, not just that it is fresh.
    #
    # THE PROVENANCE DIGEST ALONE WAS NOT ENOUGH, and this is the second bug in this file
    # found the same day. The digest proves the PDF was rendered from the committed HTML; it
    # says nothing about whether the RENDER WORKED. It had not: `width:auto` on an inline SVG
    # collapses to zero in wkhtmltopdf's QtWebKit, so the PDF carried six blank pages while
    # its digest matched perfectly and the same HTML looked right in a browser.
    #
    # MEASURED HERE, GATED IN verify_repo, and split that way deliberately: pdftotext lives
    # where the PDF is produced, and verify_repo must run with no tool beyond Python. So the
    # numbers are recorded into the provenance file and the gate reads them -- which also
    # means a regression shows up as a committed diff rather than only in someone's terminal.
    pages: list[int] = []
    names: set[str] = set()
    try:
        n_pages = int(re.search(
            r"^Pages:\s+(\d+)",
            subprocess.run(["pdfinfo", str(pdf_path)], capture_output=True, text=True,
                           check=True).stdout, re.M).group(1))
        for page in range(1, n_pages + 1):
            text = subprocess.run(
                ["pdftotext", "-f", str(page), "-l", str(page), str(pdf_path), "-"],
                capture_output=True, text=True, check=True).stdout
            pages.append(len(" ".join(text.split())))
            # DERIVED FROM naming.PREFIX, NEVER RETYPED. This list used to be a
            # hardcoded alternation reading `...|hal|link)_`, and the link prefix is
            # `lnk_`, not `link_` -- so lnk_client_job_request could never be counted.
            # The table rendered correctly the whole time; the MEASUREMENT could not see
            # it, and pdf_table_names quietly under-reported by one. That is the worse
            # half of the bug: the number the provenance gate reads was wrong, so a link
            # table that genuinely failed to render would have looked exactly like this.
            names |= set(re.findall(_PDF_NAME_RE, text))
    except (FileNotFoundError, subprocess.CalledProcessError, AttributeError) as exc:
        print(f"cannot measure the PDF ({type(exc).__name__}) -- poppler's pdfinfo and "
              f"pdftotext are needed to record what it contains. Install poppler-utils.")
        return 1

    prov_path.write_text(json.dumps({
        "_why": "The PDF is not byte-reproducible (wkhtmltopdf stamps /CreationDate), so "
                "verify_repo gates it through this digest instead. If it fails, re-run "
                "tools/render_erd.py -- the PDF is stale, not this file.",
        "accelerator_version": VERSION,
        "html_sha256": _html_digest,
        # Per-page character counts. A near-zero page means a diagram did not render --
        # which is exactly what six of these were until the SVG sizing was fixed.
        "pdf_page_chars": pages,
        "pdf_table_names": len(names),
    }, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {prov_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
