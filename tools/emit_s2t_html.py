"""The HTML rendering of the Bronze -> Silver mapping.

SPLIT FROM emit_source_to_target.py so the row builder stays readable. That module decides
WHAT the mapping is; this one decides how it looks. Both are driven from one build, so the
CSV and the HTML can never describe different mappings.

FILTERABLE, BECAUSE 400+ ROWS IS NOT A DOCUMENT ANYONE READS TOP TO BOTTOM. The reader arrives
with a question -- where does nhl_journal_line.debit come from, what does GP_US feed -- so the
page carries a search box and rule filters that work with no network and no build step. The
alternative is a beautiful table nobody can navigate.
"""

from __future__ import annotations

import html

RULE_CLASS = {
    "direct": "r-direct",
    "cast": "r-cast",
    "hash": "r-key",
    "generated": "r-gen",
}


def _rule_kind(rule: str) -> str:
    if rule.startswith("SHA-256"):
        return "hash"
    if rule.startswith("CAST"):
        return "cast"
    if rule.startswith("generated"):
        return "generated"
    return "direct"


def _e(text) -> str:
    return html.escape(str(text or ""))


def render_html(rows: list[dict], facts: list[dict]) -> str:
    n_direct = sum(1 for r in rows if _rule_kind(r["rule"]) == "direct")
    n_cast = sum(1 for r in rows if _rule_kind(r["rule"]) == "cast")
    n_hash = sum(1 for r in rows if _rule_kind(r["rule"]) == "hash")
    n_gen = sum(1 for r in rows if _rule_kind(r["rule"]) == "generated")
    n_masked = sum(1 for r in rows if r["mask"])
    n_warn = sum(1 for r in rows if "WILL NOT JOIN" in r["notes"])
    sources = sorted({f["source_table"] for f in facts if f["source_table"]})
    # NOT ALL OF THEM ARE BRONZE, and a stat labelled "bronze sources" that counted the raw
    # vault would misdescribe the business vault's whole reason for existing: a computed
    # satellite's input IS the vault. Split by what the name says.
    vault_src = sorted(x for x in sources if ".raw_vault." in x or ".business_vault." in x)
    bronze_src = [x for x in sources if x not in vault_src]

    head = f"""<title>Bronze to Silver Mapping</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
  :root {{
    --ground:#f7f8fa; --surface:#fff; --ink:#1c2230; --ink-soft:#38414f; --muted:#5c6779;
    --rule:#d8dee7; --accent:#2f5d8c; --accent-soft:#e6edf5; --signal:#9c3b2e;
    --key:#5a3d8c; --key-soft:#efe9f7; --cast:#8c5a2f; --cast-soft:#f7efe6;
    --gen:#5c6779; --gen-soft:#eef1f5; --ok:#2f6b4f; --ok-soft:#e6f2ec;
    --sans:"IBM Plex Sans",ui-sans-serif,system-ui,sans-serif;
    --mono:"IBM Plex Mono",ui-monospace,Menlo,monospace;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --ground:#12161d; --surface:#1a1f28; --ink:#e7ecf3; --ink-soft:#c3cbd7;
      --muted:#8d99aa; --rule:#2f3846; --accent:#7fb0dd; --accent-soft:#1c2836;
      --signal:#e0897a; --key:#b39ae0; --key-soft:#241d33; --cast:#d9a06a;
      --cast-soft:#2e2419; --gen:#8d99aa; --gen-soft:#222834; --ok:#7dbb9c; --ok-soft:#16241d;
    }}
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--ground); color:var(--ink); font-family:var(--sans);
         font-size:14px; line-height:1.55; -webkit-font-smoothing:antialiased; }}
  .page {{ max-width:82rem; margin:0 auto; padding:clamp(1.5rem,4vw,3rem) clamp(1rem,3vw,2rem) 5rem; }}
  .eyebrow {{ font-size:.7rem; font-weight:600; letter-spacing:.13em; text-transform:uppercase;
             color:var(--accent); margin:0 0 .5rem; }}
  h1 {{ font-weight:600; font-size:clamp(1.6rem,4vw,2.2rem); line-height:1.15;
        letter-spacing:-.02em; margin:0 0 .75rem; }}
  .lede {{ color:var(--ink-soft); max-width:52rem; margin:0 0 1.25rem; }}
  .lede strong {{ color:var(--ink); }}
  .stats {{ display:flex; flex-wrap:wrap; gap:.5rem; margin:0 0 1.5rem; }}
  .stat {{ background:var(--surface); border:1px solid var(--rule); border-radius:3px;
           padding:.5rem .75rem; font-size:.82rem; }}
  .stat b {{ font-family:var(--mono); font-size:1rem; display:block; }}
  .controls {{ position:sticky; top:0; z-index:5; background:var(--ground);
               padding:.75rem 0; border-bottom:1px solid var(--rule); margin-bottom:1rem;
               display:flex; flex-wrap:wrap; gap:.5rem; align-items:center; }}
  .controls input {{ flex:1 1 18rem; min-width:12rem; padding:.5rem .65rem; font:inherit;
                     background:var(--surface); color:var(--ink);
                     border:1px solid var(--rule); border-radius:3px; }}
  .controls button {{ padding:.45rem .7rem; font:inherit; font-size:.82rem; cursor:pointer;
                      background:var(--surface); color:var(--ink-soft);
                      border:1px solid var(--rule); border-radius:3px; }}
  .controls button[aria-pressed="true"] {{ background:var(--accent-soft);
                                           border-color:var(--accent); color:var(--accent);
                                           font-weight:600; }}
  .count {{ font-size:.8rem; color:var(--muted); font-family:var(--mono); }}
  table {{ width:100%; border-collapse:collapse; background:var(--surface);
           border:1px solid var(--rule); border-radius:3px; }}
  th, td {{ text-align:left; vertical-align:top; padding:.45rem .6rem;
            border-bottom:1px solid var(--rule); }}
  th {{ position:sticky; top:3.4rem; background:var(--surface); font-size:.72rem;
        text-transform:uppercase; letter-spacing:.07em; color:var(--muted); z-index:4; }}
  td.mono, code {{ font-family:var(--mono); font-size:.82rem; }}
  tr.grp td {{ background:var(--accent-soft); font-weight:600; color:var(--accent); }}
  .tag {{ display:inline-block; font-family:var(--mono); font-size:.7rem; padding:.05rem .35rem;
          border-radius:2px; white-space:nowrap; }}
  .r-direct {{ background:var(--ok-soft); color:var(--ok); }}
  .r-cast {{ background:var(--cast-soft); color:var(--cast); }}
  .r-key {{ background:var(--key-soft); color:var(--key); }}
  .r-gen {{ background:var(--gen-soft); color:var(--gen); }}
  .msk {{ color:var(--signal); font-weight:600; }}
  tr.warn td {{ background:color-mix(in srgb, var(--signal) 8%, transparent); }}
  tr.warn td:first-child {{ box-shadow:inset 3px 0 0 var(--signal); }}
  .warn-note {{ color:var(--signal); font-weight:600; }}
  .note {{ color:var(--muted); font-size:.78rem; }}
  h2 {{ font-size:1.05rem; margin:2rem 0 .5rem; }}
  .box {{ background:var(--surface); border:1px solid var(--rule); border-left:3px solid var(--accent);
          border-radius:3px; padding:.85rem 1rem; margin:0 0 1.25rem; }}
  .box p {{ margin:0 0 .5rem; }} .box p:last-child {{ margin:0; }}
</style>"""

    body = ['<div class="page">',
            '<p class="eyebrow">Generated &middot; HFIG Data Vault</p>',
            "<h1>Bronze to Silver: source-to-target mapping</h1>",
            '<p class="lede">Every column of the raw and business vaults, and where it comes '
            'from. <strong>Generated from <code>metadata/entities/</code></strong> &mdash; the '
            'same declarations the pipeline builds its flows from &mdash; so it cannot describe '
            'a mapping the loader does not perform. A hand-kept version of this document would '
            'be wrong the first time a binding changed, and wrong silently.</p>',
            '<div class="stats">',
            f'<div class="stat"><b>{len(rows)}</b>mapping rows</div>',
            f'<div class="stat"><b>{len({(r["target_schema"], r["target_table"]) for r in rows})}</b>target tables</div>',
            f'<div class="stat"><b>{len(bronze_src)}</b>bronze sources</div>',
            (f'<div class="stat"><b>{len(vault_src)}</b>raw-vault sources</div>'
             if vault_src else ""),
            f'<div class="stat"><b>{n_direct}</b>direct</div>',
            f'<div class="stat"><b>{n_cast}</b>cast</div>',
            f'<div class="stat"><b>{n_hash}</b>derived keys</div>',
            f'<div class="stat"><b>{n_gen}</b>generated</div>',
            f'<div class="stat"><b>{n_masked}</b>masked</div>',
            (f'<div class="stat" style="border-color:var(--signal);color:var(--signal)">'
             f'<b>{n_warn}</b>will not join</div>' if n_warn else ""),
            "</div>",
            '<div class="box">',
            "<p><strong>Column names are the source's.</strong> The generator projects and "
            "renames nothing &mdash; a flow appends the staged frame as it stands &mdash; so a "
            "payload column's source name is its target name. <code>debit</code> and "
            "<code>credit</code> are <code>ukg_raw.gl</code>'s own names, not ours.</p>",
            "<p><strong>A hash key has no single source column.</strong> It is SHA-256 over the "
            "declared key columns, so its row names every column that feeds it and the rulebook "
            "version that fixes the normalisation. A bump to that version re-keys the estate.</p>",
            ("<p><strong>Two targets read the vault, not Bronze.</strong> A business-vault "
             "computed satellite\u2019s input is the raw vault \u2014 "
             f"{', '.join('<code>' + s + '</code>' for s in vault_src)} \u2014 which is what "
             "a business vault is for. Their rows name that real input rather than "
             "pretending it is a Bronze feed.</p>" if vault_src else ""),
            "<p><strong>A conformed table appears once per binding.</strong> A satellite is one "
            "table per source; a hub, link or NHL is one table fed by every source, so the same "
            "target column legitimately appears more than once with different origins. That is "
            "what conformance means, not duplication.</p>",
            "</div>",
            (('<div class="box" style="border-left-color:var(--signal)">'
              "<p><strong>Five foreign keys in this mapping cannot join to the parent they "
              "name</strong>, and their rows say so. A federated hash key prepends its "
              "source name as a literal, so the scope alone settles joinability: a key "
              "scoped <code>BUSINESS_VAULT</code> can never equal one scoped "
              "<code>FIELDGLASS_EU</code>, whatever columns follow. Three are a parent hub "
              "not yet fed from that source, and adding the binding fixes them. Two are "
              "computed satellites that scope on their own synthetic binding name, which no "
              "feed will ever deliver &mdash; those need a modelling decision, not a "
              "cleanup.</p><p>All five are listed in "
              "<code>metadata/key_scope_exceptions.json</code> with a reason, and "
              "<code>verify_repo</code> fails the build on a sixth. Filter to them with the "
              "<em>will not join</em> button below.</p></div>") if n_warn else ""),
            '<div class="controls">',
            '<input id="q" type="search" placeholder="Filter by table, column, source or rule'
            '&hellip;" aria-label="Filter rows">',
            '<button data-rule="direct" aria-pressed="false">direct</button>',
            '<button data-rule="cast" aria-pressed="false">cast</button>',
            '<button data-rule="hash" aria-pressed="false">derived key</button>',
            '<button data-rule="generated" aria-pressed="false">generated</button>',
            '<button data-rule="masked" aria-pressed="false">masked</button>',
            ('<button data-rule="warn" aria-pressed="false" '
             'style="border-color:var(--signal);color:var(--signal)">will not join</button>'
             if n_warn else ""),
            '<span class="count" id="count"></span>',
            "</div>",
            "<table><thead><tr>",
            "<th>Target column</th><th>Type</th><th>Rule</th><th>Source table</th>",
            "<th>Source column(s)</th><th>Class</th><th>Mask</th><th>Notes</th>",
            "</tr></thead><tbody>"]

    facts_by = {(f["schema"], f["table"], f["binding"]): f for f in facts}
    last = None
    for r in rows:
        # ONE SECTION PER (table, BINDING), not per table.
        #
        # Keyed on the BINDING NAME, not the source table: a generated column has no source
        # table, so a source-table key fragments every section (36 became 148, measured).
        # A hub, link or NHL is ONE conformed table fed by EVERY binding, and the facts that
        # belong in a section header -- the dedup key, the dedup order, the expectations --
        # are the BINDING's, not the table's. hub_accounting_journal dedups on
        # (input_db, openyear, jrnentry) under GP_US and on something else under UKG_US.
        # A header that spanned the whole table would have to pick one and would then be
        # quietly wrong about the other; an earlier draft picked whichever came first.
        grp = (r["target_schema"], r["target_table"], r["binding"])
        if grp != last:
            f = facts_by.get(grp, {})
            src = f.get("source_table") or r["source_table"]
            head_bits = [x for x in (
                f'{_e(f.get("kind", ""))} in {_e(f.get("domain", ""))}' if f.get("kind") else "",
                f'grain {_e(f["grain"])}' if f.get("grain") else "",
                f'dedup on <code>{_e(f["dedup_by"])}</code>' if f.get("dedup_by") else "",
                f'ordered by <code>{_e(f["dedup_order"])}</code>' if f.get("dedup_order") else "",
                f'expectations: {_e(f["expectations"])}' if f.get("expectations") else "",
                f'sensitivity {_e(f["sensitivity"])}' if f.get("sensitivity") else "",
            ) if x]
            binding = f'<span class="note">&larr; {_e(src)}</span>' if src else ""
            body.append(
                f'<tr class="grp" data-grp="1"><td colspan="8">{_e(grp[0])}.'
                f'<code>{_e(grp[1])}</code> <strong>{_e(grp[2])}</strong> {binding}<br>'
                f'<span class="note">{" &middot; ".join(head_bits)}</span></td></tr>')
            last = grp
        kind = _rule_kind(r["rule"])
        # A ROW THAT CANNOT JOIN MUST NOT LOOK LIKE THE OTHERS. Same reasoning as the MASKED
        # note in the diagram: this artefact travels, and an unjoinable foreign key rendered
        # in the ordinary style is an invitation to build the join.
        warn = "WILL NOT JOIN" in r["notes"]
        note_html = (f'<span class="warn-note">{_e(r["notes"])}</span>' if warn
                     else _e(r["notes"]))
        body.append(
            f'<tr data-rule="{kind}" data-masked="{"1" if r["mask"] else "0"}" '
            f'data-warn="{"1" if warn else "0"}"'
            + (' class="warn"' if warn else "") + ">"
            f'<td class="mono">{_e(r["target_column"])}</td>'
            f'<td class="mono note">{_e(r["target_type"])}</td>'
            f'<td><span class="tag {RULE_CLASS[kind]}">{_e(r["rule"])}</span></td>'
            f'<td class="mono note">{_e(r["source_table"])}</td>'
            f'<td class="mono">{_e(r["source_columns"])}</td>'
            f'<td class="note">{_e(r["classification"])}</td>'
            f'<td class="mono msk">{_e(r["mask"])}</td>'
            f'<td class="note">{note_html}</td></tr>')

    body += ["</tbody></table>",
             '<h2>The same rows as CSV</h2>',
             '<p class="lede"><code>docs/source_to_target_mapping.csv</code> holds these rows '
             'for Excel. It is generated from the same build, so the two cannot disagree, and '
             'being text it diffs in git where a workbook would not.</p>',
             "</div>",
             """<script>
(function () {
  var q = document.getElementById('q'), count = document.getElementById('count');
  var rows = [].slice.call(document.querySelectorAll('tbody tr:not(.grp)'));
  var groups = [].slice.call(document.querySelectorAll('tbody tr.grp'));
  var active = {};
  function apply() {
    var term = (q.value || '').toLowerCase();
    var shown = 0;
    rows.forEach(function (tr) {
      var okRule = true, keys = Object.keys(active).filter(function (k) { return active[k]; });
      if (keys.length) {
        okRule = keys.some(function (k) {
          if (k === 'masked') return tr.dataset.masked === '1';
          if (k === 'warn') return tr.dataset.warn === '1';
          return tr.dataset.rule === k;
        });
      }
      var okTerm = !term || tr.textContent.toLowerCase().indexOf(term) !== -1;
      var show = okRule && okTerm;
      tr.hidden = !show;
      if (show) shown++;
    });
    // A group header with nothing under it is noise, so it hides with its rows.
    groups.forEach(function (g) {
      var n = g.nextElementSibling, any = false;
      while (n && !n.classList.contains('grp')) {
        if (!n.hidden) { any = true; break; }
        n = n.nextElementSibling;
      }
      g.hidden = !any;
    });
    count.textContent = shown + ' of ' + rows.length + ' rows';
  }
  q.addEventListener('input', apply);
  [].slice.call(document.querySelectorAll('.controls button')).forEach(function (b) {
    b.addEventListener('click', function () {
      var k = b.dataset.rule;
      active[k] = !active[k];
      b.setAttribute('aria-pressed', active[k] ? 'true' : 'false');
      apply();
    });
  });
  apply();
})();
</script>"""]
    return head + "\n" + "\n".join(body) + "\n"
