# Vendored Archify schemas

The JSON Schema files from [tt-a1i/archify](https://github.com/tt-a1i/archify), MIT licensed,
copied verbatim at commit `c6519401f7b9` on 6 September 2026. Verified byte-identical to the
installed 2.17 skill on the same day.

**Why vendored rather than fetched.** `verify_repo.py` must run offline, from an extracted
zip, with no network — that is a standing property of this repo's gates, not a preference.
A gate that fetches a schema is a gate that passes when GitHub is unreachable.

**Not modified.** If they diverge upstream, re-copy the files and read the diff;
`tools/emit_archify.py` validates against whatever is here, so a schema change shows up as
a validation failure rather than as a silently different diagram.

**THESE ARE NOT THE CONTRACT, AND THAT DISTINCTION COST TWO ARTEFACTS.** A document can
satisfy every schema in this directory and still be undrawable. `dataflow.schema.json`
bounds a node's `row` at `minimum: 0` with no maximum; the dataflow renderer has exactly
five rows per stage. Nothing in `architecture.schema.json` says a component whose label is
wider than its box is a hard error. On 6 September, when the renderer was first installed,
both committed documents failed it having passed the schema gate since the day they were
added.

So the schemas are the **cheap** check — they need only Python, so they run in CI and on a
stripped machine. The **real** check is `tools/archify_render_gate.py`, which asks the
installed renderer directly. It skips where Node and the skill are absent, and says so.
