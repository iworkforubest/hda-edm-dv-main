"""Render the Bronze -> Silver mapping as a document, from the model that performs it.

GENERATED, NEVER HAND-WRITTEN, and that is the whole point. A source-to-target mapping kept by
hand is stale the first time a binding changes, and it is stale silently: nothing fails, the
document simply stops describing the pipeline. This one is emitted from
`metadata/entities/*.yml` -- the same declarations the factory builds the flows from -- so it
cannot disagree with what actually loads. verify_repo.py asserts regenerating produces a
byte-identical file.

THREE RENDERINGS OF ONE MAPPING, from one build, so they cannot describe different
pipelines. The HTML is for reading and filtering in a browser. The XLSX is for Excel, with
the mapping, the per-binding facts and the unjoinable keys on separate sheets. The CSV is
the same rows as text, and it stays because it is the only one of the three that DIFFS in
git -- a reviewer can see what a model change did to the mapping in the pull request,
which no workbook will ever show them.

WHAT A ROW MEANS. One row per TARGET column, because that is the question a reader has: where
did this column come from. Seven kinds of row, and they are not interchangeable:

  direct      a bronze column lands as-is. The generator renames NOTHING -- see below.
  cast        a bronze column lands with a declared type change.
  derived     computed at staging from the binding's conform profile's own SQL.
  not sourced the source is an entity in this model and it has NO column of this name.
  hash key    DERIVED, not sourced: SHA-256 over named columns. No single source column.
  hashdiff    DERIVED over the whole payload, for change detection on a satellite.
  generated   load_dts, rec_src, batch_id and the rest. No bronze origin at all.

"DIRECT, SAME NAME" USED TO BE THE FALLBACK, AND A FALLBACK THAT ASSUMES IS A LIE AT SCALE.
Anything that was not a hash key, a hashdiff, a system column or a declared cast fell through
to "direct, same name" WITHOUT EVER ASKING WHETHER THE SOURCE HAS THAT COLUMN. Measured: all
eleven payload columns of csat_invoice_line_gie were documented as direct-or-cast from
nhl_invoice_line, and nhl_invoice_line provides NOT ONE of them -- zero overlap between what
the document claimed and what the source has. Five are derived in SQL by the conform profile
fieldglass_us_invoice_gie (DEF-58); the other six are not produced from that table at all. The
cast branch carried the same lie wearing a type: `CAST to DECIMAL(18,2)` with source_columns
=[amount], for a column the source does not provide either.

SO PRESENCE IS NOW CHECKED, AND ONLY WHERE IT CAN BE. A `derived` row is emitted whenever the
binding's resolved conform profile derives the column, with the profile's own expression as the
rule and the identifiers that expression actually READS as the source columns
(staging_columns.extract_identifiers, the same pure function the Spark suite builds its frame
from). A `not sourced` row is emitted only when the source table is AN ENTITY IN THIS MODEL --
a business-vault satellite reading the raw vault, or a hub binding recomputing a parent key
from it -- because only then does this repo know, offline, what columns that table has. For a
binding reading BRONZE the repo knows nothing about the source's shape, so no absence is
claimed and today's behaviour stands: 691 of the 762 rows are in that position, and
over-reaching into them would replace one false claim with another across almost the whole
document.

WHAT IS NOT SCOPE-LIMITED, AND WHY THAT IS NOT A CONTRADICTION. Naming the column a binding
DECLARES is not a claim about Bronze's shape, so the derivation rows, the hub business keys
and the concatenated _bk row are emitted truthfully for every binding. 15 hub business-key
positions are LITERALS hashed into the key as constants, 36 are RENAMES, and only 3 are
genuinely same-name -- every one of the 51 used to read "direct, same name" with its own
target name. accounting_journal.fiscal_year reads openyear under GP_US and hstyear under
GP_US_HIST, which the old emitter rendered as two IDENTICAL rows: these are hash-key
components, so a reader asking why two systems' keys will not join was being shown the one
thing that cannot answer them.

WHAT THE `not sourced` NOTE DELIBERATELY DOES NOT SAY. A column can be unsourced because its
inputs are absent from the source (description, line_description, amount, project_number) or
because the rule itself is undefined and open with the repo owner (invoice_amount/AME003,
accounting_date/AME009). THE MODEL DOES NOT RECORD WHICH. That distinction is written only in
prose -- the entity YAML's comments, metadata/source_unions.yml's comments, and the _GIE_OPEN
dict in tests/test_accelerator.py -- none of which this emitter reads, and none of which is a
declaration it could read. So the note says the two cannot be told apart from the model rather
than inventing a distinction the document would then be trusted for.

"BRONZE TO SILVER" IS TRUE OF MOST LEGS, NOT ALL, AND THE DOCUMENT SAYS WHICH. A raw-vault
satellite reads Bronze. A BUSINESS-vault computed satellite reads the RAW VAULT -- csat
job_request_custom_promoted reads msat_job_request_custom_field_fieldglass_eu, and csat
payroll_line_classification reads nhl_payroll_detail. That is what a business vault is for,
so the source table column names the real input rather than pretending it is a Bronze feed.

COLUMN NAMES ARE THE SOURCE'S, AND THAT IS LOAD-BEARING FOR THIS DOCUMENT. The factory
projects and renames nothing: a flow appends the staged frame as it stands. So a payload
column's source name IS its target name, and a mapping row that showed a rename would be
describing something the pipeline does not do. `debit` and `credit` are ukg_raw.gl's own names.

WHAT IS PER BINDING AND WHAT IS NOT. A satellite is one table PER source, so its mapping is
one binding. A hub, link or NHL is one conformed table fed by EVERY binding, so it gets a
section per binding and the same target column may legitimately appear more than once with
different sources. That is not duplication; it is what conformance means.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

from accelerator import contract, hashing, naming, spec, staging_columns  # noqa: E402

HTML_PATH = ROOT / "docs" / "source_to_target_mapping.html"
CSV_PATH = ROOT / "docs" / "source_to_target_mapping.csv"
XLSX_PATH = ROOT / "docs" / "source_to_target_mapping.xlsx"

RAW_SCHEMA = "raw_vault"
BUSINESS_SCHEMA = "business_vault"

# `binding` is a column of its own, and not derivable from `source_table`: a generated
# column (load_dts, rec_src) has NO source table, so grouping rows by source table breaks
# every table into fragments -- measured, it turned 36 sections into 148. The binding is
# what a row belongs to; the source table is what it reads, and for a third of the rows the
# honest answer to the second is "nothing".
COLUMNS = ("target_schema", "target_table", "binding", "target_column", "target_type",
           "rule", "source_table", "source_columns", "classification", "mask", "notes")


def _hash_rule(components, scope) -> str:
    """How one hash key is built, in the components the loader actually hashes.

    THESE ARE NOT A PARAPHRASE. `components` comes from spec.hash_key_columns -- the single
    function factory.py itself calls to decide what goes into a key -- so this rule text and
    the running loader cannot disagree. An earlier draft of this emitter printed
    "<parent key>" for twelve satellite foreign keys, because it had no way to reach the
    derivation; that placeholder was the reason hash_key_columns was extracted.

    THE SCOPE IS SHOWN FIRST AND QUOTED, because that is where hashing.hash_key puts it: a
    federated key prepends its source name as a LITERAL. A rule that named only the columns
    would render a federated and an authored key identically, and they are different keys.

    The rulebook version is part of the rule. A bump changes the normalisation, so naming
    the components without it would describe two different keys the same way.
    """
    # RENDERED BY THE RULEBOOK, NOT HERE. This line built the scope literal itself, so the
    # mapping could describe a literal hashing.hash_key would refuse to emit.
    parts = ([hashing.source_scope_literal(scope)] if scope else []) + list(components)
    return (f"SHA-256 over ({', '.join(parts)}) as BINARY(32); "
            f"rulebook {hashing.RULEBOOK_VERSION}")


def _source_columns(components) -> list[str]:
    """The real column references among a key's components, literals dropped.

    A literal ('GP_Journal_Entry', a source scope) is hashed exactly as a column value is,
    but it has no bronze origin -- so it belongs in the RULE, which says what is hashed, and
    not in the source-columns cell, which says what to go and look at in Bronze.
    """
    return [c for c in components if not c.startswith("'")]


def _entity_names(model) -> set[str]:
    return {e.name for e in model.entities}


def orphan_scopes(model) -> dict[str, set]:
    """{entity: the set of scopes its OWN key is ever built under}.

    Used to mark foreign keys that cannot join. A federated key prepends its source name as
    a literal, so the scope alone settles joinability whatever columns follow: a key scoped
    'BUSINESS_VAULT' can never equal one scoped 'FIELDGLASS_EU'. Five such keys exist in the
    model, acknowledged in metadata/key_scope_exceptions.json and gated by verify_repo
    against a sixth.

    THE DOCUMENT HAS TO SAY SO. This mapping's whole purpose is to answer "where does this
    column come from", and for these five the truthful answer includes "and it will not join
    to the parent it names". A row that listed the components and stopped would be accurate
    about the hash and misleading about the model -- which is worse than saying nothing,
    because a reader would go and build the join.
    """
    scopes: dict[str, set] = {}
    for entity in model.entities:
        for src in entity.sources:
            for col, (_cols, scope) in spec.hash_key_columns(model, entity, src).items():
                if col == entity.hk_column:
                    scopes.setdefault(entity.name, set()).add(scope)
    return scopes


def projected_columns_by_table(model) -> dict[str, frozenset]:
    """{stable table name: every column that table projects}, for every table in the model.

    THIS IS THE ONLY THING THAT MAKES "the source does not provide it" SAYABLE. A row can
    only deny a source column if the emitter knows what the source has, and for a table
    this model declares it does: contract_columns is the contract emitter's own walk, which
    is factory._projection -- the same function that decides the physical shape.

    UNIONED OVER THE BINDINGS FEEDING ONE TABLE, not taken from one of them. A hub, link or
    NHL is ONE table fed by every binding, and a per-binding payload could in principle
    differ; taking the first binding's projection would then deny a column another binding
    really does write. The union is the generous direction, which is the correct bias here:
    an over-generous "provides" set under-claims absence, and the failure mode this exists
    to stop is CLAIMING absence -- or presence -- that is not there.
    """
    by_table: dict[str, set] = {}
    for entity in model.entities:
        for src, table in entity.stable_tables():
            for binding in spec.table_bindings(entity, src):
                by_table.setdefault(table, set()).update(contract_columns(entity, binding))
    return {t: frozenset(c) for t, c in by_table.items()}


def source_provides(binding, by_table) -> frozenset | None:
    """The columns this binding's source table provides, or None when THIS REPO CANNOT KNOW.

    None IS NOT "no columns", AND THE CALLER MUST NOT TREAT IT AS ONE. Two different
    situations return it, and both mean the same thing for the document: say nothing about
    absence.

      BRONZE. The overwhelming majority of bindings read a Bronze streaming table, whose
      columns live in a workspace this emitter never contacts -- it runs offline, from
      metadata/entities alone. naming.reads_vault() is the discriminator, and it is the
      same one factory.py and spec.validate already use to tell a vault leg from a feed.

      A VAULT TABLE THIS MODEL DOES NOT DECLARE. reads_vault() is a name-prefix test, so a
      table could be named like a vault table and be absent from the model. Guessing on a
      dict miss would deny every column of it.

      A BRONZE TABLE WHOSE BASE NAME HAPPENS TO LOOK LIKE A VAULT TABLE. reads_vault()
      tests the BASE NAME ONLY and ignores the schema, so a feed landed as
      `01_usnc_bronze_dev.some_raw.nhl_invoice_line` would pass it and then match a real
      vault table in by_table -- a false absence claim about a Bronze feed, which is the
      one thing the scope limit exists to prevent. Unreachable today (no such table
      exists, and the by_table miss would return None anyway), so the SCHEMA is required
      to be a vault schema as well, and the two conditions are separate statements so
      neither can be mistaken for the other.

    Returned as a frozenset (possibly empty, if a declared table ever projected nothing) so
    that `is None` and "is empty" stay distinguishable at the call site.
    """
    table = getattr(binding, "bronze_table", "") or ""
    parts = table.split(".")
    if len(parts) < 2 or parts[-2] not in (RAW_SCHEMA, BUSINESS_SCHEMA):
        return None
    if not naming.reads_vault(table):
        return None
    return by_table.get(parts[-1])


def projected_kinds(entity, binding) -> dict:
    """{column: (kind, declared value)} for ONE binding, straight from factory._projection.

    WHY factory._projection AND NOT spec.hub_key_components, WHICH WOULD ALSO ANSWER IT.
    _projection is the single function that decides a table's emitted shape, and it already
    calls hub_key_components internally to do so -- so reading _projection means this
    document QUOTES the loader, while walking hub_key_components separately would be a
    second implementation of the same resolution, free to drift from the one that runs.
    That is the mistake this emitter's own docstring records about `<parent key>`, and the
    reason spec.hash_key_columns was extracted in the first place.

    THE REAL BINDING IS PASSED, NOT contract_columns' None-for-a-hub. contract_columns asks
    emit_data_contract for the column NAMES, and for a hub those are entity.business_keys
    whichever binding you ask -- so its `src=None` fallback to sources[0] is harmless there.
    The (kind, value) pair is NOT binding-independent: accounting_journal's fiscal_year
    reads openyear under GP_US and hstyear under GP_US_HIST. Asking sources[0] would print
    GP_US's answer on GP_US_HIST's row, which is the specific confusion this fixes.
    """
    from accelerator import factory  # noqa: PLC0415 -- pulls pyspark; only needed here
    return {name: (kind, value) for name, kind, value in factory._projection(entity, binding)}


def is_literal(kind) -> bool:
    """True when the projection says this column is a CONSTANT, not a read."""
    from accelerator import factory  # noqa: PLC0415
    return kind == factory._BK_LITERAL


def declared_source_column(entity, binding, col) -> str | None:
    """The source column the MODEL says `col` is read from, or None when it reads none.

    ALMOST ALWAYS `col` ITSELF, and the emitter's docstring says why: the factory projects
    and renames NOTHING on a payload column, so its source name IS its target name.

    A HUB BUSINESS KEY IS THE EXCEPTION, AND IT IS NOT A RARE ONE. Measured across every
    hub binding in this model: 15 positions are LITERALS and 36 are RENAMES, against only
    3 that are genuinely same-name. reference_type is the constant 'GP_Journal_Entry';
    reference_id is read from jrnentry; fiscal_year is read from openyear under GP_US and
    from hstyear under GP_US_HIST. Every one of those was documented as "direct, same
    name", and these are HASH-KEY COMPONENTS -- so a reader asking why two systems' keys
    do not join was being shown two identical rows for two different Bronze columns.
    """
    kind, value = projected_kinds(entity, binding).get(col, ("", col))
    return None if is_literal(kind) else value


def unsourced_note(entity, binding, col, src_name) -> str:
    """Why a column the source does not provide is in the target table anyway.

    THE MODEL ANSWERS THIS FOR A HUB BUSINESS KEY AND FOR NOTHING ELSE, and the note says
    which case it is in rather than printing one sentence over both. A key_literal is a
    constant the loader writes; a key column is a real read that this source simply does not
    satisfy; the concatenated _bk is built from those, never read.

    FOR EVERY OTHER COLUMN THE HONEST ANSWER IS "the model does not say". A computed
    satellite's payload column can be unsourced because its inputs are absent from the
    source or because the rule itself is undefined and open with the repo owner -- and that
    distinction is recorded only in PROSE (the entity YAML's comments,
    metadata/source_unions.yml's comments, tests/test_accelerator.py's _GIE_OPEN dict).
    None of those is a declaration this emitter reads, or could read. Printing a guess would
    be the same failure this change exists to remove, one level up.
    """
    if entity.kind == "hub":
        kind, value = projected_kinds(entity, binding).get(col, ("", col))
        if col in entity.business_keys:
            if is_literal(kind):
                return (f"a hub business key declared as a key_literal: the loader writes "
                        f"the constant {value!r} and reads nothing, so no column of "
                        f"{src_name} -- or of any source -- feeds it")
            return (f"a hub business key read from {value!r}, and {src_name} does not "
                    f"provide that column either")
        if col == naming.bk(entity.name):
            return (f"the hub's concatenated business key, built from the business-key "
                    f"columns above at load time rather than read from {src_name}")
    return (f"this column is NOT copied from the source. WHY is not in the model: the "
            f"reason -- inputs that are absent from {src_name}, a rule that is undefined "
            f"and still open with the repo owner, or a value the loader is meant to supply "
            f"-- is recorded only in prose (the entity YAML, metadata/source_unions.yml, "
            f"and tests/test_accelerator.py's _GIE_OPEN), so this document does not guess "
            f"between them")


def rows_for(entity, binding, table: str, model, own_scopes=None, by_table=None) -> list[dict]:
    """Every target column of one table under one binding, in emitted order."""
    schema = naming.vault_schema_for(entity.kind, RAW_SCHEMA, BUSINESS_SCHEMA)
    masks = dict(getattr(entity, "masks", ()) or ())
    casts = dict(getattr(binding, "cast", ()) or ())
    parent_keys = dict(getattr(binding, "parent_keys", ()) or ())
    src_table = getattr(binding, "bronze_table", "") or ""
    out: list[dict] = []

    def row(col, rule, source_cols, notes=""):
        out.append({
            "target_schema": schema,
            "target_table": table,
            "binding": binding.name,
            "target_column": col,
            "target_type": contract.column_type(entity, binding, col),
            "rule": rule,
            "source_table": src_table if source_cols else "",
            "source_columns": ", ".join(source_cols),
            "classification": contract.classify(entity, col),
            "mask": masks.get(col, ""),
            "notes": notes,
        })

    own_hk = naming.hk(entity.name)
    columns = contract_columns(entity, binding)
    # {column: (hash components, source scope)} for every _hk this binding derives -- the
    # same call factory.py makes, so the mapping quotes the loader rather than describing it.
    keys = spec.hash_key_columns(model, entity, binding)
    # Passed in by build(); computed here only when this is called directly, e.g. from a
    # check. It walks the whole model, so recomputing it per binding turned 36 lookups into
    # 1,332 -- 0.03s, so not a bug, but a reviewer would rightly ask why.
    own_scopes = orphan_scopes(model) if own_scopes is None else own_scopes
    # Same passed-in-by-build() shape and the same reason: it walks every entity.
    by_table = projected_columns_by_table(model) if by_table is None else by_table
    # spec._resolve_conformance() has already put the profile's expressions here, resolved
    # from metadata/source_unions.yml, so this reads the SAME declaration factory._stage_full
    # evaluates -- not a second opinion about what the profile says.
    derived = dict(getattr(binding, "derived_columns", ()) or ())
    provides = source_provides(binding, by_table)
    kinds = projected_kinds(entity, binding)
    src_name = src_table.rsplit(".", 1)[-1]

    def readable(names):
        """(the named columns the source really has, the ones it has not).

        A HASH-KEY ROW AND A HASHDIFF ROW NAME COLUMNS FOR A DIFFERENT REASON than a direct
        row does, and conflating the two is how this document came to contradict itself
        inside one table. `source_columns` means "go and look at this in the source"; the
        RULE means "this is what is hashed". A hashdiff names the PAYLOAD -- target column
        names, not source reads -- and csat_invoice_line_gie's hashdiff listed `description`
        as a column of nhl_invoice_line three rows above a row saying nhl_invoice_line has
        no column `description`.

        So where the source's shape is knowable, a component the source has not got is
        dropped from source_columns and NAMED IN THE NOTE instead. Nothing is lost: the
        rule still lists every hashed component verbatim. This is the same split
        _source_columns() already makes for literals, for the same stated reason.
        """
        if provides is None:
            return list(names), []
        return ([n for n in names if n in provides],
                [n for n in names if n not in provides])

    def gap_note(missing):
        # SET, NOT LIST. A hash key names the same column once per key position it fills --
        # invoice_line_hk draws buyer_tenant from three of its four parents -- and a note
        # reading ['buyer_tenant', 'buyer_tenant', 'buyer_tenant'] says nothing the first
        # one did not. The RULE cell still shows every position, which is where the
        # repetition is information.
        return (f"; {src_name} does NOT provide {sorted(set(missing))}, so this value "
                f"cannot be "
                f"built from this source as declared -- the rule above still lists every "
                f"hashed component, and source columns lists only what is really there")

    for col in columns:
        if col in keys:
            comps, scope = keys[col]
            joins = col == own_hk or scope in own_scopes.get(col[:-3], {scope})
            if col == own_hk:
                note = "derived key, not a source column"
            elif not joins:
                # NO "hashed exactly as the parent's own loader hashes it" HERE. That phrase
                # is the joinability guarantee, and on these rows it is precisely what does
                # not hold -- saying both would be a note that contradicts itself.
                parent = col[:-3]
                note = (f"foreign key to {parent}, and it WILL NOT JOIN: "
                        f"{parent} is only ever keyed under "
                        f"{sorted(str(x) for x in own_scopes.get(parent, set()))}, never "
                        f"under {scope!r}, so this value can never equal the key its parent "
                        f"builds. Acknowledged in metadata/key_scope_exceptions.json")
            else:
                # A parent foreign key is hashed as THAT parent's own loader hashes it --
                # which is what makes the join work by construction rather than by careful
                # copying. Worth saying on the row, because the columns listed are this
                # binding's columns while the key belongs to the parent.
                parent = col[:-3]
                kind = model.get(parent).kind if parent in _entity_names(model) else "hub"
                note = (f"foreign key to {kind}_{parent}, hashed exactly as "
                        f"{kind}_{parent}'s own loader hashes it")
            if scope and joins:
                note += (f"; source-scoped, so {binding.name}'s keys never collide with "
                         f"another system's identical value")
            _have, _missing = readable(_source_columns(comps))
            if _missing:
                note += gap_note(_missing)
            row(col, _hash_rule(comps, scope), _have, note)
        elif col == "hashdiff":
            # `src.payload or entity.payload` IS THE LOADER'S RULE -- factory.py:445 hashes
            # exactly that, and factory.py:489 says so in as many words. Reading only the
            # binding's payload agreed with it for every binding in the model today (there
            # is none with an empty payload and a non-empty entity payload), and would have
            # printed "SHA-256 over the payload (0 column(s))" for the first one that
            # appeared, silently, while the loader hashed the entity's.
            pay = list(getattr(binding, "payload", ()) or entity.payload or ())
            _have, _missing = readable(pay)
            _note = "change detection: a new version is written only when this differs"
            if _missing:
                _note += gap_note(_missing)
            row(col, f"SHA-256 over the payload ({len(pay)} column(s)) as BINARY(32); "
                     f"rulebook {hashing.RULEBOOK_VERSION}", _have, _note)
        elif entity.kind == "hub" and col == naming.bk(entity.name):
            # THE READABLE BUSINESS KEY IS CONCATENATED, NOT READ. factory.py:424 builds it
            # as concat_ws('||', <the business-key parts>) -- literals cast to string, key
            # columns cast to string -- and it exists nowhere in any source. Every hub
            # binding documented it as "direct, same name" from its Bronze table, which is
            # the same assumption the payload fallback made, one column along.
            # QUOTED BY THE FUNCTION THE LOADER USES. This comprehension was a second copy
            # of spec.key_components_to_hash_columns, and an unescaped one: a business key
            # literal containing an apostrophe would have been DOCUMENTED as a truncated
            # CONCAT_WS, which is the shape of the bug rather than a report of it.
            _parts = spec.key_components_to_hash_columns(
                [(is_literal(_k), _v)
                 for _k, _v in (kinds.get(_b, ("", _b)) for _b in entity.business_keys)])
            _have, _missing = readable([p for p in _parts if not p.startswith("'")])
            _note = ("the readable business key: built at load time from the business-key "
                     "parts above, never joined on and never read from the source")
            if _missing:
                _note += gap_note(_missing)
            row(col, f"CONCAT_WS('||', {', '.join(_parts)}) as STRING", _have, _note)
        elif col in naming.SYSTEM_COLUMNS:
            mapped = {
                "applied_dts": getattr(binding, "applied_dts_column", None),
                "cdc_op": getattr(binding, "cdc_op_column", None),
                "manifest_id": getattr(binding, "manifest_column", None),
            }.get(col)
            if mapped:
                row(col, "direct from the source's own clock/marker column", [mapped],
                    "a business column, not a load timestamp")
            else:
                row(col, "generated at load time", [],
                    "no bronze origin: written by the loader")
        elif is_literal(kinds.get(col, ("", col))[0]):
            # A CONSTANT IS NOT AN ABSENCE, so this sits AHEAD of the presence gate: asking
            # whether the source has a column called `reference_type` is the wrong question
            # when the loader writes 'GP_Journal_Entry' into it and reads nothing. Placing
            # it here also makes a key_literal read the SAME on a Bronze binding and on a
            # vault one, which a NOT SOURCED row would not.
            row(col, f"literal {kinds[col][1]!r}, written by the loader", [],
                "a hub business key declared as a key_literal: a CONSTANT hashed into the "
                "key, not a column read from the source. Two bindings declaring different "
                "literals for the same position build keys that can never join")
        elif col in derived:
            # THE RULE IS THE PROFILE'S OWN SQL, CHARACTER FOR CHARACTER, because a
            # paraphrase of a derivation is a second implementation of it -- and this
            # document's whole claim is that it cannot disagree with what loads.
            #
            # THE SOURCE COLUMNS ARE READ OUT OF THE EXPRESSION, not assumed to be [col].
            # extract_identifiers is the pure function tests/test_spark_derivation.py's
            # synthetic frame is built from, so the mapping and that suite cannot disagree
            # about what an expression reads. It is deliberately generous (it strips string
            # literals and known keywords and keeps the rest), which is the right bias: a
            # surplus identifier is visible and wrong-looking, a missing one is invisible.
            #
            # AN EMPTY SET IS A REAL ANSWER, NOT A FAILURE. rule_version's expression is the
            # literal '1.0.0' -- it reads no column at all -- so source_columns is blank and
            # row() therefore leaves source_table blank too, which is exactly right: there
            # is nothing in the source to go and look at.
            reads = sorted(staging_columns.extract_identifiers(derived[col]))
            rule = f"DERIVED at staging: {derived[col]}"
            if col in casts:
                rule += f"; then CAST to {casts[col]}"
            row(col, rule, reads,
                f"computed, not copied: the expression is the {binding.conform!r} conform "
                f"profile's, from metadata/source_unions.yml, applied by "
                f"factory._stage_full"
                + ("" if reads else ". It reads NO source column -- it is a literal"))
        elif provides is not None and declared_source_column(entity, binding,
                                                             col) not in provides:
            # NOT "direct, same name", AND NOT A BARE CAST. Both of those are claims that
            # the source has a column of this name, and we have just measured that it does
            # not. source_columns is left EMPTY, which also blanks source_table via row() --
            # naming a table a reader would then go and query for a column it does not have
            # is the same lie one cell to the left.
            want = declared_source_column(entity, binding, col)
            cast_part = (f"declared CAST to {casts[col]}, but " if col in casts else "")
            named = f"no column {want!r}" if want else "no source column at all"
            row(col, f"NOT SOURCED: {cast_part}{src_name} has {named}, and this binding "
                     f"declares no derivation for it", [],
                unsourced_note(entity, binding, col, src_name))
        else:
            # THE PROJECTION SAYS WHAT THIS COLUMN READS, AND FOR A HUB BUSINESS KEY IT IS
            # NOT THE COLUMN'S OWN NAME. Before this, every hub business key printed
            # "direct, same name" and its own name: 15 of them are LITERALS baked into the
            # hash key, and 36 are RENAMES. The worst shape is a target that reads a
            # DIFFERENT Bronze column per binding -- accounting_journal.fiscal_year is
            # openyear under GP_US and hstyear under GP_US_HIST -- which the old emitter
            # rendered as two identical rows, so a reader debugging why two systems' keys
            # will not join could not discover the difference existed.
            kind, value = kinds.get(col, ("", col))
            if col in casts:
                row(col, f"CAST to {casts[col]}"
                         + ("" if value == col else f", read from {value}"), [value],
                    "declared cast; source-conformance probes that every value survives it"
                    + ("" if value == col else
                       f". The source column is {value!r}, not {col!r}"))
            elif value != col:
                row(col, f"direct, RENAMED from {value}", [value],
                    f"the generator renames nothing on a payload column, but a hub business "
                    f"key is positional: {binding.name} declares {value!r} for this key "
                    f"position, and another binding of the same hub may declare a different "
                    f"column for it")
            else:
                row(col, "direct, same name", [col],
                    "the generator renames nothing")
    return out


def contract_columns(entity, binding):
    """The emitted column list for one table, from the contract emitter's own walk."""
    from emit_data_contract import _projected_columns
    src = binding if entity.kind in naming.SATELLITE_KINDS else None
    return _projected_columns(entity, src)


def build(model) -> list[dict]:
    """Every mapping row in the model, ordered for reading."""
    out: list[dict] = []
    own_scopes = orphan_scopes(model)
    by_table = projected_columns_by_table(model)
    for entity in model.entities:
        # THE VIEW, NOT THE TABLE. This document is read by a person tracing where a
        # target column comes from, and the table they will actually query is the
        # stable, unversioned view -- the physical _rev<N> table is what a cutover
        # moves out from under them. See naming.stable().
        for src, table in entity.stable_tables():
            # A satellite's table belongs to ONE binding. Every other kind is one conformed
            # table fed by all of them, so it is emitted once per binding.
            for binding in spec.table_bindings(entity, src):
                out.extend(rows_for(entity, binding, table, model, own_scopes, by_table))
    return out


def table_facts(model) -> list[dict]:
    """Per (table, binding) facts that are not column-level: dedup, expectations, grain."""
    facts = []
    for entity in model.entities:
        # Same table identity as build()'s mapping rows -- the stable view a reader
        # would look up, not the physical table a cutover renames out from under them.
        for src, table in entity.stable_tables():
            for binding in ([src] if src is not None else list(entity.sources)):
                facts.append({
                    "schema": naming.vault_schema_for(entity.kind, RAW_SCHEMA,
                                                      BUSINESS_SCHEMA),
                    "table": table,
                    "entity": entity.name,
                    "kind": entity.kind,
                    "domain": getattr(entity, "domain", "") or "unassigned",
                    "binding": getattr(binding, "name", ""),
                    "source_table": getattr(binding, "bronze_table", "") or "",
                    "dedup_by": ", ".join(getattr(binding, "dedup_by", ()) or ()),
                    "dedup_order": ", ".join(getattr(binding, "dedup_order", ()) or ()),
                    "expectations": ", ".join(getattr(binding, "expectations", ()) or ()),
                    "grain": getattr(entity, "grain", "") or "",
                    "sensitivity": getattr(entity, "sensitivity", "") or "",
                })
    return facts


def workbook_sheets(rows, facts) -> list:
    """The three sheets, in reading order.

    WHY THREE AND NOT ONE. A single sheet is what the CSV already is; if the workbook were
    only that, there would be no reason to have asked for it. The per-binding facts (dedup
    key, dedup order, expectations, grain) are a different grain from the column mapping and
    do not belong as repeated columns on 469 rows, and the unjoinable keys are the five rows
    a reader most needs to find and would otherwise have to know to filter for.

    THE WARN SHEET IS NOT A SUMMARY OF THE OTHERS. It carries the same row objects, so it
    cannot say something the mapping sheet does not -- a hand-written summary sheet would be
    free to drift from the rows it summarises, which is the shape this whole document exists
    to avoid.
    """
    from xlsx_writer import Sheet, S_WARN

    warn_rows = [r for r in rows if "WILL NOT JOIN" in str(r["notes"])]
    # Mapping-sheet columns, in the CSV's order so the two read the same way.
    widths = [15, 34, 15, 30, 14, 62, 46, 34, 12, 22, 70]
    mono = frozenset({1, 3, 4, 6, 7})

    def warn_style(rows_list):
        flags = ["WILL NOT JOIN" in str(r["notes"]) for r in rows_list]
        return lambda i: S_WARN if flags[i] else 0

    sheets = [
        Sheet("Mapping",
              [c.replace("_", " ") for c in COLUMNS],
              [[r[c] for c in COLUMNS] for r in rows],
              widths=widths, row_style=warn_style(rows), mono_columns=mono),
        Sheet("Tables",
              ["schema", "table", "kind", "domain", "binding", "source table", "grain",
               "dedup by", "dedup order", "expectations", "sensitivity"],
              [[f["schema"], f["table"], f["kind"], f["domain"], f["binding"],
                f["source_table"], f["grain"], f["dedup_by"], f["dedup_order"],
                f["expectations"], f["sensitivity"]] for f in facts],
              widths=[15, 34, 8, 12, 16, 46, 12, 34, 28, 30, 12],
              mono_columns=frozenset({1, 4, 5, 7, 8})),
    ]
    if warn_rows:
        sheets.append(
            Sheet("Will not join",
                  [c.replace("_", " ") for c in COLUMNS],
                  [[r[c] for c in COLUMNS] for r in warn_rows],
                  widths=widths, row_style=lambda i: S_WARN, mono_columns=mono))
    return sheets


def render_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({k: r[k] for k in COLUMNS})
    return buf.getvalue()


def main() -> None:
    model = spec.load_model(ROOT / "metadata" / "entities")
    rows = build(model)
    facts = table_facts(model)
    CSV_PATH.write_text(render_csv(rows), encoding="utf-8")
    import xlsx_writer
    xlsx_writer.write(XLSX_PATH, workbook_sheets(rows, facts))
    from emit_s2t_html import render_html  # noqa: E402
    HTML_PATH.write_text(render_html(rows, facts), encoding="utf-8")
    n_warn = sum(1 for r in rows if "WILL NOT JOIN" in str(r["notes"]))
    for _p in (CSV_PATH, XLSX_PATH, HTML_PATH):
        print(f"wrote {_p.relative_to(ROOT)}")
    print(f"{len(rows)} mapping row(s) over {len(facts)} (table, binding) pair(s); "
          f"{n_warn} foreign key(s) flagged WILL NOT JOIN")


if __name__ == "__main__":
    main()
