"""What the data contract says about a table, derived from the model that already says it.

Separate from tools/emit_data_contract.py deliberately: the derivations here are the part
that can be WRONG, and the part the offline suite can fire in both directions. YAML
rendering is neither. Same split as reject_digest.py against supersede_quarantine.py.

CLASSIFICATION IS DERIVED, NOT DECLARED. The model already carries entity.sensitivity,
validated at spec.py:583 as one of internal/personal/financial/restricted, and
verify_repo.py refuses a sensitive entity that declares no masks. An earlier draft of this
work proposed adding a per-column security_classification field; that would have been a
SECOND AUTHORITY for one concept, which is the trap this repo has been bitten by twice
(BUSINESS_KINDS, the system-column set) and which key_composition.json exists to catch.

THE DERIVATION IS TOTAL ONLY BY A RULE THIS FILE DOES NOT ENFORCE. It holds if, and only
if, no `internal` entity ever declares a mask -- verify_repo.py:610-614 refuses a SENSITIVE
entity with NO masks, but nothing in the repo refuses an INTERNAL entity WITH one. Today no
internal entity declares a mask, so the property holds by the current state of the model,
not by a rule that would catch a future violation. The suite's two totality checks --
"no masked column classifies as internal" and "no unmasked column classifies above
internal" -- are therefore not merely tests of this derivation. The first of them IS the
enforcement: if an internal entity ever gains a mask, that check is what would catch it,
and if it is weakened or removed, the contract would silently publish a masked column as
`internal`.
"""

from __future__ import annotations

import re

from . import naming, spec
from .hashing import key_type_sql, zero_key_sql

__all__ = ["classify", "column_type", "grain", "foreign_keys", "clustering"]

SOURCE_DERIVED = "source-derived"


def classify(entity, column: str) -> str:
    """The column's security classification.

    A column carrying a mask takes the entity's declared sensitivity; every other column is
    internal. Nothing here invents a level: spec.py validates exactly four, so a fifth
    would be unmappable by any consumer reading the contract.
    """
    masked = {c for c, _fn in entity.masks}
    return entity.sensitivity if column in masked else "internal"


def _hash_key_width() -> int:
    """Byte width of a hash key, READ off hashing.zero_key_sql() rather than hardcoded.

    hashing.py is a RATIFIED rulebook this module must never restate a constant from --
    zero_key_sql() already renders the width (as the hex length of its zero literal), so
    this parses that instead of asserting "32" as a second, driftable copy. For the
    ratified sha2_256/BINARY_OUTPUT=True combination this is UNHEX('00'*32), 64 hex
    chars, width 32. Returns 0 for xxhash64 (zero_key_sql() has no hex literal there),
    which is harmless: column_type only appends a width when key_type_sql() == "BINARY",
    and xxhash64 reports "BIGINT".
    """
    hex_literal = re.search(r"'([0-9a-fA-F]*)'", zero_key_sql())
    return len(hex_literal.group(1)) // 2 if hex_literal else 0


def _hash_type_sql() -> str:
    """The SQL type of anything the ratified rulebook hashes -- a hash key or a hashdiff.

    ONE ANSWER FOR BOTH, because hashing.py computes both through the same `_algo_sql()`
    call (hash_key() and hashdiff() at hashing.py:266 and :289 respectively), so a
    hashdiff is exactly as wide and exactly as BINARY as a hash key. Read off
    key_type_sql()/zero_key_sql() rather than restated: hashing.py is a RATIFIED rulebook
    and this module must never carry a second copy of a constant from it.
    """
    kt = key_type_sql()
    return f"{kt}({_hash_key_width()})" if kt == "BINARY" else kt


def column_type(entity, src, column: str) -> str:
    """The column's type, or SOURCE_DERIVED where the model genuinely does not know it.

    FOUR THINGS ARE KNOWN OFFLINE and one is not. Hash keys are whatever the ratified
    rulebook stores -- BINARY(32) today, with the width read from hashing.zero_key_sql()
    rather than hardcoded, and the exemplar contract's STRING(40) under SHA-1 would simply
    be false here. THE HASHDIFF IS THE SAME TYPE for the same reason: hashing.hashdiff()
    ends in the same `_algo_sql(...)` call that hash_key() does, so it is BINARY(32) too,
    and checks/load_satellites.py:278 says so outright ("a BINARY hashdiff into a
    TIMESTAMP load_dts"). It is COMPUTED BY THE RULEBOOK, not delivered by Bronze, so
    marking it source-derived understated what the model knows -- the same class of error
    as overstating a type, in the other direction. SIX of the seven system columns are
    fixed by factory._system_columns and
    declared in naming.SYSTEM_COLUMN_TYPES; manifest_id is the exception -- naming.py
    records that it is assigned via a bare F.col(...) with no cast when a binding declares
    a manifest column, so its true type passes through from Bronze and nothing verifies
    that the STRING declared here actually matches it. A binding's `cast` gives the type
    for the few columns cast for hashdiff stability.

    `src` MAY BE None. entity.tables() returns (None, base_table) for every kind except
    sat/msat/esat -- so a caller driving from tables(), as Task 3's emitter does, passes
    src=None for hub/link/nhl/hal/csat, and without a fallback every cast column on those
    kinds (13 of 21 entities, including general_journal_line's debitamt/crdtamnt) would
    silently degrade to SOURCE_DERIVED despite the model knowing the cast type. Falls back
    to entity.sources[0] in that case -- same fallback, same reason, as
    reject_digest.digest_columns (reject_digest.py:72) and checks/load_satellites.py's
    `proj_src` (load_satellites.py:431). spec.validate refuses an entity with no sources,
    so entity.sources[0] is always defined when src is None.

    Everything else -- every column that is neither hashed by the rulebook, nor a declared
    system column, nor cast by the binding -- is a payload column whose type comes from
    Bronze at run time.
    factory._derived_schema_ddl reads it from the staged frame and needs Spark, so an
    OFFLINE emitter cannot know it. It is marked, not guessed: a contract that states a
    wrong type is worse than one that admits a gap, and omitting the column entirely would
    understate the schema.
    """
    if column.endswith("_hk") or column == naming.COL["hashdiff"]:
        return _hash_type_sql()
    if column in naming.SYSTEM_COLUMN_TYPES:
        return naming.SYSTEM_COLUMN_TYPES[column]
    proj_src = src if src is not None else (entity.sources[0] if entity.sources else None)
    if proj_src is not None:
        for cast_column, cast_type in proj_src.cast:
            if cast_column == column:
                return cast_type.upper()
    return SOURCE_DERIVED


def grain(entity, columns) -> list[str]:
    """The columns whose combination append_only_check asserts unique.

    MIRRORS THAT GATE'S RULE, and the mirroring is the point: a contract whose primary_key
    claims a grain the gate does not police is worse than one that claims none.

    Read checks/append_only_check.py:173-180 before changing this. Its satellite rule picks
    the parent key as `next((c for c in sorted(cols) if c.endswith("_hk")), None)` -- the
    FIRST _hk column in sorted order -- and NOT naming.hk(entity.parents[0]). Those two can
    differ, and publishing the second would declare a grain the gate never checks. `columns`
    is the projected column list so the same rule can be applied to the same input.
    """
    if entity.kind in naming.SATELLITE_KINDS:
        parent_hk = next((c for c in sorted(columns) if c.endswith("_hk")), None)
        if parent_hk is None:
            raise ValueError(
                f"{entity.base_table}: no parent hash key among {sorted(columns)[:6]}, so "
                f"append_only_check has no grain to assert and the contract has no "
                f"primary key to declare."
            )
        cols = [parent_hk, naming.COL["load_dts"], naming.COL["sub_seq"]]
        if naming.COL["mas_key"] in columns:
            cols.append(naming.COL["mas_key"])
        # MIRRORS THE GATE'S OWN FILTER (append_only_check.py:182,
        # `grain = [g for g in grain if g in cols]`). load_dts and sub_seq were appended
        # unconditionally here: no satellite in this model lacks either, so the two agreed
        # by the state of the model rather than by rule, and one that did lack a column
        # would have had it declared as a primary_key component the gate never grouped by
        # -- a claim no consumer could act on.
        return [g for g in cols if g in columns]
    return [naming.hk(entity.name)]


def description(entity, column: str, model=None) -> str:
    """What this column MEANS, or "" when only a person can say.

    FOUR SOURCES, IN THIS ORDER, and the order is the point:

      1. the entity's own `descriptions:` map -- a human wrote it for this column here;
      2. naming.COLUMN_DOC -- the technical columns, documented once for the whole vault;
      3. a DERIVED sentence for the keys, whose meaning follows from the model rather than
         from anyone's judgement: a hash key is this object's identity, a parent key is a
         foreign key to a named table, a readable business key is never joined on;
      4. nothing.

    RETURNING "" IS A REAL ANSWER AND MUST STAY CHEAP. 133 distinct business column names
    carry no description today, and the honest state of a column nobody has described is
    blank -- not a sentence restating its own name. A generator that filled those in would
    produce 163 tautologies that read as documentation and cannot be told from the real
    thing later, which is worse than the gap.
    """
    declared = dict(getattr(entity, "descriptions", ()) or ())
    if declared.get(column):
        return declared[column]

    if column in naming.COLUMN_DOC:
        return naming.COLUMN_DOC[column]

    if column == naming.bk(entity.name):
        return (f"The business key of this {entity.kind} in readable form, for a human "
                f"reading a row. Never joined on -- {naming.hk(entity.name)} is the join.")

    if column == entity.hk_column:
        return (f"Hash key: this {entity.kind}'s identity, SHA-256 over its declared key "
                f"components. This is what everything joins on.")

    if column.endswith("_hk") and model is not None:
        ref = foreign_keys(entity, model).get(column)
        if ref:
            parent_table = ref.rsplit(".", 1)[0]
            return (f"Foreign key to {parent_table}, hashed exactly as that table's own "
                    f"loader hashes it -- so the join holds by construction rather than by "
                    f"careful copying.")

    return ""


def foreign_keys(entity, model) -> dict:
    """column -> the entity-relative reference `{parent_base_table}.{parent_hash_key}`.

    "Parent" means the parent ENTITY, not necessarily a hub: msat_journal_line_worktag,
    msat_journal_line_external_code (parent journal_line) and
    csat_payroll_line_classification (parent payroll_detail) all reference an NHL. The
    column NAME is the same on both sides -- this entity's own copy of the parent's hash
    key, produced by the same naming.hk() the parent itself uses.

    The VALUE is deliberately qualified only to table.column, not the four-part
    catalog.schema.table.column a fully qualified reference would need: this module runs
    OFFLINE and knows neither catalog nor schema, only what the model declares. Task 3's
    emitter has both at render time and is responsible for prefixing them.

    Derived from `parents`, which is what the loaders actually hash against, so the
    contract cannot declare a relationship the vault does not compute.
    """
    # BY LEG, and the dict comprehension is why this matters: keyed on the bare hub
    # column, a hierarchical link's two legs write the same key twice and the contract
    # would publish ONE foreign key where the vault computes two. The target column keeps
    # the hub's own unroled name -- the role describes this end of the edge, not the hub's.
    return {naming.hk(p, role): f"{model.get(p).base_table}.{naming.hk(p)}"
            for p, role in spec.parent_legs(entity) if model.get(p) is not None}


def clustering(entity) -> list[str]:
    """The clustering keys this entity's tables will actually be created with.

    NOT "what the factory declares", and the difference matters on the largest tables in
    the vault. factory._cluster_by's own docstring says it "DOES NOT REACH THE HUBS OR
    THE SATELLITES": those tables are not emitted by factory.py at all, they are created
    by checks/load_hubs.py:78 and checks/load_satellites.py:158 with their own
    `CLUSTER BY` clause. What this function returns is therefore the authority only for
    the link, the NHLs and the staging tables. It is right for the hubs and satellites
    too -- but only BECAUSE those two loaders declare the same `load_dts` for the same
    26 Aug 2026 decision, which is a fact about the current state of three files rather
    than a consequence of reading one. The suite asserts the hub DDL and the satellite
    DDL each agree with what this publishes, so the three cannot drift apart silently.

    Read from factory rather than restated. The exemplar contract clusters on hash keys;
    DEF-23 measured that Delta REFUSES a BINARY hash key as a clustering column, and
    factory._cluster_refusal enforces it, so a restatement here could describe something
    the runtime will not do.

    The import is lazy on purpose: factory imports pyspark at module level and the suite
    imports this module before installing its stub. Hoisting it aborts the whole suite at
    import time. reject_digest.py does the same and for the same reason.
    """
    from .factory import _cluster_by

    return list(_cluster_by(entity))
