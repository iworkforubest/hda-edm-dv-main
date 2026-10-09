"""Every source column one binding needs in scope, derived PURELY from the model.

WHY THIS IS ITS OWN MODULE, AND NOT PART OF tests/test_spark_derivation.py. That file
imports pyspark at module scope (see its own docstring on why: SPARK_LOCAL_IP,
PYSPARK_PYTHON, JAVA_HOME all have to be set before the JVM is touched), so nothing
offline can import anything it defines -- including this column derivation, which is pure
Python with no Spark dependency at all. Keeping it there meant the one thing every offline
gate would most want to check -- "does the synthetic frame this suite builds actually carry
every column the binding's SQL needs?" -- could only be checked inside the Spark job, which
runs in exactly one CI step and nowhere else.

WHAT WENT WRONG WITHOUT IT. tests/test_spark_derivation.py built its synthetic frame from
DECLARED columns only -- hash_key_columns components, payload, mas_key, dedup_by/order,
cast, the system columns. That is everything spec.hash_key_columns and the binding's own
fields can name. It is NOT everything factory._stage_full's emitted SQL references, because
`conform:` resolves a row_filter and a set of derived_columns from metadata/source_unions.yml
onto the binding (see spec._resolve_conformance), and those expressions name RAW source
columns the declared-column set never mentions -- fieldglass_us_invoice's row_filter alone
references Buyer_Code, Invoice_ID, Is_Test, Mike_Tester, META_run_id and PO_2, none of which
are a hash-key component, a payload column, or anything else the old function walked. Ten
bindings across invoice and the HubSpot hierarchy failed `key derivation in real Spark`
with UNRESOLVED_COLUMN for exactly this reason: the model was correct, the SQL was correct,
and the test's synthetic frame was missing the columns the SQL reads.

ONE IMPLEMENTATION, NOT TWO. tests/test_spark_derivation.py used to carry its own copy of
this walk, and its own docstring already carries the scar tissue from the LAST time this
area grew a second implementation instead of asking the authority (spec.hash_key_columns):
an earlier version re-derived key columns from key_columns/parent_keys directly and got
four link-parented satellites wrong. This module is that authority's column-scope
counterpart -- extended once, here, rather than copied.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from . import spec

if TYPE_CHECKING:  # pragma: no cover -- for type checkers only, no runtime import needed
    from .spec import Entity, Model, SourceBinding

# SQL keywords and built-in function names that can appear as bare identifiers in a
# row_filter or a derived_columns expression but name no column. Case-insensitive: the
# expressions in metadata/source_unions.yml mix case freely (CAST, cast, Cast never
# appears but NULLIF/nullif could).
_SQL_KEYWORDS = frozenset(w.upper() for w in (
    "AND", "OR", "NOT", "NULL", "IS", "AS", "CAST", "STRING", "INT", "BIGINT",
    "DOUBLE", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN", "THEN", "ELSE", "END",
    "CASE", "WHEN", "LIKE", "IN", "BETWEEN", "COALESCE", "NULLIF", "TRIM", "LOWER",
    "UPPER", "CONCAT", "CONCAT_WS", "SHA2", "SUBSTR", "SUBSTRING", "LENGTH",
    "REPLACE", "TO_DATE", "ABS", "ROUND",
    # DEF-58's GIE profile uses this. Left out, REGEXP_REPLACE is harvested as a COLUMN
    # NAME and tests/test_spark_derivation.py builds a synthetic frame carrying a column
    # called REGEXP_REPLACE -- harmless, per this module's own over-inclusion rule, but
    # it is noise standing exactly where a real missing column would appear, and
    # tests/test_accelerator.py now compares this extraction against nhl_invoice_line's
    # real shape, where noise is a FAILURE rather than a harmless extra.
    # ONLY what a shipped expression actually uses: INSTR was added here alongside it and
    # appears in no expression in this repo, which is a keyword list drifting ahead of
    # the SQL it describes.
    "REGEXP_REPLACE",
))

# A single-quoted SQL string literal, e.g. 'AEE1', '||', '^^', 'true'. Non-greedy so
# adjacent literals in the same expression are matched separately, and DOTALL is not
# needed -- these expressions are single-line by construction (YAML >-  folds them).
_STRING_LITERAL = re.compile(r"'[^']*'")

# A candidate identifier: starts with a letter or underscore, then letters/digits/
# underscores. This also matches function names (COALESCE, SHA2) and keywords (AND, AS),
# which is why _SQL_KEYWORDS is filtered afterward rather than trying to exclude them here
# -- generous extraction, precise filtering.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# A bare number (no letters), which _IDENTIFIER never matches anyway since it requires a
# leading letter/underscore -- kept as an explicit, documented no-op guard rather than
# silently relying on that. NULLIF(TRIM(Rate), '') has no bare numeric literal in this
# model today, but the digest function's ", 256)" length argument in line_reference does,
# and 256 is matched by neither pattern because it has no leading letter -- listed here so
# the exclusion is a decision, not an accident.
_PURE_NUMBER = re.compile(r"^\d+$")


def extract_identifiers(expr: str) -> set[str]:
    """Every plausible column identifier referenced by a SQL expression.

    BE GENEROUS RATHER THAN PRECISE. An extra column in a synthetic test frame is
    harmless -- Spark ignores columns _stage_full's SQL does not reference. A MISSING one
    is the exact bug this module exists to close. So this errs toward over-inclusion:
    string literals are stripped (the only thing actively wrong to include, since
    'true'/'AEE1'/'||'/'^^' would masquerade as columns otherwise), keywords and the fixed
    function list are dropped, and everything else that looks like an identifier --
    including any function name this list does not yet know about -- is kept.
    """
    if not expr:
        return set()
    without_literals = _STRING_LITERAL.sub(" ", expr)
    found: set[str] = set()
    for match in _IDENTIFIER.findall(without_literals):
        if match.upper() in _SQL_KEYWORDS:
            continue
        if _PURE_NUMBER.match(match):
            continue
        found.add(match)
    return found


def source_columns(model: "Model", entity: "Entity", binding: "SourceBinding") -> list[str]:
    """Every source column this binding needs in scope for _stage_full to run.

    THE KEY COLUMNS COME FROM spec.hash_key_columns, not from the binding's raw fields.
    An earlier version of this walked key_columns and parent_keys itself and got four
    bindings wrong -- a link-parented satellite hashes the LINK's key, which draws on the
    link's transaction_key and its parents' columns, none of which appear anywhere in the
    satellite's own declarations. Ask the authority; add only what it cannot know about
    (payload, mas_key, the system-column sources, and now the conform profile's own SQL).

    PLUS EVERY COLUMN conform: RESOLVES ONTO THE BINDING. spec._resolve_conformance sets
    binding.row_filter and binding.derived_columns from metadata/source_unions.yml, and
    factory._stage_full emits both verbatim as SQL against the raw bronze columns -- a
    filter predicate and a set of conformed-name <- raw-expression renames. Those raw names
    are declared nowhere else on the binding, so they are extracted from the expressions
    themselves via extract_identifiers, deliberately generously.

    DEDUPED CASE-INSENSITIVELY, AND THE DECLARED NAME WINS. Spark resolves column names
    case-insensitively, so a frame carrying both `invoice_id` (the declared hash-key
    component) and `Invoice_ID` (harvested from fieldglass_us_invoice's row_filter --
    required_columns names it, and the filter tests it IS NOT NULL) is not two columns to
    Spark, it is one ambiguous reference -- measured: staging invoice/FIELDGLASS_US raised
    [AMBIGUOUS_REFERENCE] Reference `Invoice_ID` is ambiguous, could be: [`Invoice_ID`,
    `Invoice_ID`]. Adding the harvested identifiers with `set |=` could never have seen
    that collision, because 'invoice_id' and 'Invoice_ID' are different strings and hash
    differently in a Python set. So this walks the same additions in the same order as
    before -- key components, then payload/mas_key/dedup/cast/system columns, then
    row_filter, then derived_columns -- but keeps only the FIRST spelling seen per
    case-folded name. That order is what makes the declared name win: it is always added
    before anything extract_identifiers harvests, so a later case-variant of the same
    identifier is dropped rather than the earlier one.
    """
    cols: dict[str, str] = {}   # case-folded name -> first-seen original spelling

    def add(name: str) -> None:
        key = name.casefold()
        if key not in cols:
            cols[key] = name

    for _col, (components, _scope) in spec.hash_key_columns(model, entity, binding).items():
        for c in components:
            if not c.startswith("'"):
                add(c)
    for c in set(binding.payload or entity.payload or ()):
        add(c)
    for c in (entity.mas_key or ()):           # msat: the multi-active subsequence key
        add(c)
    for c in set(binding.dedup_by or ()) | set(binding.dedup_order or ()):
        add(c)
    for c, _t in (binding.cast or ()):
        add(c)
    for c in (binding.applied_dts_column, binding.cdc_op_column, binding.manifest_column):
        if c:
            add(c)

    for c in extract_identifiers(binding.row_filter):
        add(c)
    for left, right in (binding.derived_columns or ()):
        add(left)                            # _stage_full may reference it downstream
        for c in extract_identifiers(right):
            add(c)

    return sorted(cols.values())
