r"""One escape for a single-quoted SQL string literal. There is not a second one.

WHY THIS FILE EXISTS
--------------------
Four places in this repository interpolate a value into a single-quoted literal: the
hash rulebook's regex constant, the generated control DDL's COMMENT clauses, and the two
parity gates that render golden-vector inputs into a SELECT. Each grew its own escaper,
and two of them grew the WRONG one.

BACKSLASH, NOT DOUBLING, AND THAT IS MEASURED. `''` is the ANSI escape and is what most
SQL dialects take. Spark does not, and it fails in two different ways depending on where
the literal sits:

  DDL COMMENT context   '...it''s...'   -> PARSE_SYNTAX_ERROR at the second quote.
                                           Loud. The DDL simply cannot be applied.

  EXPRESSION context    SELECT 'O''Brien' -> OBrien

The second one is the reason this module exists. It PARSES, and it silently DROPS the
apostrophe: Spark reads `'O'` and `'Brien'` as two adjacent literals and concatenates
them. checks/hash_parity_check.py is the gate that runs before every load, comparing
Spark's digest against the pure-Python reference over golden vectors -- so a vector value
containing an apostrophe would have Spark hash `OBrien` while Python hashed `O'Brien`,
and the estate's first hash-parity failure would have been reported as a HASHING problem
by a gate whose actual defect was in its ESCAPING. Both wrong escapers were dead only
because no golden vector yet contains an apostrophe.

ORDER IS LOAD-BEARING. The backslash is doubled FIRST, then the quote is escaped. The
other order re-escapes the backslash this function just inserted, and ``don't`` comes back
as ``don\\'t`` -- a literal backslash followed by a terminating quote, which ends the
literal early and leaves the rest of the statement as bare SQL.

EDITING THIS FILE CHANGES RATIFIED SQL. THAT IS NOT OBVIOUS FROM ITS SIZE.
--------------------------------------------------------------------------
`escape` is reached from `hashing._regex_literal_sql`, which `hashing.hashdiff` calls to
build the tail-strip regex of EVERY SATELLITE HASHDIFF IN THE ESTATE, and from
`hashing.hash_key` and `spec.key_components_to_hash_columns` on the business-key path.
A change to either function here is a change to generated hash SQL, which is a
RULEBOOK_VERSION event -- a reviewed decision with a re-hash of every stored key behind
it, never a tidy-up of a small utility module. `hashing.py`'s own docstring says this
about `hashing.py`; it is repeated here because the person editing a two-function string
helper has no reason to go and read it.

The guards, so a change cannot be silent: tests/test_accelerator.py pins
`_regex_literal_sql(DELIMITER + NULL_TOKEN)` to its exact 14 characters, pins every
string below to what a real Spark session returned, and asserts that no second escaper
exists anywhere in the tree.

WHAT THIS DOES NOT DO
---------------------
It does not sanitise for a context OUTSIDE the literal. tools/emit_control_contract.py
also replaces `;` before calling here, because the DDL applier splits on semicolons before
it strips comments -- that is a property of the applier, not of SQL literals, and it stays
at its own call site.
"""

from __future__ import annotations


def escape(text: str) -> str:
    r"""The INSIDE of a single-quoted SQL literal: the characters between the quotes.

    Returns text with every backslash doubled and every apostrophe backslash-escaped, in
    that order. ``don't`` -> ``don\'t``; ``a back\slash`` -> ``a back\\slash``.

    Callers that need the quotes too should use `literal`; this form exists for the call
    sites that build the surrounding syntax themselves (a DDL ``COMMENT '...'`` clause,
    a DBML ``note: '...'`` attribute).
    """
    return text.replace("\\", "\\\\").replace("'", "\\'")


def literal(text: str) -> str:
    r"""`text` as a complete single-quoted SQL string literal, quotes included.

    ``literal("don't")`` returns the 8 characters ``'don\'t'``, which Spark parses back to
    the 5 characters ``don't``. Round-tripped through a real Spark session rather than
    reasoned about -- see the escape-contract checks in tests/test_accelerator.py, which
    pin the same strings this module's docstring names.

    NULL IS NOT THIS FUNCTION'S PROBLEM. A caller that must preserve the NULL/blank
    distinction decides what a Python None means in ITS context and renders that itself;
    passing None here would render the four-character string `None`, and both parity gates
    document at length why that particular accident must not happen quietly.
    """
    return "'" + escape(text) + "'"
