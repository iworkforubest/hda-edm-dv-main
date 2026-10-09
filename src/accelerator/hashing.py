"""
The hash rulebook. One implementation, used by every generated loader in every region.

WHY THIS FILE IS SPECIAL
------------------------
A hash key is only a surrogate key if every loader, every region and every re-run
produce the same value from the same business key. That property is what allows
parallel loading, idempotent re-runs, and identity to reconcile across the four
regional lakes. It is destroyed by any of: a different algorithm, a different
delimiter, a different null token, different casing/trim handling, or a different
column order.

Therefore:
  * nothing else in this repository may build a hash expression;
  * RULEBOOK_VERSION is stamped onto every generated table as a table property;
  * reference_key() / reference_hashdiff() below reimplement the rulebook in pure
    Python, and checks/hash_parity_check.py asserts that Spark's output matches them
    byte for byte. That is the spike's Q3 hash-parity gate, automated;
  * tests/test_accelerator.py pins golden digests. Changing any constant below breaks
    those tests on purpose, because it is a breaking change to every stored key in the
    estate and must be an explicit, reviewed decision -- never a tidy-up.

ALGORITHM: RATIFIED
-------------------
SHA-256, BINARY(32) output. Ratified rather than defaulted.

Rejected alternatives and why:
  * MD5 / SHA-1 -- collision-vulnerable. These keys ARE identity in a financial audit
    trail HFIG must defend per-item; a demonstrable collision path in the key
    derivation is not defensible even where a collision is unlikely in practice.
  * xxhash64 -- 8 bytes instead of 32 and materially cheaper joins, but 64 bits of
    space and no cross-platform reproducibility guarantee outside Databricks. Keys
    must be recomputable identically in four regions and verifiable by an independent
    implementation (see reference_key), which is what settles it.

The storage cost is real and should be measured, not assumed: nhl_timesheet_line
carries its own key plus three parent keys, so 4 x 32 = 128 bytes of key per row
against 32 with xxhash64. Measure it on real volumes before anyone reopens this.

STILL TO RECONCILE BEFORE FIRST LOAD
------------------------------------
The algorithm is settled. DELIMITER, NULL_TOKEN, the trim/case rules and
HASHDIFF_UPPERCASE are still the platform default and must be verified byte-for-byte
against the EDM TECH_COLUMNS_STANDARD (sample-data workbook). If the standard differs,
change it here once and let the golden tests fail loudly.
"""

from __future__ import annotations

import hashlib
import re
from typing import Sequence

from . import sql_text

# --------------------------------------------------------------------------- #
# The rulebook. Six decisions, fixed once.
# --------------------------------------------------------------------------- #
RULEBOOK_VERSION = "1.0.0"

ALGORITHM = "sha2_256"   # RATIFIED. See module docstring before changing.
ALGORITHM_RATIFIED = "sha2_256"
DELIMITER = "||"         # never a character that can appear inside a business key
NULL_TOKEN = "^^"        # never an empty string: '' and NULL must not collide
KEY_UPPERCASE = True     # business keys are case-normalised before hashing
TRIM = True              # applied before hashing, to keys and payload alike
BINARY_OUTPUT = True     # RATIFIED 25 Aug 2026. See the guard below before changing.
BINARY_OUTPUT_RATIFIED = True

# Payload case handling for CHANGE DETECTION. RATIFIED as False -- a different
# decision from key casing, and deliberately not a knob.
#
# A satellite records what a source asserted. A title corrected from
# "SENIOR JAVA DEVELOPER" to "Senior Java Developer" is a change: the source now
# asserts something different, and consumers render that text on client-facing
# documents. Fold the case and no row is inserted, so the new casing is never stored
# anywhere -- the change is lost silently and unrecoverably.
#
# The opposing risk is real but better: a source that flips casing across a whole
# column reinserts every row once. That is loud, attributable to a batch, and additive
# -- insert-only means nothing is lost. Loud/additive/recoverable beats silent/lossy.
#
# If a source IS known to flip casing spuriously, fix it where it belongs: a declared
# per-source hard rule in staging, visible in that source's metadata binding. Not a
# global switch that silently changes semantics for every entity in the estate.
#
# Note on why TRIM but not UPPER: edge whitespace carries no semantic content in these
# fields and is a transport artefact. Case carries meaning. The asymmetry is deliberate.
HASHDIFF_UPPERCASE = False
HASHDIFF_UPPERCASE_RATIFIED = False

_SUPPORTED = ("sha2_256", "sha1", "xxhash64")


class RulebookError(ValueError):
    """Raised when a hash expression cannot be built safely."""


if ALGORITHM != ALGORITHM_RATIFIED:  # pragma: no cover - guard, not logic
    raise RulebookError(
        f"ALGORITHM is {ALGORITHM!r} but the ratified algorithm is "
        f"{ALGORITHM_RATIFIED!r}. Changing it re-keys every row in every region: "
        f"raise a migration, bump RULEBOOK_VERSION and update ALGORITHM_RATIFIED in "
        f"the same reviewed change."
    )

# DEF-49: BINARY_OUTPUT was the one storage decision with no guard, and a decision on
# 25 Aug 2026 made it load-bearing: hash keys are BINARY(32) AS STORED. Until then it read
# as a tuning choice -- "smaller, faster joins" -- which is exactly how a knob that re-keys
# an estate gets turned.
#
# Flipping it to False keeps SHA-256 and changes nothing about the digest, which is what
# makes it dangerous: golden vectors compare hex, so hash_parity_check would still PASS
# while every stored key silently became a 64-character STRING. Nothing else in the repo
# would object. Joins to already-loaded BINARY keys would then match nothing.
#
# This guard changes no hash output. It only stops the value moving without a coordinated
# RULEBOOK_VERSION bump, which is the same protection ALGORITHM and HASHDIFF_UPPERCASE
# have had since the rulebook was written.
if BINARY_OUTPUT != BINARY_OUTPUT_RATIFIED:  # pragma: no cover - guard, not logic
    raise RulebookError(
        f"BINARY_OUTPUT is {BINARY_OUTPUT!r} but the ratified value is "
        f"{BINARY_OUTPUT_RATIFIED!r}. Hash keys are BINARY(32) as stored (decided "
        f"25 Aug 2026). Changing this does NOT change the digest, so the parity gate "
        f"would still pass while every stored key became a hex STRING and every join to "
        f"an already-loaded key stopped matching. Raise a migration, bump "
        f"RULEBOOK_VERSION and update BINARY_OUTPUT_RATIFIED in the same reviewed change."
    )

if HASHDIFF_UPPERCASE != HASHDIFF_UPPERCASE_RATIFIED:  # pragma: no cover
    raise RulebookError(
        "HASHDIFF_UPPERCASE has been changed from the ratified value. Case-folding "
        "the payload makes case corrections invisible AND unstored, because no row is "
        "inserted. It invalidates every stored hashdiff in the estate. If a specific "
        "source flips casing spuriously, declare a staging normalisation on that "
        "source binding instead of changing this globally."
    )


# --------------------------------------------------------------------------- #
# Key-component safety
#
# Any printable delimiter can in principle appear inside a business key, and that
# would collide: ('AB','C') and ('A','BC') produce the same pre-hash string. The
# mitigation is NOT an exotic delimiter -- a control character would be unreadable in
# logs and would not survive the copy-paste path that hash reconciliation depends on.
# The mitigation is to REJECT such keys at the gate, so a would-be silent collision
# becomes a quarantine row with a reason.
#
# Deliberately NOT applied to hashdiff payload: a job description may legitimately
# contain '||', and rejecting valid business text would be worse than the residual
# risk. That risk is a MISSED CHANGE (not a wrong identity), and it requires two
# adjacent payload columns to shift content across the boundary in a compensating way.
# Accepted, documented. If a specific free-text column turns out to contain the
# delimiter routinely, exclude it from that satellite's hashdiff rather than changing
# the rulebook.
# --------------------------------------------------------------------------- #
def key_safety_rules(columns: Sequence[str], *, mandatory: bool = True) -> dict[str, str]:
    """Expectations that must hold for every business-key component.

    mandatory=True asserts the component is present. A NULL business key component is
    a defect, not a value to hash: hashing the null token into a hub would create one
    identity that accumulates every unkeyed record from every source into a single row.
    """
    rules: dict[str, str] = {}
    for column in columns:
        safe = column.strip("`").replace(".", "_")
        text = f"CAST({column} AS STRING)"
        if mandatory:
            rules[f"key_present_{safe}"] = f"{column} IS NOT NULL AND TRIM({text}) <> ''"
        rules[f"key_no_delimiter_{safe}"] = (
            f"COALESCE(INSTR({text}, '{DELIMITER}'), 0) = 0"
        )
        rules[f"key_not_null_token_{safe}"] = (
            f"COALESCE(TRIM({text}) <> '{NULL_TOKEN}', TRUE)"
        )
    return rules


# --------------------------------------------------------------------------- #
# Column normalisation -- SQL side
# --------------------------------------------------------------------------- #
def normalise(column: str, *, uppercase: bool = True) -> str:
    """Normalise one column to its canonical pre-hash string form.

    Order of operations is part of the rulebook: cast -> trim -> [upper] -> null token.
    Casting first means numeric and date keys normalise identically everywhere rather
    than depending on the session's default string conversion.

    uppercase=True for business keys, False for descriptive payload.
    """
    expr = f"CAST({column} AS STRING)"
    if TRIM:
        expr = f"TRIM({expr})"
    if uppercase:
        expr = f"UPPER({expr})"
    # An empty string after trimming is treated as absent, deliberately: a source
    # sending ' ' and a source sending NULL mean the same thing to the business.
    return f"COALESCE(NULLIF({expr}, ''), '{NULL_TOKEN}')"


def _concat(columns: Sequence[str], *, uppercase: bool) -> str:
    if not columns:
        raise RulebookError("cannot hash an empty column list")
    parts = [normalise(c, uppercase=uppercase) for c in columns]
    if len(parts) == 1:
        return parts[0]
    return f"CONCAT_WS('{DELIMITER}', {', '.join(parts)})"


# Regex metacharacters, for _regex_literal_sql below. Kept next to it so the reason
# it exists cannot drift away from the call site that needs it.
_REGEX_METACHARS = frozenset(r"\^$.|?*+()[]{}")


def _regex_literal_sql(literal: str) -> str:
    r"""Render a rulebook constant as a SQL string constant safe to use as a regex PATTERN.

    THIS IS THE ONLY PLACE A RULEBOOK CONSTANT IS ESCAPED, and it is deliberately not
    what the other call sites do. `_concat` interpolates DELIMITER into
    ``CONCAT_WS('||', ...)`` and `normalise` interpolates NULL_TOKEN into
    ``COALESCE(..., '^^')``. Those are plain SQL string arguments and the RAW value is
    correct there. Escaping them would be a bug. Do not "make this consistent".

    A regex argument is different, and DELIMITER (``||``) and NULL_TOKEN (``^^``) are
    made entirely of regex metacharacters -- ``|`` is alternation, ``^`` is an anchor.
    Interpolated raw, ``(||^^)+$`` compiles as an alternation of two EMPTY branches and
    two anchors: it matches the empty string, strips nothing, and does so silently. That
    was DEF-13. It made the tail-strip a no-op in Spark while the pure-Python reference
    (which uses re.escape) stripped correctly, so Spark and the reference disagreed for
    every payload with a trailing NULL -- and adding a column to any satellite would have
    reinserted every row in the estate.

    TWO escaping layers, and both are load-bearing:

      1. regex   -- prefix each metacharacter with a backslash:  ||^^  ->  \|\|\^\^
      2. SQL     -- the result is then a string LITERAL, and Spark processes backslash
                    escapes inside one, so every backslash is doubled for the parser to
                    hand a single backslash to the regex engine.

    ``_regex_literal_sql("||^^")`` therefore returns the 14 characters ``'\\|\\|\\^\\^'``,
    which Spark parses to the regex ``\|\|\^\^``, which matches the literal ``||^^``.

    Nothing about the rulebook's DEFINITION changes here: the reference implementation,
    RULEBOOK_VERSION and tests/golden_hash_vectors.json are all untouched and are what
    prove this correct. Only the generated SQL moves.

    Layer 2 is sql_text.literal, which is where every single-quoted literal in this repo
    gets its quotes. This function held its own copy of that one line; two parity gates
    held a DIFFERENT copy that doubled the apostrophe instead, which Spark reads as two
    adjacent literals rather than an escape. The output here is byte-identical -- the
    delegation moves no character of the emitted SQL -- and one escaper is what stops the
    next call site picking the wrong one.
    """
    pattern = "".join(("\\" + ch) if ch in _REGEX_METACHARS else ch for ch in literal)
    return sql_text.literal(pattern)


def _algo_sql(payload: str) -> str:
    if ALGORITHM == "sha2_256":
        # unhex(sha2(...)) yields BINARY(32); sha2(...) alone yields a 64-char STRING
        return f"UNHEX(SHA2({payload}, 256))" if BINARY_OUTPUT else f"SHA2({payload}, 256)"
    if ALGORITHM == "sha1":
        return f"UNHEX(SHA1({payload}))" if BINARY_OUTPUT else f"SHA1({payload})"
    if ALGORITHM == "xxhash64":
        if BINARY_OUTPUT:
            raise RulebookError("xxhash64 returns BIGINT; set BINARY_OUTPUT = False")
        return f"XXHASH64({payload})"
    raise RulebookError(f"unsupported algorithm {ALGORITHM!r}, expected one of {_SUPPORTED}")


# --------------------------------------------------------------------------- #
# Public SQL builders -- the only sanctioned way to produce a hash in this estate
# --------------------------------------------------------------------------- #
def _validated_scope(source_scope: str) -> str:
    """The scope token, normalised -- or a refusal.

    REFUSES RATHER THAN ESCAPES, and the choice is not stylistic. A source scope is not
    data: it names the SYSTEM whose namespace a federated key lives in, it is upper-cased
    and structural, and it goes into the hashed payload ahead of every component. There is
    no legitimate source-system name containing a quote, a backslash or a NUL -- a value
    carrying one is a mis-wired binding, and escaping it faithfully would mint a NEW
    identity namespace quietly and correctly. Every key in that scope would then fail to
    join, which is the failure that does not announce itself. audit.py, invoice_issue.py
    and supersede_quarantine.py take exactly this stance on exactly this question.

    (A key LITERAL is the opposite case and is escaped rather than refused -- see
    spec.key_components_to_hash_columns. A literal is a business value that the pure-Python
    reference hashes raw, so the SQL has to parse back to it; a tenant called O'Brien is a
    legitimate declaration and refusing it would put the two sides of the parity gate under
    different rules.)
    """
    token = source_scope.strip().upper()
    if not token:
        raise RulebookError("source_scope must be a non-empty literal when supplied")
    if "'" in token or "\\" in token or "\x00" in token:
        raise RulebookError(
            f"source_scope {source_scope!r} contains a quote, backslash or NUL and cannot "
            f"be written as a SQL literal. Refusing to escape it -- a scope is a system "
            f"name, and escaping one would silently mint a different key namespace."
        )
    return token


def source_scope_literal(source_scope: str) -> str:
    """The scope as the SQL literal hash_key prepends, for anything that DESCRIBES a key.

    tools/emit_source_to_target.py prints the key rule into the source-to-target mapping and
    built this string itself, so the document could describe a literal the rulebook would
    refuse. One renderer, one answer.
    """
    return "'" + _validated_scope(source_scope) + "'"


def hash_key(columns: Sequence[str], *, source_scope: str | None = None) -> str:
    """Business-key hash for a hub, link or NHL.

    source_scope is prepended for FEDERATED keys, where no single system is
    authoritative and the source is part of identity (EDM BK definition:
    source_system + native key). It is omitted for AUTHORED keys, where one system
    mints the identifier.

    The scope is a literal, not a column, so a mis-mapped record_source column can
    never silently change identity.
    """
    if source_scope is not None:
        parts = [source_scope_literal(source_scope)] + [
            normalise(c, uppercase=KEY_UPPERCASE) for c in columns
        ]
        payload = f"CONCAT_WS('{DELIMITER}', {', '.join(parts)})"
    else:
        payload = _concat(columns, uppercase=KEY_UPPERCASE)
    return _algo_sql(payload)


def hashdiff(columns: Sequence[str]) -> str:
    """Change-detection checksum over a satellite's descriptive payload.

    Column ORDER IS PART OF THE CONTRACT. The order given here is the order declared
    in the entity's metadata file, and metadata validation rejects reordering of an
    existing payload (spec.check_payload_order). Alphabetising a payload list to tidy
    it up invalidates every stored hashdiff in the estate -- which is why it is a
    validation failure, not a review comment.

    New columns must be appended at the END. Trailing null tokens are stripped, so a
    column added but not yet populated leaves previously-stored hashdiffs valid and
    does not reinsert every row.

    The payload is NOT case-folded by default: see HASHDIFF_UPPERCASE.
    """
    if not columns:
        raise RulebookError("a satellite must declare a non-empty payload to hashdiff")
    payload = _concat(columns, uppercase=HASHDIFF_UPPERCASE)
    # DEF-13: this is a regex PATTERN, not a plain SQL string -- see _regex_literal_sql.
    tail = _regex_literal_sql(DELIMITER + NULL_TOKEN)
    payload = f"REGEXP_REPLACE({payload}, CONCAT('(', {tail}, ')+$'), '')"
    return _algo_sql(payload)


def key_type_sql() -> str:
    """SQL type of a hash key column, for DDL and ghost-record seeding."""
    if ALGORITHM == "xxhash64":
        return "BIGINT"
    return "BINARY" if BINARY_OUTPUT else "STRING"


def zero_key_sql() -> str:
    """The ghost / zero key literal. Must be stable across regions and versions."""
    if ALGORITHM == "xxhash64":
        return "CAST(0 AS BIGINT)"
    width = 32 if ALGORITHM == "sha2_256" else 20
    if BINARY_OUTPUT:
        return f"UNHEX('{'00' * width}')"
    return f"'{'0' * (width * 2)}'"


def rulebook_properties() -> dict[str, str]:
    """Stamped onto every generated table so the rulebook in force is inspectable."""
    return {
        "hfig.hash.rulebook_version": RULEBOOK_VERSION,
        "hfig.hash.algorithm": ALGORITHM,
        "hfig.hash.delimiter": DELIMITER,
        "hfig.hash.null_token": NULL_TOKEN,
        "hfig.hash.binary_output": str(BINARY_OUTPUT).lower(),
        "hfig.hash.hashdiff_uppercase": str(HASHDIFF_UPPERCASE).lower(),
    }


# --------------------------------------------------------------------------- #
# Reference implementation -- pure Python, no Spark.
#
# Exists so byte-faithfulness is provable rather than assumed. Two uses:
#   1. checks/hash_parity_check.py runs the SQL expression in Spark over known inputs
#      and asserts the result equals these functions. Run it per region.
#   2. It is the independent implementation to reconcile against the EDM record_hash
#      convention on hash-parity day. "Close" is not "equal": the control plane
#      compares hashes across the boundary and carries no translation layer.
# --------------------------------------------------------------------------- #
_TRAILING_NULLS = re.compile(f"(?:{re.escape(DELIMITER + NULL_TOKEN)})+$")


def reference_normalise(value: object, *, uppercase: bool) -> str:
    """Python mirror of normalise(). NULL and blank both become the null token."""
    if value is None:
        return NULL_TOKEN
    text = str(value)
    if TRIM:
        text = text.strip()
    if uppercase:
        text = text.upper()
    return text if text != "" else NULL_TOKEN


def _digest(payload: str) -> str:
    if ALGORITHM == "sha2_256":
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if ALGORITHM == "sha1":
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()  # noqa: S324
    raise RulebookError(f"no reference implementation for {ALGORITHM!r}")


def reference_payload_key(
    values: Sequence[object], *, source_scope: str | None = None
) -> str:
    """The exact string that gets hashed for a business key. Useful in diagnostics."""
    parts = [reference_normalise(v, uppercase=KEY_UPPERCASE) for v in values]
    if source_scope is not None:
        # THE SAME REFUSAL AS THE SQL SIDE. If only hash_key refused, the reference would
        # happily hash a scope Spark was never asked to render, and the parity gate would
        # abort on one side rather than disagree on both -- which reads as a broken gate
        # rather than a bad value.
        parts = [_validated_scope(source_scope)] + parts
    return DELIMITER.join(parts)


def reference_payload_hashdiff(values: Sequence[object]) -> str:
    """The exact string that gets hashed for a hashdiff, after tail stripping."""
    parts = [reference_normalise(v, uppercase=HASHDIFF_UPPERCASE) for v in values]
    return _TRAILING_NULLS.sub("", DELIMITER.join(parts))


def reference_key(values: Sequence[object], *, source_scope: str | None = None) -> str:
    """Lowercase hex digest of a business key. Compare against SHA2(x, 256) in Spark."""
    if not values:
        raise RulebookError("cannot hash an empty value list")
    return _digest(reference_payload_key(values, source_scope=source_scope))


def reference_hashdiff(values: Sequence[object]) -> str:
    """Lowercase hex digest of a satellite payload."""
    if not values:
        raise RulebookError("cannot hash an empty value list")
    return _digest(reference_payload_hashdiff(values))
