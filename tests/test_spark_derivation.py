#!/usr/bin/env python3
"""REAL SPARK. Exercises factory.py's key derivation against a live engine.

WHY THIS FILE EXISTS. tests/test_accelerator.py STUBS pyspark -- deliberately, so 121
structural checks run anywhere with no JVM -- which means factory.py has never been
imported by any test in this repo, let alone executed. Its key-derivation branches were
verified only by reading them and by a hand-copy in the suite that could drift. When
spec.hash_key_columns was extracted on 4 September 2026 the honest caveat on the change
was "no live load exercised any of this". This closes that caveat.

WHAT IT PROVES, AND WHY IT IS NOT hash_parity_check. Gate zero compares Spark to the
reference over 7 hand-written golden vectors: it proves the ALGORITHM travels. It says
nothing about whether the loader feeds the right columns in the right order into that
algorithm for the model's own 54 hash-key columns. This runs factory._stage_full itself,
on synthetic rows, for every binding in metadata/entities, and compares the digests SPARK
ACTUALLY PRODUCED against hashing.reference_key computed in Python from the same values.
Algorithm parity plus component parity is the whole hash contract; gate zero is one half.

WHAT IT DOES NOT PROVE, SAID PLAINLY. The reference digest is built from the SAME
spec.hash_key_columns call factory.py uses, so a wrong component list is wrong identically
on both sides and this comparison stays green. It proves the SQL-versus-hashlib path --
normalisation, the delimiter, the null token, the case rules, hashdiff tail-stripping,
sha2 against hashlib -- over the model's own 54 keys instead of 7 hand-written vectors. It
does NOT prove the component list is the intended one. metadata/key_derivation.json's
golden record and its property checks in verify_repo cover that half, and measurably do:
reversing a link's parent order is caught there and NOT here, because it moves both sides
of this comparison together. Neither file is sufficient alone, and saying so is cheaper
than someone later deleting one of them as redundant.

AND IT PROVES THE JOINS. For every parent foreign key it re-derives the parent's own key
from the same business values and asserts the two digests are equal -- in Spark, on data.
That is the property a document can only assert and a count can never see: an orphan FK
loads perfectly and returns nothing on join.

THE ONE SEAM. _stage_full calls spark.readStream.table(bronze_table), and no local Spark
can resolve a three-part Unity Catalog name. A shim supplies a batch DataFrame for that
one call; everything after it -- withColumn, F.expr, dropDuplicates -- is the same API on
batch and streaming, so the code under test is the shipped code, unmodified. The table
READ is what is faked. The derivation is not.

    JAVA_HOME=~/.local/share/jdk/current .venv/bin/python tests/test_spark_derivation.py

Needs pyspark and a JVM, so it is NOT part of the offline suite and CI must run it as its
own step.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# A local Spark refuses to start without a resolvable local IP on some hosts, and the
# failure is a 30-second hang followed by an opaque BindException. Set it before the JVM
# is touched rather than debugging it once per machine.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# THE WORKER MUST BE THIS INTERPRETER. Spark launches its Python workers with whatever
# `python3` is on PATH, which on this machine is 3.14 while the venv driver is 3.11 --
# Spark refuses to run across minor versions and every task dies with
# PYTHON_VERSION_MISMATCH, reported as an opaque Py4JJavaError one stack removed from the
# cause. Pinned here rather than left to the caller's exports: a test that only passes
# when you already knew to set two environment variables is a test that fails in CI.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Likewise the JVM. Nothing on this host provides one on PATH, so the userspace JDK is
# found here if the caller has not named one -- and if neither exists, the failure says so
# rather than surfacing as a JavaGateway timeout.
if not os.environ.get("JAVA_HOME"):
    _jdk = Path.home() / ".local" / "share" / "jdk" / "current"
    if (_jdk / "bin" / "java").exists():
        os.environ["JAVA_HOME"] = str(_jdk)
    else:
        raise SystemExit(
            "No JAVA_HOME and no JDK at ~/.local/share/jdk/current. This suite needs a "
            "JVM; see the module docstring."
        )

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

from accelerator import factory, hashing, naming, spec  # noqa: E402
from accelerator.staging_columns import extract_identifiers, source_columns  # noqa: E402

CHECKS = 0
FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}" + (f" -- {detail}" if detail else ""))
        FAILURES.append(name)


# A distinct, deterministic value per column name, so a mis-wired component shows up as a
# different digest rather than colliding with its neighbour. NOT random: a flaky digest
# comparison would be worse than none.
#
# CASEFOLDED, because Spark resolves column names case-insensitively and this suite must
# agree with it: a column harvested as 'Rate' by one binding's derived_columns walk and as
# 'rate' by another's (staging_columns.source_columns keeps whichever spelling it saw
# FIRST -- payload before row_filter before derived_columns -- and that order differs
# between nhl_invoice_line, whose payload omits rate, and sat_invoice_line_details, whose
# payload includes it) is ONE physical column to Spark and must be ONE value here too.
# MEASURED: without the fold, invoice_line_details/FIELDGLASS_US's invoice_line_hk
# (line_reference's SHA2 hashes CAST(Rate AS STRING) among ten columns) hashed
# 'v_Rate' on one side and 'v_rate' on the other -- two different digests for what
# should be the identical shared business value, failing the FK-join test the two
# entities' invoice_line_hk share by construction. A case-only spelling difference must
# never be a source of a different synthetic value.
def value_for(column: str) -> str:
    return f"v_{column.casefold()}"


def _bare_raw_column(expr: str) -> str | None:
    """`expr` if it is NOTHING BUT a single raw column identifier, else None.

    conform: derived_columns maps a CONFORMED name to a RAW source expression --
    ('buyer_tenant', 'Buyer_Code'), ('parent_legal_entity_code', 'hs_parent_company_id') --
    and most of those expressions are exactly one bare column reference. That is the
    common case this helper isolates from the complex ones ("NULLIF(TRIM(Rate), '')", the
    large digest expression for 'line_reference'): a bare identifier is the RAW name and
    nothing else, so extract_identifiers finds exactly one identifier AND it equals the
    whole (stripped) expression -- a complex expression may also reduce to one surviving
    identifier after keywords/functions are filtered (NULLIF(TRIM(Rate), '') -> {'Rate'}),
    which is exactly why the second condition, not the count alone, is what decides it.
    """
    stripped = (expr or "").strip()
    ids = extract_identifiers(stripped)
    if len(ids) == 1 and stripped == next(iter(ids)):
        return stripped
    return None


# PROBLEM 1: A row_filter can CONSTRAIN a raw column to one specific literal, and a
# synthetic frame that does not honour that constraint is filtered down to zero rows --
# fieldglass_us_invoice's `Buyer_Code = 'AEE1'` and `coalesce(Mike_Tester, '') = ''` do
# exactly that to value_for('Buyer_Code') / value_for('Mike_Tester'). Two shapes are
# parsed, generically, from binding.row_filter itself -- never a hardcoded literal or
# column name, so a future profile's own constraints are picked up the same way:
#
#   IDENT = 'literal'                 -- IDENT must equal literal
#   coalesce(IDENT, '') = ''          -- IDENT must be empty
#
# Nothing else in a row_filter (IS NOT NULL, <>, NOT LIKE, trim(...) <> '') narrows a
# column to one value -- every one of those is already satisfied by value_for(...)'s
# non-empty, non-'test'-containing default -- so only an EQUALITY to a literal is worth
# extracting here.
_LITERAL_EQ = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'([^']*)'")
_COALESCE_EMPTY_EQ = re.compile(
    r"coalesce\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*''\s*\)\s*=\s*''", re.IGNORECASE)


def _row_filter_literals(row_filter: str) -> dict[str, str]:
    """Raw column -> the one value binding.row_filter's equality predicates require it
    to hold, or {} if the filter constrains nothing to a single value. See the two
    regexes above for exactly what is recognised.
    """
    if not row_filter:
        return {}
    literals: dict[str, str] = {}
    for ident, lit in _LITERAL_EQ.findall(row_filter):
        literals[ident] = lit
    for ident in _COALESCE_EMPTY_EQ.findall(row_filter):
        literals[ident] = ""
    return literals


def conform_seed(binding, given: dict[str, str] | None = None
                  ) -> tuple[dict[str, str], dict[str, str]]:
    """The FRAME's raw values, and the CONFORMED values they produce, made to agree.

    Returns (raw_values, conformed_values).

    raw_values seeds every raw column row_filter/derived_columns constrains to one
    literal -- FILTER-REQUIRED LITERALS WIN, because the row must satisfy the filter to
    exist in the frame at all. Everything else keeps value_for(...) (digests()'s own
    default for anything not in this dict).

    conformed_values is the value each BARE-IDENTIFIER derived_columns entry's CONFORMED
    name ends up holding once factory._stage_full renames its raw column -- literal-
    forced when the raw source is filter-constrained, else `given`'s override (foreign-key
    sharing) or the ordinary value_for(conformed) default. This is what expected() must be
    told (via its own `values` parameter) so the pure-Python reference predicts the SAME
    row digests() actually built -- Python is told what the frame contains, never what
    Spark produced from it.
    """
    given = dict(given or {})
    filter_literals = _row_filter_literals(binding.row_filter)
    raw_values: dict[str, str] = {}
    conformed_values: dict[str, str] = {}
    for conformed, raw in (binding.derived_columns or ()):
        bare = _bare_raw_column(raw)
        if bare is None:
            continue
        value = filter_literals.get(bare, given.get(conformed, value_for(conformed)))
        raw_values[bare] = value
        conformed_values[conformed] = value
    for raw, lit in filter_literals.items():
        raw_values.setdefault(raw, lit)
    return raw_values, conformed_values


def unpredictable_component(binding, components: list[str]) -> tuple[str, str] | None:
    """The first (component, raw expression) pair in `components` this suite CANNOT
    predict in pure Python, or None.

    PROBLEM 2. A component is unpredictable when binding.derived_columns produces it
    from an expression that is NOT a bare identifier -- a real SQL computation
    (`line_reference`'s SHA2/CONCAT_WS digest over ten columns is the case this exists
    for) that the pure-Python reference would have to reimplement to predict, which is
    exactly the second-implementation risk this module's own docstring warns about. A
    bare-identifier rename (buyer_tenant <- Buyer_Code) is fine: its value is simply
    whatever the raw column held, which conform_seed() already computes and expected()
    is already told.

    DERIVED FROM THE MODEL, not a hardcoded list of entity/binding/column names: any
    hash-key component that is a non-bare derived_columns entry triggers this, on
    whatever binding declares it, forever -- a hand-typed exemption list is how this
    kind of excusal becomes permanent instead of self-maintaining.
    """
    derived = dict(binding.derived_columns or ())
    for c in components:
        if c.startswith("'"):
            continue
        raw = derived.get(c)
        if raw is not None and _bare_raw_column(raw) is None:
            return c, raw
    return None


class _ReadStreamShim:
    """spark.readStream.table(name) -> a one-row batch frame with the columns declared.

    Only the READ is faked. See the module docstring: every operation _stage_full performs
    after this point is identical on a batch frame, so the derivation under test is the
    shipped code path and not a re-implementation of it.
    """

    def __init__(self, spark, columns: list[str], values: dict[str, str]):
        self._spark, self._columns, self._values = spark, columns, values

    def table(self, name: str):
        schema = T.StructType([T.StructField(c, T.StringType(), True)
                               for c in self._columns])
        return self._spark.createDataFrame(
            [tuple(self._values.get(c, value_for(c)) for c in self._columns)],
            schema=schema)


class _SparkShim:
    def __init__(self, spark, columns, values):
        self._spark = spark
        self.readStream = _ReadStreamShim(spark, columns, values)

    def __getattr__(self, item):
        return getattr(self._spark, item)


def digests(spark, entity, binding, model, values: dict[str, str] | None = None
            ) -> dict[str, str]:
    """The hex digest of every _hk column, as SPARK computed it for one synthetic row.

    `values` overrides specific columns. It is how the foreign-key comparison feeds a
    child and its parent THE SAME business values through DIFFERENT column names -- which
    is the whole situation an authored hub exists for: the ledger account id GP delivers as
    `input_db` and UKG as `company` is one identifier, and a test that gave those two
    columns different values would report a correct model as broken.

    A BARE-IDENTIFIER derived_columns ENTRY IS SEEDED UNDER ITS RAW NAME WITH THE VALUE THE
    CONFORMED NAME WOULD OTHERWISE GET. `parent_legal_entity_code` is both a hash-key
    component and a derived column produced from `hs_parent_company_id` (a bare raw column
    reference, no expression around it). factory._stage_full's rename means the CONFORMED
    column ends up holding whatever value the RAW column was given -- so if the raw column
    were left to default to value_for('hs_parent_company_id'), Spark would hash that,
    while expected() (below) predicts value_for('parent_legal_entity_code') for the same
    row, because it reads the digest straight off spec.hash_key_columns' component list and
    has never heard of the raw name. Seeding the raw column with
    values.get(conformed, value_for(conformed)) -- the exact value expected() will use --
    makes the two agree without touching spec.py, factory.py or expected() itself. This is
    deliberately NOT attempted for a complex expression (NULLIF(TRIM(Rate), ''), the SHA2
    for 'line_reference'): what Spark computes there is a real function of the raw values,
    which this suite has no business re-deriving in Python -- see this module's docstring
    on what it does and does not prove, and the caller's report on 'line_reference' by name.

    THE FRAME IS THE SINGLE SOURCE OF TRUTH FOR VALUES (PROBLEM 1). conform_seed() seeds
    any raw column binding.row_filter constrains to one literal -- e.g.
    fieldglass_us_invoice's `Buyer_Code = 'AEE1'` -- ahead of the ordinary bare-rename
    default, because the row must satisfy the filter to exist in the frame at all. See
    conform_seed()'s own docstring for why expected() must be told the same values.
    """
    cols = source_columns(model, entity, binding)
    given = values or {}
    raw_seed, _conformed = conform_seed(binding, given)
    seeded = dict(given)
    seeded.update(raw_seed)
    shim = _SparkShim(spark, cols, seeded)
    df = factory._stage_full(entity, binding, shim, model)
    hk_cols = [c for c in df.columns if c.endswith("_hk")]
    if not hk_cols:
        return {}
    rows = df.select(*[F.lower(F.hex(F.col(c))).alias(c) for c in hk_cols]).collect()
    if not rows:
        raise AssertionError(
            f"{entity.name}/{binding.name}: the synthetic frame is EMPTY after "
            f"row_filter {binding.row_filter!r} was applied to it. This suite's own "
            f"seeded row failed to satisfy the filter -- check conform_seed() against "
            f"the filter's equality predicates rather than reading this as an "
            f"IndexError, which would say nothing about why the row disappeared."
        )
    row = rows[0]
    return {c: row[c] for c in hk_cols}


def expected(entity, binding, model, column: str,
             values: dict[str, str] | None = None) -> str:
    """The same digest, computed in PYTHON from the model's declared components.

    The component list comes from spec.hash_key_columns -- the authority factory.py itself
    calls -- and the digest from hashing.reference_key, the pure-Python reference gate zero
    validates Spark against. A literal component ('GP_Journal_Entry', a source scope) is
    hashed as its own value; a column component as the value that column was given.
    """
    values = values or {}
    components, scope = spec.hash_key_columns(model, entity, binding)[column]
    vals = [c[1:-1] if c.startswith("'") and c.endswith("'")
            else values.get(c, value_for(c)) for c in components]
    return hashing.reference_key(vals, source_scope=scope)


def shared_values(child_components, parent_components) -> tuple[dict, dict]:
    """Value maps giving the child's and the parent's key columns the SAME values, by
    position, so only ORDER, SCOPE and LITERALS can make the two digests differ.

    Literal positions are skipped on both sides -- a literal is already the same on both by
    construction, and it occupies a position that has no column to name.
    """
    child_cols = [c for c in child_components if not c.startswith("'")]
    parent_cols = [c for c in parent_components if not c.startswith("'")]
    if len(child_cols) != len(parent_cols):
        return {}, {}          # arity differs; the caller reports that rather than guessing
    cv, pv = {}, {}
    for i, (cc, pc) in enumerate(zip(child_cols, parent_cols)):
        shared = f"bk{i}"
        cv[cc] = shared
        pv[pc] = shared
    return cv, pv


def main() -> int:
    spark = (SparkSession.builder
             .appName("dv-spark-derivation")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version} :: rulebook {hashing.RULEBOOK_VERSION}\n")

    model = spec.load_model(ROOT / "metadata" / "entities")

    print("== factory._stage_full runs in real Spark for every binding ==")
    all_digests: dict[tuple[str, str], dict[str, str]] = {}
    for entity in model.entities:
        for binding in entity.sources:
            key = (entity.name, binding.name)
            try:
                all_digests[key] = digests(spark, entity, binding, model)
                err = ""
            except Exception as exc:  # noqa: BLE001
                all_digests[key] = {}
                err = f"{type(exc).__name__}: {exc}"
            check(f"{entity.name}/{binding.name} stages without raising", not err, err)

    print("\n== every digest Spark produced equals the pure-Python reference ==")
    compared = 0
    not_evaluated: list[str] = []
    for (ename, sname), got in sorted(all_digests.items()):
        entity = model.get(ename)
        binding = next(b for b in entity.sources if b.name == sname)
        _raw_seed, conform_values = conform_seed(binding)
        for column, value in sorted(got.items()):
            components, _scope = spec.hash_key_columns(model, entity, binding)[column]
            # PROBLEM 2: a key built from a derived_columns expression that is NOT a
            # bare identifier (line_reference's SHA2/CONCAT_WS digest is the case this
            # exists for) cannot be predicted here without reimplementing that SQL --
            # a second implementation that would rot silently the day
            # source_unions.yml's expression changes. Reported NOT_EVALUATED, counted,
            # and named, rather than silently skipped or compared against a value this
            # suite invented.
            offending = unpredictable_component(binding, components)
            if offending is not None:
                offending_column, offending_expr = offending
                not_evaluated.append(
                    f"{ename}/{sname}/{column}: component {offending_column!r} is "
                    f"produced by derived_columns expression {offending_expr!r}, not a "
                    f"bare column reference -- predicting it in pure Python would mean "
                    f"reimplementing that SQL a second time, silently, in this test. "
                    f"NOT_EVALUATED; every other key on this binding is still compared."
                )
                continue
            want = expected(entity, binding, model, column, conform_values)
            compared += 1
            check(f"{ename}/{sname}/{column}", value == want,
                  f"Spark produced {value}, the reference says {want} -- the loader fed a "
                  f"different component list from the one the model declares")
    print(f"  ({compared} digest(s) compared, {len(not_evaluated)} key(s) NOT_EVALUATED "
          f"so far -- the full list, including any from the join check below, prints "
          f"at the end)")

    print("\n== every parent foreign key EQUALS the parent's own key, in Spark, on data ==")
    # THE JOIN, ASSERTED. A foreign key that cannot equal its parent's key loads perfectly
    # and returns nothing on join -- no count, no reconciliation and no append-only check
    # can see it. This is the only place that property is exercised on real digests.
    #
    # BOTH SIDES GET THE SAME VALUES, BY POSITION. An authored hub is the same identifier
    # whatever column carries it: GP delivers the ledger account as `input_db`, UKG as
    # `company`. Feeding those two columns different synthetic values would report a
    # perfectly correct model as fourteen broken joins -- measured, it did. With the values
    # shared positionally, only what SHOULD change a key can: component order, the source
    # scope, and the literals.
    joins = 0
    skipped_ids: set[str] = set()
    for (ename, sname), got in sorted(all_digests.items()):
        if not got:
            continue
        entity = model.get(ename)
        binding = next(b for b in entity.sources if b.name == sname)
        child_keys = spec.hash_key_columns(model, entity, binding)
        # LEG -> HUB FROM THE MODEL. `column[:-3]` reads a roled foreign key
        # (`child_legal_entity_hk`) as a hub called `child_legal_entity`, which raises
        # rather than mis-resolving -- so this matcher aborted the whole suite on the
        # first hierarchical link. Seventh site of the same name-stripping assumption.
        leg_hub = {naming.hk(h, r): h for h, r in spec.parent_legs(entity)}
        for column in sorted(got):
            if column == entity.hk_column:
                continue
            parent = model.get(leg_hub.get(column, column[:-3]))
            child_components, fk_scope = child_keys[column]
            # THE PARENT BINDING IS THE ONE WITH THE SAME SOURCE NAME. That is not a
            # heuristic, it is the rule spec.parent_key_components follows: a child's
            # parent key is derived from "the parent hub's OWN binding for the same
            # source", because that is where the parent's key_literals live.
            #
            # An earlier version of this matcher picked the first parent binding under the
            # same SCOPE, which for an AUTHORED hub is every binding (scope None) -- so it
            # compared journal_line/UKG_US against accounting_journal/GP_US and reported
            # four correct joins as broken. The literals differ per binding
            # ('UKG_Batch_ID' vs 'GP_Journal_Entry'), so that comparison was never
            # meaningful. Match the authority's rule or do not match at all.
            #
            # No binding of that name means the parent is not fed from this source: the
            # acknowledged-orphan case in metadata/key_scope_exceptions.json, gated in
            # verify_repo, and not something a digest comparison can settle.
            match = next((pb for pb in parent.sources if pb.name == binding.name), None)
            if match is not None:
                parent_scope = spec.hash_key_columns(
                    model, parent, match)[parent.hk_column][1]
                if parent_scope != fk_scope:
                    match = None
            if match is None:
                # SECOND RULE, AND IT IS THE ONE key_scope EXISTS FOR. A computed satellite
                # binding is named BUSINESS_VAULT and the parent will never carry a binding
                # of that name -- by design, since nothing delivers a feed "from the
                # business vault". What its key_scope says is "hash as the UKG_US feed
                # does", so UKG_US is the binding to compare against. Matching on name
                # alone skipped exactly the fix this suite most needs to prove.
                match = next(
                    (pb for pb in parent.sources
                     if spec.hash_key_columns(model, parent, pb)[parent.hk_column][1]
                     == fk_scope),
                    None)
            if match is None:
                skipped_ids.add(f"{ename}/{sname}/{column}")
                continue
            parent_components = spec.hash_key_columns(
                model, parent, match)[parent.hk_column][0]
            # PROBLEM 2, SAME REASON, A SECOND SITE. shared_values() gives a non-bare
            # derived component (line_reference) a FABRICATED placeholder string on both
            # sides -- and that placeholder never reaches the real SHA2 computation on
            # whichever side actually performs it (it is overwritten by the rename), while
            # the other side may read the column as a plain passthrough and use the
            # placeholder verbatim. The two are then guaranteed to disagree for a reason
            # that has nothing to do with whether the model's FK is correct. Checked on
            # BOTH the child's own components and the matched parent binding's, since
            # either side computing the column for real makes the placeholder invalid.
            offending = (unpredictable_component(binding, child_components)
                         or unpredictable_component(match, parent_components))
            if offending is not None:
                offending_column, offending_expr = offending
                not_evaluated.append(
                    f"{ename}/{sname}/{column} == {parent.name}/{match.name}'s own key: "
                    f"component {offending_column!r} is produced by derived_columns "
                    f"expression {offending_expr!r}, not a bare column reference -- a "
                    f"synthetic shared value cannot stand in for a real SQL computation "
                    f"on either side of this join. NOT_EVALUATED; every other foreign "
                    f"key on this binding is still compared."
                )
                continue
            cv, pv = shared_values(child_components, parent_components)
            if not cv and not pv:
                check(f"{ename}/{sname}/{column} and {parent.name}/{match.name} agree on "
                      f"how many key components there are",
                      False,
                      f"child hashes {len(child_components)} component(s), parent "
                      f"{len(parent_components)} -- a different component COUNT is a key "
                      f"that can never join, and spec.parent_key_components' arity check "
                      f"is what normally catches it")
                continue
            joins += 1
            child_digest = digests(spark, entity, binding, model, cv)[column]
            parent_digest = digests(spark, parent, match, model, pv)[parent.hk_column]
            check(f"{ename}/{sname}/{column} == {parent.name}/{match.name}'s own key",
                  child_digest == parent_digest,
                  f"child hashed {child_digest}, parent hashes {parent_digest} on the same "
                  f"business values -- every row would point at a parent row that does not "
                  f"exist, and both tables would load")
    print(f"  ({joins} foreign key(s) compared on shared business values)")

    # SKIPPED IS ASSERTED, NOT REPORTED. A comparison this suite declines to make is a
    # place a real defect can sit for ever, and a count printed at the end is not a gate --
    # that is the DEF-48 shape exactly (an empty manifest makes an unfailable check). The
    # only legitimate reason to skip is that the parent is not fed under the foreign key's
    # scope, which is precisely what metadata/key_scope_exceptions.json enumerates and
    # verify_repo gates. So the two sets must be equal: a new skip is either a new orphan
    # that must be acknowledged there, or a matcher that has stopped finding a parent it
    # used to compare against.
    _ack_path = ROOT / "metadata" / "key_scope_exceptions.json"
    _ack = {k for group in json.loads(_ack_path.read_text(encoding="utf-8")).values()
            if isinstance(group, dict) for k in group if not k.startswith("_")}
    check("every comparison this suite skipped is an acknowledged orphan, and every "
          "acknowledged orphan was skipped",
          skipped_ids == _ack,
          f"skipped-but-not-acknowledged {sorted(skipped_ids - _ack)}; "
          f"acknowledged-but-compared {sorted(_ack - skipped_ids)} -- the first is an "
          f"unexamined foreign key, the second means the register is stale")

    # THE FULL LIST, ONCE, HERE -- not where each half was found. PROBLEM 2 excuses a key
    # in TWO different loops (the plain digest-vs-reference comparison above, and the
    # FK-join comparison just run), and printing only the first loop's findings while the
    # second loop's landed silently in the count would be exactly the silent skip this
    # requirement forbids -- a NOT_EVALUATED number nobody could account for by reading
    # the log. Every entry named here, whichever loop excused it.
    if not_evaluated:
        print(f"\nNOT EVALUATED -- {len(not_evaluated)} key(s), a derived-expression "
              f"component this suite cannot predict without reimplementing the SQL:")
        for n in not_evaluated:
            print(f"  ~ {n}")

    print("\n" + "=" * 66)
    print(f"{CHECKS} checks run in Spark {spark.version}, "
          f"{len(not_evaluated)} key(s) NOT_EVALUATED (listed above)")
    spark.stop()
    # SAME SHAPE AS THE HARD GATES' OWN LAST LINE (see checks/mask_survival_check.py,
    # checks/publish_stable_views.py): machine-readable, printed last, and NOT_EVALUATED
    # is a count on this line even on a passing run -- a silent skip is forbidden.
    status = "FAILED" if FAILURES else "PASSED"
    print(f"GATE SUMMARY :: spark_derivation :: status={status} asserted={CHECKS} "
          f"not_evaluated={len(not_evaluated)}")
    if FAILURES:
        print(f"\nFAILED -- {len(FAILURES)}:")
        for f in FAILURES:
            print(f"  * {f}")
        return 1
    print("ALL SPARK DERIVATION CHECKS PASSED")
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc:
        sys.exit(_rc)
