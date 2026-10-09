# Source conformance: does Bronze still satisfy what Silver requires?

Decided 28 Aug 2026. The second half of the source contract, and the half where the value lands.
`source_contracts/<target>.yaml` states what Silver requires of each Bronze table it reads. This
checks whether Bronze still provides it — before a pipeline run finds out.

## 1. Why this is a separate spec, and why it comes second

`2026-08-28-source-contract-design.md` §7 named this deliberately out of its own scope:

> It does not check Bronze. Nothing here reads a live lake. Whether Bronze *satisfies* the
> contract is a different assertion ... **That check is where the value actually lands**, because
> it would catch a breaking Bronze change before a pipeline run does.

The contract had to exist and be reviewable first. It now does, and it is correct: after the
placeholder correction it covers the two targets that have declared their Bronze, naming five
tables each, all five in their own bronze catalog.

## 2. The honest split: existence for all, type for a minority

Measured on `usnc_tds`, table `great_plains_raw.gl20000`: the contract names **19 distinct
required columns** and states a type for **four** of them — `debitamt` and `crdtamnt`
(`DECIMAL(18,2)`), `ordbtamt` and `orcrdamt` (`DOUBLE`). That is by design, not an omission:
§5 of the contract spec records that Bronze's own column types are not modelled in this
repository, so a type appears only where Silver declares a `cast`.

So "every required column exists with a castable type" decomposes unevenly, and the spec says so
rather than implying uniform coverage:

* **existence** — asserted for all 19;
* **castability** — asserted for the 4 that carry a declared cast;
* **the actual Bronze type of the other 15** — REPORTED, never asserted. A report that reads like
  an assertion is the defect the preceding branch spent its entire review budget on, so these are
  labelled in the output and proven not to affect the exit status.

## 3. Three findings, reported distinctly

| finding | meaning |
|---|---|
| **ABSENT** | the table named in the contract does not exist in the lake |
| **MISSING COLUMN** | the table exists; a required column is not in it |
| **LOSSY CAST** | a cast column exists, and `try_cast` to the required type would destroy rows |

Distinctness is the point. `control_conformance_check.py` already learned this: an ABSENT table
and a wrong-columns table are different problems with different owners, and collapsing them sends
the wrong person to look.

## 4. `try_cast`, because a measurement beats an opinion

For each column carrying a declared cast, count:

```sql
SELECT COUNT(*) FROM <bronze table>
WHERE <col> IS NOT NULL AND try_cast(<col> AS <required type>) IS NULL
```

That is literally "rows this cast would silently destroy". Non-zero is a real, measured problem;
zero means the cast is safe against today's data.

**One count per (column, required type) pair, not per column.** `required_casts` is a column to a
sorted LIST of types, because Ruling T2-B established that two bindings casting one column to two
types is a CONJUNCTION of requirements — Bronze must supply a column castable to both. Measured
28 Aug: every list holds exactly one type today, so this is dormant. It is stated because an
implementer reading the SQL above would write the singular form, and a second required type would
then go silently unmeasured.

**Why not a type-compatibility matrix.** `debitamt` is cast to `DECIMAL(18,2)` precisely because
it is not already that type, so an exact-match rule would redden on a correct build — and a gate
that reddens on a correct build is worse than a blind one, because it teaches people to distrust
the gate. A curated matrix is an opinion about type theory; a null count is a fact about the data.

**Why reading data is legitimate here.** Bronze masks PII **physically**, rewriting values in
place — `2026-08-27-cross-layer-control-standard-design.md` §1 records it, citing `databricks.yml:162-171`. The value Silver reads
IS the masked value, so `try_cast` over it tests exactly what the pipeline will do.

**What it cannot prove.** Castability today, not tomorrow. A future delivery can still break it,
which is why this is a gate to re-run rather than an audit to file.

## 5. Structure

`checks/source_conformance_check.py`, in the shape `checks/control_conformance_check.py` uses:
pure decision functions with Spark confined to `main()`, so fabricated `information_schema` rows
and fabricated cast counts exercise the same decisions the live path makes.

* `missing_columns(required, deployed) -> list[str]` — pure.
* `lossy_casts(rows) -> list[str]` — pure, over already-collected rows shaped
  `{"table", "column", "type", "lossy"}` (a dict per probe, not a `(table, column, type, count)`
  tuple as an earlier draft of this section had it) — one dict per `cast_probes()` pair, `lossy`
  holding the measured count. Routes each finding through `finding("LOSSY CAST", ...)`, the same
  as the other two kinds.
* `required_columns(requires) -> set` — pure. Every list-valued role's columns, plus
  `required_casts`' keys (a dict, not a list, so it needs its own term).
* `probe_sql(table, col, required_type) -> str` — pure. The `try_cast` null-guarded COUNT query,
  extracted so the null guard and the `try_cast(...) IS NULL` predicate are themselves asserted.
* `report_lines(tables, observed) -> list[str]` — pure. The REPORTED-not-asserted lines for
  columns the contract does not type, extracted so the report's presence is asserted directly
  rather than carried by a comment substring.
* `contract_catalog(tables) -> str` — pure. The single Bronze catalog every table in the contract
  block belongs to, read off the contract's own fully-qualified table names rather than accepted
  as an operator-supplied `--catalog` flag; raises if the contract ever named more than one.
* `catalog_missing(exc) -> bool` — **reused from `control_conformance_check`**, not restated. That
  function exists because `main()` there once CRASHED on a nonexistent catalog instead of
  reporting the not-instrumented state its own docstring promised. A second copy would be the
  duplicate-authority trap this repo has been bitten by three times.
* `main() -> int` — DEF-14 compliant: returns an int, `sys.exit` only on a truthy rc.

## 6. Scope, and the two dependencies this spec does not assume

It runs for the targets that have a committed contract — `dev` and `usnc_tds` — and for no
others, because a target without a contract has not declared its Bronze.

**`try_cast` must exist in the runtime.** Databricks provides it. The check VERIFIES this at run
time and reports the alternative rather than assuming it, because a silently-unsupported function
would turn every cast measurement into a false zero.

**It needs SELECT on the Bronze `_raw` tables.** The pipeline's service principal plainly has it;
whether a human running this ad hoc does is unknown. When the cast measurement cannot run, the
check reports **which mode it ran in** and counts the cast columns as `not_evaluated`. A green
result must never conceal that the stronger half was skipped — the failure mode this repo calls a
gate that goes quiet rather than red.

## 7. Running it is a separate, consented act

Nothing in building this touches a workspace. There are eight near-identical workspaces, only
`hfig-usnc-tds` is non-production, and the profile is the human's choice — never inferred, never
defaulted.

## 8. What asserts this

| gate | assertion |
|---|---|
| **new** | `missing_columns` reports a column the contract requires and the lake lacks |
| **new** | ABSENT, MISSING COLUMN and LOSSY CAST are distinguishable in the output, not one category |
| **new** | `lossy_casts` fires on a non-zero count and stays silent on zero |
| **new** | the reported-not-asserted columns cannot change the exit status |
| **new** | a skipped cast measurement is reported as `not_evaluated`, never as a pass |
| **new** | non-vacuity: an empty contract list fails rather than passing every check trivially |
| **new** | the reported-not-asserted types are PRESENT in the output, not merely harmless |
| **new** | `catalog_missing` is IMPORTED from `control_conformance_check`, not redefined here |
| **new** | the check runs for exactly the targets with a committed contract, and no others |
| `verify_repo` | the check carries the same file-level invariants as the others, including DEF-14 |

**Four of these rows exist because a traceability pass over this spec's own §1–§7 found them
unasserted**, which is the pass that found three blockers on the preceding branch and one in the
plan before it. Two are worth naming individually:

* **The report must be present, not just harmless.** Row four says a reported type cannot change
  the exit status; without the new row above it, a report that silently vanished would satisfy
  that trivially. This is the same shape as the caveat that could be blanked while its key
  survived, found on the preceding branch inside the check written to prevent it.
* **The reuse of `catalog_missing` must be asserted.** §5 argues that a second copy would be the
  duplicate-authority trap this repo has been bitten by three times — and then asserted nothing
  against it. A claim in prose about a risk, with no gate, is precisely the defect class this
  branch keeps producing.

**One property is genuinely untestable offline, and it is narrower than this section used to
claim.** An earlier draft treated "the check verifies `try_cast` exists in the runtime" as
untestable offline. That was wrong: `main()`'s `try_cast('x' AS INT)` probe is reachable through
the same offline harness (`tests/test_accelerator.py`'s `_run_source_conformance`) as everything
else in `main()`, by fabricating a Spark stand-in that raises on that one query — and both
branches are asserted there: the availability probe succeeding (the cast columns get measured),
and it raising (every cast column lands in `not_evaluated`, naming `try_cast is unavailable in
this runtime` as the cause, and no real COUNT probe is attempted). What genuinely cannot be
proven by a fabricated input is narrower: whether Databricks' REAL `try_cast` behaves as
documented against a live warehouse. No offline mutation exercises that, because it is a claim
about the live runtime's actual behaviour, not about this check's handling of either outcome. That
is asserted by the only instrument available — running the check against a workspace, which is a
separate consented act — and this spec now states that narrower limit instead of the broader one
it used to claim.

Row four is the one to get right. If a reported column can redden the gate, the report has become
an assertion nobody agreed to; if it can silence a real finding, the gate is hollow. Both
directions must be proven able to fail.

## 9. What this does not do

**It does not check the control schema.** `control_conformance_check.py` does that, against
`control_standard`. This checks the DATA tables against the source contract. Two checks, two
inputs, deliberately separate.

**It does not fix anything.** A LOSSY CAST finding is a conversation with Bronze or a modelling
decision about the cast, not something this check resolves.

**It does not cover the columns governed expectations touch.** The contract spec §5 records that
this SQL lives in `control.ref_dq_expectation` and is read at pipeline runtime, where it may name
any source column. Those columns are not in the contract, so they are not checked here, and a
Bronze change to one of them can still break a load this gate passes.
