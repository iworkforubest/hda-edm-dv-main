# The source contract: what Silver requires of Bronze, generated from the model

Decided 28 Aug 2026. A consumer-side contract. Where `data_contracts/` describes what the
vault *produces*, this describes what the vault *depends on* — so Bronze can only break us
knowingly.

## 1. Why this is an export and not a new authored document

`2026-08-27-data-contract-export-design.md` excluded Bronze, and gave two reasons:

> the ingest boundary ... has no model in this repo to generate from. A contract for that
> boundary would be a genuinely new artefact rather than an export, and belongs to whoever
> owns Bronze.

**The first reason no longer holds, once the question is turned around.** We do not have
Bronze's model and are not proposing to build one. We have something else, complete and
already gated: **the model of our dependency on Bronze.** Every `SourceBinding` declares
`bronze_table` plus the columns Silver reads from it — `key_columns`, `parent_keys`, `payload`,
`transaction_key` (on the entity), `applied_dts_column`, `cdc_op_column`, `manifest_column`,
`dedup_by`, `dedup_order` — and `cast` states the type Silver requires where it casts.

That is a contract about the boundary, generated from the authority that already governs it.
The second reason still stands and is respected: this is **not** a specification of what Bronze
must contain. It is a statement of what we read. Bronze remains free to hold anything else.

## 2. One authority, and it stays where it is

`metadata/entities/*.yml` remains the sole source of truth. `source_contracts/<target>.yaml` is
generated from it, committed, and asserted to regenerate as a no-op — the discipline
`verify_repo.py:1970` already applies to `data_contracts/` and `metadata/key_composition.json`,
whose comment reads "the diff IS the review".

The emitter **imports** `emit_data_contract.targets_and_variables()` rather than restating target
resolution, and `spec.active_table_bindings()` rather than restating what "active" means. Both
are existing single authorities; a fourth statement of either is the duplicate-definition trap
this repo has been bitten by twice (`BUSINESS_KINDS`, the system-column set).

## 3. Only lakes that have declared their Bronze get a contract

**This section was wrong when first written, and the correction is the whole of it.** The original
argued for "active bindings only" and justified it thus: `nhl_timesheet_line` and
`nhl_payroll_detail` bind placeholder bronze tables, "neither appears in any target's
`active_sources`", so they are excluded.

That inverts the semantic. `spec.py:954` states that `active_sources=None` means **every** binding
is active — correct for the pipeline, which then creates each declared table empty. Only **two** of
the nine targets declare an `active_sources` list. So under the original design the placeholders
were active in seven targets and published there, and the artefact shipped that way before the
whole-branch review caught it.

Measured 28 Aug, and the split is total:

| | declares `active_sources` | tables published | in own `bronze_catalog` |
|---|---|---|---|
| `dev`, `usnc_tds` | yes | 5 | **5** |
| the other seven | no | 19 | **0** |

Every table in those seven files was foreign or placeholder — fourteen that `DEPLOY.md:236` says
"will not exist", plus five `01_usnc_bronze_dev` tables offered to WEU, UKS and AUE readers as
their own requirement.

So the rule is not a filter on tables. **A contract is written only for a target that has declared
what it loads** (`emit_source_contract.configured_targets()`). A lake that has not is a
declared-but-unbuilt lake, and an EMPTY contract for it would be worse than no file: it reads as
"Silver requires nothing of you" when the truth is that its Bronze does not exist yet. A target
gets a contract the day it declares one.

`databricks.yml:315` already understood this for the pipeline — it qualifies `UKG_US` per entity
precisely because that source "is NOT uniformly real", naming a real table on four finance hubs and
a placeholder on four others. The declaration was always the mechanism; this spec's error was
assuming its absence meant exclusion rather than inclusion.

## 4. What each entry states

Keyed by fully-qualified bronze table. Per table: the bindings and entities that read it, and
the required columns **grouped by the role Silver uses them in** rather than as a flat list.

Role is the useful unit because the consequences differ: dropping a payload column degrades one
satellite, dropping a key column breaks joins across the whole lake, and dropping `dedup_by`
silently changes which duplicate survives.

* `business_keys`, `parent_keys`, `transaction_key` — identity. Breaking these breaks joins.
* `payload` — descriptive attributes. Breaking these degrades one target.
* `applied_dts`, `cdc_op`, `manifest` — load semantics, each present only where declared.
* `dedup_by` — stated with meaning: Silver collapses re-deliveries on these columns, so
  together they must identify one business row.

**`dedup_order` is deliberately EXCLUDED from required columns**, and the reason is worth
recording because an earlier draft of this spec included it. Two measurements, 28 Aug:
`factory.py:367` states `dedup_order` is "**DECLARED but not applied**" — a partitioned ranking
window is not a legal streaming operation, so the runtime never reads those columns — and it is
that file's only mention of them. And `input_file_name`, which appears in most `dedup_order`
declarations, occurs NOWHERE in `src/accelerator/` as a column: it is Spark's file-metadata
function, not something Bronze holds. Listing either would publish a dependency we do not have,
and one of them names a column that does not exist. It is mentioned in the contract as declared
future intent, explicitly not a requirement, so the day a batch latest-wins path lands the
dependency is already written down.
* `required_casts` — from `cast`, as a REQUIREMENT: "Silver casts this to `DOUBLE`, so it must
  be castable to `DOUBLE`."

## 5. The two things it cannot say, printed in the artefact

**Bronze's own column types.** Not modelled here. Every type in the contract is a requirement
Silver imposes, never an observation about what Bronze holds. Stated so a reader cannot mistake
the document for a description of Bronze.

**The columns governed expectations touch.** `factory.py:624`'s `_expectations()` reads rule SQL
from `control.ref_dq_expectation` **at pipeline runtime**, and `factory.py:1035-1037` records that
that SQL "may name any SOURCE column". So the column list is complete for what the model
declares plus the two compiled-in key-safety rules, and incomplete for governed rules. A Bronze
change to a column only an expectation references would break a load this contract never
mentioned.

Enriching the artefact by reading `ref_dq_expectation` from a live lake is **rejected**: it
would make a committed, offline-reproducible artefact depend on workspace state, destroying the
regenerate-and-diff property that is the only reason to trust it. The gap belongs to a live
check (§7), not to the emitter.

Both limits are asserted PRESENT in the generated text (§6). An honest caveat that can be
silently deleted is worth little: five defects on the preceding branch were a claim outliving
the thing that justified it.

## 6. What asserts this

| gate | assertion |
|---|---|
| **new** | regenerating every `source_contracts/<target>.yaml` is a no-op |
| **new** | every target in scope has a committed contract file |
| **new** | every active binding's `bronze_table` appears exactly once in its target's contract |
| **new** | every column named in a contract exists in the model for that binding — no invented names |
| **new** | no column whose ONLY declared role is `dedup_order` appears in any contract's required roles — the absolute form false-positives, because `dex_row_ts` is legitimately required as `applied_dts` |
| **new** | every table a contract names is in THAT TARGET'S OWN `bronze_catalog` — the check that would have caught §3's error |
| **new** | a contract exists for every target declaring `active_sources`, and for no other |
| **new** | no contract names a bronze table that no active binding in that target reads |
| **new** | both §5 limits appear in every generated file |
| `verify_repo` | the emitter carries the same file-level invariants as the other tools |

Row four is the one to get right in both directions. A contract that omits a column Silver
reads tells Bronze a dependency does not exist; a contract that invents one sends them
defending a column nothing uses. Both must be proven able to fail.

## 7. What this does not do

**It does not check Bronze.** Nothing here reads a live lake. Whether Bronze *satisfies* the
contract is a different assertion — "every required column still exists, with a castable type",
against `information_schema` — and belongs with `control_conformance_check.py` in the
conformance family. That check is where the value actually lands, because it would catch a
breaking Bronze change before a pipeline run does. It is deliberately a second spec: this one
must exist and be reviewable first.

**It is not an agreement.** It is our published dependency, not something Bronze has
countersigned. The artefact says so. Presenting an unnegotiated document as a contract another
team is bound by would be a claim we cannot evidence — the same reason the SLA block was
withdrawn from `data_contracts/`.

**It does not cover the control schema.** `control_contracts/bronze.yaml` already specifies what
Bronze's control schema must contain, and that one IS a requirement rather than an export. The
two are deliberately separate artefacts with different standing, which is why this one lives in
`source_contracts/` rather than beside it.
