# The data contract: generated from the model, not authored beside it

Decided 27 Aug 2026. Third of three specs. The first gave the control schema a home, the
second used it; this one publishes what the model already knows in an interchange format.

## 1. What a contract adds here, and what it would only duplicate

The request came with an exemplar: a datacontract.com-shaped YAML covering hub, link,
satellite, non-historized link, PIT and bridge, with SLAs, per-column security
classification, quality expectations with severities, declared primary and foreign keys,
and Databricks clustering keys.

**Most of that already exists in this repo**, enforced rather than described:

| the exemplar declares | this repo already has |
|---|---|
| quality expectations | `control.ref_dq_expectation` plus two compiled-in key-safety rules |
| referential integrity | `parent_keys`, and `spec.py`'s arity check that refuses a key which could never join |
| change-detection metadata | `naming.SYSTEM_COLUMNS` — `load_dts`, `rec_src`, `hashdiff`, and four more |
| clustering keys | `factory._cluster_by` / `_cluster_refusal`, decided 26 Aug on `load_dts` |
| column masking | `entity.masks`, `governance/apply_masks.sql`, and three assertions in `mask_survival_check` |
| **security classification** | **`entity.sensitivity`** — `internal \| personal \| financial \| restricted`, validated at `spec.py:583` |

That last row is the one that matters, because an earlier draft of this design proposed
adding a per-column `security_classification` field. **That would have been a second
authority for a concept the model already carries**, which is the duplicate-definition trap
this repo has been bitten by twice — `BUSINESS_KINDS` and the system-column set — and which
`metadata/key_composition.json` exists to catch. It is withdrawn.

The real gap is **granularity, not absence**: `sensitivity` is per ENTITY, the exemplar's
classification is per COLUMN. §3 closes that by derivation rather than by a new field.

## 2. One authority, and it stays where it is

**`metadata/entities/*.yml` remains the sole source of truth. The contract is generated FROM
it and is never hand-edited.**

The alternative — authoring the contract and generating the model from it — was rejected on
what the exemplar itself contains. It specifies SHA-1 keys as `STRING` of length 40, and
clustering on hash keys. Both are wrong here, and adopting either by making the contract
authoritative would be expensive:

* `hashing.ALGORITHM_RATIFIED = "sha2_256"` and `BINARY_OUTPUT_RATIFIED = True`, both guarded
  at import. Changing them is a `RULEBOOK_VERSION` bump that **re-keys 13.2M rows**.
* **Delta refuses to cluster on a BINARY hash key.** DEF-23 measured
  `[DELTA_CLUSTERING_COLUMNS_DATATYPE_NOT_SUPPORTED] ... ledger_account_hk : BINARY`, which is
  why the 26 Aug clustering decision landed on `load_dts`. `factory._cluster_refusal` enforces
  it. A contract asserting hash-key clustering would describe something this runtime will not
  do.

Generation makes both conflicts disappear without a decision: the export emits `sha2_256`,
`BINARY(32)` and `load_dts` because that is what the model says.

**The exemplar is a template, not a proposal about this estate.** Its entities are `customer`,
`account`, `card` and `merchant`; ours are `accounting_journal`, `job_request`,
`ledger_account`, `organisation`, `pay_period`, `worker` and the journal NHLs. Its SHA-1 and
`load_ts` are the template's defaults, and are read as such rather than as requests.

## 3. Per-column classification, derived

No model change. Measured across the model: 13 entities are `internal`, 7 `financial`, 1
`restricted`, and 8 entities declare per-column masks.

```
a column carrying a mask   ->  the entity's `sensitivity`   (personal | financial | restricted)
a column carrying no mask  ->  internal
```

So `nhl_payroll_detail` (`sensitivity: restricted`, masks on `gross_amount`, `amount`,
`taxable_amount`, `ytd_amount`, `rate`) exports those five columns as `restricted` and the
rest as `internal`. `nhl_general_journal_line` (`financial`, masks on `debitamt`, `crdtamnt`)
exports those two as `financial`.

**Why derivation rather than declaration.** A column's classification then follows from two
facts that are already gated — the entity's declared sensitivity, and whether a mask is bound
to that column — instead of being a third statement free to disagree with either. It is also
already coherent: `verify_repo.py:610-615` refuses a `personal`/`financial`/`restricted`
entity that declares no masks, and `mask_survival_check`'s first assertion refuses a marked
column that carries none. The derivation reads those two gates rather than adding a third
thing to keep in step.

**The one thing the derivation cannot express**, and this is stated rather than hidden: a
column that is sensitive but deliberately unmasked would export as `internal`. Today that
cannot arise — the two gates above make an unmasked sensitive column a build failure — so the
derivation is total. If a future decision permits an unmasked sensitive column, this
derivation becomes wrong and the model would then need the per-column field after all.

## 4. Severity, which is genuinely missing

`control.ref_dq_expectation` is `(dataset, rule_name, rule_sql, is_current)`. There is no
severity column and no WARN tier: the pipeline's only mechanism is `expect_all_or_drop`, which
drops the row and writes it to the quarantine twin, plus the two compiled-in key-safety rules.
So a consumer cannot tell "this rule drops your row" from "this rule is advisory".

**Add `severity` to `ref_dq_expectation`**, one of `fail | drop | warn`:

* `fail` — the load stops. Reserved for the key-safety class, where continuing would write
  unjoinable keys.
* `drop` — the row is dropped and quarantined. This is what every rule does today, so it is
  the default and the migration is a no-op.
* `warn` — the row loads and the violation is recorded. **This tier does not exist yet**, and
  the spec does not invent its plumbing: adding the column makes the contract able to say
  `warn`, and a later change makes the pipeline able to honour it. Until then a `warn` row is
  a declaration the generator refuses, so the contract cannot promise a behaviour the pipeline
  lacks.

The table holds **0 rows**, so this is a column addition with nothing to migrate.

## 5. The export

`tools/emit_data_contract.py`, producing one datacontract.com-shaped YAML per bundle target,
committed to `data_contracts/`, with a gate asserting that regeneration is a no-op.

That last part is the whole discipline, and it has a precedent in this repo:
`verify_repo.py:1853-1858` regenerates `metadata/key_composition.json` and fails if the result
differs, with the comment "the diff IS the review". The contract gets the same treatment — a
generated artefact that is committed, so its diff is reviewable, and asserted, so it cannot
drift from the model it claims to describe.

### What the export emits

Per entity: physical name resolved from the bundle variables of that target; columns with the
types the model actually produces (`BINARY(32)` hash keys, not `STRING`); the system columns
under their real names; per-column classification per §3; the declared masks; `parent_keys` as
`foreign_key_target`; the uniqueness grain `append_only_check` asserts as `primary_key`; and
`clustering_keys` from `factory._cluster_by`, which is `load_dts` where the refusal does not
fire and empty where it does.

### What the export deliberately omits

* **The SLA block.** We measure no freshness, no latency and no uptime. `control.aud_load_run`
  records a run opening and closing but nothing computes an SLA from it. Publishing "99.9%
  pipeline availability" in a signed artefact nobody measures is worse than publishing
  nothing: it is a claim a consumer can hold us to and we cannot evidence.
* **`delta_max_file_size`.** A different lever from liquid clustering, with no measurement
  behind it here.
* **PIT and bridge entities.** `naming.PREFIX` knows both kinds and the model declares
  neither. Emitting them would describe tables that do not exist — and `OPEN_ITEMS.md` records
  that the materialization exposure "begins the day the first PIT or Gold MV is built", so
  they are not a free addition.

## 6. What asserts this

| gate | assertion |
|---|---|
| **new** | regenerating every contract is a no-op — the committed artefact matches the model |
| **new** | every column the contract classifies above `internal` carries a mask in the model, and vice versa — the derivation is total in both directions |
| **new** | no emitted `clustering_keys` entry is a hash key, and none is a column `_cluster_refusal` rejects |
| **new** | no emitted hash-key column is typed `STRING`; every one is `BINARY(32)` per `hashing.key_type_sql()` |
| **new** | the generator refuses a `warn` severity while the pipeline cannot honour it |
| `verify_repo` | the emitter is covered by the same file-level invariants as the other tools |

The second row is the one to get right. If the derivation is not total in both directions,
the contract either classifies an unmasked column as sensitive — alarming a consumer over
nothing — or classifies a masked column as `internal`, which understates the protection on
real financial data. Both directions must be asserted, and both must be proven able to fail.

## 7. What this does not do

It does not govern anything. A contract generated from the model is a **description**, and
every control it describes is enforced by a gate that already exists — masks by
`mask_survival_check`, grain by `append_only_check`, key composition by the digest, grants by
`schema_grant_check`. Nothing downstream should read this file and conclude a rule is
enforced *because the contract says so*.

It does not cover Bronze. `subproject3-source-rebinding-design.md:153` records that "Bronze
mirrors what each source delivers", and the ingest boundary — where BRZ-1, BRZ-8 and BRZ-12
live — has no model in this repo to generate from. A contract for that boundary would be a
genuinely new artefact rather than an export, and belongs to whoever owns Bronze.

It does not publish. Where the generated YAML goes — a catalogue, a registry, a repo the
consumers read — is a separate decision, and one that should be made after the first
generated contract exists and can be looked at.
