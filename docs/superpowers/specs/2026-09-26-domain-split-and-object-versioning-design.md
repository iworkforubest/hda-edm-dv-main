# Domain split, change-gated loading, and Data Vault object versioning

**Status:** design, approved in conversation 26 September 2026. Not implemented.
**Author:** Adrian Turcu (decisions), drafted with Claude.
**Supersedes nothing.** Extends the domain-split topology introduced 25 September.

---

## 1. The problem, measured

The vault load is one job of 29 tasks over 6 domains and 39 entities. The stated
target is **15-20 domains and 500+ tables**. Three measurements decide this design.

### 1.1 The task count is not the wall

Of the 29 tasks, **7 scale with the number of domains** (`raw_vault_*`,
`business_vault`) and **22 are fixed, whole-estate tasks**. At 20 domains that is
43 tasks in one job -- unpleasant, survivable.

The wall is what those 22 do. `load_hubs` loads every hub; `assert_append_only`
runs `DESCRIBE HISTORY` on every vault table; `assert_mask_survival` walks every
column of every table; `load_satellites` loads every satellite. Each is a single
task doing the whole estate's work on every run, whether or not anything changed.
At 500 tables the fixed 22 dominate, and splitting pipelines alone does not touch
them.

### 1.2 Finance costs 16 minutes whether or not anything changed

| run | `raw_vault_finance` | every other domain | job total |
|---|---|---|---|
| `351906620077470` | 16.1 min | 1.0 - 2.0 min | 27.3 min |
| `229372019259866` | 16.3 min | 1.0 - 2.0 min | 27.7 min |

Two consecutive runs with no new Bronze data between them, reproducible to
0.2 min. Approximately **23 of 27 minutes is pipeline cost independent of the
delta**. Finance is the long pole by an order of magnitude; it holds
`hub_accounting_journal` (2.2M rows), `hub_ledger_account` (1.9M) and
`nhl_general_journal_line` (2.45M).

This is the single largest recoverable cost in the load, and it is recoverable by
**not running the domain**, not by making it faster.

### 1.3 The domain graph exists in the model -- but it is not a load order

13 cross-domain parent references, all pointing down a single hierarchy, **no
cycles**:

```
reference (12 entities)        root; nothing depends on it
    |
party (4)                      <- pay_bill(5), finance(3), payroll(2), job(1)
    |
job (6)        payroll (3)
    |               |
pay_bill (6)   finance (8)     finance also <- payroll(1); pay_bill also <- job(1)
```

This is the shape Adrian described -- reference, then master data, then
sub-domains. **It is a graph of modelling references, not of load dependencies:**
section 3.1 shows a Raw Vault entity derives its parent keys from its own source
columns and never reads the parent table. The graph orders INTEGRATION, in the
Business Vault and Gold. The Raw Vault ignores it.

### 1.4 A vault table's shape cannot be altered in place

Vault tables carry `delta.appendOnly = true`, enforced by
`checks/append_only_check.py`. In-place `ALTER` is impossible and a full refresh
is refused by Delta (`DELTA_CANNOT_MODIFY_APPEND_ONLY`). `docs/RELOADING.md`
states the consequence: changing a vault table's shape means dropping and
rebuilding it, which today is `checks/drop_drifted_vault_tables.py` -- deliberately
kept out of the standing job because it destroys data.

Versioning is therefore not a preference among options. A shape change is a **new
table** or it is a **drop**. There is no third case.

---

## 2. Design brief

**Intended outcome.** Break the monolithic load into layered, per-domain blocks
that scale to 15-20 domains and 500+ tables; skip a domain whose data and
structure are both unchanged; and make a structural change non-destructive, so the
live structure keeps serving until its replacement is proven.

**Success criteria.**

1. A run in which only `pay_bill` changed does not spend 16 minutes on finance.
2. Adding the 7th..20th domain is an additive change, not a rewrite of the job.
3. A structural change to any vault object can be built, loaded, gated and
   **abandoned** without the live object being ALTERED, TRUNCATED or DROPPED. Note
   the precise claim: under sequential versioning (6.1.1) the live table STOPS RECEIVING
   NEW DATA once a new version is declared, and keeps serving what it already holds. Its
   history is never touched.
4. Every skip and every version cutover is visible in the run record. Nothing
   that did not run may look like something that passed.

**Decisions taken** (Adrian, 26 September):

| axis | decision |
|---|---|
| What triggers a domain | Automatic, from Bronze change detection |
| Job shape | One job per domain, plus a thin orchestrator |
| Versioning | Versioned table with a view alias flip |
| Gates | Split: scope what can be scoped, keep the rest global |
| Where dependencies are enforced | Raw Vault records history only; integration is asserted in the Business Vault and preferentially in Gold |
| Sub-domains (Customer / Supplier Invoices) | A SEPARATE SPEC, after this mechanism exists |
| A skipped domain and `assert_freshness` | The skip WRITES an audit row, marked as a skip |
| The view indirection | Accepted -- the view is the versioning mechanism |
| Phase order | Versioning (phase 4) FIRST |

---

## 3. Where domain dependencies live -- and where they do not

**Decided by Adrian, 26 September, and it is the decision this design turns on.**

> In the Raw Vault we just record the audit history. Cross-domain dependencies are
> checked in the Business Vault, or better, in the Gold layer, when we build the
> final tables.

### 3.1 The Raw Vault has no cross-domain load order

A Raw Vault entity **derives** its parent hash keys from its own source columns.
It never reads the parent table. Measured on `nhl_invoice_line/FIELDGLASS_US`:

```
invoice_hk       <- ['buyer_tenant', 'invoice_id']
supplier_hk      <- ['buyer_tenant', 'supplier_code']
worker_hk        <- ['worker_id']
organisation_hk  <- ["'Fieldglass_Buyer_Code'", 'buyer_tenant']
```

Every component is a column of the loading binding. `spec.hash_key_columns` is the
single authority for this and returns component names, never a join. Checked across
every raw-kind entity in the model: no hash-key component resolves outside the
binding that loads it.

Two consequences, and they are the whole reason this design is simpler than the
one first drafted:

* **Raw Vault domains load in any order, or all at once.** The 13 cross-domain
  parent references are MODELLING references -- an NHL naming `hub_supplier` as a
  parent -- not load-ordering ones. Adding a supplier to `hub_supplier` cannot
  change `nhl_invoice_line.supplier_hk`, because that value came from Fieldglass's
  `supplier_code`.
* **A raw-vault domain cannot be made stale by another domain's load.** There is
  no propagation to do.

This is Data Vault 2.0 working as intended: the Raw Vault records what each source
said, faithfully and independently. It does not integrate.

### 3.2 Integration is a Business Vault and Gold concern

Cross-domain agreement -- does this invoice's supplier exist, do payroll and the GL
agree, does a client code mean one thing -- is asserted where the final tables are
built. That is the Business Vault and, preferentially, Gold.

**Layer ordering therefore belongs to the Business Vault and Gold, not to the Raw
Vault.** It orders integration, which is a far smaller graph than the 500-table raw
estate.

**No such ordering is built by this design, because nothing in the model needs it
yet** -- every computed satellite is intra-domain (section 4.1). What is recorded
here is where the ordering belongs when the first cross-domain entity arrives, and
a build-time refusal (section 4.1) so that it cannot arrive unnoticed.

### 3.3 `party` is under review as a domain

Adrian, same conversation: *"maybe we need to rethink or drop the party
implementation."*

`party` groups `organisation`, `supplier`, `worker` and `legal_entity` -- four
entities that this design's own evidence shows nothing waits for. It was carrying
the weight of being "master data everyone depends on", and section 3.1 removes that
weight: nothing depends on it at load time.

Whether Customer, Supplier and Worker are better as domains in their own right, or
whether `party` survives as a grouping, is a **modelling decision and is out of
scope here**. It is recorded because this design must not assume `party` exists.
Nothing in sections 4 to 7 names it.

## 4. Job topology

| job | contents | grows with |
|---|---|---|
| `vault_prologue` | `create_control_objects`, `create_mask_functions`, `assert_hash_parity`, `assert_source_conformance`, `apply_source_unions` | fixed |
| `vault_<domain>` (one per domain) | the domain's Raw Vault pipeline, its hub/link/satellite loads, `publish_stable_views` (4.4), its scoped gates | one job per domain |
| `vault_epilogue` | `reconcile_loop1`, `assert_aggregate_reconciliation`, `assert_no_broad_grant`, `apply_governance`, `publish_model_metadata`, `assert_audit_completeness`, `assert_freshness` | fixed |
| `vault_load` (orchestrator) | one `run_job_task` per domain, `depends_on` from the layer graph, prologue first and epilogue last | one task per domain |

**The orchestrator fans every Raw Vault domain out in parallel.** Per section 3.1
there is no ordering between them, so `depends_on` for a domain job names only
`vault_prologue`. Integration runs after all domains; the epilogue runs last.

This is the difference between a design that scales to 500 tables and one that
does not: the raw stage is embarrassingly parallel and the ordered stage is small.

At 20 domains the orchestrator holds 22 tasks -- prologue, twenty domains in
parallel, epilogue -- and each domain job holds 6-8. No
job is a 40-task blast radius, and a domain becomes independently runnable,
independently testable and independently ownable.

### 4.1 Business Vault work splits by whether it crosses a domain

A computed satellite belongs to a domain like any other entity. Measured today,
**all three are intra-domain** -- every parent lives in the satellite's own domain:

| computed satellite | domain | parents' domains |
|---|---|---|
| `csat_invoice_line_gie` | `pay_bill` | `pay_bill` |
| `csat_job_request_custom_promoted` | `job` | `job` |
| `csat_payroll_line_classification` | `payroll` | `payroll` |

So the rule follows the evidence rather than a category:

* **Intra-domain computation runs inside its domain job.** The domain carries a
  second SDP pipeline targeting `business_vault`, filtered to that domain, and its
  own `load_satellites_business`. An SDP pipeline targets exactly one schema, which
  is why raw and business cannot share one pipeline -- that is a platform
  constraint, not a choice.
* **Cross-domain integration has no stage, because nothing needs one yet.** An
  earlier draft carried an ordered `vault_integration` job. Every computed
  satellite in the model is intra-domain, so that job would have been empty, and a
  stage that exists to be empty is a thing to maintain, explain and keep passing
  its gates for no return. Dropped on Adrian's instruction.

**The consequence, stated so nobody meets it by surprise.** The first entity whose
inputs span domains cannot simply be declared -- it needs the ordered stage built,
and it is the change that must build it. The design records what that stage is
(section 3.2: Business Vault or preferentially Gold, ordered by the domain graph)
so that the work is understood rather than invented, but the stage itself waits for
its first occupant.

**A check keeps this honest:** if an entity is ever declared whose parents live in
another domain, the build FAILS and names both domains. Silence would let a
cross-domain dependency load in an arbitrary order and appear to work.

A domain with no computed satellites emits no business pipeline, derived from the
model rather than configured.

### 4.2 Domain-specific writing tasks live in their domain

`invoice_issue` and `invoice_export` write the Ameren GIE output from
`csat_invoice_line_gie`. They belong to the `pay_bill` domain job, placed after
that domain's gates.

**DEF-44 still binds:** a writing task is gated on the correctness gates that
cover what it writes. In the split topology that means the gates of its own
domain, plus the global prologue. It does **not** mean the epilogue -- governance
and loop-1 run after every domain, so gating a domain's writer on them would
serialise the estate again. A writer that needs a global invariant belongs in the
epilogue instead, and that is a modelling decision recorded per writer rather than
a default.

### 4.3 A withdrawn gate returns to its domain

`assert_journal_integrity` is currently withdrawn from the job (26 September,
pending PLT-2). It asserts over GL journals, which are closed over `finance`, so
when PLT-2 lands it returns as a **per-domain** gate inside `vault_finance`, not
to the epilogue. `_WITHDRAWN_GATE_FILES` and `_WITHDRAWN_GATES` continue to
require that its absence stays recorded.

### 4.4 Every domain publishes the stable views for its pipeline-owned tables

**Decided 26 September, after Phase 1's whole-branch review found eleven entities with no
stable view.** A domain job carries one more task, after its pipelines and before its
gates: `publish_stable_views`.

**Why the gap exists, and why it is not a design.** A hub or a satellite needs anti-join
dedup and hashdiff change detection, which a streaming table cannot express, so it is
written by a BATCH LOADER and is `MANAGED`. A link, an NHL or a HAL is append-only by
nature, so an SDP streaming table suffices and the PIPELINE owns it. Only the batch
loaders publish stable views -- so `link`, `nhl` and `hal` have none. That is an
implementation detail of *which mechanism creates the table*, leaking into the data
model's contract, and it excludes **the largest tables in the estate**:
`nhl_general_journal_line_closed_year` at 14.6M rows and `nhl_general_journal_line` at
2.45M, precisely where a botched structural change costs most and a rebuild from Bronze is
most expensive.

**The premise that these do not change structurally does not hold.** This estate split
`nhl_invoice_line` into a thin link and a satellite in September 2026 -- the most invasive
remodelling of that week, forced by a performance problem. An NHL carries a transaction key
and a payload, and both are discovered rather than given; it is a working hypothesis about
grain, and the kind MOST likely to change.

**The task applies the same rule, from the same function.** `publish_stable_views` calls
the pure `stable_view_action()` the loaders use, with the same three cases:

  * the view is absent -> create it pointing at the table just built
  * the view points here -> leave it
  * the view points ELSEWHERE -> leave it, and report both names

One rule and one implementation, so a pipeline-owned table and a batch-loaded one cannot
drift apart. **The loader bootstraps; only `cutover_vault_version.py` moves a view.**

**It must also write an `aud_table_load` row per table**, which nothing does for
pipeline-owned tables today. Without it two things stay silently blind: `assert_freshness`
cannot see those tables at all, and `cutover --gated-by-run` can never be satisfied for
them, because it looks for exactly that row. This is not a nice-to-have -- it is what makes
cutover and retirement work uniformly across all eight kinds. `CREATE OR REPLACE VIEW` over
an existing TABLE is rejected by Spark, so without the view those tools cannot touch these
entities at all.

**`run_as` and the service principal are unchanged.** Every child job runs as the
same service principal the single job runs as today; the orchestrator does not
introduce a second identity.

---

## 5. Change-gated loading

### 5.1 State

A new control table, `control.ctl_domain_watermark`:

| column | meaning |
|---|---|
| `domain` | the domain this row describes |
| `bronze_table` | one source the domain reads |
| `last_data_version` | the Delta version of that source at the last green run |
| `model_fingerprint` | the domain's structural fingerprint at the last green run |
| `last_green_run_id` | the job run that established this watermark |
| `updated_dts` | when the row was written |

It is written **only on a green domain run**, so a failed run cannot advance a
watermark and cause the next run to skip work that never succeeded.

### 5.2 The source signal, and the trap in it

The signal is the **maximum Delta version of the source whose operation changed
data** -- `WRITE`, `MERGE`, `STREAMING UPDATE`, `DELETE`, `UPDATE`.

Operations that do not change data are excluded: `OPTIMIZE`, `VACUUM START`,
`VACUUM END`, `SET TBLPROPERTIES`, `ADD CONSTRAINT`, `CHANGE COLUMN`.

**This is not a hypothetical.** Measured 26 September:
`01_usnc_bronze_dev.sap_fieldglass_raw.invoices` is at **version 495, and that
version is an `OPTIMIZE`**. Comparing raw version numbers would re-run every
domain after every compaction, and the feature would appear to do nothing.

**Why not the approval manifest.** `control.ctl_approval_manifest` holds zero rows
and nothing in this lake can write one: no active reconcilable binding declares a
`manifest_column` (BRZ-12). A design resting on it would be blocked on the Bronze
team. The Delta history of the source is readable today and needs nothing from
anyone.

### 5.3 The model fingerprint

`model_fingerprint(domain)` = a digest over every entity YAML belonging to that
domain, plus `RULEBOOK_VERSION`, plus the domain's active binding ids.

Its job is to make a **structural** change force a run even when no data arrived.
Without it, editing a satellite's payload and redeploying would produce a skip and
a silent divergence between the model and the lake.

### 5.4 The skip rule

A domain is skipped when **all** of the following hold:

1. every Bronze source the domain reads is at the same data-changing version as
   its watermark, **and**
2. the domain's model fingerprint equals its watermark, **and**
3. `last_green_run_id` is set -- the previous run of this domain was green.

Otherwise the domain runs. In particular a domain with **no watermark row runs**,
so the first run after this ships is a full run.

### 5.5 Propagation, and why the Raw Vault needs none

**A Raw Vault domain is never made stale by another domain's load.** Section 3.1
is the reason: parent hash keys are derived from the loading binding's own columns,
so new rows in `hub_supplier` cannot change `nhl_invoice_line.supplier_hk`. A raw
domain's skip decision depends only on its own sources and its own fingerprint.

This was drafted the other way -- conservatively re-running every domain above one
that ran -- and that was wrong and expensive. Under it, any change to a widely
referenced domain would re-run nearly the whole estate, on feeds that land daily,
and the saving of section 1.2 would be lost entirely. The premise was that a parent
reload moves a dependent's keys. It does not.

**There is nothing else to propagate to.** Cross-domain integration is where a
stale join would matter, and no such entity exists (section 4.1), so this design
propagates to nothing. When the first one is built it must bring its own
propagation rule: if a domain runs, an integration entity reading that domain
rebuilds. That is recorded, not implemented.

**One exception that does propagate to a raw domain: a parent's MODEL
FINGERPRINT.** A parent's structure changing -- a business key added, a key scope
changed -- can alter how a dependent derives its parent hash key. Data changes do
not; structural changes do. So a raw domain also runs when any domain it names as a
parent has a changed fingerprint, which is rare and is precisely the case where
re-running is correct.

### 5.6 A skip is never silent

Every skipped domain is printed by the orchestrator and recorded in the run
summary, in the same form the estate already uses for
`waived_brz12=2`: a named count with the reason. A run in which four domains
skipped must not read like a run in which four domains passed.

---

## 6. Object versioning and cutover

### 6.1 Shape

An entity may declare `version: N` (default 1). The factory emits the physical table as
`<table>_rev<N>` -- `hub_invoice_rev2` beside `hub_invoice_rev1`.

**The suffix is `_rev<N>`, not `_v<N>`.** `_v1` was already taken: `naming.V1_SUFFIX` and
`naming.v1_view()` produce the DERIVED TYPE-2 VIEW over a satellite (`valid_from`,
`valid_to`, `is_current` computed and never stored; DEF-52, DEF-53), used in 16 places
with seven instances live. A physical table under that name would have collided with a
live view.

Consumers never reference a physical table. They read a **stable view** carrying the
unversioned name, `hub_invoice`, which selects from the live version.

### 6.1.1 Versions are SEQUENTIAL, not concurrent

**Decided by Adrian, 26 September.** This section previously said the two versions *"both
load concurrently from the same bindings"*, and the implementation does not do that.
`Entity.tables()` returns only the DECLARED version, so declaring `version: 2` removes v1
from the model: the factory stops emitting its staging flows, the loaders stop loading it,
`apply_governance` stops granting it and `assert_freshness` stops measuring it.

That behaviour is now the design, not a defect. What versioning guarantees is narrower than
"both load", and it is the half that matters:

* **The superseded table and its full history survive, untouched.** Nothing alters it,
  nothing truncates it, nothing drops it. `delta.appendOnly` is intact and
  `DESCRIBE HISTORY` still answers.
* **Consumers keep reading it** through the stable view until a deliberate cutover moves
  them, and a cutover is reversible by redefining one view.
* **The new version proves itself on real data** before anything points at it.

**The honest cost, stated because it is the thing that will surprise someone.** From the
moment `version: 2` is declared until cutover, the live table is FROZEN: it keeps serving,
with the rows it held at its last load, and receives nothing new. Consumers see stale data
for the length of the build window and **`assert_freshness` will not flag it**, because
that gate measures `tables()`, which now names only rev2.

So a version migration is a window to be kept short and watched, not a background activity.
Declare, build, gate, cut over, retire -- and do not leave a half-migrated entity sitting
over a weekend expecting the old one to keep current.

Concurrent loading was considered and rejected: it doubles the load cost of the estate's
largest tables for the length of the window, against a benefit -- freshness during the
window -- that a short window delivers anyway.

### 6.2 Cutover

Cutover is one view redefinition. It is reversible in seconds by redefining the
view back, because the previous physical table still exists and still has its
history.

**A version may not be cut over until it has passed the full gate set for its
domain**, at its own version. This is the whole point of the mechanism: the new
structure proves itself on real data while the old one keeps serving.

### 6.3 Retirement

The superseded table is retired by a **separate, deliberate task**, the way
`drop_drifted_vault_tables` is run today: read what it will destroy, run it, take
it back out. Retirement is never automatic and never part of the standing job.

### 6.5 Versioning covers all eight kinds, without exception

Phase 1 delivered versioning for the five batch-loaded kinds (`hub`, `sat`, `msat`,
`esat`, `csat`). `link`, `nhl` and `hal` were left out -- not by decision, but because only
the batch loaders publish views. Phase 2 closes that through `publish_stable_views`
(section 4.4).

**Until it is closed, the data contract, the DBML diagram and the source-to-target mapping
name those eleven entities as though a view existed.** It does not, so the name resolves to
whatever object holds it -- and after a pipeline builds `<table>_rev1` fresh, that is the
ORPHANED pre-migration table, frozen and silently stale. Nothing fails. That is the exact
defect section 6.1's stable view exists to prevent, reached through a different door, and
it is why this is Phase 2 work rather than a backlog item.

### 6.4 Consequence for existing checks

`drop_drifted_vault_tables` stops being the normal path for a structural change
and becomes a repair tool for genuine drift. `assert_append_only`,
`assert_mask_survival` and the loaders must all resolve a table through the view
indirection rather than assuming the unversioned physical name.

**This is a permanent interface change.** Every downstream consumer -- Gold, PIT
views, the DBML diagram, the data contract -- reads a view rather than a table
from that point on.

---

## 7. Gate scoping

The classification rule: **a gate is per-domain if and only if its assertion is
closed over one domain's tables.**

| gate | scope | why |
|---|---|---|
| `assert_hash_parity` | global, prologue | gate zero; cross-lake by nature and cheap |
| `assert_source_conformance` | global, prologue | compares regions, not domains |
| `load_hubs`, `load_satellites` | per domain | write only that domain's tables |
| `assert_mask_survival` | per domain | a masked column and its consumers live in one domain |
| `assert_append_only` | per domain | a history check is per table |
| `assert_landing_integrity` | per domain | closed over that domain's landing |
| `assert_key_derivation` | per domain | keys are derived per entity |
| `supersede_quarantine` | per domain | quarantine twins are per table |
| `load_satellites_business` | per domain | a computed satellite belongs to a domain (4.1) |
| `invoice_issue`, `invoice_export` | per domain (`pay_bill`) | domain-specific writers, gated per DEF-44 (4.2) |
| `reconcile_loop1` | **global**, epilogue | `landed + quarantined = approved` spans sources |
| `assert_aggregate_reconciliation` | **global**, epilogue | pairs cross domains (payroll <-> finance) |
| `assert_no_broad_grant`, `apply_governance` | **global**, epilogue | grants are catalog- and schema-level |
| `assert_audit_completeness`, `assert_freshness` | **global**, epilogue | ask whether anything failed to run at all |

`assert_freshness` stays global and gains meaning here: with skipping, "did this
feed load recently" and "did we choose not to load it" must be distinguishable.
It already measures against `aud_table_load` rather than `max(load_dts)`, so a
skipped domain must still be represented -- see the open question in section 10.

---

## 8. What this design does not solve

Stated because a design believed to cover more than it does is worse than none.

* **It does not make finance faster.** It avoids running finance. When finance
  does run it still costs 16 minutes.
* **It does not help a single large domain.** If one domain ever holds 500 tables,
  that domain's job is still slow. The unit of parallelism is the domain.
* **It does not unblock loop-1.** BRZ-12 still leaves `ctl_approval_manifest`
  empty and loop-1 unevaluable, globally.
* **It weakens the meaning of a green run.** A per-domain gate proves a domain,
  not the estate. That is why loop-1, aggregate reconciliation and governance stay
  global, and why a skip must be visible.

---

## 9. Risks

| risk | mitigation |
|---|---|
| A skip hides a real change | Three-part rule; fingerprint covers structure; no watermark means run; only a green run advances a watermark |
| `OPTIMIZE` causes false re-runs | Data-changing operations only, enumerated; measured against a real table already at an `OPTIMIZE` version |
| A dependent goes stale when its parent reloads | Conservative downstream propagation until measured otherwise |
| The view indirection breaks a consumer | It is a permanent interface change and is called out as one; every consumer is enumerated before cutover |
| A cutover flips to an unproven version | A version cannot be cut over until it has passed its domain's full gate set |
| 20 child jobs drift from one another | Every domain job is generated from the model by `tools/emit_pipeline_resources.py`, never hand-written; the byte-gate discipline applies |

---

## 10. Open questions

The question that had to be settled before implementation is settled, and is
recorded here as a decision rather than left in the list.

**DECIDED -- a skipped domain writes an audit row.** `assert_freshness` measures
against `aud_table_load` precisely so that a feed which legitimately delivered
nothing still proves the pipeline ran. A skip that wrote nothing would be
indistinguishable from a stalled pipeline, which is the exact failure that gate
exists to catch. So a skipped domain writes its `aud_table_load` rows with an
explicit skip marker and the watermark that justified it. One source of truth for
what happened in a run, and freshness needs no second mechanism.

Remaining, neither blocking:

1. **RESOLVED 26 September: SEQUENTIAL.** This asked whether versions load concurrently,
   as 6.1 originally claimed, or one at a time, as the implementation does. Adrian chose
   sequential and 6.1.1 now states it as the design, with its cost -- a frozen live table
   for the length of the build window, invisible to `assert_freshness`. The spec and the
   code agree again.
2. **Does `party` survive as a domain?** Section 3.3. A modelling decision, out of
   scope here, and this design does not depend on the answer.
3. **What are the 15-20 domains?** The design derives everything from the model, so
   it needs no list. The sub-domain split Adrian named -- Customer Invoices,
   Supplier Invoices -- is a modelling change and gets **its own spec**, written
   after this mechanism exists so that splitting a domain is an additive change
   rather than a migration.

## 11. Phasing

Each phase is independently shippable and leaves the estate working. **Adrian's
order: versioning first.** The safety net is in place before anyone begins changing
structures, so the first structural change made under the new topology is already
reversible.

1. **Object versioning and cutover. BUILT 26 September.** `version:` on entities,
   `_rev<N>` physical naming (NOT `_v<N>` -- that suffix was already the derived type-2
   view; see 6.1), the stable views, the one-time migration, the cutover and retirement
   tasks, foreign keys referencing physical tables, and the contract's `stable_name`.
   Ten tasks; `verify_repo` 1308 -> 1330.
   **Two things were deliberately carried into Phase 2:** stable views for the
   pipeline-owned kinds (4.4, 6.5), and the open question in 10.1, which the
   implementation contradicts and which needs a decision before this phase is final.
2. **Domain jobs and the orchestrator.** Split today's domains into per-domain jobs
   plus prologue and epilogue, with **no skipping**. Raw domains fan
   out in parallel per section 3.1. Behaviour is identical; only the topology
   changes, which is what makes it safe to prove.
   **Carries `publish_stable_views` (4.4)**, because the per-domain job is exactly where
   that task belongs: after the domain's pipelines, before its gates. Closing the
   versioning gap for `link`, `nhl` and `hal` and building the domain topology are ONE
   change, not two -- and until it lands, eleven entities including the estate's two
   largest tables have no stable view while every consumer artefact names them as if
   they did.
3. **Gate scoping.** Move the per-domain gates into the domain jobs; the global
   ones stay in the epilogue.
4. **Change-gated loading.** `ctl_domain_watermark`, the data-changing-version
   signal, the fingerprint, the skip rule and the skip audit row. This is where the
   16 minutes is recovered.

Phase 1 delivers safety; phase 4 delivers the performance outcome. Phases 2 and 3
are the topology they both rest on.
