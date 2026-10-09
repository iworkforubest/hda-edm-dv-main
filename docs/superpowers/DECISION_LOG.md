# Decision log — closed record

> **This is history, not a status board.** Everything here is closed work kept for its
> reasoning: what was measured, what was decided, and why. Newest first. For what is
> actually outstanding, read [`OPEN_ITEMS.md`](OPEN_ITEMS.md).
>
> Split out of `OPEN_ITEMS.md` on 5 September 2026, when that file reached 3,669 lines and
> was doing two jobs at once — a live board a reader must trust, and an append-only log
> nobody should have to scroll past to find it. Nothing was edited in the move except one
> still-open item, the two ungated worksheets, which was promoted into `OPEN_ITEMS.md`
> because it is not closed.

## `stream_generation` doubled an NHL's rows, because an NHL's idempotence lived in its checkpoint — closed 29 Sep 2026

**Fixed by `c3bd6c1` (`feat(naming,loader): every keyed kind is staged, and the keyed loader owns all four`).** `naming.STAGED_KINDS` is now `KEYED_KINDS | SATELLITE_KINDS`, so an NHL — like a link and a HAL — writes to a `stg_` log and is loaded into its vault table by `checks/load_hubs.py` with the same `NOT EXISTS` anti-join against the target that already made the hub idempotent. A generation bump still discards the `dropDuplicates` checkpoint and re-reads the whole log; **the loader's anti-join removes the duplicates that re-read produces**, so bumping a binding that feeds an NHL is now as safe as bumping a hub's.

Two things were retired in the same pass (`chore(spec): retire the unimplemented antijoin mode`): the `DO NOT BUMP AN NHL BINDING` paragraph at `SourceBinding.stream_generation`, replaced by a dated sentence saying the above; and the keyed multi-source refusal in `spec.validate`, whose own message named this fix as its remedy and whose condition (`keyed and NOT staged`) became unsatisfiable by any value once `KEYED_KINDS ⊆ STAGED_KINDS`.

**Still open, and tracked in [`OPEN_ITEMS.md`](OPEN_ITEMS.md):** the 1164 duplicate rows already in `nhl_invoice_line_rev2`. This entry closes the MECHANISM, not the data.

The measurement that produced the item, unedited:

Measured 28 September 2026 on usnc_tds, run 175944902540968. Bumping `stream_generation`
to 2 on the seven bindings reading `sap_fieldglass_raw.invoices` fixed the pay_bill
pipeline (SUCCESS, no DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE) and produced

    APPEND-ONLY GATE FAILED -- 1 problem(s):
      * nhl_invoice_line_rev2: 1164 duplicate rows at grain (invoice_line_hk)

`nhl_invoice_line_rev2` = 2329 rows / 1165 distinct keys. `nhl_invoice_line_rev1` = 1165 /
1165, untouched, and the stable view still resolves to rev1 -- no consumer was affected.

ROOT CAUSE. The three vault kinds do not dedupe the same way:
  * hub -- `checks/load_hubs.py` anti-joins the TARGET with NOT EXISTS. Idempotent.
  * sat -- hashdiff against rows already present. Idempotent.
  * NHL -- `factory.py`'s stateful `dropDuplicates(dedup_by)`, whose state IS the
    checkpoint. A generation bump discards it, the staging log is re-read in full
    (duplicates included, as DEF-42 intends), and every row is appended a second time.

So the staging log's "duplicates are harmless" property holds only because the consumer
removes them, and for an NHL the consumer's entire memory is the thing being replaced.

THE FIX was to make the NHL load anti-join its target the way the hub loader already
does, which makes a re-read safe for every kind permanently. **That is what `c3bd6c1`
did**, for links and HALs at the same time.

## AME001 (Invoice Number) — resolved 29 September 2026, and it needed no placeholder

`checks/invoice_export.py` has refused every run since Task 5 of the wd_fin_export plan,
because `invoice_number` is absent from `csat_invoice_line_gie`'s payload, from
`control.ctl_invoice_issuance`, and from every entity in `metadata/entities`. That refusal
was correct and is now cleared.

**The proposal was a placeholder.** Adrian, 29 September: use `hub_invoice`'s hash key as
the customer invoice number for now, the same shape as the `supplier_hk` decision for the
Workday supplier ID. Checking `MSP001_Ameren.xlsx` first showed it unnecessary.

**The template already specifies the mapping.** Sheet *4 Data Mapping*:

    Invoice Number | Invoice ID | Direct map | Source available | AEE1IN00123410 | Available

and sheet *7 Customer Invoice Example* shows the document itself:

    GUIDANT GLOBAL, INC.        Invoice Number        AEE1IN00123410

So the approved sample customer invoice is numbered with a Fieldglass-format invoice ID, and
AME001 is the direct map it was always described as.

**The clincher is the row below it:**

    Line Number | Not expected from SnapLogic | Generated sequentially by GIE within
                  Invoice ID | GIE-owned

The Guidant Invoice Engine owns LINE numbering, not invoice identity — it sequences lines
*within* an Invoice ID it receives. So "the GIE will use a different id" is true at line
level and not at invoice level, which is where the confusion sat.

**Why it looked missing, and what the fix is.** `invoice_number` really is absent from the
GIE satellite's payload. But `hub_invoice` carries it as its BUSINESS KEY —
`key_columns: [buyer_tenant, invoice_id]` — and Bronze holds it as
`sap_fieldglass_raw.invoices.Invoice_ID` (`AEE1IN00121247` in the live data; `Invoice_Code`
duplicates it, `Fieldglass_Invoice_ID` and `Consolidated_Invoice_ID` are empty). The export
projects from the GIE satellite and never joins back to the hub that holds the number.
**The fix is a projection, not a new identifier.**

**`UNDECLARED_SOURCES` anticipated this and was right to refuse.** Its entry says
*"`hub_invoice.invoice_reference` is the plausible candidate and this export deliberately
does NOT use it — that is the same inference that produced the two `unresolved` markers."*
That was correct while it was an inference. The template makes it a declaration, which is a
different thing, and the mechanism self-clears: `undeclared_required_fields()` measures
against `projection_columns()` rather than against the constant, so the entry must be
REMOVED in the same change or `verify_repo` goes red for the right reason.

**On length, which is why the check was worth doing.** The template states no limit, but the
approved example is 14 characters. A 64-character hex hash would be four and a half times
longer than the sample Ameren signed off. That is the difference from `supplier_hk`: an
internal reference nobody reads versus the identifier a customer pays against and searches
on. The placeholder was the riskier option here, not merely the uglier one.

**Not yet implementable, and the reason is the larger blocker.** `csat_invoice_line_gie`
cannot be built at all: `databricks.yml` records that "the factory has no way to compute a
csat payload — `_projection()` names the declared columns and `_project()` selects them off
the parent frame; nothing evaluates an expression", and the eleven AME rules live in
`src/accelerator/invoice_rules.py` as pure functions no factory module imports. AME001 was
the smaller of the two blockers on the Workday export. It is closed; that one is not.

## Defect index, written up 5 Sep 2026

Twelve `DEF-nn` identifiers were cited across the codebase with no entry anywhere in the
record. The reasoning was never missing — every one is explained at length in a comment at
the site it constrains, which is the right place for it. What was missing was a way in.

**So this is an index, not a second copy.** One line each, and the file that holds the
authoritative account. Copying the explanations here would create a second thing to keep in
step, which is the failure this repo spent 5 September removing. If a line here and the
comment it points at ever disagree, **the comment is right** — it sits next to the code the
defect is about.

| id | what it was | authoritative site |
|---|---|---|
| **DEF-13** | The hashdiff's trailing-NULL strip is a **regex pattern**, not a plain string, and the delimiter+token `\|\|^^` is made entirely of metacharacters. Interpolated raw it compiles as an alternation of two empty branches plus two anchors: it matches the empty string, strips nothing, and does so **silently**. Spark and the pure-Python reference then disagreed for every payload with a trailing NULL, so adding a column to any satellite would have reinserted every row in the estate. | `src/accelerator/hashing.py` — `_regex_literal_sql` |
| **DEF-15** | **An SDP pipeline library cannot locate itself.** `__file__` is unbound, and the code object's filename is a transient ipykernel path rather than the workspace path — so `src/` and `metadata/` must be *given* to the entry point, never derived from it. The second refusal is the dangerous one: it looks like it works. | `src/pipelines/silver_vault.py` head; both pipelines in `resources/vault_pipeline.yml` |
| **DEF-16** | **A masked column must declare a `cast:` type.** `debitamt MASK governance.mask_money` is not valid DDL without one, and the parser reports it obscurely — as a syntax error at the *mask function's* name, which sends you looking in the wrong place. | `src/accelerator/factory.py` — `_mask_clauses` |
| **DEF-17** | **One update is one load run.** The batch id is resolved once at build scope rather than inside a flow body, because discovering it needs `SET` and SQL refuses that inside a flow. | `src/accelerator/factory.py` — `build()` |
| **DEF-19** | **`create_streaming_table(schema=...)` is not an overlay.** Whatever is passed *is* the table's schema and must match what the flows produce — so declaring a mask means declaring every column. 83 columns for a GP journal line before DEF-26 projected the frame to the declared model, which was the defect; 26 after. | `src/accelerator/factory.py` — `_declared_schema` |
| **DEF-20** | **The function/grant split**, and why it is a filter rather than a second file. `raw_vault` cannot define a masked table until `governance.mask_money` exists, but `apply_governance` runs *after* the load in the job graph. Moving it earlier is the dangerous naive fix: the same file unconditionally `REVOKE`s on the shared Bronze catalog and then grants to groups that do not exist, so the revoke lands, the grant fails, and nobody can read Bronze. | `checks/apply_governance.py` — the `--functions-only` filter |
| **DEF-21** | **A `MASK` clause's function must be catalog-qualified.** Inside the pipeline a two-part `governance.mask_money` resolves against `spark_catalog` rather than the pipeline's catalog and fails with `SCHEMA_NOT_FOUND`. | `resources/vault_pipeline.yml`; asserted in `tests/test_accelerator.py` |
| **DEF-22** | **The two GP tables are not the same shape.** Both are 72 columns and 71 match; `gl20000` has `openyear` where `gl30000` has `hstyear` — same concept, different name. Since the split each entity names only its own table's column, so the two can no longer be confused in one file, and naming the wrong one fails at pipeline analysis rather than silently. | `metadata/entities/nhl_general_journal_line.yml` |
| **DEF-23** | **Delta refuses `CLUSTER BY` on a `BINARY` column**, and hash keys are `BINARY` by the rulebook — so clustering must be declared on something else, and an empty clustering list must be omitted rather than passed empty. | `tests/test_accelerator.py`; `factory._cluster_by` |
| **DEF-25** | **The ghost flow must supply every system column.** While schemas were inferred a missing one merely became nullable; once a masked table declares its schema the `NOT NULL` is real and the ghost flow fails outright. Partly superseded by DEF-41, which removed the `cdc_op` requirement. | `tests/test_accelerator.py`, asserted as a general property rather than per-column |
| **DEF-27** | **The uniqueness grain is derived from the table name, not sniffed from the columns.** Sniffing picks a plausible-looking key and asserts uniqueness over the wrong thing, which passes. | `checks/append_only_check.py` |
| **DEF-54** | **An inferred streaming table's `INSERT` must name its target columns.** Found by the **first real load**, not by any test — positional insertion into a table whose column order the loader does not control is a silent mis-assignment. | `checks/load_satellites.py`; asserted in `tests/test_accelerator.py` |

**BRZ identifiers are not here.** All fifteen live in
[`../bronze_layer_work_requests.html`](../bronze_layer_work_requests.html), a row and a full
article each — that is the page Bronze actually receives, and it is the record for them.

**Nor are DEF-2 through DEF-6**, which predate this log: they are written up under headings
of their own in `specs/2026-08-24-usnc-tds-retarget-design.md`. `verify_repo.py` treats the
whole `docs/superpowers/` tree plus the Bronze page as the record, so a citation resolves
wherever its write-up actually is rather than only where someone chose to look.

### Artefact gate coverage, audited 4 Sep

Measured by corrupting each committed artefact and re-running `verify_repo`, because
"is it generated?" and "is it gated?" are different questions and only the second one
matters. **11 of 14 caught.** Every generated artefact fails a gate the moment it is
edited: both mapping renderings, the silver and gold dashboards, the data / source /
control contracts, the DBML, the ontology, the ERD HTML, and `key_derivation.json`.

Three were NOT caught, and two of those are correct:
`docs/bronze_layer_work_requests.html` and `docs/first_load_findings.html` are
hand-written documents — byte-gating something meant to be edited would be wrong.

**`docs/client_mapping_worksheet.csv` is the real gap.** It is *derived* from Bronze but
maintained by hand, so nothing regenerates it and nothing gates it. Two defects found:

- **Two slugs were mis-parsed and are now fixed.** `feed_client_slug` comes from the
  table name, and whatever built it split `io_..._jr_automation` as `automation` and
  `io_..._waste_management` as `management`. Visible only because `buyer_in_data` reads
  "JR Automation" and "Waste Management" beside them. Three rows corrected, old value
  kept in the notes column.
- **Three clients are provisioned in Bronze and absent from it** — `dow_jones`,
  `everbank`, `lyondellbasell`. All nine of their tables hold **zero rows**, so the
  worksheet is not wrong today: it lists every client that has data, which is what it was
  built from. But when one starts delivering, nothing will add it and nothing will notice.

Also re-measured: **the table counts in BRZ-2 are no longer comparable to a fresh count.**
The four VMS schemas now hold 534 objects of which **264 are SDP `__materialization_*`
twins** and six are `event_log_*`. A raw schema count has roughly doubled since 26 August
with no new client arriving. Re-measure excluding those prefixes before acting on
"158 in fieldglass".

**Still open, ours:** either generate the worksheet (a tool that refreshes the measured
columns from Bronze while preserving the `__FILL` answers, and reports added or removed
clients) or accept that it needs a manual re-measure whenever BRZ-2 is answered. It
cannot be gated offline — the truth it describes lives in Bronze.

### Closed 4 Sep — the day's work, kept short

| | note |
|---|---|
| **Fieldglass live** | `hub_job_request/FIELDGLASS_US` loads through a union view over 30 per-tenant tables. 7,230 job requests. Reconciles to the row: 270,286 staged + 57,886 quarantined = 328,172 view rows |
| **`hub_job_request` re-keyed** | `federated` → `tenant_scoped`, tenant as the first key position. Forced six satellites to declare `parent_keys` — on the fallback each would have become a NEW orphan. Scope orphans 5 → 0 |
| **The reload** | 10 objects dropped following `RELOADING.md`. BULLHORN_EU restored EXACTLY (45,516 hub keys, 46,889 satellite versions), proving idempotence; a second full run changed nothing and TERMINATED SUCCESS |
| **`key_derivation_guard`** | The hash contract's third leg: the LAKE must agree with the model's key composition. Proven twice — FAILED `UNRECORDED` against the un-reloaded lake and blocked every downstream task, then MATCHED after |
| **`apply_source_unions`** | Tenant list DISCOVERED every run, not committed, so onboarding client 31 cannot silently drop them — and recorded in `ctl_source_union` so the change is visible |
| **BRZ-13 / BRZ-14** | Raised. BRZ-13 reduced from a build request to four questions once we built the view ourselves |
| **Artefact gate audit** | 14 artefacts corrupted, 11 caught. Two misses correct (hand-written docs); the worksheet was the real gap |
| **ERD PDF** | Was blank for ten days — `width:auto` on an inline SVG resolves to zero in wkhtmltopdf. Fixed, and now gated on characters-per-page, not just provenance |
| Bronze→Silver mapping | 489 rows, HTML + xlsx + CSV, gated. The xlsx writer is stdlib SO IT CAN BE GATED |
| `spec.hash_key_columns` | One authority for key composition; `factory.py`'s two helpers deleted. All 54 expressions proven byte-identical to the pre-refactor path |
| DEF-53 + `hub_worker`/UKG_US | Two orphans closed before the re-key closed the last three |
| `verify_repo` file walks | Were auditing PySpark and the Databricks CLI's deployment state. Now `git ls-files` with a floor |

| | note |
|---|---|
| Bronze→Silver mapping | `docs/source_to_target_mapping.{html,xlsx,csv}` — **489 rows**, generated from `metadata/entities/` and gated. The `.xlsx` is written by a stdlib writer (`tools/xlsx_writer.py`) rather than openpyxl **so it can be gated**; openpyxl stamps a creation timestamp. Gated on its ROWS, not its bytes — opening it in LibreOffice rewrites the container, and a gate that fires when a reader uses the document gets deleted |
| `spec.hash_key_columns` | One authority for what goes into a hash key. `factory.py`'s two expression helpers deleted. **Proven byte-identical over all 54 expressions** against the pre-refactor path — stronger than `hash_parity_check` can give, which pins the algorithm and never looked at the components |
| `metadata/key_derivation.json` | The golden record the components never had — **56** hash-key columns, gated byte-identical, plus property checks for scope/federation, transaction-key placement and the link-satellite guarantee |
| DEF-53 | Two business-vault satellites hashed parent keys under `BUSINESS_VAULT`, a name no feed delivers. Fixed with `SourceBinding.key_scope`, recorded in the identity digest |
| `hub_worker` UKG_US binding | Closed a silent orphan. Purely additive: one new derivation entry, one digest moved, hub held only its ghost row |
| `hub_job_request` → `tenant_scoped` | The last three orphans. Forced six satellites to declare `parent_keys` — on the fallback each would have become a NEW orphan. `FIELDGLASS_EU` removed: no such instance, and a source name is hashed INTO the key |
| Fieldglass union view | `metadata/source_unions.yml` + `checks/apply_source_unions.py`. Tenant list **discovered every run**, not committed, so onboarding client 31 cannot silently drop them — and recorded so the change is visible. Built and inert |
| BRZ-13 / BRZ-14 | Raised. See the outstanding table |
| `verify_repo` file walks | Were auditing `.venv` and `.databricks`. Now `git ls-files`, with a floor so over-excluding fails |

## FIXED 29 Aug: supersede_quarantine, and the second stub that hid a runtime failure

Root-caused and fixed. The gate now runs and reports honestly; twelve tasks succeed and the two
that do not were already on this board.

**Root cause.** `reject_digest.digest_columns()` defers `from .factory import _projection` to CALL
time, deliberately, to keep itself pyspark-free at import. Inside a job task that call pulled
`factory:56`'s module-level `from pyspark import pipelines`, which trips Databricks' DLT import
hook outside a pipeline. The first entity raised `Py4JJavaError o34.get`; the next two raised
`TypeError("'NoneType' object is not iterable")`, because the half-initialised module left None
where `_projection` should be. That also explains the older note in that file about "the digest
path being dead for all five NHLs ... survived four reviews" -- same path, related cause.

**`_projection` never needed any of it.** Its own docstring: *"Pure metadata: no Spark, no frame.
That is deliberate -- the shape of a vault table is a MODELLING fact."* All seven `dp.*` uses sit
inside five emitter functions that run within pipeline execution, so the import moved there.
Asserted structurally with `ast` against factory's module-level imports.

**NOT this session's work.** `reject_digest.py` and `supersede_quarantine.py` have no commits
here; the deferred import dates to `f06bfb7`. In the last 25 job runs the gate executed only
today -- the recent green runs were PARTIAL and had it `DISABLED`. Today's run was the first to
REACH it, so a long-standing fault became visible rather than new.

### The second stub that hid a runtime failure, hours after the first

`tests/test_accelerator.py:1321` creates `_stub_pipelines = types.ModuleType("pyspark.pipelines")`
and installs it. **The suite supplies the module the runtime refuses**, so importing `factory`
offline has always succeeded. This is the same shape as the `__file__` failure earlier the same
day, where the suite's loader SETS `__file__` and serverless does not.

Twice in one day the test double was more permissive than reality in precisely the dimension that
mattered. Neither was findable by making the offline tests stricter about behaviour -- both are
now asserted against the SOURCE with `ast`, where no stub can intervene. That is the general
lesson: when a double must model a hostile runtime, assert the property structurally, because the
double's job is to be cooperative.

### The gate's own output, and why NOT_EVALUATED is the right answer

`GATE SUMMARY :: supersede_quarantine :: status=NOT_EVALUATED asserted=0 not_evaluated=6` --
every quarantine twin holds no rows, and two entities have no active binding here. It exits 0 and
refuses to call that PASSED: *"Dormant by declaration or by an empty twin, not passing."*

### Still failing, both known and both external

* **`assert_journal_integrity`** -- red since 26 Aug on mask denial. Admin ask, job-runner group.
* **`reconcile_loop1`** -- DEF-48 firing exactly as designed:
  `ctl_approval_manifest: 0 manifest row(s)`. Nothing populates it, which is the finding from the
  28 Aug loop-1 investigation: the Bronze ask moved to `ctl_delivery_manifest` and the manifest's
  only named producer went with it. Blocked on BRZ-12 and on the producer decision.

## RAN 29 Aug: the gate works, and it found two of my own bugs before it worked

Three runs. The first two failed AT the new gate, both times blocking the load with nothing
written -- `raw_vault` and everything downstream `UPSTREAM_FAILED`. The third passed it and the
load proceeded.

**Run 1 -- `NameError: name '__file__' is not defined` (DEF-12).** Serverless `spark_python_task`
`exec()`s a check and does not define `__file__`; `source_conformance_check.py` used it at line 24
with no guard and died at import. `append_only_check.py` carried the identical defect, added the
same morning in `680e1d9`, and is a deployed hard gate that had not run since.

**Run 2 -- `Py4JJavaError ... NoSuchElementException: None.get`.** `contracts()` imported
`emit_source_contract` to reuse its path helpers, and that chain is
`emit_source_contract -> emit_data_contract:106 -> accelerator.factory:56 -> pyspark.pipelines`.
Importing `pyspark.pipelines` outside a DLT pipeline trips Databricks' import hook. "Reuse, do not
restate" was the wrong instinct: it coupled a job task to pipeline code.

**NEITHER WAS FINDABLE OFFLINE, and that is the lesson.** Every suite here loads a check with
`importlib.util.spec_from_file_location`, which SETS `__file__` -- including the `_FakeSpark`
harness that drives `main()` end to end and produces eighteen named failures against a
do-nothing `main()`. The harness and the real runtime differ in exactly one respect, and it was
the one thing nothing exercised: module import. Both properties are now asserted, and the import
check parses with `ast` after a first cut tripped over the docstring explaining itself.

**Run 3: `GATE SUMMARY :: source_conformance :: status=PASSED asserted=5 not_evaluated=0`.**
`raw_vault`, `load_hubs`, `load_satellites`, `business_vault`, `load_satellites_business`,
`assert_append_only` and `assert_aggregate_reconciliation` all SUCCEEDED. `assert_append_only`
passing is the DEF-12 fix proven in production on a gate that would otherwise have failed.

### Two tasks failed, and the honest position on each

* **`assert_journal_integrity` -- KNOWN, not ours.** Red since 26 Aug on mask denial; the
  run-as identity is not privileged. It is the outstanding admin ask for job-runner group
  membership.
* **`supersede_quarantine` -- `TypeError("'NoneType' object is not iterable")`,
  `asserted=0 not_evaluated=3`, for three entities with active bindings.** I can attribute this
  neither way, and say so rather than guess. **In the last 25 job runs it has executed only
  today**: the recent green runs were PARTIAL and had it `DISABLED / not selected`, and today's
  first two runs had it `UPSTREAM_FAILED` behind my own blocked gate. So there is no baseline
  showing it passing OR failing. My only touchpoint is `spec.py`, changed twice today, both
  additive -- a frozenset constant and a `validate()` refusal that raises. Neither returns None
  to this gate. The likeliest reading is that today's run is simply the first to REACH it, making
  a long-standing problem newly visible rather than newly created -- but that is a reading, not a
  measurement, and it needs its own investigation.

## DEPLOYED 29 Aug: source conformance now blocks the load in usnc_tds

`assert_source_conformance` is live in the `usnc_tds` vault job, verified by reading the DEPLOYED
job graph rather than trusting the deploy output -- "Updated jobs.vault_load" says the API accepted
something, not that the task landed with its dependency intact.

```
assert_source_conformance   depends_on: [assert_hash_parity]
                            parameters: ["--target", "usnc_tds"]
raw_vault                   depends_on: [assert_hash_parity, assert_source_conformance,
                                         create_mask_functions]
```

19 tasks, 1 changed, 0 deleted. Gate zero still runs first; nothing else in the graph moved.

`checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds` PASSED immediately before
the deploy, as DEPLOY.md:144 requires of every deploy including redeploys: both the bundle and the
CLI resolved to `adb-2593897084138079`. That check exists because eight workspaces look nearly
identical, and the profile was the human's choice, never inferred.

**It is wired, not exercised.** `main()` still has not run end to end -- the next vault load is the
first time, and a finding will block that load by design. Today's manual run against the same lake
came back PASSED with zero findings, so a surprise is unlikely; but that run exercised the
DECISIONS, not `main()`'s own I/O, exception handling or `not_evaluated` accounting. Those remain
covered only by the offline harness until a real load goes through.

**Known warning, pre-existing:** the deploy repeats DEF-43's world-writable bundle root
(`/Workspace/Shared/...`). That is the outstanding admin ask for a restricted folder, unrelated to
this change, and it is why `bundle validate --strict` is still unusable here.

## RUN 29 Aug: source conformance against usnc_tds -- PASSED, and exactly which half ran

First time any of this touched a lake. Profile `hfig-usnc-tds`, chosen by the human and passed
explicitly on every command; the other valid profile points at the EU production host and was not
used.

**Verdict: PASSED.** Zero existence findings across the contract's five tables, and ten cast
probes measuring **zero rows destroyed**.

| | |
|---|---|
| tables checked | `great_plains_raw.gl00100` (43 cols), `gl20000` (72), `gl30000` (72), `ukg_raw.gl` (16), `bullhorn_native_raw.joborders` (173) |
| required columns missing | none |
| cast probes | 10 -- six `double` to `DECIMAL(18,2)`, four `double` to `DOUBLE` |
| rows a cast would destroy | 0 |

The four `DOUBLE`->`DOUBLE` probes cannot be lossy: they are the no-op casts the
`mask_money_double` work introduced so a masked column could declare its type without a data
rewrite. The six `double` -> `DECIMAL(18,2)` are the ones that could have bitten -- a value with
more than two decimal places, or magnitude past 10^16, would have been silently nulled on the way
into the vault. None exists today.

### What ran, and what still has not

**The DECISIONS ran against real data.** Every verdict came from the check's own functions --
`contract_catalog`, `required_columns`, `missing_columns`, `cast_probes`, `lossy_casts`,
`gate_status` -- and the probe SQL was generated by `probe_sql()` itself, so what executed against
the lake is the check's SQL rather than a paraphrase of it.

**`main()` still has not run.** The I/O was supplied by hand through a SQL warehouse rather than
by `main()`, so its exception handling, its `not_evaluated` accounting and its output formatting
remain exercised only by the offline harness. The BUILT section's statement that `main()` has
never run against a workspace stays TRUE as written. This run answers "does Bronze satisfy the
contract", not "does the gate work end to end".

**One deviation from what the check issues:** the metadata query was narrowed to the three schemas
the contract names, rather than every column in the catalog's 47 schemas. Equivalent for these
five tables and materially cheaper, but it is not byte-for-byte what `main()` would send.

**Not wired into the job.** `resources/vault_job.yml` does not invoke this check, so nothing
re-runs it on the next load. Wiring it is a separate decision -- a code change and a deployment --
and until it is made, this verdict is a snapshot of 29 Aug, not a standing guarantee.

## BUILT 28 Aug: source conformance -- does Bronze still satisfy what Silver requires

Branch `feat/source-conformance`, worktree `hda-edm-dv-source`. **Not merged, and never run
against a workspace.** Spec `docs/superpowers/specs/2026-08-28-source-conformance-design.md`.
`checks/source_conformance_check.py` is the second half the source contract spec named out of its
own scope: given `source_contracts/<target>.yaml`, it asks whether Bronze still provides what the
contract says Silver needs -- in the shape `control_conformance_check.py` uses, pure decision
functions (`missing_columns`, `required_columns`, `cast_probes`, `probe_sql`, `lossy_casts`,
`report_lines`, `contract_catalog`, `gate_status`) with `catalog_missing` imported rather than
redefined, and Spark confined to `main()`.

**`main()`'s live path now has extensive offline coverage, through a harness -- not zero, as an
earlier version of this note said.** A final whole-branch review found that claim already false
by construction: the same function-local `from pyspark.sql import SparkSession` shape
`audit_completeness_check.py` uses meant `main()` could be driven end-to-end offline with a fake
Spark, exactly like `_run_audit_gate` already does for that check. `_run_source_conformance` (and
`_FakeSCCSpark`, an unrecognised-query-raises fake in the same family as `_FakeSpark`) now drive
`main()` through: a fully-conforming run to `PASSED`; ABSENT, MISSING COLUMN and LOSSY CAST
together in one run; a caller without `SELECT` on the `_raw` tables (zero `information_schema`
rows for every contract schema, reported `NOT_EVALUATED` rather than as five false ABSENT
findings); `try_cast` proving unavailable; and an exception on the cast COUNT probe itself
(§6's own named scenario, which used to kill `main()` with no `GATE SUMMARY` at all). It has
**still never been run against a real workspace** -- that remains a separate, consented act
needing the human's profile choice, and nobody has made that choice yet. See the row in the table
above.

**The coverage is uneven, and stays uneven by design.** Measured on `usnc_tds`, table
`great_plains_raw.gl20000`: the contract names 19 required columns and a type for four of them --
`debitamt`, `crdtamnt`, `ordbtamt`, `orcrdamt` -- because Bronze's own column types are not
modelled in this repository; a type appears only where Silver declares a cast. So existence is
asserted for all 19 required columns; castability is asserted only for the four carrying a
declared cast; the observed Bronze type of the other 15 is reported, and `report_lines()` is
proven to derive that set of 15 from the same `required_columns()` authority `main()` uses --
covering a payload column (`curncyid`), a column reachable only through a non-payload role
(`dex_row_ts`, via `applied_dts`), and a table whose `payload` is empty (`gl00100`, which a
payload-only derivation would drop from the report entirely) -- while the four typed columns are
proven absent from it, holding the asserted-versus-reported boundary. The block is also proven
unable to change the exit status either way -- both directions the review checked, since a report
that silently vanished would have satisfied the "cannot fail a build" half trivially.

**One cast probe per `(column, required type)` pair, not per column.** `required_casts` maps a
column to a sorted list of types, per Ruling T2-B: two bindings casting one column to two types is
a conjunction, and Bronze must supply a column castable to both. Every list holds exactly one type
in every committed contract today, so the second-type path is untested against a REAL contract --
but it is not untested outright, as an earlier version of this note said: a fixture at
`tests/test_accelerator.py:8607` fires it directly, asserting `cast_probes()` emits two probes for
a column declared against two types. What is true is narrower: no committed contract holds a
column with two required types, so that path has never fired against real data.

**One property is genuinely untestable offline, and it is narrower than this section used to
claim.** An earlier version treated "the check verifies `try_cast` exists in the runtime" as
untestable offline. That was wrong: the harness above drives that exact probe with a fake that
raises on it, proving both branches -- available (cast columns measured) and unavailable (every
cast column lands in `not_evaluated`, naming `try_cast is unavailable in this runtime` as the
cause, no COUNT probe attempted). What cannot be proven by a fabricated input is narrower: whether
Databricks' REAL `try_cast` behaves as documented against a live warehouse. That is asserted only
by running the check against a workspace, which nobody has done.

**`--skip-cast-probes` and every other not-measured cause report `not_evaluated`, never a pass,
and each names its own cause.** A caller without `SELECT` on the `_raw` tables can still run the
existence checks, but a green result must never conceal that the stronger half -- castability --
was skipped. `gate_status` treats a skipped probe the same as nothing evaluated: `NOT_EVALUATED`,
not `PASSED`. The message naming why is now specific to what actually happened --
`--skip-cast-probes was passed`, `try_cast is unavailable in this runtime`, a probe raising
mid-measurement, or zero `information_schema` rows for every contract schema -- rather than
unconditionally blaming `--skip-cast-probes` regardless of the real cause, which an earlier
version of `main()` did.

### What it does not cover

The columns governed expectations touch. That SQL lives in `control.ref_dq_expectation` and is
read at pipeline runtime, where it may name any source column -- not only the ones the contract
states. A Bronze change to one of those columns can still break a load after this gate has passed
it.

## MERGED 28 Aug: the source contract -- what Silver requires of Bronze, generated from the model

Branch `feat/source-contract`, worktree `hda-edm-dv-source`. **Merged.** Spec
`docs/superpowers/specs/2026-08-28-source-contract-design.md`. A consumer-side companion to
27 Aug's data contract: where `data_contracts/` describes what the vault *produces*,
`source_contracts/<target>.yaml` describes what it *depends on*, generated from the same
authority (`metadata/entities/*.yml`) so Bronze can only break us knowingly.

`tools/emit_source_contract.py` writes nine files, one per bundle target, keyed by fully
qualified bronze table. `verify_repo` fails if regenerating any of them produces a diff — the
same discipline `data_contracts/` and `metadata/key_composition.json` already carry, and for
the same reason: the artefact is committed so its diff is reviewable, and asserted so it
cannot drift from the model it claims to describe. **Do not hand-edit these files.**

### CLOSED 28 Aug: the placeholder tables, and the spec section that hid them

Found by the whole-branch review after all five tasks and seven task reviews had passed. **Not a
code defect** — every gate asserted `contract == model`, and the placeholders live in the model.
Nobody asked whether the model was fit to publish, because spec §3 asserted that it was.

§3 justified the design by claiming the placeholder tables "do not appear in any target's
`active_sources`" and were therefore excluded. `spec.py:954` says `active_sources=None` means
**every** binding is active, and only two of nine targets declare a list — so the placeholders were
active in seven targets and published there.

Measured, and the split was total: the two targets that declare `active_sources` published five
tables, **all five** in their own bronze catalog; the seven that declare nothing published nineteen,
of which **zero** were — fourteen placeholders per `DEPLOY.md:236` plus five `01_usnc_bronze_dev`
tables offered to WEU, UKS and AUE readers as their own requirement. The catalog check now reports
133 such tables if they ever return.

**Resolved by scope, not by filtering.** `emit_source_contract.configured_targets()` writes a
contract only for a target that has declared what it loads. Nine files became two, `dev.yaml` and
`usnc_tds.yaml`, and both were already correct. An empty contract for an undeclared lake would have
been worse than no file — it reads as "Silver requires nothing of you" when its Bronze does not
exist yet. A target gets a contract the day it declares one.

Two gates were added, both mutation-proven, and the first is the one that would have caught this:

* every table a contract names is in **that target's own `bronze_catalog`** — proven by making the
  emitter key one lake's tables under a foreign catalog;
* a contract exists for every target declaring `active_sources` **and for no other** — proven by
  restoring one of the seven deleted files, which reddens both new checks.

**There is no WEU, UKS or AUE contract, and that is correct** — we do not know those lakes' Bronze.
`databricks.yml:315` already understood this for the pipeline, qualifying `UKG_US` per entity
because it "is NOT uniformly real". The declaration was always the mechanism; the spec's error was
reading its absence as exclusion rather than inclusion.

### What it does NOT do, and where the actual value lands

**It does not check Bronze.** Nothing here reads a live lake. Whether Bronze *satisfies* the
contract — every required column still present, with a castable type — is a separate live
assertion against `information_schema`, and belongs with `control_conformance_check.py` in the
conformance family. **That check is where the value actually lands**: it would catch a
breaking Bronze change before a pipeline run does. It is deliberately a second spec, written
after this one because a live check needs something offline and reviewable to check against
first.

**It is not an agreement.** It is our published dependency, not something Bronze has
countersigned, and the artefact says so in its own text.

**It does not cover the control schema.** `control_contracts/bronze.yaml` already specifies
what Bronze's control schema must contain, and that one *is* a requirement rather than an
export. The two are deliberately separate artefacts with different standing.

### Two things a reviewer should not mistake for coverage

**`dedup_order` is excluded from required columns**, and the exclusion is a general property,
not a literal list. Two measurements justify excluding it at all: `factory.py:367` says
`dedup_order` is "DECLARED but not applied" — a partitioned ranking window is not a legal
streaming operation, and that is the runtime's only mention of it — and `input_file_name`,
which appears in most `dedup_order` declarations, occurs nowhere in `src/accelerator/` as a
column: it is Spark's file-metadata function, not something Bronze holds. A first version of
the gate named `input_file_name` specifically, following the spec's own example; measured
directly against the model, `dedup_order` actually names **three** columns model-wide —
`dex_row_ts`, `input_file_name`, `timestamp` — and `timestamp` (the tiebreaker on
`hub_accounting_journal`, `hub_organisation` and `hub_job_request`) was covered by nothing. The
gate now asserts the general property instead, computed from the model: a column whose *only*
declared role anywhere is `dedup_order` must not appear in any contract's required roles.
`dex_row_ts` is exempt and rightly so — it is also `applied_dts` on every `great_plains_raw`
GL table.

**Three `BUSINESS_VAULT` bindings are excluded**, because their `bronze_table` is
`hfig_*.raw_vault.*` — our own vault feeding business-vault entities, not Bronze. Excluding by
source *name* is a string convention; excluding by table *path* is structural. Both directions
are asserted to identify the same three bindings, so a future binding named `BUSINESS_VAULT`
that genuinely reads Bronze, or one differently named that reads `raw_vault`, fails a test
rather than silently changing what gets published.

### The strongest gate

Per bronze table, per required role (`business_keys`, `parent_keys`, `transaction_key`,
`payload`, `dedup_by`, `applied_dts`, `cdc_op`, `manifest`, `required_casts`), the stated
columns must equal the union of that role's declarations across *exactly* that table's active
bindings, recomputed independently from the model rather than read back off the contract's own
`read_by` list. It catches invention and omission as one failure, in both directions, and
exists because a model-wide membership check let a real column declared on one binding be
published as a requirement of an unrelated table — `gl00100` was told to defend a UKG-declared
column it never receives. The published contracts carry **19** distinct bronze tables across
**33** active bindings — 21 tables / 36 bindings, quoted elsewhere for this same feature,
counts the `raw_vault` tables and the three `BUSINESS_VAULT` bindings this section excludes
from publication, so it describes the whole model rather than what actually gets read by
Bronze-facing bindings. Most tables are read by several bindings; `ukg_raw.gl` and
`hfig_eu.bronze.striive_job_request` are each read by five, so requirements are *unioned* per
table — including `required_casts`, which merges to column -> sorted *list* of types, because
two bindings casting one column to two different types is a conjunction: Bronze must supply a
column castable to both.

### The limits, standing and merge note, asserted as parsed values

The artefact carries three limits, a standing statement and a merge note, and all five are
asserted present as *parsed* values compared against the emitter's own constants, per
committed file — not as key names, which YAML emits regardless of a value's content. This
does not add new coverage over the staleness check described earlier in this section: that
check already compares the whole committed file byte-for-byte against what the model
regenerates, so it would have caught a blanked caveat too, just with a generic "stale"
message. What the parsed-value check adds is a *specific* one — "standing", "limits" or
"merge_note" by name, rather than the undifferentiated diff a maintainer would otherwise have
to read to work out what changed.

* Bronze's own column types are not modelled — every type stated is a requirement Silver
  imposes, never an observation of what Bronze holds.
* The column list is complete for what the model declares, plus the two compiled-in
  key-safety rules, and *incomplete* for governed expectations: their SQL lives in
  `control.ref_dq_expectation` and is read at pipeline runtime, where it may name any source
  column. A Bronze change to a column only an expectation references would break a load this
  contract never mentioned.
* Each table's `declared_not_required` is a *sibling* of `requires`, not a member of it — a
  reader who flattens `requires` for "what's required" must not pick up `dedup_order` (or,
  transitively, `input_file_name`, Spark's file-metadata function, not a Bronze column).

## SPECIFIED 27 Aug: the cross-layer control standard -- silver verified, bronze and gold specified, nothing else instrumented

Branch `feat/control-standard`. **Not merged.** Spec
`specs/2026-08-27-cross-layer-control-standard-design.md`, plan
`plans/2026-08-27-cross-layer-control-standard.md`. Plan 1 of two -- the cross-layer dashboard
(Plan 2) is deliberately unwritten until a second layer actually has a control schema to show.

**What landed.** `src/accelerator/control_standard.py` is now the single declaration of what a
control schema must contain in any layer -- the mandatory core plus each layer's own tables.
Silver's existing `control_objects.sql` was **verified against it, not rewritten**: its DDL
already matched the declaration column-for-column with no correction needed. Gold's DDL
(`governance/control_objects_gold.sql`) and bronze's contract (`control_contracts/bronze.yaml`)
are **generated** from the same declaration and gated against drift -- `verify_repo` fails if
regenerating either produces a diff. `checks/control_conformance_check.py` adds a live check
that can compare a real workspace's control schema against the standard.

**What this does NOT do, and the distinction matters.** This branch does not instrument bronze
or gold. It specifies what their control schemas must contain and verifies that specification
against itself; standing either one up is the owning team's work. Concretely:

* **Gold's DDL exists but the gold catalog does not.** `03_usnc_gold_edm_dev` does not exist
  (spec §2) -- the DDL is unapplied by design, not by oversight. Nothing from this branch has
  been deployed anywhere.
* **Bronze has a published contract, not a control schema.** `control_contracts/bronze.yaml` is
  a description bronze can build against; no `control` schema exists in
  `01_usnc_bronze_dev` today.
* **The standard is verified across one layer, not three.** Silver was verified against the
  declaration. Bronze and gold were **specified** -- a different thing, and this document should
  not be read as claiming otherwise.
* **`checks/control_conformance_check.py`'s live path has never been run against a workspace --
  not once, deliberately.** Its decision functions (`append_only_tables_from_properties()`,
  the conformance comparisons) are pure and offline-tested against fabricated rows, the same
  shape as `schema_grant_check.py`. `main()`, which collects `SHOW TBLPROPERTIES` and catalog
  metadata from a live lake and calls those functions, has zero coverage from either suite --
  grep confirms neither `verify_repo.py` nor `tests/test_accelerator.py` calls it. So the live
  half of the append-only marker decision -- does a real bronze or gold table actually carry
  `delta.appendOnly = 'true'` -- is asserted by nothing yet. It becomes assertable only by
  pointing the check at a real workspace, which nobody has done.

**The Bronze ask changed.** `docs/loop1_control_table_request.html` now asks Bronze to stand up
**their own** `control` schema and land the delivery manifest there
(`01_usnc_bronze_dev.control.ctl_delivery_manifest`), not to write into a schema they do not
own. This corrects an earlier version of the same page, which had asked for the manifest inside
`governance` -- confirmed empty of any such table before the page was fixed. The `_manifest_id`
ask on the four feeds (BRZ-12, above) is untouched by this revision.

**Two things this branch left deliberately outside its scope:**

* **Governance was asked for and not delivered, on purpose.** The audit shape is genuinely
  uniform across layers, which is what lets one declaration serve all three -- but governance is
  not: bronze masks PII **physically**, rewriting values in place, while silver uses Unity
  Catalog **column masks**, which preserve the value and evaluate access per reader. Those are
  different guarantees, and whether they are interchangeable is a data-protection judgement with
  no owner. A shared governance standard would have defined that difference away, so this branch
  states the distinction and leaves it unsettled rather than picking an answer.
* **Plan 2, the cross-layer dashboard, is deliberately unwritten.** It needs a second layer with
  a real control schema to render against, which this branch does not create.

**Two corrections to how this work was reasoned about, worth recording because a later reader
could otherwise trust an earlier, wrong framing over the final one:**

1. The `APPEND_ONLY` / `MUTABLE` partition's *inward* direction -- that nothing declared is
   unclassified and nothing is classified twice -- predates *Task 5*, by a check at
   `tests/test_accelerator.py:7771`. **Corrected once more by the whole-branch review: that
   check is NOT pre-existing.** `git blame` puts it at `52a216d`, the THIRD commit on this
   branch (not the second -- corrected again by the scoped re-review, in the paragraph whose
   entire point is provenance precision). So both directions of the partition are new here. The sequence of claims went:
   "the whole partition is unasserted" (wrong), then "the inward direction was already
   asserted before this branch" (also wrong, and the wrongness was mine both times), and
   finally this. Recorded in full because the middle version credits pre-existing coverage
   that does not exist, which is exactly the claim a later reviewer would lean on to skip
   re-proving the check.
2. The Bronze request page's "47 schemas in `01_usnc_bronze_dev`" figure is dated **24 August**,
   not 27 -- it traces to `specs/2026-08-24-usnc-tds-retarget-design.md:432` and no re-count on
   the 27th exists anywhere in this repo. Both the request page and the plan's own verified-facts
   block were corrected to say so.

**What the whole-branch review changed, after the seven tasks were done.** It was asked for
one thing specifically -- a spec requirement no check asserts -- because mutation-proving only
ever tests the checks somebody wrote and is structurally blind to a check nobody wrote. It found
three Critical items and nine Important ones. What was fixed:

* **Spec §5 is asserted, PARTIALLY -- read the limits before trusting it.** It forbids any
  dataset subtracting one layer's count from another's, and §8 calls that "the only gate standing
  between this design and the tile section 5 forbids". Nothing asserted it;
  `quality.cross_layer_subtraction_findings()` now does, wired at `verify_repo.py:2184`, with
  fixtures for the forbidden tile and for legitimate within-layer arithmetic. **But the scoped
  re-review measured real blind spots**, and they matter because the natural way to build the
  forbidden tile is one of them: it MISSES `SUM(b.accepted) - SUM(s.accepted)` (aggregate-wrapped,
  which is what a real per-table "rows lost" tile with a GROUP BY looks like), CTE-based
  subtraction, and fully-qualified references with no alias. It also FALSE-FLAGS two legitimate
  shapes: same-layer dev-vs-prod reconciliation, and staging-vs-control in one catalog. Dormant
  today because no cross-layer dashboard exists. Plan 2 must not treat this gate as sufficient:
  it is a tripwire for the obvious spelling, not a parser.
* **Spec §6's literal is now the spec's wording, AND now guarded.** The code said
  `NOT INSTRUMENTED` where §6 requires `not instrumented` "explicitly, in those words" -- a
  future check written to the spec would have failed against the code. The scoped re-review then
  reverted that one word and found **both suites still green**: the fix was real but unguarded,
  the fifth instance on this branch of a claim exceeding its fixture, inside the fix for that
  very pattern. A check now reads the module's string literals via `ast` (so the caps in its own
  comment stay legal) and fails on the uppercase form. Also: for gold, whose catalog does not exist, `main()`
  queried `information_schema` and CRASHED rather than reporting not-instrumented. The spec's
  normal case for gold was an unhandled exception. `catalog_missing()` now decides it, pure and
  offline-tested.
* **Gold's DDL could not be applied at all** -- no `CREATE SCHEMA`, so applying it the day gold
  exists would have raised `SCHEMA_NOT_FOUND` on all three statements. Worse, two checks
  asserted a raw statement count, so *fixing* it turned them red with a message blaming a
  dropped `CREATE TABLE`. Both now assert meaning rather than a count.
* **Column TYPES were stated twice and compared nowhere.** `staged` could drift to `STRING` in
  silver's DDL while `bronze.yaml` published `BIGINT` to another team, with both suites green.
  Measured, then gated.
* **Two unfailable checks**, both of which this plan's own ledger had recorded as proven: a
  missing-column check whose fixture used the column name that appears in every message's prose,
  and a `tables_for()` guard whose deletion left the suite green because the bare dict lookup
  raises the same exception. Both now assert something the mutation can remove.
* **`docs/loop1_control_table_request.md` still carried the superseded ask** -- pointing Bronze
  at `governance.ctl_approval_manifest`, the location its own HTML twin's errata block says holds
  no such table. A live file, in the format most likely to be emailed. Fixed, and the pair is now
  gated so a correction to one can no longer be invisible to the other.

**What the review found and this branch did NOT fix -- open, and deliberately so:**

* **SETTLED 28 Aug: the loop-1 manifest mismatch, and it was not the mismatch it looked like.**
  The earlier entry here said loop-1 stays red if Bronze does what the page asks, and called it
  "the first thing to settle". Both halves were wrong in an instructive way.
  * `--manifest-table` already handles the LOCATION half; only the hard-coded `approved_count`
    at `loop1_reconciliation.py:327` was mechanically in the way.
  * The real objection is semantic: `delivered_count` (rows into `<source>_raw`) and
    `approved_count` (rows the loader takes one for one) differ wherever dedup runs, and
    `factory.py`'s `_stage` measures gl20000 at **~1.8 copies per business row across 7
    deliveries**. Substitution would put a hard gate ~80% out on a CORRECT load.
  * **But that objection does not bite where it matters.** Across the six reconcilable
    entities the split is clean: every binding with a `manifest_column` declares no `dedup_by`,
    and every binding that dedups carries no manifest. So the substitution is sound exactly
    where a manifest exists — and NOTHING asserted that. `spec.validate` now refuses
    `dedup_by` + `manifest_column` on a reconcilable binding, proven to fire against the real
    model (adding a manifest to `journal_line/UKG_US` now stops the build with an explanation).
  * **A derivation task was designed, approved, and then abandoned on evidence.** The plan was
    for silver to derive `approved_count` from its staging log. `naming.py:212` is explicit
    that links and NHLs do NOT use a staging log — it exists for hubs and satellites, which
    loop-1 REFUSES to reconcile. So the only counts inside silver are `landed` and
    `quarantined`, and deriving `approved` from them makes the identity hold by construction:
    DEF-48's vacuous gate in a new form. Not built, deliberately.
  * **What actually blocks loop-1, measured.** `databricks.yml:322`'s active sources exclude
    `payroll_detail/UKG_US` and `timesheet_line/STRIIVE_EU` — the only two reconcilable
    bindings carrying a manifest, and both bind PLACEHOLDER bronze tables
    (`nhl_timesheet_line.yml:10` says so). So **zero active reconcilable tables carry a
    manifest_id**, and loop-1 is dormant today whichever table it reads. It needs BRZ-12's
    `_manifest_id` stamp on the four feeds AND a post-dedup count, since all three active
    journal-line NHLs deduplicate. Neither is a code change on our side.

* **CLOSED 28 Aug: `append_only_check.py` now enforces what the spec says it enforces.**
  It scoped assertions to `AUDIT_PREFIXES = ("aud_",)` and routed every `ctl_`/`ref_` table to
  `not_asserted` as "written by an external producer". DEF-55's rationale for that is sound for
  the two tables it names — `ctl_approval_manifest` (external) and `ref_dq_expectation`
  (curated) — and `ctl_quarantine_superseded` was simply swept up by sharing their prefix. We
  write that table, `control_standard` declares it append-only, and
  `governance/control_objects.sql:149` says of it "THIS TABLE CAN DISARM A HARD GATE, which is
  why it is append-only **and asserted**". It was not asserted. Of every control table it was
  the one where the unbacked claim mattered most: a spurious supersede record REDUCES loop-1's
  quarantined count and makes that gate pass on a real variance.
  The split now comes from the declaration BY NAME, via a pure `classify_control_table()`
  offline-tested on fabricated names. **Undeclared control tables fail closed** — a table the
  standard classifies in neither set is a problem, not a skip, because it is invisible to every
  gate that reads the declaration. Proven both ways: reverting to the prefix test fails the
  specific check AND the "not a third restatement" check. Spec §3 and §8 row 7, which already
  claimed the property was enforced in silver, are now true rather than aspirational.

* **Spec §7's "the dashboard emitter reads the declaration" is unimplemented, not deferred.**
  `src/accelerator/quality.py:30` hand-maintains the six control tables and
  `verify_repo.py:2135` pins them to a literal set. Add a core table to the declaration and the
  dashboard silently keeps querying the old six. This is the third statement of silver's control
  shape, and the same duplicate-authority shape that `BUSINESS_KINDS` and the system-column set
  were.
* **A modelling call, for the accelerator owner, not for the branch.**
  `ctl_delivery_manifest` is classified MUTABLE, so `bronze.yaml` publishes "must stay
  correctable" for a table recording a delivery that already happened. Silver will reconcile
  against `delivered_count`; a mutable count can be rewritten after reconciliation, retroactively
  changing what a gate compared against, with no history -- the same disarm shape that made
  `ctl_quarantine_superseded` append-only. It mirrors existing practice for
  `ctl_approval_manifest`, so it is not a regression, but this branch is where it becomes a
  published standard and the spec classifies bronze's manifest neither way.
* **`main()` still has zero coverage from either suite**, and that surface is wider than an
  earlier draft of this section said: it owns the not-instrumented render, the operator-facing
  ABSENT output, and the live half of the append-only marker decision. The decision functions
  extracted from it are offline-tested; `main()` itself is exercised only by pointing it at a
  real workspace, which nobody has done.

**A standing caution, not specific to this plan.** Three separate times on this branch, a
defect was a check whose own comment or docstring claimed a stronger guarantee than its fixture
could detect -- a comment saying a check covers "both directions" when two-thirds duplicated
existing coverage; a module docstring claiming parity with another check's offline-testable
shape while the code itself was live-only; a check's failure text naming "case-sensitively"
with no case-variant fixture to prove it. None of these are visible from reading a diff
structurally -- the claim and the fixture arrive together in the same commit and read as
internally consistent. The only way any of the three was caught was mutating against the
comment's own words, not against the brief.

---

## CLOSED 27 Aug: the GL mask gap -- verified shut, 8,539,625 rows

Route A taken. `governance.mask_money_double(v DOUBLE)` added with the SAME admitted groups as
`mask_money`, and `ordbtamt` / `orcrdamt` declared in `masks:` on both GL entities with a
**no-op `cast: DOUBLE`** -- the columns already are DOUBLE, so the DDL requirement is met with
no data rewrite. Re-typing to `DECIMAL(18,2)` to reuse `mask_money` was the alternative and is
impossible: a type change needs a full refresh, which Delta refuses on `delta.appendOnly`.

Three checks bind the two functions to one policy: the DOUBLE variant exists, it admits
**exactly** the same groups as `mask_money`, and every GL amount column is masked in both
currencies. A divergence in those group lists would make the originating amount readable to
someone the functional amount denies -- the exact defect being closed.

The nine data contracts moved these columns from `classification: internal` to `financial`
with the mask function named -- 36 entries. The published artefacts had been describing
cleartext financial data as internal.

**DEPLOYED AND VERIFIED.** All four tasks SUCCESS -- `assert_hash_parity`,
`create_control_objects`, `create_mask_functions`, `raw_vault`. Gate zero ran before the load,
and it now sits in `raw_vault`'s own `depends_on` rather than arriving transitively.

**Verified as an identity the mask does NOT admit** -- the criterion below, not the deploy's
exit code:

| entity | rows | `debitamt` | `ordbtamt` | `orcrdamt` |
|---|---|---|---|---|
| `nhl_general_journal_line` | 2,453,132 | 0 | **0** (was 2,453,131) | **0** |
| `nhl_general_journal_line_closed_year` | 6,086,493 | 0 | **0** (was 6,086,492) | **0** |

`information_schema.column_masks` shows **8** masked occurrences of the two columns across
`raw_vault` -- 2 columns x 2 entities x (table + quarantine twin), so the twins are covered,
which was `mask_survival_check`'s core concern. That gate then ran and returned SUCCESS.

**THE VERIFICATION THAT MATTERED, and it was not "the deploy succeeded"** -- a validate-only
update had already returned COMPLETED, and this session produced four separate cases where an
API accepted an artefact that then did not do what it claimed. The criterion was:

```sql
SELECT count(ordbtamt), count(orcrdamt) FROM <catalog>.raw_vault.nhl_general_journal_line
```

Before: 2,453,131 visible of 2,453,132 rows, and 6,086,492 of 6,086,493 on the closed-year
entity. After a successful bind both must be **0**. Anything else means the DDL was accepted
and the mask did not take. Then re-run `mask_survival_check`, which should now cover these
columns everywhere they appear -- including the quarantine twins -- where it was structurally
blind to them before, because it only asserts over DECLARED-sensitive columns.

**STILL OPEN, deliberately: `xchgrate` and `denxrate`.** Re-measured after the fix and still
fully visible -- 2,453,131 rows. They are rates, not amounts, so they do not reconstruct a
value on their own now that both currencies' amounts are masked. But the payroll entity masks
`rate`, so the estate remains inconsistent about rates, and that is a business call about
sensitivity rather than an engineering one.

**Still owed by every other lake:** the mask function and the metadata are in the repo, so a
deploy carries them, but each lake needs its own pipeline update to bind them.

---

## SUPERSEDED 27 Aug, GOVERNANCE: the GL masks had an unmasked sibling column

> **This is the finding, kept for its measurements. It is CLOSED** -- see "CLOSED 27 Aug: the GL
> mask gap -- verified shut, 8,539,625 rows" above. `ordbtamt` and `orcrdamt` are declared in
> `masks:` on both GL entities via `governance.mask_money_double`; verified in the model on
> 28 Aug. The heading said OPEN, which a reader scanning headings would take to mean the GL
> amounts are still exposed. They are not.

`nhl_general_journal_line` masks `debitamt` and `crdtamnt` with `governance.mask_money`. It
does **not** mask `ordbtamt` or `orcrdamt`, which are the same money in the originating
currency, nor `xchgrate`, which relates the two. All three are in the entity's `payload` and
reach the vault.

**Measured 27 Aug as an identity the mask does NOT admit** — i.e. the masked columns returned
NULL to the reader, so this is the view a non-privileged holder of vault SELECT gets:

| column | rows visible of 2,453,132 |
|---|---|
| `debitamt` | 0 — masked |
| `crdtamnt` | 0 — masked |
| `ordbtamt` | **2,453,131 — unmasked** |
| `orcrdamt` | **2,453,131 — unmasked** |
| `xchgrate` | **2,453,131 — unmasked** |

So the protection on the functional amounts is defeasible from columns in the same row. For
the ~7,366 history lines whose functional amount is zero, the originating column is the only
place the value exists at all — and it is in cleartext.

**`mask_survival_check` cannot catch this, by design.** It asserts that a column DECLARED
sensitive is masked everywhere it appears. It has no way to know that an undeclared column
carries the same sensitivity as a declared one. The gap is in the declaration, not the gate.

**What closing it costs.** Adding `ordbtamt` and `orcrdamt` to the entity's `masks:` is a
metadata change, but masks are bound in the streaming table's definition
(`factory.py`), not by `ALTER TABLE` afterwards — so it needs a pipeline
redeploy, and `mask_survival_check` should then be re-run. `xchgrate` is a separate judgement:
a rate is not itself an amount, but with one masked amount and an unmasked rate the arithmetic
is available. Note the payroll entity already masks `rate`, so the estate is inconsistent
about exactly this.

**SURVEYED 27 Aug across every masked entity — the gap is confined to the two GL entities,
and it is larger than first reported.**

| entity | masked | unmasked sibling that matters | live rows |
|---|---|---|---|
| `nhl_general_journal_line` | `debitamt`, `crdtamnt` | **`ordbtamt`, `orcrdamt`** (same money), `xchgrate`, `denxrate` | 2,453,132 |
| `nhl_general_journal_line_closed_year` | `debitamt`, `crdtamnt` | **`ordbtamt`, `orcrdamt`**, `xchgrate`, `denxrate` | **6,086,493** |
| `nhl_payroll_detail` | `amount`, `gross_amount`, `rate`, `taxable_amount`, `ytd_amount` | none that reconstructs — `hours` is unmasked but `rate` IS masked | 1 |
| `nhl_timesheet_line` | `amount`, `rate` | none that reconstructs — `units` unmasked, `rate` masked | 1 |
| `nhl_journal_line` | `debit`, `credit` | none | 261 |
| `csat_payroll_line_classification`, `sat_accounting_journal_header`, `sat_job_request_commercials` | — | none | — |

So **8,539,625 live rows** carry the unmasked originating amount, not the 2.45M first
measured — the history entity is loaded and is the larger half. Confirmed on it separately:
`debitamt` 0 visible, `ordbtamt` and `orcrdamt` 6,086,492 visible.

**A hypothesis of mine was wrong and is worth recording as such.** I expected payroll to
share the defect because it masks five amount columns. It does not: it also masks `rate`, so
the unmasked `hours` cannot reconstruct a value. Payroll is better protected than the GL.
The GL is the outlier precisely because it masks one currency's amounts and leaves another's.

Two things the survey raises but does not settle, both for the business rather than us:
`xchgrate` and `denxrate` are unmasked rates beside masked amounts, and the payroll entity
already masks `rate` — so the estate is inconsistent. And `hours` / `units` are unmasked
working-time values which may be personal data in their own right, independent of any money
reconstruction.

**How it was found, which suggests where else to look.** Not by a gate — by investigating a
proposed DQ rule and asking what the rows it would drop actually contained. Any entity that
masks one member of a derived set of columns is a candidate: a masked amount beside an
unmasked amount, quantity or rate that reconstructs it. `nhl_payroll_detail` masks five
amount columns and is worth the same read.

---

## CLOSED 27 Aug: the control DDL still cannot evolve a table -- but it no longer lies about it

`governance/control_objects.sql` is all `CREATE TABLE IF NOT EXISTS`, which is a **no-op on
an existing table**. So a column added to a declaration after that table's first deploy never
lands, and `create_control_objects` still reports SUCCESS.

Found by trying to use it. `severity` was added to `ref_dq_expectation` by the data contract
work; `create_control_objects` was run on 27 Aug and reported SUCCESS; the column was still
absent. The first `INSERT` carrying a severity failed with *"automatic schema migration is not
allowed"*, having inferred the value as `col5`.

**Applied to `usnc_tds` by hand, and still owed by every other lake:**

```sql
ALTER TABLE <catalog>.<control>.ref_dq_expectation
  ADD COLUMNS (severity STRING COMMENT 'drop quarantines the row and the load continues');
ALTER TABLE <catalog>.<control>.ref_dq_expectation ALTER COLUMN severity SET NOT NULL;
```

`ALTER TABLE ... ADD COLUMNS IF NOT EXISTS` is not supported here, so the file cannot be made
self-healing in pure SQL.

**The gap worth closing was not the column, it was the silence — and that is now closed.**
`checks/apply_control_objects.py` verifies what it applied: it parses the declarations out of
the rendered SQL, reads `information_schema.columns` for the control schema, and FAILS the task
on any difference in either direction. Two pure functions, `declared_columns()` and
`column_drift()`, testable offline against fabricated rows like the decision functions in
`schema_grant_check.py`. Seven offline checks cover them, including the real historical case as
a fixture — `ref_dq_expectation` deployed without `severity` — and all were proven able to
fail.

It fails closed twice over. A declaration the parser stops matching yields no entry, and the
task refuses to report agreement on an empty result rather than passing having compared
nothing. And an undeclared column deployed in the lake is reported too, which is the same
drift in the other direction.

Ran live 27 Aug and printed what a verifying task should:

```
8 control statement(s) applied
6 control table(s) match their declaration, column for column
GATE SUMMARY :: create_control_objects :: status=PASSED asserted=9 not_evaluated=0
```

**What has NOT changed:** the file still cannot evolve a table, because
`ADD COLUMNS IF NOT EXISTS` is unsupported, so a column addition still needs a manual ALTER
per lake. The difference is that the deploy now tells you, by name, with the ALTER in the
message — instead of reporting SUCCESS and leaving it to be discovered a day later by an
INSERT failure.

---

## MERGED 27 Aug: quality reporting over the silver control schema

Branch `feat/quality-dashboard`. **Not merged.** Spec
`specs/2026-08-27-quality-dashboard-design.md`, plan
`plans/2026-08-27-quality-dashboard.md`. Runbook `docs/quality_dashboard_runbook.md`.
Silver only; bronze and gold have no control schema and no signal.

**Nothing is deployed.** The artefacts are generated and gated; no dashboard, Genie space
or `tst_` table exists in any lake. No SQL from `governance/` has been applied, and no
`databricks bundle deploy` has run.

**Only `usnc_tds` can deploy it.** All three quality resources use
`${var.sql_warehouse_id}`, which only `usnc_tds` sets (`databricks.yml:374`); the global
default is `""` (`databricks.yml:154`). An unset value **fails the deploy** rather than
pointing at another lake's warehouse, so this is loud rather than dangerous — but deploying
to any of the other eight targets means setting that variable for that target first, with a
warehouse id read from that workspace.

**The synthetic dashboard resource is COMMITTED COMMENTED OUT.** A declared bundle resource
is created by the next deploy, not when someone runs the exercise: while it was declared
unconditionally, the first deploy to any target would have published a dashboard titled
`Load quality (SYNTHETIC -- delete after testing)` over `tst_` tables that do not exist. A
check pins the disabled default, and another un-comments the template and asserts it still
declares `embed_credentials: false` — neither half of the embedded-credentials gate can see
a commented line.

### The finding that shaped it

Three sources look like quality signal and are not. The SDP event log's `data_quality`
payload reads `dropped_records: 0` across all 226 events that carry one and holds no
`expectations` array, because DEF-18 means this pipeline declares **no SDP expectations at
all** — quality is a hand-rolled filter plus a parallel quarantine flow. That field will
read zero for ever. `ref_dq_expectation` holds no rules, so every quality claim but key
safety is vacuous. Only the nine empty `qtn_` twins are real: key safety genuinely passed
on 4.7M rows.

So coverage is the headline, not pass rate, and no tile may compute a rate over a zero
denominator. `checks/audit_completeness_check.py:165` already set that precedent by
printing `GATE NOT EVALUATED` rather than passing.

### Two things to know before touching it

**Coverage counts target tables, not entities — via `explode(generated_tables)`, not
`meta_vault_model.table_name`.** That column is `base_table`, one row per entity, so
joining on it would undercount every multi-source satellite. 21 entities resolve to 25
physical tables. Keying on entities would call an entity covered when one of four bindings
had a rule — the defect that reached nine published data contracts before review caught
it.

**The dashboard and the Genie space have deliberately different exposure.** A dashboard
published with embedded credentials would show data to viewers holding no UC grant, which
is why it queries the control schema ONLY and why a gate fails the build if the committed
artefact names a vault schema. Genie evaluates access per user, so a Genie space could
point at the vault and still expose nothing.

**But the Genie space this branch ships declares the six CONTROL tables and nothing else.**
So **row-level reject investigation is not available through Genie in this release** — an
engineer who needs to see which rows were rejected queries `raw_vault.qtn_*` directly with
their own grant, as today. The reason is not exposure, it is validation: the DABs bundle
schema does not document the shape of `serialized_space`, so the emitted
`{tables, instructions, sample_questions}` structure is **unvalidated against the live
API**. The first task of any vault-scoped space is to validate that shape; adding table
names to an unconfirmed structure is not. A check asserts the `tables` list is exactly
`quality.CONTROL_TABLES`, so widening it turns red.

**Embedding credentials off is enforced by two complementary gates, not one.** A
depth-agnostic walk requires every `dashboards` entry, anywhere in `databricks.yml` or
`resources/*.yml`, to declare `embed_credentials` explicitly — the Lakeview publish API
defaults it to `true`, so an omission is the dangerous case a single value-scan would miss.
A comment-stripped text scan then requires that declared value to be a bare `false` unless
its `bundle_root_prefix` is on `_QR_RESTRICTED_PREFIXES`, which is deliberately **empty**:
default-deny until a restricted folder's ACL has actually been checked. Neither gate
removes the underlying exposure — `users CAN_MANAGE inherited=True` on the deployment
folder, DEF-43 above — it only stops one way of exploiting it. Business users cannot use
the dashboard at all until the restricted folder in that admin request exists; engineers
use the Genie space meanwhile.

### Synthetic data is the mutation test

Every tile is unproven while the estate reports zeros. `tst_` tables in the control schema
carry the seed: outside `CONTROL_PREFIXES` so `append_only_check` cannot sweep them and
they stay droppable, marked `hfig.control_object` so `misplaced_control_objects` passes,
and invisible to `audit_completeness_check` and `loop1_reconciliation`, which read their
tables by exact name. **Never seed `ref_dq_expectation` itself** — the pipeline reads it.

**What the seed does and does not exercise.** It proves the six tiles that read a `tst_`
table only: load volume, discards by reason, acceptance rate, incomplete runs, supersedes
and manifests. The acceptance-rate tile renders BOTH of its states, because
`msat_journal_line_worktag` is seeded with `staged = 0` — that row is the only thing making
the zero-denominator `not evaluated` branch render at all.

It does **not** exercise the two COVERAGE tiles, which are the headline. They read
`governance.meta_vault_model`, which has **no `tst_` twin and cannot have one** (it lives in
the governance schema, outside the control schema the test DDL may create in) and which
does not exist in any lake, because `publish_model_metadata` has never run. Applying the
seed and opening the synthetic dashboard without populating it first leaves both headline
tiles failing at view time on a missing table. The runbook now makes
`publish_model_metadata` step 1 of the exercise. Once it is populated, the coverage tile's
two branches become distinguishable: `nhl_general_journal_line` and `hub_job_request` read
`covered` from the seeded rules in `tst_ref_dq_expectation`, and every other generated table
reads `not evaluated`.

A leftover `tst_` table is no longer merely undetected by the other gates — it now fails
the build outright: `checks/schema_grant_check.py`'s `synthetic_objects()` check refuses
any `tst_` object still present, with `--allow-test-objects` for a sanctioned exercise in
progress.

### Parked findings, for triage rather than silent discard

Each is real, none is reachable without a deliberate future edit, and each was ruled on
during execution rather than dropped:

* **The commented-out synthetic resource is validated in isolation, not in place.**
  `textwrap.dedent` discards absolute indentation, so the template checks cannot see that
  re-indenting the commented block would, on un-commenting, silently make
  `quality_silver_synthetic` a *field* of `quality_silver`. The check's name says
  "un-commentable", which claims more than it verifies.
* **The template's key set never tracks the live resource's.** A field added to
  `quality_silver` never reaches the template. One line closes it:
  `set(_qr_tmpl_cfg) == set(_qr_dash["quality_silver"])`.
* **The purity check misses an aliased import.** `co_names` records the local binding, so
  `from pathlib import Path as P` passes. Its actual purpose — removing the false-positive
  direction, where honest prose tripped the check — is achieved.
* **`file_path` and catalog/schema checks are scoped to one resource file plus the template**,
  not to every dashboard the bundle can declare.
* **`render()` determinism is proven in-process only**, which is repeatability rather than
  cross-process determinism. Latent only: no set currently reaches an artefact.

### A better shape for whoever revisits this

The fix that comments out the synthetic resource, and the fix that pins
`include:` to exactly `resources/*.yml`, are in tension. The cleaner remedy for the first —
put the template in a file *outside* the deployed glob, so the YAML stays parsed and audited
by both halves of the credentials gate — is foreclosed by the second. Both were accepted in
one wave. Not worth unwinding, since the pin closes a verified reachable gap and the template
carries three checks, but the seam is here on the record.

### The defect shape worth carrying to the next branch

Ten findings on this branch were of the form "a check whose name claims more than its
condition tests". But the last one was sharper and is the general case: **a condition whose
DOMAIN is computed from the property it polices.** `_q_rate_ds` selects the datasets
containing `/` and then asserts they carry the rate guard; `_q_state_tiles` selects the tiles
carrying a literal and then asserts they render it. Remove the property and the subject leaves
the domain, so the check passes having nothing left to examine.

That is why mutation-proving could not find it: **every mutation that would fail such a check
also removes its own subject.** It is the same failure as a requirement with no check at all —
in both, nothing is looking — which is why the traceability pass found three blockers that
seven task reviews and every mutation table had missed.

Both `_q_rate_ds` and `_q_state_tiles` still have this shape, now backstopped by by-name
checks on the coverage path. The general form is unsolved: no non-self-referential predicate
was found for "every dataset whose tile can show a blank cell". **Look for this shape first.**

## MERGED 27 Aug: a data contract per target, generated from the model

Spec `specs/2026-08-27-data-contract-export-design.md`, plan
`plans/2026-08-27-data-contract-export.md`. Third of the three specs, and the only one that
needed no workspace at all — every task was verifiable offline.

`tools/emit_data_contract.py` writes nine datacontract.com-shaped YAML files to
`data_contracts/`, one per bundle target, and `verify_repo` fails if regenerating them
produces a diff. That is the discipline `metadata/key_composition.json` already gets: the
artefact is committed so its diff is reviewable, and asserted so it cannot drift from the
model it claims to describe.

**Do not hand-edit these files.** `metadata/entities/*.yml` remains the sole authority.

### Three things the reviews changed, and they are the reason to read this

**Only one of the three severity tiers exists.** The spec — mine — said `fail` stops the load
and that `expect_all_or_drop` was the pipeline's only mechanism. Both wrong. `factory.py:1005`
filters with `df.where(~failed)` and keeps the failed rows in the quarantine twin, so the load
continues; and `factory.py:988` records DEF-18, that `expect_all_or_drop` cannot be stacked
under `append_flow` and is therefore not used at all. So `fail` has no implementation either.
The emitter now refuses `fail` and `warn` alike, and the artefacts tag every rule `drop`.
`ref_dq_expectation` still gains the `severity` column — the table holds 0 rows, so nothing
migrated — because the contract must be *able* to say `warn` before the pipeline can honour it.

**A satellite has one physical table per source binding.** The first cut of the artefacts named
7 tables that do not exist and omitted 11 that do, because it took binding `[0]` and keyed on
the entity. That is worse than an omission: the four `sat_job_request_details_*` bindings carry
genuinely different column names — `title` / `job_title` / `posting_title` / `request_title`,
`numopenings` / `positions` — so a consumer integrating against the fictional name would have
looked for a column its table does not have. The artefacts now publish 25 tables, each with its
own columns, and a check asserts the published set equals the model's real set in both
directions.

**The per-column classification check is load-bearing beyond its name.** No model change was
made: a masked column takes the entity's `sensitivity`, an unmasked one is `internal`. That
derivation is total in both directions *today* only because no `internal` entity happens to
declare a mask — and **nothing enforces that**. `verify_repo.py:614` refuses a sensitive entity
with no masks; it says nothing about the converse. So the check "no masked column classifies as
internal" is not merely a test of the derivation; it is the only enforcement of the condition
the derivation needs. Weakening it would not miss a bug, it would remove the guard.

### What it deliberately omits

No SLA block — we measure no freshness, latency or uptime, and publishing "99.9% availability"
in a signed artefact nobody measures is a claim a consumer can hold us to and we cannot
evidence. No `delta_max_file_size`. No PIT or bridge entities: the model declares none, and
emitting them would describe tables that do not exist. No Bronze: that boundary has no model in
this repo to generate from, and belongs to whoever owns it.

### It does not govern anything

A contract generated from the model is a **description**. Every control it describes is enforced
by a gate that already exists — masks by `mask_survival_check`, grain by `append_only_check`,
key composition by the digest, grants by `schema_grant_check`. Nothing downstream should read
these files and conclude a rule is in force *because the contract says so*.

**Still undecided:** where the generated YAML is published — a catalogue, a registry, a repo the
consumers read. Deliberately left until the first contracts existed and could be looked at.
They exist now.

## MERGED, INERT, AND BLOCKED ON BRZ-12: error reintegration

The mechanism that lets a row we *wrongly* rejected land on a re-run without loop-1 going red.
Spec `specs/2026-08-26-error-reintegration-design.md`, plan
`plans/2026-08-26-error-reintegration.md`. Merged with **its live probe deliberately deferred**,
a decision taken knowing what the probe can and cannot prove today.

**What merged.** `control.ctl_quarantine_superseded`; `src/accelerator/reject_digest.py`
defining a rejected row's content; `checks/supersede_quarantine.py`, a batch task after
`raw_vault` that records a reject whose content later landed; and loop-1's identity becoming

```
landed + (quarantined - superseded) = approved
```

**THIS CODE IS INERT IN PRODUCTION TODAY, BY DESIGN, AND THAT IS THE HONEST STATE.** Two
independent reasons, both measured rather than assumed:

* **`manifest_id` is NULL across the entire estate** — `nhl_general_journal_line`, 2,453,132
  rows, **zero** non-null. Loop-1's variance query drives from the manifest table and joins
  `USING (manifest_id)`, so it compares nothing at all. This is **BRZ-12**, already in the
  outstanding table above, owner Bronze team.
* **Every `qtn_*` table holds 0 rows.** Nothing has ever been quarantined, so
  `supersede_quarantine` reports NOT EVALUATED and exits 0 on every run.

So the merge bought a **reviewed** mechanism, not a working one. What stands between those two
states is a Bronze request that predates this work.

### The probe is deferred, NOT done

Spec §7 specifies an end-to-end probe: force a rejection with a temporary over-strict rule,
confirm it quarantines and loop-1 is green, fix the rule, confirm the rows land and
`ctl_quarantine_superseded` gains exactly one record each, and — **the step that matters** —
confirm loop-1 goes **RED** when the supersede records are removed. Without that last step the
subtraction could be doing nothing and every other observation would look identical.

**None of it can run until BRZ-12 lands**, because every step depends on loop-1 actually
comparing something. Do not read the merge as evidence the mechanism works.

### What the reviews caught, because it is the useful part

Two Criticals survived four per-task reviews and were caught only by the whole-branch review:

1. **The digest path was dead for every NHL.** `Entity.tables()` yields `src=None` for the
   unstaged kinds, so `digest_columns(entity, None)` raised `AttributeError` inside
   `factory._projection`. Five of six reconcilable entities crashed — including all three the
   job reconciles. Only the link worked, because a link's projection never reaches `src.payload`.
   The plan's own test passed `entity.sources[0]`, a shape production never uses, so the check
   named "is never empty, for any reconcilable entity" tested a call that does not occur.
2. **The vacuity rule turned that crash into a green dormancy report.** Exception skips counted
   toward NOT_EVALUATED, so an all-crashed run exited 0 under a dormancy banner. This had been
   *parked* earlier on the reasoning that failure-to-act is safe for this gate — right about
   safety, wrong about diagnosis, and precisely what hid the first Critical.

Both are fixed, and the tests are now driven from `entity.tables()` so the production shape is
the tested shape.

### Known limitations, recorded rather than fixed

* **A shrinking twin cannot be reconciled.** A full refresh of `raw_vault` truncates `qtn_*`
  while `ctl_quarantine_superseded` is `delta.appendOnly`, so `orphaned_records()` and
  `over_subtracted()` would fire permanently. Recovery is a manual, approved control-table
  cleanup — spec §9. No delete path was added, deliberately: automating deletion from an
  append-only control table inside a gate is worse than the problem.
* **A rulebook bump needs that cleanup BEFORE the next loop-1 run.** A stale-version record is
  excluded from `recorded` but still counted by loop-1, which then sees two superseded against
  one quarantined and hard-fails. Loud and documented, but it fails until cleaned.
* The whole Spark half — `digest_counts`, the INSERT path, and both of loop-1's new queries —
  is **unexercised against real data**, and there is no fake-Spark harness for
  `loop1_reconciliation.main()`. Careful reading is all the verification it has had.

## LIVE 26 Aug: the control schema is deployed, and the split is real

Merged as `93ba184` (20 commits) and deployed to `usnc_tds`. Preflight passed; `bundle
validate` showed only the known DEF-43 bundle-root warning; all three resources updated —
both pipelines too, because `expectations_table` moved schema.

**The schemas now hold what their names say:**

```
control      aud_load_run, aud_table_discard, aud_table_load,
             ctl_approval_manifest, ref_dq_expectation
governance   pipeline_event_log, pipeline_event_log_bv
             + mask_money, mask_personal_name, mask_tax_reference, mask_tokenised_account
```

The two moved tables were dropped from `governance` only **after** the `control` copies were
confirmed present, and both were verified empty immediately before the drop. The deployed job
was also confirmed to pass `--manifest-table
02_usnc_silver_edm_dev.control.ctl_approval_manifest` before anything was dropped — so
nothing was reading the old address.

**Measured live, not assumed:**

| | |
|---|---|
| `create_control_objects` | **SUCCESS** — 7 statements applied, run `683278350345460` opened |
| the `opened` row | present, carrying the real run id and target `usnc_tds` |
| orphan-schema gate | **5 schemas present, 4 declared — PASSED.** `information_schema` excluded by owner |
| `control` grants | 0 schema-level, 0 table-level — inside the Phase 6STOP posture |
| grant gate overall | PASSED, 10 securables, `not_evaluated=0` |

**`create_control_objects` exiting SUCCESS is itself the DEF-14 fix proving out.** As the plan
originally wrote that file, a *passing* task would have marked itself FAILED and blocked the
whole job — `publish_model_metadata` depends on the audit gate, so the run's `completed` row
would never have been written, which the gate then reports as an unclosed run for ever. A
self-sustaining false failure, caught only by the whole-branch review.

**What is NOT yet exercised.** No load has run since the deploy, so `aud_table_load` and
`aud_table_discard` are empty, so the per-table loader instrumentation has not written. `aud_load_run` is NOT empty: it holds one `opened` phase with no `completed`, from run 683278350345460 on 26 Aug 18:59, so the run-level instrumentation has fired once and the incomplete-runs tile has a real row. Five of the six control tables exist; only `ctl_quarantine_superseded` is absent. The
first full job run is what proves the arithmetic end to end. `ctl_approval_manifest` and
`ref_dq_expectation` are still empty by design — the first because populating it ourselves
would make the vault self-certifying, the second because the generator falls back to its two
compiled-in key-safety rules.

**Still open from this work:** the `hfig.control_object` assertion the spec names in §6 (every
control object lives in `control`, none left in `governance`) is unimplemented, and the
event-log audit writer from §4 was deliberately deferred — only 162 of 2,759 `flow_progress`
rows carry metrics at all.

## CLOSED 26 Aug: the `silver_vault` orphan schema and the probe leftovers

Found by inspection, not by any gate — which is the point of it.

`silver_vault` was the vault's schema name before `9b3e5b0` split it into `raw_vault` and
`business_vault`. `verify_repo.py:1479` already asserted *"silver_vault survives nowhere in
the bundle"*, and that check passed — the name was gone from the **config**. The **schema**
was still in the lake, and on 24 Aug it was used as scratch space for the mask-propagation
probe. Nothing pointed at it and no gate looked at it: `schema_grant_check` is passed
`raw_vault` and `business_vault` only.

Dropped in dependency order, five statements:

```
DROP VIEW      silver_vault.probe_plain_v        -- view over the base
DROP TABLE     silver_vault.probe_plain_base     -- 2 rows; frees both mask bindings
DROP FUNCTION  governance.probe_mask
DROP FUNCTION  governance.probe_mask_priv
DROP SCHEMA    silver_vault
```

**Checked before dropping**, because the order is forced by the mask bindings and a probe
function cannot be dropped while a column still names it: no grants existed on the schema or
its tables (owner-only, so it was clutter rather than an exposure), and no column **outside**
`silver_vault` was bound to either probe mask.

**Verified after:**

| | |
|---|---|
| schemas | `business_vault`, `governance`, `information_schema`, `raw_vault` — `silver_vault` gone |
| functions | exactly the four real masks; both probe functions gone |
| column masks | **14 → 12**, all twelve `mask_money`. The two probe bindings were the only loss |

The 12 surviving bindings are the six journal tables × two amount columns, which is what
`mask_survival_check` walks. `mask_personal_name`, `mask_tax_reference` and
`mask_tokenised_account` remain defined and bound to nothing — expected, and already reported
by `verify_repo` as "functions defined but not yet bound to a column". They bind when the
payroll and PII satellites activate.

**This is the motivating example for the orphan-schema gate** in
`specs/2026-08-26-control-schema-design.md` §6. The lesson is narrower than "clean up after
probes": a check asserted the name was gone from the **bundle** and passed, while the object
it named was still in the **lake**. Config-level assertions do not see lake state. The new
gate keys on schema owner rather than a name list precisely so it cannot be satisfied that
way.

## MEASURED 26 Aug: `assert_journal_integrity` is red because of mask denial, not data

The gate reads `debitamt` / `crdtamnt` on `nhl_general_journal_line`, gets NULL for all
2,453,132 lines, and correctly refuses to assert on zeros. **The NULLs are the mask
working, not missing data:**

* both columns carry `governance.mask_money` (`information_schema.column_masks`)
* `is_account_group_member` returns **false** for the load identity
  (`adrian.turcu@headfirst.group`) on **both** `global_dataplatform_pipeline_job_runners`
  and `usnc_data_analyst_finance`

So the mask denies to exactly the identity running the loads. This is the evidence behind
admin request two, and it also closes the gap noted when that request was raised: the
masks previously named groups that did not exist, so they denied to everyone and were
never genuinely exercised. They are now enforced.

**The irreversible failure has not happened yet.** A materialized view over a masked table
would freeze these NULLs in as fact, and silver is insert-only. Checked 26 Aug: no such MV
exists — the only derived object is `sat_..._v1`, a **plain** view, which stores nothing
and inherits the mask at read time. The exposure begins the day the first PIT or Gold MV
is built.

---

## CLOSED — the reload (25 Aug, done)

Completed. Hubs and links carry their declared shapes (verified above), hashes reproduced
byte-identically, and `append_only_check` is green across 32 tables. Kept for the
reasoning.

`75bd524` projects every vault table to its declared model — `hub_accounting_journal`
92 → 13 columns, `nhl_general_journal_line` 83 → 26. The loaded tables still have the old
shape. They are append-only and pipeline-owned, so they **cannot be narrowed in place**.

**The reload is a drop-and-reload of 7 vault tables + 7 quarantine twins.** Safe in
principle — every row derives from Bronze, nothing originates in the vault — but
`checks/append_only_check.py` exists precisely to stop this happening casually, and
`full_refresh: false` is set for the same reason. It needs a deliberate decision.

**Verification the reload provides:** hashes must come out byte-identical.
`src/accelerator/hashing.py` is untouched, `RULEBOOK_VERSION` is `1.0.0`, and
`metadata/key_composition.json` is unchanged — so the same keys must reappear. If they do
not, something moved that should not have.

`create_mask_functions` must run before `raw_vault`; the quarantine twins now declare MASK
clauses too.

**Materialization grant — SETTLED 25 Aug 2026, and it is a real exposure.**

```
nhl_general_journal_line                            2,453,132 rows          0 readable debitamt
__materialization_mat_…_nhl_general_journal_line_1  2,453,132 rows  2,453,131 readable debitamt
```

Same data, same `raw_vault` schema. The backing table is `MANAGED` and carries no mask, so
**a column mask on a streaming table is defeated by anyone holding schema-level `SELECT`**.
The backing name embeds a per-update identifier, so no static grant can deny it.

The mechanism is `governance/apply_masks.sql`, which grants `SELECT ON SCHEMA` — that
covers backing tables. So the grant decision is not only *which groups* but **whether the
vault schema should be consumer-readable at all**. In a medallion design silver usually is
not: consumers read Gold or the `_v1` projections, and only the pipeline principal plus a
privileged group hold schema access.

This does **not** block the reload — the reload neither improves nor worsens it. It does
belong with the group-mapping decision, which is already held at `DEPLOY.md` Phase 6STOP.

---

## From the final whole-branch review — RE-CHECKED 26 Aug: nothing here is live

Verified against the working tree on 26 Aug 2026, not from the record below. Items 1-4 were
closed by `dbdbc29` (DEF-44), `9dda097`/`73938c9` (DEF-50, DEF-51) — all committed *after*
this list was written, which is why it read as open. The line numbers cited in 1 and 2 have
also moved. Item 5 was never a defect at all and is withdrawn on evidence. The Minor's
surviving half — the missing cast-loop placement check — was closed on 26 Aug.
**Nothing in this section is live any more.**

Kept in full for the reasoning, and as a record of a specific way this file goes wrong: a
finding written once gets re-confirmed by pattern-matching its own wording instead of
re-deriving it. Item 5 survived two passes that way.

1. ~~**`resources/vault_job.yml:158`** — `apply_governance` depends on `assert_append_only`,
   `reconcile_loop1` and `assert_aggregate_reconciliation`, but **not**
   `assert_journal_integrity`. Three of four gates block governance; the accounting one
   does not. It was added after the dependency list was written.~~
   **CLOSED (DEF-44, `dbdbc29`).** `vault_job.yml:309` now reads
   `depends_on: [{task_key: assert_no_broad_grant}, {task_key: assert_journal_integrity}]`,
   and `assert_no_broad_grant:280` carries the other three, so all four post-load gates
   block governance transitively. The comment above it records why.

   Note this is the gate that is **currently RED** for mask denial (see the MEASURED
   section above) — so with this dependency in place, `apply_governance` and
   `publish_model_metadata` will not run until the job-runner group membership lands. That
   is the wiring behaving correctly, not a second defect.

2. ~~**`vault_job.yml:78` and `:196`** pass only `--schema ${var.vault_schema}`, so
   `business_vault` is never append-only checked, and `mask_survival_check` queries the
   wrong schema while iterating all entities — `csat_payroll_line_classification`'s two
   masks will report MISSING the day its binding activates. Only the aggregate gate got
   `--business-vault-schema`.~~
   **CLOSED (DEF-44, `dbdbc29`).** `assert_append_only:176-178` and
   `assert_mask_survival:351-354` each pass both `--schema` values; `assert_no_broad_grant`
   does too. `--business-vault-schema` now also reaches `create_mask_functions`,
   `load_satellites`, `assert_aggregate_reconciliation` and `apply_governance`.

3. ~~**`tools/refresh_key_composition.py`** — the digest is still blind to two identity
   inputs: `src.cast`, applied before the key hash, so casting a key column re-keys
   silently; and bindings declaring none of the tracked fields are dropped, so under
   `key_style: federated` renaming a binding is invisible.~~
   **CLOSED (DEF-51).** The digest records `cast_on_key_columns` — key-feeding casts only,
   deliberately, so a cast on `debitamt` does not move a digest it cannot re-key — and the
   `if binding:` drop is gone. Both halves are asserted, and asserted to *fail*, at
   `tests/test_accelerator.py:3262` and `:3268`.

4. ~~**`src/accelerator/spec.py:322`** — the arity check for `parent_keys` already exists at
   `:337`, three lines below an early return that skips it. All four literal-free hubs are
   on that path; a miscount yields a foreign key that can never join. Hoisting it is three
   lines. **Trigger: sub-project 3b's first new link or NHL.**~~
   **CLOSED (DEF-50).** `spec.py:322-341` runs the arity check on both paths, above the
   early return, and raises `SpecError` naming entity, source and parent. The trigger no
   longer applies — 3b's first new link is covered on arrival.

5. ~~**Three colliding DEF numbering sequences** across the ledger and the specs — a
   reference chased from one lands on an unrelated defect elsewhere. Namespace them.~~
   **NOT A DEFECT — withdrawn 26 Aug 2026 on evidence.** An earlier revision of this line
   said "STILL LIVE, and confirmed"; that confirmation was wrong, and it was wrong in an
   instructive way: it grepped which *numbers* appear in which documents, saw `DEF-1`
   through `DEF-7` in three files, and called that a collision without ever checking what
   the numbers *mean*.

   Checked properly, **every DEF number in this repo has exactly one definition site**, and
   the three documents own disjoint contiguous ranges:

   | range | owning document |
   |---|---|
   | `DEF-1`-`DEF-7` | `specs/2026-08-24-usnc-tds-retarget-design.md` |
   | `DEF-10`-`DEF-28` | `specs/2026-08-24-subproject3a-gp-journals-design.md` (`DEF-11` retired — see below) |
   | `DEF-39`-`DEF-57` | this file |

   `DEF-8`, `DEF-9` and `DEF-29`-`DEF-38` are unused. The appearances in a *third* document
   are cross-references, not redefinitions — `subproject3a-design.md:803` and
   `factory.py:737` both cite `DEF-1` for the leading-digit catalog name, which is exactly
   what `retarget-design.md:220` defines it as. So it is one global sequence partitioned by
   the order the work happened, and a reference chased from any of ~470 call sites lands on
   the right defect. Prefixing would rewrite all of them, and invalidate the bare
   references already in commit subjects (`250c140`, `f5a95c1`), to fix nothing.

   **What WAS real, and is the inverse problem — FIXED 26 Aug 2026:** one defect carried
   two numbers. `DEF-11` (the sub-project 3a plan and its design doc, five call sites)
   and `DEF-43` (this file) were both the world-writable bundle root. Collapsed onto
   `DEF-43`, which holds the measurement and the written admin request; the plan's three
   call sites now cite it. **`DEF-11` is retired and must not be reused** — `DEF-8`, `DEF-9` and
   `DEF-29`-`DEF-38` are free, `DEF-11` is not.

**Minor:** ~~the check that the cast loop sits between `dropDuplicates` and
`_system_columns` was never added, and placement is load-bearing.~~ **CLOSED 26 Aug 2026 —
this was the last live item in the section.** Four checks now pin the ordering in
`tests/test_accelerator.py`, asserted on the source text of `factory._stage_full` because
the failure mode is an ordering one and no offline DataFrame fake distinguishes the
arrangements — which is exactly why it went unasserted for so long.

Three positions, one order, each with its own failure: hoisted above `dropDuplicates` a
cast can collapse rows the source distinguishes, and dedup then drops a row nothing has
seen the original of; sunk below `_system_columns` the system columns are stamped on
pre-coercion rows; sunk below the hashdiff the cast stops doing the only job it exists for,
since a DOUBLE's string rendering is not stable across loads and the satellite gains a
spurious version every run. A fourth check asserts the four anchor strings are still
present, so a rename cannot turn the other three into vacuous passes.

**Proven to fail before being believed**, per this repo's standing rule. Renaming the cast
anchor fails all four; hoisting the loop above `dropDuplicates` fails exactly the AFTER
check; sinking it to the end of the function fails exactly the two BEFORE checks.
`src/accelerator/factory.py` was restored byte-identical afterwards, so no digest moved and
gate zero did not need re-running.
~~The clustering test asserts against a function returning a literal `[]` and cannot
fail.~~ **CLOSED (DEF-44)** — `tests/test_accelerator.py:2611` records the swap; the checks
now exercise `factory._cluster_refusal()`, which returns real refusal strings, and
`:2642` asserts the refusal has *stopped* firing on `load_dts` now that the tables are
narrow.

---

## Decisions still with the business

- **Mask group mapping.** ~~`hfig_commercials_reader` etc. do not exist~~ **HALF CLOSED
  25 Aug 2026 (DEF-45): the MASK functions are remapped and live.** `hfig_data_engineering`
  in the GRANT half is still a placeholder. Running governance unmapped would revoke on the shared bronze catalogue and
  leave nobody able to read it. Held at `DEPLOY.md` Phase 6STOP.

  **THE PHASE 6STOP QUESTION IS ANSWERED — 26 Aug 2026.** The question was whether the
  vault schemas should be consumer-readable at all, given that a column mask on a streaming
  table is defeated by schema-level `SELECT` over its unmasked `__materialization_*` twin.

  > **The raw vault is not consumer-readable. The business vault may be, but only through
  > the gold catalog — never directly.**

  So consumers hold no grant on either vault schema. Direct readers of `raw_vault` and
  `business_vault` are the pipeline principal and a privileged group, and anything a
  consumer sees from the business vault is mediated by an object in the gold catalog.

  **IMPLEMENTED 26 Aug 2026, with one half deferred for lack of a gold catalog.**

  Measured first, and it changed the shape of the work: `vault_reader_group` already
  resolved to `us_tds_data_engineer` — a privileged group, not consumers — and DEF-40 had
  already removed every schema-level `SELECT`. So the posture was **already correct, and
  held for the wrong reason**: by the value of a variable whose name invited the opposite.
  Setting it to a consumer group would have granted the vault away, masks included, with
  nothing objecting. What was missing was not the posture but its enforcement.

  | | |
  |---|---|
  | `var.vault_reader_group` → **`var.vault_privileged_group`** | ~20 sites. Its description now states the posture and says outright that it is not a consumer group |
  | `--reader-group` → **`--privileged-group`** | same, in `apply_governance.py` and `vault_job.yml` |
  | **`schema_grant_check.unauthorised_table_readers()`** | new. Fails the build if any principal but the privileged group holds table-level `SELECT` in either vault schema |
  | **`--allow-table-select`** | new flag, no default. `assert_no_broad_grant` passes `${var.vault_privileged_group}` |

  The new predicate is the per-TABLE counterpart to `offending()`: that one fails a grant
  too **broad**, this one a grant of the right shape held by the wrong **principal**. Both
  are needed — a per-table grant to a consumer group defeats the masks exactly as
  completely as a schema grant, because a vault grant conveys the unmasked
  `__materialization_*` twin with it. The sweep reads `information_schema.table_privileges`
  rather than the declared model, precisely so a grant on a twin is in scope. Owners need
  no allow-list entry: Unity Catalog does not record ownership as a grant.

  Omitting the flag reports NOT EVALUATED rather than passing, and the flag has no default
  — a default would be a guess at which group is privileged in this lake, and a wrong guess
  allow-lists the grant the gate exists to catch.

  **DEFERRED, and it is the actual blocker on the second half: there is no gold catalog.**
  `var.gold_catalog` names `03_usnc_gold_edm_dev`, which does not exist, and there are no
  gold objects for the business vault to be read through. `apply_masks.sql` records what to
  add when it is created, and now also records what that does *not* license — analysts get
  `SELECT` on the gold catalog, never a grant extended back onto a vault schema.

  **RUN LIVE 26 Aug 2026, and it passes.** Deployed to `usnc_tds` (preflight passed;
  `bundle validate` clean but for the known DEF-43 bundle-root warning; 664 files,
  `jobs.vault_load` updated), then `assert_no_broad_grant` was run as a **partial run of
  that task alone** — one task TERMINATED SUCCESS, the other fourteen SKIPPED. That works
  because this gate depends on append-only, loop-1 and the aggregate check, but **not** on
  `assert_journal_integrity`, so it can be exercised without waiting on the admin group fix
  that has the journal gate red.

  ```
  checked CATALOG 02_usnc_silver_edm_dev: 2 assignment(s) -- ['CREATE SCHEMA', 'USE CATALOG']
  checked SCHEMA  ...raw_vault:      1 assignment(s) -- ['USE SCHEMA']
  checked TABLES in ...raw_vault:      24 table-level assignment(s) over 24 table(s)
  checked SCHEMA  ...business_vault: 1 assignment(s) -- ['USE SCHEMA']
  checked TABLES in ...business_vault:  2 table-level assignment(s) over  2 table(s)
  checked CATALOG 02_usnc_silver_edm_dev isolation: ISOLATED
  GATE SUMMARY :: schema_grant :: status=PASSED asserted=7 not_evaluated=0
  ```

  **So the posture is confirmed, not merely asserted:** no principal other than
  `us_tds_data_engineer` holds a read grant on any of the 26 vault tables, and neither vault
  schema nor the catalog carries a schema- or catalog-level read grant. The catalog is also
  confirmed `ISOLATED`, which DEF-47 had left as an assertion rather than a measurement.

  **And it is not a vacuous pass**, which matters in a repo that has shipped five checks
  that could never fail. Three independent signs: `not_evaluated=0`, so the flag reached the
  sweep for both schemas rather than being skipped; the sweep read 24 and 2 real assignments
  rather than zero; and the predicate was mutation-proven offline in eight directions before
  it ever ran. Had the flag not been wired through, the summary would have said
  `not_evaluated=2` and the run would still have passed — which is the exact failure this
  reports its way out of.

  This also settles the materialization exposure recorded under the reload section: with no
  consumer grant on either vault schema, the `__materialization_*` twins are unreachable by
  the group the mask exists to stop, which is the only durable fix — the twin names embed a
  per-update id, so no static grant can deny them individually.
- ~~**Hub deduplication.**~~ **CLOSED 25 Aug 2026 (DEF-42)** -- it did get the same
  out-of-pipeline answer as change detection: a staging log and a batch anti-join.
- **Clustering.** Hash keys are `BINARY` and cannot be clustered; the load timestamp falls
  outside the statistics window, so the Data Vault access path is unindexed.

  **DECIDED 25 Aug 2026: hash keys must be BINARY(32) as stored.** That removes the second
  option — `BINARY_OUTPUT = False` would keep SHA-256 but store a 64-character hex string,
  which is now ruled out. ~~The only remaining route is raising
  `delta.dataSkippingNumIndexedCols`~~ -- **SUPERSEDED 25 Aug 2026 (DEF-44): no property
  change is needed any more.** DEF-24 measured `load_dts` as outside the statistics window
  because a GP journal table was 85 columns wide. DEF-26's projection and DEF-41's
  narrowing took those tables to 11-24 columns, so `load_dts` now sits at position 7 of 11
  on a hub and 20 of 24 on an NHL -- inside the 32-column window on every loading table,
  asserted in the suite. Clustering on it is simply available. Whether to take it is still
  a decision -- it is a layout hint no gate reads -- but it is no longer blocked.

  **TAKEN 26 Aug 2026: cluster on `load_dts`.** Implemented in three places, because three
  different things create vault tables and only one of them is the factory:

  | creator | tables | how |
  |---|---|---|
  | `factory._cluster_candidates` | `lnk_*`, `nhl_*`, **and every `stg_*`** | `cluster_by=` on `create_streaming_table` |
  | `checks/load_hubs.py` | `hub_*` | `CLUSTER BY` in its `CREATE TABLE` |
  | `checks/load_satellites.py` | `sat_*`, `msat_*`, `csat_*` | same |

  The staging log is clustered on purpose — both loaders anti-join and `LAG` over it in load
  order, so it is on the access path as much as the hubs are. The `qtn_*` quarantine twins
  are deliberately NOT clustered: rejected rows are not on any join path, and their emit
  path does not consult `_cluster_by`. Both facts are asserted, and the prefix split was
  asserted only after a first attempt got it wrong in both directions — staging shares the
  factory's emit path, and the prefix is `qtn_`, not `qua_`.

  `_cluster_candidates` returns `load_dts` for **every** entity and lets
  `_cluster_refusal` decide per table, rather than pre-filtering. So if DEF-41's narrowing
  were ever undone, the refusal fires again and the suite fails rather than silently
  emitting an unclusterable column.

  **NOT a full refresh, confirmed against the Databricks docs, not assumed.** Changing
  clustering columns leaves existing data alone; only new writes and incremental `OPTIMIZE`
  use the new layout. `OPTIMIZE` is not in `append_only_check`'s `MUTATING` set, so the gate
  is unaffected either way.

  **LIVE STEP STILL OUTSTANDING, and it is ours.** Everything above reaches tables at
  CREATE time, and every vault table already exists — the loaders use
  `CREATE TABLE IF NOT EXISTS`. So the 13.2M rows already loaded are unclustered until:

  1. the bundle is redeployed and the pipeline updates, which applies `cluster_by` to the
     streaming tables (`lnk_*`, `nhl_*`, `stg_*`); and
  2. an explicit `ALTER TABLE ... CLUSTER BY (load_dts)` pass runs over the loader-created
     tables, which no `IF NOT EXISTS` will touch.

  Neither reorganises existing files. `OPTIMIZE ... FULL` would, and the docs warn it can
  take hours on data never clustered on that key — a separate call, not part of this one.

  **Step 1 DONE 26 Aug 2026** — deployed to `usnc_tds`, 664 files, `jobs.vault_load`
  updated. The pipelines' resource specs were unchanged, so the clustering reaches their
  tables on the next pipeline update, not at deploy time.

  **Step 2 DONE 26 Aug 2026, and it did more than "a metadata change".**

  Seven tables altered, all previously `clusteringColumns = []` and none partitioned (both
  checked first, since a partitioned table needs `REPLACE PARTITIONED BY WITH CLUSTER BY`
  instead): the six hubs and `sat_job_request_details_bullhorn_eu`. All seven now report
  `["load_dts"]`.

  **What was NOT altered, deliberately.** Two things the target list would have swept up:

  * `sat_job_request_details_bullhorn_eu_v1` is a **VIEW**. `ALTER TABLE` on it fails, and
    clustering a view is meaningless — it stores nothing.
  * `csat_job_request_custom_promoted` and `csat_payroll_line_classification` report
    `table_type = STREAMING_TABLE`, **not** `MANAGED`. They are pipeline-owned, so an
    `ALTER` against them would not survive the next update — the same lesson `apply_masks.sql`
    already records for `ALTER TABLE ... SET MASK`. They must get clustering from the
    pipeline definition instead. **This is also a discrepancy worth chasing:**
    `checks/load_satellites.py --layer business` is supposed to create the `csat_*` tables
    with `CREATE TABLE IF NOT EXISTS`, which would make them MANAGED. That they are
    streaming tables means they were created by the `business_vault` pipeline, presumably
    before DEF-52 moved change detection out of the pipeline. Their clustering is unresolved
    until that is settled.

  **THE COST, which was not advertised in advance and should have been.** The docs say
  changing clustering columns does not rewrite data, and that held — but enabling clustering
  on a table that never had it also **upgrades the Delta protocol**. Each table took three
  commits, not one:

  ```
  v6  UPGRADE PROTOCOL
  v7  ROW TRACKING BACKFILL
  v8  CLUSTER BY
  ```

  All seven went from their original protocol to **reader 3 / writer 7**, gaining the
  `clustering`, `rowTracking`, `deletionVectors`, `domainMetadata` and `v2Checkpoint`
  features, with `delta.enableRowTracking` and `delta.enableDeletionVectors` both now
  `true`. A Delta protocol upgrade is **not cleanly reversible**. Nothing in this estate
  reads these tables with an old enough client to care, but it is a bigger change than "a
  layout hint no gate reads", and the same will happen to the streaming tables when the
  pipeline applies `cluster_by` to them.

  **What was verified after, rather than assumed:**

  | check | result |
  |---|---|
  | `clusteringColumns` | `["load_dts"]` on all 7 |
  | `delta.appendOnly` | still `true` on all 7 — deletion vectors are a capability the table still refuses to use |
  | `DESCRIBE HISTORY` vs `append_only_check`'s `MUTATING` set | **no** UPDATE / DELETE / MERGE / TRUNCATE / RESTORE on any of the 7 |
  | row counts | `sat_job_request_details_bullhorn_eu` = **46,889**, matching the figure recorded before any of this work — the strongest single sign nothing moved |

  **Follow-up:** `BINARY_OUTPUT` in `src/accelerator/hashing.py` is currently a plain
  default with an explanatory comment, unlike `ALGORITHM` and `HASHDIFF_UPPERCASE`, which
  each carry a `*_RATIFIED` twin and an import-time guard. This decision makes it
  load-bearing, so it should gain the same treatment. Adding the guard changes no hash
  output — it only stops a future change happening without a `RULEBOOK_VERSION` bump. Do
  it after the reload lands rather than editing the rulebook while a load is in flight.

## Workspace prerequisites — CORRECTED, and mostly ours all along

**26 Aug 2026 (DEF-48).** This entry said `ctl_approval_manifest` and
`ref_dq_expectation` were "platform objects, neither created by this repo". Both halves
were wrong. `02_usnc_silver_edm_dev.governance` is **owned by this team**, created by our
own `create_mask_functions` task, and already held the two pipeline event logs. A request
had been drafted asking another team to create a table in a schema we own — caught before
it was sent, by the question "but I thought I have access to the governance schema".

**Both tables now exist** (`governance/control_objects.sql`).

### Creating the manifest empty would have been worse than leaving it absent

`loop1` drives its comparison FROM that table. With no rows: every variance query returns
nothing, `reconciled` still increments per table, and the gate prints **PASSED** having
compared nothing. That would have been the sixth check-that-cannot-fail in this project,
and self-inflicted. Two guards now prevent it, both verified live:

- an **empty manifest FAILS**, naming why;
- rows whose `manifest_id` is NULL — or names a manifest the control does not hold — are
  **reported as unaccounted**. They were previously invisible, because the variance query
  drives from the manifest: such rows are not a variance, they are never compared.

### `approved_count` was never actually open

The job has always passed `--entity` naming the three NHLs, so the gate has only ever run
where the identity holds. But that was a **convention in a parameter list, not a
property**: with `--entity` omitted, `targets()` returned every table including hubs, and
the gate would have reported false variances in the millions on correct data.
`RECONCILABLE_KINDS` now enforces it — a deduplicating kind is excluded from the default
set and refused if named.

### What is genuinely still external

**No active bronze feed stamps a `manifest_id`.** `nhl_general_journal_line` holds
2,453,132 rows and zero non-null ids. Four feeds need it, eleven other bindings already
declare the convention, and it was never part of BRZ-1..11. Request:
`docs/loop1_control_table_request.md`.

---

## Next after the reload: narrow the hub column set

**Decided 25 Aug 2026.** The projection fix takes `hub_accounting_journal` from 92 columns
to 13 — hash key, 4 business key columns, a readable `_bk`, and 7 system columns. That is
the difference between wrong and defensible, but it is not yet strict Data Vault.

Canonical DV 2.0 says a hub holds the **hash key, the business key, load date and record
source**. This repo's `naming.SYSTEM_COLUMNS` is wider, and applies the same set to every
kind. On a hub, three of them are arguable:

- **`cdc_op`** — a hub row asserts that a business key exists. There is no update or delete
  semantics for that assertion, so an operation flag has nothing to say.
- **`sub_seq`** — orders intra-batch satellite versions. A hub has one row per key and no
  versions.
- **`applied_dts`** — business effectivity, which belongs on satellites.

Keep: hash key, business keys, `_bk`, `load_dts`, `rec_src`, and `manifest_id` — the last
because `checks/loop1_reconciliation.py` counts landed and quarantined rows per manifest,
so removing it would break a hard gate.

**Do this after the reload has landed and been verified**, not before: narrowing is another
schema change and therefore another drop-and-reload. Doing both at once would mean a failed
hash comparison could not be attributed to either.

The same question applies to links and NHLs and should be answered in the same change — a
link has no versions either, so `sub_seq` is equally questionable there.


---

## DEF-39 — the reload of 25 Aug failed, and what it cost

**Fixed at `fb38ea1`.** The first drop-and-reload dropped the seven vault tables and
their seven quarantine twins, then failed at graph analysis before writing a single row.
The vault sat empty until the fix landed.

**The defect.** The quarantine flow built its two reason columns from the expectation
SQL — which may name any source column — and added them with `withColumn` *after*
narrowing the frame to the declared shape. Spark resolves such an expression against the
projection's output, where those names no longer exist:

    AnalysisException: name 'input_db' cannot be resolved

20 occurrences, 24 flows dead, no table created. DEF-26's projection was correct; only
the ordering around it was wrong. `_project` now takes an `extra` parameter so the reason
columns are aliased inside the same `select()`, against the staged frame.

**Why 371 green checks missed it.** No test had ever *called* a quarantine flow body, and
the fake DataFrame swallowed `withColumn` whole. This is the same shape as the four
checks-that-could-never-fail found earlier in this project: the assertion existed, the
thing it asserted was never exercised. Both halves are fixed — the harness now captures
flow bodies and records which frame a column was added to, and the new checks run every
quarantine body in the model. Verified to fail on the pre-fix code and pass on the fix.

### Three operational facts the failure surfaced

1. **`DROP TABLE` on a pipeline-owned streaming table does not cascade to its
   `__materialization_*` backing table.** Each had to be dropped by name. Dropping only
   the streaming tables would have stranded cleartext with no masked object above it.
2. **A table dropped outside its pipeline needs `--full-refresh-all` to come back
   populated** — a normal update resumes from a checkpoint that no longer matches
   anything.
3. **The only record of the pre-drop vault was gitignored scratch.** It is now committed
   at `docs/superpowers/evidence/2026-08-25-pre-reload-vault-state.json`, with the
   comparison tool beside it. Before any future drop, capture the state *into git first*.

### Standing rule this establishes

Run `databricks bundle run <pipeline> --validate-only` before any drop. It resolves the
whole graph in about ninety seconds and would have caught this defect while the vault was
still populated, turning a four-hour outage into a failed check.


---

## `--full-refresh-all` can never work on this pipeline

Established 25 Aug 2026, during the reload.

The vault tables carry `delta.appendOnly = true` — that is not incidental, it is the
property `checks/append_only_check.py` exists to enforce. A full refresh RESETS a table,
which is a truncate, which Delta refuses:

    [DELTA_CANNOT_MODIFY_APPEND_ONLY] This table is configured to only allow appends.

`--full-refresh-all` targets *every* dataset in the graph, so it fails on the first
append-only table it reaches regardless of which tables you actually meant to rebuild.
In this case it died on `sat_job_request_details_bullhorn_eu`, a ghost-only table that
had never been dropped and had no business being touched.

**Use `--full-refresh <names>` naming only the tables being rebuilt.** When those tables
have been dropped there is nothing to truncate, so the append-only property is never
contradicted and the refresh does what it says.

The general form of the constraint: **an append-only vault cannot be reset in place.**
Rebuilding a vault table means dropping it — and dropping it means dropping its
`__materialization_*` twin by name too, since the drop does not cascade. There is no
"reset and reload" path that keeps the table.


---

## The reload landed. What it proved, and what it did not.

25 Aug 2026, update `87ff2c92`. Raw vault reloaded at the projected shape; business vault
COMPLETED. Full comparison in `evidence/2026-08-25-reload-parity-result.txt`.

### The projection

| table | before | after |
|---|---:|---:|
| `hub_accounting_journal` | 92 | **13** |
| `hub_organisation` | 92 | **11** |
| `hub_ledger_account` | 62 | **11** |
| `hub_pay_period` | 22 | **11** |
| `nhl_general_journal_line` | 83 | **26** |
| `nhl_general_journal_line_closed_year` | 83 | **26** |
| `nhl_journal_line` | 25 | **17** |

The quarantine twins narrowed with them, each landing at its target plus exactly two
columns. The sixteen ghost-only tables were untouched at 8 columns.

### Row counts and hash values

**All 30 tables returned exactly their pre-drop row count.** Not one differed.

Of the 17 hash-key columns, **7 are byte-identical including the ghost row** — every hub
key, and every NHL's own key. The other 10 are all parent hash keys on NHL tables, and
they differ in one way only: `n_distinct` +1, `min` now the zero key, and `sum(crc32)` up
by exactly **884,073,675**, which is `crc32` of the zero key, in all ten. Excluding the
single ghost row, **all ten reconcile to the pre-drop capture exactly**.

The cause is not a mystery and not a regression: `_ghost_column_sql`, with its rule that
every column ending `_hk` gets the zero key, **was introduced in `75bd524`** — the DEF-26
projection commit. The pre-drop load predates it, so its ghost rows carried NULL parent
keys. The new behaviour is the documented intent: it is what makes a PIT join to the
parent hub an equi-join rather than an outer one.

**The reload is verified correct.** The vault holds the same rows under the same keys, at
the declared shape.

### Three gates fail, all on pre-existing open items

`assert_aggregate_reconciliation` **SUCCESS**. `apply_governance` correctly did not run.

- **`assert_append_only`** — 538,186 duplicate rows in `hub_accounting_journal` at grain
  `(accounting_journal_hk)`, 11 in `hub_organisation`. 538,186 is exactly
  2,759,294 − 2,221,108, the known duplicate-key figure, identical before and after. This
  is **hub deduplication**, still open, and it is the same platform constraint as ever: a
  streaming table cannot read its own contents.
- **`assert_journal_integrity`** — every money column reads NULL to the gate's identity.
  The gate names the cause itself: the run-as identity is not privileged under
  `mask_money`. This is the **materialization-grant question**, unresolved.
- **`reconcile_loop1`** — `governance.ctl_approval_manifest` does not exist. Still blocked
  on the two absent workspace prerequisites.

### The mask leak is unchanged and still real

    masked streaming table   0 readable
    __materialization_* twin 2,453,131 readable, cleartext

Twelve objects in `raw_vault` still carry money columns. The narrowing did not fix this
and was never going to: the backing table is created by the runtime, not by the generator,
and carries no mask. This is the single most important open item in the project.


---

## DEF-40 CLOSED: the materialization bypass

**Fixed 25 Aug 2026, commits `087b48c` and follow-up.** Spec:
`specs/2026-08-25-materialization-grant-design.md`.

SELECT is now granted **per table**, generated from the declared model, and
`checks/schema_grant_check.py` fails the build if any catalog- or schema-level SELECT
appears beside it. A backing table is never declared, so it can never enter the grant
list -- structural, not vigilance. The gate runs before `apply_governance`, and passed
live on 25 Aug reading 2 catalog assignments and 0 on each vault schema.

### Still open, and deliberately so

- **`apply_governance` remains under its STOP.** The groups it grants to do not exist
  in this workspace, and its unconditional REVOKE on the shared bronze catalog would
  land while the GRANT failed. The grant half is now correct; the file is still not safe
  to run. That is the mask-group-mapping decision, unchanged.
- **`assert_journal_integrity` stays red.** Its run-as identity is not privileged under
  `mask_money`, so it reads NULLs. Per DEPLOY.md 6b that identity must be a member of
  every mask's privileged group -- an account-admin action.
- **One live test could not be run.** Firing the gate in the failing direction needs a
  real `GRANT SELECT ON SCHEMA`, which was refused as a live permission change. The
  predicate is proven offline in both directions (7 checks), and `grants()` is proven to
  parse real rows, so the untested seam is the two lines joining them.

### Separately noticed, not fixed

`databricks bundle validate` warns that the bundle root `/Workspace/Shared/.bundle/...`
is **writable by all workspace users**. That is a different exposure from this one --
it concerns the deployed code rather than the data -- and it is not addressed here.


---

## DEF-41 CLOSED: hubs, links and NHLs narrowed

25 Aug 2026. Verified: **RELOAD PARITY PASSED — 17 of 17 hash-key columns byte-identical**
to the pre-narrowing capture, and every row count unchanged.

| table | before today | after DEF-26 | after DEF-41 |
|---|---:|---:|---:|
| `hub_accounting_journal` | 92 | 13 | **11** |
| `hub_organisation` | 92 | 11 | **9** |
| `hub_ledger_account` | 62 | 11 | **9** |
| `hub_pay_period` | 22 | 11 | **9** |
| `nhl_general_journal_line` | 83 | 26 | **24** |
| `nhl_general_journal_line_closed_year` | 83 | 26 | **24** |
| `nhl_journal_line` | 25 | 17 | **15** |
| ghost-only hubs/links/NHLs | 8 | 8 | **6** |

Each quarantine twin is exactly its target plus the two reason columns: 13/26/26/17/11/11/11.
Satellites are untouched at 8 -- they keep `sub_seq` (their uniqueness grain) and `cdc_op`
(what `change_detection: cdc` filters on).

A hub now holds its hash key, its business keys, a readable `_bk`, and five system
columns: `load_dts`, `applied_dts`, `rec_src`, `batch_id`, `manifest_id`.

### It took two attempts, and the reason is worth keeping

The first reload narrowed the **masked** NHLs correctly and silently failed on the
**unmasked** hubs, which came back with their 11 correct columns plus `sub_seq` and
`cdc_op` appended at positions 12 and 13.

An unmasked table declares no schema, so SDP infers it from the **union of its flows** --
and the ghost flow's system columns were a second, hand-written definition of the set,
still supplying both. The quarantine twins were all correct, which is what pinned the
cause: they have no ghost flow. There is one definition again
(`factory._ghost_system_exprs`), and the ghost's columns are asserted equal to the
projection's, kind for kind.

### Two operational notes

- **A failed full refresh auto-retries the same request**, repeatedly. During the second
  attempt it kept recreating tables between the drop and the refresh, and the loop had to
  be broken with `databricks pipelines stop` before the rebuild could proceed.
- **`while read` skips a final line with no trailing newline.** One table survived a drop
  list that way, and the refresh then failed on it as append-only. Terminate generated
  lists.


---

## DEF-42 CLOSED: hub deduplication

25 Aug 2026. `append_only_check` **PASSES** — 29 vault tables, zero mutating operations.
It had been failing on exactly this since the gate was repaired.

| hub | before | after | |
|---|---:|---:|---|
| `hub_accounting_journal` | 2,759,294 rows / 2,221,108 keys | **2,221,108 / 2,221,108** | 538,186 duplicates gone |
| `hub_organisation` | 24 / 12 | **12 / 12** | |
| `hub_ledger_account` | 1,906,877 / 1,906,877 | 1,906,877 / 1,906,877 | already unique |
| `hub_pay_period`, `hub_job_request`, `hub_worker` | unchanged | unchanged | |

Every hub's row count now equals its distinct key count. The after-counts are exactly the
before-distinct-counts, which is the assertion that matters: nothing was lost, only the
duplication.

### Verified, not assumed

- **Idempotent.** A second run of `load_hubs` left every count unchanged.
- **Strictly append-only.** `DESCRIBE HISTORY hub_accounting_journal` shows
  `CREATE TABLE AS SELECT` once and `WRITE` twice. No `UPDATE`, `DELETE`, `MERGE`,
  `TRUNCATE` or overwrite mode — so the gate keeps its exact previous meaning rather than
  having been relaxed to accommodate hubs.
- **Typed as intended.** The six hubs are `MANAGED`; the six logs are `STREAMING_TABLE`.

### The shape of the answer

The duplicates were never a bug in the sources or the keys. A hub is a conformed
identity, so several sources legitimately supply the same key — and the load needs
insert-if-not-exists across flows, which no streaming flow can do because a streaming
table cannot read its own contents.

That is the same constraint behind three earlier problems in this project. The other two
were solved by delegating to the source and by splitting into two entities. This one is
solved by moving the lookup to where a lookup is possible: the pipeline appends to
`stg_hub_x`, and a batch task inserts into `hub_x` only the keys not already there. It is
the classical Data Vault hub loader, and it was available the whole time.

### The guard that keeps links and NHLs honest

Every link and NHL is single-source, so they cannot duplicate and are written directly.
`spec.validate` now refuses a multi-source link or NHL, naming this decision — and
emptying `naming.STAGED_KINDS` makes the model fail to load rather than silently
reverting hubs to direct writes.


---

## DEF-43 OPEN: the bundle root is world-writable

**Also numbered DEF-11**, in `plans/2026-08-24-subproject3a-gp-journals.md`, which raised it
first as a `bundle validate --strict` warning before it was measured. One defect, two
numbers — collapsed 26 Aug 2026 onto DEF-43, which is the entry holding the evidence and
the admin request. All five call sites now cite DEF-43; DEF-11 is retired and must
not be reused.

```
/Shared/.bundle/hfig-dv-accelerator/usnc_tds
    users     CAN_MANAGE   inherited = True
    admins    CAN_MANAGE   inherited = True
```

The bundle root holds `src/pipelines/silver_vault.py` and everything in `checks/`, and the
pipeline **executes that code** as its run-as identity. So any workspace user can change
what the vault runs. That is a shorter route into the masked financial data than the one
DEF-40 closed — it does not defeat the masks, it goes around them.

### Two things that look like fixes and are not

- **Declaring bundle `permissions:`.** The `users` grant is INHERITED from `/Shared` and
  cannot be revoked on a child; declaring another group only ADDS a holder. Worse, applied
  to the resources it would have **widened** `vault_load`'s ACL, which is currently just
  its owner plus `admins` and is fine as it stands.
- **Making `root_path` a variable and letting the CLI stop resolving it to
  `/Workspace/Shared`.** That silenced the validate warning while changing nothing about
  the ACL. The default is deliberately `/Workspace/Shared` rather than `/Shared` so the
  warning keeps firing until the exposure is actually gone.

### What is in place

`databricks.yml` reads the location from `bundle_root_prefix`, so when the restricted
folder exists this is a one-line change that moves all eight targets. Nothing else about
the deployment changes.

**This must not reach PROD.** The same bundle deploys to production workspaces, where the
run-as identity is a service principal with production catalog rights.


---

## DEF-44 CLOSED: three gate-integrity gaps

25 Aug 2026. All three came from the final whole-branch review and had been carried as
Important-unfixed.

### The business vault was invisible to two gates

`assert_append_only` and `assert_mask_survival` each took a single `--schema`, and the job
passed only the raw vault. So the business vault's streaming tables and their
`__materialization_*` twins were **never checked for a mutating operation** -- not a
weaker assertion, none at all, on half the estate, under a green gate.

`mask_survival` walks **every** entity in the model, and the computed satellites live in
the business vault -- so `csat_payroll_line_classification`'s two masks were being looked
for in a schema that cannot contain them, and would have reported MISSING the day its
binding activates. Its catalogue-wide sweep had the same blind spot: one schema swept,
"masked everywhere" concluded.

Both take `--schema` repeatably now and the job passes both. `mask_survival` also asserts
the two schemas' table sets are **disjoint**, because it keys its sweep by table name and
a collision would let a mask on one vouch for a same-named table in the other.

### Journal integrity did not block governance

Three of the four post-load gates gated `apply_governance`; the accounting one did not,
because it was added after the dependency list was written. A run with debits != credits
would still have applied grants and published metadata. It is in the list now.

### A fifth check that could never fail

The clustering guard filtered `factory._cluster_by()`, which returns a literal `[]`, so
the list was always empty and the assertion could not fire. The refusal is now its own
function (`_cluster_refusal`) returning a reason or `None`, fired directly in both
directions -- and it produced the finding above: **`load_dts` is now clusterable.**


---

## DEF-45: the mask functions name real groups now

Applied and live, 25 Aug 2026.

| function | was | is |
|---|---|---|
| `mask_money` | `hfig_commercials_reader` | `usnc_data_analyst_finance` |
| `mask_personal_name` | `hfig_worker_pii_reader` | `pii_cleared_us` |
| `mask_tax_reference` | `hfig_worker_pii_reader` | `pii_cleared_us` |
| `mask_tokenised_account` | `hfig_paybill_privileged` | `pii_cleared_us` |

Each also admits `global_dataplatform_pipeline_job_runners`, because mask functions
evaluate with the **run-as identity's** rights during a refresh: an unprivileged pipeline
does not merely hide values from itself, a downstream materialized view **materialises the
NULLs into the vault as fact**, and silver is insert-only. That group was chosen over
`data_platform_operations` deliberately — platform engineers do not need cleartext PII to
operate the platform, the job identity does need it to load.

**Verified these resolve.** `is_account_group_member` returned true for
`data_platform_operations` and `us_tds_data_engineer` for the calling identity and false
for the two it is not in, so these are account groups and not workspace-local ones. A
workspace-local group would have returned false for everyone and the masks would have
stayed shut with no error anywhere. Post-change, `nhl_general_journal_line` reads
2,453,132 rows and 0 readable `debitamt` to an identity in neither group — the control
behaving exactly as intended.

### Still blocking `apply_governance`, and both are separate

1. **Nobody is in `global_dataplatform_pipeline_job_runners`.** Until an admin adds the
   TDS operator and the PROD service principal, `assert_journal_integrity` keeps reading
   NULLs and keeps failing — and it now gates `apply_governance` (DEF-44). Asked for in
   `docs/workspace_admin_request.md`.
2. **The GRANT half still names `hfig_data_engineering`,** and `apply_masks.sql` issues an
   unconditional `REVOKE ALL PRIVILEGES` on the **shared bronze catalog**. Running it
   today would revoke and then fail the grant, leaving nobody able to read Bronze. This
   one is ours, not the admin team's, and is the remaining reason for the Phase 6STOP.


---

## DEF-46/47 CLOSED: the Phase 6 STOP is lifted

**`GOVERNANCE APPLIED: 48 statement(s)`** — 26 Aug 2026. `apply_governance` had never once
been run. It has now, cleanly, and the grants are live.

### What was wrong, and what it cost to find out

The file had two defects, and both were invisible precisely because it had never run:

- **It governed a catalog it does not own.** `REVOKE ALL PRIVILEGES` on the shared bronze
  catalog plus a grant to `hfig_data_engineering`, a group that exists nowhere in this
  estate. Measured: bronze carries exactly one grant, `scope_tds_full_scopes_write`, and
  no `account users` grant at all — so the REVOKE had nothing to revoke and the posture it
  claimed to restore was already in place by another team's hand.
- **`ALTER CATALOG ... SET ISOLATION MODE` is not valid on this runtime.** It was the one
  statement of 49 that failed, with `PARSE_SYNTAX_ERROR`. The comment directly above it
  had warned that this DDL "has changed shape across releases" and told the reader to
  verify it in their workspace. Nobody had, because nobody had run the file.

Both catalogs turn out to be Terraform-managed — `DESCRIBE CATALOG EXTENDED` reports the
silver catalog's comment as literally "Managed by Terraform". So neither was ours to
write to, and both are now **asserted** rather than enforced:

    schema_grant_check --assert-not-world-readable 01_usnc_bronze_dev
    schema_grant_check --assert-isolated          02_usnc_silver_edm_dev

A run that cannot read either fails, because "we could not check" and "it is fine" must
never look the same.

### DEF-40 proven in practice, not just in the plan

| object | `us_tds_data_engineer` holds |
|---|---|
| `raw_vault` schema | `USE_SCHEMA` — and no SELECT |
| `hub_accounting_journal` | `SELECT` |
| its `__materialization_*` twin | **nothing** |

That is the whole design working live: readers reach the declared tables, and the backing
tables that carry no mask are reachable by nobody but their owner.

### Still open

`assert_journal_integrity` remains red until an admin puts the run-as identity into
`global_dataplatform_pipeline_job_runners` (`docs/workspace_admin_request.md`). Since
DEF-44 that gate blocks `apply_governance` in the job graph — so a full job run still
stops there, deliberately. Governance was applied here by running the task directly.


---

## BRZ-2 withdrawn: the client mapping is ours, and it has no agreed target

26 Aug 2026, after David Willson's reply. Bronze request page revised and republished.

**BRZ-2 asked for something that cannot exist.** It requested a stable, non-display buyer
identifier "alongside the existing `buyer` column", which assumed the VMS holds one and is
not sending it. It does not: the `BuyerId` that exists is generated inside the
reporting/dashboard SQL Server, not by the VMS, and within a single VMS tenant there is no
client id at all because everything in that database belongs to one client.

**Measured in `01_usnc_bronze_dev`,** which confirms his account exactly:

| | |
|---|---:|
| `fieldglass` tables | 158 |
| `beeline` | 61 |
| `vndly` | 37 |
| `fieldglass_client_owned` | 14 |

One table per client — `io_distributed_jobposting_air_liquide`, `_ameren`,
`_duke_energy` — and within each, `buyer` holds exactly **one constant value**
("Air Liquide", "Ameren", "Duke Energy"). So the client identity is present twice, in the
table name and in that constant. What is absent is a *stable code* surviving a rename, and
no third party can supply that because it is our notion of a client, not theirs.

### What replaces it

HFIG maintains **feed identity → stable HFIG client code → finance client**, roughly 140
feeds across three VMSs. The vault already supports a per-binding literal key part — the
mechanism `reference_type: Portal_Client_Code` uses today — so once the mapping exists the
bindings can be generated from it rather than hand-written.

### Two things this leaves open

1. **There is no known client master to tie back to.** Raised with David 26 Aug. Until
   answered, the mapping has a left-hand side and no agreed right-hand side, and the
   tie-back to finance that motivated the whole question is unresolved.
2. **The Fieldglass bindings cannot work as written.** `job_request_details/FIELDGLASS_EU`
   and `job_request_custom_field/FIELDGLASS_EU` key on `('buyer_code', 'posting_id')`, but
   the feed has **`buyer`**, not `buyer_code` — and it is a display name. Both bindings are
   inactive in this lake so nothing is loading wrong today. **Not corrected on purpose:**
   the right key depends on the mapping above, and guessing it now would bake in a second
   wrong answer. Correct them when the client code exists.

Also of note for anyone unioning these later: the per-client schemas drift (54 columns vs
52 on the ones sampled), so a union view needs an explicit column list, never `SELECT *`.


---

## DEF-49/50 CLOSED: two guards that existed but could not fire

26 Aug 2026. Both were carried as Important-unfixed from the whole-branch review.

**`BINARY_OUTPUT` now has a `_RATIFIED` twin and an import-time guard.** It was the one
storage decision without one, and the 25 Aug decision that hash keys are BINARY(32) as
stored made it load-bearing. It is the most dangerous of the three because flipping it
changes **nothing** about the digest — `UNHEX` wraps the same SHA2 output and the golden
vectors compare hex, so `hash_parity_check` would still PASS while every stored key became
a 64-character STRING and every join to an already-loaded key stopped matching. The guard
changes no output: `RULEBOOK_VERSION` is untouched and all 14 vectors reproduce.

**The parent-key arity check now runs on both paths.** It sat three lines below an early
return, so it only ever ran for a hub declaring a literal somewhere — two of six.
`hub_job_request`, `hub_ledger_account`, `hub_pay_period` and `hub_worker` took the return
and were never counted. A miscount is silent in the worst way: the child's parent key
becomes a well-formed value that never equals the hub's own, and no gate catches it —
`append_only` checks uniqueness, not joinability, and a PIT join simply returns nothing.

Both verified by breaking them: flipping `BINARY_OUTPUT` raises at import, and putting the
arity check back below the return fails the suite on `hub_job_request`.

### Still open, and needing nobody

- **`tools/refresh_key_composition.py`** is blind to two identity inputs: `src.cast`,
  applied before the key hash, so casting a key column re-keys silently; and bindings
  declaring none of the tracked fields are dropped, so under `key_style: federated`
  renaming a binding is invisible.
- **Clustering on `load_dts`** is now available (DEF-44) with no table property and no
  rulebook change. A layout hint no gate reads, so it is a judgement call rather than a
  defect.
- **The client mapping's left-hand side** can be generated now — ~140 feeds with their
  constant `buyer` value already known — without waiting on the client-master answer.


---

## DEF-51 CLOSED: the key-composition digest's two blind spots

26 Aug 2026. The last of the five Important-unfixed findings from the whole-branch review.

Both were **latent, not live** — no binding in the model is empty and no key column is
cast — so nothing was wrong, and `metadata/key_composition.json` is byte-identical after
the fix. What they were is holes waiting for someone to walk into, and both are silent by
nature: `spec.validate()`'s count check, the parity gate and every reconciliation stay
green, because none of them looks at a column's type or a binding's name.

**A cast on a key-feeding column.** `factory._stage_full` applies `src.cast` at line ~351
and computes the hash key at ~359, so casting a column that feeds a key changes what gets
hashed. The cast list exists for hashdiff stability on payload columns — GP's DOUBLE
amounts — so adding one that also lands on a key is an easy accident.

Only key-feeding casts are recorded. Folding in the whole list would move the digest for a
cast on `debitamt`, which cannot re-key anything — and a digest that cries wolf stops
being read, which defeats the tool more thoroughly than the gap it closes.

**The binding name.** `if binding:` dropped any binding declaring none of `key_columns`,
`key_literals` or `parent_keys`, and with it the binding's NAME. For a federated entity
the source name is hashed *into* the key, so renaming such a binding re-keys every row it
loads while the digest stays put. Recorded unconditionally now.

### The whole-branch review's Important list is now clear

All five closed: the two gates blind to the business vault and the governance dependency
(DEF-44), the arity check four hubs never reached (DEF-50), this, and the DEF numbering
collision, which turned out to be moot once the ledgers were deleted.

### What is left, and who owns it

| | owner |
|---|---|
| Job-runner group membership, deployment folder | admin team — request sent |
| `manifest_id` on four bronze feeds | Bronze team — request sent |
| A client master to map VMS feeds to | **ANSWERED 26 Aug** — GP `RM00101`; now ours, and not 1:1 |
| Clustering on `load_dts` | ours — **DECIDED and implemented 26 Aug**; a deploy plus an ALTER pass remain |
| The client mapping's left-hand side | ours — can be generated now |


---

## Decided 26 Aug 2026: no staging retention, and staging is permanent

**No retention is built on `stg_` tables.** Measured: 4,666,199 staging rows against
4,128,001 hub rows — **13% overhead**, on tables that are the reason the hubs are correct
at all.

Building retention would have cost three moving parts to save that 13%: relaxing
`delta.appendOnly`, adding an `append_only_check` exception so the relaxation stayed
honest, and reconciling the retention window against loop-1 — which counts the log as
`landed`, so a deleted manifest could never reconcile again.

**Watch the satellite logs, not the hub ones.** A full-refresh feed writes its entire
table to the log on every run, and `bullhorn_native_raw.joborders` is 173 columns wide.
That is the case that would change this decision.

**Staging is a destination, not scaffolding.** Even once CDF lands and change detection
stops needing it, the log remains the record of what Bronze delivered — which is what
loop-1 reconciles `approved` against.

*Recorded because the alternative was briefly to delete the staging tables outright, which
would have reverted DEF-42 and returned 538,186 duplicate keys to
`hub_accounting_journal`. The 13% figure is what settled it.*


---

## ANSWERED 26 Aug: the client master is GP's `RM00101`

David Willson: *"For GP there is the Customer Master which is the RM00101 table."*

Verified present and usable: **`01_usnc_bronze_dev.great_plains_raw.rm00101`**, 889 rows
over **345 distinct `custnmbr`** across 4 `input_db` companies. VMS clients do appear in
it — `AIRLIQUIDE` / AirLiquide (GGI), `BAYCL` / Bayer Climate (BARM), `BAYP` / Bayer PR
(BARM).

So `docs/client_mapping_worksheet.csv`'s `finance_client__FILL` column now has a target:
**`custnmbr`**, and the mapping's right-hand side exists.

### Three things the sample already shows

1. **It is not one-to-one.** The Fieldglass feed `bayer` corresponds to at least *two* GP
   customers, `BAYCL` and `BAYP`. A VMS tenant can be several finance customers, so the
   mapping is feed → *set of* customers, or it needs a rule for which one a row belongs to.
   That is a modelling decision, not a lookup.
2. **`custnmbr` is company-scoped.** 889 rows over 345 distinct ids across 4 companies, so
   the key is almost certainly `(input_db, custnmbr)` — exactly the scoping
   `hub_organisation` already applies to GP journals, and for the same reason.
3. **The raw table re-delivers.** `AIRLIQUIDE` appears twice and `BAYCL` three times, which
   is the known `_raw` duplication (BRZ-3), not a data error.

### Not yet done

The worksheet's right-hand column is still blank. It could be pre-populated with *candidate*
`custnmbr` matches so a human confirms rather than searches — but point 1 means the
candidates are a set, not a value, and that needs the modelling decision first.


---

## DEF-52: the satellite loader is built. Activation is NOT.

Branch `satellite-change-detection`, 9 commits, 484 offline checks. Every satellite in this
vault was dormant because a streaming table cannot read its own contents, so the loader
could not tell a genuine change from a re-delivery. The batch loader
(`checks/load_satellites.py`) closes that, and it does NOT wait on BRZ-1.

### What the reviews caught, and where it came from

Three Critical defects reached the final whole-branch review, and **two trace to this
plan's own author**:

- `load_hubs.staged_entities()` also reads `STAGED_KINDS`. Widening that set for satellites
  made the hub loader target nine satellites with hub-shaped SQL — **the vault job would
  have died at its first batch task on every run.** The ruling to share one constant was
  right; it needed the companion "audit every consumer of the widened set", and did not
  get it.
- `_emit_v1_view` receives the pipeline target, so the type-2 view was built over the
  **staging log** — every re-delivery a version, the exact defect the branch removes. The
  spec asserted "`_emit_v1_view` needs no change". That was wrong.
- `dedup_by` on a satellite drops a re-delivered row with a changed payload **before** the
  hashdiff is computed. `factory.py`'s own comment warns this "stops being acceptable when
  satellites return". Bullhorn would have stored one version per job order for ever.

Earlier rounds caught two more in the loader's SQL: `SELECT * EXCEPT` over a join, which
could never have executed, and `LAG` over a filtered subset, which lost A → B → A.

### Blocking activation

**`sat_x_v1` does not exist.** Suppressing `_v1` for staged kinds was the right fix — the
pipeline runs before the batch loader, so a view over `sat_x` would fail at graph analysis
on a first run — but it leaves the type-2 view unbuilt. `journal_integrity_check`,
`aggregate_reconciliation_check` and `mask_survival_check` all expect it.

~~**The loader must create `sat_x_v1`, carrying the mask clauses `_mask_clauses` supplies,
before any satellite is activated.**~~ **DONE 26 Aug (DEF-53)** — and it needed no mask
clauses. `checks/load_satellites.py::v1_sql()` creates it as a **plain view** after the
load, and a plain view inherits the base table's mask: probed, both the table and a view
over it return empty to an identity the mask denies. A view also *cannot* declare one —
`CREATE VIEW (col MASK fn)` and `ALTER VIEW … SET MASK` are both syntax errors here.

That in turn required fixing `mask_survival_check`, which swept `information_schema.columns`
— view columns included — and would have flagged `sat_x_v1` as unmasked, blocking every
load once a masked satellite activated. It now exempts objects whose `table_type` is
`VIEW`, and only those: a materialized view stores a copy and still must redeclare.

### Deferred with a trigger

`insert_sql` runs a `LAG` over the whole cumulative log and a `row_number()` over the whole
satellite, every run, for all 13 satellites, with no incremental predicate. Storage is not
the problem — the log carries the ~15-column projection, not `joborders`' 173 columns — but
compute is. ~~**Re-measure at 30 runs.**~~

**MEASURED 30 AUG 2026 — no optimisation needed yet, and the trigger was wrong.**

76 job runs against a threshold of 30, so the trigger fired. Four full runs carry audit rows
(27–30 Aug); the numbers are flat in every dimension:

| | 27 Aug | 29 Aug | 29 Aug | 30 Aug |
|---|---|---|---|---|
| `load_satellites` | — | 27s | 26s | 28s → 27s |
| `load_hubs` | 94s | 122s | 119s | 123s |
| `staged` | 4,711,715 | 4,780,930 | 4,780,930 | 4,780,930 |
| `accepted` | 0 | 7 | 0 | 0 |

The re-scan is real and almost entirely wasted — 4.78M rows read to insert 0 — but it is
**not growing**, and it costs 27 seconds. There is nothing here worth optimising today.

**The trigger was watching the wrong variable.** It counted RUNS, and the cost this deferral
was worried about scales with the SIZE OF THE LOG. Those are the same thing only when each
run adds data, and Bronze has not delivered any: its own monitoring views show feeds 5× to
192× past their 24-hour SLA, so `staged` has been identical for four runs. We could have run
this a thousand more times and learned nothing about the risk.

**New trigger, on the variable that actually moves the cost:** re-measure when `staged` per
run exceeds ~9.5M (double today), or when BRZ-1 (CDF on `_raw`) lands — whichever comes
first. BRZ-1 is the more likely one, because it is what makes satellites load repeatedly with
new rows instead of re-reading a static log.

**Unrelated observation, worth its own look eventually.** This deferral was written about
satellites. `load_hubs` costs **4.5× more** — a steady ~120s against `load_satellites`' 27s —
and nothing has ever examined it. Not a problem at this scale, and not being turned into one
here; noted so the next person measures the expensive one rather than inheriting our
assumption about which loader is costly.

### Separately, and on main rather than this branch — CLOSED 26 Aug

`verify_repo.py` raised `ValueError: substring not found` at lines 778/782, looking for
`SET ISOLATION MODE ISOLATED`. DEF-47 removed that statement on 26 Aug and the checker was
never updated, so `verify_repo.py` had been broken on main since. See DEF-57.


---

## DEF-55 CLOSED: `sub_seq` is never assigned, so intra-batch versions collide

**Closed 26 Aug 2026.** The vault FAILED `append_only_check` on
`sat_job_request_details_bullhorn_eu`: *1358 duplicate rows at grain
(job_request_hk, load_dts, sub_seq)*. Found by the first real satellite load, 26 Aug.

### The load itself worked

| | |
|---|---:|
| staged rows | 69,208 |
| versions stored | **46,949** |
| re-deliveries suppressed | 22,259 |
| keys with 1 version | 44,158 |
| keys with 2 | 1,283 |
| keys with 3 | 75 |

44,158 + 2×1,283 + 3×75 = 46,949 exactly. `_v1` reports 45,516 current rows over 45,516
distinct keys — one per key. `DESCRIBE HISTORY` shows only `CREATE TABLE AS SELECT` and
`WRITE`. A second run inserted zero. The change detection is real and idempotent.

### What is wrong

`factory.py:252` sets `sub_seq` to `F.lit(0)` and nothing ever increments it, while
`load_dts` is one `current_timestamp()` per batch. Measured on the loaded satellite:
**1 distinct `load_dts`, 1 distinct `sub_seq`, always 0.**

So every version of a key loaded in the same batch is indistinguishable at the grain
`append_only_check` asserts — and 1,358 keys have more than one. Hubs and NHLs never
exposed this because they hold one row per key. The first satellite is the first thing
that can hold several.

Worse than the gate failure: **the version ORDER within a batch is non-deterministic.**
The loader's `LAG` orders by `(load_dts, sub_seq)`, which is a tie for every row in a
batch, so which hashdiff counts as the predecessor is arbitrary.

### The fix is available but is a decision, not a patch

`applied_dts` is populated on all 69,208 staged rows with **45,226 distinct values** — it
is Bullhorn's own `datelastmodified`. That is a real, source-derived ordering key, and for
a satellite it is the semantically right one.

So the loader would order by `applied_dts` and assign
`sub_seq = row_number() - 1` over `(key, load_dts)`.

**What needs deciding is the fallback.** `applied_dts` is populated only where a binding
declares `applied_dts_column`, and not all do. The options are a stable-but-meaningless
order (by `hashdiff`), refusing to load a satellite whose binding declares no ordering
column, or requiring one in `spec.validate`. This decides the ORDER of stored history,
which is the property the whole loader exists to get right, so it is not being guessed.

### Meanwhile

Bullhorn is loaded and readable, and `sat_job_request_details_bullhorn_eu_v1` reports
correctly. The gate is red until this is settled.


### The fix, and what it cost

The loader now ASSIGNS `sub_seq` instead of trusting the `lit(0)` `_stage` stamps:

```sql
CAST(ROW_NUMBER() OVER (PARTITION BY <key>, load_dts
                        ORDER BY applied_dts, hashdiff) - 1 AS INT) AS __sub_seq
LAG(hashdiff) OVER (PARTITION BY <key> ORDER BY load_dts, applied_dts, hashdiff)
```

`applied_dts` is the ordering key because it is the *source's* statement of when a row
changed; `load_dts` is one `current_timestamp()` per batch and can only tie. `hashdiff`
is appended as a **tiebreak, not a meaning**: `applied_dts` is not unique (45,226 distinct
values over 69,208 staged Bullhorn rows), and an unstable `ROW_NUMBER()` would assign
different `sub_seq` on a re-run, which breaks the `NOT EXISTS` anti-join and makes the
loader non-idempotent. The anti-join and the projection both read the assigned value.

`spec.validate` now REFUSES a satellite binding that declares no `applied_dts_column`,
unless its source reads a vault table — where `applied_dts` is already a system column,
so the evidence is present by construction rather than by waiver. 11 of the 13 existing
bindings already declared one; the 2 that did not are the csats reading `raw_vault`.

### Measured after the reload

| | before | after |
|---|---:|---:|
| versions stored | 46,949 | **46,889** |
| distinct `sub_seq` | 1 | 3 |
| collisions at (key, load_dts, sub_seq) | 1,358 | **0** |
| `_v1` current rows / distinct keys | 45,516 / 45,516 | 45,516 / 45,516 |

44,158×1 + 1,343×2 + 15×3 = 46,889 exactly. A second run inserted zero rows. The 60-row
drop is the fix working, not data loss: with `load_dts` and `sub_seq` both constant the
`LAG` order was arbitrary, so some A→A pairs were never adjacent and survived as
distinct versions. Ordered by `applied_dts` they are adjacent, and collapse.

`append_only_check` is GREEN across 32 tables. `hash_parity` and `mask_survival` re-run
green after the reload.

---

## DEF-56 CLOSED: `_v1` reports a zero-length validity interval for intra-batch versions

Surfaced by the DEF-55 reload, not caused by it. `v1_sql` computes
`valid_to = LEAD(load_dts) OVER (PARTITION BY key ORDER BY load_dts, sub_seq)`. Because
`load_dts` is one timestamp for the whole batch, a version superseded by a later delivery
*in that same batch* gets `valid_to == valid_from` — a validity interval of zero duration.

Measured on `sat_job_request_details_bullhorn_eu_v1`: **1,373 of 46,889 rows**, which is
exactly every superseded version (1,343×1 + 15×2). Current rows are unaffected — 45,516
over 45,516 keys, `valid_to IS NULL` on every one.

This was present before DEF-55 and worse: with `sub_seq` constant the ORDER BY tied, so
`is_current` selected an arbitrary version of a multi-version key. That half is now fixed.
What remains is that the interval itself carries no duration.

`applied_dts` holds the real interval.

### Decided 26 Aug: end-date on business time

The owner's call was to end-date on the non-technical field. `valid_from` is now
`applied_dts` and `valid_to` is `LEAD(applied_dts)`. `load_dts` stays on the row and
still answers "when did we learn this" — it is simply no longer mistaken for "when was
this true".

**The window order moved with it, and that is not cosmetic.** `applied_dts` is not
monotonic with `load_dts`: a backfill or a late-arriving correction delivers, in a later
batch, a row effective EARLIER than a version already stored. End-dating on business time
while ordering by arrival time would emit `valid_to < valid_from` — a negative interval.
The window is now `ORDER BY applied_dts, load_dts, sub_seq`: business order first,
knowledge order to break its ties, `sub_seq` to make the order total and the view
deterministic. `is_current` consequently means "the version the source says is newest",
not "the row that happened to load last" — which is the correct reading for a business
user, and the one that survives a backfill.

Measured first: **no version of any key shares an `applied_dts` with another version of
that key** (0 groups over 46,889 rows), so every interval is real rather than merely
fewer being degenerate.

### After the change

| | before | after |
|---|---:|---:|
| zero-length intervals | 1,373 | **0** |
| negative intervals | 0 | **0** |
| current rows / distinct keys | 45,516 / 45,516 | 45,516 / 45,516 |
| current rows carrying a `valid_to` | 0 | 0 |

The worked example, one job request across three versions — previously three intervals of
zero duration:

| `sub_seq` | `valid_from` | `valid_to` | status | days |
|---|---|---|---|---:|
| 0 | 2026-07-01 16:42 | 2026-07-20 08:52 | Accepting Candidates | 19 |
| 1 | 2026-07-20 08:52 | 2026-07-22 15:06 | On Hold | 2 |
| 2 | 2026-07-22 15:06 | *(open)* | Lost to Competitor | 35 |

Contiguous: each `valid_to` is exactly the next `valid_from`.

### A second implementation, found while fixing this

`factory._emit_v1_view` is a SECOND `_v1` builder, and its guard —
`SATELLITE_KINDS - STAGED_KINDS` — is **empty**, so it is unreachable and the loader owns
`_v1` outright. Unreachable is not harmless: whoever un-stages a kind would silently
inherit whatever end-dating it was left with. It was realigned to `applied_dts` in the
same commit, and a check now pins the two together plus asserts the guard is still empty,
so re-enabling it becomes a deliberate act with its own evidence.

`hash_parity`, `append_only` and `mask_survival` re-run green. Every assertion was
mutation-tested, including the drift between the live and dormant implementations.

---

## DEF-57 CLOSED: `verify_repo.py` had been dead on main, hiding six further defects

The crash was one line. What it concealed was the finding: `verify_repo.py` exits at the
first uncaught error, so every check below the crash had stopped running, and the drift
that accumulated underneath it was invisible. Repairing the crash took the run from
~400 checks to **596**, and surfaced six more failures nobody could see.

### The crash, and where isolation actually lives now

DEF-47 stopped ISSUING `SET ISOLATION MODE ISOLATED` — it is invalid on this runtime
(the one statement of 49 that failed) and the catalog belongs to the USNC Terraform
service principal, so issuing it would fight another system's state. The dependency did
not go away; PROD and TDS still share one regional metastore. It moved from *issued* to
*asserted*: `checks/schema_grant_check.py --assert-isolated`. The checker was still
looking for the ALTER. It now asserts the assertion is wired, that the gate behind it can
refuse, and that the ALTER has **not** crept back.

### What the crash was hiding

| # | Finding | Kind |
|---|---|---|
| 1 | dry-run omitted `--reader-group`, required since DEF-40 | checker, broken since DEF-40 |
| 2 | placeholder check inferred bindings from CLI flags, so `${vault_reader_group}` read as permanently unresolved | checker, false positive |
| 3 | a third `render()` call also omitted `vault_reader_group`; `render` **exits**, so the script died after the last PASS line printing no FAIL | checker |
| 4 | `BUSINESS_KINDS` check pinned to a duplicated literal — it failed *because the code got better* and now delegates to `naming` | checker |
| 5 | `README.md` carried committed **merge-conflict markers** (`<<<<<<<` / `=======` / `>>>>>>> Stashed changes`) from a stash pop, plus a `v<X.Y.Z>` placeholder and a citation of `tooling/superpowers/`, which does not exist | **repo** |
| 6 | target `dev` omitted `job_request/BULLHORN_EU` and `job_request_details/BULLHORN_EU` | **repo** |

(5) resolved by keeping the upstream half — v6.3.0 is the installed version and
`.claude/skills/dv-accelerator-gates/SKILL.md` exists; the stashed half was wrong on both.

(6) is drift introduced in this same session. `dev` and `usnc_tds` point at the SAME
workspace and the same bronze catalog, so their active sets must agree. Activating
Bullhorn updated `usnc_tds` only. A `-t dev` deploy would have created the Bullhorn
tables and silently loaded nothing — exactly what that check exists to catch.

### The root cause, and what changed structurally

Four of the six were **source-text greps pinned to how `factory.py` is spelled** rather
than what it does — `_project`'s signature (DEF-39 added `extra`), `_emit_ghost`'s call
(DEF-41), `_emit_target`'s argument name (DEF-52 renamed `table` to `target`), the
`BUSINESS_KINDS` literal. Every one broke on a refactor that changed nothing it was
asserting. Re-pinning them to today's spelling only resets the clock, so they are parsed
with `ast` instead: structure survives reformatting but still fails if the function is
removed, renamed, or loses the argument that matters. The behaviour behind all four is
owned by `tests/test_accelerator.py`, which executes the flow bodies and inspects the
resulting frames — strictly stronger than reading source.

The `ast` parse is defensive: a `SyntaxError` in `factory.py` now degrades to a reported
failure rather than a stack trace that hides the remaining several hundred checks.
Verified against a deliberately corrupted `factory.py` — 597 checks run, 8 findings.

Every rewritten assertion was mutation-tested: removing `_project`, the quarantine
`extra=`, the ghost's `schema_fields=`, `_emit_target`'s `active_bindings=`, the job's
`--assert-isolated`, the single `BUSINESS_KINDS` definition, a renderer binding, and the
job's `--reader-group` each produce at least one named FAIL.

### The durable fix — done

`verify_repo.py` was run by a human following DEPLOY.md and by nothing else. **A gate
nobody runs rots.** `.github/workflows/verify.yml` now runs both offline suites on every
push to `main` and every pull request, on Python 3.11 and 3.13.

It is deliberately **offline**: no workspace, no profile, no secrets. Every gate that
reads a live lake stays in `vault_load` against a named target, because the standing rule
is that a profile is passed explicitly and never auto-selected — which CI cannot satisfy.

Three things make it a gate rather than decoration:

* Both scripts were confirmed to **exit non-zero** on real failures — a step that always
  exits 0 is the ultimate check that cannot fail.
* `uv run --frozen` resolves from `uv.lock`, so CI runs the reviewed dependency set and a
  lockfile that has drifted from `pyproject.toml` fails there rather than resolving to
  something else.
* The matrix includes **3.11, the declared floor**, which is what keeps
  `requires-python` honest rather than aspirational. Verified from a clean venv:
  `pyyaml` is the only runtime dependency, because the suites stub pyspark.

Actions are pinned to exact releases (`actions/checkout@v7.0.1`,
`astral-sh/setup-uv@v10.0.1`), not floating majors — a floating tag lets an external
repository change what this job runs with no commit here saying so, which is the pattern
DEF-47 refused for the isolation ALTER.

### Found while wiring it

DEPLOY.md — the runbook an operator follows against production — told the reader to run
`python3 verify_repo.py`, which **fails on a clean machine**: no `pyyaml` in the ambient
environment. Its very first instruction was a dead end, and seven more `python3 checks/…`
invocations had the same fault. It also promised "92 checks" and "67 checks" against
actual counts of 598 and 516, and claimed a 3.10 floor against `requires-python >= 3.11`.
All corrected to `uv run python …`.

The counts had drifted because the no-hardcoded-count rule covered README and CHANGELOG
but not DEPLOY.md. Both that rule and a bare-`python3` rule now cover it, and a repo-wide
**merge-conflict-marker sweep** was added — the defect in (5) above was found only via a
symptom (`<X.Y.Z>`), which would have missed the same fault in any other file.

Every rule added here was mutation-tested.

## MEASURED 30 AUG 2026: how a vault streaming table takes a NEW column

Recorded because the repo knew how a schema change FAILS (DEF-26: tables "cannot be narrowed
in place", remedy a drop-and-reload) but not how one succeeds. Widening is the easy direction
and behaves differently.

**A manual `ALTER TABLE ... ADD COLUMNS` is refused outright:**

    [STREAMING_TABLE_OPERATION_NOT_ALLOWED.INVALID_ALTER] The operation ALTER TABLE is not
    allowed: To alter the schema or properties of Streaming Tables, please use the
    CREATE OR REFRESH command.

So the pre-emptive "widen the table, then deploy into a matching schema" plan does not exist.
SDP owns the schema; the change has to arrive through the pipeline.

**The pipeline widens it in place, with no full refresh.** `create_streaming_table(schema=)`
presented 18 columns against a 15-column table and 20 against the twin's 17. The update
succeeded, the columns landed at the declared POSITIONS (10-12, between `paycheckdate` and
`load_dts` — payload sits between the hash keys and the system columns), and the operation
registered in `DESCRIBE HISTORY` as **`DLT SETUP`**, not a mutation. `append_only_check` is
unaffected: zero mutating operations across the table's whole history.

That matters because a full refresh was **not available as a fallback** — the vault tables are
`delta.appendOnly` and Delta refuses the truncate with `DELTA_CANNOT_MODIFY_APPEND_ONLY`. Had
SDP demanded one, the change would have been stuck.

**THE CATCH, AND IT IS THE IMPORTANT HALF.** Existing rows keep `NULL` for the new column,
permanently. A streaming table sees each row once, and the rows were ingested before the
column was declared. Measured immediately after: 526 rows (1 ghost + 260 + 265), and
`currencyid` non-null on **zero** of them — including the 265 lines whose Bronze source
carries `USD`. The value exists upstream and will never reach those vault rows without a
deliberate drop-and-reload, which `append_only_check` and `full_refresh: false` exist to stop
happening casually.

**So adding a column is cheap; backfilling one is not.** The rule of thumb: a new column is
worth adding as soon as the source has it, because future rows start carrying it immediately
and the cost is a metadata operation — but never assume the column answers questions about
history. It answers them from the moment it lands.

## 31 AUG 2026: two hard gates have gone dark, and not because anything failed

Found by sweeping what each gate actually covers rather than reading its verdict — the same
pass that found `qtn_` missing from `append_only_check` the day before.

**The chain:**

    assert_no_broad_grant  ─┐
                            ├─→ apply_governance ─→ assert_mask_survival ─→ assert_audit_completeness
    assert_journal_integrity ┘

`assert_no_broad_grant` is red on the platform team's schema-level SELECT. `assert_journal_integrity`
is red on the admin team's group membership. Neither is ours to clear. Everything downstream is
therefore SKIPPED — measured across both full runs on 30 Aug, and the run before that.

**So `mask_survival_check` has not executed in any recent load.** It is the gate that proves a
declared-sensitive column is still masked everywhere downstream — satellite, `_v1` view, PIT MV,
Gold. Its silence currently means "did not run", and a reader scanning a run summary sees
SKIPPED next to a green-looking job.

**The dependency is not wrong.** `apply_governance` WRITES — grants and mask DDL — and DEF-44
deliberately put both correctness gates in front of it, after a run with unbalanced debits
applied grants anyway. Gating a write on correctness is right.

What is questionable is that a **read-only assertion** inherits that gating. `assert_mask_survival`
changes nothing; it only looks. Its dependency on `apply_governance` exists so masks are applied
before they are checked — sound on a first run, but on the two-hundredth the masks are long
applied and the check is being withheld for a reason that no longer applies.

**Not restructured here.** Rewiring which gate blocks which is a control decision, not a tidy-up,
and this repo has already been bitten once by a dependency list that was edited without the
reasoning being revisited (DEF-44). Options, for a decision rather than a drive-by:

1. Leave it. Accept that the security gates are dark until the two external blockers clear, and
   run `assert_mask_survival` by hand meanwhile — which is what was done on 31 Aug.
2. Depend on `create_mask_functions` instead of `apply_governance`. The masks a first run needs
   come from that task; grants do not affect what `mask_survival_check` reads.
3. Split the chain: keep `assert_audit_completeness` behind governance, free the mask gate.

**Verified by hand in the meantime**, because a dark gate should not mean an unverified property:
`information_schema.column_masks` on 30 Aug showed all four masks intact on `nhl_journal_line`
and `qtn_journal_line` after their schema was widened, and a standalone run of the gate on
31 Aug passed outright:

    MASK GATE PASSED: 30 sensitive column occurrence(s) are masked
    GATE SUMMARY :: mask_survival :: status=PASSED asserted=30 not_evaluated=5

22 masked columns are declared across the model, 10 of them on tables that load in this lake,
20 masked in UC, over a 907-column sweep of `raw_vault` and `business_vault`. The five
not-evaluated are inactive tables created from their ghost flow alone, with no payload column
to mask — announced by name, not silently skipped.

So the property holds. What is missing is the gate SAYING SO on every load, which is the whole
point of a gate.

### Follow-on, 31 Aug: `unclosed_prior_runs=6` is an artefact of the blockers, not six incidents

The completeness gate reports it, deliberately, rather than failing on it. Worth writing down
so nobody chases it as a bug.

**Only one thing closes a run**, and it is `checks/publish_metadata.py:83`, which writes the
`completed` phase into `aud_load_run`. That runs in `publish_model_metadata` — the task gated
behind `apply_governance`, which is gated on the two correctness gates that are red on other
teams. So the only writer of the closing row is the one task that cannot run.

Every run since the blockers appeared is therefore recorded as unclosed. The count will keep
climbing, and **the signal is saturated**: "unclosed" no longer distinguishes a genuinely
broken run from the standing state, which is the whole value it had.

**Not moved, and the reason is the same one that made freeing the read-only gates safe.**
Writing `completed` is itself an assertion that the run finished — including governance. A run
whose grants were never applied did not complete, and marking it closed somewhere earlier
would make the audit say something false to make a number look better. The honest options are
to clear the blockers, or to record a third phase distinguishing "loaded but not governed"
from "completed" — which is a control-model change, not a fix.

It clears on its own the day either blocker lifts.

### Corrected 31 Aug: the post-dedup count is ours, and a 29 Aug amendment said otherwise

On 29 August, while mutation-proving the loop-1 NOT_EVALUATED branch, `spec.validate` refused a
test fixture with the message *"a reconcilable binding may not declare both dedup_by and
manifest_column… Drop one, or give loop-1 a count taken after deduplication."* That refusal is
correct and the underlying fact is real: for the three active feeds that dedup on load, a
stamped `manifest_id` alone does not switch loop-1 on, because a delivered count and a
post-dedup landed count are different numbers by design.

**The error was in who owes the second number.** BRZ-12 was amended to ask Bronze for it, and
the gate's own docstring and NOT_EVALUATED message were changed to say "ask Bronze for that
count". Bronze cannot compute it: `dedup_by` is our modelling choice —
`(company, batchid, account, userdefined1)` for UKG — so asking a delivering system to count
distinct values of a key we chose couples their delivery to our internal model.

**`docs/loop1_control_table_request.html` had it right on 27 August**, and had it in writing:
*"What loop-1 needs from you is item 3, the `_manifest_id` stamp. The count it compares against
has to be taken after our deduplication, which is ours to compute, not yours."* The newer
correction contradicted the older, more careful statement, and nothing flagged it because the
two documents make their claims in prose that no check reads.

Withdrawn in all four places it reached: BRZ-12's "Done when" and its 29 Aug block, this
table's row, and both the docstring and the operator-facing message in
`checks/loop1_reconciliation.py`.

**Worth noting for its own sake:** a gate's refusal message was read as a specification of who
should act. It said what loop-1 needs, not who provides it, and the gap got filled in by
assumption — into two business-facing documents and a message an operator reads at 3am.

### Is `loop1_control_table_request.html` still valid? Yes — checked 31 Aug

- Its "nine other bindings already declare it" is **exactly right**: nine bindings carry a
  manifest column today (eight `_manifest_id`, Striive's `approval_manifest_id`).
- "Stand up a control schema in `01_usnc_bronze_dev` — it does not exist today" is **still
  true**, independently verified 29 Aug.
- Its position on the post-dedup count is the correct one, and is now the only one.

**Who to send it to — resolved 31 Aug, and no platform ask is needed.** `scope_tds_full_scopes_write` holds `CREATE_SCHEMA` on the Bronze catalog, and the deploying identity is a member, so we can stand the schema up ourselves. `governance/control_objects_bronze.sql` is now generated from `control_standard` and committed, mirroring gold's. The page's ask is down from three items to two — write a `ctl_delivery_manifest` row per batch, stamp `_manifest_id` — both Bronze pipeline work.

**Two things that follow.** Creating those tables does NOT make us the owner of Bronze's audit: the rows stay theirs, only the delivering system knows what it delivered, and loop-1 stays dormant until they write them. This removes a dependency, not the blocker. And the mask-bypass mail's narrow scoping now matters more than when it was drafted — it asks for schema-level `SELECT` on the three SILVER schemas only, so it will not touch this `CREATE_SCHEMA` on bronze. A broader "cut that group's access" ask would have removed the very right this work depends on. It will, though, remove the deploying identity's own broad read on the silver vault, which is correct but worth knowing before sending.

### 31 Aug: Bronze's control schema exists, and it was ours to create after all

Applied `governance/control_objects_bronze.sql` against `01_usnc_bronze_dev` — five statements,
all `IF NOT EXISTS`, rendered and split with `apply_governance.render/statements` so the tested
splitter stayed the authority rather than hand-run SQL.

**Not applied with `checks/apply_control_objects.py`, deliberately.** That script is
silver-shaped: it reads one hardcoded `SQL_FILE`, renders a `governance_schema` bronze's DDL
does not use, and writes an `aud_load_run` 'opened' row. Writing into Bronze's audit is exactly
what we must not do — we created containers, we are not loading anything there. Bolting a
bronze mode onto it would have coupled the two.

**Verified, not assumed:**

    declared tables: aud_load_run, aud_table_discard, aud_table_load, ctl_delivery_manifest
    deployed tables: aud_load_run, aud_table_discard, aud_table_load, ctl_delivery_manifest
    column drift:    NONE -- column for column      (via apply_control_objects.column_drift)

    aud_load_run           appendOnly=true   control_object=true
    aud_table_load         appendOnly=true   control_object=true
    aud_table_discard      appendOnly=true   control_object=true
    ctl_delivery_manifest  appendOnly=--     control_object=true

The last line is the one worth reading: the mutable manifest did NOT get the append-only
marker. That is the `_layer_ddl` branch gold never exercised, confirmed against a live object
rather than against generated text.

**What this does NOT do.** Loop-1 is still dormant. The tables are empty, and only Bronze can
fill them — a delivery manifest records what the delivering system delivered. The remaining ask
is the two per-batch steps in `docs/loop1_control_table_request.html`: write a
`ctl_delivery_manifest` row, stamp `_manifest_id` on the rows. A dependency is gone; the blocker
is not.

**Not wired into the job, and that is a decision left open.** Re-applying is idempotent, so a
task could own it — but a task implies we re-assert Bronze's control objects on every load,
which is a stronger ownership claim than creating them once. Worth deciding when a second lake
needs them, not before.

### 31 Aug: `control_conformance_check` is a hard gate no job task runs

Found by sweeping which `checks/*.py` a job task actually invokes. Three are not in
`resources/vault_job.yml`:

| script | in the job? | verdict |
|---|---|---|
| `preflight_target.py` | no | **correct** — runs from the operator's machine BEFORE a deploy |
| `probe_satellite_behaviour.py` | no | **correct** — a diagnostic probe, not a gate |
| `control_conformance_check.py` | no | **its own docstring calls it a HARD GATE** |

It answers "does a layer's control schema match the published standard?" per layer. Until
31 Aug there was little to run it against: bronze had no control schema and gold has no
catalog, so two of three layers would have reported NOT_EVALUATED and silver's DDL is already
verified offline by `verify_repo`. **Bronze changed that** — its schema now exists, and is ours.

Run by hand on 31 Aug against live bronze, using the gate's own pure `conformance()` over
column rows fetched separately (its decision functions are Spark-free by design, precisely so
this is possible):

    deployed: aud_load_run, aud_table_discard, aud_table_load, ctl_delivery_manifest
    conformance('bronze'): NONE -- conformant

**Not wired in, and the reason is the same as the last DAG decision.** A task would assert
bronze's control schema on every load. That is defensible now the containers are ours — but it
is a control decision about how often we re-assert another team's catalog, not a tidy-up. Worth
deciding alongside whether re-applying the DDL becomes a task at all.

### And the drift that change introduced, caught the same day

`control_conformance_check`'s docstring said, under the heading WHY A CHECK AND NOT GENERATED
DDL: *"Bronze's catalog belongs to another team ... we do not deploy into it."* Applying
`control_objects_bronze.sql` made the second half false while the file still asserted it — two
parts of the repo claiming opposite things about who owns bronze's control objects.

Corrected, along with `LAYER_REFERENCE["bronze"]`, which pointed an absent-table message at
`control_contracts/bronze.yaml`. An absent TABLE is a container problem, containers are now
ours, and the message must name the thing the reader would actually change.

**The split that survives, and is now asserted twice:** we create the CONTAINERS, Bronze writes
the ROWS. One check requires the absent-table message to cite the DDL; another requires the
module to still name the published contract, so taking over the tables cannot quietly delete
the fact that the rows are Bronze's. Both mutation-proven.

### 31 Aug: Bronze could not write to the tables we created — GRANTED, same day

Checked because the whole point of `01_usnc_bronze_dev.control` is that Bronze fills it.

    schema owner: adrian.turcu@headfirst.group
    grants:       NO explicit grants on the schema

So the two remaining asks in `docs/loop1_control_table_request.html` — write a
`ctl_delivery_manifest` row, stamp `_manifest_id` — are **currently impossible for them**. The
schema is owned by an individual and carries no grant for anyone else; catalog-level
`USE_CATALOG` does not convey `USE_SCHEMA` on a schema owned by someone else. We handed them a
container they cannot see.

**Bronze's identity looks like the service principal `7732b208-8366-4aef-af09-60e9dec9cf86`**,
which owns `01_usnc_bronze_dev.ukg_raw` and `great_plains_raw` — their source schemas. That is
inference from ownership, not confirmation, which is why nothing has been granted yet: granting
write access to a principal identified by inference is not a step to take quietly.

**Proposed, pending confirmation of who their pipeline actually runs as:**

    GRANT USE SCHEMA ON SCHEMA 01_usnc_bronze_dev.control TO `<bronze principal>`;
    GRANT SELECT, MODIFY ON TABLE ...control.ctl_delivery_manifest TO `<bronze principal>`;
    GRANT SELECT, MODIFY ON TABLE ...control.aud_load_run        TO `<bronze principal>`;
    GRANT SELECT, MODIFY ON TABLE ...control.aud_table_load      TO `<bronze principal>`;
    GRANT SELECT, MODIFY ON TABLE ...control.aud_table_discard   TO `<bronze principal>`;

Per table rather than schema-level, following DEF-40 — even though these four carry no masked
column, so the usual reason for the rule does not bite here. Consistency is cheap; a
schema-level habit is what produced the silver mask bypass.

### Pre-existing, and wider than this: every schema we own is owned by a PERSON

    02_usnc_silver_edm_dev.raw_vault        owner=adrian.turcu@headfirst.group
    02_usnc_silver_edm_dev.business_vault   owner=adrian.turcu@headfirst.group
    02_usnc_silver_edm_dev.control          owner=adrian.turcu@headfirst.group
    02_usnc_silver_edm_dev.governance       owner=adrian.turcu@headfirst.group

Compare Bronze, who own theirs with a service principal. **The new bronze `control` schema
matches our existing pattern rather than deviating from it** — so this is not a problem the
31 Aug change introduced, but it is one it makes newly visible, and now spans two catalogs.

An individual owner is a single point of failure for every ALTER, GRANT and ownership transfer
in the vault, and it ties the estate to one person's account remaining active. The fix is to
transfer ownership to a group. Not done here: changing the owner of four live vault schemas is
exactly the kind of operation that should be deliberate, scheduled, and verified afterwards
rather than folded into an unrelated commit.

**GRANTED 31 Aug**, service principal confirmed by Adrian. Applied and verified:

    schema 01_usnc_bronze_dev.control        7732b208-...  ['USE_SCHEMA']
    ...control.ctl_delivery_manifest         7732b208-...  ['MODIFY', 'SELECT']
    ...control.aud_load_run                  7732b208-...  ['MODIFY', 'SELECT']
    ...control.aud_table_load                7732b208-...  ['MODIFY', 'SELECT']
    ...control.aud_table_discard             7732b208-...  ['MODIFY', 'SELECT']

    other principals on the schema: none

Per table, not schema-level, following DEF-40 — and the schema carries `USE_SCHEMA` only, so
no future table in it is readable by default. `MODIFY` on the three audit tables permits INSERT
and nothing else, because `delta.appendOnly = 'true'` refuses the rest; on
`ctl_delivery_manifest` it permits correction, which is the point of a mutable manifest.

**Verified, and that mattered here.** The apply printed `Query executed successfully` five
times while the shell was also printing `command not found` for backticks in the surrounding
echo lines — the exact shape of a step that reports success having done nothing. The grants
were read back from Unity Catalog afterwards rather than inferred from that output, and only
then recorded as done.

**The loop-1 ask is now fulfillable.** Both remaining items — write a `ctl_delivery_manifest`
row per batch, stamp `_manifest_id` on the rows — are Bronze pipeline work against tables that
exist and that their principal can write. Nothing further is needed from us, and loop-1 stays
`NOT_EVALUATED` until they land.

## 2 SEP 2026: the platform team fixed the mask bypass, and sent four asks back

**Their fix is verified, by our own gate:**

    SCHEMA GRANT GATE PASSED: no catalog- or schema-level read grant on 02_usnc_silver_edm_dev
    GATE SUMMARY :: schema_grant :: status=PASSED asserted=11 not_evaluated=0

From 180 problems to zero. Schema-level grants revoked for both full-scope groups on
`raw_vault`, `business_vault` and `control`, with a permanent carve-out merged in their access
repo so automatic propagation can never re-add them. `governance` keeps its grants, which is
correct: measured, it holds **zero masked columns** and only `meta_vault_model` and the two
event logs. Catalog-level `CREATE_SCHEMA` is also gone.

**The root cause was ours, indirectly.** Their words: our own SR-322422 write access was
implemented by adding the silver catalogs to the `full_scopes` roll-up groups, which propagate
a standard privilege set including `SELECT` onto every schema in their catalogs. Once our vault
schemas existed they picked up the standard grants. Nothing was misconfigured; what was missing
was a sensitive-schema carve-out, which did not exist because the catalog was empty when it was
wired up.

**The grants were pushed on the evening of 26 August.** That closes the question this repo
recorded on 29 Aug as unanswerable from our side -- "two groups have been added since, and we
do not know by whom or when". Now dated.

### Their four asks, and what each costs us

**1. Revoke the manual grants to `us_tds_data_engineer`, and say who needs which tables.**
Role groups on this platform carry no data grants by design: they define capability, and data
access flows through scope groups and explicit grants in the access repo.

**These grants are ours, and a hand revoke would not hold.** Measured 2 Sep: 26 table-level
`SELECT` (24 `raw_vault`, 2 `business_vault`) plus 2 `USE SCHEMA`. Prod does not exist yet, so
that is the whole picture -- their "~38" counts something we cannot see. They are emitted by
`checks/apply_governance.py::table_select_grants()` and `governance/apply_masks.sql:214,220`,
so the next successful `apply_governance` re-creates every one. **This needs a code change, not
a revoke.**

Safe to remove: measured, the masks do NOT reference `us_tds_data_engineer` at all -- they
admit `pii_cleared_us`, `usnc_data_analyst_finance` and
`global_dataplatform_pipeline_job_runners`. Revoking that group's `SELECT` removes read access
without touching masking.

**AND IT COLLIDES WITH OUR GATE, WHICH THEY NEED TO KNOW.**
`schema_grant_check.unauthorised_table_readers()` fails the build on any table-level read grant
held by a principal outside its allowlist. If the access repo grants per-table `SELECT` to
named people -- exactly what they propose -- our gate goes red on their correctly-wired grant.
The allowlist has to learn about repo-managed grantees, or the gate has to take its allowed set
from somewhere they control. Raised with them rather than discovered on their next apply.

**2. Move schema ownership to the EDM pipeline service principal, and codify the schemas in
Terraform.** Say yes. This is the finding recorded here on 31 Aug, and their reasoning is
sharper than ours was: **object owners bypass masks, exclusions and grants entirely**, so while
the vault sits under a personal account the carve-out they just merged can still be worked
around. They offer ownership transfer plus Terraform import with no objects dropped and no
downtime.

**This exposed a false reassurance in our own code.**
`unauthorised_table_readers()` said "Owners are not a hole". It meant "owners do not appear in
grant rows", which is true, but it reads as an assurance about the masking posture and is not
one. Corrected 2 Sep. No gate in this repo -- not this one, not `mask_survival_check`, which
reads mask DECLARATIONS -- can see an owner ignoring a mask.

**3. No user-created schemas in the layer catalogs.** They named
`01_uks_bronze_dev.test_arul` as an example. **Left out of the reply on Adrian's call (2 Sep):
it is the UK lake and not part of this request.** For the record it IS ours — a GrowthArc
consultant working inside the data engineering team — and the data team cannot move it in any
case: the catalog is invisible from this workspace and the `CREATE_SCHEMA` that would have
allowed it has just been revoked. If they raise it again it is a separate item, ours by whose
person it is and theirs by who can act. Two drafts over-promised on it before it was cut, both
caught by Adrian: first that it was not ours, then that we would move it.

**What the reply DOES answer on their point 3, because it is the US lake and it is ours:** we
created `01_usnc_bronze_dev.control` on 31 August, four tables, personal account — exactly the
practice they are asking to stop. Disclosed before their sweep finds it, with a request to
codify it alongside the silver schemas. Swept the two catalogs this workspace can see and it is
the only one: `test` and `test_raw` in usnc bronze belong to the ingestion service principal.

**4. Dev-owned schema creation is a design discussion, not a side effect.** Noted, no action.
The PR route is fine for us.

### What we owe them

| | |
|---|---|
| ~~Stop emitting data grants to a role group~~ | **DONE 2 Sep** — one function owns every data grant, `--emit-data-grants` defaults OFF, `apply_masks.sql` emits no GRANT at all, and a check refuses the flag in the job |
| ~~Revoke the 28 existing grants~~ | **DONE 2 Sep** — 26 table + 2 schema revoked; 0 privileges left to the role group, and the vault still reads by OWNERSHIP (526 rows). `schema_grant` still PASSES at asserted=11 |
| Name who needs which vault tables | **needs Adrian** -- this is a people question, not a technical one |
| Accept the ownership transfer + Terraform import | say the word to them |
| Declare `01_usnc_bronze_dev.control` in their repo | disclose it proactively |
| Tell them our gate will fight per-table grants to individuals | before their next apply |

### Vault access policy, stated 2 Sep 2026 — engineering only, consumers get gold

Adrian, in answer to the platform team's "tell us which people need which vault tables":

> The silver raw vault and business vault should only be accessible by the data engineering
> group. This is a historised storage/audit model. No analysts, no BI developers, no business
> users — they will have the gold layer for that.

**This is the existing design, now stated as policy rather than inferred from code.** It has
been encoded since 26 Aug (DEPLOY.md Phase 6STOP) in two places:
`schema_grant_check.unauthorised_table_readers()` fails the build on a vault read grant held by
anyone but one engineering group, and `apply_governance`'s `--privileged-group` help text says
"NOT a consumer group -- consumers read gold". So the gates already enforce what was only
written down in passing.

**Why it is not merely a preference.** A satellite carries every version of every row. Answering
a business question directly from it means end-dating correctly, and the shape invites joining
without doing so and reading superseded rows as current. The model is built to be reconciled
against, not queried. Gold exists to answer questions; silver exists to be able to prove what
was true and when.

**Two consequences that need the platform team, recorded because they will come back:**

1. **They must nominate the principal, and our gate must be told its name.** Asked once, in the
   gate heads-up, rather than twice: a paragraph explaining their own access model back to them
   was drafted and cut on Adrian's challenge. They told us role groups carry no data grants;
   restating it is not information, and which principal implements "the engineering team" is
   their design decision, not ours to reason about in their inbox. What we genuinely need is the
   resulting NAME, because `unauthorised_table_readers()` enforces an allowlist — and that is
   already the question the heads-up asks.
2. **Gold does not exist.** `03_usnc_gold_edm_dev` has not been created, so today there is *no*
   route to this data for anyone outside data engineering. The correct answer to any request is
   "gold, when it lands" — not a vault grant as an exception. An exception here is
   indistinguishable from the posture just closed, which is why the reply says so before anyone
   asks.

#### Refined 2 Sep: the split is three-way, and only one third is a grant

Adrian: `us_tds_data_engineer` is his team, ~30 engineers. He does not want thirty people able
to modify the vault model, so:

| who | what | mechanism |
|---|---|---|
| EDM pipeline service principal | owns the schemas; the job runs as it | ownership transfer (platform ask 2) — not a person, because owners bypass masks |
| Named maintainers: `adrian.turcu@headfirst.group` + one more | change the model and deploy it | **not a database grant** |
| The rest of `us_tds_data_engineer` | `SELECT` on vault tables, read only | per-table, via the platform access repo |

**The write half is not a permissions question, and that is the useful realisation.** Nobody
needs `MODIFY` on a vault table, us included. The model changes by editing
`metadata/entities/*.yml` in git and deploying — so "who may change the data vault" is
controlled by repository review plus deploy rights, not by a database privilege. That is
already how it works, and it is a stronger control than a grant: a change arrives as a reviewed
commit or it does not arrive. Thirty engineers can read the vault; two can change what it is.

So the only thing the access repo needs to carry is per-table `SELECT` on `raw_vault` and
`business_vault` for the engineering scope group. **We are not asking for the grants we revoked
to be restored to us — we are asking them to be made properly, to the right principal.**

**Measured 2 Sep, and it is the same problem three times over:** the job's creator is
`adrian.turcu@headfirst.group`, its `run_as` inherits that creator, and all four schemas are
owned by the same account. The target state separates all three — SP owns and runs, named
humans deploy, the team reads.

**Bus factor: acknowledged, with a plan and an interim gap.** The second maintainer is a hire
— an enterprise data modeller being recruited, who will hold the model lifecycle alongside
Adrian. **Until they start, Adrian is the sole maintainer.**

That makes the ownership transfer MORE urgent, not something to wait on. Today one account owns
the objects, runs the pipeline (`run_as` inherits the creator) and is the only person who can
change the model — and the platform team's own point bites hardest here: an owner bypasses
masks, exclusions and grants. Moving ownership to the service principal removes two of those
three immediately and depends on nobody being hired. The reply says the transfer is ready to go
from our side, decoupled from the hire.

**And onboarding the modeller needs nothing from the platform team**, which is worth knowing
before someone assumes it does: adding a maintainer costs a repository permission and deploy
rights, not a database grant and not an access-repo change. That falls out of keeping the write
side outside Unity Catalog.

## 3 SEP 2026: reply sent to Michael — and a sequencing hazard the transfer creates

`docs/platform_team_access_reply.html` went out. Waiting on the platform team for three
things: the principal name our gate's allowlist should trust, the ownership-transfer PR, and
codifying `01_usnc_bronze_dev.control` alongside the silver schemas.

### The hazard: after the transfer, the pipeline has no access at all

Not speculation — it follows from three things measured on 2–3 September:

| measured | value |
|---|---|
| `us_tds_data_engineer` privileges on the vault | **0** — we revoked 26 table + 2 schema on 2 Sep at their request |
| `scope_tds_full_scopes_*` schema grants on the vault | **none** — they revoked them, with a permanent carve-out |
| schema grants on `raw_vault` / `business_vault` | **NO GRANTS** |
| schema owner | `adrian.turcu@headfirst.group` |
| job `run_as` | **inherits the creator** — the same account |

So **ownership is now the pipeline's only access path.** Every other route was closed
deliberately, by us and by them, and each closure was correct on its own.

The moment ownership moves to the EDM pipeline service principal, the job — still running as
Adrian — holds nothing: no owner privileges, no grants, no group route. `raw_vault` writes fail
on the next load. Ask 2 is a change we asked for and agreed to, and on its own it breaks the
pipeline.

### What has to happen with it, not after it

**The job must run as the same service principal that takes ownership.** That is our change,
not theirs — `run_as` is bundle configuration. It has to be deployed with the transfer, not
discovered afterwards:

- transfer first, run_as second → every load between them fails;
- run_as first, transfer second → the SP is not yet the owner and holds nothing, so the same
  failure, in the other direction.

They land together, or the SP is granted `MODIFY` for the gap. Worth telling Michael
explicitly, because from their side "ownership transfer with no objects dropped, no downtime"
is true of the *objects* and not of the *pipeline*, and nothing in the reply we sent mentions
`run_as`.

**Blocked on:** the service principal's identity, which is theirs to nominate — the same
question the reply already asks for the read grant. Nothing to prepare until it is named,
beyond knowing the change is a one-line `run_as` in `resources/vault_job.yml` plus a deploy.

**Also worth confirming when they answer:** whether the SP needs `EXECUTE` on the mask
functions in `governance`, since `apply_masks.sql` grants none today and the masks admit
`global_dataplatform_pipeline_job_runners` rather than a named SP.

## 5 SEP 2026: the three legacy request documents, brought current and gated

Three documents in `docs/` were written to be sent to other teams and then left behind by the
work they described. None was wrong when written; all three had become wrong by standing still.
No gate looked at any of them.

**`loop1_control_table_request.html` — the headline asked for something we withdrew.** Its H1
read "Four feeds need a manifest id", which is the `_manifest_id` **column** withdrawn on
1 Sep, and its own body already asked for one manifest **row** per batch instead. A reader who
got as far as the body would have found the contradiction; a reader who forwarded the page on
its title would have sent Bronze after the withdrawn ask. Now "Four feeds need one manifest ROW
each — no column".

**`platform_team_mask_bypass_email.html` — closed 2 Sep, kept deliberately.** It is marked
CLOSED with the gate output that closed it (180 problems → `status=PASSED asserted=11`). Kept
rather than deleted because the DEF-40 reasoning it carries is the argument this repo reaches
for whenever a schema-level `SELECT` is proposed as a convenience — and one was very nearly
proposed again on 5 Sep, inside the `run_as` note, before being withdrawn.

**`workspace_admin_request.{html,md}` — the pair had drifted ten days apart.** The HTML
gained request three on 29 Aug and closed it on 2 Sep. The markdown mirror stopped on 26 Aug:
still titled "Two requests", still carrying the `apply_governance` Bronze-revoke blocker the
HTML had corrected that same day, still pointing at a `DEPLOY.md` phase that holds nothing.
The HTML's own footer sends a reader to that markdown for the full technical record.

Both now carry three requests, the same three asks, and the same status — requests one and two
open, request three closed 2 Sep — and `tests/test_accelerator.py` gates the pair the way the
loop-1 pair has been gated since 27 Aug: nine tokens that must appear in both or in neither,
plus a refusal of any phrasing that still counts the requests as two. Mutation-proven five
ways (drop the folder path from one side; restore either stale title; restore the `6STOP`
pointer; flip the gate outcome), each failing on the file that drifted.

**What this pattern keeps costing.** Every one of these defects is a correction that landed in
one place and not its twin, and in each case the twin is the copy most likely to be sent to
another team. The gate is cheap; the absence of one is what let ten days pass.

## 5 SEP 2026: a legal-entity hub, and the first hierarchical link this model has ever built

**The ask:** solve `buyer` everywhere. Guidant Global belongs in a legal-entity hub with
every other Impellam Group and HeadFirst Group company, connected by a parent/child link.
Design and reasoning in `docs/legal_entity_design.md`.

**What `buyer` is, measured:** three different things in one column — the client's current
name, the client's superseded name (`commonspirit_health` still delivers `Dignity Health`),
and one of *our own* operating companies (`delphi_tech` delivers `Guidant Global, Inc.`,
because Guidant runs Delphi's programme). Two distinct problems, deliberately kept apart:
same entity spelled many ways (Beeline's `KC` / `Kimberly-Clark` / `Kimberly-Clark Corp
(Consolidated)`) wants identity resolution; a different entity entirely wants a
relationship model.

**Nothing is resolved at load time.** Silver takes Bronze faithfully; Gold filters for the
use case. `hub_job_request` keeps `buyer` as delivered.

### The defect: `hal` was supported and had never been built

`hal` has been in `spec.py`, `naming.py` and `factory.py` since the model was written, with
no entity using one. Building the first, before any guard existed, it **loaded** — and
produced FK columns `['legal_entity_hk', 'legal_entity_hk']` (one column where two legs
were declared) and a link key hashing the child business key twice. A hierarchy in which
every node is its own parent, with no error anywhere.

Cause: `parent_keys` is a mapping keyed by hub name, so a hub named twice resolves both
legs to one entry. It propagated to **six** places, each independently reading a leg as a
hub — key derivation, column projection, quality rules, the contract's FK map, the
ontology's object properties, and three scope checks that recover a hub by stripping `_hk`
from a column name.

**Fixed with `parent_roles`** — mandatory the moment a hub repeats, both legs always roled,
role must be a plain identifier because it reaches a generated column name, and
`parent_keys` keyed by role rather than by hub. Mutation-proven five ways: no roles; fewer
roles than parents; two legs sharing a role; an unsafe role string; `parent_keys` still
mapped by hub. Each rejected; restored loads.

### Built

`hub_legal_entity` (authored key, so one company is one row across every feed),
`hal_legal_entity_hierarchy` (roles `parent`/`child`), `esat_legal_entity_hierarchy`
(driving key **child** — a company has one owner at a time; driving on the parent would
close every other child of that parent whenever one moved). 992 verification checks pass;
both offline suites green.

### The rebrand is the test case, and it is weeks away

Impellam Group and HeadFirst Group unify under a new brand. In this shape: one new hub row,
two new link rows, two new effectivity rows. **No key changes, no reload, nothing updated.**
Under the current model `buyer` is hashed into the tenant key, so a rename re-keys the hub
and strands every satellite row behind it. That is why this landed now rather than after.

### Not built, and why

All three entities carry **placeholder** `bronze_table` values and are absent from
`active_sources` — a declared shape that loads nothing, the same state
`sat_job_request_details/FIELDGLASS_US` is in. The roster does not exist: one row per
company across both groups plus the client legal entities the feeds name is a business
input, not a query. The same-as link resolving observed `buyer` strings needs that roster
first, and needs a decision on a mapping that is **not one-to-one** — `bayer` is already at
least two GP customers, `BAYCL` and `BAYP`.

**Blocked on, in order:** who maintains the roster and where it lives; whether an HFIG
client code already exists that `legal_entity_code` should reuse (the worksheet's
`hfig_client_code__FILL` is empty in all 65 rows); whether a legal entity can play more than
one role, which would make role a property of the relationship rather than the entity; and
the `RM00101` finance tie-back, still unmodelled since 26 Aug.

### 5 Sep, later: the domain list, and what it does and does not give us

27 domains supplied as the closest thing to a roster; seeded into
`docs/legal_entity_roster_worksheet.csv`. Three findings.

**Domain ≠ brand ≠ legal entity.** 27 domains → **18 apparent brands** (`lorien.co.uk` and
`lorienglobal.com` are one; the three `headfirst.*` are one; `ext.pro-unity.com` is a
platform hostname, not a brand domain) → an **unknown** number of legal entities. The VMS
works in trading names (`buyer` delivers `Guidant Global, Inc.`); finance works in legal
entities (`RM00101.custnmbr`). That mismatch is why `bayer` already maps to two GP
customers. **The list is a brand roster; the hub needs a legal-entity roster**, so the
worksheet asks for company number and jurisdiction rather than treating a brand as an
entity. Only business and legal can fill the right-hand side.

**Two are also source systems, and one is missing.** `pro-unity.com` is **also
`PROUNITY_EU`**, an active binding in this vault — the "more than one role" question with a
concrete instance already in front of us. And **`STRIIVE_EU` is an active source system
with no domain in the list**: either a group company was omitted, or Striive is
third-party and belongs outside the roster. Bullhorn, Fieldglass, UKG and GP are
third-party and their absence is correct.

**Three could not be placed:** `barpellam.com`, `irishrecruitment.ie`, `staffingms.com` —
marked UNKNOWN rather than guessed. The other 24 carry a `likely_group__CHECK` value, 12
Impellam / 12 HeadFirst, which is **inference from brand knowledge, not measurement**, and
is named that way so nobody reads it as fact.

**Role question answered:** yes, a legal entity can play more than one role; unknown in
this case; business and legal own it. The design already permits it — the hub holds the
entity, every relationship lives on a link. What is open is how a SECOND relationship type
("operates the programme for", alongside "is owned by") gets added. A `relationship_type`
in the hierarchy key is cheap and wrong: every traversal would then need a filter, and one
that forgets merges ownership with operating relationships, silently. **Recommended: a
separate link per relationship type** — make the wrong query impossible to write rather
than merely incorrect. Not built, because which second relationship exists is the open
question.

**Striive resolved, 5 Sep:** confirmed a **HeadFirst platform**, added to
`docs/legal_entity_roster_worksheet.csv` (28 rows, 19 brands). Its domain was not in the
supplied list and still needs one.

**And the answer raised a better question: a platform is not a legal entity.** Striive is a
platform, so is ProUnity, and possibly Comensura — a platform is a *system a legal entity
operates*, and `hub_legal_entity` takes rows of the second kind only. The worksheet now
carries `kind__CHECK` (`group_parent` / `brand` / `platform`): 3 platforms, 2 marked
`platform?` for Comensura, 2 group parents, 21 brands.

This is the `buyer` problem one level up. `Guidant Global, Inc.` arrives as an account
holder; `STRIIVE_EU` and `PROUNITY_EU` arrive as **record sources**. Neither is
automatically a legal entity. **For every platform row the roster question is not "what is
its legal entity name" but "which legal entity operates it"** — asked on each row's
`notes__FILL`.

**Two more answers, 5 Sep:** `striive.com` is the platform domain, now filled in. And
**Comensura is a company, not a platform** — trading in the UK and Australia. The
provisional `platform?` marking was wrong and is corrected; `kind__CHECK` is now 23 brands,
3 platforms (ProUnity ×2, Striive), 2 group parents.

**Comensura is the first concrete brand-to-entity fan-out.** One brand trading in two
jurisdictions is very probably two registered companies, which settles the roster's shape:
the hub is keyed per legal entity, one brand can own several rows, and the hierarchy ties
them together. It also means **the roster is larger than 19 brands by an amount nobody has
counted.**

The worksheet now derives `jurisdiction_hint` from the ccTLD — the only mechanical
derivation in the file, unlike the inferred `likely_group__CHECK`. 11 of 28 rows carry one
(NL 6, GB 2, IE 2, AU 1). A hint, not an answer: `.com` says nothing and a national site is
not proof of a national company. Only `comensura` spans more than one signalled
jurisdiction today, which is a fact about the domain list rather than the group — SRG has
`.ie` and `.com`, Lorien `.co.uk` and `.com`. Those are the next rows to check.

**SRG and Lorien confirmed UK + Ireland, 5 Sep — and recording it broke the worksheet,
usefully.** Lorien's domains are `lorien.co.uk` and `lorienglobal.com`; there is **no `.ie`
domain**, so a sheet with one row per domain had nowhere to put Lorien's Irish entity. Same
shape as Striive having no domain at all. Twice is a pattern: **the domain list is evidence
about entities, not a list of them.**

`docs/legal_entity_roster_worksheet.csv` is now keyed by **(brand, jurisdiction)** — an
entity candidate — with domains demoted to attributes, split into `country_domains`
(ccTLD, evidences a country) and `brand_domains` (`.com`, evidences only the brand). Folding
them together would have listed `lorienglobal.com` against Lorien's Irish row and hidden the
gap the pivot exists to show.

28 domains / 19 brands → **22 entity-candidate rows**: 6 jurisdictions CONFIRMED, 7 inferred
from ccTLD, **9 still unknown**, and **2 with a known jurisdiction and no domain evidence at
all** — `lorien/IE` and `srg/GB`. Those two are the proof that **the roster cannot be
finished from domains**; it needs a company-registry answer per row. The 9 UNKNOWN rows are
the next question, and each could fan out the way Comensura, SRG and Lorien just did.

**Three more answers, 5 Sep:** `barpellam` and `staffingms` confirmed **Impellam**;
`guidant_global` confirmed **Impellam, UK and US**. Group membership is now settled for
every brand but one — `irishrecruitment` is the last unplaced.

23 entity candidates: 8 jurisdictions CONFIRMED, 7 ccTLD-inferred, 8 unknown, and **4 with
a known jurisdiction and no domain evidence** — `lorien/IE`, `srg/GB`, and both Guidant
rows, which doubled that count from a single `.com`.

**Guidant Global is where the design pays for itself.** It started the exercise —
`delphi_tech`'s feed delivers `buyer = "Guidant Global, Inc."` — and it is now two legal
entities, not one. **`Inc.` is a US corporate form**, so the account holder on that feed is
probably the US entity rather than the UK one. Resolving that string therefore means
picking between two rows of *our own* roster, not merely recognising a name; a same-as link
treating "Guidant Global" as one thing would be wrong in a way no gate could see, because
both candidates are real and both are ours. Recorded as a hypothesis on the row, to confirm.

**`irishrecruitment` confirmed Impellam, UK + Ireland — every brand's group is now known**,
zero unplaced from three at the start of the day. 24 entity candidates: 10 jurisdictions
CONFIRMED, 6 ccTLD-inferred, 8 unknown.

**And a rule was stated: "Impellam is everything outside the EU."** Worth more than any
single answer, because it classifies brands nobody has asked about yet — so it was tested
against all 24 rows rather than adopted.

| group | jurisdictions present |
|---|---|
| HeadFirst | `NL` |
| Impellam | `AU`, `GB`, `IE`, `US` |

**It does not hold literally, and the exception is consistent:** three Impellam rows sit
inside the EU — `lorien/IE`, `srg/IE`, `irishrecruitment/IE` — all Ireland, all confirmed by
the business itself. No HeadFirst row sits outside Benelux. **The boundary that fits every
row is "HeadFirst is Benelux, Impellam is everything else."** Stated as "outside the EU",
the rule would misfile any future Irish entity as HeadFirst, and Ireland is where three
Impellam brands already trade.

Used as a check rather than a source, it **corroborates every inferred group value that can
be tested** — each `.nl` brand reads HeadFirst, each GB/IE/US/AU brand reads Impellam. Eight
rows still carry no jurisdiction so cannot be tested: `barpellam`, `bartech`, `between`,
`carbon60`, `impellam`, `prounity`, `staffingms`, `striive`.

**`bartech` US, `carbon60` UK, `impellam` UK confirmed 5 Sep.** Five rows still have no
jurisdiction: `barpellam`, `between`, `prounity`, `staffingms`, `striive`.

**`impellam.com/about-us/our-group` read the same day. It expands nothing, and that is the
finding.** The page names six operating companies with their footprint — Guidant Global
"80+ countries", Lorien and SRG "UK, Europe and North America", Carbon60 "UK & Europe",
Bartech "North America", Comensura none stated. **Operating footprint is not legal-entity
count**: Guidant is active in 80+ countries and certainly does not hold 80+ registered
companies. The roster is entities, so it keeps the confirmed answer (Guidant = UK + US) and
records the page as context in two separate columns, `on_official_group_page` and
`operating_footprint_stated`, so the two can never be conflated.

**Two things the page does tell us.** Four Impellam-side brands are absent from it:
`barpellam`, `irishrecruitment`, `staffingms` and `impellam` itself. The parent's absence is
expected; the other three are confirmed Impellam but are not operating companies, which
suggests legal entities, dormant names or sub-brands rather than divisions — exactly the
distinction the roster needs from them.

**And it puts a question against the Benelux rule.** Carbon60, Lorien and SRG all state they
operate in "Europe". If any holds a **Dutch** entity, an Impellam brand sits in Benelux and
the rule that currently fits all 24 rows stops fitting. Operating in Europe does not imply a
Dutch company, but it is the one thing that could break the rule — ask, do not assume.

**`prounity` BE, `between` NL, `striive` NL, `barpellam` GB confirmed 5 Sep.** 23 of 24
entity candidates now carry a confirmed jurisdiction; **`staffingms` is the last one
without.** The groups partition cleanly: HeadFirst `BE`/`NL`, Impellam `AU`/`GB`/`IE`/`US`.

**Belgium was the useful arrival, Ireland the decisive one.** Belgium is both EU and
Benelux, so `prounity/BE` corroborates either formulation of the group rule and
discriminates between neither. Ireland is EU and *not* Benelux and is Impellam three times
over, so Ireland alone settles it — "Impellam is everything outside the EU" would misfile a
future Irish entity as HeadFirst. Belgium also makes "Benelux" **evidenced** rather than
extrapolated: until it arrived every HeadFirst row was `NL`, and calling that Benelux was a
guess about the boundary's shape rather than an observation of it.

**A sharper reading, offered not adopted:** the split is linguistic rather than geographic —
`GB`/`IE`/`US`/`AU` are the English-speaking markets, `BE`/`NL` the Dutch-speaking ones.
Fits all 23 rows and is easier to apply than either boundary, but Carbon60, Lorien and SRG
all claim "Europe" operations, so one German or French Impellam entity would break it.
Confirm before using it to classify anything.

**`staffingms` US — every row now has a jurisdiction.** The roster's derivable work is
finished: 24 entity candidates, each with a group and a jurisdiction, closed on 5 Sep from
nine unknown that morning. 18 confirmed by the business, 6 still ccTLD-inferred (the `.nl`
brands, where domain and group rule agree — cheap to confirm, nothing depends on them).

Final shape: HeadFirst `BE`/`NL`, Impellam `AU`/`GB`/`IE`/`US`.

**None of it is a hub row yet.** Every column that identifies an actual company is empty
across all 24 rows — `legal_entity_name__FILL`, `company_number__FILL`,
`parent_legal_entity__FILL`, `legal_entity_code__FILL`. That is the boundary of what this
side can produce: a brand and a jurisdiction narrow the search to one registry and one
company, they do not name it. Until those four are filled, `hub_legal_entity` has nothing to
load and all three entities stay on placeholder bindings, absent from `active_sources`.

**Three rows to watch when they are:** `barpellam` (GB), `irishrecruitment` (IE) and
`staffingms` (US) are confirmed Impellam but appear nowhere on the group's own
operating-company page. Most likely dormant names, holding companies, or entities trading
under another brand — and each is a different answer for the hierarchy, not just a different
label.
