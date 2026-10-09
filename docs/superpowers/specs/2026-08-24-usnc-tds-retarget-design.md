# Retargeting the DV Accelerator to usnc_tds

Date: 2026-08-24
Status: approved (design); implementation plan not yet written
Accelerator version at time of writing: v0.2.0 · hash rulebook 1.0.0

---

## 1. Context

The accelerator was built EU-first: `weu_tds` is the bundle default, the conformance
baseline, and the assumed first build. That is no longer the plan.

**`usnc_tds` (North Central US, TDS) is the real development environment.** US data
loads there, and the raw and business Data Vault 2.0 model is prototyped in its silver
layer. `weu_tds` will not be used.

This document records the decisions taken to retarget, the defects found while taking
them, and the work decomposition. It does not cover TDS-to-PROD promotion pipelines,
which are explicitly deferred.

---

## 2. Estate model

Catalog convention is `0N_<lake>_<layer>_<domain>_<env>`, standardised across all four
regions. `edm` occupies the domain slot; it is not a modifier on the layer.

| Layer | TDS (dev) | PROD | Shape |
|---|---|---|---|
| Bronze | `01_usnc_bronze_dev` | `01_usnc_bronze` | one catalog, schema per source system |
| Silver | `02_usnc_silver_edm_dev` | `02_usnc_silver_edm` | empty; created for the Data Vault model |
| Gold | `03_usnc_gold_edm_dev` (not yet created) | `03_usnc_gold_edm` | per-domain catalogs |

PROD and TDS are separate Databricks workspaces per region. PROD catalogs carry no
`_dev` suffix. `db_usnc_datalakehouse_tds` is the workspace default catalog and is never
written to.

The domain-split silver and gold catalogs (`02_usnc_silver_finance_dev`,
`02_usnc_silver_job_order_dev`, `02_usnc_silver_job_request_dev`,
`02_usnc_silver_marketing_sales_dev`, and their gold counterparts) are pre-existing.
Those domains become hubs inside the EDM. Folding them in is separate work and is out of
scope here.

**Workspace count.** The estate has nine workspaces, not the eight `databricks.yml`
enumerates: four lakes x two environments, plus an EU "Datalakehouse-Global" workspace
that receives Gold from all four lakes via Delta Sharing. The topology comment in
`databricks.yml` is incomplete and must be corrected.

### 2.1 Bronze schema pairing

Each source system appears twice in `01_usnc_bronze_dev`:

- `<source>_raw` — the dump of files from storage. Append-only landing.
- `<source>` — Auto CDC applied, plus physical PII masking and similar treatment.

**The vault reads `<source>_raw`.** See decision D3.

---

## 3. Decisions

### D1 — Three catalog variables, not one catalog with layer schemas

The accelerator assumed a single `catalog` containing `bronze` / `silver_vault` /
`gold` / `governance` schemas. The estate places each layer in its own catalog.

Replace `catalog` + `bronze_schema` + `gold_schema` with:

    bronze_catalog     read target; schema-per-source
    catalog            silver; the vault's write target
    gold_catalog       projection target
    vault_schema       unchanged (silver_vault)
    governance_schema  unchanged (governance)

`bronze_schema` is deleted outright. There is a schema per source system, and that is
already carried in each entity's three-part `bronze_table:` value.

`vault_schema` and `governance_schema` do not yet exist in `02_usnc_silver_edm_dev` —
the catalog currently holds only `information_schema`. Both must be created.

### D2 — Identical model in all four lakes, no exceptions

The enterprise Data Vault model is standard across all four lakehouses: same entities
everywhere, same generated table inventory. A satellite whose source system does not
operate in a given region exists there and is empty.

This is a hard requirement. It keeps `conformance_check.py` valid exactly as written —
its treatment of any table present in one lake and absent in another as drift
(`conformance_check.py:154-155`) is correct under this decision.

Rejected alternative: splitting conformance into "rulebook and column layout must match
everywhere" plus "table inventory compared only within a region's declared source set".
Rejected because the requirement to run one model unmodified in every region is
non-negotiable.

### D3 — Read `_raw`; mask in silver via Unity Catalog

Bronze currently applies physical, deterministic PII masking in the non-`_raw` schema.
The vault will **not** consume that. It reads `<source>_raw` and declares masks in
entity metadata, which the factory emits into each generated table definition and
`mask_survival_check.py` enforces.

Rationale, in order of severity:

1. **Hash key stability.** Business keys derive from personal identifiers. Hashing a
   masked value is only safe if the masking is byte-identical in all four regions.
   Physical per-region masking would silently prevent keys from joining across lakes —
   defeating the purpose of gate zero. `hash_parity_check.py` cannot detect this: it
   proves the algorithm matches the reference implementation, not that the input is
   stable.
2. **Streamability.** The non-`_raw` table is an Auto CDC target: current state per key,
   receiving updates and deletes. `factory.py:152` calls `spark.readStream.table()`,
   which fails on such a table with `Detected a data update`. `_raw` is append-only and
   streams natively.
3. **Change stream.** Auto CDC consumes the operation flag and does not re-emit it. With
   no `cdc_op` column, `factory.py:107` falls back to `F.lit("I")`, every row is treated
   as an insert, the `changed_only` filter at `factory.py:315` passes everything, and the
   satellite appends every row on every run. Append-only and loop-1 gates would both
   still pass; the data would simply be wrong.

**Consequence — the silver vault holds unmasked PII at rest**, protected solely by Unity
Catalog column masks. Day-1 verification task 1 (mask survival through `_v1`, PIT
materialized view and Gold projection) therefore moves from prudent verification to
blocking prerequisite. If masks do not propagate, personal columns must move to
satellites Gold never projects — a modelling change, so it must be settled before
entities are finalised.

### D4 — Bronze ingestion is out of scope

`src/pipelines/bronze_ingest.py` performs Auto Loader ingestion from files into an
append-only `bronze_raw`, then Auto CDC. The estate's `<source>_raw` to `<source>`
pipeline already does this. The accelerator consumes the estate's bronze rather than
building a second one.

The estate is mid-migration: all bronze data sources are being moved onto Auto CDC. Once
complete, `_raw` carries a native change stream for every source and no snapshot
conversion is needed. Until then the estate is mixed, so `change_detection` (`cdc` or
`antijoin`) and `cdc_op_column` remain settable per source binding, and the `antijoin`
fallback stays available.

### D5 — Active sources declared per target

Under D2 every lake declares every source, but only some have that source in Bronze. The
factory must therefore split its two emissions:

    create_streaming_table()   always, every source, every lake  -> inventory identical
    append_flow()              only for sources active in this lake -> no definition-time failure

Without this split, a flow referencing a Bronze table that does not exist in the region
fails the pipeline at definition time.

Activity is **declared**, via an `active_sources` variable per target in
`databricks.yml`, passed to the generator as opaque configuration in the same way as
`catalog` and the existing `domains` filter.

Rejected alternative: probing the catalog with `spark.catalog.tableExists()`. Rejected
because it cannot distinguish a source absent by design from one absent by mistake — a
typo, an unprovisioned schema or a permissions failure would yield a silently empty
satellite and a green pipeline. Declaring it makes absence a reviewed decision and makes
a genuinely missing table a loud failure.

A reconciliation check asserts that the set of skipped flows equals the declared-inactive
set, closing the silent-no-op hole.

This preserves the repo's stated invariant that region and environment are not inputs to
the *model*. They are inputs to the *deploy*.

### D6 — Beeline and Bullhorn join the shared model

`01_usnc_bronze_dev` contains `beeline`, `bullhorn_native` and `bullhorn_salesforce`,
none of which the model currently binds to. Beeline is a US job-request source. Both are
added as source bindings on the relevant job-domain entities.

Under D2 this affects all four lakes: every region grows the corresponding satellites,
empty outside the US. Whether `bullhorn_native` and `bullhorn_salesforce` are one source
or two is resolved by inspection (see section 7).

### D7 — usnc_tds becomes the baseline

`default: true` moves from `weu_tds` to `usnc_tds`. The `dev` target (`mode:
development`, for individual engineers) repoints from the `weu_tds` host to the usnc TDS
host. `conformance_check.py`'s `--baseline` default becomes `usnc_tds` and its `TARGETS`
catalog values follow the new convention.

`conformance_check.py` is dormant for this project: it compares two or more lakes, and
there is nothing to compare until a second region deploys. This is a stated condition,
not a skipped gate.

### D8 — uv for dependency management

Dependencies are managed with `uv`, via a committed `pyproject.toml` and `uv.lock`.

The import graph splits along the same line as the two correctness layers, so the
dependency groups mirror it:

| Group | Contents | Needed by |
|---|---|---|
| core | `pyyaml` | `tests/test_accelerator.py`, `verify_repo.py`, `src/accelerator/spec.py` |
| workspace (optional) | `databricks-sdk` | `checks/conformance_check.py` |
| not declared | `pyspark` | supplied by the Databricks Runtime on the cluster |

`pyspark` is deliberately **not** a project dependency. Installing it locally would
invite running pipeline code off-cluster against a version that differs from the
runtime's.

The offline inner loop therefore requires only the core group. Both heavy imports are
already lazy — `checks/apply_governance.py:102` imports `pyspark` inside `main()` after
the `--dry-run` early return, and `checks/conformance_check.py:42` imports the SDK inside
a function — so neither blocks an offline run.

Commands become `uv run python tests/test_accelerator.py` and `uv run python
verify_repo.py`. The README's Getting-this-project block is updated accordingly, since it
currently presents both as runnable immediately after extracting the archive.

---

## 4. Defects found

### DEF-1 — Leading-digit catalog names emit unparseable DDL

`governance/apply_masks.sql` interpolates `${catalog}` unquoted at lines 44, 46 and
110-122. Spark SQL unquoted identifiers must match `[a-zA-Z_][a-zA-Z0-9_]*`; a leading
digit requires backticks. `ALTER CATALOG 02_usnc_silver_edm_dev SET ISOLATION MODE
ISOLATED;` will not parse.

The guard in `checks/apply_governance.py:38` uses `re.fullmatch(r"[A-Za-z0-9_]+", value)`,
which accepts `02_usnc_silver_edm_dev`. So the value passes validation and *then*
produces invalid SQL — the worst ordering, because it appears validated.

`src/accelerator/naming.py:94` already backticks correctly, so generated vault tables are
unaffected.

Fix: backtick `${catalog}` throughout `apply_masks.sql`, and tighten the guard so a
leading digit cannot pass unless the emitting SQL quotes it.

### DEF-2 — Bronze grants target a catalog that will never exist

`apply_masks.sql:116-118` grants on `${catalog}.${bronze_schema}`, which resolves to
`02_usnc_silver_edm_dev.bronze`. Bronze is a different catalog. Resolved by D1.

### DEF-3 — No dependency manifest

The repo tracks no `requirements.txt`, `pyproject.toml` or lockfile. PyYAML is absent
from a clean environment, so both `verify_repo.py` and `tests/test_accelerator.py` fail
at import (`src/accelerator/spec.py:29`). There is currently no red-green loop.

### DEF-4 — Check counts stated three ways

`README.md:45` says `verify_repo.py` runs 151 checks; `README.md:135` says 266;
`CHANGELOG.md:147` says 238. The count should be reported by the code rather than
maintained by hand in three strings. The README also states "33 files in 7 directories";
`git ls-files` returns 56.

### DEF-5 — Uncommitted README edit is broken

The working tree adds:

    Superpowers methodology layer: pinned to obra/superpowers@v<X.Y.Z>
    See tooling/superpowers/CHANGELOG.md for what that pin covers.

The version is an unresolved literal placeholder, `tooling/` does not exist in the repo,
and the installed plugin is `superpowers@claude-plugins-official` v6.3.0, not
`obra/superpowers`. The edit also removes the blank line before the following `---`.

### DEF-6 — All CLI profiles resolve to EU production

Every profile in `.databrickscfg` (`DEFAULT`, `hfig-current-user`, `hfig-current`,
`hfig-current-pat`, `tve-dev`, `hfig-eu-prod`) points at
`adb-4750792675027163` — the EU **production** workspace. Nothing authenticates to
`usnc_tds` (`adb-2593897084138079.19`), and nothing authenticates to any TDS workspace.

`tve-dev` is named like a development profile and reaches EU production. With eight
near-identical workspaces this is precisely the failure `preflight_target.py` exists to
catch, and it is currently armed. Renaming or removing it is its own task.

### DEF-7 — verify_repo.py hardcodes EU and UK TDS hosts

`verify_repo.py:310`, `:315` and `:319` pass literal workspace hosts to
`preflight_target.py` as self-tests: `adb-7405615198748199` (`weu_tds`) and
`adb-718050136221554` (`uks_tds`). Under D7, `weu_tds` is no longer the baseline and
these assertions test a target the project does not use. They must be retargeted to
`usnc_tds` (`adb-2593897084138079`) and a second lake, or the self-test loses its point.

---

## 5. Model scope

The 20 entities split by source region:

**US-sourced (`UKG_US`) — 10:** `hub_accounting_journal`, `hub_company`,
`hub_ledger_account`, `hub_pay_period`, `nhl_journal_line`, `nhl_payroll_detail`,
`sat_accounting_journal_header`, `msat_journal_line_worktag`,
`msat_journal_line_external_code`, `csat_payroll_line_classification`.

**EU-sourced — 10:** `hub_client`, `hub_job_request`, `hub_worker`,
`lnk_client_job_request`, `nhl_timesheet_line`, `sat_job_request_details`,
`sat_job_request_commercials`, `msat_job_request_custom_field`, `msat_worker_skills`,
`csat_job_request_custom_promoted`.

Under D2 all 20 are declared in every lake. Under D5 only those with active sources in a
given lake receive flows. The first US load is scoped to the finance and payroll
entities, which already bind to real US systems and need only the catalog rewrite.

**Naming collision to avoid.** The accelerator's internal `domain:` field on entities
(`party`, `finance`, `payroll`, `job`, `pay_bill`) groups entities for ERD detail pages
and the `domains` filter variable. It is a modelling grouping inside one schema and does
**not** correspond to the estate's domain catalogs. Same word, different level.

---

## 6. Decomposition

1. **Unblock the loop** — uv dependency manifest (D8, DEF-3), check-count
   reconciliation (DEF-4), README pin fix (DEF-5), usnc TDS profile and the `tve-dev`
   trap (DEF-6).
2. **Retarget the bundle** — D1, D7, DEF-1, DEF-2, DEF-7.
3. **Rebind sources** — D3, D5, D6; rewrite all 20 entity `bronze_table:` values against
   real `01_usnc_bronze_dev` schemas.
4. **Raw vault prototype** — first domain end to end, gate zero and the applicable hard
   gates green.
5. **Business vault** — `csat` is already emitted. **PIT and Bridge are net-new
   generator work**: new entity kinds in `spec.py`, new emission in `factory.py`, new
   validation rules. Per `tools/estimate_footprint.py` the PIT snapshot grain is the
   single largest storage lever (daily to month-end is roughly 30x). This warrants its
   own design document.

Sub-project 2 is completable entirely offline. Sub-project 1 is offline apart from
creating the usnc TDS profile, which requires an interactive login only the user can
perform. Sub-project 3 requires that profile, for the inspections in section 7. Sub-
projects 4 and 5 require a deployed pipeline and a load.

---

## 7. Verification required before implementation

Each of these is resolved by inspection once a usnc TDS profile exists. None is a design
question.

1. Table naming inside bronze schemas — whether tables drop the source prefix
   (`fieldglass.job_posting` versus `fieldglass.fieldglass_job_posting`).
2. Whether `<source>_raw` carries a native operation flag, and its column name, per
   source — this sets `cdc_op_column` and `change_detection` per binding (D4).
3. Whether `bullhorn_native` and `bullhorn_salesforce` are one source or two (D6).
4. The full bronze schema inventory; the available screenshot is truncated at `freshdesk`.
5. Whether catalog isolation is already bound. `databricks.yml` asserts one metastore per
   region, so PROD catalogs should be visible from the TDS workspace; they are not. Either
   binding is already configured — making the isolation statement in `apply_masks.sql` a
   no-op — or the shared-metastore premise does not hold for this estate.

---

## 8. Definition of done

Per `.claude/skills/dv-accelerator-gates`, structural and behavioural correctness are
separate layers and neither substitutes for the other.

**Inner loop (offline, seconds):** `tests/test_accelerator.py`, `verify_repo.py`,
`checks/apply_governance.py --dry-run`, and the pure-Python reference half of
`hash_parity_check.py` against `tests/golden_hash_vectors.json`. All four are blocked
today only by DEF-3, and all four run under `uv run` with the core group alone (D8).

**Outer loop (workspace-bound):** `preflight_target.py` needs only the profile, no data.
The Spark half of `hash_parity_check.py`, `append_only_check.py`,
`loop1_reconciliation.py`, `mask_survival_check.py`, `journal_integrity_check.py` and
`aggregate_reconciliation_check.py` all need a deployed pipeline and a load.

**Test-first.** Each change adds its structural check before the edit:

- before the catalog rewrite — a check asserting no entity references an `hfig_*` catalog
- before DEF-1 — a check asserting every `${catalog}` in `apply_masks.sql` is backticked,
  plus an `apply_governance` case feeding it `02_usnc_silver_edm_dev` and expecting
  rejection or quoting
- before D5 — a check asserting the factory emits `create_streaming_table` for an inactive
  source but no `append_flow`

**Outstanding-gate ledger.** Work that does not touch a workspace names the gate it could
not run:

| Change | Offline proof | Gate outstanding |
|---|---|---|
| Dependency manifest | suites execute | — |
| Three catalog variables | `bundle validate --strict -t usnc_tds` | `preflight_target` |
| DEF-1 backtick and guard | `apply_governance --dry-run` | `mask_survival_check` |
| 20 entity rewrites | `test_accelerator.py` | gate zero, `loop1_reconciliation` |
| Beeline / Bullhorn bindings | `test_accelerator.py` | gate zero |
| Metadata-declared masks | structural only | `mask_survival_check` — sole PII defence |
| `_raw` and `cdc_op` binding | none | `append_only_check`, `loop1_reconciliation` |

A sub-project is done when the offline suites are green, `bundle validate --strict` is
clean, and every outstanding gate is named in the commit message.

---

## 9. Out of scope

- TDS-to-PROD deployment pipelines — deferred by decision.
- Folding the domain-split silver and gold catalogs into the EDM as hubs.
- Gold generation. `03_usnc_gold_edm_dev` does not yet exist; Gold is downstream
  projection, not vault structure.
- Delta Sharing of Gold to the EU Datalakehouse-Global workspace. Note that US and APAC
  Gold reaching an EU workspace is a governance question for WISE intake, and is worth
  raising before go-live rather than at it.
- PIT and Bridge generation — sub-project 5, requires its own design document.
- Effectivity, record-tracking and status-tracking satellites; the metrics vault.

Note: the README lists effectivity satellites as not built, but `spec.py:417` validates
`esat.driving_key` and the factory handles `esat` within the satellite family. Document
and code disagree; resolving that is not in scope here but should not be assumed either
way.

---

## 10. Inspection results (2026-08-24, profile `hfig-usnc-tds`)

`preflight_target.py --target usnc_tds` **PASSED** — the first gate in this effort to execute.

### 10.1 Answers to section 7

**1. Bronze table names do NOT carry the source prefix.** `01_usnc_bronze_dev.ukg_raw.gl`,
not `ukg_raw.ukg_gl`. Every entity's `bronze_table:` therefore becomes
`01_usnc_bronze_dev.<source>_raw.<table>`.

**2. `_raw` carries no operation flag.** `ukg_raw.gl` has 13 columns and none of them is a
CDC operation column. Its shape is Auto Loader landing output — `rescued_data`,
`input_file_name`, `timestamp`. So `change_detection: cdc` cannot be used for UKG until the
estate's Auto CDC migration reaches it; `antijoin` is the interim path.

**3. Bullhorn: `bullhorn_native` is authoritative.** 31 tables against
`bullhorn_salesforce_raw`'s zero. One source binding, not two.

**4. Full bronze inventory: 47 schemas**, 21 of them `_raw`. Populated `_raw` schemas:

| Schema | Tables | | Schema | Tables |
|---|---|---|---|---|
| `fieldglass_raw` | 302 | | `microsoft_dynamics_raw` | 13 |
| `beeline_raw` | 120 | | `fieldglass_client_owned_raw` | 12 |
| `vndly_raw` | 35 | | `onestaff_analytics_raw` | 6 |
| `bullhorn_native_raw` | 31 | | `hubspot_raw` | 6 |
| `great_plains_raw` | 19 | | `ukg_raw` | **1** |

Empty: `prounity_raw`, `bullhorn_salesforce_raw`, `sap_fieldglass_raw`, `beeline_api_raw`,
`beeline_client_owned_raw`, `cnet_raw`, `topaz_raw`, `sun_raw`, `snaplogic_raw`,
`freshdesk_raw`, `test_raw`.

**5. No PROD catalog is visible from the TDS workspace.** Catalogs listed are the domain-split
`02_*`/`03_*` set plus `01_usnc_bronze_dev`, `02_usnc_silver_edm_dev`, `db_usnc_datalakehouse_tds`,
`system` and `samples`. Either isolation is already bound or PROD sits in a different metastore.
Either way the isolation statement in `apply_masks.sql` is not load-bearing here today.

`03_usnc_gold_edm_dev` **does not exist**, confirming the decision to remove the gold grants
rather than ship a statement that would fail on first deploy.

### 10.2 D3 is confirmed by the estate

`ukg_raw.gl` is a `STREAMING_TABLE`. `ukg.gl` is a `MATERIALIZED_VIEW` with byte-identical
columns — no masking, no transformation. This independently supports D3 on mechanical grounds
as well as governance ones: the vault needs a streaming source, `_raw` is one, and a
materialized view is not.

### 10.3 BLOCKER for sub-project 3 — the finance/payroll model does not match the source

Ten entities bind to six UKG tables: `ukg_journal_header`, `ukg_journal_line`,
`ukg_journal_line_external_code`, `ukg_journal_line_worktag`, `ukg_pay_period`,
`ukg_payroll_register_detail`. **None exists.** UKG has exactly one table, `ukg_raw.gl`:

```
company, batchid, paycheckdate, payperiodstartdate, payperiodenddate,
account, debit, credit, reference, userdefined1,
rescued_data, input_file_name, timestamp
```

Consequences, each of which invalidates part of the committed model:

- **No worker column.** `nhl_payroll_detail` is declared at worker x period x code grain. That
  grain does not exist in this source, so `aggregate_reconciliation_check.py` — which reconciles
  payroll detail against the GL journal in both directions — has no transaction-grain counterpart
  to reconcile against.
- **No line ordering.** `nhl_journal_line` declares `line_order` as a dependent child key and
  `journal_integrity_check.py` asserts it is dense and unique. There is no such column.
- **No worktags, no external codes.** `msat_journal_line_worktag` and
  `msat_journal_line_external_code` have no source columns.
- **No `ControlTotalAmount`.** `journal_integrity_check.py` asserts line counts agree with it.
- **Pay period is not an entity** — it is two date columns on `gl`, so `hub_pay_period` has no
  independent source.

**`debit` and `credit` are `DOUBLE`.** `journal_integrity_check.py` asserts
`SUM(debits) = SUM(credits)` per journal. Floating-point summation makes that equality
unreliable at scale — the gate will fail on rounding, not on real imbalance. Financial amounts
want `DECIMAL`. This needs settling before the gate is trusted.

The model was designed from spike documents rather than from this bronze. Sub-project 3 must
reconcile the two before any entity is rebound, and that is a modelling exercise, not a
find-and-replace of catalog names.
