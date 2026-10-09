# Sub-project 3 — Source rebinding and the party model

Date: 2026-08-24
Status: REVISED after adversarial review; implementation plan not yet written
Revision: 2 -- two claims in revision 1 were false and are withdrawn in section 2.6
Parent spec: `docs/superpowers/specs/2026-08-24-usnc-tds-retarget-design.md`
Accelerator: v0.2.0 · hash rulebook 1.0.0 (unchanged by this work)

---

## 1. Why this is not the sub-project the parent spec described

The parent spec scoped sub-project 3 as mechanical: rewrite twenty `bronze_table:`
values, add `active_sources`, add Beeline and Bullhorn bindings, delete
`src/pipelines/bronze_ingest.py`.

Inspecting the live workspace showed the model and the sources disagree structurally.
This document records what is actually there and the decisions taken in response. The
mechanical rebinding survives as the last step, not the first.

---

## 2. What the sources actually are

Inspected 2026-08-24 against `01_usnc_bronze_dev` via profile `hfig-usnc-tds`.

### 2.1 The VMS sources are partitioned per client

| Source | `_raw` tables | Shape |
|---|---|---|
| `fieldglass_raw` | 302 | `io_<entity>_<client>`, **38 distinct clients**, plus `_backfill` twins |
| `beeline_raw` | 120 | same pattern, **13 clients** |
| `vndly_raw` | 35 | same pattern |
| `bullhorn_native_raw` | 31 | **normalised** — `candidates`, `joborders`, `jobsubmissions`, `placements`, `clientcorporations`, `clientcontacts`, `timesheets`, `skills` |

Fieldglass entity types: `io_jobposting` (62), `io_timesheet` (62), `io_worker` (62),
`io_distributed_jobposting` (58), `io_jobseeker` (58). Zero non-`io_` tables. Those counts
are TABLES, not clients -- 38 distinct clients, unevenly covered across entity types.

`fieldglass_client_owned_raw` holds a further 12 tables (`co_*`) and is populated.

**`great_plains_raw` (19 tables) is a normalised General Ledger** and revision 1 omitted it
entirely: `gl20000` (72 columns, `debitamt`, `crdtamnt`, `curncyid`, correcting-JE flags),
`gl10110` / `gl10111`, `gl30000` history, `gl00100` account master, `fiscalperiods`, plus
payables (`pm*`) and receivables (`rm*`) modules. This is line-grain journal with an account
master -- the shape E8 argued does not exist. See E8.

`bullhorn_salesforce_raw`, `sap_fieldglass_raw`, `prounity_raw`, `beeline_api_raw`,
`beeline_client_owned_raw`, `cnet_raw`, `topaz_raw`, `sun_raw`, `snaplogic_raw`,
`freshdesk_raw` and `test_raw` are all empty.

### 2.2 The tenant is in the data

Every VMS table carries `buyer` as its first column. The tenant does not have to be
parsed from the table name.

`io_worker` is not a worker table — it is a 73-column denormalised extract where one
row carries `job_posting_id`, `job_seeker_id`, `worker_id` and `work_order_id` together
with rates, dates, statuses, `supplier` and `parent_supplier`. One staged source feeds
several hubs and links.

### 2.3 No source carries a CDC operation column

Every `_raw` table is Auto Loader landing output: `input_file_name`, `timestamp`,
`rescued_data`, and nothing else. `change_detection: cdc` — the default, declared by
four entities today — cannot work against any of them as they stand.

### 2.4 UKG delivers one report, not six tables

The model binds ten finance and payroll entities to six UKG tables. **None exists.**
`ukg_raw.gl` is the only UKG table:

```
company, batchid, paycheckdate, payperiodstartdate, payperiodenddate,
account, debit, credit, reference, userdefined1,
rescued_data, input_file_name, timestamp
```

Profiled: 520 rows, one company (`BRPLM`), one batch, one paycheck date, 101 accounts,
79 distinct `userdefined1` values. Grain is
`company x batchid x account x userdefined1`, at exactly two rows per key — the debit
side and the credit side (260 keys x 2 = 520 rows).

`userdefined1` packs four fields into a delimited string:

```
"D - FSADC - Ded - FSA Dependent"      type - code - category - description
```

There is no worker, no line ordering, no worktag, no external code and no control
total. This is a report extract, not the source object model.

### 2.5 DOUBLE amounts: a hashing problem, not a gate failure

```
SUM(debit) = SUM(credit)  as DOUBLE           -> false
SUM(debit) = SUM(credit)  as DECIMAL(18,2)    -> true
residual                                       -> -1.7462298274040222E-10
```

The warehouse `=` comparison fails. **The gate does not.**
`checks/journal_integrity_check.py:43` sets `TOLERANCE = "0.005"` and line 69 tests
`ABS(...) > TOLERANCE`, so a residual of 1.7e-10 is seven orders of magnitude inside
tolerance. Revision 1 of this document reported the `=` result as a gate failure. That was
wrong, and it was asserted as demonstrated.

What survives is the hashing concern. `factory.py:181` computes `hashdiff_expr` over the
payload, and a `DOUBLE` rendered to string is not stable across loads, so identical amounts
can yield different hashdiffs and therefore spurious satellite rows. That is a real reason
to cast, and it is now the **only** reason -- see E7, which also gains a generator
capability revision 1 did not list.

### 2.7 `_raw` contains overlapping re-deliveries

`great_plains_raw.gl20000`: 4,444,172 rows across 7 file deliveries, but only 2,453,131
distinct `(input_db, openyear, jrnentry, seqnumbr)`. Adding `input_file_name` recovers
near-uniqueness (4,431,076), so the duplication is successive full extracts landing on top of
one another. **Every business row is present roughly 1.8 times.**

This is not a Dynamics GP quirk. `fieldglass_raw.io_timesheet_air_liquide` shares 1,670
`time_sheet_id` values with its `_backfill` twin. Overlapping re-delivery is how `_raw`
behaves across the estate, and it is a direct consequence of `_raw` being append-only landing
for whole-file extracts.

Two consequences, both of which precede any change-detection question:

- A satellite bound to `gl20000` takes ~1.8 copies of every row on its **first** load.
- `nhl_journal_line` bound to `gl20000` fails `append_only_check.py`'s NHL uniqueness
  assertion immediately, since NHLs have no hashdiff to collapse repeats.

Deduplication at the staging boundary is therefore mandatory for every binding, not a
Fieldglass-specific concern. See E11 and section 5.

### 2.6 Claims withdrawn from revision 1

Both were load-bearing and both were false.

**"The journal integrity gate fails on DOUBLE."** It passes. See 2.5.

**"Satellites are protected by hashdiff."** They are not. `factory.py:306-315` filters
`cdc_op IN ('I','U')` and performs no hashdiff comparison at all -- decision 3 in
`factory.py` states plainly that a streaming table cannot read itself. Only the `antijoin`
path compares hashdiff. Combined with 2.3 (no source carries a CDC column) and
`factory.py:107` falling back to `F.lit("I")`, the true position is far worse than what
revision 1 described, and it is promoted to a decision of its own: see E11.

---

## 3. Decisions

### E1 — Bronze stays 1:1 with its sources; the vault consolidates

Bronze mirrors what each source delivers. Multiple Fieldglass clients are a faithful
reflection of the source, not a defect to be flattened upstream. Consolidation of client
identity is what the Data Vault is for.

Rejected: asking the bronze team to publish per-entity unions. It would put modelling
into a layer whose contract is fidelity to the source.

### E2 — A Fieldglass client is a TENANT within one source, keyed on a Buyer ID

Fieldglass is one source. `hub_job_request` fed by its client tables is N append flows into
one hub table — the multi-source pattern the accelerator already documents. The satellite
stays one per source, "shared by all clients", as the parent README states.

`key_style: tenant_scoped` is **mandatory** for VMS-sourced entities. Without it, two
clients' job postings carrying the same reference merge into one hub row, silently and
irreversibly.

**The tenant key is a Buyer ID that does not yet exist in the feed.** The only
buyer-bearing column in any Fieldglass table is `buyer`, and its values are display names
(`'Air Liquide'`). Keying on a display name means a cosmetic rename in Fieldglass re-keys
every job request, timesheet and worker assignment for that client — a mass re-key
triggered by an edit that changes nothing real.

This is the same defect as E8's: an extract omitting an identifier the source system holds.
**Ask the bronze team to add the Buyer ID to the Fieldglass feed** (section 6). Until it
lands, VMS entities cannot be bound — `tenant_key` has nothing stable to point at.

Rejected: normalising the display name (a rename still forks it); using the bronze table-name
suffix (equally mutable, and derived from a naming convention rather than from data);
accepting the fork and resolving downstream (correct for `hub_organisation`, too sharp an
edge for `hub_job_request`).

### E3 — One `hub_organisation`, keyed on (reference_type, reference_id), `authored`

`company` in the GL is **our own legal entity**, not a client. `hub_company` and
`hub_client` fold into a single `hub_organisation`:

```
('Organization_Reference_ID', 'BRPLM')       our legal entity, from UKG
('Portal_Client_Code',        'ACME')        client, from Client Portal
('Fieldglass_Buyer',          '<buyer id>')  tenant, from Fieldglass (pending E2)
```

**Key style is `authored`, and E4 is what makes that safe.** `key_style` is per entity
(`spec.py:81`), so one style must cover all three bindings. It does not need to be
`federated` or `tenant_scoped`, because the reference type — supplied per binding as a
literal — already scopes the value. `('Organization_Reference_ID','BRPLM')` cannot collide
with `('Fieldglass_Buyer', …)` whatever the key style. Choosing `tenant_scoped` here would
prepend the source name to UKG's key as well, silently discarding the `authored` decision
that `hub_company.yml` documents at length.

A tenant hub is not itself tenant-scoped; that is circular. E2's `tenant_scoped` governs
job and worker entities, not this hub.

The vault holds several rows for one real-world organisation until they are resolved. That is
correct: a hub records asserted identities, not resolved ones. Cross-source resolution is a
same-as link, downstream in ML entity resolution, which the parent README places outside this
accelerator.

**Blast radius — seven entities re-key, not one.** Revision 1 named only
`lnk_client_job_request`. The full set:

| Entity | How it depends on `client` or `company` |
|---|---|
| `lnk_client_job_request` | parent `client` |
| `nhl_timesheet_line` | parent `client` (three-parent NHL) |
| `nhl_journal_line` | parent `company` |
| `nhl_payroll_detail` | parent `company` |
| `msat_journal_line_worktag` | `parent_keys` reach `company` |
| `msat_journal_line_external_code` | `parent_keys` reach `company` |
| `hub_pay_period` | `business_keys` include `company_reference_id` |
| `csat_payroll_line_classification` | `parent_keys` reach `company` |

Every one is re-keyed through `_hub_key_expr` / `_link_key_expr`. Under parent decision D2
this applies to all four lakes, and EU's bindings for `hub_client` have not been inspected.
**Inspect EU before applying**, or the change lands sight-unseen on a model we have not read.

### E4 — `reference_type` is a constant supplied per source binding

No source carries a type column: UKG gives a bare `company`, Fieldglass a bare `buyer`.
The binding supplies the type as a literal key component — `UKG_US` contributes
`Organization_Reference_ID`, `FIELDGLASS_US` contributes `Fieldglass_Buyer`.

**The rulebook claim holds; the factory does not.** `hashing.hash_key(["'Fieldglass_Buyer'",
"buyer"])` normalises a quoted literal exactly as it normalises a column
(`UPPER(TRIM(CAST(...)))`), so `RULEBOOK_VERSION` stays at 1.0.0 and the golden vectors are
untouched. But three things break downstream and revision 1 named none of them:

- `factory.py:162` builds the readable business key with `F.col(c)` over `src.key_columns`.
  `F.col("'Fieldglass_Buyer'")` is an unresolved column at runtime.
- `hashing.key_safety_rules` (`hashing.py:141`) strips backticks, not quotes, producing an
  expectation named `key_present_'Fieldglass_Buyer'` whose rule is a tautology.
- `hash_key` interpolates `key_columns` raw, with no validation that a key component is
  either a valid identifier or a properly quoted literal.

So the literal needs first-class support — a distinct `key_literals:` construct on the
binding rather than a quoted string smuggled into `key_columns`.

### E5 — Role is expressed by link participation, not stored

An organisation is a client or a supplier by virtue of the links it participates in.
No role satellite, no role column on the hub — the latter is refused by validation
anyway.

This is the reversible direction. Adding a role satellite later is additive and needs
no re-key. Storing role first and then discovering an organisation is both client and
supplier requires a multi-active retrofit and reconciliation of rows already written.

**The tell:** the first time "list all suppliers" needs answering and only a link
traversal can answer it, add the satellite.

### E6 — Deactivate what has no source; delete nothing

D2 in the parent spec requires an identical model in all four lakes, so entities cannot
be deleted for the US alone. `nhl_payroll_detail`, `csat_payroll_line_classification`,
`msat_journal_line_worktag` and `msat_journal_line_external_code` stay declared and are
made inactive in usnc through the `active_sources` mechanism (parent spec D5).

Of the ten entities in the finance and payroll domains, `hub_company` is **not** among those
retained as-is — E3 folds it into `hub_organisation`. Of the remaining nine, under E8's
Dynamics GP binding:

- **Sourceable:** `hub_accounting_journal` (`jrnentry`), `hub_ledger_account` (`gl00100`),
  `nhl_journal_line` (`gl20000`, keyed on `seqnumbr`), `sat_accounting_journal_header`.
- **Unsourced:** `nhl_payroll_detail` and `csat_payroll_line_classification` — GP carries no
  payroll module; `msat_journal_line_worktag` and `msat_journal_line_external_code` — no
  worktag or external-code columns; `hub_pay_period` — `fiscalperiods` is a fiscal period, not
  a pay period, and binding it would conflate two business concepts.

Revision 1 counted differently on both sides, having missed the `line_order` blocker and
`great_plains_raw` alike.

Deactivation is not deletion and is not permanent: when E8's feed lands, the four become
active through a metadata change, with no structural change and no re-key.

Also:

- **`aggregate_drops: [worker, pay_period]` stays exactly as declared.** Revision 1 called it
  factually wrong on the grounds that `payperiodstartdate` is present on `gl`. That reasoning
  was itself wrong: `aggregate_drops` names **parent hubs of the raw counterpart** that are
  summed away, not columns. `nhl_payroll_detail.parents` is `[worker, company, pay_period]`,
  so `{worker, pay_period}` is a valid subset, disjoint from `journal_line`'s own parents, and
  `spec.py:483-500` accepts it. Changing it to `[worker]` would have been a regression.

  The open question is narrower and is with the SME: **does one journal batch correspond to
  exactly one pay period?** The data cannot answer it — the sample holds a single batch. If it
  does, the declaration over-states what was aggregated and `pay_period` arguably belongs as a
  fourth parent, which would **re-key the NHL**. Until the SME answers, leave it: it is valid,
  and it is the conservative reading, since it prevents a worker being attached to an
  account-level total — the failure the whole aggregate mechanism exists to stop.
- **The `line_order` blocker dissolves under E8.** `nhl_journal_line.transaction_key` is
  `[journal_line_external_reference_id, line_order]`, which `ukg_raw.gl` could not supply.
  `great_plains_raw.gl20000.seqnumbr` is the line sequence, so
  `journal_integrity_check.py:110-119`'s dense-and-unique assertion becomes satisfiable —
  once staging deduplicates the re-deliveries of section 2.7.
- `csat_payroll_line_classification` is repointed at `nhl_journal_line`. `userdefined1`
  is the payroll code it exists to classify and it is available today.

### E7 — Cast amounts to DECIMAL(18,2) at the staging boundary

**For hashdiff stability only.** Revision 1 claimed two independently sufficient reasons; the
gate reason was false (section 2.5) and is withdrawn.

The remaining reason is sufficient on its own: `factory.py:181` hashes the payload, and a
`DOUBLE` rendered to string is not stable across loads, so identical amounts can produce
different hashdiffs and therefore spurious satellite rows forever.

**This needs a generator capability section 4 did not list.** `_stage` reads the source table
and adds system columns; it performs no type coercion, and `hashdiff_expr` hashes payload
*column names*. A cast only helps if the cast column replaces the payload column — that is
per-binding type coercion, a fourth new capability. Note also that a type change under an
unchanged payload name is invisible to `spec.py`'s `check_payload_order`, so nothing in the
repo would catch a silent reversion.

### E8 — Dynamics GP is the book of record; finance binds to `great_plains_raw`

**UKG is fed from GP.** `ukg_raw.gl` is therefore a downstream report of a downstream system
— two removes from the book of record — which is precisely what E1's fidelity principle
argues against. Revision 1 proposed commissioning a UKG API integration to recover a
normalised shape. That work is unnecessary: the shape is already landed.

`great_plains_raw` supplies what the UKG report could not:

| Model need | GP source | Note |
|---|---|---|
| `line_order` | `gl20000.seqnumbr` | the blocker recorded in E6 dissolves |
| journal identity | `gl20000.jrnentry` | 794,697 entries |
| account master | `gl00100` | `actindx`, `actnumbr_1..5` segments, type, active |
| legal entity | `gl20000.input_db` | 11 company databases |
| period | `fiscalperiods` | `periodid`, `perdendt`, `year1`, `glclosed`, `prclosed` |
| correction lineage | `correcting_je`, `back_out_je`, `original_je`, `voided` | absent from the UKG report |
| currency | `curncyid`, `xchgrate`, `denxrate`, `orcrdamt`, `ordbtamt` | absent from the UKG report |

`ukg_raw.gl` is not rebound; it is superseded. Whether it retains value as a
reconciliation cross-check against GP is a separate question, not decided here.

**Three open items this decision does not settle.**

**The journal-line key is not yet established.** `(input_db, openyear, jrnentry, seqnumbr)`
yields 2,453,131 distinct values against 4,444,172 rows, and neither `rctrxseq`, `ledger_id`
(constant — one ledger) nor `actindx` discriminates further. The residual is re-delivery
(2.7), not a finer grain, so the natural key is likely correct once staging deduplicates —
but that must be demonstrated, not assumed, before `nhl_journal_line` is keyed.

**`fiscalperiods` is a FISCAL period, not a pay period.** Accounting months are not payroll
cycles. `hub_pay_period` must not bind here merely because a period entity exists — that
conflates two business concepts, the same class of error as `company` versus client. This
goes to the SME alongside the `aggregate_drops` question, and until answered `hub_pay_period`
stays unsourced.

**GP carries no payroll module.** The feed is GL, PM (payables), RM (receivables) and SY
(system); there are no `UPR*` tables. So `nhl_payroll_detail` remains unsourced under GP,
`csat_payroll_line_classification` with it, and
`checks/aggregate_reconciliation_check.py` stays dormant. Choosing GP as book of record does
not solve the payroll-detail gap; that needs its own answer, and it is the one place a UKG
feed may still be the right route.

**`input_db` as the legal-entity key is an inference.** Eleven databases matches the GP
convention of one company per database, which would make it the natural
`Organization_Reference_ID` under E3. The count is evidence; it is not a statement by the
data. Confirm before keying.

### E9 — Bullhorn binds first

`bullhorn_native` is normalised, needs no union and no tenant scoping, and maps onto the
model as it stands. It is the one source that can load without waiting on anything.
`bullhorn_salesforce` is empty and is not a second binding.

### E10 — The Fieldglass high-variability entities have no US source; deactivate them

`msat_job_request_custom_field` and `csat_job_request_custom_promoted` implement the parent
README's flagship pattern for Fieldglass's per-client customisation. Revision 1 never
mentioned them.

**There is no custom-field feed in US bronze.** `fieldglass_raw`'s 302 tables are exactly the
five `io_` entity types with zero unmatched, and the premise itself does not hold here:
`io_jobposting_air_liquide` and `io_jobposting_albertsons_co` have **byte-identical
43-column sets**. Whatever per-client customisation exists in Fieldglass, this extract does
not carry it.

Both entities are therefore inactive in usnc under E6's mechanism. They stay declared, since
D2 governs all four lakes and EU may well have the custom-field feed the pattern was designed
against.

This also answers, with evidence, the question of whether custom attributes should go to one
multi-active satellite or to a satellite per tenant: **neither is buildable in usnc today**,
because there are no custom attributes in the feed. The recorded multi-active design stands
unchallenged until a source contradicts it.

### E11 — Change detection is the blocking defect, and `antijoin` is the only path

Section 2.6 withdrew the claim that satellites are protected by hashdiff. The true position:

- no `_raw` source carries a CDC operation column (2.3)
- `factory.py:107` therefore falls back to `F.lit("I")`
- `factory.py:315`'s `changed_only` filter passes every row
- the `cdc` path performs **no hashdiff comparison at all**

So **every satellite re-appends every row on every run.** Not NHLs — satellites, which are
the bulk of the model. And section 2.7 makes it worse: `_raw` already holds roughly 1.8
copies of each business row from overlapping re-deliveries, so the duplication starts on the
first load, before any second run. `append_only_check.py` would still pass, because appending is exactly
what it permits; `loop1_reconciliation.py` would still balance. The vault would fill with
duplicate versions and no gate would object.

`change_detection: antijoin` is the only path that compares hashdiff, and the parent README
flags it as unproven in this workspace with a batch-into-streaming-table caveat. It therefore
stops being an alternative and becomes **mandatory until CDF lands on `_raw`** (section 6),
and proving it is the first task of this sub-project, not a day-1 nicety.

---

## 4. New generator capabilities

| Capability | Why |
|---|---|
| A source binding spanning **N physical tables** | E2 — 62 client tables into one target, `buyer` read from the data |
| A **literal key component** per binding | E4 — `reference_type` has no source column |
| `active_sources` per target | Parent D5, now load-bearing for E6 rather than merely useful |

| A **literal key component** as a first-class construct | E4 — a quoted string in `key_columns` breaks `factory.py:162`, `key_safety_rules` and validation |
| **Per-binding type coercion** | E7 — `_stage` does no casting, and `hashdiff_expr` hashes payload column names |

None of these changes the hash rulebook: a literal key component normalises exactly as a
column value does, so `RULEBOOK_VERSION` stays at 1.0.0 and the golden vectors are unaffected.

**But that reassurance answers the wrong risk.** E2, E3 and E4 each change key *composition*.
`spec.py` has `check_payload_order` for satellite payloads and a `rulebook_version` pin, and
**nothing that guards `business_keys` or `parent_keys` against change.** Because the rulebook
does not move, the repo's own safety net cannot see the largest change in this document. A
key-composition guard — a committed digest of each entity's key definition, failing the build
when it changes without acknowledgement — should be added alongside these capabilities.

---

## 5. Blocking prerequisites

**Staging deduplication** (2.7). `_raw` holds overlapping re-deliveries — ~1.8 copies of each
business row in `gl20000`, and 1,670 shared ids between a Fieldglass timesheet table and its
backfill. No binding is safe until staging collapses them, and this is independent of change
detection: it bites on the first load, not the second. Satellites would take duplicate
versions; NHLs would fail `append_only_check.py` outright.

**Change detection** (E11). Until `antijoin` is proven or CDF lands, every satellite
re-appends every row on every run and no gate objects.

**Mask survival through projection** (parent D3). The vault holds unmasked PII at rest,
defended only by Unity Catalog column masks. If masks do not propagate through `_v1`, PIT and
Gold, personal columns must move to satellites Gold never projects — a modelling change, so
it must land before entities are finalised.

**NHL re-run duplication — no longer hypothetical, and the mechanism is not what revision 1
said.** `dropDuplicates` (`factory.py:320`) operates within a single flow; base and
`_backfill` are separate `SourceBinding`s and therefore separate flows, so it could never
deduplicate across them regardless of batching. Revision 1 also claimed "nothing to catch
them" — `append_only_check.py:76-98` does assert NHL uniqueness, so the gate would fail the
load rather than silently corrupt it.

It needs no proving: `io_timesheet_air_liquide` (5,440 rows) shares **1,670 `time_sheet_id`
values** with its `_backfill` (22,201 rows). Loading both as declared will fail the
uniqueness check. A deduplication or precedence rule between base and backfill is required
before any Fieldglass timesheet binding.

## 6. Asks of the bronze team, and constraints to record

### 6.1 Three asks, one conversation

**Change Data Feed on `_raw`.** `_raw` is becoming an Auto CDC target, which removes the
vault's only append-only tier and collapses half of parent decision D3: `_raw` stays
pre-masking, so the hash-stability argument holds, but it stops being streamable and its
operation flag is consumed rather than emitted. `delta.enableChangeDataFeed` gives
`_change_type` as a real change stream and retires E11's dependence on an unproven
`antijoin`. It is a table property — cheap while the migration is being built, awkward after.

**A Buyer ID on the Fieldglass feed** (E2). The only buyer-bearing column is a mutable
display name. Without a stable identifier, VMS entities cannot be keyed.

**A payroll-detail feed.** Superseding revision 1's UKG entity ask: Dynamics GP is the book
of record for finance (E8), but GP carries no payroll module, so the worker x pay period x
code grain has no source in either system as landed. This is the one place a UKG feed may
still be the right route, and it is what `nhl_payroll_detail` and
`aggregate_reconciliation_check.py` wait on.

All three are the same defect in different clothes: an extract omitting an identifier or a
signal the source system holds.

### 6.2 Fieldglass history is truncated

The feed is extracted from a fixed start date, so satellite histories begin mid-life and
`nhl_timesheet_line` lacks earlier lines. Revision 1 stated 1-Jan-2023; the earliest
`time_sheet_start_date` observed in `io_timesheet_air_liquide_backfill` is `01/01/2024`, so
the boundary is not the one recorded and should be established per feed rather than assumed.

This is a known functional gap in the feed, not a vault defect, and it must be stated wherever
the vault's history is described — "the vault has it" will otherwise be assumed.

### 6.3 Dates arrive as strings

VMS date columns are `MM/dd/yyyy` **strings** with inconsistent zero-padding (`12/8/2025`
alongside `01/01/2024`). Any date entering a business key or a hashed payload is unstable
under that inconsistency, which is the same class of problem as E7's `DOUBLE`. Parsing rules
belong in the same per-binding coercion capability.

### 6.4 A pre-existing generator bug

`_mandatory_rules` (`factory.py:227`) calls `key_safety_rules(src.key_columns)` for links and
NHLs, but link bindings declare `parent_keys` and leave `key_columns` empty. Links and NHLs
therefore receive **no key-component safety rules at all**, contrary to the docstring. Not
caused by this sub-project; it should be fixed within it, since this work adds link bindings.

## 7. Sequencing

Revised: nothing binds until change detection works.

1. **Staging deduplication and change detection** (2.7, E11). `_raw` already holds ~1.8
   copies of each row, so dedup bites before change detection does. Prove `antijoin` or
   obtain CDF on `_raw`. Nothing below is worth doing first.
2. **Generator capabilities** (section 4) — N-table binding, first-class literal key
   component, per-binding type coercion, `active_sources`, and the key-composition guard.
3. **Bullhorn** (E9) — normalised, no union, no tenant scoping. The first real binding.
4. **Party model** (E3, E4, E5) — `hub_organisation`, fold in `hub_company` and `hub_client`,
   repoint the seven dependent entities. **Inspect EU's bindings before applying**, since D2
   propagates this to a model we have not read.
5. **Finance** (E6, E7, E8) — bind `hub_accounting_journal`, `hub_ledger_account`,
   `nhl_journal_line` and `sat_accounting_journal_header` to `great_plains_raw`; demonstrate
   the journal-line key once staging deduplicates; cast amounts; deactivate the five unsourced
   entities. Not gated on another team. `hub_pay_period` and `aggregate_drops` wait on the
   SME; `nhl_payroll_detail` waits on a payroll-detail feed.
6. **VMS binding** (E2, E10) — gated on the Buyer ID arriving and on a base/backfill
   precedence rule (section 5).

Steps 1 to 5 need nothing from another team beyond the CDF decision. Step 6 does.

## 8. Out of scope

- The bronze Auto CDC migration and the CDF request — another team's work; section 6
  records what to ask for.
- Cross-source organisation resolution — a same-as link, downstream in ML entity
  resolution.
- The EU-first `DEPLOY.md` runbook rewrite, carried from the parent spec.
- PIT and bridge generation — parent sub-project 5, still requiring its own design.
- Extending `hub_pay_period` versus the ERD's `ref_period` split, which the parent
  CHANGELOG still lists as unagreed.
