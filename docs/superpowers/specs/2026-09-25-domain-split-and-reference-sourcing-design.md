# Domain-split vault load, and a real source for reference data

**Status:** awaiting review
**Date:** 25 September 2026
**Supersedes nothing. Prerequisite for:** getting the Ameren invoice family loaded end to end.

## Why

Two problems, measured today on `usnc_tds`, that the current single-pipeline
topology cannot answer.

### 1. One pipeline declares the whole model, and dies as a whole

`raw_vault` declares **34 entities → 38 streaming tables, 54 append flows**, plus
nine definition-time schema analyses (one per masked entity, each analysing its
full staged plan only to read a type). Everything is in one graph, so:

* A `SpecError` in one entity kills all 38. That happened **four times this
  afternoon** -- an untyped mask, a case-only alias collision, a NOT NULL derived
  column, and a missing table -- each time taking the other 37 down.
* The driver spent **30+ minutes in `INITIALIZING` at 50-57% GC** and was lost
  twice (`Communication lost with driver. Cluster was not reachable for 120
  seconds`). All of that is before a single row is read, so it is graph
  construction, not data.
* The model is expected to reach ~100 entities.

`src/pipelines/silver_vault.py:90` already filters entities by domain:

```python
if DOMAINS:
    model.entities = [e for e in model.entities if e.domain in DOMAINS]
```

Every entity already declares a domain. The filter has never been used: `var.domains`
defaults to `""`, so one pipeline declares everything. The machinery exists; the
topology does not use it.

### 2. Reference data has no source at all

All eleven `reference` entities bind to `PLACEHOLDER.reference.*`, a catalog that
exists nowhere. `01_usnc_bronze_dev` holds no Workday, portal or reference schema.
`tools/land_workday_references.py` produces NDJSON and says so itself: *"IT LANDS
NOTHING ITSELF. Where these files go is unowned."*

So "load reference data first" is not a sequencing preference that the job graph can
satisfy -- there is nothing to load.

## Decisions taken

Recorded here because they were made in conversation and the reasoning should not
live only there.

1. **Split the raw vault by domain**, one pipeline per domain.
2. **Generate the pipeline resources from the model**, byte-gated, rather than
   hand-writing near-identical blocks.
3. **Measure wall-clock and cost against the single-pipeline baseline** and report
   before treating the split as permanent.
4. **Keep `job_request/FIELDGLASS_US` active**, but isolate it in the `job` domain
   pipeline so its 30-branch union cannot affect the Ameren load.
5. **Source the client half of reference data from `hubspot_raw.companies`.**
6. **One spec, reference sequenced first.**

## Part A -- Domain-split topology

### Pipelines

Six raw pipelines, all publishing into the same `raw_vault` schema. Confirmed
supported: multiple pipelines may publish into one Unity Catalog schema provided no
table is declared twice, and domains partition the entities so no table is.

| pipeline | domain | entities |
|---|---|---|
| `raw_vault_reference` | reference | 11 |
| `raw_vault_finance`   | finance   | 8 |
| `raw_vault_job`       | job       | 5 |
| `raw_vault_pay_bill`  | pay_bill  | 4 |
| `raw_vault_party`     | party     | 4 |
| `raw_vault_payroll`   | payroll   | 2 |

Each carries `hfig.domains: <domain>` and `hfig.vault_layer: raw`. Everything else
in the pipeline configuration is identical to today's `raw_vault` and is generated
from one template, so the six cannot drift apart. `business_vault` is unchanged.

### What still runs once

`apply_source_unions` builds the union views **before** any domain pipeline. A view
read by two domains (`v_fieldglass_us_invoice` is read by `pay_bill` and `party`) is
therefore not an ordering dependency between them.

Upstream, once: `assert_hash_parity`, `create_control_objects`,
`assert_source_conformance`, `assert_key_derivation`, `apply_source_unions`,
`create_mask_functions`.

Downstream, once, over the whole estate: `assert_append_only`, `assert_mask_survival`,
`reconcile_loop1`, `assert_no_broad_grant`, `assert_journal_integrity`,
`assert_aggregate_reconciliation`, `assert_freshness`, `assert_landing_integrity`,
`assert_audit_completeness`, `apply_governance`, `publish_model_metadata`.

`load_hubs` and `load_satellites` take no domain filter and process the whole model
from the staging logs. They stay single tasks, downstream of all six pipelines. This
is deliberate: they are cheap relative to graph construction, and giving them a
domain filter would add a second place where the domain partition is expressed.

### Ordering

Domains are independent except where a binding reads another domain's **vault**
table. Today there are exactly two, both on `hub_organisation` and both currently
inactive:

* `organisation[party]/BUSINESS_VAULT` reads `nhl_payroll_detail[payroll]`
* `organisation[party]/BUSINESS_VAULT_GIE` reads `nhl_invoice_line[pay_bill]`

The generator derives the domain dependency graph from the model and emits
`depends_on` from it, refusing a cycle. It is derived rather than hard-coded because
a hard-coded order rots silently the first time a binding is repointed. With today's
model this yields: `reference` first (see below), then `finance`, `job`, `payroll`,
`pay_bill` in parallel, then `party`.

**Reference runs first** regardless of whether any technical dependency requires it.
That is a stated requirement, and it is also correct: the legal entities and
consolidation hierarchies are the master data every other domain's business keys are
eventually reconciled against.

### Failure isolation

This is the benefit to argue for even if the timings come out neutral. With the
split, a `SpecError` in `pay_bill` leaves `finance`, `job`, `payroll`, `party` and
`reference` loading. Today it stops everything, which is exactly what happened four
times this afternoon.

### Generation

`tools/emit_pipeline_resources.py` writes the pipeline resource file and the job's
raw-vault task block from the model's declared domains, byte-gated by `verify_repo`
exactly as the repo's ten other generated artefacts are (`emit_source_to_target`,
`emit_data_contract`, `emit_key_derivation`, ...). Adding a domain to an entity YAML
regenerates the topology; the pipeline set therefore cannot disagree with the model.

## Part B -- Reference data from Hubspot

### The source

`01_usnc_bronze_dev.hubspot_raw.companies`. Measured today:

| measure | value |
|---|---|
| rows | 7,328 |
| distinct `hs_object_id` | 2,457 |
| ids appearing more than once | 2,444 (max 3 copies) |
| ids where `hs_lastmodifieddate` varies | 444 |
| ids where `name` varies | 6 |
| rows with `hs_parent_company_id` | 144, across 63 distinct parents |
| rows with no `name` | 0 |
| distinct countries | 44 |
| deliveries | 3 files, 4-18 December 2025 |
| **days since last delivery** | **281** |

It is a **full snapshot feed**: three deliveries, each carrying every company, which
is why 2,444 of 2,457 ids appear more than once and why the copies differ only where
the source genuinely changed (444 by modified date, 6 by name).

`hs_lastmodifieddate` and `createdate` are ISO-8601 with `Z` and parse as `TIMESTAMP`
with **zero** failures across all 7,328 rows.

### Freshness is declared, not hidden

The feed has not delivered for 281 days. The binding declares its real SLA and the
freshness gate reports the breach, following the same decision already taken for the
Fieldglass invoice feed: a feed that is meant to deliver and does not should be
visible, and an exemption would hide a silence behind something that looks
deliberate.

### Mapping

| entity | kind | Hubspot mapping | status |
|---|---|---|---|
| `hub_legal_entity` | hub | `legal_entity_code` <- `hs_object_id` | sourced |
| `hal_client_legal_entity_hierarchy` | hal | child <- `hs_object_id`, parent <- `hs_parent_company_id` | sourced, 144 edges |
| `esat_client_legal_entity_hierarchy` | esat | `relationship_status` derived from presence of a parent | sourced |
| `sat_legal_entity_details` | sat | partial -- see below | **changes shape** |
| `lnk_client_contracting_entity` | link | none -- Hubspot has no contracting-entity concept | stays placeholder |
| `esat_client_contracting_entity` | esat | none | stays placeholder |

The contracting entity is HFIG's own legal entity that contracts with the client
(Guidant Global Inc, per Amy's 24 September answer). That is organisation-side data
and belongs with the Workday half, not in a CRM's company list.

### `sat_legal_entity_details` changes shape, and this is the one real modelling call

It declares `registered_name, company_number, jurisdiction, registered_status` -- a
payload written for a companies-registry source. Hubspot is a CRM and has no
registration number and no registration status.

**Proposal: a separate, source-named satellite rather than a half-empty one.** The
repo already names satellites per source (`sat_invoice_header_fieldglass_us`,
`sat_job_request_details_bullhorn_eu`) and refuses multi-source satellites outright.
So:

* `sat_legal_entity_details` keeps its registry payload and keeps its placeholder
  binding, for whenever a registry source lands.
* A new `sat_legal_entity_profile_hubspot` carries what Hubspot actually knows:
  `name`, `domain`, `country`, `city`, `state`, `company_industry`,
  `hs_industry_group`, `regional_group`, `regional_group_global`, `lifecyclestage`,
  `numberofemployees`, `annualrevenue`.

The alternative -- mapping `jurisdiction <- country` and leaving `company_number` and
`registered_status` NULL -- puts CRM data into fields whose names promise registry
data, which is the kind of quiet mis-statement this repo has repeatedly paid for.
`country` is not a jurisdiction of incorporation.

### Dedup

The hub and the hierarchy dedup on `hs_object_id`, ordered by `hs_lastmodifieddate`,
so a re-delivered snapshot collapses to the source's own latest statement rather than
whichever copy was read first. The satellite does **not** dedup to one row: its
hashdiff already collapses identical snapshots and keeps the 444 genuine changes as
the versions they are, which is what a satellite is for.

## Sequencing

1. Reference sourcing (Part B) -- repoint and load `hub_legal_entity`,
   `hal_client_legal_entity_hierarchy`, `esat_client_legal_entity_hierarchy`, and add
   `sat_legal_entity_profile_hubspot`.
2. Domain-split topology (Part A), with `reference` first in the graph.
3. Measure and report against the single-pipeline baseline.

## Non-goals

* The Workday half of reference data (`consolidation_group`,
  `consolidation_hierarchy`, `legal_entity_consolidation` and their effectivity
  satellites). Blocked on an unowned landing step; out of scope here.
* Narrowing `v_fieldglass_us_invoice` from 182 emitted columns to the 34 the model
  uses. Worth doing, independent of topology, and deliberately not bundled in.
* Any change to `hashing.py`, the rulebook, or key composition.

## Risks and open questions

1. **The split may not fix the driver GC.** It should -- six graphs of 2-11 entities
   instead of one of 34 -- but the cause has not been isolated to graph size with
   certainty. Decision 3 exists for this reason: measure, then report.
2. **Six serverless allocations may cost more than one** for a 34-entity model. At
   ~100 entities the arithmetic reverses. To be measured, not assumed.
3. **`hs_object_id` as a vault business key needs Eva's confirmation.** It must be
   the durable company identifier, not a surrogate that HubSpot may reissue. Keying
   the vault on it and being wrong means re-keying every row.
4. **`hs_parent_company_id` must mean legal parent**, not a CRM convenience link
   between related accounts. 144 edges is small enough to review by hand and that
   review should happen before load.
5. **The reference data will be 281 days old.** Loading it is still right -- the
   structure is proved, the hierarchy becomes visible, and the freshness gate makes
   the staleness a reported fact rather than a surprise.
