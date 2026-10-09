# Workday as a Data Vault source — design

**Date:** 7 September 2026
**Status:** proposed, not implemented
**Depends on:** a Bronze landing path (unowned), PLT-7, and WDJ-5's domain grant

## What this builds

Workday's reference data becomes a **source system of the vault** rather than a set of
lookup tables read by join. Concretely: the legal-entity family that has been declared and
empty since 5 September gets a real binding, and the register everyone has been waiting on
turns out to already exist.

It does **not** build the Workday journal export. That is `docs/workday_journal_export_
analysis.html`, it has nine open asks, and it consumes what this produces.

## Why now, and why it is not a lookup table

Two facts arrived on 6–7 September, both by calling the tenant rather than reading about it:

1. **The register exists.** `Get_References` returns **82 companies** with codes
   (`UK011 Impellam Group Limited`, `NL010 HeadFirst Global BV`) across 16 jurisdictions.
   `docs/legal_entity_roster_worksheet.csv` has 24 rows with **0 of 96** identifying cells
   filled, and has blocked LEG-1..LEG-8 for two days.
2. **The hierarchy probably exists too.** Alongside the companies sit **30 consolidation
   groups**, each named for a holding company. Their membership is an ownership hierarchy.
   We cannot read it yet — `Get_Organizations` is refused with
   `Server.processingError | The task submitted is not authorized`, a permission refusal
   rather than a malformed request — but the call, the client and the parser are built.

**A lookup table would answer "who owns Guidant today". The vault answers "who owned it in
March".** That is not a theoretical preference: Impellam Group and HeadFirst Group unify
under a new brand in the coming weeks, and a hierarchy change is exactly the thing we will
want recorded rather than overwritten. `hal_legal_entity_hierarchy` and
`esat_legal_entity_hierarchy_effectivity` were built for that on 5 September, driving on the
child, and they have been waiting for a source ever since.

## The boundary: what stays `ref_`, what becomes vault

`naming.PLATFORM_OWNED` reserves `ctl_ ref_ reg_ agg_ doc_` and states that the generator
reads them by join and never creates them. That rule is not being relaxed. The line drawn
here is between two different kinds of thing that both arrive from the same service:

| | goes where | why |
|---|---|---|
| **Enumerations** — the ~160 valid worktag *types*, `Reference_IndexReferenceEnumeration`'s 5,855 type names | `ref_`, platform-owned, unchanged | A closed list of permitted values. It has no business key, no attributes and no history anyone would query. It is configuration. |
| **Instances** — companies, ledger accounts, cost centres, consolidation groups | **vault entities** | They have identity, descriptive attributes that change, and relationships that change. Recording only their current state throws away the answer to every historical question. |

The test is not "did it come from Workday". It is **"would anyone ever ask what this was
last quarter"**. `Cost_Center_Reference_ID` as a list of valid codes is configuration; the
fact that cost centre `C1100` was called Sales and belonged to a given company on a given
date is vault content.

## Architecture

### The four entities already exist and are repointed, not created

All four are declared, gated, reviewed and bound to `PLACEHOLDER` with a comment saying the
roster does not exist yet. Nothing about their shape changes.

| entity | Workday field | note |
|---|---|---|
| `hub_legal_entity` | `Company_Reference_ID` | `key_style: authored` already, which is what makes one company one row across every feed. The business key becomes the Workday code — **LEG-1 option (c)**, and this design's recommendation. |
| `sat_legal_entity_details` | `Name`, `Organization_Code`, `Inactive` | The payload is already `registered_name, company_number, jurisdiction, registered_status`. Workday supplies three of the four. |
| `hal_legal_entity_hierarchy` | `Included_Organization_Reference` | Parent and child roles already exist on the link. A consolidation group is the parent; its members are children. |
| `esat_legal_entity_hierarchy_effectivity` | successive retrievals | Driving key is the child, already. An edge that disappears between retrievals is closed; a new one opens. |

**`company_number` is the one payload column Workday does not supply.** It stays unfilled and
stays a question for business and legal — LEG-1's second option is not answered by this.

### `jurisdiction` is derived, and that needs stating

Workday's codes carry a jurisdiction prefix — `UK011`, `NL010`, `AU100`, `IE100`. It is
tempting to parse it. **This design does not**, for the same reason `buyer` was not parsed
into a legal entity: a prefix is a naming convention, not a declared attribute, and
`COM-Guid11 Guidant Global Germany GmbH` already breaks the pattern. Jurisdiction comes from
a declared field or it stays empty and visible.

### The ledger account is NOT added to the existing hub

`hub_ledger_account` is keyed `(organisation_reference_id, ledger_account_reference)` and
holds Great Plains' `actindx` — its index over a five-segment account string, **361,706
combinations across 11 companies**. Workday's chart is **135 accounts** with no organisation
scope, and the exact-code overlap is 13.

These are different concepts, not different spellings, and putting both in one hub would mix
two grains under one key — the defect WDJ-8 exists to describe, rebuilt one layer down. So:

* A **separate hub** for the Workday chart when it is needed, keyed on `Ledger_Account_ID`.
* A **`sal_` same-as link** between the two once the WDJ-8 crosswalk exists. That is what a
  same-as link is for, and the accelerator already declares the prefix.
* **Neither is built by this design.** WDJ-8's crosswalk is a finance decision that has not
  been made, and building the link before the mapping exists would produce an empty table
  that looks like an answer.

### Cost centres and the rest

`Cost_Center_Reference_ID` (666, of which 602 retired), `Region_Reference_ID` (99),
`Custom_Organization_Reference_ID` (188) are all instance data by the test above, and all
are **out of scope here**. They belong to the journal export's worktag design, which is
blocked on WDJ-8. Listing them now would be declaring entities nobody can bind.

## Workday is ONE GLOBAL TENANT, and that shapes the rest

Confirmed 7 September: there is a single Workday tenant for the whole group, hosted in
**Belgium**. Not one per region. Three consequences follow, and the third is not ours to
settle.

**The reference data is global, and the vault is regional.** This estate deploys to eight
targets across four regions — `weu`, `uks`, `usnc`, `aue`, each with a TDS twin and its own
silver catalog. Workday's 82 companies span 16 jurisdictions in one list; there is no
`usnc` subset to bind.

**That is why `hub_legal_entity` is `key_style: authored`, and it now pays off.** An
authored key hashes `(legal_entity_code)` with no source or region scope, so
`UK011` is the same hash key in `02_usnc_silver_edm` and `02_weu_silver_edm`. Every regional
lake can bind the same global source and the keys agree by construction — which is exactly
what `checks/conformance_check.py` compares regions for. A federated key would have given
one Guidant per lake, and the comment in `hub_legal_entity.yml` saying a federated key
"would give us one Guidant per VMS" turns out to apply per REGION as well.

**Whether it lands once or four times is a real choice, and this design does not make it.**
Either the landing writes into each regional lake, or one lake holds it and the others read
across. Cross-catalog reads already happen — `nhl_payroll_detail` binds
`hfig_usnc.raw_vault.nhl_payroll_detail` — so both are possible, and the trade is duplication
against a cross-region dependency. It belongs with whoever owns the landing (below), because
the answer follows from where the writer can write.

### The same question applies to every global dimension, and one of them is unresolved

Legal entities are safe: a company is a company, and `UK011` cannot mean two things.
**Cost centres are not obviously safe**, and the difference matters to a key.

Workday's 64 live cost centres are one list with no regional marker, and geography is a
separate dimension — `Region_Reference_ID` and the geographic custom organisations. So the
*structure* is global. Whether the *meaning* is global is a business fact we do not have:
is `C6130 Engineering` one cost centre four regions charge to, or four regional functions
sharing a code?

**If the meaning is global, the key is the code.** If it is regional and we key on the code
alone, two cost centres collapse into one hub row and nothing fails — the numbers are
simply wrong above. That is the `buyer` defect with a different column name.

Cost centres are out of scope for this design, so nothing here depends on the answer. **It
is recorded because the first extension of this work will need it**, and asked as
**WDJ-10**. The general rule this design proposes: *a global dimension may be keyed on its
code alone only once someone has confirmed the code means one thing group-wide.*

### And a residency question this design does not answer

A single EU-hosted Workday tenant means anything retrieved from it and landed in
`02_usnc_silver_edm` has crossed from the EU to a US lake. For the entities in scope here —
companies, consolidation groups, ledger accounts — that is corporate reference data and the
question is probably a formality. **It is not a formality for the retrievals this design
excludes:** `Employee_ID` returns 2,121 rows and `Contingent_Worker_ID` 90, and those are
people.

Nothing in this design retrieves either, and nothing should until somebody who owns data
residency has said what may cross. **Recorded here so that the day someone extends the
retrieval to workers, the question is already written down rather than discovered.**

## The dependency nobody owns: landing

**The vault reads Bronze. `tools/fetch_workday_references.py` writes a file.** Something has
to put Workday's output where a pipeline can read it, and that is not decided:

* We do **not** hold `CREATE SCHEMA` on `01_usnc_bronze_dev` or `02_usnc_silver_edm_dev` —
  measured 4 September, on the platform queue.
* A source binding can read any catalog the pipeline can reach — `nhl_payroll_detail` already
  binds `hfig_usnc.raw_vault.nhl_payroll_detail` — so it need not literally be the Bronze
  catalog. It does need to be a table, somewhere, written on a schedule.
* Whoever writes it needs the credential, which is **PLT-7**: secrets live in Azure Key Vault
  and only the platform team can create them.

**Nothing in this design is buildable until that is settled**, and the honest sequencing is:
grant (WDJ-5) → landing path → binding → `active_sources`. Repointing the YAML before the
table exists reproduces exactly the placeholder state we are trying to leave.

## What this deliberately does not do

* **It does not resolve `buyer`.** That still happens in the business vault, above the raw
  vault, exactly as `hub_legal_entity`'s own comment says.
* **It does not promote implementation-tenant values.** Everything measured so far came from
  `impl-services1`. WIDs are tenant-specific and identify nothing in production; whether
  configured reference IDs migrate is WDJ-5's question. **No binding goes live against an
  implementation tenant.**
* **It does not close LEG.** It changes what four of the eight questions are asking. LEG-1
  gains a third option; LEG-7's "who owns the register and where does it live" may become
  "Workday, retrieved on a schedule"; LEG-3..LEG-6 may be answerable from the consolidation
  hierarchy if WDJ-5's grant lands. `company_number` and the role question are untouched.

## Open questions, and who holds them

| | holder |
|---|---|
| Is a Workday **Company** one legal entity, always? | business / legal — **LEG-2** |
| Do configured reference IDs migrate from impl to production? | Workday team — **WDJ-5** |
| The production host for the global tenant | Workday team — **WDJ-5** (the *tenant* question is answered: one global tenant, Belgium) |
| Does the landing write once, or into each of four regional lakes? | **ours** — follows from who owns the landing |
| May EU-hosted worker data cross into a US lake? | **data residency owner** — out of scope here, and out of scope on purpose |
| Does a cost centre mean one thing group-wide? | finance — **WDJ-10**; not needed until cost centres are in scope |
| Grant the ISU the `Get_Organizations` domain | Workday team — **WDJ-5** |
| Where does Workday output land, and who writes it? | **unowned** — this design's blocker |
| A Key Vault secret and a scope | platform — **PLT-7** |
| Is the Workday code the legal-entity key? | business / legal — **LEG-1**, now leaning (c) |

## Success criteria

1. `hub_legal_entity` loads from Workday and `spec.active_table_bindings` reports it active.
2. `hal_legal_entity_hierarchy` carries one edge per consolidation membership, and
   `esat_legal_entity_hierarchy_effectivity` closes an edge when a company leaves a group
   between retrievals — proven by mutating a retrieval, not by inspection.
3. `docs/legal_entity_roster_worksheet.csv` is **deleted**, not filled in. A worksheet that
   duplicates a retrievable source is the second authority this repo keeps removing.
4. Gold projects whichever view a use case needs, and the vault answers "who owned this in
   March" because the effectivity satellite recorded it.
