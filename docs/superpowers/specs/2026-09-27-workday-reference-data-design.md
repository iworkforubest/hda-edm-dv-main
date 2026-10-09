# Workday reference and type data in the vault — design

**Date:** 27 September 2026
**Status:** proposed
**Scope:** subsystem E of five. Brings Workday's ReferenceID/TypeID values into the
vault with history and exposes them in gold.
**Blocks:** subsystem C. 108 fields in the customer-invoice DCDD alone carry
`CHECKREFERENCES`, and none of them can be satisfied without this.

---

## 1. Why this exists

Logan's DCDDs specify the Workday load by reference: a field does not carry "Ameren
Illinois", it carries a `Customer_Reference_ID` whose value Workday already knows. The
customer-invoice DCDD names roughly 293 distinct reference-ID types across its 108
`CHECKREFERENCES` fields, and the other four DCDDs add more.

Adrian, 27 September: *"we have a bunch of ReferenceID, TypeID in the structure. these
are coming from the Workday services, we need that data pull in into data vault -
dedicated structure, to keep the historical changes and also propagated in gold
reference or master data (it could be a view)."*

Nothing in this repo holds those values today. `tools/fetch_workday_references.py`
retrieves them through `Get_References` and writes a file, then deliberately stops --
its own docstring calls that the ARB boundary, because `ref_` is
`naming.PLATFORM_OWNED` and this accelerator reads such objects by join and never
creates them. That boundary is respected here: **nothing in this spec creates a `ref_`
table.**

## 2. Decisions taken

Recorded with who made them, because the reasoning should not live only in a chat log.

1. **A generic hub, not one hub per type.** `hub_wd_reference`, keyed by
   `(reference_id_type, reference_id)`. Adrian, 27 September. Per-type hubs would mean
   ~293 hubs for one DCDD.
2. **An exclusion list.** Reference types that are already first-class vault entities
   are not modelled twice. Adrian, same decision.
3. **Effectivity, not just change history.** Adrian, 27 September.
4. **Bronze is the real target**, landed by Snaplogic or Lakeflow Connect as any other
   Workday source. Adrian, 27 September.
5. **An interim landing schema in silver**, because Bronze has no Workday schema yet.
   Adrian authorised landing in silver; the schema name is `workday_landing` rather
   than `stg_` for the reason in §4.2.
6. **The API version discrepancy is not a blocker.** Adrian, 27 September: the DCDDs
   were issued about two months ago and he does not believe there are large structural
   changes; he will confirm with Logan. Recorded in §8, not treated as a gate.

## 3. Source

`Get_References`, the Workday SOAP service `tools/fetch_workday_references.py` already
calls. Each call is **per reference-ID type** and returns the complete current set for
that type. That completeness is what makes §5 possible: each retrieval is a snapshot,
so a value that stops being returned has been withdrawn, and absence carries meaning.

### 3.1 Two interfaces, and they are not interchangeable

Decision 4 names two landing mechanisms. They have different prerequisites and this
determines who has to be asked:

- **Snaplogic calling the SOAP service** needs nothing from Workday beyond the
  credential already in use. `Get_References` is what we call today.
- **Lakeflow Connect's Workday connector ingests Reports-as-a-Service (RaaS)**, which
  is a different interface. `Get_References` is not a RaaS report. Using the connector
  requires somebody in Workday to publish a custom report exposing these values -- a
  request to Logan or Augustin, not work this repo can do.

Neither is preferred here. The point is that the connector route carries a Workday-side
dependency and the Snaplogic route does not.

## 4. Landing

### 4.1 The target: Bronze, like every other source

`01_usnc_bronze_dev` uses a `<source>` / `<source>_raw` pair per system -- `fieldglass`
and `fieldglass_raw`, `ukg` and `ukg_raw`, `hubspot` and `hubspot_raw`. Workday's
reference values land the same way, in `workday` / `workday_raw`.

**This does not breach spec decision D4.** D4 puts Bronze ingestion outside *this
repo's* scope and has the estate's own raw-to-clean pipeline land `<source>_raw`.
Snaplogic or Lakeflow Connect doing this is that pipeline doing its job.

**It is blocked today.** Measured 27 September: `01_usnc_bronze_dev` has no schema
matching `workday` or `wd_`, and the only catalog-level grant visible to this identity
is `USE CATALOG` to `scope_tds_full_scopes_write`. Creating the schema is a Bronze-team
request, and it joins a queue that already holds PLT-2 and the view grants.

### 4.2 The interim: a schema of its own, not `stg_`

Until Bronze exists, the values land in a new silver schema `workday_landing`.

**Why not `stg_` in `raw_vault`, which is what was first suggested.** `stg_` is not a
free prefix. `naming.stg()` means one specific thing -- the SDP pipeline's append log
for a staged entity -- and DEF-42 rests on a staged kind having exactly two real
tables, the log and the vault table built from it. Landing tables there would put
non-entity objects inside a namespace the loaders enumerate and the gates assert over:
`assert_append_only` inspects every `stg_` table, `assert_no_broad_grant` counts them,
and `publish_stable_views` reasons about which kinds have them. The failure would not
be a crash; it would be gates quietly asserting the wrong things. This is the `ref_`
trap one layer in, and the same answer applies: do not reuse a name that already means
something.

A separate schema also makes the interim removable. When Bronze lands, the binding
moves and `workday_landing` is dropped -- one change, not an untangling.

## 5. Vault structure

### 5.1 `hub_wd_reference`

Business key: `(reference_id_type, reference_id)`. Hash key SHA-256 as `BINARY(32)`,
per the ratified rulebook -- this spec changes no hashing rule and bumps no
`RULEBOOK_VERSION`.

**Folding many concepts into one hub under a type qualifier is already an accepted call
in this model.** `metadata/volumes.yml` records it: *"hub_company and hub_client folded
into ONE hub_organisation: legal entities (11 input_db codes) plus clients, qualified
by reference_type."* This is that pattern, applied to a set that is too large to model
one entity at a time.

The grain is honest: the thing being identified is "a Workday reference value", and its
natural key genuinely is the pair. A `Tax_Code_ID` and a `Currency_Reference_ID` are
not the same business object, but they are the same *kind* of object -- an identifier
Workday issues and may withdraw.

### 5.2 The exclusion list

Reference types that this model already carries as first-class entities are **not**
landed into `hub_wd_reference`. Initially: Customer, Supplier, Company / legal entity,
and Worker, which resolve through `hub_organisation`, `hub_supplier`,
`hub_legal_entity` and `hub_worker`.

The list is a declared artefact, not a convention, and §7 check 3 exists because a
maintained list decays: a type added in Workday that we already model would otherwise
be modelled twice, silently, and two answers to "what is this customer's key" is worse
than none.

### 5.3 `esat_wd_reference_workday`

An effectivity satellite driven on the reference's own hash key: at most one validity
window open per reference at a time.

**This is structurally legal and was checked, not assumed.** `spec.py` requires exactly
one thing of an `esat` -- that it names a `driving_key` (line 1124) -- and line 1064
explicitly exempts `esat` from needing a payload. Nothing requires an esat's parent to
be a link, which is how every existing esat in this model happens to be shaped.

Semantics: first appearance in a snapshot opens the window; disappearance from a later
snapshot for the same type closes it. A Company or Tax Code inactivated in Workday
therefore stops validating new rows, rather than remaining valid forever because
nothing said otherwise.

**Descriptive attributes are deliberately not modelled yet.** If reference descriptors
turn out to change independently of validity, that is a `sat` alongside this esat, added
when a DCDD mapping needs it. Adding it now would be inventing a shape for data nobody
has read.

## 6. Gold

One view in `reference_data` over currently-effective rows -- the schema subsystem A
creates. A view, not a table: Adrian's own framing, and it means the vault stays the
single copy.

Per-type views are created **only for types a DCDD mapping actually consumes.** There
are ~293 candidates and the mapping will use a small fraction. Generating 293 views
against which nothing is written would be 293 objects to govern, publish and keep in
step, for the convenience of a name.

## 7. Verification

Each check must be demonstrated failing before it is believed.

1. **The exclusion list names only types that really are modelled elsewhere.** An entry
   naming an entity the model does not declare is a typo that silently disables landing
   for a type.
2. **No excluded type appears in `hub_wd_reference`.** The list and the load agree, or
   the load is wrong.
3. **No landed type is missing from both the hub and the exclusion list.** The
   complement of check 2: a type must be modelled here, or deliberately modelled
   elsewhere, and never neither.
4. **`workday_landing` holds no object carrying a `naming.GENERATED_TABLE_PREFIXES`
   prefix**, so the interim schema cannot start to look like a vault schema.
5. **The esat's driving key is the hub's hash key**, not a source column -- the
   structural error that would let two windows sit open for one reference.
6. **Nothing in this subsystem emits a `ref_` table.** The ARB line, asserted rather
   than remembered.
7. **Effectivity closes on disappearance**, proven against a two-snapshot fixture where
   the second omits a value the first returned.

## 8. Non-goals and open questions

**Non-goals.** No DCDD field mapping (C). No `ref_` tables, ever (ARB). No descriptive
satellite until a mapping needs one (§5.3). No per-type gold views beyond those
consumed (§6). No change to any hashing rule.

**The API version question is open and deprioritised.** All five DCDDs declare
`v43.0`; Augustin's email said `v45.0`; `fetch_workday_references.py` pins `v47.0` on
measurement -- both `Get_References` and `Get_Organizations` returned byte-identical
responses at v46.2 and v47.0 once the echoed version token was normalised. Adrian's
ruling, 27 September: the DCDDs are about two months old, large structural changes are
unlikely, and he will confirm with Logan. Recorded so that if a mapping later fails on
a field the DCDD promised, the version is the first thing checked rather than the last.

**The Bronze schema is a dependency with no owner yet.** §4.1 is blocked on the Bronze
team creating `workday` / `workday_raw`. The interim in §4.2 is what makes E buildable
without them, and it is removable by design -- but if that request is never made, the
interim becomes permanent by default, which is how temporary schemas usually become
permanent.

**The ~293 figure needs curation.** It is extracted from the DCDD's `Type Value` column
on rows validated by `CHECKREFERENCES`, and some of those rows carry plain data types
such as `Text` rather than reference types. The real count is smaller and must be
derived, not estimated, before the exclusion list can be called complete.
