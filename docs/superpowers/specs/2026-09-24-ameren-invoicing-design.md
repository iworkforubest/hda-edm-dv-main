# Ameren (MSP001) invoicing as a Data Vault source, and GIE as ours — design

## What this builds

A raw vault, one business-vault computed satellite, and one gold table that produces the
values the Ameren customer invoice is built from.

**CORRECTED 24 September 2026 — this document said "with the Global Invoice Engine
computed in Databricks". That overstates what is being built.** Databricks computes the
VALUES: it selects the lines, applies the rules, assigns the invoice date and the line
numbers, and gates the release. **GIE templates and delivers.** The two are different
systems and this document claimed one of them that is not ours.

It matters for one field in particular. The invoice date is **the date of the Databricks
run that produces the invoice**, set here and frozen against that invoice — not something
GIE supplies. Confirmed with Amy Keser on 24 September. If each side had assumed the
other generated it, nobody would have.

Scope is one MSP client of 54. Everything here is shaped so the 55th costs a row of
configuration rather than a change to the model — the same constraint
`metadata/source_unions.yml` already carries for job postings.

**Agreed with Adrian, 23–24 September 2026:**

* Model the invoice feed **now**, against placeholder Bronze bindings, in the pattern
  `hub_worker` and `sat_job_request_details` already use. Nothing loads until the feed
  exists; the design's job is to make the Bronze ask exact.
* Gold is the **GIE input projection** — one row per invoice line — not an analytical star.
* `hub_supplier` is keyed on a **supplier code**, placeholder until a feed carries one.
  Explicitly *not* keyed on a name.
* The rules in the workbook's `5 Business Rules` sheet (AME001–AME013) are applied on the
  silver→gold path and are traceable to it.
* **GIE is ours.** It is computed here. This is the change that made the design bigger than
  a projection, for the reason in *GIE is two jobs* below.

## What was measured, because most of this contradicts the workbook

Everything in this section came from the lake or the file, on 23 September 2026. It is
recorded because the design decisions rest on it, and because **the workbook disagrees with
the lake in three places that matter**.

```
invoice tables in 01_usnc_bronze_dev.fieldglass_raw            0
supplier_code columns across all 302 fieldglass_raw tables     0
Ameren families landed   distributed_jobposting, jobposting, jobseeker, timesheet, worker
```

**There is no invoice feed, and no supplier code anywhere.** The workbook sources its
Supplier ID from `Supplier Code` on the *Invoice Buyer Without Status Change Download* — an
extract this estate does not receive.

What we do receive identifies a supplier by name, in two columns that disagree with each
other. From `io_timesheet_ameren`, 182,914 rows:

```
rows with a supplier                     178,627
same company once normalised             178,617   (99.994%)
genuinely a different company                 10   (CSI IT, LLC under Meridian Staffing Services)
supplier blank                             4,287   (parent_supplier never blank)
suppliers 29 / parent_suppliers 28 / workers 542
```

`Zempleo` / `Zempleo, Inc.` · `Envision, LLC` / `Envision` · `Iconma, LLC` / `ICONMA LLC` ·
`SyllogisTeks Company` / `Syllogisteks`. **`parent_supplier` is not a hierarchy — it is a
second spelling of the same company** on all but ten rows. The lake holds 29 suppliers where
the workbook's master sheet lists 23, and one of them is `Lorien`, which is an HFIG brand —
so there is an intercompany relationship in this data that nobody has mentioned.

This is why `hub_supplier` is not keyed on a name. It is not a stylistic preference: the
evidence is that the same company arrives spelled two ways **within a single row**.

## Architecture

### Placement

| layer | location | note |
|---|---|---|
| source union view | `raw_vault.v_fieldglass_us_invoice` | Fieldglass delivers one table per tenant; `checks/apply_source_unions.py` discovers them |
| raw vault | `02_usnc_silver_edm_dev.raw_vault` | |
| business vault | `02_usnc_silver_edm_dev.business_vault` | `csat_` is the established pattern there |
| issuance ledger | `02_usnc_silver_edm_dev.control` | append-only, `ctl_` prefix |
| gold | `03_usnc_gold_edm_dev.wd_fin_export.invoice_line_export` | **needs a new bundle variable** — today only `gold_catalog` exists, no gold schema is named anywhere in the repo |

**The gold table is not named for Ameren, and that is deliberate.** `invoice_line_export`
carries one row per invoice line for **every** MSP client, discriminated by the client hub —
because a table per client is the churn this model exists to avoid, and it would make
onboarding the 55th a schema change rather than a row of configuration. Ameren is the first
tenant through it, not its subject.

The gold catalog's planned schemas are `reference`, `master`, `wd_fin_export`, `governance`
and `control`. Gold's generated control DDL already writes to `${control_schema}`, so it
agrees with that list and needs no change.

### Raw vault — two new hubs, two reused

* **`hub_invoice`** — `tenant_scoped` on `(invoice_tenant, invoice_reference)` from `buyer`
  and `invoice_id`. The identifier already embeds the buyer (`AEE1IN00123410`), so the scope
  is redundant *today*; it is declared anyway because this hub will take Beeline and VNDLY
  next, and those are not guaranteed to namespace theirs.
* **`hub_supplier`** — `tenant_scoped` on `(supplier_tenant, supplier_code)`. Placeholder
  binding until a feed carries the code.
* **`hub_organisation` is reused for the client, not duplicated.** It is already the client
  hub — `lnk_client_job_request` links on `client_code` — and its `(reference_type,
  reference_id)` key namespaces identifier spaces. Ameren enters as
  `reference_type: Fieldglass_Buyer_Code`, `reference_id: AEE1`. **That is the point of the
  reference-type pattern**: it lets a Fieldglass buyer code and a Workday organisation id
  coexist in one hub without asserting they are the same identifier.
* **`hub_worker` is reused**, with a Fieldglass binding on `worker_id`. It is
  `key_style: federated`, so a Fieldglass worker is its own identity and does not silently
  merge with the same human's STRIIVE or UKG row. Resolving those is a same-as concern, not
  this hub's.
* **`nhl_invoice_line`** — parents `[invoice, supplier, worker, organisation]`, transaction
  key `invoice_line_item_ref`. Payload carries **both** amounts — what the client is billed
  and what the supplier is paid — because the source states both and the difference is the
  MSP margin. Carrying one discards a stated fact and forces someone to rebuild it later
  from two systems.
* **`sat_invoice_header`** — invoice-level attributes: gross, currency, invoice type, submit
  date.

### Business vault — one computed satellite

`csat_invoice_line_gie` over `nhl_invoice_line`, holding the rules that are genuine
derivations rather than passthroughs. See *The rules* below for which.

### GIE is two jobs, and merging them is the defect

Making GIE ours does not simply add two fields. It adds a **different kind of work**:

* **Deterministic projection** — a pure function of the data. Belongs in gold.
* **Stateful issuance and submission** — what date an invoice was issued on, what its line
  numbers were, whether it was sent, what Ameren AP replied. **The vault has no place for
  this**, and a projection layer recomputed on every run is the wrong home for it.

#### Issued values must be frozen, not recomputed

**Invoice Date and Line Number appear on a document sent to a customer.** If gold derives
them on every run, a re-export can change them: the same invoice acquires a different date,
or its lines renumber. Ameren AP then reconciles against a document that no longer matches
the one it received, and **nothing in the pipeline fails** — every load succeeds, every gate
stays green, and the only symptom is a dispute.

This is the WDJ-2 argument arriving in a new place. There, a Databricks hash was proposed as
a permanent external journal identifier and rejected because its value depends on inputs
that can change. A line number on an issued invoice is the same object: **an external-facing
identifier whose value must not depend on when it was last computed.**

Therefore:

* **`ctl_invoice_issuance`** — append-only, in the `control` schema. One row per invoice
  line, written **once**, recording the issued invoice date and the assigned line number.
* Gold **reads** the ledger and joins to it. It never derives those two fields.
* An invoice absent from the ledger is issued by the GIE task and recorded; an invoice
  present is projected from what was recorded. Re-running is therefore idempotent by
  construction rather than by care.
* `assert_append_only` already guards `ctl_` tables, so the freeze is enforced by a gate
  that exists rather than by convention.

#### Line numbering needs a deterministic order

`AME006` says line numbers are sequential within an invoice. Sequential **in what order** is
unspecified, and an unspecified order makes even the first issuance arbitrary.
`invoice_line_item_ref` is unique per line and is the sort key; it is recorded in the ledger
so the question is asked once and never again.

### Implementation form: a Python file, not a notebook

GIE is implemented as `checks/`-style Python run as a `spark_python_task`, matching
`checks/publish_metadata.py`.

**This repo contains no notebooks.** It runs 21 `spark_python_task` and 2 SDP
`pipeline_task`. The reason to keep it that way is not consistency for its own sake: the
`checks/*.py` files are unit-testable with the `check(name, condition, detail)` idiom, they
diff cleanly under review, and they are covered by the byte-gate. **A notebook is none of
those three.** Every defect this repo has caught by mutation testing was caught in a file
shaped like `checks/`.

## The rules

All thirteen rules from the workbook's `5 Business Rules` sheet, and where each is applied.
With GIE ours, **all thirteen are ours** — AME002 and AME006 are no longer somebody else's.

| Rule | What it does | Where |
|---|---|---|
| AME001 | Invoice Number direct map | **NOT APPLIED — no source column is declared.** `invoice_number` is in no entity's payload, keys or transaction key, and not in `control.ctl_invoice_issuance`. `checks/invoice_export.py` names it in `UNDECLARED_SOURCES` and `required_field_refusal()` refuses every run until the owner declares one. `hub_invoice.invoice_reference` is the plausible candidate and is deliberately not used — the same inference that produced the two `unresolved` markers in the mapping. |
| AME002 | Invoice Date | **GIE task → `ctl_invoice_issuance`**, gold reads it |
| AME003 | Repeat gross on every line of an invoice | `csat` — join from `sat_invoice_header` |
| AME004 | Description concat, worker as `Last, First` | `csat` |
| AME005 | Line Description concat | `csat` |
| AME006 | Sequential line numbers within invoice | **GIE task → `ctl_invoice_issuance`**, gold reads it |
| AME007 | `Tax → TAX`, else `ITEM` | `csat` |
| AME008 | ITEM uses line amount; tax aggregated to final line; lines sum to gross | `csat` selects; **gold release gate** asserts the sum |
| AME009 | Accounting Date from week-ending | `csat` |
| AME010 | Project Number direct map | gold projection |
| AME011 | Task module selection; parse after `\|`, strip trailing `)` | `csat` |
| AME012 | Expenditure Type module selection | `csat` |
| AME013 | Expenditure Organization module selection | `csat` |

Gold therefore emits **13 fields**: eleven projected or derived, two read from the issuance
ledger — except that one of the thirteen, AME001's `invoice_number`, has no declared source
column, so nothing is emitted at all today. That field stays in
`invoice_export.REQUIRED_FIELDS`: dropping it would make the release gate pass by no longer
looking.

### Three rules carry defects, and they are not ours to resolve

**AME009 contradicts its own example.** The rule states `DD/MM/YYYY`. The mapping sheet's
example for the same field is `2026-07-26 00:00:00`, and all 14 distinct Accounting Dates in
the sample file are ISO. Rule, example and data disagree three ways. A US client receiving
`07/26/2026` against `26/07/2026` is also **ambiguous for twelve days of every month** and
produces valid-looking wrong dates rather than errors.

**AME011 is the only rule with real parsing risk and no test data.** `A0625|I-APMS-107000-CE`
must become a task number, with a trailing parenthesis stripped "where applicable" — which
is not a specification. It needs worked examples for TS, ES and MI, or the implementation
encodes a guess.

**AME008's tax half is untested by the only sample that exists.** There is exactly **one
`TAX` line in 555**. The reconciliation gate passes today on a path that has never been
exercised.

## Data flow

```
Fieldglass  ->  SnapLogic  ->  01_usnc_bronze_dev.fieldglass_raw.io_invoice_<tenant>   [DOES NOT EXIST]
                                      |
                       raw_vault.v_fieldglass_us_invoice   (union view, tenants discovered)
                                      |
   hub_invoice   hub_supplier   hub_organisation   hub_worker   nhl_invoice_line   sat_invoice_header
                                      |
                       business_vault.csat_invoice_line_gie      (AME003-005, 007-009, 011-013)
                                      |
        control.ctl_invoice_issuance  <-- GIE task issues date + line number ONCE, append-only
                                      |
                  03_usnc_gold_edm_dev.wd_fin_export.invoice_line_export   (13 fields)
                                      |
                              [OUT OF SCOPE] payload, delivery, AP response
```

## Error handling

* **Release gate** — `sum(line Amount) = Invoice Amount` per invoice, AME008. Verified to
  hold across all 225 invoices and 555 lines in the sample. An invoice failing it is not
  released.
* **Required-field gate** — the projected fields are `NOT NULL` at release; a missing
  project or task code is an exception, not a blank on a customer invoice.
* **Exceptions route** — invoices failing either gate go to an exception set for correction
  and reprocessing, as step 4 of the workbook's end-to-end describes. They are **not**
  partially released: a well-formed invoice with a missing line is worse than no invoice,
  because it looks correct.
* **Issuance is append-only** — a second attempt to issue an already-issued invoice is a
  no-op, not an update. Guarded by `assert_append_only`.

## Testing

* Structural: entity YAMLs pass `tests/test_accelerator.py`, including the refusal list — no
  hub or link carrying descriptive attributes, no NHL without a transaction key, no
  multi-source satellite, no link-of-a-link.
* Rule tests: each of AME003–AME013 gets a pure-function test with worked examples, in the
  `check(name, condition, detail)` idiom. **Each must be mutation-proven** — changed so it
  ought to fail, and observed failing — because this repo has found six checks whose stated
  guarantee exceeded what their fixture could detect.
* Issuance: re-running the GIE task over an already-issued invoice must produce byte-identical
  gold output. This is the test that protects the customer-facing guarantee and it is the one
  most worth writing first.
* Reconciliation: AME008 asserted per invoice, with a fixture that includes a **multi-line
  invoice carrying tax**, since the real sample has only one tax line and cannot exercise it.

## What this deliberately does not do

* **No supplier payment flow.** The workbook's sheet 6 describes an electronic file to
  suppliers. It needs the same vault and is a separate deliverable.
* **No invoice payload, delivery or AP response capture.** SnapLogic already carries the
  inbound leg; the outbound is an integration concern and assuming Databricks owns it would
  be an assumption, not a decision.
* **No supplier hierarchy.** `parent_supplier` was measured to be a spelling variant on all
  but ten rows, so modelling it as a hierarchy would encode a data-quality artefact as a
  business structure.
* **No name-based supplier matching.** Deferred until a code exists, deliberately.
* **Nothing loads.** Every Bronze binding is a placeholder.

## Open questions, and who holds them

1. **What is the tax-point date?** It appears three times in the workbook, always as
   something GIE owns, and is defined nowhere. It is not in the Fieldglass feed, which
   carries `Tax Amount` and `Supplier Tax Amount` only. **Making GIE ours converted
   "GIE-owned" from an answer into an open question** — these rules were never specified
   because nobody had to specify them. The sample suggests what it was in practice: Invoice
   Date has **1 distinct value across all 225 invoices** while Accounting Date has 14, so it
   behaved as the release date. That is a plausible rule and should be decided rather than
   inferred from one file. *Holder: whoever owns the GIE specification.*
2. **AME009's date format** — `DD/MM/YYYY`, ISO, or US order. *Holder: Ameren AP intake.*
3. **AME011's parsing rule** — worked examples for TS, ES and MI. *Holder: the mapping sheet's author.*
4. **Does the invoice feed carry `Supplier Code`?** If it lands without one, `hub_supplier`
   stays unbound. *Holder: Bronze / the Fieldglass integration.*
5. **Who delivers the invoice to Ameren AP**, and in what form? *Holder: integration.*
6. **Is a Fieldglass supplier code unique across all 54 clients or only within one?**
   `tenant_scoped` is the safe default and survives either answer, so this does not block —
   but if codes are global, the scope is redundant and cross-client supplier spend becomes
   answerable for free. *Holder: measurement against two other clients' extracts.*
7. **Does a TAX line carry project and task coding, and if not, how is it released?**
   `nhl_invoice_line` declares one `invoice_line_item_type`. On a tax line AME007's
   `line_type()` returns `TAX`, while `module_value()` **raises** — `Tax` selects none of
   TS/ES/MI, and guessing one would put another module's expenditure coding on the line.
   Separately, gold's required-field gate asks **every** released line for
   `project_number`, `task_number`, `expenditure_type` and `expenditure_organization`, so
   an invoice carrying the aggregated tax line AME008 describes is routed to exceptions and
   never released. The sample holds **one TAX line in 555**, so the path has never been
   exercised and the repo cannot infer the answer. Both functions' docstrings and a test
   pin the current behaviour so it cannot change unnoticed; resolving it means either
   exempting TAX lines from the coding requirement or stating what coding they carry.
   *Holder: whoever owns the GIE specification, with Ameren AP.*

## Success criteria

* The entity YAMLs validate and the structural suite passes.
* Every rule AME001–AME013 is implemented, mutation-proven, and traceable to its rule id.
* Re-running GIE over an issued invoice changes no gold row.
* AME008 holds for every released invoice.
* Onboarding a 55th client requires no change to this model.

## The GIE boundary, and what was renamed for it

Corrected 24 September 2026, after Adrian confirmed that **Databricks computes the dates and
GIE only templates**.

Three modules were named as though they WERE the Global Invoice Engine. They are not — they
produce what it renders:

```
src/accelerator/gie_rules.py  ->  invoice_rules.py    the AME rules
checks/gie_issue.py           ->  invoice_issue.py    issues date and line numbers, once
checks/gie_export.py          ->  invoice_export.py   the release gates
```

**What was deliberately NOT renamed, and why.** `csat_invoice_line_gie` and the
`BUSINESS_VAULT_GIE` binding keep their names. They read as "the invoice line prepared FOR
GIE", which is accurate under the corrected boundary — they feed it rather than claim to be
it. Renaming the entity would change its hash-key composition, re-keying it across 24 files
to settle a naming question that the current name does not actually get wrong.

**Why this was worth fixing rather than noting.** Two teams each calling a different thing
"the invoice engine" is how a gap survives a design review — and this one had a specific
gap behind it. The invoice date is assigned HERE. Had the naming kept implying GIE owned
the engine, the most likely outcome was each side assuming the other generated the date.
