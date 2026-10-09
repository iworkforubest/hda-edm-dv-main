# `wd_fin_export` — the Workday customer-invoice export

**Date:** 27 September 2026
**Status:** proposed
**Scope:** subsystem C, first object only. Customer Invoice, end to end.
**Depends on:** subsystem A (the gold schemas — **built and applied 27 September**),
subsystem E (Workday reference data — **built, unpopulated**), PLT-2 (**unsent**).

---

## 1. Intent

Produce the CSV file set Workday's DT/DTS tool loads for a customer invoice, from data
already in this vault, with every value traceable to a rule somebody authorised.

Adrian's framing, 27 September: the outcome of `wd_fin_export` is to prepare data that
will be sent to Workday. Migration first — Customers, Suppliers, Journals, Customer
Invoices, Supplier Invoices — consolidated in Databricks, exported as CSV, collected by
Snaplogic, tested by Logan in DT/DTS, then loaded once correct.

**This spec covers Customer Invoice and nothing else.** The other four objects are the
same machinery pointed at a different DCDD; proving it once on the smallest object is
cheaper than proving it five times at once.

## 2. Why Customer Invoice first

It has the smallest real surface and the only business rules already written down.

Measured from `metadata/workday/dcdd/Submit_Customer_Invoice_DCDD.xlsx` on 27 September:
of 560 field rows, **469 are "Do Not Populate"**. The real surface is **91 fields across
4 CSV files**, and only **five** fields must be populated:

| field | class | file |
|---|---|---|
| `Customer_Invoice_ID` | Design Requirement | **all CSVs** |
| `Customer_Invoice_Line_Reference_ID` | Design Requirement | both line files |
| `Company_Reference_ID` | Required | `Submit_Customer_Invoice` |
| `Customer_Reference_ID` | Required | `Submit_Customer_Invoice` |
| `Submit` | Constant Value | `Submit_Customer_Invoice` |

The business rules exist as tested pure functions in `src/accelerator/invoice_rules.py`
(AME004, AME005, AME007, AME008, AME010, AME011, AME012, AME013), and the two that must
NOT be recomputed are already excluded from it by design (§6).

## 3. The pipeline, and what already exists

```
raw vault        hub_invoice, nhl_invoice_line,
                 sat_invoice_header_fieldglass_us, sat_invoice_line_details_fieldglass_us
    |
csat_invoice_line_gie        the DERIVED half: 11 payload columns
                             DEF-58 -- 0 expressions declared today
    |
control.ctl_invoice_issuance the FROZEN half: AME002 Invoice Date, AME006 Line Number
                             checks/invoice_issue.py -- NOT IMPLEMENTED stub
    |
gold wd_fin_export           4 CSV files + the join contract
                             checks/invoice_export.py -- NOT IMPLEMENTED stub
```

Both stubs are already declared in `resources/vault_job.yml`, and `invoice_export` already
receives `--catalog`, `--control-schema`, `--business-vault-schema`, `--gold-catalog` and
`--gold-export-schema`. It prints `NOT IMPLEMENTED` and exits 0, which is why the 30-task
load is green while writing nothing to gold.

## 4. The CSV contract

Four files, and a join contract Snaplogic depends on:

| file | populated fields |
|---|---|
| `Submit_Customer_Invoice` | 58 |
| `Submit_Customer_Invoice_Details` | 19 |
| `Submit_Customer_Invoice_Lines` | 8 |
| `Submit_Customer_Invoice_Lines_Details` | 4 |

`Customer_Invoice_ID` appears in **every** file; `Customer_Invoice_Line_Reference_ID`
appears in both line files. Those two columns are what make the set a set rather than four
unrelated files, so they are asserted, not assumed: a header row with no matching line
rows, or a line row whose invoice id is absent from the header file, is a failure before
anything is written.

### 4.1 Destination

A UC Volume, with the path a bundle variable resolved per target. Snaplogic reads volumes;
a Workspace folder is a staging convenience and would be a second code path. The variable
has **no default**, for the reason `gold_export_schema` has none: a wrong-but-plausible
default is how a file lands somewhere nobody is watching.

`invoice_export.py` needs one new argument for this. It is not wired today.

## 5. The mapping lives in the repo, and two checks hold it to the DCDD

The DCDD's `SOURCETABLE.FIELD` and `TRANSFORMATION LOGIC` columns are **empty on all 560
rows**. That mapping is the work, and it is declared in this repo rather than written back
into the workbook.

**Why not into the workbook.** The five `.xlsx` files are committed verbatim with sha256
digests in `metadata/workday/PROVENANCE.json`. Editing one breaks the record that answers
"which bytes did this mapping come from", and a binary is not reviewable in a diff.

**The mapping is held honest in both directions**, the same way subsystem E's exclusion
list is — but the two directions are **not symmetric**, and an earlier draft of this
section hid that:

1. Every field the DCDD marks **must-populate** has a mapping entry. Five of the 82.
2. No mapping entry names a field the DCDD does not contain. Total, over all entries.

The remaining **77 populated fields carry no mapping entry and are written blank**. That
is what the workbook permits — it marks five fields Required and asks for the rest only if
you have them — but "77 fields are deliberately blank" and "77 fields were forgotten" look
identical in a mapping file. So the blanking is a named decision
(`wd_mapping.blanked_fields`) with two properties asserted over it: no must-populate field
is ever in it, and the count is pinned, so a field that quietly stops being sent is a
change somebody has to look at.

Item 1 as originally written — "every populated DCDD field has a mapping entry" — claimed
a guarantee 77 fields short of what anything enforced.

One check alone is worthless: the first misses a typo'd source, the second misses a
forgotten field. Together a drifting mapping goes red instead of exporting a blank column.

## 6. Issued values must be frozen

`AME002` (Invoice Date) and `AME006` (Line Number) are **not** derived and must never be
recomputed. `metadata/entities/csat_invoice_line_gie.yml` already states the reasoning:

> A satellite is recomputed; anything recomputed can change. If a re-run changed either
> value the customer's copy would no longer match ours and NOTHING WOULD FAIL — every load
> succeeds, every gate stays green, and the first symptom is a dispute.

They are issued once into `control.ctl_invoice_issuance` by `checks/invoice_issue.py` and
read from there by the export. The export never derives them. A row whose issued values
are missing is a failure, not a value to compute on the spot.

## 7. Validation runs BEFORE anything is written

The DCDD names its own vocabulary. Over the populated set: **27 `CHECKREFERENCES`**, 12
`ISNUMERIC`, 6 `DATEFORMATCHECK`, 4 `MISSINGVALUE`, 4 `CHECKBOOLEAN`, 3
`MATCHVALUE_COMPNY`, 2 `MATCHVALUE_COSTC`.

Each becomes a check over the projected rows, failing with the offending field, rule and
row. DT/DTS applies these anyway; finding a violation here costs minutes, finding it in
Workday costs a round trip through Logan.

**No file is written unless every rule passes.** A partially valid file is worse than no
file: it looks loadable.

## 8. The two external dependencies, and why they must be LOUD

Both would otherwise fail silently, and that is the single most dangerous property of this
subsystem.

**PLT-2 gates money.** The run-as service principal is not in
`global_dataplatform_pipeline_job_runners`, which every mask function in
`governance/apply_masks.sql` admits by name, so every `debitamt`/`crdtamnt` it reads
returns NULL. Measured 27 September. A `wd_fin_export` file projecting amounts would be
written with NULL in every amount column and the load would report success.

**Subsystem E gates references.** `hub_wd_reference` exists but is empty: its `WORKDAY`
binding is inactive because `01_usnc_bronze_dev` has no `workday` schema. Until it is
populated, `CHECKREFERENCES` cannot be satisfied for the 27 fields that carry it.

**THE DESIGN RULE.** The export REFUSES to write a file whose money-sourced fields are
null on every row, naming PLT-2. It REFUSES to claim a `CHECKREFERENCES` pass it could not
perform, naming the empty reference table. Neither degrades to a warning, and neither is
suppressible by a flag — a flag is how a temporary suppression becomes permanent.

This is the repo's existing idiom: `loop1_reconciliation` reports `NOT_EVALUATED` rather
than passing over a control it cannot assert, and says why.

## 9. What is buildable today

Everything except the two values above: the mapping and its checks, the projection, the
join contract, the issuance ledger, the file writer, the CSV shape, and every syntactic
validation. Those are testable offline against fixtures with no workspace at all.

The dependencies are reached at the END, populating amounts and resolving references —
not at the start. Building in this order means PLT-2 landing late costs nothing already
built.

## 10. Non-goals

| deferred | to |
|---|---|
| Supplier, Customer, Accounting Journal, Supplier Invoice exports | later C iterations — same machinery, different DCDD |
| Delta Sharing, the EU catalog, the XML/API app, scheduled flows | **subsystem D** |
| Who may read `wd_fin_export` | **subsystem B** — the TDS grant is a labelled placeholder |
| Loading anything INTO Workday | Logan, through DT/DTS. This produces files; it does not call Workday. |
| Moving `invoice_export` from `vault_load` into `gold_build` | this spec's implementation, once it does real work |

## 11. Open questions and risks

**DEF-58 is the first task, not a footnote.** `csat_invoice_line_gie` declares 11 payload
columns and **zero expressions**. Nothing downstream can be built until it produces rows.
`invoice_amount` (AME003) reportedly needs a JOIN rather than a row-local expression.

**Two AME rules have no function yet.** `invoice_rules.py` implements AME004, AME005,
AME007, AME008, AME010, AME011, AME012, AME013. The satellite's own comment says it applies
**AME003 and AME009** as well, and neither appears in that module. Either they are genuinely
missing or the comment is stale; whichever it is must be settled before the mapping claims
to implement them.

**The API version question is unresolved and now nearer.** All five DCDDs declare
`v43.0`; Augustin's mail (sent 27 September) asks about v45.0 versus the v47.0 the
reference fetcher pins. If a mapping fails on a field the DCDD promised, the version is
the first thing to check.

**The reference-type inventory needs curation before `CHECKREFERENCES` can be trusted.**
`metadata/workday/reference_types.json` holds 386 types derived from the DCDDs, of which
13 are excluded as already modelled. That count was never intended as final.
