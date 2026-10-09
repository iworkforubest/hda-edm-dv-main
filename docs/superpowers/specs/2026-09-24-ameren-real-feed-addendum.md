# The Ameren invoice feed exists — addendum and correction

> **Superseded in part (24 September):** the duplicate measurement below was taken
> over data that includes 35 synthetic test rows. See
> `2026-09-24-ameren-invoicing-duplicate-resolution.md` — on the real extract,
> keeping every row reconciles exactly and deduplicating loses 304.99.

Amends `2026-09-24-ameren-invoicing-design.md`. Written 24 September 2026, after the
platform team said the feed had landed and it turned out they were right.

## The correction

The design says, repeatedly and as a load-bearing premise, that **no invoice feed exists
in Bronze** — "measured: 0 invoice tables and 0 `supplier_code` columns across 302
`fieldglass_raw` tables". **That is wrong.** The feed is at

    01_usnc_bronze_dev.sap_fieldglass_raw.invoices

alongside `sap_fieldglass.invoices` and a `buyer_invoice_audit`.

**How the measurement missed it, because the shape of the mistake matters more than the
mistake.** Three searches were run and all three were narrower than the claim they were
used to support:

* the invoice search filtered `table_schema='fieldglass_raw'`;
* the `supplier_code` search filtered the same schema;
* the cross-schema search matched `table_name LIKE '%ameren%'`, which a table named for
  its source system does not match.

The schema listing was ordered by row count and only its first rows were ever read.
**There are 48 schemas in that catalog.** So the evidence supported "none in
`fieldglass_raw`, and none named for Ameren", and what was written down was "there is no
invoice feed in Bronze". This is the same failure that the Workday XSD regex produced
earlier on this programme: a search that silently drops what it cannot match, reported as
a fact about the thing searched.

## What the feed actually carries

169 columns, 29,426 rows, 12 buyer codes — **one wide multi-client table, not one table
per tenant.** Each client populates a different subset, and **only AEE1 carries
invoice-shaped rows**: 1,207 rows, 422 invoices, with `Supplier_Code`,
`Invoice_Line_Item_Ref` and `Worker_ID` populated on every one. Of the other 28,219 rows,
none has an `Invoice_ID` at all.

It carries what the model needs, including the two things the design treated as open:

* **`Supplier_Code` and `Supplier`** — 22 each, 1:1. `hub_supplier` can key on the code,
  which the design could only hope for. The measured evidence that a name cannot be the
  key stands; the code now exists to key on instead.
* **The nine per-module custom fields**, exactly as Ruling 3 predicted against the brief:
  `CF_Time_Sheet_*`, `CF_Expense_Sheet_*` and `CF_Miscellaneous_Invoice_*` for task code,
  expenditure type and expenditure organization. That ruling was a judgement call. The
  real feed validates it.
* Both amounts and the margin: `Invoice_Line_Item_Amount_Supplier`, `MSP_Line_Amount`,
  `Tax_Amount`.

**`Amount` is blank on all 1,207 rows, and that is expected, not a gap** — confirmed by
Adrian: the payable side is what this feed is for, and the receivable comes back from
Workday. AME008 therefore does not gate this stage.

## The source emits no line identifier, and the extract is lossy

`Invoice_Line_Item_Ref` looks like a line id and is not. It is a timesheet or
expense-sheet reference: **422 distinct across 1,207 rows**. Beneath one reference the
rows differ by `Task_Code` — and on expense sheets, not even by that:

    AEE1ES00007667  Task_Code=Food  wk=08/27/2026  supplier_amt=9.90
    AEE1ES00007667  Task_Code=Food  wk=08/27/2026  supplier_amt=13.78
    AEE1ES00007667  Task_Code=Food  wk=08/27/2026  supplier_amt=18.83

Three food receipts, identical in all 169 columns but the amount. **A fourth receipt for
a repeated amount would be invisible to us.** That is a loss in the extract; no
derivation downstream can recover it.

Measured at `(Invoice_ID, Invoice_Line_Item_Ref, Task_Code)`: 1,016 groups of one, 59
groups holding genuinely different rows, 5 groups byte-identical.

### The key that works, and why this shape

Identity is **the line's own content plus its position among genuinely identical
siblings**:

    PARTITION BY Invoice_ID, Invoice_Line_Item_Ref, Invoice_Line_Item_Type, Task_Code,
                 Rate_Category, Hours, Rate, Invoice_Line_Item_Amount_Supplier,
                 Weekending_Date, Worker_ID, Project_Code

**Measured unique across all 1,207 rows**, with only 38 rows needing an ordinal above 1
and the largest sibling group at 7.

It is stable in the way that matters: every key depends only on its own content and its
own siblings, so **a line added later changes no existing key**. Ties occur only between
rows that are identical in everything meaningful, where the assignment is arbitrary and
harmless because the rows are interchangeable.

**Partitioning by a subset would be the bug.** Two rows differing in an unpartitioned
column would both take ordinal 1 and collide, or swap between loads and churn the
hashdiff for ever. The transaction key must therefore equal the partition columns, minus
`Invoice_ID` — the invoice is already a parent hub, and repeating it would key it twice.

**This is not AME006.** The line number Ameren sees is assigned once by
`checks/invoice_issue.py` and frozen in `control.ctl_invoice_issuance`. The ordinal exists
only so the raw vault can tell two identical rows apart, and never reaches an invoice.

## Why this is a re-key, not an edit — and what blocked the first attempt

An attempt to repoint the bindings was made and deliberately reverted. `verify_repo`
failed ten checks, and it was right to:

> a hash key's composition changed. Every stored key built the old way is now unjoinable
> to every key built the new way. Run `tools/emit_key_components.py`, read the diff, and
> treat it as a **re-keying of the estate — not a regeneration**.

Three conflicts need resolving before this lands, and none should be fixed by weakening a
gate:

1. **The view cannot live in `governance/*.sql`.** A gate forbids `CREATE VIEW` there.
   The repo's mechanism for a conformance view is `metadata/source_unions.yml` plus
   `checks/apply_source_unions.py` — which today can express neither a **row filter** nor
   a **derived column**, and both are needed here. Extending a job-critical shared script
   is its own task with its own tests.
2. **The name `v_fieldglass_us_invoice` is reserved** by the deferred-union block, and a
   gate specifically refuses any binding naming it — that gate exists to stop the
   NO_TABLES landmine being resurrected. The deferral and this view must be reconciled
   deliberately: what was planned as a UNION over per-tenant tables turns out to be a
   FILTER over one multi-client table.
3. **Three gates written in the previous branch now assert the wrong thing** — they
   require the invoice bindings to name the fake placeholder catalog, and require the old
   single-column transaction key. They are correct today and obsolete the moment the feed
   is bound. Updating them is part of the change, not a workaround.

## What is settled, and what is not

**Settled:** the feed's location and shape; that `Supplier_Code` exists and is 1:1 with
the name; that the per-module fields are real; that `Amount` is absent by design; the
line-identity design, measured unique on real data; the column mapping (all 24 columns
confirmed present).

**Not settled:** where the view lives and how it is built; how the deferred union and this
filter are reconciled; whether the filter keys on `Buyer_Code` or on "has an invoice id"
once a second client's invoices land.

**Open with the Fieldglass integration:** emit a line-item sequence. Everything above is
a workaround for its absence, and two byte-identical expense lines remain one line to us
until it exists.
