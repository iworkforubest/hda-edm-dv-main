# Ameren invoicing — the duplicate lines, resolved

Addendum to `2026-09-24-ameren-invoicing-design.md`. Supersedes the duplicate
measurement in `-real-feed-addendum.md`, which was taken over contaminated data.

## What the earlier measurement said, and why it was wrong

The design recorded this trade-off:

```
keep every row      3 invoices wrong   invents  44,050.32
remove duplicates   4 invoices wrong   loses       304.99
```

Both numbers were arithmetically correct and the conclusion drawn from them was
not, because the two lines describe **different invoices in different loads**.

`01_usnc_bronze_dev.sap_fieldglass_raw.invoices` holds three loads for `AEE1`:

| date | `META_run_id` | rows | invoices | amount |
|---|---|---|---|---|
| 2026-09-08 | *(empty)* | 5 | 3 | 7,190.52 |
| 2026-09-09 | `feeder_test_1` | 30 | 3 | 43,143.12 |
| 2026-09-11 | `5f354ada…` | 1,172 | 419 | 1,152,842.86 |

The first two are synthetic. They carry columns no invoice has — `Is_Test`,
`Mike_Tester` (`test_value_4`), `PO_2` (`collision-test-row4`) — and one of them
is named `feeder_test_1`. Together they are 35 rows across **3 invoices**
totalling 43,143.12. Those are the "3 invoices wrong" and essentially all of the
"44,050.32 invented". The figure measured test scaffolding, not a modelling
failure.

## What is true on the real extract

Restricted to the 2026-09-11 load, the 11-column transaction key leaves **4 keys
with repeats, 8 surplus rows, across 4 invoices**. For each of those four, the
lines were summed both ways and compared to the invoice's own stated total:

| invoice | `Gross_Supplier_Invoice_Amount` | keep all | dedup | reconciles |
|---|---|---|---|---|
| AEE1IN00124913 | 736.23 | 736.23 | 646.23 | keep all |
| AEE1IN00125063 | 810.25 | 810.25 | 630.25 | keep all |
| AEE1IN00125150 | 362.66 | 362.66 | 352.66 | keep all |
| AEE1IN00125213 | 412.86 | 412.86 | 387.87 | keep all |

**Keeping every row is exactly right.** It ties to the penny on all four.
Deduplicating loses 304.99 of real money. The repeated rows are not
re-deliveries; each one is money the supplier is owed.

## What the duplicates actually are

Every duplicate group is an **expense-sheet line**:

| invoice | ref | type | copies | amount | description |
|---|---|---|---|---|---|
| AEE1IN00124913 | AEE1ES00007656 | ES | 3 | 45.00 | `food` |
| AEE1IN00125063 | AEE1ES00007671 | ES | 5 | 45.00 | `food` |
| AEE1IN00125150 | AEE1ES00007639 | ES | 2 | 10.00 | `Breakfast 8/13` |
| AEE1IN00125213 | AEE1ES00007686 | ES | 2 | 24.99 | `Dinner` |

Of the real load's 1,057 timesheet rows, **not one** duplicates on the key. All
110 expense-sheet rows produce all 4 collisions. Amy's instinct — that a second
*timesheet* line requires something to differ — is correct and is now measured:
TS lines always differ. The collisions are expense receipts, where three meals
at 45.00 on one expense sheet are ordinary and Fieldglass gives them all the same
`Invoice_Line_Item_Ref` because that reference identifies the *sheet*, not the
line.

Adding `Expense_Description` to the key resolves 1 of the 4 groups and leaves 3.
It is not a fix, because `food`, `food`, `food` is a legitimate expense sheet.

## Consequences

1. **The model does not change.** `line_sibling_ordinal` preserves duplicates,
   which is what reconciliation demands. The design was right for a reason it had
   not yet measured.
2. **Deduplication must not be built.** The manual-review-then-remove flow
   discussed with Amy would delete 304.99 of billable expense claims. What is
   worth surfacing instead is a *report* of repeated keys, for confidence, with
   no removal path.
3. **The row filter now excludes synthetic rows**, on the four markers rather
   than on the two load dates — both express the same 35 rows today (0
   disagreements, measured), but a date list stops working the next time someone
   feeds this table. `verify_repo.py` asserts each marker by name; dropping any
   one clause fails the build.

## Also established: the header amount model

Amy's `Ameren Invoice Value Analysis.xlsx` decodes the header amounts. Her model
was tested against all 419 real invoices and **holds on every one**:

| identity | holds |
|---|---|
| Σ `Invoice_Line_Item_Amount_Supplier` = `Gross_Supplier_Invoice_Amount` | 419/419 |
| Σ `Invoice_Amount` (per line) = `Gross_Invoice_Amount` | 419/419 |
| Σ `Supplier_Adjustment_Amount` = `Supplier_Tax_Amount` | 419/419 |
| `Net_Invoice_Amount` = `Gross_Invoice_Amount` + `Invoice_Adjustment_Amount` | 419/419 |
| `Gross_Invoice_Amount` = `Gross_Supplier_Invoice_Amount` + Σ `MSP_Line_Amount` | 419/419 |

Two findings follow.

**The names are inverted.** `Net_Invoice_Amount` is the larger figure — it is
gross plus adjustment. `Gross_Invoice_Amount` is the net. This is only *visible*
on the 2 invoices of 419 that carry a non-zero adjustment, which is why it has
not bitten anyone yet. `sat_invoice_header` carries both under the source's
names; the descriptions there should say which is which.

**MSP margin is 24,054.04 across 380 of 419 invoices.** The client-facing total
is supplier lines *plus* MSP lines. This makes AME008's reconciliation target a
decision, not a detail:

- if gold's line `amount` is `invoice_line_item_amount_supplier`, the target must
  be `Gross_Supplier_Invoice_Amount`
- if gold bills the client, `amount` must be the per-line `Invoice_Amount` and
  the target `Gross_Invoice_Amount`

Mixing the two under-reconciles by exactly the MSP margin, on 380 of 419
invoices.

### Answered 24 September: invoice the CLIENT total, including MSP margin

Amy: *"Please generate the Ameren invoice against the client total, including the
MSP margin. The supplier total should remain the underlying cost and
reconciliation component, not the final invoiced amount... We will have some
variety in when MSP fee's show, they typically do not apply to expenses."*

So AME008's reconciliation target is **`Gross_Invoice_Amount`**, the line amount
gold emits is the per-line **`Invoice_Amount`**, and the supplier figures stay in
the vault as cost rather than as the invoiced value. The invoice to Ameren totals
**1,176,896.90**, not the supplier total of 1,152,842.86.

**The per-line identity this rests on holds on every line.** Measured over all
1,172 rows of the real extract:

```
Invoice_Amount = Invoice_Line_Item_Amount_Supplier + MSP_Line_Amount    1172 / 1172
```

**And her caveat is stronger than she stated it.** MSP does not "typically" skip
expenses — on this extract it never touches them, and never touches miscellaneous
invoice lines either:

| line type | lines | lines carrying MSP | MSP total | supplier | client |
|---|---|---|---|---|---|
| TS | 1,057 | **1,057** | 24,054.04 | 1,144,003.09 | 1,168,057.13 |
| ES | 110 | **0** | 0.00 | 8,110.81 | 8,110.81 |
| MI | 5 | **0** | 0.00 | 728.96 | 728.96 |

Every timesheet line carries MSP; no expense or miscellaneous line does.

**This was recorded as an observation and NOT encoded, and Amy then reversed it.**
The original reasoning stands on its own terms — she had said *"we will have some
variety in when MSP fee's show, they typically do not apply to expenses"*, and a
rule built from one client's weekly extract would reject a correct invoice the
first time another client was billed differently. So it was put to her.

**Answered 24 September:** *"I have confirmed that MSP fee's should not be applied
on expenses across all clients and therefore we should expect a record to reject
where this is present in the data."*

So it is a rule now, and `invoice_rules.msp_violation` implements it: an MSP fee on
an expense line rejects, with a reason. Absent and zero are not violations — `None`
is how the column arrives when Fieldglass has nothing to put there, and 39 of 419
invoices are wholly expenses.

**Miscellaneous is deliberately NOT covered, and that gap is pinned by a test.**
From the same message: *"For Misc Fee's it's slightly different they may or may not
attract an MSP fee (this is a newer feature). I have asked whether this is something
applied per misc invoice entry or if it's at a client level. If we can have some
with/without MSP fee's then we will need to pass the data through I think, if
however it's an all or nothing per client we can capture this in the ref data
tables as a rule."*

Two branches, neither chosen. Until one is, MI passes through — passing through
cannot reject an invoice that is correct under either answer, and rejecting can.
**If the answer is "all or nothing per client", the rule belongs in reference data
keyed by client, not in this function** — which is a different shape from the
across-all-clients rule expenses got, and worth knowing before it is built.

**The measurement was still worth taking.** It is what made the question precise
enough to answer: "no expense line in 110 carries a fee, is that the rule?" is
answerable, and "how do MSP fees work?" is not.

**It does not disturb the duplicate finding.** Every repeated key is an ES line,
and ES carries no MSP, so the client and supplier views of those four invoices are
the same number. Keeping every row still reconciles.

**One rename this makes load-bearing.** `nhl_invoice_line.invoice_amount` is the
per-line client net, while `csat_invoice_line_gie.invoice_amount` is AME003's
invoice gross repeated on every line. Two different grains under one name was
tolerable while gold was unimplemented and both were unused. Now one is the line
amount and the other is what it reconciles to, and a projection that crossed them
would produce an invoice whose lines each carried the whole invoice's value. They
must not keep the same name.

## Method note

Every wrong conclusion in this thread came from reading; every correction came
from querying. The 44,050.32 survived because it was never asked *which rows*
those were. `count(DISTINCT col)` skips NULLs, so the first attempt to find the
differing columns reported "0 columns vary" across seven rows that plainly
differed — the NULL-aware recount found 38.
