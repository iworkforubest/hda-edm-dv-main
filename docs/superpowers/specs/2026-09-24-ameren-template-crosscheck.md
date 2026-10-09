# Amy's 22 September invoicing template — cross-check

`2026.09.22_Ameren_Invoicing_Template.xlsx`, treated as the authority over the earlier
`MSP001_Ameren.xlsx`. Cross-checked against the live feed and against
`2026-09-24-ameren-invoicing-design.md` on 24 September 2026.

**It is a much better specification than the one the design was written from**, and it
closes three open questions while confirming — independently — the two hardest problems
the measurements found.

## What it settles

**AME009's date format: `MM/DD/YYYY`.** The design carried this as open question 2 because
the old workbook contradicted itself three ways — the rule said `DD/MM/YYYY`, its own
example was ISO, and all 14 sample dates were ISO. The new Billing Rules sheet says
`MM/DD/YYYY` for both Invoice Date and Accounting Date, which is US order and consistent
with a US client. **Closed.**

**Delivery: sFTP, weekly, Thursdays, via FileZilla. No invoice image, no named
recipient.** That was open question 5. **Closed**, and it confirms the design's judgement
that delivery is an integration concern rather than something the vault does.

**The supplier crosswalk exists and is complete.** The old master sheet had
`Supplier ID VMS` blank for all 23 rows. This one carries **29 suppliers with VMS codes**,
addresses, emails and payment terms. Checked against the live feed:

```
codes in the master                 29
codes in the lake (Ameren)          22
codes in both                       22      <- every lake code is in the master
names identical, byte for byte      22
codes in the master but not the lake 7      (no invoices in the 2-week window)
```

**`hub_supplier` can bind for real.** The design keys it on `supplier_code` and could only
hope that code would arrive; it has, and it resolves cleanly for every supplier that has
invoiced.

## What it confirms that we found independently

**The expense and miscellaneous coding gap.** The sheet records it as a known gap in its
own words: *"Fieldglass expense and miscellaneous invoices do not consistently provide the
correct Ameren coding. The future rule and ownership are still to be confirmed."* Two
rows, Expense and Miscellaneous, both `TBC`.

That is the same defect the measurements found from the other end — AME011/012/013's
module selection has nothing reliable to select from on expense and miscellaneous lines,
and it is on expense sheets that line identity collapses entirely (`AEE1ES00007667`
carrying three "Food" lines identical but for the amount). **Two independent routes to one
finding**, which is the strongest evidence available that it is real.

**Line numbering.** *"Start at 1 and number each output line within the invoice. No gaps
or duplicates."* That is exactly what `checks/invoice_issue.py` does and what
`control.ctl_invoice_issuance` freezes.

## What it does NOT settle

**The invoice date rule.** Billing Rules says *"Create the invoice date using the agreed
date rule"* and Invoice Setup lists the source as *"Invoice engine"*. Neither states the
rule. **Open question 1 stands** — and it is the one that made GIE's ownership matter,
because the code refuses to default it.

**Three items the sheet itself marks Open:** the Vertage invoicing legal entity (Guidant
Global, *"required before invoicing and Workday posting"* — this is the same legal-entity
work LEG-1 covers), the Workday company and customer, and the Nominal Code (`400100` for
testing only). **The first of the three was answered by Amy on 24 September — see below.**

### Answered 24 September: the invoicing legal entity is Guidant Global Inc

Amy: *"Is it possible to update the legal entity to be Guidant Global Inc, for Ameren this
is the billing entity."*

This closes the sheet's own first Open row. It is a business fact, it needed an owner to
state it, and it is now stated.

**It also lands on a hypothesis this repo had already recorded.**
`docs/legal_entity_design.md` observes that Guidant Global trades in two jurisdictions and
is therefore **at least two legal entities**, that `Inc.` is a US corporate form, and that
resolving the string "Guidant Global" to a single thing would be wrong in a way no gate
could catch, because both candidates are real and both are ours. Ameren is a US client —
the template's own Master Data sheet carries Missouri and Michigan addresses — and Amy has
named the `Inc.` form. That is consistent with the US entity and is corroboration, **not
proof**: she answered which entity bills Ameren, not how the `delphi_tech` feed's `buyer`
string resolves. The hypothesis on that row stays a hypothesis.

**One thing to confirm before it reaches a document: the spelling.** This repo writes
`Guidant Global, Inc.` — comma, trailing period — throughout `legal_entity_design.md`,
taken from the `buyer` string the Delphi feed actually delivers. Amy wrote `Guidant Global
Inc`. On an invoice sent to Ameren AP and on a Workday posting, the registered name is the
name, and the difference is not cosmetic. One of the two is the legal form; we should be
told which rather than picking.

**It does not unblock LEG-1.** `hub_legal_entity` is still a placeholder bound to
`PLACEHOLDER.reference.legal_entity` and absent from `active_sources`, because the roster
does not exist — one row per company across Impellam Group and HeadFirst Group, plus the
client legal entities the feeds name. Amy has given us one row of it, which is the row
Ameren invoicing needs and is not the roster.

## Discrepancies worth resolving before build

1. **`Amount`'s source is stated two ways.** Invoice Setup maps the Ameren `Amount` column
   to `Invoice_Amount`; Billing Rules derives it from `Source_Line_Amount + Tax_Amount`.
   In the live feed **`Amount` is blank on all 1,207 Ameren rows** while
   `Invoice_Line_Item_Amount_Supplier` is populated on every one. Confirmed with Adrian
   that the payable side is what this feed carries and the receivable returns from
   Workday — so neither sheet is describing what Bronze holds today, and the field that
   feeds the client-facing amount needs naming explicitly.

2. **Payment terms changed and nobody flagged it.** The old master said the client is
   `60 Days DOI`; this one says **`15 Days DOI`**. A fourfold change in when we get paid
   is not a formatting difference. Worth confirming it is intended rather than a
   transcription.

3. **"Gold is already invoice-ready" introduces reference-data enrichment.** Billing Rules
   says to copy the *"reference-data-enriched"* Project Number, Task Number, Expenditure
   Type and Expenditure Organization. The design has no enrichment step — it projects what
   the module-selection rules choose. Where that enrichment happens, and from which
   reference data, is undefined. The Master Data sheet's Expense/Miscellaneous coding
   table is presumably its input, and that table is `TBC`.

## What the template does not change

**The Bronze shape matches the live table exactly.** All **161** columns on the template's
Bronze Transaction Data sheet exist in
`01_usnc_bronze_dev.sap_fieldglass_raw.invoices`; the live table carries 8 more, all
belonging to other clients. The template is a faithful picture of the feed.

**The output is unchanged** — sheet 5 carries the same 555 lines across 225 invoices, with
the same thirteen fields.

**There is still no line-item sequence.** `TIMESHEET_ID` appeared in the live table and not
the template, which looked promising; it is **empty on all 1,207 Ameren rows**. The line
identity design in `2026-09-24-ameren-real-feed-addendum.md` stands, and the ask on the
Fieldglass integration stands with it.

## Workday master data: fetched 24 September, and mostly not there

The template marks Workday company, customer and nominal code as Open. Those are ours to
retrieve rather than Amy's to key, so they were retrieved. Two of the four are usable and
two are empty of anything real:

```
Company_Reference_ID   82   REAL   NL104 HeadFirst BV, BE110 Source Automation Belgium BV
Ledger_Account_ID     135   REAL   101000:Tangible fixed assets - Cost
Customer_ID            29   OTHER CLIENTS -- Biogen, Johnson & Johnson, Lonza. NO AMEREN.
Supplier_ID            14   DEMO DATA -- ACME Lawn Care, DNU_UK_Supplier. NONE of the 29.
```

**Ameren does not exist as a Workday customer, and not one of the 29 suppliers exists as a
Workday supplier.** That is a configuration dependency on the Workday side that no
document had recorded, and it blocks posting rather than building.

Two corrections made while establishing this, recorded because both were wrong in the
direction of sounding confident. An earlier note said `Supplier ID Workday` could be
filled for up to 9 suppliers — that came from the 7 September probe, the count is now 14,
and the honest number is **zero**, because none of them is a real supplier. And the first
read of the response used the `descriptor` field, which carries the object TYPE
("Supplier", "Customer"), leading briefly to the conclusion that the service returns no
names at all. It does: `referenced_object_descriptor`.

### Decision: create the records by hand, read the ids back by service

Both writes exist and were verified against the live tenant WSDL:

```
Put_Supplier      Resource_Management   (97 Put_* operations)
Submit_Customer   Revenue_Management    (there is no bare Put_Customer;
                                         Financial_Management's Put_Basic_Customer is a
                                         cut-down variant)
```

They are not used, and the reasons are not technical:

* **The governance model says so.** LEG-7 established that master data creation sits with
  the CBO Office, which holds the approvals and the evidence, and that **CPTO implements
  approved changes but does not own the structure**. A supplier created by our integration
  user is a master-data record with no approval trail, created by the team that has just
  agreed it does not own master data.
* **The volume does not justify it.** Twenty-nine suppliers and one customer, once.
  Clients 2–54 will have their own suppliers created through the same governed process, so
  the automation has no second use.
* **The permission is a separate ask with real blast radius.** Reading a WSDL is not
  executing an operation: `Put_Supplier` needs its domain security policy granted to
  `ISU_Databricks`. That is a WRITE grant on supplier master data in a financial system,
  requested for a one-off load, and easier to grant than to remove.
* **Supplier master carries payment details.** Remit-to and eventually bank details. An
  integration user that can create suppliers can redirect payments. Not a reason never to
  automate it — a reason not to automate it for thirty records.

**The service is used for the half that repeats:** once the records exist,
`Get_References` on `Supplier_ID` and `Customer_ID` populates the blank Workday-id columns
by name match, re-runs whenever they change, and needs no keying.

**Open, and carried from WDJ-5:** this is the IMPLEMENTATION tenant. Whether master data
configured there migrates to production, or has to be created again, is unanswered — and
it decides whether hand-entry happens once or twice.
