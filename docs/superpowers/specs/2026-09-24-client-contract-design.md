# Client contracts: holding the Workday contract id, and posting with it

Design, 24 September 2026. Written from Amy Keser's requirement of the same date
and from what the lake actually holds, measured rather than assumed.

## The requirement, in her words

> We will have customer order ID's (referred to in Workday as client contracts).
> This is a brand new business process Fred is introducing whereby: a deal is
> recorded in Hubspot; this converts to a quote via the CPQ tool; the quote is
> accepted and converts into a client contract; at this point the contract
> information is entered (manually initially while we bed this in) into Workday.
> **The Workday client contract ID needs to be held in the respective client
> record within the tables in Databricks.** As we raise invoicing we need to
> ensure **the Workday posting contains the client contract ID**.

Two deliverables, and they are not the same thing. One is a fact to store. The
other is a fact that must reach a document, unchanged, every time it is produced.

## What exists today, measured 24 September

| | |
|---|---|
| **Hubspot deals** | **LANDED.** `01_usnc_bronze_dev.hubspot_raw.deals`, 15,430 rows, carrying `total_contract_value`, `contract_start_date`, `end_date_contract`, `dealstage`, `deal_stage_name`, `pipeline`, `deal_company_id` |
| **Hubspot companies** | **LANDED.** 2,457 distinct companies, incl. `Ameren Corporation`; `hs_parent_company_id` gives client parent/child |
| **CPQ quotes** | **NOWHERE.** Zero tables matching cpq/quote/contract across all **48** bronze schemas |
| **Workday client contracts** | **RETRIEVABLE AND EMPTY.** `Customer_Contract_Reference_ID` is a valid `Get_References` type — `Customer_Contract_ID` is not, and is rejected as "Not a valid Reference ID Type" — and it returns **0 results** today |
| **Workday customers** | 29, and **Ameren is not among them** |
| **A Workday posting path** | **DOES NOT EXIST** in this repo. No posting code, and `accounts_receivable.client_invoice` is a one-column stub (`docnumbr`) |

**The empty contract list is the process, not a fault.** Amy says this is brand
new and keyed manually at first. The retrieval works and returns nothing, which is
the honest state of a process that has not started.

## Scope

**In:** the contract as a modelled entity, its relationship to the client, its
commercial detail, and the rule that decides which contract an invoice posts
against — frozen so a re-run cannot change it.

**Out:** CPQ. There is no quote data and no quote system we read. The chain Amy
describes is deal → quote → contract, and this design binds the two ends that
exist. A quote is a step in a business process, not necessarily a row we need.

**Out:** building the Workday posting itself. This design makes the contract id
available to it and gates its absence; who posts and how is unowned, the same
place `2026-09-07-workday-reference-source.md` leaves the landing.

## Architecture

```
hub_client_contract              the Workday client contract -- federated on Workday
  lnk_client_contract_party      contract -> legal_entity (the client)
  esat_client_contract_party     when that relationship was in force
  sat_client_contract_details    commercial detail, ONE TABLE PER SOURCE:
                                   _WORKDAY  the id, status, dates as keyed
                                   _HUBSPOT  value, dates, stage as sold
ctl_invoice_contract             the contract each invoice was ISSUED against
```

### The contract is a hub, not a column on the client

Amy's phrasing — *"held in the respective client record"* — describes where a
reader should find it, not where it should be stored. A contract has its own
identity (Workday assigns the id), its own lifecycle (it starts, it ends, it is
superseded) and its own attributes (value, dates, status). A column on
`hub_legal_entity` would be a descriptive attribute on a hub, which this model
refuses, and it would be wrong the first time a client has two contracts.

Gold projects the contract onto the client record, which is what she asked to see.

**Federated on Workday**, like `hub_consolidation_group` and unlike
`hub_legal_entity`: the contract id is Workday's fact about its own configuration.
HFIG does not assign it, and no second system has an opinion about it.

### The client is reached by a link, and it is dated

`lnk_client_contract_party` joins `client_contract` to `legal_entity` — two
different hubs, so an ordinary link with no roles. `esat_client_contract_party`
dates it, driven on the contract: **at most one client per contract at a time.**

That direction is deliberate. Driving on the client would say "one contract per
client", which is false — Amy's own caveat about a mix of services, *"where we
have a mix of services and one is professional services (SOW)"*, is a client with
more than one commercial arrangement.

### Commercial detail has two sources and therefore two tables

`sat_client_contract_details` declares two bindings, which the raw vault emits as
`sat_client_contract_details_workday` and `sat_client_contract_details_hubspot`.
One satellite per source is this model's rule and it earns its keep here: the two
disagree by construction. Workday holds the contract **as keyed**, Hubspot the deal
**as sold**, and a single merged row would hide which one a number came from on the
day finance asks why they differ.

**Decided 24 September: Hubspot is the system of record for deal and commercial
data.** Adrian. So the split is clean and each system is authoritative for what it
actually owns:

| | authoritative for |
|---|---|
| **Workday** | the **contract identifier** — the thing Amy requires the posting to carry |
| **Hubspot** | the **commercial facts** — value, contract dates, stage |

The raw vault still lands both, unmerged, because a satellite records what a source
sent; what changes is that **Gold projects Hubspot's commercial values** rather than
leaving the choice open. If the two disagree on value, Hubspot is right and Workday
is a keying discrepancy worth reporting, not a second opinion to reconcile.

**And that decision creates a dependency this design must name rather than
discover later.** If contract *dates* are Hubspot's, then the rule that selects
which contract an invoice posts against — the contract in force on the invoice
date — reads its dates from **Hubspot** while the id it emits comes from
**Workday**. That only works if a Workday contract can be joined to its Hubspot
deal, and **nothing today states that join**: Hubspot deals key on
`hs_object_id` and carry `deal_company_id`; Workday contracts are a reference id
with a customer. There is no shared key, and Workday holds zero contracts so
there is nothing yet to join against.

**Adrian's hypothesis, 24 September: the contract id may be in Hubspot too, and
may be replicated to Workday via Databricks.** Tested against what we hold, and
the two halves have different answers.

**It is NOT in the Hubspot data in Bronze.** `deals` carries exactly **35 columns
in both `hubspot` and `hubspot_raw`** — the schemas differ only in row count, not
shape — and the only identifier on a deal is `hs_object_id`. There is no order,
contract, quote or Workday field. Nor is one hiding in the ingestion's
`rescued_data`, which is populated on 5,033 of 15,430 rows and holds exactly
**five** keys, all of them triple-underscore spellings of columns we already have
(`fee___contracting`, `sales_cycle___global`, `regional_group___global`,
`service_type___global_products`) plus `_file_path`. That is schema drift between
file versions, not a hidden field.

**But absence in Bronze is not absence in Hubspot**, and that distinction is the
whole point. 35 properties is plainly a curated subset — a Hubspot deal object
carries far more, custom properties included — so a customer-order id could exist
upstream and simply not be replicated. **We cannot see Hubspot, only what is
landed from it.** So the question is the CRM team's, and it has three branches:
does such a property exist; if so can it be added to the replication; and if not,
is `hs_object_id` itself the customer order id Amy means.

**The replication idea is better than either option above, and it dissolves the
problem rather than answering it.** If Databricks replicates the contract id
between Hubspot and Workday, then the join is something **we create** rather than
something we discover: we would hold both identifiers by construction, at the
moment we write one, and no inference on client plus dates would ever be needed.

It also costs more than it looks, though **less than first recorded here**.
Adrian clarified on 24 September that **both Hubspot and Workday write to
Databricks** — traffic flows towards us, and Workday's reference values land in
Bronze as Workday reference data. So the general worry that this accelerator has
never written to Workday does not apply to reference data at all.

It still applies to **this** case, and the distinction matters: Workday publishing
its own labels into our lake is not the same act as us writing a Hubspot deal id
back into a Workday contract record. That second direction is what option (b)
needs, and it remains a different posture — the credential arrangement PLT-7 asks
for rather than the stop-gap file, and an owner for the write, landing in the same
unowned space as "who performs the Workday posting" (question 4). Worth doing, not
worth assuming. Recorded as open question 6.

## The part that must not drift: which contract an invoice posts against

The posting must carry the contract id. Which contract?

**The one in force for that client on the invoice date**, resolved once and
**frozen**, exactly as the invoice date and line numbers already are.

This is the same failure the Ameren design named and it is worth naming again: a
satellite is recomputed, and anything recomputed can change. If a re-run selected a
different contract — because a new one was keyed in Workday, or an end date was
corrected — the customer's posting and ours would no longer agree and **nothing
would fail**. Every load succeeds, every gate stays green, and the first symptom is
a reconciliation dispute in somebody's ledger.

So `ctl_invoice_contract` records, once per invoice: the invoice, the contract id
selected, and when. It is append-only and lives in the control schema beside
`ctl_invoice_issuance`, whose shape and discipline it copies.

**Selection refuses rather than guesses.** Two open contracts for one client on the
invoice date is ambiguity, not a tie to break — the same ruling
`accelerator.hierarchy.parent_at` already makes for two open parents, and for the
same reason: picking one makes the answer depend on row order, silently.

**No contract in force is a release gate, not a warning.** An invoice whose client
has no contract on its date is routed to the exception set with a reason, and is
not released. Posting an invoice against no contract is precisely what Fred's
process exists to stop.

## Data flow

```
Hubspot (landed)          -> sat_client_contract_details_hubspot   deal as sold
Workday Get_References    -> hub_client_contract                   the id
  Customer_Contract_Reference_ID
                          -> sat_client_contract_details_workday   as keyed
                          -> lnk_client_contract_party + esat      which client, when

at issue:  invoice + client + invoice_date
             -> select the contract in force
             -> ctl_invoice_contract   (once, frozen, append-only)
             -> the WORKDAY POSTING carries contract_id
```

**The contract id does NOT go on the Ameren invoice.** Those thirteen gold fields
are the customer's own template columns — `Invoice Number`, `Invoice Date`,
`Invoice Amount`, `Description`, `Line Description`, `Line Number`, `Line Type`,
`Amount`, `Accounting Date`, `Project Number`, `Task Number`, `Expenditure Type`,
`Expenditure Organization` — and they are a contract with Ameren AP, not ours to
extend. Amy's requirement is that **the Workday posting** carries the contract id;
the document sent to the client is unchanged. Two outputs, two field lists, and
conflating them would either break Ameren's intake or lose the id.

Retrieval reuses `tools/fetch_workday_references.py` unchanged —
`Customer_Contract_Reference_ID` is a reference type like any other. **Landing is
gated on the same destination nobody owns**, so every binding here names
`PLACEHOLDER` and nothing loads, exactly like the legal-entity families.

## Testing

**Structural:** the contract hub is federated and carries no descriptive
attributes; the party link joins two different hubs and carries no payload; the
effectivity satellite drives on the **contract**; `ctl_invoice_contract` is
append-only and in the control schema's table list.

**Behavioural, and these are the ones that would catch a real mistake:**

* **A re-run selects the same contract.** Issue an invoice, change the contract
  data underneath it, re-run, and assert the recorded contract id is unchanged.
  This is the freezing requirement, and it is the only check that would notice the
  failure that costs money.
* **Two open contracts is refused**, not resolved by order — proven by a fixture
  with two, and by reversing that fixture's order.
* **No contract in force blocks release**, with a reason a human can act on.
* **The selection respects the invoice date**, not today's date: a contract that
  ended before the invoice date is not selected.

Every new check mutation-proven, per this repo's standing discipline.

## Open questions

1. ~~**Can a client have two contracts in force at once?**~~ **ANSWERED 25
   September — yes.** Amy: *"it is possible that a client can have more than one
   contract in place, we will need to have a way to map the client to the Workday
   Client Contract ID. It is likely there will be a differentiator in the data
   which we will need to map."*

   **So the selection rule above is not sufficient.** Client plus invoice date
   returns more than one contract, and `parent_at`'s refusal to guess — the right
   behaviour — would refuse every such invoice rather than post it. The design
   needs a third term, and **what that term is remains unknown**: she expects a
   differentiator in the data and has not yet identified it.

   **This does not block, and the reason is worth stating.** Contract ids arrive
   for net-new clients through the Workday implementation, so the data does not
   exist to model against yet. Amy has shared scenarios worked through with Fred.
   The honest position is that `ctl_invoice_contract` and the freezing discipline
   stand, and the **selection predicate is deliberately unwritten** until the
   differentiator is known — writing it now would be inventing a business rule.
   *Holder: Amy Keser, with Fred.*
2. **Does the posting carry one contract id per invoice, or per line?** The design
   assumes per invoice. A client billing several services on one document would
   break that. *Holder: Amy Keser, with whoever owns the Workday posting.*
3. ~~**Which system's contract value does Gold report?**~~ **ANSWERED 24 September
   — Hubspot.** It is the main system for deal and commercial data; Workday is
   authoritative for the contract identifier only. *Answered by Adrian.*
4. **Who performs the Workday posting?** No posting code exists here. Until it has
   an owner, this design can make the contract id available and gate its absence,
   and can do nothing about whether it reaches Workday. *Holder: integration.*
5. **Is Hubspot's `deal` the same grain as a contract?** A deal carries
   `total_contract_value` and contract dates, which suggests one deal is one
   contract — but 15,430 deals against 2,457 companies is six per company, and
   nothing yet says how many became contracts. Measure before binding.
   *Holder: Eva / CRM team, who own Hubspot; then measurement once contracts exist in
   Workday to join to.*
6. **How is a Workday contract joined to its Hubspot deal?** Created by question 3's
   answer: the id comes from Workday and the commercial facts from Hubspot, so the
   two must be joinable, and no shared key exists today. Three candidate answers, in
   the order they should be considered:
   **(a)** the contract id already exists as a Hubspot property that is simply not
   replicated into Bronze — cheapest if true, and only the CRM team can say;
   **(b)** Databricks replicates the id between the two systems, which dissolves the
   join entirely because we would hold both ids by construction — best outcome, but
   it makes us **write** to Workday for the first time, which needs PLT-7 and an
   owner; **(c)** Workday's record carries the Hubspot deal id when keyed in, which
   is cheap **only while the process is manual and being designed**, and that is now.
   *Holder: Eva / CRM team for (a), Fred with Amy for (b) and (c).*

## What a second invoice shape does to this design

Measured 25 September, while testing a blocker Amy raised: **ten of the twelve
buyer codes in the invoice feed identify an invoice by `Consolidated_Invoice_ID`,
not `Invoice_ID`.** Ameren is one of only two that do the opposite, and no row
anywhere carries both.

This design assumes one invoice, one client, one contract. A consolidated invoice
spanning several client legal entities breaks that assumption in the place it is
least recoverable: **the same consolidated invoice id posting against more than one
Workday client id, which an AR ledger must refuse.** Amy names this as a genuine
blocker and is analysing how many scenarios have it.

Nothing here should be built to guess the answer. What this design can say is that
`ctl_invoice_contract` records the contract **per invoice**, and if an invoice can
belong to several client entities then that grain is wrong — it would become per
invoice **per posting entity**. Recorded so the grain is revisited deliberately
rather than inherited.

## Success criteria

* Every released invoice has a contract id recorded once, unchanged since, and
  available to the Workday posting — **and the thirteen fields sent to Ameren are
  unchanged**, because that list is the customer's contract.
* An invoice whose client has no contract in force on its date is not released.
* Two contracts in force for one client on one date raises, and is never resolved
  silently.
* Workday and Hubspot contract detail are both queryable and separately
  attributable.
* Onboarding a 55th client requires no change to this model.
