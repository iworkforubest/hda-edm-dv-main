# Open items — as of 7 Sep 2026

> **Resume here.** This is the whole live board and all of it is current. Read it top to
> bottom, then `git log`. If it has grown long enough that you would not, that is the
> signal to move the closed parts into `DECISION_LOG.md` — it happened once on 5 September
> at 3,669 lines and will happen again.
>
> Closed work lives in [`DECISION_LOG.md`](DECISION_LOG.md), newest first: what was
> measured, what was decided, and why. **`DEF-nn` and `BRZ-nn` identifiers cited in code
> comments are written up there** — or in `bronze_layer_work_requests.html` for the BRZ
> asks, or in the specs for the earliest few. `verify_repo.py` fails the build on any
> cited identifier that is written up nowhere.

## Where this stands at the restart — 25 September

**`main` is clean and green, no open pull requests, 1212 verification checks and the
structural suite passing.** `git log` is the authority; PRs #14–#24 are this session.

### The one blocker, and it is external

**Three silver schemas are owned by a principal that no longer exists.** The Microsoft
tenant moved `headfirst.group` → `vertage.com`; `business_vault`, `control` and
`governance` kept the old owner, while the catalog and `raw_vault` moved to the Terraform
service principal. **Neither identity can complete a load** — measured: the SP owns
`raw_vault` but has nothing on `control`; the live user has `USE_SCHEMA` only and no
`CREATE SCHEMA` on the catalog, so **there is no workaround**.

Reported to Michael 25 September as the reply his tenant-move note invited. Two acceptable
answers: `adrian.turcu@vertage.com`, or the SP that owns the catalog — the latter tidier,
and it lets `run_as` move in the same change.

**Do not let the `run_as` half be dropped.** `var.service_principal` is `""` on
`usnc_tds`, so the job runs as whoever triggers it. The ownership fix will *look* like it
resolved everything; it will not.

### What is proven, so a fresh session need not re-establish it

* **The accelerator deploys cleanly.** 25 September: 155 files, both pipelines and the job
  updated, bundle validates, **21 active bindings all resolve to real tables**.
  `vault_load` reached its **third task of 26** before the permission wall. This is a
  permission, not a defect.
* **A real vault already exists**: `raw_vault`, **39 objects, 17.9M rows** — GP, UKG,
  Bullhorn. Queryable today.
* **Six entities wait on the permission**: `hub_invoice`, `hub_supplier`,
  `hub_organisation`/FG, `hub_worker`/FG, `nhl_invoice_line`, `sat_invoice_header`, plus
  the union view over `sap_fieldglass_raw`.

### Facts that cost time to establish — do not re-derive, and do not assume

* **`sap_fieldglass_raw` is ours. `fieldglass_raw` and `fieldglass_client_owned_raw` are
  not.** Three schemas beginning `fieldglass`; one table and 29,426 rows versus 302 tables
  and 14.8M. **Name the schema, never the system.**
* **Ameren: 419 invoices, 1,172 lines, client total 1,176,896.90** (supplier 1,152,842.86
  + MSP 24,054.04). Still current — the feed delivered 4,068 rows on 24 September and none
  were Ameren.
* **Bronze holds 35 synthetic rows** carrying `Is_Test`, `Mike_Tester`, `PO_2`
  (`collision-test-row4`), `META_run_id` = `feeder_test_1`. Excluded by the union filter.
* **Duplicates are kept.** All four repeated keys are expense lines and reconcile exactly;
  deduplicating loses 304.99.
* **MSP rejects on expense lines** (Amy, across all clients). **Miscellaneous passes
  through** — open, and its two answers need different implementations.
* **Hubspot holds the client hierarchy** (`companies.hs_parent_company_id`) and the
  commercial half of a contract. **Eva / CRM own it**, not Data & AI. `_raw` is the right
  schema.
* **Workday**: everything measured is the **implementation** tenant. 82 companies, a
  36-node consolidation tree, **zero** customer contracts, Ameren not a customer. **None of
  it is promotable**; if Paul names another tenant it all needs re-measuring.
* **A binding scan misses the Ameren dependency** — it reads `sap_fieldglass_raw` through a
  silver union view. And **`information_schema.columns` undercounts tables**; use
  `.tables`.

### Waiting on other people

| who | what |
|---|---|
| **Michael** | three `schemas update` calls; the service principal's name for `run_as` |
| **Amy** | invoice date rule; can a client hold two contracts at once; misc-fee shape; tax-line coding; nominal code; task-code examples; who delivers to Ameren AP |
| **Eva / CRM** | buyer code → Hubspot company mapping; is one deal one contract |
| **Paul** | which Workday tenant |

### Unblocked work, if you want some

* **The client-contract plan** — spec is on `main`. Its first open question (two contracts
  at once) is with Amy and decides whether the design is sufficient, so planning around it
  risks rework.
* **The quarantine ratio** — `qtn_job_request` holds **57,886** against `hub_job_request`'s
  **52,747**. Probably benign, since quarantine is append-only and accumulates across runs,
  but nobody has explained it.
* **Source ownership** — ten bronze sources have no owner recorded, and only one source in
  the lake has September data. See `docs/bronze_source_inventory.md`.

### The lesson this session kept paying for

**Findings from reading were wrong; findings from querying were right.** Three claims
written into specs this month — *"no invoice feed in Bronze"*, *"keeping every row invents
44,050"*, *"no client-structure source anywhere"* — were all confident, all wrong, and each
corrected by a query taking under a minute. The third surfaced only because Adrian mentioned
Hubspot in passing. **Measure before asserting, and prefer the lake to the document.**

## Where this stood at the last restart — 7 September, end of the second session

**`main` is clean and green with no open pull requests; `git log` is the authority.** Eighty
PRs merged. The last three of them record an exchange with Platform and one correction to
how we ask them for things.

**Nothing is waiting on us to write or send anything.** The chase for the service
principal's name went to Michael on 7 September —
`docs/platform_sp_name_chase.md`, kept after sending as the record of how we chased.

### MOVED 23 Sep — the repository now lives in the organisation

**`hfg.ghe.com/data-engineering/hda-edm-dv` is the repository.** `origin` points there and
`main` tracks it. The personal remote is kept as **`personal`** — nothing was deleted, and it
is the only place the review threads of PRs #83 and #84 still exist. The code is entirely on
GHE; only that discussion stayed behind.

**Three things the pasted setup instructions would have broken**, recorded because the
boilerplate is the same on every new repository and it assumes an empty one:

* `git remote add origin` fails — origin already existed.
* **`git branch -M main` renames whatever branch you are on.** At the time that was
  `docs/plt-measured-23sep`, not `main`. It would have renamed a feature branch over the
  real one.
* Two PRs were open. Migrating first would have stranded both on the old remote; they were
  merged before the move so `main` carried everything.

**The gates had registered and never run, which is the failure worth remembering.** The
first push to a new repository creates the default branch **without firing `on: push`**, so
`verify` sat there *active with zero runs*. **The repo read as green on a history that had
executed nowhere** — the same shape as a check whose stated guarantee exceeds what it can
detect, except here the check was not running at all. Fixed by adding `workflow_dispatch`,
so "has this actually run here?" is answerable without inventing a commit to answer it. All
three gates now pass on GHE.

**Branch protection on `main`**, applied and read back rather than assumed:

| setting | value |
|---|---|
| required checks | `offline gates (py3.11)`, `offline gates (py3.13)`, `key derivation in real Spark` |
| branch up to date | required |
| approvals | 1, **and it must be a code owner** |
| stale reviews | dismissed on new commits |
| re-approval after last push | required |
| force pushes / deletions | blocked |
| conversation resolution | required |
| admins bypass | **yes** |

**The admin exemption is load-bearing and it is wide.** GitHub forbids approving your own
pull request, and `.github/CODEOWNERS` names a single owner — so without the exemption every
PR raised by that owner would be permanently unmergeable. The cost is that **nine accounts
hold admin and can all bypass every row above**, including three whose logins are obfuscated
and `michael-auty`, who is the Platform contact on the PLT queue. Protection is a real
guarantee against the 23 read-only collaborators and `nagashekhar-akuleti`; for the other
eight admins it is advisory. **The moment a second trusted reviewer exists, add them to
CODEOWNERS and set `enforce_admins: true`** — that instruction is in the CODEOWNERS file
itself so it is not lost here.

**Access as it stands:** 9 admin, 1 write (`nagashekhar-akuleti`, granted 23 Sep so they can
raise PRs without being able to merge), 23 read. **Read-only collaborators cannot open a PR
at all** — opening one needs push to create the branch — so "only selected users submit PRs"
is enforced by the collaborator list, not by branch protection.

**One thing that is now a load-bearing string.** The three required checks are matched **by
name**. Renaming a job in `.github/workflows/verify.yml` does not fail loudly: the required
check simply never reports, and the pull request waits for ever on something that no longer
exists. Treat those names as an interface.

### DECIDED 24 Sep — Workday reference data lands in Bronze, and Workday writes it

**Adrian: both Hubspot and Workday write to Databricks.** Workday's reference values —
the `Get_References` labels — are to be **stored in Bronze as reference data coming from
Workday**.

This names the destination that three plans have been gated on. Task 3 of
`2026-09-07-workday-reference-source.md` reads *"Blocked on: a destination nobody owns.
Do not start."* It now has one.

**The convention already exists and is completely uniform.** Every one of the 15 file
sources in `01_usnc_bronze_dev` is a `<source>_raw` schema with a `<source>_raw_files`
volume — `hubspot_raw.hubspot_raw_files`, `sap_fieldglass_raw.sap_fieldglass_raw_files`,
and thirteen more. **There is no Workday schema.** So the ask is a schema and a volume
shaped like the other fifteen, and Workday's integration writes into it exactly as
Hubspot's does.

**Two consequences worth stating before anyone builds.**

**(1) The pull tool is not the production path.** `tools/land_workday_references.py` was
built for a model where *we* retrieve and *we* land. The decision is that Workday writes.
That does not make the tool waste — it is how every Workday fact in this repo was
established, and it stays the right instrument for probing a tenant — but it is a
development tool, and the plan that described it as the landing route needs to say so.

**(2) Our planned table names do not match the convention.** The plan names
`wd_reference_id`, `wd_organization`, `wd_organization_membership`. No other source
prefixes its tables with an abbreviation of its own schema: it is `hubspot_raw.deals`, not
`hubspot_raw.hs_deals`. So these should almost certainly be `workday_raw.reference_id`,
`workday_raw.organization`, `workday_raw.organization_membership` — and **six entity
bindings name the old spelling today**, so it is cheaper to settle now than after they are
repointed.

**And it removes a cost I had flagged.** The client contract design worried that
replicating a contract id into Workday would make this accelerator *write* to Workday for
the first time, needing PLT-7 and an owner. If the traffic is Workday → Databricks, that
concern does not apply to the reference data at all. Whether it applies to the **contract
id** specifically is still open — see question 6 there — because writing an id back to
Workday is a different direction from Workday publishing its own labels.

**Outstanding, and small:** who creates `workday_raw` and its volume (Bronze, presumably,
as for the other fifteen), which reference types are in scope for the first delivery, and
the cadence. *Holder: Bronze, with the Workday integration.*
### ANSWERED 25 Sep — Amy on misc fees, contracts, and a blocker we had not seen

**Replied 25 September.** Confirmed her Workday reasoning back to her (a different tenant means re-measuring everything, so the question to Paul is exactly as she framed it), gave her the consolidated-invoice measurement as evidence for her analysis, and flagged DUNS as a candidate key for `hub_legal_entity` worth settling alongside the posting decision, since both land on the same records.

Three answers and one finding that is bigger than the question that produced it.

**Misc fees: settled, and the behaviour was already right.** *"Confirmed that the MSP
application on a Misc Fee is done at ENTRY LEVEL and therefore we need to pass the data
through as it comes in from FG, no rule required at client level."* Entry level means two
miscellaneous lines on one invoice can differ, so neither a global rule nor the per-client
reference-data rule that was the other branch can express it. MI stays out of
`_MSP_FORBIDDEN_ON` permanently rather than provisionally.

**Contracts: a client CAN hold more than one.** That was the question the client-contract
design said would decide its sufficiency, and the answer is the one that changes it —
client plus invoice date does **not** identify a contract. *"It is likely there will be a
differentiator in the data which we will need to map."* The differentiator is not yet
known. **There is time**: contract ids arrive for net-new clients from the Workday
implementation. Amy has sent scenarios worked through with Fred.

**Buyer code is the tenant, not the legal entity — and she has named it as a blocker.**
*"We do not have a consistent or reliable way of mapping transactional data to a legal
entity — this cannot be done using the buyer code as this is a 1:1 with the tenant's name
not legal entity level (in the UK it is set-up at legal entity level!)"* That explains a
measurement we already had and could not account for: 12 buyer codes, one per client, no
sub-entity structure anywhere in the feed. Bayer has **2 GP customer records against ~15
legal entities**.

**And the genuine blocker she raises is real, which we could test.** Her concern is that
consolidated invoicing files span multiple client legal entities, so the same consolidated
invoice id could post against several Workday client ids — which an AR ledger must refuse.

### MEASURED 25 Sep — Ameren is the exception, and the model was built on it

Testing her blocker against the feed turned up something we had mis-explained.
**Ten of the twelve buyer codes populate `Consolidated_Invoice_ID` and
`Underlying_Invoice_Id` on every row and `Invoice_ID` on none. AEE1 does the exact
opposite. GCAP has neither. No row anywhere carries both.**

```
AEE1     1,207 rows   Invoice_ID on all    Consolidated on none
MXDG        77 rows   Invoice_ID on none   Consolidated on all   (7 underlying)
H0BH        55 rows   Invoice_ID on none   Consolidated on all   (22 underlying)
... eight more the same, up to 22 underlying invoices per consolidated invoice
```

**Two consequences, and the second is the one that matters.**

`metadata/source_unions.yml` said the other clients "carry no invoice-shaped rows" because
each populates a different subset of the 169 columns. **The absence was right and the
reason was wrong.** They carry invoices in a *different shape*. Corrected in place.

**`hub_invoice` keys on `(tenant, invoice_reference)` over `Invoice_ID`, and ten of twelve
clients have no value to hash for it.** The Ameren invoicing model is built on the
exception, not the example. Onboarding client thirteen is a modelling question — a second
invoice identity, or a business key that spans both shapes — not a filter change. That
directly contradicts the design's own success criterion, *"onboarding a 55th client
requires no change to this model"*, and it is better to know now than at client two.

**This is evidence for Amy's analysis, not an answer to it.** The feed shows consolidation
is the norm; it does not show how many consolidated files span multiple legal entities,
because no legal entity appears in the feed at all. That is exactly the gap she is
analysing.

**Also worth carrying: DUNS.** Her future state associates each parent and child record
with a DUNS id. That is a third candidate for `hub_legal_entity`'s business key, alongside
the HFIG-assigned code it uses today and the (jurisdiction, company number) pair LEG-1
asks about — and unlike both, it is externally issued and already in her design.

*Holder: Amy for the consolidated-vs-legal-entity posting decision, which she says drives
everything else; us for what a second invoice shape means to the model.*

### SENT 25 Sep — PLT-1's remainder, as the reply Michael invited

**The Microsoft tenant moved from `headfirst.group` to `vertage.com`**, and three silver
schemas still name the old principal as owner. That is the whole of PLT-1, and it is no
longer a general statement about "partial ownership transfer".

Found by deploying to `usnc_tds` and running the job. The deploy **succeeded**; the job
failed at its third task with:

```
[UNAUTHORIZED_ACCESS] PERMISSION_DENIED:
User does not have CREATE TABLE on Schema '02_usnc_silver_edm_dev.control'
```

| object | owner | |
|---|---|---|
| catalog `02_usnc_silver_edm_dev` | `7732b208-…` *Data Platform US Terraform TDS* | OK |
| `raw_vault` | `7732b208-…` | OK |
| `business_vault` | `adrian.turcu@headfirst.group` | **stale** |
| `control` | `adrian.turcu@headfirst.group` | **stale** |
| `governance` | `adrian.turcu@headfirst.group` | **stale** |

**One schema of four was transferred, and it is the one Terraform created.** The other
three were created by hand and kept their creator — an identity that no longer exists in
this tenant. `raw_vault` already holds 17.9M rows, which is why every earlier load worked
and this one did not: the job had never needed to create anything in `control` before.

**It would have failed twice more**, at `apply_governance` and `business_vault`, for the
same reason.

**The fix, three commands:**

```bash
databricks schemas update 02_usnc_silver_edm_dev.control \
  --json '{"owner":"adrian.turcu@vertage.com"}' --profile hfig-usnc-tds
databricks schemas update 02_usnc_silver_edm_dev.governance \
  --json '{"owner":"adrian.turcu@vertage.com"}' --profile hfig-usnc-tds
databricks schemas update 02_usnc_silver_edm_dev.business_vault \
  --json '{"owner":"adrian.turcu@vertage.com"}' --profile hfig-usnc-tds
```

They may need a metastore admin: the current identity is not the owner of the three, and
UC requires ownership or admin to reassign. If they are refused, **that** is the Platform
ask — and it is now one line rather than a paragraph.

**A second gap found on the way, and it is separate.** `var.service_principal` is
declared *"Service principal that production jobs run as"* with a default of `""`, and
**`usnc_tds` does not override it**. So `run_as` resolves to empty and the job runs as
whoever triggers it — which is why this failure wore a human's name. Production jobs
running as a person is not the intended arrangement; worth settling alongside the
transfer rather than after it. *Holder: Platform, with us.*

**Sent to Michael, 25 September**, as a reply to his tenant-move note rather than as an
escalation — he wrote *"if you need to edit or delete one and can't, let us know and we
will move the ownership across (this has been done for some objects but let me know if any
are missing)"*, and ours are three of the missing. The ask carries two acceptable answers:
`adrian.turcu@vertage.com`, or the service principal that already owns the catalog and
`raw_vault`, which is tidier and lets `run_as` move in the same change.

**And one correction offered with it.** His note says production pipelines are unaffected
because they run under service accounts rather than people. True of production, **not true
of `vault_load` on `usnc_tds`** — `run_as` is unset there, so it runs as whoever starts it,
which is why this failure wore a person's name. That is the half of PLT-1 open since 6
September, raised as a correction to his own assumption rather than as a chase.

**Nothing in the model or the code is implicated.** The bundle validates, all 21 active
bindings resolve to real tables, the deploy uploaded cleanly, and both offline suites are
green at 1212. This is a permission, not a defect.

### MEASURED 24 Sep — what is actually in Bronze, and who owns none of it

Full inventory in `docs/bronze_source_inventory.md`. Prompted by three confident claims
written into specs this month that were all wrong and all corrected by a one-minute query.

**The numbers that change how this estate reads:**

* **"48 schemas" overstates it.** 27 hold tables; **20 are empty** — ten source families
  provisioned as `<name>`/`<name>_raw` with a volume and no data. They are landing zones
  awaiting a feed, not sources to plan against.
* **The vault reads 4 of 27 sources**, and **22 of its 46 bindings name catalogs that do
  not exist** (`hfig_eu`, `hfig_usnc`, `PLACEHOLDER`). That is deliberate and gated — but
  "46 bindings" reads like a connected vault and the real number is 22.
* **39.3M rows unread with no decision against them** — two whole VMS platforms
  (`beeline`, `vndly`), a CRM (`microsoft_dynamics`) and `onestaff_analytics_raw`.
  Excluded as ruled out rather than overlooked: `fieldglass_raw` (14.8M) and the
  client-owned sources (1.7M).
* **Only one source is current.** `sap_fieldglass_raw` has September data; the other ten
  stopped between February and August.

**Three traps worth carrying:**

1. **THE RULE, stated by Adrian 24 September: we use `sap_fieldglass_raw` and we do NOT
   use `fieldglass_raw`.** They differ by a three-letter prefix and are not the same data
   in two shapes — 1 table and 29,426 rows against 302 tables and 14.8M. All the Ameren
   invoicing work reads the first. So `fieldglass_raw` is **out of scope by decision, not
   unexploited**, and its rows should not be counted as opportunity. A spec that says only
   "Fieldglass" is ambiguous between a source we depend on and one we have ruled out:
   **name the schema, never the system.** **The client-owned sources are out too**
   (Adrian, 25 Sep) — that is four more schemas, `fieldglass_client_owned*` and
   `beeline_client_owned*`. So of three schemas beginning `fieldglass`, exactly one is
   ours.
2. **A binding scan misses the Ameren dependency**, because it reads
   `sap_fieldglass_raw` through a silver union view rather than by naming it.
3. **`information_schema.columns` undercounts tables** — it omits SDP internals. Use
   `.tables`. Several bronze schemas are pipeline output carrying `__materialization_*`
   twins, which **hold no column masks**, so a grant on one is broader than it looks.

**The gap this cannot fill, and it is the point: nothing records who owns a source.**
Ten sources the vault does not read have no owner, no statement of whether they are meant
to be current, and no one to ask what is in them. Hubspot is the proof that matters:
208,532 rows, the smallest real source in the lake, holding the client hierarchy a spec
had asserted did not exist anywhere. **Size is not the signal.**

*Suggested ask, and it is cheap: circulate the table and request only an owner per row.*

### ANSWERED 24 Sep — MSP on expenses rejects; miscellaneous is still open

**Amy confirmed** that MSP fees are not applied to expenses **on any client**, and that a
record carrying one **should reject**. Implemented as `invoice_rules.msp_violation`,
mutation-proven four ways.

**This reversed our call, and the reversal is the system working.** The distribution was
measured — 1,057 of 1,057 timesheet lines carry margin, 0 of 110 expense and 0 of 5
miscellaneous lines do — and deliberately not encoded, because she had said the variety
was real and a rule from one client's extract would reject a correct invoice elsewhere.
Measuring could not settle it; asking did. The measurement is what made the question
precise enough to answer: *"no expense line in 110 carries a fee, is that the rule?"* is
answerable, *"how do MSP fees work?"* is not.

**Miscellaneous is open with two branches**, and they need different implementations:

* *some with, some without* → pass the data through, no rule
* *all or nothing per client* → **a rule in reference data keyed by client**, which is a
  different shape from the across-all-clients rule expenses got

Amy is confirming which. Until then MI passes through, because that cannot reject an
invoice that is correct under either answer while rejecting can. A test pins the gap and
quotes her open question, so it fails the day someone closes it silently.
*Holder: Amy Keser.*

**Also from the same message:** Paul is establishing which Workday tenant we can use, and
Amy is calling him to resolve it and to provide the reference IDs for the client and
supplier lines. That is the master-data blocker — Ameren is not a Workday customer and
none of the 29 suppliers exists there — **moving, not yet moved**.

### OWNER 24 Sep — Hubspot is the CRM team's, not Data & AI's

**Eva and the CRM team manage Hubspot.** Recorded because two open questions were briefly
addressed to the wrong person, and because nothing in this repo says who owns a source.

Hubspot matters to us more than it did yesterday. `01_usnc_bronze_dev.hubspot_raw` holds
**2,457 distinct companies** and **15,430 deals**, and it turned out to answer two things
this project had written off:

* **`companies.hs_parent_company_id` is a client parent/child hierarchy** — the source the
  legal-entity spec asserted did not exist. `Aerotek` and `TEKsystems` both roll up to
  `Allegis Group`; `Aetna` to `CVS Health`. That is spend-reporting-by-client-group, which
  is what Amy asked the client hierarchy for.
* **`deals` carries the commercial half of a client contract** — `total_contract_value`,
  `contract_start_date`, `end_date_contract`, `dealstage`, `deal_company_id` — which is one
  end of the deal → quote → contract chain Fred's new process describes.

**Two questions therefore belong with the CRM team, not with Data & AI:**

1. **How does a Fieldglass `Buyer_Code` map to a Hubspot company?** This is the single thing
   blocking client-group spend reporting. Spend attaches at `Buyer_Code`; Hubspot keys on
   `hs_object_id`; and of the **12** buyer codes in the invoice feed **only `AEE1` carries a
   buyer name at all**. `TEKS` resembles TEKsystems and that resemblance is recorded as a
   hypothesis, not a mapping — inferring identity from a name is what produced the `buyer`
   defect this whole legal-entity design exists to handle.
2. **Is one Hubspot deal one client contract?** A deal carries contract value and contract
   dates, which suggests yes — but 15,430 deals against 2,457 companies is six per company,
   and nothing says how many became contracts. It decides whether
   `sat_client_contract_details_hubspot` is bound at the deal grain or something else.

**And one thing answered before it was asked:** `hubspot` and `hubspot_raw` are two schemas
with different row counts (deals 1,640 against 15,430; companies 2,445 against 7,328).
**Adrian confirmed 24 September that `_raw` is the right one** — which is also the
convention every other source in this lake follows, and the one the Fieldglass invoice work
already binds (`sap_fieldglass_raw`). So this is a platform convention rather than a CRM
question, and it does not go to Eva. Rows are snapshots — `hs_object_id` repeats — so any
load takes the latest per id by `timestamp`.

*Holder: Eva / CRM team. Raised by Data & AI, 24 September.*

### SENT 24 Sep — Workday master data is empty, and it blocks posting

**Sent to Paul McMahon.** Retrieving the Workday reference data the Ameren template marks
Open turned up a dependency no document had recorded:

```
Company_Reference_ID   82   REAL   NL104 HeadFirst BV, BE110 Source Automation Belgium BV
Ledger_Account_ID     135   REAL   101000:Tangible fixed assets - Cost
Customer_ID            29   OTHER CLIENTS -- Biogen, Johnson & Johnson, Lonza. NO AMEREN.
Supplier_ID            14   DEMO DATA -- ACME Lawn Care, DNU_UK_Supplier. NONE of the 29.
```

**Ameren is not a Workday customer and not one of its 29 suppliers exists there.** Companies
and the chart of accounts are configured properly, so it is specifically customer and
supplier master that is empty. **It blocks posting, not building** — everything needed from
Fieldglass and from Amy's master data is in hand.

**We are not creating them by web service**, though `Put_Supplier` (Resource_Management) and
`Submit_Customer` (Revenue_Management) both exist and were verified against the live tenant
WSDL. The reasons are governance rather than capability, and they are recorded in
`docs/superpowers/specs/2026-09-24-ameren-template-crosscheck.md`: LEG-7 puts master-data
creation with the CBO Office and implementation with CPTO; it is thirty records once; and it
would mean granting our integration user write access to supplier master in a financial
system — which eventually carries remit-to and bank details — for a one-off load.

**The service is used for the half that repeats.** Once the records exist, `Get_References`
on `Supplier_ID` and `Customer_ID` fills Amy's blank Workday-id columns by name match and
re-runs whenever they change.

**Two things asked of Paul:** who creates them, and whether master data configured in the
IMPLEMENTATION tenant migrates to production or must be created again. The second decides
whether this is done once or twice, and it is the same question still open on
<span>WDJ-5</span>.

### PARKED 23 Sep — two things waiting on Platform, deliberately not chased

Both measured, both recorded in `docs/platform_team_requests.html`, both **parked rather
than forgotten** — the decision was to move on to the repository migration first.

1. **PLT-1 is a PARTIAL ownership transfer, and that is the riskier half-state.**
   `raw_vault`'s schema moved to `7732b208-8366-4aef-af09-60e9dec9cf86`; `business_vault`
   and `control` did not, and **67 of `raw_vault`'s 73 tables are still owned by a person**.
   Setting `run_as` to the principal now gives the pipeline an identity owning its schema
   and 6 of its 73 tables, so the first load fails on the other 67 and looks like our
   change broke it. **Do not set `run_as` and do not run a load until the transfer is
   finished.** Nothing ran in the fifteen-day gap — last execution 4 September, four days
   before the transfer — so the written commitment held, but only because nothing here runs
   on a timer.

2. **PLT-7's scope exists and we cannot use it.** `kv-usnc-automation`, backend
   `AZURE_KEYVAULT` — real progress from "no scopes of any kind" on 6 September. But we hold
   **no permission on it**, so we cannot see whether the Workday credential is in it, under
   what key, or grant the run-as identity access. **`.workday-credentials` therefore stays**,
   with a narrower reason than before: not "no scope exists" but "one exists and we have no
   READ on it".

**What is NOT blocked by either:** PLT-2 is done and true, PLT-4's grant is live and our
allowlist now names `scope_tds_edm_vault_read`, PLT-5's access is granted, PLT-6 is done.

### What happened, in one line

Platform answered three asks. **One answer was false, one was better than our ask, one was
already true.** We measured all three before replying, replied, and then chased the one
input that three things now wait on.

### The four things worth carrying forward

* **Measuring a claim costs three queries; believing one costs a silent failure.** Platform
  said the job-runner group "already" contained the data engineers. It does not contain the
  identity that runs the loads — `is_account_group_member` returns **false** for
  `adrian.turcu@headfirst.group`, which `jobs get` confirms is `vault_load`'s `run_as`. Had
  we accepted it, the next load would have written 8.5M NULLs through the masks and looked
  like it worked. **PLT-2 is now a correction rather than a request**, which is a much easier
  thing to chase.
* **Their answer to PLT-3 was better than our ask, and taking it was worth more than being
  right.** We asked for a restricted deployment folder; they said deploy from the repository
  into the service principal's own workspace. That removes the laptop from the deployment
  path entirely. Accepting it turned **PLT-1 into a blocker on Platform's own preferred
  route** — a stronger lever for the one input we need than anything in the original letter.
* **Re-checking a closed item earned its keep.** PLT-C1 closed on 2 September, so
  re-measuring looked redundant. It was not: those grants arrived by **propagation** from the
  `full_scopes` roll-up groups, so a hand revoke would have lasted until the next apply.
  Five days clean is evidence the carve-out is holding — the revoke alone never proved that.
* **We were asking Platform for production ceremony in a test workspace.** PLT-1 asked for
  "a name, and a time". In TDS there is no window to coordinate: nothing runs on a timer,
  every load is by hand. The card now asks for the name only — **and states that production
  still needs the window**, because dropping the word without keeping the reason is how a
  scoped concession becomes a standard we no longer hold.

### Two cautions that did not change

* **The reply we received answered a retired document.** Someone replied to an older
  three-request note, not the consolidated queue Michael has had since 6 September. Three of
  seven answered; **PLT-1, 4, 5, 6 and 7 have had no response.** Do not read the reply as
  progress on the queue.
* **`vault_load`'s no-schedule reading has a shelf life.** True on 7 September. Re-measure
  immediately before the transfer is *executed*, not before it is agreed.
## State of play

`main` is clean, pushed and green — `git log` is the authority on where it is. CI runs on
every push and pull request (`.github/workflows/verify.yml`): the offline gates on Python
3.11 and 3.13, plus a third job that runs the loader's key derivation in real Spark.

| suite | run it with | needs |
|---|---|---|
| `tests/test_accelerator.py` | `.venv/bin/python tests/test_accelerator.py` | nothing |
| `verify_repo.py` | `.venv/bin/python verify_repo.py` | nothing |
| `tests/test_spark_derivation.py` | `JAVA_HOME=~/.local/share/jdk/current .venv/bin/python tests/test_spark_derivation.py` | pyspark + a JVM |

**No count is written down here, deliberately.** Each script prints its own, and a
hand-maintained number drifts — `verify_repo`'s was corrected on 5 September and was stale
again the same evening. `verify_repo` refuses a hardcoded count in this file, the skill,
`README.md`, `CHANGELOG.md` and `DEPLOY.md`.

**Deployed and loaded** in `02_usnc_silver_edm_dev` (workspace `usnc_tds`, profile
`hfig-usnc-tds` — never auto-select a profile, every other one is EU production):

| | |
|---|---|
| Hubs | narrowed and deduplicated — `hub_accounting_journal` 92 → **11 columns** |
| Links / NHLs | `nhl_general_journal_line` 83 → **24 columns**, 2,453,132 rows |
| **`hub_job_request`** | **52,747 rows / 52,747 keys** — BULLHORN_EU 45,516 + **FIELDGLASS_US 7,230** + ghost. `key_style: tenant_scoped` since 4 Sep |
| First satellite | `sat_job_request_details_bullhorn_eu` — 46,889 versions over 45,516 keys |
| Type-2 view | `_v1` end-dates on `applied_dts`; **0 zero-length intervals** |
| Governance | applied — 54 statements; masks name real groups and are enforced |
| **Legal-entity family** | **declared 5 Sep, loads NOTHING.** `hub_legal_entity`, `hal_legal_entity_hierarchy`, `esat_legal_entity_hierarchy` — all three on **placeholder** `bronze_table` values and absent from `active_sources`, waiting on the roster |

**FIELDGLASS IS LIVE, and it reconciles to the row.** The union view
`raw_vault.v_fieldglass_us_job_posting` spans **30 live per-tenant tables**, rebuilt from
`information_schema` on every run by `checks/apply_source_unions.py` so a newly onboarded
client cannot silently miss. Of its 328,172 rows: **270,286 carry a valid key and were
staged**, **57,886 have a null `buyer` or `job_posting_id` and were quarantined**, and the
270,286 hold exactly **7,230 distinct `(buyer, job_posting_id)` pairs** — the hub's key
count. 270,286 + 57,886 = 328,172. Nothing lost, nothing invented.

**Gates green**, measured on run `386964197107813` (4 Sep) and the idempotent re-run after
it: `hash_parity`; **`key_derivation` — the new third leg, MATCHED**; `append_only` — 48
tables; `landing_integrity`; `mask_survival`; `audit_completeness`; `source_conformance`;
`aggregate_reconciliation`; `supersede_quarantine`; `reconcile_loop1` (NOT_EVALUATED,
deliberately). Both vault pipelines validate.

**ONE gate is RED, unchanged and not ours:**

| gate | blocked on | who |
|---|---|---|
| `assert_journal_integrity` | run-as identity not privileged under `mask_money`, so 8.5M debit/credit values read NULL and the gate refuses to assert on zeros | admin team — group membership |

`apply_governance`, `assert_no_broad_grant` and `publish_model_metadata` stay SKIPPED behind
it, correctly: all three WRITE, and DEF-44 gates writes on correctness.

**The model is now readable as pictures, not just as YAML.** Seven generated diagrams —
one per domain plus the load-job task graph — render inline at
[`diagram/`](../../diagram/README.md) on GitHub. They are emitted from `metadata/entities/`
and `resources/vault_job.yml`, byte-gated, and checked against Archify's own renderer.

**Scope orphans: 0.** `metadata/key_scope_exceptions.json` is empty and **kept** — the gate
reads it every run, so a sixth fails the build. Deleting the file would delete the gate.

## What is actually outstanding

**Nothing is blocked on code**, and that has been true all week. Both decisions that were
ours on 4 Sep were taken on 5 Sep — the Fieldglass clock (→ BRZ-15) and how to model
`buyer` (→ the legal-entity family). What is left is other people's answers and one roster.

| | owner | note |
|---|---|---|
| **Fieldglass satellite: which clock?** | **DECIDED 5 Sep → now BRZ-15, Bronze team** | The hub is live; `sat_job_request_details/FIELDGLASS_US` is NOT, and its `bronze_table` is deliberately a placeholder. The payload rename is **already written down in the file** (posting_title → job_posting_title, and six more). The blocker is `applied_dts`, the BUSINESS clock: Fieldglass ships **no modification timestamp**. `job_posting_create_date` is constant per posting, so every version would share an `applied_dts` and `_v1` would produce zero-length intervals. `timestamp` moves per delivery but is a DELIVERY clock, which this repo keeps deliberately distinct (see `dedup_order`). **Decided by measurement, not preference:** all four candidates were measured over 270,286 valid-key rows and every one is disqualified — `job_posting_create_date` and `uploaded` are constant per posting, `job_posting_distribution_date` is 42.4% null, and `timestamp` is FROZEN across 562 of the 1,127 postings that actually change. Declaring nothing is worse still: `applied_dts` becomes NULL and `_v1` reports every version as current. So it is **BRZ-15** — Bronze already emit `revision` on 62 `io_worker` tables. Fallback if they say no: bind `timestamp` and suppress the `_v1` view for this satellite alone |
| **17% of Fieldglass rows have a null `buyer`** | **Bronze team — question** | 57,380 of 328,172, quarantined rather than loaded. `key_safety_rules` refusing to hash a null token into the tenant position is the rule WORKING — but a fifth of a feed arriving without its tenant is a data-quality question. Belongs with BRZ-13's four |
| **BRZ-14 — backfill type inference** | **Bronze team** | Losing data now, needs nothing from us. Every `DOUBLE`-typed column in the Fieldglass estate is in a `_backfill` table (153 instances; zero in a live table). 556 of 12,491 ZIPs in `cencora_backfill` are under five digits because a `DOUBLE` cannot hold `00965`. **Until fixed, the union excludes backfill, so Fieldglass HISTORY is not in the vault** — and adding it later is a drop-and-reload of the Fieldglass satellite, not an append (change detection orders by `load_dts`, so late-arriving older versions would create spurious ones) |
| **BRZ-13 — Fieldglass scope questions** | **Bronze team** | Four answers, no build: `_backfill` precedence (same as BRZ-4), whether the client-owned family is in scope, whether custom fields exist anywhere, and whether `io_distributed_jobposting` is the right family (we chose it on its 39 distribution/bill-rate columns; one line to change) |
| **THE STABLE VIEWS ARE UNREADABLE, AND THIS REPO CANNOT GRANT THEM — a platform ask, found by Adrian in the catalog explorer 26 Sep** | **platform team — the access repo** | Every vault object now has an unversioned VIEW that the data contract, the DBML diagram and the source-to-target mapping tell consumers to bind to. **Nobody can read any of them.** `databricks grants get` on `hub_accounting_journal` returns *"User does not have SELECT"* — the object exists and carries no privilege. **We cannot fix it here.** `apply_governance` emits per-table grants only under `--emit-data-grants`, and the job deliberately does not pass it: *"On this platform data access is granted per table through the access repo, not by this job (platform team, 2 Sep 2026). The vault stays reachable to the pipeline by OWNERSHIP."* It ran 9 statements tonight — mask functions and schema REVOKEs — and zero GRANTs. **Why the physical tables are still readable:** Unity Catalog grants follow a renamed securable, so `ALTER TABLE ... RENAME` carried every existing grant onto the `_rev<N>` names for free. That is why `hub_accounting_journal_rev1` has `scope_tds_edm_vault_read -> SELECT` although nothing granted it tonight. **The ask:** add the 17 unversioned view names to the access repo alongside the `_rev<N>` tables they front. Until then the versioning mechanism is real but unreachable — consumers can only read `_rev<N>` names, which is precisely what the view exists to stop them binding to, and a cutover would move a view nobody is reading. **Our side is done and inert until the flag is used:** `table_select_grants` now includes the stable view under the same activity condition as the quarantine twin, with three checks and both mutation directions proven. |
| **DEF-59 — I DIAGNOSED THIS WRONG TWICE, AND THE FACTORY WAS NEVER AT FAULT. Corrected and FIXED 26 Sep** | **ours — a gate defect, one line** | **What is true:** `assert_mask_survival` failed on the first post-migration load with 7 problems across `nhl_payroll_detail_rev1` and `nhl_timesheet_line_rev1` — *"declares mask governance.mask_money in metadata but no mask is present on the table"*. **What I claimed, twice, and got wrong:** first that the factory creates these tables WITH their money columns and WITHOUT masks, because `_ghost_columns_sql` *"supplies EVERY declared column"*; then, when choosing how to descope payroll, that extending the gate's inactive-skip would be *"making the gate green with a reason known to be wrong"*. **Both were wrong, and the second was stated confidently.** I read `_ghost_columns_sql`'s docstring without checking its CALL SITE: `factory._emit_ghost` builds from the declared field list **only when `schema_fields is not None`**, and that requires an ACTIVE binding, because deriving a schema READS the source. With none it emits the hash key and the system columns alone. **The decisive evidence was already in hand** — `assert_append_only`'s own column dump from the same run: `nhl_payroll_detail_rev1` holds `applied_dts, batch_id, load_dts, manifest_id, payroll_detail_hk, rec_src`. **No `gross_amount`, no `amount`, no `rate`.** The masked columns DO NOT EXIST, so there is no unmasked money anywhere and never was. **The real defect:** the gate's inactive-skip was `is_staged and not active`, and an nhl/link/hal is not staged — so an INACTIVE one fell past the guard and was reported as missing a mask for a column that does not exist. **The fix is one line:** the skip is about ACTIVITY, not about kind. The DEF-59 waiver built on the wrong diagnosis is REMOVED — a waiver against a defect that does not exist is a hole with no reason to be there. Mutation-proven; restoring the `is_staged` half reddens two checks. |
| **DEF-58 (renumbered 26 Sep — I first filed this as DEF-54 and then DEF-55, and BOTH WERE ALREADY TAKEN: DEF-54 is cited in verify_repo, DEF-55 is the sub_seq ordering defect closed in DECISION_LOG. A duplicate id is a citation that leads somewhere else, which is worse than one that leads nowhere). NARROWED THE SAME HOUR. The factory CAN compute a csat payload; the expressions were never written. One of eleven needs more than an expression** | **ours — ten expressions and one join, not a missing mechanism** | **What is true:** switching on `csat_invoice_line_gie` fails, because it declares eleven payload columns and `nhl_invoice_line` provides none of them — `_projection()` names them and `_project()` selects them, so the select is unresolved. **What I got wrong:** I called it *"a mechanism the accelerator does not have"*. **It has one.** `_stage_full` applies `src.derived_columns` as `df.withColumn(name, F.expr(expr))`, **unconditionally, for any binding of any entity kind**, right after the read. A csat binding declaring per-column SQL expressions would be computed today, with no factory change at all. The conformance profiles already use it; `invoice_line/FIELDGLASS_US` itself carries fourteen `derived_columns`, one of them a ten-column SHA2. **So the gap is the expressions, not the wiring.** Against `invoice_rules.py`, ten of the eleven look like single-frame SQL: `description`/`line_description` are `CONCAT_WS`, `line_type` is a `CASE` on the source type, `task_number`/`project_number` are a strip off the task code, `expenditure_type`/`expenditure_organization` are `module_value`'s pick-the-populated-module `CASE`, `amount` is the client amount the parent already carries, `rule_version` is a literal. **The one that is genuinely different is `invoice_amount` (AME003)** — the invoice gross repeated on every line, which comes from `sat_invoice_header.gross_invoice_amount`. That is a **JOIN**, and `derived_columns` evaluates one expression against one frame; it cannot reach another table. So that column needs either a different mechanism or a different design, and it is the only one that does. **Not attempted, and nothing is claimed about correctness** — the rules are Python and their SQL equivalents are unwritten and untested; hashdiff stability and the two `mask_money` columns (`amount`, `invoice_amount`) both bear on how they are written. The binding stays in `_COMPUTED_HELD_BACK` until they exist. |
| **ANSWERS IN, 26 Sep — David (Workday/finance) and Amy (governance). One closes a question, one removes work from our plate, one is a decision we argued against and should now stop arguing about** | **theirs — answered** | **WDJ-10 IS CLOSED, by Amy, with high confidence** — *"cost centres should have a single global meaning, with regional and legal entity reporting handled through separate dimensions"*, and she designed it alongside Alex in group finance. **That is the half the tenant could not answer.** The structural half already matched (64 live cost centres, one list, no regional marker, geography a separate dimension). So `C6130 Engineering` is ONE cost centre, **keying `hub_cost_centre` on the code alone is safe**, and the silent-defect risk — two regional cost centres collapsing into one hub row with nothing failing — is retired. The general rule recorded in the source design is now satisfied for this dimension rather than merely stated. **WDJ-8's third ask is answered and it takes work OFF us:** David — *"No this is ongoing. They are working on the mapping for all chart of accounts which should cover both UKG and GP."* So the 855→135 crosswalk is **being done by them, for UKG and GP together, with no completion date**. We should not start it; the ask becomes a date, not a design. **WDJ-1 is answered and it partly resolves WDJ-2** — David reviewed with Dyneeshia and **updated the workbook**: `*_reference_id` always aligns with the Databricks hashkey, and `AccountingJournalID` *"does need to be based on the company, batch, date, and maybe other details to create a unique ID"*. **That is the shape we argued for**, from the other direction: the hash is now the REFERENCE id (ours, for reconciliation) and the permanent journal ID is a composite, rather than a hash being asked to serve as a permanent external identifier. **One caveat that does not go away:** our hashkey is recomputable by design — it depends on `RULEBOOK_VERSION` and on the business keys, and **UKG's `fiscal_year` is still the literal `NOT_APPLICABLE`**, a placeholder awaiting a value. Whatever it is called, a value that changes when a placeholder is filled cannot be a stable reference, so the placeholder is now on WDJ-2's critical path. **David flags Justis was updating these fields and may need to confirm** — so this is a strong signal, not a sign-off. |
| **Amy on legal entities, 26 Sep — governance first, vault second** | **hers — a governance position, and it overrides our modelling defaults** | Amy's central point: *"several of the legal entity decisions should be considered through the wider Global Data Catalogue, Trading Entity and Master Data governance model, rather than being driven by the current vault design."* Taken as given rather than argued with — the vault records what the business decides, it does not decide it. Four points, each of which touches something we have already built or assumed: **(1)** legal entities are MASTER DATA governed centrally, not a vault-owned dimension. **(2)** legal ownership, trading-entity relationships and contractual relationships are **three separate models**, not one hierarchy — which is a direct check on `hal_consolidation_hierarchy` and `lnk_client_contracting_entity` being treated as variants of one thing. **(3) brand unification is NOT legal entity unification**, and **a legal entity stays active and reportable until formally dissolved** — so a rebranded entity must not be retired in our model on the strength of the brand change. **(4)** the current GP customer structure **must not be assumed as the future state** and is subject to Workday migration planning — which bears on WDJ-9's company crosswalk, where we have 11 confident-ish pairings against GP codes. **PARKED FOR A DOCUMENT:** Amy says *Global Data Catalogue*; Adrian's correction is that it is actually a **Reference Data Management application**, and he has a document to supply. Nothing here is remodelled until that lands — naming it wrongly in the metadata would be worse than leaving it open. **Amy has time Monday for clarifications.** |
| **Workday journal export** | **SENT 7 Sep — now 11 asks, WDJ-1..WDJ-11; WDJ-10 and WDJ-11 added after** | **WDJ-11, 26 Sep — a granted-security discrepancy, and it is the one that decides whether a whole design has a source.** Augustin's note says *"so far the security granted would be to use Get_References"*. Tested rather than filed: **`Get_Organizations` on the `Human_Resources` service answers today** — 77 pages, ~1,528 organisations. Either the grant is broader than described, or **the implementation tenant is permissive and production will not be**. That second reading is expensive: `2026-09-24-legal-entity-hierarchies-design.md` sources `hub_legal_entity`, `hub_consolidation_group`, `hal_consolidation_hierarchy` and `lnk_legal_entity_consolidation` entirely from `--kind organisation` and `--kind membership`, and **both call `Get_Organizations`** — so on a production tenant that refuses it, that design has no source at all. **A call working in impl is not evidence that it is granted**, only that impl does not refuse it. Ask: is `Get_Organizations` granted to `ISU_Databricks` in impl AND in production? **The version half of his note is settled and needs nothing** — his example carries `v45.0`, the email is old, and `Company_Reference_ID` at `v45.0` and `v46.2` returns byte-identical rows. The tenant validates the segment (`v46.3`, `v48.0` and `v99.9` are all rejected as *Invalid request service version*), and serves **v44.0 through v47.0**. `v46.2` stays because `Integrations.xsd` was read at v46.2 and the parser and offline suite were built to it — moving the pin buys no data and costs a re-proof. Analysed 6 Sep against the `Journal` tab of `HeadFirst_YesNoGlobal_Master_Workbook.xlsx` → `docs/workday_journal_export_analysis.html`. **The good news first: no remodelling is needed.** Workday's journal is one header / many lines / many worktags per line and the vault already holds that shape — `msat_journal_line_worktag` was built from the Workday XSD before the workbook existed, with the XSD's own one-worktag-per-type rule as its `mas_key`. Of the 20 fields answered Yes: **8 read from live tables, 6 from entities bound to placeholder Bronze tables (→ BRZ-17), 3 are writer constants, 3 have no home**. **Two decide-firsts.** (1) An export written today ships a file of NULLs — `DebitAmount`/`CreditAmount` carry `mask_money` and `assert_journal_integrity` is the red gate above, so ~8.5M values read NULL; a well-formed file with every amount empty is worse than a failure because it looks right. (2) **WDJ-2 is a correction, not a sign-off** — the sheet says `AccountingJournalID = "Hashkey from Databricks"`, and **a hash cannot be a permanent external identifier**. It is *derived*: its value depends on the rulebook (versioned because it may change) and on the business keys (UKG's `fiscal_year` is the literal `NOT_APPLICABLE`, a placeholder awaiting a value). Recomputability is the vault's whole design and is exactly what an external permanent ID must not have — **there is no Databricks-side mechanism that reconciles the two**, which is why this stopped being a question for finance. Recommendation: Workday mints the ID, `JournalExternalReferenceID` carries a readable composite of the source key, and we capture Workday's ID back so reconciliation does not depend on ours staying stable either — that last part is new work and is the honest cost. **WDJ-5 is now answered on the access half:** we hold `ISU_Databricks@headfirst3` against `impl-services1.wd502.myworkday.com`, Web Services **v46.2** — recorded in `docs/workday_reference_service_access.md`, **password deliberately not in the repo** (a Databricks secret scope; the bundle root is world-writable until PLT-3 lands, so a credential in any shipped file is a credential handed to the workspace). **The tenant is an IMPLEMENTATION tenant** — fine for building, not fine as a source of reference data: WIDs are tenant-specific and identify nothing in production, and whether configured reference IDs migrate is a question for Workday, not an assumption for us. What is left of WDJ-5 is confirmation the ISU is the intended one, which types it may read, and the production host and tenant. **The client is built and has never been run against a tenant** — `src/accelerator/workday.py` (pure: build the request, parse the response) and `tools/fetch_workday_references.py` (the only thing here that opens a connection; `--dry-run` needs no credential because the request body cannot contain one). Seven offline checks, each mutation-proven; the important one is that **a SOAP fault RAISES rather than parsing as an empty result**, because Workday answers a rejected request with a well-formed envelope and a parser that reaches straight for `Response_Data` turns a bad credential into a silent load of nothing. **All 29 reference types the sheet names were probed 7 Sep**, before sending, rather than the handful I had guessed at. Three things came out of it. **(1)** `Custom_Organization_Reference_ID` holds **188 entries, none retired** — a live multi-dimensional structure (functions, lines of business, products PERM/TEMP/Secondment, payroll companies by country, geographies, legal-entity consolidation groups). GP's segments 1, 2 and 5 are 36/34/10 values, the right order of magnitude, so this is the plausible home for them — named as a candidate, not proposed as a mapping. **(2)** My earlier 'tax appears not to be configured' was imprecise: the **scaffolding exists and the data does not** — `Tax_Point_Date_Type_ID` 7, `Tax_Type_ID` 3, `Tax_Recoverability_Type_ID` 3, while every code, rate and applicability is 0. **(3)** **Two of the five balancing-worktag types the sheet offers are empty** — `Business_Unit_ID` and `Fund_ID` — worth knowing before anyone specifies a balancing worktag against them. Also non-empty and unprobed before: `Employee_ID` 2121, `Location_ID` 164, `Revenue_Category_ID` 97, `Contingent_Worker_ID` 90. **WDJ-9 — the company crosswalk, and it is nearly done.** Same check as WDJ-8, much happier answer: our vault holds **11 organisations**, Workday holds 82 (about 20 in the Americas). Exact code overlap **zero** — ours are GP database names, theirs are `US1xx` — but the names nearly solve it. Confident: `GGI`→US100 Guidant Global Inc, `CER`→US102, `CSGH`→US010, `BRPLM`→US103 (our only UKG company). Five `*SPV` codes against five `US80x … Funding LLC` entities — **the set matches, the individual pairings are a guess**. Unknown: `BARM`, `CSS`. We guess because GP keeps company names in `SY01500` and **that table is not in Bronze** (we hold 20 GP tables); confirming 11 pairings is quicker than landing a feed, so it is not a Bronze ask. **The contrast with WDJ-8 is the point:** this is a confirmation exercise, that one is a design decision. **WDJ-8 — THE LEDGER ACCOUNT IS THE WRONG GRAIN, found 7 Sep by pulling Workday's chart and comparing.** Workday has **135** ledger accounts, 6-digit. `hub_ledger_account` holds **361,706** distinct references, 1–9 digits, **13 in common**. The cause is not a formatting difference: GP keys a GL line by `actindx`, its index over the *whole account string* — five segments, `30-60-5600-606502-00`, cardinalities 36 · 34 · 710 · 855 · 11 — so our hub is a hub of account *combinations*, faithfully recording what GP sends. **Segment 4 is the natural account** (6-digit, `102000` = A/R TRADE, `201000` = A/P Trade; one value spans thousands of combinations). GP has 855 of them and **only 16 of Workday's 135 appear among them** — Workday's chart is NEW, so the mapping cannot be derived. **An export built on the model as it stands would put `105676229` into `LedgerAccountReferenceID` and have every line rejected.** The shape of the answer is that one GP account string becomes **one Workday account plus several worktags**, which is what `WorktagsReferenceID` exists for. **WDJ-10, a business question with a silent-defect behind it:** Workday is one global tenant, our vault is four regional lakes, so global reference data must mean the same thing in all four. The tenant answers the structural half — **64 live cost centres, one list, no regional marker on any** (the only name containing "Region" is `C4200 Regional Marketing`, a function), and geography is a separate dimension (`Region_Reference_ID` 99, plus the geographic custom orgs). **The unanswered half is the meaning:** is `C6130 Engineering` one cost centre four regions charge to, or four regional functions sharing a code? If global, the key is the code; **if regional and we key on the code alone, two cost centres collapse into one hub row and nothing fails — the numbers are just wrong above.** That is the `buyer` defect with a different column name. Not blocking: cost centres are out of scope for the source design, and the general rule is now recorded there — *a global dimension may be keyed on its code alone only once someone has confirmed the code means one thing group-wide.* **And two things I asserted were then tested, 7 Sep.** (a) I called `Custom_Organization_Reference_ID` the "candidate home" for GP's segments 1/2/5 on **cardinality resemblance**, which is not evidence — tested, the code overlap is **zero on all four segments**, and never could be otherwise since their codes are non-numeric. The document now says so rather than letting a resemblance read as a finding. (b) **One real lead:** strip the `C` from the 64 live cost centres and **19 are numerically identical to a GP segment-3 value** — `C1100 Sales` vs GP `1100`, `C6130 Engineering` vs `6130`, 16 of the 19 in the 6xxx band. A uniform chance model says 4.2×; **band-aware it is 2.3×** (8.2 expected against 19 observed), because their codes cluster where GP is densest — suggestive on nineteen points, not conclusive, and the honest figure is the smaller one. **We cannot settle it: GP's segment descriptions are not in Bronze** → **BRZ-18**, so we can see GP uses `6120` and cannot see what GP calls it. If the 19 hold, the 710→64 mapping starts with 19 free rows and a pattern instead of nothing. **The worktag half was checked 7 Sep and is starker:** every GP segment against every readable worktag type is **zero overlap** — their codes are entirely non-numeric (`C1100`, `REGION-Americas`, `SC002`), ours entirely numeric, so no code-level mapping was ever possible. Their 666 cost centres are **602 `zzOLD`** in five retired families — `zSAGE` 207, `zUKG` 158, `zEH` 100, `zAFAS` 79, `zCOST` 58 — plus **64 LIVE `C`-prefixed** functional codes. Two consequences: **the live target is 64, not 666**, so GP's 710 segment-3 values collapse roughly 11:1 — a redesign, not a rename; and **Great Plains has NO family at all**, so it is the one ledger source never brought across even as retired reference, while Sage, UKG, EH and AFAS all were. Third ask added: confirm GP's structure genuinely has not been mapped rather than mapped somewhere we cannot see — if that work exists, none of the other two should start. Asks: the 855→135 crosswalk (a finance decision), and which worktag type each remaining segment becomes — or an explicit decision to drop them, which discards analysis GP carries today and should be said out loud. **What the retrieval answered, 6 Sep.** `LedgerType` has exactly **one** value in the tenant (`Actuals`), so the header field is a constant. **WDJ-7 is not a lookup** — 135 journal sources exist and **none is GP or UKG**; the nearest are `EXTERNAL_CONSOLIDATION_DATA` and `Conversion`, so two sources need *creating*, which is configuration and theirs. **WDJ-6 has evidence**: `Tax_Code_ID`, `Tax_Rate_ID`, `Tax_Applicability_ID` and `Project_ID` all return zero rows, consistent with those sections being out of scope — evidence, not confirmation, since an impl tenant may just not have got there. **WDJ-2's sizing question became a cheap experiment**: `External_Reference_Id` *is* a reference type and returns zero rows, consistent with nothing yet loaded carrying one — so load a single test journal with a `JournalExternalReferenceID` and retrieve it again; if it appears, the return path is a call we already have. Volumes retrieved: Organization 1528, Cost_Center 666, Ledger_Account 135, Company 82. **RUN AGAINST THE TENANT 6 Sep and it works** — `Company_Reference_ID` returned **82 companies across 2 pages** from `headfirst3`. Two corrections came out of that run. (1) **I had asserted there was no server-side filter, and there is one, and it is mandatory**: `Reference_ID_Type` is `minOccurs=1`, so every call names exactly one type and there is no 'fetch everything'. I listed the XSD's elements with a pattern matching `<xsd:element name=… type=…>`; that element declares an inline `simpleType` and carries no `type` attribute, so the pattern skipped it and **I recorded its absence as a finding** — a search silently dropping what it cannot match, reported as a fact about the thing searched. The tenant's validation message caught it on the first authenticated call. Now asserted offline so it cannot be un-learned. (2) `Count` is three digits, so 999 is a hard page cap — that one was right and was re-verified with a parser. **It writes no `ref_` table** — that prefix is platform-owned and this accelerator reads such objects by join and never creates them. **Why the ask shrank:** we had asked the Workday team for a maintained crosswalk in the shape of the `GP Mapping_` tabs. Wrong ask — Workday's **Get_References** (Integrations v46.1) takes a reference ID *type* and returns the actual *values*, and its `@type` enumeration is the same one the Journal fields already use. So the ask is an integration user and endpoint, not a spreadsheet; the half that decays is now the half we refresh ourselves. Same service supplies the Tax Detail `…TypeReferenceID` values, and it is a better source for `ref_workday_reference_type` than the XSD — the XSD gives the ~160 type *names*, so a worktag value checked against it is checked against nothing. **It may also size WDJ-2's one genuine cost:** if journals are retrievable by the external reference id we supply, capturing Workday's id back is a call rather than a build. Worth asking before scoping it. Also: the workbook's descriptions for `AccountingJournalReferenceID` and `AccountingJournalID` **appear swapped**, and **33 of 84 rows are unanswered** — all of ALC Line, Tax Detail, Attachment and Sub-Process BP, four of them Workday-*Required* |
| **Gold access** | **platform team** | `03_usnc_gold_edm_dev` EXISTS but we have no `USE CATALOG`. Ask for the grant **and confirm it is still the target** — four sibling gold catalogs are visible to us and ours is not |
| Job-runner group membership | admin team | blocks `assert_journal_integrity` |
| **`CREATE SCHEMA` on our own catalogs** | **platform team** | Measured 4 Sep: we hold it on NEITHER `01_usnc_bronze_dev` nor `02_usnc_silver_edm_dev`, despite owning the four schemas already in silver. That is why the union view sits in `raw_vault` rather than a conformance schema of its own. Not blocking; revisit if granted |
| Principal for the read grant | **platform team** | our gate enforces an allowlist, so we need the scope-group NAME. Asked 3 Sep. Until then no human can read the vault — access is by ownership only |
| Ownership transfer + Terraform | **platform team — SENT 6 Sep, awaiting a name** | **Carries a hazard: the job's `run_as` must move to the same SP in the same change, or the pipeline loses all access.** `docs/platform_team_requests.html` (**PLT-1**), addressed to Michael, also folds in two corrections to the 3 Sep reply. **Sent to Michael on 6 Sep**, as the consolidated `docs/platform_team_requests.html` rather than the standalone letter — PLT-1 there, with PLT-2..PLT-6 alongside it. **Now waiting on one input:** the service principal's name, which the `run_as` change and the read-grant allowlist both need. What we owe them in return is recorded below |
| Codify `01_usnc_bronze_dev.control` | **platform team** | we created it 31 Aug |
| Restricted deployment folder (**DEF-43**) | admin team | `bundle validate --strict` unusable until then. Evidence and the two non-fixes are in the decision log |
| CDF on `_raw` (BRZ-1) | Bronze team | blocks every satellite load after the first |
| `delivered_count` (BRZ-12) | Bronze team | loop-1 cannot be evaluated |
| ~~**Vault satellites are created WITHOUT their column masks**~~ **FIXED 25 Sep** | ours — closed | `checks/load_satellites.py` creates each satellite with `CREATE TABLE ... AS SELECT * FROM <log> WHERE 1=0`. A CTAS copies the SHAPE but **does not inherit column masks**, so the `stg_*` staging log carries the masks the factory emits into its streaming-table definition and the `sat_*` vault table — the one holding the money — has none. `assert_mask_survival` reported **21 columns** on 25 September: `sat_invoice_header_fieldglass_us.gross_invoice_amount` and the rest of the invoice and legal-entity families. **Under parent decision D3 the column mask is the vault's only PII defence**, so an unmasked satellite is the control absent, not degraded. **Pre-existing, not introduced on 25 Sep** — invisible until that day because no satellite had ever loaded: every earlier run stopped before `load_satellites`. The fix is to build the CREATE with an explicit column list carrying `MASK` clauses rather than a CTAS; the loader already computes the declared column order independently (`declared_columns`), and the types are available from the log. **CLOSED the same night.** load_satellites now issues `ALTER TABLE ... ALTER COLUMN ... SET MASK` after create and before insert -- an ALTER rather than a column list in the CREATE, because `CREATE TABLE IF NOT EXISTS` is a no-op on the tables already created unmasked and would have left all 21 exactly as they were. The table is empty when it runs (CTAS selects WHERE 1=0), so no row is ever readable through an unmasked column. assert_mask_survival went 21 -> 7 -> 0: the remaining 7 were `_v1` plain views and were FALSE POSITIVES -- DEF-53, measured 26 Aug, says a view cannot declare a mask and does not need one, and `exemption()` already said so while the `_v1` site never consulted it. Six checks on the loader and four on the gate, all mutation-proven |
| **`assert_journal_integrity` REMOVED FROM THE JOB, 26 Sep — restore when PLT-2 lands** | **ours — a decision, not a defect** | **Removed deliberately and temporarily**, on the instruction that it comes back when the security is implemented. The gate cannot read the columns it checks: the run-as service principal cannot see money -- `governance.mask_money` and `governance.mask_money_double` admit `scope_unmask_currency_values` by name (they admitted `global_dataplatform_pipeline_job_runners` until 28 Sep, when the platform team refused to use an operational group as a data-access key), so every `debitamt`/`crdtamnt` reads NULL and the gate correctly refuses to assert on zeros. While it stayed in the graph it blocked five downstream tasks — `reconcile_loop1`, `assert_landing_integrity`, `assert_no_broad_grant`, `apply_governance`, `publish_model_metadata` — none of which is failing on its own account. **What is lost while it is out:** debits-equals-credits and the control-total assertion over **2,453,127** journal lines are not checked at all. Not failing, not passing — absent. Nothing else in the estate asserts that balance. **Restore condition:** the moment the service principal is in `scope_unmask_currency_values` (PLT-2) -- NOT the job-runner group, which no longer unmasks money, put the task back and confirm it reports a real balance rather than NULLs. **This is not blocked on us** — it is one group membership, and the ask is already written up as PLT-2. |
| **DEF-54 — WITHDRAWN THE SAME HOUR. `csat_invoice_line_gie` was missing because it is NOT DECLARED ACTIVE, not because of a code defect** | **ours — a one-line configuration omission** | **What is true:** the first fully green run (29/29 SUCCESS, 26 Sep) did not create `csat_invoice_line_gie`, `load_satellites_business` reported *"every satellite is inactive in this lake"* and exited 0, and **`checks/invoice_issue.py` reads that table, so Ameren has no input**. **What I got wrong:** I diagnosed it as a category error at `load_satellites.py:509` — that `active_table_bindings` returns `[]` for every csat because a computed satellite has no source binding. **It has one.** `invoice_line_gie` declares `BUSINESS_VAULT_GIE`, `table_bindings` returns it, and the binding id is `invoice_line_gie/BUSINESS_VAULT_GIE`. Proven three ways: with that id in the active set the binding resolves; with only the PARENT active it does not; with `active=None` it does. **The mechanism is correct and the declaration is incomplete** — `databricks.yml`'s `active_sources` for `usnc_tds` names 19 bindings over 17 entities and **no csat among them**. The loader skipped exactly what it was told to skip and said so. **How the wrong diagnosis happened:** my first probe passed an active set of ENTITY names (`invoice_line`) where the function expects BINDING IDS (`invoice_line/FIELDGLASS_US`), so everything returned `[]` and I read a bad input as a property of the code. **The `src is None` reasoning was real but irrelevant** — `table_bindings` handles it, as the loader's own `proj_src` comment already said. Fix is the declaration. |
| **BRZ-12 WAIVED in `supersede_quarantine`, 26 Sep — capped, dated, removed when Bronze fixes it** | **ours — a decision, not a defect** | **Taken on instruction, to stop two GL entities blocking five downstream tasks.** `supersede_quarantine` failed because 8 quarantined rows (4 in `nhl_general_journal_line`, 4 in `nhl_general_journal_line_closed_year`) carry a NULL `manifest_id`, and a supersede record cannot be attributed to a manifest it does not have. **That refusal was correct and is untouched for every other table** — nothing fabricates a `manifest_id`. What changed is that those two NAMED tables, at a MEASURED cap of 4 rows each, are now a declared dormancy rather than a failure. **Three ways it still fails:** more rows than the cap (the problem grew — new information); a table nobody named; and it is reported in the gate summary as `waived_brz12=N` so a run carrying it cannot read as clean. **A waiver whose table Bronze has FIXED is a loud warning and then an ordinary comparison, deliberately NOT a failure** — the first draft failed there, the offline suite caught it, and failing would have meant Bronze fixing BRZ-12 breaks our load until somebody edits a dict. **What is given up:** loop-1 for those two entities is NOT EVALUATED rather than passing — absent, like `assert_journal_integrity` withdrawn the same day, and **in the same GL domain, which now has two absent controls at once**. Seven offline checks, four mutation-proven. **Remove `_BRZ12_WAIVED` when BRZ-12 lands.** |
| **journal integrity cannot read what it checks** | **ours — one grant** | `assert_journal_integrity` on 25 September: *"2,453,127 line(s) but EVERY debitamt/crdtamnt value reads NULL. Balance and control-total assertions would both pass on zeros."* Its own message names the cause — the run-as identity is not privileged under `governance.mask_money`, so the gate reads its own masked columns as NULL. The data is intact; the reader cannot see it. This is the mask working correctly on the tables that HAVE one, and it means the debits=credits assertion is currently unevaluated rather than passing |
| A payroll detail feed (BRZ-8) | Bronze team | payroll domain, first masked satellite |
| Client mapping (BRZ-2) | **David Willson** | 46 CLIENT slugs, 0 answered. Distinct from the legal-entity roster below, which is about OUR companies |
| **Workday as a vault source — design written, not built** | **ours — needs a landing path** | Spec `docs/superpowers/specs/2026-09-07-workday-reference-source-design.md` and plan `docs/superpowers/plans/2026-09-07-workday-reference-source.md`, both 7 Sep. **Tasks 1–2 DONE 7 Sep; only those two were buildable** — a landing shape and a gate over it, offline. Tasks 3–6 are marked GATED against a destination table nobody owns, WDJ-5's domain grant, and a production tenant. The plan says **do not repoint any entity YAML until the table exists**: swapping one fictional name for another reproduces the `PLACEHOLDER` state while looking like progress. **The four legal-entity entities are repointed, not created** — they have been declared, gated and bound to `PLACEHOLDER` since 5 Sep waiting for a roster that turns out to be in Workday. The `ref_` boundary is **not** relaxed: the test is not "did it come from Workday" but **"would anyone ask what this was last quarter"** — enumerations stay `ref_`, instances become vault entities. `hub_ledger_account` is deliberately **not** extended: GP's 361,706 account combinations and Workday's 135 accounts are different grains, so a separate hub plus a `sal_` same-as link once WDJ-8's crosswalk exists — and neither is built, because an empty link looks like an answer. **Workday is ONE GLOBAL TENANT, hosted in Belgium** (confirmed 7 Sep), not one per region — so the reference data is global while the vault is regional across `weu`/`uks`/`usnc`/`aue`. **That is why `hub_legal_entity` is `key_style: authored`, and it now pays off twice:** an authored key hashes `(legal_entity_code)` with no source *or region* scope, so `UK011` is the same hash key in every lake and `conformance_check` compares regions that agree by construction. Whether the landing writes once or four times is a real choice the spec deliberately leaves to whoever owns the landing. **And a residency question is recorded rather than answered:** an EU-hosted tenant means anything landed in `02_usnc_silver_edm` has crossed EU→US. For companies and accounts that is corporate reference data; it is **not** a formality for the retrievals this design excludes — `Employee_ID` returns 2,121 rows and `Contingent_Worker_ID` 90, and those are people. Nothing retrieves either, and nothing should until someone who owns residency has said what may cross. **The blocker is unowned:** the vault reads tables and the retrieval tool writes a file; something must land Workday's output on a schedule, and we hold no `CREATE SCHEMA`. Sequencing is grant (WDJ-5) → landing → binding → `active_sources`; **no binding goes live against an implementation tenant.** Success criterion 3 is that `legal_entity_roster_worksheet.csv` is **deleted rather than filled in** |
| **Legal-entity register** | **RE-SENT 7 Sep — business / legal, but Workday may already hold it** | **6 Sep: retrieved 82 companies from the Workday impl tenant** — code, name, 16 jurisdictions (`UK011 Impellam Group Limited`, `NL010 HeadFirst Global BV`, `IE100 Irish Recruitment Consultants Limited`). The worksheet has 24 rows and **0 of 96 identifying cells filled**; Workday holds most of that already. **And 7 Sep corrected our own first reading:** we said Workday held nothing about ownership. It holds **30 consolidation groups**, each named for a holding company — `All Headfirst Global Plc Consolidated`, `Impellam Group Limited Consolidated`, `HFBG Holding BV Consolidated`. A consolidation group is how Workday rolls companies up, so **their membership is an ownership hierarchy** — which is what LEG-3..LEG-6 exist to establish. We cannot read membership through `Get_References` (it returns the group *names*). **Tried the call that can, 7 Sep — `Get_Organizations` in Human_Resources — and it is refused:** `SOAP-ENV:Server.processingError | The task submitted is not authorized`. That is a **permission** refusal, not a malformed request: a deliberately broken request to the same service returns `Client.validationError` instead, so Workday validated ours and then declined it. **One grant away**, asked as part of WDJ-5. The client is built and gated offline — `build_organizations_request` / `parse_organizations`, five checks, four mutation-proven and the fifth's fixture strengthened after a mutation failed to fire. **And the parser was written against the wrong schema type first** — `Organization_DataType` is the REQUEST side; the response is `Organization_WWS_DataType`, which spells the same fields `Reference_ID`/`Name` rather than `Organization_Reference_ID`/`Organization_Name`. It would have returned None for everything while raising nothing, and a fixture written from the same wrong type would have agreed. Caught by reading the schema twice, not by a test. And one name is a question in itself: `All Headfirst Global Plc Consolidated` reads like an apex while `Impellam Group Limited Consolidated` sits beside it as a peer — **whether one already contains the other is the same structure the brand unification has to produce.** **It does not close the questions and they were not withdrawn:** a Workday *Company* is a financial reporting entity and is not guaranteed 1:1 with a legal entity (LEG-2), the values are from an implementation tenant, and it supplies **neither a registration number nor the ownership parent** — so the hierarchy is untouched. What it does change is **LEG-1**, which gains a third option (Workday's `Company_Reference_ID`, one column, jurisdiction-prefixed, already assigned) now our lean; and **LEG-7**, where 'who owns the register and where does it live' may simply be *Workday, retrieved on a schedule* rather than a new spreadsheet. **Asked formally 5 Sep: `docs/legal_entity_questions.html`, LEG-1..LEG-8.** LEG-1 (what identifies an entity — an HFIG code, or `(jurisdiction, company_number)`) and LEG-7 (who owns the register and where it lives) block the other six. 24 entity candidates in `legal_entity_roster_worksheet.csv`, each with a confirmed group and jurisdiction; **all four identifying columns empty on all 24 rows.** A brand plus a country narrows the search to one register and one company; it does not name it. The whole family — hub, hierarchy, effectivity and now `sat_legal_entity_details` — is built, gated and loading nothing until these land |
| **Two ungated worksheets** | **ours** | `client_mapping_worksheet.csv` and now `legal_entity_roster_worksheet.csv` are both DERIVED from Bronze or from supplied lists, maintained by hand, and gated by nothing — so a correction to one measured column is invisible and a newly-provisioned client is never added. `dow_jones`, `everbank` and `lyondellbasell` are already provisioned in Bronze and absent from the client sheet. Either build an emitter that refreshes the measured columns while preserving the `__FILL` answers and reports added or removed rows, or accept a manual re-measure each time. **Cannot be gated offline — the truth lives in Bronze.** Full audit in the decision log, 4 Sep |
| **Which second relationship type?** | **business / legal — LEG-5** | A legal entity can play more than one role (confirmed 5 Sep). `hal_legal_entity_hierarchy` carries ownership only. If "operates the programme for" is also needed it should be a SEPARATE link, not a `relationship_type` column — a mixed link makes every traversal need a filter, and one that forgets merges the two silently |
| Source contract says nothing about Fieldglass | **ours** | The union view is excluded from the Bronze-facing contract (they cannot act on an object we create). So the contract expresses NOTHING about the 30 per-tenant tables underneath it — and those ARE Bronze's. What belongs there is a requirement against the PATTERN, which the contract cannot express yet |
| UKG gl: 3 columns will NOT backfill | **ours — decision** | probably leave them |
| FIELDGLASS_UK / _APAC | **ours — waits on evidence** | only US is declared. Declaring a binding to an unverified source is the `FIELDGLASS_EU` mistake |
| Masked `_v1` under a live mask | **ours** | untested; trails BRZ-8 |
| Loader compute at scale | **ours** | new trigger: staged per run over ~9.5M, or BRZ-1 landing |
| **PK/FK constraints: emitted, gated, NOT applied** | **ours — needs a workspace** | `governance/vault_constraints.sql` now carries **29 primary keys and 35 foreign keys** as informational UC constraints, derived through `contract.grain` and `contract.foreign_keys` — the same authorities `append_only_check`, the DBML and the ontology read. Gated four ways, including that the edge count equals the diagram's. **Nothing runs it, and that absence is asserted too.** Two questions only a workspace can answer: does an SDP streaming table accept `ALTER TABLE ... ADD CONSTRAINT`, and is every key column already `NOT NULL`, which UC requires of a primary key? Answer both, then wire it and remove the not-wired check in the same change |
| **Column descriptions: 65% filled, the rest is BRZ-16** | **Bronze — asked 5 Sep** | Was 0 of 409. Now **251 of 409** with no guessing: `naming.COLUMN_DOC` documents the technical columns once (promoted from comments that were already there), and the keys derive their own sentence from the model. **The remaining 158 are business columns only a person can describe** — 133 distinct names. `descriptions:` per entity is the place; `contract.description` resolves all four sources; the data contract carries it today. **A tautology fails the build** — a sentence whose words are contained in its column's own name — so the gap stays visible instead of being filled with restatements. Blind on concatenated legacy names (`debitamt`), measured and left alone deliberately. **The remaining 142 split by owner: 113 are source facts, now BRZ-16 in `bronze_layer_work_requests.html`, grouped by feed so a partial answer is useful; 29 are ours, of which 24 are written and 5 are the deliberately ILLUSTRATIVE payload of `csat_job_request_custom_promoted` — the generator emits whatever the registry promotes, so they carry no fixed meaning to describe** |
| **Descriptions not yet on the UC column comment** | **ours — needs a workspace** | The data contract, the DBML diagram (**251 of 409 column notes now LEAD with the meaning**) and the ontology (`rdfs:comment` on every datatype property that has one — only 10 today, because those are satellite payload, exactly the columns nobody has described) all consume `contract.description`. The **Unity Catalog column comment does not**: it lands in `factory._declared_schema`, the DEF-16/19/26 path where `create_streaming_table(schema=...)` is not an overlay and a mask clause sits beside every column. Adding `COMMENT` there needs verifying against a live table first, like the constraint DDL |
| **Committed diagram images can go stale** | **ours — by design, stated** | `diagram/img/*.png` is the only form of the seven diagrams visible on GitHub (a committed `.html` is served as a source blob, and Pages on a private repo would publish publicly). **The HTML beside them IS byte-gated**; the PNGs are not, because regenerating one needs Chrome — `verify_repo` asserts one image per document, none orphaned, none truncated, and nothing about whether it matches. Re-run `tools/render_archify.py` after any model change that moves a table. Closeable the same way as the render-gate row above: Node plus the skill plus a headless Chrome in CI |
| **No secret scope exists — PLT-7** | **platform team** | Secrets live in **Azure Key Vault** and only Platform can create them. Measured 6 Sep: `usnc_tds` has **no secret scopes at all**, of either kind. Two corrections that were wrong here first: `databricks secrets put-secret` is not the route (a Key Vault-backed scope is **read-only through the Databricks API** — secrets are created in the vault), and we cannot create the scope either, since it needs the vault's `resource_id`/`tenant_id`/`dns_name` and the CLI throws `UNAUTHENTICATED` without permission on the vault. **Not blocking today** — `tools/fetch_workday_references.py` reads `WD_PASSWORD` from the environment or, agreed 6 Sep as the stop-gap, `--password-file`. **Decided 7 Sep: `.workday-credentials` stays on the working machine until PLT-7 lands** — not deleted after each use, not re-typed per session, and the end condition is PLT-7 and nothing else. Recorded so a plaintext credential found on disk reads as deliberate with a named expiry rather than as an oversight; the residual exposure is the machine itself, which no check here can see. That reader **refuses** rather than warns on the two failure modes: a file readable by group or other, and a file inside the repo that git does not ignore — the second because `bundle deploy` uploads the repo *minus ignored paths* into a folder granting `CAN_MANAGE` to `users`, so an unignored credential file is published on the next deploy rather than merely committable. One `.gitignore` line covers both, and `verify_repo` asserts the line survives and that nothing matching it is tracked. **Blocking the moment anything is automated**, because the alternative is a credential in a file the bundle ships into a world-writable folder (PLT-3), which the embedded-credentials gate refuses by design |
| **The Archify render gate cannot run in CI** | **ours — stated, not hidden** | `tools/archify_render_gate.py` asks Archify's own validator whether each committed document can be drawn — the check that caught seven undrawable nodes the schema gate had passed for a day. It needs **Node and the globally-installed skill**, and CI has neither, so it SKIPS there and `verify_repo` prints why. **On CI the weaker schema gate is all there is**, which is exactly the arrangement that let the defect in. Two ways to close it: install Node plus the skill in the CI job (7.4 MB, MIT, zero runtime deps — `ARCHIFY_HOME` already exists so it need not go global), or vendor the two renderers we use and accept maintaining the copy. Neither is urgent while the gate runs on the machine the documents are regenerated on; it becomes urgent the moment someone regenerates them somewhere else |
| **`ref_dq_expectation` holds no business rule** | **ours** | 169 rules, **every one `compiled-in` key-safety**. The silver dashboard's coverage tile exists to report exactly this as 0%. Each new rule must be shown to fail before it is believed |
| ~~**The memory files are still ungated**~~ | **CLOSED 5 Sep — as far as it can be** | They live OUTSIDE the repo, so `verify_repo` cannot read them and should not try: it must run from an extracted zip with no `~` to reach. **The skill half is gated** — in-repo, so the standing no-hardcoded-count rule now covers it and this file. **The memory half is editorial, and all four notes were rewritten so that going out of date cannot make them wrong:** the interpreter version and the profile list are stated as last observations; the profile note now leads on the HOST as the discriminator (five names resolve to one EU host, one does not) with `preflight_target` as the verification that needs no recall; the ownership paragraph is flagged as in flux ahead of the service-principal transfer; and the three check counts the CI note had recorded "as of" a commit are gone, since it told the reader in the same breath not to remember them. Nothing mechanical is possible here — the durable answer is that a memory states what was observed and when |

### 6 Sep: the Archify diagrams, and the gate that found them undrawable

**Archify** (`tt-a1i/archify`, MIT) turns a typed JSON IR into HTML/SVG diagrams. It is
**not** Graphify's competitor — Graphify derives a call graph from source, Archify renders
a diagram from an IR — so the ~500-file threshold that ruled Graphify out does not apply.

**Its weakness is the one thing this repo is built against.** Its own workflow is *"the
agent creates typed JSON IR from your description"*: the determinism is in the RENDER, not
the derivation, so a diagram can be beautifully drawn and disagree with the system. We skip
the describing step — `tools/emit_archify.py` emits the IR **from** `metadata/entities/`
and `resources/vault_job.yml`.

**Then the renderer was installed, and both committed documents failed it.** This is the
part worth keeping. Until 6 Sep the gate was *conformance to the vendored JSON Schemas*,
and the schemas are not the contract: `dataflow.schema.json` bounds a node's `row` at
`minimum: 0` with no maximum, while the dataflow renderer has **exactly five rows per
stage**. Seven of twenty-nine nodes had been emitted at rows 5–12, producing non-finite
coordinates, and had passed every check here on every run since they were added. The
workflow document failed too — schema v1's fixed 92px node refuses every task key in this
job. **A checker whose stated guarantee exceeds what its fixture can detect** is the defect
this repo has now found six times in its own code, and this was the sixth — written by the
same hand that wrote the other five gates, in a module whose docstring claimed it covered
"the ways a GENERATOR realistically goes wrong."

**What changed, and why each choice was forced:**

- **`architecture`, not `dataflow`.** Dataflow's geometry is fixed — five rows, 215px
  between stage centres, 112px nodes — and a vault whose table names run to 34 characters
  cannot be laid out in it without truncating names. Architecture's grid takes `cellW`,
  `gapX`, `gapY` and per-component `size`, so the geometry is **measured from the content**.
  Nothing is truncated and nothing is invented.
- **One document per domain, not one for the vault.** The foreign keys form one connected
  graph — `hub_organisation` touches nearly everything — so there is no natural seam, and
  a single 29-table picture is unreadable whatever draws it. The split is by
  `entity.domain`, which the model declares. **Every edge that leaves a domain is kept**:
  the far table is drawn as an `external` context box, so no document lies by omission,
  and the union of connections still equals the DBML's 35 `Ref:` edges. Six documents,
  4–11 boxes each.
- **Columns are distance from a hub, not the entity kind.** Kind-based staging let a hub's
  satellite skip the links column, and the renderer rejects an edge that crosses an
  unrelated box. Longest-path depth guarantees every edge points at the adjacent column.
- **Workflow moved to schema v2**, whose compiler solves geometry from the measured
  document. The only thing the emitter has to get right is the graph. 23 tasks, 36 edges,
  12 lanes from dependency depth, matching `vault_job.yml` exactly.
- **Implied connection labels are dropped.** `hub_organisation → nhl_payroll_detail` joins
  on `organisation_hk`, which both endpoints already state; drawing it costs a label's
  clearance in every gutter and says nothing. A **roled** key is the opposite —
  `parent_legal_entity_hk` and `child_legal_entity_hk` are the only thing distinguishing
  two edges between the same pair of boxes — so those are always drawn.

**One emitter, and that is also a fix.** `tools/emit_archify_{dataflow,workflow}.py` were
the same 90 lines twice, differing in a constant, each carrying the other's dead branch as
`if "dataflow" == "dataflow":`. Two copies of one emitter is the same drift, grown in our
own tooling. They are now `tools/emit_archify.py`, and the domain list is **read from the
model**, so a new domain gets a document by existing and a removed one takes its file with
it.

**The gate is `tools/archify_render_gate.py`** — it runs Archify's own `validate` over
every committed document and fails the build on the renderer's findings. Proven to fail
twice: shrinking one component box below its label, and the original row-9 defect verbatim
(where `archify.validate` still reports CLEAN, which is the whole point). `archify.validate`
is kept and now honestly described — a shape check that needs only Python, in front of a
real check that needs Node.

**Schemas vendored, not fetched** — `vendor/archify/schemas/`, MIT, pinned at commit
`c6519401f7b9` and **verified byte-identical to the installed 2.17 skill** on 6 Sep.
`verify_repo` must run offline from an extracted zip; a gate that fetches a schema passes
when the network is down.

**What the install actually put on the machine, having read it first:** `~/.claude/skills/
archify`, 7.4 MB, copied not symlinked, **global — every project on this machine, not just
this repo.** Zero runtime dependencies (`ajv`/`parse5`/`saxes`/`simple-icons` are dev-only);
the CLI imports node builtins and `spawnSync`s only its own scripts; one network call, a
`GET` to a fixed `tt-a1i.github.io` manifest that throws if the URL is not the compiled-in
constant, disabled by `ARCHIFY_UPDATE_CHECK_DISABLED=1`. The `skills` CLI itself is
`vercel-labs/skills`.

**Showcase quality is deliberately not the bar.** All seven documents pass `validate` at
`standard` — 0 errors, which is the renderer's correctness bar. `--quality showcase` adds
crossing and corridor constraints that would need per-edge `via`/`channelX` routing
decisions, and a generated artefact must not carry hand-tuned geometry: the moment it does,
the next model change silently invalidates it.

**Rendering them found a third thing `validate` cannot see.** `deliver` produced all seven
pages at 9/9 artifact checks, and then `visual-check` — which drives a real Chrome at four
desktop viewports — failed four of them. The viewer scales the authored canvas up to the
reading width, so a tall narrow canvas is *magnified*: `hfig_job` at 614×1192 rendered its
first two boxes at 260×170px and wanted 2771px of scroll on a 1440×900 laptop. Two constants
were measured down against that check and neither is a preference:

- **a minimum 1.6 landscape aspect on the canvas**, with the grid origin re-centred in the
  padding, so **no box moves relative to another** — the layout the render gate approved is
  the one that ships. 1.4 was measured and three documents still failed.
- **row pitch 68/28**, down from 76/40, which was the most generous pitch at which `hfig_job`
  cleared `viewer/projected-text-readability`. 64/24 and 60/20 also pass and buy nothing.

Two structural alternatives were tried against the render gate and **both failed, which is
why the shape stayed as it is**: sub-ranking each depth by DV layer (Hubs / Links /
Satellites) reintroduced column-skipping and `edge-through-node` on four documents, and
wrapping a nine-row column into two grid columns failed on exactly the two documents it was
meant to help, because the edges to the second sub-column cross the first.

`hfig_workflow` still overflows vertically and that is left alone: twelve dependency stages
is a tall picture, and nothing flattens it without lying about the load.

**Both rendered forms are now committed, and the HTML is byte-gated.** `diagram/html/`
was ignored at first on the grounds that CI could not regenerate it — the argument was
sound about CI and wrong about the repo, because the same is true of the render gate and
that gate is still worth having. So the pages are tracked and `verify_repo` re-renders each
one and fails on a diff, standing down with a printed reason when the renderer is absent or
when the installed Archify differs from `accelerator.archify.RENDERER_VERSION`. `deliver`
is byte-stable within one version and says nothing about another; change the renderer and
you re-render and bump the constant in the same commit. Proven both ways: a one-character
typo in a committed page fails, and a version mismatch stands the gate down rather than
failing the build for something that is not this repo's fault.

**GitHub still will not render those pages** — it serves a committed `.html` as a source
blob, and Pages on a private repository publishes publicly unless the account is on
Enterprise, which an internal data model has no business doing. So `diagram/img/` is tracked
too, at ~90KB apiece and embedded in `diagram/README.md`; the folder renders all seven
inline at `hfg.ghe.com/data-engineering/hda-edm-dv/tree/main/diagram`. The HTML is for cloning and
opening; the PNG is for reading in a browser.

`tools/render_archify.py` writes both forms in one command. The capture is **full-page** —
`archify visual-check` writes PNGs too but clips them to the viewport, so the twelve-lane
workflow came out at Stage 4; the height is measured from the delivered page and the window
sized to it (1920×2525 for the workflow, 1920×1080 for the six domains). They are dark
because the viewer resolves its own theme and defaults to dark, and
`--force-prefers-color-scheme=light` does not override it — measured, the corner pixel stays
`(2, 6, 23)`. A dark card reads correctly on either GitHub theme.

**THE IMAGES ARE GATED FOR EXISTENCE, NOT FOR CONTENT, and that limit is the point of
writing it here.** `verify_repo` asserts one image per document with none orphaned and none
truncated — both proven to fail, by removing an image and by a 500-byte truncation. It
**cannot** assert an image matches its document, because regenerating one needs Chrome. So
these are the one artefact in the repo that can silently go stale, alongside
`hfig_data_vault.dbdiagram`. Re-run `tools/render_archify.py` whenever the model changes;
there is a row for it below.

**A third count escapee, found the day after the ban:** `pyproject.toml` said the offline
suite runs "121 structural checks" against 1,129. Removed, and the file added to the rule.

### What has gone out, and to whom

| document | to | sent |
|---|---|---|
| `docs/platform_team_requests.html` — PLT-1..PLT-7 | Michael, Platform | **6 Sep** |
| `docs/workday_journal_export_analysis.html` — WDJ-1..WDJ-9 | Workday team | **7 Sep** |
| `docs/legal_entity_questions.html` — LEG-1..LEG-8 | business / legal | 5 Sep, **re-sent 7 Sep** |
| `docs/bronze_layer_work_requests.html` — BRZ-1..BRZ-18 | Bronze team | ongoing; **follow-up 7 Sep** covering BRZ-16, 17 and 18 |
| `docs/business_legal_decisions.html` — the 12 decisions that need a business, legal or finance answer | Amy Keser, Global Programme Manager | **7 Sep** |
| Reply to Platform on their three answers — PLT-2 measured false, PLT-3 accepted as a better route, PLT-C1 confirmed | Platform | **7 Sep** |
| `docs/platform_sp_name_chase.md` — the service principal's name, one ask, no change window | Michael, Platform | **7 Sep** |
| Workday master data is empty for Ameren — no customer record, and none of the 29 suppliers exists | Paul McMahon | **24 Sep** |

**The decisions page owns no ids and only cites them** — LEG to the legal-entity questions, WDJ to the journal analysis, BRZ to the Bronze requests. `verify_repo` asserts every id it names is still a raised ask in the document that owns it, and **cannot** tell whether a decision has been made; the live state is this board. Proven three ways: a cited id its owner does not raise, an ask withdrawn from its owner, and a whole family lost from the summary. **All five were sent as HTML rather than PDF.** That is fine and was checked when the first
one went: each renders standalone in a browser or as an attachment, and the Google Fonts
link is an enhancement — every family has a real fallback stack, so a recipient with no
network sees the same document in a system font. **Nothing about them depends on this
repo being reachable.**

**The WDJ document that went out is the one that had four asks written after the analysis
was finished** — WDJ-5, 8 and 9 exist only because we called the tenant, and WDJ-2 changed
from a sign-off request into a design correction. Anyone reading a version dated before
7 September is reading a different document.

### Answered 7 Sep — Platform replied to three asks, and one answer was wrong

Platform answered, and **the reply was to the retired three-request note, not the
consolidated queue** — so it covers three asks, one of which had already closed. PLT-1, 4,
5, 6 and 7 have had no answer. Michael has the full queue from 6 September; whoever replied
was reading the older body.

**All three claims were measured the same day rather than accepted or disputed on their
face.** That is the only reason the reply we sent back says anything useful.

| their answer | what the workspace says |
|---|---|
| **PLT-2** — "all data engineers should be members of this group already" | `is_account_group_member('global_dataplatform_pipeline_job_runners')` returns **false** for `adrian.turcu@headfirst.group`, the identity `vault_load` runs as |
| **PLT-3** — "bundles are released to the service principal's workspace directly, which normal users don't have access to" | `bundle validate --target usnc_tds` **still warns** `/Workspace/Shared/.bundle/hfig-dv-accelerator/usnc_tds` is writable by all workspace users |
| **PLT-C1** — "Done." | Clean, and **still clean five days on**: schema-level `SELECT` held by nobody on `raw_vault`, `business_vault` or `control` |

**PLT-2 is the one that matters, and the gap is "should be" against "is".** Nobody needs to
be argued out of a policy — the group may well be intended to hold the data engineers. It
does not hold this one, and a column mask reads membership, not intent. Until that single
membership exists, 8.5M masked values still read NULL to the loader and
`assert_journal_integrity` stays red.

**PLT-3's answer is better than the ask, and we took it rather than restating ours.**
Repository-driven deployment into the service principal's own workspace removes the laptop
from the deployment path entirely — a better outcome than the restricted folder we asked
for, which we would still have deployed into by hand. Two consequences worth booking:
`bundle_root_prefix` is already a variable so pointing it there is one line, and **PLT-1 now
blocks Platform's own preferred route**, not just ours. That is a stronger argument for the
service principal's name than anything in the original letter.

**PLT-C1's re-check earned its keep.** The item was closed on 2 September, so re-measuring
looked redundant. It was not: those grants arrived by **propagation** from the `full_scopes`
roll-up groups, so a hand revoke would have survived until the next apply and no further.
Five days without recurrence is evidence the carve-out is doing the work — the revoke alone
would not have told us that.

**The general shape, again.** Three claims, all made in good faith, one false and one
describing a path we are not on. Measuring them cost three queries; accepting them would
have cost a load that silently writes NULLs and a deployment we believed was restricted.

### Sent 6 Sep — what we now owe Platform, and what we are waiting on

**`docs/platform_team_requests.html` went to Michael in full on 6 September**, the whole
consolidated queue rather than the standalone `run_as` letter: PLT-1 (the ownership transfer
and its sequencing hazard) alongside PLT-2..PLT-6 and the two closed items kept for their
reasoning. The pre-send checks were done and are recorded here rather than lost: no unfilled
placeholders, no credentials, renders standalone in a browser or as an attachment, and the
one measurement its central argument rests on was **re-taken the same day against the
deployed job** — `schedule`, `trigger` and `continuous` all absent, `edit_mode` `UI_LOCKED`.

**One input blocks two things.** PLT-1's transfer and PLT-4's read-grant allowlist both wait
on the service principal's name. Nothing else on the queue depends on it. **As of 7 Sep it
blocks a third** — Platform's own preferred deployment route needs the principal's name and
workspace path before `bundle_root_prefix` can point there.

**The chase asks for a name and deliberately not a time — corrected 7 Sep.** The original
PLT-1 wording asked for both. In TDS the time is not a thing to ask for: nothing runs on a
timer, every load is started by hand, so Platform can transfer whenever and we redeploy
`run_as` before the next load. Get the order wrong and the load fails with a permission
error — harmless here, and it announces itself. **Asking a platform team to coordinate a
change window for a test workspace spends their goodwill on ceremony**, and goodwill is the
scarce input on a queue where five items are already unanswered.

**The inverse is the real hazard, so it is written into the card.** In production the
coordinated window *is* required — the run-as identity holds production catalog rights and a
scheduled load can land in the gap. A TDS answer carried forward unexamined is how that gets
lost, which is why PLT-1 now says *do not carry this answer forward to production* in the
card itself rather than only here.

**Three commitments we made in writing, booked nowhere else:**

1. **We will report `assert_no_broad_grant` unprompted** the first time it runs green again.
   That is a promise to send Platform a gate result they did not ask for a second time, and
   it comes due the moment PLT-2 lands. **Two gates report for the first time in that same
   run** — `assert_freshness` is deployed and has never executed — so expect to be reading
   two new results, not one.
2. **We will not run the pipeline between the ownership transfer and the `run_as` redeploy.**
   That is the whole "the window costs nothing" argument, and it is a matter of not doing
   something rather than of engineering around it.
3. **We told them our gate will go red on their correctly-made per-table grant** unless it
   learns the grantee. If they wire PLT-4 before answering that, our build fails and it will
   look like their change broke us. Watch for it.

**And the measurement has a shelf life.** It was true when the note was sent. Re-measure
before the transfer is *executed*, not before it is agreed — a schedule is one edit away
from being true and PLT-1's argument does not survive it.

**A private artifact copy exists** at `claude.ai/code/artifact/623688bd-…`, published 6 Sep
and **still to be deleted by Adrian** — the tooling here can publish and unwatch but not
delete. It predates the consolidation, so it holds the superseded standalone letter rather
than the queue that was actually sent, which is a second reason to remove it.

### What 5 September added, in one place

Twenty PRs, all merged, all green on three CI jobs. Detail is in `git log` and
`DECISION_LOG.md`; this is what changed about the *state*.

**`buyer` became a modelled problem instead of a cleaning one.** Four entities —
`hub_legal_entity` (authored, so one company is one row across every feed),
`hal_legal_entity_hierarchy` (the first hierarchical link this model has built),
`esat_legal_entity_hierarchy` (driving key **child**) and `sat_legal_entity_details`.
Nothing resolves at load time: Silver takes Bronze faithfully, Gold filters per use case,
`hub_job_request` keeps `buyer` as delivered. All four sit on **placeholder** bindings and
load nothing. Design in `docs/legal_entity_design.md`.

**The rebrand is why it landed now.** Impellam and HeadFirst unify in weeks. In this shape
that is one hub row, two link rows, two effectivity rows — no key change, no reload. Under
the previous model `buyer` is hashed into the tenant key, so a rename re-keys the hub and
strands every satellite row behind it.

**The roster is derivable-complete and the questions are asked.** 24 entity candidates,
every one with a group and a jurisdiction. What remains needs a company registry, and is
now `LEG-1`..`LEG-8` in `docs/legal_entity_questions.html` — a sibling of the Bronze page,
addressed to business and legal. LEG-1 (what identifies an entity) and LEG-7 (who owns the
register) block the other six.

**Two things the model always knew and the lake was never told.** `governance/vault_constraints.sql`
now carries **29 primary keys and 35 foreign keys** as informational UC constraints,
derived through the same authorities `append_only_check`, the DBML and the ontology read.
And **267 of 409 columns now carry a description** — technical columns promoted from
comments that were already in `naming.COL`, keys deriving their own sentence. Both are
gated; neither is applied to a live table yet, and the not-wired state is asserted as
firmly as the content.

**The remaining 142 descriptions were split by owner rather than dumped.** 113 are source
facts — now **BRZ-16**, grouped by feed so a partial answer helps. 24 were ours and are
written. 5 are the deliberately *illustrative* payload of `csat_job_request_custom_promoted`
and stay blank. **A tautology fails the build**, so the gap stays visible instead of being
filled with restatements of column names.

**The freshness SLA is built and wired** — the floor the Thoughtworks paper names, and the
one foundational row we scored human-era. Measured against the last successful **load**,
never the last row: `aud_table_load` records a run whether or not anything landed, so a
quiet feed reads FRESH and a stalled pipeline reads STALE. Two limits stated rather than
implied — it is table-keyed, so a hub cannot prove *which* feed delivered; and it is
required only of targets that have actually decided what loads.

**The record was split, and its citations gated.** 3,669 lines became a live board plus
`DECISION_LOG.md`. Splitting it showed `DEF-nn`/`BRZ-nn` citations were never checked to
resolve anywhere — 18 already dangling, all now written up. **Hardcoded check counts are
banned** in this file, the gates skill, `README`, `CHANGELOG` and `DEPLOY.md`: each script
prints its own, and two of the three numbers here were stale within a day of being written.

**Four papers read, and the fourth was about us.** Three agree on the same missing
consumption layer. `okf_graphify_evaluation.md` asks instead how you would detect a stale
knowledge artefact within 24 hours — good answer for the repo, none for the two surfaces
loaded into every session from outside it. Both were stale. The skill half is now gated;
**the memories were rewritten so that going out of date cannot make them wrong**, which is
the only mechanism available to a file `verify_repo` cannot read.

### If you are picking this up fresh

**Everything is sent. Do not write another document.** Five documents are out with five
audiences — the send table is above. Forty-one asks sit with other people. The instinct on
a fresh session is to produce a summary of the summaries; resist it.

**What a session restart destroys, and how to get it back.** Every Workday retrieval from
6–7 September lived in a session scratchpad and is **gone**. Nothing in the repo holds the
82 companies, the 135 ledger accounts, the 666 cost centres or the 188 custom organisations
— deliberately, because promoting implementation-tenant values into a committed worksheet
is what this work exists to stop. Re-retrieve when needed:

```bash
uv run --frozen python tools/fetch_workday_references.py \
  --password-file .workday-credentials --type Company_Reference_ID --out /tmp/c.json
```

`.workday-credentials` is on the working machine, git-ignored, mode 600, **and stays there
until PLT-7 lands** — decided 7 Sep, see `docs/workday_reference_service_access.md`. The
retrieval is proven against the live tenant; the client's limits and the two schema
mistakes it cost are in `src/accelerator/workday.py`'s docstrings.

1. **Chase WDJ-5's `Get_Organizations` grant.** The first domino, and the cheapest. It is
   currently refused — `Server.processingError | not authorized`, a permission refusal and
   not a malformed request. Granted, it lets us read the consolidation hierarchy and may
   remove **LEG-3 through LEG-6 from legal's queue entirely**.
2. **Chase the job-runner group membership** (PLT-2) — **asked and answered "already done"
   on 7 Sep, and measured false the same day.** The chase is now a specific correction, not
   a request: the membership does not exist for the identity that runs the loads. It turns `assert_journal_integrity`
   green, resumes `assert_no_broad_grant`, runs `assert_freshness` for the **first time**,
   and unblocks `apply_governance` and `publish_model_metadata` behind them. **Five gates
   change state in one load, two of them reporting for the first time** — expect that
   rather than discover it. We also owe Michael the `assert_no_broad_grant` result
   unprompted when it happens.
3. **Chase the service principal's name** (PLT-1 and PLT-4 both wait on it — and **as of
   7 Sep so does Platform's own preferred deployment route**, which is the better lever),
   then execute
   PLT-1: ownership transfer and `run_as` in the *same* change, and re-measure that
   `vault_load` still has no schedule immediately before, not before it is agreed.
4. **Place the two unowned items.** Where retrieved Workday data lands blocks Tasks 3–6 of
   `plans/2026-09-07-workday-reference-source.md`. Whether EU-hosted worker data may cross
   into a US lake blocks nothing today and blocks everything the moment somebody retrieves
   `Employee_ID`.
5. **Gold access** is still unasked-for and still the largest consumption-layer unblock —
   one `USE CATALOG` grant, PLT-5.

**Do not repoint a legal-entity binding.** The plan's Task 4 says so and a check enforces
it: swapping one fictional table name for another reproduces the `PLACEHOLDER` state while
looking like progress.

**Do not go looking for work by re-running the suites.** They are green and have been at
every commit. Every defect found between 30 Aug and 4 Sep came from asking what a gate
*actually covers* versus what it claims — never from a failing test.

**Six traps, which cost time on 4 and 5 Sep. Read these before writing a check here.**

- **Comparing column NAMES across different source tables.** A child reads a different
  Bronze table from its parent, so the columns carrying one business value are named
  differently — `timesheet_line`'s `job_reference` IS the hub's `reference_code`. This
  flagged **correct** keys three separate times in three files. Compare the key's SHAPE
  (arity, scope, literals in position) statically, and its VALUES in
  `test_spark_derivation` by feeding both sides shared values positionally.
- **Counting `FAIL` lines to measure a mutation.** A mutation that stops the model loading
  makes both suites die with a traceback and print **zero** `FAIL` lines, so a caught
  mutation reads as uncaught. Measure by exit code.
- **Asserting a generated string rather than an engine's acceptance.** A check asserted the
  union SQL contained `UNION ALL BY NAME`; Databricks answered `[PARSE_SYNTAX_ERROR]`. The
  text passed and the load failed. If only a workspace can accept it, the offline check can
  only describe the SHAPE that works.
- **Gating an artefact's provenance and calling it correctness.** The ERD PDF matched its
  digest perfectly while carrying six blank pages, because `width:auto` on an inline SVG
  resolves to zero in wkhtmltopdf. A gate over a rendered artefact must assert something
  about the RENDER.
- **`--only <task>` does NOT run upstream; `--only +<task>` does.** Running
  `apply_source_unions` without the `+` skipped `create_control_objects`, so `saveAsTable`
  created `ctl_source_union` itself — silently, WITHOUT its DDL's `delta.appendOnly` — and
  `assert_append_only` failed the whole load. Both new writers now refuse to write when
  their record table is absent.

- **A name list typed out next to the module that declares it.** The single most common
  defect of 5 Sep, found five times. `render_erd` scanned PDFs for `link_` when the link
  prefix is `lnk_`; `append_only_check` hand-typed 10 vault prefixes where 15 exist, having
  already been patched twice after the fact, and was still missing `sal_` — so a same-as
  link table could have been mutated with the append-only gate never looking at it;
  `conformance_check` hand-typed a third, different set; `("link", "nhl", "hal")` appeared
  in 22 places with nothing to read; and 16 sites retyped a set whose declaration says in
  as many words not to. **Every instance had already drifted, and every one was silent.**
  `naming` now declares each set and `verify_repo` refuses a retyped copy — but the rule is
  general: if you are about to write a tuple of kinds, prefixes or system columns, find
  what already declares it.

**Before any reload, read `docs/RELOADING.md` and trust step 1.** `validate-only` caught an
`UNRESOLVED_COLUMN` in the Fieldglass satellite binding **before anything was dropped** on
4 Sep — which is the four-hour outage that procedure was written after. The 4 Sep worked
case is in that file: the drop was **10 objects**, not the 3 first estimated and not the 14
a pattern match suggested. A hub AND a satellite each load through a staging log.

**Local setup.** `uv sync --extra spark` for the Spark suite; it finds a JVM at
`~/.local/share/jdk/current` (Temurin 17, userspace, `rm -rf` to undo) when `JAVA_HOME` is
unset. **The `.venv` is Python 3.11.15** — the CI floor, not the newer rung; check it rather
than assuming, because which one it is inverts the risk. `wkhtmltopdf` and poppler
(`pdfinfo`/`pdftotext`) are needed by `tools/render_erd.py`.

**Do NOT use the Databricks MCP tools.** Measured 4 Sep: `mcp__databricks__execute_code`
resolves to profile **`tve-dev`** — an EU workspace — and failed only because its token had
expired. Use the CLI with `--profile hfig-usnc-tds`, or `databricks jobs submit`.

**One process note.** The seven commits that took Fieldglass live went **straight to
`main`**, unlike every other change on 3–4 Sep, which went through a branch and a PR. They
are green and CI agrees, but the most consequential change of the two days had no diff
reviewed before it landed. Branch first.

---

## Found during subsystem C, 27-28 September, and not recorded anywhere else

**Two named hard gates are invoked by nothing.** Measured 28 September by grepping every gate
name against `resources/`, `databricks.yml` and `.github/`:

| gate | wired? |
|---|---|
| `checks/journal_integrity_check.py` | **INVOKED BY NOTHING** |
| `checks/control_conformance_check.py` | **INVOKED BY NOTHING** |
| every other gate (`ame_parity`, `mask_survival`, `append_only`, `hash_parity`, `loop1_reconciliation`, `aggregate_reconciliation`, `schema_grant`, `conformance`) | wired |

`journal_integrity_check` is a **named hard gate** in `.claude/skills/dv-accelerator-gates`
("fails the build if debits != credits, or line count != control total, for any GL journal"). It
was removed from the job on 26 September pending PLT-2, which is recorded above — what is NOT
recorded is that `control_conformance_check` is also unwired, and for no stated reason. A required
gate nothing invokes is a gate that has never run.

**`tests/test_accelerator.py` defines `_raises` TWICE, and the second one is dangerous.**

    line   50: def _raises(fn) -> bool:   except Exception  -> True   (any raise)
    line 4700: def _raises(fn, *a):       except ValueError -> True
                                          except Exception  -> FALSE

The second shadows the first for every call after line 4700. It is worse than "narrower": a
`TypeError` returns **False**, so a check asserting *"this must raise"* reads as *"it did not
raise"* and goes **green over a genuinely broken function**. Calls between lines 50 and 4700
depend on the broad semantics, so this is not a safe drive-by rename — it needs a deliberate pass
over every call site. Three checks written during subsystem C avoided it by using a
distinctly-named helper instead.

**Emails sent 28 September** (Adrian): PLT-2 to Michael, carrying the `ctl_invoice_issuance` drop
as a second admin-only item; and the combined Finance/Bronze letter — GL-1..GL-4, the corrected
BRZ-12 (a control/ETL id carried in the Fieldglass payload, superseding the reduced form), and the
new `workday`/`workday_raw` schema request with SOAP `Get_References` stated as the preference.

---

## PARKED — the ETL control framework, 28 September

**Where it is:** branch `spec/etl-control-framework`, 5 commits, **local only, not pushed**.
`docs/superpowers/specs/2026-09-28-etl-control-framework-design.md` (rev 2) and
`docs/superpowers/plans/2026-09-28-etl-control-branch1-the-shape.md`.

**Why parked:** Bronze and Michael have been written to and have not replied. Their answers scope
branches 2 and 3; branch 1 is the only part that does not depend on them.

**What it is:** one run identity from the ingesting mechanism through to the vault row, so that
loop-1 reconciliation, silent-feed detection and period attribution — three controls that assert
nothing today — have something to group by. Ideas from a framework Adrian designed for IBM
c. 2012; the DataStage implementation is deliberately NOT carried over (Databricks and SDP
semantics throughout).

**State: the design holds, the plan does not.** Two independent reviews, both returning REWORK.

Closed and verified against the repo: bronze control DDL is the GENERATED
`governance/control_objects_bronze.sql`, not silver's hand-maintained file; the emitter has no
view support at all; D4 excludes bronze ingestion from this bundle, so there is no SDP share to
write; `ctl_delivery_manifest` is registered MUTABLE; `etl_run_id` is an opaque UUID, because a
composite put the mechanism inside the identity and its `|` collided with `hashing.DELIMITER`
(`||`), `NULL_TOKEN` (`^^`) and the `key_no_delimiter_*` rule.

Outstanding when it resumes — the second review's list:
1. Task 1 Step 3's check code has a **SyntaxError** (a replacement field split across two adjacent
   f-string literals) and uses `_im_try`, which does not exist. Measured: the suite would not
   import on 3.11 or 3.13.
2. **§2b overstates.** `aud_load_run` is `job_run_id, phase, target, active_sources, recorded_at`
   — **no outcome column**. It records that a phase RAN, never that it COMPLETED, which is what
   the dropped layer gates carried. Either add one status column or withdraw the claim.
3. `aud_load_run` is in **CORE**, not silver as §5 files it — adding a column touches all three
   layers and the published bronze contract.
4. Tasks 3 and 5 would red four currently-green checks plus `emit_control_contract.py`'s emitted
   header, which hard-codes that bronze declares one table of its own and that it is MUTABLE. None
   of it is enumerated.
5. `control_standard.py:218-223` states the mutable split as DESIGN — "a manifest records what
   SHOULD happen and must stay correctable". Task 5 inverts it with an argument about writers, not
   about modelling. The counter-argument belongs in the spec.
6. Supersede, the orphan-id gate and retention are named in spec §7 and assigned to no branch.
7. Task 2's view check is vacuous (`all()` over an empty `VIEWS`; the only view is declared in
   Task 4), and Task 4 has no Spark venue — `tests/test_accelerator.py` stubs pyspark at line 1783.
8. GL **fiscal** period attribution is promised in §1 and defined nowhere. Only Fieldglass
   (week-ending date) is settled.

**A lesson worth keeping past this framework: plan-authored check code has failed 5 times out of
5 in this repo.** Word-presence matching a comment; an eagerly-evaluated f-string detail; a
vacuous `all()`; an unfailable LEFT ANTI text match; and now code that does not parse. An
implementer has caught every one. When this resumes, plans should state WHAT must be asserted and
WHAT mutation must prove it, and leave the code to the implementer, who writes it against the live
suite where a syntax error surfaces in seconds.

---

**The record below this line has moved.** Everything from the 4 September artefact-gate
audit backwards now lives in [`DECISION_LOG.md`](DECISION_LOG.md) — closed work, kept for
its reasoning, newest first. This file holds only what is live.

## Do NOT deploy `feat/client-opco-split` alone — `key_derivation_guard` will say `RE_KEYED`, correctly

The hub split renames sixteen hash-key COLUMNS (`organisation_hk` → `client_hk` /
`operating_company_hk`, and `client_legal_entity_hk` → `engaging_legal_entity_hk`). Not one
hash-key EXPRESSION moved — that is asserted byte for byte against
`tests/golden_key_expressions.json`, and it was reproduced independently at review by
exporting both trees and re-rendering: 91 expressions each side, 75 identities unchanged
with zero expression changes, 16 renamed and all 16 byte-identical.

`checks/key_derivation_guard.py` classifies **by key NAME** (`key_differences`, "by key
name"). A rename is therefore not a *move* to it — it is a removal plus an addition. The
removed names' tables hold rows in `usnc_tds`, so `keys_at_risk` marks them risky and
`verdict()` returns **`RE_KEYED`**, which FAILS and prescribes drop-and-reload.

**That is the gate working, not a defect.** It cannot distinguish a column rename from a
re-key, and it is right not to guess: the rows really are keyed under names the model no
longer declares. The resolution belongs to Task 6, which carries the version bumps. What
must not happen is someone deploying this branch by itself, reading the refusal as a bug
and reaching for `--force` or a gate edit. Nothing in the branch says so, hence this entry.

## Suite abort: `spec.hash_key_columns` called outside a check condition, in SIX places

`tests/test_accelerator.py` calls `spec.hash_key_columns(...)` at module level, outside any
`check()` condition, in SIX places -- on this branch at roughly lines 1208, 1217, 1264,
1273, 1362 and 1524. A model defect aborts at whichever it reaches FIRST, so the line a
given failure names depends on both the defect and the current line numbering: the split
work on `feat/client-opco-split` hit ~1277 on its own tree, and this entry originally named
only 1524 because that was the one measured first. Naming a single line was the error --
the defect is the idiom, repeated. Any model that makes it raise takes
the ENTIRE suite down early -- exit 1, no summary block, and everything declared after
that line never runs. An abort is indistinguishable from a failure by exit code, which is
why the mutation-proof standard requires PASS + FAIL to equal the whole suite and the
summary block to have printed. Measured 28 September 2026 while mutation-proving the
`stream_generation` checks: renaming one binding to `FIELDGLASS_US_G2` produced

    accelerator.spec.SpecError: invoice_line/FIELDGLASS_US_G2: parent 'organisation'
    declares key_literals ... but has no source binding named 'FIELDGLASS_US_G2'

which is a CORRECT refusal reported in the worst possible way: an abort is indistinguishable
from a failure by exit code, and it hides every check behind it.

This is the tenth instance of the suite-abort-on-exception shape in this file. The fix is
the same one applied to the others -- compute into a sentinel inside try/except, then
compare in the check condition.

CONSEQUENCE TODAY: the check "no binding NAME ends in _g<digits>" could not be proven by
mutation, because the mutation that would trip it aborts the suite first. Its predicate is
proven separately (see the check immediately above it).

UPDATED 29 September 2026, hub split: **line ~1524 is not the only one, and it is not even
the first.** Line ~1277 calls `spec.hash_key_columns(model, _e, _b)` the same way, and it
is the site that actually fires. Re-pointing `nhl_payroll_detail` from
`operating_company` to `client` -- the mutation the split's own brief asked for -- aborts
from line 1277 rather than 1524, in the first tenth of the run: **no summary block, and
the overwhelming majority of the suite never evaluated.** Reproduce with
`uv run --frozen --python 3.11 python tests/test_accelerator.py` after re-pointing that
entity. Bare calls also sit at lines ~1208, ~1217, ~1362, ~14457 and ~19720.

SECOND CONSEQUENCE: the split's parent-key-lookup check could not be written as a sweep
over the live model at all. The property it would assert is exactly spec's raise
condition, so there is no model state in which the sweep is red and the suite still
reaches it -- an unfailable check, which this repo treats as worse than none. It was
written instead as a REFUSAL over a copied metadata tree (`_split_derive_copy`), which
runs every time and is provable. Until the bare call sites are fixed, that is the only
shape a check in this area can honestly take.

## `nhl_invoice_line_rev2` holds 1164 duplicate rows, and dropping it needs the service principal

Left behind by the `stream_generation` defect closed on 29 September 2026 — the mechanism
is fixed (`c3bd6c1`; the measurement and the root cause are written up in
[`DECISION_LOG.md`](DECISION_LOG.md)), **but the rows it already produced are still
there.** The fix stops new ones; it does not remove these.

`nhl_invoice_line_rev2` = 2329 rows / 1165 distinct keys at grain `invoice_line_hk`.
`nhl_invoice_line_rev1` = 1165 / 1165, untouched, and the stable view still resolves to
rev1 — **no consumer is affected**, which is why this is a cleanup and not an incident.

THE BLOCKER IS PERMISSION, NOT EFFORT. The table is not cut over and has no consumers, so
rebuilding it is cheap. It is owned by the service principal, so dropping it is subject to
the same permission wall as the `ctl_` ledger drop — the tenant-move blocker at the top of
this file. It clears when that clears.

## The publish_stable_views rehearsal aborts rather than fails

`tests/test_accelerator.py` (~15540-15600) computes `_psv_u_kinds`, `_psv_expected_stable`
and five end-to-end runs inside a `try:` whose `finally:` restores the patched module but
does not swallow. Any exception during the patched `exec_module` or during those five runs
propagates, the suite exits 1, and every check from there to end-of-file never runs.

The idiom predates the keyed-kind staging work -- the DEF-52 `_v1` block uses it too, and
the first `exec_module` already happens unguarded earlier -- so a raise is unlikely. That
change moved five full runs and a global mutation inside it, which is why it is worth
writing down now rather than after it fires.

SAME CLASS AS the `spec.hash_key_columns` abort recorded above, and the same fix: compute
into a sentinel inside `try/except`, compare in the check condition. The restore itself is
correctly guarded and mutation-proven; it is the computation around it that is not.

## A staged-kind filter that no longer filters

`tests/test_accelerator.py` (~20663) builds `_qtn_expected` with
`for e in _am.entities if e.kind in naming.STAGED_KINDS`. Since every generatable kind is
now staged, that condition selects everything -- the filter reads as a scope and is a no-op.

Harmless today. It matters if a kind is ever un-staged: the EXPECTED side would shrink while
the emitted side stayed put (quarantine twins are emitted for every kind), and the resulting
failure would read as "a twin moved" when it is a scope mismatch. That is in fact why the
hub-only mutation flipped this check during review -- it failed for the right reason by
accident rather than by design.

## Should `load_hubs` skip inactive keyed entities the way `load_satellites` does?

**Open. Adrian's call. Not decided inside the 29 September fix round, deliberately.**

`checks/load_hubs.py` has no activity filter over its TARGETS -- it takes
`--active-sources` as of 29 September, but only to decide which declared column masks to
ALTER onto the table it creates, never which tables to build -- so it builds, loads and
publishes a stable view for EVERY keyed entity of every keyed kind. `checks/load_satellites.py` takes `--active-sources`, resolves it through
`spec.resolve_active_sources` and skips a table with no active binding, and
`checks/publish_stable_views.py` did the same before it left the job. The two loaders
disagree, and until 29 September the disagreement was invisible because `load_hubs` owned
only hubs, every one of which has at least one active binding in `usnc_tds`.

It is visible now. Six of the eleven newly-staged keyed entities --
`hal_consolidation_hierarchy`, `lnk_client_contracting_entity`, `lnk_client_job_request`,
`lnk_legal_entity_consolidation`, `nhl_payroll_detail`, `nhl_timesheet_line` -- have no
active binding in this lake. `load_hubs` will create their physical table from their
staging log, skip the grain probe, **bootstrap a stable view over that table**, and write
an `aud_table_load` row.

**And the log is not empty: it holds one ghost row.** `factory.build` calls `_emit_ghost`
unconditionally against `naming.pipeline_table(entity.kind, table)`, which for a staged
kind is the LOG -- so the six log one zero-key row each, the loader inserts that row on its
first run, the probe skip prints "1 staged row over 1 distinct key", and the audit row says
staged 1, accepted 1. **The consequence that matters is not arithmetic:**
`checks/cutover_vault_version.py` takes `target_rows` from a plain `SELECT COUNT(*)`, so
`target_rows == 1` and its MIDDLE refusal -- the one that exists to stop a view being cut
over an empty table -- never fires. What fires is the third (ungated), which
`--gated-by-run` clears, and the loader has already written the audit row that makes that
lookup succeed. An operator following the old runbook text would have cut a stable view
over a ghost-only table. Corrected in the runbook §0.

**Both readings are defensible and that is why this is a question rather than a defect.**

* **BUILD THEM (today's behaviour).** D5's principle is that every lake DECLARES the whole
  model and an inactive binding produces an EMPTY table rather than a missing one, so the
  inventory is identical across the four lakes. A stable view over an empty table is the
  consistent end of that: the consumer-facing name resolves everywhere, and a binding going
  active later needs no catch-up publish. It also means `assert_mask_survival`'s F3
  existence assertion finds a view for every generatable kind in every lake.

* **SKIP THEM (`load_satellites`' precedent).** `checks/apply_governance.py` gates the
  quarantine twin and the stable view on the binding being active, so a view `load_hubs`
  bootstraps for an inactive entity gets NO grant and is unreadable by
  `scope_tds_edm_vault_read` -- an object that exists, resolves, and returns a permission
  error, which is the shape that cost an hour on 28 September when a grant-less view read
  as an empty table rather than as forbidden. It also writes a coverage audit row for a
  table nothing loaded, which is exactly the kind of row `freshness_check` has to learn to
  ignore.

Whichever way it goes, the two loaders should agree and the reason should be written down
once rather than inferred from two files. Until then, the behaviour is what the code does,
and `docs/superpowers/plans/2026-09-29-keyed-kind-staging-runbook.md` §0 describes it.

## Deploying keyed-kind staging WITHOUT the version bumps duplicates every quarantined row

**Recorded 29 September, not fixed, and deliberately not fixable here.** This is the real
reason behind "do not deploy without 6b", and it is a stronger reason than the one that was
written down (which was "`load_hubs` cannot INSERT into a streaming table, so the run fails
loudly").

**The mechanism.** The deploy renames every source flow in the vault pipelines: a flow's
name is built from the table the pipeline writes (`factory._flow_name`), and for a staged
kind that is now `stg_<table>`. SDP keys a flow's checkpoint by its name, so every renamed
flow gets a fresh checkpoint and re-reads its bronze source from the start. For the source
flows that is intended -- the staging log is new and empty and the loader's anti-join
collapses whatever arrives.

**The quarantine flow is renamed with them, and its target is not.** `factory.build` sets
that flow's `target=_quarantine_table(table)`, and `_quarantine_table` runs the name back
through `naming.unstg()` on purpose, so the twin keeps the name it has always had:
`qtn_<name>_rev<CURRENT>` -- the existing, populated, `delta.appendOnly` table. New flow
name, old target. **Every reject ever written is appended a second time, and the twin is
append-only, so it cannot be undone in place.**

**And the two gates that would catch it are skipped.** `reconcile_loop1` and
`assert_landing_integrity` both depend on `load_hubs`, which on an unbumped lake fails
first (its target is a pipeline-owned streaming table). The pipelines SUCCEED and commit
the duplication; the only signal is a red `load_hubs` whose message is about a streaming
table -- it reads as "the version bump is missing" and says nothing about the twin.

**MEASURED 29 September, and it scopes the paragraph above rather than retracting it:**
all four keyed quarantine twins in `usnc_tds` hold **0 rows** -- `qtn_invoice_line_rev1`,
`_rev2`, `_rev3` and `qtn_journal_line_rev1`. In THIS lake, today, the replay would
duplicate nothing, so the loud `load_hubs` failure is the stronger reason here and the
claim that this one is stronger holds only where a twin is populated. The mechanism is
general and applies to any such lake, and to `usnc_tds` the moment one twin takes a reject.
It is a row count on a date, not a property: re-measure before relying on it.

**It does not apply once 6b lands.** With the versions bumped, `_quarantine_table` derives
the twin from the bumped name, so the flow writes into a new, empty `qtn_<name>_rev<NEW>`
and there is nothing to replay into. So the quarantine-flow naming is NOT a defect to fix
on this branch -- it is correct behaviour with a precondition, and the precondition is the
combined migration's business. Fixing it here would mean changing how the twin is named,
which is exactly what `_quarantine_table`'s docstring exists to prevent.

**The hazard is precisely "merge and deploy without 6b".** Written into
`docs/superpowers/plans/2026-09-29-keyed-kind-staging-runbook.md` §1.0 as a prerequisite,
where an operator will meet it before running anything.

## `retire_vault_version.py` cannot retire an entity the model has REMOVED, and that is the case it is most needed for

Found 29 September while writing the combined migration's runbook (§7.4). Recorded rather
than fixed, because the fix is a judgement about what "declared" should mean and that is
Adrian's.

The tool models ONE operation: retiring a SUPERSEDED version of an entity that still
exists. `main()` walks `model.entities` for one whose `stable_tables()` yields the
`--stable-name`, and if none does it prints

> `'<name>'` is not a stable view name any entity in the model declares. Refusing to guess
> which version is 'declared'.

and returns 1. That guard runs **before** the physical name is built, before the
table-exists check, before the row count, and before `information_schema.views` is read at
all.

**So on a removed entity NEITHER of the two documented refusals ever evaluates** — and the
one that is lost is the LIVE refusal, which exists precisely because dropping the table a
stable view is serving is silent and unrecoverable. The operator is left doing the live
check by hand, in the one case where the model can no longer help them do it.

`hub_organisation` is the live instance: the split removed it, its `_rev1` table and its
stable view are still in `usnc_tds`, and the retirement is therefore a manual
`DROP VIEW` + `DROP TABLE` as the service principal. The runbook spells that out.

**The trap worth naming** is that "not declared" reads like a CLEARED refusal. §4 of the
runbook says both refusals must be cleared; an undeclared entity obviously clears the
declared one, so the natural conclusion is that only the live check remains. The code is
the other way round: being undeclared is an EARLIER refusal, and it takes the live check
down with it. The combined-migration brief made exactly this reading before the code was
read.

**And the obvious workaround is a worse defect.** Adding the entity back to
`metadata/entities/` to satisfy the tool now trips `spec.validate_model`'s refusal of the
retired name — correctly, because re-declaring `organisation` beside the two halves is the
partial revert that fails nothing and splits the rows across two hubs.

Options, none taken: teach the tool a `--removed` mode that skips the declared lookup and
keeps the live check; or let `declared_version = None` fall through to the live check with
the declared refusal treated as vacuously satisfied. The second is smaller and is probably
right, but it changes the meaning of a refusal message that is currently unambiguous.
## The tree-walking checks scan `.worktrees/`, so using a worktree reds the suite

Measured 29 September 2026 on `main` with two Superpowers worktrees present. Three checks in
`tests/test_accelerator.py` fail, and every offending path is inside `.worktrees/`:

    FAIL  and no live file still states the PRE-CHANGE identity
          ['.worktrees/client-opco-split/DEPLOY.md', '.worktrees/keyed-kind-staging/...', ...]
    FAIL  exactly ONE place in the whole tree escapes an apostrophe
          [('.worktrees/client-opco-split/src/accelerator/sql_text.py', "\\'"),
           ('.worktrees/keyed-kind-staging/src/accelerator/sql_text.py', "\\'"),
           ('src/accelerator/sql_text.py', "\\'")]
    FAIL  every quoted SQL literal built by a bare f-string ...
          unreviewed: every entry prefixed '.worktrees/'

The second is the clearest: the check asserts exactly ONE escaper exists and finds three --
the real one, correct, plus a copy per worktree.

**CI IS UNAFFECTED AND MAIN IS GREEN THERE.** `actions/checkout` produces a clean tree with
no worktrees, so this never fires in the pipeline. It fires only for a developer who uses
the workflow this repo's own skill recommends.

WHY IT MATTERS ANYWAY. Superpowers' subagent-driven-development creates worktrees under
`.worktrees/` by design, and this repo's plans invoke that skill by name. So following the
documented process makes the suite red for reasons unrelated to the work, which trains a
reader to skim past a FAIL -- in a repo whose entire discipline rests on a red check meaning
something.

TWO PARTS TO THE FIX, and the first alone is not enough:
  * `.worktrees/` is NOT in `.gitignore` -- it should be, so `git status` stays readable;
  * but the scanners use `ROOT.rglob(...)`, which does not consult `.gitignore`. They must
    skip the directory explicitly.
Whichever way it is done, it should be ONE exclusion read by every tree-walker rather than
three copies of a path check -- this repo has been bitten by a duplicated set more than once.
