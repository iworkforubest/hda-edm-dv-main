# Two legal-entity hierarchies: the group's own, and the clients'

Design, 24 September 2026. Amends the legal-entity model described in
`docs/legal_entity_design.md` and the three placeholder entities that implement
it. Written after `2026-09-24-workday-legal-entities.md` established what Workday
actually holds.

## What this is for

The model has one hierarchy. It needs two, because the two it is being asked to
carry are different in source, in authority, in shape, and in who may change
them:

* **Ours** — the Impellam Group / HeadFirst Group corporate structure. Held in
  Workday, retrievable today, changes when the group restructures.
* **Clients'** — the corporate structure of the 54 MSP clients. Not in our
  Workday, not in any feed we hold, changes when a client restructures.

Merging them is not merely untidy. It is unsound, for a reason the existing model
already states: `esat_legal_entity_hierarchy` drives on `child_legal_entity_hk`
and holds that **for any child, at most one parent edge is open at a time**. One
shared link table breaks that invariant the moment any entity has a parent in
both trees, and it breaks it silently — every row still joins, and the rollup is
wrong.

## The decision that shapes everything below

**Workday's structure lands in the EDM as Workday expresses it.** No flattening.

This matters because Workday's group hierarchy is *not* company-to-company. It is
two layers:

```
All_Headfirst_Global_Plc_Consolidated          Company_Hierarchy   (root)
└── Impellam_Group_Limited_Consolidated        Company_Hierarchy
    └── Impellam_UK_Limited_Consolidated       Company_Hierarchy
        ├── subordinate: Carbon60_Limited_Consolidated
        ├── subordinate: Science_Recruitment_Group_Limited_Consolidated
        └── includes:    IE100, UK013, UK103, IE900        Company
```

The 36 `Company_Hierarchy` nodes are **consolidation groups — accounting
constructs, not legal entities**. `Impellam_UK_Limited_Consolidated` is not a
company. Measured: 0 of the 82 Companies carry a superior; their parentage is
membership of a consolidation group, present on 80 of 82.

Flattening this to company-to-company edges would require deciding which company
"owns" each consolidation node, and the only available signal is its name. That is
an inference, and this repo has been wrong often enough when inferring from names
that it is not worth being wrong again for the convenience of one fewer table.

## Architecture

Five entities for the group side, four for the client side, over one shared
roster. The client side gained the contracting relationship and its effectivity
after Amy's 24 September reply; the sentence that said "one hierarchy" predates
it.

```
hub_legal_entity                    the conformed roster -- UNCHANGED
                                    82 Workday Companies + every client entity a feed names

GROUP SIDE (Workday -- RETRIEVABLE today, not loadable: landing is gated)
  hub_consolidation_group           the 36 Company_Hierarchy nodes
  hal_consolidation_hierarchy       group -> group        from Superior_Organization_Reference
  esat_consolidation_hierarchy      when each group edge was in force
  lnk_legal_entity_consolidation    company -> group      from Included_Organization_Reference
  esat_legal_entity_consolidation   when each membership was in force

CLIENT SIDE (no source yet, declared shape)
  hal_client_legal_entity_hierarchy   client company -> client company
  esat_client_legal_entity_hierarchy  when each client edge was in force
  lnk_client_contracting_entity       client company -> OUR company
  esat_client_contracting_entity      when each contracting relationship held
```

### The roster stays one hub

`hub_legal_entity` is not split. Its authored key exists precisely so that
Fieldglass's `"Guidant Global, Inc."` and Workday's `US100` resolve to one row,
and splitting the roster by whose company it is would reintroduce the problem the
authored key was chosen to solve. A client that is also a supplier stays one row.

**Separation is by hierarchy table, not by roster.** A legal entity appears in
whichever hierarchy applies to it. Nothing prevents an entity from appearing in
both, and nothing needs to: the invariant that was at risk was *one open parent
per child per tree*, and with two tables each tree keeps it independently.

### Consolidation groups get their own hub

`hub_consolidation_group`, because they are a different kind of thing. Putting
them in `hub_legal_entity` would mean the roster whose stated contents are "every
company in Impellam Group and HeadFirst Group" also contained 36 things that are
not companies, and any rollup that forgot to exclude them would double-count —
once through the company and once through the group it rolls into.

Key style: **federated on Workday**, not authored. These are Workday's objects,
their identity is Workday's `Organization_Reference_ID`, and HFIG does not assign
them. This is the opposite choice from `hub_legal_entity` and deliberately so —
a legal entity is our fact about the world, a consolidation group is Workday's
fact about its own configuration.

### The group hierarchy is two relationships, not one

`hal_consolidation_hierarchy` is a hierarchical link: consolidation group to
consolidation group, roles `parent`/`child`, exactly the construct
`hal_legal_entity_hierarchy` uses today and for the same reason — a restructure
must be an insert, not an update to a hub.

`lnk_legal_entity_consolidation` is an ordinary link between two different hubs:
which companies belong to which consolidation group. It is not a hal, because its
two legs are different hubs.

Workday delivers this membership twice — `Included_Organization_Reference` on the
group and `Included_In_Organization_Reference` on the company — and
`workday.landing_rows("membership", …)` already emits both directions tagged by
`relation`. **The link loads from the group's `includes` direction only.** Both
directions are the same edge; loading both would put every edge in the table
twice, and the group side is the one Workday populates more completely (35 of 36
groups, against 80 of 82 companies).

`Subordinate_Organization_Reference` is **not** loaded. It is the inverse of
`superior` and carries no information the hal does not already hold; storing both
creates two places for one fact to disagree.

### The client hierarchy is a rename

`hal_legal_entity_hierarchy` and `esat_legal_entity_hierarchy` become
`hal_client_legal_entity_hierarchy` and `esat_client_legal_entity_hierarchy`.
Their shape is already right — company to company, roles parent/child, effectivity
driven on the child — and their placeholder source is already called
`CLIENT_PORTAL`. They were client-shaped in intent; this makes the name say so.

**They stay placeholders and load nothing.** There is no client-structure source:
not Workday, not the Fieldglass feed, and not Amy's template. A declared shape
that loads nothing is the honest state and the one this repo already uses for
exactly this reason.

**The feed was checked properly rather than assumed.** An earlier draft of this
spec said `Company_Code` and `Company_Name` are blank, which is true of Ameren and
misleading everywhere else: across the whole table they hold **353 codes and 340
names**. They are not client subsidiaries. All but one sit on the 27,842 rows with
a blank buyer, every one of which also has no `Invoice_ID` — the non-invoice rows
the union already excludes — and their values are **staffing suppliers**:
`Manpower`, `LanceSoft, Inc.`, `Terra Staffing Group`, `Premier Staffing Solution
LLC`. On the twelve real buyer codes the column holds one value each, and it reads
as an accounting category (`101-IT ENTERPRISE or SYSTEM CAP`), not a company.

So the conclusion stands for the FIELDGLASS feed and the reason is now the right
one: it carries a supplier dimension where a client-subsidiary dimension would go.

### Corrected 24 September: Hubspot is a candidate source, and this spec said there was none

Adrian: *"we have a hubspot data source in bronze layer."* Measured immediately
rather than argued about, and it changes this section:

`01_usnc_bronze_dev.hubspot_raw` holds **7,328 company rows (2,457 distinct
companies)** and **15,430 deals**, and `companies` carries
**`hs_parent_company_id`** — a parent/child relationship between client companies,
which is exactly the shape this hierarchy needs and exactly what this spec said did
not exist anywhere. The claim was made from reading three sources and not from
asking the lake, which is the failure this project keeps repeating.

**It is the right shape.** Sampled edges are unmistakably client groups:

```
Aerotek Inc.      -> Allegis Group        Aetna            -> CVS Health
TEKsystems        -> Allegis Group        Airgas           -> Air Liquide
NBC Universal     -> Comcast              SCANA Corporation -> Dominion Energy, Inc.
```

"How much does Allegis Group spend with us in total" is Amy's question, and
Aerotek + TEKsystems is its answer.

**It corroborates Amy on Ameren.** `Ameren Corporation` is in Hubspot with an
**empty** `hs_parent_company_id` — independently matching her *"not applicable to
all (Ameren as an example)"* and the single buyer code the invoice feed carries.

**Three measured caveats, none of which are reasons to ignore it.**

| | |
|---|---|
| Coverage | **72 of 2,457** companies carry a parent (2.9%) |
| Referential integrity | **53 of those 72** parents resolve to a company in the table; **19 dangle** |
| Versioning | rows are snapshots — `hs_object_id` repeats, so a load must take the latest by `timestamp` |

**And the join to spend does not exist yet, which is the real blocker.** Spend
attaches at `Buyer_Code`; Hubspot keys on `hs_object_id`. Of the 12 buyer codes in
the invoice feed, **only `AEE1` carries a buyer name at all** — the other eleven
are blank. `TEKS` looks like TEKsystems, and this repo has been wrong often enough
inferring identity from a name that the resemblance is recorded as a hypothesis and
nothing more. Mapping buyer codes to Hubspot companies is a business input, exactly
as `legal_entity_design.md` argues for the roster itself — and it belongs to **Eva
and the CRM team**, who own Hubspot, not to Data & AI.

**What changes in this design: nothing structural.**
`hal_client_legal_entity_hierarchy` is the right shape for these edges and stays a
declared shape. What changes is that its source is now a named candidate with
measured properties rather than an absence, and the next question is the buyer-code
mapping rather than "where would a hierarchy even come from".

### What the client hierarchy is for: spend reporting by client group

Settled 24 September. This is an **analytical rollup**, not a billing control —
aggregating contingent-workforce spend across a client's entities up to the group,
to answer "what does this client group spend with us in total".

Three things follow.

**The rollup is buyer code to client group, and it must come from outside the
VMS.** Measured: the table holds **12 real buyer codes**, one per client, with no
sub-entity structure under any of them. A client group with several VMS programmes
would appear as several buyer codes, and nothing in the feed says they belong
together — which is precisely the fact the hierarchy has to supply. It is a
business input, exactly as `legal_entity_design.md` already argues for the roster.

**Spend attaches at buyer-code grain**, so the join is spend → legal entity →
hierarchy, and the hierarchy's leaves must be the entities the feeds actually
name. An entity in the roster that no feed names contributes nothing and is not a
defect; a buyer code with no roster row is.

**The stakes are lower than billing, and saying so is not an excuse to be
careless.** A wrong edge here produces a wrong report rather than a wrong invoice.
But client group spend totals are what account managers negotiate rates against, so
the gates stay as specified — the difference is that an error is recoverable by
reloading, which an issued invoice is not.

### Confirmed by Amy, 24 September, with one trap attached

*"Where we have multiple legal entities (children) we need 1 overarching client
record (parent) — the parent ID is the key for reporting on total spend across
multiple records."* That is this design, in her words, and it settles that the
rollup key is the parent.

**The trap is in the same sentence.** *"This is not applicable to all (Ameren as
an example)."* Most clients are a single legal entity with **no parent at all** —
Ameren among them, which the feed corroborates: one buyer code, no sub-entity.

So a spend projection that groups by parent id silently **drops every
single-entity client**, because their parent is null. Ameren's entire
1,176,896.90 would be absent from a client-group spend report and nothing would
raise: the query succeeds, the rows join, and the total is simply smaller. The
rule is **a client with no parent rolls up to itself**, and it is a test, not a
comment — see the plan's Review Focus.

### One contracting entity per client, and it changes

*"We should have only 1 contracting entity on the Vertage side per client — this
may change as we go through the legal entity unification only where we have a mix
of services and one is professional services (SOW)."*

This is a relationship between a **client** legal entity and **one of ours** —
Guidant Global Inc, in Ameren's case. Both sit in `hub_legal_entity`, so it is a
self-relationship on one hub and needs roles: `client` and `contracting`.

**`kind: link`, not `hal`.** Roles are permitted on any link kind whose parents
are not distinct, not only on hierarchical ones, and this is not a hierarchy —
our contracting entity does not *own* the client. Calling it a hal would put a
false claim in the model's own vocabulary.

**It carries effectivity because Amy says it moves.** The legal-entity unification
changes which of our companies contracts with a client, and a relationship that
changes must close and reopen as inserts or "who did we contract through last
year" stops being answerable. Driven on the **client** leg: at most one
contracting entity per client at a time, which is the "only 1" in her sentence
made enforceable rather than hoped for.

### The one question this purpose raises: as-was or as-is

Spend reporting through a hierarchy that changes has two defensible answers, and
the model supports both — `esat_client_legal_entity_hierarchy` makes "who owned
this in March" and "who owns this today" equally answerable. What must not happen
is a gold projection picking one silently.

* **As-was** — spend rolls up the structure in force when the spend occurred. A
  published report never changes. Right for audit and for contractual volume
  commitments.
* **As-is** — all spend rolls up today's structure. Year-on-year comparison is
  like-for-like, at the cost of restating history whenever a client restructures.

**Decided 24 September: as-is by default, as-was available.** "What does this
client group spend with us" is nearly always asked about the group as it stands
today, and a report whose prior-year figure moves when a client acquires a
subsidiary is the behaviour most people expect from spend analysis.

Two consequences the implementation must carry rather than leave implicit:

* **The gold spend projection states as-is in its own description**, so a reader
  who finds a restated prior-year figure meets the reason rather than a bug.
* **As-was must stay reachable, not merely theoretically derivable.** Defaulting
  to as-is is the moment as-was quietly stops being tested, and an effectivity
  satellite nothing queries is an effectivity satellite nobody notices is wrong.
  The test suite exercises a point-in-time traversal against a fixture whose
  structure changes mid-history, and it must be mutation-proven: rolling the
  as-was query up through today's parent has to turn it red.

## Data flow

```
Workday Get_Organizations (Human_Resources, v46.2)
  -> tools/land_workday_references.py --kind organisation   82 Companies + 36 groups
  -> tools/land_workday_references.py --kind membership     one row per edge
  -> Bronze
  -> hub_legal_entity          (Company rows, filtered on type = Company)
     hub_consolidation_group   (Company_Hierarchy rows)
     hal_consolidation_hierarchy   + esat
     lnk_legal_entity_consolidation + esat
```

The `type` filter is why the parser defect mattered: before it was fixed, `type`
was `None` on all 82 Companies and neither filter could be written at all.

**Retrieve, not load — the arrow into Bronze does not exist yet.** That is Task 3
of `2026-09-07-workday-reference-source.md`, and its Task 4 is gated additionally
on a **production** tenant — *"do not start against `impl-`"*. Every entity this
design adds is therefore a declared shape bound to `PLACEHOLDER`, exactly like the
ones it sits beside.

**Updated 24 September: the destination is now named, and the direction is the
other way round.** Adrian: both Hubspot and Workday write to Databricks, and
Workday's reference values land in Bronze as Workday reference data. So the
gate is no longer "nobody owns a destination" — it is Bronze creating a
`workday_raw` schema and volume on the pattern all 15 other file sources follow,
and the Workday integration writing into it. **We do not land it ourselves**, which
means the `PLACEHOLDER` table names in these entities should be settled against
that convention before they are repointed: no other source prefixes its tables with
its own schema's abbreviation.

This replaces an earlier line in the architecture block that called the group side
"loadable today". Its data is retrievable today, which is not the same thing, and
the difference is the whole reason nothing here loads.

## Effectivity, and what Workday cannot give us

Both group relationships get an effectivity satellite, same construct and same
reasoning as the existing one: a restructure closes an edge and opens another,
both as inserts, so "who owned this in March" stays answerable.

* **`esat_consolidation_hierarchy`** drives on `child_consolidation_group_hk` —
  for any consolidation group, at most one parent group edge open at a time.
* **`esat_legal_entity_consolidation`** drives on `legal_entity_hk` — for any
  company, at most one consolidation-group membership open at a time. This is the
  invariant that makes "which group did this company roll into in March"
  answerable, and it is the one Workday's current-state retrieval cannot
  reconstruct backwards.

**`Last_Updated_DateTime` is not an effective date.** It is present on every
organisation and it says a row changed; it does not say when the change took
legal effect, and `Get_Organizations` has no as-of parameter in the criteria we
send. So the first load establishes today's structure, and effectivity accrues
from subsequent loads — history before the first load is not recoverable from
this source. This is a limitation to record, not a defect to fix: a roster's job
is to be right now and to keep its changes from then on.

## What validation must refuse

Checked against the accelerator's refusal list rather than by resemblance to the
existing entities:

* **No descriptive attributes on either new hub.** A consolidation group's name
  goes on a satellite, not on `hub_consolidation_group`.
* **`hal_consolidation_hierarchy` needs `parent_roles`.** Both legs point at the
  same hub; without roles the two collapse to one foreign-key column and every
  node becomes its own parent — which loaded silently once already, on 5
  September, and is why `spec.validate` now refuses it.
* **`lnk_legal_entity_consolidation` must not carry payload.** Which companies
  were in a group and when is the effectivity satellite's job.
* **Neither new hub may reuse a reserved prefix** (`ctl_`, `ref_`, `reg_`,
  `agg_`).
* **No entity pinned to a superseded hash rulebook version.**

## Testing

**Structural**, in `tests/test_accelerator.py`: the two hierarchies are separate
tables; `hal_consolidation_hierarchy` declares roles **in the right order** (that roles
exist is `spec.validate`'s job and cannot fail here; the order is unchecked
anywhere else and inverting it reverses every rollup); `hub_consolidation_group`
is federated; the client entities are renamed and still placeholder-bound.

**No test asserts the new hub and link carry no payload.** `spec.validate`
refuses both at model load, so such a check could only ever be green, and this
repo treats an unfailable check as worse than none because it reads as coverage.
The guarantee is real and lives in `spec.validate`.

**Behavioural**, the gates that apply: `hash_parity_check` before any load;
`append_only_check`, since a restructure must never appear as a mutation;
`loop1_reconciliation`, so that landed + quarantined = approved across the
Workday retrieval.

**The one that would actually catch a mistake here** is a check that the two
hierarchies share no edge — that no (parent, child) pair appears in both the
consolidation hierarchy and the client hierarchy. That is the failure the whole
separation exists to prevent, and it is invisible to every other gate, because
both tables would be individually valid.

**Mutation-proving is required**, per this repo's standing discipline: each new
check must be shown to fail against a deliberately broken input before it is
believed. The separation check in particular must be proven against a fixture
that genuinely has an edge in both tables, or it asserts nothing.

## Out of scope

* **Resolving a `buyer` string to a roster row.** That is business-vault same-as
  work, described in `legal_entity_design.md`, and unchanged by this.
* **LEG-1**, whether the legal entity code stays HFIG-assigned or becomes
  (jurisdiction, company number). Undecided, and this design works either way.
* **Loading client structure.** No source exists, and the feed carries a supplier
  dimension where a client-subsidiary one would go. Acquiring that mapping —
  which buyer codes belong to which client group — is the prerequisite for the
  spend reporting this hierarchy exists to serve, and it is a business input.
* **The gold spend projection itself.** This design gives it a hierarchy to
  traverse and an effectivity satellite to traverse it through; which of as-was
  and as-is it serves is **decided** above — as-is by default, as-was reachable
  through the effectivity satellite. An earlier draft of this line called that a
  recommendation awaiting a decision, which contradicted the decision recorded in
  the same commit; the binding authority may not give two answers.
* **Promoting anything from the implementation tenant.** WIDs from
  `impl-services1` identify nothing in production.
* **Rate cards.** Ruled out by Amy on 24 September: *"I don't know that we need to
  capture the rate card information, we're not re-calculating or validating rates,
  this is managed upstream in the VMS."* She had not misunderstood the ask; this
  was a candidate scope and it is now closed.
* **The Workday client contract ID.** Amy describes a new business process Fred is
  introducing — a deal in Hubspot, a quote through CPQ, an accepted quote becoming
  a client contract keyed into Workday — with two requirements: the Workday client
  contract id must be **held on the client record** in Databricks, and the
  **invoice posting to Workday must carry it**. That is a new source system and a
  change to the invoicing pipeline, so it gets its own spec rather than being
  designed inside this one. Recorded here so it is not lost.

## Success criteria

* The two hierarchies are separate tables and no edge appears in both.
* The 82 Companies and 36 consolidation groups load from Workday, with the
  consolidation tree reconstructable end to end.
* `Impellam_UK_Limited_Consolidated` is never returned by a query for legal
  entities.
* The client hierarchy is a declared shape that loads nothing, and says so.
* Every gate in "Testing" is green, and every new check is mutation-proven.
