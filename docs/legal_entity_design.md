# Solving `buyer`: a legal-entity hub, a hierarchical link, and what stays unbuilt

**5 September 2026**, amended 24 September. The design behind `hub_legal_entity`,
`hal_client_legal_entity_hierarchy` and `esat_client_legal_entity_hierarchy` — renamed on
24 September, when the group's own structure became a separate family
(`hub_consolidation_group`, `hal_consolidation_hierarchy`,
`lnk_legal_entity_consolidation`); see
`docs/superpowers/specs/2026-09-24-legal-entity-hierarchies-design.md`. The DV 2.0
constructs they use, and the defect found
while building them.

## What `buyer` actually is

Every VMS feed carries an account-holder name in a column called `buyer`. Measured across
the estate, that one column holds three different kinds of thing:

| kind | example | what it is |
|---|---|---|
| the client, current name | `Albertsons Companies, Inc` | what everyone assumes `buyer` means |
| the client, **superseded** name | `commonspirit_health` → `Dignity Health` | the pre-merger name, still being delivered |
| **one of our own companies** | `delphi_tech` → `Guidant Global, Inc.` | Guidant runs Delphi's programme, so Guidant holds the account |

The third is not an anomaly and not a data error. Where an Impellam Group or HeadFirst
Group company operates a client's contingent-workforce programme, that company holds the
VMS account — so the account holder is us. `delphi_tech` also carries `BorgWarner`, who
acquired Delphi Technologies. **One feed, three identities, none of them stable.**

Two distinct problems live in that column, and conflating them is how you build the wrong
fix:

1. **Same entity, many spellings.** Beeline is where this is worst — every one of its
   twelve client feeds carries both a code and a display name, and sometimes a third
   variant: `KC`, `Kimberly-Clark`, and `Kimberly-Clark Corp (Consolidated)` are one
   company. `Dignity Health` and CommonSpirit are one company across a rename.
2. **A different entity entirely.** `Guidant Global, Inc.` is not a spelling of `Delphi
   Technologies`. It is a real, separate legal entity standing in a real relationship to
   it.

The first needs identity resolution. The second needs a relationship model. They are not
the same construct and this design keeps them apart.

## Why none of this happens at load time

The raw vault records what the source sent. A feed that said `Guidant Global, Inc.` is a
feed that said that, and rewriting it on the way in would destroy the evidence and put a
business rule inside a storage layer.

That is not a stylistic preference here. Load everything from Bronze into Silver
faithfully; filter and interpret in Gold for the use case at hand. So `buyer` stays as
delivered in `hub_job_request`, and resolution sits above it:

```
Bronze   io_distributed_jobposting_delphi_tech   buyer = 'Guidant Global, Inc.'
   ↓     (verbatim)
Silver   hub_job_request                          tenant = 'Guidant Global, Inc.'
         hub_legal_entity                         conformed roster, authored keys
         hal_client_legal_entity_hierarchy        who owns whom (CLIENTS)
         esat_client_legal_entity_hierarchy       when each client edge was true
         hal_consolidation_hierarchy              the group's own tree (OURS)
         lnk_legal_entity_consolidation           which company sits in which group
   ↓     (join, per use case)
Gold     "job postings by client"  |  "job postings by operating company"
```

Both Gold questions are legitimate and they have different answers. A raw vault that
picked one at load time could only ever answer that one.

## The DV 2.0 constructs, and which does what

Three standard constructs cover this, and the codebase already claimed to support two of
them without ever having built one.

**Hierarchical link (HAL)** — a link joining a hub to *itself* to express parent/child.
This is the DV 2.0 construct for org charts, account rollups and bills of material. It is
what carries "Guidant Global is a child of Impellam Group".

**Same-as link (SAL)** — also a hub joined to itself, but declaring that two business keys
denote the *same* real-world thing. This is the construct for problem 1 above: `KC` and
`Kimberly-Clark` resolve to one legal entity. Structurally a SAL is a HAL with different
role names (`observed` / `canonical` rather than `parent` / `child`), so the mechanism
built here covers it — but **no same-as link is built yet**, because it needs the mapping
and the mapping does not exist.

**Effectivity satellite (ESAT)** — a driving key plus effective dates, for a relationship
that changes over time. It is what makes a hierarchy answerable as of a date rather than
only in the present tense.

**Why not a `parent_code` column on the hub.** It would be a descriptive attribute on a
hub, which this model refuses to build — and for a concrete reason here. A hub is
immutable; ownership is not. Put the parent on the hub and the day the brand unifies you
either UPDATE a hub row in an insert-only vault, or re-key it and orphan every satellite
behind it. As a link, a change of ownership is a new row.

## The defect this uncovered

`hal` has been a supported kind in `spec.py`, `naming.py` and `factory.py` since the model
was written. No entity had ever used one. Building the first, before adding any guard:

```
parents      ('legal_entity', 'legal_entity')          accepted -- two parents
parent_keys  (('legal_entity', ('child_code',)),)      COLLAPSED -- the parent leg is gone
FK columns   ['legal_entity_hk', 'legal_entity_hk']    one column, not two
link key     hash(['child_code', 'child_code'])        the child, hashed twice
```

**It loaded.** No error, no failing gate. It would have produced a hierarchy in which
every node is its own parent — a table whose every row joins, whose counts look sane, and
whose every rollup is wrong.

The cause is that `parent_keys` is a mapping keyed by hub name, so a hub appearing twice
resolves both legs to one entry. That single mistake propagated to **seven** places, each
independently reading a leg as a hub: the key derivation, the column projection, the
quality rules, the contract's foreign-key map, the ontology's object properties, three
separate scope checks that recover a hub name by stripping `_hk` from a column, and the
Spark suite's own parent matcher — which found itself only in CI, because it is the one
site the offline gates cannot execute.

### The fix: `parent_roles`

A link whose parents are not all distinct must name each leg:

```yaml
parents: [legal_entity, legal_entity]
parent_roles: [parent, child]
sources:
  - name: CLIENT_PORTAL
    parent_keys:
      parent: [parent_legal_entity_code]     # keyed by ROLE, not by hub
      child:  [child_legal_entity_code]
```

which derives:

```
parent_legal_entity_hk     <- ['parent_legal_entity_code']   scope=None
child_legal_entity_hk      <- ['child_legal_entity_code']    scope=None
legal_entity_hierarchy_hk  <- ['parent_legal_entity_code', 'child_legal_entity_code']
```

Roles are mandatory the moment a hub repeats, both legs always carry one (an implicit "the
unprefixed column is the child" is a convention a reader gets backwards, and a hierarchy
read backwards inverts every rollup while every row still joins), and a role must be a
plain identifier because it reaches a generated column name.

**Mutation-proven five ways**, each rejected: no roles at all; fewer roles than parents;
two legs sharing a role; a role that is not a safe identifier; `parent_keys` still mapped
by hub name. The restored model loads.

## The rebrand, walked through

Impellam Group and HeadFirst Group unify under a new brand in the coming weeks. In this
shape that is:

1. one new row in `hub_legal_entity` for the new entity;
2. two new rows in `hal_consolidation_hierarchy` — new → Impellam, new → HeadFirst;
3. two new rows in `esat_consolidation_hierarchy` opening those edges.

   (Renamed 24 September: this worked example is the GROUP's own restructure, so it
   belongs to the consolidation family, not the client hierarchy.)

**No key changes. No reload. Nothing is updated.** Guidant Global's edge to Impellam is
untouched — it is still a child of Impellam, which is now itself a child of the new brand.
The rollup gets one level deeper. Every fact recorded before the change still rolls up the
way it did when it was recorded, and every fact after rolls up the new way, because the
effectivity satellite dates both.

Compare the alternative the current model would have forced: `buyer` is hashed into
`hub_job_request`'s tenant key, so a rename re-keys the hub and strands every satellite row
behind the old key. That failure is the reason this work is happening now rather than after
the rebrand.

**The driving key is the child**, and it is the one real modelling decision in the
effectivity satellite. A company has one owner at a time and can change owner; an owner has
many companies. So for a given child at most one parent edge is open, and a new edge closes
the child's existing one. Driving on the parent instead would close every *other* child of
that parent each time one child moved — silent, plausible, and catastrophic to any rollup.

## What is built, and what is not

**Built and gated:** the `parent_roles` mechanism and its five guards; the three entities;
role-awareness in key derivation, projection, quality rules, the data contract, the DBML
diagram, the ontology, three scope checks and the Spark parent matcher. 992 verification
checks and 137 Spark checks pass.

### A second disagreement, found only in Spark

`spec.validate` permits an effectivity satellite with no payload; `hashing.hashdiff_expr`
refuses one — *"a satellite must declare a non-empty payload to hashdiff"*. The offline
suites accepted a payload-free `esat` and the Spark suite raised on it: the two halves of
the model disagreeing about the same entity, and reachable only by actually staging it.

Resolved in the direction that is also correct rather than by relaxing the rulebook. A
satellite with nothing to hashdiff has no change to detect, so it could hold one row per
edge and could never record that an edge **ended** — which is the entire job of an
effectivity satellite. The payload is `relationship_status`, and end-dating becomes a
delivery rather than an update: the source re-sends the same pair with a status of ended
and a new `effective_from`, the hashdiff moves, and a second version lands.

### Since 5 September: the descriptive attributes have somewhere to go

`sat_legal_entity_details` was missing and is now built — registered name, company number,
jurisdiction and registered status, hanging off `hub_legal_entity`. Without it the roster's
descriptive columns would have been collected against a vault that could not receive them,
which is an omission rather than a decision.

**Attributes, not keys, and that is the whole argument in one line.** A company renames,
redomiciles, is struck off and restored; on a hub each of those is an UPDATE in an
insert-only vault or a re-key, and here each is a new version. Putting a registered name in
a key would rebuild the `buyer` defect one layer up, where it would be harder to see.

Its binding maps the parent through **`parent_keys`, not `key_columns`** — the gate
insisted, and it was right. The `key_columns` fallback scopes the parent foreign key by the
*satellite's* key style, which defaults to federated, so the FK hashed under `CLIENT_PORTAL`
while `hub_legal_entity`, being authored, hashes its own key under nothing: a well-formed
value that could never join, on all seven targets.

**Deliberately not built:**

- **The roster.** One row per company across Impellam Group and HeadFirst Group, plus the
  client legal entities the feeds name. This is a business input, not a query — a group's
  legal structure is our own fact and no third party can supply it. All three entities
  carry **placeholder** `bronze_table` values and are absent from `active_sources`, which
  is the same honest state `sat_job_request_details/FIELDGLASS_US` is in. They declare a
  shape and load nothing.
- **The same-as link** resolving observed `buyer` strings to a legal entity code. Needs the
  roster first, and needs a decision on the mapping's shape: it is not one-to-one. The
  `bayer` feed already maps to at least two GP customers, `BAYCL` and `BAYP`.
- **Rebinding `hub_job_request`.** Its tenant is still `buyer` as delivered, which is
  correct for a raw vault and stays that way. What changes when the resolution exists is
  what Gold joins through — not what silver stores.
- **A `sal` kind.** A same-as link is structurally a hierarchical link with different role
  names. Adding a kind that generates identical SQL would be vocabulary, not a control.
  Revisit only if the two need to be validated differently.

## The domain list, and what it is not

**Received 5 September:** 27 domains, as the closest thing to a roster that exists.
Seeded into `docs/legal_entity_roster_worksheet.csv`. Three things fall out of it
immediately.

### Domain ≠ brand ≠ legal entity

27 domains reduce to **18 apparent brands** — `lorien.co.uk` and `lorienglobal.com` are one
brand in two markets; `srgtalent.com`, `srgtalent.ie` and `srgsynergy.com` look like one;
`headfirst.global`, `headfirst.group` and `headfirst.nl` are one. And `ext.pro-unity.com`
is not a brand domain at all — it is a platform hostname.

18 brands then reduce to an **unknown** number of legal entities, and that gap is the
substance of the problem. A legal entity is a registered company with a number and a
jurisdiction; "Lorien" is a trading name. **The VMS works in trading names** — `buyer`
delivers `Guidant Global, Inc.` — **and finance works in legal entities**, which is what
`RM00101.custnmbr` records. That mismatch is exactly why `bayer` already maps to two GP
customers, and it is why the worksheet asks for company number and jurisdiction rather
than treating a brand as an entity.

So the list is a **brand roster**, and the hub needs a **legal-entity roster**. The
worksheet is the bridge, and only business and legal can fill the right-hand side.

### Two of these are also source systems, and one is missing

Cross-checked against the model's ten source systems:

| finding | detail |
|---|---|
| `pro-unity.com` / `ext.pro-unity.com` | **also `PROUNITY_EU`**, an active source binding in this vault |
| Striive | **`STRIIVE_EU`**, an active source system — confirmed 5 Sep as a HeadFirst platform, and added to the roster. Its domain was not in the supplied list |

### A platform is not a legal entity

Confirming Striive answered the question and then raised a better one. **Striive is a
platform, not a brand** — and so, on the same reading, is ProUnity. A platform is a *system
a legal entity operates*, and `hub_legal_entity` takes rows of the second kind only.

That distinction was not something the worksheet could hold, so it now carries a
`kind__CHECK` column: `group_parent` (2), `brand` (23), `platform` (3 — ProUnity's two
domains and `striive.com`). Comensura was provisionally marked `platform?` and **corrected
5 September: it is a company, trading in the UK and Australia.**

### One brand is not one legal entity either, and Comensura proves it

Comensura trading in two jurisdictions is very probably **two registered companies** — a UK
one and an Australian one — under one brand. That is the first concrete instance of the
brand-to-entity fan-out this design has been asserting in the abstract, and it settles the
shape of the roster: the hub is keyed per legal entity, so one brand can own several rows,
and the hierarchy is what ties them together.

It also means the roster is **larger than 19 brands**, by an amount nobody has counted yet.

### The worksheet was keyed by the wrong thing

**Confirmed 5 September: SRG and Lorien each trade in the UK and Ireland**, as Comensura
does in the UK and Australia. Trying to record that broke the worksheet, and the way it
broke is the useful part.

Lorien's domains are `lorien.co.uk` and `lorienglobal.com`. **There is no `.ie` domain**,
so in a sheet with one row per domain there was nowhere to put Lorien's Irish entity. The
same shape had already appeared once, when Striive turned out to have no domain in the
list at all. Twice is a pattern: **the domain list is evidence about entities, not a list
of them.**

So the worksheet is now keyed by **(brand, jurisdiction)** — an *entity candidate* — with
domains demoted to attributes. 28 domains and 19 brands become **22 entity-candidate
rows**, and the two columns are split on purpose:

- `country_domains` — ccTLD domains, which evidence a presence in that country;
- `brand_domains` — `.com` and friends, which evidence the brand and say nothing about
  where it trades.

Folding those into one column would have listed `lorienglobal.com` against Lorien's Irish
row and hidden the very gap the pivot exists to expose. Kept apart, the gap is countable:

| | |
|---|---:|
| entity candidates | **24** |
| jurisdiction CONFIRMED by the business | 18 |
| jurisdiction inferred from a ccTLD | 6 — every `.nl` brand, and consistent with the group rule |
| **jurisdiction still unknown** | **0** |
| **known jurisdiction with NO domain evidence** | **4** — `lorien/IE`, `srg/GB`, `guidant_global/GB`, `guidant_global/US` |

**Every row now has a jurisdiction**, closed on 5 September from a starting point of nine
unknown. The six that remain inferred are the `.nl` brands, where the ccTLD and the group
rule agree; they are cheap to confirm and nothing depends on them being wrong.

Those four are the point. All are real entities we have been told about, none appears in
the domain list, so **the roster cannot be finished from domains** — it needs a
company-registry answer per row. `guidant_global` doubled that count on its own: confirmed
5 September as **UK and US**, from a single `.com`.

### Group membership is settled, and there is now a rule

`irishrecruitment` was confirmed Impellam (UK and Ireland) on 5 September, and with it
**every brand's group is known** — zero unplaced, from three at the start of the day.

More usefully, the business stated a *rule*: **Impellam is everything outside the EU.**
That is worth more than any single answer, because it can classify a brand nobody has asked
about yet. So it was tested against all 24 rows rather than simply adopted:

| group | jurisdictions present |
|---|---|
| HeadFirst | `NL` |
| Impellam | `AU`, `GB`, `IE`, `US` |

**The rule does not hold literally, and the exception is consistent.** Three Impellam rows
sit inside the EU — `lorien/IE`, `srg/IE`, `irishrecruitment/IE` — and all three are
Ireland, all three confirmed by the business itself. No HeadFirst row sits outside Benelux.

With 23 of 24 rows now carrying a confirmed jurisdiction, the two groups partition cleanly:

| group | jurisdictions |
|---|---|
| HeadFirst | `BE`, `NL` |
| Impellam | `AU`, `GB`, `IE`, `US` |

**Belgium is the useful arrival here, and Ireland is the decisive one.** Belgium is both EU
and Benelux, so `prounity/BE` corroborates either formulation and discriminates between
neither. Ireland is EU and *not* Benelux, and it is Impellam three times over — so Ireland
alone settles it. Stated as "outside the EU", the rule would misfile any future Irish entity
as HeadFirst, and Ireland is where three Impellam brands already trade.

It also means "Benelux" is now evidenced rather than extrapolated from a single country:
until Belgium arrived, every HeadFirst row was `NL`, and calling that "Benelux" was a guess
about the shape of the boundary rather than an observation of it.

**A sharper formulation, offered rather than adopted:** the split is not really geographic.
`GB`, `IE`, `US` and `AU` are the English-speaking markets; `BE` and `NL` are the Dutch-
speaking ones. That reading fits all 23 rows and is easier to apply than either boundary —
but Carbon60, Lorien and SRG all claim "Europe" operations, so a single German or French
Impellam entity would break it. Worth confirming before it is used to classify anything.

Used as a check rather than a source, the rule **corroborates every inferred group value
that can be tested**: each `.nl` brand reads HeadFirst, each `GB`/`IE`/`US`/`AU` brand reads
Impellam.

`bartech` (US), `carbon60` (GB) and `impellam` itself (GB) were confirmed on 5 September,
leaving five rows with no jurisdiction: `barpellam`, `between`, `prounity`, `staffingms`,
`striive`.

### What the official group page adds, and the trap in reading it

`impellam.com/about-us/our-group` names **six** operating companies — Comensura, Carbon60,
Lorien, Guidant Global, Bartech, SRG — with the footprint each claims:

| brand | stated footprint |
|---|---|
| Guidant Global | **80+ countries**, 200,000+ engagements |
| Lorien | UK, Europe and North America |
| SRG | UK, Europe and North America |
| Carbon60 | UK & Europe |
| Bartech | North America |
| Comensura | no country stated |

**None of this expands the roster, and reading it as though it did would be the mistake
this whole design exists to avoid.** Operating footprint is not legal-entity count. Guidant
is active in 80+ countries and certainly does not hold 80+ registered companies; it holds
some smaller number and trades everywhere else through them. The roster is entities, so it
takes the confirmed answer — Guidant is **UK and US** — and treats the page as context.

The worksheet records both, in separate columns, so the two can never be confused:
`on_official_group_page` and `operating_footprint_stated`.

**Two things the page does tell us.**

**Four Impellam-side brands are not on it:** `barpellam`, `irishrecruitment`, `staffingms`,
and `impellam` itself. The last is the parent, so its absence is expected. The other three
are confirmed Impellam by the business but are not operating companies — which suggests
they are legal entities, dormant names, or sub-brands rather than divisions, and that is
exactly the distinction the roster needs from them.

**And it puts a question against the Benelux rule.** Carbon60, Lorien and SRG all state
they operate in "Europe". If any of them holds a *Dutch* entity, an Impellam brand sits in
Benelux and the rule that currently fits all 24 rows stops fitting. Operating in Europe
does not imply a Dutch company — but it is the one thing that could break the rule, so it
should be asked rather than assumed.

### Guidant Global is where this design pays for itself

It is the entity that started the whole exercise — `delphi_tech`'s Fieldglass feed delivers
`buyer = "Guidant Global, Inc."` — and it is now confirmed to trade in **two**
jurisdictions, so it is at least two legal entities.

**`Inc.` is a US corporate form.** So the account holder on that feed is very probably
Guidant Global's *US* entity, not its UK one — which means resolving that `buyer` string
correctly requires picking between two rows of our own roster, not merely recognising the
name. A same-as link that resolved it to "Guidant Global" as a single thing would be wrong
in a way no gate could see, because both candidate entities are real and both are ours.

That is recorded as a hypothesis on the row, to confirm — not as a fact. But it is the
clearest illustration of why `buyer` was never a cleaning problem.

**Corroborated, not confirmed, 24 September 2026.** Amy Keser states that Ameren's billing
entity is **Guidant Global Inc** — the `Inc.` form, for a US client. That is a second,
independent sighting of a US Guidant entity being the one that trades with US clients, and
it is the entity Ameren invoicing will name. It does **not** settle the row above: she
answered which entity bills Ameren, not which of the two entities the `delphi_tech` feed's
`buyer` string denotes. The hypothesis stands, with one more reason to believe it.

**And it surfaces a spelling to settle.** This document writes `Guidant Global, Inc.`
because that is the string the Delphi feed delivers. Amy wrote `Guidant Global Inc`. A feed
carrying a company's name is evidence of what that feed sends, not of how the company is
registered — which is the whole argument for the roster being a business input rather than a
query. The roster must carry the registered spelling, and neither of these two is yet known
to be it.

This matters concretely, because it is the same shape as the `buyer` problem one level up.
`Guidant Global, Inc.` arrives in a VMS feed as an account holder; `STRIIVE_EU` and
`PROUNITY_EU` arrive as *record sources*. Neither is automatically a legal entity, and
resolving both to one requires knowing which entity stands behind the name. **For every
platform row the roster question is not "what is its legal entity name" but "which legal
entity operates it".** The worksheet's `notes__FILL` says so on each.

(`BULLHORN_EU`, `FIELDGLASS_US`, `UKG_US` and `GP_US` are third-party systems and their
absence from the list is correct.)

### Three domains I could not place

`barpellam.com`, `irishrecruitment.ie` and `staffingms.com` are marked `UNKNOWN` in the
worksheet rather than guessed. The remaining 24 carry a `likely_group__CHECK` value —
split 12 Impellam, 12 HeadFirst — which is **inference from brand knowledge, not
measurement**, and is named that way so nobody reads it as fact. Every one of them needs
confirming before it becomes a hierarchy edge.

## Can a legal entity play more than one role?

**Answered 5 September: yes in general, unknown in this case — a question for business and
legal.** So the design must not foreclose it, and today it does not: the hub holds the
entity, and every relationship lives on a link.

What that leaves open is how a *second* relationship type is added when one is needed —
"operates the programme for", alongside "is owned by". Two options:

- **A `relationship_type` in the hierarchy's key.** Cheap, and wrong for the same reason
  most cheap options here are wrong: every traversal then has to filter by type, and a
  traversal that forgets rolls ownership and operating relationships into one answer.
  Silent, and plausible.
- **A separate link per relationship type.** More tables, and each one traversable without
  a filter that can be forgotten.

**Recommended: the separate link**, on the same reasoning that keeps `buyer` unresolved in
the raw vault — make the wrong query impossible to write rather than merely incorrect. Not
built, because which second relationship exists is precisely the question that is open.

## Where the roster stands, and what only a registry can answer

The derivable work is finished. 24 entity candidates, every one with a group and a
jurisdiction, and a rule that classifies future brands. **None of it is a hub row yet**,
because every column that identifies an actual company is still empty:

| column | filled, of 24 |
|---|---:|
| `legal_entity_name__FILL` | 0 |
| `company_number__FILL` | 0 |
| `parent_legal_entity__FILL` | 0 |
| `legal_entity_code__FILL` | 0 |

That is not a gap in the work; it is the boundary of what this side can produce. A brand and
a jurisdiction narrow the search to one registry and one company; they do not name it. Until
those four columns are filled, `hub_legal_entity` has nothing to load and the three entities
stay on placeholder bindings.

**Three rows deserve attention when that happens.** `barpellam` (GB), `irishrecruitment`
(IE) and `staffingms` (US) are confirmed Impellam but appear nowhere on the group's own
operating-company page. They are the rows most likely to be dormant names, holding
companies, or entities that trade under another brand — and each of those is a different
answer for the hierarchy, not merely a different label.

## The open questions, in the order they block

**These are now asked formally**, with the reasoning and the consequence of each, in
[`legal_entity_questions.html`](legal_entity_questions.html) — the page that goes to
business and legal, a sibling of `bronze_layer_work_requests.html`. Eight questions,
`LEG-1` through `LEG-8`; `LEG-1` (what identifies an entity) and `LEG-7` (who owns the
register) block the rest. The summary below stays here as the modelling view of the same
list; the page is what gets sent.

1. **The roster.** Who maintains it, and where does it live so the vault can read it?
2. **The code.** `legal_entity_code` is authored — we assign it. Does an HFIG client code
   already exist that this should reuse? `docs/client_mapping_worksheet.csv` has 65 rows
   and an `hfig_client_code__FILL` column that is empty in all of them.
3. ~~**Does an entity play more than one role?**~~ **Answered 5 Sep: yes in general,
   unknown here — business and legal own it.** What remains is which second relationship
   exists, and that decides whether a second link gets built. See above.
4. **The finance tie-back.** `RM00101` (`custnmbr`, company-scoped) is the GP customer
   master. Mapping a legal entity to a set of finance customers is a modelling decision
   nobody has made — see `OPEN_ITEMS.md`, 26 August.
