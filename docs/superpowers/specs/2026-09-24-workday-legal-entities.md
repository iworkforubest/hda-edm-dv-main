# Can Workday give us the legal entities, their hierarchy, and their changes?

Measured against `headfirst3` on `impl-services1.wd502.myworkday.com` (v46.2) on
24 September 2026, with the `ISU_Databricks` integration user we already hold.

**Short answer: yes, all three, and the access already works.** The retrieval is
`Get_Organizations` in the **Human_Resources** service — not `Get_References`,
which is what we had been calling and which cannot express a relationship.

## What the tenant holds

1,528 organisations across 16 pages, of which:

| type | count | carries members |
|---|---|---|
| `Cost_Center` | 666 | 0 |
| `Supervisory` | 470 | 0 |
| `Function` | 103 | 0 |
| `Region` | 99 | 0 |
| **`Company`** | **82** | **0** |
| **`Company_Hierarchy`** | **36** | **35** |
| `Region_Hierarchy` | 15 | 11 |
| `Cost_Center_Hierarchy` | 14 | 12 |
| `Pay_Group` | 10 | 0 |
| others | 33 | 7 |

**The 82 `Company` rows are the legal entities.** They match the 82
`Company_Reference_ID` values the earlier `Get_References` census returned — but
that census returned ids and descriptors only, which is why the hierarchy looked
unavailable.

## The hierarchy is real, and it is not where you would first look

**A `Company` has no parent.** Measured: 0 of 82 carry
`Superior_Organization_Reference`. Reading the tree from `superior` alone reports
a flat estate of 82 unrelated companies, which is wrong and fails silently.

**The tree is held by the 36 `Company_Hierarchy` nodes.** They point at each
other through `superior`, and they name their member companies through
`Included_Organization_Reference`. Each `Company` carries the inverse,
`Included_In_Organization_Reference`, on 80 of 82.

So the estate is a two-layer graph, and both layers are needed:

```
All_Headfirst_Global_Plc_Consolidated          (Company_Hierarchy, root)
└── Impellam_Group_Limited_Consolidated        (Company_Hierarchy)
    └── Impellam_UK_Limited_Consolidated       (Company_Hierarchy)
        ├── subordinate: Carbon60_Limited_Consolidated
        ├── subordinate: Science_Recruitment_Group_Limited_Consolidated
        └── includes: IE100, UK013, UK103, IE900        (Company — legal entities)
```

This is exactly the shape `hal_legal_entity_hierarchy` and
`esat_legal_entity_hierarchy` were designed for, and `landing_rows("membership",
…)` already explodes both directions into one row per edge.

## Changes are trackable

Every organisation carries `Last_Updated_DateTime`, populated on 100 of 100
sampled, with values spread across 2026-04 to 2026-09 — so it is a real modified
timestamp and not a load stamp. `Inactive` is carried too. Between them, a
satellite can detect change and an effectivity satellite can close an edge.

**Two caveats.** `Last_Updated_DateTime` is a *last* modified time, not an
effective date, so it says a row changed and not when the change took legal
effect. And `Get_Organizations` has no as-of parameter in the request criteria we
use, so this is a current-state retrieval: history before today's call is not
recoverable from it. Effective-dated history would need a different retrieval and
is not needed for a roster.

## Three parser defects, found by running it

The code to do all of this already existed and **had never been run against the
tenant**. Running it surfaced three defects, all of the same shape — a value read
from the wrong place, returning `None` or `False` without raising:

1. **`type` was read from a `wd:Descriptor` attribute.** The tenant does not send
   that attribute at all; the value is a child
   `<wd:ID wd:type="Organization_Type_ID">Company</wd:ID>`. So `type` was `None`
   for all 82 Companies — meaning nothing could be filtered to Company, and
   Company is the legal entity. **This is the same mistake as `descriptor` vs
   `referenced_object_descriptor` on `Get_References`, made again on a different
   service.**
2. **`Inactive` was compared to the string `"true"`.** Workday web services send
   `0` and `1`. So `inactive` was `False` for every organisation in the tenant,
   and a genuinely inactive legal entity would have loaded as a live one.
3. **`subtype` was not captured at all.** It is not redundant: a `Company` has
   subtype `Company`, but a `Supervisory` org has subtype `Department`.

**The fixture was why none of this was caught.** It was composed from the schema
rather than captured from the wire, and it invented both the `Descriptor`
attribute and `<wd:Inactive>false</wd:Inactive>`. The fixture and the parser
agreed with each other and disagreed with Workday, and the tests passed
throughout. It has been replaced with three captured records.

**And one test-harness defect.** Mutating the `type` bug back in **aborted the
whole suite** with `IndexError` instead of failing checks red, because a check
selected its record with `[0]` on a list filtered by the field under test. An
f-string detail argument then raised `KeyError` even when its condition was
already `False`, since arguments evaluate eagerly. Both are now defensive, and
the mutation reports three red checks.

## What this unblocks, and what it does not

**It unblocks the roster's structure.** `hub_legal_entity` was a placeholder
because "the list is a business input, not a query". For the *group's own*
companies that is now only half true: Workday holds 82 of them with codes, names,
active flags, modified timestamps and a full consolidation hierarchy, and we can
retrieve them today.

**It does not settle the client legal entities.** The roster also needs the
client companies the VMS feeds name — Ameren among them — and those are not in
our Workday. Nor does it resolve a `buyer` string to a roster row; that is the
same-as work `legal_entity_design.md` describes.

**It is the IMPLEMENTATION tenant.** WIDs from it identify nothing in production,
and the tool prints that warning on every run. Nothing retrieved here may be
promoted as reference data.

## Incidentally: it answers Amy's spelling question

Workday's US company `US100` is named **`Guidant Global Inc`** — no comma, no
trailing period, exactly as Amy wrote it. This repo carried `Guidant Global, Inc.`
because that is the string the Delphi Fieldglass feed delivers in `buyer`.

The comma form is not merely a typo, which is the interesting part: `US104` is
**`Guidant Group, Inc`** — a different company, with a comma. Two similar names,
two real entities, and a spelling that silently picks the wrong one is precisely
the failure `legal_entity_design.md` warns about.

The tenant also confirms Guidant Global is many legal entities, not two:
`US100` Inc, `UK103` UK Limited, `COM-Guid11` Germany GmbH, `PR100` Puerto Rico
Inc, `SG100` Singapore, `BE900` Belgium NV, `MX100` Mexico, `IN100` India,
`US801` Funding LLC, `US901` Holding Corporation, and more.
