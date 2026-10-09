# Legal Entity Hierarchies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate the group's own legal-entity hierarchy from clients', modelling Workday's two-layer consolidation structure as Workday expresses it.

**Architecture:** One conformed roster (`hub_legal_entity`, unchanged) gains a second hub for Workday's 36 `Company_Hierarchy` consolidation groups, which are accounting constructs rather than companies. The group tree is a hierarchical link between consolidation groups; company membership is an ordinary link between the two hubs. The existing hierarchy entities are renamed to say they are the client side, which they already were in intent.

**Tech Stack:** Declarative entity YAML under `metadata/entities/`, validated by `src/accelerator/spec.py`; pure-Python gates in `tests/test_accelerator.py` and `verify_repo.py`; generated artefacts byte-gated.

**Spec:** `docs/superpowers/specs/2026-09-24-legal-entity-hierarchies-design.md`

## Global Constraints

- **Nothing loads. Every new binding names `PLACEHOLDER.reference.*`** and no new entity joins `active_sources`. Landing Workday data is Task 3 of `docs/superpowers/plans/2026-09-07-workday-reference-source.md`, which is **GATED on a destination nobody owns**, and Task 4 there is gated additionally on a **production** host — "Do not start against `impl-`". This plan produces declared shapes only.
- **`kind: link`, not `lnk`.** The file is named `lnk_*.yml`; the `kind` value is `link`. See `metadata/entities/lnk_client_job_request.yml`.
- **`domain: reference`** for every new entity. `verify_repo` asserts every domain in the model has an architecture document under `diagram/`; a new domain would need a new Archify document and is not wanted here.
- **A hal needs `parent_roles`.** Both legs point at one hub, and without roles they collapse to a single FK column — measured 5 September: such an entity loaded and produced a hierarchy in which every node was its own parent, with no error.
- **A link may not carry payload.** `spec.validate` refuses `kind: link` with a payload.
- **Every satellite binding needs `applied_dts_column`** unless it reads a vault table (DEF-55).
- **Every payload column needs a `descriptions:` entry.** `verify_repo` gates that descriptions reach the DBML diagram and the ontology.
- **Regenerate artefacts and commit them.** `tools/emit_source_to_target.py`, `tools/emit_dbml_diagram.py`, `tools/emit_ontology.py`, `tools/emit_data_contract.py` are byte-gated; a model change without regeneration fails `verify_repo`.
- **Never `git checkout <file>` to undo.** Copy aside and copy back.
- **Every new check must be mutation-proven** before it is believed, and a mutation must produce a red check rather than an aborted suite.

## Review Focus

1. **The rename silently narrows an existing gate.** `verify_repo.py:1701` sweeps `entity.name.startswith("legal_entity")` to assert no legal-entity binding has left `PLACEHOLDER`. `client_legal_entity_hierarchy` does not start with `legal_entity`, so the rename drops two entities out of the sweep and the gate goes on passing. Task 4 fixes the gate in the same commit as the rename.
2. **`_WD_PLANNED` keys on the old entity name.** `verify_repo.py:1666` holds `"legal_entity_hierarchy"` as a dict key. After the rename nothing references that key and the planned-column check compares against an entity that no longer exists.
3. **An edge present in both hierarchies.** Both tables would be individually valid and every existing gate stays green while the rollup double-counts. Nothing in the repo would notice. Task 5.
4. **As-was traversal stops being exercised.** Defaulting spend reporting to as-is is exactly the moment the effectivity satellite stops being queried and can rot unnoticed. Task 5 pins a point-in-time traversal against a structure that changes mid-history.
5. **A client with no parent drops out of spend reporting.** Amy, 24 September: parent/child is *"not applicable to all (Ameren as an example)"*. A projection grouping by parent id silently omits every single-entity client — Ameren's whole 1,176,896.90 absent from a client-group report, the query succeeding, the rows joining, the total simply smaller. A client with no parent rolls up to itself. Task 7.
6. **A consolidation group returned by a query for legal entities.** `Impellam_UK_Limited_Consolidated` is not a company; if it reaches `hub_legal_entity` every rollup double-counts, once through the company and once through its group. Task 1.

---

### Task 1: `hub_consolidation_group`

**Files:**
- Create: `metadata/entities/hub_consolidation_group.yml`
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Produces: hub named `consolidation_group`, business key column `consolidation_group_reference`, hash key `consolidation_group_hk`. Tasks 2 and 3 name it as a parent.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accelerator.py`, after the existing `hub_legal_entity` checks:

```python
# WORKDAY'S CONSOLIDATION GROUPS ARE NOT COMPANIES. Measured 24 September: the tenant
# holds 82 organisations of type Company and 36 of type Company_Hierarchy, and the 36 are
# accounting constructs -- Impellam_UK_Limited_Consolidated is not a legal entity. Putting
# them in hub_legal_entity would mean every rollup double-counts, once through the company
# and once through the group it rolls into.
cgroup = model.get("consolidation_group")
check("consolidation groups are a hub of their own, not rows in the legal-entity roster",
      cgroup.kind == "hub" and cgroup.business_keys == ("consolidation_group_reference",),
      f"{cgroup.kind}/{cgroup.business_keys}")
check("the consolidation-group hub is FEDERATED -- these are Workday's objects and "
      "Workday assigns their identity, unlike a legal entity, which is our own fact",
      cgroup.key_style == "federated",
      f"{cgroup.key_style} -- authored would claim HFIG assigns these codes")
check("the consolidation-group hub carries no descriptive attributes",
      not cgroup.payload,
      f"{cgroup.payload} -- a group's name belongs on a satellite; a hub is immutable")
check("the consolidation-group hub loads nothing yet",
      all("PLACEHOLDER" in s.bronze_table for s in cgroup.sources),
      f"{[s.bronze_table for s in cgroup.sources]} -- landing is GATED on a destination "
      f"nobody owns and on a production tenant; impl- WIDs identify nothing")
# REVIEW FOCUS 6, from the other side. The check above says consolidation groups have
# their own hub; this says the ROSTER does not also claim them. The two hubs must key on
# different columns, or a loader could put a Company_Hierarchy row into hub_legal_entity
# and every rollup would count it twice.
legal = model.get("legal_entity")
check("the roster and the consolidation groups key on different columns, so a group "
      "cannot be loaded as a legal entity",
      set(legal.business_keys).isdisjoint(cgroup.business_keys),
      f"{legal.business_keys} vs {cgroup.business_keys} -- Impellam_UK_Limited_Consolidated "
      f"must never be returned by a query for legal entities")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: an error from `model.get("consolidation_group")` because the entity does not exist.

- [ ] **Step 3: Create the entity**

Create `metadata/entities/hub_consolidation_group.yml`:

```yaml
# WORKDAY'S CONSOLIDATION GROUPS, AND THEY ARE NOT LEGAL ENTITIES.
#
# Measured against tenant headfirst3 on 24 September 2026: 1,528 organisations, of which
# 82 are type Company and 36 are type Company_Hierarchy. The 82 are the legal entities and
# live in hub_legal_entity. The 36 are ACCOUNTING CONSTRUCTS -- financial consolidation
# rollups -- and Impellam_UK_Limited_Consolidated is not a company anyone could invoice.
#
# WHY THEY GET THEIR OWN HUB. hub_legal_entity's stated contents are "every company in
# Impellam Group and HeadFirst Group, and every client legal entity a VMS feed names".
# Adding 36 things that are not companies to that roster means any rollup which forgot to
# exclude them double-counts -- once through the company, once through the group it rolls
# into -- and nothing would raise.
#
# WHY FEDERATED, WHERE hub_legal_entity IS AUTHORED. That is the opposite choice and it is
# deliberate. A legal entity is OUR fact about the world, so we assign its code and one
# company is one row whatever feed observed it. A consolidation group is WORKDAY'S fact
# about its own configuration: its identity is Workday's Organization_Reference_ID, HFIG
# does not assign it, and no second system has an opinion about it.
#
# THE HIERARCHY IS NOT HERE. Which group contains which is hal_consolidation_hierarchy;
# which companies belong to a group is lnk_legal_entity_consolidation. A parent column on
# this hub would be a descriptive attribute on a hub, and ownership changes while a hub
# must not.
name: consolidation_group
kind: hub
domain: reference
key_style: federated
business_keys: [consolidation_group_reference]
sensitivity: internal

notes: >
  Workday's Company_Hierarchy organisations -- the financial consolidation groups the
  group's legal entities roll up through. Not legal entities: see hub_legal_entity for
  those. Federated because Workday assigns the identity.

descriptions:
  consolidation_group_reference: >-
    Workday's Organization_Reference_ID for a Company_Hierarchy organisation, such as
    Impellam_UK_Limited_Consolidated. Federated on the source because Workday assigns it.

sources:
  # PLACEHOLDER. Landing the Workday retrieval is Task 3 of
  # docs/superpowers/plans/2026-09-07-workday-reference-source.md, which is GATED on a
  # destination nobody owns; Task 4 there is gated additionally on a PRODUCTION host,
  # because WIDs from impl-services1 identify nothing in production.
  - name: WORKDAY
    bronze_table: PLACEHOLDER.reference.wd_organization
    key_columns: [reference_id]
```

- [ ] **Step 4: Run the tests — expect PASS**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Mutation-prove each new check**

Copy the file aside first: `cp metadata/entities/hub_consolidation_group.yml /tmp/cg.bak`

| Mutation | Expected red check |
|---|---|
| `key_style: authored` | the consolidation-group hub is FEDERATED |
| add `payload: [name]` | carries no descriptive attributes |
| `bronze_table: 01_usnc_bronze_dev.wd.org` | loads nothing yet |

After each: `.venv/bin/python tests/test_accelerator.py`, confirm the named check FAILS and the suite still completes, then `cp /tmp/cg.bak metadata/entities/hub_consolidation_group.yml`.

- [ ] **Step 6: Commit**

```bash
git add metadata/entities/hub_consolidation_group.yml tests/test_accelerator.py
git commit -m "Give Workday's consolidation groups their own hub

They are not legal entities. 36 Company_Hierarchy organisations against 82
Companies, measured -- and Impellam_UK_Limited_Consolidated is an accounting
construct nobody could invoice. In hub_legal_entity they would make every
rollup double-count, once through the company and once through its group."
```

---

### Task 2: the group tree — `hal_consolidation_hierarchy` and its effectivity

**Files:**
- Create: `metadata/entities/hal_consolidation_hierarchy.yml`
- Create: `metadata/entities/esat_consolidation_hierarchy.yml`
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: hub `consolidation_group` from Task 1.
- Produces: hal named `consolidation_hierarchy` with roles `parent`/`child`, FK columns `parent_consolidation_group_hk` and `child_consolidation_group_hk`; esat named `consolidation_hierarchy_effectivity` driving on `child_consolidation_group_hk`.

- [ ] **Step 1: Write the failing test**

```python
# THE GROUP TREE. Measured: 0 of 82 Companies carry a superior; the tree is held entirely
# by the 36 Company_Hierarchy nodes pointing at each other through
# Superior_Organization_Reference. Reading parentage from the companies alone reports a
# flat estate of 82 unrelated firms, which is wrong and silent.
chier = model.get("consolidation_hierarchy")
check("the group tree is a hierarchical link between consolidation groups",
      chier.kind == "hal" and chier.parents == ("consolidation_group", "consolidation_group"),
      f"{chier.kind}/{chier.parents}")
# ROLES ARE MANDATORY. Measured 5 September, before parent_roles existed: a two-leg hal on
# one hub LOADED, emitted a single foreign-key column, and produced a hierarchy in which
# every node was its own parent -- with no error anywhere.
check("the group tree names its roles, so the two legs cannot collapse into one column",
      chier.parent_roles == ("parent", "child"),
      f"{chier.parent_roles} -- without roles both legs hash to one "
      f"consolidation_group_hk and every node becomes its own parent")
ceff = model.get("consolidation_hierarchy_effectivity")
check("the group tree has an effectivity satellite driven on the CHILD",
      ceff.kind == "esat" and ceff.parents == ("consolidation_hierarchy",)
      and ceff.driving_key == "child_consolidation_group_hk",
      f"{ceff.kind}/{ceff.parents}/{ceff.driving_key} -- driving on the child is what "
      f"makes 'at most one open parent per group' expressible")
check("the group tree and its effectivity both load nothing yet",
      all("PLACEHOLDER" in s.bronze_table for s in chier.sources)
      and all("PLACEHOLDER" in s.bronze_table for s in ceff.sources),
      "landing is GATED on a destination nobody owns and on a production tenant")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: failure resolving `consolidation_hierarchy`.

- [ ] **Step 3: Create the hierarchical link**

Create `metadata/entities/hal_consolidation_hierarchy.yml`:

```yaml
# THE CONSOLIDATION TREE -- group contains group.
#
# Workday's shape, landed AS IT IS rather than flattened, which was Adrian's instruction of
# 24 September and is the reason this entity exists at all. The alternative was to collapse
# Workday's two layers into company-to-company edges, which needs someone to decide which
# company "owns" each consolidation node. The only available signal is the node's name.
# That is an inference, and this repo has been wrong enough times inferring from names.
#
# MEASURED: 36 Company_Hierarchy nodes, 35 of them carrying members, forming a tree:
#   All_Headfirst_Global_Plc_Consolidated
#   `-- Impellam_Group_Limited_Consolidated
#       `-- Impellam_UK_Limited_Consolidated  -> includes IE100, UK013, UK103, IE900
#
# SUBORDINATE IS NOT LOADED. Workday also sends Subordinate_Organization_Reference, the
# inverse of superior. It carries nothing this table does not already hold, and storing
# both creates two places for one fact to disagree.
name: consolidation_hierarchy
kind: hal
domain: reference
sensitivity: internal

# ORDER IS THE DIRECTION OF THE EDGE. parent_consolidation_group_hk contains
# child_consolidation_group_hk. A reader who gets this backwards inverts every rollup
# while every row still joins.
parents: [consolidation_group, consolidation_group]
parent_roles: [parent, child]

notes: >
  Parent/child containment between Workday consolidation groups, from
  Superior_Organization_Reference. WHEN an edge was in force is not here -- that is
  esat_consolidation_hierarchy, because a restructure closes one edge and opens another
  and both are inserts.

sources:
  # PLACEHOLDER -- see hub_consolidation_group.yml for why. The landed organisation shape
  # carries `reference_id` and `superior`, so the child is the row itself and the parent is
  # the organisation it names.
  - name: WORKDAY
    bronze_table: PLACEHOLDER.reference.wd_organization
    parent_keys:
      parent: [superior]
      child: [reference_id]
```

- [ ] **Step 4: Create the effectivity satellite**

Create `metadata/entities/esat_consolidation_hierarchy.yml`:

```yaml
# WHEN EACH CONTAINMENT EDGE WAS IN FORCE, driven on the child.
#
# For any consolidation group, at most one parent edge is open at a time. Closing and
# opening are both inserts; nothing here is updated, which is what keeps "which group did
# this roll into in March" answerable.
#
# WHAT WORKDAY CANNOT GIVE US, stated rather than discovered later. Every organisation
# carries Last_Updated_DateTime and it is a real modified timestamp -- but it says a row
# CHANGED, not when the change took legal effect, and Get_Organizations takes no as-of
# parameter in the criteria we send. So the first load establishes today's structure and
# effectivity accrues from subsequent loads. History before the first load is not
# recoverable from this source. That is a property of the source, not a defect to fix.
name: consolidation_hierarchy_effectivity
kind: esat
domain: reference
sensitivity: internal

parents: [consolidation_hierarchy]
payload: [relationship_status]
driving_key: child_consolidation_group_hk

notes: >
  Effectivity of each parent/child edge in consolidation_hierarchy, driven on the child.
  Traverse the tree through this satellite whenever the question is "what did this look
  like on a date"; traverse the link alone only to ask whether an edge has ever existed.

descriptions:
  relationship_status: >-
    Whether this containment edge is in force. An edge that ends is delivered as a new row
    saying so, never an update.

sources:
  - name: WORKDAY
    bronze_table: PLACEHOLDER.reference.wd_organization
    parent_keys:
      parent: [superior]
      child: [reference_id]
    payload: [relationship_status]
    # THE BUSINESS CLOCK, NOT THE LOAD CLOCK. An edge corrected retrospectively must date
    # from when it was effective, or every historical rollup shifts to the day we heard.
    applied_dts_column: effective_from
```

- [ ] **Step 5: Run the tests — expect PASS**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 6: Mutation-prove**

Copy both files aside. Then:

| Mutation | Expected red check |
|---|---|
| delete the `parent_roles` line | the group tree names its roles |
| `driving_key: parent_consolidation_group_hk` | effectivity satellite driven on the CHILD |
| `parents: [consolidation_group, legal_entity]` on the hal | the group tree is a hierarchical link between consolidation groups |

Confirm each produces a named FAIL and the suite completes. Copy the files back.

- [ ] **Step 7: Commit**

```bash
git add metadata/entities/hal_consolidation_hierarchy.yml \
        metadata/entities/esat_consolidation_hierarchy.yml tests/test_accelerator.py
git commit -m "Model the consolidation tree as Workday sends it

Group contains group, from Superior_Organization_Reference, with an
effectivity satellite driven on the child. Not flattened to company-to-company
edges: that needs someone to decide which company owns each consolidation node,
and the only signal available is its name."
```

---

### Task 3: company membership — `lnk_legal_entity_consolidation` and its effectivity

**Files:**
- Create: `metadata/entities/lnk_legal_entity_consolidation.yml`
- Create: `metadata/entities/esat_legal_entity_consolidation.yml`
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: hubs `legal_entity` (existing) and `consolidation_group` (Task 1).
- Produces: link named `legal_entity_consolidation`, FK columns `legal_entity_hk` and `consolidation_group_hk`; esat named `legal_entity_consolidation_effectivity` driving on `legal_entity_hk`.

- [ ] **Step 1: Write the failing test**

```python
# COMPANY MEMBERSHIP -- the other half of the group hierarchy, and an ORDINARY link
# because its two legs are different hubs. Measured: 80 of 82 Companies carry
# Included_In_Organization_Reference. Without this table the consolidation tree exists and
# no legal entity is attached to it.
memb = model.get("legal_entity_consolidation")
check("company membership of a consolidation group is a link between the two hubs",
      memb.kind == "link"
      and set(memb.parents) == {"legal_entity", "consolidation_group"},
      f"{memb.kind}/{memb.parents} -- not a hal: the legs are different hubs")
check("the membership link carries no payload -- when a company was in a group is the "
      "effectivity satellite's job, and a link must stay immutable",
      not memb.payload, f"{memb.payload}")
memb_eff = model.get("legal_entity_consolidation_effectivity")
check("membership has an effectivity satellite driven on the COMPANY",
      memb_eff.kind == "esat" and memb_eff.driving_key == "legal_entity_hk",
      f"{memb_eff.kind}/{memb_eff.driving_key} -- at most one open membership per "
      f"company is what makes 'which group did this roll into in March' answerable")
check("membership and its effectivity load nothing yet",
      all("PLACEHOLDER" in s.bronze_table for s in memb.sources)
      and all("PLACEHOLDER" in s.bronze_table for s in memb_eff.sources),
      "landing is GATED")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: failure resolving `legal_entity_consolidation`.

- [ ] **Step 3: Create the link**

Create `metadata/entities/lnk_legal_entity_consolidation.yml`:

```yaml
# WHICH COMPANIES BELONG TO WHICH CONSOLIDATION GROUP.
#
# AN ORDINARY LINK, NOT A HAL, because its two legs are DIFFERENT hubs -- a legal entity
# and a consolidation group. hal is for a hub related to itself.
#
# ONE DIRECTION ONLY, AND WHICH ONE IS MEASURED. Workday delivers this membership twice:
# Included_Organization_Reference on the group (35 of 36 groups populated) and
# Included_In_Organization_Reference on the company (80 of 82 companies). They are the same
# edge. Loading both would put every edge in the table twice, so this binds the GROUP's
# direction -- `relation = 'includes'` in the landed membership shape -- because the group
# side is the more completely populated of the two.
#
# NO PAYLOAD, and spec.validate refuses one on a link. When a company was in a group is
# esat_legal_entity_consolidation's job; a link is immutable and membership is not.
name: legal_entity_consolidation
kind: link
domain: reference
key_style: federated
parents: [legal_entity, consolidation_group]
sensitivity: internal

notes: >
  Membership of a legal entity in a Workday consolidation group. With
  consolidation_hierarchy this is the whole group structure: the groups form the tree, the
  companies hang off it. Traverse through esat_legal_entity_consolidation when the question
  has a date in it.

sources:
  # PLACEHOLDER -- see hub_consolidation_group.yml. The landed membership shape is one row
  # per edge, already exploded by workday.landing_rows("membership", ...): group_reference_id,
  # member_reference_id, relation.
  - name: WORKDAY
    bronze_table: PLACEHOLDER.reference.wd_organization_membership
    parent_keys:
      legal_entity: [member_reference_id]
      consolidation_group: [group_reference_id]
```

- [ ] **Step 4: Create the effectivity satellite**

Create `metadata/entities/esat_legal_entity_consolidation.yml`:

```yaml
# WHEN EACH COMPANY'S MEMBERSHIP WAS IN FORCE, driven on the company.
#
# For any legal entity, at most one consolidation-group membership is open at a time. This
# is the invariant that makes "which group did this company roll into in March" answerable,
# and it is the one Workday's current-state retrieval cannot reconstruct backwards -- so it
# is established at first load and accrues from there.
name: legal_entity_consolidation_effectivity
kind: esat
domain: reference
sensitivity: internal

parents: [legal_entity_consolidation]
payload: [relationship_status]
driving_key: legal_entity_hk

notes: >
  Effectivity of each company/group membership edge, driven on the company. Closing and
  opening are both inserts, so a restructure never rewrites history.

descriptions:
  relationship_status: >-
    Whether this membership is in force. A membership that ends is delivered as a new row
    saying so, never an update.

sources:
  - name: WORKDAY
    bronze_table: PLACEHOLDER.reference.wd_organization_membership
    parent_keys:
      legal_entity: [member_reference_id]
      consolidation_group: [group_reference_id]
    payload: [relationship_status]
    applied_dts_column: effective_from
```

- [ ] **Step 5: Run the tests — expect PASS**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 6: Mutation-prove**

| Mutation | Expected red check |
|---|---|
| `kind: hal` with `parent_roles: [parent, child]` on the link | membership ... is a link between the two hubs |
| `driving_key: consolidation_group_hk` | effectivity satellite driven on the COMPANY |

Copy files back after each.

- [ ] **Step 7: Commit**

```bash
git add metadata/entities/lnk_legal_entity_consolidation.yml \
        metadata/entities/esat_legal_entity_consolidation.yml tests/test_accelerator.py
git commit -m "Attach the legal entities to the consolidation tree

An ordinary link -- the legs are different hubs. Bound to the group's own
'includes' direction only: Workday sends the same edge twice, on 35 of 36
groups and 80 of 82 companies, and loading both would double every edge."
```

---

### Task 4: rename the client hierarchy, and repair the gate the rename breaks

**Files:**
- Rename: `metadata/entities/hal_legal_entity_hierarchy.yml` → `metadata/entities/hal_client_legal_entity_hierarchy.yml`
- Rename: `metadata/entities/esat_legal_entity_hierarchy.yml` → `metadata/entities/esat_client_legal_entity_hierarchy.yml`
- Modify: `verify_repo.py:1666` (`_WD_PLANNED`) and `verify_repo.py:1701` (`_wd_repointed`)
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Produces: entities named `client_legal_entity_hierarchy` and `client_legal_entity_hierarchy_effectivity`. Nothing consumes them; they load nothing.

- [ ] **Step 1: Write the failing test**

```python
# THE CLIENT HIERARCHY IS A RENAME, NOT A NEW THING. Its shape was already right --
# company to company, roles parent/child, effectivity on the child -- and its placeholder
# source was already called CLIENT_PORTAL. It was client-shaped in intent; this makes the
# name say so, now that a second hierarchy exists to be confused with.
clienth = model.get("client_legal_entity_hierarchy")
check("the client hierarchy is company-to-company over the shared roster",
      clienth.kind == "hal"
      and clienth.parents == ("legal_entity", "legal_entity")
      and clienth.parent_roles == ("parent", "child"),
      f"{clienth.kind}/{clienth.parents}/{clienth.parent_roles}")
check("the two hierarchies are separate entities, so one child can have a parent in each "
      "without breaking 'at most one open parent'",
      model.get("client_legal_entity_hierarchy") is not model.get("consolidation_hierarchy"),
      "sharing one table would break the invariant silently -- every row still joins")
check("the old undifferentiated hierarchy name is gone",
      not any(e.name == "legal_entity_hierarchy" for e in model.entities),
      "a bare legal_entity_hierarchy beside a consolidation one is the ambiguity this "
      "rename exists to remove")

# REVIEW FOCUS 1, and the reason this task renames and repairs in ONE commit.
# AND THE EXPIRY GATE MUST STILL SEE THE RENAMED ENTITIES. verify_repo swept
# entity.name.startswith("legal_entity"); client_legal_entity_hierarchy does not start
# with that, so the rename would have dropped two entities out of the sweep and the gate
# would have gone on passing over entities it no longer looked at.
_swept = {e.name for e in model.entities
          if "legal_entity" in e.name or "consolidation" in e.name}
check("every legal-entity and consolidation entity is inside the placeholder-expiry sweep",
      {"legal_entity", "client_legal_entity_hierarchy",
       "client_legal_entity_hierarchy_effectivity", "consolidation_group",
       "consolidation_hierarchy", "legal_entity_consolidation"} <= _swept,
      f"{sorted(_swept)} -- a gate that keys on a name prefix silently narrows the day "
      f"something is renamed, and goes on passing")
```

And add this to `verify_repo.py`, immediately after the `_WD_PLANNED` definition, so
REVIEW FOCUS 2 is pinned by a check rather than by having remembered to edit a dict:

```python
# AND ITS KEYS MUST NAME ENTITIES THAT EXIST. _WD_PLANNED is hand-kept, and its keys are
# entity names; the check below compares planned columns against landed ones and would
# happily compare a key naming an entity that had been renamed away, reporting nothing.
# REVIEW FOCUS 2: the 24 September rename moved legal_entity_hierarchy to
# client_legal_entity_hierarchy, and a stale key here fails nothing.
_wd_planned_entities = set(_WD_PLANNED) - {"provenance"}
_wd_planned_missing = sorted(_wd_planned_entities - {e.name for e in model.entities})
check("every hand-kept planned-column key names an entity that still exists",
      not _wd_planned_missing,
      f"{_wd_planned_missing} -- a key naming a renamed entity compares planned columns "
      f"against nothing and passes, which is the failure mode of every hand-kept list")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: failure resolving `client_legal_entity_hierarchy`.

- [ ] **Step 3: Rename the two files and their `name:` fields**

```bash
git mv metadata/entities/hal_legal_entity_hierarchy.yml \
       metadata/entities/hal_client_legal_entity_hierarchy.yml
git mv metadata/entities/esat_legal_entity_hierarchy.yml \
       metadata/entities/esat_client_legal_entity_hierarchy.yml
```

In `hal_client_legal_entity_hierarchy.yml` set `name: client_legal_entity_hierarchy`.
In `esat_client_legal_entity_hierarchy.yml` set `name: client_legal_entity_hierarchy_effectivity` and `parents: [client_legal_entity_hierarchy]`.

Add to the top of the hal file, above the existing comment block:

```yaml
# THE CLIENT SIDE, AND IT IS FOR SPEND REPORTING BY CLIENT GROUP.
#
# Renamed 24 September, when a second hierarchy arrived and a bare "legal_entity_hierarchy"
# stopped saying which. The shape did not change; its placeholder source was already called
# CLIENT_PORTAL.
#
# WHAT IT ROLLS UP, AND WHY IT CANNOT COME FROM THE VMS. Measured across the invoice feed:
# 12 real buyer codes, one per client, with no sub-entity structure under any of them. A
# client group running several VMS programmes appears as several buyer codes and nothing in
# the feed says they belong together -- which is exactly the fact this hierarchy supplies.
# Company_Code/Company_Name look like they might carry it and do not: 352 of their 353
# values sit on rows with a blank buyer and no Invoice_ID, and they are STAFFING SUPPLIERS
# (Manpower, LanceSoft, Terra Staffing Group).
#
# AS-IS BY DEFAULT, decided 24 September: spend rolls up the structure as it stands today,
# so year-on-year comparison is like-for-like, at the cost of restating history when a
# client restructures. As-was stays reachable through the effectivity satellite, and
# tests/test_accelerator.py exercises it so that defaulting to as-is does not quietly
# retire it.
```

- [ ] **Step 4: Repair `_WD_PLANNED` and the expiry sweep**

In `verify_repo.py`, change the `_WD_PLANNED` key:

```python
    # hal_client_legal_entity_hierarchy keys on (parent, child), which the membership shape
    # spells group_reference_id and member_reference_id. RENAMED 24 September with the
    # entity; a dict key naming an entity that no longer exists compares against nothing.
    "client_legal_entity_hierarchy": {"group_reference_id", "member_reference_id", "relation"},
```

Replace the sweep so it cannot narrow on a rename:

```python
# THE SWEEP IS BY SUBSTRING, NOT PREFIX, AND THAT IS THE FIX FOR A REAL NEAR-MISS.
# This read entity.name.startswith("legal_entity"). On 24 September the two client
# hierarchy entities were renamed to client_legal_entity_hierarchy*, which does not start
# with "legal_entity" -- so the rename would have dropped them out of the sweep and this
# gate would have gone on passing over entities it had stopped looking at. A gate keyed on
# a name prefix silently narrows the day something is renamed.
_LE_ENTITY = ("legal_entity", "consolidation")
_wd_repointed = sorted(
    f"{entity.name}/{src.name}"
    for entity in model.entities if any(k in entity.name for k in _LE_ENTITY)
    for src in entity.sources if "PLACEHOLDER" not in (src.bronze_table or ""))
```

- [ ] **Step 5: Run both suites — expect PASS**

Run: `.venv/bin/python tests/test_accelerator.py && .venv/bin/python verify_repo.py`
Expected: `ALL CHECKS PASSED` and `VERIFICATION PASSED`.

- [ ] **Step 6: Mutation-prove the sweep repair specifically**

This is the check most worth proving, because its failure mode is silence.

1. Revert the sweep to `entity.name.startswith("legal_entity")`.
2. Point `hal_client_legal_entity_hierarchy.yml`'s binding at `01_usnc_bronze_dev.reference.x`.
3. Run `.venv/bin/python verify_repo.py`. With the prefix version the placeholder-expiry check **passes** — that is the bug, observed.
4. Restore the substring version, keep the repointed binding, run again: the check now FAILS and names the entity.
5. Copy both files back.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "Name the client hierarchy, and stop its rename blinding a gate

The shape was already right and its placeholder source was already called
CLIENT_PORTAL; it needed a name that says which hierarchy it is now that a
second one exists.

The rename would have broken verify_repo silently. Its placeholder-expiry
sweep keyed on entity.name.startswith(\"legal_entity\"), and
client_legal_entity_hierarchy does not start with that -- so two entities
would have left the sweep and the gate would have gone on passing over
entities it had stopped looking at. Now matched by substring, and proven by
repointing a binding and watching the prefix version stay green."
```

---

### Task 5: the two gates nothing else would catch

**Files:**
- Create: `src/accelerator/hierarchy.py`
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Produces: `overlapping_edges(a: set, b: set) -> set` and `parent_at(edges: list[dict], child: str, when: str) -> str | None`.

These are pure functions with no Spark and no workspace, so they run in the offline suite. They exist now, before data lands, because the failure they describe is invisible to every other gate.

- [ ] **Step 1: Write the failing test**

```python
from accelerator import hierarchy as _hy  # noqa: E402

# REVIEW FOCUS 3. The separation exists to stop one edge living in both hierarchies. Both
# tables would be individually valid, every existing gate would stay green, and the rollup
# would double-count. Nothing else in this repo looks across the two.
check("an edge present in both hierarchies is detected",
      _hy.overlapping_edges({("A", "B"), ("C", "D")}, {("A", "B")}) == {("A", "B")},
      "the one failure the whole separation exists to prevent")
check("disjoint hierarchies report no overlap",
      _hy.overlapping_edges({("A", "B")}, {("C", "D")}) == set(),
      "a check that fires on correct input is worse than no check")
# NON-VACUITY: a fixture with nothing in it would satisfy the check above by comparing
# nothing, which is the shape this repo keeps finding.
check("the overlap fixture is not empty",
      len({("A", "B"), ("C", "D")}) == 2, "otherwise the disjoint case proves nothing")

# REVIEW FOCUS 4. Spend reporting defaults to as-is, which is exactly when the effectivity
# satellite stops being queried and can rot unnoticed. This pins a point-in-time traversal
# against a structure that CHANGES mid-history: AEE1 moved from GROUP_OLD to GROUP_NEW on
# 2026-06-01.
_edges = [
    {"child": "AEE1", "parent": "GROUP_OLD", "from": "2024-01-01", "to": "2026-06-01"},
    {"child": "AEE1", "parent": "GROUP_NEW", "from": "2026-06-01", "to": None},
]
check("as-was: spend in March rolls up the parent that was in force then",
      _hy.parent_at(_edges, "AEE1", "2026-03-15") == "GROUP_OLD",
      "if this returns GROUP_NEW the history has been restated, which is as-is behaviour "
      "wearing an as-was query's name")
check("as-is: today's question gets today's parent",
      _hy.parent_at(_edges, "AEE1", "2026-09-24") == "GROUP_NEW", "the default view")
check("a date before any edge has no parent, rather than the earliest one",
      _hy.parent_at(_edges, "AEE1", "2023-01-01") is None,
      "spend predating the structure must not be attributed to a group by accident")
check("an unknown child has no parent",
      _hy.parent_at(_edges, "NOPE", "2026-09-24") is None, "must not raise")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: `ModuleNotFoundError: No module named 'accelerator.hierarchy'`.

- [ ] **Step 3: Implement**

Create `src/accelerator/hierarchy.py`:

```python
"""Hierarchy traversal and the one cross-hierarchy check nothing else performs.

PURE AND SPARK-FREE, like invoice_rules. Both functions here decide something that is
either right or wrong regardless of where the rows came from, and a decision that can only
be tested by running a pipeline is a decision that does not get tested.
"""
from __future__ import annotations


def overlapping_edges(group_edges: set, client_edges: set) -> set:
    """Edges present in BOTH hierarchies, which must always be empty.

    THE FAILURE THE SEPARATION EXISTS TO PREVENT, and the only one invisible to every
    other gate. The group hierarchy and the client hierarchy are separate tables; each is
    individually valid whatever it contains, so an edge duplicated across them breaks no
    constraint, joins perfectly, and double-counts a rollup. Append-only, reconciliation
    and mask-survival checks all stay green.

    Returns the offending edges rather than a bool, because a gate that says only "there
    is a problem" makes someone go and find it.
    """
    return group_edges & client_edges


def parent_at(edges: list[dict], child: str, when: str) -> str | None:
    """The parent in force for `child` on date `when`, or None.

    AS-WAS TRAVERSAL, and it exists because spend reporting DEFAULTS to as-is. Defaulting
    is the moment the effectivity satellite stops being queried, and an effectivity
    satellite nothing queries is one nobody notices is wrong. This is the query that keeps
    it exercised.

    Half-open intervals -- `from` inclusive, `to` exclusive -- so an edge that closes on
    the same day another opens yields exactly one parent, never two and never none. The
    day a client restructures is precisely when a naive closed interval returns both.

    `to` of None means open. Dates compare as ISO strings, which sort correctly and avoid
    importing a date parser into a function whose inputs are already normalised by the
    vault.
    """
    for edge in edges:
        if edge.get("child") != child:
            continue
        if edge.get("from") is not None and when < edge["from"]:
            continue
        if edge.get("to") is not None and when >= edge["to"]:
            continue
        return edge.get("parent")
    return None
```

- [ ] **Step 4: Run the tests — expect PASS**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Mutation-prove**

Copy `src/accelerator/hierarchy.py` aside first.

| Mutation | Expected red check |
|---|---|
| `return set()` in `overlapping_edges` | an edge present in both hierarchies is detected |
| `return group_edges \| client_edges` | disjoint hierarchies report no overlap |
| drop the `when < edge["from"]` guard | a date before any edge has no parent |
| change `when >= edge["to"]` to `when > edge["to"]` | as-was: spend in March rolls up the parent in force then — **this is the half-open boundary; if it stays green the interval logic is untested** |
| return the last match instead of the first | as-was: spend in March rolls up the parent in force then |

Confirm each names a red check and the suite completes. Copy the file back.

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/hierarchy.py tests/test_accelerator.py
git commit -m "Add the two hierarchy checks nothing else would catch

An edge in both hierarchies breaks no constraint and joins perfectly -- every
existing gate stays green while the rollup double-counts. And as-was traversal
is exercised here precisely because spend reporting defaults to as-is, which is
the moment the effectivity satellite stops being queried and starts being able
to rot unnoticed."
```

---

### Task 6: regenerate the artefacts and correct the spec

**Files:**
- Modify: generated artefacts under `docs/`, `diagram/`, `onto/`, `data_contracts/`
- Modify: `docs/superpowers/specs/2026-09-24-legal-entity-hierarchies-design.md`

- [ ] **Step 1: Regenerate every byte-gated artefact**

```bash
.venv/bin/python tools/emit_source_to_target.py
.venv/bin/python tools/emit_dbml_diagram.py
.venv/bin/python tools/emit_ontology.py
.venv/bin/python tools/emit_data_contract.py
```

- [ ] **Step 2: Run verification and fix what it names**

Run: `.venv/bin/python verify_repo.py`

Expect it to name any missing Archify architecture document for `domain: reference` — that domain already exists, so it should not. If it names a description that fails to reach the DBML diagram, the column is missing a `descriptions:` entry; add it to the entity rather than to the diagram.

- [ ] **Step 3: Correct the spec's one overstatement**

In `docs/superpowers/specs/2026-09-24-legal-entity-hierarchies-design.md`, the architecture block says:

```
GROUP SIDE (Workday, loadable today)
```

Replace with:

```
GROUP SIDE (Workday -- RETRIEVABLE today, not loadable: landing is gated)
```

And in the "What this unblocks" section, after the sentence ending "and we can retrieve them today", add:

> **Retrieve, not load.** There is no bronze destination for the Workday
> retrieval — that is Task 3 of `2026-09-07-workday-reference-source.md`, gated
> on a destination nobody owns, and its Task 4 is gated additionally on a
> production tenant. Every entity this design adds is therefore a declared shape
> bound to `PLACEHOLDER`, exactly like the ones it sits beside.

- [ ] **Step 4: Run both suites one final time**

Run: `.venv/bin/python tests/test_accelerator.py && .venv/bin/python verify_repo.py`
Expected: `ALL CHECKS PASSED` and `VERIFICATION PASSED`, with a check count **higher** than the 1166 this branch started from.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Regenerate the artefacts, and say retrievable rather than loadable

The spec's architecture block said the group side was loadable today. It is
retrievable today; landing is gated on a destination nobody owns and on a
production tenant. Every entity added here is a declared shape on PLACEHOLDER."
```

---

### Task 7: one contracting entity per client

**Files:**
- Create: `metadata/entities/lnk_client_contracting_entity.yml`
- Create: `metadata/entities/esat_client_contracting_entity.yml`
- Modify: `src/accelerator/hierarchy.py`
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: hub `legal_entity` (existing); `parent_at` from Task 5.
- Produces: link named `client_contracting_entity` with roles `client`/`contracting`, FK columns `client_legal_entity_hk` and `contracting_legal_entity_hk`; esat `client_contracting_entity_effectivity` driving on `client_legal_entity_hk`; function `rollup_key(edges, child, when) -> str`.

- [ ] **Step 1: Write the failing test**

```python
# AMY, 24 SEPTEMBER: "We should have only 1 contracting entity on the Vertage side per
# client -- this may change as we go through the legal entity unification only where we
# have a mix of services and one is professional services (SOW)."
contracting = model.get("client_contracting_entity")
check("the contracting relationship is a link on the roster with BOTH legs roled",
      contracting.kind == "link"
      and contracting.parents == ("legal_entity", "legal_entity")
      and contracting.parent_roles == ("client", "contracting"),
      f"{contracting.kind}/{contracting.parents}/{contracting.parent_roles}")
# NOT A HAL, AND THE DISTINCTION IS A CLAIM ABOUT THE WORLD. hal means one hub related to
# itself HIERARCHICALLY -- a parent owning a child. Our contracting entity does not own
# the client. Using hal here would put a false statement in the model's own vocabulary.
check("the contracting relationship is NOT modelled as a hierarchy",
      contracting.kind != "hal",
      "Guidant Global Inc contracts with Ameren; it does not own Ameren")
contr_eff = model.get("client_contracting_entity_effectivity")
check("the contracting relationship is effectivity-dated on the CLIENT leg, which is "
      "what makes Amy's 'only 1 per client' enforceable rather than hoped for",
      contr_eff.kind == "esat" and contr_eff.driving_key == "client_legal_entity_hk",
      f"{contr_eff.kind}/{contr_eff.driving_key} -- she says it moves during the legal "
      f"entity unification, so it must close and reopen as inserts")

# REVIEW FOCUS 5. Amy: parent/child is "not applicable to all (Ameren as an example)".
# A spend projection grouping by parent id drops every single-entity client, and nothing
# raises -- the query succeeds and the total is just smaller.
check("a client with NO parent rolls up to itself, not to nothing",
      _hy.rollup_key([], "AEE1", "2026-09-24") == "AEE1",
      "Ameren has one legal entity and no parent; grouping on a null parent would drop "
      "its entire 1,176,896.90 out of client-group spend reporting in silence")
check("a client WITH a parent rolls up to the parent",
      _hy.rollup_key(
          [{"child": "SUB1", "parent": "GROUPCO", "from": "2024-01-01", "to": None}],
          "SUB1", "2026-09-24") == "GROUPCO",
      "the parent id is the reporting key, per Amy")
check("the rollup key respects the as-of date, so it cannot quietly become as-is",
      _hy.rollup_key(
          [{"child": "SUB1", "parent": "OLDCO", "from": "2024-01-01", "to": "2026-06-01"},
           {"child": "SUB1", "parent": "NEWCO", "from": "2026-06-01", "to": None}],
          "SUB1", "2026-03-15") == "OLDCO",
      "rollup_key must delegate to parent_at rather than re-implement traversal")
```

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python tests/test_accelerator.py`
Expected: failure resolving `client_contracting_entity`.

- [ ] **Step 3: Add `rollup_key` to `src/accelerator/hierarchy.py`**

```python
def rollup_key(edges: list[dict], child: str, when: str) -> str:
    """The id a child's spend rolls up to on `when` -- its parent, or itself.

    THE NULL-PARENT TRAP, AND IT IS WHY THIS IS A FUNCTION RATHER THAN A JOIN. Amy Keser,
    24 September: parent/child is "not applicable to all (Ameren as an example)", and the
    parent id is "the key for reporting on total spend across multiple records". Both are
    true at once, so most clients have no parent and the reporting key is the parent.

    A projection that simply groups by parent id therefore DROPS every single-entity
    client, in silence: the join succeeds, no row is malformed, and the report is merely
    smaller. Ameren's entire invoiced value would be absent from client-group spend.

    Falling back to the child's own id makes a single-entity client a group of one, which
    is what it is. Delegates to parent_at so the as-of date cannot drift apart from the
    rest of the traversal.
    """
    return parent_at(edges, child, when) or child
```

- [ ] **Step 4: Create the link**

Create `metadata/entities/lnk_client_contracting_entity.yml`:

```yaml
# WHICH OF OUR LEGAL ENTITIES CONTRACTS WITH WHICH CLIENT.
#
# Amy Keser, 24 September 2026: "We should have only 1 contracting entity on the Vertage
# side per client -- this may change as we go through the legal entity unification only
# where we have a mix of services and one is professional services (SOW)."
#
# BOTH LEGS ARE hub_legal_entity, because the roster is one roster: our companies and our
# clients' companies live in it together, which is the whole reason its key is authored.
# So this is a self-relationship and roles are mandatory -- without them both legs write
# to one legal_entity_hk and the link keys the client twice.
#
# `kind: link`, NOT `hal`, AND THE DIFFERENCE IS A CLAIM ABOUT THE WORLD. A hierarchical
# link says one node OWNS another. Guidant Global Inc contracts with Ameren; it does not
# own Ameren. Roles are permitted on any link kind whose parents are not distinct, not
# only on hierarchical ones, so nothing is lost by being accurate here.
#
# "ONLY 1" IS ENFORCED BY THE EFFECTIVITY SATELLITE, not by this table. A link records
# that a relationship has existed; at most one open at a time is a statement about when,
# and it lives in esat_client_contracting_entity driven on the client.
name: client_contracting_entity
kind: link
domain: reference
key_style: authored
parents: [legal_entity, legal_entity]
parent_roles: [client, contracting]
sensitivity: internal

notes: >
  The Vertage-side legal entity that contracts with each client. One open relationship per
  client at a time, enforced through esat_client_contracting_entity. Expected to move
  during the legal entity unification where a client takes a mix of services and one is
  professional services (SOW).

sources:
  # PLACEHOLDER. This is a business input like the roster itself -- no feed states which of
  # our companies contracts with a client. Ameren's is Guidant Global Inc (Workday US100),
  # confirmed by Amy on 24 September, which is one row of it and not the list.
  - name: CLIENT_PORTAL
    bronze_table: PLACEHOLDER.reference.client_contracting_entity
    parent_keys:
      client: [client_legal_entity_code]
      contracting: [contracting_legal_entity_code]
```

- [ ] **Step 5: Create the effectivity satellite**

Create `metadata/entities/esat_client_contracting_entity.yml`:

```yaml
# WHEN EACH CONTRACTING RELATIONSHIP HELD, driven on the CLIENT.
#
# This is where Amy's "only 1 contracting entity per client" becomes enforceable: for any
# client, at most one contracting relationship is open at a time. Driving on the client is
# what makes that expressible; driving on our entity would say "one client per contracting
# company", which is false -- Guidant Global Inc contracts with many clients.
#
# IT MUST BE DATED BECAUSE SHE SAYS IT MOVES. The legal entity unification changes which of
# our companies contracts with a client. Closing and reopening as inserts is what keeps
# "who did we contract through last year" answerable after it has moved.
name: client_contracting_entity_effectivity
kind: esat
domain: reference
sensitivity: internal

parents: [client_contracting_entity]
payload: [relationship_status]
driving_key: client_legal_entity_hk

notes: >
  Effectivity of each client/contracting-entity relationship, driven on the client. At most
  one open per client at a time.

descriptions:
  relationship_status: >-
    Whether this contracting relationship is in force. A relationship that ends is
    delivered as a new row saying so, never an update.

sources:
  - name: CLIENT_PORTAL
    bronze_table: PLACEHOLDER.reference.client_contracting_entity
    parent_keys:
      client: [client_legal_entity_code]
      contracting: [contracting_legal_entity_code]
    payload: [relationship_status]
    applied_dts_column: effective_from
```

- [ ] **Step 6: Run both suites — expect PASS**

Run: `.venv/bin/python tests/test_accelerator.py && .venv/bin/python verify_repo.py`
Expected: `ALL CHECKS PASSED` and `VERIFICATION PASSED`.

- [ ] **Step 7: Mutation-prove**

Copy the three touched files aside first.

| Mutation | Expected red check |
|---|---|
| `kind: hal` with the same roles | the contracting relationship is NOT modelled as a hierarchy |
| remove `parent_roles` | spec.validate refuses it — confirm the suite reports, not aborts |
| `driving_key: contracting_legal_entity_hk` | effectivity-dated on the CLIENT leg |
| `return parent_at(edges, child, when)` in `rollup_key` (drop `or child`) | a client with NO parent rolls up to itself |
| `return child` in `rollup_key` | a client WITH a parent rolls up to the parent |

The fourth is the one that matters most: it is the null-parent trap itself, and it is the mutation that would otherwise ship.

- [ ] **Step 8: Regenerate artefacts and commit**

```bash
.venv/bin/python tools/emit_source_to_target.py
.venv/bin/python tools/emit_dbml_diagram.py
.venv/bin/python tools/emit_ontology.py
.venv/bin/python tools/emit_data_contract.py
git add -A
git commit -m "Model one contracting entity per client, and stop null parents vanishing

Amy: one contracting entity on the Vertage side per client, moving during the
legal entity unification. A link on the shared roster with both legs roled --
not a hal, because Guidant Global Inc contracts with Ameren rather than owning
it -- and dated on the client leg, which is what makes 'only 1' enforceable.

And rollup_key, because her other sentence is a trap: parent/child is 'not
applicable to all (Ameren as an example)' while the parent id is the reporting
key. Grouping by parent id drops every single-entity client in silence. A client
with no parent rolls up to itself."
```
