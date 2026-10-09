# Workday Reference Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Workday's reference retrieval into something the vault can bind to, and then bind the four legal-entity entities that have been declared and empty since 5 September.

**Architecture:** The retrieval half exists — `src/accelerator/workday.py` builds requests and parses responses, `tools/fetch_workday_references.py` calls the tenant. What is missing is a **landing shape**: the tool writes one nested JSON document, and a source binding needs a flat table with stable columns. Tasks 1 and 2 build and gate that shape offline. Tasks 3–6 need a destination table and a permission that do not exist yet, and are marked so.

**Tech Stack:** Python 3.11 floor, stdlib only for the landing writer (no pyarrow — see Task 1), the repo's `check(name, condition, detail)` idiom, NOT pytest.

**Spec:** `docs/superpowers/specs/2026-09-07-workday-reference-source-design.md`

## Status: Tasks 1 and 2 are DONE (7 September). Tasks 3-6 remain gated.

## READ THIS BEFORE STARTING: only Tasks 1 and 2 were buildable

Tasks 3 onwards are blocked on three things that are not ours:

| blocker | who | why it blocks |
|---|---|---|
| A destination table for landed output | **unowned** | The vault reads tables; the tool writes a file. We hold no `CREATE SCHEMA` on either catalog |
| `Get_Organizations` domain grant | Workday team, **WDJ-5** | Without it the hierarchy cannot be retrieved at all: `Server.processingError \| not authorized` |
| A production host and tenant | Workday team, **WDJ-5** | WIDs are tenant-specific; **no binding goes live against `impl-services1`** |

**Do not repoint any entity YAML until Task 3 has a real table.** Substituting one fictional
table name for another reproduces the `PLACEHOLDER` state this work exists to leave, while
looking like progress.

## Global Constraints

* **Python 3.11 is the floor.** No backslash inside an f-string expression part; no assignment expression in a comprehension's iterable. CI runs 3.11 and 3.13.
* **Suites:** `uv run --frozen python verify_repo.py` and `uv run --frozen python tests/test_accelerator.py`. Reproduce the 3.11 leg with `--python 3.11`. **Compare check SETS, never counts.**
* **A check that cannot fail is a defect.** Mutation-prove every check: break the thing it guards, observe a NAMED FAIL, restore. Derive the mutation from the check's own prose.
* **Commit before mutation-testing. Never `git checkout` to undo a mutation** — copy the file aside and copy it back. This has destroyed uncommitted work in this project **four** times, twice on 6 September.
* **DEF-14:** `main()` returns an int; `sys.exit` only on a truthy rc.
* **DEF-12:** nothing in `checks/` may rely on `__file__` without the guard. The landing writer is a **tool**, not a check, for exactly this reason.
* **No credential in any tracked file.** `.workday-credentials` is git-ignored, which also excludes it from the bundle upload; `verify_repo` asserts both. Secrets belong in the Key Vault scope PLT-7 asks for.
* **`build_request` takes no credential and must not start.** A check asserts the request body cannot contain one; `--verbose` prints that body.

---

### Task 1: A landing writer that emits flat, stable rows

**Files:**
- Create: `tools/land_workday_references.py`
- Modify: `src/accelerator/workday.py` (add `landing_rows`, `MEMBERSHIP_COLUMNS`, `REFERENCE_COLUMNS`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `workday.parse_response`, `workday.parse_organizations` (both exist)
- Produces: `workday.landing_rows(kind, rows, *, tenant, host, version, retrieved_at) -> list[dict]` with keys exactly `REFERENCE_COLUMNS`, `ORGANISATION_COLUMNS` or `MEMBERSHIP_COLUMNS` depending on `kind`

- [x] **Step 1: Write the failing test**

```python
_LR = _wd.landing_rows("reference", _wd_rows, tenant="t", host="h",
                       version="v46.2", retrieved_at="2026-09-07T00:00:00Z")
check("landing rows are flat and carry their provenance",
      all(set(r) == set(_wd.REFERENCE_COLUMNS) for r in _LR)
      and _LR[0]["reference_id_type"] == "Company_Reference_ID"
      and _LR[0]["tenant"] == "t" and _LR[0]["retrieved_at"].endswith("Z"),
      f"{_LR[:1]} -- a source binding reads columns, not a nested document; and a landed "
      f"row that does not say which tenant and when it came from cannot be superseded by "
      f"a later retrieval")
```

- [x] **Step 2: Run it and watch it fail**

`uv run --frozen python tests/test_accelerator.py` → `AttributeError: module 'accelerator.workday' has no attribute 'landing_rows'`

- [x] **Step 3: Implement**

Three column tuples and one function in `src/accelerator/workday.py`:

```python
# THE LANDED SHAPE. Flat, because a source binding declares columns; and carrying its own
# provenance, because a later retrieval must be able to supersede an earlier one and
# `dedup_order` needs something to order by. `retrieved_at` is OUR clock -- when we asked --
# and is deliberately not called applied_dts: Workday's own change timestamp is
# Last_Updated_DateTime on the organisation payload, and the two are not the same fact.
REFERENCE_COLUMNS = ("reference_id_type", "id", "descriptor",
                     "referenced_object_descriptor", "wid",
                     "host", "tenant", "wws_version", "retrieved_at")
ORGANISATION_COLUMNS = ("reference_id", "name", "code", "inactive", "type",
                        "superior", "top_level",
                        "host", "tenant", "wws_version", "retrieved_at")
# ONE ROW PER EDGE, which is what hal_legal_entity_hierarchy binds to. A group with three
# members is three rows, not one row with an array -- an array cannot be a link's key.
MEMBERSHIP_COLUMNS = ("group_reference_id", "member_reference_id", "relation",
                      "host", "tenant", "wws_version", "retrieved_at")


def landing_rows(kind, rows, *, tenant, host, version, retrieved_at):
    """Flat rows for one retrieval, ready to land as a table.

    `kind` is "reference", "organisation" or "membership". Membership EXPLODES the
    hierarchy: `included` and `included_in` are arrays on an organisation and edges in a
    link, and a link cannot key on an array.
    """
    stamp = {"host": host, "tenant": tenant, "wws_version": version,
             "retrieved_at": retrieved_at}
    if kind == "reference":
        return [{k: r.get(k) for k in ("reference_id_type", "id", "descriptor",
                                       "referenced_object_descriptor", "wid")} | stamp
                for r in rows]
    if kind == "organisation":
        return [{k: r.get(k) for k in ("reference_id", "name", "code", "inactive",
                                       "type", "superior", "top_level")} | stamp
                for r in rows]
    if kind == "membership":
        out = []
        for r in rows:
            for member in r.get("included") or ():
                out.append({"group_reference_id": r.get("reference_id"),
                            "member_reference_id": member,
                            "relation": "includes"} | stamp)
            for group in r.get("included_in") or ():
                out.append({"group_reference_id": group,
                            "member_reference_id": r.get("reference_id"),
                            "relation": "included_in"} | stamp)
        return out
    raise ValueError(f"kind must be reference|organisation|membership, got {kind!r}")
```

- [x] **Step 4: Run the test — expect PASS**

- [x] **Step 5: Write the tool**

`tools/land_workday_references.py`, NDJSON output, one file per kind:

```python
# NDJSON, NOT PARQUET, and the reason is a dependency rather than a preference. Parquet
# needs pyarrow, which is not in this repo's lockfile and would be a new dependency for a
# landing format nobody has agreed yet. Spark reads NDJSON natively and a person can read
# it with `head`. Revisit when Task 3 names a destination -- if that destination wants
# Parquet, adding pyarrow is a decision taken with a reason.
```

One `--kind` per invocation, reusing `fetch_workday_references.read_password_file` for the
credential so the refusals it enforces are not re-implemented.

- [x] **Step 6: Prove the membership explosion**

Mutate `landing_rows` to emit `included` as an array rather than one row per member; the
Step 1 check must not fire (it tests `reference`), so add:

```python
_LM = _wd.landing_rows("membership", _wd_orgs, tenant="t", host="h",
                       version="v46.2", retrieved_at="2026-09-07T00:00:00Z")
check("membership lands one row per edge, not one row with an array",
      len(_LM) == 2
      and {r["member_reference_id"] for r in _LM} == {"UK011", "UK013"}
      and all(r["group_reference_id"] == "CONS_IGL" for r in _LM),
      f"{_LM} -- hal_legal_entity_hierarchy keys on (parent, child); an array cannot be a "
      f"link's business key, so the explosion happens here or it happens in SQL nobody "
      f"gated")
```

- [x] **Step 7: Commit**

```bash
git add src/accelerator/workday.py tools/land_workday_references.py tests/test_accelerator.py
git commit -m "A flat landing shape for Workday retrievals, one row per edge"
```

---

### Task 2: Gate the landed shape against what a binding will read

**Files:**
- Modify: `verify_repo.py`
- Test: the same file (this is a repo gate, not a suite check)

**Interfaces:**
- Consumes: `workday.REFERENCE_COLUMNS`, `ORGANISATION_COLUMNS`, `MEMBERSHIP_COLUMNS`

**Why this exists:** Task 3 will write a `bronze_table` name into four entity YAMLs, and
`key_columns` / `payload` naming columns the landed table must actually have. Nothing would
catch a rename on one side. The gate asserts the two agree **before** either is live, which
is the only moment it is cheap.

- [x] **Step 1: Write the failing check**

```python
# The columns the legal-entity bindings will read, per the design spec. Written here rather
# than derived, because the YAML does not exist yet -- and this list is DELETED in Task 4,
# replaced by reading the bindings themselves. A hand-kept list that outlives its purpose is
# the second authority this repo keeps removing.
_WD_PLANNED = {
    "legal_entity":            {"id", "name", "code"},
    "legal_entity_hierarchy":  {"group_reference_id", "member_reference_id"},
}
_wd_missing = {
    entity: sorted(cols - set(_wdmod.REFERENCE_COLUMNS) - set(_wdmod.ORGANISATION_COLUMNS)
                   - set(_wdmod.MEMBERSHIP_COLUMNS))
    for entity, cols in _WD_PLANNED.items()
}
_wd_missing = {k: v for k, v in _wd_missing.items() if v}
check("every column the planned Workday bindings will read exists in a landed shape",
      not _wd_missing,
      f"{_wd_missing} -- Task 3 writes these names into a bronze_table binding; a column "
      f"the landing writer does not emit fails at pipeline definition time, in a workspace, "
      f"rather than here")
```

- [x] **Step 2: Run it, fix whichever side is wrong, run again**
- [x] **Step 3: Mutation-prove** — remove `code` from `ORGANISATION_COLUMNS`, observe the named FAIL, copy the file back (**not** `git checkout`)
- [x] **Step 4: Commit**

---

### Task 3 — GATED: land the tables

**Blocked on:** ~~a destination nobody owns~~ → **the destination was named on 24
September** and the gate is now narrower. Adrian: both Hubspot and Workday write to
Databricks, and Workday's reference values are to be stored **in Bronze as reference data
coming from Workday**. The convention is uniform across all 15 file sources —
`<source>_raw` schema with a `<source>_raw_files` volume — and **no Workday schema exists
yet**, so this waits on Bronze creating `workday_raw` and on the Workday integration
writing into it.

**Two corrections to what is written below.** (1) **Workday writes; we do not land.**
This task was written for a pull model where `tools/land_workday_references.py` produced
the files. That tool remains how every Workday fact in this repo was established and stays
the right instrument for probing a tenant, but it is a development tool and not the
production landing route. (2) **The table names below do not follow the convention** — no
other source prefixes its tables with an abbreviation of its own schema (`hubspot_raw.deals`,
not `hubspot_raw.hs_deals`), so `wd_organization` should almost certainly be
`workday_raw.organization`. Six entity bindings name the old spelling; settle it before
they are repointed, not after.

**When unblocked:** land three tables from `tools/land_workday_references.py` —
`wd_reference_id`, `wd_organization`, `wd_organization_membership` — in whatever schema the
landing decision names. Insert-only, one batch per retrieval, `retrieved_at` distinguishing
batches so `dedup_order` has something to order by.

**Done when:** three tables exist, and a second retrieval adds rows rather than replacing
them.

---

### Task 4 — GATED: repoint the four entities

**Blocked on:** Task 3, and a **production** host and tenant. Do not start against `impl-`.

**When unblocked:** replace `PLACEHOLDER.reference.legal_entity` and
`PLACEHOLDER.reference.legal_entity_hierarchy` in the four YAMLs with the landed table names;
add each to `active_sources` in the **same** change, since the model refuses a live binding
whose source is inactive. Delete `_WD_PLANNED` from Task 2 and derive the column check from
the bindings instead.

**Done when:** `spec.active_table_bindings` reports all four active, and `verify_repo` stays
green.

---

### Task 5 — GATED: prove effectivity actually closes an edge

**Blocked on:** Task 4.

**When unblocked:** this is the task the whole design exists for, and it must be proven by
**mutation, not inspection**. Land a retrieval; land a second in which one company has moved
between consolidation groups; assert that `esat_legal_entity_hierarchy_effectivity` closes
the old edge and opens the new one, and that the old edge is still queryable.

**Done when:** "who owned this in March" and "who owns it today" are both answerable from
one table, demonstrated on a changed retrieval rather than asserted.

---

### Task 6 — GATED: delete the worksheet

**Blocked on:** Task 5 passing.

**When unblocked:** delete `docs/legal_entity_roster_worksheet.csv` and the OPEN_ITEMS row
describing it as ungated. Success criterion 3 of the spec is that it is **deleted, not filled
in** — a hand-maintained worksheet duplicating a retrievable source is the second authority
this repo has spent a fortnight removing.

**Done when:** the file is gone, `verify_repo`'s docs-citation gate is green (it will catch
any live file still pointing at it), and LEG-1/LEG-7 are answered or explicitly still open.

---

## Self-review

**Spec coverage.** The `ref_` boundary (spec §"The boundary") needs no code — it is a rule
about what gets an entity, and no task creates a `ref_` table. The ledger-account hub and
`sal_` link are explicitly out of scope in the spec and appear in no task. Cost centres,
regions and custom organisations likewise. Jurisdiction-from-prefix is refused by the spec
and no task parses it.

**One thing the spec asks for that this plan does not build:** `sat_legal_entity_details`
needs `company_number`, which Workday does not supply. Task 4 repoints the satellite and
leaves that column empty and visible, which is the spec's stated position — it is not an
omission here.

**Type consistency.** `landing_rows` returns dicts keyed by the same tuples Task 2 gates and
Task 4 reads. `retrieved_at` is a string throughout, ISO-8601 with a `Z`, never a datetime —
NDJSON has no date type and a silent format change is how two batches stop ordering.


---

## What actually happened, 7 September

**Task 1** landed as planned: `landing_rows`, three column tuples, and
`tools/land_workday_references.py`. Four checks, all four mutation-proven — dropping the
provenance stamp, emitting membership as an array, leaking the arrays into the organisation
shape, and returning `[]` for an unknown kind.

**Task 2** landed with one addition the plan did not call for: **the hand-kept
`_WD_PLANNED` list now enforces its own expiry.** The plan said the list is deleted in
Task 4; nothing would have made that happen. A third check asserts every legal-entity
binding still names `PLACEHOLDER`, so the moment Task 4 repoints one, the build fails and
names the list to delete.

**And that expiry check was wrong on its first attempt.** It used `any(... PLACEHOLDER ...)`,
which stays true until *all four* bindings are repointed — so it slept through the event it
exists to notice. Its own prose said "a legal-entity binding names a real table now". Found
by mutating it and watching nothing happen; now `all`, and the mutation fails as it should.
That is twice in two days that a check has been weaker than the sentence describing it, both
caught the same way.
