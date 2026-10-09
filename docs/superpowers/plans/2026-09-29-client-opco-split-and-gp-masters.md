# Splitting hub_organisation, and mastering the parties from Great Plains — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `hub_organisation` into `hub_client` and `hub_operating_company`, and bind Great Plains' customer and vendor masters — the only source of party *names* this estate has.

**Architecture:** The split is a partition of a literal that is already declared: every binding carries a `reference_type` key literal, and it separates the two populations exactly, four bindings each. The seven children partition the same way, each resolving to exactly one side. GP customers then bind to `hub_client` and GP vendors to `hub_supplier`, bringing 345 clients and 15,665 vendors in with names, class, status, currency and terms.

**Tech Stack:** Python 3.11 and 3.13, PySpark on Databricks serverless, Unity Catalog, Lakeflow Declarative Pipelines, Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-09-28-client-operating-company-split-design.md` (branch `spec/client-operating-company-split`, APPROVED 29 September)

## Preconditions

1. **`feat/keyed-kind-staging` must be merged first**, and `fix/stream-generation` before it. This plan's migration combines with that branch's eleven version bumps — 13 version operations rather than 20 — and Task 6 below assumes the staging loader is in place.
2. The version numbers in Task 6 are read from `metadata/entities/` at execution time, **not** from this document. The staging branch bumps eleven entities; whichever lands first shifts the other's numbers.

## Global Constraints

- **No check code in this plan.** Plan-authored `check()` code has failed 5/5 times in this repo. Every test step names the assertion and the mutation that must fail it; the implementer writes the code against the live file.
- **The test idiom is NOT pytest.** `check(name, condition, detail)` in `tests/test_accelerator.py`, inserted BEFORE the `if FAILURES:` summary block.
- **An exception inside a check's CONDITION aborts the whole suite** — it exits 1 having asserted nothing, which is worse than a red check. Compute into a sentinel inside `try/except` first, then compare. The sentinel must be a value no expected result can equal.
- **A mutation that "exits 1" is not a proof** — an abort exits 1 too. Every new check is proven by a mutation producing `PASS + FAIL == the full check count` AND printing the summary block.
- **Choose each mutation from the failure feared**, not from the check's own logic.
- **Never write a check count down as a literal.**
- **`_raises` is defined TWICE** in `tests/test_accelerator.py`; the later returns False for anything that is not a `ValueError`, so a `SpecError` refusal written through it silently asserts nothing. Use a locally-named helper.
- **Both Python legs, then `verify_repo`:** `uv run --frozen --python 3.11 python tests/test_accelerator.py`, `uv run --python 3.13 --with pyyaml --with jsonschema --with referencing python tests/test_accelerator.py`, `uv run --frozen --python 3.11 python verify_repo.py`. Revert `uv.lock` drift before committing.
- **`delta.appendOnly = true`** on every vault table: no column renamed or narrowed in place. A shape change is a new `_rev<N>`.
- **Hash values must not move for any existing binding.** `key_scope_of()` returns the binding name, never the entity name, so the split must be key-neutral. Asserted in Task 1.
- **A CTAS copies shape, not masks.** `checks/load_hubs.py` and `checks/load_satellites.py` both apply masks by `ALTER` between create and insert. Any new satellite declaring a mask inherits that, and the wiring is asserted — do not bypass it.

## Review Focus

The five failure modes the spec implies that no single task's tests would otherwise exercise:

1. **A hash key moves during the split**, so every existing child's parent key stops joining and the vault fans out silently rather than failing. Owned by Task 1.
2. **A binding lands on the wrong hub**, re-creating the conflation one binding at a time — the `reference_type` literal is the discriminator and must be asserted as a partition, both halves. Owned by Task 1.
3. **A child's parent-key lookup breaks.** `spec.parent_key_components` requires the parent hub to declare a binding of the SAME NAME as the child's, and raises when it does not — loudly enough to abort the suite. Owned by Task 2.
4. **The GP vendor key merges unrelated suppliers.** 630 vendor ids appear in more than one operating company and are different companies. Owned by Task 5.
5. **A GP party and a Fieldglass party for the same real company hash to the same key by accident**, silently asserting a mapping nobody has made. Owned by Task 4.

---

### Task 1: Split the hub, and prove no key moved

**Files:**
- Create: `metadata/entities/hub_client.yml`, `metadata/entities/hub_operating_company.yml`
- Delete: `metadata/entities/hub_organisation.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `naming.KEYED_KINDS`, `spec.key_scope_of`
- Produces: two entities named `client` and `operating_company`, both `kind: hub`, both `business_keys: [reference_type, reference_id]`

- [ ] **Step 1: Record the baseline hash-key expressions BEFORE changing anything**

Render, for every entity and binding in the model, the expression `spec.hash_key_columns()` produces, into a file committed in its own commit. This is the evidence Step 4 compares against; after the split there is nothing left to compare to. The keyed-kind staging branch did exactly this in `tests/golden_key_expressions.json` — follow that precedent and extend rather than duplicate it if the format allows.

- [ ] **Step 2: Write the two hubs**

Both carry `kind: hub`, `key_style: tenant_scoped` if and only if `hub_organisation` did, `business_keys: [reference_type, reference_id]`, and `sensitivity` copied from the original. Bindings split by their declared `reference_type` literal, which is the discriminator and requires no judgement:

| hub | bindings | literal |
|---|---|---|
| `hub_client` | `FIELDGLASS_US`, `BUSINESS_VAULT_GIE` | `Fieldglass_Buyer_Code` |
| | `CLIENT_PORTAL`, `STRIIVE_EU` | `Portal_Client_Code` |
| `hub_operating_company` | `GP_US`, `GP_US_HIST`, `UKG_US`, `BUSINESS_VAULT` | `Organization_Reference_ID` |

Copy each binding's `key_columns`, `key_literals`, `payload`, `applied_dts_column`, `cdc_op_column`, `manifest_column`, `dedup_by`, `dedup_order`, `cast`, `conform` and `freshness_sla_hours` verbatim. A binding that changes shape in transit is a re-key.

- [ ] **Step 3: Run the suite and read what breaks**

Run: `uv run --frozen --python 3.11 python tests/test_accelerator.py`
Expected: FAILURES from checks that name `organisation`, and possibly an ABORT — `spec.hash_key_columns` is called outside a check condition at roughly line 1524 and raises when a child's parent has no binding of the child's name. If it aborts, Task 2 is the fix and the two tasks must land together; say so rather than working around it.

- [ ] **Step 4: Add the key-invariance check**

Assert: for every binding that existed before the split, the hash-key expression is byte-identical to the Step 1 baseline.
**Mutation that must fail it:** make `key_scope_of()` return the entity name instead of the binding name. If the check still passes it is not reading what it claims to. This is Review Focus 1, and it is the one that fails silently rather than loudly.

- [ ] **Step 5: Add the partition checks**

Assert both halves, because "no overlap" alone passes on two empty sets: (a) the union of the two hubs' bindings is exactly the eight `hub_organisation` had; (b) their intersection is empty. Derive the sets from the model, never from a hand-typed list.
**Mutations:** move one binding to the other hub; drop one binding entirely.

- [ ] **Step 6: Add the literal-consistency check**

Assert: every binding on `hub_client` declares `Fieldglass_Buyer_Code` or `Portal_Client_Code`, and every binding on `hub_operating_company` declares `Organization_Reference_ID`. This is the discriminator; a binding whose literal contradicts its hub re-creates the conflation.
**Mutation:** change one binding's literal without moving it.

- [ ] **Step 7: Confirm the no-payload-on-a-hub refusal already covers both new hubs**

The spec requires that neither hub carry descriptive attributes — name, class and status
belong on satellites. **This is already enforced**, by the structural refusal that no
hub or link may declare `payload`, which `tests/test_accelerator.py` exercises against the
whole model. Confirm it applies to the two new entities and add nothing: a second check
asserting the same rule is a second place for it to drift, and this repo has been bitten by
exactly that (BUSINESS_KINDS, the system-column set, the append-only prefix list).

If the existing refusal does NOT reach them — say so rather than adding a parallel check,
because that would mean the refusal is scoped by name rather than by kind and the real
defect is there.

- [ ] **Step 8: Both Python legs green, then commit**

```bash
git add metadata/entities/hub_client.yml metadata/entities/hub_operating_company.yml
git rm metadata/entities/hub_organisation.yml
git add tests/test_accelerator.py
git commit -m "feat(model): split hub_organisation into client and operating company"
```

---

### Task 2: Re-point the seven children

**Files:**
- Modify: `metadata/entities/nhl_invoice_line.yml`, `nhl_timesheet_line.yml`, `lnk_client_job_request.yml`, `nhl_general_journal_line.yml`, `nhl_general_journal_line_closed_year.yml`, `nhl_journal_line.yml`, `nhl_payroll_detail.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: the two hubs from Task 1
- Produces: seven children whose `parents:` name `client` or `operating_company` instead of `organisation`

- [ ] **Step 1: Re-point each child, by the binding that feeds its leg**

Verified 29 September — every child resolves to exactly one side, and no child needs both:

| child | binding | new parent |
|---|---|---|
| `nhl_invoice_line` | FIELDGLASS_US | `client` |
| `nhl_timesheet_line` | STRIIVE_EU | `client` |
| `lnk_client_job_request` | STRIIVE_EU | `client` |
| `nhl_general_journal_line` | GP_US | `operating_company` |
| `nhl_general_journal_line_closed_year` | GP_US_HIST | `operating_company` |
| `nhl_journal_line` | UKG_US | `operating_company` |
| `nhl_payroll_detail` | UKG_US | `operating_company` |

Each child's `parents:` list and its bindings' `parent_keys:` mapping both change — the map is keyed by parent entity name.

- [ ] **Step 2: Add the parent-key-lookup check**

Assert: for every child and every binding, the parent hub named in `parents:` declares a binding of the SAME NAME. `spec.parent_key_components` requires this and raises when it is false.
**Mutation:** re-point one child to the other hub. It must produce a RED CHECK, not an abort — which means the check must compute into a sentinel first. This is Review Focus 3.

- [ ] **Step 3: Add the no-straddle check**

Assert: no child declares both `client` and `operating_company` as parents. True today of all seven; if it stops being true that is a modelling decision and must be made explicitly rather than inferred.
**Mutation:** add the second parent to one child.

- [ ] **Step 4: Both Python legs green, then commit**

```bash
git add metadata/entities/ tests/test_accelerator.py
git commit -m "feat(model): the seven children follow their binding to the right hub"
```

---

### Task 3: `hub_organisation` must not survive

**Files:**
- Modify: `src/accelerator/spec.py` (validation), `verify_repo.py`
- Test: `tests/test_accelerator.py`

- [ ] **Step 1: Add the refusal**

Refuse a model that declares an entity named `organisation`, with a message naming the two hubs that replaced it and why. Two hubs and a third holding the union is the worst outcome available, and it is what a partial revert produces.

- [ ] **Step 2: Add the check**

Assert: the refusal fires on a model declaring `organisation`, and the message names both replacements.
**Mutation:** accept the name. Note the `_raises` hazard — `SpecError` is not a `ValueError`; use a locally-named helper.

- [ ] **Step 3: Both Python legs green, then commit**

```bash
git add src/accelerator/spec.py verify_repo.py tests/test_accelerator.py
git commit -m "feat(spec): refuse the hub the split replaced"
```

---

### Task 4: Great Plains customers

**Files:**
- Modify: `metadata/entities/hub_client.yml` (add the `GP_CUSTOMER` binding)
- Create: `metadata/entities/sat_client_details.yml`, `metadata/entities/msat_client_address.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `hub_client` from Task 1
- Produces: one binding and two satellites reading `01_usnc_bronze_dev.great_plains_raw.rm00101` and `rm00102`

- [ ] **Step 1: Add the `GP_CUSTOMER` binding**

`reference_type = GP_CUSTOMER` as a `key_literal`; `reference_id` from `custnmbr`. Reads `01_usnc_bronze_dev.great_plains_raw.rm00101`.

**`custnmbr` is NOT scoped by `input_db`, and that is measured rather than assumed.** 345 distinct `custnmbr` against 346 distinct `(input_db, custnmbr)`: one collision, `KCCA`, which is "KIMBERLY CLARK CANADA" in `BARM` and "Kimberly Clark Canada" in `CER` — the same customer in two sets of books, which a shared key correctly merges.

- [ ] **Step 2: Write `sat_client_details`**

Payload `custname`, `custclas`, `inactive`, `curncyid`, `pymtrmid` — all verified present in `rm00101`. **Versioned, not current-state:** 889 rows over 345 customers across five deliveries, so `applied_dts_column` takes the delivery timestamp and the satellite stores versions.

`custclas` is **carried, not interpreted.** Nothing in the raw vault decides what `VMS`, `GROUP FILL` or `TPV` mean.

- [ ] **Step 3: Write `msat_client_address`**

Reads `rm00102`, multi-active on `adrscode` as the `mas_key`. Payload `address1`, `address2`, `city`, `country`, `phone1`, `cntcprsn`.

**`(custnmbr, adrscode)` is globally unique — measured.** 1,910 rows over 505 distinct `(input_db, custnmbr, adrscode)` AND 505 distinct `(custnmbr, adrscode)`: no cross-company collision, so the address key needs no `input_db` scope even though its parent's source rows carry one.

- [ ] **Step 4: Add the no-accidental-mapping check**

Assert: a GP customer and a Fieldglass buyer for the same real company hash to DIFFERENT keys. They must, until somebody declares the mapping — `AMEREN` and `AEE1` are the same company to a human and nothing in the model says so.
**Mutation:** make `reference_type` stop participating in the hash. This is Review Focus 5, and a check that they collide by accident is a check that the scope works.

- [ ] **Step 5: Add the inactive-customer check**

Assert: a customer with `inactive` set loads and is marked inactive, rather than being filtered out. `rm00101.INACTIVE` is set on real records.
**Mutation:** filter inactive rows at staging.

- [ ] **Step 6: Both Python legs green, then commit**

```bash
git add metadata/entities/ tests/test_accelerator.py
git commit -m "feat(model): Great Plains customers bind to hub_client"
```

---

### Task 5: Great Plains vendors

**Files:**
- Modify: `metadata/entities/hub_supplier.yml` (add the `GP_VENDOR` binding)
- Create: `metadata/entities/sat_supplier_details.yml`
- Test: `tests/test_accelerator.py`

- [ ] **Step 1: Add the `GP_VENDOR` binding, scoped by `input_db`**

`supplier_tenant` from `input_db`, `supplier_code` from `vendorid`. Reads `01_usnc_bronze_dev.great_plains_raw.pm00200`.

**THE SCOPE IS LOAD-BEARING AND IT IS MEASURED.** 32,606 rows, 15,665 distinct `vendorid`, **16,295** distinct `(input_db, vendorid)`. 630 vendor ids appear in more than one operating company and they are different companies: `0000032303` is "Premiere Credit of North America LLC" in `CER`, "WNL Technologies, LLC" in `CSS`, and "McKim & Creed, Inc." in `GGI`. GP vendor ids are per-company sequences, not global identifiers. Keying on `vendorid` alone merges 630 sets of unrelated suppliers and fans out every join to them.

- [ ] **Step 2: Write `sat_supplier_details`**

Payload `vendname`, `venddba`, `vendshnm`, `vendstts`, `curncyid`, `pymtrmid` — all verified present in `pm00200`. Versioned: 32,606 rows over 15,665 vendors, ≈2.08 per vendor, the same shape as the customer master.

- [ ] **Step 3: Add the vendor-scope check**

Assert: two GP vendors sharing a `vendorid` in different `input_db` produce DIFFERENT hash keys.
**Mutation:** set the binding's `key_columns` to `[vendorid, vendorid]`. It must go red. This is Review Focus 4, and without it the defect is invisible — 630 wrong merges produce a smaller hub, not an error.

**Do NOT use the obvious mutation, which is the one this step originally named.** Dropping `input_db` to leave `key_columns: [vendorid]` does not make the check fail — it makes the SUITE ABORT, at `spec.validate`:

```
accelerator.spec.SpecError: supplier/GP_VENDOR: key_columns plus key_literals must map 1:1
to business_keys (1 key_columns + 0 key_literals vs 2 business_keys)
```

Measured 29 September: **11 PASS, 0 FAIL, no summary block, exit 1.** The model never finishes loading, so the vendor-scope check is never reached, and an abort exits 1 exactly as a failure does. Recording that as "it must go red" proves the check can fail when it proves nothing at all — the arity refusal fires first and hides it.

`[vendorid, vendorid]` keeps the 1:1 arity so `validate` passes, while making the scope degenerate: both key components are the same column, so two vendors sharing a `vendorid` in different `input_db` hash identically, which is precisely the defect. Measured at the same commit: **PASS + FAIL == the full check count, summary block printed**, the vendor-scope check red and naming `0000032303`, plus the digest check red because the model genuinely changed.

- [ ] **Step 4: Both Python legs green, then commit**

```bash
git add metadata/entities/ tests/test_accelerator.py
git commit -m "feat(model): Great Plains vendors, scoped by operating company"
```

---

### Task 6: The combined migration

**Files:**
- Modify: every entity file whose physical version changes
- Modify: `docs/superpowers/plans/2026-09-29-keyed-kind-staging-runbook.md` (extend, do not fork)
- Test: `tests/test_accelerator.py`

**READ THE VERSIONS FROM `metadata/entities/` AT EXECUTION TIME.** They depend on whether the keyed-kind staging branch has landed. This plan does not restate them, because a version table in a document is a number that goes stale between writing and running — which this repo has already been bitten by.

- [ ] **Step 1: Bump the versions**

`hub_client` and `hub_operating_company` are new, so they start at version 1. The seven children take a new version because `organisation_hk` becomes `client_hk` or `operating_company_hk` — a column rename, which `delta.appendOnly` forbids in place.

- [ ] **Step 2: Update every check that pins a version**

Find them all; do not delete them. They are what makes a version change deliberate.

- [ ] **Step 3: Extend the existing runbook rather than writing a second one**

The keyed-kind staging runbook already carries the operational facts and they all still apply: replacing a stable view **drops its grants** (and `information_schema` then reports the view as having zero columns rather than as forbidden, so it presents as an empty table); cutover and retirement both need the owning service principal; both tools default to a dry run; and `grant_vault_access` exits 1 on 18 phantom statements until that is fixed separately.

Add what is specific to this migration: `hub_organisation` is retired at the end, after both new hubs are live and all seven children have cut over, and `retire_vault_version.py` will refuse it until it is neither live nor declared.

- [ ] **Step 4: Add the version-consistency check**

Assert: no entity is at a version whose physical table the stable view does not name, unless explicitly listed as mid-migration.
**Mutation:** bump one entity without updating its expected physical name.

- [ ] **Step 5: Both Python legs green, then commit**

```bash
git add metadata/entities/ tests/ docs/superpowers/plans/
git commit -m "feat(vault): version the split and the GP masters for migration"
```

---

## Not in this plan

- **Executing the migration.** Task 6 produces the declarations and the runbook; running it is an operator action needing the service principal for every cutover and every retirement.
- **The gold views.** They are the point of the exercise and are genuinely small once a satellite carries a name — a separate spec once this lands.
- **`sy01200` and `sy06000`** (internet addresses, 182,176 rows; EFT/bank, 40,395). Bank detail is a sensitivity question before it is a modelling one.
- **`PM00100`**, the vendor class master: absent from Bronze, and not needed (Adrian, 28 September).
- **Joining Great Plains to Fieldglass.** GP knows `AMEREN` and `ID0002215`; Fieldglass knows `AEE1`. The mapping comes from GP (Adrian, 28 September) but the column is not yet identified. Until it is, the two masters sit side by side — which is the `reference_type` scope working, not failing.
- **AR and AP** — `rm20101`, `rm30101`, `pm20000`, `pm30200`, ~8.7M transaction rows, read by nothing in the vault.
