# Ameren Invoicing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Model Fieldglass invoicing as a Data Vault source and compute the Global Invoice Engine in Databricks, producing one gold row per invoice line with issued values frozen rather than recomputed.

**Architecture:** A union view over per-tenant Fieldglass invoice tables feeds two new hubs (`invoice`, `supplier`), two reused hubs (`organisation`, `worker`), one NHL and one satellite. A business-vault computed satellite applies the derivation rules. An append-only control table issues Invoice Date and Line Number **once** per invoice; gold reads that ledger and never derives those two fields.

**Tech Stack:** Python 3.11/3.13, PySpark on Databricks serverless, Lakeflow SDP pipelines, Declarative Automation Bundles, YAML entity metadata.

**Spec:** `docs/superpowers/specs/2026-09-24-ameren-invoicing-design.md`

## Global Constraints

- **Nothing loads.** Every Bronze binding in this plan is a placeholder. The invoice feed does not exist: measured 23 Sep 2026, **0 invoice tables and 0 `supplier_code` columns across all 302 tables in `01_usnc_bronze_dev.fieldglass_raw`.**
- **Tests are NOT pytest.** This repo uses `check(name, condition, detail)` in `tests/test_accelerator.py`. Run with `uv run --frozen python tests/test_accelerator.py`.
- **Every new check must be mutation-proven**: change the code so the check ought to fail, observe it failing, change it back. A check that cannot fail is worse than no check. This repo has found six whose stated guarantee exceeded what their fixture could detect.
- **Never run `git checkout <file>`** to undo a mutation test. It has destroyed uncommitted work four times on this programme. Copy the file aside and copy it back.
- `verify_repo.py` must pass: `uv run --frozen python verify_repo.py`. It currently runs 1107 checks. **Never write a check count downward.**
- Entity `kind` prefixes are reserved: `hub_`/`sat_`/`msat_`/`csat_`/`esat_`/`nhl_`/`hal_`/`lnk_`/`qtn_`/`stg_`. The gold table carries none of them.
- Validation refuses: a hub or link carrying descriptive attributes, an NHL without a transaction key, a multi-source satellite, a link-of-a-link, a reused prefix.
- Money columns carry `governance.mask_money`. A masked column reads NULL outside the privileged group, so **any gate comparing amounts must run as an identity inside it**.
- The gold table is `invoice_line_export` and is **not** named for Ameren: it carries every MSP client, discriminated by the client hub.
- Commit after every task. Branch, never push to `main` — it is protected and requires a code-owner review.

## Review Focus

Five conditions the spec implies that no task's happy path exercises. Each has a test pinned to the task that owns the code.

1. **An invoice line with a blank supplier.** Measured: 4,287 of 182,914 timesheet rows have a blank `supplier` while `parent_supplier` is never blank. A line that cannot name its supplier must quarantine, not hash a blank into a business key — a hub row keyed on empty string silently collects every unattributable line. *Pinned to Task 3.*
2. **A line added to an already-issued invoice.** The ledger froze line numbers 1..n; a late line has no number. It must not renumber the others and must not silently vanish. *Pinned to Task 7.*
3. **A Task Code with no pipe.** AME011 parses "the value after `|`". `A0625|I-APMS-107000-CE` has one; a bare `I-ITDT-107000` does not. Returning empty string would put a blank Task Number on a customer invoice. *Pinned to Task 4.*
4. **An invoice carrying tax.** AME008 aggregates tax onto a final TAX line and still reconciles. The only sample has **one TAX line in 555**, so this path has never executed. *Pinned to Task 4 and Task 8.*
5. **Two clients using the same supplier code.** `hub_supplier` is `tenant_scoped` precisely so this cannot merge them. If the tenant key is dropped or defaulted, two suppliers collapse into one row and **nothing fails**. *Pinned to Task 2.*

---

### Task 1: Source union for the invoice feed

**Files:**
- Modify: `metadata/source_unions.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Produces: a union named `fieldglass_us_invoice` resolving to `02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice`, which every source binding in Tasks 2–3 reads.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accelerator.py`, in the source-unions section:

```python
_su = yaml.safe_load((ROOT / "metadata" / "source_unions.yml").read_text())
_inv = [u for u in _su["unions"] if u["name"] == "fieldglass_us_invoice"]
check("an invoice source union is declared",
      len(_inv) == 1,
      [u["name"] for u in _su["unions"]])
check("the invoice union keys on buyer, the tenant column",
      _inv and _inv[0]["tenant_column"] == "buyer",
      _inv[0] if _inv else None)
check("the invoice union requires the columns its hubs key on",
      _inv and set(_inv[0]["required_columns"]) >= {"buyer", "invoice_id", "invoice_line_item_ref"},
      _inv[0].get("required_columns") if _inv else None)
check("the invoice union excludes backfill tables, which carry DOUBLE-typed columns (BRZ-14)",
      _inv and _inv[0]["exclude_pattern"] == "%_backfill",
      _inv[0].get("exclude_pattern") if _inv else None)
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL — `an invoice source union is declared` reports the existing union names without `fieldglass_us_invoice`.

- [ ] **Step 3: Add the union**

Append to the `unions:` list in `metadata/source_unions.yml`:

```yaml
  - name: fieldglass_us_invoice
    # THE FEED DOES NOT EXIST YET. Measured 23 September 2026: zero tables matching
    # invoice in 01_usnc_bronze_dev.fieldglass_raw, across all 302 tables. Ameren has
    # landed distributed_jobposting, jobposting, jobseeker, timesheet and worker --
    # no invoice. This union is declared now so the Bronze ask can name the exact
    # shape required, and so nothing has to be redesigned when the feed arrives.
    #
    # The table_pattern is the expectation, not an observation. If Bronze lands the
    # family under another name, this one line changes and nothing else does.
    view_schema: raw_vault
    source_schema: fieldglass_raw
    table_pattern: io_invoice_%
    exclude_pattern: '%_backfill'
    tenant_column: buyer
    # invoice_line_item_ref is here because it is nhl_invoice_line's transaction key.
    # A union that silently lost it would produce a well-formed stream of rows that
    # cannot be keyed -- the failure this file's header exists to prevent.
    required_columns: [buyer, invoice_id, invoice_line_item_ref]
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED`

- [ ] **Step 5: Commit**

```bash
git add metadata/source_unions.yml tests/test_accelerator.py
git commit -m "Declare the Fieldglass invoice source union"
```

---

### Task 2: The two new hubs, and the two reused ones

**Files:**
- Create: `metadata/entities/hub_invoice.yml`
- Create: `metadata/entities/hub_supplier.yml`
- Modify: `metadata/entities/hub_organisation.yml`
- Modify: `metadata/entities/hub_worker.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: the union from Task 1.
- Produces: hubs named `invoice`, `supplier`, and Fieldglass bindings on `organisation` and `worker`. Task 3's `nhl_invoice_line` declares `parents: [invoice, supplier, worker, organisation]` and must match these names exactly.

- [ ] **Step 1: Write the failing test**

```python
check("hub_invoice is tenant_scoped, so a second VMS cannot collide with Fieldglass ids",
      model["invoice"].key_style == "tenant_scoped",
      model["invoice"].key_style)
check("hub_supplier is tenant_scoped -- Review Focus 5: without it two clients' "
      "supplier codes merge into one hub row and nothing fails",
      model["supplier"].key_style == "tenant_scoped",
      model["supplier"].key_style)
check("hub_supplier declares its tenant key rather than leaving it implicit",
      model["supplier"].tenant_key == ["supplier_tenant"],
      model["supplier"].tenant_key)
check("hub_supplier keys on a CODE, never on the supplier name",
      "supplier_code" in model["supplier"].business_keys
      and not any("name" in k for k in model["supplier"].business_keys),
      model["supplier"].business_keys)
check("the Fieldglass buyer enters hub_organisation under its own reference_type, "
      "not as a Workday organisation id",
      any(s.key_literals.get("reference_type") == "Fieldglass_Buyer_Code"
          for s in model["organisation"].sources),
      [s.key_literals for s in model["organisation"].sources])
check("hub_worker gained a Fieldglass binding",
      any(s.name == "FIELDGLASS_US" for s in model["worker"].sources),
      [s.name for s in model["worker"].sources])
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL with `KeyError: 'invoice'` — the model has no such entity.

- [ ] **Step 3: Create `metadata/entities/hub_invoice.yml`**

```yaml
# PLACEHOLDER BRONZE TABLE. The invoice feed does not exist: measured 23 September
# 2026, zero invoice tables in 01_usnc_bronze_dev.fieldglass_raw. The binding below
# names the union view from metadata/source_unions.yml, which will resolve once
# Bronze lands the family. Nothing loads until then.
name: invoice
kind: hub
domain: pay_bill
# TENANT_SCOPED THOUGH IT LOOKS REDUNDANT TODAY, deliberately.
# A Fieldglass invoice id already embeds the buyer -- AEE1IN00123410 -- so keying on
# the id alone would be unique across all 54 clients right now. It is scoped anyway
# because this hub will take Beeline and VNDLY next, and neither is guaranteed to
# namespace its identifiers. Adding the scope later is a re-key of every row; adding
# it now costs one column.
key_style: tenant_scoped
business_keys: [invoice_tenant, invoice_reference]
tenant_key: [invoice_tenant]
sensitivity: internal
sources:
  - name: FIELDGLASS_US
    bronze_table: 02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice
    key_columns: [buyer, invoice_id]
    applied_dts_column: invoice_submit_date
```

- [ ] **Step 4: Create `metadata/entities/hub_supplier.yml`**

```yaml
# PLACEHOLDER BRONZE TABLE, and the key column does not exist yet either.
#
# THIS HUB IS KEYED ON A CODE THAT NO FEED CARRIES, AND THAT IS THE POINT.
# Measured 23 September 2026: zero supplier_code columns across all 302 tables in
# 01_usnc_bronze_dev.fieldglass_raw. What we do receive is a NAME, in two columns
# that disagree -- of 178,627 timesheet rows carrying a supplier, 178,617 have
# `supplier` and `parent_supplier` naming the same company spelled differently
# (Zempleo / Zempleo, Inc. -- Iconma, LLC / ICONMA LLC). Only ten rows name a
# genuinely different company.
#
# So a name cannot be this hub's key: the same company arrives spelled two ways
# within a single row. Binding to a code that has to be asked for fails loudly if
# the feed lands without it. Binding to the name would load, and would be wrong.
name: supplier
kind: hub
domain: party
key_style: tenant_scoped
business_keys: [supplier_tenant, supplier_code]
tenant_key: [supplier_tenant]
sensitivity: internal
sources:
  - name: FIELDGLASS_US
    bronze_table: 02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice
    key_columns: [buyer, supplier_code]
    applied_dts_column: invoice_submit_date
```

- [ ] **Step 5: Add the Fieldglass binding to `metadata/entities/hub_organisation.yml`**

Append to its `sources:` list:

```yaml
  # THE CLIENT IS AN ORGANISATION, NOT A NEW HUB.
  # This hub is already the client hub -- lnk_client_job_request links on client_code
  # -- and its (reference_type, reference_id) key exists to namespace identifier
  # spaces. A Fieldglass buyer code and a Workday organisation id can therefore live
  # in one hub without either asserting it is the other. Creating hub_client instead
  # would be a second opinion about who a client is.
  - name: FIELDGLASS_US
    bronze_table: 02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice
    key_columns: [buyer]
    key_literals:
      reference_type: Fieldglass_Buyer_Code
    applied_dts_column: invoice_submit_date
```

- [ ] **Step 6: Add the Fieldglass binding to `metadata/entities/hub_worker.yml`**

Append to its `sources:` list:

```yaml
  # FEDERATED, so a Fieldglass worker is its own identity and does NOT merge with the
  # same human's STRIIVE_EU, HR_EU or UKG_US row. That is this hub's declared intent:
  # no system is authoritative for a worker. Resolving the four into one person is a
  # same-as-link concern for the registry and Gold, not this hub's job.
  - name: FIELDGLASS_US
    bronze_table: 02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice
    key_columns: [worker_id]
    applied_dts_column: invoice_submit_date
```

- [ ] **Step 7: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED`

- [ ] **Step 8: Mutation-prove the tenant-scope check (Review Focus 5)**

```bash
cp metadata/entities/hub_supplier.yml /tmp/hub_supplier.bak
sed -i 's/key_style: tenant_scoped/key_style: federated/' metadata/entities/hub_supplier.yml
uv run --frozen python tests/test_accelerator.py    # MUST fail on the tenant_scoped check
cp /tmp/hub_supplier.bak metadata/entities/hub_supplier.yml
uv run --frozen python tests/test_accelerator.py    # passes again
```

**Do not use `git checkout` to restore.** It has destroyed uncommitted work four times here.

- [ ] **Step 9: Commit**

```bash
git add metadata/entities/ tests/test_accelerator.py
git commit -m "Add hub_invoice and hub_supplier; bind Fieldglass to organisation and worker"
```

---

### Task 3: The invoice line and header

**Files:**
- Create: `metadata/entities/nhl_invoice_line.yml`
- Create: `metadata/entities/sat_invoice_header.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: hubs `invoice`, `supplier`, `worker`, `organisation` from Task 2.
- Produces: `nhl_invoice_line` with payload names that Task 4's rule functions and Task 5's csat consume verbatim: `invoice_line_item_type`, `invoice_line_item_amount_supplier`, `tax_amount`, `msp_line_amount`, `invoice_amount`, `weekending_date`, `worker_first_name`, `worker_last_name`, `project_code`, `task_code`, `expenditure_type`, `expenditure_organization`, `currency`.

- [ ] **Step 1: Write the failing test**

```python
check("nhl_invoice_line's transaction key is the source line reference",
      model["invoice_line"].transaction_key == ["invoice_line_item_ref"],
      model["invoice_line"].transaction_key)
check("nhl_invoice_line links all four parties",
      set(model["invoice_line"].parents) == {"invoice", "supplier", "worker", "organisation"},
      model["invoice_line"].parents)
check("nhl_invoice_line carries BOTH amounts, so the MSP margin survives",
      {"invoice_amount", "invoice_line_item_amount_supplier"} <= set(model["invoice_line"].payload),
      model["invoice_line"].payload)
check("every money column on nhl_invoice_line is masked",
      all(c in model["invoice_line"].masks
          for c in ("invoice_amount", "invoice_line_item_amount_supplier",
                    "tax_amount", "msp_line_amount")),
      model["invoice_line"].masks)
check("a line that cannot name its supplier is quarantined -- Review Focus 1: a hub "
      "row keyed on a blank collects every unattributable line",
      "supplier_code_present" in model["invoice_line"].sources[0].expectations,
      model["invoice_line"].sources[0].expectations)
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL with `KeyError: 'invoice_line'`.

- [ ] **Step 3: Create `metadata/entities/nhl_invoice_line.yml`**

```yaml
# PLACEHOLDER BRONZE TABLE -- see hub_invoice.yml. The feed does not exist yet.
#
# AN NHL, NOT A LINK PLUS SATELLITE. An invoice line is a transaction: it happens
# once, it is never updated, and its payload is the event. That is what a
# non-historised link is for, and it is the shape nhl_timesheet_line and
# nhl_payroll_detail already use for the same reason.
name: invoice_line
kind: nhl
domain: pay_bill
key_style: federated
parents: [invoice, supplier, worker, organisation]
transaction_key: [invoice_line_item_ref]
sensitivity: financial
masks:
  invoice_amount: governance.mask_money
  invoice_line_item_amount_supplier: governance.mask_money
  tax_amount: governance.mask_money
  msp_line_amount: governance.mask_money
# BOTH AMOUNTS, DELIBERATELY. One Fieldglass line creates two obligations with two
# counterparties -- a receivable from the client and a payable to the supplier -- and
# the source states both along with the margin between them (122.52 billed, 120.00 to
# the supplier, 2.52 MSP). Carrying one discards a stated fact and forces someone to
# rebuild the margin later from two systems.
payload:
  - invoice_line_item_type
  - invoice_amount
  - invoice_line_item_amount_supplier
  - tax_amount
  - msp_line_amount
  - weekending_date
  - worker_first_name
  - worker_last_name
  - project_code
  - task_code
  - expenditure_type
  - expenditure_organization
  - currency
sources:
  - name: FIELDGLASS_US
    bronze_table: 02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice
    parent_keys:
      invoice: [buyer, invoice_id]
      supplier: [buyer, supplier_code]
      worker: [worker_id]
      organisation: [buyer]
    payload:
      - invoice_line_item_type
      - invoice_amount
      - invoice_line_item_amount_supplier
      - tax_amount
      - msp_line_amount
      - weekending_date
      - worker_first_name
      - worker_last_name
      - project_code
      - task_code
      - expenditure_type
      - expenditure_organization
      - currency
    applied_dts_column: invoice_submit_date
    # REVIEW FOCUS 1. Measured on the timesheet feed, which is the only supplier data
    # that exists: 4,287 of 182,914 rows carry a blank supplier. A blank hashed into a
    # business key produces one hub row that silently collects every unattributable
    # line, and every join to it succeeds. Quarantine instead.
    expectations: [supplier_code_present, invoice_amount_non_negative]
```

- [ ] **Step 4: Create `metadata/entities/sat_invoice_header.yml`**

```yaml
# PLACEHOLDER BRONZE TABLE -- see hub_invoice.yml.
#
# SINGLE SOURCE, because validation refuses a multi-source satellite. When Beeline
# and VNDLY arrive they get their own satellites on the same hub.
name: invoice_header
kind: sat
domain: pay_bill
parents: [invoice]
sensitivity: financial
masks:
  gross_invoice_amount: governance.mask_money
  net_invoice_amount: governance.mask_money
payload:
  - invoice_type
  - gross_invoice_amount
  - net_invoice_amount
  - currency
  - invoice_submit_date
  - original_invoice_id
descriptions:
  invoice_type: >-
    Fieldglass invoice type, IN in the sample. Retained because a credit note is a
    different document and must not be released as an invoice.
  gross_invoice_amount: >-
    The invoice total. AME003 repeats this on every line and AME008 asserts the lines
    sum to it -- so this column is the reconciliation target, not a convenience copy.
    Masked by governance.mask_money.
  net_invoice_amount: >-
    Total before tax. Masked by governance.mask_money.
  original_invoice_id: >-
    Present when this invoice supersedes another. Carried so a reissue can be
    distinguished from a first issue, which the issuance ledger in Task 6 needs.
sources:
  - name: FIELDGLASS_US
    bronze_table: 02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice
    key_columns: [buyer, invoice_id]
    payload:
      - invoice_type
      - gross_invoice_amount
      - net_invoice_amount
      - currency
      - invoice_submit_date
      - original_invoice_id
    applied_dts_column: invoice_submit_date
```

- [ ] **Step 5: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED`

- [ ] **Step 6: Commit**

```bash
git add metadata/entities/ tests/test_accelerator.py
git commit -m "Add nhl_invoice_line and sat_invoice_header"
```

---

### Task 4: The GIE rule functions

**Files:**
- Create: `src/accelerator/invoice_rules.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Produces, all pure and Spark-free so they are unit-testable:
  - `worker_name(last: str, first: str) -> str`
  - `line_description(last: str, first: str, week_ending: str, line_ref: str) -> str`
  - `description(invoice_id: str, last: str, first: str, week_ending: str, line_ref: str) -> str`
  - `line_type(source_type: str) -> str`
  - `task_number(task_code: str) -> str`
  - `module_value(line_item_type: str, ts: str, es: str, mi: str) -> str`
  - `reconciles(line_amounts: list[Decimal], gross: Decimal) -> bool`
- Task 5's csat and Task 8's export both call these. No rule is implemented twice.

- [ ] **Step 1: Write the failing test**

```python
from accelerator import invoice_rules as gr

# AME004 / AME005
check("AME005 builds the line description with pipe delimiters and Last, First",
      gr.line_description("Kaver", "Joseph", "2026/07/26", "AEE1TS00126533")
      == "Kaver, Joseph|2026/07/26|AEE1TS00126533",
      gr.line_description("Kaver", "Joseph", "2026/07/26", "AEE1TS00126533"))
check("AME004 prefixes the invoice id to the same structure",
      gr.description("AEE1IN00123410", "Kaver", "Joseph", "2026/07/26", "AEE1TS00126533")
      == "AEE1IN00123410|Kaver, Joseph|2026/07/26|AEE1TS00126533",
      gr.description("AEE1IN00123410", "Kaver", "Joseph", "2026/07/26", "AEE1TS00126533"))

# AME007
check("AME007 maps Tax to TAX and everything else to ITEM",
      (gr.line_type("Tax"), gr.line_type("TS"), gr.line_type("tax"), gr.line_type("MI"))
      == ("TAX", "ITEM", "TAX", "ITEM"),
      [gr.line_type(x) for x in ("Tax", "TS", "tax", "MI")])

# AME011 -- Review Focus 3
check("AME011 takes the value after the pipe",
      gr.task_number("A0625|I-APMS-107000-CE") == "I-APMS-107000-CE",
      gr.task_number("A0625|I-APMS-107000-CE"))
check("AME011 strips a trailing parenthesis",
      gr.task_number("A0625|I-APMS-107000-CE)") == "I-APMS-107000-CE",
      gr.task_number("A0625|I-APMS-107000-CE)"))
check("REVIEW FOCUS 3: a task code with NO pipe returns the whole value, not a blank -- "
      "a blank Task Number would reach a customer invoice",
      gr.task_number("I-ITDT-107000") == "I-ITDT-107000",
      gr.task_number("I-ITDT-107000"))
check("AME011 on an empty task code raises rather than returning a blank",
      _raises(lambda: gr.task_number("")),
      "expected ValueError")

# AME012 / AME013 module selection
check("module_value selects the field matching the line item type",
      gr.module_value("TS", "ts-val", "es-val", "mi-val") == "ts-val",
      gr.module_value("TS", "ts-val", "es-val", "mi-val"))
check("module_value raises on an unknown module rather than guessing",
      _raises(lambda: gr.module_value("XX", "a", "b", "c")),
      "expected ValueError")

# AME008 -- Review Focus 4
from decimal import Decimal as D
check("AME008 reconciles a single-line invoice",
      gr.reconciles([D("698.80")], D("698.80")), True)
check("REVIEW FOCUS 4: AME008 reconciles a multi-line invoice WITH TAX -- the real "
      "sample has one TAX line in 555 and cannot exercise this",
      gr.reconciles([D("600.00"), D("60.00"), D("38.80")], D("698.80")), True)
check("AME008 fails when the lines do not sum to gross",
      not gr.reconciles([D("600.00")], D("698.80")), False)
check("AME008 compares exactly, so float drift cannot pass a wrong invoice",
      not gr.reconciles([D("0.1"), D("0.2")], D("0.30000000000000004")), False)
```

Add this helper near the top of the test file if it does not already exist:

```python
def _raises(fn):
    try:
        fn()
    except Exception:
        return True
    return False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'accelerator.invoice_rules'`.

- [ ] **Step 3: Create `src/accelerator/invoice_rules.py`**

```python
"""The invoice rules from the Ameren workbook's `5 Business Rules` sheet.

PURE AND SPARK-FREE, deliberately. Every rule here is a decision about what reaches
a customer invoice, and a decision that can only be tested by running a pipeline is a
decision that does not get tested. These run in the offline suite.

RULE IDS ARE LOAD-BEARING. Each function names the AME id it implements so a change
here is traceable to the sheet that authorised it, and so a reviewer can tell whether
a change is a bug fix or a rewrite of an agreed rule.

TWO RULES ARE NOT HERE. AME002 (Invoice Date) and AME006 (Line Number) are issued
once and recorded, not derived -- see checks/invoice_issue.py and the spec's "Issued
values must be frozen" section. Putting them here would invite exactly the
recomputation that section forbids.
"""
from __future__ import annotations

from decimal import Decimal

#: AME011/AME012/AME013 -- the Fieldglass line item type codes that select a module.
_MODULES = ("TS", "ES", "MI")


def worker_name(last: str, first: str) -> str:
    """AME004/AME005: the worker as `Last, First`."""
    last = (last or "").strip()
    first = (first or "").strip()
    if not last and not first:
        raise ValueError("worker name has neither last nor first")
    return f"{last}, {first}"


def line_description(last: str, first: str, week_ending: str, line_ref: str) -> str:
    """AME005: worker, week-ending and line reference, pipe delimited."""
    return "|".join((worker_name(last, first), week_ending, line_ref))


def description(invoice_id: str, last: str, first: str,
                week_ending: str, line_ref: str) -> str:
    """AME004: the line description with the invoice id in front."""
    return "|".join((invoice_id, line_description(last, first, week_ending, line_ref)))


def line_type(source_type: str) -> str:
    """AME007: `Tax` becomes TAX, everything else becomes ITEM.

    Case-insensitive because the sheet writes `Tax` and the feed's own line item
    types are upper-case codes (TS, ES, MI). Matching case-sensitively would send
    a tax line out as an ITEM, which reconciles and is wrong.
    """
    return "TAX" if (source_type or "").strip().lower() == "tax" else "ITEM"


def task_number(task_code: str) -> str:
    """AME011: the task number from a task code.

    The sheet says "parse the value after | and remove closing parenthesis where
    applicable". A code with no pipe therefore has no "value after", and the sheet
    does not say what to do. RETURNING THE WHOLE VALUE is the only safe reading: the
    alternative puts a blank Task Number on a customer invoice, and a blank is not
    something Ameren AP can reject intelligently.

    An EMPTY code raises. That is a missing required field, not a parsing question,
    and it belongs in the exception route rather than on an invoice.
    """
    code = (task_code or "").strip()
    if not code:
        raise ValueError("task code is empty; AME010 makes it a required field")
    value = code.split("|", 1)[1] if "|" in code else code
    return value.rstrip(")").strip()


def module_value(line_item_type: str, ts: str, es: str, mi: str) -> str:
    """AME012/AME013: pick the value belonging to the line's module.

    Raises on an unknown module. Defaulting to one of the three would silently put
    another module's expenditure coding on the line, which reconciles and is wrong.
    """
    module = (line_item_type or "").strip().upper()
    if module not in _MODULES:
        raise ValueError(f"unknown module {line_item_type!r}; expected one of {_MODULES}")
    return {"TS": ts, "ES": es, "MI": mi}[module]


def reconciles(line_amounts: list[Decimal], gross: Decimal) -> bool:
    """AME008: the lines must sum EXACTLY to the invoice gross.

    Decimal, not float. `0.1 + 0.2 != 0.3` in binary floating point, and an invoice
    released on a tolerance is an invoice whose total nobody checked.
    """
    return sum(line_amounts) == gross
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`

- [ ] **Step 5: Mutation-prove three rules**

```bash
cp src/accelerator/invoice_rules.py /tmp/invoice_rules.bak
# AME007 case sensitivity
sed -i 's/.strip().lower() == "tax"/.strip() == "tax"/' src/accelerator/invoice_rules.py
uv run --frozen python tests/test_accelerator.py   # MUST fail on the AME007 check
cp /tmp/invoice_rules.bak src/accelerator/invoice_rules.py
# AME011 no-pipe branch (Review Focus 3)
sed -i 's/if "|" in code else code/if "|" in code else ""/' src/accelerator/invoice_rules.py
uv run --frozen python tests/test_accelerator.py   # MUST fail on Review Focus 3
cp /tmp/invoice_rules.bak src/accelerator/invoice_rules.py
# AME008 exactness
sed -i 's/== gross/- gross < Decimal("0.01")/' src/accelerator/invoice_rules.py
uv run --frozen python tests/test_accelerator.py   # MUST fail on the float-drift check
cp /tmp/invoice_rules.bak src/accelerator/invoice_rules.py
uv run --frozen python tests/test_accelerator.py   # passes again
```

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/invoice_rules.py tests/test_accelerator.py
git commit -m "Implement the AME business rules as pure functions"
```

---

### Task 5: The business-vault computed satellite

**Files:**
- Create: `metadata/entities/csat_invoice_line_gie.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `nhl_invoice_line` from Task 3, and the payload names it declares.
- Produces: `csat_invoice_line_gie`, whose payload Task 8's export projects.

- [ ] **Step 1: Write the failing test**

```python
check("csat_invoice_line_gie hangs off the invoice line",
      model["invoice_line_gie"].parents == ["invoice_line"],
      model["invoice_line_gie"].parents)
check("the csat carries the derived fields and NOT the issued ones -- invoice_date "
      "and line_number are frozen in the ledger, never computed here",
      {"description", "line_description", "line_type", "accounting_date",
       "task_number", "expenditure_type", "expenditure_organization"}
      <= set(model["invoice_line_gie"].payload)
      and not {"invoice_date", "line_number"} & set(model["invoice_line_gie"].payload),
      model["invoice_line_gie"].payload)
check("the csat's amount is masked like its source",
      "amount" in model["invoice_line_gie"].masks,
      model["invoice_line_gie"].masks)
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL with `KeyError: 'invoice_line_gie'`.

- [ ] **Step 3: Create `metadata/entities/csat_invoice_line_gie.yml`**

```yaml
# THE DERIVED HALF OF THE GIE OUTPUT. Applies AME003, AME004, AME005, AME007, AME008
# (selection only), AME009, AME011, AME012 and AME013 -- the rules that are genuine
# derivations. The logic itself is src/accelerator/invoice_rules.py so it can be unit
# tested; this satellite is where the results are stored.
#
# AME002 AND AME006 ARE NOT HERE, AND THAT IS THE DESIGN.
# Invoice Date and Line Number appear on a document sent to a customer. A satellite
# is recomputed; anything recomputed can change. If a re-run changed either value the
# customer's copy would no longer match ours and NOTHING WOULD FAIL -- every load
# succeeds, every gate stays green, and the first symptom is a dispute. They are
# issued once into control.ctl_invoice_issuance instead. See the spec's "Issued
# values must be frozen" section.
name: invoice_line_gie
kind: csat
domain: pay_bill
parents: [invoice_line]
sensitivity: financial
masks:
  amount: governance.mask_money
  invoice_amount: governance.mask_money
payload:
  - description
  - line_description
  - line_type
  - amount
  - invoice_amount
  - accounting_date
  - project_number
  - task_number
  - expenditure_type
  - expenditure_organization
  - rule_version
descriptions:
  description: AME004 -- invoice id, worker as Last First, week-ending and line ref, pipe delimited.
  line_description: AME005 -- the same without the invoice id.
  line_type: AME007 -- TAX where the source type is Tax, otherwise ITEM.
  amount: >-
    AME008 -- the line amount for an ITEM line. Tax is aggregated onto the final TAX
    line and all lines sum to the invoice gross. Masked by governance.mask_money.
  invoice_amount: >-
    AME003 -- the invoice gross, repeated on every line of the invoice. The
    reconciliation target, masked by governance.mask_money.
  accounting_date: AME009 -- from the week-ending date. See the open question on its format.
  project_number: AME010 -- the source project code, unchanged.
  task_number: >-
    AME011 -- the value after the pipe in the module's task code, trailing parenthesis
    removed. A code with no pipe yields the whole code rather than a blank.
  expenditure_type: AME012 -- the expenditure type belonging to the line's module.
  expenditure_organization: AME013 -- the expenditure organization belonging to the line's module.
  rule_version: >-
    The version of src/accelerator/invoice_rules.py that produced this row. Present so a
    rule change is visible in the data rather than only in git history.
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED`

- [ ] **Step 5: Commit**

```bash
git add metadata/entities/csat_invoice_line_gie.yml tests/test_accelerator.py
git commit -m "Add the GIE computed satellite"
```

---

### Task 6: The issuance ledger control object

**Files:**
- Modify: `src/accelerator/control_standard.py`
- Modify: `governance/control_objects.sql` (generated — emit, do not hand-edit)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Produces: `ctl_invoice_issuance` in the `control` schema, with columns
  `invoice_tenant`, `invoice_reference`, `invoice_line_item_ref`, `line_number`,
  `invoice_date`, `issued_at`, `issued_by_run_id`. Task 7 writes it; Task 8 reads it.

- [ ] **Step 1: Write the failing test**

```python
check("ctl_invoice_issuance is a declared control object",
      "ctl_invoice_issuance" in cs.CONTROL_OBJECTS,
      sorted(cs.CONTROL_OBJECTS))
check("the issuance ledger is append-only -- the freeze is enforced by a gate, "
      "not by convention",
      cs.CONTROL_OBJECTS["ctl_invoice_issuance"]["append_only"] is True,
      cs.CONTROL_OBJECTS["ctl_invoice_issuance"])
check("the ledger keys a line uniquely, so a reissue cannot duplicate one",
      cs.CONTROL_OBJECTS["ctl_invoice_issuance"]["grain"]
      == ["invoice_tenant", "invoice_reference", "invoice_line_item_ref"],
      cs.CONTROL_OBJECTS["ctl_invoice_issuance"]["grain"])
check("the generated control DDL contains the issuance table",
      "ctl_invoice_issuance" in (ROOT / "governance" / "control_objects.sql").read_text(),
      "missing from generated DDL")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL — `ctl_invoice_issuance` is not in `CONTROL_OBJECTS`.

- [ ] **Step 3: Declare the object in `src/accelerator/control_standard.py`**

Add to the `CONTROL_OBJECTS` mapping, following the shape of `ctl_key_derivation`:

```python
    # THE INVOICE ISSUANCE LEDGER. Written once per invoice line, never updated.
    #
    # WHY A CONTROL TABLE AND NOT A COLUMN IN GOLD. Invoice Date and Line Number go
    # on a document sent to a customer. Gold is a projection and is rebuilt; anything
    # rebuilt can change. If a re-run renumbered an invoice's lines, Ameren AP would
    # be reconciling against a document that no longer matches ours -- and nothing
    # would fail, because every load would succeed and every gate would stay green.
    #
    # This is the WDJ-2 argument in a new place: a value that can be recomputed
    # cannot serve as a stable external reference. So it is issued once, recorded,
    # and read thereafter.
    "ctl_invoice_issuance": {
        "append_only": True,
        "grain": ["invoice_tenant", "invoice_reference", "invoice_line_item_ref"],
        "columns": [
            ("invoice_tenant", "STRING", "The buyer code, matching hub_invoice's tenant key."),
            ("invoice_reference", "STRING", "The Fieldglass invoice id."),
            ("invoice_line_item_ref", "STRING", "The line's source reference; AME006 orders by it."),
            ("line_number", "INT", "AME006. Assigned once, in invoice_line_item_ref order."),
            ("invoice_date", "DATE", "AME002. Assigned once at issuance."),
            ("issued_at", "TIMESTAMP", "When this row was written. OUR clock, not the source's."),
            ("issued_by_run_id", "STRING", "The job run that issued it, for audit."),
        ],
    },
```

- [ ] **Step 4: Regenerate the DDL and commit both**

The SQL is generated, not hand-written. Regenerate it:

```bash
uv run --frozen python tools/emit_control_contract.py
git diff --stat governance/control_objects.sql
```

- [ ] **Step 5: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED` (its byte-gate asserts the generated DDL matches the generator)

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/control_standard.py governance/control_objects.sql tests/test_accelerator.py
git commit -m "Add the append-only invoice issuance ledger"
```

---

### Task 7: The GIE issuance task

**Files:**
- Create: `checks/invoice_issue.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `ctl_invoice_issuance` from Task 6, `nhl_invoice_line` from Task 3.
- Produces: `assign_line_numbers(lines, already_issued, invoice_date)` — a pure function returning the rows to append. The Spark wrapper calls it; the tests test it.

- [ ] **Step 1: Write the failing test**

```python
from checks import invoice_issue as gi

_lines = ["AEE1TS00126533", "AEE1TS00126501", "AEE1TS00126599"]

check("issuance numbers lines from 1 in a deterministic order",
      [r["line_number"] for r in gi.assign_line_numbers(_lines, {}, "2026-07-29")] == [1, 2, 3],
      gi.assign_line_numbers(_lines, {}, "2026-07-29"))
check("the deterministic order is the line reference, not input order",
      [r["invoice_line_item_ref"] for r in gi.assign_line_numbers(_lines, {}, "2026-07-29")]
      == sorted(_lines),
      [r["invoice_line_item_ref"] for r in gi.assign_line_numbers(_lines, {}, "2026-07-29")])
check("re-issuing an already-issued invoice writes NOTHING -- the frozen values stand",
      gi.assign_line_numbers(_lines, {r: i + 1 for i, r in enumerate(sorted(_lines))},
                             "2026-08-30") == [],
      "expected no rows")
check("REVIEW FOCUS 2: a line added after issuance gets the NEXT number and does not "
      "renumber the lines already sent to the customer",
      [ (r["invoice_line_item_ref"], r["line_number"])
        for r in gi.assign_line_numbers(_lines + ["AEE1TS00126600"],
                                        {r: i + 1 for i, r in enumerate(sorted(_lines))},
                                        "2026-08-30") ]
      == [("AEE1TS00126600", 4)],
      "expected only the new line, numbered 4")
check("issuance refuses to invent a date",
      _raises(lambda: gi.assign_line_numbers(_lines, {}, "")),
      "expected ValueError")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'checks.invoice_issue'`.

- [ ] **Step 3: Create `checks/invoice_issue.py`**

```python
"""AME002 and AME006: issue an invoice date and line numbers, exactly once.

WHY THIS IS NOT A TRANSFORMATION. Both values appear on a document sent to a
customer. Deriving them in a projection means re-deriving them on every run, and
anything re-derived can change: a re-export could renumber an invoice whose lines the
customer has already received. Nothing would fail -- the load succeeds, the gates stay
green -- and the first symptom would be a dispute about which copy is correct.

So this task issues once and records the result in control.ctl_invoice_issuance, which
is append-only and guarded by assert_append_only. Gold reads the ledger.

THE PURE FUNCTION IS THE POINT. assign_line_numbers() takes the lines, what has
already been issued, and the date, and returns the rows to append. It touches no
Spark, so the guarantee this module exists to provide is tested in the offline suite.
"""
from __future__ import annotations


def assign_line_numbers(line_refs: list[str],
                        already_issued: dict[str, int],
                        invoice_date: str) -> list[dict]:
    """Rows to append to the issuance ledger for one invoice.

    `line_refs` are every line reference on the invoice now. `already_issued` maps a
    line reference to the number it was issued with, empty for a first issue.
    Returns only the rows that are NOT yet issued.

    ORDERING IS BY LINE REFERENCE, not by input order. AME006 says "sequential within
    each invoice" and does not say sequential in what, so an unspecified order would
    make even the first issuance arbitrary -- two runs over the same data could
    number the same lines differently. The line reference is unique per line and is
    stable in the source, so it is the order.

    A LINE ADDED AFTER ISSUANCE CONTINUES THE SEQUENCE. It does not renumber what the
    customer already has. That is the whole reason the ledger exists.
    """
    if not invoice_date:
        raise ValueError("invoice_date is required; AME002 does not default it")
    next_number = max(already_issued.values(), default=0) + 1
    rows = []
    for ref in sorted(line_refs):
        if ref in already_issued:
            continue
        rows.append({
            "invoice_line_item_ref": ref,
            "line_number": next_number,
            "invoice_date": invoice_date,
        })
        next_number += 1
    return rows
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`

- [ ] **Step 5: Mutation-prove the freeze (Review Focus 2)**

```bash
cp checks/invoice_issue.py /tmp/invoice_issue.bak
# make it renumber everything, which is the defect the ledger exists to prevent
sed -i 's/if ref in already_issued:\n            continue//' checks/invoice_issue.py
python3 - <<'PY'
from pathlib import Path
p = Path("checks/invoice_issue.py"); t = p.read_text()
p.write_text(t.replace("        if ref in already_issued:\n            continue\n", ""))
PY
uv run --frozen python tests/test_accelerator.py   # MUST fail on re-issue and Review Focus 2
cp /tmp/invoice_issue.bak checks/invoice_issue.py
uv run --frozen python tests/test_accelerator.py   # passes again
```

- [ ] **Step 6: Commit**

```bash
git add checks/invoice_issue.py tests/test_accelerator.py
git commit -m "Issue invoice dates and line numbers exactly once"
```

---

### Task 8: The gold export and its release gates

**Files:**
- Create: `checks/invoice_export.py`
- Modify: `databricks.yml` (add the `gold_export_schema` variable to the variables block and to every target)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `invoice_rules.reconciles` from Task 4, the ledger from Task 6.
- Produces: `releasable(invoice_lines, gross)` returning `(bool, reason)`, and the gold table `${gold_catalog}.${gold_export_schema}.invoice_line_export`.

- [ ] **Step 1: Write the failing test**

```python
check("a gold export schema variable exists -- the repo names a gold catalog but no "
      "gold schema anywhere",
      "gold_export_schema" in _bundle["variables"],
      sorted(_bundle["variables"]))
check("every target that names a gold catalog also names the export schema",
      all("gold_export_schema" in (t.get("variables") or {})
          for t in _bundle["targets"].values()
          if (t.get("variables") or {}).get("gold_catalog")),
      {n: (t.get("variables") or {}).get("gold_export_schema")
       for n, t in _bundle["targets"].items()})

from checks import invoice_export as ge
from decimal import Decimal as D

check("a reconciling invoice is releasable",
      ge.releasable([D("600.00"), D("98.80")], D("698.80"))[0], True)
check("REVIEW FOCUS 4: an invoice whose TAX line completes the total is releasable",
      ge.releasable([D("600.00"), D("60.00"), D("38.80")], D("698.80"))[0], True)
check("a non-reconciling invoice is NOT released, and says why",
      ge.releasable([D("600.00")], D("698.80")) == (False, "lines sum to 600.00, gross is 698.80"),
      ge.releasable([D("600.00")], D("698.80")))
check("an invoice with no lines is not releasable -- an empty invoice reconciles to "
      "zero only if the gross is zero, and a zero invoice is not a document we send",
      not ge.releasable([], D("698.80"))[0], True)

_good_line = {f: "x" for f in ge.REQUIRED_FIELDS}
check("a complete line has no missing required fields",
      ge.missing_required(_good_line) == [], ge.missing_required(_good_line))
check("a blank task number is caught before it reaches a customer invoice",
      ge.missing_required({**_good_line, "task_number": "   "}) == ["task_number"],
      ge.missing_required({**_good_line, "task_number": "   "}))
check("the issued fields are required too -- a line with no line_number means "
      "issuance did not run, and must not be released",
      set(ge.missing_required({**_good_line, "line_number": "", "invoice_date": ""}))
      == {"line_number", "invoice_date"},
      ge.missing_required({**_good_line, "line_number": "", "invoice_date": ""}))
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL — `gold_export_schema` is not in the bundle variables.

- [ ] **Step 3: Add the bundle variable**

In `databricks.yml`, after the `gold_catalog` variable:

```yaml
  # WHERE GOLD TABLES LIVE. The repo has named a gold CATALOG since the start and has
  # never named a schema in it, because nothing was built there. The planned schemas
  # are reference, master, wd_fin_export, governance and control; wd_fin_export is
  # where export tables go. Gold's generated control DDL already writes to
  # ${control_schema}, so it agrees with that list and needs no change.
  gold_export_schema:
    description: >-
      Schema inside the gold catalog that holds export tables. No default: a
      wrong-but-plausible default is how a table lands in the wrong schema quietly,
      and an unset one fails at configuration time naming the target that lacks it.
    default: ""
```

Then add `gold_export_schema: wd_fin_export` to the `variables:` block of every target that sets `gold_catalog` — `weu_tds`, `weu`, `dev`, `uks_tds`, `uks`, `usnc_tds`, `usnc`, `aue_tds`, `aue`.

- [ ] **Step 4: Create `checks/invoice_export.py`**

```python
"""The gold projection and its release gates.

APPLIES AME001 (Invoice Number direct map) and AME010 (Project Number direct map) --
the two rules that are pure passthroughs and therefore have no derivation function in
invoice_rules.py. They are named here so every one of the thirteen rules is traceable to
code, which verify_repo asserts.

GATES AME008 per invoice, and required fields per line.

WHY A GATE AND NOT AN EXPECTATION. A line that fails a data-quality expectation is
quarantined and the rest of the load proceeds. An invoice whose lines do not sum to
its gross must not be PARTIALLY released: a well-formed invoice with a line missing
is worse than no invoice at all, because it looks correct and will be paid.

THIS MUST RUN AS AN IDENTITY INSIDE THE PRIVILEGED GROUP. Every amount here carries
governance.mask_money, so an identity outside that group reads NULL and the sum
compares equal to nothing. A gate that passes because it cannot see the data is the
failure mode this repo has met most often.
"""
from __future__ import annotations

from decimal import Decimal

from accelerator.invoice_rules import reconciles


#: Fields that must be present on every released line. A blank here does not fail a
#: load -- it reaches a customer invoice, where a missing project or task code is a
#: rejection at Ameren AP rather than an error anyone here sees.
REQUIRED_FIELDS = ("invoice_number", "description", "line_description", "line_type",
                   "amount", "accounting_date", "project_number", "task_number",
                   "expenditure_type", "expenditure_organization",
                   "invoice_date", "line_number")


def missing_required(line: dict) -> list[str]:
    """Required fields that are absent, empty or whitespace on one projected line.

    Checked as a GATE rather than a NOT NULL constraint because the remedy differs: a
    constraint fails the write and takes the whole invoice with it, while this routes
    the invoice to the exception set with a reason a human can act on.
    """
    return [f for f in REQUIRED_FIELDS
            if str(line.get(f, "") or "").strip() == ""]


def releasable(line_amounts: list[Decimal], gross: Decimal) -> tuple[bool, str]:
    """Whether one invoice may be released, and why not when it may not.

    An invoice with no lines is never releasable. `sum([]) == 0` would let a
    zero-gross invoice through, and a zero invoice is not a document anyone sends.
    """
    if not line_amounts:
        return False, "invoice has no lines"
    if not reconciles(line_amounts, gross):
        total = sum(line_amounts)
        return False, f"lines sum to {total}, gross is {gross}"
    return True, ""
```

- [ ] **Step 5: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED`
Run: `databricks bundle validate --target usnc_tds --profile hfig-usnc-tds`
Expected: the known `/Shared` warning (PLT-3) and no error.

- [ ] **Step 6: Mutation-prove the empty-invoice branch**

```bash
cp checks/invoice_export.py /tmp/invoice_export.bak
python3 - <<'PY'
from pathlib import Path
p = Path("checks/invoice_export.py"); t = p.read_text()
p.write_text(t.replace('    if not line_amounts:\n        return False, "invoice has no lines"\n', ""))
PY
uv run --frozen python tests/test_accelerator.py   # MUST fail on the empty-invoice check
cp /tmp/invoice_export.bak checks/invoice_export.py
uv run --frozen python tests/test_accelerator.py   # passes again
```

- [ ] **Step 7: Commit**

```bash
git add checks/invoice_export.py databricks.yml tests/test_accelerator.py
git commit -m "Add the gold export release gate and the gold export schema variable"
```

---

### Task 9: Wire the tasks into the job, and gate the wiring

**Files:**
- Modify: `resources/vault_job.yml`
- Modify: `verify_repo.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: everything above.
- Produces: job tasks `invoice_issue` and `invoice_export`, ordered after `business_vault`.

- [ ] **Step 1: Write the failing test**

```python
check("the GIE tasks are wired into the vault job",
      {"invoice_issue", "invoice_export"} <= _task_keys("vault_job"),
      sorted(_task_keys("vault_job")))
check("issuance runs BEFORE export -- gold reads the ledger, so it cannot run first",
      "invoice_issue" in [d["task_key"] for d in _depends_on("invoice_export")],
      _depends_on("invoice_export"))
check("export depends on the business vault, whose csat it projects",
      "business_vault" in [d["task_key"] for d in _depends_on("invoice_issue")],
      _depends_on("invoice_issue"))
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py`
Expected: FAIL — neither task key is present.

- [ ] **Step 3: Add the tasks to `resources/vault_job.yml`**

After the `business_vault` task:

```yaml
        # ISSUE BEFORE EXPORT, ALWAYS. Gold reads the issuance ledger; if export ran
        # first it would find no row for a new invoice and would either emit a null
        # line number or invent one. The dependency is the whole guarantee.
        - task_key: invoice_issue
          depends_on: [{task_key: business_vault}]
          spark_python_task:
            python_file: ../checks/invoice_issue.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--control-schema"
              - "${var.control_schema}"
              - "--business-vault-schema"
              - "${var.business_vault_schema}"

        - task_key: invoice_export
          depends_on: [{task_key: invoice_issue}]
          spark_python_task:
            python_file: ../checks/invoice_export.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--control-schema"
              - "${var.control_schema}"
              - "--business-vault-schema"
              - "${var.business_vault_schema}"
              - "--gold-catalog"
              - "${var.gold_catalog}"
              - "--gold-export-schema"
              - "${var.gold_export_schema}"
```

- [ ] **Step 4: Add a verify_repo gate for the rule-to-code trace**

In `verify_repo.py`, near the other documentation gates:

```python
# EVERY AME RULE MUST BE TRACEABLE FROM CODE TO THE SHEET THAT AUTHORISED IT.
#
# The thirteen rules come from a customer's workbook. A rule quietly dropped during
# implementation produces an invoice that is wrong in a way no test notices, because
# the test suite only knows about rules somebody remembered to implement. This asserts
# the ids are all still named in the code that implements them.
_AME_IDS = {f"AME{n:03d}" for n in range(1, 14)}
_ame_text = ((ROOT / "src" / "accelerator" / "invoice_rules.py").read_text()
             + (ROOT / "checks" / "invoice_issue.py").read_text()
             + (ROOT / "checks" / "invoice_export.py").read_text()
             + (ROOT / "metadata" / "entities" / "csat_invoice_line_gie.yml").read_text())
_ame_missing = sorted(i for i in _AME_IDS if i not in _ame_text)
check("every AME rule id is named in the code that implements it",
      not _ame_missing,
      f"{_ame_missing} -- a rule dropped in implementation is invisible: the suite "
      f"only tests rules somebody remembered to write")
```

- [ ] **Step 5: Run to verify it passes**

Run: `uv run --frozen python tests/test_accelerator.py` → `ALL CHECKS PASSED`
Run: `uv run --frozen python verify_repo.py` → `VERIFICATION PASSED`
Run: `databricks bundle validate --target usnc_tds --profile hfig-usnc-tds` → no error

- [ ] **Step 6: Mutation-prove the traceability gate**

```bash
cp src/accelerator/invoice_rules.py /tmp/invoice_rules.bak
sed -i 's/AME007/AMEXXX/' src/accelerator/invoice_rules.py
uv run --frozen python verify_repo.py     # MUST fail naming AME007
cp /tmp/invoice_rules.bak src/accelerator/invoice_rules.py
uv run --frozen python verify_repo.py     # passes again
```

- [ ] **Step 7: Commit and open a pull request**

```bash
git add resources/vault_job.yml verify_repo.py tests/test_accelerator.py
git commit -m "Wire the GIE tasks into the vault job and gate rule traceability"
GIT_TERMINAL_PROMPT=0 git push -u origin <branch>
GH_HOST=hfg.ghe.com gh pr create --repo data-engineering/hda-edm-dv --base main \
  --title "Ameren invoicing: vault source and GIE" --body "Implements docs/superpowers/specs/2026-09-24-ameren-invoicing-design.md"
```

`main` is protected and requires a code-owner review, so the PR cannot be merged by its author without `--admin`.

---

## What this plan does NOT do

- **It loads nothing.** Every Bronze binding is a placeholder because the feed does not exist.
- **No invoice payload, delivery or AP response capture.** Out of scope per the spec.
- **No supplier payment file.** A separate deliverable on the same vault.
- **No supplier name matching.** Deferred until a code exists.

## Open questions that block go-live but not this plan

These are in the spec with holders named. None prevents the code being written; all prevent it being trusted in production.

1. **The tax-point date AME002 needs is defined nowhere** and is absent from the feed. Task 7 refuses to default it, so this surfaces as a failure rather than a guess.
2. **AME009's date format** — the rule says `DD/MM/YYYY`, its own example is ISO, and the data is ISO.
3. **AME011's parsing rule** needs worked examples for TS, ES and MI. Task 4 implements the only safe reading and says so.
4. **Does the invoice feed carry `supplier_code`?** If not, `hub_supplier` stays unbound.
5. **Who delivers the invoice to Ameren AP**, and in what form.
