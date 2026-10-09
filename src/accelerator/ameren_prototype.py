"""Read the Ameren workbook's already-issued invoices as prototype seed data.

WHAT THIS IS, AND WHAT IT IS NOT. The workbook's `3 Ameren Custom File` sheet holds
555 real invoice lines across 225 invoices. It is the GIE **output** format -- the
thirteen fields the Global Invoice Engine must produce -- not the Fieldglass source
format. Feeding it into the raw vault would run the pipeline backwards.

So this module does the two things the sheet can honestly support:

  * it seeds `control.ctl_invoice_issuance` with issuance that ALREADY HAPPENED --
    these invoices were issued on 2026-07-29 with those line numbers, and the design
    says issued values are recorded, never recomputed; and
  * it materialises the gold export shape from real data, so consumers have something
    to point at before the feed exists.

WHAT THE SHEET CANNOT SUPPORT, measured rather than assumed:

  * **No supplier at all** -- no code, no name. `hub_supplier` cannot be seeded.
  * **No worker id** -- only names, inside a pipe-delimited description. Keying a
    worker on a name is what the supplier analysis refused on evidence.
  * **No supplier or MSP amount** -- the margin, which is why `nhl_invoice_line`
    carries both amounts, has no data here.
  * **One already-selected Task Number** instead of the nine per-module columns, so
    AME011-AME013 have nothing to select from.

Nothing here writes to a workspace. It produces rows; deploying them is separate.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

#: The thirteen fields the gold export carries, in the sheet's own order.
GOLD_FIELDS = (
    "invoice_number", "invoice_date", "invoice_amount", "description",
    "line_description", "line_number", "line_type", "amount",
    "accounting_date", "project_number", "task_number",
    "expenditure_type", "expenditure_organization",
)

#: Header text in the sheet, mapped to the field names above. The sheet's row 3 carries
#: these verbatim; a rename upstream must fail loudly rather than silently produce nulls.
SHEET_HEADERS = (
    "Invoice Number", "Invoice Date", "Invoice Amount", "Description",
    "Line Description", "Line Number", "Line Type", "Amount",
    "Accounting Date", "Project Number", "Task Number",
    "Expenditure Type", "Expenditure Organization",
)

_TENANT_LEN = 4


def tenant_of(invoice_number: str) -> str:
    """The buyer code, which Fieldglass embeds in every identifier it mints.

    `AEE1IN00123410` -> `AEE1`. This is the only place the client appears in the sheet
    at all -- there is no buyer column -- so it is derived, and derived narrowly: a
    value too short to carry one raises rather than returning a truncated prefix that
    would hash into a plausible-looking wrong key.
    """
    value = (invoice_number or "").strip()
    if len(value) <= _TENANT_LEN:
        raise ValueError(f"invoice number {invoice_number!r} is too short to carry a buyer code")
    return value[:_TENANT_LEN]


def timesheet_ref_of(description: str) -> str:
    """The reference in the Description's fourth pipe-delimited field.

    AME004 builds `invoice id | worker | week ending | line ref`, so the fourth field
    is what the workbook calls the line reference. IT IS NOT ONE. Measured across the
    555 rows: 225 distinct values for 555 lines, one of them carrying 36 separate
    lines. It is a TIMESHEET reference -- `AEE1TS00126546` -- and several invoice lines
    bill against the same timesheet.

    That is why `synthetic_line_ref` exists. Returning this value as a line identifier
    would be returning an invoice-level id under a line-level name.
    """
    parts = (description or "").split("|")
    if len(parts) < 4:
        raise ValueError(f"description {description!r} has no fourth pipe field")
    ref = parts[3].strip()
    if not ref:
        raise ValueError(f"description {description!r} has an empty reference field")
    return ref


def synthetic_line_ref(timesheet_ref: str, line_number: int) -> str:
    """A per-line identifier the source does not provide, marked as ours.

    `ctl_invoice_issuance`'s grain is (tenant, invoice, LINE ref) and the sheet has no
    per-line identifier -- see `timesheet_ref_of`. Seeding the ledger with the bare
    timesheet reference would present 36 lines under one key, which
    `invoice_issue.assign_line_numbers` refuses outright as a duplicate. That refusal is
    correct and this function does not work around it: it mints a reference that is
    honestly OURS, so a reader can tell at a glance that it did not come from
    Fieldglass.

    The `#` is deliberate. No Fieldglass identifier in this estate contains one, so a
    synthetic reference can never be mistaken for a real one, and cannot collide with
    one when the real feed arrives and replaces these rows.

    IT STILL APPLIES NOW THAT THE LEDGER'S GRAIN IS line_reference, and the `#` is what
    makes that safe. line_reference is nhl_invoice_line's transaction key, a 64-character
    SHA2 hex string -- these invoices were issued BEFORE the vault existed, so no
    transaction key was ever computed for their lines and none can be reconstructed from
    a document. A synthetic reference is therefore still the only honest value, and
    because SHA2 hex contains no `#`, a seeded row can never collide with a real one nor
    be read as one.
    """
    ref = (timesheet_ref or "").strip()
    if not ref:
        raise ValueError("timesheet reference is required")
    if int(line_number) < 1:
        raise ValueError(f"line number {line_number!r} must be 1 or greater")
    return f"{ref}#{int(line_number)}"


def money(value) -> Decimal:
    """A Decimal, never a float.

    AME008 compares sums to an invoice total exactly. Reading a money column through
    float would make that comparison a coin toss on values like 0.1 + 0.2, so the
    conversion happens once, here, via str().
    """
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, AttributeError, ValueError) as exc:
        raise ValueError(f"{value!r} is not a monetary amount") from exc


def gold_row(raw: dict) -> dict:
    """One gold export row from one sheet row.

    A passthrough by design -- the sheet is already the gold shape. The work is
    validation: every field present, amounts as Decimal, tenant derivable. A seed that
    silently dropped a field would produce a gold table that looks right and is short a
    column.
    """
    missing = [h for h in SHEET_HEADERS if str(raw.get(h, "") or "").strip() == ""]
    if missing:
        raise ValueError(f"row is missing required field(s): {missing}")
    row = {f: str(raw[h]).strip() for f, h in zip(GOLD_FIELDS, SHEET_HEADERS)}
    row["invoice_amount"] = money(row["invoice_amount"])
    row["amount"] = money(row["amount"])
    row["line_number"] = int(float(row["line_number"]))
    row["invoice_tenant"] = tenant_of(row["invoice_number"])
    return row


def issuance_rows(gold_rows: list[dict], *, issued_by_run_id: str) -> list[dict]:
    """Ledger rows for invoices that were issued before this repo existed.

    These are NOT issued here. The date and the line numbers are read off the document
    the customer already received, which is the only way a historical issuance can enter
    an append-only ledger honestly -- inventing them would be exactly the recomputation
    the ledger exists to prevent.
    """
    out = []
    for r in gold_rows:
        out.append({
            "invoice_tenant": r["invoice_tenant"],
            "invoice_reference": r["invoice_number"],
            "line_reference": synthetic_line_ref(
                timesheet_ref_of(r["description"]), r["line_number"]),
            "line_number": r["line_number"],
            "invoice_date": r["invoice_date"][:10],
            "issued_by_run_id": issued_by_run_id,
        })
    return out
