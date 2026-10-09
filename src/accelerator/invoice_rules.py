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

    THE SAME SOURCE COLUMN FEEDS module_value(), AND A TAX LINE MAKES THAT FUNCTION
    RAISE. `nhl_invoice_line` declares ONE column, invoice_line_item_type, carrying
    either a module code (TS/ES/MI) or `Tax`. This function maps `Tax` to TAX and
    returns; module_value() refuses it, by design, because `Tax` selects no module
    and guessing one would put another module's expenditure coding on the line.
    Gold's required-field gate then asks every released line for project and task
    coding, so an invoice carrying a tax line is routed to exceptions rather than
    released. That interaction is UNRESOLVED, not decided: whether a TAX line carries
    project/task coding at all is a business question this repo cannot answer -- the
    sample holds one TAX line in 555, so the path has never been exercised. It is
    open question 7 in the design spec, and tests pin the current behaviour so it
    cannot change unnoticed.
    """
    return "TAX" if (source_type or "").strip().lower() == "tax" else "ITEM"


def task_number(task_code: str) -> str:
    """AME011: the task number from a task code.

    The sheet says "parse the value after | and remove closing parenthesis where
    applicable". A code with no pipe therefore has no "value after", and the sheet
    does not say what to do. RETURNING THE WHOLE VALUE is the only safe reading: the
    alternative puts a blank Task Number on a customer invoice, and a blank is not
    something Ameren AP can reject intelligently -- a value that is merely un-split
    is still a value; a value we invented from nothing is not.

    An EMPTY code raises. That is a missing required field (AME010 makes Project/Task
    coding mandatory), not a parsing question, and it belongs in the exception route
    rather than on an invoice.
    """
    code = (task_code or "").strip()
    if not code:
        raise ValueError("task code is empty; AME010 makes it a required field")
    value = code.split("|", 1)[1] if "|" in code else code
    return value.rstrip(")").strip()


def module_value(line_item_type: str, ts: str, es: str, mi: str) -> str:
    """AME012/AME013: pick the value belonging to the line's module.

    Called ONCE PER ATTRIBUTE, not once per line. `nhl_invoice_line` carries three
    module-scoped columns for each of Task Code, Expenditure Type and Expenditure
    Organization (a `_ts`/`_es`/`_mi` trio apiece, nine columns total) because
    Fieldglass only populates the one matching the line's module and the raw vault
    must not decide, on its own, which of the other two to discard. So the caller
    invokes this function three times per line -- once per attribute -- each time
    passing that attribute's own `_ts`/`_es`/`_mi` trio alongside the same
    `line_item_type`; it does not fan out task code, expenditure type and
    expenditure organization in one call.

    Raises on an unknown module. Defaulting to one of the three would silently put
    another module's expenditure coding on the line, which reconciles and is wrong.

    A TAX LINE RAISES HERE, AND THAT IS DELIBERATE. line_type() reads the SAME column
    this function reads -- nhl_invoice_line carries one invoice_line_item_type -- and
    maps `Tax` to TAX, while `Tax` selects no module here. There is no TS/ES/MI value
    to fall back on for a tax line, so a caller must not call this function for one;
    raising is what stops a tax line silently acquiring the Timesheet module's task
    code. What a TAX line should carry for project/task coding instead is open
    question 7 in the design spec, not something this function may decide.
    """
    module = (line_item_type or "").strip().upper()
    if module not in _MODULES:
        raise ValueError(f"unknown module {line_item_type!r}; expected one of {_MODULES}")
    return {"TS": ts, "ES": es, "MI": mi}[module]


def client_amount_consistent(client: Decimal, supplier: Decimal,
                             msp: Decimal | None) -> bool:
    """Whether one line's client amount is its supplier cost plus its MSP margin.

    NOT AN AME RULE, AND IT DOES NOT GET AN ID. The thirteen AME ids come from the
    customer's `5 Business Rules` sheet; this comes from Amy Keser's instruction of
    24 September 2026 -- *"generate the Ameren invoice against the client total,
    including the MSP margin. The supplier total should remain the underlying cost
    and reconciliation component, not the final invoiced amount."* Minting a
    fourteenth id would file a decision taken in an email as though the customer's
    sheet had authorised it, and the traceability gate would then point at a sheet
    row that does not exist.

    A CHECK, NOT A DERIVATION, and the difference is the point. Fieldglass states
    the client amount per line in `Invoice_Amount`, so computing it from the other
    two would be re-deriving a number the source already sent, and the arithmetic
    would then agree with itself no matter what arrived. Reading the source's value
    and asserting the identity is what makes a changed feed visible: if margin stops
    being included, or starts being applied twice, these three columns stop adding
    up and the line can be quarantined instead of invoiced.

    Measured 24 September over the whole real extract: it holds on 1,172 of 1,172
    lines. Every timesheet line carries margin and no expense or miscellaneous line
    does -- but that distribution is NOT asserted anywhere, deliberately. Amy: *"We
    will have some variety in when MSP fee's show, they typically do not apply to
    expenses."* A gate saying margin may only land on a timesheet line would reject
    a correct invoice the first time a client is billed differently.

    A missing margin is ZERO, not a failure. 39 of 419 invoices carry none at all,
    being wholly expenses, and `None` is how the column arrives when Fieldglass has
    nothing to put there.
    """
    return client == supplier + (msp if msp is not None else Decimal("0"))


#: Line item types that may NOT carry an MSP fee. Confirmed by Amy Keser, 24 September
#: 2026: "MSP fee's should not be applied on expenses across all clients and therefore we
#: should expect a record to reject where this is present in the data."
#:
#: EXPENSES ONLY, AND MISCELLANEOUS IS DELIBERATELY ABSENT -- now confirmed, not merely
#: unresolved. Amy, 25 September: "confirmed that the MSP application on a Misc Fee is done
#: at ENTRY LEVEL and therefore we need to pass the data through as it comes in from FG, no
#: rule required at client level." Entry level means two miscellaneous lines on one invoice
#: can differ, so no rule -- global or per client -- can express it and the data must carry
#: it. MI stays out of this tuple permanently, not provisionally.
_MSP_FORBIDDEN_ON = ("ES",)


def msp_violation(line_item_type: str, msp_amount: Decimal | None) -> str:
    """Why this line must be rejected for carrying an MSP fee, or "" if it is fine.

    A REVERSAL, RECORDED BECAUSE THE REASONING MATTERS. The distribution was measured
    before this rule existed -- all 1,057 timesheet lines carry margin, none of the 110
    expense or 5 miscellaneous lines do -- and deliberately NOT encoded, because Amy had
    said the variety was real and a rule built from one client's weekly extract would
    reject a correct invoice the first time another client was billed differently. She
    was then asked directly, and confirmed it holds across all clients. Asking is what
    turned an observation into a rule; measuring alone could not have.

    RETURNS A REASON, NOT A BOOL, because this routes an invoice to the exception set and
    a human has to act on it. "rejected" tells them nothing they can do.

    ZERO AND NONE ARE NOT VIOLATIONS. `None` is how the column arrives when Fieldglass has
    nothing to put there, and 39 of 419 invoices are wholly expenses -- treating an absent
    fee as a present one would reject the normal case.
    """
    if (line_item_type or "").strip().upper() not in _MSP_FORBIDDEN_ON:
        return ""
    if msp_amount is None or msp_amount == 0:
        return ""
    return (f"MSP fee of {msp_amount} on an expense line -- MSP is not applied to "
            f"expenses on any client, so this record rejects")


def reconciles(line_amounts: list[Decimal], gross: Decimal) -> bool:
    """AME008: the lines must sum EXACTLY to the invoice gross.

    WHICH GROSS, SETTLED 24 SEPTEMBER: the CLIENT total, `gross_invoice_amount`,
    against the CLIENT line amounts. Not the supplier total. The two differ by the
    MSP margin -- 24,054.04 across 380 of 419 invoices in the sample -- so passing
    supplier line amounts against the client gross, or the reverse, fails on every
    invoice carrying margin, and passing supplier against supplier reconciles a
    document for the wrong amount. The caller chooses; this function cannot tell
    which columns it was handed, which is why the choice is written down here.

    Decimal, not float, and no tolerance band. `0.1 + 0.2 != 0.3` in binary floating
    point, so a tolerance would let a real mismatch through as long as it happened to
    be smaller than the band -- and an invoice released on a tolerance is an invoice
    whose total nobody actually checked. Exact equality is the only comparison that
    cannot be talked into passing a wrong invoice.
    """
    return sum(line_amounts) == gross
