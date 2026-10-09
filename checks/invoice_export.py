"""The gold projection and its release gates.

APPLIES AME010 (Project Number direct map) -- a pure passthrough, straight off the GIE
satellite's payload, which is why it has no derivation function in invoice_rules.py.

AME001 (Invoice Number) IS NOT APPLIED, AND THIS IS THE SENTENCE THAT SAYS SO. The rule
is described upstream as a direct map, but THE COLUMN IT WOULD MAP FROM IS NOT DECLARED
ANYWHERE IN THIS REPO: `invoice_number` is not in csat_invoice_line_gie's payload, not in
control.ctl_invoice_issuance, and therefore not in projection_columns(). An earlier
version of this docstring claimed AME001 came "straight off the GIE satellite's payload",
which was false, and verify_repo's traceability gate was satisfied by substring-matching
that false sentence -- so AME001 was traceable to a claim and to nothing else.

It is now traced to the REFUSAL that names it: UNDECLARED_SOURCES below, and
required_field_refusal(), which stops every run and hands the question to the owner. That
is the same family as the two `unresolved` markers in mapping_customer_invoice.yml and it
is handled the same way, because `invoice_reference` LOOKING like the invoice number is
not authority to write it onto a customer's invoice. verify_repo asserts the
undeclared-ness mechanically -- it parses this module and measures the entity model -- so
this paragraph cannot be what keeps the gate green.

GATES AME008 per invoice, and required fields per line: release_findings(), called by
main() over the projected rows before a single byte is written. Those two gates were
present as pure functions and wired to NOTHING for the whole of Task 5; an invoice whose
lines did not sum to its gross passed every shape, join and DCDD gate and would have been
exported. TODAY required_field_refusal() stops every run before release_findings() is
reached, and that is deliberate rather than a second gap: a required-field gate that
rejects 100% of lines for a field nobody has declared a source for is not a gate, it is
an outage wearing a data-quality label.

WHY A GATE AND NOT AN EXPECTATION. A line that fails a data-quality expectation is
quarantined and the rest of the load proceeds. An invoice whose lines do not sum to
its gross must not be PARTIALLY released: a well-formed invoice with a line missing
is worse than no invoice at all, because it looks correct and will be paid.

WHY A GATE AND NOT A NOT NULL CONSTRAINT (required fields). A constraint fails the
write and takes the whole invoice with it -- one blank task code on one line loses
every line on the invoice, including the ones that were fine. A gate instead routes
the invoice to the exception set with a reason a human can act on, and lets the rest
of the load proceed.

THIS MUST RUN AS AN IDENTITY INSIDE THE PRIVILEGED GROUP. Every amount here carries
governance.mask_money, so an identity outside that group reads NULL and the sum
compares equal to nothing. A gate that passes because it cannot see the data is the
failure mode this repo has met most often -- a false PASS is worse than the FAIL it
is standing in for, because nothing downstream re-checks it.

AND THAT HAZARD IS NOW MECHANICAL RATHER THAN WRITTEN DOWN. money_refusal() refuses to
write a file whose money columns are null on every row, and money_declaration_refusal()
binds the mapping's `money: true` flags to the masks metadata/entities declares, in both
directions -- because a gate that finds its columns by reading a flag is only as good as
the flag, and an entry that sources a masked column and forgets it is invisible to the
gate that exists for it.

WHAT THIS TASK WRITES, AND WHAT IT REFUSES TO WRITE. The projection reads
csat_invoice_line_gie at its current version, joined to the issuance ledger for the two
values AME002 and AME006 froze, and renders the four csv files Workday's DT/DTS loader
takes. Three things stop it, each before a single byte is written:

  * a column mapped to a source nobody can resolve  -- unresolved_refusal()
  * a money column this identity cannot see         -- money_refusal()
  * four files that would not join                  -- join_findings()

TODAY THE FIRST ONE STOPS EVERY RUN, and that is the correct outcome rather than a gap.
Customer_Invoice_ID and Customer_Reference_ID are marked `unresolved` in
metadata/workday/mapping_customer_invoice.yml, naming source columns that do not exist,
and the first of them is the must-populate join key on all four files. Blanking it would
export four CSVs joined on an empty string -- which looks exactly like a correct export,
and which nothing downstream re-checks. So the run finishes NOT_EVALUATED, names both
fields and both reasons, and writes nothing. The decision is the mapping owner's.
"""
from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define
# __file__, so every Path(__file__) below raised NameError and the gate died before
# asserting anything. compile() still records the real path in the code object.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import csv
from collections import Counter
import io
import re
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
# checks/ IS NOT A PACKAGE, so the sibling import below needs its directory on the path
# the same way checks/landing_integrity_check.py and checks/supersede_quarantine.py do.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from accelerator import dcdd_validation, naming, spec, wd_mapping  # noqa: E402
from accelerator.invoice_rules import reconciles  # noqa: E402

import invoice_issue  # noqa: E402


#: Fields that must be present on every released line. A blank here does not fail a
#: load -- it reaches a customer invoice, where a missing project or task code is a
#: rejection at Ameren AP rather than an error anyone here sees. Includes the two
#: fields ISSUED by checks/invoice_issue.py (line_number, invoice_date): absent means
#: issuance never ran for this line, which must not be released either.
#:
#: ONE OF THE THIRTEEN HAS NO DECLARED SOURCE COLUMN, AND IT STAYS IN THIS TUPLE.
#: `invoice_number` (AME001) is not a column of any entity in metadata/entities and the
#: projection cannot return it -- see UNDECLARED_SOURCES and required_field_refusal()
#: below. Dropping it from here would make the gate pass by no longer looking, so it
#: stays, and the run refuses instead.
#:
#: ALL THIRTEEN THE SPEC SAYS GOLD EMITS, not twelve. invoice_amount (AME003, the
#: invoice gross repeated on every line) was missing from this tuple, so a line could
#: be released carrying a blank invoice total -- the one field Ameren AP reconciles
#: the document against.
#:
#: A TAX LINE IS REQUIRED TO CARRY PROJECT AND TASK CODING HERE, and that is an
#: UNRESOLVED interaction rather than a decision -- see invoice_rules.line_type and
#: invoice_rules.module_value, and open question 7 in the design spec. AME007 gives a tax
#: line line_type TAX; the same source column makes module_value raise, because `Tax`
#: is not one of the three Fieldglass modules. So an invoice carrying tax is routed to
#: exceptions by this gate and never released. Whether a TAX line carries project/task
#: coding at all is not knowable from this repo -- the sample holds one TAX line in
#: 555 -- so this tuple pins the CURRENT behaviour and does not pretend to answer it.
REQUIRED_FIELDS = ("invoice_number", "description", "line_description", "line_type",
                   "amount", "invoice_amount", "accounting_date",
                   "project_number", "task_number",
                   "expenditure_type", "expenditure_organization",
                   "invoice_date", "line_number")


def missing_required(line: dict) -> list[str]:
    """Required fields that are absent, empty or whitespace on one projected line.

    Checked as a GATE rather than a NOT NULL constraint because the remedy differs: a
    constraint fails the write and takes the whole invoice with it, while this routes
    the invoice to the exception set with a reason a human can act on.

    str(... or "").strip() catches all three shapes a "missing" value takes here --
    the key absent from the dict, an empty string, and a value that is only
    whitespace -- with one expression rather than three separate checks per field.
    """
    return [f for f in REQUIRED_FIELDS
            if str(line.get(f, "") or "").strip() == ""]


def releasable(line_amounts: list[Decimal], gross: Decimal) -> tuple[bool, str]:
    """Whether one invoice may be released, and why not when it may not.

    Delegates the reconciliation test to invoice_rules.reconciles (AME008) rather than
    reimplementing it -- that function is the one place this rule is allowed to live,
    so a change to the tolerance or comparison only ever happens there.

    An invoice with no lines is never releasable. `sum([]) == 0` would let a
    zero-gross invoice through -- reconciles([], Decimal("0")) is True -- and a zero
    invoice is not a document anyone sends, so the empty case is refused before
    reconciles() is even asked.
    """
    if not line_amounts:
        return False, "invoice has no lines"
    if not reconciles(line_amounts, gross):
        total = sum(line_amounts)
        return False, f"lines sum to {total}, gross is {gross}"
    return True, ""


#: REQUIRED FIELD -> (AME rule id, why nothing in this repo can supply it).
#:
#: THE HONEST HANDLING OF AME001, AND THE REASON IT IS A dict RATHER THAN PROSE.
#: `invoice_number` is one of the thirteen fields the spec says gold emits and one of the
#: fields REQUIRED_FIELDS above gates on -- and no entity in metadata/entities declares a
#: column of that name, control.ctl_invoice_issuance does not carry one, and
#: projection_columns() therefore cannot return one. Three ways of handling that were
#: available and two of them are wrong:
#:
#:   * GUESS A SOURCE. hub_invoice.invoice_reference is the plausible candidate and the
#:     projection already carries it. It is also exactly the inference that put
#:     `invoice_id` and `buyer_tenant` into mapping_customer_invoice.yml, which is why
#:     both carry `unresolved` markers today. A wrong identifier on a customer invoice is
#:     not recoverable by a later load.
#:   * DROP THE FIELD from REQUIRED_FIELDS. The gate then passes because it stopped
#:     looking, which is the false PASS this module exists to refuse.
#:
#: The third is this: name the field, name the rule, name the ACTION, and REFUSE. A run
#: reports NOT_EVALUATED and writes nothing until the owner declares the column. The
#: entry is machine-readable because verify_repo parses it and measures the claim against
#: metadata/entities rather than believing a comment -- so the day somebody DOES declare
#: `invoice_number`, leaving this entry here goes red instead of quietly refusing forever.
UNDECLARED_SOURCES: dict[str, tuple[str, str]] = {
    "invoice_number": (
        "AME001",
        "AME001 (Invoice Number) is described as a direct map, but no source column is "
        "declared for it: `invoice_number` is absent from csat_invoice_line_gie's "
        "payload, from control.ctl_invoice_issuance's columns, and from every other "
        "entity in metadata/entities. hub_invoice.invoice_reference is the plausible "
        "candidate and this export deliberately does NOT use it -- that is the same "
        "inference that produced the two `unresolved` markers in "
        "metadata/workday/mapping_customer_invoice.yml, and an invoice number is the "
        "identifier the customer pays against."),
}


def undeclared_required_fields(src: dict) -> tuple[str, ...]:
    """Required fields this projection carries no column for, in REQUIRED_FIELDS order.

    MEASURED AGAINST THE STATEMENT, NOT AGAINST UNDECLARED_SOURCES. The constant records
    WHY a field is undeclared; this function decides WHETHER it still is, by asking
    projection_columns() what the projection actually returns. A field that gains a real
    column stops appearing here on the next run without anybody editing a list, and a
    second field that quietly loses one starts appearing here without anybody having
    written it down -- which is the direction that matters.
    """
    carried = set(projection_columns(src))
    return tuple(f for f in REQUIRED_FIELDS if f not in carried)


def required_field_refusal(src: dict) -> str | None:
    """Why the required-field gate cannot be run at all, or None when it can.

    A PRECONDITION, TAKEN BEFORE ANY ROW IS READ, AND NOT A FAILURE. missing_required()
    would name every one of these on every line of every invoice, so wiring it in without
    this would turn one undeclared column into a 100% rejection rate -- an outage that
    reads in a log exactly like bad data, and whose obvious remedy is to delete the field
    from REQUIRED_FIELDS and thereby stop gating it. Refusing here instead keeps the
    field gated, names the rule, and hands the decision to the person who can make it.

    A field with no entry in UNDECLARED_SOURCES is reported too, and reported as the
    WORSE case: an undeclared source nobody has written down is a gate that would have
    started rejecting everything with no explanation attached.
    """
    absent = undeclared_required_fields(src)
    if not absent:
        return None
    lines: list[str] = []
    for field in absent:
        known = UNDECLARED_SOURCES.get(field)
        if known:
            lines.append(f"  {field} ({known[0]}): {known[1]}")
        else:
            lines.append(
                f"  {field}: this projection carries no column of that name, and nothing "
                f"in this module says why. REQUIRED_FIELDS gates on it, so every line of "
                f"every invoice would be rejected for it -- with no rule id, no reason "
                f"and no owner recorded anywhere")
    return (f"{len(absent)} of the {len(REQUIRED_FIELDS)} required gold field(s) have no "
            f"declared source column, so the required-field gate cannot be run and no "
            f"file is written:\n" + "\n".join(lines) + "\n"
            f"  ACTION: declare the source column for each field above -- in "
            f"metadata/entities (a payload column on csat_invoice_line_gie, or a column "
            f"on control.ctl_invoice_issuance) -- and this refusal clears itself on the "
            f"next run, because undeclared_required_fields() measures the projection "
            f"rather than reading a list. Until then this export writes nothing. The "
            f"decision is the owner's, exactly as it is for the `unresolved` markers in "
            f"metadata/workday/mapping_customer_invoice.yml.")


def release_findings(rows, src: dict) -> list[str]:
    """Every line and every invoice the two RELEASE gates refuse, as sentences.

    THIS IS WHERE missing_required() AND releasable() ARE ACTUALLY CALLED. Both existed
    as pure functions, both were unit tested, and neither was reachable from main() --
    so the module docstring's promise that it "GATES AME008 per invoice, and required
    fields per line" was true of the test suite and of nothing else. An invoice whose
    lines did not sum to its gross passed every shape, join and DCDD gate above and would
    have been written: the well-formed invoice with a line missing, which looks correct
    and gets paid.

    PER INVOICE, GROUPED ON THE SAME KEY COLUMNS invoice_rows() DEDUPLICATES ON, read off
    `src` rather than typed -- see invoice_key_columns(). Keying on anything else would
    reconcile one invoice's lines against another invoice's gross.

    A VALUE THAT IS NOT A NUMBER IS A FINDING, NEVER AN EXCEPTION. Decimal("") raises,
    and a raise here would leave main()'s guard printing "the release gates could not be
    applied" for what is really one bad row -- so the row is named instead.

    AME003 REPEATED ON EVERY LINE IS ASSERTED, NOT ASSUMED. `invoice_amount` is the
    invoice gross carried on every line of the invoice; two different values for one
    invoice mean the gross was recomputed somewhere, and picking either one to reconcile
    against would make AME008 agree with a number nobody can defend.
    """
    findings: list[str] = []
    keys = invoice_key_columns(src)
    line_ref = src["line_ref"]

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            findings.append(f"projection row {index} is {type(row).__name__}, not a row")
            continue
        absent = missing_required(row)
        if absent:
            findings.append(
                f"line {row.get(line_ref)!r} on invoice "
                f"{tuple(row.get(c) for c in keys)!r} is missing required field(s) "
                f"{absent} -- blank on a customer invoice is a rejection at Ameren AP, "
                f"not an error anybody here sees")

    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        if isinstance(row, dict):
            grouped.setdefault(tuple(row.get(c) for c in keys), []).append(row)

    for key in sorted(grouped, key=repr):
        group = grouped[key]
        amounts: list[Decimal] = []
        grosses: set[str] = set()
        bad = False
        for row in group:
            grosses.add(str(row.get("invoice_amount")))
            try:
                amounts.append(Decimal(str(row.get("amount"))))
            except Exception:  # noqa: BLE001
                bad = True
                findings.append(
                    f"line {row.get(line_ref)!r} on invoice {key!r} has amount "
                    f"{row.get('amount')!r}, which is not a number -- AME008 cannot "
                    f"reconcile a sum that one line is not part of")
        if len(grosses) != 1:
            findings.append(
                f"invoice {key!r} carries {len(grosses)} different invoice_amount "
                f"value(s) {sorted(grosses)} across its {len(group)} line(s). AME003 "
                f"repeats ONE gross on every line, so reconciling against either of "
                f"these would be reconciling against a number nobody can defend")
            continue
        if bad:
            continue
        gross_text = next(iter(grosses))
        try:
            gross = Decimal(gross_text)
        except Exception:  # noqa: BLE001
            findings.append(
                f"invoice {key!r} has invoice_amount {gross_text!r}, which is not a "
                f"number -- AME008 has nothing to reconcile the lines against")
            continue
        ok, why = releasable(amounts, gross)
        if not ok:
            findings.append(
                f"invoice {key!r} is NOT releasable (AME008): {why}. A partially "
                f"released invoice is worse than no invoice at all, because it looks "
                f"correct and will be paid")
    return findings


# --------------------------------------------------------------------------- #
# THE GOLD PROJECTION, AND THE FOUR CSV FILES IT RENDERS
#
# Everything below is the half the stub named as MISSING: argv parsing, and a
# projection that actually reads csat_invoice_line_gie. The shape follows
# checks/invoice_issue.py deliberately -- pure functions that render or adjudicate,
# a main() that parses, guards every workspace call, and prints ONE summary line.
# --------------------------------------------------------------------------- #

#: What a run of this task is called in its GATE SUMMARY line.
GATE = "invoice_export"

#: The two tables this task reads. The ledger is the FROZEN half (AME002, AME006) and
#: is never recomputed here; the GIE satellite is the DERIVED half.
LEDGER = "ctl_invoice_issuance"
GIE_TABLE = "csat_invoice_line_gie"

#: The workbook and the mapping, relative to the repo root.
DCDD_FILE = ("metadata", "workday", "dcdd", "Submit_Customer_Invoice_DCDD.xlsx")
DCDD_SHEET = "Submit_Customer_Invoice_DCDD"
MAPPING_FILE = ("metadata", "workday", "mapping_customer_invoice.yml")

#: THE TWO COLUMNS THAT MAKE FOUR FILES A SET. Customer_Invoice_ID is carried by every
#: file ("All CSVs" in the workbook); Customer_Invoice_Line_Reference_ID by the two line
#: files. An orphan on either side is a rejection at Workday, which is a round trip
#: through Logan, so it is asserted here instead.
JOIN_KEY = "Customer_Invoice_ID"
LINE_KEY = "Customer_Invoice_Line_Reference_ID"

#: The two grains a csv file can have, DERIVED rather than declared -- see file_grain().
INVOICE_GRAIN = "invoice"
LINE_GRAIN = "line"

#: The ledger columns the projection carries beside the payload. A projected line that
#: has neither is a line issuance never saw, and exporting it would print a line number
#: nobody froze.
ISSUED_COLUMNS = ("line_number", "invoice_date")

#: THE HUB THE DCDD'S 27 CHECKREFERENCES FIELDS WOULD BE VALIDATED AGAINST, and the two
#: business keys it is grained on. It exists in metadata/entities, and today it holds
#: nothing: its WORKDAY binding lands from 02_usnc_silver_edm_dev.workday_landing because
#: 01_usnc_bronze_dev has no workday schema yet (subsystem E). Reading it is therefore
#: expected to return zero rows, and a zero-row reference table is the one result this
#: task must never render as a pass.
REFERENCE_TABLE = "hub_wd_reference"

#: A UC Volume path, and nothing else. Snaplogic reads volumes; a Workspace folder or a
#: dbfs path is a second code path nobody asked for, and a relative path resolves
#: against the driver's cwd -- which on serverless is not anywhere anyone is watching.
VOLUME_PREFIX = "/Volumes/"


def file_columns(dcdd: dict, mapping: dict) -> dict[str, tuple[tuple[str, str, str], ...]]:
    """csv file -> its columns in workbook order, as (csv header, WD field, status).

    READS wd_mapping.csv_columns, WHICH WALKS per_row. The deduped views on an entry are
    of differing lengths -- Customer_Invoice_ID is 1 row / 4 files / 1 header,
    Dispute_Reason_Reference_ID is 5 rows / 1 file / 5 headers -- so zipping them agrees
    only by coincidence.

    NOT PRE-SEEDED WITH CSV_FILES. A file that lost every one of its columns must
    DISAPPEAR from this dict so the caller's "exactly these four files" comparison goes
    red, rather than appearing with an empty tuple and rendering a header-only csv.
    """
    out: dict[str, list[tuple[str, str, str]]] = {}
    for csv_file, header, field, status in wd_mapping.csv_columns(dcdd, mapping):
        out.setdefault(csv_file, []).append((header, field, status))
    return {name: tuple(columns) for name, columns in out.items()}


def file_grain(dcdd: dict, mapping: dict) -> dict[str, str]:
    """csv file -> whether it holds one row per invoice or one row per line.

    DERIVED FROM THE WORKBOOK, NOT TYPED HERE. The DCDD puts
    Customer_Invoice_Line_Reference_ID in exactly the two line files and nowhere else, so
    "carries the line key" IS the definition of a line-grained file. A hand-written map
    would keep saying `Submit_Customer_Invoice_Lines_Details: line` after a DCDD that
    moved the key, and the export would then write one row per line into a file Workday
    reads one row per invoice from -- which loads, and duplicates the invoice.

    A FILE CARRYING NEITHER KEY RAISES. Every one of the four carries Customer_Invoice_ID
    ("All CSVs"); a file that does not is a file nothing can join to the others, and
    writing it would produce the four-unrelated-files outcome this module exists to stop.
    """
    grain: dict[str, str] = {}
    for name, columns in file_columns(dcdd, mapping).items():
        fields = {field for _header, field, _status in columns}
        if JOIN_KEY not in fields:
            raise ValueError(
                f"csv file {name!r} carries no {JOIN_KEY} column, so nothing joins it to "
                f"the other files. The DCDD marks that field 'All CSVs'; a file without "
                f"it is either a file this export should not be writing or a workbook "
                f"that has changed shape, and neither is guessable here")
        grain[name] = LINE_GRAIN if LINE_KEY in fields else INVOICE_GRAIN
    return grain


def unresolved_refusal(dcdd: dict, mapping: dict) -> str | None:
    """Why no file may be written at all, or None if every mapped column resolves.

    REFUSE, NOT BLANK, AND PER COLUMN. Customer_Invoice_ID is marked `unresolved` and its
    single workbook row is "All CSVs", so it is FOUR columns a writer would otherwise
    blank -- and blanking it exports four CSVs joined on an empty string, which looks
    exactly like a correct export and is the failure mode this repo has met most often.
    Reporting the FIELD once would name one problem where there are four files' worth.

    The reason is carried through verbatim from the mapping, because the person who finds
    this in a job log has to act on it and `unresolved: true` is not something anyone can
    act on.
    """
    columns = wd_mapping.unresolved_columns(dcdd, mapping)
    if not columns:
        return None
    reasons = wd_mapping.unresolved_fields(mapping)
    files = sorted({c[0] for c in columns})
    out = [f"{len(columns)} column(s) across {len(files)} file(s) are mapped to a source "
           f"nobody can resolve. NO file is written -- not even the files whose own "
           f"columns all resolve, because a set of four joined on a blank key is worse "
           f"than no set at all."]
    for csv_file, header, field, _status in columns:
        out.append(f"  REFUSED {csv_file} :: csv column {header!r} (WD field {field})")
    for field in sorted({c[2] for c in columns}):
        out.append(f"  WHY {field}: {reasons.get(field, '<no reason recorded>')}")
    out.append("  This is a modelling decision about a customer's invoice and it belongs "
               "to the owner of metadata/workday/mapping_customer_invoice.yml, not to "
               "this writer. Nothing was written and nothing was altered.")
    return "\n".join(out)


def volume_refusal(path) -> str | None:
    """Why the configured destination cannot be written to, or None if it can.

    NO DEFAULT ANYWHERE IN THIS PATH. `gold_export_volume` has none in databricks.yml for
    the reason `gold_export_schema` has none, and this function is the other end of it: a
    wrong-but-plausible default is how a file lands somewhere nobody is watching, and a
    relative path resolves against the driver's cwd, which on serverless is nowhere.
    """
    text = "" if path is None else str(path)
    if not text.strip():
        return ("--export-volume was not supplied, and there is no default. The bundle "
                "variable gold_export_volume is where this comes from; an unset one "
                "fails here, naming the target, rather than writing four CSVs into a "
                "path nobody is collecting.")
    if text != text.strip():
        return (f"--export-volume {text!r} has leading or trailing whitespace. A path "
                f"that differs from the one somebody typed by a space is a path nobody "
                f"can find again.")
    parts = text.split("/")
    if not text.startswith(VOLUME_PREFIX) or len(parts) < 5 or not all(parts[2:5]):
        return (f"--export-volume {text!r} is not a UC Volume path of the form "
                f"{VOLUME_PREFIX}<catalog>/<schema>/<volume>[/...]. Snaplogic collects "
                f"from volumes; a Workspace folder or a dbfs path is a second code path "
                f"this export does not have, and a relative path resolves against the "
                f"driver's cwd.")
    return None


def masked_columns(model) -> set[tuple[str, str]]:
    """Every (base table, column) the entity model declares a mask over.

    The authority for "this value is masked" is metadata/entities, which is also what
    governance/apply_masks.sql is generated from -- so a mask added there is a mask this
    export knows about on the next run, rather than one somebody has to remember to
    restate in the mapping.
    """
    return {(entity.base_table, column)
            for entity in model.entities for column, _fn in entity.masks}


def money_declaration_refusal(mapping: dict, masked: set[tuple[str, str]]) -> str | None:
    """Why the mapping's `money` flags cannot be trusted, or None if they can.

    THE GATE BELOW IS ONLY AS GOOD AS THIS ONE. money_refusal() refuses a file whose
    money columns are null on every row, and it finds those columns by reading
    `money: true`. An entry that sources a MASKED column and forgets the flag is
    therefore invisible to it: the amounts read NULL for an identity outside
    scope_unmask_currency_values, the gate has nothing to look at, and the file is
    written with NULL in every amount column while the run reports success.

    THE GROUP NAMED HERE IS THE CURRENCY ONE, NOT THE JOB-RUNNER ONE, and the distinction
    is load-bearing since the masks split. What this gate needs is "can see money", which
    is scope_unmask_currency_values. global_dataplatform_pipeline_job_runners answers a
    different question -- "can run the job" -- and an identity can hold it while reading
    NULL from every amount, which is precisely the false PASS this file guards.

    So the flag is BOUND to the entity model rather than trusted -- the same stance
    wd_mapping takes for `constant`, and for the same reason: a declaration nothing
    adjudicates is a declaration that drifts. Both directions are refused. An unflagged
    masked source is the dangerous one; a flagged unmasked one is a flag that has
    outlived its column and would keep refusing files for a reason that has passed.

    THE TABLE HALF IS NORMALISED THROUGH base_table(), the way row_value(),
    reference_gap() and main()'s count comprehension all do. It was not, and the omission
    was the same table-blindness this branch has now fixed four times. masked_columns()
    keys on BARE base table names, so `csat_invoice_line_gie.amount` with no `money` flag
    was refused and the identical source written `business_vault.csat_invoice_line_gie.
    amount` was not: the pair simply missed, the entry fell through `ref in masked` into
    silence, and money_refusal() then never counted the column it exists for. The reach
    is narrow today only because the offline suite's catalogue happens to accept a
    `control.` prefix and no control column is masked -- and that mitigation lives in the
    suite, not here, which is not where a customer-invoice safeguard belongs.
    """
    findings: list[str] = []
    for name in sorted(wd_mapping.exportable_fields(mapping)):
        entry = mapping[name]
        ref = wd_mapping.source_ref(entry)
        if ref is None:
            continue
        ref = (base_table(ref[0]), ref[1])
        flagged = entry.get("money") is True
        if ref in masked and not flagged:
            findings.append(
                f"  {name}: source {ref[0]}.{ref[1]} is masked by the entity model, but "
                f"the entry does not say `money: true`. An identity outside "
                f"scope_unmask_currency_values reads NULL from it and the null-money gate "
                f"would never look at the column")
        elif flagged and ref not in masked:
            findings.append(
                f"  {name}: the entry says `money: true`, but the entity model declares "
                f"no mask over {ref[0]}.{ref[1]}. A flag that has outlived its column "
                f"refuses files for a reason that has already passed")
    if not findings:
        return None
    return ("the mapping's `money` declarations disagree with the masks the entity model "
            "declares:\n" + "\n".join(findings))


def money_refusal(mapping: dict, nonnull_counts) -> str | None:
    """Why a projection's money columns cannot be believed, or None if they can.

    PLT-2, AND THE REASON THIS IS A REFUSAL RATHER THAN A WARNING. The run-as service
    principal is not in scope_unmask_currency_values, which governance.mask_money and
    governance.mask_money_double admit by name, so every masked amount it reads comes
    back NULL. It is NOT enough for the identity to hold
    global_dataplatform_pipeline_job_runners: that group is what the money masks used to
    admit and no longer do, and it now unmasks only the three PII functions.

    A file projecting amounts would then be written with NULL in every amount column and
    the load would report success -- and AME008 would reconcile
    a sum of nothing against a gross of nothing and agree. A gate that passes because it
    cannot see the data is a false PASS, which is worse than the FAIL it stands in for
    because nothing downstream re-checks it.

    A MISSING COUNT IS REFUSED AS HARD AS A ZERO ONE. `counts.get(name, 0)` would let a
    caller that never counted a column look identical to one that counted it and found
    nothing -- and the first is the shape a refactor produces. An int is required, and
    `isinstance(x, bool)` is excluded because True would otherwise pass for a count of 1.

    TODAY THIS RETURNS None ON THE REAL MAPPING, AND SAYS SO OUT LOUD: no entry sets
    `money: true`, because none of the five mapped fields is an amount. The branch is
    therefore unexercised by the production path and exercised only by the synthetic
    mapping in the offline suite. money_declaration_refusal() above is what stops that
    emptiness from becoming permanent by accident.
    """
    money = sorted(name for name, entry in wd_mapping.exportable_fields(mapping).items()
                   if isinstance(entry, dict) and entry.get("money") is True)
    if not money:
        return None
    findings: list[str] = []
    for name in money:
        count = nonnull_counts.get(name) if isinstance(nonnull_counts, dict) else None
        if isinstance(count, bool) or not isinstance(count, int):
            findings.append(
                f"  {name}: no non-null count was taken for this column ({count!r}). A "
                f"column nobody counted cannot be reported as readable")
        elif count <= 0:
            findings.append(
                f"  {name}: null on every projected row. Either this identity cannot "
                f"read the mask, or the column really is empty -- and those two look "
                f"identical in a written file")
    if not findings:
        return None
    return ("money columns this run cannot see, so no file is written (PLT-2: the run-as "
            "identity must be inside scope_unmask_currency_values, which "
            "governance.mask_money and governance.mask_money_double admit by name -- the "
            "job-runner group is not a substitute):\n"
            + "\n".join(findings))


def unissued_findings(rows) -> list[str]:
    """Projected lines the issuance ledger never issued.

    AME002 AND AME006 ARE READ, NEVER DERIVED. The projection LEFT JOINs the ledger
    rather than INNER JOINing it on purpose: an inner join drops an unissued line
    silently, and a line missing from a customer's invoice is exactly the well-formed
    wrong document §6 of the design spec refuses. A left join makes it a row with a null
    line number, which this reports by name.
    """
    findings: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            findings.append(f"projection row {index} is {type(row).__name__}, not a row")
            continue
        absent = [c for c in ISSUED_COLUMNS
                  if str(row.get(c, "") if row.get(c) is not None else "").strip() == ""]
        if absent:
            findings.append(
                f"line {row.get('line_reference')!r} on invoice "
                f"{row.get('invoice_reference')!r} was not issued: the ledger carries no "
                f"{absent} for it, and this export never derives either -- they are "
                f"frozen once by checks/invoice_issue.py and read from there")
    return findings


def join_findings(header_rows, line_rows) -> list[str]:
    """Everything that stops these rows being a joinable SET of files, as sentences.

    SEVEN RULES, AND EACH SAYS ITS OWN WORDS. A caller (or a check) that filters these on
    a token several rules could emit measures none of them, so "orphan", "has no line
    rows", "carries no", "names no", "appears on", "blank" and "used twice" appear in
    exactly one message each.

    TWO OF THE SEVEN CANNOT FIRE ON TODAY'S CALLER, AND SAYING SO IS THE POINT. main()
    derives its header rows from the very projection the line rows come from, so "orphan
    line row" and "header row ... has no line rows" are satisfied BY CONSTRUCTION -- the
    brief's "every Customer_Invoice_ID in a line file exists in the header file" is, as
    that caller uses it, an assertion against itself. They stay because this function is
    the contract and not the caller: a header set read back from gold, or a second writer
    sourcing headers independently, makes both live immediately, and they are unit-tested
    so they will work when that happens. What protects TODAY'S data is the other five --
    a header or line row with no invoice id, one invoice id on two header rows, a blank
    line reference, and a line reference used twice.

    "ONE INVOICE ID ON TWO HEADER ROWS" IS THE ONE THAT CLOSES A REAL HOLE. The written
    header rows are deduplicated on the projection's own grain (invoice_tenant,
    invoice_reference) while Customer_Invoice_ID is whatever the mapping sources it from,
    and those are not the same key: two tenants sharing an invoice_reference produce two
    header rows carrying one Customer_Invoice_ID, so the two invoice-grained CSVs ship a
    duplicate that every other rule here would wave through.

    Rows are keyed by WD FIELD NAME here, not by csv header: the header is what gets
    written, the field is what the mapping and the DCDD both speak.
    """
    findings: list[str] = []
    header_ids: list[str] = []
    for index, row in enumerate(header_rows):
        value = str((row or {}).get(JOIN_KEY, "") or "").strip()
        if not value:
            findings.append(
                f"header row {index} carries no {JOIN_KEY}. It is the join key on all "
                f"four files, so a blank one exports a set joined on an empty string")
            continue
        header_ids.append(value)
    known = set(header_ids)

    # Counter, not list.count() inside a comprehension over the same list. That was
    # O(n^2) ON THE DRIVER, before a single byte is written: measured 7.8s at 40k
    # invoices, ~50s at 100k and ~13 minutes at 400k -- spent deciding whether to
    # refuse, on a run that may well refuse anyway.
    id_counts = Counter(header_ids)
    for invoice in sorted(i for i, n in id_counts.items() if n > 1):
        findings.append(
            f"{JOIN_KEY} {invoice!r} appears on {id_counts[invoice]} header rows. "
            f"The header files are deduplicated on the projection's own invoice grain "
            f"and this column is whatever the mapping sources it from -- two tenants "
            f"sharing one invoice reference is exactly that divergence, and Workday "
            f"loads the duplicate")

    by_invoice: dict[str, list[str]] = {}
    for index, row in enumerate(line_rows):
        invoice = str((row or {}).get(JOIN_KEY, "") or "").strip()
        reference = str((row or {}).get(LINE_KEY, "") or "").strip()
        if not invoice:
            findings.append(
                f"line row {index} names no {JOIN_KEY}, so nothing joins it to a header "
                f"row. A NULL invoice reference in the vault reaches the file as an "
                f"empty string, which joins to every other empty one")
        elif invoice not in known:
            findings.append(
                f"orphan line row {index}: {JOIN_KEY} {invoice!r} appears in the line "
                f"files and in no header row, which Workday rejects on load")
        if not reference:
            findings.append(
                f"line row {index} on invoice {invoice!r} carries a blank {LINE_KEY}")
            continue
        by_invoice.setdefault(invoice, []).append(reference)

    for invoice, references in sorted(by_invoice.items()):
        for reference in sorted({r for r in references if references.count(r) > 1}):
            findings.append(
                f"{LINE_KEY} {reference!r} is used twice on invoice {invoice!r}; it "
                f"identifies one line of one invoice, and two rows under it is either a "
                f"re-delivered extract or a union spanning two tenants")

    for invoice in sorted(known):
        if invoice not in by_invoice:
            findings.append(
                f"header row for invoice {invoice!r} has no line rows at all. A header "
                f"with no lines is an invoice for nothing, and it sums to nothing, so "
                f"AME008 would agree with it")
    return findings


def base_table(table: str) -> str:
    """The bare table name out of a possibly schema-qualified one.

    A mapping `source` may be written `hub_invoice.reference_id` or
    `control.ctl_invoice_issuance.line_reference`, and wd_mapping.source_ref splits on the
    LAST dot -- so the table half arrives as `hub_invoice` in one case and
    `control.ctl_invoice_issuance` in the other. Both name the same kind of thing and the
    adjudication below compares them against base table names, so they are normalised to
    one spelling here rather than at four call sites.
    """
    return str(table).rpartition(".")[2] or str(table)


def row_value(entry: dict, row: dict, refs: dict) -> str:
    """One mapped column's value for one projected row.

    THE FULL (TABLE, COLUMN) REFERENCE IS RESOLVED, NEVER THE COLUMN ALONE. Column names
    are not unique across this vault, and the collisions are live rather than
    hypothetical: `amount` is declared by csat_invoice_line_gie, nhl_payroll_detail AND
    nhl_timesheet_line; `accounting_date` by csat_invoice_line_gie and
    sat_accounting_journal_header; `rule_version` by three satellites. Matching on the
    column alone means an entry sourced `nhl_payroll_detail.amount` resolves in Task 4's
    gate, reads as reachable here, and is then filled from the GIE satellite's
    same-named column -- a payroll figure on a customer's invoice, with nothing refusing
    it anywhere. Payroll is out of scope for this work, which makes an accidental payroll
    amount worse rather than better.

    RAISES ON A SOURCE THE PROJECTION DOES NOT CARRY, rather than returning "". A blank
    is what an UNMAPPED column is supposed to render, and a mapped column that renders
    the same thing is a mapping that silently stopped working -- indistinguishable, in
    the written file, from a field Workday was never meant to receive.
    """
    source = entry.get("source") if isinstance(entry, dict) else None
    if isinstance(source, str) and source.startswith(wd_mapping.CONSTANT_PREFIX):
        return source[len(wd_mapping.CONSTANT_PREFIX):]
    ref = wd_mapping.source_ref(entry)
    if ref is None:
        raise ValueError(
            f"mapping entry with source {source!r} names neither a constant nor a "
            f"<table>.<column>, so there is no value to write")
    gap = reference_gap(ref, refs)
    if gap:
        raise ValueError(gap)
    column = refs[(base_table(ref[0]), ref[1])]
    # THE ROW IS CHECKED AS WELL AS THE MAP. reference_gap adjudicates what the STATEMENT
    # was built to return; this catches a row that does not actually carry it -- a
    # collect() over a different statement, a Row.asDict() that dropped a column, a
    # fixture written by hand. `row.get(column)` alone would render every one of those as
    # a blank, which is exactly what an UNMAPPED column renders.
    if column not in row:
        raise ValueError(
            f"this row carries no column {column!r} (mapped from "
            f"{base_table(ref[0])}.{ref[1]}), although the projection is built to return "
            f"it. Writing it blank would be indistinguishable from a field the DCDD says "
            f"to leave empty")
    value = row[column]
    return "" if value is None else str(value)


def reference_gap(ref, refs: dict) -> str | None:
    """Why this projection cannot supply `ref`, or None when it can.

    ONE SENTENCE FOR THE TABLE-COLLISION CASE, and it is the whole point of comparing the
    pair. "this projection does not carry that column" is the wrong thing to print when
    the projection carries a column of exactly that name from a DIFFERENT table -- the
    reader then goes looking for a missing column that is right in front of them, and
    the wrong-table reading is the one that ships a wrong number.
    """
    if not isinstance(refs, dict):
        return f"the projection's reference map is {type(refs).__name__}, not a mapping"
    table, column = base_table(ref[0]), ref[1]
    if (table, column) in refs:
        return None
    elsewhere = sorted({t for t, c in refs if c == column})
    if elsewhere:
        return (f"{table}.{column} is NOT what this projection carries: it carries "
                f"{column!r} from {elsewhere}. Filling the column from a same-named one "
                f"belonging to another table is how a figure from an unrelated grain "
                f"reaches a customer's invoice with nothing refusing it")
    return (f"the projection carries no column {column!r} at all (mapped from "
            f"{table}.{column}), so this column has no value. Writing it blank would be "
            f"indistinguishable from a field the DCDD says to leave empty")


def unjoined_refusal(dcdd: dict, mapping: dict, refs: dict) -> str | None:
    """Why a mapped column cannot be filled from this projection, or None if all can.

    THE OTHER HALF OF row_value's REFUSAL, TAKEN BEFORE A SINGLE ROW IS RENDERED. Failing
    on the first row would leave a partially written file behind; failing here leaves
    none. Company_Reference_ID is the live example: hub_operating_company.reference_id is a
    real vault column that this projection has no declared join path to, and the mapping
    declares no joins -- so it is named rather than blanked.

    Adjudicates the same (table, column) pair row_value does, through the same function,
    so the pre-flight and the writer cannot disagree about what is reachable.
    """
    gaps: list[str] = []
    for csv_file, header, field, status in wd_mapping.csv_columns(dcdd, mapping):
        if status != wd_mapping.MAPPED:
            continue
        ref = wd_mapping.source_ref(mapping.get(field) or {})
        if ref is None:
            continue
        gap = reference_gap(ref, refs)
        if gap:
            gaps.append(f"  {csv_file} :: csv column {header!r} (WD field {field}) wants "
                        f"{gap}")
    if not gaps:
        return None
    return ("mapped columns this projection cannot fill, so no file is written -- a "
            "mapped column rendered blank is indistinguishable from an unmapped one:\n"
            + "\n".join(gaps))


def csv_text(columns, mapping: dict, rows, refs: dict) -> str:
    """One csv file's whole text: the HEADER row, then one line per row.

    THE csv HEADER IS WRITTEN, NEVER THE WD FIELD NAME. 31 of the 91 populated rows carry
    a csv Header that differs from the WD Field Name, so the two are not
    interchangeable -- and a file headed with field names is one DT/DTS rejects wholesale
    rather than per row.

    AN UNRESOLVED COLUMN RAISES HERE TOO. unresolved_refusal() stops a run long before
    this, but a second writer calling straight into this function must not be able to
    render a marked column as a blank: the refusal is structural, not a step somebody
    remembers to take first.

    AND IT IS CHECKED OVER THE COLUMNS, BEFORE THE ROWS, WHICH IS NOT A TIDINESS
    PREFERENCE. Inside the row loop the refusal is unreachable for an EMPTY row list --
    the loop body never runs, the marked column never raises, and the function returns a
    header-only csv naming a column nobody can source. An export with no released line is
    an ordinary state, so that is the case a refusal inside the loop would never see:
    the emptiness-makes-it-vacuous shape, found by the offline suite asserting exactly
    this call.
    """
    for header, field, status in columns:
        if status == wd_mapping.UNRESOLVED:
            raise ValueError(
                f"csv column {header!r} (WD field {field}) is mapped to a source nobody "
                f"can resolve, so it has no blank rendering and this file has no "
                f"correct form -- not even an empty one")
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow([header for header, _field, _status in columns])
    for row in rows:
        writer.writerow(["" if status == wd_mapping.UNMAPPED
                         else row_value(mapping.get(field) or {}, row, refs)
                         for _header, field, status in columns])
    return buffer.getvalue()


def export_sources(model) -> dict:
    """Every table and column name the projection needs, resolved from the model.

    DELEGATES TO checks/invoice_issue.issuance_sources RATHER THAN RESTATING IT, and the
    delegation is the point: this export joins the very ledger that writer fills, on the
    very grain it froze. Two independently typed copies of those names would still each
    render a well-formed query, and the day one moved the export would read an empty
    join and write four empty files while every gate stayed green.
    """
    src = dict(invoice_issue.issuance_sources(model))
    gie = None
    for entity in model.entities:
        if entity.base_table == GIE_TABLE:
            gie = entity
            break
    if gie is None:
        raise ValueError(
            f"metadata/entities declares no entity with base table {GIE_TABLE!r}, which "
            f"is the whole derived half of this export")
    src["payload"] = tuple(gie.payload)
    src["load_dts"] = naming.COL["load_dts"]
    src["sub_seq"] = naming.COL["sub_seq"]
    return src


def projection_columns(src: dict) -> tuple[str, ...]:
    """The column names projection_sql() returns, in order.

    ONE DEFINITION, AND NOW ACTUALLY PINNED TO THE STATEMENT. This docstring used to say
    the gate "cannot drift from the statement that produces it" while nothing whatsoever
    held projection_sql()'s SELECT list to this tuple -- a claim of the exact shape the
    ON-clause precondition had to be added for in Task 5 round 2. projection_sql() now
    reads back the aliases it rendered and refuses to return a statement whose SELECT
    list is not this tuple, in this order, so a column added to one side and not the
    other fails loudly instead of leaving the adjudication describing a statement that
    has moved. The polarity is safe: drift raises, it does not widen what is exported.
    """
    return invoice_key_columns(src) + (src["line_ref"],) + ISSUED_COLUMNS \
        + tuple(src["payload"])


#: Every `AS `name`` alias a rendered statement declares, in order. Backticked on both
#: sides, so `AS rn` in the row_number subquery -- which is not a projected column and is
#: filtered away by `WHERE rn = 1` -- is not matched.
SELECT_ALIAS_RE = re.compile(r"AS `([^`]+)`")


def rendered_columns(statement: str) -> tuple[str, ...]:
    """The column names a rendered projection statement declares, in order."""
    return tuple(SELECT_ALIAS_RE.findall(statement))


def invoice_key_columns(src: dict) -> tuple[str, str]:
    """The two columns that identify an INVOICE in the projection, resolved from the
    model rather than typed.

    They are the ledger's first two grain columns, which are hub_invoice's business keys,
    which issuance froze its rows against -- one definition, reached through the same
    `src` everything else here reads. Typing "invoice_tenant" and "invoice_reference" as
    literals is what let the header-row derivation degrade silently: rename one in
    control_standard and every row.get() returns None, every key collapses to the SAME
    (None, None), and both invoice-grained CSVs are written with one row for all
    invoices while every gate stays green.
    """
    return (src["tenant"], src["reference"])


def projection_refs(src: dict) -> dict[tuple[str, str], str]:
    """(source base table, source column) -> the name that column has in the projection.

    THE ADJUDICATION KEY IS THE PAIR, NEVER THE COLUMN ALONE -- see row_value(). This is
    the one place that says which table each projected column came out of, so the
    pre-flight refusal, the writer and the money counter all read the same answer.
    """
    refs: dict[tuple[str, str], str] = {}
    for column in invoice_key_columns(src):
        refs[(src["hub_view"], column)] = column
    refs[(src["nhl_view"], src["line_ref"])] = src["line_ref"]
    for column in ISSUED_COLUMNS:
        refs[(LEDGER, column)] = column
    for column in src["payload"]:
        refs[(src["gie_view"], column)] = column
    # THE THREE GRAIN COLUMNS ARE REACHABLE FROM THE LEDGER TOO, AND ONLY THOSE THREE.
    # They are the LEFT JOIN's whole ON clause, so l.<c> = h.<c> (or n.<c>) holds by
    # construction on every MATCHED row, and unissued_findings() refuses every unmatched
    # one before a file is written -- which makes a mapping that names the ledger's copy
    # and a projection that selects the vault's the same value, provably rather than
    # apparently. Declared as a named equivalence with its precondition, not left to
    # coincide: a blanket "same name means same column" rule would readmit every real
    # collision (amount, accounting_date, rule_version), and without this the mapping's
    # own Customer_Invoice_Line_Reference_ID would read as one of them.
    for column in join_on_columns(src):
        refs[(LEDGER, column)] = column
    return refs


def join_on_columns(src: dict) -> tuple[str, ...]:
    """The columns the projection's LEFT JOIN to the ledger equates. Read from `src`, so
    it is the same three the statement is rendered from and cannot name a fourth."""
    return invoice_key_columns(src) + (src["line_ref"],)


def invoice_rows(rows, key_columns, order_column: str) -> list[dict]:
    """One projected row per invoice, for the two invoice-grained csv files.

    THE KEY COLUMNS ARE PASSED IN, DERIVED FROM THE MODEL BY THE CALLER (see
    invoice_key_columns, which reads them off `src`), and this
    function refuses rather than degrading. Hard-coding them was a silent data defect:
    a rename in control_standard made every row.get() return None, every key became the
    same (None, None) sentinel comparing EQUAL, and the two header files shipped ONE row
    for every invoice in the run -- with the ordering guarantee lapsing at the same
    moment, since the sort key degraded to "" for every row. Both sides degrading to one
    value is the shape this plan has met repeatedly in its checks. Here it was in the
    data path, which is worse: a check that degrades goes quiet, and a writer that
    degrades sends a customer the wrong document.

    A NULL OR BLANK IN A KEY COLUMN IS REFUSED FOR THE SAME REASON. Two invoices whose
    tenant is NULL are not one invoice, and collapsing them writes one header row where
    two belong.
    """
    keys = tuple(key_columns or ())
    if len(keys) < 2 or not order_column:
        raise ValueError(
            f"an invoice is identified by at least two columns and ordered by one; got "
            f"keys {list(keys)} and order {order_column!r}. Deriving one row per invoice "
            f"from fewer is how every invoice becomes the same invoice")
    needed = keys + (order_column,)
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"projection row {index} is {type(row).__name__}, not a row")
        absent = [c for c in needed if c not in row]
        if absent:
            raise ValueError(
                f"projection row {index} carries no {absent} -- the columns an invoice "
                f"is identified and ordered by. Every row would key on the same absent "
                f"value and the header files would hold one row for all invoices")
        blank = [c for c in keys if str(row[c] if row[c] is not None else "").strip() == ""]
        if blank:
            raise ValueError(
                f"projection row {index} has {blank} null or blank -- two invoices that "
                f"share a blank key are not one invoice, and collapsing them writes one "
                f"header row where two belong")
    out, seen = [], set()
    for row in sorted(rows, key=lambda r: str(r[order_column])):
        key = tuple(row[c] for c in keys)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def projection_sql(catalog: str, vault_schema: str, business_vault_schema: str,
                   control_schema: str, src: dict) -> str:
    """One row per line the business vault holds, with the issued values beside it.

    APPLIES AME010 (Project Number) as the direct map it is: `project_number` comes
    straight off the GIE satellite's payload with no derivation, which is why it has no
    function in invoice_rules.py.

    IT DOES NOT APPLY AME001 (Invoice Number), AND SAYING IT DID WAS THE DEFECT. This
    docstring claimed both rules came "straight off the GIE satellite's payload"; the GIE
    payload has no `invoice_number` column, so the sentence was false -- and verify_repo's
    AME traceability gate was satisfied by substring-matching it. The statement below
    selects the payload the satellite actually declares, which does not include it. See
    UNDECLARED_SOURCES and required_field_refusal().

    THE SELECT LIST IS HELD TO projection_columns(), IN ORDER, BEFORE THIS RETURNS. That
    tuple is what the pre-flight refusal, the writer and the money counter all adjudicate
    the mapping against, and until now nothing tied it to the statement it claims to
    describe -- so a column added here and not there (or renamed on one side) would have
    left every gate adjudicating a statement that had moved. Reading the aliases back off
    the rendered text is deliberately the dumbest possible check: it cannot agree with
    the tuple by construction, which is what makes it able to fail.

    A ROW_NUMBER SUBQUERY, NOT QUALIFY. The GIE satellite is historised, so a restated
    line has several versions and the export wants the current one. Databricks SQL
    accepts QUALIFY and OSS Spark 3.5's parser does not, and this repo re-parses every
    rendered statement in local Spark -- a construct only the workspace can parse is a
    defect nothing offline can see. Same idiom as load_satellites.py's `current` CTE,
    ordered by load_dts then sub_seq, because load_dts ties inside a batch.

    LEFT JOIN ON THE LEDGER, NEVER INNER. See unissued_findings(): an inner join drops a
    line issuance never saw, silently, and a missing line is the well-formed wrong
    invoice this whole subsystem exists to refuse.
    """
    payload = ",\n".join(f"  g.`{c}` AS `{c}`" for c in src["payload"])
    inner = ", ".join(f"`{c}`" for c in (src["line_hk"],) + tuple(src["payload"]))
    statement = (
        f"WITH gie AS (\n"
        f"  SELECT {inner} FROM (\n"
        f"    SELECT {inner}, row_number() OVER (\n"
        f"      PARTITION BY `{src['line_hk']}` ORDER BY `{src['load_dts']}` DESC, "
        f"`{src['sub_seq']}` DESC) AS rn\n"
        f"    FROM {_q(catalog, business_vault_schema, src['gie_view'])}) WHERE rn = 1\n"
        f")\n"
        f"SELECT\n"
        f"  h.`{src['tenant']}` AS `{src['tenant']}`,\n"
        f"  h.`{src['reference']}` AS `{src['reference']}`,\n"
        f"  n.`{src['line_ref']}` AS `{src['line_ref']}`,\n"
        f"  l.`line_number` AS `line_number`,\n"
        f"  CAST(l.`invoice_date` AS STRING) AS `invoice_date`,\n"
        f"{payload}\n"
        f"FROM gie g\n"
        f"JOIN {_q(catalog, vault_schema, src['nhl_view'])} n\n"
        f"  ON n.`{src['line_hk']}` = g.`{src['line_hk']}`\n"
        f"JOIN {_q(catalog, vault_schema, src['hub_view'])} h\n"
        f"  ON h.`{src['hub_hk']}` = n.`{src['invoice_hk']}`\n"
        f"LEFT JOIN {_q(catalog, control_schema, LEDGER)} l\n"
        f"  ON l.`{src['tenant']}` = h.`{src['tenant']}`\n"
        f"  AND l.`{src['reference']}` = h.`{src['reference']}`\n"
        f"  AND l.`{src['line_ref']}` = n.`{src['line_ref']}`"
    )
    declared, rendered = projection_columns(src), rendered_columns(statement)
    if rendered != declared:
        raise ValueError(
            f"the projection statement returns {list(rendered)} but projection_columns() "
            f"declares {list(declared)}. Every gate in this module adjudicates the "
            f"mapping against the second, and the file is written from the first -- so a "
            f"statement that has moved away from it is a set of gates describing "
            f"something that is no longer being written")
    return statement


def _q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def reference_sources(model) -> dict:
    """The stable view and two key columns hub_wd_reference publishes, from the model.

    RESOLVED, NOT TYPED, for the reason invoice_issue.issuance_sources gives: the vault
    versions its physical tables and publishes an unversioned view over them, so a rev
    bump must move this read with it rather than leave it pointed at the old table. A
    typed name that no longer exists fails loudly; a typed name that exists and is stale
    reads a frozen reference set and validates this year's invoices against last year's
    codes.
    """
    hub = None
    for entity in model.entities:
        if entity.base_table == REFERENCE_TABLE:
            hub = entity
            break
    if hub is None:
        raise ValueError(
            f"metadata/entities declares no entity with base table {REFERENCE_TABLE!r}, "
            f"which is the only thing the DCDD's CHECKREFERENCES fields could be "
            f"validated against")
    tables = [table for _src, table in hub.tables()]
    if len(tables) != 1:
        raise ValueError(
            f"{REFERENCE_TABLE} resolves to {len(tables)} physical table(s) ({tables}); "
            f"this read needs exactly one, and choosing among them is a modelling "
            f"decision rather than this writer's")
    keys = tuple(hub.business_keys)
    if len(keys) != 2:
        raise ValueError(
            f"{REFERENCE_TABLE} is grained on {list(keys)}; this read needs exactly the "
            f"pair (reference type, reference id), because a membership test that loses "
            f"the TYPE lets a Tax_Code_ID satisfy a Currency_Reference_ID column")
    return {"view": naming.stable(tables[0]), "type": keys[0], "id": keys[1]}


def reference_sql(catalog: str, vault_schema: str, ref: dict) -> str:
    """The two columns that make a reference set: its type, and the value.

    NO SEMICOLON, NO QUALIFY -- the repo's rules, and this statement is re-parsed in OSS
    Spark 3.5 by the suite like every other one here.
    """
    return (f"SELECT `{ref['type']}` AS `{ref['type']}`, `{ref['id']}` AS `{ref['id']}`\n"
            f"FROM {_q(catalog, vault_schema, ref['view'])}")


def reference_values(rows, ref: dict) -> dict[str, set[str]]:
    """reference_id_type -> its permitted values, from the rows hub_wd_reference gave.

    AN EMPTY DICT IS A REAL AND EXPECTED ANSWER, AND IT IS NOT None. The two say
    different things -- "the table was read and holds nothing" against "the table was
    never read" -- and dcdd_validation.allowed_values prints a different sentence for
    each, because two states that degrade to one sentinel are two failures that compare
    equal. Only the caller's own guard distinguishes them, so this never invents one.
    """
    out: dict[str, set[str]] = {}
    for row in rows if isinstance(rows, (list, tuple)) else ():
        if not isinstance(row, dict):
            continue
        kind = str(row.get(ref["type"]) or "").strip()
        value = str(row.get(ref["id"]) or "").strip()
        if not kind or not value:
            continue
        out.setdefault(kind, set()).add(value)
    return out


def reference_refusal(dcdd: dict, refvals) -> str | None:
    """Why this run cannot claim the DCDD's validations were applied, or None if it can.

    SUBSYSTEM E, AND WHY THIS IS A REFUSAL RATHER THAN A WARNING. 27 of the 91 populated
    workbook rows carry CHECKREFERENCES, and they need Workday reference values this lake
    does not hold: hub_wd_reference exists and holds 0 rows, because its WORKDAY binding
    lands from a bronze schema that does not exist yet. A membership test against an
    empty set finds zero violations -- and zero violations rendered as a pass reads, on a
    customer's invoice, as "27 fields were checked" when nothing was compared against
    anything. An empty result set is not agreement.

    NO FLAG TURNS THIS OFF. A flag is how a temporary suppression becomes permanent, and
    the suppression on offer is shipping an invoice claiming 27 reference fields were
    validated.

    THE ADJUDICATION LIVES IN dcdd_validation, not here. The same function decides the
    per-rule narration this task prints on every run and the refusal it returns on, so
    the log and the gate cannot disagree about which rules were evaluated.
    """
    return dcdd_validation.coverage_refusal(dcdd, refvals)


def fielded_for_validation(columns, mapping: dict, rows, refs: dict) -> list[dict]:
    """One dict per row keyed by WD FIELD NAME, holding exactly what csv_text will write.

    RENDERED THROUGH row_value, THE SAME FUNCTION THE WRITER USES, so what is validated
    is what is shipped. A second rendering would still produce a plausible string for
    every column and would be validated instead of the file.

    ONLY THE MAPPED COLUMNS. An UNMAPPED column is written blank by design -- the DCDD
    lists it and nothing sources it -- and running MISSINGVALUE over those would report a
    violation on every row of every file for a state wd_mapping.mapping_findings and
    must_populate_fields already adjudicate, burying the violations that are about DATA.
    """
    mapped = [field for _header, field, status in columns
              if status == wd_mapping.MAPPED]
    return [{field: row_value(mapping.get(field) or {}, row, refs) for field in mapped}
            for row in rows]


def validation_findings(columns_by_file, grain: dict, mapping: dict, rows, per_invoice,
                        refs: dict, dcdd: dict, refvals) -> list[str]:
    """Every DCDD validation the rows about to be written break, per file, as sentences.

    PER FILE AND AT THAT FILE'S OWN GRAIN, because the two invoice-grained files carry
    one row per invoice and the two line files one row per line -- validating the line
    set and writing the invoice set would check a superset and report on neither.
    """
    findings: list[str] = []
    for name in sorted(columns_by_file):
        columns = columns_by_file[name]
        source_rows = rows if grain.get(name) == LINE_GRAIN else per_invoice
        fielded = fielded_for_validation(columns, mapping, source_rows, refs)
        fields = {field: dcdd[field] for _header, field, status in columns
                  if status == wd_mapping.MAPPED and field in dcdd}
        findings.extend(f"{name}.csv :: {finding}" for finding
                        in dcdd_validation.gate_findings(fielded, fields, refvals))
    return findings


def _fielded_per_file(columns_by_file, grain: dict, mapping: dict, rows, per_invoice,
                      refs: dict, dcdd: dict):
    """(file name, rows keyed by WD field, the DCDD entries for its mapped fields).

    The three things all three validation readers need, derived once and in one place so
    they cannot disagree about which rows or which fields a file was judged on.
    """
    for name in sorted(columns_by_file):
        columns = columns_by_file[name]
        source_rows = rows if grain.get(name) == LINE_GRAIN else per_invoice
        yield (name,
               fielded_for_validation(columns, mapping, source_rows, refs),
               {field: dcdd[field] for _header, field, status in columns
                if status == wd_mapping.MAPPED and field in dcdd})


def validation_outcome_lines(columns_by_file, grain: dict, mapping: dict, rows,
                             per_invoice, refs: dict, dcdd: dict, refvals) -> list[str]:
    """Per file and per rule, what the DCDD's validations actually DID to the rows this
    run would write.

    WIRED BECAUSE dcdd_validation.rule_outcomes() HAD NO PRODUCTION CALLER AT ALL -- it
    was written, unit tested, and read by nothing, which is the same shape as the two
    release gates this round found orphaned. It is worth wiring rather than deleting
    because it answers the one question coverage_lines() cannot: coverage_lines() reports
    what the WORKBOOK asks for, before any row exists; this reports what was actually
    compared, and a PASS carrying `0 value(s) checked` is visible here and nowhere else.

    A RULE NO FIELD IN THIS FILE CARRIES IS NOT PRINTED. KNOWN_RULES is the whole
    catalogue, and narrating five rules per file that the file has nothing to say about
    buries the two that do.
    """
    lines: list[str] = []
    for name, fielded, fields in _fielded_per_file(columns_by_file, grain, mapping, rows,
                                                   per_invoice, refs, dcdd):
        for record in dcdd_validation.rule_outcomes(fielded, fields, refvals):
            if not record["fields"]:
                continue
            reason = record["reason"]
            lines.append(
                f"{name}.csv :: {record['rule']} :: {record['status']} :: "
                f"{record['checks']} value(s) checked over {len(record['fields'])} "
                f"field(s) {record['fields']} :: {len(record['problems'])} violation(s)"
                + (f" :: {reason}" if reason else ""))
    return lines


def validation_violations(columns_by_file, grain: dict, mapping: dict, rows, per_invoice,
                          refs: dict, dcdd: dict, refvals) -> list[str]:
    """Only the violations the rows commit -- NOT the rules that were not evaluated.

    WIRED BECAUSE dcdd_validation.problems() HAD NO PRODUCTION CALLER EITHER, and it is
    the right list for exactly one job: telling an operator how many of the findings are
    VALUES THEY CAN FIX. validation_findings() refuses on gate_findings(), which folds
    unevaluated rules in with real violations on purpose -- an empty list of violations is
    what a run that compared nothing produces, and that is what a pass looks like. So this
    list is never what the gate decides on; it is only ever the number in the sentence.
    """
    out: list[str] = []
    for name, fielded, fields in _fielded_per_file(columns_by_file, grain, mapping, rows,
                                                   per_invoice, refs, dcdd):
        out.extend(f"{name}.csv :: {p}"
                   for p in dcdd_validation.problems(fielded, fields, refvals))
    return out


def _partial(written: int) -> str:
    """What state the export volume is in when a write loop stops part way through.

    "0 file(s) had already been written; this run is PARTIAL" was printed for a failure
    on the FIRST file, which is the opposite of partial -- the volume is untouched and
    whoever reads that line at 3am goes looking for files to clean up that do not exist.
    The two cases are different facts about the world and now read differently.
    """
    if written <= 0:
        return ("No file had been written yet, so the destination is UNCHANGED and there "
                "is nothing to clean up.")
    return (f"{written} file(s) had already been written; this run is PARTIAL and the "
            f"destination now holds an INCOMPLETE set, which does not join.")


def finish(status: str, written: int, not_evaluated: int, code: int,
           reason: str) -> int:
    """The one machine-readable line this task ends on.

    `reason` EXISTS BECAUSE status ALONE CONFLATES THREE DIFFERENT WORLDS. NOT_EVALUATED
    is printed for a business vault that holds nothing to export, for a mapping question
    only the repo owner can settle, and for reference data subsystem E has not delivered
    -- three states with three different owners and three different remedies. The prose
    above the summary distinguishes them; the line somebody greps did not, so an alert
    built on it could not tell "nothing to do" from "somebody must decide something".
    Every terminal path names its own token, including the failures, so a log can be
    counted by cause rather than only by colour.
    """
    print(f"GATE SUMMARY :: {GATE} :: status={status} files_written={written} "
          f"not_evaluated={not_evaluated} reason={reason}")
    return code


def main() -> int:
    """Project the released invoice lines and write the four CSV files -- or refuse, out
    loud, naming what stopped it.

    WHAT A RUN DOES TODAY, AND WHY IT IS NOT A PASS. Two of the five mapped fields carry
    `unresolved` markers naming source columns that do not exist, and one of them is
    Customer_Invoice_ID -- the must-populate join key on all four files. So every run
    finishes NOT_EVALUATED, writes nothing, and prints both reasons. It does not print
    PASSED and it does not blank the columns: four CSVs joined on an empty string look
    exactly like a correct export, and nothing downstream would re-check them.

    EVERY WORKSPACE CALL IS GUARDED and every failure returns a GATE SUMMARY line rather
    than a traceback, for the reason invoice_issue gives: a spark_python_task that dies
    by traceback reports a failed task and nothing else, and the first thing anyone then
    does is guess which statement it was.

    THE ORDER OF THE GATES IS THE POINT. Everything that can be adjudicated offline --
    the destination, the mapping's money flags, the unresolved markers -- is adjudicated
    before a session is opened, so a misconfigured run costs no cluster time and names
    its reason in the first lines of the log.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the issuance ledger, whose two frozen values "
                         "this export reads and never derives")
    ap.add_argument("--business-vault-schema", required=True,
                    help=f"schema holding {GIE_TABLE}")
    ap.add_argument("--gold-catalog", required=True)
    ap.add_argument("--gold-export-schema", required=True,
                    help="schema inside the gold catalog the export tables live in")
    ap.add_argument("--export-volume", default="",
                    help="UC Volume path the csv file set is written to, from the "
                         "bundle variable gold_export_volume. NEVER DEFAULTED -- see "
                         "volume_refusal(); a run without it refuses and says so")
    ap.add_argument("--vault-schema", default="raw_vault",
                    help="schema holding nhl_invoice_line and hub_invoice, which carry "
                         "the ledger's three grain columns. Defaulted to the bundle's "
                         "own default -- reading the wrong schema fails on a missing "
                         "table, it does not write a wrong value")
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="render the projection without a workspace, write nothing")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    meta = Path(args.metadata) if args.metadata else (root / "metadata" / "entities")
    try:
        model = spec.load_model(meta)
        src = export_sources(model)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the export projection cannot be built from the model -- "
              f"{exc}")
        return finish("FAILED", 0, 0, 1, "model_unreadable")

    try:
        dcdd = wd_mapping.dcdd_populated_fields(root.joinpath(*DCDD_FILE), DCDD_SHEET)
        mapping = wd_mapping.load_mapping(root.joinpath(*MAPPING_FILE))
        columns_by_file = file_columns(dcdd, mapping)
        grain = file_grain(dcdd, mapping)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the DCDD and its mapping could not be read -- {exc}")
        return finish("FAILED", 0, 0, 1, "mapping_unreadable")

    # GUARDED, BECAUSE THE COMMENT BELOW SAYS EVERYTHING IS. This read was the one
    # statement in main() outside every try, directly contradicting "the three calls
    # below were the last ones outside a guard" -- and a false claim of that exact shape
    # is what this review round was looking for. wd_mapping.CSV_FILES renamed or turned
    # into something unhashable would have left this task by traceback with no GATE
    # SUMMARY line at all, which is the one outcome this file is arranged to prevent.
    try:
        declared = set(wd_mapping.CSV_FILES)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the four output file names could not be read from "
              f"wd_mapping.CSV_FILES -- {type(exc).__name__}: {exc}. Nothing was read "
              f"and nothing was written.")
        return finish("FAILED", 0, 0, 1, "file_set_unreadable")

    if set(columns_by_file) != declared:
        print(f"GATE FAILED: the workbook resolves to files {sorted(columns_by_file)}, "
              f"not {sorted(declared)}. Workday's loader takes those four names and no "
              f"others, so a fifth file is one nobody collects and a missing one is a "
              f"set that does not join.")
        return finish("FAILED", 0, 0, 1, "file_set_wrong")

    # THE DCDD'S OWN VALIDATION RULES, NAMED PER RULE, ON EVERY RUN -- including the runs
    # that refuse for a reason that has nothing to do with them. 27 of the 91 populated
    # workbook rows carry CHECKREFERENCES and none of the three fields the mapping
    # currently resolves is one of them, so a report built only from what gets written
    # would say NOTHING about the 27 -- and silence is exactly how "27 fields validated"
    # comes to be believed. This reads the workbook, so they are named either way.
    #
    # `None` HERE IS NOT A GUESS THAT THE TABLE IS EMPTY. It says this narration was
    # taken before any workspace was opened, which is a different sentence from the one
    # a measured zero produces below.
    try:
        print(f"\n-- DCDD validation coverage :: "
              f"{wd_mapping.dcdd_row_count(dcdd)} populated workbook row(s), "
              f"{dcdd_validation.unvalidated_rows(dcdd)} of them carrying no rule at all")
        for line in dcdd_validation.coverage_lines(dcdd, None):
            print(f"     {line}")
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the DCDD's VALIDATIONS column could not be read -- "
              f"{type(exc).__name__}: {exc}. A run that cannot say which rules apply "
              f"must not write a file claiming they did.")
        return finish("FAILED", 0, 0, 1, "validations_unreadable")

    # EVERYTHING THAT READS `src` OR THE MAPPING IS INSIDE A try, AND THE MODULE'S OWN
    # DOCSTRING IS WHY. A KeyError from a renamed key, or an arity change in
    # wd_mapping.csv_columns, would otherwise leave this task by traceback with no GATE
    # SUMMARY line at all -- no status, no statement, no count, which is precisely the
    # outcome the rest of this file is arranged to prevent. NOTHING in main() is outside
    # a guard now: the wd_mapping.CSV_FILES read above was the last one, and it is not
    # any more.
    try:
        statement = projection_sql(args.catalog, args.vault_schema,
                                   args.business_vault_schema, args.control_schema, src)
        ref_src = reference_sources(model)
        reference_statement = reference_sql(args.catalog, args.vault_schema, ref_src)
        refs = projection_refs(src)
        offline = (("the destination", volume_refusal(args.export_volume), True),
                   ("the mapping's money declarations",
                    money_declaration_refusal(mapping, masked_columns(model)), True),
                   ("unresolved sources", unresolved_refusal(dcdd, mapping), False),
                   ("sources this projection cannot reach",
                    unjoined_refusal(dcdd, mapping, refs), False),
                   # THE PRECONDITION OF THE RELEASE GATES, not a third kind of
                   # question. release_findings() below applies missing_required() over
                   # REQUIRED_FIELDS, and a required field with no declared source column
                   # would make it reject every line of every invoice. So it is refused
                   # here, with the other decisions that belong to the owner, rather than
                   # allowed to surface as a 100% rejection rate that looks like bad data.
                   ("required fields with no declared source",
                    required_field_refusal(src), False))
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the projection and its offline gates could not be built -- "
              f"{type(exc).__name__}: {exc}. Nothing was read and nothing was written.")
        return finish("FAILED", 0, 0, 1, "gates_unbuildable")

    if args.dry_run:
        print(f"-- {len(columns_by_file)} csv file(s): "
              + ", ".join(f"{name} ({len(cols)} columns, {grain[name]} grain)"
                          for name, cols in sorted(columns_by_file.items())))
        print(f"-- destination: {args.export_volume!r}")
        print(f"-- gold export schema: "
              f"`{args.gold_catalog}`.`{args.gold_export_schema}`")
        print("-- the projection: csat_invoice_line_gie, current version, with the "
              "issued values")
        print(statement)
        print("-- the reference set the DCDD's CHECKREFERENCES rules would be tested "
              "against:")
        print(reference_statement)

    # A MISCONFIGURED RUN IS A FAILURE. Both of these are somebody's mistake, correctable
    # without anyone deciding anything: a destination nobody set, and a `money` flag that
    # disagrees with the masks the entity model declares.
    for label, refusal, is_failure in offline:
        if refusal and is_failure:
            print(f"\nEXPORT REFUSED -- {label}. Nothing was written.\n{refusal}")
            return finish("FAILED", 0, 0, 1, "misconfigured")

    # AN OPEN QUESTION IS NOT A FAILURE, AND IT IS NOT A PASS EITHER. Both of these are
    # decisions about a customer's invoice that nobody has made yet -- a source column
    # that does not exist, and a source column that exists with no declared path to it --
    # so the run says so and returns 0, exactly as invoice_issue does with AME002's date.
    # The ordering edge behind this task is real and stays runnable; what does NOT happen
    # is a file. They are reported TOGETHER rather than one per run, because whoever
    # settles them should see all of them at once instead of discovering the next one on
    # the next run.
    blockers = [(label, refusal) for label, refusal, is_failure in offline
                if refusal and not is_failure]
    if blockers:
        print(f"\nEXPORT REFUSED -- {len(blockers)} unsettled question(s) about the "
              f"mapping. Nothing was written, and no part of this run is a pass.")
        for label, refusal in blockers:
            print(f"\n  {label.upper()}\n{refusal}")
        return finish("NOT_EVALUATED", 0, len(blockers), 0, "unsettled_mapping")

    if args.dry_run:
        print("\ndry run only -- nothing executed, nothing written")
        return finish("NOT_EVALUATED", 0, 1, 0, "dry_run")

    from pyspark.sql import SparkSession  # noqa: PLC0415 - lazy, so the suite stays pure

    spark = SparkSession.builder.getOrCreate()
    try:
        rows = [row.asDict() for row in spark.sql(statement).collect()]
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the export projection could not be read -- {exc}\nThe "
              f"statement was:\n{statement}\nNothing was written.")
        return finish("FAILED", 0, 0, 1, "projection_unreadable")

    if not rows:
        print("\nGATE NOT EVALUATED: the projection returned no line at all -- either "
              "the business vault holds nothing to export, or the join found nothing. "
              "Those two look identical in a log that claims a pass, so this one does "
              "not make that claim.")
        return finish("NOT_EVALUATED", 0, 1, 0, "empty_projection")

    unissued = unissued_findings(rows)
    if unissued:
        print(f"\nEXPORT REFUSED -- {len(unissued)} line(s) the ledger never issued. "
              f"Nothing was written.")
        for finding in unissued[:20]:
            print(f"  * {finding}")
        return finish("FAILED", 0, 0, 1, "unissued_lines")

    # THE COUNT IS TAKEN THROUGH THE SAME (table, column) MAP THE WRITER READS. Counting
    # `ref[1]` alone would count csat_invoice_line_gie.amount for an entry sourced
    # nhl_payroll_detail.amount and report the masked column readable on the strength of
    # a different table's values.
    try:
        counts = {}
        for name, ref in wd_mapping.claimed_source_refs(mapping).items():
            column = refs.get((base_table(ref[0]), ref[1]))
            if column is None:
                continue
            counts[name] = sum(
                1 for r in rows
                if str(r.get(column) if r.get(column) is not None else "").strip() != "")
        money = money_refusal(mapping, counts)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the money columns could not be counted -- {exc}")
        return finish("FAILED", 0, 0, 1, "money_uncountable")
    if money:
        print(f"\nEXPORT REFUSED -- {money}\nNothing was written.")
        return finish("FAILED", 0, 0, 1, "money_unreadable")

    # THE SET THAT IS CHECKED IS THE SET THAT IS WRITTEN, and it is the same object.
    # These used to be two derivations of "one row per invoice" -- the checked one
    # deduplicated on the mapped Customer_Invoice_ID, the written one on the projection's
    # own grain -- so two tenants sharing an invoice_reference gave the writer two rows
    # and the gate one, and the duplicate shipped unseen. A gate that validates a
    # different set than it ships is not a gate. per_invoice is now derived ONCE and the
    # checked rows are a field-keyed view of exactly those rows, which is also what makes
    # join_findings' duplicate-header rule able to see the collision at all.
    try:
        per_invoice, header_rows, by_field = export_rows(rows, mapping, refs, src)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: one row per invoice could not be derived -- "
              f"{type(exc).__name__}: {exc}. Nothing was written.")
        return finish("FAILED", 0, 0, 1, "invoice_grain_underivable")
    findings = join_findings(header_rows, by_field)
    if findings:
        print(f"\nEXPORT REFUSED -- the four files would not be a joinable set "
              f"({len(findings)} finding(s)). Nothing was written.")
        for finding in findings[:20]:
            print(f"  * {finding}")
        return finish("FAILED", 0, 0, 1, "unjoinable_set")

    # ======================================================================== #
    # THE TWO RELEASE GATES THE MODULE DOCSTRING PROMISES: AME008 per invoice, and the
    # required fields per line. They existed as pure functions from Task 1 and were
    # called by NOTHING but the test suite until this round -- so the day the mapping is
    # settled, an invoice whose lines did not sum to its gross would have passed every
    # shape, join and DCDD gate above and been exported. That is the well-formed invoice
    # with a line missing which §6 of the design spec exists to refuse.
    #
    # BEFORE the DCDD's value rules, because these are about whether the DOCUMENT may be
    # released at all, and a run that cannot release the invoice has no business
    # reporting on the formatting of its columns. Both refusals are still before the
    # first byte, so neither leaves a partial file behind.
    #
    # required_field_refusal() has already cleared by the time this runs -- it is one of
    # the offline blockers above -- so every field missing_required() names here is a
    # value that is BLANK, never a column the projection does not carry.
    # ======================================================================== #
    try:
        release = release_findings(rows, src)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the release gates could not be applied -- "
              f"{type(exc).__name__}: {exc}. Nothing was written, and no invoice is "
              f"reported as releasable.")
        return finish("FAILED", 0, 0, 1, "release_gates_unapplied")
    if release:
        print(f"\nEXPORT REFUSED -- {len(release)} line(s)/invoice(s) the release gates "
              f"hold back (AME008, and the required fields). Nothing was written: a "
              f"well-formed invoice with a line missing looks correct and gets paid, "
              f"which is worse than no invoice at all.")
        for finding in release[:20]:
            print(f"  * {finding}")
        return finish("FAILED", 0, 0, 1, "not_releasable")

    # ======================================================================== #
    # THE DCDD'S OWN VALIDATION RULES, AGAINST THE ROWS THAT WOULD BE WRITTEN.
    # Everything above this point adjudicates the SHAPE of the export -- which columns,
    # which files, which grain, which join. This is the workbook's own rules about the
    # VALUES, and both refusals below run before the first byte, so neither leaves a
    # partial file behind.
    # ======================================================================== #
    try:
        ref_rows = [row.asDict() for row in spark.sql(reference_statement).collect()]
        refvals = reference_values(ref_rows, ref_src)
    except Exception as exc:  # noqa: BLE001
        # DELIBERATELY NOT {}. A table that could not be read and a table that was read
        # and holds nothing are different states of the world, and dcdd_validation prints
        # a different sentence for each. Collapsing them to one value here is how the
        # failure nobody could measure becomes the failure somebody measured as empty --
        # the same shape that let two error strings compare equal and satisfy a check.
        print(f"\n  {REFERENCE_TABLE} could not be read -- {type(exc).__name__}: {exc}")
        refvals = None

    # ONE try OVER ALL THREE, FOR THE REASON THIS FILE STATES ABOVE: everything that
    # reads the DCDD or the mapping is inside a guard, so a future entry shape that
    # raises inside coverage() leaves a GATE SUMMARY line naming what failed rather than
    # exiting by traceback with no status, no statement and no count -- the outcome the
    # rest of this file is arranged to prevent, and the one that reads in a task log
    # exactly like an ordinary red. The identical calls in the offline block are already
    # guarded this way and these two were not.
    #
    # breaches IS NOT COMPUTED WHEN THE RUN IS ALREADY REFUSING. Validating rows against
    # rules this run could not evaluate would list findings from the four local rules
    # under a refusal that is about the other three, and the reader would fix those
    # first.
    try:
        _gaps = [r for r in dcdd_validation.coverage(dcdd, refvals) if not r["evaluable"]]
        reference = reference_refusal(dcdd, refvals)
        breaches = [] if reference else validation_findings(
            columns_by_file, grain, mapping, rows, per_invoice, refs, dcdd, refvals)
        # WHAT THE RULES ACTUALLY DID, AND HOW MANY OF THE FINDINGS ARE FIXABLE VALUES.
        # coverage_lines() above narrates what the WORKBOOK asks for, before any row
        # exists. These two narrate what was compared, and separate the violations an
        # operator can fix from the rules this run could not evaluate -- one number is
        # somebody's afternoon, the other is somebody else's dependency.
        outcomes = [] if reference else validation_outcome_lines(
            columns_by_file, grain, mapping, rows, per_invoice, refs, dcdd, refvals)
        violations = [] if reference else validation_violations(
            columns_by_file, grain, mapping, rows, per_invoice, refs, dcdd, refvals)
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the DCDD validations could not be adjudicated -- "
              f"{type(exc).__name__}: {exc}. Nothing was written, and no rule is "
              f"reported as having been applied.")
        return finish("FAILED", 0, 0, 1, "validations_unadjudicable")

    if reference:
        _held = 0 if refvals is None else sum(len(v) for v in refvals.values())
        print(f"\nEXPORT REFUSED -- {len(_gaps)} of the DCDD's validation rule(s) could "
              f"NOT be evaluated by this run, so nothing was written. {REFERENCE_TABLE} "
              f"is the only thing CHECKREFERENCES could be tested against, and this run "
              f"has {_held} usable value(s) from it (subsystem E: its WORKDAY binding is "
              f"inactive because 01_usnc_bronze_dev has no workday schema). A table "
              f"holding 0 rows reports no violations against every value in the file, so "
              f"\"nothing was wrong\" and \"nothing was compared\" would print the same "
              f"way -- and this run will not print the first when it means the second.")
        print(reference)
        return finish("NOT_EVALUATED", 0, len(_gaps), 0, "reference_data_absent")

    print(f"\n-- DCDD validation outcomes on the {len(rows)} line row(s) and "
          f"{len(per_invoice)} invoice row(s) this run would write:")
    for line in outcomes:
        print(f"     {line}")

    if breaches:
        print(f"\nEXPORT REFUSED -- {len(breaches)} DCDD validation finding(s) on the "
              f"rows this run would write, {len(violations)} of which are values in "
              f"these rows and the rest rules this run could not evaluate. Workday "
              f"rejects the FILE, not the row, so one bad value loses the whole export "
              f"-- and every one of them is listed rather than the first, because they "
              f"get fixed one at a time.")
        for finding in breaches[:20]:
            print(f"  * {finding}")
        return finish("FAILED", 0, 0, 1, "validation_breaches")

    written = 0
    for name in sorted(columns_by_file):
        source_rows = rows if grain[name] == LINE_GRAIN else per_invoice
        try:
            text = csv_text(columns_by_file[name], mapping, source_rows, refs)
        except Exception as exc:  # noqa: BLE001
            print(f"GATE FAILED: {name}.csv could not be rendered -- {exc}\n"
                  f"{_partial(written)}")
            return finish("FAILED", written, 0, 1, "render_failed")
        target = f"{args.export_volume.rstrip('/')}/{name}.csv"
        try:
            with open(target, "w", encoding="utf-8", newline="") as handle:
                handle.write(text)
        except Exception as exc:  # noqa: BLE001
            # THE ACTION LINE EXISTS BECAUSE NOTHING IN THIS REPO PROVISIONS THE VOLUME.
            # volume_refusal() adjudicates the SHAPE of --export-volume and stops there:
            # a well-formed path to a volume that was never created passes it, and the
            # first real write is where that is discovered. Without a remedy attached,
            # what reaches the on-call is "could not be written -- [Errno 2]" at 3am
            # against a target nobody in this repo declares.
            print(f"GATE FAILED: {target} could not be written -- "
                  f"{type(exc).__name__}: {exc}\n{_partial(written)}\n"
                  f"  ACTION: nothing in this repo creates the destination volume -- "
                  f"databricks.yml declares the bundle variable gold_export_volume and "
                  f"no resource behind it. If the path is right, the UC Volume it names "
                  f"has to be created and this run-as identity granted WRITE VOLUME on "
                  f"it; if the path is wrong, the variable is where it is set per target."
                  )
            return finish("FAILED", written, 0, 1, "write_failed")
        written += 1
        print(f"  wrote {target} -- {len(source_rows)} row(s), "
              f"{len(columns_by_file[name])} column(s)")

    print(f"\nEXPORT PASSED: {written} file(s) over {len(header_rows)} invoice(s) and "
          f"{len(rows)} line(s)")
    return finish("PASSED", written, 0, 0, "written")


def export_rows(rows, mapping: dict, refs: dict, src: dict):
    """(rows written to the header files, those same rows field-keyed, the line rows).

    THE SET THAT IS CHECKED AND THE SET THAT IS WRITTEN ARE DERIVED HERE, TOGETHER, FROM
    ONE OBJECT -- and that is the whole reason this function exists rather than three
    statements in main(). They used to be two derivations of "one row per invoice": the
    checked one deduplicated on the mapped Customer_Invoice_ID, the written one on the
    projection's own grain. Two tenants sharing an invoice_reference then gave the writer
    two header rows and the gate one, so the duplicate shipped having been validated by
    nothing. A gate that adjudicates a different set than it ships is not a gate.

    The second element is a one-to-one, order-preserving image of the first: same rows,
    keyed by WD field name instead of by projection column. Anything join_findings says
    about element two is therefore true of the rows that reach the file, and the offline
    suite asserts that correspondence rather than trusting this docstring.
    """
    per_invoice = invoice_rows(rows, invoice_key_columns(src), src["line_ref"])
    return (per_invoice,
            [_fielded(row, mapping, refs) for row in per_invoice],
            [_fielded(row, mapping, refs) for row in rows])


def _fielded(row: dict, mapping: dict, refs: dict) -> dict:
    """One projected row keyed by WD FIELD NAME, for the join contract.

    join_findings speaks field names because that is what the DCDD and the mapping both
    speak; the csv header is what gets written and nothing else. Only the two join keys
    are needed, and a field whose source the projection does not carry is left ABSENT
    rather than blank -- join_findings then reports it as the missing key it is.
    """
    out: dict = {}
    for field in (JOIN_KEY, LINE_KEY):
        entry = mapping.get(field)
        if not isinstance(entry, dict) or field in wd_mapping.unresolved_fields(mapping):
            continue
        try:
            out[field] = row_value(entry, row, refs)
        except ValueError:
            continue
    return out


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
