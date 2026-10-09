"""The DCDD's own VALIDATIONS column, applied to the rows we are about to write.

PURE AND SPARK-FREE, for the reason wd_reference.py gives: deciding whether a customer's
invoice is well formed is a modelling decision, and a modelling decision that can only be
tested by running a pipeline does not get tested. Everything here runs in the offline
suite against literal dicts.

WHAT THE WORKBOOK ACTUALLY ASKS FOR. Over the 91 populated rows of
Submit_Customer_Invoice_DCDD, column L names seven rules:

    CHECKREFERENCES 27 · ISNUMERIC 12 · DATEFORMATCHECK 6 · CHECKBOOLEAN 4
    MISSINGVALUE 4 · MATCHVALUE_COMPNY 3 · MATCHVALUE_COSTC 2

41 of the 91 rows carry no validation at all and 50 carry at least one. rule_counts()
reproduces exactly those numbers from the workbook, and the offline suite holds it to
them -- so a DCDD that arrives with a rule nobody here implements is a red check rather
than a silent gap.

ONE CELL CAN HOLD SEVERAL RULES, NEWLINE-SEPARATED, and five rows do -- e.g.
Company_Reference_ID is CHECKREFERENCES, MATCHVALUE_COMPNY and MISSINGVALUE in one cell.
Read as a single rule name, that cell matches nothing and validates NOTHING on the very
fields that carry the most rules. wd_mapping.dcdd_populated_fields already splits,
strips and dedupes the cell into entry["validations"], so this module CONSUMES that list
and never re-parses column L. field_rules() is the only place that reads it, and it
refuses a `validations` that is still a string rather than treating it as one rule.

=======================================================================================
THE THING THIS MODULE EXISTS TO MAKE IMPOSSIBLE
=======================================================================================
A CHECKREFERENCES PASS OVER REFERENCE DATA THAT IS NOT THERE. Those 27 fields need
Workday reference values this lake does not hold: hub_wd_reference exists but its WORKDAY
binding is inactive, because 01_usnc_bronze_dev has no workday schema yet (subsystem E).
Validating a value against an empty set finds zero violations -- and "zero violations"
rendered as a pass reads, on a customer's invoice, as "27 fields were checked" when
nothing was compared against anything.

So three separate things are arranged to stop it:

  * NO PASS WITHOUT VALUES. allowed_values() returns the set of permitted values or
    None-with-a-reason, never an empty set, and a membership rule can only reach PASS
    through a set it returned. A rule whose values are absent, empty or the wrong shape
    is NOT_EVALUATED, and each of those four says a DIFFERENT sentence -- because "there
    was nothing to compare against" and "everything compared fine" must never render the
    same way.
  * NO PASS WITHOUT COMPARISONS. `checks` counts the values actually examined, and
    status is PASS only when checks > 0. An empty row set, or a column blank on every
    row, is NOT_EVALUATED. An empty result set is not agreement.
  * CHECKREFERENCES CANNOT REACH PASS AT ALL TODAY, structurally, for a second and
    independent reason: each of those 27 fields must match its OWN reference_id_type,
    which the DCDD names in column E (`Type Value`) and wd_mapping does not expose. A
    membership test without it would compare a Tax_Code_ID against the Currency domain
    and agree. REFERENCE_TYPES maps it to None, allowed_values() refuses None first, and
    that is the one line to change when subsystem E lands the data AND the per-field
    type.

EVERY VALIDATOR RETURNS EVERY VIOLATION, never the first. An operator fixing a rejected
file one round trip at a time needs to know how many there are, and each problem string
names the field, the rule, the row and the offending value.
"""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation

#: The three outcomes a rule can have. NOT_EVALUATED is not a milder PASS -- it is the
#: statement that this run did not look, and the export refuses on it.
PASS = "PASS"
FAIL = "FAIL"
NOT_EVALUATED = "NOT_EVALUATED"

#: Coverage says whether a rule CAN be evaluated at all, which is a different question
#: from whether it passed -- so it gets its own word. Printing PASS here would say "this
#: rule was satisfied" for a rule that has merely not been prevented from running.
EVALUATED = "EVALUATED"

ISNUMERIC = "ISNUMERIC"
DATEFORMATCHECK = "DATEFORMATCHECK"
MISSINGVALUE = "MISSINGVALUE"
CHECKBOOLEAN = "CHECKBOOLEAN"
MATCHVALUE_COMPNY = "MATCHVALUE_COMPNY"
MATCHVALUE_COSTC = "MATCHVALUE_COSTC"
CHECKREFERENCES = "CHECKREFERENCES"

#: Every DATEFORMATCHECK field in the workbook carries `Type Value` = YYYY-MM-DD, and
#: the sample values are 2021-01-01. This is read from the workbook, not assumed.
DATE_FORMAT = "YYYY-MM-DD"
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: Compared upper-cased. Workday's DT/DTS loader takes these six for a Boolean column.
BOOLEAN_VALUES = ("0", "1", "FALSE", "N", "TRUE", "Y")

#: Decimal() parses all of these happily, and none of them is an amount anyone can pay.
_NON_FINITE = ("NAN", "-NAN", "+NAN", "INF", "-INF", "+INF", "INFINITY", "-INFINITY",
               "+INFINITY", "SNAN", "-SNAN", "+SNAN")

#: Rule -> the Workday reference_id_type its values must belong to.
#:
#: CHECKREFERENCES IS None ON PURPOSE, and it is the single most important line in this
#: file. Those 27 fields each match a DIFFERENT type -- the DCDD names it per row in
#: `Type Value` -- so there is no one set to test them against, and pointing them at the
#: union of every type would let a Tax_Code_ID satisfy a Currency_Reference_ID column.
#: allowed_values() refuses None before it looks at the data at all, so no arrangement of
#: reference values can make CHECKREFERENCES report PASS from here.
REFERENCE_TYPES = {
    CHECKREFERENCES: None,
    MATCHVALUE_COMPNY: "Company_Reference_ID",
    MATCHVALUE_COSTC: "Cost_Center_Reference_ID",
}


def _text(value) -> str:
    """One rendering of a cell value, the same one csv_text writes."""
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def isnumeric_reason(value) -> str | None:
    """Why a value fails ISNUMERIC, or None.

    A BLANK PASSES. ISNUMERIC constrains the shape of a value that is there; whether it
    has to be there at all is MISSINGVALUE's question, and four fields carry both. A
    validator that rejected blanks here would block every optional amount in the file.
    """
    text = _text(value).strip()
    if text == "":
        return None
    if text.upper() in _NON_FINITE:
        return "is not a finite number"
    try:
        Decimal(text)
    except (InvalidOperation, ValueError, ArithmeticError):
        return "is not a number"
    return None


def dateformat_reason(value) -> str | None:
    """Why a value fails DATEFORMATCHECK, or None. Blank passes, as for ISNUMERIC.

    THE SHAPE IS CHECKED AND THEN THE CALENDAR IS. `2021-02-30` matches the pattern and
    is not a day; Workday rejects the file rather than the row, so a whole export is lost
    to one impossible date that looked well formed.
    """
    text = _text(value).strip()
    if text == "":
        return None
    if not _DATE_PATTERN.match(text):
        return f"is not {DATE_FORMAT}"
    year, month, day = (int(part) for part in text.split("-"))
    try:
        date(year, month, day)
    except ValueError:
        return f"is not a real calendar date, though it is shaped like {DATE_FORMAT}"
    return None


def missingvalue_reason(value) -> str | None:
    """Why a value fails MISSINGVALUE, or None.

    WHITESPACE IS MISSING, NOT PRESENT. A column holding three spaces satisfies a NOT
    NULL constraint and satisfies `!= ""`, and reaches Workday as a required field with
    no value in it. .strip() here is Python's, which removes tabs and newlines too --
    Spark's TRIM removes spaces only, which is why this test lives on this side.
    """
    return None if _text(value).strip() else "is blank or whitespace-only"


def checkboolean_reason(value) -> str | None:
    """Why a value fails CHECKBOOLEAN, or None. Blank passes."""
    text = _text(value).strip()
    if text == "":
        return None
    if text.upper() in BOOLEAN_VALUES:
        return None
    return f"is not one of {list(BOOLEAN_VALUES)}"


#: Rule -> the validator that decides it here, in this process, with no external data.
LOCAL_VALIDATORS = {
    ISNUMERIC: isnumeric_reason,
    DATEFORMATCHECK: dateformat_reason,
    MISSINGVALUE: missingvalue_reason,
    CHECKBOOLEAN: checkboolean_reason,
}

KNOWN_RULES = tuple(sorted(set(LOCAL_VALIDATORS) | set(REFERENCE_TYPES)))


def membership_reason(value, allowed) -> str | None:
    """Why a value is not in `allowed`, or None.

    `allowed` MUST ALREADY BE A NON-EMPTY SET -- allowed_values() is the only thing that
    produces one. This function deliberately does NOT defend against an empty set by
    returning None, because that is the exact failure the module exists to stop: every
    value would be "not violating" and the caller would render agreement.
    """
    text = _text(value).strip()
    if text == "":
        return None
    return None if text in allowed else "is not one of the known reference values"


def allowed_values(reference_values, reference_type) -> tuple[set | None, str]:
    """(the NON-EMPTY set of permitted values for one reference type, or None and why).

    THE ONLY DOOR TO A MEMBERSHIP PASS, and it never opens on nothing. Exactly one of the
    two elements is truthy: a set with at least one value, or a sentence saying what is
    missing. There is no third shape, and in particular there is no empty set -- an empty
    set compares "no violations" against every value in the file.

    SIX DISTINCT REASONS, SIX DISTINCT SENTENCES -- one per branch below, and the count
    is stated so a seventh branch added without a sentence of its own is visibly wrong.
    "the type is not known", "no reference data at all", "the map is the wrong type", "no
    entry for this type", "this type's values are the wrong shape" and "0 rows" are six
    different states of the world, and collapsing any two to one sentinel is how two
    unrelated failures come to compare equal and satisfy a check between them. The
    offline suite asserts the six are distinct rather than trusting this paragraph.
    """
    if reference_type is None:
        return None, ("the reference_id_type each field's values must belong to is not "
                      "known here -- the DCDD names it per row in `Type Value`, which "
                      "wd_mapping does not expose -- so any membership test would "
                      "compare against the wrong domain and agree")
    if reference_values is None:
        return None, (f"no Workday reference data was supplied to this run at all, so "
                      f"there is nothing to test {reference_type} values against")
    if not isinstance(reference_values, dict):
        return None, (f"the reference data is a {type(reference_values).__name__}, not a "
                      f"mapping of reference_id_type -> values")
    if reference_type not in reference_values:
        return None, (f"the reference data holds NO entry for reference_id_type "
                      f"{reference_type} -- absent is not the same as empty, and neither "
                      f"is agreement")
    raw = reference_values[reference_type]
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (set, frozenset, list, tuple,
                                                            dict)):
        return None, (f"the reference data for {reference_type} is a "
                      f"{type(raw).__name__}, not a collection of values")
    usable = {_text(v).strip() for v in raw if _text(v).strip()}
    if not usable:
        return None, (f"hub_wd_reference holds 0 rows for reference_id_type "
                      f"{reference_type}. Validating against an empty reference table "
                      f"passes for the wrong reason")
    return usable, ""


def field_rules(entry) -> tuple[str, ...]:
    """The rules one DCDD entry carries, read from wd_mapping's ALREADY-SPLIT list.

    A `validations` THAT IS STILL A STRING RETURNS NOTHING, and the caller reports it.
    Treating the raw cell as one rule name is the failure mode the module docstring
    opens with: `CHECKREFERENCES\\nMATCHVALUE_COMPNY` matches no rule, so the two fields
    carrying the most rules in the workbook would be the two validated by nothing.
    """
    if not isinstance(entry, dict):
        return ()
    values = entry.get("validations")
    if not isinstance(values, (list, tuple)):
        return ()
    return tuple(v for v in values if isinstance(v, str) and v.strip())


def rule_counts(dcdd) -> dict[str, int]:
    """Rule -> how many populated WORKBOOK ROWS carry it.

    PER ROW, NOT PER FIELD, and the difference is not cosmetic: Worktags_Reference_ID
    occupies two rows and carries CHECKREFERENCES on both. Counted per field the totals
    come to 25/12/6/4/4/3/2 and reconcile with nothing anybody measured; counted per row
    they are the 27/12/6/4/4/3/2 the workbook actually holds, which is what the suite
    checks the seven rules against.
    """
    counts: dict[str, int] = {}
    for entry in (dcdd or {}).values():
        if not isinstance(entry, dict):
            continue
        for row in entry.get("per_row") or ():
            rules = row[4] if isinstance(row, (list, tuple)) and len(row) > 4 else ()
            for rule in rules if isinstance(rules, (list, tuple)) else ():
                counts[rule] = counts.get(rule, 0) + 1
    return counts


def unvalidated_rows(dcdd) -> int:
    """Populated workbook rows carrying NO validation at all -- 41 of the 91.

    Counted so the arithmetic closes: 41 unvalidated plus 50 validated is the whole
    populated set, and a split that stops adding up means the VALIDATIONS column is
    being read wrong somewhere upstream.
    """
    total = 0
    for entry in (dcdd or {}).values():
        if not isinstance(entry, dict):
            continue
        for row in entry.get("per_row") or ():
            rules = row[4] if isinstance(row, (list, tuple)) and len(row) > 4 else ()
            if not rules:
                total += 1
    return total


def fields_for_rule(dcdd, rule) -> list[str]:
    """Every populated DCDD field name carrying `rule`, sorted."""
    return sorted(name for name, entry in (dcdd or {}).items()
                  if rule in field_rules(entry))


def _evaluate(rows, fields, reference_values):
    """(one record per rule, shape problems). The single traversal everything reads.

    ONE TRAVERSAL, NOT TWO. problems() and rule_outcomes() are both derived from this
    call rather than each walking the rows themselves: two walks of the same data drift,
    and the drift that matters here is a violation counted by one and not the other, so
    the gate refuses on a number the report does not show.
    """
    records = {rule: {"rule": rule, "status": NOT_EVALUATED, "fields": [], "checks": 0,
                      "references_used": 0, "problems": [], "reason": ""}
               for rule in KNOWN_RULES}
    shape: list[str] = []

    if not isinstance(fields, dict):
        shape.append(f"the field set is a {type(fields).__name__}, not a mapping of DCDD "
                     f"field name -> entry, so NOTHING was validated")
        fields = {}
    if not isinstance(rows, (list, tuple)):
        shape.append(f"the row set is a {type(rows).__name__}, not a sequence of rows, "
                     f"so NOTHING was validated")
        rows = []

    # THE PERMITTED VALUES ARE RESOLVED ONCE, BEFORE ANY ROW IS LOOKED AT, so a rule with
    # no data behind it is already NOT_EVALUATED when the rows are walked and cannot be
    # talked into PASS by finding no violations among them.
    allowed: dict[str, set | None] = {}
    for rule, reference_type in REFERENCE_TYPES.items():
        values, reason = allowed_values(reference_values, reference_type)
        allowed[rule] = values
        records[rule]["reason"] = reason
        records[rule]["references_used"] = len(values) if isinstance(values, set) else 0

    for name in sorted(fields):
        entry = fields[name]
        if isinstance(entry, dict) and isinstance(entry.get("validations"), str):
            shape.append(
                f"field {name}: `validations` is still the raw workbook cell "
                f"({entry['validations']!r}), not the split list wd_mapping produces. A "
                f"cell holding several newline-separated rules matches no rule name at "
                f"all, so this field would be validated by nothing")
            continue
        for rule in field_rules(entry):
            if rule not in records:
                records[rule] = {"rule": rule, "status": FAIL, "fields": [], "checks": 0,
                                 "references_used": 0, "problems": [],
                                 "reason": (f"no validator in this module implements "
                                            f"{rule}, so nothing checked it")}
            if name not in records[rule]["fields"]:
                records[rule]["fields"].append(name)

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            shape.append(f"row {index} is a {type(row).__name__}, not a row, so no field "
                         f"on it was validated")
            continue
        for rule in sorted(records):
            record = records[rule]
            for name in record["fields"]:
                if name not in row:
                    continue
                value = row[name]
                if rule in LOCAL_VALIDATORS:
                    record["checks"] += 1
                    reason = LOCAL_VALIDATORS[rule](value)
                elif rule in REFERENCE_TYPES:
                    permitted = allowed[rule]
                    if not isinstance(permitted, set):
                        continue
                    if _text(value).strip() == "":
                        continue
                    record["checks"] += 1
                    reason = membership_reason(value, permitted)
                else:
                    record["checks"] += 1
                    reason = "was not checked: this module implements no such rule"
                if reason:
                    record["problems"].append(
                        f"row {index} field {name} rule {rule}: {_text(value)!r} "
                        f"{reason}")

    for record in records.values():
        record["status"] = status_of(record)
    return [records[rule] for rule in sorted(records)], shape


def status_of(record) -> str:
    """One rule's outcome, derived from what was actually compared.

    THE ONLY PLACE A PASS IS EVER MINTED, which is why it is public and why the offline
    suite drives it over its whole truth table rather than over the one case that was in
    mind when it was written.

    PASS REQUIRES A COMPARISON TO HAVE HAPPENED. `checks` is the number of values this
    run put in front of a validator, so an empty row set or a field absent from every row
    leaves it at zero -- and zero comparisons is NOT_EVALUATED, never PASS.

    AND A MEMBERSHIP RULE REQUIRES PERMITTED VALUES, SEPARATELY AND FIRST. Today the two
    guards overlap: with no permitted values _evaluate() compares nothing, so `checks` is
    zero anyway and either guard alone would return NOT_EVALUATED. That overlap is
    exactly how a guard rots -- it can be deleted with every check still green, and the
    day a refactor counts a skipped value as a check the empty reference table becomes a
    PASS. So this one is asserted on its own terms: a record claiming five comparisons
    and no permitted values is NOT_EVALUATED, and the suite drives that record straight
    through this function rather than hoping to reach it through _evaluate().
    """
    if not isinstance(record, dict):
        return NOT_EVALUATED
    rule = record.get("rule")
    if record.get("problems"):
        return FAIL
    if rule not in LOCAL_VALIDATORS and rule not in REFERENCE_TYPES:
        return FAIL
    if rule in REFERENCE_TYPES and _count(record, "references_used") <= 0:
        return NOT_EVALUATED
    if _count(record, "checks") <= 0:
        return NOT_EVALUATED
    return PASS


def _count(record, key) -> int:
    """One record's count, or 0 for anything that is not an int.

    A MISSING OR MALFORMED COUNT IS TREATED AS ZERO, never as "assume it was fine".
    `record.get(key, 1)` would let a refactor that stopped writing the key mint a PASS,
    and `isinstance(x, bool)` is excluded because True would otherwise pass for a count
    of one -- the same guard money_refusal() already carries for the same reason.
    """
    value = record.get(key)
    return 0 if isinstance(value, bool) or not isinstance(value, int) else value


def problems(rows, fields, reference_values=None) -> list[str]:
    """EVERY violation these rows commit against these fields' rules, as sentences.

    NOT THE FIRST ONE. An operator whose file Workday rejected fixes the violations one
    at a time, and a validator that stops at the first turns one rejected load into as
    many round trips as there are bad values.

    THIS LIST BEING EMPTY IS NOT A PASS, and callers must not read it as one -- a rule
    with no reference data behind it produces no violations because it compared nothing.
    rule_outcomes() is where a run finds out whether a rule was evaluated, and
    gate_findings() is what the export refuses on.
    """
    records, shape = _evaluate(rows, fields, reference_values)
    return sorted(shape + [p for record in records for p in record["problems"]])


def rule_outcomes(rows, fields, reference_values=None) -> list[dict]:
    """One record per rule: its status, the fields carrying it, and what it compared.

    Keys: rule, status, fields, checks, references_used, problems, reason. `status` is
    PASS, FAIL or NOT_EVALUATED, and a PASS always carries checks > 0 -- plus
    references_used > 0 for a membership rule -- which is the property the suite asserts
    rather than trusting this sentence.
    """
    return _evaluate(rows, fields, reference_values)[0]


def coverage(dcdd, reference_values=None) -> list[dict]:
    """Per rule, over the WHOLE populated DCDD: how many rows carry it and whether this
    run can evaluate it at all.

    SEPARATE FROM rule_outcomes() BECAUSE THE QUESTION IS DIFFERENT, and separate from
    the mapped columns because the dangerous case is exactly the one the mapping does not
    reach. Today three fields are mapped and none of them carries CHECKREFERENCES, so a
    report built only from what gets written would say nothing whatever about the 27
    fields that cannot be checked -- and silence is how "27 fields validated" gets
    believed. This reads the workbook, so the 27 are named on every run.
    """
    counts = rule_counts(dcdd)
    out: list[dict] = []
    for rule in sorted(counts):
        record = {"rule": rule, "rows": counts[rule], "references_used": 0,
                  "fields": fields_for_rule(dcdd, rule)}
        if rule in LOCAL_VALIDATORS:
            record["evaluable"] = True
            record["reason"] = "evaluated in this process, against the rows being written"
        elif rule in REFERENCE_TYPES:
            values, reason = allowed_values(reference_values, REFERENCE_TYPES[rule])
            record["evaluable"] = isinstance(values, set) and len(values) > 0
            record["references_used"] = len(values) if isinstance(values, set) else 0
            record["reason"] = reason or (f"{record['references_used']} permitted value(s) "
                                          f"for {REFERENCE_TYPES[rule]}")
        else:
            record["evaluable"] = False
            record["reason"] = (f"no validator in this module implements {rule}, so a run "
                                f"that reported it applied would be reporting a fiction")
        out.append(record)
    return out


def coverage_lines(dcdd, reference_values=None) -> list[str]:
    """The per-rule narration, one line each, printed by every run of the export.

    LOUD AND PER RULE, whether or not the run gets far enough to write anything. The 27
    CHECKREFERENCES rows are named as NOT_EVALUATED in the log of a run that refuses for
    an entirely unrelated reason, because the day somebody reads that log looking for
    what was checked is the day the distinction has to already be there.
    """
    return [f"{record['rule']} :: "
            f"{EVALUATED if record['evaluable'] else NOT_EVALUATED} :: "
            f"{record['rows']} populated workbook row(s), "
            f"{len(record['fields'])} field(s) :: {record['reason']}"
            for record in coverage(dcdd, reference_values)]


def coverage_refusal(dcdd, reference_values=None) -> str | None:
    """Why this run cannot claim the DCDD's validations were applied, or None if it can.

    THE REFUSAL TAKES NO FLAG, and that is deliberate: a flag is how a temporary
    suppression becomes permanent, and the suppression on offer here is "ship a customer
    invoice claiming 27 reference fields were checked".
    """
    records = coverage(dcdd, reference_values)
    gaps = [r for r in records if not r["evaluable"]]
    if not gaps:
        return None
    total = sum(r["rows"] for r in records)
    blocked = sum(r["rows"] for r in gaps)
    lines = [f"  {r['rule']} :: {NOT_EVALUATED} :: {r['rows']} workbook row(s) over "
             f"{len(r['fields'])} field(s) :: {r['reason']}" for r in gaps]
    # COUNTED IN RULE INSTANCES, NOT ROWS, and the two differ: five rows carry several
    # rules each, so the seven rules sum to 58 over 50 validated rows. Calling 58 a row
    # count would be a number that reconciles with nothing anybody measured.
    return (f"{blocked} of {total} rule instance(s) the workbook names are carried by a "
            f"rule this run did NOT evaluate:\n" + "\n".join(lines))


def gate_findings(rows, fields, reference_values=None) -> list[str]:
    """Everything that stops these rows being a validated set: every violation, AND every
    rule that was not evaluated.

    THE TWO ARE IN ONE LIST ON PURPOSE. A caller handed only the violations sees an empty
    list for a run that checked nothing, and an empty list is what a pass looks like.
    """
    records, shape = _evaluate(rows, fields, reference_values)
    out = list(shape)
    for record in records:
        out.extend(record["problems"])
        if record["status"] == NOT_EVALUATED and record["fields"]:
            out.append(f"rule {record['rule']} over field(s) "
                       f"{record['fields']} was {NOT_EVALUATED}: "
                       f"{record['reason'] or 'no value was compared against anything'}")
    return out
