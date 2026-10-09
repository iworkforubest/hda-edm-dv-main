"""AME002 and AME006: issue an invoice date and line numbers, exactly once.

WHY THIS IS NOT A TRANSFORMATION. Both values appear on a document sent to a
customer. Deriving them in a projection means re-deriving them on every run, and
anything re-derived can change: a re-export could renumber an invoice whose lines the
customer has already received. Nothing would fail -- the load succeeds, the gates stay
green -- and the first symptom would be a dispute about which copy is correct.

So this task issues once and records the result in control.ctl_invoice_issuance, which
is append-only and guarded by append_only_check. Gold (Task 8) reads the ledger instead
of recomputing either value.

THE PURE FUNCTION IS THE POINT. assign_line_numbers() takes the lines on an invoice,
what has already been issued for it, and the date, and returns only the rows to
append. It touches no Spark, so the guarantee this module exists to provide -- that
issuance never renumbers an already-issued line -- is tested in the offline suite,
not left to a workspace run.

THE WRITER turns that into the ledger's actual seven columns: it supplies
invoice_tenant, invoice_reference, issued_at (our clock, not the source's) and
issued_by_run_id around the three fields the pure function returns, and APPENDS the
result. It issues no statement that can rewrite or remove a ledger row -- the only
verb it uses against ctl_invoice_issuance is INSERT, and the only reason it ever reads
the ledger is to find out what must NOT be issued again.

WHERE THE THREE GRAIN COLUMNS COME FROM, AND WHY THREE TABLES ARE JOINED FOR THEM.
csat_invoice_line_gie carries invoice_line_hk, a hashdiff and eleven payload columns
and NOT ONE of the ledger's grain columns -- a BINARY(32) hash key is not an invoice
number. So the candidate query walks the model's own declarations:

    csat_invoice_line_gie          the set of lines the business vault has computed
    nhl_invoice_line               invoice_line_hk -> invoice_hk, line_reference
    hub_invoice                    invoice_hk -> invoice_tenant, invoice_reference

Every table name and every column name in that query is READ OUT OF metadata/entities
rather than typed here, because a hand-typed vault column that no longer exists is an
UNRESOLVED_COLUMN in the workspace and nothing at all offline -- the pipeline-only
shape this repo has shipped before. issuance_sources() raises, by name, if the model
stops declaring any of them, and the offline suite calls it.

THE THIRD GRAIN COLUMN IS line_reference, NOT invoice_line_item_ref, and that was a
measurement rather than a preference. The design spec called invoice_line_item_ref
"unique per line and the sort key"; metadata/entities/nhl_invoice_line.yml measured it
at 422 distinct values across Ameren's 1,207 rows -- it is a timesheet or expense-sheet
reference, and rows beneath one of them differ only by task code, or on expense sheets
not even by that. A grain column that does not identify a line cannot hold "one row per
line, ever". line_reference IS nhl_invoice_line's transaction key, 1,169 distinct
across the same 1,207 rows, so it is unique per line BY CONSTRUCTION. It is read from
the NHL rather than from sat_invoice_line_details, which does not carry it, and through
the STABLE view name rather than _rev1 or _rev2.

ONE OPEN QUESTION STILL STOPS THIS FROM ISSUING ANYTHING, and the run says so rather
than issuing something wrong: AME002's DATE. The tax-point date the rule derives from
is defined nowhere in the source workbook and is absent from the feed. --invoice-date
is therefore required to issue, is never defaulted, and nothing here reads a clock for
it. current_date, today's date, the week-ending date and NULL are all wrong answers
that would reach a customer invoice silently.

IT IS REPORTED AS NOT_EVALUATED, NOT AS A PASS. A gate that printed PASSED here would
read, in a job run log, exactly like a task that had issued something.

AND THE LEDGER'S REAL SHAPE IS CHECKED BEFORE ANYTHING IS WRITTEN. control_objects.sql
creates this table with CREATE TABLE IF NOT EXISTS, which is a no-op on a table that
already exists -- so a workspace holding an EARLIER version of ctl_invoice_issuance
keeps it, column for column, and the INSERT below would fail on a mismatch at issuance
time with a Spark error rather than a sentence. ledger_shape_refusal() compares what
the table actually has against what control_standard declares and refuses by name. It
alters nothing: a control table that disagrees with the standard is a deployment
question, not something a writer should quietly reconcile.
"""
from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define __file__,
# so Path(__file__) below would raise NameError before anything ran. compile() still
# records the real path in the code object.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import control_standard, naming, spec  # noqa: E402

GATE = "invoice_issue"

LEDGER = "ctl_invoice_issuance"

#: The four ledger columns the pure function does NOT return, and which the writer has
#: to supply around it. Named here so that ledger_columns() can prove the two sets
#: together cover the table the control standard declares -- a NOT NULL column added
#: there and populated by nobody is an INSERT that fails in the workspace only.
WRITER_SUPPLIED = ("invoice_tenant", "invoice_reference", "issued_at",
                   "issued_by_run_id")

#: The three keys assign_line_numbers() returns per row. The offline suite asserts this
#: against the function's actual output, so the two cannot drift.
ISSUED_BY_RULE = ("line_reference", "line_number", "invoice_date")


def assign_line_numbers(line_refs: list[str],
                         already_issued: dict[str, tuple[int, str]],
                         invoice_date: str = "") -> list[dict]:
    """Rows to append to the issuance ledger for one invoice.

    `line_refs` are every line reference on the invoice as it stands now.
    `already_issued` maps a line reference to BOTH values it was issued with --
    `(line_number, invoice_date)` -- and is empty for an invoice's first issuance.
    Returns only the rows that are NOT yet issued; an already-issued invoice re-run
    with no new lines returns [].

    THE DATE IS CARRIED BACK IN, NOT JUST THE NUMBER, and that is the fix for a real
    hole: while this mapped a reference to a bare int, a line added after issuance was
    stamped with whatever date THAT RUN passed, so one invoice ended up with two issued
    dates in an append-only ledger and neither could be removed. AME002 is issued once
    per INVOICE, not once per line, so the frozen date has to be an input to every
    later call -- a signature that cannot carry it cannot honour it.

    DUPLICATE LINE REFERENCES ARE REFUSED, not numbered twice. The ledger's grain is
    (invoice_tenant, invoice_reference, line_reference) -- one row per line,
    ever -- and Delta has no primary key to enforce it, while append_only_check
    deliberately returns no uniqueness findings for a ctl_ table. So nothing
    downstream would catch a re-delivered extract or a union view spanning two tenant
    tables that both carry one line: it would receive two frozen numbers forever, gold
    would emit it twice, the invoice would stop summing to gross and AME008 would
    refuse it permanently. This function is the write path, so this is where it is
    enforced.

    ORDERING IS BY line_reference, NOT BY INPUT ORDER. AME006 says "sequential within
    each invoice" and does not say sequential in what, so an unspecified order would
    make even the FIRST issuance arbitrary -- two runs over the same input rows in a
    different order could number the same lines differently, with nothing to detect it.

    AND THE COLUMN IT ORDERS BY CHANGED, BECAUSE THE OLD PREMISE WAS MEASURED FALSE.
    This used to order by invoice_line_item_ref and justify it as "unique per line and
    stable in the source". The first half is not true: metadata/entities/
    nhl_invoice_line.yml measured 422 distinct invoice_line_item_ref values across
    Ameren's 1,207 rows, because it is a timesheet or expense-sheet reference and one
    of them covers many lines. Ordering by a column that repeats leaves the order
    among its duplicates unspecified -- the exact defect the paragraph above says this
    must not have -- and using it as the grain would issue one line two frozen numbers.

    line_reference IS nhl_invoice_line's TRANSACTION KEY, and that is a stronger
    property than "stable in the source": the transaction key is what identifies a line
    in a feed that emits no line identifier, so it is unique per line BY CONSTRUCTION
    (1,169 distinct across the same 1,207 rows, exactly what the ten identity columns
    produce), and a value that differs is a DIFFERENT line rather than a restatement of
    the same one. That last clause is what makes an append-only ledger keyed on it
    sound: a restated line cannot silently acquire a second number, because a
    restatement that changes the key is not the same line and a restatement that does
    not change it is anti-joined out.

    THE SORT IS STILL A PLAIN LEXICOGRAPHIC ONE over those values, which is arbitrary
    to a human reading the invoice and deterministic to this function -- the property
    AME006 actually needs. It is not the source's presentation order, because the
    source emits none.

    A LINE ADDED AFTER ISSUANCE CONTINUES THE SEQUENCE, IT DOES NOT RENUMBER WHAT WAS
    SENT. next_number starts one past the highest number already issued, so a new
    line lands after every frozen one regardless of where its reference sorts among
    them. That is the whole reason the ledger exists rather than a plain re-derivation.

    RAISES rather than defaults when there is no date to use. The tax-point date AME002
    is meant to derive from is defined nowhere in the source workbook and is absent
    from the feed -- a genuine open question, not something to paper over with
    today's date or a null. Surfacing it as a failure here is the point; inventing a
    value would let a wrong date reach a customer invoice silently.

    `invoice_date` IS ONLY READ WHEN THE INVOICE HAS NO ISSUED DATE YET. Once one is
    frozen, the ledger's own value is what every later line is stamped with, so a
    writer re-running on a different day does not need to reproduce a date it cannot
    re-derive -- it may pass "" and let the frozen one stand. Passing a DIFFERENT
    non-empty date for an invoice that already has one RAISES rather than being
    quietly ignored: silently reusing the frozen value would hide a caller that
    believes it is issuing a date it is not.
    """
    duplicates = sorted({r for r in line_refs if line_refs.count(r) > 1})
    if duplicates:
        raise ValueError(
            f"duplicate line_reference(s) on one invoice: {duplicates}; the "
            f"ledger's grain is one row per line, ever, and nothing downstream would "
            f"catch a line issued two line numbers")
    issued_dates = {date for _, date in already_issued.values()}
    if len(issued_dates) > 1:
        raise ValueError(
            f"this invoice already carries more than one issued date: "
            f"{sorted(issued_dates)}; AME002 issues one date per invoice, so this "
            f"ledger has already diverged and no further line may be issued against it")
    frozen_date = next(iter(issued_dates), "")
    if frozen_date and invoice_date and invoice_date != frozen_date:
        raise ValueError(
            f"invoice_date {invoice_date!r} was supplied for an invoice already issued "
            f"with {frozen_date!r}; the issued date is frozen and a line added later "
            f"reuses it -- pass it, or pass nothing, but do not pass a second date")
    issue_date = frozen_date or invoice_date
    if not issue_date:
        raise ValueError("invoice_date is required; AME002 does not default it")
    next_number = max((n for n, _ in already_issued.values()), default=0) + 1
    rows = []
    for ref in sorted(line_refs):
        if ref in already_issued:
            continue
        rows.append({
            "line_reference": ref,
            "line_number": next_number,
            "invoice_date": issue_date,
        })
        next_number += 1
    return rows


def ledger_columns() -> tuple[str, ...]:
    """The ledger's columns, in declared order, proven to be exactly what is written.

    Read from control_standard rather than typed here, and then CHECKED against the two
    sets above: every declared column must be supplied either by the writer or by
    assign_line_numbers(), and neither may supply a column the ledger does not have.

    A column added to ctl_invoice_issuance that nothing populates is NOT NULL in the
    DDL, so it fails the INSERT -- in the workspace, at issuance time, on a job whose
    whole purpose is to write that row. Failing here instead makes it an offline
    failure with a name in it.
    """
    declared = tuple(control_standard.LAYER_TABLES["silver"][LEDGER])
    written = set(WRITER_SUPPLIED) | set(ISSUED_BY_RULE)
    unpopulated = sorted(set(declared) - written)
    unknown = sorted(written - set(declared))
    if unpopulated or unknown:
        raise ValueError(
            f"{LEDGER} declares {sorted(declared)}, the writer supplies "
            f"{sorted(WRITER_SUPPLIED)} and assign_line_numbers returns "
            f"{sorted(ISSUED_BY_RULE)}: {unpopulated} would be written by nobody and "
            f"{unknown} is written into a column the ledger does not declare")
    return declared


def _entity(model, base_table: str):
    """The model's entity for `base_table`, or a refusal naming it.

    A KeyError-shaped miss here means the vault no longer declares a table this query
    reads, which offline is a rename and in the workspace is a query against a table
    that is not there.
    """
    for entity in model.entities:
        if entity.base_table == base_table:
            return entity
    raise ValueError(
        f"metadata/entities declares no entity with base table {base_table!r}, which "
        f"the issuance candidate query reads -- it was renamed, removed, or never "
        f"existed in this model")


def _single_table(entity) -> str:
    """The one physical table for an entity, refusing a multi-binding one.

    Every table this query reads is single-bound today. A second binding would make
    "the" table ambiguous, and picking the first silently would read one tenant's rows
    and issue line numbers as though they were every line on the invoice.
    """
    tables = [table for _src, table in entity.tables()]
    if len(tables) != 1:
        raise ValueError(
            f"{entity.base_table} resolves to {len(tables)} physical table(s) "
            f"({tables}); the issuance candidate query needs exactly one, and choosing "
            f"among them is a modelling decision, not this writer's")
    return tables[0]


def _declares_transaction_key(entity, column: str) -> str:
    """`column`, proven to be part of `entity`'s TRANSACTION KEY.

    NOT "a column of the table" AND NOT "a payload column", because the property the
    ledger's grain depends on is narrower than either. line_reference identifies a line
    only because it IS the transaction key -- the thing the model nominates as a line's
    identity in a feed that emits no line identifier. A column demoted out of the
    transaction key into the payload would still resolve in the workspace, still produce
    a well-formed ledger row, and no longer be unique per line.
    """
    key = tuple(entity.transaction_key)
    if column not in key:
        raise ValueError(
            f"{entity.base_table}'s transaction key is {list(key)} and does not include "
            f"{column!r}, which the ledger uses as its per-line grain. That column is "
            f"unique per line only BY BEING the transaction key -- outside it, it "
            f"identifies nothing and a line could be issued two frozen numbers")
    return column


def issuance_sources(model) -> dict:
    """Every table and column name the candidate query needs, read from the model.

    RESOLVED, NOT TYPED. The vault versions its physical tables
    (csat_invoice_line_gie_rev1) and publishes an unversioned view over them, so the
    view name is naming.stable() of whatever the model currently declares; a rev bump
    therefore moves this query with it instead of leaving it on the old table.

    The hub's two business keys ARE the ledger's first two grain columns, by name. That
    is asserted rather than assumed: if hub_invoice were ever rekeyed on something
    else, projecting its business keys into invoice_tenant/invoice_reference would
    still produce a well-formed row, in the ledger, frozen, wrong.

    THE THIRD IS HELD TO A STRONGER TEST THAN EXISTENCE. line_reference is required to
    be nhl_invoice_line's transaction key, not merely one of its columns -- see
    _declares_transaction_key. sat_invoice_line_details, which this query used to join
    for invoice_line_item_ref, is gone from it entirely: it does not carry
    line_reference, and one fewer historised satellite in the join is one fewer
    current-version decision to get wrong.
    """
    gie = _entity(model, "csat_invoice_line_gie")
    nhl = _entity(model, "nhl_invoice_line")
    hub = _entity(model, "hub_invoice")

    grain = ledger_columns()[:3]
    if tuple(hub.business_keys) != grain[:2]:
        raise ValueError(
            f"hub_invoice's business keys are {list(hub.business_keys)}, but the "
            f"ledger's grain names {list(grain[:2])}; the candidate query projects the "
            f"hub's keys into those columns, so two different identifiers under one "
            f"name would be issued and frozen without anything raising")

    line_hk = naming.hk(gie.parents[0])
    return {
        "gie_view": naming.stable(_single_table(gie)),
        "nhl_view": naming.stable(_single_table(nhl)),
        "hub_view": naming.stable(_single_table(hub)),
        "line_hk": line_hk,
        "invoice_hk": naming.hk(nhl.parents[0]),
        "hub_hk": hub.hk_column,
        "tenant": grain[0],
        "reference": grain[1],
        "line_ref": _declares_transaction_key(nhl, grain[2]),
    }


def _q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def candidate_sql(catalog: str, vault_schema: str, business_vault_schema: str,
                  control_schema: str, src: dict) -> str:
    """The lines the business vault holds that the ledger has NEVER issued.

    LEFT ANTI JOIN ON THE LEDGER'S WHOLE GRAIN, and that anti-join is the first of the
    two guards against re-issuing. A repair run that reuses {{job.run_id}} (DEF-56)
    finds every line it issued last time already in the ledger, so it produces no
    candidate rows and appends nothing -- it does not append an identical row a second
    time. assign_line_numbers() is the second guard, and refuses the same thing from
    the other side.

    NO CURRENT-VERSION RANKING IS NEEDED ANY MORE, AND THAT IS THE GRAIN CHANGE PAYING
    FOR ITSELF. This query used to join sat_invoice_line_details for
    invoice_line_item_ref and rank its rows to take the current version -- a historised
    satellite, so a line whose payload was restated had several. That left a real hole:
    a RESTATED grain value would be compared against a ledger row frozen under the old
    one and the line would be issued a second number, silently. line_reference comes
    off nhl_invoice_line, which is append-only and NON-historised (one row per line,
    which is what an NHL is), and it is that table's transaction key -- so a restatement
    that changes it is a DIFFERENT line and one that does not is anti-joined out. The
    hole closes by construction rather than by ranking correctly.

    THE GIE SATELLITE IS STILL HISTORISED, which is why it contributes SELECT DISTINCT
    on the hash key alone: this query needs the SET of lines the business vault has
    computed, not any of their payload, so every version of a line collapses to the one
    line it is.

    A RANKED SUBQUERY WAS WHAT REPLACED QUALIFY, AND THAT WAS MEASURED RATHER THAN
    PREFERRED. Databricks SQL accepts QUALIFY; OSS Spark 3.5's parser does not, and the
    first version of this query was written with it. Parsed against the local Spark this
    repo already runs in CI: `[PARSE_SYNTAX_ERROR] Syntax error at or near
    'ROW_NUMBER'`. A construct that only the workspace can parse is a defect nothing
    offline can see -- the pipeline-only shape. The ranking is gone with the satellite,
    and the check that forbids QUALIFY stays, because the next window this query grows
    would meet the same parser.
    """
    return (
        f"WITH gie AS (\n"
        f"  SELECT DISTINCT {src['line_hk']}\n"
        f"  FROM {_q(catalog, business_vault_schema, src['gie_view'])}\n"
        f")\n"
        f"SELECT\n"
        f"  h.{src['tenant']}   AS {src['tenant']},\n"
        f"  h.{src['reference']} AS {src['reference']},\n"
        f"  n.{src['line_ref']} AS {src['line_ref']}\n"
        f"FROM gie g\n"
        f"JOIN {_q(catalog, vault_schema, src['nhl_view'])} n\n"
        f"  ON n.{src['line_hk']} = g.{src['line_hk']}\n"
        f"JOIN {_q(catalog, vault_schema, src['hub_view'])} h\n"
        f"  ON h.{src['hub_hk']} = n.{src['invoice_hk']}\n"
        f"LEFT ANTI JOIN {_q(catalog, control_schema, LEDGER)} l\n"
        f"  ON l.{src['tenant']} = h.{src['tenant']}\n"
        f"  AND l.{src['reference']} = h.{src['reference']}\n"
        f"  AND l.{src['line_ref']} = n.{src['line_ref']}"
    )


def issued_sql(catalog: str, control_schema: str, src: dict) -> str:
    """What the ledger has already issued -- the `already_issued` the rule reads.

    invoice_date is CAST to STRING because assign_line_numbers() compares the frozen
    date to the one a caller supplied as text, and a date object compared against
    '2026-07-29' is never equal -- which would make the mismatch guard fire on every
    correct call and never on a wrong one.
    """
    return (
        f"SELECT {src['tenant']}, {src['reference']}, {src['line_ref']}, line_number, "
        f"CAST(invoice_date AS STRING) AS invoice_date\n"
        f"FROM {_q(catalog, control_schema, LEDGER)}"
    )


def _lit(value, field: str) -> str:
    """A SQL string literal, refusing anything that could close it.

    Refuses rather than escapes, the same stance src/accelerator/audit.py takes: these
    values are invoice references out of a customer's feed, and a quote in one is a
    data problem worth failing on rather than quietly repairing into a frozen ledger
    row nobody can take back.
    """
    text = "" if value is None else str(value)
    if "'" in text or "\\" in text or "\x00" in text:
        raise ValueError(
            f"issuance field {field}={text!r} contains a quote, backslash or NUL and "
            f"cannot be written as a SQL literal. Refusing to escape it.")
    if not text:
        raise ValueError(
            f"issuance field {field} is empty; every ledger column is NOT NULL and an "
            f"empty grain column would issue a line number against nothing")
    return f"'{text}'"


def insert_sql(catalog: str, control_schema: str, rows: list[dict],
               issued_by_run_id: str) -> str:
    """One INSERT appending `rows` to the ledger.

    INSERT IS THE ONLY VERB THIS MODULE USES AGAINST ctl_invoice_issuance. The table is
    delta.appendOnly and the values in it are on a document a customer already has;
    there is no statement here that could rewrite or remove one of its rows, and that
    is deliberate rather than incidental.

    issued_at IS current_timestamp() -- OUR clock, as the DDL's comment requires, and
    the one column whose value is not derived from the feed, the rule or argv.
    """
    if not rows:
        raise ValueError("insert_sql called with no rows; the caller decides whether "
                         "there is anything to issue, so an empty INSERT is a bug")
    columns = ledger_columns()
    run_id = _lit(issued_by_run_id, "issued_by_run_id")
    values = []
    for row in rows:
        number = row["line_number"]
        if not isinstance(number, int) or isinstance(number, bool) or number < 1:
            raise ValueError(
                f"line_number {number!r} is not a positive integer; AME006 numbers "
                f"lines from 1 and the ledger's column is INT NOT NULL")
        values.append(
            "(" + ", ".join((
                _lit(row["invoice_tenant"], "invoice_tenant"),
                _lit(row["invoice_reference"], "invoice_reference"),
                _lit(row["line_reference"], "line_reference"),
                str(number),
                "DATE" + _lit(row["invoice_date"], "invoice_date"),
                "current_timestamp()",
                run_id,
            )) + ")")
    return (
        f"INSERT INTO {_q(catalog, control_schema, LEDGER)}\n"
        f"  ({', '.join(columns)})\n"
        f"VALUES\n  " + ",\n  ".join(values)
    )


def issue_one(line_refs: list[str], already_issued: dict, invoice_date: str,
              tenant: str, reference: str, render, execute) -> tuple[int, str | None]:
    """Issue one invoice's unissued lines. Returns (rows appended, refusal or None).

    WHY THIS IS A FUNCTION AND NOT FOUR LINES INSIDE main()'s LOOP. It used to be, with
    only assign_line_numbers() inside the try and the INSERT after it -- and _lit REFUSES
    a quote by design, so ONE invoice reference containing an apostrophe raised straight
    out of main(): the invoices already appended stayed committed, no GATE SUMMARY line
    printed at all, and the run died by traceback instead of naming the invoice that was
    wrong. The offline suite could prove insert_sql refuses the quote and could prove
    nothing about the RUN surviving to report it, because the loop needed a Spark
    session. `render` and `execute` are taken as arguments for exactly that reason.

    NOTHING RAISES OUT OF HERE. Every refusal this can meet -- a duplicate line, a
    second issued date, a quote in a reference, a line number that is not an integer,
    the execute() itself failing -- becomes the second element of the tuple, carrying
    the invoice's name, so the caller can count it, report it and keep going.

    NOTHING IS EXECUTED WHEN THERE IS NOTHING TO APPEND either: an already-issued
    invoice returns (0, None) without calling execute, which is what makes a repair run
    genuinely append nothing rather than append an empty statement.
    """
    try:
        rows = assign_line_numbers(line_refs, already_issued, invoice_date)
        if not rows:
            return 0, None
        for row in rows:
            row["invoice_tenant"] = tenant
            row["invoice_reference"] = reference
        execute(render(rows))
    except Exception as exc:  # noqa: BLE001 - reported, not swallowed
        return 0, f"{tenant}/{reference}: {exc}"
    return len(rows), None


def ledger_shape_refusal(actual_columns: list[str]) -> str | None:
    """Why the deployed ledger cannot be written to, or None if it can.

    WHAT THIS EXISTS FOR. governance/control_objects.sql creates ctl_invoice_issuance
    with CREATE TABLE IF NOT EXISTS, which is a NO-OP on a table that already exists. A
    workspace standing up this table before the grain column was renamed therefore keeps
    the old column, the DDL re-run reports success having changed nothing, and the INSERT
    below fails at issuance time -- deep in a Spark error about a column, on a job whose
    only purpose is to write that row, after the candidate query has already run.

    IT REFUSES, IT DOES NOT RECONCILE. No ALTER, no rebuild, no writing the columns that
    happen to match and leaving the rest. A control table that disagrees with
    control_standard is a deployment question for whoever owns that workspace, and a
    writer that quietly adapted to whatever it found would make the standard advisory.

    ORDER IS NOT COMPARED, only membership: a table created from the same declaration by
    an older path can hold the same columns in a different order, and the INSERT names
    its columns explicitly, so order cannot make it wrong.

    THIS IS NOT A HYPOTHETICAL SAFETY NET. Measured in TDS: ctl_invoice_issuance is
    already there under the earlier grain column, DESCRIBE returns PERMISSION_DENIED
    where a table that does not exist returns TABLE_OR_VIEW_NOT_FOUND, and a drop was
    attempted and refused because the schema is owned by a service principal. So this
    function is what fires on the next run in that lake, and the message it returns is
    the whole of what an operator gets. Each branch therefore says what was found, why
    re-running the DDL does not fix it, that nothing was issued or altered, and what the
    next action is -- because Unity Catalog's information_schema filters by privilege,
    "no columns" and "no table" arrive here as exactly the same empty list, and the two
    need different people.
    """
    declared = set(ledger_columns())
    actual = {str(c) for c in actual_columns}
    if not actual:
        return (
            f"{LEDGER} reported NO COLUMNS. information_schema returns nothing both for "
            f"a table that does not exist and for one this identity cannot see, because "
            f"Unity Catalog filters it by privilege -- so this is one of three things, "
            f"and they need different people:\n"
            f"    (1) the table has never been created here. ACTION: run the "
            f"control-objects task (checks/apply_control_objects.py) for this catalog, "
            f"then re-run this one.\n"
            f"    (2) it exists and this identity lacks SELECT on it. Confirm with "
            f"DESCRIBE TABLE: PERMISSION_DENIED means it is there, "
            f"TABLE_OR_VIEW_NOT_FOUND means it is not. ACTION: the schema owner grants "
            f"SELECT and MODIFY to the identity this job runs as. Do NOT create a "
            f"second table under another name -- an issued value lives in exactly one "
            f"ledger.\n"
            f"    (3) --catalog or --control-schema point somewhere else. ACTION: check "
            f"them against the bundle's variables.\n"
            f"  Nothing was issued and nothing was altered. Issuing against a table "
            f"whose shape is unknown is how a frozen value lands in the wrong column, "
            f"in an append-only ledger, on a document a customer already has.")
    missing = sorted(declared - actual)
    extra = sorted(actual - declared)
    if missing or extra:
        return (
            f"the deployed {LEDGER} does not match control_standard. MISSING {missing}, "
            f"UNEXPECTED {extra}. It was created under an EARLIER declaration and still "
            f"holds it.\n"
            f"    WHY RE-RUNNING THE DDL DOES NOT FIX THIS: "
            f"governance/control_objects.sql is CREATE TABLE IF NOT EXISTS, which is a "
            f"no-op on a table that already exists. It will report success and change "
            f"nothing, every run, for as long as the table stands.\n"
            f"    ACTION, and it belongs to whoever owns this schema, not to this job: "
            f"decide whether the existing table holds rows that matter. If it is empty, "
            f"drop it and re-run the control-objects task. If it is not, the issued "
            f"values in it are on documents customers already have -- migrate them into "
            f"the declared shape deliberately, keeping every row, because this ledger is "
            f"append-only and nothing in it can be re-derived.\n"
            f"  Nothing was issued and nothing was altered by this run.")
    return None


def date_refusal(invoice_date: str) -> str | None:
    """Why AME002's date cannot be issued from this argv, or None if it can.

    THE ONLY WAY A DATE ENTERS THIS MODULE IS argv. Nothing here reads a clock for it,
    and that absence is the whole control: the tax-point date AME002 derives from is
    defined nowhere in the source workbook and absent from the feed, so today's date,
    current_date(), the week-ending date and NULL are four different wrong answers on a
    customer's invoice, each of which would look exactly like a correct run.

    A MALFORMED DATE IS REFUSED TOO, before Spark sees it. DATE'2026-13-40' is a cast
    error at issuance time; DATE'29/07/2026' is worse, because a day-first string that
    Spark can parse at all lands a different date than the one someone typed, in an
    append-only ledger, on a document already sent.
    """
    if not invoice_date:
        return ("--invoice-date was not supplied, and AME002 does not default it. The "
                "tax-point date this rule derives from is defined nowhere in the "
                "source workbook and is absent from the feed -- it is an OPEN "
                "QUESTION, not a gap to fill with today's date.")
    try:
        parsed = date.fromisoformat(invoice_date)
    except ValueError:
        return (f"--invoice-date {invoice_date!r} is not an ISO yyyy-mm-dd date. It is "
                f"frozen into an append-only ledger and printed on a customer's "
                f"invoice, so it is refused rather than handed to Spark to interpret.")
    if parsed.isoformat() != invoice_date:
        return (f"--invoice-date {invoice_date!r} is not in canonical yyyy-mm-dd form "
                f"({parsed.isoformat()}); two spellings of one date in a ledger keyed "
                f"on nothing else is a difference nobody can see later.")
    return None


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def main() -> int:
    """Issue AME002 and AME006 for every vault line the ledger has not issued.

    WHAT A RUN WITH NO --invoice-date DOES, AND WHY IT IS NOT A PASS. ONE question is
    open now -- AME002's tax-point date, which is defined nowhere in the source workbook
    and absent from the feed. (The ledger's per-line grain was the second, and has been
    ruled on: it is line_reference, nhl_invoice_line's transaction key.) The run names
    the remaining one and finishes NOT_EVALUATED. It does not print PASSED, and it does
    not issue a placeholder: a demonstration that printed only PASSED would read, in a
    job run log, exactly like a task that had issued something.

    EVERY WORKSPACE CALL BELOW IS GUARDED, and each failure returns FAILED with a
    GATE SUMMARY line rather than a traceback. A spark_python_task that dies by
    traceback reports a failed task and nothing else -- no status, no statement, no
    count -- and the first thing anyone then does is guess which of the four statements
    it was.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", required=True,
                    help="schema holding the issuance ledger. Required, not defaulted: "
                         "a default would let issued values silently go somewhere else")
    ap.add_argument("--business-vault-schema", required=True,
                    help="schema holding csat_invoice_line_gie")
    ap.add_argument("--vault-schema", default="raw_vault",
                    help="schema holding nhl_invoice_line and hub_invoice, which "
                         "between them carry the ledger's three grain columns "
                         "(the NHL's transaction key, and the hub's two business "
                         "keys). Defaulted to the bundle's own vault_schema default "
                         "-- reading the wrong schema fails on a missing table, it "
                         "does not issue a wrong value")
    ap.add_argument("--invoice-date", default="",
                    help="AME002, as ISO yyyy-mm-dd. NEVER DEFAULTED -- see "
                         "date_refusal(). Without it this task issues nothing and says "
                         "so")
    ap.add_argument("--job-run-id", default="",
                    help="{{job.run_id}}, recorded as issued_by_run_id. Required to "
                         "issue, because a ledger row that cannot be traced to a run "
                         "cannot be explained to the customer who received it")
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="render the statements without a workspace")
    args = ap.parse_args()

    meta = Path(args.metadata) if args.metadata else (
        Path(__file__).resolve().parents[1] / "metadata" / "entities")
    try:
        src = issuance_sources(spec.load_model(meta))
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the issuance candidate query cannot be built from the "
              f"model -- {exc}")
        return finish("FAILED", 0, 0, 1)

    candidates = candidate_sql(args.catalog, args.vault_schema,
                               args.business_vault_schema, args.control_schema, src)
    issued = issued_sql(args.catalog, args.control_schema, src)

    if args.dry_run:
        print("-- candidate lines: in the business vault, absent from the ledger")
        print(candidates)
        print("\n-- what the ledger has already issued")
        print(issued)

    refusal = date_refusal(args.invoice_date)
    if refusal:
        print("\nISSUANCE REFUSED -- nothing was issued and nothing was written.")
        print(f"  OPEN QUESTION AME002's date: {refusal}")
        print("  That is a decision about a customer's invoice. It is not this "
              "writer's to make, and")
        print("  the ledger's third grain column -- once the second open question "
              "here -- has been ruled")
        print("  on: it is line_reference, nhl_invoice_line's transaction key, unique "
              "per line by")
        print("  construction. The date is what remains.")
        return finish("NOT_EVALUATED", 0, 1, 0)

    if args.dry_run:
        print("\ndry run only -- nothing executed, nothing issued")
        return finish("NOT_EVALUATED", 0, 1, 0)

    if not args.job_run_id:
        print("GATE FAILED: --job-run-id is empty, and issued_by_run_id is NOT NULL. A "
              "ledger row nobody can trace to a run cannot be explained to the "
              "customer who received the invoice it is on.")
        return finish("FAILED", 0, 0, 1)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # THE DEPLOYED TABLE'S SHAPE IS CHECKED BEFORE ANYTHING IS READ OR WRITTEN, because
    # control_objects.sql is CREATE TABLE IF NOT EXISTS and therefore a no-op on a table
    # standing since an earlier declaration. Read from information_schema rather than
    # DESCRIBE, whose result set carries partition and metadata rows a column list would
    # have to learn to skip.
    try:
        actual = [r["column_name"] for r in spark.sql(
            f"SELECT column_name FROM `{args.catalog}`.information_schema.columns "
            f"WHERE table_schema = {_lit(args.control_schema, 'control_schema')} "
            f"AND table_name = {_lit(LEDGER, 'table_name')}").collect()]
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: the deployed shape of {LEDGER} could not be read -- {exc}")
        return finish("FAILED", 0, 0, 1)
    shape = ledger_shape_refusal(actual)
    if shape:
        print(f"GATE FAILED: {shape}")
        return finish("FAILED", 0, 0, 1)

    # THE LEDGER IS READ FIRST, AND IT IS READ WHOLE. already_issued must carry every
    # line the invoice has ever been issued, not only the ones this run saw: the rule
    # starts the next number one past the highest ALREADY issued, and a partial read
    # would restart the sequence on top of numbers the customer already has.
    #
    # BOTH READS ARE GUARDED, LIKE THE SHAPE READ ABOVE AND THE WRITE BELOW. They were
    # the last two workspace calls that could die by traceback: no GATE SUMMARY line, no
    # statement named, and a task log that says only that something failed. The ledger
    # read can fail on permissions, the candidate read on a table the vault has not
    # built yet, and those two need different people -- so the failure names WHICH.
    already: dict[tuple[str, str], dict[str, tuple[int, str]]] = {}
    try:
        for row in spark.sql(issued).collect():
            key = (row[src["tenant"]], row[src["reference"]])
            already.setdefault(key, {})[row[src["line_ref"]]] = (
                int(row["line_number"]), row["invoice_date"])
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: reading what the ledger has already issued failed -- "
              f"{exc}\nThe statement was:\n{issued}\nNothing was issued and nothing "
              f"was written: a partial read of the ledger would restart a line-number "
              f"sequence on top of numbers a customer already has.")
        return finish("FAILED", 0, 0, 1)

    pending: dict[tuple[str, str], list[str]] = {}
    try:
        for row in spark.sql(candidates).collect():
            key = (row[src["tenant"]], row[src["reference"]])
            pending.setdefault(key, []).append(row[src["line_ref"]])
    except Exception as exc:  # noqa: BLE001
        print(f"GATE FAILED: reading the candidate lines failed -- {exc}\nThe "
              f"statement was:\n{candidates}\nNothing was issued and nothing was "
              f"written.")
        return finish("FAILED", 0, 0, 1)

    issued_rows, refused = 0, []
    for key in sorted(pending):
        tenant, reference = key
        appended, refusal = issue_one(
            pending[key], already.get(key, {}), args.invoice_date, tenant, reference,
            render=lambda rows: insert_sql(args.catalog, args.control_schema, rows,
                                           args.job_run_id),
            execute=spark.sql)
        if refusal:
            refused.append(refusal)
            continue
        issued_rows += appended
        if appended:
            print(f"  issued {appended:5} line(s) for {tenant}/{reference}")

    lines_seen = sum(len(v) for v in pending.values())
    already_present = sum(len(v) for v in already.values())
    print(f"\n{lines_seen} candidate line(s) across {len(pending)} invoice(s), "
          f"{issued_rows} issued, {already_present} line(s) already in the ledger and "
          f"left exactly as they were")

    if refused:
        print(f"\nISSUANCE REFUSED for {len(refused)} invoice(s):")
        for message in refused:
            print(f"  * {message}")
        return finish("FAILED", issued_rows, 0, 1)

    if issued_rows == 0:
        # NOT A PASS. Either the business vault holds no line this ledger has not
        # already issued -- which is the correct steady state and worth seeing -- or
        # the candidate query returned nothing because the GIE satellite is empty, and
        # those two look identical in a log that says PASSED.
        print("\nGATE NOT EVALUATED: no line was issued this run -- every candidate "
              "was already in the ledger, or there were none.")
        return finish("NOT_EVALUATED", 0, lines_seen, 0)

    print(f"\nISSUANCE PASSED: {issued_rows} line(s) issued once, and never again")
    return finish("PASSED", issued_rows, 0, 0)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
