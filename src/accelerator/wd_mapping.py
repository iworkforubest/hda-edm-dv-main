"""The mapping from vault columns to DCDD fields, and the DCDD's own view of itself.

THE MAPPING LIVES HERE, NOT IN THE WORKBOOK. metadata/workday/dcdd/*.xlsx are committed
verbatim with sha256 digests in PROVENANCE.json. Editing one breaks the record of which
bytes a mapping came from, and a binary is not reviewable in a diff. Measured, not
assumed: columns I (Mapping Status), J (SOURCETABLE.FIELD) and K (TRANSFORMATION LOGIC)
are EMPTY on all 91 populated rows of Submit_Customer_Invoice_DCDD, so there is no
mapping in the workbook to read even if reading one were allowed.

BOTH DIRECTIONS ARE CHECKED, AND THEY ARE NOT SYMMETRIC -- WHICH THIS DOCSTRING USED TO
HIDE. It said "every DCDD field has a mapping", and that was never true of this repo and
was never enforced anywhere. What is enforced is:

  * No mapping entry may name a field the DCDD does not contain. TOTAL, over all entries.
  * Every field the DCDD marks MUST-POPULATE has a mapping entry. Five of the 82.

The other 77 populated fields carry no mapping entry and are written BLANK. That is what
the workbook permits -- it marks five fields Required and asks for the rest only if you
have them -- but "77 fields are deliberately blank" and "77 fields were forgotten" look
identical in a mapping file, so the blanking is ASSERTED rather than assumed. See
blanked_fields() below and the checks over it: a must-populate field that loses its entry
goes red, and a change to WHICH fields are blanked is a change somebody has to look at
instead of a column that quietly stops being sent.

Together a drifting mapping goes red instead of exporting a blank column.

PURE AND SPARK-FREE, so it runs in the offline suite. A mapping that can only be tested
by running a pipeline does not get tested.

TWO ROWS OF THE 91 DO NOT NAME AN ORDINARY FILE, and both are must-populate, so neither
can be waved through:

  row 9    Customer_Invoice_ID                 csv File Name = "All CSVs"
  row 308  Customer_Invoice_Line_Reference_ID  csv File Name =
           "Submit_Customer_Invoice_Lines, Submit_Customer_Invoice_Lines_Details"

Read literally, those are a fifth and a sixth output file -- and an export that believed
them would write two CSVs Workday has never heard of. They are not files. "All CSVs"
means the field is the join key carried by EVERY file, and the comma-separated cell is
ONE field appearing in TWO files. `resolve_csv_files` is where that reading lives, and
it deliberately does NOT drop values it does not recognise: an unknown file name passes
straight through so the suite's "exactly these four files" check goes red on it, rather
than being silently filtered into agreement.

WD FIELD NAME IS NOT UNIQUE. 91 populated rows carry 82 distinct WD Field Names --
Dispute_Reason_Reference_ID alone occupies five rows (csv headers _1.._5), and
Worktags_Reference_ID, Memo and friends appear once in a header file and once in a line
file. Keying by field name therefore MERGES rows; it never overwrites them. Every entry
carries the `rows`, `csv_files` and `csv_headers` it was built from, and the suite
asserts the merged row count still adds up to what the workbook holds, so a future DCDD
that adds a row under an existing field name cannot make one disappear.
"""
from __future__ import annotations

from pathlib import Path

import yaml

# THE FOUR REAL OUTPUT FILES. Workday's loader takes these names and no others; every
# populated row must resolve into this set or the export is writing somewhere nobody
# reads. Measured from the workbook: 58 + 19 + 8 + 4 rows name one of these outright,
# and the two rows above resolve into them.
CSV_FILES: tuple[str, ...] = (
    "Submit_Customer_Invoice",
    "Submit_Customer_Invoice_Details",
    "Submit_Customer_Invoice_Lines",
    "Submit_Customer_Invoice_Lines_Details",
)

# The literal cell value meaning "every file", not a file.
ALL_CSVS = "All CSVs"

# The Required/Optional values that mean the DCDD will not load without the field.
# "Optional" and "Reference" are populated but not compulsory; "Do Not Populate" and a
# blank cell are not populated at all.
MUST_POPULATE: tuple[str, ...] = ("Required", "Design Requirement", "Constant Value")

DO_NOT_POPULATE = "Do Not Populate"

# A source of the form `constant:<value>` is not a vault column; it is a literal the
# writer emits on every row. Kept distinguishable so a resolver never goes looking for a
# table called "constant".
CONSTANT_PREFIX = "constant:"

# WHICH SIDE OF THE LOAD A SOURCE COLUMN IS NAMED FROM. `<table>.<column>` alone is
# ambiguous, and the ambiguity is load-bearing: a resolution gate has no target until an
# entry says whether its column name is this repo's or the source system's. See the
# mapping file's header for the full convention.
NOTATIONS: tuple[str, ...] = ("vault", "source", "constant")


def _dedup(values) -> tuple[str, ...]:
    """Distinct values, in first-seen order. Order is part of the evidence: `rows` and
    `csv_headers` line up with the workbook top to bottom."""
    seen: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.append(value)
    return tuple(seen)


def resolve_csv_files(value: str) -> tuple[str, ...]:
    """A csv File Name cell -> the real output files it names.

    "All CSVs" -> all four. "A, B" -> (A, B). Anything else -> itself. Unrecognised
    names are NOT filtered out: filtering them would make "the DCDD resolves to exactly
    four files" pass by construction, which is the vacuous check this exists to avoid.
    """
    text = (value or "").strip()
    if text == ALL_CSVS:
        return CSV_FILES
    return _dedup(part.strip() for part in text.split(","))


def load_mapping(path: Path) -> dict[str, dict]:
    """DCDD field name -> its mapping entry.

    Degrades to {} on a missing file, invalid YAML, or a non-dict root/`fields` -- all
    realistic merge outcomes. This is called at module scope by the test suite, so a
    mangled mapping must make the "both load" check go red rather than abort the caller
    and report every later check as absent. Same guard wd_reference.load_exclusions
    already ships for the same reason.
    """
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - reported by the check that consumes this
        return {}
    if not isinstance(raw, dict):
        return {}
    fields = raw.get("fields")
    if not isinstance(fields, dict):
        return {}
    return {str(name): dict(entry) for name, entry in fields.items()
            if isinstance(entry, dict)}


def dcdd_populated_fields(xlsx_path: Path, sheet: str) -> dict[str, dict]:
    """The DCDD rows that are NOT 'Do Not Populate', keyed by WD Field Name.

    ROWS ARE ADDRESSED BY COLUMN LETTER, not by header name -- tools/_xlsx.load returns
    {column letter: text} with empty cells omitted -- so row 1 is read once to build the
    header-name -> letter map and every later lookup goes through it. A DCDD that
    reordered or renamed its columns then produces empty cells and a red check, instead
    of quietly reading VALIDATIONS out of CATEGORY.
    """
    import sys  # noqa: PLC0415 - tools/ is not a package and is not on sys.path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
    from _xlsx import load  # noqa: PLC0415

    rows = load(str(xlsx_path)).get(sheet) or []
    if not rows:
        return {}
    header = {name: letter for letter, name in rows[0][1].items()}
    out: dict[str, dict] = {}
    for rownum, cells in rows[1:]:
        def get(key: str, _cells=cells) -> str:
            return (_cells.get(header.get(key, ""), "") or "").strip()

        required = get("Required/Optional")
        if required in (DO_NOT_POPULATE, ""):
            continue
        name = get("WD Field Name")
        if not name:
            continue
        # ONE TUPLE PER WORKBOOK ROW, AND IT IS THE PRIMARY STRUCTURE. The deduped views
        # below are DERIVED from it, never accumulated alongside it. Three tuples
        # deduped independently lose the correspondence between them: for
        # Customer_Invoice_ID that is 1 row, 4 files and 1 header, for
        # Dispute_Reason_Reference_ID 5 rows, 1 file and 5 headers. A consumer that
        # wanted "write header H into file F" could only zip them positionally, which is
        # correct today by coincidence and wrong the moment the lengths differ. It
        # should call csv_columns() instead, and csv_columns() reads per_row.
        out.setdefault(name, {"per_row": ()})["per_row"] += ((
            rownum,
            get("csv File Name"),
            get("csv Header"),
            required,
            tuple(v.strip() for v in get("VALIDATIONS").split("\n") if v.strip()),
        ),)
    for entry in out.values():
        entry.update(_derived_views(entry["per_row"]))
    return out


def _derived_views(per_row) -> dict:
    """The deduped views of one field's rows. DERIVED, so they cannot drift from the
    rows they summarise.

    `csv_file` and `required` are the FIRST row's values, kept because the single-row
    case is the overwhelming majority and reading them is how most callers will spell
    it. `csv_file_raw` and `required_all` are the whole truth for the nine rows where
    they differ.
    """
    return {
        "rows": tuple(r[0] for r in per_row),
        "csv_file": per_row[0][1] if per_row else "",
        "csv_file_raw": _dedup(r[1] for r in per_row),
        "csv_files": _dedup(f for r in per_row for f in resolve_csv_files(r[1])),
        "csv_headers": _dedup(r[2] for r in per_row),
        "required": per_row[0][3] if per_row else "",
        "required_all": _dedup(r[3] for r in per_row),
        "validations": list(_dedup(v for r in per_row for v in r[4])),
    }


# WHAT A WRITER MUST DO WITH A COLUMN. Three outcomes, and conflating the last two is
# the failure this module exists to stop.
MAPPED = "mapped"          # take the value from the entry's source
UNMAPPED = "unmapped"      # no mapping entry at all -- write it blank, which is correct
UNRESOLVED = "unresolved"  # mapped, but the source cannot be resolved -- REFUSE

COLUMN_STATUSES: tuple[str, ...] = (MAPPED, UNMAPPED, UNRESOLVED)


def field_status(name: str, mapping: dict[str, dict]) -> str:
    """What a writer must do with the columns of one DCDD field."""
    if name in unresolved_fields(mapping):
        return UNRESOLVED
    return MAPPED if name in mapping else UNMAPPED


def csv_columns(dcdd: dict[str, dict],
                mapping: dict[str, dict]) -> tuple[tuple[str, str, str, str], ...]:
    """Every real output column, as (csv file, csv header, WD field name, status).

    THE PAIRING TASK 5 NEEDS, and the reason per_row exists. A merged field name holds
    several rows, each row names its own header and its own file cell, and one file cell
    can resolve to several files -- so the column list is built by walking rows and
    expanding each row's files, never by zipping two deduped tuples of different
    lengths. 31 of the 91 populated rows carry a csv Header that differs from the WD
    Field Name, so the header is not recoverable from the key.

    `mapping` IS REQUIRED, WITH NO DEFAULT, AND THAT IS THE POINT. Most of the 95 columns
    have no mapping entry and a writer blanks them, which is correct -- Workday accepts
    an absent optional. A field marked `unresolved` is ALSO absent from a naive lookup,
    and blanking it is the opposite of correct: Customer_Invoice_ID is marked, and it is
    the must-populate join key on all four files, so blanking it exports four CSVs joined
    on nothing and looking exactly like a good export. Returning the columns without
    saying which kind each is would hand a caller those two cases as one. A caller that
    has no mapping cannot get a column list at all.
    """
    return tuple(
        (csv_file, header, name, field_status(name, mapping))
        for name, entry in dcdd.items() if isinstance(entry, dict)
        for _row, raw_file, header, _req, _val in (entry.get("per_row") or ())
        for csv_file in resolve_csv_files(raw_file))


def unresolved_columns(dcdd: dict[str, dict],
                       mapping: dict[str, dict]) -> tuple[tuple[str, str, str, str], ...]:
    """The columns a writer must REFUSE on, rather than blank.

    Task 5's refusal reads this. Non-empty means some field the DCDD wants is mapped to a
    source nobody can resolve, and the only safe rendering of that is no file at all.
    """
    return tuple(c for c in csv_columns(dcdd, mapping) if c[3] == UNRESOLVED)


def must_populate_fields(dcdd: dict[str, dict]) -> set[str]:
    """The field names the DCDD refuses to load without.

    Reads `required_all`, not `required`: a field name spanning several rows is
    compulsory if ANY of its rows says so. `required` is only the first row's value, and
    a merge that demoted the field would otherwise be invisible.
    """
    return {name for name, entry in dcdd.items()
            if isinstance(entry, dict)
            and set(entry.get("required_all") or ()) & set(MUST_POPULATE)}


def blanked_fields(dcdd: dict[str, dict], mapping: dict[str, dict]) -> set[str]:
    """Populated DCDD fields this repo deliberately writes BLANK -- no mapping entry.

    NAMED, SO THE DECISION CAN BE ASSERTED. 77 of the 82 populated fields are in here,
    and the module docstring used to claim "every DCDD field has a mapping" over the top
    of them. The two states this separates are "the workbook lists it and we have nothing
    to put in it" and "somebody forgot" -- indistinguishable in a mapping file, and the
    second is a column that silently stops being sent.

    A MUST-POPULATE FIELD MUST NEVER BE IN HERE, which is the property the suite asserts:
    the DCDD refuses the load without those five, so blanking one is a rejected file
    rather than an empty column.
    """
    return {name for name, entry in dcdd.items()
            if isinstance(entry, dict) and name not in mapping}


def declared_csv_files(dcdd: dict[str, dict]) -> set[str]:
    """Every output file the populated DCDD rows resolve to. Four, or something is
    wrong -- see the module docstring's two odd rows."""
    return {f for entry in dcdd.values() if isinstance(entry, dict)
            for f in (entry.get("csv_files") or ())}


def dcdd_row_count(dcdd: dict[str, dict]) -> int:
    """How many workbook rows the merged entries were built from. Guards the merge: 82
    entries built from 91 rows, and a merge that overwrote instead of merging would
    still report 82 keys while this drops."""
    return sum(len(entry.get("rows") or ()) for entry in dcdd.values()
               if isinstance(entry, dict))


def source_ref(entry: dict) -> tuple[str, str] | None:
    """(table, column) for a source that names one, or None for a constant or a source
    too malformed to split. Splits on the LAST dot, so `control.ctl_invoice_issuance.x`
    gives the two-part table and the column rather than the schema and the rest."""
    source = entry.get("source") if isinstance(entry, dict) else None
    if not isinstance(source, str) or source.startswith(CONSTANT_PREFIX):
        return None
    table, _dot, column = source.rpartition(".")
    return (table, column) if table and column else None


def unresolved_fields(mapping: dict[str, dict]) -> dict[str, str]:
    """Field name -> why its source cannot be resolved today.

    THE MECHANICAL BARRIER IN FRONT OF TASK 5. Two of the five sources this mapping was
    handed name columns that do not exist -- see the mapping file's header for both, and
    for why they were not renamed on inference. Without a marker those entries are
    indistinguishable from the three that resolve, and a writer built over them either
    raises UNRESOLVED_COLUMN in a pipeline with the offline suite green, or falls back to
    null and emits four CSVs whose join key is blank on every row. The second is the
    worse one: it looks exactly like a correct export.

    The marker is a REASON, not a flag. `unresolved: true` says nothing anybody can act
    on, and mapping_findings refuses it.
    """
    out: dict[str, str] = {}
    for name, entry in mapping.items():
        if not isinstance(entry, dict):
            continue
        reason = entry.get("unresolved")
        if reason is not None:
            out[name] = reason if isinstance(reason, str) else repr(reason)
    return out


def exportable_fields(mapping: dict[str, dict]) -> dict[str, dict]:
    """The entries a writer may export: everything NOT marked `unresolved`.

    Task 5 builds from this, not from load_mapping. An entry carrying the marker is one
    nobody can say the source column of, and a blank column is not an acceptable
    rendering of that.
    """
    marked = set(unresolved_fields(mapping))
    return {name: entry for name, entry in mapping.items() if name not in marked}


def claimed_source_refs(mapping: dict[str, dict]) -> dict[str, tuple[str, str]]:
    """Field name -> (table, column) for every entry that CLAIMS to resolve.

    Not carrying the marker IS the claim. Resolving the claim needs the entity model and
    the control standard, which this module deliberately does not import -- it stays
    pure and cheap -- so the claim is exposed here and adjudicated by the suite, which
    has both loaded already.
    """
    refs: dict[str, tuple[str, str]] = {}
    for name, entry in exportable_fields(mapping).items():
        ref = source_ref(entry)
        if ref is not None:
            refs[name] = ref
    return refs


def mapping_findings(mapping: dict[str, dict], dcdd: dict[str, dict]) -> list[str]:
    """Everything wrong with one mapping entry against the DCDD, as sentences.

    SHAPE ONLY. Whether a `source` names a column that EXISTS needs the entity model
    and the control standard, which this module does not import. What it does enforce is
    that every entry either carries an `unresolved: <reason>` marker or, by not carrying
    one, claims its source resolves -- and the suite adjudicates that claim against both
    catalogues, in BOTH directions: a claim that does not resolve goes red, and so does a
    marker on a source that does.
    """
    findings: list[str] = []
    for name in sorted(mapping):
        entry = mapping[name]
        known = dcdd.get(name)
        if not isinstance(entry, dict):
            findings.append(f"{name}: entry is {type(entry).__name__}, not a mapping")
            continue
        if not isinstance(entry.get("money"), bool):
            findings.append(f"{name}: `money` is {entry.get('money')!r}, not a bool")
        source = entry.get("source")
        if not isinstance(source, str) or not source.strip():
            findings.append(f"{name}: `source` is {source!r}, not a non-empty string")
        elif not source.startswith(CONSTANT_PREFIX):
            table, _dot, column = source.rpartition(".")
            if not table or not column:
                findings.append(
                    f"{name}: source {source!r} is neither `constant:<value>` nor "
                    f"`<table>.<column>`")
        if "unresolved" in entry and (not isinstance(entry.get("unresolved"), str)
                                      or not entry["unresolved"].strip()):
            findings.append(
                f"{name}: `unresolved` is {entry.get('unresolved')!r} -- the marker is "
                f"the REASON the source cannot be resolved, never a bare flag, because "
                f"whoever finds it next has to act on it")
        notation = entry.get("notation")
        if notation not in NOTATIONS:
            findings.append(
                f"{name}: `notation` is {notation!r}, not one of {list(NOTATIONS)} -- "
                f"`<table>.<column>` does not say whose column name it is, and a "
                f"resolution gate cannot adjudicate a claim with no declared side")
        elif (notation == "constant") != (isinstance(source, str)
                                          and source.startswith(CONSTANT_PREFIX)):
            findings.append(
                f"{name}: notation {notation!r} disagrees with source {source!r} -- "
                f"`constant` is exactly the entries that emit a literal")
        elif notation == "source" and "unresolved" not in entry:
            findings.append(
                f"{name}: notation `source` names a column upstream of the vault, which "
                f"nothing can select, so the entry must also carry `unresolved` saying "
                f"so -- otherwise it reads as exportable and is not")
        rule = entry.get("rule", None)
        if rule is not None and (not isinstance(rule, str) or not rule.strip()):
            findings.append(
                f"{name}: `rule` is {rule!r} -- a rule is the name of a transform or it "
                f"is null, never a blank string nobody can look up")
        # `constant` IS BOUND TO THE WORKBOOK'S OWN CLASSIFICATION, in both directions.
        # Without this, relabelling a source nobody can resolve as `constant:<literal>`
        # routes silently around the resolution gate -- the entry stops claiming a
        # column, so nothing adjudicates it, and a made-up literal ships instead of a
        # refusal. The DCDD says which fields are constants: Required/Optional reads
        # `Constant Value`, and on this workbook exactly one row does.
        known_req = set((known or {}).get("required_all") or ()) if isinstance(
            known, dict) else set()
        if isinstance(known, dict):
            if notation == "constant" and "Constant Value" not in known_req:
                findings.append(
                    f"{name}: notation `constant` but the DCDD classifies it "
                    f"{sorted(known_req)}, not `Constant Value` -- a literal is only a "
                    f"literal where the workbook says so, or `constant:` becomes a way "
                    f"to route around the resolution gate")
            if "Constant Value" in known_req and notation != "constant":
                findings.append(
                    f"{name}: the DCDD classifies this `Constant Value` but the entry "
                    f"declares notation {notation!r} -- a constant field sourced from a "
                    f"column is a mapping nobody wrote down")
        declared = entry.get("csv_file")
        if isinstance(known, dict) and declared not in (known.get("csv_file_raw") or ()):
            findings.append(
                f"{name}: mapped to csv_file {declared!r}, but the DCDD puts it in "
                f"{list(known.get('csv_file_raw') or ())}")
    return findings
