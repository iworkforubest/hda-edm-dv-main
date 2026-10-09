"""A minimal, deterministic .xlsx writer -- stdlib only.

WHY NOT openpyxl. Two reasons, and the second is the deciding one.

  1. DEPENDENCIES. This repo declares exactly one runtime dependency (pyyaml) and one
     optional, lazily imported one (databricks-sdk). Adding a spreadsheet library so a
     document generator can run means CI installs it on both Python floors to check a
     document. An .xlsx is a zip of XML; the part we need is small enough to write.

  2. IT COULD NOT BE GATED. openpyxl stamps docProps/core.xml with a creation timestamp,
     so regenerating the same model twice produces two different files. Every other
     generated artefact in this repo is committed and asserted to regenerate identically --
     that discipline is the only thing standing between a generated document and a stale
     one nobody notices. A workbook that changed on every run would have to be exempted,
     and an exempt artefact is an unchecked artefact.

WHAT IT WRITES. Inline strings (no shared-string table), one styles part, freeze panes and
an autofilter on the header row, and explicit column widths. Everything a reader needs from
a mapping workbook, and nothing that needs a library.

DETERMINISM IS BY CONSTRUCTION. Every zip entry is written with a fixed 1980-01-01
timestamp and in a fixed order, so the only thing that can move the bytes is the content.
Compression is left on -- the gate compares the UNZIPPED parts rather than the container,
so zlib's output is not part of the contract.

NOT A GENERAL LIBRARY. It writes strings. Numbers, dates, formulas and merged cells are all
absent because this mapping has none, and a half-implemented number format that silently
rendered a value as text would be worse than not offering one.
"""

from __future__ import annotations

import zipfile
from xml.sax.saxutils import escape

# 1980-01-01, the earliest a zip entry can express. Fixed so the bytes depend only on the
# content -- see the module docstring.
FIXED_DATE = (1980, 1, 1, 0, 0, 0)

# Style indices into cellXfs below. Named so a caller says what it means, not what it is.
S_DEFAULT, S_HEADER, S_WARN, S_MONO = 0, 1, 2, 3

_CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
{sheets}
</Types>"""

_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""

# The palette is the document's, not Excel's default: a header band and one warn fill for
# rows the mapping marks WILL NOT JOIN. Deliberately few -- a workbook that colour-codes
# everything communicates nothing.
_STYLES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="4">
<font><sz val="10"/><name val="Calibri"/></font>
<font><b/><sz val="10"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>
<font><sz val="10"/><color rgb="FF9C3B2E"/><name val="Calibri"/></font>
<font><sz val="9"/><name val="Consolas"/></font>
</fonts>
<fills count="4">
<fill><patternFill patternType="none"/></fill>
<fill><patternFill patternType="gray125"/></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FF2F5D8C"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFBEAE7"/><bgColor indexed="64"/></patternFill></fill>
</fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="4">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment vertical="center"/></xf>
<xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
<xf numFmtId="0" fontId="3" fillId="0" borderId="0" xfId="0" applyFont="1" applyAlignment="1"><alignment vertical="top"/></xf>
</cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>"""


def col_letter(index: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA. Written out rather than assumed to stop at Z."""
    out = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        out = chr(65 + rem) + out
    return out


class Sheet:
    """One worksheet: a header row, data rows, and per-column widths.

    `row_style` is called with each row's index and returns a style id, so a caller can
    highlight a row on its own criteria without this writer knowing what the criteria are.
    """

    def __init__(self, name: str, header: list[str], rows: list[list[str]],
                 widths: list[int] | None = None,
                 row_style=None,
                 mono_columns: frozenset[int] = frozenset()):
        # Excel refuses > 31 characters and the characters []:*?/\\ in a sheet name. Fail
        # loudly here rather than emitting a workbook Excel declines to open.
        assert len(name) <= 31, f"sheet name too long for Excel: {name!r}"
        assert not set(name) & set("[]:*?/\\"), f"illegal sheet name: {name!r}"
        self.name = name
        self.header = header
        self.rows = rows
        self.widths = widths or [18] * len(header)
        self.row_style = row_style or (lambda i: S_DEFAULT)
        self.mono_columns = mono_columns

    def xml(self) -> str:
        last_col = col_letter(len(self.header))
        parts = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                 '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
                 f'<dimension ref="A1:{last_col}{len(self.rows) + 1}"/>',
                 # FREEZE THE HEADER AND FILTER ON IT. A 469-row mapping without a frozen
                 # header is a document you lose your place in on the first scroll.
                 '<sheetViews><sheetView workbookViewId="0">',
                 '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>',
                 '</sheetView></sheetViews>',
                 '<sheetFormatPr defaultRowHeight="13"/>',
                 '<cols>']
        for i, w in enumerate(self.widths, start=1):
            parts.append(f'<col min="{i}" max="{i}" width="{w}" customWidth="1"/>')
        parts.append("</cols><sheetData>")

        parts.append('<row r="1" ht="20" customHeight="1">')
        for i, h in enumerate(self.header, start=1):
            parts.append(f'<c r="{col_letter(i)}1" s="{S_HEADER}" t="inlineStr">'
                         f'<is><t>{escape(str(h))}</t></is></c>')
        parts.append("</row>")

        for n, row in enumerate(self.rows):
            r = n + 2
            style = self.row_style(n)
            parts.append(f'<row r="{r}">')
            for i, value in enumerate(row, start=1):
                text = "" if value is None else str(value)
                if not text:
                    continue  # an empty cell is absence; writing it adds bytes and no meaning
                s = style if style != S_DEFAULT else (
                    S_MONO if (i - 1) in self.mono_columns else S_DEFAULT)
                parts.append(f'<c r="{col_letter(i)}{r}" s="{s}" t="inlineStr">'
                             f'<is><t xml:space="preserve">{escape(text)}</t></is></c>')
            parts.append("</row>")
        parts.append("</sheetData>")
        parts.append(f'<autoFilter ref="A1:{last_col}{len(self.rows) + 1}"/>')
        parts.append("</worksheet>")
        return "".join(parts)


def workbook_parts(sheets: list[Sheet]) -> dict[str, str]:
    """Every part of the package, by archive path. Returned rather than written so the
    staleness gate can compare parts instead of zip bytes -- see the module docstring."""
    overrides = "\n".join(
        f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
        f'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for i in range(1, len(sheets) + 1))

    wb = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
          '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
          'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
          "<sheets>"]
    rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">']
    for i, sh in enumerate(sheets, start=1):
        wb.append(f'<sheet name="{escape(sh.name)}" sheetId="{i}" r:id="rId{i}"/>')
        rels.append(f'<Relationship Id="rId{i}" '
                    f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                    f'Target="worksheets/sheet{i}.xml"/>')
    wb.append("</sheets></workbook>")
    rels.append(f'<Relationship Id="rId{len(sheets) + 1}" '
                f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
                f'Target="styles.xml"/>')
    rels.append("</Relationships>")

    parts = {
        "[Content_Types].xml": _CONTENT_TYPES.format(sheets=overrides),
        "_rels/.rels": _RELS,
        "xl/workbook.xml": "".join(wb),
        "xl/_rels/workbook.xml.rels": "".join(rels),
        "xl/styles.xml": _STYLES,
    }
    for i, sh in enumerate(sheets, start=1):
        parts[f"xl/worksheets/sheet{i}.xml"] = sh.xml()
    return parts


def write(path, sheets: list[Sheet]) -> None:
    """Write the package. Fixed timestamps and a fixed part order, so identical content
    produces an identical archive."""
    parts = workbook_parts(sheets)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name in sorted(parts):
            info = zipfile.ZipInfo(name, date_time=FIXED_DATE)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            z.writestr(info, parts[name])


# --------------------------------------------------------------------------- #
# READING BACK. Not symmetry for its own sake -- the staleness gate needs it.
#
# WHY BYTES AND PARTS BOTH TURNED OUT TO BE THE WRONG THING TO GATE. Measured 4 September
# 2026, within hours of publishing the workbook: opening it in LibreOffice and saving
# rewrote the package -- adding docProps/core.xml (with a creation timestamp), a
# sharedStrings table, and two theme parts -- so the part-equality check failed on a file
# whose CONTENT was untouched. That is the worst kind of gate: it fires on the document
# being USED as intended, and the fix a hurried reader reaches for is to delete the gate.
#
# The rows are what the document promises. Gate those, and container churn is free while a
# hand-edited cell still fails -- which is correct, because a hand-edited cell in a
# generated document is exactly the drift the whole discipline exists to catch.

def read_sheets(path) -> dict[str, list[list[str]]]:
    """{sheet name: rows of cell text}, from any writer's .xlsx.

    Handles BOTH string encodings, which is the entire point: this module writes inline
    strings, and a spreadsheet app that round-trips the file will rewrite them into a
    shared-strings table. A reader that understood only its own output would report every
    such file as empty -- passing a gate by finding nothing, which is the failure shape
    this repo keeps meeting.

    Deliberately small: cell text only, no types, no formulas, no styles. It exists to
    answer "does this workbook still hold the model's rows", and anything more would be a
    spreadsheet library nobody asked for.
    """
    import re
    import xml.etree.ElementTree as ET

    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    RELNS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"

    with zipfile.ZipFile(path) as z:
        names = set(z.namelist())

        # The shared-strings table, when the writer used one.
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall(f"{NS}si"):
                # A string can be split across runs (<r><t>..</t></r>); join them all.
                shared.append("".join(t.text or "" for t in si.iter(f"{NS}t")))

        # Sheet name -> part path, via the workbook's relationships. NOT by assuming
        # sheet1.xml is the first sheet: the order of the parts and the order of the
        # sheets are independent, and a rewritten package need not agree with ours.
        rels = {}
        if "xl/_rels/workbook.xml.rels" in names:
            for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")):
                rels[r.get("Id")] = r.get("Target")
        out: dict[str, list[list[str]]] = {}
        for sh in ET.fromstring(z.read("xl/workbook.xml")).iter(f"{NS}sheet"):
            target = rels.get(sh.get(f"{RELNS}id"), "")
            part = ("xl/" + target.lstrip("/")) if not target.startswith("xl/") else target
            if part not in names:
                continue
            rows: list[list[str]] = []
            for row in ET.fromstring(z.read(part)).iter(f"{NS}row"):
                cells: dict[int, str] = {}
                for c in row.findall(f"{NS}c"):
                    ref = c.get("r") or ""
                    letters = re.match(r"([A-Z]+)", ref)
                    idx = 0
                    for ch in (letters.group(1) if letters else ""):
                        idx = idx * 26 + (ord(ch) - 64)
                    if c.get("t") == "s":                      # shared-string index
                        v = c.find(f"{NS}v")
                        text = shared[int(v.text)] if v is not None and v.text else ""
                    elif c.get("t") == "inlineStr":
                        text = "".join(t.text or "" for t in c.iter(f"{NS}t"))
                    else:
                        v = c.find(f"{NS}v")
                        text = (v.text or "") if v is not None else ""
                    cells[idx] = text
                # An empty cell is written as absence, so rebuild by position rather than
                # by order of appearance -- otherwise a gap shifts every later column.
                width = max(cells) if cells else 0
                rows.append([cells.get(i, "") for i in range(1, width + 1)])
            out[sh.get("name") or part] = rows
    return out
