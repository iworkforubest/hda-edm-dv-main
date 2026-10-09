"""Minimal .xlsx reader. STDLIB ONLY, deliberately.

openpyxl is not a dependency of this repo and must not become one for a file we read
five times in a build. An .xlsx is a zip of XML; that is all this needs to know.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
import zipfile

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
      "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}


def load(path: str) -> dict[str, list[tuple[int, dict[str, str]]]]:
    """Sheet name -> [(row number, {column letter: cell text})]. Empty cells omitted."""
    z = zipfile.ZipFile(path)
    shared: list[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % NS["m"])))
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = {r.get("Id"): r.get("Target")
            for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
    out: dict[str, list[tuple[int, dict[str, str]]]] = {}
    for sh in wb.find("m:sheets", NS):
        target = rels[sh.get("{%s}id" % NS["r"])]
        if not target.startswith("xl/"):
            target = "xl/" + target.lstrip("/")
        rows: list[tuple[int, dict[str, str]]] = []
        for row in ET.fromstring(z.read(target)).iter("{%s}row" % NS["m"]):
            cells: dict[str, str] = {}
            for c in row.findall("m:c", NS):
                col = re.match(r"[A-Z]+", c.get("r")).group()
                v, isx = c.find("m:v", NS), c.find("m:is", NS)
                if c.get("t") == "s" and v is not None:
                    val = shared[int(v.text)]
                elif isx is not None:
                    val = "".join(t.text or "" for t in isx.iter("{%s}t" % NS["m"]))
                elif v is not None:
                    val = v.text
                else:
                    val = None
                if val:
                    cells[col] = val
            if cells:
                rows.append((int(row.get("r")), cells))
        out[sh.get("name")] = rows
    return out
