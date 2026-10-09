"""Workday Get_References: build the request, read the response. Nothing here calls out.

WHY THIS EXISTS. The Journal load needs Workday's reference values -- Company_Reference_ID,
Ledger_Account_ID, the worktag types, the Tax Detail codes. We had planned to ask the
Workday team to maintain a crosswalk by hand; they publish the values through a web
service instead (WDJ-5). This is the half of that integration that can be tested without
a network, a tenant, or a credential.

BUILT FROM THE PUBLISHED SCHEMA, AND CORRECTED BY THE TENANT. Every element name below
was read out of `Integrations.xsd` for Web Services v46.2 -- and the first version of this
module got the most important one wrong, which is worth recording rather than quietly
fixing.

  * I LISTED THE SCHEMA'S ELEMENTS WITH A PATTERN THAT ONLY MATCHED
    `<xsd:element name="..." type="...">`. `Reference_ID_Type` declares an inline
    `<xsd:simpleType>` and carries no `type` attribute, so the pattern skipped it -- and I
    wrote down its ABSENCE as a finding: "there is no server-side filter". A search that
    silently drops what it cannot match, reported as a fact about the thing searched, is
    this repo's own favourite defect wearing a different hat.

    The tenant said otherwise on the first authenticated call:
    `Element Content 'Reference_ID_Type' is required, on internal element 'Get References
    Request Criteria'`. Parsed properly it is `minOccurs="1"` -- REQUIRED, not merely
    available.

  * SO EVERY CALL NAMES ONE REFERENCE ID TYPE, and `build_request()` demands it rather
    than defaulting: a default here would pick a type on the caller's behalf and return a
    confident list of the wrong thing.

  * ORDER IS THE SCHEMA'S, and it matters twice over. `Get_References_Request_CriteriaType`
    is an `xsd:sequence`, so `Reference_ID_Type` precedes `Include_Defaulted_Values_Only`;
    `Response_FilterType` puts `As_Of_*` before `Page` and `Count`.

  * `Count` is `totalDigits="3"`, so a page holds at most 999 rows. Verified with a parser
    the second time, not a regex.

WHAT IS NOT PROVEN. This has never been run against a Workday tenant. The request shape is
the schema's and the parser is exercised against fixtures built from it, which is exactly
as far as offline testing reaches: it demonstrates that we build what the schema describes,
not that Workday accepts it. Until someone runs `tools/fetch_workday_references.py` with a
credential, treat "it works" as untested. See docs/workday_reference_service_access.md.

THE CREDENTIAL IS NOT A PARAMETER OF THE REQUEST, and that separation is deliberate rather
than tidy. `build_request()` takes no username or password and returns a body safe to log,
diff, or paste into a defect report. `envelope()` is the only function that has ever seen a
secret, it is a dozen lines, and `tests/test_accelerator.py` asserts that what
`build_request()` returns cannot contain one. The bundle's own root folder is world-writable
until PLT-3 lands, so a secret reaching any file this repo ships is a secret handed to the
whole workspace.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Iterable

# urn:com.workday/bsvc -- the Business Services namespace every wd: element lives in.
WD = "urn:com.workday/bsvc"
SOAPENV = "http://schemas.xmlsoap.org/soap/envelope/"
# The OASIS 2004/01 profile Workday's WS-Security UsernameToken uses.
WSSE = ("http://docs.oasis-open.org/wss/2004/01/"
        "oasis-200401-wss-wssecurity-secext-1.0.xsd")
PASSWORD_TEXT = ("http://docs.oasis-open.org/wss/2004/01/"
                 "oasis-200401-wss-username-token-profile-1.0#PasswordText")

# `Count` is restricted to totalDigits="3" in the schema. Asking for 1000 is not a large
# page, it is an invalid request.
MAX_COUNT = 999

_NS = {"wd": WD, "env": SOAPENV}


# --------------------------------------------------------------------------- #
# THE LANDED SHAPE: flat rows a source binding can declare columns against.
#
# `parse_response` and `parse_organizations` return nested documents, which is faithful to
# what Workday sends and useless to a vault binding -- a binding names COLUMNS. Flattening
# has to happen somewhere, and doing it here rather than in landing SQL is what lets
# tests/test_accelerator.py gate it.
#
# EVERY ROW CARRIES ITS OWN PROVENANCE, and that is not decoration. A landed table is
# insert-only and a later retrieval must be able to supersede an earlier one, which means
# `dedup_order` needs a column to order by. Without `retrieved_at` the second retrieval is
# indistinguishable from the first and the hub keeps whichever row Spark happened to read.
#
# `retrieved_at` IS OUR CLOCK AND IS DELIBERATELY NOT CALLED applied_dts. It records when we
# ASKED. Workday's own change timestamp is `Last_Updated_DateTime` on the organisation
# payload, and the two are different facts -- this repo spent a week on that distinction
# over Fieldglass, where binding a delivery clock as a business clock would have made every
# `_v1` interval wrong.
REFERENCE_COLUMNS = ("reference_id_type", "id", "descriptor",
                     "referenced_object_descriptor", "wid",
                     "host", "tenant", "wws_version", "retrieved_at")

# `subtype` EARNS ITS COLUMN: measured against the tenant on 24 September, all 82
# Companies carry Organization_Type_ID = Company AND Organization_Subtype_ID = Company, so
# it is redundant TODAY. It is kept because the subtype is how Workday distinguishes kinds
# within a type, and a roster that silently merged two subtypes of company would be wrong
# in exactly the way this repo keeps finding -- invisibly, and only in the totals.
# `effective_from` AND `relationship_status` ARE OURS, NOT WORKDAY'S, and that is the
# landing writer's job: Workday's vocabulary is translated here once, rather than in every
# binding that reads it. An esat needs an ordering column and a status column by name, and
# a binding cannot compute either -- so a shape without them fails at pipeline DEFINITION
# time on a deploy, in a workspace, which is the most expensive place to discover it.
#
# effective_from IS Last_Updated_DateTime, AND IT IS NOT AN EFFECTIVE DATE. Workday says
# when a row was last MODIFIED, not when the change took legal effect, and
# Get_Organizations takes no as-of parameter. It is the best ordering evidence the source
# has, it is what stops two versions delivered in one batch colliding, and calling it
# effective_from is the vault's name for that slot rather than a claim about Workday.
#
# relationship_status IS DERIVED FROM PRESENCE. Get_Organizations is a CURRENT-STATE
# retrieval: every edge it returns is in force at the moment of the call. So a landed edge
# is `active`, an organisation Workday marks Inactive lands `inactive`, and an edge that
# has ENDED is not delivered at all -- its closure is the absence of a row in a later
# retrieval, which the effectivity satellite's driving key turns into a closed interval.
ORGANISATION_COLUMNS = ("reference_id", "name", "code", "inactive", "type", "subtype",
                        "superior", "top_level",
                        "effective_from", "relationship_status",
                        "host", "tenant", "wws_version", "retrieved_at")

# ONE ROW PER EDGE, which is what lnk_legal_entity_consolidation binds to (RENAMED 24 Sep:
# the Workday membership feeds the consolidation family, not the client hierarchy).
# A consolidation group
# with three members is three rows, never one row with an array: a link keys on
# (parent, child), and an array cannot be a business key. Flatten here or flatten later in
# SQL nobody gated.
MEMBERSHIP_COLUMNS = ("group_reference_id", "member_reference_id", "relation",
                      "effective_from", "relationship_status",
                      "host", "tenant", "wws_version", "retrieved_at")

# THE LANDED TABLE NAMES, BESIDE THE SHAPES THEY CARRY. A binding names a table; this is
# what lets a check ask "does the shape behind this table actually have that column" without
# anybody keeping a list by hand. The names are Task 3 of the Workday reference plan.
LANDED_TABLES = {
    "wd_reference_id": REFERENCE_COLUMNS,
    "wd_organization": ORGANISATION_COLUMNS,
    "wd_organization_membership": MEMBERSHIP_COLUMNS,
}

_LANDING_PICK = {
    "reference": (REFERENCE_COLUMNS,
                  ("reference_id_type", "id", "descriptor",
                   "referenced_object_descriptor", "wid")),
    "organisation": (ORGANISATION_COLUMNS,
                     ("reference_id", "name", "code", "inactive", "type", "subtype",
                      "superior", "top_level", "effective_from", "relationship_status")),
}


def landing_rows(kind: str, rows, *, tenant: str, host: str, version: str,
                 retrieved_at: str) -> list[dict]:
    """Flat rows for one retrieval, ready to land as a table.

    `kind` is "reference", "organisation" or "membership". An unknown kind RAISES rather
    than returning nothing: a typo that yields [] lands an empty table, which reads as a
    tenant holding nothing rather than as a mistake.

    MEMBERSHIP EXPLODES THE HIERARCHY. `included` and `included_in` are arrays on an
    organisation and edges in a link. Both directions are emitted, tagged by `relation`, so
    a consumer can take whichever the source populated without having to know which way
    round this tenant happens to express containment.
    """
    stamp = {"host": host, "tenant": tenant, "wws_version": version,
             "retrieved_at": retrieved_at}

    if kind in _LANDING_PICK:
        _, keys = _LANDING_PICK[kind]
        return [{**{k: r.get(k) for k in keys},
                 **({"relationship_status":
                     "inactive" if r.get("inactive") else "active"}
                    if "relationship_status" in keys else {}),
                 **stamp} for r in rows]

    if kind == "membership":
        out: list[dict] = []
        for row in rows:
            # A DELIVERED EDGE IS AN OPEN EDGE. Get_Organizations returns current state,
            # so every edge in this retrieval is in force; an edge that has ended simply
            # is not here, and its closure is its absence from a later retrieval.
            edge = {"effective_from": row.get("effective_from"),
                    "relationship_status": "active", **stamp}
            for member in row.get("included") or ():
                out.append({"group_reference_id": row.get("reference_id"),
                            "member_reference_id": member,
                            "relation": "includes", **edge})
            for group in row.get("included_in") or ():
                out.append({"group_reference_id": group,
                            "member_reference_id": row.get("reference_id"),
                            "relation": "included_in", **edge})
        return out

    raise ValueError(
        f"kind must be reference|organisation|membership, got {kind!r} -- an unknown kind "
        f"returning [] would land an empty table that reads as an empty tenant")


def endpoint(host: str, tenant: str, version: str, service: str = "Integrations") -> str:
    """One Workday web service URL for a tenant.

    Shaped, not guessed: /ccx/service/<tenant>/<service>/<version> is Workday's published
    form, and `version` carries its own leading `v` (v46.2) because that is how the tenant
    and the schema both spell it.

    `service` DEFAULTS TO Integrations because that is where Get_References lives. The
    organisation hierarchy is in Human_Resources -- a different service, the same tenant,
    the same version, and the same `urn:com.workday/bsvc` namespace, which is why one
    envelope builder serves both.
    """
    for name, value in (("host", host), ("tenant", tenant), ("version", version),
                        ("service", service)):
        if not str(value or "").strip():
            raise ValueError(f"{name} is required to build a Workday endpoint")
    return f"https://{host.strip('/')}/ccx/service/{tenant}/{service}/{version}"


def build_request(reference_id_type: str, page: int = 1, count: int = 100, *,
                  defaulted_only: bool = False,
                  as_of_effective_date: str | None = None,
                  as_of_entry_datetime: str | None = None) -> str:
    """The Get_References_Request body. NO CREDENTIAL IS ACCEPTED OR EMITTED HERE.

    `reference_id_type` IS POSITIONAL AND HAS NO DEFAULT, deliberately. The schema makes it
    required; a default would answer a question the caller did not ask, and return a
    confident list of the wrong thing.
    """
    if not str(reference_id_type or "").strip():
        raise ValueError(
            "reference_id_type is required -- the schema has it at minOccurs=1 and the "
            "tenant refuses the call without it")
    if page < 1:
        raise ValueError(f"page is 1-based; got {page}")
    if not 1 <= count <= MAX_COUNT:
        raise ValueError(
            f"count must be 1..{MAX_COUNT}: the schema restricts Count to three digits, "
            f"so {count} is not a big page, it is an invalid request")

    req = ET.Element(f"{{{WD}}}Get_References_Request")
    # SEQUENCE ORDER: Reference_ID_Type precedes Include_Defaulted_Values_Only. A
    # validating endpoint rejects them the other way round.
    criteria = ET.SubElement(req, f"{{{WD}}}Request_Criteria")
    ET.SubElement(criteria, f"{{{WD}}}Reference_ID_Type").text = str(
        reference_id_type).strip()
    ET.SubElement(criteria, f"{{{WD}}}Include_Defaulted_Values_Only").text = (
        "true" if defaulted_only else "false")

    # ORDER IS THE SCHEMA'S: Response_FilterType is an xsd:sequence, so As_Of_* precede
    # Page and Count. A validating endpoint rejects them in any other order.
    filt = ET.SubElement(req, f"{{{WD}}}Response_Filter")
    if as_of_effective_date:
        ET.SubElement(filt, f"{{{WD}}}As_Of_Effective_Date").text = as_of_effective_date
    if as_of_entry_datetime:
        ET.SubElement(filt, f"{{{WD}}}As_Of_Entry_DateTime").text = as_of_entry_datetime
    ET.SubElement(filt, f"{{{WD}}}Page").text = str(page)
    ET.SubElement(filt, f"{{{WD}}}Count").text = str(count)

    ET.register_namespace("wd", WD)
    return ET.tostring(req, encoding="unicode")


def build_organizations_request(page: int = 1, count: int = 100, *,
                                include_inactive: bool = False,
                                organization_type: str | None = None) -> str:
    """A Get_Organizations_Request body, for the ORGANISATION HIERARCHY.

    A DIFFERENT SERVICE AND A DIFFERENT SHAPE, so it gets its own builder rather than a
    flag on the other one. Get_References answers "what ids exist"; it cannot answer "what
    contains what", because a reference index has no room for the relationship. The
    hierarchy is `Organization_Data/Superior_Organization_Reference`, and it lives in
    Human_Resources.

    NOTHING HERE IS REQUIRED, unlike Get_References' Reference_ID_Type -- every child of
    `Organization_Request_CriteriaType` is minOccurs=0. Read from Human_Resources.xsd
    v46.2 with a parser, having learned that lesson once already.

    RESPONSE_GROUP ASKS FOR THE HIERARCHY EXPLICITLY. `Include_Hierarchy_Data` is one of
    four booleans the schema offers and it is the only one this needs; the others carry
    roles, supervisory and staffing-restriction data we have no use for and would only be
    paging through.
    """
    if page < 1:
        raise ValueError(f"page is 1-based; got {page}")
    if not 1 <= count <= MAX_COUNT:
        raise ValueError(f"count must be 1..{MAX_COUNT}; got {count}")

    req = ET.Element(f"{{{WD}}}Get_Organizations_Request")
    # SEQUENCE ORDER, as the schema declares it: Request_References, Request_Criteria,
    # Response_Filter, Response_Group.
    criteria = ET.SubElement(req, f"{{{WD}}}Request_Criteria")
    if organization_type:
        type_ref = ET.SubElement(criteria, f"{{{WD}}}Organization_Type_Reference")
        node = ET.SubElement(type_ref, f"{{{WD}}}ID")
        node.set(f"{{{WD}}}type", "Organization_Type_ID")
        node.text = organization_type
    ET.SubElement(criteria, f"{{{WD}}}Include_Inactive").text = (
        "true" if include_inactive else "false")

    filt = ET.SubElement(req, f"{{{WD}}}Response_Filter")
    ET.SubElement(filt, f"{{{WD}}}Page").text = str(page)
    ET.SubElement(filt, f"{{{WD}}}Count").text = str(count)

    group = ET.SubElement(req, f"{{{WD}}}Response_Group")
    ET.SubElement(group, f"{{{WD}}}Include_Hierarchy_Data").text = "true"

    ET.register_namespace("wd", WD)
    return ET.tostring(req, encoding="unicode")


def _wws_bool(value: str | None) -> bool:
    """A Workday web-services boolean, which is `0` or `1` and not `true` or `false`.

    WORTH A FUNCTION BECAUSE THE WRONG VERSION CANNOT FAIL LOUDLY. `Inactive` was compared
    to the string "true", so it was False for every organisation the tenant has ever
    returned -- including any genuinely inactive one, which would have been read as active
    and loaded as a live legal entity. Measured 24 September: all 82 Companies send `0`,
    so no fixture built from a live response would have caught it either; only reading the
    raw XML did. Both spellings are accepted because the schema says boolean and the wire
    says 0/1, and an integration should not care which one a future version sends.
    """
    return (value or "").strip().lower() in ("1", "true")


def parse_organizations(xml: str) -> tuple[list[dict], dict]:
    """(organisations, results) from a Get_Organizations_Response.

    READ FROM Organization_WWS_DataType, WHICH IS NOT Organization_DataType. Both exist in
    Human_Resources.xsd, both describe an organisation, and only one is the response. The
    first version of this function used the other -- `Organization_Reference_ID` and
    `Organization_Name` instead of `Reference_ID` and `Name` -- and would have parsed every
    field as None while raising nothing at all. Caught by reading the schema a second time
    rather than by a test, which is worth admitting: no fixture written from the wrong type
    would have failed either.

    THE HIERARCHY IS IN ITS OWN BLOCK and only arrives if asked for -- `Hierarchy_Data`,
    populated when the request sets `Include_Hierarchy_Data`. Three of its fields matter
    here and they answer different questions:

      * `superior`  -- the parent. One per organisation, and what makes a tree.
      * `included`  -- what a CONSOLIDATION GROUP contains. This is the membership a
                       reference index cannot express, and the reason for this whole call.
      * `included_in` -- the inverse, so a company can name the groups it rolls up into
                       without anyone traversing the tree backwards.
    """
    problem = fault(xml)
    if problem:
        raise WorkdayFault(problem)
    root = ET.fromstring(xml)

    def ids_of(node) -> dict:
        out = {}
        if node is not None:
            for child in node.findall(f"{{{WD}}}ID"):
                out[child.get(f"{{{WD}}}type") or child.get("type") or ""] = child.text
        return out

    def preferred(node) -> str | None:
        """The non-WID id if there is one. A WID identifies nothing outside this tenant."""
        found = ids_of(node)
        for key, value in found.items():
            if key != "WID":
                return value
        return found.get("WID")

    rows: list[dict] = []
    for org in root.iter(f"{{{WD}}}Organization"):
        data = org.find(f"{{{WD}}}Organization_Data")
        hierarchy = data.find(f"{{{WD}}}Hierarchy_Data") if data is not None else None
        type_ref = data.find(f"{{{WD}}}Organization_Type_Reference") if data is not None else None
        subtype_ref = (data.find(f"{{{WD}}}Organization_Subtype_Reference")
                       if data is not None else None)
        rows.append({
            "reference_id": _text(data, "Reference_ID"),
            "name": _text(data, "Name"),
            "code": _text(data, "Organization_Code"),
            "inactive": _wws_bool(_text(data, "Inactive")),
            "effective_from": _text(data, "Last_Updated_DateTime"),
            "wid": ids_of(org.find(f"{{{WD}}}Organization_Reference")).get("WID"),
            # THE TYPE IS A CHILD ID, NOT AN ATTRIBUTE. This read `Descriptor` off the
            # element and returned None for all 82 Companies -- the same mistake as
            # `descriptor` vs `referenced_object_descriptor` on Get_References, made again
            # on a different service. Proven against the tenant 24 September: the value is
            # in `<wd:ID wd:type="Organization_Type_ID">Company</wd:ID>`, which is what
            # `preferred` already knows how to read.
            "type": preferred(type_ref),
            "subtype": preferred(subtype_ref),
            "superior": preferred(
                hierarchy.find(f"{{{WD}}}Superior_Organization_Reference")
                if hierarchy is not None else None),
            "top_level": preferred(
                hierarchy.find(f"{{{WD}}}Top-Level_Organization_Reference")
                if hierarchy is not None else None),
            "included": [preferred(n) for n in hierarchy.findall(
                f"{{{WD}}}Included_Organization_Reference")] if hierarchy is not None else [],
            "included_in": [preferred(n) for n in hierarchy.findall(
                f"{{{WD}}}Included_In_Organization_Reference")] if hierarchy is not None else [],
        })

    results_node = root.find(f".//{{{WD}}}Response_Results")
    results = {name.lower(): _int(_text(results_node, name))
               for name in ("Total_Results", "Total_Pages", "Page_Results", "Page")}
    return rows, results


def envelope(body: str, username: str, password: str) -> str:
    """Wrap a request body in a SOAP envelope with a WS-Security UsernameToken.

    THE ONLY FUNCTION IN THIS REPO THAT HANDLES THE PASSWORD. Kept small and kept separate
    so that everything else -- the request builder, the parser, every test fixture -- is
    provably secret-free rather than believed to be.

    PasswordText over TLS is what Workday's UsernameToken profile expects; there is no
    digest option to prefer here. The transport is therefore load-bearing: this must never
    be sent over plain HTTP, which is why `endpoint()` hardcodes https.
    """
    if not username or not password:
        raise ValueError("both username and password are required to authenticate")
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<env:Envelope xmlns:env="{SOAPENV}">'
        f'<env:Header>'
        f'<wsse:Security xmlns:wsse="{WSSE}" env:mustUnderstand="1">'
        f'<wsse:UsernameToken>'
        f'<wsse:Username>{_escape(username)}</wsse:Username>'
        f'<wsse:Password Type="{PASSWORD_TEXT}">{_escape(password)}</wsse:Password>'
        f'</wsse:UsernameToken></wsse:Security></env:Header>'
        f'<env:Body>{body}</env:Body></env:Envelope>'
    )


def _escape(value: str) -> str:
    """XML-escape a credential component. A password containing & or < is legal."""
    return (str(value).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def fault(xml: str) -> str | None:
    """The SOAP fault message, or None. Checked BEFORE parsing: a fault is a 200 too.

    Workday answers a rejected request with a well-formed envelope carrying a Fault, and an
    HTTP status that does not always say so. A parser that goes straight for Response_Data
    finds nothing and reports zero rows -- which reads exactly like a tenant with no
    reference data, and is how a credential or permission problem becomes a silent empty
    load.
    """
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        return f"response is not XML: {exc}"
    for tag in (f"{{{SOAPENV}}}Fault", "Fault"):
        found = root.iter(tag)
        for node in found:
            parts = [t.text for t in node.iter() if t.text and t.text.strip()
                     and t.tag.rsplit("}", 1)[-1] in ("faultcode", "faultstring", "Detail",
                                                      "Validation_Error", "Message")]
            return " | ".join(dict.fromkeys(parts)) or "SOAP Fault with no message"
    return None


def parse_response(xml: str) -> tuple[list[dict], dict]:
    """(rows, results) from a Get_References_Response. Raises on a fault rather than
    returning nothing, so a rejected call cannot be mistaken for an empty tenant.

    A row carries both halves the schema offers, because they answer different questions:
    `Reference_ID_Data` is the id AS CONFIGURED -- the value a journal must quote -- while
    `Reference_ID_Reference/ID` is the index entry, including the WID.

    THE WID IS TENANT-SPECIFIC. It is returned because it is in the response, not because
    it is safe to persist: one retrieved from an implementation tenant identifies nothing
    in production. See docs/workday_reference_service_access.md.
    """
    problem = fault(xml)
    if problem:
        raise WorkdayFault(problem)
    root = ET.fromstring(xml)

    rows: list[dict] = []
    for ref in root.iter(f"{{{WD}}}Reference_ID"):
        data = ref.find(f"{{{WD}}}Reference_ID_Data")
        index = ref.find(f"{{{WD}}}Reference_ID_Reference")
        ids = {}
        if index is not None:
            for node in index.findall(f"{{{WD}}}ID"):
                ids[node.get(f"{{{WD}}}type") or node.get("type") or ""] = node.text
        rows.append({
            "descriptor": ref.get(f"{{{WD}}}Descriptor") or ref.get("Descriptor"),
            "id": _text(data, "ID"),
            "new_id": _text(data, "New_ID"),
            "reference_id_type": _text(data, "Reference_ID_Type"),
            "referenced_object_descriptor": _text(data, "Referenced_Object_Descriptor"),
            "wid": ids.get("WID"),
            "index_ids": ids,
        })

    results_node = root.find(f".//{{{WD}}}Response_Results")
    results = {name.lower(): _int(_text(results_node, name))
               for name in ("Total_Results", "Total_Pages", "Page_Results", "Page")}
    return rows, results


def select(rows: Iterable[dict], reference_id_type: str) -> list[dict]:
    """The rows of one reference id type.

    NOT THE PRIMARY NARROWING -- the service filters, and `build_request` must name a type.
    This is for a caller that has merged several retrievals and wants one type back out,
    and for asserting that a response contains what was asked for. If it ever returns rows
    of a type you did not request, the request and the response disagree and that is worth
    knowing rather than silently accepting.
    """
    wanted = str(reference_id_type).strip()
    return [r for r in rows if (r.get("reference_id_type") or "").strip() == wanted]


def types_present(rows: Iterable[dict]) -> dict[str, int]:
    """How many rows of each reference id type came back. The cheapest sanity check there
    is on a retrieval nobody has run before: an empty dict means the parse found nothing,
    which is a different problem from a tenant holding nothing."""
    counts: dict[str, int] = {}
    for row in rows:
        key = (row.get("reference_id_type") or "").strip() or "(none)"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


class WorkdayFault(RuntimeError):
    """A SOAP fault. Raised rather than returned so it cannot be ignored into an empty load."""


def _text(node, name: str) -> str | None:
    if node is None:
        return None
    found = node.find(f"{{{WD}}}{name}")
    return found.text if found is not None else None


def _int(value) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
