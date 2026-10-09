"""Hierarchy traversal and the one cross-hierarchy check nothing else performs.

PURE AND SPARK-FREE, like invoice_rules. Both functions here decide something that is
either right or wrong regardless of where the rows came from, and a decision that can only
be tested by running a pipeline is a decision that does not get tested.
"""
from __future__ import annotations

# THE LEGAL-ENTITY FAMILY, DECLARED ONCE AND CALLED FROM BOTH SUITES.
#
# verify_repo asserts that no legal-entity binding has quietly left PLACEHOLDER. It used to
# sweep `entity.name.startswith("legal_entity")`, and on 24 September the client hierarchy
# entities were renamed to `client_legal_entity_hierarchy*` -- which does not start with
# that. Two entities dropped out of the sweep and the gate went on passing over entities it
# had stopped looking at. That was OBSERVED: repointing the renamed entity at a live table
# left the gate green.
#
# THE FIRST FIX WAS NOT ENOUGH, and review caught it. verify_repo was corrected but the
# test rebuilt the same predicate locally and compared it to a literal, so reverting
# verify_repo left BOTH suites green -- a check that tested a copy of the rule instead of
# the rule. The sweep lives here now and both callers use it, so there is one definition to
# revert and the tests see it.
#
# "contracting" IS IN THE TUPLE because lnk_client_contracting_entity is a legal-entity
# relationship whose name contains neither of the other two substrings. It was added four
# commits after the sweep was widened and landed outside it -- the same failure, twice, on
# the same branch. Anything relating legal entities to each other belongs here.
LEGAL_ENTITY_FAMILY = ("legal_entity", "consolidation", "contracting")


def in_legal_entity_family(name: str) -> bool:
    """Whether an entity name belongs to the legal-entity family the expiry gate sweeps.

    SUBSTRING, NOT PREFIX. A prefix test silently narrows the day something is renamed,
    and goes on passing -- which is exactly what happened here.
    """
    return any(key in (name or "") for key in LEGAL_ENTITY_FAMILY)


def repointed_legal_entity_bindings(entities) -> list[str]:
    """`entity/source` for every legal-entity binding that has left PLACEHOLDER.

    THE SWEEP ITSELF, not a description of it, so that a test can call the same code
    verify_repo calls. Returns names rather than a bool because a gate that says only
    "something is wrong" makes someone go and find it.

    Accepts anything with `.name` and `.sources`, so a test can hand it a probe rather
    than having to build a whole model to exercise one predicate.
    """
    return sorted(
        f"{entity.name}/{binding.name}"
        for entity in entities if in_legal_entity_family(entity.name)
        for binding in entity.sources
        if "PLACEHOLDER" not in (binding.bronze_table or ""))



# THE FOUR BINDINGS THAT ARE DELIBERATELY SOURCED, and nothing else in this family may
# leave PLACEHOLDER without appearing here first.
#
# Until 25 September 2026 the rule was simply "nothing in the legal-entity family loads":
# landing was gated on a destination nobody owned. That changed for the CLIENT half only.
# `hubspot_raw.companies` is real, present in this lake, and holds 2,457 companies with a
# 144-edge hs_parent_company_id hierarchy -- so hub_legal_entity, its profile satellite, the
# client hierarchy and that hierarchy's effectivity now have a source.
#
# THE ORGANISATION HALF IS STILL BLOCKED and must stay that way: consolidation groups and
# memberships come from Workday, tools/land_workday_references.py writes NDJSON and says
# itself that where it lands is unowned, and 01_usnc_bronze_dev holds no Workday schema at
# all. An allow-list rather than a relaxed rule, so wiring one of those up by accident still
# fails -- which is the whole reason this sweep exists.
SOURCED_LEGAL_ENTITY_BINDINGS: frozenset[str] = frozenset({
    "legal_entity/HUBSPOT",
    "legal_entity_profile/HUBSPOT",
    "client_legal_entity_hierarchy/HUBSPOT",
    "client_legal_entity_hierarchy_effectivity/HUBSPOT",
})


def unsanctioned_legal_entity_repoints(entities) -> list[str]:
    """Family bindings that have left PLACEHOLDER without being on the allow-list.

    The gate. `repointed_legal_entity_bindings` says what has moved; this says what has
    moved WITHOUT a decision behind it, which is the thing that should fail a build.
    """
    return sorted(set(repointed_legal_entity_bindings(entities))
                  - SOURCED_LEGAL_ENTITY_BINDINGS)


def unhonoured_sourced_bindings(entities) -> list[str]:
    """Allow-listed bindings that are NOT actually sourced -- the other direction.

    Without this the allow-list could describe bindings that have quietly reverted to
    PLACEHOLDER, or that were renamed, and the gate above would still pass by subtraction.
    A list that is never checked against reality stops being a decision and becomes a
    comment.
    """
    return sorted(SOURCED_LEGAL_ENTITY_BINDINGS
                  - set(repointed_legal_entity_bindings(entities)))


def _iso(value):
    """A date as a comparable ISO string, whatever shape it arrived in.

    COMPARING A date TO A str RAISES TypeError, and a TypeError inside a check condition
    ABORTS the whole suite -- strictly worse than a red check, and the failure this repo
    ranks lowest. The vault normalises its own columns, but a Spark `collect()` hands a
    caller `datetime.date` objects, and "the inputs are already normalised" was an
    assumption with no guard. Normalising once at the boundary costs nothing and removes
    the whole class.
    """
    return value if value is None or isinstance(value, str) else value.isoformat()


def overlapping_edges(group_edges, client_edges) -> set:
    """Edges present in BOTH hierarchies, which must always be empty.

    THE FAILURE NO OTHER GATE CAN SEE. Two relationship tables are each individually valid
    whatever they contain, so an edge duplicated across them breaks no constraint, joins
    perfectly, and double-counts a rollup. Append-only, reconciliation and mask-survival
    all stay green.

    WHICH TWO TABLES, CORRECTED IN REVIEW. This was written for the group hierarchy against
    the client hierarchy, and that pair CANNOT collide: consolidation groups got their own
    hub with its own key space, so one table holds pairs of consolidation_group_reference
    and the other pairs of legal_entity_code. The pair that genuinely shares a key space is
    `client_legal_entity_hierarchy` and `client_contracting_entity` -- both
    legal_entity -> legal_entity over the same authored roster.

    Concretely: ACME_GROUP is the parent of ACME_SUB in the client hierarchy AND ACME_SUB's
    contracting entity, which is entirely plausible for a client group that contracts
    centrally. A spend projection walking both relationships counts ACME_SUB's spend twice
    under ACME_GROUP. That is the comparison this function is for.

    IT HAS NO CALLER YET, and that is honest rather than hidden: neither table loads, so
    there is nothing to compare. It exists, tested, so that the gate is written before the
    data rather than after the first wrong report.

    Returns the offending edges rather than a bool, because a gate that says only "there
    is a problem" makes someone go and find it.
    """
    # set() ON BOTH SIDES, because a caller hands you whatever `collect()` returned. The
    # annotation said `set` and nothing enforced it, so a list raised TypeError -- which,
    # from inside a check condition, takes the suite with it.
    return set(group_edges) & set(client_edges)


def parent_at(edges: list[dict], child: str, when: str) -> str | None:
    """The parent in force for `child` on date `when`, or None.

    AS-WAS TRAVERSAL, and it exists because spend reporting DEFAULTS to as-is. Defaulting
    is the moment the effectivity satellite stops being queried, and an effectivity
    satellite nothing queries is one nobody notices is wrong. This is the query that keeps
    it exercised.

    Half-open intervals -- `from` inclusive, `to` exclusive -- so an edge that closes on
    the same day another opens yields exactly one parent, never two and never none. The
    day a client restructures is precisely when a naive closed interval returns both.

    `to` of None means open. Dates compare as ISO strings, which sort correctly and avoid
    importing a date parser into a function whose inputs are already normalised by the
    vault.
    """
    when = _iso(when)
    matched = []
    for edge in edges:
        if edge.get("child") != child:
            continue
        lo, hi = _iso(edge.get("from")), _iso(edge.get("to"))
        # MALFORMED BEFORE MATCHED. An edge whose interval runs backwards can never match,
        # so it used to be skipped in silence -- and a subsidiary whose only edge is
        # corrupt becomes its own reporting group, its spend leaving the parent's total
        # with no error. That is Review Focus 5 arriving through a different door.
        if lo is not None and hi is not None and lo > hi:
            raise ValueError(
                f"edge for {child!r} runs backwards: from {lo} to {hi}")
        if lo is not None and when < lo:
            continue
        if hi is not None and when >= hi:
            continue
        matched.append(edge)
    if not matched:
        return None
    # AMBIGUITY IS REFUSED, NOT RESOLVED BY LIST ORDER. "At most one open parent per child"
    # is enforced by the effectivity satellite's driving key -- at LOAD time, on data that
    # does not exist yet. Nothing constrains what this function is handed at query time, and
    # returning the first match made a spend report depend on row order: reverse the list
    # and the same client rolls up to a different group, silently.
    if len(matched) > 1:
        raise ValueError(
            f"{child!r} has {len(matched)} parents in force on {when}: "
            f"{sorted(str(e.get('parent')) for e in matched)} -- the effectivity satellite "
            f"guarantees at most one, so this data violates it and no answer is safe")
    parent = matched[0].get("parent")
    # A MATCHED EDGE THAT NAMES NO PARENT IS CORRUPT, NOT AN ABSENCE. rollup_key falls back
    # to the child's own id when there is no parent EDGE; it must not do so because an edge
    # exists whose parent column is null, which is a different thing and means the load is
    # wrong.
    if parent is None or (isinstance(parent, str) and not parent.strip()):
        raise ValueError(
            f"the edge in force for {child!r} on {when} names no parent -- an absent "
            f"parent column is corruption, not a client without a parent")
    return parent


def rollup_key(edges: list[dict], child: str, when: str) -> str:
    """The id a child's spend rolls up to on `when` -- its parent, or itself.

    THE NULL-PARENT TRAP, AND IT IS WHY THIS IS A FUNCTION RATHER THAN A JOIN. Amy Keser,
    24 September: parent/child is "not applicable to all (Ameren as an example)", and the
    parent id is "the key for reporting on total spend across multiple records". Both are
    true at once, so most clients have no parent and the reporting key is the parent.

    A projection that simply groups by parent id therefore DROPS every single-entity
    client, in silence: the join succeeds, no row is malformed, and the report is merely
    smaller. Ameren's entire invoiced value would be absent from client-group spend.

    Falling back to the child's own id makes a single-entity client a group of one, which
    is what it is. Delegates to parent_at so the as-of date cannot drift apart from the
    rest of the traversal.
    """
    return parent_at(edges, child, when) or child
