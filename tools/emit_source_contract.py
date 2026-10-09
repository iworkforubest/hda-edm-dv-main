"""The source contract: what Silver requires of each Bronze table it reads.

A CONSUMER-SIDE contract. data_contracts/ describes what the vault produces; this describes
what it depends on, generated from the same authority, so Bronze can only break us knowingly.

We do not model Bronze and are not proposing to. What is modelled -- completely, and already
gated -- is our DEPENDENCY on Bronze: every SourceBinding names its bronze_table and the
columns Silver reads from it.

PURE, AND OFFLINE BY DESIGN. No Spark, no workspace, no clock. The artefact is committed and
verify_repo asserts regeneration is a no-op, which is the only reason to trust it -- reading
anything from a live lake at emit time would destroy that property.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

from accelerator import spec  # noqa: E402

OUTPUT_DIR = ROOT / "source_contracts"


def binding_requirements(entity: spec.Entity, src: spec.SourceBinding) -> dict:
    """What Silver requires of one Bronze table, through one binding, grouped by ROLE.

    Role rather than a flat column list, because the consequences differ: dropping a payload
    column degrades one target, dropping a key column breaks joins across the whole lake.

    transaction_key is read from the ENTITY -- it is declared there, not on the binding.
    """
    parent_cols: set[str] = set()
    for _parent, cols in src.parent_keys:
        parent_cols.update(cols)

    # A DERIVED COLUMN IS NOT SOMETHING BRONZE PROVIDES. A conformed binding computes its
    # profile's columns in the flow -- twelve renames for the invoice profile -- so naming
    # them here would publish a dependency on columns the source has never had and can
    # never grow. Measured 25 September: the first run after the profiles moved out of
    # views reported eight of them "not present" and stopped the load, correctly by its own
    # rule and wrongly about the world.
    #
    # WHAT THE SOURCE STILL OWES IS THE EXPRESSIONS' INPUTS, and those are covered: the
    # profile's required_columns are asserted by apply_source_unions against every matched
    # table before any flow runs, and a missing one fails there by name.
    _derived = {c for c, _e in (src.derived_columns or ())}

    def _needed(cols):
        return sorted(set(cols) - _derived)

    return {
        "business_keys": _needed(src.key_columns),
        "parent_keys": _needed(parent_cols),
        "transaction_key": _needed(entity.transaction_key or ()),
        "payload": _needed(src.payload),
        "applied_dts": (None if src.applied_dts_column in _derived
                        else src.applied_dts_column),
        "cdc_op": src.cdc_op_column,
        "manifest": src.manifest_column,
        "dedup_by": _needed(src.dedup_by),
        "required_casts": {col: typ for col, typ in src.cast if col not in _derived},
        # DECLARED, NOT REQUIRED. dedup_order is not consumed by the streaming path
        # (factory.py:367: "DECLARED but not applied" -- a partitioned ranking window is not
        # streaming-legal), and input_file_name, which appears in most declarations, is Spark's
        # file-metadata function rather than a Bronze column. Requiring either would publish a
        # dependency we do not have. Carried so the day a batch latest-wins path lands, the
        # intent is already written down.
        "declared_not_required": {"dedup_order": list(src.dedup_order)},
    }


# THE BINDINGS THAT ARE NOT BRONZE. Measured 28 Aug: job_request_custom_promoted,
# payroll_line_classification and organisation each declare a BUSINESS_VAULT source whose
# `bronze_table` is hfig_*.raw_vault.* -- our own vault, feeding a business-vault entity. The
# field is named bronze_table because that is what it is for every other binding. Publishing
# these would name tables the Bronze team does not own.
#
# Excluded by NAME, and tests/test_accelerator.py asserts the name and the raw_vault path pick
# out the same bindings, in both directions -- so a future divergence fails there rather
# than quietly changing what is published.
NOT_BRONZE_SOURCE = "BUSINESS_VAULT"


def is_business_vault_binding(name: str) -> bool:
    """True for NOT_BRONZE_SOURCE itself, or a BUSINESS_VAULT_<suffix> variant.

    ONE NAME, ONE key_literals VALUE. spec.parent_key_components matches a
    literal-keyed hub's binding to a link/NHL/satellite's parent key by an EXACT NAME
    match (see its docstring). hub_organisation is authored and reached with a
    key_literals reference_type, so a business-vault satellite recomputing an
    organisation FK must be named identically to whichever of hub_organisation's own
    bindings carries the reference_type it needs. csat_payroll_line_classification
    already claims the bare 'BUSINESS_VAULT' name for Organization_Reference_ID
    (UKG); csat_invoice_line_gie (Task 5) needs Fieldglass_Buyer_Code instead, off
    the SAME hub, and one binding cannot carry two literal values under one name --
    so it is named 'BUSINESS_VAULT_GIE', the same suffix convention GP_US_HIST
    already uses for "the same family, a different binding".

    Matched by PREFIX rather than exact equality so this family can keep growing the
    same way, without this contract starting to publish a raw_vault table by
    accident every time a new suffix is added.
    """
    return name == NOT_BRONZE_SOURCE or name.startswith(NOT_BRONZE_SOURCE + "_")


def union_view_suffixes() -> set[str]:
    """`.<schema>.v_<name>` for every union view WE build, from the one declaration.

    THE SAME REASON AS NOT_BRONZE_SOURCE, one step along. This contract is the document the
    Bronze team is handed: it says what Silver requires OF THEM. A union view built by
    checks/apply_source_unions.py lives in our catalog and is created by our job, so naming
    it here would hand them a requirement about an object they neither own nor can change.
    verify_repo says so directly -- "a contract naming another lake's catalog cannot be
    acted on by the team it is handed to".

    Read from metadata/source_unions.yml rather than hardcoded, so the schema name has one
    authority shared with the task that creates the view and the gate that recognises it.

    KNOWN GAP, STATED RATHER THAN HIDDEN. Excluding the view means this contract expresses
    NOTHING about the 30 per-tenant Fieldglass tables underneath it -- and those ARE Bronze's,
    and breaking a column in one of them would break the union silently. What belongs here is
    a requirement against the PATTERN (io_distributed_jobposting_%), which this contract has
    no way to express yet. Recorded in OPEN_ITEMS; not solved by pretending the view is a
    Bronze table.
    """
    import yaml as _y

    decl = _y.safe_load(
        (ROOT / "metadata" / "source_unions.yml").read_text(encoding="utf-8")) or {}
    # A PROFILE WITHOUT `view_schema` NEVER BECOMES A VIEW, so it contributes no suffix.
    # That is the vault-sourced profile DEF-58 adds (fieldglass_us_invoice_gie): its input
    # is nhl_invoice_line, a table this pipeline builds, and checks/apply_source_unions.py
    # creates nothing for it. Reading the key unconditionally is a KeyError that aborts
    # whatever imports this -- including the offline suite, which is how it was found.
    return {f".{u['view_schema']}.v_{u['name']}" for u in (decl.get("unions") or [])
            if u.get("view_schema")}

_MERGEABLE_LIST_ROLES = ("business_keys", "parent_keys", "transaction_key", "payload",
                         "dedup_by")
_SINGLE_ROLES = ("applied_dts", "cdc_op", "manifest")


def bronze_tables(model: spec.Model, active) -> dict:
    """Every Bronze table an ACTIVE binding reads, with the union of what is required of it.

    Keyed by fully-qualified table. Several bindings legitimately read one table -- ukg_raw.gl
    is read by five -- so roles are UNIONED, never overwritten.
    """
    out: dict = {}
    for entity in model.entities:
        for src in entity.sources:
            if is_business_vault_binding(src.name):
                continue
            if any(src.bronze_table.endswith(v) for v in union_view_suffixes()):
                continue
            if not spec.active_table_bindings(entity, src, active):
                continue
            req = binding_requirements(entity, src)
            slot = out.setdefault(
                src.bronze_table,
                {"read_by": [], "requires": {r: set() for r in _MERGEABLE_LIST_ROLES},
                 "declared_not_required": {"dedup_order": set()}},
            )
            slot["read_by"].append(f"{entity.name}/{src.name}")
            for role in _MERGEABLE_LIST_ROLES:
                slot["requires"][role].update(req[role])
            # TASK 3, NORMALISED SHAPE: a single role is initialised to an EMPTY set the
            # first time any binding on this table is processed, exactly like the
            # mergeable list roles above, whether or not this particular binding needs it.
            # Before this, an absent role was simply MISSING from `requires`, while a
            # present one was a list -- one class of field (business_keys, ..., applied_dts)
            # rendering as two different YAML shapes depending on which table you looked
            # at. Every role is now always a key, always a list, empty when nothing
            # requires it -- a consumer never has to branch on whether the key exists.
            for role in _SINGLE_ROLES:
                slot["requires"].setdefault(role, set())
                if req[role]:
                    slot["requires"][role].add(req[role])
            # RULING T2-B: two bindings casting the SAME column to different types is a
            # CONJUNCTION, not a contradiction -- Bronze must supply a column castable to
            # both, and that is what the contract should say. A plain dict.update here would
            # let the later binding's type silently erase the earlier one's, the identical
            # silent-overwrite shape the union check over roles exists to catch, one field
            # over and unguarded. So required_casts accumulates a SET of types per column,
            # same shape as _SINGLE_ROLES, and is sorted to a list in the pass below.
            _casts = slot["requires"].setdefault("required_casts", {})
            for col, typ in req["required_casts"].items():
                _casts.setdefault(col, set()).add(typ)
            # DECLARED_NOT_REQUIRED IS A SIBLING OF requires, NOT A MEMBER OF IT. Nesting
            # it inside `requires` (as an earlier version of this emitter did) put a
            # not-required column one flatten() away from being read as required: a
            # reader or script that walks every column under `requires` picks up
            # dedup_order/input_file_name as if Bronze had to guarantee it -- the exact
            # dependency this field exists to DISOWN. See LIMITS["declared_not_required"]
            # for the one-sentence explanation carried in the artefact itself.
            slot["declared_not_required"]["dedup_order"].update(
                req["declared_not_required"]["dedup_order"])

    # sets are for merging; lists are what YAML should carry, sorted so the artefact is stable
    for slot in out.values():
        slot["read_by"] = sorted(slot["read_by"])
        req = slot["requires"]
        for role, value in list(req.items()):
            if isinstance(value, set):
                req[role] = sorted(value)
        req["required_casts"] = {
            col: sorted(types) for col, types in req["required_casts"].items()
        }
        slot["declared_not_required"]["dedup_order"] = sorted(
            slot["declared_not_required"]["dedup_order"])
    return out


from emit_data_contract import targets_and_variables  # noqa: E402

# REUSED, NOT RESTATED. targets_and_variables() resolves a target's variables the way the
# bundle does, and spec.active_table_bindings is "THE definition of an inactive table" already
# relied on by three gates. A second statement of either is the duplicate-definition trap this
# repo has been bitten by twice (BUSINESS_KINDS, the system-column set).

LIMITS = {
    "types_are_requirements": (
        "Every type here is a type Silver REQUIRES, never an observation about what Bronze "
        "holds. Bronze's own column types are not modelled in this repository, so this "
        "document cannot and does not describe them."
    ),
    "expectation_columns_not_listed": (
        "This lists the columns the MODEL reads, plus the two compiled-in key-safety rules. "
        "Governed data-quality rules live in control.ref_dq_expectation and their SQL is read "
        "at pipeline runtime, where it may name ANY source column. So a change to a column "
        "only a governed rule references can break a load this document never mentioned. "
        "Reading those rules here would make a committed, offline-reproducible artefact "
        "depend on workspace state, so the gap belongs to a live check instead."
    ),
    "declared_not_required": (
        "Each table's declared_not_required (a sibling of requires, not a member of it) "
        "lists columns the model records against a binding on that table without asking "
        "Bronze to guarantee them -- today only dedup_order, which is declared per "
        "binding but not applied by the streaming path, and may include Spark's own "
        "file-metadata column rather than anything Bronze holds."
    ),
}

STANDING = (
    "This is Silver's stated dependency on Bronze, generated from "
    "metadata/entities/*.yml. It is NOT an agreement Bronze has countersigned."
)

# TASK 3 DECISION, DEFERRED MINOR 1 (association of a disagreeing single role to the binding
# that needs it). bronze_tables() unions applied_dts/cdc_op/manifest across every active
# binding on a table, so two bindings wanting two different values both survive -- but the
# merged structure cannot say WHICH of `read_by` wants which; it can only list every value
# asked for on that table. Restoring the association would mean nesting a per-binding
# breakdown inside every table (duplicating binding_requirements() per read_by entry, right
# back into the shape Task 2 deliberately merged away) or keying the union by binding, which
# is a materially different, more invasive artefact than the one this brief specifies.
# Checked directly against the live model (28 Aug): zero active bindings disagree with
# another active binding on the same bronze table for any of these three roles -- every
# present role-list here today has exactly one element. So the gap is DOCUMENTED, not
# silently dropped, and is latent rather than actual as of this generation. A future
# binding that does disagree will show up as more than one entry in the list, and a reader
# who needs to know which binding wants which must cross-reference read_by against
# metadata/entities/ by hand -- this artefact does not do that cross-referencing for them.
MERGE_NOTE = (
    "Where several read_by bindings on one table disagree about applied_dts, cdc_op or "
    "manifest, every distinct value required is listed here, but NOT attributed to which "
    "binding needs which -- the merge unions VALUES, not bindings. As generated, no two "
    "active bindings on the same table disagree on any of these three roles (every "
    "populated list below has exactly one entry); if that ever changes, cross-reference "
    "read_by against metadata/entities/*.yml to find which binding wants which value."
)


def contract_path(target: str) -> Path:
    return OUTPUT_DIR / f"{target}.yaml"


def emit(model: spec.Model, target: str, variables: dict) -> dict:
    """The contract structure for one target. Pure: same inputs, same dict, no I/O, no clock."""
    active = spec.resolve_active_sources(model, variables.get("active_sources"))
    return {
        "target": target,
        "standing": STANDING,
        "generated_from": "metadata/entities/*.yml",
        "limits": dict(LIMITS),
        "merge_note": MERGE_NOTE,
        "bronze_tables": bronze_tables(model, active),
    }


def render(structure: dict) -> str:
    """The YAML text for one contract. sort_keys=True so the diff is reviewable, and
    allow_unicode=True so an em dash is a character rather than an escape."""
    return yaml.safe_dump(structure, sort_keys=True, default_flow_style=False,
                          allow_unicode=True)


def configured_targets() -> list:
    """[(target, variables)] for the targets that DECLARE what they load.

    A target with no `active_sources` has not said which bindings are real in its lake, and
    spec.resolve_active_sources treats that as "every binding is active" -- correct for the
    pipeline, which then creates every declared table empty, and wrong for a contract, which
    would name every table in the model including the placeholders.

    MEASURED 28 Aug 2026, and the split is total. The two targets that declare active_sources
    (dev, usnc_tds) resolve to five bronze tables, ALL FIVE in their own bronze catalog. The
    seven that declare nothing resolved to nineteen, of which ZERO were in their own catalog:
    fourteen placeholders that DEPLOY.md:236 says "will not exist", plus five
    01_usnc_bronze_dev tables that were being offered to WEU, UKS and AUE readers as their own
    stated requirement.

    So no contract is written for a lake that has not declared its Bronze. An EMPTY contract
    would be worse than no file: it reads as "Silver requires nothing of you", when the truth is
    that this lake has no Bronze yet. A target gets a contract the day it declares one.
    """
    return [(t, v) for t, v in targets_and_variables() if v.get("active_sources")]


def main() -> None:
    model = spec.load_model(ROOT / "metadata" / "entities")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for target, variables in configured_targets():
        contract_path(target).write_text(render(emit(model, target, variables)),
                                         encoding="utf-8")
        print(f"wrote {contract_path(target).relative_to(ROOT)}")


if __name__ == "__main__":
    main()
