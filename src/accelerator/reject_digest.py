"""Which columns constitute a rejected row's content, and how to digest them.

Separate from audit.py deliberately: that renders INSERT statements for the load audit,
this decides what a row's content IS. One file, one reason to change.

THE COLUMN SET IS NOT THE PAYLOAD. Measured against the model, lnk_client_job_request has
zero payload and zero transaction key -- a link is pure structure, its content IS its
parent hash keys -- and hashing.hashdiff() raises RulebookError on an empty column list. A
payload-only rule would therefore raise for that one entity and no other, which is the
worst shape of defect: correct on five, undefined on the sixth.

So the set is every DECLARED column of the projected row except the system columns.
load_dts and batch_id necessarily differ between the rejected load and the corrected one
-- load_dts is when WE learned it -- so a digest including them could never match.

HASH KEYS ARE DIGESTED AS LOWERCASE HEX. BINARY_OUTPUT is RATIFIED True, so every hash key
is stored BINARY(32), while hashing.normalise() is defined over strings. A BINARY column's
behaviour inside a string concatenation is not a property to rely on for a comparison that
must be exact. For the pure-structure link the keys are the ONLY digest input, so this is
load-bearing there and merely tidy everywhere else.

This module CALLS hashing.hashdiff() and never reimplements it. hashing.py is a RATIFIED
rulebook: a change there re-keys the estate.

This module is pyspark-free at IMPORT time, not at CALL time: factory.py imports pyspark
at module level, so the `from .factory import _projection` below is deliberately kept
inside digest_columns() rather than hoisted to the top of this file. tests/test_accelerator.py
imports this module before it installs its pyspark stub, and a module-level import here
would abort the whole suite.
"""

from __future__ import annotations

from collections.abc import Sequence

from . import naming
from .hashing import RULEBOOK_VERSION, hashdiff

__all__ = ["digest_columns", "hex_columns", "digest_sql", "RULEBOOK_VERSION"]


def digest_columns(entity, src) -> list[str]:
    """The projected row's declared columns, in order, minus the system columns.

    `src` MAY BE None, AND IN PRODUCTION ALWAYS IS. Entity.tables() returns
    `[(None, base_table)]` for every one-table-per-entity kind, and nhl, link and hal
    are all such kinds -- identity and relationships are conformed concepts, one table
    fed by one append flow per source, not one table per source like sat/msat. Both
    callers of this module (checks/supersede_quarantine.py and its targets()) drive from
    entity.tables(), so `src is None` for EVERY reconcilable entity, and
    factory._projection reads `src.payload or entity.payload` for an nhl. Without the
    fallback below that raised AttributeError for all five NHLs -- the digest path dead
    for five of the six reconcilable entities, including all three the job reconciles,
    while only the pure-structure link survived because its projection never reaches
    src.payload.

    Same fallback, same reason, as checks/load_satellites.py's `proj_src` (csat is
    likewise one table fed by its single declared source). It lives HERE, in the one
    module that owns "what a row's content is", rather than at each call site: every
    caller needs it, and a scattered fallback is one call site away from being missed
    again.

    spec.validate refuses a keyed non-staged kind with more than one binding and refuses
    any entity with none, so `entity.sources[0]` is well defined for exactly these kinds.
    The empty case is still refused loudly rather than allowed to return an empty column
    set: an empty set would surface as hashing.hashdiff() raising RulebookError from
    digest_sql(), a message pointing at the ratified rulebook instead of at the entity
    with no binding.
    """
    from .factory import _projection

    proj_src = src if src is not None else (entity.sources[0] if entity.sources
                                            else None)
    if proj_src is None:
        raise ValueError(
            f"{entity.base_table}: entity.tables() supplies no source binding for this "
            f"kind and the entity declares none either, so there is nothing to project "
            f"a row's content from. spec.validate requires at least one binding -- this "
            f"model was not validated, or the entity was built by hand."
        )
    system = set(naming.SYSTEM_COLUMNS)
    return [c for c, _kind, _value in _projection(entity, proj_src) if c not in system]


def projected_columns(entity, src=None) -> list[str]:
    """Every column the vault table carries, in declared order, system columns included.

    digest_columns' sibling: same projection, nothing removed. A caller comparing a DEPLOYED
    table against the model needs the whole shape and the ORDER, because a loader inserting
    by position is broken by a reordering exactly as surely as by a missing column --
    measured 25 September, when hub_organisation's system columns sat where its business
    keys belong and 'Fieldglass_Buyer_Code' was inserted into load_dts.

    HERE RATHER THAN IN THE CALLER for the reason stated at the top of this module: importing
    accelerator.factory pulls in pyspark.pipelines, which dies at the import hook outside a
    DLT pipeline, so the import is function-local and lives in ONE place that job tasks may
    import freely.
    """
    from .factory import _projection

    proj_src = src if src is not None else (entity.sources[0] if entity.sources else None)
    if proj_src is None:
        raise ValueError(
            f"{entity.base_table}: no source binding to project from")
    return [c for c, _kind, _value in _projection(entity, proj_src)]


def hex_columns(columns: Sequence[str]) -> list[str]:
    """Those of `columns` that are hash keys, and so must be rendered as hex first.

    Keyed on the `_hk` suffix, which naming.hk() guarantees for every hash key in the
    model. A column that merely ends that way and is not BINARY would be hex-rendered
    harmlessly -- hex() of a string is defined -- so the failure mode of a false positive
    here is a stable digest, not a wrong one.
    """
    return [c for c in columns if c.endswith("_hk")]


def digest_sql(entity, src) -> str:
    """The SQL expression digesting one row's content.

    Returns an expression over columns ALREADY hex-rendered under their own names. The
    caller is responsible for that rendering -- see checks/supersede_quarantine.py -- so
    that hashdiff() is called with plain column names and never with an injected
    expression. Passing `hex(x_hk)` into a ratified function as though it were a column
    name would make the rulebook's normalisation operate on something it never saw.
    """
    columns = digest_columns(entity, src)
    if not columns:
        raise ValueError(
            f"{entity.base_table}: no declared non-system column to digest. Every "
            f"reconcilable entity has at least its parent hash keys, so this means the "
            f"projection returned nothing -- a model or projection defect, not a "
            f"digestable row."
        )
    return hashdiff(columns)
