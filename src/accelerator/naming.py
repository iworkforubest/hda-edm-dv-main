"""
Naming and technical-column standard.

Every generated object's name and every system column comes from here, so that a
rename is one edit rather than a search-and-replace across four regions.

RECONCILE BEFORE FIRST LOAD: the column names below are the platform default as
evidenced in the spike documents and EDM v0.4 ERD. They must be checked against
TECH_COLUMNS_STANDARD in the sample-data workbook. Where the ERD and the spike
disagree (ingested_at vs LOAD_DATETIME vs ldts; record_hash vs hashdiff), the
workbook wins and this file changes once.
"""

from __future__ import annotations

import re

# --------------------------------------------------------------------------- #
# Version suffixing
# --------------------------------------------------------------------------- #
_VERSION_SUFFIX = re.compile(r"^(?P<base>.+)_rev(?P<version>[1-9][0-9]*)$")


def physical(base: str, version: int) -> str:
    """The physical table name for one version of a vault object.

    VERSION 1 IS SUFFIXED LIKE EVERY OTHER VERSION, and that is the whole point. If v1
    were unsuffixed, `hub_invoice` would be a TABLE until somebody declared a v2 and a
    VIEW afterwards -- so every consumer, every gate and every loader would have to know
    which regime it was in. One rule: the unversioned name is ALWAYS the view.

    The version suffix is OUTERMOST. A raw satellite is one table per source
    (sat_invoice_header_fieldglass_us), and the version qualifies that whole table.

    THE SUFFIX IS `_rev<N>` AND NOT `_v<N>`, AND THAT IS NOT A STYLE CHOICE.
    `_v1` IS ALREADY TAKEN: naming.V1_SUFFIX is "_v1" and naming.v1_view() returns
    f"{table}_v1", the derived type-2 view over a satellite (DEF-52, DEF-53). Seven
    such views are live in the lake. A physical table under that name would collide.
    """
    return f"{base}_rev{version}"


def stable(physical_name: str) -> str:
    """The view name for a physical table -- the exact inverse of physical().

    RAISES on a name carrying no version suffix rather than returning it unchanged.
    An unmigrated table passed here is a bug in the caller, and returning the input
    would let it read as a finished migration.
    """
    m = _VERSION_SUFFIX.match(physical_name)
    if not m:
        raise ValueError(
            f"{physical_name!r} carries no _rev<N> suffix, so it is not a physical "
            f"vault table name. Either it predates the versioning migration, or a "
            f"caller passed a view name where a table name was required.")
    return m.group("base")


# --------------------------------------------------------------------------- #
# Object prefixes. The prefix is load-bearing: the factory, the append-only
# check and the conformance gate all key off it.
# --------------------------------------------------------------------------- #
PREFIX = {
    "hub": "hub_",
    "link": "lnk_",
    "nhl": "nhl_",          # non-historized (transactional) link
    "hal": "hal_",          # hierarchical link
    "sal": "sal_",          # same-as link
    "sat": "sat_",
    "msat": "msat_",        # multi-active satellite
    "esat": "esat_",        # effectivity satellite
    "rsat": "rsat_",        # record-tracking satellite
    "ssat": "ssat_",        # status-tracking satellite
    "csat": "csat_",        # computed (business vault) satellite
    "pit": "pit_",
    "bridge": "br_",
    "staging": "stg_",
    "quarantine": "qtn_",
    "ghost": "ghost_",
}

# Object types this accelerator is permitted to generate. Anything else is
# platform-owned per the ARB boundary rule and is refused at validation time.
GENERATABLE = ("hub", "link", "nhl", "hal", "sat", "msat", "esat", "csat")

# Platform-owned prefixes. The generator reads these by join and never creates them.
PLATFORM_OWNED = ("ctl_", "ref_", "reg_", "agg_", "doc_")

# The kinds computed FROM the Raw Vault rather than loaded into it. An SDP pipeline
# targets one schema, so the two layers are separate pipelines writing to separate
# schemas, and this set is what divides them.
#
# It lives here, not in the pipeline module, because two consumers need it and one of
# them must run without Spark: src/pipelines/silver_vault.py selects the entities to
# build, and checks/apply_governance.py decides which schema a table's GRANT names. A
# second copy would let a new business kind be granted against raw_vault -- a grant on
# a table that does not exist there, which fails the whole governance run.
BUSINESS_KINDS = frozenset({"csat"})     # + pit, bridge when sub-project 5 lands

# Derived views: type-2 end-dating is computed, never stored.
V1_SUFFIX = "_v1"

# --------------------------------------------------------------------------- #
# Technical columns
# --------------------------------------------------------------------------- #
COL = {
    # identity
    "hk": "{entity}_hk",           # hash key of this object
    "bk": "{entity}_bk",           # business key, human readable, never joined on
    # bitemporality -- two clocks, both mandatory from day one
    "load_dts": "load_dts",        # when WE learned it. Set by the load, monotonic.
    "applied_dts": "applied_dts",  # when it happened in the source. May arrive late.
    "sub_seq": "sub_seq",          # intra-batch ordering when load_dts ties
    # provenance
    "rec_src": "rec_src",          # metadata, NOT identity. Never join on this.
    "batch_id": "batch_id",        # -> hub_load_run in the metrics vault
    "manifest_id": "manifest_id",  # -> ctl_ delivery manifest, loop-1 reconciliation
    "cdc_op": "cdc_op",            # I | U | D as delivered by Bronze
    # change detection
    "hashdiff": "hashdiff",
    # quarantine
    "failure_rule": "failure_rule",
    "failure_detail": "failure_detail",
    # multi-active
    "mas_key": "mas_key",
}

# WHAT EACH TECHNICAL COLUMN MEANS, in a form something other than a human can read.
#
# These sentences are not new. Every one of them was already written as an inline comment
# beside its entry in COL above -- "when WE learned it", "metadata, NOT identity" -- where
# a person editing this file sees it and no generated artefact ever could. Measured 5 Sep
# 2026: 0 of 409 emitted columns carried a description into the data contract, the diagram
# or the ontology, while 175 of them were system columns whose meaning was sitting six
# lines up in this module.
#
# So this is a promotion, not an invention. Where a sentence here and a comment above ever
# disagree, the comment is the older text and this is what ships -- keep them in step by
# editing both, which is cheap because they are adjacent.
#
# THE TEST FOR A GOOD ONE IS THAT IT SAYS SOMETHING THE NAME DOES NOT. "load_dts: the load
# timestamp" restates the column and reads as documentation, which is worse than silence.
# verify_repo refuses a description whose words are contained in its column's own name.
COLUMN_DOC: dict[str, str] = {
    COL["load_dts"]:
        "When WE learned this row, set by the load and monotonic within a run. Not when it "
        "happened in the source -- that is applied_dts.",
    COL["applied_dts"]:
        "When this row's change happened in the SOURCE. May arrive late, and may be NULL "
        "where a feed ships no business clock. Type-2 end-dating runs on this, never on "
        "load_dts.",
    COL["sub_seq"]:
        "Intra-batch ordering, used only to break a tie when two rows share a load_dts. "
        "Carries no business meaning and is never a business key.",
    COL["rec_src"]:
        "Which feed delivered this row. Provenance, NOT identity -- never join on it. The "
        "source name is already hashed into a federated key, so joining on both would be "
        "asserting the same thing twice.",
    COL["batch_id"]:
        "The load run that wrote this row, for tracing a value back to the run that "
        "produced it.",
    COL["manifest_id"]:
        "The delivery manifest this row was approved under. Loop-1 reconciliation compares "
        "what a manifest approved against what landed and what was quarantined.",
    COL["cdc_op"]:
        "The change operation Bronze delivered: I, U or D. What the SOURCE said happened, "
        "not what this vault did with it.",
    COL["hashdiff"]:
        "SHA-256 over this satellite's payload. Change detection: a new version is written "
        "only when this differs from the row before it, so an unchanged re-delivery costs "
        "nothing.",
    COL["failure_rule"]:
        "Which rule rejected this row into the quarantine twin.",
    COL["failure_detail"]:
        "The offending value or condition, kept verbatim so a reject can be diagnosed "
        "without re-running the load.",
    COL["mas_key"]:
        "The sub-key that makes a multi-active satellite's rows distinct within one parent "
        "and one load -- a skill code or phone type, in preference to a generated sequence.",
}


# Columns present on every generated vault table, in this order.
#
# DEF-25: cdc_op belongs here and was missing. factory._system_columns writes it onto
# every row of every table, but this inventory omitted it and so did the ghost flow --
# which stayed invisible for as long as table schemas were INFERRED, because the union of
# "the source flow supplies it" and "the ghost flow does not" is just a nullable column.
# The moment a masked table declares its schema (DEF-19) the omission is fatal:
# _system_columns builds cdc_op from a non-nullable literal, so the ghost flow fails with
# DELTA_MISSING_NOT_NULL_COLUMN_VALUE. The inventory now matches what is actually written.
SYSTEM_COLUMNS = (
    COL["load_dts"],
    COL["applied_dts"],
    COL["sub_seq"],
    COL["rec_src"],
    COL["batch_id"],
    COL["manifest_id"],
    COL["cdc_op"],
)

# The system columns' types, declared because nothing else declares them. The contract
# emitter runs OFFLINE -- factory._derived_schema_ddl reads the staged frame and needs
# Spark -- so without this the emitter would have to guess a timestamp's type or omit it,
# and a contract that types load_dts as STRING misleads every consumer that reads it.
#
# Six of these have types fixed by _system_columns() in factory.py — each through a literal,
# an explicit cast, a function with fixed return type (load_dts: current_timestamp, cdc_op:
# upper), or a source column with explicit cast (applied_dts, batch_id). Two of the six
# (applied_dts and cdc_op) read their value from a source column, but their type does not
# depend on it and is knowable without a workspace. If that function's output types ever
# change, this must change with it -- the suite asserts the two sets of NAMES agree, which
# catches a column added or removed but not a type silently altered.
#
# manifest_id is the exception: it is the only one assigned a bare F.col(...) with no cast,
# so when a binding declares a manifest column, its type is whatever Bronze supplies, passing
# straight through. Only the fallback F.lit(None).cast("string") has a fixed type. Declare it
# STRING -- every manifest id in this estate is one, and the fallback is cast that way -- but
# nothing verifies the assumption. If a source ever supplies a non-STRING manifest column, the
# emitted contract will state STRING and the suite will not notice.
SYSTEM_COLUMN_TYPES: dict[str, str] = {
    COL["load_dts"]:    "TIMESTAMP",
    COL["applied_dts"]: "TIMESTAMP",
    COL["sub_seq"]:     "INT",
    COL["rec_src"]:     "STRING",
    COL["batch_id"]:    "STRING",
    COL["manifest_id"]: "STRING",
    COL["cdc_op"]:      "STRING",
}


def hk(entity: str, role: str = "") -> str:
    """The hash-key column for `entity`, optionally under a role.

    A ROLE PREFIXES THE COLUMN, and it exists for one shape: a link whose parents are not
    all distinct hubs. `hk("legal_entity")` is `legal_entity_hk` for every ordinary
    parent; `hk("legal_entity", "parent")` is `parent_legal_entity_hk`, so the two legs of
    a hierarchical link occupy two columns instead of silently sharing one.

    The role is NOT a free-text label. spec.validate rejects a role that is not a plain
    identifier, because this string reaches a generated column name.
    """
    column = COL["hk"].format(entity=entity)
    return f"{role}_{column}" if role else column


def bk(entity: str) -> str:
    return COL["bk"].format(entity=entity)


def qualified(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def v1_view(table: str) -> str:
    return f"{table}{V1_SUFFIX}"


def is_platform_owned(table: str) -> bool:
    return table.lower().startswith(PLATFORM_OWNED)


# DEF-41: the system columns a KEYED table does not carry.
#
# A hub, link or NHL asserts that a key or a relationship exists. It has no versions, so
# there is nothing for sub_seq to order; and there is no update or delete to record, so
# cdc_op has nothing to say. Both were written to every such table anyway, and both were
# measured on 25 Aug 2026 to carry exactly ONE distinct value across 2,759,294 hub rows
# and 2,453,132 NHL rows -- sub_seq always 0, cdc_op always 'I'.
#
# Nothing reads them there either: checks/append_only_check.py takes the uniqueness grain
# for hub_/lnk_/nhl_/hal_ as [own_hk] alone, and only satellites filter on cdc_op
# (factory._register_source_flows sets changed_only for sat/msat/csat).
#
# Satellites keep both: sub_seq is part of their uniqueness grain, and cdc_op is what
# `change_detection: cdc` filters on.
#
# NOT DROPPED, and each for a measured reason: applied_dts carries 447,058 distinct
# source-effective dates and is fully populated; manifest_id is entirely NULL today but
# is what checks/loop1_reconciliation.py counts against once the control tables exist;
# batch_id is lineage no gate reads, kept deliberately so a bad load can be traced to
# the update that wrote it.
KEYED_KINDS = frozenset({"hub", "link", "nhl", "hal"})

# THE LINK-LIKE KINDS, DECLARED ONCE. KEYED_KINDS is these plus `hub`, and the difference
# matters in about twenty places -- a hub has business keys, a link has parents.
#
# Measured 5 Sep 2026: `("link", "nhl", "hal")` was typed out by hand in 22 places across
# verify_repo, the offline suite, spec, factory, render_erd and estimate_footprint, with
# nothing to read. Adding a kind therefore means finding all 22, and missing one is
# SILENT -- which is exactly how `hal` came to be a supported kind that had never been
# built and produced a broken hierarchy the first time it was.
#
# A same-as link (`sal`) is the next likely arrival: it is already in PREFIX, and
# docs/legal_entity_design.md recommends one for resolving observed `buyer` strings.
# Adding it means this line and GENERATABLE, both findable, instead of a hunt.
LINK_KINDS = frozenset({"link", "nhl", "hal"})
_KEYED_OMITS = (COL["sub_seq"], COL["cdc_op"])

# The prefixes that mark a table as VAULT-resident rather than bronze. A table with one
# of these already carries the vault system columns -- applied_dts among them -- so a
# binding that reads one does not have to name an applied_dts_column of its own. Derived
# from PREFIX so a new kind cannot be added without landing here too.
# EVERY PREFIX THIS REPO PUTS ON A TABLE IT CREATES, and the answer to "is this one of
# ours" for any gate sweeping a schema. Distinct from VAULT_TABLE_PREFIXES below, which
# answers a narrower question -- does a BINDING read a vault table -- and so leaves out
# the staging logs and quarantine twins that are unquestionably ours.
#
# Measured 5 Sep 2026: three authorities disagreed about this. This module derived 13
# prefixes; append_only_check.py hand-typed 10; conformance_check.py hand-typed 8. The
# derived one was the one nobody read. append_only's list had already been patched twice
# after the fact -- DEF-42 added `stg_`, and 30 August added `qtn_` after nine quarantine
# twins went unchecked by the one gate that notices a vault table shrinking -- and it was
# still missing five. A `sal_` table could have been UPDATEd with that gate never looking
# at it.
#
# `ghost` is the only exclusion and it is not a table prefix at all: ghost records are
# ROWS inside vault tables, written at load_dts 1900-01-01.
_NOT_A_TABLE_PREFIX = frozenset({"ghost"})
GENERATED_TABLE_PREFIXES = tuple(sorted(
    (PREFIX[k] for k in PREFIX if k not in _NOT_A_TABLE_PREFIX), key=len, reverse=True))

# THE SUBSET CROSS-REGION CONFORMANCE MAY COMPARE, and it is deliberately narrower than
# GENERATED_TABLE_PREFIXES. A table only belongs here if its EXISTENCE does not depend on
# which sources a target activates.
#
# `qtn_` and `stg_` fail that test. factory.build skips the quarantine twin for a target
# with no active binding at all -- a qtn_ table with no flows would be a flowless
# streaming table, the one thing SDP cannot be handed -- so regions with different
# active_sources legitimately hold different quarantine tables. Comparing them would
# report drift that is the model working. verify_repo asserts this exclusion by name and
# refused the widening when it was attempted on 5 Sep; the gate was right.
#
# Derived from GENERATABLE, so the line is "kinds the MODEL declares" rather than a list
# to maintain: a new generatable kind lands here automatically, and a per-binding
# derivative like staging or quarantine never does.
CONFORMANCE_TABLE_PREFIXES = tuple(sorted(
    (PREFIX[k] for k in GENERATABLE), key=len, reverse=True))

VAULT_TABLE_PREFIXES = tuple(sorted(
    (PREFIX[k] for k in PREFIX if k not in ("staging", "quarantine", "ghost")), key=len,
    reverse=True))


def reads_vault(bronze_table: str) -> bool:
    """True when `bronze_table` names a vault table rather than a bronze feed."""
    base = bronze_table.rsplit(".", 1)[-1].lower()
    return base.startswith(VAULT_TABLE_PREFIXES)


def system_columns_for(kind: str) -> tuple[str, ...]:
    """The system columns a table of this kind actually carries, in declared order."""
    if kind in KEYED_KINDS:
        return tuple(c for c in SYSTEM_COLUMNS if c not in _KEYED_OMITS)
    return SYSTEM_COLUMNS


# DEF-42: the kinds loaded through a STAGING LOG rather than straight into the vault.
#
# A hub is a conformed identity: several sources legitimately supply the same business
# key, so its table needs insert-if-not-exists ACROSS flows -- and a streaming table
# cannot read its own contents. Measured 25 Aug 2026: 538,186 duplicate keys in
# hub_accounting_journal, every one of them a key supplied by two sources, with ZERO
# duplicates inside any single source. The staging dedup was never the problem.
#
# So the pipeline appends to stg_hub_x and a batch task does the anti-join insert into
# hub_x, which is the classical Data Vault hub loader. It is an INSERT with a lookup, so
# the hub stays strictly append-only and append_only_check keeps its present meaning.
#
# EVERY KEYED KIND IS HERE, not just the hub. A keyed table -- hub, link, NHL, HAL --
# asserts that a key or a relationship EXISTS, so it needs exactly one row per hash key,
# and deciding whether a key already landed means reading the target. A streaming flow
# cannot do that: it cannot read the table it is writing, and its only defence is an
# in-stream dropDuplicates whose memory lives in the CHECKPOINT. That is not a property
# of the data, it is a property of a file -- reset the checkpoint and the whole log is
# re-read against an empty memory. Measured 28 Sep 2026: a checkpoint reset duplicated
# 1,164 rows into nhl_invoice_line_rev2, a table nothing had ever declared unsafe.
#
# Links, NHLs and HALs used to be excluded on the grounds that every one was
# single-source, so two flows could not race for the same key -- with spec.validate
# refusing a multi-source link to keep that true. That argument was always about the
# WRONG duplicate. Single-source rules out two flows supplying one key; it says nothing
# about ONE flow supplying that key twice, which is exactly what a checkpoint reset makes
# it do. The anti-join is idempotent against both, because it asks the target, and the
# target does not forget.
#
# DEF-52: satellites join for the same reason and a different anti-join. A satellite
# inserts a version only when its hashdiff differs from the LATEST stored one, and
# "latest stored" is the table being written. Without the batch step every satellite
# re-appends every row on every run -- and no gate objects, because a re-delivery
# arrives with a fresh load_dts and is unique at the grain append_only asserts.
#
# SATELLITE_KINDS is kept as its own name, not inlined into STAGED_KINDS, because
# checks/load_satellites.py needs exactly this set (every satellite kind, never a keyed
# one) for its own hashdiff compare -- the keyed anti-join and the satellite hashdiff
# anti-join are two SQL shapes keyed off two sets, and STAGED_KINDS is now their union
# rather than either of them. BUSINESS_KINDS was once duplicated
# between this module and src/pipelines/silver_vault.py, and the system-column set was
# once duplicated between the projection and the ghost flow; both copies drifted and
# both caused live defects. One definition, two readers.
# esat IS one of them. It is in GENERATABLE, spec.py treats it as a satellite in five
# places (parent hash key, hashdiff, payload rules, uniqueness grain) and factory.py
# projects it as one -- so omitting it here would give a future esat no staging log and
# no loader, silently: the pipeline would write straight into esat_x and re-append every
# delivered row for ever, which is the exact defect DEF-52 exists to close. No esat is
# declared today, so including it changes nothing that is built; it only means the first
# one to be declared is staged like every other satellite instead of being the one kind
# that quietly is not. Derive from this set rather than typing (sat, msat, esat, csat)
# out again -- that hand-written tuple is what let esat fall out in the first place.
SATELLITE_KINDS = frozenset({"sat", "msat", "esat", "csat"})
# Written as the union of the two named sets, never as a literal: every generatable kind
# is either keyed or a satellite, so this is "everything", and stating it this way is what
# makes `GENERATABLE - STAGED_KINDS` an empty set a new kind has to argue its way out of
# rather than fall out of by being forgotten.
STAGED_KINDS = KEYED_KINDS | SATELLITE_KINDS

# RAW satellites: delivered by a source, as against computed in the business vault. The
# distinction is real -- a raw satellite has one table PER SOURCE and a csat does not --
# and it was written as the hand-typed `("sat", "msat", "esat")` in six places. Derived
# by subtraction so the two sets cannot disagree about where csat belongs.
RAW_SATELLITE_KINDS = SATELLITE_KINDS - BUSINESS_KINDS


def stg(table: str) -> str:
    """The staging log a staged kind's flows append to."""
    return f"{PREFIX['staging']}{table}"


def unstg(table: str) -> str:
    """The vault table a staging log feeds -- the inverse of stg(), and a no-op
    for a table that is not staged."""
    pfx = PREFIX["staging"]
    return table[len(pfx):] if table.startswith(pfx) else table


def pipeline_table(kind: str, table: str) -> str:
    """The object the PIPELINE writes for this kind: the log if staged, else the table."""
    return stg(table) if kind in STAGED_KINDS else table


def vault_schema_for(kind: str, raw_schema: str, business_schema: str) -> str:
    """Which schema an entity of this kind is built into.

    One function rather than a comparison spelled out at each call site: the two layers
    are a deployment fact, and a caller that got the test backwards would emit grants
    against a schema where the table does not exist.
    """
    return business_schema if kind in BUSINESS_KINDS else raw_schema
