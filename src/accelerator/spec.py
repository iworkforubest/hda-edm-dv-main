"""
Entity metadata: load, validate, freeze.

The metadata in metadata/entities/*.yml is the source of truth for the model. It
lives in the repository -- not in a table -- for three reasons:

  1. Review. A structural change to the vault is a pull request, not an UPDATE.
  2. Determinism. The same committed metadata generates the same model, so the
     four regional lakes are identical by construction rather than by sync.
  3. Reproducibility. The generator (Claude Code / AI Dev Kit) writes metadata and
     code at BUILD time; nothing is generated at run time, so run N+1 cannot differ
     from run N.

The metadata is additionally PUBLISHED to a Unity Catalog table after deploy
(see checks/conformance_check.py) purely so it is inspectable and lineage-visible.
That published copy is read-only and is never the input to a load.

Validation here is deliberately strict and fails the build. Every rule below exists
because breaking it is silent at run time and expensive to unwind.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

from . import naming, sql_text
from .hashing import RULEBOOK_VERSION

IDENT = re.compile(r"^[a-z][a-z0-9_]{2,62}$")
# Source identifiers become table-name suffixes. Two characters is legitimate -- GP
# (Great Plains) and other legacy finance systems are named that way.
SOURCE_IDENT = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
# A key_literals value is interpolated into a hash expression as a quoted SQL string
# constant (see factory.py). It must not be able to carry SQL -- no quote, no backslash,
# no whitespace that could close the literal or inject a clause.
LITERAL_VALUE = re.compile(r"^[A-Za-z0-9_.-]+$")
# A cast value is interpolated into a .cast(...) call as a bare SQL type name (see
# factory.py). It must not be able to carry SQL -- a type name plus an optional
# (precision[,scale]), nothing else.
CAST_TYPE = re.compile(r"^[A-Za-z]+(\(\d+(,\d+)?\))?$")

# The accounting roles an entity may declare (see Entity.accounting). Restricted, because
# an unrecognised role is a typo that would otherwise leave the gate asserting nothing.
ACCOUNTING_ROLES = ("journal", "debit", "credit", "line_order", "control_total")


# The kinds loop-1 reconciles, and therefore the kinds where a delivered count is read as an
# approved count. Kept in step with checks/loop1_reconciliation.RECONCILABLE_KINDS by an
# assertion in tests/test_accelerator.py -- two literals that must agree, so something has to
# say so. spec.py cannot import from checks/, which is why this is a second statement at all.
RECONCILABLE_KINDS_FOR_MANIFEST: frozenset = frozenset({"nhl", "link", "hal"})


class SpecError(ValueError):
    """Metadata that must not be allowed to reach a workspace."""


# --------------------------------------------------------------------------- #
# Value objects
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SourceBinding:
    """One source feeding one entity."""

    name: str                    # e.g. UKG_US  -- becomes rec_src and the BK scope
    bronze_table: str            # fully qualified Bronze streaming table
    key_columns: tuple[str, ...] = ()
    # Constant business-key components this binding supplies rather than reads: (business
    # key name, literal value) pairs. Exists because a hub can fold several sources keyed
    # on (reference_type, reference_id) where the type qualifies the value so distinct
    # assertions cannot collide -- e.g. hub_organisation's
    # ('Organization_Reference_ID', 'BRPLM') vs ('Fieldglass_Buyer', ...) -- and no single
    # source carries a type column: Dynamics GP gives a bare input_db, UKG a bare company.
    # The binding supplies the type as a constant instead.
    #
    # Deliberately its OWN construct, not a quoted string appended to key_columns:
    # factory.py builds the readable business key with F.col(c) over key_columns, and
    # key_safety_rules/hash_key both treat key_columns entries as column references, not
    # arbitrary SQL. A literal that lived in key_columns would be silently wrong in all
    # three places. See factory.py for how key_literals and key_columns are woven back
    # together, in business_keys order, at hash-build time.
    key_literals: tuple[tuple[str, str], ...] = ()
    # For links, NHLs and link-satellites: which SOURCE columns carry each parent hub's
    # business key. A flat key_columns list cannot express this once an entity has more
    # than one parent -- a journal line has three (journal, company, ledger account) and
    # the loader has to know which columns belong to which.
    parent_keys: tuple[tuple[str, tuple[str, ...]], ...] = ()
    payload: tuple[str, ...] = ()
    applied_dts_column: str | None = None
    cdc_op_column: str | None = None
    manifest_column: str | None = None
    expectations: tuple[str, ...] = ()   # names resolved from the UC expectations table
    dedup_by: tuple[str, ...] = ()      # collapse _raw re-deliveries on these columns
    dedup_order: tuple[str, ...] = ()   # deterministic winner: greatest wins, in order
    # {source column: SQL type} -- coerced in _stage AFTER dedup and BEFORE system
    # columns. Exists because some source types render unstably to string: GP's
    # debitamt/crdtamnt arrive as DOUBLE, and a DOUBLE's string form is not stable
    # across loads, so identical amounts can produce different hashdiffs and therefore
    # spurious satellite rows forever. The cast REPLACES the payload column by name --
    # factory.py's hashdiff_expr hashes payload by column name, so a cast into a new
    # column would be invisible to it.
    cast: tuple[tuple[str, str], ...] = ()
    # THE SCOPE THIS BINDING'S KEYS HASH UNDER. Empty means the binding's own name, which
    # is right for a feed: a federated key is (source_system, native key), and the binding
    # name IS the source system.
    #
    # IT IS WRONG FOR A COMPUTED SATELLITE, which is why this exists. A business-vault
    # satellite's input is the vault, not a feed, so its binding is named for what it is
    # (BUSINESS_VAULT) rather than for a system -- and nothing will ever deliver job
    # requests "from the business vault", so a parent key scoped that way can never equal
    # the key its parent hub builds. Measured 4 September 2026: two such keys existed, and
    # they were well-formed, reproducible values that no join would ever match.
    #
    # The fix is not to rename the binding. rec_src carries the binding name, and BUSINESS
    # _VAULT is the truthful record source for a row computed here -- claiming FIELDGLASS_EU
    # produced it would trade a broken join for a false provenance. So the scope is declared
    # separately: this row was computed in the business vault, and its keys hash under the
    # scope of the feed the data came from.
    key_scope: str = ""
    # SOURCE CONFORMANCE APPLIED IN THE FLOW, NOT IN A VIEW.
    #
    # `conform` names a profile in metadata/source_unions.yml; load_model resolves it and
    # fills row_filter and derived_columns from that one declaration, so a profile read by
    # six bindings is still declared once.
    #
    # WHY NOT A VIEW, which is where this repo used to put per-source conformance. A view
    # is right when the profile genuinely spans many tables -- the job-posting one unions
    # 30, and that pipeline runs in 1.7 minutes. It is wrong for a profile matching ONE
    # table, because a MASKED entity reading a view could not be built at all: SDP must
    # reconcile the flow's streaming plan against the declared schema at definition time,
    # and through a view that did not finish. Measured 25 September across five domain
    # pipelines -- every combination worked except masked-entity-through-a-view, which
    # never left INITIALIZING in four attempts.
    conform: str = ""
    row_filter: str = ""
    derived_columns: tuple[tuple[str, str], ...] = ()
    # HOW STALE THIS FEED MAY GET BEFORE A LOAD SHOULD REFUSE TO TRUST IT, in hours.
    #
    # MEASURED AGAINST WHEN THE TABLE LAST LOADED, NOT WHEN A VALUE LAST CHANGED, and that
    # distinction is the whole point. `aud_table_load` records a row per run per table
    # whether or not any row landed, so a feed that legitimately delivered nothing reads
    # FRESH and a pipeline that stopped running reads STALE. Keying this to max(load_dts)
    # on the vault table instead would measure "when a row last arrived": it would flag
    # steady data as stale and let a stalled pipeline masquerade as fresh, which is the
    # failure the SLA exists to catch, rebuilt inside the check meant to catch it.
    #
    # ZERO MEANS DECLARED EXEMPT, and it must still be written down: an omitted SLA and a
    # deliberate exemption are different states and only one of them is a decision.
    # checks/freshness_check.py reads this; validate() below refuses an absurd value, and
    # verify_repo refuses an ACTIVE binding that declares nothing at all.
    freshness_sla_hours: int | None = None
    # THE GENERATION OF THIS BINDING'S STREAM. Bump it when the SOURCE TABLE IDENTITY
    # changes underneath us -- not when its contents change.
    #
    # An SDP flow owns a checkpoint, and that checkpoint is keyed by the FLOW NAME. When
    # Bronze drops and recreates a table, the new table is a different Delta object with a
    # different id, and every flow reading it fails at analysis with
    # [DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE]. Measured 28 September 2026:
    # 01_usnc_bronze_dev.sap_fieldglass_raw.invoices was recreated (new Delta id
    # 06b65224-ef33-44fb-beb5-6ca4892277fb) and took all seven flows reading it down.
    #
    # THE OBVIOUS FIX IS A FULL REFRESH, AND IT IS NOT AVAILABLE TO US. Every vault table
    # carries delta.appendOnly = true (factory._properties), SDP propagates that onto the
    # __materialization_* twin, and the refresh dies with DELTA_CANNOT_MODIFY_APPEND_ONLY.
    # Turning that property off to get a refresh through would switch off the one guarantee
    # that stops vault history being rewritten, on the tables append_only_check.py watches,
    # and would have to be done again on the next Bronze recreate.
    #
    # So the checkpoint is replaced instead of the data. A generation > 1 appends `_g<N>`
    # to the flow name (factory._flow_name), which SDP reads as a flow it has never seen:
    # fresh empty checkpoint, source re-read from the start, rows APPENDED. No refresh, no
    # truncation, no mutating operation in any table's history, delta.appendOnly untouched.
    #
    # RE-READING THE WHOLE SOURCE IS SAFE FOR A HUB OR A SATELLITE, AND IS NOT SAFE FOR AN
    # NHL. This paragraph claimed all three were safe when the mechanism was written on
    # 28 September 2026. Two of the three were right, and the run that same evening proved
    # the third wrong: nhl_invoice_line_rev2 came out with 1164 duplicate rows at grain
    # (invoice_line_hk), caught by checks/append_only_check.py.
    #
    # A `stg_` log is meant to hold every row every flow ever appended, duplicates included
    # (DEF-42). That is only harmless because the CONSUMER removes them, and the three
    # consumers do not remove them the same way:
    #
    #   hub  -- checks/load_hubs.py anti-joins the TARGET TABLE with NOT EXISTS, and says
    #           of itself "IDEMPOTENT BY CONSTRUCTION ... a second run inserts nothing".
    #           A re-read adds nothing. Safe.
    #   sat  -- compares hashdiff against the rows already present. An unchanged payload
    #           produces no new row. Safe.
    #   NHL  -- factory.py's `df.dropDuplicates(src.dedup_by)`, which is STATEFUL and whose
    #           state lives IN THE CHECKPOINT. A generation bump discards exactly that
    #           state, so the whole staging log -- including the duplicates it holds by
    #           design -- is re-read with an empty dedup memory and appended again.
    #
    # So an NHL's idempotence WAS stored in the one thing this mechanism replaces, and
    # bumping the generation of a binding that fed an NHL doubled that NHL's rows.
    #
    # FIXED 29 September 2026: every keyed kind is in naming.STAGED_KINDS, so an NHL --
    # like a link and a HAL -- now writes to a `stg_` log and is loaded into its vault
    # table by checks/load_hubs.py with the same NOT EXISTS anti-join against the target
    # that made the hub safe. A bump still discards the dedup checkpoint and re-reads the
    # whole log; the loader's anti-join removes the duplicates that re-read produces, so
    # bumping an NHL binding is now as safe as bumping a hub's.
    #
    # GENERATION 1 EMITS NO SUFFIX, so every existing flow keeps its name and its
    # checkpoint. Bumping this is a deliberate, reviewable act on one binding; it is not
    # something a refactor can do by accident.
    stream_generation: int = 1


def key_scope_of(src: SourceBinding) -> str:
    """The literal a federated key prepends for this binding.

    ONE FUNCTION, because `src.key_scope or src.name` written at four call sites is four
    places for the override to be forgotten -- and forgetting it is silent: the key is
    still a well-formed hash, it just never joins. That is the failure mode this whole
    field exists to remove, so it does not get a second chance to reappear as a missing
    `or`.
    """
    return src.key_scope or src.name


def parent_legs(entity: "Entity") -> tuple[tuple[str, str], ...]:
    """This entity's parent legs as (hub, role) pairs, role empty when unroled.

    ONE PLACE THAT ZIPS `parents` WITH `parent_roles`, because doing it inline is how the
    two fall out of step -- and out of step they are silent: a leg that loses its role
    reverts to the bare hub column name and collides with its sibling.
    """
    roles = entity.parent_roles or ("",) * len(entity.parents)
    return tuple(zip(entity.parents, roles))


def parent_key_columns(src: SourceBinding, parent: str, role: str = "") -> tuple[str, ...]:
    """Source columns carrying one parent leg's business key, for this binding.

    LOOKED UP BY ROLE WHEN THERE IS ONE. `parent_keys` is a mapping, so on a hub that
    appears twice a hub-keyed lookup returns the same columns for both legs -- which is
    precisely the collapse that let a self-referencing link hash the child twice and call
    it a hierarchy. With a role declared, the role is the key.
    """
    wanted = role or parent
    for name, cols in src.parent_keys:
        if name == wanted:
            return cols
    raise SpecError(
        f"source {src.name}: no parent_keys entry for {'role' if role else 'parent'} "
        f"{wanted!r}. Links, NHLs and link-satellites must map every parent explicitly, "
        f"and a roled parent is mapped by its ROLE rather than by its hub."
    )


# --------------------------------------------------------------------------- #
# Business-key assembly -- pure metadata composition, no Spark.
#
# Lives here (not factory.py) because it is a modelling concern -- how business_keys,
# key_columns and key_literals compose into an ordered list -- not a Spark concern. It
# is also the only way to unit-test this without pyspark installed: factory.py imports
# pyspark at module level, so tests/test_accelerator.py never imports it, and a defect
# here (an FK misaligned by one position) is otherwise silent -- the pipeline builds,
# rows load, append_only_check and loop1_reconciliation both pass, and it surfaces only
# when a join between a link and its parent hub returns nothing.
# --------------------------------------------------------------------------- #
def hub_key_components(entity: "Entity", src: SourceBinding) -> list[tuple[bool, str]]:
    """Ordered (is_literal, value) pairs for a HUB's own business key, one per
    entity.business_keys position.

    Walks entity.business_keys and, for each position, takes the literal from
    src.key_literals when that business key name is declared there, else the next
    unconsumed entry from src.key_columns, in order. Raises SpecError on a count
    mismatch rather than silently misaligning -- callable directly (bypassing
    validate()) so the failure mode is exercised by a unit test, not just by the
    build-time gate in validate().
    """
    literals = dict(src.key_literals)
    if len(src.key_columns) + len(literals) != len(entity.business_keys):
        raise SpecError(
            f"{entity.name}/{src.name}: key_columns ({len(src.key_columns)}) plus "
            f"key_literals ({len(literals)}) must map 1:1 to business_keys "
            f"({len(entity.business_keys)})"
        )
    columns = iter(src.key_columns)
    parts: list[tuple[bool, str]] = []
    for bk in entity.business_keys:
        if bk in literals:
            parts.append((True, literals[bk]))
        else:
            parts.append((False, next(columns)))
    return parts


@dataclass(frozen=True)
class Entity:
    name: str                    # logical entity, unprefixed: worker, job_request
    kind: str                    # hub | link | nhl | sat | msat | esat | csat | hal
    domain: str                  # pay_bill | party | ...  -- drives schema placement
    key_style: str = "federated"  # federated | authored | tenant_scoped
    business_keys: tuple[str, ...] = ()
    # For key_style: tenant_scoped -- which business key columns carry the tenant.
    # Required, because the failure it prevents is silent: if a source's references are
    # unique only WITHIN a buyer instance, keying on source + reference merges different
    # clients' records into one hub row and the numbers stay plausible.
    tenant_key: tuple[str, ...] = ()
    parents: tuple[str, ...] = ()        # hub entity names, for links / nhls / sats
    # ROLES, for a link whose parents are not all distinct hubs. A HIERARCHICAL LINK
    # relates a hub to ITSELF -- `parents: [legal_entity, legal_entity]` -- and without
    # roles the model cannot tell the two legs apart. That is not a validation gap that
    # merely rejects; MEASURED 5 Sep 2026, before this field existed, such an entity
    # LOADED and produced:
    #
    #     FK columns   ['legal_entity_hk', 'legal_entity_hk']   <- one column, not two
    #     link key     hash(['child_code', 'child_code'])       <- the child, twice
    #
    # A hierarchy in which every node is its own parent, built without an error. So roles
    # are MANDATORY the moment a hub appears twice in `parents` (validated below), and
    # they name the FK columns: role `parent` over hub `legal_entity` becomes
    # `parent_legal_entity_hk`. `parent_keys` is then keyed by ROLE rather than by hub,
    # because keying by hub is exactly what collapsed the two legs into one.
    #
    # BOTH legs carry a role, never just one. An implicit "the unprefixed column is the
    # child" is a convention a reader gets backwards, and reading a hierarchy backwards
    # inverts every rollup while every row still joins.
    parent_roles: tuple[str, ...] = ()   # per-parent role names, same order as `parents`
    # WHAT A BUSINESS COLUMN MEANS, written by a person who knows. {column: sentence}.
    #
    # Only for columns whose meaning is a business fact -- the technical columns are
    # documented once in naming.COLUMN_DOC and the keys derive their own sentence, so this
    # map should never restate either. contract.description resolves all four sources.
    #
    # DELIBERATELY SPARSE, AND STAYING THAT WAY IS THE POINT. Measured 5 Sep 2026: 133
    # distinct business column names carry nothing. Filling them with a restatement of the
    # column's own name -- "job_title: the job title" -- would read as documentation, be
    # indistinguishable from the real thing later, and leave the reader no better off.
    # verify_repo refuses a description contained in its column's own name, so a tautology
    # fails the build rather than being quietly added to the count.
    descriptions: tuple[tuple[str, str], ...] = ()
    payload: tuple[str, ...] = ()        # satellites and NHLs only
    transaction_key: tuple[str, ...] = ()  # NHL only: includes dependent child keys
    mas_key: tuple[str, ...] = ()        # msat only
    driving_key: str | None = None       # esat only
    sensitivity: str = "internal"        # internal | personal | financial | restricted
    # GRAIN. Aggregated source data is legitimate in the Raw Vault -- we record what the
    # source sent -- but it must be LABELLED, because the failure it invites is silent:
    # deriving a per-worker fact from an account-level total. A GL journal line carrying
    # every worker's HSA deduction summed cannot answer "what did we pay this worker",
    # and nothing about the row's shape says so.
    grain: str = "transaction"            # transaction | aggregate
    # For an aggregate: the transaction-grain entity it summarises. Declaring it creates
    # the reconciliation pair that checks/payroll_gl_reconciliation_check.py derives its
    # assertions from -- so the control comes from the model, not from hardcoded names.
    aggregates_from: str = ""
    # Which parents of the transaction-grain entity are LOST in the aggregation. This is
    # the fact a consumer needs and cannot infer: a GL journal line drops worker and
    # pay_period, so it can never answer a per-worker question however it is joined.
    aggregate_drops: tuple[str, ...] = ()
    masks: tuple[tuple[str, str], ...] = ()   # (column, UC mask function) pairs
    # ACCOUNTING ROLES -- (role, column) pairs naming which of THIS entity's columns play
    # the roles checks/journal_integrity_check.py asserts over. It exists because the
    # generator renames nothing: Dynamics GP calls the line ordinal `seqnumbr` and the
    # amounts `debitamt`/`crdtamnt`, UKG calls the amounts `debit`/`credit` and has no
    # line ordinal at all. A gate that hardcodes one vocabulary asserts nothing about the
    # other, and the failure is silent -- the SQL simply names a column that is not there,
    # which is how journal_integrity_check.py came to reference `line_order`,
    # `debit_amount` and `credit_amount`, none of which any bound source delivers.
    #
    # Roles: journal (which PARENT hub groups lines into one journal), debit, credit,
    # line_order (OPTIONAL -- omitted where the source has no dense line ordinal), and
    # control_total (declared on the entity that CARRIES the control figure -- the journal
    # header satellite -- not on the line). An omitted role is meaningful: the gate reports
    # the property as one it could not evaluate, by name, rather than skipping it silently.
    accounting: tuple[tuple[str, str], ...] = ()
    sources: tuple[SourceBinding, ...] = ()
    change_detection: str = "cdc"        # cdc -- the only accepted value (factory.py)
    rulebook_version: str = RULEBOOK_VERSION
    version: int = 1                     # physical table version; see naming.physical()
    notes: str = ""

    # ---- derived ------------------------------------------------------------
    @property
    def base_table(self) -> str:
        """Source-independent table name, e.g. sat_job_request_details."""
        return f"{naming.PREFIX[self.kind]}{self.name}"

    @property
    def table(self) -> str:
        """Kept for hubs, links and NHLs, which are ONE table fed by N sources.

        Satellites do not have a single table -- see tables(). Accessing this on a
        satellite is a bug, so it raises rather than returning something plausible.
        """
        if self.kind in naming.RAW_SATELLITE_KINDS:
            raise SpecError(
                f"{self.name}: a raw satellite has one table PER SOURCE. Use tables() "
                f"or base_table, not table."
            )
        return self.base_table

    def tables(self) -> list[tuple[SourceBinding | None, str]]:
        """The PHYSICAL tables this entity produces, version suffix included.

        HUB / LINK / NHL / HAL -> exactly one table, fed by one append flow per source.
        SAT / MSAT / ESAT -> one table per source, named <base>_<source>_rev<N>.
        CSAT -> one table: a computed satellite's input is the vault, not a feed.

        THIS IS THE NAME A LOADER WRITES TO. Consumers never see it -- they read the
        view returned by stable_tables(). See naming.physical().
        """
        return [(src, naming.physical(base, self.version))
                for src, base in self._base_tables()]

    def stable_tables(self) -> list[tuple[SourceBinding | None, str]]:
        """The unversioned VIEW names consumers read, one per physical table.

        Same order and same length as tables(), because a cutover pairs them by
        position.
        """
        return list(self._base_tables())

    def _base_tables(self) -> list[tuple[SourceBinding | None, str]]:
        """The version-free naming, which both of the above build on.

        HUB / LINK / NHL / HAL -> exactly one table, fed by one append flow per source.
        Identity and relationships are conformed concepts: one hub_worker holds worker
        identity from every system.

        SAT / MSAT / ESAT -> one table per source, named <base>_<source>.
        This is the one-source-per-satellite rule, enforced by construction rather than
        by refusing multi-source metadata. The ENTITY is source-independent
        (job_request_details); the TABLES carry the source, because a Bullhorn schema
        change must not be able to corrupt the change history of the HR satellite.

        CSAT -> one table: a computed satellite's input is the vault, not a feed.
        """
        if self.kind in naming.RAW_SATELLITE_KINDS:
            return [(src, f"{self.base_table}_{src.name.lower()}") for src in self.sources]
        return [(None, self.base_table)]

    @property
    def hk_column(self) -> str:
        return naming.hk(self.name)

    @property
    def is_multi_source(self) -> bool:
        return len(self.sources) > 1

    @property
    def scope_for(self) -> str | None:
        """Whether the source name participates in the business key."""
        return None if self.key_style == "authored" else "SOURCE"

    @property
    def accounting_map(self) -> dict[str, str]:
        """{role: column} for the accounting roles this entity declares.

        A role that is absent is absent on purpose -- see the field comment. Callers
        must report an absent role, never quietly drop the assertion that needed it.
        """
        return dict(self.accounting)


@dataclass
class Model:
    entities: list[Entity] = field(default_factory=list)

    def by_kind(self, *kinds: str) -> list[Entity]:
        return [e for e in self.entities if e.kind in kinds]

    def get(self, name: str) -> Entity:
        for e in self.entities:
            if e.name == name:
                return e
        raise SpecError(f"unknown parent entity {name!r}")


def parent_key_components(
    model: Model, entity: Entity, parent: str, src: SourceBinding, role: str = ""
) -> list[tuple[bool, str]]:
    """Ordered (is_literal, value) pairs for one PARENT hub's business key, as seen from
    a link/NHL/link-satellite binding `src` that references it.

    This exists to fix a defect that would otherwise be silent: a link's own binding
    only ever declares COLUMNS for a parent (parent_keys), so if the parent hub is
    literal-keyed, deriving the parent's key from `src` alone omits the literal -- the
    link computes a different hash than the hub computes for the same real-world
    parent, and every row loads with a foreign key pointing at a hub row that does not
    exist. Silent, because append_only_check and reconciliation both still pass; it only
    surfaces when a join returns nothing.

    NARROWER GUARD (round 2): the strict, literal-aware path below is only entered when
    the parent hub declares key_literals on AT LEAST ONE of its own bindings. If it
    declares none anywhere, there is no literal to omit, and a column-only derivation
    from `src` is correct -- so this returns `[(False, c) for c in
    parent_key_columns(src, parent)]` with no further validation, reproducing the
    pre-key_literals behaviour byte for byte. This matters in practice: several
    existing links/NHLs/link-satellites reference a parent hub under a source name the
    parent hub does not itself carry a binding for (e.g. an NHL fed by UKG_US whose
    `worker` parent hub only has STRIIVE_EU/HR_EU bindings) -- correct today, because
    neither side needs a literal, and must keep building.

    Once a hub DOES declare a literal somewhere, the literal belongs to the PARENT
    HUB's OWN binding for the same source name -- not to the link's metadata, where it
    could drift out of sync with the hub. So: look up the parent hub, find its
    SourceBinding whose name matches src.name, and take that binding's key_literals.
    Column positions still come from parent_key_columns(src, parent, role) -- the columns
    THIS binding declares for the parent's non-literal positions. `role` is empty for
    every ordinary parent and names the leg on a link whose parents repeat a hub; it
    changes only WHICH parent_keys entry is read, never how the key is composed, so a
    roled leg hashes exactly as the hub's own loader would.

    Raises SpecError, naming the entity, the parent and the source, if the parent hub
    declares a literal somewhere but has no binding for this source name: the parent
    key cannot be derived, and falling back to a column-only key would reproduce the
    exact silent defect this function exists to prevent.
    """
    hub = model.get(parent)
    columns = list(parent_key_columns(src, parent, role))

    # DEF-50: THE ARITY CHECK RUNS ON BOTH PATHS. It used to sit below the early return,
    # so it only ever ran for a hub that declared a literal somewhere -- which is two of
    # our six. The other four (job_request, ledger_account, pay_period, worker) took the
    # return above and were never counted at all.
    #
    # A miscount there is the exact defect this function exists to prevent, and it is
    # silent: too few or too many columns produce a hash over a different component list,
    # so the child's parent key is a well-formed value that simply never equals the hub's
    # own. No gate catches it -- append_only checks uniqueness, not joinability, and a
    # PIT join would just return nothing.
    if not any(b.key_literals for b in hub.sources):
        if len(columns) != len(hub.business_keys):
            raise SpecError(
                f"{entity.name}/{src.name}: parent_keys[{parent!r}] has {len(columns)} "
                f"column(s) but hub {parent!r} has {len(hub.business_keys)} business "
                f"key(s) {list(hub.business_keys)} and declares no literals on any "
                f"binding. The parent key would hash a different component list from the "
                f"hub's own key, so it would be a well-formed value that never joins."
            )
        return [(False, c) for c in columns]

    parent_binding = next((b for b in hub.sources if b.name == src.name), None)
    if parent_binding is None:
        raise SpecError(
            f"{entity.name}/{src.name}: parent {parent!r} declares key_literals on at "
            f"least one of its own bindings, but has no source binding named "
            f"{src.name!r} to look them up from. A link/NHL's parent key must be "
            f"derived from the parent hub's own binding for the same source -- add a "
            f"source named {src.name!r} to {parent!r}, or its literal key components "
            f"cannot be looked up and this key cannot be proven identical to the hub's "
            f"own."
        )
    literals = dict(parent_binding.key_literals)
    expected_columns = len(hub.business_keys) - len(literals)
    if len(columns) != expected_columns:
        raise SpecError(
            f"{entity.name}/{src.name}: parent_keys[{parent!r}] has {len(columns)} "
            f"column(s) but hub {parent!r} needs {expected_columns} "
            f"(business_keys {list(hub.business_keys)} minus {len(literals)} literal(s) "
            f"supplied by {parent!r}'s own {src.name!r} binding)"
        )
    columns_iter = iter(columns)
    parts: list[tuple[bool, str]] = []
    for bk in hub.business_keys:
        if bk in literals:
            parts.append((True, literals[bk]))
        else:
            parts.append((False, next(columns_iter)))
    return parts


def key_components_to_hash_columns(parts: list[tuple[bool, str]]) -> list[str]:
    """Turn (is_literal, value) pairs -- from hub_key_components / parent_key_components
    -- into hash_key()'s column-list argument.

    A literal is quoted as a SQL string constant. hashing.normalise() only CASTs, TRIMs
    and UPPERs whatever expression it is given -- it does not care whether that
    expression is a column reference or a quoted constant -- so a literal normalises
    exactly as a column value would. A column stays a bare identifier.

    Lives here (not factory.py) so both factory.py and tests/test_accelerator.py import
    the SAME function: factory.py cannot be imported without pyspark, so a test that
    hand-copied this quoting would only ever verify the copy, not the shipped code.

    ESCAPED, NOT REFUSED, and that is the opposite call from hashing's source scope. A
    literal here is a business VALUE -- a tenant, a reference type, and a tenant may be
    called O'Brien. hashing.reference_key hashes the raw Python value, so the SQL literal
    has to PARSE BACK to it; an unescaped f-string emitted `'O'Brien'`, which Spark reads
    as the three-character `O` followed by bare SQL, and a backslash in a value silently
    became an escape sequence. Refusing instead would make a legitimate declaration
    undeclarable and put the two sides of the hash-parity gate under different rules.
    """
    return [sql_text.literal(v) if is_literal else v for is_literal, v in parts]


def hash_key_columns(
    model: "Model", entity: "Entity", src: SourceBinding
) -> dict[str, tuple[list[str], str | None]]:
    """Every hash-key column one binding derives, mapped to (hash components, source scope).

    THE ONE AUTHORITY ON WHAT GOES INTO A KEY. The values returned here are the exact
    arguments factory.py hands `hashing.hash_key` -- component list and scope -- for every
    `_hk` column a flow writes. factory.py calls this rather than deciding it inline, so
    there is one derivation and not two.

    That single-authority framing is the point, not tidiness. factory.py cannot be imported
    without pyspark, so anything that wants to DESCRIBE a key -- the source-to-target
    mapping, a contract, a diagram -- previously had to re-derive it by hand, and a
    hand-copy that drifts produces a document confidently describing keys the loader does
    not build. This repo has been bitten by exactly that shape three times already
    (BUSINESS_KINDS, the system-column set, RECONCILABLE_KINDS), and each time the fix was
    to make one definition authoritative.

    THE SCOPE IS PART OF THE KEY, and it is the component a reader is most likely to miss.
    A federated hub prepends its source name as a LITERAL, so two sources delivering the
    same client code produce two different hash keys -- deliberately, because no single
    system is authoritative for that identifier. An authored hub omits it. A description of
    the key that named only the columns would render those two cases identically.

    Satellites take one of three paths, mirroring what the parent is:
      * a link/NHL/HAL parent -- the parent key is the LINK's own key expression, so the
        satellite's foreign key is identical to the link's by construction;
      * a hub parent with parent_keys declared -- the hub's key as the hub's own loader
        builds it, literals folded in from the HUB's binding for the same source;
      * a hub parent with no parent_keys -- the satellite's own key_columns, source-scoped.

    A LINK'S KEY IS BUILT FROM ITS PARENTS' BUSINESS KEY COMPONENTS, NOT THEIR HASHES.
    Hashing a hash would mean casting BINARY to STRING, which is lossy and
    platform-dependent; deriving from the business keys keeps the expression reproducible
    by the pure-Python reference implementation, which is what hash_parity_check compares
    Spark against. A link satellite draws from this same path, so its foreign key equals
    the link's own key by construction rather than by careful copying.
    """
    scope = None if entity.key_style == "authored" else key_scope_of(src)
    out: dict[str, tuple[list[str], str | None]] = {}

    if entity.kind == "hub":
        out[entity.hk_column] = (
            key_components_to_hash_columns(hub_key_components(entity, src)), scope
        )
    elif entity.kind in naming.LINK_KINDS:
        columns: list[str] = []
        # LEGS, NOT PARENTS. On a hierarchical link the same hub appears twice, so
        # iterating `parents` and naming the column from the hub alone writes one leg over
        # the other -- measured, and it produced a link key of the child hashed twice.
        for parent, role in parent_legs(entity):
            parent_cols = key_components_to_hash_columns(
                parent_key_components(model, entity, parent, src, role)
            )
            # A parent's FK is hashed as THAT hub's own loader would: the hub's key_style
            # decides its scope, not the link's. An authored parent reached from a
            # federated link is unscoped, and getting that backwards would produce a
            # well-formed key that never joins.
            hub = model.get(parent)
            out[naming.hk(parent, role)] = (
                parent_cols, None if hub.key_style == "authored" else key_scope_of(src)
            )
            columns.extend(parent_cols)
        columns.extend(entity.transaction_key)
        out[entity.hk_column] = (columns, scope)
    else:  # satellites
        parent = entity.parents[0]
        parent_entity = model.get(parent)
        if parent_entity.kind in naming.LINK_KINDS:
            columns = []
            for grandparent, gp_role in parent_legs(parent_entity):
                columns.extend(
                    key_components_to_hash_columns(
                        parent_key_components(model, parent_entity, grandparent, src,
                                              gp_role)
                    )
                )
            columns.extend(parent_entity.transaction_key)
            parent_scope = (None if parent_entity.key_style == "authored"
                            else key_scope_of(src))
        elif src.parent_keys:
            # COVERAGE NOTE, measured 4 September 2026: exactly one binding reaches this
            # branch -- accounting_journal_header/UKG_US -- and its parent hub
            # accounting_journal is AUTHORED, so the scope below is always None and the
            # `else key_scope_of(src)` limb is dead by data, not by logic. It is kept
            # because a federated parent with declared parent_keys is a shape the model can
            # legitimately grow, but do not read it as covered: tests/test_spark_derivation
            # cannot distinguish it from `None`, and a mutation removing it passes
            # everything. The two branches either side of it ARE exercised.
            columns = key_components_to_hash_columns(
                parent_key_components(model, entity, parent, src)
            )
            parent_scope = (None if parent_entity.key_style == "authored"
                            else key_scope_of(src))
        else:
            columns = list(src.key_columns)
            parent_scope = scope
        out[naming.hk(parent)] = (columns, parent_scope)

    return out


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _tuple(value: Any, what: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(str(v) for v in value)
    raise SpecError(f"{what} must be a string or a list, got {type(value).__name__}")


def _key_literals(raw: Any, what: str) -> tuple[tuple[str, str], ...]:
    """Parse and validate key_literals: {business_key_name: literal_value, ...}.

    The value is interpolated into a hash expression as a quoted SQL string constant
    (factory.py), so it must be restricted to a safe literal -- no quote, no backslash,
    nothing that could close the string and inject SQL.
    """
    pairs = tuple((str(k), str(v)) for k, v in (raw or {}).items())
    for key, value in pairs:
        if not LITERAL_VALUE.match(value):
            raise SpecError(
                f"{what}[{key!r}]: literal value {value!r} is not safe -- it must match "
                f"{LITERAL_VALUE.pattern!r}. A key_literals value is interpolated into a "
                f"hash expression as a quoted SQL string and must not be able to carry SQL."
            )
    return pairs


def _cast(raw: Any, what: str) -> tuple[tuple[str, str], ...]:
    """Parse and validate cast: {column: SQL type, ...}.

    Each type is interpolated into a .cast(...) call (factory.py) as a bare SQL
    type name, so it must be restricted to a safe token -- a type name and an
    optional (precision[,scale]), nothing that could carry SQL.
    """
    pairs = tuple((str(k), str(v)) for k, v in (raw or {}).items())
    for col, typ in pairs:
        if not CAST_TYPE.match(typ):
            raise SpecError(
                f"{what}[{col!r}]: cast type {typ!r} is not safe -- it must match "
                f"{CAST_TYPE.pattern!r}. A cast type is interpolated into a .cast() "
                f"call and must not be able to carry SQL."
            )
    return pairs


def _version(value, where: str) -> int:
    """A vault object's physical version. An integer >= 1, or it is not a version.

    NOT coerced from a string. `version: "2"` in YAML is a typo with a plausible
    reading, and a plausible reading is exactly how a table called hub_invoice_v2
    comes to exist beside one called hub_invoice_vtwo. bool is rejected explicitly
    because bool is a subclass of int and `version: true` would otherwise be 1.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise SpecError(
            f"{where}: version must be an integer >= 1, got {value!r} "
            f"({type(value).__name__}). It forms a physical table suffix.")
    if value < 1:
        raise SpecError(f"{where}: version must be >= 1, got {value}. "
                        f"Version 1 is the first version, not zero.")
    return value


# EVERY KEY A SOURCE BINDING MAY DECLARE. Not derived from SourceBinding's fields, and
# the difference is the point: `row_filter` and `derived_columns` ARE fields, but they are
# filled by load_model from the `conform` profile. Declaring either in YAML would be
# silently overwritten, so they are not accepted here.
#
# WHY THIS EXISTS AT ALL. Nothing rejected an unknown key until 28 September 2026, so a
# misspelled one was simply dropped -- `dedup_ordr` would leave the dedup unordered and
# the load would go green. That is tolerable for a key whose absence shows up downstream;
# it is not tolerable for `stream_generation`, whose whole job is to be noticed. A typo
# there leaves the flow name unchanged, the old broken checkpoint in place, and the run
# failing with the same error it was meant to fix -- while the YAML reads as if it were
# fixed. Unknown keys fail the load instead, naming the key and the entity.
BINDING_KEYS = frozenset({
    "name", "bronze_table", "key_columns", "key_literals", "parent_keys", "payload",
    "applied_dts_column", "cdc_op_column", "manifest_column", "expectations",
    "dedup_by", "dedup_order", "cast", "key_scope", "conform", "freshness_sla_hours",
    "stream_generation",
})


def _binding(raw: dict[str, Any], entity_name: str) -> SourceBinding:
    unknown = sorted(set(raw) - BINDING_KEYS)
    if unknown:
        raise SpecError(
            f"{entity_name}: source binding declares unknown key(s) {unknown}. "
            f"An unknown key is silently ignored, so a misspelling reads as a setting "
            f"that is not in force. Accepted keys: {sorted(BINDING_KEYS)}")
    try:
        name = str(raw["name"]).strip().upper()
        bronze = str(raw["bronze_table"]).strip()
    except KeyError as exc:
        raise SpecError(f"{entity_name}: source binding missing {exc.args[0]!r}") from exc
    parent_keys = tuple(
        (str(parent), _tuple(cols, f"{entity_name}.parent_keys.{parent}"))
        for parent, cols in (raw.get("parent_keys") or {}).items()
    )
    return SourceBinding(
        name=name,
        bronze_table=bronze,
        key_columns=_tuple(raw.get("key_columns"), f"{entity_name}.key_columns"),
        key_literals=_key_literals(raw.get("key_literals"), f"{entity_name}.key_literals"),
        parent_keys=parent_keys,
        payload=_tuple(raw.get("payload"), f"{entity_name}.payload"),
        applied_dts_column=raw.get("applied_dts_column"),
        cdc_op_column=raw.get("cdc_op_column"),
        manifest_column=raw.get("manifest_column"),
        expectations=_tuple(raw.get("expectations"), f"{entity_name}.expectations"),
        dedup_by=_tuple(raw.get("dedup_by"), f"{entity_name}.dedup_by"),
        dedup_order=_tuple(raw.get("dedup_order"), f"{entity_name}.dedup_order"),
        cast=_cast(raw.get("cast"), f"{entity_name}.cast"),
        # Uppercased like `name`, because hashing.hash_key uppercases the scope before
        # hashing it -- a lowercase declaration would hash identically and then read
        # differently in every document, which is the kind of near-miss that costs an hour.
        key_scope=str(raw.get("key_scope") or "").strip().upper(),
        conform=str(raw.get("conform") or "").strip(),
        freshness_sla_hours=(None if raw.get("freshness_sla_hours") is None
                             else int(raw["freshness_sla_hours"])),
        # Validated by the same rule a physical table version gets, and for the same
        # reason: `stream_generation: "2"` is a typo with a plausible reading, and a
        # plausible reading here silently keeps the old checkpoint.
        stream_generation=(1 if raw.get("stream_generation") is None
                           else _version(raw["stream_generation"],
                                         f"{entity_name}.stream_generation")),
    )


def load_entity(path: Path) -> Entity:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise SpecError(f"{path.name}: file must contain a single mapping")
    try:
        entity = Entity(
            name=str(raw["name"]).strip().lower(),
            kind=str(raw["kind"]).strip().lower(),
            domain=str(raw["domain"]).strip().lower(),
            key_style=str(raw.get("key_style", "federated")).strip().lower(),
            business_keys=_tuple(raw.get("business_keys"), "business_keys"),
            tenant_key=_tuple(raw.get("tenant_key"), "tenant_key"),
            parents=_tuple(raw.get("parents"), "parents"),
            parent_roles=_tuple(raw.get("parent_roles"), "parent_roles"),
            descriptions=tuple(
                (str(k), " ".join(str(v).split()))
                for k, v in (raw.get("descriptions") or {}).items()),
            payload=_tuple(raw.get("payload"), "payload"),
            transaction_key=_tuple(raw.get("transaction_key"), "transaction_key"),
            mas_key=_tuple(raw.get("mas_key"), "mas_key"),
            driving_key=raw.get("driving_key"),
            sensitivity=str(raw.get("sensitivity", "internal")).strip().lower(),
            grain=str(raw.get("grain", "transaction")).strip().lower(),
            aggregates_from=str(raw.get("aggregates_from", "")).strip().lower(),
            aggregate_drops=_tuple(raw.get("aggregate_drops"), "aggregate_drops"),
            masks=tuple((str(k), str(v)) for k, v in (raw.get("masks") or {}).items()),
            accounting=tuple(
                (str(k), str(v)) for k, v in (raw.get("accounting") or {}).items()
            ),
            sources=tuple(_binding(s, str(raw.get("name"))) for s in raw.get("sources", [])),
            change_detection=str(raw.get("change_detection", "cdc")).strip().lower(),
            rulebook_version=str(raw.get("rulebook_version", RULEBOOK_VERSION)),
            version=_version(raw.get("version", 1), path.name),
            notes=str(raw.get("notes", "")),
        )
    except KeyError as exc:
        raise SpecError(f"{path.name}: missing required key {exc.args[0]!r}") from exc
    validate(entity)
    return entity


def load_model(directory: str | Path) -> Model:
    directory = Path(directory)
    files = sorted(directory.glob("*.yml")) + sorted(directory.glob("*.yaml"))
    if not files:
        raise SpecError(f"no entity metadata found in {directory}")
    model = Model([load_entity(f) for f in files])
    _resolve_conformance(model, directory.parent / "source_unions.yml")
    validate_model(model)
    return model


def _resolve_conformance(model: Model, profiles_path: Path) -> None:
    """Fill row_filter and derived_columns on every binding that names a profile.

    ONE DECLARATION, MANY BINDINGS. Six invoice bindings apply the same filter and the same
    twelve renames; copying those into six YAML blocks would be six chances to disagree.
    The profile stays in metadata/source_unions.yml -- where it already was, as a view --
    and is resolved onto the bindings here.

    A binding naming a profile that does not exist is refused: a silently unconformed
    binding reads unfiltered rows under column names that do not resolve, which fails far
    from here.
    """
    import yaml  # noqa: PLC0415 -- only needed when a model is loaded from disk

    if not profiles_path.exists():
        declared: dict = {}
    else:
        raw = yaml.safe_load(profiles_path.read_text(encoding="utf-8")) or {}
        declared = {u["name"]: u for u in (raw.get("unions") or [])}

    for entity in model.entities:
        for binding in entity.sources:
            if not binding.conform:
                continue
            profile = declared.get(binding.conform)
            if profile is None:
                raise SpecError(
                    f"{entity.name}/{binding.name}: conform={binding.conform!r} names no "
                    f"profile in {profiles_path.name}. Declared: "
                    f"{sorted(declared) or 'none'}")
            object.__setattr__(binding, "row_filter",
                               str(profile.get("row_filter") or "").strip())
            object.__setattr__(binding, "derived_columns", tuple(
                (str(k), str(v)) for k, v in
                sorted((profile.get("derived_columns") or {}).items())))
    return


# --------------------------------------------------------------------------- #
# Validation -- every rule fails the build
# --------------------------------------------------------------------------- #
def validate(e: Entity) -> None:
    if not IDENT.match(e.name):
        raise SpecError(f"{e.name!r}: entity names are lower_snake_case, 3-63 chars")

    # An entity is a business concept and must be source-independent. The same entity
    # arrives from many systems; the SOURCE belongs in the sources: binding and in the
    # generated table name, never in the entity name. Baking it in means the name can
    # drift from the binding, and adding a source means copying a whole file.
    declared_sources = {src.name.lower() for src in e.sources}
    for token in declared_sources:
        if e.name.endswith(f"_{token}") or f"_{token}_" in e.name:
            raise SpecError(
                f"{e.name!r}: the entity name contains the source system {token!r}. "
                f"Entities are source-independent -- declare the source in sources: and "
                f"let the generated table be {naming.PREFIX[e.kind]}{e.name}_{token}. "
                f"Rename the entity to the business concept alone."
            )

    # DEF-42: a keyed table needs ONE row per hash key, and several sources supplying the
    # same key is how that breaks. A MULTI-SOURCE KEYED ENTITY IS NOW BUILDABLE FOR EVERY
    # KEYED KIND, which it was not until 29 September 2026.
    #
    # Until then, only a hub was loaded through a staging log and an anti-join; a link, an
    # NHL and a HAL were written straight into their table by one append flow per source,
    # and two flows supplying the same hash key could not see each other's rows. A refusal
    # stood here -- "a {kind} declares N sources ... this kind must be added to
    # naming.STAGED_KINDS and given a loader in checks/load_hubs.py" -- naming as its fix
    # exactly what this change did. naming.STAGED_KINDS is now naming.KEYED_KINDS union
    # naming.SATELLITE_KINDS, so its own condition (`keyed and NOT staged`) was false for
    # every kind the model can declare and it could no longer fire for any input. A
    # refusal that cannot fire reads as protection and is not, so it was removed rather
    # than left to be trusted.
    #
    # WHAT ACTUALLY PROTECTS THE PROPERTY NOW: both flows append to the `stg_` log, and
    # checks/load_hubs.py inserts into the vault table with NOT EXISTS against the keys
    # already there -- so the second source's rows for a key the first already landed are
    # dropped at load, per kind, for all four. The invariant that makes that true is
    # `naming.KEYED_KINDS - naming.STAGED_KINDS == frozenset()`, asserted in
    # tests/test_accelerator.py; un-stage a keyed kind and that check reds. See
    # docs/superpowers/specs/2026-08-25-hub-deduplication-design.md.

    # --- the ARB boundary rule, enforced in code -----------------------------
    if e.kind not in naming.GENERATABLE:
        raise SpecError(
            f"{e.name}: kind {e.kind!r} is not generatable. Control plane (ctl_), "
            f"config estate (ref_), registries (reg_/sal_) and derivations (agg_) are "
            f"platform-built per the ARB boundary rule; the generator consumes them by "
            f"join and never creates them."
        )
    if naming.is_platform_owned(e.base_table):
        raise SpecError(f"{e.base_table}: resolves to a platform-owned prefix")

    if e.key_style not in ("federated", "authored", "tenant_scoped"):
        raise SpecError(f"{e.name}: key_style must be federated|authored|tenant_scoped")
    if e.change_detection != "cdc":
        raise SpecError(
            f"{e.name}: change_detection must be 'cdc' -- got "
            f"{e.change_detection!r}. 'antijoin' was accepted here until 29 September "
            f"2026 and implemented in no code path: no flow read it, no entity declared "
            f"it, and its only effect was to leave factory.py's `changed_only` False. "
            f"The anti-join it named is real and lives elsewhere -- "
            f"checks/load_hubs.py inserts with NOT EXISTS against the target table, for "
            f"every kind in naming.KEYED_KINDS, reading the staging log. A satellite's "
            f"change detection is Bronze's change stream; there is no second mode."
        )
    if e.grain not in ("transaction", "aggregate"):
        raise SpecError(f"{e.name}: grain must be transaction|aggregate")
    if e.grain == "aggregate" and not e.aggregates_from:
        raise SpecError(
            f"{e.name}: grain is 'aggregate' but aggregates_from is not declared. An "
            f"aggregate with no stated transaction-grain counterpart cannot be reconciled, "
            f"and a consumer has no way to know it must not be read per-entity."
        )
    if e.aggregates_from and e.grain != "aggregate":
        raise SpecError(
            f"{e.name}: aggregates_from is declared but grain is {e.grain!r}. Set "
            f"grain: aggregate."
        )
    if e.sensitivity not in ("internal", "personal", "financial", "restricted"):
        raise SpecError(f"{e.name}: unknown sensitivity {e.sensitivity!r}")
    if e.rulebook_version != RULEBOOK_VERSION:
        raise SpecError(
            f"{e.name}: pinned to hash rulebook {e.rulebook_version} but the repository "
            f"is at {RULEBOOK_VERSION}. Re-keying an entity is a migration, not an edit."
        )
    if not e.sources:
        raise SpecError(f"{e.name}: at least one source binding is required")

    dupes = {s.name for s in e.sources if [x.name for x in e.sources].count(s.name) > 1}
    if dupes:
        raise SpecError(f"{e.name}: duplicate source names {sorted(dupes)}")

    # _raw receives overlapping full extracts, so a source may need to collapse
    # re-delivered rows before they reach the loader. dedup_order is mandatory whenever
    # dedup_by is set: a non-deterministic winner changes hashes between runs, which is
    # unrecoverable once rows are loaded.
    for src in e.sources:
        if src.dedup_by and not src.dedup_order:
            raise SpecError(
                f"{e.name}/{src.name}: dedup_by requires dedup_order -- a "
                f"non-deterministic winner changes hashes between runs"
            )

    # LOOP-1 READS A DELIVERY COUNT AS AN APPROVED COUNT, and that is only sound where the
    # loader takes delivered rows one for one. loop1_reconciliation asserts
    # landed + (quarantined - superseded) = approved, with `approved` supplied from outside
    # silver -- otherwise it is derived from the very rows it is compared against and the
    # identity holds by construction, which is DEF-48's vacuous gate in another form.
    #
    # A deduplicating binding breaks the substitution: _raw carries ~1.8 copies of every
    # gl20000 business row across 7 deliveries, so a delivered count sits ~80% above what the
    # loader accepts. The gate would fail permanently on a CORRECT pipeline and read as a gate
    # bug rather than a modelling one. Measured 28 Aug 2026: no binding declares both today,
    # which is exactly why the substitution works -- so it is refused rather than left to luck.
    if e.kind in RECONCILABLE_KINDS_FOR_MANIFEST:
        for src in e.sources:
            if src.dedup_by and src.manifest_column:
                raise SpecError(
                    f"{e.name}/{src.name}: a reconcilable binding ({e.kind}) may not "
                    f"declare both dedup_by and manifest_column. loop-1 compares a "
                    f"delivered count against what the loader accepted, and dedup makes "
                    f"those different numbers, so the gate would fail on a correct load. "
                    f"Drop one, or give loop-1 a count taken after deduplication"
                )

    if e.kind == "hub":
        if not e.business_keys:
            raise SpecError(f"{e.name}: a hub must declare business_keys")
        if e.key_style == "tenant_scoped":
            if not e.tenant_key:
                raise SpecError(
                    f"{e.name}: key_style is tenant_scoped but tenant_key is not declared. "
                    f"Name the business key column(s) carrying the tenant. Without it the "
                    f"key cannot separate two clients whose source references collide -- "
                    f"and that failure is silent, not an error."
                )
            missing = [c for c in e.tenant_key if c not in e.business_keys]
            if missing:
                raise SpecError(
                    f"{e.name}: tenant_key {missing} is not part of business_keys. The "
                    f"tenant must be IN the key, not merely carried alongside it."
                )
        elif e.tenant_key:
            raise SpecError(
                f"{e.name}: tenant_key is declared but key_style is {e.key_style!r}. "
                f"Set key_style: tenant_scoped, or remove tenant_key."
            )
        if e.payload:
            raise SpecError(
                f"{e.name}: a hub carries the business key and nothing else. "
                f"Move {list(e.payload)} to a satellite."
            )
        for s in e.sources:
            unknown_literals = [k for k, _ in s.key_literals if k not in e.business_keys]
            if unknown_literals:
                raise SpecError(
                    f"{e.name}/{s.name}: key_literals names {unknown_literals} that are "
                    f"not in business_keys {list(e.business_keys)}"
                )
            if len(s.key_columns) + len(s.key_literals) != len(e.business_keys):
                raise SpecError(
                    f"{e.name}/{s.name}: key_columns plus key_literals must map 1:1 to "
                    f"business_keys ({len(s.key_columns)} key_columns + "
                    f"{len(s.key_literals)} key_literals vs {len(e.business_keys)} "
                    f"business_keys)"
                )

    if e.kind in naming.LINK_KINDS:
        if len(e.parents) < 2:
            raise SpecError(f"{e.name}: a {e.kind} needs at least two parents")

        # --- roles ------------------------------------------------------------
        # THE RULE THAT MAKES A HIERARCHICAL LINK POSSIBLE, and the one whose absence was
        # not an error but a wrong table. Measured 5 Sep 2026 on a self-referencing hal
        # with no roles: it loaded, emitted ONE `legal_entity_hk` column where two legs
        # were declared, and hashed its link key over the child business key twice. Every
        # row would have said a node is its own parent, and nothing would have failed.
        if e.parent_roles and len(e.parent_roles) != len(e.parents):
            raise SpecError(
                f"{e.name}: parent_roles has {len(e.parent_roles)} entries for "
                f"{len(e.parents)} parents. Roles are positional -- a short list silently "
                f"unroles the trailing legs, and an unroled leg reverts to the bare hub "
                f"column name and collides with its sibling."
            )
        if len(set(e.parents)) != len(e.parents):
            if not e.parent_roles:
                raise SpecError(
                    f"{e.name}: {e.kind} names the same hub more than once "
                    f"({', '.join(sorted(set(p for p in e.parents if e.parents.count(p) > 1)))}) "
                    f"and declares no parent_roles. Both legs would be written to one "
                    f"foreign-key column and the link key would hash one leg twice -- a "
                    f"hierarchy in which every node is its own parent, built without an "
                    f"error. Declare a role per leg, e.g. parent_roles: [parent, child]."
                )
        if e.parent_roles:
            if len(set(e.parent_roles)) != len(e.parent_roles):
                raise SpecError(
                    f"{e.name}: parent_roles must be distinct; got {list(e.parent_roles)}. "
                    f"Two legs sharing a role name one column between them."
                )
            for role in e.parent_roles:
                if not IDENT.match(role):
                    raise SpecError(
                        f"{e.name}: parent role {role!r} is not a valid identifier. A role "
                        f"prefixes a generated column name."
                    )
        if e.kind == "link" and e.payload:
            raise SpecError(
                f"{e.name}: links carry keys only. Descriptive attributes belong on a "
                f"link satellite; immutable transaction payload belongs on an nhl."
            )
        for src in e.sources:
            # KEYED BY LEG. `dict(src.parent_keys).get(parent)` is the exact lookup that
            # collapsed a hierarchical link's two legs into one -- on a repeated hub both
            # legs resolve to the same entry, so a binding that mapped only the child
            # satisfied a check asking about the parent.
            for parent, role in parent_legs(e):
                cols = dict(src.parent_keys).get(role or parent)
                if not cols:
                    raise SpecError(
                        f"{e.name}/{src.name}: parent_keys must map "
                        f"{(role or parent)!r} to the source columns carrying "
                        f"{'that leg' if role else 'its'} business key. With more than one "
                        f"parent a flat key_columns list is ambiguous"
                        + (f", and a roled leg is mapped by its ROLE ({role!r}), not by "
                           f"its hub ({parent!r})." if role else ".")
                    )
        if e.kind == "nhl":
            if not e.transaction_key:
                raise SpecError(
                    f"{e.name}: an nhl must declare transaction_key (the source's own "
                    f"transaction reference plus any dependent child key such as "
                    f"line_no). Parent hub keys alone are not unique -- the same worker "
                    f"and client transact every week."
                )
            if not e.payload:
                raise SpecError(f"{e.name}: an nhl with no payload should be a link")

    if e.kind in naming.SATELLITE_KINDS:
        if len(e.parents) != 1:
            raise SpecError(f"{e.name}: a {e.kind} hangs off exactly one parent")
        # A satellite may hang off a LINK as well as a hub -- a link satellite. Worktags
        # and external codes belong to a journal LINE, not to any one hub, so forbidding
        # this would force them onto the link itself and break its immutability.
        if e.kind != "esat" and not e.payload:
            raise SpecError(f"{e.name}: a {e.kind} must declare a payload")
        if e.kind == "csat" and len(e.sources) != 1:
            raise SpecError(f"{e.name}: a computed satellite reads one input")
        # DEF-55: every satellite binding must carry an ORDERING SOURCE. The loader
        # decides which of several versions of a key delivered in one batch is newest,
        # and assigns the sub_seq that keeps them distinct. Its only ordering evidence is
        # applied_dts -- load_dts is one current_timestamp() for the whole batch, so it
        # ties. Without applied_dts, ordering falls back to arbitrary and the versions
        # collide at (parent_hk, load_dts, sub_seq): 1,358 keys collided on the first
        # real satellite load, and append_only_check went red. Refuse the binding rather
        # than load an order nobody chose.
        #
        # A binding that reads a VAULT table is exempt by construction, not by waiver:
        # applied_dts is a system column there, so the ordering evidence is already
        # present and naming it again would be redundant.
        for src in e.sources:
            if not src.applied_dts_column and not naming.reads_vault(src.bronze_table):
                raise SpecError(
                    f"{e.name}: source {src.name} declares no applied_dts_column. A "
                    f"satellite needs one to order versions delivered in the same batch "
                    f"-- load_dts is one timestamp for the whole batch and cannot break "
                    f"the tie. Name the source column that says when the row changed."
                )
        # A raw satellite may declare MANY sources. Each produces its own table
        # (<base>_<source>), so the one-source-per-satellite guarantee holds by
        # construction. What is forbidden is a source name that would collide.
        if e.kind in naming.RAW_SATELLITE_KINDS:
            names = [src.name.lower() for src in e.sources]
            if len(set(names)) != len(names):
                raise SpecError(f"{e.name}: duplicate source names would collide as tables")
            for src in e.sources:
                if not SOURCE_IDENT.match(src.name.lower()):
                    raise SpecError(
                        f"{e.name}: source name {src.name!r} cannot be a table-name "
                        f"suffix. Use letters, digits and underscores, starting with a "
                        f"letter, 2-40 characters -- the generated table is "
                        f"{e.base_table}_{src.name.lower()}."
                    )
        if e.kind == "msat" and not e.mas_key:
            raise SpecError(
                f"{e.name}: a multi-active satellite needs mas_key -- a natural sub-key "
                f"(skill_code, phone_type) in preference to a generated sequence, which "
                f"breaks whenever the source reorders its rows."
            )
        # --- freshness ---------------------------------------------------
        # A value that cannot be a duration is a typo, and a typo here is silent: the
        # check would compare against a nonsense threshold and pass or fail arbitrarily.
        # 8760 hours is a year -- anything longer is not an SLA, it is an exemption, and
        # exemptions are written as 0 so they read as a decision rather than a slip.
        for src in e.sources:
            h = src.freshness_sla_hours
            if h is None:
                continue
            if h < 0 or h > 8760:
                raise SpecError(
                    f"{e.name}/{src.name}: freshness_sla_hours={h} is not a duration. "
                    f"Use 0 to declare the binding exempt, or a value up to 8760 (a year)."
                )

        if e.kind == "esat" and not e.driving_key:
            raise SpecError(f"{e.name}: an effectivity satellite must name its driving_key")

    # --- accounting roles ----------------------------------------------------
    # Declaring a role is optional; declaring a WRONG one is not survivable, because the
    # gate that consumes it would name a column that does not exist and assert nothing.
    acct = dict(e.accounting)
    unknown_roles = [r for r in acct if r not in ACCOUNTING_ROLES]
    if unknown_roles:
        raise SpecError(
            f"{e.name}: accounting declares unknown role(s) {sorted(unknown_roles)}. "
            f"Known roles are {list(ACCOUNTING_ROLES)}."
        )
    columns_here = set(e.payload) | set(e.transaction_key)
    if ("debit" in acct) != ("credit" in acct):
        raise SpecError(
            f"{e.name}: accounting declares only one of debit/credit. Both sides are "
            f"needed or the balance assertion cannot be made at all -- and half an "
            f"assertion reads as coverage while providing none."
        )
    if "debit" in acct and "journal" not in acct:
        raise SpecError(
            f"{e.name}: accounting declares debit/credit but no journal role. The gate "
            f"has to know which PARENT groups lines into one journal before it can sum "
            f"them; without it there is nothing to balance."
        )
    if "journal" in acct and acct["journal"] not in e.parents:
        raise SpecError(
            f"{e.name}: accounting.journal names {acct['journal']!r}, which is not one "
            f"of this entity's parents {list(e.parents)}."
        )
    for role in ("debit", "credit", "line_order"):
        if role in acct and acct[role] not in columns_here:
            raise SpecError(
                f"{e.name}: accounting.{role} names {acct[role]!r}, which is not in this "
                f"entity's payload or transaction_key. The generator renames nothing, so "
                f"the role must name a column the source actually delivers."
            )
    if "control_total" in acct and acct["control_total"] not in e.payload:
        raise SpecError(
            f"{e.name}: accounting.control_total names {acct['control_total']!r}, which "
            f"is not in this entity's payload. The control total is declared on whatever "
            f"entity CARRIES it -- the journal header satellite -- not on the line."
        )
    if acct and set(acct) == {"journal"}:
        raise SpecError(
            f"{e.name}: accounting declares only a journal role and no columns. Either "
            f"name the roles this entity plays, or remove the block."
        )

    # masks must name real payload columns
    payload_cols = set(e.payload) | set(e.transaction_key)
    for column, fn in e.masks:
        if column not in payload_cols:
            raise SpecError(
                f"{e.name}: mask declared for {column!r}, which is not in this entity's "
                f"payload. Masks are declared per column and the column must exist."
            )
        if not fn.startswith("governance.mask_"):
            raise SpecError(
                f"{e.name}: mask function {fn!r} must live in the governance schema and "
                f"be named mask_* so apply_masks.sql and the conformance gate can find it."
            )

    # sensitive data without a declared mask is the failure mode that ships silently
    if e.sensitivity in ("personal", "financial", "restricted") and not e.masks:
        raise SpecError(
            f"{e.name}: sensitivity is {e.sensitivity!r} but no masks are declared. "
            f"Add a masks: block, or set sensitivity to internal if the payload really "
            f"is not sensitive. Silent unmasked personal data is the outcome this rule "
            f"exists to prevent."
        )


def validate_key_scopes(model: Model) -> None:
    """Every declared key_scope must name a binding that exists somewhere in the model.

    WHY MODEL-WIDE AND NOT PER ENTITY. A key_scope points AWAY from the entity declaring
    it -- it says "hash my parent keys as the FIELDGLASS_EU feed does" -- so the thing it
    has to agree with lives in another file. Nothing an entity can check about itself would
    catch `key_scope: FIELDGLAS_EU`.

    AND A TYPO THERE IS SILENT. It does not fail, and it does not fall back: it produces a
    hash over a different literal, which is a well-formed BINARY(32) that simply never
    equals any key the parent builds. The declaration exists to FIX exactly that failure,
    so a typo in it would reintroduce the defect through the mechanism meant to remove it.
    """
    declared = {src.name for e in model.entities for src in e.sources}
    for e in model.entities:
        for src in e.sources:
            if not src.key_scope:
                continue
            if src.key_scope not in declared:
                raise SpecError(
                    f"{e.name}/{src.name}: key_scope {src.key_scope!r} names no source "
                    f"binding in the model. Declared bindings are {sorted(declared)}. A "
                    f"scope nobody produces hashes to a well-formed key that never joins, "
                    f"which is the defect key_scope exists to fix."
                )
            if src.key_scope == src.name:
                raise SpecError(
                    f"{e.name}/{src.name}: key_scope {src.key_scope!r} is the binding's "
                    f"own name, which is already the default. Remove it -- a no-op "
                    f"declaration reads as a deliberate override and invites the next "
                    f"reader to preserve it."
                )


# THE HUB THE SPLIT REPLACED. hub_organisation became hub_client and
# hub_operating_company on 29 September 2026; the name is refused here so it cannot come
# back. See SPLIT_REPLACED_BY below for why a partial revert is worse than no revert.
SPLIT_RETIRED_ENTITY = "organisation"
SPLIT_REPLACED_BY = ("hub_client", "hub_operating_company")


def validate_model(model: Model) -> None:
    # A PARTIAL REVERT IS THE WORST OUTCOME AVAILABLE, AND IT FAILS NOTHING.
    #
    # hub_organisation held two different kinds of party under one vague word: of its
    # thirteen rows in usnc_tds, ONE was a client (AEE1, Ameren) and eleven were Guidant's
    # own operating companies. It split into hub_client and hub_operating_company on the
    # discriminator every binding already carried -- the `reference_type` key literal.
    #
    # Re-declaring `organisation` NEXT TO the two halves is the state this refuses. It
    # gives three hubs where the third holds the union of the other two, and every child
    # is then free to point at whichever it likes. Nothing detects that: both hash keys
    # are well formed, both tables load, append_only and reconciliation both still pass --
    # the rows simply split across two hubs and no join ever says so. That is strictly
    # worse than either the pre-split conflation (one wrong hub, but one) or the split
    # (two right hubs), because it is the only one of the three that cannot be read off
    # the model.
    #
    # REFUSED OVER THE MODEL, NOT THE ENTITY, and the distinction is load-bearing. The
    # NAME is not the defect -- a unit test may legitimately build an entity called
    # `organisation` to exercise literal-keyed parents, and tests/test_accelerator.py
    # does, through spec.load_entity. What is refused is a loaded MODEL carrying it, which
    # is the only form an author can deploy.
    retired = [e.base_table for e in model.entities if e.name == SPLIT_RETIRED_ENTITY]
    if retired:
        raise SpecError(
            f"{SPLIT_RETIRED_ENTITY!r}: this entity was SPLIT on 29 September 2026 and "
            f"may not be re-declared ({', '.join(retired)}). It became "
            f"{SPLIT_REPLACED_BY[0]} -- the reference types Fieldglass_Buyer_Code and "
            f"Portal_Client_Code, somebody else's company -- and {SPLIT_REPLACED_BY[1]} "
            f"-- the reference type Organization_Reference_ID, one of ours. One vague "
            f"word held both, and it was true of one row in thirteen. Declaring it again "
            f"alongside the two halves is a PARTIAL REVERT: three hubs, the third holding "
            f"the union of the other two, every child free to point at whichever it "
            f"likes -- and it fails nothing, because both keys are well formed and the "
            f"rows just split with no join to say so. Point the entity at "
            f"{SPLIT_REPLACED_BY[0]} or {SPLIT_REPLACED_BY[1]} by its reference_type "
            f"literal, exactly as the split did."
        )

    validate_key_scopes(model)
    names = [name for e in model.entities for _, name in e.tables()]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise SpecError(f"duplicate table names in metadata: {dupes}")

    hubs = {e.name for e in model.by_kind("hub")}
    linkish = {e.name for e in model.by_kind(*naming.LINK_KINDS)}
    for e in model.entities:
        if e.kind == "hub":
            continue
        satellite = e.kind in naming.SATELLITE_KINDS
        for parent in e.parents:
            if parent in hubs:
                continue
            if parent in linkish:
                if satellite:
                    # LINK SATELLITE. Legitimate and sometimes forced: worktags and
                    # external codes belong to a journal LINE, not to any one hub.
                    continue
                raise SpecError(
                    f"{e.base_table}: parent {parent!r} is a link, not a hub. Chaining "
                    f"links breaks the unit of work -- one row must be readable as a "
                    f"complete business event. Declare all participating hubs on "
                    f"{e.base_table} instead."
                )
            raise SpecError(
                f"{e.base_table}: parent {parent!r} is not a declared hub or link"
            )

    # ---- grain integrity -----------------------------------------------------
    by_name = {e.name: e for e in model.entities}
    for e in model.entities:
        if e.grain != "aggregate":
            continue
        raw_entity = by_name.get(e.aggregates_from)
        if raw_entity is None:
            raise SpecError(
                f"{e.base_table}: aggregates_from {e.aggregates_from!r} is not a declared "
                f"entity. Either add the transaction-grain entity, or if it is genuinely "
                f"not yet modelled, say so in notes and remove aggregates_from -- but then "
                f"the reconciliation control cannot exist."
            )
        if raw_entity.grain != "transaction":
            raise SpecError(
                f"{e.base_table}: aggregates_from points at {raw_entity.base_table}, which "
                f"is itself an aggregate. Reconciling an aggregate against an aggregate "
                f"proves nothing."
            )
        # An aggregate legitimately has DIFFERENT parents from its raw counterpart: the
        # journal line has a ledger account the register does not, and the register has a
        # worker the journal does not. What must be stated is which parents the
        # aggregation DROPS -- that is the fact a consumer cannot infer and will otherwise
        # assume away.
        if not e.aggregate_drops:
            raise SpecError(
                f"{e.base_table}: an aggregate must declare aggregate_drops -- the parents "
                f"of {raw_entity.base_table} that are summed away. An aggregate that drops "
                f"nothing is not an aggregate."
            )
        not_in_raw = [p for p in e.aggregate_drops if p not in raw_entity.parents]
        if not_in_raw:
            raise SpecError(
                f"{e.base_table}: aggregate_drops names {not_in_raw}, which "
                f"{raw_entity.base_table} does not have as parents. You can only drop a "
                f"grain the transaction-level entity actually carries."
            )
        still_declared = [p for p in e.aggregate_drops if p in e.parents]
        if still_declared:
            raise SpecError(
                f"{e.base_table}: aggregate_drops names {still_declared} but they are also "
                f"declared as parents. A total that has been summed across workers cannot "
                f"also be attached to a worker -- that is the exact confusion this label "
                f"exists to prevent."
            )

    if not hubs:
        raise SpecError("a model with no hubs is not a Data Vault")


# --------------------------------------------------------------------------- #
# ACTIVE SOURCES -- which declared bindings actually LOAD in this lake
#
# Parent spec decision D5. Every lake declares the whole model (D2), but a source is
# only in Bronze in some of them: `hfig_eu.bronze.striive_job_request` does not exist in
# the US lake, and a flow reading it fails the pipeline AT DEFINITION TIME -- taking
# every other flow down with it, including the ones whose tables are present.
#
# So the factory splits its two emissions (see factory.build):
#
#     create_streaming_table()   every declared binding, every lake -> inventory identical
#     append_flow()              active bindings only               -> nothing to fail
#
# An inactive binding yields an EMPTY table, never a missing one, so cross-region
# conformance still compares like with like.
#
# ACTIVITY IS DECLARED, NOT PROBED. spark.catalog.tableExists() would be less typing and
# strictly worse: it cannot tell a source absent by design from one absent by accident --
# a typo, an unprovisioned schema or a missing grant would all yield a silently empty
# table and a green pipeline. Declaring it makes absence a reviewed decision, and makes a
# genuinely missing table loud.
#
# TWO GRANULARITIES, because one is not enough for the real model. A bare source name
# (GP_US) activates every binding carrying that name. That is the common case and the
# shape D5 describes. But a source name is NOT globally uniform: UKG_US names a REAL
# table on hub_organisation and a PLACEHOLDER on sat_accounting_journal_header, and 3a
# defers every satellite. So a binding may also be named `entity/SOURCE`
# (journal_line/UKG_US), which activates exactly that one. Both forms are validated
# against the metadata and an entry matching nothing is REFUSED -- a typo in this list
# would otherwise drop a source silently, which is the failure the whole mechanism
# exists to prevent.
# --------------------------------------------------------------------------- #
def binding_id(entity: "Entity", src: SourceBinding) -> str:
    """Stable identifier for one entity/source binding, e.g. 'organisation/GP_US'."""
    return f"{entity.name}/{src.name.upper()}"


def resolve_active_sources(
    model: Model, declared: str | Iterable[str] | None
) -> frozenset[str] | None:
    """Turn a declared activity list into the set of ACTIVE binding ids.

    Returns None when nothing is declared -- meaning EVERY binding is active, so an
    existing target that sets no value keeps behaving exactly as it did.

    Accepts a comma-separated string (bundle configuration arrives that way) or any
    iterable of entries. Each entry is either a bare source name or `entity/SOURCE`.

    Raises SpecError naming every entry that matches no binding. This is deliberately
    fatal: an unmatched entry means either a typo (and a source silently dropped) or a
    binding that has been renamed or removed since the target was configured.
    """
    if isinstance(declared, str):
        declared = declared.split(",")
    entries = [str(d).strip() for d in (declared or []) if str(d).strip()]
    if not entries:
        return None

    all_ids = {binding_id(e, s) for e in model.entities for s in e.sources}
    by_source: dict[str, set[str]] = {}
    for e in model.entities:
        for s in e.sources:
            by_source.setdefault(s.name.upper(), set()).add(binding_id(e, s))

    active: set[str] = set()
    unmatched: list[str] = []
    for entry in entries:
        if "/" in entry:
            ent, _, name = entry.partition("/")
            one = f"{ent.strip().lower()}/{name.strip().upper()}"
            if one in all_ids:
                active.add(one)
            else:
                unmatched.append(entry)
        else:
            matched = by_source.get(entry.upper())
            if matched:
                active |= matched
            else:
                unmatched.append(entry)

    if unmatched:
        raise SpecError(
            f"active_sources names {sorted(unmatched)}, which match no source binding in "
            f"the metadata. Declared source names are {sorted(by_source)}; a binding may "
            f"also be named entity/SOURCE, e.g. "
            f"{sorted(all_ids)[0] if all_ids else 'organisation/GP_US'}. An entry that "
            f"matches nothing is refused rather than ignored: ignoring it would silently "
            f"drop the source it was meant to activate."
        )
    return frozenset(active)


def is_source_active(
    active: frozenset[str] | None, entity: "Entity", src: SourceBinding
) -> bool:
    """Whether this binding emits an append flow in this lake. None means all active."""
    return active is None or binding_id(entity, src) in active


def table_bindings(
    entity: "Entity", src: SourceBinding | None
) -> tuple[SourceBinding, ...]:
    """The bindings feeding ONE emitted table, given entity.tables()' (src, table) pair.

    A hub, link or NHL is one conformed table fed by EVERY binding. A satellite is one
    table PER source, so its table is fed by exactly the one binding entity.tables()
    handed back. Both the factory and the post-run gates need this distinction, and they
    must not each invent it: "is this table active" is per TABLE, not per entity.
    """
    return tuple(entity.sources) if src is None else (src,)


def active_table_bindings(
    entity: "Entity", src: SourceBinding | None, active: frozenset[str] | None
) -> list[SourceBinding]:
    """The bindings that actually load into one emitted table. Empty = the table is
    declared, created, and loads nothing in this lake.

    This is THE definition of an inactive table, and it is deliberately in one place:
    factory.build decides what to emit from it, and checks/loop1_reconciliation.py,
    checks/mask_survival_check.py and checks/journal_integrity_check.py all decide what
    they can assert from the SAME function. Two notions of "inactive" would mean a gate
    asserting something the generator never emitted -- which is exactly how a missing
    qtn_ table came to fail a hard gate.
    """
    return [b for b in table_bindings(entity, src) if is_source_active(active, entity, b)]


def inactive_bindings(model: Model, active: frozenset[str] | None) -> list[str]:
    """Binding ids that are declared but NOT active here, sorted -- for the deploy log.

    The entry point prints this. A skipped flow that nobody reports is indistinguishable
    from a flow that ran and loaded nothing.
    """
    if active is None:
        return []
    return sorted(
        binding_id(e, s)
        for e in model.entities
        for s in e.sources
        if binding_id(e, s) not in active
    )


def check_payload_order(previous: Iterable[str], current: Iterable[str]) -> None:
    """Guard against payload reordering, which invalidates every stored hashdiff.

    Called by CI with the payload from the previous commit. Additions are allowed
    only at the end; reordering or removal is a migration.
    """
    prev, cur = list(previous), list(current)
    if cur[: len(prev)] != prev:
        raise SpecError(
            "satellite payload reordered or truncated. Existing columns must keep their "
            "order and new columns must be appended at the END, so that right-trimming "
            "of trailing null tokens keeps previously stored hashdiffs valid. "
            f"was={prev} now={cur}"
        )
