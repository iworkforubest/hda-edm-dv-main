"""What a control schema must contain, in any layer.

THE SINGLE AUTHORITY. Silver's governance/control_objects.sql is VERIFIED against this rather
than generated from it -- that DDL is deployed, gated and working, and rewriting it to prove a
point about authority would risk it for no functional gain. Gold's DDL and the bronze contract
ARE generated from here, because gold does not exist yet and bronze is another team's catalog.

WHY CONTROL AND NOT GOVERNANCE. The audit is uniform: every layer takes rows in, writes some
out, and drops the difference for a reason. Masking is not. databricks.yml records that
bronze's `<source>` schemas carry PHYSICAL PII masking -- values rewritten -- while silver uses
Unity Catalog COLUMN masks, values preserved and access evaluated per reader. A
physically-masked value cannot be revealed to a privileged reader and a column-masked one can.
Standardising those together would define that difference away.
"""

from __future__ import annotations

LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")

# THE MANDATORY CORE. Identical in shape in every layer, because the invariant
# staged = accepted + sum(discarded) per (job_run_id, table_name) is what makes the core worth
# mandating rather than suggesting -- verified live across all six silver tables on 27 Aug with
# zero imbalance.
CORE: dict[str, dict[str, str]] = {
    "aud_load_run": {
        "job_run_id": "STRING",
        "phase": "STRING",
        "target": "STRING",
        "active_sources": "STRING",
        "recorded_at": "TIMESTAMP",
    },
    "aud_table_load": {
        "job_run_id": "STRING",
        "pipeline_update_id": "STRING",
        "table_name": "STRING",
        "written_by": "STRING",
        "staged": "BIGINT",
        "accepted": "BIGINT",
        "recorded_at": "TIMESTAMP",
    },
    "aud_table_discard": {
        "job_run_id": "STRING",
        "table_name": "STRING",
        "discard_reason": "STRING",
        "discarded": "BIGINT",
        "recorded_at": "TIMESTAMP",
    },
}

# LAYER-SPECIFIC, declared but not mandated everywhere.
#
# The manifest is BRONZE'S. Each layer records what IT did, and the downstream layer reads
# upstream's -- so Bronze records deliveries in its own control schema rather than reaching
# into silver's, and silver's loop-1 reads it from there. That is why the Bronze request
# changes; see the spec's section 9.
#
# Gold declares nothing of its own yet: a projection layer has no rejects to supersede and no
# expectations until someone declares them. An empty dict is the honest statement, not an
# omission.
LAYER_TABLES: dict[str, dict[str, dict[str, str]]] = {
    "bronze": {
        "ctl_delivery_manifest": {
            "manifest_id": "STRING",
            "source_system": "STRING",
            "delivered_count": "BIGINT",
            "delivered_at": "TIMESTAMP",
            "delivered_by": "STRING",
        },
        # RUN IDENTITY, SO THE CONTROLS HAVE SOMETHING TO GROUP BY. Three controls in this
        # repo currently assert nothing because a row landed in Bronze carries no statement
        # of WHICH delivery put it there. This table is that statement: one row per
        # registered run, written once, never rewritten.
        #
        # etl_run_id IS AN OPAQUE UUID AND NOTHING MAY PARSE IT. An earlier revision
        # specified a composite `<source_system>|<timestamp>|<mechanism>|<token>`, and it was
        # withdrawn for two reasons. It put the MECHANISM inside the identity, which defeats
        # technology independence in the one artefact that exists to carry it -- a run
        # re-ingested through a different mechanism would get a different id for the same
        # delivery. And `|` collides with this repo's hashing conventions: hashing.DELIMITER
        # is `||`, NULL_TOKEN is `^^`, and hashing.py emits a key_no_delimiter_* rule that
        # REJECTS `||` in key text, so a composite id would be rejected by a gate we already
        # ship. source_system, started_at and ingestion_mechanism are COLUMNS. The id carries
        # nothing.
        #
        # ingestion_mechanism, producer AND mechanism_native_run_id ARE OPERATOR-FACING ONLY.
        # They answer "who do I call" and "where is their log", and they must never appear in
        # identity, in reconciliation or in control flow -- the moment a control branches on
        # the mechanism, the framework stops being technology-independent and every new
        # mechanism becomes a code change. mechanism_native_run_id is NULLABLE by intent: a
        # mechanism that issues no run id of its own is still a first-class member here.
        "ctl_etl_run": {
            "etl_run_id": "STRING",
            "source_system": "STRING",
            "ingestion_mechanism": "STRING",
            "mechanism_native_run_id": "STRING",
            "producer": "STRING",
            "started_at": "TIMESTAMP",
            "created_at": "TIMESTAMP",
        },
        # THE STATUS OF A RUN IS AN EVENT STREAM, NOT A COLUMN. A status column on
        # ctl_etl_run would have to be UPDATED, which makes the run table mutable and throws
        # away the one thing an audit is for. Appending an event keeps the whole history --
        # and a run that never reached a terminal status stays visible AS unterminated rather
        # than being indistinguishable from one that completed.
        #
        # TWO CLOCKS, DELIBERATELY. status_at is the MECHANISM's clock and is what the
        # operator sees in their own tool. recorded_at is OURS, and it is the ordering
        # column: clock skew on a source host would otherwise let a STARTED event sort after
        # a COMPLETED one. The current-status view that depends on this arrives in branch 1
        # task 3.
        "ctl_etl_run_status": {
            "etl_run_id": "STRING",
            "status": "STRING",
            "status_at": "TIMESTAMP",
            "recorded_at": "TIMESTAMP",
            "written_by": "STRING",
        },
    },
    "silver": {
        "ctl_approval_manifest": {
            "manifest_id": "STRING",
            "approved_count": "BIGINT",
            "source_system": "STRING",
            "approved_at": "TIMESTAMP",
            "approved_by": "STRING",
        },
        "ref_dq_expectation": {
            "dataset": "STRING",
            "rule_name": "STRING",
            "rule_sql": "STRING",
            "is_current": "BOOLEAN",
            "severity": "STRING",
        },
        "ctl_quarantine_superseded": {
            "manifest_id": "STRING",
            "table_name": "STRING",
            "reject_digest": "STRING",
            "rulebook_version": "STRING",
            "superseded_by": "STRING",
            "reason": "STRING",
            "recorded_at": "TIMESTAMP",
        },
        # SILVER'S ASSERTION ABOUT BRONZE, HELD IN SILVER. Bronze's catalog is another
        # team's and carries no control schema at all, so the record of what our gate
        # concluded about their delivery lives in ours.
        #
        # ONE ROW PER (job_run_id, contract_table), NOT PER FINDING. A per-finding grain
        # cannot say "checked, nothing wrong", and a table holding only failures leaves the
        # coverage denominator empty -- DEF-48's shape, at dashboard scale. Detail comes
        # from exploding findings, as meta_vault_model.generated_tables already does.
        #
        # status IS STORED, NOT DERIVED. A dashboard recomputing it from the two counts
        # would read a NOT_EVALUATED row -- cast probes skipped, castability unmeasured --
        # as conformant, which is the gate-goes-quiet failure this repo keeps finding.
        "ctl_source_conformance": {
            "job_run_id": "STRING",
            "recorded_at": "TIMESTAMP",
            "target": "STRING",
            "contract_table": "STRING",
            "status": "STRING",
            "missing_columns": "BIGINT",
            "lossy_casts": "BIGINT",
            "findings": "ARRAY<STRING>",
        },
        # WHICH KEY DERIVATION THE LAKE WAS LOADED UNDER. One row per run, written before
        # anything writes to the vault.
        #
        # THE GAP THIS CLOSES, measured 4 September 2026. hash_parity_check proves the
        # ALGORITHM travels; metadata/key_derivation.json proves the COMPONENTS are the ones
        # intended. Nothing proved the LAKE agreed with either. So when hub_job_request was
        # re-keyed to tenant_scoped, the model said one thing and 45,516 stored rows said
        # another, and the next load would have appended 45,516 new-keyed rows beside them --
        # with every gate green. append_only checks for MUTATION and finds none; its
        # uniqueness check passes because the old and new keys are DIFFERENT, not duplicated,
        # so each appears exactly once; landing_integrity does not cover hubs at all. The hub
        # silently doubles and joins still resolve, against the old rows.
        #
        # `derivation_sha256` is the digest of metadata/key_derivation.json, which is itself
        # generated and gated byte-identical -- so comparing digests compares the whole key
        # composition of the estate in one value, and a mismatch names a re-key rather than
        # a guess.
        # WHICH TENANT TABLES EACH UNION VIEW COVERED, per run.
        #
        # checks/apply_source_unions.py rebuilds the Fieldglass union view from
        # information_schema on every run, so a newly onboarded client is picked up
        # automatically rather than silently missing. That automatic inclusion is the right
        # behaviour for a tenant-partitioned source -- and it means the vault's input surface
        # can change with nobody able to say when. This is the record that makes it sayable:
        # "when did this client start loading" has an answer, and a tenant DISAPPEARING (a
        # dropped table, a renamed feed) shows up in the same place.
        "ctl_source_union": {
            "job_run_id": "STRING",
            "recorded_at": "TIMESTAMP",
            "target": "STRING",
            "union_name": "STRING",
            "view_name": "STRING",
            "tables_covered": "BIGINT",
            "tables": "ARRAY<STRING>",
            "findings": "ARRAY<STRING>",
            "status": "STRING",
        },
        "ctl_key_derivation": {
            "job_run_id": "STRING",
            "recorded_at": "TIMESTAMP",
            "target": "STRING",
            "derivation_sha256": "STRING",
            "key_count": "BIGINT",
            "rulebook_version": "STRING",
            # THE MAP THE DIGEST IS TAKEN OVER, stored verbatim beside it. Added 25
            # September, when a whole-model digest reported RE_KEYED for a model that had
            # only GROWN -- 56 recorded keys, all deriving identically, 31 added. A digest
            # can say something changed; only the map can say which key, and "which key"
            # is the difference between reloading 2.2m rows and not.
            #
            # Re-hashed on read, so it cannot be edited into agreement.
            "derivation_json": "STRING",
        },
        # THE INVOICE ISSUANCE LEDGER. Written once per invoice line, never updated.
        #
        # WHY A CONTROL TABLE AND NOT A COLUMN IN GOLD. invoice_date and line_number appear on
        # a document sent to a customer (Ameren AP, via Fieldglass). Gold is a projection and
        # is rebuilt; anything rebuilt can change. If a re-run renumbered an invoice's lines,
        # the customer would be reconciling against a document that no longer matches ours --
        # and NOTHING WOULD FAIL, because every load would succeed and every gate would stay
        # green: append_only has nothing to say about a Gold projection, loop-1 reconciles row
        # counts not values, and hash parity is about keys, not about these two fields. The
        # first symptom would be a dispute, not a red gate.
        #
        # THIS IS THE WDJ-2 ARGUMENT IN A NEW PLACE: a value that can be recomputed cannot
        # serve as a stable external reference -- the same reasoning that rejected a Databricks
        # hash as a permanent external journal identifier. So it is issued once, recorded here,
        # and read thereafter. Task 7 writes this table; Task 8 reads it and MUST NOT
        # recompute what it finds here.
        #
        # GRAIN: (invoice_tenant, invoice_reference, line_reference) -- one row per invoice
        # line, ever. A reissue must not be able to duplicate one; Task 7 owns enforcing
        # that at the write path, the same split this standard draws everywhere else
        # between declaring a table's shape and asserting its deployed behaviour.
        #
        # line_reference, NOT invoice_line_item_ref, AND THAT IS A MEASUREMENT RATHER THAN A
        # PREFERENCE. The design spec called invoice_line_item_ref "unique per line"; the
        # model measured it at 422 distinct values across Ameren's 1,207 rows -- it is a
        # timesheet or expense-sheet reference, and rows beneath one of them differ only by
        # task code, or on expense sheets not even by that. A grain column that does not
        # identify a line cannot hold "one row per line, ever". line_reference IS
        # nhl_invoice_line's transaction key -- 1,169 distinct across the same 1,207 rows,
        # exactly what the ten identity columns produce -- so it is unique per line BY
        # CONSTRUCTION, and a different value is a different line rather than a restatement
        # of one. See metadata/entities/nhl_invoice_line.yml.
        "ctl_invoice_issuance": {
            "invoice_tenant": "STRING",
            "invoice_reference": "STRING",
            "line_reference": "STRING",
            "line_number": "INT",
            "invoice_date": "DATE",
            "issued_at": "TIMESTAMP",
            "issued_by_run_id": "STRING",
        },
    },
    "gold": {},
}

# THE SPLIT IS THE DESIGN. An audit records WHAT HAPPENED and must never be rewritten --
# append_only_check enforces it, and a layer whose audit can be edited has an audit nobody can
# rely on. A config table records WHAT SHOULD HAPPEN: an expectation gets corrected, a manifest
# gets superseded. Making those append-only would mean a mistyped rule could never be
# withdrawn. A conforming layer must get the split right, not set the property everywhere.
#
# ctl_etl_run AND ctl_etl_run_status ARE APPEND-ONLY, and ctl_delivery_manifest IS NOT, in the
# same layer. That is not an inconsistency: the manifest is the sender's CLAIM about what
# should have arrived and has to stay correctable, while a run and its status events record
# what DID happen. Rewriting a run would rewrite the identity every downstream count is
# grouped by.
APPEND_ONLY: frozenset[str] = frozenset(
    set(CORE) | {"ctl_quarantine_superseded", "ctl_source_conformance",
                 "ctl_key_derivation", "ctl_source_union", "ctl_invoice_issuance",
                 "ctl_etl_run", "ctl_etl_run_status"}
)
MUTABLE: frozenset[str] = frozenset(
    {"ctl_approval_manifest", "ref_dq_expectation", "ctl_delivery_manifest"}
)

# WHAT A COLUMN MEANS, CARRIED TO THE PEOPLE WHO WILL WRITE IT. These reach the generated DDL
# as SQL column COMMENTs and the published contract as `column_comments`, so a Bronze engineer
# reading DESCRIBE on the deployed table sees the rule rather than having to find this file.
#
# NO SEMICOLON IN ANY OF THIS TEXT. checks/apply_control_objects.py splits the generated file
# on ';' before it strips comments, so one inside a COMMENT literal cuts a CREATE TABLE apart.
# The emitter sanitises on the way out (see emit_control_contract._comment_safe), which is a
# backstop, not a licence.
COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    "ctl_etl_run": {
        # THE RETRY RULE. It lives here, on the column it constrains, because this is the one
        # place a person about to write a row is guaranteed to read.
        #
        # STATED, NOT ENFORCED -- said plainly because the difference matters. Nothing writes
        # this table yet. There is no constraint, no gate and no writer that can make a
        # second registration of the same run fail today. The writer that enforces it is
        # branch 2.
        "etl_run_id": (
            "RETRY RULE -- a retry re-registers this SAME etl_run_id and appends a status "
            "event. It does not mint a new one. DEF-56 repair runs reuse {{job.run_id}}, so a "
            "new id would turn a retry into a second delivery with a second delivered_count "
            "and reconciliation would balance twice. Opaque UUID, nothing parses it. STATED, "
            "NOT ENFORCED: nothing writes this table yet."
        ),
        "ingestion_mechanism": (
            "OPERATOR-FACING ONLY -- it says who to call. Never in identity, never in "
            "reconciliation, never in control flow. A control that branches on this stops "
            "being technology-independent and makes every new mechanism a code change."
        ),
        "mechanism_native_run_id": (
            # THE APOSTROPHE IS DELIBERATE. This text read "The mechanism own run id" for
            # exactly one commit -- ungrammatical, and it left emit_control_contract.
            # _literal_safe()'s quote-doubling branch reached by nothing we ship. A escape
            # branch no emitted text exercises is a branch nobody has seen work, and the one
            # day it matters is the day someone writes a comment with a "don't" in it and the
            # COMMENT literal closes early, leaving the rest of the sentence as bare SQL.
            "The mechanism's own run id, recorded so an operator can reach their log. Never "
            "joined on, and NULLABLE by intent -- a mechanism that issues no id of its own "
            "is still a first-class member here."
        ),
        "producer": (
            "The job or pipeline that ran, DESCRIPTIVE ONLY. Same rule as "
            "ingestion_mechanism: it is here so an operator knows what to look at, and no "
            "identity, reconciliation or control flow may read it."
        ),
    },
    "ctl_etl_run_status": {
        # THE VOCABULARY, DOCUMENTED RATHER THAN ENFORCED, and the difference is stated so
        # nobody reads this as a constraint. Nothing writes this table and nothing reads the
        # values, so a CHECK constraint or a declared enum here would be a rule with no
        # subject. What the contract CAN do today is tell the Bronze team which words to
        # write -- without that, the contract published to them says nothing at all about the
        # vocabulary, and four teams invent four spellings of COMPLETED.
        "status": (
            "One of PENDING, STARTED, COMPLETED or FAILED. DOCUMENTED, NOT ENFORCED: nothing "
            "writes this table yet and no constraint rejects another value. The status "
            "precedence that breaks a tie between two events sharing a timestamp is declared "
            "with the current-status view, not here."
        ),
        "recorded_at": (
            "OUR clock, and the ordering column. status_at is the mechanism clock and is for "
            "the operator to read -- ordering by it would let clock skew on a source host "
            "sort a STARTED event after a COMPLETED one."
        ),
    },
}

# ONE COLUMN SHAPE, THREE MEANINGS -- declared, never assumed. Without this a reader compares
# bronze's accepted with silver's and concludes rows were lost, when the difference is
# deduplication working as designed.
STAGED_MEANING: dict[str, str] = {
    "bronze": "staged = rows read from the delivered file; "
              "accepted = rows written to <source>_raw",
    "silver": "staged = rows read from the staging log; "
              "accepted = rows inserted into the vault table",
    "gold": "staged = rows read from the vault; "
            "accepted = rows published to the projection",
}


def tables_for(layer: str) -> dict[str, dict[str, str]]:
    """The core plus that layer's own tables. Raises on an unknown layer rather than
    returning the bare core, because a typo would otherwise look like a conforming layer
    that simply declares nothing extra."""
    if layer not in LAYER_TABLES:
        raise KeyError(
            f"{layer!r} is not a layer this standard covers: {LAYERS}. Returning just the "
            f"core for an unknown name would make a typo indistinguishable from a layer that "
            f"declares no tables of its own."
        )
    return {**CORE, **LAYER_TABLES[layer]}
