-- Control objects for the Data Vault accelerator, in a schema this repo OWNS.
--
-- DEF-48. These were carried for weeks as "platform objects, neither created by this
-- repo". That was wrong: 02_usnc_silver_edm_dev.governance is owned by the accelerator
-- team, created by our own create_mask_functions task, and already holds the two pipeline
-- event logs. Nobody needed to be asked.
--
-- CREATING ctl_approval_manifest EMPTY IS WORSE THAN NOT CREATING IT, which is why this
-- file is not simply run and forgotten. checks/loop1_reconciliation.py drives its
-- comparison FROM this table: with no rows, every variance query returns nothing, every
-- table still counts as reconciled, and the gate prints PASSED having compared nothing.
-- DEF-48 added two guards so that cannot happen -- an empty manifest FAILS, and rows
-- belonging to no manifest are reported -- but the table is still only useful once
-- something populates it.

CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${governance_schema}`;
CREATE SCHEMA IF NOT EXISTS `${catalog}`.`${control_schema}`;

-- WHAT approved_count COUNTS, settled.
--
-- The rows a batch approved for loading, compared against the rows that landed in ONE
-- target. That identity holds only where the loader writes one row per approved row:
-- an NHL, a link, a HAL. A hub deduplicates -- 4,444,172 GP rows become 2,221,108 hub
-- rows -- and a satellite stores only changed rows, so for those the identity is false
-- by design and the gate refuses them (loop1.RECONCILABLE_KINDS).
--
-- So one manifest row per (batch, target) is NOT required: a manifest is a source batch,
-- and every reconcilable target of that batch receives its rows one for one.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_approval_manifest (
  manifest_id     STRING  NOT NULL COMMENT 'carried onto every row this batch loads',
  approved_count  BIGINT  NOT NULL COMMENT 'rows this batch approved for loading',
  source_system   STRING           COMMENT 'which feed produced the batch',
  approved_at     TIMESTAMP        COMMENT 'when the batch was approved',
  approved_by     STRING           COMMENT 'the process or person that approved it'
)
COMMENT 'Loop-1 control: landed + (quarantined - superseded) = approved, where
         superseded counts the rejects a later run legitimately re-accepted. Populated by
         whatever approves a batch for loading -- NOT by this repo, which would make it
         self-certifying.'
TBLPROPERTIES ('hfig.control_object' = 'true');

-- SEVERITY, added 27 Aug 2026. Before it, a consumer could not tell a rule that DROPS a row
-- from one that is advisory, because there was only one behaviour: expect_all_or_drop, which
-- drops and quarantines.
--
-- ONE TIER OF THE THREE IS IMPLEMENTED. The other two are declarable in this column and
-- honoured by nothing, and tools/emit_data_contract.py REFUSES both rather than promising
-- a consumer a behaviour the pipeline lacks. The wording below and the `quality.severities`
-- block that emitter publishes are the same statement of the same fact, and the suite
-- asserts they agree -- a tier described here as implemented while the emitter refuses it
-- is exactly the overstatement this branch already shipped once in the artefacts.
--
--   drop  the row is dropped and written to the quarantine twin, and the load continues.
--         IMPLEMENTED, and the only tier that is. Every rule behaves this way today --
--         including the two compiled-in key-safety rules -- so it is the default and
--         existing rows need no migration.
--   fail  would stop the load. NOT IMPLEMENTED. DEF-18 (factory.py:988): the generated
--         flows never invoke @dp.expect_all_or_drop at all -- _valid does
--         df.where(~failed) with a parallel _invalid flow into the quarantine twin -- so
--         nothing in this pipeline stops a load.
--   warn  would load the row and record the violation. NOT IMPLEMENTED. No advisory-only
--         mechanism exists.
--
-- Governed data-quality rules. Absent, the generator falls back to an empty rule set and
-- the load proceeds on its two compiled-in key-safety rules -- so this one is a
-- capability, not a blocker.
-- AN IF-NOT-EXISTS DECLARATION CANNOT EVOLVE AN EXISTING TABLE, and nothing here notices.
-- Discovered 27 Aug 2026: `severity` was added to this declaration when the data contract
-- work landed, `create_control_objects` was then run and reported SUCCESS, and the column
-- did NOT appear -- because the table already existed, so the statement was a no-op. The
-- first INSERT carrying a severity failed with "automatic schema migration is not allowed"
-- and the column inferred as `col5`.
--
-- `ALTER TABLE ... ADD COLUMNS IF NOT EXISTS` is NOT supported here (parse error), so this
-- file cannot be made self-healing in pure SQL. ANY COLUMN ADDED TO ANY TABLE BELOW AFTER
-- ITS FIRST DEPLOY NEEDS A MANUAL ALTER PER LAKE. For `severity`, applied to usnc_tds on
-- 27 Aug and still owed by every other lake:
--
--   ALTER TABLE <catalog>.<control>.ref_dq_expectation
--     ADD COLUMNS (severity STRING)
--   ALTER TABLE <catalog>.<control>.ref_dq_expectation ALTER COLUMN severity SET NOT NULL
--   (the two statements above are written WITHOUT a terminating semicolon on purpose. The
--    splitter in checks/apply_control_objects.py cuts on that character regardless of
--    comments, so writing one anywhere in this note -- even inside quotes, describing it --
--    splits the file into fragments. Writing this note tripped that twice.)
--
-- There is no gate comparing the columns declared here against the columns deployed. That
-- is the real gap: a task that reports SUCCESS while doing nothing is worse than one that
-- fails. See OPEN_ITEMS.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ref_dq_expectation (
  dataset     STRING  NOT NULL COMMENT 'target table, e.g. nhl_general_journal_line',
  rule_name   STRING  NOT NULL COMMENT 'appears in the quarantine record as the reason',
  rule_sql    STRING  NOT NULL COMMENT 'boolean SQL evaluated against the source row',
  is_current  BOOLEAN NOT NULL COMMENT 'only current rules are read',
  severity    STRING  NOT NULL COMMENT 'drop quarantines the row and the load continues -- the only implemented tier. fail and warn are declarable here and honoured by nothing, and tools/emit_data_contract.py refuses both'
)
COMMENT 'Expectations are governed configuration, held in UC rather than in code.'
TBLPROPERTIES ('hfig.control_object' = 'true');

-- The load audit. One row per table per run, and the discard breakdown beside it.
--
-- TWO TABLES, NOT ONE. A single table repeating staged on one row per discard reason
-- forces the rule "sum accepted, but take max of staged, never sum it" -- a trap where
-- the first SELECT sum(staged) is wrong and no gate notices. It also puts a nullable
-- column inside the grain key, and SQL NULL does not compare equal, so that grain's
-- uniqueness cannot be asserted with an equality join.
--
-- Normalised, staged and accepted are stated once per table per run, so staged - accepted
-- is the total discarded and checks/audit_completeness_check.py can assert it equals
-- SUM(discarded). A discard the writer failed to attribute becomes an arithmetic gap
-- rather than nothing at all.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.aud_table_load (
  job_run_id          STRING     NOT NULL COMMENT 'the job run that wrote this, from {{job.run_id}}',
  pipeline_update_id  STRING              COMMENT 'ties to the pipeline event log, NULL for a batch loader',
  table_name          STRING     NOT NULL COMMENT 'the table written',
  written_by          STRING     NOT NULL COMMENT 'the script that wrote it',
  staged              BIGINT     NOT NULL COMMENT 'rows the writer read from its source',
  accepted            BIGINT     NOT NULL COMMENT 'rows it inserted',
  recorded_at         TIMESTAMP  NOT NULL COMMENT 'when the writer recorded this'
)
CLUSTER BY (job_run_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.aud_table_discard (
  job_run_id      STRING     NOT NULL COMMENT 'the job run that wrote this',
  table_name      STRING     NOT NULL COMMENT 'the table written',
  discard_reason  STRING     NOT NULL COMMENT 'why these rows were not inserted',
  discarded       BIGINT     NOT NULL COMMENT 'how many, for this reason',
  recorded_at     TIMESTAMP  NOT NULL COMMENT 'when the writer recorded this'
)
CLUSTER BY (job_run_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

-- Two rows per run, opened and completed, never one row updated. An UPDATE would put
-- this schema outside append_only_check for ever, which is the property that makes the
-- audit worth reading. A run with no completed row did not finish.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.aud_load_run (
  job_run_id      STRING     NOT NULL COMMENT 'from {{job.run_id}}',
  phase           STRING     NOT NULL COMMENT 'opened | completed',
  target          STRING     NOT NULL COMMENT 'bundle target, e.g. usnc_tds',
  active_sources  STRING              COMMENT 'the declared activity list this run was given',
  recorded_at     TIMESTAMP  NOT NULL COMMENT 'when the phase was recorded'
)
CLUSTER BY (job_run_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

-- Rejects we later accepted, and why. Loop-1 subtracts these from its quarantined count.
--
-- THIS TABLE CAN DISARM A HARD GATE, which is why it is append-only and asserted.
-- The identity becomes landed + (quarantined - superseded) = approved, so a spurious
-- record REDUCES the quarantined count and makes loop-1 pass on a real variance. It goes
-- green exactly when it should not. checks/loop1_reconciliation.py therefore fails if a
-- manifest supersedes more than it quarantined, and checks/supersede_quarantine.py fails
-- if a record's digest matches no row in the twin it names.
--
-- reject_digest is a digest over the row's declared non-system columns, with hash keys
-- rendered as lowercase hex. See src/accelerator/reject_digest.py for why hex, and why
-- the payload alone is not the right column set.
--
-- rulebook_version records what hashing.RULEBOOK_VERSION was when the digest was
-- computed. A bump changes the normalisation and stops outstanding digests matching --
-- acceptable, because a bump already re-keys the estate, but it must be DETECTABLE rather
-- than silently non-matching.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_quarantine_superseded (
  manifest_id       STRING     NOT NULL COMMENT 'the manifest the reject belonged to',
  table_name        STRING     NOT NULL COMMENT 'the quarantine twin that held it',
  reject_digest     STRING     NOT NULL COMMENT 'digest over declared non-system columns',
  rulebook_version  STRING     NOT NULL COMMENT 'hashing.RULEBOOK_VERSION at compute time',
  superseded_by     STRING     NOT NULL COMMENT 'job_run_id that landed the same content',
  reason            STRING              COMMENT 'why it was accepted this time',
  recorded_at       TIMESTAMP  NOT NULL COMMENT 'when the supersede was recorded'
)
CLUSTER BY (manifest_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

-- WHAT OUR GATE CONCLUDED ABOUT BRONZE'S DELIVERY, held in silver because bronze's catalog
-- belongs to another team and carries no control schema at all.
--
-- ONE ROW PER (job_run_id, contract_table), whether or not anything was wrong. A table
-- holding only failures cannot express "checked, nothing wrong", which leaves the coverage
-- denominator empty and makes an unmeasured estate indistinguishable from a clean one --
-- DEF-48's shape, at dashboard scale.
--
-- status IS STORED, NOT DERIVED FROM THE COUNTS. NOT_EVALUATED means the cast probes were
-- skipped and castability is unmeasured, so a reader recomputing status from
-- missing_columns + lossy_casts would call that row conformant.
--
-- findings carries checks/source_conformance_check.py's own "KIND: detail" lines verbatim,
-- so the dashboard shows the gate's words rather than a paraphrase that can drift from them.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_source_conformance (
  job_run_id       STRING         NOT NULL COMMENT 'the run that measured it',
  recorded_at      TIMESTAMP      NOT NULL COMMENT 'when the measurement was recorded',
  target           STRING         NOT NULL COMMENT 'bundle target the contract belongs to',
  contract_table   STRING         NOT NULL COMMENT 'three-part bronze table name',
  status           STRING         NOT NULL COMMENT 'CONFORMANT/ABSENT/NON_CONFORMANT/NOT_EVALUATED',
  missing_columns  BIGINT         NOT NULL COMMENT 'required columns absent from bronze',
  lossy_casts      BIGINT         NOT NULL COMMENT 'columns whose values do not all cast',
  findings         ARRAY<STRING>           COMMENT 'the gate''s own KIND: detail lines'
)
CLUSTER BY (contract_table)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

-- WHICH KEY DERIVATION THE LAKE WAS LOADED UNDER. One row per run, written by
-- checks/key_derivation_guard.py BEFORE anything writes to the vault.
--
-- THE GAP IT CLOSES. hash_parity_check proves the hash ALGORITHM travels;
-- metadata/key_derivation.json proves the COMPONENTS are the ones intended. Nothing proved
-- the LAKE agreed with either. When hub_job_request was re-keyed to tenant_scoped on
-- 4 September the model said one thing and 45,516 stored rows said another, and the next
-- load would have appended 45,516 new-keyed rows beside them with every gate green:
-- append_only looks for MUTATION and finds none, its uniqueness check passes because the
-- old and new keys are DIFFERENT rather than duplicated, and landing_integrity does not
-- cover hubs. The hub doubles and joins still resolve -- against the old rows.
--
-- derivation_sha256 is the digest of metadata/key_derivation.json, which is generated and
-- gated byte-identical, so one value stands for the whole key composition of the estate and
-- a mismatch names a re-key instead of inviting a guess.
-- WHICH TENANT TABLES EACH UNION VIEW COVERED, per run.
--
-- checks/apply_source_unions.py rebuilds the Fieldglass union view from information_schema
-- every run, so a client onboarded since the last load is included automatically rather than
-- silently missing -- the right behaviour for a source that ships one table per tenant, and
-- the reason the input surface can move without anyone noticing. This is the record that
-- makes it noticeable after the fact: "when did this client start loading" has an answer,
-- and a tenant DISAPPEARING shows up in the same place.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_source_union (
  job_run_id      STRING         NOT NULL COMMENT 'the run that built the view',
  recorded_at     TIMESTAMP      NOT NULL COMMENT 'when',
  target          STRING         NOT NULL COMMENT 'bundle target this lake is',
  union_name      STRING         NOT NULL COMMENT 'the declaration in metadata/source_unions.yml',
  view_name       STRING         NOT NULL COMMENT 'the view it created, fully qualified',
  tables_covered  BIGINT         NOT NULL COMMENT 'how many tenant tables the union spans',
  tables          ARRAY<STRING>           COMMENT 'the tenant tables, so a change is diffable',
  findings        ARRAY<STRING>           COMMENT 'the task''s own KIND: detail lines',
  status          STRING         NOT NULL COMMENT 'APPLIED or FAILED'
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_key_derivation (
  job_run_id         STRING     NOT NULL COMMENT 'the run that recorded it',
  recorded_at        TIMESTAMP  NOT NULL COMMENT 'when it was recorded',
  target             STRING     NOT NULL COMMENT 'bundle target this lake is',
  derivation_sha256  STRING     NOT NULL COMMENT 'sha256 of metadata/key_derivation.json',
  key_count          BIGINT     NOT NULL COMMENT 'hash-key columns the record covers',
  rulebook_version   STRING     NOT NULL COMMENT 'hashing.RULEBOOK_VERSION at load time',
  -- NULLABLE, and only for records written before 25 September 2026. The gate reads the
  -- latest record's map to answer WHICH key moved. A record with none cannot answer it and
  -- is refused as UNMAPPED rather than guessed at in either direction.
  derivation_json    STRING              COMMENT 'metadata/key_derivation.json verbatim, re-hashed on read against derivation_sha256'
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

-- THE INVOICE ISSUANCE LEDGER. Written once per invoice line, by Task 7, never updated.
--
-- WHY A CONTROL TABLE AND NOT A COLUMN IN GOLD. invoice_date and line_number appear on a
-- document sent to Ameren AP via Fieldglass. Gold is a projection and is rebuilt -- anything
-- rebuilt can change. If a re-run renumbered an invoice's lines, the customer would be
-- reconciling against a document that no longer matches ours -- and NOTHING WOULD FAIL,
-- because every load would succeed and every gate would stay green: append_only_check has
-- nothing to say about a Gold projection, loop1_reconciliation compares row counts, not
-- these two values, and hash parity is about keys, not about invoice_date or line_number.
-- The first symptom would be a dispute, not a red gate.
--
-- THIS IS THE WDJ-2 ARGUMENT IN A NEW PLACE: a value that can be recomputed cannot serve
-- as a stable external reference -- the same reasoning that rejected a Databricks hash as a
-- permanent external journal identifier. So it is issued once, recorded here, and read
-- thereafter rather than recomputed. Task 8 reads this table and must not recompute what it
-- finds here.
--
-- GRAIN: (invoice_tenant, invoice_reference, line_reference) -- one row per invoice line,
-- ever. A reissue must not be able to duplicate one -- enforcing that at the write path
-- is Task 7's, the same split every other table here draws between declaring a shape and
-- asserting deployed behaviour.
--
-- line_reference, NOT invoice_line_item_ref, AND THAT IS A MEASUREMENT RATHER THAN A
-- PREFERENCE. The design spec called invoice_line_item_ref "unique per line". The model
-- measured it at 422 distinct values across Ameren's 1,207 rows -- it is a timesheet or
-- expense-sheet reference, and rows beneath one of them differ only by task code, or on
-- expense sheets not even by that. A grain column that does not identify a line cannot
-- hold "one row per line, ever". line_reference IS nhl_invoice_line's transaction key --
-- 1,169 distinct across the same 1,207 rows, exactly what the ten identity columns
-- produce -- so it is unique per line BY CONSTRUCTION, and a different value is a
-- different line rather than a restatement of one.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_invoice_issuance (
  invoice_tenant          STRING     NOT NULL COMMENT 'the buyer code, matching hub_invoice''s tenant key',
  invoice_reference       STRING     NOT NULL COMMENT 'the Fieldglass invoice id',
  line_reference          STRING     NOT NULL COMMENT 'nhl_invoice_line''s transaction key, unique per line by construction. AME006 orders by it',
  line_number             INT        NOT NULL COMMENT 'AME006, assigned once, in line_reference order',
  invoice_date            DATE       NOT NULL COMMENT 'AME002, assigned once at issuance',
  issued_at               TIMESTAMP  NOT NULL COMMENT 'when this row was written -- OUR clock, not the source''s',
  issued_by_run_id        STRING     NOT NULL COMMENT 'the job run that issued it, for audit'
)
CLUSTER BY (invoice_tenant, invoice_reference)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');
