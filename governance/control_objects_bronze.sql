-- GENERATED from src/accelerator/control_standard.py. Do not hand-edit.
-- verify_repo.py fails the build if regenerating this produces a diff.
--
-- Bronze declares 3 table(s) of its own beyond the mandatory core:
--   ctl_delivery_manifest  MUTABLE
--   ctl_etl_run            APPEND-ONLY
--   ctl_etl_run_status     APPEND-ONLY
--
-- THE SPLIT IS THE DESIGN, and bronze is where both sides of it are real. A manifest
-- records what SHOULD happen and stays correctable, while a run, its status events
-- and the audit tables record what DID happen and must never be rewritten. A
-- conforming layer gets that split right rather than setting the property everywhere.
--
-- A RETRY IS NOT A SECOND DELIVERY. A repair run re-registers the SAME etl_run_id and
-- appends a status event rather than minting a new one. That rule is carried on the
-- column it constrains, as a COMMENT on ctl_etl_run.etl_run_id, and it is STATED
-- rather than enforced: nothing writes these tables yet.
--
-- ingestion_mechanism, producer and mechanism_native_run_id are OPERATOR-FACING. They
-- say who to call and where their own log is. Nothing joins on them, and no identity
-- or reconciliation may read them -- a control that branches on the mechanism makes
-- every new mechanism a code change.
--
-- The invariant a conforming layer satisfies is staged = accepted + sum(discarded)
--
-- In bronze, staged = rows read from the delivered file, accepted = rows written to <source>_raw
--
-- WE CREATE THESE, BRONZE POPULATES THEM. The rows are Bronze's: only the delivering
-- system knows what it delivered. See control_contracts/bronze.yaml, which is the
-- ask for the rows, and docs/loop1_control_table_request.html for why loop-1 needs
-- them.

CREATE SCHEMA IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`;

CREATE TABLE IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`.aud_load_run (
  job_run_id           STRING,
  phase                STRING,
  target               STRING,
  active_sources       STRING,
  recorded_at          TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`.aud_table_load (
  job_run_id           STRING,
  pipeline_update_id   STRING,
  table_name           STRING,
  written_by           STRING,
  staged               BIGINT,
  accepted             BIGINT,
  recorded_at          TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`.aud_table_discard (
  job_run_id           STRING,
  table_name           STRING,
  discard_reason       STRING,
  discarded            BIGINT,
  recorded_at          TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`.ctl_delivery_manifest (
  manifest_id          STRING,
  source_system        STRING,
  delivered_count      BIGINT,
  delivered_at         TIMESTAMP,
  delivered_by         STRING
)
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`.ctl_etl_run (
  etl_run_id           STRING COMMENT 'RETRY RULE -- a retry re-registers this SAME etl_run_id and appends a status event. It does not mint a new one. DEF-56 repair runs reuse {{job.run_id}}, so a new id would turn a retry into a second delivery with a second delivered_count and reconciliation would balance twice. Opaque UUID, nothing parses it. STATED, NOT ENFORCED: nothing writes this table yet.',
  source_system        STRING,
  ingestion_mechanism  STRING COMMENT 'OPERATOR-FACING ONLY -- it says who to call. Never in identity, never in reconciliation, never in control flow. A control that branches on this stops being technology-independent and makes every new mechanism a code change.',
  mechanism_native_run_id STRING COMMENT 'The mechanism\'s own run id, recorded so an operator can reach their log. Never joined on, and NULLABLE by intent -- a mechanism that issues no id of its own is still a first-class member here.',
  producer             STRING COMMENT 'The job or pipeline that ran, DESCRIPTIVE ONLY. Same rule as ingestion_mechanism: it is here so an operator knows what to look at, and no identity, reconciliation or control flow may read it.',
  started_at           TIMESTAMP,
  created_at           TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${bronze_catalog}`.`${control_schema}`.ctl_etl_run_status (
  etl_run_id           STRING,
  status               STRING COMMENT 'One of PENDING, STARTED, COMPLETED or FAILED. DOCUMENTED, NOT ENFORCED: nothing writes this table yet and no constraint rejects another value. The status precedence that breaks a tie between two events sharing a timestamp is declared with the current-status view, not here.',
  status_at            TIMESTAMP,
  recorded_at          TIMESTAMP COMMENT 'OUR clock, and the ordering column. status_at is the mechanism clock and is for the operator to read -- ordering by it would let clock skew on a source host sort a STARTED event after a COMPLETED one.',
  written_by           STRING
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');
