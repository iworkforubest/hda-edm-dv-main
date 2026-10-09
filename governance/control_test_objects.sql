-- SYNTHETIC OBJECTS FOR PROVING THE QUALITY DASHBOARD'S TILES. NOT PART OF THE ESTATE.
--
-- Applied ON DEMAND and dropped afterwards with control_test_objects_drop.sql. The
-- standing create_control_objects task must never apply this file.
--
-- WHY tst_ AND NOT A SEPARATE SCHEMA. The prefix is chosen so three existing gates behave
-- correctly without being weakened:
--
--   * append_only_check selects on CONTROL_PREFIXES = ('ctl_', 'ref_', 'aud_'). tst_ is
--     outside it, so these tables are never swept and may be dropped. That is the whole
--     point: the four real audit tables are append-only, so a synthetic row written into
--     one of them could never be removed, and a DELETE to tidy up would put the table
--     outside append-only for ever.
--   * schema_grant_check.misplaced_control_objects fails the build on any table in the
--     control schema NOT declaring hfig.control_object. So these declare it. Nothing ties
--     the marked set to control_objects.sql -- the gate's message advises declaring there
--     but asserts no such thing -- which is why a second file is legitimate.
--   * audit_completeness_check reads aud_load_run, aud_table_load and aud_table_discard by
--     EXACT NAME, and loop1_reconciliation reads ctl_approval_manifest by exact name.
--     No tst_ name matches, so the synthetic rows are invisible to both.
--
-- NOTHING SYNTHETIC GOES IN ref_dq_expectation. The pipeline reads that table to build its
-- expectations, so seeding it is not a test -- it is a production change that would
-- evaluate fabricated rules against real data. tst_ref_dq_expectation is a separate table
-- and the pipeline never reads it.
--
-- No semicolon may appear inside a COMMENT literal: the statement splitter cuts on ';'
-- regardless of quoting.

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_ctl_approval_manifest (
  manifest_id     STRING  NOT NULL,
  approved_count  BIGINT  NOT NULL,
  source_system   STRING,
  approved_at     TIMESTAMP,
  approved_by     STRING
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_ref_dq_expectation (
  dataset     STRING  NOT NULL,
  rule_name   STRING  NOT NULL,
  rule_sql    STRING  NOT NULL,
  is_current  BOOLEAN NOT NULL,
  severity    STRING  NOT NULL
)
COMMENT 'SYNTHETIC -- the pipeline never reads this, unlike ref_dq_expectation'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_load (
  job_run_id          STRING     NOT NULL,
  pipeline_update_id  STRING,
  table_name          STRING     NOT NULL,
  written_by          STRING     NOT NULL,
  staged              BIGINT     NOT NULL,
  accepted            BIGINT     NOT NULL,
  recorded_at         TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_discard (
  job_run_id      STRING     NOT NULL,
  table_name      STRING     NOT NULL,
  discard_reason  STRING     NOT NULL,
  discarded       BIGINT     NOT NULL,
  recorded_at     TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_aud_load_run (
  job_run_id      STRING     NOT NULL,
  phase           STRING     NOT NULL,
  target          STRING     NOT NULL,
  active_sources  STRING,
  recorded_at     TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.tst_ctl_quarantine_superseded (
  manifest_id       STRING     NOT NULL,
  table_name        STRING     NOT NULL,
  reject_digest     STRING     NOT NULL,
  rulebook_version  STRING     NOT NULL,
  superseded_by     STRING     NOT NULL,
  reason            STRING,
  recorded_at       TIMESTAMP  NOT NULL
)
COMMENT 'SYNTHETIC -- drop with control_test_objects_drop.sql'
TBLPROPERTIES ('hfig.control_object' = 'true');

-- SEED. WHAT IT DOES AND DOES NOT EXERCISE -- read this before treating a green
-- synthetic dashboard as proof.
--
-- EXERCISED BY THIS SEED ALONE. ds_load_volume, ds_discard_reason, ds_accept_rate,
-- ds_incomplete_runs, ds_supersede and ds_manifest read a tst_ table only, so the six
-- tiles over them render from these rows and nothing else. ds_accept_rate renders BOTH of
-- its states: three tables with staged > 0 read 'evaluated', and msat_journal_line_worktag
-- with staged = 0 reads 'not evaluated'. Without that last row every seeded table has a
-- non-zero denominator and the zero-denominator branch -- the one spec 8 calls "the one to
-- get right" -- never renders at all.
--
-- NOT EXERCISED BY THIS SEED. ds_coverage_summary and ds_coverage -- the HEADLINE tiles --
-- read `governance.meta_vault_model`, which has NO tst_ twin and cannot have one: it is
-- written by checks/publish_metadata.py into the governance schema, outside the control
-- schema this file may create in. `publish_model_metadata` has never run, so that table
-- does not exist in the lake. Applying this file and opening the synthetic dashboard
-- WITHOUT populating meta_vault_model first leaves both coverage tiles failing at view
-- time on a missing table. See docs/quality_dashboard_runbook.md, which now makes that a
-- numbered step.
--
-- Once meta_vault_model IS populated, the seeded rules make the coverage tile's two
-- branches distinguishable: nhl_general_journal_line and hub_job_request carry rules and
-- read 'covered', while every other generated table reads 'not evaluated'.

INSERT INTO `${catalog}`.`${control_schema}`.tst_ref_dq_expectation VALUES
  ('nhl_general_journal_line', 'debit_credit_balanced',
   'debitamt IS NOT NULL AND crdtamnt IS NOT NULL', true, 'drop'),
  ('nhl_general_journal_line', 'positive_amount', 'debitamt >= 0', true, 'drop'),
  ('hub_job_request', 'business_key_present', 'jobrequestid IS NOT NULL', true, 'drop');

INSERT INTO `${catalog}`.`${control_schema}`.tst_aud_table_load VALUES
  ('SYNTH-001', NULL, 'nhl_general_journal_line', 'load_hubs.py',
   1000, 940, TIMESTAMP '2026-08-27 09:00:00'),
  ('SYNTH-001', NULL, 'hub_job_request', 'load_hubs.py',
   500, 500, TIMESTAMP '2026-08-27 09:01:00'),
  ('SYNTH-002', NULL, 'sat_job_request_details_bullhorn_eu', 'load_satellites.py',
   250, 200, TIMESTAMP '2026-08-27 10:00:00'),
  -- staged = 0, accepted = 0, ON A DISTINCT TABLE NAME. This is the only row that makes
  -- ds_accept_rate render its 'not evaluated' branch: RATE_GUARD sends a zero denominator
  -- to NULL and the state column spells out why. Every other seeded table has staged > 0,
  -- so without this row the branch spec 3 requires "in those words" is never seen to
  -- render. tests/test_accelerator.py asserts a zero-staged row is present.
  ('SYNTH-002', NULL, 'msat_journal_line_worktag', 'load_satellites.py',
   0, 0, TIMESTAMP '2026-08-27 10:05:00');

INSERT INTO `${catalog}`.`${control_schema}`.tst_aud_table_discard VALUES
  ('SYNTH-001', 'nhl_general_journal_line', 'debit_credit_balanced',
   45, TIMESTAMP '2026-08-27 09:00:00'),
  ('SYNTH-001', 'nhl_general_journal_line', 'positive_amount',
   15, TIMESTAMP '2026-08-27 09:00:00'),
  ('SYNTH-002', 'sat_job_request_details_bullhorn_eu', 'hashdiff_null',
   50, TIMESTAMP '2026-08-27 10:00:00');

-- SYNTH-002 is opened and never completed, so the incomplete-runs tile has a row.
INSERT INTO `${catalog}`.`${control_schema}`.tst_aud_load_run VALUES
  ('SYNTH-001', 'opened', 'usnc_tds', '', TIMESTAMP '2026-08-27 08:59:00'),
  ('SYNTH-001', 'completed', 'usnc_tds', '', TIMESTAMP '2026-08-27 09:05:00'),
  ('SYNTH-002', 'opened', 'usnc_tds', '', TIMESTAMP '2026-08-27 09:59:00');

INSERT INTO `${catalog}`.`${control_schema}`.tst_ctl_approval_manifest VALUES
  ('SYNTH-MANIFEST-1', 1000, 'GP_US', TIMESTAMP '2026-08-27 08:50:00', 'synthetic');

INSERT INTO `${catalog}`.`${control_schema}`.tst_ctl_quarantine_superseded VALUES
  ('SYNTH-MANIFEST-1', 'qtn_general_journal_line', 'abc123', '1.0.0',
   'SYNTH-002', 'corrected at source', TIMESTAMP '2026-08-27 10:30:00');
