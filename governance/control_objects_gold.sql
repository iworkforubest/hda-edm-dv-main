-- GENERATED from src/accelerator/control_standard.py. Do not hand-edit.
-- verify_repo.py fails the build if regenerating this produces a diff.
--
-- Gold declares no control tables of its own: a projection layer has no rejects to
-- supersede and no expectations until someone declares them. This is the mandatory
-- core alone, and the invariant it must satisfy is staged = accepted + sum(discarded)
--
-- In gold, staged = rows read from the vault, accepted = rows published to the projection

CREATE SCHEMA IF NOT EXISTS `${gold_catalog}`.`${control_schema}`;

CREATE TABLE IF NOT EXISTS `${gold_catalog}`.`${control_schema}`.aud_load_run (
  job_run_id           STRING,
  phase                STRING,
  target               STRING,
  active_sources       STRING,
  recorded_at          TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${gold_catalog}`.`${control_schema}`.aud_table_load (
  job_run_id           STRING,
  pipeline_update_id   STRING,
  table_name           STRING,
  written_by           STRING,
  staged               BIGINT,
  accepted             BIGINT,
  recorded_at          TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');

CREATE TABLE IF NOT EXISTS `${gold_catalog}`.`${control_schema}`.aud_table_discard (
  job_run_id           STRING,
  table_name           STRING,
  discard_reason       STRING,
  discarded            BIGINT,
  recorded_at          TIMESTAMP
)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');
