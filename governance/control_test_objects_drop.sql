-- TEARDOWN for control_test_objects.sql.
--
-- Every statement names a tst_ object. tests/test_accelerator.py asserts that no DROP in
-- this file names anything without the prefix: a teardown able to reach a real control
-- table is a teardown that will eventually delete the audit.
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_ctl_approval_manifest;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_ref_dq_expectation;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_load;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_aud_table_discard;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_aud_load_run;
DROP TABLE IF EXISTS `${catalog}`.`${control_schema}`.tst_ctl_quarantine_superseded;
