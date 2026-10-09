-- ---------------------------------------------------------------------------
-- Unity Catalog governance, re-applied after every deploy.
--
-- WHY THIS RUNS EVERY TIME: generated pipelines create and replace tables, and a
-- recreated table loses attached policies. Re-application is idempotent and cheap;
-- discovering an unmasked bank account in a Gold view is not.
--
-- WHY MASKS ARE FUNCTIONS PER REGION: a UC metastore is regional, so a column mask
-- is a regional object. The "same" mask exists once per lake and can drift four ways.
-- checks/conformance_check.py compares routine_definition across regions.
--
-- OPEN DECISION -- do not assume either way (day-1 verification task):
--   Whether a mask on a Silver satellite column still applies when that column is
--   read through a PIT table, a bridge, or a Gold view. Verify empirically with an
--   unprivileged principal (checks/mask_survival_check.py). If it does NOT hold,
--   the model changes: personal and financial columns move to satellites that Gold
--   never projects. That is a modelling decision, not a config one.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- STEP 0 -- CATALOG ISOLATION. Required, not hardening.
--
-- A Unity Catalog account has ONE metastore PER REGION, so the PROD and TDS
-- workspaces in a region share it:
--
--     West Europe metastore  <-  "Datalake" (EU prod)  AND  db-weu-datalakehouse-tds
--
-- Both catalogs therefore live in the same metastore and are visible from BOTH
-- workspaces by default. Separate catalogs are NOT isolation: without the binding
-- below, production worker and payroll data is queryable from the TDS workspace by
-- anyone with the catalog grant -- including from a notebook in a staging pipeline.
--
-- VERIFY THE SYNTAX IN YOUR WORKSPACE (DEPLOY.md Phase 3): isolation-mode DDL has
-- changed shape across releases. If ALTER CATALOG is unavailable, do it in the
-- Catalog Explorer UI and record that it was done.
--
-- THE BINDING ITSELF IS AN API/UI OPERATION, not SQL. After setting isolation mode,
-- bind the catalog to the permitted workspace(s) only:
--     databricks workspace-bindings update-bindings catalog ${catalog} \
--       --json '{"add":[{"workspace_id":<id>,"binding_type":"BINDING_TYPE_READ_WRITE"}]}'
-- Workspace ids are recorded next to each host in databricks.yml.
-- ---------------------------------------------------------------------------

-- DEF-47: THE ALTER THAT USED TO BE HERE IS GONE, for two independent reasons.
--
-- 1. It is not valid on this runtime. Measured 25 Aug 2026, it was the ONE statement of
--    49 that failed:
--      [PARSE_SYNTAX_ERROR] Syntax error at or near 'ISOLATION'. SQLSTATE 42601
--    The comment above already warned that isolation-mode DDL "has changed shape across
--    releases" and told the reader to verify it. It had never been run, so nobody had.
--
-- 2. It is not ours to set. DESCRIBE CATALOG EXTENDED reports this catalog's comment as
--    "Managed by Terraform" and its owner as the USNC Terraform service principal -- the
--    same boundary as the bronze catalog in DEF-46. A statement here would fight another
--    system's state, and whichever ran last would win with neither one saying so.
--
-- The catalog is ALREADY isolated, set by that Terraform. The dependency is real, so it
-- is asserted rather than issued: checks/schema_grant_check.py --assert-isolated fails
-- the build if the catalog stops being ISOLATED, and the fix is then a Terraform change
-- rather than a silent ALTER from us.

USE CATALOG `${catalog}`;
CREATE SCHEMA IF NOT EXISTS governance;

-- --------------------------------------------------------------- mask functions
-- Group membership, not user lists. Groups are managed at account level and are
-- the only thing that should differ in effect between regions.
--
-- DEF-22: COMMENT COMES BEFORE RETURN. A SQL UDF's RETURN clause is terminal -- with
-- COMMENT after it the statement does not parse:
--   [PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'
-- All four were written the other way round and had therefore NEVER been created.
-- The first task that ever executed them found it. Do not "tidy" the comment back
-- down to the bottom, where it reads more naturally and does not parse.
--
-- NOTE also that this file is split on semicolons by checks/apply_governance.py, so a
-- semicolon inside a comment creates a bogus statement. Do not put one here.
--
-- DEF-45: THE GROUPS ARE REAL NOW. Until 25 Aug 2026 these functions named
-- hfig_paybill_privileged, hfig_worker_pii_reader and hfig_commercials_reader, none of
-- which exist in this estate. A mask over a nonexistent group denies to everyone, which
-- fails safe but also means the control had never actually been exercised.
--
-- Verified against the workspace: these ARE account groups and is_account_group_member
-- resolves them -- it returned true for data_platform_operations and us_tds_data_engineer
-- for the calling identity, and false for the two it is not in. A workspace-LOCAL group
-- would have returned false for everyone and the masks would have stayed shut with no
-- error anywhere.
--
-- EVERY FUNCTION ALSO ADMITS THE PIPELINE RUN-AS GROUP, and that is not a convenience.
-- DEPLOY.md 6b: mask functions evaluate with the RUN-AS identity's rights during a
-- refresh, so an unprivileged pipeline does not merely hide values from itself -- a
-- downstream materialized view MATERIALISES THE NULLS into the vault as fact, and silver
-- is insert-only. The second WHEN is what stops a load corrupting what it loads.
--
-- THE SECOND WHEN IS NO LONGER ONE GROUP, AND THE SPLIT IS THE PLATFORM TEAM'S.
-- PLT-2 asked for the load service principal to be added to
-- global_dataplatform_pipeline_job_runners, because mask_money admitted that group by
-- name. They refused, with a better argument: it is an OPERATIONAL group whose only
-- function is to let data engineers run jobs. The SP does not need it to RUN anything --
-- it needed it only because mask_money unmasked for it, which makes the ask a
-- data-access grant wearing a job-permission costume. Keep it there and anybody added
-- just to run jobs also gets cleartext currency. They created
-- scope_unmask_currency_values and put the pipeline service principals in it.
--
--   money masks -- mask_money, mask_money_double
--       admit scope_unmask_currency_values
--   PII masks   -- mask_tokenised_account, mask_personal_name, mask_tax_reference
--       admit global_dataplatform_pipeline_job_runners
--
-- THE THREE PII FUNCTIONS ARE DELIBERATELY NOT MOVED. scope_unmask_currency_values is a
-- CURRENCY group. Exempting a personal-name mask for it would be wrong semantically and
-- wrong as governance, and nothing is blocked by leaving them where they are: no column
-- in metadata/entities declares any of the three today, so the run-as admission on them
-- is currently unexercised. When the first PII column lands, that is a conversation with
-- the platform team, not a quiet edit here.
--
-- CONSEQUENCE, STATED PLAINLY: this repo now has TWO privileged groups, and "privileged"
-- is mask-dependent. Anything that reasons about privilege must say WHICH MASK it means,
-- because "can run the job" and "can see money" are no longer the same sentence. An
-- identity in global_dataplatform_pipeline_job_runners but not in
-- scope_unmask_currency_values reads NULL money -- that is the intended posture, not a
-- defect, and the remedy is an explicit request to join the scope group. Do NOT add a
-- fallback WHEN here that admits both, which would restore exactly what was refused.
--
-- Whoever runs a load by hand must be in the group its masks name -- see
-- docs/platform_team_requests.html (PLT-2).

CREATE OR REPLACE FUNCTION governance.mask_tokenised_account(v STRING)
COMMENT 'Bank/IBAN tokens. Non-privileged readers see NULL, not a partial value:
         a partial token is still a correlatable identifier.'
RETURN CASE
         WHEN is_account_group_member('pii_cleared_us') THEN v
         WHEN is_account_group_member('global_dataplatform_pipeline_job_runners') THEN v
         ELSE NULL
       END;

CREATE OR REPLACE FUNCTION governance.mask_personal_name(v STRING)
COMMENT 'Worker names. Initial retained so operational screens remain usable.'
RETURN CASE
         WHEN is_account_group_member('pii_cleared_us') THEN v
         WHEN is_account_group_member('global_dataplatform_pipeline_job_runners') THEN v
         ELSE CONCAT(LEFT(v, 1), '***')
       END;

CREATE OR REPLACE FUNCTION governance.mask_tax_reference(v STRING)
COMMENT 'Tax references are never partially revealed.'
RETURN CASE
         WHEN is_account_group_member('pii_cleared_us') THEN v
         WHEN is_account_group_member('global_dataplatform_pipeline_job_runners') THEN v
         ELSE NULL
       END;

CREATE OR REPLACE FUNCTION governance.mask_money(v DECIMAL(18,2))
COMMENT 'Bill/charge/markup. Implements the public-facade rule: commercial columns
         are absent for non-privileged readers rather than rounded or bucketed.'
RETURN CASE
         WHEN is_account_group_member('usnc_data_analyst_finance') THEN v
         WHEN is_account_group_member('scope_unmask_currency_values') THEN v
         ELSE NULL
       END;

-- ADDED 27 Aug. THE SAME POLICY AS mask_money, FOR A DOUBLE COLUMN -- and it exists because
-- of a measured exposure, not for symmetry.
--
-- ordbtamt and orcrdamt on the two GL entities are the same money as debitamt and crdtamnt,
-- in the originating currency. They were unmasked while the functional amounts were masked,
-- so the protection on those was defeasible from columns in the same row. Measured as an
-- identity the mask does not admit: debitamt 0 rows visible, ordbtamt and orcrdamt fully
-- visible across 8,539,625 rows over both entities.
--
-- WHY A SECOND FUNCTION RATHER THAN CASTING THE COLUMNS. factory.py requires a masked column
-- to declare its type, because the MASK clause goes into the table definition and the types
-- must match. Casting these to DECIMAL(18,2) to reuse mask_money would be a TYPE CHANGE on a
-- delta.appendOnly table, which needs a full refresh that Delta refuses. Declaring the cast
-- as DOUBLE -- what the columns already are -- satisfies the DDL requirement with no data
-- rewrite, and that needs a DOUBLE-typed function.
--
-- THE ADMITTED GROUPS MUST STAY IDENTICAL TO mask_money'S. They are the same sensitivity;
-- two functions are a type accommodation, not two policies. If one list changes and the other
-- does not, the originating amount becomes readable to someone the functional amount denies,
-- which is the exact defect this function was added to close. tests/test_accelerator.py
-- asserts the two admit the same groups.
CREATE OR REPLACE FUNCTION governance.mask_money_double(v DOUBLE)
COMMENT 'Bill/charge/markup held as DOUBLE. Same policy as mask_money -- see the note above
         on why the type differs and why the group lists must not diverge.'
RETURN CASE
         WHEN is_account_group_member('usnc_data_analyst_finance') THEN v
         WHEN is_account_group_member('scope_unmask_currency_values') THEN v
         ELSE NULL
       END;

-- --------------------------------------------------------------- application
-- DELIBERATELY EMPTY.
--
-- Masks are NOT applied here. Every vault object is a streaming table and every _v1 is
-- a materialized view, and for those, row filters and column masks must be set through
-- the table definition / CREATE OR REFRESH -- not by ALTER TABLE afterwards. An
-- ALTER TABLE ... SET MASK against a pipeline-owned table does not survive the next
-- pipeline update.
--
-- So: this file creates the FUNCTIONS and sets isolation and grants. The BINDING of a
-- function to a column is declared in metadata (masks: block in the entity file) and
-- emitted by the factory into the table definition.
--
-- REQUIRED, and easy to miss -- THE PIPELINE RUN-AS IDENTITY MUST BE PRIVILEGED UNDER
-- EVERY MASK. Mask functions evaluate with the pipeline owner's rights during a
-- refresh, so if the run-as identity is not a member of the group below, a downstream
-- materialized view will MATERIALISE NULLS into the vault permanently.
--
--   GRANT EXECUTE ON FUNCTION governance.mask_money TO `<pipeline run-as>`;
--   and add the run-as identity to hfig_commercials_reader (and every other mask group)
--
-- checks/mask_survival_check.py asserts the declared masks are present on the tables.

-- --------------------------------------------------------------- grants
-- Restrict, never duplicate. No filtered copy of a mart per audience: grant against
-- the governed object so there is one thing to secure and one thing to change.
--
-- DEF-40: THERE IS NO `GRANT SELECT ON SCHEMA` HERE, AND ADDING ONE RE-OPENS A BYPASS.
--
-- A vault schema also holds a `__materialization_mat_<pipeline-id>_<table>_1` backing
-- table for every streaming table -- the runtime's own storage, created by SDP and not
-- by this repo. The MASK lives on the streaming table, and the backing table carries
-- the same values with none. A schema-level SELECT covers both.
--
-- Measured 25 Aug 2026, as an identity for which is_account_group_member(
-- 'hfig_commercials_reader') is FALSE: 0 readable `debitamt` through the masked table,
-- 2,453,131 readable through its twin.
--
-- Unity Catalog has no DENY and grants are additive, so this cannot be granted broadly
-- and carved back. SELECT is therefore granted PER TABLE, generated from the declared
-- model by checks/apply_governance.table_select_grants(). A backing table is never
-- declared, so it can never enter that list -- the property is structural, not
-- vigilance. checks/schema_grant_check.py fails the build if a schema- or
-- catalog-level SELECT ever reappears.
--
-- USE SCHEMA USED TO STAY HERE, and moved to apply_governance.data_access_grants() on
-- 2 Sep 2026. It grants no data access on its own, but it was static SQL: switching the
-- per-table SELECT off would have left a schema traversal granted to a role group the
-- platform team asked us to stop touching. One flag now governs both, because they are one
-- decision. This file emits NO GRANT of any kind.

REVOKE ALL PRIVILEGES ON SCHEMA `${catalog}`.`${vault_schema}` FROM `account users`;

-- Business Vault: same treatment as the Raw Vault. It is a separate schema
-- (an SDP pipeline targets one schema), not a separate security posture -- and it has
-- backing tables of its own, so the same per-table rule applies.
REVOKE ALL PRIVILEGES ON SCHEMA `${catalog}`.`${business_vault_schema}` FROM `account users`;

-- DEF-46: THIS REPO DOES NOT GOVERN BRONZE, AND USED TO TRY.
--
-- What stood here was
--     REVOKE ALL PRIVILEGES ON CATALOG bronze FROM `account users`
--     GRANT USE CATALOG / SELECT ON CATALOG bronze TO `hfig_data_engineering`
-- against a catalog this repo only READS, owned and operated by the Bronze team. It was
-- the whole reason apply_governance sat behind a STOP: the REVOKE is unconditional and
-- lands, the GRANT then fails because that group has never existed anywhere in the
-- estate, apply_governance returns 1 -- and the compensating control it had just removed
-- is not restored.
--
-- MEASURED 25 Aug 2026 on 01_usnc_bronze_dev. The catalog carries exactly ONE grant,
-- `scope_tds_full_scopes_write` with CREATE_SCHEMA and USE_CATALOG, and there is no
-- `account users` grant at all -- so the REVOKE had nothing to revoke, and the posture
-- the comment claimed to be restoring was already in place by someone else's hand. That
-- catalog also has its own reader groups (data_platform_bronze_layer_reader,
-- usnc_data_platform_bronze_layer_reader), which is the Bronze team's convention and not
-- ours.
--
-- Two teams writing grants to one catalog is how an estate ends up with a posture nobody
-- can explain: whichever ran last wins, and neither one's code says so. We depend on
-- Bronze not being world-readable -- the `<source>_raw` schemas hold UNMASKED PII under
-- decision D3, and the vault reads them precisely so business keys hash true identifiers
-- -- so the dependency is real. It is now ASSERTED rather than enforced:
-- checks/schema_grant_check.py --assert-not-world-readable fails the build if Bronze
-- becomes readable by `account users`, and the fix is then a conversation with the team
-- that owns it rather than a silent grant from us.

-- NO GOLD GRANTS HERE, DELIBERATELY.
--
-- DECIDED 26 Aug 2026 (DEPLOY.md Phase 6STOP), and this comment predicted it: nobody
-- queries the vault directly. The RAW vault is not consumer-readable at all, and the
-- BUSINESS vault only through the gold catalog -- never by a grant on the vault itself.
--
-- That is now more than intent. Until today it held only because ${vault_privileged_group}
-- happened to name an engineering group. The variable was called vault_READER_group,
-- which invited precisely the wrong value, and setting it to a consumer group would have
-- granted the vault away with nothing objecting. It is renamed, and
-- checks/schema_grant_check.py --allow-table-select now fails the build if any principal
-- other than that group holds table-level SELECT in either vault schema. The twins are
-- covered by the same sweep, because it reads information_schema rather than the
-- declared model.
--
-- Consumers read Gold, and Gold is its own catalog with per-project schemas, so the
-- grant belongs at catalog level. But the gold catalog
-- does not exist yet (design spec section 2 records 03_usnc_gold_edm_dev as not created,
-- and section 9 puts Gold generation out of scope). A GRANT against a catalog that is
-- absent fails, checks/apply_governance.py returns 1 on any failed statement, so the
-- apply_governance job task fails -- which blocks assert_mask_survival, the vault's sole
-- PII defence under decision D3. Shipping a statement known to fail on first deploy
-- would trade a real control for a grant nobody is waiting on, and plain SQL has no
-- clean conditional to guard it with.
--
-- WHEN THE GOLD CATALOG IS CREATED, add back, in this position. Note what this does
-- NOT license: granting analysts SELECT on the gold CATALOG is the decided posture, but
-- the business vault is reached through gold OBJECTS that read it, not by extending any
-- grant back onto the vault schema. There is nothing in gold to read yet.
--     REVOKE ALL PRIVILEGES ON CATALOG gold_catalog FROM account users
--     GRANT USE CATALOG ON CATALOG gold_catalog TO hfig_analysts
--     GRANT SELECT ON CATALOG gold_catalog TO hfig_analysts
-- backticking every interpolated identifier, and using the ${...} placeholder form.
-- The gold_catalog bundle variable and the job task's --gold-catalog argument are both
-- still in place, so nothing else needs changing.
