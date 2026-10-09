-- PROPOSED data quality expectations. NOT APPLIED.
--
-- Applying this is a production change, not a test: the pipeline READS
-- control.ref_dq_expectation to build its expectations, and every rule below is the `drop`
-- tier -- the only tier implemented -- so a violating row is quarantined and does not land.
-- Each rule was therefore DRY-RUN against Bronze first and carries its measured effect.
--
-- HOW THESE ARE EVALUATED, because it changes how they must be written:
--
--   * rule_sql runs against the STAGED BRONZE FRAME, so it names SOURCE columns, not vault
--     columns. factory.py: the expressions "may name any source column -- which means they
--     can only be evaluated against the staged frame, never against the projection's
--     output, where those names no longer exist."
--   * `dataset` is a TARGET TABLE, and each target table has its own source binding with its
--     own column names. gl20000 and gl30000 feed two DIFFERENT tables, so the same rule text
--     is declared twice below rather than once.
--   * A rule that evaluates to FALSE **or NULL** fails the row. _violation_expr: "an
--     expectation that cannot be evaluated is not a pass." So every rule touching a nullable
--     column is wrapped in COALESCE(..., TRUE) where null is legitimate. Without that, a rule
--     silently quarantines every row where its column is null.
--
-- ALREADY COVERED -- deliberately not restated here. Compiled in for every key column of
-- every entity: presence and non-empty, no hash delimiter in the value, not the null token
-- (see hashing.key_safety_rules). Set-level facts belong to gates, not expectations: debits
-- equal credits and line count equals control total (journal_integrity_check), payroll detail
-- against the GL at the declared grain (aggregate_reconciliation_check), and completeness
-- (loop1_reconciliation).

-- ---------------------------------------------------------------------------------------
-- MEASURED 27 Aug 2026 against Bronze. Counts are rows that WOULD be quarantined.
--
--   dataset                                rule                     drops        of
--   nhl_general_journal_line               amount_not_negative          8   4,444,172
--   nhl_general_journal_line               line_not_degenerate        202   4,444,172
--   nhl_general_journal_line_closed_year   amount_not_negative          4  14,612,795
--   nhl_general_journal_line_closed_year   line_not_degenerate      7,523  14,612,795
--   sat_job_request_details_bullhorn_eu    openings_not_negative        5      69,207
--   sat_job_request_details_bullhorn_eu    title_not_blank              3      69,207
--
-- REJECTED BY MEASUREMENT, and this is why the dry run exists:
--
--   docdate >= DATE'1990-01-01'  would have quarantined ALL 4,444,172 rows. docdate is
--     1900-01-01 on every row -- non-null, so it looks populated, but it is a placeholder.
--     It is not in the model or the vault, so nothing depends on it, but anyone reaching for
--     a GL business date should use openyear (2025-2026) instead.
--   dateclosed >= dateadded  can never fire: dateclosed is NULL on all 69,207 bullhorn rows.
--     A rule that cannot fail is not a control.
--   (debitamt = 0) <> (crdtamnt = 0)  is redundant with line_not_degenerate -- both matched
--     exactly the same 202 rows, because no row carries both a debit and a credit.
--   openyear BETWEEN 1900 AND 2100  and  TRIM(periodid) <> ''  matched 0 rows. Harmless, but
--     they add no measured value today.
--
-- ONE DECISION IS NOT OURS: line_not_degenerate drops 7,523 rows from the history feed
-- (0.05%), an order of magnitude more than from the current-year feed. A GL line with
-- neither a debit nor a credit has no accounting effect, but whether those rows should be
-- withheld from the vault or landed as-is is a finance question. Recommend declaring the
-- two amount_not_negative rules first, which are unambiguous and total 12 rows across 19M.
-- ---------------------------------------------------------------------------------------

INSERT INTO `${catalog}`.`${control_schema}`.ref_dq_expectation VALUES

  ('nhl_general_journal_line', 'amount_not_negative',
   'COALESCE(debitamt >= 0 AND crdtamnt >= 0, TRUE)', true, 'drop'),

  ('nhl_general_journal_line_closed_year', 'amount_not_negative',
   'COALESCE(debitamt >= 0 AND crdtamnt >= 0, TRUE)', true, 'drop'),

  ('sat_job_request_details_bullhorn_eu', 'openings_not_negative',
   'COALESCE(numopenings >= 0, TRUE)', true, 'drop'),

  ('sat_job_request_details_bullhorn_eu', 'title_not_blank',
   'COALESCE(TRIM(title) <> '''', TRUE)', true, 'drop');

-- ---------------------------------------------------------------------------------------
-- WITHDRAWN 27 Aug: the rule below was WRONG, not merely a finance decision.
--
-- It was held on the grounds that dropping 7,523 zero-value lines from the history feed was
-- a finance question. Investigating those rows showed it was not a question at all:
--
--   feed      zero functional   of which ORIGINATING amount is non-zero   genuinely empty
--   gl20000       202                          64                             138
--   gl30000     7,523                       7,366  (98%)                      157
--
-- Those are FOREIGN-CURRENCY LINES. The functional-currency amount is zero while
-- ordbtamt / orcrdamt carry the real originating value. The naive rule would have
-- quarantined 7,366 legitimate lines from one feed -- silently, because `drop` is the only
-- tier and a quarantined row simply does not land.
--
-- The correct rule requires BOTH pairs to be zero. Measured: 138 + 157 = 295 rows across
-- 19,056,967, scattered thinly (at most 12 lines in any one account) rather than clustered
-- in a memo or statistical account.
--
--   ('nhl_general_journal_line', 'line_carries_no_amount',
--    'COALESCE(NOT (debitamt = 0 AND crdtamnt = 0 AND COALESCE(ordbtamt,0) = 0
--                   AND COALESCE(orcrdamt,0) = 0), TRUE)', true, 'drop'),
--   ('nhl_general_journal_line_closed_year', 'line_carries_no_amount',
--    'COALESCE(NOT (debitamt = 0 AND crdtamnt = 0 AND COALESCE(ordbtamt,0) = 0
--                   AND COALESCE(orcrdamt,0) = 0), TRUE)', true, 'drop')
--
-- STILL NOT APPLIED, and now for a smaller and answerable question: a line with no amount in
-- any currency has no accounting effect, but it may be a voided or reversed entry that
-- finance expects to see in the vault. 295 rows is a reviewable list. Note the journal gate
-- stays green either way -- dropping a zero line changes neither the debit nor the credit
-- total, no control total is declared for this journal, and loop-1 counts quarantined rows.
--
-- The general lesson, which is why this is written down rather than deleted: the FIRST rule
-- looked defensible, was measured, and the measurement was read as "0.05% -- a judgement
-- call". It took looking at WHAT those rows were to see the rule was destructive. A drop
-- rate is not enough. Characterise the rows the rule would remove.
-- ---------------------------------------------------------------------------------------
