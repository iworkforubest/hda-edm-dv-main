# Satellite loader behavior probe — fix round 1, 2026-08-26

## Why this exists

Review of commit `7181d37` (`checks/load_satellites.py`, DEF-52) returned two Critical
defects and one Important defect, all three in the SQL itself, all three invisible to the
15 substring-based offline checks then in `tests/test_accelerator.py`:

1. `SELECT * EXCEPT (prev) FROM incoming t LEFT JOIN current c` expands the star across
   BOTH sides of the join -- an arity mismatch against a target created exactly `|log|`
   columns wide, on every satellite.
2. `LAG` was computed AFTER the `NOT EXISTS` anti-join had already filtered the log down to
   only not-yet-loaded rows, so a row's predecessor was whatever the FILTERED subset put
   before it, not the true predecessor in the log's full history -- breaking the A -> B ->
   A guarantee this loader exists to provide, and breaking idempotency.
3. The msat anti-join grain omitted `mas_key`, so once any one skill for a worker was
   stored at a given `(parent_hk, load_dts, sub_seq)`, its never-loaded siblings sharing
   that same triple were silently treated as already-loaded and dropped.

The fix (this round) rewrites the SQL: an explicit, qualified column list
(`declared_columns()`, compared by an offline test against `factory._projection`); `LAG`
computed over the unfiltered log; `NOT EXISTS` moved into the final `WHERE`, ANDed with the
hashdiff comparison, with `mas_key` added to its grain for an msat.

A test that inspects the rendered SQL as a string cannot tell these two CTE shapes apart --
both contain `NOT EXISTS`, both contain `LAG(`, in either order. The reviewer's instruction
was explicit: execute the ACTUAL rendered statement shape against a fixture and assert on
the rows that come back, the way Task 1 probed `IS DISTINCT FROM` / `LAG` / `USING`. This
document is that execution, run against the live workspace, not simulated.

All queries were run with:

```
databricks experimental aitools tools query "<SQL>" --profile hfig-usnc-tds -o json
```

Profile `hfig-usnc-tds` only. Every statement executed below is the literal output of
`checks/load_satellites.py`'s `create_sql()` / `insert_sql()` (post-fix), captured by
running the fixed module locally and copying its rendered SQL verbatim into the query
tool -- not hand-retyped from memory. Six scratch tables were created under
`02_usnc_silver_edm_dev.governance._probe_*`, populated, exercised, and dropped; see
"Cleanup" for confirmation all six are gone.

## Fixture 1: a plain `sat`, the A -> B -> A trace

Tables: `02_usnc_silver_edm_dev.governance._probe_logx` (k STRING, hashdiff STRING,
load_dts TIMESTAMP, sub_seq INT), `..._probe_satx` (same shape, created via the loader's
own `create_sql()`).

### The rendered SQL under test (from `insert_sql(catalog, schema, "_probe_satx",
"_probe_logx", "k", is_msat=False, columns=["k","hashdiff","load_dts","sub_seq"])`)

```sql
INSERT INTO `02_usnc_silver_edm_dev`.`governance`.`_probe_satx`
WITH current AS (
  SELECT `k`, `hashdiff` FROM (
    SELECT `k`, `hashdiff`, row_number() OVER (
      PARTITION BY `k` ORDER BY `load_dts` DESC, `sub_seq` DESC) AS rn
    FROM `02_usnc_silver_edm_dev`.`governance`.`_probe_satx`) WHERE rn = 1),
staged AS (
  SELECT s.*, LAG(s.`hashdiff`) OVER (
    PARTITION BY s.`k` ORDER BY s.`load_dts`, s.`sub_seq`) AS prev
  FROM `02_usnc_silver_edm_dev`.`governance`.`_probe_logx` s)
SELECT t.`k`, t.`hashdiff`, t.`load_dts`, t.`sub_seq` FROM staged t
LEFT JOIN current c ON c.`k` = t.`k`
WHERE t.`hashdiff` IS DISTINCT FROM coalesce(t.prev, c.`hashdiff`)
  AND NOT EXISTS (
    SELECT 1 FROM `02_usnc_silver_edm_dev`.`governance`.`_probe_satx` x
    WHERE x.`k` = t.`k`
      AND x.`load_dts` = t.`load_dts` AND x.`sub_seq` = t.`sub_seq`);
```

This is run, VERBATIM (only the log's growing contents change), once per "run" below --
exactly the shape `checks/load_satellites.py` executes on a schedule against the real
log, which keeps accumulating (this repo decided against staging retention, so the log is
permanent and every run rescans it in full).

### Run 1 — log gains `A@2026-01-01`

`num_inserted_rows: 1`. `_probe_satx` = `[A@2026-01-01]`. Correct: first-ever row for the
key, nothing stored, inserted.

### Run 2 — log gains `A@2026-01-02` (consecutive duplicate)

`num_inserted_rows: 0`. `_probe_satx` unchanged = `[A@2026-01-01]`. Correct: `LAG` over
the (now 2-row) log gives this row `prev = 'A'`, `coalesce('A', ...) = 'A'`, and `'A' IS
DISTINCT FROM 'A'` is false. **Proves**: consecutive duplicates within the log collapse to
one stored row.

### Run 3 — log gains `B@2026-01-03` (genuine change)

`num_inserted_rows: 1`. `_probe_satx` = `[A@2026-01-01, B@2026-01-03]`. Correct.

### Run 4 — log gains `A@2026-01-04` (the return to A)

`num_inserted_rows: 1`. `_probe_satx` = `[A@2026-01-01, B@2026-01-03, A@2026-01-04]` --
**THREE rows**, confirmed by direct query of the table, not inferred from the affected-row
count. This is the property the loader exists to guarantee: `LAG`, computed over the
UNFILTERED log, correctly gives this row `prev = 'B'` (the true immediately-preceding log
row), so `'A' IS DISTINCT FROM 'B'` is true and it inserts, landing behind `B` rather than
being seeded from a filtered subset's wrong predecessor (which is exactly how the
pre-fix CTE ordering broke this case per the reviewer's trace).

### Run 5 — no new log rows (idempotency)

`num_inserted_rows: 0`. `_probe_satx` row count still 3 (`SELECT count(*)` confirmed).
**Proves**: a second run over an unchanged log inserts zero, including for the just-
inserted `A@2026-01-04` -- the exact idempotency break the reviewer flagged in the
pre-fix version (`NOT EXISTS` inside the filtered CTE could re-admit a row like this one
on a later idle run because its predecessor computation, and therefore its WHERE
evaluation, depended on which rows had already been filtered out).

## Fixture 2: the set-membership baseline, same log, run once

A second scratch table, `_probe_setmember_x`, same shape, populated in ONE statement
against the SAME 4-row log fixture 1 ended with:

```sql
INSERT INTO 02_usnc_silver_edm_dev.governance._probe_setmember_x
SELECT k, hashdiff, load_dts, sub_seq FROM (
  SELECT s.*, row_number() OVER (PARTITION BY s.k, s.hashdiff ORDER BY s.load_dts, s.sub_seq) AS rn
  FROM 02_usnc_silver_edm_dev.governance._probe_logx s
) WHERE rn = 1
```

This is the textbook set-membership rule spec section 3 names as the wrong alternative:
one stored row per DISTINCT `(key, hashdiff)` pair, first occurrence wins.

Result: `num_inserted_rows: 2` -- `[A@2026-01-01, B@2026-01-03]`. The return to `A` at
`2026-01-04` is **absent** -- `(k1, 'A')` already exists as a pair from the first row, so
the fourth row never qualifies. Directly beside fixture 1's 3-row result, on the identical
log:

| rule | rows stored | what `_v1`'s `LEAD(load_dts)` would report as current |
|---|---|---|
| compare-to-latest (this loader, fixed) | **3**: A, B, A | **A** -- correct |
| set membership | **2**: A, B | **B** -- wrong, permanently |

The two rules are compared to each other on the same fixture, so the difference the spec
requires this test to carry cannot be lost the way a substring check lost it before.

## Fixture 3: the msat grain (Important defect #3)

Tables: `_probe_msat_logx`, `_probe_msat_satx` (fixed grain), `_probe_msat_satx_old`
(pre-fix grain, for direct comparison), all `(k STRING, mas_key STRING, hashdiff STRING,
load_dts TIMESTAMP, sub_seq INT)`.

**Setup**, identical on both `_probe_msat_satx` and `_probe_msat_satx_old`: pre-seeded with
one row `('k1','sk1','H1', 2026-01-01, 0)`, as if a prior run had already loaded worker
`k1`'s first skill. The log then gains three rows sharing one `(k, load_dts, sub_seq)`
triple -- `('k1','sk1','H1',...)` (a duplicate of what's stored), `('k1','sk2','H2',...)`
and `('k1','sk3','H3',...)` (two skills never before loaded) -- reproducing exactly the
condition the reviewer named: `sub_seq` a literal 0, one `load_dts` shared by every skill
row of one worker's batch.

### Fixed anti-join (`x.mas_key IS NOT DISTINCT FROM t.mas_key` in the grain), against
`_probe_msat_satx`

`num_inserted_rows: 2`. Final contents: `sk1/H1` (pre-seeded), `sk2/H2`, `sk3/H3` -- **all
three skills present**. `sk1` correctly recognized as a duplicate (excluded); `sk2` and
`sk3` correctly recognized as never-loaded (inserted), because the anti-join now
distinguishes them from `sk1` by `mas_key` even though all three share `(k, load_dts,
sub_seq)`.

### Pre-fix anti-join (grain omits `mas_key`, i.e. the code this review round replaces),
against `_probe_msat_satx_old`, same log, same pre-seed

`num_inserted_rows: 0`. Final contents: `sk1/H1` **only**. `sk2` and `sk3` -- never
before loaded, genuinely new skills -- were silently dropped: `NOT EXISTS` matched them
against `sk1`'s already-stored row purely on `(k, load_dts, sub_seq)`, which every row in
this batch shares, so `mas_key`'s absence from the grain made every sibling of an
already-loaded skill look already-loaded too. This is the exact failure mode the reviewer
named, reproduced live, side by side with the fix on the identical fixture.

## Cleanup

```sql
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_logx;
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_satx;
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_setmember_x;
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_msat_logx;
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_msat_satx;
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_msat_satx_old;
```

Verified gone:

```sql
SHOW TABLES IN 02_usnc_silver_edm_dev.governance LIKE '_probe*'
```
→ `[]` (empty -- none of the six scratch tables exist).

## Summary

| property (spec section 6) | evidence |
|---|---|
| A -> B -> A stores three rows, vs two under set membership -- compared to each other | Fixture 1 runs 1-4 (3 rows) vs Fixture 2 (2 rows), same log |
| consecutive duplicates within the log collapse to one row | Fixture 1 run 2 (0 inserted) |
| the first row of a later delivery is compared to the stored latest, not treated as new | Fixture 1 runs 2-4, every row's `prev`/`coalesce` resolved against real history, not NULL |
| a second run inserts zero rows | Fixture 1 run 5 |
| the msat anti-join grain includes mas_key, so never-loaded siblings are not dropped | Fixture 3, fixed vs pre-fix, same log and pre-seed |

Not covered live: the rendered column list vs `factory._projection` (Critical defect #1's
fix). That comparison needs no workspace -- both sides are pure metadata functions -- so it
stays in the offline suite (`tests/test_accelerator.py`, DEF-52 section) rather than being
duplicated here, per the instruction to keep the live evidence and the offline suite
separated.
