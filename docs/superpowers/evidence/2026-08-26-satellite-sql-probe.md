# Satellite loader SQL probe — 2026-08-26

## Why this exists

`ALTER CATALOG ... SET ISOLATION MODE ISOLATED` sat in `governance/apply_masks.sql` for
weeks and turned out to be a **PARSE ERROR** on this runtime — discovered only when
someone finally ran the file. The satellite change-detection loader (Task 3) depends on
three SQL constructs that had never been run against this workspace:
`IS DISTINCT FROM`, `LAG(...) OVER (...)` inside an `INSERT ... SELECT`, and `USING` on a
join key (`mas_key`) that is NULL for every non-multi-active satellite. This probe runs
each construct as an isolated minimal repro before any loader code is written against it.

All queries were run with:

```
databricks experimental aitools tools query "<SQL>" --profile hfig-usnc-tds -o json
```

Profile `hfig-usnc-tds` only. Reads were against no persisted tables (VALUES literals);
the one write (Step 3) was to `02_usnc_silver_edm_dev.governance._probe_lag`, dropped
immediately after and confirmed gone (see "Cleanup" below).

## Runtime tested

There is no single "the runtime" here — three distinct compute surfaces are in play, and
they are not proven to be the same version. Naming them separately, with which fact came
from which surface:

**1. The SQL warehouse — where every probe in this document actually ran.**

All queries above went through `databricks experimental aitools tools query "<SQL>"
--profile hfig-usnc-tds -o json`, which executes against a SQL warehouse, not a pipeline
cluster. To identify that warehouse's own version directly (rather than inferring it from
an unrelated cluster), this was run:

```sql
SELECT current_version()
```
→
```json
[{"current_version()": "{\"dbr_version\":null,\"dbsql_version\":\"2026.32\",\"u_build_hash\":\"201497a9e4b15fa5bdee1ebebb20ffac77c78c46\",\"r_build_hash\":\"9a8e9452864f14b93c7dd96b87e06d45462d296a\"}"}]
```

So: the warehouse that ran every probe in this document is on **DBSQL 2026.32**
(`dbr_version` is null in this response — DBSQL warehouses report a `dbsql_version`
instead of a DBR version string). This is the one surface in this document backed by a
direct version query rather than an inference.

**2. The DLT pipeline cluster — a different surface, looked up for context only.**

The pipeline the brief pointed at (`a85e1be6-c4e4-425a-983b-a8a2f11857c6`, `[usnc_tds]
hfig raw vault`) was inspected because the brief asked for a `dbr_version` from its
`create_update` events:

- `databricks pipelines list-pipeline-events a85e1be6-c4e4-425a-983b-a8a2f11857c6
  --profile hfig-usnc-tds --max-results 40 -o json` was run and every event checked.
  **None carry a `dbr_version` field** in `origin` or elsewhere (confirmed by grepping
  the full 40-event dump — zero matches). Recording the discrepancy rather than
  fabricating a value.
- Fallback: the pipeline's most recent successful update (`b6350c21`, completed
  2026-08-25T21:13:55Z) ran on cluster `0825-205636-rg6fmr2r-v2n`, a serverless DLT
  cluster (`kind: SERVERLESS_DLT`). `databricks clusters get 0825-205636-rg6fmr2r-v2n
  --profile hfig-usnc-tds -o json` reports:
  ```
  "spark_version": "dlt:18.3.4-delta-pipelines-photon-dlt-release-dp-20260820-rc1-commit-f097385-image-ff26560"
  ```
  i.e. DLT/Photon runtime **18.3.4** (release build `dp-20260820-rc1`), Photon,
  serverless.

This cluster ran **none of this document's probes**. It is recorded only because the
brief asked for it and because it establishes that this workspace's DLT surface and its
SQL-warehouse surface use *different version identifiers entirely* (`dlt:18.3.4-...` vs.
`dbsql_version 2026.32`) — they are not directly comparable, and no claim is made here
that they are the same build.

**3. Serverless job compute — where the loader will actually run. NOT probed.**

`checks/load_satellites.py` (Task 3+) will execute as a `spark_python_task` on
serverless job compute, a third surface distinct from both of the above. Its version was
**not queried** — probing it would require submitting a live job run for a version
string, which is not worth doing at this stage (and the brief did not ask for it). This
is a real, currently-open gap: neither the SQL-warehouse version (2026.32, confirmed
above) nor the DLT cluster version (18.3.4, confirmed above) is evidence of what
serverless job compute runs.

**Residual risk.** All three constructs probed in this document (`IS DISTINCT FROM`,
window `LAG`, and standard `ON`-clause joins) are ANSI-standard SQL, so divergence in
their behavior across these three compute surfaces is unlikely — but it is unproven, not
ruled out, for the one surface that matters most (serverless job compute) since it was
never queried. Task 6's first real load against that surface is what will actually settle
it; until then, treat the construct verdicts below as "verified on the SQL warehouse
(DBSQL 2026.32), not yet verified on the loader's own execution surface."

## Verdicts

### 1. `IS DISTINCT FROM` + `LAG` in a subquery — **PASS**

Statement run:

```sql
WITH t AS (SELECT * FROM VALUES
    ('k1', 'A', TIMESTAMP'2026-01-01', 0),
    ('k1', 'A', TIMESTAMP'2026-01-02', 0),
    ('k1', 'B', TIMESTAMP'2026-01-03', 0),
    ('k1', 'A', TIMESTAMP'2026-01-04', 0)
  AS v(parent_hk, hashdiff, load_dts, sub_seq)),
w AS (SELECT *, LAG(hashdiff) OVER (PARTITION BY parent_hk ORDER BY load_dts, sub_seq) AS prev FROM t)
SELECT load_dts, hashdiff, prev, (hashdiff IS DISTINCT FROM prev) AS keep FROM w ORDER BY load_dts
```

Output:

```json
[
  {"load_dts": "2026-01-01T00:00:00.000Z", "hashdiff": "A", "prev": null, "keep": "true"},
  {"load_dts": "2026-01-02T00:00:00.000Z", "hashdiff": "A", "prev": "A",  "keep": "false"},
  {"load_dts": "2026-01-03T00:00:00.000Z", "hashdiff": "B", "prev": "A",  "keep": "true"},
  {"load_dts": "2026-01-04T00:00:00.000Z", "hashdiff": "A", "prev": "B",  "keep": "true"}
]
```

`keep` = `true, false, true, true` — exactly the expected A → A → B → A collapse of the
consecutive duplicate, with the return to A on row 4 kept. **PASS as written.** No
alternative form required; `IS DISTINCT FROM` and windowed `LAG` in a plain subquery both
work on this runtime.

### 2. `USING (parent_hk, mas_key)` on a nullable join column — **WRONG, required alternative confirmed**

Statement run:

```sql
WITH a AS (SELECT * FROM VALUES ('k1', CAST(NULL AS STRING)) AS v(parent_hk, mas_key)),
     b AS (SELECT * FROM VALUES ('k1', CAST(NULL AS STRING), 'X') AS v(parent_hk, mas_key, hashdiff))
SELECT a.parent_hk, b.hashdiff FROM a LEFT JOIN b USING (parent_hk, mas_key)
```

Output:

```json
[
  {"parent_hk": "k1", "hashdiff": null}
]
```

One row, `hashdiff = NULL` — the row from `b` (`hashdiff = 'X'`) did **not** match, even
though `a.mas_key` and `b.mas_key` are both NULL for the same `parent_hk = 'k1'`. This
confirms the predicted failure mode: `USING` compiles to `=`, and `NULL = NULL` is not
true, so `USING` silently fails to match rows where `mas_key` is NULL — which is every
row for a non-multi-active satellite (`sat`; only `msat` populates `mas_key`). Using
`USING (parent_hk, mas_key)` in the satellite loader would cause every plain-satellite
row to be treated as unmatched (spuriously "new"), breaking change detection.

**Verdict: WRONG. Do not use `USING` on `mas_key`.**

Required alternative, confirmed to match correctly:

```sql
... FROM a LEFT JOIN b ON a.parent_hk = b.parent_hk AND a.mas_key IS NOT DISTINCT FROM b.mas_key
```

Output:

```json
[
  {"parent_hk": "k1", "hashdiff": "X"}
]
```

One row, `hashdiff = 'X'` — the match succeeds when both `mas_key` values are NULL.
**Task 3 must join with `c.parent_hk = t.parent_hk AND c.mas_key IS NOT DISTINCT FROM
t.mas_key`, never `USING (parent_hk, mas_key)`.**

### 3. `LAG` inside `INSERT ... SELECT` — **PASS**

Statements run (against `02_usnc_silver_edm_dev.governance._probe_lag`, a scratch table
created and dropped solely for this probe):

```sql
CREATE TABLE IF NOT EXISTS 02_usnc_silver_edm_dev.governance._probe_lag (k STRING, h STRING, d TIMESTAMP)
```
→ `Query executed successfully (no results)`

```sql
INSERT INTO 02_usnc_silver_edm_dev.governance._probe_lag
SELECT k, h, d FROM (
  SELECT *, LAG(h) OVER (PARTITION BY k ORDER BY d) AS prev
  FROM VALUES ('k1','A',TIMESTAMP'2026-01-01'), ('k1','A',TIMESTAMP'2026-01-02') AS v(k,h,d)
) WHERE h IS DISTINCT FROM prev
```
→
```json
[{"num_affected_rows": "1", "num_inserted_rows": "1"}]
```

```sql
SELECT count(*) AS n FROM 02_usnc_silver_edm_dev.governance._probe_lag
```
→
```json
[{"n": "1"}]
```

Insert succeeded, `n = 1` — matches the expected count (only the first of the two
identical-`h` rows survives the `IS DISTINCT FROM` filter, since `LAG` returns NULL for
the partition's first row and NULL is distinct from nothing... concretely: row 1 has
`prev = NULL`, `h = 'A'`, `'A' IS DISTINCT FROM NULL` = true → kept; row 2 has `prev =
'A'`, `h = 'A'`, `'A' IS DISTINCT FROM 'A'` = false → dropped). **PASS as written.**
`LAG(...) OVER (...)` works unmodified inside the `SELECT` of an `INSERT ... SELECT` on
this runtime — no rewrite (e.g. no need to materialize the windowed subquery into a temp
view first) is required.

## Cleanup

```sql
DROP TABLE IF EXISTS 02_usnc_silver_edm_dev.governance._probe_lag
```
→ `Query executed successfully (no results)`

Verified gone:

```sql
SHOW TABLES IN 02_usnc_silver_edm_dev.governance LIKE '_probe_lag'
```
→ `[]` (empty — table does not exist)

## Summary for Task 3

| Construct | Verdict | Form to use in the loader |
|---|---|---|
| `IS DISTINCT FROM` (plain predicate) | PASS | Use as written |
| `LAG(...) OVER (...)` in a subquery | PASS | Use as written |
| `LAG(...) OVER (...)` inside `INSERT ... SELECT` | PASS | Use as written, no rewrite needed |
| `USING (parent_hk, mas_key)` join | **WRONG** | Use `ON c.parent_hk = t.parent_hk AND c.mas_key IS NOT DISTINCT FROM t.mas_key` instead |

## Fix report — round 1

**Reviewer finding (Important):** the "Runtime tested" section asserted, without running
any command to check it, that "the ad-hoc SQL probes below ran against the workspace's
SQL warehouse, which is governed by the same workspace runtime channel" as the DLT
pipeline cluster whose `spark_version` had been looked up. That was an unverified
equivalence claim stated as fact — exactly the failure mode this kind of evidence
document exists to prevent.

**What was actually wrong, beyond the reviewer's phrasing:** the section conflated two
surfaces (SQL warehouse, DLT pipeline cluster) when there are three relevant to this
work — the SQL warehouse (where the probes ran), the DLT pipeline cluster (looked up per
the brief's instructions, but irrelevant to the probes), and serverless job compute
(where `checks/load_satellites.py` will actually execute as a `spark_python_task`,
per Task 3+). The original section never mentioned the third surface at all.

**Command run to fix it:**

```
databricks experimental aitools tools query "SELECT current_version()" --profile hfig-usnc-tds -o json
```

Output:
```json
[{"current_version()": "{\"dbr_version\":null,\"dbsql_version\":\"2026.32\",\"u_build_hash\":\"201497a9e4b15fa5bdee1ebebb20ffac77c78c46\",\"r_build_hash\":\"9a8e9452864f14b93c7dd96b87e06d45462d296a\"}"}]
```

This directly identifies the SQL-warehouse surface as **DBSQL 2026.32** — a different
version identifier scheme from the DLT cluster's `dlt:18.3.4-...` string, confirming they
are not directly comparable and should never have been asserted equivalent.

**File changed:** `docs/superpowers/evidence/2026-08-26-satellite-sql-probe.md` — the
`## Runtime tested` section (previously lines 23-50) was rewritten to:
1. Name and separate the three surfaces (SQL warehouse / DLT pipeline cluster /
   serverless job compute) and state which recorded fact came from which.
2. Report the `current_version()` output above as the SQL warehouse's own confirmed
   version (DBSQL 2026.32), replacing the unverified equivalence claim.
3. State plainly that serverless job compute — the loader's actual execution surface —
   was **not probed**, and that this is a real, currently-open gap (deliberately not
   closed by submitting a live job run, per the coordinator's instruction that doing so
   is not worth it for a version string).
4. Add the residual-risk sentence: the three constructs are ANSI-standard SQL, so
   cross-surface divergence is unlikely but unproven for serverless job compute, and
   Task 6's first real load is what will settle it.

**Not changed:** the three construct verdicts (`IS DISTINCT FROM` PASS, `LAG` PASS both
in a subquery and inside `INSERT ... SELECT`, `USING` on `mas_key` WRONG with the
`IS NOT DISTINCT FROM` alternative confirmed) — the reviewer agreed these are well
evidenced, and the coordinator's instruction was to confine this fix to the
runtime-identification section. The Verdicts, Cleanup, and Summary for Task 3 sections
are untouched.

**Re-verification:** re-read the full file after editing to confirm the new section
reads coherently, correctly attributes each fact to its source surface, and makes no
claim beyond what was actually queried. No other probes were re-run (per instruction:
"re-run nothing else").
