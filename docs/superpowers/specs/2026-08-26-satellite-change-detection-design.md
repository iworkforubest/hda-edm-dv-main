# Satellite change detection: the fallback, built

**Date:** 26 August 2026
**Status:** design, for review
**Unblocks:** all 13 satellite tables — the vault's entire descriptive history

## 1. Why every satellite is dormant

Seven vault tables load; eighteen are declared and inactive. Eleven of the eighteen are
satellites, and the 3a spec defers them in one line:

> **Deferred until CDF lands or the fallback is built:** every satellite, including
> `sat_accounting_journal_header`.

Nothing in the vault carries a descriptive attribute today. It is hubs and transaction
links only — identity and relationships, no history.

The reason is the constraint this project keeps meeting: **a streaming table cannot read
its own contents.** A satellite inserts a new version only when a row's `hashdiff` differs
from what is already stored, and "what is already stored" is the table being written.

Without that comparison the loader cannot tell a new row from a re-delivered one, so
**every satellite re-appends every row on every run** — and, as BRZ-1 records, *no
existing gate objects*. `append_only_check` asserts uniqueness at
`(parent_hk, load_dts, sub_seq)`, and re-delivered rows arrive with a fresh `load_dts`,
so they are unique. The table grows without bound and every gate stays green.

### The streaming workarounds were tried and disproved

3a §1.3 records the probes, on `great_plains_raw.gl20000`, slice `jrnentry BETWEEN 43 AND
60` — 1,344 rows, 136 distinct hashdiffs, byte-identical on every run:

- `once=True` reported `IDLE, waiting for new data` on run 2 and never executed the
  anti-join;
- the stream-static rewrite consumed its source on run 1 and every row in both runs
  carried `probe_path = fallback_full`, meaning the anti-join branch threw.

Both produced 1,344 → 1,344 and **both looked like success**. A doubled count would have
been 2,688. The finding recorded there is the one to keep: *check the flow's run-2 event
and the branch marker, not the row count.*

What was disproved is the **streaming** anti-join. A batch one was never attempted.

## 2. What the fallback is

3a already names it, at §E3a-1:

> a second stage reading the first stage's output is precisely the shape a separate
> change-detection task needs.

That is `checks/load_hubs.py`, built on 25 August for DEF-42. The pipeline appends every
source row to `stg_hub_x`; a batch task reads the target — which a streaming flow cannot —
and inserts only the keys not already there. The hub stayed strictly append-only;
`DESCRIBE HISTORY` shows only `CREATE TABLE AS SELECT` and `WRITE`.

Satellites take the same shape, one degree harder: the hub anti-join is **set
membership**, and a satellite's is **order-sensitive**.

```
  bronze ──(SDP append flows)──► stg_sat_x        every delivered row, duplicates and all
                                     │
                       (batch task, hashdiff compare)
                                     ▼
                                  sat_x           one row per genuine change
```

## 3. The change-detection rule, and why it is not set membership

**A row is inserted when its `hashdiff` differs from the LATEST stored version for its
key — not when the pair `(parent_hk, hashdiff)` is absent from the table.**

The distinction is not academic. Take a value moving **A → B → A**:

| rule | stored | `_v1` reports current |
|---|---|---|
| latest-version compare | A, B, A | **A** — correct |
| set membership | A, B | **B** — wrong, permanently |

Set membership swallows the return to A, and `_emit_v1_view` computes `valid_to` with
`LEAD(load_dts) OVER (PARTITION BY parent_hk[, mas_key] ORDER BY load_dts, sub_seq)`, so
the history would show B as current for ever. That is silent data loss of exactly the kind
this project has repeatedly found, and it would pass every gate.

### The statement

```sql
WITH current AS (                    -- the stored latest version per key
  SELECT parent_hk, mas_key, hashdiff FROM (
    SELECT *, row_number() OVER (PARTITION BY parent_hk, mas_key
                                 ORDER BY load_dts DESC, sub_seq DESC) AS rn
    FROM sat_x) WHERE rn = 1),
incoming AS (                        -- staged rows not already loaded, each with its predecessor
  SELECT s.*, LAG(hashdiff) OVER (PARTITION BY parent_hk, mas_key
                                  ORDER BY load_dts, sub_seq) AS prev
  FROM stg_sat_x s
  WHERE NOT EXISTS (SELECT 1 FROM sat_x t
                    WHERE t.parent_hk = s.parent_hk
                      AND t.load_dts  = s.load_dts
                      AND t.sub_seq   = s.sub_seq))
INSERT INTO sat_x
SELECT <declared columns> FROM incoming i
LEFT JOIN current c USING (parent_hk, mas_key)
WHERE i.hashdiff IS DISTINCT FROM coalesce(i.prev, c.hashdiff)
```

Three properties, each load-bearing:

- **`LAG` collapses consecutive duplicates within one batch.** A full-refresh feed
  delivers unchanged rows; without this, a single run would store one version per delivery.
- **`coalesce(prev, c.hashdiff)` seeds the first row of a batch from what is stored.**
  This is what makes it correct *across* runs rather than only within one.
- **`NOT EXISTS` on `(parent_hk, load_dts, sub_seq)` makes it idempotent**, and that grain
  is deliberately the one `append_only_check` already asserts, so the loader and the gate
  agree by construction rather than by coincidence.

**`mas_key` is in every partition.** Five of the thirteen satellite tables are `msat`, so a
multi-active key is not an edge case; omitting it would collapse a worker's several skills
into one version.

**The SQL must be executed before it is believed.** `ALTER CATALOG … SET ISOLATION MODE`
sat in `apply_masks.sql` for weeks and turned out to be a parse error on this runtime, only
because nobody had run the file. `IS DISTINCT FROM`, `LAG` inside an `INSERT … SELECT`, and
`USING` on a nullable `mas_key` each need verifying on the target runtime in task 1, not
assumed.

## 4. Scope

Thirteen satellite tables: 6 `sat`, 5 `msat`, 2 `csat`.

`csat` entities read the vault rather than bronze, so they gain the same staging shape for
free — the second-stage pattern is what they already are.

**First activation: Bullhorn.** All eight columns `sat_job_request_details_bullhorn_eu`
declares map cleanly onto `01_usnc_bronze_dev.bullhorn_native_raw.joborders`
(`joborderid`, `title`, `city`, `countrycode`, `employmenttype`, `numopenings`, `status`,
`dateclosed`). It is the only satellite whose source exists in this lake and whose key does
not depend on an unresolved question. Fieldglass waits on the client mapping; UKG waits on
API ingestion; Striive, HR and ProUnity have no source here at all.

## 5. What it touches

| file | change |
|---|---|
| `src/accelerator/naming.py` | `STAGED_KINDS` gains `sat`, `msat`, `csat` |
| `checks/load_satellites.py` | **new** — the loader, rendered from metadata |
| `resources/vault_job.yml` | a task after `load_hubs` |
| `checks/loop1_reconciliation.py` | already counts the log for staged kinds; no change |
| `tests/test_accelerator.py` | as below |

`_emit_v1_view` needs no change: it reads `sat_x`, which remains append-only.

## 6. Testing

Offline:

- the A → B → A case stores **three** rows, asserted against a set-membership
  implementation which stores two — the two rules are compared to each other, so the
  difference cannot be lost
- consecutive duplicates within one batch collapse to one row
- the first row of a second batch is compared against the stored latest, not treated as new
- an `msat` partitions by `mas_key`; the same rendered SQL for a `sat` does not
- a second run inserts zero rows
- the rendered column list equals `factory._projection`'s, compared to each other

Live, after the first Bullhorn load:

- `sat_job_request_details_bullhorn_eu` row count is **less than** its staging log's, and
  the difference equals the unchanged re-deliveries
- a second run of the job inserts zero
- `DESCRIBE HISTORY` shows only `WRITE`
- `append_only_check` passes at grain `(parent_hk, load_dts, sub_seq)`
- `_v1` reports one current row per parent key

## 7. Two decisions, both now taken

**Staging retention: none. Decided 26 Aug 2026.**

The staging logs are not overhead beside the vault tables — they are how the vault tables
became correct. Measured today across the six hubs:

| | rows |
|---|---:|
| staging logs | 4,666,199 |
| the hub tables they feed | 4,128,001 |
| **overhead** | **13%** |

Retention would also cost more than it saves. A log carries `delta.appendOnly = true` and
`append_only_check` asserts no mutating operation on `stg_` tables, so deleting old rows
means relaxing an invariant, adding a gate exception to keep the relaxation honest, and
reconciling the retention window against loop-1 — which counts the log as `landed`, so a
deleted manifest can never reconcile again.

That is three moving parts to save 13% of a small table. **No retention is built.**
Revisit it when a satellite log actually hurts, which is the case to watch: a full-refresh
feed writes its whole table to the log on every run, and `joborders` is 173 columns.

**Destination, not stopgap.** Even with CDF enabled, the log remains the record of what
Bronze actually delivered — which is the thing loop-1 reconciles `approved` against. Its
value does not end when change detection stops needing it, so this is built as a permanent
part of the shape rather than as scaffolding to remove later.

## 8. What this does not do

It does not make BRZ-1 unnecessary. A change stream is better than a batch compare: it
carries deletes, which a hashdiff comparison cannot see. What this removes is BRZ-1's
status as a **blocker**.

It does not address the eight satellites whose sources are absent (UKG, Striive, HR,
ProUnity) or blocked (Fieldglass). Those are separate, and tracked in `OPEN_ITEMS.md`.
