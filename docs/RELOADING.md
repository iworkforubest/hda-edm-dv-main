# Rebuilding a vault table

A vault table cannot be altered in place. This is the procedure, written after doing it
twice on 25 August 2026 — once badly, once correctly.

## Why there is no easier way

Vault tables carry `delta.appendOnly = true`, which is the property
`checks/append_only_check.py` exists to enforce. A full refresh RESETS a table, which is
a truncate, which Delta refuses:

    [DELTA_CANNOT_MODIFY_APPEND_ONLY] This table is configured to only allow appends.

So changing a vault table's shape means **dropping and rebuilding it**. There is no
reset-in-place path that keeps the table.

## The procedure

**1. Validate the graph while the vault is still populated.**

```bash
databricks bundle run raw_vault --validate-only -t <target> --profile <profile>
```

Ninety seconds. This is the step whose absence turned a schema change into a four-hour
outage on 25 August: the pipeline was dropped first, then failed at graph analysis, and
there was nothing to roll back to. Never skip it.

**2. Capture the before-state into git.** Not into scratch — the first attempt's only
record was gitignored and nearly lost.

```bash
uv run python docs/superpowers/evidence/verify_reload_parity.py \
  --capture docs/superpowers/evidence/<date>-pre-<change>-vault-state.json \
  --catalog <catalog> --profile <profile> --tables "raw_vault.hub_x,raw_vault.nhl_y"
```

**3. Drop only what actually changes, and drop the twins by name.**

`DROP TABLE` on a pipeline-owned streaming table works, but **does not cascade to its
`__materialization_mat_<pipeline-id>_<table>_1` twin**. Dropping only the streaming table
strands the backing storage — cleartext, with no masked object above it.

Work out which tables the change actually affects. A pattern match over table names will
over-select: on 25 August it caught three satellites that a hub/NHL-only change did not
touch, and dropping them would have been needless destruction.

**4. Refresh by name, never `--full-refresh-all`.**

```bash
databricks bundle run raw_vault -t <target> --profile <profile> \
  --full-refresh <comma-separated list of the dropped tables>
```

`--full-refresh-all` targets every dataset in the graph and dies on the first append-only
table it reaches, whether or not you meant to rebuild it. Naming the dropped tables works
because a dropped table has nothing to truncate.

If an update is already running the command fails with "An active update already exists".
A failed full refresh **auto-retries the same request**, so check the pipeline is IDLE
before re-issuing:

```bash
databricks pipelines get <pipeline-id> --profile <profile> -o json
```

**5. Verify against the capture.**

```bash
uv run python docs/superpowers/evidence/verify_reload_parity.py \
  --before docs/superpowers/evidence/<the file from step 2> \
  --catalog <catalog> --profile <profile>
```

Row counts alone prove nothing — two different hashing rulebooks produce identical
counts. The comparison is over the hash keys themselves, using order-independent digests
so it cannot be fooled by a different physical layout.

Column counts are expected to differ when the change was a reshape; the tool reports them
and does not assert on them.

## Reading a mismatch

On 25 August, 10 of 17 keys differed and every one was the same finding: the ghost row's
parent hash keys had moved from NULL to the zero key, because
`_ghost_column_sql`'s `_hk` rule was introduced in the same commit as the reshape.
Excluding the ghost row, all ten reconciled exactly.

Check whether a difference is uniform before treating it as damage. A constant delta
across many keys — there, `sum(crc32)` up by exactly 884,073,675, which is `crc32` of the
zero key — points at one row, not at corrupted data.


## Verify the SHAPE, not only the hashes

The parity tool compares hash keys. It does **not** assert column counts — a reshape is
usually the reason for the reload, so it reports them and moves on.

That gap cost a second reload on 25 August. The narrowing landed correctly on the masked
NHLs and silently failed on the unmasked hubs: `hub_accounting_journal` came back with
its 11 correct columns and then `sub_seq` and `cdc_op` appended at positions 12 and 13.

The cause is worth knowing before you reshape anything:

**A table with no declared schema is inferred from the UNION of its flows.** A masked
table declares its schema explicitly, so its shape is exactly what the generator says.
An unmasked one does not, so *any* flow that emits an extra column puts that column back
— silently, at the end. There the ghost flow was still supplying the two removed columns.

The quarantine twins were all correct, which is what pinned the cause: they have no ghost
flow.

So after any reshape, check the shape against the declaration:

```sql
SELECT table_name, count(*) AS cols,
       max(CASE WHEN column_name IN ('<removed>', ...) THEN 1 ELSE 0 END) AS has_stray
FROM <catalog>.information_schema.columns
WHERE table_schema = 'raw_vault'
GROUP BY table_name ORDER BY has_stray DESC, table_name
```

An unmasked table matching the declaration is the thing to confirm, not assume.

## Worked case: the `job_request` tenant re-key, 4 September 2026

The largest re-key so far, recorded because the tables to drop were not the obvious two.

`hub_job_request` moved from `key_style: federated` with one business-key position to
`tenant_scoped` with two, because Fieldglass is multi-tenant — 200+ US clients, each its
own tenant — and a posting reference is unique only *within* a tenant. The old key would
have merged two clients' postings into one hub row, and that failure is silent: the row
count stays plausible and every gate passes.

**Every stored key changes.** Verified against the reference implementation:

    old  SHA-256('BULLHORN_EU', '12345')                    4140a6de…
    new  SHA-256('BULLHORN_EU', 'NOT_APPLICABLE', '12345')  fac7ff84…

**THREE tables, not two.** The obvious pair is the hub and its satellite. The third is the
one that bites:

| table | rows (usnc_tds, 4 Sep) | why it must go |
|---|---|---|
| `hub_job_request` | 45,517 | every hash key changes |
| `sat_job_request_details_bullhorn_eu` | 46,889 | its FK points at the old keys |
| `stg_hub_job_request` | 45,517 | **the staging log** |

The staging log is the trap. A hub loads through it with an anti-join, so leaving it in
place does **not** suppress the new rows — the new keys differ from the old ones, so the
anti-join sees them as unseen and appends them *alongside* 45,516 orphaned old rows. The
hub then holds two keys for every job request, both well-formed, and nothing fails. Drop
the staging log in the same change as the hub, always.

The general rule this case makes concrete: **when a key changes, drop every table that
stores that key**, and `naming.STAGED_KINDS` is the list of kinds that store it twice.

### One thing that did NOT need a reload

Six hub-parented satellite bindings had to declare `parent_keys` as part of this change.
Without it they take `factory`'s fallback — `hash_key(src.key_columns)` — which knows
nothing about the `NOT_APPLICABLE` literal the hub now supplies, so each would have
become a *new* orphan the moment the hub was re-keyed. That is a metadata change with no
data consequence, because those satellites hold no rows in this lake yet. Had they held
rows, they would have joined the drop list.

The gates caught all six before anything was applied: `verify_repo`'s scope-orphan check,
and `tests/test_spark_derivation.py` comparing each child's derived key against its
parent's on shared business values in real Spark.
