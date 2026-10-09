# The control schema: load-time technical audit, in a schema of its own

Decided 26 Aug 2026. This is the first of three specs. It gives the other two — error
reintegration, and per-table data contracts — a place to live, and does nothing else.

## 1. What already exists, because most of this is not new

The request was "a schema that handles the non-governance technical audit of data loads,
accepted rows and rejected rows at loading time". Measured against the lake first, most of
that is already modelled — it is just spread across two schemas and one of them is named
for something else:

```
governance.ctl_approval_manifest    loop-1's "approved" side. 0 rows.
governance.ref_dq_expectation       the governed DQ rules. 0 rows.
governance.pipeline_event_log(_bv)  SDP's own event logs, one per pipeline.
governance.mask_*                   four mask functions.
raw_vault.qtn_*                     six quarantine twins: rejected rows retained
                                    verbatim with failure_rule / failure_detail. 0 rows.
```

`checks/loop1_reconciliation.py` already asserts `landed + quarantined = approved` per
manifest. So the rejected-row *store* exists and the reconciliation *arithmetic* exists.

**What does not exist is the record of what each writer actually did.** That is the gap this
spec fills, and it is a real one — see section 3.

**Two facts that shape everything below:**

* Every `qtn_*` table holds **0 rows**. The rejection path has never fired in production, so
  any code that counts rejects will be exercised for the first time by this work.
* `governance` currently mixes masks and grants with load control. `ctl_` and `ref_` are
  control objects; `mask_*` is governance. One of the two is in the wrong schema.

## 2. Layout

```
control                                  NEW, created by a new job task -- see 2.1
  ctl_approval_manifest    moved         external producer writes it
  ref_dq_expectation       moved         we curate it
  aud_load_run             new           two rows per run: opened, completed
  aud_table_load           new           one row per (run, table)
  aud_table_discard        new           one row per (run, table, reason)

governance                               keeps only what it is named for
  mask_money, mask_personal_name, mask_tax_reference, mask_tokenised_account
  pipeline_event_log, pipeline_event_log_bv    SDP's own, not ours to move
```

Both moved tables are at 0 rows, so this is a create-and-drop rather than a migration. There
is no data to carry and no cutover window.

### 2.1 Nothing runs `control_objects.sql` today, and that is the first thing to fix

**Found in review of this spec, not before it.** An earlier draft said the schema is
"created by `governance/control_objects.sql`". That file is executed by **nothing**:
`checks/apply_governance.py:34` reads only `apply_masks.sql`, and no job task references
`control_objects.sql` at all. `ctl_approval_manifest` and `ref_dq_expectation` exist because
the file was applied **by hand**, once — `OPEN_ITEMS:441` records "Both tables now exist"
with no mechanism behind it.

So the spec's foundation was a manual step, under four gates that would then depend on it.
That is not acceptable for something `append_only_check` and `schema_grant_check` are being
pointed at.

**Fix: a new task, `create_control_objects`, placed second.**

```
0. assert_hash_parity        gate zero, unchanged -- nothing displaces it
1. create_control_objects    NEW: creates control, its tables, and writes aud_load_run
                             'opened'
2. create_mask_functions     unchanged
3. raw_vault                 unchanged
...
N. publish_model_metadata    also writes aud_load_run 'completed'
```

It runs a new `checks/apply_control_objects.py` over `governance/control_objects.sql`,
importing `render()` and `statements()` from `apply_governance.py` rather than copying them —
a second renderer is exactly the kind of duplicate definition this repo has already been
bitten by twice (`BUSINESS_KINDS`, the system-column set).

**Why second, and not first.** An earlier draft assigned the `opened` row to
`assert_hash_parity` on the grounds that it is the first task. That was impossible: task 0
runs before anything that could create the `control` schema, so it would be writing to a
schema that may not exist. Gate zero also has no business writing anything — it proves
digests and stops. The task that *guarantees* the schema exists is the first one that can
write to it, so it does.

This also mirrors an ordering the job already relies on: `create_mask_functions` must precede
`raw_vault` because the pipeline declares MASK clauses that need the functions to exist
(DEF-20). Control objects stand in the same relation to every writer that audits.

### The manifest moves, and that is a cross-team change

`ctl_approval_manifest`'s own table comment says it is "populated by whatever approves a
batch for loading -- **NOT by this repo, which would make it self-certifying**". So its
address is an interface, not an internal detail.

**Decision: it moves, and there is nothing to break.** Measured 26 Aug 2026 —
`01_usnc_bronze_dev` holds 44 schemas, every one of them either a source schema
(`great_plains_raw`, `ukg_raw`, …) or `monitoring` / `reporting` / `test`. There is **no
`governance` schema and no `control` schema in Bronze at all**, so no producer has been built
against `governance.ctl_approval_manifest` and the move costs nobody any rework.

The earlier draft of this section carried a risk that the Bronze team might already have
built against the old address. That risk is now measured out rather than mitigated. It also
means a compatibility view at the old address is not merely unnecessary but would be actively
misleading — Unity Catalog will not accept an INSERT through a view, so it would serve
readers and silently fail a producer.

**BRZ-12 therefore needs no amendment for the address.** It stays what it was: `manifest_id`
on four feeds. The address is simply stated to whoever builds the producer.

### Assumption: Bronze gets its own governance and control schemas

Bronze needs governance and control of its own for the incoming data — DQ, audit and
approval for the ingest half. **That is a separate implementation and is out of scope here.
This spec assumes those schemas exist.**

The consequence for this design is small, and it is already handled:
`checks/loop1_reconciliation.py:145` takes `--manifest-table`, defaulting to
`{catalog}.governance.ctl_approval_manifest`. Because the location is already a parameter,
**the question of which catalog ultimately holds the manifest does not have to be answered
now.** If the approval manifest ends up in Bronze's control schema rather than silver's, that
is a changed argument in `vault_job.yml`, not a code change. The default in
`loop1_reconciliation.py` moves to `{catalog}.control.ctl_approval_manifest` so that it names
a real place, and the flag remains the way to point it somewhere else.

What this spec does own, unambiguously, is the **vault load** audit —
`aud_load_run` and `aud_table_load`. Those are records of what our loaders did and belong in
our catalog regardless of where the ingest-side control surface lands.

### `${control_schema}`, and a literal that should never have been one

`governance/control_objects.sql` currently writes `` `${catalog}`.governance.ctl_… `` — a
hardcoded schema name, even though `var.governance_schema` exists and is passed to
`publish_metadata`. Adding `control` is the moment to fix that: both schema names become
placeholders, so neither can drift from the bundle variable that claims to control it.

## 3. The three new tables, and why the event log is not enough

The obvious question is whether SDP's own event log already records this. Measured:

```
flow_progress rows total                     2,759
  ... carrying num_output_rows                 162
  ... carrying an `expectations` key              0
```

A populated one carries `metrics.num_output_rows` and `data_quality.dropped_records` /
`warned_records`. So the log gives per-flow row counts and a drop count, but **no per-rule
reason breakdown**, and it is keyed by flow and pipeline update rather than by manifest.

**The decisive gap: the event log cannot see the batch loaders at all.** `load_hubs.py` and
`load_satellites.py` are `spark_python_task`s, not pipeline flows. They are also where the
vault's most interesting arithmetic happens, and all of it is currently unrecorded:

```
hub_accounting_journal      4,444,172 GP rows  ->  2,221,108 hub rows   (anti-join)
sat_job_request_details…       69,208 staged   ->     46,889 versions   (hashdiff)
```

Both discards are correct by design — a hub conforms an identity, a satellite stores only
change — but nothing anywhere records how many rows were discarded or why. That is the
difference between `landed != staged` being explained and being merely observed.

### `control.aud_table_load` and `control.aud_table_discard`

**Two tables, not one, and that is a correction made in review of this spec.** The earlier
single-table design repeated `staged` on one row per discard reason, which forced the rule
"sum `accepted`, but take `max` of `staged`, never sum it". That is a trap: the first person
to write `SELECT sum(staged)` gets a wrong number, and no gate would catch it. It also put a
nullable column (`discard_reason`) inside the grain key, and SQL NULL does not compare equal
— so the uniqueness of that grain could not be asserted with a plain equality join, and the
"discarded nothing" row was precisely the case that hit it.

Normalising removes both problems rather than documenting them.

```sql
control.aud_table_load                        -- one row per (job_run_id, table_name)
  job_run_id          STRING     NOT NULL     -- {{job.run_id}}, verified: see 4.1
  pipeline_update_id  STRING                  -- ties to the event log; NULL for batch loaders
  table_name          STRING     NOT NULL
  written_by          STRING     NOT NULL      -- 'load_hubs.py' | 'load_satellites.py' | ...
  staged              BIGINT     NOT NULL      -- rows the writer read from its source
  accepted            BIGINT     NOT NULL      -- rows it inserted
  recorded_at         TIMESTAMP  NOT NULL

control.aud_table_discard                     -- one row per (job_run_id, table_name, reason)
  job_run_id      STRING     NOT NULL
  table_name      STRING     NOT NULL
  discard_reason  STRING     NOT NULL          -- 'already_present' | 'unchanged_hashdiff' | ...
  discarded       BIGINT     NOT NULL
  recorded_at     TIMESTAMP  NOT NULL
```

`staged` and `accepted` are now stated once per table per run, so both are safely summable
and `staged - accepted` is the total discarded — which gives the completeness check something
to assert: **`staged - accepted` must equal `SUM(discarded)` over that table's discard rows.**
A discard the writer failed to attribute shows up as an arithmetic gap rather than as nothing
at all.

A writer that discards nothing writes its `aud_table_load` row and **no** discard rows. It
must still write that row — "no row" means "did not run", which is the whole point of
section 5. `discard_reason` is `NOT NULL` because it no longer has to represent absence.

### `control.aud_load_run`

```sql
job_run_id      STRING     NOT NULL
phase           STRING     NOT NULL      -- 'opened' | 'completed'
target          STRING     NOT NULL      -- bundle target, e.g. usnc_tds
active_sources  STRING                   -- the declared list this run was given
recorded_at     TIMESTAMP  NOT NULL
```

**Two rows per run, not one row updated.** An `UPDATE` would put `control` outside
`append_only_check` for ever, which is the property that makes the audit worth reading. A
run with no `completed` row did not finish.

**Who writes these two rows**, since "each writer records only what it did" does not answer
it: **`create_control_objects` writes `opened`** and `publish_model_metadata` writes
`completed`. See 2.1 for why it is not `assert_hash_parity` — task 0 runs before the schema
exists, and gate zero has no business writing anything. `create_control_objects` is the first
task that *can* write, because it is the task that guarantees the target exists.

Both tasks already fail the run on their own account, so the section 5 rule applies
unchanged: if either cannot write its row, the run stops.

`aud_load_run` is the part to cut if this needs to be leaner: `aud_table_load` keyed on
`job_run_id` already gives the grouping, at the cost of not being able to distinguish "the
run is still going" from "the run died before this table".

## 4. Each writer records only what it did

Rejected: one check that reads the event log, the staging logs, the vault and the twins
afterwards and computes everything. It would need no loader changes, but it would
*reconstruct* the anti-join and hashdiff arithmetic by differencing counts after the fact —
and when a number then disagreed, there would be no way to tell whether the load or the
audit was wrong. That is precisely the ambiguity loop-1 exists to remove.

So each writer records its own numbers:

| writer | records |
|---|---|
| `checks/load_hubs.py` | staged, already_present, inserted — per hub |
| `checks/load_satellites.py` | staged, unchanged_hashdiff, versions_inserted — per satellite |
| a new check over the event log | num_output_rows, dropped_records, warned_records — per flow |

### 4.1 The run key is `{{job.run_id}}` — VERIFIED, not assumed

`factory._batch_id()` resolves `pipelines.updateId`, which exists only *inside* a pipeline
update. The batch loaders are separate job tasks and cannot see it. So the join key is the
job run id, passed to every writer as a task parameter, with `pipeline_update_id` recorded
alongside it so the pipeline half and the batch half of one run tie together.

**The whole audit keys on this, so it was proven rather than asserted (26 Aug 2026).** A
one-time run was submitted with `--actual-host RUNID={{job.run_id}}` against the already
deployed `preflight_target.py`, and the process printed:

```
CLI resolves  RUNID=507147181456340   (from --actual-host)
```

The submitted run id was `507147181456340`. So the reference resolves in a
`spark_python_task` parameter and the process receives the resolved value.

**One trap this exposed, worth writing down:** `jobs get-run` reports the parameter
**unresolved** — it echoes `RUNID={{job.run_id}}` back. Resolution is visible only in what
the process actually received. Anyone verifying this from the API alone would conclude it does
not work.

The probe used an existing deployed file and created no objects, so it left nothing to clean
up — which matters here of all specs, given `silver_vault` exists because a probe did not.

## 5. Failure semantics: an unaudited load is a failed load

If a writer commits its rows and then fails to write its audit row, **the task exits 1**.

The guarantee that buys: a gap in `aud_table_load` always means a batch did not complete. It
never means an unrecorded success. Without it, the audit is best-effort and the only thing
standing between it and silent incompleteness is one more check that would itself have to be
proven able to fail.

Retry is safe, and this is why the ordering works: the vault tables are append-only and both
the hub anti-join and the satellite hashdiff filter are idempotent, so re-running a task
whose audit write failed re-reads the same source, inserts nothing new, and writes the audit
row it missed.

## 6. What asserts this

| gate | change |
|---|---|
| `append_only_check` | add `--schema control`. An audit that can be rewritten is not an audit. |
| `schema_grant_check` | add `--schema control` to the sweep, bringing it inside the Phase 6STOP posture — nobody reads control directly either. |
| **new** — orphan schema | fail if the catalog holds an undeclared schema. This is the check that would have caught `silver_vault`. See the note below: not a name list. |
| **new** — audit completeness | every `job_run_id` in `aud_table_load` has a `completed` row in `aud_load_run`, **and** `staged - accepted = SUM(discarded)` for every audited table. The second half is what makes an unattributed discard visible. |

Both new checks must be **proven able to fail** before they are believed — the standing rule
in this repo, which has shipped five checks that could not fail. Each gets a mutation that
makes it fire, recorded in the commit.

**The orphan-schema check is weaker than it first reads, and the design has to account for
that.** It must tolerate `information_schema`, which is system-owned and always present, and
possibly `__databricks_internal`. So the naive form is "the declared set plus an exclusion
list" — and that exclusion list is exactly where the next orphan hides. A name the check has
been taught to ignore is indistinguishable from a name it should have caught.

So it keys on two things that are harder to fool than a name:

* **the declared set comes from the bundle variables** (`vault_schema`,
  `business_vault_schema`, `governance_schema`, `control_schema`), never a literal in the
  check — so a schema this repo creates is declared by construction;
* **system schemas are identified by owner, not by name.** `information_schema` is owned by
  `System user`; every schema this repo creates is owned by the deploying principal. An
  undeclared schema with a non-system owner is the finding, whatever it is called.

`silver_vault` is owned by `adrian.turcu@headfirst.group` and is undeclared, so it fails that
rule — which is the check working on the one real example available.

A second assertion is then available for free, since `control` and `governance` already mark
their tables `TBLPROPERTIES ('hfig.control_object' = 'true')`: every control object lives in
`control`, and none is left behind in `governance`.

## 7. Scope

**In:** the schema, the two moved tables, the two new tables, instrumentation in the two
loaders and one new event-log check, the four gate changes, and the BRZ-12 amendment.

**Out, and deliberately:**

* **Error reintegration** — its own spec. It has to resolve two collisions first:
  `append_only_check` means a corrected row cannot update a vault row, so it must re-enter as
  a new load; and loop-1's `landed + quarantined = approved` double-counts a row that was
  quarantined and later lands, unless the transition is modelled explicitly.
* **Data contracts** — its own spec. The first question there is what a contract adds that
  `metadata/entities/*.yml`, `ref_dq_expectation` and `key_composition.json` do not already
  declare between them.
* **`silver_vault` and the probe objects** — a cleanup, tracked separately in OPEN_ITEMS. It
  is what motivated the orphan-schema gate, but dropping four objects needs no spec.
* **Bronze's own governance and control schemas** — needed for the incoming data, a separate
  implementation, assumed by this spec to exist. Nothing here creates or governs anything in
  `01_usnc_bronze_dev`, which this repo only reads (DEF-46).

## 8. What this does not do

It does not make the rejection path work. Every `qtn_*` table is empty, so `discarded` and
the reject counts will be exercised for the first time by whatever load first produces a real
rejection. This spec records rejects faithfully when they happen; it does not demonstrate
that they happen.

It also does not populate `ctl_approval_manifest` or `ref_dq_expectation`. Both remain empty
after this work — the first because populating it ourselves would be self-certifying, the
second because it is a capability rather than a blocker, and the generator falls back to its
two compiled-in key-safety rules when it is empty.
