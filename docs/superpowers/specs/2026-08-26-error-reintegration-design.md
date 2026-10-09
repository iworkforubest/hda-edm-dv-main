# Error reintegration: a wrongly-rejected row re-enters the vault

Decided 26 Aug 2026. Second of three specs. The first gave the control schema a home; this
one uses it. Data contracts remain unstarted.

## 1. The two conflicts this was blocked on, and why both dissolved

`docs/superpowers/specs/2026-08-26-control-schema-design.md` §7 deferred this work with two
stated blockers. Reading the code rather than the note, both turned out to be misstated —
and the real problem was a case that note had not identified.

**"`append_only_check` means a corrected row cannot update a vault row."** True, and
irrelevant here: a *quarantined* row never entered the vault. It is in `qtn_`. Correcting it
is a **new insert of a row that never landed**, which the append-only gate has nothing to say
about. The append-only conflict is real only for a row that DID land and is wrong — a
different case, out of scope (§2).

**"loop-1 double-counts a row that was quarantined and later lands."** Only under one
condition. `checks/loop1_reconciliation.py:215-227` reconciles **per `manifest_id`**:

```sql
expected     AS (SELECT manifest_id, approved_count FROM manifest)
landed       AS (SELECT manifest_id, COUNT(*) AS n FROM <landed> GROUP BY manifest_id)
quarantined  AS (SELECT manifest_id, COUNT(*) AS n FROM <qtn_>   GROUP BY manifest_id)
```

A correction arriving under a **new** `manifest_id` reconciles independently, and the old
manifest stays honest — that row *was* rejected then. Nothing breaks.

**The case that does break it, and which the earlier note missed:** the rejection was OUR
fault. An over-strict expectation or a mapping bug rejected good data. The fix is to change
the rule and re-process the *same* Bronze rows — but `manifest_id` is carried on the row, so
the row lands under its **original** manifest while its `qtn_` row persists, because the
quarantine twin is append-only too. That manifest then reads:

```
landed + quarantined = approved + 1
```

So the blocker was real in mechanism and wrong about the trigger. **This spec addresses that
case and only that case.**

## 2. Scope

**In:** a row rejected by an expectation that we later determine was our own mistake. Fix the
rule, re-process the same Bronze rows, let the row land, keep loop-1 green, and leave an
auditable record of what we wrongly rejected.

**Out, each for its own reason:**

* **A row whose DATA was wrong.** The source re-delivers under a new `manifest_id` and
  everything already works. `subproject3-source-rebinding-design.md:153` — "Bronze mirrors
  what each source delivers" — puts that fix upstream, where it belongs. Our part is
  detection and reporting, which the control schema already carries.
* **A row that LANDED and is wrong.** `append_only_check` forbids correcting it. The Data
  Vault answers are a new satellite version for descriptive data, or a reversal and repost
  for a transaction. Both are modelling decisions, not pipeline features, and nothing today
  even detects the case.
* **Hubs and satellites.** `RECONCILABLE_KINDS = {nhl, link, hal}` excludes them because
  dedup and change-detection break the identity by design (DEF-48), so there is no loop-1 to
  keep green for them.

## 3. The mechanism

### There is no loader to put this in, and that shapes the whole mechanism

An earlier draft of this section said "the loader" does the detection. **There is no loader
for the kinds this spec covers.** Measured:

```
STAGED_KINDS    = {hub, sat, msat, esat, csat}
=> nhl and link are NOT staged
```

`checks/load_hubs.py` owns `hub` and `checks/load_satellites.py` owns the satellite kinds.
An NHL and a link are written **directly by the `raw_vault` SDP pipeline** through
`append_flow` — no batch task touches them. So there is nothing to add a pre-insert
anti-join to, and adding one to the streaming flow is the wrong shape anyway: a flow cannot
read its own target, which is the whole reason `load_hubs` and `load_satellites` exist as
batch tasks in the first place (DEF-42, DEF-52).

**So detection is a separate batch task that runs AFTER the pipeline**, in exactly the
position and for exactly the reason those two loaders occupy. It compares what landed against
what was quarantined, rather than intercepting rows on the way in.

That is a better fit than the pre-insert form it replaces: the comparison needs both sides to
exist, and after the pipeline both do.

### The three steps

Every one append-only:

```
1. DETECT     a new batch task, after raw_vault, digests the landed rows of each
              reconcilable table and the rows of its qtn_ twin, per manifest_id

2. SUPERSEDE  for each digest present in BOTH, it writes one row per landed match to
              control.ctl_quarantine_superseded

3. RECONCILE  loop-1's identity becomes
              landed + (quarantined - superseded) = approved
```

Nothing is rewritten. `qtn_` keeps the original rejection, because the row *was* rejected and
that stays true. The supersede record is the separate, later statement that we accepted the
same content after all, and why.

## 4. The reject digest

### It digests CONTENT, and excludes system columns

`load_dts` and `batch_id` necessarily differ between the original rejected load and the later
corrected one — `load_dts` is when *we* learned it. A digest including them could never
match, so the digest covers the row's **declared** columns only and excludes every member of
`naming.SYSTEM_COLUMNS`.

### Why a content digest is the right grain, and not the hash key

The obvious alternative — match on the hash key — is unsound. DEF-48 records that an NHL
"takes the source row as it comes", and `checks/append_only_check.py:152` records
non-uniqueness at the parent grain as explicitly "not a defect". So an NHL's hash key is not
guaranteed unique per row, and matching on it would discount rejects that were never
superseded.

A content digest works **because of how this case is defined**: our rule changed, the data did
not. The corrected row's content is byte-identical to the rejected row's, so the digest
matches exactly. Had the content changed, it would be the out-of-scope upstream case, which
needs no superseding at all.

### The column set, which is NOT simply the payload

`hashing.hashdiff()` raises `RulebookError` on an empty column list, and measured against the
model, **one reconcilable entity has no payload at all**:

```
kind  entity                                 payload  transaction_key
link  lnk_client_job_request                       0                0
nhl   nhl_general_journal_line                    14                1
nhl   nhl_general_journal_line_closed_year        14                1
nhl   nhl_journal_line                             5                1
nhl   nhl_payroll_detail                          16                2
nhl   nhl_timesheet_line                           7                2
```

A link is pure structure: its content *is* its parent hash keys. So the digest is over **every
declared column of the projected row except the system columns** — which for a link is its
parent hash keys and its own, and for an NHL is the transaction key, the parent keys and the
payload. Defined that way the set is never empty, and one rule covers every reconcilable kind
without a per-kind branch.

**Hash-key columns are digested as lowercase hex, not as raw binary.** `BINARY_OUTPUT` is
RATIFIED `True`, so `hashing.key_type_sql()` returns `BINARY` and every hash key is stored as
`BINARY(32)`. The ratified normalisation in `hashing.normalise()` is defined over strings, and
a `BINARY` column's behaviour inside a string concatenation is not a property to rely on for a
comparison that must be exact. Rendering to hex first makes the digest's input a defined
string for every column, at the cost of one explicit conversion.

This is not optional detail: for `lnk_client_job_request` the parent hash keys are the *only*
digest input, so if binary rendering were ambiguous the digest would be ambiguous for that
entity and for nothing else — the worst kind of latent defect, correct on five entities and
undefined on the sixth. The probe in §7 must cover a link, not only an NHL.

### Duplicates are handled by COUNTING, not by pairing

Two identical source rows can legitimately both be rejected, and their digests are identical.
The detection task writes **one supersede record per landed row whose digest also appears in
the twin**, capped at the number quarantined for that digest — so re-running the task cannot
inflate the count, and loop-1 subtracts totals per `(manifest_id, table_name)` rather than
attempting to pair individual rows. No row-level identity is claimed anywhere.

**The cap matters for idempotency.** The task runs on every job run, and the landed rows stay
landed, so without a cap each run would emit another supersede record for the same reject and
the subtraction would grow until loop-1 went red the other way — reporting more superseded
than quarantined, which §6 makes a failure. The task must therefore be idempotent the way the
other batch loaders are: existing supersede records for a `(manifest_id, table_name,
reject_digest)` are counted and only the shortfall is written.

### The digest is coupled to the rulebook, and says so

Using `hashing.hashdiff()`'s normalisation gives one definition of how a row is digested,
which is the repo's standing preference. It also creates a coupling worth naming: if
`RULEBOOK_VERSION` is ever bumped in a way that changes that normalisation, outstanding
supersede records stop matching.

That is acceptable — a rulebook bump already re-keys the estate, so outstanding reconciliation
state is expected to be rebuilt — but it must be **detectable rather than silent**. So the
table carries the `rulebook_version` the digest was computed under, and a supersede record
from a superseded rulebook version is reported rather than quietly failing to match.

## 5. The control table

```sql
control.ctl_quarantine_superseded
  manifest_id       STRING     NOT NULL COMMENT 'the manifest the reject belonged to',
  table_name        STRING     NOT NULL COMMENT 'which qtn_ table held it',
  reject_digest     STRING     NOT NULL COMMENT 'digest over declared non-system columns',
  rulebook_version  STRING     NOT NULL COMMENT 'the version the digest was computed under',
  superseded_by     STRING     NOT NULL COMMENT 'job_run_id that landed the same content',
  reason            STRING              COMMENT 'why it was accepted this time',
  recorded_at       TIMESTAMP  NOT NULL
)
CLUSTER BY (manifest_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');
```

It goes in `governance/control_objects.sql` alongside the other five, so
`create_control_objects` creates it, `append_only_check` polices it, and the
`hfig.control_object` placement assertion covers it — all three for free, because the control
schema already exists.

## 6. What asserts this

| gate | change |
|---|---|
| `loop1_reconciliation` | the identity gains the superseded term |
| `loop1_reconciliation` | **superseded > quarantined for a manifest is a FAILURE** — see below |
| **new** | a supersede record whose `reject_digest` matches nothing in the named `qtn_` table is a FAILURE |
| **new** | a supersede record whose `rulebook_version` differs from `hashing.RULEBOOK_VERSION` (currently `1.0.0`) is REPORTED, not silently unmatched |
| `append_only_check` | already covers `control`; the new table inherits it |
| control-object placement | already sweeps `control`; the new table must carry the marker |

**The second and third rows are the load-bearing ones, and they exist because this feature can
turn a hard gate into a soft one.** The superseded term SUBTRACTS from the quarantined count.
A bug that emits spurious supersede records would therefore reduce that count and make loop-1
pass on a real variance — the gate would go green precisely when it should not. Asserting that
every supersede record corresponds to an actual reject, and that a manifest can never supersede
more than it quarantined, is what stops this mechanism from disarming the gate it exists to
serve.

Both new assertions must be proven able to fail before they are believed. This repo has shipped
five checks that could not fail, and the control-schema work found four more in its own plan
text — every one a check whose name claimed a property its condition did not test.

## 7. The probe this needs, because nothing here has ever run

**Every `qtn_` table is at 0 rows.** The rejection path has never fired in production. This
spec therefore describes a detect-and-supersede mechanism that has never seen a real
rejection, and shipping it on that basis would repeat the mistake the control-schema work kept
finding: asserting a property nothing had exercised.

So the implementation is not done until a deliberate end-to-end probe has run:

1. add a temporary expectation that rejects a known, small set of rows;
2. run the load; confirm they land in `qtn_` and that loop-1 is GREEN
   (`landed + quarantined = approved` — the reject is accounted for);
3. relax the expectation to its correct form;
4. re-run the load over the same manifest; confirm the rows now land, that
   `ctl_quarantine_superseded` gains exactly one record per row, and that loop-1 is **still
   green** — which is the whole point of the mechanism;
5. confirm loop-1 goes RED if the supersede records are removed, proving the subtraction is
   load-bearing rather than decorative;
6. repeat steps 1-4 for `lnk_client_job_request`, the one reconcilable entity whose digest
   input is nothing but hash keys — see §4 on hex rendering. An implementation correct on the
   five NHLs and undefined on the single link would pass a probe that only exercised NHLs;
7. remove the temporary expectation and confirm the estate is back to its prior state.

Step 5 is the one that matters most. Without it, the mechanism could be subtracting nothing
and every observation above would look identical.

## 8. What this does not do

It does not detect that a rejection was our fault. A human decides that, changes the rule, and
re-runs; the mechanism only keeps the books straight afterwards. Automating the judgement is
not in scope and probably never should be — an over-strict rule and genuinely bad data are
indistinguishable from inside the pipeline, which is exactly why `failure_rule` and
`failure_detail` are recorded verbatim for a person to read.

It does not populate `ref_dq_expectation`. Rules remain the two compiled-in key-safety ones
until someone curates that table, which means the first over-strict rule this mechanism exists
to recover from cannot even be written yet.

## 9. Known limitation: the quarantine twin can SHRINK, and the control table cannot

`ctl_quarantine_superseded` is `delta.appendOnly` — deliberately, because a rewritable
supersede record is a hard gate someone can silently retune (§5). The quarantine twin is
not append-only in the same sense: a **full refresh** of the `raw_vault` pipeline truncates
every `qtn_*` table and rebuilds it from the current source and the current expectations.

Those two facts combine badly. After a full refresh the twin no longer holds the rows the
existing supersede records account for, so:

* `checks/supersede_quarantine.py`'s `orphaned_records()` reports every surviving record as
  accounting for no reject, and the task FAILS; and
* `checks/loop1_reconciliation.py`'s `over_subtracted()` reports every affected manifest as
  superseding more rejects than it quarantined, and the hard gate FAILS.

Both fire **permanently**, on every subsequent run, because neither the records nor the
gate can remove them.

This is not an exotic case. It is the shape of this spec's own success story: the correction
that makes a wrongly-rejected row land is a change to an expectation, and re-running the
load with a full refresh is a normal way to apply one. It is also what a rulebook bump
produces — §4 couples the digest to `hashing.RULEBOOK_VERSION`, so a bump changes every
digest, and `_recorded_counts` reports the outstanding records as stale rather than failing
on them, but loop-1 still counts the rows.

**Ruled: documented, not built.** A remediation path means DELETING from an append-only
control table. That is a deliberate human exception to a property chosen precisely so it
could not be waived quietly, and automating it inside a gate would hand the gate the
ability to disarm itself — the failure mode this whole file is written against.

**Recovery is therefore a manual, approved control-table cleanup:**

1. establish which records are stranded — the twin no longer holds a matching row for the
   `(manifest_id, reject_digest)` under the current rulebook version. `supersede_quarantine`
   already names them, one finding per record group;
2. get the deletion approved by whoever owns the control schema, recording WHY the twin
   shrank (a full refresh, a rulebook bump) — the same standard as any other change to a
   control object;
3. delete exactly those records, and nothing else. Do not truncate the table: records for a
   twin that did NOT shrink are still load-bearing, and removing them silently restores a
   double count against the manifest that first rejected the row;
4. re-run `supersede_quarantine` and confirm it reports no orphans, then `reconcile_loop1`
   and confirm it is green.

Until that is done both gates stay red, which is the correct posture: the arithmetic
genuinely cannot be verified, and a gate that quietly re-derived its own inputs to go green
would be worse than one that stops.
