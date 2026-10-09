# Quality dashboard: how to exercise it, and how to take it down

**Nothing here has been deployed.** This runbook describes artefacts that exist in this
branch only. No `databricks bundle deploy` has run, no SQL from `governance/` has been
applied, and no dashboard, Genie space, or `tst_` table exists in any lake.

## Prerequisites for deploying this at all

**Only the `usnc_tds` target can deploy these resources as they stand.** All three quality
resources — both dashboards and the Genie space — use `${var.sql_warehouse_id}`, and only
`usnc_tds` sets it (`databricks.yml:374`). The global default is `""`
(`databricks.yml:154`), so for the other eight targets the variable resolves to an empty
string.

An unset warehouse **fails the deploy**; it does not silently point the dashboard at
another lake's warehouse. So this is loud rather than dangerous — but deploying to any
other target means first setting `sql_warehouse_id` for that target in `databricks.yml`,
with a warehouse id read from that workspace. Do not copy `usnc_tds`'s id: warehouse ids
are workspace-local, and a valid-looking id from the wrong workspace is exactly the kind
of cross-lake mistake this repo's profile discipline exists to prevent.

## What it shows today, and why that looks wrong

Near-total absence of measurement. That is correct, not broken:

* `ref_dq_expectation` holds **0 rows**, so no business rule has ever been evaluated.
* All nine `qtn_` twins hold **0 rows**. The two compiled-in key-safety rules genuinely
  passed on 4.7M loaded rows, which is the only real quality signal the estate has.
* `aud_table_load` holds **0 rows**: the loaders pass `--control-schema` already but have
  not run since that instrumentation landed.
* One of the six control tables does not exist in the lake yet — `ctl_quarantine_superseded`. The other five are present.
* `aud_load_run` already holds **one** row: an `opened` phase with no `completed`, from run 683278350345460 on 26 Aug 18:59. So the incomplete-runs tile renders a real row today, before any synthetic data.

The coverage tile therefore reads `0 of 25 tables`. That denominator comes from
`SELECT explode(generated_tables) FROM governance.meta_vault_model` — **target tables, not
entities**. 21 entities resolve to 25 physical tables (a satellite can have more than one
source binding), so joining on `meta_vault_model.base_table` instead — one row per entity —
would undercount every multi-source satellite. A pass-rate dashboard would have read `100%`
and meant nothing.

## Making it show real data

1. Run `create_control_objects` — three tables are missing.
2. Run `publish_model_metadata` — `governance.meta_vault_model` is the coverage
   denominator and does not exist yet.
3. Run a real load, so the loaders write `aud_table_load` and `aud_table_discard`.
4. Declare expectations in `ref_dq_expectation`. Until then only key safety is measured.

## Exercising the tiles with synthetic data

> ### Read these two things first
>
> **1. `quality_silver_synthetic` is COMMENTED OUT in `resources/quality_dashboard.yml`,
> and must stay that way between exercises.** A declared bundle resource is created by the
> **next deploy**, not when you decide to run the exercise. While it was declared
> unconditionally, the first `databricks bundle deploy` to any target published a dashboard
> titled `Load quality (SYNTHETIC -- delete after testing)` pointing at `tst_` tables that
> did not exist — every tile failing at view time, in a folder every workspace user can
> see, with nothing having gone wrong. Uncomment the block between the
> `BEGIN/END DISABLED RESOURCE` markers to run an exercise, and comment it back out
> afterwards. `tests/test_accelerator.py` fails the build if it is left enabled, and also
> un-comments the template and asserts it still says `embed_credentials: false` — neither
> half of the embedded-credentials gate can see a commented line.
>
> **2. The seed does NOT exercise the two coverage tiles.** `ds_coverage_summary` and
> `ds_coverage` — the headline — read `governance.meta_vault_model`, which has **no `tst_`
> twin and cannot have one**: it lives in the governance schema, outside the control schema
> this DDL may create in, and it is written by `publish_model_metadata`, which has never
> run. Apply the seed and open the synthetic dashboard without populating it first and
> **both headline tiles fail at view time on a missing table.** Step 1 below exists for
> that reason.

Every tile is unproven until it has rendered a non-zero value. To prove them:

1. **Run `publish_model_metadata`** (or otherwise populate `governance.meta_vault_model`)
   for the target. The two coverage tiles read it, it has no synthetic twin, and it does
   not exist in any lake yet. Skip this and the headline tiles error instead of rendering.
2. Apply `governance/control_test_objects.sql` against the target's catalog and control
   schema. It creates `tst_` tables and seeds them, including two seeded rules
   (`nhl_general_journal_line`, `hub_job_request`), one deliberately unruled table
   (`sat_job_request_details_bullhorn_eu` gets synthetic discards but **no rule row**), and
   one table with `staged = 0` (`msat_journal_line_worktag`) so the acceptance-rate tile
   renders its `not evaluated` branch.
3. Uncomment `quality_silver_synthetic` in `resources/quality_dashboard.yml` (between the
   `BEGIN/END DISABLED RESOURCE` markers), deploy, and open the dashboard named
   `Load quality (SYNTHETIC -- delete after testing)`. You will need your own UC grant on
   the control schema to see anything: embedded credentials are gated off (see below).
4. Confirm every tile renders, and note **which claim each tile actually proves**:

   | tile | what the seed alone proves |
   |---|---|
   | Rows staged and accepted | renders, from `tst_aud_table_load` |
   | Discards by reason | renders, from `tst_aud_table_discard` |
   | Acceptance rate, or not evaluated | **both states**: three tables read `evaluated`; `msat_journal_line_worktag` (`staged = 0`) reads `not evaluated` |
   | Runs that never completed | renders: `SYNTH-002` opened and never completed |
   | Rejects later accepted | renders, from `tst_ctl_quarantine_superseded` |
   | Approval manifests | renders, from `tst_ctl_approval_manifest` |
   | Target tables NOT evaluated (headline counter) | **nothing, unless step 1 was done** — reads `governance.meta_vault_model` |
   | Coverage by target table | **nothing, unless step 1 was done** — same table |

   With step 1 done, the coverage tile's two branches become distinguishable:
   `nhl_general_journal_line` and `hub_job_request` read `covered` (they have seeded rules
   in `tst_ref_dq_expectation`), and every other generated table reads `not evaluated`.
   Without step 1, neither branch renders at all — the tiles error.
5. Apply `governance/control_test_objects_drop.sql`.
6. Comment `quality_silver_synthetic` back out and redeploy, or the bundle will recreate a
   dashboard whose tables no longer exist. The build check that pins the disabled default
   will fail until you do.
7. **Do not skip the drop step.** `checks/schema_grant_check.py`'s `synthetic_objects()`
   check fails the build if any `tst_` table is still present in the control schema —
   it is invisible to `append_only_check` and `misplaced_control_objects` by design (see
   the file for why), so this gate is the only thing that notices a leftover. Only pass
   `--allow-test-objects` to that check while an exercise is genuinely in progress; it is
   not a way to silence the gate permanently. Note that without both `--control-schema`
   and `--control-object-schema` the whole block is skipped and prints `NOT EVALUATED`
   naming the synthetic sweep as well: a skipped run has asserted nothing about leftovers.

**Never seed `ref_dq_expectation` itself.** The pipeline reads it to build expectations, so
a fabricated rule there is a production change that would evaluate against real data. That
is why `tst_ref_dq_expectation` exists as a separate table.

## What the dashboard may never do

It queries the control schema only, and `verify_repo.py` fails the build if the committed
artefact names a vault schema.

**Embedded credentials are committed OFF, and gated by two complementary checks**, because
the Lakeview publish API **defaults `embed_credentials` to `true`** when the field is
omitted — an omission is the dangerous case, not just a wrong value:

* **Presence gate.** A depth-agnostic recursive walk over every entry under any key named
  `dashboards`, in `databricks.yml` and every `resources/*.yml`, requires each dashboard to
  declare `embed_credentials` explicitly. A dashboard with no `embed_credentials` line at
  all fails this gate even though no `true` was ever written.
* **Value gate.** A comment-stripped text scan of the same files for any `embed_credentials`
  whose value is not a bare `false`, while no `bundle_root_prefix` in play is on the
  proven-restricted allowlist (`_QR_RESTRICTED_PREFIXES` in `tests/test_accelerator.py`,
  currently **empty** — no restricted folder exists yet). Adding a prefix to that allowlist
  is meant to be a deliberate act, taken only after that folder's ACL has actually been
  checked; the gate is default-deny.

Together the two halves close each other's gap: nothing written badly (a stray `true`)
escapes the value scan, and nothing simply omitted escapes the presence walk.

The bundle deployment folder is world-writable (`users CAN_MANAGE inherited=True`,
measured 27 Aug 2026 — see `docs/superpowers/OPEN_ITEMS.md`, DEF-43), so an
embedded-credentials dashboard there would be editable by every workspace user and would
run its queries as the publisher — whose privileged-group membership resolves column masks
to cleartext. **Until the restricted folder requested from the admin team exists, business
users cannot use this dashboard at all**; engineers use the Genie space instead, which
evaluates access per user and so shows nothing to anyone without a grant.

### Known limits of the gate — read this before trusting it blind

* It cannot see a value injected at deploy time via `--var` — a `databricks bundle deploy
  --var embed_credentials=true` (were such a variable ever wired up) would not appear in
  the committed text and would not be caught.
* It cannot see a resources file that exists but is outside the `include: resources/*.yml`
  glob in `databricks.yml`.
* It cannot see a change made afterwards in the workspace UI — the gate scans committed
  source, not deployed state.
* It does **not** remove the underlying problem: `users CAN_MANAGE inherited=True` on the
  deployment folder is still there. Gating `embed_credentials` only stops one particular
  way of exploiting that folder; it does not close it. Only the restricted folder in the
  outstanding admin request (`docs/platform_team_requests.html`, PLT-3) does that.

## Row-level investigation, and where it is NOT

**The Genie space declares the six control tables and nothing else** — see
`genie_space()` in `tools/emit_quality_dashboard.py` and the committed
`dashboards/quality_silver.geniespace.json`. It carries no `raw_vault` or `business_vault`
object, so **row-level reject investigation is not available through it in this release.**

An engineer who needs to see *which* rows were rejected queries `raw_vault.qtn_*` directly,
with their own grant, in a notebook or the SQL editor — exactly as they do today. Nothing in
this branch adds a surface for that.

Pointing a Genie space at the vault would be safe on the access question: Genie evaluates
data access as the asking user, so a question about data the asker cannot read returns an
empty response, and no new grant is created. The reason not to do it yet is validation. The
DABs bundle schema does not document the shape of `serialized_space`, so the
`{tables, instructions, sample_questions}` structure this emitter writes is **unvalidated
against the live API**. The first task of any vault-scoped Genie space is to validate that
shape against the API; adding table names to a structure nobody has confirmed the service
reads is building on an assumption. `tests/test_accelerator.py` asserts the `tables` list is
exactly `quality.CONTROL_TABLES`, so widening it turns a check red rather than drifting.

## What the `.lvdash.json` format actually requires

Four things about this format were established by deploying and looking, after each one was
accepted by the API and then failed to work. They are written down because every one of them
cost a round trip, and because `bundle validate` catches none of them.

**1. Table names must be fully qualified in the artefact.** The bundle's `dataset_catalog`
and `dataset_schema` do not reach the deployed dashboard. A bare name resolves against the
warehouse's own default — `hive_metastore.default` here — and every tile fails with
`TABLE_OR_VIEW_NOT_FOUND`. This is why the artefact is per target: the catalog differs, so a
qualified artefact cannot be shared across targets.

**2. `queryLines` are concatenated with no separator.** A line ending in a bare token fuses
into the next: `recorded_at` followed by `FROM …` deployed as `recorded_atFROM …`, and every
query was invalid. Every entry must end with a newline.

**3. A table widget is `version: 2` and needs a `data: {"queryName": "main_query"}` block.**
Version 1 is accepted by the API and renders *"Visualization has no fields selected"*, even
with an exhaustive 22-field column spec. Each column needs `fieldName`, `type`, `displayAs`
and `title`.

**4. A counter is `version: 2` with `encodings.value`** and works without a `data` block —
which is why the counter rendered while every table stayed blank, and why the problem looked
like a data problem rather than a schema one for several rounds.

### The lesson, which is the useful part

The API accepting an artefact says nothing about the client drawing it. `bundle validate`
checks the resource fields, not the serialized body. Probing the create endpoint with a
single-line query — which is how this artefact's shape was originally established — cannot
surface a SQL-concatenation bug or an unselected column.

**What worked was comparing against a real artefact known to render.** Two public examples
settled in minutes what four rounds of reasoning had not:
`junTaniguchi/dab_data_platform` and `databricks/tmm`. For any generated artefact consumed by
an external service, diff against a working instance of that artefact before deploying.
Checks that read the generator's own output back only confirm the generator agrees with
itself.

---

# Bronze conformance dashboard

A second dashboard family, `quality_bronze_<target>`, added 29 August 2026. Different
subject from the silver one: it reports whether **Bronze delivers what Silver's contracts
require**, not how the vault load went.

## Where its numbers come from

`checks/source_conformance_check.py` writes one row per contracted table per run into
`<catalog>.control.ctl_source_conformance`, and the dashboard reads that record. It does
**not** recompute conformance. A dashboard that recomputed it would be a second derivation
of a judgement the gate already makes, free to disagree with it — and it could not measure
castability at all, because that needs a `try_cast` probe over real data rather than a join
against `information_schema`.

So the tiles show what the gate concluded, in the gate's own words. The `findings` column
holds the exact `KIND: detail` lines the job log printed.

## Read the honesty tile first

**"Runs recorded"** is the tile that tells you whether anything else on the page means
anything. If it reads **0**, `ctl_source_conformance` is empty and every other tile is a
clean zero over nothing — which looks identical to an estate in perfect health. The tile
exists because that confusion is the failure this dashboard was designed to avoid.

If it reads 0 when you expect otherwise, check in this order:

1. Did `assert_source_conformance` run in the last job? It records only when the job passes
   it `--record-catalog` and `--record-schema`; a hand run without them records nothing on
   purpose.
2. Did it fail? A failed write is **fatal** — the task goes red rather than losing the row
   quietly, so an empty table with a green job means it was never asked to record.

## Which targets have one

Only targets that have committed a source contract: **`dev` and `usnc_tds`** today. A lake
that has not declared its Bronze has nothing to conform to, and a dashboard for it would
read every tile as zero.

Because of that, the resource is declared **per target inside `databricks.yml`**, not in
`resources/quality_dashboard.yml` beside the silver one. A global declaration interpolating
`${bundle.target}` into `file_path` would point at a file that does not exist for the other
seven targets and fail every deploy to them.

**Adding a source contract for a new target means adding that block for that target too.**
`verify_repo.py` fails the build if the two sets ever disagree, in either direction.

Note the path in that block is `./dashboards/...`, not `../dashboards/...`. A relative path
in `resources/*.yml` resolves from that file's directory; one in `databricks.yml` resolves
from the bundle root. Using `../` there escapes the sync root and `bundle validate` refuses
it outright.

## What it shows today, and why that is not a green light

The conformance gate currently **passes**, so the by-table tile is all `CONFORMANT`. That is
true and thin. Read it beside the two counters that qualify it:

* **Contracted tables measured** — how much of Bronze is under contract at all.
* **Runs recorded** — whether the measurement is current.

Source contracts exist for two of nine targets. A "100% conformant" reading over that is a
statement about a small measured corner, not about the estate.

## Regenerating

    python tools/emit_quality_dashboard.py

Emits both families. The artefacts are committed and `verify_repo.py` asserts that
regenerating produces byte-identical files, so the diff is the review — never hand-edit a
`.lvdash.json`.

---

# Gold load-quality dashboard

A third family, `quality_gold_<target>`, added 30 August 2026.

## Nothing deploys it, on purpose

**Gold does not exist.** Measured 29 August:

* `03_usnc_gold_edm_dev` — `databricks schemas list` answers *"Catalog does not exist."*
* No job task produces gold. `--gold-catalog` reaches only `apply_governance` and
  `schema_grant_check`, and only to **grant** on it.
* `governance/control_objects_gold.sql` is generated, committed, gated against drift, and
  **never applied**.

So this family follows that file exactly: the artefacts are emitted for every target and kept
un-driftable, and **no bundle resource declares them**. `verify_repo.py` fails the build if a
declaration appears — deploying one today would either fail on the missing catalog or publish
a dashboard where every tile errors with `TABLE_OR_VIEW_NOT_FOUND`, which is what the silver
dashboard actually did on 27 August.

### When gold does land

1. Create the catalog and apply `governance/control_objects_gold.sql`.
2. Get something writing `aud_*` rows — until then the dashboard is honest but empty.
3. Declare the resource, and **remove the "NO bundle resource declares a gold dashboard"
   check in the same reviewed commit.** It is meant to be deleted deliberately, not
   discovered as an obstacle.
4. Set `sql_warehouse_id` for the target if it is not `usnc_tds`.

## What it measures, and what it deliberately does not

Gold declares no tables of its own — `control_standard.LAYER_TABLES["gold"]` is `{}` — so the
three core audit tables are the whole surface. `STAGED_MEANING["gold"]` gives them their
meaning: **staged = rows read from the vault, accepted = rows published to the projection.**

Tiles: runs recorded, publish rate, load volume, discards by reason, incomplete runs.

Two absences are deliberate, and both are asserted:

* **No coverage tile.** Silver leads with coverage because `ref_dq_expectation` exists there
  to be empty. Gold declares no expectations at all, so a coverage tile would count nothing
  and render 0% — describing a rule set nobody has written as one nobody follows.
* **No supersede tile.** A projection layer has no rejects to supersede.

## The two mistakes this family is exposed to

**Qualifying with the wrong catalog.** Every other dashboard here uses `${var.catalog}`; this
one must use `${var.gold_catalog}`. Get it wrong and the page renders the *vault's* audit
under a title saying Gold — real numbers, no error, wrong layer. A check asserts the emitted
text names the gold catalog and no silver one.

**Subtracting layers.** Gold reads from the vault, so "vault rows minus gold rows, labelled
rows lost" is the obvious next tile and it is wrong: a projection selects, aggregates and
filters, so the drop is the projection working. `staged - accepted` *within* one audit row is
a different thing and stays allowed — that is one layer's own arithmetic.

## Read the honesty tile first

**"Runs recorded (0 means gold has never been loaded)"**. On this family that is not a
precaution against some future emptiness — it describes today. Four empty tables read as a
layer that ran cleanly; `0 runs recorded` reads as a layer that has never run.
