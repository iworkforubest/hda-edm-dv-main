# Quality reporting over the silver control schema

Decided 27 Aug 2026. First of three layers. Bronze and gold get the same treatment once
they have a control schema and something to report; §10 records why neither is in scope
here.

## 1. What this builds

Two artefacts over the silver control schema, both bundle-managed:

* an **AI/BI dashboard** querying control aggregates only. It was designed to publish with
  embedded credentials, for business users; embedded credentials are committed **OFF** and
  gated, so **business users cannot use it in this release** and a viewer needs their own
  grant on the control schema — §5 records why, and what is served instead;
* a **Genie space** over the six silver **control tables only**, for data engineers, which
  evaluates access per user and therefore exposes nothing to anyone lacking a grant. It
  declares no vault table — §5 records why, and what engineers use instead.

Both are generated from one definition, and regeneration is asserted to be a no-op.

## 2. Three things that look like quality signal and are not

This is the section to read. The estate currently reports **zero rejected rows**, and the
zero has three different causes, only one of which is good news.

**The SDP event log is structurally blind to this pipeline's quality mechanism.**
`governance.pipeline_event_log` holds 5,324 events over 82 updates. 226 `flow_progress`
events carry a `data_quality` payload, and every one of them is either `{}` or
`{"dropped_records":0,"warned_records":0}`. There is no `expectations` array in any of
them. That is not a clean pipeline: `factory.py:988` records DEF-18, that
`expect_all_or_drop` cannot be stacked under `append_flow`, so **this pipeline declares no
SDP expectations at all**. Quality is enforced by a hand-rolled filter plus a parallel
quarantine flow. `data_quality.dropped_records` will therefore read `0` for ever, no matter
how many rows are rejected. **No dataset in either artefact may read it.**

**`ref_dq_expectation` holds 0 rows.** No business rule has ever been declared, so no
business rule has ever been evaluated. Every quality statement about this estate other
than key safety is currently vacuous.

**The nine quarantine twins hold 0 rows each** — all nine measured. Against 2,453,132 rows
in `nhl_general_journal_line`, 2,221,108 in `hub_accounting_journal` and 46,889 satellite
versions, that means the two compiled-in key-safety rules genuinely passed on every row
loaded. This one IS real signal, and it is the only real quality signal the estate has.

A dashboard that renders "0 rejects, 100% pass" cannot distinguish the third case from the
first two. It would manufacture confidence out of an absence of measurement. Defending
against that is this design's primary requirement.

## 3. Three states, not two

Every `(dataset, rule)` pair is **passed**, **failed**, or **not evaluated**. The third is
never folded into the first.

Concretely: **no tile may render a rate computed over a zero denominator.** Where the
denominator is zero the tile says `not evaluated`, in those words, and the dashboard's
headline metric is **coverage** — how many entities carry a current expectation — not pass
rate. Today coverage reads `0 of 25 tables`. That is true, it is the most useful thing
the dashboard can say right now, and it stays true as rules arrive.

**Coverage is counted per TARGET TABLE, not per entity.** `ref_dq_expectation.dataset`
is documented as "target table, e.g. nhl_general_journal_line", and the model's 21 entities
resolve to **25 physical tables** because a satellite has one table per source binding. A
coverage tile keyed on entities would report `0 of 21` and, once rules existed, would call an
entity covered when only one of its four bindings carried a rule. This is the same defect
that reached nine published artefacts in the data contract work on 27 Aug before review
caught it; the grain is stated here so the implementation cannot repeat it.

This is not a new convention. `checks/audit_completeness_check.py:165` already prints
`GATE NOT EVALUATED: no aud_table_load rows to assert over` rather than passing. The
dashboard extends an existing discipline rather than inventing one.

## 4. Signal sources

| signal | source | state today |
|---|---|---|
| coverage denominator — the target-table list | `governance.meta_vault_model` | **absent**; `publish_model_metadata` has never run |
| expectations declared, and their severity | `control.ref_dq_expectation` | present, 0 rows |
| staged vs accepted per table per run | `control.aud_table_load` | present, 0 rows |
| discards by reason | `control.aud_table_discard` | present, 0 rows |
| run opened / completed | `control.aud_load_run` | present, **1 row** — an `opened` phase with no `completed`, from run 683278350345460 on 26 Aug 18:59 |
| rejects, authoritative | `raw_vault.qtn_*` row counts | present, 0 rows in all nine |
| operational only — update outcomes, durations, rows written | `governance.pipeline_event_log` | present, 5,324 events |

The event log is used for operational facts and nothing else. §8 asserts that.

## 5. The exposure boundary, and why the two artefacts differ

The two audiences sit on opposite sides of the Phase 6STOP posture. The platform offers a
mechanism for each — but only one of the two is usable in this release: the amendment below
withdraws the dashboard's, and nothing has yet replaced it.

**Genie evaluates data access per end user.** A viewer needs `SELECT` on the underlying
objects, and a question about data they cannot read returns an empty response. So a Genie
space is safe by construction whatever it is pointed at: only the group that already holds
the grant sees anything, and no new grant is created. This is the engineers' surface.

**But the space this release ships declares the six CONTROL tables and nothing else.**
`genie_space()` emits `quality.CONTROL_TABLES`, and a check asserts the `tables` list is
exactly that set — no `raw_vault` or `business_vault` object appears in it. So **row-level
reject investigation is not available through Genie in this release.** An engineer who needs
to see *which* rows were rejected queries `raw_vault.qtn_*` directly, with their own grant,
in a notebook or the SQL editor — exactly as they do today. Nothing here adds a surface for
that.

The reason is not exposure, it is validation: the DABs bundle schema does not document the
shape of `serialized_space`, so the `{tables, instructions, sample_questions}` structure this
emitter writes is **unvalidated against the live API**. Building a wider space on an
unvalidated structure would be building on an assumption. A vault-scoped Genie space is a
reasonable next step, and its first task is to validate that serialized shape against the
API — not to add table names to a structure nobody has confirmed the service reads.

**A dashboard published with embedded credentials does not.** Viewers see data through the
publisher's credentials, with no UC grant of their own. That was the reason the original
design reached for them: it is what would let business users have quality reporting without
a grant on a 6STOP schema — and it is also why the dashboard is restricted to **control
aggregates only**. Counts, rates, coverage, reasons, per table and per run. No `raw_vault`
object is queried, so no vault row content reaches a viewer who holds no grant. 6STOP holds
in substance, not merely in letter.

The amendment below withdraws the embedded-credentials half of that design for this
release. The control-aggregates-only restriction stays, and is gated.

> **AMENDED 27 Aug 2026 — embedded credentials are committed OFF, and gated.** Measured live
> on the bundle deployment directory: `users CAN_MANAGE inherited=True`. Every workspace user
> can therefore edit anything deployed there, and an embedded-credentials dashboard runs its
> queries as the PUBLISHER — who holds the privileged group, so column masks resolve to
> cleartext. A user could repoint a dataset at `raw_vault` and read masked payroll and
> financial data with no grant at all: strictly worse than DEF-40, which at least required a
> schema grant. DEF-43 records that bundle `permissions:` cannot fix it, because the
> CAN_MANAGE is inherited from `/Shared` and is not revocable on a child.
>
> So `embed_credentials` is committed as **false**, and the gate is TWO HALVES, neither of
> which enumerates a location:
>
> * **The value half is DEFAULT-DENY against an empty allowlist.** It is not a check for one
>   bad prefix. A prefix is safe only if it appears in `_QR_RESTRICTED_PREFIXES` in
>   `tests/test_accelerator.py`, and that frozenset is deliberately **empty** — so any
>   `embed_credentials` value that is not a bare `false`, under ANY `bundle_root_prefix`
>   spelling and in any target override, fails the build today. Adding a prefix there is the
>   deliberate, reviewable act of asserting that folder's ACL was actually checked.
> * **The presence half is a depth-agnostic recursive walk.** Every entry under any key named
>   `dashboards`, at any depth in `databricks.yml` or any `resources/*.yml`, must declare
>   `embed_credentials` explicitly. Omission is the DANGEROUS case, not the benign one: the
>   Lakeview publish API defaults the field to **`true`**, so an unwritten line publishes with
>   the publisher's cleartext access. A text scan alone is blind to a line nobody wrote.
>
> Together the exposure is unrepresentable rather than merely documented, and the flag flips
> safely the day the restricted folder in `docs/workspace_admin_request.html` exists.
>
> **Consequence for this release:** business users cannot use the dashboard yet. Engineers are
> served by the Genie space, which evaluates access per user and needs no such flag. The
> business-user path arrives with the restricted folder or with gold, whichever lands first.

The control-aggregates-only consequence is deliberate and should be stated to users: once a
business user can open the dashboard at all, they can see *that* rows were rejected, from
which table, in which run, and under which rule. To see *which*
rows, they ask an engineer, who has the grant. Row-level exposure for business users needs
a gold object they can legitimately be granted on, and §10 records why that is not this
spec.

**Semantic objects stay out of the control schema.** `schema_grant_check`'s
`misplaced_control_objects` fails the build on any table in that schema not declaring
`hfig.control_object`, and a view cannot carry the property. Dashboard datasets are
therefore SQL inside the artefact; Genie is configured with instructions and sample
questions rather than a metric-view layer.

## 6. Generated, not hand-authored

`tools/emit_quality_dashboard.py`, in the shape of `tools/emit_data_contract.py`, emits
both artefacts from one definition, per bundle target, with the committed output asserted to
regenerate identically.

The reason is not symmetry. A `.lvdash.json` is a large artefact that must be parameterised
across nine catalogs, and it would otherwise be the only hand-maintained generated artefact
in a repo that generates the rest. `verify_repo.py` already regenerates
`metadata/key_composition.json` and fails on a diff, with the comment "the diff IS the
review"; the data contracts got the same treatment on 27 Aug. This follows both.

## 7. Synthetic data, and why it is the mutation test

A tile that has never rendered a non-zero value is an unproven tile, exactly as a check that
has never been seen to fail is an unproven check. With the estate reporting zeros
everywhere, **every tile in this dashboard is currently unproven.** Synthetic data is how
they get proven, which makes it part of the work rather than an aside.

It cannot be written into the real audit tables. Four of the six control tables declare
`delta.appendOnly = true` — `aud_table_load`, `aud_table_discard`, `aud_load_run`,
`ctl_quarantine_superseded` — and `append_only_check` sweeps that schema and refuses a
silent skip. A synthetic row there is permanent, and a later `DELETE` to tidy up would put
the table outside append-only for ever, which is what the `aud_load_run` DDL comment exists
to prevent. Nor may synthetic rules go into `ref_dq_expectation`: the pipeline reads it to
build its expectations, so seeding it is a production change that would evaluate against
real data.

So the synthetic objects live **in the control schema, under their own prefix, and are
dropped after use**:

```
tst_aud_table_load          tst_ctl_approval_manifest
tst_aud_table_discard       tst_ctl_quarantine_superseded
tst_aud_load_run            tst_ref_dq_expectation
```

`tst_` is chosen deliberately, and each property is load-bearing:

* **It is outside `CONTROL_PREFIXES`** — `append_only_check` selects on
  `("ctl_", "ref_", "aud_")`, so `tst_` tables are invisible to it and may be mutable and
  dropped. A `tst_` prefix is checked as free: it appears in neither `naming.PREFIX` nor
  anywhere in `src/`, `checks/` or `governance/`.
* **They declare `hfig.control_object`** so `misplaced_control_objects` passes. Nothing ties
  the marked set to `control_objects.sql` — the gate's message advises declaring there but
  asserts no such thing — so they are declared in a separate
  `governance/control_test_objects.sql`, applied on demand and never by the standing
  `create_control_objects` task.
* **They are invisible to the gates that read the audit.**
  `audit_completeness_check` reads `aud_load_run`, `aud_table_load` and `aud_table_discard`
  by exact name; `loop1_reconciliation` reads `ctl_approval_manifest` by exact name. None
  matches a `tst_` table.

The emitter takes a table prefix and emits a second dashboard artefact against the `tst_`
tables. Because both come from one definition, the synthetic dashboard and the real one
**differ only in table identifiers** — §8 asserts exactly that, which is what makes
exercising the synthetic dashboard evidence about the real one rather than about a
lookalike.

Teardown is a companion drop script. §8 asserts that `tst_` objects do not survive into a
target that has not explicitly allowed them, so leaving them behind fails the build rather
than quietly persisting.

## 8. What asserts this

| gate | assertion |
|---|---|
| **new** | regenerating every dashboard and Genie artefact is a no-op |
| **new** | no dataset in any artefact reads `data_quality` from the event log — the permanently-zero source of §2 |
| **new** | no dataset in the business dashboard references a `raw_vault` or `business_vault` object — the embedded-credentials boundary |
| **new** | no tile computes a rate whose denominator can be zero without a `not evaluated` branch |
| **new** | every table an artefact queries is created by `control_objects.sql` (or, for the synthetic artefact, by `control_test_objects.sql`) |
| **new** | the synthetic artefact differs from the business artefact only in table identifiers |
| **new** | `tst_` objects are absent unless explicitly allowed for the target |
| `verify_repo` | the emitter carries the same file-level invariants as the other tools |
| **new 27 Aug (2nd pass)** | every dataset that divides also emits a column carrying the literal `not evaluated`, and every tile reading such a dataset renders that column — RATE_GUARD yields only NULL, which is not an explanation |
| **new 27 Aug (2nd pass)** | the committed business artefact says `not evaluated` in those words, and names every column that carries them |
| **new 27 Aug (2nd pass)** | the seed gives some table a zero `staged` total, so the not-evaluated branch is actually rendered by the exercise rather than merely expressible in SQL |
| **new 27 Aug (2nd pass)** | the Genie space's `tables` list is EXACTLY `quality.CONTROL_TABLES` — no missing control table (a mention in an instruction is not a configured table) and no vault table (§5) |
| **new 27 Aug (2nd pass)** | every table the test DDL **creates** — read off its CREATE statements, not assumed from `CONTROL_TABLES` — is seeded and dropped |
| **new 27 Aug (2nd pass)** | every resource's `file_path` names ITS OWN artefact, by basename: crossing the business and synthetic dashboards leaves every boundary gate green |
| **new 27 Aug (2nd pass)** | `quality_silver_synthetic` is committed DISABLED, and its commented-out template still declares `embed_credentials: false` — a declared resource is created by the next deploy, and neither half of the credentials gate can see a commented line |
| **new 27 Aug (2nd pass)** | `databricks.yml`'s `include` is exactly `["resources/*.yml"]`, which is what the embedded-credentials scan globs — widening one without the other is the failure |
| **amended 27 Aug** | `embed_credentials` is a bare `false` everywhere in the bundle files, unless every `bundle_root_prefix` is on a proven-restricted allowlist that is EMPTY (default-deny), AND every dashboard at any nesting depth declares the field explicitly (the API defaults it to `true`) |
| **new 27 Aug (traceability pass)** | the schema `quality.datasets()` qualifies `meta_vault_model` with is exactly the `governance_schema` that every target configures — `checks/publish_metadata.py` writes it into `${var.governance_schema}`, and two unlinked authorities for one schema name would break the headline tile at view time |
| **new 27 Aug (traceability pass)** | the synthetic prefix is ONE value across the emitter, the test DDL and `schema_grant_check.SYNTHETIC_PREFIX` — the leftover gate matches on the name, so a second spelling disarms it silently |
| **new 27 Aug (traceability pass)** | the synthetic prefix collides with no prefix in `naming.PREFIX`, so no real generated table can be reported as a leftover or reached by the drop script |
| **new 27 Aug (re-review)** | the coverage path carries the literal `not evaluated` and its tile renders that column — asserted BY NAME, so it holds whether or not the SQL divides. The rate-scoped checks above could not see the headline tile, which has no division in it |
| **new 27 Aug (re-review)** | the Genie space's `tables` entries are all well-formed — a bare string entry would slip past the name-set comparison and reach the committed artefact, which nothing scans for vault schemas |
| **new 27 Aug (re-review)** | the disabled synthetic template still marks itself SYNTHETIC in its `display_name`; the business title on it would publish a `tst_`-backed dashboard indistinguishable from the real one, in the same folder |
| **new 27 Aug (re-review)** | no `governance/*.sql` creates a view, materialized view or metric view, and the quality resource file declares exactly `dashboards` and `genie_spaces` (§5: a view cannot carry `hfig.control_object`, so a semantic layer in the control schema would turn `misplaced_control_objects` red at deploy time). This row previously read NOT ASSERTED with the justification that the property was one of the deployed estate; that was wrong — §5's sentence is about what the ARTEFACTS define, which is committed source, and is assertable offline |

Every one of these must be proven able to fail, not merely observed to pass. The fourth is
the one to get right: it is the assertion that stands between this dashboard and the
false-confidence failure §2 describes, and it is the reason the artefact is generated —
a hand-edited tile could reintroduce a bare division at any time without review noticing.

## 9. Prerequisites, and what it cannot show yet

Three things must happen before the dashboard shows anything but absence, and none is part
of this spec:

1. **`create_control_objects` must run.** ONE of the six control tables does not exist in
   `02_usnc_silver_edm_dev`: `ctl_quarantine_superseded`, the supersede ledger added by the
   reintegration work. The other five are present. (An earlier version of this section said
   three were missing; that was a misread of a truncated `SHOW TABLES` listing, corrected
   27 Aug against `information_schema.tables`.)
2. **A real load must run.** The loaders pass `--control-schema` and `{{job.run_id}}`
   already, but have not executed since that instrumentation landed — the four `SUCCESS`
   runs on 26 Aug were single-task runs of `assert_no_broad_grant` with every other task
   `DISABLED`. `publish_model_metadata` has likewise never run, which is why
   `governance.meta_vault_model` is absent and the coverage denominator is currently
   unavailable.
3. **Expectations must be declared** before anything but key safety is measurable.

Until then the dashboard correctly reports a near-total absence of measurement. That is the
intended behaviour, and it is the single most useful thing it can currently say.

## 10. What this does not do

**It does not cover bronze or gold.** Bronze is one catalog per lake with one schema per
source system, owned by the Bronze team, with BRZ-1, BRZ-8 and BRZ-12 outstanding; this
repo holds no model of that boundary to generate from. Gold's catalog does not exist —
`apply_masks.sql:237` records `03_usnc_gold_edm_dev` as not created and
`databricks.yml:179` records that "gold generation is not in this repo's scope; this exists
so governance can grant on it." Neither layer has a control schema or any quality signal.

**It does not define the cross-layer standard.** What a control schema must contain in any
layer, so that a dashboard is generatable from it uniformly, is the natural second
sub-project. It should be written with silver's working artefact as evidence rather than
designed on paper first — that was the explicit scoping decision of 27 Aug.

**It does not define quality expectations.** What "valid" means per entity is a business
and modelling conversation, not a reporting one. This spec makes the absence of rules
visible and measurable; it does not fill it.

**It does not enforce anything.** Every number shown is produced by a gate or a loader that
already exists. A dashboard is a description, exactly as the data contracts are, and nothing
downstream should conclude a rule is in force because a tile is green.
