# The cross-layer control standard

Decided 27 Aug 2026. The second of the two sub-projects scoped that morning: the first built
a quality dashboard over silver's control schema, which is now live. This one defines what a
control schema must contain in **any** layer so that one dashboard can be generated over all
three, and gives the Bronze team a concrete specification rather than an invitation to adopt
a pattern.

## 1. Control, not governance — and why they are not the same problem

The request was for a governance and a control schema per layer. Only one of those is
standardisable, and saying so is part of the design.

**The audit is genuinely uniform.** Every layer takes rows in, writes some out, and drops the
difference for a reason. That is the same shape in bronze, silver and gold, which is what lets
one declaration and one emitter serve all three.

**Governance is not.** `databricks.yml:162-171` records that bronze's `<source>` schemas carry
**physical PII masking** — values rewritten in place. Silver uses **Unity Catalog column
masks** — values preserved, access evaluated per reader. Those are different mechanisms with
different guarantees: a physically-masked value cannot be revealed to a privileged reader, and
a column-masked one can. A shared governance standard would define that difference away.

So each layer keeps its own governance schema, and whether physical and column masking are
interchangeable is a data-protection judgement with no owner yet. Recorded, not settled.

## 2. Where things stand, measured

| layer | catalog | control schema | quality signal |
|---|---|---|---|
| bronze | `01_<lake>_bronze[_dev]` | **none** — 47 schemas, all `<source>` / `<source>_raw` pairs, no control, audit, governance or meta schema | none |
| silver | `02_<lake>_silver_edm[_dev]` | **6 tables, live** | `aud_*` populated 27 Aug |
| gold | `03_<lake>_gold[_dev]` | catalog does not exist | none |

So the standard is greenfield in bronze, already satisfied in silver, and ahead of gold's
existence. That ordering matters: silver is the reference implementation, and it exists.

## 3. The declaration

**A mandatory core of three tables, identical in shape in every layer.**

| table | columns |
|---|---|
| `aud_load_run` | `job_run_id, phase, target, active_sources, recorded_at` |
| `aud_table_load` | `job_run_id, pipeline_update_id, table_name, written_by, staged, accepted, recorded_at` |
| `aud_table_discard` | `job_run_id, table_name, discard_reason, discarded, recorded_at` |

**Layer-specific tables, declared but not mandated everywhere.** Bronze:
`ctl_delivery_manifest`. Silver: `ref_dq_expectation`, `ctl_quarantine_superseded`,
`ctl_approval_manifest`. Gold: none initially — a projection layer has no rejects to
supersede and no expectations of its own until someone declares them.

**Every core table is `delta.appendOnly`. The layer-specific CONFIG tables deliberately are
not.** Verified in silver: `aud_load_run`, `aud_table_load` and `aud_table_discard` are all
append-only, as is `ctl_quarantine_superseded`; `ctl_approval_manifest` and
`ref_dq_expectation` are mutable.

That split is the design, not an inconsistency. The audit records **what happened** and must
never be rewritten — `append_only_check` enforces it, and a layer whose audit can be edited
has an audit nobody can rely on. The config tables hold **what should happen**: an expectation
is corrected, a manifest is superseded. Making those append-only would mean a mistyped rule
could never be withdrawn.

A conforming layer must therefore get the split right, not merely set the property everywhere:
an append-only expectation reference is as wrong as a mutable audit.

### The invariant

```
staged = accepted + sum(discarded)     per (job_run_id, table_name)
```

This is what makes the core worth mandating rather than merely suggesting. Verified live on
27 Aug across all six silver tables with **zero imbalance** — so it is an observed property,
not an aspiration. Any layer claiming conformance must satisfy it.

## 4. Same columns, layer-local meanings — declared, never assumed

The three core tables have one shape and three meanings. Each layer must declare its own, or
the numbers look comparable and are not:

* **bronze** — `staged` is rows read from the delivered file; `accepted` is rows written to
  `<source>_raw`.
* **silver** — `staged` is rows read from the staging log; `accepted` is rows inserted into
  the vault table.
* **gold** — `staged` is rows read from the vault; `accepted` is rows published to the
  projection.

`written_by` carries the script or process, so a reader can tell which of those a row means
without consulting this document.

## 5. The cross-layer view, and the tile that must not be built

One dashboard per bundle target, with a section per layer, because rows flow
bronze → silver → gold and the question people actually ask is where they went.

**The obvious tile is wrong and is forbidden by this spec:** bronze's `accepted` minus
silver's `accepted`, labelled "rows lost". A drop between those layers is expected and
correct. Silver's hubs deduplicate — 4,444,172 GP rows become 2,221,108 hub rows — and
satellites store only changed rows.

The load of 27 Aug makes it concrete. `hub_accounting_journal` staged 2,759,294 and accepted
**zero**, entirely legitimately: 2,221,108 already present and 538,186 duplicates within the
batch, summing exactly to the staged count. A cross-layer subtraction would have reported a
catastrophe. The same figures, with `discard_reason` beside them, report an idempotent re-run.

**So the cross-layer tiles show counts per hop side by side and never a difference.** The
reason column carries the explanation; the arithmetic that is safe to assert is the per-layer
invariant in section 3, which holds within a layer and says nothing across layers.

## 6. A layer with no control schema renders "not instrumented"

Explicitly, in those words. Not an error, and not zero.

This is the normal case for months: bronze has no control schema and gold has no catalog. A
tile that errors is unreadable — measured this morning, when every tile on the silver
dashboard failed and the cause looked like missing data for three rounds. A tile showing zero
is worse, because it is legible and false.

Three states, the same discipline the silver dashboard already applies to coverage:
**instrumented and clean**, **instrumented with findings**, **not instrumented**.

## 7. Enforcement — generate what we own, contract what we do not

One declaration module is the single authority. From it:

* **Gold's DDL is generated.** We own that catalog.
* **Bronze gets a published contract** plus a conformance check that reads the live lake and
  reports which core tables are present, absent, or present with the wrong columns. We do not
  own `01_<lake>_bronze`, so we specify and verify rather than deploy.
* **The dashboard emitter reads the declaration** to know which tiles each layer can support,
  which is what makes "not instrumented" a rendered state rather than a special case.
* **Silver's existing `control_objects.sql` is VERIFIED against the declaration, not
  rewritten.** The declaration becomes authoritative without touching deployed, gated,
  working DDL. Two authorities for one concept is the trap this repo has been bitten by
  repeatedly, and a verification closes it as effectively as a rewrite — at a fraction of the
  risk. Regenerating something already working cost five rounds earlier the same day.

## 8. What asserts this

| gate | assertion |
|---|---|
| **new** | silver's `control_objects.sql` declares exactly the core tables the declaration mandates, with the same columns |
| **new** | every core table in the declaration is `delta.appendOnly` |
| **new** | the generated gold DDL and the published bronze contract both derive from the declaration — regenerating either is a no-op |
| **new** | no dashboard dataset subtracts one layer's count from another's |
| **new** | a layer absent from the live lake renders the literal `not instrumented`, and the tile that shows it is in the tile list |
| **new** | the conformance check reports a wrong-columns case distinctly from an absent-table case |
| `append_only_check` | already enforces the property in silver; extended to any layer whose control schema exists |

The fourth row is the one to get right. It is the only gate standing between this design and
the tile section 5 forbids, and that tile is the one a stakeholder will ask for by name.

## 9. What this changes elsewhere

**The Bronze ask changes.** `docs/loop1_control_table_request.html` currently asks the Bronze
team to write a manifest row into `02_<lake>_silver_edm.control.ctl_approval_manifest` — into
*our* control schema. Under this standard each layer records what it did in its own control
schema, and the downstream layer reads upstream's. So the ask becomes: stand up
`01_<lake>_bronze.control` to this specification and record deliveries there, and silver's
loop-1 reconciliation reads it from bronze.

That is a larger ask and a better one: Bronze stops reaching into a schema it does not own,
and the manifest sits with the team that knows what a delivery is. The request page must be
revised before it is sent, and the `_manifest_id` column ask on the four feeds is unchanged.

## 10. What this does not do

**It does not instrument bronze or gold.** It specifies what they must contain and verifies
whether they do. Standing them up is the owning team's work — which is the point: they get a
specification instead of an invitation.

**It does not make loop-1 cross-layer** — but the claim this section originally made about
that was wrong, and the correction matters more than the claim. It read: "Reading the manifest
from bronze's control schema changes where the `approved` count comes from, not what the
identity means." It changes both, wherever the loader deduplicates.

`approved_count` is settled in `governance/control_objects.sql` as "rows this batch approved
for loading", sound because "every reconcilable target of that batch receives its rows one for
one". Bronze's `delivered_count` is rows written to `<source>_raw`. Those are the same number
only where nothing is dropped between them, and `factory.py`'s `_stage` records that they are
not: `_raw` carries **~1.8 copies of every gl20000 business row across 7 deliveries**, and
`ukg_raw.gl` exactly 2 across 2. Substituting one for the other would put the gate ~80% out on
a correct load.

Two different controls were hiding behind one number:

1. **Did silver load everything it was given?** That is loop-1. It needs a count from OUTSIDE
   silver — derive it from silver's own post-filter tables and `landed + quarantined = approved`
   holds by construction, which is DEF-48's vacuous gate in a new form.
2. **Did silver stage everything bronze delivered, allowing for declared dedup?** That is
   cross-layer, needs bronze's delivery manifest AND the dedup factor, and §5 forbids doing it
   by naive subtraction. It does not exist yet.

Measured 28 Aug across the six reconcilable entities, the split is clean: every binding that
carries a `manifest_column` declares no `dedup_by`, and every binding that dedups carries no
manifest. That coincidence is the only reason a delivered count can stand in for an approved
one, so `spec.validate` now **refuses** the combination rather than relying on it holding.

**It does not standardise governance**, for the reasons in section 1.

**It does not decide what "valid" means in any layer.** `ref_dq_expectation` holds four rules
in silver as of 27 Aug, covering 3 of 25 target tables. Filling that in is a business
conversation, and a rule declared today validates rows that arrive, never rows already
landed — the vault is append-only and cannot be reprocessed.
