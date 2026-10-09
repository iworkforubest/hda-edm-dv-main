---
name: dv-accelerator-gates
description: >
  Applies ONLY inside the HFIG Data Vault Accelerator repo (identifiable by
  checks/hash_parity_check.py and metadata/entities/ at the project root —
  see scope check below). Do not apply based on generic Data Vault
  terminology (hub, link, satellite, hashdiff) alone, since those terms are
  common across unrelated Data Vault work — this skill is reachable from a
  global skills directory shared with other projects. When you are confirmed
  to be in this specific repo, and Superpowers' brainstorming,
  test-driven-development, or requesting-code-review skills are active: this
  repo has named hard gates in checks/ that are non-negotiable (they fail the
  build, not "flag for review"). Superpowers' generic TDD/review phases don't
  know about them by default, so this skill defines what "tests pass" and
  "review approved" actually mean here.
parent: using-superpowers
---

# DV Accelerator Hard Gates

**Scope check — do this first.** This skill only applies inside the HFIG Data
Vault Accelerator repo. Before applying anything below, confirm you're in that
repo by checking for `checks/hash_parity_check.py` AND `metadata/entities/` at
the project root. If either is missing, this skill does not apply — stop
here and don't apply DV-specific gate language to whatever the actual project
is. This guard exists because this file may be reachable from a global skills
directory shared across unrelated projects, not just this repo.

Superpowers' test-driven-development and requesting-code-review skills are
generic — they assume "tests" means the project's unit test suite and "review"
means a human or Claude reads a diff. In this repo that's necessary but not
sufficient. There are two distinct layers of correctness, and both must be
green before a task counts as done:

1. **Structural correctness** — `tests/test_accelerator.py` (pure-Python
   checks, no workspace needed). Encodes the "What validation refuses to
   build" list: no hub/link carrying descriptive attributes, no NHL without a
   transaction key, no multi-source satellite, no link-of-a-link, no reused
   `ctl_`/`ref_`/`reg_`/`agg_` prefixes, no entity pinned to a superseded hash
   rulebook version.
2. **Behavioral correctness** — the named hard gates in `checks/`, which run
   against a live workspace or pipeline and catch things a unit test
   structurally cannot: mutation history, mask survival through projection,
   cross-region drift, reconciliation totals.

**Neither layer substitutes for the other.** A change that passes
`test_accelerator.py` but hasn't been checked against the relevant hard gate
in `checks/` is not done, and Superpowers should not report the task as
complete on structural tests alone.

## Gate zero — runs before any load, no exceptions

- `hash_parity_check.py` — Spark digests must equal the pure-Python reference
  implementation over the 7 key + 7 hashdiff golden vectors. A divergence
  means keys in this lake cannot join to keys in another lake. This is not
  scoped to "risky" changes — it is gate zero for *every* load.

## Hard gates — required before review/ship is satisfied

| Gate | Fails the build if |
|---|---|
| `append_only_check.py` | `DESCRIBE HISTORY` shows any mutation on a vault table |
| `loop1_reconciliation.py` | `landed + (quarantined - superseded) ≠ approved` — a silent drop is a control-plane violation, not a style issue. `superseded` is a reject a later run legitimately re-accepted; `over_subtracted()` bounds it so a wrong or oversized subtraction is its own finding, never silently absorbed into the arithmetic |
| `mask_survival_check.py` | a declared-sensitive column is unmasked anywhere downstream (satellite → `_v1` view → PIT MV → Gold) |
| `journal_integrity_check.py` | debits ≠ credits, or line count ≠ control total, for any GL journal |
| `aggregate_reconciliation_check.py` | raw payroll detail ≠ GL journal at the declared grain, per account |
| `schema_grant_check.py` | any catalog- or schema-level SELECT exists on a vault catalog/schema — it covers the `__materialization_*` backing tables, which carry no mask, so it grants cleartext beneath every masked column |
| `conformance_check.py` | any TDS region diverges from the `weu_tds` baseline — run before deploying region two, not just once at the end |

## Preflight — blocks deploy, is not a correctness gate

- `preflight_target.py` — authenticated workspace must equal the intended
  target. With 8 near-identical workspaces, landing artifacts in the wrong
  lake is the realistic failure mode. Run before *every* deploy, including
  redeploys of unchanged code.

## Rulebook version discipline (hashing.py specifically)

`SHA-256 / BINARY(32)` and `HASHDIFF_UPPERCASE = False` are RATIFIED and
guarded at import — they cannot change without bumping `RULEBOOK_VERSION` in
the same reviewed commit as the code change, with
`tests/golden_hash_vectors.json` updated alongside. A change to `hashing.py`
that doesn't touch `RULEBOOK_VERSION` should be treated as a red flag, not
approved as a cosmetic fix.

## Red flags (Superpowers-style — call these out, don't rationalize past them)

- *"This satellite is low-risk, we can skip hash parity for it."* — Gate zero
  applies to every load. There is no low-risk exemption.
- *"Mask survival already passed, it'll still hold."* — Re-check whenever a
  view, PIT materialized view, or Gold projection changes downstream of a
  masked column. Passing once is not passing permanently.
- *"Loop-1 reconciliation is informational, close enough."* — It's a hard
  gate. `landed + (quarantined - superseded) = approved` or the task is not done -- and the subtraction is itself bounded by `over_subtracted()`, so trusting it is not optional either.
- *"Conformance only matters at final go-live."* — It runs before deploying
  *each* subsequent region, not once at the end.
- *"I'll just grant SELECT on the schema, it's simpler than listing tables."* — That is
  the DEF-40 bypass exactly. A vault schema holds SDP's `__materialization_*` twin for
  every streaming table, with no mask on it, and a schema-level grant covers those too.
  SELECT is granted per table from the declared model, and `schema_grant_check.py` fails
  the build if anything broader appears.
- *"The hashing.py change is just a formatting fix."* — Any touch to a
  RATIFIED constant requires a `RULEBOOK_VERSION` bump and golden-vector
  update in the same commit, reviewed as a rulebook change, not a formatting
  diff.
- *"Validation didn't complain, so the entity YAML is fine."* — Cross-check
  against the "What validation refuses to build" list by intent, not just by
  whether `test_accelerator.py` happened to catch it — new entity shapes can
  slip past existing checks.

## Mapping onto Superpowers' phases

- **Brainstorm/plan** — before drafting a new `metadata/entities/*.yml`,
  check it against the "What validation refuses to build" list and the
  tenant-scoped-key / high-variability-source patterns already established
  for Fieldglass-style sources. Don't let planning stop at "does it look like
  the other entities" — check it against the refusal list explicitly.
- **Test phase** — both `tests/test_accelerator.py` (structural) and the
  relevant hard gate(s) above (behavioral) must be green. If the change
  doesn't touch a live workspace, at minimum identify which hard gate would
  apply and note it as outstanding rather than silently skipping it.
- **Review phase** — the hard gates table above is the review checklist, not
  a supplement to it. A `hashing.py` diff additionally requires the rulebook
  version/golden-vector check.
- **Ship** — `preflight_target.py` must pass for the specific target
  immediately before `databricks bundle deploy`, every time, including
  redeploys.
