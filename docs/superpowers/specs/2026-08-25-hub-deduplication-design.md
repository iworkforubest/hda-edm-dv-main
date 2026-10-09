# Hub deduplication: a staging log and the classical hub loader

**Date:** 25 August 2026
**Status:** approach approved, implementing
**Closes:** the "hub deduplication" open item in `OPEN_ITEMS.md`

## 1. What is wrong, measured

`hub_accounting_journal` holds 2,759,294 rows over 2,221,108 distinct keys — 538,186
duplicates. `append_only_check` fails on it, and has been failing since the gate was
repaired.

The staging dedup is **not** the problem. There are **zero** duplicates within any single
source:

| rec_src | rows | distinct keys | duplicates within source |
|---|---:|---:|---:|
| `GP_US` | 794,888 | 794,888 | **0** |
| `GP_US_HIST` | 1,964,404 | 1,964,404 | **0** |

Every duplicate is a key supplied by **two** sources, and all 538,186 are fiscal year
2025: GP has archived 2025 into `gl30000` without purging it from `gl20000`, so
`GP_US`'s entire 2025 key set is a strict subset of `GP_US_HIST`'s. `hub_accounting_journal.yml`
states the intent plainly — the year-end close does **not** re-key a journal, because
`openyear` and `hstyear` carry the same value and only the column name differs. Both
bindings therefore produce the same hash key, by design.

This is not a GP quirk. `hub_organisation` has 10 keys supplied by two sources and one by
three, which is exactly what a conformed hub is for.

**So the sources are right and the keys are right.** The duplicates are the correct
consequence of correct inputs, and the defect is structural: a hub needs
insert-if-not-exists across flows, and **a streaming table cannot read its own contents**.

It is not cosmetic. A satellite or NHL joining a hub on its hash key fans out against a
duplicated row, so this is a live correctness bug for every consumer.

## 2. The fix

The classical Data Vault hub loader, which is an INSERT with a lookup — append-only in
the strict sense, since it never updates or deletes. The lookup is what a streaming flow
cannot do, so the load moves to a batch task where it can.

```
  bronze ──(SDP append flows, one per binding)──► stg_hub_x     append log, duplicates fine
                                                     │
                                       (batch task, anti-join)
                                                     ▼
                                                  hub_x         one row per key
```

```sql
INSERT INTO hub_x
SELECT <declared columns>
FROM (SELECT <declared columns>,
             row_number() OVER (PARTITION BY <hk> ORDER BY load_dts, rec_src) AS rn
      FROM stg_hub_x) s
WHERE s.rn = 1
  AND NOT EXISTS (SELECT 1 FROM hub_x h WHERE h.<hk> = s.<hk>)
```

`ORDER BY load_dts, rec_src` makes the surviving row deterministic, so first-seen
provenance is stable and a re-run cannot pick a different winner. `NOT EXISTS` makes the
task idempotent: a second run inserts nothing.

`hub_x` becomes a plain managed Delta table with `delta.appendOnly = true`. Its
`DESCRIBE HISTORY` shows only `WRITE`, so `append_only_check` keeps its exact present
meaning rather than being relaxed for hubs.

## 3. Scope, and why it stops at hubs

| kind | sources | can duplicate | treatment |
|---|---|---|---|
| hub | 5 of 6 are multi-source | yes, today | staging log + loader |
| link | all single-source | no | direct, unchanged |
| nhl | all single-source | no | direct, unchanged |

All six hubs get the treatment, including single-source `pay_period`: a uniform rule has
no special case to forget, and the cost for a single-source hub is one table and an
anti-join that inserts everything once and nothing thereafter.

Links and NHLs stay direct. They cannot duplicate while single-source, and
`append_only_check` already asserts uniqueness at grain `[own_hk]` for every keyed kind,
so a regression fails the build rather than corrupting silently.

**That backstop is a gate, not an assumption, and the assumption gets its own guard.**
`spec.validate` will refuse a multi-source link or NHL, naming this decision — so the day
someone adds a second binding to an NHL, the build stops and this spec is revisited,
rather than the duplicate appearing in production.

## 4. What changes

| file | change |
|---|---|
| `src/accelerator/naming.py` | `stg(table)`; `STAGED_KINDS = {"hub"}` |
| `src/accelerator/factory.py` | hubs emit `stg_hub_x` instead of `hub_x`; quarantine name derived from the ENTITY so `qtn_accounting_journal` is unchanged |
| `src/accelerator/spec.py` | refuse a multi-source link/NHL, naming this spec |
| `checks/load_hubs.py` | **new** — creates each `hub_x` and runs the anti-join insert, both rendered from metadata |
| `checks/append_only_check.py` | `stg_` joins the vault prefixes for the MUTATION assertion; the UNIQUENESS assertion skips it, because a log legitimately holds duplicates |
| `checks/loop1_reconciliation.py` | a hub's landed count comes from `stg_hub_x` — everything approved lands there, and the hub is a projection of it |
| `resources/vault_job.yml` | `load_hubs` between `raw_vault` and `business_vault` |
| `tests/test_accelerator.py` | as below |

Links and NHLs compute parent hash keys rather than looking them up, so nothing depends
on hubs being loaded first. `load_hubs` is ordered before `business_vault` for clarity,
not necessity.

## 5. Testing

Offline:

- a hub emits `stg_hub_x` and **not** `hub_x`; a link/NHL emits its table directly
- the quarantine twin keeps its present name (`qtn_accounting_journal`, not
  `qtn_hub_accounting_journal`) — asserted against the live inventory, since a renamed
  twin would orphan the existing table
- the loader's rendered SQL names only declared columns, in declared order, and its
  column list equals `factory._projection`'s — compared to each other, never to a
  hand-written list
- the loader emits `NOT EXISTS` and `row_number() … = 1`; a rendering that dropped either
  would be a silent duplicate generator
- `spec.validate` refuses a multi-source NHL, and accepts a multi-source hub

Live, after the reload:

- `hub_x` row count equals its distinct key count, for all six
- `hub_accounting_journal` = 2,221,108 rows — the distinct count measured before
- `DESCRIBE HISTORY hub_accounting_journal` shows only `WRITE`
- running `load_hubs` a second time inserts **zero** rows
- `append_only_check` passes, having previously failed on exactly this

## 6. What this does not address

`hub_organisation` had 24 rows over 12 keys, and the ghost row is one of them. The ghost
is a legitimate key like any other and flows through the loader unchanged.

This does not touch the two gates still blocked on workspace prerequisites
(`reconcile_loop1` needs `ctl_approval_manifest`; `assert_journal_integrity` needs a
privileged run-as identity). Neither is affected by this change.
