# What is actually in Bronze — an inventory

Measured 24 September 2026 against `01_usnc_bronze_dev` on `hfig-usnc-tds`. Every
figure here is a count from the lake, not an estimate.

**Why this exists.** Three confident claims written into specs this month were
wrong, and all three were corrected by a query taking under a minute: *"there is
no invoice feed in Bronze"*, *"keeping every row invents 44,050"*, and *"there is
no client-structure source anywhere"*. The last surfaced only because someone
mentioned Hubspot in passing. This is the sweep that should have preceded them.

## The headline

| | |
|---|---|
| Schemas | **48**, of which **27 hold tables** and **20 are empty** |
| Real tables | **909** (excluding SDP `__materialization_*` and `event_log_*`) |
| Rows measured | **99,113,570** |
| Sources the vault reads | **4 of 27** |
| Rows out of scope by decision | **16,559,108** (`fieldglass_raw`, client-owned) |
| Rows unread and **not** out of scope | **39,326,938** |

**"48 schemas" overstates it and I have been quoting it all month.** Twenty are
empty: ten source families, each provisioned as `<name>` and `<name>_raw`, where
the `_raw` half has a volume and neither has tables —

`beeline_api`, `beeline_client_owned`, `bullhorn_salesforce`, `cnet`, `freshdesk`,
`prounity`, `snaplogic`, `sun`, `test`, `topaz`.

Seven of those volumes are completely empty. Three (`beeline_api_raw`,
`beeline_client_owned_raw`, `test_raw`) hold a directory skeleton
(`<source>/all/parquet/`) and no data. So they are **provisioned landing zones
awaiting a feed**, not sources anyone should plan against.

## What the vault actually reads

Four sources, and one of them is invisible to a naive binding scan.

| source | bindings | note |
|---|---|---|
| `great_plains_raw` | 7 | the GL |
| `ukg_raw` | 5 | payroll GL |
| `bullhorn_native_raw` | 2 | |
| `sap_fieldglass_raw` | **0 direct** | read through `02_usnc_silver_edm_dev.raw_vault.v_fieldglass_us_invoice` — **all the Ameren invoicing work depends on it, and a scan of `bronze_table` values does not show it** |

**And 24 of the repo's 46 bindings name catalogs that do not exist.**
`hfig_eu.*` (13) and `hfig_usnc.*` (11) are not catalogs in this workspace — the
catalog list has no such entries — and `verify_repo` says so explicitly:
*"(`hfig_eu.`, `hfig_usnc.`, `PLACEHOLDER.`) names a table that exists nowhere"*.
Plus 11 on `PLACEHOLDER.reference`. That is deliberate and gated; it is recorded
here because "46 bindings" reads like a connected vault and **22 is the real
number**.

## Everything measured, by size

| schema | tables | non-empty | rows | newest data | vault reads it |
|---|---|---|---|---|---|
| `great_plains_raw` | 20 | 20 | 43,197,313 | 2026-08-24 | **yes** |
| `bullhorn_native_raw` | 31 | 31 | 19,827,364 | 2026-07-23 | **yes** |
| `fieldglass_raw` | 302 | 287 | 14,831,749 | 2026-08-18 | **no — out of scope by decision** |
| `onestaff_analytics_raw` | 6 | 6 | 10,620,468 | 2026-07-16 | no |
| `beeline_raw` | 120 | 120 | 4,621,192 | 2026-06-02 | no |
| `microsoft_dynamics_raw` | 13 | 13 | 2,830,032 | 2026-03-26 | no |
| `fieldglass_client_owned_raw` | 12 | 9 | 1,727,359 | 2026-02-06 | **no — out of scope by decision** |
| `vndly_raw` | 35 | 35 | 1,219,350 | 2026-03-27 | no |
| `hubspot_raw` | 6 | 6 | 208,532 | 2026-07-20 | no |
| `sap_fieldglass_raw` | 2 | 2 | 29,426 | **2026-09-24** | **yes**, via a view |
| `ukg_raw` | 1 | 1 | 785 | 2026-08-28 | **yes** |

## Five things worth knowing

### 1. THE RULE: we use `sap_fieldglass_raw`. We do not use `fieldglass_raw`

Stated by Adrian, 24 September, and it is a scoping decision rather than a gap:
**`fieldglass_raw` is not ours to read.**

The two are easy to confuse and are not the same data in two shapes:

| | `sap_fieldglass_raw` | `fieldglass_raw` |
|---|---|---|
| tables | **1** (plus SDP internals) | **302** |
| rows | **29,426** | **14,831,749** |
| shape | one wide invoice table, 169 columns | per-client, per-entity — `io_worker_apache_corp`, `io_timesheet_revlon` |
| newest | **2026-09-24** | 2026-08-18 |
| ours? | **yes — every invoice in the Ameren work** | **no** |

So its 14.8M rows are **not** an unexploited opportunity and should not be counted
as one. The figure that matters is the 41.1M rows that are unread *and* have no
decision against them.

**The confusion is the risk, not the exclusion.** A spec that says only
"Fieldglass" is ambiguous between a source we depend on and one we have decided
against, and the names differ by a three-letter prefix. Anything written here
should name the schema, never the system.

**And the client-owned sources are out too**, stated by Adrian on 25 September.
That covers four schemas, not one — `fieldglass_client_owned` and
`fieldglass_client_owned_raw` (12 tables, 1.7M rows, newest 2026-02-06), and
`beeline_client_owned` / `beeline_client_owned_raw`, which are empty.

So the Fieldglass-shaped sources settle cleanly:

| schema | status |
|---|---|
| `sap_fieldglass_raw` | **ours** — every Ameren invoice |
| `fieldglass_raw` | out of scope |
| `fieldglass_client_owned_raw` | out of scope |

Three schemas whose names all begin `fieldglass`, one of which we depend on
entirely. The rule stands and is worth repeating: **name the schema, never the
system.**

### 2. Only one source is current

Today is 24 September. **`sap_fieldglass_raw` is the only source with September
data**; everything else stopped between February and August. That is not
necessarily wrong — a source may be intentionally paused — but nine of eleven
sources being months stale is a fact that should be owned by someone rather than
discovered by the next person who trusts a row count.

### 3. The invoice feed is live; Ameren has had one delivery

| date | rows | Ameren |
|---|---|---|
| 2026-09-08 | 5 | 5 |
| 2026-09-09 | 50 | 30 |
| 2026-09-11 | 16,793 | **1,172** |
| 2026-09-14 … 09-24 | 8,578 | **0** |

The feed delivered **4,068 rows today** and none of them are Ameren. So the
419-invoice, 1,176,896.90 measurement behind the invoicing work is **still
current** — checked rather than assumed, because a new delivery would have
invalidated it silently.

### 4. Several schemas are pipeline output, not raw landing

`fieldglass`, `beeline`, `vndly`, `vndly_raw`, `fieldglass_client_owned`,
`fieldglass_client_owned_raw`, `sap_fieldglass`, `sap_fieldglass_raw`, `ukg` and
`ukg_raw` carry `__materialization_*` tables and an `event_log_*` — they are
Spark Declarative Pipeline outputs.

**Two consequences.** A table count from `information_schema.columns` misses these
(it showed 2 tables for `sap_fieldglass_raw` where `information_schema.tables`
shows 6), so any inventory built the first way undercounts. And materialization
twins **carry no column masks** — the DEF-40 problem this repo gates against in
the vault — so a grant on one of these bronze schemas is broader than it looks.

### 5. The unused data is not small

**39.3M rows unread with no decision against them** — two whole VMS platforms
(`beeline`, `vndly`), a CRM (`microsoft_dynamics`), an ATS (`bullhorn_native`,
partly read) and `onestaff_analytics_raw`. Excluded as ruled out rather than
overlooked: `fieldglass_raw` (14.8M) and the client-owned sources (1.7M).

Hubspot is the proof that this matters: **208,532 rows**, the smallest real source
in the lake, and it turned out to hold the client hierarchy a spec had asserted
did not exist anywhere. Size is not the signal.

## What this inventory cannot tell you, and it is the important part

**Who owns each source.** Nothing in the lake records it, and nothing in this repo
did either until Hubspot's ownership was written down on 24 September after two
questions went to the wrong person.

For each of the ten sources the vault does not read, three questions have no
answer here: **who owns it, is it meant to be current, and what is in it that we
have written off?** The Hubspot case says the third question is worth asking even
of the smallest source.

**Suggested next step, and it is cheap:** circulate this table and ask only for an
owner per row. Everything else follows from having someone to ask.

## Method

- Schemas, tables and columns: `information_schema.schemata`, `.tables`, `.columns`.
  **Use `.tables`** — `.columns` omits SDP internals.
- Row counts: `count(*)` per table, unioned in chunks of 50. Counted, not estimated.
- Freshness: `max(timestamp)` on the largest table per schema;
  `sap_fieldglass_raw` uses `date`, having no `timestamp` column — which is why the
  first freshness query failed rather than silently reporting null.
- Vault bindings: read from `metadata/entities/` through `spec.load_model`, so the
  count is what the model declares rather than what a grep finds.
- Empty volumes: `databricks fs ls` per volume.
