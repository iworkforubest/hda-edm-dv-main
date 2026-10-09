# Operationalizing Genie Ontology — what applies to us, and what does not

**Read 5 September 2026.** Databricks blog, 1 September 2026, by Srujan Alase and Richard
Tomlinson. Assessed against this accelerator the same way
[`industry_data_models_evaluation.md`](industry_data_models_evaluation.md) assessed the
Industry Data Models papers, and it reaches a similar shape of conclusion for a similar
reason. See also
[`agentic_ai_readiness_evaluation.md`](agentic_ai_readiness_evaluation.md), read the same day,
which reaches the same conclusion from a vendor-neutral direction, and
[`okf_graphify_evaluation.md`](okf_graphify_evaluation.md), which turns the same question on
the project's own knowledge artefacts.

## What the paper says

Genie Ontology combines a **modelled core** — Unity Catalog Semantics: Metric Views, Pages,
Domains — with **inferred context** learned from governed tables, queries, dashboards,
notebooks and Genie Agents. It ranks that context by authority and relevance, applies the
asking user's permissions, and hands the winner to Genie at answer time. External agents
reach the same intelligence over MCP.

The organising principle: **model the "head" and let the ontology infer the "tail."**
Deliberate curation goes to the definitions that must be right; everything else is learned.
Six layers, explicitly progressive rather than prerequisite:

| Layer | Ask |
|---|---|
| **0 · foundation** | durable gold layer on business processes; facts with a clear grain, conformed dimensions; **one identity per real-world entity** |
| **1 · metadata** | table descriptions and column comments in UC; governed tags for sensitivity/ownership; `dbxmetagen` for the first pass, human-approved |
| **2 · semantics** | **declare PK/FK in UC**; Metric Views for governed measures; synonyms and example queries on metrics; Domains and Pages |
| **3 · context assets** | rich, well-used dashboards/notebooks/queries; **certify** what you trust, **deprecate** what you do not |
| **4 · governance** | UC access controls, RLS, column masks, ABAC from governed tags; Unity AI Gateway over the model layer |
| **5 · evaluation** | ground-truth question sets per domain; Genie Agent Benchmarks; monitor citations; **fix at the layer that owns the meaning**; watch for drift |

Closing advice: one domain, one metric, harden the head, expand as you learn.

## The mismatch that decides how we use it

**This paper is about the consumption surface, and our silver is not one.** Every layer
assumes an agent will read the tables it describes. Our raw vault is an insert-only
historised audit model: `sat_*_v1` views, hashdiffs, ghost records at `1900-01-01`, quarantine
twins, `BINARY(32)` hash keys. Pointing a natural-language agent at `raw_vault` would produce
confident answers over the wrong grain — a satellite has one row per *version*, not per
business object, and nothing in a column name says so.

That is the same conclusion the Industry Data Models evaluation reached on 3 September, and
it is the same cause: **we decided on 26 August that the raw vault is not consumer-readable
and the business vault is reached only through Gold.** So Genie Ontology is not a silver-layer
technology for us. It is a Gold technology, and Gold does not exist yet.

Two consequences, and they point in opposite directions:

1. **Nothing here is actionable as a Genie deployment today.** There is no consumption
   surface to point it at. `03_usnc_gold_edm_dev` exists but we hold no `USE CATALOG` on it
   (OPEN_ITEMS, "Gold access").
2. **Several layers are actionable as *modelling* work right now**, independent of Genie,
   because the artefacts they ask for are ones we already generate and do not publish.

The second is the useful half of this paper for us.

## Layer by layer, measured against what we have

### Layer 0 — foundation: we are ahead, in the one place that usually hurts

The paper's second point is entity resolution: *"If the same customer has three different IDs
across systems, it can also be double-counted... one customer is one customer."*

**That is what a Data Vault hub is.** Our `key_style` work — `federated`, `authored`,
`tenant_scoped` — plus `key_derivation.json` as a gated golden record and
`key_derivation_guard.py` refusing loads into a re-keyed vault, is a stronger form of this
control than the paper describes. Fieldglass is the worked example: 30 tenants, one
`hub_job_request`, 7,230 distinct `(tenant, reference)` pairs, and 57,886 rows quarantined
rather than admitted with a null key.

The paper's *first* point — the durable gold layer, facts with a clear grain and conformed
dimensions — is the gap, and it is the same gap the Industry Data Models paper identified.
**Two independent Databricks papers now say the same thing about our estate: the layer we
have not built is the one both of them assume.** That convergence is worth more than either
paper's content.

### Layer 1 — metadata: the largest concrete gap, and it is ours to close

Measured against `data_contracts/dev.yaml` — 25 entities, 370 columns:

| | count |
|---|---|
| columns carrying a `description` or `comment` | **0** |
| entities carrying a non-empty `description` | **0** |

We do emit a table comment, in `factory.py:_table_comment`:

```
hub :: finance :: generated -- do not hand-edit
```

That is a comment written for us, not for a reader. It says the entity kind, the domain, and
a warning — and nothing about what the table represents. It is precisely the paper's
`fct_rev_daily` / `rev_amt` example: an agent, or a new analyst, has to guess.

**We hold the raw material and do not use it.** `metadata/entities/*.yml` carries `notes`
(appended to the table comment today), `domain`, `grain`, `sensitivity` per entity and
`classification` plus `maskFunction` per column. What is missing is prose per column, and
there is nowhere in the model to put it.

This is worth doing whether or not Genie ever runs here: a column comment improves the
catalog, the data contracts, the DBML diagram's notes and the ontology's `rdfs:comment` at
the same time, from one source. It is the highest-return item in the paper for us, and the
paper is right that it is also the cheapest.

### Layer 2 — semantics: **31 foreign keys and 56 primary keys already derived, and never declared**

This is the finding that justifies reading the paper.

`data_contracts/dev.yaml` already carries, per column:

```yaml
      job_request_hk:
        classification: internal
        primaryKey: true
        references: 02_usnc_silver_edm_dev.raw_vault.hub_job_request.job_request_hk
        type: BINARY(32)
```

Measured: **56 `primaryKey: true` and 31 fully-qualified `references`** across 370 columns —
the same 31 edges `emit_dbml_diagram.py` draws as `Ref:` lines and `emit_ontology.py` reifies
as `owl:ObjectProperty`. All three derive from `entity.parents` through
`accelerator.contract`.

**Nothing pushes any of it into Unity Catalog.** There is no `ADD CONSTRAINT` anywhere in
`governance/` or `src/accelerator/` — grep returns the string only inside comments about
unrelated `ALTER TABLE` limitations. So the relationship model exists in four places (the
YAML, the contract, the diagram, the ontology) and in none of the places a query planner, a
BI tool, or an agent would look.

The paper's framing is the right one: *"if they have to guess how tables connect, they will
sometimes guess wrong... declaring them is one of the most direct ways to reduce join
errors."* UC's PK/FK constraints are **informational, not enforced**, which suits us exactly —
we do not want the engine enforcing referential integrity on an insert-only vault where a
satellite legitimately arrives before its hub within a run.

There is also a gate-shaped opportunity here. The paper says *"these constraints are
informational rather than enforced, so your governance process has to keep them accurate."*
That is the standing weakness of every informational constraint, and it is the exact shape of
problem this repo already solves: emit from the model, commit, and let `verify_repo` assert
byte-identical regeneration. A declared constraint that drifts from `entity.parents` would be
a build failure, not a discovery.

**Metric Views: none, and correctly none for now.** They belong to Gold. Worth noting that
Metric Views are the paper's answer to "the number that cannot be wrong" — and our GL
equivalent, `assert_journal_integrity`, is a *gate* rather than a definition. Those are
complementary, not substitutes.

**Pages and Domains: none.** These are glossary and organisation objects for a consumption
estate. Premature.

### Layer 3 — context assets: mostly not applicable, one caution

We have one dashboard (silver quality), one un-deployed gold dashboard definition, no
notebooks of consequence, and no saved queries. There is very little tail to infer from, and
that is fine at this stage.

The caution is **certification and deprecation**. The paper treats them as authority signals
the ontology ranks on. We have a related but different mechanism — generated artefacts gated
byte-identical — and it is stronger for the things it covers. What it does not cover is
anything hand-authored, which is exactly where this repo has been finding defects: three
request documents had gone stale in `docs/` with no gate on them until 5 September. If we ever
put assets in front of an ontology, the certification story has to be a gate, not a flag.

### Layer 4 — governance: we are ahead, with one question we cannot answer ourselves

Column masks, ABAC-from-tags, RLS, group-based access: this is DEF-40 territory and we have
gone further than the paper. `schema_grant_check.py` fails the build on any catalog- or
schema-level `SELECT`, for a reason the paper does not mention.

**And that reason is a question to put to Databricks before Genie touches any masked table.**
Every SDP streaming table has a `__materialization_*` companion that **carries no mask**. The
paper's guarantee is *"Genie only uses content that the person asking is authorized to see"* —
which is a statement about the asking user's grants, not about which physical object the
context extraction read. Two things we do not know and should not assume:

- whether inferred-context extraction reads only the logical table, or can index the
  materialisation twin;
- whether extracted context, once ranked and cached, is re-filtered per user at answer time
  or filtered only at retrieval.

Both are answerable by Databricks and neither is answerable by us. Until they are, "Genie
respects Unity Catalog permissions" is a claim about a mechanism we have already found one
documented bypass in.

### Layer 5 — evaluation: the paper is weaker here than our own discipline, in one specific way

The layer is right in structure: ground truth per domain, expected answer and authoritative
source per question, acceptance thresholds, root-cause the failure to the layer that owns the
meaning. The routing table — Page for a definition, Metric View for a measure, UC metadata for
a source, Agent instructions for a domain quirk — is genuinely good and we should copy its
shape if we ever build Gold.

What it does not say, and what this repo has learned to insist on, is that **a check that has
never failed is not evidence.** The paper's benchmarks measure answers; nothing in it asks
whether the benchmark could distinguish a right answer from a wrong one. That is the same
caveat the Industry Data Models evaluation raised about "200+ enforceable rules," and it lands
the same way: treat a green benchmark suite as meaningful only if a deliberately wrong metric
definition makes it red.

## What to skip

- **Genie Code for Metric View authoring.** Natural-language generation of governed
  definitions is a weaker control than a reviewed diff, for the same reason we rejected the
  Modeling Agent on 3 September. Our model is declarative YAML behind 976 verification checks.
- **`dbxmetagen` for our column comments.** The accelerator's tables are 25 entities, not
  thousands; the descriptions should come from the same YAML the schema comes from, so that
  one edit reaches the catalog, the contract, the diagram and the ontology together. An
  LLM-generated comment applied directly to UC would be a fifth copy that drifts.
- **Pages, Domains, certification, benchmarks.** All Gold-layer objects. Revisit when Gold
  exists.

## Recommendation, in value order

1. **Add a `description` per column and per entity to `metadata/entities/*.yml`**, and thread
   it through `accelerator.contract` to: the UC table/column comment, `data_contracts/*.yaml`,
   the DBML `note`, and the ontology's `rdfs:comment`. One source, four consumers, all already
   gated. This is Layer 1, it is the paper's highest-return item, and its value does not depend
   on Genie at all.
2. **Emit and apply UC PK/FK constraints from `entity.parents`** — 56 primary keys and 31
   foreign keys that already exist as derived facts and are declared nowhere the engine can
   see. Informational only, gated against the model the way every other generated artefact is.
   This is Layer 2's first step and the single most direct thing in the paper.
3. **Ask Databricks the two `__materialization_*` questions** before any Genie evaluation
   touches a masked table. Cheap to ask, and the answer decides whether Genie is usable over
   the vault at all.
4. **Keep the six-layer structure as the Gold blueprint**, alongside Banking ECM from the
   Industry Data Models assessment. Two papers, one conclusion: build Gold.

## One caveat on the whole exercise

The paper is a practice guide, not a technical specification, and it is written by the vendor
whose product it is. Its central claim — that inferred context, ranked by authority and
filtered by permission, is trustworthy enough to answer business questions on — is asserted
rather than evidenced. Every ranking system has a worst case, and the paper does not describe
Genie's. That is not a reason to dismiss the layers, which are sound modelling advice
independent of the product. It is a reason to treat the evaluation layer as the load-bearing
one rather than the last one, which is the opposite of the order the paper presents them in.
