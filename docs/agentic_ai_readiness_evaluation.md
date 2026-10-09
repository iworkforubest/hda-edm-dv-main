# Making Your Data Ready for Agentic AI — assessed against this accelerator

**Read 5 September 2026.** Pramod Sadalage and Prem Chandrasekaran, Thoughtworks /
martinfowler.com, 27 August 2026. ~10,000 words. Assessed the same way as
[`industry_data_models_evaluation.md`](industry_data_models_evaluation.md) and
[`genie_ontology_evaluation.md`](genie_ontology_evaluation.md), and it is the strongest of the
three for our purposes because it is vendor-neutral and because most of what it asks for is
work this repo has already done.

## The argument

For thirty years data systems were built for human analysts, who supply context, judgment and
scepticism for free. Agents supply none of it. **"A human hesitates at data that looks wrong;
an agent acts on it anyway."** So five attributes have to move out of the analyst's head and
into the data: **Trusted, Contextual, Traceable, Governed, Operational**.

Four topics build them, and the dependency order is load-bearing: **contracts and quality →
traceability and governance → the context layer → agent-ready access**, with observability
running alongside all of it from day one rather than stacked on top. The closing self-assessment
is explicit that you do not average the rows — *"your readiness is capped by your weakest
foundational layer."*

## Where we are genuinely ahead

Three of the paper's patterns are things this repo built before reading it, and in two cases
built harder.

### The quarantine pattern

The paper describes it as a pattern to stand up: a contract-validation gate before the
agent-accessible tier, failures routed to a dead-letter queue for human review, *"the agent
never sees the bad data."*

We have it, and it is a hard gate rather than a convention. Every entity has a `qtn_` twin,
`ctl_quarantine_superseded` records what was later fixed, and `loop1_reconciliation.py` asserts
`landed + (quarantined - superseded) = approved` on every load — a silent drop is a control
violation, not a warning. The Fieldglass go-live is the worked example: 328,172 view rows =
270,286 staged + **57,886 quarantined** for a null `buyer` or `job_posting_id`, reconciled
exactly.

The paper's version has no equivalent of the reconciliation identity. Quarantining is easy;
proving nothing vanished between the two piles is the part that needs a gate.

### The two clocks, and the freshness rule we met the hard way this week

The paper's sharpest operational detail:

> Key the SLA to when the data was **last successfully loaded**, not when a value last changed,
> so that steady data isn't flagged as stale and a stalled pipeline can't masquerade as fresh.

A Data Vault separates those two clocks by construction. `naming.py` has carried both since day
one: `load_dts` — *"when WE learned it. Set by the load, monotonic"* — and `applied_dts` —
*"when it happened in the source. May arrive late."*

**And we hit the exact failure the rule guards against, four days ago.** Choosing the Fieldglass
satellite clock, we measured every candidate source timestamp: `job_posting_create_date` and
`uploaded` are constant per key (1.0 distinct values), `job_posting_distribution_date` is 42.4%
null, and `timestamp` — which is a *delivery* clock, not a business one, a distinction this
repo keeps deliberately — is frozen across **562 of the 1,127 postings that actually change**.
Three of the four are "when the value last changed" clocks and all three lie; the fourth is the
delivery clock the paper warns against substituting. Declaring any of them would have made half
the changed rows look unchanged; declaring none makes `applied_dts` NULL and every `_v1` version
read as current. That became **BRZ-15**, asking Bronze for the
`revision` column it already emits on 62 `io_worker` tables.

So the paper's general rule and our specific measurement agree, and the vault's architecture
already gives us the place to put the answer. What we do not have is the SLA itself — see below.

### Ownership, and the sentence that describes DEF-40 exactly

> An access scope with no owner quietly widens until it's a standing service account again.

That is not an analogy for what happened here; it is what happened. Our own SR-322422 write
access was implemented by adding the silver catalogs to the `full_scopes` roll-up groups, which
propagate a standard privilege set including `SELECT` onto every schema in their catalogs. Two
groups ended up reading beneath every column mask on 86 tables, nobody chose it, and it took
until 2 September to close. The paper's remedy — a named owner per contract, metric and access
scope — is the control we did not have.

## Where the gaps are

### Our "data contract" is a schema document, not a contract

`data_contracts/*.yaml` declares `dataContractSpecification: 1.1.0`. The paper recommends the
**Open Data Contract Standard v3.1.0** — the format the Data Contract CLI uses and the one on
Thoughtworks Radar 33 — which is a different specification. That difference is cosmetic. This
one is not:

| the paper's contract enforces | ours |
|---|---|
| strict logical types | **yes** — 370 columns typed |
| primary keys and relationships | **yes** — 56 PKs, 31 FK references |
| quality rules that can fail (`price > 0`, currency in an allow-list) | **compiled-in key-safety rules only** |
| **freshness SLA** (`latency: 24h`, keyed to `ingested_at`) | **none, anywhere** |
| CI/CD gate blocking deployment on contract failure | yes, for schema; nothing for freshness |

The contract's own `quality:` block says it plainly: `ref_dq_expectation` *"holds governed
data-quality rules but lives in a lake this offline emitter cannot read, so no governed
expectation is included here — only the rulebook's compiled-in key-safety rules."* Those rules
are real and they fire — `hashdiff IS NOT NULL`, no `||` delimiter inside a key component, no
`^^` null token — but they are all key-safety. **Not one business rule exists**, and of the
three declarable severity tiers, two *"do nothing, today."*

This is already visible in our own instrumentation: the silver quality dashboard leads on a
coverage tile precisely because `ref_dq_expectation` is empty, and it reads 0%. We built the
honest tile and left the rule set unwritten.

**The freshness gap is the more serious half**, because it is the paper's floor and we have no
part of it. Nothing in this repo declares how stale a Bronze feed may be before a load should
refuse to run. `aud_load_run` records when a load happened; nothing asserts one should have.

### There is no semantic model, and no consumption surface to put one on

The paper splits the context layer into three: **domain model** (nouns — what exists),
**semantic model** (numbers — one versioned formula per metric), **capability model** (verbs —
what the agent may do).

Scored against that split, we are lopsided:

- **Domain model — strong, and in the paper's preferred form.** 25 entities in version-controlled
  YAML, 31 relationship edges, and both a DBML diagram and an OWL/RDF ontology generated from
  the same source and gated byte-identical. The paper says the market name for the domain model
  *is* ontology and names RDF/OWL/SHACL as the formal machinery. We built that on 3 September for
  unrelated reasons.
- **Semantic model — absent.** No Metric Views, no metric definitions, no fiscal-calendar
  mapping. The paper's headline evidence is AtScale's text-to-SQL benchmark: **under 20% accuracy
  on a raw schema, over 92.5% with a semantic layer, same model.** That is the single most
  quotable number in either paper.
- **Capability model — absent, and correctly so.** Nothing should write to this estate through
  an agent today.

And the reason the semantic model is absent is the same reason as in the other two papers:

> **Bronze and Silver are for humans; agents see only Gold and above.**

**That is now three independent papers — two from Databricks, one from Thoughtworks — telling us
the layer we have not built is the one they all assume.** The convergence is worth more than any
one of them. Our raw vault is an insert-only historised audit model: a satellite has one row per
*version*, and nothing in a column name says so. It is exactly the "Silver" the paper says agents
must not reach.

### Traceability: we have the "what", and no "why" to record yet

`aud_load_run`, `aud_table_load`, `aud_table_discard`, plus `manifest_id`, `batch_id` and
`rec_src` per row, give us load-level lineage from day one — which is the paper's advice
(*"observability is not staged at all... retrofitting it onto a running system is painful"*)
already followed.

The agentic-lineage half — traces and spans capturing *why* an agent chose what it chose, EU AI
Act Article 12/19, six-month retention — has no subject here. No agent acts on this data. Worth
reading now and building never, until one does.

## Where we stand, scored against the paper's own table

Honestly, and without averaging:

| Attribute | Our position | Why |
|---|---|---|
| **Trusted** | **In Transition**, split | Quarantine and reconciliation are agent-ready and gated. Quality rules are key-safety only; **freshness SLAs do not exist**. That last one is the floor. |
| **Contextual** | **In Transition**, lopsided | Domain model in Git, generated and gated — the agent-ready column's description. Semantic model and capability model absent. |
| **Traceable** | **In Transition** for data; **N/A** for agents | Load lineage from day one. No agent decisions to explain. |
| **Governed** | **In Transition** | UC access control, column masks, per-table grants, `schema_grant_check` failing the build on anything broader — but the run-as identity is a **person** with a long-lived credential, not a delegated or JIT one, and the ownership transfer to a service principal is still pending. |
| **Operational** | **Human-era** | No agent acts on this data, by design. |

**Weakest foundational item: the missing freshness SLA.** The paper's rule is that readiness is
capped by the weakest foundational layer, and freshness sits in the lowest one. It is also,
conveniently, the cheapest thing on this list to add.

## Two things worth carrying into current work

**The lethal trifecta, on the ownership transfer.** Simon Willison's formulation, quoted in the
paper: an agent turns dangerous when it holds *access to private data*, *exposure to untrusted
content*, and *a way to communicate externally*. Our vault holds the first. Fieldglass tenant
data is client-authored content we do not control, which is a weak form of the second. There is
no third today. Before any MCP or Genie surface is put over this estate, that third leg is the
one to refuse — and the `run_as` service principal work is the right moment to decide it,
because that is when a standing identity with vault rights gets created.

**Reversibility over transaction size.** The paper argues that the class of damage predicts safe
automation better than the money involved: *"a $50,000 internal ledger correction you can back
out is a safer thing to automate than a $200 payment to an external account you cannot claw
back."* Our `delta.appendOnly = 'true'` is a reversibility property expressed as a table
setting, and `append_only_check.py` fails the build on any mutation. The one irreversible thing
we did this fortnight — the destructive Fieldglass reload — was correctly a human decision, and
the paper explains why that instinct was right in a way "it was a big table" does not.

## What to skip

- **Adaptive Gold** — agents curating their own materialised views. The paper labels it an
  extrapolation, and it is the one part with no shipping example behind it. Three layers below
  our position.
- **Confidence-threshold routing.** The paper concedes turning quality signals into one score is
  *"an open design problem, not a solved one"* and advises a hard gate first. We already gate
  hard. Nothing to add.
- **The agentic-lineage tooling survey** (Langfuse, Arize Phoenix, OTel-for-AI). Correct advice,
  no subject.
- **MCP capability design.** Genuinely good — "design capabilities, not endpoints", five to ten
  well-described capabilities over fifty thin API wrappers — and entirely premature for us.

## Recommendation, in value order

1. **Declare a freshness SLA per source binding**, keyed to when the feed last *loaded
   successfully*, not when a value last changed — and make a breach fail the load rather than
   warn. This is the paper's floor, it is the one foundational row where we score human-era, and
   the vault already separates the two clocks the rule depends on. It also has a live consumer:
   BRZ-15 exists because a stale-clock question had no declared answer.
2. **Write governed quality expectations into `ref_dq_expectation`.** The table exists, the
   dashboard already reports its emptiness as 0% coverage, and the compiled-in rules prove the
   mechanism works. What is missing is business rules — and per this repo's own discipline, each
   one has to be shown to fail before it is believed.
3. **When Gold is built, build the semantic model with it, not after it.** Under 20% → 92.5% is
   the strongest single argument in any of the three papers, and the domain model it needs to sit
   on already exists in `metadata/entities/`, the DBML and the ontology.
4. **Name an owner per data contract and per access scope.** The paper's operating-model point,
   and the one whose absence this estate has already paid for once.

## One caveat

The paper is consultancy thought leadership with a book to sell, and some of its evidence is
thinner than its prose: the Precisely/Drexel and KPMG surveys are cited for a rhetorical gap
between confidence and readiness rather than for anything measurable, and the AtScale benchmark
is a vendor's own number for a vendor's own category. The architecture arguments do not depend
on any of that, and they are the part that transfers.

Its real strength for us is the opposite of the Databricks papers': it is prescriptive about
*sequence* rather than about product. Contracts before context, context before access,
observability throughout, and readiness capped by the weakest foundation. That ordering is a
claim we can check our own plan against, which is more than a feature list offers.
