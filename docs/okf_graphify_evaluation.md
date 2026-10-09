# Production-Grade OKF + Graphify — assessed against this accelerator

**Read 5 September 2026.** Udaykiran Estari, *Data Science Collective* (Medium), 13 August
2026, ~12 minutes. The fourth paper assessed this way, after
[`industry_data_models_evaluation.md`](industry_data_models_evaluation.md),
[`genie_ontology_evaluation.md`](genie_ontology_evaluation.md) and
[`agentic_ai_readiness_evaluation.md`](agentic_ai_readiness_evaluation.md) — and the only
one whose subject is **the project's own memory** rather than its data.

## What it is about

Two things, six weeks old between them:

- **OKF (Open Knowledge Format)** — a vendor-neutral spec from Google Cloud, v0.1, published
  12 June 2026. Curated knowledge as a directory of markdown files with YAML frontmatter;
  `type` is the only mandatory field. Auto-generated `index.md` files give **progressive
  disclosure**, so an agent walks a hierarchy instead of loading a bundle.
- **Graphify** — tree-sitter AST parsing across 36 languages, entirely local and
  deterministic, no LLM. Produces a call graph with every edge tagged `EXTRACTED` (explicit
  in source) or `INFERRED` (resolved by the tool), plus three artefacts: `graph.json`,
  `graph.html`, `GRAPH_REPORT.md`.

**The article's thesis is not about either tool.** It opens by dismantling the "71.5x token
reduction" headline — a single favourable benchmark on a 52-file repo, against independent
replications landing at 6.8x–49x and a from-scratch run at 7.3x — and then argues the real
question is different:

> A production-grade setup is defined by its failure-detection story, not its benchmark
> story. **Write down, concretely, how you would detect a stale graph within 24 hours.** If
> you can't answer that in specific, mechanical terms, you don't have a production-grade
> setup. You have a demo that hasn't failed yet.

## Neither tool applies here, and the paper says so itself

**Graphify has a stated floor:** *"graph construction and maintenance overhead only pays for
itself above roughly 500 files. Below that threshold, you're paying tooling tax for savings
that don't exist yet."*

Measured on this repo: **871 tracked files, of which 191 are parseable code** — 76 Python,
76 YAML, 32 YML, 7 SQL. The rest is 473 markdown (mostly the plans and specs that are this
project's history) and 128 images. So we are at 38% of the tool's own break-even, and the
paper's advice on its own numbers is: don't.

There is a second reason, and it is the stronger one. **Graphify answers "what calls what"
in code.** This project's value is not in its call graph — it is in its *model*, and that is
already emitted from `metadata/entities/` as a DBML diagram (29 tables, 35 `Ref:` edges) and
an OWL/RDF ontology (1,358 triples, `rapper`-validated), both regenerated and gated
byte-identical. A Python call graph over 76 files would be a fourth artefact describing the
least interesting layer.

**OKF is a closer fit, because we hand-rolled what it specifies.** `OPEN_ITEMS.md` +
`DECISION_LOG.md` + the request pages + the design docs are a curated knowledge bundle, and
the split performed on 5 September — a 239-line live board, 3,527 lines of closed record —
was progressive disclosure arrived at from first principles, three months after a spec for it
existed. Adopting the *format* would buy portability to other agents and a conventional
`type` field. It would not buy the thing that makes ours work, which is the gating; and the
paper is explicit that OKF's *"freshness and synchronization promises are explicitly unproven
at real organizational scale."* Worth knowing about. Not worth converting to.

## Where the paper is genuinely valuable: its failure taxonomy is our week

Strip the tools away and the article is a catalogue of ways a knowledge artefact goes stale
**while every mechanism meant to keep it fresh reports success**. That is not a description
of Graphify. It is a description of what this repo found five separate times on 5 September:

| the paper's mechanism | our instance, same day |
|---|---|
| a hardcoded extension allowlist drifted from the authoritative list, so valid files silently skip a rebuild | `render_erd` scanned PDFs for `link_` when the prefix is `lnk_`; `append_only_check` hand-typed 10 vault prefixes where 15 exist |
| the three artefacts can lie to each other, with no built-in cross-check | 14 generated artefacts, all gated byte-identical, and the one that cannot be — a PDF wkhtmltopdf stamps — gated through a provenance digest **plus** characters-per-page, after it once matched its digest while carrying six blank pages |
| `graphify-out/` tracked in git makes every regeneration a dirty tree | exactly what the ERD PDF did until 5 September, churning 142 KB on every no-op; fixed by skipping the render when the HTML digest is unchanged |
| a stale bundle handed to an agent as ground truth | `GRAPH_SUMMARY.md` two weeks stale on `origin/main`; ours was three request documents and 18 dangling defect citations |

**On its central question — "how would you detect staleness within 24 hours?" — the answer
for our repo artefacts is better than the paper's own recommendation.** It proposes *"a
one-line CI check comparing these timestamps."* We assert byte-identical regeneration on
every push, on two interpreters, and mutation-prove that each gate can fail. A timestamp
comparison catches a rebuild that did not run; it does not catch a rebuild that ran and
produced something wrong.

## And then it points at the one surface we do not gate

Which is where reading it stopped being comfortable.

`verify_repo` covers the repository. **It cannot see the two knowledge artefacts that are
loaded into every session as authority and live outside it.** Both were stale when checked:

| artefact | claimed | actual, 5 Sep |
|---|---|---|
| `.claude/skills/dv-accelerator-gates/SKILL.md` | `test_accelerator.py` has **121** checks | **1,129** — off by an order of magnitude |
| the memory note on the CI floor | the repo's `.venv` is **Python 3.11.15** | **3.13.15** |

The second is the one that matters. That note exists precisely because a defect can fail on
3.11 and pass on 3.13, and it says in as many words: *"check `.venv/bin/python --version`
rather than assuming either, because which one it is inverts the risk."* The venv has now
flipped twice. **Every local suite run on 5 September was the 3.13 leg**, while the note that
exists to prevent exactly that said 3.11. Nothing shipped broken — CI runs both and was green
— but a full day's local confidence covered one leg, and the instrument that should have
caught it had itself gone stale.

**The sharpest instance is smaller and entirely mine.** The Spark suite moved from 137 to 140
checks when one satellite was added, and **137 was carried into two consecutive pull-request
bodies** from an earlier measurement, without re-running. Measured once, quoted twice, wrong
by the second. That is the article's thesis in miniature, committed the same afternoon it was
read.

Both are now corrected, and the memory note was rewritten to state the version as *the last
observation rather than the current state* — because it had been read as a fact when it was
written as an instruction.

## What to take, what to skip

**Take:** the discipline question, applied to the surfaces `verify_repo` cannot reach. Write
down how a stale skill file or memory note would be detected within a day. Today the answer
is "somebody notices", which is the answer the paper says means you have a demo.

**Take:** the artefact cross-check argument. Ours is stronger for generated files and absent
for hand-written ones, and that boundary is worth being deliberate about rather than
incidental.

**Skip:** Graphify, on its own stated threshold and because our model is already emitted in
two richer formats.

**Skip:** converting the knowledge bundle to OKF. We have the structure the format specifies;
the format would add a `type:` field and a spec version, and the paper concedes its freshness
guarantees are unproven.

**Skip:** the token-savings framing entirely. The article does this itself, in its first
section, and is right to.

## One caveat on the source

This is a Medium post, not a paper: member-gated, cross-linked to six of the author's own
articles, and its evidence is issue-tracker reports and second-hand replications rather than
anything reproduced first-hand. Several specifics — the two-week-stale summary, the 114-minute
rebuild killed on a 96-core Xeon, the Windows `nohup` failure — are cited to issues rather
than measured. That does not make them wrong, and the *argument* does not depend on them: it
depends on the observation that a maintenance mechanism which reports success while doing
nothing is worse than no mechanism at all. That observation is sound, and it was worth having
pointed at our own memory rather than at our data.
