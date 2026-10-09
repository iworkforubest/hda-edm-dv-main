"""
Silver vault pipeline entry point.

This file is deliberately tiny. All structure comes from metadata/entities/*.yml;
adding an entity or a source means adding metadata, not code. That is the property
that makes the accelerator generic across data domains rather than Job-Request
specific.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# --------------------------------------------------------------------------- #
# DEF-15: AN SDP PIPELINE LIBRARY CANNOT LOCATE ITSELF. Do not "simplify" this back
# to Path(__file__).
#
# Two separate refusals, and the second is the one that looks like it works:
#   1. __file__ is not bound at all -- an SDP library is executed as notebook-style
#      cells, exactly as a serverless spark_python_task is (DEF-12).
#   2. Recovering the path from the code object -- which IS correct for a
#      spark_python_task, and is what checks/*.py do -- yields a TRANSIENT path here,
#      e.g. /home/spark-<id>/.ipykernel/34/command-42949672961-2650799212. The
#      workspace path shown in the traceback header is display metadata, not
#      co_filename. So sys.path got a junk directory and `import accelerator` failed
#      with the entry point apparently bootstrapping correctly.
#
# The location is therefore CONFIGURATION, not something to derive: the bundle passes
# hfig.source_root: ${workspace.file_path} on both pipelines. __file__ is kept as the
# fallback for a local run, where it is bound and correct.
# --------------------------------------------------------------------------- #
_SOURCE_ROOT = spark.conf.get("hfig.source_root", "").strip()
if _SOURCE_ROOT:
    ROOT = Path(_SOURCE_ROOT)
else:  # local run, or any context that binds __file__ meaningfully
    ROOT = Path(__file__).resolve().parents[2]

_src = str(ROOT / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

try:
    from accelerator import factory, naming, spec  # noqa: E402
except ModuleNotFoundError as _e:  # pragma: no cover - diagnostic, not logic
    raise ModuleNotFoundError(
        f"could not import 'accelerator'. hfig.source_root={_SOURCE_ROOT!r} gave "
        f"ROOT={ROOT!r}, so {_src!r} was added to sys.path. If hfig.source_root is "
        f"empty the pipeline configuration is missing it -- see DEF-15."
    ) from _e

METADATA_DIR = ROOT / "metadata" / "entities"
# DEF-55: THE FALLBACK MUST NOT BE ABLE TO NAME ANOTHER LAKE'S CATALOG. This defaulted to
# "hfig_eu.governance.ref_dq_expectation" -- stale in its SCHEMA (the DQ reference moved to
# ${control_schema}) and, far worse, wrong in its CATALOG: hfig_eu is an EU PRODUCTION
# catalog, so a US or APAC pipeline whose configuration lost hfig.expectations_table would
# have read governed rules cross-region, out of a lake it has no residency right to touch.
# Being a default, that could only ever have surfaced as rules quietly not applying.
#
# The fallback is now built from hfig.catalog -- the pipeline's OWN catalog, which
# resources/vault_pipeline.yml already sets on both pipelines (see the DEF-21 note there).
# So the default cannot name a catalog other than the one this pipeline writes to. With
# neither conf set it is empty, and factory._expectations already falls back to the two
# compiled-in key-safety rules when the table cannot be read -- the same degradation as an
# absent table, rather than a read of somebody else's data.
_PIPELINE_CATALOG = spark.conf.get("hfig.catalog", "")
EXPECTATIONS_TABLE = spark.conf.get(
    "hfig.expectations_table",
    f"{_PIPELINE_CATALOG}.control.ref_dq_expectation" if _PIPELINE_CATALOG else "",
)
DOMAINS = [d.strip() for d in spark.conf.get("hfig.domains", "").split(",") if d.strip()]
# WHICH DECLARED BINDINGS ACTUALLY LOAD HERE (parent decision D5). Same shape as the
# domain filter above: comma-separated bundle configuration, empty meaning "everything".
# Region and environment stay out of the MODEL -- the generator receives an opaque list
# and never asks which lake it is running in.
ACTIVE_SOURCES = spark.conf.get("hfig.active_sources", "")

model = spec.load_model(METADATA_DIR)

# RESOLVED AGAINST THE WHOLE MODEL, BEFORE the two filters below. The Business Vault
# pipeline declares only csat entities, so resolving after filtering would make every GP
# and UKG binding "unknown" there and refuse a perfectly good list. Activity is a
# property of the lake, not of the slice of the model this pipeline happens to declare.
ACTIVE = spec.resolve_active_sources(model, ACTIVE_SOURCES)
for _skipped in spec.inactive_bindings(model, ACTIVE):
    print(f"[accelerator] source binding declared but INACTIVE in this lake: {_skipped}")

# WHAT THIS PIPELINE DECLARES, AS A SET OF NAMES -- the model itself stays whole.
#
# It used to prune `model.entities` for both filters. That is wrong, and the domain split
# proved it on its first run: a hash key is composed from its PARENT HUB's declaration, so
# pruning the parent away raises `SpecError: unknown parent entity 'organisation'` at
# definition time. raw_vault_finance and raw_vault_pay_bill both died that way --
# nhl_general_journal_line's parent organisation is in the party domain, and
# nhl_invoice_line's supplier and worker are too. Thirteen such references exist today.
#
# The layer filter had the same bug latent in it: a business-vault csat's parent is a raw
# NHL. Nobody had met it because no such csat is active yet.
DECLARED = {e.name for e in model.entities}
if DOMAINS:
    DECLARED &= {e.name for e in model.entities if e.domain in DOMAINS}

# The Business Vault computes FROM the Raw Vault, so the two layers are separate
# pipelines writing to separate schemas. An SDP pipeline targets one schema.
BUSINESS_KINDS = naming.BUSINESS_KINDS   # ONE definition, shared with apply_governance
LAYER = spark.conf.get("hfig.vault_layer", "raw").strip().lower()
if LAYER not in ("raw", "business"):
    raise ValueError(f"hfig.vault_layer must be raw|business, got {LAYER!r}")
if LAYER == "raw":
    DECLARED &= {e.name for e in model.entities if e.kind not in BUSINESS_KINDS}
else:
    DECLARED &= {e.name for e in model.entities if e.kind in BUSINESS_KINDS}

built = factory.build(model, spark, EXPECTATIONS_TABLE, active_sources=ACTIVE,
                      only=frozenset(DECLARED))
print(f"[accelerator] declared {len(built)} vault objects: {', '.join(sorted(built))}")
