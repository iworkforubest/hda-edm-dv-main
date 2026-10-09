"""The gold layer's schemas, named once.

WHY A MODULE FOR FIVE STRINGS. These names appear in the emitter, in the generated
DDL, in the verification checks, in the grant job and in databricks.yml. This repo has
already paid for restatement -- gold_quality.py's own comment warns that a duplicated
list is "a fourth thing to keep in step". This is the one source; everything else
derives from it.

FIVE, NOT THE THREE THAT WERE ASKED FOR. governance/control_objects_gold.sql already
creates the gold control schema with aud_load_run, aud_table_load and aud_table_discard.
That schema arrives the moment the file is applied whether or not anyone planned for it,
so it is planned for. `governance` joins it for symmetry with silver, where masks and
grants live apart from the load audit.

READABLE IS A SUBSET, DELIBERATELY. `governance` and `control` are readable by nobody.
That is silver's posture: the control surface is inside the Phase 6 STOP and is not read
directly. A consumer group appearing on either is a defect, not a convenience.
"""
from __future__ import annotations

#: Schema name -> what it holds. The ONE source of the gold layer's topology.
SCHEMAS: dict[str, str] = {
    "reference_data": "shared conformed reference -- calendars, code lists, currency",
    "master_data": "shared conformed master entities -- legal entity, worker, supplier",
    "wd_fin_export": "the project schema -- Workday finance export tables",
    "governance": "gold's mask functions and grant DDL",
    "control": "aud_load_run, aud_table_load, aud_table_discard",
}

#: The schemas a consumer may ever be granted on. governance and control are not here.
READABLE: frozenset[str] = frozenset({"reference_data", "master_data", "wd_fin_export"})

#: The project schema. databricks.yml sets gold_export_schema to this, per target.
EXPORT_SCHEMA: str = "wd_fin_export"
