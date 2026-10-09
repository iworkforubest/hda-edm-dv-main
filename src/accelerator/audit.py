"""Writing the load audit. ONE definition, used by every writer.

Each writer records what IT did. The rejected alternative was a single check that reads
the event log, the staging logs, the vault and the quarantine twins afterwards and
computes everything -- it would need no loader changes, but it would RECONSTRUCT the
anti-join and hashdiff arithmetic rather than observe it, and a disagreement could then
not be attributed to the load or to the audit. That is the ambiguity loop-1 exists to
remove.

The arithmetic is checked HERE, before anything is written, so an inconsistent row cannot
reach the table and the gate is not the first thing to notice.
"""

from __future__ import annotations


def _lit(value: str, field: str) -> str:
    """A SQL string literal, refusing anything that could close it.

    Refuses rather than escapes. These values come from job parameters and bundle
    variables, so a quote in one is a configuration error worth failing on, not something
    to quietly repair -- the same stance apply_governance.render() takes on identifiers.
    """
    text = "" if value is None else str(value)
    if "'" in text or "\\" in text or "\x00" in text:
        raise ValueError(
            f"audit field {field}={text!r} contains a quote, backslash or NUL and cannot "
            f"be written as a SQL literal. Refusing to escape it -- fix the value."
        )
    return f"'{text}'"


def _num(value: int, field: str) -> str:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"audit field {field}={value!r} must be an int")
    if value < 0:
        raise ValueError(f"audit field {field}={value} must not be negative")
    return str(value)


def _q(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def check_arithmetic(staged: int, accepted: int, discards: dict) -> str | None:
    """Why this writer's numbers do not balance, or None if they do.

    staged - accepted must equal the sum of the attributed discards. A discard the writer
    failed to attribute then shows up as an arithmetic gap rather than as nothing at all,
    which is what checks/audit_completeness_check.py asserts across the whole run.
    """
    if accepted > staged:
        return (f"accepted={accepted} exceeds staged={staged}: a writer cannot insert "
                f"more rows than it read")
    total = sum(discards.values())
    if staged - accepted != total:
        return (f"staged - accepted = {staged - accepted} but the attributed discards "
                f"sum to {total} ({dict(sorted(discards.items()))}): "
                f"{staged - accepted - total} row(s) unaccounted for")
    return None


def table_load_exists_sql(catalog: str, control_schema: str, *, job_run_id: str,
                          table_name: str) -> str:
    """How many audit rows this (run, table) already has.

    DEF-56: THE VAULT INSERTS ARE IDEMPOTENT AND THE AUDIT INSERT WAS NOT. A Databricks
    repair run reuses the same {{job.run_id}}, and both loaders loop over every target
    exiting 1 only at the end -- so a table that already succeeded is re-processed on the
    retry. The vault insert adds nothing (`NOT EXISTS` for a hub, the hashdiff compare for
    a satellite), but a plain INSERT INTO aud_table_load wrote a SECOND row for the same
    (run, table): same staged, accepted = 0, plus a duplicate discard set.

    audit_completeness_check.unbalanced_tables sums every discard row per (run, table)
    while iterating each LOAD row, so after that both rows report a nonzero gap and the
    completeness gate is red -- permanently, because aud_table_load is append-only and the
    duplicate cannot be removed.

    So the writers guard on existence, the same way insert_sql already does for the vault.
    The first attempt's row is the correct record: the retry genuinely inserted nothing
    new, so there is nothing new to audit.

    A COUNT, not a boolean: a caller that finds more than one row has found the very
    defect this probe exists to prevent and can say so rather than shrug.
    """
    return (
        f"SELECT count(*) AS n FROM {_q(catalog, control_schema, 'aud_table_load')} "
        f"WHERE job_run_id = {_lit(job_run_id, 'job_run_id')} "
        f"AND table_name = {_lit(table_name, 'table_name')}"
    )


def load_run_sql(catalog: str, control_schema: str, *, job_run_id: str, phase: str,
                 target: str, active_sources: str) -> str:
    if phase not in ("opened", "completed"):
        raise ValueError(f"phase={phase!r} must be 'opened' or 'completed'")
    return (
        f"INSERT INTO {_q(catalog, control_schema, 'aud_load_run')} "
        f"(job_run_id, phase, target, active_sources, recorded_at) VALUES ("
        f"{_lit(job_run_id, 'job_run_id')}, {_lit(phase, 'phase')}, "
        f"{_lit(target, 'target')}, {_lit(active_sources, 'active_sources')}, "
        f"current_timestamp())"
    )


def table_load_sql(catalog: str, control_schema: str, *, job_run_id: str,
                   pipeline_update_id: str | None, table_name: str, written_by: str,
                   staged: int, accepted: int) -> str:
    update = ("NULL" if pipeline_update_id is None
              else _lit(pipeline_update_id, "pipeline_update_id"))
    return (
        f"INSERT INTO {_q(catalog, control_schema, 'aud_table_load')} "
        f"(job_run_id, pipeline_update_id, table_name, written_by, staged, accepted, "
        f"recorded_at) VALUES ("
        f"{_lit(job_run_id, 'job_run_id')}, {update}, "
        f"{_lit(table_name, 'table_name')}, {_lit(written_by, 'written_by')}, "
        f"{_num(staged, 'staged')}, {_num(accepted, 'accepted')}, current_timestamp())"
    )


def table_discard_sql(catalog: str, control_schema: str, *, job_run_id: str,
                      table_name: str, reason: str, discarded: int) -> str:
    return (
        f"INSERT INTO {_q(catalog, control_schema, 'aud_table_discard')} "
        f"(job_run_id, table_name, discard_reason, discarded, recorded_at) VALUES ("
        f"{_lit(job_run_id, 'job_run_id')}, {_lit(table_name, 'table_name')}, "
        f"{_lit(reason, 'reason')}, {_num(discarded, 'discarded')}, current_timestamp())"
    )
