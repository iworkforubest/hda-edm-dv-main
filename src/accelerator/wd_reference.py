"""Which Workday reference types this vault lands, and which it already models.

PURE AND SPARK-FREE. Deciding what to land is a modelling decision, and a modelling
decision that can only be tested by running a pipeline does not get tested.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml


def load_types(path: Path) -> list[str]:
    """The generated inventory's `types` list. Falls back to [] on any read/parse/shape
    failure -- this is called at module scope by the test suite, so a malformed
    artefact must not abort the caller; it must be reported by the checks that consume
    this list instead (matches the guard already shipped in verify_repo.py for the
    same artefact)."""
    try:
        return list(json.loads(path.read_text(encoding="utf-8"))["types"])
    except Exception:  # noqa: BLE001
        return []


def load_exclusions(path: Path) -> dict[str, str]:
    """Reference type -> the entity name that already models it. Falls back to {} on a
    missing file, invalid YAML, or a non-dict root/`exclusions` value -- a missing or
    mangled exclusions file is a realistic merge outcome, and it must make the
    "exclusion list is not empty" check go red rather than abort the caller."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(raw, dict):
        return {}
    exclusions = raw.get("exclusions")
    return dict(exclusions) if isinstance(exclusions, dict) else {}


def classify(types: list[str], exclusions: dict[str, str]
             ) -> tuple[list[str], list[str]]:
    """(landed, excluded). Every type goes to exactly one side -- never both, never
    neither. That totality is the property Task 2's checks assert."""
    landed = sorted(t for t in types if t not in exclusions)
    excluded = sorted(t for t in types if t in exclusions)
    return landed, excluded


class EmptySnapshotError(ValueError):
    """A current snapshot with no types at all.

    A failed Get_References call writes an empty file. Diffed naively that is
    indistinguishable from "Workday withdrew every reference value in existence", and
    the resulting load would close every window and report success. Refusing is the
    only safe reading: an empty snapshot is evidence of a broken fetch, never of a
    mass withdrawal.
    """


def effectivity_rows(previous: dict[str, set[str]], current: dict[str, set[str]],
                     snapshot_dts: str) -> list[dict[str, str]]:
    """Rows recording what opened and what closed between two snapshots.

    ONLY TYPES PRESENT IN `current` ARE COMPARED. Types are fetched one call at a time,
    so a snapshot covering some types says nothing about the rest -- treating a missing
    TYPE as the withdrawal of all its VALUES would retire a whole domain because one
    call was not made.

    THE EMPTY-SNAPSHOT GUARD IS PER TYPE, NOT ONLY WHOLE-SNAPSHOT.
    tools/fetch_workday_references.py fetches exactly ONE reference type per invocation;
    on a 200 with an unparseable or empty body it writes `{"reference_id_type": X,
    "rows": []}` and returns successfully. An assembler over those per-type files
    produces a `current` dict where X maps to an EMPTY set while every other type is
    fine -- indistinguishable, at the whole-dict level, from "everything is present and
    this type genuinely has zero values". Read naively, that empty set diffs against a
    non-empty `previous` entry as "every value of X was withdrawn", so a single failed
    fetch for one type would silently retire that entire type's domain while the load
    reports success. So: a type key that IS present in `current` must map to a
    NON-EMPTY set of values, or this raises -- naming the offending type(s) -- in
    addition to (not instead of) the whole-snapshot guard below, because a wholly empty
    `current` is the same failure at a coarser grain and both are real.
    """
    if not current:
        raise EmptySnapshotError(
            "current snapshot is empty; refusing to read that as a mass withdrawal")
    empty_types = sorted(t for t, values in current.items() if not values)
    if empty_types:
        raise EmptySnapshotError(
            f"type(s) {empty_types} are present in the current snapshot with NO values; "
            f"refusing to read that as those types' entire reference sets being "
            f"withdrawn -- a failed per-type fetch (Get_References returning an "
            f"unparseable or empty body) looks exactly like this")
    rows: list[dict[str, str]] = []
    for ref_type in sorted(current):
        was = previous.get(ref_type, set())
        now = current[ref_type]
        # Validate BEFORE any sorted() over a diff of `now`/`was`: a mixed str/None set
        # raises an opaque TypeError from sorted() itself, which would surface before a
        # clear, named ValueError ever got the chance to.
        for ref_id in now:
            _validate_reference_id(ref_type, ref_id)
        for ref_id in was:
            _validate_reference_id(ref_type, ref_id)
        for ref_id in sorted(now - was):
            rows.append({"reference_id_type": ref_type, "reference_id": ref_id,
                         "reference_status": "OFFERED", "effective_from": snapshot_dts})
        for ref_id in sorted(was - now):
            rows.append({"reference_id_type": ref_type, "reference_id": ref_id,
                         "reference_status": "WITHDRAWN",
                         "effective_from": snapshot_dts})
    return rows


def _validate_reference_id(ref_type: str, ref_id: object) -> None:
    """Reject a reference id that would become a bad hub business key.

    An empty string is a legitimate-looking value that would hash into a real hub row --
    a blank business key nothing downstream would flag as wrong until someone tried to
    use it. A non-string (None from a mixed-type set, or anything else a malformed fetch
    might produce) either violates the landing table's `reference_id STRING NOT NULL` at
    load time, far from here, or raises an opaque TypeError from `sorted()` on a mixed-type
    set before this function is even reached. Loud and early, naming the offending type
    and value, beats a NOT NULL violation discovered during a load.
    """
    if not isinstance(ref_id, str) or not ref_id.strip():
        raise ValueError(
            f"reference_id_type {ref_type!r}: reference id {ref_id!r} "
            f"({type(ref_id).__name__}) is not a non-blank string; refusing to emit it "
            f"as a hub business key")
