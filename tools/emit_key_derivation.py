#!/usr/bin/env python3
"""Emit the golden record of WHAT GOES INTO EVERY HASH KEY.

WHY THIS FILE EXISTS. Measured 4 September 2026: three mutations to
spec.hash_key_columns -- dropping the source scope, reversing a link's parent order, and
dropping a link's transaction key -- each passed all 121 structural checks AND all 948
verification checks. Every one of them silently changes identity: the loader keeps
building, rows keep landing, and the only symptom is that a join to the parent hub returns
nothing. The component selection was the least-guarded thing in the repo.

WHAT IT IS NOT. tests/golden_hash_vectors.json pins the ALGORITHM -- SHA-256 over a
normalised concatenation, proven byte-identical between Spark and the pure-Python
reference by hash_parity_check, gate zero. It says nothing about WHICH columns are fed in.
This file pins that second half. Both are needed: the right algorithm over the wrong
components produces a perfectly reproducible key for the wrong thing.

NOT metadata/key_composition.json, WHICH ALREADY EXISTS AND IS NOT THIS. That file
digests the entity YAML -- business_keys, transaction_key, parents, key_style, tenant_key,
and each binding's key_columns/key_literals/parent_keys -- so a DECLARATION change shows up
as a moved digest. It is thorough about declarations, and that is exactly why the three
mutations above sailed through it: they changed the CODE that reads those declarations, not
the declarations, so no digest moved. Composition is what the model says; derivation is
what the loader does with it. Both need gating, and a digest cannot tell you what a key IS
-- this file can, which is what lets the source-to-target mapping quote real components
instead of guessing.

HOW IT GATES. verify_repo regenerates this file and fails if it differs, so a change to
any key's composition cannot land as a side effect -- it lands as a reviewed diff naming
the entity, the binding, the column and the exact component list. Re-keying the estate
should be hard to do by accident and easy to see on purpose.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import hashing, naming, spec  # noqa: E402

GOLDEN_PATH = ROOT / "metadata" / "key_derivation.json"


def _scope_owner(model, entity, column: str):
    """The entity whose key_style decides whether `column` carries a source scope.

    An entity's own hash key is scoped by its own key_style. A parent foreign key is
    scoped by the PARENT's, since the whole point of an FK is to equal the key the parent
    builds for itself. A satellite's parent column names its parent; when that parent is a
    link, the link's own key_style governs, which is the same rule one level down.
    """
    if column == getattr(entity, "hk_column", None):
        return entity
    # RESOLVE THROUGH THE MODEL'S LEGS BEFORE FALLING BACK TO THE NAME. A roled foreign
    # key is `parent_legal_entity_hk`, and stripping `_hk` yields `parent_legal_entity`,
    # which is not an entity -- so the name-only path would silently fall through to the
    # REFERENCING entity and report a correctly unscoped authored FK as a federated key
    # missing its scope. Measured: it did, on both legs of the first hierarchical link.
    for hub, role in spec.parent_legs(entity):
        if column == naming.hk(hub, role):
            return model.get(hub)
    parent = column[:-3] if column.endswith("_hk") else column
    try:
        return model.get(parent)
    except spec.SpecError:
        # A parent column whose name is not an entity would be a naming bug elsewhere;
        # falling back to the referencing entity keeps this emitter honest about not
        # knowing rather than inventing an owner.
        return entity


def record(model) -> dict:
    """Every hash-key column of every binding, with its ordered components and scope.

    Keyed by "entity/binding/column" so a diff names exactly what changed. The scope is
    recorded separately from the columns rather than folded in as a quoted literal: it is
    prepended by hashing.hash_key, not by the caller, and a reader comparing two entries
    needs to see that a federated key differs from an authored one by more than a string.
    """
    keys: dict[str, dict] = {}
    for entity in sorted(model.entities, key=lambda e: e.name):
        for src in entity.sources:
            for column, (columns, scope) in sorted(
                spec.hash_key_columns(model, entity, src).items()
            ):
                # WHOSE key_style DECIDED THE SCOPE. For an entity's own key it is
                # its own; for a parent foreign key it is the PARENT HUB's, because the
                # FK must be hashed exactly as that hub's own loader hashes it. An
                # assertion that compared the scope against the REFERENCING entity's
                # key_style would fire on nine correct keys -- measured, it did.
                governs = _scope_owner(model, entity, column)
                keys[f"{entity.name}/{src.name}/{column}"] = {
                    "kind": entity.kind,
                    "scope_from": governs.name,
                    "scope_key_style": governs.key_style,
                    "components": list(columns),
                    "source_scope": scope,
                }
    return {
        # The rulebook version belongs here as well as in hashing.py: the components and
        # the normalisation together define the key, so a record of one without the other
        # cannot say which key it describes.
        "rulebook_version": hashing.RULEBOOK_VERSION,
        "keys": keys,
    }


def render(model) -> str:
    return json.dumps(record(model), indent=2, sort_keys=True) + "\n"


def main() -> int:
    model = spec.load_model(ROOT / "metadata" / "entities")
    GOLDEN_PATH.write_text(render(model), encoding="utf-8")
    n = len(record(model)["keys"])
    print(f"wrote {GOLDEN_PATH.relative_to(ROOT)} -- {n} hash-key column(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
