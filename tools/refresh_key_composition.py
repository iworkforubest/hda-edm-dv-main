#!/usr/bin/env python3
"""
Regenerate metadata/key_composition.json -- the committed digest of every entity's
IDENTITY definition.

spec.py guards satellite payload order (check_payload_order) and pins rulebook_version
per entity. Neither watches business_keys, transaction_key, parents, key_style,
tenant_key, or a source binding's key_columns, key_literals or parent_keys -- and
changing any one of those RE-KEYS every row already loaded, silently, because the
rulebook version does not move when a key definition changes. verify_repo.py's
"[keys]" section compares the live metadata against the file this script writes, and
fails loudly on any mismatch. See signature() below for why the per-binding fields
are included, not just the entity-level ones.

    uv run python tools/refresh_key_composition.py

Run it whenever a metadata change is INTENDED to alter identity, then read the diff
before committing metadata/key_composition.json: that diff is the reviewed
acknowledgement that the change was seen and meant, not an accident that happened to
pass every other gate. Generating it blindly (never reading what changed) defeats the
purpose of having it.

Deterministic by construction: entities are read in sorted filename order, the output
is sorted by entity name, and every dict inside a signature is written with sorted
keys -- so the only lines that ever move in a diff are the ones that actually changed.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITIES_DIR = ROOT / "metadata" / "entities"
OUTPUT = ROOT / "metadata" / "key_composition.json"


def signature(entity: dict) -> dict:
    """Canonical, JSON-safe signature of one entity's identity definition.

    Covers business_keys, transaction_key, parents, key_style and tenant_key -- the
    entity-level fields that say what the key MEANS -- plus, per source BINDING,
    key_columns, key_literals, key_scope and parent_keys.

    Those three are NOT wiring that something else already guards -- they are part of
    what gets hashed, positionally, into the key. spec.py's validate() (see
    src/accelerator/spec.py ~line 569) only checks a COUNT:
    `len(key_columns) + len(key_literals) == len(business_keys)`. It never checks that
    the columns are the SAME columns. Swap `key_columns: [client_code]` for a
    different single column on an authored hub and that count check still passes
    while the hub silently re-keys -- and the same is true of parent_keys: a link/NHL
    binding's parent_keys says which of ITS OWN source columns feed a given parent
    hub's key positions, and rewiring which column feeds which position re-keys that
    parent's foreign key with every other gate (spec.py's count check,
    append_only_check, reconciliation) still green.

    This is not hypothetical: hub_accounting_journal gained a third business-key
    position (company scoping), and five children -- nhl_general_journal_line,
    nhl_journal_line, msat_journal_line_worktag, msat_journal_line_external_code and
    sat_accounting_journal_header -- had their parent_keys rewritten to add the
    company column to the accounting_journal (and, for organisation, a new) mapping.
    Only accounting_journal's OWN digest moved under the field list this function
    used to cover; the five children's parent_keys rewrites -- which change what is
    hashed into their foreign keys just as surely -- moved nothing and were
    unacknowledged. Folding key_columns and parent_keys in here, alongside
    key_literals (which already lived on the binding, not the entity, for the same
    reason: a literal composes into the key hash exactly as a source column does --
    see hub_key_components / parent_key_components), closes that gap.

    Column/parent-key ORDER is preserved, not sorted: position is meaning here (see
    hub_key_components), so a reorder that changes which value lands in which slot
    must change the digest. Only DICT keys (source name, parent name) are
    order-independent, and json.dumps(..., sort_keys=True) in digest() below handles
    that without touching list order.
    """
    # DEF-51: transaction_key is entity-level but its columns are SOURCE columns, and
    # they are hashed into a link/NHL's own key -- so a cast on one of them is an
    # identity change like any other.
    txn = [str(x) for x in (entity.get("transaction_key") or [])]

    sources = {}
    for src in entity.get("sources") or []:
        name = str(src.get("name", "")).strip().upper()
        binding: dict = {}

        key_columns = [str(c) for c in (src.get("key_columns") or [])]
        if key_columns:
            binding["key_columns"] = key_columns

        key_literals = {str(k): str(v) for k, v in (src.get("key_literals") or {}).items()}
        if key_literals:
            binding["key_literals"] = key_literals

        # DEF-53: key_scope IS the first component of a federated key.
        #
        # hashing.hash_key prepends the scope as a quoted literal ahead of every column, so
        # declaring, changing or removing a key_scope re-keys every row the binding loads --
        # exactly like editing key_literals, which has always been recorded here. It was
        # added on 4 September 2026 to fix two business-vault satellites whose parent keys
        # could never join; a field powerful enough to fix that is powerful enough to break
        # it, and it must not be able to move without moving this digest.
        key_scope = str(src.get("key_scope") or "").strip().upper()
        if key_scope:
            binding["key_scope"] = key_scope

        parent_keys = {
            str(parent): [str(c) for c in cols]
            for parent, cols in (src.get("parent_keys") or {}).items()
        }
        if parent_keys:
            binding["parent_keys"] = parent_keys

        # DEF-51, FIRST BLIND SPOT: a CAST on a key-feeding column.
        #
        # factory._stage_full applies src.cast (line ~351) BEFORE it computes the hash key
        # (~359), so casting a column that feeds a key changes the string that gets hashed
        # and re-keys every row -- with the count check in spec.validate(), the parity
        # gate and every reconciliation still green, because none of them looks at types.
        # The cast list exists for hashdiff stability on payload columns, so it is easy to
        # add one without noticing it also lands on a key.
        #
        # ONLY key-feeding casts are recorded, not the whole list. Folding in a cast on
        # debitamt -- a payload column with no identity role -- would move this digest for
        # a change that cannot re-key anything, and a digest that cries wolf stops being
        # read. That would defeat the tool more thoroughly than the gap it closes.
        key_feeding = set(key_columns) | set(txn)
        for cols in parent_keys.values():
            key_feeding.update(cols)
        casts = {
            str(c): str(t)
            for c, t in (src.get("cast") or {}).items()
            if str(c) in key_feeding
        }
        if casts:
            binding["cast_on_key_columns"] = casts

        # DEF-51, SECOND BLIND SPOT: `if binding:` used to stand here, so a binding
        # declaring NONE of the fields above vanished from the signature entirely -- and
        # with it, its NAME. For a federated entity the source name is hashed INTO the
        # key (hashing.hash_key(..., source_scope=src.name)), so renaming such a binding
        # re-keys every row it loads while this digest does not move.
        #
        # No binding in the model is empty today, so this is a hole rather than a bug.
        # The name is now recorded unconditionally, which costs one empty dict and makes
        # the rename visible whenever one does appear.
        sources[name] = binding

    return {
        "business_keys": [str(x) for x in (entity.get("business_keys") or [])],
        "transaction_key": [str(x) for x in (entity.get("transaction_key") or [])],
        "parents": [str(x) for x in (entity.get("parents") or [])],
        "key_style": str(entity.get("key_style", "federated")),
        "tenant_key": [str(x) for x in (entity.get("tenant_key") or [])],
        "sources": sources,
    }


def digest(entity: dict) -> str:
    """16-hex-char sha256 of signature(entity), with sorted keys so field order in
    the YAML source cannot change the result."""
    payload = json.dumps(signature(entity), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def compute(entities_dir: Path = ENTITIES_DIR) -> dict[str, str]:
    """{entity name: digest} for every metadata/entities/*.yml file, sorted by name.

    Reads files in sorted filename order and keys the result by the entity's own
    `name` (falling back to the filename stem), NOT the filename -- the same
    ordering and naming verify_repo.py's [keys] check uses, so the two never
    disagree over what an entity is called.
    """
    result: dict[str, str] = {}
    for path in sorted(entities_dir.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        name = str(data.get("name") or path.stem)
        result[name] = digest(data)
    return dict(sorted(result.items()))


def main() -> None:
    digests = compute()
    OUTPUT.write_text(json.dumps(digests, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(ROOT)} ({len(digests)} entities)")
    print("Review the diff before committing -- it is the acknowledgement that any "
          "identity change here was seen and meant.")


if __name__ == "__main__":
    main()
