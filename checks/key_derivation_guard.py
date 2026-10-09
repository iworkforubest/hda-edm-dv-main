"""HARD GATE: the lake must have been loaded under the key derivation the model declares.

THE THIRD LEG OF THE HASH CONTRACT, and it was missing until 4 September 2026.

  checks/hash_parity_check.py      the ALGORITHM travels -- Spark equals the reference
  metadata/key_derivation.json     the COMPONENTS are the ones intended -- gated byte-identical
  THIS                             the LAKE agrees with both

Nothing held the third. When hub_job_request was re-keyed to tenant_scoped, the model said
one thing and 45,516 stored rows said another, and the next load would have appended 45,516
NEW-keyed rows beside them -- with every gate green:

  * append_only_check looks for MUTATION and finds none: these are inserts.
  * its uniqueness check passes, because the old and new keys are DIFFERENT rather than
    duplicated, so each appears exactly once.
  * landing_integrity_check does not cover hubs -- RECONCILABLE_KINDS is links and NHLs.

So the hub silently doubles, and every join still resolves -- against the old rows. The
symptom is a hub with two keys per business object and a satellite pointing at the older
one, which is not a shape any count notices.

WHY A DIGEST AND NOT A RECOMPUTATION. Recomputing a stored key would mean re-deriving it
from columns the vault does not keep -- a narrowed hub holds its keys, not the source
columns that fed them. metadata/key_derivation.json is already generated from the model and
already gated byte-identical, so its digest stands for the whole key composition of the
estate in one value. Comparing digests names a re-key rather than inviting a guess about
which key moved.

AN UNRECORDED LAKE WITH ROWS IN IT FAILS, and that is the point rather than an oversight.
On the first run against an existing vault there is no prior record, and treating "no
record" as "fine" would make this gate unfailable exactly when it matters -- the DEF-48
shape this repo has met a dozen times. A lake whose provenance is unknown is a lake that
may already disagree with the model, so it fails and says what to do: reload, or record the
digest deliberately once you have satisfied yourself the stored keys match.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does not define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

DERIVATION_PATH = ROOT / "metadata" / "key_derivation.json"
RECORD_TABLE = "ctl_key_derivation"


def current_digest(path: Path = DERIVATION_PATH) -> str:
    """The digest of the committed key-derivation record, bytes as committed.

    Hashed as BYTES, not as re-serialised JSON: the file is gated byte-identical, so its
    bytes are the artefact. Re-serialising would make the digest depend on this script's
    json settings rather than on the reviewed file.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def current_key_count(path: Path = DERIVATION_PATH) -> int:
    return len(json.loads(path.read_text(encoding="utf-8"))["keys"])


def current_map(path: Path = DERIVATION_PATH) -> str:
    """The committed derivation record, as the exact bytes the digest is taken over.

    Returned as TEXT rather than as a parsed object because it is stored verbatim and
    re-hashed on read: a record whose stored map does not hash to its stored digest is a
    record someone edited, and the gate says so instead of trusting it.
    """
    return path.read_text(encoding="utf-8")


ARCHIVE_DIR = ROOT / "metadata" / "key_derivation_archive"


def archived_map(digest: str, archive_dir: Path = ARCHIVE_DIR) -> str | None:
    """A committed derivation map whose bytes hash to `digest`, or None.

    WHERE A PRE-MAP RECORD'S EVIDENCE LIVES. Records written before 25 September 2026 carry
    a digest and no map, so the gate cannot say WHICH key moved and refuses as UNMAPPED.
    The map that produced that digest is not unknown, though -- it is a committed
    metadata/key_derivation.json from the run that wrote the record, and
    tools/backfill_key_derivation_record.py finds it by hashing every commit that touched
    the file and keeping the one that matches.

    IT IS FILED HERE RATHER THAN WRITTEN TO THE RECORD TABLE, for two reasons. The control
    table is appendOnly and owned by the service principal, so completing a row means either
    a permission the operator may not hold or an append that looks like a new assertion. And
    the archive is better evidence: it sits in git, it is diffable, and its provenance is a
    commit rather than a row someone wrote.

    THE FILENAME IS NOT TRUSTED. Every candidate is re-hashed and only an exact match is
    returned, so a file misfiled under the wrong digest -- or edited after filing -- is
    ignored rather than believed. That keeps the archive from becoming a place to assert
    that a lake is fine.
    """
    if not archive_dir.is_dir():
        return None
    for path in sorted(archive_dir.glob("*.json")):
        text = path.read_text(encoding="utf-8")
        if hashlib.sha256(text.encode("utf-8")).hexdigest() == digest:
            return text
    return None


def keys_of(map_text: str) -> dict:
    return json.loads(map_text)["keys"]


def key_differences(stored_map: str, current_map_text: str) -> tuple[list[str], list[str], list[str]]:
    """(moved, removed, added) between two derivation records, by key name.

    MOVED is the only one that re-keys stored rows: a key that was recorded and now derives
    differently means every hash built under it disagrees with the model. REMOVED is a key
    the model no longer describes at all -- rows keyed that way are still in the lake with
    nothing to check them against. ADDED is growth, and growth is not a re-key.
    """
    was, now = keys_of(stored_map), keys_of(current_map_text)
    moved = sorted(k for k in set(was) & set(now) if was[k] != now[k])
    return moved, sorted(set(was) - set(now)), sorted(set(now) - set(was))


def verdict(stored: str | None, current: str, vault_has_rows: bool,
            stored_map: str | None = None, current_map_text: str | None = None,
            at_risk: frozenset | set | None = None):
    """(status, explanation) -- the whole decision, as a pure function.

    PER KEY, NOT PER DIGEST, since 25 September 2026. The gate used to compare one digest
    over the whole model, and that conflated the two things it most needed to tell apart:

        the model GREW            a new entity declares new keys; nothing stored moves
        the model was RE-KEYED    a key that already exists now derives differently

    Both change the digest. Measured on usnc_tds: the record was written 4 September with
    56 keys, the model declares 87, and ALL 56 recorded keys derive identically today --
    every field, same rulebook. The gate nonetheless reported RE_KEYED and prescribed
    dropping and reloading, against 2,221,108 rows whose keys provably had not moved.

    A GATE THAT CRIES WOLF ON ORDINARY GROWTH GETS SEEDED PAST, and a gate that is seeded
    past every few weeks is not a gate. So growth now passes as EXTENDED and says what it
    admitted, while a genuine re-key still fails -- and fails with the names of the keys
    that moved, which the digest could never give.

    Seven states; four of them fail:

      no record + empty vault   FIRST_LOAD  nothing to disagree with; record and proceed.
      no record + rows          UNRECORDED  FAILS. Provenance unknown, may already differ.
      digests equal             MATCHED     proceed.
      only additions            EXTENDED    proceed; the added keys are named.
      a recorded key changed,   RE_KEYED    FAILS, naming the keys. Rows exist that were
        and its table has rows              built a way the model no longer describes --
                                            whether the key MOVED or was REMOVED.
      a recorded key changed,   EXTENDED    proceed, saying so. Nothing was ever keyed
        and nothing was loaded              that way, so there is nothing to disagree.
      digest differs, no map    UNMAPPED    FAILS. A record from before this gate stored
                                            its map cannot say WHICH key moved, and
                                            "something changed" is not grounds to reload
                                            2m rows -- nor to wave them through.
      map != its own digest     CORRUPT_RECORD  FAILS. The stored map is re-hashed on read,
                                            so a hand-edited record cannot pose as evidence.
    """
    if stored is None:
        if vault_has_rows:
            return ("UNRECORDED", (
                "the vault holds rows but no run has recorded which key derivation built "
                "them. Their provenance is unknown, so they may already disagree with the "
                "model. Either reload the vault, or -- having satisfied yourself the stored "
                "keys match the current model -- record the digest deliberately with "
                "--seed. Treating an absent record as agreement would make this gate "
                "unfailable exactly when it matters"))
        return ("FIRST_LOAD", "no prior record and an empty vault: nothing can disagree")

    if stored_map is not None:
        # RE-HASHED, NOT TRUSTED. The map is the evidence a re-key did not happen, so a map
        # that does not hash to the digest recorded beside it is not evidence at all.
        if hashlib.sha256(stored_map.encode("utf-8")).hexdigest() != stored:
            return ("CORRUPT_RECORD", (
                f"the recorded derivation map does not hash to the digest recorded beside "
                f"it ({stored[:16]}…). One of the two was edited after the fact, so neither "
                f"can be used to decide whether the lake was re-keyed. Re-record from a "
                f"committed metadata/key_derivation.json whose digest matches"))

    if stored == current:
        return ("MATCHED", f"the vault agrees with the model at {current[:16]}…")

    if stored_map is None or current_map_text is None:
        return ("UNMAPPED", (
            f"the vault was loaded under key derivation {stored[:16]}… and the model now "
            f"declares {current[:16]}…, but the record carries no derivation map, so WHICH "
            f"keys changed cannot be answered. That record predates this gate storing its "
            f"map. Attach the map to the existing record with "
            f"tools/backfill_key_derivation_record.py -- it finds the committed "
            f"metadata/key_derivation.json whose digest EQUALS the recorded one and appends "
            f"it as evidence, so the attachment is an identification rather than an "
            f"assertion. Until then this neither passes nor prescribes a reload"))

    moved, removed, added = key_differences(stored_map, current_map_text)

    # A CHANGED KEY IS ONLY DANGEROUS IF SOMETHING WAS KEYED THAT WAY.
    #
    # This failed outright on any removal, and that is the same over-broad claim the
    # whole-model digest made: it asserted rows were at risk without asking whether any
    # exist. Measured 25 September, the second time in one day: repointing the client
    # legal-entity family from a PLACEHOLDER source to Hubspot removed five recorded keys,
    # and the gate prescribed "drop and reload the affected tables" for tables that had
    # NEVER LOADED -- hal_client_legal_entity_hierarchy did not even exist.
    #
    # The caller decides what is at risk, because only it can look at the lake. An empty
    # set means nothing was loaded under any removed key, which makes the removal a
    # modelling change and not a re-keying.
    # APPLIED TO BOTH, and it took three goes to get here. The whole-model digest asserted
    # every key had moved without asking whether any had; the rewrite asked the lake about
    # REMOVED keys and forgot to ask about MOVED ones. Same question either way: a key
    # nothing was ever keyed under re-keys nothing when it changes.
    #
    # MEASURED 25 September, the third time: removing line_sibling_ordinal from
    # nhl_invoice_line's transaction key moved invoice_line_hk and invoice_line_gie's
    # invoice_line_hk. Both are real moves. Neither table has ever held a row -- pay_bill
    # had not completed a single run -- so there was nothing to disagree with.
    risky_moved = sorted(set(moved) & set(at_risk or ()))
    risky_removed = sorted(set(removed) & set(at_risk or ()))
    if risky_moved or risky_removed:
        detail = []
        if risky_moved:
            detail.append(f"{len(risky_moved)} key(s) now derive differently AND their "
                          f"tables hold rows: {', '.join(risky_moved[:8])}"
                          f"{', …' if len(risky_moved) > 8 else ''}")
        if risky_removed:
            detail.append(f"{len(risky_removed)} recorded key(s) are no longer declared AND "
                          f"their tables hold rows: {', '.join(risky_removed[:8])}"
                          f"{', …' if len(risky_removed) > 8 else ''}")
        return ("RE_KEYED", (
            f"{'; '.join(detail)}. EVERY STORED HASH KEY BUILT UNDER THOSE WAS BUILT A "
            f"DIFFERENT WAY. Loading now appends new-keyed rows beside the old ones -- "
            f"inserts, so append_only passes; distinct keys, so its uniqueness check "
            f"passes; and hubs are outside landing_integrity's reach. The result is two "
            f"keys per business object with every join resolving against the old one. Drop "
            f"and reload the affected tables -- docs/RELOADING.md names which, and the "
            f"staging log is the one people forget"))

    grew = (f"grown by {len(added)}: {', '.join(added[:8])}"
            f"{', …' if len(added) > 8 else ''}" if added else "not grown")
    changed = sorted(set(moved) | set(removed))
    dropped = ("" if not changed else
               f" {len(changed)} recorded key(s) changed and NOTHING WAS LOADED under them, "
               f"so the change re-keys nothing: "
               f"{', '.join(changed[:8])}{', …' if len(changed) > 8 else ''}.")
    return ("EXTENDED", (
        f"every one of the {len(keys_of(stored_map))} recorded key(s) that is still declared "
        f"derives exactly as it did, and the model has {grew}.{dropped} Growth is not a "
        f"re-key -- no stored row is keyed under any of these -- so the load proceeds and "
        f"the new record supersedes the old"))


def require_record_table(spark, fq: str) -> None:
    """Fail if the record table is absent, instead of letting saveAsTable create it.

    THIS IS A FIX, NOT A PRECAUTION. `df.write.mode("append").saveAsTable(...)` CREATES a
    missing table -- silently, and WITHOUT the TBLPROPERTIES its DDL declares. On 4 September
    apply_source_unions was run with `--only apply_source_unions`, which does not run
    upstream tasks, so create_control_objects never made ctl_source_union and saveAsTable
    made it instead. The table came out with delta.appendOnly unset, and assert_append_only
    failed the load with exactly that: "delta.appendOnly is not set -- Delta will permit
    mutation". A control table that can be rewritten is not evidence of what a past load did.
    (`--only +task` runs upstream and would have been fine, which is precisely why this
    cannot be left to how someone invoked the job.)

    The table's existence is create_control_objects' responsibility. A writer that quietly
    provisions its own storage produces an object nothing verified -- the same reason this
    task stopped issuing CREATE SCHEMA.
    """
    if not spark.catalog.tableExists(fq):
        raise SystemExit(
            f"  * MISSING_RECORD_TABLE: {fq} does not exist. create_control_objects owns it "
            f"and has not run -- run the job, or `--only +<task>` so upstream runs too. "
            f"Writing anyway would let saveAsTable create it WITHOUT delta.appendOnly, and a "
            f"rewritable control table is not evidence."
        )


def gate_status(status: str) -> int:
    return 0 if status in ("MATCHED", "FIRST_LOAD", "EXTENDED") else 1


def is_missing_table(exc: BaseException) -> bool:
    """Is this exception Spark saying the table is not there -- and nothing else?

    Asked by error class where the runtime offers one, and by the class NAME in the
    message otherwise. Never by a loose substring on the table name: "cannot be found"
    also appears in permission and path errors, and this predicate decides whether an
    error becomes the answer "no rows".
    """
    getter = getattr(exc, "getErrorClass", None)
    if callable(getter):
        try:
            if getter() == "TABLE_OR_VIEW_NOT_FOUND":
                return True
        except Exception:  # noqa: BLE001 -- a fake or an older runtime; fall through
            pass
    return "TABLE_OR_VIEW_NOT_FOUND" in str(exc)


def table_row_count(spark, catalog, vault_schema, table):
    """Rows excluding the ghost, or None when the table does not exist.

    One place asks the lake "how many rows", so the missing-table tolerance and the refusal
    to swallow anything else are stated once. A second copy of that try/except is how the
    permissions case gets swallowed in one of them.
    """
    try:
        got = spark.sql(
            f"SELECT count(*) AS n FROM `{catalog}`.`{vault_schema}`.`{table}` "
            f"WHERE load_dts > TIMESTAMP'1900-01-01 00:00:00'").collect()
    except Exception as exc:  # noqa: BLE001 -- narrowed by is_missing_table, then re-raised
        if not is_missing_table(exc):
            raise
        return None
    return got[0]["n"] if got else 0


def keys_at_risk(spark, catalog, vault_schema, removed, model):
    """Which REMOVED keys belong to an entity whose table actually holds rows.

    THE QUESTION THE VERDICT CANNOT ASK ITSELF, because it is pure. A removed key only
    matters if something was keyed that way; a table that does not exist, or exists empty,
    puts nothing at risk. Measured 25 September: repointing the client legal-entity family
    to Hubspot removed five recorded keys and the gate demanded a reload of tables that had
    never loaded -- one of which did not exist.
    """
    tables_for: dict = {}
    for entity in model.entities:
        tables_for[entity.name] = [t for _s, t in entity.tables()]
    at_risk = set()
    for key in removed:
        entity_name = key.split("/")[0]
        for table in tables_for.get(entity_name, ()):
            n = table_row_count(spark, catalog, vault_schema, table)
            if n:
                at_risk.add(key)
                break
    return at_risk


def vault_holds_rows(spark, catalog, vault_schema, tables):
    """Does any of these tables hold a real row? -> (holds, evidence, missing).

    A DECLARED TABLE THAT DOES NOT EXIST IS AN ANSWER, NOT AN ERROR. The model declares
    what should exist; a region part-way through its first load has declared tables with
    nothing behind them yet. Measured 25 September: the scan died on
    hal_client_legal_entity_hierarchy with TABLE_OR_VIEW_NOT_FOUND before asking its first
    question about key derivation, and seven declared tables had never been built.

    EVERY OTHER ERROR PROPAGATES, and that is the load-bearing half. `holds=False` is what
    makes verdict() return FIRST_LOAD, which PASSES. A scan that swallowed a permissions
    error would report an unreadable vault as an empty one, and this gate -- whose whole
    job is to refuse a re-keyed vault -- would wave through exactly the lake it exists to
    protect. So only is_missing_table() converts an exception into an answer.

    `missing` is returned rather than logged here so the caller can say WHY the vault
    looked empty: "nothing loaded yet" and "nothing built yet" both yield FIRST_LOAD, and
    only the list of skipped tables tells them apart.
    """
    missing: list[str] = []
    for table in tables:
        n = table_row_count(spark, catalog, vault_schema, table)
        if n is None:
            missing.append(table)
            continue
        if n > 0:
            return True, f"{table} has {n:,} (excluding the ghost)", missing
    return False, None, missing


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--vault-schema", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--seed", action="store_true",
                    help="record the current digest against an unrecorded vault. Only after "
                         "verifying by hand that the stored keys match the model")
    args = ap.parse_args()

    from pyspark.sql import SparkSession, functions as F  # noqa: E402

    spark = SparkSession.builder.getOrCreate()
    digest, n_keys = current_digest(), current_key_count()
    from accelerator import hashing  # noqa: E402

    print(f"key derivation :: target={args.target} digest={digest[:16]}… keys={n_keys} "
          f"rulebook={hashing.RULEBOOK_VERSION}\n")

    rec = f"`{args.catalog}`.`{args.control_schema}`.`{RECORD_TABLE}`"
    require_record_table(spark, f"{args.catalog}.{args.control_schema}.{RECORD_TABLE}")
    rows = spark.sql(
        f"SELECT derivation_sha256, derivation_json FROM {rec} "
        f"ORDER BY recorded_at DESC LIMIT 1").collect()
    stored = rows[0]["derivation_sha256"] if rows else None
    stored_map = rows[0]["derivation_json"] if rows else None
    if stored is not None and stored_map is None:
        # A RECORD FROM BEFORE THE MAP TRAVELLED WITH THE DIGEST. Its map may still be on
        # file, and the file is only used if it hashes to the recorded digest exactly.
        stored_map = archived_map(stored)
        if stored_map is not None:
            print(f"  the record carries no map; found one in "
                  f"{ARCHIVE_DIR.name}/ hashing to {stored[:16]}… exactly")

    # DOES THE VAULT HOLD ANYTHING? Asked of the model's own keyed tables rather than by
    # listing the schema, so a table outside the model cannot make an empty vault look full.
    from accelerator import naming, spec  # noqa: E402

    model = spec.load_model(ROOT / "metadata" / "entities")
    keyed_tables = [table
                    for entity in model.entities if entity.kind in naming.KEYED_KINDS
                    for _src, table in entity.tables()]
    vault_has_rows, evidence, missing = vault_holds_rows(
        spark, args.catalog, args.vault_schema, keyed_tables)
    if evidence:
        print(f"  vault holds rows: {evidence}")
    if missing:
        # SAID OUT LOUD, because a vault that looks empty because nothing is BUILT and one
        # that looks empty because nothing is LOADED both pass as FIRST_LOAD.
        print(f"  {len(missing)} of {len(keyed_tables)} declared table(s) do not exist yet "
              f"and were skipped: {', '.join(missing[:6])}"
              f"{', …' if len(missing) > 6 else ''}")

    map_text = current_map()
    # WHICH REMOVED KEYS ARE ACTUALLY AT RISK, asked of the lake rather than assumed. The
    # verdict is pure, so the lake lookup happens here and is handed in.
    _moved: list[str] = []
    _removed: list[str] = []
    if stored_map is not None:
        _moved, _removed, _added = key_differences(stored_map, map_text)
    _changed = sorted(set(_moved) | set(_removed))
    at_risk = keys_at_risk(spark, args.catalog, args.vault_schema, _changed, model)
    if _changed:
        print(f"  {len(_changed)} recorded key(s) changed ({len(_moved)} moved, "
              f"{len(_removed)} removed); {len(at_risk)} have rows behind them")
    status, why = verdict(stored, digest, vault_has_rows, stored_map, map_text, at_risk)
    if status == "UNRECORDED" and args.seed:
        status, why = "SEEDED", "recorded deliberately with --seed against an existing vault"

    print(f"  {status}: {why}\n")

    if gate_status(status) == 0 or status == "SEEDED":
        # THE MAP TRAVELS WITH THE DIGEST from here on. Without it the next run can only
        # say "something changed"; with it, it can say which key, or that none did.
        spark.createDataFrame(
            [(args.job_run_id, args.target, digest, n_keys, hashing.RULEBOOK_VERSION,
              map_text)],
            "job_run_id string, target string, derivation_sha256 string, key_count bigint, "
            "rulebook_version string, derivation_json string",
        ).withColumn("recorded_at", F.current_timestamp()).write.mode("append") \
         .saveAsTable(rec.replace("`", ""))
        print(f"  recorded into {args.control_schema}.{RECORD_TABLE}")

    print("=" * 66)
    if gate_status(status) and status != "SEEDED":
        print(f"KEY DERIVATION GATE FAILED -- {status}")
        return 1
    print(f"KEY DERIVATION GATE PASSED -- {status}")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a task failure, for 0 as readily as 1.
    _rc = main()
    if _rc:
        sys.exit(_rc)
