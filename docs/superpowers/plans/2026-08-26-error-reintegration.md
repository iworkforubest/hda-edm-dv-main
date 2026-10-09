# Error Reintegration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a row that we wrongly rejected land on a re-run without loop-1 going red, and leave an append-only record of what we wrongly rejected and why.

**Architecture:** A new batch task after `raw_vault` digests each reconcilable table's landed rows and its quarantine twin's rows, and writes one `control.ctl_quarantine_superseded` record per landed row whose content also sits in the twin. Loop-1's identity becomes `landed + (quarantined - superseded) = approved`. Nothing is ever rewritten.

**Tech Stack:** Python 3.11/3.13, PySpark on Databricks serverless, Declarative Automation Bundles, Delta/Unity Catalog. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-26-error-reintegration-design.md`

## Global Constraints

- **This repo's tests are not pytest.** Two suites, both plain scripts: `uv run --frozen python tests/test_accelerator.py` and `uv run --frozen python verify_repo.py`. Add assertions as `check("name", condition, "detail")` calls, immediately BEFORE the final `print("\n" + "=" * 62)` block. Never introduce pytest.
- **Every new check must be proven able to fail.** Mutate the code it guards, confirm it reports FAIL and **not ABSENT** — an ABSENT means the suite aborted and every later check silently stopped running — restore, and record the mutation in the commit message. This repo has shipped five checks that could never fail, and the control-schema work found four more in its own plan text.
- **Never use `str.index`, or `[0]`/`[i+1]` indexing that can run past the end, inside a check.** All raise and abort the suite. Use `.find()` compared against `-1`, or the guarded `(result or [""])[0]`.
- **Ordering assertions on source text use the chained `-1 < src.find(A) < src.find(B)` form.** Without the leading `-1 <` the assertion is TRUE when A is absent, so it is satisfiable by deleting the call it pins.
- **No semicolon anywhere in a `.sql` file except as a statement terminator, including inside `COMMENT '...'` string literals.** `apply_governance.statements()` splits on `;` regardless of quoting. This bit the control-schema work once.
- **Every script in `checks/` ends with the DEF-14 block** — `import sys`, `_rc = main()`, `if _rc: sys.exit(_rc)`. Never `raise SystemExit(...)` on a success path: serverless surfaces SystemExit as a task failure even for exit code 0. This bit the control-schema work once, in a file a seven-file sweep missed.
- **No test may assert an absolute check count** — it differs between checkouts.
- Schema names come from bundle variables, never literals.
- `hashing.py` is a RATIFIED rulebook. **Do not modify it.** `RULEBOOK_VERSION` is `1.0.0`; a change there re-keys the estate and is out of scope. This plan only *calls* `hashing.hashdiff()`.
- Never auto-select a Databricks profile; pass `--profile hfig-usnc-tds` explicitly.

---

## File Structure

| file | responsibility |
|---|---|
| `governance/control_objects.sql` | adds `ctl_quarantine_superseded` |
| `src/accelerator/reject_digest.py` | **new** — the digest's column set and the SQL that renders it. Pure, no pyspark |
| `checks/supersede_quarantine.py` | **new** — the batch task: digest both sides, write the shortfall |
| `checks/loop1_reconciliation.py` | the identity gains the superseded term, plus a new failure |
| `resources/vault_job.yml` | the new task, and loop-1 gains `--control-schema` |
| `tests/test_accelerator.py` | checks for all of the above |

**Why a new module rather than adding to `audit.py`:** `audit.py` renders INSERT statements for the load audit. The digest is a different concern — it decides *which columns constitute a row's content* — and putting it beside the audit writer would give that file two reasons to change. It also must be importable by both the new task and the tests without dragging the audit surface along.

---

### Task 1: The supersede table

**Files:**
- Modify: `governance/control_objects.sql`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `control.ctl_quarantine_superseded` with columns `manifest_id`, `table_name`, `reject_digest`, `rulebook_version`, `superseded_by`, `reason`, `recorded_at`.

- [ ] **Step 1: Write the failing test**

```python
# The supersede table is a control object like the other five, so the placement assertion
# and append_only_check cover it for free -- but only if it declares the marker and the
# appendOnly property. It SUBTRACTS from loop-1's quarantined count, so a table that could
# be rewritten would let someone silently retune a hard gate.
check("control_objects.sql declares the supersede table",
      "${control_schema}`.ctl_quarantine_superseded" in _ctl_sql,
      "loop-1 cannot subtract superseded rejects from a table that does not exist")
_sup_chunk = ([c for c in _ctl_sql.split("CREATE TABLE IF NOT EXISTS")
               if "ctl_quarantine_superseded" in c] or [""])[0]
for _col in ("manifest_id", "table_name", "reject_digest", "rulebook_version",
             "superseded_by", "recorded_at"):
    check(f"the supersede table declares {_col}",
          _col in _sup_chunk, _sup_chunk[:200])
check("the supersede table is append-only",
      "'delta.appendOnly' = 'true'" in _sup_chunk,
      "a rewritable supersede record is a hard gate someone can retune silently")
check("and carries the control-object marker",
      "'hfig.control_object' = 'true'" in _sup_chunk,
      "without it the placement assertion built for the control schema skips this table")
check("rulebook_version is NOT NULL, so a stale digest is detectable",
      "rulebook_version  STRING     NOT NULL" in _sup_chunk
      or "rulebook_version STRING NOT NULL" in _sup_chunk,
      "a digest whose rulebook version is unknown cannot be told from one that matches")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -i supersede`
Expected: every one FAILs — the table does not exist.

- [ ] **Step 3: Add the DDL**

Append to `governance/control_objects.sql`. **No semicolons inside the comments or the COMMENT literals** — the splitter cuts on `;` regardless of quoting.

```sql
-- Rejects we later accepted, and why. Loop-1 subtracts these from its quarantined count.
--
-- THIS TABLE CAN DISARM A HARD GATE, which is why it is append-only and asserted.
-- The identity becomes landed + (quarantined - superseded) = approved, so a spurious
-- record REDUCES the quarantined count and makes loop-1 pass on a real variance. It goes
-- green exactly when it should not. checks/loop1_reconciliation.py therefore fails if a
-- manifest supersedes more than it quarantined, and checks/supersede_quarantine.py fails
-- if a record's digest matches no row in the twin it names.
--
-- reject_digest is a digest over the row's declared non-system columns, with hash keys
-- rendered as lowercase hex. See src/accelerator/reject_digest.py for why hex, and why
-- the payload alone is not the right column set.
--
-- rulebook_version records what hashing.RULEBOOK_VERSION was when the digest was
-- computed. A bump changes the normalisation and stops outstanding digests matching --
-- acceptable, because a bump already re-keys the estate, but it must be DETECTABLE rather
-- than silently non-matching.
CREATE TABLE IF NOT EXISTS `${catalog}`.`${control_schema}`.ctl_quarantine_superseded (
  manifest_id       STRING     NOT NULL COMMENT 'the manifest the reject belonged to',
  table_name        STRING     NOT NULL COMMENT 'the quarantine twin that held it',
  reject_digest     STRING     NOT NULL COMMENT 'digest over declared non-system columns',
  rulebook_version  STRING     NOT NULL COMMENT 'hashing.RULEBOOK_VERSION at compute time',
  superseded_by     STRING     NOT NULL COMMENT 'job_run_id that landed the same content',
  reason            STRING              COMMENT 'why it was accepted this time',
  recorded_at       TIMESTAMP  NOT NULL COMMENT 'when the supersede was recorded'
)
CLUSTER BY (manifest_id)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'hfig.control_object' = 'true');
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`. The pre-existing statement-count check will now expect **8**, not 7 — update it in the same commit and say so in the message; it is the guard that the splitter still produces whole statements.

- [ ] **Step 5: Prove the new checks can fail**

```bash
cp governance/control_objects.sql /tmp/co.bak
sed -i "s/'delta.appendOnly' = 'true', 'hfig.control_object' = 'true');/'hfig.control_object' = 'true');/" governance/control_objects.sql
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "supersede table is append-only"
# Expected: FAIL
cp /tmp/co.bak governance/control_objects.sql
```

- [ ] **Step 6: Commit**

```bash
git add governance/control_objects.sql tests/test_accelerator.py
git commit -m "Add the supersede table, append-only because it can disarm a hard gate"
```

---

### Task 2: The reject digest's column set

**Files:**
- Create: `src/accelerator/reject_digest.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `factory._projection(entity, src)`, `naming.SYSTEM_COLUMNS`, `hashing.hashdiff`, `hashing.RULEBOOK_VERSION`.
- Produces:
  - `digest_columns(entity, src) -> list[str]` — the ordered declared non-system columns.
  - `hex_columns(columns) -> list[str]` — those needing hex rendering (the hash keys).
  - `digest_sql(entity, src) -> str` — the SQL expression, built by calling `hashing.hashdiff` on the rendered names.

- [ ] **Step 1: Write the failing test**

```python
from accelerator import reject_digest as _rd  # add near the other accelerator imports

_rd_nhl = _am.get("general_journal_line")
_rd_src = _rd_nhl.sources[0]
_rd_cols = _rd.digest_columns(_rd_nhl, _rd_src)
check("the digest column set excludes every system column",
      not (set(_rd_cols) & set(naming.SYSTEM_COLUMNS)),
      f"load_dts and batch_id NECESSARILY differ between the rejected load and the "
      f"corrected one, so a digest including them could never match -- {_rd_cols}")
check("and is never empty, for any reconcilable entity",
      all(_rd.digest_columns(e, e.sources[0])
          for e in _am.entities if e.kind in ("nhl", "link", "hal")),
      str({e.base_table: _rd.digest_columns(e, e.sources[0])
           for e in _am.entities if e.kind in ("nhl", "link", "hal")
           and not _rd.digest_columns(e, e.sources[0])}))

# lnk_client_job_request has ZERO payload and ZERO transaction key -- it is pure
# structure. hashing.hashdiff() raises RulebookError on an empty column list, so a
# payload-only column set would raise for this one entity and no other.
_rd_link = _am.get("client_job_request")
check("the pure-structure link still has a digest column set",
      _rd.digest_columns(_rd_link, _rd_link.sources[0]),
      "lnk_client_job_request has no payload and no transaction key; its content IS its "
      "parent hash keys, and a payload-only rule would raise RulebookError here alone")
check("and that set is nothing but hash keys",
      all(c.endswith("_hk")
          for c in _rd.digest_columns(_rd_link, _rd_link.sources[0])),
      str(_rd.digest_columns(_rd_link, _rd_link.sources[0])))
check("hash-key columns are marked for hex rendering",
      _rd.hex_columns(_rd.digest_columns(_rd_link, _rd_link.sources[0]))
      == _rd.digest_columns(_rd_link, _rd_link.sources[0]),
      "keys are BINARY(32) under the RATIFIED BINARY_OUTPUT, and the ratified "
      "normalisation is defined over strings -- for this link they are the ONLY digest "
      "input, so ambiguous binary rendering would be undefined here and nowhere else")
check("a non-key column is NOT marked for hex rendering",
      "debitamt" not in _rd.hex_columns(_rd_cols),
      "hex on a decimal would change the digest for no reason")
check("the digest SQL is built by CALLING hashing.hashdiff, not reimplementing it",
      "hashdiff(" in (ROOT / "src" / "accelerator" / "reject_digest.py")
      .read_text(encoding="utf-8"),
      "a second digest implementation is the duplicate-definition trap this repo has "
      "been bitten by twice")
check("and reject_digest.py does not modify the ratified rulebook",
      "RULEBOOK_VERSION =" not in (ROOT / "src" / "accelerator" / "reject_digest.py")
      .read_text(encoding="utf-8"),
      "hashing.py is RATIFIED; this module may call it and must not redefine it")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -iE "digest column|pure-structure|hex render"`
Expected: FAIL — the module does not exist. Create it with a stub first if the suite aborts on the import.

- [ ] **Step 3: Write the module**

```python
"""Which columns constitute a rejected row's content, and how to digest them.

Separate from audit.py deliberately: that renders INSERT statements for the load audit,
this decides what a row's content IS. One file, one reason to change.

THE COLUMN SET IS NOT THE PAYLOAD. Measured against the model, lnk_client_job_request has
zero payload and zero transaction key -- a link is pure structure, its content IS its
parent hash keys -- and hashing.hashdiff() raises RulebookError on an empty column list. A
payload-only rule would therefore raise for that one entity and no other, which is the
worst shape of defect: correct on five, undefined on the sixth.

So the set is every DECLARED column of the projected row except the system columns.
load_dts and batch_id necessarily differ between the rejected load and the corrected one
-- load_dts is when WE learned it -- so a digest including them could never match.

HASH KEYS ARE DIGESTED AS LOWERCASE HEX. BINARY_OUTPUT is RATIFIED True, so every hash key
is stored BINARY(32), while hashing.normalise() is defined over strings. A BINARY column's
behaviour inside a string concatenation is not a property to rely on for a comparison that
must be exact. For the pure-structure link the keys are the ONLY digest input, so this is
load-bearing there and merely tidy everywhere else.

This module CALLS hashing.hashdiff() and never reimplements it. hashing.py is a RATIFIED
rulebook: a change there re-keys the estate.
"""

from __future__ import annotations

from collections.abc import Sequence

from . import naming
from .hashing import RULEBOOK_VERSION, hashdiff

__all__ = ["digest_columns", "hex_columns", "digest_sql", "RULEBOOK_VERSION"]


def digest_columns(entity, src) -> list[str]:
    """The projected row's declared columns, in order, minus the system columns."""
    from .factory import _projection

    system = set(naming.SYSTEM_COLUMNS)
    return [c for c, _kind, _value in _projection(entity, src) if c not in system]


def hex_columns(columns: Sequence[str]) -> list[str]:
    """Those of `columns` that are hash keys, and so must be rendered as hex first.

    Keyed on the `_hk` suffix, which naming.hk() guarantees for every hash key in the
    model. A column that merely ends that way and is not BINARY would be hex-rendered
    harmlessly -- hex() of a string is defined -- so the failure mode of a false positive
    here is a stable digest, not a wrong one.
    """
    return [c for c in columns if c.endswith("_hk")]


def digest_sql(entity, src) -> str:
    """The SQL expression digesting one row's content.

    Returns an expression over columns ALREADY hex-rendered under their own names. The
    caller is responsible for that rendering -- see checks/supersede_quarantine.py -- so
    that hashdiff() is called with plain column names and never with an injected
    expression. Passing `hex(x_hk)` into a ratified function as though it were a column
    name would make the rulebook's normalisation operate on something it never saw.
    """
    columns = digest_columns(entity, src)
    if not columns:
        raise ValueError(
            f"{entity.base_table}: no declared non-system column to digest. Every "
            f"reconcilable entity has at least its parent hash keys, so this means the "
            f"projection returned nothing -- a model or projection defect, not a "
            f"digestable row."
        )
    return hashdiff(columns)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3`
Expected: `ALL CHECKS PASSED`

- [ ] **Step 5: Prove the new checks can fail**

```bash
cp src/accelerator/reject_digest.py /tmp/rd.bak
# make it payload-only: the pure-structure link must then have an empty set
python3 - <<'EOF'
from pathlib import Path
p = Path("src/accelerator/reject_digest.py"); t = p.read_text()
p.write_text(t.replace(
    "    return [c for c, _kind, _value in _projection(entity, src) if c not in system]",
    "    return list(entity.payload or ())"))
EOF
uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "pure-structure|never empty"
# Expected: both FAIL
cp /tmp/rd.bak src/accelerator/reject_digest.py
# and drop the hex marking
sed -i 's/    return \[c for c in columns if c.endswith("_hk")\]/    return []/' src/accelerator/reject_digest.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "marked for hex"
# Expected: FAIL
cp /tmp/rd.bak src/accelerator/reject_digest.py
```

- [ ] **Step 6: Commit**

```bash
git add src/accelerator/reject_digest.py tests/test_accelerator.py
git commit -m "Define a rejected row's content: declared non-system columns, keys as hex"
```

---

### Task 3: The detection task

**Files:**
- Create: `checks/supersede_quarantine.py`
- Modify: `resources/vault_job.yml`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `reject_digest.digest_columns`, `reject_digest.hex_columns`, `reject_digest.digest_sql`, `reject_digest.RULEBOOK_VERSION`, `loop1_reconciliation.RECONCILABLE_KINDS`.
- Produces:
  - `shortfall(landed_digests, quarantined_digests, already_recorded) -> dict[str, int]` — pure.
  - `orphaned_records(recorded_digests, quarantined_digests) -> list[str]` — pure.
  - CLI `--catalog --schema --control-schema --job-run-id --active-sources [--entity] [--dry-run]`.
- Task key `supersede_quarantine`, depending on `raw_vault`, and `reconcile_loop1` must depend on it.

**Why it runs after `raw_vault` and not inside it.** `nhl` and `link` are not in `STAGED_KINDS`, so no batch loader touches them — they are written directly by the pipeline through `append_flow`. A streaming flow cannot read its own target, which is why `load_hubs` and `load_satellites` exist as batch tasks at all (DEF-42, DEF-52). The comparison needs both the landed rows and the twin's rows to exist, and after the pipeline both do.

**Why it must be idempotent, and how.** The task runs on every job run and the landed rows stay landed, so a naive implementation would emit another supersede record for the same reject on every run. The subtraction would grow until loop-1 went red the other way, reporting more superseded than quarantined. So `shortfall()` counts what is already recorded for a `(manifest_id, table_name, reject_digest)` and returns only the difference.

- [ ] **Step 1: Write the failing test**

```python
_sq_spec = _ilu2.spec_from_file_location(
    "sq", ROOT / "checks" / "supersede_quarantine.py")
_sq = _ilu2.module_from_spec(_sq_spec); _sq_spec.loader.exec_module(_sq)

# shortfall(): how many NEW supersede records to write per digest.
check("a digest in both landed and quarantined, with nothing recorded, yields one",
      _sq.shortfall({"d1": 1}, {"d1": 1}, {}) == {"d1": 1},
      "the reject was superseded and nothing has said so yet")
check("a digest only in landed yields nothing",
      _sq.shortfall({"d1": 1}, {}, {}) == {},
      "a row that landed and was never rejected supersedes nothing")
check("a digest only in quarantined yields nothing",
      _sq.shortfall({}, {"d1": 1}, {}) == {},
      "a reject that never landed is a genuine reject and must keep counting")
check("an already-recorded supersede yields nothing -- IDEMPOTENCY",
      _sq.shortfall({"d1": 1}, {"d1": 1}, {"d1": 1}) == {},
      "the task runs every job run and the landed rows stay landed, so without this the "
      "subtraction grows until loop-1 reports more superseded than quarantined")
check("two identical rejects both landing yield two, not one",
      _sq.shortfall({"d1": 2}, {"d1": 2}, {}) == {"d1": 2},
      "identical source rows are legitimate and their digests collide, so the mechanism "
      "counts rather than pairing")
check("the shortfall is CAPPED by what was quarantined",
      _sq.shortfall({"d1": 5}, {"d1": 2}, {}) == {"d1": 2},
      "five landed rows cannot supersede two rejects -- an uncapped count would subtract "
      "more than was ever quarantined and make loop-1 pass on a real variance")
check("and capped by the shortfall, not the total",
      _sq.shortfall({"d1": 5}, {"d1": 3}, {"d1": 2}) == {"d1": 1},
      "two already recorded, three quarantined, so exactly one remains to record")

# orphaned_records(): a supersede record that matches no reject at all.
check("a recorded digest present in the twin is not orphaned",
      _sq.orphaned_records({"d1": 1}, {"d1": 1}) == [],
      "it supersedes a real reject")
check("a recorded digest absent from the twin IS orphaned",
      len(_sq.orphaned_records({"d9": 1}, {"d1": 1})) == 1,
      "it subtracts from the quarantined count while superseding nothing, which makes "
      "loop-1 pass on a real variance -- the gate goes green exactly when it should not")
check("more recorded than quarantined IS orphaned",
      len(_sq.orphaned_records({"d1": 3}, {"d1": 1})) == 1,
      "the excess subtracts from a count it cannot account for")
check("and the orphan finding says what it endangers",
      "loop-1" in " ".join(_sq.orphaned_records({"d9": 1}, {"d1": 1})),
      "a finding about a gate-disarming record must say which gate")

# Wiring: the task exists, sits after the pipeline, and loop-1 waits for it.
check("supersede_quarantine runs after raw_vault",
      _depends("supersede_quarantine") == ["raw_vault"],
      "nhl and link are written by the pipeline, so both sides exist only after it")
check("and reconcile_loop1 waits for it",
      "supersede_quarantine" in _depends("reconcile_loop1"),
      "loop-1 subtracts records this task writes, so reconciling first would subtract "
      "an incomplete set and report a false variance")
check("it is handed the run id, so a record names what superseded it",
      _param_value("supersede_quarantine", "--job-run-id") == "{{job.run_id}}",
      str(_params("supersede_quarantine")))
check("and the control schema, from the variable",
      _param_value("supersede_quarantine", "--control-schema")
      == "${var.control_schema}",
      str(_params("supersede_quarantine")))
check("the task ends with the DEF-14 block, not raise SystemExit",
      "if _rc:" in (ROOT / "checks" / "supersede_quarantine.py")
      .read_text(encoding="utf-8")
      and "raise SystemExit" not in (ROOT / "checks" / "supersede_quarantine.py")
      .read_text(encoding="utf-8").split("if __name__")[-1],
      "serverless surfaces SystemExit as a task failure even for exit code 0, so a "
      "PASSING task would fail and block reconcile_loop1 behind it")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -iE "shortfall|orphan|supersede_quarantine"`
Expected: FAIL — the module and the task do not exist.

- [ ] **Step 3: Write the two pure predicates and the task**

```python
"""Record the rejects that a later run accepted, so loop-1 can stop counting them twice.

WHY THIS IS A BATCH TASK AND NOT PART OF A LOADER. nhl and link are not in
naming.STAGED_KINDS, so no batch loader touches them -- they are written directly by the
raw_vault pipeline through append_flow. A streaming flow cannot read its own target, which
is why load_hubs and load_satellites exist as batch tasks at all (DEF-42, DEF-52). The
comparison here needs both the landed rows and the twin's rows to exist, and after the
pipeline both do.

WHAT THIS CAN BREAK IF IT IS WRONG. loop-1's identity becomes
landed + (quarantined - superseded) = approved. A spurious record REDUCES the quarantined
count and makes the gate pass on a real variance -- green exactly when it should not be. So
this task also asserts the negative: every record already on file must match a row still in
the twin it names, and no digest may be superseded more times than it was quarantined.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import naming, reject_digest, spec  # noqa: E402

GATE = "supersede_quarantine"
RECONCILABLE_KINDS = frozenset({"nhl", "link", "hal"})


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def shortfall(landed: dict, quarantined: dict, recorded: dict) -> dict:
    """How many NEW supersede records to write, per digest.

    Pure. `landed`, `quarantined` and `recorded` are {digest: count}.

    CAPPED TWICE, and both caps are load-bearing. Capped by `quarantined` because five
    landed rows cannot supersede two rejects -- an uncapped count would subtract more than
    was ever quarantined. Capped by what is already `recorded` because this task runs on
    every job run while the landed rows stay landed, so an uncapped count would grow the
    subtraction on every run until loop-1 reported more superseded than quarantined.
    """
    out = {}
    for digest, q_count in sorted(quarantined.items()):
        if digest not in landed:
            continue
        want = min(landed[digest], q_count)
        have = recorded.get(digest, 0)
        if want > have:
            out[digest] = want - have
    return out


def orphaned_records(recorded: dict, quarantined: dict) -> list[str]:
    """Supersede records that account for no reject. Pure.

    A record whose digest is absent from the twin, or whose count exceeds what the twin
    holds, subtracts from loop-1's quarantined count while superseding nothing. That makes
    a hard gate pass on a real variance, so it is a failure and not an observation.
    """
    problems = []
    for digest, r_count in sorted(recorded.items()):
        q_count = quarantined.get(digest, 0)
        if r_count > q_count:
            problems.append(
                f"digest {digest}: {r_count} supersede record(s) on file but the "
                f"quarantine twin holds {q_count} matching row(s). The excess subtracts "
                f"from loop-1's quarantined count while superseding nothing, which makes "
                f"loop-1 pass on a real variance -- green exactly when it should not be."
            )
    return problems
```

The Spark half of `main()`: for each reconcilable, active entity, build the digest on both
sides by first rendering hash keys to hex under their own names, then applying
`reject_digest.digest_sql`. Rendering first is deliberate — it keeps `hashdiff()` called
with plain column names rather than an injected `hex(...)` expression, so the ratified
normalisation operates on exactly the columns it names.

```python
def digest_counts(spark, fq: str, entity, src) -> dict:
    """{digest: count} for one table, hash keys hex-rendered before digesting."""
    cols = reject_digest.digest_columns(entity, src)
    hexed = set(reject_digest.hex_columns(cols))
    select = ", ".join(
        f"lower(hex(`{c}`)) AS `{c}`" if c in hexed else f"`{c}`" for c in cols)
    expr = reject_digest.digest_sql(entity, src)
    rows = spark.sql(
        f"SELECT d AS digest, COUNT(*) AS n FROM ("
        f"  SELECT {expr} AS d FROM (SELECT {select} FROM {fq})"
        f") GROUP BY d").collect()
    return {r.asDict()["digest"]: r.asDict()["n"] for r in rows}
```

`main()` then, per entity: compute `digest_counts` for the landed table and the twin, read
the recorded counts from `control.ctl_quarantine_superseded` filtered to that
`table_name`, call `orphaned_records` (any finding fails the task), call `shortfall`, and
INSERT one row per shortfall unit with `rulebook_version =
reject_digest.RULEBOOK_VERSION` and `superseded_by = args.job_run_id`. End with the DEF-14
block.

- [ ] **Step 4: Add the task and re-point loop-1**

In `resources/vault_job.yml`, after `raw_vault`, and change `reconcile_loop1`'s
`depends_on` to include `supersede_quarantine`:

```yaml
        # Rejects a later run accepted. Runs AFTER the pipeline because nhl and link are
        # not staged kinds -- the pipeline writes them directly through append_flow and no
        # loader touches them, so both sides of the comparison exist only once it is done.
        #
        # reconcile_loop1 depends on THIS, not the reverse: loop-1 subtracts the records
        # this task writes, so reconciling first would subtract an incomplete set and
        # report a false variance.
        - task_key: supersede_quarantine
          depends_on: [{task_key: raw_vault}]
          spark_python_task:
            python_file: ../checks/supersede_quarantine.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--schema"
              - "${var.vault_schema}"
              - "--control-schema"
              - "${var.control_schema}"
              - "--job-run-id"
              - "{{job.run_id}}"
              - "--active-sources"
              - "${var.active_sources}"
          environment_key: checks
```

- [ ] **Step 5: Run both suites**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3 && uv run --frozen python verify_repo.py 2>&1 | tail -3`
Expected: both pass. If a PRE-EXISTING check on `reconcile_loop1`'s `depends_on` breaks, make it position-independent and say so in the report — appending to that file has already broken a positional check once in this project.

- [ ] **Step 6: Prove the predicates can fail**

```bash
cp checks/supersede_quarantine.py /tmp/sq.bak
sed -i 's/        want = min(landed\[digest\], q_count)/        want = landed[digest]/' checks/supersede_quarantine.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "CAPPED by what was quarantined"
# Expected: FAIL
cp /tmp/sq.bak checks/supersede_quarantine.py
sed -i 's/        have = recorded.get(digest, 0)/        have = 0/' checks/supersede_quarantine.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "IDEMPOTENCY"
# Expected: FAIL
cp /tmp/sq.bak checks/supersede_quarantine.py
sed -i 's/        if r_count > q_count:/        if False:/' checks/supersede_quarantine.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep -E "IS orphaned"
# Expected: both FAIL
cp /tmp/sq.bak checks/supersede_quarantine.py
```

- [ ] **Step 7: Commit**

```bash
git add checks/supersede_quarantine.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Record superseded rejects from a batch task, capped twice and idempotent"
```

---

### Task 4: Loop-1 subtracts, and refuses to over-subtract

**Files:**
- Modify: `checks/loop1_reconciliation.py`
- Modify: `resources/vault_job.yml` (`reconcile_loop1` gains `--control-schema`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `control.ctl_quarantine_superseded`.
- Produces: `over_subtracted(rows) -> list[str]`, pure, where `rows` is `(manifest_id, quarantined, superseded)`.

- [ ] **Step 1: Write the failing test**

```python
check("superseded within quarantined is not a finding",
      _l1.over_subtracted([("m1", 5, 2)]) == [],
      "two of five rejects were later accepted")
check("superseded EXCEEDING quarantined IS a finding",
      len(_l1.over_subtracted([("m1", 2, 5)])) == 1,
      "subtracting more than was quarantined drives the identity negative and makes "
      "loop-1 pass on a real variance")
check("equal is not a finding",
      _l1.over_subtracted([("m1", 3, 3)]) == [],
      "every reject was later accepted, which is legitimate")
check("and the finding names the manifest and both counts",
      all(x in " ".join(_l1.over_subtracted([("m1", 2, 5)]))
          for x in ("m1", "2", "5")),
      "a variance finding that does not say which manifest cannot be chased")
_l1_src = (ROOT / "checks" / "loop1_reconciliation.py").read_text(encoding="utf-8")
check("the variance query subtracts the superseded term",
      "superseded" in _l1_src and "- COALESCE(s.n, 0)" in _l1_src,
      "without the subtraction the whole mechanism is inert and loop-1 still double-counts")
check("reconcile_loop1 is handed the control schema",
      _param_value("reconcile_loop1", "--control-schema") == "${var.control_schema}",
      str(_params("reconcile_loop1")))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen python tests/test_accelerator.py 2>&1 | grep -iE "superseded|over-subtract"`
Expected: FAIL.

- [ ] **Step 3: Add the predicate and change the query**

Add above `main()` in `checks/loop1_reconciliation.py`:

```python
def over_subtracted(rows) -> list[str]:
    """Manifests superseding more rejects than they quarantined. Pure.

    The identity is landed + (quarantined - superseded) = approved. If superseded exceeds
    quarantined the parenthesised term goes negative, and the gate can then pass on a real
    variance -- it would go green exactly when it should not. That is a failure of the
    supersede mechanism, not a data variance, so it is reported separately and never
    silently absorbed into the arithmetic.
    """
    problems = []
    for manifest_id, quarantined, superseded in sorted(rows):
        if superseded > quarantined:
            problems.append(
                f"manifest {manifest_id}: {superseded} superseded record(s) against "
                f"{quarantined} quarantined row(s). The identity's quarantined term goes "
                f"negative, so this gate could pass on a real variance. Investigate "
                f"checks/supersede_quarantine.py before trusting any loop-1 result."
            )
    return problems
```

Then extend the variance query — add the CTE and the subtraction:

```sql
                superseded AS (
                    SELECT manifest_id, COUNT(*) AS n
                    FROM {superseded_table}
                    WHERE table_name = '{qtn_name}'
                    GROUP BY manifest_id
                )
```

with `SELECT ... COALESCE(s.n, 0) AS superseded`, a `LEFT JOIN superseded s USING
(manifest_id)`, and the predicate becoming:

```sql
                WHERE COALESCE(l.n, 0) + COALESCE(r.n, 0) - COALESCE(s.n, 0)
                      <> e.approved_count
```

`qtn_name` is derived exactly as line 213 already derives it —
`f"qtn_{table.split('_', 1)[1]}"` — so both sides name the twin identically. Run
`over_subtracted` over a second query grouping quarantined and superseded per manifest, and
fail the task on any finding.

- [ ] **Step 4: Add the parameter**

```yaml
              - "--control-schema"
              - "${var.control_schema}"
```

- [ ] **Step 5: Run both suites, then prove it can fail**

```bash
uv run --frozen python tests/test_accelerator.py 2>&1 | tail -3
uv run --frozen python verify_repo.py 2>&1 | tail -3
cp checks/loop1_reconciliation.py /tmp/l1.bak
sed -i 's/        if superseded > quarantined:/        if False:/' checks/loop1_reconciliation.py
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "EXCEEDING quarantined"
# Expected: FAIL
cp /tmp/l1.bak checks/loop1_reconciliation.py
python3 - <<'EOF'
from pathlib import Path
p = Path("checks/loop1_reconciliation.py"); t = p.read_text()
p.write_text(t.replace("- COALESCE(s.n, 0)", ""))
EOF
uv run --frozen python tests/test_accelerator.py 2>&1 | grep "subtracts the superseded term"
# Expected: FAIL
cp /tmp/l1.bak checks/loop1_reconciliation.py
```

- [ ] **Step 6: Commit**

```bash
git add checks/loop1_reconciliation.py resources/vault_job.yml tests/test_accelerator.py
git commit -m "Loop-1 subtracts superseded rejects, and fails if it would over-subtract"
```

---

### Task 5: The probe — STOP and get approval before running it

**Files:** none. Live workspace only.

**This task does not run under continuous execution.** It manufactures a rejection in a live
lake, which is a deliberate write to production-adjacent data, and it must be approved
explicitly. Present it and stop.

**Why it is not optional.** Every `qtn_` table is at **0 rows**. Everything above is a
mechanism that has never seen a rejection. Shipping it unexercised would repeat the mistake
this project keeps finding: asserting a property nothing had run.

- [ ] **Step 1: Get explicit approval, naming what will be written and how it is undone**

- [ ] **Step 2: Force a rejection**

Add one temporary row to `control.ref_dq_expectation` whose `rule_sql` rejects a small,
known set of rows on `nhl_journal_line` — the smallest reconcilable NHL, 5 payload columns.
Record the exact rule and row count before running.

- [ ] **Step 3: Run the job. Confirm the rows are in `qtn_journal_line`, and that loop-1 is GREEN**

`landed + quarantined = approved` must hold — the reject is accounted for. If loop-1 is red
here, stop: the baseline is wrong and nothing below means anything.

- [ ] **Step 4: Relax the rule to its correct form, and re-run**

Confirm three things: the rows now land; `ctl_quarantine_superseded` gains **exactly one
record per row**; loop-1 is **still green**. That is the mechanism working.

- [ ] **Step 5: THE STEP THAT MATTERS — delete the supersede records and confirm loop-1 goes RED**

Without this, the subtraction could be doing nothing and every observation in Step 4 would
look identical. `ctl_quarantine_superseded` is append-only, so this requires a deliberate,
approved exception — or run it against a scratch copy of the table and point loop-1 at that
with its existing override. **Prefer the scratch copy: it proves the same property without
mutating an append-only control table.**

- [ ] **Step 6: Repeat Steps 2-4 for `lnk_client_job_request`**

The one reconcilable entity whose digest input is nothing but hash keys. An implementation
correct on the NHLs and undefined on the link would pass a probe that only exercised NHLs —
see the spec's §4 on hex rendering.

- [ ] **Step 7: Remove the temporary rule, and record everything in `OPEN_ITEMS.md`**

Row counts, the digests observed, the supersede records written, and the red result from
Step 5. Commit.

---

## Notes for whoever executes this

**The one thing that makes this dangerous.** Every other gate in this repo only ever *adds*
reasons to fail. This one *subtracts* from a hard gate's count. If `supersede_quarantine`
over-records, loop-1 goes green on real variances and nothing else in the estate would
notice. That is why `orphaned_records` and `over_subtracted` exist, why both are pure and
fired offline in both directions, and why Step 5 of the probe is the step that matters. Treat
any weakening of those two predicates as a change to a hard gate, not a refactor.

**What this plan does not build.** It does not detect that a rejection was our fault — a human
decides that, changes the rule and re-runs. It does not populate `ref_dq_expectation` beyond
the probe's temporary row, so in normal operation the only expectations are still the two
compiled-in key-safety rules. And it does nothing for a row that landed and is wrong, which
`append_only_check` forbids correcting and which needs a satellite version or a reversal — a
modelling decision, not a mechanism.
