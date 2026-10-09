"""
THE STABLE VIEW RULE. One implementation, three callers.

A physical vault table carries a version suffix (`naming.physical()`, `_rev<N>`);
consumers read the UNVERSIONED name instead -- a plain view a cutover repoints. Whoever
builds a table for the first time in a lake must BOOTSTRAP that view, and must never
move it once it exists: an unconditional CREATE OR REPLACE on every run would cut
consumers over automatically the moment `version:` changed in the model, bypassing
checks/cutover_vault_version.py's gating, its empty-target refusal and its
--gated-by-run -- and it would silently UNDO a deliberate rollback on the very next
scheduled run.

MEASURED IDENTICAL BEFORE THIS MODULE EXISTED. checks/load_hubs.py and
checks/load_satellites.py each carried their own `stable_view_action()` and
`stable_view_sql()`; comparing their ASTs with docstrings stripped showed the logic was
byte-identical, the only difference being a parameter name (`hub` vs `sat`). They had
not drifted yet, but a third copy -- this task's `checks/publish_stable_views.py` -- is
how that starts. One rule, three readers, so a pipeline-owned table and a batch-loaded
one cannot drift apart in how their stable view is decided or rendered.

THE THREE CASES ARE THE WHOLE SAFETY RULE:
  view absent           -> create it pointing at the table just built (bootstrap)
  view points here       -> leave it (reissuing an identical statement is harmless)
  view points ELSEWHERE -> LEAVE IT, and the caller reports both names

THE LOADER BOOTSTRAPS, IT NEVER MOVES -- and neither does any other caller of this
module, checks/publish_stable_views.py included. Only checks/cutover_vault_version.py
repoints a stable view that already exists and points somewhere else -- that state is
a deliberate cutover or a deliberate rollback, made there, deliberately, with its own
gating.
"""

from __future__ import annotations


def stable_view_sql(catalog: str, schema: str, stable: str, physical: str) -> str:
    """THE VIEW IS THE CONSUMER-FACING NAME -- the unversioned name naming.stable()
    inverts the physical one to.

    Every caller publishes this AFTER the physical table is built and loaded --
    publishing it first would expose an empty or partial table to every reader for the
    length of the load.

    ONLY EVER ISSUED WHEN stable_view_action() SAYS "bootstrap" -- when the view does
    not exist yet. If the view exists and points somewhere else, the caller must not
    call this at all: that state is a deliberate cutover or a deliberate rollback and
    belongs to checks/cutover_vault_version.py alone.

    AND NOT WHEN IT SAYS "already_here" EITHER, WHICH IS NOT THE SAME AS HARMLESS.
    CREATE OR REPLACE VIEW does not edit a view in place: Unity Catalog replaces the
    securable, and every grant held against the old one goes with it. Reissuing an
    identical statement therefore REVOKES the view's SELECT grants while changing
    nothing a reader can see -- silent, total, and repaired only by re-granting.
    MEASURED 27 September 2026: grants applied to all 19 stable views at 00:40 CEST
    were gone after the 00:53-01:22 load, while the `_rev1` TABLES beneath them kept
    theirs (untouched, so never replaced). information_schema.tables put
    nhl_invoice_line's last_altered at 23:14:09 UTC -- inside the load window, and that
    view was reported "already_here", so the no-op branch is what rewrote it.
    """
    return (
        f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{stable}` "
        f"AS SELECT * FROM `{catalog}`.`{schema}`.`{physical}`"
    )


def stable_view_action(view_exists: bool, view_definition: str, target: str) -> str:
    """Whether the stable view should be BOOTSTRAPPED, LEFT ALONE, or is ELSEWHERE. PURE.

    Decided from information_schema.views.view_definition -- the same evidence
    checks/retire_vault_version.py's is_live check reads, so deciding this needs no new
    state.

      "bootstrap"     the view does not exist. The caller creates it pointing at the
                       table it just built. This is bootstrap, and it is correct.
      "already_here"  the view exists and already reads FROM target. Nothing to do,
                       and the caller MUST DO NOTHING -- reissuing an identical
                       CREATE OR REPLACE replaces the securable and silently drops
                       every grant on it. This said "harmless" until 27 September
                       2026, and on that claim three callers re-issued it every run;
                       see stable_view_sql()'s docstring for the measurement.
      "elsewhere"     the view exists and reads from something else. The caller MUST
                       NOT touch it: that state is a deliberate cutover or a deliberate
                       rollback, made by checks/cutover_vault_version.py, and undoing it
                       silently is exactly the defect this function exists to close.
    """
    if not view_exists:
        return "bootstrap"
    if target in (view_definition or ""):
        return "already_here"
    return "elsewhere"
