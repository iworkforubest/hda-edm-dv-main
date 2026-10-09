# Chase: the service principal's name (PLT-1, PLT-4, and Platform's own deploy route)

**Status: SENT to Michael, Platform, 7 September.** Went out the same day it was drafted, so
the measurement it quotes was current when it was read. It chases one input from
`docs/platform_team_requests.html`. Kept after sending because the reasoning below is the
record of *how* we chased — the next chase on this queue should read it before repeating or
escalating.

**Why it asks for one thing.** Five of the seven asks on that queue are unanswered. A chase
that re-lists all five is a re-send, not a chase, and gets read as one. This asks for the
single input that three separate things now wait on — and one of those three is Platform's
own preferred approach, which is the strongest version of the argument available.

**Why it deliberately does not ask for a time.** Corrected during drafting. Nothing on
`usnc_tds` runs on a timer and every load is started by hand, so there is no window to
coordinate: Platform transfers whenever, we redeploy `run_as` before the next load, and the
wrong order produces a permission error that is harmless here and announces itself. Asking a
platform team to schedule a change window for a test workspace spends goodwill on ceremony.
**In production the window is genuinely required** — the run-as identity holds production
catalog rights and a scheduled load can land in the gap. The note says so explicitly so the
concession reads as scoped rather than as a standard we do not hold.

**What was deliberately left out**, and should stay out unless someone decides otherwise:
the `assert_freshness` / `assert_no_broad_grant` first-run promise (it belongs with PLT-2),
and Gold access (PLT-5 is the largest single unblock, but folding it in weakens the chase).

**If this needs chasing again, re-measure first.** The reading below was taken and sent on
7 September; PLT-1's argument does not survive a schedule being added, so a second chase
quoting the same numbers would be quoting an unverified claim.

```bash
databricks jobs get 303337354608483 --profile hfig-usnc-tds --output json
databricks bundle validate --target usnc_tds
```

---

## The note

**Subject: One name — the service principal for the vault schemas**

Michael, chasing a single input from the queue I sent on 6 September: **the name of the
service principal that should own the four vault schemas.** Just the name — no change
window needed, see below.

It's worth chasing now because it stopped being only our blocker. Three things wait on it:

1. **PLT-1** — transferring ownership of the four vault schemas, and setting the job's
   `run_as` to the same principal. Our side is a one-line change.
2. **PLT-4** — the read-grant allowlist. Our `assert_no_broad_grant` gate fails the build on
   any table-level read grant held by a principal it doesn't know, and that includes the
   owning principal. We need the name for the gate whether or not the grant is wired yet.
3. **Your own answer on the deployment folder.** Someone replied that bundles are released
   to the service principal's workspace directly, which normal users can't reach. We'd
   rather do that than have a folder created for us — it takes my laptop out of the
   deployment path entirely. But we can't point the bundle there without the principal's
   name and its workspace path. `bundle_root_prefix` is already a variable on our side, so
   it's one line once we have it.

Until then what we deploy still lands in
`/Workspace/Shared/.bundle/hfig-dv-accelerator/usnc_tds`, and `databricks bundle validate`
still warns it's writable by all workspace users.

**You don't need to coordinate a time with us for this one.** Nothing on `usnc_tds` runs on
a timer — re-measured this morning against the deployed job:

```
[usnc_tds] hfig vault load
schedule: ABSENT   trigger: ABSENT   continuous: ABSENT   edit_mode: UI_LOCKED
run_as_user_name: adrian.turcu@headfirst.group
```

Every load here is started by hand, so transfer whenever suits you and we'll redeploy
`run_as` before the next one. If we get the order wrong the load fails with a permission
error, which is harmless in a test workspace and tells us immediately. **Production is
different and I'll ask properly then** — there the run-as identity holds production catalog
rights and a scheduled load could land in the gap, so that one will need a real window.

**So: just a name.** The redeploy will be ready.

One related note: the reply I got covered three items from an older summary of mine. The
current list is the seven in `platform_team_requests.html` I sent you on 6 September —
PLT-1, 4, 5, 6 and 7 are still open, and PLT-2 needs one group membership added that the
reply said was already in place.
