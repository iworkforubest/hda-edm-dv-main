# Workday Reference Service — connection facts and where the secret goes

**No password is in this repo, and none may be added.** This file records the four
non-secret facts and the one rule about the fifth. It exists because the alternative is
those facts living in a chat message that nobody can find again.

## What we hold

| | value | secret? |
|---|---|---|
| Host | `impl-services1.wd502.myworkday.com` | no |
| Tenant | `headfirst3` | no |
| Web Services version | `v47.0` — newest the tenant serves; moved from `v46.2` on 26 Sep, see below | no |
| Integration system user | `ISU_Databricks@headfirst3` | no — an identity, not a credential |
| Password | **NOT HERE** — see below | **yes** |

## What the ISU may actually call — measured 26 September, and it disagrees with the grant

Augustin (Workday integration consultant) confirmed the endpoint shape and added:
*"So far the security granted would be to use Get_References."*

Both halves were tested against the tenant rather than filed.

| | stated | measured 26 Sep |
|---|---|---|
| Endpoint shape | `…/ccx/service/headfirst3/[Service]/v45.0` | confirmed |
| Service for `Get_References` | `Integrations` | confirmed |
| `Get_References` granted | yes | **works** — 982 rows over 4 types |
| `Get_Organizations` granted | **not mentioned, so presumed not** | **WORKS — 77 pages, ~1,528 organisations** |

### The version: Augustin's example says v45.0, we pin v47.0, and it was measured

That email is old, so the version in it was **tested rather than followed**.

The tenant **validates** the segment rather than ignoring it, which is what makes any
comparison meaningful. Probed 26 September:

| version | tenant |
|---|---|
| `v44.0` `v45.0` `v46.0` `v46.1` `v46.2` `v47.0` | accepted |
| `v46.3` `v48.0` `v49.0` `v1.0` `v99.9` | `SOAP-ENV:Client.validationError \| Invalid request service version` |

So the range is **v44.0 to v47.0**, `v47.0` is the newest served, and `v46.3` was never a
Workday release.

**The pin moved from `v46.2` to `v47.0`.** The parser was built from `Integrations.xsd` at
v46.2, so the question was whether v47.0 returns anything that parser would silently drop.
Comparing *parsed rows* cannot answer that — they come out of a v46.2-shaped parser, which
would hide the very difference being looked for. So the **raw responses** were compared:

| call | rows | element names + counts | raw bytes |
|---|---|---|---|
| `Get_References` × 4 types | same | identical | differ only in the echoed version token |
| `Get_Organizations` (131 KB) | same | identical | differ only in the echoed version token |

Normalising `v46.2`/`v47.0` to a placeholder makes both responses **byte-identical**. In
the 42,492-byte company response the entire difference is two characters at offset 190.

**What that establishes, and what it does not.** It establishes that for the data this
tenant holds today, the two versions are indistinguishable, so moving the pin carries no
measured risk for the calls we make. It does **not** establish that the schemas are
identical: an element that is optional and unpopulated in an implementation tenant looks
the same at both versions and could still differ in production. **The XSD has not been
re-read at v47.0**, and the parser remains v46.2-derived. If a production tenant is richer,
that is where a difference would first appear.

Pinning stays deliberate either way — a version is a frozen contract, and the risk Workday
creates is the floor rising until a pin is retired, not the ceiling moving. `v47.0` simply
buys the most runway, and confirmation of the intended version is part of **WDJ-11**.

### The grant discrepancy is the part that matters

`Get_Organizations` on the `Human_Resources` service answers today, despite the granted
security being described as `Get_References` only. Two readings, and they have opposite
consequences:

* **the grant is broader than stated** — then nothing is blocked, and the description
  needs correcting; or
* **the implementation tenant is permissive and production will not be** — then
  `docs/superpowers/specs/2026-09-24-legal-entity-hierarchies-design.md` has **no source**
  the moment it moves to a production tenant. `hub_legal_entity`,
  `hub_consolidation_group`, `hal_consolidation_hierarchy` and
  `lnk_legal_entity_consolidation` all flow from `--kind organisation` and
  `--kind membership`, and both call `Get_Organizations`.

**A call working in impl is not evidence that it is granted.** It is evidence that impl
does not refuse it. The question for Augustin is narrow: *is `Get_Organizations` on
`Human_Resources` granted to `ISU_Databricks`, in impl and in production — or does it
answer in impl only because that tenant is permissive?* Asked as **WDJ-11**.

## Where the password goes, and why not here

**Azure Key Vault**, and **only the platform team can create it** — so this is a
dependency rather than a step. Raised as **PLT-7** in
`docs/platform_team_requests.html`.

Two corrections to what this file said first, both worth keeping because both were wrong in
a way that would have wasted somebody's afternoon:

* **`databricks secrets put-secret` is not the route.** A Key Vault-backed scope is
  **read-only through the Databricks secrets API**; secrets are created in the vault
  itself. Writing through Databricks only works for a Databricks-backed scope, which is not
  what this estate uses.
* **We cannot create the scope either.** A Key Vault-backed scope needs the vault's
  `resource_id`, `tenant_id` and `dns_name`, and the CLI documents that it throws
  `UNAUTHENTICATED if unable to verify user access permission on Azure KeyVault`. Those are
  platform-held facts and platform-held permissions.

**Measured 6 September in `usnc_tds`: there are no secret scopes at all.** Not one, of
either kind. So nothing here is a matter of picking a name in an existing arrangement.

### Until then: a file, with two refusals rather than two warnings

Agreed as the stop-gap. `tools/fetch_workday_references.py --password-file <path>` reads it,
and **refuses** rather than warns in the two cases that matter:

* **The file is readable by group or other** — on a shared machine that is the whole
  exposure. `chmod 600`.
* **The file is inside the repo and git does not ignore it.** That is the one people miss:
  `bundle deploy` uploads the repo *minus ignored paths* into
  `/Workspace/Shared/.bundle`, which grants `CAN_MANAGE` to `users` by inheritance until
  **PLT-3** lands. So an unignored credential file is not merely committable — it is
  published to every workspace user on the next deploy.

**One `.gitignore` line covers both**, because DABs excludes ignored paths from the sync.
`.workday-credentials` and `.workday/` are already there, and `verify_repo` asserts they
stay there and that nothing matching them is tracked — an ignore rule does nothing for a
file already in the index.

```bash
printf 'WD_PASSWORD=…\n' > .workday-credentials && chmod 600 .workday-credentials
python tools/fetch_workday_references.py --password-file .workday-credentials --max-pages 1
```

The file may hold a bare password or a `KEY=VALUE` line, so the same file can be `source`d
into a shell. Everything after the first `=` is the value, because `=` is a legal password
character.

### The file stays until PLT-7 lands. That is a decision, not an oversight.

**Decided 7 September:** `.workday-credentials` remains on the working machine until the
Azure Key Vault secret and its Databricks scope exist. It is not deleted after each use and
it is not re-typed per session.

Recorded because a plaintext credential found on disk should read as **deliberate, with a
named end condition**, rather than as something someone forgot. The end condition is
**PLT-7** — nothing else. When that lands, this file is deleted and `--password-file` stops
being the route.

What makes it acceptable in the meantime is not trust, it is that both ways it could leak
are refused rather than warned about: the reader rejects a file readable by group or other,
and rejects a file inside the repo that git does not ignore — which is also what keeps it
out of the bundle upload into a folder every workspace user can manage. `verify_repo`
asserts the ignore rule survives and that nothing matching it is tracked.

**It is still a credential in a file.** If the machine is shared, backed up, or synced
somewhere, that is the exposure and no check here can see it.

**This is a stop-gap and the code says so.** It is not a pattern to reuse for the next
credential; PLT-7 is.

### What this does and does not block

**It does not block trying the service today.** `tools/fetch_workday_references.py` reads
`WD_PASSWORD` from the environment, so a person who holds the password can run it from
their own shell right now:

```bash
read -rs WD_PASSWORD && export WD_PASSWORD     # never as a flag: flags reach `ps` and history
python tools/fetch_workday_references.py --max-pages 1 --out /tmp/refs.json
```

**It blocks anything automated.** The moment a pipeline, a job or a deployed notebook needs
that credential, it has to come from a scope, because the alternative is a secret in a file
the bundle ships — and the bundle root grants `CAN_MANAGE` to `users` by inheritance until
**PLT-3** lands. `tests/test_accelerator.py` carries a default-deny embedded-credentials
gate over exactly those files, with an empty allowlist, so committing one fails the build.
That is the intended outcome, not a reason to look for a quieter place.

Once the scope exists, reading is unchanged: `dbutils.secrets.get(scope, key)`.

## The tenant is an implementation tenant

`impl-services1` is not production. Build and prove the integration against it, and do not
promote what it returns:

* **WIDs are tenant-specific.** A WID retrieved here identifies nothing in production.
  Anything persisted keyed on one is wrong the day it is used for real.
* **Configured reference IDs usually migrate**, and "usually" is not a guarantee. Whether
  the production tenant carries the same `Company_Reference_ID` and `Ledger_Account_ID`
  values is a question for the Workday team — asked as **WDJ-5**.

## What the schema says, and how I got it wrong the first time

**A call names exactly one reference ID type, and must.** `Reference_ID_Type` is
`minOccurs="1"` inside `Get_References_Request_Criteria`. There is no "all types" call.

**I wrote the opposite here first**, and the tenant corrected it on the first authenticated
request:

```
SOAP-ENV:Client.validationError | Element Content 'Reference_ID_Type' is required,
on internal element 'Get References Request Criteria'
```

The cause is worth keeping. I listed the schema's elements with a pattern matching
`<xsd:element name="..." type="...">`. `Reference_ID_Type` declares an inline
`<xsd:simpleType>` and carries no `type` attribute, so the pattern skipped it — and I
recorded its **absence** as a finding. A search that silently drops what it cannot match,
reported as a fact about the thing searched, is this repo's own recurring defect. The
second reading used an XML parser.

**A page holds at most 999 rows.** `Count` is `totalDigits="3"`. That one was right, and
was re-verified with the parser rather than left on the regex's word.

The response shape, for anyone reading the parser:

```
Response_Results   Total_Results  Total_Pages  Page_Results  Page
Response_Data
  Reference_ID @Descriptor
    Reference_ID_Reference   ID @type=WID | @type=<a reference id type>
    Reference_ID_Data        ID  New_ID  Reference_ID_Type  Referenced_Object_Descriptor
```

`Reference_ID_Data/ID` is the value a journal must quote. `Reference_ID_Reference/ID` is
the index entry, including the WID — returned because it is in the response, not because it
is safe to keep.

## What this unblocks

`Get_References` takes a reference ID *type* and returns the actual *values* for it. That
is the source for `ref_workday_reference_type` (the ~160-member worktag enumeration — the
XSD gives the type names only, so a value checked against the XSD is checked against
nothing), for the Tax Detail `…TypeReferenceID` values, and possibly for **WDJ-2**'s return
path if journals prove retrievable by the external reference id we supply.

## What is built, and what is not

`src/accelerator/workday.py` builds the request and parses the response;
`tools/fetch_workday_references.py` is the only thing here that opens a connection.
`--dry-run` prints the endpoint and the request body and needs no credential, because the
body cannot contain one.

**It has never been run against a tenant.** The request is the schema's shape and the
parser is exercised against fixtures built from that schema, which demonstrates that we
build what the schema describes — not that Workday accepts it. Seven offline checks cover
it, each proven to fail; the one that matters most asserts that **a SOAP fault raises
rather than parsing as an empty result**, because Workday answers a rejected request with
a well-formed envelope and a parser that goes straight for `Response_Data` turns a bad
credential into a silent load of nothing.

**It writes no `ref_` table.** `ref_` is platform-owned (`naming.PLATFORM_OWNED`) and this
accelerator reads such objects by join and never creates them. The tool retrieves to a file
and stops; how that becomes `ref_workday_reference_type` is the platform team's call.
