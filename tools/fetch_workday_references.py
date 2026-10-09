#!/usr/bin/env python3
"""Retrieve Workday reference values through Get_References and write them to a file.

THE ONLY THING IN THIS REPO THAT TALKS TO WORKDAY, and it is a tool rather than a job task
on purpose. checks/ is exec()'d by a serverless spark_python_task with no `__file__`
(DEF-12) and runs inside a load; this needs a filesystem, an outbound connection and a
credential, and it is not part of any load.

IT WRITES NO ref_ TABLE, and that is the ARB boundary rather than an unfinished feature.
`ref_` is platform-owned -- naming.PLATFORM_OWNED -- and this accelerator reads such
objects by join and never creates them. So this retrieves and stops: whoever owns
ref_workday_reference_type decides how the file becomes a table.

THE PASSWORD COMES FROM THE ENVIRONMENT AND GOES NOWHERE ELSE. Not a flag, because a flag
lands in shell history and in `ps`. Not a default, because a missing credential must fail
loudly. It is read once, handed to accelerator.workday.envelope(), and never logged --
--verbose prints the request body, which is built by a function that cannot contain it.

  export WD_PASSWORD="$(databricks secrets get-secret hfig-workday isu-databricks-password ...)"
  python tools/fetch_workday_references.py --out references.json

Connection facts and the secret-scope rule: docs/workday_reference_service_access.md
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import workday  # noqa: E402

# Defaults are the IMPLEMENTATION tenant, deliberately. If someone runs this with no
# arguments they reach the tenant where a wrong answer costs nothing; production has to be
# named on purpose.
DEFAULTS = {
    "host": "impl-services1.wd502.myworkday.com",
    "tenant": "headfirst3",
    # v47.0 IS THE NEWEST THE TENANT SERVES, and the pin moved to it on 26 September on
    # MEASUREMENT rather than preference. The parser was built from Integrations.xsd at
    # v46.2, so the question was whether v47.0 returns anything it would drop. Both calls
    # were made at both versions and the raw responses compared, not just the parsed rows
    # (parsed rows go through a v46.2-shaped parser, which would hide exactly the
    # difference being looked for):
    #
    #   Get_References, all four types   same rows, same element names and counts
    #   Get_Organizations, 131KB         same rows, same element names and counts
    #   raw bytes                        IDENTICAL once the echoed version token is
    #                                    normalised -- the only difference in the whole
    #                                    response is "v46.2" -> "v47.0" in the envelope
    #
    # WHAT THAT DOES AND DOES NOT ESTABLISH. It establishes that for the data THIS tenant
    # holds today, the two versions are indistinguishable. It does NOT establish that the
    # SCHEMAS are identical: an element that is optional and unpopulated in an
    # implementation tenant would look the same at both versions and could still differ in
    # production. The XSD has not been re-read at v47.0.
    "version": "v47.0",
    "username": "ISU_Databricks@headfirst3",
}


def fetch_page(url: str, body: str, username: str, password: str, timeout: int) -> str:
    request = urllib.request.Request(
        url,
        data=workday.envelope(body, username, password).encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": ""},
        method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        # A Workday fault arrives as a 500 with a well-formed envelope. Read it: the fault
        # string says whether this is a credential, a permission or a malformed request,
        # and "HTTP 500" says none of those.
        detail = exc.read().decode("utf-8", errors="replace")
        problem = workday.fault(detail)
        raise SystemExit(f"HTTP {exc.code}: {problem or detail[:400]}") from None


# The keys this file understands. A file may also hold a bare password and no key at
# all, which is what it held before a username was ever written into it.
_CRED_KEYS = ("WD_PASSWORD", "WD_USERNAME")
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def read_credentials_file(path: str | None) -> dict[str, str]:
    """The credentials in a file, as a dict. REFUSES an unsafe file.

    A STOP-GAP, AND THE CHECKS ARE WHAT MAKE IT ONE RATHER THAN A HABIT. Secrets belong in
    the Key Vault-backed scope PLT-7 asks for; until that exists, a file is how a person
    tests this without retyping a password every shell. Two ways that goes wrong, and both
    are refused rather than warned about:

      * THE FILE IS READABLE BY OTHERS. On a shared machine that is the whole exposure.
      * THE FILE IS SOMEWHERE THE BUNDLE SHIPS. `databricks bundle deploy` uploads the
        repo to /Workspace/Shared/.bundle, which grants CAN_MANAGE to `users` by
        inheritance until PLT-3 lands -- so a credential in a tracked path is a credential
        handed to every workspace user. .gitignore covers both the commit and the upload,
        because DABs excludes ignored paths from the sync, so the test is whether git
        ignores it.

    THIS READ KEYS BY POSITION UNTIL 26 SEPTEMBER, AND THAT WAS A BUG THE MOMENT THE FILE
    GREW A SECOND LINE. It returned the value of the FIRST non-comment line, on the
    assumption that a credentials file holds exactly one thing. When WD_USERNAME was added
    above WD_PASSWORD, the username became the password: a 14-character value handed to
    the tenant as a secret, and an authentication fault whose text would have sent the
    reader looking at the password, the ISU and the tenant -- everything except the parser
    that never looked at the key. Keys are now read BY NAME. A file may still hold a bare
    password with no key, because that is what this file held before, and a person who
    writes one line should not have to name it.

    The value may be quoted, so the same file can be `source`d into a shell. Everything
    after the first '=' is the value, because '=' is a legal password character.
    """
    if not path:
        return {}
    p = Path(path).expanduser()
    if not p.is_file():
        raise SystemExit(f"--password-file {p} does not exist")

    mode = p.stat().st_mode
    if mode & 0o077:
        raise SystemExit(
            f"--password-file {p} is readable by group or other (mode "
            f"{oct(mode & 0o777)}). Run: chmod 600 {p}")

    try:
        relative = p.resolve().relative_to(ROOT)
    except ValueError:
        relative = None
    if relative is not None:
        ignored = subprocess.run(["git", "check-ignore", "-q", str(relative)],
                                 cwd=ROOT, capture_output=True).returncode == 0
        if not ignored:
            raise SystemExit(
                f"--password-file {relative} is inside the repo and NOT git-ignored. "
                f"That means it can be committed, and it means `bundle deploy` uploads it "
                f"to a folder every workspace user can read. Move it outside the repo, or "
                f"name it .workday-credentials, which is already ignored")

    found: dict[str, str] = {}
    bare: list[str] = []
    unknown: list[str] = []
    for line in p.read_text(encoding="utf-8").strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = None, line
        if "=" in line:
            left, right = line.split("=", 1)
            # A key only if it LOOKS like one. `hunter2=x` is a legal password, and
            # splitting it would be the positional bug wearing a different hat.
            if _KEY_RE.match(left.strip()):
                key, value = left.strip().upper(), right.strip()
        # A MATCHING PAIR ONLY. `.strip('"')` would eat a quote that is part of the
        # password -- and a generated credential containing " or ' is perfectly legal.
        for quote in ('"', "'"):
            if len(value) >= 2 and value[0] == quote and value[-1] == quote:
                value = value[1:-1]
                break
        if key is None:
            bare.append(value)
        elif key in _CRED_KEYS:
            if key in found:
                raise SystemExit(f"--password-file {p} sets {key} more than once. Which "
                                 f"one is live is not a question a credential should raise")
            found[key] = value
        else:
            # NAMED, AND NOT A NAME THIS KNOWS. Recorded rather than dropped: the
            # realistic cause is a typo in WD_PASSWORD, and "holds no password" about a
            # file that visibly holds one sends the reader to the wrong place entirely.
            unknown.append(key)

    if "WD_PASSWORD" not in found and bare:
        # One unnamed line in a file that names nothing else is the password, as it always
        # was. An unnamed line ALONGSIDE named keys is ambiguous, and is refused below.
        if found:
            raise SystemExit(
                f"--password-file {p} mixes named keys {sorted(found)} with "
                f"{len(bare)} unnamed line(s). Name every line, or name none")
        if len(bare) > 1:
            raise SystemExit(f"--password-file {p} holds {len(bare)} unnamed lines and no "
                             f"WD_PASSWORD. Which one is the password is not a guess worth "
                             f"making")
        found["WD_PASSWORD"] = bare[0]

    if "WD_PASSWORD" not in found:
        if unknown:
            raise SystemExit(
                f"--password-file {p} sets {sorted(unknown)}, which this does not "
                f"recognise, and no WD_PASSWORD. Expected one of {list(_CRED_KEYS)} -- "
                f"check the spelling. (A bare password containing '=' looks like a key "
                f"from here; quote it, or write it as WD_PASSWORD=...)")
        if not found:
            raise SystemExit(f"--password-file {p} holds no password")
    return found


def read_password_file(path: str | None) -> str:
    """The password from a file, or "" if none was asked for. See read_credentials_file."""
    if not path:
        return ""
    creds = read_credentials_file(path)
    if "WD_PASSWORD" not in creds:
        raise SystemExit(
            f"--password-file {path} sets {sorted(creds)} but no WD_PASSWORD. The "
            f"password is the one value this cannot proceed without")
    return creds["WD_PASSWORD"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default=os.environ.get("WD_HOST", DEFAULTS["host"]))
    ap.add_argument("--tenant", default=os.environ.get("WD_TENANT", DEFAULTS["tenant"]))
    ap.add_argument("--version", default=os.environ.get("WD_WWS_VERSION",
                                                        DEFAULTS["version"]))
    # RESOLVED AFTER PARSING, not here, because the credentials file is a fourth source
    # and argparse cannot see it. Precedence: --username, then WD_USERNAME in the
    # environment, then WD_USERNAME in the credentials file, then the impl-tenant default.
    ap.add_argument("--username", default=None)
    ap.add_argument("--type", dest="reference_id_type", required=True,
                    help="the reference id type to retrieve, e.g. Company_Reference_ID. "
                         "REQUIRED, by the schema and by the tenant -- there is no 'all "
                         "types' call, and no default here would be better than an error")
    ap.add_argument("--count", type=int, default=200,
                    help=f"page size, 1..{workday.MAX_COUNT} (a schema restriction)")
    ap.add_argument("--max-pages", type=int, default=0,
                    help="stop after N pages. 0 means every page")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--out", default=None, help="write JSON here; default is stdout")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the request body and the endpoint, call nothing. Needs no "
                         "credential, because the body cannot contain one")
    ap.add_argument("--password-file", default=os.environ.get("WD_PASSWORD_FILE"),
                    help="read the password from this file instead of WD_PASSWORD. A "
                         "stop-gap until PLT-7; the file must be owner-only and must not "
                         "be one the bundle ships")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    url = workday.endpoint(args.host, args.tenant, args.version)
    first = workday.build_request(args.reference_id_type, page=1, count=args.count)

    if args.dry_run:
        print(f"POST {url}")
        print(first)
        return 0

    creds = read_credentials_file(args.password_file)
    password = os.environ.get("WD_PASSWORD", "") or creds.get("WD_PASSWORD", "")

    # THE USERNAME IS NOT A SECRET, BUT IT IS HALF THE CREDENTIAL. It lives beside the
    # password so the pair cannot drift apart -- the ISU is tenant-qualified
    # (name@tenant), so a username from one tenant and a password from another is a
    # combination that only ever produces a fault.
    args.username = (args.username
                     or os.environ.get("WD_USERNAME")
                     or creds.get("WD_USERNAME")
                     or DEFAULTS["username"])
    tenant_suffix = args.username.rpartition("@")[2]
    if tenant_suffix and tenant_suffix != args.tenant:
        print(f"WARNING  username {args.username} is qualified for tenant "
              f"{tenant_suffix!r} but --tenant is {args.tenant!r}. Workday authenticates "
              f"the pair, so one of these is wrong.", file=sys.stderr)

    if not password:
        print("No password. Set WD_PASSWORD, or pass --password-file. It is deliberately "
              "not a value on the command line -- that lands in shell history and in `ps`. "
              "See docs/workday_reference_service_access.md", file=sys.stderr)
        return 2

    if "impl-" in args.host:
        print(f"NOTE  {args.host} is an IMPLEMENTATION tenant. WIDs from it identify "
              f"nothing in production; do not promote these values as reference data.",
              file=sys.stderr)

    rows: list[dict] = []
    page, total_pages = 1, 1
    while page <= total_pages:
        body = workday.build_request(args.reference_id_type, page=page,
                                     count=args.count)
        if args.verbose:
            print(f"[page {page}] {body}", file=sys.stderr)
        page_rows, results = workday.parse_response(
            fetch_page(url, body, args.username, password, args.timeout))
        rows.extend(page_rows)
        total_pages = results.get("total_pages") or 1
        if args.max_pages and page >= args.max_pages:
            print(f"stopping at --max-pages {args.max_pages} of {total_pages}",
                  file=sys.stderr)
            break
        page += 1

    # THE SERVICE ALREADY FILTERED. This asserts it did, rather than filtering again: a
    # response carrying a type we did not ask for means the request and the response
    # disagree, which is worth a line rather than a silent drop.
    _unexpected = {k: v for k, v in workday.types_present(rows).items()
                   if k != args.reference_id_type}
    if _unexpected:
        print(f"WARNING  asked for {args.reference_id_type}, response also carried "
              f"{_unexpected}", file=sys.stderr)

    for name, count in workday.types_present(rows).items():
        print(f"  {count:6}  {name}", file=sys.stderr)

    payload = json.dumps({"endpoint": url, "tenant": args.tenant,
                          "version": args.version,
                          "reference_id_type": args.reference_id_type, "rows": rows},
                         indent=2, ensure_ascii=False) + "\n"
    if args.out:
        Path(args.out).write_text(payload, encoding="utf-8")
        print(f"wrote {len(rows)} row(s) to {args.out}", file=sys.stderr)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
