# Ranger REST API reference (for this system)

Every endpoint below was verified two ways: against Apache Ranger's actual
server source (`apache/ranger`, `security-admin/.../rest/XUserREST.java`
and `.../biz/XUserMgr.java`, checked 2026-09-24) and, where noted, against
a live Ranger 2.8.0 instance in this repo's test cluster. These `xusers`
REST endpoints are the same ones Ranger's own UserSync component has used
internally for years — they're stable across the Ranger 2.x line, but
Ranger doesn't publish a REST changelog per release, so if something here
doesn't match your exact version, check the running admin's actual jar
version (`Help → About` in the UI, or the version string in
`ranger-admin-services.log` on startup) and compare against the
`XUserREST.java` source for that tag on GitHub before assuming this doc is
wrong.

## Base URL and auth

```
http://<ranger-host>:6080/service/xusers/...
```

All endpoints are under the `/service` context path, then `xusers`. Auth
is HTTP Basic, against a Ranger account with the `ROLE_SYS_ADMIN` role —
every endpoint this system uses is annotated
`@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")` server-side, so a lower-role
account gets a 403. Create a dedicated service account for this (see
Implementation Guide step 1) rather than reusing the human admin login.

```bash
curl -u '<service-account>:<password>' \
  -H 'Content-Type: application/json' \
  http://ranger-host:6080/service/xusers/ugsync/groups \
  -X POST -d '...'
```

## The #1 gotcha: call order matters, and failures are silent

The bulk membership endpoint (`POST /xusers/ugsync/groupusers`) does **not**
create missing users or groups — if you reference a username or group name
that doesn't already exist in Ranger's database, that entry is **silently
ignored** (verified directly in `XGroupUserService.createOrDeleteXGroupUsers`:
it looks up the group by name, and if `null`, logs a debug line and returns
`false` — no exception, no error in the HTTP response, the call still
returns 200). Same for unknown usernames within an otherwise-valid group.

**Always call in this order, every time, for every message:**

1. `POST /xusers/ugsync/users` — ensure the user exists.
2. `POST /xusers/ugsync/groups` — ensure every referenced group exists.
3. `POST /xusers/ugsync/groupusers` — sync membership.

All three are safe to call unconditionally on every message even when
nothing actually changed (they're upserts) — don't try to optimize this
away by skipping steps 1-2 "when you're pretty sure they already exist".
That optimization is exactly how this system goes silently wrong in
production.

---

## 1. `POST /service/xusers/ugsync/users` — ensure user(s) exist

Bulk create-or-update. Same endpoint Ranger's own UserSync calls on every
sync cycle for every user, so it's safe/idempotent to call repeatedly.

**Request body** — `VXUserList`:

```json
{
  "vXUsers": [
    {
      "name": "alice",
      "firstName": "Alice",
      "lastName": "Nguyen",
      "emailAddress": "alice@example.com",
      "userSource": 1,
      "isVisible": 1,
      "status": 1,
      "userRoleList": ["ROLE_USER"]
    }
  ]
}
```

| Field | Required | Notes |
|---|---|---|
| `name` | **yes** | Must match the username Trino/Ranger/Superset already use for this person. Blank/`"null"` → silently skipped. |
| `firstName` | **yes** | **Not documented as required anywhere, but verified in source it is**: `XUserMgr.createOrUpdateXUsers` silently skips any user with a blank/null `firstName` (`"Ignoring user {}: invalid firstName"` in the admin log — you will not see this unless you go looking). If your IdP doesn't reliably supply a first name, default it to the username rather than leave it blank. |
| `lastName`, `emailAddress` | no | Cosmetic; shown in the Ranger UI. |
| `userSource` | recommended | `1` = `USER_EXTERNAL` (marks this as synced from an external system, same value UserSync uses — makes it visually distinguishable in the Ranger UI from manually-created users). `0` = internal. |
| `isVisible` | recommended | `1` = visible in the Ranger UI. |
| `status` | recommended | `1` = enabled, `0` = disabled. |
| `userRoleList` | recommended | `["ROLE_USER"]` for ordinary end users. Don't grant `ROLE_SYS_ADMIN` here — that's for the sync service's own account, not synced end users. |

**Response:** `200 OK`, plain integer count of users actually created/updated as the body (not a JSON object) — e.g. `1`. A count lower than expected means some entries were silently skipped (check `firstName`).

---

## 2. `POST /service/xusers/ugsync/groups` — ensure group(s) exist

Bulk create-or-update, also idempotent (calls the same
`createXGroupWithoutLogin` upsert-by-name path Ranger's UserSync uses).

**Request body** — `VXGroupList`:

```json
{
  "vXGroups": [
    {
      "name": "SENSITIVE",
      "description": "Synced from OIDC attribute enrichment — do not edit membership by hand, it will be overwritten",
      "groupSource": 1,
      "isVisible": 1
    }
  ]
}
```

| Field | Required | Notes |
|---|---|---|
| `name` | **yes** | This is the exact string you'll reference later in a Ranger policy's `policyItems[].groups`. Pick a naming convention up front (Implementation Guide step 2) — renaming later means updating every policy that references the old name. |
| `description` | no | Put a note here that membership is machine-managed — saves a future admin from manually editing membership and having it silently overwritten on the next sync. |
| `groupSource` | recommended | `1` = `GROUP_EXTERNAL` (same convention as `userSource` above). |
| `isVisible` | recommended | `1`. |

**Response:** `200 OK`, integer count.

---

## 3. `POST /service/xusers/ugsync/groupusers` — sync membership (add/delete)

This is the core sync call — explicit add and delete sets **per group**, in
one bulk request. This is the exact mechanism to use for "desired-state"
sync: for each group whose membership might have changed for this user,
say who should be added and who should be removed; Ranger applies both in
one transaction per group.

**Request body** — a JSON array of `GroupUserInfo`:

```json
[
  {
    "groupName": "CEO",
    "addUsers": [],
    "delUsers": ["alice"]
  },
  {
    "groupName": "SENSITIVE",
    "addUsers": ["alice"],
    "delUsers": []
  }
]
```

| Field | Notes |
|---|---|
| `groupName` | Must already exist (see gotcha above). |
| `addUsers` | Set of usernames to add to this group. Any username not already a Ranger user (step 1) is silently skipped. |
| `delUsers` | Set of usernames to remove from this group. Silently skipped if the group or user doesn't exist, or if the user wasn't a member anyway (safe no-op). |

An entry with both `addUsers` and `delUsers` empty is skipped entirely
("Group memberships for source are empty" in the debug log) — only include
a `groupName` entry if it actually has something to add or remove.

**Response:** `200 OK`, integer = number of groups that had at least one
membership row actually changed (not the number of users changed).

---

## Read endpoints (for the reconciliation job)

These aren't part of UserSync's write path, but you need them to compute
"what does Ranger currently think" before diffing against desired state.

### Get a user's current groups

```
GET /service/xusers/{userId}/groups
```
Needs a numeric Ranger user ID, not a username — resolve it first:

```
GET /service/xusers/users/userName/{userName}
```
→ returns a `VXUser` object; take its `id` field.

Response of the groups call is a `VXGroupList`
(`{"vXGroups": [{"name": "SENSITIVE", ...}, ...]}`).

### Get a group's current members

```
GET /service/xusers/groupusers/groupName/{groupName}
```
Returns a `VXGroupUserInfo` containing the group and its member list —
useful if you're diffing per-group instead of per-user in the
reconciliation job (either direction works; `ranger_client.py` in this repo
implements the per-user direction since messages arrive per-user).

### Search groups/users by name (existence checks)

```
GET /service/xusers/groups?name=<name>
GET /service/xusers/users?name=<name>
```
Returns a paged list (`VXGroupList`/`VXUserList`) with a `totalCount`
field — `totalCount == 0` means it doesn't exist yet. This is what
`ranger/setup-impersonation.sh` (in `../audit-logging/`) uses to check for
an existing user before creating one, via the closely related
`/xusers/secure/users` endpoints below.

---

## Single-object "secure" endpoints (alternative to bulk)

Everything above uses the bulk `ugsync/*` endpoints because they're
built for exactly this use case (sync, not human CRUD) and because
`ugsync/groupusers`' explicit add/del-set shape maps directly onto "diff
desired vs current". There's also a parallel, single-object CRUD API under
`/secure/...`, used elsewhere in this repo (`audit-logging/ranger/
setup-impersonation.sh` uses `POST /xusers/secure/users` to create the
`superset` Ranger user, and manipulates a policy's `policyItems` directly
via the `/service/public/v2/api/policy` endpoint — a different API family
again, for policies rather than users/groups). You generally don't need
these for this system, but they exist if you need finer-grained control:

| Action | Method + path |
|---|---|
| Create one group | `POST /xusers/secure/groups` (body: one `VXGroup`) |
| Update one group | `PUT /xusers/secure/groups/{id}` |
| Get one group | `GET /xusers/secure/groups/{id}` |
| Delete one group | `DELETE /xusers/secure/groups/{groupName}` |
| Create one user | `POST /xusers/secure/users` (body: one `VXUser`) |
| Create/update one group-user link | `POST` / `PUT /xusers/groupusers` (body: one `VXGroupUser` — note: **not** the same shape as bulk `GroupUserInfo`; this one is `{"name": "<groupName>", "userId": <numeric id>}`, singular and by numeric user id, easy to confuse with the bulk endpoint's shape) |

## What this API does *not* do: policies

None of the above grants any actual access. Creating a group and adding
members to it just makes the group exist and have members — a Ranger
**policy** still has to reference that group name in its
`policyItems[].groups` list for it to mean anything at query time. That's
a separate, deliberate step (Implementation Guide step 7) using the policy
API (`/service/public/v2/api/policy`, `/service/public/v2/api/service/
<service-name>/policy/<policy-name>` — see `audit-logging/ranger/
setup-impersonation.sh` for a worked example of reading, modifying, and
PUTing back a policy's `policyItems`). Keep group-membership sync
(automated, this system) and policy authorship (manual, reviewed) as two
separate concerns — don't be tempted to have the sync service also write
policies; a bug in an automated policy-writer is a much worse blast radius
than a bug in group membership.
