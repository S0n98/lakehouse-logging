#!/usr/bin/env bash
# Grants a Trino principal permission to impersonate other users in Ranger,
# so Ranger/Trino audit logs show the real end user for queries that come
# through a shared connection (e.g. Superset's single Trino database
# connection) instead of the shared principal.
#
# Verified end-to-end on this cluster 2026-09-23: decompiled the Ranger
# plugin (io.trino_trino-ranger-480.jar,
# io.trino.plugin.ranger.RangerSystemAccessControl.checkCanImpersonateUser)
# to confirm impersonation is a real, policy-backed check -- resource type
# "trinouser", access type "impersonate" -- not a hardcoded allow/deny.
# Confirmed against Ranger's live service-def for "trino" too.
#
# Ranger's default install already creates a catch-all policy
# ("all - trinouser", resource trinouser=*) granting impersonate to
# "ranger" and "{USER}" (self-impersonation only) -- this script adds a
# given principal (default: superset) to that SAME policy rather than
# creating a competing one, since Ranger rejects a second policy matching
# the same resource pattern.
#
# Usage:
#   OS_HOST=... (not needed here)
#   RANGER_HOST=http://ranger-apache-ranger.ranger:6080 \
#   RANGER_ADMIN_USER=admin RANGER_ADMIN_PASSWORD=<...> \
#   IMPERSONATOR=superset \
#     ./setup-impersonation.sh
set -euo pipefail

RANGER_HOST="${RANGER_HOST:-http://ranger-apache-ranger.ranger:6080}"
RANGER_ADMIN_USER="${RANGER_ADMIN_USER:?set RANGER_ADMIN_USER}"
RANGER_ADMIN_PASSWORD="${RANGER_ADMIN_PASSWORD:?set RANGER_ADMIN_PASSWORD}"
IMPERSONATOR="${IMPERSONATOR:-superset}"
AUTH="${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASSWORD}"

echo "--- ensuring Ranger user '${IMPERSONATOR}' exists ---"
existing=$(curl -sk -u "$AUTH" "${RANGER_HOST}/service/xusers/users?name=${IMPERSONATOR}")
count=$(echo "$existing" | python3 -c "import json,sys; print(json.load(sys.stdin).get('totalCount', 0))")
if [ "$count" = "0" ]; then
  genpass=$(openssl rand -base64 24 | tr -d '=+/' | cut -c1-20)Aa1!
  curl -sk -u "$AUTH" -X POST \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${IMPERSONATOR}\",\"password\":\"${genpass}\",\"firstName\":\"${IMPERSONATOR}\",\"description\":\"Registered by setup-impersonation.sh so it can hold the impersonate policy grant\",\"userRoleList\":[\"ROLE_USER\"],\"status\":1,\"isVisible\":1}" \
    "${RANGER_HOST}/service/xusers/secure/users" >/dev/null
  echo "created user ${IMPERSONATOR}"
  unset genpass
else
  echo "user ${IMPERSONATOR} already exists"
fi

echo "--- fetching existing 'all - trinouser' policy ---"
policy=$(curl -sk -u "$AUTH" "${RANGER_HOST}/service/public/v2/api/service/trino/policy/all%20-%20trinouser")
policy_id=$(echo "$policy" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")

echo "--- adding '${IMPERSONATOR}' to the impersonate policyItem (policy id ${policy_id}) ---"
echo "$policy" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for item in d['policyItems']:
    if any(a['type'] == 'impersonate' and a['isAllowed'] for a in item['accesses']):
        if '$IMPERSONATOR' not in item['users']:
            item['users'].append('$IMPERSONATOR')
        break
else:
    raise SystemExit('no impersonate policyItem found on the default policy -- inspect it manually')
json.dump(d, sys.stdout)
" > /tmp/ranger-impersonation-policy-updated.json

curl -sk -u "$AUTH" -X PUT \
  -H 'Content-Type: application/json' \
  -d @/tmp/ranger-impersonation-policy-updated.json \
  "${RANGER_HOST}/service/public/v2/api/policy/${policy_id}"
rm -f /tmp/ranger-impersonation-policy-updated.json

echo
echo "Done. Trino polls Ranger for policy updates every 30s"
echo "(ranger.plugin.trino.policy.pollIntervalMs) -- this takes effect shortly, no restart needed."
