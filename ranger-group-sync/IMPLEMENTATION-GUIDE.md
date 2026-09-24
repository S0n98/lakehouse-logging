# Implementation guide

Written assuming you've never touched Ranger's REST API before. Read
[ARCHITECTURE.md](ARCHITECTURE.md) first if you haven't — this guide
builds the thing that doc describes, and won't re-explain *why* each step
exists.

Follow the steps in order — later steps depend on earlier ones (e.g. you
can't test membership sync before the service account and the group
naming convention exist).

## Step 1 — Create a dedicated Ranger service account

Don't reuse the human `admin` login for this. Create a separate account so
it can be rotated/revoked independently of any person's credentials.

Via the Ranger Admin UI: **Settings → Users/Groups → Add New User**, role
`Admin` (this maps to `ROLE_SYS_ADMIN`, which every endpoint this system
uses requires — see RANGER-REST-API-REFERENCE.md's Base URL and auth
section).

Or via REST, using the human admin's credentials just this once to bootstrap it:

```bash
curl -u 'admin:<admin-password>' -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "ranger-group-sync-svc",
    "password": "<generate-a-strong-password>",
    "firstName": "Ranger Group Sync",
    "description": "Service account for the OIDC-attribute-to-Ranger-group sync system. Do not use interactively.",
    "userRoleList": ["ROLE_SYS_ADMIN"],
    "status": 1,
    "isVisible": 1
  }' \
  http://<ranger-host>:6080/service/xusers/secure/users
```

Store the password immediately in your secret manager / a Kubernetes
`Secret` (see `k8s/secret-example.yaml`) — never in a script, config file,
or commit.

**Check yourself:** confirm the account works and has the right role
before moving on:

```bash
curl -u 'ranger-group-sync-svc:<password>' \
  http://<ranger-host>:6080/service/xusers/ugsync/groups \
  -X POST -H 'Content-Type: application/json' -d '{"vXGroups":[]}'
# expect: 200 OK, body "0"  (empty list, zero groups touched, but no 403)
```

## Step 2 — Decide your group naming convention *before* writing any code

Whatever names you push into Ranger here are the names Ranger policies
will reference forever. Two things to decide now:

1. **A prefix to avoid collisions**, e.g. `oidc_SENSITIVE` rather than bare
   `SENSITIVE` — so it's unambiguous in the Ranger UI which groups are
   managed by this system versus created by a human or a different sync
   source (native AD UserSync, if that's ever added later for other
   groups). The reference code in this repo doesn't hardcode a prefix —
   add it wherever you build the `ranger_groups` list in your OIDC
   producer.
2. **A 1:1 mapping from OIDC attribute value to Ranger group name.** Write
   this mapping down somewhere durable (a config file, a wiki page) before
   building the producer side — it's the contract between your OIDC
   system and every Ranger policy anyone writes against these groups.

## Step 3 — Set up the Kafka topic

```bash
kafka-topics.sh --create \
  --topic user-attribute-changed \
  --bootstrap-server <broker>:9092 \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=604800000   # 7 days -- enough to investigate/replay a bad deploy, not meant as a durability strategy by itself
```

Partition count matters for ordering (see ARCHITECTURE.md "Ordering") —
messages are keyed by `username`, so all events for one user always land
in the same partition and stay ordered relative to each other. 3
partitions is a reasonable starting point for most orgs; you don't need
more partitions than you plan to run consumer replicas.

If you're using RabbitMQ instead: create a single durable queue (or a
consistent-hash exchange keyed by username if you need to scale
consumers), and adapt `sync_service.py`'s Kafka-specific consumer loop to
use `pika` instead of `kafka-python` — the `process_message()` function
and everything in `ranger_client.py` is broker-agnostic and needs no
changes.

## Step 4 — Update your OIDC attribute-enrichment step to publish events

This is the one piece of "your real system" this repo can't write for
you — it lives in your OIDC provider's codebase, not here. What it needs
to do, every time a user's enriched attributes change:

```python
import json
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["<broker>:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def publish_ranger_group_change(username: str, first_name: str, last_name: str,
                                 email: str, ranger_groups: list[str]):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "attributes": {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
        },
        # FULL current desired state -- never a delta. See
        # ARCHITECTURE.md's message schema section for why this matters.
        "ranger_groups": ranger_groups,
    }
    producer.send("user-attribute-changed", key=username, value=event)
```

Wire this into whatever function in your OIDC layer currently computes the
enriched attributes — call it right after the attributes are
(re)computed, passing the **complete current set** of Ranger-relevant
groups for that user, not just whatever changed.

**Test this in isolation before moving to Step 5:** publish a test event
by hand and confirm it lands on the topic:

```bash
kafka-console-consumer.sh --topic user-attribute-changed \
  --bootstrap-server <broker>:9092 --from-beginning
```

## Step 5 — Build and deploy the consumer service

The code is already written in `consumer-service/` — this step is build +
configure + deploy, not write-from-scratch.

1. Build and push the image:
   ```bash
   cd consumer-service
   docker build -t <your-registry>/ranger-group-sync:latest .
   docker push <your-registry>/ranger-group-sync:latest
   ```
2. Create the namespace and secret:
   ```bash
   kubectl create namespace ranger-group-sync
   # edit k8s/secret-example.yaml with real values first, or create imperatively:
   kubectl create secret generic ranger-group-sync-secrets \
     --namespace=ranger-group-sync \
     --from-literal=RANGER_USER=ranger-group-sync-svc \
     --from-literal=RANGER_PASSWORD='<the password from Step 1>' \
     --from-literal=KAFKA_BOOTSTRAP_SERVERS='<broker-1>:9092,<broker-2>:9092'
   ```
3. Edit `k8s/consumer-deployment.yaml`: replace `<REPLACE-ME>` with your
   registry, and `RANGER_URL`/`KAFKA_TOPIC` if they differ from the
   defaults.
4. Deploy:
   ```bash
   kubectl apply -f k8s/consumer-deployment.yaml
   kubectl logs -n ranger-group-sync -l app=ranger-group-sync-consumer -f
   ```
5. **Dead-letter queue:** `sync_service.py` has two `# TODO` markers where
   it should publish permanently-failed messages somewhere durable instead
   of just logging and dropping them. Create a `user-attribute-changed-dlq`
   topic and fill those in with a producer call before relying on this in
   production — without it, a poison message is silently lost after
   `MAX_RETRIES`, with only a log line as evidence.

## Step 6 — Deploy the reconciliation job

1. **Implement `fetch_desired_state_from_idp()`** in `reconcile.py` —
   this is the one function this repo left as a stub, since it has to call
   *your* real OIDC/AD provider's API, which isn't something this repo has
   access to. See the docstring in that function for the exact shape it
   needs to return.
2. Rebuild and push the same image (it already contains `reconcile.py`,
   only the `args:` differ between the Deployment and the CronJob).
3. Edit `k8s/reconcile-cronjob.yaml`: same `<REPLACE-ME>` registry
   replacement, adjust `schedule` if hourly doesn't fit your needs.
4. Deploy:
   ```bash
   kubectl apply -f k8s/reconcile-cronjob.yaml
   # trigger one run manually to test, rather than waiting for the schedule:
   kubectl create job --from=cronjob/ranger-group-sync-reconcile \
     -n ranger-group-sync reconcile-manual-test
   kubectl logs -n ranger-group-sync job/reconcile-manual-test
   ```

## Step 7 — Write the Ranger policy that actually grants access

Everything so far manages **group membership only** — it grants nothing
by itself. A human still needs to write the policy. In the Ranger Admin
UI: navigate to the relevant service (e.g. your Trino service), **Add New
Policy**, and under "Select Group" add the exact group name from Step 2
(e.g. `oidc_SENSITIVE`) with whichever access types the sensitivity level
requires.

If you'd rather do this via REST (e.g. to keep policies in version
control), see `../audit-logging/ranger/setup-impersonation.sh` in this
repo for a complete, working example of reading a policy's `policyItems`
via `/service/public/v2/api/policy/<id>`, modifying it in Python, and
PUTing it back — the same pattern applies to adding a group instead of a
user to a policy item.

**This step is deliberately manual and separate from the automated sync.**
Don't have the consumer service or reconciliation job create or modify
policies — see ARCHITECTURE.md's Security section for why.

## Step 8 — End-to-end test checklist

Work through all of these before calling this done:

- [ ] Publish an event for a brand-new username Ranger has never seen →
      confirm the user AND the group(s) get created (check Ranger UI
      under Users/Groups).
- [ ] Confirm a Trino/Superset query as that user is actually granted (or
      denied) according to the policy from Step 7 — don't just check
      Ranger's UI, run a real query.
- [ ] Publish a second event for the same user with a **different** group
      list (e.g. remove one group, add another) → confirm both the
      addition and the removal took effect.
- [ ] Publish the exact same event twice (simulate a duplicate/retry) →
      confirm the second one is a harmless no-op (check the consumer's
      logs show "already matches desired state, nothing to do").
- [ ] Stop Ranger (or block network to it) briefly, publish an event, then
      restore Ranger → confirm the consumer retries and eventually
      succeeds, rather than dropping the message.
- [ ] Publish a malformed message (missing `username`, say) → confirm it's
      rejected/dead-lettered rather than crashing the consumer or being
      silently ignored forever.
- [ ] Manually change a user's group membership directly in Ranger (as if
      a human "helpfully" fixed it by hand), then run the reconciliation
      job → confirm it detects and reverts the drift back to what the IdP
      says (this also validates step 6's `fetch_desired_state_from_idp`
      implementation is correct).

## Step 9 — Observability

Wire up at minimum (see ARCHITECTURE.md's Observability section for what
and why):

- Consumer group lag on the `user-attribute-changed` topic.
- A dashboard/alert on dead-letter topic depth.
- A dashboard/alert on the reconciliation job's "corrections made" count
  and on the job itself failing.

## Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| Message processed, logs say success, but user's Ranger access didn't change | You sent a delta instead of full desired state from the OIDC producer. | Check the published message's `ranger_groups` — it must be the user's *complete* current list, not just what changed. |
| Membership sync call returns 200 but nothing happens in Ranger | The user or group referenced doesn't exist in Ranger yet — the bulk membership endpoint silently no-ops on unknown names. | Confirm `ensure_users`/`ensure_groups` ran successfully first, in that order, in the same processing attempt. See RANGER-REST-API-REFERENCE.md gotcha #1. |
| `ensure_users` reports fewer users created than expected | A `firstName` was blank/null for the skipped user(s). | Check the OIDC producer always sends a non-empty `first_name`, or that `ranger_client.py`'s username fallback is actually being hit. |
| 403 Forbidden from every Ranger call | Service account doesn't have `ROLE_SYS_ADMIN`. | Re-check Step 1; look the account up in the Ranger UI's Users list and confirm its role. |
| Consumer lag grows without bound | Ranger is slow/down, or a poison message is stuck retrying and blocking its partition. | Check Ranger's health first; check consumer logs for repeated retry warnings for the same `event_id`. |
| Reconciliation job reports corrections every single run | The event-driven path (`sync_service.py`) is unreliable — messages are being lost or failing silently before reaching the DLQ. | Check the DLQ topic depth and the consumer's error logs around the time the drifted user's attributes last changed. |
