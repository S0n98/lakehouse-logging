# OIDC attribute → Ranger group sync

**What this is:** a design + reference implementation for keeping Apache
Ranger's group membership in sync with attributes computed by a *custom
OIDC identity provider* (AD-backed, with an attribute-enrichment step that
adds extra claims like `SENSITIVE` / `CEO` / region tags before handing the
token to Superset). Ranger's native UserSync can't see those attributes —
they're synthesized by the OIDC layer, not stored as real AD groups — so
this system pushes them into Ranger explicitly, on change, via Ranger's
REST API.

**Why it exists / how we got here:** this was designed in conversation
after building the [`audit-logging`](../audit-logging/) pipeline on the
test cluster in this repo, while answering "how do I grant Ranger
permissions based on an LDAP/OIDC group?" The test cluster's Ranger has no
UserSync at all and uses a toy LDAP (lldap) — this design is for the
**real** production environment described by the user: a customized OIDC
provider in front of AD, doing attribute enrichment, already used by
Superset for login. It is not deployed anywhere in this repo's cluster;
it's a blueprint plus working reference code for building it in that real
environment.

## Read these in order

1. **[ARCHITECTURE.md](ARCHITECTURE.md)** — the system design: components,
   data flow, message schema, failure modes, why it's shaped this way.
   Read this first to understand *why* before *how*.
2. **[RANGER-REST-API-REFERENCE.md](RANGER-REST-API-REFERENCE.md)** — every
   Ranger REST endpoint this system touches, with verified request/response
   shapes and the gotchas that will silently break it if missed (e.g. the
   #1 gotcha: the bulk membership endpoint silently no-ops if the user or
   group doesn't already exist in Ranger — order of operations matters).
3. **[IMPLEMENTATION-GUIDE.md](IMPLEMENTATION-GUIDE.md)** — step-by-step
   build instructions, written assuming you've never touched Ranger's REST
   API before. Follow this to actually stand the system up, in order.

## What's in `consumer-service/` and `k8s/`

A working Python reference implementation of the Ranger-facing half of the
system (the half this doc set has verified, tested knowledge of):

| File | Purpose |
|---|---|
| `consumer-service/ranger_client.py` | Thin wrapper around the Ranger REST endpoints documented in RANGER-REST-API-REFERENCE.md — ensure user, ensure group, sync membership, read-back for reconciliation. |
| `consumer-service/sync_service.py` | Kafka consumer: reads one "user's desired Ranger groups" event at a time, calls `ranger_client` to make Ranger match it. |
| `consumer-service/reconcile.py` | Scheduled drift-correction job — the safety net under the event-driven path. Has a `TODO` stub where you plug in a call to *your* OIDC/AD provider's API, since that's specific to your real environment and not something this repo has access to. |
| `consumer-service/requirements.txt`, `Dockerfile` | Build the above into a deployable image. |
| `k8s/consumer-deployment.yaml` | Deployment for `sync_service.py`. |
| `k8s/reconcile-cronjob.yaml` | CronJob for `reconcile.py`. |
| `k8s/secret-example.yaml` | What secrets the above expect — placeholders only, no real values. |

## Quick summary of the design

```
OIDC provider (attribute enrichment)
        │  publishes full desired-state event on every relevant user change
        ▼
   Kafka topic (durable, replayable)
        │
        ▼
 ranger-group-sync consumer service ──calls──▶ Ranger REST API (/service/xusers/...)
        ▲
        │  drift-correction pass, e.g. hourly
   reconciliation CronJob ──reads authoritative state from──▶ OIDC/AD provider
```

Two hard rules that shape everything else in these docs — both came out of
explicit tradeoffs discussed before building this:

- **Events carry full desired state, not deltas.** A message says "alice's
  Ranger groups are now `[SENSITIVE]`", never "remove CEO from alice". This
  makes replay and out-of-order delivery safe by construction.
- **The event path is not a substitute for reconciliation.** Queues lose
  messages, consumers have bugs, dead-letters happen silently. The
  reconciliation job re-derives truth from the OIDC/AD provider on a
  schedule and corrects drift — treat it as required, not optional.
