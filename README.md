# datahub

Working repo for the audit/governance layer built around this cluster's
lakehouse stack (Trino, Superset, Iceberg/Nessie, MinIO, Ranger).

## What's here

```
audit-logging/       Hot (OpenSearch) + cold (Iceberg-on-MinIO) audit
                      logging for Ranger, Trino, and Superset, plus an
                      offline/air-gapped install bundle. Deployed and
                      verified on THIS cluster.

ranger-group-sync/    Design + reference implementation for syncing Ranger
                      group membership from a real, external OIDC
                      provider's attribute-enrichment step (AD-backed,
                      attributes like SENSITIVE/CEO that aren't real AD
                      groups). A blueprint for a different, real
                      production environment -- not deployed anywhere in
                      this repo's cluster.

CLAUDE.md             Operational notes about THIS cluster itself: known
                      fragility (disk pressure, IP/etcd quirks, stuck helm
                      releases), recurring gotchas, and fixes already
                      worked out. Read this before touching the cluster,
                      not just the code.
```

Each subdirectory has its own `README.md` (install/operate guide) and, for
`audit-logging/`, an `ARCHITECTURE.md` (design rationale + bugs found and
fixed during rollout). Start with the subdirectory's README for anything
you're about to actually build or deploy; start with `CLAUDE.md` for
anything that looks like the cluster itself is broken.

## Two different things living in one repo

`audit-logging/` describes what's actually running on this cluster right
now -- it's operational, and its docs describe real, verified state
(specific bugs found, specific verification steps run, specific document
counts). `ranger-group-sync/` is a design for someone else's real
production environment, written here because that's where the
conversation that produced it happened -- treat its docs as a blueprint to
build from, not as a description of anything currently deployed. Each
directory's own README says which situation it's in; don't assume one
implies the other.
