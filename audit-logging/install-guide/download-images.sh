#!/usr/bin/env bash
# Pulls and saves (as .tar files) every container image this stack needs,
# for transferring into an air-gapped environment -- see
# "Installing in an offline / air-gapped environment" in this directory's
# README.md for the full reasoning behind this list and the one image
# this script CANNOT fetch for you.
#
# Usage:
#   ./download-images.sh [output-dir]      # default output-dir: ./image-archive
#
# Then transfer output-dir to the air-gapped cluster's node(s) and import
# each tar into containerd (NOT just Docker's store -- kubelet reads from
# containerd):
#   ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images import <file>.tar
# (the `-n k8s.io` is required -- `ctr` defaults to the `default` namespace,
# which kubelet's CRI does not read from; see /root/datahub/CLAUDE.md)
#
# Requires: docker (used for pull + save, matching the rest of this
# project's documented offline-transfer method -- see CLAUDE.md).
set -uo pipefail

OUT_DIR="${1:-./image-archive}"
mkdir -p "$OUT_DIR"

# name:tag -> safe filename
safe_name() {
  echo "$1" | tr '/:@' '___'
}

PULL_FAILED=()
SAVE_FAILED=()
SAVED=()

pull_and_save() {
  local image="$1"
  local fname
  fname="$(safe_name "$image").tar"
  echo "=== $image ==="
  if ! docker pull "$image"; then
    echo "  PULL FAILED -- skipping (see summary at the end)"
    PULL_FAILED+=("$image")
    return
  fi
  if docker save "$image" -o "$OUT_DIR/$fname"; then
    SAVED+=("$image -> $fname")
  else
    echo "  SAVE FAILED"
    SAVE_FAILED+=("$image")
  fi
}

# Save-only: for images that can't be pulled from any registry at all --
# custom, local-only builds. Requires the image to already exist in this
# host's Docker store (whatever originally produced superset-ldap -- see
# this directory's README.md, "Container images" table).
save_local_only() {
  local image="$1"
  local fname
  fname="$(safe_name "$image").tar"
  echo "=== $image (custom, local-only -- not pulling, saving if present) ==="
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "  NOT FOUND locally -- cannot include. This is a custom image with no"
    echo "  public registry source; see this directory's README.md for how to"
    echo "  rebuild or re-obtain it before re-running this script."
    PULL_FAILED+=("$image (not found locally, custom image)")
    return
  fi
  if docker save "$image" -o "$OUT_DIR/$fname"; then
    SAVED+=("$image -> $fname")
  else
    echo "  SAVE FAILED"
    SAVE_FAILED+=("$image")
  fi
}

echo "##### A. Audit-logging pipeline's own additions #####"
# Deliberately NOT a pinned digest -- this is the opensearch chart's own
# unpinned default for its fsgroup-volume init container (nothing in this
# repo's values.yaml overrides it), so it drifts with upstream. A digest
# recorded here would just go stale (confirmed: an earlier version of this
# script pinned a digest that had already drifted by 2026-10-02). Pull
# "latest" fresh each time you build an offline bundle instead of trusting
# any digest written down in a doc.
pull_and_save "docker.io/library/busybox:latest"
pull_and_save "opensearchproject/opensearch:2.19.1"
pull_and_save "opensearchproject/opensearch-dashboards:3.8.0"
pull_and_save "ghcr.io/kubeflow/spark-operator/controller:2.5.2"
pull_and_save "apache/spark:3.5.3"
pull_and_save "cr.fluentbit.io/fluent/fluent-bit:5.0.9"
pull_and_save "python:3.12-alpine"
# quay.io/minio/mc has been intermittently returning 401 during this
# project (rate-limiting, apparently) -- a failure here is not fatal to
# the rest of this script. If it fails, the install-guide README's
# "Container images" table already documents the fallback: skip the mc
# image entirely and create buckets directly with any S3 SDK instead
# (e.g. boto3's create_bucket -- no image needed at all).
pull_and_save "quay.io/minio/mc:latest"

echo
echo "##### B. Pre-existing lakehouse platform #####"
pull_and_save "trinodb/trino:480"
pull_and_save "apache/ranger:2.8.0"
pull_and_save "ghcr.io/projectnessie/nessie:0.107.9"
pull_and_save "quay.io/minio/operator:v5.0.17"
pull_and_save "quay.io/minio/minio:RELEASE.2024-08-03T04-33-23Z"
pull_and_save "docker.io/bitnamilegacy/postgresql:14.17.0-debian-12-r3"
pull_and_save "docker.io/bitnamilegacy/redis:7.0.10-debian-11-r4"
save_local_only "docker.io/library/superset-ldap:6.0.0"

echo
echo "===================== SUMMARY ====================="
echo "Saved to $OUT_DIR:"
printf '  %s\n' "${SAVED[@]}"
if [ "${#PULL_FAILED[@]}" -gt 0 ]; then
  echo
  echo "FAILED to pull/find (${#PULL_FAILED[@]}):"
  printf '  %s\n' "${PULL_FAILED[@]}"
fi
if [ "${#SAVE_FAILED[@]}" -gt 0 ]; then
  echo
  echo "FAILED to save (${#SAVE_FAILED[@]}):"
  printf '  %s\n' "${SAVE_FAILED[@]}"
fi
echo "====================================================="

if [ "${#PULL_FAILED[@]}" -gt 0 ] || [ "${#SAVE_FAILED[@]}" -gt 0 ]; then
  exit 1
fi
