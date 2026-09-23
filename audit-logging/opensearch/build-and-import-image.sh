#!/usr/bin/env bash
# Builds the opensearch-with-s3 image (opensearchproject/opensearch + the
# repository-s3 plugin, needed to snapshot indices to the MinIO cold bucket)
# and imports it directly into containerd's k8s.io namespace -- the namespace
# kubelet's CRI plugin actually reads from (NOT the "default" namespace that
# `ctr images` uses without -n).
#
# This is a single-node cluster with no image registry of its own, so the
# image is never pushed anywhere; it only needs to exist in this node's local
# image store. If the cluster gains more nodes later, push this image to a
# real registry instead and reference it normally.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

IMAGE="opensearch-with-s3:2.19.1"
CONTAINERD_SOCK="${CONTAINERD_SOCK:-/run/k3s/containerd/containerd.sock}"
CTR="${CTR:-/var/lib/rancher/rke2/bin/ctr}"
TMPFILE="$(mktemp -t opensearch-with-s3-XXXX.tar)"
trap 'rm -f "$TMPFILE"' EXIT

docker build -t "$IMAGE" -f Dockerfile.opensearch-s3 .
docker save "$IMAGE" -o "$TMPFILE"
"$CTR" -a "$CONTAINERD_SOCK" -n k8s.io images import "$TMPFILE"

echo "Imported $IMAGE into containerd k8s.io namespace:"
"$CTR" -a "$CONTAINERD_SOCK" -n k8s.io images ls | grep opensearch-with-s3
