#!/usr/bin/env bash
# Pulls (as .tgz) and refreshes the values files for every Helm release
# this pipeline owns an install-guide bundle for -- the chart-and-values
# counterpart to download-images.sh (which handles container images).
# Companion to the "Standard install" / "charts/" + "values/" layout
# described in this directory's README.md.
#
# Usage:
#   ./download-charts.sh
#
# Requires: helm, pointed at a cluster with internet access (adds/updates
# the upstream chart repos below) AND, for spark-operator/trino/superset,
# a working `kubectl`/`helm` context against the cluster these were
# actually deployed on (needed for `helm get values` -- see "Values
# handling" below). Run this on the same box the charts are deployed on,
# same as every other script in this project.
#
# Scope: only the 6 releases this pipeline bundles a chart+values pair
# for (see README.md's "What's here and where it came from" table).
# `trino`/`superset` here are this pipeline's *audit sources*, pre-existing
# releases this pipeline modified -- not the rest of the platform (Ranger,
# Nessie, MinIO, Postgres, Redis). Those platform charts are deliberately
# NOT bundled here (see README.md's offline-images section, "B. Pre-existing
# lakehouse platform" -- same reasoning extends to their charts: this
# pipeline doesn't own their install, only extended their config).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHARTS_DIR="$SCRIPT_DIR/charts"
VALUES_DIR="$SCRIPT_DIR/values"
LIVE_DUMP_DIR="$SCRIPT_DIR/values-live-unredacted"
AUDIT_LOGGING_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

mkdir -p "$CHARTS_DIR" "$VALUES_DIR"

CHART_FAILED=()
CHART_OK=()
VALUES_FAILED=()
VALUES_OK=()

echo "##### 1. Helm repos #####"
# --force-update so this is safe to re-run even if a repo of the same
# name already points somewhere else on this machine.
helm repo add --force-update opensearch https://opensearch-project.github.io/helm-charts >/dev/null
helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator >/dev/null
helm repo add --force-update fluent https://fluent.github.io/helm-charts >/dev/null
helm repo add --force-update trino https://trinodb.github.io/charts/ >/dev/null
helm repo add --force-update superset https://apache.github.io/superset >/dev/null
helm repo update opensearch spark-operator fluent trino superset >/dev/null
echo "  done"

# pull_chart <repo/chart> <version>
# helm pull names the file "<chart>-<version>.tgz" itself -- matches
# this directory's existing charts/ naming exactly.
pull_chart() {
  local ref="$1" version="$2" chart_name
  chart_name="${ref#*/}"
  echo "=== $ref @ $version ==="
  if helm pull "$ref" --version "$version" -d "$CHARTS_DIR"; then
    CHART_OK+=("$chart_name-$version.tgz")
  else
    echo "  PULL FAILED"
    CHART_FAILED+=("$ref @ $version")
  fi
}

echo
echo "##### 2. Chart archives -> $CHARTS_DIR #####"
pull_chart opensearch/opensearch 3.8.0
pull_chart opensearch/opensearch-dashboards 3.8.0
pull_chart spark-operator/spark-operator 2.5.2
pull_chart fluent/fluent-bit 0.57.9
pull_chart trino/trino 1.42.2
pull_chart superset/superset 0.22.4

# --- Values handling -----------------------------------------------------
# Three different cases, matching README.md's "What's here and where it
# came from" note -- deliberately NOT one uniform `helm get values` loop:
#
#   (a) opensearch, opensearch-dashboards, fluent-bit: this repo already
#       carries the canonical values file elsewhere (../opensearch/,
#       ../fluent-bit/) and tracks it as the source of truth. No secrets
#       in any of the three. Safe to just copy straight into values/.
#   (b) spark-operator: no canonical file elsewhere, but confirmed secret-
#       free (just metrics/webhook toggles) -- safe to regenerate live and
#       overwrite values/ directly.
#   (c) trino, superset: pre-existing releases with their ENTIRE live
#       config pulled via `helm get values`, which -- confirmed directly,
#       see README.md's "Redacted secrets" section -- includes real LDAP
#       bind password, Trino's internal shared secret, bcrypt hashes,
#       MinIO keys, Superset's Flask secret, Postgres passwords. The
#       tracked values/*.yaml files have those hand-replaced with
#       <REDACTED-...> placeholders. A live re-dump must NEVER land
#       directly on top of those -- it goes to LIVE_DUMP_DIR instead,
#       untracked (see .gitignore), with a loud warning. Redact it by
#       hand (or diff against the tracked file to carry forward just the
#       real structural changes) before replacing values/*.yaml.

copy_values() {
  local src="$1" dest="$VALUES_DIR/$2"
  echo "=== values: $2 (copied from $src) ==="
  if [ ! -f "$src" ]; then
    echo "  SOURCE NOT FOUND: $src"
    VALUES_FAILED+=("$2 (source missing: $src)")
    return
  fi
  if cp "$src" "$dest"; then
    VALUES_OK+=("$2 (copied, no secrets)")
  else
    echo "  COPY FAILED"
    VALUES_FAILED+=("$2")
  fi
}

dump_values_safe() {
  local release="$1" namespace="$2" dest="$VALUES_DIR/$3"
  echo "=== values: $3 (live dump, confirmed secret-free) ==="
  if helm get values "$release" -n "$namespace" -o yaml > "$dest"; then
    VALUES_OK+=("$3 (live dump)")
  else
    echo "  DUMP FAILED"
    VALUES_FAILED+=("$3")
    rm -f "$dest"
  fi
}

dump_values_unredacted() {
  local release="$1" namespace="$2" fname="$3"
  local dest="$LIVE_DUMP_DIR/$fname"
  mkdir -p "$LIVE_DUMP_DIR"
  echo "=== values: $fname (LIVE DUMP, CONTAINS REAL SECRETS -> $LIVE_DUMP_DIR, not values/) ==="
  if helm get values "$release" -n "$namespace" -o yaml > "$dest"; then
    VALUES_OK+=("$fname (unredacted live dump -- manual redaction required, see summary)")
  else
    echo "  DUMP FAILED"
    VALUES_FAILED+=("$fname")
    rm -f "$dest"
  fi
}

echo
echo "##### 3. Values files #####"
copy_values "$AUDIT_LOGGING_DIR/opensearch/values.yaml" "opensearch-values.yaml"
copy_values "$AUDIT_LOGGING_DIR/opensearch/dashboards-values.yaml" "opensearch-dashboards-values.yaml"
copy_values "$AUDIT_LOGGING_DIR/fluent-bit/values.yaml" "fluent-bit-values.yaml"
dump_values_safe spark-operator spark-operator "spark-operator-values.yaml"
dump_values_unredacted trino default "trino-values.yaml"
dump_values_unredacted superset default "superset-values.yaml"

# Keep the unredacted dump out of git no matter what -- belt and
# suspenders alongside the human redaction step above.
GITIGNORE="$AUDIT_LOGGING_DIR/../.gitignore"
if [ -f "$GITIGNORE" ] && ! grep -qxF "audit-logging/install-guide/values-live-unredacted/" "$GITIGNORE"; then
  echo "audit-logging/install-guide/values-live-unredacted/" >> "$GITIGNORE"
  echo
  echo "(added values-live-unredacted/ to .gitignore)"
fi

echo
echo "===================== SUMMARY ====================="
echo "Charts saved to $CHARTS_DIR:"
printf '  %s\n' "${CHART_OK[@]}"
echo
echo "Values written to $VALUES_DIR:"
printf '  %s\n' "${VALUES_OK[@]}"
if [ "${#CHART_FAILED[@]}" -gt 0 ]; then
  echo
  echo "CHART PULL FAILURES (${#CHART_FAILED[@]}):"
  printf '  %s\n' "${CHART_FAILED[@]}"
fi
if [ "${#VALUES_FAILED[@]}" -gt 0 ]; then
  echo
  echo "VALUES FAILURES (${#VALUES_FAILED[@]}):"
  printf '  %s\n' "${VALUES_FAILED[@]}"
fi
if [ -d "$LIVE_DUMP_DIR" ] && [ -n "$(ls -A "$LIVE_DUMP_DIR" 2>/dev/null)" ]; then
  echo
  echo "!!! ACTION REQUIRED !!!"
  echo "$LIVE_DUMP_DIR/ contains a FRESH LIVE DUMP WITH REAL SECRETS"
  echo "(trino-values.yaml, superset-values.yaml). It is gitignored and"
  echo "must stay that way. To update the tracked, redacted files in"
  echo "$VALUES_DIR/:"
  echo "  1. diff each file in $LIVE_DUMP_DIR/ against its"
  echo "     counterpart in $VALUES_DIR/ to see what actually changed."
  echo "  2. Carry forward real structural changes by hand into the"
  echo "     $VALUES_DIR/ file, leaving every <REDACTED-...>"
  echo "     placeholder as-is (or re-redacting any new secret field)."
  echo "  3. Delete $LIVE_DUMP_DIR/ once done -- it should not"
  echo "     persist with real credentials sitting on disk."
  echo "See README.md's \"Redacted secrets\" section for exactly which"
  echo "fields need a placeholder."
fi
echo "====================================================="

if [ "${#CHART_FAILED[@]}" -gt 0 ] || [ "${#VALUES_FAILED[@]}" -gt 0 ]; then
  exit 1
fi
