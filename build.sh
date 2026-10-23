#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../mwaa-kafka-project"
case "${1:-}" in
  build-all) podman compose -f compose.yaml build; ./scripts/astro.sh dev build ;;
  run-producer) podman compose -f compose.yaml up -d --build producer ;;
  submit-flink-job) podman compose -f compose.yaml up -d --build jobmanager taskmanager ;;
  *) echo "Usage: $0 {build-all|run-producer|submit-flink-job}" >&2; exit 2 ;;
esac
