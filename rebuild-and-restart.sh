#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "${BASH_SOURCE[0]}")/../mwaa-kafka-project/scripts/pipeline.sh" up
