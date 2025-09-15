#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

LOCKFILE="/tmp/recreacion_linux_report.lock"
exec 202>"$LOCKFILE"
flock -n 202 || { echo "Another report is running"; exit 0; }

python -m recreacion_linux.main report \
  --start-row "${START_ROW:-2}" \
  --end-row "${END_ROW:-}" \
  --only-mismatches "${ONLY_MISMATCHES:-true}" \
  --prefix "${PREFIX:-Informe_}"
