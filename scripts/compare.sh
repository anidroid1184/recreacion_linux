#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

LOCKFILE="/tmp/recreacion_linux_compare.lock"
exec 201>"$LOCKFILE"
flock -n 201 || { echo "Another compare is running"; exit 0; }

python -m recreacion_linux.main compare \
  --start-row "${START_ROW:-2}" \
  --end-row "${END_ROW:-}" \
  --only-mismatches "${ONLY_MISMATCHES:-true}"
