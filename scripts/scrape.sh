#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root (this script lives in recreacion_linux/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Activate venv
if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

# Ensure single instance using flock on lockfile
LOCKFILE="/tmp/recreacion_linux_scrape.lock"
exec 200>"$LOCKFILE"
flock -n 200 || { echo "Another scrape is running"; exit 0; }

# Run scrape (tune params via env or arguments)
python -m recreacion_linux.main scrape \
  --start-row "${START_ROW:-2}" \
  --end-row "${END_ROW:-}" \
  --only-empty "${ONLY_EMPTY:-true}" \
  --max-concurrency "${MAX_CONCURRENCY:-2}" \
  --rps "${RPS:-0.8}" \
  --retries "${RETRIES:-1}" \
  --timeout-ms "${TIMEOUT_MS:-25000}" \
  --batch-size "${BATCH_SIZE:-1500}" \
  --sleep-between-batches "${SLEEP_BETWEEN_BATCHES:-15.0}"
