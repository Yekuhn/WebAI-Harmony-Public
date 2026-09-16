#!/usr/bin/env bash
set -euo pipefail

CHROME_APP="${CHROME_APP:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROFILE_DIR="${CHROME_PROFILE_DIR:-$SCRIPT_DIR/manual_chrome_profile}"
PORT="${CHROME_REMOTE_DEBUGGING_PORT:-9222}"

mkdir -p "$PROFILE_DIR"
if curl --silent --max-time 1 "http://127.0.0.1:${PORT}/json/version" >/dev/null 2>&1; then
  echo "Chrome Power Mode is already running on http://127.0.0.1:$PORT"
  exit 0
fi
"$CHROME_APP" \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port="$PORT" \
  --user-data-dir="$PROFILE_DIR" \
  --no-first-run \
  "about:blank" &

echo "Chrome Power Mode launched on http://127.0.0.1:$PORT"
