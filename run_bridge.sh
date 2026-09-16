#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -x ".venv/bin/python" ]; then
  rm -rf .venv
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install -e .

OLD_PIDS="$(lsof -tiTCP:8765 -sTCP:LISTEN 2>/dev/null || true)"
if [ -n "$OLD_PIDS" ]; then
  kill $OLD_PIDS 2>/dev/null || true
  for _ in {1..20}; do
    if ! lsof -tiTCP:8765 -sTCP:LISTEN >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  REMAINING_PIDS="$(lsof -tiTCP:8765 -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "$REMAINING_PIDS" ]; then
    kill -9 $REMAINING_PIDS 2>/dev/null || true
  fi
fi

exec python -m chatgpt_tab_bridge.server.bridge_server
