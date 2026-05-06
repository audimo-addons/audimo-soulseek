#!/usr/bin/env bash
# Run the audimo-soulseek addon natively.
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
  echo "Creating venv (.venv/)..."
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi

echo "[run] starting audimo-soulseek on http://0.0.0.0:9008"

exec .venv/bin/uvicorn server:app \
  --host 0.0.0.0 \
  --port 9008 \
  --proxy-headers \
  --no-access-log \
  --reload \
  --reload-dir "$(pwd)" \
  --reload-exclude ".venv/*"
