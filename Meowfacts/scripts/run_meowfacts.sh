#!/usr/bin/env bash
set -euo pipefail

# Optional: set workflow; leave empty to use MEOW_WORKFLOW_NAME env or default
# export MEOW_WORKFLOW_NAME=DAILY_ELT
# export MEOW_TARGET_LANGUAGES=

cd "$(dirname "$0")/.."
if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
else
  echo "Virtualenv not found at .venv/bin/activate" >&2
  exit 1
fi

python main.py
