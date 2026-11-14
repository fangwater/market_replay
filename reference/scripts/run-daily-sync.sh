#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

: "${CN_FUTURES_REF_DATABASE_URL:?CN_FUTURES_REF_DATABASE_URL is required}"
exec "${PYTHON_BIN:-python3}" scripts/daily_scheduler.py
