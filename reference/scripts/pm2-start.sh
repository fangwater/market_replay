#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"

if [ -n "${PM2_BIN:-}" ] && [ -x "$PM2_BIN" ]; then
  PM2="$PM2_BIN"
elif command -v pm2 >/dev/null 2>&1; then
  PM2=$(command -v pm2)
elif command -v npm >/dev/null 2>&1 && [ -x "$(npm prefix --global)/bin/pm2" ]; then
  PM2="$(npm prefix --global)/bin/pm2"
else
  echo "error: PM2 is required; install it with: npm install --global pm2" >&2
  exit 1
fi

test -f .env || {
  echo "error: create .env from .env.example before starting PM2" >&2
  exit 1
}

"$PM2" startOrReload ecosystem.config.cjs --env production
"$PM2" save
