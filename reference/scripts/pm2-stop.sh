#!/bin/sh
set -eu

if [ -n "${PM2_BIN:-}" ] && [ -x "$PM2_BIN" ]; then
  PM2="$PM2_BIN"
elif command -v pm2 >/dev/null 2>&1; then
  PM2=$(command -v pm2)
elif command -v npm >/dev/null 2>&1 && [ -x "$(npm prefix --global)/bin/pm2" ]; then
  PM2="$(npm prefix --global)/bin/pm2"
else
  echo "error: PM2 is required" >&2
  exit 1
fi

"$PM2" stop cn-futures-reference-api cn-futures-reference-daily-sync
