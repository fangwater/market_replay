#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"

# A host/port set by the supervisor (market-replay-server runs this as the
# 127.0.0.1:18766 fallback behind the Rust API) wins over the .env values.
preset_host="${CN_FUTURES_REF_HOST-}"
preset_port="${CN_FUTURES_REF_PORT-}"
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi
[ -z "$preset_host" ] || CN_FUTURES_REF_HOST="$preset_host"
[ -z "$preset_port" ] || CN_FUTURES_REF_PORT="$preset_port"
export CN_FUTURES_REF_HOST CN_FUTURES_REF_PORT

: "${CN_FUTURES_REF_DATABASE_URL:?CN_FUTURES_REF_DATABASE_URL is required}"
exec "${PYTHON_BIN:-python3}" -m cn_futures_ref.server
