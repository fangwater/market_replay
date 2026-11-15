#!/usr/bin/env bash
set -euo pipefail
NAMESPACE="${PM2_NAMESPACE:-cn_futures_db}"
if command -v pm2 >/dev/null 2>&1; then PM2=(pm2); else PM2=(npx pm2); fi
"${PM2[@]}" delete cn-futures-kline cn-futures-daily --namespace "$NAMESPACE"
