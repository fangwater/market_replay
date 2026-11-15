#!/usr/bin/env bash
# Starts (or restarts) the CN futures post-market services under PM2:
#   cn-futures-kline  gRPC cn_market.v1 minute bars (long-running)
#   cn-futures-daily  daily RocksDB + baseline_data_1min job (PM2 cron, 16:30 Asia/Shanghai)
#
# Secrets (DingTalk webhook / signing secret) are read from a private env file
# outside the repository: ${MARKET_REPLAY_ENV:-$HOME/.config/market_replay/env}
#   CN_FUTURES_DB_DINGTALK_WEBHOOK_URL=https://oapi.dingtalk.com/robot/send?access_token=...
#   CN_FUTURES_DB_DINGTALK_SECRET=SEC...
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 0 ]]; then
  echo "Usage: scripts/start_cn_services.sh" >&2
  exit 2
fi
NAMESPACE="${PM2_NAMESPACE:-cn_futures_db}"
BIN="$ROOT_DIR/target/release"
ENV_FILE="${MARKET_REPLAY_ENV:-$HOME/.config/market_replay/env}"
for b in cn-kline-server cn-daily cn_futures_l2 export_baseline_1min trim_cn_futures_continuous_sessions fill_1min_size_buckets; do
  [[ -x "$BIN/$b" ]] || { echo "[ERROR] $BIN/$b missing; run scripts/build-release.sh" >&2; exit 1; }
done
if [[ -f "$ENV_FILE" ]]; then
  [[ "$(stat -c %a "$ENV_FILE")" =~ ^[0-7]00$ ]] || { echo "[ERROR] $ENV_FILE must not be group/world readable (chmod 600)" >&2; exit 1; }
  set -a; . "$ENV_FILE"; set +a
else
  echo "[WARN] $ENV_FILE not found: DingTalk notifications disabled"
fi
if command -v pm2 >/dev/null 2>&1; then PM2=(pm2); else PM2=(npx pm2); fi

"${PM2[@]}" delete cn-futures-kline --namespace "$NAMESPACE" >/dev/null 2>&1 || true
"${PM2[@]}" start "$BIN/cn-kline-server" --name cn-futures-kline --namespace "$NAMESPACE" \
  --cwd "$ROOT_DIR" --interpreter none -- --config "$ROOT_DIR/config/cn_kline_server.toml" >/dev/null

# PM2 cron uses the host clock (UTC): 08:30 UTC = 16:30 Asia/Shanghai.
"${PM2[@]}" delete cn-futures-daily --namespace "$NAMESPACE" >/dev/null 2>&1 || true
"${PM2[@]}" start "$BIN/cn-daily" --name cn-futures-daily --namespace "$NAMESPACE" \
  --cwd "$ROOT_DIR" --interpreter none --cron-restart "30 8 * * *" --no-autorestart \
  -- run --config "$ROOT_DIR/config/cn_daily.toml" >/dev/null
"${PM2[@]}" save >/dev/null
echo "[INFO] cn-futures-kline on $(awk -F'"' '/^listen/ {print $2}' "$ROOT_DIR/config/cn_kline_server.toml"); cn-futures-daily cron 16:30 Asia/Shanghai"
echo "Logs: ${PM2[*]} logs --namespace $NAMESPACE cn-futures-daily"
