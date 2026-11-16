#!/usr/bin/env bash
# PM2 app ib-kline-daily (namespace cn_futures_db): IB MySQL increment ->
# kline_ib.parquet + catalog IB tables, daily at 01:30 UTC (09:30 Shanghai),
# after the previous UTC day is complete. Separate from cn-daily / lseg-daily.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-cn_futures_db}"
BIN=/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release
ENV_FILE="${MARKET_REPLAY_ENV:-$HOME/.config/market_replay/env}"
for b in ib_kline_export lseg_catalog dingtalk-send; do
  [[ -x "$BIN/$b" ]] || { echo "[ERROR] $BIN/$b missing; run scripts/build-release.sh" >&2; exit 1; }
done
[[ "$(stat -c %a "$HOME/.config/market_replay/ib_mysql.env")" =~ ^[0-7]00$ ]] || { echo "[ERROR] ib_mysql.env must be chmod 600" >&2; exit 1; }
if [[ -f "$ENV_FILE" ]]; then
  [[ "$(stat -c %a "$ENV_FILE")" =~ ^[0-7]00$ ]] || { echo "[ERROR] $ENV_FILE must be chmod 600" >&2; exit 1; }
  set -a; . "$ENV_FILE"; set +a
else
  echo "[WARN] $ENV_FILE not found: DingTalk notifications disabled"
fi
if command -v pm2 >/dev/null 2>&1; then PM2=(pm2); else PM2=(npx pm2); fi
"${PM2[@]}" delete ib-kline-daily --namespace "$NAMESPACE" >/dev/null 2>&1 || true
"${PM2[@]}" start "$ROOT_DIR/scripts/run_ib_kline_daily.sh" --name ib-kline-daily --namespace "$NAMESPACE" \
  --cwd "$ROOT_DIR" --interpreter bash --cron-restart "30 1 * * *" --no-autorestart >/dev/null
"${PM2[@]}" save >/dev/null
echo "[INFO] ib-kline-daily cron 01:30 UTC (09:30 Asia/Shanghai); logs: ${PM2[*]} logs --namespace $NAMESPACE ib-kline-daily"
