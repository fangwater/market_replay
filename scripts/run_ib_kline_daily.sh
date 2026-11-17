#!/usr/bin/env bash
# Daily IB minute export (PM2 app ib-kline-daily): MySQL increment ->
# kline_ib.parquet on the delivery disk, then refresh the catalog IB tables.
# Independent of cn-daily / lseg-daily; sends its own DingTalk message.
set -uo pipefail
BIN=/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin
ROOT=/mnt/nfs/30.100_cta_data/lseg
LOG_DIR=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/ib_kline_daily
mkdir -p "$LOG_DIR"
RUN="$LOG_DIR/$(date -u +%Y%m%dT%H%M%SZ).log"

notify() { printf '%s\n' "$1" | "$BIN/dingtalk-send" || true; }

if ! nice -n 10 "$BIN/ib_kline_export" mysql --delivery-root "$ROOT" >"$RUN" 2>&1; then
  notify "【IB 分钟导出】失败
$(tail -n 3 "$RUN" | cut -c1-200)
日志：$RUN"
  exit 1
fi
# A normal run is silent; only failures are pushed (the log keeps the details).
FAILED=$(python3 - "$RUN" <<'PY'
import json, sys
t = open(sys.argv[1]).read()
d = json.loads(t[t.index('\n{') + 1:t.rindex('}') + 1])
print(len(d.get('failures', [])))
PY
)
if [ "${FAILED:-0}" != "0" ]; then
  notify "【IB 分钟导出】有 $FAILED 个失败
日志：$RUN"
fi
if ! "$BIN/lseg_catalog" ib-sync >>"$RUN" 2>&1; then
  notify "【IB 分钟导出】数据已导出，但 catalog 同步失败
日志：$RUN"
  exit 1
fi
