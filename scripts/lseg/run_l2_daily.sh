#!/usr/bin/env bash
# Daily L2 250 ms for the newest UTC days (server job `l2-daily`):
#   download the new Normalized LL2 daily packages -> wash into the day RocksDBs -> deliver parquet to the
#   delivery tree -> incremental catalog update -> record the washed sources.
# Re-runs a short window (the last WINDOW_DAYS days) every time: files that are already there are skipped at
# every step, so a file that arrived late is picked up. Only failures are pushed (DingTalk).
# One-off backfill limits (8 workers) are NOT used here: the daily job runs with the default 2.
set -uo pipefail
BIN=${BIN:-/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin}
REPO=/home/fanghaizhou/market_replay
RT=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/l2_daily
DBROOT=/mnt/hdd-raid5-72t/fanghaizhou/lseg_data/l2_db
LSEG=/mnt/nfs/30.100_cta_data/lseg
ART=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts
WINDOW_DAYS=${WINDOW_DAYS:-3}
mkdir -p "$RT"
RUN="$RT/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
LOG="$RUN/run.log"
notify() { printf '%s\n' "$1" | "$BIN/dingtalk-send" || true; }
fail() { notify "【L2 每日】$1
日志：$RUN"; echo "FAILED: $1" >> "$LOG"; exit 1; }
log() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

STATE="$RT/state.json"
LAST=$(python3 -I -c "import json;print(json.load(open('$STATE'))['last_done'])" 2>/dev/null || echo "2026-11-25")
END=$(date -u -d "yesterday" +%F)
START=$(date -u -d "$LAST - $((WINDOW_DAYS - 1)) days" +%F)
[ "$START" \> "$END" ] && { log "nothing to do (last_done $LAST, yesterday $END)"; exit 0; }
DAYS=$(python3 -I -c "
import datetime as d
a=d.date.fromisoformat('$START');b=d.date.fromisoformat('$END');print(','.join(str(a+d.timedelta(i)) for i in range((b-a).days+1)))")
log "window $START..$END (last_done $LAST)"

# products that have a daily L2 package on the remote
python3 -I - "$RUN/products.txt" <<'PY' || fail "产品清单取不到"
import re, sys, urllib.request
op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
base = "http://10.61.20.12/public/users/u158/public/lseg_data/future_daily/normalisedLL2/"
html = op.open(base, timeout=60).read().decode()
prods = sorted(p for p in set(re.findall(r'href="([^"/?]+)/"', html)) if not p.startswith("."))
open(sys.argv[1], "w").write("\n".join(prods) + "\n")
PY

SETTLE_MINUTES=60 nice -n 10 "$REPO/scripts/lseg/fetch_normalised_ll2.sh" "$RUN/products.txt" "$DAYS" > "$RUN/fetch.log" 2>&1
GOT=$(grep -c '^GOT' "$RUN/fetch.log"); ERR=$(grep -c '^ERR\|^DIFF' "$RUN/fetch.log")
log "fetched $GOT files, $ERR errors, $(grep -c '^WAIT' "$RUN/fetch.log") not settled yet"
[ "$ERR" -gt 0 ] && notify "【L2 每日】有 $ERR 个文件下载失败（其余照常处理）
日志：$RUN"
# the wash skips files younger than 10 minutes
[ "$GOT" -gt 0 ] && { log "waiting 11 minutes for the downloads to settle"; sleep 660; }

nice -n 10 "$BIN/lseg_l2_slice" backfill --packages none --daily-start "$START" --daily-end "$END" --db-root "$DBROOT" \
  --workers 2 --producers 2 --sst-zstd-level 6 --progress "$RUN/wash_progress.json" --gaps "$RUN/wash_gaps.json" --no-gate \
  > "$RUN/wash.log" 2> "$RUN/wash.err" || fail "洗库失败：$(tail -n 2 "$RUN/wash.err" | cut -c1-200)"
log "wash: $(tail -n 1 "$RUN/wash.log" | cut -c1-200)"

python3 -I "$REPO/scripts/lseg/l2_exchange_map.py" ALL --plan "$ART/tas_status_20261109/plan.json" \
  --ric-catalog "$ART/lseg_exchange_groups_20261109/ric_catalog.csv" --conflicts "$RUN/exchange_conflicts.tsv" \
  > "$RUN/exchange_map.tsv" 2> "$RUN/exchange_map.err" || fail "交易所映射生成失败"

nice -n 10 "$BIN/lseg_l2_slice" deliver --db-root "$DBROOT" --exchange-map "$RUN/exchange_map.tsv" \
  --exchange-conflicts "$RUN/exchange_conflicts.tsv" --from "$START" --to "$END" --extract-workers 2 --publish-threads 8 \
  --batch-files 3000 --staging "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/l2_deliver_stage/daily" \
  --progress "$RUN/deliver_progress.json" --ric-years-out "$RUN/ric_years.tsv" --allow-mixed --no-gate \
  > "$RUN/deliver.json" 2> "$RUN/deliver.err" || fail "交付失败：$(tail -n 2 "$RUN/deliver.err" | cut -c1-200)"
PROBLEMS=$(python3 -I -c "import json,sys;t=open('$RUN/deliver.json').read();print(json.loads(t[t.index('{'):]).get('problems',0))" 2>/dev/null || echo "?")
log "deliver: problems=$PROBLEMS"
[ "$PROBLEMS" != "0" ] && notify "【L2 每日】交付有 $PROBLEMS 个问题（见 lseg/meta/problems/l2_deliver）
日志：$RUN"

if [ -s "$RUN/ric_years.tsv" ]; then
  "$BIN/lseg_catalog" l2-sync --ric-years "$RUN/ric_years.tsv" >> "$LOG" 2>&1 || fail "目录索引更新失败"
fi
python3 -I "$REPO/scripts/lseg/l2_washed_sources.py" --out "$RUN/l2_sources.sqlite" >> "$LOG" 2>&1 \
  && python3 -I "$REPO/scripts/lseg/l2_washed_sources.py" --publish "$RUN/l2_sources.sqlite" --replace >> "$LOG" 2>&1 \
  || notify "【L2 每日】洗过的源记录没更新成功（数据已交付）
日志：$RUN"
python3 -I -c "import json;json.dump({'last_done':'$END','run':'$RUN'},open('$STATE','w'))"
log "done through $END"
find "$RT" -maxdepth 1 -type d -name '20*' -mtime +14 -exec rm -rf {} + 2>/dev/null
