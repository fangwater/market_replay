#!/usr/bin/env bash
# Ensures the size-class threshold files of one SOURCE month for every product:
#   <dir>/dominant_by_day/<YYYYMM>/<PRODUCT>.json
# P50 / P90 of the interval amounts of the DOMINANT contract of each trading day
# of that complete Shanghai calendar month (merged across rolls). The same files
# are read by baseline_data_1min step 4 (fill_1min_size_buckets) and by the live
# gRPC bars: a trade of month M is classed with the thresholds of month M-1.
#
#   scripts/export_size_thresholds.sh [YYYYMM]     (default: previous calendar month)
#
# Read-only on RocksDB, at most 4 products at a time. Existing valid files are
# kept (FORCE=1 recomputes); the tool writes each file atomically. Products with
# no usable dominant trades in the month (new listing, no samples) are listed in
# <dir>/dominant_by_day/<YYYYMM>/_failed.txt; both consumers then leave the size
# columns unset/untouched for them (thresholds are never invented).
set -euo pipefail
export TZ=Asia/Shanghai
shopt -s nullglob
DEPLOY=/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin
BIN="${FILL_BIN:-$DEPLOY/fill_1min_size_buckets}"
OUT="${SIZE_THRESHOLD_DIR:-/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/size_thresholds}"
BASE="${BASELINE_ROOT:-/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min}"
RDB="${ROCKSDB_DIR:-/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/cn_l2_rocksdb}"
DOM="${DOMINANT_DIR:-/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/roll/result}"
JOBS="${JOBS:-4}"
(( JOBS >= 1 && JOBS <= 4 )) || JOBS=4

ym="${1:-$(date -d "$(date +%Y-%m-01) -1 day" +%Y%m)}"
[[ "$ym" =~ ^20[0-9]{2}(0[1-9]|1[0-2])$ ]] || { echo "bad month: $ym" >&2; exit 2; }
[[ -x "$BIN" ]] || { echo "missing $BIN" >&2; exit 2; }
dir="$OUT/dominant_by_day/$ym"
mkdir -p "$dir"

export_one() {
  local p="$1" err force=()
  [[ "${FORCE:-0}" == 1 ]] && force=(--force)
  if err=$("$BIN" --rocksdb-dir "$RDB" --product "$p" --export-month "$ym" \
        --threshold-dir "$OUT" --dominant-dir "$DOM" "${force[@]}" 2>&1 >/dev/null); then
    echo "ok   $p"
    return 0
  fi
  echo "FAIL $p: $(printf '%s' "$err" | tail -1 | cut -c1-160)"
  return 1
}
export -f export_one
export BIN RDB OUT DOM ym FORCE

products=$(for e in "$BASE"/*/; do
  [[ "$(basename "$e")" == _* ]] && continue
  for p in "$e"*/; do b=$(basename "$p"); [[ "$b" == _* ]] || echo "$b"; done
done | sort -u)
[[ -n "$products" ]] || { echo "no products under $BASE" >&2; exit 2; }
total=$(echo "$products" | wc -l)
log=$(mktemp)
set +e
echo "$products" | xargs -P "$JOBS" -I{} bash -c 'export_one "$1"' _ {} | tee "$log"
set -e
failed=$(grep -c '^FAIL' "$log" || true)
grep '^FAIL' "$log" | sed 's/^FAIL //' | sort > "$dir/_failed.txt" || true
[[ -s "$dir/_failed.txt" ]] || rm -f "$dir/_failed.txt"
good=$(( total - failed ))
rm -f "$log"
echo "size thresholds $ym: $good/$total products ok, $failed without thresholds (see $dir/_failed.txt)"
# Some products legitimately have no trades in a month; only a total failure is an error.
(( good > 0 ))
