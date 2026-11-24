#!/usr/bin/env bash
# TAS trade / quote parquet (ric-day layout) for whole UTC years from the main TAS RocksDB into the delivery tree.
#   run_tas_tick_years.sh 2025 2024 ... (newest first)
# Per year: exchange map -> lseg_tas_parquet (staged on HDD) -> lseg_tas_publish (8 threads, gated, resumable) -> stage removed.
# Re-running a year skips files that are already delivered (identical bytes are left alone).
set -u
BIN=${BIN:-/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release}
RT=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/tas_ticks
STAGE=/mnt/hdd-raid5-72t/fanghaizhou/lseg_data/tas_tick_stage
REPO=/home/fanghaizhou/market_replay
log() { echo "$(date -u +%FT%TZ) $*" >> $RT/run.log; }
for y in "$@"; do
  log "year $y start"
  python3 -I $REPO/scripts/lseg/tas_tick_exchange_map.py --year $y --out $RT/map_$y.json >> $RT/run.log 2>&1 || { log "year $y map FAILED"; exit 1; }
  out=$STAGE/$y
  if [ ! -f $out/_SUCCESS.json ]; then
    rm -rf $out
    nice -n 10 $BIN/lseg_tas_parquet --year $y --period-end $((y+1))-01-01 --output $out --workers 8 --layout ric-day \
      --exchange-map $RT/map_$y.json --readback sampled > $RT/export_$y.log 2>&1 || { log "year $y export FAILED"; exit 1; }
  fi
  log "year $y exported: $(python3 -I -c "import json;s=json.load(open('$out/_SUCCESS.json'));print(s['files'],'files',s['bytes'],'bytes',s['trade_rows'],'trade rows',s['quote_rows'],'quote rows')")"
  nice -n 10 $BIN/lseg_tas_publish --export $out --progress $RT/publish_$y.json --threads 8 >> $RT/publish_$y.log 2>&1 || { log "year $y publish FAILED"; exit 1; }
  log "year $y published: $(tail -1 $RT/publish_$y.log)"
  rm -rf $out
done
log "ALL DONE"
