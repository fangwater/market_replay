#!/usr/bin/env bash
# PM2 entry for the domestic futures metadata PostgreSQL. At boot PM2 resurrect
# can run before the NVMe RAID is mounted: wait for the data directory, then
# exec postgres in the foreground so PM2 signals reach the postmaster.
set -euo pipefail
PG_BIN=/mnt/nvme-raid0-28t/apps/pgsql16/bin
PG_DATA=/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/main
for _ in $(seq 1 120); do
  [[ -x "$PG_BIN/postgres" && -f "$PG_DATA/PG_VERSION" ]] && break
  sleep 5
done
[[ -x "$PG_BIN/postgres" && -f "$PG_DATA/PG_VERSION" ]]
exec "$PG_BIN/postgres" -D "$PG_DATA"
