#!/usr/bin/env bash
# Run only against the isolated supplement DB. Completed outputs refuse replay.
set -euo pipefail
task_repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$task_repo"
task_run="$task_repo/artifacts/tas_supplement_ingest_20261103"
task_bin=/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release
task_db=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_supplement_rocksdb_2026
task_manifest=/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_supplement_shards_2026/shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30/manifest.json
exec 9>"$task_run/replay_runner.lock"
flock -n 9 || { echo 'Supplement replay runner already active' >&2; exit 1; }
if [[ ${1:-} == --wait-for-shards ]]; then
    task_deadline=$((SECONDS + 1800))
    while [[ ! -f $task_manifest ]]; do
        (( SECONDS < task_deadline )) || { echo 'Timed out waiting for complete shards' >&2; exit 1; }
        sleep 2
    done
fi
python - "$task_run" "$task_manifest" <<'PY'
import json,sys
from pathlib import Path
run, manifest = map(Path, sys.argv[1:])
assert not (run/'completion.json').exists(), 'completed run; refuse to replay'
for name in ['sample_baseline.json','sample_shard_equivalence.json']:
    assert json.loads((run/name).read_text())['status']=='passed'
data=json.loads(manifest.read_text())
assert data['complete'] and all(s['complete'] for s in data['sources'])
assert sum(s['data_rows'] for s in data['sources'])==710632866
PY
"$task_bin/cme_tas_replay_all" --config config/tas_supplement_2026.toml > "$task_run/replay.stdout.log" 2>&1
"$task_bin/tas_db_audit" --db "$task_db" --require-done 2026-01-01_2026-05-30 \
    --output "$task_run/db_audit.json" > "$task_run/audit.stdout.log" 2>&1
python scripts/finalize_tas_supplement_ingest.py "$task_run" > "$task_run/finalize.stdout.log" 2>&1
