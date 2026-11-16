#!/usr/bin/env bash
# Environment-local lifecycle; retain config, inspect active jobs and archive logs.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
action="${1:-status}"
[[ "$action" == status || "$action" == start || "$action" == restart || "$action" == stop ]] || exit 2
if [[ "$action" != status && "${2:-}" != --execute ]]; then
  echo "[DRY-RUN] env=$ROOT_DIR service=market-replay-server action=$action scope=read-only-market-data-services"
  exit 0
fi
cd "$ROOT_DIR"
python3 - "$ROOT_DIR" "$action" <<'PY'
from pathlib import Path
import datetime,json,shutil,subprocess,sys,urllib.request
base=Path(sys.argv[1]);action=sys.argv[2]
r=subprocess.run(['pm2','jlist'],text=True,capture_output=True,check=True);rows=json.loads(r.stdout[r.stdout.index('['):]);rows=[p for p in rows if p['name']=='market-replay-server']
if action=='status':
 print(json.dumps([{'name':p['name'],'pid':p['pid'],'status':p['pm2_env']['status']} for p in rows]));sys.exit(0)
for p in rows:
 if Path(p['pm2_env']['pm_exec_path'])!=base/'target/release/market-replay-server':raise RuntimeError('registered server belongs to another environment')
if rows and rows[0]['pid']:
 with urllib.request.urlopen('http://127.0.0.1:18313/api/admin/status',timeout=5) as response:state=json.load(response)
 jobs=state['jobs'];jobs=list(jobs.values()) if isinstance(jobs,dict) else jobs
 if any(j.get('running') or j.get('queued') for j in jobs):raise RuntimeError('keep current server: a data job is queued or running')
backup=base/'data/service_stops'/datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');backup.mkdir(parents=True,exist_ok=True,mode=0o700)
for p in rows:
 for key in ['pm_out_log_path','pm_err_log_path']:
  source=Path(p['pm2_env'][key])
  if source.is_file():shutil.copy2(source,backup/source.name)
if action=='stop':subprocess.run(['pm2','delete','market-replay-server'],check=True)
else:
 subprocess.run([str(base/'target/release/market-replay-server'),'--config',str(base/'config/market_replay_server.toml'),'--check'],check=True)
 subprocess.run(['pm2','startOrRestart',str(base/'deploy/pm2/market-replay-server.config.cjs'),'--only','market-replay-server'],check=True)
subprocess.run(['pm2','save','--force'],check=True,stdout=subprocess.DEVNULL)
PY
