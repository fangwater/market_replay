#!/usr/bin/env python3
"""Apply witnessed revisions now, then finish automatically after source recovery."""
import fcntl
import json
import subprocess
import sys
import time
import traceback

import build_market_status_klines as pipeline
from prepare_trade_revisions import RUN,WORK

SERVICE='ly-cme-correction-identities-historical-fast.service'


def run(script,*args):
    subprocess.run([sys.executable,str(pipeline.SCRIPTS/script),*args],cwd=pipeline.SCRIPTS,check=True)


def main():
    with (RUN/'backfill_runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        issue=pipeline.DEST/'meta/problems/tas_trade_revision_runner_failure.json'
        try:
            run('compile_trade_revisions.py','--allow-pending-sources')
            run('apply_kline_trade_revisions.py','--workers','4')
            pipeline.save(RUN/'witnessed_stage.json',dict(
                compilation=json.loads((RUN/'compiled_summary.json').read_text()),
                backfill=json.loads((RUN/'backfill_summary.json').read_text())))
            while True:
                jobs=json.loads((RUN/'identity_jobs_historical.json').read_text())
                pending=[j['id'] for j in jobs if not (WORK/'identities'/(j['id']+'.done.json')).exists()]
                if not pending:break
                status=subprocess.run(['systemctl','--user','show',SERVICE,'-p','ActiveState','-p','SubState'],capture_output=True,text=True,check=True).stdout
                if 'ActiveState=active\n' not in status:raise RuntimeError(f'Historical identity recovery stopped with {len(pending)} jobs missing: {status}')
                # Service-local wait, no user-facing success/progress files.
                time.sleep(10)
            run('compile_trade_revisions.py')
            run('apply_kline_trade_revisions.py','--workers','4')
            pipeline.save(RUN/'final_state.json',dict(
                compilation=json.loads((RUN/'compiled_summary.json').read_text()),
                backfill=json.loads((RUN/'backfill_summary.json').read_text())))
            issue.unlink(missing_ok=True)
        except Exception as error:
            pipeline.save(issue,dict(problem=str(error),traceback=traceback.format_exc()));raise


if __name__=='__main__':main()
