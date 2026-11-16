#!/usr/bin/env python3
"""Run the pinned daily-source snapshot through delivery and final verification."""
from datetime import datetime, timezone
import fcntl
import subprocess
import sys

from process_tas_daily import ROOT, RUN, DEST, BATCH, SCRIPTS
from download_tas_daily import save


def status(value, **extra):
    result=dict(status=value,updated_at=datetime.now(timezone.utc).isoformat(),**extra)
    save(RUN/'finalize_wait_progress.json',result)
    if value in ['stopped_without_batch_completion','final_verification_failed']:
        save(DEST/'lseg/meta/problems'/BATCH/'pipeline.json',result)
        print(result,flush=True)


def main():
    lock=(RUN/'delivery_runner.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    status('waiting_for_verified_batch_completion',layout='ric_utc_day_v1')
    result=subprocess.run([sys.executable,str(SCRIPTS / 'process_tas_daily.py'),*sys.argv[1:]],cwd=SCRIPTS)
    if result.returncode:
        status('stopped_without_batch_completion',returncode=result.returncode,
               reason='Processing did not pass; final verification not run. See process log and product failure receipts.')
        return result.returncode
    status('running_final_verification',layout='ric_utc_day_v1')
    result=subprocess.run([sys.executable,str(SCRIPTS / 'finalize_tas_daily.py')],cwd=SCRIPTS)
    status('final_verification_passed' if result.returncode==0 else 'final_verification_failed',returncode=result.returncode)
    return result.returncode


if __name__=='__main__':
    raise SystemExit(main())
