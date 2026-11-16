#!/usr/bin/env python3
"""One-shot foreground handoff from an existing extractor to publication.

No service, daemon, restart or concurrent data stage. Waiting uses only stdlib;
the actual publisher starts through the normal Rust resource supervisor.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import sys
import time

from resource_runtime import LAUNCHER,process_identity,bounded_workers
from extract_full_tas_sparse import RUN,save


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--pid',type=int,required=True)
    parser.add_argument('--start-ticks',type=int,required=True);parser.add_argument('--workers',type=bounded_workers,default=2)
    args=parser.parse_args()
    with (RUN/'handoff.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        save(RUN/'handoff_state.json',dict(stage='waiting_for_extraction',pid=args.pid,start_ticks=args.start_ticks))
        while True:
            try:active=process_identity(args.pid)[2]==args.start_ticks
            except (OSError,ValueError,IndexError):active=False
            if not active:break
            time.sleep(5)
        state=json.loads((RUN/'extract_state.json').read_text())
        if state.get('stage')!='complete' or state.get('completed')!=state.get('jobs') or state.get('failed'):
            if state.get('stage')=='running':
                state['stage']='failed'
                state['stop_reason']='extraction process exited before completing its source checkpoints'
                failure=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/failures')/f'{args.pid}_{args.start_ticks}.json'
                if failure.exists():state['stop_reason']=json.loads(failure.read_text())['reason']
                save(RUN/'extract_state.json',state)
            save(RUN/'handoff_state.json',dict(stage='failed',error='source extraction did not complete; publisher not started'))
            raise SystemExit('Extraction is incomplete/failed; publication was not started')
        save(RUN/'handoff_state.json',dict(stage='starting_publisher',workers=args.workers))
        script=Path(__file__).with_name('build_full_tas_klines.py')
        os.execv(str(LAUNCHER),[str(LAUNCHER),'--',sys.executable,str(script),'--workers',str(args.workers)])


if __name__=='__main__':main()
