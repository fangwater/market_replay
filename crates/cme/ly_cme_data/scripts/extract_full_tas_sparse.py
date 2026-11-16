#!/usr/bin/env python3
"""Read all production TAS CFs once; resumable sparse minutes stay on NVMe."""
from resource_runtime import enter, bounded_workers
if __name__ == '__main__':
    enter()

import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
import fcntl
import json
from pathlib import Path
import subprocess

ROOT=Path(__file__).resolve().parents[1]
RUN=ROOT/'artifacts/lseg_full_rocksdb_20261112'
WORK=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_full_rocksdb_20261112')
BIN=Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/tas_sparse_extract')


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary=path.with_suffix(path.suffix+'.partial')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    temporary.replace(path)


def extract(job):
    output=WORK/'extract'/job['id'];output.mkdir(parents=True,exist_ok=True)
    checkpoint=output/'state.json'
    if checkpoint.exists():
        record=json.loads(checkpoint.read_text())
        assert record['identity']['job']==job and record['identity']['current']==(Path(job['db'])/'CURRENT').read_text()
        return record
    with (output/'extract.log').open('a') as log:
        subprocess.run([str(BIN),'--plan',str(RUN/'plan.json'),'--job',job['id'],'--output',str(output)],
            check=True,stdout=log,stderr=subprocess.STDOUT)
    return json.loads(checkpoint.read_text())


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--workers',type=bounded_workers,default=2);parser.add_argument('--job',action='append');args=parser.parse_args()
    RUN.mkdir(parents=True,exist_ok=True);WORK.mkdir(parents=True,exist_ok=True)
    with (RUN/'extract.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        jobs=json.loads((RUN/'plan.json').read_text())['jobs']
        jobs=sorted((j for j in jobs if not args.job or j['id'] in args.job),key=lambda j:(-j['year'],j['product'],j['source_index']))
        done=[];pending=[];failures=[]
        for job in jobs:
            if (WORK/'extract'/job['id']/'state.json').exists():done.append(extract(job))
            else:pending.append(job)
        print(f'Resuming {len(done):,}/{len(jobs):,} completed source jobs; {len(pending):,} pending; workers={args.workers}',flush=True)
        save(RUN/'extract_state.json',dict(stage='running',jobs=len(jobs),completed=len(done),failed=[],
            trades=sum(r['trade_rows'] for r in done),sparse_minutes=sum(r['minute_rows'] for r in done)))
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures={pool.submit(extract,j):j for j in pending}
            for future in as_completed(futures):
                job=futures[future]
                try:
                    result=future.result();done.append(result)
                    print(f'{job["id"]}: {result["trade_rows"]:,} trades -> {result["minute_rows"]:,} sparse minutes',flush=True)
                except Exception as error:
                    import traceback
                    traceback.print_exc();failures.append(dict(job=job['id'],error=str(error)))
                save(RUN/'extract_state.json',dict(stage='running',jobs=len(jobs),completed=len(done),failed=failures,
                    trades=sum(r['trade_rows'] for r in done),sparse_minutes=sum(r['minute_rows'] for r in done)))
        save(RUN/'extract_state.json',dict(stage='failed' if failures else 'complete',jobs=len(jobs),completed=len(done),failed=failures,
            trades=sum(r['trade_rows'] for r in done),sparse_minutes=sum(r['minute_rows'] for r in done)))
        if failures:raise SystemExit(1)


if __name__=='__main__':main()
