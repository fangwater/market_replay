#!/usr/bin/env python3
"""Recover source identifiers lost by the slim codec; no source mutations."""
import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pandas as pd
import pyarrow.parquet as pq

from prepare_trade_revisions import RUN,WORK
import build_market_status_klines as pipeline

DEPS=WORK/'python_deps'
PERIOD=pipeline.DATA/'future/normalised/shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2026-01-01_2026-06-01'


def stamp(ns):
    seconds,nanos=divmod(int(ns),1_000_000_000)
    return pd.Timestamp(seconds,unit='s',tz='UTC').strftime('%Y-%m-%dT%H:%M:%S')+f'.{nanos:09d}Z'


def extract(job,allowed):
    target=WORK/'identities'/(job['id']+'.jsonl')
    done=target.with_suffix('.done.json')
    pattern_hash=hashlib.sha256(Path(job['patterns']).read_bytes()).hexdigest()
    header_hash=hashlib.sha256(json.dumps(job['header'],ensure_ascii=False).encode()).hexdigest()
    if done.exists():
        previous=json.loads(done.read_text())
        old_zero_daily=not previous.get('rows') and job['header'].index('Type')!=4 and previous.get('selector_version')!=2
        if not old_zero_daily and previous.get('pattern_hash',pattern_hash)==pattern_hash and previous.get('header_hash',header_hash)==header_hash:return previous
    target.parent.mkdir(parents=True,exist_ok=True)
    temporary=target.with_suffix('.jsonl.partial')
    environment=os.environ.copy();environment['PYTHONPATH']=str(DEPS)
    if job['format']=='gzip':
        command=[str(DEPS/'bin/rapidgzip'),'-d','-c','-P','8',str(job['path'])]
    else:command=['zstd','-dc',str(job['path'])]
    log=target.with_suffix('.stderr.log')
    header_path=target.with_suffix('.header.json');header_path.write_text(json.dumps(job['header'],ensure_ascii=False))
    count=0
    with temporary.open('w') as output,log.open('w') as errors:
        decoder=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=errors,env=environment)
        selector=subprocess.Popen([str(pipeline.Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/tas_identity_select')),
            '--plan',str(RUN/'db_plan.json'),'--patterns',str(job['patterns']),'--header-json',str(header_path)],stdin=decoder.stdout,stdout=subprocess.PIPE,stderr=errors)
        decoder.stdout.close()
        try:
            for line in selector.stdout:
                number,_,raw=line.partition(b':')
                # Cheap prefix/type checks before parsing hundreds of CSV cells.
                ric=raw.split(b',',1)[0].decode('ascii')
                if ric not in allowed:continue
                fields=next(csv.reader([raw.decode('utf-8').rstrip('\r\n')]))
                if len(fields)!=len(job['header']):raise ValueError((job['id'],number,len(fields),len(job['header'])))
                row=dict(zip(job['header'],fields))
                if row['Type'] not in ('Trade','Correction'):continue
                if row['Type']=='Trade' and (not row.get('Price') or not row.get('Volume')):continue
                output.write(json.dumps(dict(source=str(job['path']),line=int(number),fields={k:v for k,v in row.items() if v}),ensure_ascii=False)+'\n')
                count+=1
            assert selector.wait()==0,(job['id'],'selection failed')
            assert decoder.wait()==0,(job['id'],'decode failed')
        except BaseException:
            selector.terminate();decoder.terminate();selector.wait();decoder.wait();raise
    temporary.replace(target)
    result=dict(id=job['id'],rows=count,source=str(job['path']),output=str(target),pattern_hash=pattern_hash,header_hash=header_hash,selector_version=2)
    pipeline.save(done,result);return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--workers',type=int,default=2);parser.add_argument('--historical-only',action='store_true');parser.add_argument('--daily-only',action='store_true');args=parser.parse_args()
    corrections=json.loads((RUN/'corrections.json').read_text())['corrections']
    allowed=set(json.loads((RUN/'db_plan.json').read_text())['rics'])
    groups={};jobs=[]
    for row in corrections:
        if row['source']=='daily':groups.setdefault((row['cf'].split(':')[-1],row['part']),set()).add(stamp(row['published_ns']))
    historical_stamps=set()
    for row in corrections:
        if row['source']=='historical':historical_stamps.add(stamp(row['published_ns']))
    for path in (WORK/'candidates').glob('*.parquet'):
        table=pq.ParquetFile(path).read(columns=['published_ns','filename'])
        for ns,name in zip(table['published_ns'].to_pylist(),table['filename'].to_pylist()):
            if '/tas_tick_parquet/2026/' in name:historical_stamps.add(stamp(ns))
            else:
                product=name.split('/daily_20261109/',1)[1].split('/',1)[0]
                day=Path(name).parent.name
                groups.setdefault((product,day),set()).add(stamp(ns))
    pattern_root=WORK/'identity_patterns';pattern_root.mkdir(parents=True,exist_ok=True)
    historical=pattern_root/'historical.txt'
    historical.write_text(',Correction,\n'+'\n'.join(sorted(historical_stamps))+'\n')
    with gzip.open(PERIOD/'merged-Data-part-000000.csv.gz','rt',newline='') as stream:header=next(csv.reader(stream))
    if not args.daily_only:
        for path in sorted(PERIOD.glob('merged-Data-part-*.csv.gz')):
            jobs.append(dict(id='historical_'+path.name.split('-part-')[1].split('.')[0],path=path,format='gzip',patterns=historical,header=header))
    manifests={}
    if not args.historical_only:
        for (product,part_or_day),times in groups.items():
            if product not in manifests:
                receipt=json.loads((pipeline.ROOT/'artifacts/tas_daily_ingest_20261109/products'/product/'staging_receipt.json').read_text())
                path=Path(receipt['manifest']);manifests[product]=(path,json.loads(path.read_text()))
            path,manifest=manifests[product]
            if isinstance(part_or_day,int):part=part_or_day
            else:
                date=pd.Timestamp(part_or_day).strftime('%Y-%m-%d')
                found=[s['original_part'] for s in manifest['sources'] if f'_{date}_' in s['file']]
                if not found:continue
                part=found[0]
            key=(product,part)
            if any(job['id']==f'daily_{product}_{part:06d}' for job in jobs):continue
            merged=set(times)|groups.get(key,set())
            source=next(s for s in manifest['sources'] if s['original_part']==part)
            day=pd.Timestamp(Path(source['file']).name.split('_')[1]).strftime('%Y%m%d')
            merged|=groups.get((product,day),set())
            patterns=pattern_root/f'{product}_{part:06d}.txt';patterns.write_text(',Correction,\n'+'\n'.join(sorted(merged))+'\n')
            for shard in manifest['shards']:
                if shard['original_part']==part:
                    jobs.append(dict(id=f'daily_{product}_{part:06d}',path=path.parent/shard['file'],format='zstd',patterns=patterns,header=next(csv.reader([manifest['header']]))))
    label='historical' if args.historical_only else 'daily' if args.daily_only else 'all'
    pipeline.save(RUN/f'identity_jobs_{label}.json',[{**j,'path':str(j['path']),'patterns':str(j['patterns'])} for j in jobs])
    # Daily files are small and already zstd; recover them while annual scans run.
    jobs.sort(key=lambda job:(job['format']=='gzip',job['id']))
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures=[pool.submit(extract,job,allowed) for job in jobs]
        for future in as_completed(futures):print(json.dumps(future.result()),flush=True)


if __name__=='__main__':main()
