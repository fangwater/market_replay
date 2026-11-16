#!/usr/bin/env python3
"""All stored futures TAS years/roots; immutable RocksDB -> daily LSEG minutes.

Reads the native sparse extraction, never DBN/calendars/IB. Work is checkpointed
by product/year. Existing 237 revised 2026 RICs are verified and reused.
"""
from resource_runtime import enter, bounded_workers, configure_arrow, IO_THREADS
if __name__ == '__main__':
    enter()

import argparse
from collections import defaultdict,Counter
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor,as_completed
import csv
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import multiprocessing
import os
import shutil
from itertools import zip_longest
from pathlib import Path
import time
from urllib.parse import quote
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
configure_arrow(pa)

import build_market_status_klines as p
from extract_full_tas_sparse import RUN,WORK
from lseg_full_policy import window,CME_VENUES
from lseg_kline_state import ordered_intervals
from lseg_session_status import decode
from rebuild_lseg_only_klines import SOURCE,publish_file
from rebuild_kline_fill_policy import local_parquet
from build_lseg_session_status import PUBLIC,SCHEMA
from parquet_delivery_checks import check_footer
from apply_kline_trade_revisions import verify_ib
from disk_session_events import prepare as prepare_events,Rows

PROBLEMS=p.DEST/'meta/problems/full_tas_klines'
POLICY=b'lseg_only_full_v1'
_WORKER_METADATA=None
_WORKER_PUBLISH=None


def ns(text):return int(pd.Timestamp(text,tz='UTC').value)
def stamp_year(year):return ns(f'{year}-01-01')
def component(text):return quote(text,safe='_-.^')
def gid(year,product):return component(f'{year}:{product}')


def initialize_worker():
    global _WORKER_METADATA,_WORKER_PUBLISH
    p.WORK=WORK
    _WORKER_METADATA=load_metadata()
    configure_arrow(pa)
    _WORKER_PUBLISH=ThreadPoolExecutor(max_workers=IO_THREADS)


def process_group(year,product,jobs):
    return build_group(year,product,jobs,_WORKER_METADATA,_WORKER_PUBLISH)


def load_metadata():
    venue=defaultdict(set);reported={}
    frame=pq.ParquetFile(p.ROOT/'artifacts/lseg_exchange_groups_20261109/source_ric_details.parquet').read(
        columns=['asset_class','source_layer','year','ric','exchange','source_end'])
    for r in frame.to_pylist():
        if r['asset_class']!='futures' or r['source_layer']!='tas':continue
        key=(int(r['year']),r['ric']);venue[key].add(r['exchange'].lower())
        try:reported[key]=max(reported.get(key,0),int(pd.Timestamp(r['source_end']).value))
        except (ValueError,TypeError):pass
    # Preserve actual 2026 routing used by the existing original/daily delivery.
    approved=json.loads((p.ROOT/'artifacts/tas_status_20261109/plan.json').read_text())['exchanges']
    for ric,v in approved.items():venue[(2026,ric)]={v}
    hrd=defaultdict(list);date_cache={};source_rics={key[1] for key in venue}
    for path in [Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
        p.ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv']:
        with path.open(encoding='utf-8-sig',newline='') as stream:
            for r in csv.DictReader(stream):
                if r['RIC'] not in source_rics:continue
                dates={}
                for name in ('First Trading Date','Last Trading Day','Expiration Date'):
                    text=r.get(name,'')
                    if text not in date_cache:
                        try:date_cache[text]=int(datetime.strptime(text,'%m/%d/%Y').replace(tzinfo=timezone.utc).timestamp())*1_000_000_000
                        except (ValueError,TypeError):date_cache[text]=None
                    dates[name]=date_cache[text]
                endings=[v+p.DAY for k,v in dates.items() if k!='First Trading Date' and v is not None]
                hrd[r['RIC']].append(dict(first=dates['First Trading Date'],expiry=min(endings) if endings else None))
    protected={}
    old=p.ROOT/'artifacts/lseg_only_klines_20261111/contracts'
    for path in old.glob('*.json'):
        r=json.loads(path.read_text())
        if r['kline_rows']:protected[(2026,r['ric'])]=r
    daily_days={}
    for path in (p.ROOT/'artifacts/tas_daily_ingest_20261109/products').glob('*/staging_receipt.json'):
        receipt=json.loads(path.read_text())
        if not receipt.get('manifest'):continue
        manifest=json.loads(Path(receipt['manifest']).read_text());product=path.parent.name;ranges=[]
        for source in manifest['sources']:
            if not source.get('complete') or not source.get('compressed_bytes'):continue
            name=Path(source['file']).name;day=ns(name[len(product)+1:len(product)+11]);ranges.append((day,day+p.DAY))
        daily_days[product]=ranges
    return venue,reported,hrd,protected,daily_days


def coverage(jobs,year,daily_days,boundary_range=None):
    start=stamp_year(year);end=stamp_year(year+1);ranges=[]
    for job in jobs:
        if job['year']!=year:continue
        lo,hi=job['period'].split('_')
        if job['source']=='daily':ranges.extend(daily_days.get(job['product'],[]))
        else:ranges.append((ns(lo),ns(hi)))
    if not ranges:
        assert boundary_range is not None
        return max(start,boundary_range[0]//p.MINUTE*p.MINUTE),min(end,(boundary_range[1]//p.MINUTE+1)*p.MINUTE),[]
    lo=min(r[0] for r in ranges);cursor=lo;gaps=[]
    for a,b in sorted(ranges):
        if a>cursor:gaps.append((cursor,a))
        cursor=max(cursor,b)
    return max(start,lo),min(end,cursor),gaps


def write_problem(name,rows):
    target=PROBLEMS/name
    if rows:p.write_csv(target,rows,list(dict.fromkeys(key for row in rows for key in row)))
    else:target.unlink(missing_ok=True)


def publish_status(table,target,sampled):
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():
        f=check_footer(target,dict(rows=len(table),ric=table['ric'][0].as_py(),utc_date=target.parent.name))
        assert f.schema_arrow.remove_metadata().equals(table.schema.remove_metadata()),target
        # Original normalized states already published for calibrated roots are
        # retained. Full-scope retries validate/reuse their own immutable rows.
        if sampled:assert f.read().equals(table,check_metadata=False),target
        return
    temporary=target.with_name('.session_status.'+uuid.uuid4().hex+'.partial')
    try:
        pq.write_table(table,temporary,compression='zstd');temporary.replace(target)
        f=check_footer(target,dict(rows=len(table),ric=table['ric'][0].as_py(),utc_date=target.parent.name))
        if sampled:assert f.read().equals(table,check_metadata=False),target
    finally:temporary.unlink(missing_ok=True)


def publish_days(table,venue,year,ric,kind,pool,sample=False):
    if not len(table):return []
    days=table['ts_utc_ns'].to_numpy()//p.DAY;edges=np.r_[0,np.flatnonzero(days[1:]!=days[:-1])+1,len(table)]
    selected={0,(len(edges)-2)//2,len(edges)-2} if sample else set();futures=[];paths=[]
    for i,(lo,hi) in enumerate(zip(edges[:-1],edges[1:])):
        day=pd.Timestamp(int(days[lo])*p.DAY,unit='ns',tz='UTC').strftime('%Y%m%d')
        target=p.DEST/venue/str(year)/component(ric)/day/(kind+'.parquet');paths.append(str(target))
        writer=publish_file if kind=='kline' else publish_status
        futures.append(pool.submit(writer,table.slice(int(lo),int(hi-lo)),target,i in selected))
    for f in futures:f.result()
    return paths


def publish_status_file(local,target,sampled):
    source=pq.ParquetFile(local)
    expected=dict(rows=source.metadata.num_rows,ric=source.read_row_group(0,columns=['ric'])['ric'][0].as_py(),utc_date=target.parent.name)
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():
        other=check_footer(target,expected)
        assert other.schema_arrow.remove_metadata().equals(source.schema_arrow.remove_metadata()),target
        if sampled:
            def rows(file):
                for batch in file.iter_batches(batch_size=8192,use_threads=False):yield from batch.to_pylist()
            missing=object()
            for left,right in zip_longest(rows(source),rows(other),fillvalue=missing):assert left==right,target
        return
    temporary=target.with_name('.session_status.'+uuid.uuid4().hex+'.partial')
    try:
        shutil.copyfile(local,temporary);temporary.replace(target);check_footer(target,expected)
        if sampled:
            with local.open('rb') as a,target.open('rb') as b:assert hashlib.file_digest(a,'sha256').digest()==hashlib.file_digest(b,'sha256').digest(),target
    finally:temporary.unlink(missing_ok=True)


def deliver_status(rows,product,ric,venue,year,folder,pool,sample,scope,issues):
    """Keep all normalized messages/duplicates, with only one 8192-row buffer."""
    metadata={b'source':b'LSEG TAS Mkt. Condition qualifiers',b'session_status_schema_version':b'1',
        b'decoder_policy_version':b'full1',b'time_basis':b'exact LSEG publication nanoseconds; raw qualifiers and duplicates retained',b'scope':scope}
    public_schema=pa.schema([SCHEMA.field(name) for name in PUBLIC],metadata=metadata)
    start=stamp_year(year);end=stamp_year(year+1);previous='UNKNOWN';day=None;day_number=None;writer=None;buffer=[];files=[];counts=Counter()
    summaries={};directory=folder/'session'/component(ric);directory.mkdir(parents=True,exist_ok=True)
    def flush():
        if buffer:
            table=pa.Table.from_pylist(buffer,schema=SCHEMA).select(PUBLIC).replace_schema_metadata(metadata)
            writer.write_table(table,row_group_size=8192);buffer.clear()
    def finish():
        if writer is not None:
            flush();writer.close();partial=directory/(day+'.parquet.partial');complete=directory/(day+'.parquet')
            partial.replace(complete);assert pq.ParquetFile(complete).metadata.num_rows==counts[day]
            files.append((day,complete))
    try:
        for message in rows:
            parsed=decode(message['qualifiers'],previous,product,cme_scope=venue in CME_VENUES)
            at=int(message['ts_utc_ns']);before=previous;previous=parsed['session_state']
            if not start<=at<end:continue
            next_day_number=at//p.DAY
            if next_day_number!=day_number:
                finish();day_number=next_day_number;day=pd.Timestamp(day_number*p.DAY,unit='ns',tz='UTC').strftime('%Y%m%d')
                writer=pq.ParquetWriter(directory/(day+'.parquet.partial'),public_schema,compression='zstd')
            counts[day]+=1
            buffer.append(dict(product=product,ric=ric,ts_utc_ns=at,published_ns=at,effective_ns=at,
                is_transition=before!=previous,source='LSEG_TAS',source_url=None,qualifiers=message['qualifiers'],**parsed))
            if len(buffer)>=8192:flush()
            if parsed.get('issue_code') and parsed['issue_code']!='uncalibrated_product':
                key=(parsed['issue_code'],message['qualifiers'])
                if key not in summaries:
                    if len(summaries)>=100_000:raise RuntimeError('too many distinct qualifier problems in one RIC')
                    summaries[key]=dict(ric=ric,published_ns=at,last_published_ns=at,count=0,issue=key[0],qualifiers=key[1])
                summaries[key]['count']+=1;summaries[key]['last_published_ns']=at
        finish();writer=None
    finally:
        if writer is not None:writer.close()
    issues.extend(summaries.values());selected={0,len(files)//2,len(files)-1} if sample else set();paths=[];futures=[]
    for i,(day,local) in enumerate(files):
        target=p.DEST/venue/str(year)/component(ric)/day/'session_status.parquet';paths.append(str(target))
        if pool is None:publish_status_file(local,target,i in selected)
        else:
            futures.append(pool.submit(publish_status_file,local,target,i in selected))
            if len(futures)>=IO_THREADS:futures.pop(0).result()
    for future in futures:future.result()
    return paths


def prune_padding(year,venue,ric,keep):
    removed=0;keep=set(keep)
    for path in (p.DEST/venue/str(year)/component(ric)).glob('*/kline.parquet'):
        if str(path) in keep:continue
        f=pq.ParquetFile(path);names=f.schema_arrow.names
        assert (f.schema_arrow.metadata or {}).get(b'kline_status_policy')==POLICY,path
        for i in range(f.num_row_groups):
            for name in ('volume','trade_count'):
                s=f.metadata.row_group(i).column(names.index(name)).statistics
                assert s and s.has_min_max and s.min==s.max==0,('refuse deleting actual trade',path)
        path.unlink();removed+=1
    return removed


def build_group(year,product,jobs,metadata,pool):
    key=gid(year,product);checkpoint=RUN/'groups'/f'{key}.json'
    identities=[json.loads((WORK/'extract'/j['id']/'state.json').read_text())['identity'] for j in jobs]
    signature=hashlib.sha256(json.dumps(identities,sort_keys=True).encode()).hexdigest()
    if checkpoint.exists():
        result=json.loads(checkpoint.read_text());assert result['input_signature']==signature
        result.pop('contracts',None)  # Per-RIC checkpoints already retain detail.
        return result
    venues,reported,hrd,protected,daily_days=metadata
    files=[str(WORK/'extract'/j['id']/'minutes.parquet') for j in jobs]
    year_start=stamp_year(year);year_end=stamp_year(year+1)
    folder=WORK/'groups'/key;folder.mkdir(parents=True,exist_ok=True)
    qualifier_issues=[];unknown=[];life_issues=[];pending=[];scope_issues=[];results=[]
    c=p.connection()
    try:
        event_paths=prepare_events(c,WORK,jobs)
        correction_paths=[path for path,job in zip(event_paths['corrections'],jobs) if job['year']==year]
        sources=pa.table({'filename':files,'source_index':[j['source_index'] for j in jobs]});c.register('source_files',sources)
        c.execute('''CREATE TEMP TABLE merged AS SELECT ric,ts_utc_ns,
            first("open" ORDER BY first_event_ns,first_published_ns,source_index,first_order)::BIGINT AS "open",
            max(high)::BIGINT high,min(low)::BIGINT low,
            last("close" ORDER BY last_event_ns,last_published_ns,source_index,last_order)::BIGINT AS "close",
            sum(volume)::UBIGINT volume,sum(trade_count)::UBIGINT trade_count,
            min(first_event_ns)::UBIGINT first_event_ns,max(last_event_ns)::UBIGINT last_event_ns,
            sum(missing_exchange_time_trade_count)::UBIGINT missing_exchange_time_trade_count,
            sum(late_trade_count)::UBIGINT late_trade_count
            FROM read_parquet(?,filename=true,hive_partitioning=false) JOIN source_files USING(filename)
            WHERE ts_utc_ns>=? AND ts_utc_ns<? GROUP BY ric,ts_utc_ns''',[files,year_start,year_end])
        counts=c.execute('''SELECT ric,count(*) n,sum(trade_count) trades,sum(volume) volume,
            min(first_event_ns) first_event,max(last_event_ns) last_event FROM merged GROUP BY ric ORDER BY ric''').fetch_arrow_table().to_pylist()
        if not counts:
            result=dict(year=year,product=product,input_signature=signature,rics=0,reused_rics=0,trades=0,volume=0,kline_rows=0,files=0,status_files=0)
            p.save(checkpoint,result);return result
        a=min(r['first_event'] for r in counts);b=max(r['last_event'] for r in counts)
        start,end,gaps=coverage(jobs,year,daily_days,(a,b))
        selected={0,len(counts)//2,len(counts)-1}
        for number,row in enumerate(counts):
            ric=row['ric'];trades=int(row['trades']);volume=int(row['volume'])
            ric_checkpoint=RUN/'contracts'/key/(component(ric)+'.json')
            if ric_checkpoint.exists():
                cached=json.loads(ric_checkpoint.read_text());assert cached['input_signature']==signature
                assert cached['trades']==trades and cached['volume']==volume
                for name,rows in [('unknown_intervals',unknown),('lifetime',life_issues),('corrections',pending),('scope',scope_issues),('qualifiers',qualifier_issues)]:
                    rows.extend(cached.get('problems',{}).get(name,[]))
                results.append(cached);continue
            if (year,ric) in protected:
                original=protected[(year,ric)]
                assert trades==original['source_trade_rows'] and volume==original['source_volume'],('protected source coverage changed',year,ric,trades,original['source_trade_rows'])
                result=dict(ric=ric,reused=True,trades=trades,volume=volume,kline_rows=original['kline_rows'],files=0,status_files=0,input_signature=signature)
                results.append(result);p.save(ric_checkpoint,result);continue
            problem_lists={'unknown_intervals':unknown,'lifetime':life_issues,'corrections':pending,'scope':scope_issues,'qualifiers':qualifier_issues}
            problem_offsets={name:len(rows) for name,rows in problem_lists.items()}
            candidate=venues.get((year,ric),set());venue=next(iter(candidate)) if len(candidate)==1 else 'review'
            if venue=='review':scope_issues.append(dict(ric=ric,issue='venue_unresolved',detail='original RIC retained under review; no prefix-based market inference'))
            is_cme=venue in CME_VENUES
            if not is_cme:scope_issues.append(dict(ric=ric,issue='status_protocol_not_calibrated',detail='only explicit H/N halt flag interpreted; native CME phase/reason codes not applied'))
            valid_refs=[r for r in hrd.get(ric,[]) if r.get('first') is not None and r.get('expiry') is not None
                and r['first']<=int(row['first_event'])+p.DAY and r['expiry']>=int(row['first_event'])-p.DAY]
            trusted_span=len({r['expiry'] for r in valid_refs})==1 and any(r['first']<start<r['expiry'] for r in valid_refs)
            status=Rows(event_paths['status'],ric,lower=None if trusted_span else start,upper=end)
            controls=Rows(correction_paths,ric,clock='published_ns');correction_count=len(controls)
            lo,hi,issues=window(ric,product,venue,start,end,status,hrd.get(ric,[]),int(row['first_event']),int(row['last_event']),reported.get((year,ric)))
            life_issues.extend(issues)
            state=ordered_intervals(status,product,lo,hi,gaps,cme_scope=is_cme);c.register('states',state)
            c.execute('DROP TABLE IF EXISTS minutes');c.execute('DROP TABLE IF EXISTS minute_status')
            c.execute('''CREATE TEMP TABLE minutes AS SELECT ts_utc_ns,"open",high,low,"close",volume,trade_count,
                0::UBIGINT outside_status_trade_count,0::UBIGINT unknown_status_trade_count,
                missing_exchange_time_trade_count,late_trade_count FROM merged WHERE ric=?''',[ric])
            seed=None
            if trusted_span:
                prior=[str(WORK/'extract'/j['id']/'minutes.parquet') for j in jobs if j['year']<year]
                if prior:
                    value=c.execute('''SELECT "close" FROM read_parquet(?) WHERE ric=? AND ts_utc_ns<?
                        ORDER BY last_event_ns DESC,last_published_ns DESC,last_order DESC LIMIT 1''',[prior,ric,lo]).fetchone()
                    if value:seed=int(value[0])
            bars=p.minute_grid(c,ric,lo,hi,seed)
            assert int(pc.sum(bars['trade_count']).as_py() or 0)==trades,(year,ric,'count')
            assert int(pc.sum(bars['volume']).as_py() or 0)==volume,(year,ric,'volume')
            md=dict(bars.schema.metadata or {});md.update({b'source':SOURCE,b'kline_status_policy':POLICY,
                b'status_time_basis':b'LSEG publication time; same-time source patch order; missing archive resets UNKNOWN',
                b'input_signature':signature.encode(),b'lifetime_source':b'LSEG HRD consistent with observed source lifetime; native CME expiry; otherwise last observed LSEG evidence',
                b'status_scope':b'CME-native phase semantics only for LSEG-verified CME Group venue; other venues explicit halt flag only, otherwise UNKNOWN',
                b'correction_pending_count':str(correction_count).encode(),
                b'correction_scope':b'raw Trade aggregation; unlinked kind=12 controls retained as problems; existing 237 revised 2026 RICs reused separately; no speculative deletion'})
            bars=bars.replace_schema_metadata(md);public=p.public_kline(bars)
            md=dict(public.schema.metadata or {});md[b'status']=b'TRADING/SCHEDULED_CLOSED/HALTED/PARTIAL/UNKNOWN; detailed LSEG phases/qualifiers in session_status.parquet'
            public=public.replace_schema_metadata(md)
            paths=publish_days(public,venue,year,ric,'kline',pool,sample=number in selected)
            prune_padding(year,venue,ric,paths)
            unknown.extend(dict(ric=ric,start_ns=r['start_ns'],end_ns=r['end_ns'],issue='unresolved_session_state_or_cause') for r in state.to_pylist() if r['phase']=='UNKNOWN')
            for control in controls:
                if len(pending)>=1_000_000:raise RuntimeError('more than one million unresolved controls in one group; finer problem partition required')
                pending.append(dict(ric=ric,published_ns=control['published_ns'],source_db=control['source_db'],source_cf=control['source_cf'],
                    source_order=control['source_order'],issue='source_witness_pending',qualifiers=control['qualifiers']))
            status_files=deliver_status(status,product,ric,venue,year,folder,pool,number in selected,md[b'status_scope'],qualifier_issues)
            if sum(len(items) for items in problem_lists.values())>1_000_000:raise RuntimeError('more than one million distinct problems in one group; finer problem partition required')
            result=dict(ric=ric,reused=False,trades=trades,volume=volume,kline_rows=len(public),files=len(paths),status_files=len(status_files),input_signature=signature)
            result['problems']={name:rows[problem_offsets[name]:] for name,rows in problem_lists.items() if len(rows)>problem_offsets[name]}
            results.append(result);p.save(ric_checkpoint,result)
            print(f'{year}/{product}/{ric}: {len(public):,} minutes; {len(paths)} days',flush=True)
        result=dict(year=year,product=product,input_signature=signature,rics=len(results),reused_rics=sum(r['reused'] for r in results),
            trades=sum(r['trades'] for r in results),volume=sum(r['volume'] for r in results),
            kline_rows=sum(r['kline_rows'] for r in results),files=sum(r['files'] for r in results),status_files=sum(r['status_files'] for r in results))
        for name,rows in [('unknown_intervals',unknown),('lifetime',life_issues),('corrections',pending),('scope',scope_issues),('qualifiers',qualifier_issues)]:
            write_problem(Path(name)/(key+'.csv'),rows)
        p.save(checkpoint,result);return result
    finally:c.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--workers',type=bounded_workers,default=2);parser.add_argument('--watch',action='store_true');parser.add_argument('--only',action='append');parser.add_argument('--processes',action=argparse.BooleanOptionalAction,default=True);args=parser.parse_args()
    RUN.mkdir(parents=True,exist_ok=True);WORK.mkdir(parents=True,exist_ok=True);p.WORK=WORK
    with (RUN/'publish.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);verify_ib()
        plan=json.loads((RUN/'plan.json').read_text())['jobs'];metadata=None if args.processes else load_metadata();seen=set();bounds={};groups={(j['year'],j['product']) for j in plan};done={};failures={}
        if args.only:groups={k for k in groups if gid(*k) in args.only}
        with ThreadPoolExecutor(max_workers=IO_THREADS) as pool:
            while True:
                available={j['id'] for j in plan if (WORK/'extract'/j['id']/'state.json').exists()}
                for job in plan:
                    if job['id'] not in available or job['id'] in seen:continue
                    f=pq.ParquetFile(WORK/'extract'/job['id']/'minutes.parquet');i=f.schema_arrow.names.index('ts_utc_ns')
                    for n in range(f.num_row_groups):
                        s=f.metadata.row_group(n).column(i).statistics
                        if not s or not s.has_min_max:continue
                        for at in (s.min,s.max):
                            year=pd.Timestamp(int(at),unit='ns',tz='UTC').year
                            if not args.only:groups.add((year,job['product']))
                    seen.add(job['id'])
                ready={}
                for key in sorted(groups,key=lambda k:(-k[0],k[1])):
                    if key in done or key in failures:continue
                    year,product=key;jobs=[j for j in plan if j['product']==product and abs(j['year']-year)<=1]
                    if all(j['id'] in available for j in jobs):ready[key]=jobs
                if ready:
                    executor=(ProcessPoolExecutor(max_workers=args.workers,initializer=initialize_worker,mp_context=multiprocessing.get_context('spawn'))
                        if args.processes else ThreadPoolExecutor(max_workers=args.workers))
                    with executor as workers:
                        futures={(workers.submit(process_group,*key,jobs) if args.processes else workers.submit(build_group,*key,jobs,metadata,pool)):key for key,jobs in ready.items()}
                        for future in as_completed(futures):
                            key=futures[future]
                            try:
                                result=future.result();done[key]=result
                                print(f'{key}: {result["rics"]} RICs; {result["files"]} kline days; reused {result["reused_rics"]}',flush=True)
                                (PROBLEMS/'failures'/f'{gid(*key)}.json').unlink(missing_ok=True)
                            except Exception as error:
                                import traceback
                                traceback.print_exc();failures[key]=str(error);p.save(PROBLEMS/'failures'/f'{gid(*key)}.json',dict(year=key[0],product=key[1],error=str(error)))
                            p.save(RUN/'publish_state.json',dict(stage='running',groups=len(groups),completed=len(done),
                                failed=[dict(year=k[0],product=k[1],error=v) for k,v in failures.items()],
                                rics=sum(r['rics'] for r in done.values()),files=sum(r['files'] for r in done.values()),kline_rows=sum(r['kline_rows'] for r in done.values())))
                extraction=json.loads((RUN/'extract_state.json').read_text()) if (RUN/'extract_state.json').exists() else {}
                if len(done)+len(failures)==len(groups):break
                if not args.watch or extraction.get('stage') in {'failed','complete'}:
                    pending=[dict(year=k[0],product=k[1],error='required source extraction not complete') for k in groups-done.keys()-failures.keys()]
                    if pending:failures.update({(r['year'],r['product']):r['error'] for r in pending})
                    break
                p.save(RUN/'publish_state.json',dict(stage='waiting_for_extraction',groups=len(groups),completed=len(done),failed=[dict(year=k[0],product=k[1],error=v) for k,v in failures.items()]))
                time.sleep(5)
        verify_ib()
        result=dict(stage='failed' if failures else 'complete',groups=len(groups),completed=len(done),
            failed=[dict(year=k[0],product=k[1],error=v) for k,v in failures.items()],rics=sum(r['rics'] for r in done.values()),
            reused_rics=sum(r['reused_rics'] for r in done.values()),trades=sum(r['trades'] for r in done.values()),volume=sum(r['volume'] for r in done.values()),
            kline_rows=sum(r['kline_rows'] for r in done.values()),files=sum(r['files'] for r in done.values()),status_files=sum(r['status_files'] for r in done.values()))
        if not failures and not args.only:
            extraction=json.loads((RUN/'extract_state.json').read_text())
            assert extraction['stage']=='complete' and result['trades']==extraction['trades'],('unaccounted Trade events',result['trades'],extraction)
        p.save(RUN/'publish_state.json',result)
        if failures:raise SystemExit(1)


if __name__=='__main__':main()
