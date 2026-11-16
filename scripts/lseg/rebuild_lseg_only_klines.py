#!/usr/bin/env python3
"""Rebuild/publish TAS minutes with LSEG-only runtime inputs; IB stays read-only."""
import argparse
from collections import Counter,defaultdict
from concurrent.futures import ThreadPoolExecutor,as_completed
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import quote
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as p
from lseg_kline_state import intervals,lifetime
from parquet_delivery_checks import check_footer
from rebuild_kline_fill_policy import local_parquet,DIAGNOSTICS
from tas_trade_revisions import load_plan,mark_corrected
from apply_kline_trade_revisions import verify_ib

RUN=p.ROOT/'artifacts/lseg_only_klines_20261111'
WORK=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_only_klines_20261111')
STATUS_WORK=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_session_status_20261111')
OLD_WORK=p.WORK
PROBLEMS=p.DEST/'meta/problems/lseg_only_klines'
SOURCE=b'LSEG TAS trades, corrections and Mkt. Condition qualifiers only'
POLICY=b'lseg_only_v1'


def ns(text):return int(pd.Timestamp(text,tz='UTC').value)


def inputs():
    """Read only LSEG source receipts, HRD, raw status and correction witnesses."""
    sources=json.loads((RUN/'trade_sources.json').read_text())
    c=p.connection()
    try:
        raw=c.execute('SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY ric,ts_utc_ns,source_index,source_order',
            [[str(f) for f in sorted((STATUS_WORK/'raw').glob('*.parquet'))]]).fetch_arrow_table().to_pylist()
    finally:c.close()
    # Previously published identity scope is just a RIC list. Its Databento
    # states/timestamps/definitions are never read by this generator.
    rics=set(pq.ParquetFile(STATUS_WORK/'decoded.parquet').read(columns=['ric'])['ric'].to_pylist())
    by_ric=defaultdict(list)
    for row in raw:
        if row['ric'] in rics:by_ric[row['ric']].append(row)
    with (RUN/'warmup_status.tsv').open(newline='') as stream:
        for order,row in enumerate(csv.DictReader(stream,delimiter='\t')):
            if row['ric'] not in rics:continue
            row.update(ts_utc_ns=int(row['ts_utc_ns']),exch_hms_ns=int(row['exch_hms_ns']),source_index=-1,source_order=order)
            by_ric[row['ric']].append(row)
    venues=json.loads((p.ROOT/'artifacts/tas_status_20261109/plan.json').read_text())['exchanges']
    info=defaultdict(lambda:defaultdict(set))
    hrd_paths=[Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
        p.ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv']
    for path in hrd_paths:
        with path.open(encoding='utf-8-sig',newline='') as stream:
            for row in csv.DictReader(stream):
                ric=row['RIC']
                if ric not in rics:continue
                for name,target,offset in [('First Trading Date','first_dates',0),('Last Trading Day','expiry_dates',p.DAY),('Expiration Date','expiry_dates',p.DAY)]:
                    date=pd.to_datetime(row.get(name),format='%m/%d/%Y',utc=True,errors='coerce')
                    if pd.notna(date):info[ric][target].add(int(date.value)+offset)
                try:info[ric]['tick_size'].add(float(row['Tick Size']))
                except (ValueError,KeyError):pass
    jobs=json.loads((p.ROOT/'artifacts/lseg_session_status_20261111/source_plan.json').read_text())['jobs']
    ib_snapshot=json.loads((p.IB/'_audit/delivery.json').read_text())
    ib_end=int(pd.Timestamp(ib_snapshot['ib_last_utc']).value)+p.MINUTE
    coverage=defaultdict(list)
    for job in jobs:
        lo,hi=job['period'].split('_');lo,hi=ns(lo),ns(hi)
        for cf in job['cfs']:
            product=cf.split(':')[2]
            if job['id'].startswith('daily_'):
                receipt=json.loads((p.ROOT/'artifacts/tas_daily_ingest_20261109/products'/product/'staging_receipt.json').read_text())
                manifest=json.loads(Path(receipt['manifest']).read_text())
                for source in manifest['sources']:
                    assert source['complete'] and source['compressed_bytes']>0
                    day=ns(Path(source['file']).name[len(product)+1:len(product)+11])
                    coverage[product].append((day,day+p.DAY))
            else:coverage[product].append((lo,hi))
    gaps={};ends={}
    for product,ranges in coverage.items():
        cursor=p.START;missing=[]
        for lo,hi in sorted(ranges):
            if lo>cursor:missing.append((cursor,lo))
            cursor=max(cursor,hi)
        ends[product]=cursor;gaps[product]=missing
    inventory=json.loads((RUN/'corrections.json').read_text())['corrections']
    correction_rics={row['ric'] for row in inventory}
    plans={};contracts={}
    for ric in sorted(rics & set(sources)):
        product={r['product'] for r in by_ric[ric]};assert len(product)==1
        product=next(iter(product))
        venue=venues.get(ric,'review');assert venue in {'cme','cbot','comex','nymex','review'}
        try:revisions=load_plan(ric)
        except RuntimeError:
            assert ric not in correction_rics,('missing correction plan',ric)
            revisions=dict(version='1',ric=ric,source_scope='LSEG 2026 read-only RocksDB census; no Correction records',
                source_corrections=0,patches=[],insertions=[],applied=[],unresolved=[])
        plans[ric]=revisions
        d=info[ric]
        contracts[ric]=dict(ric=ric,product=product,exchange=venue,trade_files=sources[ric],
            start_ns=p.START,end_ns=ends[product],gaps=gaps[product],
            first_dates=sorted(d['first_dates']),expiry_dates=sorted(d['expiry_dates']),
            comparison_end_ns=min(ends[product],ib_end),
            tick_size=next(iter(d['tick_size'])) if len(d['tick_size'])==1 else None,
            ib_file=str(p.IB/'data'/f'{ric}.parquet') if (p.IB/'data'/f'{ric}.parquet').exists() else None)
    p.save(RUN/'contracts.json',contracts);p.save(RUN/'coverage_gaps.json',gaps)
    p.save(RUN/'correction_plans.json',plans)
    return contracts,by_ric,plans


def metadata(table,contract):
    md=dict(table.schema.metadata or {})
    md.update({b'source':SOURCE,b'kline_status_policy':POLICY,b'decoder_policy_version':b'2',
        b'status_time_basis':b'LSEG publication nanoseconds; no rounding or external exchange-clock backdating; missing archives reset state to UNKNOWN',
        b'status':b'TRADING=full minute tradable; SCHEDULED_CLOSED=explicit native scheduled reason; HALTED=temporary interruption; PARTIAL=known phases mixed; UNKNOWN=incomplete state or cause; details in LSEG session_status.parquet',
        b'lifetime_source':b'LSEG observed status/trades and LSEG HRD only; explicit native expiration takes precedence over date fallback; all actual trades retained',
        b'runtime_inputs':b'LSEG Trade Parquet, LSEG raw kind=13, LSEG HRD, own pre-window RocksDB price, LSEG audited correction ledger; no Databento, calendar, CME notice or IB input',
        b'covered_end_ns':str(contract['end_ns']).encode()})
    assert all(b'Databento' not in value for key,value in md.items() if key!=b'runtime_inputs')
    return table.replace_schema_metadata(md)


def problem_parquet(table,path):
    path.parent.mkdir(parents=True,exist_ok=True)
    local_parquet(table,path)


def publish_file(table,target,sampled):
    """Authorized source migration, same 10-column public schema, atomic per day."""
    target.parent.mkdir(parents=True,exist_ok=True)
    day=target.parent.name;ric=table['ric'][0].as_py()
    if target.exists():
        old=check_footer(target,dict(rows=None,ric=ric,utc_date=day))
        prior=old.schema_arrow.metadata or {}
        assert old.schema_arrow.remove_metadata().equals(table.schema.remove_metadata()),target
        assert prior.get(b'source') in {SOURCE,b'LSEG TAS kind=1 with Databento GLBX.MDP3 status'},target
    temporary=target.with_name('.kline.'+uuid.uuid4().hex+'.partial')
    try:
        pq.write_table(table,temporary,compression='zstd',row_group_size=65536)
        with temporary.open('rb') as stream:os.fsync(stream.fileno())
        check_footer(temporary,dict(rows=len(table),ric=ric,utc_date=day))
        temporary.replace(target)
        footer=check_footer(target,dict(rows=len(table),ric=ric,utc_date=day))
        assert footer.schema_arrow.equals(table.schema,check_metadata=True),target
        if sampled:assert footer.read().equals(table,check_metadata=False),target
    finally:temporary.unlink(missing_ok=True)
    return str(target)


def publish_daily(table,contract,pool):
    if table is None or not len(table):return [],0
    days=table['ts_utc_ns'].to_numpy()//p.DAY
    edges=np.r_[0,np.flatnonzero(days[1:]!=days[:-1])+1,len(table)]
    selected={0,(len(edges)-2)//2,len(edges)-2};futures=[]
    for i,(lo,hi) in enumerate(zip(edges[:-1],edges[1:])):
        date=pd.Timestamp(int(days[lo])*p.DAY,unit='ns',tz='UTC').strftime('%Y%m%d')
        target=p.DEST/contract['exchange']/'2026'/quote(contract['ric'],safe='_-.^')/date/'kline.parquet'
        futures.append(pool.submit(publish_file,table.slice(int(lo),int(hi-lo)),target,i in selected))
    paths=[f.result() for f in futures]
    return paths,len(selected)


def remove_stale(contract,paths):
    """Delete only obsolete pure-padding files, proven by existing footers."""
    keep=set(paths);removed=[]
    folder=p.DEST/contract['exchange']/'2026'/quote(contract['ric'],safe='_-.^')
    for path in folder.glob('*/kline.parquet'):
        if str(path) in keep:continue
        f=pq.ParquetFile(path);names=f.schema_arrow.names
        assert (f.schema_arrow.metadata or {}).get(b'source') in {SOURCE,b'LSEG TAS kind=1 with Databento GLBX.MDP3 status'},path
        for i in range(f.num_row_groups):
            for name in ('volume','trade_count'):
                s=f.metadata.row_group(i).column(names.index(name)).statistics
                assert s and s.has_min_max and s.min==s.max==0,('refuse removing actual trade',path)
        path.unlink();removed.append(str(path))
    return removed


def rebuild(contract,rows,plan,pool,publish=True):
    ric=contract['ric'];checkpoint=RUN/'contracts'/f'{ric}.json'
    if checkpoint.exists():
        prior=json.loads(checkpoint.read_text())
        candidate=lifetime(p.START,contract['end_ns'],rows,contract['first_dates'],contract['expiry_dates'])[0]
        if prior['kline_rows'] and candidate!=prior['start_ns']:
            old=pq.ParquetFile(WORK/'contracts'/ric/'kline.parquet').read(columns=['ts_utc_ns','trade_count'])
            actual=old.filter(pc.greater(old['trade_count'],0))
            first=int(actual['ts_utc_ns'][0].as_py()) if len(actual) else None
            candidate=lifetime(p.START,contract['end_ns'],rows,contract['first_dates'],contract['expiry_dates'],first_trade=first)[0]
        if candidate==prior['start_ns'] or not prior['kline_rows']:return prior
        history=RUN/'initial_lifetime_checkpoints';history.mkdir(exist_ok=True)
        checkpoint.replace(history/checkpoint.name)
    folder=WORK/'contracts'/ric;folder.mkdir(parents=True,exist_ok=True)
    c=p.connection();end=contract['end_ns'];bars=None;comparison=None;files=[];samples=0;issues=[];removed=[]
    try:
        state=intervals(rows,contract['product'],p.START,end,contract['gaps'])
        counts=p.sparse_minutes(c,contract['trade_files'],state,p.START,end,plan)
        gross=c.execute('SELECT count(*),coalesce(sum(volume),0),min(event_ns),max(event_ns) FROM trade_clock_gross').fetchone()
        actual=c.execute('SELECT min(event_ns),max(event_ns) FROM trade_clock').fetchone()
        first=actual[0] if actual[0] is not None else gross[2]
        last=actual[1] if actual[1] is not None else gross[3]
        start,stop,cutoff,issues=lifetime(p.START,end,rows,contract['first_dates'],contract['expiry_dates'],first,last)
        if (gross[0] or counts[0]) and stop>start:
            state=intervals(rows,contract['product'],start,stop,contract['gaps'])
            c.register('states',state)
            seed=p.previous_close_seed(ric,start)
            # For midyear starts there are no earlier in-window prints: sparse
            # aggregation deliberately retained the full year's source relation.
            bars=metadata(mark_corrected(p.minute_grid(c,ric,start,stop,seed),plan),contract)
            assert int(pc.sum(bars['trade_count']).as_py() or 0)==counts[0],ric
            assert int(pc.sum(bars['volume']).as_py() or 0)==counts[1],ric
            local_parquet(bars,folder/'kline.parquet');local_parquet(state,folder/'intervals.parquet')
            public=metadata(p.public_kline(bars),contract)
            old=OLD_WORK/'contracts'/ric/'kline.parquet'
            if old.exists():
                prior=pq.ParquetFile(old).read(columns=p.KLINE_CORE)
                lo=int(prior['ts_utc_ns'][0].as_py()) if len(prior) else 0
                hi=int(prior['ts_utc_ns'][-1].as_py())+p.MINUTE if len(prior) else 0
                expected=prior.filter(pc.greater(prior['trade_count'],0))
                common=bars.filter(pc.and_(pc.and_(pc.greater_equal(bars['ts_utc_ns'],lo),pc.less(bars['ts_utc_ns'],hi)),pc.greater(bars['trade_count'],0))).select(p.KLINE_CORE)
                assert expected.equals(common,check_metadata=False),('actual OHLCV changed in old common window',ric)
            diagnostics=c.execute('SELECT coalesce(sum(outside_status_trade_count),0),coalesce(sum(unknown_status_trade_count),0),coalesce(sum(missing_exchange_time_trade_count),0) FROM minutes').fetchone()
            # Only genuine exceptions; no normal minute dumps or success audits.
            c.register('full_bars',bars)
            problems=c.execute('''SELECT * FROM full_bars WHERE outside_status_trade_count>0 OR unknown_status_trade_count>0
                OR missing_exchange_time_trade_count>0''').fetch_arrow_table()
            if publish:
                target=PROBLEMS/'trade_state'/f'{ric}.parquet'
                if len(problems):problem_parquet(problems,target)
                else:target.unlink(missing_ok=True)
            unknown=[dict(ric=ric,product=contract['product'],**r) for r in state.to_pylist() if r['phase']=='UNKNOWN']
            if publish:
                target=PROBLEMS/'unknown_intervals'/f'{ric}.csv'
                if unknown:p.write_csv(target,unknown,list(unknown[0]))
                else:target.unlink(missing_ok=True)
            if contract['ib_file']:
                ib=pq.ParquetFile(contract['ib_file']).read()
                compare_end=min(stop,contract['comparison_end_ns'])
                ib=ib.filter(pc.and_(pc.greater_equal(ib['ts_utc_ns'],start),pc.less(ib['ts_utc_ns'],compare_end)))
                assert len(ib['ts_utc_ns'].unique())==len(ib),ric
                if len(ib):
                    compare_bars=bars.filter(pc.less(bars['ts_utc_ns'],compare_end))
                    comparison,differences=p.compare_ib(c,ric,compare_bars,ib,contract['tick_size'])
                    if len(differences):
                        local_parquet(differences,folder/'ib_differences.parquet')
                        if publish:problem_parquet(differences,PROBLEMS/'ib_differences'/f'{ric}.parquet')
            if publish:files,samples=publish_daily(public,contract,pool)
        else:diagnostics=(0,0,0)
    finally:c.close()
    if publish:removed=remove_stale(contract,files)
    result=dict(ric=ric,product=contract['product'],exchange=contract['exchange'],start_ns=start,end_ns=stop,
        kline_rows=len(bars) if bars is not None else 0,source_trade_rows=int(gross[0]),source_volume=int(gross[1]),
        effective_trade_rows=counts[0],effective_volume=counts[1],files=files,samples=samples,removed=removed,
        status_counts=dict(Counter(public['status'].to_pylist())) if bars is not None else {},
        outside_status_trade_count=int(diagnostics[0]),unknown_status_trade_count=int(diagnostics[1]),
        missing_exchange_time_trade_count=int(diagnostics[2]),lifetime_issues=issues,comparison=comparison,
        comparison_end_ns=min(stop,contract['comparison_end_ns']),
        comparison_effective_volume=int(pc.sum(bars.filter(pc.less(bars['ts_utc_ns'],min(stop,contract['comparison_end_ns'])))['volume']).as_py() or 0) if bars is not None else 0)
    if publish:p.save(checkpoint,result)
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--ric',action='append');parser.add_argument('--preview',action='store_true')
    parser.add_argument('--workers',type=int,default=3);args=parser.parse_args()
    RUN.mkdir(parents=True,exist_ok=True);WORK.mkdir(parents=True,exist_ok=True)
    with (RUN/'lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        verify_ib();contracts,rows,plans=inputs()
        # Switch only the scratch/seed root of the shared aggregator, never its
        # Databento loading or publishing entrypoints.
        local_seed=WORK/'previous_close.json'
        if not local_seed.exists():local_seed.write_bytes((OLD_WORK/'previous_close.json').read_bytes())
        p.WORK=WORK
        selected={r:c for r,c in contracts.items() if not args.ric or r in args.ric}
        assert selected
        p.save(RUN/'state.json',dict(stage='running',contracts=len(selected),status_policy=POLICY.decode()))
        results=[];failures=[]
        with ThreadPoolExecutor(max_workers=12) as pool,ThreadPoolExecutor(max_workers=args.workers) as workers:
            jobs={workers.submit(rebuild,c,rows[r],plans[r],pool,not args.preview):r for r,c in selected.items()}
            for future in as_completed(jobs):
                ric=jobs[future]
                try:
                    result=future.result();results.append(result)
                    print(f'{ric}: {result["kline_rows"]:,} minutes; {len(result["files"])} days',flush=True)
                    (PROBLEMS/'failures'/f'{ric}.json').unlink(missing_ok=True)
                except Exception as error:
                    import traceback
                    traceback.print_exc();failures.append(dict(ric=ric,error=str(error)))
                    if not args.preview:p.save(PROBLEMS/'failures'/f'{ric}.json',failures[-1])
                p.save(RUN/'state.json',dict(stage='running',contracts=len(selected),completed=len(results),failed=failures,
                    kline_rows=sum(r['kline_rows'] for r in results),files=sum(len(r['files']) for r in results)))
        verify_ib()
        p.save(RUN/('preview.json' if args.preview else 'summary.json'),dict(contracts=len(selected),completed=len(results),failed=failures,
            kline_rows=sum(r['kline_rows'] for r in results),files=sum(len(r['files']) for r in results),
            source_trade_rows=sum(r['source_trade_rows'] for r in results),effective_trade_rows=sum(r['effective_trade_rows'] for r in results),
            source_volume=sum(r['source_volume'] for r in results),effective_volume=sum(r['effective_volume'] for r in results),
            full_readback_samples=sum(r['samples'] for r in results),removed_files=sum(len(r['removed']) for r in results)))
        if not args.preview:
            life=[dict(ric=r['ric'],**issue) for r in results for issue in r['lifetime_issues']]
            if life:p.write_csv(PROBLEMS/'lifetime_conflicts.csv',life,list(life[0]))
            p.save(RUN/'state.json',dict(stage='failed' if failures else 'complete',contracts=len(selected),completed=len(results),failed=failures))
        if failures:raise SystemExit(1)


if __name__=='__main__':main()
