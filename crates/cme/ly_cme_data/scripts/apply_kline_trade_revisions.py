#!/usr/bin/env python3
"""Rebuild affected TAS minutes and forward fills; never write native IB/ticks."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
import fcntl
import json
import os
from urllib.parse import quote

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as pipeline
from prepare_trade_revisions import RUN,WORK
from rebuild_kline_fill_policy import DIAGNOSTICS,local_parquet
from tas_trade_revisions import load_plan,mark_corrected,scope,delta
from compile_trade_revisions import digest

PROBLEMS=pipeline.DEST/'meta/problems/trade_revision_failure'


def verify_ib():
    samples=json.loads((pipeline.ROOT/'artifacts/kline_continuous_v5_20261110/ib_unchanged_samples.json').read_text())
    for row in samples:
        path=pipeline.Path(row['path']);stat=path.stat()
        assert (stat.st_ino,stat.st_size,stat.st_mtime_ns)==(row['inode'],row['bytes'],row['mtime_ns']),path
        assert len(pq.ParquetFile(path).schema_arrow.names)==23,path


def rebuild_minutes(c,old,replacement,impacted):
    """Discard gross aggregates only for affected minutes, including empty results."""
    fields=pipeline.KLINE_CORE[1:]+DIAGNOSTICS
    c.register('cached_actual',old.filter(pc.greater(old['trade_count'],0)).select(fields))
    c.register('replacement_actual',replacement.select(fields))
    c.register('impacted',pa.table({'ts_utc_ns':pa.array(sorted(impacted),type=pa.uint64())}))
    c.execute('''CREATE TEMP TABLE minutes AS
        SELECT m.* FROM cached_actual m ANTI JOIN impacted i USING(ts_utc_ns)
        UNION ALL SELECT * FROM replacement_actual''')


def changed_rows(before,after):
    assert len(before)==len(after)
    same=np.ones(len(after),dtype=bool)
    for name in after.schema.names:
        a=after[name];b=before[name]
        equal=pc.or_(pc.fill_null(pc.equal(a,b),False),pc.and_(pc.is_null(a),pc.is_null(b)))
        same&=equal.to_numpy(zero_copy_only=False)
    return ~same


def revise(contract,pool):
    ric=contract['ric'];name=quote(ric,safe='_-.^');plan=load_plan(ric);plan_hash=digest(plan)
    checkpoint=RUN/'applied'/(name+'.json')
    if checkpoint.exists():
        previous=json.loads(checkpoint.read_text())
        if previous['plan_hash']==plan_hash:return dict(previous,written_days=0)
    folder=pipeline.WORK/'contracts'/name
    receipt=pipeline.RUN/'contracts'/(name+'.json')
    base=WORK/'baseline'/name;base.mkdir(parents=True,exist_ok=True)
    if not (base/'kline.parquet').exists():
        assert not (pq.ParquetFile(folder/'kline.parquet').schema_arrow.metadata or {}).get(b'correction_policy_version'),ric
        os.link(folder/'kline.parquet',base/'kline.parquet')
    if not (base/'receipt.json').exists():pipeline.save(base/'receipt.json',json.loads(receipt.read_text()))
    original=json.loads((base/'receipt.json').read_text());start=original['start_ns'];end=original['end_ns']
    patches,insertions=scope(plan,start,end)
    impacted={r['event_ns']//pipeline.MINUTE*pipeline.MINUTE for r in patches+insertions}
    result=dict(ric=ric,plan_hash=plan_hash,patches=len(patches),insertions=len(insertions),
        pending=len(plan['unresolved']),changed_minutes=0,changed_days=0,written_days=0,count_delta=0,volume_delta=0)
    current_footer=pq.ParquetFile(folder/'kline.parquet')
    already_revised=bool((current_footer.schema_arrow.metadata or {}).get(b'correction_policy_version'))
    if not impacted and not already_revised:
        pipeline.save(checkpoint,result);return result
    old=pq.ParquetFile(base/'kline.parquet').read()
    current=current_footer.read() if already_revised else old
    assert old['ts_utc_ns'][0].as_py()==start and old['ts_utc_ns'][-1].as_py()+pipeline.MINUTE==end,ric
    intervals=pq.ParquetFile(folder/'intervals.parquet').read()
    replacement=None
    if impacted:
        probe=pipeline.connection()
        try:
            # Source footers limit reads; clock policy permits +/-12h.
            files=[]
            for path in contract['trade_files']:
                footer=pq.ParquetFile(path);names=footer.schema_arrow.names
                for i in range(footer.num_row_groups):
                    stats=footer.metadata.row_group(i).column(names.index('ts_utc_ns')).statistics
                    assert stats and stats.has_min_max,path
                    if any(stats.min-pipeline.DAY//2<=minute<stats.max+pipeline.DAY//2+pipeline.MINUTE for minute in impacted):
                        files.append(path);break
            assert files or not patches,(ric,'no source files for revised prints')
            pipeline.sparse_minutes(probe,files,intervals,start,end,plan,event_minutes=impacted)
            replacement=probe.execute('SELECT * FROM minutes ORDER BY ts_utc_ns').fetch_arrow_table()
        finally:probe.close()
    c=pipeline.connection()
    try:
        c.register('states',intervals)
        if impacted:
            rebuild_minutes(c,old,replacement,impacted)
            bars=pipeline.minute_grid(c,ric,start,end,pipeline.previous_close_seed(ric,start))
        else:bars=old
        bars=mark_corrected(bars,plan)
        count_delta,volume_delta=delta(plan,start,end)
        assert int(pc.sum(old['trade_count']).as_py())==original['source_trade_rows'],ric
        assert int(pc.sum(old['volume']).as_py())==original['source_volume'],ric
        assert int(pc.sum(bars['trade_count']).as_py())==original['source_trade_rows']+count_delta,ric
        assert int(pc.sum(bars['volume']).as_py())==original['source_volume']+volume_delta,ric
        assert bars['ts_utc_ns'].equals(old['ts_utc_ns']),ric
        for name_ in ['trading_ns','halted_ns','scheduled_closed_ns','status_known']:
            assert bars[name_].equals(old[name_]),(ric,name_)
        # A later source witness can invalidate an earlier tentative plan.
        # Compare against the current delivered cache, while recalculating from
        # the immutable gross baseline, so removed patches are also rolled back.
        public=pipeline.public_kline(bars);before=pipeline.public_kline(current)
        changed=changed_rows(before,public)
        total_changed=changed_rows(pipeline.public_kline(old),public)
        times=public['ts_utc_ns'].to_numpy();days=set((times[changed]//pipeline.DAY).tolist())
        to_publish=public.filter(pa.array(np.isin(times//pipeline.DAY,list(days))))
        issues=c.from_arrow(bars).filter('outside_status_trade_count>0 OR unknown_status_trade_count>0 OR missing_exchange_time_trade_count>0 OR (NOT status_known AND trade_count>0)').arrow().read_all()
        stats=differences=None
        if original['ib_rows']:
            native=pq.ParquetFile(folder/'kline_ib.parquet').read()
            stats,differences=pipeline.compare_ib(c,ric,bars,native,contract['tick_size'])
    finally:c.close()
    # Publish only genuinely changed days. The original local cache is hardlinked
    # above so interruption/retry always starts from the same gross baseline.
    published=pipeline.publish_daily(to_publish,contract,'kline',pool,allow_trade_revisions=True)
    assert published==len(days),ric
    local_parquet(bars,folder/'kline.parquet')
    for kind,table in [('state_issues',issues),('differences',differences)]:
        path=folder/(kind+'.parquet')
        if table is not None and len(table):local_parquet(table,path)
        else:path.unlink(missing_ok=True)
    updated=dict(original,effective_trade_rows=original['source_trade_rows']+count_delta,
        effective_volume=original['source_volume']+volume_delta,state_issue_minutes=len(issues),comparison=stats,
        correction_policy_version=plan['version'],correction_plan_sha256=plan_hash,correction_pending=len(plan['unresolved']))
    pipeline.save(receipt,updated)
    result.update(changed_minutes=int(np.sum(total_changed)),changed_days=len(set((times[total_changed]//pipeline.DAY).tolist())),
        written_days=len(days),count_delta=count_delta,volume_delta=volume_delta)
    pipeline.save(checkpoint,result)
    return result


def checked(contract,pool):
    path=PROBLEMS/(quote(contract['ric'],safe='_-.^')+'.json')
    try:
        result=revise(contract,pool);path.unlink(missing_ok=True);return result
    except Exception as error:
        import traceback
        pipeline.save(path,dict(ric=contract['ric'],problem=str(error),traceback=traceback.format_exc()));raise


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--ric');parser.add_argument('--workers',type=int,default=4)
    args=parser.parse_args();verify_ib()
    summary=json.loads((RUN/'compiled_summary.json').read_text())
    assert summary['pending_sources_explicit'] or summary['missing_jobs']==summary['archived_without_source']==0
    with (pipeline.RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        contracts=json.loads((pipeline.RUN/'contracts.json').read_text())
        selected=[c for c in contracts.values() if (not args.ric or c['ric']==args.ric)
            and json.loads((pipeline.RUN/'contracts'/(quote(c['ric'],safe='_-.^')+'.json')).read_text())['kline_rows']]
        assert len(selected)==(218 if not args.ric else 1)
        selected.sort(key=lambda c:(c['ric']!='SRAU26',c['ric']))
        results=[]
        with ThreadPoolExecutor(max_workers=16) as publisher,ThreadPoolExecutor(max_workers=args.workers) as workers:
            futures=[workers.submit(checked,c,publisher) for c in selected]
            for future in as_completed(futures):
                result=future.result();results.append(result);print(json.dumps(result),flush=True)
        if not args.ric:pipeline.finalize(contracts)
        verify_ib()
        total=Counter()
        for result in results:total.update({k:result[k] for k in ['patches','insertions','changed_minutes','changed_days','written_days','count_delta','volume_delta','pending']})
        pipeline.save(RUN/('backfill_summary.json' if not args.ric else 'backfill_'+args.ric+'.json'),dict(contracts=len(selected),affected_rics=sum(r['changed_days']>0 for r in results),**dict(total)))
        if PROBLEMS.exists() and not any(PROBLEMS.iterdir()):PROBLEMS.rmdir()


if __name__=='__main__':main()
