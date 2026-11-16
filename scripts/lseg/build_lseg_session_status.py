#!/usr/bin/env python3
"""Decode 2026 CME LSEG states on publication time, compare and optionally publish."""
import argparse
from bisect import bisect_left,bisect_right
from collections import Counter,defaultdict
from concurrent.futures import ThreadPoolExecutor,as_completed
import hashlib
import fcntl
import json
import os
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import build_market_status_klines as pipeline
from lseg_session_status import decode,official_event,POLICY_VERSION
import pyarrow.compute as pc
from rebuild_kline_fill_policy import local_parquet

RUN=pipeline.ROOT/'artifacts/lseg_session_status_20261111'
WORK=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_session_status_20261111')
PROBLEMS=pipeline.DEST/'meta/problems/lseg_session_status'
VERSION='1'
PUBLIC=['product','ric','ts_utc_ns','published_ns','effective_ns','action','session_state','is_trading',
    'reason','is_transition','source','source_url','qualifiers']
SCHEMA=pa.schema([('product',pa.string()),('ric',pa.string()),('ts_utc_ns',pa.uint64()),
    ('published_ns',pa.uint64()),('effective_ns',pa.uint64()),('action',pa.string()),('session_state',pa.string()),
    ('is_trading',pa.bool_()),('reason',pa.string()),('is_transition',pa.bool_()),('source',pa.string()),
    ('source_url',pa.string()),('qualifiers',pa.string()),('issue_code',pa.string())])


def metadata():
    return {b'source':b'LSEG TAS Mkt. Condition qualifiers; reviewed CME official supplements when available',
        b'session_status_schema_version':VERSION.encode(),b'time_basis':b'LSEG ts_utc_ns=published_ns=effective_ns; no rounding or inferred exchange time; official supplements apply at max(publication,effective)',
        b'decoder_policy_version':POLICY_VERSION.encode(),
        b'expiration_policy':b'CME-only native HALT_REASN=4 -> EXPIRE/EXPIRED/INSTRUMENT_EXPIRATION; terminal within this RIC identity; high-confidence native binding inferred from CME Tag 327 and same-RIC Databento overlap',
        b'phase_scope':b'CME ES/NQ/RTY/YM/GC/CL/SRA/S1R/URO/KWR; qualifier mappings calibrated against existing Databento overlap; outside overlap is rule extrapolation',
        b'reason':b'UNSPECIFIED does not assert scheduled or temporary closure; native status, official reason enums and publication times do not reproduce all Databento reasons',
        b'initial_state':b'UNKNOWN until first explicit marker; no synthetic daily snapshots; read preceding files to carry state across UTC days',
        b'open':b'OPEN combines native 15 and 17/T; does not recreate separate NEW_PRICE_INDICATION and TRADING messages',
        b'raw_policy':b'all original raw status records retained separately in status.parquet; auxiliary/duplicate rows preserved here; is_transition marks state changes',
        b'official_policy':b'official URL, exact publication/effective time, reviewed evidence digest and product/RIC scope required; supplements are not a complete official incident archive'}


def supplements(path):
    if path is None:return []
    records=json.loads(path.read_text());result=[]
    for record in records:
        event=official_event(record)
        proof=Path(record['evidence_path'])
        assert hashlib.sha256(proof.read_bytes()).hexdigest()==record['evidence_sha256'],proof
        assert record.get('reviewed') is True,'official scope/text must be reviewed'
        result.append(event)
    return result


def prepare(official):
    source_paths=sorted((WORK/'raw').glob('*.parquet'))
    source_plan=json.loads((RUN/'source_plan.json').read_text())
    assert {p.stem for p in source_paths}=={j['id'] for j in source_plan['jobs']}
    mapping=json.loads((pipeline.ROOT/'artifacts/tas_status_20261109/plan.json').read_text())['exchanges']
    contracts=json.loads((pipeline.RUN/'contracts.json').read_text())
    mapping.update({ric:c['exchange'] for ric,c in contracts.items() if c['exchange']!='review'})
    c=pipeline.connection()
    try:
        raw=c.execute('SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY ric,ts_utc_ns,source_index,source_order',[list(map(str,source_paths))]).fetch_arrow_table()
    finally:c.close()
    groups=defaultdict(list);unmapped=[]
    for row in raw.to_pylist():
        if mapping.get(row['ric']) not in {'cme','cbot','nymex','comex'}:
            unmapped.append(dict(ric=row['ric'],product=row['product'],problem='缺少已核定CME Group交易所归属；保留原状态，未发布推断状态'))
        else:groups[row['ric']].append(row)
    if unmapped:
        rows=list({(r['ric'],r['product']):r for r in unmapped}.values())
        pipeline.write_csv(PROBLEMS/'identity.csv',rows,list(rows[0]))
    output=[];issues=[];supplement_issues=[]
    lives=json.loads((pipeline.ROOT/'artifacts/kline_continuous_v5_20261110/lifetimes.json').read_text())
    for ric,rows in groups.items():
        products={r['product'] for r in rows};assert len(products)==1,(ric,products)
        product=next(iter(products));events=[(r['ts_utc_ns'],r['source_index'],r['source_order'],r) for r in rows]
        for event in official:
            if product not in event['products'] or not pipeline.START<=event['ts_utc_ns']<pipeline.YEAR_END:continue
            if event.get('rics') is not None:
                if ric not in event['rics']:continue
            else:
                # Product-wide notices only expand to identities with a known
                # lifetime. Explicit RIC supplements can target an unseeded RIC.
                life=lives.get(ric)
                if not life or life['activation_ns'] is None or life['expiration_ns'] is None:
                    supplement_issues.append(dict(ric=ric,problem='CME品种级公告缺少可核定的合约有效期，未猜测扩展',source_url=event['source_url']));continue
                if not life['activation_ns']<=event['ts_utc_ns']<life['expiration_ns']:continue
                if rows[0]['ts_utc_ns']>event['published_ns']:continue
            events.append((event['ts_utc_ns'],2**32,0,event))
        events.sort(key=lambda e:e[:3]);previous='UNKNOWN'
        for at,_,_,row in events:
            if row.get('source')=='CME_OFFICIAL':
                parsed=dict(action=row['action'],session_state=row['session_state'],is_trading=row['is_trading'],reason='OFFICIAL_ANNOUNCEMENT',issue_code=None)
                published=row['published_ns'];effective=row['effective_ns'];source='CME_OFFICIAL';url=row['source_url'];text=row.get('text','')
                if previous=='EXPIRED':
                    parsed.update(session_state='EXPIRED',is_trading=False,reason='INSTRUMENT_EXPIRATION',
                        issue_code='official_session_marker_after_expiration' if row['session_state']!='CLOSED' else None)
            else:
                parsed=decode(row['qualifiers'],previous,product);published=effective=at;source='LSEG_TAS';url=None;text=row['qualifiers']
            result=dict(product=product,ric=ric,ts_utc_ns=at,published_ns=published,effective_ns=effective,
                is_transition=previous!=parsed['session_state'],source=source,source_url=url,qualifiers=text,**parsed)
            previous=parsed['session_state'];output.append(result)
            if parsed['issue_code']:
                issues.append(dict(ric=ric,product=product,published_utc=pd.Timestamp(published,unit='ns',tz='UTC').isoformat(),
                    issue=parsed['issue_code'],qualifiers=text))
    assert output
    table=pa.Table.from_pylist(output,schema=SCHEMA).replace_schema_metadata(metadata())
    local_parquet(table,WORK/'decoded.parquet')
    for name,rows in [('qualifier_issues.csv',issues),('official_scope.csv',supplement_issues)]:
        path=PROBLEMS/name
        if rows:pipeline.write_csv(path,rows,list(rows[0]))
        else:path.unlink(missing_ok=True)
    return table,mapping,dict(raw_rows=len(raw),decoded_rows=len(table),unmapped_rows=len(unmapped),
        rics=len(groups),official_rows=sum(r['source']=='CME_OFFICIAL' for r in output),
        action_counts=dict(Counter(r['action'] for r in output)),issue_rows=len(issues),
        first_publication=pd.Timestamp(min(r['published_ns'] for r in output),unit='ns',tz='UTC').isoformat(),
        last_publication=pd.Timestamp(max(r['published_ns'] for r in output),unit='ns',tz='UTC').isoformat())


def write_unknown_intervals(table):
    groups=defaultdict(list)
    for row in table.select(['product','ric','ts_utc_ns','session_state']).to_pylist():groups[row['ric']].append(row)
    issues=[]
    for ric,rows in groups.items():
        start=last=None
        for row in rows:
            if row['session_state']=='UNKNOWN':
                if start is None:start=row['ts_utc_ns']
                last=row['ts_utc_ns']
            elif start is not None:
                if row['ts_utc_ns']>start:
                    issues.append(dict(ric=ric,product=row['product'],start_utc=pd.Timestamp(start,unit='ns',tz='UTC').isoformat(),
                        next_known_utc=pd.Timestamp(row['ts_utc_ns'],unit='ns',tz='UTC').isoformat(),
                        last_unknown_message_utc=pd.Timestamp(last,unit='ns',tz='UTC').isoformat(),
                        problem='已观察到消息但尚无充分状态证据；未把空字段或正常标记当成已开市'))
                start=last=None
        if start is not None:
            issues.append(dict(ric=ric,product=rows[-1]['product'],start_utc=pd.Timestamp(start,unit='ns',tz='UTC').isoformat(),
                next_known_utc=None,last_unknown_message_utc=pd.Timestamp(last,unit='ns',tz='UTC').isoformat(),
                problem='源记录结束时仍未知；不外推为后续正常交易'))
    lives=json.loads((pipeline.ROOT/'artifacts/kline_continuous_v5_20261110/lifetimes.json').read_text())
    candidates_path=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/candidate_catalog_parts/contracts_fx_2020_2026.json')
    candidates={r['ric']:r for r in json.loads(candidates_path.read_text())} if candidates_path.exists() else {}
    for issue in issues:
        start=int(pd.Timestamp(issue['start_utc']).value)
        end=int(pd.Timestamp(issue['next_known_utc']).value) if issue['next_known_utc'] else None
        activation=lives.get(issue['ric'],{}).get('activation_ns')
        issue.update(context='UNKNOWN_SOURCE_STATE',context_basis='尚不能确定所在交易阶段')
        if activation is not None and end is not None and end<=activation:
            issue.update(context='BEFORE_REFERENCE_ACTIVATION',context_basis='该段结束早于已核定definition激活时刻，不作为盘中停牌缺口')
        elif issue['ric'] in candidates and end is not None:
            candidate=candidates[issue['ric']]
            if candidate['from']=='2026-05-11' and candidate['from']>pd.Timestamp(end,unit='ns',tz='UTC').strftime('%Y-%m-%d'):
                issue.update(context='PRE_LISTING_CANDIDATE',context_basis='SER-9719及既有候选目录的2026-05-11交易日扩展；仍保留UNKNOWN，不填正常交易')
    path=PROBLEMS/'unknown_intervals.csv'
    if issues:pipeline.write_csv(path,issues,list(issues[0]))
    else:path.unlink(missing_ok=True)
    return len(issues)


def compare(table):
    c=pipeline.connection()
    c.register('decoded',table)
    try:
        reference=c.execute('''SELECT product,ric,category,ts_event_ns,min(ts_recv_ns)::UBIGINT reference_ns FROM (
            SELECT *,CASE WHEN action_name='CLOSE' THEN 'CLOSE'
                WHEN action_name='PRE_OPEN' AND trading_event_name<>'NO_CANCEL' THEN 'PRE_OPEN'
                WHEN action_name IN ('TRADING','NEW_PRICE_INDICATION') AND is_trading='Y' THEN 'OPEN'
                WHEN action_name='HALT' THEN 'HALT'
                WHEN action_name='NOT_AVAILABLE_FOR_TRADING' THEN 'NOT_AVAILABLE'
                END category FROM read_parquet(?) WHERE NOT is_midnight_snapshot)
            WHERE category IS NOT NULL GROUP BY product,ric,category,ts_event_ns ORDER BY ric,category,ts_event_ns''',
            [str(pipeline.WORK/'status.parquet')]).fetch_arrow_table().to_pylist()
        observed=c.execute("SELECT DISTINCT ric,CASE WHEN action='EXPIRE' THEN 'NOT_AVAILABLE' ELSE action END AS \"action\",ts_utc_ns FROM decoded WHERE source='LSEG_TAS' AND action IN ('CLOSE','PRE_OPEN','OPEN','HALT','NOT_AVAILABLE','EXPIRE') ORDER BY ric,action,ts_utc_ns").fetchall()
        reference_states=c.execute("SELECT ric,ts_recv_ns,is_trading FROM read_parquet(?) WHERE is_trading IN ('Y','N') ORDER BY ric,ts_recv_ns,source_index,source_order",
            [str(pipeline.WORK/'status.parquet')]).fetchall()
        reference_expirations=dict(c.execute("SELECT ric,min(ts_event_ns) FROM read_parquet(?) WHERE reason_name='INSTRUMENT_EXPIRATION' AND NOT is_midnight_snapshot AND ts_event_ns<9223372036854775808 GROUP BY ric",
            [str(pipeline.WORK/'status.parquet')]).fetchall())
    finally:c.close()
    groups=defaultdict(list)
    for ric,action,at in observed:groups[(ric,action)].append(at)
    state_groups=defaultdict(list)
    for row in table.select(['ric','ts_utc_ns','is_trading']).to_pylist():state_groups[row['ric']].append((row['ts_utc_ns'],row['is_trading']))
    state_times={ric:[r[0] for r in rows] for ric,rows in state_groups.items()}
    db_states=defaultdict(list)
    for ric,at,flag in reference_states:db_states[ric].append((at,flag=='Y'))
    db_times={ric:[r[0] for r in rows] for ric,rows in db_states.items()}
    lives=json.loads((pipeline.ROOT/'artifacts/kline_continuous_v5_20261110/lifetimes.json').read_text())
    for ric,at in reference_expirations.items():
        life=dict(lives.get(ric,{}));old=life.get('expiration_ns')
        life['expiration_ns']=min(old,at) if old is not None else at;lives[ric]=life
    outside_lifetime=0;stats=defaultdict(Counter);offsets=defaultdict(list);problems=[]
    for row in reference:
        life=lives.get(row['ric'],{})
        if (life.get('activation_ns') is not None and row['ts_event_ns']<life['activation_ns']) or (life.get('expiration_ns') is not None and row['ts_event_ns']>=life['expiration_ns']):
            outside_lifetime+=1;continue
        category=row['category'];key=(row['product'],category);stats[key]['reference_events']+=1
        times=groups[(row['ric'],category)];pos=bisect_left(times,row['reference_ns'])
        nearest=min(times[max(0,pos-1):pos+1],key=lambda x:abs(x-row['reference_ns'])) if times else None
        error=None if nearest is None else (nearest-row['reference_ns'])/1e9
        matched=error is not None and abs(error)<=2
        state_index=bisect_right(state_times.get(row['ric'],[]),row['reference_ns']+2_000_000_000)-1
        state=state_groups[row['ric']][state_index][1] if state_index>=0 else None
        db_index=bisect_right(db_times.get(row['ric'],[]),row['reference_ns']+2_000_000_000)-1
        expected=db_states[row['ric']][db_index][1] if db_index>=0 else None
        agrees=state is not None and state==expected
        stats[key]['state_agrees_after_2s']+=agrees
        if matched:stats[key]['matched_within_2s']+=1;offsets[key].append(error)
        if (not matched and category in {'CLOSE','PRE_OPEN','OPEN','HALT'}) or not agrees:
            problems.append(dict(ric=row['ric'],product=row['product'],action=category,
                databento_publication_utc=pd.Timestamp(row['reference_ns'],unit='ns',tz='UTC').isoformat(),
                databento_event_utc=pd.Timestamp(row['ts_event_ns'],unit='ns',tz='UTC').isoformat(),
                nearest_lseg_publication_utc=pd.Timestamp(nearest,unit='ns',tz='UTC').isoformat() if nearest is not None and abs(error)<86400 else None,
                difference_seconds=error if error is not None and abs(error)<86400 else None,
                state_agrees_after_2s=agrees,
                problem='参考事件附近无同类LSEG标记或两秒后可交易状态未一致；保留源时间，不平移、不伪造补齐'))
    result=[]
    for (product,action),values in sorted(stats.items()):
        n=values['reference_events'];m=values['matched_within_2s'];d=offsets[(product,action)]
        result.append(dict(product=product,action=action,reference_events=n,matched_within_2s=m,
            recall_pct=100*m/n,median_publication_difference_s=float(np.median(d)) if d else None,
            state_agrees_after_2s=values['state_agrees_after_2s'],state_agreement_after_2s_pct=100*values['state_agrees_after_2s']/n,
            p99_abs_publication_difference_s=float(np.percentile(np.abs(d),99)) if d else None))
    pipeline.save(RUN/'comparison.json',result)
    if problems:pipeline.write_csv(PROBLEMS/'databento_differences.csv',problems,list(problems[0]))
    else:(PROBLEMS/'databento_differences.csv').unlink(missing_ok=True)
    core=[r for r in result if r['action'] in {'CLOSE','PRE_OPEN','OPEN'}]
    n=sum(r['reference_events'] for r in core);m=sum(r['matched_within_2s'] for r in core)
    return dict(core_reference_events=n,core_matched_2s=m,core_recall_pct=100*m/n,
        reference_outside_verified_lifetime=outside_lifetime,comparison_problem_rows=len(problems),by_product=result)


def publish_session_file(table,target,sampled):
    """Derived status updates may add an official event without touching raw data."""
    assert target.name=='session_status.parquet'
    sink=pa.BufferOutputStream()
    with pa.ipc.new_stream(sink,table.schema.remove_metadata()) as writer:writer.write_table(table.replace_schema_metadata(None))
    content_hash=hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest().encode()
    md=dict(table.schema.metadata or {});md[b'status_content_sha256']=content_hash
    table=table.replace_schema_metadata(md)
    part=dict(rows=len(table),ric=table['ric'][0].as_py(),utc_date=target.parent.name)
    if target.exists():
        footer=pipeline.check_footer(target,{**part,'rows':None})
        prior=footer.schema_arrow.metadata or {}
        assert prior.get(b'source')==md[b'source'] and prior.get(b'session_status_schema_version')==VERSION.encode(),target
        assert footer.schema_arrow.remove_metadata().equals(table.schema.remove_metadata()),target
        if prior.get(b'status_content_sha256')==content_hash:
            assert footer.metadata.num_rows==len(table),target
            if sampled:assert footer.read().equals(table,check_metadata=False),target
            return False
    target.parent.mkdir(parents=True,exist_ok=True)
    temp=target.with_name('.'+target.name+'.partial');pq.write_table(table,temp,compression='zstd')
    with temp.open('rb') as stream:os.fsync(stream.fileno())
    footer=pipeline.check_footer(temp,part)
    assert footer.schema_arrow.equals(table.schema,check_metadata=True),target
    if sampled:assert footer.read().equals(table,check_metadata=False),target
    temp.replace(target)
    return True


def publication_changes(table,baseline):
    public=table.select(PUBLIC)
    old=baseline.select(PUBLIC)
    assert len(public)==len(old),'row set changed; this migration only reinterprets existing source records'
    stable=['product','ric','ts_utc_ns','published_ns','effective_ns','source','source_url','qualifiers']
    assert public.select(stable).equals(old.select(stable),check_metadata=False),'source identities/times/text changed'
    changed=np.zeros(len(public),dtype=bool)
    for name in PUBLIC:
        if name in stable:continue
        a,b=public[name],old[name]
        same=pc.or_(pc.fill_null(pc.equal(a,b),False),pc.and_(pc.is_null(a),pc.is_null(b)))
        changed|=~same.to_numpy(zero_copy_only=False)
    frame=public.select(['product','ric','ts_utc_ns']).to_pandas()
    keys={(str(r.product),str(r.ric),int(r.ts_utc_ns)//pipeline.DAY) for r in frame.iloc[np.flatnonzero(changed)].itertuples(index=False)}
    return keys,dict(changed_rows=int(changed.sum()),changed_days=len(keys))


def publish(table,mapping,workers,baseline=None):
    public=table.select(PUBLIC).replace_schema_metadata(metadata());frame=public.to_pandas()
    selected,delta=publication_changes(table,baseline) if baseline is not None else (None,{})
    groups=[]
    for (product,ric,day),rows in frame.groupby(['product','ric',frame.ts_utc_ns//pipeline.DAY],sort=True):
        if selected is not None and (product,ric,int(day)) not in selected:continue
        groups.append((product,ric,int(day),rows.index.to_numpy()))
    by_product=defaultdict(list)
    for i,row in enumerate(groups):by_product[row[0]].append(i)
    samples=set()
    for indexes in by_product.values():
        indexes.sort(key=lambda i:(groups[i][2],groups[i][1]))
        samples.update([indexes[0],indexes[len(indexes)//2],indexes[-1]])
    def one(index,row):
        product,ric,day,indices=row
        date=pd.Timestamp(day*pipeline.DAY,unit='ns',tz='UTC').strftime('%Y%m%d')
        target=pipeline.DEST/mapping[ric]/'2026'/quote(ric,safe='_-.^')/date/'session_status.parquet'
        return publish_session_file(public.take(pa.array(indices)),target,index in samples)
    written=0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(one,i,row) for i,row in enumerate(groups)]
        for f in as_completed(futures):written+=bool(f.result())
    return dict(files=len(groups),written_files=written,full_readback_sample_files=len(samples),
        mode='changed_days_only' if baseline is not None else 'all_days',**delta)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--publish',action='store_true')
    parser.add_argument('--official',type=Path);parser.add_argument('--workers',type=int,default=16)
    parser.add_argument('--baseline',type=Path,help='only publish UTC days whose interpretation differs from this local baseline')
    parser.add_argument('--preview',action='store_true',help='keep problem outputs local and do not publish data')
    args=parser.parse_args();RUN.mkdir(parents=True,exist_ok=True);WORK.mkdir(parents=True,exist_ok=True)
    if args.preview:
        global PROBLEMS
        PROBLEMS=RUN/'preview_problems'
        assert not args.publish,'preview cannot publish'
    with (RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        registry=RUN/'official_registry.json'
        official=supplements(registry) if registry.exists() else []
        if args.official:
            known={json.dumps(r,sort_keys=True) for r in official}
            official+=[r for r in supplements(args.official) if json.dumps(r,sort_keys=True) not in known]
        pipeline.save(registry,official)
        table,mapping,summary=prepare(official);summary['unknown_intervals']=write_unknown_intervals(table);summary['comparison']=compare(table)
        baseline=pq.ParquetFile(args.baseline).read() if args.baseline else None
        if baseline is not None:summary['delta_from_baseline']=publication_changes(table,baseline)[1]
        if args.publish:summary['delivery']=publish(table,mapping,args.workers,baseline)
        summary['decoder_policy_version']=POLICY_VERSION
        pipeline.save(RUN/('preview_state.json' if args.preview else 'state.json'),summary)
        (PROBLEMS/'execution_failure.json').unlink(missing_ok=True)
        print(json.dumps(summary,ensure_ascii=False),flush=True)


if __name__=='__main__':
    try:main()
    except Exception as error:
        import traceback
        pipeline.save(PROBLEMS/'execution_failure.json',dict(problem=str(error),traceback=traceback.format_exc()));raise
