#!/usr/bin/env python3
"""Clean downloaded daily TAS with the existing replay, verify, then publish.

Each source product uses an isolated DB and its own durable completion receipt.
The existing all-products production DB and the January-May snapshot are read-only.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta, datetime, timezone
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
import tomllib
from urllib.parse import quote

from download_tas_daily import save
from group_lseg_by_exchange import references, classify
from summarize_export import summarize
from parquet_delivery_checks import sampled_paths, check_footer

ROOT=Path(__file__).resolve().parents[1]
RUN=ROOT/'artifacts/tas_daily_ingest_20261109'
BIN=Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release')
DBROOT=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_daily_20261109')
SHARDS=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/tas_daily_shards_20261109')
EXPORT=Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/daily_20261109')
DEST=Path('/mnt/nfs/30.100_cta_data')
BATCH='tas_daily_20261109'
LAYOUT='ric_utc_day_v1'
REFS={}
EXTERNAL_NATIVE={}
OWNERS={}
OWNER_LOCK=threading.Lock()

def live_native_products():
    result={}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:argv=proc.joinpath('cmdline').read_bytes().decode().split('\0')
        except (FileNotFoundError,PermissionError,ProcessLookupError):continue
        if not argv or Path(argv[0]).name not in ['tas_daily_stage','cme_tas_replay_all','tas_db_audit','ly_cme_data']:continue
        for arg in argv:
            for prefix in [str(RUN/'products')+'/',str(DBROOT)+'/',str(SHARDS)+'/']:
                if arg.startswith(prefix):
                    result.setdefault(arg[len(prefix):].split('/')[0],[]).append(int(proc.name))
            prefix=str(RUN/'downloads')+'/'
            if arg.startswith(prefix) and arg.endswith('.json'):
                result.setdefault(Path(arg).stem,[]).append(int(proc.name))
    return result


def sha_file(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):
            h.update(b)
    return h.hexdigest()


def log_run(command, path):
    with path.open('w') as log:
        subprocess.run(command,check=True,stdout=log,stderr=subprocess.STDOUT)


def copy_verified(source,target,*,full_hash=True,part=None):
    target.parent.mkdir(parents=True,exist_ok=True)
    reference=check_footer(source,part) if part is not None else None
    if target.exists():
        assert target.stat().st_size==source.stat().st_size,target
        expected=sha_file(source) if full_hash else None
        if full_hash:assert sha_file(target)==expected,target
        if reference is not None:check_footer(target,part,reference)
        return expected
    temp=target.with_name('.'+target.name+'.partial')
    digest=hashlib.sha256() if full_hash else None
    with source.open('rb') as src,temp.open('wb') as dst:
        for block in iter(lambda:src.read(8*1024*1024),b''):
            dst.write(block)
            if digest is not None:digest.update(block)
        dst.flush();os.fsync(dst.fileno())
    assert temp.stat().st_size==source.stat().st_size,target
    expected=digest.hexdigest() if digest is not None else None
    if full_hash:assert sha_file(temp)==expected,target
    if reference is not None:check_footer(temp,part,reference)
    try:
        os.link(temp,target)
    finally:
        temp.unlink(missing_ok=True)
    return expected


def retire_legacy_files(receipt):
    """Retire recorded old deliveries once local rollback copies exist with recorded sizes."""
    for item in receipt.get('published_files',receipt.get('files',[])):
        relative=Path(item['target_relative_path'])
        assert not relative.is_absolute() and '..' not in relative.parts and relative.parts[0]=='LSEG',relative
        source=Path(item['source_export_file'])
        assert source.is_file() and source.stat().st_size==item['bytes'],source
        target=DEST/relative
        if target.exists():
            assert target.is_file() and target.stat().st_size==item['bytes'],target
            target.unlink()
        parent=target.parent
        while parent!=DEST and parent.exists():
            try:parent.rmdir()
            except OSError:break
            parent=parent.parent


def wait_for_native_export(output):
    """Adopt a running native export when restarting only its Python publisher."""
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:args=proc.joinpath('cmdline').read_bytes().decode().split('\0')
        except OSError:continue
        if not args or Path(args[0]).name!='ly_cme_data' or '--output' not in args:continue
        if args[args.index('--output')+1]!=str(output):continue
        while True:
            try:current=proc.joinpath('cmdline').read_bytes().decode().split('\0')
            except OSError:break
            if not current or current[0]!=args[0] or str(output) not in current:break
            time.sleep(1)


def process_product(product, args):
    args=argparse.Namespace(**vars(args))
    folder=RUN/'products'/product
    folder.mkdir(parents=True,exist_ok=True)
    receipt=folder/'completion.json'
    legacy=None
    if receipt.exists():
        previous=json.loads(receipt.read_text())
        if previous.get('layout')==LAYOUT:return previous
        legacy=previous
        backup=folder/'legacy_completion_before_ric_day.json'
        if not backup.exists():save(backup,legacy)
        else:assert json.loads(backup.read_text())==legacy
    running=EXTERNAL_NATIVE.get(product,[])
    for pid in set(running):
        while Path('/proc',str(pid),'cmdline').exists():
            try:
                argv=Path('/proc',str(pid),'cmdline').read_bytes().decode().split('\0')
                prefixes=[str(RUN/'products'/product)+'/',str(DBROOT/product)+'/',str(SHARDS/product)+'/']
                owns=any(any(arg.startswith(prefix) for prefix in prefixes) or arg==str(RUN/'downloads'/(product+'.json')) for arg in argv)
                if not owns:break
            except (FileNotFoundError,ProcessLookupError):break
            time.sleep(1)
    downloads=json.loads((RUN/'downloads'/(product+'.json')).read_text())
    assert downloads['status']=='all_formal_files_downloaded'
    sources=downloads['files']
    if args.large_workers and downloads['bytes']>=100_000_000_000:
        args.replay_workers=args.large_workers
        args.parse_workers=args.large_workers
    zero_dates=[s['date'] for s in sources if s['bytes']==0]
    corrected=False
    for s in sources:
        if s['bytes']==0 and s.get('status')!='source_zero_byte_placeholder_downloaded':
            s.update(status='source_zero_byte_placeholder_downloaded',full_gzip_crc_verified=False,
                     empty_gzip_payload=False,source_zero_byte_placeholder=True)
            save(Path(s['source_file']+'.download.json'),s)
            corrected=True
    if corrected:save(RUN/'downloads'/(product+'.json'),downloads)
    start=min(s['date'] for s in sources)
    end=(date.fromisoformat(max(s['date'] for s in sources))+timedelta(days=1)).isoformat()
    period=start+'_'+end
    assert start.startswith('2026-') and len(sources)<65536
    shard_root=SHARDS/product
    staged=shard_root/('shanghai_evolution_futures_time_and_sales_ric_list_0_tas_'+period)
    fingerprint=hashlib.sha256(json.dumps([(s['source_file'],s['bytes'],s['sha256']) for s in sources]).encode()).hexdigest()
    if not staged.exists():
        interrupted=staged.with_suffix('.building')
        if interrupted.exists():
            interrupted.rename(interrupted.with_name(interrupted.name+'.interrupted_'+str(time.time_ns())))
        save(folder/'progress.json',dict(stage='validating_gzip_and_staging_daily_headers',product=product,source_files=len(sources)))
        log_run(['nice','-n','10',str(BIN/'tas_daily_stage'),'--downloads',str(RUN/'downloads'/(product+'.json')),
                 '--period',period,'--output',str(staged),'--workers',str(args.replay_workers)],folder/'daily_stage.stdout.log')
        save(folder/'staging_receipt.json',dict(status='passed',raw_fingerprint=fingerprint,manifest=str(staged/'manifest.json')))
    else:
        if not (folder/'staging_receipt.json').exists():
            recovered=json.loads((staged/'manifest.json').read_text())
            assert [(s['file'],s['compressed_bytes']) for s in recovered['sources']]==[(s['source_file'],s['bytes']) for s in sources]
            save(folder/'staging_receipt.json',dict(status='passed_recovered_after_publish',raw_fingerprint=fingerprint,manifest=str(staged/'manifest.json')))
        assert json.loads((folder/'staging_receipt.json').read_text())['raw_fingerprint']==fingerprint
    stage_manifest=json.loads((staged/'manifest.json').read_text())
    source_rows=sum(s['data_rows'] for s in stage_manifest['sources'])
    assert stage_manifest['complete'] and len(stage_manifest['sources'])==len(sources)
    if source_rows==0:
        result=dict(status='complete_available_sources_verified' if zero_dates else 'complete_verified',product=product,source_period=period,source_files=len(sources),
            source_bytes=downloads['bytes'],source_rows=0,full_source_gzip_read_completed=not zero_dates,
            available_gzip_sources_full_read_verified=True,source_zero_byte_dates=zero_dates,
            db=None,db_audit=None,replay_counters={},stored=dict(rows=0,kinds={}),rics=[],
            parquet_delivery=None,published_files=[],completed_at=datetime.now(timezone.utc).isoformat(),
            original_production_db_modified=False,empty_source=True,source_fingerprint=fingerprint,
            layout=LAYOUT,date_basis='ts_utc_ns UTC calendar date')
        if legacy:retire_legacy_files(legacy)
        save(receipt,result)
        save(folder/'progress.json',dict(stage='complete_verified_empty_source',product=product))
        return result
    local_input=folder/'input'/('shanghai_evolution_futures_time_and_sales_ric_list_0_tas_'+period)
    local_input.mkdir(parents=True,exist_ok=True)
    for i,source in enumerate(sources):
        path=Path(source['source_file'])
        assert path.stat().st_size==source['bytes']
        link=local_input/f'merged-Data-part-{i:06d}.csv.gz'
        if link.is_symlink():
            assert link.resolve()==path
        elif not link.exists():
            link.symlink_to(path)
        else:
            raise RuntimeError(f'unexpected input file: {link}')
    reusable=None
    for existing in sorted((DBROOT/product).glob('attempt_*')) if (DBROOT/product).exists() else []:
        number=int(existing.name.split('_')[-1])
        old_log=folder/f'replay_{number}.log'
        old_cfg=folder/f'replay_{number}.toml'
        if not old_log.exists() or not old_cfg.exists():continue
        lines=[line for line in old_log.read_text().splitlines() if 'cme_tas_replay_all finished workers=' in line]
        if not lines:continue
        parsed={k:int(v) for k,v in re.findall(r'(\w+)=(\d+)',lines[-1])}
        old_config=tomllib.loads(old_cfg.read_text())
        if parsed.get('source_rows')==source_rows and all(parsed.get(k)==0 for k in ['skipped_unparsed','unmatched_product','skipped_after_window','skipped_ric_filter']) and old_config.get('shard_root')==str(shard_root) and old_config.get('period')==period:
            reusable=(number,existing)
    attempt=1
    while (DBROOT/product/f'attempt_{attempt}').exists():
        attempt+=1
    db=DBROOT/product/f'attempt_{attempt}'
    if reusable:attempt,db=reusable
    conf=dict(data_root=str(folder/'input'),shard_root=str(shard_root),period=period,rocksdb_dir=str(db),
        workers=args.replay_workers,parse_workers=args.parse_workers,parse_chunk_rows=16384,
        column_rules=str(ROOT/'config/tas_daily_column_rules.json'),progress_every=5000000,
        log_path=str(folder/f'replay_{attempt}.log'),unparsed_path=str(folder/f'unparsed_{attempt}.log'),
        unmatched_path=str(folder/f'unmatched_{attempt}.log'),quote_fallback_path=str(folder/f'fallback_{attempt}.log'))
    cfg=folder/f'replay_{attempt}.toml'
    if not reusable:cfg.write_text(''.join(k+' = '+json.dumps(v)+'\n' for k,v in conf.items()))
    save(folder/'progress.json',dict(stage='reusing_verified_rocksdb' if reusable else 'replaying',product=product,period=period,db=str(db),source_files=len(sources)))
    if not reusable:
        log_run(['nice','-n','10',str(BIN/'cme_tas_replay_all'),'--config',str(cfg)],folder/f'replay_{attempt}.stdout.log')
    text=(folder/f'replay_{attempt}.log').read_text()
    finished=[line for line in text.splitlines() if 'cme_tas_replay_all finished workers=' in line]
    assert finished,product
    counters={k:int(v) for k,v in re.findall(r'(\w+)=(\d+)',finished[-1])}
    assert counters['source_rows']==source_rows,(product,counters['source_rows'],source_rows)
    for key in ['skipped_unparsed','unmatched_product','skipped_after_window','skipped_ric_filter']:
        assert counters[key]==0,(product,key,counters[key])
    save(folder/'progress.json',dict(stage='auditing',product=product,source_rows=counters['source_rows']))
    audit_path=folder/f'db_audit_{attempt}.json'
    if not audit_path.exists():
        log_run(['nice','-n','10',str(BIN/'tas_db_audit'),'--db',str(db),'--require-done',period,
                 '--output',str(audit_path)],folder/f'audit_{attempt}.stdout.log')
    audit=json.loads(audit_path.read_text())
    assert audit['status']=='passed'
    assert audit['source_period']==period and audit['watermarks']['period:'+period]=='done'
    kinds=audit['all']['kinds']
    assert kinds.get('1',0)==counters['written_trades']
    assert kinds.get('2',0)==counters['written_specials']
    assert kinds.get('3',0)<=counters['written_quotes']
    # Prevent the same literal RIC from being published twice from two source
    # product directories without a source-level overlap decision.
    with OWNER_LOCK:
        for ric in audit['rics']:
            assert OWNERS.get(ric,product)==product,(ric,OWNERS.get(ric),product)
        OWNERS.update({ric:product for ric in audit['rics']})
    local_export=EXPORT/product/f'ric_day_attempt_{attempt}'
    published=[]
    delivery=None
    if kinds.get('1',0)+kinds.get('3',0)>0:
        save(folder/'progress.json',dict(stage='exporting_and_readback',product=product,stored_rows=audit['all']['rows']))
        if not (local_export/'_SUCCESS.json').exists():
            if local_export.exists():local_export.rename(local_export.with_name(local_export.name+'.interrupted_'+str(time.time_ns())))
            exchange_map=folder/'exchange_map.json'
            save(exchange_map,{ric:classify(ric,'futures',REFS.get(ric,{}),{})['exchange'].lower() for ric in audit['rics']})
            log_run(['nice','-n','10',str(BIN/'ly_cme_data'),'--db',str(db),'--year','2026',
                     '--period-start',start,'--period-end',end,'--output',str(local_export),
                     '--layout','ric-day','--exchange-map',str(exchange_map),
                     '--readback','sampled',
                     '--workers',str(args.export_workers)],folder/f'export_ric_day_{attempt}.stdout.log')
        summarize(local_export)
        delivery=json.loads((local_export/'_audit/delivery.json').read_text())
        assert delivery['trade_rows']==kinds.get('1',0) and delivery['quote_rows']==kinds.get('3',0)
        save(folder/'progress.json',dict(stage='publishing',product=product,files=delivery['files']))
        reports=[json.loads(p.read_text()) for p in (local_export/'_audit').glob('p%3A*.json')]
        copy_samples=sampled_paths(reports)
        for report in reports:
            for part in report['parts']:
                ric=part['ric']
                mapping=classify(ric,'futures',REFS.get(ric,{}),{})
                exchange=mapping['exchange']
                kind='trade' if part['kind']==1 else 'quote'
                relative=Path('lseg')/exchange.lower()/'2026'/quote(ric,safe='_-.^')/part['utc_date']/(kind+'.parquet')
                assert relative==Path(part['path']),part
                source=local_export/part['path']
                target=DEST/relative
                checked=part['path'] in copy_samples
                checksum=copy_verified(source,target,full_hash=checked,part=part)
                published.append(dict(ric=ric,data_type=kind,exchange=exchange,utc_date=part['utc_date'],mapping_status=mapping['mapping_status'],
                    mapping_method=mapping['mapping_method'],source_export_file=str(source),target_relative_path=str(relative),
                    rows=part['rows'],bytes=part['bytes'],sha256=checksum,first=part['first'],last=part['last'],
                    source_value_sha256=part['value_sha256']))
                published[-1]['target_check']='full_file_sha256' if checked else 'footer_schema_rows_ric_date_and_size'
    result=dict(status='complete_available_sources_verified' if zero_dates else 'complete_verified',product=product,source_period=period,source_files=len(sources),
        source_bytes=downloads['bytes'],source_rows=counters['source_rows'],full_source_gzip_read_completed=not zero_dates,
        available_gzip_sources_full_read_verified=True,source_zero_byte_dates=zero_dates,
        db=str(db),db_audit=str(audit_path),replay_counters=counters,stored=audit['all'],rics=list(audit['rics']),
        parquet_delivery=delivery,published_files=published,completed_at=datetime.now(timezone.utc).isoformat(),
        source_fingerprint_manifest_sha256=sha_file(RUN/'downloads'/(product+'.json')),
        original_production_db_modified=False,quote_sampling='existing one-second BBO, no additional resampling',
        time_scope='source publication time; per-product available daily files only; no calendar gap filling',
        layout=LAYOUT,date_basis='ts_utc_ns UTC calendar date',export_source='read-only RocksDB',
        copy_verification='three files per kind/CF SHA256; all files footer/schema/rows/RIC/date/size')
    if legacy:retire_legacy_files(legacy)
    save(receipt,result)
    save(folder/'progress.json',dict(stage='complete_verified',product=product,files=len(published)))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--process-workers',type=int,default=2)
    p.add_argument('--replay-workers',type=int,default=4)
    p.add_argument('--parse-workers',type=int,default=4)
    p.add_argument('--export-workers',type=int,default=4)
    p.add_argument('--large-workers',type=int)
    p.add_argument('--products',nargs='*')
    a=p.parse_args()
    assert a.replay_workers>=2,'one replay worker selects only part_index; use at least two'
    assert json.loads((RUN/'samples_v2/completion.json').read_text())['status']=='passed'
    assert json.loads((RUN/'adapter_validation/completion.json').read_text())['status']=='passed'
    assert json.loads((RUN/'ric_day_validation/completion.json').read_text())['status']=='passed'
    assert DEST.is_dir()
    lock=(RUN/'process.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    global REFS,EXTERNAL_NATIVE
    REFS=references([Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
                     ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv'])
    with (ROOT/'artifacts/tas_remote_refresh_20261109/daily_tas_products.csv').open(encoding='utf-8-sig') as f:
        inventory=[r for r in csv.DictReader(f) if not a.products or r['product'] in a.products]
    expected={r['product'] for r in inventory}
    weights={r['product']:int(r['approximate_bytes']) for r in inventory}
    EXTERNAL_NATIVE=live_native_products()
    attached=set(EXTERNAL_NATIVE)
    submitted=set()
    results={}
    errors={}
    futures={}
    legacy_products=set()
    completed_exports={p.parents[1].name for p in EXPORT.glob('*/ric_day_attempt_*/_SUCCESS.json')}
    for product in expected:
        done=RUN/'products'/product/'completion.json'
        if done.exists():
            r=json.loads(done.read_text())
            if r.get('layout')==LAYOUT:
                results[product]=r;submitted.add(product)
            else:legacy_products.add(product)
            OWNERS.update({ric:product for ric in r['rics']})
    with ThreadPoolExecutor(max_workers=a.process_workers) as pool:
        while len(results)+len(errors)<len(expected):
            candidates=sorted(expected-submitted,key=lambda p:(p not in attached,p not in completed_exports,p not in legacy_products,-weights[p],p))
            for product in candidates:
                if len(futures)>=a.process_workers:break
                if (RUN/'downloads'/(product+'.json')).exists():
                    futures[pool.submit(process_product,product,a)]=product
                    submitted.add(product)
            for future,product in list(futures.items()):
                if not future.done():continue
                try:
                    results[product]=future.result()
                except Exception as e:
                    import traceback
                    errors[product]=dict(error=repr(e),traceback=traceback.format_exc())
                    save(RUN/'products'/product/'failure.json',errors[product])
                    save(DEST/'lseg/meta/problems'/BATCH/(product+'.json'),errors[product])
                    print(f'{product}: FAILED {e!r}',flush=True)
                del futures[future]
            progress=dict(status='processing',layout=LAYOUT,products_complete=len(results),products_failed=len(errors),
                products_previously_cleaned=len(legacy_products),products_legacy_layout_pending=len(legacy_products-set(results)),
                products_expected=len(expected),products_submitted=len(submitted),
                source_rows_complete=sum(r['source_rows'] for r in results.values()),
                trade_rows_complete=sum(r['stored']['kinds'].get('1',0) for r in results.values()),
                quote_rows_complete=sum(r['stored']['kinds'].get('3',0) for r in results.values()),
                output_files_complete=sum(len(r['published_files']) for r in results.values()),
                failed_products=errors,updated_at=datetime.now(timezone.utc).isoformat())
            save(RUN/'process_progress.json',progress)
            download_end=RUN/'download_result.json'
            if download_end.exists() and not futures and expected-submitted:
                remaining=sorted(expected-submitted)
                if not any((RUN/'downloads'/(p+'.json')).exists() for p in remaining):
                    raise RuntimeError(f'downloads finished but products incomplete: {remaining}')
            if len(results)+len(errors)<len(expected):time.sleep(5)
    if errors:raise RuntimeError(f'failed products: {sorted(errors)}')
    files=[row for r in results.values() for row in r['published_files']]
    assert len({r['target_relative_path'] for r in files})==len(files)
    complete=dict(progress,status='complete_available_sources_verified',products=sorted(results),
        source_scope='120-product daily TAS listing snapshot, not all products through one common date',
        source_inventory=str(ROOT/'artifacts/tas_remote_refresh_20261109/inventory.json'),
        source_calendar_holes={'NQ':['2026-11-04']},
        source_zero_byte_dates={p:r.get('source_zero_byte_dates',[]) for p,r in results.items() if r.get('source_zero_byte_dates')},
        files=files,original_production_db_modified=False)
    name='completion.json' if not a.products else 'subset_completion_'+'_'.join(sorted(expected))+'.json'
    if a.products:complete['source_scope']='Selected source products only: '+','.join(sorted(expected))
    save(RUN/name,complete)


if __name__=='__main__':
    main()
