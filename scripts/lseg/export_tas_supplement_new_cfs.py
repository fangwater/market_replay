#!/usr/bin/env python3
"""Publish only previously merged new supplement CFs, excluding old duplicates."""
import json
from pathlib import Path
import subprocess
from urllib.parse import quote
from datetime import datetime, timezone

from download_tas_daily import save
from group_lseg_by_exchange import references, classify
from process_tas_daily import copy_verified, sha_file, retire_legacy_files, wait_for_native_export, LAYOUT
from parquet_delivery_checks import sampled_paths
from summarize_export import summarize

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
RUN=ROOT/'artifacts/tas_supplement_parquet_20261109'
BIN=Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release')
OUTPUT=Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/supplement_ric_day_20261109')
DEST=Path('/mnt/nfs/30.100_cta_data')


def main():
    RUN.mkdir(exist_ok=True)
    legacy=None
    if (RUN/'completion.json').exists():
        previous=json.loads((RUN/'completion.json').read_text())
        if previous.get('layout')==LAYOUT:
            return
        legacy=previous
        backup=RUN/'legacy_completion_before_ric_day.json'
        if not backup.exists():save(backup,legacy)
        else:assert json.loads(backup.read_text())==legacy
    merge=json.loads((ROOT/'artifacts/tas_supplement_merge_20261103/completion.json').read_text())
    assert merge['status']=='complete_verified'
    cfs=merge['new_product_cfs']
    assert len(cfs)==13 and all(cf.startswith('p:2026:') for cf in cfs)
    products=[cf.split(':',2)[2] for cf in cfs]
    source=Path(merge['source'])
    refs=references([Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
                     ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv'])
    exchange_map=RUN/'exchange_map.json'
    source_audit=json.loads((ROOT/'artifacts/tas_supplement_ingest_20261103/db_audit.json').read_text())
    save(exchange_map,{ric:classify(ric,'futures',refs.get(ric,{}),{})['exchange'].lower() for ric in source_audit['rics']})
    wait_for_native_export(OUTPUT)
    if not (OUTPUT/'_SUCCESS.json').exists():
        if OUTPUT.exists():
            import time
            OUTPUT.rename(OUTPUT.with_name(OUTPUT.name+'.interrupted_'+str(time.time_ns())))
        cmd=['nice','-n','10',str(BIN/'lseg_tas_parquet'),'--db',str(source),'--year','2026',
             '--period-end','2026-05-30','--products',','.join(products),'--output',str(OUTPUT),'--workers','4',
             '--layout','ric-day','--exchange-map',str(exchange_map),'--readback','sampled']
        with (RUN/'export_ric_day.log').open('w') as log:
            subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
    success=json.loads((OUTPUT/'_SUCCESS.json').read_text())
    assert success['source_period']=='2026-01-01_2026-05-30'
    assert success['source_current']==source.joinpath('CURRENT').read_text()
    summarize(OUTPUT)
    delivery=json.loads((OUTPUT/'_audit/delivery.json').read_text())
    assert delivery['product_cfs']==13 and delivery['status']=='passed'
    files=[]
    reports=[json.loads(path.read_text()) for path in (OUTPUT/'_audit').glob('p%3A*.json')]
    copy_samples=sampled_paths(reports)
    for report in reports:
        assert report['cf'] in cfs and report['verified']
        for part in report['parts']:
            ric=part['ric']
            mapping=classify(ric,'futures',refs.get(ric,{}),{})
            kind='trade' if part['kind']==1 else 'quote'
            relative=Path('lseg')/mapping['exchange'].lower()/'2026'/quote(ric,safe='_-.^')/part['utc_date']/(kind+'.parquet')
            assert relative==Path(part['path']),part
            checked=part['path'] in copy_samples
            checksum=copy_verified(OUTPUT/part['path'],DEST/relative,full_hash=checked,part=part)
            files.append(dict(ric=ric,data_type=kind,exchange=mapping['exchange'],utc_date=part['utc_date'],mapping_status=mapping['mapping_status'],
                              rows=part['rows'],bytes=part['bytes'],sha256=checksum,
                              source_value_sha256=part['value_sha256'],target_relative_path=str(relative),
                              source_export_file=str(OUTPUT/part['path']),
                              target_check='full_file_sha256' if checked else 'footer_schema_rows_ric_date_and_size'))
    assert len({f['target_relative_path'] for f in files})==len(files)==delivery['files']
    result=dict(status='complete_verified',completed_at=datetime.now(timezone.utc).isoformat(),
        source_db=str(source),source_period='2026-01-01_2026-05-30',selected_cfs=cfs,
        policy='Only the 13 newly merged CFs; the five existing identical CFs are excluded.',
        excluded_cfs=['p:2026:'+p for p in ['ATWQ','ATWS','ATWY','LSU','LSUTOT']],
        delivery=delivery,files=files,original_production_db_modified=False,
        source_merge_receipt_sha256=sha_file(ROOT/'artifacts/tas_supplement_merge_20261103/completion.json'),
        layout=LAYOUT,date_basis='ts_utc_ns UTC calendar date',export_source='read-only RocksDB',
        copy_verification='three files per kind/CF SHA256; all files footer/schema/rows/RIC/date/size')
    if legacy:retire_legacy_files(legacy)
    save(RUN/'completion.json',result)


if __name__=='__main__':
    main()
