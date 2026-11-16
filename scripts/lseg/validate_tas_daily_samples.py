#!/usr/bin/env python3
"""Verify new daily TAS template against independent trade and BBO bytes."""
import csv
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys

from download_tas_daily import download

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
RUN=ROOT/'artifacts/tas_daily_ingest_20261109/samples_v2'
BIN=Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release')
PERIOD='2026-06-01_2026-06-02'


def sample(product):
    rows=list(csv.DictReader((ROOT/'artifacts/tas_remote_refresh_20261109/daily_tas_files.csv').open(encoding='utf-8-sig')))
    row=next(r for r in rows if r['product']==product and r['date']=='2026-06-01')
    source=Path(download(row)['source_file'])
    folder=RUN/product
    folder.mkdir(parents=True,exist_ok=True)
    input_dir=folder/'input'/('shanghai_evolution_futures_time_and_sales_ric_list_0_tas_'+PERIOD)
    input_dir.mkdir(parents=True,exist_ok=True)
    link=input_dir/'merged-Data-part-000000.csv.gz'
    if not link.exists():link.symlink_to(source)
    db=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/ly_cme_data_validation/tas_daily_20261109_v2')/product
    config=dict(data_root=str(folder/'input'),period=PERIOD,rocksdb_dir=str(db),workers=2,
        parse_workers=2,parse_chunk_rows=8192,max_source_rows=200000,
        column_rules=str(CONFIG / 'tas_daily_column_rules.json'),progress_every=100000,
        log_path=str(folder/'replay.log'),unparsed_path=str(folder/'unparsed.log'),
        unmatched_path=str(folder/'unmatched.log'),quote_fallback_path=str(folder/'fallback.log'))
    cfg=folder/'replay.toml'
    cfg.write_text(''.join(k+' = '+json.dumps(v)+'\n' for k,v in config.items()))
    commands=[
        ('replay',[str(BIN/'cme_tas_replay_all'),'--config',str(cfg)]),
        ('audit',[str(BIN/'tas_db_audit'),'--db',str(db),'--source-period',PERIOD,'--output',str(folder/'db_audit.json'),'--dump',str(folder/'dump.jsonl')]),
        ('baseline',[sys.executable,str(SCRIPTS / 'verify_tas_ingest_sample.py'),str(source),str(folder/'dump.jsonl'),
                     '--rows','200000','--output',str(folder/'baseline.json')]),
    ]
    for name,cmd in commands:
        if name=='replay' and db.exists():
            previous=(folder/'replay.log').read_text()
            assert 'source_rows=200000' in previous and 'skipped_unparsed=0' in previous
            continue
        with (folder/(name+'.stdout.log')).open('w') as log:
            subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)
    return product,json.loads((folder/'baseline.json').read_text())


if __name__=='__main__':
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=dict(pool.map(sample,['ES','GC','LGO']))
    (RUN/'completion.json').write_text(json.dumps(dict(status='passed',samples=results),indent=2)+'\n')
    print(json.dumps(results,ensure_ascii=False,indent=2))
