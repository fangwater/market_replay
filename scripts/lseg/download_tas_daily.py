#!/usr/bin/env python3
"""Download pinned formal daily TAS files; keep source names and metadata."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import csv
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import urllib.request

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
RUN = ROOT/'artifacts/tas_daily_ingest_20261109'
SOURCE = ROOT/'artifacts/tas_remote_refresh_20261109/daily_tas_files.csv'
RAW = Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalised')
BLOCK = 4*1024*1024
# This source is on the private LAN. Do not inherit HTTP(S)/ALL_PROXY.
DIRECT_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name+'.tmp')
    with temp.open('w') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def head(url):
    with DIRECT_HTTP.open(urllib.request.Request(url, method='HEAD'), timeout=60) as r:
        return dict(bytes=int(r.headers['Content-Length']), etag=r.headers.get('ETag'),
                    last_modified=r.headers.get('Last-Modified'))


def download(row):
    directory = RAW/row['product']
    directory.mkdir(parents=True, exist_ok=True)
    target = directory/row['name']
    sidecar = target.with_name(target.name+'.download.json')
    remote = head(row['url'])
    if target.exists():
        if not sidecar.exists():
            raise RuntimeError(f'Unrecorded pre-existing source file; preserved: {target}')
        info = json.loads(sidecar.read_text())
        if info.get('remote') != remote or target.stat().st_size != remote['bytes']:
            raise RuntimeError(f'Existing source or remote changed; preserved: {target}')
        return dict(info, reused=True)
    partial = target.with_name(target.name+'.partial')
    headers = {'If-Match':remote['etag']} if remote['etag'] else {}
    digest = hashlib.sha256()
    count = 0
    with DIRECT_HTTP.open(urllib.request.Request(row['url'], headers=headers), timeout=120) as response:
        assert int(response.headers['Content-Length']) == remote['bytes']
        with partial.open('wb') as f:
            while True:
                data = response.read(BLOCK)
                if not data:
                    break
                f.write(data)
                digest.update(data)
                count += len(data)
            f.flush()
            os.fsync(f.fileno())
    assert count == remote['bytes'], (target, count, remote['bytes'])
    assert head(row['url']) == remote, f'remote fingerprint changed during download: {target}'
    # Full gzip CRC is checked later by complete replay, not claimed here.
    with gzip.open(partial, 'rt', newline='', encoding='utf-8-sig') as f:
        fields = next(csv.reader(f),None)
    empty_gzip = fields is None
    assert empty_gzip or all(name in fields for name in ['#RIC','Date-Time','Type','Price','Volume','Qualifiers'])
    fields = fields or []
    zero = remote['bytes']==0
    info = dict(status='source_zero_byte_placeholder_downloaded' if zero else 'download_complete_empty_gzip_verified' if empty_gzip else 'download_complete_header_verified', product=row['product'], date=row['date'],
                source_url=row['url'], source_file=str(target), remote=remote, bytes=count,
                sha256=digest.hexdigest(), columns=len(fields), header_sha256=hashlib.sha256(','.join(fields).encode()).hexdigest(),
                full_gzip_crc_verified=empty_gzip and not zero, empty_gzip_payload=empty_gzip and not zero,
                source_zero_byte_placeholder=zero, completed_at=datetime.now(timezone.utc).isoformat())
    # Save the sidecar first; a crash before rename leaves only a retryable partial.
    save(sidecar,info)
    partial.replace(target)
    return info


def retry_download(row):
    for attempt in range(4):
        try:
            return download(row)
        except Exception as e:
            if attempt == 3:
                return dict(status='failed',product=row['product'],date=row['date'],source_url=row['url'],error=repr(e))
            time.sleep(2**attempt)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workers',type=int,default=8)
    p.add_argument('--products',nargs='*')
    p.add_argument('--date')
    args = p.parse_args()
    RUN.mkdir(parents=True,exist_ok=True)
    lock = (RUN/'download.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    with SOURCE.open(encoding='utf-8-sig',newline='') as f:
        rows = [r for r in csv.DictReader(f) if (not args.products or r['product'] in args.products)
                and (not args.date or r['date']==args.date)]
    sizes=Counter()
    for row in rows:sizes[row['product']]+=int(row['approximate_bytes'])
    def order(row):
        path=RAW/row['product']/row['name']
        cached=path.exists() and path.with_name(path.name+'.download.json').exists()
        return (not cached,-sizes[row['product']],row['product'],row['date'])
    rows.sort(key=order)
    expected = Counter(r['product'] for r in rows)
    by_product = {p:[] for p in expected}
    failed = []
    done = 0
    done_bytes = 0
    started = time.monotonic()
    log_path = RUN/('download_samples.jsonl' if args.date else 'downloads.jsonl')
    last_log = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool, log_path.open('a') as log:
        pending = {pool.submit(retry_download,r):r for r in rows}
        for task in as_completed(pending):
            result = task.result()
            log.write(json.dumps(result,ensure_ascii=False)+'\n')
            log.flush()
            done += 1
            if result['status']=='failed':
                failed.append(result)
            else:
                done_bytes += result['bytes']
                group = by_product[result['product']]
                group.append(result)
                if len(group)==expected[result['product']] and not args.date:
                    save(RUN/'downloads'/(result['product']+'.json'),dict(status='all_formal_files_downloaded',product=result['product'],
                        files=sorted(group,key=lambda r:r['date']),bytes=sum(r['bytes'] for r in group)))
            if time.monotonic()-last_log>10 or done==len(rows):
                progress=dict(status='downloading' if done<len(rows) else 'downloads_finished',files_done=done,files_expected=len(rows),
                              bytes_downloaded_or_reused=done_bytes,failed_files=len(failed),
                              elapsed_seconds=time.monotonic()-started,completed_products=sum((RUN/'downloads'/(p+'.json')).exists() for p in expected))
                if not args.date:save(RUN/'download_progress.json',progress)
                print(json.dumps(progress),flush=True)
                last_log=time.monotonic()
    save(RUN/('sample_download_result.json' if args.date else 'download_result.json'),
         dict(status='complete' if not failed else 'incomplete',files=len(rows),bytes=done_bytes,failures=failed))
    if failed:raise SystemExit(1)


if __name__=='__main__':
    main()
