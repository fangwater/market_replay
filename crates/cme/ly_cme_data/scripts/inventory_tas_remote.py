#!/usr/bin/env python3
"""Snapshot newly published daily TAS and compare annual/supplement sources."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, unquote
import csv
import hashlib
import json
import re
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'artifacts/tas_remote_refresh_20261109'
REMOTE = 'http://10.61.20.12/public/users/u158/public/lseg_data/'
# Private LAN source: directory listings and reports must bypass proxy env vars.
DIRECT_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class Listing(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            href = dict(attrs).get('href', '')
            if href and href != '../':
                self.links.append(href)


def fetch(path):
    url = urljoin(REMOTE, path)
    with DIRECT_HTTP.open(url, timeout=30) as response:
        raw = response.read()
    target = OUT/'listings'/(path.strip('/').replace('/', '__')+'.html')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
    parsed = Listing()
    parsed.feed(raw.decode())
    return raw.decode(), parsed.links


def csv_write(name, rows, fields=None):
    with (OUT/name).open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fields or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def daily_product(product):
    path = 'future_daily/normalised/'+product+'/'
    text, links = fetch(path)
    files, partials = [], []
    for href in links:
        name = unquote(href)
        if name.endswith('.part'):
            partials.append(name)
        match = re.fullmatch(re.escape(product)+r'_(\d{4}-\d{2}-\d{2})_tick\.csv\.gz', name)
        if not match:
            continue
        line = next(line for line in text.splitlines() if 'href="'+href+'"' in line)
        size = re.sub(r'<[^>]+>', '', line).split()[-1]
        unit = re.fullmatch(r'([0-9.]+)([KMGT]?)', size)
        estimate = int(float(unit[1])*1024**('KMGT'.index(unit[2])+1 if unit[2] else 0))
        files.append(dict(product=product, date=match[1], name=name,
                          url=urljoin(REMOTE,path+href), listing_size=size,
                          approximate_bytes=estimate, status='formal_name_not_yet_download_verified'))
    dates = {r['date'] for r in files}
    missing = []
    if dates:
        day = datetime.strptime(min(dates), '%Y-%m-%d')
        end = datetime.strptime(max(dates), '%Y-%m-%d')
        while day <= end:
            if day.strftime('%Y-%m-%d') not in dates:
                missing.append(day.strftime('%Y-%m-%d'))
            day += timedelta(days=1)
    return files, dict(product=product, files=len(files), first_date=min(dates) if dates else '',
                       last_date=max(dates) if dates else '', missing_calendar_dates='|'.join(missing),
                       partials=len(partials), partial_names='|'.join(partials),
                       approximate_bytes=sum(r['approximate_bytes'] for r in files))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    root_text, root_links = fetch('')
    _, daily_links = fetch('future_daily/normalised/')
    products = sorted(h[:-1] for h in daily_links if h.endswith('/'))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(daily_product, products))
    files = [r for entries, _ in results for r in entries]
    coverage = [row for _, row in results]
    csv_write('daily_tas_files.csv',files)
    csv_write('daily_tas_products.csv',coverage)
    # Re-check annual and supplement reports against downloaded local copies.
    annual = []
    for family in ['future','future_supplement']:
        _, jobs = fetch(family+'/normalised/')
        for job in jobs:
            if not job.endswith('/'):
                continue
            _, names = fetch(family+'/normalised/'+job)
            report_name = next((h for h in names if h=='merged-Report.csv.gz'), None)
            reference = None
            if report_name:
                url = urljoin(REMOTE,family+'/normalised/'+job+report_name)
                raw = DIRECT_HTTP.open(url,timeout=30).read()
                target = OUT/'reports'/family/job.strip('/')/report_name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
                local = Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data')/family/'normalised'/job.strip('/')/report_name
                reference = dict(remote_report_sha256=hashlib.sha256(raw).hexdigest(),
                                 local_report_exists=local.exists(),
                                 local_report_identical=local.exists() and local.read_bytes()==raw)
            formal=[h for h in names if re.fullmatch(r'merged-Data(?:-part-\d+)?\.csv\.gz',h)]
            annual.append(dict(family=family,job=job.strip('/'),formal_data_files=len(formal),
                               partial_files=sum(h.endswith('.part') for h in names),**(reference or {})))
    csv_write('annual_and_supplement_reports.csv',annual)
    result=dict(status='inventory_complete',checked_at=datetime.now(timezone.utc).isoformat(),
        remote_root=REMOTE,root_directories=[unquote(h) for h in root_links if h.endswith('/')],
        scope='Futures TAS only; daily files discovered, not yet asserted complete or ingested.',
        daily_products=len(products),daily_formal_files=len(files),
        daily_first_date=min(r['date'] for r in files),daily_last_date=max(r['date'] for r in files),
        daily_approximate_bytes=sum(r['approximate_bytes'] for r in files),
        products_with_calendar_holes=[r['product'] for r in coverage if r['missing_calendar_dates']],
        products_without_formal_data=[r['product'] for r in coverage if not r['files']],
        partial_entries=sum(r['partials'] for r in coverage),
        annual_report_checks=len(annual),
        changed_downloaded_annual_reports=[r['job'] for r in annual if r.get('local_report_exists') and not r.get('local_report_identical')])
    (OUT/'inventory.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)


if __name__=='__main__':
    main()
