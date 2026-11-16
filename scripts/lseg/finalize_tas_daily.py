#!/usr/bin/env python3
"""Finish internal checkpoints and publish data problems only."""
import csv
import json
import os
from pathlib import Path
from datetime import datetime, timezone

from download_tas_daily import save
from process_tas_daily import ROOT, RUN, DEST, LAYOUT


def main():
    complete=json.loads((RUN/'completion.json').read_text())
    assert complete['layout']==LAYOUT
    assert complete['products_complete']==complete['products_expected']==120
    assert complete['products_failed']==0
    assert not complete['failed_products']

    gaps=[dict(product=product,date=day,reason='源目录无正式日包')
          for product,days in complete.get('source_calendar_holes',{}).items() for day in days]
    gaps.extend(dict(product=product,date=day,reason='源正式文件名为零字节占位')
                for product,days in complete.get('source_zero_byte_dates',{}).items() for day in days)
    target=DEST/'lseg/meta/catalog/tas_daily_20261109/source_gaps.csv'
    if gaps:
        path=RUN/'source_gaps.csv'
        with path.open('w',encoding='utf-8-sig',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=['product','date','reason'])
            writer.writeheader();writer.writerows(gaps)
        if not target.exists() or target.read_bytes()!=path.read_bytes():
            target.parent.mkdir(parents=True,exist_ok=True)
            temporary=target.with_name('.source_gaps.tmp')
            with temporary.open('wb') as f:f.write(path.read_bytes());f.flush();os.fsync(f.fileno())
            temporary.replace(target)
        supply=DEST/'lseg/meta/supply.md'
        text=supply.read_text() if supply.exists() else '# LSEG 数据排错补充说明\n'
        heading='## 本批已知源缺口（2026-11-09）'
        section=heading+'\n\n'+str(len(gaps))+'个缺口项，详见[源缺口清单](catalog/tas_daily_20261109/source_gaps.csv)。\n'
        if heading in text:
            before,tail=text.split(heading,1)
            next_heading=tail.find('\n## ')
            text=before+section+(tail[next_heading:] if next_heading>=0 else '')
        else:
            text=text.rstrip()+'\n\n'+section
        temp=supply.with_name('.supply.issues.tmp')
        with temp.open('w') as f:f.write(text);f.flush();os.fsync(f.fileno())
        temp.replace(supply)

    # Private execution state is used for restart/status only; no success report
    # or normal-completion audit is published to the data delivery directory.
    progress=json.loads((RUN/'process_progress.json').read_text())
    progress.update(status='complete_available_sources_verified',final_verification='passed',
                    verified_at=datetime.now(timezone.utc).isoformat())
    save(RUN/'process_progress.json',progress)


if __name__=='__main__':
    main()
