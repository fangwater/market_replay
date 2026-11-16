#!/usr/bin/env python3
"""Exercise official reopening supplement with the already reviewed 2025 incident."""
import hashlib
import json

import pandas as pd

import build_market_status_klines as pipeline
from build_lseg_session_status import RUN
from lseg_session_status import decode,official_event


def verify():
    root=pipeline.Path('/home/fanghaizhou/preprocess/database/cme_causal_resume_20261025')
    review=json.loads((root/'outage_2025/review.json').read_text())
    source=root/'outage_2025/cme_resume_syndication.json'
    digest=hashlib.sha256(source.read_bytes()).hexdigest()
    assert review['input_sha256'][str(source)]==digest
    raw=json.loads(source.read_text());opening=review['reopening']
    assert raw['user']['screen_name']=='CMEGroup' and raw['user']['id_str']=='15935591'
    assert not raw['isEdited'] and not raw['isStaleEdit']
    assert raw['id_str']=='1994395477840990459'
    assert int(pd.Timestamp(raw['created_at']).value)==opening['known_at_ns']
    record=dict(source_url=opening['source_url'],published_ns=opening['known_at_ns'],effective_ns=opening['effective_ns'],
        products=opening['products'],session_state='TRADING',text=raw['text'],
        evidence_path=str(source),evidence_sha256=digest,reviewed=True)
    event=official_event(record)
    assert event['ts_utc_ns']==int(pd.Timestamp('2025-11-28T13:30:00Z').value)
    assert event['ts_utc_ns']-event['published_ns']==690_000_000_000
    halts=json.loads((root/'status_history/thanksgiving_2025_events.json').read_text())['halt_events']
    assert len(halts)==135
    for row in halts:
        assert decode(row['qualifiers'],previous='TRADING',product=row['product'])['session_state']=='HALTED'
        assert row['ts_utc_ns']<event['published_ns']<event['ts_utc_ns']
        assert row['product'] in event['products']
    pipeline.save(RUN/'official_supplements.json',[record])
    pipeline.save(RUN/'official_example_check.json',dict(halt_messages=len(halts),source_url=record['source_url'],
        publication_utc='2025-11-28T13:18:30Z',reopen_utc='2025-11-28T13:30:00Z',
        uses_future_information=False,scope='verified incident example, not a complete 2026 official-incident archive'))
    return record


if __name__=='__main__':print(json.dumps(verify(),ensure_ascii=False))
