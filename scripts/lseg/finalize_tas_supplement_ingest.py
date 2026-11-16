#!/usr/bin/env python3
"""Publish completion only after source, replay and persisted DB audits agree."""
import argparse
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re

def finalize(run):
    provenance = json.loads((run / 'provenance.json').read_text())
    source = Path(provenance['source_dir'])
    period = provenance['period']
    shard_dir = Path(provenance['shard_root']) / ('shanghai_evolution_futures_time_and_sales_ric_list_0_tas_' + period)
    manifest_path = shard_dir / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    assert manifest['complete'] and all(s['complete'] for s in manifest['sources'])
    with gzip.open(source / 'merged-Report.csv.gz', 'rt', newline='') as f:
        report = list(csv.DictReader(f))
    expected_rows = sum(int(row['Count']) for row in report)
    source_rows = sum(s['data_rows'] for s in manifest['sources'])
    assert source_rows == expected_rows == provenance['expected_source_rows']
    assert sum(s['rows'] for s in manifest['shards']) == expected_rows
    assert manifest['sources'][0]['compressed_bytes'] == (source / 'merged-Data.csv.gz').stat().st_size
    final_lines = [line for line in (run / 'replay.log').read_text().splitlines()
                   if 'cme_tas_replay_all finished workers=' in line]
    assert len(final_lines) == 1, 'need exactly one full replay completion'
    counters = {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', final_lines[0])}
    assert counters['source_rows'] == expected_rows
    for key in ['unmatched_product', 'skipped_ric_filter', 'skipped_unparsed', 'skipped_after_window']:
        assert counters[key] == 0, (key, counters[key])
    for name in ['unparsed.log', 'unmatched.log']:
        assert (run / name).exists() and (run / name).stat().st_size == 0, name
    audit = json.loads((run / 'db_audit.json').read_text())
    assert audit['status'] == 'passed'
    assert audit['watermarks']['period:' + period] == 'done'
    mapping = {1: 'trades', 2: 'specials', 3: 'quotes', 4: 'renames', 5: 'limits',
               6: 'settlements', 7: 'implied_vols', 8: 'tot_volumes', 9: 'price_prints',
               10: 'imbalances', 11: 'auctions', 12: 'corrections', 13: 'statuses', 14: 'reference_changes'}
    for kind, label in mapping.items():
        actual = audit['all']['kinds'].get(str(kind), 0)
        written = counters['written_' + label]
        if kind == 3:
            assert actual <= written
        else:
            assert actual == written, (label, actual, written)
    active = {row['#RIC'] for row in report if int(row['Count']) > 0}
    assert set(audit['rics']) == active, 'persisted RIC inventory differs from active source RICs'
    targets = {}
    for ib, root, ric_count in [('GOIL', 'LGO', 84), ('NKD', 'NK', 16), ('TSR20', 'STF', 18)]:
        cf = 'p:2026:' + root
        inventory = audit['products'][cf]
        rics = [ric for ric in audit['rics'] if re.sub(r'[FGHJKMNQUVXZ]\d+$', '', ric.split('^')[0]) == root]
        assert len(rics) == ric_count, (root, len(rics))
        assert inventory['kinds'].get('1', 0) > 0 and inventory['kinds'].get('3', 0) > 0
        targets[ib] = {'root': root, 'cf': cf, 'active_rics': len(rics),
                       'trade_rics': sum(audit['rics'][r]['kinds'].get('1', 0) > 0 for r in rics),
                       'quote_rics': sum(audit['rics'][r]['kinds'].get('3', 0) > 0 for r in rics), **inventory}
    sample = json.loads((run / 'sample_baseline.json').read_text())
    assert sample['status'] == 'passed' and sample['source_rows'] == 1000000
    equivalence = json.loads((run / 'sample_shard_equivalence.json').read_text())
    assert equivalence['status'] == 'passed'
    db = Path(audit['db'])
    result = {'status': 'complete_verified', 'completed_at': datetime.now(timezone.utc).isoformat(),
              'db': str(db), 'period': period, 'source_rows': source_rows,
              'source_active_rics': len(active), 'product_cfs': len(audit['products']),
              'stored': audit['all'], 'targets': targets, 'replay_counters': counters,
              'quote_merge_write_difference': counters['written_quotes'] - audit['all']['kinds'].get('3', 0),
              'sample_baseline': sample, 'sample_shard_equivalence': equivalence,
              'db_bytes': sum(p.stat().st_size for p in db.iterdir() if p.is_file()),
              'manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
              'db_audit_sha256': hashlib.sha256((run / 'db_audit.json').read_bytes()).hexdigest(),
              'source_sha256': next(f['sha256'] for f in provenance['source_manifest']['files']
                                    if f['name'] == 'merged-Data.csv.gz'),
              'original_production_db_modified': False}
    body = json.dumps(result, ensure_ascii=False, indent=2) + '\n'
    (run / 'completion.json').write_text(body)
    (db / '_INGEST_SUCCESS.json').write_text(body)
    (run / 'shard_manifest.json').write_bytes(manifest_path.read_bytes())
    provenance.update(status=result['status'], completed_at=result['completed_at'])
    (run / 'provenance.json').write_text(json.dumps(provenance, ensure_ascii=False, indent=2) + '\n')
    print(body)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    finalize(parser.parse_args().run.resolve())
