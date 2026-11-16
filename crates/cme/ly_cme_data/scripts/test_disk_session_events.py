import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import build_full_tas_klines as full
from disk_session_events import prepare,Rows
from lseg_kline_state import ordered_intervals
from lseg_session_status import decode


class DiskSessionTests(unittest.TestCase):
    def test_archive_larger_than_old_event_guard_uses_disk_and_preserves_every_row(self):
        base=full.WORK/'tests';base.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=base) as directory:
            work=Path(directory);job=dict(id='large',source_index=0);root=work/'extract'/'large';root.mkdir(parents=True)
            count=260001
            (root/'state.json').write_text(json.dumps(dict(identity=dict(job=job),status_rows=count,correction_rows=0)))
            with (root/'status.json').open('w') as stream:
                stream.write('[')
                for i in range(count):
                    if i:stream.write(',')
                    stream.write(json.dumps(dict(ric='X',product='ES',ts_utc_ns=count-i,exch_hms_ns=2**64-1,
                        qualifiers='17[TRD_TYPE];T[INST_PHASE]',source_order=i)))
                stream.write(']')
            (root/'corrections.json').write_text('[]')
            c=full.p.connection(1)
            try:paths=prepare(c,work,[job])
            finally:c.close()
            actual=total=0;previous=0
            for row in Rows(paths['status'],'X'):
                self.assertGreaterEqual(row['ts_utc_ns'],previous);previous=row['ts_utc_ns']
                actual+=1;total+=row['ts_utc_ns']
            self.assertEqual(actual,count);self.assertEqual(total,count*(count+1)//2)

    def test_disk_sort_merge_retains_duplicates_uint64_and_empty_sources(self):
        base=full.WORK/'tests';base.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=base) as directory:
            work=Path(directory);jobs=[]
            for index,records in enumerate([
                [dict(ric='X',product='ES',ts_utc_ns=t,exch_hms_ns=2**64-1,qualifiers='N[TRD_STATUS]',source_order=o)
                    for t,o in [(4,2),(2,1),(2,1)]],
                [dict(ric='X',product='ES',ts_utc_ns=2,exch_hms_ns=2**64-1,qualifiers='H[TRD_STATUS]',source_order=0)],[]]):
                job=dict(id=f'source_{index}',source_index=index);jobs.append(job)
                root=work/'extract'/job['id'];root.mkdir(parents=True)
                (root/'state.json').write_text(json.dumps(dict(identity=dict(job=job),status_rows=len(records),correction_rows=0)))
                (root/'status.json').write_text(json.dumps(records));(root/'corrections.json').write_text('[]')
            c=full.p.connection(1)
            try:paths=prepare(c,work,jobs);self.assertEqual(prepare(c,work,jobs),paths)
            finally:c.close()
            result=list(Rows(paths['status'],'X'))
            self.assertEqual([(r['ts_utc_ns'],r['source_index'],r['source_order']) for r in result],[(2,0,1),(2,0,1),(2,1,0),(4,0,2)])
            self.assertTrue(all(r['exch_hms_ns']==2**64-1 for r in result))
            self.assertEqual(len(Rows(paths['status'],'X',lower=3,upper=5)),1)
            self.assertEqual(list(Rows(paths['corrections'],'X',clock='published_ns')),[])

    def test_streamed_daily_status_equals_original_decoder_across_midnight(self):
        base=full.WORK/'tests';base.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=base) as directory:
            work=Path(directory);start=full.stamp_year(2026);midnight=start+full.p.DAY
            source=[dict(ts_utc_ns=start-1,qualifiers='4[TRD_TYPE];C[INST_PHASE];0[HALT_REASN]')]
            source += [dict(ts_utc_ns=midnight-2+i//10000,qualifiers='17[TRD_TYPE];T[INST_PHASE]') for i in range(20000)]
            source += [dict(ts_utc_ns=midnight,qualifiers='H[TRD_STATUS]'),dict(ts_utc_ns=midnight+1,qualifiers='N[TRD_STATUS]')]
            class Repeatable:
                def __iter__(self):return (dict(r) for r in source)
            issues=[]
            with patch.object(full.p,'DEST',work/'delivery'):
                paths=full.deliver_status(Repeatable(),'ES','X','cme',2026,work,None,True,b'test',issues)
                # Repeating the same stream samples existing files with bounded
                # row iteration, and preserves same-time duplicate messages.
                self.assertEqual(full.deliver_status(Repeatable(),'ES','X','cme',2026,work,None,True,b'test',[]),paths)
            self.assertEqual(len(paths),2)
            actual=[r for path in paths for r in pq.ParquetFile(path).read().to_pylist()]
            expected=[];previous='UNKNOWN'
            for row in source:
                parsed=decode(row['qualifiers'],previous,'ES',cme_scope=True);before=previous;previous=parsed['session_state']
                if row['ts_utc_ns']<start:continue
                expected.append((row['ts_utc_ns'],previous,before!=previous,row['qualifiers']))
            self.assertEqual([(r['ts_utc_ns'],r['session_state'],r['is_transition'],r['qualifiers']) for r in actual],expected)
            self.assertEqual(issues,[])

    def test_large_same_time_patch_stream_is_consumed_once_and_coalesced(self):
        consumed=0
        def rows():
            nonlocal consumed
            for i in range(100000):
                consumed+=1
                yield dict(ts_utc_ns=1 if i<50000 else 2,source_order=i,qualifiers='17[TRD_TYPE];T[INST_PHASE]',product='ES')
        result=ordered_intervals(rows(),'ES',0,10,cme_scope=True)
        self.assertEqual(consumed,100000)
        self.assertEqual(result['phase'].to_pylist(),['UNKNOWN','TRADING'])
        self.assertEqual(result['end_ns'].to_pylist(),[1,10])


if __name__=='__main__':unittest.main()
