import unittest
import tempfile
from pathlib import Path
from lseg_session_status import decode,qualifiers,official_event


class LsegSessionTests(unittest.TestCase):
    def test_cme_native_expiration_is_not_a_temporary_halt(self):
        for raw in ['4[HALT_REASN];NH[HALT_RSN]',
                    '18[TRD_TYPE];C[INST_PHASE];4[HALT_REASN];NH[HALT_RSN]']:
            r=decode(raw,previous='TRADING',product='CL')
            self.assertEqual((r['action'],r['session_state'],r['is_trading'],r['reason']),
                ('EXPIRE','EXPIRED',False,'INSTRUMENT_EXPIRATION'))
            self.assertIsNone(r['issue_code'])
        self.assertEqual(decode('4[TRD_TYPE];C[INST_PHASE]',product='CL')['session_state'],'CLOSED')
        self.assertEqual(decode('4[HALT_REASN]',product='OTHER')['session_state'],'UNKNOWN')

    def test_expiration_cannot_be_cleared_by_auxiliary_or_reopening_markers(self):
        for raw in ['0[HALT_REASN];"  "[HALT_RSN]','N[TRD_STATUS]','15[TRD_TYPE]',
                    'H[TRD_STATUS];N[TRD_STATUS]','malformed message']:
            r=decode(raw,previous='EXPIRED',product='ES')
            self.assertEqual(r['session_state'],'EXPIRED');self.assertFalse(r['is_trading'])
        self.assertEqual(decode('15[TRD_TYPE]',previous='EXPIRED',product='ES')['issue_code'],'session_marker_after_expiration')

    def test_conflicting_expiration_and_trading_is_not_silently_accepted(self):
        r=decode('15[TRD_TYPE];4[HALT_REASN]',previous='TRADING',product='ES')
        self.assertEqual(r['session_state'],'UNKNOWN');self.assertIsNotNone(r['issue_code'])

    def test_zero_duration_unknowns_are_not_reported_as_time_gaps(self):
        import pyarrow as pa
        from unittest.mock import patch
        import build_lseg_session_status as b
        b.WORK.mkdir(parents=True,exist_ok=True)
        start=1767225600000000000
        t=pa.Table.from_pylist([dict(product='ES',ric='TEST',ts_utc_ns=start+n,session_state=s)
            for n,s in [(0,'UNKNOWN'),(0,'CLOSED'),(10**9,'UNKNOWN'),(2*10**9,'TRADING')]])
        with tempfile.TemporaryDirectory(dir=b.WORK) as tmp:
            root=Path(tmp);f=root/'artifacts/kline_continuous_v5_20261110/lifetimes.json'
            f.parent.mkdir(parents=True);f.write_text('{}')
            with patch.object(b,'PROBLEMS',root/'problems'),patch.object(b.pipeline,'ROOT',root):
                self.assertEqual(b.write_unknown_intervals(t),1)

    def test_changed_day_selection_preserves_source_records_and_times(self):
        import pyarrow as pa
        from build_lseg_session_status import SCHEMA,publication_changes
        start=1767225600000000000
        rows=[dict(product='CL',ric='CLG26',ts_utc_ns=start+i*86400000000000,published_ns=start+i*86400000000000,
            effective_ns=start+i*86400000000000,source='LSEG_TAS',qualifiers='source',
            action='NOT_AVAILABLE',session_state='CLOSED',is_trading=False,reason='UNSPECIFIED',is_transition=False) for i in range(2)]
        old=pa.Table.from_pylist(rows,schema=SCHEMA)
        rows[1]=dict(rows[1],action='EXPIRE',session_state='EXPIRED',reason='INSTRUMENT_EXPIRATION')
        new=pa.Table.from_pylist(rows,schema=SCHEMA);keys,counts=publication_changes(new,old)
        self.assertEqual(counts,dict(changed_rows=1,changed_days=1))
        self.assertEqual(len(keys),1)
        rows[1]=dict(rows[1],published_ns=start+1)
        with self.assertRaises(AssertionError):publication_changes(pa.Table.from_pylist(rows,schema=SCHEMA),old)

    def test_probe_compares_both_feeds_after_a_rapid_second_transition(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from unittest.mock import patch
        import build_lseg_session_status as b
        b.WORK.mkdir(parents=True,exist_ok=True)
        start=1767225600000000000
        source=[];reference=[]
        for seconds,action,state,flag in [(0,'OPEN','TRADING',True),(1,'PRE_OPEN','PRE_OPEN',False)]:
            source.append(dict(product='ES',ric='TEST',ts_utc_ns=start+seconds*10**9+100000000,
                action=action,session_state=state,is_trading=flag,source='LSEG_TAS'))
            reference.append(dict(product='ES',ric='TEST',ts_event_ns=start+seconds*10**9,ts_recv_ns=start+seconds*10**9,
                action_name='TRADING' if flag else 'PRE_OPEN',trading_event_name='NONE',is_midnight_snapshot=False,
                is_trading='Y' if flag else 'N',reason_name='SCHEDULED',source_index=0,source_order=seconds))
        with tempfile.TemporaryDirectory(dir=b.WORK) as tmp:
            root=Path(tmp);pq.write_table(pa.Table.from_pylist(reference),root/'status.parquet')
            with patch.multiple(b,RUN=root/'run',PROBLEMS=root/'problems'),patch.object(b.pipeline,'WORK',root):
                result=b.compare(pa.Table.from_pylist(source))
            self.assertEqual(result['core_matched_2s'],2)
            self.assertEqual(sum(r['state_agrees_after_2s'] for r in result['by_product']),2)
            self.assertEqual(result['comparison_problem_rows'],0)

    def test_derived_file_can_add_official_supplement_and_retry_idempotently(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from build_lseg_session_status import SCHEMA,PUBLIC,metadata,publish_session_file,WORK
        WORK.mkdir(parents=True,exist_ok=True)
        row=dict(product='ES',ric='ESM26',ts_utc_ns=1767225600000000001,published_ns=1767225600000000001,
            effective_ns=1767225600000000001,action='OPEN',session_state='TRADING',is_trading=True,
            reason='UNSPECIFIED',is_transition=True,source='LSEG_TAS',source_url=None,qualifiers='15[TRD_TYPE]')
        first=pa.Table.from_pylist([row],schema=SCHEMA).select(PUBLIC).replace_schema_metadata(metadata())
        extra=dict(row,ts_utc_ns=row['ts_utc_ns']+10**9,published_ns=row['published_ns']+10**9,
            effective_ns=row['effective_ns']+10**9,source='CME_OFFICIAL',action='OFFICIAL_HALTED',
            session_state='HALTED',is_trading=False,source_url='https://www.cmegroup.com/notice')
        second=pa.Table.from_pylist([row,extra],schema=SCHEMA).select(PUBLIC).replace_schema_metadata(metadata())
        with tempfile.TemporaryDirectory(dir=WORK) as tmp:
            path=Path(tmp)/'20260101/session_status.parquet'
            publish_session_file(first,path,True);publish_session_file(second,path,True)
            self.assertTrue(pq.ParquetFile(path).read().equals(second,check_metadata=False))
            before=path.stat().st_mtime_ns;publish_session_file(second,path,True)
            self.assertEqual(path.stat().st_mtime_ns,before)
            self.assertEqual(pq.ParquetFile(path).read()['ts_utc_ns'][0].as_py(),1767225600000000001)

    def test_real_close_preopen_and_open_patches(self):
        for raw,action,state,flag in [
            ('4[TRD_TYPE];C  [INST_PHASE];I[ORD_ENT_ST]','CLOSE','CLOSED',False),
            ('21[TRD_TYPE];O  [INST_PHASE];E[ORD_ENT_ST]','PRE_OPEN','PRE_OPEN',False),
            ('15[TRD_TYPE];I[ORD_ENT_ST];BBO[MKT_ST_IND]','OPEN','TRADING',True),
            ('17[TRD_TYPE];T  [INST_PHASE];E[ORD_ENT_ST]','OPEN','TRADING',True)]:
            r=decode(raw,product='GC');self.assertEqual((r['action'],r['session_state'],r['is_trading']),(action,state,flag))

    def test_order_entry_disabled_does_not_close_a_trading_session(self):
        r=decode('I[ORD_ENT_ST]',previous='TRADING',product='ES')
        self.assertEqual((r['action'],r['session_state']),('NO_CHANGE','TRADING'))

    def test_empty_reason_and_auxiliary_fields_do_not_end_a_halt(self):
        for raw in ['0[HALT_REASN];"  "[HALT_RSN]','   [MKT_ST_IND]','5[SECUR_ST]']:
            self.assertEqual(decode(raw,previous='HALTED',product='ES')['session_state'],'HALTED')

    def test_normal_releases_halt_without_manufacturing_tradability(self):
        r=decode('N[TRD_STATUS]',previous='HALTED',product='GC')
        self.assertEqual((r['action'],r['session_state'],r['is_trading']),('HALT_RELEASE','UNKNOWN',None))

    def test_nonzero_reason_alone_is_not_a_proven_halt(self):
        r=decode('1[HALT_REASN]',previous='TRADING',product='ES')
        self.assertEqual(r['session_state'],'UNKNOWN');self.assertIsNotNone(r['issue_code'])

    def test_not_halted_reason_is_not_misread_as_an_interruption(self):
        r=decode('2[HALT_REASN];NH[HALT_RSN]',previous='TRADING',product='ES')
        self.assertEqual(r['session_state'],'TRADING');self.assertIsNone(r['issue_code'])

    def test_native_recovery_halt_and_official_system_suspend(self):
        for raw in ['2[TRD_TYPE];C[ORD_ENT_ST]','1[HALT_REASN];SS[HALT_RSN]']:
            self.assertEqual(decode(raw,previous='TRADING',product='CL')['session_state'],'HALTED')
            self.assertEqual(decode(raw,previous='CLOSED',product='CL')['session_state'],'CLOSED')
        r=decode('18[TRD_TYPE];C[INST_PHASE]',previous='TRADING',product='ES')
        self.assertFalse(r['is_trading']);self.assertEqual(r['action'],'NOT_AVAILABLE')

    def test_explicit_halt_and_reopening(self):
        r=decode('H[TRD_STATUS];1[HALT_REASN];SS[HALT_RSN]',previous='TRADING',product='GC')
        self.assertEqual(r['session_state'],'HALTED')
        self.assertEqual(decode('21[TRD_TYPE];O[INST_PHASE]',previous='HALTED',product='GC')['session_state'],'HALTED')
        self.assertEqual(decode('15[TRD_TYPE]',previous='HALTED',product='GC')['session_state'],'TRADING')

    def test_conflicts_and_unknown_products_do_not_get_guessed(self):
        r=decode('H[TRD_STATUS];N[TRD_STATUS]',previous='TRADING',product='GC')
        self.assertEqual(r['session_state'],'UNKNOWN');self.assertIsNotNone(r['issue_code'])
        self.assertEqual(decode('15[TRD_TYPE]',product='OTHER')['session_state'],'UNKNOWN')
        self.assertEqual(decode('V[INST_PHASE]',previous='TRADING',product='GC')['session_state'],'UNKNOWN')
        self.assertEqual(decode('garbage without a field',previous='TRADING',product='GC')['session_state'],'UNKNOWN')

    def test_missing_and_explicit_empty_are_preserved(self):
        f=qualifiers('"  "[HALT_RSN];4[TRD_TYPE]')
        self.assertEqual(f['HALT_RSN'],{''});self.assertNotIn('TRD_STATUS',f)

    def test_official_future_reopening_is_not_applied_at_announcement_time(self):
        r=official_event(dict(source_url='https://x.com/CMEGroup/status/1994395477840990459',
            published_ns=100,effective_ns=200,products=['GC'],session_state='TRADING',evidence_sha256='a'*64))
        self.assertEqual(r['ts_utc_ns'],200)
        r=official_event(dict(r,published_ns=300));self.assertEqual(r['ts_utc_ns'],300)

    def test_official_supplements_require_scope_and_traceable_evidence(self):
        with self.assertRaises(ValueError):official_event(dict(source_url='https://example.com/'))


if __name__=='__main__':unittest.main()
