"""Bounded checks for phase reconstruction, UTC boundaries and lossless bars."""
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import build_market_status_klines as m


class StatusMinuteTests(unittest.TestCase):
    def test_backfill_removes_cancelled_minute_and_republishes_changed_values(self):
        from apply_kline_trade_revisions import rebuild_minutes,changed_rows
        from tas_trade_revisions import mark_corrected
        states=[dict(start_ns=m.START,end_ns=m.START+4*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+i*m.MINUTE,exch_hms_ns=i*m.MINUTE,price=p,volume=v)
            for i,p,v in [(0,100000000000,2),(1,110000000000,3),(3,101000000000,1)]]
        _,gross=self.build(trades,states,4)
        _,net=self.build([trades[0],trades[2]],states,4)
        c=m.connection(1)
        try:
            intervals=pa.Table.from_pylist([dict(states[0],phase='TRADING')])
            c.register('states',intervals)
            rebuild_minutes(c,gross,net.slice(0,0),{m.START+m.MINUTE})
            rebuilt=m.minute_grid(c,'TEST',m.START,m.START+4*m.MINUTE)
        finally:c.close()
        self.assertTrue(rebuilt.equals(net,check_metadata=False))
        # A plan retracted after new source evidence must restore previously
        # delivered values, even though its new result equals the gross base.
        self.assertEqual(changed_rows(m.public_kline(net),m.public_kline(gross)).tolist(),[False,True,True,False])
        public=m.public_kline(mark_corrected(rebuilt,dict(version='1',patches=[],unresolved=[])))
        with tempfile.TemporaryDirectory(dir=m.WORK/'tests') as directory:
            target=Path(directory)/'20260101/kline.parquet';target.parent.mkdir()
            pq.write_table(m.public_kline(gross),target)
            m.publish_file(public,target,True,allow_trade_revisions=True)
            self.assertTrue(pq.ParquetFile(target).read().equals(public,check_metadata=True))
            m.publish_file(public,target,True,allow_trade_revisions=True)

    def test_correction_insert_on_a_day_without_a_priced_trade_file(self):
        states=pa.Table.from_pylist([dict(start_ns=m.START,end_ns=m.START+2*m.MINUTE,
            is_trading=True,session_id=1,phase='TRADING')])
        plan=dict(patches=[],insertions=[dict(event_id='insert',published_ns=m.START,event_ns=m.START,
            exch_hms_ns=0,price=100000000000,volume=3)])
        c=m.connection(1)
        try:
            counts=m.sparse_minutes(c,[],states,m.START,m.START+2*m.MINUTE,plan)
            bars=m.minute_grid(c,'TEST',m.START,m.START+2*m.MINUTE)
        finally:c.close()
        self.assertEqual(counts,(1,3,0))
        self.assertEqual(bars['close'].to_pylist(),[100000000000,100000000000])

    def event(self,offset,flag,action=0,event=None,snapshot=False,order=0,reason=0):
        return dict(ts_event_ns=m.START+(offset if event is None else event),ts_recv_ns=m.START+offset,
            is_midnight_snapshot=snapshot,is_trading=flag,action=action,reason=reason,source_index=0,source_order=order)

    def test_snapshot_seeds_state_without_a_new_midnight_opening(self):
        rows=[self.event(0,'Y',event=-m.DAY,snapshot=True),self.event(2*m.MINUTE,'~',action=0),
              self.event(4*m.MINUTE,'N',action=12)]
        t=m.state_intervals(pa.Table.from_pylist(rows),m.START,m.START+5*m.MINUTE).to_pylist()
        self.assertEqual([(r['start_ns']-m.START,r['end_ns']-m.START,r['is_trading']) for r in t],
            [(0,4*m.MINUTE,True),(4*m.MINUTE,5*m.MINUTE,False)])

    def test_unknown_initial_state_is_not_assumed_closed(self):
        rows=[self.event(m.MINUTE,'~'),self.event(2*m.MINUTE,'Y',action=7)]
        t=m.state_intervals(pa.Table.from_pylist(rows),m.START,m.START+3*m.MINUTE).to_pylist()
        self.assertIsNone(t[0]['is_trading'])
        self.assertEqual(t[0]['end_ns'],m.START+2*m.MINUTE)

    def build(self,trades,states,minutes,seed=None,revisions=None):
        (m.WORK/'tests').mkdir(parents=True,exist_ok=True)
        folder=tempfile.TemporaryDirectory(dir=m.WORK/'tests')
        p=Path(folder.name)/'trade.parquet'
        schema=pa.schema([('ts_utc_ns',pa.uint64()),('exch_hms_ns',pa.uint64()),('price',pa.int64()),('volume',pa.uint32())])
        pq.write_table(pa.Table.from_pylist(trades,schema=schema),p)
        c=m.connection(1)
        states=[dict(row,phase=row.get('phase','UNKNOWN' if row['is_trading'] is None else 'TRADING' if row['is_trading'] else 'SCHEDULED_CLOSED')) for row in states]
        interval=pa.Table.from_pylist(states,schema=pa.schema([('start_ns',pa.int64()),('end_ns',pa.int64()),('is_trading',pa.bool_()),('session_id',pa.int64()),('phase',pa.string())]))
        revisions=revisions(p) if callable(revisions) else revisions
        counts=m.sparse_minutes(c,[str(p)],interval,m.START,m.START+minutes*m.MINUTE,revisions)
        bars=m.minute_grid(c,'TEST',m.START,m.START+minutes*m.MINUTE,seed)
        c.close();folder.cleanup()
        return counts,bars

    def test_empty_minutes_and_closed_state_trades_preserve_volume(self):
        states=[dict(start_ns=m.START,end_ns=m.START+3*m.MINUTE,is_trading=True,session_id=1),
                dict(start_ns=m.START+3*m.MINUTE,end_ns=m.START+4*m.MINUTE,is_trading=False,session_id=None),
                dict(start_ns=m.START+4*m.MINUTE,end_ns=m.START+6*m.MINUTE,is_trading=True,session_id=2)]
        trades=[dict(ts_utc_ns=m.START+i*m.MINUTE,exch_hms_ns=i*m.MINUTE,price=p,volume=v)
                for i,p,v in [(0,100000000003,2),(2,105000000007,3),(3,110000000009,4)]]
        counts,bars=self.build(trades,states,6)
        b=bars.to_pylist()
        self.assertEqual(counts[:2],(3,9))
        self.assertEqual(sum(r['volume'] for r in b),9)
        self.assertEqual(b[1]['close'],100000000003)
        self.assertTrue(b[1]['is_empty'])
        self.assertEqual(b[3]['outside_status_trade_count'],1)
        self.assertEqual(b[3]['trading_ns'],0)
        self.assertEqual(b[4]['open'],110000000009)
        self.assertTrue(all(r['status_known'] for r in b))

    def test_cross_midnight_duplicates_late_prints_and_missing_exchange_time(self):
        # A post-midnight publication belongs to the preceding exchange minute.
        publish=m.START+m.DAY+5_000_000_000
        trades=[dict(ts_utc_ns=publish,exch_hms_ns=m.DAY-30_000_000_000,price=123000000001,volume=2)]*2
        trades.append(dict(ts_utc_ns=publish,exch_hms_ns=2**64-1,price=124000000003,volume=3))
        states=[dict(start_ns=m.START,end_ns=m.START+2*m.DAY,is_trading=True,session_id=1)]
        counts,bars=self.build(trades,states,1441)
        b=[r for r in bars.to_pylist() if r['trade_count']]
        self.assertEqual(counts,(3,7,1))
        self.assertEqual(b[0]['ts_utc_ns'],m.START+m.DAY-m.MINUTE)
        self.assertEqual(b[0]['trade_count'],2)
        self.assertEqual(b[0]['late_trade_count'],2)
        self.assertEqual(b[1]['missing_exchange_time_trade_count'],1)

    def test_partial_trading_minute_is_measured_exactly(self):
        states=[dict(start_ns=m.START,end_ns=m.START+15_000_000_000,is_trading=False,session_id=None),
                dict(start_ns=m.START+15_000_000_000,end_ns=m.START+2*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+20_000_000_000,exch_hms_ns=20_000_000_000,price=1000000000,volume=1)]
        _,bars=self.build(trades,states,2)
        b=bars.to_pylist()
        self.assertEqual(b[0]['trading_ns'],45_000_000_000)
        self.assertTrue(b[0]['status_known'])

    def test_ib_comparison_keeps_original_labels_and_finds_price_difference(self):
        states=[dict(start_ns=m.START,end_ns=m.START+2*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START,exch_hms_ns=0,price=100000000000,volume=1)]
        _,bars=self.build(trades,states,2)
        ib=pa.Table.from_pylist([dict(ts_utc_ns=m.START+i*m.MINUTE,open=p,high=p,low=p,close=p,
            volume=v,time_quality_flags=0,ohlcv_quality_flags=0) for i,p,v in [(0,100.,1.),(1,101.,0.)]])
        c=m.connection(1)
        stats,diff=m.compare_ib(c,'TEST',bars,ib,.25)
        self.assertEqual(stats['matched_minutes'],2)
        self.assertEqual(stats['ohlc_equal_minutes'],1)
        self.assertEqual(stats['volume_equal_minutes'],2)
        self.assertEqual(len(diff),1)
        self.assertEqual(diff['ts_utc_ns'][0].as_py(),m.START+m.MINUTE)
        self.assertEqual(stats['shift_probes'][1]['shift_seconds'],0)
        c.close()

    def test_trade_files_outside_the_covered_window_have_zero_counts(self):
        states=[dict(start_ns=m.START,end_ns=m.START+2*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+3*m.MINUTE,exch_hms_ns=3*m.MINUTE,price=1000000000,volume=1)]
        counts,bars=self.build(trades,states,2)
        self.assertEqual(counts,(0,0,0))
        self.assertTrue(all(r['volume']==0 and r['is_empty'] and r['open'] is None for r in bars.to_pylist()))

    def test_public_fill_flag_distinguishes_unpriced_empty_minutes(self):
        states=[dict(start_ns=m.START,end_ns=m.START+3*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+m.MINUTE,exch_hms_ns=m.MINUTE,price=100000000003,volume=2)]
        _,internal=self.build(trades,states,3)
        result=m.public_kline(internal)
        self.assertEqual(result.schema.names,m.KLINE_CORE+['is_ffill','status'])
        self.assertEqual(result['is_ffill'].to_pylist(),[False,False,True])
        self.assertEqual(result['status'].to_pylist(),['TRADING']*3)
        self.assertIsNone(result['open'][0].as_py())
        unchanged=[name for name in m.KLINE_CORE if name not in m.KLINE_PRICES]
        self.assertTrue(result.select(unchanged).equals(internal.select(unchanged),check_metadata=False))
        for name in m.KLINE_PRICES:
            self.assertEqual(result[name].type,pa.float64())
            self.assertEqual(result[name].to_pylist(),[None,100.000000003,100.000000003])
        self.assertTrue(m.public_kline(result).equals(result,check_metadata=True))

    def test_public_status_retains_closed_partial_and_unknown_cases(self):
        rows=[]
        for i,(known,duration) in enumerate([(True,m.MINUTE),(True,0),(True,m.MINUTE//2),(False,m.MINUTE)]):
            rows.append(dict(ric='TEST',ts_utc_ns=m.START+i*m.MINUTE,open=100,high=101,low=99,close=100,
                volume=1,trade_count=1,is_empty=False,trading_ns=duration,scheduled_closed_ns=m.MINUTE-duration if known else 0,status_known=known))
        source=pa.Table.from_pylist(rows)
        result=m.public_kline(source)
        self.assertEqual(result['status'].to_pylist(),['TRADING','SCHEDULED_CLOSED','PARTIAL','UNKNOWN'])
        self.assertEqual(result['is_ffill'].to_pylist(),[False]*4)
        self.assertEqual(result['volume'].to_pylist(),[1]*4)

    def test_atomic_upgrade_preserves_fill_and_actual_price_units(self):
        states=[dict(start_ns=m.START,end_ns=m.START+3*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+m.MINUTE,exch_hms_ns=m.MINUTE,price=-100000000003,volume=2)]
        _,internal=self.build(trades,states,3)
        current=m.public_kline(internal)
        legacy=current
        for name in m.KLINE_PRICES:
            legacy=legacy.set_column(legacy.schema.get_field_index(name),name,internal[name])
        metadata=dict(legacy.schema.metadata)
        metadata[b'kline_schema_version']=b'2'
        metadata[b'price_scale']=b'1000000000'
        legacy=legacy.replace_schema_metadata(metadata)
        with tempfile.TemporaryDirectory(dir=m.WORK/'tests') as directory:
            target=Path(directory)/'20260101/kline.parquet'
            target.parent.mkdir()
            pq.write_table(legacy,target)
            m.publish_file(current,target,True,replace_schema=True)
            self.assertTrue(pq.ParquetFile(target).read().equals(current,check_metadata=True))
            self.assertEqual(current['close'].to_pylist(),[None,-100.000000003,-100.000000003])
            m.publish_file(current,target,True,replace_schema=True)

    def test_continuous_minutes_label_halts_and_scheduled_breaks(self):
        states=[dict(start_ns=m.START,end_ns=m.START+m.MINUTE,is_trading=True,session_id=1,phase='TRADING'),
            dict(start_ns=m.START+m.MINUTE,end_ns=m.START+3*m.MINUTE,is_trading=False,session_id=None,phase='HALTED'),
            dict(start_ns=m.START+3*m.MINUTE,end_ns=m.START+4*m.MINUTE,is_trading=True,session_id=2,phase='TRADING'),
            dict(start_ns=m.START+4*m.MINUTE,end_ns=m.START+6*m.MINUTE,is_trading=False,session_id=None,phase='SCHEDULED_CLOSED'),
            dict(start_ns=m.START+6*m.MINUTE,end_ns=m.START+8*m.MINUTE,is_trading=True,session_id=3,phase='TRADING')]
        trades=[dict(ts_utc_ns=m.START,exch_hms_ns=0,price=100000000003,volume=2),
            dict(ts_utc_ns=m.START+7*m.MINUTE,exch_hms_ns=7*m.MINUTE,price=101000000007,volume=3)]
        counts,bars=self.build(trades,states,8)
        result=m.public_kline(bars)
        self.assertEqual(counts[:2],(2,5))
        self.assertEqual(result['ts_utc_ns'].to_pylist(),[m.START+i*m.MINUTE for i in range(8)])
        self.assertEqual(result['status'].to_pylist(),['TRADING','HALTED','HALTED','TRADING','SCHEDULED_CLOSED','SCHEDULED_CLOSED','TRADING','TRADING'])
        self.assertEqual(result['close'].to_pylist(),[100.000000003]*7+[101.000000007])
        self.assertEqual(result['is_ffill'].to_pylist(),[False,True,True,True,True,True,True,False])
        self.assertEqual(result['volume'].to_pylist(),[2,0,0,0,0,0,0,3])

    def test_own_prior_seed_initializes_empty_open_and_halt_minutes(self):
        states=[dict(start_ns=m.START,end_ns=m.START+m.MINUTE,is_trading=False,session_id=None,phase='HALTED'),
            dict(start_ns=m.START+m.MINUTE,end_ns=m.START+2*m.MINUTE,is_trading=True,session_id=1,phase='TRADING')]
        _,bars=self.build([],states,2,seed=-123000000001)
        result=m.public_kline(bars)
        self.assertEqual(result['close'].to_pylist(),[-123.000000001]*2)
        self.assertEqual(result['is_ffill'].to_pylist(),[True,True])
        _,unknown=self.build([],states,2)
        self.assertEqual(m.public_kline(unknown)['is_ffill'].to_pylist(),[False,False])

    def test_halt_preopen_recovery_and_expiry_are_distinct(self):
        rows=[self.event(0,'Y',action=7,reason=1),self.event(m.MINUTE,'N',action=15,reason=2),
            self.event(2*m.MINUTE,'N',action=1,reason=1),self.event(3*m.MINUTE,'N',reason=1),
            self.event(4*m.MINUTE,'Y',action=7,reason=1),self.event(5*m.MINUTE,'N',action=15,reason=5)]
        intervals=m.state_intervals(pa.Table.from_pylist(rows),m.START,m.START+6*m.MINUTE).to_pylist()
        self.assertEqual([(r['start_ns']-m.START,r['end_ns']-m.START,r['phase']) for r in intervals],
            [(0,m.MINUTE,'TRADING'),(m.MINUTE,4*m.MINUTE,'HALTED'),(4*m.MINUTE,5*m.MINUTE,'TRADING'),(5*m.MINUTE,6*m.MINUTE,'EXPIRED')])

    def test_weekend_startup_surveillance_and_scheduled_halt_do_not_pad_closed_hours(self):
        rows=[self.event(0,'N',action=12,reason=1),self.event(m.MINUTE,'N',action=15,reason=2),
            self.event(2*m.MINUTE,'N',action=1,reason=1),self.event(3*m.MINUTE,'Y',action=7,reason=1),
            self.event(4*m.MINUTE,'N',action=8,reason=1)]
        intervals=m.state_intervals(pa.Table.from_pylist(rows),m.START,m.START+5*m.MINUTE).to_pylist()
        self.assertEqual([(r['start_ns']-m.START,r['end_ns']-m.START,r['phase']) for r in intervals],
            [(0,3*m.MINUTE,'SCHEDULED_CLOSED'),(3*m.MINUTE,4*m.MINUTE,'TRADING'),(4*m.MINUTE,5*m.MINUTE,'SCHEDULED_CLOSED')])

    def test_explicit_pause_and_suspend_are_halts_without_a_reason(self):
        for action in (8,9,10):
            rows=[self.event(0,'Y',action=7),self.event(m.MINUTE,'N',action=action)]
            intervals=m.state_intervals(pa.Table.from_pylist(rows),m.START,m.START+2*m.MINUTE).to_pylist()
            self.assertEqual(intervals[-1]['phase'],'HALTED')

    def test_policy_upgrade_adds_halt_rows_and_refills_reopen_without_changing_trades(self):
        states=[dict(start_ns=m.START,end_ns=m.START+m.MINUTE,is_trading=True,session_id=1,phase='TRADING'),
            dict(start_ns=m.START+m.MINUTE,end_ns=m.START+2*m.MINUTE,is_trading=False,session_id=None,phase='HALTED'),
            dict(start_ns=m.START+2*m.MINUTE,end_ns=m.START+4*m.MINUTE,is_trading=True,session_id=2,phase='TRADING')]
        trades=[dict(ts_utc_ns=m.START+i*m.MINUTE,exch_hms_ns=i*m.MINUTE,price=p,volume=1)
            for i,p in [(0,100000000003),(3,101000000007)]]
        _,bars=self.build(trades,states,4)
        current=m.public_kline(bars)
        rows=current.to_pylist()
        old_rows=[dict(rows[i]) for i in [0,2,3]]
        for name in m.KLINE_PRICES:old_rows[1][name]=None
        old_rows[1]['is_ffill']=False
        legacy=pa.Table.from_pylist(old_rows,schema=current.schema)
        metadata=dict(current.schema.metadata)
        metadata.pop(b'fill_policy_version',None)
        metadata[b'kline_schema_version']=b'3'
        legacy=legacy.replace_schema_metadata(metadata)
        with tempfile.TemporaryDirectory(dir=m.WORK/'tests') as directory:
            target=Path(directory)/'20260101/kline.parquet'
            target.parent.mkdir();pq.write_table(legacy,target)
            corrupted=current.to_pylist();corrupted[-1]['close']=999.
            with self.assertRaises(AssertionError):
                m.publish_file(pa.Table.from_pylist(corrupted,schema=current.schema),target,True,replace_schema=True)
            m.publish_file(current,target,True,replace_schema=True)
            self.assertTrue(pq.ParquetFile(target).read().equals(current,check_metadata=True))

    def test_unknown_cause_keeps_real_trades_and_does_not_claim_a_scheduled_close(self):
        rows=[self.event(0,'N',action=15),self.event(m.MINUTE,'Y',action=7)]
        states=m.state_intervals(pa.Table.from_pylist(rows),m.START,m.START+2*m.MINUTE).to_pylist()
        self.assertEqual(states[0]['phase'],'UNKNOWN')
        trades=[dict(ts_utc_ns=m.START+15_000_000_000,exch_hms_ns=15_000_000_000,price=100000000003,volume=2)]
        _,bars=self.build(trades,states,2)
        self.assertEqual(m.public_kline(bars)['status'].to_pylist(),['UNKNOWN','TRADING'])
        self.assertEqual(bars['unknown_status_trade_count'].to_pylist(),[1,0])
        self.assertEqual(bars['volume'].to_pylist(),[2,0])

    def test_unknown_minute_does_not_imply_its_trade_occurred_during_unknown_state(self):
        states=[dict(start_ns=m.START,end_ns=m.START+15_000_000_000,is_trading=None,session_id=None,phase='UNKNOWN'),
            dict(start_ns=m.START+15_000_000_000,end_ns=m.START+m.MINUTE,is_trading=True,session_id=1,phase='TRADING')]
        trades=[dict(ts_utc_ns=m.START+20_000_000_000,exch_hms_ns=20_000_000_000,price=100000000003,volume=1)]
        _,bars=self.build(trades,states,1)
        self.assertEqual(m.public_kline(bars)['status'].to_pylist(),['UNKNOWN'])
        self.assertEqual(bars['unknown_status_trade_count'].to_pylist(),[0])
        self.assertEqual(bars['trade_count'].to_pylist(),[1])

    def test_comparison_uses_native_ib_and_ignores_padded_scheduled_minutes(self):
        states=[dict(start_ns=m.START,end_ns=m.START+m.MINUTE,is_trading=True,session_id=1,phase='TRADING'),
            dict(start_ns=m.START+m.MINUTE,end_ns=m.START+2*m.MINUTE,is_trading=False,session_id=None,phase='SCHEDULED_CLOSED'),
            dict(start_ns=m.START+2*m.MINUTE,end_ns=m.START+3*m.MINUTE,is_trading=None,session_id=None,phase='UNKNOWN')]
        trades=[dict(ts_utc_ns=m.START,exch_hms_ns=0,price=100000000000,volume=1)]
        _,bars=self.build(trades,states,3)
        ib=pa.Table.from_pylist([dict(ts_utc_ns=m.START+i*m.MINUTE,open=100.,high=100.,low=100.,close=100.,volume=float(i==0),
            time_quality_flags=0,ohlcv_quality_flags=0) for i in [0,2]])
        names=ib.schema.names
        c=m.connection(1)
        try:stats,_=m.compare_ib(c,'TEST',bars,ib,.25)
        finally:c.close()
        self.assertEqual(stats['matched_minutes'],1)
        self.assertEqual(stats['missing_ib_minutes'],0)
        self.assertEqual(stats['ib_only_minutes'],1)
        self.assertEqual(ib.schema.names,names)
        self.assertEqual(len(ib),2)

    def test_lifetime_window_excludes_unlisted_and_expired_padding(self):
        from kline_lifetimes import window
        contract=dict(ric='TEST',start_ns=m.START,end_ns=m.START+10*m.MINUTE)
        status=pa.Table.from_pylist([self.event(0,'N',action=12,reason=1)])
        old=pa.Table.from_pylist([dict(ts_utc_ns=m.START+2*m.MINUTE,trade_count=1)])
        life=dict(activation_ns=m.START+m.MINUTE,expiration_ns=m.START+5*m.MINUTE)
        self.assertEqual(window(contract,status,old,life),(m.START+m.MINUTE,m.START+5*m.MINUTE))
        with self.assertRaises(AssertionError):
            window(contract,status,old,dict(activation_ns=m.START+3*m.MINUTE,expiration_ns=m.START+5*m.MINUTE))

    def test_explicit_expiration_status_overrides_a_date_only_reference(self):
        from kline_lifetimes import window
        contract=dict(ric='TEST',start_ns=m.START,end_ns=m.START+10*m.MINUTE)
        status=pa.Table.from_pylist([self.event(0,'Y',action=7),self.event(4*m.MINUTE,'N',action=15,reason=5)])
        old=pa.Table.from_pylist([dict(ts_utc_ns=m.START+2*m.MINUTE,trade_count=1)])
        life=dict(activation_ns=m.START,expiration_ns=m.START+8*m.MINUTE)
        self.assertEqual(window(contract,status,old,life),(m.START,m.START+4*m.MINUTE))

    def test_cancel_recalculates_high_volume_and_trade_count(self):
        states=[dict(start_ns=m.START,end_ns=m.START+2*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+i*10**9,exch_hms_ns=i*10**9,price=p,volume=v)
            for i,p,v in [(0,100000000000,2),(10,900000000000,5),(20,101000000000,3)]]
        def plan(path):return dict(patches=[dict(filename=str(path),file_row_number=1,action='cancel',price=900000000000,
            volume=5,original_price=900000000000,original_volume=5,event_ns=m.START+10*10**9)],insertions=[])
        counts,bars=self.build(trades,states,2,revisions=plan)
        self.assertEqual(counts[:2],(2,5))
        self.assertEqual(bars['high'].to_pylist(),[101000000000]*2)
        self.assertEqual(bars['trade_count'].to_pylist(),[2,0])
        self.assertEqual(bars['volume'].to_pylist(),[5,0])

    def test_cancel_only_trade_updates_empty_minute_and_following_fill(self):
        states=[dict(start_ns=m.START,end_ns=m.START+4*m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START+i*m.MINUTE,exch_hms_ns=i*m.MINUTE,price=p,volume=v)
            for i,p,v in [(0,100000000000,2),(1,900000000000,5),(3,102000000000,3)]]
        def plan(path):return dict(patches=[dict(filename=str(path),file_row_number=1,action='cancel',price=900000000000,
            volume=5,original_price=900000000000,original_volume=5,event_ns=m.START+m.MINUTE)],insertions=[])
        _,bars=self.build(trades,states,4,revisions=plan)
        public=m.public_kline(bars)
        self.assertEqual(public['close'].to_pylist(),[100.,100.,100.,102.])
        self.assertEqual(public['is_ffill'].to_pylist(),[False,True,True,False])
        self.assertEqual(public['volume'].to_pylist(),[2,0,0,3])

    def test_amendment_changes_price_size_without_creating_a_second_trade(self):
        states=[dict(start_ns=m.START,end_ns=m.START+m.MINUTE,is_trading=True,session_id=1)]
        trades=[dict(ts_utc_ns=m.START,exch_hms_ns=0,price=100000000000,volume=2)]
        def plan(path):return dict(patches=[dict(filename=str(path),file_row_number=0,action='replace',price=110000000000,
            volume=4,original_price=100000000000,original_volume=2,event_ns=m.START)],insertions=[])
        counts,bars=self.build(trades,states,1,revisions=plan)
        self.assertEqual(counts[:2],(1,4));self.assertEqual(m.public_kline(bars)['close'].to_pylist(),[110.])

    def test_insert_can_supply_a_missing_priced_trade(self):
        states=[dict(start_ns=m.START,end_ns=m.START+m.MINUTE,is_trading=True,session_id=1)]
        plan=dict(patches=[],insertions=[dict(event_id='insert',published_ns=m.START,exch_hms_ns=0,
            event_ns=m.START,price=100000000000,volume=2)])
        counts,bars=self.build([],states,1,revisions=plan)
        self.assertEqual(counts[:2],(1,2));self.assertEqual(bars['trade_count'].to_pylist(),[1])


if __name__=='__main__':unittest.main()
