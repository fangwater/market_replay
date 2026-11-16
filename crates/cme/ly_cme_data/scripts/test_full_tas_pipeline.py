import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import build_full_tas_klines as full


class FullPipelineTests(unittest.TestCase):
    def test_duckdb_obeys_runtime_budget_even_with_larger_requested_pool(self):
        with patch.dict(os.environ,{'LY_CME_DUCKDB_THREADS':'1','LY_CME_DUCKDB_MEMORY':'2GB'}):
            c=full.p.connection(32)
            try:
                self.assertEqual(c.execute("SELECT current_setting('threads')").fetchone()[0],1)
                amount,unit=c.execute("SELECT current_setting('memory_limit')").fetchone()[0].split()
                self.assertLessEqual(float(amount)*{'GiB':1024**3,'MiB':1024**2,'GB':10**9,'MB':10**6}[unit],2_000_000_000)
            finally:c.close()

    def test_late_cross_year_print_and_zero_volume_trade_are_preserved(self):
        temporary_root=full.WORK/'tests';temporary_root.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as directory:
            root=Path(directory);work=root/'work';run=root/'run';run.mkdir()
            boundary=full.stamp_year(2026);minute=full.p.MINUTE;second=1_000_000_000
            fields=['ric','ts_utc_ns','open','high','low','close','volume','trade_count','first_event_ns',
                'first_published_ns','first_order','last_event_ns','last_published_ns','last_order',
                'missing_exchange_time_trade_count','late_trade_count']
            schema=pa.schema([(f,pa.string() if f=='ric' else pa.int64() if f in {'open','high','low','close'} else pa.uint64()) for f in fields])
            def bar(at,price,volume,count,first,last,published,order):
                return dict(ric='FIXTURE0',ts_utc_ns=at,open=price*second,high=price*second,low=price*second,close=price*second,
                    volume=volume,trade_count=count,first_event_ns=first,last_event_ns=last,first_published_ns=published,
                    last_published_ns=max(published,last),first_order=order,last_order=order,missing_exchange_time_trade_count=0,
                    late_trade_count=count if published//minute>first//minute else 0)
            data={2025:[bar(boundary-minute,100,5,2,boundary-5*second,boundary-second,boundary-4*second,0)],
                2026:[bar(boundary-minute,-10,3,1,boundary-10*second,boundary-10*second,boundary+2*second,1),
                    bar(boundary,110,0,1,boundary+second,boundary+second,boundary+2*second,2)]}
            jobs=[]
            for index,year in enumerate((2025,2026)):
                job=dict(id=f'fixture_{year}',year=year,source_index=index,source='main',product='FIXTURE',
                    period=f'{year}-01-01_{year+1}-01-01')
                jobs.append(job);folder=work/'extract'/job['id'];folder.mkdir(parents=True)
                pq.write_table(pa.Table.from_pylist(data[year],schema=schema),folder/'minutes.parquet')
                (folder/'state.json').write_text(json.dumps(dict(identity=dict(job=job,current='fixture'))))
                (folder/'status.json').write_text('[]');(folder/'corrections.json').write_text('[]')
            captured={}
            def collect(table,venue,year,ric,kind,pool,sample=False):
                if kind=='kline':captured[year]=table.to_pylist()
                return []
            with patch.object(full,'WORK',work),patch.object(full,'RUN',run),patch.object(full.p,'WORK',work),\
                patch.object(full,'publish_days',collect),patch.object(full,'prune_padding'),patch.object(full,'write_problem'):
                a=full.build_group(2025,'FIXTURE',jobs,({}, {}, {}, {}, {}),None)
                b=full.build_group(2026,'FIXTURE',jobs,({}, {}, {}, {}, {}),None)
            self.assertEqual((a['trades'],b['trades'],a['volume']+b['volume']),(3,1,8))
            self.assertEqual(len(captured[2025]),1);self.assertEqual(len(captured[2026]),1)
            older=captured[2025][0]
            self.assertEqual((older['open'],older['high'],older['low'],older['close']),(-10,100,-10,100))
            self.assertEqual((older['volume'],older['trade_count'],older['ts_utc_ns']),(8,3,boundary-minute))
            newer=captured[2026][0]
            self.assertEqual((newer['open'],newer['volume'],newer['trade_count'],newer['is_ffill']),(110,0,1,False))


if __name__=='__main__':unittest.main()
