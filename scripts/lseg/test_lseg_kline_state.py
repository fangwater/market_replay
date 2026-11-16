import unittest

from lseg_kline_state import intervals,lifetime


def row(at,text,order=0):
    return dict(product='ES',ric='ESM26',ts_utc_ns=at,qualifiers=text,source_order=order)


class StateTests(unittest.TestCase):
    def test_same_time_reason_patch(self):
        t=intervals([row(1,'4[TRD_TYPE];C[INST_PHASE]'),row(1,'0[HALT_REASN]',1)],'ES',0,10)
        self.assertEqual(t['phase'].to_pylist(),['UNKNOWN','SCHEDULED_CLOSED'])

    def test_missing_reason_is_unknown(self):
        t=intervals([row(1,'4[TRD_TYPE];C[INST_PHASE]')],'ES',0,10)
        self.assertEqual(t['phase'].to_pylist(),['UNKNOWN'])

    def test_halt_preopen_and_reopen(self):
        rows=[row(1,'17[TRD_TYPE];T[INST_PHASE];0[HALT_REASN]'),
            row(2,'2[TRD_TYPE];2[HALT_REASN]'),row(3,'21[TRD_TYPE];O[INST_PHASE];0[HALT_REASN]'),
            row(4,'17[TRD_TYPE];T[INST_PHASE]')]
        t=intervals(rows,'ES',0,10)
        self.assertEqual(t['phase'].to_pylist(),['UNKNOWN','TRADING','HALTED','TRADING'])
        self.assertEqual(t['end_ns'].to_pylist(),[1,2,4,10])

    def test_weekend_preparation_not_halt(self):
        rows=[row(1,'4[TRD_TYPE];C[INST_PHASE];0[HALT_REASN]'),
            row(2,'18[TRD_TYPE];C[INST_PHASE];2[HALT_REASN]'),row(3,'21[TRD_TYPE];O[INST_PHASE];2[HALT_REASN]')]
        self.assertEqual(intervals(rows,'ES',0,10)['phase'].to_pylist(),['UNKNOWN','SCHEDULED_CLOSED'])

    def test_gap_requires_explicit_marker(self):
        rows=[row(1,'17[TRD_TYPE];T[INST_PHASE]'),row(6,'0[HALT_REASN]'),row(7,'17[TRD_TYPE];T[INST_PHASE]')]
        t=intervals(rows,'ES',0,10,[(3,5)])
        self.assertEqual(t['phase'].to_pylist(),['UNKNOWN','TRADING','UNKNOWN','TRADING'])
        self.assertEqual(t['end_ns'].to_pylist(),[1,3,7,10])

    def test_warmup(self):
        t=intervals([row(-2,'4[TRD_TYPE];C[INST_PHASE];0[HALT_REASN]')],'ES',0,10)
        self.assertEqual(t['phase'].to_pylist(),['SCHEDULED_CLOSED'])

    def test_expiry_terminal(self):
        t=intervals([row(1,'4[HALT_REASN]'),row(2,'15[TRD_TYPE]')],'ES',0,10)
        self.assertEqual(t['phase'].to_pylist(),['UNKNOWN','EXPIRED'])

    def test_lifetime_keeps_sunday_trade_and_post_expiry_trade(self):
        lo,hi,cutoff,issues=lifetime(0,100,[row(21,'4[HALT_REASN]')],[40],[90],19,25,10)
        self.assertEqual((lo,hi,cutoff),(10,30,21));self.assertEqual(len(issues),1)

    def test_auxiliary_prelisting_does_not_extend_grid(self):
        rows=[row(10,'2[HALT_REASN];NH[HALT_RSN]'),row(40,'21[TRD_TYPE];O[INST_PHASE];0[HALT_REASN]')]
        lo,_,_,_=lifetime(0,100,rows,[50],[],first_trade=52,last_trade=60,minute=10)
        self.assertEqual(lo,40)


if __name__=='__main__':unittest.main()
