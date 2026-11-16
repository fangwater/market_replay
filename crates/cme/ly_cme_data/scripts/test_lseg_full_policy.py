import unittest
from lseg_full_policy import window,DAY,MINUTE
from lseg_kline_state import intervals
from lseg_session_status import decode


class FullPolicyTests(unittest.TestCase):
    def test_decoder_cache_cannot_be_mutated_by_caller(self):
        value=decode('15[TRD_TYPE]',product='HG',cme_scope=True);value['session_state']='BROKEN'
        self.assertEqual(decode('15[TRD_TYPE]',product='TY',cme_scope=True)['session_state'],'TRADING')

    def test_no_borrowing_reference_from_future_cycle(self):
        lo,hi,issues=window('X0','X','review',0,100*DAY,[],[dict(first=200*DAY,expiry=300*DAY)],DAY,2*DAY,3*DAY)
        self.assertEqual(lo,DAY);self.assertEqual(hi,3*DAY+MINUTE)
        self.assertEqual(issues[0]['issue'],'no_unique_lseg_expiry')

    def test_same_raw_ric_conflicting_cycles_not_guessed(self):
        refs=[dict(first=-DAY,expiry=2*DAY),dict(first=-DAY,expiry=90*DAY)]
        _,hi,issues=window('X0','X','review',0,100*DAY,[],refs,DAY,DAY,2*DAY)
        self.assertEqual(hi,2*DAY+MINUTE);self.assertEqual(issues[0]['issue'],'conflicting_lseg_lifetimes')

    def test_other_exchange_native_cme_codes_not_assumed(self):
        rows=[dict(ts_utc_ns=0,qualifiers='17[TRD_TYPE];T[INST_PHASE];0[HALT_REASN]')]
        self.assertEqual(intervals(rows,'HG',0,MINUTE,cme_scope=False)['phase'].to_pylist(),['UNKNOWN'])
        self.assertEqual(intervals(rows,'HG',0,MINUTE,cme_scope=True)['phase'].to_pylist(),['TRADING'])

    def test_explicit_halt_is_exchange_independent(self):
        rows=[dict(ts_utc_ns=0,qualifiers='H[TRD_STATUS]'),dict(ts_utc_ns=MINUTE//2,qualifiers='0[HALT_REASN]')]
        self.assertEqual(intervals(rows,'X',0,MINUTE,cme_scope=False)['phase'].to_pylist(),['HALTED'])

    def test_true_trade_after_expiry_retained(self):
        rows=[dict(ts_utc_ns=DAY,qualifiers='4[HALT_REASN]')]
        _,hi,issues=window('X','X','cme',0,10*DAY,rows,[],0,2*DAY)
        self.assertEqual(hi,2*DAY+MINUTE);self.assertEqual(issues[-1]['issue'],'trade_after_lseg_expiry')


if __name__=='__main__':unittest.main()
