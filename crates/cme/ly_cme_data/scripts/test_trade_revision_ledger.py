import unittest

from trade_revision_ledger import resolve,source_record,combine_corrections

BASE=1767312000000000000


def raw(kind='Trade',at=0,**values):
    fields={'#RIC':'TEST','Date-Time':f'2026-01-02T00:00:{at:02d}.000000000Z','Type':kind,
        'Price':'100','Volume':'2','Exch Time':'00:00:00.000000000','Date':'2026-01-02',
        'Unique Trade Identification':'A','Seq. No.':'100'}
    if kind=='Correction':
        for name in ['Price','Volume','Unique Trade Identification','Seq. No.']:fields.pop(name,None)
        fields.update({'Original Price':'100','Original Volume':'2','Original Unique Trade Identification':'A',
            'Qualifiers':'611[CAN_COND];2[CAN_COND_N]'})
    fields.update(values)
    return source_record({'source':'fixture','line':at+1,'fields':fields})


def tick(row=0,at=0,price=100000000000,volume=2):
    return dict(ric='TEST',filename='test.parquet',file_row_number=row,published_ns=BASE+at*10**9,
        event_ns=BASE,exch_hms_ns=0,price=price,volume=volume)


class LedgerTests(unittest.TestCase):
    def test_cancel_by_original_id_and_repeat_is_idempotent(self):
        result=resolve([tick()],[raw(),raw('Correction',1),raw('Correction',2)])
        self.assertEqual(len(result['patches']),1)
        self.assertEqual(result['patches'][0]['action'],'cancel')
        self.assertEqual(result['unresolved'],[])

    def test_cancel_with_new_fields_is_not_an_amendment_or_insert(self):
        r=raw('Correction',1,Price='101',Volume='9')
        result=resolve([tick()],[raw(),r])
        self.assertEqual(result['patches'][0]['action'],'cancel')
        self.assertEqual(result['insertions'],[])

    def test_complementary_correction_fragments_apply_once(self):
        blank=raw('Correction',1,Qualifiers='')
        marked=raw('Correction',1,Price='101',Volume='9')
        groups=combine_corrections([blank,marked])
        self.assertEqual(len(groups),1)
        self.assertEqual(groups[0]['action'],'cancel')
        result=resolve([tick()],[raw(),blank,marked])
        self.assertEqual(len(result['applied']),1)

    def test_missing_id_can_use_unique_full_payload_and_clock(self):
        trade=raw(**{'Unique Trade Identification':''})
        result=resolve([tick()],[trade,raw('Correction',1)])
        self.assertEqual(result['patches'][0]['action'],'cancel')
        self.assertEqual(result['applied'][0]['basis'],'unique_original_payload')

    def test_ambiguous_originals_are_preserved_even_if_current_clock_matches_one(self):
        a=raw(**{'Unique Trade Identification':''})
        b=raw(at=1,**{'Unique Trade Identification':'','Exch Time':'00:00:01.000000000'})
        other=tick(row=1,at=1);other['exch_hms_ns']=10**9;other['event_ns']+=10**9
        result=resolve([tick(),other],[a,b,raw('Correction',2)])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['unresolved'][0]['reason'],'multiple_original_candidates')

    def test_known_conflicting_identifier_is_not_overridden_by_price(self):
        result=resolve([tick()],[raw(**{'Unique Trade Identification':'B'}),raw('Correction',1)])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['unresolved'][0]['reason'],'original_trade_not_found')

    def test_current_last_sale_clock_is_not_assumed_to_be_original_clock(self):
        cancel=raw('Correction',1,**{'Exch Time':'00:00:10.000000000'})
        result=resolve([tick()],[raw(**{'Unique Trade Identification':''}),cancel])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['unresolved'][0]['reason'],'original_clock_not_confirmed')

    def test_new_insert_is_counted_once_and_existing_trade_not_added_again(self):
        insert=raw('Correction',1,Price='101',Volume='3',Qualifiers='2[INSSALCOND]',**{
            'Original Price':'','Original Volume':'','Original Unique Trade Identification':''})
        result=resolve([tick()],[raw(),insert])
        self.assertEqual(len(result['insertions']),1)
        t=tick(at=1,price=101000000000,volume=3)
        result=resolve([t],[raw(at=1,Price='101',Volume='3'),insert])
        self.assertEqual(result['insertions'],[])

    def test_real_amendments_preserve_one_trade_and_support_a_later_cancel(self):
        amend=raw('Correction',1,Price='101',Volume='3',Qualifiers='')
        cancel=raw('Correction',2,**{'Original Price':'101','Original Volume':'3'})
        result=resolve([tick()],[raw(),amend,cancel])
        self.assertEqual(result['patches'][0]['action'],'cancel')
        self.assertEqual(len(result['patches']),1)
        self.assertEqual(result['unresolved'],[])

    def test_insert_can_be_cancelled_later_without_a_physical_trade_patch(self):
        insert=raw('Correction',1,Price='101',Volume='3',Qualifiers='2[INSSALCOND]',**{
            'Original Price':'','Original Volume':'','Original Unique Trade Identification':'','Unique Trade Identification':'B'})
        cancel=raw('Correction',2,**{'Original Price':'101','Original Volume':'3','Original Unique Trade Identification':'B'})
        result=resolve([tick()],[raw(),insert,cancel])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['insertions'],[])
        self.assertEqual(result['unresolved'],[])

    def test_weaker_sequence_cannot_override_conflicting_unique_id(self):
        cancel=raw('Correction',1,**{'Original Seq. No.':'100'})
        result=resolve([tick()],[raw(**{'Unique Trade Identification':'B'}),cancel])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['unresolved'][0]['reason'],'original_trade_not_found')

    def test_ambiguous_amendment_clock_is_not_silently_retimed(self):
        amend=raw('Correction',1,Price='101',Qualifiers='',**{'Exch Time':'00:00:05.000000000'})
        result=resolve([tick()],[raw(),amend])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['unresolved'][0]['reason'],'amendment_clock_change_requires_review')

    def test_missing_source_witness_is_not_treated_as_an_empty_trade_identifier(self):
        result=resolve([tick()],[raw('Correction',1)])
        self.assertEqual(result['patches'],[])
        self.assertEqual(result['unresolved'][0]['reason'],'original_source_identity_unrecovered')


if __name__=='__main__':unittest.main()
