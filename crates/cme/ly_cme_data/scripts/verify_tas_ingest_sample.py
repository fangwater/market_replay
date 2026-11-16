#!/usr/bin/env python3
"""Compare persisted trade/special/1s BBO bytes with an independent CSV baseline."""
import argparse
import calendar
from collections import Counter
import csv
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
import gzip
import itertools
import json
from pathlib import Path
import re
import struct
import sys

sys.path.insert(0, '/home/fanghaizhou/preprocess')
from lseg.cme_trade_codec import CmeTradeRecord, encode_cme_trade
from lseg.tas_replay import trade_kind

NS = 10**9
DAY = 86400 * NS
MP = -(1 << 63)
MV = (1 << 32) - 1
MT = (1 << 64) - 1

@lru_cache(maxsize=512)
def day_ns(day):
    return calendar.timegm(datetime.strptime(day, '%Y-%m-%d').timetuple()) * NS

def hms_ns(text):
    if not text:
        return MT
    hms, _, fraction = text.partition('.')
    h, m, s = map(int, hms.split(':'))
    return (h * 3600 + m * 60 + s) * NS + int(fraction.ljust(9, '0') or '0')

def ts_ns(text):
    assert text.endswith('Z'), text
    return day_ns(text[:10]) + hms_ns(text[11:-1])

def price(text):
    if not text:
        return MP
    value = Decimal(text) * NS
    assert value == value.to_integral(), text
    return int(value)

def size(text):
    return int(text) if text else MV

def quote_value(ric, row):
    ts, exch, bid, bs, ask, az = row
    return struct.pack('<2sBB16sQQqIqI4x', b'CT', 1, 3,
                       ric.encode().ljust(16, b'\0'), ts, exch, bid, bs, ask, az)

def overlay(old, new):
    return (new[0], new[1], new[2] if new[2] != MP else old[2],
            new[3] if new[3] != MV else old[3],
            new[4] if new[4] != MP else old[4], new[5] if new[5] != MV else old[5])

def verify(source, dump, rows):
    expected_trades = Counter()
    open_books, quotes = {}, {}
    classes = Counter()
    def flush(ric):
        bucket, row = open_books.pop(ric)
        key = (ric, bucket)
        previous = quotes.get(key)
        if previous is not None:
            row = overlay(previous, row) if row[0] >= previous[0] else overlay(row, previous)
        quotes[key] = row
    with gzip.open(source, 'rt', newline='') as f:
        reader = csv.DictReader(f)
        for row in itertools.islice(reader, rows):
            classes[row['Type']] += 1
            ric = row['#RIC']
            if row['Type'] == 'Trade' and trade_kind(row) in ('printable', 'special_user'):
                special = trade_kind(row) == 'special_user'
                aggr = re.findall(r'([^;\[\]]+)\[AGGRS_SID1\]', row['Qualifiers'])
                aggressor = {'BID': 1, 'ASK': 2, '': 0}[aggr[-1].strip().strip('"').strip() if aggr else '']
                expected_trades[encode_cme_trade(CmeTradeRecord(
                    ric, ts_ns(row['Date-Time']), hms_ns(row['Exch Time']),
                    price(row['Price']), size(row['Volume']), price(row['Bid Price']),
                    size(row['Bid Size']), price(row['Ask Price']), size(row['Ask Size']),
                    aggressor, 2 if special else 1))] += 1
            if row['Type'] != 'Quote':
                continue
            bid, bs = price(row['Bid Price']), size(row['Bid Size'])
            ask, az = price(row['Ask Price']), size(row['Ask Size'])
            if bid == MP or bs == MV:
                bid, bs = MP, MV
            if ask == MP or az == MV:
                ask, az = MP, MV
            if bid == MP and ask == MP:
                continue
            ts, exch = ts_ns(row['Date-Time']), hms_ns(row['Exch Time'])
            event = ts if exch == MT else (ts // DAY) * DAY + exch
            if exch != MT and event > ts + DAY // 2:
                event -= DAY
            elif exch != MT and ts > event + DAY // 2:
                event += DAY
            bucket = event // NS * NS
            value = (ts, exch, bid, bs, ask, az)
            if ric in open_books and open_books[ric][0] != bucket:
                flush(ric)
            if ric in open_books:
                value = overlay(open_books[ric][1], value)
            open_books[ric] = (bucket, value)
    for ric in list(open_books):
        flush(ric)
    actual_trades, actual_quotes = Counter(), {}
    with dump.open() as f:
        for line in f:
            _, key, value = json.loads(line)
            key, value = bytes.fromhex(key), bytes.fromhex(value)
            if key[0] in (1, 2):
                actual_trades[value] += 1
            elif key[0] == 3:
                ric = key[1:17].rstrip(b'\0').decode()
                actual_quotes[(ric, int.from_bytes(key[17:25], 'big'))] = value
    assert expected_trades == actual_trades, 'trade / special bytes or multiplicity differ'
    expected_quotes = {key: quote_value(key[0], row) for key, row in quotes.items()}
    assert expected_quotes == actual_quotes, '1s quote snapshot bytes differ'
    return {'status': 'passed', 'source_rows': sum(classes.values()), 'source_types': dict(classes),
            'trade_special_records_compared': sum(actual_trades.values()),
            'quote_records_compared': len(actual_quotes), 'comparison': 'exact value bytes and multiplicities'}

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('source', type=Path)
    p.add_argument('dump', type=Path)
    p.add_argument('--rows', type=int, default=1000000)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    result = verify(a.source, a.dump, a.rows)
    a.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
