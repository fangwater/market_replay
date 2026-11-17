#!/usr/bin/env python3
"""RIC -> delivery exchange directory map for lseg_l2_slice (read-only catalog).

  l2_exchange_map.py <PRODUCT,PRODUCT,...|ALL> > map.tsv

Lines `RIC<TAB>exchange-directory` for the RICs of the given products as listed in the
delivery catalog (`directories` table), e.g. `ESZ26<TAB>cme`. A RIC that is not in the
catalog has no line; the tool then stops with an error instead of guessing.
"""
import sqlite3
import sys

CATALOG = "file:/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite?mode=ro&immutable=1"
want = None if sys.argv[1] == "ALL" else set(sys.argv[1].split(","))
con = sqlite3.connect(CATALOG, uri=True)
seen = {}
for product, ric, directory in con.execute("select product, ric, directory from directories"):
    if want is not None and product not in want:
        continue
    exchange = directory.split("/lseg/")[1].split("/")[0]
    if seen.setdefault(ric, exchange) != exchange:
        sys.exit(f"RIC {ric} is listed under two exchange directories")
for ric in sorted(seen):
    print(f"{ric}\t{seen[ric]}")
