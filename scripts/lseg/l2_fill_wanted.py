#!/usr/bin/env python3
"""RIC lists to fill L2 for given UTC years from the supplement packages (Normalized LL2) and then Raw LL2.

usage: l2_fill_wanted.py --years 2020,2021 --reports <supplement reports dir> --raw-reports <dir with raw_<year>.csv.gz>
                         --out <dir>

Per year (written to <out>/):
  supp_globex_<y>.txt, supp_f11_<y>.txt : RICs of the supplement package with Count>0 that the main package does not hold
                                          (Count>0 in the main Report) - the main package wins; a RIC goes to the first package
                                          that has it (comex_globex, then futures_11). No TAS condition (as for 2024-2026).
  supp_comex_<y>.txt                    : same for the comex_futures supplement
  raw_<y>.txt                           : Raw-active RICs (Count>0) that have TAS directories in the year (catalog `directories`; no TAS
                                          condition with --any-tas) and no L2 yet: not delivered (catalog l2_directories), not in the main
                                          package and not in a supplement list above.
Prints the counts. Everything is read-only (catalog opened immutable).
"""
import argparse, csv, glob, gzip, os, sqlite3

ap = argparse.ArgumentParser()
ap.add_argument("--years", required=True)
ap.add_argument("--reports", required=True)
ap.add_argument("--raw-reports", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--raw-all", action="store_true", help="Raw wanted = Raw-active RICs that neither the main package nor a supplement holds (no TAS / delivered condition; for years that are washed but not delivered yet)")
ap.add_argument("--catalog", default="/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite")
ap.add_argument("--main-glob", default="/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalisedLL2/*ll2_%s-01-01_*/merged-Report.csv.gz")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)


def active(path):
    with gzip.open(path, "rt", encoding="utf-8-sig") as f:
        return {r["#RIC"] for r in csv.DictReader(f) if int(r.get("Count") or 0) > 0}


cat = sqlite3.connect("file:%s?mode=ro&immutable=1" % a.catalog, uri=True)
for y in a.years.split(","):
    main = set()
    for p in glob.glob(a.main_glob % y):
        main |= active(p)
    taken = set()
    for tag, prefix in (("globex", "comex_globex_futures"), ("f11", "futures_11"), ("comex", "comex_futures")):
        found = glob.glob(os.path.join(a.reports, "%s_market_depth_ric_list_0_ll2_%s-01-01_*.Report.csv.gz" % (prefix, y)))
        rics = set()
        for p in found:
            rics |= active(p)
        new = sorted(rics - main - taken)
        taken |= set(new)
        open(os.path.join(a.out, "supp_%s_%s.txt" % (tag, y)), "w").write("".join(r + "\n" for r in new))
        print("year %s supplement %-6s: with data %d, new vs main %d" % (y, tag, len(rics), len(new)))
    tas = {r[0] for r in cat.execute("select ric from directories where utc_year=?", (int(y),))}
    l2 = {r[0] for r in cat.execute("select ric from l2_directories where utc_year=?", (int(y),))}
    raw_path = os.path.join(a.raw_reports, "raw_%s.csv.gz" % y)
    raw = active(raw_path) if os.path.exists(raw_path) else set()
    wanted = sorted(raw - main - taken) if a.raw_all else sorted(r for r in raw & tas if r not in l2 and r not in taken)
    open(os.path.join(a.out, "raw_%s.txt" % y), "w").write("".join(r + "\n" for r in wanted))
    print("year %s raw: active %d, with TAS %d, wanted %d%s" % (y, len(raw), len(raw & tas), len(wanted), " (Raw-only, no TAS condition)" if a.raw_all else " (no L2, not from a supplement)"))
