#!/usr/bin/env python3
"""Records which L2 source files / packages were washed into RocksDB (and where to fetch them again).

usage: l2_washed_sources.py --out <local.sqlite> [--check-dir <future_daily/normalisedLL2>]
       l2_washed_sources.py --publish <local.sqlite>

Reads the JSON result lines (`"status":"washed"`) that `lseg_l2_slice backfill` wrote to its logs and writes
  washed_units     one row per washed unit: kind (daily | package | supplement | raw_segments), unit, product, day,
                   rics, updates, input_gz_bytes, first/last time stamp, database, remote source, log
  washed_daily     (product, day, gz_bytes) of every daily file, for the "was this file washed" question
  conventions      meaning and how to fetch the sources again
The source files themselves may be deleted afterwards (`source_deleted`): the databases hold every update.
--check-dir lists daily files on disk that have NO washed record (they must not be deleted).
"""
import argparse, glob, json, os, re, sqlite3, sys, datetime

RT = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime"
DBROOT = "/mnt/hdd-raid5-72t/fanghaizhou/lseg_data/l2_db"  # moved from NVMe on 2026-11-24
REMOTE = "http://10.61.20.12/public/users/u158/public/lseg_data"
LOGS = (glob.glob(RT + "/l2_backfill/backfill*.log") + glob.glob(RT + "/l2_supplement/backfill_*.log")
        + glob.glob(RT + "/l2_old/backfill*.log") + glob.glob(RT + "/l2_old/fill/wash_*.log")
        + glob.glob(RT + "/l2_raw/backfill_*.log") + glob.glob(RT + "/l2_2010_2019/backfill.log") + glob.glob(RT + "/l2_old/fill10/wash_*.log") + glob.glob(RT + "/l2_daily_nov/wash.log") + glob.glob(RT + "/l2_daily/*/wash.log"))

ap = argparse.ArgumentParser()
ap.add_argument("--out")
ap.add_argument("--check-dir")
ap.add_argument("--publish")
ap.add_argument("--replace", action="store_true", help="with --publish: replace the existing file by an atomic rename")
a = ap.parse_args()
ROOT = "/mnt/nfs/30.100_cta_data/lseg/meta"
if a.publish:
    dst = ROOT + "/l2_sources.sqlite"
    tmp = "%s.%d.tmp" % (dst, os.getpid())
    open(tmp, "wb").write(open(a.publish, "rb").read())
    if a.replace:
        os.replace(tmp, dst)  # atomic: readers see the old or the new complete file
    else:
        assert not os.path.exists(dst), "l2_sources.sqlite exists already; use --replace"
        os.link(tmp, dst)
        os.unlink(tmp)
    print("published", dst)
    sys.exit(0)
assert a.out

units = {}
for log in sorted(LOGS):
    for line in open(log, errors="ignore"):
        line = line.strip()
        if not line.startswith("{") or '"status":"washed"' not in line:
            continue
        r = json.loads(line)
        units[r["unit"]] = dict(r, log=log)  # a later log of the same unit replaces an earlier one


def classify(unit):
    if ":" in unit:
        return "daily", unit.split(":")[0], unit.split(":")[1]
    if unit.startswith("raw"):
        return "raw_segments", None, None
    if unit[:2] in ("sg", "sf", "sc"):
        return "supplement", None, None
    return "package", None, None


SUPP = {"sg": "comex_globex_futures_market_depth_ric_list_0_ll2_%s-01-01_%s-01-01",
        "sf": "futures_11_market_depth_ric_list_0_ll2_%s-01-01_%s-01-01",
        "sc": "comex_futures_market_depth_ric_list_0_ll2_%s-01-01_%s-01-01"}


def remote_of(unit, kind, product, day):
    if kind == "daily":
        return "%s/future_daily/normalisedLL2/%s/%s_%s_normll2.csv.gz" % (REMOTE, product, product, day)
    if kind == "supplement":
        y = int(unit[2:6])
        return "%s/future_supplement/normalisedLL2/%s/merged-Data.csv.gz" % (REMOTE, SUPP[unit[:2]] % (y, y + 1))
    if kind == "raw_segments":
        return "%s/future/rawLL2/ (members cut with scripts/lseg/rawll2_segments.py)" % REMOTE
    return "%s/future/normalisedLL2/ (package of the year, e.g. %s)" % (REMOTE, unit)


if os.path.exists(a.out):
    os.unlink(a.out)
db = sqlite3.connect(a.out)
db.executescript("""
CREATE TABLE washed_units (unit TEXT PRIMARY KEY, kind TEXT NOT NULL, product TEXT, day TEXT, rics INTEGER, updates INTEGER,
 input_gz_bytes INTEGER, first_ts_utc TEXT, last_ts_utc TEXT, database TEXT NOT NULL, remote_source TEXT NOT NULL, log TEXT NOT NULL);
CREATE TABLE washed_daily (product TEXT NOT NULL, day TEXT NOT NULL, gz_bytes INTEGER, PRIMARY KEY(product, day));
CREATE TABLE conventions (key TEXT PRIMARY KEY, value TEXT NOT NULL);
""")
iso = lambda ns: datetime.datetime.fromtimestamp(ns / 1e9, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if ns else None
for unit, r in sorted(units.items()):
    kind, product, day = classify(unit)
    database = (DBROOT + "/%s" % day.replace("-", "")) if kind == "daily" else (DBROOT + "/_stream/%s" % unit)
    db.execute("insert into washed_units values (?,?,?,?,?,?,?,?,?,?,?,?)",
               (unit, kind, product, day, r.get("rics"), r.get("updates"), r.get("input_gz_bytes"), iso(r.get("first_ts")),
                iso(r.get("last_ts")), database, remote_of(unit, kind, product, day), r["log"]))
    if kind == "daily":
        db.execute("insert into washed_daily values (?,?,?)", (product, day, r.get("input_gz_bytes")))
now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
db.executemany("insert into conventions values (?,?)", {
    "built_utc": now,
    "meaning": "units washed into the L2 RocksDB; the source files named here may be deleted from the local disk (source_deleted): every update is in the database",
    "daily": "kind=daily: one product-day file of future_daily/normalisedLL2, database = l2_db/<YYYYMMDD>",
    "package": "kind=package: the main Normalized LL2 package of a UTC year (p2020..p2024, p2025 + supplement s2025, p2026h1), database = l2_db/_stream/<unit>",
    "supplement": "kind=supplement: comex_globex (sg), futures_11 (sf), comex (sc) supplement package of a year",
    "raw_segments": "kind=raw_segments: only the Raw LL2 members that hold RICs Normalized lacks (see docs/lseg/l2_raw.md); the rest of those Raw packages was NOT washed",
    "refetch": "remote_source is the URL of the original file; supplements and daily files are single files, main packages are fetched with scripts/lseg/fetch_normalised_ll2.sh, Raw members with scripts/lseg/rawll2_segments.py",
    "not_washed": "all Normalized main packages 2010-2026 are washed (2010-2019 on 2026-11-24 at the user's request); supplements and Raw only for the RICs listed as washed",
}.items())
db.commit()
n = {k: c for k, c in db.execute("select kind, count(*) from washed_units group by 1")}
print("washed units", n, "daily files", db.execute("select count(*) from washed_daily").fetchone()[0])
if a.check_dir:
    missing = []
    for p in sorted(glob.glob(a.check_dir + "/*/*_normll2.csv.gz")):
        m = re.match(r"(.+)_(\d{4}-\d{2}-\d{2})_normll2\.csv\.gz$", os.path.basename(p))
        if not m or not db.execute("select 1 from washed_daily where product=? and day=?", m.groups()).fetchone():
            missing.append((p, os.path.getsize(p)))
    print("daily files on disk without a washed record:", len(missing))
    for p, s in missing[:15]:
        print("  ", p, s)
