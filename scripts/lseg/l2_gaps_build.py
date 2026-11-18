#!/usr/bin/env python3
"""Builds lseg/meta/l2_gaps.sqlite: RIC-years that have TAS data but no (or only partial) L2 250 ms data.

usage: l2_gaps_build.py --out <local.sqlite> --reports <dir>... [--packages-json f] [--problems tsv]
                        [--years 2024,2025,2026] [--threads 8] [--no-gate]
       l2_gaps_build.py --publish <local.sqlite> [--replace]

Inputs (all read-only):
  catalog.sqlite  `directories` (RIC-years that have TAS directories), `l2_directories` (what is delivered; run
                  `lseg_catalog l2-sync` first), `contracts` (product)
  <reports>       merged-Report.csv.gz of every L2 package we inspected (one file per package; the file name,
                  without .Report.csv.gz, is the package name); a *.Report.csv.gz that lists a RIC with Count 0
                  means the vendor package holds no data for it
  NFS walk        one listdir of every TAS RIC-year directory that has no L2 (bounded by --threads, default 8,
                  gated on the server job queue like the other jobs); for RIC-years that have L2 but fewer
                  L2 days than 0.8 * TAS days, each day directory is listed to count the days that really hold
                  trade.parquet or quote.parquet

tas_days   = day directories of the RIC-year (a day directory exists only when the delivery tool published at least
             one file for it); for 'partial' candidates recounted as day directories with trade.parquet or
             quote.parquet. TAS days after 2026-11-12 (the last L2 day) are not counted.
kind       none    = no L2 file for the RIC-year; partial = l2_days < 0.8 * tas_days
reason     not_in_any_package | report_count_zero | outside_package_window | unmapped_exchange | overlap_conflict
           | in_package_not_delivered (listed with data, covered, but nothing delivered: a defect to look at)
Publishing: a new file is hard-linked into place (never overwritten); --replace swaps an existing l2_gaps.sqlite
by an atomic rename, so a reader sees either the old or the new complete file.
"""
import argparse, collections, concurrent.futures as cf, csv, datetime, glob, gzip, json, os, sqlite3, sys, time, urllib.request

ROOT = "/mnt/nfs/30.100_cta_data/lseg"
CATALOG = "file:%s/meta/catalog.sqlite?mode=ro&immutable=1" % ROOT
LAST_L2_DAY = "20261112"
THRESHOLD = 0.8
SLACK_DAYS = 7

ap = argparse.ArgumentParser()
ap.add_argument("--out")
ap.add_argument("--reports", nargs="*", default=[])
ap.add_argument("--packages-json")
ap.add_argument("--problems")
ap.add_argument("--years", default="2024,2025,2026")
ap.add_argument("--threads", type=int, default=8)
ap.add_argument("--no-gate", action="store_true")
ap.add_argument("--publish")
ap.add_argument("--replace", action="store_true")
a = ap.parse_args()

if a.publish:
    dst = ROOT + "/meta/l2_gaps.sqlite"
    con = sqlite3.connect("file:%s?mode=ro" % a.publish, uri=True)
    assert con.execute("pragma integrity_check").fetchone()[0] == "ok"
    n = con.execute("select count(*) from l2_unfillable").fetchone()[0]
    con.close()
    tmp = "%s.%d.tmp" % (dst, os.getpid())
    with open(a.publish, "rb") as f, open(tmp, "wb") as g:
        while True:
            b = f.read(1 << 24)
            if not b:
                break
            g.write(b)
        g.flush()
        os.fsync(g.fileno())
    try:
        if a.replace:
            os.rename(tmp, dst)  # atomic: readers keep the old inode or see the new complete file
        else:
            os.link(tmp, dst)  # fails if the file exists: never overwrites
            os.unlink(tmp)
    except Exception:
        os.path.exists(tmp) and os.unlink(tmp)
        raise
    print("published", dst, "rows", n)
    sys.exit(0)

assert a.out, "--out required"
years = [int(y) for y in a.years.split(",")]


def gate():
    if a.no_gate:
        return
    st = json.load(urllib.request.build_opener(urllib.request.ProxyHandler({})).open("http://127.0.0.1:18313/api/admin/status", timeout=10))
    now = datetime.datetime.fromisoformat(st["now"].replace("Z", "+00:00"))
    for j in st["jobs"]:
        if j.get("running"):
            sys.exit("gate: job %s is running" % j["name"])
        nr = j.get("next_run")
        if nr and 0 <= (datetime.datetime.fromisoformat(nr.replace("Z", "+00:00")) - now).total_seconds() < 600:
            sys.exit("gate: job %s starts within 10 minutes" % j["name"])
    if st.get("queue"):
        sys.exit("gate: queue not empty")


gate()
cat = sqlite3.connect(CATALOG, uri=True)
prod_ex = {}
for ric, prod, ex in cat.execute("select ric, product, exchange from contracts"):
    prod_ex.setdefault(ric, (prod, ex))
l2 = {(r, y): (fd, ld, d) for r, y, fd, ld, d in cat.execute("select ric, utc_year, first_day, last_day, days from l2_directories")}
tas_rows = [(r, p, y, d) for _, p, r, y, d in cat.execute(
    "select source_year, product, ric, utc_year, directory from directories where utc_year in (%s)" % ",".join(map(str, years)))]
# a RIC-year can appear under two source_years: keep one row per (ric, utc_year)
tas = {}
for r, p, y, d in tas_rows:
    tas.setdefault((r, y), (p, d))
print("TAS RIC-years", len(tas), "L2 RIC-years", sum(1 for k in l2 if k[1] in years), file=sys.stderr)

# reports -------------------------------------------------------------------
reports = {}  # package -> {ric: (count, start, end)}
paths = []
for d in a.reports:
    paths += sorted(glob.glob(os.path.join(d, "*.Report.csv.gz")))
for p in paths:
    name = os.path.basename(p)[: -len(".Report.csv.gz")]
    rows = {}
    with gzip.open(p, "rt", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows[r["#RIC"]] = (int(r.get("Count") or 0), r["Start"][:10], r["End"][:10])
    reports[name] = rows


def pkg_years(name):
    import re
    m = re.search(r"_(\d{4})-(\d\d-\d\d)_(\d{4})-(\d\d-\d\d)", name)
    return (m.group(1) + m.group(2).replace("-", ""), m.group(3) + m.group(4).replace("-", "")) if m else ("", "")


def pkgs_for(year):
    out = []
    for n in reports:
        s, e = pkg_years(n)
        if s[:4] == str(year):
            out.append(n)
    return sorted(out)


problems = {}
if a.problems and os.path.exists(a.problems):
    for line in open(a.problems):
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 3:
            problems[(parts[0], int(parts[1]))] = (parts[2], parts[3] if len(parts) > 3 else "")


# NFS walk ------------------------------------------------------------------
def days_of(d):
    try:
        return sorted(x for x in os.listdir(d) if len(x) == 8 and x.isdigit() and x <= LAST_L2_DAY)
    except FileNotFoundError:
        return []


def real_days(d, days):
    out = []
    for x in days:
        try:
            names = set(os.listdir(os.path.join(d, x)))
        except FileNotFoundError:
            continue
        if "trade.parquet" in names or "quote.parquet" in names:
            out.append(x)
    return out


def work(item):
    (ric, y), (prod, d) = item
    days = days_of(d)
    if not days:
        return (ric, y), None
    have = l2.get((ric, y))
    if have and have[2] >= THRESHOLD * len(days):
        return (ric, y), days  # not a candidate: no exact recount needed
    if have:
        days = real_days(d, days)
    return (ric, y), days


t0 = time.time()
walked = {}
with cf.ThreadPoolExecutor(max_workers=min(a.threads, 8)) as ex:
    for k, days in ex.map(work, tas.items(), chunksize=64):
        walked[k] = days
print("walked in %.0fs" % (time.time() - t0), file=sys.stderr)


def d8(x):
    return "%s-%s-%s" % (x[:4], x[4:6], x[6:])


def reason_for(ric, y, tfirst, tlast):
    names = pkgs_for(y)
    found = [(n, reports[n][ric]) for n in names if ric in reports[n]]
    ev = "checked %d packages for %d; " % (len(names), y)
    if (ric, y) in problems:
        return problems[(ric, y)][0], ev + problems[(ric, y)][1]
    if not found:
        return "not_in_any_package", ev + "RIC absent from every Report"
    pos = [(n, v) for n, v in found if v[0] > 0]
    if not pos:
        n, v = found[0]
        return "report_count_zero", ev + "%s lists it with Count 0" % n
    s = min(v[1] for _, v in pos)
    e = max(v[2] for _, v in pos)
    lo = (datetime.date.fromisoformat(tfirst) + datetime.timedelta(days=SLACK_DAYS)).isoformat()
    hi = (datetime.date.fromisoformat(tlast) - datetime.timedelta(days=SLACK_DAYS)).isoformat()
    if s > lo or e < hi:
        return "outside_package_window", ev + "%s Start..End %s..%s, TAS %s..%s" % (pos[0][0], s, e, tfirst, tlast)
    return "in_package_not_delivered", ev + "%s Count=%d %s..%s" % (pos[0][0], pos[0][1][0], s, e)


now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
checked = sorted(reports)
rows = []
for (ric, y), days in sorted(walked.items()):
    if not days:
        continue
    prod, ex_ = prod_ex.get(ric, ("?", "?"))
    exdir = tas[(ric, y)][1].split("/lseg/")[1].split("/")[0]
    have = l2.get((ric, y))
    if have is None:
        kind = "none"
    elif have[2] < THRESHOLD * len(days):
        kind = "partial"
    else:
        continue
    tf, tl = d8(days[0]), d8(days[-1])
    rs, ev = reason_for(ric, y, tf, tl)
    rows.append((ric, y, prod, exdir, tf, tl, len(days), have[2] if have else 0,
                 d8(have[0]) if have else None, d8(have[1]) if have else None, kind, rs, ev))

if os.path.exists(a.out):
    os.unlink(a.out)
db = sqlite3.connect(a.out)
db.executescript("""
CREATE TABLE l2_unfillable (
 ric TEXT NOT NULL, utc_year INTEGER NOT NULL, product TEXT NOT NULL, exchange_dir TEXT NOT NULL,
 tas_first_day TEXT, tas_last_day TEXT, tas_days INTEGER NOT NULL, l2_days INTEGER NOT NULL,
 l2_first_day TEXT, l2_last_day TEXT, kind TEXT NOT NULL, reason TEXT NOT NULL, evidence TEXT NOT NULL,
 checked_packages TEXT NOT NULL, checked_utc TEXT NOT NULL, PRIMARY KEY(ric, utc_year));
CREATE TABLE l2_package_inventory (
 package TEXT PRIMARY KEY, period TEXT, gb REAL, rics_with_data INTEGER, products INTEGER,
 washed_utc TEXT, delivered_rics_days INTEGER);
CREATE TABLE conventions (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE VIEW l2_unfillable_products AS
 SELECT product, utc_year, COUNT(*) rics, kind, GROUP_CONCAT(DISTINCT reason) reasons
 FROM l2_unfillable GROUP BY product, utc_year, kind;
""")
for r in rows:
    db.execute("insert into l2_unfillable values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
               r[:2] + (r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], json.dumps(checked), now))
inv = json.load(open(a.packages_json)) if a.packages_json else {}
for n in checked:
    rr = reports[n]
    rics = [k for k, v in rr.items() if v[0] > 0]
    pr = {prod_ex.get(k, ("?",))[0] for k in rics}
    inf = inv.get(n, {})
    db.execute("insert into l2_package_inventory values (?,?,?,?,?,?,?)",
               (n, "%s..%s" % pkg_years(n), inf.get("gb"), len(rics), len(pr), inf.get("washed_utc"), inf.get("delivered_rics_days")))
conv = {
    "built_utc": now, "years": a.years, "tas_source": "catalog.sqlite directories (RIC-years with TAS directories) + day directories listed on NFS",
    "l2_source": "catalog.sqlite l2_directories (lseg_catalog l2-sync); last L2 day %s" % LAST_L2_DAY,
    "tas_days": "day directories of the RIC-year up to the last L2 day; for partial candidates only days holding trade.parquet or quote.parquet",
    "partial_threshold": "l2_days < %.1f * tas_days" % THRESHOLD, "window_slack_days": str(SLACK_DAYS),
    "reasons": "not_in_any_package | report_count_zero | outside_package_window | unmapped_exchange | overlap_conflict | in_package_not_delivered",
    "daily_2026_note": "L2 from 2026-06-01 comes from daily files that have no Report here; 2026 reasons only know the package Reports up to 2026-05-30",
    "query": "select product, rics, kind, reasons from l2_unfillable_products where utc_year=2025 order by product",
}
db.executemany("insert into conventions values (?,?)", conv.items())
db.commit()
print("rows", len(rows), file=sys.stderr)
for k, y, n in db.execute("select kind, utc_year, count(*) from l2_unfillable group by 1,2"):
    print("  %s %s %d" % (k, y, n), file=sys.stderr)
db.close()
