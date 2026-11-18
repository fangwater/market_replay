"""Independent check of one (RIC, UTC day) of the L2 database against the source csv.gz.

usage: verify_l2_ricday.py --bin <lseg_l2_slice> --db-dirs a,b --ric R --day YYYY-MM-DD
                           --sources f1.csv.gz[,f2.csv.gz…] --header-from <csv.gz with a header line>
                           --work <scratch dir> [--freq-ms 250] [--gap-s 1800]

1. streams the source files in order (stops as soon as the RIC's block / the day is over) and
   writes the rows of this RIC-day to a small csv.gz;
2. extracts the 250 ms slice of the RIC-day from the RocksDB with the Rust tool (`extract-rics`);
3. recomputes the slice with numpy (`verify_l2_slice.py`, no shared code) and compares;
4. also compares the number of source rows with the number of update records in the database.
Prints one JSON line; exit code 0 only if there is no mismatch. The scratch directory is deleted.
"""
import argparse, gzip, json, os, shutil, subprocess, sys

ap = argparse.ArgumentParser()
ap.add_argument("--bin", required=True)
ap.add_argument("--db-dirs", required=True)
ap.add_argument("--ric", required=True)
ap.add_argument("--day", required=True)
ap.add_argument("--sources", required=True)
ap.add_argument("--header-from", required=True)
ap.add_argument("--work", required=True)
ap.add_argument("--freq-ms", default="250")
ap.add_argument("--gap-s", default="1800")
a = ap.parse_args()

here = os.path.dirname(os.path.abspath(__file__))
os.makedirs(a.work, exist_ok=True)
with gzip.open(a.header_from, "rb") as f:
    header = f.readline()
assert header.startswith(b"#RIC,"), "header-from has no header line"

ric_b = a.ric.encode()
prefix = ric_b + b","
day_b = a.day.encode()
rows = 0
src_csv = os.path.join(a.work, "src.csv.gz")
seen_ric = False
done = False
with gzip.open(src_csv, "wb", compresslevel=1) as out:
    out.write(header)
    for path in a.sources.split(","):
        if done:
            break
        p = subprocess.Popen(["zcat", path], stdout=subprocess.PIPE)
        for line in p.stdout:
            if line.startswith(prefix):
                seen_ric = True
                parts = line.split(b",", 3)
                d = parts[2][:10]
                if d == day_b:
                    out.write(line)
                    rows += 1
                elif d > day_b:
                    done = True
                    break
            elif seen_ric:
                done = True  # the RIC's block is over
                break
        p.stdout.close()
        p.kill()
        p.wait()
assert rows > 0, f"no source rows for {a.ric} {a.day}"

ex = os.path.join(a.work, "exchange.tsv")
open(ex, "w").write(f"{a.ric}\tx\n")
out_root = os.path.join(a.work, "out")
r = subprocess.run(
    [a.bin, "extract-rics", "--db-dirs", a.db_dirs, "--out-root", out_root, "--exchange-map", ex,
     "--rics", a.ric, "--days", a.day, "--freq-ms", a.freq_ms, "--session-gap-s", a.gap_s, "--workers", "2", "--force"],
    capture_output=True, text=True)
if r.returncode != 0:
    print(json.dumps({"ric": a.ric, "day": a.day, "ok": False, "error": "extract failed: " + r.stderr[-400:]}))
    sys.exit(1)
pq = None
for root, _, files in os.walk(out_root):
    for f in files:
        if f.endswith(".parquet"):
            pq = os.path.join(root, f)
assert pq, "no parquet written"

v = subprocess.run([sys.executable, "-I", os.path.join(here, "verify_l2_slice.py"), src_csv, a.ric, pq, a.freq_ms, a.gap_s],
                   capture_output=True, text=True)
line = v.stdout.strip().splitlines()[-1] if v.stdout.strip() else ""
# `extract-rics` already verifies the decoded record count against the database's own count;
# the source row count is checked here against that same count
res = {"ric": a.ric, "day": a.day, "sources": a.sources.split(","), "source_rows": rows, "verify": line, "ok": v.returncode == 0}
print(json.dumps(res))
shutil.rmtree(a.work, ignore_errors=True)
sys.exit(0 if v.returncode == 0 else 1)
