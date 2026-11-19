#!/usr/bin/env python3
"""Cuts the byte ranges of a Raw LL2 package that hold the wanted RICs (local files or HTTP).

usage: rawll2_segments.py --source <package dir or http URL ending in /> --wanted <ric list> --out <dir>
                          [--parts 0,1,..] [--stride-mb 32]

A Raw LL2 part is a concatenation of independent gzip members (~20 MB compressed, ~350 MB text, header
`1f8b0800 00000000 00ff`) and the data is sorted by RIC across the parts. So a RIC can be found by bisecting
byte offsets: probe(offset) = the first member that starts at or after the offset and its first RIC.
Per wanted RIC the segment is [last member whose first RIC < RIC, the member after the first member whose
first RIC > RIC]; the extra member holds the tail of a message cut at the boundary. Overlapping
segments are merged. Each segment is written as its own gzip file `<out>/<part>.<k>.csv.gz` (whole members, so
every file decompresses on its own), listed in `<out>/segments.json` in stream order.

Only the bytes of the segments are read in full; every probe reads one member start (about half a member).
"""
import argparse, json, os, re, sys, urllib.request, zlib

MAGIC = bytes.fromhex("1f8b0800000000000" "0ff")  # 10-byte header of the members
BLOCK = 4 << 20
MAX_SCAN = 96 << 20  # no member start within this distance: end of file

ap = argparse.ArgumentParser()
ap.add_argument("--source", required=True)
ap.add_argument("--wanted", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--parts", help="comma list of part numbers (default: all)")
ap.add_argument("--dry-run", action="store_true", help="only locate and report the segment sizes; write nothing")
ap.add_argument("--stride-mb", type=int, default=32, help="bisection stops when the bracket is this small")
a = ap.parse_args()
wanted = sorted(l.strip() for l in open(a.wanted) if l.strip() and not l.startswith("#"))
if not a.dry_run:
    os.makedirs(a.out, exist_ok=True)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
remote = a.source.startswith("http")
stats = {"probe_bytes": 0, "probes": 0, "segment_bytes": 0}


class Src:
    def __init__(self, name):
        self.name = name
        if remote:
            self.url = a.source + name
            r = opener.open(urllib.request.Request(self.url, method="HEAD"), timeout=60)
            self.size = int(r.headers["Content-Length"])
        else:
            self.path = os.path.join(a.source, name)
            self.size = os.path.getsize(self.path)
        self.memo = {}

    def read(self, off, n):
        n = min(n, self.size - off)
        if n <= 0:
            return b""
        if remote:
            req = urllib.request.Request(self.url, headers={"Range": "bytes=%d-%d" % (off, off + n - 1)})
            return opener.open(req, timeout=120).read()
        with open(self.path, "rb") as f:
            f.seek(off)
            return f.read(n)

    def first_ric(self, off):
        """First RIC of the member at `off`, or None if it is not a valid member start."""
        head = self.read(off, 1 << 20)
        if head[:10] != MAGIC:
            return None
        try:
            d = zlib.decompressobj(-15)
            text = d.decompress(head[10:], 1 << 20)
        except zlib.error:
            return None
        lines = text.split(b"\n")[:-1]
        for ln in lines:
            if ln and ln[:1] not in (b",", b"#"):
                f = ln.split(b",")
                if len(f) == 14 and f[1] == b"Market Price":
                    return f[0].decode()
                return None
        return None

    def member_after(self, off):
        """(start, first RIC) of the first member starting at or after `off`; None at the end."""
        if off in self.memo:
            return self.memo[off]
        pos, res = off, None
        while pos < self.size and pos - off < MAX_SCAN:
            blk = self.read(pos, BLOCK)
            stats["probe_bytes"] += len(blk)
            i = 0
            while True:
                i = blk.find(MAGIC, i)
                if i < 0:
                    break
                r = self.first_ric(pos + i)
                if r is not None:
                    res = (pos + i, r)
                    break
                i += 1
            if res or len(blk) < BLOCK:
                break
            pos += len(blk) - len(MAGIC) + 1
        stats["probes"] += 1
        self.memo[off] = res
        return res

    def bracket(self, ric, last_lt):
        """last_lt: start of a member with first RIC < ric that is the last one up to the bracket size.
        else: start of a member whose first RIC > ric (or the file size)."""
        lo, hi = 0, self.size
        m0 = self.member_after(0)
        assert m0, "no member at the start of " + self.name
        lo_m = m0[0]
        hi_m = self.size
        while hi - lo > a.stride_mb << 20:
            mid = (lo + hi) // 2
            m = self.member_after(mid)
            if m is None or m[0] >= hi:
                hi = mid
                continue
            ok = m[1] < ric if last_lt else m[1] <= ric
            if ok:
                lo, lo_m = m[0], m[0]
            else:
                hi, hi_m = m[0], m[0]
        return lo_m if last_lt else hi_m


def parts_of():
    if remote:
        html = opener.open(a.source, timeout=60).read().decode()
        names = sorted(set(re.findall(r'href="(merged-Data-part-\d+\.csv\.gz)"', html)))
    else:
        names = sorted(n for n in os.listdir(a.source) if re.fullmatch(r"merged-Data-part-\d+\.csv\.gz", n))
    if a.parts:
        keep = {int(x) for x in a.parts.split(",")}
        names = [n for n in names if int(re.search(r"(\d+)", n[len("merged-Data-part-"):]).group(1)) in keep]
    return names


names = parts_of()
srcs = [Src(n) for n in names]
firsts = []
for s in srcs:
    m = s.member_after(0)
    firsts.append(m[1])
print("parts", len(srcs), "first RICs", firsts, file=sys.stderr)

segments = []
for pi, s in enumerate(srcs):
    lo_ric = firsts[pi]
    hi_ric = firsts[pi + 1] if pi + 1 < len(srcs) else None
    ws = [w for w in wanted if w >= lo_ric and (hi_ric is None or w <= hi_ric)]
    if pi == 0:
        ws = [w for w in wanted if hi_ric is None or w <= hi_ric]  # RICs before the first one: none in this part
    iv = []
    for w in ws:
        st = s.bracket(w, True)
        en = s.bracket(w, False)
        nxt = s.member_after(en + 1) if en < s.size else None
        end = nxt[0] if nxt else s.size
        iv.append((st, end))
    iv.sort()
    merged = []
    for st, en in iv:
        if merged and st <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], en)
        else:
            merged.append([st, en])
    for k, (st, en) in enumerate(merged):
        out = os.path.join(a.out, "%s.%d.csv.gz" % (s.name[:-len(".csv.gz")], k))
        if a.dry_run:
            stats["segment_bytes"] += en - st
            continue
        with open(out + ".tmp", "wb") as f:
            off = st
            while off < en:
                blk = s.read(off, min(64 << 20, en - off))
                f.write(blk)
                off += len(blk)
        os.rename(out + ".tmp", out)
        stats["segment_bytes"] += en - st
        segments.append({"file": out, "part": s.name, "start": st, "end": en, "bytes": en - st})
    print("%s: %d wanted RICs, %d segments, %.2f GB of %.1f GB" % (
        s.name, len(ws), len(merged), sum(e - b for b, e in merged) / 1e9, s.size / 1e9), file=sys.stderr)

if a.dry_run:
    print(json.dumps(stats), file=sys.stderr)
    sys.exit(0)
json.dump({"source": a.source, "wanted": len(wanted), "segments": segments, "stats": stats},
          open(os.path.join(a.out, "segments.json"), "w"), indent=1)
print(json.dumps(stats), file=sys.stderr)
