#!/usr/bin/env python3
"""Validate a downloaded merged TAS package against source length, ETag and MD5."""
import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    path = args.directory / "download_manifest.json"
    manifest = json.loads(path.read_text())
    files = manifest["files"]
    notes = next(f for f in files if f["name"].endswith(".notes.txt"))
    text = (args.directory / notes["name"]).read_text(encoding="utf-8-sig")
    hashes = re.findall(r"Extraction MD5 checksum for .*?:\s*([0-9a-fA-F]{32})", text)
    if len(set(hashes)) != 1:
        raise ValueError("ambiguous or missing vendor extraction MD5")
    expected_md5 = hashes[0].lower()
    data = next(f for f in files if f["name"] == "merged-Data.csv.gz")
    final = args.directory / data["name"]
    partial = args.directory / (data["name"] + ".partial")
    if Path(str(partial) + ".aria2").exists():
        raise ValueError("aria2 download is not complete")
    source = final if final.exists() else partial
    if final.exists() and partial.exists():
        raise ValueError("both final and partial files exist; refuse to overwrite")
    if source.stat().st_size != data["bytes"]:
        raise ValueError("downloaded Data size differs from source Content-Length")
    started = time.monotonic()
    md5, sha256 = hashlib.md5(), hashlib.sha256()
    with source.open("rb") as file:
        while chunk := file.read(16 * 1024 * 1024):
            md5.update(chunk)
            sha256.update(chunk)
    if md5.hexdigest() != expected_md5:
        raise ValueError(f"vendor MD5 mismatch: expected {expected_md5}, actual {md5.hexdigest()}")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for entry in files:
        with opener.open(urllib.request.Request(entry["url"], method="HEAD"), timeout=20) as response:
            headers = response.headers
        if int(headers["Content-Length"]) != entry["bytes"] or headers.get("ETag") != entry["etag"]:
            raise ValueError(f"remote object changed during download: {entry['name']}")
        if entry is not data:
            local = args.directory / entry["name"]
            if local.stat().st_size != entry["bytes"]:
                raise ValueError("metadata size mismatch")
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            if entry.get("sha256") != digest:
                raise ValueError("metadata SHA256 mismatch")
    report = next(f for f in files if f["name"] == "merged-Report.csv.gz")
    with gzip.open(args.directory / report["name"], "rt", newline="") as file:
        rows = list(csv.DictReader(file))
    roots = {}
    for root in ("LGO", "NK", "STF"):
        matches = [r for r in rows if re.fullmatch(re.escape(root) + r"[FGHJKMNQUVXZ][0-9]+(?:\^.*)?", r["#RIC"])
                   and int(r["Count"]) > 0]
        if not matches:
            raise ValueError(f"supplement Report contains no active RIC for {root}")
        roots[root] = dict(active_rics=len(matches), source_event_rows=sum(int(r["Count"]) for r in matches))
    if source == partial:
        os.rename(partial, final)
    data.update(status="downloaded_verified", md5=md5.hexdigest(), vendor_md5=expected_md5,
                sha256=sha256.hexdigest(), verification="source Content-Length, unchanged ETag, vendor extraction MD5")
    manifest.update(status="download_complete_verified", completed_at=datetime.now(timezone.utc).isoformat(),
                    verification_seconds=time.monotonic() - started,
                    report_active_rics=sum(int(r["Count"]) > 0 for r in rows),
                    report_source_event_rows=sum(int(r["Count"]) for r in rows), target_products=roots,
                    source_bytes=sum(f["bytes"] for f in files), cleaned_or_replayed=False,
                    note="Downloaded the complete merged futures_11 package; no source rows parsed or written to RocksDB.")
    tmp = path.with_suffix(".json.partial")
    tmp.write_text(json.dumps(manifest, indent=2) + "\n")
    os.replace(tmp, path)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
