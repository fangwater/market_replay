#!/usr/bin/env python3
"""Independent Python readback: rebuild exact TAS values from small Parquet files."""
import argparse
import hashlib
import json
import struct
from pathlib import Path

import pyarrow.parquet as pq


def verify(root):
    results = []
    for report_path in sorted((root / "_audit").glob("p%3A*.json")):
        report = json.loads(report_path.read_text())
        for part in report["parts"]:
            table = pq.read_table(root / part["path"])
            rows = table.to_pylist()
            digest = hashlib.sha256()
            for row in rows:
                common = struct.pack("<2sBB16sQQ", b"CT", 1, part["kind"],
                                     row["ric"].encode().ljust(16, b"\0"),
                                     row["ts_utc_ns"], row["exch_hms_ns"])
                if part["kind"] == 1:
                    value = common + struct.pack("<qIqIqIB7x", row["price"], row["volume"],
                                                 row["bid"], row["bid_size"], row["ask"],
                                                 row["ask_size"], row["aggressor"])
                else:
                    value = common + struct.pack("<qIqI4x", row["bid"], row["bid_size"],
                                                 row["ask"], row["ask_size"])
                digest.update(value)
            if len(rows) != part["rows"] or digest.hexdigest() != part["value_sha256"]:
                raise ValueError(f"source value mismatch: {part['path']}")
            for actual, expected in ((rows[0], part["first"]), (rows[-1], part["last"])):
                if actual != {key: expected[key] for key in table.column_names}:
                    raise ValueError(f"endpoint mismatch: {part['path']}")
            results.append({"path": part["path"], "rows": len(rows), "value_sha256": digest.hexdigest()})
    if not results:
        raise ValueError("no completed sample files")
    return {"status": "passed", "files": len(results), "rows": sum(r["rows"] for r in results), "results": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = verify(args.root)
    body = json.dumps(result, indent=2) + "\n"
    if args.report:
        args.report.write_text(body)
    print(body)

