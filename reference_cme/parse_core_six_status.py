from collections import Counter
import json
from pathlib import Path
import re

import databento as db
import databento_dbn as dbn
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "parsed" / "core_six_status_2020_present"
PARQUET_PATH = OUTPUT_DIR / "status_outrights.parquet"
SUMMARY_PATH = OUTPUT_DIR / "summary.json"
SAMPLE_PATH = OUTPUT_DIR / "sample.csv"
CHUNK_ROWS = 250_000
OUTRIGHT_PATTERN = re.compile(r"^(ES|NQ|RTY|YM|GC|CL)[FGHJKMNQUVXZ]\d{1,2}$")
SOURCES = (
    (
        "equity",
        PROJECT_ROOT / "cme_equity_index_status_2024_2026.dbn",
        ("ES", "NQ", "RTY", "YM"),
    ),
    (
        "commodity",
        PROJECT_ROOT
        / "batch_downloads"
        / "GLBX-20260903-NHHKXUT6LW"
        / "glbx-mdp3-20200101-20260903.status.dbn.zst",
        ("GC", "CL"),
    ),
)


def enum_names(enum_type: type) -> dict[int, str]:
    return {
        int(getattr(enum_type, name)): name
        for name in dir(enum_type)
        if name.isupper()
    }


ACTION_NAMES = enum_names(dbn.StatusAction)
REASON_NAMES = enum_names(dbn.StatusReason)
TRADING_EVENT_NAMES = enum_names(dbn.TradingEvent)


def update_counter(counter: Counter[str], series: pd.Series) -> None:
    counts = series.fillna("<null>").astype(str).value_counts(dropna=False)
    counter.update({str(key): int(value) for key, value in counts.items()})


for _, path, _ in SOURCES:
    if not path.is_file():
        raise FileNotFoundError(path)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
temporary_path = PARQUET_PATH.with_suffix(".parquet.tmp")
writer: pq.ParquetWriter | None = None
total_input_rows = 0
total_output_rows = 0
sample_frames: list[pd.DataFrame] = []
counts_by_product: Counter[str] = Counter()
counts_by_action: Counter[str] = Counter()
coverage: dict[str, dict[str, object]] = {}

try:
    for source_name, path, allowed_products in SOURCES:
        store = db.DBNStore.from_file(path)
        metadata = store.metadata
        source_rows = 0
        source_output_rows = 0
        source_min: pd.Timestamp | None = None
        source_max: pd.Timestamp | None = None

        frames = store.to_df(pretty_ts=True, map_symbols=True, count=CHUNK_ROWS)
        for frame in frames:
            source_rows += len(frame)
            total_input_rows += len(frame)
            symbols = frame["symbol"].fillna("").astype(str)
            products = symbols.str.extract(OUTRIGHT_PATTERN, expand=False)
            keep = products.isin(allowed_products)
            if not keep.any():
                print(
                    f"{source_name}: scanned {source_rows:,}, kept {source_output_rows:,}",
                    flush=True,
                )
                continue

            selected = frame.loc[
                keep,
                [
                    "ts_event",
                    "publisher_id",
                    "instrument_id",
                    "action",
                    "reason",
                    "trading_event",
                    "is_trading",
                    "is_quoting",
                    "is_short_sell_restricted",
                    "symbol",
                ],
            ].copy()
            selected.insert(0, "product", products.loc[keep].astype(str).values)
            selected["source"] = source_name
            selected["action_name"] = selected["action"].map(ACTION_NAMES).fillna("UNKNOWN")
            selected["reason_name"] = selected["reason"].map(REASON_NAMES).fillna("UNKNOWN")
            selected["trading_event_name"] = (
                selected["trading_event"].map(TRADING_EVENT_NAMES).fillna("UNKNOWN")
            )

            table = pa.Table.from_pandas(selected, preserve_index=True)
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary_path,
                    table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(table)

            if sum(len(value) for value in sample_frames) < 200:
                sample_frames.append(selected.head(200).copy())
            source_output_rows += len(selected)
            total_output_rows += len(selected)
            update_counter(counts_by_product, selected["product"])
            update_counter(counts_by_action, selected["action_name"])
            chunk_min = selected.index.min()
            chunk_max = selected.index.max()
            source_min = chunk_min if source_min is None else min(source_min, chunk_min)
            source_max = chunk_max if source_max is None else max(source_max, chunk_max)
            print(
                f"{source_name}: scanned {source_rows:,}, kept {source_output_rows:,}",
                flush=True,
            )

        coverage[source_name] = {
            "source": str(path),
            "requested_symbols": list(metadata.symbols),
            "products_kept": list(allowed_products),
            "input_rows": source_rows,
            "output_rows": source_output_rows,
            "ts_recv_min": source_min.isoformat() if source_min is not None else None,
            "ts_recv_max": source_max.isoformat() if source_max is not None else None,
        }
finally:
    if writer is not None:
        writer.close()

if total_output_rows == 0:
    raise RuntimeError("status DBN files contained no core-six outright records")

temporary_path.replace(PARQUET_PATH)
pd.concat(sample_frames).head(200).to_csv(SAMPLE_PATH, index=True)
summary = {
    "dataset": "GLBX.MDP3",
    "schema": "status",
    "scope": "core-six outright futures",
    "input_rows": total_input_rows,
    "rows": total_output_rows,
    "rows_by_product": dict(sorted(counts_by_product.items())),
    "rows_by_action": dict(
        sorted(counts_by_action.items(), key=lambda item: (-item[1], item[0]))
    ),
    "coverage": coverage,
    "parquet": str(PARQUET_PATH),
    "parquet_size_bytes": PARQUET_PATH.stat().st_size,
    "sample_csv": str(SAMPLE_PATH),
}
SUMMARY_PATH.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

print(f"status parquet: {PARQUET_PATH}", flush=True)
print(f"summary: {SUMMARY_PATH}", flush=True)
