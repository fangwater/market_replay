from collections import Counter
import json
from pathlib import Path

import databento as db
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parent
STATE_PATH = PROJECT_ROOT / ".core_six_definition_batch.json"
OUTPUT_DIR = PROJECT_ROOT / "parsed" / "core_six_definition_2020_present"
PARQUET_PATH = OUTPUT_DIR / "definition.parquet"
OUTRIGHT_PARQUET_PATH = OUTPUT_DIR / "definition_outrights.parquet"
SUMMARY_PATH = OUTPUT_DIR / "summary.json"
SAMPLE_PATH = OUTPUT_DIR / "sample.csv"
ANOMALIES_PATH = OUTPUT_DIR / "unexpected_outrights.csv"
CHUNK_ROWS = 250_000
CORE_ASSETS = ("ES", "NQ", "RTY", "YM", "GC", "CL")


def locate_input() -> Path:
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    candidates = [
        Path(path)
        for path in state.get("downloaded_paths", [])
        if str(path).endswith((".definition.dbn", ".definition.dbn.zst"))
    ]
    if len(candidates) != 1 or not candidates[0].is_file():
        raise RuntimeError(f"expected exactly one downloaded definition DBN, got {candidates}")
    return candidates[0]


def update_counter(counter: Counter[str], series: pd.Series) -> None:
    counts = series.fillna("<null>").astype(str).value_counts(dropna=False)
    counter.update({str(key): int(value) for key, value in counts.items()})


def sorted_counts(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


input_path = locate_input()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
temp_parquet_path = PARQUET_PATH.with_suffix(".parquet.tmp")
temp_outright_path = OUTRIGHT_PARQUET_PATH.with_suffix(".parquet.tmp")

store = db.DBNStore.from_file(input_path)
metadata = store.metadata
frames = store.to_df(
    price_type="float",
    pretty_ts=True,
    map_symbols=False,
    count=CHUNK_ROWS,
)

total_rows = 0
outright_rows = 0
raw_symbols: set[str] = set()
instrument_ids: set[int] = set()
outright_raw_symbols: set[str] = set()
outright_instrument_ids: set[int] = set()
outright_asset_rows: Counter[str] = Counter()
outright_asset_symbols: dict[str, set[str]] = {
    asset: set() for asset in CORE_ASSETS
}
unexpected_outrights: list[pd.DataFrame] = []
counters = {
    "asset": Counter(),
    "instrument_class": Counter(),
    "security_type": Counter(),
    "security_update_action": Counter(),
    "exchange": Counter(),
    "currency": Counter(),
}
ts_recv_min: pd.Timestamp | None = None
ts_recv_max: pd.Timestamp | None = None
ts_event_min: pd.Timestamp | None = None
ts_event_max: pd.Timestamp | None = None
sample: pd.DataFrame | None = None
writer: pq.ParquetWriter | None = None
outright_writer: pq.ParquetWriter | None = None

try:
    for frame in frames:
        if sample is None:
            sample = frame.head(100).copy()

        table = pa.Table.from_pandas(frame, preserve_index=True)
        if writer is None:
            writer = pq.ParquetWriter(
                temp_parquet_path,
                table.schema,
                compression="zstd",
                use_dictionary=True,
            )
        writer.write_table(table)

        all_outrights = frame.loc[frame["instrument_class"] == "F"]
        unexpected = all_outrights.loc[~all_outrights["asset"].isin(CORE_ASSETS)]
        if not unexpected.empty:
            unexpected_outrights.append(unexpected.copy())
        outright_frame = all_outrights.loc[all_outrights["asset"].isin(CORE_ASSETS)]
        if not outright_frame.empty:
            outright_table = pa.Table.from_pandas(outright_frame, preserve_index=True)
            if outright_writer is None:
                outright_writer = pq.ParquetWriter(
                    temp_outright_path,
                    outright_table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            outright_writer.write_table(outright_table)
            outright_rows += len(outright_frame)
            outright_raw_symbols.update(outright_frame["raw_symbol"].astype(str).unique())
            outright_instrument_ids.update(
                int(value) for value in outright_frame["instrument_id"].unique()
            )
            update_counter(outright_asset_rows, outright_frame["asset"])
            for asset, symbols in outright_asset_symbols.items():
                asset_frame = outright_frame.loc[outright_frame["asset"] == asset]
                symbols.update(asset_frame["raw_symbol"].astype(str).unique())

        total_rows += len(frame)
        raw_symbols.update(frame["raw_symbol"].astype(str).unique())
        instrument_ids.update(int(value) for value in frame["instrument_id"].unique())
        for name, counter in counters.items():
            update_counter(counter, frame[name])

        chunk_ts_recv_min = frame.index.min()
        chunk_ts_recv_max = frame.index.max()
        chunk_ts_event_min = frame["ts_event"].min()
        chunk_ts_event_max = frame["ts_event"].max()
        ts_recv_min = chunk_ts_recv_min if ts_recv_min is None else min(ts_recv_min, chunk_ts_recv_min)
        ts_recv_max = chunk_ts_recv_max if ts_recv_max is None else max(ts_recv_max, chunk_ts_recv_max)
        ts_event_min = chunk_ts_event_min if ts_event_min is None else min(ts_event_min, chunk_ts_event_min)
        ts_event_max = chunk_ts_event_max if ts_event_max is None else max(ts_event_max, chunk_ts_event_max)

        print(f"parsed rows: {total_rows:,}", flush=True)
finally:
    if writer is not None:
        writer.close()
    if outright_writer is not None:
        outright_writer.close()

if total_rows == 0 or sample is None:
    raise RuntimeError("definition DBN contained no records")
if outright_rows == 0:
    raise RuntimeError("definition DBN contained no outright futures")

temp_parquet_path.replace(PARQUET_PATH)
temp_outright_path.replace(OUTRIGHT_PARQUET_PATH)
sample.to_csv(SAMPLE_PATH, index=True)
if unexpected_outrights:
    pd.concat(unexpected_outrights).to_csv(ANOMALIES_PATH, index=True)
else:
    pd.DataFrame().to_csv(ANOMALIES_PATH, index=False)

summary = {
    "source": str(input_path),
    "dataset": str(metadata.dataset),
    "schema": str(metadata.schema),
    "requested_symbols": list(metadata.symbols),
    "not_found": list(metadata.not_found),
    "rows": total_rows,
    "columns": len(sample.reset_index().columns),
    "unique_raw_symbols": len(raw_symbols),
    "unique_instrument_ids": len(instrument_ids),
    "outrights": {
        "instrument_class": "F",
        "rows": outright_rows,
        "unique_raw_symbols": len(outright_raw_symbols),
        "unique_instrument_ids": len(outright_instrument_ids),
        "rows_by_asset": sorted_counts(outright_asset_rows),
        "unique_raw_symbols_by_asset": {
            asset: len(symbols) for asset, symbols in outright_asset_symbols.items()
        },
        "parquet": str(OUTRIGHT_PARQUET_PATH),
        "parquet_size_bytes": OUTRIGHT_PARQUET_PATH.stat().st_size,
        "unexpected_rows_excluded": sum(len(frame) for frame in unexpected_outrights),
        "unexpected_rows_csv": str(ANOMALIES_PATH),
    },
    "ts_recv_min": ts_recv_min.isoformat() if ts_recv_min is not None else None,
    "ts_recv_max": ts_recv_max.isoformat() if ts_recv_max is not None else None,
    "ts_event_min": ts_event_min.isoformat() if ts_event_min is not None else None,
    "ts_event_max": ts_event_max.isoformat() if ts_event_max is not None else None,
    "counts": {name: sorted_counts(counter) for name, counter in counters.items()},
    "parquet": str(PARQUET_PATH),
    "parquet_size_bytes": PARQUET_PATH.stat().st_size,
    "sample_csv": str(SAMPLE_PATH),
}
SUMMARY_PATH.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

print(f"parquet: {PARQUET_PATH}", flush=True)
print(f"outrights: {OUTRIGHT_PARQUET_PATH}", flush=True)
print(f"sample: {SAMPLE_PATH}", flush=True)
print(f"summary: {SUMMARY_PATH}", flush=True)
