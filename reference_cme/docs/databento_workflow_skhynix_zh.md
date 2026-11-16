# Databento 数据交接手册：海力士与 CME（完整代码）

**这一个 Markdown 即可转交。** 所需命令和完整代码均在本文中，不依赖项目里的其他 Python 脚本。代码块可以直接在 Bash 终端执行。

给接手 agent 的任务说明：先检查现有文件并离线核验；已有数据直接解析、查询；缺少数据时用本文下载代码建独立批次。交付时说明数据集、证券、区间、过滤条件、文件位置、记录数和验证结果。

本文记录核对日期为 **2026-11-02**。数据文件本身不包含在 Markdown 中：同机可使用下面的现成文件，其他机器需复制数据文件，或凭自己的 Databento API key 下载。

## 1. 环境和基本参数

同机默认工作目录：

```bash
cd /home/fanghaizhou/cn_futures_db/reference_cme
```

其他机器可创建任意工作目录。以下命令只在尚未配置 Python 环境时执行；要求 Python 3.11 或更新版本：

```bash
python3 -m venv .venv
.venv/bin/pip install databento==0.86.0 pandas==3.0.5 pyarrow==25.0.1 python-dotenv==1.2.3
```

本地读取不需要 API key。联网查询、估价和下载时，在工作目录的 `.env` 文件中配置：

```dotenv
DATABENTO_API_KEY=替换为自己的_API_key
```

不要把真实 key 放进本文或输出日志。所有示例默认以当前工作目录为数据根目录；若数据在其他位置，可统一设置：

```bash
export DATABENTO_DATA_ROOT=/实际的数据目录
```

| 场景 | dataset | symbols 示例 | stype_in | schema |
| --- | --- | --- | --- | --- |
| 海力士美股 ADR | `XNAS.ITCH` | `SKHY`、`SKHYV` | `raw_symbol` | `mbp-10`、`trades` |
| CME 单个 ES 合约行情 | `GLBX.MDP3` | `ESU6` | `raw_symbol` | `mbp-10`、`trades` |
| CME 六品种合约定义 | `GLBX.MDP3` | `ES.FUT`、`NQ.FUT`、`RTY.FUT`、`YM.FUT`、`GC.FUT`、`CL.FUT` | `parent` | `definition` |
| CME 六品种交易状态 | `GLBX.MDP3` | 同上 | `parent` | `status` |

- `dataset` 决定数据源。`XNAS.ITCH` 是 Nasdaq 单市场，不能直接解释为全美股盘口、成交量或 NBBO；本例也不是韩国本土股票 `000660` 的数据。
- `raw_symbol` 指具体证券/合约的原始代码；`parent` 请求一个品种下的相关合约，结果可能包含价差等组合，后续要筛选单腿。
- `mbp-10` 是十档价位聚合盘口，随事件更新；`trades` 是成交记录；`definition` 是历史合约定义；`status` 是状态事件，不是静态交易时间表。
- 请求区间为 **`[start, end)`**，结束时间不包含在内。本例这些 schema 按 `ts_recv` 过滤；没有接收时间的 schema 才按 `ts_event` 过滤。
- UTC 日不等于交易日。美股展示可用 `America/New_York`，CME 可用 `America/Chicago`；交易日归属和节假日须另查交易日历。

## 2. 同机已有数据：先复用这些文件

下面路径均相对于数据根目录。原始批次已固定，不会因为今天日期变化自动延长。

| 数据 | 原始文件位置 | 已保存记录数 | 实际费用 USD |
| --- | --- | ---: | ---: |
| 海力士十档盘口 | `batch_downloads/XNAS-20261019-X5FNF49AC7/*.mbp-10.dbn.zst` | 54,965,876 | 7.535309 |
| 海力士成交 | `batch_downloads/XNAS-20261019-3Q7G8LJREK/*.trades.dbn.zst` | 6,288,371 | 1.686673 |
| CME 六品种定义 | `batch_downloads/GLBX-20260903-7R53J8HP56/glbx-mdp3-20200101-20260903.definition.dbn.zst` | 9,820,506 | 8.085116 |
| CME 商品状态原始批次 | `batch_downloads/GLBX-20260903-NHHKXUT6LW/glbx-mdp3-20200101-20260903.status.dbn.zst` | 54,542,347 | 8.127443 |
| CME 四股指状态 | `cme_equity_index_status_2024_2026.dbn` | 525,394¹ | 未在本表记录 |

¹ 股指状态行数来自已有解析统计；其他行数和费用来自保存的任务结果，费用不是当前报价。

海力士请求区间为 `2026-06-01` 至 `2026-10-17T00:00:00Z`，实际有记录的日文件为 **2026-07-10 至 2026-10-16**，每种 schema 各 70 个文件，总压缩体积 1,802,587,880 字节，实际费用约 9.22 美元。请求早于该 ADR 首日，6 月没有文件并不表示下载失败。

海力士已保存的代码映射如下，日期同样左闭右开；不要把某一天的 `instrument_id` 当作永久 ID：

| symbol | UTC 开始日期 | UTC 结束日期，不含 | instrument_id |
| --- | --- | --- | ---: |
| `SKHYV` | 2026-07-10 | 2026-07-13 | 23767 |
| `SKHY` | 2026-07-13 | 2026-07-14 | 23776 |
| `SKHY` | 2026-07-14 | 2026-10-17 | 23772 |

CME 定义原始批次为 `[2020-01-01, 2026-09-03T01:14:30.090201000Z)`；商品状态为 `[2020-01-01, 2026-09-03T00:29:09.630266000Z)`。商品状态原始批次包含 `GC/CL/SR3/KRW/6E/SR1`，六品种分析只保留其中 `GC/CL`；股指状态从 2024 年开始，不能声称六品种状态全部覆盖 2020 年。

已有的过滤结果可以直接用于第 6 节查询：

```text
parsed/core_six_definition_2020_present/definition_outrights.parquet
parsed/core_six_status_2020_present/status_outrights.parquet
```

前者有 392,765 条六品种单腿定义版本；后者有 2,746,124 条六品种单腿状态记录。这是版本/事件行数，不是合约数。

## 3. 离线核验已有批次，并读取真实样本

这段代码独立可运行：核对四个批次的 manifest、文件大小、SHA-256 和 DBN 头，再各读三行。SDK 直接读取 `.dbn.zst`，无需手工解压。

```bash
.venv/bin/python - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import databento as db

root = Path(os.environ.get("DATABENTO_DATA_ROOT", ".")).resolve()
jobs = ["XNAS-20261019-X5FNF49AC7", "XNAS-20261019-3Q7G8LJREK",
        "GLBX-20260903-7R53J8HP56", "GLBX-20260903-NHHKXUT6LW"]
samples = []
for job in jobs:
    directory = root / "batch_downloads" / job
    manifest = json.loads((directory / "manifest.json").read_text())
    query = json.loads((directory / "metadata.json").read_text())["query"]
    dbn_paths = []
    for item in manifest["files"]:
        path = directory / item["filename"]
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if path.stat().st_size != item["size"] or item["hash"] != "sha256:" + digest:
            raise RuntimeError(f"文件大小/哈希不匹配：{path}")
        if path.name.endswith((".dbn", ".dbn.zst")):
            meta = db.DBNStore.from_file(path).metadata
            if str(meta.dataset) != query["dataset"] or str(meta.schema) != query["schema"]:
                raise RuntimeError(f"DBN dataset/schema 不匹配：{path}")
            dbn_paths.append(path)
    if not dbn_paths:
        raise RuntimeError(f"批次没有 DBN：{job}")
    print(job, "校验通过，DBN 文件数：", len(dbn_paths))
    samples.append(sorted(dbn_paths)[0])
samples.append(root / "cme_equity_index_status_2024_2026.dbn")
for path in samples:
    store = db.DBNStore.from_file(path)
    frame = next(store.to_df(count=3, price_type="float", pretty_ts=True, map_symbols=True))
    columns = [c for c in ["symbol", "raw_symbol", "instrument_id", "ts_event", "action",
                           "price", "size", "bid_px_00", "ask_px_00", "instrument_class",
                           "asset", "min_price_increment"] if c in frame.columns]
    print("\n", path.name, "dataset=", store.metadata.dataset, "schema=", store.metadata.schema)
    print(frame[columns].to_string())
PY
```

同机预期文件数依次是 **70、70、1、1**。首日海力士代码为 `SKHYV`；首日成交样本的第一条价格为 `170.0`、数量为 `11,950,491`，这只是单条真实样本，不是全日统计。

`manifest.json` 记录导出文件大小和哈希；`metadata.json` 记录请求；`condition.json` 记录数据源日期可用性。`condition=available` 不保证选中的证券当天有成交。只检查文件存在或文件大小不足以发现所有损坏，应按上面代码重算哈希。

## 4. 需要新数据时：海力士与 CME 共用的完整下载代码

只需修改代码顶部的 `CASE`、`MODE`、`RUN_NAME`。四个预设：

| CASE | 默认请求 |
| --- | --- |
| `skhynix` | `SKHY/SKHYV` 的盘口和成交，2026-06-01 至 2026-10-17 |
| `cme-market` | `ESU6` 的盘口和成交，2026-09-01 13:30–13:35 UTC |
| `cme-definition` | 六品种定义，2026-09-01 全天 UTC |
| `cme-status` | 六品种状态，2026-09-01 全天 UTC |

`MODE="estimate"` 只查询并保存计划和估价；改为 `download` 才提交、下载或恢复任务；`verify` 离线复核该计划的文件。新日期、证券或 schema 使用新的 `RUN_NAME`。这些计划与项目原状态文件互不混用。

```bash
.venv/bin/python - <<'PY'
import hashlib
import json
import os
import time
from pathlib import Path
import databento as db
import pandas as pd
from dotenv import load_dotenv

CASE = "cme-market"
MODE = "estimate"             # estimate / download / verify
RUN_NAME = CASE               # 新区间或新证券使用新名字
CORE = ["ES.FUT", "NQ.FUT", "RTY.FUT", "YM.FUT", "GC.FUT", "CL.FUT"]
CASES = {
    "skhynix": dict(dataset="XNAS.ITCH", symbols=["SKHY", "SKHYV"], stype_in="raw_symbol",
                    schemas=["mbp-10", "trades"], start="2026-06-01", end="2026-10-17"),
    "cme-market": dict(dataset="GLBX.MDP3", symbols=["ESU6"], stype_in="raw_symbol",
                       schemas=["mbp-10", "trades"], start="2026-09-01T13:30:00Z", end="2026-09-01T13:35:00Z"),
    "cme-definition": dict(dataset="GLBX.MDP3", symbols=CORE, stype_in="parent",
                           schemas=["definition"], start="2026-09-01", end="2026-09-02"),
    "cme-status": dict(dataset="GLBX.MDP3", symbols=CORE, stype_in="parent",
                       schemas=["status"], start="2026-09-01", end="2026-09-02"),
}
root = Path(os.environ.get("DATABENTO_DATA_ROOT", ".")).resolve()
work = root / "databento_runs" / RUN_NAME
work.mkdir(parents=True, exist_ok=True)
state_path = work / "state.json"
request = dict(case=CASE, **CASES[CASE])

def utc(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")

def save(state):
    temp = state_path.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
    temp.replace(state_path)

def verify(directory, dataset, schema):
    manifest = json.loads((directory / "manifest.json").read_text())
    count = 0
    for item in manifest["files"]:
        path = directory / item["filename"]
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if path.stat().st_size != item["size"] or item["hash"] != "sha256:" + digest:
            raise RuntimeError(f"文件校验失败：{path}")
        if path.name.endswith((".dbn", ".dbn.zst")):
            meta = db.DBNStore.from_file(path).metadata
            if str(meta.dataset) != dataset or str(meta.schema) != schema or meta.not_found:
                raise RuntimeError(f"DBN 元数据异常：{path}")
            count += 1
    if count == 0:
        raise RuntimeError(f"没有 DBN 文件：{directory}")
    print(schema, "校验通过，DBN 文件数：", count)

state = json.loads(state_path.read_text()) if state_path.exists() else None
if MODE not in {"estimate", "download", "verify"}:
    raise ValueError("MODE 必须是 estimate、download 或 verify")
if state is not None and state["request"] != request:
    raise RuntimeError("参数与保存计划不一致，请使用新 RUN_NAME")
if MODE == "verify":
    if state is None:
        raise RuntimeError("尚无已保存计划")
    for schema in state["query"]["schemas"]:
        verify(work / state["jobs"][schema]["job_id"], state["query"]["dataset"], schema)
else:
    load_dotenv(root / ".env")
    client = db.Historical()
    if state is None:
        ranges = client.metadata.get_dataset_range(dataset=request["dataset"])["schema"]
        bounds = [pd.Timestamp.now(tz="UTC")]
        bounds += [utc(ranges[s]["end"]) for s in request["schemas"]]
        if request["end"] != "latest":
            bounds.append(utc(request["end"]))
        start, end = utc(request["start"]), min(bounds)
        if start >= end or any(start < utc(ranges[s]["start"]) for s in request["schemas"]):
            raise RuntimeError("请求区间无效或早于数据源可用起点")
        query = {k: request[k] for k in ["dataset", "symbols", "stype_in", "schemas"]}
        query.update(start=start.isoformat(), end=end.isoformat())
        state = dict(request=request, query=query, jobs={})
        if request["stype_in"] == "raw_symbol":
            mapping = client.symbology.resolve(
                dataset=query["dataset"], symbols=query["symbols"], stype_in="raw_symbol",
                stype_out="instrument_id", start_date=start.date().isoformat(),
                end_date=end.ceil("D").date().isoformat())
            if mapping.get("not_found"):
                raise RuntimeError(f"代码不存在：{mapping['not_found']}")
            state["symbology"] = mapping
        save(state)
    query = state["query"]
    common = {k: query[k] for k in ["dataset", "symbols", "stype_in", "start", "end"]}
    state["estimated_cost_usd"] = {
        s: float(client.metadata.get_cost(**common, schema=s)) for s in query["schemas"]}
    save(state)
    print(json.dumps(state, ensure_ascii=False, indent=2), flush=True)
    if MODE == "download":
        for schema in query["schemas"]:
            job = state["jobs"].get(schema)
            if job is None:
                result = client.batch.submit_job(**common, schema=schema, encoding="dbn",
                                                compression="zstd", split_duration="day")
                job = dict(job_id=str(result["id"]))
                state["jobs"][schema] = job
                save(state)
            target = work / job["job_id"]
            if job.get("downloaded"):
                try:
                    verify(target, query["dataset"], schema)
                except (OSError, ValueError, RuntimeError) as exc:
                    print("需重新下载原任务：", exc, flush=True)
                else:
                    continue
            while True:
                details = client.batch.get_job_details(job_id=job["job_id"])
                status = str(details["state"])
                print(schema, status, details.get("progress"), flush=True)
                if status == "done":
                    break
                if status not in {"received", "queued", "processing"}:
                    raise RuntimeError(f"任务状态异常：{status}，保留 job ID 后检查原因")
                time.sleep(15)
            client.batch.download(job_id=job["job_id"], output_dir=work)
            verify(target, query["dataset"], schema)
            job.update(downloaded=True, record_count=details.get("record_count"),
                       actual_cost_usd=details.get("cost_usd"))
            save(state)
    print("状态文件：", state_path)
    print("输出目录：", work)
PY
```

流程是：查询 schema 可用末端 → 核对具体代码映射 → 用相同参数估价 → 保存 job ID → 等待 `done` → 下载 → 校验。输出为 `databento_runs/<RUN_NAME>/state.json` 和各 job 子目录。

恢复时保持 `CASE/RUN_NAME/请求参数` 不变，再运行 `download`。已记录任务不会再次提交；已下载文件会重新核对哈希。不要并行运行同一个计划。若服务端提交成功而本地尚未保存 job ID 就中断，应先查回原 job，避免重复提交。

把配置中的 `end` 改成 `latest` 可在首次建计划时取共同可用末端；已保存计划仍固定原末端。补新日期时，用旧计划的排他结束时间作新计划起点，并使用新 `RUN_NAME`。`partial` 代码映射可能只是有效期不足整段，不等同于下载损坏。

## 5. 完整解析代码：DBN 分块转 Parquet，保留纳秒时间

下面完整转换一个 DBN 文件；将 `SOURCE` 改成实际文件即可。海力士日文件可逐个处理。CME `definition/status` 自动筛选六品种单腿；盘口和成交不做该筛选。输出不会覆盖已有文件。

```bash
.venv/bin/python - <<'PY'
import json
import os
from pathlib import Path
import databento as db
import databento_dbn as dbn
import pyarrow as pa
import pyarrow.parquet as pq

root = Path(os.environ.get("DATABENTO_DATA_ROOT", ".")).resolve()
SOURCE = root / "batch_downloads/XNAS-20261019-3Q7G8LJREK/xnas-itch-20260710.trades.dbn.zst"
name = SOURCE.name.removesuffix(".zst").removesuffix(".dbn")
OUTPUT = root / "parsed/handoff" / (name + ".parquet")
if OUTPUT.exists():
    raise FileExistsError(f"输出已存在，请使用新路径：{OUTPUT}")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
temp = OUTPUT.with_suffix(".parquet.tmp")
if temp.exists():
    raise FileExistsError(f"请先检查上次未完成的临时文件：{temp}")
store = db.DBNStore.from_file(SOURCE)
schema = str(store.metadata.schema)
is_cme = str(store.metadata.dataset) == "GLBX.MDP3"
core = ["ES", "NQ", "RTY", "YM", "GC", "CL"]
pattern = r"^(ES|NQ|RTY|YM|GC|CL)[FGHJKMNQUVXZ]\d{1,2}$"
writer = None
scanned = written = 0
try:
    for frame in store.to_df(count=100_000, price_type="float", pretty_ts=True, map_symbols=True):
        frame = frame.copy()
        frame["row_in_source"] = range(scanned, scanned + len(frame))
        scanned += len(frame)
        if is_cme and schema == "definition":
            frame = frame.loc[(frame["instrument_class"] == "F") & frame["asset"].isin(core)].copy()
            frame["product"] = frame["asset"]
        elif is_cme and schema == "status":
            product = frame["symbol"].astype("string").str.extract(pattern, expand=False)
            frame = frame.loc[product.notna()].copy()
            frame["product"] = product.loc[product.notna()].to_numpy()
        if frame.empty:
            continue
        if schema == "status":
            for column, enum_type in [("action", dbn.StatusAction), ("reason", dbn.StatusReason),
                                      ("trading_event", dbn.TradingEvent)]:
                names = {int(getattr(enum_type, key)): key for key in dir(enum_type) if key.isupper()}
                frame[column + "_name"] = frame[column].map(names).fillna("UNKNOWN")
            frame["is_midnight_snapshot"] = frame.index == frame.index.normalize()
        frame["source_file"] = SOURCE.name
        frame["ts_recv_ns"] = frame.index.as_unit("ns").asi8
        frame["ts_event_ns"] = frame["ts_event"].dt.as_unit("ns").astype("int64")
        table = pa.Table.from_pandas(frame, preserve_index=True)
        if writer is None:
            writer = pq.ParquetWriter(temp, table.schema, compression="zstd")
        writer.write_table(table)
        written += len(frame)
finally:
    if writer is not None:
        writer.close()
if written == 0:
    raise RuntimeError("没有输出记录，检查输入和筛选条件")
temp.replace(OUTPUT)
summary = dict(source=str(SOURCE), output=str(OUTPUT), dataset=str(store.metadata.dataset),
               schema=schema, input_rows=scanned, output_rows=written)
OUTPUT.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY
```

字段口径：

- `to_df(count=...)` 返回分块迭代器。`to_df().head()` 仍会先解析完整文件，大文件应按块处理。
- `map_symbols=True` 使用 DBN 内的历史映射补出 `symbol`，保留原始代码、ID 和日期；CME 定义的具体代码在 `raw_symbol` 中。
- `price_type="float"` 已按原始 `10^-9` 单位缩放，不能再除以 `1e9`。要原始定点值可改成 `fixed`。
- 十档盘口的 `_00` 是第一档，`_09` 是第十档；看盘口使用 `bid_px_* / ask_px_* / bid_sz_* / ask_sz_*`，不能把每行 `price/size` 都算作成交。缺失价位可能是 `NaN`。
- `ts_recv` 是接收时间，`ts_event` 是源事件时间；保留两者。额外的 `*_ns` 整数列用于精确排序，避免某些查询工具将带时区时间降为微秒。
- `source_file + row_in_source` 保留文件来源与原始顺序；同一时间戳可有多条有效记录，不能只按时间戳去重。源序号在单证券过滤后有间隔也不一定是丢包。
- `status` 的枚举数字通过 SDK 转成名称，同时保留原值。`is_midnight_snapshot` 沿用本项目对 UTC 接收零点快照的识别规则，原始时间仍应保留供复核。

## 6. CME 完整查询实例：历史有效定义与具体合约状态

下面独立使用已有的单腿 Parquet。若用第 4、5 节新下载和转换的数据，替换 `DEFINITION/STATUS`，并把查询日期、合约改到实际覆盖范围；新预设只有 2026-09-01 一天，不能用于默认的 5 月查询。

```bash
.venv/bin/python - <<'PY'
import os
from pathlib import Path
import pandas as pd

root = Path(os.environ.get("DATABENTO_DATA_ROOT", ".")).resolve()
DEFINITION = root / "parsed/core_six_definition_2020_present/definition_outrights.parquet"
STATUS = root / "parsed/core_six_status_2020_present/status_outrights.parquet"
PRODUCT = "ES"
CONTRACT = "ESM6"                 # 使用 DBN 中的原始代码，不自行改成 ESM26
AS_OF = pd.Timestamp("2026-05-01T12:00:00Z")
DAY = pd.Timestamp("2026-05-01T00:00:00Z")
OUT = root / "parsed/handoff_query_es_20260501"
if OUT.exists():
    raise FileExistsError(f"输出目录已存在，请使用新目录：{OUT}")

columns = ["raw_symbol", "asset", "instrument_class", "instrument_id", "ts_event",
           "security_update_action", "activation", "expiration", "min_price_increment",
           "unit_of_measure_qty", "currency"]
definitions = pd.read_parquet(DEFINITION, columns=columns).reset_index()
known = definitions.loc[(definitions["asset"] == PRODUCT) &
                        (definitions["instrument_class"] == "F") & (definitions["ts_recv"] <= AS_OF)]
latest = known.sort_values(["ts_recv", "ts_event", "instrument_id"], kind="stable").drop_duplicates(
    "raw_symbol", keep="last")
active = latest.loc[(latest["security_update_action"] != "D") &
                    (latest["activation"] <= AS_OF) & (latest["expiration"] > AS_OF)].copy()
active = active.sort_values(["expiration", "raw_symbol"])

columns = ["product", "symbol", "instrument_id", "ts_event", "action", "reason", "trading_event",
           "action_name", "reason_name", "trading_event_name", "is_trading", "is_quoting"]
statuses = pd.read_parquet(STATUS, columns=columns).reset_index()
events = statuses.loc[(statuses["product"] == PRODUCT) & (statuses["symbol"] == CONTRACT) &
                      (statuses["ts_recv"] >= DAY) & (statuses["ts_recv"] < DAY + pd.Timedelta(days=1))].copy()
events = events.sort_values("ts_recv", kind="stable")
events["is_midnight_snapshot"] = events["ts_recv"] == events["ts_recv"].dt.normalize()
if active.empty or events.empty:
    raise RuntimeError("结果为空，请核对文件覆盖、原始合约代码和查询时点")

OUT.mkdir(parents=True)
active.to_csv(OUT / "definition_asof.csv", index=False)
events.to_csv(OUT / "status_timeline.csv", index=False)
print("有效 ES 定义数量：", len(active))
print(active[["raw_symbol", "instrument_id", "expiration", "min_price_increment", "unit_of_measure_qty"]].head(5).to_string(index=False))
print("ESM6 当日状态记录数：", len(events))
print(events[["ts_recv", "ts_event", "action_name", "trading_event_name", "is_midnight_snapshot"]].to_string(index=False))
print("输出目录：", OUT)
PY
```

同机实际验证结果：

- 2026-05-01 12:00 UTC 有 **21** 个有效 ES 合约定义。`ESM6` 的 `instrument_id=42140864`，最小价格变动 `0.25`，`unit_of_measure_qty=50.0`，该例一跳价值为 12.5 美元。
- `ESM6` 在 2026-05-01 有 **2** 条状态记录：UTC 零点的 `TRADING` 快照，以及 `2026-05-01T21:00:00.002371691Z` 收到的 `CLOSE`，后者 `ts_event=21:00:00Z`。
- 零点快照的源事件来自前一天，表示带入状态，不能算成当天刚发生的一次开盘。

定义查询必须先按接收时间选择“当时已知的最后版本”，再处理删除和激活/到期条件。不能把未来更新提前用于过去，也不能只取 `ts_event` 最新值。这里的原始合约代码包含一位年份，长期跨十年数据应结合定义里的到期日和合约身份进一步消歧。

状态查询应同时保留 `action/reason/trading_event`。例如 `NO_CANCEL` 记录可能重复携带当前阶段，不能将每行都当成新开盘；`NONE` 也不能直接解释成收盘。官方交易日历与盘中状态事件应分别处理。

## 7. 交付给下一个 agent 时的验收标准

1. 说明用了现有文件还是新批次，列出 dataset、symbols、stype、schema 和精确起止时间。
2. 新批次保存请求参数、估价、job ID 和实际费用；恢复运行继续使用已保存任务。
3. DBN 文件能读取，manifest 大小和 SHA-256 一致；日期缺文件要结合证券有效期和交易日判断。
4. CME 单腿筛选、定义版本和状态事件口径明确；保留原始代码、ID、时间及可追溯来源。
5. 输出明确的 Parquet/CSV 路径、输入与输出行数；对默认 CME 查询复现上面的 21 个定义、2 条状态结果。

本文已验证：四个原始批次的 **142 个 DBN 文件**通过 manifest 与头信息核验；海力士/CME 本地读取、跨块转换和上述 CME 查询可运行。联网下载示例本次未重新提交收费任务。
