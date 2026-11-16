# 2026 补充 TAS 原始包下载

2026-11-03 已按用户要求下载 GOIL / NKD / TSR20 所在的补充 TAS 合并包，范围为 `2026-01-01_2026-05-30`。本页记录原始文件下载；后续已于 2026-11-03 完成独立 RocksDB 入库与验收，见 [补充 TAS 入库](tas_supplement_rocksdb.md)。此后已 [合并进原库](tas_supplement_merge.md)。2026-11-09 已将新增 13 个列族的 Parquet 按交易所交付最终盘，排除 5 个完全重复列族；128 个 RIC、232 个文件，详见 [本轮 TAS 交付](tas_daily_increment_20261109.md)。补充源窗口为 `[2026-01-01,2026-05-30)`，不含 5 月 30–31 日。

## 本机位置

```text
/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_supplement/normalised/
  futures_11_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30/
    merged-Data.csv.gz
    merged-Report.csv.gz
    Futures 11 Time and Sales RIC LIST 0 TAS 2026-01-01 2026-05-30.csv.gz.notes.txt
    download_manifest.json
```

三个目标产品同属 `futures_11` 合并包；原始文件同时含其它补充产品，未按 RIC 拆分或修改源内容。本次没有下载 2010–2025 年包。

## 完成与校验

| 文件 | 字节 |
| --- | ---: |
| `merged-Data.csv.gz` | 14,980,973,366 |
| `merged-Report.csv.gz` | 6,435 |
| 原始 Notes | 97,295 |
| 合计 | 14,981,077,096，约 15.0 GB |

2026-11-03 03:57:34 UTC 完成验证：本地大小与源 Content-Length 相同，下载前后 ETag 不变，Data 的 MD5 与供应商 Notes 的 Extraction MD5 相同。

```text
Data MD5:    9449a8b6f5d1eeeabdebab5ffa1af18c
Data SHA256: 6923ae060c77ae117b429d6d3da000d6df937da81ae6f56d8322963b682951a2
```

Data 下载到 `.partial` 后经校验才改为正式文件名；无残留 `.partial` 或 aria2 控制文件。Report 已通过 gzip 解码并检查目标根的非零 Count；未把下载校验当作逐行清洗或源数据业务验收。

| IB 品种 | 正确 TAS 根 | Report 活跃 RIC | 源事件 Count |
| --- | --- | ---: | ---: |
| GOIL | LGO | 84 | 318,093,502 |
| NKD | NK | 16 | 88,865,994 |
| TSR20 | STF | 18 | 54,178,487 |

全包 Report 有 318 个活跃 RIC。Count 含各种 TAS 事件，不是成交数，三个产品的身份与误配排除见 [原始 TAS 映射](ib_all_futures_raw_tas.md)。

源目录为 `http://10.61.20.12/public/users/u158/public/lseg_data/future_supplement/normalised/futures_11_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30/`。下载使用 aria2 八连接，源站只读。

完成依据为原始包目录的 `download_manifest.json`，其状态为 `download_complete_verified`。项目内另存 [完成回执](../artifacts/tas_supplement_download_20261103/completion.json)、[aria2 日志](../artifacts/tas_supplement_download_20261103/aria2.log)和[验证日志](../artifacts/tas_supplement_download_20261103/verification.log)。

复核命令：

```bash
python scripts/verify_tas_supplement_download.py \
  /mnt/hdd-raid5-72t/liang_torch/lseg_data/future_supplement/normalised/futures_11_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30
```
