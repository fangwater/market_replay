# 2026 年 6 月起 TAS 日包增量

当前对外交付采用简短 `lseg/README.md` 作为读取入口，详细资料集中在 `lseg/meta/supply.md`。所有回执及清单已分别收至 `lseg/meta/audit/`、`lseg/meta/catalog/`；下文此前的根_audit／_catalog路径为历史阶段记录，后台最终汇总现在仅更新meta内排错补充说明。

状态：2026-11-09 13:44 UTC完成本批120个品种可用源的清洗、按RIC／日交付及用户指定的抽样验收。源44,207,088,619行，Trade 620,500,624行、Quote 2,542,040,264行，229,005日文件／57,864,849,646字节。清单18,987个文件名、精确压缩大小827,752,630,393字节，下载失败为零。1,225个零字节占位和NQ缺日仍是源缺口；可用源完成不代表缺口补齐。完成依据为项目及目标`lseg/_audit/tas_daily_20261109/`下completion.json与verification.json，最终服务成功退出。

用户要求的统一对外说明已整理到最终盘 `lseg/README.md`，配套Excel及CSV集中在 `lseg/_catalog/`。后台最终汇总也只更新此说明及LSEG内清单；原根README和根_catalog已经移入LSEG目录。README整合记录见 `artifacts/lseg_readme_consolidation_20261109/completion.json`。

## 用户最终指定的文件标准

用户后续要求降低重复校验：当前默认 `--readback sampled`。每个品种列族、每类 trade／quote 按 UTC 日期／RIC／路径排序，选首／中／末最多3个文件做完整内容读回；所有文件只做大小、footer、schema、条数、RIC及UTC日期范围检查。目标盘仅同一组样本核对完整文件 SHA256，其他文件的 `sha256=null`，并写明 `target_check=footer_schema_rows_ric_date_and_size`。最终批次汇总复用这些复制记录，不再全量重复读取目标文件内容或 footer。已经执行的全量校验结果保留，正在导出的任务接续处理，不为切换策略重跑。此处覆盖下文历史的“所有文件全值读回／全文件SHA256”要求；源读取与已有清洗口径不变。

```text
lseg/<交易所小写>/<年份>/<原始RIC>/<YYYYMMDD>/trade.parquet
lseg/<交易所小写>/<年份>/<原始RIC>/<YYYYMMDD>/quote.parquet
```

例如 `lseg/cme/2026/ESH27/20260604/trade.parquet`。日期按记录 `ts_utc_ns` 的 UTC 自然日；RIC 保留原始大小写，路径保留必要的可逆转义。没有 `tas/` 层、批次前缀或 `part-*` 文件名；每个 RIC／日期／类型一个文件，无数据不制造空文件。

直接以只读方式扫描已清洗 RocksDB，使用导出器 `--layout ric-day --exchange-map <已核定分组JSON>`；不通过旧跨日 Parquet 分片重组。按原始 key 顺序保留同日全部行及重复记录，不限制单日文件总行数。Quote 的 key 顺序与 value 发布时间可能涉及不同日期，使用独立日期 writer，不能假定日期单调而丢弃或覆盖再次出现的某天。

新布局通过了真实 RocksDB 的跨 UTC 午夜、重复成交、日期交错及单日超出旧分片阈值验证，4 项 Rust 测试通过；ES／LGO 真实样本共 38,093 行、5 个日文件，Rust 全值 SHA256、独立 Python 逐值读回及与原已验证样本按日分组的字节／重复次数对照均通过。回执为 `artifacts/tas_daily_ingest_20261109/ric_day_validation/completion.json`。

已完成清洗的品种复用原 DB 和审计，仅重新导出；全部新日文件验证及目标复制哈希通过后，才移除该品种旧布局的目标文件。本机原导出保留用于回退；历史目标回执归档于 `lseg/_audit/_before_ric_day_20261109/`。新完成回执记录 `layout=ric_utc_day_v1`、`export_source=read-only RocksDB`。

## 远端发现

本轮依据 preprocess 的 LSEG 实际源入口，只读检查 `http://10.61.20.12/public/users/u158/public/lseg_data/`。现有 NFS 中的 Databento 是不同数据源，不将其作为 LSEG TAS。

- 原 `future/normalised` 和 `future_supplement/normalised` 共 34 个年度 Report 已重新获取，与本机已有 Report 的对应副本一致。2026 补充批次此前已入库，本轮不重复清洗、合并。
- 新纳入 `future_daily/normalised`：120 个品种目录、18,987 个正式 `.csv.gz` 文件名，目录展示约 822.3 GB，日期从 2026-06-01 起，各品种截止日不同：44 个到 11-07，48 个到 11-06，27 个到 11-01，1 个到 11-05。
- 104 个 `.part` 条目不纳入。NQ 2026-11-04 无正式日包；不补造该日记录。
- CIPM 的部分正式文件名实际对应远端零字节占位，不能称为有效 gzip 或已确认无成交，单列源缺口。有效 gzip 中只有表头或内容为空的情况另行验证并记零源行，不制造空行情。
- `future_crude/normalised` 为 7 个油品／油脂的按日、按 RIC、30 列 CSV 视图，抽查 `CL/20260601/CLN26.csv` 已确认其与 293 列新日包的字段形态不同。本轮用完整字段日包作为接续来源，不将其与日包叠加成另一份独立成交。
- `future_crude_derived` 是指数、主力、近远月等衍生序列，不作为 TAS 原始成交输入。

目录快照与清单位于 `artifacts/tas_remote_refresh_20261109`，其中 `inventory.json`、`daily_tas_files.csv`、`daily_tas_products.csv` 保留本轮发现范围。未重新发现的历史补充包不冒充本轮新发布数据；原始年度清洗仍以既有 period 水位为准。

## 输入适配与验证

日包表头是 293 列：相对旧 294 列模板新增 `Alias Underlying RIC`，不含 `Vega`／`Rho`。项目专用 `config/tas_daily_column_rules.json` 明确认识新字段为可选身份参考信息，保留原始 `#RIC`，不使用别名字段改写或合并身份。为兼容既有固定 294 项规则目录校验，用该字段替换此日包模板不出现的 `Vega` 条目；未出现的旧 `Rho` 仍可识别。其它规则不变，未知字段仍报错。

每个日包都有独立表头，而原年度分包仅 part 0 有表头。新增 `tas_daily_stage` 仅完成格式适配：完整解压并验证源 gzip、读回压缩源 SHA256 与字节数、每文件只移除首行表头、逐字节复制其余数据行到 zstd、为每个源文件分配独立 part。使用既有 `TasShardManifest`，之后直接复用原 `cme_tas_replay_all`，不复制清洗实现。零字节占位不尝试 gzip 解码，并在高层完成回执中明确标记源缺口。

验证已通过：

- ES／GC／LGO 各 200,000 源行，对照独立 Python 的成交／Special 和 1 秒 BBO 字节及重复次数；黄金样本为 Quote，不将其描述为已单独核验黄金 Trade。
- 两个各 100,000 行的独立带表头文件，加一个仅表头空包，经适配、重放、完整 key/value 审计后，与单表头组合输入的独立 Python 基线一致，未解析为零。
- 适配验证输出 5 个 Parquet、38,093 行，全部列、全部行按原 TAS value SHA256 读回，Python footer／schema／文件集合核对通过。新增 `--period-start` 与按实际源周期审计已用于此验证。

验证回执：`artifacts/tas_daily_ingest_20261109/samples_v2/completion.json` 和 `adapter_validation/completion.json`。失败的早期适配尝试保留为诊断，不能作为完成结果。

## 实际处理和位置

下载器保留原始文件名，逐文件记录 URL、精确字节数、ETag、Last-Modified、SHA256；下载前后验证远端指纹。完整 gzip 校验在适配阶段完成，零字节占位明确不具有 CRC 校验结论。

2026-11-09 排查发现早期下载继承了本机 `127.0.0.1:10810` HTTP 代理，旧 `no_proxy` 未含 `10.61.20.12`。现已将目录盘点和下载器的全部 HTTP 请求显式使用 `ProxyHandler({})`，强制直连内网源，不依赖代理环境变量或 `no_proxy`。已在代理变量指向不可用端口的环境中验证 HEAD 和 GET 均成功；已完成源文件保留，重启下载器继续处理剩余文件。

用户随后要求统一排除整个 `10.61` 网段，已在 `/home/fanghaizhou/.bashrc` 的 `no_proxy`／`NO_PROXY` 中加入 `10.61.0.0/16`，保留旧排除项，并为不支持 CIDR 的 Python `urllib` 添加已用服务 `10.61.20.11/.12/.13/.64` 的完整 IP 条目。已在仅设置该 CIDR、代理指向不可用端口的环境中验证 curl 和 requests 直连源站成功；`urllib` 不支持 CIDR，不能把这项配置描述为对所有客户端通用。新 shell 需加载更新后的 `.bashrc`，已启动进程不会自动更新环境；本轮 TAS 下载仍以脚本强制直连为准，不依赖环境更新。

关于代理流量：经过本机 `10810` 入口不等于经过外部代理节点。进一步核对本机 Xray 的实际配置，私网地址命中 `geoip:private → direct`；切换前已观察到 Xray 直接连接 `172.16.30.38 → 10.61.20.12:80`。05:31 UTC 通过本机代理做 CONNECT／HEAD 小请求，也再次观察到该直接出站。切换前最后一条下载进度约 279.8 GB，含缓存复用，不能据此算作外部代理计费流量。未启用逐连接历史字节统计，不能给出历史账单侧精确数字。路由、配置哈希及连接证据见 `artifacts/tas_daily_ingest_20261109/proxy_routing_audit.json`；本次没有修改 Xray 配置。

| 内容 | 位置 |
| --- | --- |
| 原始日包 | HDD `lseg_data/future_daily/normalised/<品种>/<品种>_<日期>_tick.csv.gz` |
| 适配分片 | NVMe `lseg_data/tas_daily_shards_20261109/<品种>/` |
| 独立清洗库 | NVMe `lseg_data/cme_tas_daily_20261109/<品种>/attempt_N` |
| 已读回日文件 | HDD `lseg_data/tas_tick_parquet/daily_20261109/<品种>/ric_day_attempt_N/lseg/<交易所小写>/2026/<RIC>/<YYYYMMDD>/<trade或quote>.parquet` |
| 最终文件 | NFS `CTA_Data/lseg/<交易所小写>/2026/<RIC>/<YYYYMMDD>/<trade或quote>.parquet` |
| 本机过程及回执 | 项目 `artifacts/tas_daily_ingest_20261109/` |
| 最终盘过程及回执 | `CTA_Data/lseg/_audit/tas_daily_20261109/` |

结束日期为半开边界，按该品种实际最后可用日次日确定。各品种独立落库并提供完成回执；新 quote 仍为既有 Exch Time 的 1 秒 BBO，不再次采样，不补缺秒。所有源事件类型继续按既有协议处理，Parquet 仅导出已有 kind 1／3。

清洗结果仍核对源与分片／重放行数，要求未解析和未匹配为零，并复用已有库审计。Parquet 使用上文抽样内容读回及完整轻量元数据检查；复制目标仅抽样完整文件SHA256。交易所按实际参考信息分配，冲突或无法核定的 RIC 进入 `REVIEW`。跨源品种出现同一原始 RIC 时阻断发布，不能不经核查直接相加。

旧全品种生产库、已验收的 1–5 月 Parquet 及目标根原有 `2026/` 不改写。当前独立增量不代表已完成与原库的合并；不同批次交界的 Quote 应按源批次与既有秒桶合并规则核对，不能据文件日期盲目声称所有批次拼接后已全局唯一。

## 入口与进度

2026-11-09 约10:00 UTC检查时，之前的执行进程已经退出，最后处理进度停在08:00 UTC，未生成整批完成回执；没有日志证明具体退出原因。现已改由用户级systemd服务在后台持续接续，避免将工具会话的存在当作作业存活依据。日包服务为 `ly-cme-tas-daily-20261109.service`，补充库服务为 `ly-cme-tas-supplement-20261109.service`；启动记录在 `artifacts/tas_daily_ingest_20261109/persistent_services.json`。GC／HO／RB 已有完整新布局本机导出，优先接续发布；其它品种继续复用既有 RocksDB，不重新下载。最终完成仍必须以完成及验证回执为准。

```bash
systemctl --user show ly-cme-tas-daily-20261109.service ly-cme-tas-supplement-20261109.service --property=Id,ActiveState,SubState,MainPID,Result
```

服务输出日志分别为 `artifacts/tas_daily_ingest_20261109/process_systemd.log` 和 `artifacts/tas_supplement_parquet_20261109/systemd_delivery.log`；任务数据和回执的路径不变。

```bash
python scripts/inventory_tas_remote.py
python scripts/download_tas_daily.py --workers 8
python scripts/run_tas_daily_delivery.py --process-workers 4 --replay-workers 8 --parse-workers 8 --large-workers 16 --export-workers 2
```

下载和处理可分别运行，后者等待对应品种下载完成。已完成的品种可复用；已通过的分片、重放、审计和导出有恢复检查，失败产物保留为独立尝试，不覆盖已验收文件。`download_progress.json` 为下载进度，`process_progress.json` 为清洗及发布进度；正式文件存在不代表整个快照交付完成。

新增实现和适配均维护在本项目，Cargo 构建产物仍在项目专用 NVMe target，未修改参考项目或 preprocess 的生产规则文件。

## 本轮 1–5 月补充 Parquet 的历史交付与新布局重导出

以下232文件是旧布局历史统计。新标准重导出已经完成：只读独立补充RocksDB的同一批13个列族，13376日文件／2,737,863,884字节／128 RIC，Trade与Quote行数保持不变；新回执含`layout=ric_utc_day_v1`。当前文件为`lseg/<交易所小写>/2026/<RIC>/<UTC年月日>/trade.parquet`或`quote.parquet`，当前完成回执为`lseg/_audit/tas_supplement_2026_parquet_20261109/completion.json`。旧回执归档保留。

此前已入库的补充 TAS 于 2026-11-09 04:31:32 UTC 完成单独 Parquet 交付。只读独立补充库，仅选择此前合并新增的 13 个列族：`AFCM/AFCQ/AFCS/AFCY/CIPM/HTE/KWR/LGO/LGOTOT/NK/SEKA/SSG/STF`，排除与原有数据完全一致的 `ATWQ/ATWS/ATWY/LSU/LSUTOT` 五个列族，不重复叠加。

- 128 个 RIC、232 个文件、2,733,379,422 字节。
- Trade 30,061,381 行／103 个 RIC；Quote 127,098,723 行／120 个 RIC。
- 源窗口 `[2026-01-01,2026-05-30)`，不含 5 月 30–31 日；这两天不能因后续日包从 6 月 1 日开始而自动视为覆盖。
- 已通过全列全行 value SHA256 读回、Python footer 核对及最终盘逐文件 SHA256 核对。
- 最终文件按交易所写入 `CTA_Data/LSEG/<交易所>/tas/<trade或quote>/<RIC>/2026/`，文件名前缀为 `supp-futures11-20260101-20260530-`。

项目完成回执为 `artifacts/tas_supplement_parquet_20261109/completion.json`；最终盘完成回执为 `LSEG/_audit/tas_supplement_2026_parquet_20261109/completion.json`。这份已完成回执与仍在处理的 6 月起日包分开，不覆盖旧 1–5 月 Parquet 快照。
