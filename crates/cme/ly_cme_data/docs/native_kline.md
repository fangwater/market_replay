# Rust 行情整理工具

入口为 `lseg-kline`（Cargo bin 名称 `lseg_kline`）。生产处理全程在Rust内完成。

```text
RocksDB iterator → 当前RIC的分钟聚合／状态机 → 按日Parquet writer
```

聚合保留2个工作单元，Parquet发布使用固定8线程、有界16项队列；每RIC最多16项待确认。
状态仍8192行流式处理，逐日关闭时提交有限的writer；K线按日提交固定1440行内批次。
源迭代不再逐文件等待发布，同步落盘、footer、原子不覆盖发布及抽样读回均在确认范围内。
所有文件确认后才写RIC断点，发布失败阻止完成；普通进程退出时等待发布线程结束。

状态直接按源发布时间和源顺序解析，8192行一批写出。价格使用有界的当前RIC分钟桶，
以交易所时间归桶，保存迟报和零量成交；完整RIC处理完后按连续UTC分钟补价。
已有稀疏分钟Parquet经源身份、CURRENT和footer核对后复用，未完成源直接读RocksDB。
原来的市场消息JSON和Python接续流程已停用。少量JSON仅用于现有输入清单和本地断点。
控制清单以64 KiB缓冲读取，避免按字节发起文件读取；数据文件复用写入描述符完成同步。
新临时文件使用独占创建的读写句柄，先同步，再复用该句柄核对footer，减少NFS路径重新打开；
正式年份／RIC／UTC日目录同时核对每个row group的RIC和时间最小／最大值及空值数，
使用同一次footer读取，不增加数据页读回；同schema／行数的错误RIC或日期仍须报错，保留已有文件。
随后以硬链接发布同一inode，不重复读取同一footer；
已有目标及发布竞争仍单独核对目标footer，首／中／末日内容抽样保留。
同一RIC的日期目录只列举一次，在状态和分钟写入间共享，缓存最多400项；
已知日期不重复mkdir／stat，目录被外部删除时会重新创建。数据文件仍逐个同步和原子发布。
当前NFS不支持`renameat2(RENAME_NOREPLACE)`（实测EINVAL），不能用会覆盖目标的普通rename替代硬链接发布。

开发时须保留正式目标的逻辑身份：将`--output`直接改为NVMe临时目录会进入验证口径，
不会复用正式盘237个已改撤RIC。新增传输方式应区分临时文件位置与正式目标，继续保护已有净额结果。

## 使用

构建产物位于NVMe，构建命令：

```bash
bash scripts/build.sh --offline --jobs 1 --bin lseg_kline
```

全量续跑（普通前台程序）：

```bash
lseg-kline run --workers 2
```

`run`会在每个分钟分组完成后，接着补交仅有状态／改撤、没有当年实际成交的RIC。
也可对已有完整分钟分组单独补交：

```bash
lseg-kline status-only --workers 2
```

此步按kind=12／13的RIC前缀跳转发现工具，核对主任务的源身份及合约断点，
再次确认该RIC在目标UTC年没有实际成交分钟后，才直接写状态Parquet。
没有价格依据的RIC不生成K线；孤立改撤只列 `source_witness_pending`。
状态从UNKNOWN开始，等待本年明确阶段，不凭同名RIC猜测上一年合约身份。
规划同时核对kind=12／13每个RIC前缀的首末时间，纳入只有控制消息的跨UTC年分组；
该核查仅跳转首末键，范围缓存在本地NVMe，不全扫或缓存状态消息。
`run`和独立`status-only`共用同一分组计划；新增无成交年份只交状态和问题，不制造K线。
问题位于 `meta/problems/native_kline/status_only/`，包括 `no_trade_for_kline`。
补交有独立的status_only_groups／status_only_contracts断点；独立命令另写
`checkpoints/status_only_state.json`。旧版本的主任务完成不能证明补交完成，
新版`run`只有两部分都完成才将分组计为完成。进度中的`status_only_rics`为没有K线的RIC数。
兼容旧HG试跑在分组回执内嵌合约列表、没有独立contracts目录的格式；
复用前核对源签名、RIC唯一性及笔数／成交量／分钟数／日文件数的合计，不能把格式差异当成缺失行情。

限定范围或写入独立目录：

```bash
lseg-kline run --year 2026 --product HG --ric HGN26 --work /mnt/nvme-raid0-28t/fanghaizhou/lseg_data/check_hg --output /mnt/nvme-raid0-28t/fanghaizhou/lseg_data/check_hg_output
```

源状态的限量验证只写指定临时目录，不生成源完成断点：

```bash
lseg-kline check --job main__p%3A2018%3AGC --ric GCM8 --messages 100000 --cme --output /mnt/nvme-raid0-28t/fanghaizhou/lseg_data/check_status
```

比较两份Parquet，或两个RIC日目录的首／中／末日：

```bash
lseg-kline compare --left <新RIC目录> --right <既有RIC目录>
```

核对源计划与所列数据库当前全部品种列族（不扫描tick）：

```bash
lseg-kline check-plan
```

全量`run`启动时也执行此检查；单独核查须与数据任务按序运行，共用程序资源预算。
该检查只证明源范围，不能证明Parquet交付完成。

## 口径和断点

默认输出仍是 `lseg/<exchange>/<year>/<RIC>/<YYYYMMDD>/kline.parquet` 和
`session_status.parquet`。K线保留10列、Float64价格、自身收盘补价和五类分钟状态。
详细状态保留原Qualifiers、原发布纳秒和重复消息，不设置静态日历或交易所时间回拨。
交易所、合约有效期及缺包来自既有LSEG参考和源清单；CME原生阶段仅解释已核定CME集团市场。

已完成的旧分组按源身份复用，原237个已核定改撤的2026 RIC核对gross量／笔数后保持原结果。
其它未取得可信关联的kind=12只写 `source_witness_pending` 问题，保留原成交打印。
Trade、Quote、生产RocksDB和IB保持只读。既有交付文件按footer和抽样内容核对后复用，
字段或抽样内容冲突时报错，避免覆盖可信历史结果。

新断点为NVMe `lseg_native_klines_20261112/checkpoints/state.json` 及其groups／contracts。
分组总数取决于实际成交及控制消息的UTC年份，不能固定沿用旧计划的2,214。
旧的1,733/2,339计数是中间提取阶段；新进度按最终处理分组、RIC和日文件统计。
整体完成必须同时核对实际退出码、失败项和全源gross笔数／量守恒。
正常成功不在交付盘写audit或报告，真实问题位于 `lseg/meta/problems/native_kline/`。

默认2个工作线程；显式线程池与缓冲均有预算。程序总预算64GiB／32线程，
运行由普通Rust父进程监测，约60GiB提前停止；这仍是程序级采样保护。
