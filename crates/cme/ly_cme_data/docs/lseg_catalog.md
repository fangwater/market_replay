# LSEG SQLite 合约索引

交付入口为 `lseg/meta/catalog.sqlite`。行情仍在每日 Parquet；SQLite 保存品种、RIC、
行情首末时间、到期状态及目录，方便不指定日期查询某个品种。它不是成功回执或处理日志。

```sql
-- 当前数据快照中仍活跃的日元期货合约。
SELECT ric, last_trade_utc, last_quote_utc, as_of_utc_date, data_directory
FROM active_contracts WHERE product = 'JY';

-- 全部历史合约；source_year 保留历史周期身份，不合并同名的一位年份 RIC。
SELECT source_year, ric, last_session_state, expired_at_utc
FROM contracts WHERE product = 'JY' ORDER BY source_year, ric;

-- 不选择日期，查某个品种的所有行情目录；在目录内读取所需的每日 Parquet。
SELECT DISTINCT directory FROM directories WHERE product = 'JY';
```

`active_contracts` 是各品种最新源年份、最新实际 Trade／Quote UTC 日仍有行情，
且没有终态到期证据的 RIC。`as_of_utc_date` 是该品种的数据截至日，不表示今天的实时状态。
无成交但有报价可以入选，只有状态不算行情活跃；未入选的合约仍在 `contracts` 中。
休市日末状态可能是 CLOSED／PRE_OPEN／HALTED，不能据此判断合约已失效。

Trade 首末时间来自 RocksDB 的源发布时间端点，Quote 来自已采样的 UTC 秒桶；
它们用于覆盖／活跃索引，不冒充逐笔扫描得到的最晚交易所成交时刻。全部时间为 UTC。
`conventions` 表保存这些定义；`directories` 中的路径指向当前交付目录，
没有历史 Trade／Quote Parquet 不等于只读 RocksDB 中没有原消息。

Rust 命令（普通 bin，受项目统一资源预算保护）：

```bash
lseg-catalog build --prune-expired-status
lseg-catalog list --product JY --active
lseg-catalog list --ric JYQ26 --year 2026
```

索引通过 RocksDB 每种消息／RIC 前缀的首末键查询，不重新遍历所有 tick。
单个工作单元、最多同时打开64个品种年CF、共享64 MiB RocksDB缓存、SQLite 16 MiB页缓存，状态按批次读取。
索引在本机 NVMe 构建，事务完成并检查 SQLite 完整性后，以单文件原子替换交付盘版本；
交付文件不使用 WAL，也不创建服务、定时任务或成功审计。

`--prune-expired-status` 只清理明确 `EXPIRE` **之后的 UTC 日期**，且须满足：
该日目录只有 `status.parquet`／`session_status.parquet`；标准状态每个 row group
的 footer 均证明全为 `EXPIRED/NO_CHANGE`，RIC／UTC 日期一致且无空值。
缺少证明或有 Trade／Quote／K线／其它文件时保留。到期当天、停牌及未知状态保留；
只读 RocksDB 原消息不改写。刷新索引时可以从保留的到期消息恢复终态。
