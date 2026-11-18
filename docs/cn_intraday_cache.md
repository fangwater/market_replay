# 国内期货盘中分钟缓存

`cn-minute-cache` 是 Market Replay 托管的独立 Rust 进程。它订阅公司 ZMQ 的六条单腿期货行情频道，按交易所、原生合约和原生交易日处理累计成交与五档行情。已闭合的 `baseline_1min` 使用 Protobuf 按分钟写进 NVMe RocksDB，当前分钟的原始行情保存在进程内存。时段、乘数、tick 和分桶阈值与 `GetMinuteBars` 共用来源。

Market Replay 通过只读 RocksDB secondary 读取缓存。常规查询直接走 `GetMinuteBars`。盘中 MDL 数据优先，缓存用最后一个已闭合且逐字段相同的分钟接上后续数据。有完整开盘覆盖时，缓存也能提供这个交易日的连续前缀。跨休市按参考交易时段检查。断线换代或缺失盘中分钟会停止向后补齐，等 MDL 覆盖后再建立接续点。

同一合约的重叠闭合分钟逐字段比较。服务日志的 `minute_cache_comparison` 记录相符数、差异数、单边缺失数和首次差异时间。

配置在 `config/cn_kline_server.toml` 的 `[minute_cache]`。RocksDB 只保留一个原生交易日，自然日零点按交易所 `TradDay` 归属。盘后 `cn-daily` 确认该日归档后，采集进程在下一轮维护删除缓存并回收空间。下一交易日开始写入时，旧交易日退出可读范围。

旧版 `[intraday]` 配置对应上期所 SN 原始帧回放服务。启用该配置的环境可继续使用 `StreamIntradayFrames`。
