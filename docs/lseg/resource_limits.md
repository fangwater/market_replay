# 资源预算

2026-11-16 起，`ly_cme_run` 监控器、NVMe 资源锁和 `libly_cme_cpu_budget.so`（LD_PRELOAD）都已退役。原文见 git 历史和 [agents_history.md](agents_history.md)。

现行做法（总预算 64 GiB 内存、32 个线程）：

- 每个工具显式固定自己的线程池，不随机器规模扩张：
  - 聚合工作单元最多 2 个（`lseg_delivery::limits::AGGREGATION_WORKERS`）；
  - Parquet 发布固定 8 个线程（`limits::IO_THREADS`）；
  - 导出工具用 `--workers` 设定线程数，默认值较小。
- TAS RocksDB 一律只读，通过 `lseg_delivery::tasdb::open_read_only` 打开：后台线程 1 个，打开文件线程 1 个，block cache 共享且有上限。
- Python 工具里 DuckDB、Arrow、BLAS 的线程数显式设为 1。
- 读取、解析和写入都是流式、有界的。
- 独立的大任务不并发运行：每日任务由 PM2 串行调度，一次性回补用前台命令。
