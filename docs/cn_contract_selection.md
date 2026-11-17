# 国内期货合约选择 API

回测只通过服务接口获取选约、权威乘数和原始行情。客户端不读取 `roll/result`、NFS、分片缓存或 PostgreSQL；本地只读用户仓位文件、显式冻结的运行元数据，并写自己的运行结果。

## 选择口径

gRPC 服务：`cn_market.v1.CnFuturesMarket`，默认 `:18312`。新增 `GetContractSelections`，可用于回测和交易端。原有主力 HTTP 接口保持原口径。

| 模式 / 回测配置 | 当日有效合约 |
| --- | --- |
| `DOMINANT` / `dominant` | 已发布的当日主力，沿用 `local_rqdata_rule0` |
| `SECONDARY_OI` / `secondary_oi` | 排除当日主力后，上一交易日收盘 OI 最大的合约，允许近月 |
| `FAR_SECONDARY_OI` / `far_secondary_oi` | 交割月严格晚于当日主力，在候选中取上一交易日收盘 OI 最大者 |
| `NEAR_SECONDARY_OI` / `near_secondary_oi` | 交割月严格早于当日主力，在候选中取上一交易日收盘 OI 最大者 |
| `VOLUME_SECOND` / `volume_second` | 全品种候选按上一交易日成交量排序，取第二名；可能与当日主力相同 |

OI 排名相同则按成交量降序，成交量排名相同则按 OI 降序，仍相同则按规范化合约代码降序，确保结果稳定。OI/成交量缺失、负数、非有限值不能参与对应排名；来源标记已到期的合约不参与。近远月按规范化交割年月比较，不按字符串或月份两位数比较。

次主力模式每天重新排名，不沿用主力的 1.1 倍门槛或单向换月约束。主力规则不变：OI 超过当前主力 1.1 倍、只往远月切换；成交量用于 OI 相同时排序。另一个已有规则 `local_tqsdk_oi_and_volume` 要求 OI 与成交量共同领先，但生产配置没有选择它。

`trading_day` 按交易日，夜盘属于下一交易日。`selected` 是当日有效结果，`ranking_source_day` 是上一交易日；`scheduled_next` 使用当日收盘数据，仅在收盘后可用，不能用于当日回测。周末和节假日通过已发布的交易日序列衔接。

没有主力、没有历史截面或没有符合近远月限制的候选时，返回空 `contract` 和 `unavailable_reason`，不退回主力或其他月份。回测遇到选约缺失会明确失败，可据此缩小日期/品种范围；不会静默跳过或强行成交。

## 请求和响应

```json
{
  "products": ["RB", "SN"],
  "mode": "FAR_SECONDARY_OI",
  "start_trading_day": 20261113,
  "end_trading_day": 20261116
}
```

日期上下界均包含；两者为 0 时返回每个品种最新发布交易日。范围跨度最多 31 个自然日。`exchange` 可选；`products` 为空表示选定交易所的全部品种。返回每个品种/交易日的主力、所选合约、排名依据日、OI、成交量、下一交易日候选，以及 `rule_version` 和结果内容哈希 `snapshot_id`。

查询示例（服务须已发布包含新 RPC 的版本）：

```bash
cargo run --release -p cn_kline_server --example contract_selection -- \
  --endpoint http://127.0.0.1:18312 --products RB,SN \
  --mode far_secondary_oi --start 20261113 --end 20261116
```

## 回测接入

```toml
market_grpc = "http://127.0.0.1:18312"
contract_selection = "near_secondary_oi"
```

工作台提供同样的五个选项。整次回测固定选择模式；每天使用 API 的有效结果，将旧合约目标清零、撤销旧委托，再模拟平旧开新。既支持向远月切换，也支持向近月切换。切换时如果已有多个未完成的旧合约持仓，显式报错，防止丢弃未平敞口。实际成交仍受交易时段、延迟、深度和换月敞口约束，选约变更不保证立即完成换仓。

运行保存 `contract_selection.json`，manifest 保存 API 地址、选择模式、版本及快照哈希。选择旧主力作为基准时，也从同一 API 取数。独立 `roll_cost` 仍由用户明确指定两腿，普通策略无需额外执行该任务。

权威乘数由 `GetProductMultipliers` 返回，仅提供已验证记录。费用和指定日期的时段/tick 快照继续通过 reference HTTP API 获取。`StreamReplayFrames` 提供原始 MDL 帧、计划虚拟时钟和源游标；源选择、缓存构建与原行顺序复用 `mdl_replay`。旧 ZMQ 服务和帧格式不变。

历史流受单会话上限约束，频道缓存顺序预热、每个分片固定 2 个构建线程；流分批背压传输。客户端必须校验连续游标、时间边界、最终条数和 SHA-256，并收到服务端完成源稳定性检查后的结束标记，才能将任务标为完成。RPC 的路径配置只在服务器 `[replay]` 段。旧客户端配置里的 `msg_root`、`cache_root`、`roll_dir` 已删除，传入时会报未知字段，不能绕过接口。

gRPC 历史流按消费速度背压发送，旧客户端 `replay_speed` 配置已删除；行情/报单/撤单/回报的虚拟延迟配置保留。传输快慢不作为撮合时间。

服务端配置片段在 `config/cn_kline_server_replay.example.toml`，发布支持新 RPC 的二进制时再合入生产配置。现有生产配置保持原状，本次不自动发布或重启生产；仍按统一服务器的干净 checkout 发布流程启用新版本。

## 本机验证（2026-11-17）

- 相关四个 crate 的 release 测试 65 个通过，包含同日数据不参与有效选约、跨周末、近远月限制、成交量第二名可等于主力、候选缺失、旧委托撤销和向近月换仓。
- 启动独立 localhost 测试服务，2026-11-13 / 2026-11-16 的 80 个品种、五种模式共 800 条 API 结果，与独立 Python 按上一交易日截面计算的结果一致。
- 原始用户仓位、500 万本金、2026-11-13、SN 远月次主力 `sn2701`、taker：通过 gRPC 读取 55,800 帧，18 手成交，参考快照费用 60.32758 元，结束时目标和持仓均归零。这是该费率假设和撮合模型下的功能验证，不代表真实成交。
- 该日原始帧共 19,016,498 字节，全流 SHA-256 与既有 MDL 缓存字节一致；重复运行的 10 张 Parquet 表和 summary 逐字节一致。
- 证据在 `/mnt/nvme-raid0-28t/fanghaizhou/backtest/api_selection_validation/verification.json`，测试服务与生产端口隔离；没有重启生产服务。
