# mdl-replay：通联 MDL 单日回放服务

最后更新：2026-11-14。代码 `crates/mdl_replay`，二进制 `target/release/mdl-replay`。

## 做什么

按 **交易日（或自然日目录）+ 品种／合约 + 频道** 从 NFS `tonglian_msg` 取出当天的 MDL 行情，用 ZMQ 重新发布。每一帧和公司内网实时转发 `tcp://10.61.20.12:13005` **逐字节一致**：`<topic> || <CSV 原行>`，例如 `mdl.22.1.* || rb2601,2026-11-13,...`。

所以 mkt_signal 的 `cn-futures-pbs --zmq` 只需把 `[zmq].endpoint` 改成回放地址，后面的 IPC、因子、信号整条链路都不用改代码。用途是单日快速信号回测复盘和压力测试。

只覆盖 msg 层（2023-12-07 起）。按用户 2026-11-14 的决定，不支持 `comm_l2` 历史日包。

## 架构

```
NFS /mnt/nfs/30.3_tonglian_msg/YYYYMMDD/mdl_*.csv   (只读)
   │ 首次请求时：并行 pread → SIMD 切行 → 按 (InstruID, TradDay) 分组
   ▼
NVMe cache_root/v1/YYYYMMDD/mdl_XX_Y_0/{frames.bin, rows.bin, index.json}
   │ frames.bin = 预拼好的 ZMQ 帧（按合约分组，组内保持源文件行序）
   │ rows.bin   = 每行 24B：帧偏移/长度/单调接收时间/源行号
   ▼
plan：选目录 → 选组 → 目录内按 (单调 LocalTime, 频道, 源行号) k 路归并
   ▼
session：XPUB socket，zmq_send(mmap 切片)，按原始节奏、N 倍速或不限速发送
```

- **只看三列**：解析器只取 `InstruID`、`TradDay`、`LocalTime` 三列，列位置从文件表头读，不写死每个交易所的表。其余字节原样透传；截断或坏行也照样透传，并计入 `bad_time_rows` / `bad_day_rows`。
- **顺序**：`LocalTime` 会有毫秒级回跳。实测 20261113 大商所文件有 16.5 万处、上期所有 45 万处，所以文件行序才是真实到达顺序。同一频道内严格保持源文件行序；不同频道之间按"前缀最大的 `LocalTime`"交错，同一毫秒内按频道名排序。不同频道之间真实的先后无法从文件恢复，这是唯一的近似。
- **交易日**：交易日 D 的数据来自 D 当天目录，再往前回溯，经过周六目录（只有上期所／能源中心的夜盘尾段），直到第一个有中金所 `mdl_21_1_0.csv` 的完整开市日目录（D 的夜盘从这里开始），最多回看 7 天。行按 `TradDay == D` 过滤，目录日期不当作交易日。
- **郑商所合约**：2025-11-04 起使用三位码（`AP601`），按交易日所在年代扩成四位后比较，请求 `AP2601` 或 `AP601` 都能匹配。
- **缓存有效性**：每次使用都 stat 源文件，大小或 mtime 变化就重建。上海时间 D+1 03:00 之后目录才视为完整（`complete=true`）；在那之前当天数据还在追加，回放结果标 `partial=true`，只取到最后一个完整行。
- **构建并发**：最多同时构建 6 个分片（每个分片构建期间整个源文件在内存里），单个分片用 `build_threads` 个线程。

## 实测（2026-11-14，本机）

| 项目 | 结果 |
| --- | --- |
| 分片构建：自然日 20261113，六频道 2250 万行 / 8.0 GB | 冷构建 3.6 s（读 NFS + 解析 + 写 NVMe）；命中缓存约 0.5 s |
| 准备：交易日 20261113 全品种（2 个目录，914 个合约，2280 万帧） | 1.9 s（缓存命中） |
| 发送上限（无订阅者，只测 `zmq_send` 循环） | 392 万帧/s，1.39 GB/s |
| 不限速、无损，1 个 SUB 订阅端（tcp 和 ipc 一样） | 82–85 万帧/s，约 300 MB/s；全交易日 27 s 发完，零丢失 |
| 实际下游 `cn-futures-pbs --zmq`（7 个品种，229 万帧） | pbs 发布 2,286,624 帧，等于发送数；连续性重置只有连接时的 1 次 |
| 字节级校验（7 个品种 × 6 个频道，和 awk 直接从 NFS 抽取的结果对比） | 六个频道全部一致，顺序也一致 |
| 节奏精度（1000 倍速、200 倍速） | 总时长和理论值一致；逐帧偏差 p50 0.1–0.25 ms，p99 < 0.4 ms |

不限速时，瓶颈在 libzmq 单连接的接收端，以及下游自身的处理能力（pbs 约 57 万帧/s），不在回放服务。

缓存占用约等于源数据量：每个自然日约 8 GB。缓存按自然日 LRU 自动淘汰，上限是 `cache_max_gb`（默认 48，约 6 个交易日）：每次构建后和服务端每 10 分钟各检查一次。10 分钟内用过的日期不淘汰，以免删掉正在准备的回放计划要用的数据。中断构建留下超过 1 小时的临时目录也会一并清理。手动查看或触发用 `mdl-replay cache --config ...` 或 `{"cmd":"cache"}`。

## 运行

```bash
scripts/build-release.sh                 # cargo build --release --locked
scripts/start_mdl_replay.sh              # PM2 + taskset，命名空间 cn_futures_db，进程名 mdl_replay
scripts/stop_mdl_replay.sh
```

配置文件 `config/mdl_replay.toml`。控制面和会话 socket 默认只绑定 `127.0.0.1`；对内网开放时把 `control_endpoint`、`bind_host`、`advertise_host` 改成本机内网地址。

预热缓存（可选，例如盘后跑一遍）：

```bash
target/release/mdl-replay build --config config/mdl_replay.toml --trade-day 20261113
```

## 请求/响应协议

控制面是 ZMQ ROUTER，客户端用 **REQ**。每个请求是一个 JSON 帧，带 `cmd` 字段；每个响应也是一个 JSON 帧，带 `ok` 字段，失败时带 `error`。`replay`、`build`、`symbols` 在工作线程里执行（同时最多 `max_jobs` 个），不会阻塞其他请求。

| cmd | 参数 | 说明 |
| --- | --- | --- |
| `ping` | – | 版本和会话数 |
| `days` | – | NFS 上已有的自然日目录 |
| `symbols` | `select` | 每个 `频道:合约` 的行数（会触发分片构建） |
| `build` | `select`, `force` | 预构建或校验分片，返回每个分片的耗时 |
| `replay` | `select`, `params` | 创建会话，返回 `session`、`endpoint` 和 `plan` 摘要 |
| `status` / `sessions` | `session` / – | 状态、已发帧数、吞吐、背压等待次数、当前回放到的时刻 |
| `start` / `pause` / `resume` / `cancel` | `session` | 手动开始、暂停、继续、取消 |
| `close` | `session` | 取消并释放 socket |

`select` 字段：

```json
{"trade_day": "20261113",          // 或 "natural_day": "20261113"（整个目录，不按 TradDay 过滤）
 "products": ["rb", "IF"],          // 不区分大小写；products 和 contracts 都为空表示全部
 "contracts": ["AP2601"],           // 和 products 取并集
 "channels": ["mdl_22_1_0"]}        // 默认是 pbs 订阅的六个主行情频道；也可以填其他 mdl_* 频道
```

`params` 字段（均可省略）：

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `speed` | 0 | 1 = 原始节奏，10 = 10 倍速，0 = 不限速（压测） |
| `max_gap_ms` | 60000 | 超过这个时长的空档压缩成这个时长（午休、夜盘到日盘之间）；null 表示保留原始空档 |
| `endpoint` | `tcp://<bind_host>:*` | 指定绑定地址，例如 `ipc:///tmp/x.sock` |
| `wait_subscribers` | 1 | 这么多个订阅者连上并发出订阅后（再等 `settle_ms`）自动开始 |
| `manual_start` | false | 改为等待 `start` 命令再开始 |
| `lossless` | true | 开启 XPUB_NODROP：订阅端慢时阻塞等待，不丢帧；false 时达到 HWM 直接丢 |
| `sndhwm` | 1000000 | 发送高水位 |
| `repeat` | 1 | 整个计划重复发送 N 次（压测） |
| `hold_open_s` | 600 | 发完后连接保持多久（订阅者全部断开或 `close` 时提前释放） |
| `start_timeout_s` | 600 | 超时还没开始就取消会话 |

`hold_open_s` 必须保留。`cn-futures-pbs` 收到 TCP 断开事件后，会丢弃接收缓冲里还没处理的帧；早期版本一发完就关闭 socket，实测丢了 3.5% 的尾部帧。

会话事件在 topic `replay.ctl` 上发布：开始时 `{"event":"begin",...}`，结束时 `{"event":"end","sent":N,"state":...}`。pbs 只订阅 `mdl.*`，收不到这些事件。

### 例：用回放驱动 pbs 重算

```bash
B=target/release/mdl-replay
$B req '{"cmd":"replay","select":{"trade_day":"20261113","products":["rb","i"]},"params":{"speed":0}}'
# → "endpoint": "tcp://127.0.0.1:28311"

# mkt_signal 侧：复制公司内网配置，只改 endpoint 和 service_root
#   service_root = "cn_futures_pbs/replay_20261113"
#   [zmq] access_scope="company_intranet"  endpoint="tcp://127.0.0.1:28311"
target/release/cn-futures-pbs --zmq --config /path/replay_pbs.toml
```

### 例：Python 客户端

```python
import json, zmq
ctx = zmq.Context()
req = ctx.socket(zmq.REQ); req.connect("tcp://127.0.0.1:13100")
req.send_json({"cmd": "replay",
               "select": {"trade_day": "20261113", "contracts": ["IF2612"]},
               "params": {"speed": 100}})
rep = req.recv_json(); assert rep["ok"], rep
sub = ctx.socket(zmq.SUB); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(rep["endpoint"])
while True:
    frame = sub.recv()
    if frame.startswith(b"replay.ctl") and b'"end"' in frame:
        break
```

### 命令行工具

- `mdl-replay play --config ... --trade-day D --products rb --speed 0 [--endpoint ...] [--repeat N]`：不起服务，单次回放。
- `mdl-replay sub --endpoint ... [--topic mdl.22.1.*] [--dump frames.txt]`：订阅、统计，可把收到的帧落盘，用于逐字节比对；收到 end 事件后退出。

## 验证

```bash
cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release
```

集成测试（`tests/replay.rs`）用合成的 NFS 目录树覆盖以下场景：跨目录的夜盘、`TradDay` 过滤、`LocalTime` 回跳时保持行序、跨频道交错、郑商所三位码、源文件追加了半行时重建分片、频道文件缺失、通过 ZMQ 收到的帧逐字节一致加 begin/end 事件、倍速节奏。
