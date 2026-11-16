# SN 盘中缓存

最后更新：2026-11-16

`market-replay-server` 内的 CN gRPC 服务同时运行 SDK-free 的 ZMQ 采集和延迟 MDL 落盘导入，不另建 PM2 进程。配置在 `config/cn_kline_server.toml` 的 `[intraday]`，当前只缓存上期所 SN 全部交割月；SQLite WAL/FULL 存储在 NVMe，保留 7 个自然日。NFS 始终只读。

`StreamIntradayFrames` 接收交易所、原生合约、交易日和已覆盖历史的结束时间，按原生 `ActionDay/SeqNo` 返回原始 MDL 帧。批次有 cursor、覆盖证明和明确的 final marker；读取固定 SQLite 快照，最多 200,000 帧、256 帧/批、2 个并发读者。无 final marker 不得切到实时。

缓存连接早于历史截止时点，或延迟落盘已从历史截止时点完整导入并与当前连续 ZMQ 连接中的新鲜快照逐字段一致，才声明 ready。原生身份重复只存一次，内容冲突、源文件截断或已导入尾部变动会阻止覆盖证明。重启/重连都会换 epoch，旧 overlap 不能证明新连接的完整性；需等待落盘追上新连接。源目录按自然日，归属按 TradDay；不把文件当前长度当作已完成的整交易日。

CTA 先订阅实时并缓冲，再通过 gRPC 回放，找到同一原生快照后去重切换。历史/缓存只重建因子，不回补过去仓位。缺口仍未被落盘覆盖时保留事实仓位并重试，不能保证断线后立即恢复。

验证：`cargo test --release -p cn_kline_server --lib`；`cargo clippy --release -p cn_kline_server --all-targets -- -D warnings`。测试覆盖非零开盘、重复与身份冲突、半行追加、重连及过期 overlap、固定快照 final/cursor。
