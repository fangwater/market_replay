# market-replay-server：统一服务进程

2026-11-16 起，本项目只有一个常驻进程 `market-replay-server`（代码 `crates/server/market_replay_server`，配置 `config/market_replay_server.toml`）。PM2 里属于本项目的只剩它和数据库 `pg-domestic-futures`：

```bash
./scripts/market_replay_server.sh start --execute   # 首次注册
./scripts/market_replay_server.sh restart --execute # 升级二进制或改配置后
target/release/market-replay-server --config config/market_replay_server.toml --check  # 只校验配置
```

数据库不放进这个进程托管：服务升级、重启都不应该连带重启数据库。

## 它做什么

| 部分 | 说明 |
| --- | --- |
| gRPC `cn_market.v1` | `0.0.0.0:18312`，配置取自 `config/cn_kline_server.toml`。逻辑在 `cn_kline_server` 库里，独立二进制 `cn-kline-server` 仍可单独运行 |
| HTTP | `0.0.0.0:18313`：主力合约 `/api/v1/dominant`、管理页 `/admin`、管理 API `/api/admin/*`，之后的数据看板也挂在这里 |
| 常驻服务 `[[service]]` | 子进程，退出后按 2 秒起、最长 60 秒的退避自动重启：`cn-reference-api`（:8765）、`cn-reference-daily-sync`（自带 18:15 调度）、`cme-reference`（:8770）、`exchange-mcp`（:8780），以及 `mdl-replay`（默认关闭，按需在配置里启用） |
| 定时任务 `[[job]]` | 按时区和时刻排程，所有任务走同一个队列依次执行：`cn-daily`（上海 16:30）、`ib-kline-daily`（上海 09:30）。每个任务各自发钉钉 |

## 规则

- **环境变量：** 启动时从 `env_file`（`~/.config/market_replay/env`，权限必须是 600）读入钉钉等变量，子进程继承。代理变量沿用启动 PM2 时 shell 里的环境。
- **子进程：** 每个子进程单独一个进程组。stdout 和 stderr 合并写到 `state_dir/logs/{service,job}/<名称>.log`，超过 64 MB 时轮转成 `.log.1`。
- **停止：** 服务器收到 SIGINT 或 SIGTERM 后，向每个子进程组发 SIGTERM，超过各自的 `stop_timeout_s` 再发 SIGKILL。PM2 的配置是 `treekill: false`、`kill_timeout` 90 秒，不能改小。正在跑的任务会被记为 `interrupted`；cn-daily 等任务都能断点续跑，重启后手动触发一次即可。
- **定时任务：**
  - 每个任务最后处理到的时刻记在 `state_dir/job_slots.json`。
  - 第一次加入的任务从"现在"开始计时，所以上线时不会突然补跑。
  - 服务器停机期间错过的时刻，在 6 小时内恢复的会补跑一次（触发方式记为 `catch-up`），更早的不补。
  - 同一个任务已经在排队或在运行时，不会重复入队。
- **运行记录：** 每次运行追加一行到 `state_dir/runs.jsonl`，内容包括触发方式、计划时刻、开始和结束时间、结果和日志路径。
- **管理接口：** 读接口不需要认证。POST 接口（立即运行任务、重启服务）只接受本机请求；从其它机器调用要带 `X-Admin-Token` 请求头，值是环境变量 `MARKET_REPLAY_ADMIN_TOKEN`。

## 新增任务或服务

在 `config/market_replay_server.toml` 里加一个 `[[job]]` 或 `[[service]]`，先用 `--check` 校验，再通过 `scripts/market_replay_server.sh restart --execute` 重启。**不要再为单个任务注册新的 PM2 进程。**

环境脚本重启前检查队列和运行任务；有任务时保留当前服务，并归档相关 PM2 日志后再执行空闲服务的重启。
