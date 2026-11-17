# 国内期货账户费率记录

最后更新：2026-11-17

由 `market_replay` 的 `cn_reference` 维护，沿用本机 `market_metadata` PostgreSQL 和现有 :8765 服务。账户费率使用独立的 `cn_futures_reference.account_rate_accounts` / `account_rate_observations` 表；OpenCTP 的 `instrument_versions` 继续保存公共参考数据。

每条观察记录包含独立账户别名、经纪商、交易所、原生合约、CTP 交易日、准确查询时间、合约乘数、tick、开仓/平仓/平今按金额及按手两项手续费、多空按金额及按手两项保证金。金额单位为 CNY，手数为原生手。来源固定为 `CTP_ACCOUNT_QUERY`，保留原始查询回执 SHA-256 和记录 SHA-256；不存登录凭据、真实账号、账户余额或本地风控预算。每个账户使用唯一别名，不得将同一别名重新分配给另一个账户。

`observedAt` 表示何时确实查到这组参数；`tradingDay` 保留柜台原生交易日。历史查询仅选择 `observedAt <= asOf` 的最近记录。首条记录以前返回空，不根据今天的参数伪造过去费率；同一品种其他月份也不继承已查询合约的费率。

## 维护

CTP 登录及 SDK 只能在用户指定的外部 `sh_dev` 执行。把成功完成的只读查询回执拷回本机私有目录，然后由已发布的 `cn-reference` 导入：

```bash
bin=/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin/cn-reference
"$bin" --env-file reference/.env import-account-rates /private/query-result.json \
  --account chuangyuan01 --broker chuangyuan
```

支持既有的单合约 `rule` 和多合约 `rules` 查询回执，校验查询成功、进程正常结束、只读来源和完整费率字段。重复导入幂等；同账户、交易所、合约、查询时刻内容冲突时整批回滚，保留原始记录。后来新增的查询追加为新观察，不覆盖旧快照，也不从 OpenCTP 回填缺项。

初始实测数据为 2026-11-17 的 `SHFE/sn2612`、`SHFE/rb2701`、`DCE/c2701`、`DCE/m2701`。只记录这四个已查询合约，不宣称覆盖账户所有可交易合约。

## 查询

最新记录：

```text
http://127.0.0.1:8765/api/v1/account-rates?account=chuangyuan01
```

某一时刻已知的单合约参数：

```text
http://127.0.0.1:8765/api/v1/account-rates?account=chuangyuan01&venue=SHFE&instrument=sn2612&asOf=2026-11-17T04:00:00Z
```

观察历史（包含边界，最多 366 天 / 10,000 条）：

```text
http://127.0.0.1:8765/api/v1/account-rates?account=chuangyuan01&start=2026-11-17T00:00:00Z&end=2026-11-17T23:59:59Z
```

`asOf` 和历史 `start/end` 互斥；时间必须带时区。返回 `rates` 中的 `fees` / `margin` 使用与参考服务一致的按金额+按手结构，数值使用十进制字符串。响应同时带 `observedAt`、`tradingDay`、来源与哈希。查询账户必须显式指定。

CLI 支持相同查询，并可把 JSON 重定向到回测输入：

```bash
"$bin" --env-file reference/.env account-rates --account chuangyuan01 \
  --venue SHFE --instrument sn2612 --as-of 2026-11-17T04:00:00Z
```

## 回测

已有 `cn-backtest` 可显式选择账户快照成本情景：

```toml
fee_policy = "account_snapshot"
fee_account = "chuangyuan01"
fee_as_of = "2026-11-17T04:00:00Z"
```

其余合约元数据、交易时段和已验证乘数沿用原来源。手续费只接受该账户、该原生合约的记录；没有记录的合约在使用时失败，不回退到 OpenCTP 或同品种其他月份。已冻结的 metadata 必须匹配所选账户和费率查询截止时间，不能误用旧公共费率文件。

这是一组显式成本假设，输出 `historical_exact=false`，并保存完整账户快照及哈希到回测 provenance。用今天的账户快照跑更早行情可以评估该费率下的成本，但不能称为那个历史日期的真实账户费率。需要严格逐时点费率时，按成交时间访问 `asOf` 历史；首条观察前没有事实数据。CTP 参数估算的手续费也不代表成交回报已给出逐笔实收费用。

验证：`cargo test --release -p cn_reference -p cn_futures_backtest`；显式指向本机 5433 元数据库后运行忽略的 PostgreSQL 集成测试 `postgres_asof`，会创建并清理独立测试 schema。集成测试覆盖观察时间选择、账户隔离、重复导入、冲突保留与查询时间前不回填。
