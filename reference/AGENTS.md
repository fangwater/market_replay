# 项目协作约定

- 与用户沟通时使用中文。
- 网页前端所有面向用户的文案使用中文。
- 启动本地 Web 服务时默认监听 `0.0.0.0`。

## PostgreSQL

- 国内期货元数据统一使用本机独立 PostgreSQL 16 实例：Unix socket
  `/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run`，端口 `5433`，
  数据库 `market_metadata`，用户 `u171`。使用 peer 认证，不在仓库或 `.env`
  中保存密码。
- 严禁连接、写入或 promote 另一套 PostgreSQL 实例及其热备库。服务的连接串应为
  `postgresql:///market_metadata?host=%2Fmnt%2Fnvme-raid0-28t%2Fpostgresql%2Fdomestic_futures%2F16%2Frun&port=5433&user=u171`。
- 本项目的表必须位于独立 schema `cn_futures_reference`，不得在 `public` 创建
  `metadata`、`snapshots` 等通用名称的表，也不得修改既有的
  `public.domestic_future_product_multipliers` 或交易时段表。
- OpenCTP 原始字段作为经纪商参考保留；本地权威乘数表不被本服务写入或覆盖。
- 参考数据采用逐合约增量版本：相同内容只更新 `update_ts`；内容变化才新增一条记录并写入新的 `start_ts`。不得每次同步整表写入快照或归档整页 HTML。
