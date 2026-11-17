# 只用 LSEG 的 2026 分钟线

2026-11-11 用户接受Qualifiers补充解析，并要求完成后只用LSEG重做K线。
入口为 `scripts/lseg/rebuild_lseg_only_klines.py`，状态核心为 `scripts/lseg/lseg_kline_state.py`；
价量继续复用已有共享分钟生成器和可信改撤账本。

## 数据口径

- 行情：现有本地Trade Parquet，来源为只读生产／补充／日包RocksDB导出。
- 状态：LSEG kind=13原始消息，按发布时间、源顺序逐纳秒应用；同刻补丁只向分钟表达最终状态。
- 年初状态预热：同一RIC的2025年12月LSEG状态，只读扫描kind=13，不扫全年Trade。
- 年初价格预热：此前只读索引查出的同一RIC历史末笔LSEG成交，不借IB、不猜年份别名。
- 有效期：LSEG HRD、明确到期状态及实际观察证据。HRD首次交易日仅用于判定年前已存在；
  新挂牌不把交易日标签当作UTC时刻，以实际观察的明确阶段或成交开始；提前的辅助字段更新不单独建立覆盖。
- 明确原生原因4为终态EXPIRED，优先于HRD日期级回退；HRD结束使用最后交易／到期日UTC日末。
  真正成交即便与参考到期冲突仍保留，问题另外列出。
- 改撤：使用既有LSEG回源证据核定的关联，不推测删除704条待核取消关联。
  新增19个有Trade文件的RIC，经兼容版本只读RocksDB清点确认无Correction事件，采用有证据的空改撤计划。

不加载Databento DBN、状态或definition，不叠加CME公告或静态交易日历。
Databento仅在此前规则校准中提供对照，不参与本次任何K线时段、有效期或价量计算。
原始Trade／Quote／status、生产RocksDB、IB及独立Databento参考文件保持只读。
LSEG原始归属为review的合约仍按原始归属发布，不根据旧Databento definition搬市场。

## 状态和填充

使用端仍为10列：`ric, ts_utc_ns, open, high, low, close, volume, trade_count, is_ffill, status`。
OHLC为可空Float64，价量及时间单位不变；schema版本5，`kline_status_policy=lseg_only_v1`。
状态为TRADING／SCHEDULED_CLOSED／HALTED／PARTIAL／UNKNOWN，详细消息另见status／session_status。
只有明确原生计划原因才新建立SCHEDULED_CLOSED；仅已知交易会话被中断或明确停牌才建立HALTED。
停牌后的PRE_OPEN继续HALTED，直到明确开盘或计划收盘。
周末准备阶段的监管／恢复原因不将已经计划休市的区间误标为盘中停牌。
缺失字段不当作0，辅助消息不能凭空开盘或从缺源状态恢复。

合约有效覆盖期内保持连续UTC分钟：无成交沿用本RIC旧收盘、零量零笔数；
真正补价才is_ffill=true，无历史价为null。状态发布时间不取整、不回拨；
与交易所时钟归桶的真实成交相差毫秒或亚秒，可使边界分钟出现PARTIAL或状态外成交，不能据此删成交。

按正式源清单识别缺包，缺口起点重置UNKNOWN，直到缺口后的下一条明确阶段。
当前包括KWR 5月30–31日、NQ 11月4日；缺包结束本身不自动恢复交易。
各品种覆盖按现有源截止，为11月2／6／7／8日半开区间，不称2026全年覆盖。

## 发布和比较

只替换 `lseg/<exchange>/2026/<RIC>/<YYYYMMDD>/kline.parquet`，按日原子替换。
旧范围内的实际成交OHLCV和笔数与本地已修正工作缓存逐值核对，新的全年净计数／量与可信改撤后的源聚合守恒。
旧有效期外纯补行，只有footer证明零量零笔数才删除。
所有变更文件做footer、schema、行数、RIC、UTC日期检查；每个RIC首／中／末最多3份完整读回，
不全盘读回或重做全文件哈希。源文件及IB抽样的inode／大小／修改时间保持原样。

比较读取原始IB完整快照，截止不超过该快照最后有效分钟下一分钟（2026-11-03 05:20 UTC），
不把之后的TAS新增日期计为IB缺行。IB文件不补、不裁剪、不新增状态或写回。
比较排除TAS计划休市／UNKNOWN的纯补行，实际成交、HALTED以及真实IB缺口保留；
总体量差只按双方匹配分钟计算，不将差异自动归因为IB错误。
比较汇总入口 `scripts/lseg/finalize_lseg_only_klines.py`，更新原 `meta/comparison_ib/2026/` 中文表和问题数据。

本地断点：`artifacts/lseg_only_klines_20261111/contracts/*.json`；NVMe工作缓存为
`/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_only_klines_20261111`。
目标仅记录实际问题于 `meta/problems/lseg_only_klines/`，不创建成功audit或完成回执。

```bash
/home/fanghaizhou/market_replay/reference_cme/.venv/bin/python \
  scripts/lseg/rebuild_lseg_only_klines.py --workers 4
/home/fanghaizhou/market_replay/reference_cme/.venv/bin/python \
  scripts/lseg/finalize_lseg_only_klines.py
```

持续执行使用用户服务 `ly-cme-lseg-only-klines-20261111.service`，运行状态须用systemctl核对；
不能将旧processing JSON或服务启动当作整批已完成。恢复执行跳过有完整本地断点的RIC。
