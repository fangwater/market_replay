# LSEG Qualifiers 状态试解析

2026-11-11 用户要求参照现有 Databento 状态解析 LSEG Qualifiers，接受以源发布时间代替
交易所事件时间，并询问异常休市能否使用 CME 官方消息补充。

本轮读取生产 RocksDB 的 kind=13 状态范围，未重扫 tick。原库、原始 `status.parquet`、
Databento `market_status.parquet` 和 IB 均保持原样。标准化状态为：

```text
lseg/<exchange>/2026/<RIC>/<YYYYMMDD>/session_status.parquet
```

用户随后接受decoder_policy_version=2，并要求K线只使用LSEG数据，重做入口见
`rebuild_lseg_only_klines.py`。状态范围为已经核定交易所的
ES/NQ/RTY/YM/GC/CL/SRA/S1R/URO/KWR，不能把 KWR 与韩国市场的 KRW 混用。
640,046 条原始状态中，634,102 条／373 RIC 可发布；另5,944条／25 RIC归属证据不足，
保留原始数据并列问题。源发布日期范围为2026-01-01 22:00至2026-11-06 22:00 UTC，
不据此保证每个合约每天都有消息；缺文件不证明正常交易。已知源缺口继续适用，例如NQ缺2026-11-04正式日包，不能跨该缺源日期仅以前一状态断言正常交易。

## 时间及字段

LSEG 行的 `ts_utc_ns=published_ns=effective_ns`，逐纳秒保留源发布时间，不取整、不平移，
也不根据 Databento 时间把 LSEG 消息提前。目录按 `ts_utc_ns` UTC 日期。
只有状态变化消息和辅助原消息，没有制造每日午夜快照；首次明确状态前为 UNKNOWN，
按日读取时需要从此前日期带入状态。

| 字段 | 含义 |
| --- | --- |
| product / ric | 原品种列族、原始RIC |
| ts_utc_ns | 状态开始应用的UTC纳秒 |
| published_ns / effective_ns | 源发布时间、明确生效时间；LSEG均按发布时间 |
| action | CLOSE、PRE_OPEN、OPEN、HALT、NOT_AVAILABLE、NO_CANCEL、IMPLIED_ON/OFF、PRE_CROSS、NO_CHANGE等 |
| session_state | TRADING、CLOSED、PRE_OPEN、HALTED、NOT_TRADING、UNKNOWN |
| is_trading | 可交易／不可交易；未知为null |
| reason | 已核定原因，否则UNSPECIFIED；不擅自补成SCHEDULED |
| is_transition | 该行是否改变session_state；辅助行和重复行仍保留 |
| source / source_url | LSEG_TAS或CME_OFFICIAL；官方补充保留URL |
| qualifiers | 完整LSEG原串；官方补充为公告原文 |

这里的session_state是消息级阶段，不等同于K线的TRADING/SCHEDULED_CLOSED/HALTED/PARTIAL/UNKNOWN五类分钟状态。
尤其CLOSED本身没有声称关闭原因是计划休市。

## 本轮规则与依据

| 原字段 | 解析 |
| --- | --- |
| TRD_TYPE=4且INST_PHASE=C | CLOSE / CLOSED |
| TRD_TYPE=21且INST_PHASE=O | PRE_OPEN；已确认停牌后的预开盘仍保留HALTED直到真实开盘 |
| TRD_TYPE=15，或17且INST_PHASE=T | OPEN / TRADING；不伪造两个独立的NEW_PRICE_INDICATION和TRADING事件 |
| TRD_TYPE=2 | HALT；2026重叠中的128个CL恢复处理中停盘标记全部匹配；休市准备期间不新建盘中HALTED |
| TRD_TYPE=18且INST_PHASE=C | NOT_AVAILABLE / CLOSED；不单凭18推断关闭原因或到期 |
| TRD_STATUS=H | 明确停牌；N仅解除明确停牌，不单独证明已经开市 |
| HALT_RSN=SS | 官方枚举“系统暂停”；结合此前是否处于交易会话区分HALTED与休市准备 |
| HALT_RSN=NH | 官方枚举“Security Not Halted”；不能因其搭配的HALT_REASN=2非零就误判停牌 |
| ORD_ENT_ST=N | NO_CANCEL；I/E/C不单独决定能否交易 |
| SECUR_ST=5/6 | 本轮经验映射为隐含撮合开启／关闭；不改变session_state |
| 空原因、空MKT_ST_IND、重复辅助消息 | 不凭空开市，也不清除停牌 |

原生字段没有完整公开码表，以上原生码映射限定于已核对的CME品种；未知码、冲突和缺少原因
继续列问题。官方枚举依据为 preprocess 已保存的 LSEG SDK `enumtype.def` / `RDMFieldDictionary`，
特别不能把ORD_ENT_ST=I反向翻译成接受订单。

## Databento 对照

本轮按照同一RIC、已核定合约有效期比较2026重叠窗口，排除UTC午夜初始化快照；
1,193条有效期之外的参考事件不计入匹配分母。此口径不同于旧报告的“品种级历史时点”统计。

- 收盘／预开盘／开盘共196,625个参考事件，其中195,774个在源发布时间前后2秒内找到同类LSEG事件，99.5672%。
- 各类参考事件的两秒后探针，两端均使用同一探针时刻的最新状态，可交易标记一致率99.7108%。
  不把两秒内又发生的另一条真实状态变化误判为先前事件错误。
- 有效期内的128个CL HALT参考事件全部找到对应原生码2；这不证明公告或LSEG覆盖所有市场、所有异常。
- NOT_AVAILABLE的大量周末准备事件未逐条复现；部分场景两端本就处于不可交易状态，不能把“附近状态为false”说成“消息字段完整一致”。

两秒只是比较容差和探针偏移，文件时间没有移动两秒。不一致明细保留在NAS问题目录，
本地指标与复现断点位于 `artifacts/lseg_session_status_20261111/`。
观察到消息但尚未有充分阶段证据的13段正时长UNKNOWN区间单列为`unknown_intervals.csv`，不补成正常交易；已排除5条零时长中间态。

decoder_policy_version=2把CME原生HALT_REASN=4解析为EXPIRE／EXPIRED／INSTRUMENT_EXPIRATION；
66组合约到期事件已应用。到期后的开盘／预开盘标记仅列问题，不重新激活RIC，原文始终保留。
这是CME官方协议和同RIC跨源样本支持的高可信绑定推断，仍不推广其它交易所。

## CME 官方补充

可以补明确公告的停牌／恢复，但官方公告不是每个合约每次短暂状态变化的完整行情流。
补充记录必须包含官方URL、原文、发布时间、生效时间、适用品种／RIC和已核实的证据文件哈希。
产品级公告只有在合约有效期已核定时才展开，不把Cboe消息外推为CME全市场消息。

应用时间为 `max(published_ns,effective_ns)`：提前公告未来复市，必须等到复市时刻；
事后才发布的公告不倒填成提前可知。CME事件与LSEG消息合并后一起顺序重放，后续空字段不覆盖官方已知状态。

已经用原有2025-11-28事故证明验证：135条TAS明确停牌消息之后，
CME官方于13:18:30 UTC发布13:30 UTC恢复的公告；应在13:30应用，不在13:18:30提前开市。
证据为原帖 <https://x.com/CMEGroup/status/1994395477840990459>，作者ID和未编辑状态也已核验。
本轮尚无新核定的2026官方补充记录，**没有声称已把2026异常休市全部补齐**。

```bash
bash scripts/build.sh --offline --bin tas_status_extract --jobs 4
# 本批source_plan由既有TAS状态计划选择十个核定品种，生产库只读。
/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/tas_status_extract \
  --plan artifacts/lseg_session_status_20261111/source_plan.json \
  --output /mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_session_status_20261111/raw
python scripts/verify_cme_status_supplement.py
python scripts/build_lseg_session_status.py --publish \
  --official artifacts/lseg_session_status_20261111/official_supplements.json
```

当前代码使用 `reference_cme/.venv/bin/python`。补充公告登记保存在项目本地，
续跑不因省略命令参数就丢掉已经核定的官方事件。交付盘只有数据与实际问题，
不写成功audit或正常进度报告；每个品种按UTC日期、RIC排序选首／中／末完整读回，其余仅轻量footer检查。
