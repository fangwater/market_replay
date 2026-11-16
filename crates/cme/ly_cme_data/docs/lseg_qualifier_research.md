# Qualifiers 联网核查：原生原因4及UNKNOWN区间

2026-11-11 用户要求联网查找并允许提出推测，随后接受补充解析。
`decoder_policy_version=2` 已将本批CME原生原因4解析为到期：更新1,491行、265个状态日文件，
原消息、RIC、时间、重复行及Qualifiers保持不变。生产RocksDB和原始状态只读。
用户进一步要求重做K线且只用LSEG；独立入口为 `scripts/rebuild_lseg_only_klines.py`。

## 已查到的公开来源

1. [CME官方 MDP 3.0 - Security Status](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457223126/MDP+3.0+-+Security+Status)。
   官网公告目录虽然403，但该公开技术文档的匿名内容接口返回200，已保存正文。
   其中instrument-level状态表明确列出326=18不可交易、327=4到期。
   327字段的用途是状态变化原因，不能把字段名HaltReason理解成所有非零值都是临时停牌。
2. [LSEG官方枚举字典](https://github.com/Refinitiv/Real-Time-SDK/blob/master/Cpp-C/etc/enumtype.def)：
   HALT_RSN的显示码NH=Security Not Halted，SS=System suspended；TRD_STATUS的H/N为停牌/正常。
3. [LSEG官方字段字典](https://github.com/Refinitiv/Real-Time-SDK/blob/master/Cpp-C/etc/RDMFieldDictionary)：
   HALT_REASN为原生ALPHANUMERIC字段，不是HALT_RSN的枚举数值；TRD_TYPE也是原生字段。
   **本轮没有查到LSEG本源逐字段、逐数值与CME原生字段绑定的完整官方码表。**
4. [EPAM CME处理器附带SBE模板](https://github.com/epam/java-cme-mdp3-handler/blob/master/mbp-only/src/test/resources/templates_FixBinary.xml)
   为辅助佐证，枚举明确列出InstrumentExpiration=4。该模板是2018年version=9的第三方保存版本，
   不能冒充当前CME官方模板或覆盖后来新增码；当前官方文档还出现7=Trade Date Roll。

抓取URL、SHA256、HTTP状态和正文在项目`artifacts/lseg_qualifier_research_20261111/`。
本次下载用于解释字段，不证明这些文档版本在历史交易时刻已经可用。

## 100条原生原因待核消息

全部包含`4[HALT_REASN]`。按同RIC、发布时间分组后为66组事件：

| 证据 | 核对结果 |
| --- | --- |
| 对应Databento仍有覆盖的事件 | 52组，两秒内均出现INSTRUMENT_EXPIRATION，没有找到相反归因 |
| 没有两秒内Databento事件 | 14组，全部在2026-09-15以后，已超过当前Databento下载截止；不是据此认定源缺消息 |
| 与现有definition到期时间一致 | 31组，源发布时间与参考到期时刻相差不超过2秒 |
| 仅HRD日期级对照一致 | 15组；日期吻合不证明精确交易时刻 |
| HRD日期与实际状态时刻不一致 | 20组，均为S1R/SRA；HRD日期晚于状态时刻，见下文 |
| LSEG标准原因显示NH | 79条，字典明确表示未停牌 |

**高可信推断：在本批已核定CME Group品种里，`4[HALT_REASN]`对应Instrument Expiration，
应优先解释为合约到期／停止交易，不能按一般异常休市等待复市。**
这是CME官方协议语义、当前源样本、同RIC跨源事件对照共同支持的映射；
由于尚缺LSEG明确的本源绑定说明，研究结果仍将其标为高可信映射推断，不推广到其它交易所。

同一个4放在不同FID有不同含义：`4[TRD_TYPE]`是Close，`4[HALT_REASN]`才是本次到期原因候选。
而`HALT_RSN=NH`又是另一套标准枚举，不能混成一个码表。

已接受的落地方式是：`reason=INSTRUMENT_EXPIRATION`、`action=EXPIRE`、`session_state=EXPIRED`、不可交易，在明确到期处结束合约覆盖；
不把它写为HALTED，不用旧收盘价补到次日等待复市。发布时间仍用原始纳秒，不照参考源回拨。
正式session_status已应用此规则；同RIC到期后出现的开盘／预开盘消息保留原文并列问题，不使其重新激活。

S1R/SRA有20组HRD时间差异。例如S1RF26的源状态是2026-01-30 22:00 UTC，
当前HRD日末参考是2026-02-03 00:00 UTC；该源状态同时与Databento的到期事件一致。
这证明两者不能机械互换。**“HRD可能对应结算日期而非最后交易时刻”只是待核假设**，
不因该假设修改HRD原日期；只用源状态明确的到期时间提前结束补行，真实成交仍保留。

## 18段UNKNOWN的进一步解释

| 类型 | 数量 | 证据及边界 |
| --- | ---: | --- |
| 同发布时间内的中间态 | 5 | start等于next_known，是零时长，应从正时长缺口统计中排除；原消息仍保留 |
| GCK28挂牌准备 | 1 | 3月27日15:03—21:00的未知段在现有definition activation 21:30之前；不是已知有效交易期内的停牌 |
| 欧元新增非季度月准备期候选 | 12 | 5月8日至5月10日21:00；12个RIC均与既有SER-9719、候选挂牌目录的5月11日交易日扩展吻合；首次明确OPEN均在5月10日22:00 |

12段欧元的解释属于高可信候选，不把尚未并入正式目录的候选挂牌日冒充已发布的完整交易日历。
这13段正时长未知区间目前都位于首次明确开盘以前；不能把“首次明确开盘前”单独当作挂牌时间的证明。
原先18段的表述包括5段零时长中间态，**不等于18次异常停牌或18段盘中状态丢失**。

欧元公告与既有证据见[挂牌身份复核](/home/fanghaizhou/preprocess/database/cme_causal_resume_20261025/fx_listing_identity_review.md)，
原公告URL为[官方SER-9719](https://www.cmegroup.com/content/dam/cmegroup/notices/ser/2026/04/ser-9719.pdf)。
本轮没有再次请求已受限的CME官网PDF，使用的是此前已保存并核对的原公告证据。

逐组到期对照为`expiry_alignment.json`，未知段分类为`unknown_classification.json`，研究摘要为`summary.json`，
均位于项目研究目录。交付盘只补充实际问题的研究说明，不发布正常成功audit或重复复制全部公开文档。
