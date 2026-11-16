# 全库 LSEG-only 分钟线

2026-11-12 用户明确要求，将前一批十根品种的LSEG-only逻辑扩展到整个期货TAS RocksDB。
主库全部2,196个品种年CF，实际年份为2010、2011、2017—2026；另接已清洗日包增量。
共2,339个源作业／113个DB。没有2012—2016年度分区，不制造这些年份的年度覆盖。
源期间边界跨年的真实成交仍按交易所时间的UTC自然年保存。

范围为`cme_tas_rocksdb_all_products`及已完成日包库；合并补充CF从主库读取，
独立补充库不重复相加，不处理快照／secondary／验证库／LL2／美股库。
这次扩展生成K线及其详细session_status，不重复重建已有Trade／Quote，也不写回IB。

## 执行与状态

当前入口为独立Rust工具 `lseg-kline run --workers 2`，详见[原生工具](native_kline.md)。
旧Python提取、行情JSON中间层及等待接续流程已停止，不再启动。
工具只读RocksDB iterator，按当前RIC在有界内存中聚合成交和解析状态，直接写每日Parquet。
既有稀疏分钟缓存核对源身份／CURRENT／footer后复用；未完成源直接读库。
原237个已修订2026 RIC核对gross量／笔数后复用，不覆盖可信净额结果。
2026-11-13补齐Trade驱动遗漏：每组分钟完成后，按kind=12／13前缀发现没有当年成交的RIC，
核对没有实际分钟后仍交付状态、列孤立改撤；不制造没有成交依据的K线。
该步骤有独立可复用断点，旧分钟分组完成不等于这些状态也已交付。

实际运行状态须核对进程、退出码和本地断点，不能仅凭旧running标记判断。
一次性回补用普通前台 bin，每日任务由 PM2 串行调度，不用 systemd。
程序按 64 GiB、32 线程的总预算运行，默认且最多 2 个工作单元，
见[资源预算](resource_limits.md)。

源计划及旧断点在`artifacts/lseg_full_rocksdb_20261112`，旧稀疏分钟缓存位于NVMe
`lseg_data/lseg_full_rocksdb_20261112/extract`，仅复用有效部分。
新工具断点位于NVMe `lseg_data/lseg_native_klines_20261112/checkpoints/state.json`及groups／contracts。
每个源CF、品种年和RIC都可恢复，不将本地running状态或源提取完成当作全部交付。
最终必须核对全部源Trade数量与所有生成／复用分组的原始Trade计数守恒，包括跨年边界。

状态按发布时间／源顺序直接合并，逐条解析并合并区间，详细session_status每8192行写入，
保留所有重复和原始Qualifiers。百万真实区间等容量边界超限即失败，不能截断继续。

## 状态和有效期边界

只使用LSEG的Trade、原始状态、kind=12控制事件和HRD，不读取Databento状态／definition、静态日历或CME公告。
CME原生阶段／原因码仅用于LSEG参考明确归属CME／CBOT／COMEX／NYMEX的RIC，
包括原十根以外的已核定CME品种；HG样本已确认原生15／17／4／21与同一字段组合。
其它交易所不照搬CME码：明确TRD_STATUS=H可建立HALTED，N只释放该标记，不证明开市。
未核定阶段或原因为UNKNOWN，辅助字段不清除明确停牌，缺包重置UNKNOWN直到后续明确阶段。

历史RIC保持原文及源年份，不凭一位数字推断交割年、合并历史后缀或跨十年复用身份。
HRD先以源内真实成交所处时间核对同一生命周期，多套到期候选不猜选。
只有身份与跨年生命期可核定时，才从上一年度同RIC预热状态及价格。
无可靠到期时，纯补行限制在最后已观察的LSEG状态／Report／成交证据处，
不能把没有交易的过期合约自动补到年末。任何真实成交都保留，参考到期冲突单列问题。
上市前辅助字段更新不建立K线覆盖，周日开市不被HRD周一交易日标签裁掉。

## 改撤证据边界

原237个2026 RIC沿用既有可信改撤账本，原始量和笔数与全库提取逐RIC核对。
新增历史／品种的codec没有保存完整唯一成交身份，不能仅因出现kind=12就自动扣量，
也不能把CAN_COND带的新价量当替代打印。缺少可信原成交关联的控制事件列为
`source_witness_pending`，本轮保留相关Trade；文件metadata明确原打印口径及待核计数。
因此本轮全库处理完成也不等于每条历史改撤都已最终净额化。

## 交付和验证

使用端仍为10列，OHLC为Float64、量和成交记录数为UInt64，`is_ffill`仅在实际补价时为true。
输出为`lseg/<exchange>/<year>/<RIC>/<YYYYMMDD>/kline.parquet`及`session_status.parquet`，
全库新文件metadata为`kline_status_policy=lseg_only_full_v1`；原237个文件保持原版本。
每RIC全量源量／笔数与生成K线守恒，每日文件轻量检查footer／schema／行数／RIC／UTC日期。
抽样仅首／中／末部分RIC及其首／中／末日完整读回，不全盘重复读取或哈希。
旧纯补行只有footer证明确无实际量和笔数才可清理。

当前真实问题位于`lseg/meta/problems/native_kline/`，旧问题保留在`full_tas_klines/`：未知状态区间、参考冲突、
未关联控制事件、归属与协议未核定、执行失败。正常成功不生成NAS audit或回执。
原油调查独立见[CL原油差异](cl_difference_20261112.md)，不以诊断删除迟报或大宗成交。
