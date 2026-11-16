# MySQL 全部期货与原始 TAS 的对应关系

核查日期：2026-11-03 UTC。用户明确要求以 **MySQL 实际期货集合对照全品种原始 TAS**，不以 preprocess 已处理的 16 品种为筛选范围。

## 数量与结论

`ib_data.minute_data` 实际有 **63 个不同 symbol**，其 `sec_type` 均为 `fut`。这是代码层面的品种计数，不是 con_id / 到期合约数；部分 symbol 内还混有不同合约规格。

| 结果 | IB symbol 数 | 证据 |
| --- | ---: | --- |
| 当前 TAS 已有对应产品的 Trade / Quote | 41 | 已验收的 2026 年全品种导出与实际 HRD 身份信息 |
| 补充 TAS 已合并进入原库 | 3 | 完整源包、HRD、独立清洗验收及原库写后校验 |
| 本次源范围未找到同交易所、同规格的对应产品 | 19 | 不将相似标的、微型规格、不同交易所或不同结算产品替换为同一工具 |

因此，**44 / 63 个 IB 代码找到了对应的 TAS 源，41 个在原库、另 3 个补充产品现已合并进入同一座原库**。这不是逐合约、逐日或逐分钟覆盖率。

当前 TAS 记录范围为 `2026-01-01_2026-06-01`；补充源范围为 `2026-01-01_2026-05-30`。原批次历史 Report 清单也已检查，不能将补充末期窗口混称为完整 H1。初次身份核查为只读；后续补充整包先在独立库验收，再按用户要求将新增列族合入原库，原有行情保持不变，见 [补充 TAS 合并](tas_supplement_merge.md)。

## 当前 TAS 已有的 41 个 IB 代码

```text
BRR CC CL COIL CT DAX DX ECF ES GBL GBM GBS GC GF HE HG HO
JGB KC LE NG NQ PA PL RB RS SB SCI SI VIX W XINA50 YM
ZC ZF ZL ZM ZN ZS ZT ZW
```

部分关键映射：

| IB symbol | 原始 TAS 根 | 含义 |
| --- | --- | --- |
| BRR | BTC | CME 比特币，5 BTC 规格 |
| COIL | LCO | ICE 布伦特原油 |
| DAX | FDX | 标准 DAX，乘数 25；不覆盖 IB 同代码下所有 mini/micro 规格 |
| ECF | CFI2 | EUA 碳排放配额 |
| GBL / GBM / GBS | FGBL / FGBM / FGBS | Eurex 德债 |
| GF / HE / LE | FC / LH / LC | 饲牛 / 瘦肉猪 / 活牛 |
| SCI | SZZF | SGX 铁矿石，实际参考 ticker 为 FEF |
| VIX | VX 及相应周度根 | 已明确确认 VX 月度产品；周度合约需按各自原始 RIC 继续匹配 |
| W | LSU | ICE 白砂糖；原始 TAS 的 W 是 CBOT 小麦 |
| XINA50 | SFC | SGX A50 |
| ZC / ZS / ZL / ZM / ZW | C / S / BO / SM / W | 玉米 / 大豆 / 豆油 / 豆粕 / 小麦 |
| ZT / ZF / ZN | TU / FV / TY | 美债 2 / 5 / 10 年 |

SI 仅确认标准 5,000 盎司规格；IB `symbol=SI` 下还有 `SIL...` / 1,000 盎司，不能按 symbol 全部混为同一源。逐代码的原始 RIC 实例、Trade/Quote 数和数据量在 [63 项 CSV](../artifacts/ib_tas_all_futures_20261103/all_ib_futures_tas_matches.csv)。VIX 的 CSV 计数仅列明确 VX 根的月度记录，不冒充所有周度代码合计。

## 已补入原 TAS 库的三个

| IB symbol | 正确补充 TAS 根 | HRD 身份 |
| --- | --- | --- |
| GOIL | LGO | ICE USD 柴油，100 吨；如 LGOH6 |
| NKD | NK | CME USD 日经，乘数 5；如 NKH26 |
| TSR20 | STF | SGX SICOM TSR20，5,000 kg、以美分报价；如 STFF6 |

源位于 `future_supplement/normalised/futures_11_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30/`。已只读核对该目录的正式 `merged-Data.csv.gz`（14,980,973,366 字节）、Report 和补充 HRD；Report 中 LGO / NK / STF 的 Count 分别为 318,093,502 / 88,865,994 / 54,178,487。

这些 Count 是所有 TAS 源事件数，不是 Trade 数；实际持久化成交、报价数另见补充入库审计。

2026-11-03 已完成下载与供应商 MD5 校验，后续按用户要求将整包 710,632,866 源行清洗入独立 RocksDB，并完成 318 个活跃 RIC 的全量持久化校验。库路径、目标计数和完成回执见 [补充 TAS 入库](tas_supplement_rocksdb.md)；[下载记录](tas_supplement_download.md) 保留原始文件来源。后续已将 13 个全新列族追加进原库，5 个重叠列族全部为重复数据，跳过；见 [合并验收](tas_supplement_merge.md)。早期 63 项 CSV 是入库前身份核查快照，最新中文 Excel 已同步原库合并状态及实际计数。

## 当前未找到等价源的 19 个

```text
AH BZ CA M6E MES MGC MNK MNQ MYM NI PB
QC QG QO SIC TFM UC XIN0I ZSLME
```

- AH / CA / NI / PB / ZSLME 的 LME 产品不能用 COMEX 金属替代；CA 的 IB 元数据还混有 Eurex 单股期货，不能只按 CA 名称合并。
- M6E / MES / MGC / MNK / MNQ / MYM 与 QC / QG / QO / SIC 是不同规格或产品，不能用标准合约当作已存在的相同 TAS 源。
- BZ 是 NYMEX Brent Last Day，不是 ICE LCO / IB COIL。
- XIN0I 是 SGX China H50，不能用 SFC A50 或 HKFE HCEI 代替。
- UC 是 SGX USD/CNH；原始 `CNHZ9` 的 HRD 却是 KOFEX、KRW 报价，`CNC` 是 CME 的另一产品，均不能作为同一合约。
- TFM 的 IB 产品为 ENDEX EUR / MWh。原始 `TFUBM` 是 USD、10,000 MMBTU 的 TTF 产品，不能仅因都叫荷兰天然气就作为同一合约。

这 19 个代码并非都在 CME Group 之外。按实际 IB 合约元数据的 exchange 分类：

| 交易所集团 | 数量 | IB symbol |
| --- | ---: | --- |
| CME Group（CME / CBOT / COMEX / NYMEX） | 11 | BZ、M6E、MES、MGC、MNK、MNQ、MYM、QC、QG、QO、SIC |
| 其它交易所 | 8 | AH、CA、NI、PB、TFM、UC、XIN0I、ZSLME |

CME Group 中缺少的多为微型、迷你或不同结算规格，源包的固定采购范围不等于整个交易所集团覆盖。CA 的实际元数据同时有 LMEOTC / EUREX 产品，两者均非 CME Group。[19 项交易所分类 CSV](../artifacts/ib_tas_all_futures_20261103/missing_tas_exchange_groups.csv)保留逐项结果。

随后已实际核对用户提供的 DataMart 采购附件：上述 19 项中，18 个未列入本次同规格采购范围；TFM 则明确列入采购，但供应商映射 `0#TFUBM:` 对应的实际交付与 IB 合约规格不同。因此不能将这 19 项都归为未采购，详见 [采购范围核查](tas_procurement_scope.md)。中文 Excel 已新增逐项采购核对页。

## 已纠正的原批次身份误配

采购附件里的 ticker / Chain RIC 与原批次实际展开结果有出入。此次使用实际原始 HRD 的 Market MIC、币种、Lot Size/Units、Security Description、Ticker 交叉确认：

| 不能采用的映射 | 实际原始身份 |
| --- | --- |
| GOIL → G | GH6 是 GBP、100,000 英镑面值的英国 Short Gilt，非柴油 |
| NKD → JNI | JNIH6 是 OSE、JPY、乘数 1,000 的日经期货，非 CME USD / 5 |
| TSR20 → SINR | SINRF6 是 INE、CNY、10 吨的 NR 橡胶，非 SGX SICOM |
| TFM → TFUBM | 报价币种、数量单位及合约规格不同 |

“同名根存在”和“是 IB 的同一个合约”必须分别验证。参考信息变更跨历史版本，不能用采购 ticker 或最新代码对所有历史版本强行覆盖。

## 证据与复现

- [逐代码结果 CSV](../artifacts/ib_tas_all_futures_20261103/all_ib_futures_tas_matches.csv)
- [数量汇总及输入 SHA256](../artifacts/ib_tas_all_futures_20261103/summary.json)
- [原始 HRD 身份证据](../artifacts/ib_tas_all_futures_20261103/raw_hrd_identity_witnesses.json)
- [IB 全部期货元数据快照](../artifacts/ib_tas_all_futures_20261103/ib_futures_snapshot.json)
- [补充源 HRD 与来源哈希](../artifacts/ib_tas_all_futures_20261103/supplement_hrd_evidence.json)
- [补充源正式 Data HEAD](../artifacts/ib_tas_all_futures_20261103/supplement_data_head.json)
- [查询和匹配程序](../scripts/match_ib_all_futures_tas.py)

原始 HRD 为 `/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv`。本次只读使用该供应商原始参考文件；不读取 preprocess 加工后的主力 / HFQ 作为 TAS 源存在性的依据。
