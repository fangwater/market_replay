# IB 缺失 19 项与公布采购范围

核查时间：2026-11-03 UTC。已实际打开用户提供的 [DataMart 数据集](http://10.61.20.64:5201/market/dataset/lseg_waipan_data)，读取两份采购附件，再结合既有 IB 合约身份和实际交付 HRD 核对。

结论：19 个当前未找到同规格 TAS 的 IB 代码中，**18 个未列入此次公布的同规格采购范围，TFM 已列入采购但供应商映射／交付规格不对应**。不能把 19 个都解释为没买，也不能把未列入本次附件扩大为其它采购合同从未购买。

## 附件依据

- [采购品种 CSV 预览](http://10.61.20.64:5201/datasets/attachments/27/preview)：`symbols-global-survey-liuyang.csv`，页面说明“采购120个品种列表”。已保存预览 HTML，并提取原始 CSV 正文用于逐项核查。
- [供应商品种—RIC 映射](http://10.61.20.64:5201/datasets/attachments/37/preview)：`Futues_List_RIC_Mapping.xlsx`，本次 HTTP 返回实际 XLSX。SHA256 为 `596cd8c7a0c3cf535bcd47aa98492134131fd6d71b0e4db20748f0d32f099a36`，与 preprocess 留存附件相同。
- CSV 实际有 121 个条目，映射表有 120 个；仅 CSV 多出 AFR，均不影响此次缺失 19 项的结论。

## 18 个采购清单未列入的产品

| 分类 | IB 代码 | 附件中的相近采购产品与区别 |
| --- | --- | --- |
| 微型／迷你及独立规格，10 项 | M6E、MES、MGC、MNK、MNQ、MYM、QC、QG、QO、SIC | 买了标准 Euro FX、E-mini 股指、标准黄金／铜／天然气／白银等；未列这些具体规格 |
| NYMEX 布伦特，1 项 | BZ | 附件的 BRN 是 EU-ICE，映射 LCO；不是 NYMEX Brent Last Day |
| LME 系列，5 项 | AH、CA、NI、PB、ZSLME | 附件金属主要为 COMEX／NYMEX；COMEX 铝／铜不等于 LME 铝／铜。CA 在 IB 还混有 EUREX 家乐福单股期货，附件亦未列入 |
| SGX 工具，2 项 | UC、XIN0I | 买的是 CME 6H（CNH/USD）和 SGX CN（中国 A50）；不是 SGX USD/CNH 和中国 H50 |

以上通过名称、交易所、规格和映射链交叉检查，不是仅因采购 ticker 与 IB symbol 不同就判未采购。比如已确认覆盖的 BRR→采购 BTC、COIL→采购 BRN，始终按实际工具身份匹配。

## TFM 已列入采购，应核对映射／交付

采购 CSV 与供应商映射均明确列出：

```text
荷兰TTF天然气 | TFM | ICE-ENDEX | Dutch TTF Natural Gas Futures
供应商 Chain RIC: 0#TFUBM:
```

实际身份对照：

| 依据 | 交易所／MIC | 币种 | 规格／单位 |
| --- | --- | --- | --- |
| IB TFM 合约元数据 | ENDEX | EUR | 欧元／MWh，乘数随交割月小时数变化 |
| 实际交付 TFUBM 的 HRD，例如 TFUBMF0 | IFEU | USD | Lot Size 10,000，Lot Units MMBTU，Ticker TFU |

因此 TFM 属于“采购已列入，但当前映射／交付工具与 IB 目标规格不匹配”，应核对采购约定、供应商链映射及交付范围。当前不把 TFUBM 直接当作已覆盖 IB TFM；也不归为未采购。

原始身份记录及版本见 [核查 JSON](../artifacts/tas_procurement_check_20261103/summary.json) 的 `tfm_evidence`。公布采购范围不替代付款凭证，本次没有核查合同或发票。

## 交付和复现

- [逐项采购核查 CSV](../artifacts/tas_procurement_check_20261103/missing19_procurement.csv)
- [中文 Excel](../reports/IB期货与原始TAS中文清单_20261103.xlsx) 新增“缺失19项采购核对”页，列出相近采购 ticker、交易所、链 RIC 和具体区别。
- 证据目录：`artifacts/tas_procurement_check_20261103/`，保存页面、附件及输入 SHA256。
- 基于已保存附件复核：`python scripts/check_tas_procurement.py`。

数据集页面的 raw／rawLL2 下载进度为历史描述；本次采购范围判断使用附件及实际交付证据，不将页面进度当作当前库状态。
