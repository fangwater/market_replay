#!/usr/bin/env python3
"""Build a Chinese Excel checklist from the saved IB / raw TAS audit."""
import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
sys.path.insert(0, str(ROOT / "artifacts/python_deps"))
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter


PRODUCTS = {
    "AH": ("LME高等级原铝", "有色金属"),
    "BRR": ("CME比特币期货（5 BTC）", "数字资产"),
    "BZ": ("NYMEX布伦特原油（最后交易日型）", "能源"),
    "CA": ("LME铜／家乐福单股期货（代码混用）", "有色金属／单股期货"),
    "CC": ("纽约可可", "农产品"),
    "CL": ("WTI轻质原油", "能源"),
    "COIL": ("ICE布伦特原油", "能源"),
    "CT": ("2号棉花", "农产品"),
    "DAX": ("德国DAX40指数（含多种规格）", "股指"),
    "DX": ("美元指数", "外汇"),
    "ECF": ("欧盟碳排放配额（EUA）", "碳排放"),
    "ES": ("E-mini标普500指数", "股指"),
    "GBL": ("德国10年期国债（Bund）", "利率"),
    "GBM": ("德国5年期国债（Bobl）", "利率"),
    "GBS": ("德国2年期国债（Schatz）", "利率"),
    "GC": ("标准黄金（100盎司）", "贵金属"),
    "GF": ("饲牛", "农产品"),
    "GOIL": ("ICE低硫柴油", "能源"),
    "HE": ("瘦肉猪", "农产品"),
    "HG": ("COMEX标准铜", "有色金属"),
    "HO": ("纽约港燃油／超低硫柴油", "能源"),
    "JGB": ("日本10年期国债", "利率"),
    "KC": ("C型咖啡（阿拉比卡）", "农产品"),
    "LE": ("活牛", "农产品"),
    "M6E": ("微型欧元兑美元", "外汇"),
    "MES": ("微型E-mini标普500指数", "股指"),
    "MGC": ("微型黄金（10盎司）", "贵金属"),
    "MNK": ("微型美元计价日经225指数", "股指"),
    "MNQ": ("微型E-mini纳斯达克100指数", "股指"),
    "MYM": ("微型E-mini道琼斯指数", "股指"),
    "NG": ("亨利港天然气", "能源"),
    "NI": ("LME镍", "有色金属"),
    "NKD": ("CME美元计价日经225指数", "股指"),
    "NQ": ("E-mini纳斯达克100指数", "股指"),
    "PA": ("钯金", "贵金属"),
    "PB": ("LME铅", "有色金属"),
    "PL": ("铂金", "贵金属"),
    "QC": ("迷你铜（12,500磅）", "有色金属"),
    "QG": ("迷你天然气（2,500 MMBtu）", "能源"),
    "QO": ("迷你黄金（50盎司）", "贵金属"),
    "RB": ("RBOB汽油", "能源"),
    "RS": ("加拿大油菜籽", "农产品"),
    "SB": ("11号原糖", "农产品"),
    "SCI": ("SGX铁矿石", "黑色商品"),
    "SI": ("COMEX白银（代码内含不同规格）", "贵金属"),
    "SIC": ("白银（100盎司）", "贵金属"),
    "TFM": ("荷兰TTF天然气（欧元／MWh）", "能源"),
    "TSR20": ("SGX技术分类橡胶TSR20", "农产品"),
    "UC": ("SGX美元兑离岸人民币", "外汇"),
    "VIX": ("VIX波动率指数", "波动率"),
    "W": ("ICE白砂糖（5号糖）", "农产品"),
    "XIN0I": ("SGX富时中国H50指数", "股指"),
    "XINA50": ("SGX富时中国A50指数", "股指"),
    "YM": ("E-mini道琼斯工业平均指数", "股指"),
    "ZC": ("玉米", "农产品"),
    "ZF": ("美国5年期国债", "利率"),
    "ZL": ("豆油", "农产品"),
    "ZM": ("豆粕", "农产品"),
    "ZN": ("美国10年期国债", "利率"),
    "ZS": ("大豆", "农产品"),
    "ZSLME": ("LME特高级锌", "有色金属"),
    "ZT": ("美国2年期国债", "利率"),
    "ZW": ("芝加哥软红冬小麦", "农产品"),
}
EXCHANGES = {
    "CME": "芝加哥商业交易所", "CBOT": "芝加哥期货交易所",
    "COMEX": "纽约商品交易所（COMEX）", "NYMEX": "纽约商业交易所（NYMEX）",
    "LMEOTC": "伦敦金属交易所场外／远期市场", "NYBOT": "ICE美国期货市场（历史代码NYBOT）",
    "IPE": "ICE欧洲期货市场（历史代码IPE）", "EUREX": "欧洲期货交易所（Eurex）",
    "ENDEX": "ICE Endex能源交易所", "OSE.JPN": "大阪交易所",
    "SGX": "新加坡交易所", "CFE": "Cboe期货交易所",
    "ICEEUSOFT": "ICE欧洲软商品市场",
}
CME = {"CME", "CBOT", "COMEX", "NYMEX"}
STATUS = {
    "present_current_tas": "现有TAS已有",
    "present_supplement_source_not_current_db": "补充已下载，待入库",
    "not_found_same_instrument": "未找到同规格源",
}
COLORS = {"现有TAS已有": "E2F0D9", "补充已下载，待入库": "FFF2CC", "未找到同规格源": "FCE4D6"}
NOTES = {
    "BZ": "NYMEX最后交易日型布伦特与ICE布伦特不是同一合约，不能用LCO替代。",
    "CA": "IB同一代码下混有LME铜与Eurex家乐福单股期货，须分交易所和合约识别。",
    "DAX": "确认的是标准FDAX、乘数25；IB同代码下的迷你／微型规格未一并确认。",
    "SI": "确认标准SI、5,000盎司；IB代码下还混有SIL、1,000盎司，不能混用。",
    "VIX": "已确认VX月度产品；周度VXnn需逐合约核对。数量明细仅统计明确的VX月度根。",
    "W": "IB的W是ICE白砂糖，对应LSU；原始TAS中的W是CBOT小麦，对应IB的ZW。",
    "GOIL": "正确源为补充LGO；原批次G是英镑短期国债，不能当柴油。2026原始包已下载且MD5通过，尚未入库。",
    "NKD": "正确源为补充NK（CME、美元、乘数5）；原批次JNI是大阪、日元、乘数1,000，不能替代。补充包已下载，尚未入库。",
    "TSR20": "正确源为补充STF（SGX、5,000千克、美分报价）；原批次SINR是上期能源NR橡胶。补充包已下载，尚未入库。",
    "TFM": "IB为欧元／MWh；TAS的TFUBM为美元／10,000 MMBtu，规格不同，不能直接作为同一合约。",
    "UC": "IB为SGX美元／离岸人民币。CNC是CME产品，CNHZ9是韩国KOFEX产品，均不能替代。",
    "XIN0I": "SGX中国H50与A50（SFC）及港交所HCEI不同，不能替代。",
}
for _symbol in ("AH", "NI", "PB", "ZSLME"):
    NOTES[_symbol] = "未找到同一LME工具的对应TAS源；不能用其它交易所的同类金属替代。"
for _symbol in ("M6E", "MES", "MGC", "MNK", "MNQ", "MYM", "QC", "QG", "QO", "SIC"):
    NOTES[_symbol] = "未找到相同微型／迷你或独立规格的TAS源；不能用标准合约替代。"


def read_csv(path):
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def add_table(wb, title, headers, rows, widths, table_name, status_column=None, freeze="D5"):
    ws = wb.create_sheet(title)
    ws.sheet_view.showGridLines = False
    ws.sheet_view.zoomScale = 85
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(headers))
    ws.cell(1, 1, title).font = Font(name="微软雅黑", size=18, bold=True, color="17365D")
    ws.row_dimensions[1].height = 31
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=len(headers))
    ws.cell(2, 1, "核查日期：2026-11-03｜原始全品种TAS口径｜中文名称用于阅读，原始代码保持不变").font = Font(name="微软雅黑", size=10, color="666666")
    for col, value in enumerate(headers, 1):
        c = ws.cell(4, col, value)
        c.font = Font(name="微软雅黑", bold=True, color="FFFFFF", size=10)
        c.fill = PatternFill("solid", fgColor="17365D")
        c.alignment = Alignment(vertical="center", wrap_text=True)
    ws.row_dimensions[4].height = 31
    for row_number, values in enumerate(rows, 5):
        for col, value in enumerate(values, 1):
            cell = ws.cell(row_number, col, value)
            cell.font = Font(name="微软雅黑", size=10, color="243746")
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if isinstance(value, int):
                cell.number_format = "#,##0"
                cell.alignment = Alignment(vertical="top", horizontal="right")
        if status_column:
            cell = ws.cell(row_number, status_column)
            if cell.value in COLORS:
                cell.fill = PatternFill("solid", fgColor=COLORS[cell.value])
                cell.font = Font(name="微软雅黑", size=10, bold=True, color="243746")
        ws.row_dimensions[row_number].height = 58 if title != "补充下载3品种" else 102
    for col, width in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(col)].width = width
    end = 4 + len(rows)
    table = Table(displayName=table_name, ref=f"A4:{get_column_letter(len(headers))}{end}")
    table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
    ws.add_table(table)
    ws.freeze_panes = freeze
    ws.print_title_rows = "1:4"
    ws.print_options.horizontalCentered = True
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.page_setup.orientation = "landscape"
    ws.page_setup.paperSize = ws.PAPERSIZE_A3
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.print_area = f"A1:{get_column_letter(len(headers))}{end}"
    ws.sheet_properties.outlinePr.summaryRight = False
    return ws


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "reports/IB期货与原始TAS中文清单_20261103.xlsx")
    args = parser.parse_args()
    source = ROOT / "artifacts/ib_tas_all_futures_20261103"
    records = read_csv(source / "all_ib_futures_tas_matches.csv")
    summary = json.loads((source / "summary.json").read_text())
    download = json.loads((ROOT / "artifacts/tas_supplement_download_20261103/completion.json").read_text())
    assert download["status"] == "download_complete_verified"
    ingest_path = ROOT / "artifacts/tas_supplement_ingest_20261103/completion.json"
    ingest = json.loads(ingest_path.read_text()) if ingest_path.exists() else None
    merge_path = ROOT / "artifacts/tas_supplement_merge_20261103/completion.json"
    merged = json.loads(merge_path.read_text()) if merge_path.exists() else None
    if merged:
        assert merged["status"] == "complete_verified" and ingest
        ingest = dict(ingest, db=merged["target"])
    supplement_status = "已补入原TAS库" if merged else ("补充已入独立RocksDB" if ingest else "补充已下载，待入库")
    if ingest:
        assert ingest["status"] == "complete_verified"
        STATUS["present_supplement_source_not_current_db"] = supplement_status
        COLORS[supplement_status] = "E2F0D9"
        NOTES.update({
            "GOIL": "补充LGO已入独立库；原批次G是英镑短期国债，不能当柴油。",
            "NKD": "补充NK已入独立库（CME、美元、乘数5）；原批次JNI是大阪、日元、乘数1,000，不能替代。",
            "TSR20": "补充STF已入独立库（SGX、5,000千克、美分报价）；原批次SINR是上期能源NR橡胶。",
        })
        if merged:
            for symbol in ("GOIL", "NKD", "TSR20"):
                NOTES[symbol] = NOTES[symbol].replace("独立库", "原库")
    assert len(records) == 63 and {r["ib_symbol"] for r in records} == set(PRODUCTS)
    normalized = []
    for i, record in enumerate(records, 1):
        symbol = record["ib_symbol"]
        if ingest and symbol in ingest["targets"]:
            target = ingest["targets"][symbol]
            record.update(tas_2026_trade_rics=target["trade_rics"], tas_2026_quote_rics=target["quote_rics"],
                          tas_2026_trade_rows=target["kinds"].get("1", 0), tas_2026_quote_rows=target["kinds"].get("3", 0))
        exchange_codes = record["ib_exchanges"].split("|")
        is_cme = bool(set(exchange_codes) & CME)
        normalized.append(dict(record, number=i, chinese_name=PRODUCTS[symbol][0], category=PRODUCTS[symbol][1],
                               chinese_exchange="／".join(EXCHANGES[e] for e in exchange_codes),
                               cme="是" if is_cme else "否", chinese_status=STATUS[record["status"]],
                               chinese_note=NOTES.get(symbol, "确认有对应产品的原始TAS成交／报价；具体到期合约、日期和规格仍须逐项对齐。")))
    counts = Counter(r["chinese_status"] for r in normalized)
    assert dict(counts) == {"未找到同规格源": 19, "现有TAS已有": 41, supplement_status: 3}
    wb = Workbook()
    wb.remove(wb.active)
    wb.properties.title = "IB期货与原始TAS中文清单"
    wb.properties.subject = "63个IB期货代码的TAS覆盖与交易所分类"
    wb.properties.creator = "market_replay"
    wb.properties.description = "基于已保存的只读核查证据；不含数据库密码。"
    # Overview is deliberately numeric, so viewers do not need formula calculation.
    overview = add_table(wb, "清单概览", ["项目", "数量／结果", "说明"], [
        ["MySQL实际期货代码", 63, "minute_data实际有记录的不同symbol，sec_type均为fut；不是具体到期合约数。"],
        ["现有TAS已有", 41, "已确认对应产品的原始成交／报价；按2026现有已验收数据核对。"],
        [supplement_status, 3, "GOIL→LGO、NKD→NK、TSR20→STF；" + ("已合并进原库并完成校验。" if merged else ("已清洗写入独立补充库并完成校验。" if ingest else "合并原始包已下载并通过供应商MD5。"))],
        ["未找到同规格源", 19, "本次源范围未找到同交易所、同规格的对应产品。"],
        ["找到对应TAS源合计", 44, "41个当前已有＋3个补充包已有。"],
        ["未找到：属于CME Group", 11, "BZ、M6E、MES、MGC、MNK、MNQ、MYM、QC、QG、QO、SIC。"],
        ["未找到：其他交易所", 8, "AH、CA、NI、PB、TFM、UC、XIN0I、ZSLME。"],
        ["当前TAS源窗口", "2026-01-01 至 2026-06-01（右端不含）", "即当前2026年1—5月数据；不等同于完整2026年。"],
        ["补充TAS源窗口", "2026-01-01 至 2026-05-30（右端不含）", "三个目标产品位于同一futures_11合并包，含其它补充品种。"],
        ["补充文件合计", download["source_bytes"], "单位：字节，约15.0 GB；含Data、Report、Notes。"],
        ["关键口径", "品种存在性核查", "不以preprocess已处理的16品种限制范围；不代表逐合约、逐日、逐分钟完整覆盖。"],
    ], [29, 55, 104], "Overview")
    overview.freeze_panes = "A5"
    overview.row_dimensions[15].height = 72
    headers = ["序号", "IB代码", "中文品种名称", "资产类别", "IB交易所（中文）", "IB交易所代码", "是否CME Group", "TAS覆盖状态", "对应TAS根", "TAS原始RIC示例", "对应关系与注意事项"]
    def main_row(r):
        return [r["number"], r["ib_symbol"], r["chinese_name"], r["category"], r["chinese_exchange"], r["ib_exchanges"],
                r["cme"], r["chinese_status"], r["tas_root"] or "—", r["tas_example_ric"] or "—", r["chinese_note"]]
    add_table(wb, "全部63品种", headers, [main_row(r) for r in normalized],
              [7, 12, 37, 20, 42, 19, 15, 25, 15, 22, 85], "AllFutures", 8)
    missing = sorted([r for r in normalized if r["status"] == "not_found_same_instrument"], key=lambda r: (r["cme"] != "是", r["ib_symbol"]))
    add_table(wb, "未找到19品种", ["IB代码", "中文品种名称", "资产类别", "IB交易所（中文）", "IB交易所代码", "是否CME Group", "核查结果", "未匹配原因／注意事项"],
              [[r["ib_symbol"], r["chinese_name"], r["category"], r["chinese_exchange"], r["ib_exchanges"], r["cme"], r["chinese_status"], r["chinese_note"]] for r in missing],
              [13, 40, 22, 44, 22, 15, 25, 90], "MissingFutures", 7, "C5")
    supplemental = [r for r in normalized if r["status"] == "present_supplement_source_not_current_db"]
    data_file = next(r for r in download["files"] if r["name"] == "merged-Data.csv.gz")
    add_table(wb, "补充下载3品种", ["IB代码", "中文品种名称", "是否CME Group", "正确TAS根", "原始RIC示例", "下载与入库状态", "Report活跃RIC数", "Report源事件数", "数据所在目录", "校验结果", "注意事项"],
              [[r["ib_symbol"], r["chinese_name"], r["cme"], r["tas_root"], r["tas_example_ric"], supplement_status,
                download["target_products"][r["tas_root"]]["active_rics"], download["target_products"][r["tas_root"]]["source_event_rows"],
                (ingest["db"] if ingest else download["local_dir"]), "源大小、供应商MD5；入库核对通过" if ingest else "大小、ETag、供应商MD5均通过", r["chinese_note"]] for r in supplemental],
              [13, 34, 15, 15, 20, 32, 17, 23, 88, 34, 86], "SupplementFutures", None)
    add_table(wb, "记录数详情", ["IB代码", "中文品种名称", "TAS覆盖状态", "TAS根", "IB参考合约条数", "2026成交RIC数", "2026报价RIC数", "2026成交记录数", "2026报价记录数", "补充Report源事件数"],
              [[r["ib_symbol"], r["chinese_name"], r["chinese_status"], r["tas_root"] or "—", int(r["ib_metadata_contracts"]),
                int(r["tas_2026_trade_rics"]), int(r["tas_2026_quote_rics"]), int(r["tas_2026_trade_rows"]),
                int(r["tas_2026_quote_rows"]), int(r["supplement_report_rows"])] for r in normalized],
              [13, 40, 26, 15, 22, 20, 20, 25, 25, 28], "RecordCounts", 3, "C5")
    procurement_path = ROOT / "artifacts/tas_procurement_check_20261103/summary.json"
    procurement = json.loads(procurement_path.read_text()) if procurement_path.exists() else None
    if procurement:
        assert procurement["status"] == "verified_against_published_procurement_attachments"
        assert procurement["absent_from_published_purchase_list"] == 18
        assert procurement["purchased_but_mapping_or_spec_mismatch"] == ["TFM"]
        COLORS["采购清单未列入同规格产品"] = "FCE4D6"
        COLORS["采购已列入，交付规格待核查"] = "FFF2CC"
        add_table(wb, "缺失19项采购核对", ["IB代码", "中文品种", "IB交易所", "采购核对结论", "清单中相近／对应代码", "清单品种名", "清单交易所", "供应商链RIC", "缺失原因与注意事项"],
                  [[r["ib_symbol"], r["chinese_name"], r["ib_exchanges"], r["procurement_status"],
                    r["nearby_purchase_ticker"] or "—", r["nearby_purchase_name"] or "—",
                    r["nearby_purchase_exchange"] or "—", r["nearby_vendor_chain"] or "—", r["note"]]
                   for r in procurement["results"]],
                  [13, 40, 24, 36, 24, 28, 24, 25, 102], "ProcurementCheck", 4, "C5")
    methodology = [
        ["核查日期", "2026-11-03 UTC", "本文件按已保存的核查快照生成，没有重新查询或修改数据库。"],
        ["生成时间", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "工作簿含筛选、冻结窗格及状态颜色；代码大小写保持原值。"],
        ["MySQL数据源", "10.61.20.11:3306 / ib_data.minute_data", "原始symbol计数；contract_info用于交易所、币种、规格、local_symbol与身份核对。"],
        ["品种计数", "63个IB symbol", "一个symbol可能混有多种合约规格，例如DAX、SI、CA；不能将63理解成63种唯一完整规格。"],
        ["源范围", "全品种原始TAS及已发布补充TAS", "不以preprocess加工后的主力、HFQ或16品种清单代替源范围。"],
        ["当前数据", str(TAS_ROOT_PATH()), "2026已验收的全品种Trade／Quote导出；价格保留原定点精度。"],
        ["逐品种输入CSV", str(source / "all_ib_futures_tas_matches.csv"), "主清单逐项对应原核查结果；当前产品存在不代表所有到期合约和分钟都完整。"],
        ["原始身份信息", str(source / "raw_hrd_identity_witnesses.json"), "实际HRD的交易所、币种、Lot Size/Units、Description、Ticker优先于采购名称猜测。"],
        ["补充下载回执", str(ROOT / "artifacts/tas_supplement_download_20261103/completion.json"), "合并包中同时包含其它补充品种，不是三个单独的数据文件。"],
        ["补充Data MD5", data_file["md5"], "与供应商Notes中的Extraction MD5一致；" + ("经独立库验收后已合入原库。" if merged else ("整包已重放入独立RocksDB。" if ingest else "尚未逐行清洗、重放或写入RocksDB。"))],
        ["补充Data SHA256", data_file["sha256"], "本地完整压缩文件SHA256。"],
        ["IB参考合约条数", "contract_info中相应symbol的元数据条数", "不等于minute_data实际有数据的合约数，不等于可与TAS比较的合约数。"],
        ["TAS成交／报价RIC数", "当前明确TAS根下、各事件类型的RIC数量", "两个类型的RIC集合会重叠，不能相加作为唯一合约数；VIX仅计明确的VX月度根。"],
        ["补充Report事件数", "供应商Report的全部源事件Count", "包含多种事件类型，不是成交条数，也不是已经入库的条数。"],
        ["空白计数的表达", "未找到及未入库行在记录明细中显示0", "表示本次现有库／对应TAS根未计得该类记录，不表示全世界没有该产品。"],
        ["禁止直接替代", "标准／微型、不同交易所、不同结算币种和单位", "举例：G不能作GOIL、JNI不能作NKD、SINR不能作SGX TSR20、TFUBM不能直接作IB TFM。"],
        ["后续质量比较", "仍需核对具体合约、日期、交易时段、价格与数量单位", "此次清单没有对OHLC或成交量差异作验收结论。"],
    ]
    if ingest:
        methodology.append(["补充入库回执", str(ingest_path), ("独立库为清洗验收基线；数据已合入原库，逐字节核对通过。" if merged else "原始生产库只读；补充目标的成交、报价计数取自完整持久化扫描，未生成补充Parquet。")])
        methodology.append(["补充RocksDB", ingest["db"], "GOIL／NKD／TSR20的记录数来自本库；Quote沿用按秒BBO采样口径。"])
    if merged:
        methodology.append(["原库合并回执", str(merge_path), "13个新列族已追加；5个已有列族的完全一致重复记录已跳过，原有行情和period水位均不变。"])
        methodology.append(["原库回退快照", merged["checkpoint"], "写入前通过RocksDB Checkpoint创建，同文件系统SST使用硬链接。"])
    if procurement:
        methodology.extend([
            ["采购数据集页面", procurement["dataset_url"], "已实际读取公开采购附件；18项未列入同规格采购，TFM列入但映射／交付规格待核查。"],
            ["采购品种清单", procurement["purchase_list_url"], "页面称120项，CSV预览正文实际121项；比供应商映射表多AFR，不影响本次19项结论。"],
            ["供应商RIC映射", procurement["vendor_mapping_url"], "120项；TFM→0#TFUBM:。采购范围判断按交易所、名称和规格核对，不只按ticker字符串。"],
            ["采购核查边界", str(procurement_path), "采购附件是本次公布范围的证据；没有核对发票或付款记录，不据此断言其它采购合同从未购买。"],
        ])
    add_table(wb, "口径与数据来源", ["项目", "内容", "说明"], methodology, [28, 106, 116], "Methodology", None, "A5")
    wb.active = 0
    for ws in wb:
        ws.sheet_properties.tabColor = "17365D"
    wb["未找到19品种"].sheet_properties.tabColor = "C0504D"
    wb["补充下载3品种"].sheet_properties.tabColor = "E0A800"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    wb.save(args.output)
    # Read the actual XLSX back, checking values, groups and downloaded state.
    check = load_workbook(args.output, data_only=True)
    table_rows = list(check["全部63品种"].iter_rows(min_row=5, values_only=True))
    assert len(table_rows) == 63
    assert {r[1] for r in table_rows} == set(PRODUCTS)
    assert Counter(r[7] for r in table_rows) == counts
    missing_rows = list(check["未找到19品种"].iter_rows(min_row=5, values_only=True))
    assert len(missing_rows) == 19 and Counter(r[5] for r in missing_rows) == {"是": 11, "否": 8}
    supplements = list(check["补充下载3品种"].iter_rows(min_row=5, values_only=True))
    assert len(supplements) == 3 and {r[0] for r in supplements} == {"GOIL", "NKD", "TSR20"}
    if procurement:
        purchase_rows = list(check["缺失19项采购核对"].iter_rows(min_row=5, values_only=True))
        assert len(purchase_rows) == 19
        assert Counter(r[3] for r in purchase_rows) == {"采购清单未列入同规格产品": 18, "采购已列入，交付规格待核查": 1}
        tfm = next(r for r in purchase_rows if r[0] == "TFM")
        assert tfm[7] == "0#TFUBM:" and "EUR/MWh" in tfm[8]
    for ws in check:
        assert ws.freeze_panes and len(ws.tables) == 1
    print(json.dumps(dict(output=str(args.output.resolve()), bytes=args.output.stat().st_size,
                          sheets=check.sheetnames, symbols=len(table_rows), status_counts=dict(counts),
                          missing_cme_group=11, missing_other_exchanges=8, validation="XLSX读回及数量核对通过"), ensure_ascii=False, indent=2))


def TAS_ROOT_PATH():
    return Path("/mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/2026/_audit/ric_inventory.csv")


if __name__ == "__main__":
    main()
