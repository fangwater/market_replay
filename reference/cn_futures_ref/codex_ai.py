"""Use the current local Codex provider credentials for notice analysis."""

import base64
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover - Python 3.8-3.10 compatibility
    import tomli as tomllib

from .models import DataError


MODEL = "gpt-5.6-sol"
REASONING_EFFORT = "high"


@dataclass(frozen=True)
class CodexSettings:
    base_url: str
    api_key: str
    provider: str

    @property
    def responses_url(self):
        value = self.base_url.rstrip("/")
        return value if value.endswith("/responses") else value + "/responses"


def _codex_home():
    configured = os.environ.get("CN_FUTURES_REF_CODEX_HOME") or os.environ.get("CODEX_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".codex"


def load_settings():
    root = _codex_home()
    config_path, auth_path = root / "config.toml", root / "auth.json"
    try:
        with config_path.open("rb") as handle:
            config = tomllib.load(handle)
        auth = json.loads(auth_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise DataError("无法读取当前 Codex API 配置：%s" % error)
    provider_name = str(config.get("model_provider") or "OpenAI")
    providers = config.get("model_providers") or {}
    provider = providers.get(provider_name) or providers.get("OpenAI") or {}
    base_url = str(provider.get("base_url") or "").strip()
    api_key = str(auth.get("OPENAI_API_KEY") or "").strip()
    if not base_url:
        raise DataError("当前 Codex model provider 未配置 base_url")
    if not api_key:
        raise DataError("当前 Codex auth.json 未配置 OPENAI_API_KEY")
    return CodexSettings(base_url=base_url, api_key=api_key, provider=provider_name)


def configuration_status():
    try:
        settings = load_settings()
        return {
            "available": True,
            "provider": settings.provider,
            "baseUrl": settings.base_url,
            "model": MODEL,
            "reasoningEffort": REASONING_EFFORT,
        }
    except DataError as error:
        return {
            "available": False,
            "model": MODEL,
            "reasoningEffort": REASONING_EFFORT,
            "error": str(error),
        }


RULE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "relevance": {
            "type": "string",
            "enum": ["MESSAGE_FEE", "RELATED", "NOT_RELEVANT"],
        },
        "summary": {"type": "string"},
        "change_summary": {"type": "string"},
        "requires_manual_review": {"type": "boolean"},
        "uncertainties": {"type": "array", "items": {"type": "string"}},
        "rules": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "rule_id": {"type": "string"},
                    "exchange": {
                        "type": "string",
                        "enum": ["CFFEX", "SHFE", "INE", "DCE", "CZCE", "GFEX"],
                    },
                    "products": {"type": "array", "items": {"type": "string"}},
                    "instrument_type": {
                        "type": "string", "enum": ["FUTURE", "OPTION", "BOTH"],
                    },
                    "effective_from": {"type": "string"},
                    "effective_to": {"type": ["string", "null"]},
                    "aggregation_scope": {"type": "string"},
                    "information_volume": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "new_order": {"type": "number"},
                            "cancel": {"type": "number"},
                            "inquiry": {"type": "number"},
                            "other": {"type": "number"},
                        },
                        "required": ["new_order", "cancel", "inquiry", "other"],
                    },
                    "otr_formula": {"type": "string"},
                    "otr_threshold": {"type": "number"},
                    "zero_fill_denominator": {"type": "integer"},
                    "tiers": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "min": {"type": "integer"},
                                "max": {"type": ["integer", "null"]},
                                "rate_otr_le_threshold": {"type": "string"},
                                "rate_otr_gt_threshold": {"type": "string"},
                            },
                            "required": [
                                "min", "max", "rate_otr_le_threshold",
                                "rate_otr_gt_threshold",
                            ],
                        },
                    },
                    "exemptions": {"type": "array", "items": {"type": "string"}},
                    "evidence": {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    "rule_id", "exchange", "products", "instrument_type",
                    "effective_from", "effective_to", "aggregation_scope",
                    "information_volume", "otr_formula", "otr_threshold",
                    "zero_fill_denominator", "tiers", "exemptions", "evidence",
                ],
            },
        },
    },
    "required": [
        "relevance", "summary", "change_summary", "requires_manual_review",
        "uncertainties", "rules",
    ],
}


INSTRUCTIONS = """你是中国期货交易所期货申报费规则审校助手。只根据给定公告正文和附件提取规则，不得使用记忆补全缺失值。
只分析期货规则，纯期权公告标记为 NOT_RELEVANT；公告同时涉及期货和期权时，只提取期货部分，不生成 OPTION 规则。
重点识别期货申报费的适用品种、生效日、统计粒度、信息量定义、OTR 公式、无成交分母、分档费率和豁免。
费率表不完整、图片不可读、适用范围不明确或生效日缺失时，不要生成可执行规则；把问题写入 uncertainties。
products 使用交易品种代码并大写；effective_from/effective_to 使用 YYYY-MM-DD；无结束日用 null。
公告明确按交易日、客户和单个合约统计时，aggregation_scope 必须返回固定值 trading_day+account+instrument。
费率字段只返回不带“元/笔”等单位的十进制数字字符串。
每条可执行规则都必须给出来自公告的简短 evidence。候选规则始终需要人工审核。"""


def _input_content(notice, assets):
    text = (
        "交易所：{venue}\n标题：{title}\n发布日期：{date}\n来源：{url}\n"
        "正文：\n{body}"
    ).format(
        venue=notice["venue"], title=notice["title"],
        date=notice.get("published_date") or "未知", url=notice["source_url"],
        body=(notice.get("extracted_text") or "")[:60000],
    )
    content = [{"type": "input_text", "text": text}]
    for asset in assets[:10]:
        raw = bytes(asset["raw_content"])
        content_type = str(asset.get("content_type") or "application/octet-stream")
        encoded = base64.b64encode(raw).decode("ascii")
        if content_type in ("image/png", "image/jpeg", "image/webp", "image/gif"):
            content.append({
                "type": "input_image",
                "image_url": "data:%s;base64,%s" % (content_type, encoded),
            })
        elif content_type == "application/pdf" or str(asset.get("file_name", "")).lower().endswith(".pdf"):
            content.append({
                "type": "input_file",
                "filename": asset.get("file_name") or "attachment.pdf",
                "file_data": "data:application/pdf;base64,%s" % encoded,
            })
    return content


def build_request(notice, assets):
    return {
        "model": MODEL,
        "reasoning": {"effort": REASONING_EFFORT},
        "store": False,
        "instructions": INSTRUCTIONS,
        "input": [{"role": "user", "content": _input_content(notice, assets)}],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "message_fee_notice_analysis",
                "strict": True,
                "schema": RULE_SCHEMA,
            }
        },
        "max_output_tokens": 12000,
    }


def _output_text(response):
    if isinstance(response.get("output_text"), str):
        return response["output_text"]
    values = []
    for item in response.get("output") or []:
        if item.get("type") != "message":
            continue
        for part in item.get("content") or []:
            if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                values.append(part["text"])
    return "".join(values)


def analyze_notice(notice, assets, timeout=240):
    settings = load_settings()
    payload = json.dumps(build_request(notice, assets), ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(settings.responses_url, data=payload, method="POST", headers={
        "Authorization": "Bearer %s" % settings.api_key,
        "Content-Type": "application/json",
        "User-Agent": "cn-futures-reference/0.4.0",
    })
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(8 * 1024 * 1024)
    except urllib.error.HTTPError as error:
        detail = error.read(4096).decode("utf-8", "replace")
        raise DataError("Codex Responses API 返回 HTTP %s：%s" % (error.code, detail))
    except (urllib.error.URLError, OSError) as error:
        raise DataError("Codex Responses API 请求失败：%s" % error)
    try:
        response = json.loads(body.decode("utf-8"))
        output = _output_text(response).strip()
        if output.startswith("```"):
            output = output.split("\n", 1)[1].rsplit("```", 1)[0]
        analysis = json.loads(output)
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, IndexError) as error:
        raise DataError("Codex Responses API 未返回有效的结构化分析：%s" % error)
    if not isinstance(analysis, dict) or not isinstance(analysis.get("rules"), list):
        raise DataError("Codex Responses API 分析结果结构无效")
    return analysis, response.get("id")
