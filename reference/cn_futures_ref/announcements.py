"""Manual collection of exchange notices and their original attachments."""

import mimetypes
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import PurePosixPath
from xml.etree import ElementTree

from bs4 import BeautifulSoup

from .models import DataError
from . import storage


NOTICE_KEYWORDS = (
    "申报费",
    "调整申报费",
    "收取申报费",
    "上市交易有关事项",
    "手续费收取标准",
    "交易手续费标准",
    "结算参数",
)

MAX_LIST_BYTES = 4 * 1024 * 1024
MAX_NOTICE_BYTES = 16 * 1024 * 1024
MAX_ASSET_BYTES = 12 * 1024 * 1024
MAX_ASSETS_PER_NOTICE = 12


@dataclass(frozen=True)
class ExchangeSource:
    venue: str
    list_url: str
    detail_pattern: str
    page_pattern: str = ""

    def pages(self, page_count):
        yield self.list_url
        for index in range(2, page_count + 1):
            if self.page_pattern:
                yield self.page_pattern.format(index=index, offset=index - 1)


SOURCES = (
    ExchangeSource(
        "CFFEX",
        "http://www.cffex.com.cn/cn/jystz.html",
        r"/(?:cn/)?jystz/20\d{6}/\d+\.html$",
        "http://www.cffex.com.cn/cn/jystz_{index}.html",
    ),
    ExchangeSource(
        "SHFE",
        "https://www.shfe.com.cn/publicnotice/notice/",
        r"/publicnotice/notice/20\d{4}/t20\d{6}_\d+\.html$",
        "https://www.shfe.com.cn/publicnotice/notice/index_{offset}.html",
    ),
    ExchangeSource(
        "INE",
        "https://www.ine.cn/publicnotice/notice/",
        r"/publicnotice/notice/20\d{4}/t20\d{6}_\d+\.html$",
        "https://www.ine.cn/publicnotice/notice/index_{offset}.html",
    ),
    ExchangeSource(
        "DCE",
        "http://www.dce.com.cn/dalianshangpin/ywfw/jystz/ywtz/index.html",
        r"/dalianshangpin/.+/(?:webinfo/)?20\d{2}/.+\.(?:html?|shtml)$",
    ),
    ExchangeSource(
        "CZCE",
        "https://www.czce.com.cn/cn/jysdt/jysgg/H770301index_1.htm",
        r"/cn/.+(?:webinfo|content)/20\d{2}/.+\.html?$",
    ),
    ExchangeSource(
        "GFEX",
        "http://www.gfex.com.cn/gfex/tzts/list_yw.shtml",
        r"/gfex/tzts/20\d{4}/[a-f0-9]+\.shtml$",
        "http://www.gfex.com.cn/gfex/tzts/list_yw_{offset}.shtml",
    ),
)

# These baseline reference notices have moved far beyond the rolling list window.
# Keeping their official URLs here makes a manual refresh deterministic.
BASELINE_NOTICES = (
    {
        "venue": "SHFE",
        "title": "上海期货交易所关于调整申报费收费标准的通知",
        "url": "https://www.shfe.com.cn/publicnotice/notice/202405/t20240522_801753.html",
        "published_date": "2024-05-22",
    },
    {
        "venue": "INE",
        "title": "上海国际能源交易中心关于调整申报费收费标准的通知",
        "url": "https://www.ine.cn/publicnotice/notice/202405/t20240522_814831.html",
        "published_date": "2024-05-22",
    },
    {
        "venue": "DCE",
        "title": "关于对部分期货、期权合约收取申报费的通知",
        "url": "http://www.dce.com.cn/dalianshangpin/ywfw/jystz/ywtz/8605414/index.html",
        "published_date": "2024-07-19",
    },
    {
        "venue": "CZCE",
        "title": "关于对部分品种交易收取申报费的通知",
        "url": "https://www.czce.com.cn/cn/gyjys/jysdt/ggytz/webinfo/2024/07/1715237246527911.htm",
        "published_date": "2024-07-19",
    },
)


def _uri(url):
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((
        parts.scheme,
        parts.netloc.encode("idna").decode("ascii"),
        urllib.parse.quote(urllib.parse.unquote(parts.path), safe="/%:@"),
        urllib.parse.quote(urllib.parse.unquote(parts.query), safe="=&?/:@,+"),
        "",
    ))


def fetch(url, timeout=25, retries=1, max_bytes=MAX_NOTICE_BYTES):
    errors = []
    for attempt in range(retries + 1):
        request = urllib.request.Request(_uri(url), headers={
            "Accept": "text/html,application/pdf,image/*,application/octet-stream,*/*;q=0.5",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": urllib.parse.urlunsplit((*urllib.parse.urlsplit(url)[:2], "/", "", "")),
            "User-Agent": "Mozilla/5.0 cn-futures-reference/0.4.0",
        })
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                content = response.read(max_bytes + 1)
                if len(content) > max_bytes:
                    raise DataError("response exceeds %d bytes" % max_bytes)
                if not content:
                    raise DataError("source returned an empty response")
                content_type = response.headers.get_content_type() or "application/octet-stream"
                return content, content_type, response.geturl()
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, DataError) as error:
            errors.append(str(error))
            if attempt < retries:
                time.sleep(min(2 ** attempt, 4))
    raise DataError("failed to fetch %s: %s" % (url, "; ".join(errors)))


def _decode_html(content, content_type=""):
    encodings = []
    match = re.search(r"charset=([A-Za-z0-9_-]+)", content_type, re.I)
    if match:
        encodings.append(match.group(1))
    detected = BeautifulSoup(content[:8192], "html.parser").original_encoding
    if detected:
        encodings.append(detected)
    encodings.extend(("utf-8-sig", "utf-8", "gb18030"))
    for encoding in dict.fromkeys(encodings):
        try:
            return content.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    raise DataError("could not decode exchange notice HTML")


def _clean_text(value):
    lines = []
    for line in str(value or "").replace("\xa0", " ").splitlines():
        normalized = " ".join(line.split())
        if normalized and (not lines or normalized != lines[-1]):
            lines.append(normalized)
    return "\n".join(lines)


def _date_from(value):
    match = re.search(r"(20\d{2})[-年/]?(\d{2})[-月/]?(\d{2})", str(value or ""))
    if not match:
        return None
    try:
        return datetime.strptime("-".join(match.groups()), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _relevant(title):
    compact = "".join(str(title or "").split())
    return is_futures_notice(compact) and any(
        keyword in compact for keyword in NOTICE_KEYWORDS
    )


def is_futures_notice(title):
    """Keep mixed notices with futures content, but ignore option-only notices."""
    compact = "".join(str(title or "").split())
    return "期权" not in compact or "期货" in compact


def discover_notices(source_spec, html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    notices, seen = [], set()
    pattern = re.compile(source_spec.detail_pattern, re.I)
    for anchor in soup.find_all("a", href=True):
        href = urllib.parse.urljoin(base_url, anchor.get("href"))
        path = urllib.parse.urlsplit(href).path
        if not pattern.search(path):
            continue
        title = _clean_text(anchor.get("title") or anchor.get_text(" ", strip=True))
        if not title or not _relevant(title):
            continue
        if source_spec.venue == "SHFE" and "上海国际能源" in title:
            continue
        if source_spec.venue == "INE" and "上海期货交易所" in title and "上海国际能源" not in title:
            continue
        canonical = urllib.parse.urlunsplit((*urllib.parse.urlsplit(href)[:3], "", ""))
        if canonical in seen:
            continue
        seen.add(canonical)
        context = anchor.parent.get_text(" ", strip=True) if anchor.parent else ""
        notices.append({
            "venue": source_spec.venue,
            "title": title,
            "url": canonical,
            "published_date": _date_from(context) or _date_from(path),
        })
    return notices


def _article(soup):
    selectors = (
        ".notice_contain", ".InfoContent", ".jysggnr", "article", ".article-content",
        ".detail_content", ".content-detail", "UCAPCONTENT",
    )
    candidates = [soup.select_one(selector) for selector in selectors]
    candidates = [candidate for candidate in candidates if candidate]
    return max(candidates, key=lambda node: len(node.get_text(" ", strip=True))) if candidates else soup.body or soup


def _metadata(soup, notice):
    title_node = soup.find("meta", attrs={"name": "ArticleTitle"})
    heading = soup.find("h1")
    title = (
        title_node.get("content") if title_node else None
    ) or (heading.get_text(" ", strip=True) if heading else None) or notice["title"]
    published = notice.get("published_date")
    for selector in ("publishtime", ".notice_nav", "time"):
        date_node = soup.select_one(selector)
        if date_node:
            published = _date_from(date_node.get_text(" ", strip=True)) or published
            if published:
                break
    date_node = soup.find("meta", attrs={"name": "PubDate"})
    if not published and date_node:
        published = _date_from(date_node.get("content"))
    if not published:
        published = _date_from(soup.get_text(" ", strip=True))
    return _clean_text(title), published


def _is_asset(url, tag_name):
    path = urllib.parse.unquote(urllib.parse.urlsplit(url).path)
    suffix = PurePosixPath(path).suffix.lower()
    if tag_name == "img":
        return (
            bool(re.search(r"W020\d+", path, re.I))
            or "/u/cms/" in path
            or "/upload/" in path
        )
    return suffix in {
        ".pdf", ".png", ".jpg", ".jpeg", ".webp", ".gif",
        ".doc", ".docx", ".xls", ".xlsx", ".zip",
    }


def _asset_urls(article, base_url):
    result, seen = [], set()
    for tag, attribute in (("img", "src"), ("a", "href")):
        for node in article.find_all(tag):
            raw = node.get(attribute)
            if not raw or raw.lower().startswith(("javascript:", "data:")):
                continue
            url = urllib.parse.urljoin(base_url, raw)
            if not _is_asset(url, tag) or url in seen:
                continue
            seen.add(url)
            result.append(url)
    return result[:MAX_ASSETS_PER_NOTICE]


def _docx_text(content):
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            document = archive.read("word/document.xml")
        root = ElementTree.fromstring(document)
        words = [node.text for node in root.iter() if node.tag.endswith("}t") and node.text]
        return _clean_text(" ".join(words))
    except (KeyError, OSError, ValueError, zipfile.BadZipFile, ElementTree.ParseError):
        return ""


def parse_notice(content, content_type, notice, resolved_url, timeout=25, retries=1):
    html = _decode_html(content, content_type)
    soup = BeautifulSoup(html, "html.parser")
    article = _article(soup)
    title, published = _metadata(soup, notice)
    notice = dict(notice, title=title, published_date=published, url=resolved_url)
    text = _clean_text(article.get_text("\n", strip=True))
    assets, errors = [], []
    for url in _asset_urls(article, resolved_url):
        try:
            raw, asset_type, final_url = fetch(
                url, timeout=timeout, retries=retries, max_bytes=MAX_ASSET_BYTES
            )
            filename = urllib.parse.unquote(PurePosixPath(
                urllib.parse.urlsplit(final_url).path
            ).name) or "attachment"
            if asset_type == "application/octet-stream":
                asset_type = mimetypes.guess_type(filename)[0] or asset_type
            assets.append({
                "url": final_url,
                "file_name": filename,
                "content_type": asset_type,
                "content": raw,
                "extracted_text": _docx_text(raw) if filename.lower().endswith(".docx") else "",
            })
        except DataError as error:
            errors.append({"url": url, "error": str(error)})
    return notice, text, assets, errors


def refresh(connection, page_count=5, per_source_limit=8, timeout=25, retries=1):
    """Fetch notices only when explicitly invoked by the manual API or CLI."""
    observed_at = datetime.now(timezone.utc).replace(microsecond=0)
    totals = {"discovered": 0, "relevant": 0, "inserted": 0, "confirmed": 0, "assets": 0}
    source_results = []
    for source_spec in SOURCES:
        result = {
            "venue": source_spec.venue, "pages": 0, "discovered": 0,
            "relevant": 0, "inserted": 0, "confirmed": 0, "assets": 0,
            "errors": [],
        }
        baselines = [
            dict(notice) for notice in BASELINE_NOTICES
            if notice["venue"] == source_spec.venue
        ]
        result["baseline"] = len(baselines)
        candidates = {notice["url"]: notice for notice in baselines}
        for page_url in source_spec.pages(page_count):
            try:
                raw, content_type, resolved_url = fetch(
                    page_url, timeout=timeout, retries=retries, max_bytes=MAX_LIST_BYTES
                )
                html = _decode_html(raw, content_type)
                found = discover_notices(source_spec, html, resolved_url)
                result["pages"] += 1
                result["discovered"] += len(found)
                for notice in found:
                    candidates.setdefault(notice["url"], notice)
            except DataError as error:
                result["errors"].append({"url": page_url, "error": str(error)})
        result["relevant"] = len(candidates)
        ordered = sorted(
            candidates.values(), key=lambda item: item.get("published_date") or "", reverse=True
        )[:per_source_limit]
        for notice in ordered:
            try:
                raw, content_type, resolved_url = fetch(
                    notice["url"], timeout=timeout, retries=retries,
                    max_bytes=MAX_NOTICE_BYTES,
                )
                parsed, text, assets, asset_errors = parse_notice(
                    raw, content_type, notice, resolved_url, timeout, retries
                )
                merged = storage.merge_announcement(
                    connection, parsed, raw, content_type, text, assets, observed_at
                )
                result["inserted" if merged["inserted"] else "confirmed"] += 1
                result["assets"] += len(assets)
                result["errors"].extend(asset_errors)
            except DataError as error:
                result["errors"].append({"url": notice["url"], "error": str(error)})
        for key in totals:
            if key == "discovered":
                totals[key] += result["discovered"]
            elif key == "relevant":
                totals[key] += result["relevant"]
            else:
                totals[key] += result[key]
        source_results.append(result)
    totals["sources"] = source_results
    return totals
