"""OpenCTP transport and HTML/CSV parsing."""

import csv
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from html.parser import HTMLParser

from .models import DataError, HEADER_ALIASES, REQUIRED_FIELDS, header_key, normalize_row


class FeeTableParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self, convert_charrefs=True)
        self.tables, self.depth = [], 0
        self.rows = self.row = self.cell = None
        self.parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "table":
            self.depth += 1
            if self.depth == 1:
                self.rows = []
        elif self.depth == 1 and tag == "tr":
            self.row = []
        elif self.depth == 1 and tag in ("th", "td"):
            self.cell, self.parts = tag, []
        elif self.cell and tag == "br":
            self.parts.append(" ")

    def handle_data(self, data):
        if self.cell:
            self.parts.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if self.depth == 1 and tag in ("th", "td") and self.cell:
            self.row.append(" ".join("".join(self.parts).split()))
            self.cell, self.parts = None, []
        elif self.depth == 1 and tag == "tr":
            if self.row and any(self.row):
                self.rows.append(self.row)
            self.row = None
        elif tag == "table" and self.depth:
            if self.depth == 1 and self.rows:
                self.tables.append(self.rows)
            self.depth -= 1


def validate_rows(rows):
    if not rows:
        raise DataError("source contained no instrument rows")
    available = {key for row in rows for key, value in row.items() if value is not None}
    missing = sorted(REQUIRED_FIELDS - available)
    if missing:
        raise DataError("source is missing required fields: %s" % ", ".join(missing))
    seen = set()
    duplicates = []
    for record in rows:
        key = (record.get("venue"), str(record.get("instrument_id")).upper())
        if key in seen:
            duplicates.append("%s.%s" % key)
        seen.add(key)
    if duplicates:
        raise DataError("duplicate instruments in source: %s" % ", ".join(duplicates[:5]))


def parse_html(text):
    parser = FeeTableParser()
    parser.feed(text)
    candidates = []
    for table in parser.tables:
        for index, header in enumerate(table[:4]):
            mapped = {HEADER_ALIASES.get(header_key(value)) for value in header}
            score = len(mapped.intersection(REQUIRED_FIELDS))
            if score >= 7:
                candidates.append((score, len(table), table, index, header))
    if not candidates:
        raise DataError("no OpenCTP fee table found in HTML")
    _, _, table, header_index, header = max(
        candidates, key=lambda candidate: (candidate[0], candidate[1])
    )
    records = []
    for row in table[header_index + 1:]:
        if len(row) < 5:
            continue
        row = row + [""] * max(0, len(header) - len(row))
        raw = {header[index]: row[index] for index in range(min(len(header), len(row)))}
        record = normalize_row(raw)
        if record.get("venue") and record.get("instrument_id"):
            records.append(record)
    validate_rows(records)
    return records


def parse_csv(text):
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",\t;")
    except csv.Error:
        dialect = csv.excel
    records = [normalize_row(dict(row)) for row in csv.DictReader(text.splitlines(), dialect=dialect)]
    records = [record for record in records if record.get("venue") and record.get("instrument_id")]
    validate_rows(records)
    return records


def decode(content, content_type=""):
    encodings = []
    match = re.search(r"charset=([A-Za-z0-9_-]+)", content_type, re.I)
    if match:
        encodings.append(match.group(1))
    meta = re.search(br"charset\s*=\s*[\"']?([A-Za-z0-9_-]+)", content[:4096], re.I)
    if meta:
        encodings.append(meta.group(1).decode("ascii", "ignore"))
    encodings.extend(("utf-8-sig", "utf-8", "gb18030"))
    tried = set()
    for encoding in encodings:
        if encoding.lower() in tried:
            continue
        tried.add(encoding.lower())
        try:
            return content.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            pass
    raise DataError("could not decode source as UTF-8 or GB18030")


def fetch(url, timeout, retries, user_agent, accept=None):
    parsed = urllib.parse.urlsplit(url)
    candidates = [url]
    if parsed.scheme.lower() == "https" and (parsed.hostname or "").lower() in (
        "openctp.cn", "www.openctp.cn", "dict.openctp.cn"
    ):
        candidates.append(urllib.parse.urlunsplit(("http", parsed.netloc, parsed.path, parsed.query, parsed.fragment)))
    errors = []
    for candidate in candidates:
        hostname = (urllib.parse.urlsplit(candidate).hostname or "").lower()
        transports = [("environment", urllib.request.urlopen)]
        if hostname == "dict.openctp.cn":
            direct = urllib.request.build_opener(urllib.request.ProxyHandler({})).open
            transports = [("direct", direct), ("environment", urllib.request.urlopen)]
        for transport_name, opener in transports:
            for attempt in range(retries + 1):
                request = urllib.request.Request(candidate, headers={
                    "Accept": accept or "text/html,application/xhtml+xml,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
                    "User-Agent": user_agent,
                })
                try:
                    with opener(request, timeout=timeout) as response:
                        content = response.read()
                        if not content:
                            raise DataError("source returned an empty response")
                        return content, response.headers.get("Content-Type", ""), response.geturl()
                except (urllib.error.URLError, urllib.error.HTTPError, OSError, DataError) as error:
                    errors.append("%s via %s: %s" % (candidate, transport_name, error))
                    if attempt < retries:
                        time.sleep(min(2 ** attempt, 8))
    raise DataError("failed to fetch source; %s" % "; ".join(errors))


def source_date(text, records, supplied=None):
    if supplied:
        return parse_date(supplied, "source date")
    title_date = re.search(r"期货交易手续费与保证金\s*[-—:]?\s*(20\d{6})", text)
    if title_date:
        return datetime.strptime(title_date.group(1), "%Y%m%d").date().isoformat()
    dates = []
    for record in records:
        match = re.search(r"(20\d{2})[-/]?(\d{2})[-/]?(\d{2})", str(record.get("source_updated_at") or ""))
        if match:
            dates.append("-".join(match.groups()))
    return Counter(dates).most_common(1)[0][0] if dates else datetime.now(timezone.utc).date().isoformat()


def parse_date(value, label="date"):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date().isoformat()
    except ValueError:
        raise DataError("%s must use YYYY-MM-DD: %r" % (label, value))
