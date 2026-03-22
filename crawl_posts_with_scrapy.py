from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request, Response

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR / "working_links.txt"
DEFAULT_TEMPLATE = BASE_DIR / "Fichier de données.xlsx"
DEFAULT_SOURCE_FILE = BASE_DIR / "Cassiopée Envoi2 Cactus à CryptOn.xlsx"
DEFAULT_OUTPUT = BASE_DIR / "resultats_posts_scraped.xlsx"
DEFAULT_TOR_PROXY = "http://127.0.0.1:8118"


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def extract_body_text_from_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return clean_text(soup.get_text(" ", strip=True))


def normalize_base(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"http://{value}"
    return value.rstrip("/")


def extract_host(url: str) -> str:
    return (urlparse(url).hostname or "").lower().strip()


def is_onion(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith(".onion")


def is_probable_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().strip()
    return bool(parsed.scheme in {"http", "https"} and host)


def detect_url_column(df: pd.DataFrame) -> str:
    best_column = ""
    best_score = -1
    for column in df.columns:
        series = df[column].dropna().astype(str).map(str.strip)
        if series.empty:
            continue
        score = int(series.map(lambda x: is_probable_url(normalize_base(x))).sum())
        if score > best_score:
            best_score = score
            best_column = str(column)

    if best_score <= 0:
        raise ValueError("Could not detect URL column in input file")
    return best_column


def extract_url_candidates_from_line(line: str) -> list[str]:
    text = (line or "").strip()
    if not text:
        return []

    candidates: list[str] = []

    # First: explicit http/https links anywhere in the line.
    for match in re.findall(r"https?://[^\s\]\[\)\(\"'<>]+", text, flags=re.IGNORECASE):
        candidates.append(match.strip())

    # Second: token-based fallback for lines like "BASE: xxx.onion/path".
    tokens = [token.strip(" ,;\t\r\n\"'()[]{}") for token in text.split()]
    for token in tokens:
        if not token:
            continue
        low = token.lower()
        if low.startswith(("base:", "url:", "link:")):
            token = token.split(":", 1)[1].strip()
            if not token:
                continue
            low = token.lower()
        if ".onion" in low or re.search(r"\.[a-z]{2,}([/:]|$)", low):
            candidates.append(token)

    # Preserve order while deduplicating.
    unique: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        if value not in seen:
            unique.append(value)
            seen.add(value)
    return unique


def load_links(path: Path, url_column: str | None) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    links: list[str] = []
    suffix = path.suffix.lower()
    if suffix in {".txt", ".list"}:
        raw_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in raw_lines:
            for candidate in extract_url_candidates_from_line(line):
                normalized = normalize_base(candidate)
                if is_probable_url(normalized):
                    links.append(normalized)
    elif suffix in {".csv", ".xlsx", ".xls"}:
        if suffix == ".csv":
            frame = pd.read_csv(path)
        else:
            frame = pd.read_excel(path)
        column = url_column if url_column and url_column in frame.columns else detect_url_column(frame)
        for value in frame[column].dropna().astype(str).tolist():
            normalized = normalize_base(value)
            if is_probable_url(normalized):
                links.append(normalized)
    else:
        raise ValueError("Input must be .txt, .csv, .xlsx or .xls")

    return sorted(set(links))


def load_template_attributes(path: Path) -> list[str]:
    frame = pd.read_excel(path, sheet_name="Feuil1", header=None)
    attrs: list[str] = []
    seen: set[str] = set()
    for value in frame.iloc[1].tolist():
        cleaned = "" if pd.isna(value) else str(value).strip()
        if cleaned and cleaned not in seen:
            attrs.append(cleaned)
            seen.add(cleaned)
    return attrs


def infer_payment_method(text: str) -> str | None:
    low = text.lower()
    if "bitcoin" in low or " btc" in low:
        return "crypto (BTC)"
    if "monero" in low or " xmr" in low:
        return "crypto (XMR)"
    if "crypto" in low:
        return "crypto"
    if "bank transfer" in low or "wire transfer" in low or "fiat" in low:
        return "fiat"
    return None


def infer_raas(text: str) -> str | None:
    low = text.lower()
    if "ransomware-as-a-service" in low or "raas" in low or "affiliate" in low:
        return "yes"
    return None


def infer_extortion_strategy(text: str) -> str | None:
    low = text.lower()
    if "triple extortion" in low:
        return "triple extortion"
    if "double extortion" in low:
        return "double extortion"
    if "extortion" in low:
        return "extortion"
    return None


def find_date(text: str) -> str | None:
    patterns = [
        r"\b(20\d{2}[-/](?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01]))\b",
        r"\b((?:0[1-9]|[12]\d|3[01])[-/](?:0[1-9]|1[0-2])[-/](?:20\d{2}))\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def find_amount(text: str, paid: bool = False) -> str | None:
    low = text.lower()
    if paid and "paid" not in low and "payment" not in low:
        return None
    if not paid and all(token not in low for token in ("ask", "demand", "ransom", "request")):
        return None

    pattern = re.compile(r"(?i)(\$|usd|eur|btc|xmr)?\s?([0-9]{1,3}(?:[,\s.][0-9]{3})+|[0-9]+(?:[.,][0-9]+)?)\s?(million|m|k|btc|xmr|usd|eur)?")
    for match in pattern.finditer(text):
        chunk = " ".join(part for part in match.groups() if part)
        if chunk:
            return clean_text(chunk)
    return None


def find_records_count(text: str) -> str | None:
    pattern = re.compile(r"(?i)([0-9]{1,3}(?:[,\s.][0-9]{3})+|[0-9]+(?:[.,][0-9]+)?)\s+(records|files|entries|users|customers)")
    match = pattern.search(text)
    if not match:
        return None
    return clean_text(" ".join(match.groups()))


def guess_breach_type(text: str) -> str | None:
    mapping = [
        ("data leak", "data leak"),
        ("data breach", "data breach"),
        ("ransomware", "ransomware"),
        ("database", "database breach"),
        ("exfiltrat", "data exfiltration"),
        ("encrypt", "encryption"),
    ]
    low = text.lower()
    for key, label in mapping:
        if key in low:
            return label
    return None


def negotiation_flag(text: str) -> str | None:
    low = text.lower()
    if any(token in low for token in ("negotiat", "chat", "bargain", "counteroffer")):
        return "yes"
    return None


def negotiation_actor(text: str) -> str | None:
    low = text.lower()
    if "incident response" in low or "third-party negotiator" in low or "security firm" in low:
        return "security breach management firm"
    if "manager" in low or "ceo" in low or "company" in low:
        return "firm managers"
    return None


def negotiation_outcome(text: str) -> str | None:
    low = text.lower()
    if "paid" in low and "decryption" in low:
        return "firm accepted to pay"
    if "refused" in low or "did not pay" in low or "won't pay" in low:
        return "firm did not pay"
    if "leaked" in low or "published" in low:
        return "gang leaked data"
    return None


def find_blockchain_link(html: str) -> str | None:
    match = re.search(r"https?://[^\s\"']*(blockchain|blockchair|btc\.com|etherscan)[^\s\"']*", html, re.IGNORECASE)
    return clean_text(match.group(0)) if match else None


def find_ticker(text: str) -> str | None:
    match = re.search(r"\b([A-Z]{1,5})\b\s*(?:\(|-)?\s*(?:NASDAQ|NYSE|EURONEXT|LSE|ticker|isin)", text)
    if match:
        return clean_text(match.group(1))
    return None


def infer_victim_name(title: str, h1: str, meta_description: str) -> str | None:
    for candidate in (h1, title, meta_description):
        text = clean_text(candidate)
        if not text:
            continue
        for sep in (" - ", " | ", ": "):
            if sep in text:
                left = clean_text(text.split(sep, 1)[0])
                if 3 <= len(left) <= 120:
                    return left
        if 3 <= len(text) <= 120:
            return text
    return None


def infer_industry(text: str) -> str | None:
    low = text.lower()
    mapping = {
        "health": "Healthcare",
        "hospital": "Healthcare",
        "bank": "Finance",
        "insurance": "Insurance",
        "retail": "Retail",
        "manufactur": "Manufacturing",
        "energy": "Energy",
        "oil": "Energy",
        "gas": "Energy",
        "government": "Public Sector",
        "education": "Education",
        "school": "Education",
        "university": "Education",
        "telecom": "Telecommunications",
        "transport": "Transportation",
        "logistics": "Logistics",
        "pharma": "Pharmaceutical",
        "technology": "Technology",
        "software": "Technology",
    }
    for key, value in mapping.items():
        if key in low:
            return value
    return None


def infer_location(text: str) -> str | None:
    countries = [
        "united states", "usa", "canada", "france", "germany", "italy", "spain", "uk", "united kingdom",
        "netherlands", "belgium", "switzerland", "sweden", "norway", "denmark", "finland", "poland",
        "romania", "ukraine", "russia", "china", "japan", "india", "singapore", "australia", "brazil",
        "mexico", "argentina", "turkey", "israel", "uae", "saudi arabia", "south africa",
    ]
    low = text.lower()
    for country in countries:
        if country in low:
            return country.title()
    return None


def infer_listing_status(text: str) -> tuple[str | None, str | None]:
    low = text.lower()
    ticker = find_ticker(text)
    if "publicly traded" in low or "listed" in low or ticker:
        return "listed", "yes"
    if "private company" in low or "privately held" in low:
        return "private", "no"
    return None, None


def infer_gang_from_url(url: str) -> str | None:
    host = extract_host(url)
    if not host:
        return None
    first_label = host.split(".", 1)[0]
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "", first_label)
    return cleaned or None


def detect_intermediary_block(title: str, h1: str, body_text: str, html_text: str) -> str | None:
    corpus = clean_text(" ".join([title, h1, body_text, html_text])).lower()
    signals = [
        "you have been placed in queue",
        "awaiting forwarding to the platform",
        "checking your browser",
        "ddos-guard",
        "cloudflare",
        "captcha",
        "are you human",
        "access denied",
        "security check",
        "challenge-platform",
    ]
    for signal in signals:
        if signal in corpus:
            return signal
    return None


def extract_jsonld_texts(response: Response) -> list[str]:
    blocks: list[str] = []
    for content in response.css('script[type="application/ld+json"]::text').getall():
        raw = content.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            blocks.append(raw)
            continue

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                for nested in value.values():
                    walk(nested)
            elif isinstance(value, list):
                for nested in value:
                    walk(nested)
            elif isinstance(value, (str, int, float)):
                blocks.append(str(value))

        walk(parsed)
    return blocks


def extract_context_row(source_frame: pd.DataFrame, url: str, url_column: str) -> dict[str, Any]:
    if source_frame.empty or url_column not in source_frame.columns:
        return {}

    normalized_target = normalize_base(url)
    subset = source_frame[source_frame[url_column].astype(str).map(lambda x: normalize_base(x) == normalized_target)]
    if not subset.empty:
        return subset.iloc[0].to_dict()

    target_host = extract_host(url)
    if not target_host:
        return {}

    host_subset = source_frame[
        source_frame[url_column].astype(str).map(lambda x: extract_host(normalize_base(x)) == target_host)
    ]
    if not host_subset.empty:
        return host_subset.iloc[0].to_dict()

    return {}


def build_output_row(
    attributes: list[str],
    post_url: str,
    status: str,
    status_code: int | None,
    error: str | None,
    final_url: str | None,
    page_title: str,
    meta_description: str,
    h1: str,
    text_excerpt: str,
    html_text: str,
    context_row: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {attr: None for attr in attributes}
    corpus = clean_text(" ".join([page_title, meta_description, h1, text_excerpt, html_text]))
    inferred_victim = infer_victim_name(page_title, h1, meta_description)
    inferred_gang = infer_gang_from_url(final_url or post_url)
    inferred_industry = infer_industry(corpus)
    inferred_location = infer_location(corpus)
    inferred_listing, inferred_public_traded = infer_listing_status(corpus)

    result["GANG"] = context_row.get("Ransomware Gang") or inferred_gang
    result["Victim Name"] = context_row.get("*Claimed* Victim") or inferred_victim
    result["The type of breach"] = guess_breach_type(corpus)
    result["Number of breached records"] = find_records_count(corpus)
    result["Date of the breach"] = context_row.get("Date") or context_row.get("Detection Date (UTC+0)") or find_date(corpus)

    pricing_signal = any(token in corpus.lower() for token in ("discount", "deadline", "price", "pay in", "before"))
    result["Does the gang have a pricing strategy?"] = "yes" if pricing_signal else None
    result["if yes what is it?"] = "time-based/conditional ransom" if pricing_signal else None

    result["Amount asked"] = find_amount(corpus, paid=False)
    result["amount paid by the victim"] = find_amount(corpus, paid=True)
    result["whether the firm negotiated or not"] = negotiation_flag(corpus)
    result[
        "In case of negotiation, who is negotiating with hackers: firms managers or the firm hired a\xa0security breach management firm"
    ] = negotiation_actor(corpus)
    result["Messages exchanged during the negotiation"] = text_excerpt[:500] if negotiation_flag(corpus) else None
    result["negotiation outcome (firm accepted to pay or not; the gang leaked/sold the data or not)"] = negotiation_outcome(corpus)
    result["what payment method used crypto (BTC or other) or fiat"] = infer_payment_method(corpus)
    result["use or not of Ransomware-as-a-Service (RaaS)"] = infer_raas(corpus)
    result["Revenues (per year) of the gang"] = None
    result["Attacking stratgy (double extortion, triple extortion, etc.)"] = infer_extortion_strategy(corpus)
    result["Number of attacks per gang (per year)"] = None
    result["lien de la blockchain de paiement"] = find_blockchain_link(html_text)
    result["Number of victims that paid the ransom"] = None
    result["Name of the victim"] = context_row.get("*Claimed* Victim") or inferred_victim
    result["Its location"] = context_row.get("Victim Country") or inferred_location
    result["Its listing status"] = context_row.get("listing status") or inferred_listing
    result["type of industry"] = context_row.get("Industrial Sector") or inferred_industry
    result["whether it has a cybersecurity insurance or not"] = None
    result["whether it has publicly revealed being subject to previous breach or not"] = None
    result["whether it holds bitcoin or other cryptocurrency or not)"] = None
    result["whether the company is publicly traded"] = inferred_public_traded
    result["ticker/isin code"] = find_ticker(corpus)

    result["_Link Source"] = post_url
    result["_Link Status"] = status
    result["_HTTP Status Code"] = status_code
    result["_Error"] = error
    result["_Final URL"] = final_url
    result["_Title"] = page_title
    result["_Meta Description"] = meta_description
    result["_H1"] = h1
    result["_Text Excerpt"] = text_excerpt
    result["_Scraped At"] = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    return result


def write_output(
    path: Path,
    raw_rows: list[dict[str, Any]],
    extracted_rows: list[dict[str, Any]],
    links_count: int,
    ok_count: int,
    mode: str,
) -> None:
    raw_df = pd.DataFrame(raw_rows)
    extracted_df = pd.DataFrame(extracted_rows)
    summary_df = pd.DataFrame(
        [
            {"metric": "mode", "value": mode},
            {"metric": "total_links", "value": links_count},
            {"metric": "working_links", "value": ok_count},
            {"metric": "failed_links", "value": links_count - ok_count},
            {"metric": "raw_rows", "value": len(raw_rows)},
            {"metric": "mapped_rows", "value": len(extracted_rows)},
            {"metric": "generated_at_utc", "value": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")},
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if mode in {"raw", "both"}:
            raw_df.to_excel(writer, sheet_name="Raw Posts", index=False)
        if mode in {"mapped", "both"}:
            extracted_df.to_excel(writer, sheet_name="Extracted Data", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)


class WorkingLinksSpider(Spider):
    name = "post_data_scraper"

    def __init__(
        self,
        links: list[str],
        attributes: list[str],
        source_frame: pd.DataFrame,
        source_url_column: str,
        output_path: str,
        tor_proxy: str,
        mode: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.links = links
        self.attributes = attributes
        self.source_frame = source_frame
        self.source_url_column = source_url_column
        self.output_path = Path(output_path)
        self.tor_proxy = tor_proxy
        self.mode = mode
        self.max_block_retries = 2
        self.raw_results: list[dict[str, Any]] = []
        self.ok_count = 0

    def start_requests(self):
        for url in self.links:
            meta = {"source_url": url}
            if is_onion(url):
                meta["proxy"] = self.tor_proxy
            yield Request(
                url=url,
                callback=self.parse_page,
                errback=self.on_error,
                dont_filter=True,
                meta=meta,
            )

    def parse_page(self, response: Response):
        source_url = str(response.meta.get("source_url") or response.url)
        if not (200 <= response.status < 400):
            self.raw_results.append(
                {
                    "_Link Source": source_url,
                    "_Link Status": "NOT_WORKING",
                    "_HTTP Status Code": response.status,
                    "_Error": f"HTTP {response.status}",
                    "_Final URL": response.url,
                    "_Title": "",
                    "_Meta Description": "",
                    "_H1": "",
                    "_Text Excerpt": "",
                    "_Body Text": "",
                    "_JSONLD Text": "",
                    "_Scraped At": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                }
            )
            return

        self.ok_count += 1
        title = clean_text(response.css("title::text").get(default=""))
        meta_description = clean_text(response.css("meta[name='description']::attr(content)").get(default=""))
        h1 = clean_text(response.css("h1::text").get(default=""))
        body_text = extract_body_text_from_html(response.text)
        jsonld_text = clean_text(" ".join(extract_jsonld_texts(response)))
        text_excerpt = clean_text(" ".join([title, meta_description, h1, body_text]))[:2000]
        block_reason = detect_intermediary_block(title, h1, body_text, response.text)

        if block_reason:
            current_retry = int(response.meta.get("queue_retry") or 0)
            if current_retry < self.max_block_retries:
                retry_meta = dict(response.meta)
                retry_meta["queue_retry"] = current_retry + 1
                yield Request(
                    url=source_url,
                    callback=self.parse_page,
                    errback=self.on_error,
                    dont_filter=True,
                    meta=retry_meta,
                )
                return

        self.raw_results.append(
            {
                "_Link Source": source_url,
                "_Link Status": "BLOCKED_INTERMEDIARY" if block_reason else "WORKING",
                "_HTTP Status Code": response.status,
                "_Error": f"Intermediary page detected: {block_reason}" if block_reason else None,
                "_Final URL": response.url,
                "_Title": title,
                "_Meta Description": meta_description,
                "_H1": h1,
                "_Text Excerpt": text_excerpt,
                "_Body Text": body_text,
                "_JSONLD Text": jsonld_text,
                "_Scraped At": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            }
        )

    def on_error(self, failure):
        request = failure.request
        source_url = str(request.meta.get("source_url") or request.url)
        self.raw_results.append(
            {
                "_Link Source": source_url,
                "_Link Status": "NOT_WORKING",
                "_HTTP Status Code": None,
                "_Error": clean_text(str(failure.value)),
                "_Final URL": None,
                "_Title": "",
                "_Meta Description": "",
                "_H1": "",
                "_Text Excerpt": "",
                "_Body Text": "",
                "_JSONLD Text": "",
                "_Scraped At": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            }
        )

    def closed(self, reason: str):
        extracted_rows: list[dict[str, Any]] = []
        if self.mode in {"mapped", "both"}:
            for raw in self.raw_results:
                post_url = str(raw.get("_Link Source") or "")
                final_url = raw.get("_Final URL")
                context_row = extract_context_row(self.source_frame, post_url, self.source_url_column)
                html_text = clean_text(" ".join([str(raw.get("_Body Text") or ""), str(raw.get("_JSONLD Text") or "")]))
                extracted_rows.append(
                    build_output_row(
                        attributes=self.attributes,
                        post_url=post_url,
                        status=str(raw.get("_Link Status") or "NOT_WORKING"),
                        status_code=raw.get("_HTTP Status Code"),
                        error=raw.get("_Error"),
                        final_url=str(final_url) if final_url else None,
                        page_title=str(raw.get("_Title") or ""),
                        meta_description=str(raw.get("_Meta Description") or ""),
                        h1=str(raw.get("_H1") or ""),
                        text_excerpt=str(raw.get("_Text Excerpt") or ""),
                        html_text=html_text,
                        context_row=context_row,
                    )
                )

        write_output(
            self.output_path,
            raw_rows=self.raw_results,
            extracted_rows=extracted_rows,
            links_count=len(self.links),
            ok_count=self.ok_count,
            mode=self.mode,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape post pages and map data to Fichier de données attributes.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Path to post links file (.txt/.csv/.xlsx)")
    parser.add_argument("--input-url-column", default=None, help="Optional URL column when input is CSV/XLSX")
    parser.add_argument("--template-file", default=str(DEFAULT_TEMPLATE), help="Path to Fichier de données.xlsx")
    parser.add_argument("--source-file", default=str(DEFAULT_SOURCE_FILE), help="Optional source dataset for context fields")
    parser.add_argument("--source-url-column", default=None, help="Optional source URL column override")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to output Excel file")
    parser.add_argument(
        "--tor-proxy",
        default=DEFAULT_TOR_PROXY,
        help="HTTP proxy for onion pages (example: http://127.0.0.1:8118 via privoxy+tor)",
    )
    parser.add_argument("--timeout", type=int, default=20, help="Download timeout in seconds")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent requests")
    parser.add_argument(
        "--mode",
        choices=["raw", "mapped", "both"],
        default="both",
        help="Output mode: raw only, mapped only, or both sheets",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    template_file = Path(args.template_file)
    source_file = Path(args.source_file)

    if not template_file.exists():
        raise FileNotFoundError(f"Template file not found: {template_file}")

    links = load_links(input_path, args.input_url_column)
    attributes = load_template_attributes(template_file)

    if source_file.exists():
        source_frame = pd.read_excel(source_file, sheet_name="envoi2 Cactus à Crypton")
        source_url_column = args.source_url_column if args.source_url_column and args.source_url_column in source_frame.columns else detect_url_column(source_frame)
    else:
        source_frame = pd.DataFrame()
        source_url_column = args.source_url_column or ""

    if not links:
        if input_path.suffix.lower() in {".txt", ".list"} and input_path.exists():
            preview = "\n".join(input_path.read_text(encoding="utf-8", errors="ignore").splitlines()[:5])
            raise ValueError(
                "No valid links were parsed from input file. "
                "Check file format (expected URLs, one per line or embedded in text). "
                f"Input: {input_path}\nFirst lines:\n{preview}"
            )
        write_output(output_path, [], [], links_count=0, ok_count=0, mode=args.mode)
        print(f"No links found in input. Output written: {output_path}")
        return

    settings = {
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": args.timeout,
        "CONCURRENT_REQUESTS": args.concurrency,
        "RETRY_TIMES": 1,
        "DOWNLOADER_CLIENT_TLS_VERIFY": False,
        "HTTPERROR_ALLOW_ALL": True,
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "TELNETCONSOLE_ENABLED": False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(
        WorkingLinksSpider,
        links=links,
        attributes=attributes,
        source_frame=source_frame,
        source_url_column=source_url_column,
        output_path=str(output_path),
        tor_proxy=args.tor_proxy,
        mode=args.mode,
    )
    process.start()

    print(f"Scraping completed. Output: {output_path}")


if __name__ == "__main__":
    main()
