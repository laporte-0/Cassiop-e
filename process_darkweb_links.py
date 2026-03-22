from __future__ import annotations

import argparse
import re
import os
import time
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas.errors import EmptyDataError

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LINKS_FILE = BASE_DIR / "Cassiopée Envoi2 Cactus à CryptOn.xlsx"
DEFAULT_TEMPLATE_FILE = BASE_DIR / "Fichier de données.xlsx"
DEFAULT_OUTPUT_FILE = BASE_DIR / "resultats_liens_et_extraction.xlsx"
DEFAULT_BASE_CSV_FILE = BASE_DIR / "base_urls_checked.csv"
DEFAULT_WORKING_LINKS_TXT_FILE = BASE_DIR / "working_links.txt"
DEFAULT_POSTS_REPORT_TXT_FILE = BASE_DIR / "posts_test_report.txt"

DEFAULT_LINKS_SHEET = "envoi2 Cactus à Crypton"
DEFAULT_LINKS_COLUMN = "Ransomware URL"

DEFAULT_TIMEOUT = 20
DEFAULT_PRECHECK_TIMEOUT = 12
DEFAULT_TOR_PROXY = "socks5h://127.0.0.1:9050"


@dataclass
class LinkCheckResult:
    original_url: str
    normalized_url: str
    is_onion: bool
    works: bool
    status: str
    status_code: int | None
    error: str | None
    final_url: str | None
    title: str | None
    meta_description: str | None
    h1: str | None
    text_excerpt: str | None


@dataclass
class RuntimeConfig:
    links_file: Path
    template_file: Path
    output_file: Path
    base_csv_file: Path
    working_links_txt_file: Path
    posts_report_txt_file: Path
    base_input_csv: Path | None
    test_posts_for_base: str | None
    links_sheet: str
    links_column: str
    timeout: int
    precheck_timeout: int
    tor_proxy: str | None
    use_tor_for_clearweb: bool
    scrape_only_working: bool
    fast_working_input: bool
    family_prefix_len: int
    verbose: bool
    log_every: int


def normalize_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value):
        return f"http://{value}"
    return value


def is_probable_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    if host in {"localhost"}:
        return True
    if host.endswith(".onion"):
        return True
    if "." not in host:
        return False
    if " " in host:
        return False
    tld = host.rsplit(".", 1)[-1]
    if len(tld) < 2 or not tld.isalpha():
        return False
    return True


def detect_onion(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    return host.endswith(".onion")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check ransomware links and extract data into Excel output.")
    parser.add_argument("--links-file", default=str(DEFAULT_LINKS_FILE), help="Path to Excel containing links")
    parser.add_argument("--template-file", default=str(DEFAULT_TEMPLATE_FILE), help="Path to template Excel")
    parser.add_argument("--output-file", default=str(DEFAULT_OUTPUT_FILE), help="Path for output Excel")
    parser.add_argument(
        "--base-csv-file",
        default=str(DEFAULT_BASE_CSV_FILE),
        help="Path for CSV export of deduplicated base URL checks",
    )
    parser.add_argument(
        "--working-links-txt-file",
        default=str(DEFAULT_WORKING_LINKS_TXT_FILE),
        help="Path for TXT export of WORKING links (one URL per line)",
    )
    parser.add_argument(
        "--posts-report-txt-file",
        default=str(DEFAULT_POSTS_REPORT_TXT_FILE),
        help="Path for organized TXT report when testing posts of one base",
    )
    parser.add_argument(
        "--base-input-csv",
        default=None,
        help="Optional CSV file containing links/bases to test directly (e.g. base_urls_checked.csv)",
    )
    parser.add_argument(
        "--test-posts-for-base",
        default=None,
        help="Test all post URLs for one base URL and write organized text report",
    )
    parser.add_argument("--links-sheet", default=DEFAULT_LINKS_SHEET, help="Sheet name containing links")
    parser.add_argument("--links-column", default=DEFAULT_LINKS_COLUMN, help="Preferred links column name")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP request timeout in seconds")
    parser.add_argument(
        "--precheck-timeout",
        type=int,
        default=DEFAULT_PRECHECK_TIMEOUT,
        help="HTTP timeout used for precheck/categorization phase (seconds).",
    )
    parser.add_argument(
        "--tor-proxy",
        default=None,
        help="Tor SOCKS proxy URL (example: socks5h://127.0.0.1:9050). If omitted, uses env TOR_PROXY or default local Tor.",
    )
    parser.add_argument(
        "--no-default-tor-proxy",
        action="store_true",
        help="Disable fallback to socks5h://127.0.0.1:9050 when TOR_PROXY is not set.",
    )
    parser.add_argument(
        "--use-tor-for-clearweb",
        action="store_true",
        help="Route non-.onion HTTP/HTTPS links through Tor proxy as well.",
    )
    parser.add_argument(
        "--scrape-all",
        action="store_true",
        help="Scrape all testable links after precheck. Default is scraping only WORKING links.",
    )
    parser.add_argument(
        "--fast-working-input",
        action="store_true",
        help="Optimization mode: input already filtered to working links, skip base precheck and scrape provided URLs directly.",
    )
    parser.add_argument(
        "--family-prefix-len",
        type=int,
        default=6,
        help="Number of chars used to build structural URL family prefix (default: 6).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable detailed progress logs.",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=1,
        help="Print progress line every N processed links (default: 1).",
    )
    return parser.parse_args()


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    env_tor_proxy = (os.environ.get("TOR_PROXY") or "").strip()
    cli_tor_proxy = (args.tor_proxy or "").strip()

    tor_proxy: str | None
    if cli_tor_proxy:
        tor_proxy = cli_tor_proxy
    elif env_tor_proxy:
        tor_proxy = env_tor_proxy
    elif args.no_default_tor_proxy:
        tor_proxy = None
    else:
        tor_proxy = DEFAULT_TOR_PROXY

    return RuntimeConfig(
        links_file=Path(args.links_file),
        template_file=Path(args.template_file),
        output_file=Path(args.output_file),
        base_csv_file=Path(args.base_csv_file),
        working_links_txt_file=Path(args.working_links_txt_file),
        posts_report_txt_file=Path(args.posts_report_txt_file),
        base_input_csv=Path(args.base_input_csv) if args.base_input_csv else None,
        test_posts_for_base=(args.test_posts_for_base or "").strip() or None,
        links_sheet=args.links_sheet,
        links_column=args.links_column,
        timeout=args.timeout,
        precheck_timeout=args.precheck_timeout,
        tor_proxy=tor_proxy,
        use_tor_for_clearweb=bool(args.use_tor_for_clearweb),
        scrape_only_working=not bool(args.scrape_all),
        fast_working_input=bool(args.fast_working_input),
        family_prefix_len=max(1, int(args.family_prefix_len)),
        verbose=not bool(args.quiet),
        log_every=max(1, int(args.log_every)),
    )


def log(config: RuntimeConfig, message: str, force: bool = False) -> None:
    if not config.verbose and not force:
        return
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}")


def get_proxies(config: RuntimeConfig, is_onion: bool) -> dict[str, str] | None:
    if is_onion:
        if not config.tor_proxy:
            return None
        return {"http": config.tor_proxy, "https": config.tor_proxy}

    if config.use_tor_for_clearweb and config.tor_proxy:
        return {"http": config.tor_proxy, "https": config.tor_proxy}

    return None


def fetch_page_data(
    session: requests.Session,
    url: str,
    is_onion: bool,
    config: RuntimeConfig,
    include_content: bool,
    timeout_seconds: int,
) -> LinkCheckResult:
    proxies = get_proxies(config, is_onion)

    if is_onion and not proxies:
        return LinkCheckResult(
            original_url=url,
            normalized_url=url,
            is_onion=True,
            works=False,
            status="NOT_TESTABLE_TOR_REQUIRED",
            status_code=None,
            error=".onion link requires Tor proxy (set --tor-proxy or TOR_PROXY)",
            final_url=None,
            title=None,
            meta_description=None,
            h1=None,
            text_excerpt=None,
        )

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    try:
        response = session.get(
            url,
            timeout=timeout_seconds,
            allow_redirects=True,
            verify=False,
            headers=headers,
            proxies=proxies,
            stream=not include_content,
        )
        status_code = response.status_code

        works = 200 <= status_code < 400
        status = "WORKING" if works else "NOT_WORKING"

        title = None
        meta_description = None
        h1 = None
        text_excerpt = None

        if include_content:
            content_type = (response.headers.get("Content-Type") or "").lower()
            if "html" in content_type or "<html" in response.text.lower():
                soup = BeautifulSoup(response.text, "html.parser")
                if soup.title and soup.title.string:
                    title = soup.title.string.strip()[:300]
                meta = soup.find("meta", attrs={"name": "description"})
                if meta and meta.get("content"):
                    meta_description = str(meta["content"]).strip()[:1000]
                h1_tag = soup.find("h1")
                if h1_tag:
                    h1 = h1_tag.get_text(" ", strip=True)[:500]
                text = soup.get_text(" ", strip=True)
                text_excerpt = text[:1500] if text else None

        if not include_content:
            response.close()

        return LinkCheckResult(
            original_url=url,
            normalized_url=url,
            is_onion=is_onion,
            works=works,
            status=status,
            status_code=status_code,
            error=None,
            final_url=str(response.url),
            title=title,
            meta_description=meta_description,
            h1=h1,
            text_excerpt=text_excerpt,
        )
    except requests.exceptions.Timeout:
        return LinkCheckResult(
            original_url=url,
            normalized_url=url,
            is_onion=is_onion,
            works=False,
            status="NOT_WORKING",
            status_code=None,
            error="Timeout",
            final_url=None,
            title=None,
            meta_description=None,
            h1=None,
            text_excerpt=None,
        )
    except requests.exceptions.RequestException as exc:
        return LinkCheckResult(
            original_url=url,
            normalized_url=url,
            is_onion=is_onion,
            works=False,
            status="NOT_WORKING",
            status_code=None,
            error=str(exc)[:500],
            final_url=None,
            title=None,
            meta_description=None,
            h1=None,
            text_excerpt=None,
        )
    except Exception as exc:
        return LinkCheckResult(
            original_url=url,
            normalized_url=url,
            is_onion=is_onion,
            works=False,
            status="NOT_WORKING",
            status_code=None,
            error=str(exc)[:500],
            final_url=None,
            title=None,
            meta_description=None,
            h1=None,
            text_excerpt=None,
        )


def infer_payment_method(text: str | None) -> str | None:
    if not text:
        return None
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


def infer_raas(text: str | None) -> str | None:
    if not text:
        return None
    low = text.lower()
    if "ransomware-as-a-service" in low or "raas" in low:
        return "yes"
    return None


def infer_extortion_strategy(text: str | None) -> str | None:
    if not text:
        return None
    low = text.lower()
    if "triple extortion" in low:
        return "triple extortion"
    if "double extortion" in low:
        return "double extortion"
    if "extortion" in low:
        return "extortion"
    return None


def clean_columns(columns: list[Any]) -> list[str]:
    clean = []
    for c in columns:
        value = "" if pd.isna(c) else str(c).strip()
        clean.append(value)
    return clean


def load_template_attributes(template_file: Path) -> list[str]:
    template_df = pd.read_excel(template_file, sheet_name="Feuil1", header=None)
    attrs = clean_columns(template_df.iloc[1].tolist())
    return attrs


def detect_links_column(df: pd.DataFrame, preferred_column: str) -> str:
    url_regex = re.compile(r"^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(/.*)?$", re.IGNORECASE)

    best_column = preferred_column if preferred_column in df.columns else None
    best_count = -1

    for column in df.columns:
        series = df[column].dropna().astype(str).str.strip()
        if len(series) == 0:
            continue
        count = int(series.str.match(url_regex).sum())
        if count > best_count:
            best_count = count
            best_column = str(column)

    if not best_column or best_count <= 0:
        raise ValueError("No column containing URL-like values was detected in links file")

    return best_column


def categorize_url_structure(url: str, family_prefix_len: int) -> dict[str, str | None]:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower() or None
    host = (parsed.hostname or "").lower() or None
    path = (parsed.path or "").strip()

    first_path_segment = None
    if path:
        cleaned = path.lstrip("/")
        if cleaned:
            first_path_segment = cleaned.split("/", 1)[0]

    first_label = None
    host_base = None
    family_prefix = None
    family_alpha_prefix = None

    if host:
        host_base = host[4:] if host.startswith("www.") else host
        first_label = host_base.split(".", 1)[0]
        family_prefix = first_label[:family_prefix_len] if first_label else None
        alpha_match = re.match(r"^[a-z]+", first_label or "")
        if alpha_match:
            alpha_value = alpha_match.group(0)
            family_alpha_prefix = alpha_value[: max(3, family_prefix_len)]

    return {
        "URL Scheme": scheme,
        "URL Host": host,
        "URL Host Base": host_base,
        "URL First Label": first_label,
        "URL Family Prefix": family_prefix,
        "URL Family Alpha Prefix": family_alpha_prefix,
        "URL First Path Segment": first_path_segment,
    }


def extract_base_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "http").lower()
    host = (parsed.netloc or "").strip()
    if not host:
        return ""
    return f"{scheme}://{host}"


def build_extracted_row(base_row: pd.Series, link_result: LinkCheckResult, attrs: list[str]) -> dict[str, Any]:
    extracted: dict[str, Any] = {attr: None for attr in attrs if attr}

    scraped_text = " ".join(
        [
            link_result.title or "",
            link_result.meta_description or "",
            link_result.h1 or "",
            link_result.text_excerpt or "",
        ]
    ).strip()

    extracted["GANG"] = base_row.get("Ransomware Gang")
    extracted["Victim Name"] = base_row.get("*Claimed* Victim")
    extracted["Date of the breach"] = base_row.get("Date") or base_row.get("Detection Date (UTC+0)")
    extracted["Name of the victim"] = base_row.get("*Claimed* Victim")
    extracted[" Its location"] = base_row.get("Victim Country")
    extracted["type of industry"] = base_row.get("Industrial Sector")
    extracted["what payment method used crypto (BTC or other) or fiat"] = infer_payment_method(scraped_text)
    extracted["use or not of Ransomware-as-a-Service (RaaS)"] = infer_raas(scraped_text)
    extracted["Attacking stratgy (double extortion, triple extortion, etc.)"] = infer_extortion_strategy(scraped_text)

    extracted["_Link Source"] = link_result.original_url
    extracted["_Link Tested"] = link_result.normalized_url
    extracted["_Link Status"] = link_result.status
    extracted["_HTTP Status Code"] = link_result.status_code
    extracted["_Error"] = link_result.error
    extracted["_Final URL"] = link_result.final_url
    extracted["_Title"] = link_result.title
    extracted["_Meta Description"] = link_result.meta_description
    extracted["_H1"] = link_result.h1
    extracted["_Text Excerpt"] = link_result.text_excerpt

    return extracted


def write_posts_report(
    output_path: Path,
    target_base: str,
    post_results: dict[str, LinkCheckResult],
    generated_at: str,
) -> None:
    working = [r for r in post_results.values() if r.works]
    not_working = [r for r in post_results.values() if not r.works]

    lines: list[str] = []
    lines.append(f"Generated At: {generated_at}")
    lines.append(f"Target Base URL: {target_base}")
    lines.append(f"Total Posts Tested: {len(post_results)}")
    lines.append(f"Working Posts: {len(working)}")
    lines.append(f"Not Working Posts: {len(not_working)}")
    lines.append("")

    lines.append("=== WORKING POSTS ===")
    if working:
        for idx, result in enumerate(sorted(working, key=lambda x: x.normalized_url), start=1):
            lines.append(f"{idx}. URL: {result.normalized_url}")
            lines.append(f"   Status: {result.status}")
            lines.append(f"   HTTP: {result.status_code}")
            if result.final_url:
                lines.append(f"   Final URL: {result.final_url}")
            if result.title:
                lines.append(f"   Title: {result.title}")
            lines.append("")
    else:
        lines.append("None")
        lines.append("")

    lines.append("=== NOT WORKING POSTS ===")
    if not_working:
        for idx, result in enumerate(sorted(not_working, key=lambda x: x.normalized_url), start=1):
            error_text = result.error or (f"HTTP {result.status_code}" if result.status_code is not None else "Unknown error")
            lines.append(f"{idx}. URL: {result.normalized_url}")
            lines.append(f"   Status: {result.status}")
            lines.append(f"   HTTP: {result.status_code}")
            lines.append(f"   Error: {error_text}")
            lines.append("")
    else:
        lines.append("None")
        lines.append("")

    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def load_tabular_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    try:
        return pd.read_csv(path)
    except EmptyDataError as exc:
        raise ValueError(f"Base input file is empty or invalid CSV: {path}") from exc


def main() -> None:
    args = parse_args()
    config = build_runtime_config(args)

    if config.timeout <= 0:
        raise ValueError("--timeout must be a positive integer")
    if config.precheck_timeout <= 0:
        raise ValueError("--precheck-timeout must be a positive integer")
    if config.base_input_csv is None and not config.links_file.exists():
        raise FileNotFoundError(f"Links file not found: {config.links_file}")
    if config.base_input_csv is not None and not config.base_input_csv.exists():
        raise FileNotFoundError(f"Base input CSV not found: {config.base_input_csv}")
    if config.base_input_csv is None and not config.template_file.exists():
        raise FileNotFoundError(f"Template file not found: {config.template_file}")

    log(config, "Starting job", force=True)
    if config.base_input_csv is not None:
        log(config, f"Base input CSV: {config.base_input_csv}", force=True)
    else:
        log(config, f"Links file: {config.links_file}", force=True)
    log(config, f"Template file: {config.template_file}", force=True)
    log(config, f"Output file: {config.output_file}", force=True)
    log(config, f"Base CSV file: {config.base_csv_file}", force=True)
    log(config, f"Working links TXT file: {config.working_links_txt_file}", force=True)
    log(config, f"Posts report TXT file: {config.posts_report_txt_file}", force=True)
    log(config, f"Test posts for base: {config.test_posts_for_base or 'None'}", force=True)
    log(config, f"Precheck timeout: {config.precheck_timeout}s", force=True)
    log(config, f"Scrape timeout: {config.timeout}s", force=True)
    log(config, f"Tor proxy: {config.tor_proxy or 'None'}", force=True)
    log(config, f"Use Tor for clearweb: {config.use_tor_for_clearweb}", force=True)
    log(config, f"Scrape only WORKING links: {config.scrape_only_working}", force=True)
    log(config, f"Fast working input mode: {config.fast_working_input}", force=True)
    log(config, f"URL family prefix length: {config.family_prefix_len}", force=True)
    log(config, f"Detailed progress every {config.log_every} link(s)", force=True)

    requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    if config.base_input_csv is not None:
        log(config, "Loading links from base input CSV")
        links_df = load_tabular_file(config.base_input_csv)
        if links_df.empty:
            raise ValueError(f"Base input file has no rows: {config.base_input_csv}")
        preferred_column = "Base URL" if "Base URL" in links_df.columns else config.links_column

        if config.fast_working_input and "Base Works" in links_df.columns:
            before_count = len(links_df)
            links_df = links_df[links_df["Base Works"].apply(as_bool)].copy()
            log(
                config,
                f"Fast mode: filtered base CSV from {before_count} to {len(links_df)} rows using Base Works=True",
                force=True,
            )
            if links_df.empty:
                raise ValueError(
                    f"No rows with Base Works=True in input file: {config.base_input_csv}. "
                    "Run precheck first or disable --fast-working-input."
                )
    else:
        log(config, f"Loading links sheet '{config.links_sheet}'")
        links_df = pd.read_excel(config.links_file, sheet_name=config.links_sheet)
        preferred_column = config.links_column

    effective_links_column = detect_links_column(links_df, preferred_column)
    log(config, f"Detected URL column: {effective_links_column}", force=True)

    if config.template_file.exists():
        log(config, "Loading extraction attributes from template")
        attrs = load_template_attributes(config.template_file)
    else:
        log(config, "Template not found for this mode; extracted data will include link metadata only", force=True)
        attrs = []

    work_rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    total_rows = int(len(links_df))

    prepared_rows: list[dict[str, Any]] = []
    invalid_count = 0
    base_to_rows: dict[str, list[int]] = {}

    for index, row in links_df.iterrows():
        raw_url = "" if pd.isna(row.get(effective_links_column)) else str(row.get(effective_links_column)).strip()
        if not raw_url:
            continue

        normalized = normalize_url(raw_url)
        structure = categorize_url_structure(normalized, config.family_prefix_len)
        if not is_probable_url(normalized):
            invalid_count += 1
            prepared_rows.append(
                {
                    "row_index": index,
                    "raw_url": raw_url,
                    "normalized": normalized,
                    "base_url": "",
                    "is_valid": False,
                    "is_onion": False,
                    "structure": structure,
                    "row_data": row.to_dict(),
                }
            )
            continue

        base_url = extract_base_url(normalized)
        is_onion = detect_onion(base_url)
        prepared_rows.append(
            {
                "row_index": index,
                "raw_url": raw_url,
                "normalized": normalized,
                "base_url": base_url,
                "is_valid": True,
                "is_onion": is_onion,
                "structure": structure,
                "row_data": row.to_dict(),
            }
        )
        base_to_rows.setdefault(base_url, []).append(index)

    unique_bases = sorted(base_to_rows.keys())
    total_bases = len(unique_bases)
    log(
        config,
        f"Filtered {total_rows} rows into {total_bases} unique base URLs (invalid rows: {invalid_count})",
        force=True,
    )

    base_precheck: dict[str, LinkCheckResult] = {}
    base_scrape: dict[str, LinkCheckResult] = {}
    direct_post_results: dict[int, LinkCheckResult] = {}

    if config.fast_working_input:
        log(config, f"PHASE 1/1 DIRECT SCRAPE: scraping {len([r for r in prepared_rows if r['is_valid']])} provided URLs", force=True)
        direct_started = time.perf_counter()
        valid_rows = [r for r in prepared_rows if r["is_valid"]]
        with requests.Session() as direct_session:
            for n, item in enumerate(valid_rows, start=1):
                normalized_url = str(item["normalized"])
                raw_url = str(item["raw_url"])
                is_onion = bool(item["is_onion"])
                result = fetch_page_data(
                    session=direct_session,
                    url=normalized_url,
                    is_onion=is_onion,
                    config=config,
                    include_content=True,
                    timeout_seconds=config.timeout,
                )
                result.original_url = raw_url
                result.normalized_url = raw_url
                direct_post_results[int(item["row_index"])] = result

                if n % config.log_every == 0:
                    elapsed = time.perf_counter() - direct_started
                    rate = n / elapsed if elapsed > 0 else 0
                    remaining = (len(valid_rows) - n) / rate if rate > 0 else 0
                    log(
                        config,
                        f"DIRECT SCRAPE [{n}/{len(valid_rows)}] status={result.status} code={result.status_code} elapsed={elapsed:.1f}s eta={remaining:.1f}s",
                    )

        elapsed = time.perf_counter() - direct_started
        log(config, f"Direct scrape complete | processed={len(valid_rows)} total_time={elapsed:.1f}s", force=True)
        bases_to_scrape: list[str] = []
    else:
        precheck_stats = {"WORKING": 0, "NOT_WORKING": 0, "NOT_TESTABLE_TOR_REQUIRED": 0}
        started = time.perf_counter()
        log(config, f"PHASE 1/2 PRECHECK: checking connectivity on {total_bases} base URLs", force=True)

        with requests.Session() as session:
            for n, base_url in enumerate(unique_bases, start=1):
                is_onion = detect_onion(base_url)
                result = fetch_page_data(
                    session=session,
                    url=base_url,
                    is_onion=is_onion,
                    config=config,
                    include_content=False,
                    timeout_seconds=config.precheck_timeout,
                )
                result.original_url = base_url
                result.normalized_url = base_url
                base_precheck[base_url] = result
                precheck_stats[result.status] = precheck_stats.get(result.status, 0) + 1

                if n % config.log_every == 0:
                    elapsed = time.perf_counter() - started
                    rate = n / elapsed if elapsed > 0 else 0
                    remaining = (total_bases - n) / rate if rate > 0 else 0
                    log(
                        config,
                        f"PRECHECK BASE [{n}/{total_bases}] status={result.status} code={result.status_code} onion={is_onion} elapsed={elapsed:.1f}s eta={remaining:.1f}s",
                    )

        elapsed_total = time.perf_counter() - started
        log(
            config,
            (
                "Precheck complete on base URLs | "
                f"bases={total_bases} "
                f"working={precheck_stats.get('WORKING', 0)} "
                f"not_working={precheck_stats.get('NOT_WORKING', 0)} "
                f"not_testable={precheck_stats.get('NOT_TESTABLE_TOR_REQUIRED', 0)} "
                f"invalid_rows={invalid_count} "
                f"total_time={elapsed_total:.1f}s"
            ),
            force=True,
        )

        if config.scrape_only_working:
            bases_to_scrape = [u for u, r in base_precheck.items() if r.status == "WORKING"]
        else:
            bases_to_scrape = [u for u, r in base_precheck.items() if r.status in {"WORKING", "NOT_WORKING"}]

        log(config, f"PHASE 2/2 SCRAPE: targeted base URLs={len(bases_to_scrape)}", force=True)
        scrape_started = time.perf_counter()
        with requests.Session() as scrape_session:
            for n, base_url in enumerate(bases_to_scrape, start=1):
                is_onion = detect_onion(base_url)
                detail = fetch_page_data(
                    session=scrape_session,
                    url=base_url,
                    is_onion=is_onion,
                    config=config,
                    include_content=True,
                    timeout_seconds=config.timeout,
                )
                detail.original_url = base_url
                detail.normalized_url = base_url
                base_scrape[base_url] = detail

                if n % config.log_every == 0:
                    elapsed = time.perf_counter() - scrape_started
                    rate = n / elapsed if elapsed > 0 else 0
                    remaining = (len(bases_to_scrape) - n) / rate if rate > 0 else 0
                    log(
                        config,
                        f"SCRAPE BASE [{n}/{len(bases_to_scrape)}] status={detail.status} code={detail.status_code} onion={is_onion} elapsed={elapsed:.1f}s eta={remaining:.1f}s",
                    )

        scrape_elapsed = time.perf_counter() - scrape_started
        log(config, f"Scrape complete on base URLs | processed={len(bases_to_scrape)} total_time={scrape_elapsed:.1f}s", force=True)

    post_results: dict[str, LinkCheckResult] = {}
    selected_posts_base: str | None = None
    if config.test_posts_for_base:
        selected_posts_base = extract_base_url(normalize_url(config.test_posts_for_base))
        if not selected_posts_base:
            log(config, "Post test skipped: invalid --test-posts-for-base value", force=True)
        elif config.fast_working_input:
            for item in prepared_rows:
                if not item["is_valid"]:
                    continue
                if str(item["base_url"]) != selected_posts_base:
                    continue
                result = direct_post_results.get(int(item["row_index"]))
                if result is None:
                    continue
                post_results[str(item["raw_url"])] = result

            log(
                config,
                f"POST REPORT (from direct scrape cache): base={selected_posts_base} candidate_posts={len(post_results)}",
                force=True,
            )
            write_posts_report(
                output_path=config.posts_report_txt_file,
                target_base=selected_posts_base,
                post_results=post_results,
                generated_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            )
            log(config, f"Posts report generated: {config.posts_report_txt_file}", force=True)
        else:
            candidate_posts = sorted(
                {
                    (
                        str(item["raw_url"]),
                        str(item["normalized"]),
                    )
                    for item in prepared_rows
                    if item["is_valid"] and str(item["base_url"]) == selected_posts_base
                },
                key=lambda pair: pair[0],
            )
            log(
                config,
                f"PHASE 3/3 POST TEST: base={selected_posts_base} candidate_posts={len(candidate_posts)}",
                force=True,
            )

            with requests.Session() as post_session:
                for n, (post_url_original, post_url_normalized) in enumerate(candidate_posts, start=1):
                    is_onion = detect_onion(post_url_normalized)
                    post_result = fetch_page_data(
                        session=post_session,
                        url=post_url_normalized,
                        is_onion=is_onion,
                        config=config,
                        include_content=True,
                        timeout_seconds=config.timeout,
                    )
                    post_result.original_url = post_url_original
                    post_result.normalized_url = post_url_original
                    post_results[post_url_original] = post_result

                    if n % config.log_every == 0:
                        log(
                            config,
                            f"POST TEST [{n}/{len(candidate_posts)}] status={post_result.status} code={post_result.status_code}",
                        )

            write_posts_report(
                output_path=config.posts_report_txt_file,
                target_base=selected_posts_base,
                post_results=post_results,
                generated_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            )
            log(config, f"Posts report generated: {config.posts_report_txt_file}", force=True)

    for item in prepared_rows:
        merged = dict(item["row_data"])
        merged.update(
            {
                "Source URL Column": effective_links_column,
                "Checked At (UTC)": now,
                "Original URL": item["raw_url"],
                "Normalized URL": item["normalized"],
                "Base URL": item["base_url"],
                "Is Onion": item["is_onion"],
            }
        )
        merged.update(item["structure"])

        if not item["is_valid"]:
            merged.update(
                {
                    "Link Status": "INVALID_URL",
                    "Works": False,
                    "HTTP Status Code": None,
                    "Error": "Invalid URL format",
                    "Final URL": None,
                    "Title": None,
                    "Meta Description": None,
                    "H1": None,
                    "Text Excerpt": None,
                    "Connectivity Checked On": None,
                    "Scraping Source": None,
                }
            )
            work_rows.append(merged)
            continue

        base_url = str(item["base_url"])
        precheck_result = base_precheck.get(base_url)
        final_result = base_scrape.get(base_url) or precheck_result

        if config.fast_working_input:
            final_result = direct_post_results.get(int(item["row_index"]))

        row_original_url = str(item["raw_url"])
        if row_original_url in post_results:
            final_result = post_results[row_original_url]

        if final_result is None:
            merged.update(
                {
                    "Link Status": "NOT_WORKING",
                    "Works": False,
                    "HTTP Status Code": None,
                    "Error": "Missing base check result",
                    "Final URL": None,
                    "Title": None,
                    "Meta Description": None,
                    "H1": None,
                    "Text Excerpt": None,
                    "Connectivity Checked On": base_url,
                    "Scraping Source": "BASE_URL",
                }
            )
        else:
            if row_original_url in post_results:
                scraping_source = "POST_URL"
            elif config.fast_working_input:
                scraping_source = "POST_INPUT"
            else:
                scraping_source = "BASE_URL"
            merged.update(
                {
                    "Link Status": final_result.status,
                    "Works": final_result.works,
                    "HTTP Status Code": final_result.status_code,
                    "Error": final_result.error,
                    "Final URL": final_result.final_url,
                    "Title": final_result.title,
                    "Meta Description": final_result.meta_description,
                    "H1": final_result.h1,
                    "Text Excerpt": final_result.text_excerpt,
                    "Connectivity Checked On": row_original_url if scraping_source in {"POST_URL", "POST_INPUT"} else base_url,
                    "Scraping Source": scraping_source,
                }
            )

        work_rows.append(merged)

    result_df = pd.DataFrame(work_rows)

    scraped_bases_count = len(bases_to_scrape)
    tested_input_posts_count = len(direct_post_results)

    base_rows: list[dict[str, Any]] = []
    for base_url in unique_bases:
        base_result = base_scrape.get(base_url) or base_precheck.get(base_url)
        if base_result is None:
            continue
        base_rows.append(
            {
                "Base URL": base_url,
                "Base Status": base_result.status,
                "Base Works": base_result.works,
                "Base HTTP Status Code": base_result.status_code,
                "Base Error": base_result.error,
                "Base Final URL": base_result.final_url,
                "Base Is Onion": detect_onion(base_url),
                "Child Links Count": len(base_to_rows.get(base_url, [])),
                "Base Title": base_result.title,
                "Base Meta Description": base_result.meta_description,
                "Base H1": base_result.h1,
            }
        )
    base_df = pd.DataFrame(base_rows)
    if not base_df.empty:
        base_df = base_df.sort_values(by=["Base Works", "Child Links Count", "Base URL"], ascending=[False, False, True])

    working_df = result_df[result_df["Works"] == True].copy()  # noqa: E712
    not_working_df = result_df[result_df["Works"] == False].copy()  # noqa: E712

    family_group_col = "URL Family Alpha Prefix"
    family_summary_df = result_df.copy()
    family_summary_df[family_group_col] = family_summary_df[family_group_col].fillna("unknown")
    family_summary_df = (
        family_summary_df.groupby(family_group_col, dropna=False)
        .agg(
            total_links=("Normalized URL", "count"),
            working_links=("Works", lambda s: int((s == True).sum())),  # noqa: E712
            not_working_links=("Works", lambda s: int((s == False).sum())),  # noqa: E712
            onion_links=("Is Onion", lambda s: int(s.fillna(False).sum())),
            invalid_links=("Link Status", lambda s: int((s == "INVALID_URL").sum())),
            not_testable_tor=("Link Status", lambda s: int((s == "NOT_TESTABLE_TOR_REQUIRED").sum())),
        )
        .reset_index()
        .rename(columns={family_group_col: "family"})
    )
    family_summary_df["working_ratio"] = (
        family_summary_df["working_links"] / family_summary_df["total_links"].replace(0, pd.NA)
    ).fillna(0.0)
    family_summary_df = family_summary_df.sort_values(
        by=["working_links", "total_links", "family"], ascending=[False, False, True]
    )

    extracted_rows = []
    for _, row in result_df.iterrows():
        tested_base_url = str(row.get("Base URL", "") or "")
        link_result = LinkCheckResult(
            original_url=str(row.get("Original URL", row.get(effective_links_column, "")) or ""),
            normalized_url=tested_base_url or str(row.get("Normalized URL", "") or ""),
            is_onion=bool(row.get("Is Onion", False)),
            works=bool(row.get("Works", False)),
            status=str(row.get("Link Status", "")),
            status_code=None if pd.isna(row.get("HTTP Status Code")) else int(row.get("HTTP Status Code")),
            error=None if pd.isna(row.get("Error")) else str(row.get("Error")),
            final_url=None if pd.isna(row.get("Final URL")) else str(row.get("Final URL")),
            title=None if pd.isna(row.get("Title")) else str(row.get("Title")),
            meta_description=None if pd.isna(row.get("Meta Description")) else str(row.get("Meta Description")),
            h1=None if pd.isna(row.get("H1")) else str(row.get("H1")),
            text_excerpt=None if pd.isna(row.get("Text Excerpt")) else str(row.get("Text Excerpt")),
        )
        extracted_rows.append(build_extracted_row(row, link_result, attrs))

    extracted_df = pd.DataFrame(extracted_rows)

    summary_df = pd.DataFrame(
        [
            {"metric": "total_links", "value": int(len(result_df))},
            {"metric": "working_links", "value": int(len(working_df))},
            {"metric": "not_working_links", "value": int(len(not_working_df))},
            {"metric": "onion_links", "value": int(result_df["Is Onion"].sum()) if len(result_df) else 0},
            {
                "metric": "not_testable_tor_required",
                "value": int((result_df["Link Status"] == "NOT_TESTABLE_TOR_REQUIRED").sum()) if len(result_df) else 0,
            },
        ]
    )

    summary_df = pd.concat(
        [
            summary_df,
            pd.DataFrame(
                [
                    {"metric": "input_mode", "value": "base_csv" if config.base_input_csv is not None else "excel"},
                    {"metric": "base_input_csv", "value": str(config.base_input_csv) if config.base_input_csv else ""},
                    {"metric": "source_links_file", "value": str(config.links_file)},
                    {"metric": "source_template_file", "value": str(config.template_file)},
                    {"metric": "effective_links_column", "value": effective_links_column},
                    {"metric": "tor_proxy", "value": config.tor_proxy or ""},
                    {"metric": "use_tor_for_clearweb", "value": str(config.use_tor_for_clearweb)},
                    {"metric": "scrape_only_working", "value": str(config.scrape_only_working)},
                    {"metric": "fast_working_input", "value": str(config.fast_working_input)},
                    {"metric": "family_prefix_len", "value": int(config.family_prefix_len)},
                    {"metric": "precheck_timeout_seconds", "value": int(config.precheck_timeout)},
                    {"metric": "timeout_seconds", "value": int(config.timeout)},
                    {"metric": "unique_base_urls", "value": int(total_bases)},
                    {"metric": "scraped_base_urls_count", "value": int(scraped_bases_count)},
                    {"metric": "tested_input_posts_count", "value": int(tested_input_posts_count)},
                    {"metric": "test_posts_for_base", "value": selected_posts_base or ""},
                    {"metric": "tested_posts_count", "value": int(len(post_results))},
                    {"metric": "posts_report_txt_file", "value": str(config.posts_report_txt_file)},
                ]
            ),
        ],
        ignore_index=True,
    )

    with pd.ExcelWriter(config.output_file, engine="openpyxl") as writer:
        log(config, "Writing output workbook", force=True)
        result_df.to_excel(writer, sheet_name="All Links Checked", index=False)
        base_df.to_excel(writer, sheet_name="Base URLs Checked", index=False)
        working_df.to_excel(writer, sheet_name="Working Links", index=False)
        not_working_df.to_excel(writer, sheet_name="Not Working Links", index=False)
        family_summary_df.to_excel(writer, sheet_name="Family Summary", index=False)
        extracted_df.to_excel(writer, sheet_name="Extracted Data", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    log(config, "Writing base URLs CSV", force=True)
    base_df.to_csv(config.base_csv_file, index=False)

    log(config, "Writing working links TXT", force=True)
    if "Base URL" in working_df.columns:
        working_links = working_df["Base URL"].dropna().astype(str).str.strip()
    elif "Normalized URL" in working_df.columns:
        working_links = working_df["Normalized URL"].dropna().astype(str).str.strip()
    else:
        working_links = pd.Series([], dtype="string")

    working_links = working_links[working_links != ""].drop_duplicates()
    content = "\n".join(working_links.tolist())
    if content:
        content += "\n"
    config.working_links_txt_file.write_text(content, encoding="utf-8")

    log(config, f"Output generated: {config.output_file}", force=True)
    log(config, f"Base CSV generated: {config.base_csv_file}", force=True)
    log(config, f"Working links TXT generated: {config.working_links_txt_file}", force=True)
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
