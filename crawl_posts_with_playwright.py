from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from crawl_posts_with_scrapy import (
    DEFAULT_INPUT,
    DEFAULT_OUTPUT,
    DEFAULT_SOURCE_FILE,
    DEFAULT_TEMPLATE,
    build_output_row,
    clean_text,
    detect_intermediary_block,
    detect_url_column,
    extract_body_text_from_html,
    extract_context_row,
    load_links,
    load_template_attributes,
    write_output,
)

DEFAULT_BROWSER_PROXY = "socks5://127.0.0.1:9050"
DEFAULT_STATE_FILE = str(Path(__file__).resolve().parent / "playwright_state.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape post pages with Playwright and map data to Fichier de données attributes.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Path to post links file (.txt/.csv/.xlsx)")
    parser.add_argument("--input-url-column", default=None, help="Optional URL column when input is CSV/XLSX")
    parser.add_argument("--template-file", default=str(DEFAULT_TEMPLATE), help="Path to Fichier de données.xlsx")
    parser.add_argument("--source-file", default=str(DEFAULT_SOURCE_FILE), help="Optional source dataset for context fields")
    parser.add_argument("--source-url-column", default=None, help="Optional source URL column override")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to output Excel file")
    parser.add_argument("--timeout", type=int, default=25, help="Page load timeout in seconds")
    parser.add_argument("--wait-ms", type=int, default=3000, help="Extra wait after page load in milliseconds")
    parser.add_argument("--browser-proxy", default=DEFAULT_BROWSER_PROXY, help="Browser proxy URL (example: socks5://127.0.0.1:9050)")
    parser.add_argument("--headful", action="store_true", help="Run browser in visible mode (debug)")
    parser.add_argument("--manual-captcha", action="store_true", help="Open browser and pause to solve CAPTCHA manually, then reuse saved session")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE, help="Path to Playwright storage state JSON")
    parser.add_argument(
        "--mode",
        choices=["raw", "mapped", "both"],
        default="both",
        help="Output mode: raw only, mapped only, or both sheets",
    )
    return parser.parse_args()


def extract_jsonld_from_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    blocks: list[str] = []
    for node in soup.select("script[type='application/ld+json']"):
        raw = clean_text(node.get_text(" ", strip=True))
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            blocks.append(raw)
            continue

        def walk(value: object) -> None:
            if isinstance(value, dict):
                for nested in value.values():
                    walk(nested)
            elif isinstance(value, list):
                for nested in value:
                    walk(nested)
            elif isinstance(value, (str, int, float)):
                blocks.append(str(value))

        walk(parsed)

    return clean_text(" ".join(blocks))


def scrape_raw_rows(
    links: list[str],
    timeout_seconds: int,
    wait_ms: int,
    browser_proxy: str,
    headless: bool,
    manual_captcha: bool,
    state_file: str,
) -> tuple[list[dict[str, object]], int]:
    raw_rows: list[dict[str, object]] = []
    ok_count = 0

    launch_kwargs: dict[str, object] = {"headless": headless}
    if browser_proxy.strip():
        launch_kwargs["proxy"] = {"server": browser_proxy.strip()}

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_kwargs)
        if Path(state_file).exists():
            context = browser.new_context(ignore_https_errors=True, storage_state=state_file)
        else:
            context = browser.new_context(ignore_https_errors=True)

        if manual_captcha and links:
            warmup = context.new_page()
            try:
                warmup.goto(links[0], wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
                if wait_ms > 0:
                    warmup.wait_for_timeout(wait_ms)
                print("Manual CAPTCHA mode enabled.")
                print("Solve CAPTCHA/login in the opened browser, then press ENTER here to continue...")
                input()
                context.storage_state(path=state_file)
                print(f"Saved browser session state to: {state_file}")
            finally:
                warmup.close()

        for link in links:
            page = context.new_page()
            try:
                page.goto(link, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
                if wait_ms > 0:
                    page.wait_for_timeout(wait_ms)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                title = clean_text(page.title())
                meta_description = clean_text(
                    (soup.select_one("meta[name='description']") or {}).get("content", "")  # type: ignore[union-attr]
                )
                h1_tag = soup.find("h1")
                h1 = clean_text(h1_tag.get_text(" ", strip=True) if h1_tag else "")
                body_text = extract_body_text_from_html(html)
                jsonld_text = extract_jsonld_from_html(html)
                text_excerpt = clean_text(" ".join([title, meta_description, h1, body_text]))[:2000]
                block_reason = detect_intermediary_block(title, h1, body_text, html)

                status = "BLOCKED_INTERMEDIARY" if block_reason else "WORKING"
                error = f"Intermediary page detected: {block_reason}" if block_reason else None
                if status == "WORKING":
                    ok_count += 1

                raw_rows.append(
                    {
                        "_Link Source": link,
                        "_Link Status": status,
                        "_HTTP Status Code": 200,
                        "_Error": error,
                        "_Final URL": page.url,
                        "_Title": title,
                        "_Meta Description": meta_description,
                        "_H1": h1,
                        "_Text Excerpt": text_excerpt,
                        "_Body Text": body_text,
                        "_JSONLD Text": jsonld_text,
                        "_Scraped At": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                    }
                )
            except Exception as exc:
                raw_rows.append(
                    {
                        "_Link Source": link,
                        "_Link Status": "NOT_WORKING",
                        "_HTTP Status Code": None,
                        "_Error": clean_text(str(exc)),
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
            finally:
                page.close()

        context.close()
        browser.close()

    return raw_rows, ok_count


def build_mapped_rows(
    raw_rows: list[dict[str, object]],
    attributes: list[str],
    source_frame: pd.DataFrame,
    source_url_column: str,
) -> list[dict[str, object]]:
    mapped: list[dict[str, object]] = []
    for raw in raw_rows:
        post_url = str(raw.get("_Link Source") or "")
        final_url = raw.get("_Final URL")
        context_row = extract_context_row(source_frame, post_url, source_url_column)
        html_text = clean_text(" ".join([str(raw.get("_Body Text") or ""), str(raw.get("_JSONLD Text") or "")]))

        mapped.append(
            build_output_row(
                attributes=attributes,
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
    return mapped


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
        raise ValueError(f"No valid links found in input: {input_path}")

    raw_rows, ok_count = scrape_raw_rows(
        links=links,
        timeout_seconds=args.timeout,
        wait_ms=args.wait_ms,
        browser_proxy=args.browser_proxy,
        headless=not args.headful,
        manual_captcha=bool(args.manual_captcha),
        state_file=str(args.state_file),
    )

    mapped_rows: list[dict[str, object]] = []
    if args.mode in {"mapped", "both"}:
        mapped_rows = build_mapped_rows(raw_rows, attributes, source_frame, source_url_column)

    write_output(
        output_path,
        raw_rows=raw_rows,
        extracted_rows=mapped_rows,
        links_count=len(links),
        ok_count=ok_count,
        mode=args.mode,
    )

    print(f"Playwright scraping completed. Output: {output_path}")


if __name__ == "__main__":
    main()
