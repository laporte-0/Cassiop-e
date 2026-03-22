# Post-data scraping (clean workflow)

This project now focuses on one main step:

1. You already have valid post links
2. Scrape each post page
3. Map extracted values to attributes from `Fichier de données.xlsx`
4. Export final dataset to Excel

## 1) Environment setup

```bash
cd Cassioopée
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Tor proxy for .onion pages

The Scrapy script expects an HTTP proxy for onion pages (for example Tor + Privoxy):

- `http://127.0.0.1:8118`

If your stack is SOCKS-only, keep using the existing non-Scrapy flow or add an HTTP bridge.

## 3) Run the unified scraper

Default run (uses `working_links.txt`):

```bash
python crawl_posts_with_scrapy.py \
  --input working_links.txt \
  --template-file "Fichier de données.xlsx" \
  --source-file "Cassiopée Envoi2 Cactus à CryptOn.xlsx" \
  --tor-proxy "http://127.0.0.1:8118" \
  --output resultats_posts_scraped.xlsx
```

CSV/XLSX input example:

```bash
python crawl_posts_with_scrapy.py \
  --input your_posts.csv \
  --input-url-column "url" \
  --template-file "Fichier de données.xlsx" \
  --output resultats_posts_scraped.xlsx
```

## 4) Output

Generated Excel file contains:

- `Extracted Data`: template attributes + scraping metadata columns (`_Link Status`, `_Error`, `_Title`, ...)
- `Summary`: totals (`total_links`, `working_links`, `failed_links`, timestamp)

## 5) Notes

- The extractor is generic: it uses title/meta/H1/body/JSON-LD text to handle different page structures.
- Some business attributes can remain empty when the page does not expose the required data.
- Context fields (`GANG`, `Victim Name`, etc.) are auto-filled from the source file when URL matches.
