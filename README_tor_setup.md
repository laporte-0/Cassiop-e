# Post-data scraping (clean workflow)

This project now focuses on one main step:

1. You already have valid post links
2. Scrape each post page
3. Map extracted values to attributes from `Fichier de données.xlsx`
4. Export final dataset to Excel

## QUICKSTART (on another PC)

```bash
# 1. Clone the repo
git clone git@github.com:laporte-0/Cassiop-e.git
cd Cassiop-e

# 2. Make launcher executable
chmod +x run.sh

# 3. Prepare your links (one URL per line)
# - Use existing working_links.txt, or
# - Copy your own file: cp your_links.txt working_links.txt

# 4. Start Tor + Privoxy (if using .onion pages)
# Ubuntu/Debian:
sudo apt install -y tor privoxy
sudo systemctl start tor privoxy

# 5. Run with one simple command (raw extraction first)
./run.sh

# Output: resultats_posts_scraped.xlsx (in same folder)
```

By default, `./run.sh` uses `mode=raw` and creates:

- `Raw Posts` sheet: direct extracted data from each post (title, h1, body text, status, errors)

Then run mapping step:

```bash
./run.sh -m mapped
```

Or both in one file:

```bash
./run.sh -m both
```

If posts are blocked by queue/challenge pages, use browser engine:

```bash
./run.sh -e playwright -m raw
```

If CAPTCHA appears, run manual mode:

```bash
./run.sh -e playwright -m raw --manual-captcha
```

This opens a visible browser, you solve CAPTCHA once, press ENTER in terminal, then scraping continues with saved session.

For Tor SOCKS proxy (recommended with Playwright):

```bash
BROWSER_PROXY="socks5://127.0.0.1:9050" ./run.sh -e playwright -m raw
```

If you need custom files:
```bash
./run.sh -i
# or
./run.sh -n your_links.csv -o my_output.xlsx
# or
./run.sh -m both
```

---

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

Playwright engine supports SOCKS directly and is better when pages show queue/challenge protection.

## 3) Run (short commands)

Make launcher executable once:

```bash
chmod +x run.sh
```

Then use one command:

```bash
./run.sh
```

This uses default files in the project folder and writes:

- `resultats_posts_scraped.xlsx`

Interactive mode (prompts instead of long flags):

```bash
./run.sh -i
```

Short custom run example:

```bash
./run.sh -n working_links.txt -o result.xlsx
```

Switch engine:

```bash
./run.sh -e playwright -m raw
```

Manual CAPTCHA (Playwright):

```bash
./run.sh -e playwright -m raw --manual-captcha
```

Change proxy without long command:

```bash
TOR_HTTP_PROXY="http://127.0.0.1:8118" ./run.sh
```

You can still run python script directly if needed.

## 3.b) Direct python mode (optional)

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
